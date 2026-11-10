// Copyright 2026 The Agent Substrate Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package provider

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"plugins.go.ate.dev/objectstore-azure-blob/internal/blobstore"
)

// rangedFixture is a blob read through a rangedReader whose ranges come from
// fetch, or from the blob itself when fetch is nil.
type rangedFixture struct {
	data   []byte
	limits downloadLimits
	fetch  rangeFetcher
	calls  atomic.Int64
}

func (f *rangedFixture) open(ctx context.Context) *rangedReader {
	head := io.NopCloser(bytes.NewReader(f.data[:min(int64(len(f.data)), f.limits.rangeSize)]))
	return newRangedReader(ctx, f.limits, "blob", head, int64(len(f.data)), func(ctx context.Context, off, n int64) (io.ReadCloser, error) {
		f.calls.Add(1)
		if f.fetch != nil {
			return f.fetch(ctx, off, n)
		}
		return io.NopCloser(bytes.NewReader(f.data[off : off+n])), nil
	})
}

// buffersInUse reports how many buffers of limits are taken.
func buffersInUse(limits downloadLimits) int { return len(limits.buffers.slots) }

func TestRangedReaderReadsInOrder(t *testing.T) {
	const rangeSize = 4
	for _, size := range []int{0, 1, rangeSize, rangeSize + 1, 2 * rangeSize, 9*rangeSize + 3} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := &rangedFixture{data: patterned(size), limits: newDownloadLimits(rangeSize, 3, 1<<20)}
				// Later ranges finish first.
				f.fetch = func(_ context.Context, off, n int64) (io.ReadCloser, error) {
					time.Sleep(time.Duration(size-int(off)) * time.Millisecond)
					return io.NopCloser(bytes.NewReader(f.data[off : off+n])), nil
				}
				r := f.open(context.Background())
				got, err := io.ReadAll(r)
				if err != nil {
					t.Fatalf("ReadAll: %v", err)
				}
				if !bytes.Equal(got, f.data) {
					t.Fatalf("read %d bytes that differ from the %d byte blob", len(got), len(f.data))
				}
				if err := r.Close(); err != nil {
					t.Fatal(err)
				}
				// The head covers the first range, so a blob of up to one
				// range is a single request.
				if want := max(0, (size-1)/rangeSize); f.calls.Load() != int64(want) {
					t.Errorf("fetched %d ranges, want %d", f.calls.Load(), want)
				}
				if n := buffersInUse(f.limits); n != 0 {
					t.Errorf("%d buffers still taken after Close", n)
				}
			})
		})
	}
}

// TestRangedReaderBoundsBuffers checks that a fetch reads at most ahead
// ranges past its decoder and that fetches together hold no more buffers than
// the budget, and that fetches sharing a small budget all finish.
func TestRangedReaderBoundsBuffers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const rangeSize, ahead, shared = 4, 3, 4
		limits := newDownloadLimits(rangeSize, ahead, shared*rangeSize)
		one := &rangedFixture{data: patterned(10 * rangeSize), limits: limits}
		two := &rangedFixture{data: bytes.Repeat([]byte{7}, 10*rangeSize), limits: limits}
		r1, r2 := one.open(context.Background()), two.open(context.Background())
		defer r1.Close()
		defer r2.Close()

		synctest.Wait()
		if c1, c2 := one.calls.Load(), two.calls.Load(); c1 > ahead || c2 > ahead || c1+c2 != shared {
			t.Fatalf("fetches started %d and %d ranges before any was read, want at most %d each and %d together", c1, c2, ahead, shared)
		}
		// Read both a byte at a time, alternately, so each waits on
		// buffers the other holds.
		var got1, got2 []byte
		buf := make([]byte, 1)
		for len(got1) < len(one.data) || len(got2) < len(two.data) {
			for _, read := range []struct {
				r   *rangedReader
				got *[]byte
			}{{r1, &got1}, {r2, &got2}} {
				n, err := read.r.Read(buf)
				*read.got = append(*read.got, buf[:n]...)
				if err != nil && err != io.EOF {
					t.Fatalf("Read: %v", err)
				}
			}
			if n := buffersInUse(limits); n > shared {
				t.Fatalf("%d buffers taken, want at most %d", n, shared)
			}
		}
		if !bytes.Equal(got1, one.data) || !bytes.Equal(got2, two.data) {
			t.Fatal("concurrent fetches read the wrong bytes")
		}
	})
}

func TestRangedReaderFailures(t *testing.T) {
	const rangeSize = 4
	dropped := fmt.Errorf("%w: %w", blobstore.ErrUnavailable, io.ErrUnexpectedEOF)
	tests := []struct {
		name string
		// fetch serves the range at off of data.
		fetch func(ctx context.Context, data []byte, off, n int64) (io.ReadCloser, error)
		head  func(data []byte) []byte
		want  []error
	}{{
		// The other ranges wait until the failure cancels them, and the
		// reader reports the failure, not their cancellation.
		name: "range fails",
		fetch: func(ctx context.Context, data []byte, off, n int64) (io.ReadCloser, error) {
			if off == 3*rangeSize {
				return nil, dropped
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
		want: []error{blobstore.ErrUnavailable},
	}, {
		name: "range body fails",
		fetch: func(_ context.Context, data []byte, off, n int64) (io.ReadCloser, error) {
			return io.NopCloser(io.MultiReader(bytes.NewReader(data[off:off+1]), &errReader{err: dropped})), nil
		},
		want: []error{blobstore.ErrUnavailable},
	}, {
		name: "range ends early",
		fetch: func(_ context.Context, data []byte, off, n int64) (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(data[off : off+n-1])), nil
		},
		want: []error{blobstore.ErrUnavailable, io.ErrUnexpectedEOF},
	}, {
		name: "head ends early",
		head: func(data []byte) []byte { return data[:rangeSize-1] },
		want: []error{blobstore.ErrUnavailable, io.ErrUnexpectedEOF},
	}, {
		name: "missing blob",
		fetch: func(context.Context, []byte, int64, int64) (io.ReadCloser, error) {
			return nil, blobstore.ErrNotFound
		},
		want: []error{blobstore.ErrNotFound},
	}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				data := patterned(6 * rangeSize)
				limits := newDownloadLimits(rangeSize, 3, 1<<20)
				head := data[:rangeSize]
				if tc.head != nil {
					head = tc.head(data)
				}
				r := newRangedReader(context.Background(), limits, "blob", io.NopCloser(bytes.NewReader(head)), int64(len(data)), func(ctx context.Context, off, n int64) (io.ReadCloser, error) {
					if tc.fetch == nil {
						return io.NopCloser(bytes.NewReader(data[off : off+n])), nil
					}
					return tc.fetch(ctx, data, off, n)
				})
				_, err := io.ReadAll(r)
				for _, want := range tc.want {
					if !errors.Is(err, want) {
						t.Errorf("ReadAll = %v, want %v", err, want)
					}
				}
				if errors.Is(err, context.Canceled) || err == io.EOF {
					t.Errorf("ReadAll = %v, want the failure itself", err)
				}
				if _, again := r.Read(make([]byte, 1)); again != err {
					t.Errorf("Read after failure = %v, want %v again", again, err)
				}
				if err := r.Close(); err != nil {
					t.Fatal(err)
				}
				if n := buffersInUse(limits); n != 0 {
					t.Errorf("%d buffers still taken after Close", n)
				}
			})
		})
	}
}

func TestRangedReaderStops(t *testing.T) {
	const rangeSize = 4
	blocked := func(ctx context.Context, off, n int64) (io.ReadCloser, error) {
		<-ctx.Done()
		return nil, context.Cause(ctx)
	}
	t.Run("canceled", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			f := &rangedFixture{data: patterned(6 * rangeSize), limits: newDownloadLimits(rangeSize, 3, 1<<20), fetch: blocked}
			ctx, cancel := context.WithCancel(context.Background())
			r := f.open(ctx)
			defer r.Close()
			if _, err := io.ReadFull(r, make([]byte, rangeSize)); err != nil {
				t.Fatalf("reading the head: %v", err)
			}
			go func() {
				time.Sleep(time.Second)
				cancel()
			}()
			if _, err := r.Read(make([]byte, 1)); !errors.Is(err, context.Canceled) {
				t.Fatalf("Read after cancel = %v, want context.Canceled", err)
			}
		})
	})
	t.Run("closed", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			f := &rangedFixture{data: patterned(20 * rangeSize), limits: newDownloadLimits(rangeSize, 3, 1<<20)}
			r := f.open(context.Background())
			if _, err := io.ReadFull(r, make([]byte, 3*rangeSize+1)); err != nil {
				t.Fatal(err)
			}
			synctest.Wait()
			if err := r.Close(); err != nil {
				t.Fatal(err)
			}
			if n := buffersInUse(f.limits); n != 0 {
				t.Errorf("%d buffers still taken after Close", n)
			}
			if err := r.Close(); err != nil {
				t.Fatalf("second Close: %v", err)
			}
		})
	})
}
