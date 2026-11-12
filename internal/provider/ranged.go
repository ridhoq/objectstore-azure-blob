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
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"plugins.go.ate.dev/objectstore-azure-blob/internal/blobstore"
)

// One stream from Azure Blob reads a snapshot at about 35 MB/s, so a fetch
// reads a blob as consecutive ranges in parallel and hands them to the
// decoder in order. The ranges are the blob's bytes again before anything
// parses them, so this needs no format change. The sizes follow substrate's
// in-tree object storage (pkg/objectstorage/rangedget.go).
const (
	// downloadRangeSize is how many bytes one ranged request reads. A blob of
	// up to this many bytes is fetched with one request.
	downloadRangeSize = 16 << 20
	// downloadRangesAhead is how many ranges one fetch reads ahead of its
	// decoder, so it holds at most this many times downloadRangeSize bytes.
	downloadRangesAhead = 8
	// downloadBufferBudget bounds the range data all fetches in the process
	// hold at once.
	downloadBufferBudget = 256 << 20
)

var errFetchClosed = errors.New("fetch closed")

// downloadLimits are how a fetch reads a blob: its first rangeSize bytes from
// the request that reports the blob's size, then the rest as ranges of
// rangeSize bytes, at most ahead of them ahead of the decoder, each held in a
// buffer from buffers.
type downloadLimits struct {
	rangeSize int64
	ahead     int
	buffers   *rangeBuffers
}

// newDownloadLimits returns limits whose fetches share budget bytes of
// buffers, at least one range's worth.
func newDownloadLimits(rangeSize int64, ahead int, budget int64) downloadLimits {
	return downloadLimits{
		rangeSize: rangeSize,
		ahead:     ahead,
		buffers:   newRangeBuffers(rangeSize, max(1, int(budget/rangeSize))),
	}
}

// rangeBuffers are buffers of one range each, at most count of them in use at
// once. A buffer is allocated the first time it is needed and then kept and
// reused, so the buffers in the process never exceed count.
type rangeBuffers struct {
	size  int64
	slots chan struct{}
	free  chan *[]byte
}

func newRangeBuffers(size int64, count int) *rangeBuffers {
	return &rangeBuffers{size: size, slots: make(chan struct{}, count), free: make(chan *[]byte, count)}
}

// get waits for a free buffer.
func (b *rangeBuffers) get(ctx context.Context) (*[]byte, error) {
	select {
	case b.slots <- struct{}{}:
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	}
	select {
	case buf := <-b.free:
		return buf, nil
	default:
		buf := make([]byte, b.size)
		return &buf, nil
	}
}

// put returns a buffer from get. The free list holds a buffer for every slot,
// so it never blocks.
func (b *rangeBuffers) put(buf *[]byte) {
	b.free <- buf
	<-b.slots
}

// rangeFetcher opens count bytes of a blob from offset.
type rangeFetcher func(ctx context.Context, offset, count int64) (io.ReadCloser, error)

// download opens blobName for a fetch: one request for its first rangeSize
// bytes, which also reports its size, metadata and ETag, and, for a larger
// blob, ranged requests for the rest of that same version. The download's
// body yields exactly Size bytes or fails, and must be closed.
func (d downloadLimits) download(ctx context.Context, store blobstore.Store, blobName string) (blobstore.Download, error) {
	first, err := store.DownloadFirst(ctx, blobName, d.rangeSize)
	if err != nil {
		return blobstore.Download{}, err
	}
	if first.Size < 0 {
		// Without a size the ranges cannot be planned; read the body as is.
		return first, nil
	}
	etag := first.ETag
	fetch := func(ctx context.Context, offset, count int64) (io.ReadCloser, error) {
		return store.DownloadRange(ctx, blobName, offset, count, etag)
	}
	first.Body = newRangedReader(ctx, d, blobName, first.Body, first.Size, fetch)
	return first, nil
}

// rangedReader reads a blob of size bytes in order: the first rangeSize bytes
// from head, then the rest from ranges fetched in parallel. The first failed
// range cancels the others, and every read after it reports that failure. A
// range or head that ends early fails with ErrUnavailable, never with a bare
// EOF that a decoder would take for the blob's end.
type rangedReader struct {
	name     string
	head     io.ReadCloser
	headLen  int64
	headLeft int64
	// left is how many bytes of the blob Read has yet to return.
	left int64

	ctx     context.Context
	cancel  context.CancelCauseFunc
	ahead   chan struct{}
	buffers *rangeBuffers
	// ranges carries the fetched ranges in order. The scheduler closes it
	// once it has started every range or stopped.
	ranges  chan *fetchedRange
	workers sync.WaitGroup
	stats   fetchStats
	statsMu sync.Mutex

	cur    *fetchedRange
	err    error
	closed bool
}

// fetchStats describes how a rangedReader read its blob.
type fetchStats struct {
	// ranges is how many ranges it requested after the first.
	ranges int
	// bufferWait is how long it waited for buffers that other fetches held.
	bufferWait time.Duration
	// slowestRange is the longest any one range took, from its request to
	// its last byte.
	slowestRange time.Duration
}

// fetchedRange is one range of the blob. data and err are set when done is
// closed.
type fetchedRange struct {
	buf  *[]byte
	data []byte
	err  error
	done chan struct{}
}

func newRangedReader(ctx context.Context, limits downloadLimits, name string, head io.ReadCloser, size int64, fetch rangeFetcher) *rangedReader {
	ctx, cancel := context.WithCancelCause(ctx)
	headLen := min(size, limits.rangeSize)
	r := &rangedReader{
		name:     name,
		head:     head,
		headLen:  headLen,
		headLeft: headLen,
		left:     size,
		ctx:      ctx,
		cancel:   cancel,
		ahead:    make(chan struct{}, limits.ahead),
		buffers:  limits.buffers,
		ranges:   make(chan *fetchedRange, limits.ahead),
	}
	r.workers.Add(1)
	go r.schedule(fetch, headLen, size, limits.rangeSize)
	return r
}

// schedule starts the ranges after the head in order. Each range takes a
// place ahead of the decoder and then a buffer before its request is sent,
// and gives both back once Read has returned its bytes. Ranges take buffers
// in order, so a fetch always holds the buffer of the next range its decoder
// needs, and fetches sharing buffers cannot wait on each other forever.
func (r *rangedReader) schedule(fetch rangeFetcher, start, size, rangeSize int64) {
	defer r.workers.Done()
	defer close(r.ranges)
	for off := start; off < size; off += rangeSize {
		select {
		case r.ahead <- struct{}{}:
		case <-r.ctx.Done():
			return
		}
		waitStart := time.Now()
		buf, err := r.buffers.get(r.ctx)
		r.statsMu.Lock()
		r.stats.bufferWait += time.Since(waitStart)
		r.statsMu.Unlock()
		if err != nil {
			<-r.ahead
			return
		}
		fr := &fetchedRange{buf: buf, data: (*buf)[:min(rangeSize, size-off)], done: make(chan struct{})}
		r.ranges <- fr
		r.workers.Add(1)
		go func(off int64) {
			defer r.workers.Done()
			defer close(fr.done)
			start := time.Now()
			err := r.fetchRange(fetch, off, fr.data)
			r.statsMu.Lock()
			r.stats.ranges++
			r.stats.slowestRange = max(r.stats.slowestRange, time.Since(start))
			r.statsMu.Unlock()
			if err != nil {
				fr.err = err
				r.cancel(err)
			}
		}(off)
	}
}

func (r *rangedReader) fetchRange(fetch rangeFetcher, off int64, data []byte) error {
	body, err := fetch(r.ctx, off, int64(len(data)))
	if err != nil {
		return err
	}
	n, err := io.ReadFull(body, data)
	closeErr := body.Close()
	// Only an unwrapped EOF means the response ended; the Azure body reports
	// a dropped connection as an error wrapping one, which keeps its code.
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return r.short(off, int64(len(data)), int64(n))
	}
	if err != nil {
		return err
	}
	return closeErr
}

// short reports a range that ended after n of its want bytes. The blob's size
// is fixed by its ETag, so a short response is a failed transfer, not the
// blob's end.
func (r *rangedReader) short(off, want, n int64) error {
	return fmt.Errorf("%w: blob %q ended %d bytes into the %d byte range at %d: %w", blobstore.ErrUnavailable, r.name, n, want, off, io.ErrUnexpectedEOF)
}

func (r *rangedReader) Read(p []byte) (int, error) {
	if r.err != nil {
		return 0, r.err
	}
	if len(p) == 0 {
		return 0, nil
	}
	if r.headLeft > 0 {
		n, err := r.head.Read(p[:min(int64(len(p)), r.headLeft)])
		r.headLeft -= int64(n)
		r.left -= int64(n)
		if err == io.EOF {
			err = nil
			if r.headLeft > 0 {
				err = r.short(0, r.headLen, r.headLen-r.headLeft)
			}
		}
		if err != nil {
			r.err = err
		}
		return n, err
	}
	for r.cur == nil {
		if r.left == 0 {
			r.err = io.EOF
			return 0, io.EOF
		}
		next, ok := <-r.ranges
		if !ok {
			// The scheduler stopped before the last range, so the reader was
			// canceled.
			r.err = context.Cause(r.ctx)
			if r.err == nil {
				r.err = fmt.Errorf("fetch of blob %q stopped %d bytes before its end", r.name, r.left)
			}
			return 0, r.err
		}
		<-next.done
		if next.err != nil {
			r.release(next)
			// A failed range cancels the others; report the first failure,
			// not the cancellation it caused in a later range.
			r.err = context.Cause(r.ctx)
			return 0, r.err
		}
		r.cur = next
	}
	n := copy(p, r.cur.data)
	r.cur.data = r.cur.data[n:]
	r.left -= int64(n)
	if len(r.cur.data) == 0 {
		r.release(r.cur)
		r.cur = nil
	}
	return n, nil
}

func (r *rangedReader) release(fr *fetchedRange) {
	r.buffers.put(fr.buf)
	<-r.ahead
}

// fetchStats returns how the reader read its blob so far.
func (r *rangedReader) fetchStats() fetchStats {
	r.statsMu.Lock()
	defer r.statsMu.Unlock()
	return r.stats
}

// Close stops the fetch and waits for its ranges, returning their buffers.
func (r *rangedReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	r.cancel(errFetchClosed)
	err := r.head.Close()
	r.workers.Wait()
	if r.cur != nil {
		r.release(r.cur)
		r.cur = nil
	}
	for fr := range r.ranges {
		r.release(fr)
	}
	return err
}
