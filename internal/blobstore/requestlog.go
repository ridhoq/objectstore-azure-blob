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

package blobstore

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptrace"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
)

// defaultSlowRequest is how long one attempt of a storage request may take,
// from sending it to the last byte of its response, before it is logged.
const defaultSlowRequest = time.Second

const clientRequestIDHeader = "x-ms-client-request-id"

type operationKey struct{}

// operation is one SDK call, which the SDK's retry policy may send more than
// once.
type operation struct {
	id       string
	attempts atomic.Int32
}

// operationPolicy runs once per SDK call, before its retries. It names the
// call with an x-ms-client-request-id, which every attempt sends and Azure's
// storage logs record, so the attempts of one call can be matched to each
// other and to the service's view of them.
type operationPolicy struct{}

func (operationPolicy) Do(req *policy.Request) (*http.Response, error) {
	id := req.Raw().Header.Get(clientRequestIDHeader)
	if id == "" {
		id = newOperationID()
		req.Raw().Header.Set(clientRequestIDHeader, id)
	}
	op := &operation{id: id}
	return req.WithContext(context.WithValue(req.Raw().Context(), operationKey{}, op)).Next()
}

func newOperationID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// attemptLog records how one attempt of a request spent its time, and logs it
// once the attempt ends if it failed, was a retry, or took at least
// threshold.
type attemptLog struct {
	req       *http.Request
	op        *operation
	attempt   int32
	threshold time.Duration
	start     time.Time

	mu         sync.Mutex
	dnsStart   time.Time
	dns        time.Duration
	dialStart  time.Time
	connect    time.Duration
	tlsStart   time.Time
	tls        time.Duration
	getConn    time.Time
	waitConn   time.Duration
	reused     bool
	firstByte  time.Duration
	readTime   time.Duration
	bytes      int64
	logged     atomic.Bool
	statusCode int
	requestID  string
}

func newAttemptLog(req *http.Request, threshold time.Duration) *attemptLog {
	l := &attemptLog{req: req, threshold: threshold, start: time.Now()}
	if op, ok := req.Context().Value(operationKey{}).(*operation); ok {
		l.op = op
		l.attempt = op.attempts.Add(1)
	}
	return l
}

// trace returns the hooks that time the attempt's connection setup.
func (l *attemptLog) trace() *httptrace.ClientTrace {
	since := func(t time.Time) time.Duration {
		if t.IsZero() {
			return 0
		}
		return time.Since(t)
	}
	locked := func(f func()) {
		l.mu.Lock()
		defer l.mu.Unlock()
		f()
	}
	return &httptrace.ClientTrace{
		GetConn: func(string) { locked(func() { l.getConn = time.Now() }) },
		GotConn: func(info httptrace.GotConnInfo) {
			locked(func() { l.waitConn, l.reused = since(l.getConn), info.Reused })
		},
		DNSStart: func(httptrace.DNSStartInfo) { locked(func() { l.dnsStart = time.Now() }) },
		DNSDone:  func(httptrace.DNSDoneInfo) { locked(func() { l.dns = since(l.dnsStart) }) },
		ConnectStart: func(string, string) {
			locked(func() {
				if l.dialStart.IsZero() {
					l.dialStart = time.Now()
				}
			})
		},
		ConnectDone:          func(string, string, error) { locked(func() { l.connect = since(l.dialStart) }) },
		TLSHandshakeStart:    func() { locked(func() { l.tlsStart = time.Now() }) },
		TLSHandshakeDone:     func(tls.ConnectionState, error) { locked(func() { l.tls = since(l.tlsStart) }) },
		GotFirstResponseByte: func() { locked(func() { l.firstByte = time.Since(l.start) }) },
	}
}

func (l *attemptLog) responded(resp *http.Response) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.statusCode = resp.StatusCode
	l.requestID = resp.Header.Get("x-ms-request-id")
}

// end decides, the first time it is called, whether the attempt is worth
// logging, and logs it if so.
func (l *attemptLog) end(err error) {
	if l.logged.Swap(true) {
		return
	}
	took := time.Since(l.start)
	l.mu.Lock()
	defer l.mu.Unlock()
	failed := err != nil || retryableStatus(l.statusCode)
	if !failed && l.attempt <= 1 && took < l.threshold {
		return
	}
	attrs := []slog.Attr{
		slog.String("method", l.req.Method),
		slog.String("path", l.req.URL.Path),
		slog.String("range", l.req.Header.Get("x-ms-range")),
		slog.Int("status", l.statusCode),
		slog.String("requestID", l.requestID),
		slog.Duration("duration", took),
		slog.Duration("waitConn", l.waitConn),
		slog.Bool("connReused", l.reused),
		slog.Duration("dns", l.dns),
		slog.Duration("connect", l.connect),
		slog.Duration("tls", l.tls),
		slog.Duration("firstByte", l.firstByte),
		slog.Duration("bodyRead", l.readTime),
		slog.Int64("bodyBytes", l.bytes),
		slog.Any("err", err),
	}
	if l.op != nil {
		attrs = append(attrs, slog.String("clientRequestID", l.op.id), slog.Int("attempt", int(l.attempt)))
	}
	level := slog.LevelInfo
	if failed {
		level = slog.LevelWarn
	}
	slog.LogAttrs(l.req.Context(), level, "Slow, retried or failed Azure Blob request", attrs...)
}

// retryableStatus reports whether a response status means the request failed
// in a way the SDK or the caller retries: throttling, timeouts, server errors,
// and a blob that changed under an If-Match. Other client errors, such as a
// missing blob, are answers rather than failures.
func retryableStatus(code int) bool {
	return code == http.StatusRequestTimeout || code == http.StatusTooManyRequests ||
		code == http.StatusPreconditionFailed || code >= http.StatusInternalServerError
}

// loggedBody times the reads of a response body and ends its attempt's log at
// the body's end, first failure or close.
type loggedBody struct {
	io.ReadCloser
	log *attemptLog
}

func (b *loggedBody) Read(p []byte) (int, error) {
	start := time.Now()
	n, err := b.ReadCloser.Read(p)
	b.log.mu.Lock()
	b.log.readTime += time.Since(start)
	b.log.bytes += int64(n)
	b.log.mu.Unlock()
	switch {
	case err == io.EOF:
		b.log.end(nil)
	case err != nil:
		b.log.end(err)
	}
	return n, err
}

func (b *loggedBody) Close() error {
	err := b.ReadCloser.Close()
	b.log.end(nil)
	return err
}

// logFailedRead returns a RetryReaderOptions.OnFailedRead that logs each
// failed read of a download body, which the SDK resumes with a new request.
func logFailedRead(ctx context.Context, name string) func(int32, error, blob.HTTPRange, bool) {
	return func(failures int32, err error, rng blob.HTTPRange, willRetry bool) {
		slog.WarnContext(ctx, "Azure Blob download body read failed",
			slog.String("blob", name),
			slog.Int64("offset", rng.Offset),
			slog.Int64("count", rng.Count),
			slog.Int("failures", int(failures)),
			slog.Bool("willResume", willRetry),
			slog.Any("err", err))
	}
}
