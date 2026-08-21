// Copyright 2026 PingCAP, Inc.
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

package importer

import (
	"context"
	"io"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/pkg/objstore/objectio"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
)

// ReaderTimings accumulates time spent reading source bytes and decoding a
// compressed source stream. Its zero value is ready for an uncompressed source.
// The counters are atomic because a parser may read on a worker goroutine.
type ReaderTimings struct {
	sourceRead  atomic.Int64
	decodedRead atomic.Int64
	compressed  bool
}

type readPhaseDurations struct {
	sourceRead time.Duration
	decompress time.Duration
}

func newReaderTimings(compressed bool) *ReaderTimings {
	return &ReaderTimings{compressed: compressed}
}

func (t *ReaderTimings) addSourceRead(d time.Duration) {
	t.sourceRead.Add(int64(d))
}

func (t *ReaderTimings) addDecodedRead(d time.Duration) {
	t.decodedRead.Add(int64(d))
}

func (t *ReaderTimings) take() readPhaseDurations {
	sourceRead := time.Duration(t.sourceRead.Swap(0))
	if !t.compressed {
		return readPhaseDurations{sourceRead: sourceRead}
	}

	decodedRead := time.Duration(t.decodedRead.Swap(0))
	decompress := decodedRead - sourceRead
	if decompress < 0 {
		// Each decoded read normally encloses its source reads. Clamp defensively
		// in case an asynchronous decoder completes the nested counters later.
		decompress = 0
	}
	return readPhaseDurations{
		sourceRead: sourceRead,
		decompress: decompress,
	}
}

func (t *ReaderTimings) reset() {
	t.sourceRead.Store(0)
	t.decodedRead.Store(0)
}

func (t *ReaderTimings) wrapDecodedReader(reader io.ReadSeekCloser) io.ReadSeekCloser {
	if !t.compressed {
		return reader
	}
	return &timedReadSeekCloser{
		ReadSeekCloser: reader,
		add:            t.addDecodedRead,
	}
}

type readTimingStorage struct {
	storeapi.Storage
	timings *ReaderTimings
}

func (s *readTimingStorage) Open(
	ctx context.Context,
	path string,
	option *storeapi.ReaderOption,
) (objectio.Reader, error) {
	reader, err := s.Storage.Open(ctx, path, option)
	if err != nil {
		return nil, err
	}
	return &timedObjectReader{
		Reader: reader,
		add:    s.timings.addSourceRead,
	}, nil
}

type timedObjectReader struct {
	objectio.Reader
	add func(time.Duration)
}

func (r *timedObjectReader) Read(p []byte) (int, error) {
	start := time.Now()
	n, err := r.Reader.Read(p)
	r.add(time.Since(start))
	return n, err
}

type timedReadSeekCloser struct {
	io.ReadSeekCloser
	add func(time.Duration)
}

func (r *timedReadSeekCloser) Read(p []byte) (int, error) {
	start := time.Now()
	n, err := r.ReadSeekCloser.Read(p)
	r.add(time.Since(start))
	return n, err
}
