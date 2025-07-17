// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package prefetch

import (
	"bytes"
	"errors"
	"io"
	"sync"
)

// Reader is a reader that prefetches data from the underlying reader.
type Reader struct {
	r            io.ReadCloser
	curBufReader *bytes.Reader
	buf          [2][]byte
	bufIdx       int
	bufCh        chan []byte
	err          error // after bufCh is closed
	wg           sync.WaitGroup

	closed   bool
	closedCh chan struct{}
	// rangeSize is the size of the range of data that the reader is reading.
	rangeSize int64
}

// NewReader creates a new Reader.
func NewReader(r io.ReadCloser, rangeSize int64, prefetchSize int) io.ReadCloser {
	ret := &Reader{
		r:         r,
		bufCh:     make(chan []byte),
		err:       nil,
		closedCh:  make(chan struct{}),
		rangeSize: rangeSize,
	}
	ret.buf[0] = make([]byte, prefetchSize/2)
	ret.buf[1] = make([]byte, prefetchSize/2)
	ret.wg.Add(1)
	go ret.run()
	return ret
}

func (r *Reader) run() {
	defer r.wg.Done()
	var readSize int64
	for {
		r.bufIdx = (r.bufIdx + 1) % 2
		buf := r.buf[r.bufIdx]
		n, err := io.ReadFull(r.r, buf)
		buf = buf[:n]
		readSize += int64(n)
		select {
		case <-r.closedCh:
			return
		case r.bufCh <- buf:
		}
		if err != nil {
			if errors.Is(err, io.ErrUnexpectedEOF) && readSize == r.rangeSize {
				// this is caused by io.ReadFull. Because we are prefetching, the
				// buffer size may be larger that caller's need. So convert to io.EOF.
				// Note: the reader r.r.Read may also return io.ErrUnexpectedEOF,
				// we need skip this case.
				err = io.EOF
			}
			r.err = err
			close(r.bufCh)
			return
		}
	}
}

// Read implements io.Reader. Read should not be called concurrently with Close.
func (r *Reader) Read(data []byte) (int, error) {
	total := 0
	for {
		if r.curBufReader == nil {
			b, ok := <-r.bufCh
			if !ok {
				if total > 0 {
					return total, nil
				}
				return 0, r.err
			}

			r.curBufReader = bytes.NewReader(b)
		}

		expected := len(data)
		n, err := r.curBufReader.Read(data)
		total += n
		if n == expected {
			return total, nil
		}

		data = data[n:]
		if err == io.EOF || r.curBufReader.Len() == 0 {
			r.curBufReader = nil
			continue
		}
	}
}

// Close implements io.Closer. Close should not be called concurrently with Read.
func (r *Reader) Close() error {
	if r.closed {
		return nil
	}
	ret := r.r.Close()
	close(r.closedCh)
	r.wg.Wait()
	r.closed = true
	return ret
}
