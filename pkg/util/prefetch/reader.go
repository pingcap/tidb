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
	"fmt"
	"io"
	"math/rand"
	"sync"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

var PrintLog = false

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

	logger *zap.Logger
}

// NewReader creates a new Reader.
func NewReader(r io.ReadCloser, prefetchSize int) io.ReadCloser {
	ret := &Reader{
		r:        r,
		bufCh:    make(chan []byte),
		err:      nil,
		closedCh: make(chan struct{}),
	}
	ret.logger = log.L().With(zap.String("prefetch reader id", fmt.Sprintf("%p", ret)))
	ret.buf[0] = make([]byte, prefetchSize/2)
	ret.buf[1] = make([]byte, prefetchSize/2)
	ret.wg.Add(1)
	go ret.run()
	return ret
}

func (r *Reader) run() {
	defer r.wg.Done()
	r.logger.Info("start run()")
	defer func() {
		r.logger.Info("end run()")
	}()
	for {
		r.bufIdx = (r.bufIdx + 1) % 2
		buf := r.buf[r.bufIdx]
		n, err := r.r.Read(buf) // Clue1: may return (0, EOF) ?
		if n == 0 || err != nil {
			r.logger.Error("After abnormal read in run()", zap.Int("n", n), zap.Error(err))
		} else if rand.Intn(10) == 0 {
			r.logger.Info("After normal read in run()", zap.Int("n", n))
		}
		buf = buf[:n]
		select {
		case <-r.closedCh:
			return
		case r.bufCh <- buf:
		}
		if err != nil { // why handle the error after select clause?
			//logutil.BgLogger().Error("read error", zap.Error(err))
			r.logger.Error("run encounter error", zap.Int("n", n), zap.Error(err))
			r.err = err
			close(r.bufCh)
			return
		}
	}
}

// Read implements io.Reader. Read should not be called concurrently with Close.
func (r *Reader) Read(data []byte) (int, error) {
	total := 0
	r.logger.Info("start Read()", zap.Int("len(data)", len(data)))
	defer func() {
		r.logger.Info("end Read()")
	}()
	for {
		if r.curBufReader == nil {
			b, ok := <-r.bufCh
			if !ok {
				if total > 0 {
					//PrintLog = true
					//logutil.BgLogger().Error("set printlog = true", zap.Error(r.err), zap.Int("total", total), zap.Any("data-buf-len", len(data)))
					return total, nil
				}
				return 0, r.err
			}

			r.curBufReader = bytes.NewReader(b) // Clue1: len(b) == 0 ?
			r.logger.Info(fmt.Sprintf("reset curBufReader %p", r.curBufReader))
		}

		expected := len(data)
		n, err := r.curBufReader.Read(data) // Clue1: return 0, io.EOF ?
		if n == 0 || err != nil {
			r.logger.Error("After abnormal read in Read()", zap.Int("n", n), zap.Error(err))
		}
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
	r.logger.Info("Close reader")
	if r.closed {
		return nil
	}
	ret := r.r.Close()
	close(r.closedCh)
	r.wg.Wait()
	r.closed = true
	return ret
}
