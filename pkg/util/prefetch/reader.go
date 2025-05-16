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
	ret.logger.Info("New prefetch reader")
	go ret.run()
	return ret
}

func (r *Reader) run() {
	var loopCnt uint64
	defer r.wg.Done()
	r.logger.Info("start run()")
	defer func() {
		r.logger.Info("end run()")
	}()
	for {
		if rand.Intn(10) == 0 {
			r.logger.Info("run loop", zap.Int("loopCnt", int(loopCnt)))
		}
		r.bufIdx = (r.bufIdx + 1) % 2
		buf := r.buf[r.bufIdx]
		n, err := r.r.Read(buf) // No EOF
		if n == 0 || err != nil {
			r.logger.Error("After abnormal read in run()", zap.Int("n", n), zap.Error(err)) // n=0, failed to read file xxx: illegal n when reading from external storage / http: read on closed response body / read tcp 10.200.12.194:37046->198.18.96.40:80: use of closed network connection / read N bytes from external storage, exceed max limit
		} else if rand.Intn(10) == 0 {
			r.logger.Info("After normal read in run()", zap.Int("n", n))
		}
		buf = buf[:n] // TODO: skip this if n == 0
		select {
		case <-r.closedCh:
			return
		case r.bufCh <- buf:
		}
		if err != nil { // why handle the error after select clause?
			//logutil.BgLogger().Error("read error", zap.Error(err))
			r.logger.Error("run encounter error", zap.Int("n", n), zap.Error(err)) // n=0, failed to read file xxx: illegal n when reading from external storage
			r.err = err
			close(r.bufCh)
			return
		}
		loopCnt += 1
	}
}

// Read implements io.Reader. Read should not be called concurrently with Close.
func (r *Reader) Read(data []byte) (int, error) {
	total := 0
	var loopCnt uint64
	r.logger.Info("start Read()", zap.Int("len(data)", len(data)))
	defer func() {
		r.logger.Info("end Read()")
	}()
	for {
		if rand.Intn(10) == 0 {
			r.logger.Info("Read loop", zap.Int("loopCnt", int(loopCnt)))
		}
		if r.curBufReader == nil {
			b, ok := <-r.bufCh
			if !ok || len(b) == 0 {
				if total > 0 {
					//PrintLog = true
					//logutil.BgLogger().Error("set printlog = true", zap.Error(r.err), zap.Int("total", total), zap.Any("data-buf-len", len(data)))
					r.logger.Info("Read() return", zap.Int("total", total)) // total > 0
					return total, nil
				}
				r.logger.Info("Read() return", zap.Error(r.err)) // failed to read file 1/1/data/923c8a66-5667-477a-b735-d44b2e4f9d91/19: illegal n when reading from external storage
				return 0, r.err
			}

			r.curBufReader = bytes.NewReader(b) // Clue1: len(b) == 0 ?
			r.logger.Info(fmt.Sprintf("renew curBufReader %p", r.curBufReader))
		}

		expected := len(data)
		n, err := r.curBufReader.Read(data)
		if n == 0 || err != nil {
			r.logger.Error("After abnormal read in Read()", zap.Int("n", n), zap.Error(err)) // (0, io.EOF)
		}
		total += n
		if n == expected {
			r.logger.Info("Read() return", zap.Int("total", total), zap.Int("n", n)) // total > 0, n > 0
			return total, nil
		}

		data = data[n:]
		if rand.Intn(10) == 0 {
			r.logger.Info("normal process", zap.Int("n", n))
		}
		if err == io.EOF || r.curBufReader.Len() == 0 {
			r.curBufReader = nil
			r.logger.Info("reset curBufReader")
		}
		loopCnt += 1
	}
}

// Close implements io.Closer. Close should not be called concurrently with Read.
func (r *Reader) Close() error {
	if r.closed {
		r.logger.Info("Close closed reader")
		return nil
	}
	r.logger.Info("Close open reader start")
	ret := r.r.Close()
	close(r.closedCh)
	r.wg.Wait()
	r.closed = true
	r.logger.Info("Close open reader done")
	return ret
}
