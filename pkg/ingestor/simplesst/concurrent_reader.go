// Copyright 2023 PingCAP, Inc.
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

package simplesst

import (
	"context"
	"io"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"golang.org/x/sync/errgroup"
)

// concurrentFileReader reads a file with multiple chunks concurrently.
type concurrentFileReader struct {
	ctx            context.Context
	cancel         context.CancelFunc
	concurrency    int
	readBufferSize int

	storage storeapi.Storage
	name    string

	offset   int64
	fileSize int64

	singleWindow bool
	bufferSets   [2][][]byte
	started      bool
	resultCh     chan concurrentReadResult
	wg           sync.WaitGroup
}

type concurrentReadResult struct {
	bufferSet int
	buffers   [][]byte
	err       error
}

// newConcurrentFileReader creates a new concurrentFileReader.
func newConcurrentFileReader(
	ctx context.Context,
	st storeapi.Storage,
	name string,
	offset int64,
	fileSize int64,
	concurrency int,
	readBufferSize int,
	singleWindow bool,
) (*concurrentFileReader, error) {
	childCtx, cancel := context.WithCancel(ctx)
	return &concurrentFileReader{
		singleWindow:   singleWindow,
		ctx:            childCtx,
		cancel:         cancel,
		concurrency:    concurrency,
		readBufferSize: readBufferSize,
		offset:         offset,
		fileSize:       fileSize,
		name:           name,
		storage:        st,
		resultCh:       make(chan concurrentReadResult, 1),
	}, nil
}

// read returns the next in-order buffer window. Unless the reader was built for a
// single window, it fills the other one concurrently while the caller consumes the
// returned one.
func (r *concurrentFileReader) read(bufs [][]byte) ([][]byte, error) {
	if r.singleWindow {
		if len(bufs) < r.concurrency {
			return nil, errors.Errorf(
				"concurrent reader needs %d buffers, got %d",
				r.concurrency,
				len(bufs),
			)
		}
		return r.readOnce(bufs[:r.concurrency])
	}

	if len(bufs) < 2*r.concurrency {
		return nil, errors.Errorf(
			"concurrent reader needs %d buffers, got %d",
			2*r.concurrency,
			len(bufs),
		)
	}

	if !r.started {
		r.bufferSets[0] = bufs[:r.concurrency]
		r.bufferSets[1] = bufs[r.concurrency : 2*r.concurrency]
		r.started = true

		r.startRead(0)
		result := <-r.resultCh
		if result.err != nil {
			return nil, result.err
		}
		r.startRead(1)
		return result.buffers, nil
	}

	result := <-r.resultCh
	if result.err != nil {
		return nil, result.err
	}
	r.startRead(1 - result.bufferSet)
	return result.buffers, nil
}

func (r *concurrentFileReader) startRead(bufferSet int) {
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		buffers, err := r.readOnce(r.bufferSets[bufferSet])
		r.resultCh <- concurrentReadResult{
			bufferSet: bufferSet,
			buffers:   buffers,
			err:       err,
		}
	}()
}

func (r *concurrentFileReader) readOnce(bufs [][]byte) ([][]byte, error) {
	if r.offset >= r.fileSize {
		return nil, io.EOF
	}

	ret := make([][]byte, 0, r.concurrency)
	eg, egCtx := errgroup.WithContext(r.ctx)
	for i := range r.concurrency {
		if r.offset >= r.fileSize {
			break
		}
		end := r.readBufferSize
		if r.offset+int64(end) > r.fileSize {
			end = int(r.fileSize - r.offset)
		}
		buf := bufs[i][:end]
		ret = append(ret, buf)
		offset := r.offset
		r.offset += int64(end)
		eg.Go(func() error {
			_, err := objstore.ReadDataInRange(
				egCtx,
				r.storage,
				r.name,
				offset,
				buf,
			)
			if err != nil {
				return errors.Annotatef(err, "offset: %d, readSize: %d", offset, len(buf))
			}
			return nil
		})
	}
	err := eg.Wait()
	if err != nil {
		return nil, err
	}

	return ret, nil
}

func (r *concurrentFileReader) close() {
	r.cancel()
	r.wg.Wait()
}
