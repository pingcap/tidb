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

package chunk

import (
	"context"
	"runtime"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/util/logutil"
)

// RowContainerReader is a forward-only iterator for the row container. It provides an interface similar to other
// iterators, but it doesn't provide `ReachEnd` function and requires manually closing to release goroutine.
//
// It's recommended to use the following pattern to use it:
//
//	for iter := NewRowContainerReader(rc); iter.Current() != iter.End(); iter.Next() {
//		...
//	}
//	iter.Close()
//	if iter.Error() != nil {
//	}
type RowContainerReader interface {
	// Next returns the next Row.
	Next() Row

	// Current returns the current Row.
	Current() Row

	// End returns the invalid end Row.
	End() Row

	// Error returns none-nil error if anything wrong happens during the iteration.
	Error() error

	// Close closes the dumper
	Close()
}

var _ RowContainerReader = &rowContainerReader{}

// rowContainerReader is a forward-only iterator for the row container
// It will spawn two goroutines for reading chunks from disk, and converting the chunk to rows. The row will only be sent
// to `rowCh` inside only after when the full chunk has been read, to avoid concurrently read/write to the chunk.
//
// TODO: record the memory allocated for the channel and chunks.
type rowContainerReader struct {
	// context, cancel and waitgroup are used to stop and wait until all goroutine stops.
	ctx    context.Context
	cancel func()
	wg     sync.WaitGroup

	rc *RowContainer

	currentRow Row
	rowCh      chan Row

	// this error will only be set by worker
	err error
}

// Next implements RowContainerReader
func (reader *rowContainerReader) Next() Row {
	for row := range reader.rowCh {
		reader.currentRow = row
		return row
	}
	reader.currentRow = reader.End()
	return reader.End()
}

// Current implements RowContainerReader
func (reader *rowContainerReader) Current() Row {
	return reader.currentRow
}

// End implements RowContainerReader
func (*rowContainerReader) End() Row {
	return Row{}
}

// Error implements RowContainerReader
func (reader *rowContainerReader) Error() error {
	return reader.err
}

func (reader *rowContainerReader) initializeChannel() {
	if reader.rc.NumChunks() == 0 {
		reader.rowCh = make(chan Row, 1024)
	} else {
		assumeChunkSize := reader.rc.NumRowsOfChunk(0)
		// To avoid blocking in sending to `rowCh` and  don't start reading the next chunk, it'd be better to give it
		// a buffer at least larger than a single chunk. Here it's allocated twice the chunk size to leave some margin.
		reader.rowCh = make(chan Row, 2*assumeChunkSize)
	}
}

// Close implements RowContainerReader
func (reader *rowContainerReader) Close() {
	reader.cancel()
	reader.wg.Wait()
}

func (reader *rowContainerReader) startWorker() {
	reader.wg.Add(1)
	go func() {
		defer close(reader.rowCh)
		defer reader.wg.Done()

		for chkIdx := 0; chkIdx < reader.rc.NumChunks(); chkIdx++ {
			chk, err := reader.rc.GetChunk(chkIdx)
			failpoint.Inject("get-chunk-error", func(val failpoint.Value) {
				if val.(bool) {
					err = errors.New("fail to get chunk for test")
				}
			})
			if err != nil {
				reader.err = err
				return
			}

			for i := 0; i < chk.NumRows(); i++ {
				select {
				case reader.rowCh <- chk.GetRow(i):
				case <-reader.ctx.Done():
					return
				}
			}
		}
	}()
}

// NewRowContainerReader creates a forward only iterator for row container
func NewRowContainerReader(rc *RowContainer) *rowContainerReader {
	ctx, cancel := context.WithCancel(context.Background())

	reader := &rowContainerReader{
		ctx:    ctx,
		cancel: cancel,
		wg:     sync.WaitGroup{},

		rc: rc,
	}
	reader.initializeChannel()
	reader.startWorker()
	reader.Next()
	runtime.SetFinalizer(reader, func(reader *rowContainerReader) {
		if reader.ctx.Err() == nil {
			logutil.BgLogger().Warn("rowContainerReader is closed by finalizer")
			reader.Close()
		}
	})

	return reader
}
