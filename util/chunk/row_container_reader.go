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
	"bufio"
	"context"
	"runtime"
	"sync"

	"github.com/pingcap/tidb/util/logutil"
)

// RowContainerReader is a forward-only iterator for the row container.
// It's recommended to use the following pattern to use it:
//
//	for iter := NewRowContainerReader(rc); iter.Current() != iter.End(); iter.Next() {
//	}
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
// This implementation is much faster than the `iterator4RowContainer`, but provide less methods. Be careful, unlike the
// `iterator4RowContainer`, the `rowContainerReader` needs to be closed manually, to avoid goroutine leak.
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
	reader.rc.m.RLock()
	defer reader.rc.m.RUnlock()

	if !reader.rc.alreadySpilled() {
		// just any long enough channel should be fine. But in case the row container is spilled in the future,
		// set the length to be the default maxChunks size.
		reader.rowCh = make(chan Row, 1024)
	} else if reader.rc.NumChunks() == 0 {
		reader.rowCh = make(chan Row, 1024)
	} else {
		// TODO: give a more reasonable assumption of chunksize
		assumeChunkSize := reader.rc.m.records.inDisk.numRowsOfEachChunk[0]
		// To avoid blocking in sending to `rowCh` and  don't start reading the next chunk, it'd be better to give it
		// a buffer with double size of chunk.
		reader.rowCh = make(chan Row, 2*assumeChunkSize)
	}
}

func (reader *rowContainerReader) dumpChunk(chkIdx int) {
	reader.rc.m.RLock()
	defer reader.rc.m.RUnlock()

	if !reader.rc.alreadySpilled() {
		for rowIdx := 0; rowIdx < reader.rc.NumRowsOfChunk(chkIdx); rowIdx++ {
			row, err := reader.rc.GetRow(RowPtr{uint32(chkIdx), uint32(rowIdx)})
			if err != nil {
				reader.err = err
				break
			}

			sendToChanWithoutLock(reader, reader.rowCh, row)
		}
	} else {
		reader.dumpDiskChunk(chkIdx)
	}
}

// sendToChanWithoutLock sends the `item` to `ch`, and around it with the `reader.rc.m.R(Unl|L)ock`.
// it returns true when the item is sent to the channel successfully. It returns false when the context is canceled while
// sending the item.
// The `reader.rc.m` should has been `RLocked` when calling this function
func sendToChanWithoutLock[T any](reader *rowContainerReader, ch chan<- T, item T) bool {
	reader.rc.m.RUnlock()
	defer reader.rc.m.RLock()
	select {
	case ch <- item:
		return true
	case <-reader.ctx.Done():
		return false
	}
}

func (reader *rowContainerReader) dumpDiskChunk(chkIdx int) {
	l := reader.rc.m.records.inDisk

	// This implementation assumes that all rows in a chunk is stored together tightly.
	chk := NewChunkWithCapacity(l.fieldTypes, l.NumRowsOfChunk(chkIdx))
	chkSize := l.numRowsOfEachChunk[chkIdx]

	firstRowOffset, err := l.getOffset(uint32(chkIdx), 0)
	if err != nil {
		reader.err = err
		return
	}

	formatCh := make(chan rowInDisk, chkSize)
	reader.wg.Add(1)
	go func() {
		defer close(formatCh)
		defer reader.wg.Done()

		reader.rc.m.RLock()
		defer reader.rc.m.RUnlock()

		// If the row is small, a bufio can significantly improve the performance. As benchmark shows, it's still not bad
		// for longer rows.
		r := bufio.NewReader(l.dataFile.getSectionReader(firstRowOffset))
		format := rowInDisk{numCol: len(l.fieldTypes)}
		for rowIdx := 0; rowIdx < chkSize; rowIdx++ {
			_, err = format.ReadFrom(r)
			if err != nil {
				// it's safe to set error here. Nothing else will modify the error concurrently.
				// once this loop breaks, the outer one will also stop after draining the channel
				reader.err = err
				break
			}

			sendToChanWithoutLock(reader, formatCh, format)
		}
	}()

	rows := make([]Row, 0, chkSize)
	for format := range formatCh {
		var row Row
		row, chk = format.toRow(l.fieldTypes, chk)
		rows = append(rows, row)
	}

	for _, row := range rows {
		sendToChanWithoutLock(reader, reader.rowCh, row)
	}
}

func (reader *rowContainerReader) Close() {
	reader.cancel()
	reader.wg.Wait()
}

func (reader *rowContainerReader) startWorker() {
	reader.wg.Add(1)
	go func() {
		defer reader.wg.Done()
		for chkIdx := 0; chkIdx < reader.rc.NumChunks(); chkIdx++ {
			reader.dumpChunk(chkIdx)
		}

		close(reader.rowCh)
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
		logutil.BgLogger().Warn("rowContainerReader is closed by finalizer")
		reader.Close()
	})

	return reader
}
