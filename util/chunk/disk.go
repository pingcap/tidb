// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package chunk

import (
	"bufio"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"sync"

	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/disk"
	"github.com/pingcap/tidb/util/memory"
)

const (
	writeBufSize = 128 * 1024
	readBufSize  = 4 * 1024
)

var bufWriterPool = sync.Pool{
	New: func() interface{} { return bufio.NewWriterSize(nil, writeBufSize) },
}

var bufReaderPool = sync.Pool{
	New: func() interface{} { return bufio.NewReaderSize(nil, readBufSize) },
}

// ListInDisk represents a slice of chunks storing in temporary disk.
type ListInDisk struct {
	fieldTypes []*types.FieldType
	// offsets stores the offsets in disk of all RowPtr,
	// the offset of one RowPtr is offsets[RowPtr.ChkIdx][RowPtr.RowIdx].
	offsets [][]int64
	// offWrite is the current offset for writing.
	offWrite int64

	disk          *os.File
	bufWriter     *bufio.Writer
	bufFlushMutex sync.RWMutex
	diskTracker   *disk.Tracker // track disk usage.
	numRowsInDisk int
}

var defaultChunkListInDiskPath = "chunk.ListInDisk"

// NewListInDisk creates a new ListInDisk with field types.
func NewListInDisk(fieldTypes []*types.FieldType) *ListInDisk {
	l := &ListInDisk{
		fieldTypes: fieldTypes,
		// TODO(fengliyuan): set the quota of disk usage.
		diskTracker: disk.NewTracker(memory.LabelForChunkListInDisk, -1),
	}
	return l
}

func (l *ListInDisk) initDiskFile() (err error) {
	err = disk.CheckAndInitTempDir()
	if err != nil {
		return
	}
	l.disk, err = ioutil.TempFile(config.GetGlobalConfig().TempStoragePath, defaultChunkListInDiskPath+strconv.Itoa(l.diskTracker.Label()))
	if err != nil {
		return
	}
	l.bufWriter = bufWriterPool.Get().(*bufio.Writer)
	l.bufWriter.Reset(l.disk)
	l.bufFlushMutex = sync.RWMutex{}
	return
}

// Len returns the number of rows in ListInDisk
func (l *ListInDisk) Len() int {
	return l.numRowsInDisk
}

// GetDiskTracker returns the memory tracker of this List.
func (l *ListInDisk) GetDiskTracker() *disk.Tracker {
	return l.diskTracker
}

// flush empties the write buffer, please call flush before read!
func (l *ListInDisk) flush() (err error) {
	// buffered is not zero only after Add and before GetRow, after the first flush, buffered will always be zero,
	// hence we use a RWLock to allow quicker quit.
	l.bufFlushMutex.RLock()
	buffered := l.bufWriter.Buffered()
	l.bufFlushMutex.RUnlock()
	if buffered == 0 {
		return nil
	}
	l.bufFlushMutex.Lock()
	if l.bufWriter.Buffered() != 0 {
		err = l.bufWriter.Flush()
	}
	l.bufFlushMutex.Unlock()
	return err
}

// Add adds a chunk to the ListInDisk. Caller must make sure the input chk
// is not empty and not used any more and has the same field types.
// Warning: do not mix Add and GetRow (always use GetRow after you have added all the chunks), and do not use Add concurrently.
func (l *ListInDisk) Add(chk *Chunk) (err error) {
	if chk.NumRows() == 0 {
		return errors.New("chunk appended to List should have at least 1 row")
	}
	if l.disk == nil {
		err = l.initDiskFile()
		if err != nil {
			return
		}
	}
	chk2 := chunkInDisk{Chunk: chk, offWrite: l.offWrite}
	n, err := chk2.WriteTo(l.bufWriter)
	l.offWrite += n
	if err != nil {
		return
	}
	l.offsets = append(l.offsets, chk2.getOffsetsOfRows())
	l.diskTracker.Consume(n)
	l.numRowsInDisk += chk.NumRows()
	return
}

// GetChunk gets a Chunk from the ListInDisk by chkIdx.
func (l *ListInDisk) GetChunk(chkIdx int) (*Chunk, error) {
	chk := NewChunkWithCapacity(l.fieldTypes, l.NumRowsOfChunk(chkIdx))
	offsets := l.offsets[chkIdx]
	for rowIdx := range offsets {
		row, err := l.GetRow(RowPtr{ChkIdx: uint32(chkIdx), RowIdx: uint32(rowIdx)})
		if err != nil {
			return chk, err
		}
		chk.AppendRow(row)
	}
	return chk, nil
}

// GetRow gets a Row from the ListInDisk by RowPtr.
func (l *ListInDisk) GetRow(ptr RowPtr) (row Row, err error) {
	err = l.flush()
	if err != nil {
		return
	}
	off := l.offsets[ptr.ChkIdx][ptr.RowIdx]
	r := io.NewSectionReader(l.disk, off, l.offWrite-off)
	bufReader := bufReaderPool.Get().(*bufio.Reader)
	bufReader.Reset(r)
	defer bufReaderPool.Put(bufReader)

	format := rowInDisk{numCol: len(l.fieldTypes)}
	_, err = format.ReadFrom(bufReader)
	if err != nil {
		return row, err
	}
	row = format.toMutRow(l.fieldTypes).ToRow()
	return row, err
}

// NumRowsOfChunk returns the number of rows of a chunk in the ListInDisk.
func (l *ListInDisk) NumRowsOfChunk(chkID int) int {
	return len(l.offsets[chkID])
}

// NumChunks returns the number of chunks in the ListInDisk.
func (l *ListInDisk) NumChunks() int {
	return len(l.offsets)
}

// Close releases the disk resource.
func (l *ListInDisk) Close() error {
	if l.disk != nil {
		l.diskTracker.Consume(-l.diskTracker.BytesConsumed())
		terror.Call(l.disk.Close)
		bufWriterPool.Put(l.bufWriter)
		terror.Log(os.Remove(l.disk.Name()))
	}
	return nil
}

// chunkInDisk represents a chunk in disk format. Each row of the chunk
// is serialized and in sequence ordered. The format of each row is like
// the struct diskFormatRow, put size of each column first, then the
// data of each column.
//
// For example, a chunk has 2 rows and 3 columns, the disk format of the
// chunk is as follow:
//
// [size of row0 column0], [size of row0 column1], [size of row0 column2]
// [data of row0 column0], [data of row0 column1], [data of row0 column2]
// [size of row1 column0], [size of row1 column1], [size of row1 column2]
// [data of row1 column0], [data of row1 column1], [data of row1 column2]
//
// If a column of a row is null, the size of it is -1 and the data is empty.
type chunkInDisk struct {
	*Chunk
	// offWrite is the current offset for writing.
	offWrite int64
	// offsetsOfRows stores the offset of each row.
	offsetsOfRows []int64
}

// WriteTo serializes the chunk into the format of chunkInDisk, and
// writes to w.
func (chk *chunkInDisk) WriteTo(w io.Writer) (written int64, err error) {
	var n int64
	numRows := chk.NumRows()
	chk.offsetsOfRows = make([]int64, 0, numRows)
	var format *diskFormatRow
	for rowIdx := 0; rowIdx < numRows; rowIdx++ {
		format = convertFromRow(chk.GetRow(rowIdx), format)
		chk.offsetsOfRows = append(chk.offsetsOfRows, chk.offWrite+written)

		n, err = rowInDisk{diskFormatRow: *format}.WriteTo(w)
		written += n
		if err != nil {
			return
		}
	}
	return
}

// getOffsetsOfRows gets the offset of each row.
func (chk *chunkInDisk) getOffsetsOfRows() []int64 { return chk.offsetsOfRows }

// rowInDisk represents a Row in format of diskFormatRow.
type rowInDisk struct {
	numCol int
	diskFormatRow
}

// WriteTo serializes a row of the chunk into the format of
// diskFormatRow, and writes to w.
func (row rowInDisk) WriteTo(w io.Writer) (written int64, err error) {
	n, err := w.Write(i64SliceToBytes(row.sizesOfColumns))
	written += int64(n)
	if err != nil {
		return
	}
	for _, data := range row.cells {
		n, err = w.Write(data)
		written += int64(n)
		if err != nil {
			return
		}
	}
	return
}

// ReadFrom reads data of r, deserializes it from the format of diskFormatRow
// into Row.
func (row *rowInDisk) ReadFrom(r io.Reader) (n int64, err error) {
	b := make([]byte, 8*row.numCol)
	var n1 int
	n1, err = io.ReadFull(r, b)
	n += int64(n1)
	if err != nil {
		return
	}
	row.sizesOfColumns = bytesToI64Slice(b)
	row.cells = make([][]byte, 0, row.numCol)
	for _, size := range row.sizesOfColumns {
		if size == -1 {
			continue
		}
		cell := make([]byte, size)
		row.cells = append(row.cells, cell)
		n1, err = io.ReadFull(r, cell)
		n += int64(n1)
		if err != nil {
			return
		}
	}
	return
}

// diskFormatRow represents a row in a chunk in disk format. The disk format
// of a row is described in the doc of chunkInDisk.
type diskFormatRow struct {
	// sizesOfColumns stores the size of each column in a row.
	// -1 means the value of this column is null.
	sizesOfColumns []int64 // -1 means null
	// cells represents raw data of not-null columns in one row.
	// In convertFromRow, data from Row is shallow copied to cells.
	// In toMutRow, data in cells is shallow copied to MutRow.
	cells [][]byte
}

// convertFromRow serializes one row of chunk to diskFormatRow, then
// we can use diskFormatRow to write to disk.
func convertFromRow(row Row, reuse *diskFormatRow) (format *diskFormatRow) {
	numCols := row.Chunk().NumCols()
	if reuse != nil {
		format = reuse
		format.sizesOfColumns = format.sizesOfColumns[:0]
		format.cells = format.cells[:0]
	} else {
		format = &diskFormatRow{
			sizesOfColumns: make([]int64, 0, numCols),
			cells:          make([][]byte, 0, numCols),
		}
	}
	for colIdx := 0; colIdx < numCols; colIdx++ {
		if row.IsNull(colIdx) {
			format.sizesOfColumns = append(format.sizesOfColumns, -1)
		} else {
			cell := row.GetRaw(colIdx)
			format.sizesOfColumns = append(format.sizesOfColumns, int64(len(cell)))
			format.cells = append(format.cells, cell)
		}
	}
	return
}

// toMutRow deserializes diskFormatRow to MutRow.
func (format *diskFormatRow) toMutRow(fields []*types.FieldType) MutRow {
	chk := &Chunk{columns: make([]*Column, 0, len(format.sizesOfColumns))}
	var cellOff int
	for colIdx, size := range format.sizesOfColumns {
		col := &Column{length: 1}
		elemSize := getFixedLen(fields[colIdx])
		if size == -1 { // isNull
			col.nullBitmap = []byte{0}
			if elemSize == varElemLen {
				col.offsets = []int64{0, 0}
			} else {
				buf := make([]byte, elemSize)
				col.data = buf
				col.elemBuf = buf
			}
		} else {
			col.nullBitmap = []byte{1}
			col.data = format.cells[cellOff]
			cellOff++
			if elemSize == varElemLen {
				col.offsets = []int64{0, int64(len(col.data))}
			} else {
				col.elemBuf = col.data
			}
		}
		chk.columns = append(chk.columns, col)
	}
	return MutRow{c: chk}
}
