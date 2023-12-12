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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package chunk

import (
	"bufio"
	"io"
	"os"
	"strconv"

	errors2 "github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/disk"
	"github.com/pingcap/tidb/pkg/util/memory"
)

// DataInDiskByRows represents some data stored in temporary disk.
// These data are stored in row format, so they can only be restored by rows.
type DataInDiskByRows struct {
	fieldTypes                []*types.FieldType
	numRowsOfEachChunk        []int
	rowNumOfEachChunkFirstRow []int
	totalNumRows              int
	diskTracker               *disk.Tracker // track disk usage.

	dataFile   diskFileReaderWriter
	offsetFile diskFileReaderWriter
}

var defaultChunkDataInDiskByRowsPath = "chunk.DataInDiskByRows"
var defaultChunkDataInDiskByRowsOffsetPath = "chunk.DataInDiskByRowsOffset"

// NewDataInDiskByRows creates a new DataInDiskByRows with field types.
func NewDataInDiskByRows(fieldTypes []*types.FieldType) *DataInDiskByRows {
	l := &DataInDiskByRows{
		fieldTypes: fieldTypes,
		// TODO(fengliyuan): set the quota of disk usage.
		diskTracker: disk.NewTracker(memory.LabelForChunkDataInDiskByRows, -1),
	}
	return l
}

func (l *DataInDiskByRows) initDiskFile() (err error) {
	err = disk.CheckAndInitTempDir()
	if err != nil {
		return
	}
	err = l.dataFile.initWithFileName(defaultChunkDataInDiskByRowsPath + strconv.Itoa(l.diskTracker.Label()))
	if err != nil {
		return
	}
	err = l.offsetFile.initWithFileName(defaultChunkDataInDiskByRowsOffsetPath + strconv.Itoa(l.diskTracker.Label()))
	return
}

// Len returns the number of rows in DataInDiskByRows
func (l *DataInDiskByRows) Len() int {
	return l.totalNumRows
}

// GetDiskTracker returns the memory tracker of this List.
func (l *DataInDiskByRows) GetDiskTracker() *disk.Tracker {
	return l.diskTracker
}

// Add adds a chunk to the DataInDiskByRows. Caller must make sure the input chk
// is not empty and not used any more and has the same field types.
// Warning: Do not use Add concurrently.
func (l *DataInDiskByRows) Add(chk *Chunk) (err error) {
	if chk.NumRows() == 0 {
		return errors2.New("chunk appended to List should have at least 1 row")
	}
	if l.dataFile.file == nil {
		err = l.initDiskFile()
		if err != nil {
			return
		}
	}
	// Append data
	chkInDisk := chunkInDisk{Chunk: chk, offWrite: l.dataFile.offWrite}
	n, err := chkInDisk.WriteTo(l.dataFile.getWriter())
	l.dataFile.offWrite += n
	if err != nil {
		return
	}

	// Append offsets
	offsetsOfRows := chkInDisk.getOffsetsOfRows()
	l.numRowsOfEachChunk = append(l.numRowsOfEachChunk, len(offsetsOfRows))
	l.rowNumOfEachChunkFirstRow = append(l.rowNumOfEachChunkFirstRow, l.totalNumRows)
	n2, err := offsetsOfRows.WriteTo(l.offsetFile.getWriter())
	l.offsetFile.offWrite += n2
	if err != nil {
		return
	}

	l.diskTracker.Consume(n + n2)
	l.totalNumRows += chk.NumRows()
	return
}

// GetChunk gets a Chunk from the DataInDiskByRows by chkIdx.
func (l *DataInDiskByRows) GetChunk(chkIdx int) (*Chunk, error) {
	chk := NewChunkWithCapacity(l.fieldTypes, l.NumRowsOfChunk(chkIdx))
	chkSize := l.numRowsOfEachChunk[chkIdx]

	firstRowOffset, err := l.getOffset(uint32(chkIdx), 0)
	if err != nil {
		return nil, err
	}

	// this channel is big enough and will never be blocked.
	formatCh := make(chan rowInDisk, chkSize)
	var formatChErr error
	go func() {
		defer close(formatCh)

		// If the row is small, a bufio can significantly improve the performance. As benchmark shows, it's still not bad
		// for longer rows.
		r := bufio.NewReader(l.dataFile.getSectionReader(firstRowOffset))
		format := rowInDisk{numCol: len(l.fieldTypes)}
		for rowIdx := 0; rowIdx < chkSize; rowIdx++ {
			_, err = format.ReadFrom(r)
			if err != nil {
				formatChErr = err
				break
			}

			formatCh <- format
		}
	}()

	for format := range formatCh {
		_, chk = format.toRow(l.fieldTypes, chk)
	}
	return chk, formatChErr
}

// GetRow gets a Row from the DataInDiskByRows by RowPtr.
func (l *DataInDiskByRows) GetRow(ptr RowPtr) (row Row, err error) {
	row, _, err = l.GetRowAndAppendToChunk(ptr, nil)
	return row, err
}

// GetRowAndAppendToChunk gets a Row from the DataInDiskByRows by RowPtr. Return the Row and the Ref Chunk.
func (l *DataInDiskByRows) GetRowAndAppendToChunk(ptr RowPtr, chk *Chunk) (row Row, _ *Chunk, err error) {
	off, err := l.getOffset(ptr.ChkIdx, ptr.RowIdx)
	if err != nil {
		return
	}
	r := l.dataFile.getSectionReader(off)
	format := rowInDisk{numCol: len(l.fieldTypes)}
	_, err = format.ReadFrom(r)
	if err != nil {
		return row, nil, err
	}
	row, chk = format.toRow(l.fieldTypes, chk)
	return row, chk, err
}

func (l *DataInDiskByRows) getOffset(chkIdx uint32, rowIdx uint32) (int64, error) {
	offsetInOffsetFile := l.rowNumOfEachChunkFirstRow[chkIdx] + int(rowIdx)
	b := make([]byte, 8)
	reader := l.offsetFile.getSectionReader(int64(offsetInOffsetFile) * 8)
	n, err := io.ReadFull(reader, b)
	if err != nil {
		return 0, err
	}
	if n != 8 {
		return 0, errors2.New("The file spilled is broken, can not get data offset from the disk")
	}
	return bytesToI64Slice(b)[0], nil
}

// NumRowsOfChunk returns the number of rows of a chunk in the DataInDiskByRows.
func (l *DataInDiskByRows) NumRowsOfChunk(chkID int) int {
	return l.numRowsOfEachChunk[chkID]
}

// NumChunks returns the number of chunks in the DataInDiskByRows.
func (l *DataInDiskByRows) NumChunks() int {
	return len(l.numRowsOfEachChunk)
}

// Close releases the disk resource.
func (l *DataInDiskByRows) Close() error {
	if l.dataFile.file != nil {
		l.diskTracker.Consume(-l.diskTracker.BytesConsumed())
		terror.Call(l.dataFile.file.Close)
		terror.Log(os.Remove(l.dataFile.file.Name()))
	}
	if l.offsetFile.file != nil {
		terror.Call(l.offsetFile.file.Close)
		terror.Log(os.Remove(l.offsetFile.file.Name()))
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
	offsetsOfRows offsetsOfRows
}

type offsetsOfRows []int64

// WriteTo serializes the offsetsOfRow, and writes to w.
func (off offsetsOfRows) WriteTo(w io.Writer) (written int64, err error) {
	n, err := w.Write(i64SliceToBytes(off))
	return int64(n), err
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
func (chk *chunkInDisk) getOffsetsOfRows() offsetsOfRows { return chk.offsetsOfRows }

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
	// In toRow, data in cells is deep copied to Row.
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

// toRow deserializes diskFormatRow to Row.
func (format *diskFormatRow) toRow(fields []*types.FieldType, chk *Chunk) (Row, *Chunk) {
	if chk == nil || chk.IsFull() {
		chk = NewChunkWithCapacity(fields, 1024)
	}
	var cellOff int
	for colIdx, size := range format.sizesOfColumns {
		col := chk.columns[colIdx]
		if size == -1 { // isNull
			col.AppendNull()
		} else {
			if col.isFixed() {
				col.elemBuf = format.cells[cellOff]
				col.finishAppendFixed()
			} else {
				col.AppendBytes(format.cells[cellOff])
			}
			cellOff++
		}
	}

	return Row{c: chk, idx: chk.NumRows() - 1}, chk
}

// ReaderWithCache helps to read data that has not be flushed to underlying layer.
// By using ReaderWithCache, user can still write data into DataInDiskByRows even after reading.
type ReaderWithCache struct {
	r        io.ReaderAt
	cacheOff int64
	cache    []byte
}

// NewReaderWithCache returns a ReaderWithCache.
func NewReaderWithCache(r io.ReaderAt, cache []byte, cacheOff int64) *ReaderWithCache {
	return &ReaderWithCache{
		r:        r,
		cacheOff: cacheOff,
		cache:    cache,
	}
}

// ReadAt implements the ReadAt interface.
func (r *ReaderWithCache) ReadAt(p []byte, off int64) (readCnt int, err error) {
	readCnt, err = r.r.ReadAt(p, off)
	if err != io.EOF {
		return readCnt, err
	}

	if len(p) == readCnt {
		return readCnt, err
	} else if len(p) < readCnt {
		return readCnt, errors2.Trace(errors2.Errorf("cannot read more data than user requested"+
			"(readCnt: %v, len(p): %v", readCnt, len(p)))
	}

	// When got here, user input is not filled fully, so we need read data from cache.
	err = nil
	p = p[readCnt:]
	beg := off - r.cacheOff
	if beg < 0 {
		// This happens when only partial data of user requested resides in r.cache.
		beg = 0
	}
	end := int(beg) + len(p)
	if end > len(r.cache) {
		err = io.EOF
		end = len(r.cache)
	}
	readCnt += copy(p, r.cache[beg:end])
	return readCnt, err
}
