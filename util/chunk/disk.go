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
	"io"
	"os"
	"strconv"

	errors2 "github.com/pingcap/errors"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/checksum"
	"github.com/pingcap/tidb/util/disk"
	"github.com/pingcap/tidb/util/encrypt"
	"github.com/pingcap/tidb/util/memory"
)

// ListInDisk represents a slice of chunks storing in temporary disk.
type ListInDisk struct {
	fieldTypes                []*types.FieldType
	numRowsOfEachChunk        []int
	rowNumOfEachChunkFirstRow []int
	totalNumRows              int
	diskTracker               *disk.Tracker // track disk usage.

	dataFile   diskFileReaderWriter
	offsetFile diskFileReaderWriter
}

// diskFileReaderWriter represents a Reader and a Writer for the temporary disk file.
type diskFileReaderWriter struct {
	disk *os.File
	w    io.WriteCloser
	// offWrite is the current offset for writing.
	offWrite int64

	checksumWriter *checksum.Writer
	cipherWriter   *encrypt.Writer // cipherWriter is only enable when config SpilledFileEncryptionMethod is "aes128-ctr"

	// ctrCipher stores the key and nonce using by aes encrypt io layer
	ctrCipher *encrypt.CtrCipher
}

func (l *diskFileReaderWriter) initWithFileName(fileName string) (err error) {
	l.disk, err = os.CreateTemp(config.GetGlobalConfig().TempStoragePath, fileName)
	if err != nil {
		return errors2.Trace(err)
	}
	var underlying io.WriteCloser = l.disk
	if config.GetGlobalConfig().Security.SpilledFileEncryptionMethod != config.SpilledFileEncryptionMethodPlaintext {
		// The possible values of SpilledFileEncryptionMethod are "plaintext", "aes128-ctr"
		l.ctrCipher, err = encrypt.NewCtrCipher()
		if err != nil {
			return
		}
		l.cipherWriter = encrypt.NewWriter(l.disk, l.ctrCipher)
		underlying = l.cipherWriter
	}
	l.checksumWriter = checksum.NewWriter(underlying)
	l.w = l.checksumWriter
	return
}

func (l *diskFileReaderWriter) getReader() io.ReaderAt {
	var underlying io.ReaderAt = l.disk
	if l.ctrCipher != nil {
		underlying = NewReaderWithCache(encrypt.NewReader(l.disk, l.ctrCipher), l.cipherWriter.GetCache(), l.cipherWriter.GetCacheDataOffset())
	}
	if l.checksumWriter != nil {
		underlying = NewReaderWithCache(checksum.NewReader(underlying), l.checksumWriter.GetCache(), l.checksumWriter.GetCacheDataOffset())
	}
	return underlying
}

func (l *diskFileReaderWriter) getSectionReader(off int64) *io.SectionReader {
	checksumReader := l.getReader()
	r := io.NewSectionReader(checksumReader, off, l.offWrite-off)
	return r
}

func (l *diskFileReaderWriter) getWriter() io.Writer {
	return l.w
}

var defaultChunkListInDiskPath = "chunk.ListInDisk"
var defaultChunkListInDiskOffsetPath = "chunk.ListInDiskOffset"

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
	err = l.dataFile.initWithFileName(defaultChunkListInDiskPath + strconv.Itoa(l.diskTracker.Label()))
	if err != nil {
		return
	}
	err = l.offsetFile.initWithFileName(defaultChunkListInDiskOffsetPath + strconv.Itoa(l.diskTracker.Label()))
	return
}

// Len returns the number of rows in ListInDisk
func (l *ListInDisk) Len() int {
	return l.totalNumRows
}

// GetDiskTracker returns the memory tracker of this List.
func (l *ListInDisk) GetDiskTracker() *disk.Tracker {
	return l.diskTracker
}

// Add adds a chunk to the ListInDisk. Caller must make sure the input chk
// is not empty and not used any more and has the same field types.
// Warning: Do not use Add concurrently.
func (l *ListInDisk) Add(chk *Chunk) (err error) {
	if chk.NumRows() == 0 {
		return errors2.New("chunk appended to List should have at least 1 row")
	}
	if l.dataFile.disk == nil {
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

// GetChunk gets a Chunk from the ListInDisk by chkIdx.
func (l *ListInDisk) GetChunk(chkIdx int) (*Chunk, error) {
	chk := NewChunkWithCapacity(l.fieldTypes, l.NumRowsOfChunk(chkIdx))
	chkSize := l.numRowsOfEachChunk[chkIdx]
	for rowIdx := 0; rowIdx < chkSize; rowIdx++ {
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
	off, err := l.getOffset(ptr.ChkIdx, ptr.RowIdx)
	if err != nil {
		return
	}
	r := l.dataFile.getSectionReader(off)
	format := rowInDisk{numCol: len(l.fieldTypes)}
	_, err = format.ReadFrom(r)
	if err != nil {
		return row, err
	}
	row = format.toMutRow(l.fieldTypes).ToRow()
	return row, err
}

func (l *ListInDisk) getOffset(chkIdx uint32, rowIdx uint32) (int64, error) {
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

// NumRowsOfChunk returns the number of rows of a chunk in the ListInDisk.
func (l *ListInDisk) NumRowsOfChunk(chkID int) int {
	return l.numRowsOfEachChunk[chkID]
}

// NumChunks returns the number of chunks in the ListInDisk.
func (l *ListInDisk) NumChunks() int {
	return len(l.numRowsOfEachChunk)
}

// Close releases the disk resource.
func (l *ListInDisk) Close() error {
	if l.dataFile.disk != nil {
		l.diskTracker.Consume(-l.diskTracker.BytesConsumed())
		terror.Call(l.dataFile.disk.Close)
		terror.Log(os.Remove(l.dataFile.disk.Name()))
	}
	if l.offsetFile.disk != nil {
		terror.Call(l.offsetFile.disk.Close)
		terror.Log(os.Remove(l.offsetFile.disk.Name()))
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

// ReaderWithCache helps to read data that has not be flushed to underlying layer.
// By using ReaderWithCache, user can still write data into ListInDisk even after reading.
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
