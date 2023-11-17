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
	"io"
	"os"
	"strconv"
	"unsafe"

	errors2 "github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/disk"
	"github.com/pingcap/tidb/pkg/util/memory"
)

type colSizeMetaType = int32

const colSizeMetaLen = int(unsafe.Sizeof(colSizeMetaType(0)))

// DataInDiskByChunks represents some data stored in temporary disk.
// They can only be restored by chunks.
type DataInDiskByChunks struct {
	fieldTypes        []*types.FieldType
	offsetOfEachChunk []int64

	totalDataSize int64
	totalRowNum   int64
	diskTracker   *disk.Tracker // track disk usage.

	dataFile diskFileReaderWriter

	// Write or read data needs this buffer to temporarily store data
	buf []byte
}

// NewDataInDiskByChunks creates a new DataInDiskByChunks with field types.
func NewDataInDiskByChunks(fieldTypes []*types.FieldType) *DataInDiskByChunks {
	l := &DataInDiskByChunks{
		fieldTypes:    fieldTypes,
		totalDataSize: 0,
		totalRowNum:   0,
		// TODO: set the quota of disk usage.
		diskTracker: disk.NewTracker(memory.LabelForChunkDataInDiskByChunks, -1),
		buf:         make([]byte, 0, 4096),
	}
	return l
}

func (d *DataInDiskByChunks) initDiskFile() (err error) {
	err = disk.CheckAndInitTempDir()
	if err != nil {
		return
	}
	err = d.dataFile.initWithFileName(defaultChunkDataInDiskByRowsPath + strconv.Itoa(d.diskTracker.Label()))
	if err != nil {
		return
	}
	return
}

// GetDiskTracker returns the memory tracker of this List.
func (d *DataInDiskByChunks) GetDiskTracker() *disk.Tracker {
	return d.diskTracker
}

// Add adds a chunk to the DataInDiskByChunks. Caller must make sure the input chk has the same field types.
// Warning: Do not concurrently call this function.
func (d *DataInDiskByChunks) Add(chk *Chunk) (err error) {
	if chk.NumRows() == 0 {
		return errors2.New("Chunk spilled to disk should have at least 1 row")
	}

	if d.dataFile.file == nil {
		err = d.initDiskFile()
		if err != nil {
			return
		}
	}

	serializedBytesNum := d.serializeDataToBuf(chk)

	var writeNum int
	writeNum, err = d.dataFile.write(d.buf)
	if err != nil {
		return
	}

	if int64(writeNum) != serializedBytesNum {
		return errors2.New("Some data fail to be spilled to disk")
	}
	d.offsetOfEachChunk = append(d.offsetOfEachChunk, d.totalDataSize)
	d.totalDataSize += serializedBytesNum
	d.totalRowNum += int64(chk.NumRows())
	d.dataFile.offWrite += serializedBytesNum

	d.diskTracker.Consume(serializedBytesNum)
	return
}

func (d *DataInDiskByChunks) getChunkSize(chkIdx int) int64 {
	totalChunkNum := len(d.offsetOfEachChunk)
	if chkIdx == totalChunkNum-1 {
		return d.totalDataSize - d.offsetOfEachChunk[chkIdx]
	}
	return d.offsetOfEachChunk[chkIdx+1] - d.offsetOfEachChunk[chkIdx]
}

// GetChunk gets a Chunk from the DataInDiskByChunks by chkIdx.
func (d *DataInDiskByChunks) GetChunk(chkIdx int) (*Chunk, error) {
	reader := bufio.NewReader(d.dataFile.getSectionReader(d.offsetOfEachChunk[chkIdx]))
	chkSize := d.getChunkSize(chkIdx)

	if len(d.buf) < int(chkSize) {
		d.buf = make([]byte, chkSize)
	} else {
		d.buf = d.buf[:chkSize]
	}

	readByteNum, err := io.ReadFull(reader, d.buf)
	if err != nil {
		return nil, err
	}

	if int64(readByteNum) != chkSize {
		return nil, errors2.New("Fail to restore the spilled chunk")
	}

	chk := NewChunkWithCapacity(d.fieldTypes, 2048)
	d.deserializeDataToChunk(d.buf, chk)

	return chk, nil
}

// Close releases the disk resource.
func (d *DataInDiskByChunks) Close() error {
	if d.dataFile.file != nil {
		d.diskTracker.Consume(-d.diskTracker.BytesConsumed())
		terror.Call(d.dataFile.file.Close)
		terror.Log(os.Remove(d.dataFile.file.Name()))
	}
	return nil
}

// Format of the chunk serialized in buffer:
// row1: | col1 size | col1 data | col2 size | col2 data | col3 size | col3 data |
// row2: | col1 size | col1 data | col2 size | col2 data | col3 size | col3 data |
// row3: | col1 size | col1 data | col2 size | col2 data | col3 size | col3 data |
// row4: | col1 size | col1 data | col2 size | col2 data | col3 size | col3 data |
//
// Column size will be -1 if column data is null.
func (d *DataInDiskByChunks) serializeDataToBuf(chk *Chunk) int64 {
	d.buf = d.buf[0:0] // clear the buf data

	rowNum := chk.NumRows()
	colNum := chk.NumCols()
	colSizeRecordBytes := int64(colSizeMetaLen * colNum)
	totalBytes := int64(0)
	addedBytesNum := int64(0)
	rowBuf := make([]byte, 0, 1024)
	tmpBuf := make([]byte, colSizeMetaLen) // Used for storing column size

	for i := 0; i < rowNum; i++ {
		row := chk.GetRow(i)
		totalBytes += colSizeRecordBytes
		rowBuf, addedBytesNum = serializeDataToRowBuf(&row, rowBuf, tmpBuf, colNum)
		totalBytes += addedBytesNum
		d.buf = append(d.buf, rowBuf...)
	}
	return totalBytes
}

// Serialize data of a row into a buffer
func serializeDataToRowBuf(row *Row, rowBuf []byte, tmpBuf []byte, colNum int) ([]byte, int64) {
	rowBuf = rowBuf[0:0]
	addedBytesNum := int64(0)
	for j := 0; j < colNum; j++ {
		if row.IsNull(j) {
			*(*colSizeMetaType)(unsafe.Pointer(&tmpBuf[0])) = -1
			rowBuf = append(rowBuf, tmpBuf...)
		} else {
			colBytes := row.GetRaw(j)
			colBytesNum := colSizeMetaType(len(colBytes))

			*(*colSizeMetaType)(unsafe.Pointer(&tmpBuf[0])) = colBytesNum
			rowBuf = append(rowBuf, tmpBuf...)
			rowBuf = append(rowBuf, colBytes...)

			addedBytesNum += int64(colBytesNum)
		}
	}
	return rowBuf, addedBytesNum
}

func (d *DataInDiskByChunks) deserializeDataToChunk(data []byte, chk *Chunk) {
	colNum := len(d.fieldTypes)
	dataSize := len(data)
	offset := 0
	for offset < dataSize {
		for colIdx := 0; colIdx < colNum; colIdx++ {
			col := chk.columns[colIdx]
			colDataSize := *(*colSizeMetaType)(unsafe.Pointer(&data[offset]))
			offset += colSizeMetaLen
			if colDataSize == -1 { // The data is null
				col.AppendNull()
			} else {
				chunkData := data[offset : offset+int(colDataSize)]
				if col.isFixed() {
					col.elemBuf = chunkData
					col.finishAppendFixed()
				} else {
					col.AppendBytes(chunkData)
				}
				offset += int(colDataSize)
			}
		}
	}
}

func (d *DataInDiskByChunks) NumRows() int64 {
	return d.totalRowNum
}

func (d *DataInDiskByChunks) NumChunks() int {
	return len(d.offsetOfEachChunk)
}
