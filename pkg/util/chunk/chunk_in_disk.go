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
	"os"
	"strconv"
	"unsafe"

	errors2 "github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/disk"
	"github.com/pingcap/tidb/pkg/util/memory"
)

// DataInDiskByChunk represents some data stored in temporary disk.
// They can only be restored by chunks.
type DataInDiskByChunk struct {
	fieldTypes        []*types.FieldType
	offsetOfEachChunk []int64

	totalDataSize int64
	diskTracker   *disk.Tracker // track disk usage.

	dataFile diskFileReaderWriter
	buf      []byte // Store spilled chunk data
}

// NewDataInDiskByChunk creates a new DataInDiskByChunk with field types.
func NewDataInDiskByChunk(fieldTypes []*types.FieldType) *DataInDiskByChunk {
	l := &DataInDiskByChunk{
		fieldTypes: fieldTypes,
		// TODO: set the quota of disk usage.
		diskTracker: disk.NewTracker(memory.LabelForChunkDataInDiskByChunks, -1),
		buf:         make([]byte, 0, 4096),
	}
	return l
}

func (l *DataInDiskByChunk) initDiskFile() (err error) {
	err = disk.CheckAndInitTempDir()
	if err != nil {
		return
	}
	err = l.dataFile.initWithFileName(defaultChunkDataInDiskByRowsPath + strconv.Itoa(l.diskTracker.Label()))
	if err != nil {
		return
	}
	return
}

// GetDiskTracker returns the memory tracker of this List.
func (l *DataInDiskByChunk) GetDiskTracker() *disk.Tracker {
	return l.diskTracker
}

// Add adds a chunk to the DataInDiskByChunk. Caller must make sure the input chk
// is not empty and not used any more and has the same field types.
// Warning: Do not use Add concurrently.
func (l *DataInDiskByChunk) Add(chk *Chunk) (err error) {
	if chk.NumRows() == 0 {
		return errors2.New("chunk appended to List should have at least 1 row")
	}

	if l.dataFile.file == nil {
		err = l.initDiskFile()
		if err != nil {
			return
		}
	}

	serializedBytesNum := l.serializeDataToBuf(chk)

	var writeNum int
	writeNum, err = l.dataFile.getWriter().Write(l.buf)
	if err != nil {
		return
	}

	if int64(writeNum) != serializedBytesNum {
		return errors2.New("Some data fail to be spilled to disk")
	}

	l.dataFile.offWrite += serializedBytesNum
	l.diskTracker.Consume(serializedBytesNum)
	l.totalDataSize += serializedBytesNum
	return
}

func (l *DataInDiskByChunk) getChunkSize(chkIdx int) int {
	
}

// GetChunk gets a Chunk from the DataInDiskByChunk by chkIdx.
func (l *DataInDiskByChunk) GetChunk(chkIdx int) (*Chunk, error) {
	chk := NewChunkWithCapacity(l.fieldTypes, 1024)
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

// Close releases the disk resource.
func (l *DataInDiskByChunk) Close() error {
	if l.dataFile.file != nil {
		l.diskTracker.Consume(-l.diskTracker.BytesConsumed())
		terror.Call(l.dataFile.file.Close)
		terror.Log(os.Remove(l.dataFile.file.Name()))
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
func (l *DataInDiskByChunk) serializeDataToBuf(chk *Chunk) int64 {
	l.buf = l.buf[0:0] // clear the buf data

	rowNum := chk.NumRows()
	colNum := chk.NumCols()
	colSizeRecordBytes := int64(4 * colNum)
	totalBytes := int64(0)
	addedBytesNum := int64(0)
	rowBuf := make([]byte, 0, 1024)
	tmpBuf := make([]byte, 8) // Used for storing column size

	for i := 0; i < rowNum; i++ {
		row := chk.GetRow(i)
		totalBytes += colSizeRecordBytes
		rowBuf, addedBytesNum = serializeDataToRowBuf(&row, rowBuf, tmpBuf, colNum)
		totalBytes += addedBytesNum
		l.buf = append(l.buf, rowBuf...)
	}
	return totalBytes
}

// Serialize data in a row into a buffer
func serializeDataToRowBuf(row *Row, rowBuf []byte, tmpBuf []byte, colNum int) ([]byte, int64) {
	rowBuf = rowBuf[0:0]
	addedBytesNum := int64(0)
	for j := 0; j < colNum; j++ {
		if row.IsNull(j) {
			*(*int64)(unsafe.Pointer(&tmpBuf[0])) = -1
			rowBuf = append(rowBuf, tmpBuf...)
		} else {
			colBytes := row.GetRaw(j)
			colBytesNum := int64(len(colBytes))

			*(*int64)(unsafe.Pointer(&tmpBuf[0])) = colBytesNum
			rowBuf = append(rowBuf, tmpBuf...)
			rowBuf = append(rowBuf, colBytes...)

			addedBytesNum += colBytesNum
		}
	}
	return rowBuf, addedBytesNum
}
