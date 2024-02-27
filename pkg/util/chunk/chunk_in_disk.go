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
	"io"
	"math/rand"
	"os"
	"strconv"
	"unsafe"

	errors2 "github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/disk"
	"github.com/pingcap/tidb/pkg/util/memory"
)

const byteLen = int64(unsafe.Sizeof(byte(0)))
const intLen = int64(unsafe.Sizeof(int(0)))
const int64Len = int64(unsafe.Sizeof(int64(0)))

const chkFixedSize = intLen * 4
const colMetaSize = int64Len * 4

const defaultChunkDataInDiskByChunksPath = "defaultChunkDataInDiskByChunksPath"

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
	d := &DataInDiskByChunks{
		fieldTypes:    fieldTypes,
		totalDataSize: 0,
		totalRowNum:   0,
		// TODO: set the quota of disk usage.
		diskTracker: disk.NewTracker(memory.LabelForChunkDataInDiskByChunks, -1),
		buf:         make([]byte, 0, 4096),
	}
	return d
}

func (d *DataInDiskByChunks) initDiskFile() (err error) {
	err = disk.CheckAndInitTempDir()
	if err != nil {
		return
	}
	err = d.dataFile.initWithFileName(defaultChunkDataInDiskByChunksPath + strconv.Itoa(d.diskTracker.Label()))
	return
}

// GetDiskTracker returns the memory tracker of this List.
func (d *DataInDiskByChunks) GetDiskTracker() *disk.Tracker {
	return d.diskTracker
}

// Add adds a chunk to the DataInDiskByChunks. Caller must make sure the input chk has the same field types.
// Warning: Do not concurrently call this function.
func (d *DataInDiskByChunks) Add(chk *Chunk) (err error) {
	if err := injectChunkInDiskRandomError(); err != nil {
		return err
	}

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
	if err := injectChunkInDiskRandomError(); err != nil {
		return nil, err
	}

	reader := d.dataFile.getSectionReader(d.offsetOfEachChunk[chkIdx])
	chkSize := d.getChunkSize(chkIdx)

	if cap(d.buf) < int(chkSize) {
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

	chk := NewEmptyChunk(d.fieldTypes)
	d.deserializeDataToChunk(chk)

	return chk, nil
}

// Close releases the disk resource.
func (d *DataInDiskByChunks) Close() {
	if d.dataFile.file != nil {
		d.diskTracker.Consume(-d.diskTracker.BytesConsumed())
		terror.Call(d.dataFile.file.Close)
		terror.Log(os.Remove(d.dataFile.file.Name()))
	}
}

func (d *DataInDiskByChunks) serializeColMeta(pos int64, length int64, nullMapSize int64, dataSize int64, offsetSize int64) {
	*(*int64)(unsafe.Pointer(&d.buf[pos])) = length
	*(*int64)(unsafe.Pointer(&d.buf[pos+int64Len])) = nullMapSize
	*(*int64)(unsafe.Pointer(&d.buf[pos+int64Len*2])) = dataSize
	*(*int64)(unsafe.Pointer(&d.buf[pos+int64Len*3])) = offsetSize
}

func (d *DataInDiskByChunks) serializeOffset(pos *int64, offsets []int64, offsetSize int64) {
	d.buf = d.buf[:*pos+offsetSize]
	for _, offset := range offsets {
		*(*int64)(unsafe.Pointer(&d.buf[*pos])) = offset
		*pos += int64Len
	}
}

func (d *DataInDiskByChunks) serializeChunkData(pos *int64, chk *Chunk, selSize int64) {
	d.buf = d.buf[:chkFixedSize]
	*(*int)(unsafe.Pointer(&d.buf[*pos])) = chk.numVirtualRows
	*(*int)(unsafe.Pointer(&d.buf[*pos+intLen])) = chk.capacity
	*(*int)(unsafe.Pointer(&d.buf[*pos+intLen*2])) = chk.requiredRows
	*(*int)(unsafe.Pointer(&d.buf[*pos+intLen*3])) = int(selSize)
	*pos += chkFixedSize

	d.buf = d.buf[:*pos+selSize]

	selLen := len(chk.sel)
	for i := 0; i < selLen; i++ {
		*(*int)(unsafe.Pointer(&d.buf[*pos])) = chk.sel[i]
		*pos += intLen
	}
}

func (d *DataInDiskByChunks) serializeColumns(pos *int64, chk *Chunk) {
	for _, col := range chk.columns {
		d.buf = d.buf[:*pos+colMetaSize]
		nullMapSize := int64(len(col.nullBitmap)) * byteLen
		dataSize := int64(len(col.data)) * byteLen
		offsetSize := int64(len(col.offsets)) * int64Len
		d.serializeColMeta(*pos, int64(col.length), nullMapSize, dataSize, offsetSize)
		*pos += colMetaSize

		d.buf = append(d.buf, col.nullBitmap...)
		d.buf = append(d.buf, col.data...)
		*pos += nullMapSize + dataSize
		d.serializeOffset(pos, col.offsets, offsetSize)
	}
}

// Serialized format of a chunk:
// chunk   data: | numVirtualRows | capacity | requiredRows | selSize | sel... |
// column1 data: | length | nullMapSize | dataSize | offsetSize | nullBitmap... | data... | offsets... |
// column2 data: | length | nullMapSize | dataSize | offsetSize | nullBitmap... | data... | offsets... |
// ...
// columnN data: | length | nullMapSize | dataSize | offsetSize | nullBitmap... | data... | offsets... |
//
// `xxx...` means this is a variable field filled by bytes.
func (d *DataInDiskByChunks) serializeDataToBuf(chk *Chunk) int64 {
	totalBytes := int64(0)

	// Calculate total memory that buffer needs
	selSize := int64(len(chk.sel)) * intLen
	totalBytes += chkFixedSize + selSize
	for _, col := range chk.columns {
		nullMapSize := int64(len(col.nullBitmap)) * byteLen
		dataSize := int64(len(col.data)) * byteLen
		offsetSize := int64(len(col.offsets)) * int64Len
		totalBytes += colMetaSize + nullMapSize + dataSize + offsetSize
	}

	if cap(d.buf) < int(totalBytes) {
		d.buf = make([]byte, 0, totalBytes)
	}

	pos := int64(0)
	d.serializeChunkData(&pos, chk, selSize)
	d.serializeColumns(&pos, chk)
	return totalBytes
}

func (d *DataInDiskByChunks) deserializeColMeta(pos *int64) (length int64, nullMapSize int64, dataSize int64, offsetSize int64) {
	length = *(*int64)(unsafe.Pointer(&d.buf[*pos]))
	*pos += int64Len

	nullMapSize = *(*int64)(unsafe.Pointer(&d.buf[*pos]))
	*pos += int64Len

	dataSize = *(*int64)(unsafe.Pointer(&d.buf[*pos]))
	*pos += int64Len

	offsetSize = *(*int64)(unsafe.Pointer(&d.buf[*pos]))
	*pos += int64Len
	return
}

func (d *DataInDiskByChunks) deserializeSel(chk *Chunk, pos *int64, selSize int) {
	selLen := int64(selSize) / intLen
	chk.sel = make([]int, selLen)
	for i := int64(0); i < selLen; i++ {
		chk.sel[i] = *(*int)(unsafe.Pointer(&d.buf[*pos]))
		*pos += intLen
	}
}

func (d *DataInDiskByChunks) deserializeChunkData(chk *Chunk, pos *int64) {
	chk.numVirtualRows = *(*int)(unsafe.Pointer(&d.buf[*pos]))
	*pos += intLen

	chk.capacity = *(*int)(unsafe.Pointer(&d.buf[*pos]))
	*pos += intLen

	chk.requiredRows = *(*int)(unsafe.Pointer(&d.buf[*pos]))
	*pos += intLen

	selSize := *(*int)(unsafe.Pointer(&d.buf[*pos]))
	*pos += intLen
	if selSize != 0 {
		d.deserializeSel(chk, pos, selSize)
	}
}

func (d *DataInDiskByChunks) deserializeOffsets(dst []int64, pos *int64) {
	offsetNum := len(dst)
	for i := 0; i < offsetNum; i++ {
		dst[i] = *(*int64)(unsafe.Pointer(&d.buf[*pos]))
		*pos += int64Len
	}
}

func (d *DataInDiskByChunks) deserializeColumns(chk *Chunk, pos *int64) {
	for _, col := range chk.columns {
		length, nullMapSize, dataSize, offsetSize := d.deserializeColMeta(pos)
		col.nullBitmap = make([]byte, nullMapSize)
		col.data = make([]byte, dataSize)
		col.offsets = make([]int64, offsetSize/int64Len)

		col.length = int(length)
		copy(col.nullBitmap, d.buf[*pos:*pos+nullMapSize])
		*pos += nullMapSize
		copy(col.data, d.buf[*pos:*pos+dataSize])
		*pos += dataSize
		d.deserializeOffsets(col.offsets, pos)
	}
}

func (d *DataInDiskByChunks) deserializeDataToChunk(chk *Chunk) {
	pos := int64(0)
	d.deserializeChunkData(chk, &pos)
	d.deserializeColumns(chk, &pos)
}

// NumRows returns total spilled row number
func (d *DataInDiskByChunks) NumRows() int64 {
	return d.totalRowNum
}

// NumChunks returns total spilled chunk number
func (d *DataInDiskByChunks) NumChunks() int {
	return len(d.offsetOfEachChunk)
}

func injectChunkInDiskRandomError() error {
	var err error
	failpoint.Inject("ChunkInDiskError", func(val failpoint.Value) {
		if val.(bool) {
			randNum := rand.Int31n(10000)
			if randNum < 3 {
				err = errors2.New("random error is triggered")
			}
		}
	})
	return err
}
