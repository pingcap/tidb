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
	"os"

	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/disk"
)

// ExportedChunk is an exported alias for Chunk type to be used in tests.
type ExportedChunk = Chunk

// ExportedDataInDiskByChunks is an exported alias for DataInDiskByChunks type to be used in tests.
type ExportedDataInDiskByChunks = DataInDiskByChunks

// ExportedNewDataInDiskByChunks is an exported wrapper for NewDataInDiskByChunks function to be used in tests.
func ExportedNewDataInDiskByChunks(fieldTypes []*types.FieldType, fileNamePrefixForTest string) *ExportedDataInDiskByChunks {
	return NewDataInDiskByChunks(fieldTypes, fileNamePrefixForTest)
}

// ExportedDataInDiskByRows is an exported alias for DataInDiskByRows type to be used in tests.
type ExportedDataInDiskByRows = DataInDiskByRows

// ExportedNewDataInDiskByRows is an exported wrapper for NewDataInDiskByRows function to be used in tests.
func ExportedNewDataInDiskByRows(fieldTypes []*types.FieldType) *ExportedDataInDiskByRows {
	return NewDataInDiskByRows(fieldTypes)
}

// ExportedRow is an exported alias for Row type to be used in tests.
type ExportedRow = Row

// ExportedColumn is an exported alias for Column type to be used in tests.
type ExportedColumn = Column

// ExportedIterator is an exported alias for Iterator interface to be used in tests.
type ExportedIterator = Iterator

// ExportedRowContainer is an exported alias for RowContainer type to be used in tests.
type ExportedRowContainer = RowContainer

// ExportedDiskFileReaderWriter is an exported alias for diskFileReaderWriter type to be used in tests.
type ExportedDiskFileReaderWriter = diskFileReaderWriter

// ExportedNewAllocator is an exported wrapper for NewAllocator function to be used in tests.
func ExportedNewAllocator() Allocator {
	return NewAllocator()
}

// ExportedGetFixedLen is an exported wrapper for getFixedLen function to be used in tests.
func ExportedGetFixedLen(colType *types.FieldType) int {
	return getFixedLen(colType)
}

// ExportedAllocator is an exported alias for allocator type to be used in tests.
type ExportedAllocator = allocator

// ExportedAllocatorInterface is an exported alias for Allocator interface to be used in tests.
type ExportedAllocatorInterface = Allocator

// ExportedGetDataFile returns the dataFile from DataInDiskByChunks for tests.
func ExportedGetDataFile(d *DataInDiskByChunks) *ExportedDiskFileReaderWriter {
	return &d.dataFile
}

// ExportedGetDataFileForRows returns the dataFile from DataInDiskByRows for tests.
func ExportedGetDataFileForRows(d *DataInDiskByRows) *ExportedDiskFileReaderWriter {
	return &d.dataFile
}

// ExportedGetOffsetFileForRows returns the offsetFile from DataInDiskByRows for tests.
func ExportedGetOffsetFileForRows(d *DataInDiskByRows) *ExportedDiskFileReaderWriter {
	return &d.offsetFile
}

// ExportedGetDiskTracker returns the diskTracker from DataInDiskByChunks for tests.
func ExportedGetDiskTracker(d *DataInDiskByChunks) *disk.Tracker {
	return d.diskTracker
}

// ExportedSetDataFile sets the dataFile for DataInDiskByChunks for tests.
func ExportedSetDataFile(d *DataInDiskByChunks, file *os.File, writer io.WriteCloser) {
	d.dataFile.file = file
	d.dataFile.writer = writer
}

// ExportedNewPool is an exported wrapper for NewPool function to be used in tests.
func ExportedNewPool(capacity int) *Pool {
	return NewPool(capacity)
}

// ExportedInitChunkAllocSize is an exported wrapper for InitChunkAllocSize function to be used in tests.
func ExportedInitChunkAllocSize(maxChunkNum, maxColNum int) {
	InitChunkAllocSize(uint32(maxChunkNum), uint32(maxColNum))
}

// ExportedNewReuseHookAllocator is an exported wrapper for NewReuseHookAllocator function to be used in tests.
func ExportedNewReuseHookAllocator(alloc Allocator, hook func()) Allocator {
	return NewReuseHookAllocator(alloc, hook)
}

// ExportedNewSyncAllocator is an exported wrapper for NewSyncAllocator function to be used in tests.
func ExportedNewSyncAllocator(alloc Allocator) Allocator {
	return NewSyncAllocator(alloc)
}

// ExportedGetColumns returns the columns from a Chunk for tests.
func ExportedGetColumns(c *Chunk) []*Column {
	return c.columns
}

// ExportedSetColumns sets the columns for a Chunk for tests.
func ExportedSetColumns(c *Chunk, columns []*Column) {
	c.columns = columns
}

// ExportedMutRow is an exported alias for MutRow type to be used in tests.
type ExportedMutRow = MutRow

// ExportedGetMutRowChunk returns the underlying Chunk from a MutRow for tests.
func ExportedGetMutRowChunk(m *MutRow) *Chunk {
	return m.c
}

// ExportedGetRowChunk returns the underlying Chunk from a Row for tests.
func ExportedGetRowChunk(r *Row) *Chunk {
	return r.c
}

// ExportedGetColumnElemBuf returns the elemBuf from a Column for tests.
func ExportedGetColumnElemBuf(c *Column) []byte {
	return c.elemBuf
}

// ExportedGetColumnData returns the data from a Column for tests.
func ExportedGetColumnData(c *Column) []byte {
	return c.data
}

// ExportedGetColumnLength returns the length from a Column for tests.
func ExportedGetColumnLength(c *Column) int {
	return c.length
}

// ExportedSetColumnLength sets the length for a Column for tests.
func ExportedSetColumnLength(c *Column, length int) {
	c.length = length
}

// ExportedGetColumnNullBitmap returns the nullBitmap from a Column for tests.
func ExportedGetColumnNullBitmap(c *Column) []byte {
	return c.nullBitmap
}

// ExportedGetColumnOffsets returns the offsets from a Column for tests.
func ExportedGetColumnOffsets(c *Column) []int64 {
	return c.offsets
}

// ExportedSetColumnOffsets sets the offsets for a Column for tests.
func ExportedSetColumnOffsets(c *Column, offsets []int64) {
	c.offsets = offsets
}

// ExportedGetColumnAvoidReusing returns the avoidReusing from a Column for tests.
func ExportedGetColumnAvoidReusing(c *Column) bool {
	return c.avoidReusing
}

// ExportedSetColumnAvoidReusing sets the avoidReusing for a Column for tests.
func ExportedSetColumnAvoidReusing(c *Column, avoidReusing bool) {
	c.avoidReusing = avoidReusing
}

// ExportedMaxFreeChunks is an exported alias for maxFreeChunks variable to be used in tests.
var ExportedMaxFreeChunks = &maxFreeChunks

// ExportedMaxFreeColumnsPerType is an exported alias for maxFreeColumnsPerType variable to be used in tests.
var ExportedMaxFreeColumnsPerType = &maxFreeColumnsPerType

// ExportedPoolColumnAllocator is an exported alias for poolColumnAllocator type to be used in tests.
type ExportedPoolColumnAllocator = poolColumnAllocator

// ExportedDefaultColumnAllocator is an exported alias for DefaultColumnAllocator type to be used in tests.
type ExportedDefaultColumnAllocator = DefaultColumnAllocator

// ExportedNewDefaultColumnAllocator is an exported wrapper for creating a new DefaultColumnAllocator for tests.
func ExportedNewDefaultColumnAllocator() *DefaultColumnAllocator {
	return &DefaultColumnAllocator{}
}

// ExportedGetAllocatorFree returns the free field from an allocator for tests.
func ExportedGetAllocatorFree(alloc Allocator) []*Chunk {
	if a, ok := alloc.(*allocator); ok {
		return a.free
	}
	return nil
}

// ExportedGetColumnAlloc returns the columnAlloc field from an allocator for tests.
func ExportedGetColumnAlloc(alloc Allocator) *poolColumnAllocator {
	if a, ok := alloc.(*allocator); ok {
		return &a.columnAlloc
	}
	return nil
}

// ExportedNewColumn is an exported wrapper for NewColumn function to be used in tests.
func ExportedNewColumn(ft *types.FieldType, capacity int) *Column {
	return NewColumn(ft, capacity)
}

// ExportedPoolColumnAllocatorInit calls the init method on poolColumnAllocator for tests.
func ExportedPoolColumnAllocatorInit(p *poolColumnAllocator) {
	p.init()
}

// ExportedPoolColumnAllocatorPut calls the put method on poolColumnAllocator for tests.
func ExportedPoolColumnAllocatorPut(p *poolColumnAllocator, c *Column) {
	p.put(c)
}

// ExportedPoolColumnAllocatorPool returns the pool field from poolColumnAllocator for tests.
func ExportedPoolColumnAllocatorPool(p *poolColumnAllocator) map[int]*columnList {
	return p.pool
}

// ExportedCodec is an exported alias for Codec type to be used in tests.
type ExportedCodec = Codec

// ExportedNewCodec is an exported wrapper for creating a new Codec for tests.
func ExportedNewCodec(fieldTypes []*types.FieldType) *Codec {
	return &Codec{fieldTypes}
}

// ExportedDecoder is an exported alias for Decoder type to be used in tests.
type ExportedDecoder = Decoder

// ExportedNewDecoder is an exported wrapper for NewDecoder function to be used in tests.
func ExportedNewDecoder(chk *Chunk, fieldTypes []*types.FieldType) *Decoder {
	return NewDecoder(chk, fieldTypes)
}

// ExportedNewChunkWithCapacity is an exported wrapper for NewChunkWithCapacity function to be used in tests.
func ExportedNewChunkWithCapacity(fieldTypes []*types.FieldType, capacity int) *Chunk {
	return NewChunkWithCapacity(fieldTypes, capacity)
}

// ExportedMakeRef calls the MakeRef method on Chunk for tests.
func ExportedMakeRef(c *Chunk, idx, targetCol int) {
	c.MakeRef(idx, targetCol)
}

// ExportedColumnList is an exported alias for columnList type to be used in tests.
type ExportedColumnList = columnList

// ExportedColumnListEmpty calls the empty method on columnList for tests.
func ExportedColumnListEmpty(c *columnList) bool {
	return c.empty()
}

// ExportedColumnListPop calls the pop method on columnList for tests.
func ExportedColumnListPop(c *columnList) *Column {
	return c.pop()
}

// ExportedColumnListLen calls the Len method on columnList for tests.
func ExportedColumnListLen(c *columnList) int {
	return c.Len()
}

// ExportedNewAllocator creates a new basic allocator for tests.
func ExportedNewBasicAllocator() *allocator {
	return NewAllocator()
}

// ExportedGetAllocatorColumnAlloc returns the columnAlloc from an allocator struct for tests.
func ExportedGetAllocatorColumnAlloc(a *allocator) *poolColumnAllocator {
	return &a.columnAlloc
}

// ExportedPoolColumnAllocatorNewColumn calls NewColumn on poolColumnAllocator for tests.
func ExportedPoolColumnAllocatorNewColumn(alloc *poolColumnAllocator, ft *types.FieldType, count int) *Column {
	return alloc.NewColumn(ft, count)
}

// ExportedColumnListGetAllocColumns returns the allocColumns field from columnList for tests.
func ExportedColumnListGetAllocColumns(c *columnList) []*Column {
	return c.allocColumns
}

// ExportedVarElemLen is the exported constant for variable length columns.
const ExportedVarElemLen = VarElemLen

// ExportedAllocatorType is the exported allocator struct type.
type ExportedAllocatorType = allocator

// ExportedAllocatorTypeCast casts an Allocator interface to the concrete allocator type for tests.
func ExportedAllocatorTypeCast(alloc Allocator) *allocator {
	if a, ok := alloc.(*allocator); ok {
		return a
	}
	return nil
}

// ExportedMutRowFromValues creates a MutRow from values for tests.
func ExportedMutRowFromValues(values ...any) MutRow {
	return MutRowFromValues(values...)
}

// ExportedGetChunkNumVirtualRows returns the numVirtualRows field from a Chunk for tests.
func ExportedGetChunkNumVirtualRows(c *Chunk) int {
	return c.numVirtualRows
}

// ExportedNewChunkWithCapacityForTest is an alias for NewChunkWithCapacity for tests.
func ExportedNewChunkWithCapacityForTest(fieldTypes []*types.FieldType, capacity int) *Chunk {
	return NewChunkWithCapacity(fieldTypes, capacity)
}

// ExportedColumnNullCount calls the nullCount method on a Column for tests.
func ExportedColumnNullCount(c *Column) int {
	return c.nullCount()
}

// ExportedSizeTime is the exported constant for Time element size.
const ExportedSizeTime = sizeTime

// ExportedNewFixedLenColumn is exported version of newFixedLenColumn.
func ExportedNewFixedLenColumn(elemLen, capacity int) *Column {
	return newFixedLenColumn(elemLen, capacity)
}

// ExportedNewVarLenColumn is exported version of newVarLenColumn.
func ExportedNewVarLenColumn(capacity int) *Column {
	return newVarLenColumn(capacity)
}
