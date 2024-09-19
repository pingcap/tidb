// Copyright 2024 PingCAP, Inc.
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

package context

import (
	"time"

	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
)

// EncodeRowBuffer is used to encode a row.
type EncodeRowBuffer struct {
	// colIDs is the column ids for a row to be encoded.
	colIDs []int64
	// row is the column data for a row to be encoded.
	row []types.Datum
	// writeStmtBufs refs the `WriteStmtBufs` in session
	writeStmtBufs *variable.WriteStmtBufs
}

// Reset resets the inner buffers to a capacity.
func (b *EncodeRowBuffer) Reset(capacity int) {
	b.colIDs = ensureCapacityAndReset(b.colIDs, 0, capacity)
	b.row = ensureCapacityAndReset(b.row, 0, capacity)
}

// AddColVal adds a column value to the buffer.
func (b *EncodeRowBuffer) AddColVal(colID int64, val types.Datum) {
	b.colIDs = append(b.colIDs, colID)
	b.row = append(b.row, val)
}

// WriteMemBufferEncoded writes the encoded row to the memBuffer.
func (b *EncodeRowBuffer) WriteMemBufferEncoded(
	cfg RowEncodingConfig, loc *time.Location, ec errctx.Context,
	memBuffer kv.MemBuffer, key kv.Key, flags ...kv.FlagsOp,
) error {
	var checksum rowcodec.Checksum
	if cfg.IsRowLevelChecksumEnabled {
		checksum = rowcodec.RawChecksum{Key: key}
	}

	stmtBufs := b.writeStmtBufs

	// Adjust writeBufs.AddRowValues length, AddRowValues stores the inserting values that is used
	// by tablecodec.EncodeOldRow, the encoded row format is `id1, colval, id2, colval`,
	// so the correct length is rowLen * 2.
	// If the inserting row has null value,
	// AddRecord will skip it, so the rowLen will be different, so we need to adjust it.
	stmtBufs.AddRowValues = ensureCapacityAndReset(stmtBufs.AddRowValues, len(b.row)*2)

	encoded, err := tablecodec.EncodeRow(
		loc, b.row, b.colIDs, stmtBufs.RowValBuf, stmtBufs.AddRowValues, checksum, cfg.RowEncoder,
	)
	if err = ec.HandleError(err); err != nil {
		return err
	}
	stmtBufs.RowValBuf = encoded

	if len(flags) == 0 {
		return memBuffer.Set(key, encoded)
	}
	return memBuffer.SetWithFlags(key, encoded, flags...)
}

// EncodeBinlogRowData encodes the row data for binlog and returns the encoded row value.
// The returned slice is not referenced in the buffer, so you can cache and modify them freely.
func (b *EncodeRowBuffer) EncodeBinlogRowData(loc *time.Location, ec errctx.Context) ([]byte, error) {
	value, err := tablecodec.EncodeOldRow(loc, b.row, b.colIDs, nil, nil)
	err = ec.HandleError(err)
	if err != nil {
		return nil, err
	}
	return value, nil
}

// CheckRowBuffer is used to check row constraints
type CheckRowBuffer struct {
	rowToCheck []types.Datum
}

// GetRowToCheck gets the row data for constraint check.
func (b *CheckRowBuffer) GetRowToCheck() chunk.Row {
	return chunk.MutRowFromDatums(b.rowToCheck).ToRow()
}

// AddColVal adds a column value to the buffer for checking.
func (b *CheckRowBuffer) AddColVal(val types.Datum) {
	b.rowToCheck = append(b.rowToCheck, val)
}

// Reset resets the inner buffer to a capacity.
func (b *CheckRowBuffer) Reset(capacity int) {
	b.rowToCheck = ensureCapacityAndReset(b.rowToCheck, 0, capacity)
}

// ColSizeDeltaBuffer implements variable.DeltaCols
var _ variable.DeltaCols = &ColSizeDeltaBuffer{}

// ColSizeDeltaBuffer is a buffer to store the change of column size.
type ColSizeDeltaBuffer struct {
	delta []variable.ColSize
}

// Reset resets the inner buffers to a capacity.
func (b *ColSizeDeltaBuffer) Reset(capacity int) {
	b.delta = ensureCapacityAndReset(b.delta, 0, capacity)
}

// AddColSizeDelta adds the column size delta to the buffer.
func (b *ColSizeDeltaBuffer) AddColSizeDelta(colID int64, size int64) {
	b.delta = append(b.delta, variable.ColSize{ColID: colID, Size: size})
}

// UpdateColSizeMap updates the column size map which uses columID as the map key and column size as the value.
func (b *ColSizeDeltaBuffer) UpdateColSizeMap(m map[int64]int64) map[int64]int64 {
	if m == nil && len(b.delta) > 0 {
		m = make(map[int64]int64, len(b.delta))
	}
	for _, delta := range b.delta {
		m[delta.ColID] += delta.Size
	}
	return m
}

// MutateBuffers is a memory pool for table related memory allocation that aims to reuse memory
// and saves allocation.
// It is used in table operations like AddRecord/UpdateRecord/DeleteRecord.
// You can use `GetXXXBufferWithCap` to get the buffer and reset its inner slices to a capacity.
// Because inner slices are reused, you should not call the get methods again before finishing the previous usage.
// Otherwise, the previous data will be overwritten.
type MutateBuffers struct {
	stmtBufs     *variable.WriteStmtBufs
	encodeRow    *EncodeRowBuffer
	checkRow     *CheckRowBuffer
	colSizeDelta *ColSizeDeltaBuffer
}

// NewMutateBuffers creates a new `MutateBuffers`.
func NewMutateBuffers(stmtBufs *variable.WriteStmtBufs) *MutateBuffers {
	intest.AssertNotNil(stmtBufs)
	return &MutateBuffers{
		stmtBufs: stmtBufs,
		encodeRow: &EncodeRowBuffer{
			writeStmtBufs: stmtBufs,
		},
		checkRow:     &CheckRowBuffer{},
		colSizeDelta: &ColSizeDeltaBuffer{},
	}
}

// GetEncodeRowBufferWithCap gets the buffer to encode a row.
// Usage:
// 1. Call `MutateBuffers.GetEncodeRowBufferWithCap` to get the buffer.
// 2. Call `EncodeRowBuffer.AddColVal` for every column to add column values.
// 3. Call `EncodeRowBuffer.WriteMemBufferEncoded` to encode row and write it to the memBuffer.
// Because the inner slices are reused, you should not call this method again before finishing the previous usage.
// Otherwise, the previous data will be overwritten.
func (b *MutateBuffers) GetEncodeRowBufferWithCap(capacity int) *EncodeRowBuffer {
	buffer := b.encodeRow
	buffer.Reset(capacity)
	return buffer
}

// GetCheckRowBufferWithCap gets the buffer to check row constraints.
// Usage:
// 1. Call `GetCheckRowBufferWithCap` to get the buffer.
// 2. Call `CheckRowBuffer.AddColVal` for every column to add column values.
// 3. Call `CheckRowBuffer.GetRowToCheck` to get the row data for constraint check.
// Because the inner slices are reused, you should not call this method again before finishing the previous usage.
// Otherwise, the previous data will be overwritten.
func (b *MutateBuffers) GetCheckRowBufferWithCap(capacity int) *CheckRowBuffer {
	buffer := b.checkRow
	buffer.Reset(capacity)
	return buffer
}

// GetColSizeDeltaBufferWithCap gets the buffer for column size delta collection
// and resets the capacity of its inner slice.
// Usage:
// 1. Call `GetColSizeDeltaBufferWithCap` to get the buffer.
// 2. Call `ColSizeDeltaBuffer.AddColSizeDelta` for every column to add column size delta.
// 3. Call `ColSizeDeltaBuffer.UpdateColSizeMap` to update a column size map.
// Because the inner slices are reused, you should not call this method again before finishing the previous usage.
// Otherwise, the previous data will be overwritten.
func (b *MutateBuffers) GetColSizeDeltaBufferWithCap(capacity int) *ColSizeDeltaBuffer {
	buffer := b.colSizeDelta
	buffer.Reset(capacity)
	return buffer
}

// GetWriteStmtBufs returns the `*variable.WriteStmtBufs`
func (b *MutateBuffers) GetWriteStmtBufs() *variable.WriteStmtBufs {
	return b.stmtBufs
}

// ensureCapacityAndReset is similar to the built-in make(),
// but it reuses the given slice if it has enough capacity.
func ensureCapacityAndReset[T any](slice []T, size int, optCap ...int) []T {
	capacity := size
	if len(optCap) > 0 {
		capacity = optCap[0]
	}
	if cap(slice) < capacity {
		return make([]T, size, capacity)
	}
	return slice[:size]
}
