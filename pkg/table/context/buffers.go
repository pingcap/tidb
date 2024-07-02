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
	"github.com/pingcap/tidb/pkg/util/rowcodec"
)

type encodeRowBuffer struct {
	// colIDs is the column ids for a row to be encoded.
	colIDs []int64
	// row is the column data for a row to be encoded.
	row []types.Datum
	// encoded is the buffer for the encoding result.
	encoded []byte
	// tempValues is the buffer used temporary when encoding a row with old format.
	tempValues []types.Datum
}

// Reset resets the inner buffers to a capacity.
func (b *encodeRowBuffer) reset(capacity int) {
	b.colIDs = ensureCapacityAndReset(b.colIDs, 0, capacity)
	b.row = ensureCapacityAndReset(b.row, 0, capacity)
}

// AddColVal adds a column value to the buffer.
func (b *encodeRowBuffer) AddColVal(colID int64, val types.Datum) {
	b.colIDs = append(b.colIDs, colID)
	b.row = append(b.row, val)
}

// WriteMemBufferEncoded writes the encoded row to the memBuffer.
func (b *encodeRowBuffer) WriteMemBufferEncoded(
	cfg RowEncodingConfig, loc *time.Location, ec errctx.Context,
	memBuffer kv.MemBuffer, key kv.Key, flags ...kv.FlagsOp,
) error {
	var checksum rowcodec.Checksum
	if cfg.IsRowLevelChecksumEnabled {
		checksum = rowcodec.RawChecksum{Key: key}
	}

	b.tempValues = ensureCapacityAndReset(b.tempValues, len(b.row)*2)
	encoded, err := tablecodec.EncodeRow(
		loc, b.row, b.colIDs, b.encoded[:0], b.tempValues, checksum, cfg.RowEncoder,
	)
	if err = ec.HandleError(err); err != nil {
		return err
	}
	b.encoded = encoded

	if len(flags) == 0 {
		return memBuffer.Set(key, b.encoded)
	}
	return memBuffer.SetWithFlags(key, b.encoded, flags...)
}

// AddRecordBuffer is the buffer for AddRecord operation.
type AddRecordBuffer struct {
	encodeRowBuffer
}

// GetColDataBuffer returns the buffer for column data.
// TODO: make sure the inner buffer is not used outside directly.
func (b *AddRecordBuffer) GetColDataBuffer() ([]int64, []types.Datum) {
	return b.colIDs, b.row
}

// Reset resets the inner buffers to a capacity.
func (b *AddRecordBuffer) Reset(capacity int) {
	b.encodeRowBuffer.reset(capacity)
}

// UpdateRecordBuffer is the buffer for UpdateRecord operation.
type UpdateRecordBuffer struct {
	encodeRowBuffer
	rowToCheck []types.Datum
}

// GetRowToCheck gets the row data for constraint check.
// TODO: make sure the inner buffer is not used outside directly.
func (b *UpdateRecordBuffer) GetRowToCheck() []types.Datum {
	return b.rowToCheck
}

// AddColValToCheck adds a column value to the buffer for checking.
func (b *UpdateRecordBuffer) AddColValToCheck(val types.Datum) {
	b.rowToCheck = append(b.rowToCheck, val)
}

// Reset resets the inner buffers to a capacity.
func (b *UpdateRecordBuffer) Reset(capacity int) {
	b.encodeRowBuffer.reset(capacity)
	b.rowToCheck = ensureCapacityAndReset(b.rowToCheck, 0, capacity)
}

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

// GetColSizeDelta gets the column size delta.
// TODO: make sure the inner buffer is not used outside directly.
func (b *ColSizeDeltaBuffer) GetColSizeDelta() []variable.ColSize {
	return b.delta
}

// MutateBuffers is used to get the buffers for table mutating.
type MutateBuffers struct {
	addRecord    *AddRecordBuffer
	update       *UpdateRecordBuffer
	colSizeDelta *ColSizeDeltaBuffer
}

// NewMutateBuffers creates a new `MutateBuffers`.
func NewMutateBuffers() *MutateBuffers {
	return &MutateBuffers{
		addRecord:    &AddRecordBuffer{},
		update:       &UpdateRecordBuffer{},
		colSizeDelta: &ColSizeDeltaBuffer{},
	}
}

// GetAddRecordBufferWithCap gets the buffer for AddRecord operation and resets the capacity of its inner slices.
func (b *MutateBuffers) GetAddRecordBufferWithCap(capacity int) *AddRecordBuffer {
	buffer := b.addRecord
	buffer.Reset(capacity)
	return buffer
}

// GetUpdateRecordBufferWithCap gets the buffer for AddRecord operation and resets the capacity of its inner slices.
func (b *MutateBuffers) GetUpdateRecordBufferWithCap(capacity int) *UpdateRecordBuffer {
	buffer := b.update
	buffer.Reset(capacity)
	return buffer
}

// GetColSizeDeltaBufferWithCap gets the buffer for column size delta collection
// and resets the capacity of its inner slice.
func (b *MutateBuffers) GetColSizeDeltaBufferWithCap(capacity int) *ColSizeDeltaBuffer {
	buffer := b.colSizeDelta
	buffer.Reset(capacity)
	return buffer
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
