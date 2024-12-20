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

package tblctx

import (
	"testing"
	"time"
	"unsafe"

	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockMemBuffer struct {
	kv.MemBuffer
	mock.Mock
}

func (b *mockMemBuffer) SetWithFlags(key kv.Key, value []byte, flags ...kv.FlagsOp) error {
	args := b.Called(key, value, flags)
	return args.Error(0)
}

func (b *mockMemBuffer) Set(key kv.Key, value []byte) error {
	args := b.Called(key, value)
	return args.Error(0)
}

type mockMutateCtx struct {
	MutateContext
	buffers *MutateBuffers
}

func (ctx *mockMutateCtx) GetMutateBuffers() *MutateBuffers {
	return ctx.buffers
}

func newMockMutateCtx() (*variable.WriteStmtBufs, *mockMutateCtx) {
	stmtBufs := &variable.WriteStmtBufs{}
	ctx := &mockMutateCtx{
		buffers: NewMutateBuffers(stmtBufs),
	}
	return stmtBufs, ctx
}

func TestEncodeRow(t *testing.T) {
	stmtBufs, ctx := newMockMutateCtx()
	tm := types.NewTime(
		types.FromDate(2021, 1, 1, 1, 2, 3, 4),
		mysql.TypeTimestamp, 6,
	)
	d1 := types.NewBytesDatum([]byte{1, 2, 3})
	d2 := types.NewIntDatum(20)
	d3 := types.NewTimeDatum(tm)
	buffer := ctx.GetMutateBuffers().GetEncodeRowBufferWithCap(3)
	require.Same(t, stmtBufs, buffer.writeStmtBufs)
	buffer.AddColVal(1, d1)
	buffer.AddColVal(2, d2)
	buffer.AddColVal(3, d3)
	require.Equal(t, []int64{1, 2, 3}, buffer.colIDs)
	require.Equal(t, []types.Datum{d1, d2, d3}, buffer.row)

	for _, c := range []struct {
		loc              *time.Location
		rowLevelChecksum bool
		oldFormat        bool
		flags            []kv.FlagsOp
	}{
		{
			loc: time.UTC,
		},
		{
			loc:              time.FixedZone("fixed1", 3600),
			rowLevelChecksum: true,
			flags:            []kv.FlagsOp{kv.SetPresumeKeyNotExists},
		},
		{
			loc:       time.FixedZone("fixed2", 3600*2),
			oldFormat: true,
		},
	} {
		// test encode and write to mem buffer
		cfg := RowEncodingConfig{
			RowEncoder:                &rowcodec.Encoder{Enable: !c.oldFormat},
			IsRowLevelChecksumEnabled: c.rowLevelChecksum,
		}

		var checksum rowcodec.Checksum
		if cfg.IsRowLevelChecksumEnabled {
			checksum = rowcodec.RawChecksum{Handle: kv.IntHandle(1)}
		}

		expectedVal, err := tablecodec.EncodeRow(
			c.loc, []types.Datum{d1, d2, d3}, []int64{1, 2, 3}, nil, nil, checksum,
			&rowcodec.Encoder{Enable: !c.oldFormat},
		)
		require.NoError(t, err)

		memBuffer := &mockMemBuffer{}
		if len(c.flags) == 0 {
			memBuffer.On("Set", kv.Key("key1"), expectedVal).
				Return(nil).Once()
		} else {
			memBuffer.On("SetWithFlags", kv.Key("key1"), expectedVal, c.flags).
				Return(nil).Once()
		}
		err = buffer.WriteMemBufferEncoded(
			cfg, c.loc, errctx.StrictNoWarningContext,
			memBuffer, kv.Key("key1"), kv.IntHandle(1), c.flags...,
		)
		require.NoError(t, err)
		memBuffer.AssertExpectations(t)
		// the encoding result should be cached as a buffer
		require.Equal(t, expectedVal, buffer.writeStmtBufs.RowValBuf)

		// test encode val for binlog
		expectedVal, err =
			tablecodec.EncodeOldRow(c.loc, []types.Datum{d1, d2, d3}, []int64{1, 2, 3}, nil, nil)
		require.NoError(t, err)
		encoded, err := buffer.EncodeBinlogRowData(c.loc, errctx.StrictNoWarningContext)
		require.NoError(t, err)
		require.Equal(t, expectedVal, encoded)
		// the encoded should not be referenced by any inner buffer
		require.True(t, unsafe.SliceData(encoded) != unsafe.SliceData(buffer.writeStmtBufs.RowValBuf))
		require.True(t, unsafe.SliceData(encoded) != unsafe.SliceData(buffer.writeStmtBufs.IndexKeyBuf))
	}
}

func TestEncodeBufferReserve(t *testing.T) {
	stmtBufs, ctx := newMockMutateCtx()
	mb := &mockMemBuffer{}
	mb.On("Set", kv.Key("key1"), mock.Anything).Return(nil).Once()

	buffer := ctx.GetMutateBuffers().GetEncodeRowBufferWithCap(6)
	require.Same(t, ctx.buffers.encodeRow, buffer)
	require.Same(t, stmtBufs, buffer.writeStmtBufs)
	// data buffer should be reset to the capacity and length is 0
	require.Equal(t, 6, cap(buffer.colIDs))
	require.Equal(t, 0, len(buffer.colIDs))
	require.Equal(t, 6, cap(buffer.row))
	require.Equal(t, 0, len(buffer.row))

	// add some data and encode
	buffer.AddColVal(1, types.NewIntDatum(1))
	buffer.AddColVal(2, types.NewIntDatum(2))
	require.Equal(t, 2, len(buffer.colIDs))
	require.Equal(t, 2, len(buffer.row))
	require.NoError(t, buffer.WriteMemBufferEncoded(RowEncodingConfig{
		RowEncoder: &rowcodec.Encoder{Enable: true},
	}, time.UTC, errctx.StrictNoWarningContext, mb, kv.Key("key1"), kv.IntHandle(1)))
	encodedCap := cap(buffer.writeStmtBufs.RowValBuf)
	require.Greater(t, encodedCap, 0)
	require.Equal(t, 4, len(buffer.writeStmtBufs.AddRowValues))
	addRowValuesCap := cap(buffer.writeStmtBufs.AddRowValues)

	// reset should not shrink the capacity
	buffer.Reset(2)
	require.Equal(t, 6, cap(buffer.colIDs))
	require.Equal(t, 0, len(buffer.colIDs))
	require.Equal(t, 6, cap(buffer.row))
	require.Equal(t, 0, len(buffer.row))
	require.Equal(t, addRowValuesCap, cap(buffer.writeStmtBufs.AddRowValues))
	require.Equal(t, encodedCap, cap(buffer.writeStmtBufs.RowValBuf))
}

func TestCheckRowBuffer(t *testing.T) {
	buffer := &CheckRowBuffer{}
	buffer.Reset(6)
	require.Equal(t, 0, len(buffer.rowToCheck))
	require.Equal(t, 6, cap(buffer.rowToCheck))
	buffer.AddColVal(types.NewIntDatum(1))
	buffer.AddColVal(types.NewIntDatum(2))
	require.Equal(t, []types.Datum{types.NewIntDatum(1), types.NewIntDatum(2)}, buffer.rowToCheck)
	rowToCheck := buffer.GetRowToCheck()
	require.Equal(t, 2, rowToCheck.Len())
	require.Equal(t, int64(1), rowToCheck.GetInt64(0))
	require.Equal(t, int64(2), rowToCheck.GetInt64(1))

	// reset should not shrink the capacity
	buffer.Reset(2)
	require.Equal(t, 0, len(buffer.rowToCheck))
	require.Equal(t, 6, cap(buffer.rowToCheck))
}

func TestColSizeDeltaBuffer(t *testing.T) {
	buffer := &ColSizeDeltaBuffer{}
	buffer.Reset(6)
	require.Equal(t, 0, len(buffer.delta))
	require.Equal(t, 6, cap(buffer.delta))
	require.Nil(t, buffer.UpdateColSizeMap(nil))

	buffer.AddColSizeDelta(1, 2)
	buffer.AddColSizeDelta(3, -4)
	buffer.AddColSizeDelta(10, 11)
	require.Equal(t, []variable.ColSize{{ColID: 1, Size: 2}, {ColID: 3, Size: -4}, {ColID: 10, Size: 11}}, buffer.delta)

	require.Equal(t, map[int64]int64{1: 2, 3: -4, 10: 11}, buffer.UpdateColSizeMap(nil))
	m := make(map[int64]int64)
	m2 := buffer.UpdateColSizeMap(m)
	require.Equal(t, map[int64]int64{1: 2, 3: -4, 10: 11}, m2)
	require.Equal(t, m2, m)

	m = map[int64]int64{1: 3, 3: 5, 5: 7}
	m2 = buffer.UpdateColSizeMap(m)
	require.Equal(t, map[int64]int64{1: 5, 3: 1, 5: 7, 10: 11}, m2)
	require.Equal(t, m2, m)

	// reset should not shrink the capacity
	buffer.Reset(2)
	require.Equal(t, 0, len(buffer.delta))
	require.Equal(t, 6, cap(buffer.delta))
}

func TestMutateBuffersGetter(t *testing.T) {
	stmtBufs := &variable.WriteStmtBufs{}
	buffers := NewMutateBuffers(stmtBufs)
	add := buffers.GetEncodeRowBufferWithCap(6)
	require.Equal(t, 6, cap(add.row))
	require.Same(t, stmtBufs, add.writeStmtBufs)

	update := buffers.GetCheckRowBufferWithCap(6)
	require.Equal(t, 6, cap(update.rowToCheck))
	require.Equal(t, 6, cap(update.rowToCheck))

	colSize := buffers.GetColSizeDeltaBufferWithCap(6)
	require.Equal(t, 6, cap(colSize.delta))

	require.Same(t, stmtBufs, buffers.GetWriteStmtBufs())
}

func TestEnsureCapacityAndReset(t *testing.T) {
	slice := ensureCapacityAndReset([]int(nil), 0)
	require.Nil(t, slice)
	slice = ensureCapacityAndReset([]int{}, 0)
	require.Equal(t, []int{}, slice)

	input := []int{1, 2, 3}
	slice = ensureCapacityAndReset(input, 0)
	require.Equal(t, 0, len(slice))
	require.Equal(t, 3, cap(slice))
	// share the same underlying array
	slice[:3][2] = 4
	require.Equal(t, []int{1, 2, 4}, input)

	input = []int{1, 2, 3}
	slice = ensureCapacityAndReset(input, 2)
	require.Equal(t, 2, len(slice))
	require.Equal(t, 3, cap(slice))
	// share the same underlying array
	slice[1] = 5
	require.Equal(t, []int{1, 5, 3}, input)

	input = []int{1, 2, 3}
	slice = ensureCapacityAndReset(input, 4)
	require.Equal(t, 4, len(slice))
	require.Equal(t, 4, cap(slice))

	input = []int{1, 2, 3}
	slice = ensureCapacityAndReset(input, 1, 2)
	require.Equal(t, 1, len(slice))
	// if cap < originalCap, keep the original capacity
	require.Equal(t, 3, cap(slice))
	// share the same underlying array
	slice[0] = 10
	require.Equal(t, []int{10, 2, 3}, input)

	input = []int{1, 2, 3}
	slice = ensureCapacityAndReset(input, 2, 4)
	require.Equal(t, 2, len(slice))
	require.Equal(t, 4, cap(slice))

	input = []int{1, 2, 3}
	slice = ensureCapacityAndReset(input, 4, 5)
	require.Equal(t, 4, len(slice))
	require.Equal(t, 5, cap(slice))
}
