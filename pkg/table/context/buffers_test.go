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
	"testing"
	"time"

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

func TestEncodeRow(t *testing.T) {
	buffer := &encodeRowBuffer{}
	tm := types.NewTime(
		types.FromDate(2021, 1, 1, 1, 2, 3, 4),
		mysql.TypeDatetime, 6,
	)
	d1 := types.NewBytesDatum([]byte{1, 2, 3})
	d2 := types.NewIntDatum(20)
	d3 := types.NewTimeDatum(tm)
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
		cfg := RowEncodingConfig{
			RowEncoder:                &rowcodec.Encoder{Enable: !c.oldFormat},
			IsRowLevelChecksumEnabled: c.rowLevelChecksum,
		}

		var checksum rowcodec.Checksum
		if cfg.IsRowLevelChecksumEnabled {
			checksum = rowcodec.RawChecksum{Key: kv.Key("key1")}
		}

		expectedVal, err := tablecodec.EncodeRow(
			c.loc, []types.Datum{d1, d2, d3}, []int64{1, 2, 3}, nil, nil, checksum,
			&rowcodec.Encoder{Enable: !c.oldFormat},
		)
		require.NoError(t, err)

		memBuffer := &mockMemBuffer{}
		if len(c.flags) == 0 {
			memBuffer.On("Set", kv.Key("key1"), expectedVal).Return(nil).Once()
		} else {
			memBuffer.On("SetWithFlags", kv.Key("key1"), expectedVal, c.flags).Return(nil).Once()
		}
		err = buffer.WriteMemBufferEncoded(cfg, time.UTC, errctx.StrictNoWarningContext, memBuffer, kv.Key("key1"), c.flags...)
		require.NoError(t, err)
		memBuffer.AssertExpectations(t)

		// the encoding result should be cached as a buffer
		require.Equal(t, expectedVal, buffer.encoded)
	}
}

func TestEncodeBufferReserve(t *testing.T) {
	for _, item := range []any{&AddRecordBuffer{}, &UpdateRecordBuffer{}} {
		var buffer *encodeRowBuffer
		var reset func(int)
		var encode func()
		cfg := RowEncodingConfig{
			RowEncoder: &rowcodec.Encoder{Enable: true},
		}
		mb := &mockMemBuffer{}
		mb.On("Set", kv.Key("key1"), mock.Anything).Return(nil).Once()
		switch b := item.(type) {
		case *UpdateRecordBuffer:
			reset = b.reset
			buffer = &b.encodeRowBuffer
			encode = func() {
				err := buffer.WriteMemBufferEncoded(cfg, time.UTC, errctx.StrictNoWarningContext, mb, kv.Key("key1"))
				require.NoError(t, err)
				mb.AssertExpectations(t)
			}
		case *AddRecordBuffer:
			reset = b.reset
			buffer = &b.encodeRowBuffer
			encode = func() {
				err := buffer.WriteMemBufferEncoded(cfg, time.UTC, errctx.StrictNoWarningContext, mb, kv.Key("key1"))
				require.NoError(t, err)
				mb.AssertExpectations(t)
			}
		}
		reset(6)
		// data buffer should be reset to the capacity and length is 0
		require.Equal(t, 6, cap(buffer.colIDs))
		require.Equal(t, 0, len(buffer.colIDs))
		require.Equal(t, 6, cap(buffer.row))
		require.Equal(t, 0, len(buffer.row))
		// tempValues and encoded should be postponed to encode phase to reset
		require.Equal(t, 0, cap(buffer.tempValues))
		require.Equal(t, 0, cap(buffer.encoded))

		// add some data and encode
		buffer.AddColVal(1, types.NewIntDatum(1))
		buffer.AddColVal(2, types.NewIntDatum(2))
		require.Equal(t, 2, len(buffer.colIDs))
		require.Equal(t, 2, len(buffer.row))
		encode()
		encodedCap := cap(buffer.encoded)
		require.Greater(t, encodedCap, 0)
		require.Equal(t, 4, len(buffer.tempValues))

		// GetColDataBuffer should return the underlying buffer
		if b, ok := item.(*AddRecordBuffer); ok {
			colIDs, row := b.GetColDataBuffer()
			require.Equal(t, buffer.colIDs, colIDs)
			require.Equal(t, buffer.row, row)
		}

		// reset should not shrink the capacity
		reset(2)
		require.Equal(t, 6, cap(buffer.colIDs))
		require.Equal(t, 0, len(buffer.colIDs))
		require.Equal(t, 6, cap(buffer.row))
		require.Equal(t, 0, len(buffer.row))
		require.Equal(t, 4, cap(buffer.tempValues))
		require.Equal(t, encodedCap, cap(buffer.encoded))
	}
}

func TestUpdateRecordRowToCheckBuffer(t *testing.T) {
	buffer := &UpdateRecordBuffer{}
	buffer.Reset(6)
	require.Equal(t, 0, len(buffer.rowToCheck))
	require.Equal(t, 6, cap(buffer.rowToCheck))
	buffer.AddColValToCheck(types.NewIntDatum(1))
	buffer.AddColValToCheck(types.NewIntDatum(2))
	require.Equal(t, []types.Datum{types.NewIntDatum(1), types.NewIntDatum(2)}, buffer.rowToCheck)
	require.Equal(t, buffer.rowToCheck, buffer.GetRowToCheck())

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
	buffer.AddColSizeDelta(1, 2)
	buffer.AddColSizeDelta(3, 4)
	require.Equal(t, []variable.ColSize{{ColID: 1, Size: 2}, {ColID: 3, Size: 4}}, buffer.delta)
	require.Equal(t, buffer.delta, buffer.GetColSizeDelta())

	// reset should not shrink the capacity
	buffer.Reset(2)
	require.Equal(t, 0, len(buffer.delta))
	require.Equal(t, 6, cap(buffer.delta))
}

func TestMutateBuffersGetter(t *testing.T) {
	buffers := NewMutateBuffers()
	add := buffers.GetAddRecordBufferWithCap(6)
	require.Equal(t, 6, cap(add.row))
	update := buffers.GetUpdateRecordBufferWithCap(6)
	require.Equal(t, 6, cap(update.row))
	require.Equal(t, 6, cap(update.rowToCheck))
	colSize := buffers.GetColSizeDeltaBufferWithCap(6)
	require.Equal(t, 6, cap(colSize.delta))
}
