// Copyright 2026 PingCAP, Inc.
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

package tablecodec

import (
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/stretchr/testify/require"
)

// fullTextTestKey is the key of a non-unique index entry over the term "term"
// for int handle 42.
func fullTextTestKey(t *testing.T) []byte {
	encoded, err := codec.EncodeKey(time.UTC, nil, types.NewBytesDatum([]byte("term")), types.NewIntDatum(42))
	require.NoError(t, err)
	return EncodeIndexSeekKey(1, 2, encoded)
}

func TestTiKVFullTextIndexValueRoundTrip(t *testing.T) {
	key := fullTextTestKey(t)
	for _, positions := range [][]int{
		{},
		{0},
		{3},
		{0, 1, 2, 3},
		{7, 300, 301, 70000, 1 << 30},
	} {
		encoded := EncodeTiKVFullTextPositions(nil, positions)
		value := EncodeTiKVFullTextIndexValue(nil, encoded, false)
		require.GreaterOrEqual(t, len(value), tikvFullTextValueMinLen)
		decoded, err := DecodeTiKVFullTextIndexValue(value)
		require.NoError(t, err)
		require.Equal(t, positions, decoded)

		untouched := EncodeTiKVFullTextIndexValue(nil, encoded, true)
		decoded, err = DecodeTiKVFullTextIndexValue(untouched)
		require.NoError(t, err)
		require.Equal(t, positions, decoded)
		require.True(t, IsUntouchedIndexKValue(key, untouched))
		require.False(t, IsUntouchedIndexKValue(key, value))
	}
}

// TestTiKVFullTextIndexValueIsInertToOtherDecoders pins the properties the
// layout relies on: the existing decoders find no handle, partition or restored
// data in it, and never mistake it for a unique-index or old-layout value.
func TestTiKVFullTextIndexValueIsInertToOtherDecoders(t *testing.T) {
	// A short position list and a long one: the padding path and the
	// unpadded path.
	for _, positions := range [][]int{{1}, {1, 5, 9, 200, 201, 202, 203, 204, 205, 206}} {
		value := EncodeTiKVFullTextIndexValue(nil, EncodeTiKVFullTextPositions(nil, positions), false)
		require.Greater(t, len(value), MaxOldEncodeValueLen)
		require.Equal(t, 0, getIndexVersion(value))
		segs := SplitIndexValue(value)
		require.Empty(t, segs.IntHandle)
		require.Empty(t, segs.CommonHandle)
		require.Empty(t, segs.PartitionID)
		require.Empty(t, segs.RestoredValues)
		require.False(t, IndexKVIsUnique(value))

		// The handle of a non-unique index entry comes from the key, and the
		// value does not change that.
		key := fullTextTestKey(t)
		require.False(t, IsUntouchedIndexKValue(key, value))
		handle, err := DecodeIndexHandle(key, value, 1)
		require.NoError(t, err)
		require.Equal(t, int64(42), handle.IntValue())
	}

	// The value survives the temp-index wrapping used during ADD INDEX.
	value := EncodeTiKVFullTextIndexValue(nil, EncodeTiKVFullTextPositions(nil, []int{4, 8}), false)
	elem := TempIndexValueElem{Value: value, KeyVer: TempIndexKeyTypeBackfill}
	wrapped := elem.Encode(nil)
	decoded, err := DecodeTempIndexValue(wrapped)
	require.NoError(t, err)
	require.Equal(t, value, decoded.Current().Value)

	_, err = DecodeTiKVFullTextIndexValue([]byte{'0'})
	require.Error(t, err)
	_, err = DecodeTiKVFullTextIndexValue(append([]byte{0, 0x7f}, make([]byte, 8)...))
	require.Error(t, err)
	require.Equal(t, byte('1'), kv.UnCommitIndexKVFlag)
}
