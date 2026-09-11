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

package distsql

import (
	"bytes"
	"math"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"github.com/stretchr/testify/require"
)

const unsignedHandleSuffixTableID int64 = 71

// unsignedHandleSuffixTable builds `create table t(id bigint unsigned primary key
// clustered, a int, key ia(a))`, whose ia keys end with the handle in its physical
// int-handle encoding.
func unsignedHandleSuffixTable(unsignedPK bool, uniqueIdx bool) (*model.TableInfo, *model.IndexInfo) {
	pkType := types.NewFieldType(mysql.TypeLonglong)
	pkType.AddFlag(mysql.PriKeyFlag | mysql.NotNullFlag)
	if unsignedPK {
		pkType.AddFlag(mysql.UnsignedFlag)
	}
	idxInfo := &model.IndexInfo{
		ID:      3,
		Name:    ast.NewCIStr("ia"),
		Columns: []*model.IndexColumn{{Name: ast.NewCIStr("a"), Offset: 1, Length: types.UnspecifiedLength}},
		State:   model.StatePublic,
		Unique:  uniqueIdx,
	}
	tblInfo := &model.TableInfo{
		ID:         unsignedHandleSuffixTableID,
		Name:       ast.NewCIStr("t"),
		PKIsHandle: true,
		Columns: []*model.ColumnInfo{
			{ID: 1, Name: ast.NewCIStr("id"), Offset: 0, FieldType: *pkType, State: model.StatePublic},
			{ID: 2, Name: ast.NewCIStr("a"), Offset: 1, FieldType: *types.NewFieldType(mysql.TypeLong), State: model.StatePublic},
		},
		Indices: []*model.IndexInfo{idxInfo},
	}
	return tblInfo, idxInfo
}

func TestUnsignedIntHandleSuffixDim(t *testing.T) {
	tblInfo, idxInfo := unsignedHandleSuffixTable(true, false)
	require.Equal(t, 1, UnsignedIntHandleSuffixDim(tblInfo, idxInfo))

	// A signed handle is already stored the way codec.EncodeKey encodes a KindInt64
	// datum, so its ranges need no rewrite.
	signedTbl, signedIdx := unsignedHandleSuffixTable(false, false)
	require.Equal(t, NoIntHandleSuffix, UnsignedIntHandleSuffixDim(signedTbl, signedIdx))

	// A unique index stores the handle in its value, not its key.
	uniqueTbl, uniqueIdx := unsignedHandleSuffixTable(true, true)
	require.Equal(t, NoIntHandleSuffix, UnsignedIntHandleSuffixDim(uniqueTbl, uniqueIdx))

	// A table with no int handle at all.
	noPKTbl, noPKIdx := unsignedHandleSuffixTable(true, false)
	noPKTbl.PKIsHandle = false
	require.Equal(t, NoIntHandleSuffix, UnsignedIntHandleSuffixDim(noPKTbl, noPKIdx))
}

// indexRange builds the `a = av AND handle IN [low, high]` index range that ranger would
// produce for an unsigned handle appended to a single-column index.
func indexRange(av int64, low, high uint64, lowExclude, highExclude bool) *ranger.Range {
	return &ranger.Range{
		LowVal:      []types.Datum{types.NewIntDatum(av), types.NewUintDatum(low)},
		HighVal:     []types.Datum{types.NewIntDatum(av), types.NewUintDatum(high)},
		LowExclude:  lowExclude,
		HighExclude: highExclude,
		Collators:   collate.GetBinaryCollatorSlice(2),
	}
}

// coveredHandles returns which of handles the key ranges select, by generating the real
// index key of every (av, handle) row and testing it against the ranges.
func coveredHandles(t *testing.T, keyRanges []kv.KeyRange, tblInfo *model.TableInfo, idxInfo *model.IndexInfo, av int64, handles []uint64) []uint64 {
	t.Helper()
	covered := make([]uint64, 0, len(handles))
	for _, h := range handles {
		key, _, err := tablecodec.GenIndexKey(codec.Encoder{}, time.UTC, tblInfo, idxInfo,
			tblInfo.ID, []types.Datum{types.NewIntDatum(av)}, kv.IntHandle(int64(h)), nil)
		require.NoError(t, err)
		for _, kr := range keyRanges {
			if bytes.Compare(kr.StartKey, key) <= 0 && bytes.Compare(key, kr.EndKey) < 0 {
				covered = append(covered, h)
				break
			}
		}
	}
	return covered
}

// TestUnsignedIntHandleSuffixKVRanges checks the key ranges built for an appended unsigned
// handle against the index keys tablecodec actually writes, including handles above
// math.MaxInt64, which wrap to negative int64 in the index key.
func TestUnsignedIntHandleSuffixKVRanges(t *testing.T) {
	tblInfo, idxInfo := unsignedHandleSuffixTable(true, false)
	handleDim := UnsignedIntHandleSuffixDim(tblInfo, idxInfo)
	// Handles on both sides of the boundary, in SQL order.
	allHandles := []uint64{0, 7, 11, 22, math.MaxInt64 - 1, math.MaxInt64, math.MaxInt64 + 1, math.MaxInt64 + 3, math.MaxUint64}

	cases := []struct {
		name     string
		ranges   []*ranger.Range
		expected []uint64
	}{
		{
			name:     "point below the boundary",
			ranges:   []*ranger.Range{indexRange(5, 7, 7, false, false)},
			expected: []uint64{7},
		},
		{
			name:     "point above the boundary",
			ranges:   []*ranger.Range{indexRange(5, math.MaxInt64+3, math.MaxInt64+3, false, false)},
			expected: []uint64{math.MaxInt64 + 3},
		},
		{
			name:     "point at the maximum handle",
			ranges:   []*ranger.Range{indexRange(5, math.MaxUint64, math.MaxUint64, false, false)},
			expected: []uint64{math.MaxUint64},
		},
		{
			name:     "closed range below the boundary",
			ranges:   []*ranger.Range{indexRange(5, 7, 22, false, false)},
			expected: []uint64{7, 11, 22},
		},
		{
			name:     "exclusive bounds",
			ranges:   []*ranger.Range{indexRange(5, 7, 22, true, true)},
			expected: []uint64{11},
		},
		{
			name:     "open range crossing the boundary",
			ranges:   []*ranger.Range{indexRange(5, 11, math.MaxUint64, true, false)},
			expected: []uint64{22, math.MaxInt64 - 1, math.MaxInt64, math.MaxInt64 + 1, math.MaxInt64 + 3, math.MaxUint64},
		},
		{
			name:     "closed range straddling the boundary",
			ranges:   []*ranger.Range{indexRange(5, math.MaxInt64, math.MaxInt64+1, false, false)},
			expected: []uint64{math.MaxInt64, math.MaxInt64 + 1},
		},
		{
			name:     "range entirely above the boundary",
			ranges:   []*ranger.Range{indexRange(5, math.MaxInt64+1, math.MaxUint64, false, false)},
			expected: []uint64{math.MaxInt64 + 1, math.MaxInt64 + 3, math.MaxUint64},
		},
		{
			name: "IN list mixing both sides",
			ranges: []*ranger.Range{
				indexRange(5, 11, 11, false, false),
				indexRange(5, 22, 22, false, false),
				indexRange(5, math.MaxInt64+1, math.MaxInt64+1, false, false),
			},
			expected: []uint64{11, 22, math.MaxInt64 + 1},
		},
		{
			name: "unbounded handle covers the whole prefix",
			ranges: []*ranger.Range{{
				LowVal:    []types.Datum{types.NewIntDatum(5), types.MinNotNullDatum()},
				HighVal:   []types.Datum{types.NewIntDatum(5), types.MaxValueDatum()},
				Collators: collate.GetBinaryCollatorSlice(2),
			}},
			expected: allHandles,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			keyRanges, err := IndexRangesToKVRanges(DefaultDistSQLContext, tblInfo.ID, idxInfo.ID, c.ranges, handleDim)
			require.NoError(t, err)
			flat := keyRanges.FirstPartitionRange()
			// The coprocessor expects key ranges in key order.
			for i := 1; i < len(flat); i++ {
				require.LessOrEqual(t, bytes.Compare(flat[i-1].EndKey, flat[i].StartKey), 0,
					"key ranges must be sorted and disjoint")
			}
			require.Equal(t, c.expected, coveredHandles(t, flat, tblInfo, idxInfo, 5, allHandles))
			// Rows under a different declared-column prefix must never be selected.
			require.Empty(t, coveredHandles(t, flat, tblInfo, idxInfo, 6, allHandles))
		})
	}
}

// TestUnsignedIntHandleSuffixIgnoresShortRanges checks that ranges that stop at the
// declared index columns keep the encoding they had before the appended handle existed.
func TestUnsignedIntHandleSuffixIgnoresShortRanges(t *testing.T) {
	tblInfo, idxInfo := unsignedHandleSuffixTable(true, false)
	ranges := []*ranger.Range{{
		LowVal:    []types.Datum{types.NewIntDatum(5)},
		HighVal:   []types.Datum{types.NewIntDatum(5)},
		Collators: collate.GetBinaryCollatorSlice(1),
	}}
	withSuffix, err := IndexRangesToKVRanges(DefaultDistSQLContext, tblInfo.ID, idxInfo.ID, ranges,
		UnsignedIntHandleSuffixDim(tblInfo, idxInfo))
	require.NoError(t, err)
	withoutSuffix, err := IndexRangesToKVRanges(DefaultDistSQLContext, tblInfo.ID, idxInfo.ID, ranges, NoIntHandleSuffix)
	require.NoError(t, err)
	require.Equal(t, withoutSuffix.FirstPartitionRange(), withSuffix.FirstPartitionRange())
}
