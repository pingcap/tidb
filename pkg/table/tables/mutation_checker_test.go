// Copyright 2021 PingCAP, Inc.
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

package tables

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"github.com/stretchr/testify/require"
)

func TestCompareIndexData(t *testing.T) {
	// dimensions of the domain of compareIndexDataWithMVCache
	// 	 1. table structure, where we only care about column types that influence truncating values
	//	 2. comparison of row data & index data

	type caseData struct {
		indexData   []types.Datum
		inputData   []types.Datum
		fts         []*types.FieldType
		indexLength []int
		correct     bool
	}

	// assume the index is on all columns
	testData := []caseData{
		{
			[]types.Datum{types.NewIntDatum(1), types.NewStringDatum("some string")},
			[]types.Datum{types.NewIntDatum(1), types.NewStringDatum("some string")},
			[]*types.FieldType{types.NewFieldType(mysql.TypeShort), types.NewFieldType(mysql.TypeString)},
			[]int{types.UnspecifiedLength, types.UnspecifiedLength},
			true,
		},
		{
			[]types.Datum{types.NewIntDatum(1), types.NewStringDatum("some string")},
			[]types.Datum{types.NewIntDatum(1), types.NewStringDatum("some string2")},
			[]*types.FieldType{types.NewFieldType(mysql.TypeShort), types.NewFieldType(mysql.TypeString)},
			[]int{types.UnspecifiedLength, types.UnspecifiedLength},
			false,
		},
		{
			[]types.Datum{types.NewIntDatum(1), types.NewStringDatum("some string")},
			[]types.Datum{types.NewIntDatum(1), types.NewStringDatum("some string2")},
			[]*types.FieldType{types.NewFieldType(mysql.TypeShort), types.NewFieldType(mysql.TypeString)},
			[]int{types.UnspecifiedLength, 11},
			true,
		},
	}

	for caseID, data := range testData {
		tc := types.DefaultStmtNoWarningContext
		cols := make([]*table.Column, 0)
		indexCols := make([]*model.IndexColumn, 0)
		for i, ft := range data.fts {
			cols = append(cols, &table.Column{ColumnInfo: &model.ColumnInfo{Name: ast.NewCIStr(fmt.Sprintf("c%d", i)), FieldType: *ft}})
			indexCols = append(indexCols, &model.IndexColumn{Offset: i, Length: data.indexLength[i]})
		}
		indexInfo := &model.IndexInfo{Name: ast.NewCIStr("i0"), Columns: indexCols}

		err := compareIndexDataWithMVCache(collate.NewCollationEnabled(), tc, cols, data.indexData, data.inputData, indexInfo, &model.TableInfo{Name: ast.NewCIStr("t")}, nil, nil)
		require.Equal(t, data.correct, err == nil, "case id = %v", caseID)
	}

	// The cache of sorted array elements must reach the same verdict as the per-element scan.
	testMVIndexCompare(t)
}

// checkMVIndexWithAndWithoutCache runs the multi-valued index check with and without the sorted
// element cache and asserts that both paths reach the same verdict.
func checkMVIndexWithAndWithoutCache(t *testing.T, tc types.Context, cols []*table.Column,
	indexInfo *model.IndexInfo, tableInfo *model.TableInfo, indexData, row []types.Datum,
) error {
	t.Helper()
	withoutCache := compareIndexDataWithMVCache(
		true, tc, cols, indexData, row, indexInfo, tableInfo, nil, nil)
	// Start from a zero cache to also cover the lazy allocation in compareMVIndexVal.
	var cache mvIndexSortedElems
	for range 3 {
		// The first iteration populates the cache, the others hit it.
		withCache := compareIndexDataWithMVCache(
			true, tc, cols, indexData, row, indexInfo, tableInfo, nil, &cache)
		require.Equal(t, withoutCache == nil, withCache == nil,
			"cached and uncached multi-valued index checks disagreed")
	}
	return withoutCache
}

func testMVIndexCompare(t *testing.T) {
	tc := types.DefaultStmtNoWarningContext

	arrayFT := types.NewFieldType(mysql.TypeJSON)
	arrayFT.SetArray(true)
	cols := []*table.Column{{ColumnInfo: &model.ColumnInfo{
		ID: 1, Name: ast.NewCIStr("arr"), Offset: 0, State: model.StatePublic, FieldType: *arrayFT,
	}}}
	indexInfo := &model.IndexInfo{
		ID: 1, Name: ast.NewCIStr("idx_mv"), State: model.StatePublic, MVIndex: true,
		Columns: []*model.IndexColumn{
			{Name: ast.NewCIStr("arr"), Offset: 0, Length: types.UnspecifiedLength},
		},
	}
	tableInfo := &model.TableInfo{Name: ast.NewCIStr("t")}
	newJSONRow := func(s string) []types.Datum {
		bj, err := types.ParseBinaryJSONFromString(s)
		require.NoError(t, err)
		return []types.Datum{types.NewJSONDatum(bj)}
	}

	t.Run("mv index/hit", func(t *testing.T) {
		elems := make([]string, 0, 500)
		for i := range 500 {
			elems = append(elems, fmt.Sprintf("%d", i))
		}
		row := newJSONRow("[" + strings.Join(elems, ",") + "]")
		err := checkMVIndexWithAndWithoutCache(t, tc, cols, indexInfo, tableInfo,
			[]types.Datum{types.NewIntDatum(499)}, row)
		require.NoError(t, err)
	})
	t.Run("mv index/miss", func(t *testing.T) {
		row := newJSONRow("[3, 1, 2, 2]")
		err := checkMVIndexWithAndWithoutCache(t, tc, cols, indexInfo, tableInfo,
			[]types.Datum{types.NewIntDatum(4)}, row)
		require.Error(t, err)
		require.True(t, ErrInconsistentIndexedValue.Equal(err))
	})
	t.Run("mv index/empty array", func(t *testing.T) {
		row := newJSONRow("[]")
		err := checkMVIndexWithAndWithoutCache(t, tc, cols, indexInfo, tableInfo,
			[]types.Datum{types.NewIntDatum(1)}, row)
		require.NoError(t, err)
	})
	t.Run("mv index/typed elements", func(t *testing.T) {
		row := newJSONRow(`[1, "a", [2], {"b": 3}]`)
		err := checkMVIndexWithAndWithoutCache(t, tc, cols, indexInfo, tableInfo,
			[]types.Datum{types.NewStringDatum("a")}, row)
		require.NoError(t, err)
		err = checkMVIndexWithAndWithoutCache(t, tc, cols, indexInfo, tableInfo,
			[]types.Datum{types.NewStringDatum("b")}, row)
		require.Error(t, err)
	})

	t.Run("mv index/epsilon only match", func(t *testing.T) {
		// CAST(... AS DOUBLE ARRAY) stores a float64 index value for every element, even
		// for ones the JSON kept as integers, so mixed int/float comparisons are the norm.
		// Their absolute epsilon makes the int element 0 equal to the indexed float64(0)
		// below, while no element compares equal under the exact float-to-float path.
		// This is the shape the miss fallback exists for: CompareBinaryJSON is not a
		// strict weak ordering on such values, so the binary search can miss the only
		// match, and the per-element scan is what keeps the verdict consistent.
		row := newJSONRow(`[0, -9e-9, -5e-9, 5e-9, 1]`)
		err := checkMVIndexWithAndWithoutCache(t, tc, cols, indexInfo, tableInfo,
			[]types.Datum{types.NewFloat64Datum(0)}, row)
		require.NoError(t, err)
	})

	t.Run("mv index/null element", func(t *testing.T) {
		// The write path never stores an array containing JSON null (the array cast rejects null
		// elements), but if one ever appears, it is just the smallest element to the sorter and
		// never matches a non-NULL indexed value.
		row := newJSONRow(`[null, 1]`)
		err := checkMVIndexWithAndWithoutCache(t, tc, cols, indexInfo, tableInfo,
			[]types.Datum{types.NewIntDatum(1)}, row)
		require.NoError(t, err)
		err = checkMVIndexWithAndWithoutCache(t, tc, cols, indexInfo, tableInfo,
			[]types.Datum{types.NewIntDatum(2)}, row)
		require.Error(t, err)
	})

	// A NULL index value only occurs when the column itself is NULL, which never takes the
	// multi-valued path, so both paths must report a mismatch.
	idxVal := types.NewDatum(nil)
	var emptyCache mvIndexSortedElems
	row := newJSONRow("[1, 2]")
	cmp, err := compareMVIndexVal(tc, &row[0], &idxVal, 0, &emptyCache)
	require.NoError(t, err)
	require.NotEqual(t, 0, cmp)
	// The NULL index value returns early, before the cache is needed, so the cache stays unallocated.
	require.Nil(t, emptyCache)
	cmp, err = compareMVIndexVal(tc, &row[0], &idxVal, 0, nil)
	require.NoError(t, err)
	require.NotEqual(t, 0, cmp)
}

func TestCheckRowInsertionConsistency(t *testing.T) {
	sessVars := variable.NewSessionVars(nil)
	rd := rowcodec.Encoder{Enable: true}

	// mocked data
	mockRowKey233 := tablecodec.EncodeRowKeyWithHandle(1, kv.IntHandle(233))
	mockValue233, err := tablecodec.EncodeRow(sessVars.StmtCtx.TimeZone(), []types.Datum{types.NewIntDatum(233)}, []int64{101}, nil, nil, nil, &rd)
	require.Nil(t, err)
	fakeRowInsertion := mutation{key: []byte{1, 1}, value: []byte{1, 1, 1}}

	type caseData struct {
		columnIDToInfo      map[int64]*model.ColumnInfo
		columnIDToFieldType map[int64]*types.FieldType
		rowToInsert         []types.Datum
		rowInsertion        mutation
		correct             bool
	}

	testData := []caseData{
		{
			// expected correct behavior
			map[int64]*model.ColumnInfo{
				101: {
					ID:        101,
					Offset:    0,
					FieldType: *types.NewFieldType(mysql.TypeShort),
				},
			},
			map[int64]*types.FieldType{
				101: types.NewFieldType(mysql.TypeShort),
			},
			[]types.Datum{types.NewIntDatum(233)},
			mutation{key: mockRowKey233, value: mockValue233},
			true,
		},
		{
			// mismatching mutation
			map[int64]*model.ColumnInfo{
				101: {
					ID:        101,
					Offset:    0,
					FieldType: *types.NewFieldType(mysql.TypeShort),
				},
			},
			map[int64]*types.FieldType{
				101: types.NewFieldType(mysql.TypeShort),
			},
			[]types.Datum{types.NewIntDatum(1)},
			fakeRowInsertion,
			false,
		},
		{
			// no input row
			map[int64]*model.ColumnInfo{},
			map[int64]*types.FieldType{},
			nil,
			fakeRowInsertion,
			true,
		},
		{
			// invalid value
			map[int64]*model.ColumnInfo{
				101: {
					ID:        101,
					Offset:    0,
					FieldType: *types.NewFieldType(mysql.TypeShort),
				},
			},
			map[int64]*types.FieldType{
				101: types.NewFieldType(mysql.TypeShort),
			},
			[]types.Datum{types.NewIntDatum(233)},
			mutation{key: mockRowKey233, value: []byte{0, 1, 2, 3}},
			false,
		},
	}

	for caseID, data := range testData {
		err := checkRowInsertionConsistency(
			sessVars, data.rowToInsert, data.rowInsertion, data.columnIDToInfo, data.columnIDToFieldType, "t",
		)
		require.Equal(t, data.correct, err == nil, "case id = %v", caseID)
	}
}

func TestCheckIndexKeysAndCheckHandleConsistency(t *testing.T) {
	//	dimensions of the domain of checkIndexKeys:
	//	1. location										*2
	//	2. table structure
	//		(1) unique index/non-unique index			*2
	//		(2) clustered index							*2
	//		(3) string collation						*2
	//		We don't test primary clustered index and int handle, since they should not have index mutations.
	// Assume PK is always the first column (string).

	// cases
	locations := []*time.Location{time.UTC, time.Local}
	indexInfos := []*model.IndexInfo{
		{
			ID:      1,
			Name:    ast.NewCIStr("idx_unique"),
			State:   model.StatePublic,
			Primary: false,
			Unique:  true,
			Columns: []*model.IndexColumn{
				{
					Name:   ast.NewCIStr("c2"),
					Offset: 1,
					Length: types.UnspecifiedLength,
				},
				{
					Name:   ast.NewCIStr("c1"),
					Offset: 0,
					Length: types.UnspecifiedLength,
				},
			},
		},
		{
			ID:      2,
			Name:    ast.NewCIStr("idx_non_unique"),
			State:   model.StatePublic,
			Primary: false,
			Unique:  false,
			Columns: []*model.IndexColumn{
				{
					Name:   ast.NewCIStr("c2"),
					Offset: 1,
					Length: types.UnspecifiedLength,
				},
				{
					Name:   ast.NewCIStr("c1"),
					Offset: 0,
					Length: types.UnspecifiedLength,
				},
			},
		},
	}
	columnInfoSets := [][]*model.ColumnInfo{
		{
			{ID: 1, Name: ast.NewCIStr("c1"), Offset: 0, State: model.StatePublic, FieldType: *types.NewFieldType(mysql.TypeString)},
			{ID: 2, Name: ast.NewCIStr("c2"), Offset: 1, State: model.StatePublic, FieldType: *types.NewFieldType(mysql.TypeDatetime)},
		},
		{
			{ID: 1, Name: ast.NewCIStr("c1"), Offset: 0, State: model.StatePublic, FieldType: *types.NewFieldTypeWithCollation(mysql.TypeString, "utf8_unicode_ci",
				types.UnspecifiedLength)},
			{ID: 2, Name: ast.NewCIStr("c2"), Offset: 1, State: model.StatePublic, FieldType: *types.NewFieldType(mysql.TypeDatetime)},
		},
	}
	tc := types.DefaultStmtNoWarningContext
	rd := rowcodec.Encoder{Enable: true}

	now := types.CurrentTime(mysql.TypeDatetime)
	rowToInsert := []types.Datum{
		types.NewStringDatum("some string"),
		types.NewTimeDatum(now),
	}
	anotherTime, err := now.Add(tc, types.NewDuration(24, 0, 0, 0, 0))
	require.Nil(t, err)
	rowToRemove := []types.Datum{
		types.NewStringDatum("old string"),
		types.NewTimeDatum(anotherTime),
	}

	getter := func() (map[int64]columnMaps, bool) {
		return nil, false
	}
	setter := func(maps map[int64]columnMaps) {}

	// test
	originNewCollation := collate.NewCollationEnabled()
	defer collate.SetNewCollationEnabledForTest(originNewCollation)
	for _, useNewCollate := range []bool{true, false} {
		collate.SetNewCollationEnabledForTest(useNewCollate)
		for _, isCommonHandle := range []bool{true, false} {
			for _, lc := range locations {
				for _, columnInfos := range columnInfoSets {
					tc = tc.WithLocation(lc)
					tableInfo := model.TableInfo{
						ID:             1,
						Name:           ast.NewCIStr("t"),
						State:          model.StatePublic,
						Columns:        columnInfos,
						Indices:        indexInfos,
						PKIsHandle:     false,
						IsCommonHandle: isCommonHandle,
					}
					tableFromMeta, err := TableFromMeta(autoid.NewAllocators(false), &tableInfo)
					require.NoError(t, err)
					table := tableFromMeta.(*TableCommon)
					require.Equal(t, useNewCollate, table.encoder.UseNewCollate())
					var handle, corruptedHandle kv.Handle
					if isCommonHandle {
						encoded, err := codec.EncodeKey(tc.Location(), nil, rowToInsert[0])
						require.Nil(t, err)
						corrupted := make([]byte, len(encoded))
						copy(corrupted, encoded)
						corrupted[len(corrupted)-1] ^= 1
						handle, err = kv.NewCommonHandle(encoded)
						require.Nil(t, err)
						corruptedHandle, err = kv.NewCommonHandle(corrupted)
						require.Nil(t, err)
					} else {
						handle = kv.IntHandle(1)
						corruptedHandle = kv.IntHandle(2)
					}

					for i, indexInfo := range indexInfos {
						index := table.indices[i]
						maps := getOrBuildColumnMaps(getter, setter, table)

						// test checkIndexKeys
						insertionKey, insertionValue, err := buildIndexKeyValue(index, rowToInsert, tc, indexInfo, table, handle)
						require.Nil(t, err)
						requireIndexKVDecodeMatchesRow(t, tc, table, indexInfo, rowToInsert, insertionKey, insertionValue,
							maps.IndexIDToRowColInfos[indexInfo.ID])
						deletionKey, _, err := buildIndexKeyValue(index, rowToRemove, tc, indexInfo, table, handle)
						require.Nil(t, err)
						indexMutations := []mutation{
							{key: insertionKey, value: insertionValue, indexID: indexInfo.ID},
							{key: deletionKey, indexID: indexInfo.ID},
						}
						err = checkIndexKeys(
							tc, table, rowToInsert, rowToRemove, indexMutations, maps.IndexIDToInfo,
							maps.IndexIDToRowColInfos, nil,
						)
						require.Nil(t, err)

						// test checkHandleConsistency
						rowKey := tablecodec.EncodeRowKeyWithHandle(table.tableID, handle)
						corruptedRowKey := tablecodec.EncodeRowKeyWithHandle(table.tableID, corruptedHandle)
						rowValue, err := tablecodec.EncodeRow(tc.Location(), rowToInsert, []int64{1, 2}, nil, nil, nil, &rd)
						require.Nil(t, err)
						rowMutation := mutation{key: rowKey, value: rowValue}
						corruptedRowMutation := mutation{key: corruptedRowKey, value: rowValue}
						err = checkHandleConsistency(rowMutation, indexMutations, maps.IndexIDToInfo, &tableInfo)
						require.Nil(t, err)
						err = checkHandleConsistency(corruptedRowMutation, indexMutations, maps.IndexIDToInfo, &tableInfo)
						require.NotNil(t, err)
					}
				}
			}
		}
	}
}

func requireIndexKVDecodeMatchesRow(
	t *testing.T,
	tc types.Context,
	table *TableCommon,
	indexInfo *model.IndexInfo,
	row []types.Datum,
	key []byte,
	value []byte,
	rowColInfos []rowcodec.ColInfo,
) {
	t.Helper()

	decodedIndexValues, err := tablecodec.DecodeIndexKV(
		key, value, len(indexInfo.Columns), tablecodec.HandleNotNeeded, rowColInfos,
	)
	require.NoError(t, err)
	requireDecodedIndexValuesMatchRow(t, tc, table, indexInfo, row, decodedIndexValues)

	preAlloc := make([][]byte, len(indexInfo.Columns), len(indexInfo.Columns)+len(rowColInfos))
	decodedIndexValuesEx, err := tablecodec.DecodeIndexKVEx(
		key, value, len(indexInfo.Columns), tablecodec.HandleNotNeeded, rowColInfos, nil, preAlloc,
	)
	require.NoError(t, err)
	require.Equal(t, decodedIndexValues, decodedIndexValuesEx)
	requireDecodedIndexValuesMatchRow(t, tc, table, indexInfo, row, decodedIndexValuesEx)
}

func requireDecodedIndexValuesMatchRow(
	t *testing.T,
	tc types.Context,
	table *TableCommon,
	indexInfo *model.IndexInfo,
	row []types.Datum,
	decodedIndexValues [][]byte,
) {
	t.Helper()

	require.Len(t, decodedIndexValues, len(indexInfo.Columns))
	indexData := make([]types.Datum, 0, len(decodedIndexValues))
	for i, value := range decodedIndexValues {
		fieldType := table.Columns[indexInfo.Columns[i].Offset].FieldType.ArrayType()
		datum, err := tablecodec.DecodeColumnValue(value, fieldType, tc.Location())
		require.NoError(t, err)
		indexData = append(indexData, datum)
	}
	require.NoError(t, compareIndexDataWithMVCache(table.encoder.UseNewCollate(), tc, table.Columns, indexData, row, indexInfo, table.Meta(), nil, nil))
}

func buildIndexKeyValue(index table.Index, rowToInsert []types.Datum, tc types.Context,
	indexInfo *model.IndexInfo, table *TableCommon, handle kv.Handle) ([]byte, []byte, error) {
	indexedValues, err := index.FetchValues(rowToInsert, nil)
	if err != nil {
		return nil, nil, err
	}
	key, distinct, err := index.GenIndexKey(errctx.StrictNoWarningContext, tc.Location(), indexedValues, handle, nil)
	if err != nil {
		return nil, nil, err
	}
	rsData := TryGetHandleRestoredDataWrapper(table, rowToInsert, nil, indexInfo)
	value, err := index.GenIndexValue(errctx.StrictNoWarningContext, tc.Location(), distinct, false, indexedValues, handle, rsData, nil)
	if err != nil {
		return nil, nil, err
	}
	return key, value, nil
}
