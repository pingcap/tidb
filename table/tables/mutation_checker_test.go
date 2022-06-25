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
	"testing"
	"time"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/rowcodec"
	"github.com/stretchr/testify/require"
)

func TestCompareIndexData(t *testing.T) {
	// dimensions of the domain of compareIndexData
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
		sc := &stmtctx.StatementContext{}
		cols := make([]*table.Column, 0)
		indexCols := make([]*model.IndexColumn, 0)
		for i, ft := range data.fts {
			cols = append(cols, &table.Column{ColumnInfo: &model.ColumnInfo{Name: model.NewCIStr(fmt.Sprintf("c%d", i)), FieldType: *ft}})
			indexCols = append(indexCols, &model.IndexColumn{Offset: i, Length: data.indexLength[i]})
		}
		indexInfo := &model.IndexInfo{Name: model.NewCIStr("i0"), Columns: indexCols}

		err := compareIndexData(sc, cols, data.indexData, data.inputData, indexInfo, &model.TableInfo{Name: model.NewCIStr("t")})
		require.Equal(t, data.correct, err == nil, "case id = %v", caseID)
	}
}

func TestCheckRowInsertionConsistency(t *testing.T) {
	sessVars := variable.NewSessionVars()
	rd := rowcodec.Encoder{Enable: true}

	// mocked data
	mockRowKey233 := tablecodec.EncodeRowKeyWithHandle(1, kv.IntHandle(233))
	mockValue233, err := tablecodec.EncodeRow(
		sessVars.StmtCtx, []types.Datum{types.NewIntDatum(233)}, []int64{101}, nil, nil, &rd,
	)
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
			State:   model.StatePublic,
			Primary: false,
			Unique:  true,
			Columns: []*model.IndexColumn{
				{
					Offset: 1,
					Length: types.UnspecifiedLength,
				},
				{
					Offset: 0,
					Length: types.UnspecifiedLength,
				},
			},
		},
		{
			ID:      2,
			State:   model.StatePublic,
			Primary: false,
			Unique:  false,
			Columns: []*model.IndexColumn{
				{
					Offset: 1,
					Length: types.UnspecifiedLength,
				},
				{
					Offset: 0,
					Length: types.UnspecifiedLength,
				},
			},
		},
	}
	columnInfoSets := [][]*model.ColumnInfo{
		{
			{ID: 1, Offset: 0, FieldType: *types.NewFieldType(mysql.TypeString)},
			{ID: 2, Offset: 1, FieldType: *types.NewFieldType(mysql.TypeDatetime)},
		},
		{
			{ID: 1, Offset: 0, FieldType: *types.NewFieldTypeWithCollation(mysql.TypeString, "utf8_unicode_ci",
				types.UnspecifiedLength)},
			{ID: 2, Offset: 1, FieldType: *types.NewFieldType(mysql.TypeDatetime)},
		},
	}
	sessVars := variable.NewSessionVars()
	rd := rowcodec.Encoder{Enable: true}

	now := types.CurrentTime(mysql.TypeDatetime)
	rowToInsert := []types.Datum{
		types.NewStringDatum("some string"),
		types.NewTimeDatum(now),
	}
	anotherTime, err := now.Add(sessVars.StmtCtx, types.NewDuration(24, 0, 0, 0, 0))
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
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)
	for _, isCommonHandle := range []bool{true, false} {
		for _, lc := range locations {
			for _, columnInfos := range columnInfoSets {
				sessVars.StmtCtx.TimeZone = lc
				tableInfo := model.TableInfo{
					ID:             1,
					Name:           model.NewCIStr("t"),
					Columns:        columnInfos,
					Indices:        indexInfos,
					PKIsHandle:     false,
					IsCommonHandle: isCommonHandle,
				}
				table := MockTableFromMeta(&tableInfo).(*TableCommon)
				var handle, corruptedHandle kv.Handle
				if isCommonHandle {
					encoded, err := codec.EncodeKey(sessVars.StmtCtx, nil, rowToInsert[0])
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
					insertionKey, insertionValue, err := buildIndexKeyValue(index, rowToInsert, sessVars, tableInfo,
						indexInfo, table, handle)
					require.Nil(t, err)
					deletionKey, _, err := buildIndexKeyValue(index, rowToRemove, sessVars, tableInfo, indexInfo, table,
						handle)
					require.Nil(t, err)
					indexMutations := []mutation{
						{key: insertionKey, value: insertionValue, indexID: indexInfo.ID},
						{key: deletionKey, indexID: indexInfo.ID},
					}
					err = checkIndexKeys(
						sessVars, table, rowToInsert, rowToRemove, indexMutations, maps.IndexIDToInfo,
						maps.IndexIDToRowColInfos,
					)
					require.Nil(t, err)

					// test checkHandleConsistency
					rowKey := tablecodec.EncodeRowKeyWithHandle(table.tableID, handle)
					corruptedRowKey := tablecodec.EncodeRowKeyWithHandle(table.tableID, corruptedHandle)
					rowValue, err := tablecodec.EncodeRow(sessVars.StmtCtx, rowToInsert, []int64{1, 2}, nil, nil, &rd)
					require.Nil(t, err)
					rowMutation := mutation{key: rowKey, value: rowValue}
					corruptedRowMutation := mutation{key: corruptedRowKey, value: rowValue}
					err = checkHandleConsistency(rowMutation, indexMutations, maps.IndexIDToInfo, "t")
					require.Nil(t, err)
					err = checkHandleConsistency(corruptedRowMutation, indexMutations, maps.IndexIDToInfo, "t")
					require.NotNil(t, err)
				}
			}
		}
	}
}

func buildIndexKeyValue(index table.Index, rowToInsert []types.Datum, sessVars *variable.SessionVars,
	tableInfo model.TableInfo, indexInfo *model.IndexInfo, table *TableCommon, handle kv.Handle) ([]byte, []byte, error) {
	indexedValues, err := index.FetchValues(rowToInsert, nil)
	if err != nil {
		return nil, nil, err
	}
	key, distinct, err := tablecodec.GenIndexKey(
		sessVars.StmtCtx, &tableInfo, indexInfo, 1, indexedValues, handle, nil,
	)
	if err != nil {
		return nil, nil, err
	}
	rsData := TryGetHandleRestoredDataWrapper(table, rowToInsert, nil, indexInfo)
	value, err := tablecodec.GenIndexValuePortal(
		sessVars.StmtCtx, &tableInfo, indexInfo, NeedRestoredData(indexInfo.Columns, tableInfo.Columns),
		distinct, false, indexedValues, handle, 0, rsData,
	)
	if err != nil {
		return nil, nil, err
	}
	return key, value, nil
}
