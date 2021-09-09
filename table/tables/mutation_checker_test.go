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
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/rowcodec"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCompareIndexData(t *testing.T) {
	// dimensions of the domain of compareIndexData
	// 	 1. table structure, where we only care about column types that influence truncating values
	//	 2. comparison of row data & index data

	type caseData = struct {
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
			cols = append(cols, &table.Column{ColumnInfo: &model.ColumnInfo{FieldType: *ft}})
			indexCols = append(indexCols, &model.IndexColumn{Offset: i, Length: data.indexLength[i]})
		}
		indexInfo := &model.IndexInfo{Columns: indexCols}

		err := compareIndexData(sc, cols, data.indexData, data.inputData, indexInfo)
		require.Equal(t, data.correct, err == nil, "case id = %v", caseID)
	}
}

func TestCheckRowAdditionConsistency(t *testing.T) {
	sessVars := variable.NewSessionVars()
	rd := rowcodec.Encoder{Enable: true}

	// mocked data
	mockRowKey := tablecodec.EncodeRowKeyWithHandle(1, kv.IntHandle(1))
	mockRowKey233 := tablecodec.EncodeRowKeyWithHandle(1, kv.IntHandle(233))
	mockValue233, err := tablecodec.EncodeRow(sessVars.StmtCtx, []types.Datum{types.NewIntDatum(233)}, []int64{101}, nil, nil, &rd)
	require.Nil(t, err)
	fakeMutations := []mutation{
		{key: []byte{1, 1}, value: []byte{1, 1, 1}},
	}
	type caseData = struct {
		tableColumns []*model.ColumnInfo
		inputRow     []types.Datum
		mutations    []mutation
		autoEncode   bool // encode mutations from row and cols, then append the result to the mutations
		correct      bool
	}

	testData := []caseData{
		{ // no mutations
			[]*model.ColumnInfo{
				{
					ID:        101,
					Offset:    0,
					FieldType: *types.NewFieldType(mysql.TypeShort),
				},
			},
			[]types.Datum{types.NewIntDatum(1)},
			nil,
			false,
			false,
		},
		{ // no corresponding mutation
			[]*model.ColumnInfo{
				{
					ID:        101,
					Offset:    0,
					FieldType: *types.NewFieldType(mysql.TypeShort),
				},
			},
			[]types.Datum{types.NewIntDatum(1)},
			fakeMutations,
			false,
			false,
		},
		{
			[]*model.ColumnInfo{
				{
					ID:        101,
					Offset:    0,
					FieldType: *types.NewFieldType(mysql.TypeShort),
				},
			},
			[]types.Datum{types.NewIntDatum(1)},
			fakeMutations,
			true,
			true,
		},
		{ // no input row
			[]*model.ColumnInfo{},
			nil,
			fakeMutations,
			true,
			true,
		},
		{ // duplicated mutation
			[]*model.ColumnInfo{
				{
					ID:        101,
					Offset:    0,
					FieldType: *types.NewFieldType(mysql.TypeShort),
				},
			},
			[]types.Datum{types.NewIntDatum(233)},
			[]mutation{
				{key: mockRowKey233, value: mockValue233},
			},
			true,
			false,
		},
		{ // different value
			[]*model.ColumnInfo{
				{
					ID:        101,
					Offset:    0,
					FieldType: *types.NewFieldType(mysql.TypeInt24),
				},
			},
			[]types.Datum{types.NewIntDatum(1)},
			[]mutation{
				{key: mockRowKey233, value: mockValue233},
			},
			false,
			false,
		},
		{ // invalid value
			[]*model.ColumnInfo{
				{
					ID:        101,
					Offset:    0,
					FieldType: *types.NewFieldType(mysql.TypeShort),
				},
			},
			[]types.Datum{types.NewIntDatum(233)},
			[]mutation{
				{key: mockRowKey233, value: []byte{0, 1, 2, 3}},
			},
			false,
			false,
		},
	}

	for caseID, data := range testData {
		if data.autoEncode {
			value, err := tablecodec.EncodeRow(sessVars.StmtCtx, data.inputRow, columnsToIDs(data.tableColumns), nil, nil, &rd)
			require.Nil(t, err)
			data.mutations = append(data.mutations, mutation{key: mockRowKey, value: value})
		}
		err := checkRowAdditionConsistency(sessVars, data.tableColumns, data.inputRow, data.mutations)
		require.Equal(t, data.correct, err == nil, "case id = %v", caseID)
	}
}

func columnsToIDs(columns []*model.ColumnInfo) []int64 {
	colIDs := make([]int64, 0, len(columns))
	for _, col := range columns {
		colIDs = append(colIDs, col.ID)
	}
	return colIDs
}
