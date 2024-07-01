// Copyright 2019 PingCAP, Inc.
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

package kv_test

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/encode"
	lkv "github.com/pingcap/tidb/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/verification"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	_ "github.com/pingcap/tidb/pkg/planner/core" // to setup expression.EvalAstExpr. Otherwise we cannot parse the default value
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestMarshal(t *testing.T) {
	nullDatum := types.Datum{}
	nullDatum.SetNull()
	minNotNull := types.Datum{}
	minNotNull.SetMinNotNull()
	encoder := zapcore.NewMapObjectEncoder()
	err := encoder.AddArray("test", lkv.RowArrayMarshaller{types.NewStringDatum("1"), nullDatum, minNotNull, types.MaxValueDatum()})
	require.NoError(t, err)
	require.Equal(t, encoder.Fields["test"], []any{
		map[string]any{"kind": "string", "val": "1"},
		map[string]any{"kind": "null", "val": "NULL"},
		map[string]any{"kind": "min", "val": "-inf"},
		map[string]any{"kind": "max", "val": "+inf"},
	})

	invalid := types.Datum{}
	invalid.SetInterface(1)
	err = encoder.AddArray("bad-test", lkv.RowArrayMarshaller{minNotNull, invalid})
	require.Regexp(t, "cannot convert.*", err)
	require.Equal(t, encoder.Fields["bad-test"], []any{
		map[string]any{"kind": "min", "val": "-inf"},
	})
}

type mockTable struct {
	table.Table
}

func (mockTable) AddRecord(ctx table.MutateContext, r []types.Datum, opts ...table.AddRecordOption) (recordID kv.Handle, err error) {
	return kv.IntHandle(-1), errors.New("mock error")
}

func TestEncode(t *testing.T) {
	c1 := &model.ColumnInfo{ID: 1, Name: model.NewCIStr("c1"), State: model.StatePublic, Offset: 0, FieldType: *types.NewFieldType(mysql.TypeTiny)}
	cols := []*model.ColumnInfo{c1}
	tblInfo := &model.TableInfo{ID: 1, Columns: cols, PKIsHandle: false, State: model.StatePublic}
	tbl, err := tables.TableFromMeta(lkv.NewPanickingAllocators(tblInfo.SepAutoInc(), 0), tblInfo)
	require.NoError(t, err)

	logger := log.Logger{Logger: zap.NewNop()}
	rows := []types.Datum{
		types.NewIntDatum(10000000),
	}

	// Strict mode
	strictMode, err := lkv.NewTableKVEncoder(&encode.EncodingConfig{
		Table: tbl,
		SessionOptions: encode.SessionOptions{
			SQLMode:   mysql.ModeStrictAllTables,
			Timestamp: 1234567890,
		},
		Logger: logger,
	}, nil)
	require.NoError(t, err)
	pairs, err := strictMode.Encode(rows, 1, []int{0, 1}, 1234)
	require.Regexp(t, "failed to cast value as tinyint\\(4\\) for column `c1` \\(#1\\):.*overflows tinyint", err)
	require.Nil(t, pairs)

	rowsWithPk := []types.Datum{
		types.NewIntDatum(1),
		types.NewStringDatum("invalid-pk"),
	}
	_, err = strictMode.Encode(rowsWithPk, 2, []int{0, 1}, 1234)
	require.Regexp(t, "failed to cast value as bigint\\(20\\) for column `_tidb_rowid`.*Truncated.*", err)

	rowsWithPk2 := []types.Datum{
		types.NewIntDatum(1),
		types.NewStringDatum("1"),
	}
	pairs, err = strictMode.Encode(rowsWithPk2, 2, []int{0, 1}, 1234)
	require.NoError(t, err)
	require.Equal(t, pairs, lkv.MakeRowFromKvPairs([]common.KvPair{
		{
			Key:   []uint8{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			Val:   []uint8{0x8, 0x2, 0x8, 0x2},
			RowID: common.EncodeIntRowID(2),
		},
	}))

	// Mock add record error
	mockTbl := &mockTable{Table: tbl}
	mockMode, err := lkv.NewTableKVEncoder(&encode.EncodingConfig{
		Table: mockTbl,
		SessionOptions: encode.SessionOptions{
			SQLMode:   mysql.ModeStrictAllTables,
			Timestamp: 1234567891,
		},
		Logger: logger,
	}, nil)
	require.NoError(t, err)
	_, err = mockMode.Encode(rowsWithPk2, 2, []int{0, 1}, 1234)
	require.EqualError(t, err, "mock error")

	// Non-strict mode
	noneMode, err := lkv.NewTableKVEncoder(&encode.EncodingConfig{
		Table: tbl,
		SessionOptions: encode.SessionOptions{
			SQLMode:   mysql.ModeNone,
			Timestamp: 1234567892,
			SysVars:   map[string]string{"tidb_row_format_version": "1"},
		},
		Logger: logger,
	}, nil)
	require.NoError(t, err)
	pairs, err = noneMode.Encode(rows, 1, []int{0, 1}, 1234)
	require.NoError(t, err)
	require.Equal(t, pairs, lkv.MakeRowFromKvPairs([]common.KvPair{
		{
			Key:   []uint8{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			Val:   []uint8{0x8, 0x2, 0x8, 0xfe, 0x1},
			RowID: common.EncodeIntRowID(1),
		},
	}))
}

func TestDecode(t *testing.T) {
	c1 := &model.ColumnInfo{ID: 1, Name: model.NewCIStr("c1"), State: model.StatePublic, Offset: 0, FieldType: *types.NewFieldType(mysql.TypeTiny)}
	cols := []*model.ColumnInfo{c1}
	tblInfo := &model.TableInfo{ID: 1, Columns: cols, PKIsHandle: false, State: model.StatePublic}
	tbl, err := tables.TableFromMeta(lkv.NewPanickingAllocators(tblInfo.SepAutoInc(), 0), tblInfo)
	require.NoError(t, err)
	decoder, err := lkv.NewTableKVDecoder(tbl, "`test`.`c1`", &encode.SessionOptions{
		SQLMode:   mysql.ModeStrictAllTables,
		Timestamp: 1234567890,
	}, log.L())
	require.NoError(t, err)
	require.NotNil(t, decoder)
	require.Equal(t, decoder.Name(), "`test`.`c1`")
	p := common.KvPair{
		Key: []byte{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
		Val: []byte{0x8, 0x2, 0x8, 0x2},
	}
	h, err := decoder.DecodeHandleFromRowKey(p.Key)
	require.NoError(t, err)
	require.NotNil(t, p.Val)
	rows, _, err := decoder.DecodeRawRowData(h, p.Val)
	require.NoError(t, err)
	require.Equal(t, rows, []types.Datum{
		types.NewIntDatum(1),
	})
}

type LocalKvPairs struct {
	pairs []common.KvPair
}

func fromRow(r encode.Row) (l LocalKvPairs) {
	l.pairs = lkv.Row2KvPairs(r)
	return l
}

func TestDecodeIndex(t *testing.T) {
	tblInfo := &model.TableInfo{
		ID: 1,
		Indices: []*model.IndexInfo{
			{
				ID:   2,
				Name: model.NewCIStr("test"),
				Columns: []*model.IndexColumn{
					{Offset: 0},
					{Offset: 1},
				},
				Primary: true,
				State:   model.StatePublic,
			},
		},
		Columns: []*model.ColumnInfo{
			{ID: 1, Name: model.NewCIStr("c1"), State: model.StatePublic, Offset: 0, FieldType: *types.NewFieldType(mysql.TypeInt24)},
			{ID: 2, Name: model.NewCIStr("c2"), State: model.StatePublic, Offset: 1, FieldType: *types.NewFieldType(mysql.TypeString)},
		},
		State:      model.StatePublic,
		PKIsHandle: false,
	}
	tbl, err := tables.TableFromMeta(lkv.NewPanickingAllocators(tblInfo.SepAutoInc(), 0), tblInfo)
	if err != nil {
		fmt.Printf("error: %v", err.Error())
	}
	require.NoError(t, err)
	rows := []types.Datum{
		types.NewIntDatum(2),
		types.NewStringDatum("abc"),
	}

	// Strict mode
	strictMode, err := lkv.NewTableKVEncoder(&encode.EncodingConfig{
		Table: tbl,
		SessionOptions: encode.SessionOptions{
			SQLMode:   mysql.ModeStrictAllTables,
			Timestamp: 1234567890,
		},
		Logger: log.L(),
	}, nil)
	require.NoError(t, err)
	pairs, err := strictMode.Encode(rows, 1, []int{0, 1, -1}, 123)
	data := fromRow(pairs)
	require.Len(t, data.pairs, 2)

	decoder, err := lkv.NewTableKVDecoder(tbl, "`test`.``", &encode.SessionOptions{
		SQLMode:   mysql.ModeStrictAllTables,
		Timestamp: 1234567890,
	}, log.L())
	require.NoError(t, err)
	h1, err := decoder.DecodeHandleFromRowKey(data.pairs[0].Key)
	require.NoError(t, err)
	h2, err := decoder.DecodeHandleFromIndex(tbl.Indices()[0].Meta(), data.pairs[1].Key, data.pairs[1].Val)
	require.NoError(t, err)
	require.True(t, h1.Equal(h2))
	rawData, _, err := decoder.DecodeRawRowData(h1, data.pairs[0].Val)
	require.NoError(t, err)
	require.Equal(t, rawData, rows)
}

func TestEncodeRowFormatV2(t *testing.T) {
	// Test encoding in row format v2, as described in <https://github.com/pingcap/tidb/blob/master/docs/design/2018-07-19-row-format.md>.

	c1 := &model.ColumnInfo{ID: 1, Name: model.NewCIStr("c1"), State: model.StatePublic, Offset: 0, FieldType: *types.NewFieldType(mysql.TypeTiny)}
	cols := []*model.ColumnInfo{c1}
	tblInfo := &model.TableInfo{ID: 1, Columns: cols, PKIsHandle: false, State: model.StatePublic}
	tbl, err := tables.TableFromMeta(lkv.NewPanickingAllocators(tblInfo.SepAutoInc(), 0), tblInfo)
	require.NoError(t, err)

	rows := []types.Datum{
		types.NewIntDatum(10000000),
	}

	noneMode, err := lkv.NewTableKVEncoder(&encode.EncodingConfig{
		Table: tbl,
		SessionOptions: encode.SessionOptions{
			SQLMode:   mysql.ModeNone,
			Timestamp: 1234567892,
			SysVars:   map[string]string{"tidb_row_format_version": "2"},
		},
		Logger: log.L(),
	}, nil)
	require.NoError(t, err)
	pairs, err := noneMode.Encode(rows, 1, []int{0, 1}, 1234)
	require.NoError(t, err)
	require.Equal(t, pairs, lkv.MakeRowFromKvPairs([]common.KvPair{
		{
			// the key should be the same as TestEncode()
			Key: []uint8{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			Val: []uint8{
				0x80,     // version
				0x0,      // flag = 0 = not big
				0x1, 0x0, // number of not null columns = 1
				0x0, 0x0, // number of null columns = 0
				0x1,      // column IDs = [1]
				0x1, 0x0, // not null offsets = [1]
				0x7f, // column version = 127 (10000000 clamped to TINYINT)
			},
			RowID: common.EncodeIntRowID(1),
		},
	}))
}

func TestEncodeTimestamp(t *testing.T) {
	ty := *types.NewFieldType(mysql.TypeDatetime)
	ty.AddFlag(mysql.NotNullFlag)
	c1 := &model.ColumnInfo{
		ID:           1,
		Name:         model.NewCIStr("c1"),
		State:        model.StatePublic,
		Offset:       0,
		FieldType:    ty,
		DefaultValue: "CURRENT_TIMESTAMP",
		Version:      1,
	}
	cols := []*model.ColumnInfo{c1}
	tblInfo := &model.TableInfo{ID: 1, Columns: cols, PKIsHandle: false, State: model.StatePublic}
	tbl, err := tables.TableFromMeta(lkv.NewPanickingAllocators(tblInfo.SepAutoInc(), 0), tblInfo)
	require.NoError(t, err)

	encoder, err := lkv.NewTableKVEncoder(&encode.EncodingConfig{
		Table: tbl,
		SessionOptions: encode.SessionOptions{
			SQLMode:   mysql.ModeStrictAllTables,
			Timestamp: 1234567893,
			SysVars: map[string]string{
				"tidb_row_format_version": "1",
				"time_zone":               "+08:00",
			},
		},
		Logger: log.L(),
	}, nil)
	require.NoError(t, err)
	pairs, err := encoder.Encode(nil, 70, []int{-1, 1}, 1234)
	require.NoError(t, err)
	require.Equal(t, pairs, lkv.MakeRowFromKvPairs([]common.KvPair{
		{
			Key:   []uint8{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x46},
			Val:   []uint8{0x8, 0x2, 0x9, 0x80, 0x80, 0x80, 0xf0, 0xfd, 0x8e, 0xf7, 0xc0, 0x19},
			RowID: common.EncodeIntRowID(70),
		},
	}))
}

func TestEncodeDoubleAutoIncrement(t *testing.T) {
	tblInfo := mockTableInfo(t, "create table t (id double not null auto_increment, unique key `u_id` (`id`));")
	tbl, err := tables.TableFromMeta(lkv.NewPanickingAllocators(tblInfo.SepAutoInc(), 0), tblInfo)
	require.NoError(t, err)

	encoder, err := lkv.NewTableKVEncoder(&encode.EncodingConfig{
		Table: tbl,
		SessionOptions: encode.SessionOptions{
			SQLMode: mysql.ModeStrictAllTables,
			SysVars: map[string]string{
				"tidb_row_format_version": "2",
			},
		},
		Logger: log.L(),
	}, nil)
	require.NoError(t, err)

	strDatumForID := types.NewStringDatum("1")
	actualDatum, err := lkv.GetActualDatum(encoder, tbl.Cols()[0], 70, &strDatumForID)
	require.NoError(t, err)
	require.Equal(t, types.NewFloat64Datum(1.0), actualDatum)

	pairsExpect, err := encoder.Encode([]types.Datum{
		types.NewFloat64Datum(1.0),
	}, 70, []int{0, -1}, 1234)
	require.NoError(t, err)
	require.Equal(t, lkv.MakeRowFromKvPairs([]common.KvPair{
		{
			Key:   []uint8{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x46},
			Val:   []uint8{0x80, 0x0, 0x1, 0x0, 0x0, 0x0, 0x1, 0x8, 0x0, 0xbf, 0xf0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0},
			RowID: common.EncodeIntRowID(70),
		},
		{
			Key:   []uint8{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x5f, 0x69, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x5, 0xbf, 0xf0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0},
			Val:   []uint8{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x46},
			RowID: common.EncodeIntRowID(70),
		},
	}), pairsExpect)

	pairs, err := encoder.Encode([]types.Datum{
		types.NewStringDatum("1"),
	}, 70, []int{0, -1}, 1234)
	require.NoError(t, err)

	require.Equal(t, pairsExpect, pairs)
	require.Equal(t, tbl.Allocators(lkv.GetEncoderSe(encoder).GetTableCtx()).Get(autoid.AutoIncrementType).Base(), int64(70))
}

func TestEncodeMissingAutoValue(t *testing.T) {
	var rowID int64 = 70
	type testTableInfo struct {
		AllocType  autoid.AllocatorType
		CreateStmt string
	}

	for _, testTblInfo := range []testTableInfo{
		{
			AllocType:  autoid.AutoIncrementType,
			CreateStmt: "create table t (id integer primary key auto_increment);",
		},
		{
			AllocType:  autoid.AutoRandomType,
			CreateStmt: "create table t (id bigint primary key auto_random(3));",
		},
	} {
		tblInfo := mockTableInfo(t, testTblInfo.CreateStmt)
		tbl, err := tables.TableFromMeta(lkv.NewPanickingAllocators(tblInfo.SepAutoInc(), 0), tblInfo)
		require.NoError(t, err)

		encoder, err := lkv.NewTableKVEncoder(&encode.EncodingConfig{
			Table: tbl,
			SessionOptions: encode.SessionOptions{
				SQLMode: mysql.ModeStrictAllTables,
				SysVars: map[string]string{
					"tidb_row_format_version": "2",
				},
			},
			Logger: log.L(),
		}, nil)
		require.NoError(t, err)

		realRowID := lkv.GetEncoderIncrementalID(encoder, rowID)

		var nullDatum types.Datum
		nullDatum.SetNull()

		expectIDDatum := types.NewIntDatum(realRowID)
		actualIDDatum, err := lkv.GetActualDatum(encoder, tbl.Cols()[0], rowID, nil)
		require.NoError(t, err)
		require.Equal(t, expectIDDatum, actualIDDatum)

		actualIDDatum, err = lkv.GetActualDatum(encoder, tbl.Cols()[0], rowID, &nullDatum)
		require.NoError(t, err)
		require.Equal(t, expectIDDatum, actualIDDatum)

		pairsExpect, err := encoder.Encode([]types.Datum{
			types.NewIntDatum(realRowID),
		}, rowID, []int{0}, 1234)
		require.NoError(t, err)

		// test insert a NULL value on auto_xxxx column, and it is set to NOT NULL
		pairs, err := encoder.Encode([]types.Datum{
			nullDatum,
		}, rowID, []int{0}, 1234)
		require.NoError(t, err)
		require.Equalf(t, pairsExpect, pairs, "test table info: %+v", testTblInfo)
		require.Equalf(t, rowID, tbl.Allocators(lkv.GetEncoderSe(encoder).GetTableCtx()).Get(testTblInfo.AllocType).Base(), "test table info: %+v", testTblInfo)

		// test insert a row without specifying the auto_xxxx column
		pairs, err = encoder.Encode([]types.Datum{}, rowID, []int{0}, 1234)
		require.NoError(t, err)
		require.Equalf(t, pairsExpect, pairs, "test table info: %+v", testTblInfo)
		require.Equalf(t, rowID, tbl.Allocators(lkv.GetEncoderSe(encoder).GetTableCtx()).Get(testTblInfo.AllocType).Base(), "test table info: %+v", testTblInfo)
	}
}

func TestEncodeExpressionColumn(t *testing.T) {
	tblInfo := mockTableInfo(t, "create table t (id varchar(40) not null DEFAULT uuid(), unique key `u_id` (`id`));")
	tbl, err := tables.TableFromMeta(lkv.NewPanickingAllocators(tblInfo.SepAutoInc(), 0), tblInfo)
	require.NoError(t, err)

	encoder, err := lkv.NewTableKVEncoder(&encode.EncodingConfig{
		Table: tbl,
		SessionOptions: encode.SessionOptions{
			SQLMode: mysql.ModeStrictAllTables,
			SysVars: map[string]string{
				"tidb_row_format_version": "2",
			},
		},
		Logger: log.L(),
	}, nil)
	require.NoError(t, err)

	strDatumForID := types.NewStringDatum("1")
	actualDatum, err := lkv.GetActualDatum(encoder, tbl.Cols()[0], 70, &strDatumForID)
	require.NoError(t, err)
	require.Equal(t, strDatumForID, actualDatum)

	actualDatum, err = lkv.GetActualDatum(encoder, tbl.Cols()[0], 70, nil)
	require.NoError(t, err)
	require.Equal(t, types.KindString, actualDatum.Kind())
	require.Len(t, actualDatum.GetString(), 36) // uuid length

	actualDatum2, err := lkv.GetActualDatum(encoder, tbl.Cols()[0], 70, nil)
	require.NoError(t, err)
	require.Equal(t, types.KindString, actualDatum2.Kind())
	require.Len(t, actualDatum2.GetString(), 36)
	require.NotEqual(t, actualDatum.GetString(), actualDatum2.GetString()) // check different uuid
}

func mockTableInfo(t *testing.T, createSQL string) *model.TableInfo {
	parser := parser.New()
	node, err := parser.ParseOneStmt(createSQL, "", "")
	require.NoError(t, err)
	sctx := mock.NewContext()
	info, err := ddl.MockTableInfo(sctx, node.(*ast.CreateTableStmt), 1)
	require.NoError(t, err)
	info.State = model.StatePublic
	return info
}

func TestDefaultAutoRandoms(t *testing.T) {
	tblInfo := mockTableInfo(t, "create table t (id bigint unsigned NOT NULL auto_random primary key clustered, a varchar(100));")
	tbl, err := tables.TableFromMeta(lkv.NewPanickingAllocators(tblInfo.SepAutoInc(), 0), tblInfo)
	require.NoError(t, err)
	encoder, err := lkv.NewTableKVEncoder(&encode.EncodingConfig{
		Table: tbl,
		SessionOptions: encode.SessionOptions{
			SQLMode:        mysql.ModeStrictAllTables,
			Timestamp:      1234567893,
			SysVars:        map[string]string{"tidb_row_format_version": "2"},
			AutoRandomSeed: 456,
		},
		Logger: log.L(),
	}, nil)
	require.NoError(t, err)
	pairs, err := encoder.Encode([]types.Datum{types.NewStringDatum("")}, 70, []int{-1, 0}, 1234)
	require.NoError(t, err)
	require.Equal(t, pairs, lkv.MakeRowFromKvPairs([]common.KvPair{
		{
			Key:   []uint8{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x5f, 0x72, 0xf0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x46},
			Val:   []uint8{0x80, 0x0, 0x1, 0x0, 0x0, 0x0, 0x2, 0x0, 0x0},
			RowID: common.EncodeIntRowID(70),
		},
	}))
	require.Equal(t, tbl.Allocators(lkv.GetSession4test(encoder).GetTableCtx()).Get(autoid.AutoRandomType).Base(), int64(70))

	pairs, err = encoder.Encode([]types.Datum{types.NewStringDatum("")}, 71, []int{-1, 0}, 1234)
	require.NoError(t, err)
	require.Equal(t, pairs, lkv.MakeRowFromKvPairs([]common.KvPair{
		{
			Key:   []uint8{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x5f, 0x72, 0xf0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x47},
			Val:   []uint8{0x80, 0x0, 0x1, 0x0, 0x0, 0x0, 0x2, 0x0, 0x0},
			RowID: common.EncodeIntRowID(71),
		},
	}))
	require.Equal(t, tbl.Allocators(lkv.GetSession4test(encoder).GetTableCtx()).Get(autoid.AutoRandomType).Base(), int64(71))
}

func TestShardRowId(t *testing.T) {
	tblInfo := mockTableInfo(t, "create table t (s varchar(16)) shard_row_id_bits = 3;")
	tbl, err := tables.TableFromMeta(lkv.NewPanickingAllocators(tblInfo.SepAutoInc(), 0), tblInfo)
	require.NoError(t, err)
	encoder, err := lkv.NewTableKVEncoder(&encode.EncodingConfig{
		Table: tbl,
		SessionOptions: encode.SessionOptions{
			SQLMode:        mysql.ModeStrictAllTables,
			Timestamp:      1234567893,
			SysVars:        map[string]string{"tidb_row_format_version": "2"},
			AutoRandomSeed: 456,
		},
		Logger: log.L(),
	}, nil)
	require.NoError(t, err)
	keyMap := make(map[int64]struct{}, 16)
	for i := int64(1); i <= 32; i++ {
		pairs, err := encoder.Encode([]types.Datum{types.NewStringDatum(fmt.Sprintf("%d", i))}, i, []int{0, -1}, i*32)
		require.NoError(t, err)
		kvs := fromRow(pairs)
		require.Len(t, kvs.pairs, 1)
		_, h, err := tablecodec.DecodeRecordKey(kvs.pairs[0].Key)
		require.NoError(t, err)
		rowID := h.IntValue()
		require.Equal(t, rowID&((1<<60)-1), i)
		keyMap[rowID>>60] = struct{}{}
	}
	require.Len(t, keyMap, 8)
	require.Equal(t, tbl.Allocators(lkv.GetSession4test(encoder).GetTableCtx()).Get(autoid.RowIDAllocType).Base(), int64(32))
}

func TestClassifyAndAppend(t *testing.T) {
	kvs := lkv.MakeRowFromKvPairs([]common.KvPair{
		{
			Key: []byte("txxxxxxxx_ryyyyyyyy"),
			Val: []byte("value1"),
		},
		{
			Key: []byte("txxxxxxxx_rwwwwwwww"),
			Val: []byte("value2"),
		},
		{
			Key: []byte("txxxxxxxx_izzzzzzzz"),
			Val: []byte("index1"),
		},
	})

	data := lkv.MakeRowsFromKvPairs(nil)
	indices := lkv.MakeRowsFromKvPairs(nil)
	dataChecksum := verification.MakeKVChecksum(0, 0, 0)
	indexChecksum := verification.MakeKVChecksum(0, 0, 0)

	kvs.ClassifyAndAppend(&data, &dataChecksum, &indices, &indexChecksum)

	require.Equal(t, data, lkv.MakeRowsFromKvPairs([]common.KvPair{
		{
			Key: []byte("txxxxxxxx_ryyyyyyyy"),
			Val: []byte("value1"),
		},
		{
			Key: []byte("txxxxxxxx_rwwwwwwww"),
			Val: []byte("value2"),
		},
	}))
	require.Equal(t, indices, lkv.MakeRowsFromKvPairs([]common.KvPair{
		{
			Key: []byte("txxxxxxxx_izzzzzzzz"),
			Val: []byte("index1"),
		},
	}))
	require.Equal(t, dataChecksum.SumKVS(), uint64(2))
	require.Equal(t, indexChecksum.SumKVS(), uint64(1))
}

type benchSQL2KVSuite struct {
	row     []types.Datum
	colPerm []int
	encoder encode.Encoder
	logger  log.Logger
}

func SetUpTest(b *testing.B) *benchSQL2KVSuite {
	// First, create the table info corresponding to TPC-C's "CUSTOMER" table.
	p := parser.New()
	se := mock.NewContext()
	node, err := p.ParseOneStmt(`
		create table bmsql_customer(
			c_w_id         integer not null,
			c_d_id         integer not null,
			c_id           integer not null,
			c_discount     decimal(4,4),
			c_credit       char(2),
			c_last         varchar(16),
			c_first        varchar(16),
			c_credit_lim   decimal(12,2),
			c_balance      decimal(12,2),
			c_ytd_payment  decimal(12,2),
			c_payment_cnt  integer,
			c_delivery_cnt integer,
			c_street_1     varchar(20),
			c_street_2     varchar(20),
			c_city         varchar(20),
			c_state        char(2),
			c_zip          char(9),
			c_phone        char(16),
			c_since        timestamp,
			c_middle       char(2),
			c_data         varchar(500),
			primary key (c_w_id, c_d_id, c_id)
		);
	`, "", "")
	require.NoError(b, err)
	tableInfo, err := ddl.MockTableInfo(se, node.(*ast.CreateTableStmt), 123456)
	require.NoError(b, err)
	tableInfo.State = model.StatePublic

	// Construct the corresponding KV encoder.
	tbl, err := tables.TableFromMeta(lkv.NewPanickingAllocators(tableInfo.SepAutoInc(), 0), tableInfo)
	require.NoError(b, err)
	encoder, err := lkv.NewTableKVEncoder(&encode.EncodingConfig{
		Table: tbl,
		SessionOptions: encode.SessionOptions{
			SysVars: map[string]string{"tidb_row_format_version": "2"},
		},
		Logger: log.L(),
	}, nil)
	require.NoError(b, err)
	logger := log.Logger{Logger: zap.NewNop()}

	// Prepare the row to insert.
	row := []types.Datum{
		types.NewIntDatum(15),
		types.NewIntDatum(10),
		types.NewIntDatum(3000),
		types.NewStringDatum("0.3646"),
		types.NewStringDatum("GC"),
		types.NewStringDatum("CALLYPRIANTI"),
		types.NewStringDatum("Rg6mDFlVnP5yh"),
		types.NewStringDatum("50000.0"),
		types.NewStringDatum("-10.0"),
		types.NewStringDatum("10.0"),
		types.NewIntDatum(1),
		types.NewIntDatum(0),
		types.NewStringDatum("aJK7CuRnE0NUxNHSX"),
		types.NewStringDatum("Q1rps77cXYoj"),
		types.NewStringDatum("MigXbS6UoUS"),
		types.NewStringDatum("UJ"),
		types.NewStringDatum("638611111"),
		types.NewStringDatum("7743262784364376"),
		types.NewStringDatum("2020-02-05 19:29:58.903970"),
		types.NewStringDatum("OE"),
		types.NewStringDatum("H5p3dpjp7uu8n1l3j0o1buecfV6FngNNgftpNALDhOzJaSzMCMlrQwXuvLAFPIFg215D3wAYB62kiixIuasfbD729oq8TwgKzPPsx8kHE1b4AdhHwpCml3ELKiwuNGQl7CcBQOiq6aFEMMHzjGwQyXwGey0wutjp2KP3Nd4qj3FHtmHbsD8cJ0pH9TswNmdQBgXsFPZeJJhsG3rTimQpS9Tmn3vNeI9fFas3ClDZuQtBjqoTJlyzmBIYT8HeV3TuS93TNFDaXZpQqh8HsvlPq4uTTLOO9CguiY29zlSmIjkZYtva3iscG3YDOQVLeGpP9dtqEJwlRvJ4oe9jWkvRMlCeslSNEuzLxjUBtJBnGRFAzJF6RMlIWCkdCpIhcnIy3jUEsxTuiAU3hsZxUjLg2dnOG62h5qR"),
	}
	colPerm := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, -1}
	s := &benchSQL2KVSuite{
		encoder: encoder,
		logger:  logger,
		row:     row,
		colPerm: colPerm,
	}
	return s
}

// BenchmarkSQL2KV Run `go test -benchmem -run=^$ -bench ^BenchmarkSQL2KV$ github.com/pingcap/tidb/pkg/lightning/backend/kv` to get benchmark result.
func BenchmarkSQL2KV(b *testing.B) {
	s := SetUpTest(b)
	for i := 0; i < b.N; i++ {
		rows, err := s.encoder.Encode(s.row, 1, s.colPerm, 0)
		require.NoError(b, err)
		l := reflect.ValueOf(rows).Elem().Field(0).Len()
		require.Equal(b, l, 2)
	}
}
