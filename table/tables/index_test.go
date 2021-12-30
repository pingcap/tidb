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

package tables_test

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/rowcodec"
	"github.com/stretchr/testify/require"
)

func TestIndex(t *testing.T) {
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
			},
		},
		Columns: []*model.ColumnInfo{
			{ID: 1, Name: model.NewCIStr("c2"), State: model.StatePublic, Offset: 0, FieldType: *types.NewFieldType(mysql.TypeVarchar)},
			{ID: 2, Name: model.NewCIStr("c2"), State: model.StatePublic, Offset: 1, FieldType: *types.NewFieldType(mysql.TypeString)},
		},
	}
	index := tables.NewIndex(tblInfo.ID, tblInfo, tblInfo.Indices[0])

	// Test ununiq index.
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	txn, err := store.Begin()
	require.NoError(t, err)

	values := types.MakeDatums(1, 2)
	mockCtx := mock.NewContext()
	_, err = index.Create(mockCtx, txn, values, kv.IntHandle(1), nil)
	require.NoError(t, err)

	it, err := index.SeekFirst(txn)
	require.NoError(t, err)

	getValues, h, err := it.Next()
	require.NoError(t, err)
	require.Len(t, getValues, 2)
	require.Equal(t, int64(1), getValues[0].GetInt64())
	require.Equal(t, int64(2), getValues[1].GetInt64())
	require.Equal(t, int64(1), h.IntValue())
	it.Close()
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	exist, _, err := index.Exist(sc, txn, values, kv.IntHandle(100))
	require.NoError(t, err)
	require.False(t, exist)

	exist, _, err = index.Exist(sc, txn, values, kv.IntHandle(1))
	require.NoError(t, err)
	require.True(t, exist)

	err = index.Delete(sc, txn, values, kv.IntHandle(1))
	require.NoError(t, err)

	it, err = index.SeekFirst(txn)
	require.NoError(t, err)

	_, _, err = it.Next()
	require.Truef(t, terror.ErrorEqual(err, io.EOF), "err %v", err)
	it.Close()

	_, err = index.Create(mockCtx, txn, values, kv.IntHandle(0), nil)
	require.NoError(t, err)

	_, err = index.SeekFirst(txn)
	require.NoError(t, err)

	_, hit, err := index.Seek(sc, txn, values)
	require.NoError(t, err)
	require.False(t, hit)

	err = index.Drop(txn)
	require.NoError(t, err)

	it, hit, err = index.Seek(sc, txn, values)
	require.NoError(t, err)
	require.False(t, hit)

	_, _, err = it.Next()
	require.Truef(t, terror.ErrorEqual(err, io.EOF), "err %v", err)
	it.Close()

	it, err = index.SeekFirst(txn)
	require.NoError(t, err)

	_, _, err = it.Next()
	require.Truef(t, terror.ErrorEqual(err, io.EOF), "err %v", err)
	it.Close()

	err = txn.Commit(context.Background())
	require.NoError(t, err)

	tblInfo = &model.TableInfo{
		ID: 2,
		Indices: []*model.IndexInfo{
			{
				ID:     3,
				Name:   model.NewCIStr("test"),
				Unique: true,
				Columns: []*model.IndexColumn{
					{Offset: 0},
					{Offset: 1},
				},
			},
		},
		Columns: []*model.ColumnInfo{
			{ID: 1, Name: model.NewCIStr("c2"), State: model.StatePublic, Offset: 0, FieldType: *types.NewFieldType(mysql.TypeVarchar)},
			{ID: 2, Name: model.NewCIStr("c2"), State: model.StatePublic, Offset: 1, FieldType: *types.NewFieldType(mysql.TypeString)},
		},
	}
	index = tables.NewIndex(tblInfo.ID, tblInfo, tblInfo.Indices[0])

	// Test uniq index.
	txn, err = store.Begin()
	require.NoError(t, err)

	_, err = index.Create(mockCtx, txn, values, kv.IntHandle(1), nil)
	require.NoError(t, err)

	_, err = index.Create(mockCtx, txn, values, kv.IntHandle(2), nil)
	require.NotNil(t, err)

	it, err = index.SeekFirst(txn)
	require.NoError(t, err)

	getValues, h, err = it.Next()
	require.NoError(t, err)
	require.Len(t, getValues, 2)
	require.Equal(t, int64(1), getValues[0].GetInt64())
	require.Equal(t, int64(2), getValues[1].GetInt64())
	require.Equal(t, int64(1), h.IntValue())
	it.Close()

	exist, h, err = index.Exist(sc, txn, values, kv.IntHandle(1))
	require.NoError(t, err)
	require.Equal(t, int64(1), h.IntValue())
	require.True(t, exist)

	exist, h, err = index.Exist(sc, txn, values, kv.IntHandle(2))
	require.NotNil(t, err)
	require.Equal(t, int64(1), h.IntValue())
	require.True(t, exist)

	err = txn.Commit(context.Background())
	require.NoError(t, err)

	_, err = index.FetchValues(make([]types.Datum, 0), nil)
	require.NotNil(t, err)

	txn, err = store.Begin()
	require.NoError(t, err)

	// Test the function of Next when the value of unique key is nil.
	values2 := types.MakeDatums(nil, nil)
	_, err = index.Create(mockCtx, txn, values2, kv.IntHandle(2), nil)
	require.NoError(t, err)
	it, err = index.SeekFirst(txn)
	require.NoError(t, err)
	getValues, h, err = it.Next()
	require.NoError(t, err)
	require.Len(t, getValues, 2)
	require.Equal(t, nil, getValues[0].GetInterface())
	require.Equal(t, nil, getValues[1].GetInterface())
	require.Equal(t, int64(2), h.IntValue())
	it.Close()

	err = txn.Commit(context.Background())
	require.NoError(t, err)
}

func TestCombineIndexSeek(t *testing.T) {
	tblInfo := &model.TableInfo{
		ID: 1,
		Indices: []*model.IndexInfo{
			{
				ID:   2,
				Name: model.NewCIStr("test"),
				Columns: []*model.IndexColumn{
					{Offset: 1},
					{Offset: 2},
				},
			},
		},
		Columns: []*model.ColumnInfo{
			{Offset: 0},
			{Offset: 1},
			{Offset: 2},
		},
	}
	index := tables.NewIndex(tblInfo.ID, tblInfo, tblInfo.Indices[0])

	store, clean := testkit.CreateMockStore(t)
	defer clean()
	txn, err := store.Begin()
	require.NoError(t, err)

	mockCtx := mock.NewContext()
	values := types.MakeDatums("abc", "def")
	_, err = index.Create(mockCtx, txn, values, kv.IntHandle(1), nil)
	require.NoError(t, err)

	index2 := tables.NewIndex(tblInfo.ID, tblInfo, tblInfo.Indices[0])
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	iter, hit, err := index2.Seek(sc, txn, types.MakeDatums("abc", nil))
	require.NoError(t, err)
	defer iter.Close()
	require.False(t, hit)
	_, h, err := iter.Next()
	require.NoError(t, err)
	require.Equal(t, int64(1), h.IntValue())
}

func TestMultiColumnCommonHandle(t *testing.T) {
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)
	tblInfo := buildTableInfo(t, "create table t (a int, b int, u varchar(64) unique, nu varchar(64), primary key (a, b), index nu (nu))")
	var idxUnique, idxNonUnique table.Index
	for _, idxInfo := range tblInfo.Indices {
		idx := tables.NewIndex(tblInfo.ID, tblInfo, idxInfo)
		if idxInfo.Name.L == "u" {
			idxUnique = idx
		} else if idxInfo.Name.L == "nu" {
			idxNonUnique = idx
		}
	}
	var a, b *model.ColumnInfo
	for _, col := range tblInfo.Columns {
		if col.Name.String() == "a" {
			a = col
		} else if col.Name.String() == "b" {
			b = col
		}
	}
	require.NotNil(t, a)
	require.NotNil(t, b)

	store, clean := testkit.CreateMockStore(t)
	defer clean()
	txn, err := store.Begin()
	require.NoError(t, err)
	mockCtx := mock.NewContext()
	sc := mockCtx.GetSessionVars().StmtCtx
	// create index for "insert t values (3, 2, "abc", "abc")
	idxColVals := types.MakeDatums("abc")
	handleColVals := types.MakeDatums(3, 2)
	encodedHandle, err := codec.EncodeKey(sc, nil, handleColVals...)
	require.NoError(t, err)
	commonHandle, err := kv.NewCommonHandle(encodedHandle)
	require.NoError(t, err)
	_ = idxNonUnique
	for _, idx := range []table.Index{idxUnique, idxNonUnique} {
		key, _, err := idx.GenIndexKey(sc, idxColVals, commonHandle, nil)
		require.NoError(t, err)
		_, err = idx.Create(mockCtx, txn, idxColVals, commonHandle, nil)
		require.NoError(t, err)
		val, err := txn.Get(context.Background(), key)
		require.NoError(t, err)
		colInfo := tables.BuildRowcodecColInfoForIndexColumns(idx.Meta(), tblInfo)
		colInfo = append(colInfo, rowcodec.ColInfo{
			ID:         a.ID,
			IsPKHandle: false,
			Ft:         rowcodec.FieldTypeFromModelColumn(a),
		})
		colInfo = append(colInfo, rowcodec.ColInfo{
			ID:         b.ID,
			IsPKHandle: false,
			Ft:         rowcodec.FieldTypeFromModelColumn(b),
		})
		colVals, err := tablecodec.DecodeIndexKV(key, val, 1, tablecodec.HandleDefault, colInfo)
		require.NoError(t, err)
		require.Len(t, colVals, 3)
		_, d, err := codec.DecodeOne(colVals[0])
		require.NoError(t, err)
		require.Equal(t, "abc", d.GetString())
		_, d, err = codec.DecodeOne(colVals[1])
		require.NoError(t, err)
		require.Equal(t, int64(3), d.GetInt64())
		_, d, err = codec.DecodeOne(colVals[2])
		require.NoError(t, err)
		require.Equal(t, int64(2), d.GetInt64())
		handle, err := tablecodec.DecodeIndexHandle(key, val, 1)
		require.NoError(t, err)
		require.False(t, handle.IsInt())
		require.Equal(t, commonHandle.Encoded(), handle.Encoded())
	}
}

func TestSingleColumnCommonHandle(t *testing.T) {
	tblInfo := buildTableInfo(t, "create table t (a varchar(255) primary key, u int unique, nu int, index nu (nu))")
	var idxUnique, idxNonUnique table.Index
	for _, idxInfo := range tblInfo.Indices {
		idx := tables.NewIndex(tblInfo.ID, tblInfo, idxInfo)
		if idxInfo.Name.L == "u" {
			idxUnique = idx
		} else if idxInfo.Name.L == "nu" {
			idxNonUnique = idx
		}
	}
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	txn, err := store.Begin()
	require.NoError(t, err)

	mockCtx := mock.NewContext()
	sc := mockCtx.GetSessionVars().StmtCtx
	// create index for "insert t values ('abc', 1, 1)"
	idxColVals := types.MakeDatums(1)
	handleColVals := types.MakeDatums("abc")
	encodedHandle, err := codec.EncodeKey(sc, nil, handleColVals...)
	require.NoError(t, err)
	commonHandle, err := kv.NewCommonHandle(encodedHandle)
	require.NoError(t, err)

	for _, idx := range []table.Index{idxUnique, idxNonUnique} {
		key, _, err := idx.GenIndexKey(sc, idxColVals, commonHandle, nil)
		require.NoError(t, err)
		_, err = idx.Create(mockCtx, txn, idxColVals, commonHandle, nil)
		require.NoError(t, err)
		val, err := txn.Get(context.Background(), key)
		require.NoError(t, err)
		colVals, err := tablecodec.DecodeIndexKV(key, val, 1, tablecodec.HandleDefault,
			tables.BuildRowcodecColInfoForIndexColumns(idx.Meta(), tblInfo))
		require.NoError(t, err)
		require.Len(t, colVals, 2)
		_, d, err := codec.DecodeOne(colVals[0])
		require.NoError(t, err)
		require.Equal(t, int64(1), d.GetInt64())
		_, d, err = codec.DecodeOne(colVals[1])
		require.NoError(t, err)
		require.Equal(t, "abc", d.GetString())
		handle, err := tablecodec.DecodeIndexHandle(key, val, 1)
		require.NoError(t, err)
		require.False(t, handle.IsInt())
		require.Equal(t, commonHandle.Encoded(), handle.Encoded())

		unTouchedVal := append([]byte{1}, val[1:]...)
		unTouchedVal = append(unTouchedVal, kv.UnCommitIndexKVFlag)
		_, err = tablecodec.DecodeIndexKV(key, unTouchedVal, 1, tablecodec.HandleDefault,
			tables.BuildRowcodecColInfoForIndexColumns(idx.Meta(), tblInfo))
		require.NoError(t, err)
	}
}

func buildTableInfo(t *testing.T, sql string) *model.TableInfo {
	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	require.NoError(t, err)
	tblInfo, err := ddl.BuildTableInfoFromAST(stmt.(*ast.CreateTableStmt))
	require.NoError(t, err)
	return tblInfo
}
