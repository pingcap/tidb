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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/encode"
	lkv "github.com/pingcap/tidb/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"github.com/stretchr/testify/require"
)

func TestMultiColumnCommonHandle(t *testing.T) {
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

	store := testkit.CreateMockStore(t)
	txn, err := store.Begin()
	require.NoError(t, err)
	mockCtx := mock.NewContext()
	sc := mockCtx.GetSessionVars().StmtCtx
	// create index for "insert t values (3, 2, "abc", "abc")
	idxColVals := types.MakeDatums("abc")
	handleColVals := types.MakeDatums(3, 2)
	encodedHandle, err := codec.EncodeKey(sc.TimeZone(), nil, handleColVals...)
	require.NoError(t, err)
	commonHandle, err := kv.NewCommonHandle(encodedHandle)
	require.NoError(t, err)
	_ = idxNonUnique
	for _, idx := range []table.Index{idxUnique, idxNonUnique} {
		key, _, err := idx.GenIndexKey(sc.ErrCtx(), sc.TimeZone(), idxColVals, commonHandle, nil)
		require.NoError(t, err)
		_, err = idx.Create(mockCtx.GetTableCtx(), txn, idxColVals, commonHandle, nil)
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
	store := testkit.CreateMockStore(t)
	txn, err := store.Begin()
	require.NoError(t, err)

	mockCtx := mock.NewContext()
	sc := mockCtx.GetSessionVars().StmtCtx
	// create index for "insert t values ('abc', 1, 1)"
	idxColVals := types.MakeDatums(1)
	handleColVals := types.MakeDatums("abc")
	encodedHandle, err := codec.EncodeKey(sc.TimeZone(), nil, handleColVals...)
	require.NoError(t, err)
	commonHandle, err := kv.NewCommonHandle(encodedHandle)
	require.NoError(t, err)

	for _, idx := range []table.Index{idxUnique, idxNonUnique} {
		key, _, err := idx.GenIndexKey(sc.ErrCtx(), sc.TimeZone(), idxColVals, commonHandle, nil)
		require.NoError(t, err)
		_, err = idx.Create(mockCtx.GetTableCtx(), txn, idxColVals, commonHandle, nil)
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

func TestGenIndexValueFromIndex(t *testing.T) {
	tblInfo := buildTableInfo(t, "create table a (a int primary key, b int not null, c text, unique key key_b(b));")
	tblInfo.State = model.StatePublic
	tbl, err := tables.TableFromMeta(lkv.NewPanickingAllocators(tblInfo.SepAutoInc(), 0), tblInfo)
	require.NoError(t, err)

	sessionOpts := encode.SessionOptions{
		SQLMode:   mysql.ModeStrictAllTables,
		Timestamp: 1234567890,
	}

	encoder, err := lkv.NewBaseKVEncoder(&encode.EncodingConfig{
		Table:          tbl,
		SessionOptions: sessionOpts,
	})
	require.NoError(t, err)
	encoder.SessionCtx.GetSessionVars().RowEncoder.Enable = true

	data1 := []types.Datum{
		types.NewIntDatum(1),
		types.NewIntDatum(23),
		types.NewStringDatum("4.csv"),
	}
	tctx := encoder.SessionCtx.GetTableCtx()
	_, err = encoder.Table.AddRecord(tctx, data1)
	require.NoError(t, err)
	kvPairs := encoder.SessionCtx.TakeKvPairs()

	indexKey := kvPairs.Pairs[1].Key
	indexValue := kvPairs.Pairs[1].Val

	_, idxID, _, err := tablecodec.DecodeIndexKey(indexKey)
	require.NoError(t, err)

	idxInfo := model.FindIndexInfoByID(tbl.Meta().Indices, idxID)

	valueStr, err := tables.GenIndexValueFromIndex(indexKey, indexValue, tbl.Meta(), idxInfo)
	require.NoError(t, err)
	require.Equal(t, []string{"23"}, valueStr)
}

func TestGenIndexValueWithLargePaddingSize(t *testing.T) {
	// ref https://github.com/pingcap/tidb/issues/47115
	tblInfo := buildTableInfo(t, "create table t (a int, b int, k varchar(255), primary key (a, b), key (k))")
	var idx table.Index
	for _, idxInfo := range tblInfo.Indices {
		if !idxInfo.Primary {
			idx = tables.NewIndex(tblInfo.ID, tblInfo, idxInfo)
			break
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

	store := testkit.CreateMockStore(t)
	txn, err := store.Begin()
	require.NoError(t, err)
	mockCtx := mock.NewContext()
	sc := mockCtx.GetSessionVars().StmtCtx
	padding := strings.Repeat(" ", 128)
	idxColVals := types.MakeDatums("abc" + padding)
	handleColVals := types.MakeDatums(1, 2)
	encodedHandle, err := codec.EncodeKey(sc.TimeZone(), nil, handleColVals...)
	require.NoError(t, err)
	commonHandle, err := kv.NewCommonHandle(encodedHandle)
	require.NoError(t, err)

	key, _, err := idx.GenIndexKey(sc.ErrCtx(), sc.TimeZone(), idxColVals, commonHandle, nil)
	require.NoError(t, err)
	_, err = idx.Create(mockCtx.GetTableCtx(), txn, idxColVals, commonHandle, nil)
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
	require.Equal(t, "abc"+padding, d.GetString())
	_, d, err = codec.DecodeOne(colVals[1])
	require.NoError(t, err)
	require.Equal(t, int64(1), d.GetInt64())
	_, d, err = codec.DecodeOne(colVals[2])
	require.NoError(t, err)
	require.Equal(t, int64(2), d.GetInt64())
	handle, err := tablecodec.DecodeIndexHandle(key, val, 1)
	require.NoError(t, err)
	require.False(t, handle.IsInt())
	require.Equal(t, commonHandle.Encoded(), handle.Encoded())
}

// See issue: https://github.com/pingcap/tidb/issues/62337
func TestForceLockNonUniqueIndexInDDLMergingTempIndex(t *testing.T) {
	tblInfo := buildTableInfo(t, "create table t (id int primary key, k int, key k(k))")

	var idxInfo *model.IndexInfo
	for _, info := range tblInfo.Indices {
		if info.Name.L == "k" {
			idxInfo = info
			break
		}
	}

	require.NotNil(t, idxInfo)
	cases := []struct {
		idxState      model.SchemaState
		backfillState model.BackfillState
		forceLock     bool
	}{
		{model.StateWriteReorganization, model.BackfillStateReadyToMerge, true},
		{model.StateWriteReorganization, model.BackfillStateMerging, true},
		{model.StatePublic, model.BackfillStateInapplicable, false},
	}

	mockCtx := mock.NewContext()
	store := testkit.CreateMockStore(t)
	h := kv.IntHandle(1)
	indexedValues := []types.Datum{types.NewIntDatum(100)}
	idx := tables.NewIndex(tblInfo.ID, tblInfo, idxInfo)
	indexKey, distinct, err := idx.GenIndexKey(mockCtx.ErrCtx(), time.UTC, indexedValues, h, nil)
	require.NoError(t, err)
	require.False(t, distinct)

	for _, c := range cases {
		idxInfo.State = c.idxState
		idxInfo.BackfillState = c.backfillState

		t.Run(fmt.Sprintf("DeleteIndex in %s-%s", c.idxState, c.backfillState), func(t *testing.T) {
			txn, err := store.Begin()
			require.NoError(t, err)
			defer func() {
				require.NoError(t, txn.Rollback())
			}()
			txn.SetOption(kv.Pessimistic, true)

			err = idx.Delete(mockCtx.GetTableCtx(), txn, []types.Datum{types.NewIntDatum(100)}, kv.IntHandle(1))
			require.NoError(t, err)
			flags, err := txn.GetMemBuffer().GetFlags(indexKey)
			require.NoError(t, err)
			require.Equal(t, c.forceLock, flags.HasNeedLocked())
		})

		t.Run(fmt.Sprintf("CreateIndex in %s-%s", c.idxState, c.backfillState), func(t *testing.T) {
			txn, err := store.Begin()
			require.NoError(t, err)
			defer func() {
				require.NoError(t, txn.Rollback())
			}()
			txn.SetOption(kv.Pessimistic, true)

			_, err = idx.Create(mockCtx.GetTableCtx(), txn, indexedValues, h, nil)
			require.NoError(t, err)
			flags, err := txn.GetMemBuffer().GetFlags(indexKey)
			require.NoError(t, err)
			require.Equal(t, c.forceLock, flags.HasNeedLocked())
		})
	}
}
