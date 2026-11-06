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
<<<<<<< HEAD
=======
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/expression/exprstatic"
>>>>>>> adee1920e10 (ddl: fix global multi-valued index cleanup during TRUNCATE PARTITION (#71299))
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/encode"
	lkv "github.com/pingcap/tidb/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/pkg/meta/metabuild"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
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
	tblInfo, err := ddl.BuildTableInfoFromAST(metabuild.NewContext(), stmt.(*ast.CreateTableStmt))
	require.NoError(t, err)
	return tblInfo
}

func TestIndexKVGenerator(t *testing.T) {
	sc := mock.NewContext().GetSessionVars().StmtCtx
	for _, tc := range []struct {
		name   string
		sql    string
		values []types.Datum
		want   [][]types.Datum
	}{
		{"plain", "create table t (a int, unique index i(a))", types.MakeDatums(7), [][]types.Datum{types.MakeDatums(7)}},
		{"plain null", "create table t (a int, unique index i(a))", types.MakeDatums(nil), [][]types.Datum{types.MakeDatums(nil)}},
		{"multi", "create table t (a json, unique index i((cast(a as signed array))))", types.MakeDatums(types.CreateBinaryJSON([]any{int64(7), int64(8), int64(7)})), [][]types.Datum{types.MakeDatums(7), types.MakeDatums(8)}},
		{"empty", "create table t (a json, unique index i((cast(a as signed array))))", types.MakeDatums(types.CreateBinaryJSON([]any{})), nil},
		{"null", "create table t (a json, unique index i((cast(a as signed array))))", types.MakeDatums(nil), [][]types.Datum{types.MakeDatums(nil)}},
		{"composite", "create table t (a json, b int, unique index i(b, (cast(a as signed array))))", types.MakeDatums(3, types.CreateBinaryJSON([]any{int64(7), int64(8), int64(7)})), [][]types.Datum{types.MakeDatums(3, 7), types.MakeDatums(3, 8)}},
		{"composite prefix", "create table t (a json, b varchar(8), unique index i(b(2), (cast(a as signed array))))", types.MakeDatums("abcd", types.CreateBinaryJSON([]any{int64(7), int64(8)})), [][]types.Datum{types.MakeDatums("abcd", 7), types.MakeDatums("abcd", 8)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tblInfo := buildTableInfo(t, tc.sql)
			idx, err := tables.NewIndex(tblInfo.ID, tblInfo, tblInfo.Indices[0])
			require.NoError(t, err)
			iter := idx.GenIndexKVIter(sc.ErrCtx(), sc.TimeZone(), tc.values, kv.IntHandle(1), nil)
			var retained [][]types.Datum
			for _, want := range tc.want {
				require.True(t, iter.Valid())
				values := iter.PeekIndexedValues()
				require.Equal(t, want, values)
				require.Equal(t, values, iter.PeekIndexedValues()) // Access does not advance.
				retained = append(retained, values)
				wantKey, wantDistinct, err := idx.GenIndexKey(sc.ErrCtx(), sc.TimeZone(), want, kv.IntHandle(1), nil)
				require.NoError(t, err)
				key, value, distinct, err := iter.Next(nil, nil)
				require.NoError(t, err)
				require.Equal(t, wantKey, key)
				require.Equal(t, wantDistinct, distinct)
				wantValue, err := idx.GenIndexValue(sc.ErrCtx(), sc.TimeZone(), distinct, false, want, kv.IntHandle(1), nil, nil)
				require.NoError(t, err)
				require.Equal(t, wantValue, value)
			}
			require.False(t, iter.Valid())
			require.Equal(t, tc.want, retained) // Advancing does not reuse entry slices.
		})
	}
}

type failingGeneratorIndex struct {
	table.Index
	keyErr, valueErr error
}

func (idx *failingGeneratorIndex) GenIndexKey(_ errctx.Context, _ *time.Location, _ []types.Datum, _ kv.Handle, _ []byte) ([]byte, bool, error) {
	return []byte("key"), true, idx.keyErr
}

func (idx *failingGeneratorIndex) GenIndexValue(_ errctx.Context, _ *time.Location, _, _ bool, _ []types.Datum, _ kv.Handle, _ []types.Datum, _ []byte) ([]byte, error) {
	return nil, idx.valueErr
}

func TestIndexKVGeneratorErrors(t *testing.T) {
	sc := mock.NewContext().GetSessionVars().StmtCtx
	keyErr, valueErr := fmt.Errorf("key error"), fmt.Errorf("value error")
	idx := &failingGeneratorIndex{keyErr: keyErr, valueErr: valueErr}
	values := types.MakeDatums(7)
	iter := table.NewPlainIndexKVGenerator(idx, sc.ErrCtx(), sc.TimeZone(), kv.IntHandle(1), nil, values)
	_, _, _, err := iter.Next(nil, nil)
	require.ErrorIs(t, err, keyErr)
	require.True(t, iter.Valid())
	require.Equal(t, values, iter.PeekIndexedValues())

	idx.keyErr = nil
	_, _, _, err = iter.Next(nil, nil)
	require.ErrorIs(t, err, valueErr)
	require.True(t, iter.Valid())
	require.Equal(t, values, iter.PeekIndexedValues())

	idx.valueErr = nil
	key, _, distinct, err := iter.Next(nil, nil)
	require.NoError(t, err)
	require.Equal(t, []byte("key"), key)
	require.True(t, distinct)
	require.False(t, iter.Valid())
}

func TestGenIndexValueFromIndex(t *testing.T) {
	tblInfo := buildTableInfo(t, "create table a (a int primary key, b int not null, c text, unique key key_b(b));")
	tblInfo.State = model.StatePublic
	tbl, err := tables.TableFromMeta(lkv.NewPanickingAllocators(tblInfo.SepAutoInc()), tblInfo)
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
	encoder.SessionCtx.GetTableCtx().GetRowEncodingConfig().RowEncoder.Enable = true

	data1 := []types.Datum{
		types.NewIntDatum(1),
		types.NewIntDatum(23),
		types.NewStringDatum("4.csv"),
	}
	_, err = encoder.AddRecord(data1)
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

// See issue: https://github.com/pingcap/tidb/issues/55313
func TestTableOperationsInDDLDropIndexWriteOnly(t *testing.T) {
	store, do := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, key a(a), key(b))")
	tk.MustExec("insert into t values(1, 1), (2, 2), (3, 3)")
	// use MDL to block drop index DDL in `StateWriteOnly`
	tk.MustExec("begin pessimistic")
	tk.MustQuery("select * from t order by a asc").Check(testkit.Rows("1 1", "2 2", "3 3"))
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")

	ch := make(chan struct{})
	go func() {
		defer close(ch)
		// run `DROP INDEX` in background, because a transaction is started,
		// the DDL will hang in state `StateWriteOnly` until all transactions are committed or rollback.
		tk3 := testkit.NewTestKit(t, store)
		tk3.MustExec("use test")
		tk3.MustExec("alter table t drop index a")
	}()

	defer func() {
		// after test case, clear transactions and wait background goroutine exit.
		tk.MustExec("rollback")
		tk2.MustExec("rollback")
		select {
		case <-ch:
		case <-time.After(time.Minute):
			require.FailNow(t, "timeout")
		}
	}()

	start := time.Now()
	for {
		time.Sleep(20 * time.Millisecond)
		// wait the DDL state change to `StateWriteOnly`
		tblInfo, err := do.InfoSchema().TableInfoByName(ast.NewCIStr("test"), ast.NewCIStr("t"))
		require.NoError(t, err)
		if state := tblInfo.Indices[0].State; state != model.StatePublic {
			require.Equal(t, model.StateWriteOnly, state)
			break
		}
		if time.Since(start) > time.Minute {
			require.FailNow(t, "timeout")
		}
	}

	// tk2 is used to do some operations when DDL is in state `WriteOnly`.
	// In this state, the dropping index is still written.
	// We set `@@tidb_txn_assertion_level='STRICT'` to detect any inconsistency.
	tk2.MustExec("set @@tidb_txn_assertion_level='STRICT'")
	tk2.MustExec("begin pessimistic")
	// insert new values.
	tk2.MustExec("insert into t values(4, 4), (5, 5), (6, 6)")
	// delete some rows: 1 in storage, 1 in memory buffer.
	tk2.MustExec("delete from t where a in (1, 4)")
	// update some rows: 1 in storage, 1 in memory buffer.
	tk2.MustExec("update t set a = a + 10 where a in (2, 6)")
	// should be tested in `StateWriteOnly` state.
	tblInfo, err := tk2.Session().GetInfoSchema().TableInfoByName(ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	require.Equal(t, model.StateWriteOnly, tblInfo.Indices[0].State)
	// commit should success without any assertion fail.
	tk2.MustExec("commit")
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
