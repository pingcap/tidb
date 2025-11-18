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
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/expression/exprstatic"
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
	"github.com/pingcap/tidb/pkg/table/tables/testutil"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
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
		idx, err := tables.NewIndex(tblInfo.ID, tblInfo, idxInfo)
		require.NoError(t, err)
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
		idx, err := tables.NewIndex(tblInfo.ID, tblInfo, idxInfo)
		require.NoError(t, err)
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
	var err error
	for _, idxInfo := range tblInfo.Indices {
		if !idxInfo.Primary {
			idx, err = tables.NewIndex(tblInfo.ID, tblInfo, idxInfo)
			require.NoError(t, err)
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
	idx, err := tables.NewIndex(tblInfo.ID, tblInfo, idxInfo)
	require.NoError(t, err)
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

func TestMeetPartialCondition(t *testing.T) {
	// The index name for the index must be `testidx`
	type testCase struct {
		tableDefinition string
		row             []any
		meet            bool
	}
	testCases := []testCase{
		// cluster index case
		{
			tableDefinition: "create table t (a int, b int, c int, primary key (a, b), key testidx(c) where c > 2)",
			row:             []any{1, 2, 3},
			meet:            true,
		},
		{
			tableDefinition: "create table t (a int, b int, c int, primary key (a, b), key testidx(c) where c > 3)",
			row:             []any{1, 2, 3},
			meet:            false,
		},
		// primary as handle case
		{
			tableDefinition: "create table t (a int, b int, c int, primary key (a), key testidx(c) where c > 2)",
			row:             []any{1, 2, 3},
			meet:            true,
		},
		{
			tableDefinition: "create table t (a int, b int, c int, primary key (a), key testidx(c) where c > 3)",
			row:             []any{1, 2, 3},
			meet:            false,
		},
		// tidb rowid case
		{
			tableDefinition: "create table t (a int, b int, c int, key testidx(c) where c > 2)",
			row:             []any{1, 2, 3, 100},
			meet:            true,
		},
		{
			tableDefinition: "create table t (a int, b int, c int, key testidx(c) where c > 3)",
			row:             []any{1, 2, 3, 500},
			meet:            false,
		},
	}

	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	for _, tc := range testCases {
		tk.MustExec(tc.tableDefinition)

		tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
		require.NoError(t, err)
		require.NotNil(t, t)
		var idx table.Index
		for _, i := range tbl.Indices() {
			if i.Meta().Name.L == "testidx" {
				idx = i
				break
			}
		}

		rowData := types.MakeDatums(tc.row...)

		meet, err := idx.MeetPartialCondition(rowData)
		require.NoError(t, err)
		require.Equal(t, tc.meet, meet)

		tk.MustExec("drop table t")
	}
}

func TestPartialIndexDML(t *testing.T) {
	// The index name for `indexDefinition` must be `testidx`
	type testCase struct {
		tableDefinition   string
		dml               []string
		shouldCreateIndex bool
	}
	testCases := []testCase{
		// cluster index case
		{
			tableDefinition:   "create table t (a int, b int, c int, primary key (a, b), key testidx(c) where c > 2)",
			dml:               []string{"insert into t values (1, 2, 3)"},
			shouldCreateIndex: true,
		},
		{
			tableDefinition:   "create table t (a int, b int, c int, primary key (a, b), key testidx(c) where c > 3)",
			dml:               []string{"insert into t values (1, 2, 3)"},
			shouldCreateIndex: false,
		},
		// primary as handle case
		{
			tableDefinition:   "create table t (a int, b int, c int, primary key (a), key testidx(c) where c > 2)",
			dml:               []string{"insert into t values (1, 2, 3)"},
			shouldCreateIndex: true,
		},
		{
			tableDefinition:   "create table t (a int, b int, c int, primary key (a), key testidx(c) where c > 3)",
			dml:               []string{"insert into t values (1, 2, 3)"},
			shouldCreateIndex: false,
		},
		// tidb rowid case
		{
			tableDefinition:   "create table t (a int, b int, c int, key testidx(c) where c > 2)",
			dml:               []string{"insert into t values (1, 2, 3)"},
			shouldCreateIndex: true,
		},
		{
			tableDefinition:   "create table t (a int, b int, c int, key testidx(c) where c > 3)",
			dml:               []string{"insert into t values (1, 2, 3)"},
			shouldCreateIndex: false,
		},
		// update case
		{
			tableDefinition:   "create table t (a int, b int, c int, primary key (a), key testidx(c) where c > 2)",
			dml:               []string{"insert into t values (1, 2, 3)", "update t set c = 4 where a = 1"},
			shouldCreateIndex: true,
		},
		{
			tableDefinition:   "create table t (a int, b int, c int, primary key (a), key testidx(c) where c > 2)",
			dml:               []string{"insert into t values (1, 2, 3)", "update t set c = 1 where a = 1"},
			shouldCreateIndex: false,
		},
		{
			tableDefinition:   "create table t (a int, b int, c int, primary key (a), key testidx(c) where c > 2)",
			dml:               []string{"insert into t values (1, 2, 1)", "update t set c = 3 where a = 1"},
			shouldCreateIndex: true,
		},
	}

	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	for _, tc := range testCases {
		tk.MustExec(tc.tableDefinition)

		for _, dml := range tc.dml {
			tk.MustExec(dml)
		}
		if tc.shouldCreateIndex {
			testutil.CheckIndexKVCount(t, tk, dom, "t", "testidx", 1)
		} else {
			testutil.CheckIndexKVCount(t, tk, dom, "t", "testidx", 0)
		}

		if tc.shouldCreateIndex {
			// test delete
			tk.MustExec("delete from t")
			testutil.CheckIndexKVCount(t, tk, dom, "t", "testidx", 0)
		}

		tk.MustExec("drop table t")
	}
}

func TestExtractColumnsFromCondition(t *testing.T) {
	// Mock the necessary context and inputs
	ctx := exprstatic.NewExprContext()
	tblInfo := &model.TableInfo{
		Name: ast.NewCIStr("test_table"),
		Columns: []*model.ColumnInfo{
			{Name: ast.NewCIStr("c1"), Offset: 0, State: model.StatePublic},
			{Name: ast.NewCIStr("c2"), Offset: 1, State: model.StatePublic},
			{Name: ast.NewCIStr("c3"), Offset: 2, State: model.StatePublic, GeneratedExprString: "c1 + c2", GeneratedStored: false},
			{Name: ast.NewCIStr("c4"), Offset: 3, State: model.StatePublic, GeneratedExprString: "c1 + c2", GeneratedStored: true},
		},
	}
	idxInfo := &model.IndexInfo{
		Columns: []*model.IndexColumn{
			{Name: ast.NewCIStr("c1"), Offset: 0},
		},
	}

	tests := []struct {
		cond                                       string
		expected                                   []*model.IndexColumn
		expectedColumnInWithVirtualGeneratedColumn []*model.IndexColumn
	}{
		{
			cond:     "c1 AND c2",
			expected: []*model.IndexColumn{{Name: ast.NewCIStr("c1"), Offset: 0}, {Name: ast.NewCIStr("c2"), Offset: 1}},
			expectedColumnInWithVirtualGeneratedColumn: []*model.IndexColumn{{Name: ast.NewCIStr("c1"), Offset: 0}, {Name: ast.NewCIStr("c2"), Offset: 1}},
		},
		{
			cond:     "c1 > 100",
			expected: []*model.IndexColumn{{Name: ast.NewCIStr("c1"), Offset: 0}},
			expectedColumnInWithVirtualGeneratedColumn: []*model.IndexColumn{{Name: ast.NewCIStr("c1"), Offset: 0}},
		},
		{
			cond:     "c3 > 50",
			expected: []*model.IndexColumn{{Name: ast.NewCIStr("c3"), Offset: 2}},
			expectedColumnInWithVirtualGeneratedColumn: []*model.IndexColumn{{Name: ast.NewCIStr("c1"), Offset: 0}, {Name: ast.NewCIStr("c2"), Offset: 1}, {Name: ast.NewCIStr("c3"), Offset: 2}},
		},
		{
			cond:     "c4 > 50",
			expected: []*model.IndexColumn{{Name: ast.NewCIStr("c4"), Offset: 3}},
			expectedColumnInWithVirtualGeneratedColumn: []*model.IndexColumn{{Name: ast.NewCIStr("c4"), Offset: 3}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.cond, func(t *testing.T) {
			idxInfo.ConditionExprString = tt.cond

			got, err := tables.ExtractColumnsFromCondition(ctx, idxInfo, tblInfo, false)
			require.NoError(t, err)
			require.ElementsMatch(t, tt.expected, got)

			got, err = tables.ExtractColumnsFromCondition(ctx, idxInfo, tblInfo, true)
			require.NoError(t, err)
			require.ElementsMatch(t, tt.expectedColumnInWithVirtualGeneratedColumn, got)
		})
	}
}

func TestDedupIndexColumns4Test(t *testing.T) {
	colCount := 100
	allCols := make([]*model.IndexColumn, 0, colCount)
	for i := range colCount {
		allCols = append(allCols, &model.IndexColumn{
			Name:   ast.NewCIStr(fmt.Sprintf("c%d", i)),
			Offset: i,
		})
	}

	// add many existing columns and some duplicated columns
	cols := make([]*model.IndexColumn, 0, colCount*2)
	for i := range colCount {
		cols = append(cols, allCols[i])
	}
	for range colCount {
		cols = append(cols, allCols[rand.IntN(colCount)])
	}

	result := tables.DedupIndexColumns(cols)
	require.Equal(t, allCols, result)
}

func TestPartialIndexDMLDuringDDL(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	type testCase struct {
		ddl string
	}
	testCases := []testCase{
		{
			ddl: "alter table t modify column b int first",
		},
		{
			ddl: "alter table t modify column b int unsigned first",
		},
		{
			ddl: "alter table t modify column b int unsigned",
		},
		{
			ddl: "alter table t change column b e int unsigned",
		},
		{
			ddl: "alter table t change column b e int unsigned, change column d f int unsigned",
		},
	}
	for _, tc := range testCases {
		tk.MustExec("use test")
		tk.MustExec("create table t(a int, b int, c int, d int, key testidx(a) where c > 4)")

		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterWaitSchemaSynced", func(job *model.Job) {
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustExec("insert into t(a, c) values (1, 2), (2, 3), (3, 4), (4, 5)")
			testutil.CheckIndexKVCount(t, tk, dom, "t", "testidx", 1)
			// the hint here is a workaround before we have a valid planner for partial index
			tk.MustExec("update /*+ ignore_index(t, testidx) */ t set c = 5 where a = 1")
			testutil.CheckIndexKVCount(t, tk, dom, "t", "testidx", 2)
			tk.MustExec("delete from t where a = 1")
			testutil.CheckIndexKVCount(t, tk, dom, "t", "testidx", 1)
			tk.MustExec("delete from t")
			testutil.CheckIndexKVCount(t, tk, dom, "t", "testidx", 0)
		})
		tk.MustExec(tc.ddl)
		testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/ddl/afterWaitSchemaSynced")

		tk.MustExec("drop table t")
	}
}

func TestPartialIndexDMLUniqueness(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, c int, unique key testidx(a) where c >= 2)")
	tk.MustExec("insert into t values (1, 1, 1), (2, 2, 2), (3, 3, 3)")
	testutil.CheckIndexKVCount(t, tk, dom, "t", "testidx", 2)
	tk.MustGetErrCode("insert into t values (2, 4, 4)", errno.ErrDupEntry)
	tk.MustExec("insert into t values (2, 4, 1)")
}
