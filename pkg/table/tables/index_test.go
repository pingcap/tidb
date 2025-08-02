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
	"github.com/pingcap/tidb/pkg/distsql"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/encode"
	lkv "github.com/pingcap/tidb/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/pkg/meta/metabuild"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"github.com/pingcap/tipb/go-tipb"
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
	tk.MustExec("set @@global.tidb_enable_metadata_lock='ON'")
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
	// The index name for `indexDefinition` must be `testidx`
	type testCase struct {
		tableDefinition string
		indexDefinition string
		row             []any
		meet            bool
	}
	testCases := []testCase{
		// cluster index case
		{
			tableDefinition: "create table t (a int, b int, c int, primary key (a, b))",
			indexDefinition: "create index testidx on t (c) where c > 2",
			row:             []any{1, 2, 3},
			meet:            true,
		},
		{
			tableDefinition: "create table t (a int, b int, c int, primary key (a, b))",
			indexDefinition: "create index testidx on t (c) where c > 3",
			row:             []any{1, 2, 3},
			meet:            false,
		},
		// primary as handle case
		{
			tableDefinition: "create table t (a int, b int, c int, primary key (a))",
			indexDefinition: "create index testidx on t (c) where c > 2",
			row:             []any{1, 2, 3},
			meet:            true,
		},
		{
			tableDefinition: "create table t (a int, b int, c int, primary key (a))",
			indexDefinition: "create index testidx on t (c) where c > 3",
			row:             []any{1, 2, 3},
			meet:            false,
		},
		// generated column case
		{
			tableDefinition: "create table t (a int, b int, c int as (a+b), primary key (a))",
			indexDefinition: "create index testidx on t (c) where c > 2",
			row:             []any{1, 2, 3},
			meet:            true,
		},
		{
			tableDefinition: "create table t (a int, b int, c int as (a+b), primary key (a))",
			indexDefinition: "create index testidx on t (c) where c > 3",
			row:             []any{1, 2, 3},
			meet:            false,
		},
		// tidb rowid case
		{
			tableDefinition: "create table t (a int, b int, c int)",
			indexDefinition: "create index testidx on t (c) where c > 2",
			row:             []any{1, 2, 3, 100},
			meet:            true,
		},
		{
			tableDefinition: "create table t (a int, b int, c int)",
			indexDefinition: "create index testidx on t (c) where c > 3",
			row:             []any{1, 2, 3, 500},
			meet:            false,
		},
	}

	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	for _, tc := range testCases {
		tk.MustExec(tc.tableDefinition)
		tk.MustExec(tc.indexDefinition)

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

// checkIndexEmpty returns true if the index has no KV.
func checkIndexEmpty(t *testing.T, tk *testkit.TestKit, tableName string, indexName string, dom *domain.Domain) bool {
	tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr(tableName))
	require.NoError(t, err)
	require.NotNil(t, tbl)
	var idx table.Index
	for _, i := range tbl.Indices() {
		if i.Meta().Name.L == indexName {
			idx = i
			break
		}
	}

	dctx := tk.Session().GetDistSQLCtx()
	idxFirstColumn := tbl.Meta().Columns[idx.Meta().Columns[0].Offset]
	dagRequest := &tipb.DAGRequest{Executors: []*tipb.Executor{
		{
			Tp: tipb.ExecType_TypeIndexScan,
			IdxScan: &tipb.IndexScan{
				TableId: tbl.Meta().ID,
				IndexId: idx.Meta().ID,
				Columns: util.ColumnsToProto(
					[]*model.ColumnInfo{idxFirstColumn},
					tbl.Meta().PKIsHandle, true, false),
			},
		},
	}}

	tk.MustExec("BEGIN")
	defer tk.MustExec("COMMIT")
	txnManager := sessiontxn.GetTxnManager(tk.Session())
	startTS, err := txnManager.GetStmtReadTS()
	require.NoError(t, err)
	request, err := (&distsql.RequestBuilder{}).
		SetIndexRanges(dctx, tbl.Meta().ID, idx.Meta().ID, ranger.FullIntRange(false)).
		SetDAGRequest(dagRequest).
		SetDesc(false).
		SetKeepOrder(false).
		SetFromSessionVars(tk.Session().GetDistSQLCtx()).
		SetMemTracker(memory.NewTracker(-1, -1)).
		SetStartTS(startTS).
		Build()
	require.NoError(t, err)
	result, err := distsql.Select(context.Background(), dctx, request, []*types.FieldType{
		&idxFirstColumn.FieldType,
	})
	defer func() {
		require.NoError(t, result.Close())
	}()
	require.NoError(t, err)
	chk := chunk.New([]*types.FieldType{&idxFirstColumn.FieldType}, 1024, 1024)
	err = result.Next(context.Background(), chk)
	require.NoError(t, err)

	return chk.NumRows() == 0
}

func TestPartialIndexDML(t *testing.T) {
	// The index name for `indexDefinition` must be `testidx`
	type testCase struct {
		tableDefinition   string
		indexDefinition   string
		dml               []string
		shouldCreateIndex bool
	}
	testCases := []testCase{
		// cluster index case
		{
			tableDefinition:   "create table t (a int, b int, c int, primary key (a, b))",
			indexDefinition:   "create index testidx on t (c) where c > 2",
			dml:               []string{"insert into t values (1, 2, 3)"},
			shouldCreateIndex: true,
		},
		{
			tableDefinition:   "create table t (a int, b int, c int, primary key (a, b))",
			indexDefinition:   "create index testidx on t (c) where c > 3",
			dml:               []string{"insert into t values (1, 2, 3)"},
			shouldCreateIndex: false,
		},
		// primary as handle case
		{
			tableDefinition:   "create table t (a int, b int, c int, primary key (a))",
			indexDefinition:   "create index testidx on t (c) where c > 2",
			dml:               []string{"insert into t values (1, 2, 3)"},
			shouldCreateIndex: true,
		},
		{
			tableDefinition:   "create table t (a int, b int, c int, primary key (a))",
			indexDefinition:   "create index testidx on t (c) where c > 3",
			dml:               []string{"insert into t values (1, 2, 3)"},
			shouldCreateIndex: false,
		},
		// generated column case
		{
			tableDefinition:   "create table t (a int, b int, c int as (a+b), primary key (a))",
			indexDefinition:   "create index testidx on t (c) where c > 2",
			dml:               []string{"insert into t(a,b) values (1, 2)"},
			shouldCreateIndex: true,
		},
		{
			tableDefinition:   "create table t (a int, b int, c int as (a+b), primary key (a))",
			indexDefinition:   "create index testidx on t (c) where c > 3",
			dml:               []string{"insert into t(a,b) values (1, 2)"},
			shouldCreateIndex: false,
		},
		// tidb rowid case
		{
			tableDefinition:   "create table t (a int, b int, c int)",
			indexDefinition:   "create index testidx on t (c) where c > 2",
			dml:               []string{"insert into t values (1, 2, 3)"},
			shouldCreateIndex: true,
		},
		{
			tableDefinition:   "create table t (a int, b int, c int)",
			indexDefinition:   "create index testidx on t (c) where c > 3",
			dml:               []string{"insert into t values (1, 2, 3)"},
			shouldCreateIndex: false,
		},
		// update case
		{
			tableDefinition:   "create table t (a int, b int, c int, primary key (a))",
			indexDefinition:   "create index testidx on t (c) where c > 2",
			dml:               []string{"insert into t values (1, 2, 3)", "update t set c = 4 where a = 1"},
			shouldCreateIndex: true,
		},
		{
			tableDefinition:   "create table t (a int, b int, c int, primary key (a))",
			indexDefinition:   "create index testidx on t (c) where c > 2",
			dml:               []string{"insert into t values (1, 2, 3)", "update t set c = 1 where a = 1"},
			shouldCreateIndex: false,
		},
		{
			tableDefinition:   "create table t (a int, b int, c int, primary key (a))",
			indexDefinition:   "create index testidx on t (c) where c > 2",
			dml:               []string{"insert into t values (1, 2, 1)", "update t set c = 3 where a = 1"},
			shouldCreateIndex: true,
		},
	}

	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	for _, tc := range testCases {
		tk.MustExec(tc.tableDefinition)
		tk.MustExec(tc.indexDefinition)

		for _, dml := range tc.dml {
			tk.MustExec(dml)
		}
		require.Equal(t, tc.shouldCreateIndex, !checkIndexEmpty(t, tk, "t", "testidx", dom))

		// TODO: pass the test.
		// Now the `Delete` is not well implemented because `Delete` usually doesn't have all columns needed to eval
		// `MeetPartialCondition`. Need some modification from planner to make it work.
		// if tc.shouldCreateIndex {
		// 	// test delete
		// 	tk.MustExec("delete from t")
		// 	require.True(t, checkIndexEmpty(t, tk, "t", "testidx", dom))
		// }

		tk.MustExec("drop table t")
	}
}

func TestPartialIndexDDL(t *testing.T) {
	type testCase struct {
		tableDefinition   string
		dml               string
		indexDefinition   string
		shouldCreateIndex bool
	}
	testCases := []testCase{
		{
			tableDefinition:   "create table t (a int, b int, c int, primary key (a, b))",
			dml:               "insert into t values (1, 2, 3)",
			indexDefinition:   "create index testidx on t (c) where c > 2",
			shouldCreateIndex: true,
		},
		{
			tableDefinition:   "create table t (a int, b int, c int, primary key (a, b))",
			dml:               "insert into t values (1, 2, 3)",
			indexDefinition:   "create index testidx on t (c) where c > 3",
			shouldCreateIndex: false,
		},
		// TODO: add more test cases for at least the following scenarios:
		// - partition table
		// - global index
		// - fast add index (though ingest)
	}

	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	for _, tc := range testCases {
		tk.MustExec(tc.tableDefinition)
		tk.MustExec(tc.dml)

		tk.MustExec(tc.indexDefinition)
		require.Equal(t, tc.shouldCreateIndex, !checkIndexEmpty(t, tk, "t", "testidx", dom))

		tk.MustExec("drop table t")
	}
}
