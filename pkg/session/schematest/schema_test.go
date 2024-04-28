// Copyright 2022 PingCAP, Inc.
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

package schematest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/server"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/testutils"
)

func createMockStoreForSchemaTest(t *testing.T, opts ...mockstore.MockTiKVStoreOption) kv.Storage {
	store, err := mockstore.NewMockStore(opts...)
	require.NoError(t, err)
	session.DisableStats4Test()
	dom, err := session.BootstrapSession(store)
	require.NoError(t, err)

	dom.SetStatsUpdating(true)

	sv := server.CreateMockServer(t, store)
	sv.SetDomain(dom)
	dom.InfoSyncer().SetSessionManager(sv)

	t.Cleanup(func() {
		dom.Close()
		require.NoError(t, store.Close())
	})
	return store
}

func TestPrepareStmtCommitWhenSchemaChanged(t *testing.T) {
	store := createMockStoreForSchemaTest(t)

	setTxnTk := testkit.NewTestKit(t, store)
	setTxnTk.MustExec("set global tidb_txn_mode=''")
	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)

	tk1.MustExec("use test")
	tk1.MustExec("set global tidb_enable_metadata_lock=0")
	tk2.MustExec("use test")

	tk1.MustExec("create table t (a int, b int)")
	tk2.MustExec("prepare stmt from 'insert into t values (?, ?)'")
	tk2.MustExec("set @a = 1")

	// Commit find unrelated schema change.
	tk2.MustExec("begin")
	tk1.MustExec("create table t1 (id int)")
	tk2.MustExec("execute stmt using @a, @a")
	tk2.MustExec("commit")
}

func TestRetrySchemaChangeForEmptyChange(t *testing.T) {
	store := createMockStoreForSchemaTest(t)

	setTxnTk := testkit.NewTestKit(t, store)
	setTxnTk.MustExec("set global tidb_txn_mode=''")
	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)

	tk1.MustExec("use test")
	tk2.MustExec("use test")

	tk1.MustExec("create table t (i int)")
	tk1.MustExec("create table t1 (i int)")
	tk1.MustExec("begin")
	tk2.MustExec("alter table t add j int")
	tk1.MustExec("select * from t for update")
	tk1.MustExec("update t set i = -i")
	tk1.MustExec("delete from t")
	tk1.MustExec("insert into t1 values (1)")
	tk1.MustExec("commit")
}

func TestTableReaderChunk(t *testing.T) {
	// Since normally a single region mock tikv only returns one partial result we need to manually split the
	// table to test multiple chunks.
	var cluster testutils.Cluster
	store := testkit.CreateMockStore(t, mockstore.WithClusterInspector(func(c testutils.Cluster) {
		mockstore.BootstrapWithSingleStore(c)
		cluster = c
	}))

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table chk (a int)")
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert chk values (%d)", i))
	}
	tbl, err := domain.GetDomain(tk.Session()).InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("chk"))
	require.NoError(t, err)
	tableStart := tablecodec.GenTableRecordPrefix(tbl.Meta().ID)
	cluster.SplitKeys(tableStart, tableStart.PrefixNext(), 10)

	tk.Session().GetSessionVars().SetDistSQLScanConcurrency(1)
	tk.MustExec("set tidb_init_chunk_size = 2")
	defer func() {
		tk.MustExec(fmt.Sprintf("set tidb_init_chunk_size = %d", variable.DefInitChunkSize))
	}()
	rs, err := tk.Exec("select * from chk")
	require.NoError(t, err)
	defer func() { require.NoError(t, rs.Close()) }()

	req := rs.NewChunk(nil)
	var count int
	var numChunks int
	for {
		err = rs.Next(context.TODO(), req)
		require.NoError(t, err)
		numRows := req.NumRows()
		if numRows == 0 {
			break
		}
		for i := 0; i < numRows; i++ {
			require.Equal(t, int64(count), req.GetRow(i).GetInt64(0))
			count++
		}
		numChunks++
	}
	require.Equal(t, 100, count)
	// FIXME: revert this result to new group value after distsql can handle initChunkSize.
	require.Equal(t, 1, numChunks)
}

func TestInsertExecChunk(t *testing.T) {
	store := createMockStoreForSchemaTest(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table test1(a int)")
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert test1 values (%d)", i))
	}
	tk.MustExec("create table test2(a int)")

	tk.Session().GetSessionVars().SetDistSQLScanConcurrency(1)
	tk.MustExec("insert into test2(a) select a from test1;")

	rs, err := tk.Exec("select * from test2")
	require.NoError(t, err)
	defer func() { require.NoError(t, rs.Close()) }()
	var idx int
	for {
		req := rs.NewChunk(nil)
		err = rs.Next(context.TODO(), req)
		require.NoError(t, err)
		if req.NumRows() == 0 {
			break
		}

		for rowIdx := 0; rowIdx < req.NumRows(); rowIdx++ {
			row := req.GetRow(rowIdx)
			require.Equal(t, int64(idx), row.GetInt64(0))
			idx++
		}
	}
	require.Equal(t, 100, idx)
}

func TestUpdateExecChunk(t *testing.T) {
	store := createMockStoreForSchemaTest(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table chk(a int)")
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert chk values (%d)", i))
	}

	tk.Session().GetSessionVars().SetDistSQLScanConcurrency(1)
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("update chk set a = a + 100 where a = %d", i))
	}

	rs, err := tk.Exec("select * from chk")
	require.NoError(t, err)
	defer func() { require.NoError(t, rs.Close()) }()
	var idx int
	for {
		req := rs.NewChunk(nil)
		err = rs.Next(context.TODO(), req)
		require.NoError(t, err)
		if req.NumRows() == 0 {
			break
		}

		for rowIdx := 0; rowIdx < req.NumRows(); rowIdx++ {
			row := req.GetRow(rowIdx)
			require.Equal(t, int64(idx+100), row.GetInt64(0))
			idx++
		}
	}

	require.Equal(t, 100, idx)
}

func TestDeleteExecChunk(t *testing.T) {
	store := createMockStoreForSchemaTest(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table chk(a int)")

	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert chk values (%d)", i))
	}

	tk.Session().GetSessionVars().SetDistSQLScanConcurrency(1)

	for i := 0; i < 99; i++ {
		tk.MustExec(fmt.Sprintf("delete from chk where a = %d", i))
	}

	rs, err := tk.Exec("select * from chk")
	require.NoError(t, err)
	defer func() { require.NoError(t, rs.Close()) }()

	req := rs.NewChunk(nil)
	err = rs.Next(context.TODO(), req)
	require.NoError(t, err)
	require.Equal(t, 1, req.NumRows())

	row := req.GetRow(0)
	require.Equal(t, int64(99), row.GetInt64(0))
}

func TestDeleteMultiTableExecChunk(t *testing.T) {
	store := createMockStoreForSchemaTest(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table chk1(a int)")
	tk.MustExec("create table chk2(a int)")

	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert chk1 values (%d)", i))
	}

	for i := 0; i < 50; i++ {
		tk.MustExec(fmt.Sprintf("insert chk2 values (%d)", i))
	}

	tk.Session().GetSessionVars().SetDistSQLScanConcurrency(1)

	tk.MustExec("delete chk1, chk2 from chk1 inner join chk2 where chk1.a = chk2.a")

	rs, err := tk.Exec("select * from chk1")
	require.NoError(t, err)

	var idx int
	for {
		req := rs.NewChunk(nil)
		err = rs.Next(context.TODO(), req)
		require.NoError(t, err)

		if req.NumRows() == 0 {
			break
		}

		for i := 0; i < req.NumRows(); i++ {
			row := req.GetRow(i)
			require.Equal(t, int64(idx+50), row.GetInt64(0))
			idx++
		}
	}
	require.Equal(t, 50, idx)
	require.NoError(t, rs.Close())

	rs, err = tk.Exec("select * from chk2")
	require.NoError(t, err)

	req := rs.NewChunk(nil)
	err = rs.Next(context.TODO(), req)
	require.NoError(t, err)
	require.Equal(t, 0, req.NumRows())
	require.NoError(t, rs.Close())
}

func TestIndexLookUpReaderChunk(t *testing.T) {
	// Since normally a single region mock tikv only returns one partial result we need to manually split the
	// table to test multiple chunks.
	var cluster testutils.Cluster
	store := testkit.CreateMockStore(t, mockstore.WithClusterInspector(func(c testutils.Cluster) {
		mockstore.BootstrapWithSingleStore(c)
		cluster = c
	}))

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists chk")
	tk.MustExec("create table chk (k int unique, c int)")
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert chk values (%d, %d)", i, i))
	}
	tbl, err := domain.GetDomain(tk.Session()).InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("chk"))
	require.NoError(t, err)
	indexStart := tablecodec.EncodeTableIndexPrefix(tbl.Meta().ID, tbl.Indices()[0].Meta().ID)
	cluster.SplitKeys(indexStart, indexStart.PrefixNext(), 10)

	tk.Session().GetSessionVars().IndexLookupSize = 10
	rs, err := tk.Exec("select * from chk order by k")
	require.NoError(t, err)
	req := rs.NewChunk(nil)
	var count int
	for {
		err = rs.Next(context.TODO(), req)
		require.NoError(t, err)
		numRows := req.NumRows()
		if numRows == 0 {
			break
		}
		for i := 0; i < numRows; i++ {
			require.Equal(t, int64(count), req.GetRow(i).GetInt64(0))
			require.Equal(t, int64(count), req.GetRow(i).GetInt64(1))
			count++
		}
	}
	require.Equal(t, 100, count)
	require.NoError(t, rs.Close())

	rs, err = tk.Exec("select k from chk where c < 90 order by k")
	require.NoError(t, err)
	req = rs.NewChunk(nil)
	count = 0
	for {
		err = rs.Next(context.TODO(), req)
		require.NoError(t, err)
		numRows := req.NumRows()
		if numRows == 0 {
			break
		}
		for i := 0; i < numRows; i++ {
			require.Equal(t, int64(count), req.GetRow(i).GetInt64(0))
			count++
		}
	}
	require.Equal(t, 90, count)
	require.NoError(t, rs.Close())
}

func TestTxnSize(t *testing.T) {
	store := createMockStoreForSchemaTest(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists txn_size")
	tk.MustExec("create table txn_size (k int , v varchar(64))")
	tk.MustExec("begin")
	tk.MustExec("insert txn_size values (1, 'dfaasdfsdf')")
	tk.MustExec("insert txn_size values (2, 'dsdfaasdfsdf')")
	tk.MustExec("insert txn_size values (3, 'abcdefghijkl')")
	txn, err := tk.Session().Txn(false)
	require.NoError(t, err)
	require.Greater(t, txn.Size(), 0)
}

func TestValidationRecursion(t *testing.T) {
	// We have to expect that validation functions will call GlobalVarsAccessor.GetGlobalSysVar().
	// This tests for a regression where GetGlobalSysVar() can not safely call the validation
	// function because it might cause infinite recursion.
	// See: https://github.com/pingcap/tidb/issues/30255
	sv := variable.SysVar{Scope: variable.ScopeGlobal, Name: "mynewsysvar", Value: "test", Validation: func(vars *variable.SessionVars, normalizedValue string, originalValue string, scope variable.ScopeFlag) (string, error) {
		return vars.GlobalVarsAccessor.GetGlobalSysVar("mynewsysvar")
	}}
	variable.RegisterSysVar(&sv)

	store := createMockStoreForSchemaTest(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	val, err := sv.Validate(tk.Session().GetSessionVars(), "test2", variable.ScopeGlobal)
	require.NoError(t, err)
	require.Equal(t, "test", val)
}

func TestGlobalAndLocalTxn(t *testing.T) {
	// Because the PD config of check_dev_2 test is not compatible with local/global txn yet,
	// so we will skip this test for now.
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_local_txn = on;")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop placement policy if exists p1")
	tk.MustExec("drop placement policy if exists p2")
	tk.MustExec("create placement policy p1 leader_constraints='[+zone=dc-1]'")
	tk.MustExec("create placement policy p2 leader_constraints='[+zone=dc-2]'")
	tk.MustExec(`create table t1 (c int)
PARTITION BY RANGE (c) (
	PARTITION p0 VALUES LESS THAN (100) placement policy p1,
	PARTITION p1 VALUES LESS THAN (200) placement policy p2
);`)
	defer func() {
		tk.MustExec("drop table if exists t1")
		tk.MustExec("drop placement policy if exists p1")
		tk.MustExec("drop placement policy if exists p2")
	}()

	// set txn_scope to global
	tk.MustExec(fmt.Sprintf("set @@session.txn_scope = '%s';", kv.GlobalTxnScope))
	result := tk.MustQuery("select @@txn_scope;")
	result.Check(testkit.Rows(kv.GlobalTxnScope))

	// test global txn auto commit
	tk.MustExec("insert into t1 (c) values (1)") // write dc-1 with global scope
	result = tk.MustQuery("select * from t1")    // read dc-1 and dc-2 with global scope
	require.Equal(t, 1, len(result.Rows()))

	// begin and commit with global txn scope
	tk.MustExec("begin")
	txn, err := tk.Session().Txn(true)
	require.NoError(t, err)
	require.Equal(t, kv.GlobalTxnScope, tk.Session().GetSessionVars().TxnCtx.TxnScope)
	require.True(t, txn.Valid())
	tk.MustExec("insert into t1 (c) values (1)") // write dc-1 with global scope
	result = tk.MustQuery("select * from t1")    // read dc-1 and dc-2 with global scope
	require.Equal(t, 2, len(result.Rows()))
	require.True(t, txn.Valid())
	tk.MustExec("commit")
	result = tk.MustQuery("select * from t1")
	require.Equal(t, 2, len(result.Rows()))

	// begin and rollback with global txn scope
	tk.MustExec("begin")
	txn, err = tk.Session().Txn(true)
	require.NoError(t, err)
	require.Equal(t, kv.GlobalTxnScope, tk.Session().GetSessionVars().TxnCtx.TxnScope)
	require.True(t, txn.Valid())
	tk.MustExec("insert into t1 (c) values (101)") // write dc-2 with global scope
	result = tk.MustQuery("select * from t1")      // read dc-1 and dc-2 with global scope
	require.Equal(t, 3, len(result.Rows()))
	require.True(t, txn.Valid())
	tk.MustExec("rollback")
	result = tk.MustQuery("select * from t1")
	require.Equal(t, 2, len(result.Rows()))

	timeBeforeWriting := time.Now()
	tk.MustExec("insert into t1 (c) values (101)") // write dc-2 with global scope
	result = tk.MustQuery("select * from t1")      // read dc-1 and dc-2 with global scope
	require.Equal(t, 3, len(result.Rows()))

	failpoint.Enable("tikvclient/injectTxnScope", `return("dc-1")`)
	defer failpoint.Disable("tikvclient/injectTxnScope")
	// set txn_scope to local
	tk.MustExec("set @@session.txn_scope = 'local';")
	result = tk.MustQuery("select @@txn_scope;")
	result.Check(testkit.Rows("local"))

	// test local txn auto commit
	tk.MustExec("insert into t1 (c) values (1)")          // write dc-1 with dc-1 scope
	result = tk.MustQuery("select * from t1 where c = 1") // point get dc-1 with dc-1 scope
	require.Equal(t, 3, len(result.Rows()))
	result = tk.MustQuery("select * from t1 where c < 100") // read dc-1 with dc-1 scope
	require.Equal(t, 3, len(result.Rows()))

	// begin and commit with dc-1 txn scope
	tk.MustExec("begin")
	txn, err = tk.Session().Txn(true)
	require.NoError(t, err)
	require.Equal(t, "dc-1", tk.Session().GetSessionVars().CheckAndGetTxnScope())
	require.True(t, txn.Valid())
	tk.MustExec("insert into t1 (c) values (1)")            // write dc-1 with dc-1 scope
	result = tk.MustQuery("select * from t1 where c < 100") // read dc-1 with dc-1 scope
	require.Equal(t, 4, len(result.Rows()))
	require.True(t, txn.Valid())
	tk.MustExec("commit")
	result = tk.MustQuery("select * from t1 where c < 100")
	require.Equal(t, 4, len(result.Rows()))

	// begin and rollback with dc-1 txn scope
	tk.MustExec("begin")
	txn, err = tk.Session().Txn(true)
	require.NoError(t, err)
	require.Equal(t, "dc-1", tk.Session().GetSessionVars().CheckAndGetTxnScope())
	require.True(t, txn.Valid())
	tk.MustExec("insert into t1 (c) values (1)")            // write dc-1 with dc-1 scope
	result = tk.MustQuery("select * from t1 where c < 100") // read dc-1 with dc-1 scope
	require.Equal(t, 5, len(result.Rows()))
	require.True(t, txn.Valid())
	tk.MustExec("rollback")
	result = tk.MustQuery("select * from t1 where c < 100")
	require.Equal(t, 4, len(result.Rows()))

	// test wrong scope local txn auto commit
	_, err = tk.Exec("insert into t1 (c) values (101)") // write dc-2 with dc-1 scope
	require.Error(t, err)
	require.Regexp(t, ".*out of txn_scope.*", err)
	err = tk.ExecToErr("select * from t1 where c = 101") // point get dc-2 with dc-1 scope
	require.Error(t, err)
	require.Regexp(t, ".*can not be read by.*", err)
	err = tk.ExecToErr("select * from t1 where c > 100") // read dc-2 with dc-1 scope
	require.Error(t, err)
	require.Regexp(t, ".*can not be read by.*", err)
	tk.MustExec("begin")
	err = tk.ExecToErr("select * from t1 where c = 101") // point get dc-2 with dc-1 scope
	require.Error(t, err)
	require.Regexp(t, ".*can not be read by.*", err)
	err = tk.ExecToErr("select * from t1 where c > 100") // read dc-2 with dc-1 scope
	require.Error(t, err)
	require.Regexp(t, ".*can not be read by.*", err)
	tk.MustExec("commit")

	// begin and commit reading & writing the data in dc-2 with dc-1 txn scope
	tk.MustExec("begin")
	txn, err = tk.Session().Txn(true)
	require.NoError(t, err)
	require.Equal(t, "dc-1", tk.Session().GetSessionVars().CheckAndGetTxnScope())
	require.True(t, txn.Valid())
	tk.MustExec("insert into t1 (c) values (101)")       // write dc-2 with dc-1 scope
	err = tk.ExecToErr("select * from t1 where c > 100") // read dc-2 with dc-1 scope
	require.Error(t, err)
	require.Regexp(t, ".*can not be read by.*", err)
	tk.MustExec("insert into t1 (c) values (99)")           // write dc-1 with dc-1 scope
	result = tk.MustQuery("select * from t1 where c < 100") // read dc-1 with dc-1 scope
	require.Equal(t, 5, len(result.Rows()))
	require.True(t, txn.Valid())
	_, err = tk.Exec("commit")
	require.Error(t, err)
	require.Regexp(t, ".*out of txn_scope.*", err)
	// Won't read the value 99 because the previous commit failed
	result = tk.MustQuery("select * from t1 where c < 100") // read dc-1 with dc-1 scope
	require.Equal(t, 4, len(result.Rows()))

	// Stale Read will ignore the cross-dc txn scope.
	require.Equal(t, "dc-1", tk.Session().GetSessionVars().CheckAndGetTxnScope())
	result = tk.MustQuery("select @@txn_scope;")
	result.Check(testkit.Rows("local"))
	err = tk.ExecToErr("select * from t1 where c > 100") // read dc-2 with dc-1 scope
	require.Error(t, err)
	require.Regexp(t, ".*can not be read by.*", err)
	// Read dc-2 with Stale Read (in dc-1 scope)
	timestamp := timeBeforeWriting.Format(time.RFC3339Nano)
	// TODO: check the result of Stale Read when we figure out how to make the time precision more accurate.
	tk.MustExec(fmt.Sprintf("select * from t1 AS OF TIMESTAMP '%s' where c = 101", timestamp))
	tk.MustExec(fmt.Sprintf("select * from t1 AS OF TIMESTAMP '%s' where c > 100", timestamp))
	tk.MustExec(fmt.Sprintf("START TRANSACTION READ ONLY AS OF TIMESTAMP '%s'", timestamp))
	tk.MustExec("select * from t1 where c = 101")
	tk.MustExec("select * from t1 where c > 100")
	tk.MustExec("commit")
	tk.MustExec("set @@tidb_replica_read='closest-replicas'")
	tk.MustExec(fmt.Sprintf("select * from t1 AS OF TIMESTAMP '%s' where c > 100", timestamp))
	tk.MustExec(fmt.Sprintf("START TRANSACTION READ ONLY AS OF TIMESTAMP '%s'", timestamp))
	tk.MustExec("select * from t1 where c = 101")
	tk.MustExec("select * from t1 where c > 100")
	tk.MustExec("commit")

	tk.MustExec("set global tidb_enable_local_txn = off;")
}
