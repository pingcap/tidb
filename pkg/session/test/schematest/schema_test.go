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

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/server"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
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
	if kerneltype.IsNextGen() {
		t.Skip("MDL is always enabled and read only in nextgen")
	}
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
	for i := range 100 {
		tk.MustExec(fmt.Sprintf("insert chk values (%d)", i))
	}
	tbl, err := domain.GetDomain(tk.Session()).InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("chk"))
	require.NoError(t, err)
	tableStart := tablecodec.GenTableRecordPrefix(tbl.Meta().ID)
	if kerneltype.IsNextGen() {
		tableStart = store.GetCodec().EncodeKey(tableStart)
	}
	cluster.SplitKeys(tableStart, tableStart.PrefixNext(), 10)

	tk.Session().GetSessionVars().SetDistSQLScanConcurrency(1)
	tk.MustExec("set tidb_init_chunk_size = 2")
	defer func() {
		tk.MustExec(fmt.Sprintf("set tidb_init_chunk_size = %d", vardef.DefInitChunkSize))
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
		for i := range numRows {
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
	for i := range 100 {
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

		for rowIdx := range req.NumRows() {
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
	for i := range 100 {
		tk.MustExec(fmt.Sprintf("insert chk values (%d)", i))
	}

	tk.Session().GetSessionVars().SetDistSQLScanConcurrency(1)
	for i := range 100 {
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

		for rowIdx := range req.NumRows() {
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

	for i := range 100 {
		tk.MustExec(fmt.Sprintf("insert chk values (%d)", i))
	}

	tk.Session().GetSessionVars().SetDistSQLScanConcurrency(1)

	for i := range 99 {
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

	for i := range 100 {
		tk.MustExec(fmt.Sprintf("insert chk1 values (%d)", i))
	}

	for i := range 50 {
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

		for i := range req.NumRows() {
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
	for i := range 100 {
		tk.MustExec(fmt.Sprintf("insert chk values (%d, %d)", i, i))
	}
	tbl, err := domain.GetDomain(tk.Session()).InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("chk"))
	require.NoError(t, err)
	indexStart := tablecodec.EncodeTableIndexPrefix(tbl.Meta().ID, tbl.Indices()[0].Meta().ID)
	if kerneltype.IsNextGen() {
		indexStart = store.GetCodec().EncodeKey(indexStart)
	}
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
		for i := range numRows {
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
		for i := range numRows {
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
	sv := variable.SysVar{Scope: vardef.ScopeGlobal, Name: "mynewsysvar", Value: "test", Validation: func(vars *variable.SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		return vars.GlobalVarsAccessor.GetGlobalSysVar("mynewsysvar")
	}}
	variable.RegisterSysVar(&sv)

	store := createMockStoreForSchemaTest(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	val, err := sv.Validate(tk.Session().GetSessionVars(), "test2", vardef.ScopeGlobal)
	require.NoError(t, err)
	require.Equal(t, "test", val)
}
