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

package executor_test

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil/consistency"
	"github.com/pingcap/tidb/util/mock"
	"github.com/stretchr/testify/require"
)

func TestAdminCheckTableFailed(t *testing.T) {
	store, domain, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists admin_test")
	tk.MustExec("create table admin_test (c1 int, c2 int, c3 varchar(255) default '1', primary key(c1), key(c3), unique key(c2), key(c2, c3))")
	tk.MustExec("insert admin_test (c1, c2, c3) values (-10, -20, 'y'), (-1, -10, 'z'), (1, 11, 'a'), (2, 12, 'b'), (5, 15, 'c'), (10, 20, 'd'), (20, 30, 'e')")

	// Make some corrupted index. Build the index information.
	ctx := mock.NewContext()
	ctx.Store = store
	is := domain.InfoSchema()
	dbName := model.NewCIStr("test")
	tblName := model.NewCIStr("admin_test")
	tbl, err := is.TableByName(dbName, tblName)
	require.NoError(t, err)
	tblInfo := tbl.Meta()
	idxInfo := tblInfo.Indices[1]
	indexOpr := tables.NewIndex(tblInfo.ID, tblInfo, idxInfo)
	sc := ctx.GetSessionVars().StmtCtx
	tk.Session().GetSessionVars().IndexLookupSize = 3
	tk.Session().GetSessionVars().MaxChunkSize = 3

	// Reduce one row of index.
	// Table count > index count.
	// Index c2 is missing 11.
	txn, err := store.Begin()
	require.NoError(t, err)
	err = indexOpr.Delete(sc, txn, types.MakeDatums(-10), kv.IntHandle(-1))
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	err = tk.ExecToErr("admin check table admin_test")
	require.Error(t, err)
	require.EqualError(t, err, "[admin:8223]data inconsistency in table: admin_test, index: c2, handle: -1, index-values:\"\" != record-values:\"handle: -1, values: [KindInt64 -10]\"")
	require.True(t, consistency.ErrAdminCheckInconsistent.Equal(err))
	tk.MustExec("set @@tidb_redact_log=1;")
	err = tk.ExecToErr("admin check table admin_test")
	require.Error(t, err)
	require.EqualError(t, err, "[admin:8223]data inconsistency in table: admin_test, index: c2, handle: ?, index-values:\"?\" != record-values:\"?\"")
	tk.MustExec("set @@tidb_redact_log=0;")
	r := tk.MustQuery("admin recover index admin_test c2")
	r.Check(testkit.Rows("1 7"))
	tk.MustExec("admin check table admin_test")

	// Add one row of index.
	// Table count < index count.
	// Index c2 has one more values than table data: 0, and the handle 0 hasn't correlative record.
	txn, err = store.Begin()
	require.NoError(t, err)
	_, err = indexOpr.Create(ctx, txn, types.MakeDatums(0), kv.IntHandle(0), nil)
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	err = tk.ExecToErr("admin check table admin_test")
	require.Error(t, err)
	require.EqualError(t, err, "[admin:8223]data inconsistency in table: admin_test, index: c2, handle: 0, index-values:\"handle: 0, values: [KindInt64 0 KindInt64 0]\" != record-values:\"\"")
	tk.MustExec("set @@tidb_redact_log=1;")
	err = tk.ExecToErr("admin check table admin_test")
	require.Error(t, err)
	require.EqualError(t, err, "[admin:8223]data inconsistency in table: admin_test, index: c2, handle: ?, index-values:\"?\" != record-values:\"?\"")
	tk.MustExec("set @@tidb_redact_log=0;")

	// Add one row of index.
	// Table count < index count.
	// Index c2 has two more values than table data: 10, 13, and these handles have correlative record.
	txn, err = store.Begin()
	require.NoError(t, err)
	err = indexOpr.Delete(sc, txn, types.MakeDatums(0), kv.IntHandle(0))
	require.NoError(t, err)
	// Make sure the index value "19" is smaller "21". Then we scan to "19" before "21".
	_, err = indexOpr.Create(ctx, txn, types.MakeDatums(19), kv.IntHandle(10), nil)
	require.NoError(t, err)
	_, err = indexOpr.Create(ctx, txn, types.MakeDatums(13), kv.IntHandle(2), nil)
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	err = tk.ExecToErr("admin check table admin_test")
	require.Error(t, err)
	require.EqualError(t, err, "[executor:8134]data inconsistency in table: admin_test, index: c2, col: c2, handle: \"2\", index-values:\"KindInt64 13\" != record-values:\"KindInt64 12\", compare err:<nil>")
	tk.MustExec("set @@tidb_redact_log=1;")
	err = tk.ExecToErr("admin check table admin_test")
	require.Error(t, err)
	require.EqualError(t, err, "[executor:8134]data inconsistency in table: admin_test, index: c2, col: c2, handle: \"?\", index-values:\"?\" != record-values:\"?\", compare err:\"?\"")
	tk.MustExec("set @@tidb_redact_log=0;")

	// Table count = index count.
	// Two indices have the same handle.
	txn, err = store.Begin()
	require.NoError(t, err)
	err = indexOpr.Delete(sc, txn, types.MakeDatums(13), kv.IntHandle(2))
	require.NoError(t, err)
	err = indexOpr.Delete(sc, txn, types.MakeDatums(12), kv.IntHandle(2))
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	err = tk.ExecToErr("admin check table admin_test")
	require.Error(t, err)
	require.EqualError(t, err, "[executor:8134]data inconsistency in table: admin_test, index: c2, col: c2, handle: \"10\", index-values:\"KindInt64 19\" != record-values:\"KindInt64 20\", compare err:<nil>")
	tk.MustExec("set @@tidb_redact_log=1;")
	err = tk.ExecToErr("admin check table admin_test")
	require.Error(t, err)
	require.EqualError(t, err, "[executor:8134]data inconsistency in table: admin_test, index: c2, col: c2, handle: \"?\", index-values:\"?\" != record-values:\"?\", compare err:\"?\"")
	tk.MustExec("set @@tidb_redact_log=0;")

	// Table count = index count.
	// Index c2 has one line of data is 19, the corresponding table data is 20.
	txn, err = store.Begin()
	require.NoError(t, err)
	_, err = indexOpr.Create(ctx, txn, types.MakeDatums(12), kv.IntHandle(2), nil)
	require.NoError(t, err)
	err = indexOpr.Delete(sc, txn, types.MakeDatums(20), kv.IntHandle(10))
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	err = tk.ExecToErr("admin check table admin_test")
	require.Error(t, err)
	require.EqualError(t, err, "[executor:8134]data inconsistency in table: admin_test, index: c2, col: c2, handle: \"10\", index-values:\"KindInt64 19\" != record-values:\"KindInt64 20\", compare err:<nil>")
	tk.MustExec("set @@tidb_redact_log=1;")
	err = tk.ExecToErr("admin check table admin_test")
	require.Error(t, err)
	require.EqualError(t, err, "[executor:8134]data inconsistency in table: admin_test, index: c2, col: c2, handle: \"?\", index-values:\"?\" != record-values:\"?\", compare err:\"?\"")
	tk.MustExec("set @@tidb_redact_log=0;")

	// Recover records.
	txn, err = store.Begin()
	require.NoError(t, err)
	err = indexOpr.Delete(sc, txn, types.MakeDatums(19), kv.IntHandle(10))
	require.NoError(t, err)
	_, err = indexOpr.Create(ctx, txn, types.MakeDatums(20), kv.IntHandle(10), nil)
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	tk.MustExec("admin check table admin_test")
}
