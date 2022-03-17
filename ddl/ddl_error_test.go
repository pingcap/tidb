//// Copyright 2022 PingCAP, Inc.
////
//// Licensed under the Apache License, Version 2.0 (the "License");
//// you may not use this file except in compliance with the License.
//// You may obtain a copy of the License at
////
////     http://www.apache.org/licenses/LICENSE-2.0
////
//// Unless required by applicable law or agreed to in writing, software
//// distributed under the License is distributed on an "AS IS" BASIS,
//// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//// See the License for the specific language governing permissions and
//// limitations under the License.
//

package ddl_test

import (
	"context"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
	"testing"
)

// This test file contains tests that test the expected or unexpected DDL error.
// For expected error, we use SQL to check it.
// For unexpected error, we mock a SQL job to check it.

func TestTableError(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomainWithSchemaLease(t, testLease)
	defer clean()
	ctx := testNewContext(store)

	// Schema ID is wrong, so dropping table is failed.
	doDDLJobErr(t, -1, 1, model.ActionDropTable, nil, ctx, dom.DDL(), store)
	// Table ID is wrong, so dropping table is failed.
	job := doDDLJobErr(t, 2, -1, model.ActionDropTable, nil, ctx, dom.DDL(), store)

	// Table ID or schema ID is wrong, so getting table is failed.
	err := kv.RunInNewTxn(context.Background(), store, false, func(ctx context.Context, txn kv.Transaction) error {
		job.SchemaID = -1
		job.TableID = -1
		m := meta.NewMeta(txn)
		_, err1 := ddl.GetTableInfoAndCancelFaultJob(m, job, job.SchemaID)
		require.Error(t, err1)
		job.SchemaID = 2
		_, err1 = ddl.GetTableInfoAndCancelFaultJob(m, job, job.SchemaID)
		require.Error(t, err1)
		return nil
	})
	require.NoError(t, err)

	// Args is wrong, so creating table is failed.
	doDDLJobErr(t, 1, 1, model.ActionCreateTable, []interface{}{1}, ctx, dom.DDL(), store)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/mockModifyJobSchemaId", `return(-1)`))
	// Table exists, so creating table is failed.
	tk := testkit.NewTestKit(t, store)
	_, err = tk.Exec("create table test.t1(a int)")
	require.Error(t, err)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/mockModifyJobSchemaId"))
	// Table exists, so creating table is failed.
	tk.MustExec("create table test.t2(a int)")
	tk.MustGetErrCode("create table test.t1(a int)", errno.ErrTableExists)
}

func TestViewError(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomainWithSchemaLease(t, testLease)
	defer clean()
	ctx := testNewContext(store)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int)")
	tk.MustExec("create view v as select * from t")

	// Args is wrong, so creating view is failed.
	doDDLJobErr(t, 1, 1, model.ActionCreateView, []interface{}{1}, ctx, dom.DDL(), store)
}

func TestForeignKeyError(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomainWithSchemaLease(t, testLease)
	defer clean()
	ctx := testNewContext(store)

	doDDLJobErr(t, -1, 1, model.ActionAddForeignKey, nil, ctx, dom.DDL(), store)
	doDDLJobErr(t, -1, 1, model.ActionDropForeignKey, nil, ctx, dom.DDL(), store)
}

func TestIndexError(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomainWithSchemaLease(t, testLease)
	defer clean()
	ctx := testNewContext(store)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int)")
	tk.MustExec("alter table t add index a(a)")

	// Schema ID is wrong.
	doDDLJobErr(t, -1, 1, model.ActionAddIndex, nil, ctx, dom.DDL(), store)
	doDDLJobErr(t, -1, 1, model.ActionDropIndex, nil, ctx, dom.DDL(), store)

	// for adding index
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/MockModifyJobArg", `return(true)`))
	_, err := tk.Exec("alter table t add index idx(a)")
	require.Error(t, err)
	_, err = tk.Exec("alter table t drop index a")
	require.Error(t, err)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/MockModifyJobArg"))
}
