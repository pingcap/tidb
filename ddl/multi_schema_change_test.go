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

package ddl_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util/admin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMultiSchemaChangeAddColumns(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@global.tidb_enable_change_multi_schema = 1")

	// Test add multiple columns in multiple specs.
	tk.MustExec("create table t (a int);")
	tk.MustExec("insert into t values (1);")
	tk.MustExec("alter table t add column b int default 2, add column c int default 3;")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 2 3"))

	// Test add multiple columns in one spec.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int);")
	tk.MustExec("insert into t values (1);")
	tk.MustExec("alter table t add column (b int default 2, c int default 3);")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 2 3"))

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b int, c int);")
	tk.MustExec("insert into t values (1, 2, 3);")
	tk.MustExec("alter table t add column (d int default 4, e int default 5);")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 2 3 4 5"))

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int default 1);")
	tk.MustExec("insert into t values ();")
	tk.MustExec("alter table t add column if not exists (b int default 2, c int default 3);")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 2 3"))
	tk.MustExec("alter table t add column if not exists (c int default 3, d int default 4);")
	tk.MustQuery("show warnings;").Check(testkit.Rows("Note 1060 Duplicate column name 'c'"))
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 2 3 4"))

	// Test referencing previous column in multi-schema change is not supported.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int);")
	tk.MustGetErrCode("alter table t add column b int after a, add column c int after b", errno.ErrBadField)
	tk.MustGetErrCode("alter table t add column c int after b, add column b int", errno.ErrBadField)

	// Test add multiple columns with different position.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b int, c int);")
	tk.MustExec("insert into t values (1, 2, 3);")
	tk.MustExec(`alter table t
        add column d int default 4 first,
        add column e int default 5 after b,
        add column f int default 6 after b;`)
	tk.MustQuery("select * from t;").Check(testkit.Rows("4 1 2 6 5 3"))

	// Test [if not exists] for adding columns.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int default 1);")
	tk.MustExec("insert into t values ();")
	tk.MustExec("alter table t add column b int default 2, add column if not exists a int;")
	tk.MustQuery("show warnings;").Check(testkit.Rows("Note 1060 Duplicate column name 'a'"))
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 2"))

	// Test add columns with same name
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int default 1, c int default 4);")
	tk.MustGetErrCode("alter table t add column b int default 2, add column b int default 3", errno.ErrUnsupportedDDLOperation)
}

func TestMultiSchemaChangeAddColumnsCancelled(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@global.tidb_enable_change_multi_schema = 1")
	originHook := dom.DDL().GetHook()

	tk.MustExec("create table t (a int);")
	tk.MustExec("insert into t values (1);")
	hook := newCancelJobHook(store, dom, func(job *model.Job) bool {
		// Cancel job when the column 'c' is in write-reorg.
		return job.MultiSchemaInfo.SubJobs[1].SchemaState == model.StateWriteReorganization
	})
	dom.DDL().SetHook(hook)
	sql := "alter table t add column b int default 2, add column c int default 3, add column d int default 4;"
	tk.MustGetErrCode(sql, errno.ErrCancelledDDLJob)
	dom.DDL().SetHook(originHook)
	hook.MustCancelDone(t)
	tk.MustQuery("select * from t;").Check(testkit.Rows("1"))
}

func TestMultiSchemaChangeAddColumnsParallel(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@global.tidb_enable_change_multi_schema = 1;")
	tk.MustExec("create table t (a int default 1);")
	tk.MustExec("insert into t values ();")
	putTheSameDDLJobTwice(t, func() {
		tk.MustExec("alter table t add column if not exists b int default 2, " +
			"add column if not exists c int default 3;")
		tk.MustQuery("show warnings").Check(testkit.Rows(
			"Note 1060 Duplicate column name 'b'",
			"Note 1060 Duplicate column name 'c'",
		))
	})
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 2 3"))
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int);")
	putTheSameDDLJobTwice(t, func() {
		tk.MustGetErrCode("alter table t add column b int, add column c int;", errno.ErrDupFieldName)
	})
}

func TestMultiSchemaChangeDropColumns(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("set @@global.tidb_enable_change_multi_schema = 1;")

	// Test drop all columns
	tk.MustExec("create table t (a int, b int);")
	tk.MustGetErrCode("alter table t drop column a, drop column b;", errno.ErrCantRemoveAllFields)

	// Test drop multiple columns in multiple specs
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b int, c int, d int, e int);")
	tk.MustExec("insert into t values (1, 2, 3, 4, 5);")
	tk.MustExec("alter table t drop column a, drop column d, drop column b;")
	tk.MustQuery("select * from t;").Check(testkit.Rows("3 5"))

	// Test drop same column
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int default 1, c int default 4);")
	tk.MustGetErrCode("alter table t drop column a, drop column a", errno.ErrUnsupportedDDLOperation)

	// Test drop if exists column.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int default 1, b int default 2);")
	tk.MustExec("insert into t values ();")
	tk.MustExec("alter table t drop column if exists c, drop column a;")
	tk.MustQuery("show warnings;").Check(testkit.Rows("Note 1091 Can't DROP 'c'; check that column/key exists"))
	tk.MustQuery("select * from t;").Check(testkit.Rows("2"))
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int default 1, b int default 2, c int default 3);")
	tk.MustExec("insert into t values ();")
	tk.MustExec("alter table t drop column a, drop column if exists d, drop column c;")
	tk.MustQuery("show warnings;").Check(testkit.Rows("Note 1091 Can't DROP 'd'; check that column/key exists"))
	tk.MustQuery("select * from t;").Check(testkit.Rows("2"))
}

func TestMultiSchemaChangeDropColumnsCancelled(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@global.tidb_enable_change_multi_schema = 1")
	originHook := dom.DDL().GetHook()

	// Test for cancelling the job in a middle state.
	tk.MustExec("create table t (a int default 1, b int default 2, c int default 3, d int default 4);")
	tk.MustExec("insert into t values ();")
	hook := newCancelJobHook(store, dom, func(job *model.Job) bool {
		// Cancel job when the column 'a' is in delete-reorg.
		return job.MultiSchemaInfo.SubJobs[1].SchemaState == model.StateDeleteReorganization
	})
	dom.DDL().SetHook(hook)
	tk.MustExec("alter table t drop column b, drop column a, drop column d;")
	dom.DDL().SetHook(originHook)
	hook.MustCancelFailed(t)
	tk.MustQuery("select * from t;").Check(testkit.Rows("3"))

	// Test for cancelling the job in public.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int default 1, b int default 2, c int default 3, d int default 4);")
	tk.MustExec("insert into t values ();")
	hook = newCancelJobHook(store, dom, func(job *model.Job) bool {
		// Cancel job when the column 'a' is in public.
		return job.MultiSchemaInfo.SubJobs[1].SchemaState == model.StatePublic
	})
	dom.DDL().SetHook(hook)
	tk.MustGetErrCode("alter table t drop column b, drop column a, drop column d;", errno.ErrCancelledDDLJob)
	dom.DDL().SetHook(originHook)
	hook.MustCancelDone(t)
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 2 3 4"))
}

func TestMultiSchemaChangeDropIndexedColumnsCancelled(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@global.tidb_enable_change_multi_schema = 1")
	originHook := dom.DDL().GetHook()
	getIndexID := func(name string) int64 {
		tt, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
		require.NoError(t, err)
		for _, idx := range tt.Indices() {
			if idx.Meta().Name.L == name {
				return idx.Meta().ID
			}
		}
		return -1
	}

	// Test for cancelling the job in a middle state.
	tk.MustExec("create table t (a int default 1, b int default 2, c int default 3, d int default 4, " +
		"index(a), index(b), index(c), index(d));")
	tk.MustExec("insert into t values ();")
	hook := newCancelJobHook(store, dom, func(job *model.Job) bool {
		// Cancel job when the column 'a' is in delete-reorg.
		return job.MultiSchemaInfo.SubJobs[1].SchemaState == model.StateDeleteReorganization
	})
	jobIDExt := wrapJobIDExtCallback(originHook)
	dom.DDL().SetHook(composeHooks(dom, jobIDExt, hook))
	ib, ia, id := getIndexID("b"), getIndexID("a"), getIndexID("d")
	tk.MustExec("alter table t drop column b, drop column a, drop column d;")
	dom.DDL().SetHook(originHook)
	hook.MustCancelFailed(t)
	tk.MustQuery("select * from t;").Check(testkit.Rows("3"))
	checkDelRangeAdded(tk, jobIDExt.jobID, ib)
	checkDelRangeAdded(tk, jobIDExt.jobID, ia)
	checkDelRangeAdded(tk, jobIDExt.jobID, id)
}

func TestMultiSchemaChangeDropColumnsParallel(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@global.tidb_enable_change_multi_schema = 1;")
	tk.MustExec("create table t (a int, b int, c int);")
	putTheSameDDLJobTwice(t, func() {
		tk.MustExec("alter table t drop column if exists b, drop column if exists c;")
		tk.MustQuery("show warnings").Check(testkit.Rows(
			"Note 1091 column b doesn't exist",
			"Note 1091 column c doesn't exist"))
	})
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b int, c int);")
	putTheSameDDLJobTwice(t, func() {
		tk.MustGetErrCode("alter table t drop column b, drop column a;", errno.ErrCantDropFieldOrKey)
	})
}

func TestMultiSchemaChangeAddDropColumns(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("set @@global.tidb_enable_change_multi_schema = 1;")

	// [a, b] -> [+c, -a, +d, -b] -> [c, d]
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int default 1, b int default 2);")
	tk.MustExec("insert into t values ();")
	tk.MustExec("alter table t add column c int default 3, drop column a, add column d int default 4, drop column b;")
	tk.MustQuery("select * from t;").Check(testkit.Rows("3 4"))

	// [a, b] -> [-a, -b, +c, +d] -> [c, d]
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int default 1, b int default 2);")
	tk.MustExec("insert into t values ();")
	tk.MustExec("alter table t drop column a, drop column b, add column c int default 3, add column d int default 4;")
	tk.MustQuery("select * from t;").Check(testkit.Rows("3 4"))

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int default 1, b int default 2);")
	tk.MustExec("insert into t values ();")
	tk.MustGetErrCode("alter table t add column c int default 3 after a, add column d int default 4 first, drop column a, drop column b;", errno.ErrUnsupportedDDLOperation)
}

func TestMultiSchemaRenameColumns(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@global.tidb_enable_change_multi_schema = 1")

	// Test add and rename to same column name
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int default 1, b int default 2);")
	tk.MustExec("insert into t values ();")
	tk.MustGetErrCode("alter table t rename column b to c, add column c int", errno.ErrUnsupportedDDLOperation)

	// Test drop and rename with same column
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int default 1, b int default 2);")
	tk.MustExec("insert into t values ();")
	tk.MustGetErrCode("alter table t drop column b, rename column b to c", errno.ErrUnsupportedDDLOperation)

	// Test add index and rename with same column
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int default 1, b int default 2, index t(a, b));")
	tk.MustExec("insert into t values ();")
	tk.MustGetErrCode("alter table t rename column b to c, add index t1(a, b)", errno.ErrUnsupportedDDLOperation)
}

func TestMultiSchemaModifyColumns(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@global.tidb_enable_change_multi_schema = 1")

	// Test modify and drop with same column
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int default 1, b int default 2);")
	tk.MustExec("insert into t values ();")
	tk.MustGetErrCode("alter table t modify column b double, drop column b", errno.ErrUnsupportedDDLOperation)
}

func TestMultiSchemaAlterColumns(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@global.tidb_enable_change_multi_schema = 1")

	// Test alter and drop with same column
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int default 1, b int default 2);")
	tk.MustExec("insert into t values ();")
	tk.MustGetErrCode("alter table t alter column b set default 3, drop column b", errno.ErrUnsupportedDDLOperation)
}

func TestMultiSchemaChangeColumns(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@global.tidb_enable_change_multi_schema = 1")

	// Test change and drop with same column
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int default 1, b int default 2);")
	tk.MustExec("insert into t values ();")
	tk.MustGetErrCode("alter table t change column b c double, drop column b", errno.ErrUnsupportedDDLOperation)

	// Test change and add with same column
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int default 1, b int default 2);")
	tk.MustExec("insert into t values ();")
	tk.MustGetErrCode("alter table t change column b c double, add column c int", errno.ErrUnsupportedDDLOperation)

	// Test add index and rename with same column
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int default 1, b int default 2, index t(a, b));")
	tk.MustExec("insert into t values ();")
	tk.MustGetErrCode("alter table t change column b c double, add index t1(a, b)", errno.ErrUnsupportedDDLOperation)
}

func TestMultiSchemaChangeAddIndexes(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@global.tidb_enable_change_multi_schema = 1;")

	// Test add multiple indexes with same column.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b int, c int);")
	tk.MustExec("insert into t values (1, 2, 3);")
	tk.MustExec("alter table t add index t(a, b), add index t1(a);")
	tk.MustExec("alter table t add index t2(a), add index t3(a, b);")
	tk.MustQuery("select * from t use index (t, t1, t2, t3);").Check(testkit.Rows("1 2 3"))
	tk.MustExec("admin check table t;")

	// Test add multiple indexes with same name.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, c int)")
	tk.MustGetErrCode("alter table t add index t(a), add index t(b)", errno.ErrUnsupportedDDLOperation)

	// Test add indexes with drop column.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, c int)")
	tk.MustGetErrCode("alter table t add index t(a), drop column a", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t add index t(a, b), drop column a", errno.ErrUnsupportedDDLOperation)
}

func TestMultiSchemaChangeAddIndexesCancelled(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@global.tidb_enable_change_multi_schema = 1;")
	originHook := dom.DDL().GetHook()

	// Test cancel successfully.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b int, c int);")
	tk.MustExec("insert into t values (1, 2, 3);")
	jobIDExt := wrapJobIDExtCallback(originHook)
	idxIDExt := newIdxIDExtHook(store, dom)
	cancelHook := newCancelJobHook(store, dom, func(job *model.Job) bool {
		// Cancel the job when index 't2' is in write-reorg.
		return job.MultiSchemaInfo.SubJobs[2].SchemaState == model.StateWriteReorganization
	})
	dom.DDL().SetHook(composeHooks(dom, jobIDExt, idxIDExt, cancelHook))
	tk.MustGetErrCode("alter table t "+
		"add index t(a, b), add index t1(a), "+
		"add index t2(a), add index t3(a, b);", errno.ErrCancelledDDLJob)
	dom.DDL().SetHook(originHook)
	cancelHook.MustCancelDone(t)
	tk.MustQuery("show index from t;").Check(testkit.Rows( /* no index */ ))
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 2 3"))
	tk.MustExec("admin check table t;")
	// Check the adding indexes are added to del-ranges.
	require.NotEqual(t, -1, idxIDExt.IndexID("t"))
	require.NotEqual(t, -1, idxIDExt.IndexID("t1"))
	require.NotEqual(t, -1, idxIDExt.IndexID("t2"))
	checkDelRangeAdded(tk, jobIDExt.jobID, idxIDExt.IndexID("t"))
	checkDelRangeAdded(tk, jobIDExt.jobID, idxIDExt.IndexID("t1"))
	checkDelRangeAdded(tk, jobIDExt.jobID, idxIDExt.IndexID("t2"))

	// Test cancel failed when some sub-jobs have been finished.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b int, c int);")
	tk.MustExec("insert into t values (1, 2, 3);")
	cancelHook = newCancelJobHook(store, dom, func(job *model.Job) bool {
		// Cancel the job when index 't1' is in public.
		return job.MultiSchemaInfo.SubJobs[1].SchemaState == model.StatePublic
	})
	dom.DDL().SetHook(cancelHook)
	tk.MustExec("alter table t add index t(a, b), add index t1(a), " +
		"add index t2(a), add index t3(a, b);")
	dom.DDL().SetHook(originHook)
	cancelHook.MustCancelFailed(t)
	tk.MustQuery("select * from t use index(t, t1, t2, t3);").Check(testkit.Rows("1 2 3"))
	tk.MustExec("admin check table t;")
}

func TestMultiSchemaChangeDropIndexes(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("set @@global.tidb_enable_change_multi_schema = 1;")
	originHook := dom.DDL().GetHook()
	getIndexID := func(name string) int64 {
		tt, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
		require.NoError(t, err)
		for _, idx := range tt.Indices() {
			if idx.Meta().Name.L == name {
				return idx.Meta().ID
			}
		}
		return -1
	}

	jobIDExt := wrapJobIDExtCallback(originHook)
	dom.DDL().SetHook(jobIDExt)

	// Test drop same index.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b int, c int, index t(a));")
	tk.MustGetErrCode("alter table t drop index t, drop index t", errno.ErrUnsupportedDDLOperation)

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (id int, c1 int, c2 int, primary key(id) nonclustered, key i1(c1), key i2(c2), key i3(c1, c2));")
	tk.MustExec("insert into t values (1, 2, 3);")
	jobIDExt.Clear()
	i1, i2 := getIndexID("i1"), getIndexID("i2")
	tk.MustExec("alter table t drop index i1, drop index i2;")
	tk.MustGetErrCode("select * from t use index(i1);", errno.ErrKeyDoesNotExist)
	tk.MustGetErrCode("select * from t use index(i2);", errno.ErrKeyDoesNotExist)
	checkDelRangeAdded(tk, jobIDExt.jobID, i1)
	checkDelRangeAdded(tk, jobIDExt.jobID, i2)
	jobIDExt.Clear()
	pk, i3 := getIndexID("primary"), getIndexID("i3")
	tk.MustExec("alter table t drop index i3, drop primary key;")
	tk.MustGetErrCode("select * from t use index(primary);", errno.ErrKeyDoesNotExist)
	tk.MustGetErrCode("select * from t use index(i3);", errno.ErrKeyDoesNotExist)
	checkDelRangeAdded(tk, jobIDExt.jobID, pk)
	checkDelRangeAdded(tk, jobIDExt.jobID, i3)

	// Test drop index with drop column.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int default 1, b int default 2, c int default 3, index t(a))")
	tk.MustExec("insert into t values ();")
	jobIDExt.Clear()
	idx := getIndexID("t")
	tk.MustExec("alter table t drop index t, drop column a")
	tk.MustGetErrCode("select * from t force index(t)", errno.ErrKeyDoesNotExist)
	checkDelRangeAdded(tk, jobIDExt.jobID, idx)

	dom.DDL().SetHook(originHook)
}

func TestMultiSchemaChangeDropIndexesCancelled(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("set @@global.tidb_enable_change_multi_schema = 1;")
	originHook := dom.DDL().GetHook()

	// Test for cancelling the job in a middle state.
	tk.MustExec("create table t (a int, b int, index(a), unique index(b), index idx(a, b));")
	hook := newCancelJobHook(store, dom, func(job *model.Job) bool {
		return job.MultiSchemaInfo.SubJobs[1].SchemaState == model.StateDeleteOnly
	})
	dom.DDL().SetHook(hook)
	tk.MustExec("alter table t drop index a, drop index b, drop index idx;")
	dom.DDL().SetHook(originHook)
	hook.MustCancelFailed(t)
	tk.MustGetErrCode("select * from t use index (a);", errno.ErrKeyDoesNotExist)
	tk.MustGetErrCode("select * from t use index (b);", errno.ErrKeyDoesNotExist)
	tk.MustGetErrCode("select * from t use index (idx);", errno.ErrKeyDoesNotExist)

	// Test for cancelling the job in none state.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b int, index(a), unique index(b), index idx(a, b));")
	hook = newCancelJobHook(store, dom, func(job *model.Job) bool {
		return job.MultiSchemaInfo.SubJobs[1].SchemaState == model.StateNone
	})
	dom.DDL().SetHook(hook)
	tk.MustGetErrCode("alter table t drop index a, drop index b, drop index idx;", errno.ErrCancelledDDLJob)
	dom.DDL().SetHook(originHook)
	hook.MustCancelDone(t)
	tk.MustQuery("select * from t use index (a);").Check(testkit.Rows())
	tk.MustQuery("select * from t use index (b);").Check(testkit.Rows())
	tk.MustQuery("select * from t use index (idx);").Check(testkit.Rows())
}

func TestMultiSchemaChangeDropIndexesParallel(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@global.tidb_enable_change_multi_schema = 1;")
	tk.MustExec("create table t (a int, b int, c int, index(a), index(b), index(c));")
	putTheSameDDLJobTwice(t, func() {
		tk.MustExec("alter table t drop index if exists b, drop index if exists c;")
		tk.MustQuery("show warnings").Check(testkit.Rows(
			"Note 1091 index b doesn't exist",
			"Note 1091 index c doesn't exist"))
	})
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b int, c int, index (a), index(b), index(c));")
	putTheSameDDLJobTwice(t, func() {
		tk.MustGetErrCode("alter table t drop index b, drop index a;", errno.ErrCantDropFieldOrKey)
	})
}

func TestMultiSchemaChangeAddDropIndexes(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@global.tidb_enable_change_multi_schema = 1")

	// Test add and drop same index.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, c int, index t(a))")
	tk.MustGetErrCode("alter table t drop index t, add index t(b)", errno.ErrDupKeyName)

	// Test add and drop same index.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, c int, index t(a))")
	tk.MustGetErrCode("alter table t add index t1(b), drop index t1", errno.ErrCantDropFieldOrKey)

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b int, c int, index (a), index(b), index(c));")
	tk.MustExec("insert into t values (1, 2, 3);")
	tk.MustExec("alter table t add index aa(a), drop index a, add index cc(c), drop index b, drop index c, add index bb(b);")
	tk.MustQuery("select * from t use index(aa, bb, cc);").Check(testkit.Rows("1 2 3"))
	tk.MustGetErrCode("select * from t use index(a);", errno.ErrKeyDoesNotExist)
	tk.MustGetErrCode("select * from t use index(b);", errno.ErrKeyDoesNotExist)
	tk.MustGetErrCode("select * from t use index(c);", errno.ErrKeyDoesNotExist)
	tk.MustExec("admin check table t;")
}

func TestMultiSchemaRenameIndexes(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@global.tidb_enable_change_multi_schema = 1")
	originHook := dom.DDL().GetHook()
	getIndexID := func(name string) int64 {
		tt, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
		require.NoError(t, err)
		for _, idx := range tt.Indices() {
			if idx.Meta().Name.L == name {
				return idx.Meta().ID
			}
		}
		return -1
	}

	// Test rename index.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, c int, index t(a), index t1(b))")
	tk.MustExec("alter table t rename index t to x, rename index t1 to x1")
	tk.MustExec("select * from t use index (x);")
	tk.MustExec("select * from t use index (x1);")
	tk.MustGetErrCode("select * from t use index (t);", errno.ErrKeyDoesNotExist)
	tk.MustGetErrCode("select * from t use index (t1);", errno.ErrKeyDoesNotExist)

	// Test drop and rename same index.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, c int, index t(a))")
	tk.MustGetErrCode("alter table t drop index t, rename index t to t1", errno.ErrUnsupportedDDLOperation)

	// Test add and rename to same index name.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, c int, index t(a))")
	tk.MustGetErrCode("alter table t add index t1(b), rename index t to t1", errno.ErrUnsupportedDDLOperation)

	// Test drop column with rename index.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int default 1, b int default 2, c int default 3, index t(a))")
	tk.MustExec("insert into t values ();")
	i1 := getIndexID("t")
	jobIDExt := wrapJobIDExtCallback(originHook)
	dom.DDL().SetHook(jobIDExt)
	tk.MustExec("alter table t drop column a, rename index t to x")
	tk.MustGetErrCode("select * from t use index (x);", errno.ErrKeyDoesNotExist)
	tk.MustQuery("select * from t;").Check(testkit.Rows("2 3"))
	checkDelRangeAdded(tk, jobIDExt.jobID, i1)

	// Test cancel job with renameIndex
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int default 1, b int default 2, index t(a))")
	tk.MustExec("insert into t values ()")
	hook := newCancelJobHook(store, dom, func(job *model.Job) bool {
		// Cancel job when the column 'c' is in write-reorg.
		return job.MultiSchemaInfo.SubJobs[0].SchemaState == model.StateWriteReorganization
	})
	dom.DDL().SetHook(hook)
	tk.MustGetErrCode("alter table t add column c int default 3, rename index t to t1;", errno.ErrCancelledDDLJob)
	dom.DDL().SetHook(originHook)
	tk.MustQuery("select * from t use index (t);").Check(testkit.Rows("1 2"))
	tk.MustGetErrCode("select * from t use index (t1);", errno.ErrKeyDoesNotExist)
}

func TestMultiSchemaChangeModifyColumns(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("set @@global.tidb_enable_change_multi_schema = 1;")

	tk.MustExec("create table t (a int, b int, c int);")
	tk.MustExec("alter table t modify column a bigint, modify column b bigint;")
	tk.MustExec("insert into t values (9223372036854775807, 9223372036854775807, 1);")
	tk.MustQuery("select * from t;").Check(
		testkit.Rows("9223372036854775807 9223372036854775807 1"))
}

func TestMultiSchemaChangeMix(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("set @@global.tidb_enable_change_multi_schema = 1;")

	tk.MustExec("create table t (a int, b int, c int, index i1(c), index i2(c));")
	tk.MustExec("insert into t values (1, 2, 3);")
	tk.MustExec("alter table t add column d int default 4, add index i3(c), " +
		"drop column a, drop column if exists z, add column if not exists e int default 5, " +
		"drop index i2, add column f int default 6, drop column b, drop index i1, add column if not exists c int;")
	tk.MustQuery("select * from t;").Check(testkit.Rows("3 4 5 6"))
	tk.MustGetErrCode("select * from t use index (i1);", errno.ErrKeyDoesNotExist)
	tk.MustGetErrCode("select * from t use index (i2);", errno.ErrKeyDoesNotExist)
	tk.MustQuery("select * from t use index (i3);").Check(testkit.Rows("3 4 5 6"))
}

func TestMultiSchemaChangeAdminShowDDLJobs(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@global.tidb_enable_change_multi_schema = 1")
	originHook := dom.DDL().GetHook()
	hook := &ddl.TestDDLCallback{Do: dom}
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		assert.Equal(t, model.ActionMultiSchemaChange, job.Type)
		if job.MultiSchemaInfo.SubJobs[0].SchemaState == model.StateDeleteOnly {
			newTk := testkit.NewTestKit(t, store)
			rows := newTk.MustQuery("admin show ddl jobs 1").Rows()
			// 1 history job and 1 running job with 2 subjobs
			assert.Equal(t, len(rows), 4)
			assert.Equal(t, rows[1][1], "test")
			assert.Equal(t, rows[1][2], "t")
			assert.Equal(t, rows[1][3], "add index /* subjob */")
			assert.Equal(t, rows[1][4], "delete only")
			assert.Equal(t, rows[1][len(rows[1])-1], "running")

			assert.Equal(t, rows[2][3], "add index /* subjob */")
			assert.Equal(t, rows[2][4], "queueing")
			assert.Equal(t, rows[2][len(rows[2])-1], "none")
		}
	}

	tk.MustExec("create table t (a int, b int, c int)")
	tk.MustExec("insert into t values (1, 2, 3)")

	dom.DDL().SetHook(hook)
	tk.MustExec("alter table t add index t(a), add index t1(b)")
	dom.DDL().SetHook(originHook)
}

func composeHooks(dom *domain.Domain, cbs ...ddl.Callback) ddl.Callback {
	return &ddl.TestDDLCallback{
		Do: dom,
		OnJobRunBeforeExported: func(job *model.Job) {
			for _, c := range cbs {
				c.OnJobRunBefore(job)
			}
		},
		OnJobUpdatedExported: func(job *model.Job) {
			for _, c := range cbs {
				c.OnJobUpdated(job)
			}
		},
	}
}

type idxIDExt struct {
	store       kv.Storage
	idxNameToID map[string]int64
	getIdxErr   error

	ddl.TestDDLCallback
}

func newIdxIDExtHook(store kv.Storage, dom *domain.Domain) *idxIDExt {
	return &idxIDExt{store: store, idxNameToID: map[string]int64{}, TestDDLCallback: ddl.TestDDLCallback{Do: dom}}
}

func (i *idxIDExt) OnJobUpdated(job *model.Job) {
	i.getIdxErr = kv.RunInNewTxn(context.Background(), i.store, false,
		func(ctx context.Context, txn kv.Transaction) error {
			t := meta.NewMeta(txn)
			tbl, err := t.GetTable(job.SchemaID, job.TableID)
			if err != nil {
				return err
			}
			for _, idx := range tbl.Indices {
				i.idxNameToID[idx.Name.L] = idx.ID
			}
			return nil
		})
}

func (i *idxIDExt) IndexID(name string) int64 {
	id, ok := i.idxNameToID[name]
	if !ok {
		return -1
	}
	return id
}

type cancelOnceHook struct {
	store     kv.Storage
	triggered bool
	cancelErr error
	pred      func(job *model.Job) bool

	ddl.TestDDLCallback
}

func (c *cancelOnceHook) OnJobUpdated(job *model.Job) {
	if c.triggered || !c.pred(job) {
		return
	}
	c.triggered = true
	c.cancelErr = kv.RunInNewTxn(context.Background(), c.store, false,
		func(ctx context.Context, txn kv.Transaction) error {
			errs, err := admin.CancelJobs(txn, []int64{job.ID})
			if errs[0] != nil {
				return errs[0]
			}
			return err
		})
}

func (c *cancelOnceHook) MustCancelDone(t *testing.T) {
	require.True(t, c.triggered)
	require.NoError(t, c.cancelErr)
}

func (c *cancelOnceHook) MustCancelFailed(t *testing.T) {
	require.True(t, c.triggered)
	require.Contains(t, c.cancelErr.Error(), strconv.Itoa(errno.ErrCannotCancelDDLJob))
}

func newCancelJobHook(store kv.Storage, dom *domain.Domain,
	pred func(job *model.Job) bool) *cancelOnceHook {
	return &cancelOnceHook{
		store:           store,
		pred:            pred,
		TestDDLCallback: ddl.TestDDLCallback{Do: dom},
	}
}

func putTheSameDDLJobTwice(t *testing.T, fn func()) {
	err := failpoint.Enable("github.com/pingcap/tidb/ddl/mockParallelSameDDLJobTwice", `return(true)`)
	require.NoError(t, err)
	fn()
	err = failpoint.Disable("github.com/pingcap/tidb/ddl/mockParallelSameDDLJobTwice")
	require.NoError(t, err)
}
