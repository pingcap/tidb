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
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMultiSchemaChangeAddColumns(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

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

	// Test add generate column
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int, b int);")
	tk.MustExec("insert into t values (1, 2);")
	tk.MustExec("alter table t add column c double default 3.0, add column d double as (a + b);")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 2 3 3"))

	// Test add columns with same name
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int default 1, c int default 4);")
	tk.MustGetErrCode("alter table t add column b int default 2, add column b int default 3", errno.ErrUnsupportedDDLOperation)

	// Test add generate column dependents on a modifying column
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int, b int);")
	tk.MustExec("insert into t values (1, 2);")
	tk.MustGetErrCode("alter table t modify column b double, add column c double as (a + b);", errno.ErrUnsupportedDDLOperation)
}

func TestMultiSchemaChangeAddColumnsCancelled(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
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
	originHook := dom.DDL().GetHook()

	// Test for cancelling the job in a middle state.
	tk.MustExec("create table t (a int default 1, b int default 2, c int default 3, d int default 4, " +
		"index(a), index(b), index(c), index(d));")
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
}

func TestMultiSchemaChangeDropColumnsParallel(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
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

func TestMultiSchemaChangeAddIndexes(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

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
	tk.MustQuery("show index from t;").Check(testkit.Rows( /* no index */ ))

	// Test add indexes with drop column.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, c int)")
	tk.MustGetErrCode("alter table t add index t(a), drop column a", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t add index t(a, b), drop column a", errno.ErrUnsupportedDDLOperation)
	tk.MustQuery("show index from t;").Check(testkit.Rows( /* no index */ ))

	// Test add index failed.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b int, c int);")
	tk.MustExec("insert into t values (1, 1, 1), (2, 2, 2), (3, 3, 1);")
	tk.MustGetErrCode("alter table t add unique index i1(a), add unique index i2(a, b), add unique index i3(c);",
		errno.ErrDupEntry)
	tk.MustQuery("show index from t;").Check(testkit.Rows( /* no index */ ))
	tk.MustExec("alter table t add index i1(a), add index i2(a, b), add index i3(c);")
}

func TestMultiSchemaChangeAddIndexesCancelled(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	originHook := dom.DDL().GetHook()

	// Test cancel successfully.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b int, c int);")
	tk.MustExec("insert into t values (1, 2, 3);")
	cancelHook := newCancelJobHook(store, dom, func(job *model.Job) bool {
		// Cancel the job when index 't2' is in write-reorg.
		return job.MultiSchemaInfo.SubJobs[2].SchemaState == model.StateWriteReorganization
	})
	dom.DDL().SetHook(cancelHook)
	tk.MustGetErrCode("alter table t "+
		"add index t(a, b), add index t1(a), "+
		"add index t2(a), add index t3(a, b);", errno.ErrCancelledDDLJob)
	dom.DDL().SetHook(originHook)
	cancelHook.MustCancelDone(t)
	tk.MustQuery("show index from t;").Check(testkit.Rows( /* no index */ ))
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 2 3"))
	tk.MustExec("admin check table t;")

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
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")

	// Test drop same index.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b int, c int, index t(a));")
	tk.MustGetErrCode("alter table t drop index t, drop index t", errno.ErrUnsupportedDDLOperation)

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (id int, c1 int, c2 int, primary key(id) nonclustered, key i1(c1), key i2(c2), key i3(c1, c2));")
	tk.MustExec("insert into t values (1, 2, 3);")
	tk.MustExec("alter table t drop index i1, drop index i2;")
	tk.MustGetErrCode("select * from t use index(i1);", errno.ErrKeyDoesNotExist)
	tk.MustGetErrCode("select * from t use index(i2);", errno.ErrKeyDoesNotExist)
	tk.MustExec("alter table t drop index i3, drop primary key;")
	tk.MustGetErrCode("select * from t use index(primary);", errno.ErrKeyDoesNotExist)
	tk.MustGetErrCode("select * from t use index(i3);", errno.ErrKeyDoesNotExist)

	// Test drop index with drop column.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int default 1, b int default 2, c int default 3, index t(a))")
	tk.MustExec("insert into t values ();")
	tk.MustExec("alter table t drop index t, drop column a")
	tk.MustGetErrCode("select * from t force index(t)", errno.ErrKeyDoesNotExist)
}

func TestMultiSchemaChangeDropIndexesCancelled(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
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
		return job.MultiSchemaInfo.SubJobs[1].SchemaState == model.StatePublic
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

func TestMultiSchemaChangeModifyColumns(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")

	// unsupported ddl operations
	{
		// Test modify and drop with same column
		tk.MustExec("drop table if exists t;")
		tk.MustExec("create table t (a int default 1, b int default 2);")
		tk.MustExec("insert into t values ();")
		tk.MustGetErrCode("alter table t modify column b double, drop column b", errno.ErrUnsupportedDDLOperation)

		// Test modify column related with dropped column
		tk.MustExec("drop table if exists t;")
		tk.MustExec("create table t (a int default 1, b int default 2, c int default 3);")
		tk.MustExec("insert into t values ();")
		tk.MustGetErrCode("alter table t modify column b double after c, drop column c", errno.ErrUnsupportedDDLOperation)

		// Test modify column related with add index
		tk.MustExec("drop table if exists t;")
		tk.MustExec("create table t(a int, b int);")
		tk.MustExec("insert into t values (1, 2);")
		tk.MustGetErrCode("alter table t add index i(a), modify column a int null default 1 after a;", errno.ErrUnsupportedDDLOperation)

		// Test modify column related with add primary index
		tk.MustExec("drop table if exists t;")
		tk.MustExec("create table t(a int, b int);")
		tk.MustExec("insert into t values (1, 2);")
		tk.MustGetErrCode("alter table t add primary key(a), modify column a int null default 1 after a;", errno.ErrUnsupportedDDLOperation)

		// Test modify column related with expression index
		tk.MustExec("drop table if exists t;")
		tk.MustExec("create table t(a int, b int);")
		tk.MustExec("insert into t values (1, 2);")
		tk.MustGetErrCode("alter table t modify column b double, add index idx((a + b));", errno.ErrUnsupportedDDLOperation)
	}

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int default 1, b int default 2);")
	tk.MustExec("insert into t values ();")
	tk.MustExec("alter table t modify column b double default 2 after a, add column c int default 3 after a;")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 3 2"))

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b int, c int);")
	tk.MustExec("insert into t values (1, 2, 3);")
	tk.MustExec("alter table t modify column a bigint, modify column b bigint;")
	tk.MustExec("insert into t values (9223372036854775807, 9223372036854775807, 1);")
	tk.MustQuery("select * from t;").Check(
		testkit.Rows("1 2 3", "9223372036854775807 9223372036854775807 1"))

	// Modify index-covered columns.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b int, c int, index i1(a), index i2(b), index i3(c), index i4(a, b), index i5(a, b, c));")
	tk.MustExec("insert into t values (1, 2, 3);")
	tk.MustExec("alter table t modify column a tinyint, modify column b tinyint, modify column c tinyint;")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 2 3"))
	tk.MustQuery("select * from t use index(i1, i2, i3, i4, i5);").Check(testkit.Rows("1 2 3"))
	tk.MustExec("admin check table t;")

	// Modify index-covered columns with position change.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b int, c int, index i1(a), index i2(b), index i3(c), index i4(a, b), index i5(a, b, c));")
	tk.MustExec("insert into t values (1, 2, 3);")
	tk.MustExec("alter table t modify column a tinyint after c, modify column b tinyint, modify column c tinyint first;")
	tk.MustQuery("select * from t;").Check(testkit.Rows("3 2 1"))
	tk.MustQuery("select * from t use index(i1, i2, i3, i4, i5);").Check(testkit.Rows("3 2 1"))
	tk.MustExec("admin check table t;")

	// Modify columns that require and don't require reorganization of data.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b int, c int, index i1(a), index i2(c, b));")
	tk.MustExec("insert into t values (1, 2, 3), (11, 22, 33);")
	tk.MustExec("alter table t modify column b char(255) after c, modify column a bigint;")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 3 2", "11 33 22"))
	tk.MustQuery("select * from t use index(i1, i2);").Check(testkit.Rows("1 3 2", "11 33 22"))
	tk.MustExec("admin check table t;")

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, _Col$_a double, index _Idx$_i(a, _Col$_a), index i(a, _Col$_a));")
	tk.MustExec("alter table t modify column a tinyint;")
	tk.MustQuery("select count(distinct KEY_NAME) from information_schema.TIDB_INDEXES where TABLE_NAME='t';").Check(testkit.Rows("2"))

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a BIGINT NULL DEFAULT '-283977870758975838', b double);")
	tk.MustExec("insert into t values (-283977870758975838, 0);")
	tk.MustGetErrCode("alter table t change column a c tinyint null default '111' after b, modify column b time null default '13:51:02' FIRST;", errno.ErrDataOutOfRange)

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int, b int);")
	tk.MustExec("insert into t values (1, 2);")
	tk.MustGetErrCode("alter table t add index i(b), modify column a int null default 1 after a;", errno.ErrBadField)
}

func TestMultiSchemaChangeModifyColumnsCancelled(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	originHook := dom.DDL().GetHook()

	// Test for cancelling the job in a middle state.
	tk.MustExec("create table t (a int, b int, c int, index i1(a), unique index i2(b), index i3(a, b));")
	tk.MustExec("insert into t values (1, 2, 3);")
	hook := newCancelJobHook(store, dom, func(job *model.Job) bool {
		return job.MultiSchemaInfo.SubJobs[2].SchemaState == model.StateWriteReorganization
	})
	dom.DDL().SetHook(hook)
	sql := "alter table t modify column a tinyint, modify column b bigint, modify column c char(20);"
	tk.MustGetErrCode(sql, errno.ErrCancelledDDLJob)
	dom.DDL().SetHook(originHook)
	hook.MustCancelDone(t)
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 2 3"))
	tk.MustQuery("select * from t use index (i1, i2, i3);").Check(testkit.Rows("1 2 3"))
	tk.MustExec("admin check table t;")
	tk.MustQuery("select data_type from information_schema.columns where table_name = 't' and column_name = 'c';").
		Check(testkit.Rows("int"))
}

func TestMultiSchemaChangeWithExpressionIndex(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("create table t (a int, b int);")
	tk.MustExec("insert into t values (1, 2), (2, 1);")
	tk.MustGetErrCode("alter table t drop column a, add unique index idx((a + b));", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t add column c int, change column a d bigint, add index idx((a + a));", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t add column c int default 10, add index idx1((a + b)), add unique index idx2((a + b));",
		errno.ErrDupEntry)
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 2", "2 1"))

	originHook := dom.DDL().GetHook()
	hook := &ddl.TestDDLCallback{Do: dom}
	var checkErr error
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if checkErr != nil {
			return
		}
		assert.Equal(t, model.ActionMultiSchemaChange, job.Type)
		if job.MultiSchemaInfo.SubJobs[1].SchemaState == model.StateWriteOnly {
			tk2 := testkit.NewTestKit(t, store)
			tk2.MustExec("use test;")
			_, checkErr = tk2.Exec("update t set a = 3 where a = 1;")
			if checkErr != nil {
				return
			}
			_, checkErr = tk2.Exec("insert into t values (10, 10);")
		}
	}
	dom.DDL().SetHook(hook)
	tk.MustExec("alter table t add column c int default 10, add index idx1((a + b)), add unique index idx2((a + b));")
	require.NoError(t, checkErr)
	dom.DDL().SetHook(originHook)

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b int);")
	tk.MustExec("insert into t values (1, 2), (2, 1);")
	tk.MustExec("alter table t add column c int default 10, add index idx1((a + b)), add unique index idx2((a*10 + b));")
	tk.MustQuery("select * from t use index(idx1, idx2);").Check(testkit.Rows("1 2 10", "2 1 10"))
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
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	c.cancelErr = kv.RunInNewTxn(ctx, c.store, false,
		func(ctx context.Context, txn kv.Transaction) error {
			errs, err := ddl.CancelJobs(txn, []int64{job.ID})
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
