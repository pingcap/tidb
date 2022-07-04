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

	// Test add multiple columns and constraints in one spec.
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
	tk.MustExec("create table t (a int);")
	tk.MustExec("insert into t values (1);")
	tk.MustExec("alter table t add column (index i(a), index i1(a));")
	tk.MustQuery("select * from t use index (i, i1);").Check(testkit.Rows("1"))

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int);")
	tk.MustExec("insert into t values (1);")
	tk.MustExec("alter table t add column (b int default 2, index i(a), primary key (a));")
	tk.MustQuery("select * from t use index (i, primary)").Check(testkit.Rows("1 2"))

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int);")
	tk.MustExec("insert into t values (1);")
	tk.MustExec("alter table t add column (index i(a));")
	tk.MustQuery("select * from t use index (i)").Check(testkit.Rows("1"))

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

func TestMultiSchemaChangeRenameColumns(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	originHook := dom.DDL().GetHook()
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// unsupported ddl operations
	{
		// Test add and rename to same column name
		tk.MustExec("drop table if exists t;")
		tk.MustExec("create table t (a int default 1, b int default 2);")
		tk.MustExec("insert into t values ();")
		tk.MustGetErrCode("alter table t rename column b to c, add column c int", errno.ErrUnsupportedDDLOperation)

		// Test add column related with rename column
		tk.MustExec("drop table if exists t;")
		tk.MustExec("create table t (a int default 1, b int default 2);")
		tk.MustExec("insert into t values ();")
		tk.MustGetErrCode("alter table t rename column b to c, add column e int after b", errno.ErrUnsupportedDDLOperation)

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

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int default 1, b int default 2, index t(a, b));")
	tk.MustExec("insert into t values ();")
	tk.MustExec("alter table t rename column b to c, add column e int default 3")
	tk.MustQuery("select c from t").Check(testkit.Rows("2"))
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2 3"))

	// Test cancel job with rename columns
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int default 1, b int default 2)")
	tk.MustExec("insert into t values ()")
	hook := newCancelJobHook(store, dom, func(job *model.Job) bool {
		// Cancel job when the column 'c' is in write-reorg.
		return job.MultiSchemaInfo.SubJobs[0].SchemaState == model.StateWriteReorganization
	})
	dom.DDL().SetHook(hook)
	tk.MustGetErrCode("alter table t add column c int default 3, rename column b to d;", errno.ErrCancelledDDLJob)
	dom.DDL().SetHook(originHook)
	tk.MustQuery("select b from t").Check(testkit.Rows("2"))
	tk.MustGetErrCode("select d from t", errno.ErrBadField)

	// Test dml stmts when do rename
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int default 1, b int default 2)")
	tk.MustExec("insert into t values ()")
	hook1 := &ddl.TestDDLCallback{Do: dom}
	hook1.OnJobRunBeforeExported = func(job *model.Job) {
		assert.Equal(t, model.ActionMultiSchemaChange, job.Type)
		if job.MultiSchemaInfo.SubJobs[0].SchemaState == model.StateWriteReorganization {
			rs, _ := tk.Exec("select b from t")
			assert.Equal(t, tk.ResultSetToResult(rs, "").Rows()[0][0], "2")
		}
	}
	dom.DDL().SetHook(hook1)
	tk.MustExec("alter table t add column c int default 3, rename column b to d;")
	dom.DDL().SetHook(originHook)
	tk.MustQuery("select d from t").Check(testkit.Rows("2"))
	tk.MustGetErrCode("select b from t", errno.ErrBadField)
}

func TestMultiSchemaChangeAlterColumns(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	originHook := dom.DDL().GetHook()
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// unsupported ddl operations
	{
		// Test alter and drop with same column
		tk.MustExec("drop table if exists t;")
		tk.MustExec("create table t (a int default 1, b int default 2);")
		tk.MustExec("insert into t values ();")
		tk.MustGetErrCode("alter table t alter column b set default 3, drop column b", errno.ErrUnsupportedDDLOperation)

		// Test alter and rename with same column
		tk.MustExec("drop table if exists t;")
		tk.MustExec("create table t (a int default 1, b int default 2);")
		tk.MustExec("insert into t values ();")
		tk.MustGetErrCode("alter table t alter column b set default 3, rename column b to c", errno.ErrUnsupportedDDLOperation)

		// Test alter and drop modify same column
		tk.MustExec("drop table if exists t;")
		tk.MustExec("create table t (a int default 1, b int default 2);")
		tk.MustExec("insert into t values ();")
		tk.MustGetErrCode("alter table t alter column b set default 3, modify column b double", errno.ErrUnsupportedDDLOperation)
	}

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int default 1, b int default 2, index t(a, b));")
	tk.MustExec("insert into t values ();")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2"))
	tk.MustExec("alter table t rename column a to c, alter column b set default 3;")
	tk.MustExec("truncate table t;")
	tk.MustExec("insert into t values ();")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 3"))

	// Test cancel job with alter columns
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int default 1, b int default 2)")
	hook := newCancelJobHook(store, dom, func(job *model.Job) bool {
		// Cancel job when the column 'a' is in write-reorg.
		return job.MultiSchemaInfo.SubJobs[0].SchemaState == model.StateWriteReorganization
	})
	dom.DDL().SetHook(hook)
	tk.MustGetErrCode("alter table t add column c int default 3, alter column b set default 3;", errno.ErrCancelledDDLJob)
	dom.DDL().SetHook(originHook)
	tk.MustExec("insert into t values ()")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2"))

	// Test dml stmts when do alter
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int default 1, b int default 2)")
	hook1 := &ddl.TestDDLCallback{Do: dom}
	hook1.OnJobRunBeforeExported = func(job *model.Job) {
		assert.Equal(t, model.ActionMultiSchemaChange, job.Type)
		if job.MultiSchemaInfo.SubJobs[0].SchemaState == model.StateWriteOnly {
			tk.Exec("insert into t values ()")
		}
	}
	dom.DDL().SetHook(hook1)
	tk.MustExec("alter table t add column c int default 3, alter column b set default 3;")
	dom.DDL().SetHook(originHook)
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2 3"))
}

func TestMultiSchemaChangeChangeColumns(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	originHook := dom.DDL().GetHook()
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// unsupported ddl operations
	{
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

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int default 1, b int default 2, index t(a, b));")
	tk.MustExec("insert into t values ();")
	tk.MustExec("alter table t rename column b to c, change column a e bigint default 3;")
	tk.MustQuery("select e,c from t").Check(testkit.Rows("1 2"))
	tk.MustExec("truncate table t;")
	tk.MustExec("insert into t values ();")
	tk.MustQuery("select e,c from t").Check(testkit.Rows("3 2"))

	// Test cancel job with change columns
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int default 1, b int default 2)")
	tk.MustExec("insert into t values ()")
	hook := newCancelJobHook(store, dom, func(job *model.Job) bool {
		// Cancel job when the column 'c' is in write-reorg.
		return job.MultiSchemaInfo.SubJobs[0].SchemaState == model.StateWriteReorganization
	})
	dom.DDL().SetHook(hook)
	tk.MustGetErrCode("alter table t add column c int default 3, change column b d bigint default 4;", errno.ErrCancelledDDLJob)
	dom.DDL().SetHook(originHook)
	tk.MustQuery("select b from t").Check(testkit.Rows("2"))
	tk.MustGetErrCode("select d from t", errno.ErrBadField)
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

	// Test add indexes with drop column.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, c int)")
	tk.MustGetErrCode("alter table t add index t(a), drop column a", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t add index t(a, b), drop column a", errno.ErrUnsupportedDDLOperation)

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

func TestMultiSchemaChangeRenameIndexes(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	originHook := dom.DDL().GetHook()

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
	tk.MustExec("alter table t drop column a, rename index t to x")
	tk.MustGetErrCode("select * from t use index (x);", errno.ErrKeyDoesNotExist)
	tk.MustQuery("select * from t;").Check(testkit.Rows("2 3"))

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
	tk.MustExec("create table t(a bigint null default '1761233443433596323', index t(a));")
	tk.MustExec("insert into t set a = '-7184819032643664798';")
	tk.MustGetErrCode("alter table t change column a b datetime null default '8972-12-24 10:56:03', rename index t to t1;", errno.ErrTruncatedWrongValue)

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b double, index i(a, b));")
	tk.MustExec("alter table t rename index i to i1, change column b c int;")
	tk.MustQuery("select count(*) from information_schema.TIDB_INDEXES where TABLE_NAME='t' and COLUMN_NAME='c' and KEY_NAME='i1';").Check(testkit.Rows("1"))

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b double, index i(a, b), index _Idx$_i(a, b));")
	tk.MustExec("alter table t rename index i to i1, change column b c int;")
	tk.MustQuery("select count(*) from information_schema.TIDB_INDEXES where TABLE_NAME='t' and COLUMN_NAME='c' and KEY_NAME='i1';").Check(testkit.Rows("1"))
	tk.MustQuery("select count(*) from information_schema.TIDB_INDEXES where TABLE_NAME='t' and COLUMN_NAME='c' and KEY_NAME='_Idx$_i';").Check(testkit.Rows("1"))

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, _Col$_a double, index _Idx$_i(a, _Col$_a), index i(a, _Col$_a));")
	tk.MustExec("alter table t modify column a tinyint;")
	tk.MustQuery("select count(distinct KEY_NAME) from information_schema.TIDB_INDEXES where TABLE_NAME='t';").Check(testkit.Rows("2"))

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a BIGINT NULL DEFAULT '-283977870758975838', b double);")
	tk.MustExec("insert into t values (-283977870758975838, 0);")
	tk.MustGetErrCode("alter table t change column a c tinyint null default '111' after b, modify column b time null default '13:51:02' FIRST;", errno.ErrDataOutOfRange)
	rows := tk.MustQuery("admin show ddl jobs 1").Rows()
	require.Equal(t, rows[0][11], "rollback done")
	require.Equal(t, rows[1][11], "rollback done")
	require.Equal(t, rows[2][11], "cancelled")

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

func TestMultiSchemaChangeAlterIndex(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")

	// unsupported ddl operations
	{
		// Test alter the same index
		tk.MustExec("drop table if exists t;")
		tk.MustExec("create table t (a int, b int, index idx(a, b));")
		tk.MustGetErrCode("alter table t alter index idx visible, alter index idx invisible;", errno.ErrUnsupportedDDLOperation)

		// Test drop and alter the same index
		tk.MustExec("drop table if exists t;")
		tk.MustExec("create table t (a int, b int, index idx(a, b));")
		tk.MustGetErrCode("alter table t drop index idx, alter index idx visible;", errno.ErrUnsupportedDDLOperation)

		// Test add and alter the same index
		tk.MustExec("drop table if exists t;")
		tk.MustExec("create table t (a int, b int);")
		tk.MustGetErrCode("alter table t add index idx(a, b), alter index idx invisible", errno.ErrKeyDoesNotExist)
	}

	tk.MustExec("drop table t;")
	tk.MustExec("create table t (a int, b int, index i1(a, b), index i2(b));")
	tk.MustExec("insert into t values (1, 2);")
	tk.MustExec("alter table t modify column a tinyint, alter index i2 invisible, alter index i1 invisible;")
	tk.MustGetErrCode("select * from t use index (i1);", errno.ErrKeyDoesNotExist)
	tk.MustGetErrCode("select * from t use index (i2);", errno.ErrKeyDoesNotExist)
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 2"))
	tk.MustExec("admin check table t;")

	tk.MustExec("drop table t;")
	tk.MustExec("create table t (a int, b int, index i1(a, b), index i2(b));")
	tk.MustExec("insert into t values (1, 2);")
	originHook := dom.DDL().GetHook()
	var checked bool
	dom.DDL().SetHook(&ddl.TestDDLCallback{Do: dom,
		OnJobUpdatedExported: func(job *model.Job) {
			assert.NotNil(t, job.MultiSchemaInfo)
			// "modify column a tinyint" in write-reorg.
			if job.MultiSchemaInfo.SubJobs[1].SchemaState == model.StateWriteReorganization {
				checked = true
				rs, err := tk.Exec("select * from t use index(i1);")
				assert.NoError(t, err)
				assert.NoError(t, rs.Close())
			}
		}})
	tk.MustExec("alter table t alter index i1 invisible, modify column a tinyint, alter index i2 invisible;")
	dom.DDL().SetHook(originHook)
	require.True(t, checked)
	tk.MustGetErrCode("select * from t use index (i1);", errno.ErrKeyDoesNotExist)
	tk.MustGetErrCode("select * from t use index (i2);", errno.ErrKeyDoesNotExist)
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 2"))
	tk.MustExec("admin check table t;")
}

func TestMultiSchemaChangeMix(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")

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

func TestMultiSchemaChangeMixCancelled(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")

	tk.MustExec("create table t (a int, b int, c int, index i1(c), index i2(c));")
	tk.MustExec("insert into t values (1, 2, 3);")
	origin := dom.DDL().GetHook()
	cancelHook := newCancelJobHook(store, dom, func(job *model.Job) bool {
		return job.MultiSchemaInfo != nil &&
			len(job.MultiSchemaInfo.SubJobs) > 8 &&
			job.MultiSchemaInfo.SubJobs[8].SchemaState == model.StateWriteReorganization
	})
	dom.DDL().SetHook(cancelHook)
	tk.MustGetErrCode("alter table t add column d int default 4, add index i3(c), "+
		"drop column a, drop column if exists z, add column if not exists e int default 5, "+
		"drop index i2, add column f int default 6, drop column b, drop index i1, add column if not exists g int;",
		errno.ErrCancelledDDLJob)
	dom.DDL().SetHook(origin)
	cancelHook.MustCancelDone(t)
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 2 3"))
	tk.MustQuery("select * from t use index(i1, i2);").Check(testkit.Rows("1 2 3"))
	tk.MustExec("admin check table t;")
}

func TestMultiSchemaChangeAdminShowDDLJobs(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
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
			assert.Equal(t, rows[2][4], "none")
			assert.Equal(t, rows[2][len(rows[2])-1], "queueing")
		}
	}

	tk.MustExec("create table t (a int, b int, c int)")
	tk.MustExec("insert into t values (1, 2, 3)")

	dom.DDL().SetHook(hook)
	tk.MustExec("alter table t add index t(a), add index t1(b)")
	dom.DDL().SetHook(originHook)
}

func TestMultiSchemaChangeTableOption(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("create table t (a int auto_increment primary key, b int);")
	tk.MustExec("alter table t modify column b tinyint, auto_increment = 100;")
	tk.MustExec("insert into t (b) values (1);")
	tk.MustQuery("select * from t;").Check(testkit.Rows("100 1"))

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int auto_increment primary key, b int);")
	tk.MustExec("alter table t auto_increment = 110, auto_increment = 90;")
	tk.MustQuery("show warnings;").Check(testkit.Rows("Note 1105 Can't reset AUTO_INCREMENT to 90 without FORCE option, using 110 instead"))
	tk.MustExec("insert into t (b) values (1);")
	tk.MustQuery("select * from t;").Check(testkit.Rows("110 1"))

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b int) charset = utf8 shard_row_id_bits=2;")
	tk.MustExec("alter table t modify column a tinyint, shard_row_id_bits = 3, comment = 'abc', charset = utf8mb4;")
	tk.MustQuery("select TIDB_ROW_ID_SHARDING_INFO, TABLE_COMMENT, TABLE_COLLATION from information_schema.tables where table_name = 't';").
		Check(testkit.Rows("SHARD_BITS=3 abc utf8mb4_bin"))
}

func TestMultiSchemaChangeNonPublicDefaultValue(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("create table t (a tinyint);")
	tk.MustExec("insert into t set a = 10;")
	tk.MustExec("alter table t add column b int not null, change column a c char(5) first;")
	tk.MustQuery("select * from t;").Check(testkit.Rows("10 0"))
}

func TestMultiSchemaChangeAlterIndexVisibility(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("create table t (a int, b int, index idx(b));")
	tk.MustExec("alter table t add index idx2(a), alter index idx visible;")
	tk.MustQuery("select * from t use index (idx, idx2);").Check(testkit.Rows( /* no rows */ ))
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

func TestMultiSchemaChangeNoSubJobs(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")

	tk.MustExec("create table t (a int, b int);")
	tk.MustExec("alter table t add column if not exists a int, add column if not exists b int;")
	tk.MustQuery("show warnings;").Check(testkit.Rows(
		"Note 1060 Duplicate column name 'a'", "Note 1060 Duplicate column name 'b'"))
	rs := tk.MustQuery("admin show ddl jobs 1;").Rows()
	require.Equal(t, "create table", rs[0][3])
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
