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
