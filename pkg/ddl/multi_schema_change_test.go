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
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMultiSchemaChangeAddColumnsCancelled(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t (a int);")
	tk.MustExec("insert into t values (1);")
	hook := newCancelJobHook(t, store, func(job *model.Job) bool {
		// Cancel job when the column 'c' is in write-reorg.
		if job.Type != model.ActionMultiSchemaChange {
			return false
		}
		assertMultiSchema(t, job, 3)
		return job.MultiSchemaInfo.SubJobs[1].SchemaState == model.StateWriteReorganization
	})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/onJobUpdated", hook.OnJobUpdated)
	sql := "alter table t add column b int default 2, add column c int default 3, add column d int default 4;"
	tk.MustGetErrCode(sql, errno.ErrCancelledDDLJob)
	testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/ddl/onJobUpdated")
	hook.MustCancelDone(t)
	tk.MustQuery("select * from t;").Check(testkit.Rows("1"))
}

func TestMultiSchemaChangeAddColumnsParallel(t *testing.T) {
	store := testkit.CreateMockStore(t)
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

func TestMultiSchemaChangeSetAutoIncrementPK(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists tmp_test_cannot_set_auto_increment")
	tk.MustExec(`create table tmp_test_cannot_set_auto_increment(
  id bigint primary key  comment 'ID',
  source_id bigint comment '资源id'
)`)
	tk.MustExec("insert into tmp_test_cannot_set_auto_increment values (1,1), (2,2), (3,3), (114514,114514);")
	tk.MustExec("ALTER TABLE tmp_test_cannot_set_auto_increment MODIFY COLUMN id bigint NOT NULL AUTO_INCREMENT;")
	tk.MustExec("INSERT INTO tmp_test_cannot_set_auto_increment (source_id) VALUES (4), (5);")
	tk.MustQuery("table tmp_test_cannot_set_auto_increment").Check(testkit.Rows(
		"1 1",
		"2 2",
		"3 3",
		"114514 114514",
		"114515 4",
		"114516 5",
	))

	tk.MustQuery("show create table tmp_test_cannot_set_auto_increment").Check(testkit.RowsWithSep("|",
		"tmp_test_cannot_set_auto_increment|CREATE TABLE `tmp_test_cannot_set_auto_increment` (\n"+
			"  `id` bigint(20) NOT NULL AUTO_INCREMENT,\n"+
			"  `source_id` bigint(20) DEFAULT NULL COMMENT '资源id',\n"+
			"  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin AUTO_INCREMENT="+strconv.FormatInt(114515+autoid.GetStep(), 10),
	))
}

func TestMultiSchemaChangeAddColumnsAfterNewColumn(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists tmp_test_unknown_column")
	tk.MustExec(`create table tmp_test_unknown_column(
  id bigint(20) DEFAULT NULL,
  enable_follow tinyint(4) DEFAULT NULL
)`)
	tk.MustExec(`ALTER TABLE tmp_test_unknown_column
ADD COLUMN account_type int NULL COMMENT '激活码帐号类型：1:基础帐号，2:互通帐号' AFTER enable_follow,
ADD COLUMN account_expire_time datetime NULL COMMENT '互通账号过期时间' AFTER account_type`)
	tk.MustQuery("show create table tmp_test_unknown_column").Check(testkit.RowsWithSep("|",
		"tmp_test_unknown_column|CREATE TABLE `tmp_test_unknown_column` (\n"+
			"  `id` bigint(20) DEFAULT NULL,\n"+
			"  `enable_follow` tinyint(4) DEFAULT NULL,\n"+
			"  `account_type` int(11) DEFAULT NULL COMMENT '激活码帐号类型：1:基础帐号，2:互通帐号',\n"+
			"  `account_expire_time` datetime DEFAULT NULL COMMENT '互通账号过期时间'\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))
}

func TestMultiSchemaChangeDropColumnsCancelled(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// Test for cancelling the job in a middle state.
	tk.MustExec("create table t (a int default 1, b int default 2, c int default 3, d int default 4);")
	tk.MustExec("insert into t values ();")
	hook := newCancelJobHook(t, store, func(job *model.Job) bool {
		// Cancel job when the column 'a' is in delete-reorg.
		if job.Type != model.ActionMultiSchemaChange {
			return false
		}
		assertMultiSchema(t, job, 3)
		return job.MultiSchemaInfo.SubJobs[1].SchemaState == model.StateDeleteReorganization
	})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/onJobUpdated", hook.OnJobUpdated)
	tk.MustExec("alter table t drop column b, drop column a, drop column d;")
	testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/ddl/onJobUpdated")
	hook.MustCancelFailed(t)
	tk.MustQuery("select * from t;").Check(testkit.Rows("3"))

	// Test for cancelling the job in public.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int default 1, b int default 2, c int default 3, d int default 4);")
	tk.MustExec("insert into t values ();")
	hook = newCancelJobHook(t, store, func(job *model.Job) bool {
		// Cancel job when the column 'a' is in public.
		if job.Type != model.ActionMultiSchemaChange {
			return false
		}
		assertMultiSchema(t, job, 3)
		return job.MultiSchemaInfo.SubJobs[1].SchemaState == model.StatePublic
	})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/onJobUpdated", hook.OnJobUpdated)
	tk.MustGetErrCode("alter table t drop column b, drop column a, drop column d;", errno.ErrCancelledDDLJob)
	testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/ddl/onJobUpdated")
	hook.MustCancelDone(t)
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 2 3 4"))
}

func TestMultiSchemaChangeDropIndexedColumnsCancelled(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// Test for cancelling the job in a middle state.
	tk.MustExec("create table t (a int default 1, b int default 2, c int default 3, d int default 4, " +
		"index(a), index(b), index(c), index(d));")
	tk.MustExec("insert into t values ();")
	hook := newCancelJobHook(t, store, func(job *model.Job) bool {
		// Cancel job when the column 'a' is in delete-reorg.
		if job.Type != model.ActionMultiSchemaChange {
			return false
		}
		assertMultiSchema(t, job, 3)
		return job.MultiSchemaInfo.SubJobs[1].SchemaState == model.StateDeleteReorganization
	})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/onJobUpdated", hook.OnJobUpdated)
	tk.MustExec("alter table t drop column b, drop column a, drop column d;")
	hook.MustCancelFailed(t)
	tk.MustQuery("select * from t;").Check(testkit.Rows("3"))
}

func TestMultiSchemaChangeDropColumnsParallel(t *testing.T) {
	store := testkit.CreateMockStore(t)
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

func TestMultiSchemaChangeDropColumnAfterAddCompositeIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t (a int, b int, c int);")
	tk.MustGetErrCode("alter table t add index t(a, b), drop column a;", errno.ErrKeyColumnDoesNotExits)
	tk.MustQuery("show index from t;").Check(testkit.Rows())
}

func TestMultiSchemaChangeRenameColumns(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")

	// unsupported ddl operations
	{
		// Test add and rename to same column name
		tk.MustExec("drop table if exists t;")
		tk.MustExec("create table t (a int default 1, b int default 2);")
		tk.MustExec("insert into t values ();")
		tk.MustGetErrCode("alter table t rename column b to c, add column c int", errno.ErrDupFieldName)

		// Test add column related with rename column
		tk.MustExec("drop table if exists t;")
		tk.MustExec("create table t (a int default 1, b int default 2);")
		tk.MustExec("insert into t values ();")
		tk.MustGetErrCode("alter table t rename column b to c, add column e int after b", errno.ErrBadField)

		// Test drop and rename with same column
		tk.MustExec("drop table if exists t;")
		tk.MustExec("create table t (a int default 1, b int default 2);")
		tk.MustExec("insert into t values ();")
		tk.MustGetErrCode("alter table t drop column b, rename column b to c", errno.ErrBadField)

		// Test add index and rename with same column
		tk.MustExec("drop table if exists t;")
		tk.MustExec("create table t (a int default 1, b int default 2, index t(a, b));")
		tk.MustExec("insert into t values ();")
		tk.MustGetErrCode("alter table t rename column b to c, add index t1(a, b)", errno.ErrKeyColumnDoesNotExits)
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
	tk.MustExec("alter table t add column c int default 3, rename column b to d;")
	tk.MustQuery("select a, d, c from t").Check(testkit.Rows("1 2 3"))

	// Test dml stmts when do rename
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int default 1, b int default 2)")
	tk.MustExec("insert into t values ()")
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/onJobRunBefore", func(job *model.Job) {
		assert.Equal(t, model.ActionMultiSchemaChange, job.Type)
		if job.MultiSchemaInfo.SubJobs[0].SchemaState == model.StateWriteReorganization {
			rs, _ := tk2.Exec("select b from t")
			assert.Equal(t, tk2.ResultSetToResult(rs, "").Rows()[0][0], "2")
		}
	})
	tk.MustExec("alter table t add column c int default 3, rename column b to d;")
	testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/ddl/onJobRunBefore")
	tk.MustQuery("select d from t").Check(testkit.Rows("2"))
	tk.MustGetErrCode("select b from t", errno.ErrBadField)
}

func TestMultiSchemaChangeAlterColumns(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// unsupported ddl operations
	{
		// Test alter and drop with same column
		tk.MustExec("drop table if exists t;")
		tk.MustExec("create table t (a int default 1, b int default 2);")
		tk.MustExec("insert into t values ();")
		tk.MustGetErrCode("alter table t alter column b set default 3, drop column b", errno.ErrBadField)

		// Test alter and rename with same column
		tk.MustExec("drop table if exists t;")
		tk.MustExec("create table t (a int default 1, b int default 2);")
		tk.MustExec("insert into t values ();")
		tk.MustGetErrCode("alter table t alter column b set default 3, rename column b to c", errno.ErrBadField)

		// Test alter and drop modify same column
		tk.MustExec("drop table if exists t;")
		tk.MustExec("create table t (a int default 1, b int default 2);")
		tk.MustExec("insert into t values ();")
		tk.MustGetErrCode("alter table t alter column b set default 3, modify column b double", errno.ErrBadField)
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
	tk.MustExec("alter table t add column c int default 3, alter column b set default 3;")
	tk.MustExec("insert into t values ()")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 3 3"))

	// Test dml stmts when do alter
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int default 1, b int default 2)")
	tk.MustExec("alter table t add column c int default 3, alter column b set default 3;")
	tk.MustQuery("select * from t").Check(testkit.Rows())
	tk.MustExec("insert into t values ()")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 3 3"))
}

func TestMultiSchemaChangeChangeColumns(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// unsupported ddl operations
	{
		// Test rename and drop with same column
		tk.MustExec("drop table if exists t;")
		tk.MustExec("create table t (a int default 1, b int default 2);")
		tk.MustExec("insert into t values ();")
		tk.MustGetErrCode("alter table t change column b c double, drop column b", errno.ErrBadField)

		// Test change and add with same column
		tk.MustExec("drop table if exists t;")
		tk.MustExec("create table t (a int default 1, b int default 2);")
		tk.MustExec("insert into t values ();")
		tk.MustGetErrCode("alter table t change column b c double, add column c int", errno.ErrDupFieldName)

		// Test add index and rename with same column
		tk.MustExec("drop table if exists t;")
		tk.MustExec("create table t (a int default 1, b int default 2, index t(a, b));")
		tk.MustExec("insert into t values ();")
		tk.MustGetErrCode("alter table t change column b c double, add index t1(a, b)", errno.ErrKeyColumnDoesNotExits)
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
	tk.MustExec("alter table t add column c int default 3, change column b d bigint default 4;")
	tk.MustQuery("select a, d, c from t").Check(testkit.Rows("1 2 3"))
}

func TestMultiSchemaChangeAddIndexesCancelled(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// Test cancel successfully.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b int, c int);")
	tk.MustExec("insert into t values (1, 2, 3);")
	cancelHook := newCancelJobHook(t, store, func(job *model.Job) bool {
		// Cancel the job when index 't2' is in write-reorg.
		if job.Type != model.ActionMultiSchemaChange {
			return false
		}
		assertMultiSchema(t, job, 1)
		return job.MultiSchemaInfo.SubJobs[0].SchemaState == model.StateWriteReorganization
	})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/onJobUpdated", cancelHook.OnJobUpdated)
	tk.MustGetErrCode("alter table t "+
		"add index t(a, b), add index t1(a), "+
		"add index t2(a), add index t3(a, b);", errno.ErrCancelledDDLJob)
	testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/ddl/onJobUpdated")
	cancelHook.MustCancelDone(t)
	tk.MustQuery("show index from t;").Check(testkit.Rows( /* no index */ ))
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 2 3"))
	tk.MustExec("admin check table t;")

	// Test cancel failed when some sub-jobs have been finished.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b int, c int);")
	tk.MustExec("insert into t values (1, 2, 3);")
	cancelHook = newCancelJobHook(t, store, func(job *model.Job) bool {
		// Cancel the job when index 't1' is in public.
		if job.Type != model.ActionMultiSchemaChange {
			return false
		}
		assertMultiSchema(t, job, 1)
		return job.MultiSchemaInfo.SubJobs[0].SchemaState == model.StatePublic
	})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/onJobUpdated", cancelHook.OnJobUpdated)
	tk.MustExec("alter table t add index t(a, b), add index t1(a), " +
		"add index t2(a), add index t3(a, b);")
	testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/ddl/onJobUpdated")
	cancelHook.MustCancelFailed(t)
	tk.MustQuery("select * from t use index(t, t1, t2, t3);").Check(testkit.Rows("1 2 3"))
	tk.MustExec("admin check table t;")
}

func TestMultiSchemaChangeDropIndexesCancelled(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")

	// Test for cancelling the job in a middle state.
	tk.MustExec("create table t (a int, b int, index(a), unique index(b), index idx(a, b));")
	hook := newCancelJobHook(t, store, func(job *model.Job) bool {
		if job.Type != model.ActionMultiSchemaChange {
			return false
		}
		assertMultiSchema(t, job, 3)
		return job.MultiSchemaInfo.SubJobs[1].SchemaState == model.StateDeleteOnly
	})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/onJobUpdated", hook.OnJobUpdated)
	tk.MustExec("alter table t drop index a, drop index b, drop index idx;")
	testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/ddl/onJobUpdated")
	hook.MustCancelFailed(t)
	tk.MustGetErrCode("select * from t use index (a);", errno.ErrKeyDoesNotExist)
	tk.MustGetErrCode("select * from t use index (b);", errno.ErrKeyDoesNotExist)
	tk.MustGetErrCode("select * from t use index (idx);", errno.ErrKeyDoesNotExist)

	// Test for cancelling the job in none state.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b int, index(a), unique index(b), index idx(a, b));")
	hook = newCancelJobHook(t, store, func(job *model.Job) bool {
		if job.Type != model.ActionMultiSchemaChange {
			return false
		}
		assertMultiSchema(t, job, 3)
		return job.MultiSchemaInfo.SubJobs[1].SchemaState == model.StatePublic
	})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/onJobUpdated", hook.OnJobUpdated)
	tk.MustGetErrCode("alter table t drop index a, drop index b, drop index idx;", errno.ErrCancelledDDLJob)
	testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/ddl/onJobUpdated")
	hook.MustCancelDone(t)
	tk.MustQuery("select * from t use index (a);").Check(testkit.Rows())
	tk.MustQuery("select * from t use index (b);").Check(testkit.Rows())
	tk.MustQuery("select * from t use index (idx);").Check(testkit.Rows())
}

func TestMultiSchemaChangeDropIndexesParallel(t *testing.T) {
	store := testkit.CreateMockStore(t)
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

func TestMultiSchemaChangeRenameIndexes(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

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
	tk.MustGetErrCode("alter table t drop index t, rename index t to t1", errno.ErrKeyDoesNotExist)

	// Test add and rename to same index name.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, c int, index t(a))")
	tk.MustGetErrCode("alter table t add index t1(b), rename index t to t1", errno.ErrDupKeyName)

	// Test drop column with rename index.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int default 1, b int default 2, c int default 3, index t(a))")
	tk.MustExec("insert into t values ();")
	tk.MustGetErrCode("alter table t drop column a, rename index t to x", errno.ErrKeyDoesNotExist)
	tk.MustGetErrCode("select * from t use index (x);", errno.ErrKeyDoesNotExist)
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 2 3"))

	// Test cancel job with renameIndex
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int default 1, b int default 2, index t(a))")
	tk.MustExec("insert into t values ()")
	tk.MustExec("alter table t add column c int default 3, rename index t to t1;")
	tk.MustQuery("select * from t use index (t1);").Check(testkit.Rows("1 2 3"))
	tk.MustQuery("select * from t use index (t);").Check(testkit.Rows("1 2 3"))
}

func TestMultiSchemaChangeModifyColumnsCancelled(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")

	// Test for cancelling the job in a middle state.
	tk.MustExec("create table t (a int, b int, c int, index i1(a), unique index i2(b), index i3(a, b));")
	tk.MustExec("insert into t values (1, 2, 3);")
	hook := newCancelJobHook(t, store, func(job *model.Job) bool {
		if job.Type != model.ActionMultiSchemaChange {
			return false
		}
		assertMultiSchema(t, job, 3)
		return job.MultiSchemaInfo.SubJobs[2].SchemaState == model.StateWriteReorganization
	})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/onJobUpdated", hook.OnJobUpdated)
	sql := "alter table t modify column a tinyint, modify column b bigint, modify column c char(20);"
	tk.MustGetErrCode(sql, errno.ErrCancelledDDLJob)
	testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/ddl/onJobUpdated")
	hook.MustCancelDone(t)
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 2 3"))
	tk.MustQuery("select * from t use index (i1, i2, i3);").Check(testkit.Rows("1 2 3"))
	tk.MustExec("admin check table t;")
	tk.MustQuery("select data_type from information_schema.columns where table_name = 't' and column_name = 'c';").
		Check(testkit.Rows("int"))
}

func TestMultiSchemaChangeAlterIndex(t *testing.T) {
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/disableLossyDDLOptimization", "return(true)")
	defer testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/ddl/disableLossyDDLOptimization")

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test;")

	// unsupported ddl operations
	{
		// Test alter the same index
		tk.MustExec("drop table if exists t;")
		tk.MustExec("create table t (a int, b int, index idx(a, b));")
		tk.MustExec("alter table t alter index idx visible, alter index idx invisible;")

		// Test drop and alter the same index
		tk.MustExec("drop table if exists t;")
		tk.MustExec("create table t (a int, b int, index idx(a, b));")
		tk.MustGetErrCode("alter table t drop index idx, alter index idx visible;", errno.ErrKeyDoesNotExist)

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
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterWaitSchemaSynced", func(job *model.Job) {
		if job.MultiSchemaInfo == nil {
			return
		}
		// "modify column a tinyint" in write-reorg.
		if job.MultiSchemaInfo.SubJobs[1].SchemaState == model.StateWriteReorganization {
			rs, err := tk2.Exec("select * from t use index(i1);")
			assert.NoError(t, err)
			assert.NoError(t, rs.Close())
		}
	})
	tk.MustExec("alter table t alter index i1 invisible, modify column a tinyint, alter index i2 invisible;")
	testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/ddl/afterWaitSchemaSynced")
	tk.MustGetErrCode("select * from t use index (i1);", errno.ErrKeyDoesNotExist)
	tk.MustGetErrCode("select * from t use index (i2);", errno.ErrKeyDoesNotExist)
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 2"))
	tk.MustExec("admin check table t;")
}

func TestMultiSchemaChangeMixCancelled(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("set global tidb_enable_dist_task = 0;")
	tk.MustExec("set global tidb_ddl_enable_fast_reorg = 0;")

	tk.MustExec("create table t (a int, b int, c int, index i1(c), index i2(c));")
	tk.MustExec("insert into t values (1, 2, 3);")
	cancelHook := newCancelJobHook(t, store, func(job *model.Job) bool {
		return job.MultiSchemaInfo != nil &&
			len(job.MultiSchemaInfo.SubJobs) > 8 &&
			job.MultiSchemaInfo.SubJobs[8].SchemaState == model.StateWriteReorganization
	})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/onJobUpdated", cancelHook.OnJobUpdated)
	tk.MustGetErrCode("alter table t add column d int default 4, add index i3(c), "+
		"drop column a, drop column if exists z, add column if not exists e int default 5, "+
		"drop index i2, add column f int default 6, drop column b, drop index i1, add column if not exists g int;",
		errno.ErrCancelledDDLJob)
	testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/ddl/onJobUpdated")
	cancelHook.MustCancelDone(t)
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 2 3"))
	tk.MustQuery("select * from t use index(i1, i2);").Check(testkit.Rows("1 2 3"))
	tk.MustExec("admin check table t;")
}

func TestMultiSchemaChangeAdminShowDDLJobs(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_ddl_enable_fast_reorg = 1;")
	tk.MustExec("create table t (a int, b int, c int)")
	tk.MustExec("insert into t values (1, 2, 3)")

	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/onJobRunBefore", func(job *model.Job) {
		assert.Equal(t, model.ActionMultiSchemaChange, job.Type)
		if job.MultiSchemaInfo.SubJobs[0].SchemaState == model.StateDeleteOnly {
			newTk := testkit.NewTestKit(t, store)
			rows := newTk.MustQuery("admin show ddl jobs 1").Rows()
			// 1 history job and 1 running job with 1 subjobs
			assert.Equal(t, 3, len(rows))
			assert.Equal(t, "test", rows[1][1])
			assert.Equal(t, "t", rows[1][2])
			assert.Equal(t, "add index /* subjob */", rows[1][3])
			assert.Equal(t, "delete only", rows[1][4])
			assert.Equal(t, "running", rows[1][len(rows[1])-2])
			assert.True(t, len(rows[1][8].(string)) > 0)
			assert.True(t, len(rows[1][9].(string)) > 0)
			assert.True(t, len(rows[1][10].(string)) > 0)
			assert.Equal(t, "create table", rows[2][3])
		}
	})

	tk.MustExec("alter table t add index t(a), add index t1(b)")
}

func TestMultiSchemaChangeWithExpressionIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("create table t (a int, b int);")
	tk.MustExec("insert into t values (1, 2), (2, 1);")
	tk.MustGetErrCode("alter table t drop column a, add unique index idx((a + b));", errno.ErrBadField)
	tk.MustGetErrCode("alter table t add column c int, change column a d bigint, add index idx((a + a));", errno.ErrBadField)
	tk.MustGetErrCode("alter table t add column c int default 10, add index idx1((a + b)), add unique index idx2((a + b));",
		errno.ErrDupEntry)
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 2", "2 1"))

	var checkErr error
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/onJobRunBefore", func(job *model.Job) {
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
	})
	tk.MustExec("alter table t add column c int default 10, add index idx1((a + b)), add unique index idx2((a + b));")
	require.NoError(t, checkErr)
	testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/ddl/onJobRunBefore")

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b int);")
	tk.MustExec("insert into t values (1, 2), (2, 1);")
	tk.MustExec("alter table t add column c int default 10, add index idx1((a + b)), add unique index idx2((a*10 + b));")
	tk.MustQuery("select * from t use index(idx1, idx2);").Check(testkit.Rows("1 2 10", "2 1 10"))
}

func TestMultiSchemaChangeNoSubJobs(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")

	tk.MustExec("create table t (a int, b int);")
	tk.MustExec("alter table t add column if not exists a int, add column if not exists b int;")
	tk.MustQuery("show warnings;").Check(testkit.Rows(
		"Note 1060 Duplicate column name 'a'", "Note 1060 Duplicate column name 'b'"))
	rs := tk.MustQuery("admin show ddl jobs 1;").Rows()
	require.Equal(t, "create table", rs[0][3])
}

func TestMultiSchemaChangeSchemaVersion(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("create table t(a int, b int, c int, d int)")
	tk.MustExec("insert into t values (1,2,3,4)")

	schemaVerMap := map[int64]struct{}{}

	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeWaitSchemaChanged", func(_ *model.Job, schemaVer int64) {
		if schemaVer != 0 {
			// No same return schemaVer during multi-schema change
			_, ok := schemaVerMap[schemaVer]
			assert.False(t, ok)
			schemaVerMap[schemaVer] = struct{}{}
		}
	})
	tk.MustExec("alter table t drop column b, drop column c")
	tk.MustExec("alter table t add column b int, add column c int")
	tk.MustExec("alter table t add index k(b), add column e int")
	tk.MustExec("alter table t alter index k invisible, drop column e")
}

func TestMultiSchemaChangeMixedWithUpdate(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("create table t (c_1 int, c_2 char(20), c_pos_1 int, c_idx_visible int, c_3 decimal(5, 3), " +
		"c_drop_1 time, c_4 datetime, c_drop_idx char(10), c_5 time, c_6 double, c_drop_2 int, c_pos_2 char(10), " +
		"c_add_idx_1 int, c_add_idx_2 char(20), index idx_1(c_1), index idx_2(c_2), index idx_drop(c_drop_idx), " +
		"index idx_3(c_drop_1), index idx_4(c_4), index idx_5(c_pos_1, c_pos_2), index idx_visible(c_idx_visible));")
	tk.MustExec("insert into t values (100, 'c_2_insert', 101, 12, 2.1, '10:00:00', " +
		"'2020-01-01 10:00:00', 'wer', '10:00:00', 2.1, 12, 'qwer', 12, 'asdf');")

	var checkErr error
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/onJobRunBefore", func(job *model.Job) {
		if checkErr != nil {
			return
		}
		assert.Equal(t, model.ActionMultiSchemaChange, job.Type)
		// Wait for "drop column c_drop_2" entering delete-only state.
		if job.MultiSchemaInfo.SubJobs[8].SchemaState == model.StateDeleteOnly {
			tk2 := testkit.NewTestKit(t, store)
			tk2.MustExec("use test;")
			_, checkErr = tk2.Exec("update t set c_4 = '2020-01-01 10:00:00', c_5 = 'c_5_update', c_1 = 102, " +
				"c_2 = '1', c_pos_1 = 102, c_idx_visible = 102, c_3 = 3.1, c_drop_idx = 'er', c_6 = 2, c_pos_2 = 'dddd', " +
				"c_add_idx_1 = 102, c_add_idx_2 = 'zxc', c_add_2 = 10001, c_add_1 = 10001 where c_drop_idx = 'wer';")
			if checkErr != nil {
				return
			}
		}
	})
	tk.MustExec("alter table t " +
		"add index i_add_1(c_add_idx_1), " +
		"drop index idx_drop, " +
		"add index i_add_2(c_add_idx_2),  " +
		"modify column c_2 char(100), " +
		"add column c_add_2 bigint, " +
		"modify column c_1 bigint, " +
		"add column c_add_1 bigint, " +
		"modify column c_5 varchar(255) first, " +
		"modify column c_4 datetime first, " +
		"drop column c_drop_1, " +
		"drop column c_drop_2, " +
		"modify column c_6 int, " +
		"alter index idx_visible invisible, " +
		"modify column c_3 decimal(10, 2);")
	testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep")
	require.NoError(t, checkErr)
}

func TestMultiSchemaChangeModifyColumnOrderByStates(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")

	tk.MustExec("create table t (a int, b int);")
	tk.MustExec("insert into t values (1, 1);")
	tk.MustExec("alter table t modify column b smallint, add column d int;")

	tk.MustExec("drop table t;")
	tk.MustExec("create table t (a int, b int);")
	tk.MustExec("insert into t values (1, 1);")
	tk.MustExec("alter table t modify column a smallint, add column c int, modify column b smallint;")

	tk.MustExec("drop table t;")
	tk.MustExec("create table t (a int, b int, c char(10));")
	tk.MustExec("insert into t values (1, 1, '1');")
	tk.MustExec("alter table t modify column c int after a, add column d int, add column e int, modify column b smallint;")

	tk.MustExec("drop table t;")
	tk.MustExec("create table t (id bigint, c1 bigint, c2 bigint);")
	tk.MustExec("alter table t modify column c2 int after id, modify column id int after c2;")

	tk.MustExec("drop table t;")
	tk.MustExec("create table t1(id bigint, c1 bigint, c2 bigint);")
	tk.MustExec("alter table t1 modify column c2 int, drop column id;")
}

func TestMultiSchemaChangeDMLUpdate(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, c int, d int)")

	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/onJobUpdated", func(job *model.Job) {
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("insert into t(a, c) values (1, 2), (2, 3), (3, 4), (4, 5)")
		tk.MustExec("update t set c = 5 where a = 1")
		tk.MustExec("delete from t")
	})
	tk.MustExec("alter table t change column b e int unsigned, change column d f int unsigned")
	testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/ddl/onJobUpdated")

	tk.MustExec("drop table t")
}

func TestMultiSchemaChangeBlockedByRowLevelChecksum(t *testing.T) {
	store := testkit.CreateMockStore(t)

	orig := variable.EnableRowLevelChecksum.Load()
	defer variable.EnableRowLevelChecksum.Store(orig)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (c int)")

	variable.EnableRowLevelChecksum.Store(true)
	tk.Session().GetSessionVars().EnableRowLevelChecksum = false
	tk.MustGetErrCode("alter table t add column c1 int, add column c2 int", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t add (c1 int, c2 int)", errno.ErrUnsupportedDDLOperation)

	variable.EnableRowLevelChecksum.Store(false)
	tk.Session().GetSessionVars().EnableRowLevelChecksum = true
	tk.MustGetErrCode("alter table t add column c1 int, add column c2 int", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t add (c1 int, c2 int)", errno.ErrUnsupportedDDLOperation)
}

func TestMultiSchemaChangePollJobCount(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int)")
	tk.MustExec("insert into t values (1);")
	tk.MustExec("set global tidb_ddl_enable_fast_reorg = 0;")
	tk.MustExec("set global tidb_enable_dist_task = 0;")
	runOneJobCounter := 0
	pollJobCounter := 0
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/onRunOneJobStep", func() {
		runOneJobCounter++
	})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforePollDDLJob", func() {
		pollJobCounter++
	})
	// Should not test reorg DDL because the result can be unstable.
	tk.MustExec("alter table t add column b int,  modify column a bigint, add column c char(10);")
	require.Equal(t, 20, runOneJobCounter)
	require.Equal(t, 10, pollJobCounter)
}

type cancelOnceHook struct {
	store     kv.Storage
	triggered bool
	cancelErr error
	pred      func(job *model.Job) bool
	s         sessionctx.Context
}

func (c *cancelOnceHook) OnJobUpdated(job *model.Job) {
	if c.triggered || !c.pred(job) {
		return
	}
	c.triggered = true
	errs, err := ddl.CancelJobs(context.Background(), c.s, []int64{job.ID})
	if len(errs) > 0 && errs[0] != nil {
		c.cancelErr = errs[0]
		return
	}
	c.cancelErr = err
}

func (c *cancelOnceHook) MustCancelDone(t *testing.T) {
	require.True(t, c.triggered)
	require.NoError(t, c.cancelErr)
}

func (c *cancelOnceHook) MustCancelFailed(t *testing.T) {
	require.True(t, c.triggered)
	require.Contains(t, c.cancelErr.Error(), strconv.Itoa(errno.ErrCannotCancelDDLJob))
}

func newCancelJobHook(t *testing.T, store kv.Storage,
	pred func(job *model.Job) bool) *cancelOnceHook {
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	return &cancelOnceHook{
		store: store,
		pred:  pred,
		s:     tk.Session(),
	}
}

func putTheSameDDLJobTwice(t *testing.T, fn func()) {
	err := failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockParallelSameDDLJobTwice", `return(true)`)
	require.NoError(t, err)
	fn()
	err = failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockParallelSameDDLJobTwice")
	require.NoError(t, err)
}

func assertMultiSchema(t *testing.T, job *model.Job, subJobLen int) {
	assert.NotNil(t, job.MultiSchemaInfo, job)
	assert.Len(t, job.MultiSchemaInfo.SubJobs, subJobLen, job)
}
