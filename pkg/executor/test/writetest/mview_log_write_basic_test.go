// Copyright 2026 PingCAP, Inc.
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

package writetest

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/testkit"
)

func execAsMViewMaintenance(tk *testkit.TestKit, sql string) {
	vars := tk.Session().GetSessionVars()
	origMaint := vars.InMViewMaintenance
	origRestr := vars.InRestrictedSQL
	vars.InMViewMaintenance = true
	vars.InRestrictedSQL = true
	defer func() {
		vars.InMViewMaintenance = origMaint
		vars.InRestrictedSQL = origRestr
	}()
	tk.MustExec(sql)
}

func TestMLogInsert(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_mview_enable = on")

	tk.MustExec("create table t (a int primary key, b int, c int)")
	tk.MustExec("create materialized view log on t (a, b, c)")

	// Single-row insert.
	tk.MustExec("insert into t values (1, 10, 100)")
	tk.MustQuery(
		"select a, b, c, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`",
	).Check(testkit.Rows(
		"1 10 100 I 1",
	))

	// Multi-row insert.
	execAsMViewMaintenance(tk, "delete from `$mlog$t`")
	tk.MustExec("insert into t values (2,20,200), (3,30,300), (4,40,400)")
	tk.MustQuery(
		"select a, b, c, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t` order by a",
	).Check(testkit.Rows(
		"2 20 200 I 1",
		"3 30 300 I 1",
		"4 40 400 I 1",
	))

	// Partial-column insert with DEFAULT value.
	tk.MustExec("drop materialized view log on t")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int primary key, b int, c int default 99)")
	tk.MustExec("create materialized view log on t (a, b, c)")
	tk.MustExec("insert into t (a, b) values (5, 50)")
	tk.MustQuery(
		"select a, b, c, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`",
	).Check(testkit.Rows(
		"5 50 99 I 1",
	))
}

func TestMLogUpdate(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_mview_enable = on")

	tk.MustExec("create table t (a int primary key, b int unique, c int)")
	tk.MustExec("insert into t values (1,10,100), (2,20,200), (3,30,300)")
	tk.MustExec("create materialized view log on t (a, b, c)")

	// Single-row update.
	tk.MustExec("update t set c=101 where a=1")
	tk.MustQuery(
		"select a, b, c, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`",
	).Sort().Check(testkit.Rows(
		"1 10 100 U -1",
		"1 10 101 U 1",
	))

	// Multi-row update.
	execAsMViewMaintenance(tk, "delete from `$mlog$t`")
	tk.MustExec("update t set c = c + 1 where a in (2, 3)")
	tk.MustQuery(
		"select a, b, c, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`",
	).Sort().Check(testkit.Rows(
		"2 20 200 U -1",
		"2 20 201 U 1",
		"3 30 300 U -1",
		"3 30 301 U 1",
	))
}

func TestMLogDelete(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_mview_enable = on")

	tk.MustExec("create table t (a int primary key, b int unique, c int)")
	tk.MustExec("insert into t values (1,10,100), (2,20,200), (3,30,300)")
	tk.MustExec("create materialized view log on t (a, b, c)")

	// Single-row delete.
	tk.MustExec("delete from t where a=1")
	tk.MustQuery(
		"select a, b, c, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`",
	).Check(testkit.Rows(
		"1 10 100 D -1",
	))

	// Multi-row delete.
	execAsMViewMaintenance(tk, "delete from `$mlog$t`")
	tk.MustExec("delete from t where a in (2, 3)")
	tk.MustQuery(
		"select a, b, c, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`",
	).Sort().Check(testkit.Rows(
		"2 20 200 D -1",
		"3 30 300 D -1",
	))
}

func TestMLogUpdatePK(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_mview_enable = on")

	tk.MustExec("create table t (a int primary key, b int)")
	tk.MustExec("insert into t values (1, 100)")
	tk.MustExec("create materialized view log on t (a, b)")

	// Updating the primary key triggers the handle-changed path:
	// RemoveRecord(old) + AddRecord(new, IsUpdate).
	tk.MustExec("update t set a = 2 where a = 1")

	tk.MustQuery("select a, b from t order by a").Check(
		testkit.Rows("2 100"),
	)
	tk.MustQuery(
		"select a, b, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`",
	).Sort().Check(testkit.Rows(
		"1 100 U -1",
		"2 100 U 1",
	))
}

func TestMLogReplaceIdenticalRow(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_mview_enable = on")

	tk.MustExec("create table t (a int primary key, b int)")
	tk.MustExec("insert into t values (1, 100)")
	tk.MustExec("create materialized view log on t (a, b)")

	// REPLACE with an identical row: executor skips RemoveRecord + AddRecord.
	tk.MustExec("replace into t values (1, 100)")

	// Mlog should be empty because the base table was not mutated.
	tk.MustQuery("select * from `$mlog$t`").Check(testkit.Rows())
}

func TestMLogReplacePKConflict(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_mview_enable = on")

	tk.MustExec("create table t (a int primary key, b int)")
	tk.MustExec("insert into t values (1, 100)")
	tk.MustExec("create materialized view log on t (a, b)")

	// PK-only conflict: old row removed, new row added -> U -1, U 1.
	tk.MustExec("replace into t values (1, 200)")

	tk.MustQuery("select a, b from t order by a").Check(
		testkit.Rows("1 200"),
	)
	tk.MustQuery(
		"select a, b, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`",
	).Sort().Check(testkit.Rows(
		"1 100 U -1",
		"1 200 U 1",
	))
}

func TestMLogReplaceUKConflict(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_mview_enable = on")

	tk.MustExec("create table t (a int primary key, b int unique, c int)")
	tk.MustExec("insert into t values (1, 10, 100)")
	tk.MustExec("create materialized view log on t (a, b, c)")

	// UK-only conflict (no PK conflict): old row (1,10,100) removed, new row (99,10,200) added.
	tk.MustExec("replace into t values (99, 10, 200)")

	tk.MustQuery("select a, b, c from t order by a").Check(
		testkit.Rows("99 10 200"),
	)
	tk.MustQuery(
		"select a, b, c, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`",
	).Sort().Check(testkit.Rows(
		"1 10 100 U -1",
		"99 10 200 U 1",
	))
}

func TestMLogReplaceNoConflict(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_mview_enable = on")

	tk.MustExec("create table t (a int primary key, b int)")
	tk.MustExec("create materialized view log on t (a, b)")

	// Multi-row REPLACE with no conflicts -> all logged as I 1.
	tk.MustExec("replace into t values (1, 10), (2, 20)")

	tk.MustQuery("select a, b from t order by a").Check(
		testkit.Rows("1 10", "2 20"),
	)
	tk.MustQuery(
		"select a, b, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t` order by a",
	).Check(testkit.Rows(
		"1 10 I 1",
		"2 20 I 1",
	))
}

func TestMLogReplacePKAndUKConflict(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_mview_enable = on")

	tk.MustExec("create table t (a int primary key, b int unique, c int)")
	// Seed rows before creating mlog so that seed inserts won't be logged.
	tk.MustExec("insert into t values (1,10,100), (2,20,200)")
	tk.MustExec("create materialized view log on t (a, b, c)")

	// The first row conflicts with (1,10,100) on PK and with (2,20,200) on unique index.
	// The second row is new and should be inserted as is, even though its primary key value
	// conflicts with the old row that the first row removes.
	tk.MustExec("replace into t values (1,20,999), (2,30,100)")

	tk.MustQuery("select a, b, c from t order by a").Check(
		testkit.Rows(
			"1 20 999",
			"2 30 100",
		),
	)

	tk.MustQuery(
		"select a, b, c, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`",
	).Sort().Check(testkit.Rows(
		"1 10 100 U -1",
		"1 20 999 U 1",
		"2 20 200 U -1",
		"2 30 100 I 1",
	))
}

func TestMLogInsertIgnore(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_mview_enable = on")

	tk.MustExec("create table t (a int primary key, b int unique, c int)")
	tk.MustExec("insert into t values (1,10,100)")
	tk.MustExec("create materialized view log on t (a, b, c)")

	// (1,11,111) conflicts on PK, (2,10,222) conflicts on unique index, only the last is inserted.
	tk.MustExec("insert ignore into t values (1,11,111), (2,10,222), (3,30,333)")

	tk.MustQuery("select a, b, c from t order by a").Check(
		testkit.Rows("1 10 100", "3 30 333"),
	)
	tk.MustQuery(
		"select a, b, c, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`",
	).Check(testkit.Rows(
		"3 30 333 I 1",
	))
}

func TestMLogIODKU(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_mview_enable = on")

	tk.MustExec("create table t (a int primary key, b int unique, c int)")
	tk.MustExec("insert into t values (1,10,100)")
	tk.MustExec("create materialized view log on t (a, b, c)")

	tk.MustExec("insert into t values (1,10,101) on duplicate key update c=values(c)")

	tk.MustQuery("select a, b, c from t order by a").Check(
		testkit.Rows("1 10 101"),
	)
	tk.MustQuery(
		"select a, b, c, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`",
	).Sort().Check(testkit.Rows(
		"1 10 100 U -1",
		"1 10 101 U 1",
	))
}

func TestMLogIODKUChangePK(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_mview_enable = on")

	tk.MustExec("create table t (a int primary key, b int unique, c int)")
	tk.MustExec("insert into t values (1, 10, 100)")
	tk.MustExec("create materialized view log on t (a, b, c)")

	// IODKU that changes the primary key triggers the handle-changed path:
	// the old row is removed and the new row is added, both logged as U (update).
	tk.MustExec("insert into t values (1, 10, 200) on duplicate key update a = 3, c = values(c)")

	tk.MustQuery("select a, b, c from t order by a").Check(
		testkit.Rows("3 10 200"),
	)
	tk.MustQuery(
		"select a, b, c, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`",
	).Sort().Check(testkit.Rows(
		"1 10 100 U -1",
		"3 10 200 U 1",
	))
}

func TestMLogMultiRowIODKU(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_mview_enable = on")

	tk.MustExec("create table t (a int primary key, b int)")
	tk.MustExec("insert into t values (1,10), (2,20)")
	tk.MustExec("create materialized view log on t (a, b)")

	// Row (1,...) conflicts on PK -> update; row (3,...) is new -> insert.
	tk.MustExec("insert into t values (1,11), (3,30) on duplicate key update b=values(b)")

	tk.MustQuery("select a, b from t order by a").Check(
		testkit.Rows("1 11", "2 20", "3 30"),
	)
	tk.MustQuery(
		"select a, b, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`",
	).Sort().Check(testkit.Rows(
		"1 10 U -1",
		"1 11 U 1",
		"3 30 I 1",
	))
}

func TestMLogIODKUNoOp(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_mview_enable = on")

	tk.MustExec("create table t (a int primary key, b int)")
	tk.MustExec("insert into t values (1, 10)")
	tk.MustExec("create materialized view log on t (a, b)")

	// IODKU hits a duplicate key but the update is a no-op (b = b, no column touched).
	// Because no tracked column actually changes, mlog should be empty.
	tk.MustExec("insert into t values (1, 10) on duplicate key update b = b")

	tk.MustQuery("select a, b from t").Check(testkit.Rows("1 10"))
	tk.MustQuery("select * from `$mlog$t`").Check(testkit.Rows())
}

func TestMLogIODKUPKAndUKConflictDiffRows(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_mview_enable = on")

	tk.MustExec("create table t (a int primary key, b int unique, c int)")
	// Row A: a=1, b=10; Row B: a=2, b=20.
	tk.MustExec("insert into t values (1,10,100), (2,20,200)")
	tk.MustExec("create materialized view log on t (a, b, c)")

	// Insert (1,20,999): PK conflicts with row A (a=1), UK conflicts with row B (b=20).
	// Unlike REPLACE (which deletes conflicting rows first), IODKU finds the PK conflict
	// and tries to update that row, but the update itself violates the UK constraint on
	// another row.
	tk.MustGetErrCode(
		"insert into t values (1,20,999) on duplicate key update b=values(b), c=values(c)",
		mysql.ErrDupEntry,
	)

	// Base table unchanged.
	tk.MustQuery("select a, b, c from t order by a").Check(
		testkit.Rows("1 10 100", "2 20 200"),
	)
	// Mlog should be empty because the statement errored out.
	tk.MustQuery("select * from `$mlog$t`").Check(testkit.Rows())
}
