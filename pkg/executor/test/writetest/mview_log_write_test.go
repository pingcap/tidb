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
	"io"
	"testing"

	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/testkit"
)

func TestMLogWriteReplacePKAndUKConflict(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int primary key, b int unique, c int)")
	// Seed rows before creating mlog so that seed inserts won't be logged.
	tk.MustExec("insert into t values (1,10,100), (2,20,200)")
	tk.MustExec("create materialized view log on t (a, b, c)")

	// This row conflicts with (1,10,100) on PK and with (2,20,200) on unique index.
	tk.MustExec("replace into t values (1,20,999)")

	tk.MustQuery("select a, b, c from t order by a").Check(
		testkit.Rows("1 20 999"),
	)

	tk.MustQuery(
		"select a, b, c, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`",
	).Sort().Check(testkit.Rows(
		"1 10 100 U -1",
		"1 20 999 U 1",
		"2 20 200 U -1",
	))
}

func TestMLogWriteInsertIgnore(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int primary key, b int unique, c int)")
	tk.MustExec("insert into t values (1,10,100)")
	tk.MustExec("create materialized view log on t (a, b, c)")

	// (1,11,111) conflicts on PK, (2,10,222) conflicts on unique index, only the last row is inserted.
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

func TestMLogWriteIODKU(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
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

func setLoadDataReader(tk *testkit.TestKit, data string) {
	var readerBuilder executor.LoadDataReaderBuilder = func(_ string) (io.ReadCloser, error) {
		return mydump.NewStringReader(data), nil
	}
	tk.Session().(sessionctx.Context).SetValue(executor.LoadDataReaderBuilderKey, readerBuilder)
}

func TestMLogWriteLoadDataIgnore(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int primary key, b int unique, c int)")
	// Seed rows before creating mlog so that seed inserts won't be logged.
	tk.MustExec("insert into t values (1,10,100)")
	tk.MustExec("create materialized view log on t (a, b, c)")

	setLoadDataReader(tk, "1,11,111\n2,10,222\n3,30,333\n")

	tk.MustExec("load data local infile '/tmp/nonexistence.csv' ignore into table t fields terminated by ',' (a, b, c)")

	tk.MustQuery("select a, b, c from t order by a").Check(
		testkit.Rows("1 10 100", "3 30 333"),
	)
	tk.MustQuery(
		"select a, b, c, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`",
	).Check(testkit.Rows(
		"3 30 333 I 1",
	))
}

func TestMLogWriteLoadDataReplacePKAndUKConflict(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int primary key, b int unique, c int)")
	tk.MustExec("insert into t values (1,10,100), (2,20,200)")
	tk.MustExec("create materialized view log on t (a, b, c)")

	setLoadDataReader(tk, "1,20,999\n")

	tk.MustExec("load data local infile '/tmp/nonexistence.csv' replace into table t fields terminated by ',' (a, b, c)")

	tk.MustQuery("select a, b, c from t order by a").Check(
		testkit.Rows("1 20 999"),
	)
	tk.MustQuery(
		"select a, b, c, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`",
	).Sort().Check(testkit.Rows(
		"1 10 100 U -1",
		"1 20 999 U 1",
		"2 20 200 U -1",
	))
}

func TestMLogWriteUpdateAndDelete(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int primary key, b int unique, c int)")
	tk.MustExec("insert into t values (1,10,100)")
	tk.MustExec("create materialized view log on t (a, b, c)")

	tk.MustExec("update t set c=101 where a=1")
	tk.MustQuery(
		"select a, b, c, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`",
	).Sort().Check(testkit.Rows(
		"1 10 100 U -1",
		"1 10 101 U 1",
	))

	tk.MustExec("delete from t where a=1")
	tk.MustQuery(
		"select a, b, c, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`",
	).Sort().Check(testkit.Rows(
		"1 10 100 U -1",
		"1 10 101 D -1",
		"1 10 101 U 1",
	))
}

func TestMLogWritePrunedColumns(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	t.Run("delete", func(t *testing.T) {
		tk.MustExec("drop table if exists `$mlog$t`")
		tk.MustExec("drop table if exists t")
		tk.MustExec("create table t (a int, b int)")
		tk.MustExec("create materialized view log on t (a, b)")
		tk.MustExec("insert into t values (1,10)")
		tk.MustExec("delete from `$mlog$t`")

		// Delete normally can prune non-handle/index columns, but mlog RemoveRecord reads
		// tracked columns by base offsets; pruning them would make mlog writing fail.
		tk.MustExec("delete from t where b=10")

		tk.MustQuery("select a, b from t").Check(testkit.Rows())
		tk.MustQuery(
			"select a, b, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`",
		).Check(testkit.Rows(
			"1 10 D -1",
		))
	})

	t.Run("update", func(t *testing.T) {
		tk.MustExec("drop table if exists `$mlog$t`")
		tk.MustExec("drop table if exists t")
		tk.MustExec("create table t (a int, b int)")
		tk.MustExec("create materialized view log on t (a, b)")
		tk.MustExec("insert into t values (1,10)")
		tk.MustExec("delete from `$mlog$t`")

		// Even if only column b is updated, UpdateRecord still needs full writable row data.
		// If update column pruning drops tracked columns, mlog writing would fail.
		tk.MustExec("update t set b=11 where b=10")

		tk.MustQuery("select a, b from t").Check(testkit.Rows("1 11"))
		tk.MustQuery(
			"select a, b, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`",
		).Sort().Check(testkit.Rows(
			"1 10 U -1",
			"1 11 U 1",
		))
	})
}

func TestMLogWriteSkipUntrackedColumns(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int primary key, b int, c int)")
	tk.MustExec("insert into t values (1,100,1000)")
	// mlog tracks (a, b); c is untracked.
	tk.MustExec("create materialized view log on t (a, b)")

	tk.MustExec("update t set c=2000 where a=1")
	tk.MustQuery("select * from `$mlog$t`").Check(testkit.Rows())

	tk.MustExec("update t set b=101 where a=1")
	tk.MustQuery(
		"select a, b, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`",
	).Sort().Check(testkit.Rows(
		"1 100 U -1",
		"1 101 U 1",
	))
}

func TestMLogWritePartialColumnsMapping(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int primary key, b int, c int, d int)")
	// Track columns in a different order to verify mapping by column name.
	tk.MustExec("create materialized view log on t (d, b)")

	tk.MustExec("insert into t values (1,10,20,30)")
	tk.MustQuery(
		"select d, b, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`",
	).Check(testkit.Rows(
		"30 10 I 1",
	))

	tk.MustExec("delete from `$mlog$t`")
	tk.MustExec("update t set c=21 where a=1")
	tk.MustQuery("select * from `$mlog$t`").Check(testkit.Rows())

	tk.MustExec("update t set b=11 where a=1")
	tk.MustQuery(
		"select d, b, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`",
	).Sort().Check(testkit.Rows(
		"30 10 U -1",
		"30 11 U 1",
	))

	tk.MustExec("delete from `$mlog$t`")
	tk.MustExec("update t set d=31 where a=1")
	tk.MustQuery(
		"select d, b, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`",
	).Sort().Check(testkit.Rows(
		"30 11 U -1",
		"31 11 U 1",
	))
}

func TestMLogPartitionedTableNotSupported(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec(
		"create table t (a int, b int) " +
			"partition by range (a) (" +
			"partition p0 values less than (10)," +
			"partition p1 values less than (maxvalue)" +
			")",
	)
	tk.MustExec("create materialized view log on t (a, b)")

	tk.MustGetErrCode("insert into t values (1,100)", mysql.ErrNotSupportedYet)
}

func TestMLogWriteUpdatePK(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
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

func TestMLogWriteIODKUChangePK(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int primary key, b int unique, c int)")
	tk.MustExec("insert into t values (1, 10, 100)")
	tk.MustExec("create materialized view log on t (a, b, c)")

	// IODKU that changes the primary key triggers handle-changed path:
	// RemoveRecord(old) + AddRecord(new, IsUpdate), all under MLogDMLTypeInsert.
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

func TestMLogWriteReplaceIdenticalRow(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int primary key, b int)")
	tk.MustExec("insert into t values (1, 100)")
	tk.MustExec("create materialized view log on t (a, b)")

	// REPLACE with an identical row: executor skips RemoveRecord + AddRecord.
	tk.MustExec("replace into t values (1, 100)")

	// Mlog should be empty because the base table was not mutated.
	tk.MustQuery("select * from `$mlog$t`").Check(testkit.Rows())
}

func TestMLogWriteMultiRowInsert(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int primary key, b int, c int)")
	tk.MustExec("create materialized view log on t (a, b, c)")

	tk.MustExec("insert into t values (1,10,100), (2,20,200), (3,30,300)")

	tk.MustQuery(
		"select a, b, c, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t` order by a",
	).Check(testkit.Rows(
		"1 10 100 I 1",
		"2 20 200 I 1",
		"3 30 300 I 1",
	))
}

func TestMLogWriteMultiRowIODKU(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int primary key, b int)")
	tk.MustExec("insert into t values (1,10), (2,20)")
	tk.MustExec("create materialized view log on t (a, b)")

	// Row (1,...) conflicts on PK → update; row (3,...) is new → insert.
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

func TestMLogWriteMultiTableUpdate(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1 (a int primary key, b int)")
	tk.MustExec("create table t2 (a int primary key, b int)")
	tk.MustExec("insert into t1 values (1,10)")
	tk.MustExec("insert into t2 values (1,100)")
	tk.MustExec("create materialized view log on t1 (a, b)")
	tk.MustExec("create materialized view log on t2 (a, b)")

	tk.MustExec("update t1, t2 set t1.b=11, t2.b=111 where t1.a=t2.a")

	tk.MustQuery(
		"select a, b, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t1`",
	).Sort().Check(testkit.Rows(
		"1 10 U -1",
		"1 11 U 1",
	))
	tk.MustQuery(
		"select a, b, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t2`",
	).Sort().Check(testkit.Rows(
		"1 100 U -1",
		"1 111 U 1",
	))
}

func TestMLogWriteMultiTableDelete(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1 (a int primary key, b int, c int)")
	tk.MustExec("create table t2 (a int primary key, b int, c int)")
	tk.MustExec("insert into t1 values (1,10,100), (2,20,200)")
	tk.MustExec("insert into t2 values (1,100,1000), (2,200,2000)")
	// Use different tracked columns for two tables to cover per-table mlog mapping in
	// multi-table DELETE, including non-handle tracked columns that used to be pruned.
	tk.MustExec("create materialized view log on t1 (b)")
	tk.MustExec("create materialized view log on t2 (c)")

	tk.MustExec("delete t1, t2 from t1, t2 where t1.a=t2.a and t1.a=1")

	tk.MustQuery("select a, b, c from t1 order by a").Check(testkit.Rows("2 20 200"))
	tk.MustQuery("select a, b, c from t2 order by a").Check(testkit.Rows("2 200 2000"))

	tk.MustQuery(
		"select b, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t1`",
	).Check(testkit.Rows(
		"10 D -1",
	))
	tk.MustQuery(
		"select c, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t2`",
	).Check(testkit.Rows(
		"1000 D -1",
	))
}

func TestMLogWriteIODKUPKAndUKConflictDiffRows(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int primary key, b int unique, c int)")
	// Row A: a=1, b=10; Row B: a=2, b=20.
	tk.MustExec("insert into t values (1,10,100), (2,20,200)")
	tk.MustExec("create materialized view log on t (a, b, c)")

	// Insert (1,20,999): PK conflicts with row A (a=1), UK conflicts with row B (b=20).
	// Unlike REPLACE (which deletes conflicting rows first), IODKU finds the PK conflict
	// and tries to update that row, but the update itself violates the UK constraint on
	// another row. TiDB correctly returns a duplicate key error.
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

func TestMLogWriteGeneratedColumn(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int primary key, b int, c int, d int as (b+c) stored)")
	// Track the stored generated column in the mlog.
	tk.MustExec("create materialized view log on t (a, b, d)")

	tk.MustExec("insert into t (a, b, c) values (1, 10, 20)")
	tk.MustQuery(
		"select a, b, d, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`",
	).Check(testkit.Rows(
		"1 10 30 I 1",
	))

	tk.MustExec("delete from `$mlog$t`")
	tk.MustExec("update t set b=11 where a=1")
	tk.MustQuery(
		"select a, b, d, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`",
	).Sort().Check(testkit.Rows(
		"1 10 30 U -1",
		"1 11 31 U 1",
	))
}

func TestMLogWriteAutoIncrement(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int auto_increment primary key, b int)")
	// Track the auto-increment PK in the mlog.
	tk.MustExec("create materialized view log on t (a, b)")

	tk.MustExec("insert into t (b) values (10), (20)")

	tk.MustQuery(
		"select a, b, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t` order by a",
	).Check(testkit.Rows(
		"1 10 I 1",
		"2 20 I 1",
	))

	tk.MustExec("delete from `$mlog$t`")
	tk.MustExec("delete from t where a=1")
	tk.MustQuery(
		"select a, b, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`",
	).Check(testkit.Rows(
		"1 10 D -1",
	))
}

func TestMLogWriteLoadDataReplaceConflictAddFailureNoLeak(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@global.tidb_enable_check_constraint = 1")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int primary key, b int unique, c int, constraint chk_c check (c > 0))")
	tk.MustExec("insert into t values (1,10,1), (2,20,1)")
	tk.MustExec("create materialized view log on t (a, b, c)")

	data := "1,20,-1\n3,30,1\n"
	var readerBuilder executor.LoadDataReaderBuilder = func(_ string) (r io.ReadCloser, err error) {
		return mydump.NewStringReader(data), nil
	}
	tk.Session().(sessionctx.Context).SetValue(executor.LoadDataReaderBuilderKey, readerBuilder)

	// First row removes old rows due to REPLACE conflicts but add fails with check constraint.
	// The second row is a plain insert and must still be marked as I (not leaked U).
	tk.MustExec("load data local infile '/tmp/nonexistence.csv' replace into table t fields terminated by ',' (a, b, c)")

	tk.MustQuery("select a, b, c from t order by a").Check(
		testkit.Rows("3 30 1"),
	)
	tk.MustQuery(
		"select a, b, c, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`",
	).Sort().Check(testkit.Rows(
		"1 10 1 U -1",
		"2 20 1 U -1",
		"3 30 1 I 1",
	))
}
