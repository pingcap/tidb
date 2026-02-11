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
		"1 10 100 R -1",
		"1 20 999 R 1",
		"2 20 200 R -1",
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

func TestMLogWriteLoadDataIgnore(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int primary key, b int unique, c int)")
	// Seed rows before creating mlog so that seed inserts won't be logged.
	tk.MustExec("insert into t values (1,10,100)")
	tk.MustExec("create materialized view log on t (a, b, c)")

	data := "1\t11\t111\n2\t10\t222\n3\t30\t333\n"
	var readerBuilder executor.LoadDataReaderBuilder = func(_ string) (r io.ReadCloser, err error) {
		return mydump.NewStringReader(data), nil
	}
	tk.Session().(sessionctx.Context).SetValue(executor.LoadDataReaderBuilderKey, readerBuilder)

	tk.MustExec("load data local infile '/tmp/nonexistence.csv' ignore into table t (a, b, c)")

	tk.MustQuery("select a, b, c from t order by a").Check(
		testkit.Rows("1 10 100", "3 30 333"),
	)
	tk.MustQuery(
		"select a, b, c, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`",
	).Check(testkit.Rows(
		"3 30 333 L 1",
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

	data := "1\t20\t999\n"
	var readerBuilder executor.LoadDataReaderBuilder = func(_ string) (r io.ReadCloser, err error) {
		return mydump.NewStringReader(data), nil
	}
	tk.Session().(sessionctx.Context).SetValue(executor.LoadDataReaderBuilderKey, readerBuilder)

	tk.MustExec("load data local infile '/tmp/nonexistence.csv' replace into table t (a, b, c)")

	tk.MustQuery("select a, b, c from t order by a").Check(
		testkit.Rows("1 20 999"),
	)
	tk.MustQuery(
		"select a, b, c, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`",
	).Sort().Check(testkit.Rows(
		"1 10 100 L -1",
		"1 20 999 L 1",
		"2 20 200 L -1",
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

		// TODO: DELETE may prune output columns to only keep handle/index data.
		// For a table without PK/index, RemoveRecord receives an empty row and mlog write fails.
		tk.MustContainErrMsg(
			"delete from t where b=10",
			"write mlog row: base row too short",
		)

		// The statement should be rolled back when mlog writing fails.
		tk.MustQuery("select a, b from t").Check(testkit.Rows("1 10"))
		tk.MustQuery("select * from `$mlog$t`").Check(testkit.Rows())
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

	// TODO: In multi-table DELETE, the row passed to RemoveRecord contains only the columns
	// projected by the planner (typically just handle + index columns). Tracked mlog columns
	// that fall outside this projection would cause an out-of-bounds error. To avoid this,
	// track only the PK column which is always available as the handle.
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1 (a int primary key, b int)")
	tk.MustExec("create table t2 (a int primary key, b int)")
	tk.MustExec("insert into t1 values (1,10), (2,20)")
	tk.MustExec("insert into t2 values (1,100), (2,200)")
	tk.MustExec("create materialized view log on t1 (a)")
	tk.MustExec("create materialized view log on t2 (a)")

	tk.MustExec("delete t1, t2 from t1, t2 where t1.a=t2.a and t1.a=1")

	tk.MustQuery("select a, b from t1 order by a").Check(testkit.Rows("2 20"))
	tk.MustQuery("select a, b from t2 order by a").Check(testkit.Rows("2 200"))

	tk.MustQuery(
		"select a, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t1`",
	).Check(testkit.Rows(
		"1 D -1",
	))
	tk.MustQuery(
		"select a, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t2`",
	).Check(testkit.Rows(
		"1 D -1",
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
