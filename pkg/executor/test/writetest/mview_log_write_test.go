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
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

const (
	addColumnStateWriteReorgFailpoint = "github.com/pingcap/tidb/pkg/ddl/onAddColumnStateWriteReorg"
	dropColumnStateWriteOnlyFailpoint = "github.com/pingcap/tidb/pkg/ddl/onDropColumnStateWriteOnly"
)

// ddlCtrl controls DDL statements paused at a failpoint for testing mlog behavior during online DDL.
type ddlCtrl struct {
	paused chan struct{}
	resume chan struct{}

	ddlWg  sync.WaitGroup
	ddlErr error

	pausedOnce  sync.Once
	releaseOnce sync.Once
}

// startDDLPausedAtFailpoint installs a failpoint callback and starts DDL in background.
func startDDLPausedAtFailpoint(
	t *testing.T,
	tkDDL *testkit.TestKit,
	failpointName string,
	ddlSQL string,
) *ddlCtrl {
	ctrl := &ddlCtrl{
		paused: make(chan struct{}),
		resume: make(chan struct{}),
	}

	testfailpoint.EnableCall(t, failpointName, func() {
		ctrl.pausedOnce.Do(func() {
			close(ctrl.paused)
		})
		<-ctrl.resume
	})

	ctrl.ddlWg.Add(1)
	go func() {
		defer ctrl.ddlWg.Done()
		ctrl.ddlErr = tkDDL.ExecToErr(ddlSQL)
	}()
	return ctrl
}

// waitUntilPaused waits until the failpoint callback is hit.
func (c *ddlCtrl) waitUntilPaused(t *testing.T, desc string) {
	select {
	case <-c.paused:
	case <-time.After(10 * time.Second):
		require.FailNow(t, "timed out waiting ddl failpoint", "desc=%s", desc)
	}
}

// releaseAndWaitFinish resumes the paused DDL and waits for completion.
// It is idempotent and safe to call multiple times.
func (c *ddlCtrl) releaseAndWaitFinish(t *testing.T) {
	c.releaseOnce.Do(func() {
		close(c.resume)
		c.ddlWg.Wait()
		require.NoError(t, c.ddlErr)
	})
}

func TestMLogInsert(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

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
	tk.MustExec("delete from `$mlog$t`")
	tk.MustExec("insert into t values (2,20,200), (3,30,300), (4,40,400)")
	tk.MustQuery(
		"select a, b, c, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t` order by a",
	).Check(testkit.Rows(
		"2 20 200 I 1",
		"3 30 300 I 1",
		"4 40 400 I 1",
	))
}

func TestMLogUpdate(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

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
	tk.MustExec("delete from `$mlog$t`")
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
	tk.MustExec("delete from `$mlog$t`")
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

	tk.MustExec("create table t (a int primary key, b int)")
	tk.MustExec("insert into t values (1, 100)")
	tk.MustExec("create materialized view log on t (a, b)")

	// REPLACE with an identical row: executor skips RemoveRecord + AddRecord.
	tk.MustExec("replace into t values (1, 100)")

	// Mlog should be empty because the base table was not mutated.
	tk.MustQuery("select * from `$mlog$t`").Check(testkit.Rows())
}

func TestMLogReplacePKAndUKConflict(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

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

func TestMLogInsertIgnore(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

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

func TestMLogIODKUPKAndUKConflictDiffRows(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

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

func setLoadDataReader(tk *testkit.TestKit, data string) {
	var readerBuilder executor.LoadDataReaderBuilder = func(_ string) (io.ReadCloser, error) {
		return mydump.NewStringReader(data), nil
	}
	tk.Session().(sessionctx.Context).SetValue(executor.LoadDataReaderBuilderKey, readerBuilder)
}

func TestMLogLoadDataIgnore(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

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

func TestMLogLoadDataReplacePKAndUKConflict(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

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

func TestMLogLoadDataReplaceConflictAddFailureNoLeak(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@global.tidb_enable_check_constraint = 1")

	tk.MustExec("create table t (a int primary key, b int unique, c int, constraint chk_c check (c > 0))")
	tk.MustExec("insert into t values (1,10,1), (2,20,1)")
	tk.MustExec("create materialized view log on t (a, b, c)")

	setLoadDataReader(tk, "1,20,-1\n3,30,1\n")

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

func TestMLogMultiTableUpdate(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

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

func TestMLogMultiTableDelete(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

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

func TestMLogSkipUntrackedColumns(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t (a int primary key, b int, c int)")
	tk.MustExec("insert into t values (1,100,1000)")
	// mlog tracks (a, b); c is untracked.
	tk.MustExec("create materialized view log on t (a, b)")

	// Updating an untracked column should not produce any mlog entry.
	tk.MustExec("update t set c=2000 where a=1")
	tk.MustQuery("select * from `$mlog$t`").Check(testkit.Rows())

	// Updating a tracked column should produce mlog entries.
	tk.MustExec("update t set b=101 where a=1")
	tk.MustQuery(
		"select a, b, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`",
	).Sort().Check(testkit.Rows(
		"1 100 U -1",
		"1 101 U 1",
	))
}

func TestMLogPartialColumnsMapping(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

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

func TestMLogPrunedColumns(t *testing.T) {
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

func TestMLogOnlineDDLAddUntrackedColumn(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@global.tidb_enable_metadata_lock=0")

	tk.MustExec("create table t (id int primary key, tracked int, untracked int)")
	tk.MustExec("create materialized view log on t (id, tracked)")
	tk.MustExec("insert into t values (1, 10, 100)")
	tk.MustExec("delete from `$mlog$t`")

	tkDDL := testkit.NewTestKit(t, store)
	tkDDL.MustExec("use test")
	ctrl := startDDLPausedAtFailpoint(
		t,
		tkDDL,
		addColumnStateWriteReorgFailpoint,
		"alter table t add column c_new int default 0",
	)
	defer ctrl.releaseAndWaitFinish(t)

	ctrl.waitUntilPaused(t, "add-column write-reorg")

	// Update an untracked column during online DDL: mlog should stay empty.
	tk.MustExec("update t set untracked = 101 where id = 1")
	tk.MustQuery("select * from `$mlog$t`").Check(testkit.Rows())

	// Insert during online DDL: mlog should still capture tracked columns.
	tk.MustExec("insert into t (id, tracked, untracked) values (2, 20, 200)")
	tk.MustQuery(
		"select id, tracked, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`",
	).Check(testkit.Rows(
		"2 20 I 1",
	))

	ctrl.releaseAndWaitFinish(t)

	tk.MustQuery("select id, tracked, untracked, c_new from t order by id").Check(testkit.Rows(
		"1 10 101 0",
		"2 20 200 0",
	))
}

func TestMLogOnlineDDLDropUntrackedColumn(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@global.tidb_enable_metadata_lock=0")

	tk.MustExec("create table t (id int primary key, tracked int, to_drop int)")
	tk.MustExec("create materialized view log on t (id, tracked)")
	tk.MustExec("insert into t values (1, 10, 100)")
	tk.MustExec("delete from `$mlog$t`")

	tkDDL := testkit.NewTestKit(t, store)
	tkDDL.MustExec("use test")
	ctrl := startDDLPausedAtFailpoint(
		t,
		tkDDL,
		dropColumnStateWriteOnlyFailpoint,
		"alter table t drop column to_drop",
	)
	defer ctrl.releaseAndWaitFinish(t)

	ctrl.waitUntilPaused(t, "drop-untracked-column write-only")

	// Update a tracked column during online DDL should still emit update logs.
	tk.MustExec("update t set tracked = 11 where id = 1")
	// Insert during online DDL should still emit insert logs.
	tk.MustExec("insert into t (id, tracked) values (2, 20)")

	tk.MustQuery(
		"select id, tracked, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`",
	).Sort().Check(testkit.Rows(
		"1 10 U -1",
		"1 11 U 1",
		"2 20 I 1",
	))

	ctrl.releaseAndWaitFinish(t)

	tk.MustQuery("select id, tracked from t order by id").Check(testkit.Rows(
		"1 11",
		"2 20",
	))
}

// TODO: DDL should reject dropping a tracked column
func TestMLogOnlineDDLDropTrackedColumnCurrentBehavior(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@global.tidb_enable_metadata_lock=0")

	tk.MustExec("create table t (id int primary key, tracked int, untracked int)")
	tk.MustExec("create materialized view log on t (id, tracked)")
	tk.MustExec("insert into t values (1, 10, 100)")
	tk.MustExec("delete from `$mlog$t`")

	tkDDL := testkit.NewTestKit(t, store)
	tkDDL.MustExec("use test")
	ctrl := startDDLPausedAtFailpoint(
		t,
		tkDDL,
		dropColumnStateWriteOnlyFailpoint,
		"alter table t drop column tracked",
	)
	defer ctrl.releaseAndWaitFinish(t)

	ctrl.waitUntilPaused(t, "drop-tracked-column write-only")

	// While online DDL is paused in an intermediate state, tracked column offsets in mlog metadata
	// can no longer match the partial insert row layout, so mlog writing fails in the DML path.
	err := tk.ExecToErr("insert into t (id, untracked) values (2, 200)")
	require.ErrorContains(t, err, "write mlog row: column at offset")

	ctrl.releaseAndWaitFinish(t)

	// Current behavior after DDL completion: wrapped DML fails because tracked column metadata
	// still exists in mlog definition but is removed from the base table schema.
	err = tk.ExecToErr("insert into t (id, untracked) values (3, 300)")
	require.ErrorContains(t, err, "wrap table with mlog: base column tracked not found")
}

// TODO: DDL should reject dropping a tracked column
func TestMLogDropTrackedColumnCurrentBehavior(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t (id int primary key, tracked int, untracked int)")
	tk.MustExec("create materialized view log on t (id, tracked)")
	tk.MustExec("alter table t drop column tracked")

	// Current behavior: DDL succeeds, then wrapped DML fails at execution build time because
	// mlog metadata still references a tracked column that no longer exists on base table.
	err := tk.ExecToErr("insert into t values (1, 100)")
	require.ErrorContains(t, err, "wrap table with mlog: base column tracked not found")

	err = tk.ExecToErr("update t set untracked = 101 where id = 1")
	require.ErrorContains(t, err, "wrap table with mlog: base column tracked not found")

	err = tk.ExecToErr("delete from t where id = 1")
	require.ErrorContains(t, err, "wrap table with mlog: base column tracked not found")
}

func TestMLogGeneratedColumn(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

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

func TestMLogAutoIncrement(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

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

func TestMLogPartitionedTableNotSupported(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

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

func TestMLogImportIntoNotSupported(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t (a int primary key, b int)")
	tk.MustExec("create materialized view log on t (a, b)")

	tk.MustGetErrCode(
		"import into t from '/nonexistent.csv'",
		mysql.ErrNotSupportedYet,
	)
}
