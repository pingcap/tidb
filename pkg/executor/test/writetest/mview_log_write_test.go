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

	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func execAsMViewMaintenance(tk *testkit.TestKit, sql string) {
	vars := tk.Session().GetSessionVars()
	originalMaintenance := vars.InMViewMaintenance
	originalRestrictedSQL := vars.InRestrictedSQL
	vars.InMViewMaintenance = true
	vars.InRestrictedSQL = true
	defer func() {
		vars.InMViewMaintenance = originalMaintenance
		vars.InRestrictedSQL = originalRestrictedSQL
	}()
	tk.MustExec(sql)
}

func mlogRows(tk *testkit.TestKit, tableName string) *testkit.Result {
	return tk.MustQuery("select * from `$mlog$" + tableName + "`")
}

func newMLogTestKit(t *testing.T) *testkit.TestKit {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set tidb_mview_enable = on")
	tk.MustExec("use test")
	return tk
}

func TestMLogInsert(t *testing.T) {
	tk := newMLogTestKit(t)

	tk.MustExec("create table t (a int primary key, b int, c int)")
	tk.MustExec("create materialized view log on t (a, b, c)")
	tk.MustExec("insert into t values (1, 10, 100), (2, 20, 200)")
	tk.MustQuery("select a, b, c, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t` order by a").
		Check(testkit.Rows("1 10 100 I 1", "2 20 200 I 1"))

	execAsMViewMaintenance(tk, "delete from `$mlog$t`")
	tk.MustExec("create table src (a int primary key, b int, c int)")
	tk.MustExec("insert into src values (3, 30, 300), (4, 40, 400)")
	tk.MustExec("insert into t select * from src")
	tk.MustQuery("select a, b, c, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t` order by a").
		Check(testkit.Rows("3 30 300 I 1", "4 40 400 I 1"))
}

func TestMLogInsertGeneratedColumn(t *testing.T) {
	tk := newMLogTestKit(t)

	tk.MustExec("create table t_gen (id bigint primary key, base int not null, gv int as (base + 1) virtual, gs int as (base + 2) stored)")
	tk.MustExec("create materialized view log on t_gen (id, gv, gs)")
	tk.MustExec("insert into t_gen(id, base) values (1, 10), (2, 20)")
	tk.MustQuery("select id, gv, gs, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t_gen` order by id").
		Check(testkit.Rows("1 11 12 I 1", "2 21 22 I 1"))
}

func TestMLogUpdateDelete(t *testing.T) {
	tk := newMLogTestKit(t)

	tk.MustExec("create table t (a int primary key, b int unique, c int)")
	tk.MustExec("insert into t values (1, 10, 100), (2, 20, 200), (3, 30, 300)")
	tk.MustExec("create materialized view log on t (a, b, c)")
	tk.MustExec("update t set c = c + 1 where a in (1, 2)")
	tk.MustQuery("select a, b, c, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`").Sort().
		Check(testkit.Rows("1 10 100 U -1", "1 10 101 U 1", "2 20 200 U -1", "2 20 201 U 1"))

	execAsMViewMaintenance(tk, "delete from `$mlog$t`")
	tk.MustExec("delete from t where a = 3")
	tk.MustQuery("select a, b, c, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`").
		Check(testkit.Rows("3 30 300 D -1"))

	execAsMViewMaintenance(tk, "delete from `$mlog$t`")
	tk.MustExec("update t set b = b where a = 1")

	// An update that does not change a tracked column must not create log rows.
	mlogRows(tk, "t").Check(testkit.Rows())
}

func TestMLogUpdateHandleAndReplace(t *testing.T) {
	tk := newMLogTestKit(t)

	tk.MustExec("create table t (id int primary key, uk int unique, v int)")
	tk.MustExec("insert into t values (1, 10, 100), (2, 20, 200)")
	tk.MustExec("create materialized view log on t (id, uk, v)")

	tk.MustExec("update t set id = 3 where id = 1")
	tk.MustQuery("select id, uk, v, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`").Sort().
		Check(testkit.Rows("1 10 100 U -1", "3 10 100 U 1"))

	execAsMViewMaintenance(tk, "delete from `$mlog$t`")
	tk.MustExec("replace into t values (3, 20, 999)")
	tk.MustQuery("select id, uk, v, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`").Sort().
		Check(testkit.Rows("2 20 200 U -1", "3 10 100 U -1", "3 20 999 U 1"))

	execAsMViewMaintenance(tk, "delete from `$mlog$t`")
	tk.MustExec("replace into t values (4, 40, 400), (5, 50, 500)")
	tk.MustQuery("select id, uk, v, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t` order by id").
		Check(testkit.Rows("4 40 400 I 1", "5 50 500 I 1"))
}

func TestMLogInsertOnDuplicateKeyUpdate(t *testing.T) {
	tk := newMLogTestKit(t)

	tk.MustExec("create table t (id int primary key, uk int unique, v int)")
	tk.MustExec("insert into t values (1, 10, 100), (2, 20, 200)")
	tk.MustExec("create materialized view log on t (id, uk, v)")

	tk.MustExec("insert into t values (1, 10, 101), (3, 30, 300) on duplicate key update v = values(v)")
	tk.MustQuery("select id, uk, v, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`").Sort().
		Check(testkit.Rows("1 10 100 U -1", "1 10 101 U 1", "3 30 300 I 1"))

	execAsMViewMaintenance(tk, "delete from `$mlog$t`")
	tk.MustExec("insert into t values (1, 10, 999) on duplicate key update id = 4, v = values(v)")
	tk.MustQuery("select id, uk, v, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`").Sort().
		Check(testkit.Rows("1 10 101 U -1", "4 10 999 U 1"))
}

func setLoadDataReader(tk *testkit.TestKit, data string) {
	readerBuilder := executor.LoadDataReaderBuilder{
		Build: func(_ string) (io.ReadCloser, error) {
			return mydump.NewStringReader(data), nil
		},
		Wg: &sync.WaitGroup{},
	}
	tk.Session().(sessionctx.Context).SetValue(executor.LoadDataReaderBuilderKey, readerBuilder)
}

func TestMLogLoadData(t *testing.T) {
	tk := newMLogTestKit(t)

	tk.MustExec("create table t (id int primary key, uk int unique, v int)")
	tk.MustExec("insert into t values (1, 10, 100)")
	tk.MustExec("create materialized view log on t (id, uk, v)")

	setLoadDataReader(tk, "1,11,111\n2,10,222\n3,30,333\n")
	tk.MustExec("load data local infile '/tmp/nonexistence.csv' ignore into table t fields terminated by ',' (id, uk, v)")
	tk.MustQuery("select id, uk, v from t order by id").Check(testkit.Rows("1 10 100", "3 30 333"))
	tk.MustQuery("select id, uk, v, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`").
		Check(testkit.Rows("3 30 333 I 1"))

	execAsMViewMaintenance(tk, "delete from `$mlog$t`")
	setLoadDataReader(tk, "1,20,999\n2,30,100\n")
	tk.MustExec("load data local infile '/tmp/nonexistence.csv' replace into table t fields terminated by ',' (id, uk, v)")
	tk.MustQuery("select id, uk, v, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`").Sort().
		Check(testkit.Rows("1 10 100 U -1", "1 20 999 U 1", "2 30 100 U 1", "3 30 333 U -1"))
}

func TestMLogMultiTableDMLAndColumnMapping(t *testing.T) {
	tk := newMLogTestKit(t)

	tk.MustExec("create table t1 (id int primary key, tracked int, extra int)")
	tk.MustExec("create table t2 (id int primary key, tracked int, extra int)")
	tk.MustExec("insert into t1 values (1, 10, 100)")
	tk.MustExec("insert into t2 values (1, 20, 200)")
	tk.MustExec("create materialized view log on t1 (tracked)")
	tk.MustExec("create materialized view log on t2 (extra, tracked)")

	tk.MustExec("update t1, t2 set t1.tracked = 11, t2.extra = 201 where t1.id = t2.id")
	tk.MustQuery("select tracked, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t1`").
		Check(testkit.Rows("10 U -1", "11 U 1"))
	tk.MustQuery("select extra, tracked, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t2`").
		Check(testkit.Rows("200 20 U -1", "201 20 U 1"))

	execAsMViewMaintenance(tk, "delete from `$mlog$t1`")
	execAsMViewMaintenance(tk, "delete from `$mlog$t2`")
	tk.MustExec("delete t1, t2 from t1, t2 where t1.id = t2.id")
	tk.MustQuery("select tracked, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t1`").
		Check(testkit.Rows("11 D -1"))
	tk.MustQuery("select extra, tracked, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t2`").
		Check(testkit.Rows("201 20 D -1"))
}

func TestMLogReservedRowIDAndRollback(t *testing.T) {
	tk := newMLogTestKit(t)

	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("create materialized view log on t (a, b)")
	tk.MustExec("insert into t values (1, 10), (2, 20)")
	tk.MustQuery("select _tidb_rowid, a from t order by _tidb_rowid").Check(testkit.Rows("1 1", "2 2"))
	tk.MustQuery("select _tidb_rowid, a from `$mlog$t` order by _tidb_rowid").Check(testkit.Rows("1 1", "2 2"))

	tk.MustExec("begin")
	tk.MustExec("insert into t values (3, 30)")
	tk.MustExec("rollback")
	tk.MustQuery("select * from t order by a").Check(testkit.Rows("1 10", "2 20"))
	tk.MustQuery("select * from `$mlog$t` order by a").Check(testkit.Rows("1 10 I 1", "2 20 I 1"))
}

func TestMLogImportIntoNotSupported(t *testing.T) {
	tk := newMLogTestKit(t)
	tk.MustExec("create table t (a int primary key, b int)")
	tk.MustExec("create materialized view log on t (a, b)")
	tk.MustGetErrCode("import into t from '/nonexistent.csv'", mysql.ErrNotSupportedYet)
}

func TestMLogUpdateWithUntrackedColumn(t *testing.T) {
	tk := newMLogTestKit(t)
	tk.MustExec("create table t (id int primary key, tracked int, untracked int)")
	tk.MustExec("create materialized view log on t (id, tracked)")
	tk.MustExec("insert into t values (1, 10, 100)")
	execAsMViewMaintenance(tk, "delete from `$mlog$t`")

	tk.MustExec("update t set untracked = 101 where id = 1")
	require.Empty(t, mlogRows(tk, "t").Rows())
	tk.MustExec("update t set tracked = 11 where id = 1")
	tk.MustQuery("select id, tracked, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`").Sort().
		Check(testkit.Rows("1 10 U -1", "1 11 U 1"))
}

func TestMLogInsertSelectAndIgnore(t *testing.T) {
	tk := newMLogTestKit(t)

	tk.MustExec("create table src (a int primary key, b int, c int)")
	tk.MustExec("create table t (a int primary key, b int, c int)")
	tk.MustExec("create materialized view log on t (a, b, c)")
	tk.MustExec("insert into src values (1,10,100), (2,20,200)")
	tk.MustExec("insert into t select * from src")
	tk.MustQuery("select a, b, c, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t` order by a").
		Check(testkit.Rows("1 10 100 I 1", "2 20 200 I 1"))

	execAsMViewMaintenance(tk, "delete from `$mlog$t`")
	tk.MustExec("delete from src")
	tk.MustExec("insert into src values (1,11,111), (3,30,300)")
	tk.MustExec("insert ignore into t select * from src")
	tk.MustQuery("select a, b, c from t order by a").
		Check(testkit.Rows("1 10 100", "2 20 200", "3 30 300"))
	tk.MustQuery("select a, b, c, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`").
		Check(testkit.Rows("3 30 300 I 1"))

	execAsMViewMaintenance(tk, "delete from `$mlog$t`")
	tk.MustExec("delete from src")
	tk.MustExec("insert into src values (1,11,111), (4,40,400)")
	tk.MustExec("insert into t select * from src on duplicate key update b=values(b), c=values(c)")
	tk.MustQuery("select a, b, c, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`").Sort().
		Check(testkit.Rows("1 10 100 U -1", "1 11 111 U 1", "4 40 400 I 1"))
}

func TestMLogReplaceConflictCombinations(t *testing.T) {
	tk := newMLogTestKit(t)

	tk.MustExec("create table t (a int primary key, b int unique, c int)")
	tk.MustExec("insert into t values (1,10,100), (2,20,200)")
	tk.MustExec("create materialized view log on t (a, b, c)")

	// One replacement can remove rows for both a primary-key and unique-key conflict.
	tk.MustExec("replace into t values (1,20,999), (2,30,100)")
	tk.MustQuery("select a, b, c from t order by a").Check(testkit.Rows("1 20 999", "2 30 100"))
	tk.MustQuery("select a, b, c, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`").Sort().
		Check(testkit.Rows(
			"1 10 100 U -1",
			"1 20 999 U 1",
			"2 20 200 U -1",
			"2 30 100 I 1",
		))

	execAsMViewMaintenance(tk, "delete from `$mlog$t`")
	tk.MustExec("replace into t values (1,20,999)")
	tk.MustQuery("select * from `$mlog$t`").Check(testkit.Rows())
}

func TestMLogReplaceSelectAndInsertIgnore(t *testing.T) {
	tk := newMLogTestKit(t)

	tk.MustExec("create table src (a int primary key, b int)")
	tk.MustExec("create table t (a int primary key, b int unique)")
	tk.MustExec("insert into t values (1, 10)")
	tk.MustExec("create materialized view log on t (a, b)")
	tk.MustExec("insert into src values (1, 11), (2, 20)")
	tk.MustExec("replace into t select * from src")
	tk.MustQuery("select a, b, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`").Sort().
		Check(testkit.Rows("1 10 U -1", "1 11 U 1", "2 20 I 1"))

	execAsMViewMaintenance(tk, "delete from `$mlog$t`")
	tk.MustExec("insert ignore into t values (1,30), (3,20), (4,40)")
	tk.MustQuery("select a, b, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`").
		Check(testkit.Rows("4 40 I 1"))
}

func TestMLogIODKUNoOpAndFailure(t *testing.T) {
	tk := newMLogTestKit(t)

	tk.MustExec("create table t (a int primary key, b int unique, c int)")
	tk.MustExec("insert into t values (1,10,100), (2,20,200)")
	tk.MustExec("create materialized view log on t (a, b, c)")

	tk.MustExec("insert into t values (1,10,100) on duplicate key update c=c")
	tk.MustQuery("select * from `$mlog$t`").Check(testkit.Rows())

	tk.MustGetErrCode(
		"insert into t values (1,20,999) on duplicate key update b=values(b), c=values(c)",
		mysql.ErrDupEntry,
	)
	tk.MustQuery("select a, b, c from t order by a").Check(testkit.Rows("1 10 100", "2 20 200"))
	tk.MustQuery("select * from `$mlog$t`").Check(testkit.Rows())
}

func TestMLogLoadDataReplaceFailureDoesNotLeakConflictState(t *testing.T) {
	tk := newMLogTestKit(t)
	tk.MustExec("set @@global.tidb_enable_check_constraint = 1")

	tk.MustExec("create table t (a int primary key, b int unique, c int, constraint chk_c check (c > 0))")
	tk.MustExec("insert into t values (1,10,1), (2,20,1)")
	tk.MustExec("create materialized view log on t (a, b, c)")
	setLoadDataReader(tk, "1,20,-1\n3,30,1\n")

	// The failed REPLACE row consumes the conflict marker. The following insert must remain I.
	tk.MustExec("load data local infile '/tmp/nonexistence.csv' replace into table t fields terminated by ',' (a, b, c)")
	tk.MustQuery("select a, b, c from t order by a").Check(testkit.Rows("3 30 1"))
	tk.MustQuery("select a, b, c, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`").Sort().
		Check(testkit.Rows("1 10 1 U -1", "2 20 1 U -1", "3 30 1 I 1"))
}

func TestMLogPartialColumnsMappingAndPrunedRows(t *testing.T) {
	tk := newMLogTestKit(t)

	tk.MustExec("create table t (a int, b int, c int, d int)")
	tk.MustExec("create materialized view log on t (d, b)")
	tk.MustExec("insert into t values (1,10,20,30)")
	tk.MustQuery("select d, b, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`").
		Check(testkit.Rows("30 10 I 1"))

	execAsMViewMaintenance(tk, "delete from `$mlog$t`")
	tk.MustExec("update t set c=21 where a=1")
	tk.MustQuery("select * from `$mlog$t`").Check(testkit.Rows())
	tk.MustExec("update t set b=11 where a=1")
	tk.MustQuery("select d, b, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`").Sort().
		Check(testkit.Rows("30 10 U -1", "30 11 U 1"))

	tk.MustExec("create table t_delete (a int, b int)")
	tk.MustExec("create materialized view log on t_delete (a, b)")
	tk.MustExec("insert into t_delete values (1,10)")
	execAsMViewMaintenance(tk, "delete from `$mlog$t_delete`")
	// DELETE normally prunes non-handle columns. MLog writing needs the full row layout.
	tk.MustExec("delete from t_delete where b=10")
	tk.MustQuery("select a, b, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t_delete`").
		Check(testkit.Rows("1 10 D -1"))
}

func TestMLogReferenceTypesAndGeneratedColumns(t *testing.T) {
	tk := newMLogTestKit(t)

	tk.MustExec("create table t_types (id int primary key, s varchar(20), txt text, d decimal(10,2), vb varbinary(20))")
	tk.MustExec("create materialized view log on t_types (s, txt, d, vb)")
	tk.MustExec("insert into t_types values (1, 'alpha', 'payload1', 12.34, 'bin1')")
	execAsMViewMaintenance(tk, "delete from `$mlog$t_types`")
	tk.MustExec("update t_types set s='beta', txt='payload2', d=56.78, vb='bin2' where id=1")
	tk.MustQuery("select s, txt, d, hex(vb), `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t_types`").Sort().
		Check(testkit.Rows("alpha payload1 12.34 62696E31 U -1", "beta payload2 56.78 62696E32 U 1"))

	for _, kind := range []string{"stored", "virtual"} {
		tk.MustExec("create table t_gen_" + kind + " (a int primary key, b int, c int, d int as (b+c) " + kind + ")")
		tk.MustExec("create materialized view log on t_gen_" + kind + " (a, d)")
		tk.MustExec("insert into t_gen_" + kind + " (a, b, c) values (1, 10, 20)")
		execAsMViewMaintenance(tk, "delete from `$mlog$t_gen_"+kind+"`")
		tk.MustExec("update t_gen_" + kind + " set b=11 where a=1")
		tk.MustQuery("select a, d, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t_gen_" + kind + "`").Sort().
			Check(testkit.Rows("1 30 U -1", "1 31 U 1"))
	}
}

func TestMLogAutoIncrement(t *testing.T) {
	tk := newMLogTestKit(t)

	tk.MustExec("create table t (a int auto_increment primary key, b int)")
	tk.MustExec("create materialized view log on t (a, b)")
	tk.MustExec("insert into t (b) values (10), (20)")
	tk.MustQuery("select a, b, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t` order by a").
		Check(testkit.Rows("1 10 I 1", "2 20 I 1"))

	execAsMViewMaintenance(tk, "delete from `$mlog$t`")
	tk.MustExec("replace into t values (2, 21)")
	tk.MustQuery("select a, b, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`").Sort().
		Check(testkit.Rows("2 20 U -1", "2 21 U 1"))
}
