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

package ddl

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestMaterializedViewMetadataDDLNoCurrentDB(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustGetErrCode("create materialized view mv1 (k, cnt) as select k, count(*) from t group by k", errno.ErrNoDB)
	tk.MustGetErrCode("alter materialized view mv1 comment = 'c1'", errno.ErrNoDB)
	tk.MustGetErrCode("drop materialized view mv1", errno.ErrNoDB)

	tk.MustExec("create database mview_no_db")
	tk.MustExec("create table mview_no_db.t (k int, v int)")
	tk.MustExec("create materialized view log on mview_no_db.t (k, v)")
	tk.MustExec("create materialized view mview_no_db.mv1 (k, cnt, sum_v) as select k, count(*), sum(v) from mview_no_db.t group by k")
	tk.MustQuery(`select count(*) from information_schema.tidb_mviews where table_schema='mview_no_db' and mview_name='mv1'`).Check(testkit.Rows("1"))

	tk.MustExec("alter materialized view mview_no_db.mv1 comment = 'c1'")
	tk.MustQuery(`select mview_comment from information_schema.tidb_mviews where table_schema='mview_no_db' and mview_name='mv1'`).Check(testkit.Rows("c1"))

	tk.MustExec("drop materialized view mview_no_db.mv1")
	tk.MustQuery(`select count(*) from information_schema.tidb_mviews where table_schema='mview_no_db' and mview_name='mv1'`).Check(testkit.Rows("0"))
}

func TestMaterializedViewMetadataDDL(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("create database mview_meta")
	tk.MustExec("use mview_meta")
	tk.MustExec("create table t (k int, v int)")
	tk.MustExec("create materialized view log on t (k, v)")

	// default refresh clause: REFRESH FAST START WITH NOW() NEXT 300
	tk.MustExec("create materialized view mv1 (k, cnt, sum_v) as select k, count(*), sum(v) from t group by k")
	rows := tk.MustQuery(`
		select table_schema, mview_name, mview_id, mview_sql_content, mview_comment, refresh_method, start_with, next
		from information_schema.tidb_mviews
		where table_schema='mview_meta' and mview_name='mv1'`).Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "mview_meta", rows[0][0])
	require.Equal(t, "mv1", rows[0][1])
	mviewID, err := strconv.ParseInt(fmt.Sprint(rows[0][2]), 10, 64)
	require.NoError(t, err)
	require.Greater(t, mviewID, int64(0))
	require.Contains(t, strings.ToUpper(fmt.Sprint(rows[0][3])), "SELECT")
	require.Equal(t, "<nil>", fmt.Sprint(rows[0][4]))
	require.Equal(t, "REFRESH FAST", rows[0][5])
	require.Equal(t, "NOW()", fmt.Sprint(rows[0][6]))
	require.Equal(t, "300", fmt.Sprint(rows[0][7]))

	tk.MustQuery(fmt.Sprintf("select refresh_method, start_with, `next` from mysql.tidb_mview_refresh where mview_id=%d", mviewID)).
		Check(testkit.Rows("REFRESH FAST NOW() 300"))

	tk.MustQuery("select count(*) from information_schema.statistics where table_schema='mview_meta' and table_name='mv1' and non_unique=0 and column_name='k'").
		Check(testkit.Rows("1"))

	// comment is visible in information_schema.tidb_mviews
	tk.MustExec("create materialized view mv2 (k, cnt, sum_v) comment = 'c1' as select k, count(*), sum(v) from t group by k")
	rows = tk.MustQuery(`
		select mview_comment
		from information_schema.tidb_mviews
		where table_schema='mview_meta' and mview_name='mv2'`).Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "c1", rows[0][0])

	// explicit refresh schedule
	tk.MustExec("create materialized view mv3 (k, cnt, sum_v) refresh fast start with now() next 600 as select k, count(*), sum(v) from t group by k")
	tk.MustQuery("select start_with, `next` from information_schema.tidb_mviews where table_schema='mview_meta' and mview_name='mv3'").
		Check(testkit.Rows("NOW() 600"))

	// duplicate name in same schema
	tk.MustGetErrCode("create materialized view mv1 (k, cnt, sum_v) as select k, count(*), sum(v) from t group by k", errno.ErrTableExists)

	// MV shares the same namespace with normal tables/views.
	tk.MustExec("create table mv_conflict (a int)")
	tk.MustGetErrCode("create materialized view mv_conflict (k, cnt, sum_v) as select k, count(*), sum(v) from t group by k", errno.ErrTableExists)

	// Column list length must match SELECT output column count.
	tk.MustGetErrCode("create materialized view mv_wrong_cols (k, cnt) as select k, count(*), sum(v) from t group by k", errno.ErrViewWrongList)

	// ALTER comment / refresh schedule
	tk.MustExec("alter materialized view mv1 comment = 'c2'")
	rows = tk.MustQuery(`
		select mview_comment
		from information_schema.tidb_mviews
		where table_schema='mview_meta' and mview_name='mv1'`).Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "c2", rows[0][0])

	tk.MustExec("alter materialized view mv1 refresh start with now() next 900")
	tk.MustQuery(fmt.Sprintf("select start_with is null, `next` from mysql.tidb_mview_refresh where mview_id=%d", mviewID)).
		Check(testkit.Rows("0 900"))

	tk.MustExec("alter materialized view mv1 refresh")
	tk.MustQuery(fmt.Sprintf("select start_with, `next` from mysql.tidb_mview_refresh where mview_id=%d", mviewID)).
		Check(testkit.Rows("<nil> <nil>"))

	// DROP removes metadata row.
	tk.MustExec("drop materialized view mv2")
	tk.MustQuery(`
		select count(*) from information_schema.tidb_mviews
		where table_schema='mview_meta' and mview_name='mv2'`).Check(testkit.Rows("0"))
	tk.MustQuery("select count(*) from mysql.tidb_mview_refresh where mview_id=" + fmt.Sprint(mviewID)).Check(testkit.Rows("1"))
}

func TestMaterializedViewMetadataDDLErrorCases(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("create database mview_err")
	tk.MustExec("use mview_err")

	tk.MustGetErrCode("alter materialized view mv_not_exist comment = 'c1'", errno.ErrNoSuchTable)
	tk.MustGetErrCode("drop materialized view mv_not_exist", errno.ErrNoSuchTable)
}

func TestMaterializedViewMetadataDDLDatabaseNotExists(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("create database mview_db_not_exists")
	tk.MustExec("use mview_db_not_exists")
	tk.MustExec("create table t (k int)")

	tk.MustGetErrCode("create materialized view not_exist.mv1 (k, cnt) as select k, count(*) from t group by k", errno.ErrBadDB)
	tk.MustGetErrCode("alter materialized view not_exist.mv1 comment = 'c1'", errno.ErrBadDB)
	tk.MustGetErrCode("drop materialized view not_exist.mv1", errno.ErrBadDB)
}

func TestMaterializedViewLogMetadataDDLNoCurrentDB(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("create database mlog_no_db")
	tk.MustExec("use mlog_no_db")
	tk.MustExec("create table t (a int)")

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustGetErrCode("create materialized view log on t (a)", errno.ErrNoDB)
	tk2.MustGetErrCode("alter materialized view log on t purge immediate", errno.ErrNoDB)
	tk2.MustGetErrCode("drop materialized view log on t", errno.ErrNoDB)

	tk2.MustExec("create materialized view log on mlog_no_db.t (a)")
	tk2.MustQuery(`select count(*) from information_schema.tidb_mlogs where base_table_schema='mlog_no_db' and base_table_name='t'`).Check(testkit.Rows("1"))

	tk2.MustExec("alter materialized view log on mlog_no_db.t purge immediate")
	tk2.MustExec("drop materialized view log on mlog_no_db.t")
	tk2.MustQuery(`select count(*) from information_schema.tidb_mlogs where base_table_schema='mlog_no_db' and base_table_name='t'`).Check(testkit.Rows("0"))
}

func TestMaterializedViewLogMetadataDDL(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("create database mlog_meta")
	tk.MustExec("use mlog_meta")
	tk.MustExec("create table t (a int, b int)")

	// default purge policy: IMMEDIATE (interval 0).
	tk.MustExec("create materialized view log on t (a,b)")
	rows := tk.MustQuery(`
		select mlog_id, mlog_name, mlog_columns, purge_method, purge_interval
		from information_schema.tidb_mlogs
		where base_table_schema='mlog_meta' and base_table_name='t'`).Rows()
	require.Len(t, rows, 1)
	mlogID, err := strconv.ParseInt(fmt.Sprint(rows[0][0]), 10, 64)
	require.NoError(t, err)
	require.Greater(t, mlogID, int64(0))
	require.Equal(t, "$mlog$t", rows[0][1])
	require.Equal(t, "a,b", fmt.Sprint(rows[0][2]))
	require.Equal(t, "IMMEDIATE", rows[0][3])
	require.Equal(t, "0", fmt.Sprint(rows[0][4]))

	// duplicate mlog on same base table
	tk.MustGetErrMsg(
		"create materialized view log on t (a)",
		"[schema:1050]Table 'materialized view log on mlog_meta.t' already exists",
	)

	// MV LOG name shares the same namespace with normal tables/views.
	tk.MustExec("create table `$mlog$t_conflict` (a int)")
	tk.MustExec("create table t_conflict (a int)")
	tk.MustGetErrCode("create materialized view log on t_conflict (a)", errno.ErrTableExists)

	// columns must exist on base table
	tk.MustExec("create table t2 (a int)") // ensure table exists for the next statement
	tk.MustGetErrMsg(
		"create materialized view log on t2 (b)",
		"[schema:1054]Unknown column 'b' in 't2'",
	)

	// ALTER purge to deferred with deterministic start/interval.
	tk.MustExec("alter materialized view log on t purge start with '2026-01-01 00:00:00' next 600")
	tk.MustQuery(fmt.Sprintf(`
		select purge_method, purge_start, purge_interval
		from mysql.tidb_mlog_purge
		where mlog_id=%d`, mlogID)).Check(testkit.Rows("DEFERRED 2026-01-01 00:00:00 600"))

	// DROP removes metadata row.
	tk.MustExec("drop materialized view log on t")
	tk.MustQuery(`
		select count(*) from information_schema.tidb_mlogs
		where base_table_schema='mlog_meta' and base_table_name='t'`).Check(testkit.Rows("0"))
}

func TestMaterializedViewLogMetadataDDLErrorCases(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("create database mlog_err")
	tk.MustExec("use mlog_err")

	// base table missing
	tk.MustGetErrCode("create materialized view log on t_not_exist (a)", errno.ErrNoSuchTable)

	// mlog missing on existing base table
	tk.MustExec("create table t (a int)")
	tk.MustGetErrCode("alter materialized view log on t purge immediate", errno.ErrNoSuchTable)
	tk.MustGetErrCode("drop materialized view log on t", errno.ErrNoSuchTable)

	// column case-insensitive match
	tk.MustExec("create table t2 (A int, b int)")
	tk.MustExec("create materialized view log on t2 (a, b)")
	tk.MustExec("drop materialized view log on t2")
}

func TestRefreshMaterializedViewUnsupported(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("create database mview_refresh_unsupported")
	tk.MustExec("use mview_refresh_unsupported")

	err := tk.ExecToErr("refresh materialized view mv1")
	require.Error(t, err)
	require.ErrorContains(t, err, "REFRESH MATERIALIZED VIEW is not supported")
}

func TestDropMaterializedViewLogBlockedByDependentMVs(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("create database mlog_drop_guard_mv")
	tk.MustExec("use mlog_drop_guard_mv")
	tk.MustExec("create table t (k int, v int)")
	tk.MustExec("create materialized view log on t (k, v)")
	tk.MustExec("create materialized view mv (k, cnt, sum_v) as select k, count(*), sum(v) from t group by k")

	err := tk.ExecToErr("drop materialized view log on t")
	require.Error(t, err)
	require.ErrorContains(t, err, "dependent materialized views exist")

	tk.MustExec("drop materialized view mv")
	tk.MustExec("drop materialized view log on t")
}

func TestDropTableBlockedByMaterializedViewLog(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("create database mlog_drop_guard")
	tk.MustExec("use mlog_drop_guard")
	tk.MustExec("create table t (a int)")
	tk.MustExec("create materialized view log on t (a)")

	tk.MustGetErrMsg(
		"drop table t",
		"can't drop table mlog_drop_guard.t: materialized view log exists, drop it first",
	)

	tk.MustExec("drop materialized view log on t")
	tk.MustExec("drop table t")
}

func TestDropTableBlockedByMaterializedViewLogMultiTable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("create database mlog_drop_guard_multi")
	tk.MustExec("use mlog_drop_guard_multi")
	tk.MustExec("create table t1 (a int)")
	tk.MustExec("create table t2 (a int)")
	tk.MustExec("create materialized view log on t1 (a)")

	tk.MustGetErrMsg(
		"drop table t1, t2",
		"can't drop table mlog_drop_guard_multi.t1: materialized view log exists, drop it first",
	)
	tk.MustQuery("select count(*) from information_schema.tables where table_schema='mlog_drop_guard_multi' and table_name='t1'").Check(testkit.Rows("1"))
	tk.MustQuery("select count(*) from information_schema.tables where table_schema='mlog_drop_guard_multi' and table_name='t2'").Check(testkit.Rows("1"))
}

func TestDropTableIfExistsBlockedByMaterializedViewLog(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("create database mlog_drop_guard_if_exists")
	tk.MustExec("use mlog_drop_guard_if_exists")
	tk.MustExec("create table t (a int)")
	tk.MustExec("create materialized view log on t (a)")

	tk.MustGetErrMsg(
		"drop table if exists not_exist, t",
		"can't drop table mlog_drop_guard_if_exists.t: materialized view log exists, drop it first",
	)

	tk.MustExec("drop materialized view log on t")
	tk.MustExec("drop table if exists not_exist, t")
}

func TestDropDatabaseBlockedByMaterializedViewLog(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("create database mlog_drop_db_guard")
	tk.MustExec("use mlog_drop_db_guard")
	tk.MustExec("create table t (a int)")
	tk.MustExec("create materialized view log on t (a)")

	tk.MustGetErrMsg(
		"drop database mlog_drop_db_guard",
		"can't drop database mlog_drop_db_guard: materialized view log exists, drop it first",
	)

	tk.MustExec("drop materialized view log on t")
	tk.MustExec("drop database mlog_drop_db_guard")
}
