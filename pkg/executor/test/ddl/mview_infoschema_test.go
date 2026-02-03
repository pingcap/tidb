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
	"testing"

	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestInfoSchemaTiDBMViewsAndMLogs(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("set time_zone = '+00:00'")

	tk.MustExec("create database is_mview")
	tk.MustExec("use is_mview")
	tk.MustExec("create table t (k int, v int)")
	tk.MustExec("create materialized view log on t (k, v)")
	tk.MustExec("create materialized view mv1 (k, cnt, sum_v) as select k, count(*), sum(v) from t group by k")

	rows := tk.MustQuery(`
		select table_schema, mview_name, mview_id, mview_sql_content, mview_comment, refresh_method, start_with, ` + "`next`" + `
		from information_schema.tidb_mviews
		where table_schema='is_mview' and mview_name='mv1'`,
	).Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "is_mview", rows[0][0])
	require.Equal(t, "mv1", rows[0][1])
	mviewID := fmt.Sprint(rows[0][2])
	require.NotEmpty(t, mviewID)
	require.NotEmpty(t, fmt.Sprint(rows[0][3]))
	require.Equal(t, "<nil>", fmt.Sprint(rows[0][4]))
	require.Equal(t, "REFRESH FAST", rows[0][5])
	require.NotEmpty(t, fmt.Sprint(rows[0][6]))
	require.NotEmpty(t, fmt.Sprint(rows[0][7]))

	tk.MustQuery(`
		select count(*) from mysql.tidb_mview_refresh
		where mview_id=` + fmt.Sprint(mviewID),
	).Check(testkit.Rows("1"))

	tk.MustExec("create database is_mlog")
	tk.MustExec("use is_mlog")
	tk.MustExec("create table t (a int)")
	tk.MustExec("create materialized view log on t (a)")
	rows = tk.MustQuery("select mlog_id from information_schema.tidb_mlogs where base_table_schema='is_mlog' and base_table_name='t'").Rows()
	require.Len(t, rows, 1)
	mlogID := fmt.Sprint(rows[0][0])
	tk.MustExec(fmt.Sprintf("update mysql.tidb_mlog_purge set purge_start='2026-01-02 03:04:05', last_purge_time='2026-01-02 03:04:05', last_purge_rows=7, last_purge_duration=9 where mlog_id=%s", mlogID))

	rows = tk.MustQuery(`
		select base_table_schema, base_table_name, purge_method, purge_start, purge_interval, last_purge_time, last_purge_rows, last_purge_duration
		from information_schema.tidb_mlogs
		where base_table_schema='is_mlog' and base_table_name='t'`,
	).Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "is_mlog", rows[0][0])
	require.Equal(t, "t", rows[0][1])
	require.Equal(t, "IMMEDIATE", rows[0][2])
	require.Equal(t, "2026-01-02 03:04:05", rows[0][3])
	require.Equal(t, "0", rows[0][4])
	require.Equal(t, "2026-01-02 03:04:05", rows[0][5])
	require.Equal(t, "7", rows[0][6])
	require.Equal(t, "9", rows[0][7])
}

func TestInfoSchemaTiDBMViewRefreshHistAndMLogPurgeHist(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("set time_zone = '+00:00'")

	tk.MustExec("create database is_mvhist")
	tk.MustExec("use is_mvhist")
	tk.MustExec("create table t (k int, v int)")
	tk.MustExec("create materialized view log on t (k, v)")
	tk.MustExec("create materialized view mv (k, cnt, sum_v) as select k, count(*), sum(v) from t group by k")
	rows := tk.MustQuery("select mview_id, mview_name from information_schema.tidb_mviews where table_schema='is_mvhist' and mview_name='mv'").Rows()
	require.Len(t, rows, 1)
	mviewID := fmt.Sprint(rows[0][0])
	mviewName := rows[0][1].(string)

	rows = tk.MustQuery("select mlog_id, mlog_name from information_schema.tidb_mlogs where base_table_schema='is_mvhist' and base_table_name='t'").Rows()
	require.Len(t, rows, 1)
	mlogID := fmt.Sprint(rows[0][0])
	mlogName := rows[0][1].(string)

	tk.MustExec(fmt.Sprintf(`
		insert into mysql.tidb_mview_refresh_hist
			(mview_id, mview_name, refresh_job_id, is_newest_refresh, refresh_method, refresh_time, refresh_endtime, refresh_status)
		values
			(%s, '%s', 123, 'Y', 'REFRESH FAST', '2026-01-01 00:00:00', NULL, NULL)`,
		mviewID, mviewName))

	tk.MustExec(fmt.Sprintf(`
		insert into mysql.tidb_mlog_purge_hist
			(mlog_id, mlog_name, purge_job_id, is_newest_purge, purge_method, purge_time, purge_endtime, purge_rows, purge_status)
		values
			(%s, '%s', 456, 'Y', 'IMMEDIATE', NULL, '2026-01-01 00:00:00', 0, NULL)`,
		mlogID, mlogName))

	// Make sure the rows are visible to restricted SQL (infoschema reader uses restricted SQL internally).
	tk.MustExec("commit")
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mview_refresh_hist where mview_id=%s", mviewID)).Check(testkit.Rows("1"))
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mlog_purge_hist where mlog_id=%s", mlogID)).Check(testkit.Rows("1"))

	tk.MustQuery(`
		select mview_id, mview_name, refresh_job_id, is_newest_refresh, refresh_method, refresh_time, refresh_endtime, refresh_status
		from information_schema.tidb_mview_refresh_hist
		where mview_id=` + fmt.Sprint(mviewID),
	).Check(testkit.Rows(fmt.Sprintf("%s %s 123 Y REFRESH FAST 2026-01-01 00:00:00 <nil> <nil>", mviewID, mviewName)))

	tk.MustQuery(`
		select mlog_id, mlog_name, purge_job_id, is_newest_purge, purge_method, purge_time, purge_endtime, purge_rows, purge_status
		from information_schema.tidb_mlog_purge_hist
		where mlog_id=` + fmt.Sprint(mlogID),
	).Check(testkit.Rows(fmt.Sprintf("%s %s 456 Y IMMEDIATE <nil> 2026-01-01 00:00:00 0 <nil>", mlogID, mlogName)))
}

func TestInfoSchemaTiDBMViewsAndMLogsPrivilegeFilteringBySchema(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("create database is_mview_priv1")
	tk.MustExec("create database is_mview_priv2")

	// db1 objects
	tk.MustExec("use is_mview_priv1")
	tk.MustExec("create table t (a int)")
	tk.MustExec("create materialized view log on t (a)")
	tk.MustExec("create materialized view mv1 (a, cnt) as select a, count(*) from t group by a")

	// db2 objects
	tk.MustExec("use is_mview_priv2")
	tk.MustExec("create table t (a int)")
	tk.MustExec("create materialized view log on t (a)")
	tk.MustExec("create materialized view mv2 (a, cnt) as select a, count(*) from t group by a")

	rows := tk.MustQuery("select mview_id from information_schema.tidb_mviews where table_schema='is_mview_priv1' and mview_name='mv1'").Rows()
	require.Len(t, rows, 1)
	mviewID1 := fmt.Sprint(rows[0][0])
	rows = tk.MustQuery("select mview_id from information_schema.tidb_mviews where table_schema='is_mview_priv2' and mview_name='mv2'").Rows()
	require.Len(t, rows, 1)
	mviewID2 := fmt.Sprint(rows[0][0])

	rows = tk.MustQuery("select mlog_id, mlog_name from information_schema.tidb_mlogs where base_table_schema='is_mview_priv1' and base_table_name='t'").Rows()
	require.Len(t, rows, 1)
	mlogID1 := fmt.Sprint(rows[0][0])
	mlogName1 := rows[0][1].(string)
	rows = tk.MustQuery("select mlog_id, mlog_name from information_schema.tidb_mlogs where base_table_schema='is_mview_priv2' and base_table_name='t'").Rows()
	require.Len(t, rows, 1)
	mlogID2 := fmt.Sprint(rows[0][0])
	mlogName2 := rows[0][1].(string)

	tk.MustExec(fmt.Sprintf(`
		insert into mysql.tidb_mview_refresh_hist
			(mview_id, mview_name, refresh_job_id, is_newest_refresh, refresh_method, refresh_time, refresh_endtime, refresh_status)
		values
			(%s, 'mv1', 1, 'Y', 'REFRESH FAST', '2026-01-01 00:00:00', NULL, NULL),
			(%s, 'mv2', 2, 'Y', 'REFRESH FAST', '2026-01-01 00:00:00', NULL, NULL)`,
		mviewID1, mviewID2))
	tk.MustExec(fmt.Sprintf(`
		insert into mysql.tidb_mlog_purge_hist
			(mlog_id, mlog_name, purge_job_id, is_newest_purge, purge_method, purge_time, purge_endtime, purge_rows, purge_status)
		values
			(%s, '%s', 3, 'Y', 'IMMEDIATE', NULL, '2026-01-01 00:00:00', 0, NULL),
			(%s, '%s', 4, 'Y', 'IMMEDIATE', NULL, '2026-01-01 00:00:00', 0, NULL)`,
		mlogID1, mlogName1, mlogID2, mlogName2))
	tk.MustExec("commit")

	tk.MustExec("create user 'mv_priv_u1'@'%'")
	tk.MustExec("grant select on is_mview_priv1.* to 'mv_priv_u1'@'%'")

	tkUser := testkit.NewTestKit(t, store)
	require.NoError(t, tkUser.Session().Auth(&auth.UserIdentity{Username: "mv_priv_u1", Hostname: "%"}, nil, nil, nil))

	tkUser.MustQuery("select table_schema, mview_name from information_schema.tidb_mviews order by table_schema, mview_name").
		Check(testkit.Rows("is_mview_priv1 mv1"))
	tkUser.MustQuery("select base_table_schema, base_table_name from information_schema.tidb_mlogs order by base_table_schema, base_table_name").
		Check(testkit.Rows("is_mview_priv1 t"))
	tkUser.MustQuery("select mview_id, refresh_job_id from information_schema.tidb_mview_refresh_hist order by refresh_job_id").
		Check(testkit.Rows(fmt.Sprintf("%s 1", mviewID1)))
	tkUser.MustQuery("select mlog_id, purge_job_id from information_schema.tidb_mlog_purge_hist order by purge_job_id").
		Check(testkit.Rows(fmt.Sprintf("%s 3", mlogID1)))
}
