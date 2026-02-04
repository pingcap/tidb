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

	tk.MustExec("create user 'mv_priv_u1'@'%'")
	tk.MustExec("grant select on is_mview_priv1.* to 'mv_priv_u1'@'%'")

	tkUser := testkit.NewTestKit(t, store)
	require.NoError(t, tkUser.Session().Auth(&auth.UserIdentity{Username: "mv_priv_u1", Hostname: "%"}, nil, nil, nil))

	tkUser.MustQuery("select table_schema, mview_name from information_schema.tidb_mviews order by table_schema, mview_name").
		Check(testkit.Rows("is_mview_priv1 mv1"))
	tkUser.MustQuery("select base_table_schema, base_table_name from information_schema.tidb_mlogs order by base_table_schema, base_table_name").
		Check(testkit.Rows("is_mview_priv1 t"))
}
