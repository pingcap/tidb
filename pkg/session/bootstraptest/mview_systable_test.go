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

package bootstraptest_test

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestBootstrapMaterializedViewSystemTables(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	for _, tbl := range []string{
		"tidb_mview_refresh_info",
		"tidb_mlog_purge_info",
		"tidb_mview_refresh_hist",
		"tidb_mview_refresh_alert",
		"tidb_mlog_purge_hist",
	} {
		tk.MustQuery("select count(*) from information_schema.tables where table_schema='mysql' and table_name='" + tbl + "'").Check(testkit.Rows("1"))
	}

	tk.MustQuery("select lower(column_name) from information_schema.statistics where table_schema='mysql' and table_name='tidb_mview_refresh_info' and index_name='PRIMARY' order by seq_in_index").
		Check(testkit.Rows("mview_id"))
	// The out-of-place MV refresh cutover path updates this table through the
	// table API in migrateMViewRefreshInfoForOutOfPlaceCutover. If this schema
	// or the MVIEW_ID handle property changes, update that function together
	// with this test.
	tk.MustQuery("select lower(column_name), lower(column_type), is_nullable from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_info' order by ordinal_position").
		Check(testkit.Rows("mview_id bigint(20) NO", "last_success_read_tso bigint(20) unsigned YES", "last_success_refresh_end_unix_seconds bigint(20) YES", "next_refresh_unix_seconds bigint(20) YES"))

	tk.MustQuery("select lower(column_name) from information_schema.statistics where table_schema='mysql' and table_name='tidb_mlog_purge_info' and index_name='PRIMARY' order by seq_in_index").
		Check(testkit.Rows("mlog_id"))
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_info' and column_name='LAST_SUCCESS_READ_TSO'").
		Check(testkit.Rows("bigint(20) unsigned"))
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_info' and column_name='NEXT_REFRESH_UNIX_SECONDS'").
		Check(testkit.Rows("bigint(20)"))
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_info' and column_name='LAST_SUCCESS_REFRESH_END_UNIX_SECONDS'").
		Check(testkit.Rows("bigint(20)"))
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mlog_purge_info' and column_name='LAST_PURGED_TSO'").
		Check(testkit.Rows("bigint(20) unsigned"))
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mlog_purge_info' and column_name='NEXT_PURGE_UNIX_SECONDS'").
		Check(testkit.Rows("bigint(20)"))

	tk.MustQuery("select lower(column_name) from information_schema.statistics where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and index_name='PRIMARY' order by seq_in_index").
		Check(testkit.Rows("refresh_job_id"))
	tk.MustQuery("select lower(column_name) from information_schema.statistics where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and index_name='idx_mview_start_time' order by seq_in_index").
		Check(testkit.Rows("mview_id", "refresh_start_time"))
	tk.MustQuery("select lower(column_name) from information_schema.statistics where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and index_name='idx_mv_name_start_time' order by seq_in_index").
		Check(testkit.Rows("mv_schema", "mv_name", "refresh_start_time"))
	tk.MustQuery("select lower(column_name) from information_schema.statistics where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and index_name='idx_mv_name_commit_tso' order by seq_in_index").
		Check(testkit.Rows("mv_schema", "mv_name", "refresh_commit_tso"))
	tk.MustQuery("select lower(column_name) from information_schema.statistics where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and index_name='idx_mview_status_start_time' order by seq_in_index").
		Check(testkit.Rows("mview_id", "refresh_status", "refresh_start_time"))
	tk.MustQuery("select lower(column_name) from information_schema.statistics where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and index_name='idx_refresh_duration_sec' order by seq_in_index").
		Check(testkit.Rows("refresh_duration_sec"))
	tk.MustQuery("select lower(column_name) from information_schema.statistics where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and index_name='idx_refresh_schedule_duration_sec' order by seq_in_index").
		Check(testkit.Rows("refresh_schedule_duration_sec"))
	tk.MustQuery("select lower(column_name) from information_schema.statistics where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and index_name='idx_refresh_start_time' order by seq_in_index").
		Check(testkit.Rows("refresh_start_time"))
	tk.MustQuery("select lower(column_name) from information_schema.statistics where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and index_name='idx_refresh_status_start_time' order by seq_in_index").
		Check(testkit.Rows("refresh_status", "refresh_start_time"))
	tk.MustQuery("select lower(column_name) from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_hist' order by ordinal_position limit 1").
		Check(testkit.Rows("refresh_job_id"))
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and column_name='REFRESH_JOB_ID'").
		Check(testkit.Rows("bigint(20) unsigned"))
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and column_name in ('MV_SCHEMA', 'MV_NAME') order by column_name").
		Check(testkit.Rows("varchar(64)", "varchar(64)"))
	tk.MustQuery("select lower(collation_name) from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and column_name in ('MV_SCHEMA', 'MV_NAME') order by column_name").
		Check(testkit.Rows("utf8mb4_general_ci", "utf8mb4_general_ci"))
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and column_name='REFRESH_READ_TSO'").
		Check(testkit.Rows("bigint(20) unsigned"))
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and column_name='REFRESH_COMMIT_TSO'").
		Check(testkit.Rows("bigint(20) unsigned"))
	tk.MustQuery("select lower(column_name) from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and ordinal_position between 2 and 9 order by ordinal_position").
		Check(testkit.Rows("mview_id", "mv_schema", "mv_name", "refresh_method", "refresh_start_time", "refresh_end_time", "refresh_duration_sec", "refresh_schedule_duration_sec"))
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and column_name='REFRESH_DURATION_SEC'").
		Check(testkit.Rows("decimal(18,6)"))
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and column_name='REFRESH_SCHEDULE_DURATION_SEC'").
		Check(testkit.Rows("decimal(18,6)"))
	tk.MustQuery("select datetime_precision from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and column_name in ('REFRESH_START_TIME', 'REFRESH_END_TIME') order by column_name").
		Check(testkit.Rows("6", "6"))
	tk.MustQuery("select datetime_precision from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and column_name='CANCEL_REQUEST_TIME'").
		Check(testkit.Rows("6"))
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and column_name='CANCEL_REQUESTED_BY'").
		Check(testkit.Rows("varchar(512)"))
	tk.MustQuery("select datetime_precision from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and column_name='LAST_HEARTBEAT_TIME'").
		Check(testkit.Rows("6"))
	tk.MustQuery("select lower(column_name) from information_schema.statistics where table_schema='mysql' and table_name='tidb_mview_refresh_alert' and index_name='PRIMARY' order by seq_in_index").
		Check(testkit.Rows("mview_id"))
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_alert' and column_name in ('MV_SCHEMA', 'MV_NAME') order by column_name").
		Check(testkit.Rows("varchar(64)", "varchar(64)"))
	tk.MustQuery("select lower(collation_name) from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_alert' and column_name in ('MV_SCHEMA', 'MV_NAME') order by column_name").
		Check(testkit.Rows("utf8mb4_general_ci", "utf8mb4_general_ci"))
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_alert' and column_name='ALERT_LEVEL'").
		Check(testkit.Rows("varchar(16)"))
	tk.MustQuery("select is_nullable from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_alert' and column_name='ALERT_LEVEL'").
		Check(testkit.Rows("YES"))
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_alert' and column_name='REFRESH_FAILED'").
		Check(testkit.Rows("varchar(3)"))
	tk.MustQuery("select datetime_precision from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_alert' and column_name in ('LAST_SUCCESS_SNAPSHOT_TIME', 'UPDATE_TIME') order by column_name").
		Check(testkit.Rows("6", "6"))
	tk.MustQuery("select lower(column_name) from information_schema.statistics where table_schema='mysql' and table_name='tidb_mlog_purge_hist' and index_name='PRIMARY' order by seq_in_index").
		Check(testkit.Rows("purge_job_id"))
	tk.MustQuery("select lower(column_name) from information_schema.statistics where table_schema='mysql' and table_name='tidb_mlog_purge_hist' and index_name='idx_mlog_start_time' order by seq_in_index").
		Check(testkit.Rows("mlog_id", "purge_start_time"))
	tk.MustQuery("select lower(column_name) from information_schema.statistics where table_schema='mysql' and table_name='tidb_mlog_purge_hist' and index_name='idx_table_name_start_time' order by seq_in_index").
		Check(testkit.Rows("base_table_schema", "base_table_name", "purge_start_time"))
	tk.MustQuery("select lower(column_name) from information_schema.statistics where table_schema='mysql' and table_name='tidb_mlog_purge_hist' and index_name='idx_mlog_status_start_time' order by seq_in_index").
		Check(testkit.Rows("mlog_id", "purge_status", "purge_start_time"))
	tk.MustQuery("select lower(column_name) from information_schema.statistics where table_schema='mysql' and table_name='tidb_mlog_purge_hist' and index_name='idx_purge_duration_sec' order by seq_in_index").
		Check(testkit.Rows("purge_duration_sec"))
	tk.MustQuery("select lower(column_name) from information_schema.statistics where table_schema='mysql' and table_name='tidb_mlog_purge_hist' and index_name='idx_purge_start_time' order by seq_in_index").
		Check(testkit.Rows("purge_start_time"))
	tk.MustQuery("select lower(column_name) from information_schema.statistics where table_schema='mysql' and table_name='tidb_mlog_purge_hist' and index_name='idx_purge_status_start_time' order by seq_in_index").
		Check(testkit.Rows("purge_status", "purge_start_time"))
	tk.MustQuery("select lower(column_name) from information_schema.columns where table_schema='mysql' and table_name='tidb_mlog_purge_hist' order by ordinal_position limit 1").
		Check(testkit.Rows("purge_job_id"))
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mlog_purge_hist' and column_name='PURGE_JOB_ID'").
		Check(testkit.Rows("bigint(20) unsigned"))
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mlog_purge_hist' and column_name in ('BASE_TABLE_SCHEMA', 'BASE_TABLE_NAME') order by column_name").
		Check(testkit.Rows("varchar(64)", "varchar(64)"))
	tk.MustQuery("select lower(collation_name) from information_schema.columns where table_schema='mysql' and table_name='tidb_mlog_purge_hist' and column_name in ('BASE_TABLE_SCHEMA', 'BASE_TABLE_NAME') order by column_name").
		Check(testkit.Rows("utf8mb4_general_ci", "utf8mb4_general_ci"))
	tk.MustQuery("select count(*) from information_schema.columns where table_schema='mysql' and table_name='tidb_mlog_purge_hist' and column_name='PURGE_FAILED_REASON' and lower(data_type)='text'").
		Check(testkit.Rows("1"))
	tk.MustQuery("select lower(column_name) from information_schema.columns where table_schema='mysql' and table_name='tidb_mlog_purge_hist' and ordinal_position between 2 and 8 order by ordinal_position").
		Check(testkit.Rows("mlog_id", "base_table_schema", "base_table_name", "purge_method", "purge_start_time", "purge_end_time", "purge_duration_sec"))
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mlog_purge_hist' and column_name='PURGE_DURATION_SEC'").
		Check(testkit.Rows("decimal(18,6)"))
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mlog_purge_hist' and column_name='PURGE_CUTOFF_TSO'").
		Check(testkit.Rows("bigint(20) unsigned"))
	tk.MustQuery("select datetime_precision from information_schema.columns where table_schema='mysql' and table_name='tidb_mlog_purge_hist' and column_name in ('PURGE_START_TIME', 'PURGE_END_TIME') order by column_name").
		Check(testkit.Rows("6", "6"))
	tk.MustQuery("select datetime_precision from information_schema.columns where table_schema='mysql' and table_name='tidb_mlog_purge_hist' and column_name='CANCEL_REQUEST_TIME'").
		Check(testkit.Rows("6"))
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mlog_purge_hist' and column_name='CANCEL_REQUESTED_BY'").
		Check(testkit.Rows("varchar(512)"))

	is := domain.GetDomain(tk.Session()).InfoSchema()
	refreshInfoTbl, err := is.TableByName(context.Background(), model.NewCIStr("mysql"), model.NewCIStr("tidb_mview_refresh_info"))
	require.NoError(t, err)
	refreshInfoMeta := refreshInfoTbl.Meta()
	require.True(t, refreshInfoMeta.PKIsHandle)
	require.False(t, refreshInfoMeta.IsCommonHandle)
	require.Len(t, refreshInfoMeta.Columns, 4)
	require.Equal(t, "MVIEW_ID", refreshInfoMeta.Columns[0].Name.O)
	tk.MustQuery("select datetime_precision from information_schema.columns where table_schema='mysql' and table_name='tidb_mlog_purge_hist' and column_name='LAST_HEARTBEAT_TIME'").
		Check(testkit.Rows("6"))
}

func TestUpgradeToVer221MaterializedViewSystemTables(t *testing.T) {
	ctx := context.Background()
	store, dom := session.CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	seV220 := session.CreateSessionAndSetID(t, store)

	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMutator(txn)
	require.NoError(t, m.FinishBootstrap(int64(220)))
	require.NoError(t, txn.Commit(ctx))

	revertVersionAndVariables(t, seV220, 220)
	session.MustExec(t, seV220, "drop table if exists mysql.tidb_mview_refresh_info, mysql.tidb_mlog_purge_info, mysql.tidb_mview_refresh_hist, mysql.tidb_mview_refresh_alert, mysql.tidb_mlog_purge_hist")
	session.MustExec(t, seV220, "create user 'v222_super'@'%'")
	session.MustExec(t, seV220, "create user 'v222_normal'@'%'")
	session.MustExec(t, seV220, "update mysql.user set Super_priv='Y', Operate_view_priv='N' where User='v222_super' and Host='%'")
	session.MustExec(t, seV220, "update mysql.user set Super_priv='N', Operate_view_priv='N' where User='v222_normal' and Host='%'")
	session.MustExec(t, seV220, "commit")

	session.UnsetStoreBootstrapped(store.UUID())
	ver, err := session.GetBootstrapVersion(seV220)
	require.NoError(t, err)
	require.Equal(t, int64(220), ver)

	dom.Close()
	domUpgraded, err := session.BootstrapSession(store)
	require.NoError(t, err)
	defer domUpgraded.Close()

	tk := testkit.NewTestKit(t, store)
	ver, err = session.GetBootstrapVersion(tk.Session())
	require.NoError(t, err)
	require.Equal(t, session.CurrentBootstrapVersion, ver)
	tk.MustQuery("select User, Operate_view_priv from mysql.user where User in ('v222_super', 'v222_normal') order by User").
		Check(testkit.Rows("v222_normal N", "v222_super Y"))
	for _, tbl := range []string{
		"tidb_mview_refresh_info",
		"tidb_mlog_purge_info",
		"tidb_mview_refresh_hist",
		"tidb_mview_refresh_alert",
		"tidb_mlog_purge_hist",
	} {
		tk.MustQuery("select count(*) from information_schema.tables where table_schema='mysql' and table_name='" + tbl + "'").Check(testkit.Rows("1"))
	}
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_info' and column_name='LAST_SUCCESS_READ_TSO'").
		Check(testkit.Rows("bigint(20) unsigned"))
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_info' and column_name='NEXT_REFRESH_UNIX_SECONDS'").
		Check(testkit.Rows("bigint(20)"))
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_info' and column_name='LAST_SUCCESS_REFRESH_END_UNIX_SECONDS'").
		Check(testkit.Rows("bigint(20)"))
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mlog_purge_info' and column_name='LAST_PURGED_TSO'").
		Check(testkit.Rows("bigint(20) unsigned"))
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mlog_purge_info' and column_name='NEXT_PURGE_UNIX_SECONDS'").
		Check(testkit.Rows("bigint(20)"))
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and column_name='REFRESH_JOB_ID'").
		Check(testkit.Rows("bigint(20) unsigned"))
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and column_name in ('MV_SCHEMA', 'MV_NAME') order by column_name").
		Check(testkit.Rows("varchar(64)", "varchar(64)"))
	tk.MustQuery("select lower(collation_name) from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and column_name in ('MV_SCHEMA', 'MV_NAME') order by column_name").
		Check(testkit.Rows("utf8mb4_general_ci", "utf8mb4_general_ci"))
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and column_name='REFRESH_READ_TSO'").
		Check(testkit.Rows("bigint(20) unsigned"))
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and column_name='REFRESH_COMMIT_TSO'").
		Check(testkit.Rows("bigint(20) unsigned"))
	tk.MustQuery("select lower(column_name) from information_schema.statistics where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and index_name='idx_mv_name_commit_tso' order by seq_in_index").
		Check(testkit.Rows("mv_schema", "mv_name", "refresh_commit_tso"))
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and column_name='REFRESH_DURATION_SEC'").
		Check(testkit.Rows("decimal(18,6)"))
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and column_name='REFRESH_SCHEDULE_DURATION_SEC'").
		Check(testkit.Rows("decimal(18,6)"))
	tk.MustQuery("select lower(column_name) from information_schema.statistics where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and index_name='idx_refresh_schedule_duration_sec' order by seq_in_index").
		Check(testkit.Rows("refresh_schedule_duration_sec"))
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mlog_purge_hist' and column_name='PURGE_JOB_ID'").
		Check(testkit.Rows("bigint(20) unsigned"))
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mlog_purge_hist' and column_name in ('BASE_TABLE_SCHEMA', 'BASE_TABLE_NAME') order by column_name").
		Check(testkit.Rows("varchar(64)", "varchar(64)"))
	tk.MustQuery("select lower(collation_name) from information_schema.columns where table_schema='mysql' and table_name='tidb_mlog_purge_hist' and column_name in ('BASE_TABLE_SCHEMA', 'BASE_TABLE_NAME') order by column_name").
		Check(testkit.Rows("utf8mb4_general_ci", "utf8mb4_general_ci"))
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mlog_purge_hist' and column_name='PURGE_DURATION_SEC'").
		Check(testkit.Rows("decimal(18,6)"))
	tk.MustQuery("select datetime_precision from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and column_name in ('REFRESH_START_TIME', 'REFRESH_END_TIME') order by column_name").
		Check(testkit.Rows("6", "6"))
	tk.MustQuery("select datetime_precision from information_schema.columns where table_schema='mysql' and table_name='tidb_mlog_purge_hist' and column_name in ('PURGE_START_TIME', 'PURGE_END_TIME') order by column_name").
		Check(testkit.Rows("6", "6"))
	tk.MustQuery("select datetime_precision from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and column_name='CANCEL_REQUEST_TIME'").
		Check(testkit.Rows("6"))
	tk.MustQuery("select datetime_precision from information_schema.columns where table_schema='mysql' and table_name='tidb_mlog_purge_hist' and column_name='CANCEL_REQUEST_TIME'").
		Check(testkit.Rows("6"))
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and column_name='CANCEL_REQUESTED_BY'").
		Check(testkit.Rows("varchar(512)"))
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mlog_purge_hist' and column_name='CANCEL_REQUESTED_BY'").
		Check(testkit.Rows("varchar(512)"))
}
