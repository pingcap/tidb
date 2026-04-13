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

	"github.com/pingcap/tidb/pkg/meta"
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

	tk.MustQuery("select lower(column_name) from information_schema.statistics where table_schema='mysql' and table_name='tidb_mlog_purge_info' and index_name='PRIMARY' order by seq_in_index").
		Check(testkit.Rows("mlog_id"))
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_info' and column_name='LAST_SUCCESS_READ_TSO'").
		Check(testkit.Rows("bigint(20) unsigned"))
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mlog_purge_info' and column_name='LAST_PURGED_TSO'").
		Check(testkit.Rows("bigint(20) unsigned"))

	tk.MustQuery("select lower(column_name) from information_schema.statistics where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and index_name='PRIMARY' order by seq_in_index").
		Check(testkit.Rows("refresh_job_id"))
	tk.MustQuery("select lower(column_name) from information_schema.statistics where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and index_name='idx_mview_time' order by seq_in_index").
		Check(testkit.Rows("mview_id", "refresh_time"))
	tk.MustQuery("select lower(column_name) from information_schema.statistics where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and index_name='idx_mv_name_time' order by seq_in_index").
		Check(testkit.Rows("mv_schema", "mv_name", "refresh_time"))
	tk.MustQuery("select lower(column_name) from information_schema.statistics where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and index_name='idx_mview_status' order by seq_in_index").
		Check(testkit.Rows("mview_id", "refresh_status", "refresh_time"))
	tk.MustQuery("select lower(column_name) from information_schema.statistics where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and index_name='idx_refresh_duration_sec' order by seq_in_index").
		Check(testkit.Rows("refresh_duration_sec"))
	tk.MustQuery("select lower(column_name) from information_schema.statistics where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and index_name='idx_refresh_time' order by seq_in_index").
		Check(testkit.Rows("refresh_time"))
	tk.MustQuery("select lower(column_name) from information_schema.statistics where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and index_name='idx_refresh_status' order by seq_in_index").
		Check(testkit.Rows("refresh_status", "refresh_time"))
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
	tk.MustQuery("select lower(column_name) from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and ordinal_position between 2 and 8 order by ordinal_position").
		Check(testkit.Rows("mview_id", "mv_schema", "mv_name", "refresh_method", "refresh_time", "refresh_endtime", "refresh_duration_sec"))
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and column_name='REFRESH_DURATION_SEC'").
		Check(testkit.Rows("decimal(18,6)"))
	tk.MustQuery("select datetime_precision from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and column_name in ('REFRESH_TIME', 'REFRESH_ENDTIME') order by column_name").
		Check(testkit.Rows("6", "6"))
	tk.MustQuery("select datetime_precision from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and column_name='CANCEL_REQUESTED_AT'").
		Check(testkit.Rows("6"))
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and column_name='CANCEL_REQUESTED_BY'").
		Check(testkit.Rows("varchar(512)"))
	tk.MustQuery("select datetime_precision from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and column_name='LAST_HEARTBEAT_AT'").
		Check(testkit.Rows("6"))
	tk.MustQuery("select lower(column_name) from information_schema.statistics where table_schema='mysql' and table_name='tidb_mview_refresh_alert' and index_name='PRIMARY' order by seq_in_index").
		Check(testkit.Rows("mview_id"))
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_alert' and column_name in ('MV_SCHEMA', 'MV_NAME') order by column_name").
		Check(testkit.Rows("varchar(64)", "varchar(64)"))
	tk.MustQuery("select lower(collation_name) from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_alert' and column_name in ('MV_SCHEMA', 'MV_NAME') order by column_name").
		Check(testkit.Rows("utf8mb4_general_ci", "utf8mb4_general_ci"))
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_alert' and column_name='ALERT_LEVEL'").
		Check(testkit.Rows("varchar(16)"))
	tk.MustQuery("select datetime_precision from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_alert' and column_name in ('LAST_SUCCESS_TIME', 'UPDATED_AT') order by column_name").
		Check(testkit.Rows("6", "6"))
	tk.MustQuery("select lower(column_name) from information_schema.statistics where table_schema='mysql' and table_name='tidb_mlog_purge_hist' and index_name='PRIMARY' order by seq_in_index").
		Check(testkit.Rows("purge_job_id"))
	tk.MustQuery("select lower(column_name) from information_schema.statistics where table_schema='mysql' and table_name='tidb_mlog_purge_hist' and index_name='idx_mlog_time' order by seq_in_index").
		Check(testkit.Rows("mlog_id", "purge_time"))
	tk.MustQuery("select lower(column_name) from information_schema.statistics where table_schema='mysql' and table_name='tidb_mlog_purge_hist' and index_name='idx_table_name_time' order by seq_in_index").
		Check(testkit.Rows("base_table_schema", "base_table_name", "purge_time"))
	tk.MustQuery("select lower(column_name) from information_schema.statistics where table_schema='mysql' and table_name='tidb_mlog_purge_hist' and index_name='idx_mlog_status' order by seq_in_index").
		Check(testkit.Rows("mlog_id", "purge_status", "purge_time"))
	tk.MustQuery("select lower(column_name) from information_schema.statistics where table_schema='mysql' and table_name='tidb_mlog_purge_hist' and index_name='idx_purge_duration_sec' order by seq_in_index").
		Check(testkit.Rows("purge_duration_sec"))
	tk.MustQuery("select lower(column_name) from information_schema.statistics where table_schema='mysql' and table_name='tidb_mlog_purge_hist' and index_name='idx_purge_time' order by seq_in_index").
		Check(testkit.Rows("purge_time"))
	tk.MustQuery("select lower(column_name) from information_schema.statistics where table_schema='mysql' and table_name='tidb_mlog_purge_hist' and index_name='idx_purge_status' order by seq_in_index").
		Check(testkit.Rows("purge_status", "purge_time"))
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
		Check(testkit.Rows("mlog_id", "base_table_schema", "base_table_name", "purge_method", "purge_time", "purge_endtime", "purge_duration_sec"))
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mlog_purge_hist' and column_name='PURGE_DURATION_SEC'").
		Check(testkit.Rows("decimal(18,6)"))
	tk.MustQuery("select datetime_precision from information_schema.columns where table_schema='mysql' and table_name='tidb_mlog_purge_hist' and column_name in ('PURGE_TIME', 'PURGE_ENDTIME') order by column_name").
		Check(testkit.Rows("6", "6"))
	tk.MustQuery("select datetime_precision from information_schema.columns where table_schema='mysql' and table_name='tidb_mlog_purge_hist' and column_name='CANCEL_REQUESTED_AT'").
		Check(testkit.Rows("6"))
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mlog_purge_hist' and column_name='CANCEL_REQUESTED_BY'").
		Check(testkit.Rows("varchar(512)"))
	tk.MustQuery("select datetime_precision from information_schema.columns where table_schema='mysql' and table_name='tidb_mlog_purge_hist' and column_name='LAST_HEARTBEAT_AT'").
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
	session.MustExec(t, seV220, "drop table if exists mysql.tidb_mview_refresh_info, mysql.tidb_mlog_purge_info, mysql.tidb_mview_refresh_hist, mysql.tidb_mlog_purge_hist")
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
	for _, tbl := range []string{
		"tidb_mview_refresh_info",
		"tidb_mlog_purge_info",
		"tidb_mview_refresh_hist",
		"tidb_mlog_purge_hist",
	} {
		tk.MustQuery("select count(*) from information_schema.tables where table_schema='mysql' and table_name='" + tbl + "'").Check(testkit.Rows("1"))
	}
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_info' and column_name='LAST_SUCCESS_READ_TSO'").
		Check(testkit.Rows("bigint(20) unsigned"))
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mlog_purge_info' and column_name='LAST_PURGED_TSO'").
		Check(testkit.Rows("bigint(20) unsigned"))
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and column_name='REFRESH_JOB_ID'").
		Check(testkit.Rows("bigint(20) unsigned"))
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and column_name in ('MV_SCHEMA', 'MV_NAME') order by column_name").
		Check(testkit.Rows("varchar(64)", "varchar(64)"))
	tk.MustQuery("select lower(collation_name) from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and column_name in ('MV_SCHEMA', 'MV_NAME') order by column_name").
		Check(testkit.Rows("utf8mb4_general_ci", "utf8mb4_general_ci"))
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and column_name='REFRESH_READ_TSO'").
		Check(testkit.Rows("bigint(20) unsigned"))
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and column_name='REFRESH_DURATION_SEC'").
		Check(testkit.Rows("decimal(18,6)"))
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mlog_purge_hist' and column_name='PURGE_JOB_ID'").
		Check(testkit.Rows("bigint(20) unsigned"))
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mlog_purge_hist' and column_name in ('BASE_TABLE_SCHEMA', 'BASE_TABLE_NAME') order by column_name").
		Check(testkit.Rows("varchar(64)", "varchar(64)"))
	tk.MustQuery("select lower(collation_name) from information_schema.columns where table_schema='mysql' and table_name='tidb_mlog_purge_hist' and column_name in ('BASE_TABLE_SCHEMA', 'BASE_TABLE_NAME') order by column_name").
		Check(testkit.Rows("utf8mb4_general_ci", "utf8mb4_general_ci"))
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mlog_purge_hist' and column_name='PURGE_DURATION_SEC'").
		Check(testkit.Rows("decimal(18,6)"))
	tk.MustQuery("select datetime_precision from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and column_name in ('REFRESH_TIME', 'REFRESH_ENDTIME') order by column_name").
		Check(testkit.Rows("6", "6"))
	tk.MustQuery("select datetime_precision from information_schema.columns where table_schema='mysql' and table_name='tidb_mlog_purge_hist' and column_name in ('PURGE_TIME', 'PURGE_ENDTIME') order by column_name").
		Check(testkit.Rows("6", "6"))
	tk.MustQuery("select datetime_precision from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and column_name='CANCEL_REQUESTED_AT'").
		Check(testkit.Rows("6"))
	tk.MustQuery("select datetime_precision from information_schema.columns where table_schema='mysql' and table_name='tidb_mlog_purge_hist' and column_name='CANCEL_REQUESTED_AT'").
		Check(testkit.Rows("6"))
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and column_name='CANCEL_REQUESTED_BY'").
		Check(testkit.Rows("varchar(512)"))
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mlog_purge_hist' and column_name='CANCEL_REQUESTED_BY'").
		Check(testkit.Rows("varchar(512)"))
}

func TestUpgradeToVer223MaterializedViewHistoryColumnsAndIndexes(t *testing.T) {
	ctx := context.Background()
	store, dom := session.CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	seV222 := session.CreateSessionAndSetID(t, store)

	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMutator(txn)
	require.NoError(t, m.FinishBootstrap(int64(222)))
	require.NoError(t, txn.Commit(ctx))

	revertVersionAndVariables(t, seV222, 222)
	session.MustExec(t, seV222, "drop table if exists mysql.tidb_mview_refresh_hist, mysql.tidb_mlog_purge_hist")
	session.MustExec(t, seV222, `create table mysql.tidb_mview_refresh_hist (
		REFRESH_JOB_ID bigint unsigned NOT NULL,
		MVIEW_ID bigint NOT NULL,
		REFRESH_METHOD varchar(32) NOT NULL,
		REFRESH_TIME datetime(6) DEFAULT NULL,
		REFRESH_ENDTIME datetime(6) DEFAULT NULL,
		REFRESH_STATUS varchar(16) DEFAULT NULL,
		REFRESH_ROWS bigint DEFAULT NULL,
		REFRESH_READ_TSO bigint unsigned DEFAULT NULL,
		REFRESH_FAILED_REASON text DEFAULT NULL,
		CANCEL_REQUESTED_AT datetime(6) DEFAULT NULL,
		CANCEL_REQUESTED_BY varchar(512) DEFAULT NULL,
		PRIMARY KEY(REFRESH_JOB_ID),
		KEY idx_mview_status (MVIEW_ID, REFRESH_STATUS, REFRESH_TIME),
		KEY idx_refresh_status (REFRESH_STATUS, REFRESH_TIME))`)
	session.MustExec(t, seV222, `create table mysql.tidb_mlog_purge_hist (
		PURGE_JOB_ID bigint unsigned NOT NULL,
		MLOG_ID bigint NOT NULL,
		PURGE_METHOD varchar(32) NOT NULL,
		PURGE_TIME datetime(6) DEFAULT NULL,
		PURGE_ENDTIME datetime(6) DEFAULT NULL,
		PURGE_ROWS bigint NOT NULL,
		PURGE_STATUS varchar(16) DEFAULT NULL,
		PURGE_FAILED_REASON text DEFAULT NULL,
		CANCEL_REQUESTED_AT datetime(6) DEFAULT NULL,
		CANCEL_REQUESTED_BY varchar(512) DEFAULT NULL,
		PRIMARY KEY(PURGE_JOB_ID),
		KEY idx_mlog_status (MLOG_ID, PURGE_STATUS, PURGE_TIME),
		KEY idx_purge_status (PURGE_STATUS, PURGE_TIME))`)
	session.MustExec(t, seV222, "commit")

	session.UnsetStoreBootstrapped(store.UUID())
	ver, err := session.GetBootstrapVersion(seV222)
	require.NoError(t, err)
	require.Equal(t, int64(222), ver)

	dom.Close()
	domUpgraded, err := session.BootstrapSession(store)
	require.NoError(t, err)
	defer domUpgraded.Close()

	tk := testkit.NewTestKit(t, store)
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and column_name in ('MV_SCHEMA', 'MV_NAME') order by column_name").
		Check(testkit.Rows("varchar(64)", "varchar(64)"))
	tk.MustQuery("select lower(collation_name) from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and column_name in ('MV_SCHEMA', 'MV_NAME') order by column_name").
		Check(testkit.Rows("utf8mb4_general_ci", "utf8mb4_general_ci"))
	tk.MustQuery("select lower(column_name) from information_schema.statistics where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and index_name='idx_mview_time' order by seq_in_index").
		Check(testkit.Rows("mview_id", "refresh_time"))
	tk.MustQuery("select lower(column_name) from information_schema.statistics where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and index_name='idx_mv_name_time' order by seq_in_index").
		Check(testkit.Rows("mv_schema", "mv_name", "refresh_time"))
	tk.MustQuery("select lower(column_name) from information_schema.statistics where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and index_name='idx_refresh_duration_sec' order by seq_in_index").
		Check(testkit.Rows("refresh_duration_sec"))
	tk.MustQuery("select lower(column_name) from information_schema.statistics where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and index_name='idx_refresh_time' order by seq_in_index").
		Check(testkit.Rows("refresh_time"))
	tk.MustQuery("select lower(column_name) from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and ordinal_position between 2 and 8 order by ordinal_position").
		Check(testkit.Rows("mview_id", "mv_schema", "mv_name", "refresh_method", "refresh_time", "refresh_endtime", "refresh_duration_sec"))
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and column_name='REFRESH_DURATION_SEC'").
		Check(testkit.Rows("decimal(18,6)"))
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mlog_purge_hist' and column_name in ('BASE_TABLE_SCHEMA', 'BASE_TABLE_NAME') order by column_name").
		Check(testkit.Rows("varchar(64)", "varchar(64)"))
	tk.MustQuery("select lower(collation_name) from information_schema.columns where table_schema='mysql' and table_name='tidb_mlog_purge_hist' and column_name in ('BASE_TABLE_SCHEMA', 'BASE_TABLE_NAME') order by column_name").
		Check(testkit.Rows("utf8mb4_general_ci", "utf8mb4_general_ci"))
	tk.MustQuery("select lower(column_name) from information_schema.statistics where table_schema='mysql' and table_name='tidb_mlog_purge_hist' and index_name='idx_mlog_time' order by seq_in_index").
		Check(testkit.Rows("mlog_id", "purge_time"))
	tk.MustQuery("select lower(column_name) from information_schema.statistics where table_schema='mysql' and table_name='tidb_mlog_purge_hist' and index_name='idx_table_name_time' order by seq_in_index").
		Check(testkit.Rows("base_table_schema", "base_table_name", "purge_time"))
	tk.MustQuery("select lower(column_name) from information_schema.statistics where table_schema='mysql' and table_name='tidb_mlog_purge_hist' and index_name='idx_purge_duration_sec' order by seq_in_index").
		Check(testkit.Rows("purge_duration_sec"))
	tk.MustQuery("select lower(column_name) from information_schema.statistics where table_schema='mysql' and table_name='tidb_mlog_purge_hist' and index_name='idx_purge_time' order by seq_in_index").
		Check(testkit.Rows("purge_time"))
	tk.MustQuery("select lower(column_name) from information_schema.columns where table_schema='mysql' and table_name='tidb_mlog_purge_hist' and ordinal_position between 2 and 8 order by ordinal_position").
		Check(testkit.Rows("mlog_id", "base_table_schema", "base_table_name", "purge_method", "purge_time", "purge_endtime", "purge_duration_sec"))
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mlog_purge_hist' and column_name='PURGE_DURATION_SEC'").
		Check(testkit.Rows("decimal(18,6)"))
}

func TestUpgradeToVer224MaterializedViewHistoryDurationIndexesAndAlertTable(t *testing.T) {
	ctx := context.Background()
	store, dom := session.CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	seV223 := session.CreateSessionAndSetID(t, store)

	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMutator(txn)
	require.NoError(t, m.FinishBootstrap(int64(223)))
	require.NoError(t, txn.Commit(ctx))

	revertVersionAndVariables(t, seV223, 223)
	session.MustExec(t, seV223, "drop table if exists mysql.tidb_mview_refresh_hist, mysql.tidb_mview_refresh_alert, mysql.tidb_mlog_purge_hist")
	session.MustExec(t, seV223, `create table mysql.tidb_mview_refresh_hist (
		REFRESH_JOB_ID bigint unsigned NOT NULL,
		MVIEW_ID bigint NOT NULL,
		MV_SCHEMA varchar(64) CHARSET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,
		MV_NAME varchar(64) CHARSET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,
		REFRESH_METHOD varchar(32) NOT NULL,
		REFRESH_TIME datetime(6) DEFAULT NULL,
		REFRESH_ENDTIME datetime(6) DEFAULT NULL,
		REFRESH_DURATION_SEC decimal(18,6) DEFAULT NULL,
		REFRESH_STATUS varchar(16) DEFAULT NULL,
		REFRESH_ROWS bigint DEFAULT NULL,
		REFRESH_READ_TSO bigint unsigned DEFAULT NULL,
		REFRESH_FAILED_REASON text DEFAULT NULL,
		CANCEL_REQUESTED_AT datetime(6) DEFAULT NULL,
		CANCEL_REQUESTED_BY varchar(512) DEFAULT NULL,
		PRIMARY KEY(REFRESH_JOB_ID),
		KEY idx_mview_time (MVIEW_ID, REFRESH_TIME),
		KEY idx_mv_name_time (MV_SCHEMA, MV_NAME, REFRESH_TIME),
		KEY idx_mview_status (MVIEW_ID, REFRESH_STATUS, REFRESH_TIME),
		KEY idx_refresh_time (REFRESH_TIME),
		KEY idx_refresh_status (REFRESH_STATUS, REFRESH_TIME))`)
	session.MustExec(t, seV223, `create table mysql.tidb_mlog_purge_hist (
		PURGE_JOB_ID bigint unsigned NOT NULL,
		MLOG_ID bigint NOT NULL,
		BASE_TABLE_SCHEMA varchar(64) CHARSET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,
		BASE_TABLE_NAME varchar(64) CHARSET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,
		PURGE_METHOD varchar(32) NOT NULL,
		PURGE_TIME datetime(6) DEFAULT NULL,
		PURGE_ENDTIME datetime(6) DEFAULT NULL,
		PURGE_DURATION_SEC decimal(18,6) DEFAULT NULL,
		PURGE_ROWS bigint NOT NULL,
		PURGE_STATUS varchar(16) DEFAULT NULL,
		PURGE_FAILED_REASON text DEFAULT NULL,
		CANCEL_REQUESTED_AT datetime(6) DEFAULT NULL,
		CANCEL_REQUESTED_BY varchar(512) DEFAULT NULL,
		PRIMARY KEY(PURGE_JOB_ID),
		KEY idx_mlog_time (MLOG_ID, PURGE_TIME),
		KEY idx_table_name_time (BASE_TABLE_SCHEMA, BASE_TABLE_NAME, PURGE_TIME),
		KEY idx_mlog_status (MLOG_ID, PURGE_STATUS, PURGE_TIME),
		KEY idx_purge_time (PURGE_TIME),
		KEY idx_purge_status (PURGE_STATUS, PURGE_TIME))`)
	session.MustExec(t, seV223, "commit")

	session.UnsetStoreBootstrapped(store.UUID())
	ver, err := session.GetBootstrapVersion(seV223)
	require.NoError(t, err)
	require.Equal(t, int64(223), ver)

	dom.Close()
	domUpgraded, err := session.BootstrapSession(store)
	require.NoError(t, err)
	defer domUpgraded.Close()

	tk := testkit.NewTestKit(t, store)
	tk.MustQuery("select count(*) from information_schema.tables where table_schema='mysql' and table_name='tidb_mview_refresh_alert'").
		Check(testkit.Rows("1"))
	tk.MustQuery("select lower(column_name) from information_schema.statistics where table_schema='mysql' and table_name='tidb_mview_refresh_alert' and index_name='PRIMARY' order by seq_in_index").
		Check(testkit.Rows("mview_id"))
	tk.MustQuery("select lower(column_name) from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_alert' order by ordinal_position").
		Check(testkit.Rows("mview_id", "mv_schema", "mv_name", "alert_level", "last_success_time", "updated_at"))
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_alert' and column_name in ('MV_SCHEMA', 'MV_NAME') order by column_name").
		Check(testkit.Rows("varchar(64)", "varchar(64)"))
	tk.MustQuery("select lower(collation_name) from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_alert' and column_name in ('MV_SCHEMA', 'MV_NAME') order by column_name").
		Check(testkit.Rows("utf8mb4_general_ci", "utf8mb4_general_ci"))
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_alert' and column_name='ALERT_LEVEL'").
		Check(testkit.Rows("varchar(16)"))
	tk.MustQuery("select datetime_precision from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_alert' and column_name in ('LAST_SUCCESS_TIME', 'UPDATED_AT') order by column_name").
		Check(testkit.Rows("6", "6"))
	tk.MustQuery("select lower(column_name) from information_schema.statistics where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and index_name='idx_refresh_duration_sec' order by seq_in_index").
		Check(testkit.Rows("refresh_duration_sec"))
	tk.MustQuery("select lower(column_name) from information_schema.statistics where table_schema='mysql' and table_name='tidb_mlog_purge_hist' and index_name='idx_purge_duration_sec' order by seq_in_index").
		Check(testkit.Rows("purge_duration_sec"))
	tk.MustQuery("select datetime_precision from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and column_name='LAST_HEARTBEAT_AT'").
		Check(testkit.Rows("6"))
	tk.MustQuery("select datetime_precision from information_schema.columns where table_schema='mysql' and table_name='tidb_mlog_purge_hist' and column_name='LAST_HEARTBEAT_AT'").
		Check(testkit.Rows("6"))
}

func TestUpgradeToVer222MaterializedViewHistoryCancelRequestColumns(t *testing.T) {
	ctx := context.Background()
	store, dom := session.CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	seV221 := session.CreateSessionAndSetID(t, store)

	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMutator(txn)
	require.NoError(t, m.FinishBootstrap(int64(221)))
	require.NoError(t, txn.Commit(ctx))

	revertVersionAndVariables(t, seV221, 221)
	session.MustExec(t, seV221, "drop table if exists mysql.tidb_mview_refresh_hist, mysql.tidb_mlog_purge_hist")
	session.MustExec(t, seV221, `create table mysql.tidb_mview_refresh_hist (
		REFRESH_JOB_ID bigint unsigned NOT NULL,
		MVIEW_ID bigint NOT NULL,
		REFRESH_METHOD varchar(32) NOT NULL,
		REFRESH_TIME datetime(6) DEFAULT NULL,
		REFRESH_ENDTIME datetime(6) DEFAULT NULL,
		REFRESH_STATUS varchar(16) DEFAULT NULL,
		REFRESH_ROWS bigint DEFAULT NULL,
		REFRESH_READ_TSO bigint unsigned DEFAULT NULL,
		REFRESH_FAILED_REASON text DEFAULT NULL,
		PRIMARY KEY(REFRESH_JOB_ID),
		KEY idx_mview_status (MVIEW_ID, REFRESH_STATUS, REFRESH_TIME),
		KEY idx_refresh_status (REFRESH_STATUS, REFRESH_TIME))`)
	session.MustExec(t, seV221, `create table mysql.tidb_mlog_purge_hist (
		PURGE_JOB_ID bigint unsigned NOT NULL,
		MLOG_ID bigint NOT NULL,
		PURGE_METHOD varchar(32) NOT NULL,
		PURGE_TIME datetime(6) DEFAULT NULL,
		PURGE_ENDTIME datetime(6) DEFAULT NULL,
		PURGE_ROWS bigint NOT NULL,
		PURGE_STATUS varchar(16) DEFAULT NULL,
		PURGE_FAILED_REASON text DEFAULT NULL,
		PRIMARY KEY(PURGE_JOB_ID),
		KEY idx_mlog_status (MLOG_ID, PURGE_STATUS, PURGE_TIME),
		KEY idx_purge_status (PURGE_STATUS, PURGE_TIME))`)
	session.MustExec(t, seV221, "commit")

	session.UnsetStoreBootstrapped(store.UUID())
	ver, err := session.GetBootstrapVersion(seV221)
	require.NoError(t, err)
	require.Equal(t, int64(221), ver)

	dom.Close()
	domUpgraded, err := session.BootstrapSession(store)
	require.NoError(t, err)
	defer domUpgraded.Close()

	tk := testkit.NewTestKit(t, store)
	tk.MustQuery("select datetime_precision from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and column_name='CANCEL_REQUESTED_AT'").
		Check(testkit.Rows("6"))
	tk.MustQuery("select datetime_precision from information_schema.columns where table_schema='mysql' and table_name='tidb_mlog_purge_hist' and column_name='CANCEL_REQUESTED_AT'").
		Check(testkit.Rows("6"))
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and column_name='CANCEL_REQUESTED_BY'").
		Check(testkit.Rows("varchar(512)"))
	tk.MustQuery("select lower(column_type) from information_schema.columns where table_schema='mysql' and table_name='tidb_mlog_purge_hist' and column_name='CANCEL_REQUESTED_BY'").
		Check(testkit.Rows("varchar(512)"))
}

func TestUpgradeToVer224MaterializedViewHistoryHeartbeatColumns(t *testing.T) {
	ctx := context.Background()
	store, dom := session.CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	seV224 := session.CreateSessionAndSetID(t, store)

	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMutator(txn)
	require.NoError(t, m.FinishBootstrap(int64(224)))
	require.NoError(t, txn.Commit(ctx))

	revertVersionAndVariables(t, seV224, 224)
	session.MustExec(t, seV224, "drop table if exists mysql.tidb_mview_refresh_hist, mysql.tidb_mlog_purge_hist")
	session.MustExec(t, seV224, `create table mysql.tidb_mview_refresh_hist (
		REFRESH_JOB_ID bigint unsigned NOT NULL,
		MVIEW_ID bigint NOT NULL,
		MV_SCHEMA varchar(64) CHARSET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,
		MV_NAME varchar(64) CHARSET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,
		REFRESH_METHOD varchar(32) NOT NULL,
		REFRESH_TIME datetime(6) DEFAULT NULL,
		REFRESH_ENDTIME datetime(6) DEFAULT NULL,
		REFRESH_DURATION_SEC decimal(18,6) DEFAULT NULL,
		REFRESH_STATUS varchar(16) DEFAULT NULL,
		REFRESH_ROWS bigint DEFAULT NULL,
		REFRESH_READ_TSO bigint unsigned DEFAULT NULL,
		REFRESH_FAILED_REASON text DEFAULT NULL,
		CANCEL_REQUESTED_AT datetime(6) DEFAULT NULL,
		CANCEL_REQUESTED_BY varchar(512) DEFAULT NULL,
		PRIMARY KEY(REFRESH_JOB_ID),
		KEY idx_mview_time (MVIEW_ID, REFRESH_TIME),
		KEY idx_mv_name_time (MV_SCHEMA, MV_NAME, REFRESH_TIME),
		KEY idx_mview_status (MVIEW_ID, REFRESH_STATUS, REFRESH_TIME),
		KEY idx_refresh_duration_sec (REFRESH_DURATION_SEC),
		KEY idx_refresh_time (REFRESH_TIME),
		KEY idx_refresh_status (REFRESH_STATUS, REFRESH_TIME))`)
	session.MustExec(t, seV224, `create table mysql.tidb_mlog_purge_hist (
		PURGE_JOB_ID bigint unsigned NOT NULL,
		MLOG_ID bigint NOT NULL,
		BASE_TABLE_SCHEMA varchar(64) CHARSET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,
		BASE_TABLE_NAME varchar(64) CHARSET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,
		PURGE_METHOD varchar(32) NOT NULL,
		PURGE_TIME datetime(6) DEFAULT NULL,
		PURGE_ENDTIME datetime(6) DEFAULT NULL,
		PURGE_DURATION_SEC decimal(18,6) DEFAULT NULL,
		PURGE_ROWS bigint NOT NULL,
		PURGE_STATUS varchar(16) DEFAULT NULL,
		PURGE_FAILED_REASON text DEFAULT NULL,
		CANCEL_REQUESTED_AT datetime(6) DEFAULT NULL,
		CANCEL_REQUESTED_BY varchar(512) DEFAULT NULL,
		PRIMARY KEY(PURGE_JOB_ID),
		KEY idx_mlog_time (MLOG_ID, PURGE_TIME),
		KEY idx_table_name_time (BASE_TABLE_SCHEMA, BASE_TABLE_NAME, PURGE_TIME),
		KEY idx_mlog_status (MLOG_ID, PURGE_STATUS, PURGE_TIME),
		KEY idx_purge_duration_sec (PURGE_DURATION_SEC),
		KEY idx_purge_time (PURGE_TIME),
		KEY idx_purge_status (PURGE_STATUS, PURGE_TIME))`)
	session.MustExec(t, seV224, "commit")

	session.UnsetStoreBootstrapped(store.UUID())
	ver, err := session.GetBootstrapVersion(seV224)
	require.NoError(t, err)
	require.Equal(t, int64(224), ver)

	dom.Close()
	domUpgraded, err := session.BootstrapSession(store)
	require.NoError(t, err)
	defer domUpgraded.Close()

	tk := testkit.NewTestKit(t, store)
	tk.MustQuery("select datetime_precision from information_schema.columns where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and column_name='LAST_HEARTBEAT_AT'").
		Check(testkit.Rows("6"))
	tk.MustQuery("select datetime_precision from information_schema.columns where table_schema='mysql' and table_name='tidb_mlog_purge_hist' and column_name='LAST_HEARTBEAT_AT'").
		Check(testkit.Rows("6"))
}
