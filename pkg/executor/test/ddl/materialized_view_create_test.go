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

package ddl_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl"
	ddlsess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/stretchr/testify/require"
)

func newMViewTestKit(t testing.TB, store kv.Storage) *testkit.TestKit {
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set tidb_materialized_view_enable = on")
	return tk
}

func TestCreateMaterializedViewAndLog(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set div_precision_increment = 9")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")

	tk.MustExec("set tidb_materialized_view_enable = off")
	err := tk.ExecToErr("create materialized view log on t (a, b)")
	require.ErrorContains(t, err, "tidb_materialized_view_enable")
	tk.MustExec("set tidb_materialized_view_enable = on")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("set tidb_materialized_view_enable = off")
	err = tk.ExecToErr("create materialized view mv_disabled (a, s, cnt) as select a, sum(b), count(1) from t group by a")
	require.ErrorContains(t, err, "tidb_materialized_view_enable")
	tk.MustExec("set tidb_materialized_view_enable = on")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")
	tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 7 1"))

	is := dom.InfoSchema()
	baseTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	mlogTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("$mlog$t"))
	require.NoError(t, err)
	mviewTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv"))
	require.NoError(t, err)

	require.NotNil(t, baseTable.Meta().MaterializedViewBase)
	require.Equal(t, mlogTable.Meta().ID, baseTable.Meta().MaterializedViewBase.MLogID)
	require.Contains(t, baseTable.Meta().MaterializedViewBase.MViewIDs, mviewTable.Meta().ID)
	require.NotNil(t, mlogTable.Meta().MaterializedViewLog)
	require.Equal(t, baseTable.Meta().ID, mlogTable.Meta().MaterializedViewLog.BaseTableID)
	require.NotNil(t, mviewTable.Meta().MaterializedView)
	require.Equal(t, model.MViewInitBuildReady, mviewTable.Meta().MaterializedView.GetInitBuildState())
	require.Equal(t, 9, mviewTable.Meta().MaterializedView.DefinitionDivPrecisionIncrement)

	tk.MustQuery("select count(*) from mysql.tidb_mlog_purge_info where mlog_id = ?", mlogTable.Meta().ID).Check(testkit.Rows("1"))
	tk.MustQuery("select last_success_read_tso > 0 from mysql.tidb_mview_refresh_info where mview_id = ?", mviewTable.Meta().ID).Check(testkit.Rows("1"))
}

func TestCreateMaterializedViewLogBasic(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, b int)")
	expectedSQLMode := tk.Session().GetSessionVars().SQLMode

	tk.MustExec("create materialized view log on t (a) purge start with cast('2026-01-02 03:04:05' as datetime) next cast('2026-01-02 03:14:05' as datetime) alert rows 1234")

	tk.MustQuery("select count(*) from information_schema.tables where table_schema='test' and table_name='$mlog$t'").Check(testkit.Rows("1"))

	is := dom.InfoSchema()
	baseTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	mlogTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("$mlog$t"))
	require.NoError(t, err)

	require.NotNil(t, baseTable.Meta().MaterializedViewBase)
	require.Equal(t, mlogTable.Meta().ID, baseTable.Meta().MaterializedViewBase.MLogID)
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mlog_purge_info where mlog_id = %d", mlogTable.Meta().ID)).
		Check(testkit.Rows("1"))

	mlogInfo := mlogTable.Meta().MaterializedViewLog
	require.NotNil(t, mlogInfo)
	require.Equal(t, baseTable.Meta().ID, mlogInfo.BaseTableID)
	require.Equal(t, []ast.CIStr{ast.NewCIStr("a")}, mlogInfo.Columns)
	require.Equal(t, "DEFERRED", mlogInfo.PurgeMethod)
	require.Equal(t, "CAST('2026-01-02 03:04:05' AS DATETIME)", mlogInfo.PurgeStartWith)
	require.Equal(t, "CAST('2026-01-02 03:14:05' AS DATETIME)", mlogInfo.PurgeNext)
	require.NotNil(t, mlogInfo.LogAccumulationAlertRows)
	require.Equal(t, uint64(1234), *mlogInfo.LogAccumulationAlertRows)
	require.Equal(t, expectedSQLMode, mlogInfo.DefinitionSQLMode)

	var hasDMLType, hasOldNew bool
	for _, col := range mlogTable.Meta().Columns {
		if col.Name.L == strings.ToLower(model.MaterializedViewLogDMLTypeColumnName) {
			hasDMLType = true
		}
		if col.Name.L == strings.ToLower(model.MaterializedViewLogOldNewColumnName) {
			hasOldNew = true
			require.Equal(t, mysql.TypeTiny, col.FieldType.GetType())
		}
	}
	require.True(t, hasDMLType)
	require.True(t, hasOldNew)

	tk.MustGetErrMsg("create materialized view log on t (a)", "[schema:1050]Table 'test.$mlog$t' already exists")
}

func TestCreateMaterializedViewLogPreservesTextColumnTypes(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t_text_types (id bigint not null primary key, c_tiny tinytext, c_text text, c_medium mediumtext, c_long longtext)")
	tk.MustExec("create materialized view log on t_text_types (id, c_tiny, c_text, c_medium, c_long)")

	showCreate := tk.MustQuery("show create table `$mlog$t_text_types`").Rows()[0][1].(string)
	require.Contains(t, showCreate, "  `c_tiny` tinytext DEFAULT NULL")
	require.Contains(t, showCreate, "  `c_text` text DEFAULT NULL")
	require.Contains(t, showCreate, "  `c_medium` mediumtext DEFAULT NULL")
	require.Contains(t, showCreate, "  `c_long` longtext DEFAULT NULL")
}

func TestCreateMaterializedViewLogPreSplitOptions(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	originSplit := atomic.LoadUint32(&ddl.EnableSplitTableRegion)
	atomic.StoreUint32(&ddl.EnableSplitTableRegion, 1)
	defer atomic.StoreUint32(&ddl.EnableSplitTableRegion, originSplit)
	tk.MustExec("set @@session.tidb_scatter_region='table'")
	tk.MustExec("create table t_mlog_presplit (a int, b int)")

	tk.MustExec("create materialized view log on t_mlog_presplit (a) shard_row_id_bits = 2 pre_split_regions = 2 purge next date_add(now(), interval 1 hour)")

	showCreate := tk.MustQuery("show create table `$mlog$t_mlog_presplit`").Rows()[0][1].(string)
	require.Contains(t, showCreate, "SHARD_ROW_ID_BITS=2")
	require.Contains(t, showCreate, "PRE_SPLIT_REGIONS=2")

	is := dom.InfoSchema()
	mlogTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("$mlog$t_mlog_presplit"))
	require.NoError(t, err)
	require.Equal(t, uint64(2), mlogTable.Meta().ShardRowIDBits)
	require.Equal(t, uint64(2), mlogTable.Meta().PreSplitRegions)

	regions := tk.MustQuery("show table `$mlog$t_mlog_presplit` regions").Rows()
	regionNames := make([]string, 0, len(regions))
	for _, row := range regions {
		regionNames = append(regionNames, fmt.Sprint(row[1]))
	}
	require.Contains(t, regionNames, fmt.Sprintf("t_%d_r_2305843009213693952", mlogTable.Meta().ID))
	require.Contains(t, regionNames, fmt.Sprintf("t_%d_r_4611686018427387904", mlogTable.Meta().ID))
	require.Contains(t, regionNames, fmt.Sprintf("t_%d_r_6917529027641081856", mlogTable.Meta().ID))
}

func TestCreateMaterializedViewLogPurgeExprTypeValidation(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, b int)")

	err := tk.ExecToErr("create materialized view log on t (a) purge immediate")
	require.Truef(t, dbterror.ErrGeneralUnsupportedDDL.Equal(err), "err %v", err)
	require.ErrorContains(t, err, "PURGE IMMEDIATE is not supported for CREATE MATERIALIZED VIEW LOG")

	err = tk.ExecToErr("create materialized view log on t (a) purge start with 1 next date_add(now(), interval 1 hour)")
	require.ErrorContains(t, err, "PURGE START WITH expression must return DATETIME/TIMESTAMP")

	err = tk.ExecToErr("create materialized view log on t (a) purge next 600")
	require.ErrorContains(t, err, "PURGE NEXT expression must return DATETIME/TIMESTAMP")

	tk.MustExec("create materialized view log on t (a) purge start with now() next date_add(now(), interval 1 hour)")
}

func TestCreateMaterializedViewLogAccumulationAlert(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_alert_default (a int)")
	tk.MustExec("create table t_alert_zero (a int)")
	tk.MustExec("create table t_alert_custom (a int)")
	tk.MustExec("create table t_alert_negative (a int)")

	err := tk.ExecToErr("create materialized view log on t_alert_negative (a) alert rows -1")
	require.ErrorContains(t, err, "invalid ALERT ROWS value: -1 (must be non-negative)")

	tk.MustExec("create materialized view log on t_alert_default (a)")
	tk.MustExec("create materialized view log on t_alert_zero (a) alert rows 0")
	tk.MustExec("create materialized view log on t_alert_custom (a) alert rows 2048")

	getMLogInfo := func(baseTable string) *model.MaterializedViewLogInfo {
		is := dom.InfoSchema()
		mlogTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("$mlog$"+baseTable))
		require.NoError(t, err)
		require.NotNil(t, mlogTable.Meta().MaterializedViewLog)
		return mlogTable.Meta().MaterializedViewLog
	}

	defaultInfo := getMLogInfo("t_alert_default")
	require.Nil(t, defaultInfo.LogAccumulationAlertRows)
	defaultRows, defaultEnabled := defaultInfo.EffectiveLogAccumulationAlertRows()
	require.False(t, defaultEnabled)
	require.Equal(t, uint64(0), defaultRows)

	zeroInfo := getMLogInfo("t_alert_zero")
	require.NotNil(t, zeroInfo.LogAccumulationAlertRows)
	require.Equal(t, uint64(0), *zeroInfo.LogAccumulationAlertRows)
	zeroRows, zeroEnabled := zeroInfo.EffectiveLogAccumulationAlertRows()
	require.False(t, zeroEnabled)
	require.Equal(t, uint64(0), zeroRows)

	customInfo := getMLogInfo("t_alert_custom")
	require.NotNil(t, customInfo.LogAccumulationAlertRows)
	require.Equal(t, uint64(2048), *customInfo.LogAccumulationAlertRows)
	customRows, customEnabled := customInfo.EffectiveLogAccumulationAlertRows()
	require.True(t, customEnabled)
	require.Equal(t, uint64(2048), customRows)
}

func TestCreateMaterializedViewLogPurgeInfoNextUnixSecondsDerivation(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")

	getMLogID := func(baseTable string) int64 {
		is := dom.InfoSchema()
		mlogTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("$mlog$"+baseTable))
		require.NoError(t, err)
		return mlogTable.Meta().ID
	}

	tk.MustExec("create table t_purge_start_only (a int)")
	tk.MustExec("create materialized view log on t_purge_start_only (a) purge start with date_add(now(), interval 40 minute) next date_add(now(), interval 20 minute)")
	mlogStartOnlyID := getMLogID("t_purge_start_only")
	tk.MustQuery(fmt.Sprintf(
		"select NEXT_PURGE_UNIX_SECONDS is not null, NEXT_PURGE_UNIX_SECONDS > TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP() + interval 30 minute), NEXT_PURGE_UNIX_SECONDS < TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP() + interval 2 hour) from mysql.tidb_mlog_purge_info where MLOG_ID = %d",
		mlogStartOnlyID,
	)).Check(testkit.Rows("1 1 1"))

	tk.MustExec("create table t_purge_next_only (a int)")
	tk.MustExec("create materialized view log on t_purge_next_only (a) purge next date_add(now(), interval 20 minute)")
	mlogNextOnlyID := getMLogID("t_purge_next_only")
	tk.MustQuery(fmt.Sprintf(
		"select NEXT_PURGE_UNIX_SECONDS is not null, NEXT_PURGE_UNIX_SECONDS > TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP() + interval 10 minute), NEXT_PURGE_UNIX_SECONDS < TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP() + interval 1 hour) from mysql.tidb_mlog_purge_info where MLOG_ID = %d",
		mlogNextOnlyID,
	)).Check(testkit.Rows("1 1 1"))

	tk.MustExec("create table t_purge_no_schedule (a int)")
	tk.MustExec("create materialized view log on t_purge_no_schedule (a)")
	mlogNoScheduleID := getMLogID("t_purge_no_schedule")
	tk.MustQuery(fmt.Sprintf(
		"select NEXT_PURGE_UNIX_SECONDS is null from mysql.tidb_mlog_purge_info where MLOG_ID = %d",
		mlogNoScheduleID,
	)).Check(testkit.Rows("1"))

	tk.MustExec("create table t_purge_near_now (a int)")
	tk.MustExec("create materialized view log on t_purge_near_now (a) purge start with now() next date_add(now(), interval 40 minute)")
	mlogNearNowID := getMLogID("t_purge_near_now")
	tk.MustQuery(fmt.Sprintf(
		"select NEXT_PURGE_UNIX_SECONDS is not null, NEXT_PURGE_UNIX_SECONDS > TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP() + interval 20 minute), NEXT_PURGE_UNIX_SECONDS < TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP() + interval 2 hour) from mysql.tidb_mlog_purge_info where MLOG_ID = %d",
		mlogNearNowID,
	)).Check(testkit.Rows("1 1 1"))
}

func TestCreateMaterializedViewLogPurgeInfoNextUnixSecondsUsesScheduleTimeZone(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set time_zone = '+08:00'")

	getMLogID := func(baseTable string) int64 {
		is := dom.InfoSchema()
		mlogTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("$mlog$"+baseTable))
		require.NoError(t, err)
		return mlogTable.Meta().ID
	}

	tk.MustExec("create table t_purge_schedule_next (a int)")
	tk.MustExec("create materialized view log on t_purge_schedule_next (a) purge next cast('2030-01-02 10:00:00' as datetime)")
	mlogNextID := getMLogID("t_purge_schedule_next")
	tk.MustQuery(fmt.Sprintf(
		"select NEXT_PURGE_UNIX_SECONDS = 1893549600, NEXT_PURGE_UNIX_SECONDS = 1893578400 from mysql.tidb_mlog_purge_info where MLOG_ID = %d",
		mlogNextID,
	)).Check(testkit.Rows("1 0"))

	tk.MustExec("create table t_purge_schedule_start (a int)")
	tk.MustExec("create materialized view log on t_purge_schedule_start (a) purge start with cast('2030-01-02 10:00:00' as datetime) next cast('2030-01-03 10:00:00' as datetime)")
	mlogStartID := getMLogID("t_purge_schedule_start")
	tk.MustQuery(fmt.Sprintf(
		"select NEXT_PURGE_UNIX_SECONDS = 1893549600, NEXT_PURGE_UNIX_SECONDS = 1893636000 from mysql.tidb_mlog_purge_info where MLOG_ID = %d",
		mlogStartID,
	)).Check(testkit.Rows("1 0"))
}

func TestCreateMaterializedViewLogMetaColumnNameConflict(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_conflict (`_MLOG$_DML_TYPE` int, a int)")
	tk.MustGetErrCode("create materialized view log on t_conflict (`_MLOG$_DML_TYPE`, a)", 1060)
}

func TestCreateMaterializedViewLogRejectNonBaseObject(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int)")
	tk.MustExec("create view v as select a from t")
	tk.MustExec("create sequence s")
	tk.MustExec("create global temporary table gt (a int) on commit delete rows")
	tk.MustExec("create materialized view log on t (a)")
	tk.MustExec("create table t_mv_base (a int not null, b int not null)")
	tk.MustExec("create materialized view log on t_mv_base (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv (a, cnt) refresh fast as select a, count(1) from t_mv_base group by a")

	for _, testCase := range []struct {
		sql  string
		name string
	}{
		{"create materialized view log on v (a)", "v"},
		{"create materialized view log on s (a)", "s"},
		{"create materialized view log on gt (a)", "gt"},
		{"create materialized view log on mysql.user (User)", "user"},
		{"create materialized view log on information_schema.tables (TABLE_SCHEMA)", "tables"},
		{"create materialized view log on mv (a, cnt)", "mv"},
	} {
		err := tk.ExecToErr(testCase.sql)
		require.Error(t, err, testCase.sql)
		require.Contains(t, err.Error(), "is not BASE TABLE")
	}
	tk.MustExec("create table t_mlog_base (a int)")
	tk.MustExec("create materialized view log on t_mlog_base (a)")
	err := tk.ExecToErr("create materialized view log on `$mlog$t_mlog_base` (a)")
	require.Error(t, err)
	require.Contains(t, err.Error(), "is not BASE TABLE")
}

func TestCreateMaterializedViewLogRejectUnsupportedColumns(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")

	for _, tableName := range []string{"t_tinyblob", "t_blob", "t_mediumblob", "t_longblob"} {
		tk.MustExec(fmt.Sprintf("create table %s (id bigint not null primary key, b %s null)", tableName, tableName[2:]))
		err := tk.ExecToErr(fmt.Sprintf("create materialized view log on %s (id, b)", tableName))
		require.ErrorContains(t, err, "CREATE MATERIALIZED VIEW LOG does not support BLOB column b")
	}

	tk.MustExec("create table t_text_ok (id bigint not null primary key, c1 tinytext null, c2 text null, c3 mediumtext null, c4 longtext null)")
	tk.MustExec("create materialized view log on t_text_ok (id, c1, c2, c3, c4)")
	tk.MustExec("create table t_json (id bigint not null primary key, j json null)")
	err := tk.ExecToErr("create materialized view log on t_json (id, j)")
	require.ErrorContains(t, err, "CREATE MATERIALIZED VIEW LOG does not support JSON column j")

	tk.MustExec("create table t_gen (id bigint not null primary key, g1 int not null, g_virtual int as (g1 + 1) virtual, g_stored int as (g1 + 2) stored)")
	tk.MustExec("create materialized view log on t_gen (id, g_virtual, g_stored)")
	tk.MustExec("create table t_untracked_unsupported (id bigint not null primary key, b blob null, j json null, g int as (id + 1) stored)")
	tk.MustExec("create materialized view log on t_untracked_unsupported (id)")
}

func TestCreateMaterializedViewLogUpdatesPlacementBundle(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create placement policy mlog_p followers=1")
	tk.MustExec("alter database test placement policy mlog_p")
	tk.MustExec("create table t_placement (a int)")
	tk.MustExec("create materialized view log on t_placement (a)")

	tk.MustQuery("show placement for table `$mlog$t_placement`").CheckContain("TABLE test.$mlog$t_placement")
	tk.MustQuery("show placement for table `$mlog$t_placement`").CheckContain("FOLLOWERS=1")
}

func TestCreateMaterializedViewLogAllowsGeneratedColumns(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE t_gen (id BIGINT NOT NULL PRIMARY KEY, base BIGINT NOT NULL, gv BIGINT AS (base + 1) VIRTUAL, gs BIGINT AS (base + 2) STORED)")
	tk.MustExec("CREATE MATERIALIZED VIEW LOG ON t_gen (id, gv, gs)")

	tk.MustQuery("select column_name from information_schema.columns where table_schema='test' and table_name='$mlog$t_gen' order by ordinal_position").
		Check(testkit.Rows("id", "gv", "gs", "_MLOG$_DML_TYPE", "_MLOG$_OLD_NEW"))

	is := dom.InfoSchema()
	mlogTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("$mlog$t_gen"))
	require.NoError(t, err)
	require.NotNil(t, mlogTable.Meta().MaterializedViewLog)
	require.Equal(t, []ast.CIStr{ast.NewCIStr("id"), ast.NewCIStr("gv"), ast.NewCIStr("gs")}, mlogTable.Meta().MaterializedViewLog.Columns)
}

func TestCreateMaterializedViewLogColumnKeyFlag(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table base_test(id int, v1 int, v2 int, v3 int, v4 int, index k(v1,v2,v3,v4))")
	tk.MustExec("create materialized view log on base_test(v1, v2) purge next date_add(now(), interval 1 hour)")
	tk.MustQuery("select column_key from information_schema.columns where table_schema='test' and table_name='$mlog$base_test' and column_name='v1'").
		Check(testkit.Rows(""))
	tk.MustQuery("select column_key from information_schema.columns where table_schema='test' and table_name='$mlog$base_test' and column_name='v2'").
		Check(testkit.Rows(""))
}

func TestCreateMaterializedViewColumnFlags(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table base_mv_flags(id bigint not null auto_increment primary key, g1 int not null, v1 bigint not null, key idx_g1_id(g1, id))")
	tk.MustExec("create materialized view log on base_mv_flags(id, g1, v1) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_flags (g1, cnt, s_v1, min_id, max_id) as select g1, count(1), sum(v1), min(id), max(id) from base_mv_flags group by g1")
	for _, col := range []string{"min_id", "max_id"} {
		tk.MustQuery(fmt.Sprintf("select column_key from information_schema.columns where table_schema='test' and table_name='mv_flags' and column_name='%s'", col)).Check(testkit.Rows(""))
		tk.MustQuery(fmt.Sprintf("select extra from information_schema.columns where table_schema='test' and table_name='mv_flags' and column_name='%s'", col)).Check(testkit.Rows(""))
	}
}

func TestMaterializedViewCommentLength(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_mv_comment_len (id bigint not null primary key, g1 int not null, v1 bigint not null, key idx_g1(g1))")
	tk.MustExec("create materialized view log on t_mv_comment_len (id, g1, v1)")

	maxTableCommentLength := ddl.MaxCommentLength * 2
	commentMaxLen := strings.Repeat("y", maxTableCommentLength)
	commentTooLong := strings.Repeat("y", maxTableCommentLength+1)
	createMVSQL := func(name, comment string) string {
		return fmt.Sprintf("create materialized view %s (g1, cnt) comment = '%s' refresh fast as select g1, count(1) from t_mv_comment_len group by g1", name, comment)
	}
	errTooLongComment := func(name string) string {
		return fmt.Sprintf("Comment for table '%s' is too long (max = %d)", name, maxTableCommentLength)
	}

	tk.MustExec("set @@sql_mode='STRICT_TRANS_TABLES'")
	tk.MustExec(createMVSQL("mv_comment_max", commentMaxLen))
	err := tk.ExecToErr(createMVSQL("mv_comment_too_long", commentTooLong))
	require.ErrorContains(t, err, errTooLongComment("mv_comment_too_long"))

	tk.MustExec("set @@sql_mode=''")
	tk.MustExec(createMVSQL("mv_comment_truncated", commentTooLong))
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1628|"+errTooLongComment("mv_comment_truncated")))
	tk.MustQuery("select length(table_comment) from information_schema.tables where table_schema = 'test' and table_name = 'mv_comment_truncated'").Check(testkit.Rows(strconv.Itoa(maxTableCommentLength)))
}

func TestCreateMaterializedViewRefreshExprTypeValidation(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")

	err := tk.ExecToErr("create materialized view mv_bad_next (a, s, cnt) refresh fast next 300 as select a, sum(b), count(1) from t group by a")
	require.ErrorContains(t, err, "REFRESH NEXT expression must return DATETIME/TIMESTAMP")
	err = tk.ExecToErr("create materialized view mv_bad_start (a, s, cnt) refresh fast start with 1 next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")
	require.ErrorContains(t, err, "REFRESH START WITH expression must return DATETIME/TIMESTAMP")
	tk.MustExec("create materialized view mv_ok (a, s, cnt) refresh fast start with now() next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")
}

func TestCreateMaterializedViewRejectsUnsupportedSelectClauses(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("create materialized view log on t (a, b)")

	tests := []struct {
		name    string
		sql     string
		errPart string
	}{
		{
			name:    "cte",
			sql:     "create materialized view mv_cte (a, s, cnt) as with cte as (select a from t) select a, sum(b), count(1) from t group by a",
			errPart: "common table expressions",
		},
		{
			name:    "locking clause",
			sql:     "create materialized view mv_lock (a, s, cnt) as select a, sum(b), count(1) from t group by a for update",
			errPart: "locking clauses",
		},
		{
			name:    "select into",
			sql:     "create materialized view mv_into (a, s, cnt) as select a, sum(b), count(1) from t group by a into outfile '/tmp/mv.out'",
			errPart: "SELECT INTO",
		},
		{
			name:    "as of",
			sql:     "create materialized view mv_as_of (a, s, cnt) as select a, sum(b), count(1) from t as of timestamp now() group by a",
			errPart: "AS OF",
		},
		{
			name:    "table sample",
			sql:     "create materialized view mv_sample (a, s, cnt) as select a, sum(b), count(1) from t tablesample system (50) group by a",
			errPart: "TABLESAMPLE",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tk.ExecToErr(tt.sql)
			require.ErrorContains(t, err, tt.errPart)
		})
	}
}

func TestCreateTableLikeShouldNotCarryMaterializedViewMetadata(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_src (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")
	tk.MustExec("create table t_like like t")
	tk.MustExec("create table mv_like like mv_src")
	tk.MustExec("create table mlog_like like `$mlog$t`")

	is := dom.InfoSchema()
	baseTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	mvSrc, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_src"))
	require.NoError(t, err)
	mvLike, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_like"))
	require.NoError(t, err)
	mlogSrc, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("$mlog$t"))
	require.NoError(t, err)
	mlogLike, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mlog_like"))
	require.NoError(t, err)

	require.NotNil(t, mvSrc.Meta().MaterializedView)
	require.NotNil(t, mlogSrc.Meta().MaterializedViewLog)
	for _, tbl := range []*model.TableInfo{mvLike.Meta(), mlogLike.Meta()} {
		require.Nil(t, tbl.MaterializedView)
		require.Nil(t, tbl.MaterializedViewLog)
		require.Nil(t, tbl.MaterializedViewBase)
	}
	require.NotNil(t, baseTable.Meta().MaterializedViewBase)
	require.Equal(t, mlogSrc.Meta().ID, baseTable.Meta().MaterializedViewBase.MLogID)
	require.Equal(t, []int64{mvSrc.Meta().ID}, baseTable.Meta().MaterializedViewBase.MViewIDs)
}

func TestCreateMaterializedViewRefreshInfoNextUnixSecondsDerivation(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")

	getMViewID := func(name string) int64 {
		is := dom.InfoSchema()
		mvTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr(name))
		require.NoError(t, err)
		return mvTable.Meta().ID
	}

	tk.MustExec("create materialized view mv_start_only (a, s, cnt) refresh fast start with date_add(now(), interval 40 minute) next date_add(now(), interval 20 minute) as select a, sum(b), count(1) from t group by a")
	mvStartOnlyID := getMViewID("mv_start_only")
	tk.MustQuery(fmt.Sprintf(
		"select NEXT_REFRESH_UNIX_SECONDS is not null, NEXT_REFRESH_UNIX_SECONDS > TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP() + interval 30 minute), NEXT_REFRESH_UNIX_SECONDS < TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP() + interval 2 hour) from mysql.tidb_mview_refresh_info where MVIEW_ID = %d",
		mvStartOnlyID,
	)).Check(testkit.Rows("1 1 1"))

	tk.MustExec("create materialized view mv_next_only (a, s, cnt) refresh fast next date_add(now(), interval 20 minute) as select a, sum(b), count(1) from t group by a")
	mvNextOnlyID := getMViewID("mv_next_only")
	tk.MustQuery(fmt.Sprintf(
		"select NEXT_REFRESH_UNIX_SECONDS is not null, NEXT_REFRESH_UNIX_SECONDS > TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP() + interval 10 minute), NEXT_REFRESH_UNIX_SECONDS < TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP() + interval 1 hour) from mysql.tidb_mview_refresh_info where MVIEW_ID = %d",
		mvNextOnlyID,
	)).Check(testkit.Rows("1 1 1"))

	tk.MustExec("create materialized view mv_no_schedule (a, s, cnt) refresh fast as select a, sum(b), count(1) from t group by a")
	mvNoScheduleID := getMViewID("mv_no_schedule")
	tk.MustQuery(fmt.Sprintf("select NEXT_REFRESH_UNIX_SECONDS is null from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", mvNoScheduleID)).Check(testkit.Rows("1"))

	tk.MustExec("create materialized view mv_near_now (a, s, cnt) refresh fast start with now() next date_add(now(), interval 40 minute) as select a, sum(b), count(1) from t group by a")
	mvNearNowID := getMViewID("mv_near_now")
	tk.MustQuery(fmt.Sprintf(
		"select NEXT_REFRESH_UNIX_SECONDS is not null, NEXT_REFRESH_UNIX_SECONDS > TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP() + interval 20 minute), NEXT_REFRESH_UNIX_SECONDS < TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP() + interval 2 hour) from mysql.tidb_mview_refresh_info where MVIEW_ID = %d",
		mvNearNowID,
	)).Check(testkit.Rows("1 1 1"))
}

func TestCreateMaterializedViewRefreshInfoNextUnixSecondsUsesScheduleTimeZone(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set time_zone = '+08:00'")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")

	getMViewID := func(name string) int64 {
		is := dom.InfoSchema()
		mvTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr(name))
		require.NoError(t, err)
		return mvTable.Meta().ID
	}

	tk.MustExec("create materialized view mv_schedule_next (a, s, cnt) refresh fast next cast('2030-01-02 10:00:00' as datetime) as select a, sum(b), count(1) from t group by a")
	mvID := getMViewID("mv_schedule_next")
	tk.MustQuery(fmt.Sprintf(
		"select NEXT_REFRESH_UNIX_SECONDS = 1893549600, NEXT_REFRESH_UNIX_SECONDS = 1893578400 from mysql.tidb_mview_refresh_info where MVIEW_ID = %d",
		mvID,
	)).Check(testkit.Rows("1 0"))

	tk.MustExec("create materialized view mv_schedule_start (a, s, cnt) refresh fast start with cast('2030-01-02 10:00:00' as datetime) next cast('2030-01-03 10:00:00' as datetime) as select a, sum(b), count(1) from t group by a")
	mvStartID := getMViewID("mv_schedule_start")
	tk.MustQuery(fmt.Sprintf(
		"select NEXT_REFRESH_UNIX_SECONDS = 1893549600, NEXT_REFRESH_UNIX_SECONDS = 1893636000 from mysql.tidb_mview_refresh_info where MVIEW_ID = %d",
		mvStartID,
	)).Check(testkit.Rows("1 0"))
}

func TestCreateMaterializedViewRejectNonBaseObject(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int)")
	tk.MustExec("create materialized view log on t (a)")
	tk.MustExec("create view v as select a from t")

	err := tk.ExecToErr("create materialized view mv_v (a, c) as select a, count(1) from v group by a")
	require.ErrorContains(t, err, "is not BASE TABLE")
}

func TestCreateMaterializedViewBuildFailureRollback(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockCreateMaterializedViewBuildErr", "return"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockCreateMaterializedViewBuildErr"))
	}()

	err := tk.ExecToErr("create materialized view mv_fail (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")
	require.Error(t, err)
	tk.MustQuery("select count(*) from information_schema.tables where table_schema = 'test' and table_name = 'mv_fail'").Check(testkit.Rows("0"))
	tk.MustQuery("select count(*) from mysql.tidb_mview_refresh_info").Check(testkit.Rows("0"))

	is := dom.InfoSchema()
	baseTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	require.NotNil(t, baseTable.Meta().MaterializedViewBase)
	require.NotZero(t, baseTable.Meta().MaterializedViewBase.MLogID)
	require.Empty(t, baseTable.Meta().MaterializedViewBase.MViewIDs)
}

func TestCreateMaterializedViewBuildContextCanceledRollback(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockCreateMaterializedViewBuildErr", `return("context-canceled")`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockCreateMaterializedViewBuildErr"))
	}()

	err := tk.ExecToErr("create materialized view mv_ctx_cancel (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")
	require.Error(t, err)
	tk.MustQuery("show tables like 'mv_ctx_cancel'").Check(testkit.Rows())
	tk.MustQuery("select count(*) from mysql.tidb_mview_refresh_info").Check(testkit.Rows("0"))

	is := dom.InfoSchema()
	baseTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	require.NotNil(t, baseTable.Meta().MaterializedViewBase)
	require.NotZero(t, baseTable.Meta().MaterializedViewBase.MLogID)
	require.Empty(t, baseTable.Meta().MaterializedViewBase.MViewIDs)
}

func TestCreateMaterializedViewRollbackIgnoreMissingRefreshInfoTable(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockCreateMaterializedViewBuildErr", "return"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockDeleteCreateMaterializedViewRefreshInfoTableNotExists", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockDeleteCreateMaterializedViewRefreshInfoTableNotExists"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockCreateMaterializedViewBuildErr"))
	}()

	err := tk.ExecToErr("create materialized view mv_missing_refresh_meta (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")
	require.Error(t, err)
	tk.MustQuery("show tables like 'mv_missing_refresh_meta'").Check(testkit.Rows())
	tk.MustQuery("select count(*) from mysql.tidb_mview_refresh_info").Check(testkit.Rows("0"))

	is := dom.InfoSchema()
	baseTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	require.NotNil(t, baseTable.Meta().MaterializedViewBase)
	require.NotZero(t, baseTable.Meta().MaterializedViewBase.MLogID)
	require.Empty(t, baseTable.Meta().MaterializedViewBase.MViewIDs)
}

func TestCreateMaterializedViewRefreshInfoUpsertFailureRollback(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockUpsertCreateMaterializedViewRefreshInfoTableNotExists", "1*return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockUpsertCreateMaterializedViewRefreshInfoTableNotExists"))
	}()

	err := tk.ExecToErr("create materialized view mv_upsert_fail (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")
	require.Error(t, err)
	require.ErrorContains(t, err, "tidb_mview_refresh_info")
	require.NotContains(t, err.Error(), "Information schema is changed")
	require.NotContains(t, err.Error(), "Duplicate entry")
	tk.MustQuery("show tables like 'mv_upsert_fail'").Check(testkit.Rows())
	tk.MustQuery("select count(*) from mysql.tidb_mview_refresh_info").Check(testkit.Rows("0"))
	rows := tk.MustQuery("admin show ddl jobs where JOB_TYPE='create materialized view'").Rows()
	require.NotEmpty(t, rows)
	jobID := fmt.Sprint(rows[0][0])
	tk.MustQuery("select ((select count(*) from mysql.gc_delete_range where job_id=" + jobID + ") + (select count(*) from mysql.gc_delete_range_done where job_id=" + jobID + ")) > 0").Check(testkit.Rows("1"))

	is := dom.InfoSchema()
	baseTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	require.NotNil(t, baseTable.Meta().MaterializedViewBase)
	require.NotZero(t, baseTable.Meta().MaterializedViewBase.MLogID)
	require.Empty(t, baseTable.Meta().MaterializedViewBase.MViewIDs)
}

func TestCreateMaterializedViewLogPurgeInfoFailureRollback(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")

	const failpointName = "github.com/pingcap/tidb/pkg/ddl/mockInsertMLogPurgeTableNotExists"
	require.NoError(t, failpoint.Enable(failpointName, "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable(failpointName))
	}()

	err := tk.ExecToErr("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	require.ErrorContains(t, err, "tidb_mlog_purge_info")
	tk.MustQuery("show tables like '$mlog$t'").Check(testkit.Rows())
	tk.MustQuery("select count(*) from mysql.tidb_mlog_purge_info").Check(testkit.Rows("0"))

	is := dom.InfoSchema()
	baseTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	require.Nil(t, baseTable.Meta().MaterializedViewBase)
}

func TestCreateMaterializedViewRetryWithResidualBuildRowsRollback(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (null, 7), (null, 3)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockCreateMaterializedViewPostBuildRetryableErr", "1*return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockCreateMaterializedViewPostBuildRetryableErr"))
	}()

	err := tk.ExecToErr("create materialized view mv_retry_residual (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")
	require.Error(t, err)
	require.ErrorContains(t, err, "detected residual build rows on retry")
	require.NotContains(t, err.Error(), "Duplicate entry")
	tk.MustQuery("show tables like 'mv_retry_residual'").Check(testkit.Rows())
	tk.MustQuery("select count(*) from mysql.tidb_mview_refresh_info").Check(testkit.Rows("0"))

	is := dom.InfoSchema()
	baseTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	require.NotNil(t, baseTable.Meta().MaterializedViewBase)
	require.NotZero(t, baseTable.Meta().MaterializedViewBase.MLogID)
	require.Empty(t, baseTable.Meta().MaterializedViewBase.MViewIDs)
}

func TestCreateMaterializedViewRetryAfterUpsertFailure(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockUpsertCreateMaterializedViewRefreshInfoTableNotExists", "1*return(true)"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockDeleteCreateMaterializedViewRefreshAlertErr", `return("mock rollback alert delete error")`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockUpsertCreateMaterializedViewRefreshInfoTableNotExists"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockDeleteCreateMaterializedViewRefreshAlertErr"))
	}()

	err := tk.ExecToErr("create materialized view mv_retry (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")
	require.Error(t, err)
	require.ErrorContains(t, err, "tidb_mview_refresh_info")
	require.NotContains(t, err.Error(), "Information schema is changed")
	require.NotContains(t, err.Error(), "mock rollback alert delete error")
	tk.MustQuery("show tables like 'mv_retry'").Check(testkit.Rows())

	tk.MustExec("create materialized view mv_retry (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")
	tk.MustQuery("select a, s, cnt from mv_retry order by a").Check(testkit.Rows("1 15 2", "2 7 1"))
}

func TestCreateMaterializedViewLogPrivilege(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_create_mlog_priv (a int)")
	users := []string{"u_create_mlog_no_create", "u_create_mlog_no_select", "u_create_mlog_table_create", "u_create_mlog_ok"}
	t.Cleanup(func() {
		for _, user := range users {
			tk.MustExec("drop user '" + user + "'@'%'")
		}
	})
	for _, user := range users {
		tk.MustExec("create user '" + user + "'@'%'")
	}

	tk.MustExec("grant select on test.t_create_mlog_priv to 'u_create_mlog_no_create'@'%'")
	tkNoCreate := newMViewTestKit(t, store)
	require.NoError(t, tkNoCreate.Session().Auth(&auth.UserIdentity{Username: "u_create_mlog_no_create", Hostname: "%"}, nil, nil, nil))
	err := tkNoCreate.ExecToErr("create materialized view log on test.t_create_mlog_priv (a)")
	require.ErrorContains(t, err, "CREATE MATERIALIZED VIEW LOG command denied")
	require.ErrorContains(t, err, "t_create_mlog_priv")
	require.NotContains(t, err.Error(), "$mlog$")

	tk.MustExec("grant create view on test.* to 'u_create_mlog_no_select'@'%'")
	tkNoSelect := newMViewTestKit(t, store)
	require.NoError(t, tkNoSelect.Session().Auth(&auth.UserIdentity{Username: "u_create_mlog_no_select", Hostname: "%"}, nil, nil, nil))
	err = tkNoSelect.ExecToErr("create materialized view log on test.t_create_mlog_priv (a)")
	require.ErrorContains(t, err, "SELECT command denied")

	tk.MustExec("grant create view on test.* to 'u_create_mlog_ok'@'%'")
	tk.MustExec("grant select on test.t_create_mlog_priv to 'u_create_mlog_ok'@'%'")
	tkOK := newMViewTestKit(t, store)
	require.NoError(t, tkOK.Session().Auth(&auth.UserIdentity{Username: "u_create_mlog_ok", Hostname: "%"}, nil, nil, nil))
	tkOK.MustExec("create materialized view log on test.t_create_mlog_priv (a)")

	tk.MustExec("grant create view on test.t_create_mlog_priv to 'u_create_mlog_table_create'@'%'")
	tk.MustExec("grant select on test.t_create_mlog_priv to 'u_create_mlog_table_create'@'%'")
	tkTableCreate := newMViewTestKit(t, store)
	require.NoError(t, tkTableCreate.Session().Auth(&auth.UserIdentity{Username: "u_create_mlog_table_create", Hostname: "%"}, nil, nil, nil))
	err = tkTableCreate.ExecToErr("create materialized view log on test.t_create_mlog_priv (a)")
	require.ErrorContains(t, err, "CREATE MATERIALIZED VIEW LOG command denied")
	require.ErrorContains(t, err, "t_create_mlog_priv")
	require.NotContains(t, err.Error(), "$mlog$")
}

func TestCreateMaterializedViewHistoryJobSchemaVersion(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_hist_schema_ver (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")

	rows := tk.MustQuery("admin show ddl jobs where JOB_TYPE='create materialized view'").Rows()
	require.NotEmpty(t, rows)
	jobID, err := strconv.ParseInt(fmt.Sprint(rows[0][0]), 10, 64)
	require.NoError(t, err)
	historyJob, err := ddl.GetHistoryJobByID(tk.Session(), jobID)
	require.NoError(t, err)
	require.Greater(t, historyJob.BinlogInfo.SchemaVersion, int64(0))
	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_hist_schema_ver"))
	require.NoError(t, err)
	require.NotNil(t, historyJob.BinlogInfo.TableInfo)
	require.Equal(t, mvTable.Meta().ID, historyJob.BinlogInfo.TableInfo.ID)
	require.Equal(t, mvTable.Meta().Name, historyJob.BinlogInfo.TableInfo.Name)
}

func TestCreateMaterializedViewCancelRollback(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tkCancel := newMViewTestKit(t, store)
	tkCancel.MustExec("use test")

	const pauseBuildFailpoint = "github.com/pingcap/tidb/pkg/ddl/pauseCreateMaterializedViewBuild"
	require.NoError(t, failpoint.Enable(pauseBuildFailpoint, "pause"))
	enabled := true
	defer func() {
		if enabled {
			require.NoError(t, failpoint.Disable(pauseBuildFailpoint))
		}
	}()

	ddlDone := make(chan error, 1)
	go func() {
		tkDDL := newMViewTestKit(t, store)
		tkDDL.MustExec("use test")
		ddlDone <- tkDDL.ExecToErr("create materialized view mv_cancel (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")
	}()

	jobID := ""
	require.Eventually(t, func() bool {
		rows := tkCancel.MustQuery("admin show ddl jobs where JOB_TYPE='create materialized view'").Rows()
		if len(rows) == 0 || len(rows[0]) < 5 || strings.ToLower(fmt.Sprint(rows[0][4])) != "write reorganization" {
			return false
		}
		jobID = fmt.Sprint(rows[0][0])
		return jobID != ""
	}, 30*time.Second, 100*time.Millisecond)

	tkCancel.MustExec("admin cancel ddl jobs " + jobID)
	require.NoError(t, failpoint.Disable(pauseBuildFailpoint))
	enabled = false

	err := <-ddlDone
	require.ErrorContains(t, err, "Cancelled DDL job")
	rows := tkCancel.MustQuery("admin show ddl jobs where JOB_ID=" + jobID).Rows()
	require.Equal(t, "rollback done", rows[0][len(rows[0])-2])
	tk.MustQuery("show tables like 'mv_cancel'").Check(testkit.Rows())
	tk.MustQuery("select count(*) from mysql.tidb_mview_refresh_info").Check(testkit.Rows("0"))

	is := dom.InfoSchema()
	baseTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	require.NotNil(t, baseTable.Meta().MaterializedViewBase)
	require.NotZero(t, baseTable.Meta().MaterializedViewBase.MLogID)
	require.Empty(t, baseTable.Meta().MaterializedViewBase.MViewIDs)
}

func TestCreateMaterializedViewRefreshInfoRunningAndSuccess(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")

	const pauseBuildFailpoint = "github.com/pingcap/tidb/pkg/ddl/pauseCreateMaterializedViewBuild"
	require.NoError(t, failpoint.Enable(pauseBuildFailpoint, "pause"))
	enabled := true
	defer func() {
		if enabled {
			require.NoError(t, failpoint.Disable(pauseBuildFailpoint))
		}
	}()

	ddlDone := make(chan error, 1)
	go func() {
		tkDDL := newMViewTestKit(t, store)
		tkDDL.MustExec("use test")
		ddlDone <- tkDDL.ExecToErr("create materialized view mv_state (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")
	}()

	var initTS uint64
	var mviewID int64
	require.Eventually(t, func() bool {
		rows := tk.MustQuery("select MVIEW_ID, LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info").Rows()
		if len(rows) != 1 {
			return false
		}
		id, err := strconv.ParseInt(fmt.Sprint(rows[0][0]), 10, 64)
		if err != nil || id == 0 {
			return false
		}
		ts, err := strconv.ParseUint(fmt.Sprint(rows[0][1]), 10, 64)
		if err != nil || ts == 0 {
			return false
		}
		mviewID, initTS = id, ts
		return true
	}, 30*time.Second, 100*time.Millisecond)

	require.NoError(t, failpoint.Disable(pauseBuildFailpoint))
	enabled = false
	require.NoError(t, <-ddlDone)
	tk.MustQuery("select a, s, cnt from mv_state order by a").Check(testkit.Rows("1 15 2", "2 7 1"))

	rows := tk.MustQuery(fmt.Sprintf("select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", mviewID)).Rows()
	finalTS, err := strconv.ParseUint(fmt.Sprint(rows[0][0]), 10, 64)
	require.NoError(t, err)
	require.Greater(t, finalTS, initTS)
}

func TestCreateMaterializedViewBuildReadTSQueryTypeAlignment(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int)")
	tk.MustExec("insert into t values (1)")

	ddlSe := ddlsess.NewSession(tk.Session())
	tk.MustExec("select * from t")
	expected := tk.Session().GetSessionVars().LastQueryInfo.StartTS
	require.NotZero(t, expected)

	rows, err := ddlSe.Execute(context.Background(),
		"SELECT COALESCE(CAST(JSON_UNQUOTE(JSON_EXTRACT(@@tidb_last_query_info, '$.start_ts')) AS UNSIGNED), CAST(0 AS UNSIGNED))",
		"create-materialized-view-build-read-ts-ut",
	)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, expected, rows[0].GetUint64(0))
}

func TestCreateMaterializedViewLogRejectsDuplicateColumns(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_dup (id bigint not null primary key, g1 int not null)")

	err := tk.ExecToErr("create materialized view log on t_dup (id, id)")
	require.ErrorContains(t, err, "Duplicate column name")
}

func TestCreateMaterializedViewLogNameLengthByRune(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")

	maxBaseNameLen := mysql.MaxTableNameLength - len([]rune(model.MaterializedViewLogTableNamePrefix))
	maxName := strings.Repeat("表", maxBaseNameLen)
	maxMLogName := model.MaterializedViewLogTableName(ast.NewCIStr(maxName)).O
	require.Equal(t, mysql.MaxTableNameLength, len([]rune(maxMLogName)))
	tk.MustExec(fmt.Sprintf("create table `%s` (a int)", maxName))
	tk.MustExec(fmt.Sprintf("create materialized view log on `%s` (a)", maxName))
	tk.MustQuery(fmt.Sprintf("select count(*) from information_schema.tables where table_schema='test' and table_name='%s'", maxMLogName)).Check(testkit.Rows("1"))

	tooLongName := strings.Repeat("表", maxBaseNameLen+1)
	require.Equal(t, maxMLogName, model.MaterializedViewLogTableName(ast.NewCIStr(tooLongName)).O)
	tk.MustExec(fmt.Sprintf("create table `%s` (a int)", tooLongName))
	err := tk.ExecToErr(fmt.Sprintf("create materialized view log on `%s` (a)", tooLongName))
	require.ErrorContains(t, err, "already exists")
}

func TestCreateMaterializedViewSuccessRefreshInfoVisibilityBeforeCommit(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")

	const afterUpsertFailpoint = "github.com/pingcap/tidb/pkg/ddl/afterCreateMaterializedViewSuccessRefreshInfoUpsert"
	const postUpsertRetryableErr = "github.com/pingcap/tidb/pkg/ddl/mockCreateMaterializedViewPostBuildAfterRefreshInfoUpsertRetryableErr"

	paused := make(chan struct{})
	resume := make(chan struct{})
	var pausedOnce sync.Once
	var resumeOnce sync.Once
	release := func() { resumeOnce.Do(func() { close(resume) }) }
	testfailpoint.EnableCall(t, afterUpsertFailpoint, func() {
		pausedOnce.Do(func() { close(paused) })
		<-resume
	})

	require.NoError(t, failpoint.Enable(postUpsertRetryableErr, "1*return(true)"))
	defer func() {
		release()
		require.NoError(t, failpoint.Disable(postUpsertRetryableErr))
	}()

	ddlDone := make(chan error, 1)
	go func() {
		tkDDL := newMViewTestKit(t, store)
		tkDDL.MustExec("use test")
		ddlDone <- tkDDL.ExecToErr("create materialized view mv_upsert_visibility (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")
	}()

	var prewriteTS uint64
	require.Eventually(t, func() bool {
		rows := tk.MustQuery("select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info").Rows()
		if len(rows) != 1 {
			return false
		}
		ts, err := strconv.ParseUint(fmt.Sprint(rows[0][0]), 10, 64)
		if err != nil || ts == 0 {
			return false
		}
		prewriteTS = ts
		return true
	}, 30*time.Second, 100*time.Millisecond)

	select {
	case <-paused:
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for post-upsert failpoint")
	}

	rows := tk.MustQuery("select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info").Rows()
	require.Len(t, rows, 1)
	visibleTS, err := strconv.ParseUint(fmt.Sprint(rows[0][0]), 10, 64)
	require.NoError(t, err)
	require.Equal(t, prewriteTS, visibleTS)

	release()
	err = <-ddlDone
	require.Error(t, err)
	require.ErrorContains(t, err, "detected residual build rows on retry")
	require.NotContains(t, err.Error(), "Duplicate entry")
	tk.MustQuery("show tables like 'mv_upsert_visibility'").Check(testkit.Rows())
	tk.MustQuery("select count(*) from mysql.tidb_mview_refresh_info").Check(testkit.Rows("0"))

	is := dom.InfoSchema()
	baseTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	require.NotNil(t, baseTable.Meta().MaterializedViewBase)
	require.NotZero(t, baseTable.Meta().MaterializedViewBase.MLogID)
	require.Empty(t, baseTable.Meta().MaterializedViewBase.MViewIDs)
}

func TestCreateMaterializedViewPauseAndResume(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")

	const pauseBuildFailpoint = "github.com/pingcap/tidb/pkg/ddl/pauseCreateMaterializedViewBuild"
	require.NoError(t, failpoint.Enable(pauseBuildFailpoint, "pause"))
	enabled := true
	defer func() {
		if enabled {
			require.NoError(t, failpoint.Disable(pauseBuildFailpoint))
		}
	}()

	ddlDone := make(chan error, 1)
	go func() {
		tkDDL := newMViewTestKit(t, store)
		tkDDL.MustExec("use test")
		ddlDone <- tkDDL.ExecToErr("create materialized view mv_pause (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")
	}()

	tkCtl := newMViewTestKit(t, store)
	tkCtl.MustExec("use test")
	jobID := ""
	require.Eventually(t, func() bool {
		rows := tkCtl.MustQuery("admin show ddl jobs where JOB_TYPE='create materialized view'").Rows()
		if len(rows) == 0 || len(rows[0]) < 5 || strings.ToLower(fmt.Sprint(rows[0][4])) != "write reorganization" {
			return false
		}
		jobID = fmt.Sprint(rows[0][0])
		return jobID != ""
	}, 30*time.Second, 100*time.Millisecond)

	tkCtl.MustExec("admin pause ddl jobs " + jobID)
	require.NoError(t, failpoint.Disable(pauseBuildFailpoint))
	enabled = false

	require.Eventually(t, func() bool {
		rows := tkCtl.MustQuery("admin show ddl jobs where JOB_ID=" + jobID).Rows()
		if len(rows) == 0 {
			return false
		}
		state := strings.ToLower(fmt.Sprint(rows[0][len(rows[0])-2]))
		return state == "paused"
	}, 30*time.Second, 100*time.Millisecond)

	tk.MustQuery("show tables like 'mv_pause'").Check(testkit.Rows("mv_pause"))
	err := tk.ExecToErr("select * from mv_pause")
	require.ErrorContains(t, err, "initial build is in progress")
	for _, sql := range []string{
		"insert into mv_pause values (9, 1, 1)",
		"replace into mv_pause values (9, 1, 1)",
		"load data local infile '/tmp/nonexistent.csv' into table mv_pause",
		"import into mv_pause from '/tmp/nonexistent.csv'",
	} {
		err = tk.ExecToErr(sql)
		require.ErrorContains(t, err, "not updatable", sql)
	}
	tkCtl.MustQuery("admin resume ddl jobs " + jobID).Check(testkit.Rows(jobID + " successful"))
	select {
	case err := <-ddlDone:
		if err != nil {
			require.ErrorContains(t, err, "detected residual build rows on retry")
			tk.MustQuery("show tables like 'mv_pause'").Check(testkit.Rows())
			return
		}
	case <-time.After(60 * time.Second):
		t.Fatal("timed out waiting CREATE MATERIALIZED VIEW to finish after resume")
	}
	tk.MustQuery("select a, s, cnt from mv_pause order by a").Check(testkit.Rows("1 15 2", "2 7 1"))
}
