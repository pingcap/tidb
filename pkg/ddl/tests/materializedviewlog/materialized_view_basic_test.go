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

package materializedviewlog

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/stretchr/testify/require"
)

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
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mlog_purge_info where mlog_id = %d", mlogTable.Meta().ID)).Check(testkit.Rows("1"))

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

func TestCreateMaterializedViewLogMetaColumnNameConflict(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_conflict (`_MLOG$_DML_TYPE` int, a int)")
	tk.MustGetErrCode("create materialized view log on t_conflict (`_MLOG$_DML_TYPE`, a)", 1060)
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

func TestDropMaterializedViewLogBeforeBaseTable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_drop_seq (a int)")
	tk.MustExec("create materialized view log on t_drop_seq (a)")
	tk.MustExec("drop materialized view log on t_drop_seq")
	tk.MustExec("drop table if exists t_drop_seq")
}

func TestAlterMaterializedViewLogPurgeExprTypeValidation(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("create materialized view log on t (a) purge next date_add(now(), interval 1 hour)")

	err := tk.ExecToErr("alter materialized view log on t purge start with 1 next date_add(now(), interval 1 hour)")
	require.ErrorContains(t, err, "PURGE START WITH expression must return DATETIME/TIMESTAMP")

	err = tk.ExecToErr("alter materialized view log on t purge next 300")
	require.ErrorContains(t, err, "PURGE NEXT expression must return DATETIME/TIMESTAMP")

	err = tk.ExecToErr("alter materialized view log on t purge immediate")
	require.ErrorContains(t, err, "PURGE IMMEDIATE is not supported for ALTER MATERIALIZED VIEW LOG")

	tk.MustExec("alter materialized view log on t purge start with now() next date_add(now(), interval 1 hour)")
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

func TestDropMaterializedViewLogRemovesPurgeState(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_drop_mlog_purge_state (a int)")
	tk.MustExec("create materialized view log on t_drop_mlog_purge_state (a)")
	is := dom.InfoSchema()
	mlogTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("$mlog$t_drop_mlog_purge_state"))
	require.NoError(t, err)
	mlogID := mlogTable.Meta().ID
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mlog_purge_info where MLOG_ID = %d", mlogID)).Check(testkit.Rows("1"))
	tk.MustExec("drop materialized view log on t_drop_mlog_purge_state")
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mlog_purge_info where MLOG_ID = %d", mlogID)).Check(testkit.Rows("0"))
}

func TestAlterMaterializedViewLogDDL(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b varchar(10) not null, c int)")
	tk.MustExec("create materialized view log on t (a) purge next date_add(now(), interval 2 hour)")
	tk.MustExec("alter materialized view log on t add column (b, c)")

	getMLog := func() (*model.TableInfo, *model.MaterializedViewLogInfo) {
		is := dom.InfoSchema()
		tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("$mlog$t"))
		require.NoError(t, err)
		require.NotNil(t, tbl.Meta().MaterializedViewLog)
		return tbl.Meta(), tbl.Meta().MaterializedViewLog
	}

	mlogTable, mlogInfo := getMLog()
	require.Equal(t, []ast.CIStr{ast.NewCIStr("a"), ast.NewCIStr("b"), ast.NewCIStr("c")}, mlogInfo.Columns)
	columnNames := make([]string, 0, len(mlogTable.Columns))
	for _, col := range mlogTable.Columns {
		columnNames = append(columnNames, col.Name.O)
	}
	require.Equal(t, []string{"a", "b", "c", "_MLOG$_DML_TYPE", "_MLOG$_OLD_NEW"}, columnNames)

	err := tk.ExecToErr("alter materialized view log on t add column (b)")
	require.ErrorContains(t, err, "Duplicate column name")
	err = tk.ExecToErr("alter materialized view log on t add column (missing_col)")
	require.ErrorContains(t, err, "Unknown column")

	tk.MustExec("alter materialized view log on t purge start with date_add(now(), interval 40 minute) next date_add(now(), interval 20 minute)")
	mlogTable, mlogInfo = getMLog()
	require.Equal(t, "DEFERRED", mlogInfo.PurgeMethod)
	require.Equal(t, "DATE_ADD(NOW(), INTERVAL 40 MINUTE)", mlogInfo.PurgeStartWith)
	require.Equal(t, "DATE_ADD(NOW(), INTERVAL 20 MINUTE)", mlogInfo.PurgeNext)
	tk.MustQuery(fmt.Sprintf("select NEXT_PURGE_UNIX_SECONDS is not null from mysql.tidb_mlog_purge_info where MLOG_ID = %d", mlogTable.ID)).Check(testkit.Rows("1"))

	tk.MustExec("alter materialized view log on t purge")
	_, mlogInfo = getMLog()
	require.Empty(t, mlogInfo.PurgeStartWith)
	require.Empty(t, mlogInfo.PurgeNext)
	tk.MustQuery(fmt.Sprintf("select NEXT_PURGE_UNIX_SECONDS is null from mysql.tidb_mlog_purge_info where MLOG_ID = %d", mlogTable.ID)).Check(testkit.Rows("1"))
}

func TestAlterMaterializedViewLogPurgeScheduleTimeZone(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set time_zone = '+00:00'")
	tk.MustExec("create table t (a int)")
	tk.MustExec("create materialized view log on t (a) purge next cast('2030-01-01 10:00:00' as datetime)")

	getMLog := func() (*model.TableInfo, *model.MaterializedViewLogInfo) {
		is := dom.InfoSchema()
		tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("$mlog$t"))
		require.NoError(t, err)
		require.NotNil(t, tbl.Meta().MaterializedViewLog)
		return tbl.Meta(), tbl.Meta().MaterializedViewLog
	}

	mlogTable, info := getMLog()
	initialTimeZoneName := info.PurgeScheduleTimeZone.Name
	initialTimeZoneOffset := info.PurgeScheduleTimeZone.Offset
	require.Equal(t, 0, initialTimeZoneOffset)

	tk.MustExec("set time_zone = '+08:00'")
	tk.MustExec("alter materialized view log on t purge")
	_, info = getMLog()
	require.Equal(t, initialTimeZoneName, info.PurgeScheduleTimeZone.Name)
	require.Equal(t, initialTimeZoneOffset, info.PurgeScheduleTimeZone.Offset)
	require.Empty(t, info.PurgeNext)

	tk.MustExec("alter materialized view log on t purge next cast('2030-01-02 10:00:00' as datetime)")
	_, info = getMLog()
	require.Equal(t, 8*60*60, info.PurgeScheduleTimeZone.Offset)
	tk.MustQuery("select NEXT_PURGE_UNIX_SECONDS = 1893549600 from mysql.tidb_mlog_purge_info where MLOG_ID = " + strconv.FormatInt(mlogTable.ID, 10)).Check(testkit.Rows("1"))
}

func TestAlterMaterializedViewLogAddColumnRejectsInvalidColumns(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_add_mlog_invalid (a int, b int, c int)")
	tk.MustExec("create materialized view log on t_add_mlog_invalid (a)")

	tk.MustGetErrCode("alter materialized view log on t_add_mlog_invalid add column (a)", errno.ErrDupFieldName)
	tk.MustGetErrCode("alter materialized view log on t_add_mlog_invalid add column (b, b)", errno.ErrDupFieldName)
	tk.MustGetErrCode("alter materialized view log on t_add_mlog_invalid add column (missing_col)", errno.ErrBadField)
	tk.MustGetErrCode("alter materialized view log on t_add_mlog_invalid add column (`_MLOG$_DML_TYPE`)", errno.ErrDupFieldName)
	tk.MustGetErrMsg(
		"alter materialized view log on t_add_mlog_invalid add column (b), add column (c)",
		"[ddl:8200]Unsupported ALTER MATERIALIZED VIEW LOG with multiple ADD COLUMN actions",
	)

	tk.MustExec("create table t_add_mlog_unsupported (id int, b blob, j json, g1 int, g2 int as (g1 + 1) stored)")
	tk.MustExec("create materialized view log on t_add_mlog_unsupported (id)")
	err := tk.ExecToErr("alter materialized view log on t_add_mlog_unsupported add column (b)")
	require.ErrorContains(t, err, "ALTER MATERIALIZED VIEW LOG does not support BLOB column b")
	err = tk.ExecToErr("alter materialized view log on t_add_mlog_unsupported add column (j)")
	require.ErrorContains(t, err, "ALTER MATERIALIZED VIEW LOG does not support JSON column j")
	tk.MustExec("alter materialized view log on t_add_mlog_unsupported add column (g2)")
}

func TestAlterMaterializedViewLogAddColumnCopiesBaseColumnMetadata(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_add_mlog_col (id int not null, n int not null, s varchar(10) not null, d date not null, note text not null)")
	tk.MustExec("create materialized view log on t_add_mlog_col (id)")
	tk.MustExec("alter materialized view log on t_add_mlog_col add column (n, s, d, note)")

	is := dom.InfoSchema()
	mlogTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("$mlog$t_add_mlog_col"))
	require.NoError(t, err)
	require.Equal(t, []ast.CIStr{ast.NewCIStr("id"), ast.NewCIStr("n"), ast.NewCIStr("s"), ast.NewCIStr("d"), ast.NewCIStr("note")}, mlogTable.Meta().MaterializedViewLog.Columns)

	columns := make(map[string]*model.ColumnInfo, len(mlogTable.Meta().Columns))
	for _, column := range mlogTable.Meta().Columns {
		columns[column.Name.L] = column
	}
	for _, name := range []string{"n", "s", "d", "note"} {
		require.True(t, mysql.HasNotNullFlag(columns[name].GetFlag()))
	}
	require.Equal(t, mysql.TypeBlob, columns["note"].GetType())
}
