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
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/stretchr/testify/require"
)

func newMViewTestKit(t testing.TB, store kv.Storage) *testkit.TestKit {
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set tidb_mview_enable = on")
	return tk
}

func TestCreateMaterializedViewAndLog(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set div_precision_increment = 9")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")

	tk.MustExec("set tidb_mview_enable = off")
	err := tk.ExecToErr("create materialized view log on t (a, b)")
	require.ErrorContains(t, err, "tidb_mview_enable")
	tk.MustExec("set tidb_mview_enable = on")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("set tidb_mview_enable = off")
	err = tk.ExecToErr("create materialized view mv_disabled (a, s, cnt) as select a, sum(b), count(1) from t group by a")
	require.ErrorContains(t, err, "tidb_mview_enable")
	tk.MustExec("set tidb_mview_enable = on")
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
	require.Equal(t, []int64{mviewTable.Meta().ID}, mlogTable.Meta().MaterializedViewLog.DependentMViewIDs)
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

func TestDropTableMaterializedViewConstraints(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_drop_constraints (a int not null, b int not null)")
	tk.MustExec("create materialized view log on t_drop_constraints (a, b)")
	tk.MustExec("create materialized view mv_drop_constraints (a, s, cnt) as select a, sum(b), count(1) from t_drop_constraints group by a")
	err := tk.ExecToErr("drop table mv_drop_constraints")
	require.ErrorContains(t, err, "DROP TABLE on materialized view table")
	err = tk.ExecToErr("drop table `$mlog$t_drop_constraints`")
	require.ErrorContains(t, err, "DROP TABLE on materialized view log table")
	err = tk.ExecToErr("drop table t_drop_constraints")
	require.ErrorContains(t, err, "DROP TABLE on base table with materialized view dependencies")

	err = tk.ExecToErr("truncate table mv_drop_constraints")
	require.ErrorContains(t, err, "TRUNCATE TABLE on materialized view table")
	err = tk.ExecToErr("truncate table `$mlog$t_drop_constraints`")
	require.ErrorContains(t, err, "TRUNCATE TABLE on materialized view log table")
	err = tk.ExecToErr("truncate table t_drop_constraints")
	require.ErrorContains(t, err, "TRUNCATE TABLE on base table with materialized view dependencies")
	tk.MustExec("drop materialized view mv_drop_constraints")
	err = tk.ExecToErr("drop table t_drop_constraints")
	require.ErrorContains(t, err, "DROP TABLE on base table with materialized view log")
	err = tk.ExecToErr("truncate table t_drop_constraints")
	require.ErrorContains(t, err, "TRUNCATE TABLE on base table with materialized view log")
	tk.MustExec("drop materialized view log on t_drop_constraints")
	tk.MustExec("truncate table t_drop_constraints")
	tk.MustExec("drop table t_drop_constraints")
}

func TestDropMaterializedViewWhenDisabled(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_drop_when_disabled (a int not null)")
	tk.MustExec("create materialized view log on t_drop_when_disabled (a)")
	tk.MustExec("create materialized view mv_drop_when_disabled (a, cnt) as select a, count(1) from t_drop_when_disabled group by a")

	tk.MustExec("set tidb_mview_enable = off")
	tk.MustExec("drop materialized view mv_drop_when_disabled")
	tk.MustExec("drop materialized view log on t_drop_when_disabled")
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

func TestDropMaterializedViewLogBeforeBaseTable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_drop_seq (a int)")
	tk.MustExec("create materialized view log on t_drop_seq (a)")
	tk.MustExec("drop materialized view log on t_drop_seq")
	tk.MustExec("drop table if exists t_drop_seq")
}

func TestDropMaterializedViewIfExists(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop materialized view if exists missing_mv")
	tk.MustExec("drop materialized view if exists missing_schema.missing_mv")

	err := tk.ExecToErr("drop materialized view log if exists on missing_base")
	require.ErrorContains(t, err, "Table 'test.missing_base' doesn't exist")
	err = tk.ExecToErr("drop materialized view log if exists on missing_schema.missing_base")
	require.ErrorContains(t, err, "Table 'missing_schema.missing_base' doesn't exist")

	tk.MustExec("create table t_drop_if_exists (a int not null)")
	tk.MustExec("create table t_no_mlog_drop_if_exists (a int not null)")
	tk.MustExec("create materialized view log on t_drop_if_exists (a)")
	tk.MustExec("create materialized view mv_drop_if_exists (a, cnt) as select a, count(1) from t_drop_if_exists group by a")

	err = tk.ExecToErr("drop materialized view if exists t_drop_if_exists")
	require.ErrorContains(t, err, "is not MATERIALIZED VIEW")
	err = tk.ExecToErr("drop materialized view log if exists on mv_drop_if_exists")
	require.ErrorContains(t, err, "is not BASE TABLE")

	tk.MustExec("drop materialized view if exists mv_drop_if_exists")
	tk.MustExec("drop materialized view if exists mv_drop_if_exists")

	tk.MustExec("create view v_drop_if_exists as select * from t_drop_if_exists")
	err = tk.ExecToErr("drop materialized view log if exists on v_drop_if_exists")
	require.ErrorContains(t, err, "is not BASE TABLE")

	tk.MustExec("drop materialized view log if exists on t_no_mlog_drop_if_exists")
	tk.MustQuery("show warnings").Check(testkit.Rows("Note 1051 Unknown table 'test.t_no_mlog_drop_if_exists'"))
	tk.MustExec("drop materialized view log if exists on t_drop_if_exists")
	tk.MustExec("drop materialized view log if exists on t_drop_if_exists")
}

func TestAlterMaterializedViewRefreshExprTypeValidation(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("create materialized view log on t (a, b)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")

	err := tk.ExecToErr("alter materialized view mv refresh next 300")
	require.ErrorContains(t, err, "REFRESH NEXT expression must return DATETIME/TIMESTAMP")

	err = tk.ExecToErr("alter materialized view mv refresh start with 1 next date_add(now(), interval 1 hour)")
	require.ErrorContains(t, err, "REFRESH START WITH expression must return DATETIME/TIMESTAMP")

	tk.MustExec("alter materialized view mv refresh start with now() next date_add(now(), interval 1 hour)")
}

func TestAlterMaterializedViewMetadataDDL(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 2 hour) as select a, sum(b), count(1) from t group by a")

	getMView := func() (*model.TableInfo, *model.MaterializedViewInfo) {
		is := dom.InfoSchema()
		tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv"))
		require.NoError(t, err)
		require.NotNil(t, tbl.Meta().MaterializedView)
		return tbl.Meta(), tbl.Meta().MaterializedView
	}

	tk.MustExec("alter materialized view mv comment = 'updated comment'")
	mvTable, mvInfo := getMView()
	require.Equal(t, "updated comment", mvTable.Comment)

	tk.MustExec("alter materialized view mv refresh start with date_add(now(), interval 40 minute) next date_add(now(), interval 20 minute)")
	_, mvInfo = getMView()
	require.Equal(t, "FAST", mvInfo.RefreshMethod)
	require.Equal(t, "DATE_ADD(NOW(), INTERVAL 40 MINUTE)", mvInfo.RefreshStartWith)
	require.Equal(t, "DATE_ADD(NOW(), INTERVAL 20 MINUTE)", mvInfo.RefreshNext)
	tk.MustQuery(fmt.Sprintf("select NEXT_REFRESH_UNIX_SECONDS is not null from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", mvTable.ID)).Check(testkit.Rows("1"))

	tk.MustExec("alter materialized view mv attributes='mview_alert_warning=5,mview_alert_overdue=10,mview_alert_refresh_failed=yes'")
	_, mvInfo = getMView()
	require.Equal(t, int64(5), mvInfo.AlertWarningSec)
	require.Equal(t, int64(10), mvInfo.AlertOverdueSec)
	require.True(t, mvInfo.AlertRefreshFailed)

	err := tk.ExecToErr("alter materialized view mv attributes='mview_alert_warning=20,mview_alert_overdue=10'")
	require.ErrorContains(t, err, "must be less than or equal")

	tk.MustExec("alter materialized view mv refresh")
	_, mvInfo = getMView()
	require.Empty(t, mvInfo.RefreshStartWith)
	require.Empty(t, mvInfo.RefreshNext)
	tk.MustQuery(fmt.Sprintf("select NEXT_REFRESH_UNIX_SECONDS is null from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", mvTable.ID)).Check(testkit.Rows("1"))
}

func TestAlterMaterializedViewAttributesUpdatesAlertThresholds(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_mv_attr (a int not null, b int not null)")
	tk.MustExec("create materialized view log on t_mv_attr (a, b)")
	tk.MustExec("create materialized view mv_attr (a, s, cnt) refresh fast next date_add(now(), interval 2 hour) attributes='mview_alert_warning=300,mview_alert_overdue=600,mview_alert_refresh_failed=no' as select a, sum(b), count(1) from t_mv_attr group by a")

	getMViewInfo := func() *model.MaterializedViewInfo {
		tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_attr"))
		require.NoError(t, err)
		require.NotNil(t, tbl.Meta().MaterializedView)
		return tbl.Meta().MaterializedView
	}

	mvInfo := getMViewInfo()
	require.Equal(t, int64(300), mvInfo.AlertWarningSec)
	require.Equal(t, int64(600), mvInfo.AlertOverdueSec)
	require.False(t, mvInfo.AlertRefreshFailed)
	require.Equal(t, "DATE_ADD(NOW(), INTERVAL 2 HOUR)", mvInfo.RefreshNext)

	tk.MustExec("alter materialized view mv_attr attributes='mview_alert_warning=5,mview_alert_overdue=5,mview_alert_refresh_failed=yes'")
	mvInfo = getMViewInfo()
	require.Equal(t, int64(5), mvInfo.AlertWarningSec)
	require.Equal(t, int64(5), mvInfo.AlertOverdueSec)
	require.True(t, mvInfo.AlertRefreshFailed)
	require.Equal(t, "DATE_ADD(NOW(), INTERVAL 2 HOUR)", mvInfo.RefreshNext)

	tk.MustExec("alter materialized view mv_attr refresh next date_add(now(), interval 25 minute)")
	tk.MustExec("alter materialized view mv_attr attributes='mview_alert_warning=10,mview_alert_overdue=20,mview_alert_refresh_failed=no'")
	mvInfo = getMViewInfo()
	require.Equal(t, int64(10), mvInfo.AlertWarningSec)
	require.Equal(t, int64(20), mvInfo.AlertOverdueSec)
	require.False(t, mvInfo.AlertRefreshFailed)
	require.Equal(t, "DATE_ADD(NOW(), INTERVAL 25 MINUTE)", mvInfo.RefreshNext)

	err := tk.ExecToErr("alter materialized view mv_attr attributes='unknown=1'")
	require.ErrorContains(t, err, "unsupported ATTRIBUTES key")
	err = tk.ExecToErr("alter materialized view mv_attr attributes='mview_alert_refresh_failed=maybe'")
	require.ErrorContains(t, err, "must be yes or no")
	err = tk.ExecToErr("alter materialized view mv_attr attributes='mview_alert_warning=20,mview_alert_overdue=10'")
	require.ErrorContains(t, err, "must be less than or equal")
}

func TestAlterMaterializedViewRefreshDisableScheduleUpdatesAlert(t *testing.T) {
	tests := []struct {
		name           string
		refreshFailed  bool
		expectAlertRow bool
	}{
		{name: "clears non-failed alert"},
		{name: "preserves refresh-failed alert", refreshFailed: true, expectAlertRow: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, dom := testkit.CreateMockStoreAndDomain(t)
			tk := newMViewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustExec("create table t_mv_alert (a int not null, b int not null)")
			tk.MustExec("create materialized view log on t_mv_alert (a, b)")
			tk.MustExec("create materialized view mv_alert (a, s, cnt) refresh fast next date_add(now(), interval 2 hour) as select a, sum(b), count(1) from t_mv_alert group by a")

			tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_alert"))
			require.NoError(t, err)
			refreshFailed := "NULL"
			if tt.refreshFailed {
				refreshFailed = "'YES'"
			}
			tk.MustExec(fmt.Sprintf(
				"insert into mysql.tidb_mview_refresh_alert (MVIEW_ID, MVIEW_SCHEMA, MVIEW_NAME, ALERT_LEVEL, REFRESH_FAILED, LAST_SUCCESS_SNAPSHOT_TIME, UPDATE_TIME) values (%d, 'test', 'mv_alert', 'warning', %s, UTC_TIMESTAMP(), UTC_TIMESTAMP())",
				tbl.Meta().ID,
				refreshFailed,
			))

			tk.MustExec("alter materialized view mv_alert refresh")
			tk.MustQuery(fmt.Sprintf("select NEXT_REFRESH_UNIX_SECONDS is null from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", tbl.Meta().ID)).Check(testkit.Rows("1"))
			if !tt.expectAlertRow {
				tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mview_refresh_alert where MVIEW_ID = %d", tbl.Meta().ID)).Check(testkit.Rows("0"))
				return
			}
			tk.MustQuery(fmt.Sprintf("select ALERT_LEVEL is null, REFRESH_FAILED from mysql.tidb_mview_refresh_alert where MVIEW_ID = %d", tbl.Meta().ID)).Check(testkit.Rows("1 YES"))
		})
	}
}

func TestAlterMaterializedViewRefreshScheduleTimeZone(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set time_zone = '+00:00'")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("create materialized view log on t (a, b)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next cast('2030-01-01 10:00:00' as datetime) as select a, sum(b), count(1) from t group by a")

	getMView := func() (*model.TableInfo, *model.MaterializedViewInfo) {
		is := dom.InfoSchema()
		tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv"))
		require.NoError(t, err)
		require.NotNil(t, tbl.Meta().MaterializedView)
		return tbl.Meta(), tbl.Meta().MaterializedView
	}

	mvTable, info := getMView()
	initialTimeZoneName := info.RefreshScheduleTimeZone.Name
	initialTimeZoneOffset := info.RefreshScheduleTimeZone.Offset
	require.Equal(t, 0, initialTimeZoneOffset)

	tk.MustExec("set time_zone = '+08:00'")
	tk.MustExec("alter materialized view mv refresh")
	_, info = getMView()
	require.Equal(t, initialTimeZoneName, info.RefreshScheduleTimeZone.Name)
	require.Equal(t, initialTimeZoneOffset, info.RefreshScheduleTimeZone.Offset)
	require.Empty(t, info.RefreshNext)

	tk.MustExec("alter materialized view mv refresh next cast('2030-01-02 10:00:00' as datetime)")
	_, info = getMView()
	require.Equal(t, 8*60*60, info.RefreshScheduleTimeZone.Offset)
	tk.MustQuery("select NEXT_REFRESH_UNIX_SECONDS = 1893549600 from mysql.tidb_mview_refresh_info where MVIEW_ID = " + strconv.FormatInt(mvTable.ID, 10)).Check(testkit.Rows("1"))
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

func TestAlterMaterializedViewLogAddColumnSupportsNewMaterializedView(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_add_mlog_mv (a int not null, b int not null, c int not null)")
	tk.MustExec("insert into t_add_mlog_mv values (1, 10, 100), (1, 20, 200), (2, 30, 300)")
	tk.MustExec("create materialized view log on t_add_mlog_mv (a, b)")

	err := tk.ExecToErr("create materialized view mv_add_mlog_col_before (a, s, cnt) refresh fast as select a, sum(c), count(1) from t_add_mlog_mv group by a")
	require.ErrorContains(t, err, "materialized view log does not contain column c")

	tk.MustExec("alter materialized view log on t_add_mlog_mv add column (c)")
	tk.MustExec("create materialized view mv_add_mlog_col_after (a, s, cnt) refresh fast as select a, sum(c), count(1) from t_add_mlog_mv group by a")
	tk.MustQuery("select a, s, cnt from mv_add_mlog_col_after order by a").Check(testkit.Rows("1 300 2", "2 300 1"))
}

func TestAlterTableWithMaterializedViewDependencies(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (2, -1)")
	tk.MustExec("create materialized view log on t (a, b)")
	tk.MustExec("create materialized view mv (a, cnt) as select a, count(1) from t group by a")
	tk.MustExec("create materialized view mv_where (a, cnt) as select a, count(1) from t where b > 0 group by a")

	tk.MustExec("alter table t modify column a bigint not null")
	showCreate := tk.MustQuery("show create table `$mlog$t`").Rows()[0][1].(string)
	require.Contains(t, showCreate, "`a` bigint")
	showCreate = tk.MustQuery("show create table mv").Rows()[0][1].(string)
	require.Contains(t, showCreate, "`a` bigint")

	err := tk.ExecToErr("alter table t modify column b bigint not null")
	require.ErrorContains(t, err, "does not support modifying columns used in WHERE clause")
	err = tk.ExecToErr("alter table t change column a a2 bigint")
	require.ErrorContains(t, err, "does not support renaming")
	err = tk.ExecToErr("alter table t modify column b smallint")
	require.ErrorContains(t, err, "only supports no-reorg compatible type changes")
}

func TestMaterializedViewRelatedTablesDDLRejected(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_ddl_mv (a int not null, b int)")
	tk.MustExec("create materialized view log on t_ddl_mv (a, b)")

	err := tk.ExecToErr("drop table t_ddl_mv")
	require.ErrorContains(t, err, "DROP TABLE on base table with materialized view log")
	err = tk.ExecToErr("rename table t_ddl_mv to t_ddl_mv2")
	require.ErrorContains(t, err, "RENAME TABLE on base table with materialized view log")
	err = tk.ExecToErr("drop table `$mlog$t_ddl_mv`")
	require.ErrorContains(t, err, "DROP TABLE on materialized view log table")

	tk.MustExec("create materialized view mv_ddl_mv (a, cnt) as select a, count(1) from t_ddl_mv group by a")
	tk.MustExec("alter table t_ddl_mv add column c int")
	err = tk.ExecToErr("alter table t_ddl_mv modify column a bigint")
	require.ErrorContains(t, err, "does not support changing charset/collation/nullability of group keys")
	err = tk.ExecToErr("drop table t_ddl_mv")
	require.ErrorContains(t, err, "DROP TABLE on base table with materialized view dependencies")
	err = tk.ExecToErr("alter table mv_ddl_mv add column x int")
	require.ErrorContains(t, err, "ALTER TABLE on materialized view table")
	err = tk.ExecToErr("alter table `$mlog$t_ddl_mv` add index idx_mlog_b(b)")
	require.ErrorContains(t, err, "ALTER TABLE on materialized view log table")
	err = tk.ExecToErr("create index idx_mlog_b_create on `$mlog$t_ddl_mv`(b)")
	require.ErrorContains(t, err, "CREATE INDEX on materialized view log table")
}
