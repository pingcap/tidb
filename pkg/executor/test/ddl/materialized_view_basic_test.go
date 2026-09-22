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
