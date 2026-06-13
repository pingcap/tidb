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
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/kv"
	metamodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/auth"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func mustExecInternal(t *testing.T, tk *testkit.TestKit, sql string) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnMVMaintenance)
	vars := tk.Session().GetSessionVars()
	origMaint := vars.InMaterializedViewMaintenance
	origRestr := vars.InRestrictedSQL
	vars.InMaterializedViewMaintenance = true
	vars.InRestrictedSQL = true
	defer func() {
		vars.InMaterializedViewMaintenance = origMaint
		vars.InRestrictedSQL = origRestr
	}()
	rs, err := tk.Session().ExecuteInternal(ctx, sql)
	require.NoError(t, err)
	require.Nil(t, rs)
}

func waitMVTaskCancelWatcherRequested(t *testing.T, watchNamePrefix string) <-chan struct{} {
	t.Helper()

	requestedCh := make(chan struct{})
	var requestedChClosed atomic.Bool
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/mvTaskCancelWatcherRequested", func(watchName string) {
		if strings.HasPrefix(watchName, watchNamePrefix) && requestedChClosed.CompareAndSwap(false, true) {
			close(requestedCh)
		}
	})
	return requestedCh
}

func readAffectedRowsMetricValue(t *testing.T, label string) float64 {
	t.Helper()

	counter, err := metrics.AffectedRowsCounter.GetMetricWithLabelValues(label)
	require.NoError(t, err)
	pb := &dto.Metric{}
	require.NoError(t, counter.Write(pb))
	return pb.GetCounter().GetValue()
}

func differentIsolationReadEnginesForTest(current string) string {
	for _, candidate := range []string{
		"tikv",
		"tiflash",
		"tidb",
		"tikv,tidb",
		"tikv,tiflash",
		"tikv,tiflash,tidb",
	} {
		if candidate != current {
			return candidate
		}
	}
	return "tikv"
}

func TestCreateMaterializedViewLogBasic(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, b int)")
	expectedSQLMode := tk.Session().GetSessionVars().SQLMode

	tk.MustExec("create materialized view log on t (a) purge start with cast('2026-01-02 03:04:05' as datetime) next cast('2026-01-02 03:14:05' as datetime)")

	// Physical table created.
	tk.MustQuery("select count(*) from information_schema.tables where table_schema='test' and table_name='$mlog$t'").Check(testkit.Rows("1"))

	is := dom.InfoSchema()
	baseTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
	require.NoError(t, err)
	mlogTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("$mlog$t"))
	require.NoError(t, err)

	require.NotNil(t, baseTable.Meta().MaterializedViewBase)
	require.Equal(t, mlogTable.Meta().ID, baseTable.Meta().MaterializedViewBase.MLogID)
	// Lock row for PURGE MATERIALIZED VIEW LOG should be inserted on CREATE MATERIALIZED VIEW LOG success.
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mlog_purge_info where mlog_id = %d", mlogTable.Meta().ID)).
		Check(testkit.Rows("1"))

	mlogInfo := mlogTable.Meta().MaterializedViewLog
	require.NotNil(t, mlogInfo)
	require.Equal(t, baseTable.Meta().ID, mlogInfo.BaseTableID)
	require.Equal(t, []pmodel.CIStr{pmodel.NewCIStr("a")}, mlogInfo.Columns)
	require.Equal(t, "DEFERRED", mlogInfo.PurgeMethod)
	require.Equal(t, "CAST('2026-01-02 03:04:05' AS DATETIME)", mlogInfo.PurgeStartWith)
	require.Equal(t, "CAST('2026-01-02 03:14:05' AS DATETIME)", mlogInfo.PurgeNext)
	require.Equal(t, expectedSQLMode, mlogInfo.DefinitionSQLMode)

	// Meta columns should exist on the log table.
	dmlTypeColName := pmodel.NewCIStr("_MLOG$_DML_TYPE")
	oldNewColName := pmodel.NewCIStr("_MLOG$_OLD_NEW")

	var hasDMLType, hasOldNew bool
	for _, c := range mlogTable.Meta().Columns {
		if c.Name.L == dmlTypeColName.L {
			hasDMLType = true
		}
		if c.Name.L == oldNewColName.L {
			hasOldNew = true
			require.Equal(t, mysql.TypeTiny, c.FieldType.GetType())
		}
	}
	require.True(t, hasDMLType)
	require.True(t, hasOldNew)

	// Duplicated MV LOG should fail (same derived table name).
	tk.MustGetErrMsg("create materialized view log on t (a)", "[schema:1050]Table 'test.$mlog$t' already exists")
}

// TestAlterMaterializedViewLogAddColumnBasic verifies that ADD COLUMN updates
// mlog metadata, backfills old log rows with mlog defaults, and records future
// changes only when newly tracked columns are touched.
func TestAlterMaterializedViewLogAddColumnBasic(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_add_mlog_col (id int not null, n int not null, s varchar(10) not null, d date not null, note text not null, untouched int)")
	tk.MustExec("create materialized view log on t_add_mlog_col (id)")
	tk.MustExec("insert into t_add_mlog_col values (1, 7, 'old', '2026-01-02', 'memo', 100)")

	tk.MustExec("alter materialized view log on t_add_mlog_col add column (n, s, d, note)")

	is := dom.InfoSchema()
	mlogTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("$mlog$t_add_mlog_col"))
	require.NoError(t, err)
	mlogInfo := mlogTable.Meta().MaterializedViewLog
	require.NotNil(t, mlogInfo)
	require.Equal(t, []pmodel.CIStr{
		pmodel.NewCIStr("id"),
		pmodel.NewCIStr("n"),
		pmodel.NewCIStr("s"),
		pmodel.NewCIStr("d"),
		pmodel.NewCIStr("note"),
	}, mlogInfo.Columns)

	colNames := make([]string, 0, len(mlogTable.Meta().Columns))
	colByName := make(map[string]*metamodel.ColumnInfo, len(mlogTable.Meta().Columns))
	for _, col := range mlogTable.Meta().Columns {
		colNames = append(colNames, col.Name.O)
		colByName[col.Name.L] = col
	}
	require.Equal(t, []string{"id", "n", "s", "d", "note", "_MLOG$_DML_TYPE", "_MLOG$_OLD_NEW"}, colNames)
	for _, name := range []string{"n", "s", "d", "note"} {
		require.True(t, mysql.HasNotNullFlag(colByName[name].GetFlag()))
	}
	require.Equal(t, "0", fmt.Sprint(colByName["n"].GetOriginDefaultValue()))
	require.Equal(t, " ", fmt.Sprint(colByName["s"].GetOriginDefaultValue()))
	require.Equal(t, "0000-00-00", fmt.Sprint(colByName["d"].GetOriginDefaultValue()))
	require.Equal(t, " ", fmt.Sprint(colByName["note"].GetOriginDefaultValue()))

	tk.MustQuery("select n, hex(s), cast(d as char), hex(note), `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t_add_mlog_col`").
		Check(testkit.Rows("0 20 0000-00-00 20 I 1"))

	showCreate := tk.MustQuery("show create materialized view log on t_add_mlog_col").Rows()[0][1].(string)
	require.Contains(t, showCreate, "CREATE MATERIALIZED VIEW LOG ON `t_add_mlog_col` (`id`, `n`, `s`, `d`, `note`)")

	mustExecInternal(t, tk, "delete from `$mlog$t_add_mlog_col`")
	tk.MustExec("update t_add_mlog_col set untouched = 101 where id = 1")
	tk.MustQuery("select * from `$mlog$t_add_mlog_col`").Check(testkit.Rows())

	tk.MustExec("update t_add_mlog_col set s = 'new' where id = 1")
	tk.MustQuery("select id, n, s, cast(d as char), note, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t_add_mlog_col`").Sort().
		Check(testkit.Rows(
			"1 7 new 2026-01-02 memo U 1",
			"1 7 old 2026-01-02 memo U -1",
		))

	tk.MustExec("create table t_add_mlog_atomic (id int, b int, c int)")
	tk.MustExec("create materialized view log on t_add_mlog_atomic (id)")
	cancelTK := testkit.NewTestKit(t, store)
	cancelTK.MustExec("use test")
	cancelTriggered := atomic.Bool{}
	cancelDone := make(chan error, 1)
	require.NoError(t, failpoint.EnableCall("github.com/pingcap/tidb/pkg/ddl/onJobUpdated", func(job *metamodel.Job) {
		if !cancelTriggered.CompareAndSwap(false, true) {
			return
		}
		if job.Type != metamodel.ActionMultiSchemaChange ||
			job.SchemaName != "test" ||
			job.TableName != "$mlog$t_add_mlog_atomic" ||
			job.MultiSchemaInfo == nil ||
			len(job.MultiSchemaInfo.SubJobs) != 2 ||
			job.MultiSchemaInfo.SubJobs[1].SchemaState != metamodel.StateWriteReorganization {
			cancelTriggered.Store(false)
			return
		}
		errs, err := ddl.CancelJobs(context.Background(), cancelTK.Session(), []int64{job.ID})
		if len(errs) > 0 && errs[0] != nil {
			cancelDone <- errs[0]
			return
		}
		cancelDone <- err
	}))
	err = tk.ExecToErr("alter materialized view log on t_add_mlog_atomic add column (b, c)")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/onJobUpdated"))
	require.ErrorContains(t, err, "Cancelled DDL job")
	select {
	case cancelErr := <-cancelDone:
		require.NoError(t, cancelErr)
	default:
		require.FailNow(t, "expected mlog multi-column add cancellation")
	}
	showCreate = tk.MustQuery("show create materialized view log on t_add_mlog_atomic").Rows()[0][1].(string)
	require.Contains(t, showCreate, "CREATE MATERIALIZED VIEW LOG ON `t_add_mlog_atomic` (`id`)")
	require.NotContains(t, showCreate, "`b`")
	require.NotContains(t, showCreate, "`c`")
}

// TestAlterMaterializedViewLogAddColumnDefaultSemantics verifies the default
// values used for existing mlog rows when ADD COLUMN tracks nullable columns,
// enum/set columns, and not-null string columns.
func TestAlterMaterializedViewLogAddColumnDefaultSemantics(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	// This table covers the mlog backfill defaults that differ from normal AddColumn:
	// nullable columns should keep NULL for old mlog rows, enum/set should use the
	// regular TiDB default semantics, and not-null string columns should use a
	// single-space placeholder required by materialized view log history rows.
	tk.MustExec("create table t_add_mlog_defaults (" +
		"id int," +
		"nullable_varchar varchar(10)," +
		"nullable_text text," +
		"nn_enum enum('a','b') not null," +
		"nn_set set('x','y') not null," +
		"nullable_enum enum('a','b')," +
		"nullable_set set('x','y')," +
		"nn_varchar varchar(10) not null," +
		"nn_text text not null)")
	tk.MustExec("create materialized view log on t_add_mlog_defaults (id)")
	tk.MustExec("insert into t_add_mlog_defaults values (1, null, null, 'b', 'x', null, null, 'old', 'memo')")

	tk.MustExec("alter materialized view log on t_add_mlog_defaults add column (" +
		"nullable_varchar, nullable_text, nn_enum, nn_set, nullable_enum, nullable_set, nn_varchar, nn_text)")

	// The existing INSERT log row is historical data. It should not read current
	// base-table values for newly tracked columns; it should only expose the
	// metadata default chosen for old mlog rows.
	tk.MustQuery("select " +
		"nullable_varchar is null, nullable_text is null, " +
		"cast(nn_enum as char), cast(nn_set as char), " +
		"nullable_enum is null, nullable_set is null, " +
		"hex(nn_varchar), hex(nn_text), `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` " +
		"from `$mlog$t_add_mlog_defaults`").
		Check(testkit.Rows("1 1 a  1 1 20 20 I 1"))
}

// TestAlterMaterializedViewLogAddColumnRejectsInvalidColumns verifies that ADD
// COLUMN rejects duplicate tracked columns, duplicate names in one statement,
// missing base columns, and reserved mlog metadata columns.
func TestAlterMaterializedViewLogAddColumnRejectsInvalidColumns(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
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
}

// TestAlterMaterializedViewLogAddColumnPrivilege verifies that ADD COLUMN
// requires ALTER privilege on the mlog table and SELECT privilege on the base
// table.
func TestAlterMaterializedViewLogAddColumnPrivilege(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_add_mlog_priv (a int, b int)")
	tk.MustExec("create materialized view log on t_add_mlog_priv (a)")
	tk.MustExec("create user 'u_add_mlog_no_select'@'%'")
	tk.MustExec("create user 'u_add_mlog_ok'@'%'")
	defer tk.MustExec("drop user 'u_add_mlog_no_select'@'%'")
	defer tk.MustExec("drop user 'u_add_mlog_ok'@'%'")

	tk.MustExec("grant alter on test.`$mlog$t_add_mlog_priv` to 'u_add_mlog_no_select'@'%'")
	tkNoSelect := testkit.NewTestKit(t, store)
	require.NoError(t, tkNoSelect.Session().Auth(&auth.UserIdentity{Username: "u_add_mlog_no_select", Hostname: "%"}, nil, nil, nil))
	err := tkNoSelect.ExecToErr("alter materialized view log on test.t_add_mlog_priv add column (b)")
	require.ErrorContains(t, err, "SELECT command denied")

	tk.MustExec("grant alter on test.`$mlog$t_add_mlog_priv` to 'u_add_mlog_ok'@'%'")
	tk.MustExec("grant select on test.t_add_mlog_priv to 'u_add_mlog_ok'@'%'")
	tkOK := testkit.NewTestKit(t, store)
	require.NoError(t, tkOK.Session().Auth(&auth.UserIdentity{Username: "u_add_mlog_ok", Hostname: "%"}, nil, nil, nil))
	tkOK.MustExec("alter materialized view log on test.t_add_mlog_priv add column (b)")
}

// TestAlterMaterializedViewLogAddColumnSupportsNewMaterializedView verifies
// that a fast-refresh materialized view can be created and refreshed after its
// referenced base column is added to the materialized view log.
func TestAlterMaterializedViewLogAddColumnSupportsNewMaterializedView(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_add_mlog_mv (a int not null, b int not null, c int not null)")
	tk.MustExec("insert into t_add_mlog_mv values (1, 10, 100), (1, 20, 200), (2, 30, 300)")
	tk.MustExec("create materialized view log on t_add_mlog_mv (a, b)")

	err := tk.ExecToErr("create materialized view mv_add_mlog_col_before (a, s, cnt) refresh fast as select a, sum(c), count(1) from t_add_mlog_mv group by a")
	require.ErrorContains(t, err, "materialized view log does not contain column c")

	tk.MustExec("alter materialized view log on t_add_mlog_mv add column (c)")
	tk.MustExec("create materialized view mv_add_mlog_col_after (a, s, cnt) refresh fast as select a, sum(c), count(1) from t_add_mlog_mv group by a")
	tk.MustQuery("select a, s, cnt from mv_add_mlog_col_after order by a").Check(testkit.Rows(
		"1 300 2",
		"2 300 1",
	))

	tk.MustExec("update t_add_mlog_mv set c = 150 where a = 1 and b = 10")
	tk.MustExec("insert into t_add_mlog_mv values (2, 40, 400)")
	tk.MustExec("refresh materialized view mv_add_mlog_col_after fast")
	tk.MustQuery("select a, s, cnt from mv_add_mlog_col_after order by a").Check(testkit.Rows(
		"1 350 2",
		"2 700 2",
	))
}

func TestCreateMaterializedViewLogPrivilege(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_create_mlog_priv (a int)")
	tk.MustExec("create user 'u_create_mlog_no_create'@'%'")
	tk.MustExec("create user 'u_create_mlog_no_select'@'%'")
	tk.MustExec("create user 'u_create_mlog_table_create'@'%'")
	tk.MustExec("create user 'u_create_mlog_ok'@'%'")
	defer tk.MustExec("drop user 'u_create_mlog_no_create'@'%'")
	defer tk.MustExec("drop user 'u_create_mlog_no_select'@'%'")
	defer tk.MustExec("drop user 'u_create_mlog_table_create'@'%'")
	defer tk.MustExec("drop user 'u_create_mlog_ok'@'%'")

	tk.MustExec("grant select on test.t_create_mlog_priv to 'u_create_mlog_no_create'@'%'")
	tkNoCreate := testkit.NewTestKit(t, store)
	require.NoError(t, tkNoCreate.Session().Auth(&auth.UserIdentity{Username: "u_create_mlog_no_create", Hostname: "%"}, nil, nil, nil))
	err := tkNoCreate.ExecToErr("create materialized view log on test.t_create_mlog_priv (a)")
	require.ErrorContains(t, err, "CREATE VIEW command denied")

	tk.MustExec("grant create view on test.* to 'u_create_mlog_no_select'@'%'")
	tkNoSelect := testkit.NewTestKit(t, store)
	require.NoError(t, tkNoSelect.Session().Auth(&auth.UserIdentity{Username: "u_create_mlog_no_select", Hostname: "%"}, nil, nil, nil))
	err = tkNoSelect.ExecToErr("create materialized view log on test.t_create_mlog_priv (a)")
	require.ErrorContains(t, err, "SELECT command denied")

	tk.MustExec("grant create view on test.* to 'u_create_mlog_ok'@'%'")
	tk.MustExec("grant select on test.t_create_mlog_priv to 'u_create_mlog_ok'@'%'")
	tkOK := testkit.NewTestKit(t, store)
	require.NoError(t, tkOK.Session().Auth(&auth.UserIdentity{Username: "u_create_mlog_ok", Hostname: "%"}, nil, nil, nil))
	tkOK.MustExec("create materialized view log on test.t_create_mlog_priv (a)")

	tk.MustExec("grant create view on test.t_create_mlog_priv to 'u_create_mlog_table_create'@'%'")
	tk.MustExec("grant select on test.t_create_mlog_priv to 'u_create_mlog_table_create'@'%'")
	tkTableCreate := testkit.NewTestKit(t, store)
	require.NoError(t, tkTableCreate.Session().Auth(&auth.UserIdentity{Username: "u_create_mlog_table_create", Hostname: "%"}, nil, nil, nil))
	err = tkTableCreate.ExecToErr("create materialized view log on test.t_create_mlog_priv (a)")
	require.ErrorContains(t, err, "CREATE VIEW command denied")
}

func TestGrantMaterializedViewObjectPrivileges(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_grant_mv_priv (a int)")
	tk.MustExec("create materialized view log on t_grant_mv_priv (a)")
	tk.MustExec("create materialized view mv_grant_priv (a, cnt) as select a, count(1) from t_grant_mv_priv group by a")
	tk.MustExec("create user 'u_grant_mv_priv'@'%'")
	defer tk.MustExec("drop user 'u_grant_mv_priv'@'%'")

	tk.MustExec("grant all privileges on test.mv_grant_priv to 'u_grant_mv_priv'@'%'")
	rows := tk.MustQuery("select Table_priv, Column_priv from mysql.tables_priv where User = 'u_grant_mv_priv' and Table_name = 'mv_grant_priv'").Rows()
	require.Len(t, rows, 1)
	mvTablePrivs := fmt.Sprint(rows[0][0])
	require.Contains(t, mvTablePrivs, "Select")
	require.Contains(t, mvTablePrivs, "Show View")
	require.Contains(t, mvTablePrivs, "Alter")
	require.Contains(t, mvTablePrivs, "Drop")
	require.Contains(t, mvTablePrivs, "Operate View")
	require.NotContains(t, mvTablePrivs, "Insert")
	require.NotContains(t, mvTablePrivs, "Update")
	require.NotContains(t, mvTablePrivs, "Delete")
	require.Equal(t, "", fmt.Sprint(rows[0][1]))

	err := tk.ExecToErr("grant insert on test.mv_grant_priv to 'u_grant_mv_priv'@'%'")
	require.ErrorContains(t, err, "cannot grant Insert privilege on materialized view")
	err = tk.ExecToErr("grant operate view (a) on test.mv_grant_priv to 'u_grant_mv_priv'@'%'")
	require.ErrorContains(t, err, "COLUMN GRANT")

	tk.MustExec("grant all privileges on test.`$mlog$t_grant_mv_priv` to 'u_grant_mv_priv'@'%'")
	rows = tk.MustQuery("select Table_priv, Column_priv from mysql.tables_priv where User = 'u_grant_mv_priv' and Table_name = '$mlog$t_grant_mv_priv'").Rows()
	require.Len(t, rows, 1)
	mlogTablePrivs := fmt.Sprint(rows[0][0])
	require.Contains(t, mlogTablePrivs, "Select")
	require.Contains(t, mlogTablePrivs, "Show View")
	require.Contains(t, mlogTablePrivs, "Alter")
	require.Contains(t, mlogTablePrivs, "Drop")
	require.Contains(t, mlogTablePrivs, "Operate View")
	require.NotContains(t, mlogTablePrivs, "Insert")
	require.NotContains(t, mlogTablePrivs, "Update")
	require.NotContains(t, mlogTablePrivs, "Delete")
	require.Equal(t, "", fmt.Sprint(rows[0][1]))

	err = tk.ExecToErr("grant update on test.`$mlog$t_grant_mv_priv` to 'u_grant_mv_priv'@'%'")
	require.ErrorContains(t, err, "cannot grant Update privilege on materialized view log")
}

func TestShowCreateMaterializedViewLog(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_show_mlog (a int, b int)")
	tk.MustExec("create materialized view log on t_show_mlog (a, b) shard_row_id_bits = 2 pre_split_regions = 2 purge start with cast('2026-01-02 03:04:05' as datetime) next date_add(now(), interval 1 hour)")

	rows := tk.MustQuery("show create materialized view log on t_show_mlog").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "t_show_mlog", rows[0][0])
	showCreate, ok := rows[0][1].(string)
	require.True(t, ok)
	require.Contains(t, showCreate, "CREATE MATERIALIZED VIEW LOG ON `t_show_mlog` (`a`, `b`)")
	require.Contains(t, showCreate, "SHARD_ROW_ID_BITS = 2 PRE_SPLIT_REGIONS = 2")
	require.Contains(t, showCreate, "PURGE START WITH CAST('2026-01-02 03:04:05' AS DATETIME) NEXT DATE_ADD(NOW(), INTERVAL 1 HOUR)")
	_, err := parser.New().ParseOneStmt(showCreate, "", "")
	require.NoError(t, err)

	tk.MustExec("create temporary table t_show_mlog (a int)")
	rows = tk.MustQuery("show create materialized view log on t_show_mlog").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "t_show_mlog", rows[0][0])
	require.Contains(t, rows[0][1], "CREATE MATERIALIZED VIEW LOG ON `t_show_mlog` (`a`, `b`)")

	tk.MustExec("create user 'u_show_create_mlog'@'%'")
	defer tk.MustExec("drop user 'u_show_create_mlog'@'%'")
	tkShow := testkit.NewTestKit(t, store)
	require.NoError(t, tkShow.Session().Auth(&auth.UserIdentity{Username: "u_show_create_mlog", Hostname: "%"}, nil, nil, nil))
	err = tkShow.ExecToErr("show create materialized view log on test.t_show_mlog")
	require.ErrorContains(t, err, "SHOW VIEW command denied")
	tk.MustExec("grant show view on test.`$mlog$t_show_mlog` to 'u_show_create_mlog'@'%'")
	err = tkShow.ExecToErr("show create materialized view log on test.t_show_mlog")
	require.ErrorContains(t, err, "SELECT command denied")
	tk.MustExec("grant select on test.`$mlog$t_show_mlog` to 'u_show_create_mlog'@'%'")
	userRows := tkShow.MustQuery("show create materialized view log on test.t_show_mlog").Rows()
	require.Len(t, userRows, 1)
	require.Equal(t, "t_show_mlog", userRows[0][0])
	require.Equal(t, showCreate, userRows[0][1])

	tk.MustExec("create table t_no_mlog (a int)")
	err = tk.QueryToErr("show create materialized view log on t_no_mlog")
	require.ErrorContains(t, err, "'test.t_no_mlog' is not BASE TABLE WITH MATERIALIZED VIEW LOG")
}

func TestCreateMaterializedViewLogPreSplitOptions(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
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
	mlogTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("$mlog$t_mlog_presplit"))
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
	tk := testkit.NewTestKit(t, store)
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

func TestCreateMaterializedViewLogPurgeInfoNextTimeDerivation(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	getMLogID := func(baseTable string) int64 {
		is := dom.InfoSchema()
		mlogTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("$mlog$"+baseTable))
		require.NoError(t, err)
		return mlogTable.Meta().ID
	}

	// START WITH and NEXT both present, START WITH is not near-now: NEXT_TIME should use START WITH.
	tk.MustExec("create table t_purge_start_only (a int)")
	tk.MustExec("create materialized view log on t_purge_start_only (a) purge start with date_add(now(), interval 40 minute) next date_add(now(), interval 20 minute)")
	mlogStartOnlyID := getMLogID("t_purge_start_only")
	tk.MustQuery(fmt.Sprintf(
		"select NEXT_TIME is not null, NEXT_TIME > UTC_TIMESTAMP() + interval 30 minute, NEXT_TIME < UTC_TIMESTAMP() + interval 2 hour from mysql.tidb_mlog_purge_info where MLOG_ID = %d",
		mlogStartOnlyID,
	)).Check(testkit.Rows("1 1 1"))

	// NEXT only: NEXT_TIME should use evaluated NEXT.
	tk.MustExec("create table t_purge_next_only (a int)")
	tk.MustExec("create materialized view log on t_purge_next_only (a) purge next date_add(now(), interval 20 minute)")
	mlogNextOnlyID := getMLogID("t_purge_next_only")
	tk.MustQuery(fmt.Sprintf(
		"select NEXT_TIME is not null, NEXT_TIME > UTC_TIMESTAMP() + interval 10 minute, NEXT_TIME < UTC_TIMESTAMP() + interval 1 hour from mysql.tidb_mlog_purge_info where MLOG_ID = %d",
		mlogNextOnlyID,
	)).Check(testkit.Rows("1 1 1"))

	// Neither START WITH nor NEXT: NEXT_TIME should stay unchanged (create path: NULL).
	tk.MustExec("create table t_purge_no_schedule (a int)")
	tk.MustExec("create materialized view log on t_purge_no_schedule (a)")
	mlogNoScheduleID := getMLogID("t_purge_no_schedule")
	tk.MustQuery(fmt.Sprintf(
		"select NEXT_TIME is null from mysql.tidb_mlog_purge_info where MLOG_ID = %d",
		mlogNoScheduleID,
	)).Check(testkit.Rows("1"))

	// START WITH near-now and NEXT present: NEXT_TIME should use NEXT.
	tk.MustExec("create table t_purge_near_now (a int)")
	tk.MustExec("create materialized view log on t_purge_near_now (a) purge start with now() next date_add(now(), interval 40 minute)")
	mlogNearNowID := getMLogID("t_purge_near_now")
	tk.MustQuery(fmt.Sprintf(
		"select NEXT_TIME is not null, NEXT_TIME > UTC_TIMESTAMP() + interval 20 minute, NEXT_TIME < UTC_TIMESTAMP() + interval 2 hour from mysql.tidb_mlog_purge_info where MLOG_ID = %d",
		mlogNearNowID,
	)).Check(testkit.Rows("1 1 1"))
}

func TestCreateMaterializedViewLogPurgeInfoNextTimeUsesUTC(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set time_zone = '+08:00'")

	getMLogID := func(baseTable string) int64 {
		is := dom.InfoSchema()
		mlogTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("$mlog$"+baseTable))
		require.NoError(t, err)
		return mlogTable.Meta().ID
	}

	tk.MustExec("create table t_purge_utc_next (a int)")
	tk.MustExec("create materialized view log on t_purge_utc_next (a) purge next date_add(now(), interval 1 hour)")
	mlogNextID := getMLogID("t_purge_utc_next")

	tk.MustQuery(fmt.Sprintf(
		"select NEXT_TIME is not null, "+
			"NEXT_TIME > UTC_TIMESTAMP(6) + interval 50 minute, "+
			"NEXT_TIME < UTC_TIMESTAMP(6) + interval 2 hour, "+
			"NEXT_TIME < NOW(6) - interval 6 hour "+
			"from mysql.tidb_mlog_purge_info where MLOG_ID = %d",
		mlogNextID,
	)).Check(testkit.Rows("1 1 1 1"))

	// START WITH should also be evaluated in UTC even when session timezone is +08:00.
	tk.MustExec("create table t_purge_utc_start (a int)")
	tk.MustExec("create materialized view log on t_purge_utc_start (a) purge start with date_add(now(), interval 40 minute) next date_add(now(), interval 20 minute)")
	mlogStartID := getMLogID("t_purge_utc_start")
	tk.MustQuery(fmt.Sprintf(
		"select NEXT_TIME is not null, "+
			"NEXT_TIME > UTC_TIMESTAMP(6) + interval 20 minute, "+
			"NEXT_TIME < UTC_TIMESTAMP(6) + interval 2 hour, "+
			"NEXT_TIME < NOW(6) - interval 7 hour "+
			"from mysql.tidb_mlog_purge_info where MLOG_ID = %d",
		mlogStartID,
	)).Check(testkit.Rows("1 1 1 1"))
}

func TestAlterMaterializedViewLogPurgeExprTypeValidation(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("create materialized view log on t (a) purge next date_add(now(), interval 1 hour)")

	err := tk.ExecToErr("alter materialized view log on t purge start with 1 next date_add(now(), interval 1 hour)")
	require.ErrorContains(t, err, "PURGE START WITH expression must return DATETIME/TIMESTAMP")

	err = tk.ExecToErr("alter materialized view log on t purge next 300")
	require.ErrorContains(t, err, "PURGE NEXT expression must return DATETIME/TIMESTAMP")

	tk.MustExec("alter materialized view log on t purge start with now() next date_add(now(), interval 1 hour)")
}

func TestAlterMaterializedViewLogPurgeUpdatesMetaAndNextTime(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 2 hour)")

	getMLogMeta := func() (int64, string, string, string) {
		is := dom.InfoSchema()
		mlogTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("$mlog$t"))
		require.NoError(t, err)
		require.NotNil(t, mlogTable.Meta().MaterializedViewLog)
		return mlogTable.Meta().ID,
			mlogTable.Meta().MaterializedViewLog.PurgeMethod,
			mlogTable.Meta().MaterializedViewLog.PurgeStartWith,
			mlogTable.Meta().MaterializedViewLog.PurgeNext
	}

	mlogID, purgeMethod, purgeStartWith, purgeNext := getMLogMeta()
	require.Equal(t, "DEFERRED", purgeMethod)
	require.Equal(t, "", purgeStartWith)
	require.Equal(t, "DATE_ADD(NOW(), INTERVAL 2 HOUR)", purgeNext)

	tk.MustExec("alter materialized view log on t purge start with date_add(now(), interval 40 minute) next date_add(now(), interval 20 minute)")
	_, purgeMethod, purgeStartWith, purgeNext = getMLogMeta()
	require.Equal(t, "DEFERRED", purgeMethod)
	require.Equal(t, "DATE_ADD(NOW(), INTERVAL 40 MINUTE)", purgeStartWith)
	require.Equal(t, "DATE_ADD(NOW(), INTERVAL 20 MINUTE)", purgeNext)
	tk.MustQuery(fmt.Sprintf(
		"select NEXT_TIME is not null, NEXT_TIME > UTC_TIMESTAMP() + interval 30 minute, NEXT_TIME < UTC_TIMESTAMP() + interval 2 hour from mysql.tidb_mlog_purge_info where MLOG_ID = %d",
		mlogID,
	)).Check(testkit.Rows("1 1 1"))

	tk.MustExec("alter materialized view log on t purge next date_add(now(), interval 25 minute)")
	_, purgeMethod, purgeStartWith, purgeNext = getMLogMeta()
	require.Equal(t, "DEFERRED", purgeMethod)
	require.Equal(t, "", purgeStartWith)
	require.Equal(t, "DATE_ADD(NOW(), INTERVAL 25 MINUTE)", purgeNext)
	tk.MustQuery(fmt.Sprintf(
		"select NEXT_TIME is not null, NEXT_TIME > UTC_TIMESTAMP() + interval 15 minute, NEXT_TIME < UTC_TIMESTAMP() + interval 1 hour from mysql.tidb_mlog_purge_info where MLOG_ID = %d",
		mlogID,
	)).Check(testkit.Rows("1 1 1"))

	/*
		tk.MustExec("alter materialized view log on t purge")
		_, purgeMethod, purgeStartWith, purgeNext = getMLogMeta()
		require.Equal(t, "DEFERRED", purgeMethod)
		require.Equal(t, "", purgeStartWith)
		require.Equal(t, "", purgeNext)
		tk.MustQuery(fmt.Sprintf(
			"select NEXT_TIME is null from mysql.tidb_mlog_purge_info where MLOG_ID = %d",
			mlogID,
		)).Check(testkit.Rows("1"))
	*/

	err := tk.ExecToErr("alter materialized view log on t purge immediate")
	require.ErrorContains(t, err, "PURGE IMMEDIATE is not supported for ALTER MATERIALIZED VIEW LOG")
	// meta is unchanged
	_, purgeMethod, purgeStartWith, purgeNext = getMLogMeta()
	require.Equal(t, "DEFERRED", purgeMethod)
	require.Equal(t, "", purgeStartWith)
	require.Equal(t, "DATE_ADD(NOW(), INTERVAL 25 MINUTE)", purgeNext)

	tk.MustExec("drop materialized view log on t")
}

func TestAlterMaterializedViewLogPurgeUpdatesNextTimeWithMLogAlterPrivilege(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 2 hour)")
	tk.MustExec("create user 'mv_alter_purge_u'@'%' identified by ''")
	tk.MustExec("create user 'mv_alter_purge_select_u'@'%' identified by ''")
	defer tk.MustExec("drop user 'mv_alter_purge_u'@'%'")
	defer tk.MustExec("drop user 'mv_alter_purge_select_u'@'%'")
	tk.MustExec("grant alter on test.`$mlog$t` to 'mv_alter_purge_u'@'%'")
	tk.MustExec("grant select on test.t to 'mv_alter_purge_select_u'@'%'")

	getMLogMeta := func() (int64, string, string, string) {
		is := dom.InfoSchema()
		mlogTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("$mlog$t"))
		require.NoError(t, err)
		require.NotNil(t, mlogTable.Meta().MaterializedViewLog)
		return mlogTable.Meta().ID,
			mlogTable.Meta().MaterializedViewLog.PurgeMethod,
			mlogTable.Meta().MaterializedViewLog.PurgeStartWith,
			mlogTable.Meta().MaterializedViewLog.PurgeNext
	}

	tkUser := testkit.NewTestKit(t, store)
	require.NoError(t, tkUser.Session().Auth(&auth.UserIdentity{Username: "mv_alter_purge_u", Hostname: "%"}, nil, nil, nil))
	tkUser.MustExec("alter materialized view log on test.t purge next date_add(now(), interval 25 minute)")

	tkSelectUser := testkit.NewTestKit(t, store)
	require.NoError(t, tkSelectUser.Session().Auth(&auth.UserIdentity{Username: "mv_alter_purge_select_u", Hostname: "%"}, nil, nil, nil))
	err := tkSelectUser.ExecToErr("alter materialized view log on test.t purge next date_add(now(), interval 30 minute)")
	require.ErrorContains(t, err, "ALTER command denied")

	mlogID, purgeMethod, purgeStartWith, purgeNext := getMLogMeta()
	require.Equal(t, "DEFERRED", purgeMethod)
	require.Equal(t, "", purgeStartWith)
	require.Equal(t, "DATE_ADD(NOW(), INTERVAL 25 MINUTE)", purgeNext)
	tk.MustQuery(fmt.Sprintf(
		"select NEXT_TIME is not null, NEXT_TIME > UTC_TIMESTAMP() + interval 15 minute, NEXT_TIME < UTC_TIMESTAMP() + interval 1 hour from mysql.tidb_mlog_purge_info where MLOG_ID = %d",
		mlogID,
	)).Check(testkit.Rows("1 1 1"))
}

func TestDropMaterializedViewLogPrivilege(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_drop_mlog_priv (a int)")
	tk.MustExec("create materialized view log on t_drop_mlog_priv (a)")
	tk.MustExec("create user 'u_drop_mlog_select'@'%'")
	tk.MustExec("create user 'u_drop_mlog_ok'@'%'")
	defer tk.MustExec("drop user 'u_drop_mlog_select'@'%'")
	defer tk.MustExec("drop user 'u_drop_mlog_ok'@'%'")
	tk.MustExec("grant select on test.t_drop_mlog_priv to 'u_drop_mlog_select'@'%'")
	tk.MustExec("grant drop on test.`$mlog$t_drop_mlog_priv` to 'u_drop_mlog_ok'@'%'")

	tkSelect := testkit.NewTestKit(t, store)
	require.NoError(t, tkSelect.Session().Auth(&auth.UserIdentity{Username: "u_drop_mlog_select", Hostname: "%"}, nil, nil, nil))
	err := tkSelect.ExecToErr("drop materialized view log on test.t_drop_mlog_priv")
	require.ErrorContains(t, err, "DROP command denied")

	tkDrop := testkit.NewTestKit(t, store)
	require.NoError(t, tkDrop.Session().Auth(&auth.UserIdentity{Username: "u_drop_mlog_ok", Hostname: "%"}, nil, nil, nil))
	tkDrop.MustExec("drop materialized view log on test.t_drop_mlog_priv")
}

func TestAlterMaterializedViewLogPurgeBestEffortInfoUpdateWarning(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tkLock := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tkLock.MustExec("use test")
	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 2 hour)")

	getMLogMeta := func() (int64, string, string, string) {
		is := dom.InfoSchema()
		mlogTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("$mlog$t"))
		require.NoError(t, err)
		require.NotNil(t, mlogTable.Meta().MaterializedViewLog)
		return mlogTable.Meta().ID,
			mlogTable.Meta().MaterializedViewLog.PurgeMethod,
			mlogTable.Meta().MaterializedViewLog.PurgeStartWith,
			mlogTable.Meta().MaterializedViewLog.PurgeNext
	}

	mlogID, purgeMethod, purgeStartWith, purgeNext := getMLogMeta()
	require.Equal(t, "DEFERRED", purgeMethod)
	require.Equal(t, "", purgeStartWith)
	require.Equal(t, "DATE_ADD(NOW(), INTERVAL 2 HOUR)", purgeNext)

	const expectedNextTime = "2031-01-02 03:04:05"
	tk.MustExec(fmt.Sprintf(
		"update mysql.tidb_mlog_purge_info set NEXT_TIME = cast('%s' as datetime) where MLOG_ID = %d",
		expectedNextTime,
		mlogID,
	))
	tkLock.MustExec("begin pessimistic")
	defer tkLock.MustExec("rollback")
	tkLock.MustExec(fmt.Sprintf(
		"update mysql.tidb_mlog_purge_info set NEXT_TIME = NEXT_TIME where MLOG_ID = %d",
		mlogID,
	))

	tk.MustExec("alter materialized view log on t purge next date_add(now(), interval 25 minute)")
	tk.MustQuery("show warnings").CheckContain(
		"alter materialized view log purge: metadata updated but failed to update mysql.tidb_mlog_purge_info.NEXT_TIME within 10s due to row lock contention",
	)

	_, purgeMethod, purgeStartWith, purgeNext = getMLogMeta()
	require.Equal(t, "DEFERRED", purgeMethod)
	require.Equal(t, "", purgeStartWith)
	require.Equal(t, "DATE_ADD(NOW(), INTERVAL 25 MINUTE)", purgeNext)
	tk.MustQuery(fmt.Sprintf(
		"select NEXT_TIME = cast('%s' as datetime) from mysql.tidb_mlog_purge_info where MLOG_ID = %d",
		expectedNextTime,
		mlogID,
	)).Check(testkit.Rows("1"))
}

func TestCreateMaterializedViewLogMetaColumnNameConflict(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_conflict (`_MLOG$_DML_TYPE` int, a int)")
	tk.MustGetErrCode("create materialized view log on t_conflict (`_MLOG$_DML_TYPE`, a)", errno.ErrDupFieldName)
}

func TestCreateMaterializedViewLogRejectNonBaseObject(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int)")
	tk.MustExec("create view v as select a from t")
	tk.MustExec("create sequence s")

	err := tk.ExecToErr("create materialized view log on v (a)")
	require.Equal(t, dbterror.ErrWrongObject.GenWithStackByArgs("test", "v", "BASE TABLE").Error(), err.Error())

	err = tk.ExecToErr("create materialized view log on s (a)")
	require.Equal(t, dbterror.ErrWrongObject.GenWithStackByArgs("test", "s", "BASE TABLE").Error(), err.Error())
}

func TestCreateMaterializedViewLogNameLengthByRune(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	maxName := strings.Repeat("表", 58)
	tk.MustExec(fmt.Sprintf("create table `%s` (a int)", maxName))
	tk.MustExec(fmt.Sprintf("create materialized view log on `%s` (a)", maxName))
	tk.MustQuery(fmt.Sprintf("select count(*) from information_schema.tables where table_schema='test' and table_name='%s'", "$mlog$"+maxName)).Check(testkit.Rows("1"))

	tooLongName := strings.Repeat("表", 59)
	tk.MustExec(fmt.Sprintf("create table `%s` (a int)", tooLongName))
	tk.MustGetErrCode(fmt.Sprintf("create materialized view log on `%s` (a)", tooLongName), errno.ErrTooLongIdent)
}

func TestCreateMaterializedViewLogUpdatesPlacementBundle(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create placement policy mlog_p followers=1")
	tk.MustExec("alter database test placement policy mlog_p")
	tk.MustExec("create table t_placement (a int)")
	tk.MustExec("create materialized view log on t_placement (a)")

	tk.MustQuery("show placement for table `$mlog$t_placement`").CheckContain("TABLE test.$mlog$t_placement")
	tk.MustQuery("show placement for table `$mlog$t_placement`").CheckContain("FOLLOWERS=1")
}

func TestTruncateMaterializedViewRelatedTablesRejected(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_truncate_mv (a int not null, b int)")
	tk.MustExec("create materialized view log on t_truncate_mv (a, b)")

	err := tk.ExecToErr("truncate table t_truncate_mv")
	require.ErrorContains(t, err, "TRUNCATE TABLE on base table with materialized view log")

	err = tk.ExecToErr("truncate table `$mlog$t_truncate_mv`")
	require.ErrorContains(t, err, "TRUNCATE TABLE on materialized view log table")

	tk.MustExec("create materialized view mv_truncate_mv (a, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, count(1) from t_truncate_mv group by a")

	err = tk.ExecToErr("truncate table mv_truncate_mv")
	require.ErrorContains(t, err, "TRUNCATE TABLE on materialized view table")

	err = tk.ExecToErr("truncate table `$mlog$t_truncate_mv`")
	require.ErrorContains(t, err, "TRUNCATE TABLE on materialized view log table")

	err = tk.ExecToErr("truncate table t_truncate_mv")
	require.ErrorContains(t, err, "TRUNCATE TABLE on base table with materialized view log")
}

func TestMaterializedViewRelatedTablesDDLRejected(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_ddl_mv (a int not null, b int)")
	tk.MustExec("create materialized view log on t_ddl_mv (a, b)")

	err := tk.ExecToErr("drop table t_ddl_mv")
	require.ErrorContains(t, err, "DROP TABLE on base table with materialized view log")
	err = tk.ExecToErr("rename table t_ddl_mv to t_ddl_mv2")
	require.ErrorContains(t, err, "RENAME TABLE on base table with materialized view log")
	err = tk.ExecToErr("drop table `$mlog$t_ddl_mv`")
	require.ErrorContains(t, err, "DROP TABLE on materialized view log table")
	err = tk.ExecToErr("rename table `$mlog$t_ddl_mv` to `$mlog$t_ddl_mv2`")
	require.ErrorContains(t, err, "RENAME TABLE on materialized view log table")

	tk.MustExec("create materialized view mv_ddl_mv (a, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, count(1) from t_ddl_mv group by a")

	tk.MustExec("alter table t_ddl_mv add column c int")
	err = tk.ExecToErr("alter table t_ddl_mv modify column a bigint")
	require.ErrorContains(t, err, "does not support changing charset/collation/nullability of group keys")
	err = tk.ExecToErr("drop table t_ddl_mv")
	require.ErrorContains(t, err, "DROP TABLE on base table with materialized view dependencies")
	err = tk.ExecToErr("rename table t_ddl_mv to t_ddl_mv2")
	require.ErrorContains(t, err, "RENAME TABLE on base table with materialized view dependencies")

	// Restricted MODIFY/CHANGE COLUMN should be allowed at ALTER TABLE entry, but still rejected on reorg/renaming.
	tk.MustExec("alter table t_ddl_mv modify column b bigint")
	err = tk.ExecToErr("alter table t_ddl_mv modify column b smallint")
	require.ErrorContains(t, err, "only supports no-reorg compatible type changes")
	err = tk.ExecToErr("alter table t_ddl_mv change column b b2 bigint")
	require.ErrorContains(t, err, "does not support renaming")

	err = tk.ExecToErr("alter table mv_ddl_mv add column x int")
	require.ErrorContains(t, err, "ALTER TABLE on materialized view table")
	err = tk.ExecToErr("drop table mv_ddl_mv")
	require.ErrorContains(t, err, "DROP TABLE on materialized view table")
	err = tk.ExecToErr("rename table mv_ddl_mv to mv_ddl_mv2")
	require.ErrorContains(t, err, "RENAME TABLE on materialized view table")

	err = tk.ExecToErr("alter table `$mlog$t_ddl_mv` set tiflash replica 1")
	if err != nil {
		require.NotContains(t, err.Error(), "ALTER TABLE on materialized view log table")
	}
	err = tk.ExecToErr("alter table `$mlog$t_ddl_mv` add index idx_mlog_b(b)")
	require.ErrorContains(t, err, "ALTER TABLE on materialized view log table")
	err = tk.ExecToErr("alter table `$mlog$t_ddl_mv` add column c int")
	require.ErrorContains(t, err, "ALTER TABLE on materialized view log table")
	err = tk.ExecToErr("create index idx_mlog_b_create on `$mlog$t_ddl_mv`(b)")
	require.ErrorContains(t, err, "CREATE INDEX on materialized view log table")
	err = tk.ExecToErr("create vector index idx_mlog_vec_create on `$mlog$t_ddl_mv` ((vec_cosine_distance(b))) using hnsw")
	require.ErrorContains(t, err, "CREATE INDEX on materialized view log table")
	err = tk.ExecToErr("drop index idx_mlog_b_create on `$mlog$t_ddl_mv`")
	require.ErrorContains(t, err, "DROP INDEX on materialized view log table")
}

func TestCreateVectorIndexOnMaterializedViewLogTableRejected(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomainWithSchemaLease(t, 100*time.Millisecond, mockstore.WithMockTiFlash(2))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_mlog_vec (id int, v vector(3))")
	tk.MustExec("create materialized view log on t_mlog_vec (v) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("alter table `$mlog$t_mlog_vec` set tiflash replica 1")

	err := tk.ExecToErr("create vector index idx_mlog_vec on `$mlog$t_mlog_vec`((vec_cosine_distance(v))) USING HNSW")
	require.Truef(t, dbterror.ErrGeneralUnsupportedDDL.Equal(err), "err %v", err)
	require.ErrorContains(t, err, "CREATE INDEX on materialized view log table")
}

func TestTruncateOrdinaryTableStillWorks(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_normal_truncate (a int)")
	tk.MustExec("insert into t_normal_truncate values (1), (2)")
	tk.MustExec("truncate table t_normal_truncate")
	tk.MustQuery("select count(*) from t_normal_truncate").Check(testkit.Rows("0"))
}

func TestDropMaterializedViewLogTableAfterBaseDropped(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists `$mlog$t_drop_seq`")
	tk.MustExec("drop table if exists t_drop_seq")

	tk.MustExec("create table t_drop_seq (a int)")
	tk.MustExec("create materialized view log on t_drop_seq (a)")
	tk.MustExec("drop table if exists t_drop_seq")
	tk.MustExec("drop table if exists `$mlog$t_drop_seq`")
}

func TestDropMaterializedViewLogRemovesPurgeState(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t_drop_mlog_purge_state (a int)")
	tk.MustExec("create materialized view log on t_drop_mlog_purge_state (a)")

	is := dom.InfoSchema()
	mlogTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("$mlog$t_drop_mlog_purge_state"))
	require.NoError(t, err)
	mlogID := mlogTable.Meta().ID

	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mlog_purge_info where MLOG_ID = %d", mlogID)).
		Check(testkit.Rows("1"))

	tk.MustExec("drop materialized view log on t_drop_mlog_purge_state")
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mlog_purge_info where MLOG_ID = %d", mlogID)).
		Check(testkit.Rows("0"))
}

func TestPurgeMaterializedViewLogNoDB(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustGetErrCode("purge materialized view log on t", errno.ErrNoDB)
}

func TestPurgeMaterializedViewLogMissingMLog(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_purge_no_mlog (a int)")
	err := tk.ExecToErr("purge materialized view log on t_purge_no_mlog")
	require.ErrorContains(t, err, "materialized view log does not exist")
}

func TestPurgeMaterializedViewLogUsesCurrentSessionMVMaintainMemQuota(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_purge_quota (id int primary key, v int)")
	tk.MustExec("create materialized view log on t_purge_quota (id, v) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("insert into t_purge_quota values (1, 10), (2, 20), (3, 30)")
	tk.MustExec("set @@session.tidb_mem_quota_query = 1073741824")
	tk.MustExec("set @@global.tidb_mv_maintain_mem_quota = 536870912")
	tk.MustExec("set @@session.tidb_mv_maintain_mem_quota = 268435456")
	defer tk.MustExec(fmt.Sprintf("set @@global.tidb_mv_maintain_mem_quota = %d", 2*1024*1024*1024))

	applied := false
	lastAppliedMemQuotaQuery := int64(0)
	lastAppliedMaintainQuota := int64(0)
	failpointName := "github.com/pingcap/tidb/pkg/executor/mvMaintainMemQuotaAppliedOnPurgeSession"
	require.NoError(t, failpoint.EnableCall(failpointName, func(memQuotaQuery int64, maintainMemQuota int64) {
		applied = true
		lastAppliedMemQuotaQuery = memQuotaQuery
		lastAppliedMaintainQuota = maintainMemQuota
	}))
	defer func() {
		require.NoError(t, failpoint.Disable(failpointName))
	}()

	tk.MustExec("purge materialized view log on t_purge_quota")
	require.True(t, applied)
	require.Equal(t, int64(268435456), lastAppliedMaintainQuota)
	require.Equal(t, lastAppliedMaintainQuota, lastAppliedMemQuotaQuery)

	tk.MustExec("insert into t_purge_quota values (4, 40)")
	applied = false
	lastAppliedMemQuotaQuery = 0
	lastAppliedMaintainQuota = 0
	mustExecInternal(t, tk, "purge materialized view log on t_purge_quota")
	require.True(t, applied)
	require.Equal(t, int64(268435456), lastAppliedMaintainQuota)
	require.Equal(t, lastAppliedMaintainQuota, lastAppliedMemQuotaQuery)
}

func TestPurgeMaterializedViewLogManualSQLFailsWhenApplyMaintenanceMemQuotaFails(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_purge_apply_quota_manual (id int primary key, v int)")
	tk.MustExec("create materialized view log on t_purge_apply_quota_manual (id, v) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("insert into t_purge_apply_quota_manual values (1, 10), (2, 20)")

	failpointName := "github.com/pingcap/tidb/pkg/executor/mockMVMaintenanceMemQuotaApplyError"
	require.NoError(t, failpoint.Enable(failpointName, "return(true)"))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable(failpointName))
	})

	err := tk.ExecToErr("purge materialized view log on t_purge_apply_quota_manual")
	require.ErrorContains(t, err, "mock mv maintenance mem quota apply error")
}

func TestPurgeMaterializedViewLogInternalSQLFallsBackWhenApplyMaintenanceMemQuotaFails(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_purge_apply_quota_internal (id int primary key, v int)")
	tk.MustExec("create materialized view log on t_purge_apply_quota_internal (id, v) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("insert into t_purge_apply_quota_internal values (1, 10), (2, 20)")
	tk.MustExec("set @@session.tidb_mem_quota_query = 1073741824")
	tk.MustExec("set @@session.tidb_mv_maintain_mem_quota = 268435456")

	applied := false
	gotMemQuotaQuery := int64(0)
	failpointName := "github.com/pingcap/tidb/pkg/executor/mockMVMaintenanceMemQuotaApplyError"
	require.NoError(t, failpoint.Enable(failpointName, "return(true)"))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable(failpointName))
	})
	appliedFailpointName := "github.com/pingcap/tidb/pkg/executor/mvMaintainMemQuotaAppliedOnPurgeSession"
	require.NoError(t, failpoint.EnableCall(appliedFailpointName, func(memQuotaQuery int64, maintainMemQuota int64) {
		applied = true
		gotMemQuotaQuery = memQuotaQuery
		require.Equal(t, int64(268435456), maintainMemQuota)
	}))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable(appliedFailpointName))
	})

	mustExecInternal(t, tk, "purge materialized view log on t_purge_apply_quota_internal")
	require.True(t, applied)
	require.Equal(t, int64(1073741824), gotMemQuotaQuery)
}
func TestPurgeMaterializedViewLogUsesCurrentSessionMVMaintainIsolationReadEngines(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_purge_isolation (id int primary key, v int)")
	tk.MustExec("create materialized view log on t_purge_isolation (id, v) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("insert into t_purge_isolation values (1, 10), (2, 20), (3, 30)")
	tk.MustExec(fmt.Sprintf("set @@session.%s = 'tikv,tiflash,tidb'", variable.TiDBIsolationReadEngines))
	tk.MustExec(fmt.Sprintf("set @@session.%s = 'tikv'", variable.TiDBMVMaintainIsolationReadEngines))

	applied := false
	gotIsolationReadEngines := ""
	failpointName := "github.com/pingcap/tidb/pkg/executor/mvMaintainIsolationReadEnginesAppliedOnPurgeSession"
	require.NoError(t, failpoint.EnableCall(failpointName, func(currentIsolationReadEngines string, targetIsolationReadEngines string) {
		applied = true
		gotIsolationReadEngines = currentIsolationReadEngines
		require.Equal(t, "tikv", targetIsolationReadEngines)
	}))
	defer func() {
		require.NoError(t, failpoint.Disable(failpointName))
	}()

	tk.MustExec("purge materialized view log on t_purge_isolation")
	require.True(t, applied)
	require.Equal(t, "tikv", gotIsolationReadEngines)
	tk.MustQuery(fmt.Sprintf("select @@session.%s", variable.TiDBIsolationReadEngines)).Check(testkit.Rows("tikv,tiflash,tidb"))

	tk.MustExec("insert into t_purge_isolation values (4, 40)")
	applied = false
	gotIsolationReadEngines = ""
	mustExecInternal(t, tk, "purge materialized view log on t_purge_isolation")
	require.True(t, applied)
	require.Equal(t, "tikv", gotIsolationReadEngines)
	tk.MustQuery(fmt.Sprintf("select @@session.%s", variable.TiDBIsolationReadEngines)).Check(testkit.Rows("tikv,tiflash,tidb"))
}

func TestPurgeMaterializedViewLogDefaultMVMaintainIsolationReadEnginesDoesNotInheritCurrentSession(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_purge_isolation_default (id int primary key, v int)")
	tk.MustExec("create materialized view log on t_purge_isolation_default (id, v) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("insert into t_purge_isolation_default values (1, 10), (2, 20), (3, 30)")

	defaultMaintainIsolationReadEngines := variable.GetSysVar(variable.TiDBMVMaintainIsolationReadEngines).Value
	currentIsolationReadEngines := differentIsolationReadEnginesForTest(defaultMaintainIsolationReadEngines)
	tk.MustExec(fmt.Sprintf("set @@session.%s = '%s'", variable.TiDBIsolationReadEngines, currentIsolationReadEngines))

	applied := false
	gotIsolationReadEngines := ""
	failpointName := "github.com/pingcap/tidb/pkg/executor/mvMaintainIsolationReadEnginesAppliedOnPurgeSession"
	require.NoError(t, failpoint.EnableCall(failpointName, func(currentIsolationReadEngines string, targetIsolationReadEngines string) {
		applied = true
		gotIsolationReadEngines = currentIsolationReadEngines
		require.Equal(t, defaultMaintainIsolationReadEngines, targetIsolationReadEngines)
	}))
	defer func() {
		require.NoError(t, failpoint.Disable(failpointName))
	}()

	tk.MustExec("purge materialized view log on t_purge_isolation_default")
	require.True(t, applied)
	require.Equal(t, defaultMaintainIsolationReadEngines, gotIsolationReadEngines)
	tk.MustQuery(fmt.Sprintf("select @@session.%s", variable.TiDBIsolationReadEngines)).Check(testkit.Rows(currentIsolationReadEngines))
}

func TestPurgeMaterializedViewLogPrivilege(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_purge_priv (a int)")
	tk.MustExec("create materialized view log on t_purge_priv (a) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_purge_priv (a, cnt) as select a, count(1) from t_purge_priv group by a")
	tk.MustExec("create user 'u1'@'%'")
	tk.MustExec("create user 'u2'@'%'")
	tk.MustExec("create user 'u3'@'%'")
	defer tk.MustExec("drop user 'u1'@'%'")
	defer tk.MustExec("drop user 'u2'@'%'")
	defer tk.MustExec("drop user 'u3'@'%'")
	tk.MustExec("grant select on test.t_purge_priv to 'u1'@'%'")
	tk.MustExec("grant operate view on test.`$mlog$t_purge_priv` to 'u2'@'%'")
	tk.MustExec("grant operate view on *.* to 'u3'@'%'")

	tkUser := testkit.NewTestKit(t, store)
	require.NoError(t, tkUser.Session().Auth(&auth.UserIdentity{Username: "u1", Hostname: "%"}, nil, nil, nil))
	tkUser.MustExec("use test")

	err := tkUser.ExecToErr("purge materialized view log on t_purge_priv")
	require.ErrorContains(t, err, "OPERATE VIEW command denied")

	tkUser = testkit.NewTestKit(t, store)
	require.NoError(t, tkUser.Session().Auth(&auth.UserIdentity{Username: "u2", Hostname: "%"}, nil, nil, nil))
	tkUser.MustExec("use test")
	tkUser.MustExec("purge materialized view log on t_purge_priv")

	tk.MustExec("drop materialized view mv_purge_priv")
	tkUser = testkit.NewTestKit(t, store)
	require.NoError(t, tkUser.Session().Auth(&auth.UserIdentity{Username: "u3", Hostname: "%"}, nil, nil, nil))
	tkUser.MustExec("use test")
	tkUser.MustExec("purge materialized view log on t_purge_priv")
}

func TestPurgeMaterializedViewLogDisallowExplicitTransaction(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t_mlog_purge_txn (a int not null, b int not null)")
	tk.MustExec("create materialized view log on t_mlog_purge_txn (a, b) purge next date_add(now(), interval 1 hour)")

	tk.MustExec("begin")
	tk.MustGetErrMsg(
		"purge materialized view log on t_mlog_purge_txn",
		"cannot run PURGE MATERIALIZED VIEW LOG in explicit transaction",
	)
	tk.MustExec("rollback")

	tk.MustExec(`prepare stmt from "purge materialized view log on t_mlog_purge_txn"`)
	defer tk.MustExec("deallocate prepare stmt")
	tk.MustExec("begin")
	tk.MustGetErrMsg("execute stmt", "cannot run PURGE MATERIALIZED VIEW LOG in explicit transaction")
	tk.MustExec("rollback")
}

func TestPurgeMaterializedViewLogCancelWatcherUsesHistRequest(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_purge_cancel_watch (id int primary key, v int)")
	tk.MustExec("create materialized view log on t_purge_cancel_watch (id, v) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("insert into t_purge_cancel_watch values (1, 10)")

	is := dom.InfoSchema()
	mlogTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("$mlog$t_purge_cancel_watch"))
	require.NoError(t, err)
	mlogID := mlogTable.Meta().ID

	pollIntervalFailpoint := "github.com/pingcap/tidb/pkg/executor/mockMVTaskMonitorPollInterval"
	require.NoError(t, failpoint.Enable(pollIntervalFailpoint, "return(50)"))
	defer func() {
		require.NoError(t, failpoint.Disable(pollIntervalFailpoint))
	}()

	pauseFailpoint := "github.com/pingcap/tidb/pkg/executor/pausePurgeMaterializedViewLogAfterInsertPurgeHistRunning"
	require.NoError(t, failpoint.Enable(pauseFailpoint, "pause"))
	paused := true
	defer func() {
		if paused {
			require.NoError(t, failpoint.Disable(pauseFailpoint))
		}
	}()

	errCh := make(chan error, 1)
	go func() {
		tkPurge := testkit.NewTestKit(t, store)
		tkPurge.MustExec("use test")
		errCh <- tkPurge.ExecToErr("purge materialized view log on t_purge_cancel_watch")
	}()

	require.Eventually(t, func() bool {
		rows := tk.MustQuery(fmt.Sprintf(
			"select count(*) from mysql.tidb_mlog_purge_hist where MLOG_ID = %d and PURGE_STATUS = 'running'",
			mlogID,
		)).Rows()
		return fmt.Sprint(rows[0][0]) == "1"
	}, 10*time.Second, 100*time.Millisecond)

	requester := "'purge_watcher_req'@'stage-d'"
	requestedCh := waitMVTaskCancelWatcherRequested(t, "mlog-purge-")
	tk.MustExec(
		`UPDATE mysql.tidb_mlog_purge_hist
SET CANCEL_REQUESTED_AT = NOW(6),
	CANCEL_REQUESTED_BY = ?
WHERE MLOG_ID = ?
  AND PURGE_STATUS = 'running'
  AND CANCEL_REQUESTED_AT IS NULL`,
		requester,
		mlogID,
	)
	select {
	case <-requestedCh:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for purge cancel watcher to observe request")
	}

	require.NoError(t, failpoint.Disable(pauseFailpoint))
	paused = false

	select {
	case err := <-errCh:
		require.Error(t, err)
		require.ErrorContains(t, err, "materialized view task canceled manually")
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for purge to finish")
	}

	tk.MustQuery(fmt.Sprintf(
		"select PURGE_STATUS, PURGE_METHOD, PURGE_ENDTIME is not null from mysql.tidb_mlog_purge_hist where MLOG_ID = %d order by PURGE_JOB_ID desc limit 1",
		mlogID,
	)).Check(testkit.Rows("failed manual 1"))
	tk.MustQuery(fmt.Sprintf(
		"select count(*) from mysql.tidb_mlog_purge_hist where MLOG_ID = %d and PURGE_STATUS = 'running'",
		mlogID,
	)).Check(testkit.Rows("0"))
	reasonRows := tk.MustQuery(fmt.Sprintf(
		"select PURGE_FAILED_REASON from mysql.tidb_mlog_purge_hist where MLOG_ID = %d order by PURGE_JOB_ID desc limit 1",
		mlogID,
	)).Rows()
	require.Len(t, reasonRows, 1)
	require.Equal(t, "cancelled manually by "+requester, fmt.Sprint(reasonRows[0][0]))
}

func TestCancelMaterializedViewLogPurgeJob(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_purge_cancel_job (id int primary key, v int not null)")
	tk.MustExec("create materialized view log on t_purge_cancel_job (id, v) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("insert into t_purge_cancel_job values (1, 10)")
	tk.MustExec("create materialized view mv_purge_cancel_job (id, s, cnt) as select id, sum(v), count(1) from t_purge_cancel_job group by id")

	is := dom.InfoSchema()
	mlogTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("$mlog$t_purge_cancel_job"))
	require.NoError(t, err)
	mlogID := mlogTable.Meta().ID

	pollIntervalFailpoint := "github.com/pingcap/tidb/pkg/executor/mockMVTaskMonitorPollInterval"
	require.NoError(t, failpoint.Enable(pollIntervalFailpoint, "return(50)"))
	defer func() {
		require.NoError(t, failpoint.Disable(pollIntervalFailpoint))
	}()

	pauseFailpoint := "github.com/pingcap/tidb/pkg/executor/pausePurgeMaterializedViewLogAfterInsertPurgeHistRunning"
	require.NoError(t, failpoint.Enable(pauseFailpoint, "pause"))
	paused := true
	defer func() {
		if paused {
			require.NoError(t, failpoint.Disable(pauseFailpoint))
		}
	}()

	errCh := make(chan error, 1)
	go func() {
		tkPurge := testkit.NewTestKit(t, store)
		tkPurge.MustExec("use test")
		errCh <- tkPurge.ExecToErr("purge materialized view log on t_purge_cancel_job")
	}()

	require.Eventually(t, func() bool {
		rows := tk.MustQuery(fmt.Sprintf(
			"select count(*) from mysql.tidb_mlog_purge_hist where MLOG_ID = %d and PURGE_STATUS = 'running'",
			mlogID,
		)).Rows()
		return fmt.Sprint(rows[0][0]) == "1"
	}, 10*time.Second, 100*time.Millisecond)

	jobIDRows := tk.MustQuery(fmt.Sprintf(
		"select PURGE_JOB_ID from mysql.tidb_mlog_purge_hist where MLOG_ID = %d and PURGE_STATUS = 'running' order by PURGE_JOB_ID desc limit 1",
		mlogID,
	)).Rows()
	require.Len(t, jobIDRows, 1)
	jobID := fmt.Sprint(jobIDRows[0][0])

	tk.MustExec("create user 'mv_purge_cancel_u'@'%' identified by ''")
	defer tk.MustExec("drop user 'mv_purge_cancel_u'@'%'")

	tkCancel := testkit.NewTestKit(t, store)
	require.NoError(t, tkCancel.Session().Auth(&auth.UserIdentity{Username: "mv_purge_cancel_u", Hostname: "%"}, nil, nil, nil))
	err = tkCancel.ExecToErr(fmt.Sprintf("cancel materialized view log purge job %s", jobID))
	require.ErrorContains(t, err, "cannot cancel materialized view log purge job")
	tk.MustExec("grant operate view on test.`$mlog$t_purge_cancel_job` to 'mv_purge_cancel_u'@'%'")
	requestedCh := waitMVTaskCancelWatcherRequested(t, "mlog-purge-")
	tkCancel.MustExec(fmt.Sprintf("cancel materialized view log purge job %s", jobID))
	select {
	case <-requestedCh:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for purge cancel watcher to observe request")
	}

	require.NoError(t, failpoint.Disable(pauseFailpoint))
	paused = false

	select {
	case err := <-errCh:
		require.Error(t, err)
		require.ErrorContains(t, err, "materialized view task canceled manually")
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for purge to finish")
	}

	tk.MustQuery(fmt.Sprintf(
		"select PURGE_STATUS, PURGE_METHOD, PURGE_ENDTIME is not null from mysql.tidb_mlog_purge_hist where MLOG_ID = %d order by PURGE_JOB_ID desc limit 1",
		mlogID,
	)).Check(testkit.Rows("failed manual 1"))
	reasonRows := tk.MustQuery(fmt.Sprintf(
		"select PURGE_FAILED_REASON from mysql.tidb_mlog_purge_hist where MLOG_ID = %d order by PURGE_JOB_ID desc limit 1",
		mlogID,
	)).Rows()
	require.Len(t, reasonRows, 1)
	require.Equal(t, "cancelled manually by 'mv_purge_cancel_u'@'%'", fmt.Sprint(reasonRows[0][0]))
}

func TestPurgeMaterializedViewLogRunningHistHeartbeat(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_purge_heartbeat (id int primary key, v int)")
	tk.MustExec("create materialized view log on t_purge_heartbeat (id, v) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("insert into t_purge_heartbeat values (1, 10), (2, 20)")

	is := dom.InfoSchema()
	mlogTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("$mlog$t_purge_heartbeat"))
	require.NoError(t, err)
	mlogID := mlogTable.Meta().ID

	pollIntervalFailpoint := "github.com/pingcap/tidb/pkg/executor/mockMVTaskMonitorPollInterval"
	require.NoError(t, failpoint.Enable(pollIntervalFailpoint, "return(50)"))
	defer func() {
		require.NoError(t, failpoint.Disable(pollIntervalFailpoint))
	}()
	heartbeatIntervalFailpoint := "github.com/pingcap/tidb/pkg/executor/mockMVTaskHistHeartbeatInterval"
	require.NoError(t, failpoint.Enable(heartbeatIntervalFailpoint, "return(50)"))
	defer func() {
		require.NoError(t, failpoint.Disable(heartbeatIntervalFailpoint))
	}()

	pauseFailpoint := "github.com/pingcap/tidb/pkg/executor/pausePurgeMaterializedViewLogAfterInsertPurgeHistRunning"
	require.NoError(t, failpoint.Enable(pauseFailpoint, "pause"))
	paused := true
	defer func() {
		if paused {
			require.NoError(t, failpoint.Disable(pauseFailpoint))
		}
	}()

	errCh := make(chan error, 1)
	go func() {
		tkPurge := testkit.NewTestKit(t, store)
		tkPurge.MustExec("use test")
		errCh <- tkPurge.ExecToErr("purge materialized view log on t_purge_heartbeat")
	}()

	var firstHeartbeat string
	require.Eventually(t, func() bool {
		rows := tk.MustQuery(fmt.Sprintf(
			"select cast(LAST_HEARTBEAT_AT as char) from mysql.tidb_mlog_purge_hist where MLOG_ID = %d and PURGE_STATUS = 'running' order by PURGE_JOB_ID desc limit 1",
			mlogID,
		)).Rows()
		if len(rows) == 0 || rows[0][0] == nil {
			return false
		}
		firstHeartbeat = fmt.Sprint(rows[0][0])
		return firstHeartbeat != ""
	}, 10*time.Second, 100*time.Millisecond)

	require.Eventually(t, func() bool {
		rows := tk.MustQuery(fmt.Sprintf(
			"select cast(LAST_HEARTBEAT_AT as char) from mysql.tidb_mlog_purge_hist where MLOG_ID = %d and PURGE_STATUS = 'running' order by PURGE_JOB_ID desc limit 1",
			mlogID,
		)).Rows()
		if len(rows) == 0 || rows[0][0] == nil {
			return false
		}
		return fmt.Sprint(rows[0][0]) != firstHeartbeat
	}, 10*time.Second, 100*time.Millisecond)

	require.NoError(t, failpoint.Disable(pauseFailpoint))
	paused = false

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for purge to finish")
	}

	tk.MustQuery(fmt.Sprintf(
		"select PURGE_STATUS, LAST_HEARTBEAT_AT is not null from mysql.tidb_mlog_purge_hist where MLOG_ID = %d order by PURGE_JOB_ID desc limit 1",
		mlogID,
	)).Check(testkit.Rows("success 1"))
}

func TestPurgeMaterializedViewLogLockRowMissing(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_purge_lock_row_missing (a int)")
	tk.MustExec("create materialized view log on t_purge_lock_row_missing (a) purge next date_add(now(), interval 1 hour)")

	is := dom.InfoSchema()
	mlogTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("$mlog$t_purge_lock_row_missing"))
	require.NoError(t, err)
	mlogID := mlogTable.Meta().ID

	tk.MustExec(fmt.Sprintf("delete from mysql.tidb_mlog_purge_info where mlog_id = %d", mlogID))
	err = tk.ExecToErr("purge materialized view log on t_purge_lock_row_missing")
	require.ErrorContains(t, err, "mlog purge lock row does not exist")
}

func TestPurgeMaterializedViewLogNowaitConflict(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk2.MustExec("use test")
	tk1.MustExec("create table t_purge_nowait_conflict (a int)")
	tk1.MustExec("create materialized view log on t_purge_nowait_conflict (a) purge next date_add(now(), interval 1 hour)")

	is := dom.InfoSchema()
	mlogTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("$mlog$t_purge_nowait_conflict"))
	require.NoError(t, err)
	mlogID := mlogTable.Meta().ID

	tk1.MustExec("begin pessimistic")
	tk1.MustQuery(fmt.Sprintf("select 1 from mysql.tidb_mlog_purge_info where mlog_id = %d for update", mlogID)).
		Check(testkit.Rows("1"))
	rolledBack := false
	defer func() {
		if !rolledBack {
			tk1.MustExec("rollback")
		}
	}()

	err = tk2.ExecToErr("purge materialized view log on t_purge_nowait_conflict")
	require.ErrorContains(t, err, "another purge is running")

	tk1.MustExec("rollback")
	rolledBack = true
	tk2.MustExec("purge materialized view log on t_purge_nowait_conflict")
}

func TestPurgeMaterializedViewLogBatchDelete(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t_purge_batch_delete (id int primary key, v int)")
	tk.MustExec("create materialized view log on t_purge_batch_delete (id, v) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("insert into t_purge_batch_delete values (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)")

	is := dom.InfoSchema()
	mlogTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("$mlog$t_purge_batch_delete"))
	require.NoError(t, err)
	mlogID := mlogTable.Meta().ID

	tk.MustQuery("select count(*) from `$mlog$t_purge_batch_delete`").Check(testkit.Rows("5"))
	maxCommitTS, err := strconv.ParseUint(fmt.Sprint(tk.MustQuery("select max(_tidb_commit_ts) from `$mlog$t_purge_batch_delete`").Rows()[0][0]), 10, 64)
	require.NoError(t, err)
	tk.MustExec("set @@session.tidb_mlog_purge_batch_size = 2")
	beforeDelete := readAffectedRowsMetricValue(t, "Delete")
	beforePurgeMVLog := readAffectedRowsMetricValue(t, "PurgeMVLog")
	tk.MustExec("purge materialized view log on t_purge_batch_delete")
	require.Equal(t, uint64(5), tk.Session().AffectedRows())
	tk.CheckLastMessage("Rows inserted: 0  Updated: 0  Deleted: 5")
	require.Equal(t, 0.0, readAffectedRowsMetricValue(t, "Delete")-beforeDelete)
	require.Equal(t, 5.0, readAffectedRowsMetricValue(t, "PurgeMVLog")-beforePurgeMVLog)

	tk.MustQuery("select count(*) from `$mlog$t_purge_batch_delete`").Check(testkit.Rows("0"))
	tk.MustQuery(fmt.Sprintf(
		"select PURGE_STATUS, PURGE_ROWS, PURGE_DURATION_SEC = cast(timestampdiff(microsecond, PURGE_TIME, PURGE_ENDTIME) as decimal(18,6)) / 1000000, PURGE_DURATION_SEC >= 0 "+
			"from mysql.tidb_mlog_purge_hist where MLOG_ID = %d order by PURGE_JOB_ID desc limit 1",
		mlogID,
	)).Check(testkit.Rows("success 5 1 1"))
	tk.MustQuery(fmt.Sprintf(
		"select PURGE_CUTOFF_TSO is not null, PURGE_CUTOFF_TSO >= %d from mysql.tidb_mlog_purge_hist where MLOG_ID = %d order by PURGE_JOB_ID desc limit 1",
		maxCommitTS,
		mlogID,
	)).Check(testkit.Rows("1 1"))
	tk.MustQuery(fmt.Sprintf("select BASE_TABLE_SCHEMA, BASE_TABLE_NAME from mysql.tidb_mlog_purge_hist where MLOG_ID = %d order by PURGE_JOB_ID desc limit 1", mlogID)).
		Check(testkit.Rows("test t_purge_batch_delete"))
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mlog_purge_hist where BASE_TABLE_SCHEMA = 'TEST' and BASE_TABLE_NAME = 'T_PURGE_BATCH_DELETE' and MLOG_ID = %d", mlogID)).
		Check(testkit.Rows("1"))
	tk.MustQuery(fmt.Sprintf("select LAST_PURGED_TSO is not null, LAST_PURGED_TSO >= %d from mysql.tidb_mlog_purge_info where MLOG_ID = %d", maxCommitTS, mlogID)).
		Check(testkit.Rows("1 1"))
}

func TestPurgeMaterializedViewLogLastPurgedTSOShortCircuit(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t_purge_short_circuit (a int not null, b int)")
	tk.MustExec("create materialized view log on t_purge_short_circuit (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_purge_short_circuit (a, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, count(1) from t_purge_short_circuit group by a")
	tk.MustExec("insert into t_purge_short_circuit values (1, 10), (1, 20), (2, 30)")

	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv_purge_short_circuit"))
	require.NoError(t, err)
	mvID := mvTable.Meta().ID
	mlogTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("$mlog$t_purge_short_circuit"))
	require.NoError(t, err)
	mlogID := mlogTable.Meta().ID

	maxCommitTS, err := strconv.ParseUint(fmt.Sprint(tk.MustQuery("select max(_tidb_commit_ts) from `$mlog$t_purge_short_circuit`").Rows()[0][0]), 10, 64)
	require.NoError(t, err)
	tk.MustExec(fmt.Sprintf("update mysql.tidb_mview_refresh_info set LAST_SUCCESS_READ_TSO = %d where MVIEW_ID = %d", maxCommitTS, mvID))

	tk.MustExec("set @@session.tidb_mlog_purge_batch_size = 2")
	tk.MustExec("purge materialized view log on t_purge_short_circuit")
	tk.MustQuery("select count(*) from `$mlog$t_purge_short_circuit`").Check(testkit.Rows("0"))
	tk.MustQuery(fmt.Sprintf("select LAST_PURGED_TSO from mysql.tidb_mlog_purge_info where MLOG_ID = %d", mlogID)).
		Check(testkit.Rows(fmt.Sprintf("%d", maxCommitTS)))

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/mockPurgeMaterializedViewLogDeleteErr", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/mockPurgeMaterializedViewLogDeleteErr"))
	}()

	// `LAST_PURGED_TSO >= safe_purge_tso` should short-circuit before delete SQL execution.
	tk.MustExec("purge materialized view log on t_purge_short_circuit")
	tk.MustQuery("select count(*) from `$mlog$t_purge_short_circuit`").Check(testkit.Rows("0"))
	tk.MustQuery(fmt.Sprintf("select PURGE_STATUS, PURGE_ROWS from mysql.tidb_mlog_purge_hist where MLOG_ID = %d order by PURGE_JOB_ID desc limit 1", mlogID)).
		Check(testkit.Rows("success 0"))
	tk.MustQuery(fmt.Sprintf("select LAST_PURGED_TSO from mysql.tidb_mlog_purge_info where MLOG_ID = %d", mlogID)).
		Check(testkit.Rows(fmt.Sprintf("%d", maxCommitTS)))
}

func TestPurgeMaterializedViewLogSkipsWhenCutoffFenceWouldGoBackward(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t_purge_cutoff_fence (a int not null, b int)")
	tk.MustExec("create materialized view log on t_purge_cutoff_fence (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_purge_cutoff_fence (a, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, count(1) from t_purge_cutoff_fence group by a")
	tk.MustExec("insert into t_purge_cutoff_fence values (1, 10), (1, 20), (2, 30)")

	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv_purge_cutoff_fence"))
	require.NoError(t, err)
	mvID := mvTable.Meta().ID
	mlogTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("$mlog$t_purge_cutoff_fence"))
	require.NoError(t, err)
	mlogID := mlogTable.Meta().ID

	maxCommitTS, err := strconv.ParseUint(fmt.Sprint(tk.MustQuery("select max(_tidb_commit_ts) from `$mlog$t_purge_cutoff_fence`").Rows()[0][0]), 10, 64)
	require.NoError(t, err)
	tk.MustExec(fmt.Sprintf("update mysql.tidb_mview_refresh_info set LAST_SUCCESS_READ_TSO = %d where MVIEW_ID = %d", maxCommitTS, mvID))

	cutoffFenceTSO := maxCommitTS + 1000
	tk.MustExec(fmt.Sprintf(
		`insert into mysql.tidb_mlog_purge_hist (
			PURGE_JOB_ID, MLOG_ID, BASE_TABLE_SCHEMA, BASE_TABLE_NAME, PURGE_METHOD,
			PURGE_TIME, PURGE_ROWS, PURGE_STATUS, PURGE_CUTOFF_TSO, LAST_HEARTBEAT_AT
		) values (
			%[1]d, %[2]d, 'test', 't_purge_cutoff_fence', 'manual',
			now(6), 0, 'success', %[1]d, now(6)
		)`,
		cutoffFenceTSO,
		mlogID,
	))

	tk.MustExec("purge materialized view log on t_purge_cutoff_fence")
	require.Equal(t, uint64(0), tk.Session().AffectedRows())
	tk.CheckLastMessage("Rows inserted: 0  Updated: 0  Deleted: 0")

	tk.MustQuery("select count(*) from `$mlog$t_purge_cutoff_fence`").Check(testkit.Rows("3"))
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mlog_purge_hist where MLOG_ID = %d", mlogID)).
		Check(testkit.Rows("1"))
	tk.MustQuery(fmt.Sprintf("select PURGE_CUTOFF_TSO from mysql.tidb_mlog_purge_hist where MLOG_ID = %d order by PURGE_JOB_ID desc limit 1", mlogID)).
		Check(testkit.Rows(fmt.Sprintf("%d", cutoffFenceTSO)))
	tk.MustQuery(fmt.Sprintf("select LAST_PURGED_TSO is null from mysql.tidb_mlog_purge_info where MLOG_ID = %d", mlogID)).
		Check(testkit.Rows("1"))
}

func TestPurgeMaterializedViewLogDeleteErrorNoDirtyWrite(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t_purge_delete_err (id int primary key, v int)")
	tk.MustExec("create materialized view log on t_purge_delete_err (id, v) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("insert into t_purge_delete_err values (1, 10), (2, 20), (3, 30)")

	is := dom.InfoSchema()
	mlogTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("$mlog$t_purge_delete_err"))
	require.NoError(t, err)
	mlogID := mlogTable.Meta().ID

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/mockPurgeMaterializedViewLogDeleteErr", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/mockPurgeMaterializedViewLogDeleteErr"))
	}()

	err = tk.ExecToErr("purge materialized view log on t_purge_delete_err")
	require.ErrorContains(t, err, "mock purge mlog delete error")

	tk.MustQuery("select count(*) from `$mlog$t_purge_delete_err`").Check(testkit.Rows("3"))
	tk.MustQuery(fmt.Sprintf("select PURGE_STATUS, PURGE_ROWS, PURGE_ENDTIME is not null, PURGE_FAILED_REASON like '%%mock purge mlog delete error%%' from mysql.tidb_mlog_purge_hist where MLOG_ID = %d order by PURGE_JOB_ID desc limit 1", mlogID)).
		Check(testkit.Rows("failed 0 1 1"))
}

func TestPurgeMaterializedViewLogEarlyFailureWritesHist(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t_purge_early_fail (id int primary key, v int)")
	tk.MustExec("create materialized view log on t_purge_early_fail (id, v) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("insert into t_purge_early_fail values (1, 10), (2, 20), (3, 30)")

	is := dom.InfoSchema()
	mlogTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("$mlog$t_purge_early_fail"))
	require.NoError(t, err)
	mlogID := mlogTable.Meta().ID

	histCountBeforeRows := tk.MustQuery(fmt.Sprintf(
		"select count(*) from mysql.tidb_mlog_purge_hist where MLOG_ID = %d",
		mlogID,
	)).Rows()
	require.Len(t, histCountBeforeRows, 1)
	require.Len(t, histCountBeforeRows[0], 1)
	histCountBefore, err := strconv.Atoi(fmt.Sprintf("%v", histCountBeforeRows[0][0]))
	require.NoError(t, err)

	const failpointName = "github.com/pingcap/tidb/pkg/executor/mockPurgeMaterializedViewLogErrorBeforeInsertHist"
	require.NoError(t, failpoint.Enable(failpointName, `return("mock early purge failure")`))
	defer func() {
		require.NoError(t, failpoint.Disable(failpointName))
	}()

	err = tk.ExecToErr("purge materialized view log on t_purge_early_fail")
	require.Error(t, err)
	require.ErrorContains(t, err, "mock early purge failure")

	tk.MustQuery(fmt.Sprintf(
		"select count(*) from mysql.tidb_mlog_purge_hist where MLOG_ID = %d",
		mlogID,
	)).Check(testkit.Rows(fmt.Sprintf("%d", histCountBefore+1)))
	tk.MustQuery(fmt.Sprintf(
		"select PURGE_STATUS, PURGE_METHOD = 'manual', PURGE_ROWS, PURGE_TIME is not null, PURGE_ENDTIME is not null, "+
			"PURGE_DURATION_SEC = cast(timestampdiff(microsecond, PURGE_TIME, PURGE_ENDTIME) as decimal(18,6)) / 1000000, PURGE_FAILED_REASON is not null "+
			"from mysql.tidb_mlog_purge_hist where MLOG_ID = %d order by PURGE_JOB_ID desc limit 1",
		mlogID,
	)).Check(testkit.Rows("failed 1 0 1 1 1 1"))
	tk.MustQuery(fmt.Sprintf(
		"select count(*) from mysql.tidb_mlog_purge_hist where MLOG_ID = %d and PURGE_STATUS = 'running'",
		mlogID,
	)).Check(testkit.Rows("0"))
	reasonRow := tk.MustQuery(fmt.Sprintf(
		"select PURGE_FAILED_REASON from mysql.tidb_mlog_purge_hist where MLOG_ID = %d order by PURGE_JOB_ID desc limit 1",
		mlogID,
	)).Rows()
	require.Len(t, reasonRow, 1)
	require.Contains(t, fmt.Sprintf("%v", reasonRow[0][0]), "mock early purge failure")
}

func TestPurgeMaterializedViewLogFinalizeFailureAfterCommitIsWarning(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t_purge_finalize_warn (id int primary key, v int)")
	tk.MustExec("create materialized view log on t_purge_finalize_warn (id, v) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("insert into t_purge_finalize_warn values (1, 10), (2, 20), (3, 30)")

	is := dom.InfoSchema()
	mlogTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("$mlog$t_purge_finalize_warn"))
	require.NoError(t, err)
	mlogID := mlogTable.Meta().ID

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/mockPurgeMaterializedViewLogFinalizeSuccessErr", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/mockPurgeMaterializedViewLogFinalizeSuccessErr"))
	}()

	tk.MustExec("purge materialized view log on t_purge_finalize_warn")
	tk.MustQuery("select count(*) from `$mlog$t_purge_finalize_warn`").Check(testkit.Rows("0"))
	tk.MustQuery(fmt.Sprintf("select PURGE_STATUS, PURGE_ENDTIME is null from mysql.tidb_mlog_purge_hist where MLOG_ID = %d order by PURGE_JOB_ID desc limit 1", mlogID)).
		Check(testkit.Rows("running 1"))
}

func TestPurgeMaterializedViewLogFinalizeRetrySucceeds(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t_purge_finalize_retry (id int primary key, v int)")
	tk.MustExec("create materialized view log on t_purge_finalize_retry (id, v) purge next date_add(now(), interval 1 hour)")

	is := dom.InfoSchema()
	mlogTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("$mlog$t_purge_finalize_retry"))
	require.NoError(t, err)
	mlogID := mlogTable.Meta().ID

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/mockUpdateMaterializedViewLogPurgeStateErr", "1*return(true)->return(false)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/mockUpdateMaterializedViewLogPurgeStateErr"))
	}()

	tk.MustExec("purge materialized view log on t_purge_finalize_retry")
	tk.MustQuery(fmt.Sprintf("select PURGE_STATUS, PURGE_ENDTIME is not null, PURGE_ROWS from mysql.tidb_mlog_purge_hist where MLOG_ID = %d order by PURGE_JOB_ID desc limit 1", mlogID)).
		Check(testkit.Rows("success 1 0"))
}

func TestPurgeMaterializedViewLogFinalizeFailureUsesWithoutCancel(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tkObserver := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tkObserver.MustExec("use test")

	tk.MustExec("create table t_purge_cancel_finalize (id int primary key, v int)")
	tk.MustExec("create materialized view log on t_purge_cancel_finalize (id, v) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("insert into t_purge_cancel_finalize values (1, 10)")

	is := dom.InfoSchema()
	mlogTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("$mlog$t_purge_cancel_finalize"))
	require.NoError(t, err)
	mlogID := mlogTable.Meta().ID

	pauseFailpoint := "github.com/pingcap/tidb/pkg/executor/pausePurgeMaterializedViewLogAfterInsertPurgeHistRunning"
	require.NoError(t, failpoint.Enable(pauseFailpoint, "pause"))
	paused := true
	defer func() {
		if paused {
			require.NoError(t, failpoint.Disable(pauseFailpoint))
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		_, execErr := tk.ExecWithContext(ctx, "purge materialized view log on t_purge_cancel_finalize")
		errCh <- execErr
	}()

	require.Eventually(t, func() bool {
		rows := tkObserver.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mlog_purge_hist where MLOG_ID = %d and PURGE_STATUS = 'running'", mlogID)).Rows()
		return fmt.Sprint(rows[0][0]) == "1"
	}, 5*time.Second, 100*time.Millisecond)

	cancel()
	require.NoError(t, failpoint.Disable(pauseFailpoint))
	paused = false

	err = <-errCh
	require.Error(t, err)
	require.ErrorContains(t, err, "context canceled")

	tkObserver.MustQuery(fmt.Sprintf("select PURGE_STATUS, PURGE_ENDTIME is not null, PURGE_FAILED_REASON like '%%context canceled%%' from mysql.tidb_mlog_purge_hist where MLOG_ID = %d order by PURGE_JOB_ID desc limit 1", mlogID)).
		Check(testkit.Rows("failed 1 1"))
}

func TestPurgeMaterializedViewLogDeleteErrorAfterPartialSuccess(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t_purge_partial_delete_err (id int primary key, v int)")
	tk.MustExec("create materialized view log on t_purge_partial_delete_err (id, v) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("insert into t_purge_partial_delete_err values (1, 10), (2, 20), (3, 30)")

	is := dom.InfoSchema()
	mlogTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("$mlog$t_purge_partial_delete_err"))
	require.NoError(t, err)
	mlogID := mlogTable.Meta().ID

	tk.MustExec("set @@session.tidb_mlog_purge_batch_size = 1")
	beforeNextTime := fmt.Sprint(tk.MustQuery(fmt.Sprintf("select NEXT_TIME from mysql.tidb_mlog_purge_info where MLOG_ID = %d", mlogID)).Rows()[0][0])
	beforeLastPurgedTSO := tk.MustQuery(fmt.Sprintf("select LAST_PURGED_TSO is null from mysql.tidb_mlog_purge_info where MLOG_ID = %d", mlogID))
	beforeLastPurgedTSO.Check(testkit.Rows("1"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/mockPurgeMaterializedViewLogDeleteErr", "1*return(false)->return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/mockPurgeMaterializedViewLogDeleteErr"))
	}()

	err = tk.ExecToErr("purge materialized view log on t_purge_partial_delete_err")
	require.ErrorContains(t, err, "mock purge mlog delete error")

	tk.MustQuery("select count(*) from `$mlog$t_purge_partial_delete_err`").Check(testkit.Rows("2"))
	tk.MustQuery(fmt.Sprintf("select PURGE_STATUS, PURGE_ROWS, PURGE_FAILED_REASON like '%%mock purge mlog delete error%%' from mysql.tidb_mlog_purge_hist where MLOG_ID = %d order by PURGE_JOB_ID desc limit 1", mlogID)).
		Check(testkit.Rows("failed 1 1"))
	tk.MustQuery(fmt.Sprintf("select LAST_PURGED_TSO is null, NEXT_TIME from mysql.tidb_mlog_purge_info where MLOG_ID = %d", mlogID)).
		Check(testkit.Rows("1 " + beforeNextTime))
}

func TestPurgeMaterializedViewLogManualCancelAfterPartialSuccess(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tkObserver := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tkObserver.MustExec("use test")

	tk.MustExec("create table t_purge_partial_delete_cancel (id int primary key, v int)")
	tk.MustExec("create materialized view log on t_purge_partial_delete_cancel (id, v) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("insert into t_purge_partial_delete_cancel values (1, 10), (2, 20), (3, 30)")

	is := dom.InfoSchema()
	mlogTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("$mlog$t_purge_partial_delete_cancel"))
	require.NoError(t, err)
	mlogID := mlogTable.Meta().ID

	beforeNextTime := fmt.Sprint(tk.MustQuery(fmt.Sprintf("select NEXT_TIME from mysql.tidb_mlog_purge_info where MLOG_ID = %d", mlogID)).Rows()[0][0])
	tk.MustExec("set @@session.tidb_mlog_purge_batch_size = 1")

	pollIntervalFailpoint := "github.com/pingcap/tidb/pkg/executor/mockMVTaskMonitorPollInterval"
	require.NoError(t, failpoint.Enable(pollIntervalFailpoint, "return(50)"))
	defer func() {
		require.NoError(t, failpoint.Disable(pollIntervalFailpoint))
	}()

	pauseFailpoint := "github.com/pingcap/tidb/pkg/executor/pausePurgeMaterializedViewLogAfterDeleteBatch"
	require.NoError(t, failpoint.Enable(pauseFailpoint, "pause"))
	paused := true
	defer func() {
		if paused {
			require.NoError(t, failpoint.Disable(pauseFailpoint))
		}
	}()

	errCh := make(chan error, 1)
	go func() {
		tkPurge := testkit.NewTestKit(t, store)
		tkPurge.MustExec("use test")
		tkPurge.MustExec("set @@session.tidb_mlog_purge_batch_size = 1")
		errCh <- tkPurge.ExecToErr("purge materialized view log on t_purge_partial_delete_cancel")
	}()

	require.Eventually(t, func() bool {
		rows := tkObserver.MustQuery("select count(*) from `$mlog$t_purge_partial_delete_cancel`").Rows()
		return fmt.Sprint(rows[0][0]) == "2"
	}, 10*time.Second, 100*time.Millisecond)

	require.Eventually(t, func() bool {
		rows := tkObserver.MustQuery(fmt.Sprintf(
			"select count(*) from mysql.tidb_mlog_purge_hist where MLOG_ID = %d and PURGE_STATUS = 'running'",
			mlogID,
		)).Rows()
		return fmt.Sprint(rows[0][0]) == "1"
	}, 10*time.Second, 100*time.Millisecond)

	purgeJobID := fmt.Sprint(tkObserver.MustQuery(fmt.Sprintf(
		"select PURGE_JOB_ID from mysql.tidb_mlog_purge_hist where MLOG_ID = %d and PURGE_STATUS = 'running' limit 1",
		mlogID,
	)).Rows()[0][0])
	expectedMonitorName := fmt.Sprintf("mlog-purge-%s", purgeJobID)
	cancelObservedCh := waitMVTaskCancelWatcherRequested(t, expectedMonitorName)

	requester := "'partial_cancel_req'@'stage-d'"
	tkObserver.MustExec(
		`UPDATE mysql.tidb_mlog_purge_hist
SET CANCEL_REQUESTED_AT = NOW(6),
	CANCEL_REQUESTED_BY = ?
WHERE MLOG_ID = ?
  AND PURGE_STATUS = 'running'
  AND CANCEL_REQUESTED_AT IS NULL`,
		requester,
		mlogID,
	)

	select {
	case <-cancelObservedCh:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for purge task monitor to observe manual cancel request")
	}

	require.NoError(t, failpoint.Disable(pauseFailpoint))
	paused = false

	err = <-errCh
	require.Error(t, err)
	require.ErrorContains(t, err, "materialized view task canceled manually")

	tkObserver.MustQuery("select count(*) from `$mlog$t_purge_partial_delete_cancel`").Check(testkit.Rows("2"))
	tkObserver.MustQuery(fmt.Sprintf(
		"select PURGE_STATUS, PURGE_ROWS, PURGE_FAILED_REASON from mysql.tidb_mlog_purge_hist where MLOG_ID = %d order by PURGE_JOB_ID desc limit 1",
		mlogID,
	)).Check(testkit.Rows("failed 1 cancelled manually by " + requester))
	tkObserver.MustQuery(fmt.Sprintf("select LAST_PURGED_TSO is null, NEXT_TIME from mysql.tidb_mlog_purge_info where MLOG_ID = %d", mlogID)).
		Check(testkit.Rows("1 " + beforeNextTime))
}

func TestPurgeMaterializedViewLogBeginFailure(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t_purge_begin_fail (id int primary key, v int)")
	tk.MustExec("create materialized view log on t_purge_begin_fail (id, v) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("insert into t_purge_begin_fail values (1, 10), (2, 20), (3, 30)")

	is := dom.InfoSchema()
	mlogTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("$mlog$t_purge_begin_fail"))
	require.NoError(t, err)
	mlogID := mlogTable.Meta().ID

	tk.MustExec("set @@session.tidb_mlog_purge_batch_size = 1")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/mockPurgeMaterializedViewLogBeginErr", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/mockPurgeMaterializedViewLogBeginErr"))
	}()

	err = tk.ExecToErr("purge materialized view log on t_purge_begin_fail")
	require.ErrorContains(t, err, "mock purge begin error")

	tk.MustQuery("select count(*) from `$mlog$t_purge_begin_fail`").Check(testkit.Rows("3"))
	tk.MustQuery(fmt.Sprintf(
		"select PURGE_STATUS, PURGE_ROWS, PURGE_ENDTIME is not null, PURGE_FAILED_REASON like '%%mock purge begin error%%' from mysql.tidb_mlog_purge_hist where MLOG_ID = %d order by PURGE_JOB_ID desc limit 1",
		mlogID,
	)).Check(testkit.Rows("failed 0 1 1"))
}

func TestPurgeMaterializedViewLogZeroStartTS(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t_purge_zero_start_ts (id int primary key, v int)")
	tk.MustExec("create materialized view log on t_purge_zero_start_ts (id, v) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("insert into t_purge_zero_start_ts values (1, 10), (2, 20)")

	is := dom.InfoSchema()
	mlogTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("$mlog$t_purge_zero_start_ts"))
	require.NoError(t, err)
	mlogID := mlogTable.Meta().ID

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/mockPurgeMaterializedViewLogZeroStartTS", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/mockPurgeMaterializedViewLogZeroStartTS"))
	}()

	err = tk.ExecToErr("purge materialized view log on t_purge_zero_start_ts")
	require.ErrorContains(t, err, "purge materialized view log: invalid transaction start tso")

	tk.MustQuery("select count(*) from `$mlog$t_purge_zero_start_ts`").Check(testkit.Rows("2"))
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mlog_purge_hist where MLOG_ID = %d", mlogID)).
		Check(testkit.Rows("1"))
}

func TestPurgeMaterializedViewLogAdaptiveThrottleFallbackOnCountFailure(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t_purge_adaptive_count_fail (id int primary key, v int)")
	tk.MustExec("create materialized view log on t_purge_adaptive_count_fail (id, v) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("insert into t_purge_adaptive_count_fail values (1, 10), (2, 20), (3, 30)")
	tk.MustExec("set @@session.tidb_mlog_purge_min_rate = 1")
	tk.MustExec("set @@session.tidb_mlog_purge_rate_budget_ratio = 0.5")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/mockMLogPurgeAdaptiveCountErr", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/mockMLogPurgeAdaptiveCountErr"))
	}()

	tk.MustExec("purge materialized view log on t_purge_adaptive_count_fail")
	require.Equal(t, uint64(3), tk.Session().AffectedRows())
	tk.CheckLastMessage("Rows inserted: 0  Updated: 0  Deleted: 3")
	tk.MustQuery("select count(*) from `$mlog$t_purge_adaptive_count_fail`").Check(testkit.Rows("0"))
}

func TestPurgeMaterializedViewLogAdaptiveThrottleFallbackOnDeadlineFailure(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t_purge_adaptive_deadline_fail (id int primary key, v int)")
	tk.MustExec("create materialized view log on t_purge_adaptive_deadline_fail (id, v) purge start with now() next date_add(now(), interval 1 hour)")
	tk.MustExec("insert into t_purge_adaptive_deadline_fail values (1, 10), (2, 20), (3, 30)")
	tk.MustExec("set @@session.tidb_mlog_purge_min_rate = 1")
	tk.MustExec("set @@session.tidb_mlog_purge_rate_budget_ratio = 0.5")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/mockMLogPurgeAdaptiveDeadlineErr", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/mockMLogPurgeAdaptiveDeadlineErr"))
	}()

	tk.MustExec("purge materialized view log on t_purge_adaptive_deadline_fail")
	require.Equal(t, uint64(3), tk.Session().AffectedRows())
	tk.CheckLastMessage("Rows inserted: 0  Updated: 0  Deleted: 3")
	tk.MustQuery("select count(*) from `$mlog$t_purge_adaptive_deadline_fail`").Check(testkit.Rows("0"))
}

func TestPurgeMaterializedViewLogAdaptiveThrottleFallbackOnSleepFailure(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t_purge_adaptive_sleep_fail (id int primary key, v int)")
	tk.MustExec("create materialized view log on t_purge_adaptive_sleep_fail (id, v) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("insert into t_purge_adaptive_sleep_fail values (1, 10), (2, 20), (3, 30)")
	tk.MustExec("set @@session.tidb_mlog_purge_min_rate = 1")
	tk.MustExec("set @@session.tidb_mlog_purge_rate_budget_ratio = 0.5")
	tk.MustExec("set @@session.tidb_mlog_purge_batch_size = 1")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/mockMLogPurgeAdaptiveSleepErr", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/mockMLogPurgeAdaptiveSleepErr"))
	}()

	tk.MustExec("purge materialized view log on t_purge_adaptive_sleep_fail")
	require.Equal(t, uint64(3), tk.Session().AffectedRows())
	tk.CheckLastMessage("Rows inserted: 0  Updated: 0  Deleted: 3")
	tk.MustQuery("select count(*) from `$mlog$t_purge_adaptive_sleep_fail`").Check(testkit.Rows("0"))
}

func TestPurgeMaterializedViewLogMissingPublicMViewRefreshRow(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t_purge_missing_public_refresh (a int)")
	tk.MustExec("create materialized view log on t_purge_missing_public_refresh (a) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_purge_missing_public_refresh (a, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, count(1) from t_purge_missing_public_refresh group by a")

	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv_purge_missing_public_refresh"))
	require.NoError(t, err)
	mvID := mvTable.Meta().ID
	mlogTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("$mlog$t_purge_missing_public_refresh"))
	require.NoError(t, err)
	mlogID := mlogTable.Meta().ID

	tk.MustExec(fmt.Sprintf("delete from mysql.tidb_mview_refresh_info where mview_id = %d", mvID))
	err = tk.ExecToErr("purge materialized view log on t_purge_missing_public_refresh")
	require.ErrorContains(t, err, "materialized view refresh info is missing")

	// Purge failure should still be finalized in history.
	tk.MustQuery(fmt.Sprintf("select PURGE_STATUS, PURGE_ROWS, PURGE_ENDTIME is not null from mysql.tidb_mlog_purge_hist where MLOG_ID = %d order by PURGE_JOB_ID desc limit 1", mlogID)).
		Check(testkit.Rows("failed 0 1"))
}

func TestPurgeMaterializedViewLogWritesState(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t_purge_state (id int primary key, v int)")
	tk.MustExec("create materialized view log on t_purge_state (id, v) purge next date_add(now(), interval 1 hour)")

	is := dom.InfoSchema()
	mlogTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("$mlog$t_purge_state"))
	require.NoError(t, err)
	mlogID := mlogTable.Meta().ID

	tk.MustExec("purge materialized view log on t_purge_state")
	tk.MustQuery(fmt.Sprintf("select PURGE_STATUS, PURGE_METHOD, PURGE_ROWS, PURGE_ENDTIME is not null from mysql.tidb_mlog_purge_hist where MLOG_ID = %d order by PURGE_JOB_ID desc limit 1", mlogID)).
		Check(testkit.Rows("success manual 0 1"))
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mlog_purge_hist where MLOG_ID = %d", mlogID)).
		Check(testkit.Rows("1"))

	tk.MustExec("purge materialized view log on t_purge_state")
	tk.MustQuery(fmt.Sprintf("select PURGE_STATUS, PURGE_METHOD, PURGE_ROWS, PURGE_ENDTIME is not null from mysql.tidb_mlog_purge_hist where MLOG_ID = %d order by PURGE_JOB_ID desc limit 1", mlogID)).
		Check(testkit.Rows("success manual 0 1"))
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mlog_purge_hist where MLOG_ID = %d", mlogID)).
		Check(testkit.Rows("2"))
}

func TestPurgeMaterializedViewLogNextTimeOnlyUpdatesForInternalSQL(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_purge_internal_next (a int not null, b int not null)")
	tk.MustExec("create materialized view log on t_purge_internal_next (a, b) purge start with date_add(now(), interval 2 hour) next date_add(now(), interval 40 minute)")

	is := dom.InfoSchema()
	mlogTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("$mlog$t_purge_internal_next"))
	require.NoError(t, err)
	mlogID := mlogTable.Meta().ID

	tk.MustExec(fmt.Sprintf("update mysql.tidb_mlog_purge_info set NEXT_TIME = null where MLOG_ID = %d", mlogID))

	// User SQL purge should not update NEXT_TIME.
	tk.MustExec("purge materialized view log on t_purge_internal_next")
	tk.MustQuery(fmt.Sprintf("select NEXT_TIME is null from mysql.tidb_mlog_purge_info where MLOG_ID = %d", mlogID)).
		Check(testkit.Rows("1"))
	tk.MustQuery(fmt.Sprintf(
		"select PURGE_METHOD from mysql.tidb_mlog_purge_hist where MLOG_ID = %d order by PURGE_JOB_ID desc limit 1",
		mlogID,
	)).Check(testkit.Rows("manual"))

	// Internal SQL purge should update NEXT_TIME by evaluating PurgeNext.
	mustExecInternal(t, tk, "purge materialized view log on t_purge_internal_next")
	tk.MustQuery(fmt.Sprintf(
		"select NEXT_TIME is not null, NEXT_TIME > UTC_TIMESTAMP() + interval 20 minute, NEXT_TIME < UTC_TIMESTAMP() + interval 2 hour from mysql.tidb_mlog_purge_info where MLOG_ID = %d",
		mlogID,
	)).Check(testkit.Rows("1 1 1"))
	tk.MustQuery(fmt.Sprintf(
		"select PURGE_METHOD from mysql.tidb_mlog_purge_hist where MLOG_ID = %d order by PURGE_JOB_ID desc limit 1",
		mlogID,
	)).Check(testkit.Rows("auto"))
}

func TestPurgeMaterializedViewLogInternalSQLStartWithNoNextSetsNextTimeNull(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_purge_internal_start_only (a int not null, b int not null)")
	tk.MustExec("create materialized view log on t_purge_internal_start_only (a, b) purge start with date_add(now(), interval 2 hour) next date_add(now(), interval 40 minute)")

	is := dom.InfoSchema()
	mlogTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("$mlog$t_purge_internal_start_only"))
	require.NoError(t, err)
	require.NotNil(t, mlogTable.Meta().MaterializedViewLog)
	// Simulate scheduler metadata state: START WITH is set but NEXT is empty.
	mlogTable.Meta().MaterializedViewLog.PurgeStartWith = "DATE_ADD(NOW(), INTERVAL 2 HOUR)"
	mlogTable.Meta().MaterializedViewLog.PurgeNext = ""
	mlogID := mlogTable.Meta().ID

	tk.MustExec(fmt.Sprintf("update mysql.tidb_mlog_purge_info set NEXT_TIME = UTC_TIMESTAMP() + interval 3 hour where MLOG_ID = %d", mlogID))

	// User SQL purge should keep NEXT_TIME unchanged.
	tk.MustExec("purge materialized view log on t_purge_internal_start_only")
	tk.MustQuery(fmt.Sprintf("select NEXT_TIME is not null from mysql.tidb_mlog_purge_info where MLOG_ID = %d", mlogID)).
		Check(testkit.Rows("1"))
	tk.MustQuery(fmt.Sprintf(
		"select PURGE_METHOD from mysql.tidb_mlog_purge_hist where MLOG_ID = %d order by PURGE_JOB_ID desc limit 1",
		mlogID,
	)).Check(testkit.Rows("manual"))

	// Internal SQL purge should explicitly set NEXT_TIME = NULL when START WITH exists and NEXT is empty.
	mustExecInternal(t, tk, "purge materialized view log on t_purge_internal_start_only")
	tk.MustQuery(fmt.Sprintf("select NEXT_TIME is null from mysql.tidb_mlog_purge_info where MLOG_ID = %d", mlogID)).
		Check(testkit.Rows("1"))
	tk.MustQuery(fmt.Sprintf(
		"select PURGE_METHOD from mysql.tidb_mlog_purge_hist where MLOG_ID = %d order by PURGE_JOB_ID desc limit 1",
		mlogID,
	)).Check(testkit.Rows("auto"))
}

func TestPurgeMaterializedViewLogInternalSQLNoScheduleSetsNextTimeNull(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_purge_internal_no_schedule (a int not null, b int not null)")
	tk.MustExec("create materialized view log on t_purge_internal_no_schedule (a, b) purge start with date_add(now(), interval 2 hour) next date_add(now(), interval 40 minute)")

	is := dom.InfoSchema()
	mlogTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("$mlog$t_purge_internal_no_schedule"))
	require.NoError(t, err)
	require.NotNil(t, mlogTable.Meta().MaterializedViewLog)
	// Simulate scheduler metadata state: schedule is fully removed.
	mlogTable.Meta().MaterializedViewLog.PurgeStartWith = ""
	mlogTable.Meta().MaterializedViewLog.PurgeNext = ""
	mlogID := mlogTable.Meta().ID

	tk.MustExec(fmt.Sprintf("update mysql.tidb_mlog_purge_info set NEXT_TIME = UTC_TIMESTAMP() + interval 3 hour where MLOG_ID = %d", mlogID))

	mustExecInternal(t, tk, "purge materialized view log on t_purge_internal_no_schedule")
	tk.MustQuery(fmt.Sprintf("select NEXT_TIME is null from mysql.tidb_mlog_purge_info where MLOG_ID = %d", mlogID)).
		Check(testkit.Rows("1"))
	tk.MustQuery(fmt.Sprintf(
		"select PURGE_METHOD from mysql.tidb_mlog_purge_hist where MLOG_ID = %d order by PURGE_JOB_ID desc limit 1",
		mlogID,
	)).Check(testkit.Rows("auto"))
}
