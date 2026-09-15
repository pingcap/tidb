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

package materializedviewlog_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func mustExecMViewMaintenance(t *testing.T, tk *testkit.TestKit, sql string) {
	t.Helper()
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnMViewMaintenance)
	vars := tk.Session().GetSessionVars()
	originalMaintenance := vars.InMViewMaintenance
	originalRestrictedSQL := vars.InRestrictedSQL
	vars.InMViewMaintenance = true
	vars.InRestrictedSQL = true
	defer func() {
		vars.InMViewMaintenance = originalMaintenance
		vars.InRestrictedSQL = originalRestrictedSQL
	}()
	rs, err := tk.Session().ExecuteInternal(ctx, sql)
	require.NoError(t, err)
	require.Nil(t, rs)
}

func TestCreateVectorIndexOnMaterializedViewLogTableRejected(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomainWithSchemaLease(t, 100*time.Millisecond, mockstore.WithMockTiFlash(2))
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_mlog_vec (id int, v vector(3))")
	tk.MustExec("create materialized view log on t_mlog_vec (v) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("alter table `$mlog$t_mlog_vec` set tiflash replica 1")

	err := tk.ExecToErr("create vector index idx_mlog_vec on `$mlog$t_mlog_vec`((vec_cosine_distance(v))) USING HNSW")
	require.ErrorContains(t, err, "CREATE INDEX on materialized view log table")
}

func TestAlterMaterializedViewLogPurgeUpdatesNextUnixSecondsWithMLogAlterPrivilege(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_mlog_purge_priv (a int, b int)")
	tk.MustExec("create materialized view log on t_mlog_purge_priv (a, b) purge next date_add(now(), interval 2 hour)")
	tk.MustExec("create user 'mv_alter_purge_u'@'%'")
	tk.MustExec("create user 'mv_alter_purge_select_u'@'%'")
	defer tk.MustExec("drop user 'mv_alter_purge_u'@'%'")
	defer tk.MustExec("drop user 'mv_alter_purge_select_u'@'%'")
	tk.MustExec("grant alter on test.`$mlog$t_mlog_purge_priv` to 'mv_alter_purge_u'@'%'")
	tk.MustExec("grant select on test.t_mlog_purge_priv to 'mv_alter_purge_select_u'@'%'")

	tkUser := newMViewTestKit(t, store)
	require.NoError(t, tkUser.Session().Auth(&auth.UserIdentity{Username: "mv_alter_purge_u", Hostname: "%"}, nil, nil, nil))
	tkUser.MustExec("alter materialized view log on test.t_mlog_purge_priv purge next date_add(now(), interval 25 minute)")

	tkSelectUser := newMViewTestKit(t, store)
	require.NoError(t, tkSelectUser.Session().Auth(&auth.UserIdentity{Username: "mv_alter_purge_select_u", Hostname: "%"}, nil, nil, nil))
	err := tkSelectUser.ExecToErr("alter materialized view log on test.t_mlog_purge_priv purge next date_add(now(), interval 30 minute)")
	require.ErrorContains(t, err, "ALTER command denied")

	mlogTable, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("$mlog$t_mlog_purge_priv"))
	require.NoError(t, err)
	require.Equal(t, "DATE_ADD(NOW(), INTERVAL 25 MINUTE)", mlogTable.Meta().MaterializedViewLog.PurgeNext)
	tk.MustQuery(fmt.Sprintf(
		"select NEXT_PURGE_UNIX_SECONDS is not null, NEXT_PURGE_UNIX_SECONDS > TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP() + interval 15 minute), NEXT_PURGE_UNIX_SECONDS < TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP() + interval 1 hour) from mysql.tidb_mlog_purge_info where MLOG_ID = %d",
		mlogTable.Meta().ID,
	)).Check(testkit.Rows("1 1 1"))
}

func TestAlterMaterializedViewLogAddColumnBasic(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_mlog_add_column (id int not null, n int not null, s varchar(10) not null, d date not null, note text not null, untouched int)")
	tk.MustExec("create materialized view log on t_mlog_add_column (id)")
	mustExecMViewMaintenance(t, tk, "insert into `$mlog$t_mlog_add_column` (id, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW`) values (1, 'I', 1)")

	tk.MustExec("alter materialized view log on t_mlog_add_column add column (n, s, d, note)")
	mlogTable, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("$mlog$t_mlog_add_column"))
	require.NoError(t, err)
	require.Equal(t, []ast.CIStr{ast.NewCIStr("id"), ast.NewCIStr("n"), ast.NewCIStr("s"), ast.NewCIStr("d"), ast.NewCIStr("note")}, mlogTable.Meta().MaterializedViewLog.Columns)

	columnNames := make([]string, 0, len(mlogTable.Meta().Columns))
	columns := make(map[string]*model.ColumnInfo, len(mlogTable.Meta().Columns))
	for _, column := range mlogTable.Meta().Columns {
		columnNames = append(columnNames, column.Name.O)
		columns[column.Name.L] = column
	}
	require.Equal(t, []string{"id", "n", "s", "d", "note", "_MLOG$_DML_TYPE", "_MLOG$_OLD_NEW"}, columnNames)
	for _, name := range []string{"n", "s", "d", "note"} {
		require.True(t, mysql.HasNotNullFlag(columns[name].GetFlag()))
	}
	require.Equal(t, "0", fmt.Sprint(columns["n"].GetOriginDefaultValue()))
	require.Equal(t, " ", fmt.Sprint(columns["s"].GetOriginDefaultValue()))
	require.Equal(t, "0000-00-00", fmt.Sprint(columns["d"].GetOriginDefaultValue()))
	require.Equal(t, " ", fmt.Sprint(columns["note"].GetOriginDefaultValue()))
	require.Equal(t, mysql.TypeBlob, columns["note"].GetType())
	tk.MustQuery("select n, hex(s), cast(d as char), hex(note), `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t_mlog_add_column`").Check(testkit.Rows("0 20 0000-00-00 20 I 1"))

	tk.MustExec("create table t_mlog_add_column_atomic (id int, b int, c int)")
	tk.MustExec("create materialized view log on t_mlog_add_column_atomic (id)")
	cancelTK := testkit.NewTestKit(t, store)
	cancelTK.MustExec("use test")
	cancelTriggered := atomic.Bool{}
	cancelDone := make(chan error, 1)
	const fp = "github.com/pingcap/tidb/pkg/ddl/afterWaitSchemaSynced"
	require.NoError(t, failpoint.EnableCall(fp, func(job *model.Job) {
		if !cancelTriggered.CompareAndSwap(false, true) {
			return
		}
		if job.Type != model.ActionMultiSchemaChange || job.SchemaName != "test" || job.TableName != "$mlog$t_mlog_add_column_atomic" || job.MultiSchemaInfo == nil || len(job.MultiSchemaInfo.SubJobs) != 2 || job.MultiSchemaInfo.SubJobs[1].SchemaState != model.StateWriteReorganization {
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
	err = tk.ExecToErr("alter materialized view log on t_mlog_add_column_atomic add column (b, c)")
	require.NoError(t, failpoint.Disable(fp))
	require.ErrorContains(t, err, "Cancelled DDL job")
	select {
	case cancelErr := <-cancelDone:
		require.NoError(t, cancelErr)
	default:
		require.FailNow(t, "expected mlog multi-column add cancellation")
	}
	atomicMLog, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("$mlog$t_mlog_add_column_atomic"))
	require.NoError(t, err)
	require.Equal(t, []ast.CIStr{ast.NewCIStr("id")}, atomicMLog.Meta().MaterializedViewLog.Columns)
	require.Nil(t, model.FindColumnInfo(atomicMLog.Meta().Columns, "b"))
	require.Nil(t, model.FindColumnInfo(atomicMLog.Meta().Columns, "c"))
}

func TestAlterMaterializedViewLogAddColumnDefaultSemantics(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_mlog_add_defaults (id int, nullable_varchar varchar(10), nullable_text text, nn_enum enum('a','b') not null, nn_set set('x','y') not null, nullable_enum enum('a','b'), nullable_set set('x','y'), nn_varchar varchar(10) not null, nn_text text not null)")
	tk.MustExec("create materialized view log on t_mlog_add_defaults (id)")
	mustExecMViewMaintenance(t, tk, "insert into `$mlog$t_mlog_add_defaults` (id, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW`) values (1, 'I', 1)")

	tk.MustExec("alter materialized view log on t_mlog_add_defaults add column (nullable_varchar, nullable_text, nn_enum, nn_set, nullable_enum, nullable_set, nn_varchar, nn_text)")
	tk.MustQuery("select nullable_varchar is null, nullable_text is null, cast(nn_enum as char), cast(nn_set as char), nullable_enum is null, nullable_set is null, hex(nn_varchar), hex(nn_text), `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t_mlog_add_defaults`").Check(testkit.Rows("1 1 a  1 1 20 20 I 1"))
}

func TestAlterMaterializedViewLogPurgeBestEffortInfoUpdateWarning(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tkLock := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tkLock.MustExec("use test")
	tk.MustExec("create table t_mlog_info_lock (a int, b int)")
	tk.MustExec("create materialized view log on t_mlog_info_lock (a, b) purge next date_add(now(), interval 2 hour)")

	mlogTable, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("$mlog$t_mlog_info_lock"))
	require.NoError(t, err)
	const expectedNextUnixSeconds int64 = 1_925_089_445
	tk.MustExec(fmt.Sprintf("update mysql.tidb_mlog_purge_info set NEXT_PURGE_UNIX_SECONDS = %d where MLOG_ID = %d", expectedNextUnixSeconds, mlogTable.Meta().ID))
	tkLock.MustExec("begin pessimistic")
	defer tkLock.MustExec("rollback")
	tkLock.MustExec(fmt.Sprintf("update mysql.tidb_mlog_purge_info set NEXT_PURGE_UNIX_SECONDS = NEXT_PURGE_UNIX_SECONDS where MLOG_ID = %d", mlogTable.Meta().ID))

	tk.MustExec("alter materialized view log on t_mlog_info_lock purge next date_add(now(), interval 25 minute)")
	tk.MustQuery("show warnings").CheckContain("alter materialized view log purge: metadata updated but failed to update mysql.tidb_mlog_purge_info.NEXT_PURGE_UNIX_SECONDS within 10s due to row lock contention")
	mlogTable, err = dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("$mlog$t_mlog_info_lock"))
	require.NoError(t, err)
	require.Equal(t, "DATE_ADD(NOW(), INTERVAL 25 MINUTE)", mlogTable.Meta().MaterializedViewLog.PurgeNext)
	tk.MustQuery(fmt.Sprintf("select NEXT_PURGE_UNIX_SECONDS = %d from mysql.tidb_mlog_purge_info where MLOG_ID = %d", expectedNextUnixSeconds, mlogTable.Meta().ID)).Check(testkit.Rows("1"))
}

func TestAlterMaterializedViewLogAddColumnPrivilege(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_add_mlog_priv (a int, b int)")
	tk.MustExec("create materialized view log on t_add_mlog_priv (a)")
	tk.MustExec("create user 'u_add_mlog_no_select'@'%'")
	tk.MustExec("create user 'u_add_mlog_ok'@'%'")
	defer tk.MustExec("drop user 'u_add_mlog_no_select'@'%'")
	defer tk.MustExec("drop user 'u_add_mlog_ok'@'%'")

	tk.MustExec("grant alter on test.`$mlog$t_add_mlog_priv` to 'u_add_mlog_no_select'@'%'")
	tkNoSelect := newMViewTestKit(t, store)
	require.NoError(t, tkNoSelect.Session().Auth(&auth.UserIdentity{Username: "u_add_mlog_no_select", Hostname: "%"}, nil, nil, nil))
	err := tkNoSelect.ExecToErr("alter materialized view log on test.t_add_mlog_priv add column (b)")
	require.ErrorContains(t, err, "SELECT command denied")

	tk.MustExec("grant alter on test.`$mlog$t_add_mlog_priv` to 'u_add_mlog_ok'@'%'")
	tk.MustExec("grant select on test.t_add_mlog_priv to 'u_add_mlog_ok'@'%'")
	tkOK := newMViewTestKit(t, store)
	require.NoError(t, tkOK.Session().Auth(&auth.UserIdentity{Username: "u_add_mlog_ok", Hostname: "%"}, nil, nil, nil))
	tkOK.MustExec("alter materialized view log on test.t_add_mlog_priv add column (b)")
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
