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

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/parser/auth"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/stretchr/testify/require"
)

func TestCreateMaterializedViewLogBasic(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, b int)")

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

func TestCreateMaterializedViewLogPreSplitOptions(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	originSplit := atomic.LoadUint32(&ddl.EnableSplitTableRegion)
	atomic.StoreUint32(&ddl.EnableSplitTableRegion, 1)
	defer atomic.StoreUint32(&ddl.EnableSplitTableRegion, originSplit)
	tk.MustExec("set @@session.tidb_scatter_region='table'")
	tk.MustExec("create table t_mlog_presplit (a int, b int)")

	tk.MustExec("create materialized view log on t_mlog_presplit (a) shard_row_id_bits = 2 pre_split_regions = 2 purge immediate")

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

	err := tk.ExecToErr("create materialized view log on t (a) purge start with 1 next now()")
	require.ErrorContains(t, err, "PURGE START WITH expression must return DATETIME/TIMESTAMP")

	err = tk.ExecToErr("create materialized view log on t (a) purge next 600")
	require.ErrorContains(t, err, "PURGE NEXT expression must return DATETIME/TIMESTAMP")

	tk.MustExec("create materialized view log on t (a) purge start with now() next now()")
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

	tk.MustExec("create materialized view mv_truncate_mv (a, cnt) refresh fast next now() as select a, count(1) from t_truncate_mv group by a")

	err := tk.ExecToErr("truncate table mv_truncate_mv")
	require.ErrorContains(t, err, "TRUNCATE TABLE on materialized view table")

	err = tk.ExecToErr("truncate table t_truncate_mv")
	require.ErrorContains(t, err, "TRUNCATE TABLE on base table with materialized view dependencies")
}

func TestMaterializedViewRelatedTablesDDLRejected(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_ddl_mv (a int not null, b int)")
	tk.MustExec("create materialized view log on t_ddl_mv (a, b)")
	tk.MustExec("create materialized view mv_ddl_mv (a, cnt) refresh fast next now() as select a, count(1) from t_ddl_mv group by a")

	err := tk.ExecToErr("alter table t_ddl_mv add column c int")
	require.ErrorContains(t, err, "ALTER TABLE on base table with materialized view dependencies")
	err = tk.ExecToErr("drop table t_ddl_mv")
	require.ErrorContains(t, err, "DROP TABLE on base table with materialized view dependencies")
	err = tk.ExecToErr("rename table t_ddl_mv to t_ddl_mv2")
	require.ErrorContains(t, err, "RENAME TABLE on base table with materialized view dependencies")

	err = tk.ExecToErr("alter table mv_ddl_mv add column x int")
	require.ErrorContains(t, err, "ALTER TABLE on materialized view table")
	err = tk.ExecToErr("drop table mv_ddl_mv")
	require.ErrorContains(t, err, "DROP TABLE on materialized view table")
	err = tk.ExecToErr("rename table mv_ddl_mv to mv_ddl_mv2")
	require.ErrorContains(t, err, "RENAME TABLE on materialized view table")
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

func TestPurgeMaterializedViewLogPrivilege(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_purge_priv (a int)")
	tk.MustExec("create materialized view log on t_purge_priv (a) purge immediate")
	tk.MustExec("create user 'u1'@'%'")
	tk.MustExec("grant select on test.t_purge_priv to 'u1'@'%'")

	tkUser := testkit.NewTestKit(t, store)
	require.NoError(t, tkUser.Session().Auth(&auth.UserIdentity{Username: "u1", Hostname: "%"}, nil, nil, nil))
	tkUser.MustExec("use test")

	err := tkUser.ExecToErr("purge materialized view log on t_purge_priv")
	require.ErrorContains(t, err, "ALTER command denied")

	tk.MustExec("grant alter on test.t_purge_priv to 'u1'@'%'")
	tkUser.MustExec("purge materialized view log on t_purge_priv")
}

func TestPurgeMaterializedViewLogLockRowMissing(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_purge_lock_row_missing (a int)")
	tk.MustExec("create materialized view log on t_purge_lock_row_missing (a) purge immediate")

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
	tk1.MustExec("create materialized view log on t_purge_nowait_conflict (a) purge immediate")

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
	tk.MustExec("create materialized view log on t_purge_batch_delete (id, v) purge immediate")
	tk.MustExec("insert into t_purge_batch_delete values (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)")

	is := dom.InfoSchema()
	mlogTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("$mlog$t_purge_batch_delete"))
	require.NoError(t, err)
	mlogID := mlogTable.Meta().ID

	tk.MustQuery("select count(*) from `$mlog$t_purge_batch_delete`").Check(testkit.Rows("5"))
	maxCommitTS, err := strconv.ParseUint(fmt.Sprint(tk.MustQuery("select max(_tidb_commit_ts) from `$mlog$t_purge_batch_delete`").Rows()[0][0]), 10, 64)
	require.NoError(t, err)
	tk.MustExec("set @@session.tidb_mlog_purge_batch_size = 2")
	tk.MustExec("purge materialized view log on t_purge_batch_delete")

	tk.MustQuery("select count(*) from `$mlog$t_purge_batch_delete`").Check(testkit.Rows("0"))
	tk.MustQuery(fmt.Sprintf("select PURGE_STATUS, PURGE_ROWS from mysql.tidb_mlog_purge_hist where MLOG_ID = %d order by PURGE_JOB_ID desc limit 1", mlogID)).
		Check(testkit.Rows("success 5"))
	tk.MustQuery(fmt.Sprintf("select LAST_PURGED_TSO is not null, LAST_PURGED_TSO >= %d from mysql.tidb_mlog_purge_info where MLOG_ID = %d", maxCommitTS, mlogID)).
		Check(testkit.Rows("1 1"))
}

func TestPurgeMaterializedViewLogLastPurgedTSOShortCircuit(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t_purge_short_circuit (a int not null, b int)")
	tk.MustExec("create materialized view log on t_purge_short_circuit (a, b) purge immediate")
	tk.MustExec("create materialized view mv_purge_short_circuit (a, cnt) refresh fast next now() as select a, count(1) from t_purge_short_circuit group by a")
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

func TestPurgeMaterializedViewLogDeleteErrorNoDirtyWrite(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t_purge_delete_err (id int primary key, v int)")
	tk.MustExec("create materialized view log on t_purge_delete_err (id, v) purge immediate")
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
	tk.MustQuery(fmt.Sprintf("select PURGE_STATUS, PURGE_ROWS, PURGE_ENDTIME is not null from mysql.tidb_mlog_purge_hist where MLOG_ID = %d order by PURGE_JOB_ID desc limit 1", mlogID)).
		Check(testkit.Rows("failed 0 1"))
}

func TestPurgeMaterializedViewLogLockConflictAfterPartialSuccess(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t_purge_partial_conflict (id int primary key, v int)")
	tk.MustExec("create materialized view log on t_purge_partial_conflict (id, v) purge immediate")
	tk.MustExec("insert into t_purge_partial_conflict values (1, 10), (2, 20), (3, 30)")

	is := dom.InfoSchema()
	mlogTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("$mlog$t_purge_partial_conflict"))
	require.NoError(t, err)
	mlogID := mlogTable.Meta().ID

	tk.MustExec("set @@session.tidb_mlog_purge_batch_size = 1")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/mockPurgeMaterializedViewLogLockConflict", "1*return(false)->return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/mockPurgeMaterializedViewLogLockConflict"))
	}()

	tk.MustExec("purge materialized view log on t_purge_partial_conflict")
	tk.MustQuery("show warnings").CheckContain("lock conflict after deleting 1 rows")

	tk.MustQuery("select count(*) from `$mlog$t_purge_partial_conflict`").Check(testkit.Rows("2"))
	tk.MustQuery(fmt.Sprintf("select PURGE_STATUS, PURGE_ROWS from mysql.tidb_mlog_purge_hist where MLOG_ID = %d order by PURGE_JOB_ID desc limit 1", mlogID)).
		Check(testkit.Rows("success 1"))
}

func TestPurgeMaterializedViewLogMissingPublicMViewRefreshRow(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t_purge_missing_public_refresh (a int)")
	tk.MustExec("create materialized view log on t_purge_missing_public_refresh (a) purge immediate")
	tk.MustExec("create materialized view mv_purge_missing_public_refresh (a, cnt) refresh fast next now() as select a, count(1) from t_purge_missing_public_refresh group by a")

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
	tk.MustExec("create materialized view log on t_purge_state (id, v) purge immediate")

	is := dom.InfoSchema()
	mlogTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("$mlog$t_purge_state"))
	require.NoError(t, err)
	mlogID := mlogTable.Meta().ID

	tk.MustExec("purge materialized view log on t_purge_state")
	tk.MustQuery(fmt.Sprintf("select PURGE_STATUS, PURGE_ROWS, PURGE_ENDTIME is not null from mysql.tidb_mlog_purge_hist where MLOG_ID = %d order by PURGE_JOB_ID desc limit 1", mlogID)).
		Check(testkit.Rows("success 0 1"))
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mlog_purge_hist where MLOG_ID = %d", mlogID)).
		Check(testkit.Rows("1"))

	tk.MustExec("purge materialized view log on t_purge_state")
	tk.MustQuery(fmt.Sprintf("select PURGE_STATUS, PURGE_ROWS, PURGE_ENDTIME is not null from mysql.tidb_mlog_purge_hist where MLOG_ID = %d order by PURGE_JOB_ID desc limit 1", mlogID)).
		Check(testkit.Rows("success 0 1"))
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mlog_purge_hist where MLOG_ID = %d", mlogID)).
		Check(testkit.Rows("2"))
}
