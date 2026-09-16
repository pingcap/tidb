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

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func newMViewTestKit(t testing.TB, store kv.Storage) *testkit.TestKit {
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set tidb_mview_enable = on")
	return tk
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

	// The MV physical table follows the same pre-split region flow as its MLog.
	tk.MustExec("create table t_mv_presplit (a int, b int)")
	tk.MustExec("create materialized view log on t_mv_presplit (a)")
	tk.MustExec("create materialized view mv_presplit (a, cnt) shard_row_id_bits = 2 pre_split_regions = 2 as select a, count(1) from t_mv_presplit group by a")
	showCreate = tk.MustQuery("show create table mv_presplit").Rows()[0][1].(string)
	require.Contains(t, showCreate, "SHARD_ROW_ID_BITS=2")
	require.Contains(t, showCreate, "PRE_SPLIT_REGIONS=2")

	is = dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_presplit"))
	require.NoError(t, err)
	require.Equal(t, uint64(2), mvTable.Meta().ShardRowIDBits)
	require.Equal(t, uint64(2), mvTable.Meta().PreSplitRegions)
	mvRegions := tk.MustQuery("show table mv_presplit regions").Rows()
	mvRegionNames := make([]string, 0, len(mvRegions))
	for _, row := range mvRegions {
		mvRegionNames = append(mvRegionNames, fmt.Sprint(row[1]))
	}
	require.Contains(t, mvRegionNames, fmt.Sprintf("t_%d_r_2305843009213693952", mvTable.Meta().ID))
	require.Contains(t, mvRegionNames, fmt.Sprintf("t_%d_r_4611686018427387904", mvTable.Meta().ID))
	require.Contains(t, mvRegionNames, fmt.Sprintf("t_%d_r_6917529027641081856", mvTable.Meta().ID))
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
