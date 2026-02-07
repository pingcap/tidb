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
	"strings"
	"testing"

	"github.com/pingcap/failpoint"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestMaterializedViewDDLBasic(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int)")

	// Base table must have MV LOG first.
	err := tk.ExecToErr("create materialized view mv_no_log (a, cnt) as select a, count(1) from t group by a")
	require.ErrorContains(t, err, "materialized view log does not exist")

	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge immediate")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next 300 as select a, sum(b), count(1) from t group by a")
	tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 7 1"))

	// Physical table created.
	tk.MustQuery("select count(*) from information_schema.tables where table_schema='test' and table_name='mv'").Check(testkit.Rows("1"))

	showCreate := tk.MustQuery("show create table mv").Rows()[0][1].(string)
	require.Contains(t, showCreate, "PRIMARY KEY (`a`)")

	is := dom.InfoSchema()
	baseTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
	require.NoError(t, err)
	mlogTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("$mlog$t"))
	require.NoError(t, err)
	mvTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv"))
	require.NoError(t, err)

	require.NotNil(t, mvTable.Meta().MaterializedView)
	require.Equal(t, []int64{baseTable.Meta().ID}, mvTable.Meta().MaterializedView.BaseTableIDs)
	require.Equal(t, "FAST", mvTable.Meta().MaterializedView.RefreshMethod)
	require.Equal(t, "NOW()", mvTable.Meta().MaterializedView.RefreshStartWith)
	require.Equal(t, "300", mvTable.Meta().MaterializedView.RefreshNext)
	tk.MustQuery(fmt.Sprintf("select LAST_REFRESH_RESULT, LAST_REFRESH_TYPE, LAST_SUCCESSFUL_REFRESH_READ_TSO > 0, LAST_REFRESH_FAILED_REASON is null from mysql.tidb_mview_refresh where MVIEW_ID = %d", mvTable.Meta().ID)).
		Check(testkit.Rows("success complete 1 1"))

	// Base table reverse mapping maintained by DDL.
	require.NotNil(t, baseTable.Meta().MaterializedViewBase)
	require.Equal(t, mlogTable.Meta().ID, baseTable.Meta().MaterializedViewBase.MLogID)
	require.Contains(t, baseTable.Meta().MaterializedViewBase.MViewIDs, mvTable.Meta().ID)

	// MV must contain count(*|1).
	err = tk.ExecToErr("create materialized view mv_bad (a, s) as select a, sum(b) from t group by a")
	require.ErrorContains(t, err, "must contain count(*)/count(1)")

	// Count(column), non-deterministic WHERE and unsupported aggregate should fail.
	err = tk.ExecToErr("create materialized view mv_bad_count_col (a, c) as select a, count(b) from t group by a")
	require.ErrorContains(t, err, "only supports count(*)/count(1)")
	err = tk.ExecToErr("create materialized view mv_bad_avg (a, avgv, c) as select a, avg(b), count(1) from t group by a")
	require.ErrorContains(t, err, "unsupported aggregate function")
	err = tk.ExecToErr("create materialized view mv_bad_where (a, c) as select a, count(1) from t where rand() > 0 group by a")
	require.ErrorContains(t, err, "WHERE clause must be deterministic")

	// MV LOG must contain all referenced columns.
	tk.MustExec("create table t_mlog_missing (a int not null, b int)")
	tk.MustExec("create materialized view log on t_mlog_missing (a) purge immediate")
	err = tk.ExecToErr("create materialized view mv_bad_mlog_cols (a, s, c) as select a, sum(b), count(1) from t_mlog_missing group by a")
	require.ErrorContains(t, err, "does not contain column b")

	// Nullable group-by key should use UNIQUE KEY.
	tk.MustExec("create table t_nullable (a int, b int)")
	tk.MustExec("create materialized view log on t_nullable (a, b) purge immediate")
	tk.MustExec("create materialized view mv_nullable (a, c) as select a, count(1) from t_nullable group by a")
	showCreate = tk.MustQuery("show create table mv_nullable").Rows()[0][1].(string)
	require.Contains(t, showCreate, "UNIQUE KEY")
	require.False(t, strings.Contains(showCreate, "PRIMARY KEY (`a`)"))

	// MV LOG cannot be dropped while dependent MVs exist.
	err = tk.ExecToErr("drop materialized view log on t")
	require.ErrorContains(t, err, "dependent materialized views exist")

	// Drop MV and then drop MV LOG.
	tk.MustExec("drop materialized view mv")
	tk.MustExec("drop materialized view mv_nullable")
	tk.MustExec("drop materialized view log on t")
	tk.MustExec("drop materialized view log on t_nullable")

	// Reverse mapping cleared.
	is = dom.InfoSchema()
	baseTable, err = is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
	require.NoError(t, err)
	require.True(t, baseTable.Meta().MaterializedViewBase == nil || (baseTable.Meta().MaterializedViewBase.MLogID == 0 && len(baseTable.Meta().MaterializedViewBase.MViewIDs) == 0))
}

func TestCreateMaterializedViewBuildFailureWritesRefreshMeta(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge immediate")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockCreateMaterializedViewBuildErr", "return"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockCreateMaterializedViewBuildErr"))
	}()

	err := tk.ExecToErr("create materialized view mv_fail (a, s, cnt) refresh fast next 300 as select a, sum(b), count(1) from t group by a")
	require.ErrorContains(t, err, "mock create materialized view build error")

	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv_fail"))
	require.NoError(t, err)
	tk.MustQuery(fmt.Sprintf("select LAST_REFRESH_RESULT, LAST_REFRESH_TYPE, LAST_SUCCESSFUL_REFRESH_READ_TSO is null, LAST_REFRESH_FAILED_REASON like '%%mock create materialized view build error%%' from mysql.tidb_mview_refresh where MVIEW_ID = %d", mvTable.Meta().ID)).
		Check(testkit.Rows("failed complete 1 1"))
}

func TestCreateMaterializedViewRejectNonBaseObject(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int)")
	tk.MustExec("create materialized view log on t (a)")
	tk.MustExec("create view v as select a from t")

	err := tk.ExecToErr("create materialized view mv_v (a, c) as select a, count(1) from v group by a")
	require.ErrorContains(t, err, "is not BASE TABLE")
}
