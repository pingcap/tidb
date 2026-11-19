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

package mviewnested

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

func TestCreateMaterializedViewOnMaterializedView(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table mv_on_mv_base (a int not null, b int not null)")
	tk.MustExec("insert into mv_on_mv_base values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on mv_on_mv_base (a, b)")
	tk.MustExec("create materialized view mv_parent (a, cnt, total) refresh fast as select a, count(1), sum(b) from mv_on_mv_base group by a")
	tk.MustExec("create materialized view log on mv_parent (a, cnt)")
	tk.MustExec("create materialized view mv_child (a, parent_cnt, child_cnt) refresh fast as select a, sum(cnt), count(1) from mv_parent group by a")

	tk.MustQuery("select a, parent_cnt, child_cnt from mv_child order by a").Check(testkit.Rows("1 2 1", "2 1 1"))

	is := dom.InfoSchema()
	parent, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_parent"))
	require.NoError(t, err)
	child, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_child"))
	require.NoError(t, err)
	parentMLog, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("$mlog$mv_parent"))
	require.NoError(t, err)

	require.Equal(t, []int64{parent.Meta().ID}, child.Meta().MaterializedView.BaseTableIDs)
	require.NotNil(t, parent.Meta().MaterializedViewBase)
	require.Equal(t, parentMLog.Meta().ID, parent.Meta().MaterializedViewBase.MLogID)
	require.Equal(t, parent.Meta().ID, parentMLog.Meta().MaterializedViewLog.BaseTableID)
	require.Contains(t, parent.Meta().MaterializedViewBase.MViewIDs, child.Meta().ID)
	require.Equal(t, []int64{child.Meta().ID}, parentMLog.Meta().MaterializedViewLog.DependentMViewIDs)

	err = tk.ExecToErr("drop materialized view mv_parent")
	require.ErrorContains(t, err, "DROP MATERIALIZED VIEW with materialized view dependencies")
	err = tk.ExecToErr("drop materialized view log on mv_parent")
	require.ErrorContains(t, err, "dependent materialized views exist")

	tk.MustExec("drop materialized view mv_child")
	parent, err = dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_parent"))
	require.NoError(t, err)
	parentMLog, err = dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("$mlog$mv_parent"))
	require.NoError(t, err)
	require.Empty(t, parent.Meta().MaterializedViewBase.MViewIDs)
	require.Empty(t, parentMLog.Meta().MaterializedViewLog.DependentMViewIDs)
	require.Equal(t, parentMLog.Meta().ID, parent.Meta().MaterializedViewBase.MLogID)

	t.Run("initial_build_rollback", func(t *testing.T) {
		testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/mockCreateMaterializedViewBuildErr", `return("context-canceled")`)
		err := tk.ExecToErr("create materialized view mv_child_failed (a, parent_cnt, child_cnt) as select a, sum(cnt), count(1) from mv_parent group by a")
		require.Error(t, err)
		tk.MustQuery("show tables like 'mv_child_failed'").Check(testkit.Rows())
		parent, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_parent"))
		require.NoError(t, err)
		parentMLog, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("$mlog$mv_parent"))
		require.NoError(t, err)
		require.Empty(t, parent.Meta().MaterializedViewBase.MViewIDs)
		require.Empty(t, parentMLog.Meta().MaterializedViewLog.DependentMViewIDs)
		require.Equal(t, parentMLog.Meta().ID, parent.Meta().MaterializedViewBase.MLogID)
	})

	tk.MustExec("drop materialized view log on mv_parent")
	tk.MustExec("drop materialized view mv_parent")
	tk.MustExec("drop materialized view log on mv_on_mv_base")
	tk.MustExec("drop table mv_on_mv_base")
}

func TestMaterializedViewNestedDependencyChain(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table mv_dependency_chain_base (a int not null, b int not null)")
	tk.MustExec("insert into mv_dependency_chain_base values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on mv_dependency_chain_base (a, b)")
	tk.MustExec("create materialized view mv_dependency_chain_1 (a, cnt, total) refresh fast as select a, count(1), sum(b) from mv_dependency_chain_base group by a")
	tk.MustExec("create materialized view log on mv_dependency_chain_1 (a, cnt, total)")
	tk.MustExec("create materialized view mv_dependency_chain_2 (a, total, total_cnt, cnt) refresh fast as select a, sum(total), count(total), count(1) from mv_dependency_chain_1 group by a")
	tk.MustExec("create materialized view log on mv_dependency_chain_2 (a, total, total_cnt, cnt)")
	tk.MustExec("create materialized view mv_dependency_chain_3 (a, total, total_cnt, cnt) refresh fast as select a, sum(total), count(total), count(1) from mv_dependency_chain_2 group by a")

	tk.MustQuery("select a, total, cnt from mv_dependency_chain_3 order by a").Check(testkit.Rows("1 15 1", "2 7 1"))

	is := dom.InfoSchema()
	base, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_dependency_chain_base"))
	require.NoError(t, err)
	mv1, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_dependency_chain_1"))
	require.NoError(t, err)
	mv2, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_dependency_chain_2"))
	require.NoError(t, err)
	mv3, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_dependency_chain_3"))
	require.NoError(t, err)

	require.Equal(t, []int64{base.Meta().ID}, mv1.Meta().MaterializedView.BaseTableIDs)
	require.Equal(t, []int64{mv1.Meta().ID}, mv2.Meta().MaterializedView.BaseTableIDs)
	require.Equal(t, []int64{mv2.Meta().ID}, mv3.Meta().MaterializedView.BaseTableIDs)
	require.Contains(t, base.Meta().MaterializedViewBase.MViewIDs, mv1.Meta().ID)
	require.Contains(t, mv1.Meta().MaterializedViewBase.MViewIDs, mv2.Meta().ID)
	require.Contains(t, mv2.Meta().MaterializedViewBase.MViewIDs, mv3.Meta().ID)

	err = tk.ExecToErr("drop materialized view mv_dependency_chain_1")
	require.ErrorContains(t, err, "DROP MATERIALIZED VIEW with materialized view dependencies")
	err = tk.ExecToErr("drop materialized view mv_dependency_chain_2")
	require.ErrorContains(t, err, "DROP MATERIALIZED VIEW with materialized view dependencies")

	tk.MustExec("drop materialized view mv_dependency_chain_3")
	tk.MustExec("drop materialized view log on mv_dependency_chain_2")
	tk.MustExec("drop materialized view mv_dependency_chain_2")
	tk.MustExec("drop materialized view log on mv_dependency_chain_1")
	tk.MustExec("drop materialized view mv_dependency_chain_1")
	tk.MustExec("drop materialized view log on mv_dependency_chain_base")
	tk.MustExec("drop table mv_dependency_chain_base")
}

func TestCreateMaterializedViewOnMaterializedViewRequiresTrackedColumns(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table mv_on_mv_missing_log_col_base (a int not null, b int not null)")
	tk.MustExec("insert into mv_on_mv_missing_log_col_base values (1, 10)")
	tk.MustExec("create materialized view log on mv_on_mv_missing_log_col_base (a, b)")
	tk.MustExec("create materialized view mv_parent_missing_log_col (a, cnt) as select a, count(1) from mv_on_mv_missing_log_col_base group by a")
	err := tk.ExecToErr("create materialized view mv_child_missing_log_col (a, parent_cnt, child_cnt) as select a, sum(cnt), count(1) from mv_parent_missing_log_col group by a")
	require.ErrorContains(t, err, "materialized view log does not exist for source object test.mv_parent_missing_log_col")
	tk.MustExec("create materialized view log on mv_parent_missing_log_col (a)")

	err = tk.ExecToErr("create materialized view mv_child_missing_log_col (a, parent_cnt, child_cnt) as select a, sum(cnt), count(1) from mv_parent_missing_log_col group by a")
	require.ErrorContains(t, err, "materialized view log does not contain column cnt")
}
