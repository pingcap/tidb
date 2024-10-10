// Copyright 2023 PingCAP, Inc.
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

package physicalplantest

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/planner/util/coretestsdk"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/external"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/stretchr/testify/require"
)

func assertSameHints(t *testing.T, expected, actual []*ast.TableOptimizerHint) {
	expectedStr := make([]string, 0, len(expected))
	actualStr := make([]string, 0, len(actual))
	for _, h := range expected {
		expectedStr = append(expectedStr, hint.RestoreTableOptimizerHint(h))
	}
	for _, h := range actual {
		actualStr = append(actualStr, hint.RestoreTableOptimizerHint(h))
	}
	require.ElementsMatch(t, expectedStr, actualStr)
}

func TestRefine(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")

	var input []string
	var output []struct {
		SQL  string
		Best string
	}
	planSuiteData := GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	p := parser.New()
	is := infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable()})
	for i, tt := range input {
		comment := fmt.Sprintf("input: %s", tt)
		stmt, err := p.ParseOneStmt(tt, "", "")
		require.NoError(t, err, comment)
		sc := tk.Session().GetSessionVars().StmtCtx
		sc.SetTypeFlags(sc.TypeFlags().WithIgnoreTruncateErr(false))
		nodeW := resolve.NewNodeW(stmt)
		p, _, err := planner.Optimize(context.TODO(), tk.Session(), nodeW, is)
		require.NoError(t, err, comment)
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Best = core.ToString(p)
		})
		require.Equal(t, output[i].Best, core.ToString(p), comment)
	}
}

func TestAggEliminator(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("set tidb_opt_limit_push_down_threshold=0")
	tk.MustExec("set sql_mode='STRICT_TRANS_TABLES'") // disable only full group by
	var input []string
	var output []struct {
		SQL  string
		Best string
	}
	planSuiteData := GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	p := parser.New()
	is := infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable()})
	for i, tt := range input {
		comment := fmt.Sprintf("input: %s", tt)
		stmt, err := p.ParseOneStmt(tt, "", "")
		require.NoError(t, err, comment)
		sc := tk.Session().GetSessionVars().StmtCtx
		sc.SetTypeFlags(sc.TypeFlags().WithIgnoreTruncateErr(false))
		nodeW := resolve.NewNodeW(stmt)
		p, _, err := planner.Optimize(context.TODO(), tk.Session(), nodeW, is)
		require.NoError(t, err)
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Best = core.ToString(p)
		})
		require.Equal(t, output[i].Best, core.ToString(p), fmt.Sprintf("input: %s", tt))
	}
}

// Fix Issue #45822
func TestRuleColumnPruningLogicalApply(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	var input []string
	var output []struct {
		SQL  string
		Best string
	}
	planSuiteData := GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	p := parser.New()
	is := infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable()})
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_opt_fix_control = '45822:ON';")
	for i, tt := range input {
		comment := fmt.Sprintf("input: %s", tt)
		stmt, err := p.ParseOneStmt(tt, "", "")
		require.NoError(t, err, comment)
		nodeW := resolve.NewNodeW(stmt)
		p, _, err := planner.Optimize(context.TODO(), tk.Session(), nodeW, is)
		require.NoError(t, err)
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Best = core.ToString(p)
		})
		require.Equal(t, output[i].Best, core.ToString(p), fmt.Sprintf("input: %s", tt))
	}
}

func TestSemiJoinToInner(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")

	var input []string
	var output []struct {
		SQL  string
		Best string
	}
	planSuiteData := GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)

	p := parser.New()
	is := infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable()})
	for i, tt := range input {
		stmt, err := p.ParseOneStmt(tt, "", "")
		require.NoError(t, err)
		nodeW := resolve.NewNodeW(stmt)
		p, _, err := planner.Optimize(context.TODO(), tk.Session(), nodeW, is)
		require.NoError(t, err)
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Best = core.ToString(p)
		})
		require.Equal(t, output[i].Best, core.ToString(p))
	}
}

func TestUnmatchedTableInHint(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	var input []string
	var output []struct {
		SQL     string
		Warning string
	}
	planSuiteData := GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	p := parser.New()
	is := infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable()})
	for i, test := range input {
		tk.Session().GetSessionVars().StmtCtx.SetWarnings(nil)
		stmt, err := p.ParseOneStmt(test, "", "")
		require.NoError(t, err)
		nodeW := resolve.NewNodeW(stmt)
		_, _, err = planner.Optimize(context.TODO(), tk.Session(), nodeW, is)
		require.NoError(t, err)
		warnings := tk.Session().GetSessionVars().StmtCtx.GetWarnings()
		testdata.OnRecord(func() {
			output[i].SQL = test
			if len(warnings) > 0 {
				output[i].Warning = warnings[0].Err.Error()
			}
		})
		if output[i].Warning == "" {
			require.Len(t, warnings, 0)
		} else {
			require.Len(t, warnings, 1)
			require.Equal(t, contextutil.WarnLevelWarning, warnings[0].Level)
			require.Equal(t, output[i].Warning, warnings[0].Err.Error())
		}
	}
}

func TestIssue37520(t *testing.T) {
	store := testkit.CreateMockStore(t, mockstore.WithMockTiFlash(2))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int primary key, b int);")
	tk.MustExec("create table t2(a int, b int, index ia(a));")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Warn []string
	}

	planSuiteData := GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)

	for i, ts := range input {
		testdata.OnRecord(func() {
			output[i].SQL = ts
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + ts).Rows())
			output[i].Warn = testdata.ConvertSQLWarnToStrings(tk.Session().GetSessionVars().StmtCtx.GetWarnings())
		})
		tk.MustQuery("explain format = 'brief' " + ts).Check(testkit.Rows(output[i].Plan...))
		require.Equal(t, output[i].Warn, testdata.ConvertSQLWarnToStrings(tk.Session().GetSessionVars().StmtCtx.GetWarnings()))
	}
}

func TestMPPHints(t *testing.T) {
	store := testkit.CreateMockStore(t, mockstore.WithMockTiFlash(2))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("create table t (a int, b int, c int, index idx_a(a), index idx_b(b))")
	tk.MustExec("alter table t set tiflash replica 1")
	tk.MustExec("set @@session.tidb_allow_mpp=ON")
	tk.MustExec("create definer='root'@'localhost' view v as select a, sum(b) from t group by a, c;")
	tk.MustExec("create definer='root'@'localhost' view v1 as select t1.a from t t1, t t2 where t1.a=t2.a;")
	tb := external.GetTableByName(t, tk, "test", "t")
	err := domain.GetDomain(tk.Session()).DDLExecutor().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)

	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Warn []string
	}

	planSuiteData := GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)

	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
		})
		if strings.HasPrefix(tt, "set") || strings.HasPrefix(tt, "UPDATE") {
			tk.MustExec(tt)
			continue
		}
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + tt).Rows())
			output[i].Warn = testdata.ConvertSQLWarnToStrings(tk.Session().GetSessionVars().StmtCtx.GetWarnings())
		})
		tk.MustQuery("explain format = 'brief' " + tt).Check(testkit.Rows(output[i].Plan...))
		require.Equal(t, output[i].Warn, testdata.ConvertSQLWarnToStrings(tk.Session().GetSessionVars().StmtCtx.GetWarnings()))
	}
}

func TestMPPHintsScope(t *testing.T) {
	store := testkit.CreateMockStore(t, mockstore.WithMockTiFlash(2))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("create table t (a int, b int, c int, index idx_a(a), index idx_b(b))")
	tk.MustExec("select /*+ MPP_1PHASE_AGG() */ a, sum(b) from t group by a, c")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1815 The agg can not push down to the MPP side, the MPP_1PHASE_AGG() hint is invalid"))
	tk.MustExec("select /*+ MPP_2PHASE_AGG() */ a, sum(b) from t group by a, c")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1815 The agg can not push down to the MPP side, the MPP_2PHASE_AGG() hint is invalid"))
	tk.MustExec("select /*+ shuffle_join(t1, t2) */ * from t t1, t t2 where t1.a=t2.a")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1815 The join can not push down to the MPP side, the shuffle_join() hint is invalid"))
	tk.MustExec("select /*+ broadcast_join(t1, t2) */ * from t t1, t t2 where t1.a=t2.a")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1815 The join can not push down to the MPP side, the broadcast_join() hint is invalid"))
	tk.MustExec("alter table t set tiflash replica 1")
	tb := external.GetTableByName(t, tk, "test", "t")
	err := domain.GetDomain(tk.Session()).DDLExecutor().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)

	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Warn []string
	}
	planSuiteData := GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
		})
		if strings.HasPrefix(tt, "set") || strings.HasPrefix(tt, "UPDATE") {
			tk.MustExec(tt)
			continue
		}
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
			output[i].Warn = testdata.ConvertSQLWarnToStrings(tk.Session().GetSessionVars().StmtCtx.GetWarnings())
		})
		res := tk.MustQuery(tt)
		res.Check(testkit.Rows(output[i].Plan...))
		require.Equal(t, output[i].Warn, testdata.ConvertSQLWarnToStrings(tk.Session().GetSessionVars().StmtCtx.GetWarnings()))
	}
}

func TestMPPBCJModel(t *testing.T) {
	/*
		if there are 3 mpp stores, planner won't choose broadcast join enven if `tidb_prefer_broadcast_join_by_exchange_data_size` is ON
		broadcast exchange size:
			Build: 2 * sizeof(Data)
			Probe: 0
			exchange size: Build = 2 * sizeof(Data)
		hash exchange size:
			Build: sizeof(Data) * 2 / 3
			Probe: sizeof(Data) * 2 / 3
			exchange size: Build + Probe = 4/3 * sizeof(Data)
	*/
	store := testkit.CreateMockStore(t, mockstore.WithMockTiFlash(3))
	{
		cnt, err := store.GetMPPClient().GetMPPStoreCount()
		require.Equal(t, cnt, 3)
		require.Nil(t, err)
	}
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, b int, c int, index idx_a(a), index idx_b(b))")
	tk.MustExec("alter table t set tiflash replica 1")
	tb := external.GetTableByName(t, tk, "test", "t")
	err := domain.GetDomain(tk.Session()).DDLExecutor().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)

	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Warn []string
	}
	planSuiteData := GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
		})
		if strings.HasPrefix(tt, "set") || strings.HasPrefix(tt, "UPDATE") {
			tk.MustExec(tt)
			continue
		}
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
			output[i].Warn = testdata.ConvertSQLWarnToStrings(tk.Session().GetSessionVars().StmtCtx.GetWarnings())
		})
		res := tk.MustQuery(tt)
		res.Check(testkit.Rows(output[i].Plan...))
		require.Equal(t, output[i].Warn, testdata.ConvertSQLWarnToStrings(tk.Session().GetSessionVars().StmtCtx.GetWarnings()))
	}
}

func TestMPPPreferBCJ(t *testing.T) {
	store := testkit.CreateMockStore(t, mockstore.WithMockTiFlash(3))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int)")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t2 (b int)")

	tk.MustExec("insert into t1 values (1);")
	tk.MustExec("insert into t2 values (1), (2), (3), (4), (5), (6), (7), (8);")

	{
		tk.MustExec("alter table t1 set tiflash replica 1")
		tb := external.GetTableByName(t, tk, "test", "t1")
		err := domain.GetDomain(tk.Session()).DDLExecutor().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
		require.NoError(t, err)
	}
	{
		tk.MustExec("alter table t2 set tiflash replica 1")
		tb := external.GetTableByName(t, tk, "test", "t2")
		err := domain.GetDomain(tk.Session()).DDLExecutor().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
		require.NoError(t, err)
	}
	tk.MustExec("analyze table t1 all columns")
	tk.MustExec("analyze table t2 all columns")
	tk.MustExec("set @@tidb_allow_mpp=1; set @@tidb_enforce_mpp=1;")
	{
		var input []string
		var output []struct {
			SQL  string
			Plan []string
			Warn []string
		}
		planSuiteData := GetPlanSuiteData()
		planSuiteData.LoadTestCases(t, &input, &output)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i].SQL = tt
			})
			if strings.HasPrefix(tt, "set") || strings.HasPrefix(tt, "insert") {
				tk.MustExec(tt)
				continue
			}
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
				output[i].Warn = testdata.ConvertSQLWarnToStrings(tk.Session().GetSessionVars().StmtCtx.GetWarnings())
			})
			res := tk.MustQuery(tt)
			res.Check(testkit.Rows(output[i].Plan...))
			require.Equal(t, output[i].Warn, testdata.ConvertSQLWarnToStrings(tk.Session().GetSessionVars().StmtCtx.GetWarnings()))
		}
	}
}

func TestMPPBCJModelOneTiFlash(t *testing.T) {
	/*
		if there are 1 mpp stores, planner should choose broadcast join if `tidb_prefer_broadcast_join_by_exchange_data_size` is ON
		broadcast exchange size:
			Build: 0 * sizeof(Data)
			Probe: 0
			exchange size: Build = 0 * sizeof(Data)
		hash exchange size:
			Build: sizeof(Data) * 0 / 1
			Probe: sizeof(Data) * 0 / 1
			exchange size: Build + Probe = 0 * sizeof(Data)
	*/
	store := testkit.CreateMockStore(t, mockstore.WithMockTiFlash(1))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, b int, c int, index idx_a(a), index idx_b(b))")
	tk.MustExec("alter table t set tiflash replica 1")
	tb := external.GetTableByName(t, tk, "test", "t")
	err := domain.GetDomain(tk.Session()).DDLExecutor().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)
	{
		cnt, err := store.GetMPPClient().GetMPPStoreCount()
		require.Equal(t, cnt, 1)
		require.Nil(t, err)
	}
	{
		tk.MustExecToErr("set @@session.tidb_prefer_broadcast_join_by_exchange_data_size=-1")
		tk.MustExecToErr("set @@session.tidb_prefer_broadcast_join_by_exchange_data_size=2")
	}
	{
		// no BCJ if `tidb_prefer_broadcast_join_by_exchange_data_size` is OFF
		tk.MustExec("set @@session.tidb_broadcast_join_threshold_size=0")
		tk.MustExec("set @@session.tidb_broadcast_join_threshold_count=0")
	}

	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Warn []string
	}
	planSuiteData := GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
		})
		if strings.HasPrefix(tt, "set") || strings.HasPrefix(tt, "UPDATE") {
			tk.MustExec(tt)
			continue
		}
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
			output[i].Warn = testdata.ConvertSQLWarnToStrings(tk.Session().GetSessionVars().StmtCtx.GetWarnings())
		})
		res := tk.MustQuery(tt)
		res.Check(testkit.Rows(output[i].Plan...))
		require.Equal(t, output[i].Warn, testdata.ConvertSQLWarnToStrings(tk.Session().GetSessionVars().StmtCtx.GetWarnings()))
	}
}

func TestMPPRightSemiJoin(t *testing.T) {
	store := testkit.CreateMockStore(t, mockstore.WithMockTiFlash(3))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int)")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t2 (b int)")

	tk.MustExec("insert into t1 values (1);")
	tk.MustExec("insert into t2 values (1), (2), (3), (4), (5), (6), (7), (8);")

	{
		tk.MustExec("alter table t1 set tiflash replica 1")
		tb := external.GetTableByName(t, tk, "test", "t1")
		err := domain.GetDomain(tk.Session()).DDLExecutor().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
		require.NoError(t, err)
	}
	{
		tk.MustExec("alter table t2 set tiflash replica 1")
		tb := external.GetTableByName(t, tk, "test", "t2")
		err := domain.GetDomain(tk.Session()).DDLExecutor().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
		require.NoError(t, err)
	}
	tk.MustExec("analyze table t1 all columns")
	tk.MustExec("analyze table t2 all columns")
	tk.MustExec("set @@tidb_allow_mpp=1; set @@tidb_enforce_mpp=1;")
	{
		var input []string
		var output []struct {
			SQL  string
			Plan []string
			Warn []string
		}
		planSuiteData := GetPlanSuiteData()
		planSuiteData.LoadTestCases(t, &input, &output)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i].SQL = tt
			})
			if strings.HasPrefix(tt, "set") || strings.HasPrefix(tt, "insert") {
				tk.MustExec(tt)
				continue
			}
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
				output[i].Warn = testdata.ConvertSQLWarnToStrings(tk.Session().GetSessionVars().StmtCtx.GetWarnings())
			})
			res := tk.MustQuery(tt)
			res.Check(testkit.Rows(output[i].Plan...))
			require.Equal(t, output[i].Warn, testdata.ConvertSQLWarnToStrings(tk.Session().GetSessionVars().StmtCtx.GetWarnings()))
		}
	}
}

func TestMPPRightOuterJoin(t *testing.T) {
	store := testkit.CreateMockStore(t, mockstore.WithMockTiFlash(3))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int, c int)")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t2 (b int, d int)")

	tk.MustExec("insert into t1 values (1, 10), (2, 20), (3, 30), (4, 40), (5, 50);")
	tk.MustExec("insert into t2 values (1, 12), (2, 18), (7, 66);")

	{
		tk.MustExec("alter table t1 set tiflash replica 1")
		tb := external.GetTableByName(t, tk, "test", "t1")
		err := domain.GetDomain(tk.Session()).DDLExecutor().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
		require.NoError(t, err)
	}
	{
		tk.MustExec("alter table t2 set tiflash replica 1")
		tb := external.GetTableByName(t, tk, "test", "t2")
		err := domain.GetDomain(tk.Session()).DDLExecutor().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
		require.NoError(t, err)
	}
	tk.MustExec("analyze table t1 all columns")
	tk.MustExec("analyze table t2 all columns")
	tk.MustExec("set @@tidb_allow_mpp=1; set @@tidb_enforce_mpp=1;")
	{
		var input []string
		var output []struct {
			SQL  string
			Plan []string
			Warn []string
		}
		planSuiteData := GetPlanSuiteData()
		planSuiteData.LoadTestCases(t, &input, &output)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i].SQL = tt
			})
			if strings.HasPrefix(tt, "set") || strings.HasPrefix(tt, "insert") {
				tk.MustExec(tt)
				continue
			}
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
				output[i].Warn = testdata.ConvertSQLWarnToStrings(tk.Session().GetSessionVars().StmtCtx.GetWarnings())
			})
			res := tk.MustQuery(tt)
			res.Check(testkit.Rows(output[i].Plan...))
			require.Equal(t, output[i].Warn, testdata.ConvertSQLWarnToStrings(tk.Session().GetSessionVars().StmtCtx.GetWarnings()))
		}
	}
}

func TestHintScope(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec(`set @@tidb_opt_advanced_join_hint=0`)

	var input []string
	var output []struct {
		SQL  string
		Best string
	}
	planSuiteData := GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	p := parser.New()
	is := infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable()})

	for i, test := range input {
		comment := fmt.Sprintf("case:%v sql:%s", i, test)
		stmt, err := p.ParseOneStmt(test, "", "")
		require.NoError(t, err, comment)

		nodeW := resolve.NewNodeW(stmt)
		p, _, err := planner.Optimize(context.Background(), tk.Session(), nodeW, is)
		require.NoError(t, err)
		testdata.OnRecord(func() {
			output[i].SQL = test
			output[i].Best = core.ToString(p)
		})
		require.Equal(t, output[i].Best, core.ToString(p))
		warnings := tk.Session().GetSessionVars().StmtCtx.GetWarnings()
		require.Len(t, warnings, 0, comment)
	}
}

func TestJoinHints(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")

	var input []string
	var output []struct {
		SQL     string
		Best    string
		Warning string
		Hints   string
	}
	planSuiteData := GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	ctx := context.Background()
	p := parser.New()
	is := infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable()})

	for i, test := range input {
		comment := fmt.Sprintf("case:%v sql:%s", i, test)
		stmt, err := p.ParseOneStmt(test, "", "")
		require.NoError(t, err, comment)

		tk.Session().GetSessionVars().StmtCtx.SetWarnings(nil)
		nodeW := resolve.NewNodeW(stmt)
		p, _, err := planner.Optimize(ctx, tk.Session(), nodeW, is)
		require.NoError(t, err)
		warnings := tk.Session().GetSessionVars().StmtCtx.GetWarnings()

		testdata.OnRecord(func() {
			output[i].SQL = test
			output[i].Best = core.ToString(p)
			if len(warnings) > 0 {
				output[i].Warning = warnings[0].Err.Error()
			}
			output[i].Hints = hint.RestoreOptimizerHints(core.GenHintsFromPhysicalPlan(p))
		})
		require.Equal(t, output[i].Best, core.ToString(p))
		if output[i].Warning == "" {
			require.Len(t, warnings, 0)
		} else {
			require.Len(t, warnings, 1, fmt.Sprintf("%v", warnings))
			require.Equal(t, contextutil.WarnLevelWarning, warnings[0].Level)
			require.Equal(t, output[i].Warning, warnings[0].Err.Error())
		}
		hints := core.GenHintsFromPhysicalPlan(p)

		// test the new genHints code
		flat := core.FlattenPhysicalPlan(p, false)
		newHints := core.GenHintsFromFlatPlan(flat)
		assertSameHints(t, hints, newHints)

		require.Equal(t, output[i].Hints, hint.RestoreOptimizerHints(hints), comment)
	}
}

func TestAggregationHints(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	sessionVars := tk.Session().GetSessionVars()
	sessionVars.SetHashAggFinalConcurrency(1)
	sessionVars.SetHashAggPartialConcurrency(1)

	var input []struct {
		SQL         string
		AggPushDown bool
	}
	var output []struct {
		SQL     string
		Best    string
		Warning string
	}
	planSuiteData := GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	ctx := context.Background()
	p := parser.New()
	is := infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable()})
	for i, test := range input {
		comment := fmt.Sprintf("case: %v sql: %v", i, test)
		tk.Session().GetSessionVars().StmtCtx.SetWarnings(nil)
		tk.Session().GetSessionVars().AllowAggPushDown = test.AggPushDown

		stmt, err := p.ParseOneStmt(test.SQL, "", "")
		require.NoError(t, err, comment)

		nodeW := resolve.NewNodeW(stmt)
		p, _, err := planner.Optimize(ctx, tk.Session(), nodeW, is)
		require.NoError(t, err)
		warnings := tk.Session().GetSessionVars().StmtCtx.GetWarnings()

		testdata.OnRecord(func() {
			output[i].SQL = test.SQL
			output[i].Best = core.ToString(p)
			if len(warnings) > 0 {
				output[i].Warning = warnings[0].Err.Error()
			}
		})
		require.Equal(t, output[i].Best, core.ToString(p), comment)
		if output[i].Warning == "" {
			require.Len(t, warnings, 0)
		} else {
			require.Len(t, warnings, 1, fmt.Sprintf("%v", warnings))
			require.Equal(t, contextutil.WarnLevelWarning, warnings[0].Level)
			require.Equal(t, output[i].Warning, warnings[0].Err.Error())
		}
	}
}

func TestSemiJoinRewriteHints(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("create table t(a int, b int, c int)")

	sessionVars := tk.Session().GetSessionVars()
	sessionVars.SetHashAggFinalConcurrency(1)
	sessionVars.SetHashAggPartialConcurrency(1)

	var input []string
	var output []struct {
		SQL     string
		Plan    []string
		Warning string
	}
	planSuiteData := GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	ctx := context.Background()
	p := parser.New()
	is := infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable()})
	for i, test := range input {
		comment := fmt.Sprintf("case: %v sql: %v", i, test)
		tk.Session().GetSessionVars().StmtCtx.SetWarnings(nil)

		stmt, err := p.ParseOneStmt(test, "", "")
		require.NoError(t, err, comment)

		nodeW := resolve.NewNodeW(stmt)
		_, _, err = planner.Optimize(ctx, tk.Session(), nodeW, is)
		require.NoError(t, err)
		warnings := tk.Session().GetSessionVars().StmtCtx.GetWarnings()

		testdata.OnRecord(func() {
			output[i].SQL = test
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief'" + test).Rows())
			if len(warnings) > 0 {
				output[i].Warning = warnings[0].Err.Error()
			}
		})
		tk.MustQuery("explain format = 'brief'" + test).Check(testkit.Rows(output[i].Plan...))
		if output[i].Warning == "" {
			require.Len(t, warnings, 0)
		} else {
			require.Len(t, warnings, 1, fmt.Sprintf("%v", warnings))
			require.Equal(t, contextutil.WarnLevelWarning, warnings[0].Level)
			require.Equal(t, output[i].Warning, warnings[0].Err.Error())
		}
	}
}

func TestAggToCopHint(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists ta")
	tk.MustExec("create table ta(a int, b int, index(a))")

	var (
		input  []string
		output []struct {
			SQL     string
			Best    string
			Warning string
		}
	)
	planSuiteData := GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)

	ctx := context.Background()
	is := domain.GetDomain(tk.Session()).InfoSchema()
	p := parser.New()
	for i, test := range input {
		comment := fmt.Sprintf("case:%v sql:%s", i, test)
		testdata.OnRecord(func() {
			output[i].SQL = test
		})
		require.Equal(t, output[i].SQL, test, comment)

		tk.Session().GetSessionVars().StmtCtx.SetWarnings(nil)

		stmt, err := p.ParseOneStmt(test, "", "")
		require.NoError(t, err, comment)

		nodeW := resolve.NewNodeW(stmt)
		p, _, err := planner.Optimize(ctx, tk.Session(), nodeW, is)
		require.NoError(t, err, comment)
		planString := core.ToString(p)
		testdata.OnRecord(func() {
			output[i].Best = planString
		})
		require.Equal(t, output[i].Best, planString, comment)

		warnings := tk.Session().GetSessionVars().StmtCtx.GetWarnings()
		testdata.OnRecord(func() {
			if len(warnings) > 0 {
				output[i].Warning = warnings[0].Err.Error()
			}
		})
		if output[i].Warning == "" {
			require.Len(t, warnings, 0)
		} else {
			require.Len(t, warnings, 1, fmt.Sprintf("%v", warnings))
			require.Equal(t, contextutil.WarnLevelWarning, warnings[0].Level)
			require.Equal(t, output[i].Warning, warnings[0].Err.Error())
		}
	}
}

func TestGroupConcatOrderby(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune", `return(true)`)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune")
	var (
		input  []string
		output []struct {
			SQL    string
			Plan   []string
			Result []string
		}
	)
	planSuiteData := GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists test;")
	tk.MustExec("create table test(id int, name int)")
	tk.MustExec("insert into test values(1, 10);")
	tk.MustExec("insert into test values(1, 20);")
	tk.MustExec("insert into test values(1, 30);")
	tk.MustExec("insert into test values(2, 20);")
	tk.MustExec("insert into test values(3, 200);")
	tk.MustExec("insert into test values(3, 500);")

	tk.MustExec("drop table if exists ptest;")
	tk.MustExec("CREATE TABLE ptest (id int,name int) PARTITION BY RANGE ( id ) " +
		"(PARTITION `p0` VALUES LESS THAN (2), PARTITION `p1` VALUES LESS THAN (11))")
	tk.MustExec("insert into ptest select * from test;")
	tk.MustExec(fmt.Sprintf("set session tidb_opt_distinct_agg_push_down = %v", 1))
	tk.MustExec(fmt.Sprintf("set session tidb_opt_agg_push_down = %v", 1))

	for i, ts := range input {
		testdata.OnRecord(func() {
			output[i].SQL = ts
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + ts).Rows())
			output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(ts).Sort().Rows())
		})
		tk.MustQuery("explain format = 'brief' " + ts).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(ts).Check(testkit.Rows(output[i].Result...))
	}
}

func TestIndexHint(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	var input []string
	var output []struct {
		SQL     string
		Best    string
		HasWarn bool
		Hints   string
	}
	planSuiteData := GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	ctx := context.Background()
	p := parser.New()
	is := infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable()})

	for i, test := range input {
		comment := fmt.Sprintf("case:%v sql:%s", i, test)
		tk.Session().GetSessionVars().StmtCtx.SetWarnings(nil)

		stmt, err := p.ParseOneStmt(test, "", "")
		require.NoError(t, err, comment)

		nodeW := resolve.NewNodeW(stmt)
		p, _, err := planner.Optimize(ctx, tk.Session(), nodeW, is)
		require.NoError(t, err)
		testdata.OnRecord(func() {
			output[i].SQL = test
			output[i].Best = core.ToString(p)
			output[i].HasWarn = len(tk.Session().GetSessionVars().StmtCtx.GetWarnings()) > 0
			output[i].Hints = hint.RestoreOptimizerHints(core.GenHintsFromPhysicalPlan(p))
		})
		require.Equal(t, output[i].Best, core.ToString(p), comment)
		warnings := tk.Session().GetSessionVars().StmtCtx.GetWarnings()
		if output[i].HasWarn {
			require.Len(t, warnings, 1, comment)
		} else {
			require.Len(t, warnings, 0, comment)
		}
		hints := core.GenHintsFromPhysicalPlan(p)

		// test the new genHints code
		flat := core.FlattenPhysicalPlan(p, false)
		newHints := core.GenHintsFromFlatPlan(flat)
		assertSameHints(t, hints, newHints)

		require.Equal(t, output[i].Hints, hint.RestoreOptimizerHints(hints), comment)
	}
}

func TestIndexMergeHint(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")

	var input []string
	var output []struct {
		SQL     string
		Best    string
		HasWarn bool
		Hints   string
	}
	planSuiteData := GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	ctx := context.Background()
	p := parser.New()
	is := infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable()})

	for i, test := range input {
		comment := fmt.Sprintf("case:%v sql:%s", i, test)
		tk.Session().GetSessionVars().StmtCtx.SetWarnings(nil)
		stmt, err := p.ParseOneStmt(test, "", "")
		require.NoError(t, err, comment)
		sctx := tk.Session()
		err = executor.ResetContextOfStmt(sctx, stmt)
		require.NoError(t, err)
		nodeW := resolve.NewNodeW(stmt)
		p, _, err := planner.Optimize(ctx, tk.Session(), nodeW, is)
		require.NoError(t, err)
		testdata.OnRecord(func() {
			output[i].SQL = test
			output[i].Best = core.ToString(p)
			output[i].HasWarn = len(tk.Session().GetSessionVars().StmtCtx.GetWarnings()) > 0
			output[i].Hints = hint.RestoreOptimizerHints(core.GenHintsFromPhysicalPlan(p))
		})
		require.Equal(t, output[i].Best, core.ToString(p), comment)
		warnings := tk.Session().GetSessionVars().StmtCtx.GetWarnings()
		if output[i].HasWarn {
			require.Len(t, warnings, 1, comment)
		} else {
			require.Len(t, warnings, 0, comment)
		}
		hints := core.GenHintsFromPhysicalPlan(p)

		// test the new genHints code
		flat := core.FlattenPhysicalPlan(p, false)
		newHints := core.GenHintsFromFlatPlan(flat)
		assertSameHints(t, hints, newHints)

		require.Equal(t, output[i].Hints, hint.RestoreOptimizerHints(hints), comment)
	}
}

func TestQueryBlockHint(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")

	var input []string
	var output []struct {
		SQL   string
		Plan  string
		Hints string
	}
	planSuiteData := GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	ctx := context.TODO()
	p := parser.New()
	is := infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable()})

	for i, tt := range input {
		comment := fmt.Sprintf("case:%v sql: %s", i, tt)
		stmt, err := p.ParseOneStmt(tt, "", "")
		require.NoError(t, err, comment)

		nodeW := resolve.NewNodeW(stmt)
		p, _, err := planner.Optimize(ctx, tk.Session(), nodeW, is)
		require.NoError(t, err, comment)
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = core.ToString(p)
			output[i].Hints = hint.RestoreOptimizerHints(core.GenHintsFromPhysicalPlan(p))
		})
		require.Equal(t, output[i].Plan, core.ToString(p), comment)
		hints := core.GenHintsFromPhysicalPlan(p)

		// test the new genHints code
		flat := core.FlattenPhysicalPlan(p, false)
		newHints := core.GenHintsFromFlatPlan(flat)
		assertSameHints(t, hints, newHints)

		require.Equal(t, output[i].Hints, hint.RestoreOptimizerHints(hints), comment)
	}
}

func TestInlineProjection(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists test.t1, test.t2;`)
	tk.MustExec(`create table test.t1(a bigint, b bigint, index idx_a(a), index idx_b(b));`)
	tk.MustExec(`create table test.t2(a bigint, b bigint, index idx_a(a), index idx_b(b));`)

	var input []string
	var output []struct {
		SQL   string
		Plan  string
		Hints string
	}
	is := domain.GetDomain(tk.Session()).InfoSchema()
	planSuiteData := GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)

	ctx := context.Background()
	p := parser.New()

	for i, tt := range input {
		comment := fmt.Sprintf("case:%v sql: %s", i, tt)
		stmt, err := p.ParseOneStmt(tt, "", "")
		require.NoError(t, err, comment)

		nodeW := resolve.NewNodeW(stmt)
		p, _, err := planner.Optimize(ctx, tk.Session(), nodeW, is)
		require.NoError(t, err, comment)
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = core.ToString(p)
			output[i].Hints = hint.RestoreOptimizerHints(core.GenHintsFromPhysicalPlan(p))
		})
		require.Equal(t, output[i].Plan, core.ToString(p), comment)
		hints := core.GenHintsFromPhysicalPlan(p)

		// test the new genHints code
		flat := core.FlattenPhysicalPlan(p, false)
		newHints := core.GenHintsFromFlatPlan(flat)
		assertSameHints(t, hints, newHints)

		require.Equal(t, output[i].Hints, hint.RestoreOptimizerHints(hints), comment)
	}
}

func TestIndexJoinHint(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec(`drop table if exists test.t1, test.t2, test.t;`)
	tk.MustExec(`create table test.t1(a bigint, b bigint, index idx_a(a), index idx_b(b));`)
	tk.MustExec(`create table test.t2(a bigint, b bigint, index idx_a(a), index idx_b(b));`)
	tk.MustExec("CREATE TABLE `t` ( `a` bigint(20) NOT NULL, `b` tinyint(1) DEFAULT NULL, `c` datetime DEFAULT NULL, `d` int(10) unsigned DEFAULT NULL, `e` varchar(20) DEFAULT NULL, `f` double DEFAULT NULL, `g` decimal(30,5) DEFAULT NULL, `h` float DEFAULT NULL, `i` date DEFAULT NULL, `j` timestamp NULL DEFAULT NULL, PRIMARY KEY (`a`), UNIQUE KEY `b` (`b`), KEY `c` (`c`,`d`,`e`), KEY `f` (`f`), KEY `g` (`g`,`h`), KEY `g_2` (`g`), UNIQUE KEY `g_3` (`g`), KEY `i` (`i`) );")

	var input []string
	var output []struct {
		SQL   string
		Plan  string
		Warns []string
	}

	is := domain.GetDomain(tk.Session()).InfoSchema()
	p := parser.New()
	ctx := context.Background()

	planSuiteData := GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	filterWarnings := func(originalWarnings []contextutil.SQLWarn) []contextutil.SQLWarn {
		warnings := make([]contextutil.SQLWarn, 0, 4)
		for _, warning := range originalWarnings {
			// filter out warning about skyline pruning
			if !strings.Contains(warning.Err.Error(), "remain after pruning paths for") {
				warnings = append(warnings, warning)
			}
		}
		return warnings
	}
	for i, tt := range input {
		comment := fmt.Sprintf("case:%v sql: %s", i, tt)
		stmt, err := p.ParseOneStmt(tt, "", "")
		require.NoError(t, err, comment)
		nodeW := resolve.NewNodeW(stmt)
		p, _, err := planner.Optimize(ctx, tk.Session(), nodeW, is)
		require.NoError(t, err, comment)
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = core.ToString(p)
			output[i].Warns = testdata.ConvertSQLWarnToStrings(filterWarnings(tk.Session().GetSessionVars().StmtCtx.GetWarnings()))
		})
		tk.Session().GetSessionVars().StmtCtx.TruncateWarnings(0)
		require.Equal(t, output[i].Plan, core.ToString(p), comment)
	}
}

func TestHintFromDiffDatabase(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists test.t1`)
	tk.MustExec(`create table test.t1(a bigint, index idx_a(a));`)
	tk.MustExec(`create table test.t2(a bigint, index idx_a(a));`)
	tk.MustExec("drop database if exists test2")
	tk.MustExec("create database test2")
	tk.MustExec("use test2")

	var input []string
	var output []struct {
		SQL  string
		Plan string
	}
	is := domain.GetDomain(tk.Session()).InfoSchema()
	p := parser.New()
	ctx := context.Background()

	planSuiteData := GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		comment := fmt.Sprintf("case:%v sql: %s", i, tt)
		stmt, err := p.ParseOneStmt(tt, "", "")
		require.NoError(t, err, comment)
		nodeW := resolve.NewNodeW(stmt)
		p, _, err := planner.Optimize(ctx, tk.Session(), nodeW, is)
		require.NoError(t, err, comment)
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = core.ToString(p)
		})
		require.Equal(t, output[i].Plan, core.ToString(p), comment)
	}
}

func TestHJBuildAndProbeHint4DynamicPartitionTable(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune", `return(true)`)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune")

	var (
		input  []string
		output []struct {
			SQL     string
			Plan    []string
			Result  []string
			Warning []string
		}
	)
	planSuiteData := GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2, t3")
	tk.MustExec(`create table t1(a int, b int) partition by hash(a) partitions 4`)
	tk.MustExec(`create table t2(a int, b int) partition by hash(a) partitions 5`)
	tk.MustExec(`create table t3(a int, b int) partition by hash(b) partitions 3`)
	tk.MustExec("insert into t1 values(1,1),(2,2)")
	tk.MustExec("insert into t2 values(1,1),(2,1)")
	tk.MustExec("insert into t3 values(1,1),(2,1)")
	tk.MustExec(`set @@tidb_partition_prune_mode="dynamic"`)

	for i, ts := range input {
		testdata.OnRecord(func() {
			output[i].SQL = ts
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + ts).Rows())
			output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(ts).Sort().Rows())
			output[i].Warning = testdata.ConvertRowsToStrings(tk.MustQuery("show warnings").Rows())
		})
		tk.MustQuery("explain format = 'brief' " + ts).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(ts).Sort().Check(testkit.Rows(output[i].Result...))
	}
}

func TestHJBuildAndProbeHint4TiFlash(t *testing.T) {
	var (
		input  []string
		output []struct {
			SQL     string
			Plan    []string
			Warning []string
		}
	)
	planSuiteData := GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("drop table if exists t1, t2, t3")
	tk.MustExec("create table t1(a int primary key, b int not null)")
	tk.MustExec("create table t2(a int primary key, b int not null)")
	tk.MustExec("create table t3(a int primary key, b int not null)")
	tk.MustExec("insert into t1 values(1,1),(2,2)")
	tk.MustExec("insert into t2 values(1,1),(2,1)")
	tk.MustExec("insert into t3 values(1,1),(2,1)")
	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Session())
	coretestsdk.SetTiFlashReplica(t, dom, "test", "t1")
	coretestsdk.SetTiFlashReplica(t, dom, "test", "t2")
	coretestsdk.SetTiFlashReplica(t, dom, "test", "t3")

	tk.MustExec("set @@tidb_allow_mpp=1; set @@tidb_enforce_mpp=1;")
	for i, ts := range input {
		testdata.OnRecord(func() {
			output[i].SQL = ts
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + ts).Rows())
			output[i].Warning = testdata.ConvertRowsToStrings(tk.MustQuery("show warnings").Rows())
		})
		tk.MustQuery("explain format = 'brief' " + ts).Check(testkit.Rows(output[i].Plan...))
	}
}

func TestMPPSinglePartitionType(t *testing.T) {
	var (
		input  []string
		output []struct {
			SQL  string
			Plan []string
		}
	)
	planSuiteData := GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("drop table if exists employee")
	tk.MustExec("create table employee(empid int, deptid int, salary decimal(10,2))")
	tk.MustExec("set tidb_enforce_mpp=0")

	coretestsdk.SetTiFlashReplica(t, dom, "test", "employee")

	for i, ts := range input {
		testdata.OnRecord(func() {
			output[i].SQL = ts
		})
		if strings.HasPrefix(ts, "set") {
			tk.MustExec(ts)
			continue
		}
		testdata.OnRecord(func() {
			output[i].SQL = ts
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format='brief'" + ts).Rows())
		})
		tk.MustQuery("explain format='brief' " + ts).Check(testkit.Rows(output[i].Plan...))
	}
}

func TestCountStarForTiFlash(t *testing.T) {
	var (
		input  []string
		output []struct {
			SQL     string
			Plan    []string
			Warning []string
		}
	)
	planSuiteData := GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=1")
	tk.MustExec("create table t (a int(11) not null, b varchar(10) not null, c date not null, d char(1) not null, e bigint not null, f datetime not null, g bool not null, h bool )")
	tk.MustExec("create table t_pick_row_id (a char(20) not null)")

	// tiflash
	dom := domain.GetDomain(tk.Session())
	coretestsdk.SetTiFlashReplica(t, dom, "test", "t")
	coretestsdk.SetTiFlashReplica(t, dom, "test", "t_pick_row_id")

	tk.MustExec("set @@tidb_allow_mpp=1; set @@tidb_enforce_mpp=1;")
	for i, ts := range input {
		testdata.OnRecord(func() {
			output[i].SQL = ts
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + ts).Rows())
		})
		tk.MustQuery("explain format = 'brief' " + ts).Check(testkit.Rows(output[i].Plan...))
	}
}

func TestIssues49377Plan(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists employee")
	tk.MustExec("create table employee (employee_id int, name varchar(20), dept_id int)")

	var (
		input  []string
		output []struct {
			SQL     string
			Plan    []string
			Warning []string
		}
	)
	planSuiteData := GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	for i, ts := range input {
		testdata.OnRecord(func() {
			output[i].SQL = ts
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + ts).Rows())
		})
		tk.MustQuery("explain format = 'brief' " + ts).Check(testkit.Rows(output[i].Plan...))
	}
}

func TestHashAggPushdownToTiFlashCompute(t *testing.T) {
	var (
		input  []string
		output []struct {
			SQL     string
			Plan    []string
			Warning []string
		}
	)
	planSuiteData := GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists tbl_15;")
	tk.MustExec(`create table tbl_15 (col_89 text (473) collate utf8mb4_bin ,
					col_90 timestamp default '1976-04-03' ,
					col_91 tinyint unsigned not null ,
					col_92 tinyint ,
					col_93 double not null ,
					col_94 datetime not null default '1970-06-08' ,
					col_95 datetime default '2028-02-13' ,
					col_96 int unsigned not null default 2532480521 ,
					col_97 char (168) default '') partition by hash (col_91) partitions 4;`)

	tk.MustExec("drop table if exists tbl_16;")
	tk.MustExec(`create table tbl_16 (col_98 text (246) not null ,
					col_99 decimal (30 ,19) ,
					col_100 mediumint unsigned ,
					col_101 text (410) collate utf8mb4_bin ,
					col_102 date not null ,
					col_103 timestamp not null default '2003-08-27' ,
					col_104 text (391) not null ,
					col_105 date default '2010-10-24' ,
					col_106 text (9) not null,primary key (col_100, col_98(5), col_103),
					unique key idx_23 (col_100, col_106 (3), col_101 (3))) partition by hash (col_100) partitions 2;`)

	config.UpdateGlobal(func(conf *config.Config) {
		conf.DisaggregatedTiFlash = true
	})
	defer config.UpdateGlobal(func(conf *config.Config) {
		conf.DisaggregatedTiFlash = false
	})

	dom := domain.GetDomain(tk.Session())
	coretestsdk.SetTiFlashReplica(t, dom, "test", "tbl_15")
	coretestsdk.SetTiFlashReplica(t, dom, "test", "tbl_16")

	tk.MustExec("set @@tidb_allow_mpp=1; set @@tidb_enforce_mpp=1;")
	tk.MustExec("set @@tidb_partition_prune_mode = 'static';")
	tk.MustExec("set @@tidb_isolation_read_engines = 'tiflash';")

	for i, ts := range input {
		testdata.OnRecord(func() {
			output[i].SQL = ts
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + ts).Rows())
		})
		tk.MustQuery("explain format = 'brief' " + ts).Check(testkit.Rows(output[i].Plan...))
	}
}

func TestPointgetIndexChoosen(t *testing.T) {
	var (
		input  []string
		output []struct {
			SQL     string
			Plan    []string
			Warning []string
		}
	)
	planSuiteData := GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE t ( a int NOT NULL ,  b int NOT NULL,
			c varchar(64) NOT NULL ,  d varchar(64) NOT NULL  ,
			UNIQUE KEY ub (b),
			UNIQUE KEY ubc (b, c));`)
	for i, ts := range input {
		testdata.OnRecord(func() {
			output[i].SQL = ts
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + ts).Rows())
		})
		tk.MustQuery("explain format = 'brief' " + ts).Check(testkit.Rows(output[i].Plan...))
	}
}

// Test issue #46962 plan
func TestAlwaysTruePredicateWithSubquery(t *testing.T) {
	var (
		input  []string
		output []struct {
			SQL     string
			Plan    []string
			Warning []string
		}
	)
	planSuiteData := GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE t ( a int NOT NULL ,  b int NOT NULL ) `)
	for i, ts := range input {
		testdata.OnRecord(func() {
			output[i].SQL = ts
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(ts).Rows())
		})
		tk.MustQuery(ts).Check(testkit.Rows(output[i].Plan...))
	}
}

// TestExplainExpand
func TestExplainExpand(t *testing.T) {
	var (
		input  []string
		output []struct {
			SQL     string
			Plan    []string
			Warning []string
		}
	)
	planSuiteData := GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists s")
	tk.MustExec("create table t(a int, b int, c int, d int, e int)")
	tk.MustExec("create table s(a int, b int, c int, d int, e int)")
	tk.MustExec("CREATE TABLE `sales` (`year` int(11) DEFAULT NULL, `country` varchar(20) DEFAULT NULL,  `product` varchar(32) DEFAULT NULL,  `profit` int(11) DEFAULT NULL, `whatever` int)")

	// error test
	err := tk.ExecToErr("explain format = 'brief' SELECT country, product, SUM(profit) AS profit FROM sales GROUP BY country, country, product with rollup order by grouping(year);")
	require.Equal(t, err.Error(), "[planner:3602]Argument #0 of GROUPING function is not in GROUP BY")

	for i, ts := range input {
		testdata.OnRecord(func() {
			output[i].SQL = ts
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(ts).Rows())
		})
		tk.MustQuery(ts).Check(testkit.Rows(output[i].Plan...))
	}
}

func TestPhysicalApplyIsNotPhysicalJoin(t *testing.T) {
	// PhysicalApply is expected not to implement PhysicalJoin.
	require.NotImplements(t, (*core.PhysicalJoin)(nil), new(core.PhysicalApply))
}
