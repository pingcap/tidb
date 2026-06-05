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

package fulljoin

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/planner/core/rule"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

type plannerSuite struct {
	p    *parser.Parser
	is   infoschema.InfoSchema
	sctx sessionctx.Context
	ctx  base.PlanContext
}

func createPlannerSuite(t *testing.T) *plannerSuite {
	t.Helper()

	tblInfos := []*model.TableInfo{
		core.MockSignedTable(),
	}
	for id, tblInfo := range tblInfos {
		tblInfo.ID = int64(id + 1)
	}
	is := infoschema.MockInfoSchema(tblInfos)

	ctx := mock.NewContext()
	ctx.Store = &mock.Store{Client: &mock.Client{}}
	initStatsCtx := mock.NewContext()
	initStatsCtx.Store = &mock.Store{Client: &mock.Client{}}
	ctx.GetSessionVars().CurrentDB = "test"
	do := domain.NewMockDomain()
	require.NoError(t, do.CreateStatsHandle(context.Background(), initStatsCtx))
	ctx.BindDomain(do)
	ctx.SetInfoSchema(is)
	domain.GetDomain(ctx).MockInfoCacheAndLoadInfoSchema(is)

	p := parser.New()
	p.SetParserConfig(parser.ParserConfig{EnableStrictDoubleTypeCheck: true})
	return &plannerSuite{
		p:    p,
		is:   is,
		sctx: ctx,
		ctx:  ctx,
	}
}

func (s *plannerSuite) Close() {
	domain.GetDomain(s.ctx).StatsHandle().Close()
}

func parseNode(t *testing.T, s *plannerSuite, sql string) *resolve.NodeW {
	t.Helper()
	stmt, err := s.p.ParseOneStmt(sql, "", "")
	require.NoError(t, err, sql)
	return resolve.NewNodeW(stmt)
}

func buildLogicalPlan(t *testing.T, s *plannerSuite, sql string) base.LogicalPlan {
	t.Helper()
	nodeW := parseNode(t, s, sql)
	p, err := core.BuildLogicalPlanForTest(context.Background(), s.sctx, nodeW, s.is)
	require.NoError(t, err, sql)
	logicalPlan, ok := p.(base.LogicalPlan)
	require.True(t, ok, sql)
	return logicalPlan
}

func optimizeLogicalPlan(t *testing.T, s *plannerSuite, sql string, flag uint64) base.PhysicalPlan {
	t.Helper()
	logicalPlan := buildLogicalPlan(t, s, sql)
	p, _, err := core.DoOptimize(context.Background(), s.ctx, flag, logicalPlan)
	require.NoError(t, err, sql)
	return p
}

func optimizeWithPlanner(t *testing.T, s *plannerSuite, sql string) base.PhysicalPlan {
	t.Helper()
	nodeW := parseNode(t, s, sql)
	p, _, err := planner.Optimize(context.Background(), s.sctx, nodeW, s.is)
	require.NoError(t, err, sql)
	physicalPlan, ok := p.(base.PhysicalPlan)
	require.True(t, ok, sql)
	return physicalPlan
}

func findFirstLogicalJoin(p base.LogicalPlan) (*logicalop.LogicalJoin, bool) {
	if join, ok := p.(*logicalop.LogicalJoin); ok {
		return join, true
	}
	for _, child := range p.Children() {
		if join, ok := findFirstLogicalJoin(child); ok {
			return join, true
		}
	}
	return nil, false
}

func findFirstPhysicalJoin(p base.PhysicalPlan) (core.PhysicalJoin, bool) {
	if join, ok := p.(core.PhysicalJoin); ok {
		return join, true
	}
	for _, child := range p.Children() {
		if join, ok := findFirstPhysicalJoin(child); ok {
			return join, true
		}
	}
	return nil, false
}

func findFirstPhysicalHashJoin(p base.PhysicalPlan) (*core.PhysicalHashJoin, bool) {
	if join, ok := p.(*core.PhysicalHashJoin); ok {
		return join, true
	}
	for _, child := range p.Children() {
		if join, ok := findFirstPhysicalHashJoin(child); ok {
			return join, true
		}
	}
	return nil, false
}

func collectPhysicalJoins(p base.PhysicalPlan) []core.PhysicalJoin {
	joins := make([]core.PhysicalJoin, 0, 4)
	if join, ok := p.(core.PhysicalJoin); ok {
		joins = append(joins, join)
	}
	for _, child := range p.Children() {
		joins = append(joins, collectPhysicalJoins(child)...)
	}
	return joins
}

func TestFullOuterJoinFeatureSwitchDefaultOff(t *testing.T) {
	s := createPlannerSuite(t)
	defer s.Close()
	ctx := context.Background()
	sql := "select * from t t1 full outer join t t2 on t1.a = t2.a"
	nodeW := parseNode(t, s, sql)
	err := core.Preprocess(ctx, s.sctx, nodeW, core.WithPreprocessorReturn(&core.PreprocessorReturn{InfoSchema: s.is}))
	require.NoError(t, err, sql)
	_, err = core.BuildLogicalPlanForTest(ctx, s.sctx, nodeW, s.is)
	require.Error(t, err, sql)
	require.True(t, plannererrors.ErrNotSupportedYet.Equal(err), sql)
	require.ErrorContains(t, err, "FULL OUTER JOIN", sql)
}

func TestFullOuterJoinLogicalBuild(t *testing.T) {
	s := createPlannerSuite(t)
	defer s.Close()
	s.sctx.GetSessionVars().EnableFullOuterJoin = true

	sql := "select * from t t1 full outer join t t2 on t1.a = t2.a and t1.b > 1 and t2.b > 1"
	nodeW := parseNode(t, s, sql)
	err := core.Preprocess(context.Background(), s.sctx, nodeW, core.WithPreprocessorReturn(&core.PreprocessorReturn{InfoSchema: s.is}))
	require.NoError(t, err, sql)
	p, err := core.BuildLogicalPlanForTest(context.Background(), s.sctx, nodeW, s.is)
	require.NoError(t, err, sql)

	join, ok := findFirstLogicalJoin(p.(base.LogicalPlan))
	require.True(t, ok, sql)
	require.Equal(t, logicalop.FullOuterJoin, join.JoinType, sql)

	for _, col := range join.Schema().Columns {
		require.False(t, mysql.HasNotNullFlag(col.RetType.GetFlag()), sql)
	}
	require.NotNil(t, join.FullSchema, sql)
	for _, col := range join.FullSchema.Columns {
		require.False(t, mysql.HasNotNullFlag(col.RetType.GetFlag()), sql)
	}

	physicalPlan, _, err := core.DoOptimize(context.Background(), s.ctx, rule.FlagPredicatePushDown, p.(base.LogicalPlan))
	require.NoError(t, err, sql)
	physicalJoin, ok := findFirstPhysicalJoin(physicalPlan)
	require.True(t, ok, sql)
	require.Equal(t, logicalop.FullOuterJoin, physicalJoin.GetJoinType(), sql)
	require.Contains(t, physicalJoin.ExplainInfo(), "left cond:", sql)
	require.Contains(t, physicalJoin.ExplainInfo(), "right cond:", sql)
	require.NotContains(t, core.ToString(physicalPlan), "Selection", sql)
}

func TestFullOuterJoinUnsupportedFormsFailFast(t *testing.T) {
	s := createPlannerSuite(t)
	defer s.Close()
	s.sctx.GetSessionVars().EnableFullOuterJoin = true

	ctx := context.Background()
	sqls := []string{
		"select * from t t1 full outer join t t2 using (a)",
		"select * from t t1 natural full outer join t t2",
	}
	for _, sql := range sqls {
		nodeW := parseNode(t, s, sql)
		err := core.Preprocess(ctx, s.sctx, nodeW, core.WithPreprocessorReturn(&core.PreprocessorReturn{InfoSchema: s.is}))
		require.NoError(t, err, sql)
		_, err = core.BuildLogicalPlanForTest(ctx, s.sctx, nodeW, s.is)
		require.Error(t, err, sql)
		require.True(t, plannererrors.ErrNotSupportedYet.Equal(err), sql)
		require.ErrorContains(t, err, "FULL OUTER JOIN", sql)
	}
}

func TestFullOuterJoinCascadesFailFast(t *testing.T) {
	s := createPlannerSuite(t)
	defer s.Close()
	s.sctx.GetSessionVars().EnableFullOuterJoin = true
	s.sctx.GetSessionVars().SetEnableCascadesPlanner(true)
	defer s.sctx.GetSessionVars().SetEnableCascadesPlanner(false)

	ctx := context.Background()
	sql := "select * from t t1 full outer join t t2 on t1.a = t2.a"
	nodeW := parseNode(t, s, sql)
	err := core.Preprocess(ctx, s.sctx, nodeW, core.WithPreprocessorReturn(&core.PreprocessorReturn{InfoSchema: s.is}))
	require.NoError(t, err, sql)
	_, err = core.BuildLogicalPlanForTest(ctx, s.sctx, nodeW, s.is)
	require.Error(t, err, sql)
	require.True(t, plannererrors.ErrNotSupportedYet.Equal(err), sql)
	require.ErrorContains(t, err, "FULL OUTER JOIN", sql)
}

func TestFullOuterJoinPhysicalPlanHashJoinOnly(t *testing.T) {
	s := createPlannerSuite(t)
	defer s.Close()
	s.sctx.GetSessionVars().EnableFullOuterJoin = true

	sql := "select * from t t1 full outer join t t2 on t1.a = t2.a"
	p := optimizeWithPlanner(t, s, sql)
	hashJoin, ok := findFirstPhysicalHashJoin(p)
	require.True(t, ok, sql)
	require.Equal(t, logicalop.FullOuterJoin, hashJoin.GetJoinType(), sql)
	require.False(t, hashJoin.UseOuterToBuild, sql)
}

func TestFullOuterJoinSimplifyOuterJoin(t *testing.T) {
	s := createPlannerSuite(t)
	defer s.Close()
	s.sctx.GetSessionVars().EnableFullOuterJoin = true

	tests := []struct {
		sql      string
		joinType logicalop.JoinType
	}{
		{
			sql:      "select * from t t1 full outer join t t2 on t1.a = t2.a where t1.b > 1",
			joinType: logicalop.LeftOuterJoin,
		},
		{
			sql:      "select * from t t1 full outer join t t2 on t1.a = t2.a where t2.b > 1",
			joinType: logicalop.RightOuterJoin,
		},
		{
			sql:      "select * from t t1 full outer join t t2 on t1.a = t2.a where t1.b > 1 and t2.b > 1",
			joinType: logicalop.InnerJoin,
		},
		{
			sql:      "select * from t t1 full outer join t t2 on t1.a = t2.a where t1.b > 1 or t2.b > 1",
			joinType: logicalop.FullOuterJoin,
		},
	}
	for _, tt := range tests {
		p := optimizeLogicalPlan(t, s, tt.sql, rule.FlagPredicatePushDown)
		join, ok := findFirstPhysicalJoin(p)
		require.True(t, ok, tt.sql)
		require.Equal(t, tt.joinType, join.GetJoinType(), tt.sql)
	}
}

func TestFullOuterJoinSkipJoinReOrder(t *testing.T) {
	s := createPlannerSuite(t)
	defer s.Close()
	s.sctx.GetSessionVars().EnableFullOuterJoin = true

	sql := "select * from t t1 full outer join t t2 on t1.a = t2.a full outer join t t3 on t2.a = t3.a"
	p := optimizeLogicalPlan(t, s, sql, rule.FlagPredicatePushDown|rule.FlagJoinReOrder)
	joins := collectPhysicalJoins(p)
	require.NotEmpty(t, joins, sql)
	fullOuterJoinCnt := 0
	for _, join := range joins {
		if join.GetJoinType() == logicalop.FullOuterJoin {
			fullOuterJoinCnt++
		}
	}
	require.Equal(t, 2, fullOuterJoinCnt, sql)
}

func TestFullOuterJoinConvertOuterToInner(t *testing.T) {
	s := createPlannerSuite(t)
	defer s.Close()
	s.sctx.GetSessionVars().EnableFullOuterJoin = true

	tests := []struct {
		sql      string
		joinType logicalop.JoinType
	}{
		{
			sql:      "select * from t t1 full outer join t t2 on t1.a = t2.a where t1.b > 1",
			joinType: logicalop.LeftOuterJoin,
		},
		{
			sql:      "select * from t t1 full outer join t t2 on t1.a = t2.a where t2.b > 1",
			joinType: logicalop.RightOuterJoin,
		},
		{
			sql:      "select * from t t1 full outer join t t2 on t1.a = t2.a where t1.b > 1 and t2.b > 1",
			joinType: logicalop.InnerJoin,
		},
		{
			sql:      "select * from t t1 full outer join t t2 on t1.a = t2.a where t1.b > 1 or t2.b > 1",
			joinType: logicalop.FullOuterJoin,
		},
	}
	for _, tt := range tests {
		p := optimizeLogicalPlan(t, s, tt.sql, rule.FlagConvertOuterToInnerJoin)
		join, ok := findFirstPhysicalJoin(p)
		require.True(t, ok, tt.sql)
		require.Equal(t, tt.joinType, join.GetJoinType(), tt.sql)
	}
}

func skipPostOptimizedProjection(plan [][]any) int {
	for i, r := range plan {
		cost := r[2].(string)
		if cost == "0.00" && strings.Contains(r[0].(string), "Projection") {
			// projection injected in post-optimization, whose cost is always 0 under the old cost implementation
			continue
		}
		return i
	}
	return 0
}

func TestFullOuterJoinTailScanCostVer1(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_cost_model_version=1")
	tk.MustExec("set @@tidb_enable_full_outer_join=1")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int)")
	tk.MustExec("create table t2(a int)")

	parseTopHashJoinCost := func(sql string) float64 {
		rows := tk.MustQuery("explain format=verbose " + sql).Rows()
		require.NotEmpty(t, rows, "sql=%s", sql)
		idx := skipPostOptimizedProjection(rows)
		for idx < len(rows) && !strings.Contains(rows[idx][0].(string), "HashJoin") {
			idx++
		}
		require.Less(t, idx, len(rows), "sql=%s, explain=%v", sql, rows)
		planCost, err := strconv.ParseFloat(rows[idx][2].(string), 64)
		require.NoError(t, err)
		return planCost
	}

	innerJoinSQL := "select /*+ HASH_JOIN(t1, t2) */ * from t1 join t2 on t1.a = t2.a"
	fullOuterJoinSQL := "select /*+ HASH_JOIN(t1, t2) */ * from t1 full outer join t2 on t1.a = t2.a"

	innerCost := parseTopHashJoinCost(innerJoinSQL)
	fullOuterCost := parseTopHashJoinCost(fullOuterJoinSQL)
	require.Greater(t, fullOuterCost, innerCost)
}

func TestFullOuterJoinTailScanCostVer2(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_cost_model_version=2")
	tk.MustExec("set @@tidb_enable_full_outer_join=1")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int)")
	tk.MustExec("create table t2(a int)")

	parseTopHashJoinCost := func(sql string) float64 {
		rows := tk.MustQuery("explain format=verbose " + sql).Rows()
		require.NotEmpty(t, rows, "sql=%s", sql)
		idx := -1
		for i, row := range rows {
			if strings.Contains(row[0].(string), "HashJoin") {
				idx = i
				break
			}
		}
		require.GreaterOrEqual(t, idx, 0, "sql=%s, explain=%v", sql, rows)
		planCost, err := strconv.ParseFloat(rows[idx][2].(string), 64)
		require.NoError(t, err)
		return planCost
	}

	innerJoinSQL := "select /*+ HASH_JOIN(t1, t2) */ * from t1 join t2 on t1.a = t2.a"
	fullOuterJoinSQL := "select /*+ HASH_JOIN(t1, t2) */ * from t1 full outer join t2 on t1.a = t2.a"

	innerCost := parseTopHashJoinCost(innerJoinSQL)
	fullOuterCost := parseTopHashJoinCost(fullOuterJoinSQL)
	require.Greater(t, fullOuterCost, innerCost)
}
