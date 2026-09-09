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
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/planner/core/rule"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util/costusage"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
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

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int not null, b int not null, key(a))")
	ctx := tk.Session()
	is := domain.GetDomain(ctx).InfoSchema()

	p := parser.New()
	p.SetParserConfig(parser.ParserConfig{EnableStrictDoubleTypeCheck: true})
	return &plannerSuite{
		p:    p,
		is:   is,
		sctx: ctx,
		ctx:  ctx.GetPlanCtx(),
	}
}

func (*plannerSuite) Close() {}

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

func findFirstPhysicalJoin(p base.PhysicalPlan) (base.PhysicalJoin, bool) {
	if join, ok := p.(base.PhysicalJoin); ok {
		return join, true
	}
	for _, child := range p.Children() {
		if join, ok := findFirstPhysicalJoin(child); ok {
			return join, true
		}
	}
	return nil, false
}

func findFirstPhysicalHashJoin(p base.PhysicalPlan) (*physicalop.PhysicalHashJoin, bool) {
	if join, ok := p.(*physicalop.PhysicalHashJoin); ok {
		return join, true
	}
	for _, child := range p.Children() {
		if join, ok := findFirstPhysicalHashJoin(child); ok {
			return join, true
		}
	}
	return nil, false
}

func collectPhysicalJoins(p base.PhysicalPlan) []base.PhysicalJoin {
	joins := make([]base.PhysicalJoin, 0, 4)
	if join, ok := p.(base.PhysicalJoin); ok {
		joins = append(joins, join)
	}
	for _, child := range p.Children() {
		joins = append(joins, collectPhysicalJoins(child)...)
	}
	return joins
}

func warningText(s *plannerSuite) string {
	warnings := s.sctx.GetSessionVars().StmtCtx.GetWarnings()
	warnText := make([]string, 0, len(warnings))
	for _, warn := range warnings {
		warnText = append(warnText, warn.Err.Error())
	}
	return strings.Join(warnText, "\n")
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
	require.Equal(t, base.FullOuterJoin, join.JoinType, sql)

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
	require.Equal(t, base.FullOuterJoin, physicalJoin.GetJoinType(), sql)
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
		"select * from t t1 full outer join lateral (select 1 as a) as t2 on false",
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
	require.Equal(t, base.FullOuterJoin, hashJoin.GetJoinType(), sql)
	require.False(t, hashJoin.UseOuterToBuild, sql)
}

func TestFullOuterJoinUnsupportedJoinMethodHintsWarn(t *testing.T) {
	s := createPlannerSuite(t)
	defer s.Close()
	s.sctx.GetSessionVars().EnableFullOuterJoin = true

	tests := []struct {
		sql          string
		warnContains []string
	}{
		{
			sql: "select /*+ MERGE_JOIN(t1, t2) */ * from t t1 full outer join t t2 on t1.a = t2.a",
			warnContains: []string{
				"MERGE_JOIN",
				"inapplicable",
			},
		},
		{
			sql: "select /*+ INL_JOIN(t2) */ * from t t1 full outer join t t2 on t1.a = t2.a",
			warnContains: []string{
				"INL_JOIN",
				"inapplicable",
			},
		},
	}
	for _, tt := range tests {
		s.sctx.GetSessionVars().StmtCtx.SetWarnings(nil)
		p := optimizeWithPlanner(t, s, tt.sql)
		hashJoin, ok := findFirstPhysicalHashJoin(p)
		require.True(t, ok, tt.sql)
		require.Equal(t, base.FullOuterJoin, hashJoin.GetJoinType(), tt.sql)

		warnings := warningText(s)
		for _, expected := range tt.warnContains {
			require.Contains(t, warnings, expected, tt.sql)
		}
	}
}

func TestFullOuterJoinSimplifyOuterJoin(t *testing.T) {
	s := createPlannerSuite(t)
	defer s.Close()
	s.sctx.GetSessionVars().EnableFullOuterJoin = true

	tests := []struct {
		sql      string
		joinType base.JoinType
	}{
		{
			sql:      "select * from t t1 full outer join t t2 on t1.a = t2.a where t1.b > 1",
			joinType: base.LeftOuterJoin,
		},
		{
			sql:      "select * from t t1 full outer join t t2 on t1.a = t2.a where t2.b > 1",
			joinType: base.RightOuterJoin,
		},
		{
			sql:      "select * from t t1 full outer join t t2 on t1.a = t2.a where t1.b > 1 and t2.b > 1",
			joinType: base.InnerJoin,
		},
		{
			sql:      "select * from t t1 full outer join t t2 on t1.a = t2.a where t1.b > 1 or t2.b > 1",
			joinType: base.FullOuterJoin,
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
		if join.GetJoinType() == base.FullOuterJoin {
			fullOuterJoinCnt++
		}
	}
	require.Equal(t, 2, fullOuterJoinCnt, sql)
}

func TestFullOuterJoinTailScanCostVer1(t *testing.T) {
	s := createPlannerSuite(t)
	defer s.Close()
	s.sctx.GetSessionVars().EnableFullOuterJoin = true
	s.sctx.GetSessionVars().CostModelVersion = 1

	innerJoinSQL := "select /*+ HASH_JOIN(t1, t2) */ * from t t1 join t t2 on t1.a = t2.a"
	fullOuterJoinSQL := "select /*+ HASH_JOIN(t1, t2) */ * from t t1 full outer join t t2 on t1.a = t2.a"

	innerHashJoin, ok := findFirstPhysicalHashJoin(optimizeWithPlanner(t, s, innerJoinSQL))
	require.True(t, ok, innerJoinSQL)
	fullOuterHashJoin, ok := findFirstPhysicalHashJoin(optimizeWithPlanner(t, s, fullOuterJoinSQL))
	require.True(t, ok, fullOuterJoinSQL)

	option := costusage.NewDefaultPlanCostOption().WithCostFlag(costusage.CostFlagRecalculate)
	innerCost, err := innerHashJoin.GetPlanCostVer1(property.RootTaskType, option)
	require.NoError(t, err)
	fullOuterCost, err := fullOuterHashJoin.GetPlanCostVer1(property.RootTaskType, option)
	require.NoError(t, err)
	require.Greater(t, fullOuterCost, innerCost)
}

func TestFullOuterJoinTailScanCostVer2(t *testing.T) {
	s := createPlannerSuite(t)
	defer s.Close()
	s.sctx.GetSessionVars().EnableFullOuterJoin = true
	s.sctx.GetSessionVars().CostModelVersion = 2

	innerJoinSQL := "select /*+ HASH_JOIN(t1, t2) */ * from t t1 join t t2 on t1.a = t2.a"
	fullOuterJoinSQL := "select /*+ HASH_JOIN(t1, t2) */ * from t t1 full outer join t t2 on t1.a = t2.a"

	innerHashJoin, ok := findFirstPhysicalHashJoin(optimizeWithPlanner(t, s, innerJoinSQL))
	require.True(t, ok, innerJoinSQL)
	fullOuterHashJoin, ok := findFirstPhysicalHashJoin(optimizeWithPlanner(t, s, fullOuterJoinSQL))
	require.True(t, ok, fullOuterJoinSQL)

	option := costusage.NewDefaultPlanCostOption().WithCostFlag(costusage.CostFlagRecalculate)
	innerCost, err := innerHashJoin.GetPlanCostVer2(property.RootTaskType, option)
	require.NoError(t, err)
	fullOuterCost, err := fullOuterHashJoin.GetPlanCostVer2(property.RootTaskType, option)
	require.NoError(t, err)
	require.Greater(t, fullOuterCost.GetCost(), innerCost.GetCost())
}
