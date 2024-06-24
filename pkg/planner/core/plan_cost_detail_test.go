// Copyright 2022 PingCAP, Inc.
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

package core_test

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/parser"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/tracing"
	"github.com/stretchr/testify/require"
)

func TestPlanCostDetail(t *testing.T) {
	p := parser.New()
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t (a int primary key, b int, c int, d int, k int, key b(b), key cd(c, d), unique key(k))`)
	testcases := []struct {
		sql        string
		assertLbls []string
		tp         string
	}{
		{
			tp:  plancodec.TypeHashJoin,
			sql: "select /*+ HASH_JOIN(t1, t2) */  * from t t1 join t t2 on t1.k = t2.k where t1.a = 1;",
			assertLbls: []string{
				plannercore.CPUCostDetailLbl,
				plannercore.CPUCostDescLbl,
				plannercore.ProbeCostDescLbl,
				plannercore.MemCostDetailLbl,
				plannercore.MemCostDescLbl,
				plannercore.DiskCostDetailLbl,
			},
		},
		{
			tp:  plancodec.TypePointGet,
			sql: "select * from t where a = 1",
			assertLbls: []string{
				plannercore.RowSizeLbl,
				plannercore.NetworkFactorLbl,
				plannercore.SeekFactorLbl,
			},
		},
		{
			tp:  plancodec.TypeBatchPointGet,
			sql: "select * from t where a = 1 or a = 2 or a = 3",
			assertLbls: []string{
				plannercore.RowCountLbl,
				plannercore.RowSizeLbl,
				plannercore.NetworkFactorLbl,
				plannercore.SeekFactorLbl,
				plannercore.ScanConcurrencyLbl,
			},
		},
		{
			tp:  plancodec.TypeTableFullScan,
			sql: "select * from t",
			assertLbls: []string{
				plannercore.RowCountLbl,
				plannercore.RowSizeLbl,
				plannercore.ScanFactorLbl,
			},
		},
		{
			tp:  plancodec.TypeTableReader,
			sql: "select * from t",
			assertLbls: []string{
				plannercore.RowCountLbl,
				plannercore.RowSizeLbl,
				plannercore.NetworkFactorLbl,
				plannercore.NetSeekCostLbl,
				plannercore.TablePlanCostLbl,
				plannercore.ScanConcurrencyLbl,
			},
		},
		{
			tp:  plancodec.TypeIndexFullScan,
			sql: "select b from t",
			assertLbls: []string{
				plannercore.RowCountLbl,
				plannercore.RowSizeLbl,
				plannercore.ScanFactorLbl,
			},
		},
		{
			tp:  plancodec.TypeIndexReader,
			sql: "select b from t",
			assertLbls: []string{
				plannercore.RowCountLbl,
				plannercore.RowSizeLbl,
				plannercore.NetworkFactorLbl,
				plannercore.NetSeekCostLbl,
				plannercore.IndexPlanCostLbl,
				plannercore.ScanConcurrencyLbl,
			},
		},
	}
	for _, tc := range testcases {
		costDetails := optimize(t, tc.sql, p, tk.Session(), dom)
		asserted := false
		for _, cd := range costDetails {
			if cd.GetPlanType() == tc.tp {
				asserted = true
				for _, lbl := range tc.assertLbls {
					require.True(t, cd.Exists(lbl))
				}
			}
		}
		require.True(t, asserted)
	}
}

func optimize(t *testing.T, sql string, p *parser.Parser, ctx sessionctx.Context, dom *domain.Domain) map[string]*tracing.PhysicalPlanCostDetail {
	stmt, err := p.ParseOneStmt(sql, "", "")
	require.NoError(t, err)
	err = plannercore.Preprocess(context.Background(), ctx, stmt, plannercore.WithPreprocessorReturn(&plannercore.PreprocessorReturn{InfoSchema: dom.InfoSchema()}))
	require.NoError(t, err)
	sctx := plannercore.MockContext()
	defer func() {
		domain.GetDomain(sctx).StatsHandle().Close()
	}()
	sctx.GetSessionVars().StmtCtx.EnableOptimizeTrace = true
	sctx.GetSessionVars().EnableNewCostInterface = true
	sctx.GetSessionVars().CostModelVersion = 1
	builder, _ := plannercore.NewPlanBuilder().Init(sctx, dom.InfoSchema(), hint.NewQBHintHandler(nil))
	domain.GetDomain(sctx).MockInfoCacheAndLoadInfoSchema(dom.InfoSchema())
	plan, err := builder.Build(context.TODO(), stmt)
	require.NoError(t, err)
	_, _, err = plannercore.DoOptimize(context.TODO(), sctx, builder.GetOptFlag(), plan.(base.LogicalPlan))
	require.NoError(t, err)
	return sctx.GetSessionVars().StmtCtx.OptimizeTracer.Physical.PhysicalPlanCostDetails
}
