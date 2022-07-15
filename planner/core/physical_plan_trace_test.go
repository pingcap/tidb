// Copyright 2021 PingCAP, Inc.
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
	"sort"
	"strings"
	"testing"

	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util/hint"
	"github.com/pingcap/tidb/util/tracing"
	"github.com/stretchr/testify/require"
)

func TestPhysicalOptimizeWithTraceEnabled(t *testing.T) {
	p := parser.New()
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	ctx := tk.Session().(sessionctx.Context)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, primary key (a))")
	testcases := []struct {
		sql          string
		physicalList []string
	}{
		{
			sql: "select * from t",
			physicalList: []string{
				"Projection_3", "TableFullScan_4", "TableReader_5",
			},
		},
		{
			sql: "select * from t where a = 1",
			physicalList: []string{
				"Point_Get_5", "Projection_4",
			},
		},
	}

	for _, testcase := range testcases {
		sql := testcase.sql
		stmt, err := p.ParseOneStmt(sql, "", "")
		require.NoError(t, err)
		err = core.Preprocess(ctx, stmt, core.WithPreprocessorReturn(&core.PreprocessorReturn{InfoSchema: dom.InfoSchema()}))
		require.NoError(t, err)
		sctx := core.MockContext()
		sctx.GetSessionVars().StmtCtx.EnableOptimizeTrace = true
		builder, _ := core.NewPlanBuilder().Init(sctx, dom.InfoSchema(), &hint.BlockHintProcessor{})
		domain.GetDomain(sctx).MockInfoCacheAndLoadInfoSchema(dom.InfoSchema())
		plan, err := builder.Build(context.TODO(), stmt)
		require.NoError(t, err)
		_, _, err = core.DoOptimize(context.TODO(), sctx, builder.GetOptFlag(), plan.(core.LogicalPlan))
		require.NoError(t, err)
		otrace := sctx.GetSessionVars().StmtCtx.OptimizeTracer.Physical
		require.NotNil(t, otrace)
		physicalList := getList(otrace)
		require.True(t, checkList(physicalList, testcase.physicalList))
	}
}

func checkList(d []string, s []string) bool {
	if len(d) != len(s) {
		return false
	}
	for i := 0; i < len(d); i++ {
		if strings.Compare(d[i], s[i]) != 0 {
			return false
		}
	}
	return true
}

func getList(otrace *tracing.PhysicalOptimizeTracer) (pl []string) {
	for _, v := range otrace.Candidates {
		pl = append(pl, tracing.CodecPlanName(v.TP, v.ID))
	}
	sort.Strings(pl)
	return pl
}

// assert the case in https://github.com/pingcap/tidb/issues/34863
func TestPhysicalOptimizerTrace(t *testing.T) {
	p := parser.New()
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	ctx := tk.Session().(sessionctx.Context)
	tk.MustExec("use test")
	tk.MustExec("create table customer2(c_id bigint);")
	tk.MustExec("create table orders2(o_id bigint, c_id bigint);")
	tk.MustExec("insert into customer2 values(1),(2),(3),(4),(5);")
	tk.MustExec("insert into orders2 values(1,1),(2,1),(3,2),(4,2),(5,2);")
	tk.MustExec("set @@tidb_opt_agg_push_down=1;")

	sql := "select count(*) from customer2 c left join orders2 o on c.c_id=o.c_id;"

	stmt, err := p.ParseOneStmt(sql, "", "")
	require.NoError(t, err)
	err = core.Preprocess(ctx, stmt, core.WithPreprocessorReturn(&core.PreprocessorReturn{InfoSchema: dom.InfoSchema()}))
	require.NoError(t, err)
	sctx := core.MockContext()
	sctx.GetSessionVars().StmtCtx.EnableOptimizeTrace = true
	sctx.GetSessionVars().AllowAggPushDown = true
	builder, _ := core.NewPlanBuilder().Init(sctx, dom.InfoSchema(), &hint.BlockHintProcessor{})
	domain.GetDomain(sctx).MockInfoCacheAndLoadInfoSchema(dom.InfoSchema())
	plan, err := builder.Build(context.TODO(), stmt)
	require.NoError(t, err)
	flag := uint64(0)
	flag = flag | 1<<1 | 1<<3 | 1<<8 | 1<<12 | 1<<15
	_, _, err = core.DoOptimize(context.TODO(), sctx, flag, plan.(core.LogicalPlan))
	require.NoError(t, err)
	otrace := sctx.GetSessionVars().StmtCtx.OptimizeTracer.Physical
	require.NotNil(t, otrace)
	elements := map[int]string{
		12: "TableReader",
		14: "TableReader",
		13: "TableFullScan",
		15: "HashJoin",
		6:  "Projection",
		11: "TableFullScan",
		9:  "HashJoin",
		10: "HashJoin",
		7:  "HashAgg",
		16: "HashJoin",
		8:  "StreamAgg",
	}
	final := map[int]struct{}{
		11: {},
		12: {},
		13: {},
		14: {},
		9:  {},
		7:  {},
		6:  {},
	}
	for _, c := range otrace.Candidates {
		tp, ok := elements[c.ID]
		if !ok || tp != c.TP {
			t.FailNow()
		}
	}
	require.Len(t, otrace.Candidates, len(elements))
	for _, p := range otrace.Final {
		_, ok := final[p.ID]
		if !ok {
			t.FailNow()
		}
	}
	require.Len(t, otrace.Final, len(final))
}
