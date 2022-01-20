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
	tk.MustExec("create table t(a int)")

	sql := "select * from t where a in (1,2)"

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
	flag := uint64(0)
	_, _, err = core.DoOptimize(context.TODO(), sctx, flag, plan.(core.LogicalPlan))
	require.NoError(t, err)
	otrace := sctx.GetSessionVars().StmtCtx.PhysicalOptimizeTrace
	require.NotNil(t, otrace)
	logicalList, physicalList := getList(otrace)
	require.True(t, checkList(logicalList, []string{"Projection_3", "Selection_2"}))
	require.True(t, checkList(physicalList, []string{"Projection_4", "Selection_5"}))
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

func getList(otrace *tracing.PhysicalOptimizeTracer) (ll []string, pl []string) {
	for logicalPlan, v := range otrace.State {
		ll = append(ll, logicalPlan)
		for _, info := range v {
			for _, task := range info.Candidates {
				pl = append(pl, tracing.CodecPlanName(task.TP, task.ID))
			}
		}
	}
	sort.Strings(ll)
	sort.Strings(pl)
	return ll, pl
}
