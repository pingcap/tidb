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

	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util/hint"
	"github.com/stretchr/testify/require"
)

func TestPlanReplayerCaptureRecordJsonStats(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	ctx := tk.Session().(sessionctx.Context)
	tk.MustExec("use test")
	tk.MustExec("create table t1(a int)")
	tk.MustExec("create table t2(a int)")
	tk.MustExec("analyze table t1")
	tk.MustExec("analyze table t2")
	testcases := []struct {
		sql   string
		count int
	}{
		{
			sql:   "select * from t1",
			count: 1,
		},
		{
			sql:   "select * from t2",
			count: 1,
		},
		{
			sql:   "select * from t1,t2",
			count: 2,
		},
	}
	for _, tc := range testcases {
		tableStats := getTableStats(tc.sql, t, ctx, dom)
		require.Equal(t, tc.count, len(tableStats))
	}
}

func getTableStats(sql string, t *testing.T, ctx sessionctx.Context, dom *domain.Domain) map[int64]*statistics.Table {
	p := parser.New()
	stmt, err := p.ParseOneStmt(sql, "", "")
	require.NoError(t, err)
	err = core.Preprocess(context.Background(), ctx, stmt, core.WithPreprocessorReturn(&core.PreprocessorReturn{InfoSchema: dom.InfoSchema()}))
	require.NoError(t, err)
	sctx := core.MockContext()
	sctx.GetSessionVars().EnablePlanReplayerCapture = true
	builder, _ := core.NewPlanBuilder().Init(sctx, dom.InfoSchema(), &hint.BlockHintProcessor{})
	domain.GetDomain(sctx).MockInfoCacheAndLoadInfoSchema(dom.InfoSchema())
	plan, err := builder.Build(context.TODO(), stmt)
	require.NoError(t, err)
	_, _, err = core.DoOptimize(context.TODO(), sctx, builder.GetOptFlag(), plan.(core.LogicalPlan))
	require.NoError(t, err)
	tableStats := sctx.GetSessionVars().StmtCtx.TableStats
	r := make(map[int64]*statistics.Table)
	for key, v := range tableStats {
		r[key] = v.(*statistics.Table)
	}
	return r
}
