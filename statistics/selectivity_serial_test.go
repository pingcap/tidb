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

package statistics_test

import (
	"context"
	"os"
	"runtime/pprof"
	"testing"

	"github.com/pingcap/tidb/domain"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/testdata"
	"github.com/pingcap/tidb/util/collate"
	"github.com/stretchr/testify/require"
)

func TestCollationColumnEstimate(t *testing.T) {
	domain.RunAutoAnalyze = false
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a varchar(20) collate utf8mb4_general_ci)")
	tk.MustExec("insert into t values('aaa'), ('bbb'), ('AAA'), ('BBB')")
	tk.MustExec("set @@session.tidb_analyze_version=2")
	h := dom.StatsHandle()
	require.Nil(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	tk.MustExec("analyze table t")
	tk.MustExec("explain select * from t where a = 'aaa'")
	require.Nil(t, h.LoadNeededHistograms())
	var (
		input  []string
		output [][]string
	)
	statsSuiteData := statistics.GetStatsSuiteData()
	statsSuiteData.GetTestCases(t, &input, &output)
	for i := 0; i < len(input); i++ {
		testdata.OnRecord(func() {
			output[i] = testdata.ConvertRowsToStrings(tk.MustQuery(input[i]).Rows())
		})
		tk.MustQuery(input[i]).Check(testkit.Rows(output[i]...))
	}
}

func BenchmarkSelectivity(b *testing.B) {
	domain.RunAutoAnalyze = false
	store, dom, clean := testkit.CreateMockStoreAndDomain(b)
	defer clean()
	testKit := testkit.NewTestKit(b, store)
	statsTbl, err := prepareSelectivity(testKit, dom)
	require.NoError(b, err)
	exprs := "a > 1 and b < 2 and c > 3 and d < 4 and e > 5"
	sql := "select * from t where " + exprs
	sctx := testKit.Session().(sessionctx.Context)
	stmts, err := session.Parse(sctx, sql)
	require.NoErrorf(b, err, "error %v, for expr %s", err, exprs)
	require.Len(b, stmts, 1)
	ret := &plannercore.PreprocessorReturn{}
	err = plannercore.Preprocess(sctx, stmts[0], plannercore.WithPreprocessorReturn(ret))
	require.NoErrorf(b, err, "for %s", exprs)
	p, _, err := plannercore.BuildLogicalPlanForTest(context.Background(), sctx, stmts[0], ret.InfoSchema)
	require.NoErrorf(b, err, "error %v, for building plan, expr %s", err, exprs)

	file, err := os.Create("cpu.profile")
	require.NoError(b, err)
	defer func() {
		err := file.Close()
		require.NoError(b, err)
	}()
	err = pprof.StartCPUProfile(file)
	require.NoError(b, err)

	b.Run("Selectivity", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _, err := statsTbl.Selectivity(sctx, p.(plannercore.LogicalPlan).Children()[0].(*plannercore.LogicalSelection).Conditions, nil)
			require.NoError(b, err)
		}
		b.ReportAllocs()
	})
	pprof.StopCPUProfile()
}
