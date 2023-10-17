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

package cbotest

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/planner"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/stretchr/testify/require"
)

func loadTableStats(fileName string, dom *domain.Domain) error {
	statsPath := filepath.Join("testdata", fileName)
	bytes, err := os.ReadFile(statsPath)
	if err != nil {
		return err
	}
	statsTbl := &util.JSONTable{}
	err = json.Unmarshal(bytes, statsTbl)
	if err != nil {
		return err
	}
	statsHandle := dom.StatsHandle()
	err = statsHandle.LoadStatsFromJSON(context.Background(), dom.InfoSchema(), statsTbl, 0)
	if err != nil {
		return err
	}
	return nil
}

// TestCBOWithoutAnalyze tests the plan with stats that only have count info.
func TestCBOWithoutAnalyze(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t1 (a int)")
	testKit.MustExec("create table t2 (a int)")
	h := dom.StatsHandle()
	require.NoError(t, h.HandleDDLEvent(<-h.DDLEventCh()))
	require.NoError(t, h.HandleDDLEvent(<-h.DDLEventCh()))
	testKit.MustExec("insert into t1 values (1), (2), (3), (4), (5), (6)")
	testKit.MustExec("insert into t2 values (1), (2), (3), (4), (5), (6)")
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(dom.InfoSchema()))
	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	analyzeSuiteData := GetAnalyzeSuiteData()
	analyzeSuiteData.LoadTestCases(t, &input, &output)
	for i, sql := range input {
		plan := testKit.MustQuery(sql)
		testdata.OnRecord(func() {
			output[i].SQL = sql
			output[i].Plan = testdata.ConvertRowsToStrings(plan.Rows())
		})
		plan.Check(testkit.Rows(output[i].Plan...))
	}
}

func TestStraightJoin(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	h := dom.StatsHandle()
	for _, tblName := range []string{"t1", "t2", "t3", "t4"} {
		testKit.MustExec(fmt.Sprintf("create table %s (a int)", tblName))
		require.NoError(t, h.HandleDDLEvent(<-h.DDLEventCh()))
	}
	var input []string
	var output [][]string
	analyzeSuiteData := GetAnalyzeSuiteData()
	analyzeSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i] = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
		})
		testKit.MustQuery(tt).Check(testkit.Rows(output[i]...))
	}
}

func TestTableDual(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec(`use test`)
	h := dom.StatsHandle()
	testKit.MustExec(`create table t(a int)`)
	testKit.MustExec("insert into t values (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)")
	require.NoError(t, h.HandleDDLEvent(<-h.DDLEventCh()))

	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(dom.InfoSchema()))
	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	analyzeSuiteData := GetAnalyzeSuiteData()
	analyzeSuiteData.LoadTestCases(t, &input, &output)
	for i, sql := range input {
		plan := testKit.MustQuery(sql)
		testdata.OnRecord(func() {
			output[i].SQL = sql
			output[i].Plan = testdata.ConvertRowsToStrings(plan.Rows())
		})
		plan.Check(testkit.Rows(output[i].Plan...))
	}
}

func TestEstimation(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	statistics.RatioOfPseudoEstimate.Store(10.0)
	defer statistics.RatioOfPseudoEstimate.Store(0.7)
	testKit.MustExec("use test")
	testKit.MustExec("set tidb_cost_model_version=2")
	testKit.MustExec("create table t (a int)")
	testKit.MustExec("insert into t values (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)")
	testKit.MustExec("insert into t select * from t")
	testKit.MustExec("insert into t select * from t")
	h := dom.StatsHandle()
	require.NoError(t, h.HandleDDLEvent(<-h.DDLEventCh()))
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	testKit.MustExec("analyze table t")
	for i := 1; i <= 8; i++ {
		testKit.MustExec("delete from t where a = ?", i)
	}
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(dom.InfoSchema()))
	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	analyzeSuiteData := GetAnalyzeSuiteData()
	analyzeSuiteData.LoadTestCases(t, &input, &output)
	for i, sql := range input {
		plan := testKit.MustQuery(sql)
		testdata.OnRecord(func() {
			output[i].SQL = sql
			output[i].Plan = testdata.ConvertRowsToStrings(plan.Rows())
		})
		plan.Check(testkit.Rows(output[i].Plan...))
	}
}

func TestIndexRead(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("set tidb_cost_model_version=2")
	testKit.MustExec("set @@session.tidb_executor_concurrency = 4;")
	testKit.MustExec("set @@session.tidb_hash_join_concurrency = 5;")
	testKit.MustExec("set @@session.tidb_distsql_scan_concurrency = 15;")

	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t, t1")
	testKit.MustExec("create table t (a int primary key, b int, c varchar(200), d datetime DEFAULT CURRENT_TIMESTAMP, e int, ts timestamp DEFAULT CURRENT_TIMESTAMP)")
	testKit.MustExec("create index b on t (b)")
	testKit.MustExec("create index d on t (d)")
	testKit.MustExec("create index e on t (e)")
	testKit.MustExec("create index b_c on t (b,c)")
	testKit.MustExec("create index ts on t (ts)")
	testKit.MustExec("create table t1 (a int, b int, index idx(a), index idxx(b))")

	// Default RPC encoding may cause statistics explain result differ and then the test unstable.
	testKit.MustExec("set @@tidb_enable_chunk_rpc = on")

	// This stats is generated by following format:
	// fill (a, b, c, e) as (i*100+j, i, i+j, i*100+j), i and j is dependent and range of this two are [0, 99].
	require.NoError(t, loadTableStats("analyzesSuiteTestIndexReadT.json", dom))
	for i := 1; i < 16; i++ {
		testKit.MustExec(fmt.Sprintf("insert into t1 values(%v, %v)", i, i))
	}
	testKit.MustExec("analyze table t1")
	ctx := testKit.Session()
	var input, output []string
	analyzeSuiteData := GetAnalyzeSuiteData()
	analyzeSuiteData.LoadTestCases(t, &input, &output)

	for i, tt := range input {
		stmts, err := session.Parse(ctx, tt)
		require.NoError(t, err)
		require.Len(t, stmts, 1)
		stmt := stmts[0]
		ret := &core.PreprocessorReturn{}
		err = core.Preprocess(context.Background(), ctx, stmt, core.WithPreprocessorReturn(ret))
		require.NoError(t, err)
		p, _, err := planner.Optimize(context.TODO(), ctx, stmt, ret.InfoSchema)
		require.NoError(t, err)
		planString := core.ToString(p)
		testdata.OnRecord(func() {
			output[i] = planString
		})
		require.Equalf(t, output[i], planString, "case: %v", tt)
	}
}

func TestEmptyTable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("set tidb_cost_model_version=2")
	testKit.MustExec("drop table if exists t, t1")
	testKit.MustExec("create table t (c1 int)")
	testKit.MustExec("create table t1 (c1 int)")
	testKit.MustExec("analyze table t, t1")
	var input, output []string
	analyzeSuiteData := GetAnalyzeSuiteData()
	analyzeSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		ctx := testKit.Session()
		stmts, err := session.Parse(ctx, tt)
		require.NoError(t, err)
		require.Len(t, stmts, 1)
		stmt := stmts[0]
		ret := &core.PreprocessorReturn{}
		err = core.Preprocess(context.Background(), ctx, stmt, core.WithPreprocessorReturn(ret))
		require.NoError(t, err)
		p, _, err := planner.Optimize(context.TODO(), ctx, stmt, ret.InfoSchema)
		require.NoError(t, err)
		planString := core.ToString(p)
		testdata.OnRecord(func() {
			output[i] = planString
		})
		require.Equalf(t, output[i], planString, "case: %v", tt)
	}
}

func TestAnalyze(t *testing.T) {
	store := testkit.CreateMockStore(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t, t1, t2, t3")
	testKit.MustExec("create table t (a int, b int)")
	testKit.MustExec("create index a on t (a)")
	testKit.MustExec("create index b on t (b)")
	testKit.MustExec("insert into t (a,b) values (1,1),(1,2),(1,3),(1,4),(2,5),(2,6),(2,7),(2,8)")
	testKit.MustExec("analyze table t")

	testKit.MustExec("create table t1 (a int, b int)")
	testKit.MustExec("create index a on t1 (a)")
	testKit.MustExec("create index b on t1 (b)")
	testKit.MustExec("insert into t1 (a,b) values (1,1),(1,2),(1,3),(1,4),(2,5),(2,6),(2,7),(2,8)")

	testKit.MustExec("create table t2 (a int, b int)")
	testKit.MustExec("create index a on t2 (a)")
	testKit.MustExec("create index b on t2 (b)")
	testKit.MustExec("insert into t2 (a,b) values (1,1),(1,2),(1,3),(1,4),(2,5),(2,6),(2,7),(2,8)")
	testKit.MustExec("analyze table t2 index a")

	testKit.MustExec("create table t3 (a int, b int)")
	testKit.MustExec("create index a on t3 (a)")

	testKit.MustExec("set @@tidb_partition_prune_mode = 'static';")
	testKit.MustExec("create table t4 (a int, b int) partition by range (a) (partition p1 values less than (2), partition p2 values less than (3))")
	testKit.MustExec("create index a on t4 (a)")
	testKit.MustExec("create index b on t4 (b)")
	testKit.MustExec("insert into t4 (a,b) values (1,1),(1,2),(1,3),(1,4),(2,5),(2,6),(2,7),(2,8)")
	testKit.MustExec("analyze table t4")

	testKit.MustExec("create view v as select * from t")
	_, err := testKit.Exec("analyze table v")
	require.EqualError(t, err, "analyze view v is not supported now")
	testKit.MustExec("drop view v")

	testKit.MustExec("create sequence seq")
	_, err = testKit.Exec("analyze table seq")
	require.EqualError(t, err, "analyze sequence seq is not supported now")
	testKit.MustExec("drop sequence seq")

	var input, output []string
	analyzeSuiteData := GetAnalyzeSuiteData()
	analyzeSuiteData.LoadTestCases(t, &input, &output)

	for i, tt := range input {
		ctx := testKit.Session()
		stmts, err := session.Parse(ctx, tt)
		require.NoError(t, err)
		require.Len(t, stmts, 1)
		stmt := stmts[0]
		err = executor.ResetContextOfStmt(ctx, stmt)
		require.NoError(t, err)
		ret := &core.PreprocessorReturn{}
		err = core.Preprocess(context.Background(), ctx, stmt, core.WithPreprocessorReturn(ret))
		require.NoError(t, err)
		p, _, err := planner.Optimize(context.TODO(), ctx, stmt, ret.InfoSchema)
		require.NoError(t, err)
		planString := core.ToString(p)
		testdata.OnRecord(func() {
			output[i] = planString
		})
		require.Equalf(t, output[i], planString, "case: %v", tt)
	}
}

func TestOutdatedAnalyze(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (a int, b int, index idx(a))")
	for i := 0; i < 10; i++ {
		testKit.MustExec(fmt.Sprintf("insert into t values (%d,%d)", i, i))
	}
	h := dom.StatsHandle()
	require.NoError(t, h.HandleDDLEvent(<-h.DDLEventCh()))
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	testKit.MustExec("analyze table t")
	testKit.MustExec("insert into t select * from t")
	testKit.MustExec("insert into t select * from t")
	testKit.MustExec("insert into t select * from t")
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(dom.InfoSchema()))
	var input []struct {
		SQL                          string
		EnablePseudoForOutdatedStats bool
		RatioOfPseudoEstimate        float64
	}
	var output []struct {
		SQL                          string
		EnablePseudoForOutdatedStats bool
		RatioOfPseudoEstimate        float64
		Plan                         []string
	}
	analyzeSuiteData := GetAnalyzeSuiteData()
	analyzeSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testKit.Session().GetSessionVars().SetEnablePseudoForOutdatedStats(tt.EnablePseudoForOutdatedStats)
		statistics.RatioOfPseudoEstimate.Store(tt.RatioOfPseudoEstimate)
		plan := testKit.MustQuery(tt.SQL)
		testdata.OnRecord(func() {
			output[i].SQL = tt.SQL
			output[i].EnablePseudoForOutdatedStats = tt.EnablePseudoForOutdatedStats
			output[i].RatioOfPseudoEstimate = tt.RatioOfPseudoEstimate
			output[i].Plan = testdata.ConvertRowsToStrings(plan.Rows())
		})
		plan.Check(testkit.Rows(output[i].Plan...))
	}
}

func TestNullCount(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	testKit.MustExec("create table t (a int, b int, index idx(a))")
	testKit.MustExec("insert into t values (null, null), (null, null)")
	testKit.MustExec("analyze table t")
	var input []string
	var output [][]string
	analyzeSuiteData := GetAnalyzeSuiteData()
	analyzeSuiteData.LoadTestCases(t, &input, &output)
	for i := 0; i < 2; i++ {
		testdata.OnRecord(func() {
			output[i] = testdata.ConvertRowsToStrings(testKit.MustQuery(input[i]).Rows())
		})
		testKit.MustQuery(input[i]).Check(testkit.Rows(output[i]...))
	}
	h := dom.StatsHandle()
	h.Clear()
	require.NoError(t, h.Update(dom.InfoSchema()))
	for i := 2; i < 4; i++ {
		testdata.OnRecord(func() {
			output[i] = testdata.ConvertRowsToStrings(testKit.MustQuery(input[i]).Rows())
		})
		testKit.MustQuery(input[i]).Check(testkit.Rows(output[i]...))
	}
}

func TestCorrelatedEstimation(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("set sql_mode='STRICT_TRANS_TABLES'") // disable only full group by
	tk.MustExec("create table t(a int, b int, c int, index idx(c,b,a))")
	tk.MustExec("insert into t values(1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5), (6,6,6), (7,7,7), (8,8,8), (9,9,9),(10,10,10)")
	tk.MustExec("analyze table t")
	var (
		input  []string
		output [][]string
	)
	analyzeSuiteData := GetAnalyzeSuiteData()
	analyzeSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		rs := tk.MustQuery(tt)
		testdata.OnRecord(func() {
			output[i] = testdata.ConvertRowsToStrings(rs.Rows())
		})
		rs.Check(testkit.Rows(output[i]...))
	}
}

func TestInconsistentEstimation(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, c int, index ab(a,b), index ac(a,c))")
	tk.MustExec("insert into t values (1,1,1), (1000,1000,1000)")
	for i := 0; i < 10; i++ {
		tk.MustExec("insert into t values (5,5,5), (10,10,10)")
	}
	tk.MustExec("set @@tidb_analyze_version=1")
	tk.MustExec("analyze table t with 2 buckets")
	// Force using the histogram to estimate.
	tk.MustExec("update mysql.stats_histograms set stats_ver = 0")
	dom.StatsHandle().Clear()
	require.NoError(t, dom.StatsHandle().Update(dom.InfoSchema()))
	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	analyzeSuiteData := GetAnalyzeSuiteData()
	analyzeSuiteData.LoadTestCases(t, &input, &output)
	for i, sql := range input {
		plan := tk.MustQuery(sql)
		testdata.OnRecord(func() {
			output[i].SQL = sql
			output[i].Plan = testdata.ConvertRowsToStrings(plan.Rows())
		})
		plan.Check(testkit.Rows(output[i].Plan...))
	}
}

func TestIssue9562(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	var input [][]string
	var output []struct {
		SQL  []string
		Plan []string
	}
	analyzeSuiteData := GetAnalyzeSuiteData()
	analyzeSuiteData.LoadTestCases(t, &input, &output)
	for i, ts := range input {
		for j, tt := range ts {
			if j != len(ts)-1 {
				tk.MustExec(tt)
			}
			testdata.OnRecord(func() {
				output[i].SQL = ts
				if j == len(ts)-1 {
					output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
				}
			})
			if j == len(ts)-1 {
				tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
			}
		}
	}
}

func TestLimitCrossEstimation(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("set @@session.tidb_executor_concurrency = 4;")
	tk.MustExec("set @@session.tidb_hash_join_concurrency = 5;")
	tk.MustExec("set @@session.tidb_distsql_scan_concurrency = 15;")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b int not null, c int not null default 0, index idx_bc(b, c))")
	var input [][]string
	var output []struct {
		SQL  []string
		Plan []string
	}
	analyzeSuiteData := GetAnalyzeSuiteData()
	analyzeSuiteData.LoadTestCases(t, &input, &output)
	for i, ts := range input {
		for j, tt := range ts {
			if j != len(ts)-1 {
				tk.MustExec(tt)
			}
			testdata.OnRecord(func() {
				output[i].SQL = ts
				if j == len(ts)-1 {
					output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
				}
			})
			if j == len(ts)-1 {
				tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
			}
		}
	}
}

func TestLowSelIndexGreedySearch(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("set tidb_cost_model_version=2")
	testKit.MustExec(`set tidb_opt_limit_push_down_threshold=0`)
	testKit.MustExec("drop table if exists t")
	testKit.MustExec("create table t (a varchar(32) default null, b varchar(10) default null, c varchar(12) default null, d varchar(32) default null, e bigint(10) default null, key idx1 (d,a), key idx2 (a,c), key idx3 (c,b), key idx4 (e))")
	require.NoError(t, loadTableStats("analyzeSuiteTestLowSelIndexGreedySearchT.json", dom))
	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	// The test purposes are:
	// - index `idx2` runs much faster than `idx4` experimentally;
	// - estimated row count of IndexLookUp should be 0;
	analyzeSuiteData := GetAnalyzeSuiteData()
	analyzeSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
		})
		testKit.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
	}
}

func TestTiFlashCostModel(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("create table t (a int, b int, c int, primary key(a))")
	tk.MustExec("insert into t values(1,1,1), (2,2,2), (3,3,3)")

	tbl, err := dom.InfoSchema().TableByName(model.CIStr{O: "test", L: "test"}, model.CIStr{O: "t", L: "t"})
	require.NoError(t, err)
	// Set the hacked TiFlash replica for explain tests.
	tbl.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: true}

	var input, output [][]string
	analyzeSuiteData := GetAnalyzeSuiteData()
	analyzeSuiteData.LoadTestCases(t, &input, &output)
	for i, ts := range input {
		for j, tt := range ts {
			if j != len(ts)-1 {
				tk.MustExec(tt)
			}
			testdata.OnRecord(func() {
				if j == len(ts)-1 {
					output[i] = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
				}
			})
			if j == len(ts)-1 {
				tk.MustQuery(tt).Check(testkit.Rows(output[i]...))
			}
		}
	}
}

func TestIndexEqualUnknown(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t, t1")
	testKit.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeIntOnly
	testKit.MustExec("CREATE TABLE t(a bigint(20) NOT NULL, b bigint(20) NOT NULL, c bigint(20) NOT NULL, PRIMARY KEY (a,c,b), KEY (b))")
	require.NoError(t, loadTableStats("analyzeSuiteTestIndexEqualUnknownT.json", dom))
	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	analyzeSuiteData := GetAnalyzeSuiteData()
	analyzeSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
		})
		testKit.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
	}
}

func TestLimitIndexEstimation(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, key idx_a(a), key idx_b(b))")
	tk.MustExec("set session tidb_enable_extended_stats = on")
	// Values in column a are from 1 to 1000000, values in column b are from 1000000 to 1,
	// these 2 columns are strictly correlated in reverse order.
	require.NoError(t, loadTableStats("analyzeSuiteTestLimitIndexEstimationT.json", dom))
	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}

	analyzeSuiteData := GetAnalyzeSuiteData()
	analyzeSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
	}
}
