// Copyright 2017 PingCAP, Inc.
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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/planner"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/testdata"
	"github.com/stretchr/testify/require"
)

func loadTableStats(fileName string, dom *domain.Domain) error {
	statsPath := filepath.Join("testdata", fileName)
	bytes, err := os.ReadFile(statsPath)
	if err != nil {
		return err
	}
	statsTbl := &handle.JSONTable{}
	err = json.Unmarshal(bytes, statsTbl)
	if err != nil {
		return err
	}
	statsHandle := dom.StatsHandle()
	err = statsHandle.LoadStatsFromJSON(dom.InfoSchema(), statsTbl)
	if err != nil {
		return err
	}
	return nil
}

func TestExplainAnalyze(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set sql_mode='STRICT_TRANS_TABLES'") // disable only full group by
	tk.MustExec("create table t1(a int, b int, c int, key idx(a, b))")
	tk.MustExec("create table t2(a int, b int)")
	tk.MustExec("insert into t1 values (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4), (5, 5, 5)")
	tk.MustExec("insert into t2 values (2, 22), (3, 33), (5, 55), (233, 2), (333, 3), (3434, 5)")
	tk.MustExec("analyze table t1, t2")
	rs := tk.MustQuery("explain analyze select t1.a, t1.b, sum(t1.c) from t1 join t2 on t1.a = t2.b where t1.a > 1")
	require.Len(t, rs.Rows(), 10)
	for _, row := range rs.Rows() {
		require.Len(t, row, 9)
		execInfo := row[5].(string)
		require.Contains(t, execInfo, "time")
		require.Contains(t, execInfo, "loops")
		if strings.Contains(row[0].(string), "Reader") || strings.Contains(row[0].(string), "IndexLookUp") {
			require.Contains(t, execInfo, "cop_task")
		}
	}
}

// TestCBOWithoutAnalyze tests the plan with stats that only have count info.
func TestCBOWithoutAnalyze(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t1 (a int)")
	testKit.MustExec("create table t2 (a int)")
	h := dom.StatsHandle()
	require.NoError(t, h.HandleDDLEvent(<-h.DDLEventCh()))
	require.NoError(t, h.HandleDDLEvent(<-h.DDLEventCh()))
	testKit.MustExec("insert into t1 values (1), (2), (3), (4), (5), (6)")
	testKit.MustExec("insert into t2 values (1), (2), (3), (4), (5), (6)")
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	require.NoError(t, h.Update(dom.InfoSchema()))
	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	analyzeSuiteData := core.GetAnalyzeSuiteData()
	analyzeSuiteData.GetTestCases(t, &input, &output)
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
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	h := dom.StatsHandle()
	for _, tblName := range []string{"t1", "t2", "t3", "t4"} {
		testKit.MustExec(fmt.Sprintf("create table %s (a int)", tblName))
		require.NoError(t, h.HandleDDLEvent(<-h.DDLEventCh()))
	}
	var input []string
	var output [][]string
	analyzeSuiteData := core.GetAnalyzeSuiteData()
	analyzeSuiteData.GetTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i] = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
		})
		testKit.MustQuery(tt).Check(testkit.Rows(output[i]...))
	}
}

func TestTableDual(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec(`use test`)
	h := dom.StatsHandle()
	testKit.MustExec(`create table t(a int)`)
	testKit.MustExec("insert into t values (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)")
	require.NoError(t, h.HandleDDLEvent(<-h.DDLEventCh()))

	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	require.NoError(t, h.Update(dom.InfoSchema()))
	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	analyzeSuiteData := core.GetAnalyzeSuiteData()
	analyzeSuiteData.GetTestCases(t, &input, &output)
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
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	testKit := testkit.NewTestKit(t, store)
	statistics.RatioOfPseudoEstimate.Store(10.0)
	defer statistics.RatioOfPseudoEstimate.Store(0.7)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (a int)")
	testKit.MustExec("insert into t values (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)")
	testKit.MustExec("insert into t select * from t")
	testKit.MustExec("insert into t select * from t")
	h := dom.StatsHandle()
	require.NoError(t, h.HandleDDLEvent(<-h.DDLEventCh()))
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	testKit.MustExec("analyze table t")
	for i := 1; i <= 8; i++ {
		testKit.MustExec("delete from t where a = ?", i)
	}
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	require.NoError(t, h.Update(dom.InfoSchema()))
	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	analyzeSuiteData := core.GetAnalyzeSuiteData()
	analyzeSuiteData.GetTestCases(t, &input, &output)
	for i, sql := range input {
		plan := testKit.MustQuery(sql)
		testdata.OnRecord(func() {
			output[i].SQL = sql
			output[i].Plan = testdata.ConvertRowsToStrings(plan.Rows())
		})
		plan.Check(testkit.Rows(output[i].Plan...))
	}
}

func constructInsertSQL(i, n int) string {
	sql := "insert into t (a,b,c,e)values "
	for j := 0; j < n; j++ {
		sql += fmt.Sprintf("(%d, %d, '%d', %d)", i*n+j, i, i+j, i*n+j)
		if j != n-1 {
			sql += ", "
		}
	}
	return sql
}

func TestIndexRead(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	testKit := testkit.NewTestKit(t, store)
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

	// This stats is generated by following format:
	// fill (a, b, c, e) as (i*100+j, i, i+j, i*100+j), i and j is dependent and range of this two are [0, 99].
	require.NoError(t, loadTableStats("analyzesSuiteTestIndexReadT.json", dom))
	for i := 1; i < 16; i++ {
		testKit.MustExec(fmt.Sprintf("insert into t1 values(%v, %v)", i, i))
	}
	testKit.MustExec("analyze table t1")
	ctx := testKit.Session()
	var input, output []string
	analyzeSuiteData := core.GetAnalyzeSuiteData()
	analyzeSuiteData.GetTestCases(t, &input, &output)

	for i, tt := range input {
		stmts, err := session.Parse(ctx, tt)
		require.NoError(t, err)
		require.Len(t, stmts, 1)
		stmt := stmts[0]
		ret := &core.PreprocessorReturn{}
		err = core.Preprocess(ctx, stmt, core.WithPreprocessorReturn(ret))
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
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t, t1")
	testKit.MustExec("create table t (c1 int)")
	testKit.MustExec("create table t1 (c1 int)")
	testKit.MustExec("analyze table t, t1")
	var input, output []string
	analyzeSuiteData := core.GetAnalyzeSuiteData()
	analyzeSuiteData.GetTestCases(t, &input, &output)
	for i, tt := range input {
		ctx := testKit.Session()
		stmts, err := session.Parse(ctx, tt)
		require.NoError(t, err)
		require.Len(t, stmts, 1)
		stmt := stmts[0]
		ret := &core.PreprocessorReturn{}
		err = core.Preprocess(ctx, stmt, core.WithPreprocessorReturn(ret))
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
	store, clean := testkit.CreateMockStore(t)
	defer clean()
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
	require.EqualError(t, err, "analyze view v is not supported now.")
	testKit.MustExec("drop view v")

	testKit.MustExec("create sequence seq")
	_, err = testKit.Exec("analyze table seq")
	require.EqualError(t, err, "analyze sequence seq is not supported now.")
	testKit.MustExec("drop sequence seq")

	var input, output []string
	analyzeSuiteData := core.GetAnalyzeSuiteData()
	analyzeSuiteData.GetTestCases(t, &input, &output)

	for i, tt := range input {
		ctx := testKit.Session()
		stmts, err := session.Parse(ctx, tt)
		require.NoError(t, err)
		require.Len(t, stmts, 1)
		stmt := stmts[0]
		err = executor.ResetContextOfStmt(ctx, stmt)
		require.NoError(t, err)
		ret := &core.PreprocessorReturn{}
		err = core.Preprocess(ctx, stmt, core.WithPreprocessorReturn(ret))
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
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (a int, b int, index idx(a))")
	for i := 0; i < 10; i++ {
		testKit.MustExec(fmt.Sprintf("insert into t values (%d,%d)", i, i))
	}
	h := dom.StatsHandle()
	require.NoError(t, h.HandleDDLEvent(<-h.DDLEventCh()))
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	testKit.MustExec("analyze table t")
	testKit.MustExec("insert into t select * from t")
	testKit.MustExec("insert into t select * from t")
	testKit.MustExec("insert into t select * from t")
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
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
	analyzeSuiteData := core.GetAnalyzeSuiteData()
	analyzeSuiteData.GetTestCases(t, &input, &output)
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
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	testKit.MustExec("create table t (a int, b int, index idx(a))")
	testKit.MustExec("insert into t values (null, null), (null, null)")
	testKit.MustExec("analyze table t")
	var input []string
	var output [][]string
	analyzeSuiteData := core.GetAnalyzeSuiteData()
	analyzeSuiteData.GetTestCases(t, &input, &output)
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
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set sql_mode='STRICT_TRANS_TABLES'") // disable only full group by
	tk.MustExec("create table t(a int, b int, c int, index idx(c,b,a))")
	tk.MustExec("insert into t values(1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5), (6,6,6), (7,7,7), (8,8,8), (9,9,9),(10,10,10)")
	tk.MustExec("analyze table t")
	var (
		input  []string
		output [][]string
	)
	analyzeSuiteData := core.GetAnalyzeSuiteData()
	analyzeSuiteData.GetTestCases(t, &input, &output)
	for i, tt := range input {
		rs := tk.MustQuery(tt)
		testdata.OnRecord(func() {
			output[i] = testdata.ConvertRowsToStrings(rs.Rows())
		})
		rs.Check(testkit.Rows(output[i]...))
	}
}

func TestInconsistentEstimation(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
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
	analyzeSuiteData := core.GetAnalyzeSuiteData()
	analyzeSuiteData.GetTestCases(t, &input, &output)
	for i, sql := range input {
		plan := tk.MustQuery(sql)
		testdata.OnRecord(func() {
			output[i].SQL = sql
			output[i].Plan = testdata.ConvertRowsToStrings(plan.Rows())
		})
		plan.Check(testkit.Rows(output[i].Plan...))
	}
}

func BenchmarkOptimize(b *testing.B) {
	store, clean := testkit.CreateMockStore(b)
	defer clean()

	testKit := testkit.NewTestKit(b, store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	testKit.MustExec("create table t (a int primary key, b int, c varchar(200), d datetime DEFAULT CURRENT_TIMESTAMP, e int, ts timestamp DEFAULT CURRENT_TIMESTAMP)")
	testKit.MustExec("create index b on t (b)")
	testKit.MustExec("create index d on t (d)")
	testKit.MustExec("create index e on t (e)")
	testKit.MustExec("create index b_c on t (b,c)")
	testKit.MustExec("create index ts on t (ts)")
	for i := 0; i < 100; i++ {
		testKit.MustExec(constructInsertSQL(i, 100))
	}
	testKit.MustExec("analyze table t")
	tests := []struct {
		sql  string
		best string
	}{
		{
			sql:  "select count(*) from t group by e",
			best: "IndexReader(Index(t.e)[[NULL,+inf]])->StreamAgg",
		},
		{
			sql:  "select count(*) from t where e <= 10 group by e",
			best: "IndexReader(Index(t.e)[[-inf,10]])->StreamAgg",
		},
		{
			sql:  "select count(*) from t where e <= 50",
			best: "IndexReader(Index(t.e)[[-inf,50]]->HashAgg)->HashAgg",
		},
		{
			sql:  "select count(*) from t where c > '1' group by b",
			best: "IndexReader(Index(t.b_c)[[NULL,+inf]]->Sel([gt(test.t.c, 1)]))->StreamAgg",
		},
		{
			sql:  "select count(*) from t where e = 1 group by b",
			best: "IndexLookUp(Index(t.e)[[1,1]], Table(t)->HashAgg)->HashAgg",
		},
		{
			sql:  "select count(*) from t where e > 1 group by b",
			best: "TableReader(Table(t)->Sel([gt(test.t.e, 1)])->HashAgg)->HashAgg",
		},
		{
			sql:  "select count(e) from t where t.b <= 20",
			best: "IndexLookUp(Index(t.b)[[-inf,20]], Table(t)->HashAgg)->HashAgg",
		},
		{
			sql:  "select count(e) from t where t.b <= 30",
			best: "IndexLookUp(Index(t.b)[[-inf,30]], Table(t)->HashAgg)->HashAgg",
		},
		{
			sql:  "select count(e) from t where t.b <= 40",
			best: "IndexLookUp(Index(t.b)[[-inf,40]], Table(t)->HashAgg)->HashAgg",
		},
		{
			sql:  "select count(e) from t where t.b <= 50",
			best: "TableReader(Table(t)->Sel([le(test.t.b, 50)])->HashAgg)->HashAgg",
		},
		{
			sql:  "select * from t where t.b <= 40",
			best: "IndexLookUp(Index(t.b)[[-inf,40]], Table(t))",
		},
		{
			sql:  "select * from t where t.b <= 50",
			best: "TableReader(Table(t)->Sel([le(test.t.b, 50)]))",
		},
		// test panic
		{
			sql:  "select * from t where 1 and t.b <= 50",
			best: "TableReader(Table(t)->Sel([le(test.t.b, 50)]))",
		},
		{
			sql:  "select * from t where t.b <= 100 order by t.a limit 1",
			best: "TableReader(Table(t)->Sel([le(test.t.b, 100)])->Limit)->Limit",
		},
		{
			sql:  "select * from t where t.b <= 1 order by t.a limit 10",
			best: "IndexLookUp(Index(t.b)[[-inf,1]]->TopN([test.t.a],0,10), Table(t))->TopN([test.t.a],0,10)",
		},
		{
			sql:  "select * from t use index(b) where b = 1 order by a",
			best: "IndexLookUp(Index(t.b)[[1,1]], Table(t))->Sort",
		},
		// test datetime
		{
			sql:  "select * from t where d < cast('1991-09-05' as datetime)",
			best: "IndexLookUp(Index(t.d)[[-inf,1991-09-05 00:00:00)], Table(t))",
		},
		// test timestamp
		{
			sql:  "select * from t where ts < '1991-09-05'",
			best: "IndexLookUp(Index(t.ts)[[-inf,1991-09-05 00:00:00)], Table(t))",
		},
	}
	for _, tt := range tests {
		ctx := testKit.Session()
		stmts, err := session.Parse(ctx, tt.sql)
		require.NoError(b, err)
		require.Len(b, stmts, 1)
		stmt := stmts[0]
		ret := &core.PreprocessorReturn{}
		err = core.Preprocess(ctx, stmt, core.WithPreprocessorReturn(ret))
		require.NoError(b, err)

		b.Run(tt.sql, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _, err := planner.Optimize(context.TODO(), ctx, stmt, ret.InfoSchema)
				require.NoError(b, err)
			}
			b.ReportAllocs()
		})
	}
}

func TestIssue9562(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	var input [][]string
	var output []struct {
		SQL  []string
		Plan []string
	}
	analyzeSuiteData := core.GetAnalyzeSuiteData()
	analyzeSuiteData.GetTestCases(t, &input, &output)
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

func TestIssue9805(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec(`
		create table t1 (
			id bigint primary key,
			a bigint not null,
			b varchar(100) not null,
			c varchar(10) not null,
			d bigint as (a % 30) not null,
			key (d, b, c)
		)
	`)
	tk.MustExec(`
		create table t2 (
			id varchar(50) primary key,
			a varchar(100) unique,
			b datetime,
			c varchar(45),
			d int not null unique auto_increment
		)
	`)
	// Test when both tables are empty, EXPLAIN ANALYZE for IndexLookUp would not panic.
	tk.MustQuery("explain analyze select /*+ TIDB_INLJ(t2) */ t1.id, t2.a from t1 join t2 on t1.a = t2.d where t1.b = 't2' and t1.d = 4")
}

func TestLimitCrossEstimation(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

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
	analyzeSuiteData := core.GetAnalyzeSuiteData()
	analyzeSuiteData.GetTestCases(t, &input, &output)
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
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
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
	analyzeSuiteData := core.GetAnalyzeSuiteData()
	analyzeSuiteData.GetTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
		})
		testKit.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
	}
}

func TestUpdateProjEliminate(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("explain update t t1, (select distinct b from t) t2 set t1.b = t2.b")
}

func TestTiFlashCostModel(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("create table t (a int, b int, c int, primary key(a))")
	tk.MustExec("insert into t values(1,1,1), (2,2,2), (3,3,3)")

	tbl, err := dom.InfoSchema().TableByName(model.CIStr{O: "test", L: "test"}, model.CIStr{O: "t", L: "t"})
	require.NoError(t, err)
	// Set the hacked TiFlash replica for explain tests.
	tbl.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: true}

	var input, output [][]string
	analyzeSuiteData := core.GetAnalyzeSuiteData()
	analyzeSuiteData.GetTestCases(t, &input, &output)
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
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
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
	analyzeSuiteData := core.GetAnalyzeSuiteData()
	analyzeSuiteData.GetTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
		})
		testKit.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
	}
}

func TestLimitIndexEstimation(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
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
	analyzeSuiteData := core.GetAnalyzeSuiteData()
	analyzeSuiteData.GetTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
	}
}

func TestBatchPointGetTablePartition(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t1,t2,t3,t4,t5,t6")

	testKit.MustExec("create table t1(a int, b int, primary key(a,b)) partition by hash(b) partitions 2")
	testKit.MustExec("insert into t1 values(1,1),(1,2),(2,1),(2,2)")
	testKit.MustExec("set @@tidb_partition_prune_mode = 'static'")
	testKit.MustQuery("explain format = 'brief' select * from t1 where a in (1,2) and b = 1").Check(testkit.Rows(
		"Batch_Point_Get 2.00 root table:t1, index:PRIMARY(a, b) keep order:false, desc:false",
	))
	testKit.MustQuery("select * from t1 where a in (1,2) and b = 1").Sort().Check(testkit.Rows(
		"1 1",
		"2 1",
	))
	testKit.MustQuery("explain format = 'brief' select * from t1 where a = 1 and b in (1,2)").Check(testkit.Rows(
		"PartitionUnion 4.00 root  ",
		"├─Batch_Point_Get 2.00 root table:t1, index:PRIMARY(a, b) keep order:false, desc:false",
		"└─Batch_Point_Get 2.00 root table:t1, index:PRIMARY(a, b) keep order:false, desc:false",
	))
	testKit.MustQuery("select * from t1 where a = 1 and b in (1,2)").Sort().Check(testkit.Rows(
		"1 1",
		"1 2",
	))
	testKit.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
	testKit.MustQuery("explain format = 'brief' select * from t1 where a in (1,2) and b = 1").Check(testkit.Rows(
		"IndexReader 2.00 root partition:p1 index:IndexRangeScan",
		"└─IndexRangeScan 2.00 cop[tikv] table:t1, index:PRIMARY(a, b) range:[1 1,1 1], [2 1,2 1], keep order:false, stats:pseudo",
	))
	testKit.MustQuery("select * from t1 where a in (1,2) and b = 1").Sort().Check(testkit.Rows(
		"1 1",
		"2 1",
	))
	testKit.MustQuery("explain format = 'brief' select * from t1 where a = 1 and b in (1,2)").Check(testkit.Rows(
		"IndexReader 2.00 root partition:p0,p1 index:IndexRangeScan",
		"└─IndexRangeScan 2.00 cop[tikv] table:t1, index:PRIMARY(a, b) range:[1 1,1 1], [1 2,1 2], keep order:false, stats:pseudo",
	))
	testKit.MustQuery("select * from t1 where a = 1 and b in (1,2)").Sort().Check(testkit.Rows(
		"1 1",
		"1 2",
	))

	testKit.MustExec("create table t2(a int, b int, primary key(a,b)) partition by range(b) (partition p0 values less than (2), partition p1 values less than maxvalue)")
	testKit.MustExec("insert into t2 values(1,1),(1,2),(2,1),(2,2)")
	testKit.MustExec("set @@tidb_partition_prune_mode = 'static'")
	testKit.MustQuery("explain format = 'brief' select * from t2 where a in (1,2) and b = 1").Check(testkit.Rows(
		"IndexReader 2.00 root  index:IndexRangeScan",
		"└─IndexRangeScan 2.00 cop[tikv] table:t2, partition:p0, index:PRIMARY(a, b) range:[1 1,1 1], [2 1,2 1], keep order:false, stats:pseudo",
	))
	testKit.MustQuery("select * from t2 where a in (1,2) and b = 1").Sort().Check(testkit.Rows(
		"1 1",
		"2 1",
	))
	testKit.MustQuery("explain format = 'brief' select * from t2 where a = 1 and b in (1,2)").Check(testkit.Rows(
		"PartitionUnion 4.00 root  ",
		"├─IndexReader 2.00 root  index:IndexRangeScan",
		"│ └─IndexRangeScan 2.00 cop[tikv] table:t2, partition:p0, index:PRIMARY(a, b) range:[1 1,1 1], [1 2,1 2], keep order:false, stats:pseudo",
		"└─IndexReader 2.00 root  index:IndexRangeScan",
		"  └─IndexRangeScan 2.00 cop[tikv] table:t2, partition:p1, index:PRIMARY(a, b) range:[1 1,1 1], [1 2,1 2], keep order:false, stats:pseudo",
	))
	testKit.MustQuery("select * from t2 where a = 1 and b in (1,2)").Sort().Check(testkit.Rows(
		"1 1",
		"1 2",
	))
	testKit.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
	testKit.MustQuery("explain format = 'brief' select * from t2 where a in (1,2) and b = 1").Check(testkit.Rows(
		"IndexReader 2.00 root partition:p0 index:IndexRangeScan",
		"└─IndexRangeScan 2.00 cop[tikv] table:t2, index:PRIMARY(a, b) range:[1 1,1 1], [2 1,2 1], keep order:false, stats:pseudo",
	))
	testKit.MustQuery("select * from t2 where a in (1,2) and b = 1").Sort().Check(testkit.Rows(
		"1 1",
		"2 1",
	))
	testKit.MustQuery("explain format = 'brief' select * from t2 where a = 1 and b in (1,2)").Check(testkit.Rows(
		"IndexReader 2.00 root partition:all index:IndexRangeScan",
		"└─IndexRangeScan 2.00 cop[tikv] table:t2, index:PRIMARY(a, b) range:[1 1,1 1], [1 2,1 2], keep order:false, stats:pseudo",
	))
	testKit.MustQuery("select * from t2 where a = 1 and b in (1,2)").Sort().Check(testkit.Rows(
		"1 1",
		"1 2",
	))

	testKit.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn

	testKit.MustExec("create table t3(a int, b int, primary key(a,b)) partition by hash(b) partitions 2")
	testKit.MustExec("insert into t3 values(1,1),(1,2),(2,1),(2,2)")
	testKit.MustExec("set @@tidb_partition_prune_mode = 'static'")
	testKit.MustQuery("explain format = 'brief' select * from t3 where a in (1,2) and b = 1").Check(testkit.Rows(
		"Batch_Point_Get 2.00 root table:t3, clustered index:PRIMARY(a, b) keep order:false, desc:false",
	))
	testKit.MustQuery("select * from t3 where a in (1,2) and b = 1").Sort().Check(testkit.Rows(
		"1 1",
		"2 1",
	))
	testKit.MustQuery("explain format = 'brief' select * from t3 where a = 1 and b in (1,2)").Check(testkit.Rows(
		"PartitionUnion 0.04 root  ",
		"├─Batch_Point_Get 2.00 root table:t3, clustered index:PRIMARY(a, b) keep order:false, desc:false",
		"└─Batch_Point_Get 2.00 root table:t3, clustered index:PRIMARY(a, b) keep order:false, desc:false",
	))
	testKit.MustQuery("select * from t3 where a = 1 and b in (1,2)").Sort().Check(testkit.Rows(
		"1 1",
		"1 2",
	))
	testKit.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
	testKit.MustQuery("explain format = 'brief' select * from t3 where a in (1,2) and b = 1").Check(testkit.Rows(
		"TableReader 2.00 root partition:p1 data:TableRangeScan",
		"└─TableRangeScan 2.00 cop[tikv] table:t3 range:[1 1,1 1], [2 1,2 1], keep order:false, stats:pseudo",
	))
	testKit.MustQuery("select * from t3 where a in (1,2) and b = 1").Sort().Check(testkit.Rows(
		"1 1",
		"2 1",
	))
	testKit.MustQuery("explain format = 'brief' select * from t3 where a = 1 and b in (1,2)").Check(testkit.Rows(
		"TableReader 2.00 root partition:p0,p1 data:TableRangeScan",
		"└─TableRangeScan 2.00 cop[tikv] table:t3 range:[1 1,1 1], [1 2,1 2], keep order:false, stats:pseudo",
	))
	testKit.MustQuery("select * from t3 where a = 1 and b in (1,2)").Sort().Check(testkit.Rows(
		"1 1",
		"1 2",
	))

	testKit.MustExec("create table t4(a int, b int, primary key(a,b)) partition by range(b) (partition p0 values less than (2), partition p1 values less than maxvalue)")
	testKit.MustExec("insert into t4 values(1,1),(1,2),(2,1),(2,2)")
	testKit.MustExec("set @@tidb_partition_prune_mode = 'static'")
	testKit.MustQuery("explain format = 'brief' select * from t4 where a in (1,2) and b = 1").Check(testkit.Rows(
		"TableReader 2.00 root  data:TableRangeScan",
		"└─TableRangeScan 2.00 cop[tikv] table:t4, partition:p0 range:[1 1,1 1], [2 1,2 1], keep order:false, stats:pseudo",
	))
	testKit.MustQuery("select * from t4 where a in (1,2) and b = 1").Sort().Check(testkit.Rows(
		"1 1",
		"2 1",
	))
	testKit.MustQuery("explain format = 'brief' select * from t4 where a = 1 and b in (1,2)").Check(testkit.Rows(
		"PartitionUnion 0.04 root  ",
		"├─TableReader 2.00 root  data:TableRangeScan",
		"│ └─TableRangeScan 2.00 cop[tikv] table:t4, partition:p0 range:[1 1,1 1], [1 2,1 2], keep order:false, stats:pseudo",
		"└─TableReader 2.00 root  data:TableRangeScan",
		"  └─TableRangeScan 2.00 cop[tikv] table:t4, partition:p1 range:[1 1,1 1], [1 2,1 2], keep order:false, stats:pseudo",
	))
	testKit.MustQuery("select * from t4 where a = 1 and b in (1,2)").Sort().Check(testkit.Rows(
		"1 1",
		"1 2",
	))
	testKit.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
	testKit.MustQuery("explain format = 'brief' select * from t4 where a in (1,2) and b = 1").Check(testkit.Rows(
		"TableReader 2.00 root partition:p0 data:TableRangeScan",
		"└─TableRangeScan 2.00 cop[tikv] table:t4 range:[1 1,1 1], [2 1,2 1], keep order:false, stats:pseudo",
	))
	testKit.MustQuery("select * from t4 where a in (1,2) and b = 1").Sort().Check(testkit.Rows(
		"1 1",
		"2 1",
	))
	testKit.MustQuery("explain format = 'brief' select * from t4 where a = 1 and b in (1,2)").Check(testkit.Rows(
		"TableReader 2.00 root partition:all data:TableRangeScan",
		"└─TableRangeScan 2.00 cop[tikv] table:t4 range:[1 1,1 1], [1 2,1 2], keep order:false, stats:pseudo",
	))
	testKit.MustQuery("select * from t4 where a = 1 and b in (1,2)").Sort().Check(testkit.Rows(
		"1 1",
		"1 2",
	))

	testKit.MustExec("create table t5(a int, b int, primary key(a)) partition by hash(a) partitions 2")
	testKit.MustExec("insert into t5 values(1,0),(2,0),(3,0),(4,0)")
	testKit.MustExec("set @@tidb_partition_prune_mode = 'static'")
	testKit.MustQuery("explain format = 'brief' select * from t5 where a in (1,2) and 1 = 1").Check(testkit.Rows(
		"PartitionUnion 4.00 root  ",
		"├─Batch_Point_Get 2.00 root table:t5 handle:[1 2], keep order:false, desc:false",
		"└─Batch_Point_Get 2.00 root table:t5 handle:[1 2], keep order:false, desc:false",
	))
	testKit.MustQuery("select * from t5 where a in (1,2) and 1 = 1").Sort().Check(testkit.Rows(
		"1 0",
		"2 0",
	))
	testKit.MustQuery("explain format = 'brief' select * from t5 where a in (1,3) and 1 = 1").Check(testkit.Rows(
		"Batch_Point_Get 2.00 root table:t5 handle:[1 3], keep order:false, desc:false",
	))
	testKit.MustQuery("select * from t5 where a in (1,3) and 1 = 1").Sort().Check(testkit.Rows(
		"1 0",
		"3 0",
	))
	testKit.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
	testKit.MustQuery("explain format = 'brief' select * from t5 where a in (1,2) and 1 = 1").Check(testkit.Rows(
		"TableReader 2.00 root partition:p0,p1 data:TableRangeScan",
		"└─TableRangeScan 2.00 cop[tikv] table:t5 range:[1,1], [2,2], keep order:false, stats:pseudo",
	))
	testKit.MustQuery("select * from t5 where a in (1,2) and 1 = 1").Sort().Check(testkit.Rows(
		"1 0",
		"2 0",
	))
	testKit.MustQuery("explain format = 'brief' select * from t5 where a in (1,3) and 1 = 1").Check(testkit.Rows(
		"TableReader 2.00 root partition:p1 data:TableRangeScan",
		"└─TableRangeScan 2.00 cop[tikv] table:t5 range:[1,1], [3,3], keep order:false, stats:pseudo",
	))
	testKit.MustQuery("select * from t5 where a in (1,3) and 1 = 1").Sort().Check(testkit.Rows(
		"1 0",
		"3 0",
	))

	testKit.MustExec("create table t6(a int, b int, primary key(a)) partition by range(a) (partition p0 values less than (3), partition p1 values less than maxvalue)")
	testKit.MustExec("insert into t6 values(1,0),(2,0),(3,0),(4,0)")
	testKit.MustExec("set @@tidb_partition_prune_mode = 'static'")
	testKit.MustQuery("explain format = 'brief' select * from t6 where a in (1,2) and 1 = 1").Check(testkit.Rows(
		"TableReader 2.00 root  data:TableRangeScan",
		"└─TableRangeScan 2.00 cop[tikv] table:t6, partition:p0 range:[1,1], [2,2], keep order:false, stats:pseudo",
	))
	testKit.MustQuery("select * from t6 where a in (1,2) and 1 = 1").Sort().Check(testkit.Rows(
		"1 0",
		"2 0",
	))
	testKit.MustQuery("explain format = 'brief' select * from t6 where a in (1,3) and 1 = 1").Check(testkit.Rows(
		"PartitionUnion 4.00 root  ",
		"├─TableReader 2.00 root  data:TableRangeScan",
		"│ └─TableRangeScan 2.00 cop[tikv] table:t6, partition:p0 range:[1,1], [3,3], keep order:false, stats:pseudo",
		"└─TableReader 2.00 root  data:TableRangeScan",
		"  └─TableRangeScan 2.00 cop[tikv] table:t6, partition:p1 range:[1,1], [3,3], keep order:false, stats:pseudo",
	))
	testKit.MustQuery("select * from t6 where a in (1,3) and 1 = 1").Sort().Check(testkit.Rows(
		"1 0",
		"3 0",
	))
	testKit.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
	testKit.MustQuery("explain format = 'brief' select * from t6 where a in (1,2) and 1 = 1").Check(testkit.Rows(
		"TableReader 2.00 root partition:p0 data:TableRangeScan",
		"└─TableRangeScan 2.00 cop[tikv] table:t6 range:[1,1], [2,2], keep order:false, stats:pseudo",
	))
	testKit.MustQuery("select * from t6 where a in (1,2) and 1 = 1").Sort().Check(testkit.Rows(
		"1 0",
		"2 0",
	))
	testKit.MustQuery("explain format = 'brief' select * from t6 where a in (1,3) and 1 = 1").Check(testkit.Rows(
		"TableReader 2.00 root partition:all data:TableRangeScan",
		"└─TableRangeScan 2.00 cop[tikv] table:t6 range:[1,1], [3,3], keep order:false, stats:pseudo",
	))
	testKit.MustQuery("select * from t6 where a in (1,3) and 1 = 1").Sort().Check(testkit.Rows(
		"1 0",
		"3 0",
	))
}

// TestAppendIntPkToIndexTailForRangeBuilding tests for issue25219 https://github.com/pingcap/tidb/issues/25219.
func TestAppendIntPkToIndexTailForRangeBuilding(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t25219(a int primary key, col3 int, col1 int, index idx(col3))")
	tk.MustExec("insert into t25219 values(1, 1, 1)")
	tk.MustExec("analyze table t25219")
	tk.MustQuery("select * from t25219 WHERE (col3 IS NULL OR col1 IS NOT NULL AND col3 <= 6659) AND col3 = 1;").Check(testkit.Rows("1 1 1"))
}
