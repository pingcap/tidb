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
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/planner"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/planner/util"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/unistore"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/external"
	"github.com/pingcap/tidb/testkit/testdata"
	"github.com/pingcap/tidb/util/hint"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/testutils"
)

func TestDAGPlanBuilderSimpleCase(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("set tidb_opt_limit_push_down_threshold=0")
	var input []string
	var output []struct {
		SQL  string
		Best string
	}
	planSuiteData := core.GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	p := parser.New()
	is := infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable()})
	for i, tt := range input {
		comment := fmt.Sprintf("case: %v, sql: %s", i, tt)
		stmt, err := p.ParseOneStmt(tt, "", "")
		require.NoError(t, err, comment)
		require.NoError(t, sessiontxn.NewTxn(context.Background(), tk.Session()))
		p, _, err := planner.Optimize(context.TODO(), tk.Session(), stmt, is)
		require.NoError(t, err)
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Best = core.ToString(p)
		})
		require.Equal(t, output[i].Best, core.ToString(p), comment)
	}
}

func TestAnalyzeBuildSucc(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int)")
	tests := []struct {
		sql      string
		succ     bool
		statsVer int
	}{
		{
			sql:      "analyze table t with 0.1 samplerate",
			succ:     true,
			statsVer: 2,
		},
		{
			sql:      "analyze table t with 0.1 samplerate",
			succ:     false,
			statsVer: 1,
		},
		{
			sql:      "analyze table t with 10 samplerate",
			succ:     false,
			statsVer: 2,
		},
		{
			sql:      "analyze table t with 0.1 samplerate, 100000 samples",
			succ:     false,
			statsVer: 2,
		},
		{
			sql:      "analyze table t with 0.1 samplerate, 100000 samples",
			succ:     false,
			statsVer: 1,
		},
	}

	p := parser.New()
	is := infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable()})
	for i, tt := range tests {
		comment := fmt.Sprintf("The %v-th test failed", i)
		tk.MustExec(fmt.Sprintf("set @@tidb_analyze_version=%v", tt.statsVer))

		stmt, err := p.ParseOneStmt(tt.sql, "", "")
		if tt.succ {
			require.NoError(t, err, comment)
		} else if err != nil {
			continue
		}
		err = core.Preprocess(context.Background(), tk.Session(), stmt, core.WithPreprocessorReturn(&core.PreprocessorReturn{InfoSchema: is}))
		require.NoError(t, err)
		_, _, err = planner.Optimize(context.Background(), tk.Session(), stmt, is)
		if tt.succ {
			require.NoError(t, err, comment)
		} else {
			require.Error(t, err, comment)
		}
	}
}

func TestAnalyzeSetRate(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int)")
	tests := []struct {
		sql  string
		rate float64
	}{
		{
			sql:  "analyze table t",
			rate: -1,
		},
		{
			sql:  "analyze table t with 0.1 samplerate",
			rate: 0.1,
		},
		{
			sql:  "analyze table t with 10000 samples",
			rate: -1,
		},
	}

	p := parser.New()
	is := infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable()})
	for i, tt := range tests {
		comment := fmt.Sprintf("The %v-th test failed", i)
		stmt, err := p.ParseOneStmt(tt.sql, "", "")
		require.NoError(t, err, comment)

		err = core.Preprocess(context.Background(), tk.Session(), stmt, core.WithPreprocessorReturn(&core.PreprocessorReturn{InfoSchema: is}))
		require.NoError(t, err, comment)
		p, _, err := planner.Optimize(context.Background(), tk.Session(), stmt, is)
		require.NoError(t, err, comment)
		ana := p.(*core.Analyze)
		require.Equal(t, tt.rate, math.Float64frombits(ana.Opts[ast.AnalyzeOptSampleRate]))
	}
}

func TestDAGPlanBuilderJoin(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	sessionVars := tk.Session().GetSessionVars()
	sessionVars.ExecutorConcurrency = 4
	sessionVars.SetDistSQLScanConcurrency(15)
	sessionVars.SetHashJoinConcurrency(5)

	var input []string
	var output []struct {
		SQL  string
		Best string
	}
	planSuiteData := core.GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)

	p := parser.New()
	is := infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable()})
	for i, tt := range input {
		comment := fmt.Sprintf("case:%v sql:%s", i, tt)
		stmt, err := p.ParseOneStmt(tt, "", "")
		require.NoError(t, err, comment)

		p, _, err := planner.Optimize(context.TODO(), tk.Session(), stmt, is)
		require.NoError(t, err)
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Best = core.ToString(p)
		})
		require.Equal(t, output[i].Best, core.ToString(p), comment)
	}
}

func TestDAGPlanBuilderSubquery(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("set sql_mode='STRICT_TRANS_TABLES'") // disable only full group by
	sessionVars := tk.Session().GetSessionVars()
	sessionVars.SetHashAggFinalConcurrency(1)
	sessionVars.SetHashAggPartialConcurrency(1)
	sessionVars.SetHashJoinConcurrency(5)
	sessionVars.SetDistSQLScanConcurrency(15)
	sessionVars.ExecutorConcurrency = 4
	var input []string
	var output []struct {
		SQL  string
		Best string
	}
	planSuiteData := core.GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	p := parser.New()
	is := infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable()})
	for i, tt := range input {
		comment := fmt.Sprintf("input: %s", tt)
		stmt, err := p.ParseOneStmt(tt, "", "")
		require.NoError(t, err, comment)

		p, _, err := planner.Optimize(context.TODO(), tk.Session(), stmt, is)
		require.NoError(t, err)
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Best = core.ToString(p)
		})
		require.Equal(t, output[i].Best, core.ToString(p), fmt.Sprintf("input: %s", tt))
	}
}

func TestDAGPlanTopN(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	var input []string
	var output []struct {
		SQL  string
		Best string
	}
	planSuiteData := core.GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	p := parser.New()
	is := infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable()})
	for i, tt := range input {
		comment := fmt.Sprintf("case:%v sql:%s", i, tt)
		stmt, err := p.ParseOneStmt(tt, "", "")
		require.NoError(t, err, comment)

		p, _, err := planner.Optimize(context.TODO(), tk.Session(), stmt, is)
		require.NoError(t, err)
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Best = core.ToString(p)
		})
		require.Equal(t, output[i].Best, core.ToString(p), comment)
	}
}

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

func TestDAGPlanBuilderBasePhysicalPlan(t *testing.T) {
	store := testkit.CreateMockStore(t)

	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	_, err = se.Execute(context.Background(), "use test")
	require.NoError(t, err)

	var input []string
	var output []struct {
		SQL   string
		Best  string
		Hints string
	}
	planSuiteData := core.GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	p := parser.New()
	is := infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable()})
	for i, tt := range input {
		comment := fmt.Sprintf("input: %s", tt)
		stmt, err := p.ParseOneStmt(tt, "", "")
		require.NoError(t, err, comment)

		err = core.Preprocess(context.Background(), se, stmt, core.WithPreprocessorReturn(&core.PreprocessorReturn{InfoSchema: is}))
		require.NoError(t, err)
		p, _, err := planner.Optimize(context.TODO(), se, stmt, is)
		require.NoError(t, err)
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Best = core.ToString(p)
			output[i].Hints = hint.RestoreOptimizerHints(core.GenHintsFromPhysicalPlan(p))
		})
		require.Equal(t, output[i].Best, core.ToString(p), fmt.Sprintf("input: %s", tt))
		hints := core.GenHintsFromPhysicalPlan(p)

		// test the new genHints code
		flat := core.FlattenPhysicalPlan(p, false)
		newHints := core.GenHintsFromFlatPlan(flat)
		assertSameHints(t, hints, newHints)

		require.Equal(t, output[i].Hints, hint.RestoreOptimizerHints(hints), fmt.Sprintf("input: %s", tt))
	}
}

func TestDAGPlanBuilderUnion(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	var input []string
	var output []struct {
		SQL  string
		Best string
	}
	planSuiteData := core.GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	p := parser.New()
	is := infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable()})
	for i, tt := range input {
		comment := fmt.Sprintf("case:%v sql:%s", i, tt)
		stmt, err := p.ParseOneStmt(tt, "", "")
		require.NoError(t, err, comment)

		p, _, err := planner.Optimize(context.TODO(), tk.Session(), stmt, is)
		require.NoError(t, err)
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Best = core.ToString(p)
		})
		require.Equal(t, output[i].Best, core.ToString(p), comment)
	}
}

func TestDAGPlanBuilderUnionScan(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, c int)")

	var input []string
	var output []struct {
		SQL  string
		Best string
	}
	planSuiteData := core.GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)

	p := parser.New()
	for i, tt := range input {
		tk.MustExec("begin;")
		tk.MustExec("insert into t values(2, 2, 2);")

		comment := fmt.Sprintf("input: %s", tt)
		stmt, err := p.ParseOneStmt(tt, "", "")
		require.NoError(t, err, comment)
		dom := domain.GetDomain(tk.Session())
		require.NoError(t, dom.Reload())
		plan, _, err := planner.Optimize(context.TODO(), tk.Session(), stmt, dom.InfoSchema())
		require.NoError(t, err)
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Best = core.ToString(plan)
		})
		require.Equal(t, output[i].Best, core.ToString(plan), fmt.Sprintf("input: %s", tt))
		tk.MustExec("rollback;")
	}
}

func TestDAGPlanBuilderAgg(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("set sql_mode='STRICT_TRANS_TABLES'") // disable only full group by
	sessionVars := tk.Session().GetSessionVars()
	sessionVars.SetHashAggFinalConcurrency(1)
	sessionVars.SetHashAggPartialConcurrency(1)
	sessionVars.SetDistSQLScanConcurrency(15)
	sessionVars.ExecutorConcurrency = 4

	var input []string
	var output []struct {
		SQL  string
		Best string
	}
	planSuiteData := core.GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	p := parser.New()
	is := infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable()})
	for i, tt := range input {
		comment := fmt.Sprintf("input: %s", tt)
		stmt, err := p.ParseOneStmt(tt, "", "")
		require.NoError(t, err, comment)

		p, _, err := planner.Optimize(context.TODO(), tk.Session(), stmt, is)
		require.NoError(t, err)
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Best = core.ToString(p)
		})
		require.Equal(t, output[i].Best, core.ToString(p), fmt.Sprintf("input: %s", tt))
	}
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
	planSuiteData := core.GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	p := parser.New()
	is := infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable()})
	for i, tt := range input {
		comment := fmt.Sprintf("input: %s", tt)
		stmt, err := p.ParseOneStmt(tt, "", "")
		require.NoError(t, err, comment)
		sc := tk.Session().GetSessionVars().StmtCtx
		sc.IgnoreTruncate = false
		p, _, err := planner.Optimize(context.TODO(), tk.Session(), stmt, is)
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
	planSuiteData := core.GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	p := parser.New()
	is := infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable()})
	for i, tt := range input {
		comment := fmt.Sprintf("input: %s", tt)
		stmt, err := p.ParseOneStmt(tt, "", "")
		require.NoError(t, err, comment)
		sc := tk.Session().GetSessionVars().StmtCtx
		sc.IgnoreTruncate = false
		p, _, err := planner.Optimize(context.TODO(), tk.Session(), stmt, is)
		require.NoError(t, err)
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Best = core.ToString(p)
		})
		require.Equal(t, output[i].Best, core.ToString(p), fmt.Sprintf("input: %s", tt))
	}
}

func TestINMJHint(t *testing.T) {
	var (
		input  []string
		output []struct {
			SQL    string
			Plan   []string
			Result []string
		}
	)
	planSuiteData := core.GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int primary key, b int not null)")
	tk.MustExec("create table t2(a int primary key, b int not null)")
	tk.MustExec("insert into t1 values(1,1),(2,2)")
	tk.MustExec("insert into t2 values(1,1),(2,1)")

	for i, ts := range input {
		testdata.OnRecord(func() {
			output[i].SQL = ts
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + ts).Rows())
			output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(ts).Sort().Rows())
		})
		tk.MustQuery("explain format = 'brief' " + ts).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(ts).Sort().Check(testkit.Rows(output[i].Result...))
	}
}

func TestEliminateMaxOneRow(t *testing.T) {
	var (
		input  []string
		output []struct {
			SQL    string
			Plan   []string
			Result []string
		}
	)
	planSuiteData := core.GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("drop table if exists t2;")
	tk.MustExec("drop table if exists t3;")
	tk.MustExec("create table t1(a int(11) DEFAULT NULL, b int(11) DEFAULT NULL, UNIQUE KEY idx_a (a))")
	tk.MustExec("create table t2(a int(11) DEFAULT NULL, b int(11) DEFAULT NULL)")
	tk.MustExec("create table t3(a int(11) DEFAULT NULL, b int(11) DEFAULT NULL, c int(11) DEFAULT NULL, UNIQUE KEY idx_abc (a, b, c))")

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

type overrideStore struct{ kv.Storage }

func (store overrideStore) GetClient() kv.Client {
	cli := store.Storage.GetClient()
	return overrideClient{cli}
}

type overrideClient struct{ kv.Client }

func (cli overrideClient) IsRequestTypeSupported(_, _ int64) bool {
	return false
}

func TestRequestTypeSupportedOff(t *testing.T) {
	store := testkit.CreateMockStore(t)
	se, err := session.CreateSession4Test(overrideStore{store})
	require.NoError(t, err)
	_, err = se.Execute(context.Background(), "use test")
	require.NoError(t, err)

	sql := "select * from t where a in (1, 10, 20)"
	expect := "TableReader(Table(t))->Sel([in(test.t.a, 1, 10, 20)])"

	is := infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable()})
	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	require.NoError(t, err)
	p, _, err := planner.Optimize(context.TODO(), se, stmt, is)
	require.NoError(t, err)
	require.Equal(t, expect, core.ToString(p), fmt.Sprintf("sql: %s", sql))
}

func TestIndexJoinUnionScan(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("create table t (a int primary key, b int, index idx(a))")
	tk.MustExec("create table tt (a int primary key) partition by range (a) (partition p0 values less than (100), partition p1 values less than (200))")
	tk.MustExec(`set @@tidb_partition_prune_mode='` + string(variable.Static) + `'`)

	var input [][]string
	var output []struct {
		SQL  []string
		Plan []string
	}
	planSuiteData := core.GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)

	for i, ts := range input {
		tk.MustExec("begin")
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
		tk.MustExec("rollback")
	}
}

func TestMergeJoinUnionScan(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1  (c_int int, c_str varchar(40), primary key (c_int))")
	tk.MustExec("create table t2  (c_int int, c_str varchar(40), primary key (c_int))")
	tk.MustExec("insert into t1 (`c_int`, `c_str`) values (11, 'keen williamson'), (10, 'gracious hermann')")
	tk.MustExec("insert into t2 (`c_int`, `c_str`) values (10, 'gracious hermann')")

	var input [][]string
	var output []struct {
		SQL  []string
		Plan []string
	}
	planSuiteData := core.GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)

	for i, ts := range input {
		tk.MustExec("begin")
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
		tk.MustExec("rollback")
	}
}

func TestDoSubQuery(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tests := []struct {
		sql  string
		best string
	}{
		{
			sql:  "do 1 in (select a from t)",
			best: "LeftHashJoin{Dual->PointGet(Handle(t.a)1)}->Projection",
		},
	}

	p := parser.New()
	is := infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable()})

	for _, tt := range tests {
		comment := fmt.Sprintf("for %s", tt.sql)
		stmt, err := p.ParseOneStmt(tt.sql, "", "")
		require.NoError(t, err, comment)
		p, _, err := planner.Optimize(context.TODO(), tk.Session(), stmt, is)
		require.NoError(t, err)
		require.Equal(t, tt.best, core.ToString(p), comment)
	}
}

func TestIndexLookupCartesianJoin(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	stmt, err := parser.New().ParseOneStmt("select /*+ TIDB_INLJ(t1, t2) */ * from t t1 join t t2", "", "")
	require.NoError(t, err)

	is := infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable()})
	p, _, err := planner.Optimize(context.TODO(), tk.Session(), stmt, is)
	require.NoError(t, err)
	require.Equal(t, "LeftHashJoin{TableReader(Table(t))->TableReader(Table(t))}", core.ToString(p))

	warnings := tk.Session().GetSessionVars().StmtCtx.GetWarnings()
	lastWarn := warnings[len(warnings)-1]
	err = core.ErrInternal.GenWithStack("TIDB_INLJ hint is inapplicable without column equal ON condition")
	require.True(t, terror.ErrorEqual(err, lastWarn.Err))
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
	planSuiteData := core.GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)

	p := parser.New()
	is := infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable()})
	for i, tt := range input {
		stmt, err := p.ParseOneStmt(tt, "", "")
		require.NoError(t, err)
		p, _, err := planner.Optimize(context.TODO(), tk.Session(), stmt, is)
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
	planSuiteData := core.GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	p := parser.New()
	is := infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable()})
	for i, test := range input {
		tk.Session().GetSessionVars().StmtCtx.SetWarnings(nil)
		stmt, err := p.ParseOneStmt(test, "", "")
		require.NoError(t, err)
		_, _, err = planner.Optimize(context.TODO(), tk.Session(), stmt, is)
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
			require.Equal(t, stmtctx.WarnLevelWarning, warnings[0].Level)
			require.Equal(t, output[i].Warning, warnings[0].Err.Error())
		}
	}
}

// withMockTiFlash sets the mockStore to have N TiFlash stores (naming as tiflash0, tiflash1, ...).
func withMockTiFlash(nodes int) mockstore.MockTiKVStoreOption {
	return mockstore.WithMultipleOptions(
		mockstore.WithClusterInspector(func(c testutils.Cluster) {
			mockCluster := c.(*unistore.Cluster)
			_, _, region1 := mockstore.BootstrapWithSingleStore(c)
			tiflashIdx := 0
			for tiflashIdx < nodes {
				store2 := c.AllocID()
				peer2 := c.AllocID()
				addr2 := fmt.Sprintf("tiflash%d", tiflashIdx)
				mockCluster.AddStore(store2, addr2, &metapb.StoreLabel{Key: "engine", Value: "tiflash"})
				mockCluster.AddPeer(region1, store2, peer2)
				tiflashIdx++
			}
		}),
		mockstore.WithStoreType(mockstore.EmbedUnistore),
	)
}

func TestMPPHints(t *testing.T) {
	store := testkit.CreateMockStore(t, withMockTiFlash(2))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("create table t (a int, b int, c int)")
	tk.MustExec("alter table t set tiflash replica 1")
	tk.MustExec("set @@session.tidb_allow_mpp=ON")
	tb := external.GetTableByName(t, tk, "test", "t")
	err := domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)

	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}

	planSuiteData := core.GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)

	for i, ts := range input {
		testdata.OnRecord(func() {
			output[i].SQL = ts
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + ts).Rows())
		})
		tk.MustQuery("explain format = 'brief' " + ts).Check(testkit.Rows(output[i].Plan...))
	}
}

func TestHintScope(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")

	var input []string
	var output []struct {
		SQL  string
		Best string
	}
	planSuiteData := core.GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	p := parser.New()
	is := infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable()})

	for i, test := range input {
		comment := fmt.Sprintf("case:%v sql:%s", i, test)
		stmt, err := p.ParseOneStmt(test, "", "")
		require.NoError(t, err, comment)

		p, _, err := planner.Optimize(context.Background(), tk.Session(), stmt, is)
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
	planSuiteData := core.GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	ctx := context.Background()
	p := parser.New()
	is := infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable()})

	for i, test := range input {
		comment := fmt.Sprintf("case:%v sql:%s", i, test)
		stmt, err := p.ParseOneStmt(test, "", "")
		require.NoError(t, err, comment)

		tk.Session().GetSessionVars().StmtCtx.SetWarnings(nil)
		p, _, err := planner.Optimize(ctx, tk.Session(), stmt, is)
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
			require.Equal(t, stmtctx.WarnLevelWarning, warnings[0].Level)
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
	planSuiteData := core.GetPlanSuiteData()
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

		p, _, err := planner.Optimize(ctx, tk.Session(), stmt, is)
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
			require.Equal(t, stmtctx.WarnLevelWarning, warnings[0].Level)
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
	planSuiteData := core.GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	ctx := context.Background()
	p := parser.New()
	is := infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable()})
	for i, test := range input {
		comment := fmt.Sprintf("case: %v sql: %v", i, test)
		tk.Session().GetSessionVars().StmtCtx.SetWarnings(nil)

		stmt, err := p.ParseOneStmt(test, "", "")
		require.NoError(t, err, comment)

		_, _, err = planner.Optimize(ctx, tk.Session(), stmt, is)
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
			require.Equal(t, stmtctx.WarnLevelWarning, warnings[0].Level)
			require.Equal(t, output[i].Warning, warnings[0].Err.Error())
		}
	}
}

func TestExplainJoinHints(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, c int, key(b), key(c))")
	tk.MustQuery("explain format='hint' select /*+ inl_merge_join(t2) */ * from t t1 inner join t t2 on t1.b = t2.b and t1.c = 1").Check(testkit.Rows(
		"inl_merge_join(@`sel_1` `test`.`t2`), use_index(@`sel_1` `test`.`t1` `c`), use_index(@`sel_1` `test`.`t2` `b`), inl_merge_join(`t2`)",
	))
	tk.MustQuery("explain format='hint' select /*+ inl_hash_join(t2) */ * from t t1 inner join t t2 on t1.b = t2.b and t1.c = 1").Check(testkit.Rows(
		"inl_hash_join(@`sel_1` `test`.`t2`), use_index(@`sel_1` `test`.`t1` `c`), use_index(@`sel_1` `test`.`t2` `b`), inl_hash_join(`t2`)",
	))
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
	planSuiteData := core.GetPlanSuiteData()
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

		p, _, err := planner.Optimize(ctx, tk.Session(), stmt, is)
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
			require.Equal(t, stmtctx.WarnLevelWarning, warnings[0].Level)
			require.Equal(t, output[i].Warning, warnings[0].Err.Error())
		}
	}
}

func TestLimitToCopHint(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("drop table if exists tn")
	tk.MustExec("create table tn(a int, b int, c int, d int, key (a, b, c, d))")
	tk.MustExec(`set tidb_opt_limit_push_down_threshold=0`)

	var (
		input  []string
		output []struct {
			SQL     string
			Plan    []string
			Warning []string
		}
	)

	planSuiteData := core.GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)

	for i, ts := range input {
		testdata.OnRecord(func() {
			output[i].SQL = ts
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + ts).Rows())
		})
		tk.MustQuery("explain format = 'brief' " + ts).Check(testkit.Rows(output[i].Plan...))

		comment := fmt.Sprintf("case:%v sql:%s", i, ts)
		warnings := tk.Session().GetSessionVars().StmtCtx.GetWarnings()
		testdata.OnRecord(func() {
			if len(warnings) > 0 {
				output[i].Warning = make([]string, len(warnings))
				for j, warning := range warnings {
					output[i].Warning[j] = warning.Err.Error()
				}
			}
		})
		if len(output[i].Warning) == 0 {
			require.Len(t, warnings, 0)
		} else {
			require.Len(t, warnings, len(output[i].Warning), comment)
			for j, warning := range warnings {
				require.Equal(t, stmtctx.WarnLevelWarning, warning.Level, comment)
				require.Equal(t, output[i].Warning[j], warning.Err.Error(), comment)
			}
		}
	}
}

func TestCTEMergeHint(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists tc")
	tk.MustExec("drop table if exists te")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("drop table if exists t3")
	tk.MustExec("drop table if exists t4")
	tk.MustExec("drop view if exists v")
	tk.MustExec("create table tc(a int)")
	tk.MustExec("create table te(c int)")
	tk.MustExec("create table t1(a int)")
	tk.MustExec("create table t2(b int)")
	tk.MustExec("create table t3(c int)")
	tk.MustExec("create table t4(d int)")
	tk.MustExec("insert into tc values (1), (5), (10), (15), (20), (30), (50);")
	tk.MustExec("insert into te values (1), (5), (10), (25), (40), (60), (100);")
	tk.MustExec("insert into t1 values (1), (5), (10), (25), (40), (60), (100);")
	tk.MustExec("insert into t2 values (1), (5), (10), (25), (40), (60), (100);")
	tk.MustExec("insert into t3 values (1), (5), (10), (25), (40), (60), (100);")
	tk.MustExec("insert into t4 values (1), (5), (10), (25), (40), (60), (100);")
	tk.MustExec("analyze table tc;")
	tk.MustExec("analyze table te;")
	tk.MustExec("analyze table t1;")
	tk.MustExec("analyze table t2;")
	tk.MustExec("analyze table t3;")
	tk.MustExec("analyze table t4;")
	tk.MustExec("create definer='root'@'localhost' view v as select * from tc")
	var (
		input  []string
		output []struct {
			SQL     string
			Plan    []string
			Warning []string
		}
	)

	planSuiteData := core.GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)

	for i, ts := range input {
		testdata.OnRecord(func() {
			output[i].SQL = ts
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + ts).Rows())
		})
		tk.MustQuery("explain format = 'brief' " + ts).Check(testkit.Rows(output[i].Plan...))

		comment := fmt.Sprintf("case:%v sql:%s", i, ts)
		warnings := tk.Session().GetSessionVars().StmtCtx.GetWarnings()
		testdata.OnRecord(func() {
			if len(warnings) > 0 {
				output[i].Warning = make([]string, len(warnings))
				for j, warning := range warnings {
					output[i].Warning[j] = warning.Err.Error()
				}
			}
		})
		if len(output[i].Warning) == 0 {
			require.Len(t, warnings, 0)
		} else {
			require.Len(t, warnings, len(output[i].Warning), comment)
			for j, warning := range warnings {
				require.Equal(t, stmtctx.WarnLevelWarning, warning.Level, comment)
				require.Equal(t, output[i].Warning[j], warning.Err.Error(), comment)
			}
		}
	}
}

func TestForceInlineCTE(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("CREATE TABLE `t` (`a` int(11));")
	tk.MustExec("insert into t values (1), (5), (10), (15), (20), (30), (50);")

	var (
		input  []string
		output []struct {
			SQL     string
			Plan    []string
			Warning []string
		}
	)
	planSuiteData := core.GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)

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
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format='brief' " + ts).Rows())
		})
		tk.MustQuery("explain format='brief' " + ts).Check(testkit.Rows(output[i].Plan...))

		comment := fmt.Sprintf("case:%v sql:%s", i, ts)
		warnings := tk.Session().GetSessionVars().StmtCtx.GetWarnings()
		testdata.OnRecord(func() {
			if len(warnings) > 0 {
				output[i].Warning = make([]string, len(warnings))
				for j, warning := range warnings {
					output[i].Warning[j] = warning.Err.Error()
				}
			}
		})
		if len(output[i].Warning) == 0 {
			require.Len(t, warnings, 0)
		} else {
			require.Len(t, warnings, len(output[i].Warning), comment)
			for j, warning := range warnings {
				require.Equal(t, stmtctx.WarnLevelWarning, warning.Level, comment)
				require.Equal(t, output[i].Warning[j], warning.Err.Error(), comment)
			}
		}
	}
}

func TestSingleConsumerCTE(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("CREATE TABLE `t` (`a` int(11));")
	tk.MustExec("insert into t values (1), (5), (10), (15), (20), (30), (50);")

	var (
		input  []string
		output []struct {
			SQL     string
			Plan    []string
			Warning []string
		}
	)
	planSuiteData := core.GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)

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
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format='brief' " + ts).Rows())
		})
		tk.MustQuery("explain format='brief' " + ts).Check(testkit.Rows(output[i].Plan...))
	}
}

func TestPushdownDistinctEnable(t *testing.T) {
	var (
		input  []string
		output []struct {
			SQL    string
			Plan   []string
			Result []string
		}
	)
	planSuiteData := core.GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	vars := []string{
		fmt.Sprintf("set @@session.%s = 1", variable.TiDBOptDistinctAggPushDown),
		"set session tidb_opt_agg_push_down = 1",
		"set tidb_cost_model_version = 2",
	}
	doTestPushdownDistinct(t, vars, input, output)
}

func TestPushdownDistinctDisable(t *testing.T) {
	var (
		input  []string
		output []struct {
			SQL    string
			Plan   []string
			Result []string
		}
	)

	planSuiteData := core.GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	vars := []string{
		fmt.Sprintf("set @@session.%s = 0", variable.TiDBOptDistinctAggPushDown),
		"set session tidb_opt_agg_push_down = 1",
	}
	doTestPushdownDistinct(t, vars, input, output)
}

func TestPushdownDistinctEnableAggPushDownDisable(t *testing.T) {
	var (
		input  []string
		output []struct {
			SQL    string
			Plan   []string
			Result []string
		}
	)
	planSuiteData := core.GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	vars := []string{
		fmt.Sprintf("set @@session.%s = 1", variable.TiDBOptDistinctAggPushDown),
		"set session tidb_opt_agg_push_down = 0",
		"set tidb_cost_model_version=2",
	}
	doTestPushdownDistinct(t, vars, input, output)
}

func doTestPushdownDistinct(t *testing.T, vars, input []string, output []struct {
	SQL    string
	Plan   []string
	Result []string
}) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, c int, index(c))")
	tk.MustExec("insert into t values (1, 1, 1), (1, 1, 3), (1, 2, 3), (2, 1, 3), (1, 2, NULL);")

	tk.MustExec("drop table if exists pt")
	tk.MustExec(`CREATE TABLE pt (a int, b int) PARTITION BY RANGE (a) (
		PARTITION p0 VALUES LESS THAN (2),
		PARTITION p1 VALUES LESS THAN (100)
	);`)

	tk.MustExec("drop table if exists tc;")
	tk.MustExec("CREATE TABLE `tc`(`timestamp` timestamp NULL DEFAULT NULL, KEY `idx_timestamp` (`timestamp`)) PARTITION BY RANGE ( UNIX_TIMESTAMP(`timestamp`) ) (PARTITION `p2020072312` VALUES LESS THAN (1595480400),PARTITION `p2020072313` VALUES LESS THAN (1595484000));")

	tk.MustExec("drop table if exists ta")
	tk.MustExec("create table ta(a int);")
	tk.MustExec("insert into ta values(1), (1);")
	tk.MustExec("drop table if exists tb")
	tk.MustExec("create table tb(a int);")
	tk.MustExec("insert into tb values(1), (1);")

	tk.MustExec("set session sql_mode=''")
	tk.MustExec(fmt.Sprintf("set session %s=1", variable.TiDBHashAggPartialConcurrency))
	tk.MustExec(fmt.Sprintf("set session %s=1", variable.TiDBHashAggFinalConcurrency))

	tk.MustExec(`set @@tidb_partition_prune_mode='` + string(variable.Static) + `'`)

	for _, v := range vars {
		tk.MustExec(v)
	}

	for i, ts := range input {
		testdata.OnRecord(func() {
			output[i].SQL = ts
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + ts).Rows())
			output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(ts).Sort().Rows())
		})
		tk.MustQuery("explain format = 'brief' " + ts).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(ts).Sort().Check(testkit.Rows(output[i].Result...))
	}
}

func TestGroupConcatOrderby(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/planner/core/forceDynamicPrune", `return(true)`)
	defer failpoint.Disable("github.com/pingcap/tidb/planner/core/forceDynamicPrune")
	var (
		input  []string
		output []struct {
			SQL    string
			Plan   []string
			Result []string
		}
	)
	planSuiteData := core.GetPlanSuiteData()
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

func TestHintAlias(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tests := []struct {
		sql1 string
		sql2 string
	}{
		{
			sql1: "select /*+ TIDB_SMJ(t1) */ t1.a, t1.b from t t1, (select /*+ TIDB_INLJ(t3) */ t2.a from t t2, t t3 where t2.a = t3.c) s where t1.a=s.a",
			sql2: "select /*+ MERGE_JOIN(t1) */ t1.a, t1.b from t t1, (select /*+ INL_JOIN(t3) */ t2.a from t t2, t t3 where t2.a = t3.c) s where t1.a=s.a",
		},
		{
			sql1: "select /*+ TIDB_HJ(t1) */ t1.a, t1.b from t t1, (select /*+ TIDB_SMJ(t2) */ t2.a from t t2, t t3 where t2.a = t3.c) s where t1.a=s.a",
			sql2: "select /*+ HASH_JOIN(t1) */ t1.a, t1.b from t t1, (select /*+ MERGE_JOIN(t2) */ t2.a from t t2, t t3 where t2.a = t3.c) s where t1.a=s.a",
		},
		{
			sql1: "select /*+ TIDB_INLJ(t1) */ t1.a, t1.b from t t1, (select /*+ TIDB_HJ(t2) */ t2.a from t t2, t t3 where t2.a = t3.c) s where t1.a=s.a",
			sql2: "select /*+ INL_JOIN(t1) */ t1.a, t1.b from t t1, (select /*+ HASH_JOIN(t2) */ t2.a from t t2, t t3 where t2.a = t3.c) s where t1.a=s.a",
		},
	}
	ctx := context.TODO()
	p := parser.New()
	is := infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable()})

	for i, tt := range tests {
		comment := fmt.Sprintf("case:%v sql1:%s sql2:%s", i, tt.sql1, tt.sql2)
		stmt1, err := p.ParseOneStmt(tt.sql1, "", "")
		require.NoError(t, err, comment)
		stmt2, err := p.ParseOneStmt(tt.sql2, "", "")
		require.NoError(t, err, comment)

		p1, _, err := planner.Optimize(ctx, tk.Session(), stmt1, is)
		require.NoError(t, err)
		p2, _, err := planner.Optimize(ctx, tk.Session(), stmt2, is)
		require.NoError(t, err)

		require.Equal(t, core.ToString(p2), core.ToString(p1))
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
	planSuiteData := core.GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	ctx := context.Background()
	p := parser.New()
	is := infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable()})

	for i, test := range input {
		comment := fmt.Sprintf("case:%v sql:%s", i, test)
		tk.Session().GetSessionVars().StmtCtx.SetWarnings(nil)

		stmt, err := p.ParseOneStmt(test, "", "")
		require.NoError(t, err, comment)

		p, _, err := planner.Optimize(ctx, tk.Session(), stmt, is)
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
	planSuiteData := core.GetPlanSuiteData()
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
		p, _, err := planner.Optimize(ctx, tk.Session(), stmt, is)
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
	planSuiteData := core.GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	ctx := context.TODO()
	p := parser.New()
	is := infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable()})

	for i, tt := range input {
		comment := fmt.Sprintf("case:%v sql: %s", i, tt)
		stmt, err := p.ParseOneStmt(tt, "", "")
		require.NoError(t, err, comment)

		p, _, err := planner.Optimize(ctx, tk.Session(), stmt, is)
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
	planSuiteData := core.GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)

	ctx := context.Background()
	p := parser.New()

	for i, tt := range input {
		comment := fmt.Sprintf("case:%v sql: %s", i, tt)
		stmt, err := p.ParseOneStmt(tt, "", "")
		require.NoError(t, err, comment)

		p, _, err := planner.Optimize(ctx, tk.Session(), stmt, is)
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

func TestDAGPlanBuilderSplitAvg(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tests := []struct {
		sql  string
		plan string
	}{
		{
			sql:  "select avg(a),avg(b),avg(c) from t",
			plan: "TableReader(Table(t)->HashAgg)->HashAgg",
		},
		{
			sql:  "select /*+ HASH_AGG() */ avg(a),avg(b),avg(c) from t",
			plan: "TableReader(Table(t)->HashAgg)->HashAgg",
		},
	}

	p := parser.New()
	is := infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable()})

	for _, tt := range tests {
		comment := fmt.Sprintf("for %s", tt.sql)
		stmt, err := p.ParseOneStmt(tt.sql, "", "")
		require.NoError(t, err, comment)

		err = core.Preprocess(context.Background(), tk.Session(), stmt, core.WithPreprocessorReturn(&core.PreprocessorReturn{InfoSchema: is}))
		require.NoError(t, err)
		p, _, err := planner.Optimize(context.TODO(), tk.Session(), stmt, is)
		require.NoError(t, err, comment)

		require.Equal(t, tt.plan, core.ToString(p), comment)
		root, ok := p.(core.PhysicalPlan)
		if !ok {
			continue
		}
		testDAGPlanBuilderSplitAvg(t, root)
	}
}

func testDAGPlanBuilderSplitAvg(t *testing.T, root core.PhysicalPlan) {
	if p, ok := root.(*core.PhysicalTableReader); ok {
		if p.TablePlans != nil {
			baseAgg := p.TablePlans[len(p.TablePlans)-1]
			if agg, ok := baseAgg.(*core.PhysicalHashAgg); ok {
				for i, aggfunc := range agg.AggFuncs {
					require.Equal(t, aggfunc.RetTp, agg.Schema().Columns[i].RetType)
				}
			}
			if agg, ok := baseAgg.(*core.PhysicalStreamAgg); ok {
				for i, aggfunc := range agg.AggFuncs {
					require.Equal(t, aggfunc.RetTp, agg.Schema().Columns[i].RetType)
				}
			}
		}
	}

	childs := root.Children()
	if childs == nil {
		return
	}
	for _, son := range childs {
		testDAGPlanBuilderSplitAvg(t, son)
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
		SQL  string
		Plan string
	}

	is := domain.GetDomain(tk.Session()).InfoSchema()
	p := parser.New()
	ctx := context.Background()

	planSuiteData := core.GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		comment := fmt.Sprintf("case:%v sql: %s", i, tt)
		stmt, err := p.ParseOneStmt(tt, "", "")
		require.NoError(t, err, comment)
		p, _, err := planner.Optimize(ctx, tk.Session(), stmt, is)
		require.NoError(t, err, comment)
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = core.ToString(p)
		})
		require.Equal(t, output[i].Plan, core.ToString(p), comment)
	}
}

func TestDAGPlanBuilderWindow(t *testing.T) {
	var input []string
	var output []struct {
		SQL  string
		Best string
	}
	planSuiteData := core.GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	vars := []string{
		"set @@session.tidb_window_concurrency = 1",
	}
	doTestDAGPlanBuilderWindow(t, vars, input, output)
}

func TestDAGPlanBuilderWindowParallel(t *testing.T) {
	var input []string
	var output []struct {
		SQL  string
		Best string
	}
	planSuiteData := core.GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	vars := []string{
		"set @@session.tidb_window_concurrency = 4",
	}
	doTestDAGPlanBuilderWindow(t, vars, input, output)
}

func TestTopNPushDownEmpty(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, c int, index idx_a(a))")
	tk.MustQuery("select extract(day_hour from 'ziy') as res from t order by res limit 1").Check(testkit.Rows())
}

func doTestDAGPlanBuilderWindow(t *testing.T, vars, input []string, output []struct {
	SQL  string
	Best string
}) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	for _, v := range vars {
		tk.MustExec(v)
	}

	p := parser.New()
	is := infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable()})

	for i, tt := range input {
		comment := fmt.Sprintf("case:%v sql:%s", i, tt)
		stmt, err := p.ParseOneStmt(tt, "", "")
		require.NoError(t, err, comment)

		err = sessiontxn.NewTxn(context.Background(), tk.Session())
		require.NoError(t, err)
		p, _, err := planner.Optimize(context.TODO(), tk.Session(), stmt, is)
		require.NoError(t, err)
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Best = core.ToString(p)
		})
		require.Equal(t, output[i].Best, core.ToString(p), comment)
	}
}

func TestNominalSort(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	var input []string
	var output []struct {
		SQL    string
		Plan   []string
		Result []string
	}
	tk.MustExec("create table t (a int, b int, index idx_a(a), index idx_b(b))")
	tk.MustExec("insert into t values(1, 1)")
	tk.MustExec("insert into t values(1, 2)")
	tk.MustExec("insert into t values(2, 4)")
	tk.MustExec("insert into t values(3, 5)")
	planSuiteData := core.GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	for i, ts := range input {
		testdata.OnRecord(func() {
			output[i].SQL = ts
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + ts).Rows())
			output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(ts).Rows())
		})
		tk.MustQuery("explain format = 'brief' " + ts).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(ts).Check(testkit.Rows(output[i].Result...))
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

	planSuiteData := core.GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		comment := fmt.Sprintf("case:%v sql: %s", i, tt)
		stmt, err := p.ParseOneStmt(tt, "", "")
		require.NoError(t, err, comment)
		p, _, err := planner.Optimize(ctx, tk.Session(), stmt, is)
		require.NoError(t, err, comment)
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = core.ToString(p)
		})
		require.Equal(t, output[i].Plan, core.ToString(p), comment)
	}
}

func TestNthPlanHintWithExplain(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists test.tt`)
	tk.MustExec(`create table test.tt (a int,b int, index(a), index(b));`)
	tk.MustExec("insert into tt values (1, 1), (2, 2), (3, 4)")
	tk.MustExec(`set @@tidb_partition_prune_mode='` + string(variable.Static) + `'`)

	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	planSuiteData := core.GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	for i, ts := range input {
		testdata.OnRecord(func() {
			output[i].SQL = ts
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + ts).Rows())
		})
		tk.MustQuery("explain format = 'brief' " + ts).Check(testkit.Rows(output[i].Plan...))
	}

	// This assertion makes sure a query with or without nth_plan() hint output exactly the same plan(including plan ID).
	// The query below is the same as queries in the testdata except for nth_plan() hint.
	// Currently, its output is the same as the second test case in the testdata, which is `output[1]`. If this doesn't
	// hold in the future, you may need to modify this.
	tk.MustQuery("explain format = 'brief' select * from test.tt where a=1 and b=1").Check(testkit.Rows(output[1].Plan...))
}

func TestEnumIndex(t *testing.T) {
	var (
		input  []string
		output []struct {
			SQL    string
			Plan   []string
			Result []string
		}
	)
	planSuiteData := core.GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(e enum('c','b','a',''), index idx(e))")
	tk.MustExec("insert ignore into t values(0),(1),(2),(3),(4);")

	for i, ts := range input {
		testdata.OnRecord(func() {
			output[i].SQL = ts
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format='brief'" + ts).Rows())
			output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(ts).Sort().Rows())
		})
		tk.MustQuery("explain format='brief' " + ts).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(ts).Sort().Check(testkit.Rows(output[i].Result...))
	}
}

func TestIssue27233(t *testing.T) {
	var (
		input  []string
		output []struct {
			SQL    string
			Plan   []string
			Result []string
		}
	)
	planSuiteData := core.GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE `PK_S_MULTI_31` (\n  `COL1` tinyint(45) NOT NULL,\n  `COL2` tinyint(45) NOT NULL,\n  PRIMARY KEY (`COL1`,`COL2`) /*T![clustered_index] NONCLUSTERED */\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;")
	tk.MustExec("insert into PK_S_MULTI_31 values(122,100),(124,-22),(124,34),(127,103);")

	for i, ts := range input {
		testdata.OnRecord(func() {
			output[i].SQL = ts
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format='brief'" + ts).Rows())
			output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(ts).Sort().Rows())
		})
		tk.MustQuery("explain format='brief' " + ts).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(ts).Sort().Check(testkit.Rows(output[i].Result...))
	}
}

func TestPossibleProperties(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists student, sc")
	tk.MustExec("create table student(id int primary key auto_increment, name varchar(4) not null)")
	tk.MustExec("create table sc(id int primary key auto_increment, student_id int not null, course_id int not null, score int not null)")
	tk.MustExec("insert into student values (1,'s1'), (2,'s2')")
	tk.MustExec("insert into sc (student_id, course_id, score) values (1,1,59), (1,2,57), (1,3,76), (2,1,99), (2,2,100), (2,3,100)")
	tk.MustQuery("select /*+ stream_agg() */ a.id, avg(b.score) as afs from student a join sc b on a.id = b.student_id where b.score < 60 group by a.id having count(b.course_id) >= 2").Check(testkit.Rows(
		"1 58.0000",
	))
}

func TestSelectionPartialPushDown(t *testing.T) {
	var (
		input  []string
		output []struct {
			SQL  string
			Plan []string
		}
	)
	planSuiteData := core.GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int, b int as (a+1) virtual)")
	tk.MustExec("create table t2(a int, b int as (a+1) virtual, c int, key idx_a(a))")

	for i, ts := range input {
		testdata.OnRecord(func() {
			output[i].SQL = ts
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format='brief'" + ts).Rows())
		})
		tk.MustQuery("explain format='brief' " + ts).Check(testkit.Rows(output[i].Plan...))
	}
}

func TestIssue28316(t *testing.T) {
	var (
		input  []string
		output []struct {
			SQL  string
			Plan []string
		}
	)
	planSuiteData := core.GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")

	for i, ts := range input {
		testdata.OnRecord(func() {
			output[i].SQL = ts
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format='brief'" + ts).Rows())
		})
		tk.MustQuery("explain format='brief' " + ts).Check(testkit.Rows(output[i].Plan...))
	}
}

func TestIssue30965(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t30965")
	tk.MustExec("CREATE TABLE `t30965` ( `a` int(11) DEFAULT NULL, `b` int(11) DEFAULT NULL, `c` int(11) DEFAULT NULL, `d` int(11) GENERATED ALWAYS AS (`a` + 1) VIRTUAL, KEY `ib` (`b`));")
	tk.MustExec("insert into t30965 (a,b,c) value(3,4,5);")
	tk.MustQuery("select count(*) from t30965 where d = 2 and b = 4 and a = 3 and c = 5;").Check(testkit.Rows("0"))
	tk.MustQuery("explain format = 'brief' select count(*) from t30965 where d = 2 and b = 4 and a = 3 and c = 5;").Check(
		testkit.Rows(
			"StreamAgg 1.00 root  funcs:count(1)->Column#6",
			"└─Selection 0.00 root  eq(test.t30965.d, 2)",
			"  └─IndexLookUp 0.00 root  ",
			"    ├─IndexRangeScan(Build) 10.00 cop[tikv] table:t30965, index:ib(b) range:[4,4], keep order:false, stats:pseudo",
			"    └─Selection(Probe) 0.00 cop[tikv]  eq(test.t30965.a, 3), eq(test.t30965.c, 5)",
			"      └─TableRowIDScan 10.00 cop[tikv] table:t30965 keep order:false, stats:pseudo"))
}

func TestSkewDistinctAgg(t *testing.T) {
	var (
		input  []string
		output []struct {
			SQL  string
			Plan []string
		}
	)
	planSuiteData := core.GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE `t` (`a` int(11), `b` int(11), `c` int(11), `d` date)")
	tk.MustExec("insert into t (a,b,c,d) value(1,4,5,'2019-06-01')")
	tk.MustExec("insert into t (a,b,c,d) value(2,null,1,'2019-07-01')")
	tk.MustExec("insert into t (a,b,c,d) value(3,4,5,'2019-08-01')")
	tk.MustExec("insert into t (a,b,c,d) value(3,6,2,'2019-09-01')")
	tk.MustExec("insert into t (a,b,c,d) value(10,4,null,'2020-06-01')")
	tk.MustExec("insert into t (a,b,c,d) value(20,null,1,'2020-07-01')")
	tk.MustExec("insert into t (a,b,c,d) value(30,4,5,'2020-08-01')")
	tk.MustExec("insert into t (a,b,c,d) value(30,6,5,'2020-09-01')")
	tk.MustQuery("select date_format(d,'%Y') as df, sum(a), count(b), count(distinct c) " +
		"from t group by date_format(d,'%Y') order by df;").Check(
		testkit.Rows("2019 9 3 3", "2020 90 3 2"))
	tk.MustExec("set @@tidb_opt_skew_distinct_agg=1")
	tk.MustQuery("select date_format(d,'%Y') as df, sum(a), count(b), count(distinct c) " +
		"from t group by date_format(d,'%Y') order by df;").Check(
		testkit.Rows("2019 9 3 3", "2020 90 3 2"))
	tk.MustQuery("select count(distinct b), sum(c) from t group by a order by 1,2;").Check(
		testkit.Rows("0 1", "0 1", "1 <nil>", "1 5", "2 7", "2 10"))
	tk.MustQuery("select count(distinct b) from t group by date_format(d,'%Y') order by 1;").Check(
		testkit.Rows("2", "2"))
	tk.MustQuery("select count(a), count(distinct b), max(b) from t group by date_format(d,'%Y') order by 1,2,3;").Check(
		testkit.Rows("4 2 6", "4 2 6"))
	tk.MustQuery("select count(a), count(distinct b), max(b) from t group by date_format(d,'%Y'),c order by 1,2,3;").Check(
		testkit.Rows("1 0 <nil>", "1 0 <nil>", "1 1 4", "1 1 6", "2 1 4", "2 2 6"))
	tk.MustQuery("select avg(distinct b), count(a), sum(b) from t group by date_format(d,'%Y'),c order by 1,2,3;").Check(
		testkit.Rows("<nil> 1 <nil>", "<nil> 1 <nil>", "4.0000 1 4", "4.0000 2 8", "5.0000 2 10", "6.0000 1 6"))

	for i, ts := range input {
		testdata.OnRecord(func() {
			output[i].SQL = ts
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format='brief' " + ts).Rows())
		})
		tk.MustQuery("explain format='brief' " + ts).Check(testkit.Rows(output[i].Plan...))
	}
}

func TestHJBuildAndProbeHint(t *testing.T) {
	var (
		input  []string
		output []struct {
			SQL     string
			Plan    []string
			Result  []string
			Warning []string
		}
	)
	planSuiteData := core.GetPlanSuiteData()
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

	for i, ts := range input {
		testdata.OnRecord(func() {
			output[i].SQL = ts
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + ts).Rows())
			output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(ts).Sort().Rows())
			output[i].Warning = testdata.ConvertRowsToStrings(tk.MustQuery("show warnings").Rows())
		})
		tk.MustQuery("explain format = 'brief' " + ts).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(ts).Sort().Check(testkit.Rows(output[i].Result...))
		tk.MustQuery("show warnings").Check(testkit.Rows(output[i].Warning...))
	}
}

func TestHJBuildAndProbeHint4StaticPartitionTable(t *testing.T) {
	var (
		input  []string
		output []struct {
			SQL     string
			Plan    []string
			Result  []string
			Warning []string
		}
	)
	planSuiteData := core.GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("drop table if exists t1, t2, t3")
	tk.MustExec(`create table t1(a int, b int) partition by hash(a) partitions 4`)
	tk.MustExec(`create table t2(a int, b int) partition by hash(a) partitions 5`)
	tk.MustExec(`create table t3(a int, b int) partition by hash(b) partitions 3`)
	tk.MustExec("insert into t1 values(1,1),(2,2)")
	tk.MustExec("insert into t2 values(1,1),(2,1)")
	tk.MustExec("insert into t3 values(1,1),(2,1)")
	tk.MustExec(`set @@tidb_partition_prune_mode="static"`)

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

func TestHJBuildAndProbeHint4DynamicPartitionTable(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/planner/core/forceDynamicPrune", `return(true)`)
	defer failpoint.Disable("github.com/pingcap/tidb/planner/core/forceDynamicPrune")

	var (
		input  []string
		output []struct {
			SQL     string
			Plan    []string
			Result  []string
			Warning []string
		}
	)
	planSuiteData := core.GetPlanSuiteData()
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
	planSuiteData := core.GetPlanSuiteData()
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
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	require.True(t, exists)
	for _, tblInfo := range db.Tables {
		tableName := tblInfo.Name.L
		if tableName == "t1" || tableName == "t2" || tableName == "t3" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

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

func TestHJBuildAndProbeHintWithBinding(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("drop table if exists t, t1, t2, t3;")
	tk.MustExec("create table t(a int, b int, key(a));")
	tk.MustExec("create table t1(a int, b int, key(a));")
	tk.MustExec("create table t2(a int, b int, key(a));")
	tk.MustExec("create table t3(a int, b int, key(a));")

	tk.MustExec("select * from t1 join t2 on t1.a=t2.a join t3 on t2.b=t3.b")
	tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("0"))
	tk.MustExec("create global binding for select * from t1 join t2 on t1.a=t2.a join t3 on t2.b=t3.b using select /*+ hash_join_build(t1) */ * from t1 join t2 on t1.a=t2.a join t3 on t2.b=t3.b")
	tk.MustExec("select * from t1 join t2 on t1.a=t2.a join t3 on t2.b=t3.b")
	tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("1"))
	res := tk.MustQuery("show global bindings").Rows()
	require.Equal(t, res[0][0], "select * from ( `test` . `t1` join `test` . `t2` on `t1` . `a` = `t2` . `a` ) join `test` . `t3` on `t2` . `b` = `t3` . `b`", "SELECT /*+ hash_join_build(t1)*/ * FROM (`test`.`t1` JOIN `test`.`t2` ON `t1`.`a` = `t2`.`a`) JOIN `test`.`t3` ON `t2`.`b` = `t3`.`b`")

	tk.MustExec("create global binding for select * from t1 join t2 on t1.a=t2.a join t3 on t2.b=t3.b using select /*+ hash_join_probe(t1) */ * from t1 join t2 on t1.a=t2.a join t3 on t2.b=t3.b")
	tk.MustExec("select * from t1 join t2 on t1.a=t2.a join t3 on t2.b=t3.b")
	tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("1"))
	res = tk.MustQuery("show global bindings").Rows()
	require.Equal(t, res[0][0], "select * from ( `test` . `t1` join `test` . `t2` on `t1` . `a` = `t2` . `a` ) join `test` . `t3` on `t2` . `b` = `t3` . `b`", "SELECT /*+ hash_join_probe(t1)*/ * FROM (`test`.`t1` JOIN `test`.`t2` ON `t1`.`a` = `t2`.`a`) JOIN `test`.`t3` ON `t2`.`b` = `t3`.`b`")

	tk.MustExec("drop global binding for select * from t1 join t2 on t1.a=t2.a join t3 on t2.b=t3.b")
	tk.MustExec("select * from t1 join t2 on t1.a=t2.a join t3 on t2.b=t3.b")
	tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("0"))
	res = tk.MustQuery("show global bindings").Rows()
	require.Equal(t, len(res), 0)
}

func TestMPPSinglePartitionType(t *testing.T) {
	var (
		input  []string
		output []struct {
			SQL  string
			Plan []string
		}
	)
	planSuiteData := core.GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("drop table if exists employee")
	tk.MustExec("create table employee(empid int, deptid int, salary decimal(10,2))")
	tk.MustExec("set tidb_enforce_mpp=0")

	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	require.True(t, exists)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "employee" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

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

func TestPhysicalPlanMemoryTrace(t *testing.T) {
	// PhysicalSort
	ls := core.PhysicalSort{}
	size := ls.MemoryUsage()
	ls.ByItems = append(ls.ByItems, &util.ByItems{})
	require.Greater(t, ls.MemoryUsage(), size)

	// PhysicalProperty
	pp := property.PhysicalProperty{}
	size = pp.MemoryUsage()
	pp.MPPPartitionCols = append(pp.MPPPartitionCols, &property.MPPPartitionColumn{})
	require.Greater(t, pp.MemoryUsage(), size)
}

func TestNoDecorrelateHint(t *testing.T) {
	var (
		input  []string
		output []struct {
			SQL     string
			Plan    []string
			Result  []string
			Warning []string
		}
	)
	planSuiteData := core.GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int, b int)")
	tk.MustExec("create table t2(a int primary key, b int)")
	tk.MustExec("create table t3(a int, b int)")
	tk.MustExec("insert into t1 values(1,1),(2,2)")
	tk.MustExec("insert into t2 values(1,1),(2,1)")
	tk.MustExec("insert into t3 values(1,1),(2,1)")

	tk.MustExec("create table ta(id int, code int, name varchar(20), index idx_ta_id(id),index idx_ta_name(name), index idx_ta_code(code))")
	tk.MustExec("create table tb(id int, code int, name varchar(20), index idx_tb_id(id),index idx_tb_name(name))")
	tk.MustExec("create table tc(id int, code int, name varchar(20), index idx_tc_id(id),index idx_tc_name(name))")
	tk.MustExec("create table td(id int, code int, name varchar(20), index idx_tc_id(id),index idx_tc_name(name))")

	for i, ts := range input {
		testdata.OnRecord(func() {
			output[i].SQL = ts
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + ts).Rows())
			output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(ts).Sort().Rows())
			output[i].Warning = testdata.ConvertRowsToStrings(tk.MustQuery("show warnings").Rows())
		})
		tk.MustQuery("explain format = 'brief' " + ts).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(ts).Sort().Check(testkit.Rows(output[i].Result...))
		tk.MustQuery("show warnings").Check(testkit.Rows(output[i].Warning...))
	}
}

func TestCountStarForTikv(t *testing.T) {
	var (
		input  []string
		output []struct {
			SQL     string
			Plan    []string
			Warning []string
		}
	)
	planSuiteData := core.GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=1")
	tk.MustExec("create table t (a int(11) not null, b varchar(10) not null, c date not null, d char(1) not null, e bigint not null, f datetime not null, g bool not null, h bool )")
	tk.MustExec("create table t_pick_row_id (a char(20) not null)")

	// tikv
	for i, ts := range input {
		testdata.OnRecord(func() {
			output[i].SQL = ts
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + ts).Rows())
		})
		tk.MustQuery("explain format = 'brief' " + ts).Check(testkit.Rows(output[i].Plan...))
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
	planSuiteData := core.GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=1")
	tk.MustExec("create table t (a int(11) not null, b varchar(10) not null, c date not null, d char(1) not null, e bigint not null, f datetime not null, g bool not null, h bool )")
	tk.MustExec("create table t_pick_row_id (a char(20) not null)")

	// tiflash
	dom := domain.GetDomain(tk.Session())
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	require.True(t, exists)
	for _, tblInfo := range db.Tables {
		tableName := tblInfo.Name.L
		if tableName == "t" || tableName == "t_pick_row_id" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	tk.MustExec("set @@tidb_allow_mpp=1; set @@tidb_enforce_mpp=1;")
	for i, ts := range input {
		testdata.OnRecord(func() {
			output[i].SQL = ts
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + ts).Rows())
		})
		tk.MustQuery("explain format = 'brief' " + ts).Check(testkit.Rows(output[i].Plan...))
	}
}
