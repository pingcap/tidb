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

	. "github.com/pingcap/check"
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
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/hint"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/testutil"
)

var _ = Suite(&testPlanSuite{})
var _ = SerialSuites(&testPlanSerialSuite{})

type testPlanSuiteBase struct {
	*parser.Parser
	is infoschema.InfoSchema
}

func (s *testPlanSuiteBase) SetUpSuite(c *C) {
	s.is = infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable()})
	s.Parser = parser.New()
	s.Parser.SetParserConfig(parser.ParserConfig{EnableWindowFunction: true, EnableStrictDoubleTypeCheck: true})
}

type testPlanSerialSuite struct {
	testPlanSuiteBase
}

type testPlanSuite struct {
	testPlanSuiteBase

	testData testutil.TestData
}

func (s *testPlanSuite) SetUpSuite(c *C) {
	s.testPlanSuiteBase.SetUpSuite(c)

	var err error
	s.testData, err = testutil.LoadTestSuiteData("testdata", "plan_suite")
	c.Assert(err, IsNil)
}

func (s *testPlanSuite) TearDownSuite(c *C) {
	c.Assert(s.testData.GenerateOutputIfNeeded(), IsNil)
}

func (s *testPlanSuite) TestDAGPlanBuilderSimpleCase(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	se, err := session.CreateSession4Test(store)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "set tidb_opt_limit_push_down_threshold=0")
	c.Assert(err, IsNil)
	var input []string
	var output []struct {
		SQL  string
		Best string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		comment := Commentf("case:%v sql:%s", i, tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)

		err = se.NewTxn(context.Background())
		c.Assert(err, IsNil)
		p, _, err := planner.Optimize(context.TODO(), se, stmt, s.is)
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Best = core.ToString(p)
		})
		c.Assert(core.ToString(p), Equals, output[i].Best, comment)
	}
}

func (s *testPlanSuite) TestAnalyzeBuildSucc(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	se, err := session.CreateSession4Test(store)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)
	sctx := se.(sessionctx.Context)
	_, err = se.Execute(context.Background(), "create table t(a int)")
	c.Assert(err, IsNil)
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
	for i, tt := range tests {
		comment := Commentf("The %v-th test failed", i)
		_, err := se.Execute(context.Background(), fmt.Sprintf("set @@tidb_analyze_version=%v", tt.statsVer))
		c.Assert(err, IsNil)

		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		if tt.succ {
			c.Assert(err, IsNil, comment)
		} else if err != nil {
			continue
		}
		err = core.Preprocess(se, stmt, core.WithPreprocessorReturn(&core.PreprocessorReturn{InfoSchema: s.is}))
		c.Assert(err, IsNil)
		_, _, err = planner.Optimize(context.Background(), sctx, stmt, s.is)
		if tt.succ {
			c.Assert(err, IsNil, comment)
		} else {
			c.Assert(err, NotNil, comment)
		}
	}
}

func (s *testPlanSuite) TestAnalyzeSetRate(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	se, err := session.CreateSession4Test(store)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)
	sctx := se.(sessionctx.Context)
	_, err = se.Execute(context.Background(), "create table t(a int)")
	c.Assert(err, IsNil)
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
	for i, tt := range tests {
		comment := Commentf("The %v-th test failed", i)
		c.Assert(err, IsNil)

		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)
		err = core.Preprocess(se, stmt, core.WithPreprocessorReturn(&core.PreprocessorReturn{InfoSchema: s.is}))
		c.Assert(err, IsNil)
		p, _, err := planner.Optimize(context.Background(), sctx, stmt, s.is)
		c.Assert(err, IsNil, comment)
		ana := p.(*core.Analyze)
		c.Assert(math.Float64frombits(ana.Opts[ast.AnalyzeOptSampleRate]), Equals, tt.rate)
	}
}

func (s *testPlanSuite) TestDAGPlanBuilderJoin(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	se, err := session.CreateSession4Test(store)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)
	ctx := se.(sessionctx.Context)
	sessionVars := ctx.GetSessionVars()
	sessionVars.ExecutorConcurrency = 4
	sessionVars.SetDistSQLScanConcurrency(15)
	sessionVars.SetHashJoinConcurrency(5)

	var input []string
	var output []struct {
		SQL  string
		Best string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		comment := Commentf("case:%v sql:%s", i, tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)

		p, _, err := planner.Optimize(context.TODO(), se, stmt, s.is)
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Best = core.ToString(p)
		})
		c.Assert(core.ToString(p), Equals, output[i].Best, comment)
	}
}

func (s *testPlanSuite) TestDAGPlanBuilderSubquery(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	se, err := session.CreateSession4Test(store)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "set sql_mode='STRICT_TRANS_TABLES'")
	c.Assert(err, IsNil) // disable only full group by
	ctx := se.(sessionctx.Context)
	sessionVars := ctx.GetSessionVars()
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
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		comment := Commentf("for %s", tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)

		p, _, err := planner.Optimize(context.TODO(), se, stmt, s.is)
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Best = core.ToString(p)
		})
		c.Assert(core.ToString(p), Equals, output[i].Best, Commentf("for %s", tt))
	}
}

func (s *testPlanSuite) TestDAGPlanTopN(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	se, err := session.CreateSession4Test(store)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)

	var input []string
	var output []struct {
		SQL  string
		Best string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		comment := Commentf("case:%v sql:%s", i, tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)

		p, _, err := planner.Optimize(context.TODO(), se, stmt, s.is)
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Best = core.ToString(p)
		})
		c.Assert(core.ToString(p), Equals, output[i].Best, comment)
	}
}

func (s *testPlanSuite) TestDAGPlanBuilderBasePhysicalPlan(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	se, err := session.CreateSession4Test(store)
	c.Assert(err, IsNil)

	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)

	var input []string
	var output []struct {
		SQL   string
		Best  string
		Hints string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		comment := Commentf("for %s", tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)

		err = core.Preprocess(se, stmt, core.WithPreprocessorReturn(&core.PreprocessorReturn{InfoSchema: s.is}))
		c.Assert(err, IsNil)
		p, _, err := planner.Optimize(context.TODO(), se, stmt, s.is)
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Best = core.ToString(p)
			output[i].Hints = hint.RestoreOptimizerHints(core.GenHintsFromPhysicalPlan(p))
		})
		c.Assert(core.ToString(p), Equals, output[i].Best, Commentf("for %s", tt))
		c.Assert(hint.RestoreOptimizerHints(core.GenHintsFromPhysicalPlan(p)), Equals, output[i].Hints, Commentf("for %s", tt))
	}
}

func (s *testPlanSuite) TestDAGPlanBuilderUnion(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	se, err := session.CreateSession4Test(store)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)

	var input []string
	var output []struct {
		SQL  string
		Best string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		comment := Commentf("case:%v sql:%s", i, tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)

		p, _, err := planner.Optimize(context.TODO(), se, stmt, s.is)
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Best = core.ToString(p)
		})
		c.Assert(core.ToString(p), Equals, output[i].Best, comment)
	}
}

func (s *testPlanSuite) TestDAGPlanBuilderUnionScan(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	se, err := session.CreateSession4Test(store)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)

	var input []string
	var output []struct {
		SQL  string
		Best string
	}
	for i, tt := range input {
		comment := Commentf("for %s", tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)

		err = se.NewTxn(context.Background())
		c.Assert(err, IsNil)
		// Make txn not read only.
		txn, err := se.Txn(true)
		c.Assert(err, IsNil)
		err = txn.Set(kv.Key("AAA"), []byte("BBB"))
		c.Assert(err, IsNil)
		se.StmtCommit()
		p, _, err := planner.Optimize(context.TODO(), se, stmt, s.is)
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Best = core.ToString(p)
		})
		c.Assert(core.ToString(p), Equals, output[i].Best, Commentf("for %s", tt))
	}
}

func (s *testPlanSuite) TestDAGPlanBuilderAgg(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	se, err := session.CreateSession4Test(store)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "set sql_mode='STRICT_TRANS_TABLES'")
	c.Assert(err, IsNil) // disable only full group by
	ctx := se.(sessionctx.Context)
	sessionVars := ctx.GetSessionVars()
	sessionVars.SetHashAggFinalConcurrency(1)
	sessionVars.SetHashAggPartialConcurrency(1)
	sessionVars.SetDistSQLScanConcurrency(15)
	sessionVars.ExecutorConcurrency = 4

	var input []string
	var output []struct {
		SQL  string
		Best string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		comment := Commentf("for %s", tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)

		p, _, err := planner.Optimize(context.TODO(), se, stmt, s.is)
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Best = core.ToString(p)
		})
		c.Assert(core.ToString(p), Equals, output[i].Best, Commentf("for %s", tt))
	}
}

func (s *testPlanSuite) TestRefine(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	se, err := session.CreateSession4Test(store)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)

	var input []string
	var output []struct {
		SQL  string
		Best string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		comment := Commentf("for %s", tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)
		sc := se.(sessionctx.Context).GetSessionVars().StmtCtx
		sc.IgnoreTruncate = false
		p, _, err := planner.Optimize(context.TODO(), se, stmt, s.is)
		c.Assert(err, IsNil, comment)
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Best = core.ToString(p)
		})
		c.Assert(core.ToString(p), Equals, output[i].Best, comment)
	}
}

func (s *testPlanSuite) TestAggEliminator(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	se, err := session.CreateSession4Test(store)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "set tidb_opt_limit_push_down_threshold=0")
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "set sql_mode='STRICT_TRANS_TABLES'")
	c.Assert(err, IsNil) // disable only full group by
	var input []string
	var output []struct {
		SQL  string
		Best string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		comment := Commentf("for %s", tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)
		sc := se.(sessionctx.Context).GetSessionVars().StmtCtx
		sc.IgnoreTruncate = false
		p, _, err := planner.Optimize(context.TODO(), se, stmt, s.is)
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Best = core.ToString(p)
		})
		c.Assert(core.ToString(p), Equals, output[i].Best, Commentf("for %s", tt))
	}
}

func (s *testPlanSuite) TestINMJHint(c *C) {
	var (
		input  []string
		output []struct {
			SQL    string
			Plan   []string
			Result []string
		}
	)
	s.testData.GetTestCases(c, &input, &output)
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	tk := testkit.NewTestKit(c, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int primary key, b int not null)")
	tk.MustExec("create table t2(a int primary key, b int not null)")
	tk.MustExec("insert into t1 values(1,1),(2,2)")
	tk.MustExec("insert into t2 values(1,1),(2,1)")

	for i, ts := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = ts
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + ts).Rows())
			output[i].Result = s.testData.ConvertRowsToStrings(tk.MustQuery(ts).Sort().Rows())
		})
		tk.MustQuery("explain format = 'brief' " + ts).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(ts).Sort().Check(testkit.Rows(output[i].Result...))
	}
}

func (s *testPlanSuite) TestEliminateMaxOneRow(c *C) {
	var (
		input  []string
		output []struct {
			SQL    string
			Plan   []string
			Result []string
		}
	)
	s.testData.GetTestCases(c, &input, &output)
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	tk := testkit.NewTestKit(c, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("drop table if exists t2;")
	tk.MustExec("drop table if exists t3;")
	tk.MustExec("create table t1(a int(11) DEFAULT NULL, b int(11) DEFAULT NULL, UNIQUE KEY idx_a (a))")
	tk.MustExec("create table t2(a int(11) DEFAULT NULL, b int(11) DEFAULT NULL)")
	tk.MustExec("create table t3(a int(11) DEFAULT NULL, b int(11) DEFAULT NULL, c int(11) DEFAULT NULL, UNIQUE KEY idx_abc (a, b, c))")

	for i, ts := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = ts
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + ts).Rows())
			output[i].Result = s.testData.ConvertRowsToStrings(tk.MustQuery(ts).Sort().Rows())
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

func (cli overrideClient) IsRequestTypeSupported(reqType, subType int64) bool {
	return false
}

func (s *testPlanSuite) TestRequestTypeSupportedOff(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	se, err := session.CreateSession4Test(overrideStore{store})
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)

	sql := "select * from t where a in (1, 10, 20)"
	expect := "TableReader(Table(t))->Sel([in(test.t.a, 1, 10, 20)])"

	stmt, err := s.ParseOneStmt(sql, "", "")
	c.Assert(err, IsNil)
	p, _, err := planner.Optimize(context.TODO(), se, stmt, s.is)
	c.Assert(err, IsNil)
	c.Assert(core.ToString(p), Equals, expect, Commentf("for %s", sql))
}

func (s *testPlanSuite) TestIndexJoinUnionScan(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		store.Close()
	}()

	tk.MustExec("use test")
	var input [][]string
	var output []struct {
		SQL  []string
		Plan []string
	}
	tk.MustExec("create table t (a int primary key, b int, index idx(a))")
	tk.MustExec("create table tt (a int primary key) partition by range (a) (partition p0 values less than (100), partition p1 values less than (200))")

	tk.MustExec(`set @@tidb_partition_prune_mode='` + string(variable.Static) + `'`)

	s.testData.GetTestCases(c, &input, &output)
	for i, ts := range input {
		tk.MustExec("begin")
		for j, tt := range ts {
			if j != len(ts)-1 {
				tk.MustExec(tt)
			}
			s.testData.OnRecord(func() {
				output[i].SQL = ts
				if j == len(ts)-1 {
					output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
				}
			})
			if j == len(ts)-1 {
				tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
			}
		}
		tk.MustExec("rollback")
	}
}

func (s *testPlanSuite) TestMergeJoinUnionScan(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		store.Close()
	}()

	tk.MustExec("use test")
	var input [][]string
	var output []struct {
		SQL  []string
		Plan []string
	}
	tk.MustExec("create table t1  (c_int int, c_str varchar(40), primary key (c_int))")
	tk.MustExec("create table t2  (c_int int, c_str varchar(40), primary key (c_int))")
	tk.MustExec("insert into t1 (`c_int`, `c_str`) values (11, 'keen williamson'), (10, 'gracious hermann')")
	tk.MustExec("insert into t2 (`c_int`, `c_str`) values (10, 'gracious hermann')")

	s.testData.GetTestCases(c, &input, &output)
	for i, ts := range input {
		tk.MustExec("begin")
		for j, tt := range ts {
			if j != len(ts)-1 {
				tk.MustExec(tt)
			}
			s.testData.OnRecord(func() {
				output[i].SQL = ts
				if j == len(ts)-1 {
					output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
				}
			})
			if j == len(ts)-1 {
				tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
			}
		}
		tk.MustExec("rollback")
	}
}

func (s *testPlanSuite) TestDoSubquery(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	se, err := session.CreateSession4Test(store)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)
	tests := []struct {
		sql  string
		best string
	}{
		{
			sql:  "do 1 in (select a from t)",
			best: "LeftHashJoin{Dual->PointGet(Handle(t.a)1)}->Projection",
		},
	}
	for _, tt := range tests {
		comment := Commentf("for %s", tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)
		p, _, err := planner.Optimize(context.TODO(), se, stmt, s.is)
		c.Assert(err, IsNil)
		c.Assert(core.ToString(p), Equals, tt.best, comment)
	}
}

func (s *testPlanSuite) TestIndexLookupCartesianJoin(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	se, err := session.CreateSession4Test(store)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)
	sql := "select /*+ TIDB_INLJ(t1, t2) */ * from t t1 join t t2"
	stmt, err := s.ParseOneStmt(sql, "", "")
	c.Assert(err, IsNil)
	p, _, err := planner.Optimize(context.TODO(), se, stmt, s.is)
	c.Assert(err, IsNil)
	c.Assert(core.ToString(p), Equals, "LeftHashJoin{TableReader(Table(t))->TableReader(Table(t))}")
	warnings := se.GetSessionVars().StmtCtx.GetWarnings()
	lastWarn := warnings[len(warnings)-1]
	err = core.ErrInternal.GenWithStack("TIDB_INLJ hint is inapplicable without column equal ON condition")
	c.Assert(terror.ErrorEqual(err, lastWarn.Err), IsTrue)
}

func (s *testPlanSuite) TestSemiJoinToInner(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	se, err := session.CreateSession4Test(store)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)
	var input []string
	var output []struct {
		SQL  string
		Best string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil)
		p, _, err := planner.Optimize(context.TODO(), se, stmt, s.is)
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Best = core.ToString(p)
		})
		c.Assert(core.ToString(p), Equals, output[i].Best)
	}
}

func (s *testPlanSuite) TestUnmatchedTableInHint(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	se, err := session.CreateSession4Test(store)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)
	var input []string
	var output []struct {
		SQL     string
		Warning string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, test := range input {
		se.GetSessionVars().StmtCtx.SetWarnings(nil)
		stmt, err := s.ParseOneStmt(test, "", "")
		c.Assert(err, IsNil)
		_, _, err = planner.Optimize(context.TODO(), se, stmt, s.is)
		c.Assert(err, IsNil)
		warnings := se.GetSessionVars().StmtCtx.GetWarnings()
		s.testData.OnRecord(func() {
			output[i].SQL = test
			if len(warnings) > 0 {
				output[i].Warning = warnings[0].Err.Error()
			}
		})
		if output[i].Warning == "" {
			c.Assert(len(warnings), Equals, 0)
		} else {
			c.Assert(len(warnings), Equals, 1)
			c.Assert(warnings[0].Level, Equals, stmtctx.WarnLevelWarning)
			c.Assert(warnings[0].Err.Error(), Equals, output[i].Warning)
		}
	}
}

func (s *testPlanSuite) TestHintScope(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	se, err := session.CreateSession4Test(store)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)

	var input []string
	var output []struct {
		SQL  string
		Best string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, test := range input {
		comment := Commentf("case:%v sql:%s", i, test)
		stmt, err := s.ParseOneStmt(test, "", "")
		c.Assert(err, IsNil, comment)

		p, _, err := planner.Optimize(context.Background(), se, stmt, s.is)
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[i].SQL = test
			output[i].Best = core.ToString(p)
		})
		c.Assert(core.ToString(p), Equals, output[i].Best)

		warnings := se.GetSessionVars().StmtCtx.GetWarnings()
		c.Assert(warnings, HasLen, 0, comment)
	}
}

func (s *testPlanSuite) TestJoinHints(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	se, err := session.CreateSession4Test(store)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)

	var input []string
	var output []struct {
		SQL     string
		Best    string
		Warning string
		Hints   string
	}
	s.testData.GetTestCases(c, &input, &output)
	ctx := context.Background()
	for i, test := range input {
		comment := Commentf("case:%v sql:%s", i, test)
		stmt, err := s.ParseOneStmt(test, "", "")
		c.Assert(err, IsNil, comment)

		se.GetSessionVars().StmtCtx.SetWarnings(nil)
		p, _, err := planner.Optimize(ctx, se, stmt, s.is)
		c.Assert(err, IsNil)
		warnings := se.GetSessionVars().StmtCtx.GetWarnings()

		s.testData.OnRecord(func() {
			output[i].SQL = test
			output[i].Best = core.ToString(p)
			if len(warnings) > 0 {
				output[i].Warning = warnings[0].Err.Error()
			}
			output[i].Hints = hint.RestoreOptimizerHints(core.GenHintsFromPhysicalPlan(p))
		})
		c.Assert(core.ToString(p), Equals, output[i].Best)
		if output[i].Warning == "" {
			c.Assert(len(warnings), Equals, 0)
		} else {
			c.Assert(len(warnings), Equals, 1, Commentf("%v", warnings))
			c.Assert(warnings[0].Level, Equals, stmtctx.WarnLevelWarning)
			c.Assert(warnings[0].Err.Error(), Equals, output[i].Warning)
		}
		c.Assert(hint.RestoreOptimizerHints(core.GenHintsFromPhysicalPlan(p)), Equals, output[i].Hints, comment)
	}
}

func (s *testPlanSuite) TestAggregationHints(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	se, err := session.CreateSession4Test(store)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)

	sessionVars := se.(sessionctx.Context).GetSessionVars()
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
	s.testData.GetTestCases(c, &input, &output)
	ctx := context.Background()
	for i, test := range input {
		comment := Commentf("case:%v sql:%s", i, test)
		se.GetSessionVars().StmtCtx.SetWarnings(nil)
		se.GetSessionVars().AllowAggPushDown = test.AggPushDown

		stmt, err := s.ParseOneStmt(test.SQL, "", "")
		c.Assert(err, IsNil, comment)

		p, _, err := planner.Optimize(ctx, se, stmt, s.is)
		c.Assert(err, IsNil)
		warnings := se.GetSessionVars().StmtCtx.GetWarnings()

		s.testData.OnRecord(func() {
			output[i].SQL = test.SQL
			output[i].Best = core.ToString(p)
			if len(warnings) > 0 {
				output[i].Warning = warnings[0].Err.Error()
			}
		})
		c.Assert(core.ToString(p), Equals, output[i].Best, comment)
		if output[i].Warning == "" {
			c.Assert(len(warnings), Equals, 0, comment)
		} else {
			c.Assert(len(warnings), Equals, 1, comment)
			c.Assert(warnings[0].Level, Equals, stmtctx.WarnLevelWarning, comment)
			c.Assert(warnings[0].Err.Error(), Equals, output[i].Warning, comment)
		}
	}
}

func (s *testPlanSuite) TestExplainJoinHints(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	tk := testkit.NewTestKit(c, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, c int, key(b), key(c))")
	tk.MustQuery("explain format='hint' select /*+ inl_merge_join(t2) */ * from t t1 inner join t t2 on t1.b = t2.b and t1.c = 1").Check(testkit.Rows(
		"use_index(@`sel_1` `test`.`t1` `c`), use_index(@`sel_1` `test`.`t2` `b`), inl_merge_join(@`sel_1` `test`.`t2`), inl_merge_join(`t2`)",
	))
	tk.MustQuery("explain format='hint' select /*+ inl_hash_join(t2) */ * from t t1 inner join t t2 on t1.b = t2.b and t1.c = 1").Check(testkit.Rows(
		"use_index(@`sel_1` `test`.`t1` `c`), use_index(@`sel_1` `test`.`t2` `b`), inl_hash_join(@`sel_1` `test`.`t2`), inl_hash_join(`t2`)",
	))
}

func (s *testPlanSuite) TestAggToCopHint(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	tk := testkit.NewTestKit(c, store)
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
	s.testData.GetTestCases(c, &input, &output)

	ctx := context.Background()
	is := domain.GetDomain(tk.Se).InfoSchema()
	for i, test := range input {
		comment := Commentf("case:%v sql:%s", i, test)
		s.testData.OnRecord(func() {
			output[i].SQL = test
		})
		c.Assert(test, Equals, output[i].SQL, comment)

		tk.Se.GetSessionVars().StmtCtx.SetWarnings(nil)

		stmt, err := s.ParseOneStmt(test, "", "")
		c.Assert(err, IsNil, comment)

		p, _, err := planner.Optimize(ctx, tk.Se, stmt, is)
		c.Assert(err, IsNil, comment)
		planString := core.ToString(p)
		s.testData.OnRecord(func() {
			output[i].Best = planString
		})
		c.Assert(planString, Equals, output[i].Best, comment)

		warnings := tk.Se.GetSessionVars().StmtCtx.GetWarnings()
		s.testData.OnRecord(func() {
			if len(warnings) > 0 {
				output[i].Warning = warnings[0].Err.Error()
			}
		})
		if output[i].Warning == "" {
			c.Assert(len(warnings), Equals, 0, comment)
		} else {
			c.Assert(len(warnings), Equals, 1, comment)
			c.Assert(warnings[0].Level, Equals, stmtctx.WarnLevelWarning, comment)
			c.Assert(warnings[0].Err.Error(), Equals, output[i].Warning, comment)
		}
	}
}

func (s *testPlanSuite) TestLimitToCopHint(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	tk := testkit.NewTestKit(c, store)
	tk.MustExec("use test")
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

	s.testData.GetTestCases(c, &input, &output)

	for i, ts := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = ts
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + ts).Rows())
		})
		tk.MustQuery("explain format = 'brief' " + ts).Check(testkit.Rows(output[i].Plan...))

		comment := Commentf("case:%v sql:%s", i, ts)
		warnings := tk.Se.GetSessionVars().StmtCtx.GetWarnings()
		s.testData.OnRecord(func() {
			if len(warnings) > 0 {
				output[i].Warning = make([]string, len(warnings))
				for j, warning := range warnings {
					output[i].Warning[j] = warning.Err.Error()
				}
			}
		})
		if len(output[i].Warning) == 0 {
			c.Assert(len(warnings), Equals, 0, comment)
		} else {
			c.Assert(len(warnings), Equals, len(output[i].Warning), comment)
			for j, warning := range warnings {
				c.Assert(warning.Level, Equals, stmtctx.WarnLevelWarning, comment)
				c.Assert(warning.Err.Error(), Equals, output[i].Warning[j], comment)
			}
		}
	}
}

func (s *testPlanSuite) TestPushdownDistinctEnable(c *C) {
	defer testleak.AfterTest(c)()
	var (
		input  []string
		output []struct {
			SQL    string
			Plan   []string
			Result []string
		}
	)
	s.testData.GetTestCases(c, &input, &output)
	vars := []string{
		fmt.Sprintf("set @@session.%s = 1", variable.TiDBOptDistinctAggPushDown),
		"set session tidb_opt_agg_push_down = 1",
	}
	s.doTestPushdownDistinct(c, vars, input, output)
}

func (s *testPlanSuite) TestPushdownDistinctDisable(c *C) {
	defer testleak.AfterTest(c)()
	var (
		input  []string
		output []struct {
			SQL    string
			Plan   []string
			Result []string
		}
	)

	s.testData.GetTestCases(c, &input, &output)
	vars := []string{
		fmt.Sprintf("set @@session.%s = 0", variable.TiDBOptDistinctAggPushDown),
		"set session tidb_opt_agg_push_down = 1",
	}
	s.doTestPushdownDistinct(c, vars, input, output)
}

func (s *testPlanSuite) TestPushdownDistinctEnableAggPushDownDisable(c *C) {
	var (
		input  []string
		output []struct {
			SQL    string
			Plan   []string
			Result []string
		}
	)
	s.testData.GetTestCases(c, &input, &output)
	vars := []string{
		fmt.Sprintf("set @@session.%s = 1", variable.TiDBOptDistinctAggPushDown),
		"set session tidb_opt_agg_push_down = 0",
	}
	s.doTestPushdownDistinct(c, vars, input, output)
}

func (s *testPlanSuite) doTestPushdownDistinct(c *C, vars, input []string, output []struct {
	SQL    string
	Plan   []string
	Result []string
}) {
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	tk := testkit.NewTestKit(c, store)
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
		s.testData.OnRecord(func() {
			output[i].SQL = ts
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + ts).Rows())
			output[i].Result = s.testData.ConvertRowsToStrings(tk.MustQuery(ts).Sort().Rows())
		})
		tk.MustQuery("explain format = 'brief' " + ts).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(ts).Sort().Check(testkit.Rows(output[i].Result...))
	}
}

func (s *testPlanSuite) TestGroupConcatOrderby(c *C) {
	var (
		input  []string
		output []struct {
			SQL    string
			Plan   []string
			Result []string
		}
	)
	s.testData.GetTestCases(c, &input, &output)
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	tk := testkit.NewTestKit(c, store)
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
		s.testData.OnRecord(func() {
			output[i].SQL = ts
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + ts).Rows())
			output[i].Result = s.testData.ConvertRowsToStrings(tk.MustQuery(ts).Sort().Rows())
		})
		tk.MustQuery("explain format = 'brief' " + ts).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(ts).Check(testkit.Rows(output[i].Result...))
	}
}

func (s *testPlanSuite) TestHintAlias(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	se, err := session.CreateSession4Test(store)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)

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
	for i, tt := range tests {
		comment := Commentf("case:%v sql1:%s sql2:%s", i, tt.sql1, tt.sql2)
		stmt1, err := s.ParseOneStmt(tt.sql1, "", "")
		c.Assert(err, IsNil, comment)
		stmt2, err := s.ParseOneStmt(tt.sql2, "", "")
		c.Assert(err, IsNil, comment)

		p1, _, err := planner.Optimize(ctx, se, stmt1, s.is)
		c.Assert(err, IsNil)
		p2, _, err := planner.Optimize(ctx, se, stmt2, s.is)
		c.Assert(err, IsNil)

		c.Assert(core.ToString(p1), Equals, core.ToString(p2))
	}
}

func (s *testPlanSuite) TestIndexHint(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	se, err := session.CreateSession4Test(store)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)

	var input []string
	var output []struct {
		SQL     string
		Best    string
		HasWarn bool
		Hints   string
	}
	s.testData.GetTestCases(c, &input, &output)
	ctx := context.Background()
	for i, test := range input {
		comment := Commentf("case:%v sql:%s", i, test)
		se.GetSessionVars().StmtCtx.SetWarnings(nil)

		stmt, err := s.ParseOneStmt(test, "", "")
		c.Assert(err, IsNil, comment)

		p, _, err := planner.Optimize(ctx, se, stmt, s.is)
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[i].SQL = test
			output[i].Best = core.ToString(p)
			output[i].HasWarn = len(se.GetSessionVars().StmtCtx.GetWarnings()) > 0
			output[i].Hints = hint.RestoreOptimizerHints(core.GenHintsFromPhysicalPlan(p))
		})
		c.Assert(core.ToString(p), Equals, output[i].Best, comment)
		warnings := se.GetSessionVars().StmtCtx.GetWarnings()
		if output[i].HasWarn {
			c.Assert(warnings, HasLen, 1, comment)
		} else {
			c.Assert(warnings, HasLen, 0, comment)
		}
		c.Assert(hint.RestoreOptimizerHints(core.GenHintsFromPhysicalPlan(p)), Equals, output[i].Hints, comment)
	}
}

func (s *testPlanSuite) TestIndexMergeHint(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	se, err := session.CreateSession4Test(store)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)

	var input []string
	var output []struct {
		SQL     string
		Best    string
		HasWarn bool
		Hints   string
	}
	s.testData.GetTestCases(c, &input, &output)
	ctx := context.Background()
	for i, test := range input {
		comment := Commentf("case:%v sql:%s", i, test)
		se.GetSessionVars().StmtCtx.SetWarnings(nil)
		stmt, err := s.ParseOneStmt(test, "", "")
		c.Assert(err, IsNil, comment)
		sctx := se.(sessionctx.Context)
		err = executor.ResetContextOfStmt(sctx, stmt)
		c.Assert(err, IsNil)
		p, _, err := planner.Optimize(ctx, se, stmt, s.is)
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[i].SQL = test
			output[i].Best = core.ToString(p)
			output[i].HasWarn = len(se.GetSessionVars().StmtCtx.GetWarnings()) > 0
			output[i].Hints = hint.RestoreOptimizerHints(core.GenHintsFromPhysicalPlan(p))
		})
		c.Assert(core.ToString(p), Equals, output[i].Best, comment)
		warnings := se.GetSessionVars().StmtCtx.GetWarnings()
		if output[i].HasWarn {
			c.Assert(warnings, HasLen, 1, comment)
		} else {
			c.Assert(warnings, HasLen, 0, comment)
		}
		c.Assert(hint.RestoreOptimizerHints(core.GenHintsFromPhysicalPlan(p)), Equals, output[i].Hints, comment)
	}
}

func (s *testPlanSuite) TestQueryBlockHint(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	se, err := session.CreateSession4Test(store)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)

	var input []string
	var output []struct {
		SQL   string
		Plan  string
		Hints string
	}
	s.testData.GetTestCases(c, &input, &output)
	ctx := context.TODO()
	for i, tt := range input {
		comment := Commentf("case:%v sql: %s", i, tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)

		p, _, err := planner.Optimize(ctx, se, stmt, s.is)
		c.Assert(err, IsNil, comment)
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = core.ToString(p)
			output[i].Hints = hint.RestoreOptimizerHints(core.GenHintsFromPhysicalPlan(p))
		})
		c.Assert(core.ToString(p), Equals, output[i].Plan, comment)
		c.Assert(hint.RestoreOptimizerHints(core.GenHintsFromPhysicalPlan(p)), Equals, output[i].Hints, comment)
	}
}

func (s *testPlanSuite) TestInlineProjection(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	se, err := session.CreateSession4Test(store)
	c.Assert(err, IsNil)
	ctx := context.Background()
	_, err = se.Execute(ctx, "use test")
	c.Assert(err, IsNil)
	_, err = se.Execute(ctx, `drop table if exists test.t1, test.t2;`)
	c.Assert(err, IsNil)
	_, err = se.Execute(ctx, `create table test.t1(a bigint, b bigint, index idx_a(a), index idx_b(b));`)
	c.Assert(err, IsNil)
	_, err = se.Execute(ctx, `create table test.t2(a bigint, b bigint, index idx_a(a), index idx_b(b));`)
	c.Assert(err, IsNil)

	var input []string
	var output []struct {
		SQL   string
		Plan  string
		Hints string
	}
	is := domain.GetDomain(se).InfoSchema()
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		comment := Commentf("case:%v sql: %s", i, tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)

		p, _, err := planner.Optimize(ctx, se, stmt, is)
		c.Assert(err, IsNil, comment)
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = core.ToString(p)
			output[i].Hints = hint.RestoreOptimizerHints(core.GenHintsFromPhysicalPlan(p))
		})
		c.Assert(core.ToString(p), Equals, output[i].Plan, comment)
		c.Assert(hint.RestoreOptimizerHints(core.GenHintsFromPhysicalPlan(p)), Equals, output[i].Hints, comment)
	}
}

func (s *testPlanSuite) TestDAGPlanBuilderSplitAvg(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	se, err := session.CreateSession4Test(store)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)
	tests := []struct {
		sql  string
		plan string
	}{
		{
			sql:  "select avg(a),avg(b),avg(c) from t",
			plan: "TableReader(Table(t)->StreamAgg)->StreamAgg",
		},
		{
			sql:  "select /*+ HASH_AGG() */ avg(a),avg(b),avg(c) from t",
			plan: "TableReader(Table(t)->HashAgg)->HashAgg",
		},
	}

	for _, tt := range tests {
		comment := Commentf("for %s", tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)

		err = core.Preprocess(se, stmt, core.WithPreprocessorReturn(&core.PreprocessorReturn{InfoSchema: s.is}))
		c.Assert(err, IsNil)
		p, _, err := planner.Optimize(context.TODO(), se, stmt, s.is)
		c.Assert(err, IsNil, comment)

		c.Assert(core.ToString(p), Equals, tt.plan, comment)
		root, ok := p.(core.PhysicalPlan)
		if !ok {
			continue
		}
		testDAGPlanBuilderSplitAvg(c, root)
	}
}

func testDAGPlanBuilderSplitAvg(c *C, root core.PhysicalPlan) {
	if p, ok := root.(*core.PhysicalTableReader); ok {
		if p.TablePlans != nil {
			baseAgg := p.TablePlans[len(p.TablePlans)-1]
			if agg, ok := baseAgg.(*core.PhysicalHashAgg); ok {
				for i, aggfunc := range agg.AggFuncs {
					c.Assert(agg.Schema().Columns[i].RetType, Equals, aggfunc.RetTp)
				}
			}
			if agg, ok := baseAgg.(*core.PhysicalStreamAgg); ok {
				for i, aggfunc := range agg.AggFuncs {
					c.Assert(agg.Schema().Columns[i].RetType, Equals, aggfunc.RetTp)
				}
			}
		}
	}

	childs := root.Children()
	if childs == nil {
		return
	}
	for _, son := range childs {
		testDAGPlanBuilderSplitAvg(c, son)
	}
}

func (s *testPlanSuite) TestIndexJoinHint(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	se, err := session.CreateSession4Test(store)
	c.Assert(err, IsNil)
	ctx := context.Background()
	_, err = se.Execute(ctx, "use test")
	c.Assert(err, IsNil)
	_, err = se.Execute(ctx, `drop table if exists test.t1, test.t2, test.t;`)
	c.Assert(err, IsNil)
	_, err = se.Execute(ctx, `create table test.t1(a bigint, b bigint, index idx_a(a), index idx_b(b));`)
	c.Assert(err, IsNil)
	_, err = se.Execute(ctx, `create table test.t2(a bigint, b bigint, index idx_a(a), index idx_b(b));`)
	c.Assert(err, IsNil)
	_, err = se.Execute(ctx, "CREATE TABLE `t` ( `a` bigint(20) NOT NULL, `b` tinyint(1) DEFAULT NULL, `c` datetime DEFAULT NULL, `d` int(10) unsigned DEFAULT NULL, `e` varchar(20) DEFAULT NULL, `f` double DEFAULT NULL, `g` decimal(30,5) DEFAULT NULL, `h` float DEFAULT NULL, `i` date DEFAULT NULL, `j` timestamp NULL DEFAULT NULL, PRIMARY KEY (`a`), UNIQUE KEY `b` (`b`), KEY `c` (`c`,`d`,`e`), KEY `f` (`f`), KEY `g` (`g`,`h`), KEY `g_2` (`g`), UNIQUE KEY `g_3` (`g`), KEY `i` (`i`) );")
	c.Assert(err, IsNil)
	var input []string
	var output []struct {
		SQL  string
		Plan string
	}
	is := domain.GetDomain(se).InfoSchema()
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		comment := Commentf("case:%v sql: %s", i, tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)
		p, _, err := planner.Optimize(ctx, se, stmt, is)
		c.Assert(err, IsNil, comment)
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = core.ToString(p)
		})
		c.Assert(core.ToString(p), Equals, output[i].Plan, comment)
	}
}

func (s *testPlanSuite) TestDAGPlanBuilderWindow(c *C) {
	defer testleak.AfterTest(c)()
	var input []string
	var output []struct {
		SQL  string
		Best string
	}
	s.testData.GetTestCases(c, &input, &output)
	vars := []string{
		"set @@session.tidb_window_concurrency = 1",
	}
	s.doTestDAGPlanBuilderWindow(c, vars, input, output)
}

func (s *testPlanSuite) TestDAGPlanBuilderWindowParallel(c *C) {
	defer testleak.AfterTest(c)()
	var input []string
	var output []struct {
		SQL  string
		Best string
	}
	s.testData.GetTestCases(c, &input, &output)
	vars := []string{
		"set @@session.tidb_window_concurrency = 4",
	}
	s.doTestDAGPlanBuilderWindow(c, vars, input, output)
}

func (s *testPlanSuite) TestTopNPushDownEmpty(c *C) {
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	tk := testkit.NewTestKit(c, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, c int, index idx_a(a))")
	tk.MustQuery("select extract(day_hour from 'ziy') as res from t order by res limit 1").Check(testkit.Rows())
}

func (s *testPlanSuite) doTestDAGPlanBuilderWindow(c *C, vars, input []string, output []struct {
	SQL  string
	Best string
}) {
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	se, err := session.CreateSession4Test(store)
	c.Assert(err, IsNil)
	ctx := context.Background()
	_, err = se.Execute(ctx, "use test")
	c.Assert(err, IsNil)

	for _, v := range vars {
		_, err = se.Execute(ctx, v)
		c.Assert(err, IsNil)
	}

	for i, tt := range input {
		comment := Commentf("case:%v sql:%s", i, tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)

		err = se.NewTxn(context.Background())
		c.Assert(err, IsNil)
		p, _, err := planner.Optimize(context.TODO(), se, stmt, s.is)
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Best = core.ToString(p)
		})
		c.Assert(core.ToString(p), Equals, output[i].Best, comment)
	}
}

func (s *testPlanSuite) TestNominalSort(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		store.Close()
	}()
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
	s.testData.GetTestCases(c, &input, &output)
	for i, ts := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = ts
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + ts).Rows())
			output[i].Result = s.testData.ConvertRowsToStrings(tk.MustQuery(ts).Rows())
		})
		tk.MustQuery("explain format = 'brief' " + ts).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(ts).Check(testkit.Rows(output[i].Result...))
	}
}

func (s *testPlanSuite) TestHintFromDiffDatabase(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	se, err := session.CreateSession4Test(store)
	c.Assert(err, IsNil)
	ctx := context.Background()
	_, err = se.Execute(ctx, "use test")
	c.Assert(err, IsNil)
	_, err = se.Execute(ctx, `drop table if exists test.t1`)
	c.Assert(err, IsNil)
	_, err = se.Execute(ctx, `create table test.t1(a bigint, index idx_a(a));`)
	c.Assert(err, IsNil)
	_, err = se.Execute(ctx, `create table test.t2(a bigint, index idx_a(a));`)
	c.Assert(err, IsNil)

	_, err = se.Execute(ctx, "drop database if exists test2")
	c.Assert(err, IsNil)
	_, err = se.Execute(ctx, "create database test2")
	c.Assert(err, IsNil)
	_, err = se.Execute(ctx, "use test2")
	c.Assert(err, IsNil)

	var input []string
	var output []struct {
		SQL  string
		Plan string
	}
	is := domain.GetDomain(se).InfoSchema()
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		comment := Commentf("case:%v sql: %s", i, tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)
		p, _, err := planner.Optimize(ctx, se, stmt, is)
		c.Assert(err, IsNil, comment)
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = core.ToString(p)
		})
		c.Assert(core.ToString(p), Equals, output[i].Plan, comment)
	}
}

func (s *testPlanSuite) TestNthPlanHintWithExplain(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		store.Close()
	}()
	se, err := session.CreateSession4Test(store)
	c.Assert(err, IsNil)
	ctx := context.Background()
	_, err = se.Execute(ctx, "use test")
	c.Assert(err, IsNil)
	_, err = se.Execute(ctx, `drop table if exists test.tt`)
	c.Assert(err, IsNil)
	_, err = se.Execute(ctx, `create table test.tt (a int,b int, index(a), index(b));`)
	c.Assert(err, IsNil)

	_, err = se.Execute(ctx, "insert into tt values (1, 1), (2, 2), (3, 4)")
	c.Assert(err, IsNil)

	tk.MustExec(`set @@tidb_partition_prune_mode='` + string(variable.Static) + `'`)

	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, ts := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = ts
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + ts).Rows())
		})
		tk.MustQuery("explain format = 'brief' " + ts).Check(testkit.Rows(output[i].Plan...))
	}

	// This assert makes sure a query with or without nth_plan() hint output exactly the same plan(including plan ID).
	// The query below is the same as queries in the testdata except for nth_plan() hint.
	// Currently its output is the same as the second test case in the testdata, which is `output[1]`. If this doesn't
	// hold in the future, you may need to modify this.
	tk.MustQuery("explain format = 'brief' select * from test.tt where a=1 and b=1").Check(testkit.Rows(output[1].Plan...))
}

func (s *testPlanSuite) TestEnumIndex(c *C) {
	var (
		input  []string
		output []struct {
			SQL    string
			Plan   []string
			Result []string
		}
	)
	s.testData.GetTestCases(c, &input, &output)
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	tk := testkit.NewTestKit(c, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(e enum('c','b','a',''), index idx(e))")
	tk.MustExec("insert ignore into t values(0),(1),(2),(3),(4);")

	for i, ts := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = ts
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain format='brief'" + ts).Rows())
			output[i].Result = s.testData.ConvertRowsToStrings(tk.MustQuery(ts).Sort().Rows())
		})
		tk.MustQuery("explain format='brief' " + ts).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(ts).Sort().Check(testkit.Rows(output[i].Result...))
	}
}

func (s *testPlanSuite) TestIssue27233(c *C) {
	var (
		input  []string
		output []struct {
			SQL    string
			Plan   []string
			Result []string
		}
	)
	s.testData.GetTestCases(c, &input, &output)
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	tk := testkit.NewTestKit(c, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE `PK_S_MULTI_31` (\n  `COL1` tinyint(45) NOT NULL,\n  `COL2` tinyint(45) NOT NULL,\n  PRIMARY KEY (`COL1`,`COL2`) /*T![clustered_index] NONCLUSTERED */\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;")
	tk.MustExec("insert into PK_S_MULTI_31 values(122,100),(124,-22),(124,34),(127,103);")

	for i, ts := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = ts
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain format='brief'" + ts).Rows())
			output[i].Result = s.testData.ConvertRowsToStrings(tk.MustQuery(ts).Sort().Rows())
		})
		tk.MustQuery("explain format='brief' " + ts).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(ts).Sort().Check(testkit.Rows(output[i].Result...))
	}
}

func (s *testPlanSuite) TestPossibleProperties(c *C) {
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	tk := testkit.NewTestKit(c, store)
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

func (s *testPlanSuite) TestSelectionPartialPushDown(c *C) {
	var (
		input  []string
		output []struct {
			SQL  string
			Plan []string
		}
	)
	s.testData.GetTestCases(c, &input, &output)
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	tk := testkit.NewTestKit(c, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int, b int as (a+1) virtual)")
	tk.MustExec("create table t2(a int, b int as (a+1) virtual, c int, key idx_a(a))")

	for i, ts := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = ts
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain format='brief'" + ts).Rows())
		})
		tk.MustQuery("explain format='brief' " + ts).Check(testkit.Rows(output[i].Plan...))
	}
}

func (s *testPlanSuite) TestIssue28316(c *C) {
	var (
		input  []string
		output []struct {
			SQL  string
			Plan []string
		}
	)
	s.testData.GetTestCases(c, &input, &output)
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	tk := testkit.NewTestKit(c, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")

	for i, ts := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = ts
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain format='brief'" + ts).Rows())
		})
		tk.MustQuery("explain format='brief' " + ts).Check(testkit.Rows(output[i].Plan...))
	}
}

func (s *testPlanSuite) TestIssue30965(c *C) {
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	tk := testkit.NewTestKit(c, store)
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

func (s *testPlanSuite) TestMPPPassThroughType(c *C) {
	var (
		input  []string
		output []struct {
			SQL  string
			Plan []string
		}
	)
	s.testData.GetTestCases(c, &input, &output)
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	tk := testkit.NewTestKit(c, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists employee")
	tk.MustExec("create table employee(empid int, deptid int, salary decimal(10,2))")
	tk.MustExec("set tidb_enforce_mpp=1")

	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	c.Assert(exists, IsTrue)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "employee" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	for i, ts := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = ts
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain format='brief'" + ts).Rows())
		})
		tk.MustQuery("explain format='brief' " + ts).Check(testkit.Rows(output[i].Plan...))
	}
}
