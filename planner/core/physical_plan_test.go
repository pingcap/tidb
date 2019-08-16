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
// See the License for the specific language governing permissions and
// limitations under the License.

package core_test

import (
	"context"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/planner"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/testutil"
)

var _ = Suite(&testPlanSuite{})

type testPlanSuite struct {
	*parser.Parser
	is infoschema.InfoSchema

	testData testutil.TestData
}

func (s *testPlanSuite) SetUpSuite(c *C) {
	s.is = infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable()})
	s.Parser = parser.New()
	s.Parser.EnableWindowFunc(true)

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
		p, err := planner.Optimize(context.TODO(), se, stmt, s.is)
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Best = core.ToString(p)
		})
		c.Assert(core.ToString(p), Equals, output[i].Best, comment)
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

		p, err := planner.Optimize(context.TODO(), se, stmt, s.is)
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
	se.Execute(context.Background(), "set sql_mode='STRICT_TRANS_TABLES'") // disable only full group by
	ctx := se.(sessionctx.Context)
	sessionVars := ctx.GetSessionVars()
	sessionVars.HashAggFinalConcurrency = 1
	sessionVars.HashAggPartialConcurrency = 1
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

		p, err := planner.Optimize(context.TODO(), se, stmt, s.is)
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

		p, err := planner.Optimize(context.TODO(), se, stmt, s.is)
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
		SQL  string
		Best string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		comment := Commentf("for %s", tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)

		core.Preprocess(se, stmt, s.is)
		p, err := planner.Optimize(context.TODO(), se, stmt, s.is)
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Best = core.ToString(p)
		})
		c.Assert(core.ToString(p), Equals, output[i].Best, Commentf("for %s", tt))
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

		p, err := planner.Optimize(context.TODO(), se, stmt, s.is)
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
		txn.Set(kv.Key("AAA"), []byte("BBB"))
		c.Assert(se.StmtCommit(), IsNil)
		p, err := planner.Optimize(context.TODO(), se, stmt, s.is)
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
	se.Execute(context.Background(), "use test")
	se.Execute(context.Background(), "set sql_mode='STRICT_TRANS_TABLES'") // disable only full group by
	c.Assert(err, IsNil)
	ctx := se.(sessionctx.Context)
	sessionVars := ctx.GetSessionVars()
	sessionVars.HashAggFinalConcurrency = 1
	sessionVars.HashAggPartialConcurrency = 1

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

		p, err := planner.Optimize(context.TODO(), se, stmt, s.is)
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

<<<<<<< HEAD
	var input []string
	var output []struct {
		SQL  string
		Best string
=======
	tests := []struct {
		sql  string
		best string
	}{
		{
			sql:  "select a from t where c is not null",
			best: "IndexReader(Index(t.c_d_e)[[-inf,+inf]])->Projection",
		},
		{
			sql:  "select a from t where c >= 4",
			best: "IndexReader(Index(t.c_d_e)[[4,+inf]])->Projection",
		},
		{
			sql:  "select a from t where c <= 4",
			best: "IndexReader(Index(t.c_d_e)[[-inf,4]])->Projection",
		},
		{
			sql:  "select a from t where c = 4 and d = 5 and e = 6",
			best: "IndexReader(Index(t.c_d_e)[[4 5 6,4 5 6]])->Projection",
		},
		{
			sql:  "select a from t where d = 4 and c = 5",
			best: "IndexReader(Index(t.c_d_e)[[5 4,5 4]])->Projection",
		},
		{
			sql:  "select a from t where c = 4 and e < 5",
			best: "IndexReader(Index(t.c_d_e)[[4,4]]->Sel([lt(test.t.e, 5)]))->Projection",
		},
		{
			sql:  "select a from t where c = 4 and d <= 5 and d > 3",
			best: "IndexReader(Index(t.c_d_e)[(4 3,4 5]])->Projection",
		},
		{
			sql:  "select a from t where d <= 5 and d > 3",
			best: "TableReader(Table(t)->Sel([le(test.t.d, 5) gt(test.t.d, 3)]))->Projection",
		},
		{
			sql:  "select a from t where c between 1 and 2",
			best: "IndexReader(Index(t.c_d_e)[[1,2]])->Projection",
		},
		{
			sql:  "select a from t where c not between 1 and 2",
			best: "IndexReader(Index(t.c_d_e)[[-inf,1) (2,+inf]])->Projection",
		},
		{
			sql:  "select a from t where c <= 5 and c >= 3 and d = 1",
			best: "IndexReader(Index(t.c_d_e)[[3,5]]->Sel([eq(test.t.d, 1)]))->Projection",
		},
		{
			sql:  "select a from t where c = 1 or c = 2 or c = 3",
			best: "IndexReader(Index(t.c_d_e)[[1,3]])->Projection",
		},
		{
			sql:  "select b from t where c = 1 or c = 2 or c = 3 or c = 4 or c = 5",
			best: "IndexLookUp(Index(t.c_d_e)[[1,5]], Table(t))->Projection",
		},
		{
			sql:  "select a from t where c = 5",
			best: "IndexReader(Index(t.c_d_e)[[5,5]])->Projection",
		},
		{
			sql:  "select a from t where c = 5 and b = 1",
			best: "IndexLookUp(Index(t.c_d_e)[[5,5]], Table(t)->Sel([eq(test.t.b, 1)]))->Projection",
		},
		{
			sql:  "select a from t where not a",
			best: "TableReader(Table(t)->Sel([not(test.t.a)]))",
		},
		{
			sql:  "select a from t where c in (1)",
			best: "IndexReader(Index(t.c_d_e)[[1,1]])->Projection",
		},
		{
			sql:  "select a from t where c in ('1')",
			best: "IndexReader(Index(t.c_d_e)[[1,1]])->Projection",
		},
		{
			sql:  "select a from t where c = 1.0",
			best: "IndexReader(Index(t.c_d_e)[[1,1]])->Projection",
		},
		{
			sql:  "select a from t where c in (1) and d > 3",
			best: "IndexReader(Index(t.c_d_e)[(1 3,1 +inf]])->Projection",
		},
		{
			sql:  "select a from t where c in (1, 2, 3) and (d > 3 and d < 4 or d > 5 and d < 6)",
			best: "Dual->Projection",
		},
		{
			sql:  "select a from t where c in (1, 2, 3) and (d > 2 and d < 4 or d > 5 and d < 7)",
			best: "IndexReader(Index(t.c_d_e)[(1 2,1 4) (1 5,1 7) (2 2,2 4) (2 5,2 7) (3 2,3 4) (3 5,3 7)])->Projection",
		},
		{
			sql:  "select a from t where c in (1, 2, 3)",
			best: "IndexReader(Index(t.c_d_e)[[1,1] [2,2] [3,3]])->Projection",
		},
		{
			sql:  "select a from t where c in (1, 2, 3) and d in (1,2) and e = 1",
			best: "IndexReader(Index(t.c_d_e)[[1 1 1,1 1 1] [1 2 1,1 2 1] [2 1 1,2 1 1] [2 2 1,2 2 1] [3 1 1,3 1 1] [3 2 1,3 2 1]])->Projection",
		},
		{
			sql:  "select a from t where d in (1, 2, 3)",
			best: "TableReader(Table(t)->Sel([in(test.t.d, 1, 2, 3)]))->Projection",
		},
		{
			sql:  "select a from t where c not in (1)",
			best: "IndexReader(Index(t.c_d_e)[[-inf,1) (1,+inf]])->Projection",
		},
		// test like
		{
			sql:  "select a from t use index(c_d_e) where c != 1",
			best: "IndexReader(Index(t.c_d_e)[[-inf,1) (1,+inf]])->Projection",
		},
		{
			sql:  "select a from t where c_str like ''",
			best: `IndexReader(Index(t.c_d_e_str)[["",""]])->Projection`,
		},
		{
			sql:  "select a from t where c_str like 'abc'",
			best: `IndexReader(Index(t.c_d_e_str)[["abc","abc"]])->Projection`,
		},
		{
			sql:  "select a from t where c_str not like 'abc'",
			best: `IndexReader(Index(t.c_d_e_str)[[-inf,"abc") ("abc",+inf]])->Projection`,
		},
		{
			sql:  "select a from t where not (c_str like 'abc' or c_str like 'abd')",
			best: `IndexReader(Index(t.c_d_e_str)[[-inf,"abc") ("abc","abd") ("abd",+inf]])->Projection`,
		},
		{
			sql:  "select a from t where c_str like '_abc'",
			best: "TableReader(Table(t)->Sel([like(test.t.c_str, _abc, 92)]))->Projection",
		},
		{
			sql:  `select a from t where c_str like 'abc%'`,
			best: `IndexReader(Index(t.c_d_e_str)[["abc","abd")])->Projection`,
		},
		{
			sql:  "select a from t where c_str like 'abc_'",
			best: `IndexReader(Index(t.c_d_e_str)[("abc","abd")]->Sel([like(test.t.c_str, abc_, 92)]))->Projection`,
		},
		{
			sql:  "select a from t where c_str like 'abc%af'",
			best: `IndexReader(Index(t.c_d_e_str)[["abc","abd")]->Sel([like(test.t.c_str, abc%af, 92)]))->Projection`,
		},
		{
			sql:  `select a from t where c_str like 'abc\\_' escape ''`,
			best: `IndexReader(Index(t.c_d_e_str)[["abc_","abc_"]])->Projection`,
		},
		{
			sql:  `select a from t where c_str like 'abc\\_'`,
			best: `IndexReader(Index(t.c_d_e_str)[["abc_","abc_"]])->Projection`,
		},
		{
			sql:  `select a from t where c_str like 'abc\\\\_'`,
			best: "IndexReader(Index(t.c_d_e_str)[(\"abc\\\",\"abc]\")]->Sel([like(test.t.c_str, abc\\\\_, 92)]))->Projection",
		},
		{
			sql:  `select a from t where c_str like 'abc\\_%'`,
			best: "IndexReader(Index(t.c_d_e_str)[[\"abc_\",\"abc`\")])->Projection",
		},
		{
			sql:  `select a from t where c_str like 'abc=_%' escape '='`,
			best: "IndexReader(Index(t.c_d_e_str)[[\"abc_\",\"abc`\")])->Projection",
		},
		{
			sql:  `select a from t where c_str like 'abc\\__'`,
			best: "IndexReader(Index(t.c_d_e_str)[(\"abc_\",\"abc`\")]->Sel([like(test.t.c_str, abc\\__, 92)]))->Projection",
		},
		{
			// Check that 123 is converted to string '123'. index can be used.
			sql:  `select a from t where c_str like 123`,
			best: "IndexReader(Index(t.c_d_e_str)[[\"123\",\"123\"]])->Projection",
		},
		{
			sql:  `select a from t where c = 1.9 and d > 3`,
			best: "Dual",
		},
		{
			sql:  `select a from t where c < 1.1`,
			best: "IndexReader(Index(t.c_d_e)[[-inf,2)])->Projection",
		},
		{
			sql:  `select a from t where c <= 1.9`,
			best: "IndexReader(Index(t.c_d_e)[[-inf,1]])->Projection",
		},
		{
			sql:  `select a from t where c >= 1.1`,
			best: "IndexReader(Index(t.c_d_e)[[2,+inf]])->Projection",
		},
		{
			sql:  `select a from t where c > 1.9`,
			best: "IndexReader(Index(t.c_d_e)[(1,+inf]])->Projection",
		},
		{
			sql:  `select a from t where c = 123456789098765432101234`,
			best: "Dual",
		},
		{
			sql:  `select a from t where c = 'hanfei'`,
			best: "TableReader(Table(t))->Sel([eq(cast(test.t.c), cast(hanfei))])->Projection",
		},
>>>>>>> 103cbcd... planner: rewrite `in` as `eq` when the rhs list has only one item
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		comment := Commentf("for %s", tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)
		sc := se.(sessionctx.Context).GetSessionVars().StmtCtx
		sc.IgnoreTruncate = false
		p, err := planner.Optimize(context.TODO(), se, stmt, s.is)
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
	se.Execute(context.Background(), "set sql_mode='STRICT_TRANS_TABLES'") // disable only full group by
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
		p, err := planner.Optimize(context.TODO(), se, stmt, s.is)
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Best = core.ToString(p)
		})
		c.Assert(core.ToString(p), Equals, output[i].Best, Commentf("for %s", tt))
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
	p, err := planner.Optimize(context.TODO(), se, stmt, s.is)
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
			best: "LeftHashJoin{Dual->TableReader(Table(t))}->Projection",
		},
	}
	for _, tt := range tests {
		comment := Commentf("for %s", tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)
		p, err := planner.Optimize(context.TODO(), se, stmt, s.is)
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
	p, err := planner.Optimize(context.TODO(), se, stmt, s.is)
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
		p, err := planner.Optimize(context.TODO(), se, stmt, s.is)
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
		_, err = planner.Optimize(context.TODO(), se, stmt, s.is)
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
	}
	s.testData.GetTestCases(c, &input, &output)
	ctx := context.Background()
	for i, test := range input {
		comment := Commentf("case:%v sql:%s", i, test)
		stmt, err := s.ParseOneStmt(test, "", "")
		c.Assert(err, IsNil, comment)

		se.GetSessionVars().StmtCtx.SetWarnings(nil)
		p, err := planner.Optimize(ctx, se, stmt, s.is)
		c.Assert(err, IsNil)
		warnings := se.GetSessionVars().StmtCtx.GetWarnings()

		s.testData.OnRecord(func() {
			output[i].SQL = test
			output[i].Best = core.ToString(p)
			if len(warnings) > 0 {
				output[i].Warning = warnings[0].Err.Error()
			}
		})
		c.Assert(core.ToString(p), Equals, output[i].Best)
		if output[i].Warning == "" {
			c.Assert(len(warnings), Equals, 0)
		} else {
			c.Assert(len(warnings), Equals, 1, Commentf("%v", warnings))
			c.Assert(warnings[0].Level, Equals, stmtctx.WarnLevelWarning)
			c.Assert(warnings[0].Err.Error(), Equals, output[i].Warning)
		}
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

	tests := []struct {
		sql         string
		best        string
		warning     string
		aggPushDown bool
	}{
		// without Aggregation hints
		{
			sql:  "select count(*) from t t1, t t2 where t1.a = t2.b",
			best: "LeftHashJoin{IndexReader(Index(t.f)[[NULL,+inf]])->TableReader(Table(t))}(test.t1.a,test.t2.b)->HashAgg",
		},
		{
			sql:  "select count(t1.a) from t t1, t t2 where t1.a = t2.a*2 group by t1.a",
			best: "LeftHashJoin{IndexReader(Index(t.f)[[NULL,+inf]])->IndexReader(Index(t.f)[[NULL,+inf]])->Projection}(test.t1.a,mul(test.t2.a, 2))->HashAgg",
		},
		// with Aggregation hints
		{
			sql:  "select /*+ HASH_AGG(), USE_INDEX(t1), USE_INDEX(t2) */ count(*) from t t1, t t2 where t1.a = t2.b",
			best: "LeftHashJoin{TableReader(Table(t))->TableReader(Table(t))}(test.t1.a,test.t2.b)->HashAgg",
		},
		{
			sql:  "select /*+ STREAM_AGG(), USE_INDEX(t1), USE_INDEX(t2) */ count(t1.a) from t t1, t t2 where t1.a = t2.a*2 group by t1.a",
			best: "LeftHashJoin{TableReader(Table(t))->TableReader(Table(t))->Projection}(test.t1.a,mul(test.t2.a, 2))->Sort->StreamAgg",
		},
		// test conflict warning
		{
			sql:     "select /*+ HASH_AGG(), STREAM_AGG(), USE_INDEX(t1), USE_INDEX(t2) */ count(*) from t t1, t t2 where t1.a = t2.b",
			best:    "LeftHashJoin{TableReader(Table(t))->TableReader(Table(t))}(test.t1.a,test.t2.b)->HashAgg",
			warning: "[planner:1815]Optimizer aggregation hints are conflicted",
		},
		// additional test
		{
			sql:  "select /*+ STREAM_AGG(), USE_INDEX(t) */ distinct a from t",
			best: "TableReader(Table(t)->StreamAgg)->StreamAgg",
		},
		{
			sql:  "select /*+ HASH_AGG(), USE_INDEX(t1) */ t1.a from t t1 where t1.a < any(select t2.b from t t2)",
			best: "LeftHashJoin{TableReader(Table(t)->Sel([if(isnull(test.t1.a), <nil>, 1)]))->TableReader(Table(t)->HashAgg)->HashAgg->Sel([ne(agg_col_cnt, 0)])}->Projection->Projection",
		},
		{
			sql:  "select /*+ hash_agg(), USE_INDEX(t1) */ t1.a from t t1 where t1.a != any(select t2.b from t t2)",
			best: "LeftHashJoin{TableReader(Table(t)->Sel([if(isnull(test.t1.a), <nil>, 1)]))->TableReader(Table(t))->Projection->HashAgg->Sel([ne(agg_col_cnt, 0)])}->Projection->Projection",
		},
		{
			sql:  "select /*+ hash_agg(), USE_INDEX(t1) */ t1.a from t t1 where t1.a = all(select t2.b from t t2)",
			best: "LeftHashJoin{TableReader(Table(t))->TableReader(Table(t))->Projection->HashAgg}->Projection->Projection",
		},
		{
			sql:         "select /*+ STREAM_AGG(), USE_INDEX(t1), USE_INDEX(t2) */ sum(t1.a) from t t1 join t t2 on t1.b = t2.b group by t1.b",
			best:        "LeftHashJoin{TableReader(Table(t))->TableReader(Table(t))->Sort->Projection->StreamAgg}(test.t2.b,test.t1.b)->HashAgg",
			warning:     "[planner:1815]Optimizer Hint STREAM_AGG is inapplicable",
			aggPushDown: true,
		},
	}
	ctx := context.Background()
	for i, test := range tests {
		comment := Commentf("case:%v sql:%s", i, test)
		se.GetSessionVars().StmtCtx.SetWarnings(nil)
		se.GetSessionVars().AllowAggPushDown = test.aggPushDown

		stmt, err := s.ParseOneStmt(test.sql, "", "")
		c.Assert(err, IsNil, comment)

		p, err := planner.Optimize(ctx, se, stmt, s.is)
		c.Assert(err, IsNil)
		c.Assert(core.ToString(p), Equals, test.best, comment)

		warnings := se.GetSessionVars().StmtCtx.GetWarnings()
		if test.warning == "" {
			c.Assert(len(warnings), Equals, 0, comment)
		} else {
			c.Assert(len(warnings), Equals, 1, comment)
			c.Assert(warnings[0].Level, Equals, stmtctx.WarnLevelWarning, comment)
			c.Assert(warnings[0].Err.Error(), Equals, test.warning, comment)
		}
	}
}

func (s *testPlanSuite) TestAggToCopHint(c *C) {
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
	_, err = se.Execute(context.Background(), "insert into mysql.opt_rule_blacklist values(\"aggregation_eliminate\")")
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "admin reload opt_rule_blacklist")
	c.Assert(err, IsNil)

	tests := []struct {
		sql     string
		best    string
		warning string
	}{
		{
			sql:  "select /*+ AGG_TO_COP(), HASH_AGG(), USE_INDEX(t) */ sum(a) from t group by a",
			best: "TableReader(Table(t)->HashAgg)->HashAgg",
		},
		{
			sql:  "select /*+ AGG_TO_COP(), USE_INDEX(t) */ sum(b) from t group by b",
			best: "TableReader(Table(t)->HashAgg)->HashAgg",
		},
		{
			sql:     "select /*+ AGG_TO_COP(), HASH_AGG(), USE_INDEX(t) */ distinct a from t group by a",
			best:    "TableReader(Table(t)->HashAgg)->HashAgg->HashAgg",
			warning: "[planner:1815]Optimizer Hint AGG_TO_COP is inapplicable",
		},
		{
			sql:     "select /*+ AGG_TO_COP(), HASH_AGG(), HASH_JOIN(t1), USE_INDEX(t1), USE_INDEX(t2) */ sum(t1.a) from t t1, t t2 where t1.a = t2.b group by t1.a",
			best:    "LeftHashJoin{TableReader(Table(t))->TableReader(Table(t))}(test.t1.a,test.t2.b)->Projection->HashAgg",
			warning: "[planner:1815]Optimizer Hint AGG_TO_COP is inapplicable",
		},
	}
	ctx := context.Background()
	for i, test := range tests {
		comment := Commentf("case:%v sql:%s", i, test)
		se.GetSessionVars().StmtCtx.SetWarnings(nil)

		stmt, err := s.ParseOneStmt(test.sql, "", "")
		c.Assert(err, IsNil, comment)

		p, err := planner.Optimize(ctx, se, stmt, s.is)
		c.Assert(err, IsNil)
		c.Assert(core.ToString(p), Equals, test.best, comment)

		warnings := se.GetSessionVars().StmtCtx.GetWarnings()
		if test.warning == "" {
			c.Assert(len(warnings), Equals, 0, comment)
		} else {
			c.Assert(len(warnings), Equals, 1, comment)
			c.Assert(warnings[0].Level, Equals, stmtctx.WarnLevelWarning, comment)
			c.Assert(warnings[0].Err.Error(), Equals, test.warning, comment)
		}
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
			sql2: "select /*+ SM_JOIN(t1) */ t1.a, t1.b from t t1, (select /*+ INL_JOIN(t3) */ t2.a from t t2, t t3 where t2.a = t3.c) s where t1.a=s.a",
		},
		{
			sql1: "select /*+ TIDB_HJ(t1) */ t1.a, t1.b from t t1, (select /*+ TIDB_SMJ(t2) */ t2.a from t t2, t t3 where t2.a = t3.c) s where t1.a=s.a",
			sql2: "select /*+ HASH_JOIN(t1) */ t1.a, t1.b from t t1, (select /*+ SM_JOIN(t2) */ t2.a from t t2, t t3 where t2.a = t3.c) s where t1.a=s.a",
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

		p1, err := planner.Optimize(ctx, se, stmt1, s.is)
		c.Assert(err, IsNil)
		p2, err := planner.Optimize(ctx, se, stmt2, s.is)
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
	}
	s.testData.GetTestCases(c, &input, &output)
	ctx := context.Background()
	for i, test := range input {
		comment := Commentf("case:%v sql:%s", i, test)
		se.GetSessionVars().StmtCtx.SetWarnings(nil)

		stmt, err := s.ParseOneStmt(test, "", "")
		c.Assert(err, IsNil, comment)

		p, err := planner.Optimize(ctx, se, stmt, s.is)
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[i].SQL = test
			output[i].Best = core.ToString(p)
			output[i].HasWarn = len(se.GetSessionVars().StmtCtx.GetWarnings()) > 0
		})
		c.Assert(core.ToString(p), Equals, output[i].Best, comment)
		warnings := se.GetSessionVars().StmtCtx.GetWarnings()
		if output[i].HasWarn {
			c.Assert(warnings, HasLen, 1, comment)
		} else {
			c.Assert(warnings, HasLen, 0, comment)
		}
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

	tests := []struct {
		sql  string
		plan string
	}{
		{
			sql:  "select /*+ SM_JOIN(@sel_1 t1), INL_JOIN(@sel_2 t3) */ t1.a, t1.b from t t1, (select t2.a from t t2, t t3 where t2.a = t3.c) s where t1.a=s.a",
			plan: "MergeInnerJoin{TableReader(Table(t))->IndexJoin{TableReader(Table(t))->IndexReader(Index(t.c_d_e)[[NULL,+inf]])}(test.t2.a,test.t3.c)}(test.t1.a,test.t2.a)->Projection",
		},
		{
			sql:  "select /*+ SM_JOIN(@sel_1 t1), INL_JOIN(@qb t3) */ t1.a, t1.b from t t1, (select /*+ QB_NAME(qb) */ t2.a from t t2, t t3 where t2.a = t3.c) s where t1.a=s.a",
			plan: "MergeInnerJoin{TableReader(Table(t))->IndexJoin{TableReader(Table(t))->IndexReader(Index(t.c_d_e)[[NULL,+inf]])}(test.t2.a,test.t3.c)}(test.t1.a,test.t2.a)->Projection",
		},
		{
			sql:  "select /*+ HASH_JOIN(@sel_1 t1), SM_JOIN(@sel_2 t2) */ t1.a, t1.b from t t1, (select t2.a from t t2, t t3 where t2.a = t3.c) s where t1.a=s.a",
			plan: "RightHashJoin{TableReader(Table(t))->MergeInnerJoin{TableReader(Table(t))->IndexReader(Index(t.c_d_e)[[NULL,+inf]])}(test.t2.a,test.t3.c)}(test.t1.a,test.t2.a)->Projection",
		},
		{
			sql:  "select /*+ HASH_JOIN(@sel_1 t1), SM_JOIN(@qb t2) */ t1.a, t1.b from t t1, (select /*+ QB_NAME(qb) */ t2.a from t t2, t t3 where t2.a = t3.c) s where t1.a=s.a",
			plan: "RightHashJoin{TableReader(Table(t))->MergeInnerJoin{TableReader(Table(t))->IndexReader(Index(t.c_d_e)[[NULL,+inf]])}(test.t2.a,test.t3.c)}(test.t1.a,test.t2.a)->Projection",
		},
		{
			sql:  "select /*+ INL_JOIN(@sel_1 t1), HASH_JOIN(@sel_2 t2) */ t1.a, t1.b from t t1, (select t2.a from t t2, t t3 where t2.a = t3.c) s where t1.a=s.a",
			plan: "IndexJoin{TableReader(Table(t))->LeftHashJoin{IndexReader(Index(t.f)[[NULL,+inf]])->IndexReader(Index(t.c_d_e)[[NULL,+inf]])}(test.t2.a,test.t3.c)}(test.t2.a,test.t1.a)->Projection",
		},
		{
			sql:  "select /*+ INL_JOIN(@sel_1 t1), HASH_JOIN(@qb t2) */ t1.a, t1.b from t t1, (select /*+ QB_NAME(qb) */ t2.a from t t2, t t3 where t2.a = t3.c) s where t1.a=s.a",
			plan: "IndexJoin{TableReader(Table(t))->LeftHashJoin{IndexReader(Index(t.f)[[NULL,+inf]])->IndexReader(Index(t.c_d_e)[[NULL,+inf]])}(test.t2.a,test.t3.c)}(test.t2.a,test.t1.a)->Projection",
		},
		{
			sql:  "select /*+ HASH_AGG(@sel_1), STREAM_AGG(@sel_2) */ count(*) from t t1 where t1.a < (select count(*) from t t2 where t1.a > t2.a)",
			plan: "Apply{IndexReader(Index(t.f)[[NULL,+inf]])->IndexReader(Index(t.f)[[NULL,+inf]]->Sel([gt(test.t1.a, test.t2.a)])->StreamAgg)->StreamAgg->Sel([not(isnull(5_col_0))])}->HashAgg",
		},
		{
			sql:  "select /*+ STREAM_AGG(@sel_1), HASH_AGG(@qb) */ count(*) from t t1 where t1.a < (select /*+ QB_NAME(qb) */ count(*) from t t2 where t1.a > t2.a)",
			plan: "Apply{IndexReader(Index(t.f)[[NULL,+inf]])->IndexReader(Index(t.f)[[NULL,+inf]]->Sel([gt(test.t1.a, test.t2.a)])->HashAgg)->HashAgg->Sel([not(isnull(5_col_0))])}->StreamAgg",
		},
		{
			sql:  "select /*+ HASH_AGG(@sel_2) */ a, (select count(*) from t t1 where t1.b > t.a) from t where b > (select b from t t2 where t2.b = t.a limit 1)",
			plan: "Apply{Apply{TableReader(Table(t))->TableReader(Table(t)->Sel([eq(test.t2.b, test.t.a)])->Limit)->Limit}->TableReader(Table(t)->Sel([gt(test.t1.b, test.t.a)])->HashAgg)->HashAgg}->Projection",
		},
	}
	ctx := context.TODO()
	for i, tt := range tests {
		comment := Commentf("case:%v sql: %s", i, tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)

		p, err := planner.Optimize(ctx, se, stmt, s.is)
		c.Assert(err, IsNil, comment)

		c.Assert(core.ToString(p), Equals, tt.plan, comment)
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

		core.Preprocess(se, stmt, s.is)
		p, err := planner.Optimize(context.TODO(), se, stmt, s.is)
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
