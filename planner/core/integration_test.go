// Copyright 2019 PingCAP, Inc.
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
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testutil"
)

var _ = Suite(&testIntegrationSuite{})

type testIntegrationSuite struct {
	testData testutil.TestData
}

func (s *testIntegrationSuite) SetUpSuite(c *C) {
	var err error
	s.testData, err = testutil.LoadTestSuiteData("testdata", "integration_suite")
	c.Assert(err, IsNil)
}

func (s *testIntegrationSuite) TearDownSuite(c *C) {
	c.Assert(s.testData.GenerateOutputIfNeeded(), IsNil)
}

func (s *testIntegrationSuite) TestShowSubquery(c *C) {
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		store.Close()
	}()
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a varchar(10), b int, c int)")
	tk.MustQuery("show columns from t where true").Check(testkit.Rows(
		"a varchar(10) YES  <nil> ",
		"b int(11) YES  <nil> ",
		"c int(11) YES  <nil> ",
	))
	tk.MustQuery("show columns from t where field = 'b'").Check(testkit.Rows(
		"b int(11) YES  <nil> ",
	))
	tk.MustQuery("show columns from t where field in (select 'b')").Check(testkit.Rows(
		"b int(11) YES  <nil> ",
	))
	tk.MustQuery("show columns from t where field in (select 'b') and true").Check(testkit.Rows(
		"b int(11) YES  <nil> ",
	))
	tk.MustQuery("show columns from t where field in (select 'b') and false").Check(testkit.Rows())
	tk.MustExec("insert into t values('c', 0, 0)")
	tk.MustQuery("show columns from t where field < all (select a from t)").Check(testkit.Rows(
		"a varchar(10) YES  <nil> ",
		"b int(11) YES  <nil> ",
	))
	tk.MustExec("insert into t values('b', 0, 0)")
	tk.MustQuery("show columns from t where field < all (select a from t)").Check(testkit.Rows(
		"a varchar(10) YES  <nil> ",
	))
}

func (s *testIntegrationSuite) TestPpdWithSetVar(c *C) {
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		store.Close()
	}()
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(c1 int, c2 varchar(255))")
	tk.MustExec("insert into t values(1,'a'),(2,'d'),(3,'c')")

	tk.MustQuery("select t01.c1,t01.c2,t01.c3 from (select t1.*,@c3:=@c3+1 as c3 from (select t.*,@c3:=0 from t order by t.c1)t1)t01 where t01.c3=1 and t01.c2='d'").Check(testkit.Rows())
	tk.MustQuery("select t01.c1,t01.c2,t01.c3 from (select t1.*,@c3:=@c3+1 as c3 from (select t.*,@c3:=0 from t order by t.c1)t1)t01 where t01.c3=2 and t01.c2='d'").Check(testkit.Rows("2 d 2"))
}

func (s *testIntegrationSuite) TestBitColErrorMessage(c *C) {
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		store.Close()
	}()

	tk.MustExec("use test")
	tk.MustExec("drop table if exists bit_col_t")
	tk.MustExec("create table bit_col_t (a bit(64))")
	tk.MustExec("drop table bit_col_t")
	tk.MustExec("create table bit_col_t (a bit(1))")
	tk.MustExec("drop table bit_col_t")
	tk.MustGetErrCode("create table bit_col_t (a bit(0))", mysql.ErrInvalidFieldSize)
	tk.MustGetErrCode("create table bit_col_t (a bit(65))", mysql.ErrTooBigDisplaywidth)
}

func (s *testIntegrationSuite) TestPushLimitDownIndexLookUpReader(c *C) {
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		store.Close()
	}()

	tk.MustExec("use test")
	tk.MustExec("drop table if exists tbl")
	tk.MustExec("create table tbl(a int, b int, c int, key idx_b_c(b,c))")
	tk.MustExec("insert into tbl values(1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5)")
	tk.MustExec("analyze table tbl")
	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
	}
}
