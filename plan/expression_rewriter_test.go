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

package plan_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testExpressionRewriterSuite{})

type testExpressionRewriterSuite struct {
}

func (s *testExpressionRewriterSuite) TestCompareSubquery(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		store.Close()
	}()
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists s")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("create table s(a int, b int)")
	tk.MustExec("insert into t values(1, null), (2, null)")

	// Test empty checker.
	tk.MustQuery("select a != any (select a from s) from t").Check(testkit.Rows(
		"0",
		"0",
	))
	tk.MustQuery("select b != any (select a from s) from t").Check(testkit.Rows(
		"0",
		"0",
	))
	tk.MustQuery("select a = all (select a from s) from t").Check(testkit.Rows(
		"1",
		"1",
	))
	tk.MustQuery("select b = all (select a from s) from t").Check(testkit.Rows(
		"1",
		"1",
	))
	tk.MustQuery("select * from t where a != any (select a from s)").Check(testkit.Rows())
	tk.MustQuery("select * from t where b != any (select a from s)").Check(testkit.Rows())
	tk.MustQuery("select * from t where a = all (select a from s)").Check(testkit.Rows(
		"1 <nil>",
		"2 <nil>",
	))
	tk.MustQuery("select * from t where b = all (select a from s)").Check(testkit.Rows(
		"1 <nil>",
		"2 <nil>",
	))
	// Test outer null checker.
	tk.MustQuery("select b != any (select a from t t2) from t t1").Check(testkit.Rows(
		"<nil>",
		"<nil>",
	))
	tk.MustQuery("select b = all (select a from t t2) from t t1").Check(testkit.Rows(
		"<nil>",
		"<nil>",
	))
	tk.MustQuery("select * from t t1 where b != any (select a from t t2)").Check(testkit.Rows())
	tk.MustQuery("select * from t t1 where b = all (select a from t t2)").Check(testkit.Rows())

	tk.MustExec("delete from t where a = 2")
	tk.MustQuery("select b != any (select a from t t2) from t t1").Check(testkit.Rows(
		"<nil>",
	))
	tk.MustQuery("select b = all (select a from t t2) from t t1").Check(testkit.Rows(
		"<nil>",
	))
	tk.MustQuery("select * from t t1 where b != any (select a from t t2)").Check(testkit.Rows())
	tk.MustQuery("select * from t t1 where b = all (select a from t t2)").Check(testkit.Rows())

	// Test inner null checker.
	tk.MustExec("insert into t values(null, 1)")
	tk.MustQuery("select b != any (select a from t t2) from t t1").Check(testkit.Rows(
		"<nil>",
		"<nil>",
	))
	tk.MustQuery("select b = all (select a from t t2) from t t1").Check(testkit.Rows(
		"<nil>",
		"<nil>",
	))
	tk.MustQuery("select * from t t1 where b != any (select a from t t2)").Check(testkit.Rows())
	tk.MustQuery("select * from t t1 where b = all (select a from t t2)").Check(testkit.Rows())

	tk.MustExec("delete from t where b = 1")
	tk.MustExec("insert into t values(null, 2)")
	tk.MustQuery("select b != any (select a from t t2) from t t1").Check(testkit.Rows(
		"<nil>",
		"1",
	))
	tk.MustQuery("select b = all (select a from t t2) from t t1").Check(testkit.Rows(
		"<nil>",
		"0",
	))
	tk.MustQuery("select * from t t1 where b != any (select a from t t2)").Check(testkit.Rows(
		"<nil> 2",
	))
	tk.MustQuery("select * from t t1 where b = all (select a from t t2)").Check(testkit.Rows())
}
