// Copyright 2020 PingCAP, Inc.
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

package executor_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testkit"
)

var _ = SerialSuites(&testCollationSuite{})

type testCollationSuite struct {
	store kv.Storage
	dom   *domain.Domain
	ctx   sessionctx.Context
}

func (s *testCollationSuite) SetUpSuite(c *C) {
	var err error
	s.store, s.dom, err = newStoreWithBootstrap()
	c.Assert(err, IsNil)
	s.ctx = mock.NewContext()
}

func (s *testCollationSuite) prepare4Join(c *C) *testkit.TestKit {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("USE test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE `t` ( `a` int(11) NOT NULL,`b` varchar(5) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL)")
	tk.MustExec("CREATE TABLE `t_bin` ( `a` int(11) NOT NULL,`b` varchar(5) CHARACTER SET binary)")
	tk.MustExec("insert into t values (1, 'a'), (2, 'À'), (3, 'á'), (4, 'à'), (5, 'b'), (6, 'c'), (7, ' ')")
	return tk
}

func (s *testCollationSuite) TestHashJoin(c *C) {
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)
	tk := s.prepare4Join(c)
	tk.MustExec("insert into t_bin values (1, 'a'), (2, 'À'), (3, 'á'), (4, 'à'), (5, 'b'), (6, 'c'), (7, ' ')")
	tk.MustQuery("select /*+ TIDB_HJ(t1, t2) */ t1.a, t1.b from t t1, t t2 where t1.b=t2.b order by t1.a").Check(
		testkit.Rows("1 a", "1 a", "1 a", "1 a", "2 À", "2 À", "2 À", "2 À", "3 á", "3 á", "3 á", "3 á", "4 à", "4 à", "4 à", "4 à", "5 b", "6 c", "7  "))
	tk.MustQuery("select /*+ TIDB_HJ(t1, t2) */ t1.a, t1.b from t_bin t1, t_bin t2 where t1.b=t2.b order by t1.a").Check(
		testkit.Rows("1 a", "2 À", "3 á", "4 à", "5 b", "6 c", "7  "))
	tk.MustQuery("select /*+ TIDB_HJ(t1, t2) */ t1.a, t1.b from t t1, t t2 where t1.b=t2.b and t1.a>3 order by t1.a").Check(
		testkit.Rows("4 à", "4 à", "4 à", "4 à", "5 b", "6 c", "7  "))
	tk.MustQuery("select /*+ TIDB_HJ(t1, t2) */ t1.a, t1.b from t_bin t1, t_bin t2 where t1.b=t2.b and t1.a>3 order by t1.a").Check(
		testkit.Rows("4 à", "5 b", "6 c", "7  "))
	tk.MustQuery("select /*+ TIDB_HJ(t1, t2) */ t1.a, t1.b from t t1, t t2 where t1.b=t2.b and t1.a>3 order by t1.a").Check(
		testkit.Rows("4 à", "4 à", "4 à", "4 à", "5 b", "6 c", "7  "))
	tk.MustQuery("select /*+ TIDB_HJ(t1, t2) */ t1.a, t1.b from t_bin t1, t_bin t2 where t1.b=t2.b and t1.a>3 order by t1.a").Check(
		testkit.Rows("4 à", "5 b", "6 c", "7  "))
	tk.MustQuery("select /*+ TIDB_HJ(t1, t2) */ t1.a, t1.b from t t1, t t2 where t1.b=t2.b and t1.a>t2.a order by t1.a").Check(
		testkit.Rows("2 À", "3 á", "3 á", "4 à", "4 à", "4 à"))
	tk.MustQuery("select /*+ TIDB_HJ(t1, t2) */ t1.a, t1.b from t_bin t1, t_bin t2 where t1.b=t2.b and t1.a>t2.a order by t1.a").Check(
		testkit.Rows())
}

func (s *testCollationSuite) TestMergeJoin(c *C) {
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)
	tk := s.prepare4Join(c)
	tk.MustExec("insert into t_bin values (1, 'a'), (2, 'À'), (3, 'á'), (4, 'à'), (5, 'b'), (6, 'c'), (7, ' ')")
	tk.MustQuery("select /*+ TIDB_SMJ(t1, t2) */ t1.a, t1.b from t t1, t t2 where t1.b=t2.b order by t1.a").Check(
		testkit.Rows("1 a", "1 a", "1 a", "1 a", "2 À", "2 À", "2 À", "2 À", "3 á", "3 á", "3 á", "3 á", "4 à", "4 à", "4 à", "4 à", "5 b", "6 c", "7  "))
	tk.MustQuery("select /*+ TIDB_SMJ(t1, t2) */ t1.a, t1.b from t_bin t1, t_bin t2 where t1.b=t2.b order by t1.a").Check(
		testkit.Rows("1 a", "2 À", "3 á", "4 à", "5 b", "6 c", "7  "))
	tk.MustQuery("select /*+ TIDB_SMJ(t1, t2) */ t1.a, t1.b from t t1, t t2 where t1.b=t2.b and t1.a>3 order by t1.a").Check(
		testkit.Rows("4 à", "4 à", "4 à", "4 à", "5 b", "6 c", "7  "))
	tk.MustQuery("select /*+ TIDB_SMJ(t1, t2) */ t1.a, t1.b from t_bin t1, t_bin t2 where t1.b=t2.b and t1.a>3 order by t1.a").Check(
		testkit.Rows("4 à", "5 b", "6 c", "7  "))
	tk.MustQuery("select /*+ TIDB_SMJ(t1, t2) */ t1.a, t1.b from t t1, t t2 where t1.b=t2.b and t1.a>3 order by t1.a").Check(
		testkit.Rows("4 à", "4 à", "4 à", "4 à", "5 b", "6 c", "7  "))
	tk.MustQuery("select /*+ TIDB_SMJ(t1, t2) */ t1.a, t1.b from t_bin t1, t_bin t2 where t1.b=t2.b and t1.a>3 order by t1.a").Check(
		testkit.Rows("4 à", "5 b", "6 c", "7  "))
	tk.MustQuery("select /*+ TIDB_SMJ(t1, t2) */ t1.a, t1.b from t t1, t t2 where t1.b=t2.b and t1.a>t2.a order by t1.a").Check(
		testkit.Rows("2 À", "3 á", "3 á", "4 à", "4 à", "4 à"))
	tk.MustQuery("select /*+ TIDB_SMJ(t1, t2) */ t1.a, t1.b from t_bin t1, t_bin t2 where t1.b=t2.b and t1.a>t2.a order by t1.a").Check(
		testkit.Rows())
}

func (s *testCollationSuite) TestSelection(c *C) {
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("USE test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int, v varchar(5) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL)")
	tk.MustExec("create table t_bin (id int, v varchar(5) CHARACTER SET binary);")
	tk.MustExec("insert into t values (1, 'a'), (2, 'À'), (3, 'á'), (4, 'à'), (5, 'b'), (6, 'c'), (7, ' ')")
	tk.MustExec("insert into t_bin values (1, 'a'), (2, 'À'), (3, 'á'), (4, 'à'), (5, 'b'), (6, 'c'), (7, ' ')")
	tk.MustQuery("select v from t where v='a' order by id").Check(testkit.Rows("a", "À", "á", "à"))
	tk.MustQuery("select v from t_bin where v='a' order by id").Check(testkit.Rows("a"))
	tk.MustQuery("select v from t where v<'b' and id<=3").Check(testkit.Rows("a", "À", "á"))
	tk.MustQuery("select v from t_bin where v<'b' and id<=3").Check(testkit.Rows("a"))
}
