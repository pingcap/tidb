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

package executor_test

import (
	"fmt"
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/auth"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testSuite1) TestExplainPriviliges(c *C) {
	se, err := session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil), IsTrue)
	tk := testkit.NewTestKit(c, s.store)
	tk.Se = se

	tk.MustExec("create database explaindatabase")
	tk.MustExec("use explaindatabase")
	tk.MustExec("create table t (id int)")
	tk.MustExec("create view v as select * from t")
	tk.MustExec(`create user 'explain'@'%'`)
	tk.MustExec(`flush privileges`)

	tk1 := testkit.NewTestKit(c, s.store)
	se, err = session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "explain", Hostname: "%"}, nil, nil), IsTrue)
	tk1.Se = se

	tk.MustExec(`grant select on explaindatabase.v to 'explain'@'%'`)
	tk.MustExec(`flush privileges`)
	tk1.MustQuery("show databases").Check(testkit.Rows("INFORMATION_SCHEMA", "explaindatabase"))

	tk1.MustExec("use explaindatabase")
	tk1.MustQuery("select * from v")
	err = tk1.ExecToErr("explain select * from v")
	c.Assert(err.Error(), Equals, plannercore.ErrViewNoExplain.Error())

	tk.MustExec(`grant show view on explaindatabase.v to 'explain'@'%'`)
	tk.MustExec(`flush privileges`)
	tk1.MustQuery("explain select * from v")

	tk.MustExec(`revoke select on explaindatabase.v from 'explain'@'%'`)
	tk.MustExec(`flush privileges`)

	err = tk1.ExecToErr("explain select * from v")
	c.Assert(err.Error(), Equals, plannercore.ErrTableaccessDenied.GenWithStackByArgs("SELECT", "explain", "%", "v").Error())
}

func (s *testSuite1) TestExplainCartesianJoin(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (v int)")

	cases := []struct {
		sql             string
		isCartesianJoin bool
	}{
		{"explain select * from t t1, t t2", true},
		{"explain select * from t t1 where exists (select 1 from t t2 where t2.v > t1.v)", true},
		{"explain select * from t t1 where exists (select 1 from t t2 where t2.v in (t1.v+1, t1.v+2))", true},
		{"explain select * from t t1, t t2 where t1.v = t2.v", false},
	}
	for _, ca := range cases {
		rows := tk.MustQuery(ca.sql).Rows()
		ok := false
		for _, row := range rows {
			str := fmt.Sprintf("%v", row)
			if strings.Contains(str, "CARTESIAN") {
				ok = true
			}
		}

		c.Assert(ok, Equals, ca.isCartesianJoin)
	}
}

func (s *testSuite1) TestExplainAnalyzeMemory(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (v int, k int, key(k))")
	tk.MustExec("insert into t values (1, 1), (1, 1), (1, 1), (1, 1), (1, 1)")

	s.checkMemoryInfo(c, tk, "explain analyze select * from t order by v")
	s.checkMemoryInfo(c, tk, "explain analyze select * from t order by v limit 5")
	s.checkMemoryInfo(c, tk, "explain analyze select /*+ TIDB_HJ(t1, t2) */ t1.k from t t1, t t2 where t1.v = t2.v+1")
	s.checkMemoryInfo(c, tk, "explain analyze select /*+ TIDB_SMJ(t1, t2) */ t1.k from t t1, t t2 where t1.k = t2.k+1")
	s.checkMemoryInfo(c, tk, "explain analyze select /*+ TIDB_INLJ(t1, t2) */ t1.k from t t1, t t2 where t1.k = t2.k and t1.v=1")
	s.checkMemoryInfo(c, tk, "explain analyze select sum(k) from t group by v")
	s.checkMemoryInfo(c, tk, "explain analyze select sum(v) from t group by k")
	s.checkMemoryInfo(c, tk, "explain analyze select * from t")
	s.checkMemoryInfo(c, tk, "explain analyze select k from t use index(k)")
	s.checkMemoryInfo(c, tk, "explain analyze select * from t use index(k)")
}

func (s *testSuite1) checkMemoryInfo(c *C, tk *testkit.TestKit, sql string) {
	memCol := 5
	ops := []string{"Join", "Reader", "Top", "Sort", "LookUp"}
	rows := tk.MustQuery(sql).Rows()
	for _, row := range rows {
		strs := make([]string, len(row))
		for i, c := range row {
			strs[i] = c.(string)
		}
		if strings.Contains(strs[2], "cop") {
			continue
		}

		shouldHasMem := false
		for _, op := range ops {
			if strings.Contains(strs[0], op) {
				shouldHasMem = true
				break
			}
		}

		if shouldHasMem {
			c.Assert(strs[memCol], Not(Equals), "N/A")
		} else {
			c.Assert(strs[memCol], Equals, "N/A")
		}
	}
}

func (s *testSuite1) TestExplainWrite(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int)")
	tk.MustExec("explain analyze insert into t select 1")
	tk.MustQuery("select * from t").Check(testkit.Rows("1"))
	tk.MustExec("explain analyze update t set a=2 where a=1")
	tk.MustQuery("select * from t").Check(testkit.Rows("2"))
	tk.MustExec("explain insert into t select 1")
	tk.MustQuery("select * from t").Check(testkit.Rows("2"))
	tk.MustExec("explain analyze insert into t select 1")
	tk.MustQuery("select * from t order by a").Check(testkit.Rows("1", "2"))
}
