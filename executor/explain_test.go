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
	"bytes"
	"fmt"
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/auth"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testSuite1) TestExplainPrivileges(c *C) {
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

	tk1 := testkit.NewTestKit(c, s.store)
	se, err = session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "explain", Hostname: "%"}, nil, nil), IsTrue)
	tk1.Se = se

	tk.MustExec(`grant select on explaindatabase.v to 'explain'@'%'`)
	tk1.MustQuery("show databases").Check(testkit.Rows("INFORMATION_SCHEMA", "explaindatabase"))

	tk1.MustExec("use explaindatabase")
	tk1.MustQuery("select * from v")
	err = tk1.ExecToErr("explain select * from v")
	c.Assert(err.Error(), Equals, plannercore.ErrViewNoExplain.Error())

	tk.MustExec(`grant show view on explaindatabase.v to 'explain'@'%'`)
	tk1.MustQuery("explain select * from v")

	tk.MustExec(`revoke select on explaindatabase.v from 'explain'@'%'`)

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

func (s *testSuite1) TestExplainAnalyzeMemory(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (v int, k int, key(k))")
	tk.MustExec("insert into t values (1, 1), (1, 1), (1, 1), (1, 1), (1, 1)")

	s.checkMemoryInfo(c, tk, "explain analyze select * from t order by v")
	s.checkMemoryInfo(c, tk, "explain analyze select * from t order by v limit 5")
	s.checkMemoryInfo(c, tk, "explain analyze select /*+ HASH_JOIN(t1, t2) */ t1.k from t t1, t t2 where t1.v = t2.v+1")
	s.checkMemoryInfo(c, tk, "explain analyze select /*+ MERGE_JOIN(t1, t2) */ t1.k from t t1, t t2 where t1.k = t2.k+1")
	s.checkMemoryInfo(c, tk, "explain analyze select /*+ INL_JOIN(t1, t2) */ t1.k from t t1, t t2 where t1.k = t2.k and t1.v=1")
	s.checkMemoryInfo(c, tk, "explain analyze select /*+ INL_HASH_JOIN(t1, t2) */ t1.k from t t1, t t2 where t1.k = t2.k and t1.v=1")
	s.checkMemoryInfo(c, tk, "explain analyze select /*+ INL_MERGE_JOIN(t1, t2) */ t1.k from t t1, t t2 where t1.k = t2.k and t1.v=1")
	s.checkMemoryInfo(c, tk, "explain analyze select sum(k) from t group by v")
	s.checkMemoryInfo(c, tk, "explain analyze select sum(v) from t group by k")
	s.checkMemoryInfo(c, tk, "explain analyze select * from t")
	s.checkMemoryInfo(c, tk, "explain analyze select k from t use index(k)")
	s.checkMemoryInfo(c, tk, "explain analyze select * from t use index(k)")
	s.checkMemoryInfo(c, tk, "explain analyze select v+k from t")
}

func (s *testSuite1) checkMemoryInfo(c *C, tk *testkit.TestKit, sql string) {
	memCol := 6
	ops := []string{"Join", "Reader", "Top", "Sort", "LookUp", "Projection", "Selection", "Agg"}
	rows := tk.MustQuery(sql).Rows()
	for _, row := range rows {
		strs := make([]string, len(row))
		for i, c := range row {
			strs[i] = c.(string)
		}
		if strings.Contains(strs[3], "cop") {
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

func (s *testSuite1) TestMemoryAndDiskUsageAfterClose(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (v int, k int, key(k))")
	batch := 128
	limit := tk.Se.GetSessionVars().MaxChunkSize*2 + 10
	var buf bytes.Buffer
	for i := 0; i < limit; {
		buf.Reset()
		_, err := buf.WriteString("insert into t values ")
		c.Assert(err, IsNil)
		for j := 0; j < batch && i < limit; i, j = i+1, j+1 {
			if j > 0 {
				_, err = buf.WriteString(", ")
				c.Assert(err, IsNil)
			}
			_, err = buf.WriteString(fmt.Sprintf("(%v,%v)", i, i))
			c.Assert(err, IsNil)
		}
		tk.MustExec(buf.String())
	}
	SQLs := []string{"select v+abs(k) from t",
		"select v from t where abs(v) > 0",
		"select v from t order by v",
		"select count(v) from t",            // StreamAgg
		"select count(v) from t group by v", // HashAgg
	}
	for _, sql := range SQLs {
		tk.MustQuery(sql)
		c.Assert(tk.Se.GetSessionVars().StmtCtx.MemTracker.BytesConsumed(), Equals, int64(0))
		c.Assert(tk.Se.GetSessionVars().StmtCtx.MemTracker.MaxConsumed(), Greater, int64(0))
		c.Assert(tk.Se.GetSessionVars().StmtCtx.DiskTracker.BytesConsumed(), Equals, int64(0))
	}
}

func (s *testSuite2) TestExplainAnalyzeExecutionInfo(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (v int, k int, key(k))")
	tk.MustExec("insert into t values (1, 1), (1, 1), (1, 1), (1, 1), (1, 1)")

	s.checkExecutionInfo(c, tk, "explain analyze select * from t order by v")
	s.checkExecutionInfo(c, tk, "explain analyze select * from t order by v limit 5")
	s.checkExecutionInfo(c, tk, "explain analyze select /*+ HASH_JOIN(t1, t2) */ t1.k from t t1, t t2 where t1.v = t2.v+1")
	s.checkExecutionInfo(c, tk, "explain analyze select /*+ MERGE_JOIN(t1, t2) */ t1.k from t t1, t t2 where t1.k = t2.k+1")
	s.checkExecutionInfo(c, tk, "explain analyze select /*+ INL_JOIN(t1, t2) */ t1.k from t t1, t t2 where t1.k = t2.k and t1.v=1")
	s.checkExecutionInfo(c, tk, "explain analyze select /*+ INL_HASH_JOIN(t1, t2) */ t1.k from t t1, t t2 where t1.k = t2.k and t1.v=1")
	s.checkExecutionInfo(c, tk, "explain analyze select /*+ INL_MERGE_JOIN(t1, t2) */ t1.k from t t1, t t2 where t1.k = t2.k and t1.v=1")
	s.checkExecutionInfo(c, tk, "explain analyze select sum(k) from t group by v")
	s.checkExecutionInfo(c, tk, "explain analyze select sum(v) from t group by k")
	s.checkExecutionInfo(c, tk, "explain analyze select * from t")
	s.checkExecutionInfo(c, tk, "explain analyze select k from t use index(k)")
	s.checkExecutionInfo(c, tk, "explain analyze select * from t use index(k)")

	tk.MustExec("CREATE TABLE IF NOT EXISTS nation  ( N_NATIONKEY  BIGINT NOT NULL,N_NAME       CHAR(25) NOT NULL,N_REGIONKEY  BIGINT NOT NULL,N_COMMENT    VARCHAR(152),PRIMARY KEY (N_NATIONKEY));")
	tk.MustExec("CREATE TABLE IF NOT EXISTS part  ( P_PARTKEY     BIGINT NOT NULL,P_NAME        VARCHAR(55) NOT NULL,P_MFGR        CHAR(25) NOT NULL,P_BRAND       CHAR(10) NOT NULL,P_TYPE        VARCHAR(25) NOT NULL,P_SIZE        BIGINT NOT NULL,P_CONTAINER   CHAR(10) NOT NULL,P_RETAILPRICE DECIMAL(15,2) NOT NULL,P_COMMENT     VARCHAR(23) NOT NULL,PRIMARY KEY (P_PARTKEY));")
	tk.MustExec("CREATE TABLE IF NOT EXISTS supplier  ( S_SUPPKEY     BIGINT NOT NULL,S_NAME        CHAR(25) NOT NULL,S_ADDRESS     VARCHAR(40) NOT NULL,S_NATIONKEY   BIGINT NOT NULL,S_PHONE       CHAR(15) NOT NULL,S_ACCTBAL     DECIMAL(15,2) NOT NULL,S_COMMENT     VARCHAR(101) NOT NULL,PRIMARY KEY (S_SUPPKEY),CONSTRAINT FOREIGN KEY SUPPLIER_FK1 (S_NATIONKEY) references nation(N_NATIONKEY));")
	tk.MustExec("CREATE TABLE IF NOT EXISTS partsupp ( PS_PARTKEY     BIGINT NOT NULL,PS_SUPPKEY     BIGINT NOT NULL,PS_AVAILQTY    BIGINT NOT NULL,PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL,PS_COMMENT     VARCHAR(199) NOT NULL,PRIMARY KEY (PS_PARTKEY,PS_SUPPKEY),CONSTRAINT FOREIGN KEY PARTSUPP_FK1 (PS_SUPPKEY) references supplier(S_SUPPKEY),CONSTRAINT FOREIGN KEY PARTSUPP_FK2 (PS_PARTKEY) references part(P_PARTKEY));")
	tk.MustExec("CREATE TABLE IF NOT EXISTS orders  ( O_ORDERKEY       BIGINT NOT NULL,O_CUSTKEY        BIGINT NOT NULL,O_ORDERSTATUS    CHAR(1) NOT NULL,O_TOTALPRICE     DECIMAL(15,2) NOT NULL,O_ORDERDATE      DATE NOT NULL,O_ORDERPRIORITY  CHAR(15) NOT NULL,O_CLERK          CHAR(15) NOT NULL,O_SHIPPRIORITY   BIGINT NOT NULL,O_COMMENT        VARCHAR(79) NOT NULL,PRIMARY KEY (O_ORDERKEY),CONSTRAINT FOREIGN KEY ORDERS_FK1 (O_CUSTKEY) references customer(C_CUSTKEY));")
	tk.MustExec("CREATE TABLE IF NOT EXISTS lineitem ( L_ORDERKEY    BIGINT NOT NULL,L_PARTKEY     BIGINT NOT NULL,L_SUPPKEY     BIGINT NOT NULL,L_LINENUMBER  BIGINT NOT NULL,L_QUANTITY    DECIMAL(15,2) NOT NULL,L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,L_DISCOUNT    DECIMAL(15,2) NOT NULL,L_TAX         DECIMAL(15,2) NOT NULL,L_RETURNFLAG  CHAR(1) NOT NULL,L_LINESTATUS  CHAR(1) NOT NULL,L_SHIPDATE    DATE NOT NULL,L_COMMITDATE  DATE NOT NULL,L_RECEIPTDATE DATE NOT NULL,L_SHIPINSTRUCT CHAR(25) NOT NULL,L_SHIPMODE     CHAR(10) NOT NULL,L_COMMENT      VARCHAR(44) NOT NULL,PRIMARY KEY (L_ORDERKEY,L_LINENUMBER),CONSTRAINT FOREIGN KEY LINEITEM_FK1 (L_ORDERKEY)  references orders(O_ORDERKEY),CONSTRAINT FOREIGN KEY LINEITEM_FK2 (L_PARTKEY,L_SUPPKEY) references partsupp(PS_PARTKEY, PS_SUPPKEY));")

	s.checkExecutionInfo(c, tk, "select nation, o_year, sum(amount) as sum_profit from ( select n_name as nation, extract(year from o_orderdate) as o_year, l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount from part, supplier, lineitem, partsupp, orders, nation where s_suppkey = l_suppkey and ps_suppkey = l_suppkey and ps_partkey = l_partkey and p_partkey = l_partkey and o_orderkey = l_orderkey and s_nationkey = n_nationkey and p_name like '%dim%' ) as profit group by nation, o_year order by nation, o_year desc;")

	tk.MustExec("drop table if exists nation")
	tk.MustExec("drop table if exists part")
	tk.MustExec("drop table if exists supplier")
	tk.MustExec("drop table if exists partsupp")
	tk.MustExec("drop table if exists orders")
	tk.MustExec("drop table if exists lineitem")
}

func (s *testSuite2) checkExecutionInfo(c *C, tk *testkit.TestKit, sql string) {
	executionInfoCol := 4
	rows := tk.MustQuery(sql).Rows()
	for _, row := range rows {
		strs := make([]string, len(row))
		for i, c := range row {
			strs[i] = c.(string)
		}

		c.Assert(strs[executionInfoCol], Not(Equals), "time:0s, loops:0, rows:0")
	}
}

func (s *testSuite2) TestExplainAnalyzeActRowsNotEmpty(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, index (a))")
	tk.MustExec("insert into t values (1, 1)")

	s.checkActRowsNotEmpty(c, tk, "explain analyze select * from t t1, t t2 where t1.b = t2.a and t1.b = 2333")
}

func (s *testSuite2) checkActRowsNotEmpty(c *C, tk *testkit.TestKit, sql string) {
	actRowsCol := 2
	rows := tk.MustQuery(sql).Rows()
	for _, row := range rows {
		strs := make([]string, len(row))
		for i, c := range row {
			strs[i] = c.(string)
		}

		c.Assert(strs[actRowsCol], Not(Equals), "")
	}
}
