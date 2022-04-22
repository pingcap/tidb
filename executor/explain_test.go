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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor_test

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/parser/auth"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestExplainPrivileges(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	require.True(t, se.Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil))
	tk := testkit.NewTestKit(t, store)
	tk.SetSession(se)

	tk.MustExec("create database explaindatabase")
	tk.MustExec("use explaindatabase")
	tk.MustExec("create table t (id int)")
	tk.MustExec("create view v as select * from t")
	tk.MustExec(`create user 'explain'@'%'`)

	tk1 := testkit.NewTestKit(t, store)
	se, err = session.CreateSession4Test(store)
	require.NoError(t, err)
	require.True(t, se.Auth(&auth.UserIdentity{Username: "explain", Hostname: "%"}, nil, nil))
	tk1.SetSession(se)

	tk.MustExec(`grant select on explaindatabase.v to 'explain'@'%'`)
	tk1.MustQuery("show databases").Check(testkit.Rows("INFORMATION_SCHEMA", "explaindatabase"))

	tk1.MustExec("use explaindatabase")
	tk1.MustQuery("select * from v")
	err = tk1.ExecToErr("explain format = 'brief' select * from v")
	require.Equal(t, plannercore.ErrViewNoExplain.Error(), err.Error())

	tk.MustExec(`grant show view on explaindatabase.v to 'explain'@'%'`)
	tk1.MustQuery("explain format = 'brief' select * from v")

	tk.MustExec(`revoke select on explaindatabase.v from 'explain'@'%'`)

	err = tk1.ExecToErr("explain format = 'brief' select * from v")
	require.Equal(t, plannercore.ErrTableaccessDenied.GenWithStackByArgs("SELECT", "explain", "%", "v").Error(), err.Error())
}

func TestExplainCartesianJoin(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (v int)")

	cases := []struct {
		sql             string
		isCartesianJoin bool
	}{
		{"explain format = 'brief' select * from t t1, t t2", true},
		{"explain format = 'brief' select * from t t1 where exists (select 1 from t t2 where t2.v > t1.v)", true},
		{"explain format = 'brief' select * from t t1 where exists (select 1 from t t2 where t2.v in (t1.v+1, t1.v+2))", true},
		{"explain format = 'brief' select * from t t1, t t2 where t1.v = t2.v", false},
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

		require.Equal(t, ca.isCartesianJoin, ok)
	}
}

func TestExplainWrite(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int)")
	tk.MustQuery("explain analyze insert into t select 1")
	tk.MustQuery("select * from t").Check(testkit.Rows("1"))
	tk.MustQuery("explain analyze update t set a=2 where a=1")
	tk.MustQuery("select * from t").Check(testkit.Rows("2"))
	tk.MustQuery("explain format = 'brief' insert into t select 1")
	tk.MustQuery("select * from t").Check(testkit.Rows("2"))
	tk.MustQuery("explain analyze insert into t select 1")
	tk.MustQuery("explain analyze replace into t values (3)")
	tk.MustQuery("select * from t order by a").Check(testkit.Rows("1", "2", "3"))
}

func TestExplainAnalyzeMemory(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (v int, k int, key(k))")
	tk.MustExec("insert into t values (1, 1), (1, 1), (1, 1), (1, 1), (1, 1)")

	checkMemoryInfo(t, tk, "explain analyze select * from t order by v")
	checkMemoryInfo(t, tk, "explain analyze select * from t order by v limit 5")
	checkMemoryInfo(t, tk, "explain analyze select /*+ HASH_JOIN(t1, t2) */ t1.k from t t1, t t2 where t1.v = t2.v+1")
	checkMemoryInfo(t, tk, "explain analyze select /*+ MERGE_JOIN(t1, t2) */ t1.k from t t1, t t2 where t1.k = t2.k+1")
	checkMemoryInfo(t, tk, "explain analyze select /*+ INL_JOIN(t1, t2) */ t1.k from t t1, t t2 where t1.k = t2.k and t1.v=1")
	checkMemoryInfo(t, tk, "explain analyze select /*+ INL_HASH_JOIN(t1, t2) */ t1.k from t t1, t t2 where t1.k = t2.k and t1.v=1")
	checkMemoryInfo(t, tk, "explain analyze select /*+ INL_MERGE_JOIN(t1, t2) */ t1.k from t t1, t t2 where t1.k = t2.k and t1.v=1")
	checkMemoryInfo(t, tk, "explain analyze select sum(k) from t group by v")
	checkMemoryInfo(t, tk, "explain analyze select sum(v) from t group by k")
	checkMemoryInfo(t, tk, "explain analyze select * from t")
	checkMemoryInfo(t, tk, "explain analyze select k from t use index(k)")
	checkMemoryInfo(t, tk, "explain analyze select * from t use index(k)")
	checkMemoryInfo(t, tk, "explain analyze select v+k from t")
}

func checkMemoryInfo(t *testing.T, tk *testkit.TestKit, sql string) {
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
			require.NotEqual(t, "N/A", strs[memCol])
		} else {
			require.Equal(t, "N/A", strs[memCol])
		}
	}
}

func TestMemoryAndDiskUsageAfterClose(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (v int, k int, key(k))")
	batch := 128
	limit := tk.Session().GetSessionVars().MaxChunkSize*2 + 10
	var buf bytes.Buffer
	for i := 0; i < limit; {
		buf.Reset()
		_, err := buf.WriteString("insert into t values ")
		require.NoError(t, err)
		for j := 0; j < batch && i < limit; i, j = i+1, j+1 {
			if j > 0 {
				_, err = buf.WriteString(", ")
				require.NoError(t, err)
			}
			_, err = buf.WriteString(fmt.Sprintf("(%v,%v)", i, i))
			require.NoError(t, err)
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
		require.Equal(t, int64(0), tk.Session().GetSessionVars().StmtCtx.MemTracker.BytesConsumed())
		require.Greater(t, tk.Session().GetSessionVars().StmtCtx.MemTracker.MaxConsumed(), int64(0))
		require.Equal(t, int64(0), tk.Session().GetSessionVars().StmtCtx.DiskTracker.BytesConsumed())
	}
}

func TestExplainAnalyzeExecutionInfo(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (v int, k int, key(k))")
	tk.MustExec("insert into t values (1, 1), (1, 1), (1, 1), (1, 1), (1, 1)")

	checkExecutionInfo(t, tk, "explain analyze select * from t order by v")
	checkExecutionInfo(t, tk, "explain analyze select * from t order by v limit 5")
	checkExecutionInfo(t, tk, "explain analyze select /*+ HASH_JOIN(t1, t2) */ t1.k from t t1, t t2 where t1.v = t2.v+1")
	checkExecutionInfo(t, tk, "explain analyze select /*+ MERGE_JOIN(t1, t2) */ t1.k from t t1, t t2 where t1.k = t2.k+1")
	checkExecutionInfo(t, tk, "explain analyze select /*+ INL_JOIN(t1, t2) */ t1.k from t t1, t t2 where t1.k = t2.k and t1.v=1")
	checkExecutionInfo(t, tk, "explain analyze select /*+ INL_HASH_JOIN(t1, t2) */ t1.k from t t1, t t2 where t1.k = t2.k and t1.v=1")
	checkExecutionInfo(t, tk, "explain analyze select /*+ INL_MERGE_JOIN(t1, t2) */ t1.k from t t1, t t2 where t1.k = t2.k and t1.v=1")
	checkExecutionInfo(t, tk, "explain analyze select sum(k) from t group by v")
	checkExecutionInfo(t, tk, "explain analyze select sum(v) from t group by k")
	checkExecutionInfo(t, tk, "explain analyze select * from t")
	checkExecutionInfo(t, tk, "explain analyze select k from t use index(k)")
	checkExecutionInfo(t, tk, "explain analyze select * from t use index(k)")
	checkExecutionInfo(t, tk, "explain analyze with recursive cte(a) as (select 1 union select a + 1 from cte where a < 1000) select * from cte;")

	tk.MustExec("CREATE TABLE IF NOT EXISTS nation  ( N_NATIONKEY  BIGINT NOT NULL,N_NAME       CHAR(25) NOT NULL,N_REGIONKEY  BIGINT NOT NULL,N_COMMENT    VARCHAR(152),PRIMARY KEY (N_NATIONKEY));")
	tk.MustExec("CREATE TABLE IF NOT EXISTS part  ( P_PARTKEY     BIGINT NOT NULL,P_NAME        VARCHAR(55) NOT NULL,P_MFGR        CHAR(25) NOT NULL,P_BRAND       CHAR(10) NOT NULL,P_TYPE        VARCHAR(25) NOT NULL,P_SIZE        BIGINT NOT NULL,P_CONTAINER   CHAR(10) NOT NULL,P_RETAILPRICE DECIMAL(15,2) NOT NULL,P_COMMENT     VARCHAR(23) NOT NULL,PRIMARY KEY (P_PARTKEY));")
	tk.MustExec("CREATE TABLE IF NOT EXISTS supplier  ( S_SUPPKEY     BIGINT NOT NULL,S_NAME        CHAR(25) NOT NULL,S_ADDRESS     VARCHAR(40) NOT NULL,S_NATIONKEY   BIGINT NOT NULL,S_PHONE       CHAR(15) NOT NULL,S_ACCTBAL     DECIMAL(15,2) NOT NULL,S_COMMENT     VARCHAR(101) NOT NULL,PRIMARY KEY (S_SUPPKEY),CONSTRAINT FOREIGN KEY SUPPLIER_FK1 (S_NATIONKEY) references nation(N_NATIONKEY));")
	tk.MustExec("CREATE TABLE IF NOT EXISTS partsupp ( PS_PARTKEY     BIGINT NOT NULL,PS_SUPPKEY     BIGINT NOT NULL,PS_AVAILQTY    BIGINT NOT NULL,PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL,PS_COMMENT     VARCHAR(199) NOT NULL,PRIMARY KEY (PS_PARTKEY,PS_SUPPKEY),CONSTRAINT FOREIGN KEY PARTSUPP_FK1 (PS_SUPPKEY) references supplier(S_SUPPKEY),CONSTRAINT FOREIGN KEY PARTSUPP_FK2 (PS_PARTKEY) references part(P_PARTKEY));")
	tk.MustExec("CREATE TABLE IF NOT EXISTS orders  ( O_ORDERKEY       BIGINT NOT NULL,O_CUSTKEY        BIGINT NOT NULL,O_ORDERSTATUS    CHAR(1) NOT NULL,O_TOTALPRICE     DECIMAL(15,2) NOT NULL,O_ORDERDATE      DATE NOT NULL,O_ORDERPRIORITY  CHAR(15) NOT NULL,O_CLERK          CHAR(15) NOT NULL,O_SHIPPRIORITY   BIGINT NOT NULL,O_COMMENT        VARCHAR(79) NOT NULL,PRIMARY KEY (O_ORDERKEY),CONSTRAINT FOREIGN KEY ORDERS_FK1 (O_CUSTKEY) references customer(C_CUSTKEY));")
	tk.MustExec("CREATE TABLE IF NOT EXISTS lineitem ( L_ORDERKEY    BIGINT NOT NULL,L_PARTKEY     BIGINT NOT NULL,L_SUPPKEY     BIGINT NOT NULL,L_LINENUMBER  BIGINT NOT NULL,L_QUANTITY    DECIMAL(15,2) NOT NULL,L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,L_DISCOUNT    DECIMAL(15,2) NOT NULL,L_TAX         DECIMAL(15,2) NOT NULL,L_RETURNFLAG  CHAR(1) NOT NULL,L_LINESTATUS  CHAR(1) NOT NULL,L_SHIPDATE    DATE NOT NULL,L_COMMITDATE  DATE NOT NULL,L_RECEIPTDATE DATE NOT NULL,L_SHIPINSTRUCT CHAR(25) NOT NULL,L_SHIPMODE     CHAR(10) NOT NULL,L_COMMENT      VARCHAR(44) NOT NULL,PRIMARY KEY (L_ORDERKEY,L_LINENUMBER),CONSTRAINT FOREIGN KEY LINEITEM_FK1 (L_ORDERKEY)  references orders(O_ORDERKEY),CONSTRAINT FOREIGN KEY LINEITEM_FK2 (L_PARTKEY,L_SUPPKEY) references partsupp(PS_PARTKEY, PS_SUPPKEY));")

	checkExecutionInfo(t, tk, "select nation, o_year, sum(amount) as sum_profit from ( select n_name as nation, extract(year from o_orderdate) as o_year, l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount from part, supplier, lineitem, partsupp, orders, nation where s_suppkey = l_suppkey and ps_suppkey = l_suppkey and ps_partkey = l_partkey and p_partkey = l_partkey and o_orderkey = l_orderkey and s_nationkey = n_nationkey and p_name like '%dim%' ) as profit group by nation, o_year order by nation, o_year desc;")

	tk.MustExec("drop table if exists nation")
	tk.MustExec("drop table if exists part")
	tk.MustExec("drop table if exists supplier")
	tk.MustExec("drop table if exists partsupp")
	tk.MustExec("drop table if exists orders")
	tk.MustExec("drop table if exists lineitem")
}

func checkExecutionInfo(t *testing.T, tk *testkit.TestKit, sql string) {
	executionInfoCol := 4
	rows := tk.MustQuery(sql).Rows()
	for _, row := range rows {
		strs := make([]string, len(row))
		for i, c := range row {
			strs[i] = c.(string)
		}
		require.NotEqual(t, "time:0s, loops:0, rows:0", strs[executionInfoCol])
	}
}

func TestExplainAnalyzeActRowsNotEmpty(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, index (a))")
	tk.MustExec("insert into t values (1, 1)")

	checkActRowsNotEmpty(t, tk, "explain analyze select * from t t1, t t2 where t1.b = t2.a and t1.b = 2333")
}

func checkActRowsNotEmpty(t *testing.T, tk *testkit.TestKit, sql string) {
	actRowsCol := 2
	rows := tk.MustQuery(sql).Rows()
	for _, row := range rows {
		strs := make([]string, len(row))
		for i, c := range row {
			strs[i] = c.(string)
		}
		require.NotEqual(t, "", strs[actRowsCol])
	}
}

func checkActRows(t *testing.T, tk *testkit.TestKit, sql string, expected []string) {
	actRowsCol := 2
	rows := tk.MustQuery("explain analyze " + sql).Rows()
	require.Equal(t, len(expected), len(rows))
	for id, row := range rows {
		strs := make([]string, len(row))
		for i, c := range row {
			strs[i] = c.(string)
		}

		require.Equal(t, expected[id], strs[actRowsCol], fmt.Sprintf("error comparing %s", sql))
	}
}

func TestCheckActRowsWithUnistore(t *testing.T) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.EnableCollectExecutionInfo = true
	})
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	// testSuite1 use default mockstore which is unistore
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_unistore_act_rows")
	tk.MustExec("create table t_unistore_act_rows(a int, b int, index(a, b))")
	tk.MustExec("insert into t_unistore_act_rows values (1, 0), (1, 0), (2, 0), (2, 1)")
	tk.MustExec("analyze table t_unistore_act_rows")

	type testStruct struct {
		sql      string
		expected []string
	}

	tests := []testStruct{
		{
			sql:      "select * from t_unistore_act_rows",
			expected: []string{"4", "4"},
		},
		{
			sql:      "select * from t_unistore_act_rows where a > 1",
			expected: []string{"2", "2"},
		},
		{
			sql:      "select * from t_unistore_act_rows where a > 1 and b > 0",
			expected: []string{"1", "1", "2"},
		},
		{
			sql:      "select b from t_unistore_act_rows",
			expected: []string{"4", "4"},
		},
		{
			sql:      "select * from t_unistore_act_rows where b > 0",
			expected: []string{"1", "1", "4"},
		},
		{
			sql:      "select count(*) from t_unistore_act_rows",
			expected: []string{"1", "1", "1", "4"},
		},
		{
			sql:      "select count(*) from t_unistore_act_rows group by a",
			expected: []string{"2", "2", "2", "4"},
		},
		{
			sql:      "select count(*) from t_unistore_act_rows group by b",
			expected: []string{"2", "2", "2", "4"},
		},
		{
			sql:      "with cte(a) as (select a from t_unistore_act_rows) select (select 1 from cte limit 1) from cte;",
			expected: []string{"4", "4", "4", "4", "4"},
		},
	}

	for _, test := range tests {
		checkActRows(t, tk, test.sql, test.expected)
	}
}

func TestExplainAnalyzeCTEMemoryAndDiskInfo(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int)")
	tk.MustExec("insert into t with recursive cte(a) as (select 1 union select a + 1 from cte where a < 1000) select * from cte;")

	rows := tk.MustQuery("explain analyze with recursive cte(a) as (select 1 union select a + 1 from cte where a < 1000)" +
		" select * from cte, t;").Rows()

	require.NotEqual(t, "N/A", rows[4][7].(string))
	require.Equal(t, "0 Bytes", rows[4][8].(string))

	tk.MustExec("set @@tidb_mem_quota_query=10240;")
	rows = tk.MustQuery("explain analyze with recursive cte(a) as (select 1 union select a + 1 from cte where a < 1000)" +
		" select * from cte, t;").Rows()

	require.NotEqual(t, "N/A", rows[4][7].(string))
	require.NotEqual(t, "N/A", rows[4][8].(string))
}

func TestExplainStatementsSummary(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustQuery("desc select * from information_schema.statements_summary").Check(testkit.Rows(
		`MemTableScan_4 10000.00 root table:STATEMENTS_SUMMARY `))
	tk.MustQuery("desc select * from information_schema.statements_summary where digest is null").Check(testkit.RowsWithSep("|",
		`Selection_5|8000.00|root| isnull(Column#5)`, `└─MemTableScan_6|10000.00|root|table:STATEMENTS_SUMMARY|`))
	tk.MustQuery("desc select * from information_schema.statements_summary where digest = 'abcdefg'").Check(testkit.RowsWithSep(" ",
		`MemTableScan_5 10000.00 root table:STATEMENTS_SUMMARY digests: ["abcdefg"]`))
	tk.MustQuery("desc select * from information_schema.statements_summary where digest in ('a','b','c')").Check(testkit.RowsWithSep(" ",
		`MemTableScan_5 10000.00 root table:STATEMENTS_SUMMARY digests: ["a","b","c"]`))
}

func TestFix29401(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists tt123;")
	tk.MustExec(`CREATE TABLE tt123 (
  id int(11) NOT NULL,
  a bigint(20) DEFAULT NULL,
  b char(20) DEFAULT NULL,
  c datetime DEFAULT NULL,
  d double DEFAULT NULL,
  e json DEFAULT NULL,
  f decimal(40,6) DEFAULT NULL,
  PRIMARY KEY (id) /*T![clustered_index] CLUSTERED */,
  KEY a (a),
  KEY b (b),
  KEY c (c),
  KEY d (d),
  KEY f (f)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;`)
	tk.MustExec(" explain select /*+ inl_hash_join(t1) */ * from tt123 t1 join tt123 t2 on t1.b=t2.e;")
}
