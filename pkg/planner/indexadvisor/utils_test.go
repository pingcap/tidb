// Copyright 2024 PingCAP, Inc.
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

package indexadvisor_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/planner/indexadvisor"
	"github.com/pingcap/tidb/pkg/testkit"
	s "github.com/pingcap/tidb/pkg/util/set"
	"github.com/stretchr/testify/require"
)

func TestCollectTableFromQuery(t *testing.T) {
	names, err := indexadvisor.CollectTableNamesFromQuery("test", "select * from t where a = 1")
	require.NoError(t, err)
	require.Equal(t, names[0], "test.t")

	names, err = indexadvisor.CollectTableNamesFromQuery("test", "select * from t1, t2")
	require.NoError(t, err)
	require.Equal(t, names[0], "test.t1")
	require.Equal(t, names[1], "test.t2")

	names, err = indexadvisor.CollectTableNamesFromQuery("test", "select * from t1 where t1.a < (select max(b) from t2)")
	require.NoError(t, err)
	require.Equal(t, names[0], "test.t1")
	require.Equal(t, names[1], "test.t2")

	names, err = indexadvisor.CollectTableNamesFromQuery("test", "select * from t1 where t1.a < (select max(b) from db2.t2)")
	require.NoError(t, err)
	require.Equal(t, names[0], "test.t1")
	require.Equal(t, names[1], "db2.t2")
}

func TestCollectSelectColumnsFromQuery(t *testing.T) {
	names, err := indexadvisor.CollectSelectColumnsFromQuery(indexadvisor.Query{Text: "select a, b from test.t"})
	require.NoError(t, err)
	require.True(t, names.String() == "{test.t.a, test.t.b}")

	names, err = indexadvisor.CollectSelectColumnsFromQuery(indexadvisor.Query{Text: "select a, b, c from test.t"})
	require.NoError(t, err)
	require.True(t, names.String() == "{test.t.a, test.t.b, test.t.c}")
}

func TestCollectOrderByColumnsFromQuery(t *testing.T) {
	cols, err := indexadvisor.CollectOrderByColumnsFromQuery(indexadvisor.Query{Text: "select a, b from test.t order by a"})
	require.NoError(t, err)
	require.Equal(t, len(cols), 1)
	require.Equal(t, cols[0].Key(), "test.t.a")

	cols, err = indexadvisor.CollectOrderByColumnsFromQuery(indexadvisor.Query{Text: "select a, b from test.t order by a, b"})
	require.NoError(t, err)
	require.Equal(t, len(cols), 2)
	require.Equal(t, cols[0].Key(), "test.t.a")
	require.Equal(t, cols[1].Key(), "test.t.b")
}

func TestCollectDNFColumnsFromQuery(t *testing.T) {
	cols, err := indexadvisor.CollectDNFColumnsFromQuery(indexadvisor.Query{Text: "select a, b from test.t where a = 1 or b = 2"})
	require.NoError(t, err)
	require.Equal(t, cols.String(), "{test.t.a, test.t.b}")

	cols, err = indexadvisor.CollectDNFColumnsFromQuery(indexadvisor.Query{Text: "select a, b from test.t where a = 1 or b = 2 or c=3"})
	require.NoError(t, err)
	require.Equal(t, cols.String(), "{test.t.a, test.t.b, test.t.c}")
}

func TestRestoreSchemaName(t *testing.T) {
	q1 := indexadvisor.Query{Text: "select * from t1"}
	q2 := indexadvisor.Query{Text: "select * from t2", SchemaName: "test2"}
	q3 := indexadvisor.Query{Text: "select * from t3"}
	q4 := indexadvisor.Query{Text: "wrong"}
	set1 := s.NewSet[indexadvisor.Query]()
	set1.Add(q1, q2, q3, q4)

	set2, err := indexadvisor.RestoreSchemaName("test", set1, true)
	require.NoError(t, err)
	require.Equal(t, set2.String(), "{SELECT * FROM `test2`.`t2`, SELECT * FROM `test`.`t1`, SELECT * FROM `test`.`t3`}")

	_, err = indexadvisor.RestoreSchemaName("test", set1, false)
	require.Error(t, err)
}

func TestFilterSQLAccessingSystemTables(t *testing.T) {
	set1 := s.NewSet[indexadvisor.Query]()
	set1.Add(indexadvisor.Query{Text: "select * from mysql.stats_meta"})
	set1.Add(indexadvisor.Query{Text: "select * from information_schema.test"})
	set1.Add(indexadvisor.Query{Text: "select * from metrics_schema.test"})
	set1.Add(indexadvisor.Query{Text: "select * from performance_schema.test"})
	set1.Add(indexadvisor.Query{Text: "select * from mysql.stats_meta", SchemaName: "test"})
	set1.Add(indexadvisor.Query{Text: "select * from mysql.stats_meta, test.t1", SchemaName: "test"})
	set1.Add(indexadvisor.Query{Text: "select * from test.t1", SchemaName: "mysql"})
	set1.Add(indexadvisor.Query{Text: "select @@var", SchemaName: "test"})
	set1.Add(indexadvisor.Query{Text: "select sleep(1)", SchemaName: "test"})
	set1.Add(indexadvisor.Query{Text: "wrong", SchemaName: "information_schema"})

	set2, err := indexadvisor.FilterSQLAccessingSystemTables(set1, true)
	require.NoError(t, err)
	require.Equal(t, set2.String(), "{select * from test.t1}")

	_, err = indexadvisor.FilterSQLAccessingSystemTables(set1, false)
	require.Error(t, err)
}

func TestFilterInvalidQueries(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t1 (a int, b int, c int)`)
	tk.MustExec(`create table t2 (a int, b int, c int)`)
	opt := indexadvisor.NewOptimizer(tk.Session())

	set1 := s.NewSet[indexadvisor.Query]()
	set1.Add(indexadvisor.Query{Text: "select * from test.t1"})
	set1.Add(indexadvisor.Query{Text: "select * from test.t3"})                            // table t3 does not exist
	set1.Add(indexadvisor.Query{Text: "select d from t1"})                                 // column d does not exist
	set1.Add(indexadvisor.Query{Text: "select * from t1 where a<(select max(b) from t2)"}) // Fix43817
	set1.Add(indexadvisor.Query{Text: "wrong"})                                            // invalid query

	set2, err := indexadvisor.FilterInvalidQueries(opt, set1, true)
	require.NoError(t, err)
	require.Equal(t, set2.String(), "{select * from test.t1}")

	_, err = indexadvisor.FilterInvalidQueries(opt, set1, false)
	require.Error(t, err)
}

func TestCollectIndexableColumnsForQuerySet(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b int, c int, d int, e int, f int)`)
	opt := indexadvisor.NewOptimizer(tk.Session())

	set1 := s.NewSet[indexadvisor.Query]()
	set1.Add(indexadvisor.Query{Text: "select * from test.t where a=1 and b=1 and e like 'abc'"})
	set1.Add(indexadvisor.Query{Text: "select * from test.t where a<1 and b>1 and e like 'abc'"})
	set1.Add(indexadvisor.Query{Text: "select * from test.t where c in (1, 2, 3) order by d"})
	set1.Add(indexadvisor.Query{Text: "select 1 from test.t where c in (1, 2, 3) group by e"})

	set2, err := indexadvisor.CollectIndexableColumnsForQuerySet(opt, set1)
	require.NoError(t, err)
	require.Equal(t, "{test.t.a, test.t.b, test.t.c, test.t.d, test.t.e}", set2.String())
}

func TestCollectIndexableColumnsFromQuery(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b int, c int, d int, e int)`)
	opt := indexadvisor.NewOptimizer(tk.Session())

	cols, err := indexadvisor.CollectIndexableColumnsFromQuery(
		indexadvisor.Query{SchemaName: "test", Text: "select * from t where a<1 and b>1 and e like 'abc'"}, opt)
	require.NoError(t, err)
	require.Equal(t, cols.String(), "{test.t.a, test.t.b}")

	cols, err = indexadvisor.CollectIndexableColumnsFromQuery(
		indexadvisor.Query{SchemaName: "test", Text: "select * from t where c in (1, 2, 3) order by d"}, opt)
	require.NoError(t, err)
	require.Equal(t, cols.String(), "{test.t.c, test.t.d}")

	cols, err = indexadvisor.CollectIndexableColumnsFromQuery(
		indexadvisor.Query{SchemaName: "test", Text: "select 1 from t where c in (1, 2, 3) group by d"}, opt)
	require.NoError(t, err)
	require.Equal(t, cols.String(), "{test.t.c, test.t.d}")

	tk.MustExec("drop table t")

	tk.MustExec(`create table t1 (a int)`)
	tk.MustExec(`create table t2 (a int)`)
	cols, err = indexadvisor.CollectIndexableColumnsFromQuery(
		indexadvisor.Query{SchemaName: "test", Text: "select * from t2 tx where a<1"}, opt)
	require.NoError(t, err)
	require.Equal(t, cols.String(), "{test.t1.a, test.t2.a}")
	tk.MustExec("drop table t1")
	tk.MustExec("drop table t2")

	tk.MustExec(`create database tpch`)
	tk.MustExec(`use tpch`)
	tk.MustExec(`CREATE TABLE tpch.nation ( N_NATIONKEY bigint(20) NOT NULL,
		N_NAME char(25) NOT NULL, N_REGIONKEY bigint(20) NOT NULL, N_COMMENT varchar(152) DEFAULT NULL,
		PRIMARY KEY (N_NATIONKEY) /*T![clustered_index] CLUSTERED */)`)
	q := ` select supp_nation, cust_nation, l_year, sum(volume) as revenue from
	( select n1.n_name as supp_nation, n2.n_name as cust_nation,
			extract(year from l_shipdate) as l_year, l_extendedprice * (1 - l_discount) as volume
		from supplier, lineitem, orders, customer, nation n1, nation n2
		where s_suppkey = l_suppkey and o_orderkey = l_orderkey and c_custkey = o_custkey
		  and s_nationkey = n1.n_nationkey and c_nationkey = n2.n_nationkey
			and ( (n1.n_name = 'MOZAMBIQUE' and n2.n_name = 'UNITED KINGDOM')
				or (n1.n_name = 'UNITED KINGDOM' and n2.n_name = 'MOZAMBIQUE')
			) and l_shipdate between date '1995-01-01' and date '1996-12-31'
	) as shipping group by supp_nation, cust_nation, l_year
	order by supp_nation, cust_nation, l_year`
	cols, err = indexadvisor.CollectIndexableColumnsFromQuery(
		indexadvisor.Query{SchemaName: "tpch", Text: q}, opt)
	require.NoError(t, err)
	require.Equal(t, cols.String(), "{tpch.nation.n_name, tpch.nation.n_nationkey}")
}
