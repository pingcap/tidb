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
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/indexadvisor"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestOptimizerColumnType(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t1 (a int, b float, c varchar(255))`)
	tk.MustExec(`create table t2 (a int, b decimal(10,2), c varchar(1024))`)
	opt := indexadvisor.NewOptimizer(tk.Session())

	tp, err := opt.ColumnType(indexadvisor.Column{SchemaName: "test", TableName: "t1", ColumnName: "a"})
	require.NoError(t, err)
	require.Equal(t, mysql.TypeLong, tp.GetType())

	tp, err = opt.ColumnType(indexadvisor.Column{SchemaName: "test", TableName: "t1", ColumnName: "b"})
	require.NoError(t, err)
	require.Equal(t, mysql.TypeFloat, tp.GetType())

	tp, err = opt.ColumnType(indexadvisor.Column{SchemaName: "test", TableName: "t1", ColumnName: "c"})
	require.NoError(t, err)
	require.Equal(t, mysql.TypeVarchar, tp.GetType())

	tp, err = opt.ColumnType(indexadvisor.Column{SchemaName: "test", TableName: "t2", ColumnName: "a"})
	require.NoError(t, err)
	require.Equal(t, mysql.TypeLong, tp.GetType())

	tp, err = opt.ColumnType(indexadvisor.Column{SchemaName: "test", TableName: "t2", ColumnName: "b"})
	require.NoError(t, err)
	require.Equal(t, mysql.TypeNewDecimal, tp.GetType())

	tp, err = opt.ColumnType(indexadvisor.Column{SchemaName: "test", TableName: "t2", ColumnName: "c"})
	require.NoError(t, err)
	require.Equal(t, mysql.TypeVarchar, tp.GetType())

	_, err = opt.ColumnType(indexadvisor.Column{SchemaName: "test", TableName: "t2", ColumnName: "d"})
	require.Error(t, err)

	_, err = opt.ColumnType(indexadvisor.Column{SchemaName: "test", TableName: "t3", ColumnName: "a"})
	require.Error(t, err)
}

func TestOptimizerPrefixContainIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t1 (a int, b int, c int, d int, key(a), key(b, c))`)
	tk.MustExec(`create table t2 (a int, b int, c int, d int, key(a, b, c, d), key(d, c, b, a))`)
	opt := indexadvisor.NewOptimizer(tk.Session())

	check := func(expected bool, tableName string, columns ...string) {
		ok, err := opt.PrefixContainIndex(indexadvisor.NewIndex("test", tableName, "idx", columns...))
		require.NoError(t, err)
		require.Equal(t, expected, ok)
	}

	check(true, "t1", "a")
	check(true, "t1", "b")
	check(true, "t1", "b", "c")
	check(false, "t1", "c")
	check(false, "t1", "a", "b")
	check(false, "t1", "b", "c", "a")
	check(true, "t2", "a")
	check(true, "t2", "a", "b")
	check(true, "t2", "a", "b", "c")
	check(true, "t2", "a", "b", "c", "d")
	check(true, "t2", "d")
	check(true, "t2", "d", "c")
	check(false, "t2", "b")
	check(false, "t2", "b", "a")
	check(false, "t2", "b", "a", "c")
}

func TestOptimizerPossibleColumns(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t1 (a int, b int, c int, d int)`)
	tk.MustExec(`create table t2 (a int, b int, c int, d int)`)
	tk.MustExec(`create table t3 (c int, d int, e int, f int)`)
	opt := indexadvisor.NewOptimizer(tk.Session())

	check := func(schema, colName string, expected []string) {
		cols, err := opt.PossibleColumns(schema, colName)
		require.NoError(t, err)
		var tmp []string
		for _, col := range cols {
			tmp = append(tmp, fmt.Sprintf("%v.%v", col.TableName, col.ColumnName))
		}
		sort.Strings(tmp)
		require.Equal(t, expected, tmp)
	}

	check("test", "a", []string{"t1.a", "t2.a"})
	check("test", "b", []string{"t1.b", "t2.b"})
	check("test", "c", []string{"t1.c", "t2.c", "t3.c"})
	check("test", "d", []string{"t1.d", "t2.d", "t3.d"})
	check("test", "e", []string{"t3.e"})
	check("test", "f", []string{"t3.f"})
	check("test", "g", nil)
}

func TestOptimizerTableColumns(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t1 (a int, b int, c int, d int)`)
	tk.MustExec(`create table t2 (a int, b int, c int, d int)`)
	tk.MustExec(`create table t3 (c int, d int, e int, f int)`)
	opt := indexadvisor.NewOptimizer(tk.Session())

	check := func(schemaName, tableName string, columns []string) {
		cols, err := opt.TableColumns(schemaName, tableName)
		require.NoError(t, err)
		var tmp []string
		for _, col := range cols {
			require.Equal(t, schemaName, col.SchemaName)
			require.Equal(t, tableName, col.TableName)
			tmp = append(tmp, col.ColumnName)
		}
		sort.Strings(tmp)
		require.Equal(t, columns, tmp)
	}

	check("test", "t1", []string{"a", "b", "c", "d"})
	check("test", "t2", []string{"a", "b", "c", "d"})
	check("test", "t3", []string{"c", "d", "e", "f"})
}

func TestOptimizerIndexNameExist(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t1 (a int, b int, c int, d int, index ka(a), index kbc(b, c))`)
	tk.MustExec(`create table t2 (a int, b int, c int, d int, index ka(a), index kbc(b, c))`)
	opt := indexadvisor.NewOptimizer(tk.Session())

	check := func(schema, table, indexName string, expected bool) {
		ok, err := opt.IndexNameExist(schema, table, indexName)
		require.NoError(t, err)
		require.Equal(t, expected, ok)
	}

	check("test", "t1", "ka", true)
	check("test", "t1", "kbc", true)
	check("test", "t1", "kbc2", false)
	check("test", "t2", "ka", true)
	check("test", "t2", "kbc", true)
	check("test", "t2", "kbc2", false)
}

func TestOptimizerEstIndexSize(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	h := dom.StatsHandle()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b varchar(64))`)
	opt := indexadvisor.NewOptimizer(tk.Session())

	s, err := opt.EstIndexSize("test", "t", "a")
	require.NoError(t, err)
	require.Equal(t, float64(0), s)

	s, err = opt.EstIndexSize("test", "t", "b")
	require.NoError(t, err)
	require.Equal(t, float64(0), s)

	tk.MustExec(`insert into t values (1, space(32))`)
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(context.Background(), dom.InfoSchema()))
	tk.MustExec("analyze table t all columns")
	s, err = opt.EstIndexSize("test", "t", "a")
	require.NoError(t, err)
	require.Equal(t, float64(1), s)

	s, err = opt.EstIndexSize("test", "t", "b")
	require.NoError(t, err)
	require.Equal(t, float64(33), s) // 32 + 1

	s, err = opt.EstIndexSize("test", "t", "a", "b")
	require.NoError(t, err)
	require.Equal(t, float64(34), s) // 32 + 1 + 1

	tk.MustExec(`insert into t values (1, space(64))`)
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(context.Background(), dom.InfoSchema()))
	tk.MustExec("analyze table t all columns")
	s, err = opt.EstIndexSize("test", "t", "a")
	require.NoError(t, err)
	require.Equal(t, float64(2), s) // 2 rows

	s, err = opt.EstIndexSize("test", "t", "b")
	require.NoError(t, err)
	require.Equal(t, float64(99), s) // 32 + 64 + x

	s, err = opt.EstIndexSize("test", "t", "b", "a")
	require.NoError(t, err)
	require.Equal(t, float64(99+2), s)
}

func TestOptimizerQueryCost(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t1 (a int, b int, c int, d int, index ka(a), index kbc(b, c))`)
	tk.MustExec(`create table t2 (a int, b int, c int, d int, index ka(a), index kbc(b, c))`)
}

func TestOptimizerQueryPlanCost(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t0 (a int, b int, c int)`)

	opt := indexadvisor.NewOptimizer(tk.Session())
	cost1, err := opt.QueryPlanCost("select a, b from t0 where a=1 and b=1")
	require.NoError(t, err)

	cost2, err := opt.QueryPlanCost("select a, b from t0 where a=1 and b=1", indexadvisor.Index{
		SchemaName: "test",
		TableName:  "t0",
		IndexName:  "idx_a",
		Columns: []indexadvisor.Column{
			{SchemaName: "test", TableName: "t0", ColumnName: "a"}},
	})
	require.NoError(t, err)
	require.True(t, cost2 < cost1)

	cost3, err := opt.QueryPlanCost("select a, b from t0 where a=1 and b=1", indexadvisor.Index{
		SchemaName: "test",
		TableName:  "t0",
		IndexName:  "idx_a",
		Columns: []indexadvisor.Column{
			{SchemaName: "test", TableName: "t0", ColumnName: "a"},
			{SchemaName: "test", TableName: "t0", ColumnName: "b"}},
	})
	require.NoError(t, err)
	require.True(t, cost3 < cost2)
}
