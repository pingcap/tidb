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
	"sort"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/planner/indexadvisor"
	"github.com/pingcap/tidb/pkg/testkit"
	s "github.com/pingcap/tidb/pkg/util/set"
	"github.com/stretchr/testify/require"
)

func TestOptionMaxNumIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec(`use mysql`)
	tk.MustExec(`recommend index set max_num_index=10`)
	tk.MustQuery(`select value from tidb_kernel_options where module='index_advisor' and name='max_num_index'`).
		Check(testkit.Rows("10"))
	tk.MustExec(`recommend index set max_num_index=11`)
	tk.MustQuery(`select value from tidb_kernel_options where module='index_advisor' and name='max_num_index'`).
		Check(testkit.Rows("11"))
	tk.MustExec(`recommend index set max_num_index=33`)
	tk.MustQuery(`select value from tidb_kernel_options where module='index_advisor' and name='max_num_index'`).
		Check(testkit.Rows("33"))
	tk.MustExecToErr(`recommend index set max_num_index=-1`)
	tk.MustExecToErr(`recommend index set max_num_index=0`)

	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b int, c int)`)
	querySet := s.NewSet[indexadvisor.Query]()
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: "select * from t where a=1", Frequency: 1})
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: "select * from t where b=1", Frequency: 1})
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: "select * from t where c=1", Frequency: 1})
	ctx := context.WithValue(context.Background(), indexadvisor.TestKey("query_set"), querySet)
	check(ctx, t, tk, "test.t.a,test.t.b,test.t.c", "") // 3 indexes
	tk.MustExec(`recommend index set max_num_index=2`)
	check(ctx, t, tk, "test.t.a,test.t.b", "") // 2 indexes
	tk.MustExec(`recommend index set max_num_index=1`)
	check(ctx, t, tk, "test.t.a", "") // 1 index
}

func TestOptionMaxIndexColumns(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec(`use mysql`)
	tk.MustExec(`recommend index set max_index_columns=10`)
	tk.MustQuery(`select value from tidb_kernel_options where module='index_advisor' and name='max_index_columns'`).
		Check(testkit.Rows("10"))
	tk.MustExec(`recommend index set max_index_columns=11`)
	tk.MustQuery(`select value from tidb_kernel_options where module='index_advisor' and name='max_index_columns'`).
		Check(testkit.Rows("11"))
	tk.MustExec(`recommend index set max_index_columns=33`)
	tk.MustQuery(`select value from tidb_kernel_options where module='index_advisor' and name='max_index_columns'`).
		Check(testkit.Rows("33"))
	tk.MustExecToErr(`recommend index set max_index_columns=-1`)
	tk.MustExecToErr(`recommend index set max_index_columns=0`)

	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b int, c int, d int)`)
	check(nil, t, tk, "test.t.a_b", "select b from t where a=1")
	check(nil, t, tk, "test.t.a_b_c", "select b, c from t where a=1")
	check(nil, t, tk, "test.t.a_d_b", "select b from t where a=1 and d=1")
	tk.MustExec(`recommend index set max_index_columns=2`)
	check(nil, t, tk, "test.t.a_b", "select b from t where a=1")
	check(nil, t, tk, "test.t.a", "select b, c from t where a=1")
	check(nil, t, tk, "test.t.a_d", "select b from t where a=1 and d=1")
	tk.MustExec(`recommend index set max_index_columns=1`)
	check(nil, t, tk, "test.t.a", "select b from t where a=1")
	check(nil, t, tk, "test.t.a", "select b, c from t where a=1")
	check(nil, t, tk, "test.t.a", "select b from t where a=1 and d=1")
}

func TestOptionMaxNumQuery(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec(`use mysql`)
	tk.MustExec(`recommend index set max_num_query=10`)
	tk.MustQuery(`select value from tidb_kernel_options where module='index_advisor' and name='max_num_query'`).
		Check(testkit.Rows("10"))
	tk.MustExec(`recommend index set max_num_query=22`)
	tk.MustQuery(`select value from tidb_kernel_options where module='index_advisor' and name='max_num_query'`).
		Check(testkit.Rows("22"))
	tk.MustExec(`recommend index set max_num_query=1111`)
	tk.MustQuery(`select value from tidb_kernel_options where module='index_advisor' and name='max_num_query'`).
		Check(testkit.Rows("1111"))
	tk.MustExecToErr(`recommend index set max_num_query=-1`)
	tk.MustExecToErr(`recommend index set max_num_query=0`)
}

func TestOptionTimeout(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec(`use mysql`)
	tk.MustExec(`recommend index set timeout='123ms'`)
	tk.MustQuery(`select value from tidb_kernel_options where module='index_advisor' and name='timeout'`).
		Check(testkit.Rows("123ms"))
	tk.MustExec(`recommend index set timeout='1s'`)
	tk.MustQuery(`select value from tidb_kernel_options where module='index_advisor' and name='timeout'`).
		Check(testkit.Rows("1s"))
	tk.MustExec(`recommend index set timeout='0s'`)
	tk.MustQuery(`select value from tidb_kernel_options where module='index_advisor' and name='timeout'`).
		Check(testkit.Rows("0s")) // allow 0
	tk.MustExec(`recommend index set timeout='1m'`)
	tk.MustQuery(`select value from tidb_kernel_options where module='index_advisor' and name='timeout'`).
		Check(testkit.Rows("1m"))
	tk.MustExecToErr(`recommend index set timeout='-1s'`)

	tk.MustExec(`use test`)
	tk.MustExec(`recommend index set timeout='1m'`)
	tk.MustExec(`create table t (a int, b int, c int)`)
	rows := tk.MustQuery(`recommend index run for 'select a from t where a=1'`).Rows()
	require.Equal(t, 1, len(rows))

	tk.MustExec(`recommend index set timeout='0m'`)
	tk.MustQueryToErr(`recommend index run for 'select a from t where a=1'`) // timeout

	tk.MustExec(`recommend index set timeout='1m'`)
	rows = tk.MustQuery(`recommend index run for 'select a from t where a=1'`).Rows()
	require.Equal(t, 1, len(rows))
}

func TestOptionsMultiple(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustQuery(`recommend index show option`).Sort().
		Check(testkit.Rows("max_index_columns 3 The maximum number of columns in an index.",
			"max_num_index 5 The maximum number of indexes to recommend for a table.",
			"max_num_query 1000 The maximum number of queries to recommend indexes.",
			"timeout 30s The timeout of index advisor."))

	tk.MustExec(`recommend index set max_num_query=111, max_index_columns=11, timeout='11m'`)
	tk.MustQuery(`recommend index show option`).Sort().
		Check(testkit.Rows("max_index_columns 11 The maximum number of columns in an index.",
			"max_num_index 5 The maximum number of indexes to recommend for a table.",
			"max_num_query 111 The maximum number of queries to recommend indexes.",
			"timeout 11m The timeout of index advisor."))

	tk.MustExec(`recommend index set max_num_query=222, max_index_columns=22, timeout='22m'`)
	tk.MustQuery(`recommend index show option`).Sort().
		Check(testkit.Rows("max_index_columns 22 The maximum number of columns in an index.",
			"max_num_index 5 The maximum number of indexes to recommend for a table.",
			"max_num_query 222 The maximum number of queries to recommend indexes.",
			"timeout 22m The timeout of index advisor."))

	tk.MustExecToErr(`recommend index set max_num_query=333, max_index_columns=33, timeout='-33m'`)
	tk.MustQuery(`recommend index show option`).Sort().
		Check(testkit.Rows("max_index_columns 33 The maximum number of columns in an index.",
			"max_num_index 5 The maximum number of indexes to recommend for a table.",
			"max_num_query 333 The maximum number of queries to recommend indexes.",
			"timeout 22m The timeout of index advisor.")) // unchanged
}

func TestOptionWithRun(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b int, c int, d int)`)

	check := func(sql, expected string) {
		rs := tk.MustQuery(sql).Rows()
		indexes := make([]string, 0, len(rs))
		for _, r := range rs {
			indexes = append(indexes, r[2].(string))
		}
		sort.Strings(indexes)
		require.Equal(t, expected, strings.Join(indexes, ","))
	}

	check(`recommend index run for 'select * from t where a=1 and b=1 and c=1'`, "idx_a_b_c")
	check(`recommend index run for 'select * from t where a=1 and b=1 and c=1' with max_index_columns=2`,
		"idx_a_b")
	check(`recommend index run for 'select * from t where a=1 and b=1 and c=1' with max_index_columns=1`,
		"idx_a")
	check(`recommend index run for 'select a from t where a=1; select b from t where b=1'`,
		"idx_a,idx_b")
	check(`recommend index run for 'select a from t where a=1; select b from t where b=1' with max_num_index=1`,
		"idx_a_b")
	check(`recommend index run for 'select a from t where a=1; select b from t where b=1'
                with max_num_index=1, max_index_columns=1`,
		"idx_a")

	tk.MustQueryToErr(`recommend index run for 'select a from t' with timeout='0s'`)
	tk.MustQueryToErr(`recommend index run for 'select a from t' with timeout='-0s'`)
	tk.MustQueryToErr(`recommend index run for 'select a from t' with timeout='xxx'`)
	tk.MustQueryToErr(`recommend index run for 'select a from t' with xxx=0`)
}

func TestOptionShow(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustQuery(`recommend index show option`).Sort().
		Check(testkit.Rows("max_index_columns 3 The maximum number of columns in an index.",
			"max_num_index 5 The maximum number of indexes to recommend for a table.",
			"max_num_query 1000 The maximum number of queries to recommend indexes.",
			"timeout 30s The timeout of index advisor."))

	tk.MustExec(`recommend index set max_num_query=1111`)
	tk.MustQuery(`recommend index show option`).Sort().
		Check(testkit.Rows("max_index_columns 3 The maximum number of columns in an index.",
			"max_num_index 5 The maximum number of indexes to recommend for a table.",
			"max_num_query 1111 The maximum number of queries to recommend indexes.",
			"timeout 30s The timeout of index advisor."))

	tk.MustExec(`recommend index set max_index_columns=10`)
	tk.MustQuery(`recommend index show option`).Sort().
		Check(testkit.Rows("max_index_columns 10 The maximum number of columns in an index.",
			"max_num_index 5 The maximum number of indexes to recommend for a table.",
			"max_num_query 1111 The maximum number of queries to recommend indexes.",
			"timeout 30s The timeout of index advisor."))

	tk.MustExec(`recommend index set timeout='10m'`)
	tk.MustQuery(`recommend index show option`).Sort().
		Check(testkit.Rows("max_index_columns 10 The maximum number of columns in an index.",
			"max_num_index 5 The maximum number of indexes to recommend for a table.",
			"max_num_query 1111 The maximum number of queries to recommend indexes.",
			"timeout 10m The timeout of index advisor."))
}
