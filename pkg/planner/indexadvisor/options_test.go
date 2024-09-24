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
	check(ctx, t, tk, "test.t.a,test.t.b,test.t.c", new(indexadvisor.Option)) // 3 indexes
	tk.MustExec(`recommend index set max_num_index=2`)
	check(ctx, t, tk, "test.t.a,test.t.b", new(indexadvisor.Option)) // 2 indexes
	tk.MustExec(`recommend index set max_num_index=1`)
	check(ctx, t, tk, "test.t.a", new(indexadvisor.Option)) // 1 index
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
	opt := func(sql string) *indexadvisor.Option {
		return &indexadvisor.Option{SpecifiedSQLs: []string{sql}}
	}
	check(nil, t, tk, "test.t.a_b", opt("select b from t where a=1"))
	check(nil, t, tk, "test.t.a_b_c", opt("select b, c from t where a=1"))
	check(nil, t, tk, "test.t.a_d_b", opt("select b from t where a=1 and d=1"))
	tk.MustExec(`recommend index set max_index_columns=2`)
	check(nil, t, tk, "test.t.a_b", opt("select b from t where a=1"))
	check(nil, t, tk, "test.t.a", opt("select b, c from t where a=1"))
	check(nil, t, tk, "test.t.a_d", opt("select b from t where a=1 and d=1"))
	tk.MustExec(`recommend index set max_index_columns=1`)
	check(nil, t, tk, "test.t.a", opt("select b from t where a=1"))
	check(nil, t, tk, "test.t.a", opt("select b, c from t where a=1"))
	check(nil, t, tk, "test.t.a", opt("select b from t where a=1 and d=1"))
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
