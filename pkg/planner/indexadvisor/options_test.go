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
)

func TestSetOption(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`recommend index set max_num_index=10`)
	tk.MustExec(`recommend index set max_index_columns=5`)
	tk.MustQuery(`select name, value from mysql.tidb_kernel_options where module='index_advisor'`).
		Sort().Check(testkit.Rows("max_index_columns 5", "max_num_index 10"))

	tk.MustExec(`recommend index set max_num_index=11`)
	tk.MustExec(`recommend index set max_index_columns=11`)
	tk.MustQuery(`select name, value from mysql.tidb_kernel_options where module='index_advisor'`).
		Sort().Check(testkit.Rows("max_index_columns 11", "max_num_index 11"))

	tk.MustExec(`recommend index set max_num_index=22`)
	tk.MustExec(`recommend index set max_index_columns=22`)
	tk.MustQuery(`select name, value from mysql.tidb_kernel_options where module='index_advisor'`).
		Sort().Check(testkit.Rows("max_index_columns 22", "max_num_index 22"))

	// invalid option values
	tk.MustExecToErr(`recommend index set xxx=1`)
	tk.MustExecToErr(`recommend index set xxx=-1`)
	tk.MustExecToErr(`recommend index set max_num_index=-1`)
	tk.MustExecToErr(`recommend index set max_num_index=0`)
	tk.MustExecToErr(`recommend index set max_index_columns=-1`)
	tk.MustExecToErr(`recommend index set max_index_columns=0`)
}

func TestOptionEffectiveness(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
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

	tk.MustExec(`drop table t`)
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
