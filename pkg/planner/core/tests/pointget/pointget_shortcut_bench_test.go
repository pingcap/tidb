// Copyright 2026 PingCAP, Inc.
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

package pointget

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

// BenchmarkPointGetExecShortcut measures the per-statement cost of a
// non-prepared point-get SELECT with and without the execution shortcut.
// The sub-benchmarks share schema, data, and query shape; only the value of
// tidb_enable_point_get_exec_shortcut differs, so the delta isolates the
// runStmt-vs-shortcut overhead (rich trace events, flight recorder
// triggers, telemetry counters, stmt-count bookkeeping).
//
// PointGet and BatchPointGet are covered separately because they exercise
// different executor types; both go through the same shortcut entry point.
//
// The shortcut sits after optimization, so the non-prepared plan cache is
// orthogonal — both paths benefit equally from cache hits. The benchmark
// leaves the default cache behavior in place to reflect realistic load.
func BenchmarkPointGetExecShortcut(b *testing.B) {
	cases := []struct {
		name  string
		query string
	}{
		{"PointGet", "select * from t_pg_bench where a = 1"},
		{"BatchPointGet", "select * from t_pg_bench where a in (1, 5, 9)"},
	}
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			for _, sub := range []struct {
				name    string
				enabled bool
			}{
				{"ShortcutOff", false},
				{"ShortcutOn", true},
			} {
				b.Run(sub.name, func(b *testing.B) {
					store := testkit.CreateMockStore(b)
					tk := testkit.NewTestKit(b, store)
					tk.MustExec("use test")
					tk.MustExec("drop table if exists t_pg_bench")
					tk.MustExec("create table t_pg_bench(a int primary key, b int, c varchar(32))")
					for i := range 16 {
						tk.MustExec("insert into t_pg_bench values(?, ?, ?)", i, i*10, "row")
					}
					if sub.enabled {
						tk.MustExec("set @@tidb_enable_point_get_exec_shortcut = ON")
					}
					// Warm-up amortizes one-time costs (schema fetch, plan cache fill).
					tk.MustQuery(c.query).Rows()

					b.ResetTimer()
					for range b.N {
						tk.MustQuery(c.query).Rows()
					}
				})
			}
		})
	}
}
