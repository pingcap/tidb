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

// BenchmarkTrivialPlan measures the per-statement cost of an eligible SELECT
// with and without the trivial plan fast path. The fast path is gated by
// fixcontrol 52592 (when set, TryTrivialPlan bails immediately and the full
// optimizer runs), so the two sub-benchmarks share the exact same query,
// schema, and data — only the optimizer path differs.
//
// The non-prepared plan cache is disabled so the optimizer runs every
// iteration; otherwise the first iteration would warm the cache and
// subsequent iterations would skip the optimizer regardless of the fast
// path, masking the delta.
func BenchmarkTrivialPlan(b *testing.B) {
	for _, sub := range []struct {
		name           string
		disableTrivial bool
	}{
		{"FastPathEnabled", false},
		{"FullOptimizer", true},
	} {
		b.Run(sub.name, func(b *testing.B) {
			store := testkit.CreateMockStore(b)
			tk := testkit.NewTestKit(b, store)
			tk.MustExec("use test")
			tk.MustExec("drop table if exists t_bench")
			tk.MustExec("create table t_bench(a int primary key, b int, c varchar(32))")
			for i := range 16 {
				tk.MustExec("insert into t_bench values(?, ?, ?)", i, i*10, "row")
			}
			tk.MustExec("set @@tidb_enable_non_prepared_plan_cache=0")
			if sub.disableTrivial {
				tk.MustExec("set @@tidb_opt_fix_control = '52592:ON'")
			}
			// Warm-up to amortize any one-time work (schema fetch, parser cache).
			tk.MustQuery("select * from t_bench").Rows()

			b.ResetTimer()
			for range b.N {
				tk.MustQuery("select * from t_bench").Rows()
			}
		})
	}
}
