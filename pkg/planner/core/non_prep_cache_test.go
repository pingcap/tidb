// Copyright 2025 PingCAP, Inc.
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

package core_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

// TestNonPrepPlanCacheOptimization tests the optimization that skips redundant
// cacheability checks for repeated SQL execution.
func TestNonPrepPlanCacheOptimization(t *testing.T) {
	store := testkit.CreateMockStore(t)

	t.Run("SkipCacheabilityCheck", func(t *testing.T) {
		// Verifies repeated execution of cacheable SQL skips cacheability checks.
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t_skip")
		tk.MustExec("create table t_skip (id int primary key, name varchar(100))")
		tk.MustExec("set tidb_enable_non_prepared_plan_cache=1")

		// First execution: cache miss
		tk.MustQuery("select * from t_skip where id = 1").Check(testkit.Rows())
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))

		// Second execution: cache hit (skips cacheability check)
		tk.MustQuery("select * from t_skip where id = 2").Check(testkit.Rows())
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

		// Third execution: cache hit
		tk.MustQuery("select * from t_skip where id = 3").Check(testkit.Rows())
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	})

	t.Run("DifferentPatterns", func(t *testing.T) {
		// Verifies different SQL patterns have separate cache entries.
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t_pattern")
		tk.MustExec("create table t_pattern (id int primary key, name varchar(100))")
		tk.MustExec("set tidb_enable_non_prepared_plan_cache=1")

		// Pattern 1: WHERE id = ?
		tk.MustQuery("select * from t_pattern where id = 1").Check(testkit.Rows())
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))

		// Pattern 2: WHERE name = ? (different pattern, cache miss)
		tk.MustQuery("select * from t_pattern where name = 'foo'").Check(testkit.Rows())
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))

		// Pattern 1 again: cache hit
		tk.MustQuery("select * from t_pattern where id = 2").Check(testkit.Rows())
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

		// Pattern 2 again: cache hit
		tk.MustQuery("select * from t_pattern where name = 'bar'").Check(testkit.Rows())
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	})

	t.Run("SchemaChange", func(t *testing.T) {
		// Verifies cache invalidation on schema change.
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t_schema")
		tk.MustExec("create table t_schema (id int primary key)")
		tk.MustExec("set tidb_enable_non_prepared_plan_cache=1")

		// Build cache
		tk.MustQuery("select id from t_schema where id = 1").Check(testkit.Rows())
		tk.MustQuery("select id from t_schema where id = 2").Check(testkit.Rows())
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

		// Schema change invalidates cache
		tk.MustExec("alter table t_schema add column name varchar(100)")

		// Cache miss after schema change
		tk.MustQuery("select id from t_schema where id = 3").Check(testkit.Rows())
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))

		// Cache hit after rebuild
		tk.MustQuery("select id from t_schema where id = 4").Check(testkit.Rows())
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	})

	t.Run("UncacheableSQL", func(t *testing.T) {
		// Verifies uncacheable SQLs are not cached.
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t_json")
		tk.MustExec("create table t_json (id int, data json)")
		tk.MustExec("set tidb_enable_non_prepared_plan_cache=1")

		// SQL with JSON column is uncacheable
		tk.MustQuery("select * from t_json where JSON_EXTRACT(data, '$.key') = 'v1'").Check(testkit.Rows())
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))

		// Second execution: still uncacheable
		tk.MustQuery("select * from t_json where JSON_EXTRACT(data, '$.key') = 'v2'").Check(testkit.Rows())
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	})
}

// BenchmarkNonPrepPlanCacheOptimization benchmarks the optimization effect.
func BenchmarkNonPrepPlanCacheOptimization(b *testing.B) {
	store := testkit.CreateMockStore(b)

	b.Run("Cacheable", func(b *testing.B) {
		// Cacheable SQL benefits from skipping cacheability check.
		tk := testkit.NewTestKit(b, store)
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t_bench")
		tk.MustExec("create table t_bench (id int primary key, name varchar(100))")
		tk.MustExec("insert into t_bench values (1, 'a'), (2, 'b'), (3, 'c')")
		tk.MustExec("set tidb_enable_non_prepared_plan_cache=1")

		// Warm up and verify it's cacheable
		tk.MustQuery("select * from t_bench where id = 1")
		tk.MustQuery("select * from t_bench where id = 2")
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tk.MustQuery("select * from t_bench where id = ?", i%3+1)
		}
	})

	b.Run("UncacheableFilterJSON", func(b *testing.B) {
		// Uncacheable: JSON column in WHERE clause requires type checking.
		tk := testkit.NewTestKit(b, store)
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t_json")
		tk.MustExec("create table t_json (id int, data json)")
		tk.MustExec("set tidb_enable_non_prepared_plan_cache=1")

		// Verify it's uncacheable
		tk.MustQuery("select * from t_json where data = '{}'")
		tk.MustQuery("select * from t_json where data = '{}'")
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Each execution: parameterization + cacheability check (JSON type check)
			tk.MustQuery("select * from t_json where data = '{}'")
		}
	})

	b.Run("UncacheableFilterEnum", func(b *testing.B) {
		// Uncacheable: ENUM column in WHERE clause requires type checking.
		tk := testkit.NewTestKit(b, store)
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t_enum")
		tk.MustExec("create table t_enum (id int, status enum('active','inactive'))")
		tk.MustExec("set tidb_enable_non_prepared_plan_cache=1")

		// Verify it's uncacheable
		tk.MustQuery("select * from t_enum where status = 'active'")
		tk.MustQuery("select * from t_enum where status = 'inactive'")
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// ENUM type check in cacheability checker
			tk.MustQuery("select * from t_enum where status = 'active'")
		}
	})

	b.Run("UncacheableUncacheableFunc", func(b *testing.B) {
		// Uncacheable: DATABASE() is in UnCacheableFunctions.
		tk := testkit.NewTestKit(b, store)
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t_func")
		tk.MustExec("create table t_func (id int, name varchar(100))")
		tk.MustExec("set tidb_enable_non_prepared_plan_cache=1")

		// Verify it's uncacheable
		tk.MustQuery("select * from t_func where name = database()")
		tk.MustQuery("select * from t_func where name = database()")
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Function checking in cacheability checker
			tk.MustQuery("select * from t_func where name = database()")
		}
	})

	b.Run("UncacheableMultiTable", func(b *testing.B) {
		// Uncacheable: Multiple tables with special column types.
		tk := testkit.NewTestKit(b, store)
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t1_multi, t2_multi")
		tk.MustExec("create table t1_multi (id int, data json)")
		tk.MustExec("create table t2_multi (id int, status enum('on','off'))")
		tk.MustExec("set tidb_enable_non_prepared_plan_cache=1")

		// Verify it's uncacheable
		tk.MustQuery("select * from t1_multi join t2_multi on t1_multi.id = t2_multi.id where t1_multi.data = '{}' and t2_multi.status = 'on'")
		tk.MustQuery("select * from t1_multi join t2_multi on t1_multi.id = t2_multi.id where t1_multi.data = '{}' and t2_multi.status = 'off'")
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Multiple table lookups + type checks
			tk.MustQuery("select * from t1_multi join t2_multi on t1_multi.id = t2_multi.id where t1_multi.data = '{}' and t2_multi.status = 'on'")
		}
	})
}
