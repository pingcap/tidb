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

package integration_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

func TestIssue64822LikePredicateOrderIndependence(t *testing.T) {
	testWithVecMode := func(t *testing.T, vecMode string) {
		store := testkit.CreateMockStore(t)
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("set tidb_enable_vectorized_expression = " + vecMode)
		tk.MustExec("set tidb_opt_projection_push_down = 0")
		tk.MustExec("set tidb_hash_join_version = 'legacy'")
		tk.MustExec("drop table if exists t0, t1")
		tk.MustExec("create table t0(c0 char, c1 blob)")
		tk.MustExec("create table t1 like t0")
		tk.MustExec("insert into t0 values ('_', '1')")
		tk.MustExec("insert into t1 values ('ᡖ', '0')")
		tk.MustQuery(
			"select t1.c0 like t0.c0, t1.c0 not like t0.c0, not(case true when false then t1.c1 else t1.c0 end) from t1 join t0",
		).Check(testkit.Rows("1 0 1"))
		tk.MustQuery(
			"select t1.c0 like t0.c0, not(case true when false then t1.c1 else t1.c0 end), t1.c0 not like t0.c0 from t1 join t0",
		).Check(testkit.Rows("1 1 0"))
		tk.MustQuery(
			"select * from t1 join t0 where t1.c0 like t0.c0 and t1.c0 not like t0.c0 and not(case true when false then t1.c1 else t1.c0 end)",
		).Check(testkit.Rows())
		tk.MustQuery(
			"select * from t1 join t0 where t1.c0 like t0.c0 and not(case true when false then t1.c1 else t1.c0 end) and t1.c0 not like t0.c0",
		).Check(testkit.Rows())
	}
	t.Run("vec_on", func(t *testing.T) {
		testWithVecMode(t, "on")
	})
	t.Run("vec_off", func(t *testing.T) {
		testWithVecMode(t, "off")
	})
}
