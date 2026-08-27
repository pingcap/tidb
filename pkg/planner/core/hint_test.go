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
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/logutil"
	semv2 "github.com/pingcap/tidb/pkg/util/sem/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestHintSuite(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec(`use test`)
		t.Run("TestSetVarTimestampHintsWorks", func(t *testing.T) {
			testKit.MustExec(`set timestamp=default;`)
			require.Equal(t, "42", testKit.MustQuery(`select /*+ set_var(timestamp=1) */ @@timestamp + 41;`).Rows()[0][0].(string))
			firstts := testKit.MustQuery(`select @@timestamp;`).Rows()[0][0].(string)
			require.Eventually(t, func() bool {
				return firstts < testKit.MustQuery(`select @@timestamp;`).Rows()[0][0].(string)
			}, time.Second, time.Microsecond*10)

			testKit.MustExec(`set timestamp=1745862208.446495;`)
			require.Equal(t, "1745862208.446495", testKit.MustQuery(`select @@timestamp;`).Rows()[0][0].(string))
			require.Equal(t, "1", testKit.MustQuery(`select /*+ set_var(timestamp=1) */ @@timestamp;`).Rows()[0][0].(string))
			require.Equal(t, "1745862208.446495", testKit.MustQuery(`select @@timestamp;`).Rows()[0][0].(string))
		})

		t.Run("TestSetVarTimestampHintsWorksWithBindings", func(t *testing.T) {
			testKit.MustExec(`create session binding for select @@timestamp + 41 using select /*+ set_var(timestamp=1) */ @@timestamp + 41;`)
			testKit.MustExec(`set timestamp=default;`)
			require.Equal(t, "42", testKit.MustQuery(`select @@timestamp + 41;`).Rows()[0][0].(string))
			testKit.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("1"))
			firstts := testKit.MustQuery(`select @@timestamp;`).Rows()[0][0].(string)
			require.Eventually(t, func() bool {
				return firstts < testKit.MustQuery(`select @@timestamp;`).Rows()[0][0].(string)
			}, time.Second, time.Microsecond*10)

			testKit.MustExec(`set timestamp=1745862208.446495;`)
			require.Equal(t, "1745862208.446495", testKit.MustQuery(`select @@timestamp;`).Rows()[0][0].(string))
			require.Equal(t, "42", testKit.MustQuery(`select @@timestamp + 41;`).Rows()[0][0].(string))
			testKit.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("1"))
			require.Equal(t, "1745862208.446495", testKit.MustQuery(`select @@timestamp;`).Rows()[0][0].(string))
		})

		t.Run("TestSetVarInQueriesAndBindingsWorkTogether", func(t *testing.T) {
			testKit.MustExec(`set @@max_execution_time=2000;`)
			testKit.MustExec(`drop table if exists foo`)
			testKit.MustExec(`create table foo (a int);`)
			testKit.MustExec(`create session binding for select * from foo where a = 1 using select /*+ set_var(max_execution_time=1234) */ * from foo where a = 1;`)
			testKit.MustExec(`select /*+ set_var(max_execution_time=2222) */ * from foo where a = 1;`)
			testKit.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("1"))
			testKit.MustQuery("select @@max_execution_time;").Check(testkit.Rows("2000"))
		})

		t.Run("TestRestrictedBindingPlanHint", func(t *testing.T) {
			testKit.MustExec(`drop table if exists sem_binding_hint_t`)
			testKit.MustExec(`create table sem_binding_hint_t(a int, b int, key idx_a(a), key idx_b(b))`)
			testKit.MustExec(`insert into sem_binding_hint_t values
				(1, 1), (2, 1), (3, 1), (4, 1), (5, 1),
				(6, 6), (7, 7), (8, 8), (9, 9), (10, 10),
				(11, 11), (12, 12), (13, 13), (14, 14), (15, 15),
				(16, 16), (17, 17), (18, 18), (19, 19), (20, 20)`)
			testKit.MustExec(`analyze table sem_binding_hint_t`)

			testKit.MustQuery(`explain format = 'plan_tree' select a from sem_binding_hint_t where a = 1`).CheckContain("idx_a")
			testKit.MustQuery(`explain format = 'plan_tree' select /*+ ignore_index(sem_binding_hint_t, idx_a) */ a from sem_binding_hint_t where a = 1`).CheckNotContain("idx_a")

			testKit.MustExec(`create binding using select /*+ ignore_index(sem_binding_hint_t, idx_a) */ a from sem_binding_hint_t where a = 1`)
			require.NoError(t, semv2.EnableBy(&semv2.Config{
				TiDBVersion:     "v0.0.0",
				RestrictedHints: []string{"ignore_index"},
			}))
			t.Cleanup(semv2.Disable)

			plan := testKit.MustQuery(`explain format = 'plan_tree' select a from sem_binding_hint_t where a = 1`)
			require.True(t, testKit.Session().GetSessionVars().FoundInBinding)
			testKit.MustQuery(`show warnings`).CheckContain("the IGNORE_INDEX() optimizer hint is restricted under the current security policy and is ignored")
			plan.CheckContain("idx_a")
		})

		t.Run("TestSetVarDecimalValueHintsWork", func(t *testing.T) {
			testKit.MustExec(`set @@tidb_default_string_match_selectivity = 0.8`)
			testKit.MustQuery(`select /*+ set_var(tidb_default_string_match_selectivity=0.3) */ @@tidb_default_string_match_selectivity`).Check(testkit.Rows("0.3"))
			testKit.MustQuery(`show warnings`).Check(testkit.Rows())
			testKit.MustQuery(`select @@tidb_default_string_match_selectivity`).Check(testkit.Rows("0.8"))

			testKit.MustExec(`create session binding for select @@tidb_default_string_match_selectivity using select /*+ set_var(tidb_default_string_match_selectivity=0.3) */ @@tidb_default_string_match_selectivity`)
			testKit.MustQuery(`select @@tidb_default_string_match_selectivity`).Check(testkit.Rows("0.3"))
			testKit.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("1"))
			testKit.MustQuery(`show warnings`).Check(testkit.Rows())

			rows := testKit.MustQuery(`show session bindings`).Rows()
			found := slices.ContainsFunc(rows, func(row []any) bool {
				return strings.Contains(strings.ToLower(row[1].(string)), "set_var(tidb_default_string_match_selectivity = '0.3')")
			})
			require.True(t, found)

			testKit.MustExec(`drop session binding for select @@tidb_default_string_match_selectivity`)
			testKit.MustQuery(`select @@tidb_default_string_match_selectivity`).Check(testkit.Rows("0.8"))
		})

		t.Run("TestSetVarHintsWithExplain", func(t *testing.T) {
			testKit.MustExec(`set @@max_execution_time=2000;`)
			testKit.MustExec(`explain select /*+ set_var(max_execution_time=100) */ @@max_execution_time;`)
			testKit.MustQuery("select @@max_execution_time;").Check(testkit.Rows("2000"))

			testKit.MustExec(`drop table if exists t`)
			testKit.MustExec(`create table t(a int);`)
			testKit.MustExec(`create global binding for select * from t where a = 1 and sleep(0.1) using select /*+ SET_VAR(max_execution_time=500) */ * from t where a = 1 and sleep(0.1);`)
			testKit.MustExec(`select * from t where a = 1 and sleep(0.1);`)
			testKit.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("1"))
			testKit.MustQuery("select @@max_execution_time;").Check(testkit.Rows("2000"))

			testKit.MustExec(`explain select * from t where a = 1 and sleep(0.1);`)
			testKit.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("1"))
			testKit.MustQuery("select @@max_execution_time;").Check(testkit.Rows("2000"))
		})

		t.Run("TestWriteSlowLogHint", func(t *testing.T) {
			testKit.MustExec(`drop table if exists t`)
			testKit.MustExec(`create table t(a int);`)
			testKit.MustExec(`select * from t where a = 1;`)

			core, recorded := observer.New(zap.WarnLevel)
			logger := zap.New(core)
			prev := logutil.SlowQueryLogger
			logutil.SlowQueryLogger = logger
			defer func() { logutil.SlowQueryLogger = prev }()

			sql := "select /*+ write_slow_log */ * from t where a = 1;"
			checkWriteSlowLog := func(expectWrite bool) {
				if !expectWrite {
					require.Equal(t, 0, recorded.Len())
				} else {
					require.NotEqual(t, 0, recorded.Len())
				}

				writeMsg := slices.ContainsFunc(recorded.All(), func(entry observer.LoggedEntry) bool {
					if entry.Level == zap.WarnLevel && strings.Contains(entry.Message, sql) {
						return true
					}
					return false
				})
				require.Equal(t, expectWrite, writeMsg)
			}

			testKit.MustExec(`select * from t where a = 1;`)
			checkWriteSlowLog(false)

			testKit.MustExec(sql)
			checkWriteSlowLog(true)
		})

		t.Run("TestSetVarPartialOrderedIndexForTopN", func(t *testing.T) {
			testKit.MustQuery(`select @@tidb_opt_partial_ordered_index_for_topn`).Check(testkit.Rows("DISABLE"))

			testKit.MustExec(`set @@tidb_opt_partial_ordered_index_for_topn = DISABLE`)
			testKit.MustQuery(`select /*+ set_var(tidb_opt_partial_ordered_index_for_topn=COST) */ @@tidb_opt_partial_ordered_index_for_topn`).Check(testkit.Rows("COST"))
			testKit.MustQuery(`select @@tidb_opt_partial_ordered_index_for_topn`).Check(testkit.Rows("DISABLE"))

			testKit.MustExec(`set @@tidb_opt_partial_ordered_index_for_topn = COST`)
			testKit.MustQuery(`select /*+ set_var(tidb_opt_partial_ordered_index_for_topn=DISABLE) */ @@tidb_opt_partial_ordered_index_for_topn`).Check(testkit.Rows("DISABLE"))
			testKit.MustQuery(`select @@tidb_opt_partial_ordered_index_for_topn`).Check(testkit.Rows("COST"))

			testKit.MustExec(`drop table if exists t, t_multi`)
			testKit.MustExec(`create table t(a int, b varchar(10), index idx_b(b(5)));`)
			testKit.MustExec(`set @@tidb_opt_partial_ordered_index_for_topn = DISABLE`)
			testKit.MustExec(`select /*+ set_var(tidb_opt_partial_ordered_index_for_topn=COST) */ * from t order by b limit 10;`)
			testKit.MustQuery(`select @@tidb_opt_partial_ordered_index_for_topn`).Check(testkit.Rows("DISABLE"))

			testKit.MustExec(`set @@tidb_opt_partial_ordered_index_for_topn = DISABLE`)
			testKit.MustExec(`explain select /*+ set_var(tidb_opt_partial_ordered_index_for_topn=COST) */ * from t order by b limit 10;`)
			testKit.MustQuery(`select @@tidb_opt_partial_ordered_index_for_topn`).Check(testkit.Rows("DISABLE"))

			testKit.MustExec(`create table t_multi(a int, b varchar(30), c varchar(30), key idx_ab_prefix(a, b(15)))`)
			testKit.MustExec(`insert into t_multi values
			(1, 'alpha', 'first'),
			(1, 'beta', 'second'),
			(1, 'gamma', 'third'),
			(2, 'alpha', 'fourth'),
			(2, 'beta', 'fifth'),
			(2, 'gamma', 'sixth')`)
			testKit.MustExec(`set @@tidb_opt_partial_ordered_index_for_topn = COST`)
			testKit.MustQuery(`select /*+ use_index(t_multi, idx_ab_prefix) */ * from t_multi order by a, b limit 3 offset 2`).
				Check(testkit.Rows(
					"1 gamma third",
					"2 alpha fourth",
					"2 beta fifth",
				))
		})
	})
}
