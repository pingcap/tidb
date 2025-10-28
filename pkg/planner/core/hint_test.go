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
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestSetVarTimestampHintsWorks(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec(`use test`)

		// test that the timestamp continues to update (default behavior)
		testKit.MustExec(`set timestamp=default;`)
		require.Equal(t, "42", testKit.MustQuery(`select /*+ set_var(timestamp=1) */ @@timestamp + 41;`).Rows()[0][0].(string))
		firstts := testKit.MustQuery(`select @@timestamp;`).Rows()[0][0].(string)
		require.Eventually(t, func() bool {
			return firstts < testKit.MustQuery(`select @@timestamp;`).Rows()[0][0].(string)
		}, time.Second, time.Microsecond*10)

		// test that the set value is preserved
		testKit.MustExec(`set timestamp=1745862208.446495;`)
		require.Equal(t, "1745862208.446495", testKit.MustQuery(`select @@timestamp;`).Rows()[0][0].(string))
		require.Equal(t, "1", testKit.MustQuery(`select /*+ set_var(timestamp=1) */ @@timestamp;`).Rows()[0][0].(string))
		require.Equal(t, "1745862208.446495", testKit.MustQuery(`select @@timestamp;`).Rows()[0][0].(string))
	})
}

func TestSetVarTimestampHintsWorksWithBindings(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec(`use test`)
		testKit.MustExec(`create session binding for select @@timestamp + 41 using select /*+ set_var(timestamp=1) */ @@timestamp + 41;`)

		// test that bindings with hints correctly restore the default timestamp
		testKit.MustExec(`set timestamp=default;`)
		require.Equal(t, "42", testKit.MustQuery(`select @@timestamp + 41;`).Rows()[0][0].(string))
		testKit.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("1"))
		firstts := testKit.MustQuery(`select @@timestamp;`).Rows()[0][0].(string)
		require.Eventually(t, func() bool {
			return firstts < testKit.MustQuery(`select @@timestamp;`).Rows()[0][0].(string)
		}, time.Second, time.Microsecond*10)

		// test that bindings with hints correctly restore the previous non-default timestamp value
		testKit.MustExec(`set timestamp=1745862208.446495;`)
		require.Equal(t, "1745862208.446495", testKit.MustQuery(`select @@timestamp;`).Rows()[0][0].(string))
		require.Equal(t, "42", testKit.MustQuery(`select @@timestamp + 41;`).Rows()[0][0].(string))
		testKit.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("1"))
		require.Equal(t, "1745862208.446495", testKit.MustQuery(`select @@timestamp;`).Rows()[0][0].(string))
	})
}

func TestSetVarInQueriesAndBindingsWorkTogether(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec(`use test`)
		testKit.MustExec(`set @@max_execution_time=2000;`)
		testKit.MustExec(`create table foo (a int);`)
		testKit.MustExec(`create session binding for select * from foo where a = 1 using select /*+ set_var(max_execution_time=1234) */ * from foo where a = 1;`)
		testKit.MustExec(`select /*+ set_var(max_execution_time=2222) */ * from foo where a = 1;`)
		testKit.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("1"))
		testKit.MustQuery("select @@max_execution_time;").Check(testkit.Rows("2000"))
	})
}

func TestSetVarHintsWithExplain(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec(`use test`)

		testKit.MustExec(`set @@max_execution_time=2000;`)
		testKit.MustExec(`explain select /*+ set_var(max_execution_time=100) */ @@max_execution_time;`)
		testKit.MustQuery("select @@max_execution_time;").Check(testkit.Rows("2000"))

		testKit.MustExec(`create table t(a int);`)
		testKit.MustExec(`create global binding for select * from t where a = 1 and sleep(0.1) using select /*+ SET_VAR(max_execution_time=500) */ * from t where a = 1 and sleep(0.1);`)
		testKit.MustExec(`select * from t where a = 1 and sleep(0.1);`)
		testKit.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("1"))
		testKit.MustQuery("select @@max_execution_time;").Check(testkit.Rows("2000"))

		testKit.MustExec(`explain select * from t where a = 1 and sleep(0.1);`)
		testKit.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("1"))
		testKit.MustQuery("select @@max_execution_time;").Check(testkit.Rows("2000"))
	})
}

func TestWriteSlowLogHint(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec(`use test`)
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
}
