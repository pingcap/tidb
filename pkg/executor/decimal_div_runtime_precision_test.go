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

package executor_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

func TestDecimalDivRuntimePrecision(t *testing.T) {
	for _, vectorized := range []string{"OFF", "ON"} {
		t.Run(vectorized, func(t *testing.T) {
			store := testkit.CreateMockStore(t)
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("USE test")
			tk.MustExec("SET @@session.tidb_allow_mpp = 0")
			tk.MustExec("SET @@session.tidb_enable_vectorized_expression = " + vectorized)
			tk.MustExec("CREATE TABLE t (id INT, num DECIMAL)")
			tk.MustExec("INSERT INTO t VALUES (1, 100)")

			tk.MustQuery("SELECT CAST(CAST('1.2300' AS DECIMAL(10,4)) AS CHAR)").Check(testkit.Rows("1.2300"))
			tk.MustQuery("SELECT /*+ READ_FROM_STORAGE(TIKV[t]) */ LENGTH(SUM(num) / 10) FROM t GROUP BY id").Check(testkit.Rows("7"))
			tk.MustQuery("SELECT /*+ READ_FROM_STORAGE(TIKV[t]) */ CAST(SUM(num) / 10 AS CHAR) FROM t GROUP BY id").Check(testkit.Rows("10.0000"))
		})
	}
}
