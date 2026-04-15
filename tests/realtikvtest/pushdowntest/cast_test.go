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

package pushdowntest

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
)

// TestIssue44135WithTiKV tests that CAST(float AS DATETIME) returns consistent results
// for both generated columns and base columns when running with real TiKV.
// See https://github.com/pingcap/tidb/issues/44135
func TestIssue44135WithTiKV(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// Case 1: With generated column
	tk.MustExec("DROP TABLE IF EXISTS t0")
	tk.MustExec("CREATE TABLE t0(`c0` float GENERATED ALWAYS AS (`c1`), `c1` float)")
	tk.MustExec("INSERT INTO t0(c1) VALUES (0.5822439)")
	result1 := tk.MustQuery("SELECT * FROM t0 WHERE (~ (CAST(c0 AS DATETIME)))")

	// Case 2: Without generated column (base column only)
	tk.MustExec("DROP TABLE IF EXISTS t1")
	tk.MustExec("CREATE TABLE t1(`c1` float)")
	tk.MustExec("INSERT INTO t1(c1) VALUES (0.5822439)")
	result2 := tk.MustQuery("SELECT * FROM t1 WHERE (~ (CAST(c1 AS DATETIME)))")

	// Both queries should return consistent results (either both return the row or both return empty)
	rows1 := result1.Rows()
	rows2 := result2.Rows()
	require.Equal(t, len(rows1), len(rows2), "CAST(float AS DATETIME) should return consistent results for generated column and base column")
}
