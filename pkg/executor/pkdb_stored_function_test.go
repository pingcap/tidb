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
	"github.com/stretchr/testify/require"
)

func TestStoredFunctionReturnValueLeakAcrossRows(t *testing.T) {
	// Reproduce the "execution context leak across rows" problem for stored functions.
	//
	// The stored function is evaluated multiple times (per row) within *one* SQL statement.
	// If the procedure execution context (ProcedureCtx) is reused across rows without reset/clone,
	// the 2nd evaluation can incorrectly reuse the return value from the 1st evaluation.
	//
	// The function below only returns on one code path. When p=0, MySQL raises error 1321:
	//   "FUNCTION <db>.<name> ended without RETURN"
	//
	// With the leak bug, the 2nd row (p=0) silently returns the previous return value instead of error.
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")

	tk.MustExec("create function sf_ret_leak_rows(p int) returns int begin if p = 1 then return 1; end if; end;")
	tk.MustQuery("select sf_ret_leak_rows(1)").Check(testkit.Rows("1"))

	tk.MustExec("drop table if exists t_sf_ret_leak_rows")
	tk.MustExec("create table t_sf_ret_leak_rows(id int primary key, p int)")
	tk.MustExec("insert into t_sf_ret_leak_rows values (1, 1), (2, 0)")

	query := "select sf_ret_leak_rows(p) from t_sf_ret_leak_rows order by id"
	err := tk.QueryToErr(query)
	if err == nil {
		// On buggy implementation, the 2nd row reuses the return value from the 1st row and the query succeeds.
		rows := tk.MustQuery(query).Rows()
		t.Fatalf("expected \"ended without RETURN\" error on 2nd row, but query succeeded with rows %v", rows)
	}
	require.Contains(t, err.Error(), "ended without RETURN")
}
