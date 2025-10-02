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

package tiflashtest

import (
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/external"
	"github.com/stretchr/testify/require"
)

func TestTiFlashDivisionAsPredicate(t *testing.T) {
	store := testkit.CreateMockStore(t, withMockTiFlash(2))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// checkConsistency is a helper function to check if a query returns the same result in TiDB and TiFlash(MPP).
	checkConsistency := func(query string) {
		tk.MustExec("set tidb_enforce_mpp = 0")
		tidbResult := tk.MustQuery(query).Rows()
		tk.MustExec("set tidb_enforce_mpp = 1")
		tiflashResult := tk.MustQuery(query).Rows()
		require.Equal(t, tidbResult, tiflashResult, "Query should return consistent results: %s", query)
	}

	tk.MustExec(`drop table if exists t_div`)
	tk.MustExec(`create table t_div (
		c_int int,
		c_decimal decimal(10,5),
		c_float float,
		c_double double
	)`)
	tk.MustExec("alter table t_div set tiflash replica 1")
	tb := external.GetTableByName(t, tk, "test", "t_div")
	err := domain.GetDomain(tk.Session()).DDLExecutor().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)

	tk.MustExec(`insert into t_div values
		(65535, 65535.00001, 65535.5, 65535.123456),
		(1, 1.00001, 1.5, 1.123456),
		(2, 2.00001, 2.5, 2.123456),
		(100, 100.123, 100.5, 100.987),
		(32767, 32767.1, 32767.5, 32767.4)
	`)

	queries := []string{
		"select c_int from t_div where (1/c_int)",
		"select count(*) from t_div where (1/c_int)",
		"select count(*) from t_div where (1/c_decimal)",
		"select count(*) from t_div where (1/c_float)",
		"select count(*) from t_div where (1/c_double)",
		"select count(*) from t_div where (1/c_int) > 0",
		"select count(*) from t_div where (1/c_int) and (2/c_int)",
	}

	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			checkConsistency(query)
		})
	}

	t.Run("DivisionByZero", func(t *testing.T) {
		tk.MustExec("insert into t_div (c_int) values(0)")
		// In non-strict SQL mode, division by zero returns NULL. NULL as a boolean predicate is FALSE.
		tk.MustExec("set sql_mode = ''")
		checkConsistency("select count(*) from t_div where (1/c_int)")

		// In strict SQL mode, division by zero should produce a warning in TiDB, but the query will still complete.
		// We check for consistency of the final result.
		tk.MustExec("set sql_mode = 'ERROR_FOR_DIVISION_BY_ZERO'")
		checkConsistency("select count(*) from t_div where (1/c_int)")
	})
}
