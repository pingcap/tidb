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

package usage_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestCleanupPredicateColumns(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	// Create table and select data with predicate.
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("insert into t values (1, 1), (2, 2), (3, 3)")
	// Enable column tracking.
	tk.MustExec("set global tidb_enable_column_tracking = 1")
	tk.MustQuery("select * from t where a > 1").Check(testkit.Rows("2 2", "3 3"))
	tk.MustQuery("select * from t where b > 1").Check(testkit.Rows("2 2", "3 3"))

	// Dump the statistics usage.
	h := dom.StatsHandle()
	err := h.DumpColStatsUsageToKV()
	require.NoError(t, err)

	// Check the statistics usage.
	rows := tk.MustQuery("select * from mysql.column_stats_usage").Rows()
	require.Len(t, rows, 2)

	// Drop column b.
	tk.MustExec("alter table t drop column b")
	// Get table ID.
	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	columns, err := h.GetPredicateColumns(tbl.Meta().ID)
	require.NoError(t, err)
	require.Len(t, columns, 1)
}
