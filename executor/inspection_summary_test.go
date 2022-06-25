// Copyright 2020 PingCAP, Inc.
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
	"context"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/set"
	"github.com/stretchr/testify/require"
)

func TestValidInspectionSummaryRules(t *testing.T) {
	for rule, tbls := range executor.InspectionSummaryRules {
		tables := set.StringSet{}
		for _, tbl := range tbls {
			require.False(t, tables.Exist(tbl), "duplicate table name: %v in rule: %v", tbl, rule)
			tables.Insert(tbl)

			_, found := infoschema.MetricTableMap[tbl]
			require.True(t, found, "metric table %v not define", tbl)
		}
	}
}

func TestInspectionSummary(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	fpName := "github.com/pingcap/tidb/executor/mockMetricsTableData"
	require.NoError(t, failpoint.Enable(fpName, "return"))
	defer func() { require.NoError(t, failpoint.Disable(fpName)) }()

	datetime := func(s string) types.Time {
		time, err := types.ParseTime(tk.Session().GetSessionVars().StmtCtx, s, mysql.TypeDatetime, types.MaxFsp)
		require.NoError(t, err)
		return time
	}

	// construct some mock data
	mockData := map[string][][]types.Datum{
		// columns: time, instance, type, result, value
		"tidb_qps": {
			types.MakeDatums(datetime("2020-02-12 10:35:00"), "tidb-0", "Query", "OK", 0.0),
			types.MakeDatums(datetime("2020-02-12 10:36:00"), "tidb-0", "Query", "Error", 1.0),
			types.MakeDatums(datetime("2020-02-12 10:37:00"), "tidb-1", "Quit", "Error", 5.0),
			types.MakeDatums(datetime("2020-02-12 10:37:00"), "tidb-1", "Quit", "Error", 9.0),
		},
		// columns: time, instance, sql_type, quantile, value
		"tidb_query_duration": {
			types.MakeDatums(datetime("2020-02-12 10:35:00"), "tikv-0", "Select", 0.99, 0.0),
			types.MakeDatums(datetime("2020-02-12 10:36:00"), "tikv-1", "Update", 0.99, 1.0),
			types.MakeDatums(datetime("2020-02-12 10:36:00"), "tikv-1", "Update", 0.99, 3.0),
			types.MakeDatums(datetime("2020-02-12 10:37:00"), "tikv-2", "Delete", 0.99, 5.0),
		},
	}

	ctx := context.WithValue(context.Background(), "__mockMetricsTableData", mockData)
	ctx = failpoint.WithHook(ctx, func(_ context.Context, fpname string) bool {
		return fpName == fpname
	})

	rs, err := tk.Session().Execute(ctx, "select * from information_schema.inspection_summary where rule='query-summary' and metrics_name in ('tidb_qps', 'tidb_query_duration')")
	require.NoError(t, err)
	result := tk.ResultSetToResultWithCtx(ctx, rs[0], "execute inspect SQL failed")
	require.Equal(t, uint16(0), tk.Session().GetSessionVars().StmtCtx.WarningCount(), "unexpected warnings: %+v", tk.Session().GetSessionVars().StmtCtx.GetWarnings())
	result.Check(testkit.Rows(
		"query-summary tikv-0 tidb_query_duration Select 0.99 0 0 0 The quantile of TiDB query durations(second)",
		"query-summary tikv-1 tidb_query_duration Update 0.99 2 1 3 The quantile of TiDB query durations(second)",
		"query-summary tikv-2 tidb_query_duration Delete 0.99 5 5 5 The quantile of TiDB query durations(second)",
		"query-summary tidb-0 tidb_qps Query, Error <nil> 1 1 1 TiDB query processing numbers per second",
		"query-summary tidb-0 tidb_qps Query, OK <nil> 0 0 0 TiDB query processing numbers per second",
		"query-summary tidb-1 tidb_qps Quit, Error <nil> 7 5 9 TiDB query processing numbers per second",
	))

	// Test for select * from information_schema.inspection_summary without specify rules.
	rs, err = tk.Session().Execute(ctx, "select * from information_schema.inspection_summary where metrics_name = 'tidb_qps'")
	require.NoError(t, err)
	result = tk.ResultSetToResultWithCtx(ctx, rs[0], "execute inspect SQL failed")
	require.Equal(t, uint16(0), tk.Session().GetSessionVars().StmtCtx.WarningCount(), "unexpected warnings: %+v", tk.Session().GetSessionVars().StmtCtx.GetWarnings())
	result.Check(testkit.Rows(
		"query-summary tidb-0 tidb_qps Query, Error <nil> 1 1 1 TiDB query processing numbers per second",
		"query-summary tidb-0 tidb_qps Query, OK <nil> 0 0 0 TiDB query processing numbers per second",
		"query-summary tidb-1 tidb_qps Quit, Error <nil> 7 5 9 TiDB query processing numbers per second",
	))
}
