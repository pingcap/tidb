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

package executor

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/set"
	"github.com/stretchr/testify/require"
)

type planChangeFakeRestrictedSQLExecutor struct {
	stmtRows     []chunk.Row
	histRows     []chunk.Row
	slowRows     []chunk.Row
	resultFields []*resolve.ResultField
}

func (e *planChangeFakeRestrictedSQLExecutor) ParseWithParams(_ context.Context, _ string, _ ...any) (ast.StmtNode, error) {
	return nil, nil
}

func (e *planChangeFakeRestrictedSQLExecutor) ExecRestrictedStmt(_ context.Context, _ ast.StmtNode, _ ...sqlexec.OptionFuncAlias) ([]chunk.Row, []*resolve.ResultField, error) {
	return nil, nil, nil
}

func (e *planChangeFakeRestrictedSQLExecutor) ExecRestrictedSQL(_ context.Context, _ []sqlexec.OptionFuncAlias, sql string, _ ...any) ([]chunk.Row, []*resolve.ResultField, error) {
	sqlLower := strings.ToLower(sql)
	switch {
	case strings.Contains(sqlLower, "cluster_statements_summary_history"):
		return e.histRows, e.resultFields, nil
	case strings.Contains(sqlLower, "cluster_statements_summary"):
		return e.stmtRows, e.resultFields, nil
	case strings.Contains(sqlLower, "cluster_slow_query"):
		return e.slowRows, e.resultFields, nil
	}
	return nil, nil, nil
}

type planChangeFakeSession struct {
	*mock.Context
	exec sqlexec.RestrictedSQLExecutor
}

func (s *planChangeFakeSession) GetRestrictedSQLExecutor() sqlexec.RestrictedSQLExecutor {
	return s.exec
}

func makePlanChangeStmtSummaryRow(digest, planDigest string, latency uint64, query string) chunk.Row {
	return chunk.MutRowFromDatums([]types.Datum{
		types.NewStringDatum(digest),
		types.NewStringDatum(planDigest),
		types.NewUintDatum(latency),
		types.NewStringDatum(query),
	}).ToRow()
}

func makePlanChangeSlowQueryRow(digest, planDigest string, latency float64, query string) chunk.Row {
	return chunk.MutRowFromDatums([]types.Datum{
		types.NewStringDatum(digest),
		types.NewStringDatum(planDigest),
		types.NewFloat64Datum(latency),
		types.NewStringDatum(query),
	}).ToRow()
}

func newPlanChangeFakeSession(t *testing.T) *planChangeFakeSession {
	t.Helper()
	ctx := mock.NewContext()
	return &planChangeFakeSession{Context: ctx}
}

func newPlanChangeTimeFilter() inspectionFilter {
	from := time.Date(2020, 2, 14, 5, 10, 0, 0, time.UTC)
	to := time.Date(2020, 2, 14, 5, 25, 0, 0, time.UTC)
	return inspectionFilter{timeRange: plannerutil.QueryTimeRange{From: from, To: to}}
}

func TestPlanChangeInspectionFromStmtSummary(t *testing.T) {
	sctx := newPlanChangeFakeSession(t)
	sctx.exec = &planChangeFakeRestrictedSQLExecutor{
	stmtRows: []chunk.Row{
		makePlanChangeStmtSummaryRow("digest1", "plan1", 1e9, "SELECT * FROM t1"),
		makePlanChangeStmtSummaryRow("digest1", "plan2", 3e9, "SELECT * FROM t1"),
		makePlanChangeStmtSummaryRow("digest2", "plan3", 5e8, "SELECT * FROM t2"),
		makePlanChangeStmtSummaryRow("digest3", "plan4", 2e9, "SELECT * FROM t3"),
		makePlanChangeStmtSummaryRow("digest3", "plan5", 3e9, "SELECT * FROM t3"),
	},
	}

	results := planChangeInspection{}.inspect(context.Background(), sctx, newPlanChangeTimeFilter())
	require.Equal(t, uint16(0), sctx.GetSessionVars().StmtCtx.WarningCount())
	require.Len(t, results, 1)
	require.Equal(t, "digest1", results[0].item)
	require.Equal(t, "3.00", results[0].actual)
	require.Equal(t, "tidb", results[0].tp)
	require.Equal(t, "warning", results[0].severity)
	require.Equal(t, "< 2.00", results[0].expected)
}

func TestPlanChangeInspectionFromSlowQuery(t *testing.T) {
	sctx := newPlanChangeFakeSession(t)
	sctx.exec = &planChangeFakeRestrictedSQLExecutor{
	slowRows: []chunk.Row{
		makePlanChangeSlowQueryRow("digest1", "plan1", 1.0, "SELECT * FROM t1"),
		makePlanChangeSlowQueryRow("digest1", "plan2", 3.0, "SELECT * FROM t1"),
		makePlanChangeSlowQueryRow("digest2", "plan3", 0.5, "SELECT * FROM t2"),
		makePlanChangeSlowQueryRow("digest3", "plan4", 2.0, "SELECT * FROM t3"),
		makePlanChangeSlowQueryRow("digest3", "plan5", 3.0, "SELECT * FROM t3"),
	},
	}

	results := planChangeInspection{}.inspect(context.Background(), sctx, newPlanChangeTimeFilter())
	require.Equal(t, uint16(0), sctx.GetSessionVars().StmtCtx.WarningCount())
	require.Len(t, results, 1)
	require.Equal(t, "digest1", results[0].item)
	require.Equal(t, "3.00", results[0].actual)
}

func TestPlanChangeInspectionItemFilter(t *testing.T) {
	sctx := newPlanChangeFakeSession(t)
	sctx.exec = &planChangeFakeRestrictedSQLExecutor{
		stmtRows: []chunk.Row{
			makePlanChangeStmtSummaryRow("digest1", "plan1", 1e9, "SELECT * FROM t1"),
			makePlanChangeStmtSummaryRow("digest1", "plan2", 3e9, "SELECT * FROM t1"),
			makePlanChangeStmtSummaryRow("digest2", "plan3", 1e9, "SELECT * FROM t2"),
			makePlanChangeStmtSummaryRow("digest2", "plan4", 5e9, "SELECT * FROM t2"),
		},
	}

	filter := inspectionFilter{
		set:       set.NewStringSet("digest2"),
		timeRange: newPlanChangeTimeFilter().timeRange,
	}
	results := planChangeInspection{}.inspect(context.Background(), sctx, filter)
	require.Len(t, results, 1)
	require.Equal(t, "digest2", results[0].item)
	require.Equal(t, "5.00", results[0].actual)
}

func TestPlanChangeInspectionNoResult(t *testing.T) {
	sctx := newPlanChangeFakeSession(t)
	sctx.exec = &planChangeFakeRestrictedSQLExecutor{}

	results := planChangeInspection{}.inspect(context.Background(), sctx, newPlanChangeTimeFilter())
	require.Equal(t, uint16(0), sctx.GetSessionVars().StmtCtx.WarningCount())
	require.Empty(t, results)
}

var _ sessionctx.Context = (*planChangeFakeSession)(nil)
var _ sqlexec.RestrictedSQLExecutor = (*planChangeFakeRestrictedSQLExecutor)(nil)
