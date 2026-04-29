// Copyright 2018 PingCAP, Inc.
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
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestTraceExec(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	testSQL := `create table trace (id int PRIMARY KEY AUTO_INCREMENT, c1 int, c2 int, c3 int default 1);`
	tk.MustExec(testSQL)
	tk.MustExec("trace insert into trace (c1, c2, c3) values (1, 2, 3)")
	rows := tk.MustQuery("trace select * from trace where id = 0;").Rows()
	require.GreaterOrEqual(t, len(rows), 1)

	// +---------------------------+-----------------+------------+
	// | operation                 | snapshotTS      | duration   |
	// +---------------------------+-----------------+------------+
	// | session.getTxnFuture      | 22:08:38.247834 | 78.909µs   |
	// |   ├─session.Execute       | 22:08:38.247829 | 1.478487ms |
	// |   ├─session.ParseSQL      | 22:08:38.248457 | 71.159µs   |
	// |   ├─executor.Compile      | 22:08:38.248578 | 45.329µs   |
	// |   ├─session.runStmt       | 22:08:38.248661 | 75.13µs    |
	// |   ├─session.CommitTxn     | 22:08:38.248699 | 13.213µs   |
	// |   └─recordSet.Next        | 22:08:38.249340 | 155.317µs  |
	// +---------------------------+-----------------+------------+
	rows = tk.MustQuery("trace format='row' select * from trace where id = 0;").Rows()
	require.Greater(t, len(rows), 1)
	require.True(t, rowsOrdered(rows))

	rows = tk.MustQuery("trace format='row' delete from trace where id = 0").Rows()
	require.Greater(t, len(rows), 1)
	require.True(t, rowsOrdered(rows))

	rows = tk.MustQuery("trace format='row' analyze table trace").Rows()
	require.Greater(t, len(rows), 1)
	require.True(t, rowsOrdered(rows))

	tk.MustExec("trace format='log' insert into trace (c1, c2, c3) values (1, 2, 3)")
	rows = tk.MustQuery("trace format='log' select * from trace where id = 0;").Rows()
	require.GreaterOrEqual(t, len(rows), 1)
}

func TestTraceExecStoredFunctionIncludesRoutineSpans(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")
	tk.MustExec("create table trace_inner (id int primary key, v int)")
	tk.MustExec("insert into trace_inner values (1, 42)")
	tk.MustExec(`create function fn_trace_reads_sql() returns int reads sql data
begin
	declare outv int default 0;
	select v into outv from trace_inner where id = 1;
	return outv;
end`)

	rows := tk.MustQuery("trace format='row' select fn_trace_reads_sql();").Rows()
	require.True(t, traceRowsContainOperation(rows, func(op string) bool {
		return op == "routine.function test.fn_trace_reads_sql"
	}), "rows=%v", rows)
	require.True(t, traceRowsContainOperation(rows, func(op string) bool {
		return strings.HasPrefix(op, "routine.statement SELECT") && strings.Contains(op, "`trace_inner`")
	}), "rows=%v", rows)
}

func TestTraceExecNestedStoredFunctionTracksRoutineHierarchy(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")
	tk.MustExec(`create function fn_trace_inner_nested() returns int
begin
	return 7;
end`)
	tk.MustExec(`create function fn_trace_outer_nested() returns int
begin
	declare outv int default 0;
	select fn_trace_inner_nested() into outv;
	return outv;
end`)

	rows := tk.MustQuery("trace format='row' select fn_trace_outer_nested();").Rows()
	outerDepth, ok := traceRowFindOperationDepth(rows, func(op string) bool {
		return op == "routine.function test.fn_trace_outer_nested"
	})
	require.True(t, ok, "rows=%v", rows)
	outerStmtDepth, ok := traceRowFindOperationDepth(rows, func(op string) bool {
		return strings.HasPrefix(op, "routine.statement SELECT") && strings.Contains(op, "fn_trace_inner_nested")
	})
	require.True(t, ok, "rows=%v", rows)
	innerDepth, ok := traceRowFindOperationDepth(rows, func(op string) bool {
		return op == "routine.function test.fn_trace_inner_nested"
	})
	require.True(t, ok, "rows=%v", rows)
	require.Greater(t, outerStmtDepth, outerDepth, "rows=%v", rows)
	require.Greater(t, innerDepth, outerStmtDepth, "rows=%v", rows)
}

func rowsOrdered(rows [][]any) bool {
	for idx := range rows {
		if _, ok := rows[idx][1].(string); !ok {
			return false
		}
		if idx == 0 {
			continue
		}
		if rows[idx-1][1].(string) > rows[idx][1].(string) {
			return false
		}
	}
	return true
}

func traceRowsContainOperation(rows [][]any, pred func(string) bool) bool {
	for _, row := range rows {
		_, op, ok := traceRowDepthAndOperation(row)
		if ok && pred(op) {
			return true
		}
	}
	return false
}

func traceRowFindOperationDepth(rows [][]any, pred func(string) bool) (int, bool) {
	for _, row := range rows {
		depth, op, ok := traceRowDepthAndOperation(row)
		if ok && pred(op) {
			return depth, true
		}
	}
	return 0, false
}

func traceRowDepthAndOperation(row []any) (int, string, bool) {
	if len(row) == 0 {
		return 0, "", false
	}
	op, ok := row[0].(string)
	if !ok {
		return 0, "", false
	}
	return traceOperationDepth(op), strings.TrimLeft(op, " │├└─"), true
}

func traceOperationDepth(op string) int {
	if op == "" {
		return 0
	}
	runes := []rune(op)
	decorations := 0
	for len(runes) >= 2 {
		token := string(runes[:2])
		switch token {
		case "  ", "│ ", "├─", "└─":
			decorations++
			runes = runes[2:]
		default:
			if decorations == 0 {
				return 0
			}
			return decorations - 1
		}
	}
	if decorations == 0 {
		return 0
	}
	return decorations - 1
}

func TestTracePlanStmt(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table tp1(id int);")
	tk.MustExec("create table tp2(id int);")
	tk.MustExec("set @@tidb_cost_model_version=2")
	rows := tk.MustQuery("trace plan select * from tp1 t1, tp2 t2 where t1.id = t2.id").Rows()
	require.Len(t, rows, 1)
	require.Len(t, rows[0], 1)
	require.Regexp(t, ".*zip", rows[0][0])
}
