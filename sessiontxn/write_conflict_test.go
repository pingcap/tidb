// Copyright 2022 PingCAP, Inc.
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

package sessiontxn_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/testfork"
)

var pessimisticNormalBreakPoints = []string{
	sessiontxn.BreakPointBeforeExecutorFirstBuild,
	sessiontxn.BreakPointBeforeExecutorFirstRun,
}

var pessimisticRetryBreakPoints = []string{
	sessiontxn.BreakPointBeforeOnStmtRetryAfterLockError,
	sessiontxn.BreakPointBeforeExecutorRebuildWhenLockError,
	sessiontxn.BreakPointBeforeExecutorRerunWhenLockError,
}

func TestPessimisticUpdateConflict(t *testing.T) {
	queries := []string{
		"update t set v=v+1 where id=1",
		"update t set v=v+1 where id=1 and v>0",
		"update t set v=v+1 where id in (1, 2, 3)",
		"update t set v=v+1 where id in (1, 2, 3) and v>0",
		"update t set v=v+1",
		"update t set v=v+1 where v>0",
	}

	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			testPessimisticWriteConflict(t, query, 1)
		})
	}
}

func TestPessimisticSelectForUpdateConflict(t *testing.T) {
	queries := []string{
		"select * from t where id=1 for update",
		"select * from t where id=1 and v>0 for update",
		"select * from t where id=1 for update union select * from t where id=1 for update",
		"select * from t where id in (1, 2, 3) for update",
		"select * from t where id in (1, 2, 3) and v > 0 for update",
		"select * from t for update",
		"select * from t where v > 0 for update",
	}

	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			testPessimisticWriteConflict(t, query, 0)
		})
	}
}

func testPessimisticWriteConflict(t *testing.T, query string, valueChange int) {
	store, _, deferFunc := setupTxnContextTest(t)
	defer deferFunc()

	allBreakPoints := append([]string{}, pessimisticNormalBreakPoints...)
	allBreakPoints = append(allBreakPoints, pessimisticRetryBreakPoints...)

	testfork.RunTest(t, func(t *testfork.T) {
		var records []string
		t.Desc = func() string {
			var sb strings.Builder
			for _, item := range records {
				sb.WriteString(item)
				sb.WriteString("\n")
			}
			return sb.String()
		}

		isolation := testfork.PickEnum(t, ast.RepeatableRead, ast.ReadCommitted)
		autocommit := testfork.PickEnum(t, 1, 0)

		tk := testkit.NewSteppedTestKit(t, store)
		tk.SetBreakPoints(allBreakPoints...)
		defer tk.MustExec("rollback")

		tk.MustExec("use test")
		tk.MustExec("drop table if exists t")
		tk.MustExec("create table t(id int primary key, v int)")
		tk.MustExec("insert into t values(1, 10)")

		tk2 := testkit.NewTestKit(t, store)
		tk2.MustExec("use test")

		tk.MustExec(fmt.Sprintf("set tx_isolation='%s'", isolation))
		tk.MustExec(fmt.Sprintf("set tidb_txn_mode='pessimistic'"))
		tk.MustExec(fmt.Sprintf("set autocommit=%d", autocommit))
		if autocommit == 1 {
			tk.MustExec("begin")
		}

		isSelectForUpdate := strings.HasPrefix(strings.ToLower(strings.TrimSpace(query)), "select")
		expectedValue := 10 + valueChange
		if isSelectForUpdate {
			tk.SteppedMustQuery(query)
		}

		maxRetry := 2
		for retry := 0; retry <= maxRetry; retry++ {
			var path []string
			if retry == 0 {
				path = pessimisticNormalBreakPoints
				records = append(records, fmt.Sprintf("START %s autocommit-%d '%s'", isolation, autocommit, query))
			} else {
				path = pessimisticRetryBreakPoints
				records = append(records, fmt.Sprintf("retry%d", retry))
			}

			for _, breakPoint := range path {
				tk.ExpectStopOnBreakPoint(breakPoint)
				if retry < maxRetry && testfork.PickEnum(t, false, true) {
					expectedValue += 1
					records = append(records, fmt.Sprintf("    -> %s tk2 +1 => %d", breakPoint, expectedValue))
					tk2.MustExec("update t set v=v+1 where id=1")
				} else {
					records = append(records, "    -> "+breakPoint)
				}
				tk.Continue()
			}

			if tk.IsIdle() {
				break
			}
		}

		tk.ExpectIdle()
		if isSelectForUpdate {
			tk.GetQueryResult().Check(testkit.Rows(fmt.Sprintf("1 %d", expectedValue)))
		}
		tk.MustExec("commit")
		tk.MustExec("set autocommit=1")
		tk.MustQuery("select * from t where id=1").Check(testkit.Rows(fmt.Sprintf("1 %d", expectedValue)))
	})
}
