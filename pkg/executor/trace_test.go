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
