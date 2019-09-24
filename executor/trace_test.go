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
// See the License for the specific language governing permissions and
// limitations under the License.

package executor_test

import (
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testSuite1) TestTraceExec(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	testSQL := `create table trace (id int PRIMARY KEY AUTO_INCREMENT, c1 int, c2 int, c3 int default 1);`
	tk.MustExec(testSQL)
	tk.MustExec("trace insert into trace (c1, c2, c3) values (1, 2, 3)")
	rows := tk.MustQuery("trace select * from trace where id = 0;").Rows()
	c.Assert(rows, HasLen, 1)

	// +---------------------------+-----------------+------------+
	// | operation                 | startTS         | duration   |
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
	c.Assert(len(rows) > 1, IsTrue)
	c.Assert(rowsOrdered(rows), IsTrue)
}

func rowsOrdered(rows [][]interface{}) (ordered bool) {
	for idx := range rows {
		if idx == 0 || !isSibling(rows[idx-1][0].(string), rows[idx][0].(string)) {
			continue
		}

		if rows[idx-1][1].(string) > rows[idx][1].(string) {
			return false
		}
	}
	return true
}

func isSibling(x string, y string) bool {
	indexF := func(c rune) bool {
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') {
			return false
		}
		return true
	}
	return strings.IndexFunc(x, indexF) == strings.IndexFunc(y, indexF)
}
