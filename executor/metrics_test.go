// Copyright 2017-present PingCAP, Inc.
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
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testSuite) TestStmtLabel(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table label (c1 int primary key, c2 int, c3 int, index (c2))")
	tests := []struct {
		sql   string
		label string
	}{
		{"select 1", "SelectSimpleFull"},
		{"select * from label t1, label t2", "SelectJoinFull"},
		{"select * from label t1 where t1.c3 > (select count(t1.c1 = t2.c1) = 0 from label t2)", "SelectApplyFull"},
		{"select count(*) from label", "SelectAggFull"},
		{"select * from label where c2 = 1", "SelectIndexRange"},
		{"select c1, c2 from label where c2 = 1", "SelectIndexOnlyRange"},
		{"select * from label where c1 > 1", "SelectTableRange"},
		{"select * from label where c1 > 1", "SelectTableRange"},
		{"select * from label order by c3 limit 1", "SelectTableFullOrderLimit"},
		{"delete from label", "DeleteTableFull"},
		{"delete from label where c1 = 1", "DeleteTableRange"},
		{"delete from label where c2 = 1", "DeleteIndexRange"},
		{"delete from label where c2 = 1 order by c3 limit 1", "DeleteIndexRangeOrderLimit"},
		{"update label set c3 = 3", "UpdateTableFull"},
		{"update label set c3 = 3 where c1 = 1", "UpdateTableRange"},
		{"update label set c3 = 3 where c2 = 1", "UpdateIndexRange"},
		{"update label set c3 = 3 where c2 = 1 order by c3 limit 1", "UpdateIndexRangeOrderLimit"},
	}
	for _, tt := range tests {
		stmtNode, err := parser.New().ParseOneStmt(tt.sql, "", "")
		c.Check(err, IsNil)
		is := executor.GetInfoSchema(tk.Se)
		c.Assert(plan.Preprocess(stmtNode, is, tk.Se), IsNil)
		p, err := plan.Optimize(tk.Se, stmtNode, is)
		c.Assert(err, IsNil)
		c.Assert(executor.StatementLabel(stmtNode, p), Equals, tt.label)
	}
}
