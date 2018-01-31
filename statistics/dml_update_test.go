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

package statistics_test

import (
	"fmt"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testStatsUpdateSuite) TestDMLUpdate(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	statistics.RunDynamicUpdate = true
	defer func() {
		statistics.RunDynamicUpdate = false
	}()
	testKit.MustExec("use test")
	testKit.MustExec("create table t (a int, b int, primary key(a), index idx(a,b))")
	testKit.MustExec("insert into t values (1,1), (3,3)")
	testKit.MustExec("analyze table t")

	do := s.do
	is := do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo := tbl.Meta()
	colID, idxID := tableInfo.Columns[0].ID, tableInfo.Indices[0].ID
	h := do.StatsHandle()
	t := h.GetTableStats(tableInfo.ID)
	sc := &stmtctx.StatementContext{}

	testKit.MustExec("insert into t values (2,2)")
	testKit.MustExec("insert into t values (0,0)")
	delta := h.GetStatsDelta(tableInfo.ID)
	newTable := delta.UpdateTable(sc, t)
	c.Assert(newTable.Columns[colID].String(), Equals, "column:1 ndv:2\n[0, 1] 1 2\n[2, 3] 1 4")
	c.Assert(newTable.Indices[idxID].String(), Equals, "index:1 ndv:2\n[(0, 0), (1, 1)] 1 2\n[(2, 2), (3, 3)] 1 4")

	testKit.MustExec("begin")
	for i := 0; i < 2048; i++ {
		testKit.MustExec(fmt.Sprintf("insert into t values (%d, %d)", i+4, i+4))
	}
	testKit.MustExec("commit")
	delta = h.GetStatsDelta(tableInfo.ID)
	newTable = delta.UpdateTable(sc, t)
	c.Assert(newTable.Columns[colID].String(), Equals, "column:1 ndv:2\n[0, 1] 1 2\n[2, 2051] 1 2052")
	c.Assert(newTable.Indices[idxID].String(), Equals, "index:1 ndv:2\n[(0, 0), (1, 1)] 1 2\n[(2, 2), (2051, 2051)] 1 2052")

	testKit.MustExec("delete from t where a >= 4")
	delta = h.GetStatsDelta(tableInfo.ID)
	newTable = delta.UpdateTable(sc, t)
	c.Assert(newTable.Columns[colID].String(), Equals, "column:1 ndv:2\n[0, 1] 1 2\n[2, 2051] 1 4")
	c.Assert(newTable.Indices[idxID].String(), Equals, "index:1 ndv:2\n[(0, 0), (1, 1)] 1 2\n[(2, 2), (2051, 2051)] 1 4")

	testKit.MustExec("delete from t where a = 0")
	testKit.MustExec("delete from t where a = 2")
	delta = h.GetStatsDelta(tableInfo.ID)
	newTable = delta.UpdateTable(sc, t)
	c.Assert(newTable.Columns[colID].String(), Equals, "column:1 ndv:2\n[0, 1] 1 1\n[2, 2051] 1 2")
	c.Assert(newTable.Indices[idxID].String(), Equals, "index:1 ndv:2\n[(0, 0), (1, 1)] 1 1\n[(2, 2), (2051, 2051)] 1 2")

	testKit.MustExec("update t set a = a - 2")
	delta = h.GetStatsDelta(tableInfo.ID)
	newTable = delta.UpdateTable(sc, t)
	c.Assert(newTable.Columns[colID].String(), Equals, "column:1 ndv:2\n[-1, 1] 1 2\n[2, 2051] 1 2")
	c.Assert(newTable.Indices[idxID].String(), Equals, "index:1 ndv:2\n[(-1, 1), (1, 1)] 1 1\n[(1, 3), (2051, 2051)] 1 2")

	testKit.MustExec("replace into t(a, b) values (1, 0)")
	delta = h.GetStatsDelta(tableInfo.ID)
	newTable = delta.UpdateTable(sc, t)
	c.Assert(newTable.Columns[colID].String(), Equals, "column:1 ndv:2\n[-1, 1] 1 2\n[2, 2051] 1 2")
	c.Assert(newTable.Indices[idxID].String(), Equals, "index:1 ndv:2\n[(-1, 1), (1, 1)] 1 2\n[(1, 3), (2051, 2051)] 1 2")
}
