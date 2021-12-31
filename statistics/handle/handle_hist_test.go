// Copyright 2021 PingCAP, Inc.
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

package handle_test

import (
	"time"

	"github.com/cznic/mathutil"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/util/testkit"
)

var _ = Suite(&testLoadHistSuite{})

type testLoadHistSuite struct {
	testSuiteBase
}

func (s *testLoadHistSuite) TestConcurrentLoadHist(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	testKit.MustExec("set @@session.tidb_analyze_version=2")
	testKit.MustExec("create table t(a int, b int, c int, primary key(a), key idx(b))")
	testKit.MustExec("insert into t values (1,1,1),(2,2,2),(3,3,3)")

	oriLease := s.do.StatsHandle().Lease()
	s.do.StatsHandle().SetLease(1)
	defer func() {
		s.do.StatsHandle().SetLease(oriLease)
	}()
	testKit.MustExec("analyze table t")

	is := s.do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo := tbl.Meta()
	h := s.do.StatsHandle()
	stat := h.GetTableStats(tableInfo)
	hg := stat.Columns[tableInfo.Columns[0].ID].Histogram
	topn := stat.Columns[tableInfo.Columns[0].ID].TopN
	c.Assert(hg.Len()+topn.Num(), Greater, 0)
	hg = stat.Columns[tableInfo.Columns[2].ID].Histogram
	topn = stat.Columns[tableInfo.Columns[2].ID].TopN
	c.Assert(hg.Len()+topn.Num(), Equals, 0)
	stmtCtx := &stmtctx.StatementContext{}
	neededColumns := make([]model.TableColumnID, 0, len(tableInfo.Columns))
	for _, col := range tableInfo.Columns {
		neededColumns = append(neededColumns, model.TableColumnID{TableID: tableInfo.ID, ColumnID: col.ID})
	}
	timeout := time.Nanosecond * mathutil.MaxInt
	h.SendLoadRequests(stmtCtx, neededColumns, timeout)
	rs := h.SyncWaitStatsLoad(stmtCtx)
	c.Assert(rs, Equals, true)
	stat = h.GetTableStats(tableInfo)
	hg = stat.Columns[tableInfo.Columns[2].ID].Histogram
	topn = stat.Columns[tableInfo.Columns[2].ID].TopN
	c.Assert(hg.Len()+topn.Num(), Greater, 0)
}

func (s *testLoadHistSuite) TestConcurrentLoadHistTimeout(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	testKit.MustExec("set @@session.tidb_analyze_version=2")
	testKit.MustExec("set @@session.tidb_stats_load_sync_wait =9999999")
	testKit.MustExec("create table t(a int, b int, c int, primary key(a), key idx(b))")
	testKit.MustExec("insert into t values (1,1,1),(2,2,2),(3,3,3)")

	oriLease := s.do.StatsHandle().Lease()
	s.do.StatsHandle().SetLease(1)
	defer func() {
		s.do.StatsHandle().SetLease(oriLease)
	}()
	testKit.MustExec("analyze table t")

	is := s.do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo := tbl.Meta()
	h := s.do.StatsHandle()
	stat := h.GetTableStats(tableInfo)
	hg := stat.Columns[tableInfo.Columns[0].ID].Histogram
	topn := stat.Columns[tableInfo.Columns[0].ID].TopN
	c.Assert(hg.Len()+topn.Num(), Greater, 0)
	hg = stat.Columns[tableInfo.Columns[2].ID].Histogram
	topn = stat.Columns[tableInfo.Columns[2].ID].TopN
	c.Assert(hg.Len()+topn.Num(), Equals, 0)
	stmtCtx := &stmtctx.StatementContext{}
	neededColumns := make([]model.TableColumnID, 0, len(tableInfo.Columns))
	for _, col := range tableInfo.Columns {
		neededColumns = append(neededColumns, model.TableColumnID{TableID: tableInfo.ID, ColumnID: col.ID})
	}
	h.SendLoadRequests(stmtCtx, neededColumns, 0) // set timeout to 0 so task will go to timeout channel
	rs := h.SyncWaitStatsLoad(stmtCtx)
	c.Assert(rs, Equals, false)
	stat = h.GetTableStats(tableInfo)
	hg = stat.Columns[tableInfo.Columns[2].ID].Histogram
	topn = stat.Columns[tableInfo.Columns[2].ID].TopN
	c.Assert(hg.Len()+topn.Num(), Equals, 0)
	// wait for timeout task to be handled
	for {
		time.Sleep(time.Millisecond * 100)
		if len(h.StatsLoad.TimeoutColumnsCh)+len(h.StatsLoad.NeededColumnsCh) == 0 {
			break
		}
	}
	stat = h.GetTableStats(tableInfo)
	hg = stat.Columns[tableInfo.Columns[2].ID].Histogram
	topn = stat.Columns[tableInfo.Columns[2].ID].TopN
	c.Assert(hg.Len()+topn.Num(), Greater, 0)
}
