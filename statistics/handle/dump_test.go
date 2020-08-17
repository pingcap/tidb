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

package handle_test

import (
	"fmt"
	"sync"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testStatsSuite) TestConversion(c *C) {
	defer cleanEnv(c, s.store, s.do)
	clearRW.RLock()
	defer func() {
		clearRW.RUnlock()
	}()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("create index c on t(a,b)")
	tk.MustExec("insert into t(a,b) values (3, 1),(2, 1),(1, 10)")
	tk.MustExec("analyze table t")
	tk.MustExec("insert into t(a,b) values (1, 1),(3, 1),(5, 10)")
	is := s.do.InfoSchema()
	h := s.do.StatsHandle()
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(is), IsNil)

	tableInfo, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	jsonTbl, err := h.DumpStatsToJSON("test", tableInfo.Meta(), nil)
	c.Assert(err, IsNil)
	loadTbl, err := handle.TableStatsFromJSON(tableInfo.Meta(), tableInfo.Meta().ID, jsonTbl)
	c.Assert(err, IsNil)

	tbl := h.GetTableStats(tableInfo.Meta())
	assertTableEqual(c, loadTbl, tbl)

	clearRW.RUnlock()
	cleanEnv(c, s.store, s.do)
	clearRW.RLock()

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		c.Assert(h.Update(is), IsNil)
		wg.Done()
	}()
	err = h.LoadStatsFromJSON(is, jsonTbl)
	wg.Wait()
	time.Sleep(10 * time.Millisecond)
	c.Assert(err, IsNil)
	loadTblInStorage := h.GetTableStats(tableInfo.Meta())
	assertTableEqual(c, loadTblInStorage, tbl)
}

func (s *testStatsSuite) TestDumpPartitions(c *C) {
	defer cleanEnv(c, s.store, s.do)
	clearRW.RLock()
	defer clearRW.RUnlock()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	createTable := `CREATE TABLE t (a int, b int, primary key(a), index idx(b))
PARTITION BY RANGE ( a ) (
		PARTITION p0 VALUES LESS THAN (6),
		PARTITION p1 VALUES LESS THAN (11),
		PARTITION p2 VALUES LESS THAN (16),
		PARTITION p3 VALUES LESS THAN (21)
)`
	tk.MustExec(createTable)
	for i := 1; i < 21; i++ {
		tk.MustExec(fmt.Sprintf(`insert into t values (%d, %d)`, i, i))
	}
	tk.MustExec("analyze table t")
	is := s.do.InfoSchema()
	h := s.do.StatsHandle()
	c.Assert(h.Update(is), IsNil)

	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo := table.Meta()
	jsonTbl, err := h.DumpStatsToJSON("test", tableInfo, nil)
	c.Assert(err, IsNil)
	pi := tableInfo.GetPartitionInfo()
	originTables := make([]*statistics.Table, 0, len(pi.Definitions))
	for _, def := range pi.Definitions {
		originTables = append(originTables, h.GetPartitionStats(tableInfo, def.ID))
	}

	tk.MustExec("delete from mysql.stats_meta")
	tk.MustExec("delete from mysql.stats_histograms")
	tk.MustExec("delete from mysql.stats_buckets")
	clearRW.RUnlock()
	cleanHandle(c, s.do)
	clearRW.RLock()

	err = h.LoadStatsFromJSON(s.do.InfoSchema(), jsonTbl)
	time.Sleep(10 * time.Millisecond)

	c.Assert(err, IsNil)
	for i, def := range pi.Definitions {
		t := h.GetPartitionStats(tableInfo, def.ID)
		assertTableEqual(c, originTables[i], t)
	}
}

func (s *testStatsSuite) TestDumpAlteredTable(c *C) {
	defer cleanEnv(c, s.store, s.do)
	clearRW.RLock()
	defer clearRW.RUnlock()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	h := s.do.StatsHandle()
	oriLease := h.Lease()
	h.SetLease(1)
	defer func() { h.SetLease(oriLease) }()
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("analyze table t")
	tk.MustExec("alter table t drop column a")
	table, err := s.do.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	_, err = h.DumpStatsToJSON("test", table.Meta(), nil)
	c.Assert(err, IsNil)
}

func (s *testStatsSuite) TestDumpCMSketchWithTopN(c *C) {
	defer cleanEnv(c, s.store, s.do)
	clearRW.RLock()
	defer clearRW.RUnlock()
	// Just test if we can store and recover the Top N elements stored in database.
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t(a int)")
	testKit.MustExec("insert into t values (1),(3),(4),(2),(5)")
	testKit.MustExec("analyze table t")

	is := s.do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo := tbl.Meta()
	h := s.do.StatsHandle()
	c.Assert(h.Update(is), IsNil)

	// Insert 30 fake data
	fakeData := make([][]byte, 0, 30)
	for i := 0; i < 30; i++ {
		fakeData = append(fakeData, []byte(fmt.Sprintf("%01024d", i)))
	}
	cms, _, _ := statistics.NewCMSketchWithTopN(5, 2048, fakeData, 20, 100)

	stat := h.GetTableStats(tableInfo)
	err = h.SaveStatsToStorage(tableInfo.ID, 1, 0, &stat.Columns[tableInfo.Columns[0].ID].Histogram, cms, 1)
	c.Assert(err, IsNil)
	c.Assert(h.Update(is), IsNil)

	stat = h.GetTableStats(tableInfo)
	cmsFromStore := stat.Columns[tableInfo.Columns[0].ID].CMSketch
	c.Assert(cmsFromStore, NotNil)
	c.Check(cms.Equal(cmsFromStore), IsTrue)

	jsonTable, err := h.DumpStatsToJSON("test", tableInfo, nil)
	c.Check(err, IsNil)
	err = h.LoadStatsFromJSON(is, jsonTable)
	c.Check(err, IsNil)
	stat = h.GetTableStats(tableInfo)
	cmsFromJSON := stat.Columns[tableInfo.Columns[0].ID].CMSketch.Copy()
	c.Check(cms.Equal(cmsFromJSON), IsTrue)
}

func (s *testStatsSuite) TestDumpPseudoColumns(c *C) {
	defer cleanEnv(c, s.store, s.do)
	clearRW.RLock()
	defer clearRW.RUnlock()
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t(a int, b int, index idx(a))")
	// Force adding an pseudo tables in stats cache.
	testKit.MustQuery("select * from t")
	testKit.MustExec("analyze table t index idx")

	is := s.do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	h := s.do.StatsHandle()
	_, err = h.DumpStatsToJSON("test", tbl.Meta(), nil)
	c.Assert(err, IsNil)
}
