// Copyright 2017 PingCAP, Inc.
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
	"testing"

	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/types"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testStatsCacheSuite{})

type testStatsCacheSuite struct{}

func (s *testStatsCacheSuite) TestStatsCache(c *C) {
	store, do, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer store.Close()
	testKit := testkit.NewTestKit(c, store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (c1 int, c2 int)")
	testKit.MustExec("insert into t values(1, 2)")
	is := do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo := tbl.Meta()
	statsTbl := statistics.GetStatisticsTableCache(tableInfo)
	c.Assert(statsTbl.Pseudo, IsTrue)
	testKit.MustExec("analyze table t")
	statsTbl = statistics.GetStatisticsTableCache(tableInfo)
	c.Assert(statsTbl.Pseudo, IsFalse)
	testKit.MustExec("create index idx_t on t(c1)")
	is = do.InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo = tbl.Meta()
	statsTbl = statistics.GetStatisticsTableCache(tableInfo)
	c.Assert(statsTbl.Pseudo, IsTrue)
	testKit.MustExec("analyze table t")
	statsTbl = statistics.GetStatisticsTableCache(tableInfo)
	c.Assert(statsTbl.Pseudo, IsFalse)
	// If the new schema drop a column, the table stats can still work.
	testKit.MustExec("alter table t drop column c2")
	is = do.InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo = tbl.Meta()

	do.StatsHandle().Clear()
	do.StatsHandle().Update(is)
	statsTbl = statistics.GetStatisticsTableCache(tableInfo)
	c.Assert(statsTbl.Pseudo, IsFalse)

	// If the new schema add a column, the table stats cannot work.
	testKit.MustExec("alter table t add column c10 int")
	is = do.InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo = tbl.Meta()

	do.StatsHandle().Clear()
	do.StatsHandle().Update(is)
	statsTbl = statistics.GetStatisticsTableCache(tableInfo)
	c.Assert(statsTbl.Pseudo, IsTrue)
}

func compareTwoColumnsStatsSlice(cols0 []*statistics.Column, cols1 []*statistics.Column, c *C) {
	c.Assert(len(cols0), Equals, len(cols1))
	for _, col0 := range cols0 {
		find := false
		for _, col1 := range cols1 {
			if col0.ID == col1.ID {
				c.Assert(col0.NDV, Equals, col1.NDV)
				c.Assert(len(col0.Numbers), Equals, len(col1.Numbers))
				for j := 0; j < len(col0.Numbers); j++ {
					c.Assert(col0.Numbers[j], Equals, col1.Numbers[j])
					c.Assert(col0.Repeats[j], Equals, col1.Repeats[j])
					c.Assert(col0.Values[j], DeepEquals, col1.Values[j])
				}
				find = true
				break
			}
		}
		c.Assert(find, IsTrue)
	}
}

func (s *testStatsCacheSuite) TestStatsStoreAndLoad(c *C) {
	store, do, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer store.Close()
	testKit := testkit.NewTestKit(c, store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (c1 int, c2 int)")
	recordCount := 1000
	for i := 0; i < recordCount; i++ {
		testKit.MustExec("insert into t values (?, ?)", i, i+1)
	}
	testKit.MustExec("create index idx_t on t(c2)")
	is := do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo := tbl.Meta()

	testKit.MustExec("analyze table t")
	statsTbl1 := statistics.GetStatisticsTableCache(tableInfo)

	do.StatsHandle().Clear()
	do.StatsHandle().Update(is)
	statsTbl2 := statistics.GetStatisticsTableCache(tableInfo)
	c.Assert(statsTbl2.Pseudo, IsFalse)
	c.Assert(statsTbl2.Count, Equals, int64(recordCount))

	compareTwoColumnsStatsSlice(statsTbl1.Columns, statsTbl2.Columns, c)
	compareTwoColumnsStatsSlice(statsTbl1.Indices, statsTbl2.Indices, c)
}

func (s *testStatsCacheSuite) TestDDLAfterLoad(c *C) {
	store, do, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer store.Close()
	testKit := testkit.NewTestKit(c, store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (c1 int, c2 int)")
	testKit.MustExec("analyze table t")
	is := do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo := tbl.Meta()
	statsTbl := statistics.GetStatisticsTableCache(tableInfo)
	c.Assert(statsTbl.Pseudo, IsFalse)
	recordCount := 1000
	for i := 0; i < recordCount; i++ {
		testKit.MustExec("insert into t values (?, ?)", i, i+1)
	}
	testKit.MustExec("analyze table t")
	statsTbl = statistics.GetStatisticsTableCache(tableInfo)
	c.Assert(statsTbl.Pseudo, IsFalse)
	// add column
	testKit.MustExec("alter table t add column c10 int")
	is = do.InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo = tbl.Meta()

	sc := new(variable.StatementContext)
	count, err := statsTbl.ColumnGreaterRowCount(sc, types.NewDatum(recordCount+1), tableInfo.Columns[0])
	c.Assert(err, IsNil)
	c.Assert(count, Equals, int64(0))
	count, err = statsTbl.ColumnGreaterRowCount(sc, types.NewDatum(recordCount+1), tableInfo.Columns[2])
	c.Assert(err, IsNil)
	c.Assert(count, Equals, int64(333))
}

func (s *testStatsCacheSuite) TestEmptyTable(c *C) {
	store, do, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer store.Close()
	testKit := testkit.NewTestKit(c, store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (c1 int, c2 int, key cc1(c1), key cc2(c2))")
	testKit.MustExec("analyze table t")
	is := do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo := tbl.Meta()
	statsTbl := statistics.GetStatisticsTableCache(tableInfo)
	sc := new(variable.StatementContext)
	count, err := statsTbl.ColumnGreaterRowCount(sc, types.NewDatum(1), tableInfo.Columns[0])
	c.Assert(err, IsNil)
	// FIXME: The result should be zero.
	c.Assert(count, Equals, int64(3333333))
}

func newStoreWithBootstrap() (kv.Storage, *domain.Domain, error) {
	store, err := tidb.NewStore(tidb.EngineGoLevelDBMemory)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	do, err := tidb.BootstrapSession(store)
	return store, do, errors.Trace(err)
}
