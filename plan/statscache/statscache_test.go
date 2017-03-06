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

package statscache_test

import (
	"testing"

	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/plan/statscache"
	"github.com/pingcap/tidb/util/testkit"
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
	statsTbl := statscache.GetStatisticsTableCache(tableInfo)
	c.Assert(statsTbl.Pseudo, IsTrue)
	testKit.MustExec("analyze table t")
	statsTbl = statscache.GetStatisticsTableCache(tableInfo)
	c.Assert(statsTbl.Pseudo, IsFalse)
	testKit.MustExec("create index idx_t on t(c1)")
	is = do.InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo = tbl.Meta()
	statsTbl = statscache.GetStatisticsTableCache(tableInfo)
	c.Assert(statsTbl.Pseudo, IsTrue)
	testKit.MustExec("analyze table t")
	statsTbl = statscache.GetStatisticsTableCache(tableInfo)
	c.Assert(statsTbl.Pseudo, IsFalse)
}

func newStoreWithBootstrap() (kv.Storage, *domain.Domain, error) {
	store, err := tidb.NewStore(tidb.EngineGoLevelDBMemory)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	do, err := tidb.BootstrapSession(store)
	return store, do, errors.Trace(err)
}
