// Copyright 2019 PingCAP, Inc.
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

package cascades_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util/testkit"
)

var _ = Suite(&testIntegrationSuite{})

type testIntegrationSuite struct {
	store kv.Storage
}

func newStoreWithBootstrap() (kv.Storage, error) {
	store, err := mockstore.NewMockTikvStore()
	if err != nil {
		return nil, err
	}
	_, err = session.BootstrapSession(store)
	return store, err
}

func (s *testIntegrationSuite) SetUpSuite(c *C) {
	var err error
	s.store, err = newStoreWithBootstrap()
	c.Assert(err, IsNil)
}

func (s *testIntegrationSuite) TearDownSuite(c *C) {
	s.store.Close()
}

func (s *testIntegrationSuite) TestSimpleProjDual(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("set session tidb_enable_cascades_planner = 1")
	tk.MustQuery("explain select 1").Check(testkit.Rows(
		"Projection_3 1.00 root 1",
		"└─TableDual_4 1.00 root rows:1",
	))
	tk.MustQuery("select 1").Check(testkit.Rows(
		"1",
	))
}

func (s *testIntegrationSuite) TestPKIsHandleRangeScan(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b int)")
	tk.MustExec("insert into t values(1,2),(3,4)")
	tk.MustExec("set session tidb_enable_cascades_planner = 1")
	tk.MustQuery("explain select b from t where a > 1").Check(testkit.Rows(
		"Projection_8 3333.33 root Column#2",
		"└─TableReader_9 3333.33 root data:TableScan_10",
		"  └─TableScan_10 3333.33 cop[tikv] table:t, range:(1,+inf], keep order:false, stats:pseudo",
	))
	tk.MustQuery("select b from t where a > 1").Check(testkit.Rows(
		"4",
	))
	tk.MustQuery("explain select b from t where a > 1 and a < 3").Check(testkit.Rows(
		"Projection_8 2.00 root Column#2",
		"└─TableReader_9 2.00 root data:TableScan_10",
		"  └─TableScan_10 2.00 cop[tikv] table:t, range:(1,3), keep order:false, stats:pseudo",
	))
	tk.MustQuery("select b from t where a > 1 and a < 3").Check(testkit.Rows())
}

func (s *testIntegrationSuite) TestBasicShow(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b int)")
	tk.MustExec("set session tidb_enable_cascades_planner = 1")
	tk.MustQuery("desc t").Check(testkit.Rows(
		"a int(11) NO PRI <nil> ",
		"b int(11) YES  <nil> ",
	))
}
