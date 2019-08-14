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
	tk    *testkit.TestKit
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
	s.tk = testkit.NewTestKitWithInit(c, s.store)
	s.tk.MustExec("set session tidb_enable_cascades_planner = 1")
}

func (s *testIntegrationSuite) TearDownSuite(c *C) {
	s.store.Close()
}

func (s *testIntegrationSuite) TestSimpleProjDual(c *C) {
	s.tk.MustQuery("explain select 1").Check(testkit.Rows(
		"Projection_3 1.00 root 1",
		"└─TableDual_4 1.00 root rows:1",
	))
	s.tk.MustQuery("select 1").Check(testkit.Rows(
		"1",
	))
}
