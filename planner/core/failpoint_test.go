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

package core_test

import (
	. "github.com/pingcap/check"
	gofail "github.com/pingcap/gofail/runtime"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testFailPointSuit{})

type testFailPointSuit struct {
	store kv.Storage
	dom   *domain.Domain
	ctx   sessionctx.Context
}

func (s *testFailPointSuit) SetUpSuite(c *C) {
	testleak.BeforeTest()

	var err error
	s.store, s.dom, err = newStoreWithBootstrap()
	c.Assert(err, IsNil)
	s.ctx = mock.NewContext()
}

func (s *testFailPointSuit) TearDownSuite(c *C) {
	s.dom.Close()
	s.store.Close()
	testleak.AfterTest(c)()
}

func (s *testFailPointSuit) TestColumnPruningError(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec(`use test;`)
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a int, b int);`)
	tk.MustExec(`insert into t values(1,1);`)

	// test normal behavior
	tk.MustQuery(`select a from t;`).Check(testkit.Rows(`1`))

	// test the injected fail point
	gofail.Enable("github.com/pingcap/tidb/planner/core/enableGetUsedListErr", `return(true)`)
	defer gofail.Disable("github.com/pingcap/tidb/executor/enableGetUsedListErr")
	err := tk.ExecToErr(`select a from t;`)
	c.Assert(err.Error(), Equals, "getUsedList failed, triggered by gofail enableGetUsedListErr")
}
