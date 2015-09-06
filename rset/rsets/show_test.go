// Copyright 2015 PingCAP, Inc.
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

package rsets_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/plan/plans"
	"github.com/pingcap/tidb/rset/rsets"
)

var _ = Suite(&testShowRsetSuite{})

type testShowRsetSuite struct {
	r   *rsets.ShowRset
	ctx context.Context
}

func (s *testShowRsetSuite) SetUpSuite(c *C) {
	dbName := "rset_test"
	s.r = &rsets.ShowRset{DBName: dbName}

	store := newStore(c)
	se := newSession(c, store, dbName)

	ctx, ok := se.(context.Context)
	c.Assert(ok, IsTrue)

	s.ctx = ctx
}

func (s *testShowRsetSuite) TestShowRsetPlan(c *C) {
	p, err := s.r.Plan(nil)
	c.Assert(err, IsNil)

	_, ok := p.(*plans.ShowPlan)
	c.Assert(ok, IsTrue)

	// check current db
	s.r.DBName = ""

	_, err = s.r.Plan(s.ctx)
	c.Assert(err, IsNil)
}
