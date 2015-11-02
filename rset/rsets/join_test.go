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
	"fmt"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/plan/plans"
	"github.com/pingcap/tidb/rset/rsets"
	"github.com/pingcap/tidb/table"
)

var _ = Suite(&testJoinRsetSuite{})

type testJoinRsetSuite struct {
	r   *rsets.JoinRset
	ctx context.Context
}

func (s *testJoinRsetSuite) SetUpSuite(c *C) {
	dbName := "rset_test"
	tableName := "rset_table"

	tableIdent := table.Ident{Schema: model.NewCIStr(dbName), Name: model.NewCIStr(tableName)}
	ts := &rsets.TableSource{Source: tableIdent}

	s.r = &rsets.JoinRset{Left: ts, Type: rsets.CrossJoin}

	store := newStore(c)
	se := newSession(c, store, dbName)

	ctx, ok := se.(context.Context)
	c.Assert(ok, IsTrue)

	s.ctx = ctx
}

func (s *testJoinRsetSuite) TestJoinTypeString(c *C) {
	joinType := rsets.CrossJoin
	str := joinType.String()
	c.Assert(str, Equals, plans.CrossJoin)

	joinType = rsets.LeftJoin
	str = joinType.String()
	c.Assert(str, Equals, plans.LeftJoin)

	joinType = rsets.RightJoin
	str = joinType.String()
	c.Assert(str, Equals, plans.RightJoin)

	joinType = rsets.JoinType(0)
	str = joinType.String()
	c.Assert(str, Equals, "Unknown")
}

func (s *testJoinRsetSuite) TestJoinRsetPlan(c *C) {
	p, err := s.r.Plan(s.ctx)
	c.Assert(err, IsNil)

	_, ok := p.(*plans.JoinPlan)
	c.Assert(ok, IsTrue)

	// check join right is statement.
	querySql := fmt.Sprintf("select 1")
	stmtList, err := tidb.Compile(s.ctx, querySql)
	c.Assert(err, IsNil)
	c.Assert(len(stmtList), Greater, 0)

	ts := &rsets.TableSource{Source: stmtList[0]}
	s.r.Right = ts

	_, err = s.r.Plan(s.ctx)
	c.Assert(err, IsNil)

	// check join right is join rset.
	s.r.Right = &rsets.JoinRset{Left: ts}

	_, err = s.r.Plan(s.ctx)
	c.Assert(err, IsNil)

	// check error.
	s.r.Right = "xxx"
	_, err = s.r.Plan(s.ctx)
	c.Assert(err, NotNil)

	s.r.Right = nil
}

func (s *testJoinRsetSuite) TestJoinRsetString(c *C) {
	s.r.Right = s.r.Left
	s.r.On = expression.Value{Val: 1}
	str := s.r.String()
	c.Assert(len(str), Greater, 0)
}
