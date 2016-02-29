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

	"github.com/ngaut/log"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/plan/plans"
	"github.com/pingcap/tidb/rset"
	"github.com/pingcap/tidb/rset/rsets"
	"github.com/pingcap/tidb/table"
)

func newSession(c *C, store kv.Storage, dbName string) tidb.Session {
	se, err := tidb.CreateSession(store)
	c.Assert(err, IsNil)
	sessionExec(c, se, "create database if not exists "+dbName)
	sessionExec(c, se, "use "+dbName)
	return se
}

func newStore(c *C) kv.Storage {
	store, err := tidb.NewStore(tidb.EngineGoLevelDBMemory)
	c.Assert(err, IsNil)
	return store
}

func sessionExec(c *C, se tidb.Session, sql string) ([]rset.Recordset, error) {
	se.Execute("BEGIN;")
	r, err := se.Execute(sql)
	c.Assert(err, IsNil)
	se.Execute("COMMIT;")
	return r, err
}

var _ = Suite(&testTableRsetSuite{})

type testTableRsetSuite struct {
	dbName         string
	tableName      string
	createTableSql string
	querySql       string
}

func (s *testTableRsetSuite) SetUpSuite(c *C) {
	log.SetLevelByString("error")
	s.dbName = "rset_test"
	s.tableName = "rset_table"

	s.createTableSql = fmt.Sprintf("create table if not exists %s(id int);", s.tableName)
	s.querySql = fmt.Sprintf("select * from %s;", s.tableName)
}

func (s *testTableRsetSuite) TestTableRsetPlan(c *C) {
	store := newStore(c)
	se := newSession(c, store, s.dbName)
	ctx, ok := se.(context.Context)
	c.Assert(ok, IsTrue)

	r := &rsets.TableRset{}
	// check exists table
	_, err := sessionExec(c, se, s.createTableSql)
	c.Assert(err, IsNil)

	r.Schema = s.dbName
	r.Name = s.tableName

	p, err := r.Plan(ctx)
	c.Assert(err, IsNil)

	tdp, ok := p.(*plans.TableDefaultPlan)
	c.Assert(ok, IsTrue)
	c.Assert(tdp.Fields, HasLen, 1)

	// check not exists table
	r.Name = s.tableName + "xxx"

	p, err = r.Plan(ctx)
	c.Assert(err, NotNil)
	c.Assert(p, IsNil)
}

func (s *testTableRsetSuite) TestTableSourceString(c *C) {
	tableIdent := table.Ident{Schema: model.NewCIStr(s.dbName), Name: model.NewCIStr(s.tableName)}
	ts := &rsets.TableSource{Source: tableIdent, Name: s.tableName}

	str := ts.String()
	c.Assert(len(str), Greater, 0)

	store := newStore(c)
	se := newSession(c, store, s.dbName)
	ctx, ok := se.(context.Context)
	c.Assert(ok, IsTrue)
	rawStmtList, err := tidb.Parse(ctx, s.querySql)
	c.Assert(err, IsNil)
	c.Assert(len(rawStmtList), Greater, 0)
	stmtList, err := tidb.Compile(ctx, rawStmtList)
	c.Assert(err, IsNil)
	c.Assert(len(stmtList), Greater, 0)

	ts = &rsets.TableSource{Source: stmtList[0], Name: s.tableName}
	str = ts.String()
	c.Assert(len(str), Greater, 0)

	ts = &rsets.TableSource{Source: stmtList[0]}
	str = ts.String()
	c.Assert(len(str), Greater, 0)

	// check panic
	defer func() {
		e := recover()
		c.Assert(e, NotNil)
	}()

	ts = &rsets.TableSource{}
	str = ts.String()
}
