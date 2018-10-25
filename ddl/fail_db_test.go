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

package ddl_test

import (
	"time"

	gofail "github.com/etcd-io/gofail/runtime"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util/testleak"
	"golang.org/x/net/context"
)

var _ = Suite(&testFailDBSuite{})

type testFailDBSuite struct {
	lease time.Duration
	store kv.Storage
	dom   *domain.Domain
	se    session.Session
	p     *parser.Parser
}

func (s *testFailDBSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	s.lease = 200 * time.Millisecond
	var err error
	s.store, err = mockstore.NewMockTikvStore()
	c.Assert(err, IsNil)
	session.SetSchemaLease(s.lease)
	s.dom, err = session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
	s.se, err = session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)
	s.p = parser.New()
}

func (s *testFailDBSuite) TearDownSuite(c *C) {
	s.se.Execute(context.Background(), "drop database if exists test_db_state")
	s.se.Close()
	s.dom.Close()
	s.store.Close()
	testleak.AfterTest(c)()
}

// TestHalfwayCancelOperations tests the case that the schema is correct after the execution of operations are cancelled halfway.
func (s *testFailDBSuite) TestHalfwayCancelOperations(c *C) {
	gofail.Enable("github.com/pingcap/tidb/ddl/truncateTableErr", `return(true)`)
	defer gofail.Disable("github.com/pingcap/tidb/ddl/truncateTableErr")

	// test for truncating table
	_, err := s.se.Execute(context.Background(), "create database cancel_job_db")
	c.Assert(err, IsNil)
	_, err = s.se.Execute(context.Background(), "use cancel_job_db")
	c.Assert(err, IsNil)
	_, err = s.se.Execute(context.Background(), "create table t(a int)")
	c.Assert(err, IsNil)
	_, err = s.se.Execute(context.Background(), "insert into t values(1)")
	c.Assert(err, IsNil)
	_, err = s.se.Execute(context.Background(), "truncate table t")
	c.Assert(err, NotNil)
	// Make sure that the table's data has not been deleted.
	rs, err := s.se.Execute(context.Background(), "select count(*) from t")
	c.Assert(err, IsNil)
	chk := rs[0].NewChunk()
	err = rs[0].Next(context.Background(), chk)
	c.Assert(err, IsNil)
	c.Assert(chk.NumRows() == 0, IsFalse)
	row := chk.GetRow(0)
	c.Assert(row.Len(), Equals, 1)
	c.Assert(row.GetInt64(0), DeepEquals, int64(1))
	c.Assert(rs[0].Close(), IsNil)
	// Reload schema.
	s.dom.ResetHandle(s.store)
	err = s.dom.DDL().GetHook().OnChanged(nil)
	c.Assert(err, IsNil)
	s.se, err = session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)
	_, err = s.se.Execute(context.Background(), "use cancel_job_db")
	c.Assert(err, IsNil)
	// Test schema is correct.
	_, err = s.se.Execute(context.Background(), "select * from t")
	c.Assert(err, IsNil)

	// test for renaming table
	gofail.Enable("github.com/pingcap/tidb/ddl/errRenameTable", `return(true)`)
	defer gofail.Disable("github.com/pingcap/tidb/ddl/errRenameTable")
	_, err = s.se.Execute(context.Background(), "create table tx(a int)")
	c.Assert(err, IsNil)
	_, err = s.se.Execute(context.Background(), "insert into tx values(1)")
	c.Assert(err, IsNil)
	_, err = s.se.Execute(context.Background(), "rename table tx to ty")
	c.Assert(err, NotNil)
	// Make sure that the table's data has not been deleted.
	rs, err = s.se.Execute(context.Background(), "select count(*) from tx")
	c.Assert(err, IsNil)
	chk = rs[0].NewChunk()
	err = rs[0].Next(context.Background(), chk)
	c.Assert(err, IsNil)
	c.Assert(chk.NumRows() == 0, IsFalse)
	row = chk.GetRow(0)
	c.Assert(row.Len(), Equals, 1)
	c.Assert(row.GetInt64(0), DeepEquals, int64(1))
	c.Assert(rs[0].Close(), IsNil)
	// Reload schema.
	s.dom.ResetHandle(s.store)
	err = s.dom.DDL().GetHook().OnChanged(nil)
	c.Assert(err, IsNil)
	s.se, err = session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)
	_, err = s.se.Execute(context.Background(), "use cancel_job_db")
	c.Assert(err, IsNil)
	// Test schema is correct.
	_, err = s.se.Execute(context.Background(), "select * from tx")
	c.Assert(err, IsNil)

	// clean up
	_, err = s.se.Execute(context.Background(), "drop database cancel_job_db")
	c.Assert(err, IsNil)
}
