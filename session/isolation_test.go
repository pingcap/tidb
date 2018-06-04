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

package session_test

import (
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testIsolationSuite{})

type testIsolationSuite struct {
	cluster   *mocktikv.Cluster
	mvccStore mocktikv.MVCCStore
	store     kv.Storage
	dom       *domain.Domain
}

func (s *testIsolationSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	s.cluster = mocktikv.NewCluster()
	mocktikv.BootstrapWithSingleStore(s.cluster)
	s.mvccStore = mocktikv.MustNewMVCCStore()
	store, err := mockstore.NewMockTikvStore(
		mockstore.WithCluster(s.cluster),
		mockstore.WithMVCCStore(s.mvccStore),
	)
	c.Assert(err, IsNil)
	s.store = store
	session.SetSchemaLease(0)
	session.SetStatsLease(0)
	s.dom, err = session.BootstrapSession(s.store)
	c.Assert(err, IsNil)

	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("set @@global.tidb_retry_limit = 0")
	time.Sleep(3 * time.Second)
}

func (s *testIsolationSuite) TearDownSuite(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("set @@global.tidb_retry_limit = 10")

	s.dom.Close()
	s.store.Close()
	testleak.AfterTest(c)()
}

/*
These test cases come from the paper <A Critique of ANSI SQL Isolation Levels>.
The sign 'P0', 'P1'.... can be found in the paper. These cases will run under snapshot isolation.
*/
func (s *testIsolationSuite) TestP0DirtyWrite(c *C) {
	session1 := testkit.NewTestKitWithInit(c, s.store)
	session2 := testkit.NewTestKitWithInit(c, s.store)

	session1.MustExec("drop table if exists x;")
	session1.MustExec("create table x (id int primary key, c int);")
	session1.MustExec("insert into x values(1, 1);")

	session1.MustExec("begin;")
	session1.MustExec("update x set c = c+1 where id = 1;")
	session2.MustExec("begin;")
	session2.MustExec("update x set c = c+1 where id = 1;")
	session1.MustExec("commit;")
	_, err := session2.Exec("commit;")
	c.Assert(err, NotNil)
}

func (s *testIsolationSuite) TestP1DirtyRead(c *C) {
	session1 := testkit.NewTestKitWithInit(c, s.store)
	session2 := testkit.NewTestKitWithInit(c, s.store)

	session1.MustExec("drop table if exists x;")
	session1.MustExec("create table x (id int primary key, c int);")
	session1.MustExec("insert into x values(1, 1);")

	session1.MustExec("begin;")
	session1.MustExec("update x set c = c+1 where id = 1;")
	session2.MustExec("begin;")
	session2.MustQuery("select c from x where id = 1;").Check(testkit.Rows("1"))
	session1.MustExec("commit;")
	session2.MustExec("commit;")
}

func (s *testIsolationSuite) TestP2NonRepeatableRead(c *C) {
	session1 := testkit.NewTestKitWithInit(c, s.store)
	session2 := testkit.NewTestKitWithInit(c, s.store)

	session1.MustExec("drop table if exists x;")
	session1.MustExec("drop table if exists y;")
	session1.MustExec("create table x (id int primary key, c int);")
	session1.MustExec("insert into x values(1, 1);")
	session1.MustExec("create table y (id int primary key, c int);")
	session1.MustExec("insert into y values(1, 1);")

	session1.MustExec("begin;")
	session2.MustExec("begin;")
	session1.MustQuery("select c from x where id = 1;").Check(testkit.Rows("1"))
	session2.MustQuery("select c from x where id = 1;").Check(testkit.Rows("1"))
	session2.MustExec("update x set c = c+1 where id = 1;")
	session2.MustQuery("select c from y where id = 1;").Check(testkit.Rows("1"))
	session2.MustExec("update y set c = c+1 where id = 1;")
	session2.MustExec("commit;")
	session1.MustQuery("select c from y where id = 1;").Check(testkit.Rows("1"))
	session1.MustExec("commit;")
}

func (s *testIsolationSuite) TestP3Phantom(c *C) {
	session1 := testkit.NewTestKitWithInit(c, s.store)
	session2 := testkit.NewTestKitWithInit(c, s.store)

	session1.MustExec("drop table if exists x;")
	session1.MustExec("drop table if exists z;")
	session1.MustExec("create table x (id int primary key, c int);")
	session1.MustExec("insert into x values(1, 1);")
	session1.MustExec("create table z (id int primary key, c int);")
	session1.MustExec("insert into z values(1, 1);")

	session1.MustExec("begin;")
	session2.MustExec("begin;")
	session1.MustQuery("select c from x where id < 5;").Check(testkit.Rows("1"))
	session2.MustExec("insert into x values(2, 1);")
	session2.MustQuery("select c from z where id = 1;").Check(testkit.Rows("1"))
	session2.MustExec("update z set c = c+1 where id = 1;")
	session2.MustExec("commit;")
	session1.MustQuery("select c from z where id = 1;").Check(testkit.Rows("1"))
	session1.MustExec("commit;")
}

func (s *testIsolationSuite) TestP4LostUpdate(c *C) {
	session1 := testkit.NewTestKitWithInit(c, s.store)
	session2 := testkit.NewTestKitWithInit(c, s.store)

	session1.MustExec("drop table if exists x;")
	session1.MustExec("create table x (id int primary key, c int);")
	session1.MustExec("insert into x values(1, 1);")

	session1.MustExec("begin;")
	session1.MustQuery("select c from x where id = 1;").Check(testkit.Rows("1"))
	session2.MustExec("begin;")
	session2.MustQuery("select c from x where id = 1;").Check(testkit.Rows("1"))
	session2.MustExec("update x set c = c+1 where id = 1;")
	session2.MustExec("commit;")
	session1.MustExec("update x set c = c+1 where id = 1;")
	_, err := session1.Exec("commit;")
	c.Assert(err, NotNil)
}

// cursor is not supported
func (s *testIsolationSuite) TestP4CLostUpdate(c *C) {}

func (s *testIsolationSuite) TestA3Phantom(c *C) {
	session1 := testkit.NewTestKitWithInit(c, s.store)
	session2 := testkit.NewTestKitWithInit(c, s.store)

	session1.MustExec("drop table if exists x;")
	session1.MustExec("create table x (id int primary key, c int);")
	session1.MustExec("insert into x values(1, 1);")

	session1.MustExec("begin;")
	session2.MustExec("begin;")
	session2.MustQuery("select c from x where id < 5;").Check(testkit.Rows("1"))
	session1.MustExec("insert into x values(2, 1);")
	session1.MustExec("commit;")
	session2.MustQuery("select c from x where id < 5;").Check(testkit.Rows("1"))
	session2.MustExec("commit;")
}

func (s *testIsolationSuite) TestA5AReadSkew(c *C) {
	session1 := testkit.NewTestKitWithInit(c, s.store)
	session2 := testkit.NewTestKitWithInit(c, s.store)

	session1.MustExec("drop table if exists x;")
	session1.MustExec("drop table if exists y;")
	session1.MustExec("create table x (id int primary key, c int);")
	session1.MustExec("insert into x values(1, 1);")
	session1.MustExec("create table y (id int primary key, c int);")
	session1.MustExec("insert into y values(1, 1);")

	session1.MustExec("begin;")
	session2.MustExec("begin;")
	session1.MustQuery("select c from x where id = 1;").Check(testkit.Rows("1"))
	session2.MustExec("update x set c = c+1 where id = 1;")
	session2.MustExec("update y set c = c+1 where id = 1;")
	session2.MustExec("commit;")
	session1.MustQuery("select c from y where id = 1;").Check(testkit.Rows("1"))
	session1.MustExec("commit;")
}

func (s *testIsolationSuite) TestA5BWriteSkew(c *C) {
	session1 := testkit.NewTestKitWithInit(c, s.store)
	session2 := testkit.NewTestKitWithInit(c, s.store)

	session1.MustExec("drop table if exists x;")
	session1.MustExec("drop table if exists y;")
	session1.MustExec("create table x (id int primary key, c int);")
	session1.MustExec("insert into x values(1, 1);")
	session1.MustExec("create table y (id int primary key, c int);")
	session1.MustExec("insert into y values(1, 1);")

	session1.MustExec("begin;")
	session2.MustExec("begin;")
	session1.MustQuery("select c from x where id = 1;").Check(testkit.Rows("1"))
	session2.MustQuery("select c from y where id = 1;").Check(testkit.Rows("1"))
	session1.MustExec("update y set c = c+1 where id = 1;")
	session2.MustExec("update x set c = c+1 where id = 1;")
	session2.MustExec("commit;")
	session1.MustExec("commit;")
}

/*
These test cases come from the paper <Highly Available Transactions: Virtues and Limitations>
for tidb, we support read-after-write on cluster level.
*/
func (s *testIsolationSuite) TestReadAfterWrite(c *C) {
	session1 := testkit.NewTestKitWithInit(c, s.store)
	session2 := testkit.NewTestKitWithInit(c, s.store)

	session1.MustExec("drop table if exists x;")
	session1.MustExec("create table x (id int primary key, c int);")
	session1.MustExec("insert into x values(1, 1);")

	session1.MustExec("begin;")
	session1.MustExec("update x set c = c+1 where id = 1;")
	session1.MustExec("commit;")
	session1.MustExec("begin;")
	session2.MustQuery("select c from x where id = 1;").Check(testkit.Rows("2"))
	session2.MustExec("commit;")
}

/*
This case will do harm in Innodb, even if in snapshot isolation, but harmless in tidb.
*/
func (s *testIsolationSuite) TestPhantomReadInInnodb(c *C) {
	session1 := testkit.NewTestKitWithInit(c, s.store)
	session2 := testkit.NewTestKitWithInit(c, s.store)

	session1.MustExec("drop table if exists x;")
	session1.MustExec("create table x (id int primary key, c int);")
	session1.MustExec("insert into x values(1, 1);")

	session1.MustExec("begin;")
	session1.MustQuery("select c from x where id < 5;").Check(testkit.Rows("1"))
	session2.MustExec("begin;")
	session2.MustExec("insert into x values(2, 1);")
	session2.MustExec("commit;")
	session1.MustExec("update x set c = c+1 where id < 5;")
	session1.MustQuery("select c from x where id < 5;").Check(testkit.Rows("2"))
	session1.MustExec("commit;")
}
