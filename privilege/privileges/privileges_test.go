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

package privileges_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util/testleak"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testPrivilegeSuite{})

type testPrivilegeSuite struct {
	store  kv.Storage
	dom    *domain.Domain
	dbName string

	createDBSQL              string
	createDB1SQL             string
	dropDBSQL                string
	useDBSQL                 string
	createTableSQL           string
	createSystemDBSQL        string
	createUserTableSQL       string
	createDBPrivTableSQL     string
	createTablePrivTableSQL  string
	createColumnPrivTableSQL string
}

func (s *testPrivilegeSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	s.dbName = "test"
	s.dom, s.store = newStore(c, s.dbName)
}

func (s *testPrivilegeSuite) TearDownSuite(c *C) {
	s.dom.Close()
	s.store.Close()
	testleak.AfterTest(c)()
}

func (s *testPrivilegeSuite) SetUpTest(c *C) {
	se := newSession(c, s.store, s.dbName)
	s.createDBSQL = fmt.Sprintf("create database if not exists %s;", s.dbName)
	s.createDB1SQL = fmt.Sprintf("create database if not exists %s1;", s.dbName)
	s.dropDBSQL = fmt.Sprintf("drop database if exists %s;", s.dbName)
	s.useDBSQL = fmt.Sprintf("use %s;", s.dbName)
	s.createTableSQL = `CREATE TABLE test(id INT NOT NULL DEFAULT 1, name varchar(255), PRIMARY KEY(id));`

	mustExec(c, se, s.createDBSQL)
	mustExec(c, se, s.createDB1SQL) // create database test1
	mustExec(c, se, s.useDBSQL)
	mustExec(c, se, s.createTableSQL)

	s.createSystemDBSQL = fmt.Sprintf("create database if not exists %s;", mysql.SystemDB)
	s.createUserTableSQL = session.CreateUserTable
	s.createDBPrivTableSQL = session.CreateDBPrivTable
	s.createTablePrivTableSQL = session.CreateTablePrivTable
	s.createColumnPrivTableSQL = session.CreateColumnPrivTable

	mustExec(c, se, s.createSystemDBSQL)
	mustExec(c, se, s.createUserTableSQL)
	mustExec(c, se, s.createDBPrivTableSQL)
	mustExec(c, se, s.createTablePrivTableSQL)
	mustExec(c, se, s.createColumnPrivTableSQL)
}

func (s *testPrivilegeSuite) TearDownTest(c *C) {
	// drop db
	se := newSession(c, s.store, s.dbName)
	mustExec(c, se, s.dropDBSQL)
}

func mustExec(c *C, se session.Session, sql string) {
	_, err := se.ExecuteInternal(context.Background(), sql)
	c.Assert(err, IsNil)
}

func newStore(c *C, dbPath string) (*domain.Domain, kv.Storage) {
	store, err := mockstore.NewMockStore()
	session.SetSchemaLease(0)
	session.DisableStats4Test()
	c.Assert(err, IsNil)
	dom, err := session.BootstrapSession(store)
	c.Assert(err, IsNil)
	return dom, store
}

func newSession(c *C, store kv.Storage, dbName string) session.Session {
	se, err := session.CreateSession4Test(store)
	c.Assert(err, IsNil)
	mustExec(c, se, "create database if not exists "+dbName)
	mustExec(c, se, "use "+dbName)
	return se
}

func (s *testPrivilegeSuite) TestRenameUser(c *C) {
	rootSe := newSession(c, s.store, s.dbName)
	mustExec(c, rootSe, "CREATE USER 'ru1'@'localhost'")
	mustExec(c, rootSe, "GRANT SELECT ON mysql.user TO 'ru1'@'localhost'")
	mustExec(c, rootSe, "CREATE USER ru3")
	mustExec(c, rootSe, "GRANT SELECT ON mysql.db TO ru3")
	mustExec(c, rootSe, "CREATE USER ru6@localhost")
	mustExec(c, rootSe, "GRANT SELECT ON mysql.global_grants TO 'ru6'@'localhost'")

	se1 := newSession(c, s.store, s.dbName)

	c.Assert(se1.Auth(&auth.UserIdentity{Username: "ru1", Hostname: "localhost"}, nil, nil), IsTrue)
	_, err := se1.ExecuteInternal(context.Background(), "RENAME USER ru3 TO ru4")
	c.Assert(err.Error(), Equals, "[planner:1227]Access denied; you need (at least one of) the CREATE USER privilege(s) for this operation")
	mustExec(c, rootSe, "GRANT UPDATE ON mysql.user TO 'ru1'@'localhost'")
	_, err = se1.ExecuteInternal(context.Background(), "RENAME USER ru3 TO ru4")
	// Workaround for *errors.withStack type
	errString := err.Error()
	c.Assert(errString, Equals, "[planner:1227]Access denied; you need (at least one of) the CREATE USER privilege(s) for this operation")
	mustExec(c, rootSe, "GRANT CREATE USER ON *.* TO 'ru1'@'localhost'")
	_, err = se1.ExecuteInternal(context.Background(), "RENAME USER ru3 TO ru4")
	c.Assert(err, IsNil)
	_, err = se1.ExecuteInternal(context.Background(), "RENAME USER 'ru4'@'%' TO 'ru3'@'localhost'")
	c.Assert(err, IsNil)
	_, err = se1.ExecuteInternal(context.Background(), "RENAME USER 'ru3'@'localhost' TO 'ru3'@'%'")
	c.Assert(err, IsNil)
	_, err = rootSe.ExecuteInternal(context.Background(), "RENAME USER ru3 TO ru1@localhost")
	c.Assert(err.Error(), Matches, "\\[executor:1396\\]Operation RENAME USER failed for ru3@%.*")
	_, err = se1.ExecuteInternal(context.Background(), "RENAME USER ru4 TO ru5@localhost")
	c.Assert(err.Error(), Matches, "\\[executor:1396\\]Operation RENAME USER failed for ru4@%.*")
	_, err = se1.ExecuteInternal(context.Background(), "RENAME USER ru3 TO ru3")
	c.Assert(err.Error(), Matches, "\\[executor:1396\\]Operation RENAME USER failed for ru3@%.*")
	// Needed to avoid panic due to loc == nil in Time.ConvertTimeZone
	se1.GetSessionVars().TimeZone = time.UTC
	_, err = se1.ExecuteInternal(context.Background(), "RENAME USER 'ru3' TO 'ru3_tmp', ru6@localhost TO ru3, 'ru3_tmp' to ru6@localhost")
	c.Assert(err, IsNil)
	mustExec(c, rootSe, "DROP USER ru6@localhost")
	mustExec(c, rootSe, "DROP USER ru3")
	mustExec(c, rootSe, "DROP USER 'ru1'@'localhost'")
}
