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

package tidb

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/pd/pkg/logutil"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/auth"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/types"
)

var store = flag.String("store", "memory", "registered store name, [memory, goleveldb, boltdb]")

func TestT(t *testing.T) {
	logLevel := os.Getenv("log_level")
	logutil.InitLogger(&logutil.LogConfig{
		Level: logLevel,
	})
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testMainSuite{})

type testMainSuite struct {
	dbName string
	store  kv.Storage
	dom    *domain.Domain
}

type brokenStore struct{}

func (s *brokenStore) Open(schema string) (kv.Storage, error) {
	return nil, errors.New("try again later")
}

func (s *testMainSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	s.dbName = "test_main_db"
	s.store = newStore(c, s.dbName)
	dom, err := BootstrapSession(s.store)
	c.Assert(err, IsNil)
	s.dom = dom
}

func (s *testMainSuite) TearDownSuite(c *C) {
	defer testleak.AfterTest(c)()
	s.dom.Close()
	err := s.store.Close()
	c.Assert(err, IsNil)
	removeStore(c, s.dbName)
}

// Testcase for arg type.
func (s *testMainSuite) TestCheckArgs(c *C) {
	checkArgs(nil, true, false, int8(1), int16(1), int32(1), int64(1), 1,
		uint8(1), uint16(1), uint32(1), uint64(1), uint(1), float32(1), float64(1),
		"abc", []byte("abc"), time.Now(), time.Hour, time.Local)
}

func (s *testMainSuite) TestIsQuery(c *C) {
	tbl := []struct {
		sql string
		ok  bool
	}{
		{"/*comment*/ select 1;", true},
		{"/*comment*/ /*comment*/ select 1;", true},
		{"select /*comment*/ 1 /*comment*/;", true},
		{"(select /*comment*/ 1 /*comment*/);", true},
	}
	for _, t := range tbl {
		c.Assert(IsQuery(t.sql), Equals, t.ok, Commentf(t.sql))
	}
}

func (s *testMainSuite) TestTrimSQL(c *C) {
	tbl := []struct {
		sql    string
		target string
	}{
		{"/*comment*/ select 1; ", "select 1;"},
		{"/*comment*/ /*comment*/ select 1;", "select 1;"},
		{"select /*comment*/ 1 /*comment*/;", "select /*comment*/ 1 /*comment*/;"},
		{"/*comment select 1; ", "/*comment select 1;"},
	}
	for _, t := range tbl {
		c.Assert(trimSQL(t.sql), Equals, t.target, Commentf(t.sql))
	}
}

func (s *testMainSuite) TestRetryOpenStore(c *C) {
	begin := time.Now()
	RegisterStore("dummy", &brokenStore{})
	_, err := newStoreWithRetry("dummy://dummy-store", 3)
	c.Assert(err, NotNil)
	elapse := time.Since(begin)
	c.Assert(uint64(elapse), GreaterEqual, uint64(3*time.Second))
}

func (s *testMainSuite) TestSysSessionPoolGoroutineLeak(c *C) {
	c.Skip("make leak should check it")
	// TODO: testleak package should be able to find this leak.
	store, dom := newStoreWithBootstrap(c, s.dbName+"goroutine_leak")
	defer dom.Close()
	defer store.Close()
	se, err := createSession(store)
	c.Assert(err, IsNil)

	// Test an issue that sysSessionPool doesn't call session's Close, cause
	// asyncGetTSWorker goroutine leak.
	before := runtime.NumGoroutine()
	count := 200
	var wg sync.WaitGroup
	wg.Add(count)
	for i := 0; i < count; i++ {
		go func(se *session) {
			_, _, err := se.ExecRestrictedSQL(se, "select * from mysql.user limit 1")
			c.Assert(err, IsNil)
			wg.Done()
		}(se)
	}
	wg.Wait()
	se.sysSessionPool().Close()
	c.Assert(se.sysSessionPool().IsClosed(), Equals, true)
	for i := 0; i < 300; i++ {
		// After and before should be Equal, but this test may be disturbed by other factors.
		// So I relax the strict check to make CI more stable.
		after := runtime.NumGoroutine()
		if after-before < 3 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	after := runtime.NumGoroutine()
	c.Assert(after-before, Less, 3)
}

func newStore(c *C, dbPath string) kv.Storage {
	store, err := NewStore(*store + "://" + dbPath)
	c.Assert(err, IsNil)
	return store
}

func newStoreWithBootstrap(c *C, dbPath string) (kv.Storage, *domain.Domain) {
	store := newStore(c, dbPath)
	dom, err := BootstrapSession(store)
	c.Assert(err, IsNil)
	return store, dom
}

var testConnID uint64

func newSession(c *C, store kv.Storage, dbName string) Session {
	se, err := CreateSession(store)
	id := atomic.AddUint64(&testConnID, 1)
	se.SetConnectionID(id)
	c.Assert(err, IsNil)
	se.Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, []byte("012345678901234567890"))
	mustExecSQL(c, se, "create database if not exists "+dbName)
	mustExecSQL(c, se, "use "+dbName)
	return se
}

func removeStore(c *C, dbPath string) {
	os.RemoveAll(dbPath)
}

func exec(se Session, sql string, args ...interface{}) (ast.RecordSet, error) {
	if len(args) == 0 {
		rs, err := se.Execute(sql)
		if err == nil && len(rs) > 0 {
			return rs[0], nil
		}
		return nil, err
	}
	stmtID, _, _, err := se.PrepareStmt(sql)
	if err != nil {
		return nil, err
	}
	rs, err := se.ExecutePreparedStmt(stmtID, args...)
	if err != nil {
		return nil, err
	}
	return rs, nil
}

func mustExecSQL(c *C, se Session, sql string, args ...interface{}) ast.RecordSet {
	rs, err := exec(se, sql, args...)
	c.Assert(err, IsNil)
	return rs
}

func match(c *C, row []types.Datum, expected ...interface{}) {
	c.Assert(len(row), Equals, len(expected))
	for i := range row {
		got := fmt.Sprintf("%v", row[i].GetValue())
		need := fmt.Sprintf("%v", expected[i])
		c.Assert(got, Equals, need)
	}
}

func matches(c *C, rows [][]types.Datum, expected [][]interface{}) {
	c.Assert(len(rows), Equals, len(expected))
	for i := 0; i < len(rows); i++ {
		match(c, rows[i], expected[i]...)
	}
}

func mustExecMatch(c *C, se Session, sql string, expected [][]interface{}) {
	r := mustExecSQL(c, se, sql)
	rows, err := GetRows(r)
	c.Assert(err, IsNil)
	matches(c, rows, expected)
}

func mustExecFailed(c *C, se Session, sql string, args ...interface{}) {
	r, err := exec(se, sql, args...)
	if err == nil && r != nil {
		// sometimes we may meet error after executing first row.
		_, err = r.Next()
	}
	c.Assert(err, NotNil)
}
