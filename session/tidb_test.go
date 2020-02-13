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

package session

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/testleak"
)

func TestT(t *testing.T) {
	logLevel := os.Getenv("log_level")
	logutil.InitLogger(logutil.NewLogConfig(logLevel, logutil.DefaultLogFormat, "", logutil.EmptyFileLogConfig, false))
	CustomVerboseFlag = true
	SetSchemaLease(20 * time.Millisecond)
	TestingT(t)
}

var _ = Suite(&testMainSuite{})
var _ = SerialSuites(&testBootstrapSuite{})

type testMainSuite struct {
	dbName string
	store  kv.Storage
	dom    *domain.Domain
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

func (s *testMainSuite) TestSysSessionPoolGoroutineLeak(c *C) {
	store, dom := newStoreWithBootstrap(c, s.dbName+"goroutine_leak")
	defer store.Close()
	defer dom.Close()
	se, err := createSession(store)
	c.Assert(err, IsNil)

	// Test an issue that sysSessionPool doesn't call session's Close, cause
	// asyncGetTSWorker goroutine leak.
	count := 200
	var wg sync.WaitGroup
	wg.Add(count)
	for i := 0; i < count; i++ {
		go func(se *session) {
			_, _, err := se.ExecRestrictedSQL("select * from mysql.user limit 1")
			c.Assert(err, IsNil)
			wg.Done()
		}(se)
	}
	wg.Wait()
}

func (s *testMainSuite) TestParseErrorWarn(c *C) {
	ctx := core.MockContext()

	nodes, err := Parse(ctx, "select /*+ adf */ 1")
	c.Assert(err, IsNil)
	c.Assert(len(nodes), Equals, 1)
	c.Assert(len(ctx.GetSessionVars().StmtCtx.GetWarnings()), Equals, 1)

	_, err = Parse(ctx, "select")
	c.Assert(err, NotNil)
}

func newStore(c *C, dbPath string) kv.Storage {
	store, err := mockstore.NewMockTikvStore()
	c.Assert(err, IsNil)
	return store
}

func newStoreWithBootstrap(c *C, dbPath string) (kv.Storage, *domain.Domain) {
	store, err := mockstore.NewMockTikvStore()
	c.Assert(err, IsNil)
	dom, err := BootstrapSession(store)
	c.Assert(err, IsNil)
	return store, dom
}

var testConnID uint64

func newSession(c *C, store kv.Storage, dbName string) Session {
	se, err := CreateSession4Test(store)
	id := atomic.AddUint64(&testConnID, 1)
	se.SetConnectionID(id)
	c.Assert(err, IsNil)
	se.Auth(&auth.UserIdentity{Username: "root", Hostname: `%`}, nil, []byte("012345678901234567890"))
	mustExecSQL(c, se, "create database if not exists "+dbName)
	mustExecSQL(c, se, "use "+dbName)
	return se
}

func removeStore(c *C, dbPath string) {
	os.RemoveAll(dbPath)
}

func exec(se Session, sql string, args ...interface{}) (sqlexec.RecordSet, error) {
	ctx := context.Background()
	if len(args) == 0 {
		rs, err := se.Execute(ctx, sql)
		if err == nil && len(rs) > 0 {
			return rs[0], nil
		}
		return nil, err
	}
	stmtID, _, _, err := se.PrepareStmt(sql)
	if err != nil {
		return nil, err
	}
	params := make([]types.Datum, len(args))
	for i := 0; i < len(params); i++ {
		params[i] = types.NewDatum(args[i])
	}
	rs, err := se.ExecutePreparedStmt(ctx, stmtID, params)
	if err != nil {
		return nil, err
	}
	return rs, nil
}

func mustExecSQL(c *C, se Session, sql string, args ...interface{}) sqlexec.RecordSet {
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

func (s *testMainSuite) TestKeysNeedLock(c *C) {
	rowKey := tablecodec.EncodeRowKeyWithHandle(1, 1)
	indexKey := tablecodec.EncodeIndexSeekKey(1, 1, []byte{1})
	uniqueValue := make([]byte, 8)
	uniqueUntouched := append(uniqueValue, '1')
	nonUniqueVal := []byte{'0'}
	nonUniqueUntouched := []byte{'1'}
	var deleteVal []byte
	rowVal := []byte{'a', 'b', 'c'}
	tests := []struct {
		key  []byte
		val  []byte
		need bool
	}{
		{rowKey, rowVal, true},
		{rowKey, deleteVal, true},
		{indexKey, nonUniqueVal, false},
		{indexKey, nonUniqueUntouched, false},
		{indexKey, uniqueValue, true},
		{indexKey, uniqueUntouched, false},
		{indexKey, deleteVal, false},
	}
	for _, tt := range tests {
		c.Assert(keyNeedToLock(tt.key, tt.val), Equals, tt.need)
	}
}
