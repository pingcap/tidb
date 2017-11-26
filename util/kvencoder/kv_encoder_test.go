// Copyright 2017 PingCAP, Inc.
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

package kvenc

import (
	"bytes"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testKvEncoderSuite{})

func TestT(t *testing.T) {
	TestingT(t)
}

func newStoreWithBootstrap() (kv.Storage, *domain.Domain, error) {
	store, err := tikv.NewMockTikvStore()
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	tidb.SetSchemaLease(0)
	dom, err := tidb.BootstrapSession(store)
	return store, dom, errors.Trace(err)
}

type mockAllocator struct {
	base     int64
	retrying bool
}

func NewMockAllocator() autoid.Allocator {
	return &mockAllocator{base: 1}
}

func (alloc *mockAllocator) Alloc(tableID int64) (int64, error) {
	if alloc.retrying {
		// return previous id directly
		return alloc.base, nil
	}
	id := atomic.AddInt64(&alloc.base, 1)
	return id, nil
}

func (alloc *mockAllocator) Rebase(tableID, newBase int64, allocIDs bool) error {
	atomic.StoreInt64(&alloc.base, newBase)
	return nil
}

func (alloc *mockAllocator) Base() int64 {
	return alloc.base
}

func (alloc *mockAllocator) End() int64 {
	return alloc.base + 100
}

type testKvEncoderSuite struct {
	store       kv.Storage
	dom         *domain.Domain
	storeExpect kv.Storage
	dom1        *domain.Domain
}

func (s *testKvEncoderSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	var err error
	s.store, s.dom, err = newStoreWithBootstrap()
	c.Assert(err, IsNil)
	s.storeExpect, s.dom1, err = newStoreWithBootstrap()
	c.Assert(err, IsNil)
}

func (s *testKvEncoderSuite) TearDownSuite(c *C) {
	s.dom.Close()
	s.store.Close()
	testleak.AfterTest(c)()
}

func cleanup(c *C, store kv.Storage) {
	tk := testkit.NewTestKit(c, store)
	tk.MustExec("use test")
	r := tk.MustQuery("show tables")
	for _, tb := range r.Rows() {
		tableName := tb[0]
		tk.MustExec(fmt.Sprintf("drop table %v", tableName))
	}
}

func (s *testKvEncoderSuite) cleanEnv(c *C) {
	cleanup(c, s.store)
	cleanup(c, s.storeExpect)
}

func getExpectKvPairs(tkExpect *testkit.TestKit, sql string) []KvPair {
	tkExpect.MustExec("begin")
	tkExpect.MustExec(sql)
	kvPairsExpect := make([]KvPair, 0)
	kv.WalkMemBuffer(tkExpect.Se.Txn().GetMemBuffer(), func(k kv.Key, v []byte) error {
		kvPairsExpect = append(kvPairsExpect, KvPair{Key: k, Val: v})
		return nil
	})
	tkExpect.MustExec("rollback")
	return kvPairsExpect
}

type testCase struct {
	sql string
	// expectEmptyValCnt only check if expectEmptyValCnt > 0
	expectEmptyValCnt int
	// expectKvCnt only check if expectKvCnt > 0
	expectKvCnt int
}

func (s *testKvEncoderSuite) runTestSQL(c *C, tk, tkExpect *testkit.TestKit, encoder KvEncoder, cases []testCase) {
	for _, ca := range cases {
		comment := fmt.Sprintf("sql:%v", ca.sql)
		kvPairs, err := encoder.Encode(ca.sql)
		c.Assert(err, IsNil, Commentf(comment))

		kvPairsExpect := getExpectKvPairs(tkExpect, ca.sql)
		c.Assert(len(kvPairs), Equals, len(kvPairsExpect), Commentf(comment))
		if ca.expectKvCnt > 0 {
			c.Assert(len(kvPairs), Equals, ca.expectKvCnt, Commentf(comment))
		}

		emptyValCount := 0
		for i, kv := range kvPairs {
			c.Assert(bytes.Compare(kv.Key, kvPairsExpect[i].Key), Equals, 0, Commentf(comment))
			c.Assert(bytes.Compare(kv.Val, kvPairsExpect[i].Val), Equals, 0, Commentf(comment))
			if len(kv.Val) == 0 {
				emptyValCount++
			}
		}
		if ca.expectEmptyValCnt > 0 {
			c.Assert(emptyValCount, Equals, ca.expectEmptyValCnt, Commentf(comment))
		}
	}
}

func (s *testKvEncoderSuite) TestInsertPkIsHandle(c *C) {
	defer s.cleanEnv(c)
	tk := testkit.NewTestKit(c, s.store)
	tkExpect := testkit.NewTestKit(c, s.storeExpect)
	tk.MustExec("use test")
	tkExpect.MustExec("use test")

	encoder, err := New(s.store, "test", nil, true)
	c.Assert(err, IsNil)

	schemaSQL := "create table t(id int auto_increment, a char(10), primary key(id))"
	tk.MustExec(schemaSQL)
	tkExpect.MustExec(schemaSQL)

	sqls := []testCase{
		{"insert into t values(1, 'test');", 0, 1},
		{"insert into t(a) values('test')", 0, 1},
		{"insert into t(id, a) values(3, 'test')", 0, 1},
		{"insert into t values(1000000, 'test')", 0, 1},
		{"insert into t(a) values('test')", 0, 1},
		{"insert into t(id, a) values(4, 'test')", 0, 1},
	}

	s.runTestSQL(c, tk, tkExpect, encoder, sqls)

	schemaSQL = "create table t1(id int auto_increment, a char(10), primary key(id), key a_idx(a))"
	tk.MustExec(schemaSQL)
	tkExpect.MustExec(schemaSQL)

	sqls = []testCase{
		{"insert into t1 values(1, 'test');", 0, 2},
		{"insert into t1(a) values('test')", 0, 2},
		{"insert into t1(id, a) values(3, 'test')", 0, 2},
		{"insert into t1 values(1000000, 'test')", 0, 2},
		{"insert into t1(a) values('test')", 0, 2},
		{"insert into t1(id, a) values(4, 'test')", 0, 2},
	}

	s.runTestSQL(c, tk, tkExpect, encoder, sqls)

	schemaSQL = `create table t2(
		id int auto_increment, 
		a char(10), 
		b datetime default NULL, 
		primary key(id), 
		key a_idx(a),
		unique b_idx(b))
		`

	tk.MustExec(schemaSQL)
	tkExpect.MustExec(schemaSQL)

	sqls = []testCase{
		{"insert into t2(id, a) values(1, 'test')", 0, 3},
		{"insert into t2 values(2, 'test', '2017-11-27 23:59:59')", 0, 3},
		{"insert into t2 values(3, 'test', '2017-11-28 23:59:59')", 0, 3},
	}
	s.runTestSQL(c, tk, tkExpect, encoder, sqls)
}

func (s *testKvEncoderSuite) TestInsertPkIsNotHandle(c *C) {
	defer s.cleanEnv(c)
	tk := testkit.NewTestKit(c, s.store)
	tkExpect := testkit.NewTestKit(c, s.storeExpect)
	tk.MustExec("use test")
	tkExpect.MustExec("use test")

	encoder, err := New(s.store, "test", nil, true)
	c.Assert(err, IsNil)

	schemaSQL := `create table t(
		id varchar(20),
		a char(10),
		primary key(id))`
	tk.MustExec(schemaSQL)
	tkExpect.MustExec(schemaSQL)

	sqls := []testCase{
		{"insert into t values(1, 'test');", 0, 2},
		{"insert into t(id, a) values(2, 'test')", 0, 2},
		{"insert into t(id, a) values(3, 'test')", 0, 2},
		{"insert into t values(1000000, 'test')", 0, 2},
		{"insert into t(id, a) values(5, 'test')", 0, 2},
		{"insert into t(id, a) values(4, 'test')", 0, 2},
	}

	s.runTestSQL(c, tk, tkExpect, encoder, sqls)
}

func insertData(tk, tkExpect *testkit.TestKit, sqls []string) {
	for _, sql := range sqls {
		tk.MustExec(sql)
		tkExpect.MustExec(sql)
	}
}

func (s *testKvEncoderSuite) TestUpdate(c *C) {
	defer s.cleanEnv(c)
	tk := testkit.NewTestKit(c, s.store)
	tkExpect := testkit.NewTestKit(c, s.storeExpect)
	tk.MustExec("use test")
	tkExpect.MustExec("use test")

	encoder, err := New(s.store, "test", nil, false)
	c.Assert(err, IsNil)

	schemaSQL := `create table t(
		id varchar(20),
		a char(10),
		primary key(id))`
	tk.MustExec(schemaSQL)
	tkExpect.MustExec(schemaSQL)
	sqls := []string{
		"insert into t values(1, 'test');",
		"insert into t(id, a) values(2, 'test')",
		"insert into t(id, a) values(3, 'test')",
		"insert into t values(1000000, 'test')",
		"insert into t(id, a) values(5, 'test')",
		"insert into t(id, a) values(4, 'test')",
	}
	insertData(tk, tkExpect, sqls)

	cases := []testCase{
		{"update t set a = 'hello' where id = 1", 0, 1},
		{"update t set a = 'hello' where id = 2", 0, 1},
		{"update t set a = 'hello' where id = 10000000000000", 0, 0},
		// remove old handle, and update primary key index, add new handle
		{"update t set id = 10 where id = 1", 1, 3},
	}
	s.runTestSQL(c, tk, tkExpect, encoder, cases)
}

func (s *testKvEncoderSuite) TestDelete(c *C) {
	defer s.cleanEnv(c)
	tk := testkit.NewTestKit(c, s.store)
	tkExpect := testkit.NewTestKit(c, s.storeExpect)
	tk.MustExec("use test")
	tkExpect.MustExec("use test")

	encoder, err := New(s.store, "test", nil, false)
	c.Assert(err, IsNil)

	schemaSQL := `create table t(
		id varchar(20),
		a char(10),
		primary key(id))`
	tk.MustExec(schemaSQL)
	tkExpect.MustExec(schemaSQL)
	sqls := []string{
		"insert into t values(1, 'test');",
		"insert into t(id, a) values(2, 'test')",
		"insert into t(id, a) values(3, 'test')",
		"insert into t values(1000000, 'test')",
		"insert into t(id, a) values(5, 'test')",
		"insert into t(id, a) values(4, 'test')",
	}
	insertData(tk, tkExpect, sqls)

	cases := []testCase{
		// remove handle and primary key
		{"delete from t where id = 1", 2, 2},
		{"delete from t where id = 1000000000", 0, 0},
		{"delete from t where id = 2", 2, 2},
	}
	s.runTestSQL(c, tk, tkExpect, encoder, cases)
}

func (s *testKvEncoderSuite) TestRetryWithAllocator(c *C) {
	fmt.Println("TestRetryWithAllocator")
	defer s.cleanEnv(c)
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer store.Close()
	defer dom.Close()
	tk := testkit.NewTestKit(c, store)

	tk.MustExec("use test")
	alloc := NewMockAllocator()
	encoder, err := New(store, "test", alloc, true)
	c.Assert(err, IsNil)
	mockAlloc := alloc.(*mockAllocator)

	schemaSQL := `create table t(
		id int auto_increment, 
		a char(10),
		primary key(id))`
	tk.MustExec(schemaSQL)
	sqls := []string{
		"insert into t(a) values('test');",
		"insert into t(id, a) values(1000000, 'test')",
		"insert into t(id, a) values(5, 'test')",
		"insert into t(id, a) values(4, 'test')",
	}

	for _, sql := range sqls {
		kvPairs, err1 := encoder.Encode(sql)
		c.Assert(err1, IsNil, Commentf("sql:%s", sql))
		mockAlloc.retrying = true
		retryKvPairs, err1 := encoder.Encode(sql)
		c.Assert(err1, IsNil, Commentf("sql:%s", sql))
		c.Assert(len(kvPairs), Equals, len(retryKvPairs))
		for i, kv := range kvPairs {
			c.Assert(bytes.Compare(kv.Key, retryKvPairs[i].Key), Equals, 0, Commentf(sql))
			c.Assert(bytes.Compare(kv.Val, retryKvPairs[i].Val), Equals, 0, Commentf(sql))
		}
		mockAlloc.retrying = false
	}

	mockAlloc.retrying = false
	// specify id, it must be the same kv, no need to use mockAlloc
	sql := "insert into t(id, a) values(5, 'test')"
	kvPairs, err := encoder.Encode(sql)
	c.Assert(err, IsNil, Commentf("sql:%s", sql))
	retryKvPairs, err := encoder.Encode(sql)
	c.Assert(err, IsNil, Commentf("sql:%s", sql))
	c.Assert(len(kvPairs), Equals, len(retryKvPairs))
	for i, kv := range kvPairs {
		c.Assert(bytes.Compare(kv.Key, retryKvPairs[i].Key), Equals, 0, Commentf(sql))
		c.Assert(bytes.Compare(kv.Val, retryKvPairs[i].Val), Equals, 0, Commentf(sql))
	}
}
