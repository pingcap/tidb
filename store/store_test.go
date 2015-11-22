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

package store

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/localstore"
	"github.com/pingcap/tidb/store/localstore/boltdb"
	"github.com/pingcap/tidb/store/localstore/goleveldb"
)

var (
	testStore     = flag.String("teststore", "memory", "test store name, [memory, goleveldb, boltdb, hbase]")
	testStorePath = flag.String("testpath", "testkv", "test storage path")
)

const (
	startIndex = 0
	testCount  = 2
	indexStep  = 2
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testKVSuite{})

type testKVSuite struct {
	s kv.Storage
}

func (s *testKVSuite) SetUpSuite(c *C) {
	store, err := tidb.NewStore(fmt.Sprintf("%s://%s", *testStore, *testStorePath))
	c.Assert(err, IsNil)
	s.s = store

	cacheS, _ := tidb.NewStore(fmt.Sprintf("%s://%s", *testStore, *testStorePath))
	c.Assert(cacheS, Equals, store)
}

func (s *testKVSuite) TearDownSuite(c *C) {
	err := s.s.Close()
	c.Assert(err, IsNil)
}

func insertData(c *C, txn kv.Transaction) {
	for i := startIndex; i < testCount; i++ {
		val := encodeInt(i * indexStep)
		err := txn.Set(val, val)
		c.Assert(err, IsNil)
	}
}

func mustDel(c *C, txn kv.Transaction) {
	for i := startIndex; i < testCount; i++ {
		val := encodeInt(i * indexStep)
		err := txn.Delete(val)
		c.Assert(err, IsNil)
	}
}

func encodeInt(n int) []byte {
	return []byte(fmt.Sprintf("%010d", n))
}

func decodeInt(s []byte) int {
	var n int
	fmt.Sscanf(string(s), "%010d", &n)
	return n
}

func valToStr(c *C, iter kv.Iterator) string {
	val := iter.Value()
	return string(val)
}

func checkSeek(c *C, txn kv.Transaction) {
	for i := startIndex; i < testCount; i++ {
		val := encodeInt(i * indexStep)
		iter, err := txn.Seek(val)
		c.Assert(err, IsNil)
		c.Assert(iter.Key(), Equals, string(val))
		c.Assert(decodeInt([]byte(valToStr(c, iter))), Equals, i*indexStep)
		iter.Close()
	}

	// Test iterator Next()
	for i := startIndex; i < testCount-1; i++ {
		val := encodeInt(i * indexStep)
		iter, err := txn.Seek(val)
		c.Assert(err, IsNil)
		c.Assert(iter.Key(), Equals, string(val))
		c.Assert(valToStr(c, iter), Equals, string(val))

		err = iter.Next()
		c.Assert(err, IsNil)
		c.Assert(iter.Valid(), IsTrue)

		val = encodeInt((i + 1) * indexStep)
		c.Assert(iter.Key(), Equals, string(val))
		c.Assert(valToStr(c, iter), Equals, string(val))
		iter.Close()
	}

	// Non exist and beyond maximum seek test
	iter, err := txn.Seek(encodeInt(testCount * indexStep))
	c.Assert(err, IsNil)
	c.Assert(iter.Valid(), IsFalse)

	// Non exist but between existing keys seek test,
	// it returns the smallest key that larger than the one we are seeking
	inBetween := encodeInt((testCount-1)*indexStep - 1)
	last := encodeInt((testCount - 1) * indexStep)
	iter, err = txn.Seek(inBetween)
	c.Assert(err, IsNil)
	c.Assert(iter.Valid(), IsTrue)
	c.Assert(iter.Key(), Not(Equals), string(inBetween))
	c.Assert(iter.Key(), Equals, string(last))
	iter.Close()
}

func mustNotGet(c *C, txn kv.Transaction) {
	for i := startIndex; i < testCount; i++ {
		s := encodeInt(i * indexStep)
		_, err := txn.Get(s)
		c.Assert(err, NotNil)
	}
}

func mustGet(c *C, txn kv.Transaction) {
	for i := startIndex; i < testCount; i++ {
		s := encodeInt(i * indexStep)
		val, err := txn.Get(s)
		c.Assert(err, IsNil)
		c.Assert(string(val), Equals, string(s))
	}
}

func (s *testKVSuite) TestGetSet(c *C) {
	txn, err := s.s.Begin()
	c.Assert(err, IsNil)

	insertData(c, txn)

	mustGet(c, txn)

	// Check transaction results
	err = txn.Commit()
	c.Assert(err, IsNil)

	txn, err = s.s.Begin()
	c.Assert(err, IsNil)
	defer txn.Commit()

	mustGet(c, txn)
	mustDel(c, txn)
}

func (s *testKVSuite) TestSeek(c *C) {
	txn, err := s.s.Begin()
	c.Assert(err, IsNil)

	insertData(c, txn)
	checkSeek(c, txn)

	// Check transaction results
	err = txn.Commit()
	c.Assert(err, IsNil)

	txn, err = s.s.Begin()
	c.Assert(err, IsNil)
	defer txn.Commit()

	checkSeek(c, txn)
	mustDel(c, txn)
}

func (s *testKVSuite) TestInc(c *C) {
	txn, err := s.s.Begin()
	c.Assert(err, IsNil)

	key := []byte("incKey")
	n, err := txn.Inc(key, 100)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(100))

	// Check transaction results
	err = txn.Commit()
	c.Assert(err, IsNil)

	txn, err = s.s.Begin()
	c.Assert(err, IsNil)

	n, err = txn.Inc(key, -200)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(-100))

	err = txn.Delete(key)
	c.Assert(err, IsNil)

	n, err = txn.Inc(key, 100)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(100))

	err = txn.Delete(key)
	c.Assert(err, IsNil)

	err = txn.Commit()
	c.Assert(err, IsNil)
}

func (s *testKVSuite) TestDelete(c *C) {
	txn, err := s.s.Begin()
	c.Assert(err, IsNil)

	insertData(c, txn)

	mustDel(c, txn)

	mustNotGet(c, txn)
	txn.Commit()

	// Try get
	txn, err = s.s.Begin()
	c.Assert(err, IsNil)

	mustNotGet(c, txn)

	// Insert again
	insertData(c, txn)
	txn.Commit()

	// Delete all
	txn, err = s.s.Begin()
	c.Assert(err, IsNil)

	mustDel(c, txn)
	txn.Commit()

	txn, err = s.s.Begin()
	c.Assert(err, IsNil)

	mustNotGet(c, txn)
	txn.Commit()
}

func (s *testKVSuite) TestDelete2(c *C) {
	txn, err := s.s.Begin()
	c.Assert(err, IsNil)
	val := []byte("test")
	txn.Set([]byte("DATA_test_tbl_department_record__0000000001_0003"), val)
	txn.Set([]byte("DATA_test_tbl_department_record__0000000001_0004"), val)
	txn.Set([]byte("DATA_test_tbl_department_record__0000000002_0003"), val)
	txn.Set([]byte("DATA_test_tbl_department_record__0000000002_0004"), val)
	txn.Commit()

	// Delete all
	txn, err = s.s.Begin()
	c.Assert(err, IsNil)

	it, err := txn.Seek([]byte("DATA_test_tbl_department_record__0000000001_0003"))
	c.Assert(err, IsNil)
	for it.Valid() {
		err = txn.Delete([]byte(it.Key()))
		c.Assert(err, IsNil)
		err = it.Next()
		c.Assert(err, IsNil)
	}
	txn.Commit()

	txn, err = s.s.Begin()
	c.Assert(err, IsNil)
	it, _ = txn.Seek([]byte("DATA_test_tbl_department_record__000000000"))
	c.Assert(it.Valid(), IsFalse)
	txn.Commit()

}

func (s *testKVSuite) TestSetNil(c *C) {
	txn, err := s.s.Begin()
	defer txn.Commit()
	c.Assert(err, IsNil)
	err = txn.Set([]byte("1"), nil)
	c.Assert(err, NotNil)
}

func (s *testKVSuite) TestBasicSeek(c *C) {
	txn, err := s.s.Begin()
	c.Assert(err, IsNil)
	txn.Set([]byte("1"), []byte("1"))
	txn.Commit()
	txn, err = s.s.Begin()
	c.Assert(err, IsNil)
	defer txn.Commit()

	it, err := txn.Seek([]byte("2"))
	c.Assert(err, IsNil)
	c.Assert(it.Valid(), Equals, false)
	txn.Delete([]byte("1"))
}

func (s *testKVSuite) TestBasicTable(c *C) {
	txn, err := s.s.Begin()
	c.Assert(err, IsNil)
	for i := 1; i < 5; i++ {
		b := []byte(strconv.Itoa(i))
		txn.Set(b, b)
	}
	txn.Commit()
	txn, err = s.s.Begin()
	c.Assert(err, IsNil)
	defer txn.Commit()

	err = txn.Set([]byte("1"), []byte("1"))
	c.Assert(err, IsNil)

	it, err := txn.Seek([]byte("0"))
	c.Assert(err, IsNil)
	c.Assert(it.Key(), Equals, "1")

	err = txn.Set([]byte("0"), []byte("0"))
	c.Assert(err, IsNil)
	it, err = txn.Seek([]byte("0"))
	c.Assert(err, IsNil)
	c.Assert(it.Key(), Equals, "0")
	err = txn.Delete([]byte("0"))
	c.Assert(err, IsNil)

	txn.Delete([]byte("1"))
	it, err = txn.Seek([]byte("0"))
	c.Assert(err, IsNil)
	c.Assert(it.Key(), Equals, "2")

	err = txn.Delete([]byte("3"))
	c.Assert(err, IsNil)
	it, err = txn.Seek([]byte("2"))
	c.Assert(err, IsNil)
	c.Assert(it.Key(), Equals, "2")

	it, err = txn.Seek([]byte("3"))
	c.Assert(err, IsNil)
	c.Assert(it.Key(), Equals, "4")
	err = txn.Delete([]byte("2"))
	c.Assert(err, IsNil)
	err = txn.Delete([]byte("4"))
	c.Assert(err, IsNil)
	// Test delete a key which not exist
	err = txn.Delete([]byte("5"))
	c.Assert(err, NotNil)
}

func (s *testKVSuite) TestRollback(c *C) {
	txn, err := s.s.Begin()
	c.Assert(err, IsNil)

	err = txn.Rollback()
	c.Assert(err, IsNil)

	txn, err = s.s.Begin()
	c.Assert(err, IsNil)

	insertData(c, txn)

	mustGet(c, txn)

	err = txn.Rollback()
	c.Assert(err, IsNil)

	txn, err = s.s.Begin()
	c.Assert(err, IsNil)
	defer txn.Commit()

	for i := startIndex; i < testCount; i++ {
		_, err := txn.Get([]byte(strconv.Itoa(i)))
		c.Assert(err, NotNil)
	}
}

func (s *testKVSuite) TestSeekMin(c *C) {
	kvs := []struct {
		key   string
		value string
	}{
		{"DATA_test_main_db_tbl_tbl_test_record__00000000000000000001", "lock-version"},
		{"DATA_test_main_db_tbl_tbl_test_record__00000000000000000001_0002", "1"},
		{"DATA_test_main_db_tbl_tbl_test_record__00000000000000000001_0003", "hello"},
		{"DATA_test_main_db_tbl_tbl_test_record__00000000000000000002", "lock-version"},
		{"DATA_test_main_db_tbl_tbl_test_record__00000000000000000002_0002", "2"},
		{"DATA_test_main_db_tbl_tbl_test_record__00000000000000000002_0003", "hello"},
	}

	txn, err := s.s.Begin()
	c.Assert(err, IsNil)
	for _, kv := range kvs {
		txn.Set([]byte(kv.key), []byte(kv.value))
	}

	it, err := txn.Seek(nil)
	for it.Valid() {
		fmt.Printf("%s, %s\n", it.Key(), it.Value())
		it.Next()
	}

	it, err = txn.Seek([]byte("DATA_test_main_db_tbl_tbl_test_record__00000000000000000000"))
	c.Assert(err, IsNil)
	c.Assert(string(it.Key()), Equals, "DATA_test_main_db_tbl_tbl_test_record__00000000000000000001")

	for _, kv := range kvs {
		txn.Delete([]byte(kv.key))
	}
}

func (s *testKVSuite) TestConditionIfNotExist(c *C) {
	var success int64
	cnt := 100
	b := []byte("1")
	var wg sync.WaitGroup
	wg.Add(cnt)
	for i := 0; i < cnt; i++ {
		go func() {
			defer wg.Done()
			txn, err := s.s.Begin()
			c.Assert(err, IsNil)
			err = txn.Set(b, b)
			if err != nil {
				return
			}
			err = txn.Commit()
			if err == nil {
				atomic.AddInt64(&success, 1)
			}
		}()
	}
	wg.Wait()
	// At least one txn can success.
	c.Assert(success, Greater, int64(0))

	// Clean up
	txn, err := s.s.Begin()
	c.Assert(err, IsNil)
	err = txn.Delete(b)
	c.Assert(err, IsNil)
	err = txn.Commit()
	c.Assert(err, IsNil)
}

func (s *testKVSuite) TestConditionIfEqual(c *C) {
	var success int64
	cnt := 100
	b := []byte("1")
	var wg sync.WaitGroup
	wg.Add(cnt)

	txn, err := s.s.Begin()
	c.Assert(err, IsNil)
	txn.Set(b, b)
	err = txn.Commit()
	c.Assert(err, IsNil)

	for i := 0; i < cnt; i++ {
		go func() {
			defer wg.Done()
			// Use txn1/err1 instead of txn/err is
			// to pass `go tool vet -shadow` check.
			txn1, err1 := s.s.Begin()
			c.Assert(err1, IsNil)
			txn1.Set(b, []byte("newValue"))
			err1 = txn1.Commit()
			if err1 == nil {
				atomic.AddInt64(&success, 1)
			}
		}()
	}
	wg.Wait()
	c.Assert(success, Greater, int64(0))

	// Clean up
	txn, err = s.s.Begin()
	c.Assert(err, IsNil)
	err = txn.Delete(b)
	c.Assert(err, IsNil)
	err = txn.Commit()
	c.Assert(err, IsNil)
}

func (s *testKVSuite) TestConditionUpdate(c *C) {
	txn, err := s.s.Begin()
	c.Assert(err, IsNil)
	txn.Delete([]byte("b"))
	txn.Inc([]byte("a"), 1)
	err = txn.Commit()
	c.Assert(err, IsNil)
}

func (s *testKVSuite) TestDBClose(c *C) {
	path := "memory:test"
	d := localstore.Driver{
		Driver: goleveldb.MemoryDriver{},
	}
	store, err := d.Open(path)
	c.Assert(err, IsNil)

	txn, err := store.Begin()
	c.Assert(err, IsNil)

	err = txn.Set([]byte("a"), []byte("b"))
	c.Assert(err, IsNil)

	err = txn.Commit()
	c.Assert(err, IsNil)

	ver, err := store.CurrentVersion()
	c.Assert(err, IsNil)
	c.Assert(kv.MaxVersion.Cmp(ver), Equals, 1)

	snap, err := store.GetSnapshot(kv.MaxVersion)
	c.Assert(err, IsNil)

	_, err = snap.MvccGet(kv.EncodeKey([]byte("a")), kv.MaxVersion)
	c.Assert(err, IsNil)

	txn, err = store.Begin()
	c.Assert(err, IsNil)

	err = store.Close()
	c.Assert(err, IsNil)

	_, err = store.Begin()
	c.Assert(err, NotNil)

	_, err = store.GetSnapshot(kv.MaxVersion)
	c.Assert(err, NotNil)

	err = txn.Set([]byte("a"), []byte("b"))
	c.Assert(err, IsNil)

	err = txn.Commit()
	c.Assert(err, NotNil)

	snap.MvccRelease()
}

func (s *testKVSuite) TestBoltDBDeadlock(c *C) {
	d := localstore.Driver{
		Driver: boltdb.Driver{},
	}
	path := "boltdb_test"
	defer os.Remove(path)
	store, err := d.Open(path)
	c.Assert(err, IsNil)
	defer store.Close()

	kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
		txn.Set([]byte("a"), []byte("0"))
		txn.Inc([]byte("a"), 1)

		kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
			txn.Set([]byte("b"), []byte("0"))
			txn.Inc([]byte("b"), 1)

			return nil
		})

		return nil
	})

	kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
		n, err := txn.GetInt64([]byte("a"))
		c.Assert(err, IsNil)
		c.Assert(n, Equals, int64(1))

		n, err = txn.GetInt64([]byte("b"))
		c.Assert(err, IsNil)
		c.Assert(n, Equals, int64(1))
		return nil
	})
}
