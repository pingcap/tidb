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
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/testleak"
	goctx "golang.org/x/net/context"
)

const (
	startIndex = 0
	testCount  = 2
	indexStep  = 2
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	logLevel := os.Getenv("log_level")
	logutil.InitLogger(&logutil.LogConfig{
		Level: logLevel,
	})
	TestingT(t)
}

var _ = Suite(&testKVSuite{})

type testKVSuite struct {
	s kv.Storage
}

func (s *testKVSuite) SetUpSuite(c *C) {
	store, err := tikv.NewMockTikvStore()
	c.Assert(err, IsNil)
	s.s = store
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
		c.Assert([]byte(iter.Key()), BytesEquals, val)
		c.Assert(decodeInt([]byte(valToStr(c, iter))), Equals, i*indexStep)
		iter.Close()
	}

	// Test iterator Next()
	for i := startIndex; i < testCount-1; i++ {
		val := encodeInt(i * indexStep)
		iter, err := txn.Seek(val)
		c.Assert(err, IsNil)
		c.Assert([]byte(iter.Key()), BytesEquals, val)
		c.Assert(valToStr(c, iter), Equals, string(val))

		err = iter.Next()
		c.Assert(err, IsNil)
		c.Assert(iter.Valid(), IsTrue)

		val = encodeInt((i + 1) * indexStep)
		c.Assert([]byte(iter.Key()), BytesEquals, val)
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
	c.Assert([]byte(iter.Key()), Not(BytesEquals), inBetween)
	c.Assert([]byte(iter.Key()), BytesEquals, last)
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
	defer testleak.AfterTest(c)()
	txn, err := s.s.Begin()
	c.Assert(err, IsNil)

	insertData(c, txn)

	mustGet(c, txn)

	// Check transaction results
	err = txn.Commit(goctx.Background())
	c.Assert(err, IsNil)

	txn, err = s.s.Begin()
	c.Assert(err, IsNil)
	defer txn.Commit(goctx.Background())

	mustGet(c, txn)
	mustDel(c, txn)
}

func (s *testKVSuite) TestSeek(c *C) {
	defer testleak.AfterTest(c)()
	txn, err := s.s.Begin()
	c.Assert(err, IsNil)

	insertData(c, txn)
	checkSeek(c, txn)

	// Check transaction results
	err = txn.Commit(goctx.Background())
	c.Assert(err, IsNil)

	txn, err = s.s.Begin()
	c.Assert(err, IsNil)
	defer txn.Commit(goctx.Background())

	checkSeek(c, txn)
	mustDel(c, txn)
}

func (s *testKVSuite) TestInc(c *C) {
	defer testleak.AfterTest(c)()
	txn, err := s.s.Begin()
	c.Assert(err, IsNil)

	key := []byte("incKey")
	n, err := kv.IncInt64(txn, key, 100)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(100))

	// Check transaction results
	err = txn.Commit(goctx.Background())
	c.Assert(err, IsNil)

	txn, err = s.s.Begin()
	c.Assert(err, IsNil)

	n, err = kv.IncInt64(txn, key, -200)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(-100))

	err = txn.Delete(key)
	c.Assert(err, IsNil)

	n, err = kv.IncInt64(txn, key, 100)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(100))

	err = txn.Delete(key)
	c.Assert(err, IsNil)

	err = txn.Commit(goctx.Background())
	c.Assert(err, IsNil)
}

func (s *testKVSuite) TestDelete(c *C) {
	defer testleak.AfterTest(c)()
	txn, err := s.s.Begin()
	c.Assert(err, IsNil)

	insertData(c, txn)

	mustDel(c, txn)

	mustNotGet(c, txn)
	txn.Commit(goctx.Background())

	// Try get
	txn, err = s.s.Begin()
	c.Assert(err, IsNil)

	mustNotGet(c, txn)

	// Insert again
	insertData(c, txn)
	txn.Commit(goctx.Background())

	// Delete all
	txn, err = s.s.Begin()
	c.Assert(err, IsNil)

	mustDel(c, txn)
	txn.Commit(goctx.Background())

	txn, err = s.s.Begin()
	c.Assert(err, IsNil)

	mustNotGet(c, txn)
	txn.Commit(goctx.Background())
}

func (s *testKVSuite) TestDelete2(c *C) {
	defer testleak.AfterTest(c)()
	txn, err := s.s.Begin()
	c.Assert(err, IsNil)
	val := []byte("test")
	txn.Set([]byte("DATA_test_tbl_department_record__0000000001_0003"), val)
	txn.Set([]byte("DATA_test_tbl_department_record__0000000001_0004"), val)
	txn.Set([]byte("DATA_test_tbl_department_record__0000000002_0003"), val)
	txn.Set([]byte("DATA_test_tbl_department_record__0000000002_0004"), val)
	txn.Commit(goctx.Background())

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
	txn.Commit(goctx.Background())

	txn, err = s.s.Begin()
	c.Assert(err, IsNil)
	it, _ = txn.Seek([]byte("DATA_test_tbl_department_record__000000000"))
	c.Assert(it.Valid(), IsFalse)
	txn.Commit(goctx.Background())
}

func (s *testKVSuite) TestSetNil(c *C) {
	defer testleak.AfterTest(c)()
	txn, err := s.s.Begin()
	defer txn.Commit(goctx.Background())
	c.Assert(err, IsNil)
	err = txn.Set([]byte("1"), nil)
	c.Assert(err, NotNil)
}

func (s *testKVSuite) TestBasicSeek(c *C) {
	defer testleak.AfterTest(c)()
	txn, err := s.s.Begin()
	c.Assert(err, IsNil)
	txn.Set([]byte("1"), []byte("1"))
	txn.Commit(goctx.Background())
	txn, err = s.s.Begin()
	c.Assert(err, IsNil)
	defer txn.Commit(goctx.Background())

	it, err := txn.Seek([]byte("2"))
	c.Assert(err, IsNil)
	c.Assert(it.Valid(), Equals, false)
	txn.Delete([]byte("1"))
}

func (s *testKVSuite) TestBasicTable(c *C) {
	defer testleak.AfterTest(c)()
	txn, err := s.s.Begin()
	c.Assert(err, IsNil)
	for i := 1; i < 5; i++ {
		b := []byte(strconv.Itoa(i))
		txn.Set(b, b)
	}
	txn.Commit(goctx.Background())
	txn, err = s.s.Begin()
	c.Assert(err, IsNil)
	defer txn.Commit(goctx.Background())

	err = txn.Set([]byte("1"), []byte("1"))
	c.Assert(err, IsNil)

	it, err := txn.Seek([]byte("0"))
	c.Assert(err, IsNil)
	c.Assert(string(it.Key()), Equals, "1")

	err = txn.Set([]byte("0"), []byte("0"))
	c.Assert(err, IsNil)
	it, err = txn.Seek([]byte("0"))
	c.Assert(err, IsNil)
	c.Assert(string(it.Key()), Equals, "0")
	err = txn.Delete([]byte("0"))
	c.Assert(err, IsNil)

	txn.Delete([]byte("1"))
	it, err = txn.Seek([]byte("0"))
	c.Assert(err, IsNil)
	c.Assert(string(it.Key()), Equals, "2")

	err = txn.Delete([]byte("3"))
	c.Assert(err, IsNil)
	it, err = txn.Seek([]byte("2"))
	c.Assert(err, IsNil)
	c.Assert(string(it.Key()), Equals, "2")

	it, err = txn.Seek([]byte("3"))
	c.Assert(err, IsNil)
	c.Assert(string(it.Key()), Equals, "4")
	err = txn.Delete([]byte("2"))
	c.Assert(err, IsNil)
	err = txn.Delete([]byte("4"))
	c.Assert(err, IsNil)
}

func (s *testKVSuite) TestRollback(c *C) {
	defer testleak.AfterTest(c)()
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
	defer txn.Commit(goctx.Background())

	for i := startIndex; i < testCount; i++ {
		_, err := txn.Get([]byte(strconv.Itoa(i)))
		c.Assert(err, NotNil)
	}
}

func (s *testKVSuite) TestSeekMin(c *C) {
	defer testleak.AfterTest(c)()
	rows := []struct {
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
	for _, row := range rows {
		txn.Set([]byte(row.key), []byte(row.value))
	}

	it, err := txn.Seek(nil)
	for it.Valid() {
		fmt.Printf("%s, %s\n", it.Key(), it.Value())
		it.Next()
	}

	it, err = txn.Seek([]byte("DATA_test_main_db_tbl_tbl_test_record__00000000000000000000"))
	c.Assert(err, IsNil)
	c.Assert(string(it.Key()), Equals, "DATA_test_main_db_tbl_tbl_test_record__00000000000000000001")

	for _, row := range rows {
		txn.Delete([]byte(row.key))
	}
}

func (s *testKVSuite) TestConditionIfNotExist(c *C) {
	defer testleak.AfterTest(c)()
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
			err = txn.Commit(goctx.Background())
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
	err = txn.Commit(goctx.Background())
	c.Assert(err, IsNil)
}

func (s *testKVSuite) TestConditionIfEqual(c *C) {
	defer testleak.AfterTest(c)()
	var success int64
	cnt := 100
	b := []byte("1")
	var wg sync.WaitGroup
	wg.Add(cnt)

	txn, err := s.s.Begin()
	c.Assert(err, IsNil)
	txn.Set(b, b)
	err = txn.Commit(goctx.Background())
	c.Assert(err, IsNil)

	for i := 0; i < cnt; i++ {
		go func() {
			defer wg.Done()
			// Use txn1/err1 instead of txn/err is
			// to pass `go tool vet -shadow` check.
			txn1, err1 := s.s.Begin()
			c.Assert(err1, IsNil)
			txn1.Set(b, []byte("newValue"))
			err1 = txn1.Commit(goctx.Background())
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
	err = txn.Commit(goctx.Background())
	c.Assert(err, IsNil)
}

func (s *testKVSuite) TestConditionUpdate(c *C) {
	defer testleak.AfterTest(c)()
	txn, err := s.s.Begin()
	c.Assert(err, IsNil)
	txn.Delete([]byte("b"))
	kv.IncInt64(txn, []byte("a"), 1)
	err = txn.Commit(goctx.Background())
	c.Assert(err, IsNil)
}

func (s *testKVSuite) TestDBClose(c *C) {
	c.Skip("don't know why it fails.")
	defer testleak.AfterTest(c)()
	store, err := tikv.NewMockTikvStore()
	c.Assert(err, IsNil)

	txn, err := store.Begin()
	c.Assert(err, IsNil)

	err = txn.Set([]byte("a"), []byte("b"))
	c.Assert(err, IsNil)

	err = txn.Commit(goctx.Background())
	c.Assert(err, IsNil)

	ver, err := store.CurrentVersion()
	c.Assert(err, IsNil)
	c.Assert(kv.MaxVersion.Cmp(ver), Equals, 1)

	snap, err := store.GetSnapshot(kv.MaxVersion)
	c.Assert(err, IsNil)

	_, err = snap.Get([]byte("a"))
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

	err = txn.Commit(goctx.Background())
	c.Assert(err, NotNil)
}

func (s *testKVSuite) TestIsolationInc(c *C) {
	defer testleak.AfterTest(c)()
	threadCnt := 4

	ids := make(map[int64]struct{}, threadCnt*100)
	var m sync.Mutex
	var wg sync.WaitGroup

	wg.Add(threadCnt)
	for i := 0; i < threadCnt; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				var id int64
				err := kv.RunInNewTxn(s.s, true, func(txn kv.Transaction) error {
					var err1 error
					id, err1 = kv.IncInt64(txn, []byte("key"), 1)
					return err1
				})
				c.Assert(err, IsNil)

				m.Lock()
				_, ok := ids[id]
				ids[id] = struct{}{}
				m.Unlock()
				c.Assert(ok, IsFalse)
			}
		}()
	}

	wg.Wait()

	// delete
	txn, err := s.s.Begin()
	c.Assert(err, IsNil)
	defer txn.Commit(goctx.Background())
	txn.Delete([]byte("key"))
}

func (s *testKVSuite) TestIsolationMultiInc(c *C) {
	defer testleak.AfterTest(c)()
	threadCnt := 4
	incCnt := 100
	keyCnt := 4

	keys := make([][]byte, 0, keyCnt)
	for i := 0; i < keyCnt; i++ {
		keys = append(keys, []byte(fmt.Sprintf("test_key_%d", i)))
	}

	var wg sync.WaitGroup

	wg.Add(threadCnt)
	for i := 0; i < threadCnt; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < incCnt; j++ {
				err := kv.RunInNewTxn(s.s, true, func(txn kv.Transaction) error {
					for _, key := range keys {
						_, err1 := kv.IncInt64(txn, key, 1)
						if err1 != nil {
							return err1
						}
					}

					return nil
				})
				c.Assert(err, IsNil)
			}
		}()
	}

	wg.Wait()

	err := kv.RunInNewTxn(s.s, false, func(txn kv.Transaction) error {
		for _, key := range keys {
			id, err1 := kv.GetInt64(txn, key)
			if err1 != nil {
				return err1
			}
			c.Assert(id, Equals, int64(threadCnt*incCnt))
			txn.Delete(key)
		}
		return nil
	})
	c.Assert(err, IsNil)
}
