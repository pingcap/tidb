// Copyright 2016 PingCAP, Inc.
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

package tikv

import (
	"fmt"
	"time"

	"github.com/ngaut/log"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/kv"
)

type testSnapshotSuite struct {
	store   *tikvStore
	prefix  string
	rowNums []int
}

var _ = Suite(&testSnapshotSuite{})

func (s *testSnapshotSuite) SetUpSuite(c *C) {
	s.store = newTestStore(c)
	s.prefix = fmt.Sprintf("snapshot_%d", time.Now().Unix())
	s.rowNums = append(s.rowNums, 1, 100, 191)
}

func (s *testSnapshotSuite) TearDownSuite(c *C) {
	txn := s.beginTxn(c)
	scanner, err := txn.Seek(encodeKey(s.prefix, ""))
	c.Assert(err, IsNil)
	c.Assert(scanner, NotNil)
	for scanner.Valid() {
		k := scanner.Key()
		err = txn.Delete(k)
		c.Assert(err, IsNil)
		scanner.Next()
	}
	err = txn.Commit()
	c.Assert(err, IsNil)
	err = s.store.Close()
	c.Assert(err, IsNil)
}

func (s *testSnapshotSuite) beginTxn(c *C) *tikvTxn {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	return txn.(*tikvTxn)
}

func (s *testSnapshotSuite) checkAll(keys []kv.Key, c *C) {
	txn := s.beginTxn(c)
	snapshot := newTiKVSnapshot(s.store, kv.Version{Ver: txn.StartTS()})
	m, err := snapshot.BatchGet(keys)
	c.Assert(err, IsNil)

	scan, err := txn.Seek(encodeKey(s.prefix, ""))
	c.Assert(err, IsNil)
	cnt := 0
	for scan.Valid() {
		cnt++
		k := scan.Key()
		v := scan.Value()
		v2, ok := m[string(k)]
		c.Assert(ok, IsTrue, Commentf("key: %q", k))
		c.Assert(v, BytesEquals, v2)
		scan.Next()
	}
	err = txn.Commit()
	c.Assert(err, IsNil)
	c.Assert(m, HasLen, cnt)
}

func (s *testSnapshotSuite) deleteKeys(keys []kv.Key, c *C) {
	txn := s.beginTxn(c)
	for _, k := range keys {
		err := txn.Delete(k)
		c.Assert(err, IsNil)
	}
	err := txn.Commit()
	c.Assert(err, IsNil)
}

func (s *testSnapshotSuite) TestBatchGet(c *C) {
	for _, rowNum := range s.rowNums {
		log.Debugf("Test BatchGet with length[%d]", rowNum)
		txn := s.beginTxn(c)
		for i := 0; i < rowNum; i++ {
			k := encodeKey(s.prefix, s08d("key", i))
			err := txn.Set(k, valueBytes(i))
			c.Assert(err, IsNil)
		}
		err := txn.Commit()
		c.Assert(err, IsNil)

		keys := makeKeys(rowNum, s.prefix)
		s.checkAll(keys, c)
		s.deleteKeys(keys, c)
	}
}

func (s *testSnapshotSuite) TestBatchGetLock(c *C) {
	for _, rowNum := range s.rowNums {
		log.Debugf("Test BatchGetLock with length[%d]", rowNum)
		txn := s.beginTxn(c)
		for i := 0; i < rowNum; i++ {
			k := encodeKey(s.prefix, s08d("key", i))
			err := txn.Set(k, valueBytes(i))
			c.Assert(err, IsNil)
		}
		err := txn.Commit()
		c.Assert(err, IsNil)

		txn2 := s.beginTxn(c)
		txn2.DONOTCOMMIT = true
		lockKey := encodeKey(s.prefix, s08d("key", rowNum/2))
		err = txn2.Set(lockKey, []byte("lock"))
		c.Assert(err, IsNil)
		err = txn2.Commit()
		c.Assert(err, IsNil)

		keys := makeKeys(rowNum, s.prefix)
		txn3 := s.beginTxn(c)
		snapshot := newTiKVSnapshot(s.store, kv.Version{Ver: txn3.StartTS()})
		_, err = snapshot.BatchGet(keys)
		c.Assert(err, IsNil)

		s.checkAll(keys, c)
		s.deleteKeys(keys, c)
	}
}

func (s *testSnapshotSuite) TestBatchGetNotExist(c *C) {
	for _, rowNum := range s.rowNums {
		log.Debugf("Test BatchGetNotExist with length[%d]", rowNum)
		txn := s.beginTxn(c)
		for i := 0; i < rowNum; i++ {
			k := encodeKey(s.prefix, s08d("key", i))
			err := txn.Set(k, valueBytes(i))
			c.Assert(err, IsNil)
		}
		err := txn.Commit()
		c.Assert(err, IsNil)

		keys := makeKeys(rowNum, s.prefix)
		keys = append(keys, kv.Key("noSuchKey"))
		s.checkAll(keys, c)
		s.deleteKeys(keys, c)
	}
}

func (s *testSnapshotSuite) TestMergeResult(c *C) {
	d1 := makeDict([]string{"1", "2"})
	d2 := makeDict([]string{"a", "foo"})
	d1, err := mergeResult(d1, d2)
	c.Assert(err, IsNil)
	r1 := makeDict([]string{"a", "foo", "1", "2"})
	equalByteDict(c, d1, r1)
}

func (s *testSnapshotSuite) TestMergeResultNil(c *C) {
	var d1 map[string][]byte
	d2 := makeDict([]string{"a", "foo"})
	d1, err := mergeResult(d1, d2)
	c.Assert(err, IsNil)
	r1 := makeDict([]string{"a", "foo"})
	equalByteDict(c, d1, r1)

	var d3 map[string][]byte
	var d4 map[string][]byte
	d3, err = mergeResult(d3, d4)
	c.Assert(err, IsNil)
	var r2 map[string][]byte
	equalByteDict(c, d3, r2)
}

func (s *testSnapshotSuite) TestMergeResultConflict(c *C) {
	d1 := makeDict([]string{"1", "2"})
	d2 := makeDict([]string{"a", "foo", "1"})
	_, err := mergeResult(d1, d2)
	c.Assert(err, NotNil)
}

func makeDict(keys []string) map[string][]byte {
	d := make(map[string][]byte)
	for _, k := range keys {
		d[k] = []byte(k)
	}
	return d
}

func equalByteDict(c *C, lhs, rhs map[string][]byte) {
	c.Assert(lhs, HasLen, len(rhs))
	for k, v1 := range lhs {
		v2, ok := rhs[k]
		c.Assert(ok, IsTrue)
		c.Assert(v1, BytesEquals, v2)
	}
}

func makeKeys(rowNum int, prefix string) []kv.Key {
	keys := make([]kv.Key, 0, rowNum)
	for i := 0; i < rowNum; i++ {
		k := encodeKey(prefix, s08d("key", i))
		keys = append(keys, k)
	}
	return keys
}
