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

package localstore

import (
	"bytes"
	"fmt"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/localstore/goleveldb"
)

var _ = Suite(&testMvccSuite{})

type testMvccSuite struct {
	s kv.Storage
}

func createMemStore(suffix int) kv.Storage {
	// avoid cache
	path := fmt.Sprintf("memory://%d", suffix)
	d := Driver{
		goleveldb.MemoryDriver{},
	}
	store, err := d.Open(path)
	if err != nil {
		panic(err)
	}
	return store
}

func (t *testMvccSuite) addDirtyData() {
	engineDB := t.s.(*dbStore).db
	b := engineDB.NewBatch()
	b.Put([]byte("\xf0dirty"), []byte("testvalue"))
	b.Put([]byte("\x00dirty"), []byte("testvalue"))
	engineDB.Commit(b)
}

func (t *testMvccSuite) TestMvccEncode(c *C) {
	encodedKey1 := MvccEncodeVersionKey([]byte("A"), kv.Version{Ver: 1})
	encodedKey2 := MvccEncodeVersionKey([]byte("A"), kv.Version{Ver: 2})
	// A_2
	// A_1
	c.Assert(encodedKey1.Cmp(encodedKey2), Greater, 0)

	// decode test
	key, ver, err := MvccDecode(encodedKey1)
	c.Assert(err, IsNil)
	c.Assert(bytes.Compare(key, []byte("A")), Equals, 0)
	c.Assert(ver.Ver, Equals, uint64(1))
}

func (t *testMvccSuite) scanRawEngine(c *C, f func([]byte, []byte)) {
	// scan raw db
	var k kv.Key
	var v []byte
	for {
		var err error
		k, v, err = t.s.(*dbStore).db.Seek(k)
		if err != nil {
			break
		}
		f(k, v)
		k = k.Next()
	}
}

func (t *testMvccSuite) SetUpTest(c *C) {
	// create new store
	t.s = createMemStore(time.Now().Nanosecond())
	t.addDirtyData()
	// insert test data
	txn, err := t.s.Begin()
	c.Assert(err, IsNil)
	for i := 0; i < 5; i++ {
		val := encodeInt(i)
		err := txn.Set(val, val)
		c.Assert(err, IsNil)
	}
	txn.Commit()
}

func (t *testMvccSuite) TestMvccGet(c *C) {
	txn, err := t.s.Begin()
	c.Assert(err, IsNil)
	k := encodeInt(1)
	_, err = txn.Get(k)
	c.Assert(err, IsNil)
	// no such key
	k = encodeInt(1024)
	_, err = txn.Get(k)
	c.Assert(err, NotNil)
	txn.Commit()
}

func (t *testMvccSuite) TestMvccPutAndDel(c *C) {
	txn, err := t.s.Begin()
	c.Assert(err, IsNil)
	// remove 0,1,2
	for i := 0; i < 3; i++ {
		val := encodeInt(i)
		err = txn.Delete(val)
		c.Assert(err, IsNil)
	}
	txn.Commit()

	txn, _ = t.s.Begin()
	_, err = txn.Get(encodeInt(0))
	c.Assert(err, NotNil)
	v, err := txn.Get(encodeInt(4))
	c.Assert(err, IsNil)
	c.Assert(len(v), Greater, 0)
	txn.Commit()

	cnt := 0
	t.scanRawEngine(c, func(k, v []byte) {
		cnt++
	})
	txn, _ = t.s.Begin()
	txn.Set(encodeInt(0), []byte("v"))
	_, err = txn.Get(encodeInt(0))
	c.Assert(err, IsNil)
	txn.Commit()

	cnt1 := 0
	t.scanRawEngine(c, func(k, v []byte) {
		cnt1++
	})
	c.Assert(cnt1, Greater, cnt)
}

func (t *testMvccSuite) TestMvccNext(c *C) {
	txn, _ := t.s.Begin()
	it, err := txn.Seek(encodeInt(2))
	c.Assert(err, IsNil)
	c.Assert(it.Valid(), IsTrue)
	for it.Valid() {
		err = it.Next()
		c.Assert(err, IsNil)
	}
	txn.Commit()
}

func encodeTestDataKey(i int) []byte {
	return encodeInt(i)
}

func (t *testMvccSuite) TestSnapshotGet(c *C) {
	tx, _ := t.s.Begin()
	b, err := tx.Get(encodeInt(1))
	c.Assert(err, IsNil)
	tx.Commit()

	lastVer, err := globalVersionProvider.CurrentVersion()
	c.Assert(err, IsNil)
	// Modify
	tx, _ = t.s.Begin()
	err = tx.Set(encodeInt(1), []byte("new"))
	c.Assert(err, IsNil)
	err = tx.Commit()
	c.Assert(err, IsNil)
	testKey := encodeTestDataKey(1)

	snapshot, err := t.s.GetSnapshot(kv.MaxVersion)
	defer snapshot.Release()
	b, err = snapshot.Get(testKey)
	c.Assert(err, IsNil)
	c.Assert(string(b), Equals, "new")

	// Get last version
	lastVerSnapshot, err := t.s.GetSnapshot(lastVer)
	c.Assert(err, IsNil)
	b, err = lastVerSnapshot.Get(testKey)
	c.Assert(err, IsNil)
	c.Assert(string(b), Equals, string(encodeInt(1)))

	// Get version not exists
	minVerSnapshot, err := t.s.GetSnapshot(kv.MinVersion)
	c.Assert(err, IsNil)
	_, err = minVerSnapshot.Get(testKey)
	c.Assert(err, NotNil)
}

func (t *testMvccSuite) getSnapshot(c *C, ver kv.Version) *dbSnapshot {
	snapshot, err := t.s.GetSnapshot(ver)
	c.Assert(err, IsNil)
	dbs, ok := snapshot.(*dbSnapshot)
	c.Assert(ok, IsTrue)
	return dbs
}

func (t *testMvccSuite) TestMvccSeek(c *C) {
	s := t.getSnapshot(c, kv.MaxVersion)
	k, v, err := s.mvccSeek(encodeInt(1), false)
	c.Assert(err, IsNil)
	c.Assert([]byte(k), BytesEquals, encodeInt(1))
	c.Assert(v, BytesEquals, encodeInt(1))

	k, v, err = s.mvccSeek(encodeInt(1024), false)
	c.Assert(err, NotNil)

	k, v, err = s.mvccSeek(append(encodeInt(1), byte(0)), false)
	c.Assert(err, IsNil)
	c.Assert([]byte(k), BytesEquals, encodeInt(2))

	k, v, err = s.mvccSeek(append(encodeInt(1), byte(0)), true)
	c.Assert(err, NotNil)

	s = t.getSnapshot(c, kv.Version{Ver: 1})
	k, v, err = s.mvccSeek(encodeInt(1), false)
	c.Assert(err, NotNil)

	txn, err := t.s.Begin()
	c.Assert(err, IsNil)
	err = txn.Set(encodeInt(3), encodeInt(1003))
	c.Assert(err, IsNil)
	err = txn.Commit()
	c.Assert(err, IsNil)
	v1, err := globalVersionProvider.CurrentVersion()
	c.Assert(err, IsNil)

	txn, err = t.s.Begin()
	c.Assert(err, IsNil)
	err = txn.Delete(encodeInt(2))
	c.Assert(err, IsNil)
	err = txn.Commit()
	c.Assert(err, IsNil)
	v2, err := globalVersionProvider.CurrentVersion()
	c.Assert(err, IsNil)

	s = t.getSnapshot(c, v2)
	k, v, err = s.mvccSeek(encodeInt(2), false)
	c.Assert(err, IsNil)
	c.Assert([]byte(k), BytesEquals, encodeInt(3))
	c.Assert(v, BytesEquals, encodeInt(1003))

	s = t.getSnapshot(c, v1)
	k, v, err = s.mvccSeek(encodeInt(2), false)
	c.Assert(err, IsNil)
	c.Assert([]byte(k), BytesEquals, encodeInt(2))
	c.Assert(v, BytesEquals, encodeInt(2))
}

func (t *testMvccSuite) TestReverseMvccSeek(c *C) {
	s := t.getSnapshot(c, kv.MaxVersion)
	k, v, err := s.reverseMvccSeek(encodeInt(1024))
	c.Assert(err, IsNil)
	c.Assert([]byte(k), BytesEquals, encodeInt(4))
	c.Assert(v, BytesEquals, encodeInt(4))

	k, v, err = s.reverseMvccSeek(encodeInt(0))
	c.Assert(err, NotNil)

	k, v, err = s.reverseMvccSeek(append(encodeInt(1), byte(0)))
	c.Assert(err, IsNil)
	c.Assert([]byte(k), BytesEquals, encodeInt(1))

	s = t.getSnapshot(c, kv.Version{Ver: 1})
	k, v, err = s.reverseMvccSeek(encodeInt(1024))
	c.Assert(err, NotNil)

	v0, err := globalVersionProvider.CurrentVersion()
	c.Assert(err, IsNil)

	txn, err := t.s.Begin()
	c.Assert(err, IsNil)
	err = txn.Set(encodeInt(3), encodeInt(1003))
	c.Assert(err, IsNil)
	err = txn.Commit()
	c.Assert(err, IsNil)
	v1, err := globalVersionProvider.CurrentVersion()
	c.Assert(err, IsNil)

	txn, err = t.s.Begin()
	c.Assert(err, IsNil)
	err = txn.Delete(encodeInt(4))
	c.Assert(err, IsNil)
	err = txn.Commit()
	c.Assert(err, IsNil)
	v2, err := globalVersionProvider.CurrentVersion()
	c.Assert(err, IsNil)

	s = t.getSnapshot(c, v2)
	k, v, err = s.reverseMvccSeek(encodeInt(5))
	c.Assert(err, IsNil)
	c.Assert([]byte(k), BytesEquals, encodeInt(3))
	c.Assert(v, BytesEquals, encodeInt(1003))

	s = t.getSnapshot(c, v1)
	k, v, err = s.reverseMvccSeek(encodeInt(5))
	c.Assert(err, IsNil)
	c.Assert([]byte(k), BytesEquals, encodeInt(4))
	c.Assert(v, BytesEquals, encodeInt(4))

	s = t.getSnapshot(c, v0)
	k, v, err = s.reverseMvccSeek(encodeInt(4))
	c.Assert(err, IsNil)
	c.Assert([]byte(k), BytesEquals, encodeInt(3))
	c.Assert(v, BytesEquals, encodeInt(3))
}

func (t *testMvccSuite) TestMvccSuiteGetLatest(c *C) {
	// update some new data
	for i := 0; i < 10; i++ {
		tx, _ := t.s.Begin()
		err := tx.Set(encodeInt(5), encodeInt(100+i))
		c.Assert(err, IsNil)
		err = tx.Commit()
		c.Assert(err, IsNil)
	}
	// we can always read newest data
	tx, _ := t.s.Begin()
	b, err := tx.Get(encodeInt(5))
	c.Assert(err, IsNil)
	c.Assert(string(b), Equals, string(encodeInt(100+9)))
	// we can always scan newest data
	it, err := tx.Seek(encodeInt(5))
	c.Assert(err, IsNil)
	c.Assert(it.Valid(), IsTrue)
	c.Assert(string(it.Value()), Equals, string(encodeInt(100+9)))
	tx.Commit()

	testKey := []byte("testKey")
	txn0, _ := t.s.Begin()
	txn0.Set(testKey, []byte("0"))
	txn0.Commit()
	txn1, _ := t.s.Begin()
	{
		// Commit another version
		txn2, _ := t.s.Begin()
		txn2.Set(testKey, []byte("2"))
		txn2.Commit()
	}
	r, err := txn1.Get(testKey)
	c.Assert(err, IsNil)
	// Test isolation in transaction.
	c.Assert(string(r), Equals, "0")
	txn1.Commit()
}

func (t *testMvccSuite) TestBufferedIterator(c *C) {
	s := createMemStore(time.Now().Nanosecond())
	tx, _ := s.Begin()
	tx.Set([]byte{0x0, 0x0}, []byte("1"))
	tx.Set([]byte{0x0, 0xff}, []byte("2"))
	tx.Set([]byte{0x0, 0xee}, []byte("3"))
	tx.Set([]byte{0x0, 0xee, 0xff}, []byte("4"))
	tx.Set([]byte{0xff, 0xff, 0xee, 0xff}, []byte("5"))
	tx.Set([]byte{0xff, 0xff, 0xff}, []byte("6"))
	tx.Commit()

	tx, _ = s.Begin()
	iter, err := tx.Seek([]byte{0})
	c.Assert(err, IsNil)
	cnt := 0
	for iter.Valid() {
		err = iter.Next()
		c.Assert(err, IsNil)
		cnt++
	}
	tx.Commit()
	c.Assert(cnt, Equals, 6)

	tx, _ = s.Begin()
	it, err := tx.Seek([]byte{0xff, 0xee})
	c.Assert(err, IsNil)
	c.Assert(it.Valid(), IsTrue)
	c.Assert(string(it.Key()), Equals, "\xff\xff\xee\xff")
	tx.Commit()

	// no such key
	tx, _ = s.Begin()
	it, err = tx.Seek([]byte{0xff, 0xff, 0xff, 0xff})
	c.Assert(err, IsNil)
	c.Assert(it.Valid(), IsFalse)

	it, err = tx.Seek([]byte{0x0, 0xff})
	c.Assert(err, IsNil)
	c.Assert(it.Valid(), IsTrue)
	c.Assert(it.Value(), DeepEquals, []byte("2"))
	tx.Commit()

	tx, _ = s.Begin()
	iter, err = tx.SeekReverse(nil)
	c.Assert(err, IsNil)
	cnt = 0
	for iter.Valid() {
		err = iter.Next()
		c.Assert(err, IsNil)
		cnt++
	}
	tx.Commit()
	c.Assert(cnt, Equals, 6)

	tx, _ = s.Begin()
	it, err = tx.SeekReverse([]byte{0xff, 0xff, 0xff})
	c.Assert(err, IsNil)
	c.Assert(it.Valid(), IsTrue)
	c.Assert(string(it.Key()), Equals, "\xff\xff\xee\xff")
	tx.Commit()

	// no such key
	tx, _ = s.Begin()
	it, err = tx.SeekReverse([]byte{0x0, 0x0})
	c.Assert(err, IsNil)
	c.Assert(it.Valid(), IsFalse)

	it, err = tx.SeekReverse([]byte{0x0, 0xee})
	c.Assert(err, IsNil)
	c.Assert(it.Valid(), IsTrue)
	c.Assert(it.Value(), DeepEquals, []byte("1"))
	tx.Commit()
}

func encodeInt(n int) []byte {
	return []byte(fmt.Sprintf("%010d", n))
}

func decodeInt(s []byte) int {
	var n int
	fmt.Sscanf(string(s), "%010d", &n)
	return n
}
