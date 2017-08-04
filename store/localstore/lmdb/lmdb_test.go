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

package lmdb

import (
	"fmt"
	"os"
	"testing"

	"github.com/bmatsuo/lmdb-go/lmdb"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/store/localstore/engine"
	"github.com/pingcap/tidb/util/testleak"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testSuite{})

type testSuite struct {
	db engine.DB
}

const testPath = "/tmp/test-tidb-lmdb"

func (s *testSuite) SetUpTest(c *C) {
	var (
		d   Driver
		err error
	)
	s.db, err = d.Open(testPath)
	c.Assert(err, IsNil)
}

func (s *testSuite) TearDownTest(c *C) {
	s.db.Close()
	os.RemoveAll(testPath)
}

func (s *testSuite) TestGetSet(c *C) {
	defer testleak.AfterTest(c)()
	db := s.db

	b := db.NewBatch()
	b.Put([]byte("a"), []byte("1"))
	b.Put([]byte("b"), []byte("2"))
	b.Delete([]byte("c"))

	err := db.Commit(b)
	c.Assert(err, IsNil)

	v, err := db.Get([]byte("a"))
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, []byte("1"))

	v, err = db.Get([]byte("c"))
	c.Assert(err, NotNil)
	c.Assert(v, IsNil)

	b = db.NewBatch()
	b.Put([]byte("a"), []byte("2"))
	err = db.Commit(b)
	c.Assert(err, IsNil)

	v, err = db.Get([]byte("a"))
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, []byte("2"))
}

func (s *testSuite) TestPutNilAndDelete(c *C) {
	defer testleak.AfterTest(c)()
	d := s.db
	rawDB := d.(*db)
	b := s.db.NewBatch()
	b.Put([]byte("aa"), nil)
	err := d.Commit(b)
	c.Assert(err, IsNil)

	v, err := d.Get([]byte("aa"))
	c.Assert(err, IsNil)
	c.Assert(len(v), Equals, 0)

	found := false
	rawDB.env.View(func(txn *lmdb.Txn) error {
		txn.RawRead = true
		cur, err := txn.OpenCursor(rawDB.dbi)
		if err != nil {
			return nil
		}
		for {
			k, _, err := cur.Get(nil, nil, lmdb.Next)
			if lmdb.IsNotFound(err) {
				return nil
			}
			if err != nil {
				return err
			}
			if string(k) == "aa" {
				found = true
			}
		}
	})
	c.Assert(found, Equals, true)

	// real delete
	b = s.db.NewBatch()
	b.Delete([]byte("aa"))
	err = d.Commit(b)
	c.Assert(err, IsNil)

	found = false
	rawDB.env.View(func(txn *lmdb.Txn) error {
		txn.RawRead = true
		cur, err := txn.OpenCursor(rawDB.dbi)
		if err != nil {
			return nil
		}
		for {
			k, _, err := cur.Get(nil, nil, lmdb.Next)
			if lmdb.IsNotFound(err) {
				return nil
			}
			if err != nil {
				return err
			}
			if string(k) == "aa" {
				found = true
			}
		}
	})
	c.Assert(found, Equals, false)
}

func (s *testSuite) TestSeek(c *C) {
	defer testleak.AfterTest(c)()
	b := s.db.NewBatch()
	b.Put([]byte("a"), []byte("1"))
	b.Put([]byte("b"), []byte("2"))
	err := s.db.Commit(b)
	c.Assert(err, IsNil)

	k, v, err := s.db.Seek(nil)
	c.Assert(err, IsNil)
	c.Assert(k, BytesEquals, []byte("a"))
	c.Assert(v, BytesEquals, []byte("1"))

	k, v, err = s.db.Seek([]byte("a"))
	c.Assert(err, IsNil)
	c.Assert(k, BytesEquals, []byte("a"))
	c.Assert(v, BytesEquals, []byte("1"))

	k, v, err = s.db.Seek([]byte("b"))
	c.Assert(err, IsNil)
	c.Assert(k, BytesEquals, []byte("b"))
	c.Assert(v, BytesEquals, []byte("2"))

	k, v, err = s.db.Seek([]byte("a1"))
	c.Assert(err, IsNil)
	c.Assert(k, BytesEquals, []byte("b"))
	c.Assert(v, BytesEquals, []byte("2"))

	k, v, err = s.db.Seek([]byte("c1"))
	c.Assert(err, NotNil)
	c.Assert(k, IsNil)
	c.Assert(v, IsNil)
}

func (s *testSuite) TestPrevSeek(c *C) {
	defer testleak.AfterTest(c)()
	b := s.db.NewBatch()
	b.Put([]byte("b"), []byte("1"))
	b.Put([]byte("c"), []byte("2"))
	err := s.db.Commit(b)
	c.Assert(err, IsNil)

	k, v, err := s.db.SeekReverse(nil)
	c.Assert(err, IsNil)
	c.Assert(k, BytesEquals, []byte("c"))
	c.Assert(v, BytesEquals, []byte("2"))

	k, v, err = s.db.SeekReverse([]byte("d"))
	c.Assert(err, IsNil)
	c.Assert(k, BytesEquals, []byte("c"))
	c.Assert(v, BytesEquals, []byte("2"))

	k, v, err = s.db.SeekReverse([]byte("c"))
	c.Assert(err, IsNil)
	c.Assert(k, BytesEquals, []byte("b"))
	c.Assert(v, BytesEquals, []byte("1"))

	k, v, err = s.db.SeekReverse([]byte("bb"))
	c.Assert(err, IsNil)
	c.Assert(k, BytesEquals, []byte("b"))
	c.Assert(v, BytesEquals, []byte("1"))

	k, v, err = s.db.SeekReverse([]byte("b"))
	c.Assert(string(k), Equals, "")
	c.Assert(string(v), Equals, "")
	c.Assert(err, NotNil)

	k, v, err = s.db.SeekReverse([]byte("a"))
	c.Assert(err, NotNil)
	c.Assert(k, IsNil)
	c.Assert(v, IsNil)
}

func (s *testSuite) TestBytesClone(c *C) {
	b := []byte("hello world")
	shadowB := b
	newB := cloneBytes(b)
	c.Assert(b, DeepEquals, newB)
	// Ugly hacks.Go doesn't allow compare two slice (except nil).
	// For example:  b == newB <-- it's invalid.
	// In this test, we must ensure CloneBytes method returns a new slice with
	// the same value, so we need to check the new slice's address.
	c.Assert(fmt.Sprintf("%p", b) != fmt.Sprintf("%p", newB), IsTrue)
	// But the addresses are the same when it's a shadow copy.
	c.Assert(fmt.Sprintf("%p", b), Equals, fmt.Sprintf("%p", shadowB))
}
