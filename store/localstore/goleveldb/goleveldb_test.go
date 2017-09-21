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

package goleveldb

import (
	"testing"

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

func (s *testSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
}

func (s *testSuite) TearDownSuite(c *C) {
	testleak.AfterTest(c)()
}

func (s *testSuite) SetUpTest(c *C) {
	var (
		d   MemoryDriver
		err error
	)
	s.db, err = d.Open("memory")
	c.Assert(err, IsNil)
}

func (s *testSuite) TearDownTest(c *C) {
	s.db.Close()
}

func (s *testSuite) TestGetSet(c *C) {
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

func (s *testSuite) TestSeek(c *C) {
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
	c.Assert(err, NotNil)
	c.Assert(string(k), Equals, "")
	c.Assert(string(v), Equals, "")

	k, v, err = s.db.SeekReverse([]byte("a"))
	c.Assert(err, NotNil)
	c.Assert(k, IsNil)
	c.Assert(v, IsNil)
}
