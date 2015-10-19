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

package memkv

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/types"
)

var _ = Suite(&testBtreeSuite{})

type testBtreeSuite struct {
}

func (*testBtreeSuite) TestBtree(c *C) {
	t := NewTree(types.Collators[true])
	c.Assert(t, NotNil)
	// Insert
	for i := 0; i < 512; i++ {
		k := []interface{}{i}
		v := []interface{}{string(i + 1)}
		t.Set(k, v)
	}
	for i := 0; i < 1024; i++ {
		k := []interface{}{i}
		v := []interface{}{string(i)}
		t.Set(k, v)
	}
	// Delete
	for i := 512; i < 1024; i++ {
		k := []interface{}{i}
		t.Delete(k)
	}
	// Get
	for i := 0; i < 512; i++ {
		k := []interface{}{i}
		v, ok := t.Get(k)
		c.Assert(ok, IsTrue)
		c.Assert(v, HasLen, 1)
		c.Assert(v[0], Equals, string(i))
	}
	// Get unexists key
	for i := 512; i < 1024; i++ {
		k := []interface{}{i}
		v, ok := t.Get(k)
		c.Assert(ok, IsFalse)
		c.Assert(v, IsNil)
	}
	// First
	k, v := t.First()
	c.Assert(k, NotNil)
	c.Assert(v, NotNil)
	c.Assert(k[0], Equals, 0)

	// Seek
	for i := 0; i < 512; i++ {
		k := []interface{}{i}
		e, ok := t.Seek(k)
		c.Assert(ok, IsTrue)
		c.Assert(e, NotNil)
		c.Assert(e.k[0], Equals, i)
		c.Assert(e.q.d[e.i].v[0], Equals, string(i))

		pk, pv, err := e.Prev()
		c.Assert(err, IsNil)
		c.Assert(pk, NotNil)
		c.Assert(pv, NotNil)
		c.Assert(pk[0], Equals, i)
		c.Assert(pv[0], Equals, string(i))
		pk, pv, err = e.Prev()
		if i > 0 {
			c.Assert(err, IsNil)
			c.Assert(pk, NotNil)
			c.Assert(pv, NotNil)
			c.Assert(pk[0], Equals, i-1)
			c.Assert(pv[0], Equals, string(i-1))
		}
	}

	t.Clear()
	e, err := t.SeekLast()
	c.Assert(e, IsNil)
	c.Assert(err, NotNil)
}
