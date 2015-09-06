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
	"testing"

	. "github.com/pingcap/check"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testTempSuite{})

type testTempSuite struct {
}

type InvalidMySQLType struct {
}

func (*testTempSuite) TestTemp(c *C) {
	// Create memkv
	kv, err := CreateTemp(true)
	c.Assert(err, IsNil)

	// Seek in an empty kv
	iter, err := kv.SeekFirst()
	c.Assert(err, NotNil)

	// Set can't accept invalid type
	k := []interface{}{1}
	v := []interface{}{InvalidMySQLType{}}
	err = kv.Set(k, v)
	c.Assert(err, NotNil)

	// Test Set
	for i := 0; i < 10; i++ {
		k = []interface{}{i}
		v = []interface{}{string(i)}
		err = kv.Set(k, v)
		c.Assert(err, IsNil)
	}
	// Test Get
	for i := 0; i < 10; i++ {
		k = []interface{}{i}
		expectV := []interface{}{string(i)}
		v, err = kv.Get(k)
		c.Assert(err, IsNil)
		c.Assert(v, HasLen, 1)
		c.Assert(v[0], Equals, expectV[0])
	}
	// Test iterator
	iter, err = kv.SeekFirst()
	c.Assert(err, IsNil)
	for i := 0; i < 10; i++ {
		expectK := []interface{}{i}
		expectV := []interface{}{string(i)}
		k, v, err = iter.Next()
		c.Assert(err, IsNil)
		c.Assert(k, HasLen, 1)
		c.Assert(v, HasLen, 1)
		c.Assert(k[0], Equals, expectK[0])
		c.Assert(v[0], Equals, expectV[0])
	}

	err = kv.Drop()
	c.Assert(err, IsNil)
}
