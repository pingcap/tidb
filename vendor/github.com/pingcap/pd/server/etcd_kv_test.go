// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import . "github.com/pingcap/check"

type testEtcdKVSuite struct{}

var _ = Suite(&testEtcdKVSuite{})

func (s *testEtcdKVSuite) TestEtcdKV(c *C) {
	server, cleanup := mustRunTestServer(c)
	defer cleanup()
	kv := server.kv.KVBase

	keys := []string{"test/key1", "test/key2", "test/key3", "test/key4", "test/key5"}
	vals := []string{"val1", "val2", "val3", "val4", "val5"}

	v, err := kv.Load(keys[0])
	c.Assert(err, IsNil)
	c.Assert(v, Equals, "")

	for i := range keys {
		err = kv.Save(keys[i], vals[i])
		c.Assert(err, IsNil)
	}
	for i := range keys {
		v, err = kv.Load(keys[i])
		c.Assert(err, IsNil)
		c.Assert(v, Equals, vals[i])
	}
	values, err := kv.LoadRange(keys[0], "test/zzz", 100)
	c.Assert(err, IsNil)
	c.Assert(values, DeepEquals, vals)
	values, err = kv.LoadRange(keys[0], "test/zzz", 3)
	c.Assert(err, IsNil)
	c.Assert(values, DeepEquals, vals[:3])
	values, err = kv.LoadRange(keys[0], keys[3], 100)
	c.Assert(err, IsNil)
	c.Assert(values, DeepEquals, vals[:3])
}
