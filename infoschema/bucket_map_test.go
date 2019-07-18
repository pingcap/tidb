// Copyright 2019 PingCAP, Inc.
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

package infoschema

import (
	"fmt"

	. "github.com/pingcap/check"
)

var _ = Suite(&testBucketMapSuite{})

type testBucketMapSuite struct{}

func (s *testBucketMapSuite) Test1(c *C) {
	bm := NewBucketMap()

	v, ok := bm.Get("K1")
	c.Assert(ok, Equals, false)

	bm.Set("K1", "V1")

	v, ok = bm.Get("K1")
	c.Assert(ok, Equals, true)
	c.Assert(v.(string), Equals, "V1")

	bm.Set("K1", "V2")

	v, ok = bm.Get("K1")
	c.Assert(ok, Equals, true)
	c.Assert(v.(string), Equals, "V2")

	bm.Delete("K1")
	bm.Delete("K2")
	v, ok = bm.Get("K1")
	c.Assert(ok, Equals, false)
}

func (s *testBucketMapSuite) TestSpawn(c *C) {
	bm := NewBucketMap()
	bm.Set("K1", "V1")

	bm1 := bm.Spawn()
	bm1.Set("K1", "V2")

	v, ok := bm.Get("K1")
	c.Assert(ok, Equals, true)
	c.Assert(v.(string), Equals, "V1")

	v, ok = bm1.Get("K1")
	c.Assert(ok, Equals, true)
	c.Assert(v.(string), Equals, "V2")

	bm1.Delete("K1")
	v, ok = bm1.Get("K1")
	c.Assert(ok, Equals, false)

	bm.Set("K0", "V0")
	bm.Delete("K0")
	v, ok = bm.Get("K0")
	c.Assert(ok, Equals, false)

	v, ok = bm.Get("K1")
	c.Assert(ok, Equals, true)
	c.Assert(v.(string), Equals, "V1")

	bm1.Set("K2", "V2")
	v, ok = bm.Get("K2")
	c.Assert(ok, Equals, false)

	bm1.Set("K3", "V3")
	bm1.Delete("K2")
	v, ok = bm1.Get("K3")
	c.Assert(ok, Equals, true)
	c.Assert(v, Equals, "V3")

	v, ok = bm1.Get("K2")
	c.Assert(ok, Equals, false)
}

func (s *testBucketMapSuite) TestConcurrent(c *C) {
	bm := NewBucketMap()

	ch := make(chan int, 2)

	go func() {
		for i := 0; i < 5; i++ {
			k1 := fmt.Sprintf("K%d", i)
			v1 := fmt.Sprintf("V%d", i)
			bm.Set(k1, v1)
		}
		ch <- 1
	}()

	go func() {
		for i := 5; i < 10; i++ {
			k1 := fmt.Sprintf("K%d", i)
			v1 := fmt.Sprintf("V%d", i)
			bm.Set(k1, v1)
		}
		ch <- 1
	}()

	<-ch
	<-ch

	for i := 0; i < 10; i++ {
		k1 := fmt.Sprintf("K%d", i)
		_, ok := bm.Get(k1)
		c.Assert(ok, Equals, true)
	}
}
