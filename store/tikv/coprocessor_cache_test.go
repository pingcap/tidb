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
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
)

type testCoprocessorCacheSuite struct {
	OneByOneSuite
}

var _ = Suite(&testCoprocessorCacheSuite{})

func (s *testCoprocessorSuite) TestBuildCacheKey(c *C) {
	req := coprocessor.Request{
		Tp:      0xAB,
		StartTs: 0xAABBCC,
		Data:    []uint8{0x18, 0x0, 0x20, 0x0, 0x40, 0x0, 0x5a, 0x0},
		Ranges: []*coprocessor.KeyRange{
			{
				Start: kv.Key{0x01},
				End:   kv.Key{0x01, 0x02},
			},
			{
				Start: kv.Key{0x01, 0x01, 0x02},
				End:   kv.Key{0x01, 0x01, 0x03},
			},
		},
	}

	key, err := coprCacheBuildKey(&req)
	c.Assert(err, IsNil)
	expectKey := ""
	expectKey += "\xab"                             // 1 byte Tp
	expectKey += "\x08\x00\x00\x00"                 // 4 bytes Data len
	expectKey += "\x18\x00\x20\x00\x40\x00\x5a\x00" // Data
	expectKey += "\x01\x00"                         // 2 bytes StartKey len
	expectKey += "\x01"                             // StartKey
	expectKey += "\x02\x00"                         // 2 bytes EndKey len
	expectKey += "\x01\x02"                         // EndKey
	expectKey += "\x03\x00"                         // 2 bytes StartKey len
	expectKey += "\x01\x01\x02"                     // StartKey
	expectKey += "\x03\x00"                         // 2 bytes EndKey len
	expectKey += "\x01\x01\x03"                     // EndKey
	c.Assert(key, DeepEquals, []byte(expectKey))

	req = coprocessor.Request{
		Tp:      0xABCC, // Tp too big
		StartTs: 0xAABBCC,
		Data:    []uint8{0x18},
		Ranges:  []*coprocessor.KeyRange{},
	}

	_, err = coprCacheBuildKey(&req)
	c.Assert(err, NotNil)
}

func (s *testCoprocessorSuite) TestDisable(c *C) {
	cache, err := newCoprCache(&config.CoprocessorCache{Enable: false})
	c.Assert(err, IsNil)
	c.Assert(cache, IsNil)

	v := cache.Set([]byte("foo"), &coprCacheValue{})
	c.Assert(v, Equals, false)

	v2 := cache.Get([]byte("foo"))
	c.Assert(v2, IsNil)

	v = cache.CheckAdmission(1024, time.Second*5)
	c.Assert(v, Equals, false)

	cache, err = newCoprCache(&config.CoprocessorCache{Enable: true, CapacityMB: 0, AdmissionMaxResultMB: 1})
	c.Assert(err, NotNil)
	c.Assert(cache, IsNil)

	cache, err = newCoprCache(&config.CoprocessorCache{Enable: true, CapacityMB: 0.001})
	c.Assert(err, NotNil)
	c.Assert(cache, IsNil)

	cache, err = newCoprCache(&config.CoprocessorCache{Enable: true, CapacityMB: 0.001, AdmissionMaxResultMB: 1})
	c.Assert(err, IsNil)
	c.Assert(cache, NotNil)
}

func (s *testCoprocessorSuite) TestAdmission(c *C) {
	cache, err := newCoprCache(&config.CoprocessorCache{Enable: true, AdmissionMinProcessMs: 5, AdmissionMaxResultMB: 1, CapacityMB: 1})
	c.Assert(err, IsNil)
	c.Assert(cache, NotNil)

	v := cache.CheckAdmission(0, 0)
	c.Assert(v, Equals, false)

	v = cache.CheckAdmission(0, 4*time.Millisecond)
	c.Assert(v, Equals, false)

	v = cache.CheckAdmission(0, 5*time.Millisecond)
	c.Assert(v, Equals, false)

	v = cache.CheckAdmission(1, 0)
	c.Assert(v, Equals, false)

	v = cache.CheckAdmission(1, 4*time.Millisecond)
	c.Assert(v, Equals, false)

	v = cache.CheckAdmission(1, 5*time.Millisecond)
	c.Assert(v, Equals, true)

	v = cache.CheckAdmission(1024, 5*time.Millisecond)
	c.Assert(v, Equals, true)

	v = cache.CheckAdmission(1024*1024, 5*time.Millisecond)
	c.Assert(v, Equals, true)

	v = cache.CheckAdmission(1024*1024+1, 5*time.Millisecond)
	c.Assert(v, Equals, false)

	v = cache.CheckAdmission(1024*1024+1, 4*time.Millisecond)
	c.Assert(v, Equals, false)
}

func (s *testCoprocessorSuite) TestCacheValueLen(c *C) {
	v := coprCacheValue{
		TimeStamp:         0x123,
		RegionID:          0x1,
		RegionDataVersion: 0x3,
	}
	// 72 = (8 byte pointer + 8 byte for length + 8 byte for cap) * 2 + 8 byte * 3
	c.Assert(v.Len(), Equals, 72)

	v = coprCacheValue{
		Key:               []byte("foobar"),
		Data:              []byte("12345678"),
		TimeStamp:         0x123,
		RegionID:          0x1,
		RegionDataVersion: 0x3,
	}
	c.Assert(v.Len(), Equals, 72+6+8)
}

func (s *testCoprocessorSuite) TestGetSet(c *C) {
	cache, err := newCoprCache(&config.CoprocessorCache{Enable: true, AdmissionMinProcessMs: 5, AdmissionMaxResultMB: 1, CapacityMB: 1})
	c.Assert(err, IsNil)
	c.Assert(cache, NotNil)

	v := cache.Get([]byte("foo"))
	c.Assert(v, IsNil)

	v2 := cache.Set([]byte("foo"), &coprCacheValue{
		Data:              []byte("bar"),
		TimeStamp:         0x123,
		RegionID:          0x1,
		RegionDataVersion: 0x3,
	})
	c.Assert(v2, Equals, true)

	// See https://github.com/dgraph-io/ristretto/blob/83508260cb49a2c3261c2774c991870fd18b5a1b/cache_test.go#L13
	// Changed from 10ms to 50ms to resist from unstable CI environment.
	time.Sleep(time.Millisecond * 50)

	v = cache.Get([]byte("foo"))
	c.Assert(v, NotNil)
	c.Assert(v.Data, DeepEquals, []byte("bar"))
}
