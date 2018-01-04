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

package server

import (
	"math/rand"
	"time"

	. "github.com/pingcap/check"
)

var _ = Suite(&testUtilSuite{})

type testUtilSuite struct{}

func (s *testUtilSuite) TestParseTimestap(c *C) {
	for i := 0; i < 3; i++ {
		t := time.Now().Add(time.Second * time.Duration(rand.Int31n(1000)))
		data := uint64ToBytes(uint64(t.UnixNano()))
		nt, err := parseTimestamp(data)
		c.Assert(err, IsNil)
		c.Assert(nt.Equal(t), IsTrue)
	}
	data := []byte("pd")
	nt, err := parseTimestamp(data)
	c.Assert(err, NotNil)
	c.Assert(nt.Equal(zeroTime), IsTrue)
}

func (s *testUtilSuite) TestSubTimeByWallClock(c *C) {
	for i := 0; i < 3; i++ {
		r := rand.Int31n(1000)
		t1 := time.Now()
		t2 := t1.Add(time.Second * time.Duration(r))
		duration := subTimeByWallClock(t2, t1)
		c.Assert(duration, Equals, time.Second*time.Duration(r))
	}
}
