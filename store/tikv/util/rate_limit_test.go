// Copyright 2021 PingCAP, Inc.
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

package util

import (
	"time"

	. "github.com/pingcap/check"
)

func (s *testMiscSuite) TestRateLimit(c *C) {
	done := make(chan struct{}, 1)
	rl := NewRateLimit(1)
	c.Assert(rl.PutToken, PanicMatches, "put a redundant token")
	exit := rl.GetToken(done)
	c.Assert(exit, Equals, false)
	rl.PutToken()
	c.Assert(rl.PutToken, PanicMatches, "put a redundant token")

	exit = rl.GetToken(done)
	c.Assert(exit, Equals, false)
	done <- struct{}{}
	exit = rl.GetToken(done) // blocked but exit
	c.Assert(exit, Equals, true)

	sig := make(chan int, 1)
	go func() {
		exit = rl.GetToken(done) // blocked
		c.Assert(exit, Equals, false)
		close(sig)
	}()
	time.Sleep(200 * time.Millisecond)
	rl.PutToken()
	<-sig
}
