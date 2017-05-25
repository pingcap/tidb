// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.
//
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
	"sync"
	"time"

	. "github.com/pingcap/check"
	"google.golang.org/grpc"
)

type testPoolSuite struct {
}

var _ = Suite(&testPoolSuite{})

func (s *testPoolSuite) TestPool(c *C) {
	count := 0
	f := func(addr string) (*grpc.ClientConn, error) {
		count++
		return grpc.Dial(addr, grpc.WithInsecure(), grpc.WithTimeout(dialTimeout))
	}
	p := NewConnPool(f)

	addr := "127.0.0.1:6379"
	conn1, err := p.Get(addr)
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 1)

	conn2, err := p.Get(addr)
	c.Assert(err, IsNil)
	c.Assert(conn2, Equals, conn1)
	c.Assert(count, Equals, 1)

	p.Put(addr, conn1)
	c.Assert(count, Equals, 1)
	p.Put(addr, conn2)
	c.Assert(count, Equals, 1)

	p.Close()
	conn3, err := p.Get(addr)
	c.Assert(err, NotNil)
	c.Assert(conn3, IsNil)
}

func (s *testPoolSuite) TestPoolCleaner(c *C) {
	p := new(ConnPool)
	p.f = func(addr string) (*grpc.ClientConn, error) {
		return grpc.Dial(
			addr,
			grpc.WithInsecure(),
			grpc.WithTimeout(time.Minute*5))
	}
	p.m.conns = make(map[string]*Conn)
	checkCleanupInterval := time.Millisecond
	testAddr := "127.0.0.1:26666"
	closeCh := make(chan int, 1)
	cleaner := NewConnPoolCleaner(p, checkCleanupInterval, closeCh)
	conn, err := p.Get(testAddr)
	c.Assert(err, IsNil)
	p.Put(testAddr, conn)
	c.Assert(len(p.m.conns), Equals, 1)
	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		cleaner.run()
		wg.Done()
	}()
	time.Sleep(checkCleanupInterval * 2)
	closeCh <- 1
	wg.Wait()
	c.Assert(len(p.m.conns), Equals, 0)
}
