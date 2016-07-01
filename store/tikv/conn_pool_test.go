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
	"net"
	"time"

	. "github.com/pingcap/check"
)

type testPoolSuite struct {
}

var _ = Suite(&testPoolSuite{})

type testDummyConn struct {
}

func (c *testDummyConn) Read(b []byte) (n int, err error)   { return len(b), nil }
func (c *testDummyConn) Write(b []byte) (n int, err error)  { return len(b), nil }
func (c *testDummyConn) Close() error                       { return nil }
func (c *testDummyConn) LocalAddr() net.Addr                { return nil }
func (c *testDummyConn) RemoteAddr() net.Addr               { return nil }
func (c *testDummyConn) SetDeadline(t time.Time) error      { return nil }
func (c *testDummyConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *testDummyConn) SetWriteDeadline(t time.Time) error { return nil }

func (s *testPoolSuite) TestPool(c *C) {
	count := 0
	f := func(addr string) (*Conn, error) {
		count++
		return &Conn{closed: false, nc: &testDummyConn{}}, nil
	}

	capability := 4
	p := NewPool("127.0.0.1:6379", 4, f)

	conns := make([]*Conn, 0, capability)
	for i := 0; i < capability; i++ {
		conn, err := p.GetConn()
		c.Assert(err, IsNil)
		conns = append(conns, conn)
	}

	c.Assert(count, Equals, capability)

	// we can not get any connection now, so TryGet will return nil
	emptyConn, err := p.p.TryGet()
	c.Assert(err, IsNil)
	c.Assert(emptyConn, IsNil)

	for i := 0; i < len(conns); i++ {
		p.PutConn(conns[i])
	}

	conns = conns[0:0]

	conn, err := p.GetConn()
	c.Assert(err, IsNil)
	conn.Close()
	p.PutConn(conn)

	// get all connections again, now only one needs to be created
	for i := 0; i < capability; i++ {
		conn, err1 := p.GetConn()
		c.Assert(err1, IsNil)
		conns = append(conns, conn)
	}

	for i := 0; i < len(conns); i++ {
		p.PutConn(conns[i])
	}

	c.Assert(count, Equals, capability+1)

	p.Close()

	_, err = p.GetConn()
	c.Assert(err, NotNil)
}
