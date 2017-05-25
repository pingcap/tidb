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
	"fmt"
	"sync"

	"google.golang.org/grpc"

	"github.com/juju/errors"
)

// Conn is a simple wrapper of grpc.ClientConn.
type Conn struct {
	c *grpc.ClientConn
	// TODO: add refCount to track how many TiKVClient uses this grpc.ClientConn.
}

// createConnFunc is the type of functions that can be used to create a grpc.ClientConn.
type createConnFunc func(addr string) (*grpc.ClientConn, error)

// ConnPool is a pool that maintains Conn with a specific addr.
// TODO: Implement background cleanup. It adds a backgroud goroutine to periodically check
// whether there is any Conn in pool have no usage (refCount == 0), and then
// close and remove it from pool.
type ConnPool struct {
	m struct {
		sync.RWMutex
		isClosed bool
		conns    map[string]*Conn
	}
	f createConnFunc
}

// NewConnPool creates a ConnPool.
func NewConnPool(f createConnFunc) *ConnPool {
	p := new(ConnPool)
	p.f = f
	p.m.conns = make(map[string]*Conn)
	return p
}

// Get takes a Conn out of the pool by the specific addr.
func (p *ConnPool) Get(addr string) (*grpc.ClientConn, error) {
	p.m.RLock()
	if p.m.isClosed {
		p.m.RUnlock()
		return nil, errors.Errorf("ConnPool is closed")
	}
	conn, ok := p.m.conns[addr]
	p.m.RUnlock()
	if !ok {
		var err error
		conn, err = p.tryCreate(addr)
		if err != nil {
			return nil, err
		}
	}
	// TODO: increase refCount for conn.
	return conn.c, nil
}

func (p *ConnPool) tryCreate(addr string) (*Conn, error) {
	p.m.Lock()
	defer p.m.Unlock()
	conn, ok := p.m.conns[addr]
	if !ok {
		c, err := p.f(addr)
		if err != nil {
			return nil, errors.Trace(err)
		}
		conn = &Conn{
			c: c,
		}
		p.m.conns[addr] = conn
	}
	return conn, nil
}

// Put puts a Conn back to the pool by the specific addr.
func (p *ConnPool) Put(addr string, c *grpc.ClientConn) {
	p.m.RLock()
	conn, ok := p.m.conns[addr]
	p.m.RUnlock()
	if !ok {
		panic(fmt.Errorf("Attempt to Put for a non-existent addr %s", addr))
	}
	if conn.c != c {
		panic(fmt.Errorf("Attempt to Put a non-existent ClientConn for addr %s", addr))
	}
	// TODO: decrease refCount for conn.
}

// Close closes the pool.
func (p *ConnPool) Close() {
	p.m.Lock()
	defer p.m.Unlock()
	p.m.isClosed = true
}
