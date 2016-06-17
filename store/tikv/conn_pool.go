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

	"github.com/juju/errors"
	"github.com/ngaut/pools"
)

const poolIdleTimeoutSeconds = 120

type cerateConnFunc func(addr string) (*Conn, error)

// Pool is a TCP connection pool that maintains connections with a specific addr.
type Pool struct {
	p *pools.ResourcePool
}

// NewPool creates a Pool.
func NewPool(addr string, capability int, f cerateConnFunc) *Pool {
	poolFunc := func() (pools.Resource, error) {
		r, err := f(addr)
		if err == nil && r == nil {
			return nil, errors.Errorf("cannot create nil connection")
		}
		return r, errors.Trace(err)
	}

	p := new(Pool)
	p.p = pools.NewResourcePool(poolFunc, capability, capability, poolIdleTimeoutSeconds*time.Second)
	return p
}

// GetConn takes a connection out of the pool.
func (p *Pool) GetConn() (*Conn, error) {
	conn, err := p.p.Get()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return conn.(*Conn), nil
}

// PutConn puts a connection back to the pool.
func (p *Pool) PutConn(c *Conn) {
	if c == nil {
		return
	} else if c.closed {
		// if c is closed, we will put nil
		p.p.Put(nil)
	} else {
		p.p.Put(c)
	}
}

// Close closes the pool.
func (p *Pool) Close() {
	p.p.Close()
}

// Pools maintains connections with multiple addrs.
type Pools struct {
	m sync.Mutex

	capability int

	mpools map[string]*Pool

	f cerateConnFunc
}

// NewPools creates a Pools.
func NewPools(capability int, f cerateConnFunc) *Pools {
	p := new(Pools)
	p.f = f
	p.capability = capability
	p.mpools = make(map[string]*Pool)
	return p
}

// GetConn takes a connection out of the pool by addr.
func (p *Pools) GetConn(addr string) (*Conn, error) {
	p.m.Lock()
	pool, ok := p.mpools[addr]
	if !ok {
		pool = NewPool(addr, p.capability, p.f)
		p.mpools[addr] = pool
	}
	p.m.Unlock()

	return pool.GetConn()
}

// PutConn puts a connection back to the pool.
func (p *Pools) PutConn(c *Conn) {
	if c == nil {
		return
	}

	p.m.Lock()
	pool, ok := p.mpools[c.addr]
	p.m.Unlock()
	if !ok {
		c.Close()
	} else {
		pool.PutConn(c)
	}
}

// Close closes the pool.
func (p *Pools) Close() {
	p.m.Lock()
	defer p.m.Unlock()

	for _, pool := range p.mpools {
		pool.Close()
	}

	p.mpools = map[string]*Pool{}
}
