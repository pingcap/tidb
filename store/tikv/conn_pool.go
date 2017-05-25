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
	"time"

	"google.golang.org/grpc"

	"github.com/juju/errors"
)

const (
	poolIterateMaxCount      = 1000
	poolCleanupMaxCount      = 200
	poolCheckCleanupInterval = 10 * time.Minute
)

// Conn is a simple wrapper of grpc.ClientConn.
type Conn struct {
	c *grpc.ClientConn
	// The reference count of how many TiKVClient uses this grpc.ClientConn at present.
	// It's used for the implementation of backgroud cleanup of idle connections in ConnPool.
	refCount uint32
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
	f                  createConnFunc
	backgroudCleanerCh chan int
	backgroudCleanerWg *sync.WaitGroup
}

// NewConnPool creates a ConnPool.
func NewConnPool(f createConnFunc) *ConnPool {
	p := new(ConnPool)
	p.f = f
	p.m.conns = make(map[string]*Conn)
	// initialize backgroud cleaner
	closeCh := make(chan int, 1)
	wg := new(sync.WaitGroup)
	cleaner := NewConnPoolCleaner(p, poolCheckCleanupInterval, closeCh)
	wg.Add(1)
	// spawn the cleaner goroutine
	go func() {
		cleaner.run()
		wg.Done()
	}()
	p.backgroudCleanerCh = closeCh
	p.backgroudCleanerWg = wg
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
	if ok {
		// Increase refCount.
		conn.refCount += 1
		p.m.RUnlock()
		return conn.c, nil
	}
	p.m.RUnlock()
	var err error
	conn, err = p.tryCreate(addr)
	if err != nil {
		return nil, err
	}
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
			c:        c,
			refCount: 1,
		}
		p.m.conns[addr] = conn
	}
	return conn, nil
}

// Put puts a Conn back to the pool by the specific addr.
func (p *ConnPool) Put(addr string, c *grpc.ClientConn) {
	p.m.RLock()
	defer p.m.RUnlock()
	conn, ok := p.m.conns[addr]
	if !ok {
		panic(fmt.Errorf("Attempt to Put for a non-existent addr %s", addr))
	}
	if conn.c != c {
		panic(fmt.Errorf("Attempt to Put a non-existent ClientConn for addr %s", addr))
	}
	// Decrease refCount.
	if conn.refCount == 0 {
		panic(fmt.Errorf("Attempt to Put a ClientConn with refCount 0 for addr %s", addr))
	}
	conn.refCount -= 1
}

// Close closes the pool.
func (p *ConnPool) Close() {
	p.m.Lock()
	if !p.m.isClosed {
		p.m.isClosed = true
		p.m.Unlock()
		// Ask the cleaner to exit and wait for it.
		select {
		case p.backgroudCleanerCh <- 1:
			p.backgroudCleanerWg.Wait()
		default:
		}
		return
	}
	p.m.Unlock()
}

func (p *ConnPool) cleanupIdleConn() {
	p.m.Lock()
	defer p.m.Unlock()
	iterateCount, cleanupCount := 0, 0
	for addr, conn := range p.m.conns {
		if conn.refCount == 0 {
			delete(p.m.conns, addr)
			cleanupCount += 1
			if cleanupCount >= poolCleanupMaxCount {
				return
			}
		}
		iterateCount += 1
		if iterateCount >= poolIterateMaxCount {
			return
		}
	}
}

// ConnPoolCleaner clean up idle connection periodically in specified time interval.
type ConnPoolCleaner struct {
	pool     *ConnPool
	interval time.Duration
	closeCh  chan int
}

func NewConnPoolCleaner(pool *ConnPool, interval time.Duration, closeCh chan int) *ConnPoolCleaner {
	return &ConnPoolCleaner{
		pool:     pool,
		interval: interval,
		closeCh:  closeCh,
	}
}

func (c *ConnPoolCleaner) run() {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	for {
		select {
		case <-c.closeCh:
			return
		case <-ticker.C:
			c.pool.cleanupIdleConn()
		}
	}
}
