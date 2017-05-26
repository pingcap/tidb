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

	"github.com/juju/errors"
	"google.golang.org/grpc"
)

const (
	// poolIterateMaxCount specifies at most how many connections are iterated
	// and checked by the backgroud cleanup goroutine at once.
	// Both poolIterateMaxCount and poolCleanupMaxCount are used to avoid the cleanup time
	// being too long so that the backgroud cleanup will not block the foreground business.
	poolIterateMaxCount = 1000
	// poolCleanupMaxCount specifies at most how many idle connections are cleaned up
	// by the background cleanup goroutine at once.
	poolCleanupMaxCount = 200
	// poolCheckCleanupInterval specifies the time interval that the background cleanup goroutine
	// periodically wakes up and does the cleanup if necessary.
	poolCheckCleanupInterval = 5 * time.Minute
	// When a connection keep idle more than poolCleanupIdleDuration, it would be cleaned up
	// by the backgroud cleanup goroutine.
	poolCleanupIdleDuration = 3 * time.Minute
)

// Conn is a simple wrapper of grpc.ClientConn.
type Conn struct {
	c *grpc.ClientConn
	// The reference count of how many TiKVClient uses this grpc.ClientConn at present.
	// It's used for the implementation of backgroud cleanup of idle connections in ConnPool.
	refCount uint32
	// The timestamp of this grpc.ClientConn becomes idle (refCount == 0).
	// It's used for the implementation of backgroud cleanup of idel connections in ConnPool.
	becomeIdle time.Time
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
	cleaner := newConnPoolCleaner(p, poolCheckCleanupInterval, poolCleanupIdleDuration, closeCh)
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
		conn.refCount++
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
	conn.refCount--
	if conn.refCount == 0 {
		conn.becomeIdle = time.Now()
	}
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

func (p *ConnPool) cleanupConnIdleAfter(d time.Duration) {
	now := time.Now()
	p.m.Lock()
	defer p.m.Unlock()
	iterateCount, cleanupCount := 0, 0
	for addr, conn := range p.m.conns {
		if conn.refCount == 0 && now.After(conn.becomeIdle.Add(d)) {
			delete(p.m.conns, addr)
			cleanupCount++
			if cleanupCount >= poolCleanupMaxCount {
				return
			}
		}
		iterateCount++
		if iterateCount >= poolIterateMaxCount {
			return
		}
	}
}

// ConnPoolCleaner clean up idle connection periodically in specified time interval.
type ConnPoolCleaner struct {
	pool                *ConnPool
	checkInterval       time.Duration
	cleanupIdleDuration time.Duration
	closeCh             chan int
}

func newConnPoolCleaner(
	pool *ConnPool,
	checkInterval time.Duration,
	cleanupIdleDuration time.Duration,
	closeCh chan int) *ConnPoolCleaner {

	return &ConnPoolCleaner{
		pool:                pool,
		checkInterval:       checkInterval,
		cleanupIdleDuration: cleanupIdleDuration,
		closeCh:             closeCh,
	}
}

func (c *ConnPoolCleaner) run() {
	ticker := time.NewTicker(c.checkInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.closeCh:
			return
		case <-ticker.C:
			c.pool.cleanupConnIdleAfter(c.cleanupIdleDuration)
		}
	}
}
