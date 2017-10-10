// Copyright 2017 PingCAP, Inc.
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
// +build !leak

package gp

import (
	"sync"
	"sync/atomic"
	"time"
)

// Pool is a struct to represent goroutine pool.
type Pool struct {
	head        goroutine
	tail        *goroutine
	count       int
	idleTimeout time.Duration
	sync.Mutex
}

// goroutine is actually a background goroutine, with a channel binded for communication.
type goroutine struct {
	ch     chan func()
	next   *goroutine
	status int32
}

const (
	statusIdle  int32 = 0
	statusInUse int32 = 1
	statusDead  int32 = 2
)

// New returns a new *Pool object.
func New(idleTimeout time.Duration) *Pool {
	pool := &Pool{
		idleTimeout: idleTimeout,
	}
	pool.tail = &pool.head
	return pool
}

// Go works like go func(), but goroutines are pooled for reusing.
// This strategy can avoid runtime.morestack, because pooled goroutine is already enlarged.
func (pool *Pool) Go(f func()) {
	for {
		g := pool.get()
		if atomic.CompareAndSwapInt32(&g.status, statusIdle, statusInUse) {
			g.ch <- f
			return
		}
		// Status already changed from statusIdle => statusDead, drop it, find next one.
	}
}

func (pool *Pool) get() *goroutine {
	pool.Lock()
	head := &pool.head
	if head.next == nil {
		pool.Unlock()
		return pool.alloc()
	}

	ret := head.next
	head.next = ret.next
	if ret == pool.tail {
		pool.tail = head
	}
	pool.count--
	pool.Unlock()
	ret.next = nil
	return ret
}

func (pool *Pool) alloc() *goroutine {
	g := &goroutine{
		ch: make(chan func()),
	}
	go g.workLoop(pool)
	return g
}

func (g *goroutine) put(pool *Pool) {
	g.status = statusIdle
	pool.Lock()
	pool.tail.next = g
	pool.tail = g
	pool.count++
	pool.Unlock()
}

func (g *goroutine) workLoop(pool *Pool) {
	timer := time.NewTimer(pool.idleTimeout)
	for {
		select {
		case <-timer.C:
			// Check to avoid a corner case that the goroutine is take out from pool,
			// and get this signal at the same time.
			succ := atomic.CompareAndSwapInt32(&g.status, statusIdle, statusDead)
			if succ {
				return
			}
		case work := <-g.ch:
			work()
			// Put g back to the pool.
			// This is the normal usage for a resource pool:
			//
			//     obj := pool.get()
			//     use(obj)
			//     pool.put(obj)
			//
			// But when goroutine is used as a resource, we can't pool.put() immediately,
			// because the resource(goroutine) maybe still in use.
			// So, put back resource is done here,  when the goroutine finish its work.
			g.put(pool)
		}
		timer.Reset(pool.idleTimeout)
	}
}
