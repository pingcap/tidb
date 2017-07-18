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

package gp

import (
	"sync"
	"time"
)

// Pool is a struct to represent goroutine pool.
type Pool struct {
	head        goroutine
	tail        *goroutine
	count       int
	idleTimeout time.Duration
	sync.Mutex

	// gcWorker marks whether there is a gcWorker currently.
	// only gc worker goroutine can modify it, others just read it.
	gcWorker struct {
		sync.RWMutex
		value bool
	}
}

// goroutine is actually a background goroutine, with a channel binded for communication.
type goroutine struct {
	ch      chan func()
	lastRun time.Time
	pool    *Pool
	next    *goroutine
}

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
	g := pool.get()
	g.ch <- f
	// When the goroutine finish f(), it will be put back to pool automatically,
	// so it doesn't need to call pool.put() here.
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

func (pool *Pool) put(p *goroutine) {
	p.next = nil
	pool.Lock()
	pool.tail.next = p
	pool.tail = p
	pool.count++
	pool.Unlock()

	pool.gcWorker.RLock()
	gcWorker := pool.gcWorker.value
	pool.gcWorker.RUnlock()
	if !gcWorker {
		go pool.gcLoop()
	}
}

func (pool *Pool) alloc() *goroutine {
	g := &goroutine{
		ch:   make(chan func()),
		pool: pool,
	}
	go func(g *goroutine) {
		for work := range g.ch {
			work()
			g.lastRun = time.Now()
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
			pool.put(g)
		}
	}(g)
	return g
}

func (pool *Pool) gcLoop() {
	pool.gcWorker.Lock()
	if pool.gcWorker.value == true {
		pool.gcWorker.Unlock()
		return
	}
	pool.gcWorker.value = true
	pool.gcWorker.Unlock()

	for {
		finish, more := pool.gcOnce(30)
		if finish {
			pool.gcWorker.Lock()
			pool.gcWorker.value = false
			pool.gcWorker.Unlock()
			return
		}
		if more {
			time.Sleep(min(pool.idleTimeout/10, 500*time.Millisecond))
		} else {
			time.Sleep(min(5*time.Second, pool.idleTimeout/3))
		}
	}
}

// gcOnce runs gc once, recycles at most count goroutines.
// finish indicates there're no more goroutines in the pool after gc,
// more indicates there're still many goroutines to be recycled.
func (pool *Pool) gcOnce(count int) (finish bool, more bool) {
	now := time.Now()
	i := 0
	pool.Lock()
	head := &pool.head
	for head.next != nil && i < count {
		save := head.next
		duration := now.Sub(save.lastRun)
		if duration < pool.idleTimeout {
			break
		}
		close(save.ch)
		head.next = save.next
		pool.count--
		i++
	}
	if head.next == nil {
		finish = true
		pool.tail = head
	}
	pool.Unlock()
	more = (i == count)
	return
}

func min(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
