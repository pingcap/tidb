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
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const (
	OnceLoopGoroutineNum = 30
)

const (
	PoolInit = iota
	PoolStarting
	PoolGcing
	PoolClosing
	PoolClosed
)

var (
	ErrGoroutinePoolClosed = fmt.Errorf("GoroutinePool has been closed.")
)

// Pool is a struct to represent goroutine pool.
type Pool struct {
	head        goroutine
	tail        *goroutine
	count       int // idle list size
	idleTimeout time.Duration
	done        bool
	sync.Mutex

	grID         int32
	state        int32
	workStartNum int64
	workFinNum   int64
}

// goroutine is actually a background goroutine, with a channel bound for communication.
type goroutine struct {
	id      int32
	ch      chan func()
	lastRun time.Time
	sync.Mutex
	pool *Pool
	next *goroutine
}

// New returns a new *Pool object.
func NewGoroutinePool(idleTimeout time.Duration) *Pool {
	pool := &Pool{
		idleTimeout: idleTimeout,
		state:       PoolInit,
	}
	pool.tail = &pool.head
	go pool.gc()
	return pool
}

// check whether the pool has been closed.
func (p *Pool) IsClosed() bool {
	p.Lock()
	defer p.Unlock()
	return p.done
}

func (p *Pool) stop() {
	p.Lock()
	defer p.Unlock()
	p.done = true
}

func (p *Pool) Close() {
	p.stop()
	atomic.CompareAndSwapInt32(&p.state, PoolGcing, PoolClosing)
	t := min(p.idleTimeout/10, 50*time.Millisecond)
	for {
		if atomic.LoadInt64(&p.workStartNum) == atomic.LoadInt64(&p.workFinNum) {
			return
		}
		time.Sleep(t)
	}
}

// Go works like go func(), but goroutines are pooled for reusing.
// This strategy can avoid runtime.morestack, because pooled goroutine is already enlarged.
func (p *Pool) Go(f func()) error {
	if p.IsClosed() {
		return ErrGoroutinePoolClosed
	}

	g := p.get()
	g.ch <- f
	atomic.AddInt64(&p.workStartNum, 1)
	// When the goroutine finish f(), it will be put back to pool automatically,
	// so it doesn't need to call pool.put() here.

	return nil
}

func (p *Pool) get() *goroutine {
	p.Lock()
	head := &p.head
	if head.next == nil {
		p.Unlock()
		return p.alloc()
	}

	ret := head.next
	head.next = ret.next
	if ret == p.tail {
		p.tail = head
	}
	p.count--
	p.Unlock()
	ret.next = nil
	return ret
}

func (p *Pool) put(g *goroutine) {
	g.next = nil
	p.Lock()
	p.tail.next = g
	p.tail = g
	p.count++
	p.Unlock()

	atomic.CompareAndSwapInt32(&p.state, PoolStarting, PoolGcing)
}

func (p *Pool) alloc() *goroutine {
	id := atomic.AddInt32(&p.grID, 1)
	g := &goroutine{
		ch:   make(chan func()),
		pool: p,
		id:   id,
	}

	go func(g *goroutine) {
		for work := range g.ch {
			work()
			atomic.AddInt64(&p.workFinNum, 1)
			g.Lock()
			g.lastRun = time.Now()
			g.Unlock()
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
			p.put(g)
		}
	}(g)

	atomic.CompareAndSwapInt32(&p.state, PoolInit, PoolStarting)

	return g
}

func (p *Pool) gc() {
	var (
		finish bool
		more   bool
		state  int32
		t      time.Duration
	)

	for {
		finish = false
		more = false
		if state = atomic.LoadInt32(&p.state); state > PoolStarting {
			finish, more = p.gcOnce(OnceLoopGoroutineNum)
			if finish {
				atomic.CompareAndSwapInt32(&p.state, PoolClosing, PoolClosed)
				return
			}
		}

		if atomic.LoadInt32(&p.state) >= PoolClosing {
			t = min(p.idleTimeout/10, 10*time.Millisecond)
		} else if more {
			t = min(p.idleTimeout/10, 500*time.Millisecond)
		} else {
			t = min(5*time.Second, p.idleTimeout/3)
		}
		time.Sleep(t)
	}
}

// gcOnce runs gc once, recycles at most count goroutines.
// finish indicates there're no more goroutines in the pool after gc,
// more indicates there're still many goroutines to be recycled.
func (p *Pool) gcOnce(count int) (finish bool, more bool) {
	now := time.Now()
	i := 0
	p.Lock()
	head := &p.head

	var lastRun time.Time
	for head.next != nil && i < count {
		save := head.next
		save.Lock()
		lastRun = save.lastRun
		save.Unlock()
		duration := now.Sub(lastRun)
		if duration < p.idleTimeout {
			break
		}
		close(save.ch)
		head.next = save.next
		p.count--
		i++
	}

	if head.next == nil {
		finish = true
		p.tail = head
	}

	p.Unlock()
	more = (i == count)
	return
}

func min(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}

	return b
}
