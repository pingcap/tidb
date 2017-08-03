// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pools

import (
	"testing"
	"time"
)

func TestPool(t *testing.T) {
	lastId.Set(0)
	p := NewRoundRobin(5, time.Duration(10e9))
	p.Open(PoolFactory)
	defer p.Close()

	for i := 0; i < 2; i++ {
		r, err := p.TryGet()
		if err != nil {
			t.Errorf("TryGet failed: %v", err)
		}
		if r.(*TestResource).num != 1 {
			t.Errorf("Expecting 1, received %d", r.(*TestResource).num)
		}
		p.Put(r)
	}
	// p = [1]

	all := make([]Resource, 5)
	for i := 0; i < 5; i++ {
		if all[i], _ = p.TryGet(); all[i] == nil {
			t.Errorf("TryGet failed with nil")
		}
	}
	// all = [1-5], p is empty
	if none, _ := p.TryGet(); none != nil {
		t.Errorf("TryGet failed with non-nil")
	}

	ch := make(chan bool)
	go ResourceWait(p, t, ch)
	time.Sleep(1e8)
	for i := 0; i < 5; i++ {
		p.Put(all[i])
	}
	// p = [1-5]
	<-ch
	// p = [1-5]
	if p.waitCount != 1 {
		t.Errorf("Expecting 1, received %d", p.waitCount)
	}

	for i := 0; i < 5; i++ {
		all[i], _ = p.Get()
	}
	// all = [1-5], p is empty
	all[0].(*TestResource).Close()
	p.Put(nil)
	for i := 1; i < 5; i++ {
		p.Put(all[i])
	}
	// p = [2-5]

	for i := 0; i < 4; i++ {
		r, _ := p.Get()
		if r.(*TestResource).num != int64(i+2) {
			t.Errorf("Expecting %d, received %d", i+2, r.(*TestResource).num)
		}
		p.Put(r)
	}

	p.SetCapacity(3)
	// p = [2-4]
	if p.size != 3 {
		t.Errorf("Expecting 3, received %d", p.size)
	}

	p.SetIdleTimeout(time.Duration(1e8))
	time.Sleep(2e8)
	r, _ := p.Get()
	if r.(*TestResource).num != 6 {
		t.Errorf("Expecting 6, received %d", r.(*TestResource).num)
	}
	p.Put(r)
	// p = [6]
}

func TestPoolFail(t *testing.T) {
	p := NewRoundRobin(5, time.Duration(10e9))
	p.Open(FailFactory)
	defer p.Close()
	if _, err := p.Get(); err.Error() != "Failed" {
		t.Errorf("Expecting Failed, received %v", err)
	}
}

func TestPoolFullFail(t *testing.T) {
	p := NewRoundRobin(2, time.Duration(10e9))
	p.Open(SlowFailFactory)
	defer p.Close()
	ch := make(chan bool)
	// The third get should not wait indefinitely
	for i := 0; i < 3; i++ {
		go func() {
			p.Get()
			ch <- true
		}()
	}
	for i := 0; i < 3; i++ {
		<-ch
	}
}

func ResourceWait(p *RoundRobin, t *testing.T, ch chan bool) {
	for i := 0; i < 5; i++ {
		if r, err := p.Get(); err != nil {
			t.Errorf("TryGet failed: %v", err)
		} else if r.(*TestResource).num != int64(i+1) {
			t.Errorf("Expecting %d, received %d", i+1, r.(*TestResource).num)
		} else {
			p.Put(r)
		}
	}
	ch <- true
}
