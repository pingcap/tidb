// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pools

import (
	"errors"
	"testing"
	"time"

	atomicutil "go.uber.org/atomic"
)

var lastId, count = atomicutil.NewInt64(0), atomicutil.NewInt64(0)

type TestResource struct {
	num    int64
	closed bool
}

func (tr *TestResource) Close() {
	if !tr.closed {
		count.Add(-1)
		tr.closed = true
	}
}

func (tr *TestResource) IsClosed() bool {
	return tr.closed
}

func PoolFactory() (Resource, error) {
	count.Add(1)
	return &TestResource{lastId.Add(1), false}, nil
}

func FailFactory() (Resource, error) {
	return nil, errors.New("Failed")
}

func SlowFailFactory() (Resource, error) {
	time.Sleep(10 * time.Nanosecond)
	return nil, errors.New("Failed")
}

func TestOpen(t *testing.T) {
	lastId.Store(0)
	count.Store(0)
	p := NewResourcePool(PoolFactory, 6, 6, time.Second)
	p.SetCapacity(5)
	var resources [10]Resource

	// Test Get
	for i := 0; i < 5; i++ {
		r, err := p.Get()
		resources[i] = r
		if err != nil {
			t.Errorf("Unexpected error %v", err)
		}
		_, available, _, waitCount, waitTime, _ := p.Stats()
		if available != int64(5-i-1) {
			t.Errorf("expecting %d, received %d", 5-i-1, available)
		}
		if waitCount != 0 {
			t.Errorf("expecting 0, received %d", waitCount)
		}
		if waitTime != 0 {
			t.Errorf("expecting 0, received %d", waitTime)
		}
		if lastId.Load() != int64(i+1) {
			t.Errorf("Expecting %d, received %d", i+1, lastId.Load())
		}
		if count.Load() != int64(i+1) {
			t.Errorf("Expecting %d, received %d", i+1, count.Load())
		}
	}

	// Test TryGet
	r, err := p.TryGet()
	if err != nil {
		t.Errorf("Unexpected error %v", err)
	}
	if r != nil {
		t.Errorf("Expecting nil")
	}
	for i := 0; i < 5; i++ {
		p.Put(resources[i])
		_, available, _, _, _, _ := p.Stats()
		if available != int64(i+1) {
			t.Errorf("expecting %d, received %d", 5-i-1, available)
		}
	}
	for i := 0; i < 5; i++ {
		r, err := p.TryGet()
		resources[i] = r
		if err != nil {
			t.Errorf("Unexpected error %v", err)
		}
		if r == nil {
			t.Errorf("Expecting non-nil")
		}
		if lastId.Load() != 5 {
			t.Errorf("Expecting 5, received %d", lastId.Load())
		}
		if count.Load() != 5 {
			t.Errorf("Expecting 5, received %d", count.Load())
		}
	}

	// Test that Get waits
	ch := make(chan bool)
	go func() {
		for i := 0; i < 5; i++ {
			r, err := p.Get()
			if err != nil {
				t.Errorf("Get failed: %v", err)
			}
			resources[i] = r
		}
		for i := 0; i < 5; i++ {
			p.Put(resources[i])
		}
		ch <- true
	}()
	for i := 0; i < 5; i++ {
		// Sleep to ensure the goroutine waits
		time.Sleep(10 * time.Nanosecond)
		p.Put(resources[i])
	}
	<-ch
	_, _, _, waitCount, waitTime, _ := p.Stats()
	if waitCount != 5 {
		t.Errorf("Expecting 5, received %d", waitCount)
	}
	if waitTime == 0 {
		t.Errorf("Expecting non-zero")
	}
	if lastId.Load() != 5 {
		t.Errorf("Expecting 5, received %d", lastId.Load())
	}

	// Test Close resource
	r, err = p.Get()
	if err != nil {
		t.Errorf("Unexpected error %v", err)
	}
	r.Close()
	p.Put(nil)
	if count.Load() != 4 {
		t.Errorf("Expecting 4, received %d", count.Load())
	}
	for i := 0; i < 5; i++ {
		r, err := p.Get()
		if err != nil {
			t.Errorf("Get failed: %v", err)
		}
		resources[i] = r
	}
	for i := 0; i < 5; i++ {
		p.Put(resources[i])
	}
	if count.Load() != 5 {
		t.Errorf("Expecting 5, received %d", count.Load())
	}
	if lastId.Load() != 6 {
		t.Errorf("Expecting 6, received %d", lastId.Load())
	}

	// SetCapacity
	p.SetCapacity(3)
	if count.Load() != 3 {
		t.Errorf("Expecting 3, received %d", count.Load())
	}
	if lastId.Load() != 6 {
		t.Errorf("Expecting 6, received %d", lastId.Load())
	}
	capacity, available, _, _, _, _ := p.Stats()
	if capacity != 3 {
		t.Errorf("Expecting 3, received %d", capacity)
	}
	if available != 3 {
		t.Errorf("Expecting 3, received %d", available)
	}
	p.SetCapacity(6)
	capacity, available, _, _, _, _ = p.Stats()
	if capacity != 6 {
		t.Errorf("Expecting 6, received %d", capacity)
	}
	if available != 6 {
		t.Errorf("Expecting 6, received %d", available)
	}
	for i := 0; i < 6; i++ {
		r, err := p.Get()
		if err != nil {
			t.Errorf("Get failed: %v", err)
		}
		resources[i] = r
	}
	for i := 0; i < 6; i++ {
		p.Put(resources[i])
	}
	if count.Load() != 6 {
		t.Errorf("Expecting 5, received %d", count.Load())
	}
	if lastId.Load() != 9 {
		t.Errorf("Expecting 9, received %d", lastId.Load())
	}

	// Close
	p.Close()
	capacity, available, _, _, _, _ = p.Stats()
	if capacity != 0 {
		t.Errorf("Expecting 0, received %d", capacity)
	}
	if available != 0 {
		t.Errorf("Expecting 0, received %d", available)
	}
	if count.Load() != 0 {
		t.Errorf("Expecting 0, received %d", count.Load())
	}
}

func TestShrinking(t *testing.T) {
	lastId.Store(0)
	count.Store(0)
	p := NewResourcePool(PoolFactory, 5, 5, time.Second)
	var resources [10]Resource
	// Leave one empty slot in the pool
	for i := 0; i < 4; i++ {
		r, err := p.Get()
		if err != nil {
			t.Errorf("Get failed: %v", err)
		}
		resources[i] = r
	}
	go p.SetCapacity(3)
	time.Sleep(10 * time.Nanosecond)
	stats := p.StatsJSON()
	expected := `{"Capacity": 3, "Available": 0, "MaxCapacity": 5, "WaitCount": 0, "WaitTime": 0, "IdleTimeout": 1000000000}`
	if stats != expected {
		t.Errorf(`expecting '%s', received '%s'`, expected, stats)
	}

	// TryGet is allowed when shrinking
	r, err := p.TryGet()
	if err != nil {
		t.Errorf("Unexpected error %v", err)
	}
	if r != nil {
		t.Errorf("Expecting nil")
	}

	// Get is allowed when shrinking, but it will wait
	getdone := make(chan bool)
	go func() {
		r, err := p.Get()
		if err != nil {
			t.Errorf("Unexpected error %v", err)
		}
		p.Put(r)
		getdone <- true
	}()

	// Put is allowed when shrinking. It's necessary.
	for i := 0; i < 4; i++ {
		p.Put(resources[i])
	}
	// Wait for Get test to complete
	<-getdone
	stats = p.StatsJSON()
	expected = `{"Capacity": 3, "Available": 3, "MaxCapacity": 5, "WaitCount": 0, "WaitTime": 0, "IdleTimeout": 1000000000}`
	if stats != expected {
		t.Errorf(`expecting '%s', received '%s'`, expected, stats)
	}
	if count.Load() != 3 {
		t.Errorf("Expecting 3, received %d", count.Load())
	}

	// Ensure no deadlock if SetCapacity is called after we start
	// waiting for a resource
	for i := 0; i < 3; i++ {
		resources[i], err = p.Get()
		if err != nil {
			t.Errorf("Unexpected error %v", err)
		}
	}
	// This will wait because pool is empty
	go func() {
		r, err := p.Get()
		if err != nil {
			t.Errorf("Unexpected error %v", err)
		}
		p.Put(r)
		getdone <- true
	}()
	time.Sleep(10 * time.Nanosecond)

	// This will wait till we Put
	go p.SetCapacity(2)
	time.Sleep(10 * time.Nanosecond)

	// This should not hang
	for i := 0; i < 3; i++ {
		p.Put(resources[i])
	}
	<-getdone
	capacity, available, _, _, _, _ := p.Stats()
	if capacity != 2 {
		t.Errorf("Expecting 2, received %d", capacity)
	}
	if available != 2 {
		t.Errorf("Expecting 2, received %d", available)
	}
	if count.Load() != 2 {
		t.Errorf("Expecting 2, received %d", count.Load())
	}

	// Test race condition of SetCapacity with itself
	p.SetCapacity(3)
	for i := 0; i < 3; i++ {
		resources[i], err = p.Get()
		if err != nil {
			t.Errorf("Unexpected error %v", err)
		}
	}
	// This will wait because pool is empty
	go func() {
		r, err := p.Get()
		if err != nil {
			t.Errorf("Unexpected error %v", err)
		}
		p.Put(r)
		getdone <- true
	}()
	time.Sleep(10 * time.Nanosecond)

	// This will wait till we Put
	go p.SetCapacity(2)
	time.Sleep(10 * time.Nanosecond)
	go p.SetCapacity(4)
	time.Sleep(10 * time.Nanosecond)

	// This should not hang
	for i := 0; i < 3; i++ {
		p.Put(resources[i])
	}
	<-getdone

	err = p.SetCapacity(-1)
	if err == nil {
		t.Errorf("Expecting error")
	}
	err = p.SetCapacity(255555)
	if err == nil {
		t.Errorf("Expecting error")
	}

	capacity, available, _, _, _, _ = p.Stats()
	if capacity != 4 {
		t.Errorf("Expecting 4, received %d", capacity)
	}
	if available != 4 {
		t.Errorf("Expecting 4, received %d", available)
	}
}

func TestClosing(t *testing.T) {
	lastId.Store(0)
	count.Store(0)
	p := NewResourcePool(PoolFactory, 5, 5, time.Second)
	var resources [10]Resource
	for i := 0; i < 5; i++ {
		r, err := p.Get()
		if err != nil {
			t.Errorf("Get failed: %v", err)
		}
		resources[i] = r
	}
	ch := make(chan bool)
	go func() {
		p.Close()
		ch <- true
	}()

	// Wait for goroutine to call Close
	time.Sleep(10 * time.Nanosecond)
	stats := p.StatsJSON()
	expected := `{"Capacity": 0, "Available": 0, "MaxCapacity": 5, "WaitCount": 0, "WaitTime": 0, "IdleTimeout": 1000000000}`
	if stats != expected {
		t.Errorf(`expecting '%s', received '%s'`, expected, stats)
	}

	// Put is allowed when closing
	for i := 0; i < 5; i++ {
		p.Put(resources[i])
	}

	// Wait for Close to return
	<-ch

	// SetCapacity must be ignored after Close
	err := p.SetCapacity(1)
	if err == nil {
		t.Errorf("expecting error")
	}

	stats = p.StatsJSON()
	expected = `{"Capacity": 0, "Available": 0, "MaxCapacity": 5, "WaitCount": 0, "WaitTime": 0, "IdleTimeout": 1000000000}`
	if stats != expected {
		t.Errorf(`expecting '%s', received '%s'`, expected, stats)
	}
	if lastId.Load() != 5 {
		t.Errorf("Expecting 5, received %d", count.Load())
	}
	if count.Load() != 0 {
		t.Errorf("Expecting 0, received %d", count.Load())
	}
}

func TestIdleTimeout(t *testing.T) {
	lastId.Store(0)
	count.Store(0)
	p := NewResourcePool(PoolFactory, 1, 1, 10*time.Nanosecond)
	defer p.Close()

	r, err := p.Get()
	if err != nil {
		t.Errorf("Unexpected error %v", err)
	}
	p.Put(r)
	if lastId.Load() != 1 {
		t.Errorf("Expecting 1, received %d", count.Load())
	}
	if count.Load() != 1 {
		t.Errorf("Expecting 1, received %d", count.Load())
	}
	time.Sleep(20 * time.Nanosecond)
	r, err = p.Get()
	if err != nil {
		t.Errorf("Unexpected error %v", err)
	}
	if lastId.Load() != 2 {
		t.Errorf("Expecting 2, received %d", count.Load())
	}
	if count.Load() != 1 {
		t.Errorf("Expecting 1, received %d", count.Load())
	}
	p.Put(r)
}

func TestCreateFail(t *testing.T) {
	lastId.Store(0)
	count.Store(0)
	p := NewResourcePool(FailFactory, 5, 5, time.Second)
	defer p.Close()
	if _, err := p.Get(); err.Error() != "Failed" {
		t.Errorf("Expecting Failed, received %v", err)
	}
	stats := p.StatsJSON()
	expected := `{"Capacity": 5, "Available": 5, "MaxCapacity": 5, "WaitCount": 0, "WaitTime": 0, "IdleTimeout": 1000000000}`
	if stats != expected {
		t.Errorf(`expecting '%s', received '%s'`, expected, stats)
	}
}

func TestSlowCreateFail(t *testing.T) {
	lastId.Store(0)
	count.Store(0)
	p := NewResourcePool(SlowFailFactory, 2, 2, time.Second)
	defer p.Close()
	ch := make(chan bool)
	// The third Get should not wait indefinitely
	for i := 0; i < 3; i++ {
		go func() {
			p.Get()
			ch <- true
		}()
	}
	for i := 0; i < 3; i++ {
		<-ch
	}
	_, available, _, _, _, _ := p.Stats()
	if available != 2 {
		t.Errorf("Expecting 2, received %d", available)
	}
}
