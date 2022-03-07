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

// Package pools provides functionality to manage and reuse resources
// like connections.
package pools

import (
	"fmt"
	"time"

	"go.uber.org/atomic"
)

type Factory func() (Resource, error)

// Resource is an abstraction for all data structures
// that can be pooling. Thread synchronization between Close()
// and IsClosed() is the responsibility the caller.
type Resource interface {
	Close()
}

// ResourcePool allows you to use a pool of resources.
type ResourcePool struct {
	resources   chan resourceWrapper
	factory     Factory
	capacity    *atomic.Int64
	idleTimeout *atomic.Duration
	waitCount   *atomic.Int64
	waitTime    *atomic.Duration
}

type resourceWrapper struct {
	resource Resource
	timeUsed time.Time
}

// NewResourcePool creates a new ResourcePool pool.
// capacity is the initial capacity of the pool.
// maxCap is the maximum capacity.
// If a resource is unused beyond idleTimeout, it's discarded.
// An idleTimeout of 0 means that there is no timeout.
func NewResourcePool(factory Factory, capacity, maxCap int, idleTimeout time.Duration) *ResourcePool {
	if capacity <= 0 || maxCap <= 0 || capacity > maxCap {
		panic(fmt.Errorf("invalid/out of range capacity"))
	}
	rp := &ResourcePool{
		resources:   make(chan resourceWrapper, maxCap),
		factory:     factory,
		capacity:    atomic.NewInt64(int64(capacity)),
		idleTimeout: atomic.NewDuration(idleTimeout),
	}
	for i := 0; i < capacity; i++ {
		rp.resources <- resourceWrapper{}
	}
	return rp
}

// Close empties the pool calling Close on all its resources.
// You can call Close while there are outstanding resources.
// It waits for all resources to be returned (Put).
// After a Close, Get are not allowed.
func (rp *ResourcePool) Close() {
	// Atomically swap new capacity with old, but only
	// if old capacity is non-zero.
	var capacity int
	for {
		capacity = int(rp.capacity.Load())
		if capacity == 0 {
			return
		}
		if rp.capacity.CAS(int64(capacity), int64(0)) {
			break
		}
	}

	for i := 0; i < capacity; i++ {
		wrapper := <-rp.resources
		if wrapper.resource != nil {
			wrapper.resource.Close()
		}
	}

	close(rp.resources)
}

// IsClosed returns whether the ResourcePool is closed.
func (rp *ResourcePool) IsClosed() (closed bool) {
	return rp.capacity.Load() == 0
}

// Get will return the next available resource. If capacity
// has not been reached, it will create a new one using the factory. Otherwise,
// it will indefinitely wait till the next resource becomes available.
func (rp *ResourcePool) Get() (resource Resource, err error) {
	// Fetch
	var wrapper resourceWrapper
	var ok bool
	wrapper, ok = <-rp.resources
	if !ok {
		return nil, fmt.Errorf("ResourcePool is closed")
	}

	// Unwrap
	timeout := rp.idleTimeout.Load()
	if wrapper.resource != nil && timeout > 0 && wrapper.timeUsed.Add(timeout).Sub(time.Now()) < 0 {
		wrapper.resource.Close()
		wrapper.resource = nil
	}
	if wrapper.resource == nil {
		wrapper.resource, err = rp.factory()
		if err != nil {
			rp.resources <- resourceWrapper{}
		}
	}
	return wrapper.resource, err
}

// Put will return a resource to the pool. For every successful Get,
// a corresponding Put is required. If you no longer need a resource,
// you will need to call Put(nil) instead of returning the closed resource.
// This will eventually cause a new resource to be created in its place.
func (rp *ResourcePool) Put(resource Resource) {
	var wrapper resourceWrapper
	if resource != nil {
		wrapper = resourceWrapper{resource, time.Now()}
	}
	select {
	case rp.resources <- wrapper:
	default:
		panic(fmt.Errorf("attempt to Put into a full ResourcePool"))
	}
}
