// Copyright 2024 PingCAP, Inc.
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

package util

import (
	"errors"
	"sync"

	"github.com/ngaut/pools"
	"github.com/pingcap/failpoint"
)

// SessionPool is a recyclable resource pool for the session.
type SessionPool interface {
	Get() (pools.Resource, error)
	Put(pools.Resource)
	Close()
}

// resourceCallback is a helper function to be triggered after Get/Put call.
type resourceCallback func(pools.Resource)

type pool struct {
	resources chan pools.Resource
	factory   pools.Factory
	mu        struct {
		sync.RWMutex
		closed bool
	}
	getCallback resourceCallback
	putCallback resourceCallback
}

// NewSessionPool creates a new session pool with the given capacity and factory function.
func NewSessionPool(capacity int, factory pools.Factory, getCallback, putCallback resourceCallback) SessionPool {
	return &pool{
		resources:   make(chan pools.Resource, capacity),
		factory:     factory,
		getCallback: getCallback,
		putCallback: putCallback,
	}
}

// Get gets a session from the session pool.
func (p *pool) Get() (resource pools.Resource, err error) {
	var ok bool
	select {
	case resource, ok = <-p.resources:
		if !ok {
			err = errors.New("session pool closed")
		}
	default:
		resource, err = p.factory()
	}

	// Put the internal session to the map of SessionManager
	failpoint.Inject("mockSessionPoolReturnError", func() {
		err = errors.New("mockSessionPoolReturnError")
	})

	if err == nil && p.getCallback != nil {
		p.getCallback(resource)
	}

	return
}

// Put puts the session back to the pool.
func (p *pool) Put(resource pools.Resource) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.putCallback != nil {
		p.putCallback(resource)
	}
	if p.mu.closed {
		resource.Close()
		return
	}

	select {
	case p.resources <- resource:
	default:
		resource.Close()
	}
}

// Close closes the pool to release all resources.
func (p *pool) Close() {
	p.mu.Lock()
	if p.mu.closed {
		p.mu.Unlock()
		return
	}
	p.mu.closed = true
	close(p.resources)
	p.mu.Unlock()

	for r := range p.resources {
		r.Close()
	}
}
