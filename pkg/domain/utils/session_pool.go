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

package utils

import (
	"errors"
	"sync"

	"github.com/ngaut/pools"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// SessionPool is a recyclable resource pool for the session.
type SessionPool interface {
	Get() (pools.Resource, error)
	Put(pools.Resource)
	Close()
}

type pool struct {
	resources chan pools.Resource
	factory   pools.Factory
	mu        struct {
		sync.RWMutex
		closed bool
	}
}

// NewSessionPool creates a new session pool with the given capacity and factory function.
func NewSessionPool(capacity int, factory pools.Factory) SessionPool {
	return &pool{
		resources: make(chan pools.Resource, capacity),
		factory:   factory,
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

	if nil == err {
		_, ok = resource.(sessionctx.Context)
		intest.Assert(ok)
		infosync.StoreInternalSession(resource)
	}

	return
}

// Put puts the session back to the pool.
func (p *pool) Put(resource pools.Resource) {
	_, ok := resource.(sessionctx.Context)
	intest.Assert(ok)

	p.mu.RLock()
	defer p.mu.RUnlock()
	// Delete the internal session to the map of SessionManager
	infosync.DeleteInternalSession(resource)
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
