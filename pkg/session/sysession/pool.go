// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sysession

import (
	"context"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

const PoolMaxSize int = 1024 * 1024 * 1024

// Factory is a function to create a new session context
type Factory func() (SessionContext, error)

// Pool is a recyclable resource pool for the system internal session.
type Pool struct {
	noopOwnerHook
	pool    chan *session
	factory Factory
	mu      struct {
		sync.RWMutex
		closed bool
	}
}

// NewPool creates a new session pool with the given capacity and factory function.
func NewPool(capacity int, factory Factory) *Pool {
	intest.AssertNotNil(factory)
	if capacity < 0 || capacity > PoolMaxSize {
		intest.Assert(false, "invalid capacity: %d", capacity)
		capacity = PoolMaxSize
	}

	return &Pool{
		pool:    make(chan *session, capacity),
		factory: factory,
	}
}

func (p *Pool) getInternal() (s *session, _ error) {
	select {
	case r, ok := <-p.pool:
		if !ok {
			return nil, errors.New("session pool closed")
		}
		return r, nil
	default:
		// the pool is empty, continue to create a new session
	}

	sctx, err := p.factory()
	if err != nil {
		return nil, err
	}

	defer func() {
		if s == nil {
			// s == nil means the internal session is not successfully created.
			// close it instead.
			sctx.Close()
		}
	}()

	return newInternalSession(sctx, p)
}

// Get gets a session from the session pool.
func (p *Pool) Get() (*Session, error) {
	internal, err := p.getInternal()
	if err != nil {
		return nil, err
	}

	se := &Session{}
	defer func() {
		if se.internal != internal {
			// se.internal != internal means the session is not successfully created.
			// We need to close the internal session
			internal.Close()
		}
	}()

	if err = internal.OwnerResetState(context.Background(), p); err != nil {
		return nil, err
	}

	if err = internal.TransferOwner(p, se); err != nil {
		return nil, err
	}

	se.internal = internal
	return se, nil
}

// Put puts the session back to the pool.
func (p *Pool) Put(se *Session) {
	if se == nil || se.internal == nil || se.internal.Owner() != se {
		// Do nothing when the input session is invalid.
		return
	}

	internal := se.internal
	if internal.AvoidReuse() {
		// If the internal session is marked as avoid-reuse, we should close it directly.
		// Notice that we should not call `internal.Close` to make sure only close the internal session when its owner
		// is the current session.
		se.Close()
	}

	// We should transfer the owner back to the pool first for reasons:
	// 1. Make sure the input Session is valid by checking the internal session's owner.
	// 2. After ownership is transferred back to pool, we can ensure only the pool can access the internal session.
	if err := internal.TransferOwner(se, p); err != nil {
		// Notice that we should not call `internal.Close` to make sure only close the internal session when its owner
		// is the current session.
		se.Close()
		logutil.BgLogger().Warn("TransferOwner failed when put back a session", zap.Error(err))
		return
	}

	returned := false
	defer func() {
		if !returned {
			internal.Close()
		}
	}()

	if err := internal.OwnerResetState(context.Background(), p); err != nil {
		logutil.BgLogger().Warn("OwnerResetState failed when put back a session", zap.Error(err))
		return
	}

	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.mu.closed {
		logutil.BgLogger().Info("session pool closed, close the session directly")
		return
	}

	select {
	case p.pool <- internal:
		returned = true
	default:
		internal.Close()
	}
}

// Close closes the pool to release all resources.
func (p *Pool) Close() {
	p.mu.Lock()
	if p.mu.closed {
		p.mu.Unlock()
		return
	}

	p.mu.closed = true
	close(p.pool)
	p.mu.Unlock()

	for r := range p.pool {
		r.Close()
	}
}
