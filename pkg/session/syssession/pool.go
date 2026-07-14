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

package syssession

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// PoolMaxSize is the maximum size of the session pool.
const PoolMaxSize int = 1024 * 1024 * 1024

// Factory is a function to create a new session context
type Factory func() (SessionContext, error)

// Pool is an interface for system internal session pool.
type Pool interface {
	// Get gets a session from the session pool.
	Get() (*Session, error)
	// Put puts the session back to the pool.
	Put(*Session)
	// WithForceBlockGCSession executes the input function with the session and ensures the session is registered to
	// the session manager so GC can be blocked safely.
	WithForceBlockGCSession(ctx context.Context, fn func(*Session) error) error
	// WithSession executes the input function with the session.
	// After the function called, the session will be returned to the pool automatically.
	WithSession(func(*Session) error) error
}

// AdvancedSessionPool is a recyclable resource pool for the system internal session.
type AdvancedSessionPool struct {
	noopOwnerHook
	ctx     context.Context
	pool    chan *session
	factory Factory
	mu      struct {
		sync.RWMutex
		closed bool
		cancel context.CancelFunc
	}
}

// NewAdvancedSessionPool creates a default session pool with the given capacity and factory function.
func NewAdvancedSessionPool(capacity int, factory Factory) *AdvancedSessionPool {
	intest.AssertNotNil(factory)
	if capacity <= 0 || capacity > PoolMaxSize {
		intest.Assert(suppressAssertInTest, "invalid capacity: %d", capacity)
		capacity = PoolMaxSize
	}

	ctx, cancel := context.WithCancel(context.Background())
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnOthers)
	pool := &AdvancedSessionPool{
		ctx:     ctx,
		pool:    make(chan *session, capacity),
		factory: factory,
	}
	pool.mu.cancel = cancel
	return pool
}

func (p *AdvancedSessionPool) getInternal() (s *session, _ error) {
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
		return nil, errors.Trace(err)
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
func (p *AdvancedSessionPool) Get() (*Session, error) {
	internal, err := p.getInternal()
	if err != nil {
		return nil, err
	}

	intest.AssertFunc(func() bool {
		intest.AssertNotNil(internal)
		intest.Assert(internal.Owner() == p)
		intest.Assert(!internal.IsAvoidReuse())
		intest.Assert(!internal.IsClosed())
		return true
	})

	se := &Session{}
	defer func() {
		if se.internal != internal {
			// se.internal != internal means the session is not successfully created.
			// We need to close the internal session
			internal.Close()
		}
	}()

	if err = internal.TransferOwner(p, se); err != nil {
		return nil, err
	}

	se.internal = internal
	return se, nil
}

// Put puts the session back to the pool.
// After the session is put back to the pool, the owner of the internal session will transfer to pool.
func (p *AdvancedSessionPool) Put(se *Session) {
	if se == nil {
		intest.Assert(suppressAssertInTest)
		return
	}

	if se.internal == nil {
		intest.Assert(suppressAssertInTest)
		return
	}

	if se.internal.Owner() != se {
		// This is regarded as normal of the below cases and do nothing when the Session is no longer the owner.
		// 1. `p.Put(se)` is called more than once and the internal session already in the pool (internal.Owner() == p).
		// 2. `se` is closed and then `p.Put(se)` is called (internal.Owner() == nil).
		// 3. `p.Put(se)` is called, and the internal session is obtained by another session.
		//    Then Calling `p.Put(se)` again with the old Session (internal.Owner() != anotherSe).
		return
	}

	internal := se.internal
	// We should transfer the owner back to the pool first for reasons:
	// 1. Make sure the input Session is valid by checking the internal session's owner.
	// 2. After ownership is transferred back to pool, we can ensure only the pool can access the internal session.
	if err := internal.TransferOwner(se, p); err != nil {
		// Use `se.Close()` instead of `se.internal.Close()` because the former will close the internal session only
		// when it is the owner.
		// Consider the below case that Put is called concurrently:
		// 1. goroutine 1: In `p.Put(se)`, before `internal.TransferOwner(se, p)`
		// 2. goroutine 2: Call `p.Put(se)` with the same `Session`, the internal session is put back to the pool.
		// 3. goroutine 2: Call `p.Get()` and get a new `Session` with the same internal session with goroutine 1.
		// 4. goroutine 1: Call `internal.TransferOwner(se, p)` and failed, then it should close the session.
		// If we use `se.internal.Close()` in step4, the `Session` got in step3 will be closed unexpectedly.
		logutil.BgLogger().Error(
			"TransferOwner failed when put back a session",
			zap.String("sctx", objectStr(internal.sctx)),
			zap.Error(err),
			zap.Stack("stack"),
		)
		se.Close()
		intest.Assert(suppressAssertInTest)
		return
	}

	returned := false
	defer func() {
		if !returned {
			internal.Close()
		}
	}()

	if internal.IsAvoidReuse() {
		// If the internal session is marked as avoid-reuse, we should close it directly.
		// Notice that we should not call `internal.Close` to make sure only close the internal session when its owner
		// is the current session.
		logutil.BgLogger().Info(
			"the Session is marked as avoid-reusing when put back, close it instead",
			zap.String("sctx", objectStr(internal.sctx)),
		)
		return
	}

	if err := internal.CheckNoPendingTxn(); err != nil {
		// If the session has an unterminated transaction, it should close it instead of put back it to the pool
		// to avoid some potential issues.
		logutil.BgLogger().Error(
			"pending txn found when put back, close it instead to avoid undetermined state",
			zap.String("sctx", objectStr(internal.sctx)),
			zap.Error(err),
			zap.Stack("stack"),
		)
		intest.Assert(suppressAssertInTest)
		return
	}

	// for safety, still reset the inner state to make session clean
	if err := internal.OwnerResetState(p.ctx, p); err != nil {
		logutil.BgLogger().Error(
			"OwnerResetState failed when put back, close it instead to avoid undetermined state",
			zap.String("sctx", objectStr(internal.sctx)),
			zap.Error(err),
			zap.Stack("stack"),
		)
		intest.Assert(suppressAssertInTest)
		return
	}

	intest.AssertFunc(func() bool {
		intest.Assert(internal.Owner() == p)
		intest.Assert(!internal.IsAvoidReuse())
		intest.Assert(!internal.IsClosed())
		return true
	})

	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.mu.closed {
		logutil.BgLogger().Info("session pool closed, close the session directly")
		return
	}

	// Please notice that we should make sure `p.pool <- internal` is protected by the mutex.
	// Consider a case the `p.Put` and `p.Close` is called concurrently, without the mutex protection, the internal
	// channel may be closed by `p.Close` after checking `p.mu.closed` is false, and then `p.pool <- internal` will
	// panic.
	select {
	case p.pool <- internal:
		returned = true
	default:
		// That means the pool is full now, and the session will then be closed in the defer function.
	}
}

// WithSession executes the input function with the session.
// After the function called, the session will be returned to the pool automatically.
func (p *AdvancedSessionPool) WithSession(fn func(*Session) error) error {
	se, err := p.Get()
	if err != nil {
		return err
	}

	success := false
	defer func() {
		if success {
			p.Put(se)
		} else {
			se.Close()
		}
	}()

	if err = fn(se); err != nil {
		return err
	}
	success = true
	return nil
}

// WithForceBlockGCSession executes the input function with the session and ensures the internal session is
// registered to the session manager so GC can be blocked safely.
func (p *AdvancedSessionPool) WithForceBlockGCSession(ctx context.Context, fn func(*Session) error) error {
	se, err := p.Get()
	if err != nil {
		return err
	}

	success := false
	defer func() {
		if success {
			p.Put(se)
		} else {
			se.Close()
		}
	}()

	// Make sure the internal session is registered to the session manager to block GC.
	const retryInterval = 100 * time.Millisecond
	if !infosync.ContainsInternalSession(se.internal.sctx) {
		for !infosync.StoreInternalSession(se.internal.sctx) {
			// In most cases, the session manager is not set, so this step will be skipped.
			// It is only enabled explicitly in tests through a failpoint.
			if intest.InTest {
				forceBlockGCInTest := false
				failpoint.Inject("ForceBlockGCInTest", func(val failpoint.Value) {
					forceBlockGCInTest = val.(bool)
				})
				if !forceBlockGCInTest {
					break
				}
			}

			// Check context cancellation
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(retryInterval):
				// Continue retry
			}
		}
	}

	if err = fn(se); err != nil {
		return err
	}
	success = true
	return nil
}

// Close closes the pool to release all resources.
func (p *AdvancedSessionPool) Close() {
	p.mu.Lock()
	if p.mu.closed {
		p.mu.Unlock()
		return
	}

	p.mu.closed = true
	close(p.pool)
	p.mu.cancel()
	p.mu.Unlock()

	for r := range p.pool {
		r.Close()
	}
}

// IsClosed returns whether the pool is closed
func (p *AdvancedSessionPool) IsClosed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.mu.closed
}
