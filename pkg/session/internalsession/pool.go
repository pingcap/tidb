package internalsession

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/util/intest"
)

type PoolFactory func() (SessionContext, error)

// Pool is a recyclable resource pool for the session.
type Pool struct {
	noopContextOwnerHook
	capacity  int
	resources chan *session
	factory   PoolFactory
	mu        struct {
		sync.RWMutex
		closed bool
	}
}

// NewPool creates a new session pool with the given capacity and factory function.
func NewPool(capacity int, factory PoolFactory) *Pool {
	return &Pool{
		resources: make(chan *session, capacity),
		factory:   factory,
	}
}

func (p *Pool) getInternal() (*session, error) {
	select {
	case internal, ok := <-p.resources:
		if !ok {
			return nil, errors.New("session pool closed")
		}
		return internal, nil
	default:
		resource, err := p.factory()
		if err != nil {
			return nil, err
		}
		return newInternalSession(resource, p), nil
	}
}

// Get gets a session from the session pool.
func (p *Pool) Get() (*Session, error) {
	internal, err := p.getInternal()
	if err != nil {
		return nil, err
	}

	success := false
	defer func() {
		if !success {
			internal.Destroy()
		}
	}()

	if err = internal.RollbackTxn(context.Background(), p); err != nil {
		intest.AssertNoError(err)
		return nil, err
	}

	se := &Session{internal: internal}
	if err = internal.TransferOwner(p, se); err != nil {
		return nil, err
	}

	success = true
	return se, nil
}

// Put puts the session back to the pool.
func (p *Pool) Put(se *Session) {
	intest.AssertNotNil(se)
	if se == nil {
		return
	}

	internal := se.internal
	intest.AssertNotNil(internal)
	if internal == nil {
		return
	}

	internal.AssertTxnCommitted()
	// TransferOwner from the session to the pool first.
	// If `Put` is called concurrently with the same `Session`, only one `TransferOwner` will succeed.
	// That avoids a same session put back to the pool multiple times.
	if err := internal.TransferOwner(se, p); err != nil {
		// When error happens, it may be below reasons:
		// - The session is still in use.
		// - The session is already destroyed because `Session.Destroy` has already bee called before put back.
		// - Concurrency call of `Put` that makes the owner has already been transferred to the pool
		// (or then transferred to another Session).
		// We should call `internal.OwnerDestroy` instead of `internal.Destroy` to avoid destroying the session
		// that has been obtained by another valid `Session`.
		internal.OwnerDestroy(se)
		terror.Log(err)
		return
	}

	success := false
	defer func() {
		if !success {
			internal.Destroy()
		}
	}()

	if internal.MayCorrupted() {
		return
	}

	if err := internal.RollbackTxn(context.Background(), p); err != nil {
		// should not happen
		terror.Log(err)
		intest.AssertNoError(err)
	}

	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.mu.closed {
		return
	}

	select {
	case p.resources <- internal:
		success = true
	default:
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
	close(p.resources)
	p.mu.Unlock()

	for r := range p.resources {
		r.Destroy()
	}
}

func WithSession(pool *Pool, fn func(*Session) error) error {
	se, err := pool.Get()
	if err != nil {
		return err
	}

	success := false
	defer func() {
		if success {
			pool.Put(se)
		} else {
			se.Destroy()
		}
	}()

	if err = fn(se); err != nil {
		return err
	}

	success = true
	return nil
}

func WithSessionContext(pool *Pool, fn func(SessionContext) error) error {
	return WithSession(pool, func(se *Session) error {
		return se.WithContext(fn)
	})
}
