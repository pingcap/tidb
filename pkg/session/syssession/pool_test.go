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
	"sync/atomic"
	"testing"

	"github.com/pingcap/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockSessionFactory struct {
	mock.Mock
}

func (f *mockSessionFactory) create() (SessionContext, error) {
	args := f.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(SessionContext), args.Error(1)
}

func TestNewSessionPool(t *testing.T) {
	factory := func() (SessionContext, error) {
		return &mockSessionContext{}, nil
	}

	p := NewPool(128, factory)
	require.NotNil(t, p)
	require.Equal(t, 128, cap(p.pool))
	require.Equal(t, 0, len(p.pool))
	require.False(t, p.IsClosed())
	require.NotNil(t, p.ctx)
	require.NoError(t, p.ctx.Err())

	// pool with PoolMaxSize
	p = NewPool(PoolMaxSize, factory)
	require.Equal(t, PoolMaxSize, cap(p.pool))
	require.False(t, p.IsClosed())

	// pool with zero-size, it means always return a new session
	p = NewPool(0, factory)
	require.Equal(t, 0, cap(p.pool))
	require.False(t, p.IsClosed())

	// test pool size limit
	WithSuppressAssert(func() {
		p = NewPool(PoolMaxSize+1, factory)
		require.Equal(t, PoolMaxSize, cap(p.pool))
		require.False(t, p.IsClosed())
	})

	WithSuppressAssert(func() {
		p = NewPool(-1, factory)
		require.Equal(t, PoolMaxSize, cap(p.pool))
		require.False(t, p.IsClosed())
	})
}

func TestSessionPoolGet(t *testing.T) {
	mockFactory := &mockSessionFactory{}
	p := NewPool(128, mockFactory.create)

	// get a new Session from pool
	sctx := &mockSessionContext{}
	mockFactory.On("create").Return(sctx, nil).Once()
	sctx.On("StoreInternalSession", sctx).Once()
	se, err := p.Get()
	require.NoError(t, err)
	require.Same(t, se, se.internal.Owner())
	require.False(t, se.internal.IsClosed())
	require.Zero(t, se.internal.Inuse())
	mockFactory.AssertExpectations(t)
	sctx.AssertExpectations(t)

	// reuse the session
	sctx.On("DeleteInternalSession", sctx).Once()
	sctx.MockNoPendingTxn()
	sctx.MockResetState(p.ctx, "")
	p.Put(se)
	require.Equal(t, 1, len(p.pool))
	sctx.AssertExpectations(t)
	sctx.On("StoreInternalSession", sctx).Once()
	se2, err := p.Get()
	require.NoError(t, err)
	require.NotSame(t, se, se2)
	require.Equal(t, 0, len(p.pool))
	require.Same(t, se.internal, se2.internal)
	require.Same(t, se2, se2.internal.Owner())
	require.False(t, se2.internal.IsClosed())
	require.Zero(t, se2.internal.Inuse())
	mockFactory.AssertExpectations(t)
	sctx.AssertExpectations(t)

	// factory returns error
	mockFactory.On("create").Return(nil, errors.New("mockErr")).Once()
	se, err = p.Get()
	require.EqualError(t, err, "mockErr")
	require.Nil(t, se)
	mockFactory.AssertExpectations(t)

	// hook panics
	mockFactory.On("create").Return(sctx, nil).Once()
	sctx.On("StoreInternalSession", sctx).Panic("mockPanic").Once()
	sctx.On("Close").Once()
	require.PanicsWithValue(t, "mockPanic", func() {
		_, _ = p.Get()
	})
	mockFactory.AssertExpectations(t)
	sctx.AssertExpectations(t)

	// get session from a closed pool
	p.Close()
	se, err = p.Get()
	require.EqualError(t, err, "session pool closed")
	require.Nil(t, se)
}

func TestSessionPoolPut(t *testing.T) {
	mockFactory := &mockSessionFactory{}
	poolCap := 4
	p := NewPool(poolCap, mockFactory.create)
	require.Equal(t, 4, cap(p.pool))
	// Put invalid Session
	p.Put(nil)
	p.Put(&Session{})
	require.Equal(t, 0, len(p.pool))

	getCachedSessionFromPool := func(sctx *mockSessionContext) *Session {
		sctx.On("StoreInternalSession", sctx).Once()
		se, err := p.Get()
		require.NoError(t, err)
		require.Same(t, se, se.internal.Owner())
		require.True(t, se.IsOwner())
		mockFactory.AssertExpectations(t)
		sctx.AssertExpectations(t)
		return se
	}

	getNewSessionFromPool := func(sctx *mockSessionContext) *Session {
		mockFactory.On("create").Return(sctx, nil).Once()
		sctx.On("StoreInternalSession", sctx).Once()
		se, err := p.Get()
		require.NoError(t, err)
		require.Same(t, se, se.internal.Owner())
		require.True(t, se.IsOwner())
		mockFactory.AssertExpectations(t)
		sctx.AssertExpectations(t)
		return se
	}

	// Put a normal session
	sctx := &mockSessionContext{}
	se := getNewSessionFromPool(sctx)
	sctx.On("DeleteInternalSession", sctx).Once()
	sctx.MockResetState(p.ctx, "")
	sctx.MockNoPendingTxn()
	p.Put(se)
	require.Same(t, p, se.internal.Owner())
	require.False(t, se.IsOwner())
	require.False(t, se.IsInternalClosed())
	mockFactory.AssertExpectations(t)
	sctx.AssertExpectations(t)
	require.Equal(t, 1, len(p.pool))

	// Get a cached Session and put the old one that is not the owner
	se2 := getCachedSessionFromPool(sctx)
	require.Equal(t, 0, len(p.pool))
	p.Put(se)
	require.Equal(t, 0, len(p.pool))

	// Put a Session that is the owner
	sctx.On("DeleteInternalSession", sctx).Once()
	sctx.MockNoPendingTxn()
	sctx.MockResetState(p.ctx, "")
	p.Put(se2)
	require.Same(t, p, se2.internal.Owner())
	mockFactory.AssertExpectations(t)
	sctx.AssertExpectations(t)
	require.Equal(t, 1, len(p.pool))

	// Put a Session again takes no effect
	p.Put(se2)
	require.Equal(t, 1, len(p.pool))
	require.Same(t, p, se2.internal.Owner())

	// Put a Session that is inuse
	se = getCachedSessionFromPool(sctx)
	require.Equal(t, 0, len(p.pool))
	_, _, err := se.internal.EnterOperation(se)
	require.NoError(t, err)
	sctx.On("DeleteInternalSession", sctx).Once()
	sctx.On("Close").Once()
	WithSuppressAssert(func() {
		p.Put(se)
	})
	require.True(t, se.internal.IsClosed())
	require.False(t, se.IsOwner())
	require.True(t, se.IsInternalClosed())
	require.Equal(t, 0, len(p.pool))
	sctx.AssertExpectations(t)

	// Put a Session that avoids reusing
	se = getNewSessionFromPool(sctx)
	require.Equal(t, 0, len(p.pool))
	se.internal.MarkAvoidReuse()
	sctx.On("DeleteInternalSession", sctx).Once()
	sctx.On("Close").Once()
	p.Put(se)
	require.True(t, se.internal.IsClosed())
	require.Equal(t, 0, len(p.pool))
	sctx.AssertExpectations(t)

	// Put a Session that has pending txn
	se = getNewSessionFromPool(sctx)
	sctx.On("DeleteInternalSession", sctx).Once()
	sctx.On("GetPreparedTxnFuture").Return(&mockPreparedFuture{}).Once()
	sctx.On("Close").Once()
	WithSuppressAssert(func() {
		p.Put(se)
	})
	require.True(t, se.internal.IsClosed())
	require.Equal(t, 0, len(p.pool))
	sctx.AssertExpectations(t)

	// Put a Session but `CheckPendingTxn` panics
	se = getNewSessionFromPool(sctx)
	sctx.On("DeleteInternalSession", sctx).Once()
	sctx.On("GetPreparedTxnFuture").Panic("txnFuturePanic").Once()
	sctx.On("Close").Once()
	WithSuppressAssert(func() {
		require.PanicsWithValue(t, "txnFuturePanic", func() {
			p.Put(se)
		})
	})
	require.True(t, se.internal.IsClosed())
	require.Equal(t, 0, len(p.pool))
	sctx.AssertExpectations(t)

	// Put a Session but `OwnerResetState` panics
	se = getNewSessionFromPool(sctx)
	sctx.On("DeleteInternalSession", sctx).Once()
	sctx.MockNoPendingTxn()
	sctx.MockResetState(p.ctx, "resetStatePanic")
	sctx.On("Close").Once()
	WithSuppressAssert(func() {
		require.PanicsWithValue(t, "resetStatePanic", func() {
			p.Put(se)
		})
	})
	require.True(t, se.internal.IsClosed())
	require.Equal(t, 0, len(p.pool))
	sctx.AssertExpectations(t)

	// Put a closed session
	se = getNewSessionFromPool(sctx)
	require.Equal(t, 0, len(p.pool))
	require.False(t, se.internal.IsClosed())
	sctx.On("DeleteInternalSession", sctx).Once()
	sctx.On("Close").Once()
	se.Close()
	require.True(t, se.internal.IsClosed())
	p.Put(se)
	require.Equal(t, 0, len(p.pool))
	sctx.AssertExpectations(t)

	// onResignOwner panics
	se = getNewSessionFromPool(sctx)
	sctx.On("DeleteInternalSession", sctx).Panic("mockPanic").Once()
	sctx.On("Close").Once()
	require.PanicsWithValue(t, "mockPanic", func() {
		p.Put(se)
	})
	require.Equal(t, 0, len(p.pool))
	require.True(t, se.internal.IsClosed())
	sctx.AssertExpectations(t)

	// put a full pool
	sessions := make([]*Session, poolCap+2)
	for i := 0; i <= poolCap+1; i++ {
		sctx = &mockSessionContext{}
		se = getNewSessionFromPool(sctx)
		sessions[i] = se
	}

	for i := 0; i < poolCap; i++ {
		require.Equal(t, i, len(p.pool))
		sctx = sessions[i].internal.sctx.(*mockSessionContext)
		sctx.On("DeleteInternalSession", sctx).Once()
		sctx.MockNoPendingTxn()
		sctx.MockResetState(p.ctx, "")
		p.Put(sessions[i])
		require.Equal(t, i+1, len(p.pool))
		require.Same(t, p, sessions[i].internal.Owner())
		sctx.AssertExpectations(t)
	}

	se = sessions[poolCap]
	sctx = se.internal.sctx.(*mockSessionContext)
	sctx.On("DeleteInternalSession", sctx).Once()
	sctx.MockNoPendingTxn()
	sctx.MockResetState(p.ctx, "")
	sctx.On("Close").Once()
	p.Put(se)
	require.Equal(t, poolCap, len(p.pool))
	require.Nil(t, se.internal.Owner())
	require.True(t, se.internal.IsClosed())
	sctx.AssertExpectations(t)

	// put a closed pool
	for i := 0; i < poolCap; i++ {
		sctx = sessions[i].internal.sctx.(*mockSessionContext)
		sctx.On("Close").Once()
	}
	p.Close()
	require.True(t, p.IsClosed())
	require.Equal(t, 0, len(p.pool))
	for i := 0; i < poolCap; i++ {
		sctx = sessions[i].internal.sctx.(*mockSessionContext)
		sctx.AssertExpectations(t)
	}

	se = sessions[poolCap+1]
	sctx = se.internal.sctx.(*mockSessionContext)
	sctx.On("DeleteInternalSession", sctx).Once()
	sctx.MockNoPendingTxn()
	sctx.MockResetState(p.ctx, "")
	sctx.On("Close").Once()
	p.Put(se)
	require.Nil(t, se.internal.Owner())
	require.True(t, se.internal.IsClosed())
	sctx.AssertExpectations(t)
}

func TestSessionPoolWithSession(t *testing.T) {
	factory := &mockSessionFactory{}
	capacity := 8
	sctx := &mockSessionContext{}
	p := NewPool(capacity, factory.create)

	var called atomic.Bool
	fn := func(err error, panicS string) func(*Session) error {
		return func(se *Session) error {
			factory.AssertExpectations(t)
			sctx.AssertExpectations(t)
			require.Zero(t, len(p.pool))
			require.True(t, called.CompareAndSwap(false, true))
			sctx.On("DeleteInternalSession", sctx).Once()
			sctx.MockNoPendingTxn()
			sctx.MockResetState(p.ctx, "")
			if panicS != "" {
				panic(panicS)
			}
			return err
		}
	}

	// success case
	require.Zero(t, len(p.pool))
	factory.On("create").Return(sctx, nil).Once()
	sctx.On("StoreInternalSession", sctx).Once()
	err := p.WithSession(fn(nil, ""))
	require.Nil(t, err)
	require.True(t, called.CompareAndSwap(true, false))
	sctx.AssertExpectations(t)

	// error case
	require.Equal(t, 1, len(p.pool))
	sctx.On("StoreInternalSession", sctx).Once()
	err = p.WithSession(fn(errors.New("mockErr1"), ""))
	require.EqualError(t, err, "mockErr1")
	require.True(t, called.CompareAndSwap(true, false))
	sctx.AssertExpectations(t)

	// panic case
	require.Equal(t, 1, len(p.pool))
	sctx.On("StoreInternalSession", sctx).Once()
	require.PanicsWithValue(t, "mockPanic1", func() {
		_ = p.WithSession(fn(nil, "mockPanic1"))
	})
	require.True(t, called.CompareAndSwap(true, false))
	sctx.AssertExpectations(t)

	// clear pool
	sctx.On("StoreInternalSession", sctx).Once()
	se, err := p.Get()
	require.NoError(t, err)
	require.Zero(t, len(p.pool))
	sctx.On("DeleteInternalSession", sctx).Once()
	sctx.On("Close").Once()
	se.Close()
	sctx.AssertExpectations(t)

	// p.Get returns error, the function should not be called
	factory.On("create").Return(nil, errors.New("mockErr2")).Once()
	err = p.WithSession(fn(nil, ""))
	require.EqualError(t, err, "mockErr2")
	factory.AssertExpectations(t)
	require.False(t, called.Load())
}

func TestSessionPoolClose(t *testing.T) {
	factory := &mockSessionFactory{}
	capacity := 8
	p := NewPool(capacity, factory.create)

	// make a pool with some sessions
	sctxs := make([]*mockSessionContext, capacity)
	ses := make([]*Session, capacity)
	for i := 0; i < capacity; i++ {
		sctx := &mockSessionContext{}
		sctxs[i] = sctx
		factory.On("create").Return(sctx, nil).Once()
		sctx.On("StoreInternalSession", sctx).Once()
		se, err := p.Get()
		require.NoError(t, err)
		ses[i] = se
		factory.AssertExpectations(t)
		sctx.AssertExpectations(t)
	}
	for i := 0; i < capacity; i++ {
		sctx := sctxs[i]
		se := ses[i]
		sctx.On("DeleteInternalSession", sctx).Once()
		sctx.MockNoPendingTxn()
		sctx.MockResetState(p.ctx, "")
		p.Put(se)
		sctx.AssertExpectations(t)
	}

	// close pool should close all sessions in it
	for i := 0; i < capacity; i++ {
		sctx := sctxs[i]
		sctx.On("Close").Once()
	}
	p.Close()
	require.True(t, p.IsClosed())
	require.Error(t, p.ctx.Err())
	require.Equal(t, 0, len(p.pool))
	select {
	case _, ok := <-p.pool:
		require.False(t, ok)
	default:
		require.FailNow(t, "pool is still active")
	}
	for i := 0; i < capacity; i++ {
		sctx := sctxs[i]
		sctx.AssertExpectations(t)
	}

	// close a closed pool should take no effect
	p.Close()
	require.True(t, p.IsClosed())
}
