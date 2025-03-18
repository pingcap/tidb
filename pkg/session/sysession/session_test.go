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
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func withSuppressAssert(fn func()) {
	defer func() {
		suppressAssertInTest = false
	}()
	suppressAssertInTest = true
	fn()
}

type mockOwner struct {
	sessionOwner
	mock.Mock
}

func (m *mockOwner) onBecameOwner(sctx SessionContext) error {
	return m.Called(sctx).Error(0)
}

func (m *mockOwner) onResignOwner(sctx SessionContext) error {
	return m.Called(sctx).Error(0)
}

type mockSessionContext struct {
	sessionctx.Context
	mock.Mock
}

func (m *mockSessionContext) Close() {
	m.Called()
}

func TestNewInternalSession(t *testing.T) {
	sctx := &mockSessionContext{}
	owner := &mockOwner{}

	// newInternalSession success case
	owner.On("onBecameOwner", sctx).Return(nil).Once()
	se, err := newInternalSession(sctx, owner)
	require.NoError(t, err)
	require.NotNil(t, se)
	require.Same(t, owner, se.owner)
	require.Same(t, sctx, se.sctx)
	require.Zero(t, se.inuse)
	require.False(t, se.avoidReuse)
	owner.AssertExpectations(t)

	// onBecameOwner returns an error
	mockErr := errors.New("mockOnBecameOwnerErr")
	owner.On("onBecameOwner", sctx).Return(mockErr).Once()
	se, err = newInternalSession(sctx, owner)
	require.EqualError(t, err, mockErr.Error())
	require.Nil(t, se)
	owner.AssertExpectations(t)
}

func mockInternalSession(t *testing.T, sctx *mockSessionContext, owner *mockOwner) *session {
	owner.On("onBecameOwner", sctx).Return(nil).Once()
	se, err := newInternalSession(sctx, owner)
	require.NoError(t, err)
	require.Same(t, owner, se.Owner())
	require.False(t, se.IsClosed())
	owner.AssertExpectations(t)
	return se
}

func TestInternalSessionTransferOwner(t *testing.T) {
	sctx := &mockSessionContext{}
	// TransferOwner success
	owner1 := &mockOwner{}
	se := mockInternalSession(t, sctx, owner1)
	owner2 := &mockOwner{}
	owner1.On("onResignOwner", sctx).Return(nil).Once()
	owner2.On("onBecameOwner", sctx).Return(nil).Once()
	require.NoError(t, se.TransferOwner(owner1, owner2))
	require.Same(t, owner2, se.Owner())
	owner1.AssertExpectations(t)
	owner2.AssertExpectations(t)

	owner3 := &mockOwner{}
	// transfer from an invalid owner should fail
	require.Error(t, se.TransferOwner(owner1, owner3))
	require.Same(t, owner2, se.Owner())
	require.Error(t, se.TransferOwner(owner1, owner2))
	require.Same(t, owner2, se.Owner())

	// transfer to the same owner should take no effect
	require.NoError(t, se.TransferOwner(owner2, owner2))
	require.Same(t, owner2, se.Owner())

	// TransferOwner from=nil should fail
	require.Error(t, se.TransferOwner(nil, owner3))
	require.Same(t, owner2, se.Owner())

	// TransferOwner to=nil should fail
	require.Error(t, se.TransferOwner(owner2, nil))
	require.Same(t, owner2, se.Owner())

	// TransferOwner from=to=nil should fail
	require.Error(t, se.TransferOwner(nil, nil))
	require.Same(t, owner2, se.Owner())
	require.False(t, se.IsClosed())

	// onResignOwner returns an error and should close the session
	mockErr := errors.New("mockOnResignOwnerErr")
	owner2.On("onResignOwner", sctx).Return(mockErr).Once()
	sctx.On("Close").Once()
	require.EqualError(t, se.TransferOwner(owner2, owner1), mockErr.Error())
	require.Nil(t, se.Owner())
	require.True(t, se.IsClosed())
	owner2.AssertExpectations(t)
	sctx.AssertExpectations(t)

	// onResignOwner panics should close the session
	se = mockInternalSession(t, sctx, owner2)
	owner2.On("onResignOwner", sctx).Panic("mock panic1").Once()
	sctx.On("Close").Once()
	require.PanicsWithValue(t, "mock panic1", func() {
		_ = se.TransferOwner(owner2, owner3)
	})
	require.Nil(t, se.Owner())
	require.True(t, se.IsClosed())
	owner2.AssertExpectations(t)
	sctx.AssertExpectations(t)

	// onBecameOwner returns an error and should close the session
	se = mockInternalSession(t, sctx, owner2)
	mockErr = errors.New("mockOnBecameOwner")
	owner2.On("onResignOwner", sctx).Return(nil).Once()
	owner3.On("onBecameOwner", sctx).Return(mockErr).Once()
	sctx.On("Close").Once()
	require.EqualError(t, se.TransferOwner(owner2, owner3), mockErr.Error())
	require.Nil(t, se.Owner())
	require.True(t, se.IsClosed())
	owner2.AssertExpectations(t)
	owner3.AssertExpectations(t)
	sctx.AssertExpectations(t)

	// onBecameOwner panics should close the session
	se = mockInternalSession(t, sctx, owner2)
	mockErr = errors.New("mockOnBecameOwner")
	owner2.On("onResignOwner", sctx).Return(nil).Once()
	owner3.On("onBecameOwner", sctx).Panic("mock panic2").Once()
	sctx.On("Close").Once()
	require.PanicsWithValue(t, "mock panic2", func() {
		_ = se.TransferOwner(owner2, owner3)
	})
	require.Nil(t, se.Owner())
	require.True(t, se.IsClosed())
	owner2.AssertExpectations(t)
	owner3.AssertExpectations(t)
	sctx.AssertExpectations(t)

	// close panics
	se = mockInternalSession(t, sctx, owner2)
	owner2.On("onResignOwner", sctx).Return(nil).Once()
	owner3.On("onBecameOwner", sctx).Return(mockErr).Once()
	sctx.On("Close").Panic("close panic").Once()
	require.PanicsWithValue(t, "close panic", func() {
		_ = se.TransferOwner(owner2, owner3)
	})
	require.Nil(t, se.Owner())
	require.True(t, se.IsClosed())
	owner2.AssertExpectations(t)
	owner3.AssertExpectations(t)
	sctx.AssertExpectations(t)

	// A closed session should not transfer the owner again
	require.Error(t, se.TransferOwner(nil, owner1))
	require.Nil(t, se.Owner())
	require.True(t, se.IsClosed())
	require.Error(t, se.TransferOwner(owner2, owner1))
	require.Nil(t, se.Owner())
	require.True(t, se.IsClosed())
	require.Error(t, se.TransferOwner(nil, nil))
	require.Nil(t, se.Owner())
	require.True(t, se.IsClosed())

	// inuse session should not transfer the owner
	se = mockInternalSession(t, sctx, owner1)
	_, exit, err := se.EnterOperation(owner1)
	require.NoError(t, err)
	require.Equal(t, uint64(1), se.Inuse())
	require.EqualError(t, se.TransferOwner(owner1, owner2), "session is still inuse: 1")
	require.Same(t, owner1, se.Owner())
	require.False(t, se.IsClosed())

	// after exit, the session can be transferred again
	exit()
	owner1.On("onResignOwner", sctx).Return(nil).Once()
	owner2.On("onBecameOwner", sctx).Return(nil).Once()
	require.NoError(t, se.TransferOwner(owner1, owner2))
	require.Same(t, owner2, se.Owner())
	require.False(t, se.IsClosed())
	owner1.AssertExpectations(t)
	owner2.AssertExpectations(t)

	// avoid reuse session should not transfer the owner
	require.False(t, se.AvoidReuse())
	se.MarkAvoidReuse()
	require.True(t, se.AvoidReuse())
	require.EqualError(t, se.TransferOwner(owner2, owner1), "session is avoided to be reused by the new owner")
	require.Same(t, owner2, se.Owner())
	require.False(t, se.IsClosed())
}

func TestInternalSessionClose(t *testing.T) {
	sctx := &mockSessionContext{}
	owner := &mockOwner{}
	se := mockInternalSession(t, sctx, owner)
	require.False(t, se.IsClosed())
	require.Same(t, owner, se.Owner())

	// Close with a nil owner
	se.OwnerClose(nil)
	require.False(t, se.IsClosed())
	require.Same(t, owner, se.Owner())

	// Close with an invalid owner
	owner2 := &mockOwner{}
	se.OwnerClose(owner2)
	require.False(t, se.IsClosed())
	require.Same(t, owner, se.Owner())

	// Close with the current owner
	owner.On("onResignOwner", sctx).Return(nil).Once()
	sctx.On("Close").Once()
	se.OwnerClose(owner)
	require.True(t, se.IsClosed())
	require.Nil(t, se.Owner())
	owner.AssertExpectations(t)
	sctx.AssertExpectations(t)

	// Close a closed session
	se.OwnerClose(owner)
	se.OwnerClose(nil)
	se.OwnerClose(owner2)
	require.True(t, se.IsClosed())
	require.Nil(t, se.Owner())

	// Close without an owner
	se = mockInternalSession(t, sctx, owner)
	owner.On("onResignOwner", sctx).Return(nil).Once()
	sctx.On("Close").Once()
	se.Close()
	require.Nil(t, se.Owner())
	require.True(t, se.IsClosed())

	// Close after close
	se.Close()
	require.Nil(t, se.Owner())
	require.True(t, se.IsClosed())

	// test with error when close
	testWithErrorMock := func(closeFn func(se *session, owner *mockOwner)) {
		// onResignOwner failed should also close the context
		se = mockInternalSession(t, sctx, owner)
		owner.On("onResignOwner", sctx).Return(errors.New("mockErr1")).Once()
		sctx.On("Close").Once()
		withSuppressAssert(func() {
			closeFn(se, owner)
		})
		require.True(t, se.IsClosed())
		require.Nil(t, se.Owner())
		owner.AssertExpectations(t)
		sctx.AssertExpectations(t)

		// onResignOwner panics should also close the context
		se = mockInternalSession(t, sctx, owner)
		owner.On("onResignOwner", sctx).Panic("panic1").Once()
		sctx.On("Close").Once()
		require.PanicsWithValue(t, "panic1", func() {
			closeFn(se, owner)
		})
		require.True(t, se.IsClosed())
		require.Nil(t, se.Owner())
		owner.AssertExpectations(t)
		sctx.AssertExpectations(t)

		// context.Close() panics
		se = mockInternalSession(t, sctx, owner)
		owner.On("onResignOwner", sctx).Return(nil).Once()
		sctx.On("Close").Panic("panic2").Once()
		require.PanicsWithValue(t, "panic2", func() {
			closeFn(se, owner)
		})
		require.True(t, se.IsClosed())
		require.Nil(t, se.Owner())
		owner.AssertExpectations(t)
		sctx.AssertExpectations(t)

		// inuse > 0 when close
		se = mockInternalSession(t, sctx, owner)
		_, exit, err := se.EnterOperation(owner)
		require.NoError(t, err)
		require.Equal(t, uint64(1), se.Inuse())
		owner.On("onResignOwner", sctx).Return(nil).Once()
		sctx.On("Close").Once()
		withSuppressAssert(func() {
			closeFn(se, owner)
		})
		require.True(t, se.IsClosed())
		require.Nil(t, se.Owner())
		owner.AssertExpectations(t)
		sctx.AssertExpectations(t)
		withSuppressAssert(exit)
	}

	testWithErrorMock(func(se *session, owner *mockOwner) {
		se.OwnerClose(owner)
	})

	testWithErrorMock(func(_ *session, _ *mockOwner) {
		se.Close()
	})
}

func TestInternalSessionEnterOperation(t *testing.T) {
	sctx := &mockSessionContext{}
	owner := &mockOwner{}
	se := mockInternalSession(t, sctx, owner)

	// test EnterOperation will add inuse
	gotSctx, exit1, err := se.EnterOperation(owner)
	require.NoError(t, err)
	require.Same(t, sctx, gotSctx)
	require.Equal(t, uint64(1), se.Inuse())

	gotSctx, exit2, err := se.EnterOperation(owner)
	require.NoError(t, err)
	require.Same(t, sctx, gotSctx)
	require.Equal(t, uint64(2), se.Inuse())

	gotSctx, exit3, err := se.EnterOperation(owner)
	require.NoError(t, err)
	require.Same(t, sctx, gotSctx)
	require.Equal(t, uint64(3), se.Inuse())

	// test exit will decrease inuse
	exit1()
	require.Equal(t, uint64(2), se.Inuse())

	exit3()
	require.Equal(t, uint64(1), se.Inuse())

	withSuppressAssert(func() {
		// multiple exit should take no effect
		exit1()
		require.Equal(t, uint64(1), se.Inuse())
	})

	require.Equal(t, uint64(1), se.Inuse())
	exit2()
	require.Equal(t, uint64(0), se.Inuse())

	// call with an invalid owner should report an error
	withSuppressAssert(func() {
		gotSctx, exit, err := se.EnterOperation(&mockOwner{})
		require.Error(t, err)
		require.Nil(t, gotSctx)
		require.Nil(t, exit)
		require.Equal(t, uint64(0), se.Inuse())
	})

	// call with a nil owner should report an error
	withSuppressAssert(func() {
		gotSctx, exit, err := se.EnterOperation(nil)
		require.Error(t, err)
		require.Nil(t, gotSctx)
		require.Nil(t, exit)
		require.Equal(t, uint64(0), se.Inuse())
	})

	// call in a closed session should report an error
	owner.On("onResignOwner", sctx).Return(nil).Once()
	sctx.On("Close").Once()
	se.Close()
	withSuppressAssert(func() {
		gotSctx, exit, err := se.EnterOperation(owner)
		require.Error(t, err)
		require.Nil(t, gotSctx)
		require.Nil(t, exit)
		require.Equal(t, uint64(0), se.Inuse())
	})
	owner.AssertExpectations(t)
	sctx.AssertExpectations(t)

	// close the session after enter operation
	se = mockInternalSession(t, sctx, owner)
	gotSctx, exit4, err := se.EnterOperation(owner)
	require.NoError(t, err)
	require.Same(t, sctx, gotSctx)
	require.Equal(t, uint64(1), se.Inuse())

	gotSctx, exit5, err := se.EnterOperation(owner)
	require.NoError(t, err)
	require.Same(t, sctx, gotSctx)
	require.Equal(t, uint64(2), se.Inuse())

	// Close the session before exit
	owner.On("onResignOwner", sctx).Return(nil).Once()
	sctx.On("Close").Once()
	withSuppressAssert(se.Close)
	require.True(t, se.IsClosed())
	require.Equal(t, uint64(2), se.Inuse())
	owner.AssertExpectations(t)
	sctx.AssertExpectations(t)

	// The exit should still work to decrease inuse
	withSuppressAssert(exit4)
	require.Equal(t, uint64(1), se.Inuse())
	withSuppressAssert(exit5)
	require.Equal(t, uint64(0), se.Inuse())
}

func TestInternalSessionAvoidReuse(t *testing.T) {
	sctx := &mockSessionContext{}
	owner := &mockOwner{}
	se := mockInternalSession(t, sctx, owner)
	require.False(t, se.AvoidReuse())

	execute := func(do func()) {
		gotSctx, exit, err := se.EnterOperation(owner)
		require.NoError(t, err)
		require.Same(t, sctx, gotSctx)
		defer exit()
		do()
	}

	// normal use
	execute(func() {})
	require.False(t, se.AvoidReuse())
	require.False(t, se.IsClosed())
	require.Same(t, owner, se.Owner())

	// panic use
	require.PanicsWithValue(t, "panic1", func() {
		execute(func() {
			panic("panic1")
		})
	})
	require.True(t, se.AvoidReuse())
	require.False(t, se.IsClosed())
	require.Same(t, owner, se.Owner())

	// not allow transferring an owner when avoid reusing
	require.EqualError(t,
		se.TransferOwner(owner, &mockOwner{}),
		"session is avoided to be reused by the new owner",
	)
	require.True(t, se.AvoidReuse())
	require.False(t, se.IsClosed())
	require.Same(t, owner, se.Owner())

	// only allow to close
	owner.On("onResignOwner", sctx).Return(nil).Once()
	sctx.On("Close").Once()
	se.OwnerClose(owner)
	require.True(t, se.AvoidReuse())
	require.True(t, se.IsClosed())
	require.Nil(t, se.Owner())
	owner.AssertExpectations(t)
	sctx.AssertExpectations(t)
}
