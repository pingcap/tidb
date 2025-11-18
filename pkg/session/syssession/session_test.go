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
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/session/sessmgr"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// WithSuppressAssert suppress asserts in test
func WithSuppressAssert(fn func()) {
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

func (m *mockOwner) onBecameOwner(sctx sessionctx.Context) error {
	return m.Called(sctx).Error(0)
}

func (m *mockOwner) onResignOwner(sctx sessionctx.Context) error {
	return m.Called(sctx).Error(0)
}

type mockTxn struct {
	mock.Mock
	kv.Transaction
	valid bool
}

func (txn *mockTxn) Valid() bool {
	return txn.valid
}

func (txn *mockTxn) String() string {
	return txn.Transaction.String()
}

type mockPreparedFuture struct {
	sessionctx.TxnFuture
}

type mockSessionContext struct {
	sessionctx.Context
	sessmgr.Manager
	mock.Mock
}

func (m *mockSessionContext) Close() {
	m.Called()
}

func (m *mockSessionContext) RollbackTxn(ctx context.Context) {
	m.Called(ctx)
}

func (m *mockSessionContext) GetSQLExecutor() sqlexec.SQLExecutor {
	return m
}

func (m *mockSessionContext) Execute(ctx context.Context, sql string) (rs []sqlexec.RecordSet, err error) {
	args := m.Called(ctx, sql)
	if arg := args.Get(0); arg != nil {
		rs = arg.([]sqlexec.RecordSet)
	}
	err = args.Error(1)
	return
}

func (m *mockSessionContext) ExecuteInternal(ctx context.Context, sql string, sqlArgs ...any) (rs sqlexec.RecordSet, err error) {
	args := m.Called(ctx, sql, sqlArgs)
	if arg := args.Get(0); arg != nil {
		rs = arg.(sqlexec.RecordSet)
	}
	err = args.Error(1)
	return
}

func (m *mockSessionContext) ExecuteStmt(ctx context.Context, stmtNode ast.StmtNode) (rs sqlexec.RecordSet, err error) {
	args := m.Called(ctx, stmtNode)
	if arg := args.Get(0); arg != nil {
		rs = arg.(sqlexec.RecordSet)
	}
	err = args.Error(1)
	return
}

func (m *mockSessionContext) ParseWithParams(ctx context.Context, sql string, sqlArgs ...any) (n ast.StmtNode, err error) {
	args := m.Called(ctx, sql, sqlArgs)
	if arg := args.Get(0); arg != nil {
		n = arg.(ast.StmtNode)
	}
	err = args.Error(1)
	return
}

func (m *mockSessionContext) ExecRestrictedStmt(ctx context.Context, stmt ast.StmtNode, opts ...sqlexec.OptionFuncAlias) (rows []chunk.Row, fields []*resolve.ResultField, err error) {
	args := m.Called(ctx, stmt, opts)
	if arg := args.Get(0); arg != nil {
		rows = arg.([]chunk.Row)
	}
	if arg := args.Get(1); arg != nil {
		fields = arg.([]*resolve.ResultField)
	}
	err = args.Error(2)
	return
}

func (m *mockSessionContext) ExecRestrictedSQL(ctx context.Context, opts []sqlexec.OptionFuncAlias, sql string, sqlArgs ...any) (rows []chunk.Row, fields []*resolve.ResultField, err error) {
	args := m.Called(ctx, opts, sql, sqlArgs)
	if arg := args.Get(0); arg != nil {
		rows = arg.([]chunk.Row)
	}
	if arg := args.Get(1); arg != nil {
		fields = arg.([]*resolve.ResultField)
	}
	err = args.Error(2)
	return
}

func (m *mockSessionContext) GetRestrictedSQLExecutor() sqlexec.RestrictedSQLExecutor {
	return m
}

func (m *mockSessionContext) GetPreparedTxnFuture() sessionctx.TxnFuture {
	if arg := m.Called().Get(0); arg != nil {
		return arg.(sessionctx.TxnFuture)
	}
	return nil
}

func (m *mockSessionContext) Txn(active bool) (txn kv.Transaction, err error) {
	args := m.Called(active)
	err = args.Error(1)
	if args.Get(0) != nil {
		txn = args.Get(0).(kv.Transaction)
	}
	return
}

func (m *mockSessionContext) MockNoPendingTxn() {
	m.On("Txn", false).Return(&mockTxn{valid: false}, nil).Once()
	m.On("GetPreparedTxnFuture").Return(nil).Once()
}

func (m *mockSessionContext) MockResetState(ctx context.Context, panicStr string) {
	m.On("RollbackTxn", ctx).Run(func(args mock.Arguments) {
		if panicStr != "" {
			panic(panicStr)
		}
	}).Once()
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
	require.Zero(t, se.inUse)
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

func mockSession(t *testing.T, sctx *mockSessionContext) *Session {
	se, err := NewSessionForTest(sctx)
	require.NoError(t, err)
	require.Same(t, se, se.internal.Owner())
	require.False(t, se.internal.IsClosed())
	sctx.AssertExpectations(t)
	return se
}

func TestResignOwnerAndCloseSctx(t *testing.T) {
	sctx := &mockSessionContext{}
	owner := &mockOwner{}

	// success case
	owner.On("onResignOwner", sctx).Return(nil).Once()
	sctx.On("Close").Once()
	require.NoError(t, resignOwnerAndCloseSctx(owner, sctx))
	owner.AssertExpectations(t)
	sctx.AssertExpectations(t)

	// onResignOwner returns an error, should also close the session
	owner.On("onResignOwner", sctx).Return(errors.New("mockErr")).Once()
	sctx.On("Close").Once()
	require.EqualError(t, resignOwnerAndCloseSctx(owner, sctx), "mockErr")
	owner.AssertExpectations(t)
	sctx.AssertExpectations(t)

	// onResignOwner panics, should also close the session
	owner.On("onResignOwner", sctx).Panic("mockPanic").Once()
	sctx.On("Close").Once()
	require.PanicsWithValue(t, "mockPanic", func() {
		_ = resignOwnerAndCloseSctx(owner, sctx)
	})
	owner.AssertExpectations(t)
	sctx.AssertExpectations(t)

	// Close panics
	owner.On("onResignOwner", sctx).Return(nil).Once()
	sctx.On("Close").Panic("mockClosePanic")
	require.PanicsWithValue(t, "mockClosePanic", func() {
		_ = resignOwnerAndCloseSctx(owner, sctx)
	})
	owner.AssertExpectations(t)
	sctx.AssertExpectations(t)
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

	// inUse session should not transfer the owner
	se = mockInternalSession(t, sctx, owner1)
	_, exit, err := se.EnterOperation(owner1, false)
	require.NoError(t, err)
	require.Equal(t, uint64(1), se.Inuse())
	err = se.TransferOwner(owner1, owner2)
	require.Error(t, err)
	require.Contains(t, err.Error(), "session is still inUse: 1")
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
		WithSuppressAssert(func() {
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

		// inUse > 0 when close
		se = mockInternalSession(t, sctx, owner)
		_, exit1, err := se.EnterOperation(owner, false)
		require.NoError(t, err)
		require.Equal(t, uint64(1), se.Inuse())
		_, exit2, err := se.EnterOperation(owner, true)
		require.NoError(t, err)
		require.Equal(t, uint64(2), se.Inuse())
		_, exit3, err := se.EnterOperation(owner, true)
		require.NoError(t, err)
		require.Equal(t, uint64(3), se.Inuse())
		// should not call `sctx.Close()` when inuse > 0 to avoid some data race
		WithSuppressAssert(func() {
			closeFn(se, owner)
		})
		require.True(t, se.IsClosed())
		require.Nil(t, se.Owner())
		// sctx.Close should not be called when inuse > 0 after exit
		require.Equal(t, uint64(3), se.Inuse())
		WithSuppressAssert(exit1)
		require.Equal(t, uint64(2), se.Inuse())
		WithSuppressAssert(exit3)
		require.Equal(t, uint64(1), se.Inuse())
		// sctx.Close should be called after inuse decreased to 0
		owner.On("onResignOwner", sctx).Return(nil).Once()
		sctx.On("Close").Once()
		WithSuppressAssert(exit2)
		require.Zero(t, se.Inuse())
		sctx.AssertExpectations(t)
		owner.AssertExpectations(t)
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

	// test EnterOperation will add inUse
	gotSctx, exit1, err := se.EnterOperation(owner, true)
	require.NoError(t, err)
	require.Same(t, sctx, gotSctx)
	require.Equal(t, uint64(1), se.Inuse())

	gotSctx, exit2, err := se.EnterOperation(owner, false)
	require.NoError(t, err)
	require.Same(t, sctx, gotSctx)
	require.Equal(t, uint64(2), se.Inuse())

	gotSctx, exit3, err := se.EnterOperation(owner, true)
	require.NoError(t, err)
	require.Same(t, sctx, gotSctx)
	require.Equal(t, uint64(3), se.Inuse())

	// test exit will decrease inUse
	exit1()
	require.Equal(t, uint64(2), se.Inuse())

	exit3()
	require.Equal(t, uint64(1), se.Inuse())

	WithSuppressAssert(func() {
		// multiple exit should take no effect
		exit1()
		require.Equal(t, uint64(1), se.Inuse())
	})

	require.Equal(t, uint64(1), se.Inuse())
	exit2()
	require.Equal(t, uint64(0), se.Inuse())

	// call with an invalid owner should report an error
	WithSuppressAssert(func() {
		gotSctx, exit, err := se.EnterOperation(&mockOwner{}, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "caller is not the owner")
		require.Nil(t, gotSctx)
		require.Nil(t, exit)
		require.Equal(t, uint64(0), se.Inuse())
	})

	// call with a nil owner should report an error
	WithSuppressAssert(func() {
		gotSctx, exit, err := se.EnterOperation(nil, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "caller is not the owner")
		require.Nil(t, gotSctx)
		require.Nil(t, exit)
		require.Equal(t, uint64(0), se.Inuse())
	})

	// call in a closed session should report an error
	owner.On("onResignOwner", sctx).Return(nil).Once()
	sctx.On("Close").Once()
	se.Close()
	WithSuppressAssert(func() {
		gotSctx, exit, err := se.EnterOperation(owner, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "session is closed")
		require.Nil(t, gotSctx)
		require.Nil(t, exit)
		require.Equal(t, uint64(0), se.Inuse())
	})
	owner.AssertExpectations(t)
	sctx.AssertExpectations(t)

	// close the session after enter operation
	se = mockInternalSession(t, sctx, owner)
	gotSctx, exit4, err := se.EnterOperation(owner, true)
	require.NoError(t, err)
	require.Same(t, sctx, gotSctx)
	require.Equal(t, uint64(1), se.Inuse())

	gotSctx, exit5, err := se.EnterOperation(owner, true)
	require.NoError(t, err)
	require.Same(t, sctx, gotSctx)
	require.Equal(t, uint64(2), se.Inuse())

	// Close the session before exit
	WithSuppressAssert(se.Close)
	require.True(t, se.IsClosed())
	require.Equal(t, uint64(2), se.Inuse())
	require.True(t, se.IsClosed())

	// new EnterOperation should be rejected after close but inuse > 0
	WithSuppressAssert(func() {
		_, _, err = se.EnterOperation(owner, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "session is closed")
	})

	// The exit should still work to decrease inUse
	WithSuppressAssert(exit4)
	require.Equal(t, uint64(1), se.Inuse())
	// when inuse > 0, the session should not call sctx.Close() even if onResignOwner fails
	owner.On("onResignOwner", sctx).Return(errors.New("mockErr")).Once()
	sctx.On("Close").Once()
	WithSuppressAssert(exit5)
	require.Equal(t, uint64(0), se.Inuse())
	owner.AssertExpectations(t)
	sctx.AssertExpectations(t)
}

func TestInternalSessionOwnerWithSctx(t *testing.T) {
	sctx := &mockSessionContext{}
	owner := &mockOwner{}
	se := mockInternalSession(t, sctx, owner)
	mockCb := &mock.Mock{}
	cb := func(sctx SessionContext) error {
		require.Equal(t, uint64(1), se.Inuse())
		return mockCb.MethodCalled("cb", sctx).Error(0)
	}

	// normal case
	mockCb.On("cb", sctx).Return(nil).Once()
	require.NoError(t, se.OwnerWithSctx(owner, cb))
	require.Zero(t, se.Inuse())
	mockCb.AssertExpectations(t)

	// invalid owner
	WithSuppressAssert(func() {
		err := se.OwnerWithSctx(&mockOwner{}, cb)
		require.Error(t, err)
		require.Contains(t, err.Error(), "caller is not the owner")
		require.Zero(t, se.Inuse())
	})

	// error in cb
	mockCb.On("cb", sctx).Return(errors.New("mockErr")).Once()
	err := se.OwnerWithSctx(owner, cb)
	require.Error(t, err)
	require.Contains(t, err.Error(), "mockErr")
	require.Zero(t, se.Inuse())
	mockCb.AssertExpectations(t)

	// panic in cb
	mockCb.On("cb", sctx).Panic("mockPanic").Once()
	require.PanicsWithValue(t, "mockPanic", func() {
		_ = se.OwnerWithSctx(owner, cb)
	})
	require.Zero(t, se.Inuse())
	mockCb.AssertExpectations(t)

	// session closed
	sctx.On("Close").Once()
	owner.On("onResignOwner", sctx).Return(nil).Once()
	se.Close()
	require.True(t, se.IsClosed())
	sctx.AssertExpectations(t)
	owner.AssertExpectations(t)
	WithSuppressAssert(func() {
		err := se.OwnerWithSctx(owner, cb)
		require.Error(t, err)
		require.Contains(t, err.Error(), "session is closed")
		require.Zero(t, se.Inuse())
	})
}

func TestInternalSessionAvoidReuse(t *testing.T) {
	sctx := &mockSessionContext{}
	owner := &mockOwner{}
	se := mockInternalSession(t, sctx, owner)
	require.False(t, se.IsAvoidReuse())

	execute := func(do func()) {
		gotSctx, exit, err := se.EnterOperation(owner, false)
		require.NoError(t, err)
		require.Same(t, sctx, gotSctx)
		defer exit()
		do()
	}

	// normal use
	execute(func() {})
	require.False(t, se.IsAvoidReuse())
	require.False(t, se.IsClosed())
	require.Same(t, owner, se.Owner())

	// panic use
	require.PanicsWithValue(t, "panic1", func() {
		execute(func() {
			panic("panic1")
		})
	})
	require.True(t, se.IsAvoidReuse())
	require.False(t, se.IsClosed())
	require.Same(t, owner, se.Owner())

	// allow to close
	owner.On("onResignOwner", sctx).Return(nil).Once()
	sctx.On("Close").Once()
	se.OwnerClose(owner)
	require.True(t, se.IsAvoidReuse())
	require.True(t, se.IsClosed())
	require.Nil(t, se.Owner())
	owner.AssertExpectations(t)
	sctx.AssertExpectations(t)
}

func TestInternalSessionCheckNoPendingTxn(t *testing.T) {
	sctx := &mockSessionContext{}
	se := mockInternalSession(t, sctx, &mockOwner{})

	sctx.MockNoPendingTxn()
	require.NoError(t, se.CheckNoPendingTxn())
	sctx.AssertExpectations(t)

	sctx.On("GetPreparedTxnFuture").Return(&mockPreparedFuture{}).Once()
	require.EqualError(t, se.CheckNoPendingTxn(), "txn is pending for TSO")
	sctx.AssertExpectations(t)

	sctx.On("GetPreparedTxnFuture").Return(nil).Once()
	sctx.On("Txn", false).Return(nil, errors.New("mockErr")).Once()
	require.EqualError(t, se.CheckNoPendingTxn(), "mockErr")
	sctx.AssertExpectations(t)

	sctx.On("GetPreparedTxnFuture").Return(nil).Once()
	sctx.On("Txn", false).Return(&mockTxn{valid: true}, nil).Once()
	require.EqualError(t, se.CheckNoPendingTxn(), "txn is still valid")
	sctx.AssertExpectations(t)
}

func TestInternalSessionResetState(t *testing.T) {
	sctx := &mockSessionContext{}
	owner := &mockOwner{}
	se := mockInternalSession(t, sctx, owner)
	ctx := context.WithValue(context.Background(), "a", "b")
	checkInuse := func(mock.Arguments) { require.Equal(t, uint64(1), se.Inuse()) }

	// normal case
	sctx.On("RollbackTxn", ctx).Run(checkInuse).Once()
	require.NoError(t, se.OwnerResetState(ctx, owner))
	require.Zero(t, se.Inuse())
	sctx.AssertExpectations(t)

	// RollbackTxn panic
	sctx.On("RollbackTxn", ctx).Run(checkInuse).Panic("mockPanic1").Once()
	require.PanicsWithValue(t, "mockPanic1", func() {
		_ = se.OwnerResetState(ctx, owner)
	})
	require.Zero(t, se.Inuse())
	sctx.AssertExpectations(t)

	// not owner
	WithSuppressAssert(func() {
		err := se.OwnerResetState(ctx, &mockOwner{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "caller is not the owner")
		require.Zero(t, se.Inuse())
	})

	// closed session
	owner.On("onResignOwner", sctx).Return(nil).Once()
	sctx.On("Close").Once()
	se.Close()
	sctx.AssertExpectations(t)
	owner.AssertExpectations(t)
	WithSuppressAssert(func() {
		err := se.OwnerResetState(ctx, owner)
		require.Error(t, err)
		require.Contains(t, err.Error(), "session is closed")
		require.Zero(t, se.Inuse())
	})
}

func testCallProxyMethod(
	t *testing.T,
	sctx *mockSessionContext,
	se *Session,
	name string,
	args []any,
	returns []any,
	callMethod func([]any) []any,
) {
	// normal call
	inuse := se.internal.Inuse()
	require.GreaterOrEqual(t, inuse, uint64(0))
	inCallInuse := uint64(0)
	sctx.On(name, args...).Return(returns...).Run(func(_ mock.Arguments) {
		inCallInuse = se.internal.Inuse()
	}).Once()
	actualRets := callMethod(args)
	require.Equal(t, returns, actualRets)
	require.Equal(t, inuse+1, inCallInuse)
	require.Equal(t, inuse, se.internal.Inuse())
	sctx.AssertExpectations(t)

	// transfer the owner to make call fail
	owner2 := noopOwnerHook{}
	require.NoError(t, se.internal.TransferOwner(se, owner2))
	sctx.AssertExpectations(t)

	// error call
	WithSuppressAssert(func() {
		actualRets = callMethod(args)
	})
	require.Equal(t, inuse, se.internal.Inuse())
	for i := range actualRets {
		if i < len(actualRets)-1 {
			require.Nil(t, actualRets[i], fmt.Sprintf("%d: %v", i, actualRets[i]))
		} else {
			err := actualRets[i].(error)
			require.Contains(t, err.Error(), "caller is not the owner")
		}
	}

	// transfer the owner back
	require.NoError(t, se.internal.TransferOwner(owner2, se))
	sctx.AssertExpectations(t)

	// test panics
	require.False(t, se.internal.IsAvoidReuse())
	sctx.On(name, args...).Run(func(_ mock.Arguments) {
		inCallInuse = se.internal.Inuse()
		panic("panicTest")
	}).Once()
	require.PanicsWithValue(t, "panicTest", func() {
		callMethod(args)
	})
	require.Equal(t, inuse+1, inCallInuse)
	require.Equal(t, inuse, se.internal.Inuse())
	require.True(t, se.internal.IsAvoidReuse())
	se.internal.avoidReuse = false
	sctx.AssertExpectations(t)
}

func testCallProxyMethod22[A1 any, A2 any, R1 any, R2 any](
	t *testing.T,
	sctx *mockSessionContext,
	se *Session,
	name string,
	arg1 A1, arg2 A2,
	r1 R1, r2 R2,
	callMethod func(arg1 A1, arg2 A2) (R1, R2),
) {
	testCallProxyMethod(t, sctx, se, name, []any{arg1, arg2}, []any{r1, r2}, func(args []any) []any {
		ret1, ret2 := callMethod(args[0].(A1), args[1].(A2))
		return []any{ret1, ret2}
	})
}

func testCallProxyMethod32[A1 any, A2 any, A3 any, R1 any, R2 any](
	t *testing.T,
	sctx *mockSessionContext,
	se *Session,
	name string,
	arg1 A1, arg2 A2, arg3 A3,
	r1 R1, r2 R2,
	callMethod func(arg1 A1, arg2 A2, arg3 A3) (R1, R2),
) {
	testCallProxyMethod(t, sctx, se, name, []any{arg1, arg2, arg3}, []any{r1, r2}, func(args []any) []any {
		ret1, ret2 := callMethod(args[0].(A1), args[1].(A2), args[2].(A3))
		return []any{ret1, ret2}
	})
}

func testCallProxyMethod33[A1 any, A2 any, A3 any, R1 any, R2 any, R3 any](
	t *testing.T,
	sctx *mockSessionContext,
	se *Session,
	name string,
	arg1 A1, arg2 A2, arg3 A3,
	r1 R1, r2 R2, r3 R3,
	callMethod func(arg1 A1, arg2 A2, arg3 A3) (R1, R2, R3),
) {
	testCallProxyMethod(t, sctx, se, name, []any{arg1, arg2, arg3}, []any{r1, r2, r3}, func(args []any) []any {
		ret1, ret2, ret3 := callMethod(args[0].(A1), args[1].(A2), args[2].(A3))
		return []any{ret1, ret2, ret3}
	})
}

func testCallProxyMethod43[A1 any, A2 any, A3 any, A4 any, R1 any, R2 any, R3 any](
	t *testing.T,
	sctx *mockSessionContext,
	se *Session,
	name string,
	arg1 A1, arg2 A2, arg3 A3, arg4 A4,
	r1 R1, r2 R2, r3 R3,
	callMethod func(arg1 A1, arg2 A2, arg3 A3, arg4 A4) (R1, R2, R3),
) {
	testCallProxyMethod(t, sctx, se, name, []any{arg1, arg2, arg3, arg4}, []any{r1, r2, r3}, func(args []any) []any {
		ret1, ret2, ret3 := callMethod(args[0].(A1), args[1].(A2), args[2].(A3), args[3].(A4))
		return []any{ret1, ret2, ret3}
	})
}

type mockRecordSet struct {
	v int
	sqlexec.RecordSet
}

func TestSessionProxyMethods(t *testing.T) {
	sctx := &mockSessionContext{}
	se := mockSession(t, sctx)
	require.Same(t, se, se.GetSQLExecutor())
	require.Same(t, se, se.GetRestrictedSQLExecutor())

	ctx := context.WithValue(context.Background(), "a", "b")

	testCallProxyMethod22(
		t, sctx, se, "Execute",
		ctx, "select 1, 2, 3",
		[]sqlexec.RecordSet{&mockRecordSet{v: 1}, &mockRecordSet{v: 2}}, errors.New("err"),
		se.Execute,
	)

	testCallProxyMethod32(
		t, sctx, se, "ExecuteInternal",
		ctx, "select 1, 2, 3", []any{"a", "c", 2},
		sqlexec.RecordSet(&mockRecordSet{v: 3}), errors.New("err"),
		func(arg1 context.Context, arg2 string, arg3 []any) (sqlexec.RecordSet, error) {
			return se.ExecuteInternal(arg1, arg2, arg3...)
		},
	)

	testCallProxyMethod22(
		t, sctx, se, "ExecuteStmt",
		ctx, ast.StmtNode(&ast.SelectStmt{Distinct: true}),
		sqlexec.RecordSet(&mockRecordSet{v: 3}), errors.New("err"),
		se.ExecuteStmt,
	)

	testCallProxyMethod32(
		t, sctx, se, "ParseWithParams",
		ctx, "select a+?, b+?, c+? from t", []any{"a", 10, "5"},
		ast.StmtNode(&ast.SelectStmt{}), errors.New("mockErr"),
		func(arg1 context.Context, arg2 string, arg3 []any) (ast.StmtNode, error) {
			return se.ParseWithParams(arg1, arg2, arg3...)
		},
	)

	testCallProxyMethod33(
		t, sctx, se, "ExecRestrictedStmt",
		ctx,
		ast.StmtNode(&ast.SelectStmt{}),
		[]sqlexec.OptionFuncAlias{sqlexec.ExecOptionIgnoreWarning, sqlexec.ExecOptionAnalyzeVer1},
		[]chunk.Row{{}, {}},
		[]*resolve.ResultField{{DBName: ast.NewCIStr("v1")}, {DBName: ast.NewCIStr("v2")}},
		errors.New("mockErr"),
		func(arg1 context.Context, arg2 ast.StmtNode, arg3 []sqlexec.OptionFuncAlias) (
			[]chunk.Row, []*resolve.ResultField, error,
		) {
			return se.ExecRestrictedStmt(arg1, arg2, arg3...)
		},
	)

	testCallProxyMethod43(
		t, sctx, se, "ExecRestrictedSQL",
		ctx,
		[]sqlexec.OptionFuncAlias{sqlexec.ExecOptionIgnoreWarning, sqlexec.ExecOptionAnalyzeVer1},
		"select ?, ?, ?",
		[]any{1, 2, "3"},
		[]chunk.Row{{}, {}},
		[]*resolve.ResultField{{DBName: ast.NewCIStr("v1")}, {DBName: ast.NewCIStr("v2")}},
		errors.New("mockErr"),
		func(arg1 context.Context, arg2 []sqlexec.OptionFuncAlias, arg3 string, arg4 []any) (
			[]chunk.Row, []*resolve.ResultField, error,
		) {
			return se.ExecRestrictedSQL(arg1, arg2, arg3, arg4...)
		},
	)

	testCallProxyMethod22(
		t, sctx, se, "Execute",
		ctx, "select 1, 2, 3",
		[]sqlexec.RecordSet{&mockRecordSet{v: 1}, &mockRecordSet{v: 2}}, errors.New("err"),
		func(arg1 context.Context, arg2 string) (rs []sqlexec.RecordSet, err error) {
			var execErr error
			err = se.WithSessionContext(func(sctx sessionctx.Context) error {
				rs, execErr = sctx.GetSQLExecutor().Execute(arg1, arg2)
				return execErr
			})

			if execErr != nil {
				require.Same(t, execErr, err)
			}

			return
		},
	)
}

func TestSessionClose(t *testing.T) {
	sctx := &mockSessionContext{}
	se := mockSession(t, sctx)

	// normal close
	sctx.On("Close").Once()
	se.Close()
	require.True(t, se.internal.IsClosed())
	require.True(t, se.IsInternalClosed())
	sctx.AssertExpectations(t)

	// cannot use it again after closed
	WithSuppressAssert(func() {
		_, err := se.Execute(context.TODO(), "select 1")
		require.Error(t, err)
		require.Contains(t, err.Error(), "session is closed")
	})

	// close a closed session should take no effect
	se.Close()

	// close a not own session should take no effect
	se2 := mockSession(t, sctx)
	se.internal = se2.internal
	require.False(t, se.IsInternalClosed())
	se.Close()
	require.False(t, se.IsInternalClosed())
	require.False(t, se2.IsInternalClosed())

	// owner should close
	sctx.On("Close").Once()
	se2.Close()
	require.True(t, se.IsInternalClosed())
	require.True(t, se2.IsInternalClosed())
	sctx.AssertExpectations(t)
}

func TestSessionAvoidReuse(t *testing.T) {
	// owner
	sctx := &mockSessionContext{}
	s, err := NewSessionForTest(sctx)
	require.NoError(t, err)
	require.False(t, s.internal.avoidReuse)
	s.AvoidReuse()
	require.True(t, s.internal.avoidReuse)

	// multiple calls of AvoidReuse
	s.AvoidReuse()
	require.True(t, s.internal.avoidReuse)

	// still can use the session after AvoidReuse
	ctx := context.WithValue(context.Background(), "a", "b")
	sctx.On("ExecuteInternal", ctx, "select 1", []any(nil)).Return(nil, nil).Once()
	rs, err := s.ExecuteInternal(ctx, "select 1")
	require.Nil(t, rs)
	require.NoError(t, err)
	sctx.AssertExpectations(t)

	// not owner
	s, err = NewSessionForTest(sctx)
	require.NoError(t, err)
	s2 := &Session{internal: s.internal}
	require.False(t, s.internal.avoidReuse)
	s2.AvoidReuse()
	require.False(t, s.internal.avoidReuse)
}

func TestInternalSessionUnThreadSafeOperations(t *testing.T) {
	sctx := &mockSessionContext{}
	owner := &mockOwner{}
	se := mockInternalSession(t, sctx, owner)

	enterThreadSafeOperation := func() func() {
		inUse, unsafe := se.inUse, se.unsafe
		gotSctx, exit, err := se.EnterOperation(owner, true)
		require.NoError(t, err)
		require.Same(t, sctx, gotSctx)
		require.NotNil(t, exit)
		require.Equal(t, inUse+1, se.inUse)
		require.Equal(t, unsafe, se.unsafe)
		return exit
	}

	enterFirstThreadUnsafeOperation := func() func() {
		inUse, unsafe := se.inUse, se.unsafe
		gotSctx, exit, err := se.EnterOperation(owner, false)
		require.NoError(t, err)
		require.Same(t, sctx, gotSctx)
		require.NotNil(t, exit)
		require.Equal(t, inUse+1, se.inUse)
		require.Equal(t, unsafe+1, se.unsafe)
		return exit
	}

	enterThreadUnsafeOperationExpectError := func() {
		inUse, unsafe := se.inUse, se.unsafe
		WithSuppressAssert(func() {
			gotSctx, exit, err := se.EnterOperation(owner, false)
			require.EqualError(t, err, "EnterOperation error: race detected for concurrent thread-unsafe operations")
			require.Nil(t, gotSctx)
			require.Nil(t, exit)
			require.Equal(t, inUse, se.inUse)
			require.Equal(t, unsafe+1, se.unsafe)
		})
	}

	// multiple thread-safe operations should be allowed
	exit1 := enterThreadSafeOperation()
	exit2 := enterThreadSafeOperation()

	// the first thread-unsafe operation should be allowed
	exitUnsafe := enterFirstThreadUnsafeOperation()

	// next thread-unsafe operation should return error
	enterThreadUnsafeOperationExpectError()

	// net thread safe operation should still success
	exit3 := enterThreadSafeOperation()

	// next thread-unsafe operation should return error
	enterThreadUnsafeOperationExpectError()

	// exit
	require.Equal(t, uint64(4), se.inUse)
	require.Equal(t, uint64(3), se.unsafe)
	exit1()
	require.Equal(t, uint64(3), se.inUse)
	require.Equal(t, uint64(3), se.unsafe)
	exit3()
	require.Equal(t, uint64(2), se.inUse)
	require.Equal(t, uint64(3), se.unsafe)
	WithSuppressAssert(exitUnsafe)
	require.Equal(t, uint64(1), se.inUse)
	require.Equal(t, uint64(0), se.unsafe)
	exit2()
	require.Equal(t, uint64(0), se.inUse)
	require.Equal(t, uint64(0), se.unsafe)

	// new thread-unsafe operation should be allowed when after previous exits
	exitUnsafe = enterThreadSafeOperation()
	exitUnsafe()
	require.Equal(t, uint64(0), se.inUse)
	require.Equal(t, uint64(0), se.unsafe)

	// when panic happens, the session should also decrease `inUse` and `unsafe`
	func() {
		defer func() {
			require.Equal(t, uint64(0), se.inUse)
			require.Equal(t, uint64(0), se.unsafe)
			r := recover()
			require.Equal(t, "mockPanic", r)
		}()

		exitUnsafe = enterFirstThreadUnsafeOperation()
		defer exitUnsafe()
		require.Equal(t, uint64(1), se.inUse)
		require.Equal(t, uint64(1), se.unsafe)
		enterThreadUnsafeOperationExpectError()
		require.Equal(t, uint64(1), se.inUse)
		require.Equal(t, uint64(2), se.unsafe)
		panic("mockPanic")
	}()
}

func TestSessionThreadUnsafeOperations(t *testing.T) {
	sctx := &mockSessionContext{}
	se := mockSession(t, sctx)
	ctx := context.WithValue(context.Background(), "a", "b")
	ch := make(chan bool)
	waitCh := func(inOp bool) {
		select {
		case fromOp, ok := <-ch:
			require.True(t, ok)
			require.True(t, inOp != fromOp)
			return
		case <-time.After(10 * time.Second):
			require.FailNow(t, "timed out")
		}
	}

	sendCh := func(fromOp bool) {
		select {
		case ch <- fromOp:
		case <-time.After(10 * time.Second):
			require.FailNow(t, "timed out")
		}
	}

	called := false
	onCalled := func(_ mock.Arguments) {
		require.False(t, called)
		called = true
		require.Equal(t, uint64(1), se.internal.Inuse())
		sendCh(true)
		waitCh(true)
	}

	operations := []struct {
		name string
		call func(first bool) error
	}{
		{
			name: "WithSessionContext",
			call: func(first bool) error {
				err := se.WithSessionContext(func(got sessionctx.Context) error {
					require.Same(t, sctx, got)
					onCalled(nil)
					return nil
				})
				if first {
					require.True(t, called)
				}
				return err
			},
		},
		{
			name: "Execute",
			call: func(first bool) error {
				if first {
					sctx.On("Execute", ctx, "select 1").
						Run(onCalled).
						Return(nil, nil).
						Once()
				}
				_, err := se.Execute(ctx, "select 1")
				sctx.AssertExpectations(t)
				return err
			},
		},
		{
			name: "ExecuteInternal",
			call: func(first bool) error {
				if first {
					sctx.On("ExecuteInternal", ctx, "select 1", []any(nil)).
						Run(onCalled).
						Return(nil, nil).
						Once()
				}
				_, err := se.ExecuteInternal(ctx, "select 1")
				sctx.AssertExpectations(t)
				return err
			},
		},
		{
			name: "ExecuteStmt",
			call: func(first bool) error {
				n := &ast.SelectStmt{}
				if first {
					sctx.On("ExecuteStmt", ctx, n).
						Run(onCalled).
						Return(nil, nil).
						Once()
				}
				_, err := se.ExecuteStmt(ctx, n)
				sctx.AssertExpectations(t)
				return err
			},
		},
		{
			name: "ParseWithParams",
			call: func(first bool) error {
				if first {
					sctx.On("ParseWithParams", ctx, "select 1", []any(nil)).
						Run(onCalled).
						Return(nil, nil).
						Once()
				}
				_, err := se.ParseWithParams(ctx, "select 1")
				sctx.AssertExpectations(t)
				return err
			},
		},
		{
			name: "ExecRestrictedStmt",
			call: func(first bool) error {
				n := &ast.SelectStmt{}
				if first {
					sctx.On("ExecRestrictedStmt", ctx, n, []sqlexec.OptionFuncAlias(nil)).
						Run(onCalled).
						Return(nil, nil, nil).
						Once()
				}
				_, _, err := se.ExecRestrictedStmt(ctx, n)
				sctx.AssertExpectations(t)
				return err
			},
		},
		{
			name: "ExecRestrictedSQL",
			call: func(first bool) error {
				if first {
					sctx.On("ExecRestrictedSQL", ctx, []sqlexec.OptionFuncAlias(nil), "select 1", []any(nil)).
						Run(onCalled).
						Return(nil, nil, nil).
						Once()
				}
				_, _, err := se.ExecRestrictedSQL(ctx, nil, "select 1")
				sctx.AssertExpectations(t)
				return err
			},
		},
	}

	for i, op := range operations {
		t.Run(op.name, func(t *testing.T) {
			WithSuppressAssert(func() {
				called = false
				require.Zero(t, se.internal.inUse)
				require.Zero(t, se.internal.unsafe)
				// the first operation should be allowed
				go func() {
					require.NoError(t, op.call(true))
					require.True(t, called)
					sendCh(true)
				}()
				waitCh(false)
				require.Equal(t, uint64(1), se.internal.inUse)
				require.Equal(t, uint64(1), se.internal.unsafe)
				// other operations should return errors
				for j := range 3 {
					next := i + j
					if next >= len(operations) {
						next = len(operations) - 1
					}
					require.EqualError(
						t, operations[next].call(false),
						"EnterOperation error: race detected for concurrent thread-unsafe operations",
					)
					require.Equal(t, uint64(1), se.internal.inUse)
					require.Equal(t, uint64(j+2), se.internal.unsafe)
				}

				// notify first op to continue
				sendCh(false)
				// wait first op exit
				waitCh(false)
				require.Zero(t, se.internal.inUse)
				require.Zero(t, se.internal.unsafe)
			})
		})
	}
}
