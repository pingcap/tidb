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
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)

// suppressAssertInTest is only used for testing to suppress the assertions.
var suppressAssertInTest = false

// SessionContext provides a context for a session.
type SessionContext interface {
	sessionctx.Context
	// Close closes the session context.
	Close()
}

// sessionOwner defines the interface for the owner of the session.
type sessionOwner interface {
	// onBecameOwner is a hook that will be called when it becomes the owner of the session.
	onBecameOwner(SessionContext) error
	// onResignOwner is a hook that will be called when it resigns the ownership of the session.
	onResignOwner(SessionContext) error
}

func ownerStr(owner sessionOwner) string {
	if owner == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%T(%p)", owner, owner)
}

// noopOwnerHook does nothing when the owner of the session is changed.
type noopOwnerHook struct{}

func (noopOwnerHook) onBecameOwner(SessionContext) error { return nil }

func (noopOwnerHook) onResignOwner(SessionContext) error { return nil }

// session is the internal session that will be cached in the session pool.
// It is private and should not be accessed directly by the outside callers.
type session struct {
	mu sync.Mutex
	// sctx is the raw state of the session.
	sctx SessionContext
	// owner is the one that holds the current session.
	// In most cases, only the "owner" can do operations on the session.
	// If `owner == nil`, it means the session is closed and should not be used.
	owner sessionOwner
	// inUse is the counter to record how many operations are on going on the session.
	// When `EnterOperation` is called, it will increase the counter.
	// When the operation is finished, it will decrease the counter.
	inuse uint64
	// avoidReuse will be marked as true when some panics detected.
	// When it is true, the session should not be reused to avoid unexpected behavior.
	avoidReuse bool
}

func newInternalSession(sctx SessionContext, owner sessionOwner) (*session, error) {
	intest.AssertNotNil(sctx)
	intest.AssertNotNil(owner)
	if err := owner.onBecameOwner(sctx); err != nil {
		return nil, err
	}
	return &session{sctx: sctx, owner: owner}, nil
}

// Owner returns the current owner of the session.
func (s *session) Owner() sessionOwner {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.owner
}

// IsClosed returns whether the session is closed.
func (s *session) IsClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.owner == nil
}

// TransferOwner transfers the ownership of the session to another owner.
func (s *session) TransferOwner(from, to sessionOwner) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOwner(from); err != nil {
		return err
	}

	if to == nil {
		return errors.New("cannot transfer to a nil owner")
	}

	if from == to {
		return nil
	}

	if err := s.checkNotInuse(); err != nil {
		return err
	}

	if s.avoidReuse {
		return errors.New("session is avoided to be reused by the new owner")
	}

	// assign a noop owner to session temporarily to make sure the close in defer can work
	s.owner = noopOwnerHook{}
	defer func() {
		if s.owner != to {
			// This means some error or panic occurs.
			// We should close the session to avoid some unexpected behavior.
			s.doClose()
			return
		}
	}()

	if err := from.onResignOwner(s.sctx); err != nil {
		return err
	}

	if err := to.onBecameOwner(s.sctx); err != nil {
		return err
	}

	s.owner = to
	return nil
}

// AvoidReuse returns whether the session should be avoided to reuse.
func (s *session) AvoidReuse() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.avoidReuse
}

// MarkAvoidReuse marks the session as avoid-reuse.
func (s *session) MarkAvoidReuse() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.avoidReuse = true
}

// Inuse returns the number of operations that are on going on the session.
func (s *session) Inuse() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inuse
}

// EnterOperation enters an operation on the session.
// It receives a caller with the type sessionOwner as the argument,
// and if the session is closed or the caller is not the owner, it will return an error.
// If the operation is entered successfully,
// it will return the internal SessionContext for further usage and a function to exit the operation.
func (s *session) EnterOperation(caller sessionOwner) (SessionContext, func(), error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOwner(caller); err != nil {
		s.reportError(err.Error(), zap.String("caller", ownerStr(caller)))
		return nil, nil, err
	}

	s.inuse++
	exit := false
	return s.sctx, func() {
		r := recover()
		s.mu.Lock()
		defer s.mu.Unlock()
		if exit {
			s.reportError("multiple exits for the same operation")
			return
		}

		exit = true
		s.inuse--
		if s.owner != caller {
			s.reportError(
				"session owner transferred when executing operation",
				zap.String("caller", ownerStr(caller)),
			)
		}
		if r != nil {
			s.avoidReuse = true
			panic(r)
		}
	}, nil
}

// OwnerClose indicates to close the session.
// It receives a caller with the type sessionOwner as the argument,
// and if the caller is not the owner, or it is already closed, it will return without doing anything.
func (s *session) OwnerClose(caller sessionOwner) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if caller != nil && caller == s.owner {
		// only close the session when the caller is the owner
		s.doClose()
	}
}

// OwnerResetState resets the state of the session.
// It receives a caller with the type sessionOwner as the argument,
// and if the session is closed or the caller is not the owner, it will return an error.
func (s *session) OwnerResetState(ctx context.Context, caller sessionOwner) error {
	sctx, exit, err := s.EnterOperation(caller)
	if err != nil {
		return err
	}
	defer exit()
	sctx.RollbackTxn(ctx)
	return nil
}

// Close closes the session without checking the owner.
func (s *session) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.doClose()
}

// doClose closes the session.
// It should be called with the protection of the mutex.
func (s *session) doClose() {
	if s.owner == nil {
		return
	}

	defer func() {
		s.owner = nil
		s.sctx.Close()
	}()

	if s.owner != nil {
		if err := s.owner.onResignOwner(s.sctx); err != nil {
			s.reportError("error occurs when resigning the owner", zap.Error(err))
		}
	}

	if s.inuse > 0 {
		s.reportError("session owner is still in use when closing", zap.Uint64("inuse", s.inuse))
	}
}

// checkNotInuse returns an error if the session is still in use.
// It should be called with the protection of the mutex.
func (s *session) checkNotInuse() error {
	if s.inuse > 0 {
		return errors.Errorf("session is still inuse: %d", s.inuse)
	}
	return nil
}

// checkOwner returns an error if the session is closed or the caller is not the owner.
// It should be called with the protection of the mutex.
func (s *session) checkOwner(caller sessionOwner) error {
	var msg string
	switch {
	case caller == nil:
		msg = "session has already been closed"
	case s.owner != caller:
		msg = "caller is not the current session owner"
	default:
		return nil
	}
	return errors.Errorf("%s, caller: %s, owner: %s", msg, ownerStr(caller), ownerStr(s.owner))
}

// reportError does not return an error to the caller, it logs some message and stack information instead.
// However, it will panic in the test environment to make the test fail.
func (s *session) reportError(msg string, fields ...zap.Field) {
	fields = append(fields, zap.String("owner", ownerStr(s.owner)), zap.Stack("stack"))
	logutil.BgLogger().Error(msg, fields...)
	if !suppressAssertInTest {
		intest.Assert(false, msg)
	}
}

// Session implements the sessionOwner interface.
var _ sessionOwner = &Session{}
var _ sqlexec.SQLExecutor = &Session{}
var _ sqlexec.RestrictedSQLExecutor = &Session{}

// Session is public and can be accessed by the outside callers.
type Session struct {
	internal *session
}

// Close closes the session.
func (s *Session) Close() {
	s.internal.OwnerClose(s)
}

// WithSessionContext executes the input function with the session context.
func (s *Session) WithSessionContext(fn func(ctx SessionContext) error) error {
	sctx, exit, err := s.internal.EnterOperation(s)
	if err != nil {
		return err
	}
	defer exit()
	return fn(sctx)
}

func (s *Session) ResetState(ctx context.Context) error {
	return s.internal.OwnerResetState(ctx, s)
}

// Execute implements the SQLExecutor.Execute interface.
func (s *Session) Execute(ctx context.Context, sql string) ([]sqlexec.RecordSet, error) {
	sctx, exit, err := s.internal.EnterOperation(s)
	if err != nil {
		return nil, err
	}
	defer exit()
	return sctx.GetSQLExecutor().Execute(ctx, sql)
}

// ExecuteInternal implements the SQLExecutor.ExecuteInternal interface.
func (s *Session) ExecuteInternal(ctx context.Context, sql string, args ...any) (sqlexec.RecordSet, error) {
	sctx, exit, err := s.internal.EnterOperation(s)
	if err != nil {
		return nil, err
	}
	defer exit()
	return sctx.GetSQLExecutor().ExecuteInternal(ctx, sql, args...)
}

// ExecuteStmt implements the SQLExecutor.ExecuteStmt interface.
func (s *Session) ExecuteStmt(ctx context.Context, stmtNode ast.StmtNode) (sqlexec.RecordSet, error) {
	sctx, exit, err := s.internal.EnterOperation(s)
	if err != nil {
		return nil, err
	}
	defer exit()
	return sctx.GetSQLExecutor().ExecuteStmt(ctx, stmtNode)
}

// GetSQLExecutor returns the SQLExecutor.
func (s *Session) GetSQLExecutor() sqlexec.SQLExecutor {
	return s
}

// ParseWithParams implements the RestrictedSQLExecutor.ParseWithParams interface.
func (s *Session) ParseWithParams(ctx context.Context, sql string, args ...any) (ast.StmtNode, error) {
	sctx, exit, err := s.internal.EnterOperation(s)
	if err != nil {
		return nil, err
	}
	defer exit()
	return sctx.GetRestrictedSQLExecutor().ParseWithParams(ctx, sql, args...)
}

// ExecRestrictedStmt implements the RestrictedSQLExecutor.ExecutePreparedStmt interface.
func (s *Session) ExecRestrictedStmt(ctx context.Context, stmt ast.StmtNode, opts ...sqlexec.OptionFuncAlias,
) ([]chunk.Row, []*resolve.ResultField, error) {
	sctx, exit, err := s.internal.EnterOperation(s)
	if err != nil {
		return nil, nil, err
	}
	defer exit()
	return sctx.GetRestrictedSQLExecutor().ExecRestrictedStmt(ctx, stmt, opts...)
}

// ExecRestrictedSQL implements the RestrictedSQLExecutor.ExecRestrictedSQL interface.
func (s *Session) ExecRestrictedSQL(ctx context.Context, opts []sqlexec.OptionFuncAlias, sql string, args ...any,
) ([]chunk.Row, []*resolve.ResultField, error) {
	sctx, exit, err := s.internal.EnterOperation(s)
	if err != nil {
		return nil, nil, err
	}
	defer exit()
	return sctx.GetRestrictedSQLExecutor().ExecRestrictedSQL(ctx, opts, sql, args...)
}

// GetRestrictedSQLExecutor returns the RestrictedSQLExecutor.
func (s *Session) GetRestrictedSQLExecutor() sqlexec.RestrictedSQLExecutor {
	return s
}

func (s *Session) onBecameOwner(sctx SessionContext) error {
	if mgr := sctx.GetSessionManager(); mgr != nil {
		mgr.StoreInternalSession(sctx)
	}
	return nil
}

func (s *Session) onResignOwner(sctx SessionContext) error {
	if mgr := sctx.GetSessionManager(); mgr != nil {
		mgr.DeleteInternalSession(sctx)
	}
	return nil
}
