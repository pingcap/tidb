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
	"github.com/pingcap/tidb/pkg/domain/infosync"
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
	onBecameOwner(sessionctx.Context) error
	// onResignOwner is a hook that will be called when it resigns the ownership of the session.
	onResignOwner(sessionctx.Context) error
}

func objectStr(obj any) string {
	if obj == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%T(%p)", obj, obj)
}

// noopOwnerHook does nothing when the owner of the session is changed.
type noopOwnerHook struct{}

func (noopOwnerHook) onBecameOwner(sessionctx.Context) error { return nil }

func (noopOwnerHook) onResignOwner(sessionctx.Context) error { return nil }

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
	// seq is a monotone increasing uint64 to provide the unique sequence number for each operation
	seq uint64
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
	seq := s.seqIncWithoutLock()
	errMsgPrefix := fmt.Sprintf("TransferOwner error, opSeq: %d, ", seq)
	if err := s.checkOwnerWithoutLock(from, errMsgPrefix); err != nil {
		return err
	}

	if to == nil {
		return errors.Errorf("%scannot transfer to a nil owner", errMsgPrefix)
	}

	if from == to {
		return nil
	}

	if err := s.checkNotInuseWithoutLock(errMsgPrefix); err != nil {
		return err
	}

	// assign a noop owner to session temporarily to make sure the close in defer can work
	s.owner = noopOwnerHook{}
	defer func() {
		if s.owner != to {
			// This means some error or panic occurs.
			// We should close the session to avoid some unexpected behavior.
			s.doCloseWithoutLock(seq)
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

// IsAvoidReuse returns whether the session should be avoided to reuse.
func (s *session) IsAvoidReuse() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.avoidReuse
}

// OwnerMarkAvoidReuse marks the session as avoid-reuse.
func (s *session) OwnerMarkAvoidReuse(caller sessionOwner) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.owner != nil && caller == s.owner {
		s.avoidReuse = true
	}
}

// Inuse returns the number of operations that are on going on the session.
func (s *session) Inuse() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inuse
}

// CheckNoPendingTxn checks whether some txn is not committed or pending for TSO.
// If it happens, it returns an error.
func (s *session) CheckNoPendingTxn() error {
	if s.sctx.GetPreparedTxnFuture() != nil {
		return errors.New("txn is pending for TSO")
	}

	txn, err := s.sctx.Txn(false)
	if err != nil {
		return err
	}

	if txn.Valid() {
		return errors.New("txn is still valid")
	}

	return nil
}

// OwnerResetState resets the state of the session by owner
func (s *session) OwnerResetState(ctx context.Context, caller sessionOwner) error {
	sctx, exit, err := s.EnterOperation(caller)
	if err != nil {
		return err
	}
	defer exit()
	sctx.RollbackTxn(ctx)
	return nil
}

// EnterOperation enters an operation on the session.
// It receives a caller with the type sessionOwner as the argument,
// and if the session is closed or the caller is not the owner, it will return an error.
// If the operation is entered successfully,
// it will return the internal SessionContext for further usage and a function to exit the operation.
func (s *session) EnterOperation(caller sessionOwner) (SessionContext, func(), error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	seqStart := s.seqIncWithoutLock()
	if err := s.checkOwnerWithoutLock(caller, "EnterOperation error: "); err != nil {
		s.reportErrorWithoutLock(
			err.Error(),
			zap.Uint64("seqStart", seqStart),
			zap.String("caller", objectStr(caller)),
		)
		return nil, nil, err
	}

	s.inuse++
	exit := false
	return s.sctx, func() {
		r := recover()
		s.mu.Lock()
		defer s.mu.Unlock()
		seqEnd := s.seqIncWithoutLock()
		if exit {
			s.reportErrorWithoutLock(
				"EnterOperation error: multiple exits for the same operation",
				zap.Uint64("seqStart", seqStart),
				zap.Uint64("seqEnd", seqEnd),
				zap.String("caller", objectStr(caller)),
			)
			return
		}

		exit = true
		s.inuse--
		if s.owner != caller {
			s.reportErrorWithoutLock(
				"ExitOperation error: session owner transferred when executing operation",
				zap.Uint64("seqStart", seqStart),
				zap.Uint64("seqEnd", seqEnd),
				zap.String("caller", objectStr(caller)),
			)
		}
		if r != nil {
			logutil.BgLogger().Error(
				"EnterOperation error: panic occurs, avoid to use session again",
				zap.Any("panic", r),
				zap.Uint64("seqStart", seqStart),
				zap.Uint64("seqEnd", seqEnd),
				zap.String("sctx", objectStr(s.sctx)),
				zap.String("caller", objectStr(caller)),
				zap.String("owner", objectStr(s.owner)),
				zap.Stack("stack"),
			)
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
		s.doCloseWithoutLock(s.seqIncWithoutLock())
	}
}

// Close closes the session without checking the owner.
func (s *session) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.doCloseWithoutLock(s.seqIncWithoutLock())
}

// doCloseWithoutLock closes the session.
// It should be called with the protection of the mutex.
func (s *session) doCloseWithoutLock(seq uint64) {
	if s.owner == nil {
		return
	}

	defer func() {
		s.owner = nil
		s.sctx.Close()
	}()

	if err := s.owner.onResignOwner(s.sctx); err != nil {
		s.reportErrorWithoutLock(
			"error occurs when resigning the owner",
			zap.Uint64("seq", seq),
			zap.Error(err),
		)
	}

	if s.inuse > 0 {
		s.reportErrorWithoutLock(
			"session owner is still in use when closing",
			zap.Uint64("inuse", s.inuse),
			zap.Uint64("seq", seq),
		)
	}
}

// checkNotInuseWithoutLock returns an error if the session is still in use.
// It should be called with the protection of the mutex.
func (s *session) checkNotInuseWithoutLock(errMsgPrefix string) error {
	if s.inuse > 0 {
		return errors.Errorf("%ssession is still inuse: %d", errMsgPrefix, s.inuse)
	}
	return nil
}

// checkOwnerWithoutLock returns an error if the session is closed or the caller is not the owner.
// It should be called with the protection of the mutex.
func (s *session) checkOwnerWithoutLock(caller sessionOwner, errMsgPrefix string) error {
	var msg string
	switch {
	case s.owner == nil:
		msg = "session is closed"
	case s.owner != caller:
		msg = "caller is not the owner"
	default:
		return nil
	}
	return errors.Errorf("%s%s, caller: %s, owner: %s", errMsgPrefix, msg, objectStr(caller), objectStr(s.owner))
}

// seqIncWithoutLock increases the sequence and returns the new one
func (s *session) seqIncWithoutLock() uint64 {
	s.seq++
	return s.seq
}

// reportErrorWithoutLock does not return an error to the caller, it logs some message and stack information instead.
// However, it will panic in the test environment to make the test fail.
func (s *session) reportErrorWithoutLock(msg string, fields ...zap.Field) {
	fields = append(fields,
		zap.String("sctx", objectStr(s.sctx)),
		zap.String("owner", objectStr(s.owner)),
		zap.Stack("stack"),
	)
	logutil.BgLogger().Error(msg, fields...)
	intest.Assert(suppressAssertInTest, msg)
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

// IsOwner returns whether the Session is the owner of the internal one.
func (s *Session) IsOwner() bool {
	return s.internal.Owner() == s
}

// AvoidReuse avoids to reuse the session
func (s *Session) AvoidReuse() {
	s.internal.OwnerMarkAvoidReuse(s)
}

// WithSessionContext executes the input function with the session context.
func (s *Session) WithSessionContext(fn func(ctx sessionctx.Context) error) error {
	sctx, exit, err := s.internal.EnterOperation(s)
	if err != nil {
		return err
	}
	defer exit()
	return fn(sctx)
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

func (*Session) onBecameOwner(sctx sessionctx.Context) error {
	infosync.StoreInternalSession(sctx)
	return nil
}

func (*Session) onResignOwner(sctx sessionctx.Context) error {
	infosync.DeleteInternalSession(sctx)
	return nil
}
