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

func resignOwnerAndCloseSctx(owner sessionOwner, sctx SessionContext) error {
	intest.AssertNotNil(owner)
	intest.AssertNotNil(sctx)
	defer sctx.Close()
	return owner.onResignOwner(sctx)
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

// `session` is a private struct, accessible only within the same package.
// It encapsulates `SessionContext` and provides controlled access via dedicated methods.
//
// The `session` is primarily used by `AdvancedSessionPool` and `Session`:
//   - `AdvancedSessionPool` manages a pool of `session` instances, performing state validation
//     when acquiring or releasing them.
//   - `Session` is the public-facing structure that external callers use. It acts as a proxy
//     for `session`, ensuring restricted access to `SessionContext`.
//
// To safeguard the internal `SessionContext`, `session` includes additional fields:
//
//   - `owner`: Represents the entity that "owns" the session, ensuring only the owner can
//     perform operations on it.
//     The `session.OwnerWithSctx(caller, fn)` method verifies whether `caller` is the
//     current owner before executing `fn`, returning an error if not.
//     Ownership can be transferred using `TransferOwner`.
//     The session can be closed via `Close` or `OwnerClose`, which resets the owner to `nil`.
//
//   - `inUse`: A counter tracking the number of ongoing operations on the session.
//     `session.OwnerWithSctx(caller, fn)` increments `inUse` at the start of an operation
//     and decrements it upon completion.
//     When `inUse == 0`, the session is idle, allowing ownership transfer or closure.
//     When `inUse > 0`, ongoing operations exist, leading to:
//     `session.TransferOwner` failing to prevent concurrent access by different owners.
//     `session.Close` triggering a panic in tests, but logging a warning in production.
//
//   - `unsafe`: A counter to detect race conditions in thread-unsafe operations.
//     When `s.unsafe == 0`, it means no thread-unsafe operations are in progress (expected case).
//     When `s.unsafe == 1`, it means a thread-unsafe operation is in progress (expected case).
//     When `s.unsafe > 1`, it means race detected for a progressing thread-unsafe operation (unexpected case),
//     and only the first operation is allowed to execute at this time.
//
//   - `seq`: A monotonically increasing `uint64` used for logging and debugging.
//     It provides a unique sequence number for each operation.
//     In `EnterOperation`, `seq` increments to mark an operation’s start and increments again upon completion.
//     These sequence numbers help track operation order and diagnose unexpected behaviors.
//
// The conventions for methods in `session`:
//   - The methods which have the uppercase prefix can be accessed by the other structs such as `Session` and `Pool`.
//   - The methods which have the lowercase prefix are private even to the other structs in the same package.
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
	inUse uint64
	// `unsafe` is used to detect race conditions in thread-unsafe operations.
	// - `s.unsafe == 0` means no thread-unsafe operations are in progress (expected).
	// - `s.unsafe == 1` means a thread-unsafe operation is in progress (expected).
	// - `s.unsafe > 1` means race detected for a progressing thread-unsafe operation (unexpected).
	//    The value `s.unsafe - 1` indicates how many thread-unsafe operations are conflicted with the first one.
	//    Only the first operation is allowed to execute at this time.
	//
	// Below is an explanation of how this value changes in both expected and unexpected scenarios:
	//
	// - **Expected scenario**: All thread-unsafe methods are called sequentially.
	//   - `unsafe` increases from `0` to `1` when an operation begins and decreases back to `0` when it completes.
	//
	// - **Unexpected scenario**: Multiple threads invoke thread-unsafe methods concurrently.
	//   1. The first thread starts execution, incrementing `unsafe` (`inuse == 1` at this point).
	//   2. A second thread attempts to start execution, also incrementing `unsafe`.
	//      Since a race condition is detected (`inuse > 0`), the operation is rejected and an error is returned.
	//      The second thread does not decrement `unsafe`, leaving `inuse == 2` after it exits.
	//   3. When the first thread completes, it also detects the race condition (`inuse == 2`).
	//      Then it logs a message indicating the issue and resets `inuse` to `0`.
	unsafe uint64
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
	return s.inUse
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
	return s.OwnerWithSctx(caller, func(sctx SessionContext) error {
		sctx.RollbackTxn(ctx)
		return nil
	})
}

// OwnerWithSctx executes the input function with the session context.
func (s *session) OwnerWithSctx(caller sessionOwner, fn func(SessionContext) error) error {
	intest.AssertNotNil(fn)
	sctx, exit, err := s.EnterOperation(caller, false)
	if err != nil {
		return err
	}
	defer exit()
	return fn(sctx)
}

// EnterOperation enters an operation on the session.
// It receives a caller with the type sessionOwner as the argument,
// and if the session is closed or the caller is not the owner, it will return an error.
// If the operation is entered successfully,
// it will return the internal SessionContext for further usage and a function to exit the operation.
func (s *session) EnterOperation(caller sessionOwner, threadSafe bool) (SessionContext, func(), error) {
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

	if !threadSafe {
		s.unsafe++
		if s.unsafe > 1 {
			msg := "EnterOperation error: race detected for concurrent thread-unsafe operations"
			s.reportErrorWithoutLock(
				msg,
				zap.Uint64("seqStart", seqStart),
				zap.Uint64("unsafeCnt", s.unsafe),
				zap.String("caller", objectStr(caller)),
			)
			return nil, nil, errors.New(msg)
		}
	}
	s.inUse++
	exit := false
	return s.sctx, func() {
		var seqEnd uint64
		if r := recover(); r != nil {
			s.OwnerMarkAvoidReuse(caller)
			defer func() {
				// The panic is from the proxied internal operation.
				// If it happens, the defer function should also panic here to keep the same behavior as directly called.
				// The panic from internal operation has higher priority to throw than the one from the assertions.
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
				panic(r)
			}()
		}

		s.mu.Lock()
		defer s.mu.Unlock()
		seqEnd = s.seqIncWithoutLock()
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
		s.inUse--
		unsafeCnt := uint64(0)
		if !threadSafe {
			unsafeCnt = s.unsafe
			s.unsafe = 0
		}

		if s.owner == nil && s.inUse == 0 {
			// `s.owner == nil` here indicates the session is closed, but to avoid data race
			// it did not call `s.sctx.Close()` in `doCloseWithoutLock` before.
			// So when the `inUse` decreased to 0, we need to call `s.sctx.Close()` make sure the session is closed.
			// The first argument `resignOwnerAndCloseSctx` is the owner that should call the hook `resignOwner`,
			// and we use the `caller` here which is exactly the owner before session closing.
			if err := resignOwnerAndCloseSctx(caller, s.sctx); err != nil {
				s.reportErrorWithoutLock(
					"ExitOperation error: error occurs when close context and resigning the owner",
					zap.Error(err),
					zap.Uint64("seqStart", seqStart),
					zap.Uint64("seqEnd", seqEnd),
					zap.String("caller", objectStr(caller)),
				)
			}
		}

		if s.owner != caller {
			s.reportErrorWithoutLock(
				"ExitOperation error: session owner transferred when executing operation",
				zap.Uint64("seqStart", seqStart),
				zap.Uint64("seqEnd", seqEnd),
				zap.String("caller", objectStr(caller)),
			)
		}

		if unsafeCnt > 1 {
			s.reportErrorWithoutLock(
				"ExitOperation error: race detected for concurrent thread-unsafe operations",
				zap.Uint64("seqStart", seqStart),
				zap.Uint64("seqEnd", seqEnd),
				zap.Uint64("unsafeCnt", unsafeCnt),
				zap.String("caller", objectStr(caller)),
			)
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
// If the session is already closed, it will return without doing anything.
func (s *session) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.doCloseWithoutLock(s.seqIncWithoutLock())
}

// doCloseWithoutLock closes the session.
// It should be called with the protection of the mutex.
func (s *session) doCloseWithoutLock(seq uint64) {
	if s.owner == nil {
		// `s.owner == nil` means it is already closed, do nothing.
		return
	}

	defer func() {
		s.owner = nil
	}()

	if s.inUse > 0 {
		// `s.inUse > 0` indicates some operation(s) are still ongoing on the session.
		// Though unexpected, it's better to avoid unnecessary data race here to postpone
		// the `p.sctx.Close` after all operations are finished (i.e. `s.inuse` decreased to 0).
		s.reportErrorWithoutLock(
			"session owner is still in use when closing",
			zap.Uint64("inUse", s.inUse),
			zap.Uint64("seq", seq),
		)
		return
	}

	if err := resignOwnerAndCloseSctx(s.owner, s.sctx); err != nil {
		s.reportErrorWithoutLock(
			"error occurs when close context and resigning the owner",
			zap.Uint64("seq", seq),
			zap.Error(err),
		)
	}
}

// checkNotInuseWithoutLock returns an error if the session is still in use.
// It should be called with the protection of the mutex.
func (s *session) checkNotInuseWithoutLock(errMsgPrefix string) error {
	if s.inUse > 0 {
		return errors.Errorf("%ssession is still inUse: %d", errMsgPrefix, s.inUse)
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
func (s *Session) WithSessionContext(fn func(sessionctx.Context) error) error {
	intest.AssertNotNil(fn)
	return s.internal.OwnerWithSctx(s, func(sctx SessionContext) error {
		return fn(sctx)
	})
}

// Execute implements the SQLExecutor.Execute interface.
func (s *Session) Execute(ctx context.Context, sql string) ([]sqlexec.RecordSet, error) {
	sctx, exit, err := s.internal.EnterOperation(s, false)
	if err != nil {
		return nil, err
	}
	defer exit()
	return sctx.GetSQLExecutor().Execute(ctx, sql)
}

// ExecuteInternal implements the SQLExecutor.ExecuteInternal interface.
func (s *Session) ExecuteInternal(ctx context.Context, sql string, args ...any) (sqlexec.RecordSet, error) {
	sctx, exit, err := s.internal.EnterOperation(s, false)
	if err != nil {
		return nil, err
	}
	defer exit()
	return sctx.GetSQLExecutor().ExecuteInternal(ctx, sql, args...)
}

// ExecuteStmt implements the SQLExecutor.ExecuteStmt interface.
func (s *Session) ExecuteStmt(ctx context.Context, stmtNode ast.StmtNode) (sqlexec.RecordSet, error) {
	sctx, exit, err := s.internal.EnterOperation(s, false)
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
	sctx, exit, err := s.internal.EnterOperation(s, false)
	if err != nil {
		return nil, err
	}
	defer exit()
	return sctx.GetRestrictedSQLExecutor().ParseWithParams(ctx, sql, args...)
}

// ExecRestrictedStmt implements the RestrictedSQLExecutor.ExecutePreparedStmt interface.
func (s *Session) ExecRestrictedStmt(ctx context.Context, stmt ast.StmtNode, opts ...sqlexec.OptionFuncAlias,
) ([]chunk.Row, []*resolve.ResultField, error) {
	sctx, exit, err := s.internal.EnterOperation(s, false)
	if err != nil {
		return nil, nil, err
	}
	defer exit()
	return sctx.GetRestrictedSQLExecutor().ExecRestrictedStmt(ctx, stmt, opts...)
}

// ExecRestrictedSQL implements the RestrictedSQLExecutor.ExecRestrictedSQL interface.
func (s *Session) ExecRestrictedSQL(ctx context.Context, opts []sqlexec.OptionFuncAlias, sql string, args ...any,
) ([]chunk.Row, []*resolve.ResultField, error) {
	sctx, exit, err := s.internal.EnterOperation(s, false)
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
