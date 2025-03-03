package internalsession

import (
	"context"
	"fmt"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)

type SessionContext interface {
	sessionctx.Context
	Close()
}

type contextOwner interface {
	onBecameOwner(SessionContext)
	onResignOwner(SessionContext)
}

func ownerStr(owner contextOwner) string {
	if owner == nil {
		return ""
	}
	return fmt.Sprintf("%T(%p)", owner, owner)
}

type noopContextOwnerHook struct{}

func (noopContextOwnerHook) onBecameOwner(SessionContext) {}

func (noopContextOwnerHook) onResignOwner(SessionContext) {}

type session struct {
	mu sync.Mutex
	// sctx is the inner context
	sctx SessionContext
	// owner the current owner of the session, only the owner the session can use the session.
	// owner == nil indicates it is destroyed
	owner contextOwner
	// useOpSeq is used to identify the operation sequence of the session.
	opSeq uint64
	// inuse is the number of goroutines that are using the session.
	inuse uint64
	// inuseUnsafe is the number of goroutines that are using the session in an un-thread-safe way.
	inuseUnsafe uint64
	// racing is true indicates more than one un-thread-safe operation is ongoing.
	racing bool
	// mayCorrupted is true indicates the session may be corrupted.
	mayCorrupted bool
}

func newInternalSession(sctx SessionContext, owner contextOwner) *session {
	intest.Assert(sctx != nil)
	intest.Assert(owner != nil)
	owner.onBecameOwner(sctx)
	return &session{sctx: sctx, owner: owner}
}

func (s *session) incSequence() uint64 {
	s.opSeq++
	return s.opSeq
}

func (s *session) CheckOwner(check contextOwner) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.owner == check
}

func (s *session) MayCorrupted() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mayCorrupted
}

func (s *session) TransferOwner(from, to contextOwner) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.incSequence()
	if err := s.checkDestroy(); err != nil {
		return err
	}

	if to == nil {
		return errors.New("cannot transfer to a nil owner")
	}

	if s.owner != from {
		return errors.New("session does not belongs to the specified owner")
	}

	if from == to {
		return nil
	}

	if err := s.checkNoInuse(); err != nil {
		return err
	}

	defer func() {
		if s.owner != to {
			// if s.owner != to, it means panic happens.
			// destroy the session here.
			s.owner = nil
			s.sctx.Close()
		}
	}()

	from.onResignOwner(s.sctx)
	to.onBecameOwner(s.sctx)
	s.owner = to
	return nil
}

func (s *session) OwnerDestroy(owner contextOwner) {
	s.mu.Lock()
	defer s.mu.Unlock()
	intest.AssertNotNil(owner)
	if owner != nil {
		s.destroy(owner)
	}
}

func (s *session) destroy(owner contextOwner) {
	s.mu.Lock()
	defer s.mu.Unlock()
	seq := s.incSequence()
	if s.owner == nil {
		return
	}

	// owner == nil means destroy the session without comparing the owner.
	if owner == nil {
		owner = s.owner
	}

	if s.owner != owner {
		return
	}

	defer func() {
		s.owner = nil
		s.sctx.Close()
		s.assertNoError(s.checkNoInuse(), owner, seq)
	}()
	owner.onResignOwner(s.sctx)
}

func (s *session) RollbackTxn(ctx context.Context, owner contextOwner) error {
	sctx, rel, err := s.UseSession(owner, false, false)
	if err != nil {
		return err
	}
	defer rel()
	sctx.RollbackTxn(ctx)
	return nil
}

func (s *session) UseSession(owner contextOwner, threadSafe bool, suppressErr bool) (SessionContext, func(), error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	opSeq := s.incSequence()
	handleErr := func(err error) error {
		if suppressErr {
			s.assertNoError(err, owner, opSeq)
			return nil
		}
		return err
	}

	if err := handleErr(s.checkDestroy()); err != nil {
		return nil, nil, err
	}

	if err := s.checkOwner(owner); err != nil {
		if err = handleErr(err); err != nil {
			return nil, nil, err
		}
		s.mayCorrupted = true
	}

	race := !threadSafe && s.inuseUnsafe > 0
	if race && !suppressErr {
		return nil, nil, errors.New("another unsafe operation ongoing")
	}

	if race {
		s.racing = true
		s.mayCorrupted = true
	}

	s.inuse++
	if !threadSafe {
		s.inuseUnsafe++
	}

	return s.sctx, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.inuse--
		if !threadSafe {
			s.inuseUnsafe--
			race = race || s.racing
			if race {
				s.mayCorrupted = true
			}
			if s.inuseUnsafe == 0 {
				s.racing = false
			}
		}

		exitSeq := s.incSequence()
		if race {
			logutil.BgLogger().Error(
				"data race occurs",
				zap.Uint64("seq", opSeq),
				zap.Uint64("exitSeq", exitSeq),
				zap.String("session", fmt.Sprintf("%p", s)),
				zap.String("owner", ownerStr(owner)),
				zap.Stack("stack"),
			)
			intest.Assert(false, "data race occurs")
		}

		s.assertNoError(s.checkDestroy(), owner, opSeq)
		if err := s.checkOwner(owner); err != nil {
			s.mayCorrupted = true
			s.assertNoError(err, owner, opSeq)
		}

		if r := recover(); r != nil {
			s.mayCorrupted = true
			panic(r)
		}
	}, nil
}

func (s *session) assertNoError(err error, owner contextOwner, seq uint64) {
	if err == nil {
		return
	}

	logutil.BgLogger().Error("unexpected error",
		zap.Uint64("seq", seq),
		zap.String("session", fmt.Sprintf("%p", s)),
		zap.String("owner", ownerStr(owner)),
		zap.Error(err),
		zap.Stack("stack"),
	)
	intest.AssertNoError(err)
}

func (s *session) checkOwner(owner contextOwner) error {
	if s.owner != owner {
		return errors.Newf(
			"%s is not the owner of the session, the current owner is: %s", ownerStr(owner), ownerStr(s.owner),
		)
	}
	return nil
}

func (s *session) checkDestroy() error {
	if s.owner == nil {
		return errors.New("session is destroyed")
	}
	return nil
}

func (s *session) checkNoInuse() error {
	if s.inuse > 0 {
		return errors.New("session is being used by another operation")
	}
	return nil
}

func (s *session) AssertTxnCommitted() {
	s.mu.Lock()
	defer s.mu.Unlock()
	seq := s.incSequence()
	txn, err := s.sctx.Txn(false)
	if err == nil && txn.Valid() {
		err = errors.New("session has transaction uncommitted")
	}
	s.assertNoError(err, s.owner, seq)
}

func (s *session) Destroy() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.destroy(nil)
}

type Session struct {
	internal *session
}

func (s *Session) onBecameOwner(sctx SessionContext) {
	if m := sctx.GetSessionManager(); m != nil {
		m.StoreInternalSession(sctx)
	}
}

func (s *Session) onResignOwner(sctx SessionContext) {
	if m := sctx.GetSessionManager(); m != nil {
		m.DeleteInternalSession(sctx)
	}
}

func (s *Session) Destroy() {
	s.internal.OwnerDestroy(s)
}

func (s *Session) useSession(threadSafe bool) (SessionContext, func(), error) {
	return s.internal.UseSession(s, threadSafe, false)
}

func (s *Session) useSessionSuppressError(threadSafe bool) (SessionContext, func()) {
	sctx, rel, err := s.useSession(threadSafe)
	intest.AssertNoError(err)
	return sctx, rel
}

func (s *Session) ParseWithParams(ctx context.Context, sql string, args ...any) (ast.StmtNode, error) {
	sctx, rel := s.useSessionSuppressError(false)
	defer rel()
	return sctx.GetRestrictedSQLExecutor().ParseWithParams(ctx, sql, args...)
}

func (s *Session) ExecRestrictedStmt(ctx context.Context, stmt ast.StmtNode, opts ...sqlexec.OptionFuncAlias) ([]chunk.Row, []*resolve.ResultField, error) {
	sctx, rel := s.useSessionSuppressError(false)
	defer rel()
	return sctx.GetRestrictedSQLExecutor().ExecRestrictedStmt(ctx, stmt, opts...)
}

func (s *Session) ExecRestrictedSQL(ctx context.Context, opts []sqlexec.OptionFuncAlias, sql string, args ...any) ([]chunk.Row, []*resolve.ResultField, error) {
	sctx, rel := s.useSessionSuppressError(false)
	defer rel()
	return sctx.GetRestrictedSQLExecutor().ExecRestrictedSQL(ctx, opts, sql, args...)
}

// GetRestrictedSQLExecutor returns the sqlexec.RestrictedSQLExecutor.
func (s *Session) GetRestrictedSQLExecutor() sqlexec.RestrictedSQLExecutor {
	return s
}

func (s *Session) Execute(ctx context.Context, sql string) ([]sqlexec.RecordSet, error) {
	sctx, rel := s.useSessionSuppressError(false)
	defer rel()
	return sctx.GetSQLExecutor().Execute(ctx, sql)
}

func (s *Session) ExecuteInternal(ctx context.Context, sql string, args ...any) (sqlexec.RecordSet, error) {
	sctx, rel := s.useSessionSuppressError(false)
	defer rel()
	return sctx.GetSQLExecutor().ExecuteInternal(ctx, sql, args...)
}

func (s *Session) ExecuteStmt(ctx context.Context, stmtNode ast.StmtNode) (sqlexec.RecordSet, error) {
	sctx, rel := s.useSessionSuppressError(false)
	defer rel()
	return sctx.GetSQLExecutor().ExecuteStmt(ctx, stmtNode)
}

func (s *Session) GetSQLExecutor() sqlexec.SQLExecutor {
	return s
}

func (s *Session) WithContext(fn func(SessionContext) error) error {
	sctx, rel := s.useSessionSuppressError(false)
	defer rel()
	return fn(sctx)
}

func (s *Session) GetContext() SessionContext {
	sctx, rel := s.useSessionSuppressError(false)
	defer rel()
	return sctx
}

func (s *Session) GetSessionVars() *variable.SessionVars {
	return s.GetContext().GetSessionVars()
}

func (s *Session) ShowProcess() *util.ProcessInfo {
	return s.GetContext().ShowProcess()
}
