// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// TODO combine with the pkg/kv package outside.

package kv

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/docker/go-units"
	"github.com/pingcap/tidb/pkg/errctx"
	exprctx "github.com/pingcap/tidb/pkg/expression/context"
	exprctximpl "github.com/pingcap/tidb/pkg/expression/contextsession"
	infoschema "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/encode"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/manual"
	"github.com/pingcap/tidb/pkg/meta/model"
	planctx "github.com/pingcap/tidb/pkg/planner/context"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	tbctx "github.com/pingcap/tidb/pkg/table/context"
	tbctximpl "github.com/pingcap/tidb/pkg/table/contextimpl"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mathutil"
	"github.com/pingcap/tidb/pkg/util/topsql/stmtstats"
	"go.uber.org/zap"
)

const maxAvailableBufSize int = 20

// invalidIterator is a trimmed down Iterator type which is invalid.
type invalidIterator struct {
	kv.Iterator
}

// Valid implements the kv.Iterator interface
func (*invalidIterator) Valid() bool {
	return false
}

// Close implements the kv.Iterator interface
func (*invalidIterator) Close() {
}

// BytesBuf bytes buffer.
type BytesBuf struct {
	buf []byte
	idx int
	cap int
}

func (b *BytesBuf) add(v []byte) []byte {
	start := b.idx
	copy(b.buf[start:], v)
	b.idx += len(v)
	return b.buf[start:b.idx:b.idx]
}

func newBytesBuf(size int) *BytesBuf {
	return &BytesBuf{
		buf: manual.New(size),
		cap: size,
	}
}

func (b *BytesBuf) destroy() {
	if b != nil {
		manual.Free(b.buf)
		b.buf = nil
	}
}

// MemBuf used to store the data in memory.
type MemBuf struct {
	sync.Mutex
	kv.MemBuffer
	buf           *BytesBuf
	availableBufs []*BytesBuf
	kvPairs       *Pairs
	size          int
}

// Recycle recycles the byte buffer.
func (mb *MemBuf) Recycle(buf *BytesBuf) {
	buf.idx = 0
	buf.cap = len(buf.buf)
	mb.Lock()
	if len(mb.availableBufs) >= maxAvailableBufSize {
		// too many byte buffers, evict one byte buffer and continue
		evictedByteBuf := mb.availableBufs[0]
		evictedByteBuf.destroy()
		mb.availableBufs = mb.availableBufs[1:]
	}
	mb.availableBufs = append(mb.availableBufs, buf)
	mb.Unlock()
}

// AllocateBuf allocates a byte buffer.
func (mb *MemBuf) AllocateBuf(size int) {
	mb.Lock()
	size = max(units.MiB, int(mathutil.NextPowerOfTwo(int64(size)))*2)
	var (
		existingBuf    *BytesBuf
		existingBufIdx int
	)
	for i, buf := range mb.availableBufs {
		if buf.cap >= size {
			existingBuf = buf
			existingBufIdx = i
			break
		}
	}
	if existingBuf != nil {
		mb.buf = existingBuf
		mb.availableBufs[existingBufIdx] = mb.availableBufs[0]
		mb.availableBufs = mb.availableBufs[1:]
	} else {
		mb.buf = newBytesBuf(size)
	}
	mb.Unlock()
}

// Set sets the key-value pair.
func (mb *MemBuf) Set(k kv.Key, v []byte) error {
	kvPairs := mb.kvPairs
	size := len(k) + len(v)
	if mb.buf == nil || mb.buf.cap-mb.buf.idx < size {
		if mb.buf != nil {
			kvPairs.BytesBuf = mb.buf
		}
		mb.AllocateBuf(size)
	}
	kvPairs.Pairs = append(kvPairs.Pairs, common.KvPair{
		Key: mb.buf.add(k),
		Val: mb.buf.add(v),
	})
	mb.size += size
	return nil
}

// SetWithFlags implements the kv.MemBuffer interface.
func (mb *MemBuf) SetWithFlags(k kv.Key, v []byte, _ ...kv.FlagsOp) error {
	return mb.Set(k, v)
}

// Delete implements the kv.MemBuffer interface.
func (*MemBuf) Delete(_ kv.Key) error {
	return errors.New("unsupported operation")
}

// Release publish all modifications in the latest staging buffer to upper level.
func (*MemBuf) Release(_ kv.StagingHandle) {
}

// Staging creates a new staging buffer.
func (*MemBuf) Staging() kv.StagingHandle {
	return 0
}

// Cleanup the resources referenced by the StagingHandle.
// If the changes are not published by `Release`, they will be discarded.
func (*MemBuf) Cleanup(_ kv.StagingHandle) {}

// GetLocal implements the kv.MemBuffer interface.
func (mb *MemBuf) GetLocal(ctx context.Context, key []byte) ([]byte, error) {
	return mb.Get(ctx, key)
}

// Size returns sum of keys and values length.
func (mb *MemBuf) Size() int {
	return mb.size
}

// Len returns the number of entries in the DB.
func (t *transaction) Len() int {
	return t.GetMemBuffer().Len()
}

type kvUnionStore struct {
	MemBuf
}

// GetMemBuffer implements the kv.UnionStore interface.
func (s *kvUnionStore) GetMemBuffer() kv.MemBuffer {
	return &s.MemBuf
}

// GetIndexName implements the kv.UnionStore interface.
func (*kvUnionStore) GetIndexName(_, _ int64) string {
	panic("Unsupported Operation")
}

// CacheIndexName implements the kv.UnionStore interface.
func (*kvUnionStore) CacheIndexName(_, _ int64, _ string) {
}

// CacheTableInfo implements the kv.UnionStore interface.
func (*kvUnionStore) CacheTableInfo(_ int64, _ *model.TableInfo) {
}

// transaction is a trimmed down Transaction type which only supports adding a
// new KV pair.
type transaction struct {
	kv.Transaction
	kvUnionStore
}

// GetMemBuffer implements the kv.Transaction interface.
func (t *transaction) GetMemBuffer() kv.MemBuffer {
	return &t.kvUnionStore.MemBuf
}

// Discard implements the kv.Transaction interface.
func (*transaction) Discard() {
	// do nothing
}

// Flush implements the kv.Transaction interface.
func (*transaction) Flush() (int, error) {
	// do nothing
	return 0, nil
}

// Reset implements the kv.MemBuffer interface
func (*transaction) Reset() {}

// Get implements the kv.Retriever interface
func (*transaction) Get(_ context.Context, _ kv.Key) ([]byte, error) {
	return nil, kv.ErrNotExist
}

// Iter implements the kv.Retriever interface
func (*transaction) Iter(_ kv.Key, _ kv.Key) (kv.Iterator, error) {
	return &invalidIterator{}, nil
}

// Set implements the kv.Mutator interface
func (t *transaction) Set(k kv.Key, v []byte) error {
	return t.MemBuf.Set(k, v)
}

// GetTableInfo implements the kv.Transaction interface.
func (*transaction) GetTableInfo(_ int64) *model.TableInfo {
	return nil
}

// CacheTableInfo implements the kv.Transaction interface.
func (*transaction) CacheTableInfo(_ int64, _ *model.TableInfo) {
}

// SetAssertion implements the kv.Transaction interface.
func (*transaction) SetAssertion(_ []byte, _ ...kv.FlagsOp) error {
	return nil
}

// IsPipelined implements the kv.Transaction interface.
func (*transaction) IsPipelined() bool {
	return false
}

// MayFlush implements the kv.Transaction interface.
func (*transaction) MayFlush() error {
	return nil
}

// sessExprContext implements the ExprContext interface
// It embedded an `ExprContext` and a `sessEvalContext` to provide no optional properties.
type sessExprContext struct {
	*exprctximpl.SessionExprContext
	evalCtx *sessEvalContext
}

// GetEvalCtx implements the ExprContext.GetEvalCtx interface
func (ctx *sessExprContext) GetEvalCtx() exprctx.EvalContext {
	return ctx.evalCtx
}

// sessEvalContext implements the EvalContext interface
// It embedded an `EvalContext` and provide no optional properties.
type sessEvalContext struct {
	exprctx.EvalContext
}

// GetOptionalPropSet returns the optional properties provided by this context.
func (*sessEvalContext) GetOptionalPropSet() exprctx.OptionalEvalPropKeySet {
	return 0
}

// GetOptionalPropProvider gets the optional property provider by key
func (*sessEvalContext) GetOptionalPropProvider(exprctx.OptionalEvalPropKey) (exprctx.OptionalEvalPropProvider, bool) {
	return nil, false
}

// Deprecated: `session` will be removed soon.
// session is a trimmed down Session type which only wraps our own trimmed-down
// transaction type and provides the session variables to the TiDB library
// optimized for Lightning.
// The `session` object is private to make sure it is only used by public `Session` struct to provide limited access.
// TODO: remove `session` and build related context without a mocked `sessionctx.Context` instead.
type session struct {
	sessionctx.Context
	planctx.EmptyPlanContextExtended
	txn     transaction
	Vars    *variable.SessionVars
	exprCtx *sessExprContext
	tblctx  *tbctximpl.TableContextImpl
	// currently, we only set `CommonAddRecordCtx`
	values map[fmt.Stringer]any
}

// Deprecated: this function will be removed soon.
// newSession creates a new trimmed down Session matching the options.
func newSession(options *encode.SessionOptions, logger log.Logger) *session {
	s := &session{
		values: make(map[fmt.Stringer]any, 1),
	}
	sqlMode := options.SQLMode
	vars := variable.NewSessionVars(s)
	vars.SkipUTF8Check = true
	vars.StmtCtx.InInsertStmt = true
	vars.SQLMode = sqlMode

	typeFlags := vars.StmtCtx.TypeFlags().
		WithTruncateAsWarning(!sqlMode.HasStrictMode()).
		WithIgnoreInvalidDateErr(sqlMode.HasAllowInvalidDatesMode()).
		WithIgnoreZeroInDate(!sqlMode.HasStrictMode() || sqlMode.HasAllowInvalidDatesMode() ||
			!sqlMode.HasNoZeroInDateMode() || !sqlMode.HasNoZeroDateMode())
	vars.StmtCtx.SetTypeFlags(typeFlags)

	errLevels := vars.StmtCtx.ErrLevels()
	errLevels[errctx.ErrGroupBadNull] = errctx.ResolveErrLevel(false, !sqlMode.HasStrictMode())
	errLevels[errctx.ErrGroupDividedByZero] =
		errctx.ResolveErrLevel(!sqlMode.HasErrorForDivisionByZeroMode(), !sqlMode.HasStrictMode())
	vars.StmtCtx.SetErrLevels(errLevels)

	if options.SysVars != nil {
		for k, v := range options.SysVars {
			// since 6.3(current master) tidb checks whether we can set a system variable
			// lc_time_names is a read-only variable for now, but might be implemented later,
			// so we not remove it from defaultImportantVariables and check it in below way.
			if sv := variable.GetSysVar(k); sv == nil {
				logger.DPanic("unknown system var", zap.String("key", k))
				continue
			} else if sv.ReadOnly {
				logger.Debug("skip read-only variable", zap.String("key", k))
				continue
			}
			if err := vars.SetSystemVar(k, v); err != nil {
				logger.DPanic("new session: failed to set system var",
					log.ShortError(err),
					zap.String("key", k))
			}
		}
	}
	vars.StmtCtx.SetTimeZone(vars.Location())
	if err := vars.SetSystemVar("timestamp", strconv.FormatInt(options.Timestamp, 10)); err != nil {
		logger.Warn("new session: failed to set timestamp",
			log.ShortError(err))
	}
	vars.TxnCtx = nil
	s.Vars = vars
	exprCtx := exprctximpl.NewSessionExprContext(s)
	// The exprCtx should be an expression context providing no optional properties in `EvalContext`.
	// That is to make sure it only allows expressions that require basic context.
	s.exprCtx = &sessExprContext{
		SessionExprContext: exprCtx,
		evalCtx: &sessEvalContext{
			EvalContext: exprCtx.GetEvalCtx(),
		},
	}
	s.tblctx = tbctximpl.NewTableContextImpl(s)
	s.txn.kvPairs = &Pairs{}

	return s
}

// Txn implements the sessionctx.Context interface
func (se *session) Txn(_ bool) (kv.Transaction, error) {
	return &se.txn, nil
}

// GetSessionVars implements the sessionctx.Context interface
func (se *session) GetSessionVars() *variable.SessionVars {
	return se.Vars
}

// GetExprCtx returns the expression context of the session.
func (se *session) GetExprCtx() exprctx.ExprContext {
	return se.exprCtx
}

// GetTableCtx returns the table.MutateContext
func (se *session) GetTableCtx() tbctx.MutateContext {
	return se.tblctx
}

// SetValue saves a value associated with this context for key.
func (se *session) SetValue(key fmt.Stringer, value any) {
	se.values[key] = value
}

// Value returns the value associated with this context for key.
func (se *session) Value(key fmt.Stringer) any {
	return se.values[key]
}

// StmtAddDirtyTableOP implements the sessionctx.Context interface
func (*session) StmtAddDirtyTableOP(_ int, _ int64, _ kv.Handle) {}

// GetInfoSchema implements the sessionctx.Context interface.
func (*session) GetInfoSchema() infoschema.MetaOnlyInfoSchema {
	return nil
}

// GetStmtStats implements the sessionctx.Context interface.
func (*session) GetStmtStats() *stmtstats.StatementStats {
	return nil
}

// Session is used to provide context for lightning.
type Session struct {
	txn     transaction
	exprCtx *litExprContext
	tblCtx  *litTableMutateContext
}

// NewSession creates a new Session.
func NewSession(options *encode.SessionOptions, logger log.Logger) (*Session, error) {
	sysVars := make(map[string]string, len(options.SysVars))
	if options.SysVars != nil {
		// This sessVars is only used to do validations.
		sessVars := variable.NewSessionVars(nil)
		// To keep compatible with the old versions, we should to skip errors caused by illegal system variables.
		for k, v := range options.SysVars {
			// since 6.3(current master) tidb checks whether we can set a system variable
			// lc_time_names is a read-only variable for now, but might be implemented later,
			// so we not remove it from defaultImportantVariables and check it in below way.
			if sv := variable.GetSysVar(k); sv == nil {
				logger.DPanic("unknown system var", zap.String("key", k))
				continue
			} else if sv.ReadOnly {
				logger.Debug("skip read-only variable", zap.String("key", k))
				continue
			}
			if err := sessVars.SetSystemVar(k, v); err != nil {
				logger.DPanic("new session: failed to set system var",
					log.ShortError(err),
					zap.String("key", k))
				continue
			}
			sysVars[k] = v
		}
	}

	exprCtx, err := newLitExprContext(options.SQLMode, sysVars, options.Timestamp)
	if err != nil {
		return nil, err
	}

	tblCtx, err := newLitTableMutateContext(exprCtx, sysVars)
	if err != nil {
		return nil, err
	}

	s := &Session{
		exprCtx: exprCtx,
		tblCtx:  tblCtx,
	}
	s.txn.kvPairs = &Pairs{}
	return s, nil
}

// GetExprCtx returns the expression context
func (s *Session) GetExprCtx() exprctx.ExprContext {
	return s.exprCtx
}

// Txn returns the internal txn.
func (s *Session) Txn() kv.Transaction {
	return &s.txn
}

// GetTableCtx returns the table MutateContext.
func (s *Session) GetTableCtx() tbctx.MutateContext {
	return s.tblCtx
}

// TakeKvPairs returns the current Pairs and resets the buffer.
func (s *Session) TakeKvPairs() *Pairs {
	memBuf := &s.txn.MemBuf
	pairs := memBuf.kvPairs
	if pairs.BytesBuf != nil {
		pairs.MemBuf = memBuf
	}
	memBuf.kvPairs = &Pairs{Pairs: make([]common.KvPair, 0, len(pairs.Pairs))}
	memBuf.size = 0
	return pairs
}

// SetUserVarVal sets the value of a user variable.
func (s *Session) SetUserVarVal(name string, dt types.Datum) {
	s.exprCtx.setUserVarVal(name, dt)
}

// UnsetUserVar unsets a user variable.
func (s *Session) UnsetUserVar(varName string) {
	s.exprCtx.unsetUserVar(varName)
}

// GetColumnSize returns the size of each column.
func (s *Session) GetColumnSize(tblID int64) (ret map[int64]int64) {
	return s.tblCtx.GetColumnSize(tblID)
}

// Close closes the session
func (s *Session) Close() {
	memBuf := &s.txn.MemBuf
	if memBuf.buf != nil {
		memBuf.buf.destroy()
		memBuf.buf = nil
	}
	for _, b := range memBuf.availableBufs {
		b.destroy()
	}
	memBuf.availableBufs = nil
}
