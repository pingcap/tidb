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
	"sync"

	"github.com/docker/go-units"
	"github.com/pingcap/tidb/pkg/expression/exprctx"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/encode"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/manual"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table/tblctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mathutil"
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
func (s *Session) GetTableCtx() tblctx.MutateContext {
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
