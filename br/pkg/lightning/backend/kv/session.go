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
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/lightning/manual"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/pingcap/tidb/util/topsql/stmtstats"
	"go.uber.org/zap"
)

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

type bytesBuf struct {
	buf []byte
	idx int
	cap int
}

func (b *bytesBuf) add(v []byte) []byte {
	start := b.idx
	copy(b.buf[start:], v)
	b.idx += len(v)
	return b.buf[start:b.idx:b.idx]
}

func newBytesBuf(size int) *bytesBuf {
	return &bytesBuf{
		buf: manual.New(size),
		cap: size,
	}
}

func (b *bytesBuf) destroy() {
	if b != nil {
		manual.Free(b.buf)
		b.buf = nil
	}
}

type kvMemBuf struct {
	sync.Mutex
	kv.MemBuffer
	buf           *bytesBuf
	availableBufs []*bytesBuf
	kvPairs       *KvPairs
	size          int
}

func (mb *kvMemBuf) Recycle(buf *bytesBuf) {
	buf.idx = 0
	buf.cap = len(buf.buf)
	mb.Lock()
	mb.availableBufs = append(mb.availableBufs, buf)
	mb.Unlock()
}

func (mb *kvMemBuf) AllocateBuf(size int) {
	mb.Lock()
	size = mathutil.Max(units.MiB, int(utils.NextPowerOfTwo(int64(size)))*2)
	if len(mb.availableBufs) > 0 && mb.availableBufs[0].cap >= size {
		mb.buf = mb.availableBufs[0]
		mb.availableBufs = mb.availableBufs[1:]
	} else {
		mb.buf = newBytesBuf(size)
	}
	mb.Unlock()
}

func (mb *kvMemBuf) Set(k kv.Key, v []byte) error {
	kvPairs := mb.kvPairs
	size := len(k) + len(v)
	if mb.buf == nil || mb.buf.cap-mb.buf.idx < size {
		if mb.buf != nil {
			kvPairs.bytesBuf = mb.buf
		}
		mb.AllocateBuf(size)
	}
	kvPairs.pairs = append(kvPairs.pairs, common.KvPair{
		Key: mb.buf.add(k),
		Val: mb.buf.add(v),
	})
	mb.size += size
	return nil
}

func (mb *kvMemBuf) SetWithFlags(k kv.Key, v []byte, ops ...kv.FlagsOp) error {
	return mb.Set(k, v)
}

func (mb *kvMemBuf) Delete(k kv.Key) error {
	return errors.New("unsupported operation")
}

// Release publish all modifications in the latest staging buffer to upper level.
func (mb *kvMemBuf) Release(h kv.StagingHandle) {
}

func (mb *kvMemBuf) Staging() kv.StagingHandle {
	return 0
}

// Cleanup cleanup the resources referenced by the StagingHandle.
// If the changes are not published by `Release`, they will be discarded.
func (mb *kvMemBuf) Cleanup(h kv.StagingHandle) {}

// Size returns sum of keys and values length.
func (mb *kvMemBuf) Size() int {
	return mb.size
}

// Len returns the number of entries in the DB.
func (t *transaction) Len() int {
	return t.GetMemBuffer().Len()
}

type kvUnionStore struct {
	kvMemBuf
}

func (s *kvUnionStore) GetMemBuffer() kv.MemBuffer {
	return &s.kvMemBuf
}

func (s *kvUnionStore) GetIndexName(tableID, indexID int64) string {
	panic("Unsupported Operation")
}

func (s *kvUnionStore) CacheIndexName(tableID, indexID int64, name string) {
}

func (s *kvUnionStore) CacheTableInfo(id int64, info *model.TableInfo) {
}

// transaction is a trimmed down Transaction type which only supports adding a
// new KV pair.
type transaction struct {
	kv.Transaction
	kvUnionStore
}

func (t *transaction) GetMemBuffer() kv.MemBuffer {
	return &t.kvUnionStore.kvMemBuf
}

func (t *transaction) Discard() {
	// do nothing
}

func (t *transaction) Flush() (int, error) {
	// do nothing
	return 0, nil
}

// Reset implements the kv.MemBuffer interface
func (t *transaction) Reset() {}

// Get implements the kv.Retriever interface
func (t *transaction) Get(ctx context.Context, key kv.Key) ([]byte, error) {
	return nil, kv.ErrNotExist
}

// Iter implements the kv.Retriever interface
func (t *transaction) Iter(k kv.Key, upperBound kv.Key) (kv.Iterator, error) {
	return &invalidIterator{}, nil
}

// Set implements the kv.Mutator interface
func (t *transaction) Set(k kv.Key, v []byte) error {
	return t.kvMemBuf.Set(k, v)
}

// GetTableInfo implements the kv.Transaction interface.
func (t *transaction) GetTableInfo(id int64) *model.TableInfo {
	return nil
}

// CacheTableInfo implements the kv.Transaction interface.
func (t *transaction) CacheTableInfo(id int64, info *model.TableInfo) {
}

// SetAssertion implements the kv.Transaction interface.
func (t *transaction) SetAssertion(key []byte, assertion ...kv.FlagsOp) error {
	return nil
}

// session is a trimmed down Session type which only wraps our own trimmed-down
// transaction type and provides the session variables to the TiDB library
// optimized for Lightning.
type session struct {
	sessionctx.Context
	txn  transaction
	vars *variable.SessionVars
	// currently, we only set `CommonAddRecordCtx`
	values map[fmt.Stringer]interface{}
}

// SessionOptions is the initial configuration of the session.
type SessionOptions struct {
	SQLMode   mysql.SQLMode
	Timestamp int64
	SysVars   map[string]string
	// a seed used for tableKvEncoder's auto random bits value
	AutoRandomSeed int64
}

// NewSession creates a new trimmed down Session matching the options.
func NewSession(options *SessionOptions) sessionctx.Context {
	return newSession(options)
}

func newSession(options *SessionOptions) *session {
	sqlMode := options.SQLMode
	vars := variable.NewSessionVars()
	vars.SkipUTF8Check = true
	vars.StmtCtx.InInsertStmt = true
	vars.StmtCtx.BatchCheck = true
	vars.StmtCtx.BadNullAsWarning = !sqlMode.HasStrictMode()
	vars.StmtCtx.TruncateAsWarning = !sqlMode.HasStrictMode()
	vars.StmtCtx.OverflowAsWarning = !sqlMode.HasStrictMode()
	vars.StmtCtx.AllowInvalidDate = sqlMode.HasAllowInvalidDatesMode()
	vars.StmtCtx.IgnoreZeroInDate = !sqlMode.HasStrictMode() || sqlMode.HasAllowInvalidDatesMode()
	vars.SQLMode = sqlMode
	if options.SysVars != nil {
		for k, v := range options.SysVars {
			if err := vars.SetSystemVar(k, v); err != nil {
				log.L().DPanic("new session: failed to set system var",
					log.ShortError(err),
					zap.String("key", k))
			}
		}
	}
	vars.StmtCtx.TimeZone = vars.Location()
	if err := vars.SetSystemVar("timestamp", strconv.FormatInt(options.Timestamp, 10)); err != nil {
		log.L().Warn("new session: failed to set timestamp",
			log.ShortError(err))
	}
	vars.TxnCtx = nil
	s := &session{
		vars:   vars,
		values: make(map[fmt.Stringer]interface{}, 1),
	}
	s.txn.kvPairs = &KvPairs{}

	return s
}

func (se *session) takeKvPairs() *KvPairs {
	memBuf := &se.txn.kvMemBuf
	pairs := memBuf.kvPairs
	if pairs.bytesBuf != nil {
		pairs.memBuf = memBuf
	}
	memBuf.kvPairs = &KvPairs{pairs: make([]common.KvPair, 0, len(pairs.pairs))}
	memBuf.size = 0
	return pairs
}

// Txn implements the sessionctx.Context interface
func (se *session) Txn(active bool) (kv.Transaction, error) {
	return &se.txn, nil
}

// GetSessionVars implements the sessionctx.Context interface
func (se *session) GetSessionVars() *variable.SessionVars {
	return se.vars
}

// SetValue saves a value associated with this context for key.
func (se *session) SetValue(key fmt.Stringer, value interface{}) {
	se.values[key] = value
}

// Value returns the value associated with this context for key.
func (se *session) Value(key fmt.Stringer) interface{} {
	return se.values[key]
}

// StmtAddDirtyTableOP implements the sessionctx.Context interface
func (se *session) StmtAddDirtyTableOP(op int, physicalID int64, handle kv.Handle) {}

// GetInfoSchema implements the sessionctx.Context interface.
func (se *session) GetInfoSchema() sessionctx.InfoschemaMetaVersion {
	return nil
}

// GetBuiltinFunctionUsage returns the BuiltinFunctionUsage of current Context, which is not thread safe.
// Use primitive map type to prevent circular import. Should convert it to telemetry.BuiltinFunctionUsage before using.
func (se *session) GetBuiltinFunctionUsage() map[string]uint32 {
	return make(map[string]uint32)
}

// BuiltinFunctionUsageInc implements the sessionctx.Context interface.
func (se *session) BuiltinFunctionUsageInc(scalarFuncSigName string) {
}

// GetStmtStats implements the sessionctx.Context interface.
func (se *session) GetStmtStats() *stmtstats.StatementStats {
	return nil
}

func (se *session) Close() {
	memBuf := &se.txn.kvMemBuf
	if memBuf.buf != nil {
		memBuf.buf.destroy()
		memBuf.buf = nil
	}
	for _, b := range memBuf.availableBufs {
		b.destroy()
	}
	memBuf.availableBufs = nil
}
