// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package txn

import (
	"context"
	"sync/atomic"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	derr "github.com/pingcap/tidb/store/driver/error"
	"github.com/pingcap/tidb/store/tikv"
	tikverr "github.com/pingcap/tidb/store/tikv/error"
	tikvstore "github.com/pingcap/tidb/store/tikv/kv"
	"github.com/pingcap/tidb/tablecodec"
)

type tikvTxn struct {
	*tikv.KVTxn
	idxNameCache map[int64]*model.TableInfo
}

// NewTiKVTxn returns a new Transaction.
func NewTiKVTxn(txn *tikv.KVTxn) kv.Transaction {
	txn.SetKVFilter(TiDBKVFilter{})

	entryLimit := atomic.LoadUint64(&kv.TxnEntrySizeLimit)
	totalLimit := atomic.LoadUint64(&kv.TxnTotalSizeLimit)
	txn.GetUnionStore().SetEntrySizeLimit(entryLimit, totalLimit)

	return &tikvTxn{txn, make(map[int64]*model.TableInfo)}
}

func (txn *tikvTxn) GetTableInfo(id int64) *model.TableInfo {
	return txn.idxNameCache[id]
}

func (txn *tikvTxn) CacheTableInfo(id int64, info *model.TableInfo) {
	txn.idxNameCache[id] = info
}

// lockWaitTime in ms, except that kv.LockAlwaysWait(0) means always wait lock, kv.LockNowait(-1) means nowait lock
func (txn *tikvTxn) LockKeys(ctx context.Context, lockCtx *kv.LockCtx, keysInput ...kv.Key) error {
	keys := toTiKVKeys(keysInput)
	err := txn.KVTxn.LockKeys(ctx, lockCtx, keys...)
	return txn.extractKeyErr(err)
}

func (txn *tikvTxn) Commit(ctx context.Context) error {
	err := txn.KVTxn.Commit(ctx)
	return txn.extractKeyErr(err)
}

// GetSnapshot returns the Snapshot binding to this transaction.
func (txn *tikvTxn) GetSnapshot() kv.Snapshot {
	return &tikvSnapshot{txn.KVTxn.GetSnapshot()}
}

// Iter creates an Iterator positioned on the first entry that k <= entry's key.
// If such entry is not found, it returns an invalid Iterator with no error.
// It yields only keys that < upperBound. If upperBound is nil, it means the upperBound is unbounded.
// The Iterator must be Closed after use.
func (txn *tikvTxn) Iter(k kv.Key, upperBound kv.Key) (kv.Iterator, error) {
	it, err := txn.KVTxn.Iter(k, upperBound)
	return newKVIterator(it), derr.ToTiDBErr(err)
}

// IterReverse creates a reversed Iterator positioned on the first entry which key is less than k.
// The returned iterator will iterate from greater key to smaller key.
// If k is nil, the returned iterator will be positioned at the last key.
// TODO: Add lower bound limit
func (txn *tikvTxn) IterReverse(k kv.Key) (kv.Iterator, error) {
	it, err := txn.KVTxn.IterReverse(k)
	return newKVIterator(it), derr.ToTiDBErr(err)
}

// BatchGet gets kv from the memory buffer of statement and transaction, and the kv storage.
// Do not use len(value) == 0 or value == nil to represent non-exist.
// If a key doesn't exist, there shouldn't be any corresponding entry in the result map.
func (txn *tikvTxn) BatchGet(ctx context.Context, keys []kv.Key) (map[string][]byte, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("tikvTxn.BatchGet", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}
	return NewBufferBatchGetter(txn.GetMemBuffer(), nil, txn.GetSnapshot()).BatchGet(ctx, keys)
}

func (txn *tikvTxn) Delete(k kv.Key) error {
	err := txn.KVTxn.Delete(k)
	return derr.ToTiDBErr(err)
}

func (txn *tikvTxn) Get(ctx context.Context, k kv.Key) ([]byte, error) {
	data, err := txn.KVTxn.Get(ctx, k)
	return data, derr.ToTiDBErr(err)
}

func (txn *tikvTxn) Set(k kv.Key, v []byte) error {
	err := txn.KVTxn.Set(k, v)
	return derr.ToTiDBErr(err)
}

func (txn *tikvTxn) GetMemBuffer() kv.MemBuffer {
	return newMemBuffer(txn.KVTxn.GetMemBuffer())
}

func (txn *tikvTxn) GetUnionStore() kv.UnionStore {
	return &tikvUnionStore{txn.KVTxn.GetUnionStore()}
}

func (txn *tikvTxn) SetOption(opt int, val interface{}) {
	switch opt {
	case tikvstore.BinlogInfo:
		txn.SetBinlogExecutor(&binlogExecutor{
			txn:     txn.KVTxn,
			binInfo: val.(*binloginfo.BinlogInfo), // val cannot be other type.
		})
	case tikvstore.SchemaChecker:
		txn.SetSchemaLeaseChecker(val.(tikv.SchemaLeaseChecker))
	case tikvstore.IsolationLevel:
		level := getTiKVIsolationLevel(val.(kv.IsoLevel))
		txn.KVTxn.GetSnapshot().SetIsolationLevel(level)
	case tikvstore.Priority:
		txn.KVTxn.SetPriority(getTiKVPriority(val.(int)))
	case tikvstore.NotFillCache:
		txn.KVTxn.GetSnapshot().SetNotFillCache(val.(bool))
	case tikvstore.SyncLog:
		txn.EnableForceSyncLog()
	case tikvstore.Pessimistic:
		txn.SetPessimistic(val.(bool))
	case tikvstore.SnapshotTS:
		txn.KVTxn.GetSnapshot().SetSnapshotTS(val.(uint64))
	case tikvstore.ReplicaRead:
		txn.KVTxn.GetSnapshot().SetReplicaRead(val.(tikvstore.ReplicaReadType))
	case tikvstore.TaskID:
		txn.KVTxn.GetSnapshot().SetTaskID(val.(uint64))
	case tikvstore.InfoSchema:
		txn.SetSchemaVer(val.(tikv.SchemaVer))
	case tikvstore.SchemaAmender:
		txn.SetSchemaAmender(val.(tikv.SchemaAmender))
	case tikvstore.SampleStep:
		txn.KVTxn.GetSnapshot().SetSampleStep(val.(uint32))
	case tikvstore.CommitHook:
		txn.SetCommitCallback(val.(func(string, error)))
	case tikvstore.EnableAsyncCommit:
		txn.SetEnableAsyncCommit(val.(bool))
	case tikvstore.Enable1PC:
		txn.SetEnable1PC(val.(bool))
	case tikvstore.GuaranteeLinearizability:
		txn.SetCasualConsistency(!val.(bool))
	case tikvstore.TxnScope:
		txn.SetScope(val.(string))
	case tikvstore.IsStalenessReadOnly:
		txn.KVTxn.GetSnapshot().SetIsStatenessReadOnly(val.(bool))
	case tikvstore.MatchStoreLabels:
		txn.KVTxn.GetSnapshot().SetMatchStoreLabels(val.([]*metapb.StoreLabel))
	default:
		txn.KVTxn.SetOption(opt, val)
	}
}

func (txn *tikvTxn) GetOption(opt int) interface{} {
	switch opt {
	case tikvstore.GuaranteeLinearizability:
		return !txn.KVTxn.IsCasualConsistency()
	case tikvstore.TxnScope:
		return txn.KVTxn.GetScope()
	default:
		return txn.KVTxn.GetOption(opt)
	}
}

// SetVars sets variables to the transaction.
func (txn *tikvTxn) SetVars(vars interface{}) {
	if vs, ok := vars.(*tikv.Variables); ok {
		txn.KVTxn.SetVars(vs)
	}
}

func (txn *tikvTxn) GetVars() interface{} {
	return txn.KVTxn.GetVars()
}

func (txn *tikvTxn) extractKeyErr(err error) error {
	if e, ok := errors.Cause(err).(*tikverr.ErrKeyExist); ok {
		return txn.extractKeyExistsErr(e.GetKey())
	}
	return extractKeyErr(err)
}

func (txn *tikvTxn) extractKeyExistsErr(key kv.Key) error {
	tableID, indexID, isRecord, err := tablecodec.DecodeKeyHead(key)
	if err != nil {
		return genKeyExistsError("UNKNOWN", key.String(), err)
	}

	tblInfo := txn.GetTableInfo(tableID)
	if tblInfo == nil {
		return genKeyExistsError("UNKNOWN", key.String(), errors.New("cannot find table info"))
	}
	value, err := txn.KVTxn.GetUnionStore().GetMemBuffer().SelectValueHistory(key, func(value []byte) bool { return len(value) != 0 })
	if err != nil {
		return genKeyExistsError("UNKNOWN", key.String(), err)
	}

	if isRecord {
		return extractKeyExistsErrFromHandle(key, value, tblInfo)
	}
	return extractKeyExistsErrFromIndex(key, value, tblInfo, indexID)
}

// TiDBKVFilter is the filter specific to TiDB to filter out KV pairs that needn't be committed.
type TiDBKVFilter struct{}

// IsUnnecessaryKeyValue defines which kinds of KV pairs from TiDB needn't be committed.
func (f TiDBKVFilter) IsUnnecessaryKeyValue(key, value []byte, flags tikvstore.KeyFlags) bool {
	return tablecodec.IsUntouchedIndexKValue(key, value)
}
