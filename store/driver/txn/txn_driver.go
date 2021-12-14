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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package txn

import (
	"context"
	"sync/atomic"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	derr "github.com/pingcap/tidb/store/driver/error"
	"github.com/pingcap/tidb/store/driver/options"
	"github.com/pingcap/tidb/tablecodec"
	tikverr "github.com/tikv/client-go/v2/error"
	tikvstore "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv/txnsnapshot"
)

type tikvTxn struct {
	*tikv.KVTxn
	idxNameCache        map[int64]*model.TableInfo
	snapshotInterceptor kv.SnapshotInterceptor
}

// NewTiKVTxn returns a new Transaction.
func NewTiKVTxn(txn *tikv.KVTxn) kv.Transaction {
	txn.SetKVFilter(TiDBKVFilter{})

	entryLimit := atomic.LoadUint64(&kv.TxnEntrySizeLimit)
	totalLimit := atomic.LoadUint64(&kv.TxnTotalSizeLimit)
	txn.GetUnionStore().SetEntrySizeLimit(entryLimit, totalLimit)

	return &tikvTxn{txn, make(map[int64]*model.TableInfo), nil}
}

func (txn *tikvTxn) GetTableInfo(id int64) *model.TableInfo {
	return txn.idxNameCache[id]
}

func (txn *tikvTxn) SetDiskFullOpt(level kvrpcpb.DiskFullOpt) {
	txn.KVTxn.SetDiskFullOpt(level)
}

func (txn *tikvTxn) CacheTableInfo(id int64, info *model.TableInfo) {
	txn.idxNameCache[id] = info
}

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
	return &tikvSnapshot{txn.KVTxn.GetSnapshot(), txn.snapshotInterceptor}
}

// Iter creates an Iterator positioned on the first entry that k <= entry's key.
// If such entry is not found, it returns an invalid Iterator with no error.
// It yields only keys that < upperBound. If upperBound is nil, it means the upperBound is unbounded.
// The Iterator must be Closed after use.
func (txn *tikvTxn) Iter(k kv.Key, upperBound kv.Key) (iter kv.Iterator, err error) {
	var dirtyIter, snapIter kv.Iterator

	if dirtyIter, err = txn.GetMemBuffer().Iter(k, upperBound); err != nil {
		return nil, err
	}

	if snapIter, err = txn.GetSnapshot().Iter(k, upperBound); err != nil {
		dirtyIter.Close()
		return nil, err
	}

	iter, err = NewUnionIter(dirtyIter, snapIter, false)
	if err != nil {
		dirtyIter.Close()
		snapIter.Close()
	}

	return iter, err
}

// IterReverse creates a reversed Iterator positioned on the first entry which key is less than k.
// The returned iterator will iterate from greater key to smaller key.
// If k is nil, the returned iterator will be positioned at the last key.
// TODO: Add lower bound limit
func (txn *tikvTxn) IterReverse(k kv.Key) (iter kv.Iterator, err error) {
	var dirtyIter, snapIter kv.Iterator

	if dirtyIter, err = txn.GetMemBuffer().IterReverse(k); err != nil {
		return nil, err
	}

	if snapIter, err = txn.GetSnapshot().IterReverse(k); err != nil {
		dirtyIter.Close()
		return nil, err
	}

	iter, err = NewUnionIter(dirtyIter, snapIter, true)
	if err != nil {
		dirtyIter.Close()
		snapIter.Close()
	}

	return iter, err
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
	val, err := txn.GetMemBuffer().Get(ctx, k)
	if kv.ErrNotExist.Equal(err) {
		val, err = txn.GetSnapshot().Get(ctx, k)
	}

	if err == nil && len(val) == 0 {
		return nil, kv.ErrNotExist
	}

	return val, err
}

func (txn *tikvTxn) Set(k kv.Key, v []byte) error {
	err := txn.KVTxn.Set(k, v)
	return derr.ToTiDBErr(err)
}

func (txn *tikvTxn) GetMemBuffer() kv.MemBuffer {
	return newMemBuffer(txn.KVTxn.GetMemBuffer())
}

func (txn *tikvTxn) SetOption(opt int, val interface{}) {
	switch opt {
	case kv.BinlogInfo:
		txn.SetBinlogExecutor(&binlogExecutor{
			txn:     txn.KVTxn,
			binInfo: val.(*binloginfo.BinlogInfo), // val cannot be other type.
		})
	case kv.SchemaChecker:
		txn.SetSchemaLeaseChecker(val.(tikv.SchemaLeaseChecker))
	case kv.IsolationLevel:
		level := getTiKVIsolationLevel(val.(kv.IsoLevel))
		txn.KVTxn.GetSnapshot().SetIsolationLevel(level)
	case kv.Priority:
		txn.KVTxn.SetPriority(getTiKVPriority(val.(int)))
	case kv.NotFillCache:
		txn.KVTxn.GetSnapshot().SetNotFillCache(val.(bool))
	case kv.SyncLog:
		txn.EnableForceSyncLog()
	case kv.Pessimistic:
		txn.SetPessimistic(val.(bool))
	case kv.SnapshotTS:
		txn.KVTxn.GetSnapshot().SetSnapshotTS(val.(uint64))
	case kv.ReplicaRead:
		t := options.GetTiKVReplicaReadType(val.(kv.ReplicaReadType))
		txn.KVTxn.GetSnapshot().SetReplicaRead(t)
	case kv.TaskID:
		txn.KVTxn.GetSnapshot().SetTaskID(val.(uint64))
	case kv.InfoSchema:
		txn.SetSchemaVer(val.(tikv.SchemaVer))
	case kv.CollectRuntimeStats:
		if val == nil {
			txn.KVTxn.GetSnapshot().SetRuntimeStats(nil)
		} else {
			txn.KVTxn.GetSnapshot().SetRuntimeStats(val.(*txnsnapshot.SnapshotRuntimeStats))
		}
	case kv.SchemaAmender:
		txn.SetSchemaAmender(val.(tikv.SchemaAmender))
	case kv.SampleStep:
		txn.KVTxn.GetSnapshot().SetSampleStep(val.(uint32))
	case kv.CommitHook:
		txn.SetCommitCallback(val.(func(string, error)))
	case kv.EnableAsyncCommit:
		txn.SetEnableAsyncCommit(val.(bool))
	case kv.Enable1PC:
		txn.SetEnable1PC(val.(bool))
	case kv.GuaranteeLinearizability:
		txn.SetCausalConsistency(!val.(bool))
	case kv.TxnScope:
		txn.SetScope(val.(string))
	case kv.IsStalenessReadOnly:
		txn.KVTxn.GetSnapshot().SetIsStatenessReadOnly(val.(bool))
	case kv.MatchStoreLabels:
		txn.KVTxn.GetSnapshot().SetMatchStoreLabels(val.([]*metapb.StoreLabel))
	case kv.ResourceGroupTag:
		txn.KVTxn.SetResourceGroupTag(val.([]byte))
	case kv.ResourceGroupTagger:
		txn.KVTxn.SetResourceGroupTagger(val.(tikvrpc.ResourceGroupTagger))
	case kv.KVFilter:
		txn.KVTxn.SetKVFilter(val.(tikv.KVFilter))
	case kv.SnapInterceptor:
		txn.snapshotInterceptor = val.(kv.SnapshotInterceptor)
	case kv.CommitTSUpperBoundCheck:
		txn.KVTxn.SetCommitTSUpperBoundCheck(val.(func(commitTS uint64) bool))
	}
}

func (txn *tikvTxn) GetOption(opt int) interface{} {
	switch opt {
	case kv.GuaranteeLinearizability:
		return !txn.KVTxn.IsCasualConsistency()
	case kv.TxnScope:
		return txn.KVTxn.GetScope()
	default:
		return nil
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
