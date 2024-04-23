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
	"bytes"
	"context"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx/binloginfo"
	derr "github.com/pingcap/tidb/pkg/store/driver/error"
	"github.com/pingcap/tidb/pkg/store/driver/options"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/tracing"
	tikverr "github.com/tikv/client-go/v2/error"
	tikvstore "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/tikvrpc/interceptor"
	"github.com/tikv/client-go/v2/txnkv"
	"github.com/tikv/client-go/v2/txnkv/txnsnapshot"
	"go.uber.org/zap"
)

type tikvTxn struct {
	*tikv.KVTxn
	idxNameCache        map[int64]*model.TableInfo
	snapshotInterceptor kv.SnapshotInterceptor
	// columnMapsCache is a cache used for the mutation checker
	columnMapsCache    any
	isCommitterWorking atomic.Bool
}

// NewTiKVTxn returns a new Transaction.
func NewTiKVTxn(txn *tikv.KVTxn) kv.Transaction {
	txn.SetKVFilter(TiDBKVFilter{})

	// init default size limits by config
	entryLimit := kv.TxnEntrySizeLimit.Load()
	totalLimit := kv.TxnTotalSizeLimit.Load()
	txn.GetUnionStore().SetEntrySizeLimit(entryLimit, totalLimit)

	return &tikvTxn{txn, make(map[int64]*model.TableInfo), nil, nil, atomic.Bool{}}
}

func (txn *tikvTxn) GetTableInfo(id int64) *model.TableInfo {
	return txn.idxNameCache[id]
}

func (txn *tikvTxn) SetDiskFullOpt(level kvrpcpb.DiskFullOpt) {
	txn.KVTxn.SetDiskFullOpt(level)
}

func (txn *tikvTxn) CacheTableInfo(id int64, info *model.TableInfo) {
	txn.idxNameCache[id] = info
	// For partition table, also cached tblInfo with TableID for global index.
	if info != nil && info.ID != id {
		txn.idxNameCache[info.ID] = info
	}
}

func (txn *tikvTxn) LockKeys(ctx context.Context, lockCtx *kv.LockCtx, keysInput ...kv.Key) error {
	if intest.InTest {
		txn.isCommitterWorking.Store(true)
		defer txn.isCommitterWorking.Store(false)
	}
	keys := toTiKVKeys(keysInput)
	err := txn.KVTxn.LockKeys(ctx, lockCtx, keys...)
	if err != nil {
		return txn.extractKeyErr(err)
	}
	return txn.generateWriteConflictForLockedWithConflict(lockCtx)
}

func (txn *tikvTxn) LockKeysFunc(ctx context.Context, lockCtx *kv.LockCtx, fn func(), keysInput ...kv.Key) error {
	if intest.InTest {
		txn.isCommitterWorking.Store(true)
		defer txn.isCommitterWorking.Store(false)
	}
	keys := toTiKVKeys(keysInput)
	err := txn.KVTxn.LockKeysFunc(ctx, lockCtx, fn, keys...)
	if err != nil {
		return txn.extractKeyErr(err)
	}
	return txn.generateWriteConflictForLockedWithConflict(lockCtx)
}

func (txn *tikvTxn) Commit(ctx context.Context) error {
	if intest.InTest {
		txn.isCommitterWorking.Store(true)
	}
	err := txn.KVTxn.Commit(ctx)
	return txn.extractKeyErr(err)
}

func (txn *tikvTxn) GetMemDBCheckpoint() *tikv.MemDBCheckpoint {
	buf := txn.KVTxn.GetMemBuffer()
	return buf.Checkpoint()
}

func (txn *tikvTxn) RollbackMemDBToCheckpoint(savepoint *tikv.MemDBCheckpoint) {
	buf := txn.KVTxn.GetMemBuffer()
	buf.RevertToCheckpoint(savepoint)
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
func (txn *tikvTxn) IterReverse(k kv.Key, lowerBound kv.Key) (iter kv.Iterator, err error) {
	var dirtyIter, snapIter kv.Iterator

	if dirtyIter, err = txn.GetMemBuffer().IterReverse(k, lowerBound); err != nil {
		return nil, err
	}

	if snapIter, err = txn.GetSnapshot().IterReverse(k, lowerBound); err != nil {
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
	r, ctx := tracing.StartRegionEx(ctx, "tikvTxn.BatchGet")
	defer r.End()
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
	return newMemBuffer(txn.KVTxn.GetMemBuffer(), txn.IsPipelined())
}

func (txn *tikvTxn) SetOption(opt int, val any) {
	if intest.InTest {
		txn.assertCommitterNotWorking()
	}
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
		txn.KVTxn.GetSnapshot().SetIsStalenessReadOnly(val.(bool))
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
	case kv.RPCInterceptor:
		txn.KVTxn.AddRPCInterceptor(val.(interceptor.RPCInterceptor))
	case kv.AssertionLevel:
		txn.KVTxn.SetAssertionLevel(val.(kvrpcpb.AssertionLevel))
	case kv.TableToColumnMaps:
		txn.columnMapsCache = val
	case kv.RequestSourceInternal:
		txn.KVTxn.SetRequestSourceInternal(val.(bool))
	case kv.RequestSourceType:
		txn.KVTxn.SetRequestSourceType(val.(string))
	case kv.ExplicitRequestSourceType:
		txn.KVTxn.SetExplicitRequestSourceType(val.(string))
	case kv.ReplicaReadAdjuster:
		txn.KVTxn.GetSnapshot().SetReplicaReadAdjuster(val.(txnkv.ReplicaReadAdjuster))
	case kv.TxnSource:
		txn.KVTxn.SetTxnSource(val.(uint64))
	case kv.ResourceGroupName:
		txn.KVTxn.SetResourceGroupName(val.(string))
	case kv.LoadBasedReplicaReadThreshold:
		txn.KVTxn.GetSnapshot().SetLoadBasedReplicaReadThreshold(val.(time.Duration))
	case kv.TiKVClientReadTimeout:
		txn.KVTxn.GetSnapshot().SetKVReadTimeout(time.Duration(val.(uint64) * uint64(time.Millisecond)))
	case kv.SizeLimits:
		limits := val.(kv.TxnSizeLimits)
		txn.KVTxn.GetUnionStore().SetEntrySizeLimit(limits.Entry, limits.Total)
	case kv.SessionID:
		txn.KVTxn.SetSessionID(val.(uint64))
	}
}

func (txn *tikvTxn) GetOption(opt int) any {
	switch opt {
	case kv.GuaranteeLinearizability:
		return !txn.KVTxn.IsCasualConsistency()
	case kv.TxnScope:
		return txn.KVTxn.GetScope()
	case kv.TableToColumnMaps:
		return txn.columnMapsCache
	case kv.RequestSourceInternal:
		return txn.RequestSourceInternal
	case kv.RequestSourceType:
		return txn.RequestSourceType
	default:
		return nil
	}
}

// SetVars sets variables to the transaction.
func (txn *tikvTxn) SetVars(vars any) {
	if vs, ok := vars.(*tikv.Variables); ok {
		txn.KVTxn.SetVars(vs)
	}
}

func (txn *tikvTxn) GetVars() any {
	return txn.KVTxn.GetVars()
}

func (txn *tikvTxn) extractKeyErr(err error) error {
	if e, ok := errors.Cause(err).(*tikverr.ErrKeyExist); ok {
		return txn.extractKeyExistsErr(e)
	}
	return extractKeyErr(err)
}

func (txn *tikvTxn) extractKeyExistsErr(errExist *tikverr.ErrKeyExist) error {
	var key kv.Key = errExist.GetKey()
	tableID, indexID, isRecord, err := tablecodec.DecodeKeyHead(key)
	if err != nil {
		return genKeyExistsError("UNKNOWN", key.String(), err)
	}
	indexID = tablecodec.IndexIDMask & indexID

	tblInfo := txn.GetTableInfo(tableID)
	if tblInfo == nil {
		return genKeyExistsError("UNKNOWN", key.String(), errors.New("cannot find table info"))
	}
	var value []byte
	if txn.IsPipelined() {
		value = errExist.Value
		if len(value) == 0 {
			return genKeyExistsError(
				"UNKNOWN",
				key.String(),
				errors.New("The value is empty (a delete)"),
			)
		}
	} else {
		value, err = txn.KVTxn.GetUnionStore().GetMemBuffer().GetMemDB().SelectValueHistory(key, func(value []byte) bool { return len(value) != 0 })
	}
	if err != nil {
		return genKeyExistsError("UNKNOWN", key.String(), err)
	}

	if isRecord {
		return ExtractKeyExistsErrFromHandle(key, value, tblInfo)
	}
	return ExtractKeyExistsErrFromIndex(key, value, tblInfo, indexID)
}

// SetAssertion sets an assertion for the key operation.
func (txn *tikvTxn) SetAssertion(key []byte, assertion ...kv.FlagsOp) error {
	f, err := txn.GetUnionStore().GetMemBuffer().GetFlags(key)
	if err != nil && !tikverr.IsErrNotFound(err) {
		return err
	}
	if err == nil && f.HasAssertionFlags() {
		return nil
	}
	txn.UpdateMemBufferFlags(key, assertion...)
	return nil
}

func (txn *tikvTxn) UpdateMemBufferFlags(key []byte, flags ...kv.FlagsOp) {
	txn.GetUnionStore().GetMemBuffer().UpdateFlags(key, getTiKVFlagsOps(flags)...)
}

func (txn *tikvTxn) generateWriteConflictForLockedWithConflict(lockCtx *kv.LockCtx) error {
	if lockCtx.MaxLockedWithConflictTS != 0 {
		failpoint.Inject("lockedWithConflictOccurs", func() {})
		var bufTableID, bufRest bytes.Buffer
		foundKey := false
		for k, v := range lockCtx.Values {
			if v.LockedWithConflictTS >= lockCtx.MaxLockedWithConflictTS {
				foundKey = true
				prettyWriteKey(&bufTableID, &bufRest, []byte(k))
				break
			}
		}
		if !foundKey {
			bufTableID.WriteString("<unknown>")
		}
		// TODO: Primary is not exported here.
		primary := " primary=<unknown>"
		primaryRest := ""
		return kv.ErrWriteConflict.FastGenByArgs(txn.StartTS(), 0, lockCtx.MaxLockedWithConflictTS, bufTableID.String(), bufRest.String(), primary, primaryRest, "LockedWithConflict")
	}
	return nil
}

// StartFairLocking adapts the method signature of `KVTxn` to satisfy kv.FairLockingController.
// TODO: Update the methods' signatures in client-go to avoid this adaptor functions.
// TODO: Rename aggressive locking in client-go to fair locking.
func (txn *tikvTxn) StartFairLocking() error {
	txn.KVTxn.StartAggressiveLocking()
	return nil
}

// RetryFairLocking adapts the method signature of `KVTxn` to satisfy kv.FairLockingController.
func (txn *tikvTxn) RetryFairLocking(ctx context.Context) error {
	txn.KVTxn.RetryAggressiveLocking(ctx)
	return nil
}

// CancelFairLocking adapts the method signature of `KVTxn` to satisfy kv.FairLockingController.
func (txn *tikvTxn) CancelFairLocking(ctx context.Context) error {
	txn.KVTxn.CancelAggressiveLocking(ctx)
	return nil
}

// DoneFairLocking adapts the method signature of `KVTxn` to satisfy kv.FairLockingController.
func (txn *tikvTxn) DoneFairLocking(ctx context.Context) error {
	txn.KVTxn.DoneAggressiveLocking(ctx)
	return nil
}

// IsInFairLockingMode adapts the method signature of `KVTxn` to satisfy kv.FairLockingController.
func (txn *tikvTxn) IsInFairLockingMode() bool {
	return txn.KVTxn.IsInAggressiveLockingMode()
}

// MayFlush wraps the flush function and extract the error.
func (txn *tikvTxn) MayFlush() error {
	if !txn.IsPipelined() {
		return nil
	}
	if intest.InTest {
		txn.isCommitterWorking.Store(true)
	}
	_, err := txn.KVTxn.GetMemBuffer().Flush(false)
	return txn.extractKeyErr(err)
}

// assertCommitterNotWorking asserts that the committer is not working, so it's safe to modify the options for txn and committer.
// It panics when committer is working, only use it when test with --tags=intest tag.
func (txn *tikvTxn) assertCommitterNotWorking() {
	if txn.isCommitterWorking.Load() {
		panic("committer is working")
	}
}

// TiDBKVFilter is the filter specific to TiDB to filter out KV pairs that needn't be committed.
type TiDBKVFilter struct{}

// IsUnnecessaryKeyValue defines which kinds of KV pairs from TiDB needn't be committed.
func (f TiDBKVFilter) IsUnnecessaryKeyValue(key, value []byte, flags tikvstore.KeyFlags) (bool, error) {
	isUntouchedValue := tablecodec.IsUntouchedIndexKValue(key, value)
	if isUntouchedValue && flags.HasPresumeKeyNotExists() {
		logutil.BgLogger().Error("unexpected path the untouched key value with PresumeKeyNotExists flag",
			zap.Stringer("key", kv.Key(key)), zap.Stringer("value", kv.Key(value)),
			zap.Uint16("flags", uint16(flags)), zap.Stack("stack"))
		return false, errors.Errorf(
			"unexpected path the untouched key=%s value=%s contains PresumeKeyNotExists flag keyFlags=%v",
			kv.Key(key).String(), kv.Key(value).String(), flags)
	}
	return isUntouchedValue, nil
}
