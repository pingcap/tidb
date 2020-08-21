// Copyright 2016 PingCAP, Inc.
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

package tikv

import (
	"bytes"
	"container/list"
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/logutil"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

// ResolvedCacheSize is max number of cached txn status.
const ResolvedCacheSize = 2048

// bigTxnThreshold : transaction involves keys exceed this threshold can be treated as `big transaction`.
const bigTxnThreshold = 16

var (
	tikvLockResolverCountWithBatchResolve             = metrics.TiKVLockResolverCounter.WithLabelValues("batch_resolve")
	tikvLockResolverCountWithExpired                  = metrics.TiKVLockResolverCounter.WithLabelValues("expired")
	tikvLockResolverCountWithNotExpired               = metrics.TiKVLockResolverCounter.WithLabelValues("not_expired")
	tikvLockResolverCountWithWaitExpired              = metrics.TiKVLockResolverCounter.WithLabelValues("wait_expired")
	tikvLockResolverCountWithResolve                  = metrics.TiKVLockResolverCounter.WithLabelValues("resolve")
	tikvLockResolverCountWithResolveForWrite          = metrics.TiKVLockResolverCounter.WithLabelValues("resolve_for_write")
	tikvLockResolverCountWithWriteConflict            = metrics.TiKVLockResolverCounter.WithLabelValues("write_conflict")
	tikvLockResolverCountWithQueryTxnStatus           = metrics.TiKVLockResolverCounter.WithLabelValues("query_txn_status")
	tikvLockResolverCountWithQueryTxnStatusCommitted  = metrics.TiKVLockResolverCounter.WithLabelValues("query_txn_status_committed")
	tikvLockResolverCountWithQueryTxnStatusRolledBack = metrics.TiKVLockResolverCounter.WithLabelValues("query_txn_status_rolled_back")
	tikvLockResolverCountWithResolveLocks             = metrics.TiKVLockResolverCounter.WithLabelValues("query_resolve_locks")
	tikvLockResolverCountWithResolveLockLite          = metrics.TiKVLockResolverCounter.WithLabelValues("query_resolve_lock_lite")
)

// LockResolver resolves locks and also caches resolved txn status.
type LockResolver struct {
	store Storage
	mu    struct {
		sync.RWMutex
		// resolved caches resolved txns (FIFO, txn id -> txnStatus).
		resolved       map[uint64]TxnStatus
		recentResolved *list.List
	}
	testingKnobs struct {
		meetLock func(locks []*Lock)
	}
}

func newLockResolver(store Storage) *LockResolver {
	r := &LockResolver{
		store: store,
	}
	r.mu.resolved = make(map[uint64]TxnStatus)
	r.mu.recentResolved = list.New()
	return r
}

// NewLockResolver is exported for other pkg to use, suppress unused warning.
var _ = NewLockResolver

// NewLockResolver creates a LockResolver.
// It is exported for other pkg to use. For instance, binlog service needs
// to determine a transaction's commit state.
func NewLockResolver(etcdAddrs []string, security config.Security, opts ...pd.ClientOption) (*LockResolver, error) {
	pdCli, err := pd.NewClient(etcdAddrs, pd.SecurityOption{
		CAPath:   security.ClusterSSLCA,
		CertPath: security.ClusterSSLCert,
		KeyPath:  security.ClusterSSLKey,
	}, opts...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	pdCli = execdetails.InterceptedPDClient{Client: pdCli}
	uuid := fmt.Sprintf("tikv-%v", pdCli.GetClusterID(context.TODO()))

	tlsConfig, err := security.ToTLSConfig()
	if err != nil {
		return nil, errors.Trace(err)
	}

	spkv, err := NewEtcdSafePointKV(etcdAddrs, tlsConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}

	s, err := newTikvStore(uuid, &codecPDClient{pdCli}, spkv, newRPCClient(security), false, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return s.lockResolver, nil
}

// TxnStatus represents a txn's final status. It should be Lock or Commit or Rollback.
type TxnStatus struct {
	ttl      uint64
	commitTS uint64
	action   kvrpcpb.Action
}

// IsCommitted returns true if the txn's final status is Commit.
func (s TxnStatus) IsCommitted() bool { return s.ttl == 0 && s.commitTS > 0 }

// CommitTS returns the txn's commitTS. It is valid iff `IsCommitted` is true.
func (s TxnStatus) CommitTS() uint64 { return s.commitTS }

// TTL returns the TTL of the transaction if the transaction is still alive.
func (s TxnStatus) TTL() uint64 { return s.ttl }

// Action returns what the CheckTxnStatus request have done to the transaction.
func (s TxnStatus) Action() kvrpcpb.Action { return s.action }

// By default, locks after 3000ms is considered unusual (the client created the
// lock might be dead). Other client may cleanup this kind of lock.
// For locks created recently, we will do backoff and retry.
var defaultLockTTL uint64 = 3000

// ttl = ttlFactor * sqrt(writeSizeInMiB)
var ttlFactor = 6000

// Lock represents a lock from tikv server.
type Lock struct {
	Key             []byte
	Primary         []byte
	TxnID           uint64
	TTL             uint64
	TxnSize         uint64
	LockType        kvrpcpb.Op
	LockForUpdateTS uint64
}

func (l *Lock) String() string {
	buf := bytes.NewBuffer(make([]byte, 0, 128))
	buf.WriteString("key: ")
	prettyWriteKey(buf, l.Key)
	buf.WriteString(", primary: ")
	prettyWriteKey(buf, l.Primary)
	return fmt.Sprintf("%s, txnStartTS: %d, lockForUpdateTS:%d, ttl: %d, type: %s", buf.String(), l.TxnID, l.LockForUpdateTS, l.TTL, l.LockType)
}

// NewLock creates a new *Lock.
func NewLock(l *kvrpcpb.LockInfo) *Lock {
	return &Lock{
		Key:             l.GetKey(),
		Primary:         l.GetPrimaryLock(),
		TxnID:           l.GetLockVersion(),
		TTL:             l.GetLockTtl(),
		TxnSize:         l.GetTxnSize(),
		LockType:        l.LockType,
		LockForUpdateTS: l.LockForUpdateTs,
	}
}

func (lr *LockResolver) saveResolved(txnID uint64, status TxnStatus) {
	lr.mu.Lock()
	defer lr.mu.Unlock()

	if _, ok := lr.mu.resolved[txnID]; ok {
		return
	}
	lr.mu.resolved[txnID] = status
	lr.mu.recentResolved.PushBack(txnID)
	if len(lr.mu.resolved) > ResolvedCacheSize {
		front := lr.mu.recentResolved.Front()
		delete(lr.mu.resolved, front.Value.(uint64))
		lr.mu.recentResolved.Remove(front)
	}
}

func (lr *LockResolver) getResolved(txnID uint64) (TxnStatus, bool) {
	lr.mu.RLock()
	defer lr.mu.RUnlock()

	s, ok := lr.mu.resolved[txnID]
	return s, ok
}

// BatchResolveLocks resolve locks in a batch.
// Used it in gcworker only!
func (lr *LockResolver) BatchResolveLocks(bo *Backoffer, locks []*Lock, loc RegionVerID) (bool, error) {
	if len(locks) == 0 {
		return true, nil
	}

	tikvLockResolverCountWithBatchResolve.Inc()

	// The GCWorker kill all ongoing transactions, because it must make sure all
	// locks have been cleaned before GC.
	expiredLocks := locks

	callerStartTS, err := lr.store.GetOracle().GetTimestamp(bo.ctx)
	if err != nil {
		return false, errors.Trace(err)
	}

	txnInfos := make(map[uint64]uint64)
	startTime := time.Now()
	for _, l := range expiredLocks {
		if _, ok := txnInfos[l.TxnID]; ok {
			continue
		}
		tikvLockResolverCountWithExpired.Inc()

		// Use currentTS = math.MaxUint64 means rollback the txn, no matter the lock is expired or not!
		status, err := lr.getTxnStatus(bo, l.TxnID, l.Primary, callerStartTS, math.MaxUint64, true)
		if err != nil {
			return false, err
		}

		if status.ttl > 0 {
			logutil.BgLogger().Error("BatchResolveLocks fail to clean locks, this result is not expected!")
			return false, errors.New("TiDB ask TiKV to rollback locks but it doesn't, the protocol maybe wrong")
		}

		txnInfos[l.TxnID] = status.commitTS
	}
	logutil.BgLogger().Info("BatchResolveLocks: lookup txn status",
		zap.Duration("cost time", time.Since(startTime)),
		zap.Int("num of txn", len(txnInfos)))

	listTxnInfos := make([]*kvrpcpb.TxnInfo, 0, len(txnInfos))
	for txnID, status := range txnInfos {
		listTxnInfos = append(listTxnInfos, &kvrpcpb.TxnInfo{
			Txn:    txnID,
			Status: status,
		})
	}

	req := tikvrpc.NewRequest(tikvrpc.CmdResolveLock, &kvrpcpb.ResolveLockRequest{TxnInfos: listTxnInfos})
	startTime = time.Now()
	resp, err := lr.store.SendReq(bo, req, loc, readTimeoutShort)
	if err != nil {
		return false, errors.Trace(err)
	}

	regionErr, err := resp.GetRegionError()
	if err != nil {
		return false, errors.Trace(err)
	}

	if regionErr != nil {
		err = bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
		if err != nil {
			return false, errors.Trace(err)
		}
		return false, nil
	}

	if resp.Resp == nil {
		return false, errors.Trace(ErrBodyMissing)
	}
	cmdResp := resp.Resp.(*kvrpcpb.ResolveLockResponse)
	if keyErr := cmdResp.GetError(); keyErr != nil {
		return false, errors.Errorf("unexpected resolve err: %s", keyErr)
	}

	logutil.BgLogger().Info("BatchResolveLocks: resolve locks in a batch",
		zap.Duration("cost time", time.Since(startTime)),
		zap.Int("num of locks", len(expiredLocks)))
	return true, nil
}

// ResolveLocks tries to resolve Locks. The resolving process is in 3 steps:
// 1) Use the `lockTTL` to pick up all expired locks. Only locks that are too
//    old are considered orphan locks and will be handled later. If all locks
//    are expired then all locks will be resolved so the returned `ok` will be
//    true, otherwise caller should sleep a while before retry.
// 2) For each lock, query the primary key to get txn(which left the lock)'s
//    commit status.
// 3) Send `ResolveLock` cmd to the lock's region to resolve all locks belong to
//    the same transaction.
func (lr *LockResolver) ResolveLocks(bo *Backoffer, callerStartTS uint64, locks []*Lock) (int64, []uint64 /*pushed*/, error) {
	return lr.resolveLocks(bo, callerStartTS, locks, false, false)
}

func (lr *LockResolver) resolveLocksLite(bo *Backoffer, callerStartTS uint64, locks []*Lock) (int64, []uint64 /*pushed*/, error) {
	return lr.resolveLocks(bo, callerStartTS, locks, false, true)
}

func (lr *LockResolver) resolveLocks(bo *Backoffer, callerStartTS uint64, locks []*Lock, forWrite bool, lite bool) (int64, []uint64 /*pushed*/, error) {
	if lr.testingKnobs.meetLock != nil {
		lr.testingKnobs.meetLock(locks)
	}
	var msBeforeTxnExpired txnExpireTime
	if len(locks) == 0 {
		return msBeforeTxnExpired.value(), nil, nil
	}

	if forWrite {
		tikvLockResolverCountWithResolveForWrite.Inc()
	} else {
		tikvLockResolverCountWithResolve.Inc()
	}

	var pushFail bool
	// TxnID -> []Region, record resolved Regions.
	// TODO: Maybe put it in LockResolver and share by all txns.
	cleanTxns := make(map[uint64]map[RegionVerID]struct{})
	var pushed []uint64
	// pushed is only used in the read operation.
	if !forWrite {
		pushed = make([]uint64, 0, len(locks))
	}

	for _, l := range locks {
		status, err := lr.getTxnStatusFromLock(bo, l, callerStartTS)
		if err != nil {
			msBeforeTxnExpired.update(0)
			err = errors.Trace(err)
			return msBeforeTxnExpired.value(), nil, err
		}

		if status.ttl == 0 {
			tikvLockResolverCountWithExpired.Inc()
			// If the lock is committed or rollbacked, resolve lock.
			cleanRegions, exists := cleanTxns[l.TxnID]
			if !exists {
				cleanRegions = make(map[RegionVerID]struct{})
				cleanTxns[l.TxnID] = cleanRegions
			}

			if l.LockType == kvrpcpb.Op_PessimisticLock {
				err = lr.resolvePessimisticLock(bo, l, cleanRegions)
			} else {
				err = lr.resolveLock(bo, l, status, lite, cleanRegions)
			}
			if err != nil {
				msBeforeTxnExpired.update(0)
				err = errors.Trace(err)
				return msBeforeTxnExpired.value(), nil, err
			}
		} else {
			tikvLockResolverCountWithNotExpired.Inc()
			// If the lock is valid, the txn may be a pessimistic transaction.
			// Update the txn expire time.
			msBeforeLockExpired := lr.store.GetOracle().UntilExpired(l.TxnID, status.ttl)
			msBeforeTxnExpired.update(msBeforeLockExpired)
			if forWrite {
				// Write conflict detected!
				// If it's a optimistic conflict and current txn is earlier than the lock owner,
				// abort current transaction.
				// This could avoids the deadlock scene of two large transaction.
				if l.LockType != kvrpcpb.Op_PessimisticLock && l.TxnID > callerStartTS {
					tikvLockResolverCountWithWriteConflict.Inc()
					return msBeforeTxnExpired.value(), nil, kv.ErrWriteConflict.GenWithStackByArgs(callerStartTS, l.TxnID, status.commitTS, l.Key)
				}
			} else {
				if status.action != kvrpcpb.Action_MinCommitTSPushed {
					pushFail = true
					continue
				}
				pushed = append(pushed, l.TxnID)
			}
		}
	}
	if pushFail {
		// If any of the lock fails to push minCommitTS, don't return the pushed array.
		pushed = nil
	}

	if msBeforeTxnExpired.value() > 0 && len(pushed) == 0 {
		// If len(pushed) > 0, the caller will not block on the locks, it push the minCommitTS instead.
		tikvLockResolverCountWithWaitExpired.Inc()
	}
	return msBeforeTxnExpired.value(), pushed, nil
}

func (lr *LockResolver) resolveLocksForWrite(bo *Backoffer, callerStartTS uint64, locks []*Lock) (int64, error) {
	msBeforeTxnExpired, _, err := lr.resolveLocks(bo, callerStartTS, locks, true, false)
	return msBeforeTxnExpired, err
}

type txnExpireTime struct {
	initialized bool
	txnExpire   int64
}

func (t *txnExpireTime) update(lockExpire int64) {
	if lockExpire <= 0 {
		lockExpire = 0
	}
	if !t.initialized {
		t.txnExpire = lockExpire
		t.initialized = true
		return
	}
	if lockExpire < t.txnExpire {
		t.txnExpire = lockExpire
	}
}

func (t *txnExpireTime) value() int64 {
	if !t.initialized {
		return 0
	}
	return t.txnExpire
}

// GetTxnStatus queries tikv-server for a txn's status (commit/rollback).
// If the primary key is still locked, it will launch a Rollback to abort it.
// To avoid unnecessarily aborting too many txns, it is wiser to wait a few
// seconds before calling it after Prewrite.
func (lr *LockResolver) GetTxnStatus(txnID uint64, callerStartTS uint64, primary []byte) (TxnStatus, error) {
	var status TxnStatus
	bo := NewBackofferWithVars(context.Background(), cleanupMaxBackoff, nil)
	currentTS, err := lr.store.GetOracle().GetLowResolutionTimestamp(bo.ctx)
	if err != nil {
		return status, err
	}
	return lr.getTxnStatus(bo, txnID, primary, callerStartTS, currentTS, true)
}

func (lr *LockResolver) getTxnStatusFromLock(bo *Backoffer, l *Lock, callerStartTS uint64) (TxnStatus, error) {
	var currentTS uint64
	var err error
	var status TxnStatus
	if l.TTL == 0 {
		// NOTE: l.TTL = 0 is a special protocol!!!
		// When the pessimistic txn prewrite meets locks of a txn, it should resolve the lock **unconditionally**.
		// In this case, TiKV use lock TTL = 0 to notify TiDB, and TiDB should resolve the lock!
		// Set currentTS to max uint64 to make the lock expired.
		currentTS = math.MaxUint64
	} else {
		currentTS, err = lr.store.GetOracle().GetLowResolutionTimestamp(bo.ctx)
		if err != nil {
			return TxnStatus{}, err
		}
	}

	rollbackIfNotExist := false
	failpoint.Inject("getTxnStatusDelay", func() {
		time.Sleep(100 * time.Millisecond)
	})
	for {
		status, err = lr.getTxnStatus(bo, l.TxnID, l.Primary, callerStartTS, currentTS, rollbackIfNotExist)
		if err == nil {
			return status, nil
		}
		// If the error is something other than txnNotFoundErr, throw the error (network
		// unavailable, tikv down, backoff timeout etc) to the caller.
		if _, ok := errors.Cause(err).(txnNotFoundErr); !ok {
			return TxnStatus{}, err
		}

		failpoint.Inject("txnNotFoundRetTTL", func() {
			failpoint.Return(TxnStatus{l.TTL, 0, kvrpcpb.Action_NoAction}, nil)
		})

		// Handle txnNotFound error.
		// getTxnStatus() returns it when the secondary locks exist while the primary lock doesn't.
		// This is likely to happen in the concurrently prewrite when secondary regions
		// success before the primary region.
		if err := bo.Backoff(boTxnNotFound, err); err != nil {
			logutil.Logger(bo.ctx).Warn("getTxnStatusFromLock backoff fail", zap.Error(err))
		}

		if lr.store.GetOracle().UntilExpired(l.TxnID, l.TTL) <= 0 {
			logutil.Logger(bo.ctx).Warn("lock txn not found, lock has expired",
				zap.Uint64("CallerStartTs", callerStartTS),
				zap.Stringer("lock str", l))
			if l.LockType == kvrpcpb.Op_PessimisticLock {
				failpoint.Inject("txnExpireRetTTL", func() {
					failpoint.Return(TxnStatus{l.TTL, 0, kvrpcpb.Action_NoAction},
						errors.New("error txn not found and lock expired"))
				})
				return TxnStatus{}, nil
			}
			rollbackIfNotExist = true
		} else {
			if l.LockType == kvrpcpb.Op_PessimisticLock {
				return TxnStatus{ttl: l.TTL}, nil
			}
		}
	}
}

type txnNotFoundErr struct {
	*kvrpcpb.TxnNotFound
}

func (e txnNotFoundErr) Error() string {
	return e.TxnNotFound.String()
}

// getTxnStatus sends the CheckTxnStatus request to the TiKV server.
// When rollbackIfNotExist is false, the caller should be careful with the txnNotFoundErr error.
func (lr *LockResolver) getTxnStatus(bo *Backoffer, txnID uint64, primary []byte, callerStartTS, currentTS uint64, rollbackIfNotExist bool) (TxnStatus, error) {
	if s, ok := lr.getResolved(txnID); ok {
		return s, nil
	}

	tikvLockResolverCountWithQueryTxnStatus.Inc()

	// CheckTxnStatus may meet the following cases:
	// 1. LOCK
	// 1.1 Lock expired -- orphan lock, fail to update TTL, crash recovery etc.
	// 1.2 Lock TTL -- active transaction holding the lock.
	// 2. NO LOCK
	// 2.1 Txn Committed
	// 2.2 Txn Rollbacked -- rollback itself, rollback by others, GC tomb etc.
	// 2.3 No lock -- pessimistic lock rollback, concurrence prewrite.

	var status TxnStatus
	req := tikvrpc.NewRequest(tikvrpc.CmdCheckTxnStatus, &kvrpcpb.CheckTxnStatusRequest{
		PrimaryKey:         primary,
		LockTs:             txnID,
		CallerStartTs:      callerStartTS,
		CurrentTs:          currentTS,
		RollbackIfNotExist: rollbackIfNotExist,
	})
	for {
		loc, err := lr.store.GetRegionCache().LocateKey(bo, primary)
		if err != nil {
			return status, errors.Trace(err)
		}
		resp, err := lr.store.SendReq(bo, req, loc.Region, readTimeoutShort)
		if err != nil {
			return status, errors.Trace(err)
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return status, errors.Trace(err)
		}
		if regionErr != nil {
			err = bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return status, errors.Trace(err)
			}
			continue
		}
		if resp.Resp == nil {
			return status, errors.Trace(ErrBodyMissing)
		}
		cmdResp := resp.Resp.(*kvrpcpb.CheckTxnStatusResponse)
		if keyErr := cmdResp.GetError(); keyErr != nil {
			txnNotFound := keyErr.GetTxnNotFound()
			if txnNotFound != nil {
				return status, txnNotFoundErr{txnNotFound}
			}

			err = errors.Errorf("unexpected err: %s, tid: %v", keyErr, txnID)
			logutil.BgLogger().Error("getTxnStatus error", zap.Error(err))
			return status, err
		}
		status.action = cmdResp.Action
		if cmdResp.LockTtl != 0 {
			status.ttl = cmdResp.LockTtl
		} else {
			if cmdResp.CommitVersion == 0 {
				tikvLockResolverCountWithQueryTxnStatusRolledBack.Inc()
			} else {
				tikvLockResolverCountWithQueryTxnStatusCommitted.Inc()
			}

			status.commitTS = cmdResp.CommitVersion
			lr.saveResolved(txnID, status)
		}
		return status, nil
	}
}

func (lr *LockResolver) resolveLock(bo *Backoffer, l *Lock, status TxnStatus, lite bool, cleanRegions map[RegionVerID]struct{}) error {
	tikvLockResolverCountWithResolveLocks.Inc()
	resolveLite := lite || l.TxnSize < bigTxnThreshold
	for {
		loc, err := lr.store.GetRegionCache().LocateKey(bo, l.Key)
		if err != nil {
			return errors.Trace(err)
		}
		if _, ok := cleanRegions[loc.Region]; ok {
			return nil
		}
		lreq := &kvrpcpb.ResolveLockRequest{
			StartVersion: l.TxnID,
		}
		if status.IsCommitted() {
			lreq.CommitVersion = status.CommitTS()
		} else {
			logutil.BgLogger().Info("resolveLock rollback", zap.String("lock", l.String()))
		}

		if resolveLite {
			// Only resolve specified keys when it is a small transaction,
			// prevent from scanning the whole region in this case.
			tikvLockResolverCountWithResolveLockLite.Inc()
			lreq.Keys = [][]byte{l.Key}
		}
		req := tikvrpc.NewRequest(tikvrpc.CmdResolveLock, lreq)
		resp, err := lr.store.SendReq(bo, req, loc.Region, readTimeoutShort)
		if err != nil {
			return errors.Trace(err)
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return errors.Trace(err)
		}
		if regionErr != nil {
			err = bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return errors.Trace(err)
			}
			continue
		}
		if resp.Resp == nil {
			return errors.Trace(ErrBodyMissing)
		}
		cmdResp := resp.Resp.(*kvrpcpb.ResolveLockResponse)
		if keyErr := cmdResp.GetError(); keyErr != nil {
			err = errors.Errorf("unexpected resolve err: %s, lock: %v", keyErr, l)
			logutil.BgLogger().Error("resolveLock error", zap.Error(err))
			return err
		}
		if !resolveLite {
			cleanRegions[loc.Region] = struct{}{}
		}
		return nil
	}
}

func (lr *LockResolver) resolvePessimisticLock(bo *Backoffer, l *Lock, cleanRegions map[RegionVerID]struct{}) error {
	tikvLockResolverCountWithResolveLocks.Inc()
	for {
		loc, err := lr.store.GetRegionCache().LocateKey(bo, l.Key)
		if err != nil {
			return errors.Trace(err)
		}
		if _, ok := cleanRegions[loc.Region]; ok {
			return nil
		}
		forUpdateTS := l.LockForUpdateTS
		if forUpdateTS == 0 {
			forUpdateTS = math.MaxUint64
		}
		pessimisticRollbackReq := &kvrpcpb.PessimisticRollbackRequest{
			StartVersion: l.TxnID,
			ForUpdateTs:  forUpdateTS,
			Keys:         [][]byte{l.Key},
		}
		req := tikvrpc.NewRequest(tikvrpc.CmdPessimisticRollback, pessimisticRollbackReq)
		resp, err := lr.store.SendReq(bo, req, loc.Region, readTimeoutShort)
		if err != nil {
			return errors.Trace(err)
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return errors.Trace(err)
		}
		if regionErr != nil {
			err = bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return errors.Trace(err)
			}
			continue
		}
		if resp.Resp == nil {
			return errors.Trace(ErrBodyMissing)
		}
		cmdResp := resp.Resp.(*kvrpcpb.PessimisticRollbackResponse)
		if keyErr := cmdResp.GetErrors(); len(keyErr) > 0 {
			err = errors.Errorf("unexpected resolve pessimistic lock err: %s, lock: %v", keyErr[0], l)
			logutil.Logger(bo.ctx).Error("resolveLock error", zap.Error(err))
			return err
		}
		return nil
	}
}
