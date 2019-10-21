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
	"container/list"
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/pd/client"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/util/logutil"
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
func NewLockResolver(etcdAddrs []string, security config.Security) (*LockResolver, error) {
	pdCli, err := pd.NewClient(etcdAddrs, pd.SecurityOption{
		CAPath:   security.ClusterSSLCA,
		CertPath: security.ClusterSSLCert,
		KeyPath:  security.ClusterSSLKey,
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	uuid := fmt.Sprintf("tikv-%v", pdCli.GetClusterID(context.TODO()))

	tlsConfig, err := security.ToTLSConfig()
	if err != nil {
		return nil, errors.Trace(err)
	}

	spkv, err := NewEtcdSafePointKV(etcdAddrs, tlsConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}

	s, err := newTikvStore(uuid, &codecPDClient{pdCli}, spkv, newRPCClient(security), false)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return s.lockResolver, nil
}

// TxnStatus represents a txn's final status. It should be Lock or Commit or Rollback.
type TxnStatus struct {
	ttl      uint64
	commitTS uint64
}

// IsCommitted returns true if the txn's final status is Commit.
func (s TxnStatus) IsCommitted() bool { return s.ttl == 0 && s.commitTS > 0 }

// CommitTS returns the txn's commitTS. It is valid iff `IsCommitted` is true.
func (s TxnStatus) CommitTS() uint64 { return uint64(s.commitTS) }

// By default, locks after 3000ms is considered unusual (the client created the
// lock might be dead). Other client may cleanup this kind of lock.
// For locks created recently, we will do backoff and retry.
var defaultLockTTL uint64 = 3000

// TODO: Consider if it's appropriate.
var maxLockTTL uint64 = 120000

// ttl = ttlFactor * sqrt(writeSizeInMiB)
var ttlFactor = 6000

// Lock represents a lock from tikv server.
type Lock struct {
	Key     []byte
	Primary []byte
	TxnID   uint64
	TTL     uint64
	TxnSize uint64
}

func (l *Lock) String() string {
	return fmt.Sprintf("key: %s, primary: %s, txnStartTS: %d, ttl: %d", l.Key, l.Primary, l.TxnID, l.TTL)
}

// NewLock creates a new *Lock.
func NewLock(l *kvrpcpb.LockInfo) *Lock {
	return &Lock{
		Key:     l.GetKey(),
		Primary: l.GetPrimaryLock(),
		TxnID:   l.GetLockVersion(),
		TTL:     l.GetLockTtl(),
		TxnSize: l.GetTxnSize(),
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

	expiredLocks := make([]*Lock, 0, len(locks))
	for _, l := range locks {
		if lr.store.GetOracle().IsExpired(l.TxnID, l.TTL) {
			tikvLockResolverCountWithExpired.Inc()
			expiredLocks = append(expiredLocks, l)
		} else {
			tikvLockResolverCountWithNotExpired.Inc()
		}
	}
	if len(expiredLocks) != len(locks) {
		logutil.BgLogger().Error("BatchResolveLocks: maybe safe point is wrong!",
			zap.Int("get locks", len(locks)),
			zap.Int("expired locks", len(expiredLocks)))
		return false, nil
	}

	startTS, err := lr.store.GetOracle().GetTimestamp(bo.ctx)
	if err != nil {
		return false, errors.Trace(err)
	}

	txnInfos := make(map[uint64]uint64)
	startTime := time.Now()
	for _, l := range expiredLocks {
		if _, ok := txnInfos[l.TxnID]; ok {
			continue
		}

		currentTS, err := lr.store.GetOracle().GetLowResolutionTimestamp(bo.ctx)
		if err != nil {
			return false, err
		}
		status, err := lr.getTxnStatus(bo, l.TxnID, l.Primary, startTS, currentTS)
		if err != nil {
			return false, err
		}

		if status.ttl > 0 {
			// Do not clean lock that is not expired.
			continue
		}

		txnInfos[l.TxnID] = uint64(status.commitTS)
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
func (lr *LockResolver) ResolveLocks(bo *Backoffer, callerStartTS uint64, locks []*Lock) (int64, error) {
	var msBeforeTxnExpired txnExpireTime
	if len(locks) == 0 {
		return msBeforeTxnExpired.value(), nil
	}

	tikvLockResolverCountWithResolve.Inc()

	var expiredLocks []*Lock
	for _, l := range locks {
		msBeforeLockExpired := lr.store.GetOracle().UntilExpired(l.TxnID, l.TTL)
		if msBeforeLockExpired <= 0 {
			expiredLocks = append(expiredLocks, l)
		} else {
			msBeforeTxnExpired.update(int64(l.TTL))
			tikvLockResolverCountWithNotExpired.Inc()
		}
	}
	// TxnID -> []Region, record resolved Regions.
	// TODO: Maybe put it in LockResolver and share by all txns.
	cleanTxns := make(map[uint64]map[RegionVerID]struct{})
	for _, l := range expiredLocks {
		status, err := lr.getTxnStatusFromLock(bo, l, callerStartTS)
		if err != nil {
			msBeforeTxnExpired.update(0)
			err = errors.Trace(err)
			return msBeforeTxnExpired.value(), err
		}

		if status.ttl == 0 {
			tikvLockResolverCountWithExpired.Inc()
			// If the lock is committed or rollbacked, resolve lock.
			cleanRegions, exists := cleanTxns[l.TxnID]
			if !exists {
				cleanRegions = make(map[RegionVerID]struct{})
				cleanTxns[l.TxnID] = cleanRegions
			}

			err = lr.resolveLock(bo, l, status, cleanRegions)
			if err != nil {
				msBeforeTxnExpired.update(0)
				err = errors.Trace(err)
				return msBeforeTxnExpired.value(), err
			}
		} else {
			tikvLockResolverCountWithNotExpired.Inc()
			// If the lock is valid, the txn may be a pessimistic transaction.
			// Update the txn expire time.
			msBeforeLockExpired := lr.store.GetOracle().UntilExpired(l.TxnID, status.ttl)
			msBeforeTxnExpired.update(msBeforeLockExpired)
		}
	}

	if msBeforeTxnExpired.value() > 0 {
		tikvLockResolverCountWithWaitExpired.Inc()
	}
	return msBeforeTxnExpired.value(), nil
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
	return
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
	bo := NewBackoffer(context.Background(), cleanupMaxBackoff)
	currentTS, err := lr.store.GetOracle().GetLowResolutionTimestamp(bo.ctx)
	if err != nil {
		return status, err
	}
	return lr.getTxnStatus(bo, txnID, primary, callerStartTS, currentTS)
}

func (lr *LockResolver) getTxnStatusFromLock(bo *Backoffer, l *Lock, callerStartTS uint64) (TxnStatus, error) {
	var currentTS uint64
	if l.TTL == 0 {
		// NOTE: l.TTL = 0 is a special protocol!!!
		// When the pessimistic txn prewrite meets locks of a txn, it should resolve the lock **unconditionally**.
		// In this case, TiKV use lock TTL = 0 to notify TiDB, and TiDB should resolve the lock!
		// Set currentTS to max uint64 to make the lock expired.
		currentTS = math.MaxUint64
	} else {
		var err error
		currentTS, err = lr.store.GetOracle().GetLowResolutionTimestamp(bo.ctx)
		if err != nil {
			return TxnStatus{}, err
		}
	}
	return lr.getTxnStatus(bo, l.TxnID, l.Primary, callerStartTS, currentTS)
}

func (lr *LockResolver) getTxnStatus(bo *Backoffer, txnID uint64, primary []byte, callerStartTS, currentTS uint64) (TxnStatus, error) {
	if s, ok := lr.getResolved(txnID); ok {
		return s, nil
	}

	tikvLockResolverCountWithQueryTxnStatus.Inc()

	var status TxnStatus
	req := tikvrpc.NewRequest(tikvrpc.CmdCheckTxnStatus, &kvrpcpb.CheckTxnStatusRequest{
		PrimaryKey:    primary,
		LockTs:        txnID,
		CallerStartTs: callerStartTS,
		CurrentTs:     currentTS,
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
			err = errors.Errorf("unexpected err: %s, tid: %v", keyErr, txnID)
			logutil.BgLogger().Error("getTxnStatus error", zap.Error(err))
			return status, err
		}
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

func (lr *LockResolver) resolveLock(bo *Backoffer, l *Lock, status TxnStatus, cleanRegions map[RegionVerID]struct{}) error {
	tikvLockResolverCountWithResolveLocks.Inc()
	cleanWholeRegion := l.TxnSize >= bigTxnThreshold
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
		}
		if l.TxnSize < bigTxnThreshold {
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
		if cleanWholeRegion {
			cleanRegions[loc.Region] = struct{}{}
		}
		return nil
	}
}
