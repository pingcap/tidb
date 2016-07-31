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
	"sync"

	"github.com/gogo/protobuf/proto"
	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
)

const resolvedCacheSize = 512

// LockResolver resolves locks and also caches resolved txn status.
type LockResolver struct {
	store *tikvStore
	mu    struct {
		sync.RWMutex
		// Cache resolved txns (FIFO, txn id -> txnStatus).
		resolved       map[uint64]txnStatus
		recentResolved *list.List
		// Maximum txnID that guaranteed to be expired.
		maxExpire uint64
	}
}

// NewLockResolver creates a LockResolver.
func NewLockResolver(store *tikvStore) *LockResolver {
	r := &LockResolver{
		store: store,
	}
	r.mu.resolved = make(map[uint64]txnStatus)
	r.mu.recentResolved = list.New()
	return r
}

type txnStatus uint64

func (s txnStatus) isCommitted() bool { return s > 0 }
func (s txnStatus) commitTS() uint64  { return uint64(s) }

// locks after 3000ms is considered unusual (the client created the lock might
// be dead). Other client may cleanup this kind of lock.
// For locks created recently, we will do backoff and retry.
var lockTTL uint64 = 3000

// Lock represents a lock from tikv server.
type Lock struct {
	Key     []byte
	Primary []byte
	TxnID   uint64
}

func (lr *LockResolver) saveResolved(txnID uint64, status txnStatus) {
	lr.mu.Lock()
	defer lr.mu.Unlock()

	if _, ok := lr.mu.resolved[txnID]; ok {
		return
	}
	lr.mu.resolved[txnID] = status
	lr.mu.recentResolved.PushBack(txnID)
	if len(lr.mu.resolved) > resolvedCacheSize {
		front := lr.mu.recentResolved.Front()
		delete(lr.mu.resolved, front.Value.(uint64))
		lr.mu.recentResolved.Remove(front)
	}
}

func (lr *LockResolver) getResolved(txnID uint64) (txnStatus, bool) {
	lr.mu.RLock()
	defer lr.mu.RUnlock()

	s, ok := lr.mu.resolved[txnID]
	return s, ok
}

func (lr *LockResolver) isExpire(bo *Backoffer, l *Lock) (bool, error) {
	lr.mu.RLock()
	maxExpire := lr.mu.maxExpire
	lr.mu.RUnlock()

	if l.TxnID <= maxExpire {
		return true, nil
	}
	expired, err := lr.store.checkTimestampExpiredWithRetry(bo, l.TxnID, lockTTL)
	if err != nil {
		return false, errors.Trace(err)
	}
	if expired {
		lr.mu.Lock()
		if l.TxnID > lr.mu.maxExpire {
			lr.mu.maxExpire = l.TxnID
		}
		lr.mu.Unlock()
	}
	return expired, nil
}

// ResolveLocks tries to resolve Locks. If returned `ok` is false, there
// are some locks not expired, caller need to sleep then retry later.
func (lr *LockResolver) ResolveLocks(bo *Backoffer, locks []*Lock) (ok bool, err error) {
	var expiredLocks []*Lock
	for _, l := range locks {
		isExpired, err := lr.isExpire(bo, l)
		if err != nil {
			return false, errors.Trace(err)
		}
		if isExpired {
			expiredLocks = append(expiredLocks, l)
		}
	}

	// TxnID -> []Region, record resolved Regions.
	// TODO: Maybe put it in LockResolver and share by all txns.
	cleanTxns := make(map[uint64]map[RegionVerID]struct{})
	for _, l := range expiredLocks {
		status, err := lr.getTxnStatus(bo, l.TxnID, l.Primary)
		if err != nil {
			return false, errors.Trace(err)
		}

		cleanRegions := cleanTxns[l.TxnID]
		if cleanRegions == nil {
			cleanRegions = make(map[RegionVerID]struct{})
			cleanTxns[l.TxnID] = cleanRegions
		}

		err = lr.resolveLock(bo, l, status, cleanRegions)
		if err != nil {
			return false, errors.Trace(err)
		}
	}
	return len(expiredLocks) == len(locks), nil
}

func (lr *LockResolver) getTxnStatus(bo *Backoffer, txnID uint64, primary []byte) (txnStatus, error) {
	if s, ok := lr.getResolved(txnID); ok {
		return s, nil
	}
	var status txnStatus
	req := &kvrpcpb.Request{
		Type: kvrpcpb.MessageType_CmdCleanup.Enum(),
		CmdCleanupReq: &kvrpcpb.CmdCleanupRequest{
			Key:          primary,
			StartVersion: proto.Uint64(txnID),
		},
	}
	for {
		region, err := lr.store.regionCache.GetRegion(bo, primary)
		if err != nil {
			return status, errors.Trace(err)
		}
		resp, err := lr.store.SendKVReq(bo, req, region.VerID())
		if err != nil {
			return status, errors.Trace(err)
		}
		if regionErr := resp.GetRegionError(); regionErr != nil {
			err = bo.Backoff(boRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return status, errors.Trace(err)
			}
			continue
		}
		cmdResp := resp.GetCmdCleanupResp()
		if cmdResp == nil {
			return status, errors.Trace(errBodyMissing)
		}
		if keyErr := cmdResp.GetError(); keyErr != nil {
			return status, errors.Errorf("unexpected cleanup err: %s", keyErr)
		}
		if cmdResp.CommitVersion != nil {
			status = txnStatus(cmdResp.GetCommitVersion())
		}
		lr.saveResolved(txnID, status)
		return status, nil
	}
}

func (lr *LockResolver) resolveLock(bo *Backoffer, l *Lock, status txnStatus, cleanRegions map[RegionVerID]struct{}) error {
	for {
		region, err := lr.store.regionCache.GetRegion(bo, l.Key)
		if err != nil {
			return errors.Trace(err)
		}
		if _, ok := cleanRegions[region.VerID()]; ok {
			return nil
		}
		req := &kvrpcpb.Request{
			Type: kvrpcpb.MessageType_CmdResolveLock.Enum(),
			CmdResolveLockReq: &kvrpcpb.CmdResolveLockRequest{
				StartVersion: proto.Uint64(l.TxnID),
			},
		}
		if status.isCommitted() {
			req.GetCmdResolveLockReq().CommitVersion = proto.Uint64(status.commitTS())
		}
		resp, err := lr.store.SendKVReq(bo, req, region.VerID())
		if err != nil {
			return errors.Trace(err)
		}
		if regionErr := resp.GetRegionError(); regionErr != nil {
			err = bo.Backoff(boRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return errors.Trace(err)
			}
			continue
		}
		cmdResp := resp.GetCmdResolveLockResp()
		if cmdResp == nil {
			return errors.Trace(errBodyMissing)
		}
		if keyErr := cmdResp.GetError(); keyErr != nil {
			return errors.Errorf("unexpected resolve err: %s", keyErr)
		}
		cleanRegions[region.VerID()] = struct{}{}
		return nil
	}
}
