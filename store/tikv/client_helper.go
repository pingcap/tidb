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

package tikv

import (
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/store/tikv/util"
)

// ClientHelper wraps LockResolver and RegionRequestSender.
// It's introduced to support the new lock resolving pattern in the large transaction.
// In the large transaction protocol, sending requests and resolving locks are
// context-dependent. For example, when a send request meets a secondary lock, we'll
// call ResolveLock, and if the lock belongs to a large transaction, we may retry
// the request. If there is no context information about the resolved locks, we'll
// meet the secondary lock again and run into a deadloop.
type ClientHelper struct {
	lockResolver  *LockResolver
	regionCache   *RegionCache
	resolvedLocks *util.TSSet
	client        Client
	resolveLite   bool
	RegionRequestRuntimeStats
}

// NewClientHelper creates a helper instance.
func NewClientHelper(store *KVStore, resolvedLocks *util.TSSet, resolveLockLite bool) *ClientHelper {
	return &ClientHelper{
		lockResolver:  store.GetLockResolver(),
		regionCache:   store.GetRegionCache(),
		resolvedLocks: resolvedLocks,
		client:        store.GetTiKVClient(),
		resolveLite:   resolveLockLite,
	}
}

// ResolveLocks wraps the ResolveLocks function and store the resolved result.
func (ch *ClientHelper) ResolveLocks(bo *Backoffer, callerStartTS uint64, locks []*Lock) (int64, error) {
	var err error
	var resolvedLocks []uint64
	var msBeforeTxnExpired int64
	if ch.Stats != nil {
		defer func(start time.Time) {
			RecordRegionRequestRuntimeStats(ch.Stats, tikvrpc.CmdResolveLock, time.Since(start))
		}(time.Now())
	}
	if ch.resolveLite {
		msBeforeTxnExpired, resolvedLocks, err = ch.lockResolver.ResolveLocksLite(bo, callerStartTS, locks)
	} else {
		msBeforeTxnExpired, resolvedLocks, err = ch.lockResolver.ResolveLocks(bo, callerStartTS, locks)
	}
	if err != nil {
		return msBeforeTxnExpired, err
	}
	if len(resolvedLocks) > 0 {
		ch.resolvedLocks.Put(resolvedLocks...)
		return 0, nil
	}
	return msBeforeTxnExpired, nil
}

// SendReqCtx wraps the SendReqCtx function and use the resolved lock result in the kvrpcpb.Context.
func (ch *ClientHelper) SendReqCtx(bo *Backoffer, req *tikvrpc.Request, regionID RegionVerID, timeout time.Duration, et tikvrpc.EndpointType, directStoreAddr string, opts ...StoreSelectorOption) (*tikvrpc.Response, *RPCContext, string, error) {
	sender := NewRegionRequestSender(ch.regionCache, ch.client)
	if len(directStoreAddr) > 0 {
		sender.SetStoreAddr(directStoreAddr)
	}
	sender.Stats = ch.Stats
	req.Context.ResolvedLocks = ch.resolvedLocks.GetAll()
	failpoint.Inject("assertStaleReadFlag", func(val failpoint.Value) {
		if val.(bool) {
			if len(opts) > 0 && !req.StaleRead {
				panic("req.StaleRead shouldn't be false when opts is not empty")
			}
		}
	})
	resp, ctx, err := sender.SendReqCtx(bo, req, regionID, timeout, et, opts...)
	return resp, ctx, sender.GetStoreAddr(), err
}
