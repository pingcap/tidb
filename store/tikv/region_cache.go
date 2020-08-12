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
	"context"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/gogo/protobuf/proto"
	"github.com/google/btree"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	pd "github.com/pingcap/pd/v4/client"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	atomic2 "go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"
)

const (
	btreeDegree               = 32
	invalidatedLastAccessTime = -1
	defaultRegionsPerBatch    = 128
)

// RegionCacheTTLSec is the max idle time for regions in the region cache.
var RegionCacheTTLSec int64 = 600

var (
	tikvRegionCacheCounterWithInvalidateRegionFromCacheOK = metrics.TiKVRegionCacheCounter.WithLabelValues("invalidate_region_from_cache", "ok")
	tikvRegionCacheCounterWithSendFail                    = metrics.TiKVRegionCacheCounter.WithLabelValues("send_fail", "ok")
	tikvRegionCacheCounterWithGetRegionByIDOK             = metrics.TiKVRegionCacheCounter.WithLabelValues("get_region_by_id", "ok")
	tikvRegionCacheCounterWithGetRegionByIDError          = metrics.TiKVRegionCacheCounter.WithLabelValues("get_region_by_id", "err")
	tikvRegionCacheCounterWithGetRegionOK                 = metrics.TiKVRegionCacheCounter.WithLabelValues("get_region", "ok")
	tikvRegionCacheCounterWithGetRegionError              = metrics.TiKVRegionCacheCounter.WithLabelValues("get_region", "err")
	tikvRegionCacheCounterWithScanRegionsOK               = metrics.TiKVRegionCacheCounter.WithLabelValues("scan_regions", "ok")
	tikvRegionCacheCounterWithScanRegionsError            = metrics.TiKVRegionCacheCounter.WithLabelValues("scan_regions", "err")
	tikvRegionCacheCounterWithGetStoreOK                  = metrics.TiKVRegionCacheCounter.WithLabelValues("get_store", "ok")
	tikvRegionCacheCounterWithGetStoreError               = metrics.TiKVRegionCacheCounter.WithLabelValues("get_store", "err")
	tikvRegionCacheCounterWithInvalidateStoreRegionsOK    = metrics.TiKVRegionCacheCounter.WithLabelValues("invalidate_store_regions", "ok")

	tikvStatusCountWithOK    = metrics.TiKVStatusCounter.WithLabelValues("ok")
	tikvStatusCountWithError = metrics.TiKVStatusCounter.WithLabelValues("err")
)

const (
	updated  int32 = iota // region is updated and no need to reload.
	needSync              //  need sync new region info.
)

// Region presents kv region
type Region struct {
	meta       *metapb.Region // raw region meta from PD immutable after init
	store      unsafe.Pointer // point to region store info, see RegionStore
	syncFlag   int32          // region need be sync in next turn
	lastAccess int64          // last region access time, see checkRegionCacheTTL
}

// AccessMode uses to index stores for different region cache access requirements.
type AccessMode int

const (
	// TiKvOnly indicates stores list that use for TiKv access(include both leader request and follower read).
	TiKvOnly AccessMode = iota
	// TiFlashOnly indicates stores list that use for TiFlash request.
	TiFlashOnly
	// NumAccessMode reserved to keep max access mode value.
	NumAccessMode
)

func (a AccessMode) String() string {
	switch a {
	case TiKvOnly:
		return "TiKvOnly"
	case TiFlashOnly:
		return "TiFlashOnly"
	default:
		return fmt.Sprintf("%d", a)
	}
}

// AccessIndex represent the index for accessIndex array
type AccessIndex int

// RegionStore represents region stores info
// it will be store as unsafe.Pointer and be load at once
type RegionStore struct {
	workTiKVIdx    AccessIndex          // point to current work peer in meta.Peers and work store in stores(same idx) for tikv peer
	workTiFlashIdx int32                // point to current work peer in meta.Peers and work store in stores(same idx) for tiflash peer
	stores         []*Store             // stores in this region
	storeEpochs    []uint32             // snapshots of store's epoch, need reload when `storeEpochs[curr] != stores[cur].fail`
	accessIndex    [NumAccessMode][]int // AccessMode => idx in stores
}

func (r *RegionStore) accessStore(mode AccessMode, idx AccessIndex) (int, *Store) {
	sidx := r.accessIndex[mode][idx]
	return sidx, r.stores[sidx]
}

func (r *RegionStore) accessStoreNum(mode AccessMode) int {
	return len(r.accessIndex[mode])
}

// clone clones region store struct.
func (r *RegionStore) clone() *RegionStore {
	storeEpochs := make([]uint32, len(r.stores))
	rs := &RegionStore{
		workTiFlashIdx: r.workTiFlashIdx,
		workTiKVIdx:    r.workTiKVIdx,
		stores:         r.stores,
		storeEpochs:    storeEpochs,
	}
	copy(storeEpochs, r.storeEpochs)
	for i := 0; i < int(NumAccessMode); i++ {
		rs.accessIndex[i] = make([]int, len(r.accessIndex[i]))
		copy(rs.accessIndex[i], r.accessIndex[i])
	}
	return rs
}

// return next follower store's index
func (r *RegionStore) follower(seed uint32) AccessIndex {
	l := uint32(r.accessStoreNum(TiKvOnly))
	if l <= 1 {
		return r.workTiKVIdx
	}

	for retry := l - 1; retry > 0; retry-- {
		followerIdx := AccessIndex(seed % (l - 1))
		if followerIdx >= r.workTiKVIdx {
			followerIdx++
		}
		storeIdx, s := r.accessStore(TiKvOnly, followerIdx)
		if r.storeEpochs[storeIdx] == atomic.LoadUint32(&s.epoch) {
			return followerIdx
		}
		seed++
	}
	return r.workTiKVIdx
}

// return next leader or follower store's index
func (r *RegionStore) kvPeer(seed uint32) AccessIndex {
	candidates := make([]AccessIndex, 0, r.accessStoreNum(TiKvOnly))
	for i := 0; i < r.accessStoreNum(TiKvOnly); i++ {
		storeIdx, s := r.accessStore(TiKvOnly, AccessIndex(i))
		if r.storeEpochs[storeIdx] != atomic.LoadUint32(&s.epoch) {
			continue
		}
		candidates = append(candidates, AccessIndex(i))
	}

	if len(candidates) == 0 {
		return r.workTiKVIdx
	}
	return candidates[int32(seed)%int32(len(candidates))]
}

// init initializes region after constructed.
func (r *Region) init(c *RegionCache) error {
	// region store pull used store from global store map
	// to avoid acquire storeMu in later access.
	rs := &RegionStore{
		workTiKVIdx:    0,
		workTiFlashIdx: 0,
		stores:         make([]*Store, 0, len(r.meta.Peers)),
		storeEpochs:    make([]uint32, 0, len(r.meta.Peers)),
	}
	for _, p := range r.meta.Peers {
		c.storeMu.RLock()
		store, exists := c.storeMu.stores[p.StoreId]
		c.storeMu.RUnlock()
		if !exists {
			store = c.getStoreByStoreID(p.StoreId)
		}
		_, err := store.initResolve(NewNoopBackoff(context.Background()), c)
		if err != nil {
			return err
		}
		switch store.storeType {
		case kv.TiKV:
			rs.accessIndex[TiKvOnly] = append(rs.accessIndex[TiKvOnly], len(rs.stores))
		case kv.TiFlash:
			rs.accessIndex[TiFlashOnly] = append(rs.accessIndex[TiFlashOnly], len(rs.stores))
		}
		rs.stores = append(rs.stores, store)
		rs.storeEpochs = append(rs.storeEpochs, atomic.LoadUint32(&store.epoch))
	}
	atomic.StorePointer(&r.store, unsafe.Pointer(rs))

	// mark region has been init accessed.
	r.lastAccess = time.Now().Unix()
	return nil
}

func (r *Region) getStore() (store *RegionStore) {
	store = (*RegionStore)(atomic.LoadPointer(&r.store))
	return
}

func (r *Region) compareAndSwapStore(oldStore, newStore *RegionStore) bool {
	return atomic.CompareAndSwapPointer(&r.store, unsafe.Pointer(oldStore), unsafe.Pointer(newStore))
}

func (r *Region) checkRegionCacheTTL(ts int64) bool {
	for {
		lastAccess := atomic.LoadInt64(&r.lastAccess)
		if ts-lastAccess > RegionCacheTTLSec {
			return false
		}
		if atomic.CompareAndSwapInt64(&r.lastAccess, lastAccess, ts) {
			return true
		}
	}
}

// invalidate invalidates a region, next time it will got null result.
func (r *Region) invalidate() {
	tikvRegionCacheCounterWithInvalidateRegionFromCacheOK.Inc()
	atomic.StoreInt64(&r.lastAccess, invalidatedLastAccessTime)
}

// scheduleReload schedules reload region request in next LocateKey.
func (r *Region) scheduleReload() {
	oldValue := atomic.LoadInt32(&r.syncFlag)
	if oldValue != updated {
		return
	}
	atomic.CompareAndSwapInt32(&r.syncFlag, oldValue, needSync)
}

// needReload checks whether region need reload.
func (r *Region) needReload() bool {
	oldValue := atomic.LoadInt32(&r.syncFlag)
	if oldValue == updated {
		return false
	}
	return atomic.CompareAndSwapInt32(&r.syncFlag, oldValue, updated)
}

// RegionCache caches Regions loaded from PD.
type RegionCache struct {
	pdClient pd.Client

	mu struct {
		sync.RWMutex                         // mutex protect cached region
		regions      map[RegionVerID]*Region // cached regions be organized as regionVerID to region ref mapping
		sorted       *btree.BTree            // cache regions be organized as sorted key to region ref mapping
	}
	storeMu struct {
		sync.RWMutex
		stores map[uint64]*Store
	}
	notifyCheckCh chan struct{}
	closeCh       chan struct{}
}

// NewRegionCache creates a RegionCache.
func NewRegionCache(pdClient pd.Client) *RegionCache {
	c := &RegionCache{
		pdClient: pdClient,
	}
	c.mu.regions = make(map[RegionVerID]*Region)
	c.mu.sorted = btree.New(btreeDegree)
	c.storeMu.stores = make(map[uint64]*Store)
	c.notifyCheckCh = make(chan struct{}, 1)
	c.closeCh = make(chan struct{})
	go c.asyncCheckAndResolveLoop()
	return c
}

// Close releases region cache's resource.
func (c *RegionCache) Close() {
	close(c.closeCh)
}

// asyncCheckAndResolveLoop with
func (c *RegionCache) asyncCheckAndResolveLoop() {
	var needCheckStores []*Store
	for {
		select {
		case <-c.closeCh:
			return
		case <-c.notifyCheckCh:
			needCheckStores = needCheckStores[:0]
			c.checkAndResolve(needCheckStores)
		}
	}
}

// checkAndResolve checks and resolve addr of failed stores.
// this method isn't thread-safe and only be used by one goroutine.
func (c *RegionCache) checkAndResolve(needCheckStores []*Store) {
	defer func() {
		r := recover()
		if r != nil {
			logutil.BgLogger().Error("panic in the checkAndResolve goroutine",
				zap.Reflect("r", r),
				zap.Stack("stack trace"))
		}
	}()

	c.storeMu.RLock()
	for _, store := range c.storeMu.stores {
		state := store.getResolveState()
		if state == needCheck {
			needCheckStores = append(needCheckStores, store)
		}
	}
	c.storeMu.RUnlock()

	for _, store := range needCheckStores {
		store.reResolve(c)
	}
}

// RPCContext contains data that is needed to send RPC to a region.
type RPCContext struct {
	Region     RegionVerID
	Meta       *metapb.Region
	Peer       *metapb.Peer
	AccessIdx  AccessIndex
	Store      *Store
	Addr       string
	AccessMode AccessMode
}

func (c *RPCContext) String() string {
	var runStoreType string
	if c.Store != nil {
		runStoreType = c.Store.storeType.Name()
	}
	return fmt.Sprintf("region ID: %d, meta: %s, peer: %s, addr: %s, idx: %d, reqStoreType: %s, runStoreType: %s",
		c.Region.GetID(), c.Meta, c.Peer, c.Addr, c.AccessIdx, c.AccessMode, runStoreType)
}

// GetTiKVRPCContext returns RPCContext for a region. If it returns nil, the region
// must be out of date and already dropped from cache.
func (c *RegionCache) GetTiKVRPCContext(bo *Backoffer, id RegionVerID, replicaRead kv.ReplicaReadType, followerStoreSeed uint32) (*RPCContext, error) {
	ts := time.Now().Unix()

	cachedRegion := c.getCachedRegionWithRLock(id)
	if cachedRegion == nil {
		return nil, nil
	}

	if !cachedRegion.checkRegionCacheTTL(ts) {
		return nil, nil
	}

	regionStore := cachedRegion.getStore()
	var (
		store     *Store
		peer      *metapb.Peer
		storeIdx  int
		accessIdx AccessIndex
	)
	switch replicaRead {
	case kv.ReplicaReadFollower:
		store, peer, accessIdx, storeIdx = cachedRegion.FollowerStorePeer(regionStore, followerStoreSeed)
	case kv.ReplicaReadMixed:
		store, peer, accessIdx, storeIdx = cachedRegion.AnyStorePeer(regionStore, followerStoreSeed)
	default:
		store, peer, accessIdx, storeIdx = cachedRegion.WorkStorePeer(regionStore)
	}
	addr, err := c.getStoreAddr(bo, cachedRegion, store, storeIdx)
	if err != nil {
		return nil, err
	}
	// enable by `curl -XPUT -d '1*return("[some-addr]")->return("")' http://host:port/github.com/pingcap/tidb/store/tikv/injectWrongStoreAddr`
	failpoint.Inject("injectWrongStoreAddr", func(val failpoint.Value) {
		if a, ok := val.(string); ok && len(a) > 0 {
			addr = a
		}
	})
	if store == nil || len(addr) == 0 {
		// Store not found, region must be out of date.
		cachedRegion.invalidate()
		return nil, nil
	}

	storeFailEpoch := atomic.LoadUint32(&store.epoch)
	if storeFailEpoch != regionStore.storeEpochs[storeIdx] {
		cachedRegion.invalidate()
		logutil.BgLogger().Info("invalidate current region, because others failed on same store",
			zap.Uint64("region", id.GetID()),
			zap.String("store", store.addr))
		return nil, nil
	}

	return &RPCContext{
		Region:     id,
		Meta:       cachedRegion.meta,
		Peer:       peer,
		AccessIdx:  accessIdx,
		Store:      store,
		Addr:       addr,
		AccessMode: TiKvOnly,
	}, nil
}

// GetTiFlashRPCContext returns RPCContext for a region must access flash store. If it returns nil, the region
// must be out of date and already dropped from cache or not flash store found.
func (c *RegionCache) GetTiFlashRPCContext(bo *Backoffer, id RegionVerID) (*RPCContext, error) {
	ts := time.Now().Unix()

	cachedRegion := c.getCachedRegionWithRLock(id)
	if cachedRegion == nil {
		return nil, nil
	}
	if !cachedRegion.checkRegionCacheTTL(ts) {
		return nil, nil
	}

	regionStore := cachedRegion.getStore()

	// sIdx is for load balance of TiFlash store.
	sIdx := int(atomic.AddInt32(&regionStore.workTiFlashIdx, 1))
	for i := 0; i < regionStore.accessStoreNum(TiFlashOnly); i++ {
		accessIdx := AccessIndex((sIdx + i) % regionStore.accessStoreNum(TiFlashOnly))
		storeIdx, store := regionStore.accessStore(TiFlashOnly, accessIdx)
		addr, err := c.getStoreAddr(bo, cachedRegion, store, storeIdx)
		if err != nil {
			return nil, err
		}
		if len(addr) == 0 {
			cachedRegion.invalidate()
			return nil, nil
		}
		if store.getResolveState() == needCheck {
			store.reResolve(c)
		}
		atomic.StoreInt32(&regionStore.workTiFlashIdx, int32(accessIdx))
		peer := cachedRegion.meta.Peers[storeIdx]
		storeFailEpoch := atomic.LoadUint32(&store.epoch)
		if storeFailEpoch != regionStore.storeEpochs[storeIdx] {
			cachedRegion.invalidate()
			logutil.BgLogger().Info("invalidate current region, because others failed on same store",
				zap.Uint64("region", id.GetID()),
				zap.String("store", store.addr))
			// TiFlash will always try to find out a valid peer, avoiding to retry too many times.
			continue
		}
		return &RPCContext{
			Region:     id,
			Meta:       cachedRegion.meta,
			Peer:       peer,
			AccessIdx:  accessIdx,
			Store:      store,
			Addr:       addr,
			AccessMode: TiFlashOnly,
		}, nil
	}

	cachedRegion.invalidate()
	return nil, nil
}

// KeyLocation is the region and range that a key is located.
type KeyLocation struct {
	Region   RegionVerID
	StartKey kv.Key
	EndKey   kv.Key
}

// Contains checks if key is in [StartKey, EndKey).
func (l *KeyLocation) Contains(key []byte) bool {
	return bytes.Compare(l.StartKey, key) <= 0 &&
		(bytes.Compare(key, l.EndKey) < 0 || len(l.EndKey) == 0)
}

// LocateKey searches for the region and range that the key is located.
func (c *RegionCache) LocateKey(bo *Backoffer, key []byte) (*KeyLocation, error) {
	r, err := c.findRegionByKey(bo, key, false)
	if err != nil {
		return nil, err
	}
	return &KeyLocation{
		Region:   r.VerID(),
		StartKey: r.StartKey(),
		EndKey:   r.EndKey(),
	}, nil
}

// LocateEndKey searches for the region and range that the key is located.
// Unlike LocateKey, start key of a region is exclusive and end key is inclusive.
func (c *RegionCache) LocateEndKey(bo *Backoffer, key []byte) (*KeyLocation, error) {
	r, err := c.findRegionByKey(bo, key, true)
	if err != nil {
		return nil, err
	}
	return &KeyLocation{
		Region:   r.VerID(),
		StartKey: r.StartKey(),
		EndKey:   r.EndKey(),
	}, nil
}

func (c *RegionCache) findRegionByKey(bo *Backoffer, key []byte, isEndKey bool) (r *Region, err error) {
	r = c.searchCachedRegion(key, isEndKey)
	if r == nil {
		// load region when it is not exists or expired.
		lr, err := c.loadRegion(bo, key, isEndKey)
		if err != nil {
			// no region data, return error if failure.
			return nil, err
		}
		logutil.Eventf(bo.ctx, "load region %d from pd, due to cache-miss", lr.GetID())
		r = lr
		c.mu.Lock()
		c.insertRegionToCache(r)
		c.mu.Unlock()
	} else if r.needReload() {
		// load region when it be marked as need reload.
		lr, err := c.loadRegion(bo, key, isEndKey)
		if err != nil {
			// ignore error and use old region info.
			logutil.Logger(bo.ctx).Error("load region failure",
				zap.ByteString("key", key), zap.Error(err))
		} else {
			logutil.Eventf(bo.ctx, "load region %d from pd, due to need-reload", lr.GetID())
			r = lr
			c.mu.Lock()
			c.insertRegionToCache(r)
			c.mu.Unlock()
		}
	}
	return r, nil
}

// OnSendFail handles send request fail logic.
func (c *RegionCache) OnSendFail(bo *Backoffer, ctx *RPCContext, scheduleReload bool, err error) {
	tikvRegionCacheCounterWithSendFail.Inc()
	r := c.getCachedRegionWithRLock(ctx.Region)
	if r != nil {
		rs := r.getStore()
		if err != nil {
			storeIdx, s := rs.accessStore(ctx.AccessMode, ctx.AccessIdx)
			followerRead := rs.workTiKVIdx != ctx.AccessIdx

			// send fail but store is reachable, keep retry current peer for replica leader request.
			// but we still need switch peer for follower-read or learner-read(i.e. tiflash)
			if ctx.Store.storeType == kv.TiKV && !followerRead && s.requestLiveness(bo) == reachable {
				return
			}

			// invalidate regions in store.
			epoch := rs.storeEpochs[storeIdx]
			if atomic.CompareAndSwapUint32(&s.epoch, epoch, epoch+1) {
				logutil.BgLogger().Info("mark store's regions need be refill", zap.String("store", s.addr))
				tikvRegionCacheCounterWithInvalidateStoreRegionsOK.Inc()
			}

			// schedule a store addr resolve.
			s.markNeedCheck(c.notifyCheckCh)
		}

		// try next peer to found new leader.
		if ctx.AccessMode == TiKvOnly {
			rs.switchNextTiKVPeer(r, ctx.AccessIdx)
		} else {
			rs.switchNextFlashPeer(r, ctx.AccessIdx)
		}

		// force reload region when retry all known peers in region.
		if scheduleReload {
			r.scheduleReload()
		}
		logutil.Logger(bo.ctx).Info("switch region peer to next due to send request fail",
			zap.Stringer("current", ctx),
			zap.Bool("needReload", scheduleReload),
			zap.Error(err))
	}
}

// LocateRegionByID searches for the region with ID.
func (c *RegionCache) LocateRegionByID(bo *Backoffer, regionID uint64) (*KeyLocation, error) {
	c.mu.RLock()
	r := c.getRegionByIDFromCache(regionID)
	c.mu.RUnlock()
	if r != nil {
		if r.needReload() {
			lr, err := c.loadRegionByID(bo, regionID)
			if err != nil {
				// ignore error and use old region info.
				logutil.Logger(bo.ctx).Error("load region failure",
					zap.Uint64("regionID", regionID), zap.Error(err))
			} else {
				r = lr
				c.mu.Lock()
				c.insertRegionToCache(r)
				c.mu.Unlock()
			}
		}
		loc := &KeyLocation{
			Region:   r.VerID(),
			StartKey: r.StartKey(),
			EndKey:   r.EndKey(),
		}
		return loc, nil
	}

	r, err := c.loadRegionByID(bo, regionID)
	if err != nil {
		return nil, errors.Trace(err)
	}

	c.mu.Lock()
	c.insertRegionToCache(r)
	c.mu.Unlock()
	return &KeyLocation{
		Region:   r.VerID(),
		StartKey: r.StartKey(),
		EndKey:   r.EndKey(),
	}, nil
}

// GroupKeysByRegion separates keys into groups by their belonging Regions.
// Specially it also returns the first key's region which may be used as the
// 'PrimaryLockKey' and should be committed ahead of others.
// filter is used to filter some unwanted keys.
func (c *RegionCache) GroupKeysByRegion(bo *Backoffer, keys [][]byte, filter func(key, regionStartKey []byte) bool) (map[RegionVerID][][]byte, RegionVerID, error) {
	groups := make(map[RegionVerID][][]byte)
	var first RegionVerID
	var lastLoc *KeyLocation
	for i, k := range keys {
		if lastLoc == nil || !lastLoc.Contains(k) {
			var err error
			lastLoc, err = c.LocateKey(bo, k)
			if err != nil {
				return nil, first, errors.Trace(err)
			}
			if filter != nil && filter(k, lastLoc.StartKey) {
				continue
			}
		}
		id := lastLoc.Region
		if i == 0 {
			first = id
		}
		groups[id] = append(groups[id], k)
	}
	return groups, first, nil
}

type groupedMutations struct {
	region    RegionVerID
	mutations CommitterMutations
}

// GroupSortedMutationsByRegion separates keys into groups by their belonging Regions.
func (c *RegionCache) GroupSortedMutationsByRegion(bo *Backoffer, m CommitterMutations) ([]groupedMutations, error) {
	var (
		groups  []groupedMutations
		lastLoc *KeyLocation
	)
	lastUpperBound := 0
	for i := range m.keys {
		if lastLoc == nil || !lastLoc.Contains(m.keys[i]) {
			if lastLoc != nil {
				groups = append(groups, groupedMutations{
					region:    lastLoc.Region,
					mutations: m.subRange(lastUpperBound, i),
				})
				lastUpperBound = i
			}
			var err error
			lastLoc, err = c.LocateKey(bo, m.keys[i])
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
	}
	if lastLoc != nil {
		groups = append(groups, groupedMutations{
			region:    lastLoc.Region,
			mutations: m.subRange(lastUpperBound, m.len()),
		})
	}
	return groups, nil
}

// ListRegionIDsInKeyRange lists ids of regions in [start_key,end_key].
func (c *RegionCache) ListRegionIDsInKeyRange(bo *Backoffer, startKey, endKey []byte) (regionIDs []uint64, err error) {
	for {
		curRegion, err := c.LocateKey(bo, startKey)
		if err != nil {
			return nil, errors.Trace(err)
		}
		regionIDs = append(regionIDs, curRegion.Region.id)
		if curRegion.Contains(endKey) {
			break
		}
		startKey = curRegion.EndKey
	}
	return regionIDs, nil
}

// LoadRegionsInKeyRange lists regions in [start_key,end_key].
func (c *RegionCache) LoadRegionsInKeyRange(bo *Backoffer, startKey, endKey []byte) (regions []*Region, err error) {
	var batchRegions []*Region
	for {
		batchRegions, err = c.BatchLoadRegionsWithKeyRange(bo, startKey, endKey, defaultRegionsPerBatch)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if len(batchRegions) == 0 {
			// should never happen
			break
		}
		regions = append(regions, batchRegions...)
		endRegion := batchRegions[len(batchRegions)-1]
		if endRegion.Contains(endKey) {
			break
		}
		startKey = endRegion.EndKey()
	}
	return
}

// BatchLoadRegionsWithKeyRange loads at most given numbers of regions to the RegionCache,
// within the given key range from the startKey to endKey. Returns the loaded regions.
func (c *RegionCache) BatchLoadRegionsWithKeyRange(bo *Backoffer, startKey []byte, endKey []byte, count int) (regions []*Region, err error) {
	regions, err = c.scanRegions(bo, startKey, endKey, count)
	if err != nil {
		return
	}
	if len(regions) == 0 {
		err = errors.New("PD returned no region")
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, region := range regions {
		c.insertRegionToCache(region)
	}

	return
}

// BatchLoadRegionsFromKey loads at most given numbers of regions to the RegionCache, from the given startKey. Returns
// the endKey of the last loaded region. If some of the regions has no leader, their entries in RegionCache will not be
// updated.
func (c *RegionCache) BatchLoadRegionsFromKey(bo *Backoffer, startKey []byte, count int) ([]byte, error) {
	regions, err := c.BatchLoadRegionsWithKeyRange(bo, startKey, nil, count)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return regions[len(regions)-1].EndKey(), nil
}

// InvalidateCachedRegion removes a cached Region.
func (c *RegionCache) InvalidateCachedRegion(id RegionVerID) {
	cachedRegion := c.getCachedRegionWithRLock(id)
	if cachedRegion == nil {
		return
	}
	cachedRegion.invalidate()
}

// UpdateLeader update some region cache with newer leader info.
func (c *RegionCache) UpdateLeader(regionID RegionVerID, leaderStoreID uint64, currentPeerIdx AccessIndex) {
	r := c.getCachedRegionWithRLock(regionID)
	if r == nil {
		logutil.BgLogger().Debug("regionCache: cannot find region when updating leader",
			zap.Uint64("regionID", regionID.GetID()),
			zap.Uint64("leaderStoreID", leaderStoreID))
		return
	}

	if leaderStoreID == 0 {
		rs := r.getStore()
		rs.switchNextTiKVPeer(r, currentPeerIdx)
		logutil.BgLogger().Info("switch region peer to next due to NotLeader with NULL leader",
			zap.Int("currIdx", int(currentPeerIdx)),
			zap.Uint64("regionID", regionID.GetID()))
		return
	}

	if !c.switchWorkLeaderToPeer(r, leaderStoreID) {
		logutil.BgLogger().Info("invalidate region cache due to cannot find peer when updating leader",
			zap.Uint64("regionID", regionID.GetID()),
			zap.Int("currIdx", int(currentPeerIdx)),
			zap.Uint64("leaderStoreID", leaderStoreID))
		r.invalidate()
	} else {
		logutil.BgLogger().Info("switch region leader to specific leader due to kv return NotLeader",
			zap.Uint64("regionID", regionID.GetID()),
			zap.Int("currIdx", int(currentPeerIdx)),
			zap.Uint64("leaderStoreID", leaderStoreID))
	}
}

// insertRegionToCache tries to insert the Region to cache.
func (c *RegionCache) insertRegionToCache(cachedRegion *Region) {
	old := c.mu.sorted.ReplaceOrInsert(newBtreeItem(cachedRegion))
	if old != nil {
		// Don't refresh TiFlash work idx for region. Otherwise, it will always goto a invalid store which
		// is under transferring regions.
		cachedRegion.getStore().workTiFlashIdx = old.(*btreeItem).cachedRegion.getStore().workTiFlashIdx
		delete(c.mu.regions, old.(*btreeItem).cachedRegion.VerID())
	}
	c.mu.regions[cachedRegion.VerID()] = cachedRegion
}

// searchCachedRegion finds a region from cache by key. Like `getCachedRegion`,
// it should be called with c.mu.RLock(), and the returned Region should not be
// used after c.mu is RUnlock().
// If the given key is the end key of the region that you want, you may set the second argument to true. This is useful
// when processing in reverse order.
func (c *RegionCache) searchCachedRegion(key []byte, isEndKey bool) *Region {
	ts := time.Now().Unix()
	var r *Region
	c.mu.RLock()
	c.mu.sorted.DescendLessOrEqual(newBtreeSearchItem(key), func(item btree.Item) bool {
		r = item.(*btreeItem).cachedRegion
		if isEndKey && bytes.Equal(r.StartKey(), key) {
			r = nil     // clear result
			return true // iterate next item
		}
		if !r.checkRegionCacheTTL(ts) {
			r = nil
			return true
		}
		return false
	})
	c.mu.RUnlock()
	if r != nil && (!isEndKey && r.Contains(key) || isEndKey && r.ContainsByEnd(key)) {
		return r
	}
	return nil
}

// getRegionByIDFromCache tries to get region by regionID from cache. Like
// `getCachedRegion`, it should be called with c.mu.RLock(), and the returned
// Region should not be used after c.mu is RUnlock().
func (c *RegionCache) getRegionByIDFromCache(regionID uint64) *Region {
	var newestRegion *Region
	ts := time.Now().Unix()
	for v, r := range c.mu.regions {
		if v.id == regionID {
			lastAccess := atomic.LoadInt64(&r.lastAccess)
			if ts-lastAccess > RegionCacheTTLSec {
				continue
			}
			if newestRegion == nil {
				newestRegion = r
				continue
			}
			nv := newestRegion.VerID()
			cv := r.VerID()
			if nv.GetConfVer() < cv.GetConfVer() {
				newestRegion = r
				continue
			}
			if nv.GetVer() < cv.GetVer() {
				newestRegion = r
				continue
			}
		}
	}
	if newestRegion != nil {
		atomic.CompareAndSwapInt64(&newestRegion.lastAccess, atomic.LoadInt64(&newestRegion.lastAccess), ts)
	}
	return newestRegion
}

func filterUnavailablePeers(region *pd.Region) {
	if len(region.DownPeers) == 0 {
		return
	}
	new := region.Meta.Peers[:0]
	for _, p := range region.Meta.Peers {
		available := true
		for _, downPeer := range region.DownPeers {
			if p.Id == downPeer.Id && p.StoreId == downPeer.StoreId {
				available = false
				break
			}
		}
		if available {
			new = append(new, p)
		}
	}
	for i := len(new); i < len(region.Meta.Peers); i++ {
		region.Meta.Peers[i] = nil
	}
	region.Meta.Peers = new
}

// loadRegion loads region from pd client, and picks the first peer as leader.
// If the given key is the end key of the region that you want, you may set the second argument to true. This is useful
// when processing in reverse order.
func (c *RegionCache) loadRegion(bo *Backoffer, key []byte, isEndKey bool) (*Region, error) {
	var backoffErr error
	searchPrev := false
	for {
		if backoffErr != nil {
			err := bo.Backoff(BoPDRPC, backoffErr)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		var reg *pd.Region
		var err error
		if searchPrev {
			reg, err = c.pdClient.GetPrevRegion(bo.ctx, key)
		} else {
			reg, err = c.pdClient.GetRegion(bo.ctx, key)
		}
		if err != nil {
			tikvRegionCacheCounterWithGetRegionError.Inc()
		} else {
			tikvRegionCacheCounterWithGetRegionOK.Inc()
		}
		if err != nil {
			backoffErr = errors.Errorf("loadRegion from PD failed, key: %q, err: %v", key, err)
			continue
		}
		if reg == nil || reg.Meta == nil {
			backoffErr = errors.Errorf("region not found for key %q", key)
			continue
		}
		filterUnavailablePeers(reg)
		if len(reg.Meta.Peers) == 0 {
			return nil, errors.New("receive Region with no available peer")
		}
		if isEndKey && !searchPrev && bytes.Equal(reg.Meta.StartKey, key) && len(reg.Meta.StartKey) != 0 {
			searchPrev = true
			continue
		}
		region := &Region{meta: reg.Meta}
		err = region.init(c)
		if err != nil {
			return nil, err
		}
		if reg.Leader != nil {
			c.switchWorkLeaderToPeer(region, reg.Leader.StoreId)
		}
		return region, nil
	}
}

// loadRegionByID loads region from pd client, and picks the first peer as leader.
func (c *RegionCache) loadRegionByID(bo *Backoffer, regionID uint64) (*Region, error) {
	var backoffErr error
	for {
		if backoffErr != nil {
			err := bo.Backoff(BoPDRPC, backoffErr)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		reg, err := c.pdClient.GetRegionByID(bo.ctx, regionID)
		if err != nil {
			tikvRegionCacheCounterWithGetRegionByIDError.Inc()
		} else {
			tikvRegionCacheCounterWithGetRegionByIDOK.Inc()
		}
		if err != nil {
			backoffErr = errors.Errorf("loadRegion from PD failed, regionID: %v, err: %v", regionID, err)
			continue
		}
		if reg == nil || reg.Meta == nil {
			return nil, errors.Errorf("region not found for regionID %d", regionID)
		}
		filterUnavailablePeers(reg)
		if len(reg.Meta.Peers) == 0 {
			return nil, errors.New("receive Region with no available peer")
		}
		region := &Region{meta: reg.Meta}
		err = region.init(c)
		if err != nil {
			return nil, err
		}
		if reg.Leader != nil {
			c.switchWorkLeaderToPeer(region, reg.Leader.GetStoreId())
		}
		return region, nil
	}
}

// scanRegions scans at most `limit` regions from PD, starts from the region containing `startKey` and in key order.
// Regions with no leader will not be returned.
func (c *RegionCache) scanRegions(bo *Backoffer, startKey, endKey []byte, limit int) ([]*Region, error) {
	if limit == 0 {
		return nil, nil
	}

	var backoffErr error
	for {
		if backoffErr != nil {
			err := bo.Backoff(BoPDRPC, backoffErr)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		regionsInfo, err := c.pdClient.ScanRegions(bo.ctx, startKey, endKey, limit)
		if err != nil {
			tikvRegionCacheCounterWithScanRegionsError.Inc()
			backoffErr = errors.Errorf(
				"scanRegion from PD failed, startKey: %q, limit: %q, err: %v",
				startKey,
				limit,
				err)
			continue
		}

		tikvRegionCacheCounterWithScanRegionsOK.Inc()

		if len(regionsInfo) == 0 {
			return nil, errors.New("PD returned no region")
		}
		regions := make([]*Region, 0, len(regionsInfo))
		for _, r := range regionsInfo {
			region := &Region{meta: r.Meta}
			err := region.init(c)
			if err != nil {
				return nil, err
			}
			leader := r.Leader
			// Leader id = 0 indicates no leader.
			if leader.GetId() != 0 {
				c.switchWorkLeaderToPeer(region, leader.GetStoreId())
				regions = append(regions, region)
			}
		}
		if len(regions) == 0 {
			return nil, errors.New("receive Regions with no peer")
		}
		if len(regions) < len(regionsInfo) {
			logutil.Logger(context.Background()).Debug(
				"regionCache: scanRegion finished but some regions has no leader.")
		}
		return regions, nil
	}
}

func (c *RegionCache) getCachedRegionWithRLock(regionID RegionVerID) (r *Region) {
	c.mu.RLock()
	r = c.mu.regions[regionID]
	c.mu.RUnlock()
	return
}

func (c *RegionCache) getStoreAddr(bo *Backoffer, region *Region, store *Store, storeIdx int) (addr string, err error) {
	state := store.getResolveState()
	switch state {
	case resolved, needCheck:
		addr = store.addr
		return
	case unresolved:
		addr, err = store.initResolve(bo, c)
		return
	case deleted:
		addr = c.changeToActiveStore(region, store, storeIdx)
		return
	default:
		panic("unsupported resolve state")
	}
}

func (c *RegionCache) changeToActiveStore(region *Region, store *Store, storeIdx int) (addr string) {
	c.storeMu.RLock()
	store = c.storeMu.stores[store.storeID]
	c.storeMu.RUnlock()
	for {
		oldRegionStore := region.getStore()
		newRegionStore := oldRegionStore.clone()
		newRegionStore.stores = make([]*Store, 0, len(oldRegionStore.stores))
		for i, s := range oldRegionStore.stores {
			if i == storeIdx {
				newRegionStore.stores = append(newRegionStore.stores, store)
			} else {
				newRegionStore.stores = append(newRegionStore.stores, s)
			}
		}
		if region.compareAndSwapStore(oldRegionStore, newRegionStore) {
			break
		}
	}
	addr = store.addr
	return
}

func (c *RegionCache) getStoreByStoreID(storeID uint64) (store *Store) {
	var ok bool
	c.storeMu.Lock()
	store, ok = c.storeMu.stores[storeID]
	if ok {
		c.storeMu.Unlock()
		return
	}
	store = &Store{storeID: storeID}
	c.storeMu.stores[storeID] = store
	c.storeMu.Unlock()
	return
}

// OnRegionEpochNotMatch removes the old region and inserts new regions into the cache.
func (c *RegionCache) OnRegionEpochNotMatch(bo *Backoffer, ctx *RPCContext, currentRegions []*metapb.Region) error {
	// Find whether the region epoch in `ctx` is ahead of TiKV's. If so, backoff.
	for _, meta := range currentRegions {
		if meta.GetId() == ctx.Region.id &&
			(meta.GetRegionEpoch().GetConfVer() < ctx.Region.confVer ||
				meta.GetRegionEpoch().GetVersion() < ctx.Region.ver) {
			err := errors.Errorf("region epoch is ahead of tikv. rpc ctx: %+v, currentRegions: %+v", ctx, currentRegions)
			logutil.BgLogger().Info("region epoch is ahead of tikv", zap.Error(err))
			return bo.Backoff(BoRegionMiss, err)
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	needInvalidateOld := true
	// If the region epoch is not ahead of TiKV's, replace region meta in region cache.
	for _, meta := range currentRegions {
		if _, ok := c.pdClient.(*codecPDClient); ok {
			var err error
			if meta, err = decodeRegionMetaKeyWithShallowCopy(meta); err != nil {
				return errors.Errorf("newRegion's range key is not encoded: %v, %v", meta, err)
			}
		}
		region := &Region{meta: meta}
		err := region.init(c)
		if err != nil {
			return err
		}
		var initLeader uint64
		if ctx.Store.storeType == kv.TiFlash {
			initLeader = region.findElectableStoreID()
		} else {
			initLeader = ctx.Store.storeID
		}
		c.switchWorkLeaderToPeer(region, initLeader)
		c.insertRegionToCache(region)
		if ctx.Region == region.VerID() {
			needInvalidateOld = false
		}
	}
	if needInvalidateOld {
		cachedRegion, ok := c.mu.regions[ctx.Region]
		if ok {
			cachedRegion.invalidate()
		}
	}
	return nil
}

// PDClient returns the pd.Client in RegionCache.
func (c *RegionCache) PDClient() pd.Client {
	return c.pdClient
}

// btreeItem is BTree's Item that uses []byte to compare.
type btreeItem struct {
	key          []byte
	cachedRegion *Region
}

func newBtreeItem(cr *Region) *btreeItem {
	return &btreeItem{
		key:          cr.StartKey(),
		cachedRegion: cr,
	}
}

func newBtreeSearchItem(key []byte) *btreeItem {
	return &btreeItem{
		key: key,
	}
}

func (item *btreeItem) Less(other btree.Item) bool {
	return bytes.Compare(item.key, other.(*btreeItem).key) < 0
}

// GetID returns id.
func (r *Region) GetID() uint64 {
	return r.meta.GetId()
}

// GetMeta returns region meta.
func (r *Region) GetMeta() *metapb.Region {
	return proto.Clone(r.meta).(*metapb.Region)
}

// GetLeaderPeerID returns leader peer ID.
func (r *Region) GetLeaderPeerID() uint64 {
	store := r.getStore()
	if int(store.workTiKVIdx) >= len(r.meta.Peers) {
		return 0
	}
	storeIdx, _ := store.accessStore(TiKvOnly, store.workTiKVIdx)
	return r.meta.Peers[storeIdx].Id
}

// GetLeaderStoreID returns the store ID of the leader region.
func (r *Region) GetLeaderStoreID() uint64 {
	store := r.getStore()
	if int(store.workTiKVIdx) >= len(r.meta.Peers) {
		return 0
	}
	storeIdx, _ := store.accessStore(TiKvOnly, store.workTiKVIdx)
	return r.meta.Peers[storeIdx].StoreId
}

func (r *Region) getKvStorePeer(rs *RegionStore, aidx AccessIndex) (store *Store, peer *metapb.Peer, accessIdx AccessIndex, storeIdx int) {
	storeIdx, store = rs.accessStore(TiKvOnly, aidx)
	peer = r.meta.Peers[storeIdx]
	accessIdx = aidx
	return
}

// WorkStorePeer returns current work store with work peer.
func (r *Region) WorkStorePeer(rs *RegionStore) (store *Store, peer *metapb.Peer, accessIdx AccessIndex, storeIdx int) {
	return r.getKvStorePeer(rs, rs.workTiKVIdx)
}

// FollowerStorePeer returns a follower store with follower peer.
func (r *Region) FollowerStorePeer(rs *RegionStore, followerStoreSeed uint32) (store *Store, peer *metapb.Peer, accessIdx AccessIndex, storeIdx int) {
	return r.getKvStorePeer(rs, rs.follower(followerStoreSeed))
}

// AnyStorePeer returns a leader or follower store with the associated peer.
func (r *Region) AnyStorePeer(rs *RegionStore, followerStoreSeed uint32) (store *Store, peer *metapb.Peer, accessIdx AccessIndex, storeIdx int) {
	return r.getKvStorePeer(rs, rs.kvPeer(followerStoreSeed))
}

// RegionVerID is a unique ID that can identify a Region at a specific version.
type RegionVerID struct {
	id      uint64
	confVer uint64
	ver     uint64
}

// GetID returns the id of the region
func (r *RegionVerID) GetID() uint64 {
	return r.id
}

// GetVer returns the version of the region's epoch
func (r *RegionVerID) GetVer() uint64 {
	return r.ver
}

// GetConfVer returns the conf ver of the region's epoch
func (r *RegionVerID) GetConfVer() uint64 {
	return r.confVer
}

// VerID returns the Region's RegionVerID.
func (r *Region) VerID() RegionVerID {
	return RegionVerID{
		id:      r.meta.GetId(),
		confVer: r.meta.GetRegionEpoch().GetConfVer(),
		ver:     r.meta.GetRegionEpoch().GetVersion(),
	}
}

// StartKey returns StartKey.
func (r *Region) StartKey() []byte {
	return r.meta.StartKey
}

// EndKey returns EndKey.
func (r *Region) EndKey() []byte {
	return r.meta.EndKey
}

// switchWorkLeaderToPeer switches current store to the one on specific store. It returns
// false if no peer matches the storeID.
func (c *RegionCache) switchWorkLeaderToPeer(r *Region, targetStoreID uint64) (found bool) {
	globalStoreIdx, found := c.getPeerStoreIndex(r, targetStoreID)
retry:
	// switch to new leader.
	oldRegionStore := r.getStore()
	var leaderIdx AccessIndex
	for i, gIdx := range oldRegionStore.accessIndex[TiKvOnly] {
		if gIdx == globalStoreIdx {
			leaderIdx = AccessIndex(i)
		}
	}
	if oldRegionStore.workTiKVIdx == leaderIdx {
		return
	}
	newRegionStore := oldRegionStore.clone()
	newRegionStore.workTiKVIdx = leaderIdx
	if !r.compareAndSwapStore(oldRegionStore, newRegionStore) {
		goto retry
	}
	return
}

func (r *RegionStore) switchNextFlashPeer(rr *Region, currentPeerIdx AccessIndex) {
	nextIdx := (currentPeerIdx + 1) % AccessIndex(r.accessStoreNum(TiFlashOnly))
	newRegionStore := r.clone()
	newRegionStore.workTiFlashIdx = int32(nextIdx)
	rr.compareAndSwapStore(r, newRegionStore)
}

func (r *RegionStore) switchNextTiKVPeer(rr *Region, currentPeerIdx AccessIndex) {
	if r.workTiKVIdx != currentPeerIdx {
		return
	}
	nextIdx := (currentPeerIdx + 1) % AccessIndex(r.accessStoreNum(TiKvOnly))
	newRegionStore := r.clone()
	newRegionStore.workTiKVIdx = nextIdx
	rr.compareAndSwapStore(r, newRegionStore)
}

func (r *Region) findElectableStoreID() uint64 {
	if len(r.meta.Peers) == 0 {
		return 0
	}
	for _, p := range r.meta.Peers {
		if !p.IsLearner {
			return p.StoreId
		}
	}
	return 0
}

func (c *RegionCache) getPeerStoreIndex(r *Region, id uint64) (idx int, found bool) {
	if len(r.meta.Peers) == 0 {
		return
	}
	for i, p := range r.meta.Peers {
		if p.GetStoreId() == id {
			idx = i
			found = true
			return
		}
	}
	return
}

// Contains checks whether the key is in the region, for the maximum region endKey is empty.
// startKey <= key < endKey.
func (r *Region) Contains(key []byte) bool {
	return bytes.Compare(r.meta.GetStartKey(), key) <= 0 &&
		(bytes.Compare(key, r.meta.GetEndKey()) < 0 || len(r.meta.GetEndKey()) == 0)
}

// ContainsByEnd check the region contains the greatest key that is less than key.
// for the maximum region endKey is empty.
// startKey < key <= endKey.
func (r *Region) ContainsByEnd(key []byte) bool {
	return bytes.Compare(r.meta.GetStartKey(), key) < 0 &&
		(bytes.Compare(key, r.meta.GetEndKey()) <= 0 || len(r.meta.GetEndKey()) == 0)
}

// Store contains a kv process's address.
type Store struct {
	addr         string        // loaded store address
	saddr        string        // loaded store status address
	storeID      uint64        // store's id
	state        uint64        // unsafe store storeState
	resolveMutex sync.Mutex    // protect pd from concurrent init requests
	epoch        uint32        // store fail epoch, see RegionStore.storeEpochs
	storeType    kv.StoreType  // type of the store
	tokenCount   atomic2.Int64 // used store token count
}

type resolveState uint64

const (
	unresolved resolveState = iota
	resolved
	needCheck
	deleted
)

// initResolve resolves addr for store that never resolved.
func (s *Store) initResolve(bo *Backoffer, c *RegionCache) (addr string, err error) {
	s.resolveMutex.Lock()
	state := s.getResolveState()
	defer s.resolveMutex.Unlock()
	if state != unresolved {
		addr = s.addr
		return
	}
	var store *metapb.Store
	for {
		store, err = c.pdClient.GetStore(bo.ctx, s.storeID)
		if err != nil {
			tikvRegionCacheCounterWithGetStoreError.Inc()
		} else {
			tikvRegionCacheCounterWithGetStoreOK.Inc()
		}
		if err != nil {
			// TODO: more refine PD error status handle.
			if errors.Cause(err) == context.Canceled {
				return
			}
			err = errors.Errorf("loadStore from PD failed, id: %d, err: %v", s.storeID, err)
			if err = bo.Backoff(BoPDRPC, err); err != nil {
				return
			}
			continue
		}
		if store == nil {
			return
		}
		addr = store.GetAddress()
		s.addr = addr
		s.saddr = store.GetStatusAddress()
		s.storeType = GetStoreTypeByMeta(store)
	retry:
		state = s.getResolveState()
		if state != unresolved {
			addr = s.addr
			return
		}
		if !s.compareAndSwapState(state, resolved) {
			goto retry
		}
		return
	}
}

// GetStoreTypeByMeta gets store type by store meta pb.
func GetStoreTypeByMeta(store *metapb.Store) kv.StoreType {
	tp := kv.TiKV
	for _, label := range store.Labels {
		if label.Key == "engine" {
			if label.Value == kv.TiFlash.Name() {
				tp = kv.TiFlash
			}
			break
		}
	}
	return tp
}

// reResolve try to resolve addr for store that need check.
func (s *Store) reResolve(c *RegionCache) {
	var addr string
	store, err := c.pdClient.GetStore(context.Background(), s.storeID)
	if err != nil {
		tikvRegionCacheCounterWithGetStoreError.Inc()
	} else {
		tikvRegionCacheCounterWithGetStoreOK.Inc()
	}
	if err != nil {
		logutil.BgLogger().Error("loadStore from PD failed", zap.Uint64("id", s.storeID), zap.Error(err))
		// we cannot do backoff in reResolve loop but try check other store and wait tick.
		return
	}
	if store == nil {
		// store has be removed in PD, we should invalidate all regions using those store.
		logutil.BgLogger().Info("invalidate regions in removed store",
			zap.Uint64("store", s.storeID), zap.String("add", s.addr))
		atomic.AddUint32(&s.epoch, 1)
		tikvRegionCacheCounterWithInvalidateStoreRegionsOK.Inc()
		return
	}

	storeType := GetStoreTypeByMeta(store)
	addr = store.GetAddress()
	if s.addr != addr {
		state := resolved
		newStore := &Store{storeID: s.storeID, addr: addr, saddr: store.GetStatusAddress(), storeType: storeType}
		newStore.state = *(*uint64)(&state)
		c.storeMu.Lock()
		c.storeMu.stores[newStore.storeID] = newStore
		c.storeMu.Unlock()
	retryMarkDel:
		// all region used those
		oldState := s.getResolveState()
		if oldState == deleted {
			return
		}
		newState := deleted
		if !s.compareAndSwapState(oldState, newState) {
			goto retryMarkDel
		}
		return
	}
retryMarkResolved:
	oldState := s.getResolveState()
	if oldState != needCheck {
		return
	}
	newState := resolved
	if !s.compareAndSwapState(oldState, newState) {
		goto retryMarkResolved
	}
}

func (s *Store) getResolveState() resolveState {
	var state resolveState
	if s == nil {
		return state
	}
	return resolveState(atomic.LoadUint64(&s.state))
}

func (s *Store) compareAndSwapState(oldState, newState resolveState) bool {
	return atomic.CompareAndSwapUint64(&s.state, uint64(oldState), uint64(newState))
}

// markNeedCheck marks resolved store to be async resolve to check store addr change.
func (s *Store) markNeedCheck(notifyCheckCh chan struct{}) {
retry:
	oldState := s.getResolveState()
	if oldState != resolved {
		return
	}
	if !s.compareAndSwapState(oldState, needCheck) {
		goto retry
	}
	select {
	case notifyCheckCh <- struct{}{}:
	default:
	}

}

type livenessState uint32

var (
	livenessSf singleflight.Group
	// StoreLivenessTimeout is the max duration of resolving liveness of a TiKV instance.
	StoreLivenessTimeout time.Duration
)

const (
	unknown livenessState = iota
	reachable
	unreachable
	offline
)

func (s *Store) requestLiveness(bo *Backoffer) (l livenessState) {
	if StoreLivenessTimeout == 0 {
		return unreachable
	}

	saddr := s.saddr
	if len(saddr) == 0 {
		l = unknown
		return
	}
	rsCh := livenessSf.DoChan(saddr, func() (interface{}, error) {
		return invokeKVStatusAPI(saddr, StoreLivenessTimeout), nil
	})
	var ctx context.Context
	if bo != nil {
		ctx = bo.ctx
	} else {
		ctx = context.Background()
	}
	select {
	case rs := <-rsCh:
		l = rs.Val.(livenessState)
	case <-ctx.Done():
		l = unknown
		return
	}
	return
}

func invokeKVStatusAPI(saddr string, timeout time.Duration) (l livenessState) {
	start := time.Now()
	defer func() {
		if l == reachable {
			tikvStatusCountWithOK.Inc()
		} else {
			tikvStatusCountWithError.Inc()
		}
		metrics.TiKVStatusDuration.WithLabelValues(saddr).Observe(time.Since(start).Seconds())
	}()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	url := fmt.Sprintf("%s://%s/status", util.InternalHTTPSchema(), saddr)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		logutil.BgLogger().Info("[liveness] build kv status request fail", zap.String("store", saddr), zap.Error(err))
		l = unreachable
		return
	}
	resp, err := util.InternalHTTPClient().Do(req)
	if err != nil {
		logutil.BgLogger().Info("[liveness] request kv status fail", zap.String("store", saddr), zap.Error(err))
		l = unreachable
		return
	}
	defer func() {
		err1 := resp.Body.Close()
		if err1 != nil {
			logutil.BgLogger().Debug("[liveness] close kv status api body failed", zap.String("store", saddr), zap.Error(err))
		}
	}()
	if resp.StatusCode != http.StatusOK {
		logutil.BgLogger().Info("[liveness] request kv status fail", zap.String("store", saddr), zap.String("status", resp.Status))
		l = unreachable
		return
	}
	l = reachable
	return
}
