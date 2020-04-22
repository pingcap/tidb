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
	"strconv"
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
	"github.com/pingcap/tidb/config"
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

// RegionStore represents region stores info
// it will be store as unsafe.Pointer and be load at once
type RegionStore struct {
	workTiKVIdx    int32    // point to current work peer in meta.Peers and work store in stores(same idx) for tikv peer
	workTiFlashIdx int32    // point to current work peer in meta.Peers and work store in stores(same idx) for tiflash peer
	stores         []*Store // stores in this region
	storeEpochs    []uint32 // snapshots of store's fail, need reload when `storeEpochs[curr] != stores[cur].fail`
}

// clone clones region store struct.
func (rs *RegionStore) clone() *RegionStore {
	storeFails := make([]uint32, len(rs.stores))
	copy(storeFails, rs.storeEpochs)
	return &RegionStore{
		workTiFlashIdx: rs.workTiFlashIdx,
		workTiKVIdx:    rs.workTiKVIdx,
		stores:         rs.stores,
		storeEpochs:    storeFails,
	}
}

// return next follower store's index
func (rs *RegionStore) follower(seed uint32) int32 {
	l := uint32(len(rs.stores))
	if l <= 1 {
		return rs.workTiKVIdx
	}

	for retry := l - 1; retry > 0; retry-- {
		followerIdx := int32(seed % (l - 1))
		if followerIdx >= rs.workTiKVIdx {
			followerIdx++
		}
		if rs.stores[followerIdx].storeType != kv.TiKV {
			continue
		}
		if rs.storeEpochs[followerIdx] == rs.stores[followerIdx].currentEpoch() {
			return followerIdx
		}
		seed++
	}
	return rs.workTiKVIdx
}

// return next leader or follower store's index
func (rs *RegionStore) peer(seed uint32) int32 {
	candidates := make([]int32, 0, len(rs.stores))
	for i := 0; i < len(rs.stores); i++ {
		if rs.stores[i].storeType != kv.TiKV {
			continue
		}
		if rs.storeEpochs[i] != rs.stores[i].currentEpoch() {
			continue
		}
		candidates = append(candidates, int32(i))
	}

	if len(candidates) == 0 {
		return rs.workTiKVIdx
	}
	return candidates[int32(seed)%int32(len(candidates))]
}

// init initializes region after constructed.
func (r *Region) init(c *RegionCache) {
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
		rs.stores = append(rs.stores, store)
		rs.storeEpochs = append(rs.storeEpochs, store.currentEpoch())
	}
	atomic.StorePointer(&r.store, unsafe.Pointer(rs))

	// mark region has been init accessed.
	r.lastAccess = time.Now().Unix()
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
	closeCh chan struct{}
}

// NewRegionCache creates a RegionCache.
func NewRegionCache(pdClient pd.Client) *RegionCache {
	c := &RegionCache{
		pdClient: pdClient,
	}
	c.mu.regions = make(map[RegionVerID]*Region)
	c.mu.sorted = btree.New(btreeDegree)
	c.storeMu.stores = make(map[uint64]*Store)
	c.closeCh = make(chan struct{})
	return c
}

// Close releases region cache's resource.
func (c *RegionCache) Close() {
	close(c.closeCh)
}

// RPCContext contains data that is needed to send RPC to a region.
type RPCContext struct {
	Region  RegionVerID
	Meta    *metapb.Region
	Peer    *metapb.Peer
	PeerIdx int
	Store   *Store
	Addr    string
}

func (c *RPCContext) String() string {
	return fmt.Sprintf("region ID: %d, meta: %s, peer: %s, addr: %s, idx: %d",
		c.Region.GetID(), c.Meta, c.Peer, c.Addr, c.PeerIdx)
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
	var store *Store
	var peer *metapb.Peer
	var storeIdx int
	switch replicaRead {
	case kv.ReplicaReadFollower:
		store, peer, storeIdx = cachedRegion.FollowerStorePeer(regionStore, followerStoreSeed)
	case kv.ReplicaReadMixed:
		store, peer, storeIdx = cachedRegion.AnyStorePeer(regionStore, followerStoreSeed)
	default:
		store, peer, storeIdx = cachedRegion.WorkStorePeer(regionStore)
	}
	addr, _, err := store.getAddr(bo, cachedRegion, storeIdx)
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

	if store.currentEpoch() != regionStore.storeEpochs[storeIdx] {
		cachedRegion.invalidate()
		logutil.BgLogger().Info("invalidate current region, because others request detect store was down",
			zap.Uint64("region", id.GetID()),
			zap.String("store", store.addr))
		return nil, nil
	}
	return &RPCContext{
		Region:  id,
		Meta:    cachedRegion.meta,
		Peer:    peer,
		PeerIdx: storeIdx,
		Store:   store,
		Addr:    addr,
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
	for i := range regionStore.stores {
		storeIdx := (sIdx + i) % len(regionStore.stores)
		store := regionStore.stores[storeIdx]
		addr, _, err := store.getAddr(bo, cachedRegion, storeIdx)
		if err != nil {
			return nil, err
		}
		if len(addr) == 0 {
			cachedRegion.invalidate()
			return nil, nil
		}
		if store.storeType != kv.TiFlash {
			continue
		}
		atomic.StoreInt32(&regionStore.workTiFlashIdx, int32(storeIdx))
		peer := cachedRegion.meta.Peers[storeIdx]
		if store.currentEpoch() != regionStore.storeEpochs[storeIdx] {
			cachedRegion.invalidate()
			logutil.BgLogger().Info("invalidate current region, because others request detect store was down",
				zap.Uint64("region", id.GetID()),
				zap.String("store", store.addr))
			return nil, nil
		}
		return &RPCContext{
			Region:  id,
			Meta:    cachedRegion.meta,
			Peer:    peer,
			PeerIdx: storeIdx,
			Store:   store,
			Addr:    addr,
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

// OnStoreDown handles send request fail and store down logic.
func (c *RegionCache) OnStoreDown(bo *Backoffer, ctx *RPCContext, scheduleReload bool, err error) {
	tikvRegionCacheCounterWithSendFail.Inc()
	r := c.getCachedRegionWithRLock(ctx.Region)
	if r != nil {
		rs := r.getStore()
		s := rs.stores[ctx.PeerIdx]

		if err != nil && s.accessible() {
			if s.requestLiveness(bo) != reachable {
				s.updateLivenessState(uint32(reachable), uint32(unreachable), "send fail and store unreachable", true)
			}
		}

		if ctx.Store.storeType == kv.TiKV {
			rs.switchNextPeer(r, ctx.PeerIdx)
		} else {
			rs.switchNextFlashPeer(r, ctx.PeerIdx)
		}
		if scheduleReload {
			r.scheduleReload()
		}
		logutil.Logger(bo.ctx).Info("switch region peer to next due to send request fail and store liveness is down",
			zap.Stringer("current", ctx),
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
	mutations committerMutations
}

// GroupSortedMutationsByRegion separates keys into groups by their belonging Regions.
func (c *RegionCache) GroupSortedMutationsByRegion(bo *Backoffer, m committerMutations) ([]groupedMutations, error) {
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
func (c *RegionCache) UpdateLeader(regionID RegionVerID, leaderStoreID uint64, currentPeerIdx int) {
	r := c.getCachedRegionWithRLock(regionID)
	if r == nil {
		logutil.BgLogger().Debug("regionCache: cannot find region when updating leader",
			zap.Uint64("regionID", regionID.GetID()),
			zap.Uint64("leaderStoreID", leaderStoreID))
		return
	}

	if leaderStoreID == 0 {
		r.getStore().switchNextPeer(r, currentPeerIdx)
		logutil.BgLogger().Info("switch region peer to next due to NotLeader with NULL leader",
			zap.Int("currIdx", currentPeerIdx),
			zap.Uint64("regionID", regionID.GetID()))
		return
	}

	if !c.switchToPeer(r, leaderStoreID) {
		logutil.BgLogger().Info("invalidate region cache due to cannot find peer when updating leader",
			zap.Uint64("regionID", regionID.GetID()),
			zap.Int("currIdx", currentPeerIdx),
			zap.Uint64("leaderStoreID", leaderStoreID))
		r.invalidate()
	} else {
		logutil.BgLogger().Info("switch region leader to specific leader due to kv return NotLeader",
			zap.Uint64("regionID", regionID.GetID()),
			zap.Int("currIdx", currentPeerIdx),
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
	for v, r := range c.mu.regions {
		if v.id == regionID {
			return r
		}
	}
	return nil
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
		var meta *metapb.Region
		var leader *metapb.Peer
		var err error
		if searchPrev {
			meta, leader, err = c.pdClient.GetPrevRegion(bo.ctx, key)
		} else {
			meta, leader, err = c.pdClient.GetRegion(bo.ctx, key)
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
		if meta == nil {
			backoffErr = errors.Errorf("region not found for key %q", key)
			continue
		}
		if len(meta.Peers) == 0 {
			return nil, errors.New("receive Region with no peer")
		}
		if isEndKey && !searchPrev && bytes.Equal(meta.StartKey, key) && len(meta.StartKey) != 0 {
			searchPrev = true
			continue
		}
		region := &Region{meta: meta}
		region.init(c)
		if leader != nil {
			c.switchToPeer(region, leader.StoreId)
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
		meta, leader, err := c.pdClient.GetRegionByID(bo.ctx, regionID)
		if err != nil {
			tikvRegionCacheCounterWithGetRegionByIDError.Inc()
		} else {
			tikvRegionCacheCounterWithGetRegionByIDOK.Inc()
		}
		if err != nil {
			backoffErr = errors.Errorf("loadRegion from PD failed, regionID: %v, err: %v", regionID, err)
			continue
		}
		if meta == nil {
			return nil, errors.Errorf("region not found for regionID %d", regionID)
		}
		if len(meta.Peers) == 0 {
			return nil, errors.New("receive Region with no peer")
		}
		region := &Region{meta: meta}
		region.init(c)
		if leader != nil {
			c.switchToPeer(region, leader.GetStoreId())
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
		metas, leaders, err := c.pdClient.ScanRegions(bo.ctx, startKey, endKey, limit)
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

		if len(metas) == 0 {
			return nil, errors.New("PD returned no region")
		}
		if len(metas) != len(leaders) {
			return nil, errors.New("PD returned mismatching region metas and leaders")
		}
		regions := make([]*Region, 0, len(metas))
		for i, meta := range metas {
			region := &Region{meta: meta}
			region.init(c)
			leader := leaders[i]
			// Leader id = 0 indicates no leader.
			if leader.GetId() != 0 {
				c.switchToPeer(region, leader.GetStoreId())
				regions = append(regions, region)
			}
		}
		if len(regions) == 0 {
			return nil, errors.New("receive Regions with no peer")
		}
		if len(regions) < len(metas) {
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

// getAddr takes the store's addr.
// it will init resolve / re-resolve addr / revise store referenced by region base on resolve state machine.
//
//              +-----------+
//              | unresolve |
//              +---+-------+
//                  |
//                  |initResolve()
//                  |
//                  |
//              +---v-------+
//              | resolved  |
//              +---+---^---+
//                  |   |                     +--------+
// markNeedResolve()|   |  doResolve()+------>+obsolete|
//                  |   |                     +--------+
//              +---v---+---+
//              | needCheck |
//              +-----------+
//
// - new store found by region view will be init as `unresolved`
// - first access request wil trigger `unresolved` store to resolve address and change to `resolved`
// - when store down or storeNotMatch will change `resolved` store as `needCheck`
// - first request access `needCheck` store will resolve addr again, then:
//    - invalidate region when re-resolve can not found that storeID in PD
//    - address changed will copy-on-write insert a new `resolved` region in store cache and mark old store as `obsolete`(region will refer old store at this time)
//    - address not change try to change status to `resolved` status if it's  in `needResolve`
// - first request access `obsolete` store will cas change current region's reference store to new `resolved` store from store cache
//
func (s *Store) getAddr(bo *Backoffer, region *Region, storeIdx int) (addr, saddr string, err error) {
	state := s.currentResolveState()
	switch state {
	case resolved:
		addr, saddr = s.addr, s.saddr
	case needCheck:
		err = s.doResolve(bo)
		addr, saddr = s.addr, s.saddr
	case unresolved:
		addr, saddr, err = s.initResolve(bo)
	case obsolete:
		if region != nil {
			addr, saddr = s.rc.changeToActiveStore(region, s, storeIdx)
		} else {
			s.rc.storeMu.RLock()
			ns := s.rc.storeMu.stores[s.storeID]
			s.rc.storeMu.RUnlock()
			addr, saddr = ns.addr, ns.saddr
		}
	default:
		panic("unsupported resolve state")
	}
	return
}

// changeToActiveStore replace region's referenced but obsolete store with new one.
func (c *RegionCache) changeToActiveStore(region *Region, store *Store, storeIdx int) (addr, saddr string) {
	c.storeMu.RLock()
	nstore := c.storeMu.stores[store.storeID]
	c.storeMu.RUnlock()
	for {
		oldRegionStore := region.getStore()
		newRegionStore := oldRegionStore.clone()
		newRegionStore.stores = make([]*Store, 0, len(oldRegionStore.stores))
		for i, s := range oldRegionStore.stores {
			if i == storeIdx {
				newRegionStore.stores = append(newRegionStore.stores, nstore)
			} else {
				newRegionStore.stores = append(newRegionStore.stores, s)
			}
		}
		if region.compareAndSwapStore(oldRegionStore, newRegionStore) {
			break
		}
	}
	addr, saddr = nstore.addr, nstore.saddr
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
	store = &Store{storeID: storeID, rc: c}
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
			if err := decodeRegionMetaKey(meta); err != nil {
				return errors.Errorf("newRegion's range key is not encoded: %v, %v", meta, err)
			}
		}
		region := &Region{meta: meta}
		region.init(c)
		c.switchToPeer(region, ctx.Store.storeID)
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

// GetLeaderID returns leader region ID.
func (r *Region) GetLeaderID() uint64 {
	store := r.getStore()
	if int(store.workTiKVIdx) >= len(r.meta.Peers) {
		return 0
	}
	return r.meta.Peers[int(r.getStore().workTiKVIdx)].Id
}

// GetLeaderStoreID returns the store ID of the leader region.
func (r *Region) GetLeaderStoreID() uint64 {
	store := r.getStore()
	if int(store.workTiKVIdx) >= len(r.meta.Peers) {
		return 0
	}
	return r.meta.Peers[int(r.getStore().workTiKVIdx)].StoreId
}

func (r *Region) getStorePeer(rs *RegionStore, pidx int32) (store *Store, peer *metapb.Peer, idx int) {
	store = rs.stores[pidx]
	peer = r.meta.Peers[pidx]
	idx = int(pidx)
	return
}

// WorkStorePeer returns current work store with work peer.
func (r *Region) WorkStorePeer(rs *RegionStore) (store *Store, peer *metapb.Peer, idx int) {
	return r.getStorePeer(rs, rs.workTiKVIdx)
}

// FollowerStorePeer returns a follower store with follower peer.
func (r *Region) FollowerStorePeer(rs *RegionStore, followerStoreSeed uint32) (*Store, *metapb.Peer, int) {
	return r.getStorePeer(rs, rs.follower(followerStoreSeed))
}

// AnyStorePeer returns a leader or follower store with the associated peer.
func (r *Region) AnyStorePeer(rs *RegionStore, followerStoreSeed uint32) (*Store, *metapb.Peer, int) {
	return r.getStorePeer(rs, rs.peer(followerStoreSeed))
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

// switchToPeer switches current store to the one on specific store. It returns
// false if no peer matches the storeID.
func (c *RegionCache) switchToPeer(r *Region, targetStoreID uint64) (found bool) {
	leaderIdx, found := c.getPeerStoreIndex(r, targetStoreID)
	c.switchWorkIdx(r, leaderIdx)
	return
}

func (rs *RegionStore) switchNextFlashPeer(r *Region, currentPeerIdx int) {
	nextIdx := (currentPeerIdx + 1) % len(rs.stores)
	newRegionStore := rs.clone()
	newRegionStore.workTiFlashIdx = int32(nextIdx)
	r.compareAndSwapStore(rs, newRegionStore)
}

func (rs *RegionStore) switchNextPeer(r *Region, currentPeerIdx int) {
	if int(rs.workTiKVIdx) != currentPeerIdx {
		return
	}

	nextIdx := (currentPeerIdx + 1) % len(rs.stores)
	for rs.stores[nextIdx].storeType == kv.TiFlash {
		nextIdx = (nextIdx + 1) % len(rs.stores)
	}
	newRegionStore := rs.clone()
	newRegionStore.workTiKVIdx = int32(nextIdx)
	r.compareAndSwapStore(rs, newRegionStore)
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

func (c *RegionCache) switchWorkIdx(r *Region, leaderIdx int) {
retry:
	// switch to new leader.
	oldRegionStore := r.getStore()
	if oldRegionStore.workTiKVIdx == int32(leaderIdx) {
		return
	}
	newRegionStore := oldRegionStore.clone()
	newRegionStore.workTiKVIdx = int32(leaderIdx)
	if !r.compareAndSwapStore(oldRegionStore, newRegionStore) {
		goto retry
	}
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

var (
	resolveSf, livenessSf singleflight.Group
)

// Store contains a kv process's type and addresses.
type Store struct {
	addr      string       // loaded store address
	saddr     string       // loaded store status address
	storeType kv.StoreType // type of the store
	storeID   uint64       // store's id
	rc        *RegionCache

	tokenCount atomic2.Int64 // used store token coun
	closed     chan struct{}

	resolveState uint64 // unsafe store storeState

	epoch         uint32 // store fail epoch, see RegionStore.storeEpochs
	livenessState uint32 // store liveness status
	liveness      struct {
		resetCh       chan struct{}
		checkInterval uint64
	}
}

// resolveState indicates store's resolve state in resolve state machine.
// see more detail in Store#getAddr's comments.
type resolveState uint64

const (
	unresolved resolveState = iota
	resolved
	needCheck
	obsolete
)

// initResolve init store's addr and change resolve status from unresolve to resolved.
func (s *Store) initResolve(bo *Backoffer) (addr, saddr string, err error) {
	state := s.currentResolveState()
	if state != unresolved {
		addr, saddr = s.addr, s.saddr
		return
	}
	err = s.requestResolveAddr(bo, func(store *metapb.Store) {
		if s.casResolveState(state, resolved) {
			s.addr = store.Address
			s.saddr = store.StatusAddress
			s.storeType = GetStoreTypeByMeta(store)
			s.initLiveness(normalLivenessCheckInterval)
			s.updateLivenessState(0, uint32(s.requestLiveness(bo)), "init store fail", true)
		}
	})
	if err != nil {
		return
	}
	addr, saddr = s.addr, s.saddr
	return
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

// doResolve resolves store's addr.
// it will COW change store's addr and mark old store as delete to notify regions reference old store to switch to new one.
func (s *Store) doResolve(bo *Backoffer) (err error) {
	err = s.requestResolveAddr(bo, func(store *metapb.Store) {
		if store == nil {
			// store has be removed in PD, we should invalidate all regions using those store.
			s.updateLivenessState(atomic.LoadUint32(&s.livenessState), uint32(offline), "resolve loop found store has be removed", true)
			return
		}
		storeType := GetStoreTypeByMeta(store)
		addr, saddr := store.GetAddress(), store.GetStatusAddress()
		s.storeType = GetStoreTypeByMeta(store)
		if s.addr != addr || s.saddr != saddr {
			// store addr has be changed.
			// copy-on-write a new resolved store and insert store cache.
			newStore := &Store{storeID: s.storeID, addr: addr, saddr: saddr, storeType: storeType, livenessState: uint32(reachable), rc: s.rc}
			state := resolved
			newStore.resolveState = *(*uint64)(unsafe.Pointer(&state))
			newStore.initLiveness(time.Duration(atomic.LoadUint64(&s.liveness.checkInterval)))
			newStore.updateLivenessState(0, uint32(s.requestLiveness(bo)), "re-resolve init", true)
			s.rc.storeMu.Lock()
			s.rc.storeMu.stores[newStore.storeID] = newStore
			s.rc.storeMu.Unlock()
		retryMarkDel:
			// mark old store as obsolete to notify region ref old store switch to new one, see RegionCache#changeToActiveStore
			oldState := s.currentResolveState()
			if oldState == obsolete {
				return
			}
			newState := obsolete
			if !s.casResolveState(oldState, newState) {
				goto retryMarkDel
			}
			close(s.closed)
			return
		}
		// store add hasn't changed.
		// mark store back to resolved state directly.
	retryMarkResolved:
		oldState := s.currentResolveState()
		if oldState != needCheck {
			return
		}
		newState := resolved
		if !s.casResolveState(oldState, newState) {
			goto retryMarkResolved
		}
	})
	if err != nil {
		logutil.BgLogger().Error("loadStore from PD failed and use old addr", zap.Uint64("id", s.storeID), zap.Error(err))
		return
	}
	return
}

func (s *Store) requestResolveAddr(retryBo *Backoffer, f func(store *metapb.Store)) (err error) {
	rsCh := resolveSf.DoChan(strconv.FormatUint(s.storeID, 10), func() (interface{}, error) {
		var st *metapb.Store
		st, err = s.rc.pdClient.GetStore(context.Background(), s.storeID) // TODO: how long we should set timeout?
		if err != nil {
			tikvRegionCacheCounterWithGetStoreError.Inc()
		} else {
			tikvRegionCacheCounterWithGetStoreOK.Inc()
		}
		if err != nil {
			return nil, err
		}
		f(st)
		return nil, nil
	})
	var ctx context.Context
	if retryBo != nil {
		ctx = retryBo.ctx
	} else {
		ctx = context.Background()
	}
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case rs := <-rsCh:
			if rs.Err != nil {
				// TODO: more refine PD error status handle.
				if errors.Cause(rs.Err) == context.Canceled {
					return errors.Trace(rs.Err)
				}
				err = errors.Errorf("loadStore from PD failed, id: %d, err: %v", s.storeID, rs.Err)
				if retryBo == nil {
					// no need retry when retryBo is nil
					return err
				}
				if err = retryBo.Backoff(BoPDRPC, err); err != nil {
					return err
				}
				continue
			}
			return nil
		}
	}
}

func (s *Store) currentResolveState() resolveState {
	var state resolveState
	if s == nil {
		return state
	}
	return resolveState(atomic.LoadUint64(&s.resolveState))
}

func (s *Store) casResolveState(oldState, newState resolveState) bool {
	return atomic.CompareAndSwapUint64(&s.resolveState, uint64(oldState), uint64(newState))
}

// scheduleResolve schedules a addr resolve when next getAddr called.
func (s *Store) scheduleResolve() {
	s.casResolveState(resolved, needCheck)
}

type livenessState uint32

const (
	unknown livenessState = 1 + iota
	reachable
	unreachable
	offline
)

const (
	normalLivenessCheckInterval    = 30 * time.Second
	criticalLivenessCheckInterval  = 1 * time.Second
	hibernateLivenessCheckInterval = 1 * time.Minute
)

func (s *Store) initLiveness(livenessCheckInterval time.Duration) {
	s.liveness.resetCh = make(chan struct{}, 1)
	s.liveness.checkInterval = uint64(livenessCheckInterval)
	go s.livenessLoop()
}

func (s *Store) livenessLoop() {
	t := time.NewTimer(time.Duration(atomic.LoadUint64(&s.liveness.checkInterval)))
	for {
		select {
		case <-s.closed:
			t.Stop()
			return
		case <-t.C:
			ns := s.requestLiveness(nil)
			os := atomic.LoadUint32(&s.livenessState)
			if livenessState(os) == unreachable && ns == unreachable {
				s.scheduleResolve()
			}
			s.updateLivenessState(os, uint32(ns), "liveness check loop found unreachable", false)
			t.Reset(time.Duration(atomic.LoadUint64(&s.liveness.checkInterval)))
		case <-s.liveness.resetCh:
			t.Reset(time.Duration(atomic.LoadUint64(&s.liveness.checkInterval)))
		}
	}
}

func (s *Store) resetLivenessInterval(i time.Duration, schedReset bool) {
	atomic.StoreUint64(&s.liveness.checkInterval, uint64(i))
	if schedReset {
		select {
		case s.liveness.resetCh <- struct{}{}:
		default:
		}
	}
}

func (s *Store) updateLivenessState(os, ns uint32, reason string, schedReset bool) {
	if ns == os {
		return
	}
	if ns == uint32(unreachable) && os == uint32(offline) {
		// keep hibernate interval, no need offline -> unreachable
		return
	}
	changed := atomic.CompareAndSwapUint32(&s.livenessState, os, ns)
	if changed {
		switch livenessState(ns) {
		case reachable:
			s.resetLivenessInterval(normalLivenessCheckInterval, schedReset)
		case unreachable:
			logutil.BgLogger().Info("invalidate regions using unreachable store",
				zap.Uint64("store", s.storeID), zap.String("add", s.saddr),
				zap.String("reason", reason))
			atomic.AddUint32(&s.epoch, 1)
			tikvRegionCacheCounterWithInvalidateStoreRegionsOK.Inc()

			s.scheduleResolve()

			s.resetLivenessInterval(criticalLivenessCheckInterval, schedReset)
		case offline:
			logutil.BgLogger().Info("invalidate regions using offline store",
				zap.Uint64("store", s.storeID), zap.String("add", s.saddr),
				zap.String("reason", reason))
			atomic.AddUint32(&s.epoch, 1)
			tikvRegionCacheCounterWithInvalidateStoreRegionsOK.Inc()

			s.resetLivenessInterval(hibernateLivenessCheckInterval, schedReset)
		}
	}
}

func (s *Store) currentEpoch() uint32 {
	return atomic.LoadUint32(&s.epoch)
}

func (s *Store) currentLivenessState() livenessState {
	return livenessState(atomic.LoadUint32(&s.livenessState))
}

func (s *Store) accessible() bool {
	st := s.currentLivenessState()
	return st == unknown || st == reachable
}

func (s *Store) requestLiveness(bo *Backoffer) (l livenessState) {
	_, saddr, err := s.getAddr(bo, nil, 0)
	if err != nil || len(saddr) == 0 {
		l = unknown
		return
	}
	rsCh := livenessSf.DoChan(saddr, func() (interface{}, error) {
		return invokeKVStatusAPI(saddr, time.Duration(config.GetGlobalConfig().TiKVClient.StoreLivenessTimeout)*time.Second), nil
	})
	var ctx context.Context
	if bo != nil {
		ctx = bo.ctx
	} else {
		ctx = context.Background()
	}
	select {
	case rs := <-rsCh:
		if rs.Err != nil {
			l = unknown
			return
		}
		l = rs.Val.(livenessState)
	case <-ctx.Done():
		l = unknown
		return
	}
	return
}

func invokeKVStatusAPI(saddr string, timeout time.Duration) livenessState {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	url := fmt.Sprintf("%s://%s/status", util.InternalHTTPSchema(), saddr)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		logutil.BgLogger().Info("[liveness] build kv status request fail", zap.String("store", saddr), zap.Error(err))
		return unreachable
	}
	resp, err := util.InternalHTTPClient().Do(req)
	if err != nil {
		logutil.BgLogger().Info("[liveness] request kv status fail", zap.String("store", saddr), zap.Error(err))
		return unreachable
	}
	defer func() {
		err1 := resp.Body.Close()
		if err1 != nil {
			logutil.BgLogger().Debug("[liveness] close kv status api body failed", zap.String("store", saddr), zap.Error(err))
		}
	}()
	if resp.StatusCode != http.StatusOK {
		logutil.BgLogger().Info("[liveness] request kv status fail", zap.String("store", saddr), zap.String("status", resp.Status))
		return unreachable
	}
	return reachable
}
