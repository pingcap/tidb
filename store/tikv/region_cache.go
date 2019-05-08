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
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/google/btree"
	"github.com/grpc-ecosystem/go-grpc-middleware/util/backoffutils"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/client"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

const (
	btreeDegree                = 32
	rcDefaultRegionCacheTTLSec = 600
	invalidatedLastAccessTime  = -1
)

var (
	tikvRegionCacheCounterWithDropRegionFromCacheOK = metrics.TiKVRegionCacheCounter.WithLabelValues("drop_region_from_cache", "ok")
	tikvRegionCacheCounterWithGetRegionByIDOK       = metrics.TiKVRegionCacheCounter.WithLabelValues("get_region_by_id", "ok")
	tikvRegionCacheCounterWithGetRegionByIDError    = metrics.TiKVRegionCacheCounter.WithLabelValues("get_region_by_id", "err")
	tikvRegionCacheCounterWithGetRegionOK           = metrics.TiKVRegionCacheCounter.WithLabelValues("get_region", "ok")
	tikvRegionCacheCounterWithGetRegionError        = metrics.TiKVRegionCacheCounter.WithLabelValues("get_region", "err")
	tikvRegionCacheCounterWithGetStoreOK            = metrics.TiKVRegionCacheCounter.WithLabelValues("get_store", "ok")
	tikvRegionCacheCounterWithGetStoreError         = metrics.TiKVRegionCacheCounter.WithLabelValues("get_store", "err")
)

const (
	updated  int32 = iota // region is updated and no need to reload.
	needSync              //  need sync new region info.
)

// Region presents kv region
type Region struct {
	meta         *metapb.Region // immutable after fetched from pd
	stores       []*Store       // stores in this region
	workStoreIdx int32          // point to current work peer in meta.Peers and work store in stores(same idx)
	syncStoreIdx int32          // point to init peer index and need reload if meet syncStoreIdx again
	syncFlag     int32          // mark region need be sync in next turn
	lastAccess   int64          // last region access time, see checkRegionCacheTTL
}

func (r *Region) initStores(c *RegionCache) {
	for _, p := range r.meta.Peers {
		c.storeMu.RLock()
		store, exists := c.storeMu.stores[p.StoreId]
		c.storeMu.RUnlock()
		if !exists {
			store = c.getStoreByStoreID(p.StoreId)
		}
		r.stores = append(r.stores, store)
	}
}

func (r *Region) checkRegionCacheTTL(ts int64) bool {
retry:
	lastAccess := atomic.LoadInt64(&r.lastAccess)
	if ts-lastAccess > rcDefaultRegionCacheTTLSec {
		return false
	}
	if !atomic.CompareAndSwapInt64(&r.lastAccess, lastAccess, ts) {
		goto retry
	}
	return true
}

// invalidate invalidates a region, next time it will got null result.
func (r *Region) invalidate() {
	atomic.StoreInt64(&r.lastAccess, invalidatedLastAccessTime)
}

// scheduleReload schedules reload region request in next LocateKey.
func (r *Region) scheduleReload() {
retry:
	oldValue := atomic.LoadInt32(&r.syncFlag)
	if oldValue != updated {
		return
	}
	if !atomic.CompareAndSwapInt32(&r.syncFlag, oldValue, needSync) {
		goto retry
	}
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
	go c.asyncCheckAndResolveLoop()
	return c
}

// Close releases region cache's resource.
func (c *RegionCache) Close() {
	close(c.closeCh)
}

const checkStoreInterval = 1 * time.Second

// asyncCheckAndResolveLoop with
func (c *RegionCache) asyncCheckAndResolveLoop() {
	tick := time.NewTicker(checkStoreInterval)
	defer tick.Stop()
	var needCheckStores []*Store
	for {
		select {
		case <-tick.C:
			c.checkAndResolve(&needCheckStores)
		case <-c.closeCh:
			return
		}
	}
}

// checkAndResolve checks and resolve addr of failed stores.
// this method isn't thread-safe and only be used by one goroutine.
func (c *RegionCache) checkAndResolve(needCheckStores *[]*Store) {
	defer func() {
		r := recover()
		if r != nil {
			logutil.Logger(context.Background()).Error("panic in the checkAndResolve goroutine",
				zap.Reflect("r", r),
				zap.Stack("stack trace"))
		}
	}()

	*needCheckStores = (*needCheckStores)[0:]
	c.storeMu.RLock()
	for _, store := range c.storeMu.stores {
		if store.needCheck() {
			*needCheckStores = append(*needCheckStores, store)
		}
	}
	c.storeMu.RUnlock()

	for _, store := range *needCheckStores {
		store.reResolve()
	}
}

// RPCContext contains data that is needed to send RPC to a region.
type RPCContext struct {
	Region RegionVerID
	Meta   *metapb.Region
	Peer   *metapb.Peer
	Store  *Store
	Addr   string
}

// GetStoreID returns StoreID.
func (c *RPCContext) GetStoreID() uint64 {
	if c.Store != nil {
		return c.Store.storeID
	}
	return 0
}

func (c *RPCContext) String() string {
	return fmt.Sprintf("region ID: %d, meta: %s, peer: %s, addr: %s",
		c.Region.GetID(), c.Meta, c.Peer, c.Addr)
}

// GetRPCContext returns RPCContext for a region. If it returns nil, the region
// must be out of date and already dropped from cache.
func (c *RegionCache) GetRPCContext(bo *Backoffer, id RegionVerID) (*RPCContext, error) {
	ts := time.Now().Unix()

	cachedRegion := c.getCachedRegionWithRLock(id)
	if cachedRegion == nil {
		return nil, nil
	}

	if !cachedRegion.checkRegionCacheTTL(ts) {
		return nil, nil
	}

	store, peer := c.routeStoreInRegion(cachedRegion, ts)
	if store == nil {
		// Store not found, region must be out of date.
		cachedRegion.invalidate()
		return nil, nil
	}

	addr, err := store.getAddr(bo)
	if err != nil {
		return nil, err
	}
	if len(addr) == 0 {
		// Store not found, region must be out of date.
		cachedRegion.invalidate()
		return nil, nil
	}

	return &RPCContext{
		Region: id,
		Meta:   cachedRegion.meta,
		Peer:   peer,
		Store:  store,
		Addr:   addr,
	}, nil
}

// KeyLocation is the region and range that a key is located.
type KeyLocation struct {
	Region   RegionVerID
	StartKey []byte
	EndKey   []byte
}

// Contains checks if key is in [StartKey, EndKey).
func (l *KeyLocation) Contains(key []byte) bool {
	return bytes.Compare(l.StartKey, key) <= 0 &&
		(bytes.Compare(key, l.EndKey) < 0 || len(l.EndKey) == 0)
}

// LocateKey searches for the region and range that the key is located.
func (c *RegionCache) LocateKey(bo *Backoffer, key []byte) (*KeyLocation, error) {
	r := c.searchCachedRegion(key, false)
	if r == nil || r.needReload() {
		var err error
		r, err = c.loadRegion(bo, key, false)
		if err != nil {
			return nil, errors.Trace(err)
		}

		c.mu.Lock()
		c.insertRegionToCache(r)
		c.mu.Unlock()
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
	r := c.searchCachedRegion(key, true)
	if r != nil {
		loc := &KeyLocation{
			Region:   r.VerID(),
			StartKey: r.StartKey(),
			EndKey:   r.EndKey(),
		}
		return loc, nil
	}

	r, err := c.loadRegion(bo, key, true)
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

// LocateRegionByID searches for the region with ID.
func (c *RegionCache) LocateRegionByID(bo *Backoffer, regionID uint64) (*KeyLocation, error) {
	c.mu.RLock()
	r := c.getRegionByIDFromCache(regionID)
	if r != nil {
		loc := &KeyLocation{
			Region:   r.VerID(),
			StartKey: r.StartKey(),
			EndKey:   r.EndKey(),
		}
		c.mu.RUnlock()
		return loc, nil
	}
	c.mu.RUnlock()

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
func (c *RegionCache) GroupKeysByRegion(bo *Backoffer, keys [][]byte) (map[RegionVerID][][]byte, RegionVerID, error) {
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
		}
		id := lastLoc.Region
		if i == 0 {
			first = id
		}
		groups[id] = append(groups[id], k)
	}
	return groups, first, nil
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

// InvalidateCachedRegion removes a cached Region.
func (c *RegionCache) InvalidateCachedRegion(id RegionVerID) {
	cachedRegion := c.getCachedRegionWithRLock(id)
	if cachedRegion == nil {
		return
	}
	tikvRegionCacheCounterWithDropRegionFromCacheOK.Inc()
	cachedRegion.invalidate()
}

// UpdateLeader update some region cache with newer leader info.
func (c *RegionCache) UpdateLeader(regionID RegionVerID, leaderStoreID uint64) {
	r := c.getCachedRegionWithRLock(regionID)
	if r == nil {
		logutil.Logger(context.Background()).Debug("regionCache: cannot find region when updating leader",
			zap.Uint64("regionID", regionID.GetID()),
			zap.Uint64("leaderStoreID", leaderStoreID))
		return
	}
	if !c.switchWorkStore(r, leaderStoreID) {
		logutil.Logger(context.Background()).Debug("regionCache: cannot find peer when updating leader",
			zap.Uint64("regionID", regionID.GetID()),
			zap.Uint64("leaderStoreID", leaderStoreID))
		r.invalidate()
	}
}

// insertRegionToCache tries to insert the Region to cache.
func (c *RegionCache) insertRegionToCache(cachedRegion *Region) {
	old := c.mu.sorted.ReplaceOrInsert(newBtreeItem(cachedRegion))
	if old != nil {
		delete(c.mu.regions, old.(*btreeItem).cachedRegion.VerID())
	}
	c.mu.regions[cachedRegion.VerID()] = cachedRegion
}

// searchCachedRegion finds a region from cache by key. Like `getCachedRegion`,
// it should be called with c.mu.RLock(), and the returned Region should not be
// used after c.mu is RUnlock().
// If the given key is the end key of the region that you want, you may set the second argument to true. This is useful when processing in reverse order.
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
		if !c.hasAvailableStore(r, ts) {
			return nil
		}
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
// If the given key is the end key of the region that you want, you may set the second argument to true. This is useful when processing in reverse order.
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
		if isEndKey && !searchPrev && bytes.Compare(meta.StartKey, key) == 0 && len(meta.StartKey) != 0 {
			searchPrev = true
			continue
		}
		region := &Region{
			meta:       meta,
			lastAccess: time.Now().Unix(),
			stores:     make([]*Store, 0, len(meta.Peers)),
		}
		region.initStores(c)
		if leader != nil {
			c.switchWorkStore(region, leader.StoreId)
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
			backoffErr = errors.Errorf("region not found for regionID %q", regionID)
			continue
		}
		if len(meta.Peers) == 0 {
			return nil, errors.New("receive Region with no peer")
		}
		region := &Region{
			meta:       meta,
			lastAccess: time.Now().Unix(),
			stores:     make([]*Store, 0, len(meta.Peers)),
		}
		region.initStores(c)
		if leader != nil {
			c.switchWorkStore(region, leader.GetStoreId())
		}
		return region, nil
	}
}

func (c *RegionCache) getCachedRegionWithRLock(regionID RegionVerID) (r *Region) {
	c.mu.RLock()
	r = c.mu.regions[regionID]
	c.mu.RUnlock()
	return
}

// routeStoreInRegion ensures region have workable store and return it.
func (c *RegionCache) routeStoreInRegion(region *Region, ts int64) (workStore *Store, workPeer *metapb.Peer) {
	if len(region.stores) == 0 {
		return
	}
retry:
	cachedStore, cachedPeer, cachedIdx := region.WorkStorePeer()
	// almost time requests be directly routed to stable leader.
	if cachedStore != nil && cachedStore.stableLeader() {
		workStore = cachedStore
		workPeer = cachedPeer
		return
	}

	// try round-robin find & switch to other peers when old leader meet error.
	newIdx := -1
	storeNum := len(region.stores)
	i := (cachedIdx + 1) % storeNum
	start := i
	for {
		store := region.stores[i]
		if store.Available(ts) {
			newIdx = i
			break
		}
		i = (i + 1) % storeNum
		if i == start {
			break
		}
	}
	if newIdx < 0 {
		return
	}
	if !atomic.CompareAndSwapInt32(&region.workStoreIdx, int32(cachedIdx), int32(newIdx)) {
		goto retry
	}
	if int32(newIdx) == atomic.LoadInt32(&region.syncStoreIdx) {
		region.scheduleReload()
	}
	workStore = region.stores[newIdx]
	workPeer = region.meta.Peers[newIdx]
	return
}

// hasAvailableStore checks whether region has available store.
// different to `routeStoreInRegion` just check and never change work store or peer.
func (c *RegionCache) hasAvailableStore(region *Region, ts int64) bool {
	if len(region.stores) == 0 {
		return false
	}
	for _, store := range region.stores {
		if store.Available(ts) {
			return true
		}
	}
	return false
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
	store.resolve.fn = c.pdClient.GetStore
	c.storeMu.stores[storeID] = store
	c.storeMu.Unlock()
	return
}

// OnSendRequestFail is used for clearing cache when a tikv server does not respond.
func (c *RegionCache) OnSendRequestFail(ctx *RPCContext, err error) {
	failedStoreID := ctx.Store.storeID

	c.storeMu.RLock()
	store, exists := c.storeMu.stores[failedStoreID]
	if !exists {
		c.storeMu.RUnlock()
		return
	}
	c.storeMu.RUnlock()

	store.markAccess(false)

	r := c.getCachedRegionWithRLock(ctx.Region)
	if r == nil {
		return
	}
	lastAccess := atomic.LoadInt64(&r.lastAccess)
	if lastAccess == invalidatedLastAccessTime {
		return
	}
}

// OnRegionEpochNotMatch removes the old region and inserts new regions into the cache.
func (c *RegionCache) OnRegionEpochNotMatch(bo *Backoffer, ctx *RPCContext, currentRegions []*metapb.Region) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	cachedRegion, ok := c.mu.regions[ctx.Region]
	if ok {
		tikvRegionCacheCounterWithDropRegionFromCacheOK.Inc()
		cachedRegion.invalidate()
	}

	// Find whether the region epoch in `ctx` is ahead of TiKV's. If so, backoff.
	for _, meta := range currentRegions {
		if meta.GetId() == ctx.Region.id &&
			(meta.GetRegionEpoch().GetConfVer() < ctx.Region.confVer ||
				meta.GetRegionEpoch().GetVersion() < ctx.Region.ver) {
			err := errors.Errorf("region epoch is ahead of tikv. rpc ctx: %+v, currentRegions: %+v", ctx, currentRegions)
			logutil.Logger(context.Background()).Info("region epoch is ahead of tikv", zap.Error(err))
			return bo.Backoff(BoRegionMiss, err)
		}
	}

	// If the region epoch is not ahead of TiKV's, replace region meta in region cache.
	for _, meta := range currentRegions {
		if _, ok := c.pdClient.(*codecPDClient); ok {
			if err := decodeRegionMetaKey(meta); err != nil {
				return errors.Errorf("newRegion's range key is not encoded: %v, %v", meta, err)
			}
		}
		region := &Region{
			meta:       meta,
			lastAccess: time.Now().Unix(),
			stores:     make([]*Store, 0, len(meta.Peers)),
		}
		region.initStores(c)
		c.switchWorkStore(region, ctx.Store.storeID)
		c.insertRegionToCache(region)
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

// WorkStorePeer returns current work store with work peer.
func (r *Region) WorkStorePeer() (store *Store, peer *metapb.Peer, idx int) {
	idx = int(atomic.LoadInt32(&r.workStoreIdx))
	store = r.stores[idx]
	peer = r.meta.Peers[idx]
	return
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

// switchWorkStore switches current store to the one on specific store. It returns
// false if no peer matches the storeID.
func (c *RegionCache) switchWorkStore(r *Region, storeID uint64) (foundLeader bool) {
	if len(r.meta.Peers) == 0 {
		return
	}
	leaderIdx := -1
	for i, p := range r.meta.Peers {
		if p.GetStoreId() == storeID {
			leaderIdx = i
		}
	}
	if leaderIdx >= 0 {
		foundLeader = true
	} else {
		leaderIdx = 0
	}
	atomic.StoreInt32(&r.workStoreIdx, int32(leaderIdx))
	atomic.StoreInt32(&r.syncStoreIdx, int32(leaderIdx))
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

// fn loads the Store info given by storeId.
type resolveFunc func(ctx context.Context, id uint64) (*metapb.Store, error)

// Store contains a kv process's address.
type Store struct {
	addr    atomic.Value // loaded store address(*string)
	storeID uint64       // store's id
	state   uint64       // unsafe store storeState

	resolve struct {
		sync.Mutex             // protect pd from concurrent init requests
		fn         resolveFunc // func to get store address from PD
	}
}

// storeState contains store's access info.
type storeState struct {
	lastFailedTime uint32
	failedAttempt  uint16
	resolveState   resolveState
}

type resolveState uint8

const (
	unresolved resolveState = iota
	resolved
	needCheck
)

// getAddr resolves the address of store.
// following up resolve request will reuse previous result until
// store become unreachable and after reResolveUnreachableStoreIntervalSec
func (s *Store) getAddr(bo *Backoffer) (addr string, err error) {
	// always use current addr event if it maybe staled.
	if !s.needInitResolve() {
		v := s.addr.Load()
		if v == nil {
			addr = ""
			return
		}
		addr = v.(string)
		return
	}
	// only resolve store addr from init status at first time.
	addr, err = s.initResolve(bo)
	return
}

// initResolve resolves addr for store that never resolved.
func (s *Store) initResolve(bo *Backoffer) (addr string, err error) {
	s.resolve.Lock()
	if !s.needInitResolve() {
		s.resolve.Unlock()
		v := s.addr.Load()
		if v == nil {
			addr = ""
			return
		}
		addr = v.(string)
		return
	}
	var store *metapb.Store
	for {
		store, err = s.resolve.fn(bo.ctx, s.storeID)
		if err != nil {
			tikvRegionCacheCounterWithGetStoreError.Inc()
		} else {
			tikvRegionCacheCounterWithGetStoreOK.Inc()
		}
		if err != nil {
			// TODO: more refine PD error status handle.
			if errors.Cause(err) == context.Canceled {
				s.resolve.Unlock()
				return
			}
			err = errors.Errorf("loadStore from PD failed, id: %d, err: %v", s.storeID, err)
			if err = bo.Backoff(BoPDRPC, err); err != nil {
				s.resolve.Unlock()
				return
			}
			continue
		}
		if store == nil {
			s.resolve.Unlock()
			return
		}
		addr = store.GetAddress()
		s.addr.Store(addr)
		s.markResolved()
		s.resolve.Unlock()
		return
	}
}

// reResolve try to resolve addr for store that need check.
func (s *Store) reResolve() {
	var addr string
	store, err := s.resolve.fn(context.Background(), s.storeID)
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
		logutil.Logger(context.Background()).Error("loadStore from PD failed", zap.Uint64("id", s.storeID), zap.Error(err))
		// we cannot do backoff in reResolve loop but try check other store and wait tick.
		return
	}
	if store == nil {
		return
	}
	addr = store.GetAddress()
	s.addr.Store(addr)
	s.markResolved()
	return
}

const (
	// maxExponentAttempt before this blackout time is exponent increment.
	maxExponentAttempt = 10
	// startBlackoutAfterAttempt after continue fail attempts start blackout store.
	startBlackoutAfterAttempt = 20
)

// stableLeader returns whether store is stable leader and no need retry other node.
func (s *Store) stableLeader() bool {
	var state storeState
	*(*uint64)(unsafe.Pointer(&state)) = atomic.LoadUint64(&s.state)
	if state.failedAttempt == 0 || state.lastFailedTime == 0 {
		// return quickly if it's continue success.
		return true
	}
	return false
}

// Available returns whether store be available for current.
func (s *Store) Available(ts int64) bool {
	var state storeState
	*(*uint64)(unsafe.Pointer(&state)) = atomic.LoadUint64(&s.state)
	if state.failedAttempt == 0 || state.lastFailedTime == 0 {
		// return quickly if it's continue success.
		return true
	}
	// first `startBlackoutAfterAttempt` can retry immediately.
	if state.failedAttempt < startBlackoutAfterAttempt {
		return true
	}
	// continue fail over than `startBlackoutAfterAttempt` start blackout store logic.
	// check blackout time window to determine store's reachable.
	if state.failedAttempt > startBlackoutAfterAttempt+maxExponentAttempt {
		state.failedAttempt = startBlackoutAfterAttempt + maxExponentAttempt
	}
	blackoutDeadline := int64(state.lastFailedTime) + 1*int64(backoffutils.ExponentBase2(uint(state.failedAttempt-startBlackoutAfterAttempt+1)))
	return blackoutDeadline <= ts
}

// needInitResolve checks whether store need to do init block resolve.
func (s *Store) needInitResolve() bool {
	var state storeState
	*(*uint64)(unsafe.Pointer(&state)) = atomic.LoadUint64(&s.state)
	return state.resolveState == unresolved
}

// needCheck checks whether store need to do async resolve check.
func (s *Store) needCheck() bool {
	var state storeState
	*(*uint64)(unsafe.Pointer(&state)) = atomic.LoadUint64(&s.state)
	return state.resolveState == needCheck
}

// markAccess marks the processing result.
func (s *Store) markAccess(success bool) {
retry:
	oldValue := atomic.LoadUint64(&s.state)
	var state storeState
	*(*uint64)(unsafe.Pointer(&state)) = oldValue
	if (state.failedAttempt == 0 && success) || (!success && state.failedAttempt >= (startBlackoutAfterAttempt+maxExponentAttempt)) {
		// return quickly if continue success, and no more mark when attempt meet max bound.
		return
	}
	if !success {
		if state.lastFailedTime == 0 {
			state.lastFailedTime = uint32(time.Now().Unix())
		}
		state.failedAttempt = state.failedAttempt + 1
		if state.resolveState == resolved {
			state.resolveState = needCheck
		}
	} else {
		state.lastFailedTime = 0
		state.failedAttempt = 0
	}
	newValue := *(*uint64)(unsafe.Pointer(&state))
	if !atomic.CompareAndSwapUint64(&s.state, oldValue, newValue) {
		goto retry
	}
}

// markResolved marks store has be resolved.
func (s *Store) markResolved() {
retry:
	oldValue := atomic.LoadUint64(&s.state)
	var state storeState
	*(*uint64)(unsafe.Pointer(&state)) = oldValue
	if state.resolveState == resolved {
		return
	}
	state.resolveState = resolved
	newValue := *(*uint64)(unsafe.Pointer(&state))
	if !atomic.CompareAndSwapUint64(&s.state, oldValue, newValue) {
		goto retry
	}
}

// markNeedCheck marks resolved store to be async resolve to check store addr change.
func (s *Store) markNeedCheck() {
retry:
	oldValue := atomic.LoadUint64(&s.state)
	var state storeState
	*(*uint64)(unsafe.Pointer(&state)) = oldValue
	if state.resolveState != resolved {
		return
	}
	state.resolveState = needCheck
	newValue := *(*uint64)(unsafe.Pointer(&state))
	if !atomic.CompareAndSwapUint64(&s.state, oldValue, newValue) {
		goto retry
	}
}
