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
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/client"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

const (
	btreeDegree                = 32
	rcDefaultRegionCacheTTLSec = 600
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

// CachedRegion encapsulates {Region, TTL}
type CachedRegion struct {
	region     *Region
	lastAccess int64
}

const invalidatedLastAccessTime = -1

func (c *CachedRegion) checkRegionCacheTTL(ts int64) bool {
retry:
	lastAccess := atomic.LoadInt64(&c.lastAccess)
	if ts-lastAccess > rcDefaultRegionCacheTTLSec {
		return false
	}
	if !atomic.CompareAndSwapInt64(&c.lastAccess, lastAccess, ts) {
		goto retry
	}
	return true
}

func (c *CachedRegion) invalidate() {
	atomic.StoreInt64(&c.lastAccess, invalidatedLastAccessTime)
}

// RegionCache caches Regions loaded from PD.
type RegionCache struct {
	pdClient pd.Client

	mu struct {
		sync.RWMutex                               // mutex protect cached region
		regions      map[RegionVerID]*CachedRegion // cached regions be organized as regionVerID to region ref mapping
		sorted       *btree.BTree                  // cache regions be organized as sorted key to region ref mapping
	}
	storeMu struct {
		sync.RWMutex
		stores map[uint64]*Store
	}
}

// NewRegionCache creates a RegionCache.
func NewRegionCache(pdClient pd.Client) *RegionCache {
	c := &RegionCache{
		pdClient: pdClient,
	}
	c.mu.regions = make(map[RegionVerID]*CachedRegion)
	c.mu.sorted = btree.New(btreeDegree)
	c.storeMu.stores = make(map[uint64]*Store)
	return c
}

// RPCContext contains data that is needed to send RPC to a region.
type RPCContext struct {
	Region RegionVerID
	Meta   *metapb.Region
	Peer   *metapb.Peer
	Addr   string
}

// GetStoreID returns StoreID.
func (c *RPCContext) GetStoreID() uint64 {
	if c.Peer != nil {
		return c.Peer.StoreId
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

	cachedRegion := c.getRegionCacheItemWithRLock(id)
	if cachedRegion == nil {
		return nil, nil
	}

	if !cachedRegion.checkRegionCacheTTL(ts) {
		return nil, nil
	}

	store, peer := c.ensureRegionWorkPeer(cachedRegion.region, ts)
	if store == nil {
		return nil, nil
	}

	addr, err := store.ResolveAddr(bo, ts)
	if err != nil {
		return nil, err
	}
	if len(addr) == 0 {
		// Store not found, region must be out of date. TODO: check me?
		cachedRegion.invalidate()
		return nil, nil
	}

	return &RPCContext{
		Region: id,
		Meta:   cachedRegion.region.meta,
		Peer:   peer,
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
	c.mu.RLock()
	r := c.searchCachedRegion(key, false)
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

	r, err := c.loadRegion(bo, key, false)
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

// LocateEndKey searches for the region and range that the key is located.
// Unlike LocateKey, start key of a region is exclusive and end key is inclusive.
func (c *RegionCache) LocateEndKey(bo *Backoffer, key []byte) (*KeyLocation, error) {
	c.mu.RLock()
	r := c.searchCachedRegion(key, true)
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
	cachedRegion := c.getRegionCacheItemWithRLock(id)
	if cachedRegion == nil {
		return
	}
	tikvRegionCacheCounterWithDropRegionFromCacheOK.Inc()
	cachedRegion.invalidate()
}

// UpdateLeader update some region cache with newer leader info.
func (c *RegionCache) UpdateLeader(regionID RegionVerID, leaderStoreID uint64) {
	cachedRegion := c.getRegionCacheItemWithRLock(regionID)
	if cachedRegion == nil {
		logutil.Logger(context.Background()).Debug("regionCache: cannot find region when updating leader",
			zap.Uint64("regionID", regionID.GetID()),
			zap.Uint64("leaderStoreID", leaderStoreID))
		return
	}
	if !cachedRegion.region.SwitchWorkPeer(leaderStoreID) {
		logutil.Logger(context.Background()).Debug("regionCache: cannot find peer when updating leader",
			zap.Uint64("regionID", regionID.GetID()),
			zap.Uint64("leaderStoreID", leaderStoreID))
		cachedRegion.invalidate()
	}

}

// insertRegionToCache tries to insert the Region to cache.
func (c *RegionCache) insertRegionToCache(r *Region) {
	cachedRegion := &CachedRegion{
		region:     r,
		lastAccess: time.Now().Unix(),
	}
	old := c.mu.sorted.ReplaceOrInsert(newBtreeItem(cachedRegion))
	if old != nil {
		delete(c.mu.regions, old.(*btreeItem).cachedRegion.region.VerID())
	}
	c.mu.regions[r.VerID()] = cachedRegion
}

// searchCachedRegion finds a region from cache by key. Like `getCachedRegion`,
// it should be called with c.mu.RLock(), and the returned Region should not be
// used after c.mu is RUnlock().
// If the given key is the end key of the region that you want, you may set the second argument to true. This is useful when processing in reverse order.
func (c *RegionCache) searchCachedRegion(key []byte, isEndKey bool) *Region {
	ts := time.Now().Unix()
	var cr *CachedRegion
	c.mu.sorted.DescendLessOrEqual(newBtreeSearchItem(key), func(item btree.Item) bool {
		cr = item.(*btreeItem).cachedRegion
		if isEndKey && bytes.Equal(cr.region.StartKey(), key) {
			cr = nil    // clear result
			return true // iterate next item
		}
		if !cr.checkRegionCacheTTL(ts) {
			cr = nil
			return true
		}
		return false
	})
	if cr != nil && (!isEndKey && cr.region.Contains(key) || isEndKey && cr.region.ContainsByEnd(key)) {
		workStore, _ := c.ensureRegionWorkPeer(cr.region, ts)
		if workStore == nil {
			return nil
		}
		return cr.region
	}
	return nil
}

// getRegionByIDFromCache tries to get region by regionID from cache. Like
// `getCachedRegion`, it should be called with c.mu.RLock(), and the returned
// Region should not be used after c.mu is RUnlock().
func (c *RegionCache) getRegionByIDFromCache(regionID uint64) *Region {
	for v, r := range c.mu.regions {
		if v.id == regionID {
			return r.region
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
			meta:      meta,
			_workPeer: unsafe.Pointer(meta.Peers[0]),
		}
		if leader != nil {
			region.SwitchWorkPeer(leader.GetStoreId())
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
			meta:      meta,
			_workPeer: unsafe.Pointer(meta.Peers[0]),
		}
		if leader != nil {
			region.SwitchWorkPeer(leader.GetStoreId())
		}
		return region, nil
	}
}

func (c *RegionCache) getRegionCacheItemWithRLock(regionID RegionVerID) (cacheItem *CachedRegion) {
	c.mu.RLock()
	cacheItem = c.mu.regions[regionID]
	c.mu.RUnlock()
	return
}

func (c *RegionCache) ensureRegionWorkPeer(region *Region, ts int64) (workStore *Store, workPeer *metapb.Peer) {
retry:
	regionPeer := region.WorkPeer()

	var (
		cachedStore *Store
		exists      bool
	)
	if regionPeer != nil {
		c.storeMu.RLock()
		cachedStore, exists = c.storeMu.stores[regionPeer.GetStoreId()]
		c.storeMu.RUnlock()
		if !exists {
			cachedStore = c.getStoreByStoreID(regionPeer.GetStoreId())
		}

		if cachedStore.Reachable(ts) {
			workStore = cachedStore
			workPeer = regionPeer
			return
		}
	}

	cachedStore = nil
	var newPeer *metapb.Peer
	for i := range region.meta.Peers {
		otherPeer := region.meta.Peers[i]
		if regionPeer != nil && otherPeer.StoreId == regionPeer.GetStoreId() {
			continue
		}

		c.storeMu.RLock()
		peerStore, exists := c.storeMu.stores[otherPeer.GetStoreId()]
		c.storeMu.RUnlock()
		if !exists {
			peerStore = c.getStoreByStoreID(otherPeer.GetStoreId())
		}

		if peerStore.Reachable(ts) {
			cachedStore = peerStore
			newPeer = otherPeer
			break
		}
	}
	if !atomic.CompareAndSwapPointer(&region._workPeer, unsafe.Pointer(regionPeer), unsafe.Pointer(newPeer)) {
		goto retry
	}
	workStore = cachedStore
	workPeer = newPeer
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
	access := &storeAccess{}
	store = &Store{
		ID:            storeID,
		StoreLoader:   c.pdClient.GetStore,
		accessibility: unsafe.Pointer(access),
	}
	c.storeMu.stores[storeID] = store
	c.storeMu.Unlock()
	return
}

// ClearStoreByID clears store from cache with storeID.
func (c *RegionCache) ClearStoreByID(id uint64) {
	c.storeMu.Lock()
	defer c.storeMu.Unlock()
	delete(c.storeMu.stores, id)
}

// OnSendRequestFail is used for clearing cache when a tikv server does not respond.
func (c *RegionCache) OnSendRequestFail(ctx *RPCContext, err error) {
	failedStoreID := ctx.Peer.StoreId

	c.storeMu.RLock()
	store, exists := c.storeMu.stores[failedStoreID]
	if !exists {
		c.storeMu.RUnlock()
		return
	}
	c.storeMu.RUnlock()

	store.Mark(false)

	cr := c.getRegionCacheItemWithRLock(ctx.Region)
	if cr == nil {
		return
	}
	lastAccess := atomic.LoadInt64(&cr.lastAccess)
	if lastAccess == invalidatedLastAccessTime {
		return
	}
	store, _ = c.ensureRegionWorkPeer(cr.region, time.Now().Unix())
	if store == nil {
		cr.invalidate()
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
			meta: meta,
		}
		region.SwitchWorkPeer(ctx.Peer.GetStoreId())
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
	cachedRegion *CachedRegion
}

func newBtreeItem(cr *CachedRegion) *btreeItem {
	return &btreeItem{
		key:          cr.region.StartKey(),
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

// Region stores region's meta and its leader peer.
type Region struct {
	meta      *metapb.Region // region meta, immutable after creation
	_workPeer unsafe.Pointer // peer used by current region(*metapb.Peer)
}

// GetID returns id.
func (r *Region) GetID() uint64 {
	return r.meta.GetId()
}

// WorkPeer returns current work peer.
func (r *Region) WorkPeer() *metapb.Peer {
	return (*metapb.Peer)(atomic.LoadPointer(&r._workPeer))
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

// GetContext constructs kvprotopb.Context from region info.
func (r *Region) GetContext() *kvrpcpb.Context {
	return &kvrpcpb.Context{
		RegionId:    r.meta.Id,
		RegionEpoch: r.meta.RegionEpoch,
		Peer:        r.WorkPeer(),
	}
}

// SwitchWorkPeer switches current peer to the one on specific store. It returns
// false if no peer matches the storeID.
func (r *Region) SwitchWorkPeer(storeID uint64) bool {
	var leaderFound bool
	for i := range r.meta.Peers {
		v := r.meta.Peers[i]
		if v.GetStoreId() == storeID {
			leaderFound = true
			atomic.StorePointer(&r._workPeer, unsafe.Pointer(v))
		}
	}
	return leaderFound
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

// storeLoader loads the Store info given by storeId.
type storeLoader func(ctx context.Context, id uint64) (*metapb.Store, error)

// Store contains a kv server's address.
type Store struct {
	sync.Mutex              // mutex to avoid duplicate load requests
	StoreLoader storeLoader // loader to get store address from PD
	lastLoadTS  int64       // last load success timestamp(sec)
	addr        string      // loaded store address

	ID uint64 // store id

	accessibility unsafe.Pointer // store's accessibility, point to *storeAccess
}

// storeAccess contains store's access info.
type storeAccess struct {
	failAttempt    uint  // continue fail attempt count
	lastFailedTime int64 // last fail attempt time.
}

// reResolveUnreachableStoreIntervalSec after it will trigger re-resolve if store becomes unreachable.
const reResolveUnreachableStoreIntervalSec = 3600

// ResolveAddr resolves the address of store.
// following up resolve request will reuse previous result until
// store become unreachable and after reResolveUnreachableStoreIntervalSec
func (s *Store) ResolveAddr(bo *Backoffer, ts int64) (addr string, err error) {
	var store *metapb.Store
	s.Lock() // hold store-level mutex to protect duplicate resolve request.
	if len(s.addr) > 0 {
		if s.Reachable(ts) || ts-s.lastLoadTS > reResolveUnreachableStoreIntervalSec {
			addr = s.addr
			s.Unlock()
			return
		}
	}
	// re-resolve if unreachable and long time from last load.
	for {
		store, err = s.StoreLoader(bo.ctx, s.ID)
		if err != nil {
			tikvRegionCacheCounterWithGetStoreError.Inc()
		} else {
			tikvRegionCacheCounterWithGetStoreOK.Inc()
		}
		if err != nil {
			// TODO: more refine PD error status handle.
			if errors.Cause(err) == context.Canceled {
				s.Unlock()
				return
			}
			err = errors.Errorf("loadStore from PD failed, id: %d, err: %v", s.ID, err)
			if err = bo.Backoff(BoPDRPC, err); err != nil {
				s.Unlock()
				return
			}
			continue
		}
		if store == nil {
			s.Unlock()
			return
		}
		addr = store.GetAddress()
		s.addr = addr
		s.lastLoadTS = ts
		s.Unlock()
		return
	}
}

// maxExponentAttempt before this blackout time is exponent increment.
const maxExponentAttempt = 10

// Reachable returns whether store can be reachable.
func (s *Store) Reachable(ts int64) bool {
	status := (*storeAccess)(atomic.LoadPointer(&s.accessibility))
	if status.failAttempt == 0 || status.lastFailedTime == 0 {
		// return quickly if it's continue success.
		return true
	}
	// check blackout time window to determine store's reachable.
	attempt := status.failAttempt
	if attempt > maxExponentAttempt {
		attempt = maxExponentAttempt
	}
	blackoutDeadline := status.lastFailedTime + int64(backoffutils.ExponentBase2(attempt))
	return blackoutDeadline < ts
}

// Mark marks the processing result.
func (s *Store) Mark(success bool) {
retry:
	old := (*storeAccess)(atomic.LoadPointer(&s.accessibility))
	if (old.failAttempt == 0 && success) || (!success && old.failAttempt >= maxExponentAttempt) {
		// return quickly if continue success, and no more mark when attempt meet max bound.
		return
	}
	// mark store be success or fail
	var newAccess storeAccess
	if !success {
		newAccess.lastFailedTime = time.Now().Unix()
		newAccess.failAttempt = old.failAttempt + 1
	}
	if !atomic.CompareAndSwapPointer(&s.accessibility, unsafe.Pointer(old), unsafe.Pointer(&newAccess)) {
		goto retry
	}
}
