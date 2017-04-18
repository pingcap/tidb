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
	"sync"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/petar/GoLLRB/llrb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/pd-client"
	goctx "golang.org/x/net/context"
)

// RegionCache caches Regions loaded from PD.
type RegionCache struct {
	pdClient pd.Client
	mu       struct {
		sync.RWMutex
		regions map[RegionVerID]*Region
		sorted  *llrb.LLRB
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
	c.mu.regions = make(map[RegionVerID]*Region)
	c.mu.sorted = llrb.New()
	c.storeMu.stores = make(map[uint64]*Store)
	return c
}

// RPCContext contains data that is needed to send RPC to a region.
type RPCContext struct {
	goctx.Context

	Region RegionVerID
	KVCtx  *kvrpcpb.Context
	Addr   string
}

// GetStoreID returns StoreID.
func (c *RPCContext) GetStoreID() uint64 {
	if c.KVCtx != nil && c.KVCtx.Peer != nil {
		return c.KVCtx.Peer.StoreId
	}
	return 0
}

// GetRPCContext returns RPCContext for a region. If it returns nil, the region
// must be out of date and already dropped from cache.
func (c *RegionCache) GetRPCContext(bo *Backoffer, id RegionVerID) (*RPCContext, error) {
	c.mu.RLock()
	region, ok := c.mu.regions[id]
	if !ok {
		c.mu.RUnlock()
		return nil, nil
	}
	kvCtx := region.GetContext()
	c.mu.RUnlock()

	addr, err := c.GetStoreAddr(bo, kvCtx.GetPeer().GetStoreId())
	if err != nil {
		return nil, errors.Trace(err)
	}
	if addr == "" {
		// Store not found, region must be out of date.
		c.DropRegion(id)
		return nil, nil
	}
	return &RPCContext{
		Context: bo.ctx,
		Region:  id,
		KVCtx:   kvCtx,
		Addr:    addr,
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
	if r := c.getRegionFromCache(key); r != nil {
		loc := &KeyLocation{
			Region:   r.VerID(),
			StartKey: r.StartKey(),
			EndKey:   r.EndKey(),
		}
		c.mu.RUnlock()
		return loc, nil
	}
	c.mu.RUnlock()

	r, err := c.loadRegion(bo, key)
	if err != nil {
		return nil, errors.Trace(err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	r = c.insertRegionToCache(r)
	return &KeyLocation{
		Region:   r.VerID(),
		StartKey: r.StartKey(),
		EndKey:   r.EndKey(),
	}, nil
}

// LocateRegionByID searches for the region with ID
func (c *RegionCache) LocateRegionByID(bo *Backoffer, regionID uint64) (*KeyLocation, error) {
	c.mu.RLock()
	if r := c.getRegionByIDFromCache(regionID); r != nil {
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
	defer c.mu.Unlock()
	r = c.insertRegionToCache(r)
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

// DropRegion removes a cached Region.
func (c *RegionCache) DropRegion(id RegionVerID) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.dropRegionFromCache(id)
}

// UpdateLeader update some region cache with newer leader info.
func (c *RegionCache) UpdateLeader(regionID RegionVerID, leaderStoreID uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	r, ok := c.mu.regions[regionID]
	if !ok {
		log.Debugf("regionCache: cannot find region when updating leader %d,%d", regionID, leaderStoreID)
		return
	}

	if !r.SwitchPeer(leaderStoreID) {
		log.Debugf("regionCache: cannot find peer when updating leader %d,%d", regionID, leaderStoreID)
		c.dropRegionFromCache(r.VerID())
	}
}

func (c *RegionCache) getRegionFromCache(key []byte) *Region {
	var r *Region
	c.mu.sorted.DescendLessOrEqual(newRBSearchItem(key), func(item llrb.Item) bool {
		r = item.(*llrbItem).region
		return false
	})
	if r != nil && r.Contains(key) {
		return r
	}
	return nil
}

// insertRegionToCache tries to insert the Region to cache. If there is an old
// Region with the same VerID, it will return the old one instead.
func (c *RegionCache) insertRegionToCache(r *Region) *Region {
	if old, ok := c.mu.regions[r.VerID()]; ok {
		return old
	}
	old := c.mu.sorted.ReplaceOrInsert(newRBItem(r))
	if old != nil {
		delete(c.mu.regions, old.(*llrbItem).region.VerID())
	}
	c.mu.regions[r.VerID()] = r
	return r
}

// getRegionByIDFromCache tries to get region by regionID from cache
func (c *RegionCache) getRegionByIDFromCache(regionID uint64) *Region {
	for v, r := range c.mu.regions {
		if v.id == regionID {
			return r
		}
	}
	return nil
}

func (c *RegionCache) dropRegionFromCache(verID RegionVerID) {
	r, ok := c.mu.regions[verID]
	if !ok {
		return
	}
	c.mu.sorted.Delete(newRBItem(r))
	delete(c.mu.regions, r.VerID())
}

// loadRegion loads region from pd client, and picks the first peer as leader.
func (c *RegionCache) loadRegion(bo *Backoffer, key []byte) (*Region, error) {
	var backoffErr error
	for {
		if backoffErr != nil {
			err := bo.Backoff(boPDRPC, backoffErr)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}

		meta, leader, err := c.pdClient.GetRegion(bo.ctx, key)
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
		region := &Region{
			meta: meta,
			peer: meta.Peers[0],
		}
		if leader != nil {
			region.SwitchPeer(leader.GetStoreId())
		}
		return region, nil
	}
}

// loadRegionByID loads region from pd client, and picks the first peer as leader.
func (c *RegionCache) loadRegionByID(bo *Backoffer, regionID uint64) (*Region, error) {
	var backoffErr error
	for {
		if backoffErr != nil {
			err := bo.Backoff(boPDRPC, backoffErr)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}

		meta, leader, err := c.pdClient.GetRegionByID(bo.ctx, regionID)
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
			meta: meta,
			peer: meta.Peers[0],
		}
		if leader != nil {
			region.SwitchPeer(leader.GetStoreId())
		}
		return region, nil
	}
}

// GetStoreAddr returns a tikv server's address by its storeID. It checks cache
// first, sends request to pd server when necessary.
func (c *RegionCache) GetStoreAddr(bo *Backoffer, id uint64) (string, error) {
	c.storeMu.RLock()
	if store, ok := c.storeMu.stores[id]; ok {
		c.storeMu.RUnlock()
		return store.Addr, nil
	}
	c.storeMu.RUnlock()
	return c.ReloadStoreAddr(bo, id)
}

// ReloadStoreAddr reloads store's address.
func (c *RegionCache) ReloadStoreAddr(bo *Backoffer, id uint64) (string, error) {
	addr, err := c.loadStoreAddr(bo, id)
	if err != nil || addr == "" {
		return "", errors.Trace(err)
	}

	c.storeMu.Lock()
	defer c.storeMu.Unlock()
	c.storeMu.stores[id] = &Store{
		ID:   id,
		Addr: addr,
	}
	return addr, nil
}

// ClearStoreByID clears store from cache with storeID.
func (c *RegionCache) ClearStoreByID(id uint64) {
	c.storeMu.Lock()
	defer c.storeMu.Unlock()
	delete(c.storeMu.stores, id)
}

func (c *RegionCache) loadStoreAddr(bo *Backoffer, id uint64) (string, error) {
	for {
		store, err := c.pdClient.GetStore(bo.ctx, id)
		if err != nil {
			err = errors.Errorf("loadStore from PD failed, id: %d, err: %v", id, err)
			if err = bo.Backoff(boPDRPC, err); err != nil {
				return "", errors.Trace(err)
			}
			continue
		}
		if store == nil {
			return "", nil
		}
		return store.GetAddress(), nil
	}
}

// OnRequestFail is used for clearing cache when a tikv server does not respond.
func (c *RegionCache) OnRequestFail(ctx *RPCContext) {
	// Switch region's leader peer to next one.
	regionID := ctx.Region
	c.mu.Lock()
	if region, ok := c.mu.regions[regionID]; ok {
		if !region.OnRequestFail(ctx.KVCtx.GetPeer().GetStoreId()) {
			c.dropRegionFromCache(regionID)
		}
	}
	c.mu.Unlock()

	// Store's meta may be out of date.
	storeID := ctx.KVCtx.GetPeer().GetStoreId()
	c.storeMu.Lock()
	delete(c.storeMu.stores, storeID)
	c.storeMu.Unlock()

	c.mu.Lock()
	for id, r := range c.mu.regions {
		if r.peer.GetStoreId() == storeID {
			c.dropRegionFromCache(id)
		}
	}
	c.mu.Unlock()
}

// OnRegionStale removes the old region and inserts new regions into the cache.
func (c *RegionCache) OnRegionStale(ctx *RPCContext, newRegions []*metapb.Region) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.dropRegionFromCache(ctx.Region)

	for _, meta := range newRegions {
		if _, ok := c.pdClient.(*codecPDClient); ok {
			if err := decodeRegionMetaKey(meta); err != nil {
				return errors.Errorf("newRegion's range key is not encoded: %v, %v", meta, err)
			}
		}
		region := &Region{
			meta: meta,
			peer: meta.Peers[0],
		}
		region.SwitchPeer(ctx.KVCtx.GetPeer().GetStoreId())
		c.insertRegionToCache(region)
	}
	return nil
}

// moveLeaderToFirst moves the leader peer to the first and makes it easier to
// try the next peer if the current peer does not respond.
func moveLeaderToFirst(r *metapb.Region, leaderStoreID uint64) {
	for i := range r.Peers {
		if r.Peers[i].GetStoreId() == leaderStoreID {
			r.Peers[0], r.Peers[i] = r.Peers[i], r.Peers[0]
			return
		}
	}
}

// llrbItem is llrbTree's Item that uses []byte to compare.
type llrbItem struct {
	key    []byte
	region *Region
}

func newRBItem(r *Region) *llrbItem {
	return &llrbItem{
		key:    r.StartKey(),
		region: r,
	}
}

func newRBSearchItem(key []byte) *llrbItem {
	return &llrbItem{
		key: key,
	}
}

func (item *llrbItem) Less(other llrb.Item) bool {
	return bytes.Compare(item.key, other.(*llrbItem).key) < 0
}

// Region stores region's meta and its leader peer.
type Region struct {
	meta              *metapb.Region
	peer              *metapb.Peer
	unreachableStores []uint64
}

// GetID returns id.
func (r *Region) GetID() uint64 {
	return r.meta.GetId()
}

// RegionVerID is a unique ID that can identify a Region at a specific version.
type RegionVerID struct {
	id      uint64
	confVer uint64
	ver     uint64
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
		Peer:        r.peer,
	}
}

// OnRequestFail records unreachable peer and tries to select another valid peer.
// It returns false if all peers are unreachable.
func (r *Region) OnRequestFail(storeID uint64) bool {
	if r.peer.GetStoreId() != storeID {
		return true
	}
	r.unreachableStores = append(r.unreachableStores, storeID)
L:
	for _, p := range r.meta.Peers {
		for _, id := range r.unreachableStores {
			if p.GetStoreId() == id {
				continue L
			}
		}
		r.peer = p
		return true
	}
	return false
}

// SwitchPeer switches current peer to the one on specific store. It returns
// false if no peer matches the storeID.
func (r *Region) SwitchPeer(storeID uint64) bool {
	for _, p := range r.meta.Peers {
		if p.GetStoreId() == storeID {
			r.peer = p
			return true
		}
	}
	return false
}

// Contains checks whether the key is in the region, for the maximum region endKey is empty.
// startKey <= key < endKey.
func (r *Region) Contains(key []byte) bool {
	return bytes.Compare(r.meta.GetStartKey(), key) <= 0 &&
		(bytes.Compare(key, r.meta.GetEndKey()) < 0 || len(r.meta.GetEndKey()) == 0)
}

// Store contains a tikv server's address.
type Store struct {
	ID   uint64
	Addr string
}
