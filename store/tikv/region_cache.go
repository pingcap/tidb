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
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/pd-client"
)

// RegionCache store region cache by region id.
type RegionCache struct {
	pdClient pd.Client
	mu       sync.RWMutex
	// TODO: store in array and use binary search
	regions map[uint64]*Region
}

// NewRegionCache new region cache.
func NewRegionCache(pdClient pd.Client) *RegionCache {
	return &RegionCache{
		pdClient: pdClient,
		regions:  make(map[uint64]*Region),
	}
}

// GetRegion find in cache, or get new region.
func (c *RegionCache) GetRegion(key []byte) (*Region, error) {
	if r := c.getRegionFromCache(key); r != nil {
		return r, nil
	}
	r, err := c.loadRegion(key)
	return r, errors.Trace(err)
}

// DropRegion remove some region cache.
func (c *RegionCache) DropRegion(regionID uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.regions, regionID)
}

// NextStore picks next store as new leader, if out of range of stores delete region.
func (c *RegionCache) NextStore(regionID uint64) {
	// A and B get the same region and current leader is 1, they both will pick
	// store 2 as leader.
	c.mu.RLock()
	region, ok := c.regions[regionID]
	c.mu.RUnlock()
	if !ok {
		return
	}
	if leader, err := region.NextStore(); err != nil {
		c.mu.Lock()
		delete(c.regions, regionID)
		c.mu.Unlock()
	} else {
		c.UpdateLeader(regionID, leader)
	}
}

// UpdateLeader update some region cache with newer leader store ID.
func (c *RegionCache) UpdateLeader(regionID, leaderStoreID uint64) {
	c.mu.RLock()
	old, ok := c.regions[regionID]
	c.mu.RUnlock()
	if !ok {
		log.Debugf("regionCache: cannot find region when updating leader %d,%d", regionID, leaderStoreID)
		return
	}
	var (
		store *metapb.Store
		err   error
	)

	curStoreIdx := -1
	for idx, storeID := range old.meta.StoreIds {
		if storeID == leaderStoreID {
			// No need update leader.
			if idx == old.curStoreIdx {
				return
			}
			curStoreIdx = idx
			break
		}
	}
	if curStoreIdx != -1 {
		store, err = c.pdClient.GetStore(leaderStoreID)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.regions, regionID)

	if curStoreIdx == -1 || err != nil {
		// Can't find the store in cache, or error occurs when loading
		// store from PD.
		// Leave the region deleted, it will be filled later.
		return
	}

	c.regions[regionID] = &Region{
		meta:        old.meta,
		addr:        store.GetAddress(),
		curStoreIdx: curStoreIdx,
	}
}

// getRegionFromCache scan all region cache and find which region contains key.
func (c *RegionCache) getRegionFromCache(key []byte) *Region {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, r := range c.regions {
		if r.Contains(key) {
			return r
		}
	}
	return nil
}

// loadRegion get region from pd client, and pick the random store as leader.
func (c *RegionCache) loadRegion(key []byte) (*Region, error) {
	meta, err := c.pdClient.GetRegion(key)
	if err != nil {
		// We assume PD will recover soon.
		return nil, errors.Annotate(err, txnRetryableMark)
	}
	if len(meta.StoreIds) == 0 {
		return nil, errors.New("receive Region with no store")
	}
	curStoreIdx := 0
	storeID := meta.StoreIds[curStoreIdx]
	store, err := c.pdClient.GetStore(storeID)
	if err != nil {
		// We assume PD will recover soon.
		return nil, errors.Annotate(err, txnRetryableMark)
	}
	region := &Region{
		meta:        meta,
		addr:        store.GetAddress(),
		curStoreIdx: curStoreIdx,
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if r, ok := c.regions[region.GetID()]; ok {
		return r, nil
	}
	c.regions[region.GetID()] = region
	return region, nil
}

// Region store region info. Region is a readonly class.
type Region struct {
	meta        *metapb.Region
	addr        string
	curStoreIdx int
}

// GetID return id.
func (r *Region) GetID() uint64 {
	return r.meta.GetId()
}

// StartKey return StartKey.
func (r *Region) StartKey() []byte {
	return r.meta.StartKey
}

// EndKey return EndKey.
func (r *Region) EndKey() []byte {
	return r.meta.EndKey
}

// GetAddress return address.
func (r *Region) GetAddress() string {
	return r.addr
}

// GetContext construct kvprotopb.Context from region info.
func (r *Region) GetContext() *kvrpcpb.Context {
	return &kvrpcpb.Context{
		RegionId:    r.meta.Id,
		RegionEpoch: r.meta.RegionEpoch,
	}
}

// Contains checks whether the key is in the region, for the maximum region endKey is empty.
// startKey <= key < endKey.
func (r *Region) Contains(key []byte) bool {
	return bytes.Compare(r.meta.GetStartKey(), key) <= 0 &&
		(bytes.Compare(key, r.meta.GetEndKey()) < 0 || len(r.meta.GetEndKey()) == 0)
}

// NextStore picks next store as leader, if out of range return error.
func (r *Region) NextStore() (uint64, error) {
	nextStoreIdx := r.curStoreIdx + 1
	if nextStoreIdx >= len(r.meta.StoreIds) {
		return 0, errors.New("out of range of store")
	}
	return r.meta.StoreIds[nextStoreIdx], nil
}

// regionMissBackoff is for region cache miss retry.
func regionMissBackoff() func() error {
	const (
		maxRetry  = 2
		sleepBase = 1
		sleepCap  = 1
	)
	return NewBackoff(maxRetry, sleepBase, sleepCap, NoJitter)
}
