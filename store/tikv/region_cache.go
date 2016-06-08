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

	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/petar/GoLLRB/llrb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/pd-client"
)

// RegionCache caches Regions loaded from PD.
type RegionCache struct {
	pdClient pd.Client
	mu       sync.RWMutex
	regions  map[RegionVerID]*Region
	sorted   *llrb.LLRB
}

// NewRegionCache creates a RegionCache.
func NewRegionCache(pdClient pd.Client) *RegionCache {
	return &RegionCache{
		pdClient: pdClient,
		regions:  make(map[RegionVerID]*Region),
		sorted:   llrb.New(),
	}
}

// GetRegionByVerID finds a Region by Region's verID.
func (c *RegionCache) GetRegionByVerID(id RegionVerID) *Region {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if r, ok := c.regions[id]; ok {
		return r.Clone()
	}
	return nil
}

// GetRegion find in cache, or get new region.
func (c *RegionCache) GetRegion(key []byte) (*Region, error) {
	c.mu.RLock()
	r := c.getRegionFromCache(key)
	c.mu.RUnlock()
	if r != nil {
		return r, nil
	}
	r, err := c.loadRegion(key)
	if err != nil {
		return nil, errors.Trace(err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.insertRegionToCache(r), nil
}

// GroupKeysByRegion separates keys into groups by their belonging Regions.
// Specially it also returns the first key's region which may be used as the
// 'PrimaryLockKey' and should be committed ahead of others.
func (c *RegionCache) GroupKeysByRegion(keys [][]byte) (map[RegionVerID][][]byte, RegionVerID, error) {
	groups := make(map[RegionVerID][][]byte)
	var first RegionVerID
	var lastRegion *Region
	for i, k := range keys {
		var region *Region
		if lastRegion != nil && lastRegion.Contains(k) {
			region = lastRegion
		} else {
			var err error
			region, err = c.GetRegion(k)
			if err != nil {
				return nil, first, errors.Trace(err)
			}
			lastRegion = region
		}
		id := region.VerID()
		if i == 0 {
			first = id
		}
		groups[id] = append(groups[id], k)
	}
	return groups, first, nil
}

// DropRegion removes a cached Region.
func (c *RegionCache) DropRegion(id RegionVerID) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.dropRegionFromCache(id)
}

// NextPeer picks next peer as new leader, if out of range of peers delete region.
func (c *RegionCache) NextPeer(id RegionVerID) {
	// A and B get the same region and current leader is 1, they both will pick
	// peer 2 as leader.
	region := c.GetRegionByVerID(id)
	if region == nil {
		return
	}
	if leader, err := region.NextPeer(); err != nil {
		c.DropRegion(id)
	} else {
		c.UpdateLeader(id, leader.GetId())
	}
}

// UpdateLeader update some region cache with newer leader info.
func (c *RegionCache) UpdateLeader(regionID RegionVerID, leaderID uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	r, ok := c.regions[regionID]
	if !ok {
		log.Debugf("regionCache: cannot find region when updating leader %d,%d", regionID, leaderID)
		return
	}

	var found bool
	for i, p := range r.meta.Peers {
		if p.GetId() == leaderID {
			r.curPeerIdx, r.peer = i, p
			found = true
			break
		}
	}
	if !found {
		log.Debugf("regionCache: cannot find peer when updating leader %d,%d", regionID, leaderID)
		c.dropRegionFromCache(r.VerID())
		return
	}

	store, err := c.pdClient.GetStore(r.peer.GetStoreId())
	if err != nil {
		log.Warnf("regionCache: failed load store %d", r.peer.GetStoreId())
		c.dropRegionFromCache(r.VerID())
		return
	}

	r.addr = store.GetAddress()
}

func (c *RegionCache) getRegionFromCache(key []byte) *Region {
	var r *Region
	c.sorted.DescendLessOrEqual(newRBSearchItem(key), func(item llrb.Item) bool {
		r = item.(*llrbItem).region
		return false
	})
	if r == nil {
		return nil
	}
	if r.Contains(key) {
		return r.Clone()
	}
	return nil
}

// insertRegionToCache tries to insert the Region to cache. If there is an old
// Region with the same VerID, it will return the old one instead.
func (c *RegionCache) insertRegionToCache(r *Region) *Region {
	if old, ok := c.regions[r.VerID()]; ok {
		return old
	}
	old := c.sorted.ReplaceOrInsert(newRBItem(r))
	if old != nil {
		delete(c.regions, old.(*llrbItem).region.VerID())
	}
	c.regions[r.VerID()] = r
	return r
}

func (c *RegionCache) dropRegionFromCache(verID RegionVerID) {
	r, ok := c.regions[verID]
	if !ok {
		return
	}
	c.sorted.Delete(newRBItem(r))
	delete(c.regions, r.VerID())
}

// loadRegion loads region from pd client, and picks the first peer as leader.
func (c *RegionCache) loadRegion(key []byte) (*Region, error) {
	var region *Region
	var backoffErr error
	for backoff := pdBackoff(); backoffErr == nil; backoffErr = backoff() {
		meta, err := c.pdClient.GetRegion(key)
		if err != nil {
			log.Warnf("loadRegion from PD failed, key: %q, err: %v", key, err)
			continue
		}
		if meta == nil {
			log.Warnf("region not found for key %q", key)
			continue
		}
		if len(meta.Peers) == 0 {
			return nil, errors.New("receive Region with no peer")
		}
		peer := meta.Peers[0]
		store, err := c.pdClient.GetStore(peer.GetStoreId())
		if err != nil {
			log.Warnf("loadStore from PD failed, key %q, storeID: %d, err: %v", key, peer.GetStoreId(), err)
			continue
		}
		region = &Region{
			meta:       meta,
			peer:       peer,
			addr:       store.GetAddress(),
			curPeerIdx: 0,
		}
		break
	}
	if backoffErr != nil {
		return nil, errors.Annotate(backoffErr, txnRetryableMark)
	}
	return region, nil
}

// llrbItem is llrbTree's Item that uses []byte for compare.
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

// Region stores region info. Region is a readonly class.
type Region struct {
	meta       *metapb.Region
	peer       *metapb.Peer
	addr       string
	curPeerIdx int
}

// Clone returns a copy of Region.
func (r *Region) Clone() *Region {
	return &Region{
		meta:       proto.Clone(r.meta).(*metapb.Region),
		peer:       proto.Clone(r.peer).(*metapb.Peer),
		addr:       r.addr,
		curPeerIdx: r.curPeerIdx,
	}
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

// GetAddress returns address.
func (r *Region) GetAddress() string {
	return r.addr
}

// GetContext constructs kvprotopb.Context from region info.
func (r *Region) GetContext() *kvrpcpb.Context {
	return &kvrpcpb.Context{
		RegionId:    r.meta.Id,
		RegionEpoch: r.meta.RegionEpoch,
		Peer:        r.peer,
	}
}

// Contains checks whether the key is in the region, for the maximum region endKey is empty.
// startKey <= key < endKey.
func (r *Region) Contains(key []byte) bool {
	return bytes.Compare(r.meta.GetStartKey(), key) <= 0 &&
		(bytes.Compare(key, r.meta.GetEndKey()) < 0 || len(r.meta.GetEndKey()) == 0)
}

// NextPeer picks next peer as leader, if out of range return error.
func (r *Region) NextPeer() (*metapb.Peer, error) {
	nextPeerIdx := r.curPeerIdx + 1
	if nextPeerIdx >= len(r.meta.Peers) {
		return nil, errors.New("out of range of peer")
	}
	return r.meta.Peers[nextPeerIdx], nil
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

// pdBackoff is for PD RPC retry.
func pdBackoff() func() error {
	const (
		maxRetry  = 10
		sleepBase = 500
		sleepCap  = 3000
	)
	return NewBackoff(maxRetry, sleepBase, sleepCap, EqualJitter)
}
