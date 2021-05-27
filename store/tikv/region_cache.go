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
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/gogo/protobuf/proto"
	"github.com/google/btree"
	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/store/tikv/client"
	"github.com/pingcap/tidb/store/tikv/config"
	"github.com/pingcap/tidb/store/tikv/kv"
	"github.com/pingcap/tidb/store/tikv/logutil"
	"github.com/pingcap/tidb/store/tikv/metrics"
	"github.com/pingcap/tidb/store/tikv/retry"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	pd "github.com/tikv/pd/client"
	atomic2 "go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
)

const (
	btreeDegree               = 32
	invalidatedLastAccessTime = -1
	defaultRegionsPerBatch    = 128
)

// RegionCacheTTLSec is the max idle time for regions in the region cache.
var RegionCacheTTLSec int64 = 600

const (
	updated  int32 = iota // region is updated and no need to reload.
	needSync              // need sync new region info.
)

// InvalidReason is the reason why a cached region is invalidated.
// The region cache may take different strategies to handle different reasons.
// For example, when a cached region is invalidated due to no leader, region cache
// will always access to a different peer.
type InvalidReason int32

const (
	// Ok indicates the cached region is valid
	Ok InvalidReason = iota
	// NoLeader indicates it's invalidated due to no leader
	NoLeader
	// RegionNotFound indicates it's invalidated due to region not found in the store
	RegionNotFound
	// EpochNotMatch indicates it's invalidated due to epoch not match
	EpochNotMatch
	// StoreNotFound indicates it's invalidated due to store not found in PD
	StoreNotFound
	// Other indicates it's invalidated due to other reasons, e.g., the store
	// is removed from the cluster, fail to send requests to the store.
	Other
)

// Region presents kv region
type Region struct {
	meta          *metapb.Region // raw region meta from PD immutable after init
	store         unsafe.Pointer // point to region store info, see RegionStore
	syncFlag      int32          // region need be sync in next turn
	lastAccess    int64          // last region access time, see checkRegionCacheTTL
	invalidReason InvalidReason  // the reason why the region is invalidated
}

// AccessIndex represent the index for accessIndex array
type AccessIndex int

// RegionStore represents region stores info
// it will be store as unsafe.Pointer and be load at once
type RegionStore struct {
	workTiKVIdx    AccessIndex          // point to current work peer in meta.Peers and work store in stores(same idx) for tikv peer
	proxyTiKVIdx   AccessIndex          // point to the tikv peer that can forward requests to the leader. -1 means not using proxy
	workTiFlashIdx int32                // point to current work peer in meta.Peers and work store in stores(same idx) for tiflash peer
	stores         []*Store             // stores in this region
	storeEpochs    []uint32             // snapshots of store's epoch, need reload when `storeEpochs[curr] != stores[cur].fail`
	accessIndex    [NumAccessMode][]int // AccessMode => idx in stores
}

func (r *RegionStore) accessStore(mode AccessMode, idx AccessIndex) (int, *Store) {
	sidx := r.accessIndex[mode][idx]
	return sidx, r.stores[sidx]
}

func (r *RegionStore) getAccessIndex(mode AccessMode, store *Store) AccessIndex {
	for index, sidx := range r.accessIndex[mode] {
		if r.stores[sidx].storeID == store.storeID {
			return AccessIndex(index)
		}
	}
	return -1
}

func (r *RegionStore) accessStoreNum(mode AccessMode) int {
	return len(r.accessIndex[mode])
}

// clone clones region store struct.
func (r *RegionStore) clone() *RegionStore {
	storeEpochs := make([]uint32, len(r.stores))
	rs := &RegionStore{
		workTiFlashIdx: r.workTiFlashIdx,
		proxyTiKVIdx:   r.proxyTiKVIdx,
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
func (r *RegionStore) follower(seed uint32, op *storeSelectorOp) AccessIndex {
	l := uint32(r.accessStoreNum(TiKVOnly))
	if l <= 1 {
		return r.workTiKVIdx
	}

	for retry := l - 1; retry > 0; retry-- {
		followerIdx := AccessIndex(seed % (l - 1))
		if followerIdx >= r.workTiKVIdx {
			followerIdx++
		}
		storeIdx, s := r.accessStore(TiKVOnly, followerIdx)
		if r.storeEpochs[storeIdx] == atomic.LoadUint32(&s.epoch) && r.filterStoreCandidate(followerIdx, op) {
			return followerIdx
		}
		seed++
	}
	return r.workTiKVIdx
}

// return next leader or follower store's index
func (r *RegionStore) kvPeer(seed uint32, op *storeSelectorOp) AccessIndex {
	candidates := make([]AccessIndex, 0, r.accessStoreNum(TiKVOnly))
	for i := 0; i < r.accessStoreNum(TiKVOnly); i++ {
		storeIdx, s := r.accessStore(TiKVOnly, AccessIndex(i))
		if r.storeEpochs[storeIdx] != atomic.LoadUint32(&s.epoch) || !r.filterStoreCandidate(AccessIndex(i), op) {
			continue
		}
		candidates = append(candidates, AccessIndex(i))
	}
	if len(candidates) == 0 {
		return r.workTiKVIdx
	}
	return candidates[seed%uint32(len(candidates))]
}

func (r *RegionStore) filterStoreCandidate(aidx AccessIndex, op *storeSelectorOp) bool {
	_, s := r.accessStore(TiKVOnly, aidx)
	// filter label unmatched store
	return s.IsLabelsMatch(op.labels)
}

// init initializes region after constructed.
func (r *Region) init(bo *Backoffer, c *RegionCache) error {
	// region store pull used store from global store map
	// to avoid acquire storeMu in later access.
	rs := &RegionStore{
		workTiKVIdx:    0,
		proxyTiKVIdx:   -1,
		workTiFlashIdx: 0,
		stores:         make([]*Store, 0, len(r.meta.Peers)),
		storeEpochs:    make([]uint32, 0, len(r.meta.Peers)),
	}
	availablePeers := r.meta.GetPeers()[:0]
	for _, p := range r.meta.Peers {
		c.storeMu.RLock()
		store, exists := c.storeMu.stores[p.StoreId]
		c.storeMu.RUnlock()
		if !exists {
			store = c.getStoreByStoreID(p.StoreId)
		}
		addr, err := store.initResolve(bo, c)
		if err != nil {
			return err
		}
		// Filter the peer on a tombstone store.
		if addr == "" {
			continue
		}
		availablePeers = append(availablePeers, p)
		switch store.storeType {
		case tikvrpc.TiKV:
			rs.accessIndex[TiKVOnly] = append(rs.accessIndex[TiKVOnly], len(rs.stores))
		case tikvrpc.TiFlash:
			rs.accessIndex[TiFlashOnly] = append(rs.accessIndex[TiFlashOnly], len(rs.stores))
		}
		rs.stores = append(rs.stores, store)
		rs.storeEpochs = append(rs.storeEpochs, atomic.LoadUint32(&store.epoch))
	}
	// TODO(youjiali1995): It's possible the region info in PD is stale for now but it can recover.
	// Maybe we need backoff here.
	if len(availablePeers) == 0 {
		return errors.Errorf("no available peers, region: {%v}", r.meta)
	}
	r.meta.Peers = availablePeers

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
	// Only consider use percentage on this failpoint, for example, "2%return"
	failpoint.Inject("invalidateRegionCache", func() {
		r.invalidate(Other)
	})
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
func (r *Region) invalidate(reason InvalidReason) {
	metrics.RegionCacheCounterWithInvalidateRegionFromCacheOK.Inc()
	atomic.StoreInt32((*int32)(&r.invalidReason), int32(reason))
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

// checkNeedReloadAndMarkUpdated returns whether the region need reload and marks the region to be updated.
func (r *Region) checkNeedReloadAndMarkUpdated() bool {
	oldValue := atomic.LoadInt32(&r.syncFlag)
	if oldValue == updated {
		return false
	}
	return atomic.CompareAndSwapInt32(&r.syncFlag, oldValue, updated)
}

func (r *Region) checkNeedReload() bool {
	v := atomic.LoadInt32(&r.syncFlag)
	return v != updated
}

// RegionCache caches Regions loaded from PD.
type RegionCache struct {
	pdClient         pd.Client
	enableForwarding bool

	mu struct {
		sync.RWMutex                           // mutex protect cached region
		regions        map[RegionVerID]*Region // cached regions are organized as regionVerID to region ref mapping
		latestVersions map[uint64]RegionVerID  // cache the map from regionID to its latest RegionVerID
		sorted         *btree.BTree            // cache regions are organized as sorted key to region ref mapping
	}
	storeMu struct {
		sync.RWMutex
		stores map[uint64]*Store
	}
	notifyCheckCh chan struct{}
	closeCh       chan struct{}

	testingKnobs struct {
		// Replace the requestLiveness function for test purpose. Note that in unit tests, if this is not set,
		// requestLiveness always returns unreachable.
		mockRequestLiveness func(s *Store, bo *Backoffer) livenessState
	}
}

// NewRegionCache creates a RegionCache.
func NewRegionCache(pdClient pd.Client) *RegionCache {
	c := &RegionCache{
		pdClient: pdClient,
	}
	c.mu.regions = make(map[RegionVerID]*Region)
	c.mu.latestVersions = make(map[uint64]RegionVerID)
	c.mu.sorted = btree.New(btreeDegree)
	c.storeMu.stores = make(map[uint64]*Store)
	c.notifyCheckCh = make(chan struct{}, 1)
	c.closeCh = make(chan struct{})
	interval := config.GetGlobalConfig().StoresRefreshInterval
	go c.asyncCheckAndResolveLoop(time.Duration(interval) * time.Second)
	c.enableForwarding = config.GetGlobalConfig().EnableForwarding
	return c
}

// clear clears all cached data in the RegionCache. It's only used in tests.
func (c *RegionCache) clear() {
	c.mu.Lock()
	c.mu.regions = make(map[RegionVerID]*Region)
	c.mu.latestVersions = make(map[uint64]RegionVerID)
	c.mu.sorted = btree.New(btreeDegree)
	c.mu.Unlock()
	c.storeMu.Lock()
	c.storeMu.stores = make(map[uint64]*Store)
	c.storeMu.Unlock()
}

// Close releases region cache's resource.
func (c *RegionCache) Close() {
	close(c.closeCh)
}

// asyncCheckAndResolveLoop with
func (c *RegionCache) asyncCheckAndResolveLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	var needCheckStores []*Store
	for {
		needCheckStores = needCheckStores[:0]
		select {
		case <-c.closeCh:
			return
		case <-c.notifyCheckCh:
			c.checkAndResolve(needCheckStores, func(s *Store) bool {
				return s.getResolveState() == needCheck
			})
		case <-ticker.C:
			// refresh store to update labels.
			c.checkAndResolve(needCheckStores, func(s *Store) bool {
				state := s.getResolveState()
				// Only valid stores should be reResolved. In fact, it's impossible
				// there's a deleted store in the stores map which guaranteed by reReslve().
				return state != unresolved && state != tombstone && state != deleted
			})
		}
	}
}

// checkAndResolve checks and resolve addr of failed stores.
// this method isn't thread-safe and only be used by one goroutine.
func (c *RegionCache) checkAndResolve(needCheckStores []*Store, needCheck func(*Store) bool) {
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
		if needCheck(store) {
			needCheckStores = append(needCheckStores, store)
		}
	}
	c.storeMu.RUnlock()

	for _, store := range needCheckStores {
		_, err := store.reResolve(c)
		terror.Log(err)
	}
}

// RPCContext contains data that is needed to send RPC to a region.
type RPCContext struct {
	Region         RegionVerID
	Meta           *metapb.Region
	Peer           *metapb.Peer
	AccessIdx      AccessIndex
	Store          *Store
	Addr           string
	AccessMode     AccessMode
	ProxyStore     *Store      // nil means proxy is not used
	ProxyAccessIdx AccessIndex // valid when ProxyStore is not nil
	ProxyAddr      string      // valid when ProxyStore is not nil
	TiKVNum        int         // Number of TiKV nodes among the region's peers. Assuming non-TiKV peers are all TiFlash peers.
}

func (c *RPCContext) String() string {
	var runStoreType string
	if c.Store != nil {
		runStoreType = c.Store.storeType.Name()
	}
	res := fmt.Sprintf("region ID: %d, meta: %s, peer: %s, addr: %s, idx: %d, reqStoreType: %s, runStoreType: %s",
		c.Region.GetID(), c.Meta, c.Peer, c.Addr, c.AccessIdx, c.AccessMode, runStoreType)
	if c.ProxyStore != nil {
		res += fmt.Sprintf(", proxy store id: %d, proxy addr: %s, proxy idx: %d", c.ProxyStore.storeID, c.ProxyAddr, c.ProxyAccessIdx)
	}
	return res
}

type storeSelectorOp struct {
	labels []*metapb.StoreLabel
}

// StoreSelectorOption configures storeSelectorOp.
type StoreSelectorOption func(*storeSelectorOp)

// WithMatchLabels indicates selecting stores with matched labels
func WithMatchLabels(labels []*metapb.StoreLabel) StoreSelectorOption {
	return func(op *storeSelectorOp) {
		op.labels = labels
	}
}

// GetTiKVRPCContext returns RPCContext for a region. If it returns nil, the region
// must be out of date and already dropped from cache.
func (c *RegionCache) GetTiKVRPCContext(bo *Backoffer, id RegionVerID, replicaRead kv.ReplicaReadType, followerStoreSeed uint32, opts ...StoreSelectorOption) (*RPCContext, error) {
	ts := time.Now().Unix()

	cachedRegion := c.getCachedRegionWithRLock(id)
	if cachedRegion == nil {
		return nil, nil
	}

	if cachedRegion.checkNeedReload() {
		// TODO: This may cause a fake EpochNotMatch error, and reload the region after a backoff. It's better to reload
		// the region directly here.
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
	options := &storeSelectorOp{}
	for _, op := range opts {
		op(options)
	}
	failpoint.Inject("assertStoreLabels", func(val failpoint.Value) {
		if len(opts) > 0 {
			kv := strings.Split(val.(string), "_")
			for _, label := range options.labels {
				if label.Key == kv[0] && label.Value != kv[1] {
					panic(fmt.Sprintf("StoreSelectorOption's label %v is not %v", kv[0], kv[1]))
				}
			}
		}
	})
	isLeaderReq := false
	switch replicaRead {
	case kv.ReplicaReadFollower:
		store, peer, accessIdx, storeIdx = cachedRegion.FollowerStorePeer(regionStore, followerStoreSeed, options)
	case kv.ReplicaReadMixed:
		store, peer, accessIdx, storeIdx = cachedRegion.AnyStorePeer(regionStore, followerStoreSeed, options)
	default:
		isLeaderReq = true
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
		cachedRegion.invalidate(StoreNotFound)
		return nil, nil
	}

	storeFailEpoch := atomic.LoadUint32(&store.epoch)
	if storeFailEpoch != regionStore.storeEpochs[storeIdx] {
		cachedRegion.invalidate(Other)
		logutil.BgLogger().Info("invalidate current region, because others failed on same store",
			zap.Uint64("region", id.GetID()),
			zap.String("store", store.addr))
		return nil, nil
	}

	var (
		proxyStore     *Store
		proxyAddr      string
		proxyAccessIdx AccessIndex
		proxyStoreIdx  int
	)
	if c.enableForwarding && isLeaderReq {
		if atomic.LoadInt32(&store.needForwarding) == 0 {
			regionStore.unsetProxyStoreIfNeeded(cachedRegion)
		} else {
			proxyStore, proxyAccessIdx, proxyStoreIdx = c.getProxyStore(cachedRegion, store, regionStore, accessIdx)
			if proxyStore != nil {
				proxyAddr, err = c.getStoreAddr(bo, cachedRegion, proxyStore, proxyStoreIdx)
				if err != nil {
					return nil, err
				}
			}
		}
	}

	return &RPCContext{
		Region:         id,
		Meta:           cachedRegion.meta,
		Peer:           peer,
		AccessIdx:      accessIdx,
		Store:          store,
		Addr:           addr,
		AccessMode:     TiKVOnly,
		ProxyStore:     proxyStore,
		ProxyAccessIdx: proxyAccessIdx,
		ProxyAddr:      proxyAddr,
		TiKVNum:        regionStore.accessStoreNum(TiKVOnly),
	}, nil
}

// GetAllValidTiFlashStores returns the store ids of all valid TiFlash stores, the store id of currentStore is always the first one
func (c *RegionCache) GetAllValidTiFlashStores(id RegionVerID, currentStore *Store) []uint64 {
	// set the cap to 2 because usually, TiFlash table will have 2 replicas
	allStores := make([]uint64, 0, 2)
	// make sure currentStore id is always the first in allStores
	allStores = append(allStores, currentStore.storeID)
	ts := time.Now().Unix()
	cachedRegion := c.getCachedRegionWithRLock(id)
	if cachedRegion == nil {
		return allStores
	}
	if !cachedRegion.checkRegionCacheTTL(ts) {
		return allStores
	}
	regionStore := cachedRegion.getStore()
	currentIndex := regionStore.getAccessIndex(TiFlashOnly, currentStore)
	if currentIndex == -1 {
		return allStores
	}
	for startOffset := 1; startOffset < regionStore.accessStoreNum(TiFlashOnly); startOffset++ {
		accessIdx := AccessIndex((int(currentIndex) + startOffset) % regionStore.accessStoreNum(TiFlashOnly))
		storeIdx, store := regionStore.accessStore(TiFlashOnly, accessIdx)
		if store.getResolveState() == needCheck {
			continue
		}
		storeFailEpoch := atomic.LoadUint32(&store.epoch)
		if storeFailEpoch != regionStore.storeEpochs[storeIdx] {
			continue
		}
		allStores = append(allStores, store.storeID)
	}
	return allStores
}

// GetTiFlashRPCContext returns RPCContext for a region must access flash store. If it returns nil, the region
// must be out of date and already dropped from cache or not flash store found.
// `loadBalance` is an option. For MPP and batch cop, it is pointless and might cause try the failed store repeatly.
func (c *RegionCache) GetTiFlashRPCContext(bo *Backoffer, id RegionVerID, loadBalance bool) (*RPCContext, error) {
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
	var sIdx int
	if loadBalance {
		sIdx = int(atomic.AddInt32(&regionStore.workTiFlashIdx, 1))
	} else {
		sIdx = int(atomic.LoadInt32(&regionStore.workTiFlashIdx))
	}
	for i := 0; i < regionStore.accessStoreNum(TiFlashOnly); i++ {
		accessIdx := AccessIndex((sIdx + i) % regionStore.accessStoreNum(TiFlashOnly))
		storeIdx, store := regionStore.accessStore(TiFlashOnly, accessIdx)
		addr, err := c.getStoreAddr(bo, cachedRegion, store, storeIdx)
		if err != nil {
			return nil, err
		}
		if len(addr) == 0 {
			cachedRegion.invalidate(StoreNotFound)
			return nil, nil
		}
		if store.getResolveState() == needCheck {
			_, err := store.reResolve(c)
			terror.Log(err)
		}
		atomic.StoreInt32(&regionStore.workTiFlashIdx, int32(accessIdx))
		peer := cachedRegion.meta.Peers[storeIdx]
		storeFailEpoch := atomic.LoadUint32(&store.epoch)
		if storeFailEpoch != regionStore.storeEpochs[storeIdx] {
			cachedRegion.invalidate(Other)
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
			TiKVNum:    regionStore.accessStoreNum(TiKVOnly),
		}, nil
	}

	cachedRegion.invalidate(Other)
	return nil, nil
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

// String implements fmt.Stringer interface.
func (l *KeyLocation) String() string {
	return fmt.Sprintf("region %s,startKey:%s,endKey:%s", l.Region.String(), kv.StrKey(l.StartKey), kv.StrKey(l.EndKey))
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

// SplitRegionRanges gets the split ranges from pd region.
func (c *RegionCache) SplitRegionRanges(bo *Backoffer, keyRanges []kv.KeyRange) ([]kv.KeyRange, error) {
	ranges := NewKeyRanges(keyRanges)

	locations, err := c.SplitKeyRangesByLocations(bo, ranges)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var ret []kv.KeyRange
	for _, loc := range locations {
		for i := 0; i < loc.Ranges.Len(); i++ {
			ret = append(ret, loc.Ranges.At(i))
		}
	}
	return ret, nil
}

// LocationKeyRanges wrapps a real Location in PD and its logical ranges info.
type LocationKeyRanges struct {
	// Location is the real location in PD.
	Location *KeyLocation
	// Ranges is the logic ranges the current Location contains.
	Ranges *KeyRanges
}

// SplitKeyRangesByLocations splits the KeyRanges by logical info in the cache.
func (c *RegionCache) SplitKeyRangesByLocations(bo *Backoffer, ranges *KeyRanges) ([]*LocationKeyRanges, error) {
	res := make([]*LocationKeyRanges, 0)
	for ranges.Len() > 0 {
		loc, err := c.LocateKey(bo, ranges.At(0).StartKey)
		if err != nil {
			return res, errors.Trace(err)
		}

		// Iterate to the first range that is not complete in the region.
		var i int
		for ; i < ranges.Len(); i++ {
			r := ranges.At(i)
			if !(loc.Contains(r.EndKey) || bytes.Equal(loc.EndKey, r.EndKey)) {
				break
			}
		}
		// All rest ranges belong to the same region.
		if i == ranges.Len() {
			res = append(res, &LocationKeyRanges{Location: loc, Ranges: ranges})
			break
		}

		r := ranges.At(i)
		if loc.Contains(r.StartKey) {
			// Part of r is not in the region. We need to split it.
			taskRanges := ranges.Slice(0, i)
			taskRanges.last = &kv.KeyRange{
				StartKey: r.StartKey,
				EndKey:   loc.EndKey,
			}
			res = append(res, &LocationKeyRanges{Location: loc, Ranges: taskRanges})

			ranges = ranges.Slice(i+1, ranges.Len())
			ranges.first = &kv.KeyRange{
				StartKey: loc.EndKey,
				EndKey:   r.EndKey,
			}
		} else {
			// rs[i] is not in the region.
			taskRanges := ranges.Slice(0, i)
			res = append(res, &LocationKeyRanges{Location: loc, Ranges: taskRanges})
			ranges = ranges.Slice(i, ranges.Len())
		}
	}

	return res, nil
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
		logutil.Eventf(bo.GetCtx(), "load region %d from pd, due to cache-miss", lr.GetID())
		r = lr
		c.mu.Lock()
		c.insertRegionToCache(r)
		c.mu.Unlock()
	} else if r.checkNeedReloadAndMarkUpdated() {
		// load region when it be marked as need reload.
		lr, err := c.loadRegion(bo, key, isEndKey)
		if err != nil {
			// ignore error and use old region info.
			logutil.Logger(bo.GetCtx()).Error("load region failure",
				zap.ByteString("key", key), zap.Error(err))
		} else {
			logutil.Eventf(bo.GetCtx(), "load region %d from pd, due to need-reload", lr.GetID())
			r = lr
			c.mu.Lock()
			c.insertRegionToCache(r)
			c.mu.Unlock()
		}
	}
	return r, nil
}

// OnSendFailForBatchRegions handles send request fail logic.
func (c *RegionCache) OnSendFailForBatchRegions(bo *Backoffer, store *Store, regionInfos []RegionInfo, scheduleReload bool, err error) {
	metrics.RegionCacheCounterWithSendFail.Add(float64(len(regionInfos)))
	if store.storeType != tikvrpc.TiFlash {
		logutil.Logger(bo.GetCtx()).Info("Should not reach here, OnSendFailForBatchRegions only support TiFlash")
		return
	}
	for _, ri := range regionInfos {
		if ri.Meta == nil {
			continue
		}
		r := c.getCachedRegionWithRLock(ri.Region)
		if r != nil {
			peersNum := len(r.meta.Peers)
			if len(ri.Meta.Peers) != peersNum {
				logutil.Logger(bo.GetCtx()).Info("retry and refresh current region after send request fail and up/down stores length changed",
					zap.Stringer("region", &ri.Region),
					zap.Bool("needReload", scheduleReload),
					zap.Reflect("oldPeers", ri.Meta.Peers),
					zap.Reflect("newPeers", r.meta.Peers),
					zap.Error(err))
				continue
			}

			rs := r.getStore()

			accessMode := TiFlashOnly
			accessIdx := rs.getAccessIndex(accessMode, store)
			if accessIdx == -1 {
				logutil.Logger(bo.GetCtx()).Warn("can not get access index for region " + ri.Region.String())
				continue
			}
			if err != nil {
				storeIdx, s := rs.accessStore(accessMode, accessIdx)
				epoch := rs.storeEpochs[storeIdx]
				if atomic.CompareAndSwapUint32(&s.epoch, epoch, epoch+1) {
					logutil.BgLogger().Info("mark store's regions need be refill", zap.String("store", s.addr))
					metrics.RegionCacheCounterWithInvalidateStoreRegionsOK.Inc()
				}
				// schedule a store addr resolve.
				s.markNeedCheck(c.notifyCheckCh)
			}

			// try next peer
			rs.switchNextFlashPeer(r, accessIdx)
			logutil.Logger(bo.GetCtx()).Info("switch region tiflash peer to next due to send request fail",
				zap.Stringer("region", &ri.Region),
				zap.Bool("needReload", scheduleReload),
				zap.Error(err))

			// force reload region when retry all known peers in region.
			if scheduleReload {
				r.scheduleReload()
			}
		}
	}
}

// OnSendFail handles send request fail logic.
func (c *RegionCache) OnSendFail(bo *Backoffer, ctx *RPCContext, scheduleReload bool, err error) {
	metrics.RegionCacheCounterWithSendFail.Inc()
	r := c.getCachedRegionWithRLock(ctx.Region)
	if r != nil {
		peersNum := len(r.meta.Peers)
		if len(ctx.Meta.Peers) != peersNum {
			logutil.Logger(bo.GetCtx()).Info("retry and refresh current ctx after send request fail and up/down stores length changed",
				zap.Stringer("current", ctx),
				zap.Bool("needReload", scheduleReload),
				zap.Reflect("oldPeers", ctx.Meta.Peers),
				zap.Reflect("newPeers", r.meta.Peers),
				zap.Error(err))
			return
		}

		rs := r.getStore()
		startForwarding := false
		incEpochStoreIdx := -1

		if err != nil {
			storeIdx, s := rs.accessStore(ctx.AccessMode, ctx.AccessIdx)
			leaderReq := ctx.Store.storeType == tikvrpc.TiKV && rs.workTiKVIdx == ctx.AccessIdx

			//  Mark the store as failure if it's not a redirection request because we
			//  can't know the status of the proxy store by it.
			if ctx.ProxyStore == nil {
				// send fail but store is reachable, keep retry current peer for replica leader request.
				// but we still need switch peer for follower-read or learner-read(i.e. tiflash)
				if leaderReq {
					if s.requestLiveness(bo, c) == reachable {
						return
					} else if c.enableForwarding {
						s.startHealthCheckLoopIfNeeded(c)
						startForwarding = true
					}
				}

				// invalidate regions in store.
				epoch := rs.storeEpochs[storeIdx]
				if atomic.CompareAndSwapUint32(&s.epoch, epoch, epoch+1) {
					logutil.BgLogger().Info("mark store's regions need be refill", zap.String("store", s.addr))
					incEpochStoreIdx = storeIdx
					metrics.RegionCacheCounterWithInvalidateStoreRegionsOK.Inc()
				}
				// schedule a store addr resolve.
				s.markNeedCheck(c.notifyCheckCh)
			}
		}

		// try next peer to found new leader.
		if ctx.AccessMode == TiKVOnly {
			if startForwarding || ctx.ProxyStore != nil {
				var currentProxyIdx AccessIndex = -1
				if ctx.ProxyStore != nil {
					currentProxyIdx = ctx.ProxyAccessIdx
				}
				// In case the epoch of the store is increased, try to avoid reloading the current region by also
				// increasing the epoch stored in `rs`.
				rs.switchNextProxyStore(r, currentProxyIdx, incEpochStoreIdx)
				logutil.Logger(bo.GetCtx()).Info("switch region proxy peer to next due to send request fail",
					zap.Stringer("current", ctx),
					zap.Bool("needReload", scheduleReload),
					zap.Error(err))
			} else {
				rs.switchNextTiKVPeer(r, ctx.AccessIdx)
				logutil.Logger(bo.GetCtx()).Info("switch region peer to next due to send request fail",
					zap.Stringer("current", ctx),
					zap.Bool("needReload", scheduleReload),
					zap.Error(err))
			}
		} else {
			rs.switchNextFlashPeer(r, ctx.AccessIdx)
			logutil.Logger(bo.GetCtx()).Info("switch region tiflash peer to next due to send request fail",
				zap.Stringer("current", ctx),
				zap.Bool("needReload", scheduleReload),
				zap.Error(err))
		}

		// force reload region when retry all known peers in region.
		if scheduleReload {
			r.scheduleReload()
		}
	}
}

// LocateRegionByID searches for the region with ID.
func (c *RegionCache) LocateRegionByID(bo *Backoffer, regionID uint64) (*KeyLocation, error) {
	c.mu.RLock()
	r := c.getRegionByIDFromCache(regionID)
	c.mu.RUnlock()
	if r != nil {
		if r.checkNeedReloadAndMarkUpdated() {
			lr, err := c.loadRegionByID(bo, regionID)
			if err != nil {
				// ignore error and use old region info.
				logutil.Logger(bo.GetCtx()).Error("load region failure",
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

// groupSortedMutationsByRegion separates keys into groups by their belonging Regions.
func (c *RegionCache) groupSortedMutationsByRegion(bo *Backoffer, m CommitterMutations) ([]groupedMutations, error) {
	var (
		groups  []groupedMutations
		lastLoc *KeyLocation
	)
	lastUpperBound := 0
	for i := 0; i < m.Len(); i++ {
		if lastLoc == nil || !lastLoc.Contains(m.GetKey(i)) {
			if lastLoc != nil {
				groups = append(groups, groupedMutations{
					region:    lastLoc.Region,
					mutations: m.Slice(lastUpperBound, i),
				})
				lastUpperBound = i
			}
			var err error
			lastLoc, err = c.LocateKey(bo, m.GetKey(i))
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
	}
	if lastLoc != nil {
		groups = append(groups, groupedMutations{
			region:    lastLoc.Region,
			mutations: m.Slice(lastUpperBound, m.Len()),
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
		if endRegion.ContainsByEnd(endKey) {
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
	c.InvalidateCachedRegionWithReason(id, Other)
}

// InvalidateCachedRegionWithReason removes a cached Region with the reason why it's invalidated.
func (c *RegionCache) InvalidateCachedRegionWithReason(id RegionVerID, reason InvalidReason) {
	cachedRegion := c.getCachedRegionWithRLock(id)
	if cachedRegion == nil {
		return
	}
	cachedRegion.invalidate(reason)
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
		r.invalidate(StoreNotFound)
	} else {
		logutil.BgLogger().Info("switch region leader to specific leader due to kv return NotLeader",
			zap.Uint64("regionID", regionID.GetID()),
			zap.Int("currIdx", int(currentPeerIdx)),
			zap.Uint64("leaderStoreID", leaderStoreID))
	}
}

// removeVersionFromCache removes a RegionVerID from cache, tries to cleanup
// both c.mu.regions and c.mu.versions. Note this function is not thread-safe.
func (c *RegionCache) removeVersionFromCache(oldVer RegionVerID, regionID uint64) {
	delete(c.mu.regions, oldVer)
	if ver, ok := c.mu.latestVersions[regionID]; ok && ver.Equals(oldVer) {
		delete(c.mu.latestVersions, regionID)
	}
}

// insertRegionToCache tries to insert the Region to cache.
// It should be protected by c.mu.Lock().
func (c *RegionCache) insertRegionToCache(cachedRegion *Region) {
	old := c.mu.sorted.ReplaceOrInsert(newBtreeItem(cachedRegion))
	if old != nil {
		store := cachedRegion.getStore()
		oldRegion := old.(*btreeItem).cachedRegion
		oldRegionStore := oldRegion.getStore()
		// Joint consensus is enabled in v5.0, which is possible to make a leader step down as a learner during a conf change.
		// And if hibernate region is enabled, after the leader step down, there can be a long time that there is no leader
		// in the region and the leader info in PD is stale until requests are sent to followers or hibernate timeout.
		// To solve it, one solution is always to try a different peer if the invalid reason of the old cached region is no-leader.
		// There is a small probability that the current peer who reports no-leader becomes a leader and TiDB has to retry once in this case.
		if InvalidReason(atomic.LoadInt32((*int32)(&oldRegion.invalidReason))) == NoLeader {
			store.workTiKVIdx = (oldRegionStore.workTiKVIdx + 1) % AccessIndex(store.accessStoreNum(TiKVOnly))
		}
		// Don't refresh TiFlash work idx for region. Otherwise, it will always goto a invalid store which
		// is under transferring regions.
		store.workTiFlashIdx = atomic.LoadInt32(&oldRegionStore.workTiFlashIdx)
		c.removeVersionFromCache(oldRegion.VerID(), cachedRegion.VerID().id)
	}
	c.mu.regions[cachedRegion.VerID()] = cachedRegion
	newVer := cachedRegion.VerID()
	latest, ok := c.mu.latestVersions[cachedRegion.VerID().id]
	if !ok || latest.GetVer() < newVer.GetVer() || latest.GetConfVer() < newVer.GetConfVer() {
		c.mu.latestVersions[cachedRegion.VerID().id] = newVer
	}
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
	ts := time.Now().Unix()
	ver, ok := c.mu.latestVersions[regionID]
	if !ok {
		return nil
	}
	latestRegion, ok := c.mu.regions[ver]
	if !ok {
		// should not happen
		logutil.BgLogger().Warn("region version not found",
			zap.Uint64("regionID", regionID), zap.Stringer("version", &ver))
		return nil
	}
	lastAccess := atomic.LoadInt64(&latestRegion.lastAccess)
	if ts-lastAccess > RegionCacheTTLSec {
		return nil
	}
	if latestRegion != nil {
		atomic.CompareAndSwapInt64(&latestRegion.lastAccess, atomic.LoadInt64(&latestRegion.lastAccess), ts)
	}
	return latestRegion
}

// TODO: revise it by get store by closure.
func (c *RegionCache) getStoresByType(typ tikvrpc.EndpointType) []*Store {
	c.storeMu.Lock()
	defer c.storeMu.Unlock()
	stores := make([]*Store, 0)
	for _, store := range c.storeMu.stores {
		if store.getResolveState() != resolved {
			continue
		}
		if store.storeType == typ {
			//TODO: revise it with store.clone()
			storeLabel := make([]*metapb.StoreLabel, 0)
			for _, label := range store.labels {
				storeLabel = append(storeLabel, &metapb.StoreLabel{
					Key:   label.Key,
					Value: label.Value,
				})
			}
			stores = append(stores, &Store{
				addr:    store.addr,
				storeID: store.storeID,
				labels:  store.labels,
			})
		}
	}
	return stores
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
	region.Meta.Peers = new
}

// loadRegion loads region from pd client, and picks the first peer as leader.
// If the given key is the end key of the region that you want, you may set the second argument to true. This is useful
// when processing in reverse order.
func (c *RegionCache) loadRegion(bo *Backoffer, key []byte, isEndKey bool) (*Region, error) {
	ctx := bo.GetCtx()
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("loadRegion", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	var backoffErr error
	searchPrev := false
	for {
		if backoffErr != nil {
			err := bo.Backoff(retry.BoPDRPC, backoffErr)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		var reg *pd.Region
		var err error
		if searchPrev {
			reg, err = c.pdClient.GetPrevRegion(ctx, key)
		} else {
			reg, err = c.pdClient.GetRegion(ctx, key)
		}
		if err != nil {
			metrics.RegionCacheCounterWithGetRegionError.Inc()
		} else {
			metrics.RegionCacheCounterWithGetRegionOK.Inc()
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
		err = region.init(bo, c)
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
	ctx := bo.GetCtx()
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("loadRegionByID", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}
	var backoffErr error
	for {
		if backoffErr != nil {
			err := bo.Backoff(retry.BoPDRPC, backoffErr)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		reg, err := c.pdClient.GetRegionByID(ctx, regionID)
		if err != nil {
			metrics.RegionCacheCounterWithGetRegionByIDError.Inc()
		} else {
			metrics.RegionCacheCounterWithGetRegionByIDOK.Inc()
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
		err = region.init(bo, c)
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
	ctx := bo.GetCtx()
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("scanRegions", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	var backoffErr error
	for {
		if backoffErr != nil {
			err := bo.Backoff(retry.BoPDRPC, backoffErr)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		regionsInfo, err := c.pdClient.ScanRegions(ctx, startKey, endKey, limit)
		if err != nil {
			metrics.RegionCacheCounterWithScanRegionsError.Inc()
			backoffErr = errors.Errorf(
				"scanRegion from PD failed, startKey: %q, limit: %q, err: %v",
				startKey,
				limit,
				err)
			continue
		}

		metrics.RegionCacheCounterWithScanRegionsOK.Inc()

		if len(regionsInfo) == 0 {
			return nil, errors.New("PD returned no region")
		}
		regions := make([]*Region, 0, len(regionsInfo))
		for _, r := range regionsInfo {
			region := &Region{meta: r.Meta}
			err := region.init(bo, c)
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
	case tombstone:
		return "", nil
	default:
		panic("unsupported resolve state")
	}
}

func (c *RegionCache) getProxyStore(region *Region, store *Store, rs *RegionStore, workStoreIdx AccessIndex) (proxyStore *Store, proxyAccessIdx AccessIndex, proxyStoreIdx int) {
	if !c.enableForwarding || store.storeType != tikvrpc.TiKV || atomic.LoadInt32(&store.needForwarding) == 0 {
		return
	}

	if rs.proxyTiKVIdx >= 0 {
		storeIdx, proxyStore := rs.accessStore(TiKVOnly, rs.proxyTiKVIdx)
		return proxyStore, rs.proxyTiKVIdx, storeIdx
	}

	tikvNum := rs.accessStoreNum(TiKVOnly)
	if tikvNum <= 1 {
		return
	}

	// Randomly select an non-leader peer
	first := rand.Intn(tikvNum - 1)
	if first >= int(workStoreIdx) {
		first = (first + 1) % tikvNum
	}

	// If the current selected peer is not reachable, switch to the next one, until a reachable peer is found or all
	// peers are checked.
	for i := 0; i < tikvNum; i++ {
		index := (i + first) % tikvNum
		// Skip work store which is the actual store to be accessed
		if index == int(workStoreIdx) {
			continue
		}
		storeIdx, store := rs.accessStore(TiKVOnly, AccessIndex(index))
		// Skip unreachable stores.
		if atomic.LoadInt32(&store.needForwarding) != 0 {
			continue
		}

		rs.setProxyStoreIdx(region, AccessIndex(index))
		return store, AccessIndex(index), storeIdx
	}

	return nil, 0, 0
}

// changeToActiveStore replace the deleted store in the region by an up-to-date store in the stores map.
// The order is guaranteed by reResolve() which adds the new store before marking old store deleted.
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

func (c *RegionCache) getStoresByLabels(labels []*metapb.StoreLabel) []*Store {
	c.storeMu.RLock()
	defer c.storeMu.RUnlock()
	s := make([]*Store, 0)
	for _, store := range c.storeMu.stores {
		if store.IsLabelsMatch(labels) {
			s = append(s, store)
		}
	}
	return s
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
			return bo.Backoff(retry.BoRegionMiss, err)
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	needInvalidateOld := true
	// If the region epoch is not ahead of TiKV's, replace region meta in region cache.
	for _, meta := range currentRegions {
		if _, ok := c.pdClient.(*CodecPDClient); ok {
			var err error
			if meta, err = decodeRegionMetaKeyWithShallowCopy(meta); err != nil {
				return errors.Errorf("newRegion's range key is not encoded: %v, %v", meta, err)
			}
		}
		region := &Region{meta: meta}
		err := region.init(bo, c)
		if err != nil {
			return err
		}
		var initLeader uint64
		if ctx.Store.storeType == tikvrpc.TiFlash {
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
			cachedRegion.invalidate(EpochNotMatch)
		}
	}
	return nil
}

// PDClient returns the pd.Client in RegionCache.
func (c *RegionCache) PDClient() pd.Client {
	return c.pdClient
}

// GetTiFlashStoreAddrs returns addresses of all tiflash nodes.
func (c *RegionCache) GetTiFlashStoreAddrs() []string {
	c.storeMu.RLock()
	defer c.storeMu.RUnlock()
	var addrs []string
	for _, s := range c.storeMu.stores {
		if s.storeType == tikvrpc.TiFlash {
			addrs = append(addrs, s.addr)
		}
	}
	return addrs
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
	if int(store.workTiKVIdx) >= store.accessStoreNum(TiKVOnly) {
		return 0
	}
	storeIdx, _ := store.accessStore(TiKVOnly, store.workTiKVIdx)
	return r.meta.Peers[storeIdx].Id
}

// GetLeaderStoreID returns the store ID of the leader region.
func (r *Region) GetLeaderStoreID() uint64 {
	store := r.getStore()
	if int(store.workTiKVIdx) >= store.accessStoreNum(TiKVOnly) {
		return 0
	}
	storeIdx, _ := store.accessStore(TiKVOnly, store.workTiKVIdx)
	return r.meta.Peers[storeIdx].StoreId
}

func (r *Region) getKvStorePeer(rs *RegionStore, aidx AccessIndex) (store *Store, peer *metapb.Peer, accessIdx AccessIndex, storeIdx int) {
	storeIdx, store = rs.accessStore(TiKVOnly, aidx)
	peer = r.meta.Peers[storeIdx]
	accessIdx = aidx
	return
}

// WorkStorePeer returns current work store with work peer.
func (r *Region) WorkStorePeer(rs *RegionStore) (store *Store, peer *metapb.Peer, accessIdx AccessIndex, storeIdx int) {
	return r.getKvStorePeer(rs, rs.workTiKVIdx)
}

// FollowerStorePeer returns a follower store with follower peer.
func (r *Region) FollowerStorePeer(rs *RegionStore, followerStoreSeed uint32, op *storeSelectorOp) (store *Store, peer *metapb.Peer, accessIdx AccessIndex, storeIdx int) {
	return r.getKvStorePeer(rs, rs.follower(followerStoreSeed, op))
}

// AnyStorePeer returns a leader or follower store with the associated peer.
func (r *Region) AnyStorePeer(rs *RegionStore, followerStoreSeed uint32, op *storeSelectorOp) (store *Store, peer *metapb.Peer, accessIdx AccessIndex, storeIdx int) {
	return r.getKvStorePeer(rs, rs.kvPeer(followerStoreSeed, op))
}

// RegionVerID is a unique ID that can identify a Region at a specific version.
type RegionVerID struct {
	id      uint64
	confVer uint64
	ver     uint64
}

// NewRegionVerID creates a region ver id, which used for invalidating regions.
func NewRegionVerID(id, confVer, ver uint64) RegionVerID {
	return RegionVerID{id, confVer, ver}
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

// String formats the RegionVerID to string
func (r *RegionVerID) String() string {
	return fmt.Sprintf("{ region id: %v, ver: %v, confVer: %v }", r.id, r.ver, r.confVer)
}

// Equals checks whether the RegionVerID equals to another one
func (r *RegionVerID) Equals(another RegionVerID) bool {
	return r.id == another.id && r.confVer == another.confVer && r.ver == another.ver
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
	for i, gIdx := range oldRegionStore.accessIndex[TiKVOnly] {
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
	nextIdx := (currentPeerIdx + 1) % AccessIndex(r.accessStoreNum(TiKVOnly))
	newRegionStore := r.clone()
	newRegionStore.workTiKVIdx = nextIdx
	rr.compareAndSwapStore(r, newRegionStore)
}

// switchNextProxyStore switches the index of the peer that will forward requests to the leader to the next peer.
// If proxy is currently not used on this region, the value of `currentProxyIdx` should be -1, and a random peer will
// be select in this case.
func (r *RegionStore) switchNextProxyStore(rr *Region, currentProxyIdx AccessIndex, incEpochStoreIdx int) {
	if r.proxyTiKVIdx != currentProxyIdx {
		return
	}

	tikvNum := r.accessStoreNum(TiKVOnly)
	var nextIdx AccessIndex

	// If the region is not using proxy before, randomly select a non-leader peer for the first try.
	if currentProxyIdx == -1 {
		// Randomly select an non-leader peer
		// TODO: Skip unreachable peers here.
		nextIdx = AccessIndex(rand.Intn(tikvNum - 1))
		if nextIdx >= r.workTiKVIdx {
			nextIdx++
		}
	} else {
		nextIdx = (currentProxyIdx + 1) % AccessIndex(tikvNum)
		// skips the current workTiKVIdx
		if nextIdx == r.workTiKVIdx {
			nextIdx = (nextIdx + 1) % AccessIndex(tikvNum)
		}
	}

	newRegionStore := r.clone()
	newRegionStore.proxyTiKVIdx = nextIdx
	if incEpochStoreIdx >= 0 {
		newRegionStore.storeEpochs[incEpochStoreIdx]++
	}
	rr.compareAndSwapStore(r, newRegionStore)
}

func (r *RegionStore) setProxyStoreIdx(rr *Region, idx AccessIndex) {
	if r.proxyTiKVIdx == idx {
		return
	}

	newRegionStore := r.clone()
	newRegionStore.proxyTiKVIdx = idx
	success := rr.compareAndSwapStore(r, newRegionStore)
	logutil.BgLogger().Debug("try set proxy store index",
		zap.Uint64("region", rr.GetID()),
		zap.Int("index", int(idx)),
		zap.Bool("success", success))
}

func (r *RegionStore) unsetProxyStoreIfNeeded(rr *Region) {
	r.setProxyStoreIdx(rr, -1)
}

func (r *Region) findElectableStoreID() uint64 {
	if len(r.meta.Peers) == 0 {
		return 0
	}
	for _, p := range r.meta.Peers {
		if p.Role != metapb.PeerRole_Learner {
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
	addr         string               // loaded store address
	saddr        string               // loaded store status address
	storeID      uint64               // store's id
	state        uint64               // unsafe store storeState
	labels       []*metapb.StoreLabel // stored store labels
	resolveMutex sync.Mutex           // protect pd from concurrent init requests
	epoch        uint32               // store fail epoch, see RegionStore.storeEpochs
	storeType    tikvrpc.EndpointType // type of the store
	tokenCount   atomic2.Int64        // used store token count

	// whether the store is disconnected due to some reason, therefore requests to the store needs to be
	// forwarded by other stores. this is also the flag that a checkUntilHealth goroutine is running for this store.
	// this mechanism is currently only applicable for TiKV stores.
	needForwarding int32
}

type resolveState uint64

const (
	// The store is just created and normally is being resolved.
	// Store in this state will only be resolved by initResolve().
	unresolved resolveState = iota
	// The store is resolved and its address is valid.
	resolved
	// Request failed on this store and it will be re-resolved by asyncCheckAndResolveLoop().
	needCheck
	// The store's address or label is changed and marked deleted.
	// There is a new store struct replaced it in the RegionCache and should
	// call changeToActiveStore() to get the new struct.
	deleted
	// The store is a tombstone. Should invalidate the region if tries to access it.
	tombstone
)

// initResolve resolves the address of the store that never resolved and returns an
// empty string if it's a tombstone.
func (s *Store) initResolve(bo *Backoffer, c *RegionCache) (addr string, err error) {
	s.resolveMutex.Lock()
	state := s.getResolveState()
	defer s.resolveMutex.Unlock()
	if state != unresolved {
		if state != tombstone {
			addr = s.addr
		}
		return
	}
	var store *metapb.Store
	for {
		store, err = c.pdClient.GetStore(bo.GetCtx(), s.storeID)
		if err != nil {
			metrics.RegionCacheCounterWithGetStoreError.Inc()
		} else {
			metrics.RegionCacheCounterWithGetStoreOK.Inc()
		}
		if bo.GetCtx().Err() != nil && errors.Cause(bo.GetCtx().Err()) == context.Canceled {
			return
		}
		if err != nil && !isStoreNotFoundError(err) {
			// TODO: more refine PD error status handle.
			err = errors.Errorf("loadStore from PD failed, id: %d, err: %v", s.storeID, err)
			if err = bo.Backoff(retry.BoPDRPC, err); err != nil {
				return
			}
			continue
		}
		// The store is a tombstone.
		if store == nil {
			s.setResolveState(tombstone)
			return "", nil
		}
		addr = store.GetAddress()
		if addr == "" {
			return "", errors.Errorf("empty store(%d) address", s.storeID)
		}
		s.addr = addr
		s.saddr = store.GetStatusAddress()
		s.storeType = GetStoreTypeByMeta(store)
		s.labels = store.GetLabels()
		// Shouldn't have other one changing its state concurrently, but we still use changeResolveStateTo for safety.
		s.changeResolveStateTo(unresolved, resolved)
		return s.addr, nil
	}
}

// A quick and dirty solution to find out whether an err is caused by StoreNotFound.
// todo: A better solution, maybe some err-code based error handling?
func isStoreNotFoundError(err error) bool {
	return strings.Contains(err.Error(), "invalid store ID") && strings.Contains(err.Error(), "not found")
}

// reResolve try to resolve addr for store that need check. Returns false if the region is in tombstone state or is
// deleted.
func (s *Store) reResolve(c *RegionCache) (bool, error) {
	var addr string
	store, err := c.pdClient.GetStore(context.Background(), s.storeID)
	if err != nil {
		metrics.RegionCacheCounterWithGetStoreError.Inc()
	} else {
		metrics.RegionCacheCounterWithGetStoreOK.Inc()
	}
	// `err` here can mean either "load Store from PD failed" or "store not found"
	// If load Store from PD is successful but PD didn't find the store
	// the err should be handled by next `if` instead of here
	if err != nil && !isStoreNotFoundError(err) {
		logutil.BgLogger().Error("loadStore from PD failed", zap.Uint64("id", s.storeID), zap.Error(err))
		// we cannot do backoff in reResolve loop but try check other store and wait tick.
		return false, err
	}
	if store == nil {
		// store has be removed in PD, we should invalidate all regions using those store.
		logutil.BgLogger().Info("invalidate regions in removed store",
			zap.Uint64("store", s.storeID), zap.String("add", s.addr))
		atomic.AddUint32(&s.epoch, 1)
		s.setResolveState(tombstone)
		metrics.RegionCacheCounterWithInvalidateStoreRegionsOK.Inc()
		return false, nil
	}

	storeType := GetStoreTypeByMeta(store)
	addr = store.GetAddress()
	if s.addr != addr || !s.IsSameLabels(store.GetLabels()) {
		newStore := &Store{storeID: s.storeID, addr: addr, saddr: store.GetStatusAddress(), storeType: storeType, labels: store.GetLabels(), state: uint64(resolved)}
		c.storeMu.Lock()
		c.storeMu.stores[newStore.storeID] = newStore
		c.storeMu.Unlock()
		s.setResolveState(deleted)
		return false, nil
	}
	s.changeResolveStateTo(needCheck, resolved)
	return true, nil
}

func (s *Store) getResolveState() resolveState {
	var state resolveState
	if s == nil {
		return state
	}
	return resolveState(atomic.LoadUint64(&s.state))
}

func (s *Store) setResolveState(state resolveState) {
	atomic.StoreUint64(&s.state, uint64(state))
}

// changeResolveStateTo changes the store resolveState from the old state to the new state.
// Returns true if it changes the state successfully, and false if the store's state
// is changed by another one.
func (s *Store) changeResolveStateTo(from, to resolveState) bool {
	for {
		state := s.getResolveState()
		if state == to {
			return true
		}
		if state != from {
			return false
		}
		if atomic.CompareAndSwapUint64(&s.state, uint64(from), uint64(to)) {
			return true
		}
	}
}

// markNeedCheck marks resolved store to be async resolve to check store addr change.
func (s *Store) markNeedCheck(notifyCheckCh chan struct{}) {
	if s.changeResolveStateTo(resolved, needCheck) {
		select {
		case notifyCheckCh <- struct{}{}:
		default:
		}
	}
}

// IsSameLabels returns whether the store have the same labels with target labels
func (s *Store) IsSameLabels(labels []*metapb.StoreLabel) bool {
	if len(s.labels) != len(labels) {
		return false
	}
	return s.IsLabelsMatch(labels)
}

// IsLabelsMatch return whether the store's labels match the target labels
func (s *Store) IsLabelsMatch(labels []*metapb.StoreLabel) bool {
	if len(labels) < 1 {
		return true
	}
	for _, targetLabel := range labels {
		match := false
		for _, label := range s.labels {
			if targetLabel.Key == label.Key && targetLabel.Value == label.Value {
				match = true
				break
			}
		}
		if !match {
			return false
		}
	}
	return true
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
)

func (s *Store) startHealthCheckLoopIfNeeded(c *RegionCache) {
	// This mechanism doesn't support non-TiKV stores currently.
	if s.storeType != tikvrpc.TiKV {
		logutil.BgLogger().Info("[health check] skip running health check loop for non-tikv store",
			zap.Uint64("storeID", s.storeID), zap.String("addr", s.addr))
		return
	}

	// It may be already started by another thread.
	if atomic.CompareAndSwapInt32(&s.needForwarding, 0, 1) {
		go s.checkUntilHealth(c)
	}
}

func (s *Store) checkUntilHealth(c *RegionCache) {
	defer atomic.CompareAndSwapInt32(&s.needForwarding, 1, 0)

	ticker := time.NewTicker(time.Second)
	lastCheckPDTime := time.Now()

	// TODO(MyonKeminta): Set a more proper ctx here so that it can be interrupted immediately when the RegionCache is
	// shutdown.
	ctx := context.Background()
	for {
		select {
		case <-c.closeCh:
			return
		case <-ticker.C:
			if time.Since(lastCheckPDTime) > time.Second*30 {
				lastCheckPDTime = time.Now()

				valid, err := s.reResolve(c)
				if err != nil {
					logutil.BgLogger().Warn("[health check] failed to re-resolve unhealthy store", zap.Error(err))
				} else if !valid {
					logutil.BgLogger().Info("[health check] store meta deleted, stop checking", zap.Uint64("storeID", s.storeID), zap.String("addr", s.addr))
					return
				}
			}

			bo := retry.NewNoopBackoff(ctx)
			l := s.requestLiveness(bo, c)
			if l == reachable {
				logutil.BgLogger().Info("[health check] store became reachable", zap.Uint64("storeID", s.storeID))

				return
			}
		}
	}
}

func (s *Store) requestLiveness(bo *Backoffer, c *RegionCache) (l livenessState) {
	if c != nil && c.testingKnobs.mockRequestLiveness != nil {
		return c.testingKnobs.mockRequestLiveness(s, bo)
	}

	if StoreLivenessTimeout == 0 {
		return unreachable
	}

	if s.getResolveState() != resolved {
		l = unknown
		return
	}
	addr := s.addr
	rsCh := livenessSf.DoChan(addr, func() (interface{}, error) {
		return invokeKVStatusAPI(addr, StoreLivenessTimeout), nil
	})
	var ctx context.Context
	if bo != nil {
		ctx = bo.GetCtx()
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

func invokeKVStatusAPI(addr string, timeout time.Duration) (l livenessState) {
	start := time.Now()
	defer func() {
		if l == reachable {
			metrics.StatusCountWithOK.Inc()
		} else {
			metrics.StatusCountWithError.Inc()
		}
		metrics.TiKVStatusDuration.WithLabelValues(addr).Observe(time.Since(start).Seconds())
	}()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	conn, cli, err := createKVHealthClient(ctx, addr)
	if err != nil {
		logutil.BgLogger().Info("[health check] create grpc connection failed", zap.String("store", addr), zap.Error(err))
		l = unreachable
		return
	}
	defer func() {
		err := conn.Close()
		if err != nil {
			logutil.BgLogger().Info("[health check] failed to close the grpc connection for health check", zap.String("store", addr), zap.Error(err))
		}
	}()

	req := &healthpb.HealthCheckRequest{}
	resp, err := cli.Check(ctx, req)
	if err != nil {
		logutil.BgLogger().Info("[health check] check health error", zap.String("store", addr), zap.Error(err))
		l = unreachable
		return
	}

	status := resp.GetStatus()
	if status == healthpb.HealthCheckResponse_UNKNOWN {
		logutil.BgLogger().Info("[health check] check health returns unknown", zap.String("store", addr))
		l = unknown
		return
	}

	if status != healthpb.HealthCheckResponse_SERVING {
		logutil.BgLogger().Info("[health check] service not serving", zap.Stringer("status", status))
		l = unreachable
		return
	}

	l = reachable
	return
}

func createKVHealthClient(ctx context.Context, addr string) (*grpc.ClientConn, healthpb.HealthClient, error) {
	// Temporarily directly load the config from the global config, however it's not a good idea to let RegionCache to
	// access it.
	// TODO: Pass the config in a better way, or use the connArray inner the client directly rather than creating new
	// connection.

	cfg := config.GetGlobalConfig()

	opt := grpc.WithInsecure()
	if len(cfg.Security.ClusterSSLCA) != 0 {
		tlsConfig, err := cfg.Security.ToTLSConfig()
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		opt = grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))
	}
	keepAlive := cfg.TiKVClient.GrpcKeepAliveTime
	keepAliveTimeout := cfg.TiKVClient.GrpcKeepAliveTimeout
	conn, err := grpc.DialContext(
		ctx,
		addr,
		opt,
		grpc.WithInitialWindowSize(client.GrpcInitialWindowSize),
		grpc.WithInitialConnWindowSize(client.GrpcInitialConnWindowSize),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  100 * time.Millisecond, // Default was 1s.
				Multiplier: 1.6,                    // Default
				Jitter:     0.2,                    // Default
				MaxDelay:   3 * time.Second,        // Default was 120s.
			},
			MinConnectTimeout: 5 * time.Second,
		}),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                time.Duration(keepAlive) * time.Second,
			Timeout:             time.Duration(keepAliveTimeout) * time.Second,
			PermitWithoutStream: true,
		}),
	)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	cli := healthpb.NewHealthClient(conn)
	return conn, cli, nil
}
