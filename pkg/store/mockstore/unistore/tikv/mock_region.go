// Copyright 2019-present PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tikv

import (
	"bytes"
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/google/btree"
	"github.com/pingcap/badger"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/store/mockstore/unistore/cophandler"
	"github.com/pingcap/tidb/pkg/store/mockstore/unistore/tikv/dbreader"
	"github.com/pingcap/tidb/pkg/store/mockstore/unistore/tikv/mvcc"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/tikv/client-go/v2/oracle"
	pdclient "github.com/tikv/pd/client"
)

// MPPTaskHandlerMap is a map of *cophandler.MPPTaskHandler.
type MPPTaskHandlerMap struct {
	mu           sync.RWMutex
	taskHandlers map[int64]*cophandler.MPPTaskHandler
}

// MockRegionManager implements RegionManager interface.
type MockRegionManager struct {
	regionManager

	bundle        *mvcc.DBBundle
	sortedRegions *btree.BTree
	stores        map[uint64]*metapb.Store
	id            uint64
	clusterID     uint64
	regionSize    int64
	closed        uint32

	// used for mpp test
	mppTaskSet map[uint64]*MPPTaskHandlerMap
}

// NewMockRegionManager returns a new MockRegionManager.
func NewMockRegionManager(bundle *mvcc.DBBundle, clusterID uint64, opts RegionOptions) (*MockRegionManager, error) {
	rm := &MockRegionManager{
		bundle:        bundle,
		clusterID:     clusterID,
		regionSize:    opts.RegionSize,
		sortedRegions: btree.New(32),
		stores:        make(map[uint64]*metapb.Store),
		mppTaskSet:    make(map[uint64]*MPPTaskHandlerMap),
		regionManager: regionManager{
			regions:   make(map[uint64]*regionCtx),
			storeMeta: new(metapb.Store),
			latches:   newLatches(),
		},
	}
	var maxID uint64
	err := rm.regionManager.loadFromLocal(bundle, func(r *regionCtx) {
		if maxID < r.meta.Id {
			maxID = r.meta.Id
		}
		for _, p := range r.meta.Peers {
			if maxID < p.Id {
				maxID = p.Id
			}
			if maxID < p.StoreId {
				maxID = p.StoreId
			}
		}
		rm.sortedRegions.ReplaceOrInsert(newBtreeItem(r))
	})
	rm.id = maxID
	if rm.storeMeta.Id != 0 {
		rm.stores[rm.storeMeta.Id] = rm.storeMeta
	}
	return rm, err
}

// Close closes the MockRegionManager.
func (rm *MockRegionManager) Close() error {
	atomic.StoreUint32(&rm.closed, 1)
	return nil
}

// AllocID allocs an id.
func (rm *MockRegionManager) AllocID() uint64 {
	return atomic.AddUint64(&rm.id, 1)
}

// AllocIDs allocs ids with the given number n.
func (rm *MockRegionManager) AllocIDs(n int) []uint64 {
	maxID := atomic.AddUint64(&rm.id, uint64(n))
	ids := make([]uint64, n)
	base := maxID - uint64(n-1)
	for i := range ids {
		ids[i] = base + uint64(i)
	}
	return ids
}

// GetStoreIDByAddr gets a store id by the store address.
func (rm *MockRegionManager) GetStoreIDByAddr(addr string) (uint64, error) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	for _, store := range rm.stores {
		if store.Address == addr {
			return store.Id, nil
		}
	}
	return 0, errors.New("Store not match")
}

// GetStoreAddrByStoreID gets a store address by the store id.
func (rm *MockRegionManager) GetStoreAddrByStoreID(storeID uint64) (string, error) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	for _, store := range rm.stores {
		if store.Id == storeID {
			return store.Address, nil
		}
	}
	return "", errors.New("Store not match")
}

// GetStoreInfoFromCtx gets the store info from the context.
func (rm *MockRegionManager) GetStoreInfoFromCtx(ctx *kvrpcpb.Context) (string, uint64, *errorpb.Error) {
	ctxPeer := ctx.GetPeer()
	if ctxPeer != nil {
		addr, err := rm.GetStoreAddrByStoreID(ctxPeer.GetStoreId())
		if err != nil {
			return "", 0, &errorpb.Error{
				Message:       "store not match",
				StoreNotMatch: &errorpb.StoreNotMatch{},
			}
		}
		return addr, ctxPeer.GetStoreId(), nil
	}
	return rm.storeMeta.Address, rm.storeMeta.Id, nil
}

// GetRegionFromCtx gets the region from the context.
func (rm *MockRegionManager) GetRegionFromCtx(ctx *kvrpcpb.Context) (RegionCtx, *errorpb.Error) {
	ctxPeer := ctx.GetPeer()
	if ctxPeer != nil {
		_, err := rm.GetStoreAddrByStoreID(ctxPeer.GetStoreId())
		if err != nil {
			return nil, &errorpb.Error{
				Message:       "store not match",
				StoreNotMatch: &errorpb.StoreNotMatch{},
			}
		}
	}
	rm.mu.RLock()
	ri := rm.regions[ctx.RegionId]
	rm.mu.RUnlock()
	if ri == nil {
		return nil, &errorpb.Error{
			Message: "region not found",
			RegionNotFound: &errorpb.RegionNotFound{
				RegionId: ctx.GetRegionId(),
			},
		}
	}
	// Region epoch does not match.
	if rm.isEpochStale(ri.getRegionEpoch(), ctx.GetRegionEpoch()) {
		return nil, &errorpb.Error{
			Message: "stale epoch",
			EpochNotMatch: &errorpb.EpochNotMatch{
				CurrentRegions: []*metapb.Region{{
					Id:          ri.meta.Id,
					StartKey:    ri.meta.StartKey,
					EndKey:      ri.meta.EndKey,
					RegionEpoch: ri.getRegionEpoch(),
					Peers:       ri.meta.Peers,
				}},
			},
		}
	}
	return ri, nil
}

// btreeItem is BTree's Item that uses []byte to compare.
type btreeItem struct {
	key    []byte
	inf    bool
	region RegionCtx
}

func newBtreeItem(r RegionCtx) *btreeItem {
	return &btreeItem{
		key:    r.Meta().EndKey,
		inf:    len(r.Meta().EndKey) == 0,
		region: r,
	}
}

func newBtreeSearchItem(key []byte) *btreeItem {
	return &btreeItem{
		key: key,
	}
}

func (item *btreeItem) Less(o btree.Item) bool {
	other := o.(*btreeItem)
	if item.inf {
		return false
	}
	if other.inf {
		return true
	}
	return bytes.Compare(item.key, other.key) < 0
}

// GetRegion gets a region by the id.
func (rm *MockRegionManager) GetRegion(id uint64) *metapb.Region {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return proto.Clone(rm.regions[id].meta).(*metapb.Region)
}

// GetRegionByKey gets a region by the key.
func (rm *MockRegionManager) GetRegionByKey(key []byte) (region *metapb.Region, peer *metapb.Peer, buckets *metapb.Buckets, downPeers []*metapb.Peer) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	rm.sortedRegions.AscendGreaterOrEqual(newBtreeSearchItem(key), func(item btree.Item) bool {
		region = item.(*btreeItem).region.Meta()
		if bytes.Equal(region.EndKey, key) {
			region = nil
			return true
		}
		return false
	})
	if region == nil || !rm.regionContainsKey(region, key) {
		return nil, nil, nil, nil
	}
	return proto.Clone(region).(*metapb.Region), proto.Clone(region.Peers[0]).(*metapb.Peer), nil, nil
}

// GetRegionByEndKey gets a region by the end key.
func (rm *MockRegionManager) GetRegionByEndKey(key []byte) (region *metapb.Region, peer *metapb.Peer) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	rm.sortedRegions.AscendGreaterOrEqual(newBtreeSearchItem(key), func(item btree.Item) bool {
		region = item.(*btreeItem).region.Meta()
		return false
	})
	if region == nil || !rm.regionContainsKeyByEnd(region, key) {
		return nil, nil
	}
	return proto.Clone(region).(*metapb.Region), proto.Clone(region.Peers[0]).(*metapb.Peer)
}

func (rm *MockRegionManager) regionContainsKey(r *metapb.Region, key []byte) bool {
	return bytes.Compare(r.GetStartKey(), key) <= 0 &&
		(bytes.Compare(key, r.GetEndKey()) < 0 || len(r.GetEndKey()) == 0)
}

func (rm *MockRegionManager) regionContainsKeyByEnd(r *metapb.Region, key []byte) bool {
	return bytes.Compare(r.GetStartKey(), key) < 0 &&
		(bytes.Compare(key, r.GetEndKey()) <= 0 || len(r.GetEndKey()) == 0)
}

// Bootstrap implements gRPC PDServer
func (rm *MockRegionManager) Bootstrap(stores []*metapb.Store, region *metapb.Region) error {
	bootstrapped, err := rm.IsBootstrapped()
	if err != nil {
		return err
	}
	if bootstrapped {
		return nil
	}

	regions := make([]*regionCtx, 0, 5)
	rm.mu.Lock()

	// We must in TiDB's tests if we got more than one store.
	// So we use the first one to check requests and put others into stores map.
	rm.storeMeta = stores[0]
	for _, s := range stores {
		rm.stores[s.Id] = s
	}

	region.RegionEpoch.ConfVer = 1
	region.RegionEpoch.Version = 1
	root := newRegionCtx(region, rm.latches, nil)
	rm.regions[region.Id] = root
	rm.sortedRegions.ReplaceOrInsert(newBtreeItem(root))
	regions = append(regions, root)
	rm.mu.Unlock()

	err = rm.saveRegions(regions)
	if err != nil {
		return err
	}

	storeBuf, err := rm.storeMeta.Marshal()
	if err != nil {
		return err
	}

	err = rm.bundle.DB.Update(func(txn *badger.Txn) error {
		ts := atomic.AddUint64(&rm.bundle.StateTS, 1)
		return txn.SetEntry(&badger.Entry{
			Key:   y.KeyWithTs(InternalStoreMetaKey, ts),
			Value: storeBuf,
		})
	})
	return err
}

// IsBootstrapped returns whether the MockRegionManager is bootstrapped or not.
func (rm *MockRegionManager) IsBootstrapped() (bool, error) {
	err := rm.bundle.DB.View(func(txn *badger.Txn) error {
		_, err := txn.Get(InternalStoreMetaKey)
		return err
	})
	if err == nil {
		return true, nil
	}
	if err == badger.ErrKeyNotFound {
		err = nil
	}
	return false, err
}

// Split splits a Region at the key (encoded) and creates new Region.
func (rm *MockRegionManager) Split(regionID, newRegionID uint64, key []byte, peerIDs []uint64, leaderPeerID uint64) {
	_, err := rm.split(regionID, newRegionID, codec.EncodeBytes(nil, key), peerIDs)
	if err != nil {
		panic(err)
	}
}

// SplitRaw splits a Region at the key (not encoded) and creates new Region.
func (rm *MockRegionManager) SplitRaw(regionID, newRegionID uint64, rawKey []byte, peerIDs []uint64, leaderPeerID uint64) *metapb.Region {
	r, err := rm.split(regionID, newRegionID, rawKey, peerIDs)
	if err != nil {
		panic(err)
	}
	return proto.Clone(r).(*metapb.Region)
}

// SplitTable evenly splits the data in table into count regions.
func (rm *MockRegionManager) SplitTable(tableID int64, count int) {
	tableStart := tablecodec.GenTableRecordPrefix(tableID)
	tableEnd := tableStart.PrefixNext()
	keys := rm.calculateSplitKeys(tableStart, tableEnd, count)
	rm.mu.Lock()
	defer rm.mu.Unlock()
	if _, err := rm.splitKeys(keys); err != nil {
		panic(err)
	}
}

// SplitIndex evenly splits the data in index into count regions.
func (rm *MockRegionManager) SplitIndex(tableID, indexID int64, count int) {
	indexStart := tablecodec.EncodeTableIndexPrefix(tableID, indexID)
	indexEnd := indexStart.PrefixNext()
	keys := rm.calculateSplitKeys(indexStart, indexEnd, count)
	rm.mu.Lock()
	defer rm.mu.Unlock()
	if _, err := rm.splitKeys(keys); err != nil {
		panic(err)
	}
}

// SplitKeys evenly splits the start, end key into "count" regions.
func (rm *MockRegionManager) SplitKeys(start, end kv.Key, count int) {
	keys := rm.calculateSplitKeys(start, end, count)
	rm.mu.Lock()
	defer rm.mu.Unlock()
	if _, err := rm.splitKeys(keys); err != nil {
		panic(err)
	}
}

// SplitArbitrary splits the cluster by the split point manually provided.
// The keys provided are raw key.
func (rm *MockRegionManager) SplitArbitrary(keys ...[]byte) {
	splitKeys := make([][]byte, 0, len(keys))
	for _, key := range keys {
		encKey := codec.EncodeBytes(nil, key)
		splitKeys = append(splitKeys, encKey)
	}
	if _, err := rm.splitKeys(splitKeys); err != nil {
		panic(err)
	}
}

// SplitRegion implements the RegionManager interface.
func (rm *MockRegionManager) SplitRegion(req *kvrpcpb.SplitRegionRequest) *kvrpcpb.SplitRegionResponse {
	splitKeys := make([][]byte, 0, len(req.SplitKeys))
	for _, rawKey := range req.SplitKeys {
		splitKeys = append(splitKeys, codec.EncodeBytes(nil, rawKey))
	}
	slices.SortFunc(splitKeys, bytes.Compare)

	ctxPeer := req.Context.GetPeer()
	if ctxPeer != nil {
		_, err := rm.GetStoreAddrByStoreID(ctxPeer.GetStoreId())
		if err != nil {
			return &kvrpcpb.SplitRegionResponse{RegionError: &errorpb.Error{
				Message:       "store not match",
				StoreNotMatch: &errorpb.StoreNotMatch{},
			}}
		}
	}

	rm.mu.Lock()
	defer rm.mu.Unlock()
	ri := rm.regions[req.Context.RegionId]
	if ri == nil {
		return &kvrpcpb.SplitRegionResponse{RegionError: &errorpb.Error{
			Message: "region not found",
			RegionNotFound: &errorpb.RegionNotFound{
				RegionId: req.Context.GetRegionId(),
			},
		}}
	}
	// Region epoch does not match.
	if rm.isEpochStale(ri.getRegionEpoch(), req.Context.GetRegionEpoch()) {
		return &kvrpcpb.SplitRegionResponse{RegionError: &errorpb.Error{
			Message: "stale epoch",
			EpochNotMatch: &errorpb.EpochNotMatch{
				CurrentRegions: []*metapb.Region{{
					Id:          ri.meta.Id,
					StartKey:    ri.meta.StartKey,
					EndKey:      ri.meta.EndKey,
					RegionEpoch: ri.getRegionEpoch(),
					Peers:       ri.meta.Peers,
				}},
			},
		}}
	}
	newRegions, err := rm.splitKeys(splitKeys)
	if err != nil {
		return &kvrpcpb.SplitRegionResponse{RegionError: &errorpb.Error{Message: err.Error()}}
	}

	ret := make([]*metapb.Region, 0, len(newRegions))
	for _, regCtx := range newRegions {
		ret = append(ret, proto.Clone(regCtx.meta).(*metapb.Region))
	}
	return &kvrpcpb.SplitRegionResponse{Regions: ret}
}

func (rm *MockRegionManager) calculateSplitKeys(start, end []byte, count int) [][]byte {
	var keys [][]byte
	txn := rm.bundle.DB.NewTransaction(false)
	it := dbreader.NewIterator(txn, false, start, end)
	it.SetAllVersions(true)
	for it.Seek(start); it.Valid(); it.Next() {
		item := it.Item()
		key := item.Key()
		if bytes.Compare(key, end) >= 0 {
			break
		}

		keys = append(keys, safeCopy(key))
	}

	splitKeys := make([][]byte, 0, count)
	quotient := len(keys) / count
	remainder := len(keys) % count
	i := 0
	for i < len(keys) {
		regionEntryCount := quotient
		if remainder > 0 {
			remainder--
			regionEntryCount++
		}
		i += regionEntryCount
		if i < len(keys) {
			splitKeys = append(splitKeys, codec.EncodeBytes(nil, keys[i]))
		}
	}
	return splitKeys
}

// Should call `rm.mu.Lock()` before call this method.
func (rm *MockRegionManager) splitKeys(keys [][]byte) ([]*regionCtx, error) {
	newRegions := make([]*regionCtx, 0, len(keys))
	rm.sortedRegions.AscendGreaterOrEqual(newBtreeSearchItem(keys[0]), func(item btree.Item) bool {
		if len(keys) == 0 {
			return false
		}
		region := item.(*btreeItem).region.Meta()

		var i int
		for i = 0; i < len(keys); i++ {
			if len(region.EndKey) > 0 && bytes.Compare(keys[i], region.EndKey) >= 0 {
				break
			}
		}
		splits := keys[:i]
		keys = keys[i:]
		if len(splits) == 0 {
			return true
		}

		startKey := region.StartKey
		if bytes.Equal(startKey, splits[0]) {
			splits = splits[1:]
		}
		if len(splits) == 0 {
			return true
		}

		newRegions = append(newRegions, newRegionCtx(&metapb.Region{
			Id:       region.Id,
			StartKey: startKey,
			EndKey:   splits[0],
			RegionEpoch: &metapb.RegionEpoch{
				ConfVer: region.RegionEpoch.ConfVer,
				Version: region.RegionEpoch.Version + 1,
			},
			Peers: region.Peers,
		}, rm.latches, nil))

		for i := 0; i < len(splits)-1; i++ {
			newRegions = append(newRegions, newRegionCtx(&metapb.Region{
				Id:          rm.AllocID(),
				RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
				Peers:       []*metapb.Peer{{Id: rm.AllocID(), StoreId: rm.storeMeta.Id}},
				StartKey:    splits[i],
				EndKey:      splits[i+1],
			}, rm.latches, nil))
		}

		if !bytes.Equal(splits[len(splits)-1], region.EndKey) {
			newRegions = append(newRegions, newRegionCtx(&metapb.Region{
				Id:          rm.AllocID(),
				RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
				Peers:       []*metapb.Peer{{Id: rm.AllocID(), StoreId: rm.storeMeta.Id}},
				StartKey:    splits[len(splits)-1],
				EndKey:      region.EndKey,
			}, rm.latches, nil))
		}
		return true
	})
	for _, region := range newRegions {
		rm.regions[region.meta.Id] = region
		rm.sortedRegions.ReplaceOrInsert(newBtreeItem(region))
	}
	return newRegions, rm.saveRegions(newRegions)
}

func (rm *MockRegionManager) split(regionID, newRegionID uint64, key []byte, peerIDs []uint64) (*metapb.Region, error) {
	rm.mu.RLock()
	old := rm.regions[regionID]
	rm.mu.RUnlock()
	oldRegion := old.meta
	leftMeta := &metapb.Region{
		Id:       oldRegion.Id,
		StartKey: oldRegion.StartKey,
		EndKey:   key,
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: oldRegion.RegionEpoch.ConfVer,
			Version: oldRegion.RegionEpoch.Version + 1,
		},
		Peers: oldRegion.Peers,
	}
	left := newRegionCtx(leftMeta, rm.latches, nil)

	peers := make([]*metapb.Peer, 0, len(leftMeta.Peers))
	for i, p := range leftMeta.Peers {
		peers = append(peers, &metapb.Peer{
			StoreId: p.StoreId,
			Id:      peerIDs[i],
		})
	}
	rightMeta := &metapb.Region{
		Id:       newRegionID,
		StartKey: key,
		EndKey:   oldRegion.EndKey,
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 1,
			Version: 1,
		},
		Peers: peers,
	}
	right := newRegionCtx(rightMeta, rm.latches, nil)

	if err1 := rm.saveRegions([]*regionCtx{left, right}); err1 != nil {
		return nil, err1
	}

	rm.mu.Lock()
	rm.regions[left.meta.Id] = left
	rm.sortedRegions.ReplaceOrInsert(newBtreeItem(left))
	rm.regions[right.meta.Id] = right
	rm.sortedRegions.ReplaceOrInsert(newBtreeItem(right))
	rm.mu.Unlock()

	return right.meta, nil
}

func (rm *MockRegionManager) saveRegions(regions []*regionCtx) error {
	if atomic.LoadUint32(&rm.closed) == 1 {
		return nil
	}
	return rm.bundle.DB.Update(func(txn *badger.Txn) error {
		ts := atomic.AddUint64(&rm.bundle.StateTS, 1)
		for _, r := range regions {
			err := txn.SetEntry(&badger.Entry{
				Key:   y.KeyWithTs(InternalRegionMetaKey(r.meta.Id), ts),
				Value: r.marshal(),
			})
			if err != nil {
				return errors.Trace(err)
			}
		}
		return nil
	})
}

// ScanRegions gets a list of regions, starts from the region that contains key.
// Limit limits the maximum number of regions returned.
// If a region has no leader, corresponding leader will be placed by a peer
// with empty value (PeerID is 0).
func (rm *MockRegionManager) ScanRegions(startKey, endKey []byte, limit int, _ ...pdclient.GetRegionOption) []*pdclient.Region {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	regions := make([]*pdclient.Region, 0, len(rm.regions))
	rm.sortedRegions.AscendGreaterOrEqual(newBtreeSearchItem(startKey), func(i btree.Item) bool {
		r := i.(*btreeItem).region
		if len(endKey) > 0 && bytes.Compare(r.Meta().StartKey, endKey) >= 0 {
			return false
		}

		if len(regions) == 0 && bytes.Equal(r.Meta().EndKey, startKey) {
			return true
		}

		regions = append(regions, &pdclient.Region{
			Meta:   proto.Clone(r.Meta()).(*metapb.Region),
			Leader: proto.Clone(r.Meta().Peers[0]).(*metapb.Peer),
		})

		return !(limit > 0 && len(regions) >= limit)
	})
	return regions
}

// GetAllStores gets all stores from pd.
// The store may expire later. Caller is responsible for caching and taking care
// of store change.
func (rm *MockRegionManager) GetAllStores() []*metapb.Store {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	stores := make([]*metapb.Store, 0, len(rm.stores))
	for _, store := range rm.stores {
		stores = append(stores, proto.Clone(store).(*metapb.Store))
	}
	return stores
}

// AddStore adds a new Store to the cluster.
func (rm *MockRegionManager) AddStore(storeID uint64, addr string, labels ...*metapb.StoreLabel) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	rm.stores[storeID] = &metapb.Store{
		Id:      storeID,
		Address: addr,
		Labels:  labels,
	}
	rm.mppTaskSet[storeID] = &MPPTaskHandlerMap{
		taskHandlers: make(map[int64]*cophandler.MPPTaskHandler),
	}
}

func (rm *MockRegionManager) getMPPTaskSet(storeID uint64) *MPPTaskHandlerMap {
	return rm.mppTaskSet[storeID]
}

// RemoveStore removes a Store from the cluster.
func (rm *MockRegionManager) RemoveStore(storeID uint64) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	delete(rm.stores, storeID)
	delete(rm.mppTaskSet, storeID)
}

// AddPeer adds a new Peer to the cluster.
func (rm *MockRegionManager) AddPeer(regionID, storeID, peerID uint64) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	rm.regions[regionID].addPeer(peerID, storeID)
}

// MockPD implements gRPC PDServer.
type MockPD struct {
	rm          *MockRegionManager
	gcSafePoint uint64

	externalTimestamp atomic.Uint64
}

// NewMockPD returns a new MockPD.
func NewMockPD(rm *MockRegionManager) *MockPD {
	return &MockPD{
		rm: rm,
	}
}

// GetClusterID implements gRPC PDServer.
func (pd *MockPD) GetClusterID(ctx context.Context) uint64 {
	return pd.rm.clusterID
}

// AllocID implements gRPC PDServer.
func (pd *MockPD) AllocID(ctx context.Context) (uint64, error) {
	return pd.rm.AllocID(), nil
}

// Bootstrap implements gRPC PDServer.
func (pd *MockPD) Bootstrap(ctx context.Context, store *metapb.Store, region *metapb.Region) (*pdpb.BootstrapResponse, error) {
	if err := pd.rm.Bootstrap([]*metapb.Store{store}, region); err != nil {
		return nil, err
	}
	return &pdpb.BootstrapResponse{
		Header: &pdpb.ResponseHeader{ClusterId: pd.rm.clusterID},
	}, nil
}

// IsBootstrapped implements gRPC PDServer.
func (pd *MockPD) IsBootstrapped(ctx context.Context) (bool, error) {
	return pd.rm.IsBootstrapped()
}

// PutStore implements gRPC PDServer.
func (pd *MockPD) PutStore(ctx context.Context, store *metapb.Store) error {
	pd.rm.mu.Lock()
	defer pd.rm.mu.Unlock()
	pd.rm.stores[store.Id] = store
	return nil
}

// GetStore implements gRPC PDServer.
func (pd *MockPD) GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error) {
	pd.rm.mu.RLock()
	defer pd.rm.mu.RUnlock()
	return proto.Clone(pd.rm.stores[storeID]).(*metapb.Store), nil
}

// GetRegion implements gRPC PDServer.
func (pd *MockPD) GetRegion(ctx context.Context, key []byte, opts ...pdclient.GetRegionOption) (*pdclient.Region, error) {
	r, p, b, d := pd.rm.GetRegionByKey(key)
	return &pdclient.Region{Meta: r, Leader: p, Buckets: b, DownPeers: d}, nil
}

// GetRegionByID implements gRPC PDServer.
func (pd *MockPD) GetRegionByID(ctx context.Context, regionID uint64, opts ...pdclient.GetRegionOption) (*pdclient.Region, error) {
	pd.rm.mu.RLock()
	defer pd.rm.mu.RUnlock()

	r := pd.rm.regions[regionID]
	if r == nil {
		return nil, nil
	}
	return &pdclient.Region{Meta: proto.Clone(r.meta).(*metapb.Region), Leader: proto.Clone(r.meta.Peers[0]).(*metapb.Peer)}, nil
}

// ReportRegion implements gRPC PDServer.
func (pd *MockPD) ReportRegion(*pdpb.RegionHeartbeatRequest) {}

// AskSplit implements gRPC PDServer.
func (pd *MockPD) AskSplit(ctx context.Context, region *metapb.Region) (*pdpb.AskSplitResponse, error) {
	panic("unimplemented")
}

// AskBatchSplit implements gRPC PDServer.
func (pd *MockPD) AskBatchSplit(ctx context.Context, region *metapb.Region, count int) (*pdpb.AskBatchSplitResponse, error) {
	panic("unimplemented")
}

// ReportBatchSplit implements gRPC PDServer.
func (pd *MockPD) ReportBatchSplit(ctx context.Context, regions []*metapb.Region) error {
	panic("unimplemented")
}

// SetRegionHeartbeatResponseHandler sets the region heartbeat.
func (pd *MockPD) SetRegionHeartbeatResponseHandler(h func(*pdpb.RegionHeartbeatResponse)) {
	panic("unimplemented")
}

// GetGCSafePoint gets the gc safePoint
func (pd *MockPD) GetGCSafePoint(ctx context.Context) (uint64, error) {
	return atomic.LoadUint64(&pd.gcSafePoint), nil
}

// UpdateGCSafePoint implements gRPC PDServer.
// TiKV will check it and do GC themselves if necessary.
// If the given safePoint is less than the current one, it will not be updated.
// Returns the new safePoint after updating.
func (pd *MockPD) UpdateGCSafePoint(ctx context.Context, safePoint uint64) (uint64, error) {
	for {
		old := atomic.LoadUint64(&pd.gcSafePoint)
		if safePoint <= old {
			return old, nil
		}
		if atomic.CompareAndSwapUint64(&pd.gcSafePoint, old, safePoint) {
			return safePoint, nil
		}
	}
}

// StoreHeartbeat stores the heartbeat.
func (pd *MockPD) StoreHeartbeat(ctx context.Context, stats *pdpb.StoreStats) error { return nil }

// GetExternalTimestamp returns external timestamp
func (pd *MockPD) GetExternalTimestamp(ctx context.Context) (uint64, error) {
	return pd.externalTimestamp.Load(), nil
}

// SetExternalTimestamp sets external timestamp
func (pd *MockPD) SetExternalTimestamp(ctx context.Context, newTimestamp uint64) error {
	p, l := GetTS()
	currentTSO := oracle.ComposeTS(p, l)
	if newTimestamp > currentTSO {
		return errors.New("external timestamp is greater than global tso")
	}
	for {
		externalTimestamp := pd.externalTimestamp.Load()
		if externalTimestamp > newTimestamp {
			return errors.New("cannot decrease the external timestamp")
		}

		if pd.externalTimestamp.CompareAndSwap(externalTimestamp, newTimestamp) {
			return nil
		}
	}
}

// Use global variables to prevent pdClients from creating duplicate timestamps.
var tsMu = struct {
	sync.Mutex
	physicalTS int64
	logicalTS  int64
}{}

// GetTS gets a timestamp from MockPD.
func (pd *MockPD) GetTS(ctx context.Context) (int64, int64, error) {
	p, l := GetTS()
	return p, l, nil
}

// GetTS gets a timestamp.
func GetTS() (int64, int64) {
	tsMu.Lock()
	defer tsMu.Unlock()

	ts := time.Now().UnixMilli()
	if tsMu.physicalTS >= ts {
		tsMu.logicalTS++
	} else {
		tsMu.physicalTS = ts
		tsMu.logicalTS = 0
	}
	return tsMu.physicalTS, tsMu.logicalTS
}

// GetPrevRegion gets the previous region and its leader Peer of the region where the key is located.
func (pd *MockPD) GetPrevRegion(ctx context.Context, key []byte, opts ...pdclient.GetRegionOption) (*pdclient.Region, error) {
	r, p := pd.rm.GetRegionByEndKey(key)
	return &pdclient.Region{Meta: r, Leader: p}, nil
}

// GetAllStores gets all stores from pd.
// The store may expire later. Caller is responsible for caching and taking care
// of store change.
func (pd *MockPD) GetAllStores(ctx context.Context, opts ...pdclient.GetStoreOption) ([]*metapb.Store, error) {
	return pd.rm.GetAllStores(), nil
}

// ScanRegions gets a list of regions, starts from the region that contains key.
// Limit limits the maximum number of regions returned.
// If a region has no leader, corresponding leader will be placed by a peer
// with empty value (PeerID is 0).
func (pd *MockPD) ScanRegions(ctx context.Context, startKey []byte, endKey []byte, limit int, opts ...pdclient.GetRegionOption) ([]*pdclient.Region, error) {
	regions := pd.rm.ScanRegions(startKey, endKey, limit, opts...)
	return regions, nil
}

// BatchScanRegions scans regions in batch, return flattened regions.
// limit limits the maximum number of regions returned.
func (pd *MockPD) BatchScanRegions(ctx context.Context, keyRanges []pdclient.KeyRange, limit int, opts ...pdclient.GetRegionOption) ([]*pdclient.Region, error) {
	regions := make([]*pdclient.Region, 0, len(keyRanges))
	var lastRegion *pdclient.Region
	for _, keyRange := range keyRanges {
		if lastRegion != nil && lastRegion.Meta != nil {
			endKey := lastRegion.Meta.EndKey
			if len(endKey) == 0 {
				return regions, nil
			}
			if bytes.Compare(endKey, keyRange.EndKey) >= 0 {
				continue
			} else if bytes.Compare(endKey, keyRange.StartKey) > 0 {
				keyRange.StartKey = endKey
			}
		}
		rangeRegions := pd.rm.ScanRegions(keyRange.StartKey, keyRange.EndKey, limit, opts...)
		if len(rangeRegions) == 0 {
			continue
		}
		lastRegion = rangeRegions[len(rangeRegions)-1]
		regions = append(regions, rangeRegions...)
		limit -= len(rangeRegions)
		if limit <= 0 {
			break
		}
	}
	return regions, nil
}

// ScatterRegion scatters the specified region. Should use it for a batch of regions,
// and the distribution of these regions will be dispersed.
// NOTICE: This method is the old version of ScatterRegions, you should use the later one as your first choice.
func (pd *MockPD) ScatterRegion(ctx context.Context, regionID uint64) error {
	return nil
}

// Close closes the MockPD.
func (pd *MockPD) Close() {}
<<<<<<< HEAD
=======

type gcState struct {
	txnSafePoint uint64
	gcSafePoint  uint64
	gcBarriers   map[string]uint64
}

func newGCState() *gcState {
	return &gcState{
		gcBarriers: make(map[string]uint64),
	}
}

// gcStatesManagerSimulator is a mock implementation of GC APIs of PD. Note that it's currently a simple map and
// assumes keyspace IDs passed here are all valid, without caring about the keyspace metas of the cluster. Neither
// does it handle redirection logic of keyspaces configured running unified GC.
type gcStatesManagerSimulator struct {
	mu               sync.Mutex
	keyspaceGCStates map[uint32]*gcState
}

func newGCStatesManager() *gcStatesManagerSimulator {
	return &gcStatesManagerSimulator{
		keyspaceGCStates: make(map[uint32]*gcState),
	}
}

// getGCStates assuming the mutex is already acquired, and returns the GC state of the specified keyspace.
func (m *gcStatesManagerSimulator) getGCState(keyspaceID uint32) *gcState {
	internalState, ok := m.keyspaceGCStates[keyspaceID]
	if !ok {
		internalState = newGCState()
		m.keyspaceGCStates[keyspaceID] = internalState
	}
	return internalState
}

func (m *gcStatesManagerSimulator) UpdateServiceGCSafePoint(ctx context.Context, serviceID string, ttlSecs int64, safePoint uint64) (uint64, error) {
	// Compatibility code. See: https://github.com/tikv/pd/blob/b486e2181603e0140be9647f1f05b25f3177634a/pkg/gc/gc_state_manager.go#L705
	if serviceID == "gc_worker" {
		if ttlSecs != math.MaxInt64 {
			return 0, errors.New("ttl of gc_worker's service safe point must be math.MaxInt64")
		}

		res, err := m.GetGCInternalController(constants.NullKeyspaceID).AdvanceTxnSafePoint(ctx, safePoint)
		if err != nil {
			return 0, err
		}
		// Simulate the case that the minimal service safe point is not the "gc_worker".
		return res.NewTxnSafePoint, nil
	}

	startTime := time.Now()

	m.mu.Lock()
	defer m.mu.Unlock()

	if ttlSecs > 0 {
		ttl := time.Duration(math.MaxInt64)
		if ttlSecs <= math.MaxInt64/int64(time.Second) {
			ttl = time.Duration(ttlSecs) * time.Second
		}
		_, err := m.setGCBarrierImpl(ctx, constants.NullKeyspaceID, serviceID, safePoint, ttl, startTime)
		if err != nil && goerrors.Is(err, errGCBarrierTSBehindTxnSafePoint{}) {
			return 0, err
		}
	} else {
		_, err := m.deleteGCBarrierImpl(ctx, constants.NullKeyspaceID, serviceID, startTime)
		if err != nil {
			return 0, err
		}
	}

	// Simulate the case in which the "gc_worker" has the minimum service safe point, by return the value of txn safe
	// point directly.
	return m.getGCState(constants.NullKeyspaceID).txnSafePoint, nil
}

func (m *gcStatesManagerSimulator) GetGCInternalController(keyspaceID uint32) pdgc.InternalController {
	return gcInternalController{
		inner:      m,
		keyspaceID: keyspaceID,
	}
}

func (m *gcStatesManagerSimulator) GetGCStatesClient(keyspaceID uint32) pdgc.GCStatesClient {
	return gcStatesClient{
		inner:      m,
		keyspaceID: keyspaceID,
	}
}

type errGCBarrierTSBehindTxnSafePoint struct {
	attemptedBarrierTS uint64
	txnSafePoint       uint64
}

func (e errGCBarrierTSBehindTxnSafePoint) Error() string {
	return fmt.Sprintf("trying to set a GC barrier on ts %d which is already behind the txn safe point %d", e.attemptedBarrierTS, e.txnSafePoint)
}

type errDecreasingGCSafePoint struct {
	currentGCSafePoint uint64
	target             uint64
}

func (e errDecreasingGCSafePoint) Error() string {
	return fmt.Sprintf("trying to update gc safe point to a smaller value, current value: %v, given: %v",
		e.currentGCSafePoint, e.target)
}

func (m *gcStatesManagerSimulator) setGCBarrierImpl(ctx context.Context, keyspaceID uint32, barrierID string, barrierTS uint64, ttl time.Duration, startTime time.Time) (*pdgc.GCBarrierInfo, error) {
	internalState := m.getGCState(keyspaceID)

	if barrierTS == 0 || barrierID == "" || ttl <= 0 {
		return nil, errors.New("invalid arguments")
	}

	// TTL is unimplemented here.

	if barrierTS < internalState.txnSafePoint {
		return nil, errGCBarrierTSBehindTxnSafePoint{
			attemptedBarrierTS: barrierTS,
			txnSafePoint:       internalState.txnSafePoint,
		}
	}

	res := pdgc.NewGCBarrierInfo(barrierID, barrierTS, pdgc.TTLNeverExpire, startTime)
	internalState.gcBarriers[barrierID] = barrierTS
	return res, nil
}

func (m *gcStatesManagerSimulator) deleteGCBarrierImpl(ctx context.Context, keyspaceID uint32, barrierID string, startTime time.Time) (*pdgc.GCBarrierInfo, error) {
	internalState := m.getGCState(keyspaceID)

	barrierTS, exists := internalState.gcBarriers[barrierID]

	if !exists {
		return nil, nil
	}

	delete(internalState.gcBarriers, barrierID)
	return pdgc.NewGCBarrierInfo(barrierID, barrierTS, pdgc.TTLNeverExpire, startTime), nil
}

type gcInternalController struct {
	inner      *gcStatesManagerSimulator
	keyspaceID uint32
}

func (c gcInternalController) AdvanceTxnSafePoint(ctx context.Context, target uint64) (pdgc.AdvanceTxnSafePointResult, error) {
	c.inner.mu.Lock()
	defer c.inner.mu.Unlock()

	internalState := c.inner.getGCState(c.keyspaceID)

	if target < internalState.txnSafePoint {
		// GC worker needs to identify this error type by the error message currently. Attach the real error code
		// to the beginning to workaround it temporarily.
		return pdgc.AdvanceTxnSafePointResult{},
			errors.Errorf("[PD:gc:ErrDecreasingTxnSafePoint] trying to update txn safe point to a smaller value, current value: %v, given: %v",
				internalState.txnSafePoint, target)
	}

	res := pdgc.AdvanceTxnSafePointResult{
		OldTxnSafePoint:    internalState.txnSafePoint,
		Target:             target,
		NewTxnSafePoint:    target,
		BlockerDescription: "",
	}

	minGCBarrierName := ""
	var minGCBarrierTS uint64 = 0
	for name, ts := range internalState.gcBarriers {
		if ts == 0 {
			panic("found 0 in barrier ts of GC barriers")
		}
		if ts < minGCBarrierTS || minGCBarrierTS == 0 {
			minGCBarrierName = name
			minGCBarrierTS = ts
		}
	}

	if minGCBarrierTS != 0 && minGCBarrierTS < res.NewTxnSafePoint {
		res.NewTxnSafePoint = minGCBarrierTS
		res.BlockerDescription = fmt.Sprintf("GCBarrier { BarrierID: %+q, BarrierTS: %d, ExpirationTime: <nil> }", minGCBarrierName, res.NewTxnSafePoint)
		logutil.Logger(ctx).Info("txn safe point blocked",
			zap.Uint64("oldTxnSafePoint", res.OldTxnSafePoint), zap.Uint64("newTxnSafePoint", res.NewTxnSafePoint),
			zap.String("blocker", res.BlockerDescription))
	}

	if res.NewTxnSafePoint < res.OldTxnSafePoint {
		res.NewTxnSafePoint = res.OldTxnSafePoint
		logutil.Logger(ctx).Info("txn safe point unable to be blocked",
			zap.Uint64("oldTxnSafePoint", res.OldTxnSafePoint), zap.Uint64("newTxnSafePoint", res.NewTxnSafePoint),
			zap.String("blocker", res.BlockerDescription))
	}

	internalState.txnSafePoint = res.NewTxnSafePoint

	return res, nil
}

func (c gcInternalController) AdvanceGCSafePoint(ctx context.Context, target uint64) (pdgc.AdvanceGCSafePointResult, error) {
	c.inner.mu.Lock()
	defer c.inner.mu.Unlock()

	internalState := c.inner.getGCState(c.keyspaceID)

	if target < internalState.gcSafePoint {
		return pdgc.AdvanceGCSafePointResult{}, errDecreasingGCSafePoint{
			currentGCSafePoint: internalState.gcSafePoint,
			target:             target,
		}
	}

	if target > internalState.txnSafePoint {
		return pdgc.AdvanceGCSafePointResult{},
			errors.Errorf("trying to update GC safe point to a too large value that exceeds the txn safe point, current value: %v, given: %v, current txn safe point: %v",
				internalState.gcSafePoint, target, internalState.txnSafePoint)
	}

	res := pdgc.AdvanceGCSafePointResult{
		OldGCSafePoint: internalState.gcSafePoint,
		Target:         target,
		NewGCSafePoint: target,
	}

	internalState.gcSafePoint = res.NewGCSafePoint

	return res, nil
}

type gcStatesClient struct {
	inner      *gcStatesManagerSimulator
	keyspaceID uint32
}

func (c gcStatesClient) SetGCBarrier(ctx context.Context, barrierID string, barrierTS uint64, ttl time.Duration) (*pdgc.GCBarrierInfo, error) {
	startTime := time.Now()

	c.inner.mu.Lock()
	defer c.inner.mu.Unlock()

	return c.inner.setGCBarrierImpl(ctx, c.keyspaceID, barrierID, barrierTS, ttl, startTime)
}

func (c gcStatesClient) DeleteGCBarrier(ctx context.Context, barrierID string) (*pdgc.GCBarrierInfo, error) {
	startTime := time.Now()

	c.inner.mu.Lock()
	defer c.inner.mu.Unlock()

	return c.inner.deleteGCBarrierImpl(ctx, c.keyspaceID, barrierID, startTime)
}

func (c gcStatesClient) GetGCState(ctx context.Context) (pdgc.GCState, error) {
	startTime := time.Now()

	c.inner.mu.Lock()
	defer c.inner.mu.Unlock()

	internalState := c.inner.getGCState(c.keyspaceID)

	res := pdgc.GCState{
		KeyspaceID:   c.keyspaceID,
		TxnSafePoint: internalState.txnSafePoint,
		GCSafePoint:  internalState.gcSafePoint,
	}

	gcBarriers := make([]*pdgc.GCBarrierInfo, 0, len(internalState.gcBarriers))
	for barrierID, barrierTS := range internalState.gcBarriers {
		gcBarriers = append(gcBarriers, pdgc.NewGCBarrierInfo(barrierID, barrierTS, pdgc.TTLNeverExpire, startTime))
	}
	res.GCBarriers = gcBarriers

	return res, nil
}

func (c gcStatesClient) SetGlobalGCBarrier(ctx context.Context, barrierID string, barrierTS uint64, ttl time.Duration) (*pdgc.GlobalGCBarrierInfo, error) {
	panic("unimplemented")
}

func (c gcStatesClient) DeleteGlobalGCBarrier(ctx context.Context, barrierID string) (*pdgc.GlobalGCBarrierInfo, error) {
	panic("unimplemented")
}

func (c gcStatesClient) GetAllKeyspacesGCStates(ctx context.Context) (pdgc.ClusterGCStates, error) {
	panic("unimplemented")
}
>>>>>>> 524a282a213 (*: update pd client and client go (#65044))
