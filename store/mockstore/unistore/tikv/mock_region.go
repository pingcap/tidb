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
	"sort"
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
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/mockstore/unistore/cophandler"
	"github.com/pingcap/tidb/store/mockstore/unistore/tikv/dbreader"
	"github.com/pingcap/tidb/store/mockstore/unistore/tikv/mvcc"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
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
	max := atomic.AddUint64(&rm.id, uint64(n))
	ids := make([]uint64, n)
	base := max - uint64(n-1)
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
func (rm *MockRegionManager) GetRegionByKey(key []byte) (region *metapb.Region, peer *metapb.Peer, buckets *metapb.Buckets) {
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
		return nil, nil, nil
	}
	return proto.Clone(region).(*metapb.Region), proto.Clone(region.Peers[0]).(*metapb.Peer), nil
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
	if _, err := rm.splitKeys(keys); err != nil {
		panic(err)
	}
}

// SplitIndex evenly splits the data in index into count regions.
func (rm *MockRegionManager) SplitIndex(tableID, indexID int64, count int) {
	indexStart := tablecodec.EncodeTableIndexPrefix(tableID, indexID)
	indexEnd := indexStart.PrefixNext()
	keys := rm.calculateSplitKeys(indexStart, indexEnd, count)
	if _, err := rm.splitKeys(keys); err != nil {
		panic(err)
	}
}

// SplitKeys evenly splits the start, end key into "count" regions.
func (rm *MockRegionManager) SplitKeys(start, end kv.Key, count int) {
	keys := rm.calculateSplitKeys(start, end, count)
	if _, err := rm.splitKeys(keys); err != nil {
		panic(err)
	}
}

// SplitRegion implements the RegionManager interface.
func (rm *MockRegionManager) SplitRegion(req *kvrpcpb.SplitRegionRequest) *kvrpcpb.SplitRegionResponse {
	if _, err := rm.GetRegionFromCtx(req.Context); err != nil {
		return &kvrpcpb.SplitRegionResponse{RegionError: err}
	}
	splitKeys := make([][]byte, 0, len(req.SplitKeys))
	for _, rawKey := range req.SplitKeys {
		splitKeys = append(splitKeys, codec.EncodeBytes(nil, rawKey))
	}
	sort.Slice(splitKeys, func(i, j int) bool {
		return bytes.Compare(splitKeys[i], splitKeys[j]) < 0
	})

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

func (rm *MockRegionManager) splitKeys(keys [][]byte) ([]*regionCtx, error) {
	rm.mu.Lock()
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
	rm.mu.Unlock()
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
func (rm *MockRegionManager) ScanRegions(startKey, endKey []byte, limit int) []*pdclient.Region {
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
	r, p, b := pd.rm.GetRegionByKey(key)
	return &pdclient.Region{Meta: r, Leader: p, Buckets: b}, nil
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

	ts := time.Now().UnixNano() / int64(time.Millisecond)
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
func (pd *MockPD) ScanRegions(ctx context.Context, startKey []byte, endKey []byte, limit int) ([]*pdclient.Region, error) {
	regions := pd.rm.ScanRegions(startKey, endKey, limit)
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
