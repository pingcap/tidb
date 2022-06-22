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
	"encoding/binary"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pingcap/badger"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/store/mockstore/unistore/metrics"
	"github.com/pingcap/tidb/store/mockstore/unistore/pd"
	"github.com/pingcap/tidb/store/mockstore/unistore/tikv/mvcc"
	"github.com/pingcap/tidb/util/codec"
	"go.uber.org/zap"
)

// InternalKey
var (
	InternalKeyPrefix        = []byte{0xff}
	InternalRegionMetaPrefix = append(InternalKeyPrefix, "region"...)
	InternalStoreMetaKey     = append(InternalKeyPrefix, "store"...)
	InternalSafePointKey     = append(InternalKeyPrefix, "safepoint"...)
)

// InternalRegionMetaKey returns internal region meta key with the given region id.
func InternalRegionMetaKey(regionID uint64) []byte {
	return []byte(string(InternalRegionMetaPrefix) + strconv.FormatUint(regionID, 10))
}

// RegionCtx defines the region context interface.
type RegionCtx interface {
	Meta() *metapb.Region
	Diff() *int64
	RawStart() []byte
	RawEnd() []byte
	AcquireLatches(hashes []uint64)
	ReleaseLatches(hashes []uint64)
}

type regionCtx struct {
	meta            *metapb.Region
	regionEpoch     unsafe.Pointer // *metapb.RegionEpoch
	rawStartKey     []byte
	rawEndKey       []byte
	approximateSize int64
	diff            int64

	latches *latches
}

type latches struct {
	slots [256]map[uint64]*sync.WaitGroup
	locks [256]sync.Mutex
}

func newLatches() *latches {
	l := &latches{}
	for i := 0; i < 256; i++ {
		l.slots[i] = map[uint64]*sync.WaitGroup{}
	}
	return l
}

func (l *latches) acquire(keyHashes []uint64) (waitCnt int) {
	wg := new(sync.WaitGroup)
	wg.Add(1)
	for _, hash := range keyHashes {
		waitCnt += l.acquireOne(hash, wg)
	}
	return
}

func (l *latches) acquireOne(hash uint64, wg *sync.WaitGroup) (waitCnt int) {
	slotID := hash >> 56
	for {
		m := l.slots[slotID]
		l.locks[slotID].Lock()
		w, ok := m[hash]
		if !ok {
			m[hash] = wg
		}
		l.locks[slotID].Unlock()
		if ok {
			w.Wait()
			waitCnt++
			continue
		}
		return
	}
}

func (l *latches) release(keyHashes []uint64) {
	var w *sync.WaitGroup
	for _, hash := range keyHashes {
		slotID := hash >> 56
		l.locks[slotID].Lock()
		m := l.slots[slotID]
		if w == nil {
			w = m[hash]
		}
		delete(m, hash)
		l.locks[slotID].Unlock()
	}
	if w != nil {
		w.Done()
	}
}

func newRegionCtx(meta *metapb.Region, latches *latches, _ interface{}) *regionCtx {
	regCtx := &regionCtx{
		meta:        meta,
		latches:     latches,
		regionEpoch: unsafe.Pointer(meta.GetRegionEpoch()),
	}
	regCtx.rawStartKey = regCtx.decodeRawStartKey()
	regCtx.rawEndKey = regCtx.decodeRawEndKey()
	if len(regCtx.rawEndKey) == 0 {
		// Avoid reading internal meta data.
		regCtx.rawEndKey = InternalKeyPrefix
	}
	return regCtx
}

func (ri *regionCtx) Meta() *metapb.Region {
	return ri.meta
}

func (ri *regionCtx) Diff() *int64 {
	return &ri.diff
}

func (ri *regionCtx) RawStart() []byte {
	return ri.rawStartKey
}

func (ri *regionCtx) RawEnd() []byte {
	return ri.rawEndKey
}

func (ri *regionCtx) getRegionEpoch() *metapb.RegionEpoch {
	return (*metapb.RegionEpoch)(atomic.LoadPointer(&ri.regionEpoch))
}

func (ri *regionCtx) updateRegionEpoch(epoch *metapb.RegionEpoch) {
	atomic.StorePointer(&ri.regionEpoch, (unsafe.Pointer)(epoch))
}

func (ri *regionCtx) decodeRawStartKey() []byte {
	if len(ri.meta.StartKey) == 0 {
		return nil
	}
	_, rawKey, err := codec.DecodeBytes(ri.meta.StartKey, nil)
	if err != nil {
		panic("invalid region start key")
	}
	return rawKey
}

func (ri *regionCtx) decodeRawEndKey() []byte {
	if len(ri.meta.EndKey) == 0 {
		return nil
	}
	_, rawKey, err := codec.DecodeBytes(ri.meta.EndKey, nil)
	if err != nil {
		panic("invalid region end key")
	}
	return rawKey
}

func (ri *regionCtx) greaterEqualEndKey(key []byte) bool {
	return len(ri.rawEndKey) > 0 && bytes.Compare(key, ri.rawEndKey) >= 0
}

func newPeerMeta(peerID, storeID uint64) *metapb.Peer {
	return &metapb.Peer{
		Id:      peerID,
		StoreId: storeID,
	}
}

func (ri *regionCtx) incConfVer() {
	ri.meta.RegionEpoch = &metapb.RegionEpoch{
		ConfVer: ri.meta.GetRegionEpoch().GetConfVer() + 1,
		Version: ri.meta.GetRegionEpoch().GetVersion(),
	}
	ri.updateRegionEpoch(ri.meta.RegionEpoch)
}

func (ri *regionCtx) addPeer(peerID, storeID uint64) {
	ri.meta.Peers = append(ri.meta.Peers, newPeerMeta(peerID, storeID))
	ri.incConfVer()
}

func (ri *regionCtx) unmarshal(data []byte) error {
	ri.approximateSize = int64(binary.LittleEndian.Uint64(data))
	data = data[8:]
	ri.meta = &metapb.Region{}
	err := ri.meta.Unmarshal(data)
	if err != nil {
		return errors.Trace(err)
	}
	ri.rawStartKey = ri.decodeRawStartKey()
	ri.rawEndKey = ri.decodeRawEndKey()
	ri.regionEpoch = unsafe.Pointer(ri.meta.RegionEpoch)
	return nil
}

func (ri *regionCtx) marshal() []byte {
	data := make([]byte, 8+ri.meta.Size())
	binary.LittleEndian.PutUint64(data, uint64(ri.approximateSize))
	_, err := ri.meta.MarshalTo(data[8:])
	if err != nil {
		log.Error("region ctx marshal failed", zap.Error(err))
	}
	return data
}

// AcquireLatches add latches for all input hashVals, the input hashVals should be
// sorted and have no duplicates
func (ri *regionCtx) AcquireLatches(hashVals []uint64) {
	start := time.Now()
	waitCnt := ri.latches.acquire(hashVals)
	dur := time.Since(start)
	metrics.LatchWait.Observe(dur.Seconds())
	if dur > time.Millisecond*50 {
		var id string
		if ri.meta == nil {
			id = "unknown"
		} else {
			id = strconv.FormatUint(ri.meta.Id, 10)
		}
		log.S().Warnf("region %s acquire %d locks takes %v, waitCnt %d", id, len(hashVals), dur, waitCnt)
	}
}

func (ri *regionCtx) ReleaseLatches(hashVals []uint64) {
	ri.latches.release(hashVals)
}

// RegionOptions represents the region options.
type RegionOptions struct {
	StoreAddr  string
	PDAddr     string
	RegionSize int64
}

// RegionManager defines the region manager interface.
type RegionManager interface {
	GetRegionFromCtx(ctx *kvrpcpb.Context) (RegionCtx, *errorpb.Error)
	GetStoreInfoFromCtx(ctx *kvrpcpb.Context) (string, uint64, *errorpb.Error)
	SplitRegion(req *kvrpcpb.SplitRegionRequest) *kvrpcpb.SplitRegionResponse
	GetStoreIDByAddr(addr string) (uint64, error)
	GetStoreAddrByStoreID(storeID uint64) (string, error)
	Close() error
}

type regionManager struct {
	storeMeta *metapb.Store
	mu        sync.RWMutex
	regions   map[uint64]*regionCtx
	latches   *latches
}

func (rm *regionManager) GetStoreIDByAddr(addr string) (uint64, error) {
	if rm.storeMeta.Address != addr {
		return 0, errors.New("store not match")
	}
	return rm.storeMeta.Id, nil
}

func (rm *regionManager) GetStoreAddrByStoreID(storeID uint64) (string, error) {
	if rm.storeMeta.Id != storeID {
		return "", errors.New("store not match")
	}
	return rm.storeMeta.Address, nil
}

func (rm *regionManager) GetStoreInfoFromCtx(ctx *kvrpcpb.Context) (string, uint64, *errorpb.Error) {
	if ctx.GetPeer() != nil && ctx.GetPeer().GetStoreId() != rm.storeMeta.Id {
		return "", 0, &errorpb.Error{
			Message:       "store not match",
			StoreNotMatch: &errorpb.StoreNotMatch{},
		}
	}
	return rm.storeMeta.Address, rm.storeMeta.Id, nil
}

func (rm *regionManager) GetRegionFromCtx(ctx *kvrpcpb.Context) (RegionCtx, *errorpb.Error) {
	ctxPeer := ctx.GetPeer()
	if ctxPeer != nil && ctxPeer.GetStoreId() != rm.storeMeta.Id {
		return nil, &errorpb.Error{
			Message:       "store not match",
			StoreNotMatch: &errorpb.StoreNotMatch{},
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

func (rm *regionManager) isEpochStale(lhs, rhs *metapb.RegionEpoch) bool {
	return lhs.GetConfVer() != rhs.GetConfVer() || lhs.GetVersion() != rhs.GetVersion()
}

func (rm *regionManager) loadFromLocal(bundle *mvcc.DBBundle, f func(*regionCtx)) error {
	err := bundle.DB.View(func(txn *badger.Txn) error {
		item, err1 := txn.Get(InternalStoreMetaKey)
		if err1 != nil {
			return err1
		}
		val, err1 := item.Value()
		if err1 != nil {
			return err1
		}
		err1 = rm.storeMeta.Unmarshal(val)
		if err1 != nil {
			return err1
		}
		// load region meta
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()
		prefix := InternalRegionMetaPrefix
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			val, err1 = item.Value()
			if err1 != nil {
				return err1
			}
			r := new(regionCtx)
			err := r.unmarshal(val)
			if err != nil {
				return errors.Trace(err)
			}
			r.latches = rm.latches
			rm.regions[r.meta.Id] = r
			f(r)
		}
		return nil
	})
	if err == badger.ErrKeyNotFound {
		err = nil
	}
	return err
}

// StandAloneRegionManager represents a standalone region manager.
type StandAloneRegionManager struct {
	regionManager
	bundle     *mvcc.DBBundle
	pdc        pd.Client
	clusterID  uint64
	regionSize int64
	closeCh    chan struct{}
	wg         sync.WaitGroup
}

// NewStandAloneRegionManager returns a new standalone region manager.
func NewStandAloneRegionManager(bundle *mvcc.DBBundle, opts RegionOptions, pdc pd.Client) *StandAloneRegionManager {
	var err error
	clusterID := pdc.GetClusterID(context.TODO())
	log.S().Infof("cluster id %v", clusterID)
	rm := &StandAloneRegionManager{
		bundle:     bundle,
		pdc:        pdc,
		clusterID:  clusterID,
		regionSize: opts.RegionSize,
		closeCh:    make(chan struct{}),
		regionManager: regionManager{
			regions:   make(map[uint64]*regionCtx),
			storeMeta: new(metapb.Store),
			latches:   newLatches(),
		},
	}
	err = rm.loadFromLocal(bundle, func(r *regionCtx) {
		req := &pdpb.RegionHeartbeatRequest{
			Region:          r.meta,
			Leader:          r.meta.Peers[0],
			ApproximateSize: uint64(r.approximateSize),
		}
		rm.pdc.ReportRegion(req)
	})
	if err != nil {
		log.Fatal("load from local failed", zap.Error(err))
	}
	if rm.storeMeta.Id == 0 {
		err = rm.initStore(opts.StoreAddr)
		if err != nil {
			log.Fatal("init store failed", zap.Error(err))
		}
	}
	rm.storeMeta.Address = opts.StoreAddr
	err = rm.pdc.PutStore(context.TODO(), rm.storeMeta)
	if err != nil {
		log.Fatal("put store failed", zap.Error(err))
	}
	rm.wg.Add(2)
	go rm.runSplitWorker()
	go rm.storeHeartBeatLoop()
	return rm
}

func (rm *StandAloneRegionManager) initStore(storeAddr string) error {
	log.Info("initializing store")
	ids, err := rm.allocIDs(3)
	if err != nil {
		return err
	}
	storeID, regionID, peerID := ids[0], ids[1], ids[2]
	rm.storeMeta.Id = storeID
	rm.storeMeta.Address = storeAddr
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	rootRegion := &metapb.Region{
		Id:          regionID,
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
		Peers:       []*metapb.Peer{{Id: peerID, StoreId: storeID}},
	}
	rm.regions[rootRegion.Id] = newRegionCtx(rootRegion, rm.latches, nil)
	_, err = rm.pdc.Bootstrap(ctx, rm.storeMeta, rootRegion)
	cancel()
	if err != nil {
		log.Fatal("initialize failed", zap.Error(err))
	}
	rm.initialSplit(rootRegion)
	storeBuf, err := rm.storeMeta.Marshal()
	if err != nil {
		log.Fatal("marshal store meta failed", zap.Error(err))
	}
	err = rm.bundle.DB.Update(func(txn *badger.Txn) error {
		ts := atomic.AddUint64(&rm.bundle.StateTS, 1)
		err = txn.SetEntry(&badger.Entry{
			Key:   y.KeyWithTs(InternalStoreMetaKey, ts),
			Value: storeBuf,
		})
		if err != nil {
			return err
		}
		for rid, region := range rm.regions {
			regionBuf := region.marshal()
			err = txn.SetEntry(&badger.Entry{
				Key:   y.KeyWithTs(InternalRegionMetaKey(rid), ts),
				Value: regionBuf,
			})
			if err != nil {
				log.Fatal("save region info failed", zap.Error(err))
			}
		}
		return nil
	})
	for _, region := range rm.regions {
		req := &pdpb.RegionHeartbeatRequest{
			Region:          region.meta,
			Leader:          region.meta.Peers[0],
			ApproximateSize: uint64(region.approximateSize),
		}
		rm.pdc.ReportRegion(req)
	}
	log.Info("Initialize success")
	return nil
}

// initSplit splits the cluster into multiple regions.
func (rm *StandAloneRegionManager) initialSplit(root *metapb.Region) {
	root.EndKey = codec.EncodeBytes(nil, []byte{'m'})
	root.RegionEpoch.Version = 2
	rm.regions[root.Id] = newRegionCtx(root, rm.latches, nil)
	preSplitStartKeys := [][]byte{{'m'}, {'n'}, {'t'}, {'u'}}
	ids, err := rm.allocIDs(len(preSplitStartKeys) * 2)
	if err != nil {
		log.Fatal("alloc ids failed", zap.Error(err))
	}
	for i, startKey := range preSplitStartKeys {
		var endKey []byte
		if i < len(preSplitStartKeys)-1 {
			endKey = codec.EncodeBytes(nil, preSplitStartKeys[i+1])
		}
		newRegion := &metapb.Region{
			Id:          ids[i*2],
			RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
			Peers:       []*metapb.Peer{{Id: ids[i*2+1], StoreId: rm.storeMeta.Id}},
			StartKey:    codec.EncodeBytes(nil, startKey),
			EndKey:      endKey,
		}
		rm.regions[newRegion.Id] = newRegionCtx(newRegion, rm.latches, nil)
	}
}

func (rm *StandAloneRegionManager) allocIDs(n int) ([]uint64, error) {
	ids := make([]uint64, n)
	for i := 0; i < n; i++ {
		id, err := rm.pdc.AllocID(context.Background())
		if err != nil {
			return nil, errors.Trace(err)
		}
		ids[i] = id
	}
	return ids, nil
}

func (rm *StandAloneRegionManager) storeHeartBeatLoop() {
	defer rm.wg.Done()
	ticker := time.Tick(time.Second * 3)
	for {
		select {
		case <-rm.closeCh:
			return
		case <-ticker:
		}
		storeStats := new(pdpb.StoreStats)
		storeStats.StoreId = rm.storeMeta.Id
		storeStats.Available = 1024 * 1024 * 1024
		rm.mu.RLock()
		storeStats.RegionCount = uint32(len(rm.regions))
		rm.mu.RUnlock()
		storeStats.Capacity = 2048 * 1024 * 1024
		if err := rm.pdc.StoreHeartbeat(context.Background(), storeStats); err != nil {
			log.Warn("store heartbeat failed", zap.Error(err))
		}
	}
}

type keySample struct {
	key      []byte
	leftSize int64
}

// sampler samples keys in a region for later pick a split key.
type sampler struct {
	samples   [64]keySample
	length    int
	step      int
	scanned   int
	totalSize int64
}

func newSampler() *sampler {
	return &sampler{step: 1}
}

func (s *sampler) shrinkIfNeeded() {
	if s.length < len(s.samples) {
		return
	}
	for i := 0; i < len(s.samples)/2; i++ {
		s.samples[i], s.samples[i*2] = s.samples[i*2], s.samples[i]
	}
	s.length /= 2
	s.step *= 2
}

func (s *sampler) shouldSample() bool {
	// It's an optimization for 's.scanned % s.step == 0'
	return s.scanned&(s.step-1) == 0
}

func (s *sampler) scanKey(key []byte, size int64) {
	s.totalSize += size
	s.scanned++
	if s.shouldSample() {
		sample := s.samples[s.length]
		// safe copy the key.
		sample.key = append(sample.key[:0], key...)
		sample.leftSize = s.totalSize
		s.samples[s.length] = sample
		s.length++
		s.shrinkIfNeeded()
	}
}

func (s *sampler) getSplitKeyAndSize() ([]byte, int64) {
	targetSize := s.totalSize * 2 / 3
	for _, sample := range s.samples[:s.length] {
		if sample.leftSize >= targetSize {
			return sample.key, sample.leftSize
		}
	}
	return []byte{}, 0
}

func (rm *StandAloneRegionManager) runSplitWorker() {
	defer rm.wg.Done()
	ticker := time.NewTicker(time.Second * 5)
	var regionsToCheck []*regionCtx
	var regionsToSave []*regionCtx
	for {
		regionsToCheck = regionsToCheck[:0]
		rm.mu.RLock()
		for _, ri := range rm.regions {
			if ri.approximateSize+atomic.LoadInt64(&ri.diff) > rm.regionSize*3/2 {
				regionsToCheck = append(regionsToCheck, ri)
			}
		}
		rm.mu.RUnlock()
		for _, ri := range regionsToCheck {
			err := rm.splitCheckRegion(ri)
			if err != nil {
				log.Error("split region failed", zap.Error(err))
			}
		}

		regionsToSave = regionsToSave[:0]
		rm.mu.RLock()
		for _, ri := range rm.regions {
			if atomic.LoadInt64(&ri.diff) > rm.regionSize/8 {
				regionsToSave = append(regionsToSave, ri)
			}
		}
		rm.mu.RUnlock()
		rm.saveSize(regionsToSave)
		select {
		case <-rm.closeCh:
			return
		case <-ticker.C:
		}
	}
}

func (rm *StandAloneRegionManager) saveSize(regionsToSave []*regionCtx) {
	err1 := rm.bundle.DB.Update(func(txn *badger.Txn) error {
		ts := atomic.AddUint64(&rm.bundle.StateTS, 1)
		for _, ri := range regionsToSave {
			ri.approximateSize += atomic.LoadInt64(&ri.diff)
			err := txn.SetEntry(&badger.Entry{
				Key:   y.KeyWithTs(InternalRegionMetaKey(ri.meta.Id), ts),
				Value: ri.marshal(),
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err1 != nil {
		log.Error("region manager save size failed", zap.Error(err1))
	}
}

func (rm *StandAloneRegionManager) splitCheckRegion(region *regionCtx) error {
	s := newSampler()
	err := rm.bundle.DB.View(func(txn *badger.Txn) error {
		iter := txn.NewIterator(badger.IteratorOptions{})
		defer iter.Close()
		for iter.Seek(region.rawStartKey); iter.Valid(); iter.Next() {
			item := iter.Item()
			if region.greaterEqualEndKey(item.Key()) {
				break
			}
			s.scanKey(item.Key(), int64(len(item.Key())+item.ValueSize()))
		}
		return nil
	})
	if err != nil {
		log.Error("sample region failed", zap.Error(err))
		return errors.Trace(err)
	}
	// Need to update the diff to avoid split check again.
	atomic.StoreInt64(&region.diff, s.totalSize-region.approximateSize)
	if s.totalSize < rm.regionSize {
		return nil
	}
	splitKey, leftSize := s.getSplitKeyAndSize()
	log.Info("try to split region", zap.Uint64("id", region.meta.Id), zap.Binary("split key", splitKey),
		zap.Int64("left size", leftSize), zap.Int64("right size", s.totalSize-leftSize))
	err = rm.splitRegion(region, splitKey, s.totalSize, leftSize)
	if err != nil {
		log.Error("split region failed", zap.Error(err))
	}
	return errors.Trace(err)
}

func (rm *StandAloneRegionManager) splitRegion(oldRegionCtx *regionCtx, splitKey []byte, oldSize, leftSize int64) error {
	oldRegion := oldRegionCtx.meta
	rightMeta := &metapb.Region{
		Id:       oldRegion.Id,
		StartKey: codec.EncodeBytes(nil, splitKey),
		EndKey:   oldRegion.EndKey,
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: oldRegion.RegionEpoch.ConfVer,
			Version: oldRegion.RegionEpoch.Version + 1,
		},
		Peers: oldRegion.Peers,
	}
	right := newRegionCtx(rightMeta, rm.latches, nil)
	right.approximateSize = oldSize - leftSize
	id, err := rm.pdc.AllocID(context.Background())
	if err != nil {
		return errors.Trace(err)
	}
	leftMeta := &metapb.Region{
		Id:       id,
		StartKey: oldRegion.StartKey,
		EndKey:   codec.EncodeBytes(nil, splitKey),
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 1,
			Version: 1,
		},
		Peers: oldRegion.Peers,
	}
	left := newRegionCtx(leftMeta, rm.latches, nil)
	left.approximateSize = leftSize
	err1 := rm.bundle.DB.Update(func(txn *badger.Txn) error {
		ts := atomic.AddUint64(&rm.bundle.StateTS, 1)
		err := txn.SetEntry(&badger.Entry{
			Key:   y.KeyWithTs(InternalRegionMetaKey(left.meta.Id), ts),
			Value: left.marshal(),
		})
		if err != nil {
			return errors.Trace(err)
		}
		err = txn.SetEntry(&badger.Entry{
			Key:   y.KeyWithTs(InternalRegionMetaKey(right.meta.Id), ts),
			Value: right.marshal(),
		})
		return errors.Trace(err)
	})
	if err1 != nil {
		return errors.Trace(err1)
	}
	rm.mu.Lock()
	rm.regions[left.meta.Id] = left
	rm.regions[right.meta.Id] = right
	rm.mu.Unlock()
	rm.pdc.ReportRegion(&pdpb.RegionHeartbeatRequest{
		Region:          right.meta,
		Leader:          right.meta.Peers[0],
		ApproximateSize: uint64(right.approximateSize),
	})
	rm.pdc.ReportRegion(&pdpb.RegionHeartbeatRequest{
		Region:          left.meta,
		Leader:          left.meta.Peers[0],
		ApproximateSize: uint64(left.approximateSize),
	})
	log.Info("region splitted", zap.Uint64("old id", oldRegion.Id),
		zap.Uint64("left id", left.meta.Id), zap.Int64("left size", left.approximateSize),
		zap.Uint64("right id", right.meta.Id), zap.Int64("right size", right.approximateSize))
	return nil
}

// SplitRegion splits a region.
func (rm *StandAloneRegionManager) SplitRegion(req *kvrpcpb.SplitRegionRequest) *kvrpcpb.SplitRegionResponse {
	return &kvrpcpb.SplitRegionResponse{}
}

// Close closes the standalone region manager.
func (rm *StandAloneRegionManager) Close() error {
	close(rm.closeCh)
	rm.wg.Wait()
	return nil
}
