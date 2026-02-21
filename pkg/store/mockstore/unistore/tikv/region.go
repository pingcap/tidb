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
	"encoding/binary"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/store/mockstore/unistore/metrics"
	"github.com/pingcap/tidb/pkg/util/codec"
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
	for i := range 256 {
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

func newRegionCtx(meta *metapb.Region, latches *latches, _ any) *regionCtx {
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

