// Copyright 2019 PingCAP, Inc.
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

package local

import (
	"bytes"
	"context"
	"math"
	"math/rand"
	"sort"
	"sync"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/br/pkg/lightning/glue"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule/placement"
	"go.uber.org/atomic"
)

type testClient struct {
	mu           sync.RWMutex
	stores       map[uint64]*metapb.Store
	regions      map[uint64]*restore.RegionInfo
	regionsInfo  *core.RegionsInfo // For now it's only used in ScanRegions
	nextRegionID uint64
	splitCount   atomic.Int32
	hook         clientHook
}

func newTestClient(
	stores map[uint64]*metapb.Store,
	regions map[uint64]*restore.RegionInfo,
	nextRegionID uint64,
	hook clientHook,
) *testClient {
	regionsInfo := core.NewRegionsInfo()
	for _, regionInfo := range regions {
		regionsInfo.SetRegion(core.NewRegionInfo(regionInfo.Region, regionInfo.Leader))
	}
	return &testClient{
		stores:       stores,
		regions:      regions,
		regionsInfo:  regionsInfo,
		nextRegionID: nextRegionID,
		hook:         hook,
	}
}

func (c *testClient) GetAllRegions() map[uint64]*restore.RegionInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.regions
}

func (c *testClient) GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	store, ok := c.stores[storeID]
	if !ok {
		return nil, errors.Errorf("store not found")
	}
	return store, nil
}

func (c *testClient) GetRegion(ctx context.Context, key []byte) (*restore.RegionInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, region := range c.regions {
		if bytes.Compare(key, region.Region.StartKey) >= 0 &&
			(len(region.Region.EndKey) == 0 || bytes.Compare(key, region.Region.EndKey) < 0) {
			return region, nil
		}
	}
	return nil, errors.Errorf("region not found: key=%s", string(key))
}

func (c *testClient) GetRegionByID(ctx context.Context, regionID uint64) (*restore.RegionInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	region, ok := c.regions[regionID]
	if !ok {
		return nil, errors.Errorf("region not found: id=%d", regionID)
	}
	return region, nil
}

func (c *testClient) SplitRegion(
	ctx context.Context,
	regionInfo *restore.RegionInfo,
	key []byte,
) (*restore.RegionInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var target *restore.RegionInfo
	splitKey := codec.EncodeBytes([]byte{}, key)
	for _, region := range c.regions {
		if bytes.Compare(splitKey, region.Region.StartKey) >= 0 &&
			(len(region.Region.EndKey) == 0 || bytes.Compare(splitKey, region.Region.EndKey) < 0) {
			target = region
		}
	}
	if target == nil {
		return nil, errors.Errorf("region not found: key=%s", string(key))
	}
	newRegion := &restore.RegionInfo{
		Region: &metapb.Region{
			Peers:    target.Region.Peers,
			Id:       c.nextRegionID,
			StartKey: target.Region.StartKey,
			EndKey:   splitKey,
			RegionEpoch: &metapb.RegionEpoch{
				Version: target.Region.RegionEpoch.Version,
				ConfVer: target.Region.RegionEpoch.ConfVer + 1,
			},
		},
	}
	c.regions[c.nextRegionID] = newRegion
	c.regionsInfo.SetRegion(core.NewRegionInfo(newRegion.Region, newRegion.Leader))
	c.nextRegionID++
	target.Region.StartKey = splitKey
	target.Region.RegionEpoch.ConfVer++
	c.regions[target.Region.Id] = target
	c.regionsInfo.SetRegion(core.NewRegionInfo(target.Region, target.Leader))
	return newRegion, nil
}

func (c *testClient) BatchSplitRegionsWithOrigin(
	ctx context.Context, regionInfo *restore.RegionInfo, keys [][]byte,
) (*restore.RegionInfo, []*restore.RegionInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.splitCount.Inc()

	if c.hook != nil {
		regionInfo, keys = c.hook.BeforeSplitRegion(ctx, regionInfo, keys)
	}
	if len(keys) == 0 {
		return nil, nil, errors.New("no valid key")
	}

	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	default:
	}

	newRegions := make([]*restore.RegionInfo, 0)
	target, ok := c.regions[regionInfo.Region.Id]
	if !ok {
		return nil, nil, errors.New("region not found")
	}
	if target.Region.RegionEpoch.Version != regionInfo.Region.RegionEpoch.Version ||
		target.Region.RegionEpoch.ConfVer != regionInfo.Region.RegionEpoch.ConfVer {
		return regionInfo, nil, errors.New("epoch not match")
	}
	splitKeys := make([][]byte, 0, len(keys))
	for _, k := range keys {
		splitKey := codec.EncodeBytes([]byte{}, k)
		splitKeys = append(splitKeys, splitKey)
	}
	sort.Slice(splitKeys, func(i, j int) bool {
		return bytes.Compare(splitKeys[i], splitKeys[j]) < 0
	})

	startKey := target.Region.StartKey
	for _, key := range splitKeys {
		if bytes.Compare(key, startKey) <= 0 || bytes.Compare(key, target.Region.EndKey) >= 0 {
			continue
		}
		newRegion := &restore.RegionInfo{
			Region: &metapb.Region{
				Peers:    target.Region.Peers,
				Id:       c.nextRegionID,
				StartKey: startKey,
				EndKey:   key,
			},
		}
		c.regions[c.nextRegionID] = newRegion
		c.regionsInfo.SetRegion(core.NewRegionInfo(newRegion.Region, newRegion.Leader))
		c.nextRegionID++
		startKey = key
		newRegions = append(newRegions, newRegion)
	}
	if !bytes.Equal(target.Region.StartKey, startKey) {
		target.Region.StartKey = startKey
		c.regions[target.Region.Id] = target
		c.regionsInfo.SetRegion(core.NewRegionInfo(target.Region, target.Leader))
	}

	if len(newRegions) == 0 {
		return target, nil, errors.New("no valid key")
	}

	var err error
	if c.hook != nil {
		newRegions, err = c.hook.AfterSplitRegion(ctx, target, keys, newRegions, nil)
	}

	return target, newRegions, err
}

func (c *testClient) BatchSplitRegions(
	ctx context.Context, regionInfo *restore.RegionInfo, keys [][]byte,
) ([]*restore.RegionInfo, error) {
	_, newRegions, err := c.BatchSplitRegionsWithOrigin(ctx, regionInfo, keys)
	return newRegions, err
}

func (c *testClient) ScatterRegion(ctx context.Context, regionInfo *restore.RegionInfo) error {
	return nil
}

func (c *testClient) GetOperator(ctx context.Context, regionID uint64) (*pdpb.GetOperatorResponse, error) {
	return &pdpb.GetOperatorResponse{
		Header: new(pdpb.ResponseHeader),
	}, nil
}

func (c *testClient) ScanRegions(ctx context.Context, key, endKey []byte, limit int) ([]*restore.RegionInfo, error) {
	if c.hook != nil {
		key, endKey, limit = c.hook.BeforeScanRegions(ctx, key, endKey, limit)
	}

	infos := c.regionsInfo.ScanRange(key, endKey, limit)
	regions := make([]*restore.RegionInfo, 0, len(infos))
	for _, info := range infos {
		regions = append(regions, &restore.RegionInfo{
			Region: info.GetMeta(),
			Leader: info.GetLeader(),
		})
	}

	var err error
	if c.hook != nil {
		regions, err = c.hook.AfterScanRegions(regions, nil)
	}
	return regions, err
}

func (c *testClient) GetPlacementRule(ctx context.Context, groupID, ruleID string) (r placement.Rule, err error) {
	return
}

func (c *testClient) SetPlacementRule(ctx context.Context, rule placement.Rule) error {
	return nil
}

func (c *testClient) DeletePlacementRule(ctx context.Context, groupID, ruleID string) error {
	return nil
}

func (c *testClient) SetStoresLabel(ctx context.Context, stores []uint64, labelKey, labelValue string) error {
	return nil
}

func cloneRegion(region *restore.RegionInfo) *restore.RegionInfo {
	r := &metapb.Region{}
	if region.Region != nil {
		b, _ := region.Region.Marshal()
		_ = r.Unmarshal(b)
	}

	l := &metapb.Peer{}
	if region.Region != nil {
		b, _ := region.Region.Marshal()
		_ = l.Unmarshal(b)
	}
	return &restore.RegionInfo{Region: r, Leader: l}
}

// region: [, aay), [aay, bba), [bba, bbh), [bbh, cca), [cca, )
func initTestClient(keys [][]byte, hook clientHook) *testClient {
	peers := make([]*metapb.Peer, 1)
	peers[0] = &metapb.Peer{
		Id:      1,
		StoreId: 1,
	}
	regions := make(map[uint64]*restore.RegionInfo)
	for i := uint64(1); i < uint64(len(keys)); i++ {
		startKey := keys[i-1]
		if len(startKey) != 0 {
			startKey = codec.EncodeBytes([]byte{}, startKey)
		}
		endKey := keys[i]
		if len(endKey) != 0 {
			endKey = codec.EncodeBytes([]byte{}, endKey)
		}
		regions[i] = &restore.RegionInfo{
			Region: &metapb.Region{
				Id:          i,
				Peers:       peers,
				StartKey:    startKey,
				EndKey:      endKey,
				RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
			},
		}
	}
	stores := make(map[uint64]*metapb.Store)
	stores[1] = &metapb.Store{
		Id: 1,
	}
	return newTestClient(stores, regions, uint64(len(keys)), hook)
}

func checkRegionRanges(c *C, regions []*restore.RegionInfo, keys [][]byte) {
	for i, r := range regions {
		_, regionStart, _ := codec.DecodeBytes(r.Region.StartKey, []byte{})
		_, regionEnd, _ := codec.DecodeBytes(r.Region.EndKey, []byte{})
		c.Assert(regionStart, DeepEquals, keys[i])
		c.Assert(regionEnd, DeepEquals, keys[i+1])
	}
}

type clientHook interface {
	BeforeSplitRegion(ctx context.Context, regionInfo *restore.RegionInfo, keys [][]byte) (*restore.RegionInfo, [][]byte)
	AfterSplitRegion(context.Context, *restore.RegionInfo, [][]byte, []*restore.RegionInfo, error) ([]*restore.RegionInfo, error)
	BeforeScanRegions(ctx context.Context, key, endKey []byte, limit int) ([]byte, []byte, int)
	AfterScanRegions([]*restore.RegionInfo, error) ([]*restore.RegionInfo, error)
}

type noopHook struct{}

func (h *noopHook) BeforeSplitRegion(ctx context.Context, regionInfo *restore.RegionInfo, keys [][]byte) (*restore.RegionInfo, [][]byte) {
	delayTime := rand.Int31n(10) + 1
	time.Sleep(time.Duration(delayTime) * time.Millisecond)
	return regionInfo, keys
}

func (h *noopHook) AfterSplitRegion(c context.Context, r *restore.RegionInfo, keys [][]byte, res []*restore.RegionInfo, err error) ([]*restore.RegionInfo, error) {
	return res, err
}

func (h *noopHook) BeforeScanRegions(ctx context.Context, key, endKey []byte, limit int) ([]byte, []byte, int) {
	return key, endKey, limit
}

func (h *noopHook) AfterScanRegions(res []*restore.RegionInfo, err error) ([]*restore.RegionInfo, error) {
	return res, err
}

type batchSplitHook interface {
	setup(c *C) func()
	check(c *C, cli *testClient)
}

type defaultHook struct{}

func (d defaultHook) setup(*C) func() {
	oldLimit := maxBatchSplitKeys
	oldSplitBackoffTime := splitRegionBaseBackOffTime
	maxBatchSplitKeys = 4
	splitRegionBaseBackOffTime = time.Millisecond
	return func() {
		maxBatchSplitKeys = oldLimit
		splitRegionBaseBackOffTime = oldSplitBackoffTime
	}
}

func (d defaultHook) check(c *C, cli *testClient) {
	// so with a batch split size of 4, there will be 7 time batch split
	// 1. region: [aay, bba), keys: [b, ba, bb]
	// 2. region: [bbh, cca), keys: [bc, bd, be, bf]
	// 3. region: [bf, cca), keys: [bg, bh, bi, bj]
	// 4. region: [bj, cca), keys: [bk, bl, bm, bn]
	// 5. region: [bn, cca), keys: [bo, bp, bq, br]
	// 6. region: [br, cca), keys: [bs, bt, bu, bv]
	// 7. region: [bv, cca), keys: [bw, bx, by, bz]

	// since it may encounter error retries, here only check the lower threshold.
	c.Assert(cli.splitCount.Load() >= 7, IsTrue)
}

func (s *localSuite) doTestBatchSplitRegionByRanges(ctx context.Context, c *C, hook clientHook, errPat string, splitHook batchSplitHook) {
	if splitHook == nil {
		splitHook = defaultHook{}
	}
	deferFunc := splitHook.setup(c)
	defer deferFunc()

	keys := [][]byte{[]byte(""), []byte("aay"), []byte("bba"), []byte("bbh"), []byte("cca"), []byte("")}
	client := initTestClient(keys, hook)
	local := &local{
		splitCli: client,
		g:        glue.NewExternalTiDBGlue(nil, mysql.ModeNone),
	}

	// current region ranges: [, aay), [aay, bba), [bba, bbh), [bbh, cca), [cca, )
	rangeStart := codec.EncodeBytes([]byte{}, []byte("b"))
	rangeEnd := codec.EncodeBytes([]byte{}, []byte("c"))
	regions, err := paginateScanRegion(ctx, client, rangeStart, rangeEnd, 5)
	c.Assert(err, IsNil)
	// regions is: [aay, bba), [bba, bbh), [bbh, cca)
	checkRegionRanges(c, regions, [][]byte{[]byte("aay"), []byte("bba"), []byte("bbh"), []byte("cca")})

	// generate:  ranges [b, ba), [ba, bb), [bb, bc), ... [by, bz)
	ranges := make([]Range, 0)
	start := []byte{'b'}
	for i := byte('a'); i <= 'z'; i++ {
		end := []byte{'b', i}
		ranges = append(ranges, Range{start: start, end: end})
		start = end
	}

	err = local.SplitAndScatterRegionByRanges(ctx, ranges, nil, true)
	if len(errPat) == 0 {
		c.Assert(err, IsNil)
	} else {
		c.Assert(err, ErrorMatches, errPat)
		return
	}

	splitHook.check(c, client)

	// check split ranges
	regions, err = paginateScanRegion(ctx, client, rangeStart, rangeEnd, 5)
	c.Assert(err, IsNil)
	result := [][]byte{
		[]byte("b"), []byte("ba"), []byte("bb"), []byte("bba"), []byte("bbh"), []byte("bc"),
		[]byte("bd"), []byte("be"), []byte("bf"), []byte("bg"), []byte("bh"), []byte("bi"), []byte("bj"),
		[]byte("bk"), []byte("bl"), []byte("bm"), []byte("bn"), []byte("bo"), []byte("bp"), []byte("bq"),
		[]byte("br"), []byte("bs"), []byte("bt"), []byte("bu"), []byte("bv"), []byte("bw"), []byte("bx"),
		[]byte("by"), []byte("bz"), []byte("cca"),
	}
	checkRegionRanges(c, regions, result)
}

func (s *localSuite) TestBatchSplitRegionByRanges(c *C) {
	s.doTestBatchSplitRegionByRanges(context.Background(), c, nil, "", nil)
}

type batchSizeHook struct{}

func (h batchSizeHook) setup(c *C) func() {
	oldSizeLimit := maxBatchSplitSize
	oldSplitBackoffTime := splitRegionBaseBackOffTime
	maxBatchSplitSize = 6
	splitRegionBaseBackOffTime = time.Millisecond
	return func() {
		maxBatchSplitSize = oldSizeLimit
		splitRegionBaseBackOffTime = oldSplitBackoffTime
	}
}

func (h batchSizeHook) check(c *C, cli *testClient) {
	// so with a batch split key size of 6, there will be 9 time batch split
	// 1. region: [aay, bba), keys: [b, ba, bb]
	// 2. region: [bbh, cca), keys: [bc, bd, be]
	// 3. region: [bf, cca), keys: [bf, bg, bh]
	// 4. region: [bj, cca), keys: [bi, bj, bk]
	// 5. region: [bj, cca), keys: [bl, bm, bn]
	// 6. region: [bn, cca), keys: [bo, bp, bq]
	// 7. region: [bn, cca), keys: [br, bs, bt]
	// 9. region: [br, cca), keys: [bu, bv, bw]
	// 10. region: [bv, cca), keys: [bx, by, bz]

	// since it may encounter error retries, here only check the lower threshold.
	c.Assert(cli.splitCount.Load(), Equals, int32(9))
}

func (s *localSuite) TestBatchSplitRegionByRangesKeySizeLimit(c *C) {
	s.doTestBatchSplitRegionByRanges(context.Background(), c, nil, "", batchSizeHook{})
}

type scanRegionEmptyHook struct {
	noopHook
	cnt int
}

func (h *scanRegionEmptyHook) AfterScanRegions(res []*restore.RegionInfo, err error) ([]*restore.RegionInfo, error) {
	h.cnt++
	// skip the first call
	if h.cnt == 1 {
		return res, err
	}
	return nil, err
}

func (s *localSuite) TestBatchSplitRegionByRangesScanFailed(c *C) {
	s.doTestBatchSplitRegionByRanges(context.Background(), c, &scanRegionEmptyHook{}, "paginate scan region returns empty result", defaultHook{})
}

type splitRegionEpochNotMatchHook struct {
	noopHook
}

func (h *splitRegionEpochNotMatchHook) BeforeSplitRegion(ctx context.Context, regionInfo *restore.RegionInfo, keys [][]byte) (*restore.RegionInfo, [][]byte) {
	regionInfo, keys = h.noopHook.BeforeSplitRegion(ctx, regionInfo, keys)
	regionInfo = cloneRegion(regionInfo)
	// decrease the region epoch, so split region will fail
	regionInfo.Region.RegionEpoch.Version--
	return regionInfo, keys
}

func (s *localSuite) TestBatchSplitByRangesEpochNotMatch(c *C) {
	s.doTestBatchSplitRegionByRanges(context.Background(), c, &splitRegionEpochNotMatchHook{}, "batch split regions failed: epoch not match.*", defaultHook{})
}

// return epoch not match error in every other call
type splitRegionEpochNotMatchHookRandom struct {
	noopHook
	cnt atomic.Int32
}

func (h *splitRegionEpochNotMatchHookRandom) BeforeSplitRegion(ctx context.Context, regionInfo *restore.RegionInfo, keys [][]byte) (*restore.RegionInfo, [][]byte) {
	regionInfo, keys = h.noopHook.BeforeSplitRegion(ctx, regionInfo, keys)
	if h.cnt.Inc() != 0 {
		return regionInfo, keys
	}
	regionInfo = cloneRegion(regionInfo)
	// decrease the region epoch, so split region will fail
	regionInfo.Region.RegionEpoch.Version--
	return regionInfo, keys
}

func (s *localSuite) TestBatchSplitByRangesEpochNotMatchOnce(c *C) {
	s.doTestBatchSplitRegionByRanges(context.Background(), c, &splitRegionEpochNotMatchHookRandom{}, "", defaultHook{})
}

type splitRegionNoValidKeyHook struct {
	noopHook
	returnErrTimes int32
	errorCnt       atomic.Int32
}

func (h *splitRegionNoValidKeyHook) BeforeSplitRegion(ctx context.Context, regionInfo *restore.RegionInfo, keys [][]byte) (*restore.RegionInfo, [][]byte) {
	regionInfo, keys = h.noopHook.BeforeSplitRegion(ctx, regionInfo, keys)
	if h.errorCnt.Inc() <= h.returnErrTimes {
		// clean keys to trigger "no valid keys" error
		keys = keys[:0]
	}
	return regionInfo, keys
}

func (s *localSuite) TestBatchSplitByRangesNoValidKeysOnce(c *C) {
	s.doTestBatchSplitRegionByRanges(context.Background(), c, &splitRegionNoValidKeyHook{returnErrTimes: 1}, "", defaultHook{})
}

func (s *localSuite) TestBatchSplitByRangesNoValidKeys(c *C) {
	s.doTestBatchSplitRegionByRanges(context.Background(), c, &splitRegionNoValidKeyHook{returnErrTimes: math.MaxInt32}, ".*no valid key.*", defaultHook{})
}

type reportAfterSplitHook struct {
	noopHook
	ch chan<- struct{}
}

func (h *reportAfterSplitHook) AfterSplitRegion(ctx context.Context, region *restore.RegionInfo, keys [][]byte, resultRegions []*restore.RegionInfo, err error) ([]*restore.RegionInfo, error) {
	h.ch <- struct{}{}
	return resultRegions, err
}

func (s *localSuite) TestBatchSplitByRangeCtxCanceled(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan struct{})
	// cancel ctx after the first region split success.
	go func() {
		i := 0
		for range ch {
			if i == 0 {
				cancel()
			}
			i++
		}
	}()

	s.doTestBatchSplitRegionByRanges(ctx, c, &reportAfterSplitHook{ch: ch}, ".*context canceled.*", defaultHook{})
	close(ch)
}

func (s *localSuite) doTestBatchSplitByRangesWithClusteredIndex(c *C, hook clientHook) {
	oldLimit := maxBatchSplitKeys
	oldSplitBackoffTime := splitRegionBaseBackOffTime
	maxBatchSplitKeys = 10
	splitRegionBaseBackOffTime = time.Millisecond
	defer func() {
		maxBatchSplitKeys = oldLimit
		splitRegionBaseBackOffTime = oldSplitBackoffTime
	}()

	stmtCtx := new(stmtctx.StatementContext)

	tableID := int64(1)
	tableStartKey := tablecodec.EncodeTablePrefix(tableID)
	tableEndKey := tablecodec.EncodeTablePrefix(tableID + 1)
	keys := [][]byte{[]byte(""), tableStartKey}
	// pre split 2 regions
	for i := int64(0); i < 2; i++ {
		keyBytes, err := codec.EncodeKey(stmtCtx, nil, types.NewIntDatum(i))
		c.Assert(err, IsNil)
		h, err := kv.NewCommonHandle(keyBytes)
		c.Assert(err, IsNil)
		key := tablecodec.EncodeRowKeyWithHandle(tableID, h)
		keys = append(keys, key)
	}
	keys = append(keys, tableEndKey, []byte(""))
	client := initTestClient(keys, hook)
	local := &local{
		splitCli: client,
		g:        glue.NewExternalTiDBGlue(nil, mysql.ModeNone),
	}
	ctx := context.Background()

	// we batch generate a batch of row keys for table 1 with common handle
	rangeKeys := make([][]byte, 0, 20+1)
	for i := int64(0); i < 2; i++ {
		for j := int64(0); j < 10; j++ {
			keyBytes, err := codec.EncodeKey(stmtCtx, nil, types.NewIntDatum(i), types.NewIntDatum(j*10000))
			c.Assert(err, IsNil)
			h, err := kv.NewCommonHandle(keyBytes)
			c.Assert(err, IsNil)
			key := tablecodec.EncodeRowKeyWithHandle(tableID, h)
			rangeKeys = append(rangeKeys, key)
		}
	}

	start := rangeKeys[0]
	ranges := make([]Range, 0, len(rangeKeys)-1)
	for _, e := range rangeKeys[1:] {
		ranges = append(ranges, Range{start: start, end: e})
		start = e
	}

	err := local.SplitAndScatterRegionByRanges(ctx, ranges, nil, true)
	c.Assert(err, IsNil)

	startKey := codec.EncodeBytes([]byte{}, rangeKeys[0])
	endKey := codec.EncodeBytes([]byte{}, rangeKeys[len(rangeKeys)-1])
	// check split ranges
	regions, err := paginateScanRegion(ctx, client, startKey, endKey, 5)
	c.Assert(err, IsNil)
	c.Assert(len(regions), Equals, len(ranges)+1)

	checkKeys := append([][]byte{}, rangeKeys[:10]...)
	checkKeys = append(checkKeys, keys[3])
	checkKeys = append(checkKeys, rangeKeys[10:]...)
	checkRegionRanges(c, regions, checkKeys)
}

func (s *localSuite) TestBatchSplitByRangesWithClusteredIndex(c *C) {
	s.doTestBatchSplitByRangesWithClusteredIndex(c, nil)
}

func (s *localSuite) TestBatchSplitByRangesWithClusteredIndexEpochNotMatch(c *C) {
	s.doTestBatchSplitByRangesWithClusteredIndex(c, &splitRegionEpochNotMatchHookRandom{})
}

func (s *localSuite) TestNeedSplit(c *C) {
	tableID := int64(1)
	peers := make([]*metapb.Peer, 1)
	peers[0] = &metapb.Peer{
		Id:      1,
		StoreId: 1,
	}
	keys := []int64{10, 100, 500, 1000, 999999, -1}
	start := tablecodec.EncodeRowKeyWithHandle(tableID, kv.IntHandle(0))
	regionStart := codec.EncodeBytes([]byte{}, start)
	regions := make([]*restore.RegionInfo, 0)
	for _, end := range keys {
		var regionEndKey []byte
		if end >= 0 {
			endKey := tablecodec.EncodeRowKeyWithHandle(tableID, kv.IntHandle(end))
			regionEndKey = codec.EncodeBytes([]byte{}, endKey)
		}
		region := &restore.RegionInfo{
			Region: &metapb.Region{
				Id:          1,
				Peers:       peers,
				StartKey:    regionStart,
				EndKey:      regionEndKey,
				RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
			},
		}
		regions = append(regions, region)
		regionStart = regionEndKey
	}

	checkMap := map[int64]int{
		0:         -1,
		5:         0,
		99:        1,
		100:       -1,
		512:       3,
		8888:      4,
		999999:    -1,
		100000000: 5,
	}

	for hdl, idx := range checkMap {
		checkKey := tablecodec.EncodeRowKeyWithHandle(tableID, kv.IntHandle(hdl))
		res := needSplit(checkKey, regions)
		if idx < 0 {
			c.Assert(res, IsNil)
		} else {
			c.Assert(res, DeepEquals, regions[idx])
		}
	}
}
