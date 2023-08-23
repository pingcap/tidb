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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package local

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/store/pdtypes"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func init() {
	// Reduce the time cost for test cases.
	splitRetryTimes = 2
}

type testSplitClient struct {
	mu           sync.RWMutex
	stores       map[uint64]*metapb.Store
	regions      map[uint64]*split.RegionInfo
	regionsInfo  *pdtypes.RegionTree // For now it's only used in ScanRegions
	nextRegionID uint64
	splitCount   atomic.Int32
	hook         clientHook
}

func newTestSplitClient(
	stores map[uint64]*metapb.Store,
	regions map[uint64]*split.RegionInfo,
	nextRegionID uint64,
	hook clientHook,
) *testSplitClient {
	regionsInfo := &pdtypes.RegionTree{}
	for _, regionInfo := range regions {
		regionsInfo.SetRegion(pdtypes.NewRegionInfo(regionInfo.Region, regionInfo.Leader))
	}
	return &testSplitClient{
		stores:       stores,
		regions:      regions,
		regionsInfo:  regionsInfo,
		nextRegionID: nextRegionID,
		hook:         hook,
	}
}

// ScatterRegions scatters regions in a batch.
func (c *testSplitClient) ScatterRegions(ctx context.Context, regionInfo []*split.RegionInfo) error {
	return nil
}

func (c *testSplitClient) GetAllRegions() map[uint64]*split.RegionInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.regions
}

func (c *testSplitClient) GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	store, ok := c.stores[storeID]
	if !ok {
		return nil, errors.Errorf("store not found")
	}
	return store, nil
}

func (c *testSplitClient) GetRegion(ctx context.Context, key []byte) (*split.RegionInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, region := range c.regions {
		if bytes.Compare(key, region.Region.StartKey) >= 0 && beforeEnd(key, region.Region.EndKey) {
			return region, nil
		}
	}
	return nil, errors.Errorf("region not found: key=%s", string(key))
}

func (c *testSplitClient) GetRegionByID(ctx context.Context, regionID uint64) (*split.RegionInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	region, ok := c.regions[regionID]
	if !ok {
		return nil, errors.Errorf("region not found: id=%d", regionID)
	}
	return region, nil
}

func (c *testSplitClient) SplitRegion(
	ctx context.Context,
	regionInfo *split.RegionInfo,
	key []byte,
) (*split.RegionInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var target *split.RegionInfo
	splitKey := codec.EncodeBytes([]byte{}, key)
	for _, region := range c.regions {
		if bytes.Compare(splitKey, region.Region.StartKey) >= 0 &&
			beforeEnd(splitKey, region.Region.EndKey) {
			target = region
		}
	}
	if target == nil {
		return nil, errors.Errorf("region not found: key=%s", string(key))
	}
	newRegion := &split.RegionInfo{
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
	c.regionsInfo.SetRegion(pdtypes.NewRegionInfo(newRegion.Region, newRegion.Leader))
	c.nextRegionID++
	target.Region.StartKey = splitKey
	target.Region.RegionEpoch.ConfVer++
	c.regions[target.Region.Id] = target
	c.regionsInfo.SetRegion(pdtypes.NewRegionInfo(target.Region, target.Leader))
	return newRegion, nil
}

func (c *testSplitClient) BatchSplitRegionsWithOrigin(
	ctx context.Context, regionInfo *split.RegionInfo, keys [][]byte,
) (*split.RegionInfo, []*split.RegionInfo, error) {
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

	newRegions := make([]*split.RegionInfo, 0)
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
		newRegion := &split.RegionInfo{
			Region: &metapb.Region{
				Peers:    target.Region.Peers,
				Id:       c.nextRegionID,
				StartKey: startKey,
				EndKey:   key,
			},
		}
		c.regions[c.nextRegionID] = newRegion
		c.regionsInfo.SetRegion(pdtypes.NewRegionInfo(newRegion.Region, newRegion.Leader))
		c.nextRegionID++
		startKey = key
		newRegions = append(newRegions, newRegion)
	}
	if !bytes.Equal(target.Region.StartKey, startKey) {
		target.Region.StartKey = startKey
		c.regions[target.Region.Id] = target
		c.regionsInfo.SetRegion(pdtypes.NewRegionInfo(target.Region, target.Leader))
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

func (c *testSplitClient) BatchSplitRegions(
	ctx context.Context, regionInfo *split.RegionInfo, keys [][]byte,
) ([]*split.RegionInfo, error) {
	_, newRegions, err := c.BatchSplitRegionsWithOrigin(ctx, regionInfo, keys)
	return newRegions, err
}

func (c *testSplitClient) ScatterRegion(ctx context.Context, regionInfo *split.RegionInfo) error {
	return nil
}

func (c *testSplitClient) GetOperator(ctx context.Context, regionID uint64) (*pdpb.GetOperatorResponse, error) {
	return &pdpb.GetOperatorResponse{
		Header: new(pdpb.ResponseHeader),
	}, nil
}

func (c *testSplitClient) ScanRegions(ctx context.Context, key, endKey []byte, limit int) ([]*split.RegionInfo, error) {
	if c.hook != nil {
		key, endKey, limit = c.hook.BeforeScanRegions(ctx, key, endKey, limit)
	}

	infos := c.regionsInfo.ScanRange(key, endKey, limit)
	regions := make([]*split.RegionInfo, 0, len(infos))
	for _, info := range infos {
		regions = append(regions, &split.RegionInfo{
			Region: info.Meta,
			Leader: info.Leader,
		})
	}

	var err error
	if c.hook != nil {
		regions, err = c.hook.AfterScanRegions(regions, nil)
	}
	return regions, err
}

func (c *testSplitClient) GetPlacementRule(ctx context.Context, groupID, ruleID string) (r pdtypes.Rule, err error) {
	return
}

func (c *testSplitClient) SetPlacementRule(ctx context.Context, rule pdtypes.Rule) error {
	return nil
}

func (c *testSplitClient) DeletePlacementRule(ctx context.Context, groupID, ruleID string) error {
	return nil
}

func (c *testSplitClient) SetStoresLabel(ctx context.Context, stores []uint64, labelKey, labelValue string) error {
	return nil
}

func cloneRegion(region *split.RegionInfo) *split.RegionInfo {
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
	return &split.RegionInfo{Region: r, Leader: l}
}

// For keys ["", "aay", "bba", "bbh", "cca", ""], the key ranges of
// regions are [, aay), [aay, bba), [bba, bbh), [bbh, cca), [cca, ).
func initTestSplitClient(keys [][]byte, hook clientHook) *testSplitClient {
	peers := make([]*metapb.Peer, 1)
	peers[0] = &metapb.Peer{
		Id:      1,
		StoreId: 1,
	}
	regions := make(map[uint64]*split.RegionInfo)
	for i := uint64(1); i < uint64(len(keys)); i++ {
		startKey := keys[i-1]
		if len(startKey) != 0 {
			startKey = codec.EncodeBytes([]byte{}, startKey)
		}
		endKey := keys[i]
		if len(endKey) != 0 {
			endKey = codec.EncodeBytes([]byte{}, endKey)
		}
		regions[i] = &split.RegionInfo{
			Region: &metapb.Region{
				Id:          i,
				Peers:       peers,
				StartKey:    startKey,
				EndKey:      endKey,
				RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
			},
			Leader: peers[0],
		}
	}
	stores := make(map[uint64]*metapb.Store)
	stores[1] = &metapb.Store{
		Id: 1,
	}
	return newTestSplitClient(stores, regions, uint64(len(keys)), hook)
}

// initTestSplitClient3Replica will create a client that each region has 3 replicas, and their IDs and StoreIDs are
// (1, 2, 3), (11, 12, 13), ...
// For keys ["", "aay", "bba", "bbh", "cca", ""], the key ranges of
// region ranges are [, aay), [aay, bba), [bba, bbh), [bbh, cca), [cca, ).
func initTestSplitClient3Replica(keys [][]byte, hook clientHook) *testSplitClient {
	regions := make(map[uint64]*split.RegionInfo)
	stores := make(map[uint64]*metapb.Store)
	for i := uint64(1); i < uint64(len(keys)); i++ {
		startKey := keys[i-1]
		if len(startKey) != 0 {
			startKey = codec.EncodeBytes([]byte{}, startKey)
		}
		endKey := keys[i]
		if len(endKey) != 0 {
			endKey = codec.EncodeBytes([]byte{}, endKey)
		}
		baseID := (i-1)*10 + 1
		peers := make([]*metapb.Peer, 3)
		for j := 0; j < 3; j++ {
			peers[j] = &metapb.Peer{
				Id:      baseID + uint64(j),
				StoreId: baseID + uint64(j),
			}
		}

		regions[baseID] = &split.RegionInfo{
			Region: &metapb.Region{
				Id:          baseID,
				Peers:       peers,
				StartKey:    startKey,
				EndKey:      endKey,
				RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
			},
			Leader: peers[0],
		}
		stores[baseID] = &metapb.Store{
			Id: baseID,
		}
	}
	return newTestSplitClient(stores, regions, uint64(len(keys)), hook)
}

func checkRegionRanges(t *testing.T, regions []*split.RegionInfo, keys [][]byte) {
	for i, r := range regions {
		_, regionStart, _ := codec.DecodeBytes(r.Region.StartKey, []byte{})
		_, regionEnd, _ := codec.DecodeBytes(r.Region.EndKey, []byte{})
		require.Equal(t, keys[i], regionStart)
		require.Equal(t, keys[i+1], regionEnd)
	}
}

type clientHook interface {
	BeforeSplitRegion(ctx context.Context, regionInfo *split.RegionInfo, keys [][]byte) (*split.RegionInfo, [][]byte)
	AfterSplitRegion(context.Context, *split.RegionInfo, [][]byte, []*split.RegionInfo, error) ([]*split.RegionInfo, error)
	BeforeScanRegions(ctx context.Context, key, endKey []byte, limit int) ([]byte, []byte, int)
	AfterScanRegions([]*split.RegionInfo, error) ([]*split.RegionInfo, error)
}

type noopHook struct{}

func (h *noopHook) BeforeSplitRegion(ctx context.Context, regionInfo *split.RegionInfo, keys [][]byte) (*split.RegionInfo, [][]byte) {
	delayTime := rand.Int31n(10) + 1
	time.Sleep(time.Duration(delayTime) * time.Millisecond)
	return regionInfo, keys
}

func (h *noopHook) AfterSplitRegion(c context.Context, r *split.RegionInfo, keys [][]byte, res []*split.RegionInfo, err error) ([]*split.RegionInfo, error) {
	return res, err
}

func (h *noopHook) BeforeScanRegions(ctx context.Context, key, endKey []byte, limit int) ([]byte, []byte, int) {
	return key, endKey, limit
}

func (h *noopHook) AfterScanRegions(res []*split.RegionInfo, err error) ([]*split.RegionInfo, error) {
	return res, err
}

type batchSplitHook interface {
	setup(t *testing.T) func()
	check(t *testing.T, cli *testSplitClient)
}

type defaultHook struct{}

func (d defaultHook) setup(t *testing.T) func() {
	oldSplitBackoffTime := splitRegionBaseBackOffTime
	splitRegionBaseBackOffTime = time.Millisecond
	return func() {
		splitRegionBaseBackOffTime = oldSplitBackoffTime
	}
}

func (d defaultHook) check(t *testing.T, cli *testSplitClient) {
	// so with a batch split size of 4, there will be 7 time batch split
	// 1. region: [aay, bba), keys: [b, ba, bb]
	// 2. region: [bbh, cca), keys: [bc, bd, be, bf]
	// 3. region: [bf, cca), keys: [bg, bh, bi, bj]
	// 4. region: [bj, cca), keys: [bk, bl, bm, bn]
	// 5. region: [bn, cca), keys: [bo, bp, bq, br]
	// 6. region: [br, cca), keys: [bs, bt, bu, bv]
	// 7. region: [bv, cca), keys: [bw, bx, by, bz]

	// since it may encounter error retries, here only check the lower threshold.
	require.GreaterOrEqual(t, cli.splitCount.Load(), int32(7))
}

func doTestBatchSplitRegionByRanges(ctx context.Context, t *testing.T, hook clientHook, errPat string, splitHook batchSplitHook) {
	if splitHook == nil {
		splitHook = defaultHook{}
	}
	deferFunc := splitHook.setup(t)
	defer deferFunc()

	keys := [][]byte{[]byte(""), []byte("aay"), []byte("bba"), []byte("bbh"), []byte("cca"), []byte("")}
	client := initTestSplitClient(keys, hook)
	local := &Backend{
		splitCli:         client,
		regionSizeGetter: &TableRegionSizeGetterImpl{},
		logger:           log.L(),
	}
	local.RegionSplitBatchSize = 4
	local.RegionSplitConcurrency = 4

	// current region ranges: [, aay), [aay, bba), [bba, bbh), [bbh, cca), [cca, )
	rangeStart := codec.EncodeBytes([]byte{}, []byte("b"))
	rangeEnd := codec.EncodeBytes([]byte{}, []byte("c"))
	regions, err := split.PaginateScanRegion(ctx, client, rangeStart, rangeEnd, 5)
	require.NoError(t, err)
	// regions is: [aay, bba), [bba, bbh), [bbh, cca)
	checkRegionRanges(t, regions, [][]byte{[]byte("aay"), []byte("bba"), []byte("bbh"), []byte("cca")})

	// generate:  ranges [b, ba), [ba, bb), [bb, bc), ... [by, bz)
	ranges := make([]Range, 0)
	start := []byte{'b'}
	for i := byte('a'); i <= 'z'; i++ {
		end := []byte{'b', i}
		ranges = append(ranges, Range{start: start, end: end})
		start = end
	}

	err = local.SplitAndScatterRegionByRanges(ctx, ranges, true)
	if len(errPat) != 0 {
		require.Error(t, err)
		require.Regexp(t, errPat, err.Error())
		return
	}
	require.NoError(t, err)
	splitHook.check(t, client)

	// check split ranges
	regions, err = split.PaginateScanRegion(ctx, client, rangeStart, rangeEnd, 5)
	require.NoError(t, err)
	result := [][]byte{
		[]byte("b"), []byte("ba"), []byte("bb"), []byte("bba"), []byte("bbh"), []byte("bc"),
		[]byte("bd"), []byte("be"), []byte("bf"), []byte("bg"), []byte("bh"), []byte("bi"), []byte("bj"),
		[]byte("bk"), []byte("bl"), []byte("bm"), []byte("bn"), []byte("bo"), []byte("bp"), []byte("bq"),
		[]byte("br"), []byte("bs"), []byte("bt"), []byte("bu"), []byte("bv"), []byte("bw"), []byte("bx"),
		[]byte("by"), []byte("bz"), []byte("cca"),
	}
	checkRegionRanges(t, regions, result)
}

func TestBatchSplitRegionByRanges(t *testing.T) {
	doTestBatchSplitRegionByRanges(context.Background(), t, nil, "", nil)
}

type checkScatterClient struct {
	*testSplitClient

	mu                sync.Mutex
	notFoundFirstTime map[uint64]struct{}
	scatterCounter    atomic.Int32
}

func newCheckScatterClient(inner *testSplitClient) *checkScatterClient {
	return &checkScatterClient{
		testSplitClient:   inner,
		notFoundFirstTime: map[uint64]struct{}{},
		scatterCounter:    atomic.Int32{},
	}
}

func (c *checkScatterClient) ScatterRegion(ctx context.Context, regionInfo *split.RegionInfo) error {
	c.scatterCounter.Add(1)
	return nil
}

func (c *checkScatterClient) GetRegionByID(ctx context.Context, regionID uint64) (*split.RegionInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.notFoundFirstTime[regionID]; !ok {
		c.notFoundFirstTime[regionID] = struct{}{}
		return nil, nil
	}
	return c.testSplitClient.GetRegionByID(ctx, regionID)
}

func TestMissingScatter(t *testing.T) {
	ctx := context.Background()
	splitHook := defaultHook{}
	deferFunc := splitHook.setup(t)
	defer deferFunc()

	keys := [][]byte{[]byte(""), []byte("aay"), []byte("bba"), []byte("bbh"), []byte("cca"), []byte("")}
	client := initTestSplitClient(keys, nil)
	checkClient := newCheckScatterClient(client)
	local := &Backend{
		splitCli: checkClient,
		logger:   log.L(),
	}
	local.RegionSplitBatchSize = 4
	local.RegionSplitConcurrency = 4

	// current region ranges: [, aay), [aay, bba), [bba, bbh), [bbh, cca), [cca, )
	rangeStart := codec.EncodeBytes([]byte{}, []byte("b"))
	rangeEnd := codec.EncodeBytes([]byte{}, []byte("c"))
	regions, err := split.PaginateScanRegion(ctx, client, rangeStart, rangeEnd, 5)
	require.NoError(t, err)
	// regions is: [aay, bba), [bba, bbh), [bbh, cca)
	checkRegionRanges(t, regions, [][]byte{[]byte("aay"), []byte("bba"), []byte("bbh"), []byte("cca")})

	// generate:  ranges [b, ba), [ba, bb), [bb, bc), ... [by, bz)
	ranges := make([]Range, 0)
	start := []byte{'b'}
	for i := byte('a'); i <= 'z'; i++ {
		end := []byte{'b', i}
		ranges = append(ranges, Range{start: start, end: end})
		start = end
	}

	err = local.SplitAndScatterRegionByRanges(ctx, ranges, true)
	require.NoError(t, err)

	splitHook.check(t, client)

	// check split ranges
	regions, err = split.PaginateScanRegion(ctx, client, rangeStart, rangeEnd, 5)
	require.NoError(t, err)
	result := [][]byte{
		[]byte("b"), []byte("ba"), []byte("bb"), []byte("bba"), []byte("bbh"), []byte("bc"),
		[]byte("bd"), []byte("be"), []byte("bf"), []byte("bg"), []byte("bh"), []byte("bi"), []byte("bj"),
		[]byte("bk"), []byte("bl"), []byte("bm"), []byte("bn"), []byte("bo"), []byte("bp"), []byte("bq"),
		[]byte("br"), []byte("bs"), []byte("bt"), []byte("bu"), []byte("bv"), []byte("bw"), []byte("bx"),
		[]byte("by"), []byte("bz"), []byte("cca"),
	}
	checkRegionRanges(t, regions, result)

	// the old regions will not be scattered. They are [..., bba), [bba, bbh), [..., cca)
	require.Equal(t, len(result)-3, int(checkClient.scatterCounter.Load()))
}

type batchSizeHook struct{}

func (h batchSizeHook) setup(t *testing.T) func() {
	oldSizeLimit := maxBatchSplitSize
	oldSplitBackoffTime := splitRegionBaseBackOffTime
	maxBatchSplitSize = 6
	splitRegionBaseBackOffTime = time.Millisecond
	return func() {
		maxBatchSplitSize = oldSizeLimit
		splitRegionBaseBackOffTime = oldSplitBackoffTime
	}
}

func (h batchSizeHook) check(t *testing.T, cli *testSplitClient) {
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
	require.Equal(t, int32(9), cli.splitCount.Load())
}

func TestBatchSplitRegionByRangesKeySizeLimit(t *testing.T) {
	doTestBatchSplitRegionByRanges(context.Background(), t, nil, "", batchSizeHook{})
}

type scanRegionEmptyHook struct {
	noopHook
	cnt int
}

func (h *scanRegionEmptyHook) AfterScanRegions(res []*split.RegionInfo, err error) ([]*split.RegionInfo, error) {
	h.cnt++
	// skip the first call
	if h.cnt == 1 {
		return res, err
	}
	return nil, err
}

func TestBatchSplitRegionByRangesScanFailed(t *testing.T) {
	backup := split.WaitRegionOnlineAttemptTimes
	split.WaitRegionOnlineAttemptTimes = 3
	defer func() {
		split.WaitRegionOnlineAttemptTimes = backup
	}()
	doTestBatchSplitRegionByRanges(context.Background(), t, &scanRegionEmptyHook{}, "scan region return empty result", defaultHook{})
}

type splitRegionEpochNotMatchHook struct {
	noopHook
}

func (h *splitRegionEpochNotMatchHook) BeforeSplitRegion(ctx context.Context, regionInfo *split.RegionInfo, keys [][]byte) (*split.RegionInfo, [][]byte) {
	regionInfo, keys = h.noopHook.BeforeSplitRegion(ctx, regionInfo, keys)
	regionInfo = cloneRegion(regionInfo)
	// decrease the region epoch, so split region will fail
	regionInfo.Region.RegionEpoch.Version--
	return regionInfo, keys
}

func TestBatchSplitByRangesEpochNotMatch(t *testing.T) {
	doTestBatchSplitRegionByRanges(context.Background(), t, &splitRegionEpochNotMatchHook{}, "batch split regions failed: epoch not match", defaultHook{})
}

// return epoch not match error in every other call
type splitRegionEpochNotMatchHookRandom struct {
	noopHook
	cnt atomic.Int32
}

func (h *splitRegionEpochNotMatchHookRandom) BeforeSplitRegion(ctx context.Context, regionInfo *split.RegionInfo, keys [][]byte) (*split.RegionInfo, [][]byte) {
	regionInfo, keys = h.noopHook.BeforeSplitRegion(ctx, regionInfo, keys)
	if h.cnt.Inc() != 0 {
		return regionInfo, keys
	}
	regionInfo = cloneRegion(regionInfo)
	// decrease the region epoch, so split region will fail
	regionInfo.Region.RegionEpoch.Version--
	return regionInfo, keys
}

func TestBatchSplitByRangesEpochNotMatchOnce(t *testing.T) {
	doTestBatchSplitRegionByRanges(context.Background(), t, &splitRegionEpochNotMatchHookRandom{}, "", defaultHook{})
}

type splitRegionNoValidKeyHook struct {
	noopHook
	returnErrTimes int32
	errorCnt       atomic.Int32
}

func (h *splitRegionNoValidKeyHook) BeforeSplitRegion(ctx context.Context, regionInfo *split.RegionInfo, keys [][]byte) (*split.RegionInfo, [][]byte) {
	regionInfo, keys = h.noopHook.BeforeSplitRegion(ctx, regionInfo, keys)
	if h.errorCnt.Inc() <= h.returnErrTimes {
		// clean keys to trigger "no valid keys" error
		keys = keys[:0]
	}
	return regionInfo, keys
}

func TestBatchSplitByRangesNoValidKeysOnce(t *testing.T) {
	doTestBatchSplitRegionByRanges(context.Background(), t, &splitRegionNoValidKeyHook{returnErrTimes: 1}, "", defaultHook{})
}

func TestBatchSplitByRangesNoValidKeys(t *testing.T) {
	doTestBatchSplitRegionByRanges(context.Background(), t, &splitRegionNoValidKeyHook{returnErrTimes: math.MaxInt32}, "no valid key", defaultHook{})
}

func TestSplitAndScatterRegionInBatches(t *testing.T) {
	splitHook := defaultHook{}
	deferFunc := splitHook.setup(t)
	defer deferFunc()

	keys := [][]byte{[]byte(""), []byte("a"), []byte("b"), []byte("")}
	client := initTestSplitClient(keys, nil)
	local := &Backend{
		splitCli:         client,
		regionSizeGetter: &TableRegionSizeGetterImpl{},
		logger:           log.L(),
	}
	local.RegionSplitBatchSize = 4
	local.RegionSplitConcurrency = 4

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var ranges []Range
	for i := 0; i < 20; i++ {
		ranges = append(ranges, Range{
			start: []byte(fmt.Sprintf("a%02d", i)),
			end:   []byte(fmt.Sprintf("a%02d", i+1)),
		})
	}

	err := local.SplitAndScatterRegionInBatches(ctx, ranges, true, 4)
	require.NoError(t, err)

	rangeStart := codec.EncodeBytes([]byte{}, []byte("a"))
	rangeEnd := codec.EncodeBytes([]byte{}, []byte("b"))
	regions, err := split.PaginateScanRegion(ctx, client, rangeStart, rangeEnd, 5)
	require.NoError(t, err)
	result := [][]byte{[]byte("a"), []byte("a00"), []byte("a01"), []byte("a02"), []byte("a03"), []byte("a04"),
		[]byte("a05"), []byte("a06"), []byte("a07"), []byte("a08"), []byte("a09"), []byte("a10"), []byte("a11"),
		[]byte("a12"), []byte("a13"), []byte("a14"), []byte("a15"), []byte("a16"), []byte("a17"), []byte("a18"),
		[]byte("a19"), []byte("a20"), []byte("b"),
	}
	checkRegionRanges(t, regions, result)
}

type reportAfterSplitHook struct {
	noopHook
	ch chan<- struct{}
}

func (h *reportAfterSplitHook) AfterSplitRegion(ctx context.Context, region *split.RegionInfo, keys [][]byte, resultRegions []*split.RegionInfo, err error) ([]*split.RegionInfo, error) {
	h.ch <- struct{}{}
	return resultRegions, err
}

func TestBatchSplitByRangeCtxCanceled(t *testing.T) {
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

	doTestBatchSplitRegionByRanges(ctx, t, &reportAfterSplitHook{ch: ch}, "context canceled", defaultHook{})
	close(ch)
}

func doTestBatchSplitByRangesWithClusteredIndex(t *testing.T, hook clientHook) {
	oldSplitBackoffTime := splitRegionBaseBackOffTime
	splitRegionBaseBackOffTime = time.Millisecond
	defer func() {
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
		require.NoError(t, err)
		h, err := kv.NewCommonHandle(keyBytes)
		require.NoError(t, err)
		key := tablecodec.EncodeRowKeyWithHandle(tableID, h)
		keys = append(keys, key)
	}
	keys = append(keys, tableEndKey, []byte(""))
	client := initTestSplitClient(keys, hook)
	local := &Backend{
		splitCli:         client,
		regionSizeGetter: &TableRegionSizeGetterImpl{},
		logger:           log.L(),
	}
	local.RegionSplitBatchSize = 10
	local.RegionSplitConcurrency = 10
	ctx := context.Background()

	// we batch generate a batch of row keys for table 1 with common handle
	rangeKeys := make([][]byte, 0, 20+1)
	for i := int64(0); i < 2; i++ {
		for j := int64(0); j < 10; j++ {
			keyBytes, err := codec.EncodeKey(stmtCtx, nil, types.NewIntDatum(i), types.NewIntDatum(j*10000))
			require.NoError(t, err)
			h, err := kv.NewCommonHandle(keyBytes)
			require.NoError(t, err)
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

	err := local.SplitAndScatterRegionByRanges(ctx, ranges, true)
	require.NoError(t, err)

	startKey := codec.EncodeBytes([]byte{}, rangeKeys[0])
	endKey := codec.EncodeBytes([]byte{}, rangeKeys[len(rangeKeys)-1])
	// check split ranges
	regions, err := split.PaginateScanRegion(ctx, client, startKey, endKey, 5)
	require.NoError(t, err)
	require.Equal(t, len(ranges)+1, len(regions))

	checkKeys := append([][]byte{}, rangeKeys[:10]...)
	checkKeys = append(checkKeys, keys[3])
	checkKeys = append(checkKeys, rangeKeys[10:]...)
	checkRegionRanges(t, regions, checkKeys)
}

func TestBatchSplitByRangesWithClusteredIndex(t *testing.T) {
	doTestBatchSplitByRangesWithClusteredIndex(t, nil)
}

func TestBatchSplitByRangesWithClusteredIndexEpochNotMatch(t *testing.T) {
	doTestBatchSplitByRangesWithClusteredIndex(t, &splitRegionEpochNotMatchHookRandom{})
}

func TestNeedSplit(t *testing.T) {
	tableID := int64(1)
	peers := make([]*metapb.Peer, 1)
	peers[0] = &metapb.Peer{
		Id:      1,
		StoreId: 1,
	}
	keys := []int64{10, 100, 500, 1000, 999999, -1}
	start := tablecodec.EncodeRowKeyWithHandle(tableID, kv.IntHandle(0))
	regionStart := codec.EncodeBytes([]byte{}, start)
	regions := make([]*split.RegionInfo, 0)
	for _, end := range keys {
		var regionEndKey []byte
		if end >= 0 {
			endKey := tablecodec.EncodeRowKeyWithHandle(tableID, kv.IntHandle(end))
			regionEndKey = codec.EncodeBytes([]byte{}, endKey)
		}
		region := &split.RegionInfo{
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
		res := needSplit(checkKey, regions, log.L())
		if idx < 0 {
			require.Nil(t, res)
		} else {
			require.Equal(t, regions[idx], res)
		}
	}
}

func TestStoreWriteLimiter(t *testing.T) {
	// Test create store write limiter with limit math.MaxInt.
	limiter := newStoreWriteLimiter(math.MaxInt)
	err := limiter.WaitN(context.Background(), 1, 1024)
	require.NoError(t, err)

	// Test WaitN exceeds the burst.
	limiter = newStoreWriteLimiter(100)
	start := time.Now()
	// 120 is the initial burst, 150 is the number of new tokens.
	err = limiter.WaitN(context.Background(), 1, 120+120)
	require.NoError(t, err)
	require.Greater(t, time.Since(start), time.Second)

	// Test WaitN with different store id.
	limiter = newStoreWriteLimiter(100)
	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(storeID uint64) {
			defer wg.Done()
			start := time.Now()
			var gotTokens int
			for {
				n := rand.Intn(50)
				if limiter.WaitN(ctx, storeID, n) != nil {
					break
				}
				gotTokens += n
			}
			elapsed := time.Since(start)
			maxTokens := 120 + int(float64(elapsed)/float64(time.Second)*100)
			// In theory, gotTokens should be less than or equal to maxTokens.
			// But we allow a little of error to avoid the test being flaky.
			require.LessOrEqual(t, gotTokens, maxTokens+1)
		}(uint64(i))
	}
	wg.Wait()
}

type scatterRegionCli struct {
	split.SplitClient

	respM    map[uint64][]*pdpb.GetOperatorResponse
	scatterM map[uint64]int
}

func newScatterRegionCli() *scatterRegionCli {
	return &scatterRegionCli{
		respM:    make(map[uint64][]*pdpb.GetOperatorResponse),
		scatterM: make(map[uint64]int),
	}
}

func (c *scatterRegionCli) ScatterRegion(_ context.Context, regionInfo *split.RegionInfo) error {
	c.scatterM[regionInfo.Region.Id]++
	return nil
}

func (c *scatterRegionCli) GetOperator(_ context.Context, regionID uint64) (*pdpb.GetOperatorResponse, error) {
	ret := c.respM[regionID][0]
	c.respM[regionID] = c.respM[regionID][1:]
	return ret, nil
}

func TestWaitForScatterRegions(t *testing.T) {
	ctx := context.Background()

	regionCnt := 6
	regions := make([]*split.RegionInfo, 0, regionCnt)
	for i := 1; i <= regionCnt; i++ {
		regions = append(regions, &split.RegionInfo{
			Region: &metapb.Region{
				Id: uint64(i),
			},
		})
	}
	checkRespDrained := func(cli *scatterRegionCli) {
		for i := 1; i <= regionCnt; i++ {
			require.Len(t, cli.respM[uint64(i)], 0)
		}
	}
	checkNoRetry := func(cli *scatterRegionCli) {
		for i := 1; i <= regionCnt; i++ {
			require.Equal(t, 0, cli.scatterM[uint64(i)])
		}
	}

	cli := newScatterRegionCli()
	cli.respM[1] = []*pdpb.GetOperatorResponse{
		{Header: &pdpb.ResponseHeader{Error: &pdpb.Error{Type: pdpb.ErrorType_REGION_NOT_FOUND}}},
	}
	cli.respM[2] = []*pdpb.GetOperatorResponse{
		{Desc: []byte("not-scatter-region")},
	}
	cli.respM[3] = []*pdpb.GetOperatorResponse{
		{Desc: []byte("scatter-region"), Status: pdpb.OperatorStatus_SUCCESS},
	}
	cli.respM[4] = []*pdpb.GetOperatorResponse{
		{Desc: []byte("scatter-region"), Status: pdpb.OperatorStatus_RUNNING},
		{Desc: []byte("scatter-region"), Status: pdpb.OperatorStatus_RUNNING},
		{Desc: []byte("scatter-region"), Status: pdpb.OperatorStatus_SUCCESS},
	}
	cli.respM[5] = []*pdpb.GetOperatorResponse{
		{Desc: []byte("scatter-region"), Status: pdpb.OperatorStatus_CANCEL},
		{Desc: []byte("scatter-region"), Status: pdpb.OperatorStatus_CANCEL},
		{Desc: []byte("scatter-region"), Status: pdpb.OperatorStatus_CANCEL},
		{Desc: []byte("scatter-region"), Status: pdpb.OperatorStatus_RUNNING},
		{Desc: []byte("scatter-region"), Status: pdpb.OperatorStatus_RUNNING},
		{Desc: []byte("not-scatter-region")},
	}
	// should trigger a retry
	cli.respM[6] = []*pdpb.GetOperatorResponse{
		{Desc: []byte("scatter-region"), Status: pdpb.OperatorStatus_REPLACE},
		{Desc: []byte("scatter-region"), Status: pdpb.OperatorStatus_SUCCESS},
	}

	local := &Backend{splitCli: cli}
	cnt, err := local.waitForScatterRegions(ctx, regions)
	require.NoError(t, err)
	require.Equal(t, 6, cnt)
	for i := 1; i <= 4; i++ {
		require.Equal(t, 0, cli.scatterM[uint64(i)])
	}
	require.Equal(t, 3, cli.scatterM[uint64(5)])
	require.Equal(t, 1, cli.scatterM[uint64(6)])
	checkRespDrained(cli)

	// test non-retryable error

	cli = newScatterRegionCli()
	cli.respM[1] = []*pdpb.GetOperatorResponse{
		{Header: &pdpb.ResponseHeader{Error: &pdpb.Error{Type: pdpb.ErrorType_REGION_NOT_FOUND}}},
	}
	cli.respM[2] = []*pdpb.GetOperatorResponse{
		{Desc: []byte("not-scatter-region")},
	}
	// mimic non-retryable error
	cli.respM[3] = []*pdpb.GetOperatorResponse{
		{Header: &pdpb.ResponseHeader{Error: &pdpb.Error{Type: pdpb.ErrorType_DATA_COMPACTED}}},
	}
	local = &Backend{splitCli: cli}
	cnt, err = local.waitForScatterRegions(ctx, regions)
	require.ErrorContains(t, err, "failed to get region operator, error type: DATA_COMPACTED")
	require.Equal(t, 2, cnt)
	checkRespDrained(cli)
	checkNoRetry(cli)

	// test backoff is timed-out

	backup := split.WaitRegionOnlineAttemptTimes
	split.WaitRegionOnlineAttemptTimes = 2
	t.Cleanup(func() {
		split.WaitRegionOnlineAttemptTimes = backup
	})

	cli = newScatterRegionCli()
	cli.respM[1] = []*pdpb.GetOperatorResponse{
		{Header: &pdpb.ResponseHeader{Error: &pdpb.Error{Type: pdpb.ErrorType_REGION_NOT_FOUND}}},
	}
	cli.respM[2] = []*pdpb.GetOperatorResponse{
		{Desc: []byte("not-scatter-region")},
	}
	cli.respM[3] = []*pdpb.GetOperatorResponse{
		{Desc: []byte("scatter-region"), Status: pdpb.OperatorStatus_SUCCESS},
	}
	cli.respM[4] = []*pdpb.GetOperatorResponse{
		{Desc: []byte("scatter-region"), Status: pdpb.OperatorStatus_RUNNING},
		{Desc: []byte("scatter-region"), Status: pdpb.OperatorStatus_RUNNING}, // first retry
		{Desc: []byte("scatter-region"), Status: pdpb.OperatorStatus_RUNNING}, // second retry
	}
	cli.respM[5] = []*pdpb.GetOperatorResponse{
		{Desc: []byte("not-scatter-region")},
	}
	cli.respM[6] = []*pdpb.GetOperatorResponse{
		{Desc: []byte("scatter-region"), Status: pdpb.OperatorStatus_SUCCESS},
	}
	local = &Backend{splitCli: cli}
	cnt, err = local.waitForScatterRegions(ctx, regions)
	require.ErrorContains(t, err, "wait for scatter region timeout, print the first unfinished region id:4")
	require.Equal(t, 5, cnt)
	checkRespDrained(cli)
	checkNoRetry(cli)
}
