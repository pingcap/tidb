// Copyright 2024 PingCAP, Inc. Licensed under Apache-2.0.

package split

import (
	"bytes"
	"context"
	"math"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/tidb/br/pkg/pdutil"
	"github.com/pingcap/tidb/pkg/store/pdtypes"
	"github.com/pingcap/tidb/pkg/util/codec"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/clients/router"
	pdhttp "github.com/tikv/pd/client/http"
	"github.com/tikv/pd/client/opt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

// TODO consilodate TestClient and MockPDClientForSplit and FakePDClient
// into one test client.
type TestClient struct {
	SplitClient
	pd.Client

	mu           sync.RWMutex
	stores       map[uint64]*metapb.Store
	Regions      map[uint64]*RegionInfo
	RegionsInfo  *pdtypes.RegionTree // For now it's only used in ScanRegions
	nextRegionID uint64

	scattered   map[uint64]bool
	InjectErr   bool
	InjectTimes int32
}

func NewTestClient(
	stores map[uint64]*metapb.Store,
	regions map[uint64]*RegionInfo,
	nextRegionID uint64,
) *TestClient {
	regionsInfo := &pdtypes.RegionTree{}
	for _, regionInfo := range regions {
		regionsInfo.SetRegion(pdtypes.NewRegionInfo(regionInfo.Region, regionInfo.Leader))
	}
	return &TestClient{
		stores:       stores,
		Regions:      regions,
		RegionsInfo:  regionsInfo,
		nextRegionID: nextRegionID,
		scattered:    map[uint64]bool{},
	}
}

func (c *TestClient) GetAllRegions() map[uint64]*RegionInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Regions
}

func (c *TestClient) GetPDClient() *FakePDClient {
	stores := make([]*metapb.Store, 0, len(c.stores))
	for _, store := range c.stores {
		stores = append(stores, store)
	}
	return NewFakePDClient(stores, false, nil)
}

func (c *TestClient) GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	store, ok := c.stores[storeID]
	if !ok {
		return nil, errors.Errorf("store not found")
	}
	return store, nil
}

func (c *TestClient) GetRegion(ctx context.Context, key []byte) (*RegionInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, region := range c.Regions {
		if bytes.Compare(key, region.Region.StartKey) >= 0 &&
			(len(region.Region.EndKey) == 0 || bytes.Compare(key, region.Region.EndKey) < 0) {
			return region, nil
		}
	}
	return nil, errors.Errorf("region not found: key=%s", string(key))
}

func (c *TestClient) GetRegionByID(ctx context.Context, regionID uint64) (*RegionInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	region, ok := c.Regions[regionID]
	if !ok {
		return nil, errors.Errorf("region not found: id=%d", regionID)
	}
	return region, nil
}

func (c *TestClient) SplitWaitAndScatter(_ context.Context, _ *RegionInfo, keys [][]byte) ([]*RegionInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	newRegions := make([]*RegionInfo, 0)
	for _, key := range keys {
		var target *RegionInfo
		splitKey := codec.EncodeBytes([]byte{}, key)
		for _, region := range c.Regions {
			if region.ContainsInterior(splitKey) {
				target = region
			}
		}
		if target == nil {
			continue
		}
		newRegion := &RegionInfo{
			Region: &metapb.Region{
				Peers:    target.Region.Peers,
				Id:       c.nextRegionID,
				StartKey: target.Region.StartKey,
				EndKey:   splitKey,
			},
		}
		c.Regions[c.nextRegionID] = newRegion
		c.nextRegionID++
		target.Region.StartKey = splitKey
		c.Regions[target.Region.Id] = target
		newRegions = append(newRegions, newRegion)
	}
	return newRegions, nil
}

func (c *TestClient) GetOperator(context.Context, uint64) (*pdpb.GetOperatorResponse, error) {
	return &pdpb.GetOperatorResponse{
		Header: new(pdpb.ResponseHeader),
	}, nil
}

func (c *TestClient) ScanRegions(ctx context.Context, key, endKey []byte, limit int) ([]*RegionInfo, error) {
	if c.InjectErr && c.InjectTimes > 0 {
		c.InjectTimes -= 1
		return nil, status.Error(codes.Unavailable, "not leader")
	}
	if len(key) != 0 && bytes.Equal(key, endKey) {
		return nil, status.Error(codes.Internal, "key and endKey are the same")
	}

	infos := c.RegionsInfo.ScanRange(key, endKey, limit)
	regions := make([]*RegionInfo, 0, len(infos))
	for _, info := range infos {
		regions = append(regions, &RegionInfo{
			Region: info.Meta,
			Leader: info.Leader,
		})
	}
	return regions, nil
}

func (c *TestClient) WaitRegionsScattered(context.Context, []*RegionInfo) (int, error) {
	return 0, nil
}

// MockPDClientForSplit is a mock PD client for testing split and scatter.
type MockPDClientForSplit struct {
	pd.Client

	mu sync.Mutex

	stores       map[uint64]*metapb.Store
	Regions      *pdtypes.RegionTree
	lastRegionID uint64
	scanRegions  struct {
		errors     []error
		beforeHook func()
	}
	splitRegions struct {
		count    int
		hijacked func() (bool, *kvrpcpb.SplitRegionResponse, error)
	}
	scatterRegion struct {
		eachRegionFailBefore int
		count                map[uint64]int
	}
	scatterRegions struct {
		notImplemented bool
		regionCount    int
	}
	getOperator struct {
		responses map[uint64][]*pdpb.GetOperatorResponse
	}
}

// NewMockPDClientForSplit creates a new MockPDClientForSplit.
func NewMockPDClientForSplit() *MockPDClientForSplit {
	ret := &MockPDClientForSplit{}
	ret.Regions = &pdtypes.RegionTree{}
	ret.scatterRegion.count = make(map[uint64]int)
	return ret
}

func newRegionNotFullyReplicatedErr(regionID uint64) error {
	return status.Errorf(codes.Unknown, "region %d is not fully replicated", regionID)
}

func (c *MockPDClientForSplit) SetRegions(boundaries [][]byte) []*metapb.Region {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.setRegions(boundaries)
}

func (c *MockPDClientForSplit) SetStores(stores map[uint64]*metapb.Store) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.stores = stores
}

func (c *MockPDClientForSplit) setRegions(boundaries [][]byte) []*metapb.Region {
	ret := make([]*metapb.Region, 0, len(boundaries)-1)
	for i := 1; i < len(boundaries); i++ {
		c.lastRegionID++
		r := &metapb.Region{
			Id:       c.lastRegionID,
			StartKey: boundaries[i-1],
			EndKey:   boundaries[i],
		}
		p := &metapb.Peer{
			Id:      c.lastRegionID,
			StoreId: 1,
		}
		c.Regions.SetRegion(&pdtypes.Region{
			Meta:   r,
			Leader: p,
		})
		ret = append(ret, r)
	}
	return ret
}

func (c *MockPDClientForSplit) ScanRegions(
	_ context.Context,
	key, endKey []byte,
	limit int,
	_ ...opt.GetRegionOption,
) ([]*router.Region, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.scanRegions.errors) > 0 {
		err := c.scanRegions.errors[0]
		c.scanRegions.errors = c.scanRegions.errors[1:]
		return nil, err
	}

	if c.scanRegions.beforeHook != nil {
		c.scanRegions.beforeHook()
	}

	regions := c.Regions.ScanRange(key, endKey, limit)
	ret := make([]*router.Region, 0, len(regions))
	for _, r := range regions {
		ret = append(ret, &router.Region{
			Meta:   r.Meta,
			Leader: r.Leader,
		})
	}
	return ret, nil
}

func (c *MockPDClientForSplit) BatchScanRegions(
	_ context.Context,
	keyRanges []router.KeyRange,
	limit int,
	_ ...opt.GetRegionOption,
) ([]*router.Region, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.scanRegions.errors) > 0 {
		err := c.scanRegions.errors[0]
		c.scanRegions.errors = c.scanRegions.errors[1:]
		return nil, err
	}

	if c.scanRegions.beforeHook != nil {
		c.scanRegions.beforeHook()
	}

	regions := make([]*router.Region, 0, len(keyRanges))
	var lastRegion *pdtypes.Region
	for _, keyRange := range keyRanges {
		if lastRegion != nil {
			if len(lastRegion.Meta.EndKey) == 0 || bytes.Compare(lastRegion.Meta.EndKey, keyRange.EndKey) >= 0 {
				continue
			}
			if bytes.Compare(lastRegion.Meta.EndKey, keyRange.StartKey) > 0 {
				keyRange.StartKey = lastRegion.Meta.EndKey
			}
		}
		rs := c.Regions.ScanRange(keyRange.StartKey, keyRange.EndKey, limit)
		for _, r := range rs {
			lastRegion = r
			regions = append(regions, &router.Region{
				Meta:   r.Meta,
				Leader: r.Leader,
			})
		}
	}
	return regions, nil
}

func (c *MockPDClientForSplit) GetRegionByID(_ context.Context, regionID uint64, _ ...opt.GetRegionOption) (*router.Region, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, r := range c.Regions.Regions {
		if r.Meta.Id == regionID {
			return &router.Region{
				Meta:   r.Meta,
				Leader: r.Leader,
			}, nil
		}
	}
	return nil, errors.New("region not found")
}

func (c *MockPDClientForSplit) SplitRegion(
	region *RegionInfo,
	keys [][]byte,
	isRawKV bool,
) (bool, *kvrpcpb.SplitRegionResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.splitRegions.count++
	if c.splitRegions.hijacked != nil {
		return c.splitRegions.hijacked()
	}

	if !isRawKV {
		for i := range keys {
			keys[i] = codec.EncodeBytes(nil, keys[i])
		}
	}

	newRegionBoundaries := make([][]byte, 0, len(keys)+2)
	newRegionBoundaries = append(newRegionBoundaries, region.Region.StartKey)
	newRegionBoundaries = append(newRegionBoundaries, keys...)
	newRegionBoundaries = append(newRegionBoundaries, region.Region.EndKey)
	newRegions := c.setRegions(newRegionBoundaries)
	newRegions[0].Id = region.Region.Id
	return false, &kvrpcpb.SplitRegionResponse{Regions: newRegions}, nil
}

func (c *MockPDClientForSplit) ScatterRegion(_ context.Context, regionID uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.scatterRegion.count[regionID]++
	if c.scatterRegion.count[regionID] > c.scatterRegion.eachRegionFailBefore {
		return nil
	}
	return newRegionNotFullyReplicatedErr(regionID)
}

func (c *MockPDClientForSplit) ScatterRegions(_ context.Context, regionIDs []uint64, _ ...opt.RegionsOption) (*pdpb.ScatterRegionResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.scatterRegions.notImplemented {
		return nil, status.Error(codes.Unimplemented, "Ah, yep")
	}
	c.scatterRegions.regionCount += len(regionIDs)
	return &pdpb.ScatterRegionResponse{}, nil
}

func (c *MockPDClientForSplit) GetOperator(_ context.Context, regionID uint64) (*pdpb.GetOperatorResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.getOperator.responses == nil {
		return &pdpb.GetOperatorResponse{Desc: []byte("scatter-region"), Status: pdpb.OperatorStatus_SUCCESS}, nil
	}
	ret := c.getOperator.responses[regionID][0]
	c.getOperator.responses[regionID] = c.getOperator.responses[regionID][1:]
	return ret, nil
}

func (c *MockPDClientForSplit) GetStore(_ context.Context, storeID uint64) (*metapb.Store, error) {
	return c.stores[storeID], nil
}

var DefaultTestKeepaliveCfg = keepalive.ClientParameters{
	Time:    3 * time.Second,
	Timeout: 10 * time.Second,
}

var (
	ExpectPDCfgGeneratorsResult = map[string]any{
		"merge-schedule-limit":        0,
		"leader-schedule-limit":       float64(40),
		"region-schedule-limit":       float64(40),
		"max-snapshot-count":          float64(40),
		"enable-location-replacement": "false",
		"max-pending-peer-count":      uint64(math.MaxInt32),
	}

	ExistPDCfgGeneratorBefore = map[string]any{
		"merge-schedule-limit":        100,
		"leader-schedule-limit":       float64(100),
		"region-schedule-limit":       float64(100),
		"max-snapshot-count":          float64(100),
		"enable-location-replacement": "true",
		"max-pending-peer-count":      100,
	}
)

type FakePDHTTPClient struct {
	pdhttp.Client

	expireSchedulers map[string]time.Time
	cfgs             map[string]any

	rules map[string]*pdhttp.Rule
}

func NewFakePDHTTPClient() *FakePDHTTPClient {
	return &FakePDHTTPClient{
		expireSchedulers: make(map[string]time.Time),
		cfgs:             make(map[string]any),

		rules: make(map[string]*pdhttp.Rule),
	}
}

func (fpdh *FakePDHTTPClient) GetScheduleConfig(_ context.Context) (map[string]any, error) {
	return ExistPDCfgGeneratorBefore, nil
}

func (fpdh *FakePDHTTPClient) GetSchedulers(_ context.Context) ([]string, error) {
	schedulers := make([]string, 0, len(pdutil.Schedulers))
	for scheduler := range pdutil.Schedulers {
		schedulers = append(schedulers, scheduler)
	}
	return schedulers, nil
}

func (fpdh *FakePDHTTPClient) SetSchedulerDelay(_ context.Context, key string, delay int64) error {
	expireTime, ok := fpdh.expireSchedulers[key]
	if ok {
		if time.Now().Compare(expireTime) > 0 {
			return errors.Errorf("the scheduler config set is expired")
		}
		if delay == 0 {
			delete(fpdh.expireSchedulers, key)
		}
	}
	if !ok && delay == 0 {
		return errors.Errorf("set the nonexistent scheduler")
	}
	expireTime = time.Now().Add(time.Second * time.Duration(delay))
	fpdh.expireSchedulers[key] = expireTime
	return nil
}

func (fpdh *FakePDHTTPClient) SetConfig(_ context.Context, config map[string]any, ttl ...float64) error {
	for key, value := range config {
		fpdh.cfgs[key] = value
	}
	return nil
}

func (fpdh *FakePDHTTPClient) GetConfig(_ context.Context) (map[string]any, error) {
	return fpdh.cfgs, nil
}

func (fpdh *FakePDHTTPClient) GetDelaySchedulers() map[string]struct{} {
	delaySchedulers := make(map[string]struct{})
	for key, t := range fpdh.expireSchedulers {
		now := time.Now()
		if now.Compare(t) < 0 {
			delaySchedulers[key] = struct{}{}
		}
	}
	return delaySchedulers
}

func (fpdh *FakePDHTTPClient) GetPlacementRule(_ context.Context, groupID string, ruleID string) (*pdhttp.Rule, error) {
	rule, ok := fpdh.rules[ruleID]
	if !ok {
		rule = &pdhttp.Rule{
			GroupID: groupID,
			ID:      ruleID,
		}
		fpdh.rules[ruleID] = rule
	}
	return rule, nil
}

func (fpdh *FakePDHTTPClient) SetPlacementRule(_ context.Context, rule *pdhttp.Rule) error {
	fpdh.rules[rule.ID] = rule
	return nil
}

func (fpdh *FakePDHTTPClient) DeletePlacementRule(_ context.Context, groupID string, ruleID string) error {
	delete(fpdh.rules, ruleID)
	return nil
}

type FakePDClient struct {
	pd.Client
	stores  []*metapb.Store
	regions []*router.Region

	notLeader  bool
	retryTimes *int

	peerStoreId uint64
}

func NewFakePDClient(stores []*metapb.Store, notLeader bool, retryTime *int) *FakePDClient {
	var retryTimeInternal int
	if retryTime == nil {
		retryTime = &retryTimeInternal
	}
	return &FakePDClient{
		stores: stores,

		notLeader:  notLeader,
		retryTimes: retryTime,

		peerStoreId: 0,
	}
}

func (fpdc *FakePDClient) SetRegions(regions []*router.Region) {
	fpdc.regions = regions
}

func (fpdc *FakePDClient) GetAllStores(context.Context, ...opt.GetStoreOption) ([]*metapb.Store, error) {
	return append([]*metapb.Store{}, fpdc.stores...), nil
}

func (fpdc *FakePDClient) ScanRegions(
	ctx context.Context,
	key, endKey []byte,
	limit int,
	opts ...opt.GetRegionOption,
) ([]*router.Region, error) {
	regions := make([]*router.Region, 0, len(fpdc.regions))
	fpdc.peerStoreId = fpdc.peerStoreId + 1
	peerStoreId := (fpdc.peerStoreId + 1) / 2
	for _, region := range fpdc.regions {
		if len(endKey) != 0 && bytes.Compare(region.Meta.StartKey, endKey) >= 0 {
			continue
		}
		if len(region.Meta.EndKey) != 0 && bytes.Compare(region.Meta.EndKey, key) <= 0 {
			continue
		}
		region.Meta.Peers = []*metapb.Peer{{StoreId: peerStoreId}}
		regions = append(regions, region)
	}
	return regions, nil
}

func (fpdc *FakePDClient) BatchScanRegions(
	ctx context.Context,
	ranges []router.KeyRange,
	limit int,
	opts ...opt.GetRegionOption,
) ([]*router.Region, error) {
	regions := make([]*router.Region, 0, len(fpdc.regions))
	fpdc.peerStoreId = fpdc.peerStoreId + 1
	peerStoreId := (fpdc.peerStoreId + 1) / 2
	for _, region := range fpdc.regions {
		inRange := false
		for _, keyRange := range ranges {
			if len(keyRange.EndKey) != 0 && bytes.Compare(region.Meta.StartKey, keyRange.EndKey) >= 0 {
				continue
			}
			if len(region.Meta.EndKey) != 0 && bytes.Compare(region.Meta.EndKey, keyRange.StartKey) <= 0 {
				continue
			}
			inRange = true
		}
		if inRange {
			region.Meta.Peers = []*metapb.Peer{{StoreId: peerStoreId}}
			regions = append(regions, region)
		}
	}
	return nil, nil
}

func (fpdc *FakePDClient) GetTS(ctx context.Context) (int64, int64, error) {
	(*fpdc.retryTimes)++
	if *fpdc.retryTimes >= 3 { // the mock PD leader switched successfully
		fpdc.notLeader = false
	}

	if fpdc.notLeader {
		return 0, 0, errors.Errorf(
			"rpc error: code = Unknown desc = [PD:tso:ErrGenerateTimestamp]generate timestamp failed, " +
				"requested pd is not leader of cluster",
		)
	}
	return 1, 1, nil
}

type FakeSplitClient struct {
	SplitClient
	regions []*RegionInfo
}

func NewFakeSplitClient() *FakeSplitClient {
	return &FakeSplitClient{
		regions: make([]*RegionInfo, 0),
	}
}

func (f *FakeSplitClient) AppendRegion(startKey, endKey []byte) {
	f.regions = append(f.regions, &RegionInfo{
		Region: &metapb.Region{
			StartKey: startKey,
			EndKey:   endKey,
		},
	})
}

func (f *FakeSplitClient) AppendPdRegion(region *router.Region) {
	f.regions = append(f.regions, &RegionInfo{
		Region: region.Meta,
		Leader: region.Leader,
	})
}

func (f *FakeSplitClient) ScanRegions(
	ctx context.Context,
	startKey, endKey []byte,
	limit int,
) ([]*RegionInfo, error) {
	result := make([]*RegionInfo, 0)
	count := 0
	for _, rng := range f.regions {
		if bytes.Compare(rng.Region.StartKey, endKey) <= 0 && bytes.Compare(rng.Region.EndKey, startKey) > 0 {
			result = append(result, rng)
			count++
		}
		if count >= limit {
			break
		}
	}
	return result, nil
}

func (f *FakeSplitClient) WaitRegionsScattered(context.Context, []*RegionInfo) (int, error) {
	return 0, nil
}
