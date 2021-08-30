// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore_test

import (
	"bytes"
	"context"
	"sync"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/util/codec"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule/placement"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type TestClient struct {
	mu           sync.RWMutex
	stores       map[uint64]*metapb.Store
	regions      map[uint64]*restore.RegionInfo
	regionsInfo  *core.RegionsInfo // For now it's only used in ScanRegions
	nextRegionID uint64

	scattered map[uint64]bool
}

func NewTestClient(
	stores map[uint64]*metapb.Store,
	regions map[uint64]*restore.RegionInfo,
	nextRegionID uint64,
) *TestClient {
	regionsInfo := core.NewRegionsInfo()
	for _, regionInfo := range regions {
		regionsInfo.SetRegion(core.NewRegionInfo(regionInfo.Region, regionInfo.Leader))
	}
	return &TestClient{
		stores:       stores,
		regions:      regions,
		regionsInfo:  regionsInfo,
		nextRegionID: nextRegionID,
		scattered:    map[uint64]bool{},
	}
}

func (c *TestClient) GetAllRegions() map[uint64]*restore.RegionInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.regions
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

func (c *TestClient) GetRegion(ctx context.Context, key []byte) (*restore.RegionInfo, error) {
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

func (c *TestClient) GetRegionByID(ctx context.Context, regionID uint64) (*restore.RegionInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	region, ok := c.regions[regionID]
	if !ok {
		return nil, errors.Errorf("region not found: id=%d", regionID)
	}
	return region, nil
}

func (c *TestClient) SplitRegion(
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
		},
	}
	c.regions[c.nextRegionID] = newRegion
	c.nextRegionID++
	target.Region.StartKey = splitKey
	c.regions[target.Region.Id] = target
	return newRegion, nil
}

func (c *TestClient) BatchSplitRegionsWithOrigin(
	ctx context.Context, regionInfo *restore.RegionInfo, keys [][]byte,
) (*restore.RegionInfo, []*restore.RegionInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	newRegions := make([]*restore.RegionInfo, 0)
	var region *restore.RegionInfo
	for _, key := range keys {
		var target *restore.RegionInfo
		splitKey := codec.EncodeBytes([]byte{}, key)
		for _, region := range c.regions {
			if region.ContainsInterior(splitKey) {
				target = region
			}
		}
		if target == nil {
			continue
		}
		newRegion := &restore.RegionInfo{
			Region: &metapb.Region{
				Peers:    target.Region.Peers,
				Id:       c.nextRegionID,
				StartKey: target.Region.StartKey,
				EndKey:   splitKey,
			},
		}
		c.regions[c.nextRegionID] = newRegion
		c.nextRegionID++
		target.Region.StartKey = splitKey
		c.regions[target.Region.Id] = target
		region = target
		newRegions = append(newRegions, newRegion)
	}
	return region, newRegions, nil
}

func (c *TestClient) BatchSplitRegions(
	ctx context.Context, regionInfo *restore.RegionInfo, keys [][]byte,
) ([]*restore.RegionInfo, error) {
	_, newRegions, err := c.BatchSplitRegionsWithOrigin(ctx, regionInfo, keys)
	return newRegions, err
}

func (c *TestClient) ScatterRegion(ctx context.Context, regionInfo *restore.RegionInfo) error {
	if _, ok := c.scattered[regionInfo.Region.Id]; !ok {
		c.scattered[regionInfo.Region.Id] = false
		return status.Errorf(codes.Unknown, "region %d is not fully replicated", regionInfo.Region.Id)
	}
	c.scattered[regionInfo.Region.Id] = true
	return nil
}

func (c *TestClient) GetOperator(ctx context.Context, regionID uint64) (*pdpb.GetOperatorResponse, error) {
	return &pdpb.GetOperatorResponse{
		Header: new(pdpb.ResponseHeader),
	}, nil
}

func (c *TestClient) ScanRegions(ctx context.Context, key, endKey []byte, limit int) ([]*restore.RegionInfo, error) {
	infos := c.regionsInfo.ScanRange(key, endKey, limit)
	regions := make([]*restore.RegionInfo, 0, len(infos))
	for _, info := range infos {
		regions = append(regions, &restore.RegionInfo{
			Region: info.GetMeta(),
			Leader: info.GetLeader(),
		})
	}
	return regions, nil
}

func (c *TestClient) GetPlacementRule(ctx context.Context, groupID, ruleID string) (r placement.Rule, err error) {
	return
}

func (c *TestClient) SetPlacementRule(ctx context.Context, rule placement.Rule) error {
	return nil
}

func (c *TestClient) DeletePlacementRule(ctx context.Context, groupID, ruleID string) error {
	return nil
}

func (c *TestClient) SetStoresLabel(ctx context.Context, stores []uint64, labelKey, labelValue string) error {
	return nil
}

func (c *TestClient) checkScatter(check *C) {
	regions := c.GetAllRegions()
	for key := range regions {
		if !c.scattered[key] {
			check.Fatalf("region %d has not been scattered: %#v", key, regions[key])
		}
	}
}

// region: [, aay), [aay, bba), [bba, bbh), [bbh, cca), [cca, )
// range: [aaa, aae), [aae, aaz), [ccd, ccf), [ccf, ccj)
// rewrite rules: aa -> xx,  cc -> bb
// expected regions after split:
//   [, aay), [aay, bb), [bb, bba), [bba, bbf), [bbf, bbh), [bbh, bbj),
//   [bbj, cca), [cca, xx), [xx, xxe), [xxe, xxz), [xxz, )
func (s *testRangeSuite) TestSplitAndScatter(c *C) {
	client := initTestClient()
	ranges := initRanges()
	rewriteRules := initRewriteRules()
	regionSplitter := restore.NewRegionSplitter(client)

	ctx := context.Background()
	err := regionSplitter.Split(ctx, ranges, rewriteRules, func(key [][]byte) {})
	if err != nil {
		c.Assert(err, IsNil, Commentf("split regions failed: %v", err))
	}
	regions := client.GetAllRegions()
	if !validateRegions(regions) {
		for _, region := range regions {
			c.Logf("region: %v\n", region.Region)
		}
		c.Log("get wrong result")
		c.Fail()
	}
	regionInfos := make([]*restore.RegionInfo, 0, len(regions))
	for _, info := range regions {
		regionInfos = append(regionInfos, info)
	}
	regionSplitter.ScatterRegions(ctx, regionInfos)
	client.checkScatter(c)
}

// region: [, aay), [aay, bba), [bba, bbh), [bbh, cca), [cca, )
func initTestClient() *TestClient {
	peers := make([]*metapb.Peer, 1)
	peers[0] = &metapb.Peer{
		Id:      1,
		StoreId: 1,
	}
	keys := [6]string{"", "aay", "bba", "bbh", "cca", ""}
	regions := make(map[uint64]*restore.RegionInfo)
	for i := uint64(1); i < 6; i++ {
		startKey := []byte(keys[i-1])
		if len(startKey) != 0 {
			startKey = codec.EncodeBytes([]byte{}, startKey)
		}
		endKey := []byte(keys[i])
		if len(endKey) != 0 {
			endKey = codec.EncodeBytes([]byte{}, endKey)
		}
		regions[i] = &restore.RegionInfo{
			Region: &metapb.Region{
				Id:       i,
				Peers:    peers,
				StartKey: startKey,
				EndKey:   endKey,
			},
		}
	}
	stores := make(map[uint64]*metapb.Store)
	stores[1] = &metapb.Store{
		Id: 1,
	}
	return NewTestClient(stores, regions, 6)
}

// range: [aaa, aae), [aae, aaz), [ccd, ccf), [ccf, ccj)
func initRanges() []rtree.Range {
	var ranges [4]rtree.Range
	ranges[0] = rtree.Range{
		StartKey: []byte("aaa"),
		EndKey:   []byte("aae"),
	}
	ranges[1] = rtree.Range{
		StartKey: []byte("aae"),
		EndKey:   []byte("aaz"),
	}
	ranges[2] = rtree.Range{
		StartKey: []byte("ccd"),
		EndKey:   []byte("ccf"),
	}
	ranges[3] = rtree.Range{
		StartKey: []byte("ccf"),
		EndKey:   []byte("ccj"),
	}
	return ranges[:]
}

func initRewriteRules() *restore.RewriteRules {
	var rules [2]*import_sstpb.RewriteRule
	rules[0] = &import_sstpb.RewriteRule{
		OldKeyPrefix: []byte("aa"),
		NewKeyPrefix: []byte("xx"),
	}
	rules[1] = &import_sstpb.RewriteRule{
		OldKeyPrefix: []byte("cc"),
		NewKeyPrefix: []byte("bb"),
	}
	return &restore.RewriteRules{
		Data: rules[:],
	}
}

// expected regions after split:
//   [, aay), [aay, bb), [bb, bba), [bba, bbf), [bbf, bbh), [bbh, bbj),
//   [bbj, cca), [cca, xx), [xx, xxe), [xxe, xxz), [xxz, )
func validateRegions(regions map[uint64]*restore.RegionInfo) bool {
	keys := [12]string{"", "aay", "bb", "bba", "bbf", "bbh", "bbj", "cca", "xx", "xxe", "xxz", ""}
	if len(regions) != 11 {
		return false
	}
FindRegion:
	for i := 1; i < len(keys); i++ {
		for _, region := range regions {
			startKey := []byte(keys[i-1])
			if len(startKey) != 0 {
				startKey = codec.EncodeBytes([]byte{}, startKey)
			}
			endKey := []byte(keys[i])
			if len(endKey) != 0 {
				endKey = codec.EncodeBytes([]byte{}, endKey)
			}
			if bytes.Equal(region.Region.GetStartKey(), startKey) &&
				bytes.Equal(region.Region.GetEndKey(), endKey) {
				continue FindRegion
			}
		}
		return false
	}
	return true
}

func (s *testRangeSuite) TestNeedSplit(c *C) {
	regions := []*restore.RegionInfo{
		{
			Region: &metapb.Region{
				StartKey: codec.EncodeBytes([]byte{}, []byte("b")),
				EndKey:   codec.EncodeBytes([]byte{}, []byte("d")),
			},
		},
	}
	// Out of region
	c.Assert(restore.NeedSplit([]byte("a"), regions), IsNil)
	// Region start key
	c.Assert(restore.NeedSplit([]byte("b"), regions), IsNil)
	// In region
	region := restore.NeedSplit([]byte("c"), regions)
	c.Assert(bytes.Compare(region.Region.GetStartKey(), codec.EncodeBytes([]byte{}, []byte("b"))), Equals, 0)
	c.Assert(bytes.Compare(region.Region.GetEndKey(), codec.EncodeBytes([]byte{}, []byte("d"))), Equals, 0)
	// Region end key
	c.Assert(restore.NeedSplit([]byte("d"), regions), IsNil)
	// Out of region
	c.Assert(restore.NeedSplit([]byte("e"), regions), IsNil)
}
