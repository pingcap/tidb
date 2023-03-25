// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore_test

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/br/pkg/utils/iter"
	"github.com/pingcap/tidb/store/pdtypes"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	"github.com/stretchr/testify/require"
	"go.uber.org/multierr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type TestClient struct {
	mu                  sync.RWMutex
	stores              map[uint64]*metapb.Store
	regions             map[uint64]*split.RegionInfo
	regionsInfo         *pdtypes.RegionTree // For now it's only used in ScanRegions
	nextRegionID        uint64
	injectInScatter     func(*split.RegionInfo) error
	supportBatchScatter bool

	scattered   map[uint64]bool
	InjectErr   bool
	InjectTimes int32
}

func NewTestClient(
	stores map[uint64]*metapb.Store,
	regions map[uint64]*split.RegionInfo,
	nextRegionID uint64,
) *TestClient {
	regionsInfo := &pdtypes.RegionTree{}
	for _, regionInfo := range regions {
		regionsInfo.SetRegion(pdtypes.NewRegionInfo(regionInfo.Region, regionInfo.Leader))
	}
	return &TestClient{
		stores:          stores,
		regions:         regions,
		regionsInfo:     regionsInfo,
		nextRegionID:    nextRegionID,
		scattered:       map[uint64]bool{},
		injectInScatter: func(*split.RegionInfo) error { return nil },
	}
}

func (c *TestClient) InstallBatchScatterSupport() {
	c.supportBatchScatter = true
}

// ScatterRegions scatters regions in a batch.
func (c *TestClient) ScatterRegions(ctx context.Context, regionInfo []*split.RegionInfo) error {
	if !c.supportBatchScatter {
		return status.Error(codes.Unimplemented, "Ah, yep")
	}
	regions := map[uint64]*split.RegionInfo{}
	for _, region := range regionInfo {
		regions[region.Region.Id] = region
	}
	var err error
	for i := 0; i < 3; i++ {
		if len(regions) == 0 {
			return nil
		}
		for id, region := range regions {
			splitErr := c.ScatterRegion(ctx, region)
			if splitErr == nil {
				delete(regions, id)
			}
			err = multierr.Append(err, splitErr)
		}
	}
	return nil
}

func (c *TestClient) GetAllRegions() map[uint64]*split.RegionInfo {
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

func (c *TestClient) GetRegion(ctx context.Context, key []byte) (*split.RegionInfo, error) {
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

func (c *TestClient) GetRegionByID(ctx context.Context, regionID uint64) (*split.RegionInfo, error) {
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
	regionInfo *split.RegionInfo,
	key []byte,
) (*split.RegionInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var target *split.RegionInfo
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
	newRegion := &split.RegionInfo{
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
	ctx context.Context, regionInfo *split.RegionInfo, keys [][]byte,
) (*split.RegionInfo, []*split.RegionInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	newRegions := make([]*split.RegionInfo, 0)
	var region *split.RegionInfo
	for _, key := range keys {
		var target *split.RegionInfo
		splitKey := codec.EncodeBytes([]byte{}, key)
		for _, region := range c.regions {
			if region.ContainsInterior(splitKey) {
				target = region
			}
		}
		if target == nil {
			continue
		}
		newRegion := &split.RegionInfo{
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
	ctx context.Context, regionInfo *split.RegionInfo, keys [][]byte,
) ([]*split.RegionInfo, error) {
	_, newRegions, err := c.BatchSplitRegionsWithOrigin(ctx, regionInfo, keys)
	return newRegions, err
}

func (c *TestClient) ScatterRegion(ctx context.Context, regionInfo *split.RegionInfo) error {
	return c.injectInScatter(regionInfo)
}

func (c *TestClient) GetOperator(ctx context.Context, regionID uint64) (*pdpb.GetOperatorResponse, error) {
	return &pdpb.GetOperatorResponse{
		Header: new(pdpb.ResponseHeader),
	}, nil
}

func (c *TestClient) ScanRegions(ctx context.Context, key, endKey []byte, limit int) ([]*split.RegionInfo, error) {
	if c.InjectErr && c.InjectTimes > 0 {
		c.InjectTimes -= 1
		return nil, status.Error(codes.Unavailable, "not leader")
	}

	infos := c.regionsInfo.ScanRange(key, endKey, limit)
	regions := make([]*split.RegionInfo, 0, len(infos))
	for _, info := range infos {
		regions = append(regions, &split.RegionInfo{
			Region: info.Meta,
			Leader: info.Leader,
		})
	}
	return regions, nil
}

func (c *TestClient) GetPlacementRule(ctx context.Context, groupID, ruleID string) (r pdtypes.Rule, err error) {
	return
}

func (c *TestClient) SetPlacementRule(ctx context.Context, rule pdtypes.Rule) error {
	return nil
}

func (c *TestClient) DeletePlacementRule(ctx context.Context, groupID, ruleID string) error {
	return nil
}

func (c *TestClient) SetStoresLabel(ctx context.Context, stores []uint64, labelKey, labelValue string) error {
	return nil
}

type assertRetryLessThanBackoffer struct {
	max     int
	already int
	t       *testing.T
}

func assertRetryLessThan(t *testing.T, times int) utils.Backoffer {
	return &assertRetryLessThanBackoffer{
		max:     times,
		already: 0,
		t:       t,
	}
}

// NextBackoff returns a duration to wait before retrying again
func (b *assertRetryLessThanBackoffer) NextBackoff(err error) time.Duration {
	b.already++
	if b.already >= b.max {
		b.t.Logf("retry more than %d time: test failed", b.max)
		b.t.FailNow()
	}
	return 0
}

// Attempt returns the remain attempt times
func (b *assertRetryLessThanBackoffer) Attempt() int {
	return b.max - b.already
}

func TestScatterFinishInTime(t *testing.T) {
	client := initTestClient(false)
	ranges := initRanges()
	rewriteRules := initRewriteRules()
	regionSplitter := restore.NewRegionSplitter(client)

	ctx := context.Background()
	err := regionSplitter.Split(ctx, ranges, rewriteRules, false, func(key [][]byte) {})
	require.NoError(t, err)
	regions := client.GetAllRegions()
	if !validateRegions(regions) {
		for _, region := range regions {
			t.Logf("region: %v\n", region.Region)
		}
		t.Log("get wrong result")
		t.Fail()
	}

	regionInfos := make([]*split.RegionInfo, 0, len(regions))
	for _, info := range regions {
		regionInfos = append(regionInfos, info)
	}
	failed := map[uint64]int{}
	client.injectInScatter = func(r *split.RegionInfo) error {
		failed[r.Region.Id]++
		if failed[r.Region.Id] > 7 {
			return nil
		}
		return status.Errorf(codes.Unknown, "region %d is not fully replicated", r.Region.Id)
	}

	// When using a exponential backoffer, if we try to backoff more than 40 times in 10 regions,
	// it would cost time unacceptable.
	regionSplitter.ScatterRegionsWithBackoffer(ctx,
		regionInfos,
		assertRetryLessThan(t, 40))
}

// region: [, aay), [aay, bba), [bba, bbh), [bbh, cca), [cca, )
// range: [aaa, aae), [aae, aaz), [ccd, ccf), [ccf, ccj)
// rewrite rules: aa -> xx,  cc -> bb
// expected regions after split:
//
//	[, aay), [aay, bba), [bba, bbf), [bbf, bbh), [bbh, bbj),
//	[bbj, cca), [cca, xxe), [xxe, xxz), [xxz, )
func TestSplitAndScatter(t *testing.T) {
	t.Run("BatchScatter", func(t *testing.T) {
		client := initTestClient(false)
		client.InstallBatchScatterSupport()
		runTestSplitAndScatterWith(t, client)
	})
	t.Run("BackwardCompatibility", func(t *testing.T) {
		client := initTestClient(false)
		runTestSplitAndScatterWith(t, client)
	})
}

func runTestSplitAndScatterWith(t *testing.T, client *TestClient) {
	ranges := initRanges()
	rewriteRules := initRewriteRules()
	regionSplitter := restore.NewRegionSplitter(client)

	ctx := context.Background()
	err := regionSplitter.Split(ctx, ranges, rewriteRules, false, func(key [][]byte) {})
	require.NoError(t, err)
	regions := client.GetAllRegions()
	if !validateRegions(regions) {
		for _, region := range regions {
			t.Logf("region: %v\n", region.Region)
		}
		t.Log("get wrong result")
		t.Fail()
	}
	regionInfos := make([]*split.RegionInfo, 0, len(regions))
	for _, info := range regions {
		regionInfos = append(regionInfos, info)
	}
	scattered := map[uint64]bool{}
	const alwaysFailedRegionID = 1
	client.injectInScatter = func(regionInfo *split.RegionInfo) error {
		if _, ok := scattered[regionInfo.Region.Id]; !ok || regionInfo.Region.Id == alwaysFailedRegionID {
			scattered[regionInfo.Region.Id] = false
			return status.Errorf(codes.Unknown, "region %d is not fully replicated", regionInfo.Region.Id)
		}
		scattered[regionInfo.Region.Id] = true
		return nil
	}
	regionSplitter.ScatterRegions(ctx, regionInfos)
	for key := range regions {
		if key == alwaysFailedRegionID {
			require.Falsef(t, scattered[key], "always failed region %d was scattered successfully", key)
		} else if !scattered[key] {
			t.Fatalf("region %d has not been scattered: %#v", key, regions[key])
		}
	}
}

func TestRawSplit(t *testing.T) {
	// Fix issue #36490.
	ranges := []rtree.Range{
		{
			StartKey: []byte{0},
			EndKey:   []byte{},
		},
	}
	client := initTestClient(true)
	ctx := context.Background()

	regionSplitter := restore.NewRegionSplitter(client)
	err := regionSplitter.Split(ctx, ranges, nil, true, func(key [][]byte) {})
	require.NoError(t, err)
	regions := client.GetAllRegions()
	expectedKeys := []string{"", "aay", "bba", "bbh", "cca", ""}
	if !validateRegionsExt(regions, expectedKeys, true) {
		for _, region := range regions {
			t.Logf("region: %v\n", region.Region)
		}
		t.Log("get wrong result")
		t.Fail()
	}
}

// region: [, aay), [aay, bba), [bba, bbh), [bbh, cca), [cca, )
func initTestClient(isRawKv bool) *TestClient {
	peers := make([]*metapb.Peer, 1)
	peers[0] = &metapb.Peer{
		Id:      1,
		StoreId: 1,
	}
	keys := [6]string{"", "aay", "bba", "bbh", "cca", ""}
	regions := make(map[uint64]*split.RegionInfo)
	for i := uint64(1); i < 6; i++ {
		startKey := []byte(keys[i-1])
		if len(startKey) != 0 {
			startKey = codec.EncodeBytesExt([]byte{}, startKey, isRawKv)
		}
		endKey := []byte(keys[i])
		if len(endKey) != 0 {
			endKey = codec.EncodeBytesExt([]byte{}, endKey, isRawKv)
		}
		regions[i] = &split.RegionInfo{
			Leader: &metapb.Peer{
				Id: i,
			},
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
//
//	[, aay), [aay, bba), [bba, bbf), [bbf, bbh), [bbh, bbj),
//	[bbj, cca), [cca, xxe), [xxe, xxz), [xxz, )
func validateRegions(regions map[uint64]*split.RegionInfo) bool {
	keys := [...]string{"", "aay", "bba", "bbf", "bbh", "bbj", "cca", "xxe", "xxz", ""}
	return validateRegionsExt(regions, keys[:], false)
}

func validateRegionsExt(regions map[uint64]*split.RegionInfo, expectedKeys []string, isRawKv bool) bool {
	if len(regions) != len(expectedKeys)-1 {
		return false
	}
FindRegion:
	for i := 1; i < len(expectedKeys); i++ {
		for _, region := range regions {
			startKey := []byte(expectedKeys[i-1])
			if len(startKey) != 0 {
				startKey = codec.EncodeBytesExt([]byte{}, startKey, isRawKv)
			}
			endKey := []byte(expectedKeys[i])
			if len(endKey) != 0 {
				endKey = codec.EncodeBytesExt([]byte{}, endKey, isRawKv)
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

func TestNeedSplit(t *testing.T) {
	testNeedSplit(t, false)
	testNeedSplit(t, true)
}

func testNeedSplit(t *testing.T, isRawKv bool) {
	regions := []*split.RegionInfo{
		{
			Region: &metapb.Region{
				StartKey: codec.EncodeBytesExt(nil, []byte("b"), isRawKv),
				EndKey:   codec.EncodeBytesExt(nil, []byte("d"), isRawKv),
			},
		},
	}
	// Out of region
	require.Nil(t, restore.NeedSplit([]byte("a"), regions, isRawKv))
	// Region start key
	require.Nil(t, restore.NeedSplit([]byte("b"), regions, isRawKv))
	// In region
	region := restore.NeedSplit([]byte("c"), regions, isRawKv)
	require.Equal(t, 0, bytes.Compare(region.Region.GetStartKey(), codec.EncodeBytesExt(nil, []byte("b"), isRawKv)))
	require.Equal(t, 0, bytes.Compare(region.Region.GetEndKey(), codec.EncodeBytesExt(nil, []byte("d"), isRawKv)))
	// Region end key
	require.Nil(t, restore.NeedSplit([]byte("d"), regions, isRawKv))
	// Out of region
	require.Nil(t, restore.NeedSplit([]byte("e"), regions, isRawKv))
}

func TestRegionConsistency(t *testing.T) {
	cases := []struct {
		startKey []byte
		endKey   []byte
		err      string
		regions  []*split.RegionInfo
	}{
		{
			codec.EncodeBytes([]byte{}, []byte("a")),
			codec.EncodeBytes([]byte{}, []byte("a")),
			"scan region return empty result, startKey: (.*?), endKey: (.*?)",
			[]*split.RegionInfo{},
		},
		{
			codec.EncodeBytes([]byte{}, []byte("a")),
			codec.EncodeBytes([]byte{}, []byte("a")),
			"first region's startKey > startKey, startKey: (.*?), regionStartKey: (.*?)",
			[]*split.RegionInfo{
				{
					Region: &metapb.Region{
						StartKey: codec.EncodeBytes([]byte{}, []byte("b")),
						EndKey:   codec.EncodeBytes([]byte{}, []byte("d")),
					},
				},
			},
		},
		{
			codec.EncodeBytes([]byte{}, []byte("b")),
			codec.EncodeBytes([]byte{}, []byte("e")),
			"last region's endKey < endKey, endKey: (.*?), regionEndKey: (.*?)",
			[]*split.RegionInfo{
				{
					Region: &metapb.Region{
						StartKey: codec.EncodeBytes([]byte{}, []byte("b")),
						EndKey:   codec.EncodeBytes([]byte{}, []byte("d")),
					},
				},
			},
		},
		{
			codec.EncodeBytes([]byte{}, []byte("c")),
			codec.EncodeBytes([]byte{}, []byte("e")),
			"region endKey not equal to next region startKey(.*?)",
			[]*split.RegionInfo{
				{
					Region: &metapb.Region{
						StartKey: codec.EncodeBytes([]byte{}, []byte("b")),
						EndKey:   codec.EncodeBytes([]byte{}, []byte("d")),
					},
				},
				{
					Region: &metapb.Region{
						StartKey: codec.EncodeBytes([]byte{}, []byte("e")),
						EndKey:   codec.EncodeBytes([]byte{}, []byte("f")),
					},
				},
			},
		},
	}
	for _, ca := range cases {
		err := split.CheckRegionConsistency(ca.startKey, ca.endKey, ca.regions)
		require.Error(t, err)
		require.Regexp(t, ca.err, err.Error())
	}
}

type fakeRestorer struct {
	mu sync.Mutex

	errorInSplit  bool
	splitRanges   []rtree.Range
	restoredFiles []*backuppb.File
}

func (f *fakeRestorer) SplitRanges(ctx context.Context, ranges []rtree.Range, rewriteRules *restore.RewriteRules, updateCh glue.Progress, isRawKv bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if ctx.Err() != nil {
		return ctx.Err()
	}
	f.splitRanges = append(f.splitRanges, ranges...)
	if f.errorInSplit {
		err := errors.Annotatef(berrors.ErrRestoreSplitFailed,
			"the key space takes many efforts and finally get together, how dare you split them again... :<")
		log.Error("error happens :3", logutil.ShortError(err))
		return err
	}
	return nil
}

func (f *fakeRestorer) RestoreSSTFiles(ctx context.Context, files []*backuppb.File, rewriteRules *restore.RewriteRules, updateCh glue.Progress) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if ctx.Err() != nil {
		return ctx.Err()
	}
	f.restoredFiles = append(f.restoredFiles, files...)
	err := errors.Annotatef(berrors.ErrRestoreWriteAndIngest, "the files to restore are taken by a hijacker, meow :3")
	log.Error("error happens :3", logutil.ShortError(err))
	return err
}

func fakeRanges(keys ...string) (r restore.DrainResult) {
	for i := range keys {
		if i+1 == len(keys) {
			return
		}
		r.Ranges = append(r.Ranges, rtree.Range{
			StartKey: []byte(keys[i]),
			EndKey:   []byte(keys[i+1]),
			Files:    []*backuppb.File{{Name: "fake.sst"}},
		})
	}
	return
}

type errorInTimeSink struct {
	ctx   context.Context
	errCh chan error
	t     *testing.T
}

func (e errorInTimeSink) EmitTables(tables ...restore.CreatedTable) {}

func (e errorInTimeSink) EmitError(err error) {
	e.errCh <- err
}

func (e errorInTimeSink) Close() {}

func (e errorInTimeSink) Wait() {
	select {
	case <-e.ctx.Done():
		e.t.Logf("The context is canceled but no error happen")
		e.t.FailNow()
	case <-e.errCh:
	}
}

func assertErrorEmitInTime(ctx context.Context, t *testing.T) errorInTimeSink {
	errCh := make(chan error, 1)
	return errorInTimeSink{
		ctx:   ctx,
		errCh: errCh,
		t:     t,
	}
}

func TestRestoreFailed(t *testing.T) {
	ranges := []restore.DrainResult{
		fakeRanges("aax", "abx", "abz"),
		fakeRanges("abz", "bbz", "bcy"),
		fakeRanges("bcy", "cad", "xxy"),
	}
	r := &fakeRestorer{}
	sender, err := restore.NewTiKVSender(context.TODO(), r, nil, 1)
	require.NoError(t, err)
	dctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	sink := assertErrorEmitInTime(dctx, t)
	sender.PutSink(sink)
	for _, r := range ranges {
		sender.RestoreBatch(r)
	}
	sink.Wait()
	sink.Close()
	sender.Close()
	require.GreaterOrEqual(t, len(r.restoredFiles), 1)
}

func TestSplitFailed(t *testing.T) {
	ranges := []restore.DrainResult{
		fakeRanges("aax", "abx", "abz"),
		fakeRanges("abz", "bbz", "bcy"),
		fakeRanges("bcy", "cad", "xxy"),
	}
	r := &fakeRestorer{errorInSplit: true}
	sender, err := restore.NewTiKVSender(context.TODO(), r, nil, 1)
	require.NoError(t, err)
	dctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	sink := assertErrorEmitInTime(dctx, t)
	sender.PutSink(sink)
	for _, r := range ranges {
		sender.RestoreBatch(r)
	}
	sink.Wait()
	sender.Close()
	require.GreaterOrEqual(t, len(r.splitRanges), 2)
	require.Len(t, r.restoredFiles, 0)
}

func keyWithTablePrefix(tableID int64, key string) []byte {
	rawKey := append(tablecodec.GenTableRecordPrefix(tableID), []byte(key)...)
	return codec.EncodeBytes([]byte{}, rawKey)
}

func TestSplitPoint(t *testing.T) {
	ctx := context.Background()
	var oldTableID int64 = 50
	var tableID int64 = 100
	rewriteRules := &restore.RewriteRules{
		Data: []*import_sstpb.RewriteRule{
			{
				OldKeyPrefix: tablecodec.EncodeTablePrefix(oldTableID),
				NewKeyPrefix: tablecodec.EncodeTablePrefix(tableID),
			},
		},
	}

	// range:     b   c d   e       g         i
	//            +---+ +---+       +---------+
	//          +-------------+----------+---------+
	// region:  a             f          h         j
	splitHelper := split.NewSplitHelper()
	splitHelper.Merge(split.Valued{Key: split.Span{StartKey: keyWithTablePrefix(oldTableID, "b"), EndKey: keyWithTablePrefix(oldTableID, "c")}, Value: split.Value{Size: 100, Number: 100}})
	splitHelper.Merge(split.Valued{Key: split.Span{StartKey: keyWithTablePrefix(oldTableID, "d"), EndKey: keyWithTablePrefix(oldTableID, "e")}, Value: split.Value{Size: 200, Number: 200}})
	splitHelper.Merge(split.Valued{Key: split.Span{StartKey: keyWithTablePrefix(oldTableID, "g"), EndKey: keyWithTablePrefix(oldTableID, "i")}, Value: split.Value{Size: 300, Number: 300}})
	client := NewFakeSplitClient()
	client.AppendRegion(keyWithTablePrefix(tableID, "a"), keyWithTablePrefix(tableID, "f"))
	client.AppendRegion(keyWithTablePrefix(tableID, "f"), keyWithTablePrefix(tableID, "h"))
	client.AppendRegion(keyWithTablePrefix(tableID, "h"), keyWithTablePrefix(tableID, "j"))
	client.AppendRegion(keyWithTablePrefix(tableID, "j"), keyWithTablePrefix(tableID+1, "a"))

	iter := restore.NewSplitHelperIteratorForTest(splitHelper, tableID, rewriteRules)
	err := restore.SplitPoint(ctx, iter, client, func(ctx context.Context, rs *restore.RegionSplitter, u uint64, o int64, ri *split.RegionInfo, v []split.Valued) error {
		require.Equal(t, u, uint64(0))
		require.Equal(t, o, int64(0))
		require.Equal(t, ri.Region.StartKey, keyWithTablePrefix(tableID, "a"))
		require.Equal(t, ri.Region.EndKey, keyWithTablePrefix(tableID, "f"))
		require.EqualValues(t, v[0].Key.StartKey, keyWithTablePrefix(tableID, "b"))
		require.EqualValues(t, v[0].Key.EndKey, keyWithTablePrefix(tableID, "c"))
		require.EqualValues(t, v[1].Key.StartKey, keyWithTablePrefix(tableID, "d"))
		require.EqualValues(t, v[1].Key.EndKey, keyWithTablePrefix(tableID, "e"))
		require.Equal(t, len(v), 2)
		return nil
	})
	require.NoError(t, err)
}

func getCharFromNumber(prefix string, i int) string {
	c := '1' + (i % 10)
	b := '1' + (i%100)/10
	a := '1' + i/100
	return fmt.Sprintf("%s%c%c%c", prefix, a, b, c)
}

func TestSplitPoint2(t *testing.T) {
	ctx := context.Background()
	var oldTableID int64 = 50
	var tableID int64 = 100
	rewriteRules := &restore.RewriteRules{
		Data: []*import_sstpb.RewriteRule{
			{
				OldKeyPrefix: tablecodec.EncodeTablePrefix(oldTableID),
				NewKeyPrefix: tablecodec.EncodeTablePrefix(tableID),
			},
		},
	}

	// range:     b   c d   e f                 i j    k l        n
	//            +---+ +---+ +-----------------+ +----+ +--------+
	//          +---------------+--+.....+----+------------+---------+
	// region:  a               g   >128      h            m         o
	splitHelper := split.NewSplitHelper()
	splitHelper.Merge(split.Valued{Key: split.Span{StartKey: keyWithTablePrefix(oldTableID, "b"), EndKey: keyWithTablePrefix(oldTableID, "c")}, Value: split.Value{Size: 100, Number: 100}})
	splitHelper.Merge(split.Valued{Key: split.Span{StartKey: keyWithTablePrefix(oldTableID, "d"), EndKey: keyWithTablePrefix(oldTableID, "e")}, Value: split.Value{Size: 200, Number: 200}})
	splitHelper.Merge(split.Valued{Key: split.Span{StartKey: keyWithTablePrefix(oldTableID, "f"), EndKey: keyWithTablePrefix(oldTableID, "i")}, Value: split.Value{Size: 300, Number: 300}})
	splitHelper.Merge(split.Valued{Key: split.Span{StartKey: keyWithTablePrefix(oldTableID, "j"), EndKey: keyWithTablePrefix(oldTableID, "k")}, Value: split.Value{Size: 200, Number: 200}})
	splitHelper.Merge(split.Valued{Key: split.Span{StartKey: keyWithTablePrefix(oldTableID, "l"), EndKey: keyWithTablePrefix(oldTableID, "n")}, Value: split.Value{Size: 200, Number: 200}})
	client := NewFakeSplitClient()
	client.AppendRegion(keyWithTablePrefix(tableID, "a"), keyWithTablePrefix(tableID, "g"))
	client.AppendRegion(keyWithTablePrefix(tableID, "g"), keyWithTablePrefix(tableID, getCharFromNumber("g", 0)))
	for i := 0; i < 256; i++ {
		client.AppendRegion(keyWithTablePrefix(tableID, getCharFromNumber("g", i)), keyWithTablePrefix(tableID, getCharFromNumber("g", i+1)))
	}
	client.AppendRegion(keyWithTablePrefix(tableID, getCharFromNumber("g", 256)), keyWithTablePrefix(tableID, "h"))
	client.AppendRegion(keyWithTablePrefix(tableID, "h"), keyWithTablePrefix(tableID, "m"))
	client.AppendRegion(keyWithTablePrefix(tableID, "m"), keyWithTablePrefix(tableID, "o"))
	client.AppendRegion(keyWithTablePrefix(tableID, "o"), keyWithTablePrefix(tableID+1, "a"))

	firstSplit := true
	iter := restore.NewSplitHelperIteratorForTest(splitHelper, tableID, rewriteRules)
	err := restore.SplitPoint(ctx, iter, client, func(ctx context.Context, rs *restore.RegionSplitter, u uint64, o int64, ri *split.RegionInfo, v []split.Valued) error {
		if firstSplit {
			require.Equal(t, u, uint64(0))
			require.Equal(t, o, int64(0))
			require.Equal(t, ri.Region.StartKey, keyWithTablePrefix(tableID, "a"))
			require.Equal(t, ri.Region.EndKey, keyWithTablePrefix(tableID, "g"))
			require.EqualValues(t, v[0].Key.StartKey, keyWithTablePrefix(tableID, "b"))
			require.EqualValues(t, v[0].Key.EndKey, keyWithTablePrefix(tableID, "c"))
			require.EqualValues(t, v[1].Key.StartKey, keyWithTablePrefix(tableID, "d"))
			require.EqualValues(t, v[1].Key.EndKey, keyWithTablePrefix(tableID, "e"))
			require.EqualValues(t, v[2].Key.StartKey, keyWithTablePrefix(tableID, "f"))
			require.EqualValues(t, v[2].Key.EndKey, keyWithTablePrefix(tableID, "g"))
			require.Equal(t, v[2].Value.Size, uint64(1))
			require.Equal(t, v[2].Value.Number, int64(1))
			require.Equal(t, len(v), 3)
			firstSplit = false
		} else {
			require.Equal(t, u, uint64(1))
			require.Equal(t, o, int64(1))
			require.Equal(t, ri.Region.StartKey, keyWithTablePrefix(tableID, "h"))
			require.Equal(t, ri.Region.EndKey, keyWithTablePrefix(tableID, "m"))
			require.EqualValues(t, v[0].Key.StartKey, keyWithTablePrefix(tableID, "j"))
			require.EqualValues(t, v[0].Key.EndKey, keyWithTablePrefix(tableID, "k"))
			require.EqualValues(t, v[1].Key.StartKey, keyWithTablePrefix(tableID, "l"))
			require.EqualValues(t, v[1].Key.EndKey, keyWithTablePrefix(tableID, "m"))
			require.Equal(t, v[1].Value.Size, uint64(100))
			require.Equal(t, v[1].Value.Number, int64(100))
			require.Equal(t, len(v), 2)
		}
		return nil
	})
	require.NoError(t, err)
}

type fakeSplitClient struct {
	regions []*split.RegionInfo
}

func NewFakeSplitClient() *fakeSplitClient {
	return &fakeSplitClient{
		regions: make([]*split.RegionInfo, 0),
	}
}

func (f *fakeSplitClient) AppendRegion(startKey, endKey []byte) {
	f.regions = append(f.regions, &split.RegionInfo{
		Region: &metapb.Region{
			StartKey: startKey,
			EndKey:   endKey,
		},
	})
}

func (*fakeSplitClient) GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error) {
	return nil, nil
}
func (*fakeSplitClient) GetRegion(ctx context.Context, key []byte) (*split.RegionInfo, error) {
	return nil, nil
}
func (*fakeSplitClient) GetRegionByID(ctx context.Context, regionID uint64) (*split.RegionInfo, error) {
	return nil, nil
}
func (*fakeSplitClient) SplitRegion(ctx context.Context, regionInfo *split.RegionInfo, key []byte) (*split.RegionInfo, error) {
	return nil, nil
}
func (*fakeSplitClient) BatchSplitRegions(ctx context.Context, regionInfo *split.RegionInfo, keys [][]byte) ([]*split.RegionInfo, error) {
	return nil, nil
}
func (*fakeSplitClient) BatchSplitRegionsWithOrigin(ctx context.Context, regionInfo *split.RegionInfo, keys [][]byte) (*split.RegionInfo, []*split.RegionInfo, error) {
	return nil, nil, nil
}
func (*fakeSplitClient) ScatterRegion(ctx context.Context, regionInfo *split.RegionInfo) error {
	return nil
}
func (*fakeSplitClient) ScatterRegions(ctx context.Context, regionInfo []*split.RegionInfo) error {
	return nil
}
func (*fakeSplitClient) GetOperator(ctx context.Context, regionID uint64) (*pdpb.GetOperatorResponse, error) {
	return nil, nil
}
func (f *fakeSplitClient) ScanRegions(ctx context.Context, startKey, endKey []byte, limit int) ([]*split.RegionInfo, error) {
	result := make([]*split.RegionInfo, 0)
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
func (*fakeSplitClient) GetPlacementRule(ctx context.Context, groupID, ruleID string) (pdtypes.Rule, error) {
	return pdtypes.Rule{}, nil
}
func (*fakeSplitClient) SetPlacementRule(ctx context.Context, rule pdtypes.Rule) error { return nil }
func (*fakeSplitClient) DeletePlacementRule(ctx context.Context, groupID, ruleID string) error {
	return nil
}
func (*fakeSplitClient) SetStoresLabel(ctx context.Context, stores []uint64, labelKey, labelValue string) error {
	return nil
}

func TestGetRewriteTableID(t *testing.T) {
	var tableID int64 = 76
	var oldTableID int64 = 80
	{
		rewriteRules := &restore.RewriteRules{
			Data: []*import_sstpb.RewriteRule{
				{
					OldKeyPrefix: tablecodec.EncodeTablePrefix(oldTableID),
					NewKeyPrefix: tablecodec.EncodeTablePrefix(tableID),
				},
			},
		}

		newTableID := restore.GetRewriteTableID(oldTableID, rewriteRules)
		require.Equal(t, tableID, newTableID)
	}

	{
		rewriteRules := &restore.RewriteRules{
			Data: []*import_sstpb.RewriteRule{
				{
					OldKeyPrefix: tablecodec.GenTableRecordPrefix(oldTableID),
					NewKeyPrefix: tablecodec.GenTableRecordPrefix(tableID),
				},
			},
		}

		newTableID := restore.GetRewriteTableID(oldTableID, rewriteRules)
		require.Equal(t, tableID, newTableID)
	}
}

type mockLogIter struct {
	next int
}

func (m *mockLogIter) TryNext(ctx context.Context) iter.IterResult[*backuppb.DataFileInfo] {
	if m.next > 10000 {
		return iter.Done[*backuppb.DataFileInfo]()
	}
	m.next += 1
	return iter.Emit(&backuppb.DataFileInfo{
		StartKey: []byte(fmt.Sprintf("a%d", m.next)),
		EndKey:   []byte("b"),
		Length:   1024, // 1 KB
	})
}

func TestLogFilesIterWithSplitHelper(t *testing.T) {
	var tableID int64 = 76
	var oldTableID int64 = 80
	rewriteRules := &restore.RewriteRules{
		Data: []*import_sstpb.RewriteRule{
			{
				OldKeyPrefix: tablecodec.EncodeTablePrefix(oldTableID),
				NewKeyPrefix: tablecodec.EncodeTablePrefix(tableID),
			},
		},
	}
	rewriteRulesMap := map[int64]*restore.RewriteRules{
		oldTableID: rewriteRules,
	}
	mockIter := &mockLogIter{}
	ctx := context.Background()
	logIter := restore.NewLogFilesIterWithSplitHelper(mockIter, rewriteRulesMap, NewFakeSplitClient(), 144*1024*1024, 1440000)
	next := 0
	for r := logIter.TryNext(ctx); !r.Finished; r = logIter.TryNext(ctx) {
		require.NoError(t, r.Err)
		next += 1
		require.Equal(t, []byte(fmt.Sprintf("a%d", next)), r.Item.StartKey)
	}
}

func regionInfo(startKey, endKey string) *split.RegionInfo {
	return &split.RegionInfo{
		Region: &metapb.Region{
			StartKey: []byte(startKey),
			EndKey:   []byte(endKey),
		},
	}
}

func TestSplitCheckPartRegionConsistency(t *testing.T) {
	var (
		startKey []byte = []byte("a")
		endKey   []byte = []byte("f")
		err      error
	)
	err = split.CheckPartRegionConsistency(startKey, endKey, nil)
	require.Error(t, err)
	err = split.CheckPartRegionConsistency(startKey, endKey, []*split.RegionInfo{
		regionInfo("b", "c"),
	})
	require.Error(t, err)
	err = split.CheckPartRegionConsistency(startKey, endKey, []*split.RegionInfo{
		regionInfo("a", "c"),
		regionInfo("d", "e"),
	})
	require.Error(t, err)
	err = split.CheckPartRegionConsistency(startKey, endKey, []*split.RegionInfo{
		regionInfo("a", "c"),
		regionInfo("c", "d"),
	})
	require.NoError(t, err)
	err = split.CheckPartRegionConsistency(startKey, endKey, []*split.RegionInfo{
		regionInfo("a", "c"),
		regionInfo("c", "d"),
		regionInfo("d", "f"),
	})
	require.NoError(t, err)
	err = split.CheckPartRegionConsistency(startKey, endKey, []*split.RegionInfo{
		regionInfo("a", "c"),
		regionInfo("c", "z"),
	})
	require.NoError(t, err)
}
