// Copyright 2024 PingCAP, Inc.
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

package utiltest

import (
	"bytes"
	"context"
	"math"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/pdutil"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pkg/errors"
	pd "github.com/tikv/pd/client"
	pdhttp "github.com/tikv/pd/client/http"
	"google.golang.org/grpc/keepalive"
)

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
	regions []*pd.Region

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

func (fpdc *FakePDClient) SetRegions(regions []*pd.Region) {
	fpdc.regions = regions
}

func (fpdc *FakePDClient) GetAllStores(context.Context, ...pd.GetStoreOption) ([]*metapb.Store, error) {
	return append([]*metapb.Store{}, fpdc.stores...), nil
}

func (fpdc *FakePDClient) ScanRegions(
	ctx context.Context,
	key, endKey []byte,
	limit int,
	opts ...pd.GetRegionOption,
) ([]*pd.Region, error) {
	regions := make([]*pd.Region, 0, len(fpdc.regions))
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
	ranges []pd.KeyRange,
	limit int,
	opts ...pd.GetRegionOption,
) ([]*pd.Region, error) {
	regions := make([]*pd.Region, 0, len(fpdc.regions))
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
	split.SplitClient
	regions []*split.RegionInfo
}

func NewFakeSplitClient() *FakeSplitClient {
	return &FakeSplitClient{
		regions: make([]*split.RegionInfo, 0),
	}
}

func (f *FakeSplitClient) AppendRegion(startKey, endKey []byte) {
	f.regions = append(f.regions, &split.RegionInfo{
		Region: &metapb.Region{
			StartKey: startKey,
			EndKey:   endKey,
		},
	})
}

func (f *FakeSplitClient) AppendPdRegion(region *pd.Region) {
	f.regions = append(f.regions, &split.RegionInfo{
		Region: region.Meta,
		Leader: region.Leader,
	})
}

func (f *FakeSplitClient) ScanRegions(
	ctx context.Context,
	startKey, endKey []byte,
	limit int,
) ([]*split.RegionInfo, error) {
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

func (f *FakeSplitClient) WaitRegionsScattered(context.Context, []*split.RegionInfo) (int, error) {
	return 0, nil
}
