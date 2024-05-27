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
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pkg/errors"
	pd "github.com/tikv/pd/client"
	"google.golang.org/grpc/keepalive"
)

var DefaultTestKeepaliveCfg = keepalive.ClientParameters{
	Time:    3 * time.Second,
	Timeout: 10 * time.Second,
}

type FakePDClient struct {
	pd.Client
	stores []*metapb.Store

	notLeader  bool
	retryTimes *int
}

func NewFakePDClient(stores []*metapb.Store, notLeader bool, retryTime *int) FakePDClient {
	var retryTimeInternal int
	if retryTime == nil {
		retryTime = &retryTimeInternal
	}
	return FakePDClient{
		stores: stores,

		notLeader:  notLeader,
		retryTimes: retryTime,
	}
}

func (fpdc FakePDClient) GetAllStores(context.Context, ...pd.GetStoreOption) ([]*metapb.Store, error) {
	return append([]*metapb.Store{}, fpdc.stores...), nil
}

func (fpdc FakePDClient) GetTS(ctx context.Context) (int64, int64, error) {
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
