// Copyright 2020 PingCAP, Inc.
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

package unistore

import (
	"errors"
	"math"
	"sync"

	"github.com/pingcap/kvproto/pkg/pdpb"
	us "github.com/pingcap/tidb/store/mockstore/unistore/tikv"
	pd "github.com/tikv/pd/client"
	"golang.org/x/net/context"
)

var _ pd.Client = new(pdClient)

type pdClient struct {
	*us.MockPD

	serviceSafePoints map[string]uint64
	gcSafePointMu     sync.Mutex
	globalConfig      map[string]string
}

func newPDClient(pd *us.MockPD) *pdClient {
	return &pdClient{
		MockPD:            pd,
		serviceSafePoints: make(map[string]uint64),
		globalConfig:      make(map[string]string),
	}
}

func (c *pdClient) LoadGlobalConfig(ctx context.Context, names []string) ([]pd.GlobalConfigItem, error) {
	ret := make([]pd.GlobalConfigItem, len(names))
	for i, name := range names {
		if r, ok := c.globalConfig["/global/config/"+name]; ok {
			ret[i] = pd.GlobalConfigItem{Name: "/global/config/" + name, Value: r}
		} else {
			ret[i] = pd.GlobalConfigItem{Name: "/global/config/" + name, Error: errors.New("not found")}
		}
	}
	return ret, nil
}

func (c *pdClient) StoreGlobalConfig(ctx context.Context, items []pd.GlobalConfigItem) error {
	for _, item := range items {
		c.globalConfig["/global/config/"+item.Name] = item.Value
	}
	return nil
}

func (c *pdClient) WatchGlobalConfig(ctx context.Context) (chan []pd.GlobalConfigItem, error) {
	globalConfigWatcherCh := make(chan []pd.GlobalConfigItem, 16)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				return
			}
		}()
		for i := 0; i < 10; i++ {
			for k, v := range c.globalConfig {
				globalConfigWatcherCh <- []pd.GlobalConfigItem{{Name: k, Value: v}}
			}
		}
	}()
	return globalConfigWatcherCh, nil
}

func (c *pdClient) GetLocalTS(ctx context.Context, dcLocation string) (int64, int64, error) {
	return c.GetTS(ctx)
}

func (c *pdClient) GetTSAsync(ctx context.Context) pd.TSFuture {
	return &mockTSFuture{c, ctx, false}
}

func (c *pdClient) GetLocalTSAsync(ctx context.Context, dcLocation string) pd.TSFuture {
	return &mockTSFuture{c, ctx, false}
}

type mockTSFuture struct {
	pdc  *pdClient
	ctx  context.Context
	used bool
}

func (m *mockTSFuture) Wait() (int64, int64, error) {
	if m.used {
		return 0, 0, errors.New("cannot wait tso twice")
	}
	m.used = true
	return m.pdc.GetTS(m.ctx)
}

func (c *pdClient) GetLeaderAddr() string { return "mockpd" }

func (c *pdClient) UpdateServiceGCSafePoint(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
	c.gcSafePointMu.Lock()
	defer c.gcSafePointMu.Unlock()

	if ttl == 0 {
		delete(c.serviceSafePoints, serviceID)
	} else {
		var minSafePoint uint64 = math.MaxUint64
		for _, ssp := range c.serviceSafePoints {
			if ssp < minSafePoint {
				minSafePoint = ssp
			}
		}

		if len(c.serviceSafePoints) == 0 || minSafePoint <= safePoint {
			c.serviceSafePoints[serviceID] = safePoint
		}
	}

	// The minSafePoint may have changed. Reload it.
	var minSafePoint uint64 = math.MaxUint64
	for _, ssp := range c.serviceSafePoints {
		if ssp < minSafePoint {
			minSafePoint = ssp
		}
	}
	return minSafePoint, nil
}

func (c *pdClient) GetOperator(ctx context.Context, regionID uint64) (*pdpb.GetOperatorResponse, error) {
	return &pdpb.GetOperatorResponse{Status: pdpb.OperatorStatus_SUCCESS}, nil
}

func (c *pdClient) GetAllMembers(ctx context.Context) ([]*pdpb.Member, error) {
	return nil, nil
}

func (c *pdClient) ScatterRegions(ctx context.Context, regionsID []uint64, opts ...pd.RegionsOption) (*pdpb.ScatterRegionResponse, error) {
	return nil, nil
}

func (c *pdClient) SplitRegions(ctx context.Context, splitKeys [][]byte, opts ...pd.RegionsOption) (*pdpb.SplitRegionsResponse, error) {
	return nil, nil
}

func (c *pdClient) GetRegionFromMember(ctx context.Context, key []byte, memberURLs []string) (*pd.Region, error) {
	return nil, nil
}

func (c *pdClient) UpdateOption(option pd.DynamicOption, value interface{}) error {
	return nil
}
