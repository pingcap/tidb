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
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	us "github.com/pingcap/tidb/store/mockstore/unistore/tikv"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
)

var _ pd.Client = new(pdClient)

type pdClient struct {
	*us.MockPD

	serviceSafePoints    map[string]uint64
	gcSafePointMu        sync.Mutex
	globalConfig         map[string]string
	externalTimestamp    atomic.Uint64
	resourceGroupManager struct {
		sync.RWMutex
		groups map[string]*rmpb.ResourceGroup
	}
}

func newPDClient(pd *us.MockPD) *pdClient {
	return &pdClient{
		MockPD:            pd,
		serviceSafePoints: make(map[string]uint64),
		globalConfig:      make(map[string]string),
		resourceGroupManager: struct {
			sync.RWMutex
			groups map[string]*rmpb.ResourceGroup
		}{
			groups: make(map[string]*rmpb.ResourceGroup),
		},
	}
}

func (c *pdClient) LoadGlobalConfig(ctx context.Context, names []string, configPath string) ([]pd.GlobalConfigItem, int64, error) {
	ret := make([]pd.GlobalConfigItem, len(names))
	for i, name := range names {
		if r, ok := c.globalConfig["/global/config/"+name]; ok {
			ret[i] = pd.GlobalConfigItem{Name: "/global/config/" + name, Value: r, EventType: pdpb.EventType_PUT}
		} else {
			ret[i] = pd.GlobalConfigItem{Name: "/global/config/" + name, Value: ""}
		}
	}
	return ret, 0, nil
}

func (c *pdClient) StoreGlobalConfig(ctx context.Context, configPath string, items []pd.GlobalConfigItem) error {
	for _, item := range items {
		c.globalConfig["/global/config/"+item.Name] = item.Value
	}
	return nil
}

func (c *pdClient) WatchGlobalConfig(ctx context.Context, configPath string, revision int64) (chan []pd.GlobalConfigItem, error) {
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

func (c *pdClient) SplitAndScatterRegions(ctx context.Context, splitKeys [][]byte, opts ...pd.RegionsOption) (*pdpb.SplitAndScatterRegionsResponse, error) {
	return nil, nil
}

func (c *pdClient) GetRegionFromMember(ctx context.Context, key []byte, memberURLs []string) (*pd.Region, error) {
	return nil, nil
}

func (c *pdClient) UpdateOption(option pd.DynamicOption, value interface{}) error {
	return nil
}

// LoadKeyspace loads and returns target keyspace's metadata.
func (c *pdClient) LoadKeyspace(ctx context.Context, name string) (*keyspacepb.KeyspaceMeta, error) {
	return nil, nil
}

// WatchKeyspaces watches keyspace meta changes.
// It returns a stream of slices of keyspace metadata.
// The first message in stream contains all current keyspaceMeta,
// all subsequent messages contains new put events for all keyspaces.
func (c *pdClient) WatchKeyspaces(ctx context.Context) (chan []*keyspacepb.KeyspaceMeta, error) {
	return nil, nil
}

func (c *pdClient) UpdateKeyspaceState(ctx context.Context, id uint32, state keyspacepb.KeyspaceState) (*keyspacepb.KeyspaceMeta, error) {
	return nil, nil
}

func (c *pdClient) ListResourceGroups(ctx context.Context) ([]*rmpb.ResourceGroup, error) {
	c.resourceGroupManager.RLock()
	defer c.resourceGroupManager.RUnlock()
	groups := make([]*rmpb.ResourceGroup, 0, len(c.resourceGroupManager.groups))
	for _, group := range c.resourceGroupManager.groups {
		groups = append(groups, group)
	}
	return groups, nil
}

func (c *pdClient) GetResourceGroup(ctx context.Context, name string) (*rmpb.ResourceGroup, error) {
	c.resourceGroupManager.RLock()
	defer c.resourceGroupManager.RUnlock()
	group, ok := c.resourceGroupManager.groups[name]
	if !ok {
		return nil, nil
	}
	return group, nil
}

func (c *pdClient) AddResourceGroup(ctx context.Context, group *rmpb.ResourceGroup) (string, error) {
	c.resourceGroupManager.Lock()
	defer c.resourceGroupManager.Unlock()
	c.resourceGroupManager.groups[group.Name] = group
	return "Success!", nil
}

func (c *pdClient) ModifyResourceGroup(ctx context.Context, group *rmpb.ResourceGroup) (string, error) {
	c.resourceGroupManager.Lock()
	defer c.resourceGroupManager.Unlock()
	c.resourceGroupManager.groups[group.Name] = group
	return "Success!", nil
}

func (c *pdClient) DeleteResourceGroup(ctx context.Context, name string) (string, error) {
	c.resourceGroupManager.Lock()
	defer c.resourceGroupManager.Unlock()
	delete(c.resourceGroupManager.groups, name)
	return "Success!", nil
}

func (c *pdClient) WatchResourceGroup(ctx context.Context, revision int64) (chan []*rmpb.ResourceGroup, error) {
	return nil, nil
}

func (c *pdClient) AcquireTokenBuckets(ctx context.Context, request *rmpb.TokenBucketsRequest) ([]*rmpb.TokenBucketResponse, error) {
	return nil, nil
}

func (c *pdClient) SetExternalTimestamp(ctx context.Context, newTimestamp uint64) error {
	p, l, err := c.GetTS(ctx)
	if err != nil {
		return err
	}

	currentTSO := oracle.ComposeTS(p, l)
	if newTimestamp > currentTSO {
		return errors.New("external timestamp is greater than global tso")
	}
	for {
		externalTimestamp := c.externalTimestamp.Load()
		if externalTimestamp > newTimestamp {
			return errors.New("cannot decrease the external timestamp")
		} else if externalTimestamp == newTimestamp {
			return nil
		}

		if c.externalTimestamp.CompareAndSwap(externalTimestamp, newTimestamp) {
			return nil
		}
	}
}

func (c *pdClient) GetExternalTimestamp(ctx context.Context) (uint64, error) {
	return c.externalTimestamp.Load(), nil
}
