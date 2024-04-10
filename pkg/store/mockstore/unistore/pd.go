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
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/asaskevich/govalidator"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/meta_storagepb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	us "github.com/pingcap/tidb/pkg/store/mockstore/unistore/tikv"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
	"google.golang.org/grpc"
)

var _ pd.Client = new(pdClient)

type pdClient struct {
	*us.MockPD
	pd.ResourceManagerClient

	serviceSafePoints map[string]uint64
	gcSafePointMu     sync.Mutex
	globalConfig      map[string]string
	externalTimestamp atomic.Uint64

	// After using PD http client, we should impl mock PD service discovery
	// which needs PD server HTTP address.
	addrs []string
}

func newPDClient(pd *us.MockPD, addrs []string) *pdClient {
	return &pdClient{
		MockPD:                pd,
		ResourceManagerClient: infosync.NewMockResourceManagerClient(),
		serviceSafePoints:     make(map[string]uint64),
		globalConfig:          make(map[string]string),
		addrs:                 addrs,
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

func (c *pdClient) GetServiceDiscovery() pd.ServiceDiscovery {
	return NewMockPDServiceDiscovery(c.addrs)
}

var (
	_ pd.ServiceDiscovery = (*mockPDServiceDiscovery)(nil)
	_ pd.ServiceClient    = (*mockPDServiceClient)(nil)
)

type mockPDServiceClient struct {
	addr string
}

func newMockPDServiceClient(addr string) pd.ServiceClient {
	if !strings.HasPrefix(addr, "http") {
		addr = fmt.Sprintf("%s://%s", "http", addr)
	}
	return &mockPDServiceClient{addr: addr}
}

func (c *mockPDServiceClient) GetAddress() string {
	return c.addr
}

func (c *mockPDServiceClient) GetURL() string {
	return c.addr
}

func (c *mockPDServiceClient) GetClientConn() *grpc.ClientConn {
	return nil
}

func (c *mockPDServiceClient) BuildGRPCTargetContext(ctx context.Context, _ bool) context.Context {
	return ctx
}

func (c *mockPDServiceClient) Available() bool {
	return true
}

func (c *mockPDServiceClient) NeedRetry(*pdpb.Error, error) bool {
	return false
}

func (c *mockPDServiceClient) IsConnectedToLeader() bool {
	return true
}

type mockPDServiceDiscovery struct {
	addrs []string
	clis  []pd.ServiceClient
}

// NewMockPDServiceDiscovery returns a mock PD ServiceDiscovery
func NewMockPDServiceDiscovery(addrs []string) pd.ServiceDiscovery {
	addresses := make([]string, 0)
	clis := make([]pd.ServiceClient, 0)
	for _, addr := range addrs {
		if check := govalidator.IsURL(addr); !check {
			continue
		}
		addresses = append(addresses, addr)
		clis = append(clis, newMockPDServiceClient(addr))
	}
	return &mockPDServiceDiscovery{addrs: addresses, clis: clis}
}

func (c *mockPDServiceDiscovery) Init() error {
	return nil
}

func (c *mockPDServiceDiscovery) Close() {}

func (c *mockPDServiceDiscovery) GetClusterID() uint64 { return 0 }

func (c *mockPDServiceDiscovery) GetKeyspaceID() uint32 { return 0 }

func (c *mockPDServiceDiscovery) GetKeyspaceGroupID() uint32 { return 0 }

func (c *mockPDServiceDiscovery) GetServiceURLs() []string {
	return c.addrs
}

func (c *mockPDServiceDiscovery) GetServingEndpointClientConn() *grpc.ClientConn { return nil }

func (c *mockPDServiceDiscovery) GetClientConns() *sync.Map { return nil }

func (c *mockPDServiceDiscovery) GetServingURL() string { return "" }

func (c *mockPDServiceDiscovery) GetBackupURLs() []string { return nil }

func (c *mockPDServiceDiscovery) GetServiceClient() pd.ServiceClient {
	if len(c.clis) > 0 {
		return c.clis[0]
	}
	return nil
}

func (c *mockPDServiceDiscovery) GetAllServiceClients() []pd.ServiceClient {
	return c.clis
}

func (c *mockPDServiceDiscovery) GetOrCreateGRPCConn(addr string) (*grpc.ClientConn, error) {
	return nil, nil
}

func (c *mockPDServiceDiscovery) ScheduleCheckMemberChanged() {}

func (c *mockPDServiceDiscovery) CheckMemberChanged() error { return nil }

func (c *mockPDServiceDiscovery) AddServingAddrSwitchedCallback(callbacks ...func()) {}

func (c *mockPDServiceDiscovery) AddServiceAddrsSwitchedCallback(callbacks ...func()) {}

func (c *mockPDServiceDiscovery) AddServingURLSwitchedCallback(callbacks ...func()) {}

func (c *mockPDServiceDiscovery) AddServiceURLsSwitchedCallback(callbacks ...func()) {}

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

func (c *pdClient) GetLeaderURL() string { return "mockpd" }

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

func (c *pdClient) GetRegionFromMember(ctx context.Context, key []byte, memberURLs []string, opts ...pd.GetRegionOption) (*pd.Region, error) {
	return nil, nil
}

func (c *pdClient) UpdateOption(option pd.DynamicOption, value any) error {
	return nil
}

func (c *pdClient) GetAllKeyspaces(ctx context.Context, startID uint32, limit uint32) ([]*keyspacepb.KeyspaceMeta, error) {
	return nil, nil
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

func (c *pdClient) GetTSWithinKeyspace(ctx context.Context, keyspaceID uint32) (int64, int64, error) {
	return 0, 0, nil
}

func (c *pdClient) GetTSWithinKeyspaceAsync(ctx context.Context, keyspaceID uint32) pd.TSFuture {
	return nil
}

func (c *pdClient) GetLocalTSWithinKeyspace(ctx context.Context, dcLocation string, keyspaceID uint32) (int64, int64, error) {
	return 0, 0, nil
}

func (c *pdClient) GetLocalTSWithinKeyspaceAsync(ctx context.Context, dcLocation string, keyspaceID uint32) pd.TSFuture {
	return nil
}

func (c *pdClient) Get(ctx context.Context, key []byte, opts ...pd.OpOption) (*meta_storagepb.GetResponse, error) {
	return nil, nil
}

func (c *pdClient) Put(ctx context.Context, key []byte, value []byte, opts ...pd.OpOption) (*meta_storagepb.PutResponse, error) {
	return nil, nil
}

func (c *pdClient) GetMinTS(ctx context.Context) (int64, int64, error) {
	return 0, 0, nil
}

func (c *pdClient) LoadResourceGroups(ctx context.Context) ([]*rmpb.ResourceGroup, int64, error) {
	return nil, 0, nil
}

func (c *pdClient) UpdateGCSafePointV2(ctx context.Context, keyspaceID uint32, safePoint uint64) (uint64, error) {
	panic("unimplemented")
}

func (c *pdClient) UpdateServiceSafePointV2(ctx context.Context, keyspaceID uint32, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
	panic("unimplemented")
}

func (c *pdClient) WatchGCSafePointV2(ctx context.Context, revision int64) (chan []*pdpb.SafePointEvent, error) {
	panic("unimplemented")
}
