// Copyright 2022 PingCAP, Inc.
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

package infosync

import (
	"context"
	"fmt"
	"math"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/pingcap/kvproto/pkg/meta_storagepb"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/tidb/domain/resourcegroup"
	pd "github.com/tikv/pd/client"
)

type mockResourceManagerClient struct {
	sync.RWMutex
	groups  map[string]*rmpb.ResourceGroup
	eventCh chan []*meta_storagepb.Event
}

// NewMockResourceManagerClient return a mock ResourceManagerClient for test usage.
func NewMockResourceManagerClient() pd.ResourceManagerClient {
	mockMgr := &mockResourceManagerClient{
		groups:  make(map[string]*rmpb.ResourceGroup),
		eventCh: make(chan []*meta_storagepb.Event, 100),
	}
	mockMgr.groups[resourcegroup.DefaultResourceGroupName] = &rmpb.ResourceGroup{
		Name: resourcegroup.DefaultResourceGroupName,
		Mode: rmpb.GroupMode_RUMode,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{
					FillRate:   math.MaxInt32,
					BurstLimit: -1,
				},
			},
		},
		Priority: 8,
	}
	return mockMgr
}

var _ pd.ResourceManagerClient = (*mockResourceManagerClient)(nil)

func (m *mockResourceManagerClient) ListResourceGroups(ctx context.Context) ([]*rmpb.ResourceGroup, error) {
	m.RLock()
	defer m.RUnlock()
	groups := make([]*rmpb.ResourceGroup, 0, len(m.groups))
	for _, group := range m.groups {
		groups = append(groups, group)
	}
	return groups, nil
}

func (m *mockResourceManagerClient) GetResourceGroup(ctx context.Context, name string) (*rmpb.ResourceGroup, error) {
	m.RLock()
	defer m.RUnlock()
	group, ok := m.groups[name]
	if !ok {
		return nil, fmt.Errorf("the group %s does not exist", name)
	}
	return group, nil
}

func (m *mockResourceManagerClient) AddResourceGroup(ctx context.Context, group *rmpb.ResourceGroup) (string, error) {
	m.Lock()
	defer m.Unlock()
	if _, ok := m.groups[group.Name]; ok {
		return "", fmt.Errorf("the group %s already exists", group.Name)
	}
	m.groups[group.Name] = group
	value, err := proto.Marshal(group)
	if err != nil {
		return "", err
	}
	m.eventCh <- []*meta_storagepb.Event{{
		Type: meta_storagepb.Event_PUT,
		Kv: &meta_storagepb.KeyValue{
			Value: value,
		}}}

	return "Success!", nil
}

func (m *mockResourceManagerClient) ModifyResourceGroup(ctx context.Context, group *rmpb.ResourceGroup) (string, error) {
	m.Lock()
	defer m.Unlock()

	m.groups[group.Name] = group
	value, err := proto.Marshal(group)
	if err != nil {
		return "", err
	}
	m.eventCh <- []*meta_storagepb.Event{{
		Type: meta_storagepb.Event_PUT,
		Kv: &meta_storagepb.KeyValue{
			Value: value,
		}}}
	return "Success!", nil
}

func (m *mockResourceManagerClient) DeleteResourceGroup(ctx context.Context, name string) (string, error) {
	m.Lock()
	defer m.Unlock()
	group := m.groups[name]
	delete(m.groups, name)
	value, err := proto.Marshal(group)
	if err != nil {
		return "", err
	}
	m.eventCh <- []*meta_storagepb.Event{{
		Type: meta_storagepb.Event_DELETE,
		Kv: &meta_storagepb.KeyValue{
			Value: value,
		}}}
	return "Success!", nil
}

func (m *mockResourceManagerClient) AcquireTokenBuckets(ctx context.Context, request *rmpb.TokenBucketsRequest) ([]*rmpb.TokenBucketResponse, error) {
	return nil, nil
}

func (m *mockResourceManagerClient) WatchResourceGroup(ctx context.Context, revision int64) (chan []*rmpb.ResourceGroup, error) {
	return nil, nil
}

func (m *mockResourceManagerClient) LoadResourceGroups(ctx context.Context) ([]*rmpb.ResourceGroup, int64, error) {
	return nil, 0, nil
}

func (m *mockResourceManagerClient) Watch(ctx context.Context, key []byte, opts ...pd.OpOption) (chan []*meta_storagepb.Event, error) {
	return m.eventCh, nil
}
