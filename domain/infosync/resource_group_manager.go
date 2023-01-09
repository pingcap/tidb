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
	"sync"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/log"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// ResourceGroupManager manages resource group settings
type ResourceGroupManager interface {
	// GetResourceGroup is used to get one specific rule bundle from ResourceGroup Manager.
	GetResourceGroup(ctx context.Context, name string) (*rmpb.ResourceGroup, error)
	// GetAllResourceGroups is used to get all rule bundles from ResourceGroup Manager.
	GetAllResourceGroups(ctx context.Context) ([]*rmpb.ResourceGroup, error)
	// PutResourceGroup is used to post specific rule bundles to ResourceGroup Manager.
	CreateResourceGroup(ctx context.Context, group *rmpb.ResourceGroup) error
	// ModifyResourceGroup is used to modify specific rule bundles to ResourceGroup Manager.
	ModifyResourceGroup(ctx context.Context, group *rmpb.ResourceGroup) error
	// DeleteResourceGroup is used to delete specific rule bundles to ResourceGroup Manager.
	DeleteResourceGroup(ctx context.Context, name string) error
}

// externalResourceGroupManager manages placement with resource manager.
// TODO: replace with resource manager client.
type externalResourceGroupManager struct {
	etcdCli *clientv3.Client
}

// NewResourceManager is used to create a new resource manager in client side.
func NewResourceManager(etcdCli *clientv3.Client) ResourceGroupManager {
	return &externalResourceGroupManager{etcdCli: etcdCli}
}

// GetResourceGroupClient is used to get resource group client.
func (m *externalResourceGroupManager) GetResourceGroupClient() rmpb.ResourceManagerClient {
	conn := m.etcdCli.ActiveConnection()
	return rmpb.NewResourceManagerClient(conn)
}

// GetResourceGroup is used to get one specific rule bundle from ResourceGroup Manager.
func (m *externalResourceGroupManager) GetResourceGroup(ctx context.Context, name string) (*rmpb.ResourceGroup, error) {
	group := &rmpb.GetResourceGroupRequest{ResourceGroupName: name}
	resp, err := m.GetResourceGroupClient().GetResourceGroup(ctx, group)
	if err != nil {
		return nil, err
	}
	return resp.GetGroup(), nil
}

// GetAllResourceGroups is used to get all resource group from ResourceGroup Manager. It is used to load full resource groups from PD while fullload infoschema.
func (m *externalResourceGroupManager) GetAllResourceGroups(ctx context.Context) ([]*rmpb.ResourceGroup, error) {
	req := &rmpb.ListResourceGroupsRequest{}
	resp, err := m.GetResourceGroupClient().ListResourceGroups(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp.GetGroups(), nil
}

// CreateResourceGroup is used to post specific resource group to ResourceGroup Manager.
func (m *externalResourceGroupManager) CreateResourceGroup(ctx context.Context, group *rmpb.ResourceGroup) error {
	req := &rmpb.PutResourceGroupRequest{Group: group}
	_, err := m.GetResourceGroupClient().AddResourceGroup(ctx, req)
	return err
}

// ModifyResourceGroup is used to modify specific resource group to ResourceGroup Manager.
func (m *externalResourceGroupManager) ModifyResourceGroup(ctx context.Context, group *rmpb.ResourceGroup) error {
	req := &rmpb.PutResourceGroupRequest{Group: group}
	_, err := m.GetResourceGroupClient().ModifyResourceGroup(ctx, req)
	return err
}

// DeleteResourceGroup is used to delete specific resource group to ResourceGroup Manager.
func (m *externalResourceGroupManager) DeleteResourceGroup(ctx context.Context, name string) error {
	req := &rmpb.DeleteResourceGroupRequest{ResourceGroupName: name}
	log.Info("delete resource group", zap.String("name", name))
	_, err := m.GetResourceGroupClient().DeleteResourceGroup(ctx, req)
	return err
}

type mockResourceGroupManager struct {
	sync.Mutex
	groups map[string]*rmpb.ResourceGroup
}

func (m *mockResourceGroupManager) GetResourceGroup(ctx context.Context, name string) (*rmpb.ResourceGroup, error) {
	m.Lock()
	defer m.Unlock()
	group, ok := m.groups[name]
	if !ok {
		return nil, nil
	}
	return group, nil
}

func (m *mockResourceGroupManager) GetAllResourceGroups(ctx context.Context) ([]*rmpb.ResourceGroup, error) {
	m.Lock()
	defer m.Unlock()
	groups := make([]*rmpb.ResourceGroup, 0, len(m.groups))
	for _, group := range m.groups {
		groups = append(groups, group)
	}
	return groups, nil
}

func (m *mockResourceGroupManager) CreateResourceGroup(ctx context.Context, group *rmpb.ResourceGroup) error {
	m.Lock()
	defer m.Unlock()
	m.groups[group.Name] = group
	return nil
}

func (m *mockResourceGroupManager) ModifyResourceGroup(ctx context.Context, group *rmpb.ResourceGroup) error {
	m.Lock()
	defer m.Unlock()
	m.groups[group.Name] = group
	return nil
}

func (m *mockResourceGroupManager) DeleteResourceGroup(ctx context.Context, name string) error {
	m.Lock()
	defer m.Unlock()
	delete(m.groups, name)
	return nil
}
