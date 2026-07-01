// Copyright 2026 PingCAP, Inc.
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

package domain

import (
	"context"
	"errors"
	"testing"

	"github.com/pingcap/kvproto/pkg/meta_storagepb"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/tidb/pkg/resourcegroup/runaway"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/opt"
	rmclient "github.com/tikv/pd/client/resource_group/controller"
)

type resourceGroupProviderStub struct {
	resourceGroup *rmpb.ResourceGroup
	resourceErr   error
}

func (s *resourceGroupProviderStub) GetResourceGroup(context.Context, string, ...pd.GetResourceGroupOption) (*rmpb.ResourceGroup, error) {
	return s.resourceGroup, s.resourceErr
}

func (s *resourceGroupProviderStub) ListResourceGroups(context.Context, ...pd.GetResourceGroupOption) ([]*rmpb.ResourceGroup, error) {
	return nil, nil
}

func (s *resourceGroupProviderStub) AddResourceGroup(context.Context, *rmpb.ResourceGroup) (string, error) {
	return "", nil
}

func (s *resourceGroupProviderStub) ModifyResourceGroup(context.Context, *rmpb.ResourceGroup) (string, error) {
	return "", nil
}

func (s *resourceGroupProviderStub) DeleteResourceGroup(context.Context, string) (string, error) {
	return "", nil
}

func (s *resourceGroupProviderStub) AcquireTokenBuckets(context.Context, *rmpb.TokenBucketsRequest) ([]*rmpb.TokenBucketResponse, error) {
	return nil, nil
}

func (s *resourceGroupProviderStub) LoadResourceGroups(context.Context) ([]*rmpb.ResourceGroup, int64, error) {
	return nil, 0, nil
}

func (s *resourceGroupProviderStub) Watch(context.Context, []byte, ...opt.MetaStorageOption) (chan []*meta_storagepb.Event, error) {
	return make(chan []*meta_storagepb.Event), nil
}

func (s *resourceGroupProviderStub) Get(context.Context, []byte, ...opt.MetaStorageOption) (*meta_storagepb.GetResponse, error) {
	return &meta_storagepb.GetResponse{Header: &meta_storagepb.ResponseHeader{}}, nil
}

func (s *resourceGroupProviderStub) Put(context.Context, []byte, []byte, ...opt.MetaStorageOption) (*meta_storagepb.PutResponse, error) {
	return &meta_storagepb.PutResponse{Header: &meta_storagepb.ResponseHeader{}}, nil
}

func TestResourceGroupsControllerOptionsProvideDegradedFallback(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	provider := &resourceGroupProviderStub{
		resourceGroup: &rmpb.ResourceGroup{
			Name: "test-group",
			Mode: rmpb.GroupMode_RUMode,
			RUSettings: &rmpb.GroupRequestUnitSettings{
				RU: &rmpb.TokenBucket{
					Settings: &rmpb.TokenLimitSettings{FillRate: 1},
				},
			},
		},
		resourceErr: errors.New("resource group unavailable"),
	}

	controllerWithFallback, err := rmclient.NewResourceGroupController(
		ctx,
		1,
		provider,
		nil,
		0,
		newResourceGroupsControllerOptions()...,
	)
	require.NoError(t, err)
	controllerWithFallback.Start(ctx)
	defer func() {
		require.NoError(t, controllerWithFallback.Stop())
	}()

	group, err := controllerWithFallback.GetResourceGroup("test-group")
	require.NoError(t, err)
	require.Equal(t, &rmpb.ResourceGroup{
		Name:       "test-group",
		Mode:       rmpb.GroupMode_RUMode,
		RUSettings: newDefaultDegradedRUSettings(),
	}, group)

	controllerWithoutFallback, err := rmclient.NewResourceGroupController(
		ctx,
		2,
		provider,
		nil,
		0,
		rmclient.WithMaxWaitDuration(runaway.MaxWaitDuration),
	)
	require.NoError(t, err)
	controllerWithoutFallback.Start(ctx)
	defer func() {
		require.NoError(t, controllerWithoutFallback.Stop())
	}()

	group, err = controllerWithoutFallback.GetResourceGroup("test-group")
	require.Error(t, err)
	require.Nil(t, group)
}
