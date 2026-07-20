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
	"encoding/json"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/meta_storagepb"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/deploymode"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/resourcegroup/runaway"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikvrpc"
	pd "github.com/tikv/pd/client"
	pderr "github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/opt"
	rmclient "github.com/tikv/pd/client/resource_group/controller"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type resourceGroupProviderStub struct {
	rmclient.ResourceGroupProvider
	resourceGroup    *rmpb.ResourceGroup
	resourceErr      error
	controllerConfig *rmclient.Config
}

func newResourceGroupProviderStub(t *testing.T, resourceGroup *rmpb.ResourceGroup, resourceErr error) *resourceGroupProviderStub {
	t.Helper()
	baseProvider, ok := infosync.NewMockResourceManagerClient(0).(rmclient.ResourceGroupProvider)
	require.True(t, ok)
	return &resourceGroupProviderStub{
		ResourceGroupProvider: baseProvider,
		resourceGroup:         resourceGroup,
		resourceErr:           resourceErr,
	}
}

// GetResourceGroup returns both the mocked resource group and the mocked error.
// This lets the test verify whether the controller uses the degraded fallback
// only for the editions that enable it.
func (s *resourceGroupProviderStub) GetResourceGroup(context.Context, string, ...pd.GetResourceGroupOption) (*rmpb.ResourceGroup, error) {
	return s.resourceGroup, s.resourceErr
}

func (s *resourceGroupProviderStub) Get(ctx context.Context, key []byte, opts ...opt.MetaStorageOption) (*meta_storagepb.GetResponse, error) {
	if s.controllerConfig == nil {
		return s.ResourceGroupProvider.Get(ctx, key, opts...)
	}
	value, err := json.Marshal(s.controllerConfig)
	if err != nil {
		return nil, err
	}
	return &meta_storagepb.GetResponse{
		Kvs: []*meta_storagepb.KeyValue{{
			Key:   key,
			Value: value,
		}},
	}, nil
}

func newStarterControllerForTest(t *testing.T, provider rmclient.ResourceGroupProvider) *rmclient.ResourceGroupsController {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	require.NoError(t, deploymode.Set(deploymode.Starter))
	controller, err := rmclient.NewResourceGroupController(
		ctx,
		1,
		provider,
		nil,
		0,
		newResourceGroupsControllerOptions()...,
	)
	require.NoError(t, err)
	controller.Start(ctx)
	t.Cleanup(func() {
		require.NoError(t, controller.Stop())
	})
	return controller
}

func requireDegradedResourceGroup(t *testing.T, group *rmpb.ResourceGroup, name string) {
	t.Helper()
	require.NotNil(t, group)
	require.Equal(t, name, group.Name)
	require.Equal(t, rmpb.GroupMode_RUMode, group.Mode)
	require.NotNil(t, group.RUSettings)
	require.NotNil(t, group.RUSettings.RU)
	require.NotNil(t, group.RUSettings.RU.Settings)
	require.EqualValues(t, defaultDegradedRUFillRate, group.RUSettings.RU.Settings.FillRate)
	require.EqualValues(t, defaultDegradedRUBurstLimit, group.RUSettings.RU.Settings.BurstLimit)
}

func newTransientGetResourceGroupErr(name string) error {
	err := status.Error(codes.Unavailable, "resource manager unavailable")
	return &pderr.ErrClientGetResourceGroup{
		ResourceGroupName: name,
		Cause:             err.Error(),
		Err:               err,
	}
}

func TestResourceGroupsControllerOptionsProvideDegradedFallback(t *testing.T) {
	restoreConfig := config.RestoreFunc()
	t.Cleanup(restoreConfig)
	if kerneltype.IsNextGen() {
		// Preserve the process-wide deploy mode because deploymode.IsStarter reads
		// it directly when newResourceGroupsControllerOptions builds controller options.
		originalDeployMode := deploymode.Get()
		t.Cleanup(func() {
			require.NoError(t, deploymode.Set(originalDeployMode))
		})
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	provider := newResourceGroupProviderStub(t, &rmpb.ResourceGroup{
		Name: "test-group",
		Mode: rmpb.GroupMode_RUMode,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{FillRate: 1},
			},
		},
	}, newTransientGetResourceGroupErr("test-group"))

	if !kerneltype.IsNextGen() {
		controller, err := rmclient.NewResourceGroupController(
			ctx,
			1,
			provider,
			nil,
			0,
			newResourceGroupsControllerOptions()...,
		)
		require.NoError(t, err)
		controller.Start(ctx)
		defer func() {
			require.NoError(t, controller.Stop())
		}()

		group, err := controller.GetResourceGroup("test-group")
		require.Error(t, err)
		require.Nil(t, group)
		return
	}

	config.UpdateGlobal(func(conf *config.Config) {
		conf.StarterParams.EnableGetResourceGroupDegraded = true
	})
	controllerWithFallback := newStarterControllerForTest(t, provider)
	group, err := controllerWithFallback.GetResourceGroup("test-group")
	require.NoError(t, err)
	requireDegradedResourceGroup(t, group, "test-group")

	// The degraded group should not be cached. Once the provider recovers, the
	// next lookup should observe the real resource-group metadata.
	provider.resourceGroup = &rmpb.ResourceGroup{
		Name: "test-group",
		Mode: rmpb.GroupMode_RUMode,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{FillRate: 1},
			},
		},
	}
	provider.resourceErr = nil
	group, err = controllerWithFallback.GetResourceGroup("test-group")
	require.NoError(t, err)
	require.Equal(t, provider.resourceGroup, group)

	require.NoError(t, deploymode.Set(deploymode.Premium))
	provider = newResourceGroupProviderStub(t, nil, newTransientGetResourceGroupErr("test-group"))
	controllerWithoutFallback, err := rmclient.NewResourceGroupController(
		ctx,
		2,
		provider,
		nil,
		0,
		newResourceGroupsControllerOptions()...,
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

func TestStarterSwitchGroupUsesDegradedGroupOnTransientLookupError(t *testing.T) {
	if !kerneltype.IsNextGen() {
		t.Skip("Starter deploy mode is only available in NextGen builds")
	}
	restoreConfig := config.RestoreFunc()
	t.Cleanup(restoreConfig)
	originalDeployMode := deploymode.Get()
	t.Cleanup(func() {
		require.NoError(t, deploymode.Set(originalDeployMode))
	})

	config.UpdateGlobal(func(conf *config.Config) {
		conf.StarterParams.EnableGetResourceGroupDegraded = true
	})
	provider := newResourceGroupProviderStub(t, nil, newTransientGetResourceGroupErr("target-switch-group"))
	controller := newStarterControllerForTest(t, provider)
	manager := runaway.NewRunawayManager(controller, "127.0.0.1:4000", nil, make(chan struct{}), nil, nil)
	t.Cleanup(manager.Stop)
	checker := runaway.NewChecker(
		manager,
		"source-group",
		&rmpb.RunawaySettings{
			Action:          rmpb.RunawayAction_SwitchGroup,
			SwitchGroupName: "target-switch-group",
			Rule:            &rmpb.RunawayRule{ProcessedKeys: 1},
		},
		"SELECT 1",
		"sql_digest",
		"plan_digest",
		time.Now(),
	)

	require.NoError(t, checker.CheckThresholds(nil, 10, nil))
	req := &tikvrpc.Request{
		Context: kvrpcpb.Context{
			ResourceControlContext: &kvrpcpb.ResourceControlContext{},
		},
	}
	require.NoError(t, checker.BeforeCopRequest(req))
	require.Equal(t, "target-switch-group", req.GetResourceControlContext().GetResourceGroupName())
}

func TestResourceGroupsControllerOptionsUseExplicitGetResourceGroupDegraded(t *testing.T) {
	if !kerneltype.IsNextGen() {
		t.Skip("Starter deploy mode is only available in NextGen builds")
	}
	restoreConfig := config.RestoreFunc()
	t.Cleanup(restoreConfig)
	originalDeployMode := deploymode.Get()
	t.Cleanup(func() {
		require.NoError(t, deploymode.Set(originalDeployMode))
	})

	newController := func(t *testing.T) *rmclient.ResourceGroupsController {
		t.Helper()
		provider := newResourceGroupProviderStub(t, nil, nil)
		provider.controllerConfig = rmclient.DefaultConfig()
		provider.controllerConfig.WaitRetryInterval = rmclient.NewDuration(250 * time.Millisecond)
		provider.controllerConfig.WaitRetryTimes = 4
		provider.controllerConfig.LTBTokenRPCMaxDelay = rmclient.NewDuration(time.Second)

		controller, err := rmclient.NewResourceGroupController(
			context.Background(),
			1,
			provider,
			nil,
			0,
			newResourceGroupsControllerOptions()...,
		)
		require.NoError(t, err)
		return controller
	}

	t.Run("enabled explicitly in starter", func(t *testing.T) {
		require.NoError(t, deploymode.Set(deploymode.Starter))
		config.UpdateGlobal(func(conf *config.Config) {
			conf.StarterParams.PodNamespace = "starter-standard-ns"
			conf.StarterParams.EnableGetResourceGroupDegraded = true
		})

		ruConfig := newController(t).GetConfig()
		require.Equal(t, tokenWaitRetryInterval, ruConfig.WaitRetryInterval)
		require.Equal(t, tokenWaitRetryTimes, ruConfig.WaitRetryTimes)
		require.Equal(t, defaultDegradedModeWaitTimeout, ruConfig.DegradedModeWaitDuration)
	})

	t.Run("namespace alone does not enable degraded mode", func(t *testing.T) {
		require.NoError(t, deploymode.Set(deploymode.Starter))
		config.UpdateGlobal(func(conf *config.Config) {
			conf.StarterParams.PodNamespace = "starter-vip-ns"
			conf.StarterParams.EnableGetResourceGroupDegraded = false
		})

		ruConfig := newController(t).GetConfig()
		require.Equal(t, 250*time.Millisecond, ruConfig.WaitRetryInterval)
		require.Equal(t, 4, ruConfig.WaitRetryTimes)
		require.Zero(t, ruConfig.DegradedModeWaitDuration)
	})

	t.Run("ignored outside starter", func(t *testing.T) {
		require.NoError(t, deploymode.Set(deploymode.Premium))
		config.UpdateGlobal(func(conf *config.Config) {
			conf.StarterParams.EnableGetResourceGroupDegraded = true
		})

		ruConfig := newController(t).GetConfig()
		require.Equal(t, 250*time.Millisecond, ruConfig.WaitRetryInterval)
		require.Equal(t, 4, ruConfig.WaitRetryTimes)
		require.Zero(t, ruConfig.DegradedModeWaitDuration)
	})
}
