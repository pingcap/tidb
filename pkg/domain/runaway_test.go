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
	"time"

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
	rmclient "github.com/tikv/pd/client/resource_group/controller"
)

type resourceGroupProviderStub struct {
	rmclient.ResourceGroupProvider
	resourceGroup *rmpb.ResourceGroup
	resourceErr   error
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

func newStarterControllerForTest(t *testing.T, provider rmclient.ResourceGroupProvider) *rmclient.ResourceGroupsController {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	require.NoError(t, deploymode.Set(deploymode.Starter))
	controller, err := rmclient.NewResourceGroupController(
		ctx,
		1,
		newStarterResourceGroupProvider(provider),
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

func TestResourceGroupsControllerOptionsProvideDegradedFallback(t *testing.T) {
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
	}, &pderr.ErrClientGetResourceGroup{
		ResourceGroupName: "test-group",
		Cause:             "resource group unavailable",
	})

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

	controllerWithFallback := newStarterControllerForTest(t, provider)
	group, err := controllerWithFallback.GetResourceGroup("test-group")
	require.NoError(t, err)
	require.Equal(t, newDegradedResourceGroup("test-group", newDefaultDegradedRUSettings()), group)

	require.NoError(t, deploymode.Set(deploymode.Premium))
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

func TestStarterResourceGroupProviderNonRetryableErrors(t *testing.T) {
	if !kerneltype.IsNextGen() {
		t.Skip("Starter deploy mode is only available in NextGen builds")
	}
	originalDeployMode := deploymode.Get()
	t.Cleanup(func() {
		require.NoError(t, deploymode.Set(originalDeployMode))
	})

	t.Run("NotFound", func(t *testing.T) {
		provider := newResourceGroupProviderStub(t, nil, rmclient.NewResourceGroupNotExistErr("missing-group"))
		controller := newStarterControllerForTest(t, provider)

		group, err := controller.GetResourceGroup("missing-group")
		require.Error(t, err)
		require.Nil(t, group)
	})

	t.Run("ContextCanceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		provider := newResourceGroupProviderStub(t, nil, context.Canceled)
		wrapped := newStarterResourceGroupProvider(provider)
		group, err := wrapped.GetResourceGroup(ctx, "test-group")
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, group)
	})

	t.Run("GenericNonRetryableError", func(t *testing.T) {
		provider := newResourceGroupProviderStub(t, nil, errors.New("invalid resource group lookup"))
		controller := newStarterControllerForTest(t, provider)

		group, err := controller.GetResourceGroup("test-group")
		require.Error(t, err)
		require.Nil(t, group)
	})
}

func TestStarterSwitchGroupRejectsMissingGroup(t *testing.T) {
	if !kerneltype.IsNextGen() {
		t.Skip("Starter deploy mode is only available in NextGen builds")
	}
	originalDeployMode := deploymode.Get()
	t.Cleanup(func() {
		require.NoError(t, deploymode.Set(originalDeployMode))
	})

	provider := newResourceGroupProviderStub(t, nil, rmclient.NewResourceGroupNotExistErr("missing-switch-group"))
	controller := newStarterControllerForTest(t, provider)
	manager := runaway.NewRunawayManager(controller, "127.0.0.1:4000", nil, make(chan struct{}), nil, nil)
	t.Cleanup(manager.Stop)
	checker := runaway.NewChecker(
		manager,
		"source-group",
		&rmpb.RunawaySettings{
			Action:          rmpb.RunawayAction_SwitchGroup,
			SwitchGroupName: "missing-switch-group",
			Rule:            &rmpb.RunawayRule{ProcessedKeys: 1},
		},
		"SELECT 1",
		"sql_digest",
		"plan_digest",
		time.Now(),
	)

	require.NoError(t, checker.CheckThresholds(nil, 10, nil))
	req := &tikvrpc.Request{}
	require.NoError(t, checker.BeforeCopRequest(req))
	require.Empty(t, req.GetResourceControlContext().GetResourceGroupName())
}

func TestResourceGroupsControllerOptionsUseStarterPodNamespaceForVIPWaitRetry(t *testing.T) {
	restoreConfig := config.RestoreFunc()
	t.Cleanup(restoreConfig)
	vipOptionCount := 3
	standardOptionCount := 1
	if kerneltype.IsNextGen() {
		originalDeployMode := deploymode.Get()
		t.Cleanup(func() {
			require.NoError(t, deploymode.Set(originalDeployMode))
		})
		require.NoError(t, deploymode.Set(deploymode.Starter))
		vipOptionCount = 4
		standardOptionCount = 2
	}

	config.UpdateGlobal(func(conf *config.Config) {
		conf.StarterParams.PodNamespace = "starter-vip-ns"
	})
	require.Len(t, newResourceGroupsControllerOptions(), vipOptionCount)

	config.UpdateGlobal(func(conf *config.Config) {
		conf.StarterParams.PodNamespace = "starter-standard-ns"
	})
	require.Len(t, newResourceGroupsControllerOptions(), standardOptionCount)
}
