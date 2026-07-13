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
	stderrors "errors"
	"strings"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	pd "github.com/tikv/pd/client"
	pderr "github.com/tikv/pd/client/errs"
	rmclient "github.com/tikv/pd/client/resource_group/controller"
)

// starterResourceGroupProvider wraps a resource group provider and synthesizes
// degraded resource groups only when resource-manager lookup fails due to
// unavailability.
type starterResourceGroupProvider struct {
	rmclient.ResourceGroupProvider
	degradedRUSettings *rmpb.GroupRequestUnitSettings
}

func newStarterResourceGroupProvider(provider rmclient.ResourceGroupProvider) *starterResourceGroupProvider {
	return &starterResourceGroupProvider{
		ResourceGroupProvider: provider,
		degradedRUSettings:    newDefaultDegradedRUSettings(),
	}
}

func (p *starterResourceGroupProvider) GetResourceGroup(
	ctx context.Context, resourceGroupName string, opts ...pd.GetResourceGroupOption,
) (*rmpb.ResourceGroup, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	group, err := p.ResourceGroupProvider.GetResourceGroup(ctx, resourceGroupName, opts...)
	if err == nil {
		return group, nil
	}
	if !shouldUseDegradedResourceGroupFallback(ctx, err) {
		return nil, err
	}
	return newDegradedResourceGroup(resourceGroupName, p.degradedRUSettings), nil
}

func newDegradedResourceGroup(name string, ruSettings *rmpb.GroupRequestUnitSettings) *rmpb.ResourceGroup {
	return &rmpb.ResourceGroup{
		Name:       name,
		Mode:       rmpb.GroupMode_RUMode,
		RUSettings: ruSettings,
	}
}

func shouldUseDegradedResourceGroupFallback(ctx context.Context, err error) bool {
	if err == nil {
		return false
	}
	if ctx.Err() != nil {
		return false
	}
	if stderrors.Is(err, context.Canceled) || stderrors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if isResourceGroupNotExistError(err) {
		return false
	}
	if stderrors.Is(err, pderr.ErrClientResourceGroupConfigUnavailable) {
		return true
	}
	var getRGErr *pderr.ErrClientGetResourceGroup
	if stderrors.As(err, &getRGErr) {
		return isResourceManagerUnavailableCause(getRGErr.Cause)
	}
	return isResourceManagerUnavailableCause(err.Error())
}

func isResourceGroupNotExistError(err error) bool {
	var getRGErr *pderr.ErrClientGetResourceGroup
	if !stderrors.As(err, &getRGErr) {
		return false
	}
	return isResourceGroupNotExistCause(getRGErr.Cause)
}

func isResourceGroupNotExistCause(cause string) bool {
	lower := strings.ToLower(cause)
	return strings.Contains(lower, "does not exist") ||
		strings.Contains(lower, "not found") ||
		strings.Contains(lower, "not exist")
}

func isResourceManagerUnavailableCause(cause string) bool {
	if isResourceGroupNotExistCause(cause) {
		return false
	}
	lower := strings.ToLower(cause)
	return strings.Contains(lower, "unavailable") ||
		strings.Contains(lower, "connection refused") ||
		strings.Contains(lower, "connection reset") ||
		strings.Contains(lower, "deadline exceeded") ||
		strings.Contains(lower, "transport is closing") ||
		strings.Contains(lower, "rpc error")
}
