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

package infoschema

import (
	"sort"
	"strings"

	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// MaskingPolicyByName returns masking policy metadata by policy name with delayed loading.
// Note: Policy name is only unique per table, not globally. This method returns the first matching
// policy if multiple tables have policies with the same name. For precise lookup, use MaskingPolicyByTableColumn.
func (is *infoSchema) MaskingPolicyByName(name pmodel.CIStr) (*model.MaskingPolicyInfo, bool) {
	is.loadMaskingPoliciesIfNeeded()

	is.maskingPolicyMutex.RLock()
	defer is.maskingPolicyMutex.RUnlock()

	// Search through all tables to find the first policy with matching name
	// Note: This may return a policy from any table if multiple tables have the same policy name
	for _, colMap := range is.maskingPolicyTableColumnMap {
		for _, policy := range colMap {
			if policy.Name.L == name.L {
				return policy, true
			}
		}
	}
	return nil, false
}

// MaskingPolicyByTableColumn returns masking policy metadata by table and column IDs with delayed loading.
func (is *infoSchema) MaskingPolicyByTableColumn(tableID, columnID int64) (*model.MaskingPolicyInfo, bool) {
	is.loadMaskingPoliciesIfNeeded()

	is.maskingPolicyMutex.RLock()
	defer is.maskingPolicyMutex.RUnlock()

	if is.maskingPolicyTableColumnMap == nil {
		return nil, false
	}

	colMap, ok := is.maskingPolicyTableColumnMap[tableID]
	if !ok {
		return nil, false
	}

	policy, ok := colMap[columnID]
	return policy, ok
}

// AllMaskingPolicies returns all masking policies in a stable order with delayed loading.
func (is *infoSchema) AllMaskingPolicies() []*model.MaskingPolicyInfo {
	is.loadMaskingPoliciesIfNeeded()

	is.maskingPolicyMutex.RLock()
	defer is.maskingPolicyMutex.RUnlock()

	policies := make([]*model.MaskingPolicyInfo, 0)
	for _, colMap := range is.maskingPolicyTableColumnMap {
		for _, policy := range colMap {
			policies = append(policies, policy)
		}
	}
	sort.Slice(policies, func(i, j int) bool {
		if policies[i].Name.L == policies[j].Name.L {
			return policies[i].ID < policies[j].ID
		}
		return policies[i].Name.L < policies[j].Name.L
	})
	return policies
}

// loadMaskingPoliciesIfNeeded loads masking policies from system table on first access.
// It uses double-check locking and detects recursive calls to avoid deadlock.
func (is *infoSchema) loadMaskingPoliciesIfNeeded() {
	// Fast path: already loaded
	if is.maskingPoliciesLoaded {
		return
	}

	// Detect recursive calls - if we're already loading in this goroutine, skip
	// This prevents deadlock when ExecRestrictedSQL triggers masking policy loading recursively
	if is.loadingMaskingPolicies {
		logutil.BgLogger().Debug("recursive call to loadMaskingPoliciesIfNeeded detected, skipping")
		return
	}

	// Mark as loading (without lock to avoid blocking)
	is.loadingMaskingPolicies = true
	defer func() { is.loadingMaskingPolicies = false }()

	// Check if factory is available (without lock)
	if is.factory == nil {
		logutil.BgLogger().Debug("factory is nil, skipping masking policies loading")
		is.maskingPoliciesLoaded = true
		return
	}

	// Load policies WITHOUT holding the lock to avoid deadlock
	// (SQL execution may trigger recursive calls to this method)
	policies, err := LoadMaskingPolicies(is.factory)
	if err != nil {
		// If table doesn't exist (e.g., during bootstrap), mark as loaded to avoid repeated attempts
		errStr := err.Error()
		if strings.Contains(errStr, "doesn't exist") || strings.Contains(errStr, "not exist") {
			logutil.BgLogger().Debug("masking policy table not available yet, skipping", zap.Error(err))
		} else {
			logutil.BgLogger().Warn("failed to load masking policies", zap.Error(err))
		}
		is.maskingPoliciesLoaded = true
		return
	}

	// Now acquire lock to update the maps
	is.maskingPolicyMutex.Lock()
	defer is.maskingPolicyMutex.Unlock()

	// Double check after acquiring lock
	if is.maskingPoliciesLoaded {
		return
	}

	// Update maps
	for _, policy := range policies {
		if is.maskingPolicyTableColumnMap == nil {
			is.maskingPolicyTableColumnMap = make(map[int64]map[int64]*model.MaskingPolicyInfo)
		}
		if is.maskingPolicyTableColumnMap[policy.TableID] == nil {
			is.maskingPolicyTableColumnMap[policy.TableID] = make(map[int64]*model.MaskingPolicyInfo)
		}
		is.maskingPolicyTableColumnMap[policy.TableID][policy.ColumnID] = policy
	}

	// Mark as loaded
	is.maskingPoliciesLoaded = true
	logutil.BgLogger().Info("masking policies loaded", zap.Int("count", len(policies)))
}

// Note: setMaskingPolicy and deleteMaskingPolicy methods are no longer needed.
// Masking policies are now loaded entirely through delayed loading mechanism in loadMaskingPoliciesIfNeeded().
// These methods were used during initialization, but now the maps are updated directly in loadMaskingPoliciesIfNeeded().
