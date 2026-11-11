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
	"context"
	"sort"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// MaskingPolicyByName returns masking policy metadata by policy name with delayed loading.
// Note: Policy name is only unique per table, not globally. Ambiguous names return no policy. For precise lookup, use MaskingPolicyByTableColumn.
func (is *infoSchema) MaskingPolicyByName(ctx context.Context, name pmodel.CIStr) (*model.MaskingPolicyInfo, bool, error) {
	if err := is.loadMaskingPoliciesIfNeeded(ctx); err != nil {
		return nil, false, err
	}

	is.maskingPolicyMutex.RLock()
	defer is.maskingPolicyMutex.RUnlock()
	var found *model.MaskingPolicyInfo
	for _, colMap := range is.maskingPolicyTableColumnMap {
		for _, policy := range colMap {
			if policy.Name.L == name.L {
				if found != nil {
					return nil, false, nil
				}
				found = policy
			}
		}
	}
	return found, found != nil, nil
}

// MaskingPolicyByTableColumn returns masking policy metadata by table and column IDs with delayed loading.
func (is *infoSchema) MaskingPolicyByTableColumn(ctx context.Context, tableID, columnID int64) (*model.MaskingPolicyInfo, bool, error) {
	if err := is.loadMaskingPoliciesIfNeeded(ctx); err != nil {
		return nil, false, err
	}

	is.maskingPolicyMutex.RLock()
	defer is.maskingPolicyMutex.RUnlock()

	if is.maskingPolicyTableColumnMap == nil {
		return nil, false, nil
	}

	colMap, ok := is.maskingPolicyTableColumnMap[tableID]
	if !ok {
		return nil, false, nil
	}

	policy, ok := colMap[columnID]
	return policy, ok, nil
}

// AllMaskingPolicies returns all masking policies in a stable order with delayed loading.
func (is *infoSchema) AllMaskingPolicies(ctx context.Context) ([]*model.MaskingPolicyInfo, error) {
	if err := is.loadMaskingPoliciesIfNeeded(ctx); err != nil {
		return nil, err
	}

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
	return policies, nil
}

// loadMaskingPoliciesIfNeeded loads masking policies from system table on first access.
// Only one goroutine performs loading, others wait for completion.
func (is *infoSchema) loadMaskingPoliciesIfNeeded(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		var loadCh chan struct{}
		is.maskingPolicyMutex.Lock()
		if is.maskingPoliciesLoaded {
			is.maskingPolicyMutex.Unlock()
			return nil
		}
		if is.factory == nil {
			logutil.BgLogger().Debug("factory is nil, skipping masking policies loading")
			is.maskingPoliciesLoaded = true
			is.maskingPolicyMutex.Unlock()
			return nil
		}
		if is.maskingPoliciesLoadCh != nil {
			loadCh = is.maskingPoliciesLoadCh
			is.maskingPolicyMutex.Unlock()
			select {
			case <-loadCh:
			case <-ctx.Done():
				return ctx.Err()
			}
			continue
		}
		loadCh = make(chan struct{})
		is.maskingPoliciesLoadCh = loadCh
		snapshotTS := is.maskingPolicySnapshotTS
		is.maskingPolicyMutex.Unlock()

		// Load policies WITHOUT holding the lock to avoid blocking readers.
		loadCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		policies, err := loadMaskingPoliciesWithTableIDs(loadCtx, is.factory, nil, snapshotTS)
		cancel()

		is.maskingPolicyMutex.Lock()
		if err != nil {
			if isMaskingPolicyTableNotReady(err) {
				// The table may not be available during bootstrap/upgrade. Stop retrying
				// on this infoschema instance; next schema reload will re-evaluate.
				logutil.BgLogger().Debug("masking policy table not available yet, skipping", zap.Error(err))
				is.maskingPoliciesLoaded = true
				err = nil
			} else {
				// Keep loaded=false so future requests can retry loading.
				logutil.BgLogger().Warn("failed to load masking policies", zap.Error(err))
			}
		} else if !is.maskingPoliciesLoaded {
			newMap := make(map[int64]map[int64]*model.MaskingPolicyInfo, len(policies))
			for _, policy := range policies {
				if newMap[policy.TableID] == nil {
					newMap[policy.TableID] = make(map[int64]*model.MaskingPolicyInfo)
				}
				newMap[policy.TableID][policy.ColumnID] = policy
			}
			is.maskingPolicyTableColumnMap = newMap
			is.maskingPoliciesLoaded = true
			logutil.BgLogger().Info("masking policies loaded", zap.Int("count", len(policies)))
		}
		close(loadCh)
		is.maskingPoliciesLoadCh = nil
		is.maskingPolicyMutex.Unlock()
		return err
	}
}

// UpdateMaskingPolicySnapshotTS advances the policy read time after the caller
// has verified that this schema version is valid at ts. Policy state cannot
// change within one schema version. Advancing avoids retaining an expired read
// timestamp when the schema stays unchanged across GC cycles.
func UpdateMaskingPolicySnapshotTS(is InfoSchema, ts uint64) {
	base := is.base()
	base.maskingPolicyMutex.Lock()
	if ts > base.maskingPolicySnapshotTS {
		base.maskingPolicySnapshotTS = ts
	}
	base.maskingPolicyMutex.Unlock()
}

// HasMaskingPolicies checks for policies without copying or sorting the policy map.
func (is *infoSchema) HasMaskingPolicies(ctx context.Context) (bool, error) {
	if err := is.loadMaskingPoliciesIfNeeded(ctx); err != nil {
		return false, err
	}
	is.maskingPolicyMutex.RLock()
	defer is.maskingPolicyMutex.RUnlock()
	return len(is.maskingPolicyTableColumnMap) > 0, nil
}

func isMaskingPolicyTableNotReady(err error) bool {
	return ErrTableNotExists.Equal(errors.Cause(err))
}
