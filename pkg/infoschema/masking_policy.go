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

	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
)

// MaskingPolicyByName returns masking policy metadata by policy name.
func (is *infoSchema) MaskingPolicyByName(name pmodel.CIStr) (*model.MaskingPolicyInfo, bool) {
	policy, ok := is.maskingPolicyMap[name.L]
	return policy, ok
}

// MaskingPolicyByTableColumn returns masking policy metadata by table and column IDs.
func (is *infoSchema) MaskingPolicyByTableColumn(tableID, columnID int64) (*model.MaskingPolicyInfo, bool) {
	colMap, ok := is.maskingPolicyTableColumnMap[tableID]
	if !ok {
		return nil, false
	}
	policy, ok := colMap[columnID]
	return policy, ok
}

// AllMaskingPolicies returns all masking policies in a stable order.
func (is *infoSchema) AllMaskingPolicies() []*model.MaskingPolicyInfo {
	policies := make([]*model.MaskingPolicyInfo, 0, len(is.maskingPolicyMap))
	for _, policy := range is.maskingPolicyMap {
		policies = append(policies, policy)
	}
	sort.Slice(policies, func(i, j int) bool {
		if policies[i].Name.L == policies[j].Name.L {
			return policies[i].ID < policies[j].ID
		}
		return policies[i].Name.L < policies[j].Name.L
	})
	return policies
}

func (is *infoSchema) setMaskingPolicy(policy *model.MaskingPolicyInfo) {
	if policy == nil {
		return
	}
	is.maskingPolicyMap[policy.Name.L] = policy
	colMap, ok := is.maskingPolicyTableColumnMap[policy.TableID]
	if !ok {
		colMap = make(map[int64]*model.MaskingPolicyInfo)
		is.maskingPolicyTableColumnMap[policy.TableID] = colMap
	}
	colMap[policy.ColumnID] = policy
}

func (is *infoSchema) deleteMaskingPolicy(nameL string) {
	policy, ok := is.maskingPolicyMap[nameL]
	if !ok {
		return
	}
	delete(is.maskingPolicyMap, nameL)
	colMap, ok := is.maskingPolicyTableColumnMap[policy.TableID]
	if !ok {
		return
	}
	delete(colMap, policy.ColumnID)
	if len(colMap) == 0 {
		delete(is.maskingPolicyTableColumnMap, policy.TableID)
	}
}
