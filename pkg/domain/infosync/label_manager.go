// Copyright 2021 PingCAP, Inc.
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
	"encoding/json"
	"sync"

	"github.com/pingcap/tidb/pkg/ddl/label"
	pd "github.com/tikv/pd/client/http"
)

// LabelRuleManager manages label rules
type LabelRuleManager interface {
	PutLabelRule(ctx context.Context, rule *label.Rule) error
	UpdateLabelRules(ctx context.Context, patch *pd.LabelRulePatch) error
	GetAllLabelRules(ctx context.Context) ([]*label.Rule, error)
	GetLabelRules(ctx context.Context, ruleIDs []string) (map[string]*label.Rule, error)
}

// PDLabelManager manages rules with pd
type PDLabelManager struct {
	pdHTTPCli pd.Client
}

// PutLabelRule implements PutLabelRule
func (lm *PDLabelManager) PutLabelRule(ctx context.Context, rule *label.Rule) error {
	return lm.pdHTTPCli.SetRegionLabelRule(ctx, (*pd.LabelRule)(rule))
}

// UpdateLabelRules implements UpdateLabelRules
func (lm *PDLabelManager) UpdateLabelRules(ctx context.Context, patch *pd.LabelRulePatch) error {
	return lm.pdHTTPCli.PatchRegionLabelRules(ctx, patch)
}

// GetAllLabelRules implements GetAllLabelRules
func (lm *PDLabelManager) GetAllLabelRules(ctx context.Context) ([]*label.Rule, error) {
	labelRules, err := lm.pdHTTPCli.GetAllRegionLabelRules(ctx)
	if err != nil {
		return nil, err
	}
	r := make([]*label.Rule, 0, len(labelRules))
	for _, labelRule := range labelRules {
		r = append(r, (*label.Rule)(labelRule))
	}
	return r, nil
}

// GetLabelRules implements GetLabelRules
func (lm *PDLabelManager) GetLabelRules(ctx context.Context, ruleIDs []string) (map[string]*label.Rule, error) {
	labelRules, err := lm.pdHTTPCli.GetRegionLabelRulesByIDs(ctx, ruleIDs)
	if err != nil {
		return nil, err
	}
	ruleMap := make(map[string]*label.Rule, len((labelRules)))
	for _, r := range labelRules {
		ruleMap[r.ID] = (*label.Rule)(r)
	}
	return ruleMap, err
}

type mockLabelManager struct {
	sync.RWMutex
	labelRules map[string][]byte
}

// PutLabelRule implements PutLabelRule
func (mm *mockLabelManager) PutLabelRule(_ context.Context, rule *label.Rule) error {
	mm.Lock()
	defer mm.Unlock()
	if rule == nil {
		return nil
	}
	r, err := json.Marshal(rule)
	if err != nil {
		return err
	}
	mm.labelRules[rule.ID] = r
	return nil
}

// UpdateLabelRules implements UpdateLabelRules
func (mm *mockLabelManager) UpdateLabelRules(_ context.Context, patch *pd.LabelRulePatch) error {
	mm.Lock()
	defer mm.Unlock()
	if patch == nil {
		return nil
	}
	for _, p := range patch.DeleteRules {
		delete(mm.labelRules, p)
	}
	for _, p := range patch.SetRules {
		if p == nil {
			continue
		}
		r, err := json.Marshal(p)
		if err != nil {
			return err
		}
		mm.labelRules[p.ID] = r
	}
	return nil
}

// mockLabelManager implements GetAllLabelRules
func (mm *mockLabelManager) GetAllLabelRules(context.Context) ([]*label.Rule, error) {
	mm.RLock()
	defer mm.RUnlock()
	r := make([]*label.Rule, 0, len(mm.labelRules))
	for _, labelRule := range mm.labelRules {
		if labelRule == nil {
			continue
		}
		var rule *label.Rule
		err := json.Unmarshal(labelRule, &rule)
		if err != nil {
			return nil, err
		}
		r = append(r, rule)
	}
	return r, nil
}

// mockLabelManager implements GetLabelRules
func (mm *mockLabelManager) GetLabelRules(_ context.Context, ruleIDs []string) (map[string]*label.Rule, error) {
	mm.RLock()
	defer mm.RUnlock()
	r := make(map[string]*label.Rule, len(ruleIDs))
	for _, ruleID := range ruleIDs {
		for id, labelRule := range mm.labelRules {
			if id == ruleID {
				if labelRule == nil {
					continue
				}
				var rule *label.Rule
				err := json.Unmarshal(labelRule, &rule)
				if err != nil {
					return nil, err
				}
				r[ruleID] = rule
				break
			}
		}
	}
	return r, nil
}
