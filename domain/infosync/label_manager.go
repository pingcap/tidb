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
	"bytes"
	"context"
	"encoding/json"
	"path"
	"sync"

	"github.com/pingcap/tidb/ddl/label"
	"github.com/pingcap/tidb/util/pdapi"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// LabelRuleManager manages label rules
type LabelRuleManager interface {
	PutLabelRule(ctx context.Context, rule *label.Rule) error
	UpdateLabelRules(ctx context.Context, patch *label.RulePatch) error
	GetAllLabelRules(ctx context.Context) ([]*label.Rule, error)
	GetLabelRules(ctx context.Context, ruleIDs []string) (map[string]*label.Rule, error)
}

// PDLabelManager manages rules with pd
type PDLabelManager struct {
	etcdCli *clientv3.Client
}

// PutLabelRule implements PutLabelRule
func (lm *PDLabelManager) PutLabelRule(ctx context.Context, rule *label.Rule) error {
	r, err := json.Marshal(rule)
	if err != nil {
		return err
	}
	_, err = doRequest(ctx, "PutLabelRule", lm.etcdCli.Endpoints(), path.Join(pdapi.Config, "region-label", "rule"), "POST", bytes.NewReader(r))
	return err
}

// UpdateLabelRules implements UpdateLabelRules
func (lm *PDLabelManager) UpdateLabelRules(ctx context.Context, patch *label.RulePatch) error {
	r, err := json.Marshal(patch)
	if err != nil {
		return err
	}

	_, err = doRequest(ctx, "UpdateLabelRules", lm.etcdCli.Endpoints(), path.Join(pdapi.Config, "region-label", "rules"), "PATCH", bytes.NewReader(r))
	return err
}

// GetAllLabelRules implements GetAllLabelRules
func (lm *PDLabelManager) GetAllLabelRules(ctx context.Context) ([]*label.Rule, error) {
	var rules []*label.Rule
	res, err := doRequest(ctx, "GetAllLabelRules", lm.etcdCli.Endpoints(), path.Join(pdapi.Config, "region-label", "rules"), "GET", nil)

	if err == nil && res != nil {
		err = json.Unmarshal(res, &rules)
	}
	return rules, err
}

// GetLabelRules implements GetLabelRules
func (lm *PDLabelManager) GetLabelRules(ctx context.Context, ruleIDs []string) (map[string]*label.Rule, error) {
	ids, err := json.Marshal(ruleIDs)
	if err != nil {
		return nil, err
	}

	rules := []*label.Rule{}
	res, err := doRequest(ctx, "GetLabelRules", lm.etcdCli.Endpoints(), path.Join(pdapi.Config, "region-label", "rules", "ids"), "GET", bytes.NewReader(ids))

	if err == nil && res != nil {
		err = json.Unmarshal(res, &rules)
	}

	ruleMap := make(map[string]*label.Rule, len((rules)))
	for _, r := range rules {
		ruleMap[r.ID] = r
	}
	return ruleMap, err
}

type mockLabelManager struct {
	sync.RWMutex
	labelRules map[string][]byte
}

// PutLabelRule implements PutLabelRule
func (mm *mockLabelManager) PutLabelRule(ctx context.Context, rule *label.Rule) error {
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
func (mm *mockLabelManager) UpdateLabelRules(ctx context.Context, patch *label.RulePatch) error {
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
func (mm *mockLabelManager) GetAllLabelRules(ctx context.Context) ([]*label.Rule, error) {
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
func (mm *mockLabelManager) GetLabelRules(ctx context.Context, ruleIDs []string) (map[string]*label.Rule, error) {
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
