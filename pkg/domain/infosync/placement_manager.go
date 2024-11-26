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
	"fmt"
	"sort"
	"sync"

	"github.com/pingcap/tidb/pkg/ddl/placement"
	pd "github.com/tikv/pd/client/http"
)

// PlacementManager manages placement settings
type PlacementManager interface {
	// GetRuleBundle is used to get one specific rule bundle from PD.
	GetRuleBundle(ctx context.Context, name string) (*placement.Bundle, error)
	// GetAllRuleBundles is used to get all rule bundles from PD. It is used to load full rules from PD while fullload infoschema.
	GetAllRuleBundles(ctx context.Context) ([]*placement.Bundle, error)
	// PutRuleBundles is used to post specific rule bundles to PD.
	PutRuleBundles(ctx context.Context, bundles []*placement.Bundle) error
}

// PDPlacementManager manages placement with pd
type PDPlacementManager struct {
	pdHTTPCli pd.Client
}

// GetRuleBundle is used to get one specific rule bundle from PD.
func (m *PDPlacementManager) GetRuleBundle(ctx context.Context, name string) (*placement.Bundle, error) {
	groupBundle, err := m.pdHTTPCli.GetPlacementRuleBundleByGroup(ctx, name)
	if err != nil {
		return nil, err
	}
	groupBundle.ID = name
	return (*placement.Bundle)(groupBundle), err
}

// GetAllRuleBundles is used to get all rule bundles from PD. It is used to load full rules from PD while fullload infoschema.
func (m *PDPlacementManager) GetAllRuleBundles(ctx context.Context) ([]*placement.Bundle, error) {
	bundles, err := m.pdHTTPCli.GetAllPlacementRuleBundles(ctx)
	if err != nil {
		return nil, err
	}
	rules := make([]*placement.Bundle, 0, len(bundles))
	for _, bundle := range bundles {
		rules = append(rules, (*placement.Bundle)(bundle))
	}
	return rules, nil
}

// PutRuleBundles is used to post specific rule bundles to PD.
func (m *PDPlacementManager) PutRuleBundles(ctx context.Context, bundles []*placement.Bundle) error {
	if len(bundles) == 0 {
		return nil
	}
	ruleBundles := make([]*pd.GroupBundle, 0, len(bundles))
	for _, bundle := range bundles {
		ruleBundles = append(ruleBundles, (*pd.GroupBundle)(bundle))
	}
	return m.pdHTTPCli.SetPlacementRuleBundles(ctx, ruleBundles, true)
}

type mockPlacementManager struct {
	sync.Mutex
	bundles map[string]*placement.Bundle
}

func (m *mockPlacementManager) GetRuleBundle(_ context.Context, name string) (*placement.Bundle, error) {
	m.Lock()
	defer m.Unlock()

	if bundle, ok := m.bundles[name]; ok {
		return bundle, nil
	}

	return &placement.Bundle{ID: name}, nil
}

func (m *mockPlacementManager) GetAllRuleBundles(_ context.Context) ([]*placement.Bundle, error) {
	m.Lock()
	defer m.Unlock()

	bundles := make([]*placement.Bundle, 0, len(m.bundles))
	for _, bundle := range m.bundles {
		bundles = append(bundles, bundle)
	}
	return bundles, nil
}

type keyRange struct {
	groupID  string
	start    string
	end      string
	id       string
	string   int
	override bool
	isLeader bool
}

// CheckBundle check that the rules don't overlap without explicit Override
// Exported for testing reasons.
// Tries to match prepareRulesForApply + checkApplyRules from pd.
// And additionally checks for key overlaps.
func CheckBundle(bundle *placement.Bundle) error {
	keys := make([]keyRange, 0, len(bundle.Rules))
	for _, rule := range bundle.Rules {
		if rule.GroupID != bundle.ID {
			return fmt.Errorf("rule group id %s is not same as bundle id %s", rule.GroupID, bundle.ID)
		}
		keys = append(keys, keyRange{
			groupID:  rule.GroupID,
			id:       rule.ID,
			start:    rule.StartKeyHex,
			end:      rule.EndKeyHex,
			override: rule.Override,
			isLeader: rule.Role == pd.Leader,
		})
	}
	if len(keys) == 0 {
		return fmt.Errorf(`ERROR 8243 (HY000): "[PD:placement:ErrBuildRuleList]build rule list failed, no rule left`)
	}
	// Skip overridden rules, but only within the bundle, not across groups
	applyKeys := keys[:0]
	j := 0
	for i := 1; i < len(keys); i++ {
		if keys[i].override {
			j = i // skip all previous rules in the same group
			// TODO: Should we only override the matching key range?
		}
	}
	applyKeys = append(applyKeys, keys[j:]...)
	if len(applyKeys) == 0 {
		return fmt.Errorf(`ERROR 8243 (HY000): "[PD:placement:ErrBuildRuleList]build rule list failed, no rule left`)
	}

	// Additionally check for range overlapping leaders.
	keys = keys[:0]
	for i := 0; i < len(applyKeys); i++ {
		if applyKeys[i].isLeader {
			keys = append(keys, applyKeys[i])
		}
	}
	if len(keys) == 0 {
		return nil
	}

	// Sort on Start, id, end
	// Could use pd's placement.sortRules() instead.
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].start == keys[j].start {
			if keys[i].id == keys[j].id {
				return keys[i].end < keys[j].end
			}
			return keys[i].id < keys[j].id
		}
		return keys[i].start < keys[j].start
	})

	for i := 1; i < len(keys); i++ {
		if keys[i].start < keys[i-1].end {
			return fmt.Errorf(`ERROR 8243 (HY000): "[PD:placement:ErrBuildRuleList]build rule list failed, multiple leader replicas for range {%s, %s}`, keys[i-1].start, keys[i].end)
		}
	}
	return nil
}

func checkBundles(bundles map[string]*placement.Bundle) error {
	// Check that no bundles have leaders overlapping ranges
	for k := range bundles {
		if err := CheckBundle(bundles[k]); err != nil {
			return err
		}
	}
	return nil
}

func (m *mockPlacementManager) PutRuleBundles(_ context.Context, bundles []*placement.Bundle) error {
	m.Lock()
	defer m.Unlock()

	if m.bundles == nil {
		m.bundles = make(map[string]*placement.Bundle)
	}

	rules := 0
	for _, bundle := range bundles {
		if bundle.IsEmpty() {
			delete(m.bundles, bundle.ID)
		} else {
			m.bundles[bundle.ID] = bundle
			rules += len(bundle.Rules)
		}
	}

	return checkBundles(m.bundles)
}
