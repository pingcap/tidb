package infosync

import (
	"bytes"
	"context"
	"encoding/json"
	"path"

	"github.com/pingcap/tidb/ddl/label"
	"github.com/pingcap/tidb/util/pdapi"
)

// LabelRuleManager manages label rules
type LabelRuleManager interface {
	PutLabelRule(ctx context.Context, rule *label.Rule) error
	UpdateLabelRules(ctx context.Context, patch *label.RulePatch) error
	GetAllLabelRules(ctx context.Context) ([]*label.Rule, error)
	GetLabelRules(ctx context.Context, ruleIDs []string) ([]*label.Rule, error)
}

// PDLabelManager manages rules with pd
type PDLabelManager struct {
	addrs []string
}

// PutLabelRule implements PutLabelRule
func (lm *PDLabelManager) PutLabelRule(ctx context.Context, rule *label.Rule) error {
	r, err := json.Marshal(rule)
	if err != nil {
		return err
	}
	_, err = doRequest(ctx, lm.addrs, path.Join(pdapi.Config, "region-label", "rule"), "POST", bytes.NewReader(r))
	return err
}

// UpdateLabelRules implements UpdateLabelRules
func (lm *PDLabelManager) UpdateLabelRules(ctx context.Context, patch *label.RulePatch) error {
	r, err := json.Marshal(patch)
	if err != nil {
		return err
	}

	_, err = doRequest(ctx, lm.addrs, path.Join(pdapi.Config, "region-label", "rules"), "PATCH", bytes.NewReader(r))
	return err
}

// GetAllLabelRules implements GetAllLabelRules
func (lm *PDLabelManager) GetAllLabelRules(ctx context.Context) ([]*label.Rule, error) {
	var rules []*label.Rule
	res, err := doRequest(ctx, lm.addrs, path.Join(pdapi.Config, "region-label", "rules"), "GET", nil)

	if err == nil && res != nil {
		err = json.Unmarshal(res, &rules)
	}
	return rules, err
}

// GetLabelRules implements GetLabelRules
func (lm *PDLabelManager) GetLabelRules(ctx context.Context, ruleIDs []string) ([]*label.Rule, error) {
	ids, err := json.Marshal(ruleIDs)
	if err != nil {
		return nil, err
	}

	rules := []*label.Rule{}
	res, err := doRequest(ctx, lm.addrs, path.Join(pdapi.Config, "region-label", "rules", "ids"), "GET", bytes.NewReader(ids))

	if err == nil && res != nil {
		err = json.Unmarshal(res, &rules)
	}
	return rules, err
}

type mockLabelManager struct {
	labelRules map[string]*label.Rule
}

// PutLabelRule implements PutLabelRule
func (mm *mockLabelManager) PutLabelRule(ctx context.Context, rule *label.Rule) error {
	mm.labelRules[rule.ID] = rule
	return nil
}

// UpdateLabelRules implements UpdateLabelRules
func (mm *mockLabelManager) UpdateLabelRules(ctx context.Context, patch *label.RulePatch) error {
	for _, p := range patch.DeleteRules {
		delete(mm.labelRules, p)
	}
	for _, p := range patch.SetRules {
		mm.labelRules[p.ID] = p
	}
	return nil
}

// mockLabelManager implements GetAllLabelRules
func (mm *mockLabelManager) GetAllLabelRules(ctx context.Context) ([]*label.Rule, error) {
	r := make([]*label.Rule, len(mm.labelRules))
	for _, labelRule := range mm.labelRules {
		r = append(r, labelRule)
	}
	return r, nil
}

// mockLabelManager implements GetLabelRules
func (mm *mockLabelManager) GetLabelRules(ctx context.Context, ruleIDs []string) ([]*label.Rule, error) {
	r := make([]*label.Rule, len(mm.labelRules))
	for _, ruleID := range ruleIDs {
		for _, labelRule := range mm.labelRules {
			if labelRule.ID == ruleID {
				r = append(r, labelRule)
				break
			}
		}
	}
	return r, nil
}
