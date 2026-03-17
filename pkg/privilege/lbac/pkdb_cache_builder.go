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

package lbac

import (
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// BuildCache constructs the in-memory LBAC cache from raw privilege records.
func BuildCache(components []Component, policies []Policy, labels []Label, grants []UserLabel, exemptions []UserExemption) *SecurityLabelCache {
	cache := &SecurityLabelCache{
		Components:       make(map[string]*Component, len(components)),
		Policies:         make(map[string]*Policy, len(policies)),
		Labels:           make(map[string]*Label, len(labels)),
		Exemptions:       make(map[ExemptionKey]struct{}),
		userPolicyGrants: make(map[UserHost]map[string]*policyGrant),
	}
	for i := range components {
		comp := &components[i]
		comp.Name = strings.ToLower(comp.Name)
		cache.Components[comp.Name] = comp
	}
	for i := range policies {
		policy := &policies[i]
		policy.Name = strings.ToLower(policy.Name)
		for j := range policy.ComponentNames {
			policy.ComponentNames[j] = strings.ToLower(policy.ComponentNames[j])
		}
		cache.Policies[policy.Name] = policy
	}
	labelCounts := make(map[string]int, len(labels))
	for i := range labels {
		label := &labels[i]
		label.PolicyName = strings.ToLower(label.PolicyName)
		labelCounts[label.PolicyName]++
	}
	for _, policy := range cache.Policies {
		if err := initPolicyLabelIndex(cache, policy, labelCounts[policy.Name]); err != nil {
			logutil.BgLogger().Warn("lbac policy meta build failed",
				zap.String("policy", policy.Name),
				zap.Error(err))
		}
	}
	for i := range labels {
		label := &labels[i]
		labelName := strings.ToLower(label.Name)
		policy := cache.Policies[label.PolicyName]
		if policy == nil {
			logutil.BgLogger().Warn("lbac label policy not found in cache",
				zap.String("label", labelName),
				zap.String("policy", label.PolicyName))
			continue
		}
		label.Components = canonicalizeLabelComponents(label.Components)
		value, err := buildLabelString(cache.Components, policy, label.Components)
		if err != nil {
			logutil.BgLogger().Warn("lbac label string build failed",
				zap.String("label", labelName),
				zap.String("policy", label.PolicyName),
				zap.Error(err))
			continue
		}
		encoded, err := EncodeLabelComponents(cache.Components, policy, label.Components)
		if err != nil {
			logutil.BgLogger().Warn("lbac label encode failed",
				zap.String("label", labelName),
				zap.String("policy", label.PolicyName),
				zap.Error(err))
			continue
		}
		label.StringValue = value
		label.EncodedValue = encoded
		cache.Labels[labelName] = label
		if policy.labelsByName != nil {
			policy.labelsByName[labelName] = label
			if label.StringValue != "" {
				policy.labelsByString[label.StringValue] = label
			}
			if len(label.EncodedValue) > 0 {
				policy.labelsByEncoded[string(label.EncodedValue)] = label
			}
		}
	}
	for i := range exemptions {
		exemption := &exemptions[i]
		exemption.Host = normalizeHost(exemption.Host)
		exemption.PolicyName = strings.ToLower(exemption.PolicyName)
		key := ExemptionKey{
			User:       exemption.UserName,
			Host:       exemption.Host,
			PolicyName: exemption.PolicyName,
		}
		cache.Exemptions[key] = struct{}{}
	}
	for i := range grants {
		grant := &grants[i]
		grant.Host = normalizeHost(grant.Host)
		grant.LabelName = strings.ToLower(grant.LabelName)
		userKey := UserHost{User: grant.UserName, Host: grant.Host}
		if err := cache.addGrantToIndex(userKey, grant); err != nil {
			logutil.BgLogger().Warn("lbac grant skipped",
				zap.String("user", grant.UserName),
				zap.String("host", grant.Host),
				zap.String("label", grant.LabelName),
				zap.Error(err))
		}
	}
	return cache
}

func canonicalizeLabelComponents(raw map[string][]string) map[string][]string {
	if len(raw) == 0 {
		return raw
	}
	components := make(map[string][]string, len(raw))
	for name, values := range raw {
		components[strings.ToLower(name)] = values
	}
	return components
}

func initPolicyLabelIndex(cache *SecurityLabelCache, policy *Policy, labelCount int) error {
	if policy == nil {
		return ErrPolicyNotFound
	}
	if len(policy.ComponentNames) == 0 {
		return ErrEmptyPolicyComponents
	}
	for _, name := range policy.ComponentNames {
		component := cache.Components[name]
		if component == nil {
			return ErrComponentNotFound
		}
	}
	policy.labelsByName = make(map[string]*Label, labelCount)
	policy.labelsByString = make(map[string]*Label, labelCount)
	policy.labelsByEncoded = make(map[string]*Label, labelCount)
	return nil
}

func (c *SecurityLabelCache) addGrantToIndex(userKey UserHost, grant *UserLabel) error {
	if c == nil || grant == nil {
		return ErrLBACCacheUnavailable
	}
	label := c.Labels[grant.LabelName]
	if label == nil {
		return ErrLabelNotFound
	}
	policyName := label.PolicyName
	policy, err := c.policyMeta(policyName)
	if err != nil {
		return err
	}
	lbl := policy.labelsByName[grant.LabelName]
	if lbl == nil || len(lbl.EncodedValue) == 0 {
		return ErrLabelNotFound
	}
	if c.userPolicyGrants[userKey] == nil {
		c.userPolicyGrants[userKey] = make(map[string]*policyGrant)
	}
	pg, ok := c.userPolicyGrants[userKey][policyName]
	if !ok {
		pg = &policyGrant{
			exempted: c.HasExemption(grant.UserName, grant.Host, policyName),
		}
		c.userPolicyGrants[userKey][policyName] = pg
	}
	if hasAccessType(grant.AccessType, ast.SecurityLabelAccessTypeRead) {
		pg.read = append(pg.read, lbl)
	}
	if hasAccessType(grant.AccessType, ast.SecurityLabelAccessTypeWrite) {
		pg.write = append(pg.write, lbl)
	}
	return nil
}
