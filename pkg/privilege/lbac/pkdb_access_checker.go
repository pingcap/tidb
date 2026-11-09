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
	"sort"
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
	lbacmodel "github.com/pingcap/tidb/pkg/privilege/lbac/model"
)

// SecurityLabelValidator validates label existence for a policy.
type SecurityLabelValidator struct {
	cache *SecurityLabelCache
}

// SecurityLabelAccessChecker caches access checks for a user and access type.
type SecurityLabelAccessChecker struct {
	cache      *SecurityLabelCache
	userName   string
	host       string
	accessType ast.SecurityLabelAccessType
	policies   map[string]*policyAccess
}

// policyAccess stores LBAC policy metadata and user grants for access checks.
type policyAccess struct {
	policy   *Policy
	grants   []*Label
	exempted bool
}

// NewSecurityLabelValidator prepares label validation for policies.
func NewSecurityLabelValidator(cache *SecurityLabelCache) (*SecurityLabelValidator, error) {
	if cache == nil {
		return nil, ErrLBACCacheUnavailable
	}
	return &SecurityLabelValidator{
		cache: cache,
	}, nil
}

// NewSecurityLabelAccessChecker prepares access checks for a user and access type.
func NewSecurityLabelAccessChecker(cache *SecurityLabelCache, userName, host string, accessType ast.SecurityLabelAccessType) (*SecurityLabelAccessChecker, error) {
	if host == "" {
		host = "%"
	}
	return &SecurityLabelAccessChecker{
		cache:      cache,
		userName:   userName,
		host:       host,
		accessType: accessType,
		policies:   make(map[string]*policyAccess),
	}, nil
}

// EncodeLabelValue returns the encoded label bytes for a canonical label string.
func (v *SecurityLabelValidator) EncodeLabelValue(policyName, labelValue string) ([]byte, error) {
	policy, err := v.cache.policyMeta(policyName)
	if err != nil {
		return nil, err
	}
	if labelValue == "" {
		return nil, ErrLabelNotFound
	}
	label, ok := policy.labelsByString[labelValue]
	if !ok {
		return nil, ErrLabelNotFound
	}
	return label.EncodedValue, nil
}

// DecodeLabelValue returns the canonical label string for encoded label bytes.
func (v *SecurityLabelValidator) DecodeLabelValue(policyName string, labelValue []byte) (string, error) {
	policy, err := v.cache.policyMeta(policyName)
	if err != nil {
		return "", err
	}
	if len(labelValue) == 0 {
		return "", ErrLabelNotFound
	}
	label, ok := policy.labelsByEncoded[string(labelValue)]
	if !ok || label.StringValue == "" {
		return "", ErrLabelNotFound
	}
	return label.StringValue, nil
}

// NormalizeLabelValue returns the canonical encoded bytes and string for a label.
func (v *SecurityLabelValidator) NormalizeLabelValue(policyName string, labelValue []byte) ([]byte, string, error) {
	policy, err := v.cache.policyMeta(policyName)
	if err != nil {
		return nil, "", err
	}
	if len(labelValue) == 0 {
		return nil, "", ErrLabelNotFound
	}
	if label, ok := policy.labelsByEncoded[string(labelValue)]; ok {
		return label.EncodedValue, label.StringValue, nil
	}
	labelStr := string(labelValue)
	if label, ok := policy.labelsByString[labelStr]; ok {
		return label.EncodedValue, label.StringValue, nil
	}
	return nil, "", ErrLabelNotFound
}

func (c *SecurityLabelAccessChecker) policyAccess(policyName string) (*policyAccess, error) {
	if access := c.policies[policyName]; access != nil {
		return access, nil
	}
	policy, err := c.cache.policyMeta(policyName)
	if err != nil {
		return nil, err
	}
	access := &policyAccess{
		policy: policy,
	}
	if pg := c.cache.policyGrant(c.userName, c.host, policyName); pg != nil {
		access.grants = pg.grants(c.accessType)
		access.exempted = pg.exempted
	} else {
		access.exempted = c.cache.HasExemption(c.userName, c.host, policyName)
	}
	c.policies[policyName] = access
	return access, nil
}

// RowFilterLabelValues returns the encoded grant labels to build row filters.
func (c *SecurityLabelAccessChecker) RowFilterLabelValues(policyName string) ([][]byte, error) {
	access, err := c.policyAccess(policyName)
	if err != nil {
		return nil, err
	}
	if access.exempted {
		return nil, nil
	}
	return compactGrantLabels(access.grants)
}

// CheckLabelValue validates and checks access for a row label value.
func (c *SecurityLabelAccessChecker) CheckLabelValue(policyName string, labelValue []byte) (bool, error) {
	access, err := c.policyAccess(policyName)
	if err != nil {
		return false, err
	}
	if len(labelValue) == 0 {
		return false, ErrLabelNotFound
	}
	if _, ok := access.policy.labelsByEncoded[string(labelValue)]; !ok {
		return false, ErrLabelNotFound
	}
	if access.exempted {
		return true, nil
	}
	return isLabelAccessible(access.grants, labelValue)
}

// CheckLabelName validates and checks access for a named security label.
func (c *SecurityLabelAccessChecker) CheckLabelName(policyName, labelName string) (bool, error) {
	access, err := c.policyAccess(policyName)
	if err != nil {
		return false, err
	}
	if labelName == "" {
		return false, ErrLabelNotFound
	}
	label, ok := access.policy.labelsByName[strings.ToLower(labelName)]
	if !ok {
		return false, ErrLabelNotFound
	}
	if access.exempted {
		return true, nil
	}
	return isLabelAccessible(access.grants, label.EncodedValue)
}

// IsExempted returns whether the user has any LBAC exemption.
func (c *SecurityLabelAccessChecker) IsExempted(policyName string) (bool, error) {
	access, err := c.policyAccess(policyName)
	if err != nil {
		return false, err
	}
	return access.exempted, nil
}

// IsWriteControlOverride returns whether write control is OVERRIDE.
func (c *SecurityLabelAccessChecker) IsWriteControlOverride(policyName string) (bool, error) {
	access, err := c.policyAccess(policyName)
	if err != nil {
		return false, err
	}
	return access.policy.Option == ast.SecurityPolicyOverride, nil
}

// PreferredWriteLabel returns a deterministic write label from user grants.
func (c *SecurityLabelAccessChecker) PreferredWriteLabel(policyName string) ([]byte, string, bool, error) {
	access, err := c.policyAccess(policyName)
	if err != nil {
		return nil, "", false, err
	}
	label, ok, err := preferredWriteLabel(access.grants)
	if err != nil || !ok {
		return nil, "", ok, err
	}
	if label == nil {
		return nil, "", false, nil
	}
	return label.EncodedValue, label.StringValue, true, nil
}

func preferredWriteLabel(grants []*Label) (*Label, bool, error) {
	if len(grants) == 0 {
		return nil, false, nil
	}
	unique := uniqueLabels(grants)
	if len(unique) == 0 {
		return nil, false, nil
	}
	if len(unique) == 1 {
		return pickLabel(unique), true, nil
	}
	candidates := make([]*Label, 0, len(unique))
	for _, candidate := range unique {
		dominatesAll := true
		for _, other := range unique {
			if candidate == other {
				continue
			}
			ok, err := lbacmodel.LabelBytesDominates(candidate.EncodedValue, other.EncodedValue)
			if err != nil {
				return nil, false, err
			}
			if !ok {
				dominatesAll = false
				break
			}
		}
		if dominatesAll {
			candidates = append(candidates, candidate)
		}
	}
	if len(candidates) == 0 {
		label := pickLabel(unique)
		if label == nil {
			return nil, false, nil
		}
		return label, true, nil
	}
	return pickLabel(candidates), true, nil
}

func isLabelAccessible(grants []*Label, labelValue []byte) (bool, error) {
	if len(grants) == 0 {
		return false, nil
	}
	for _, grant := range grants {
		ok, err := lbacmodel.LabelBytesDominates(grant.EncodedValue, labelValue)
		if err != nil {
			return false, err
		}
		if ok {
			return true, nil
		}
	}
	return false, nil
}

func uniqueLabels(labels []*Label) []*Label {
	seen := make(map[string]struct{}, len(labels))
	unique := make([]*Label, 0, len(labels))
	for _, label := range labels {
		if label == nil || len(label.EncodedValue) == 0 {
			continue
		}
		key := string(label.EncodedValue)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		unique = append(unique, label)
	}
	return unique
}

func compactGrantLabels(labels []*Label) ([][]byte, error) {
	unique := uniqueLabels(labels)
	if len(unique) == 0 {
		return nil, nil
	}
	if len(unique) == 1 {
		return [][]byte{unique[0].EncodedValue}, nil
	}
	filtered := make([]*Label, 0, len(unique))
	for i, grant := range unique {
		dominated := false
		for j, other := range unique {
			if i == j {
				continue
			}
			ok, err := lbacmodel.LabelBytesDominates(other.EncodedValue, grant.EncodedValue)
			if err != nil {
				return nil, err
			}
			if ok {
				dominated = true
				break
			}
		}
		if !dominated {
			filtered = append(filtered, grant)
		}
	}
	sort.Slice(filtered, func(i, j int) bool {
		return filtered[i].StringValue < filtered[j].StringValue
	})
	values := make([][]byte, 0, len(filtered))
	for _, label := range filtered {
		values = append(values, label.EncodedValue)
	}
	return values, nil
}

func pickLabel(labels []*Label) *Label {
	var best *Label
	for _, label := range labels {
		if label == nil || label.StringValue == "" {
			continue
		}
		if best == nil || label.StringValue < best.StringValue {
			best = label
		}
	}
	return best
}

func hasAccessType(grantedType, accessType ast.SecurityLabelAccessType) bool {
	if accessType == ast.SecurityLabelAccessTypeNone {
		return false
	}
	return grantedType&accessType == accessType
}
