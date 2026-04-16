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
	"github.com/pingcap/tidb/pkg/parser/ast"
	lbacmodel "github.com/pingcap/tidb/pkg/privilege/lbac/model"
	"strings"
)

// Component caches a security label component definition.
type Component struct {
	Name         string
	Type         ast.LBACComponentType
	Values       []string
	TreeParent   map[string]string
	valueOrdinal map[string]uint64
	treeRange    map[string]lbacmodel.TreeValue
}

// Policy caches a security policy definition.
type Policy struct {
	Name           string
	ComponentNames []string
	Option         ast.SecurityPolicyOption

	labelsByName    map[string]*Label
	labelsByString  map[string]*Label
	labelsByEncoded map[string]*Label
}

// Label caches a security label definition.
type Label struct {
	Name         string
	PolicyName   string
	Components   map[string][]string
	StringValue  string
	EncodedValue []byte
}

// UserLabel caches a user security label grant.
type UserLabel struct {
	UserName   string
	Host       string
	LabelName  string
	AccessType ast.SecurityLabelAccessType
}

// UserExemption caches a user exemption grant.
type UserExemption struct {
	UserName   string
	Host       string
	PolicyName string
	Rule       string
}

// TreeNodeSpec stores TREE component nodes in JSON format.
type TreeNodeSpec struct {
	Name   string `json:"name"`
	Parent string `json:"parent,omitempty"`
}

// ExemptionKey identifies an exemption grant for a user on a policy.
type ExemptionKey struct {
	User       string
	Host       string
	PolicyName string
}

type policyGrant struct {
	read     []*Label
	write    []*Label
	exempted bool
}

func (g *policyGrant) grants(accessType ast.SecurityLabelAccessType) []*Label {
	if g == nil {
		return nil
	}
	switch accessType {
	case ast.SecurityLabelAccessTypeRead:
		return g.read
	case ast.SecurityLabelAccessTypeWrite:
		return g.write
	case ast.SecurityLabelAccessTypeAll:
		// ALL isn't used in callers today; return combined grants if needed.
		labels := make([]*Label, 0, len(g.read)+len(g.write))
		labels = append(labels, g.read...)
		labels = append(labels, g.write...)
		return labels
	default:
		return nil
	}
}

// SecurityLabelCache stores LBAC metadata in memory.
type SecurityLabelCache struct {
	Components map[string]*Component
	Policies   map[string]*Policy
	Labels     map[string]*Label
	Exemptions map[ExemptionKey]struct{}

	userPolicyGrants map[UserHost]map[string]*policyGrant // user+host -> policy -> grants
}

// HasExemption returns true if the user has exemption for the policy.
func (c *SecurityLabelCache) HasExemption(userName, host, policyName string) bool {
	if c == nil || c.Exemptions == nil {
		return false
	}
	_, ok := c.Exemptions[ExemptionKey{
		User:       userName,
		Host:       normalizeHost(host),
		PolicyName: strings.ToLower(policyName),
	}]
	return ok
}

func (c *SecurityLabelCache) policyMeta(policyName string) (*Policy, error) {
	if c == nil {
		return nil, ErrLBACCacheUnavailable
	}
	policy := c.Policies[strings.ToLower(policyName)]
	if policy == nil || policy.labelsByName == nil {
		return nil, ErrPolicyNotFound
	}
	return policy, nil
}

func (c *SecurityLabelCache) policyGrant(userName, host, policyName string) *policyGrant {
	if c == nil || c.userPolicyGrants == nil {
		return nil
	}
	key := UserHost{User: userName, Host: normalizeHost(host)}
	userGrants := c.userPolicyGrants[key]
	if len(userGrants) == 0 {
		return nil
	}
	return userGrants[strings.ToLower(policyName)]
}

// CacheProvider exposes the SecurityLabelCache for access checks.
type CacheProvider interface {
	GetSecurityLabelCache() *SecurityLabelCache
}

// UserHost keeps a user and host pair.
type UserHost struct {
	User string
	Host string
}

// normalizeHost defaults an empty host to '%' to match privilege lookup rules.
func normalizeHost(host string) string {
	if host == "" {
		return "%"
	}
	return host
}
