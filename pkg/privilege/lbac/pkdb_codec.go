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
	"encoding/json"
	"sort"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/ast"
)

// ParseComponentValues decodes component_values into raw values and optional tree parents.
func ParseComponentValues(componentType ast.LBACComponentType, raw []byte) ([]string, map[string]string, error) {
	switch componentType {
	case ast.LBACComponentTypeArray, ast.LBACComponentTypeSet:
		var values []string
		if err := json.Unmarshal(raw, &values); err != nil {
			return nil, nil, errors.Wrap(err, "failed to parse component values")
		}
		return values, nil, nil
	case ast.LBACComponentTypeTree:
		var nodes []TreeNodeSpec
		if err := json.Unmarshal(raw, &nodes); err == nil {
			values := make([]string, 0, len(nodes))
			treeParent := make(map[string]string, len(nodes))
			for _, node := range nodes {
				values = append(values, node.Name)
				treeParent[node.Name] = node.Parent
			}
			return values, treeParent, nil
		}
		var values []string
		if err := json.Unmarshal(raw, &values); err != nil {
			return nil, nil, errors.Wrap(err, "failed to parse tree component values")
		}
		return values, nil, nil
	default:
		return nil, nil, ErrInvalidComponentType
	}
}

// ParseLabelComponents decodes label components into a canonical map.
func ParseLabelComponents(raw []byte) (map[string][]string, error) {
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, errors.Wrap(err, "failed to parse label components")
	}
	components := make(map[string][]string, len(payload))
	for name, value := range payload {
		switch v := value.(type) {
		case string:
			components[name] = []string{v}
		case []any:
			values := make([]string, 0, len(v))
			for _, entry := range v {
				str, ok := entry.(string)
				if !ok || str == "" {
					return nil, ErrInvalidComponentValue
				}
				values = append(values, str)
			}
			components[name] = values
		default:
			return nil, ErrInvalidComponentValue
		}
	}
	return components, nil
}

func buildLabelString(components map[string]*Component, policy *Policy, labelComponents map[string][]string) (string, error) {
	if policy == nil {
		return "", ErrPolicyNotFound
	}
	parts := make([]string, 0, len(policy.ComponentNames))
	for _, name := range policy.ComponentNames {
		component := components[name]
		if component == nil {
			return "", ErrComponentNotFound
		}
		values := labelComponents[name]
		if len(values) == 0 {
			return "", ErrInvalidComponentForLabel
		}
		switch component.Type {
		case ast.LBACComponentTypeSet:
			values = normalizeSetValues(values)
			parts = append(parts, strings.Join(values, ","))
		case ast.LBACComponentTypeArray, ast.LBACComponentTypeTree:
			if len(values) != 1 {
				return "", ErrInvalidComponentValue
			}
			parts = append(parts, values[0])
		default:
			return "", ErrInvalidComponentType
		}
	}
	return strings.Join(parts, "|"), nil
}

func normalizeSetValues(values []string) []string {
	normalized := make([]string, 0, len(values))
	for _, value := range values {
		if value != "" {
			normalized = append(normalized, value)
		}
	}
	sort.Strings(normalized)
	return normalized
}
