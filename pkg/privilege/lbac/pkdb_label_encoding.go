// Copyright 2025 PingCAP, Inc.
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

	"github.com/pingcap/tidb/pkg/parser/ast"
	lbacmodel "github.com/pingcap/tidb/pkg/privilege/lbac/model"
)

// EncodeLabelComponents encodes a label definition into compact binary form.
func EncodeLabelComponents(components map[string]*Component, policy *Policy, labelComponents map[string][]string) ([]byte, error) {
	if policy == nil {
		return nil, ErrPolicyNotFound
	}
	label := lbacmodel.Label{
		Version:    lbacmodel.CurrentVersion,
		Components: make([]lbacmodel.Component, 0, len(policy.ComponentNames)),
	}
	for i, name := range policy.ComponentNames {
		component := components[name]
		if component == nil {
			return nil, ErrComponentNotFound
		}
		values := labelComponents[name]
		if len(values) == 0 {
			return nil, ErrInvalidComponentForLabel
		}
		componentID := uint8(i + 1)
		typ, err := componentTypeToModel(component.Type)
		if err != nil {
			return nil, err
		}
		value, err := encodeComponentValue(component, typ, values)
		if err != nil {
			return nil, err
		}
		label.Components = append(label.Components, lbacmodel.Component{
			ID:    componentID,
			Type:  typ,
			Value: value,
		})
	}
	return lbacmodel.EncodeLabel(label)
}

func encodeComponentValue(component *Component, typ lbacmodel.ComponentType, values []string) (lbacmodel.ComponentValue, error) {
	switch typ {
	case lbacmodel.ComponentTypeArray:
		if len(values) != 1 {
			return nil, ErrInvalidComponentValue
		}
		ordinal, err := component.valueOrdinalFor(values[0])
		if err != nil {
			return nil, err
		}
		return lbacmodel.ArrayValue{Ordinal: ordinal}, nil
	case lbacmodel.ComponentTypeSet:
		ids, err := component.setValueIDs(values)
		if err != nil {
			return nil, err
		}
		return lbacmodel.SetValue{Values: ids}, nil
	case lbacmodel.ComponentTypeTree:
		if len(values) != 1 {
			return nil, ErrInvalidComponentValue
		}
		tr, err := component.treeRangeFor(values[0])
		if err != nil {
			return nil, err
		}
		return lbacmodel.TreeValue{In: tr.In, Out: tr.Out}, nil
	default:
		return nil, ErrInvalidComponentType
	}
}

func componentTypeToModel(componentType ast.LBACComponentType) (lbacmodel.ComponentType, error) {
	switch componentType {
	case ast.LBACComponentTypeArray:
		return lbacmodel.ComponentTypeArray, nil
	case ast.LBACComponentTypeSet:
		return lbacmodel.ComponentTypeSet, nil
	case ast.LBACComponentTypeTree:
		return lbacmodel.ComponentTypeTree, nil
	default:
		return 0, ErrInvalidComponentType
	}
}

func (c *Component) valueOrdinalFor(value string) (uint64, error) {
	if value == "" {
		return 0, ErrInvalidComponentValue
	}
	ordinals, err := c.ensureValueOrdinals()
	if err != nil {
		return 0, err
	}
	ordinal, ok := ordinals[value]
	if !ok {
		return 0, ErrInvalidComponentValue
	}
	return ordinal, nil
}

func (c *Component) setValueIDs(values []string) ([]uint64, error) {
	if len(values) == 0 {
		return nil, ErrInvalidComponentValue
	}
	ordinals, err := c.ensureValueOrdinals()
	if err != nil {
		return nil, err
	}
	ids := make([]uint64, 0, len(values))
	seen := make(map[uint64]struct{}, len(values))
	for _, value := range values {
		ordinal, ok := ordinals[value]
		if !ok {
			return nil, ErrInvalidComponentValue
		}
		if _, exists := seen[ordinal]; exists {
			return nil, ErrDuplicateComponentValue
		}
		seen[ordinal] = struct{}{}
		ids = append(ids, ordinal)
	}
	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})
	return ids, nil
}

func (c *Component) treeRangeFor(value string) (lbacmodel.TreeValue, error) {
	if value == "" {
		return lbacmodel.TreeValue{}, ErrInvalidComponentValue
	}
	ranges, err := c.ensureTreeRanges()
	if err != nil {
		return lbacmodel.TreeValue{}, err
	}
	tr, ok := ranges[value]
	if !ok {
		return lbacmodel.TreeValue{}, ErrInvalidComponentValue
	}
	return tr, nil
}

func (c *Component) ensureValueOrdinals() (map[string]uint64, error) {
	if c.valueOrdinal != nil {
		return c.valueOrdinal, nil
	}
	if len(c.Values) == 0 {
		return nil, ErrInvalidComponentValue
	}
	ordinals := make(map[string]uint64, len(c.Values))
	for i, value := range c.Values {
		if value == "" {
			return nil, ErrInvalidComponentValue
		}
		if _, exists := ordinals[value]; exists {
			return nil, ErrDuplicateComponentValue
		}
		ordinals[value] = uint64(i)
	}
	c.valueOrdinal = ordinals
	return ordinals, nil
}

func (c *Component) ensureTreeRanges() (map[string]lbacmodel.TreeValue, error) {
	if c.treeRange != nil {
		return c.treeRange, nil
	}
	if len(c.TreeParent) == 0 {
		return nil, ErrInvalidComponentValue
	}
	ranges, err := buildTreeRanges(c.TreeParent)
	if err != nil {
		return nil, err
	}
	c.treeRange = ranges
	return ranges, nil
}

func buildTreeRanges(treeParent map[string]string) (map[string]lbacmodel.TreeValue, error) {
	children := make(map[string][]string, len(treeParent))
	roots := make([]string, 0, 1)
	for name, parent := range treeParent {
		if parent == "" {
			roots = append(roots, name)
			continue
		}
		children[parent] = append(children[parent], name)
	}
	if len(roots) != 1 {
		return nil, ErrInvalidComponentValue
	}
	for name := range children {
		sort.Strings(children[name])
	}
	visited := make(map[string]struct{}, len(treeParent))
	ranges := make(map[string]lbacmodel.TreeValue, len(treeParent))
	var cursor uint64
	var walk func(string) error
	walk = func(node string) error {
		if _, ok := visited[node]; ok {
			return ErrInvalidComponentValue
		}
		visited[node] = struct{}{}
		cursor++
		inVal := cursor
		for _, child := range children[node] {
			if err := walk(child); err != nil {
				return err
			}
		}
		cursor++
		outVal := cursor
		ranges[node] = lbacmodel.TreeValue{In: inVal, Out: outVal}
		return nil
	}
	if err := walk(roots[0]); err != nil {
		return nil, err
	}
	if len(visited) != len(treeParent) {
		return nil, ErrInvalidComponentValue
	}
	return ranges, nil
}
