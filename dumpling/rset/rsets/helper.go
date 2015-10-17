// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package rsets

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/plan/plans"
)

// GetAggFields gets aggregate fields position map.
func GetAggFields(fields []*field.Field) map[int]struct{} {
	aggFields := map[int]struct{}{}
	for i, v := range fields {
		if expression.ContainAggregateFunc(v.Expr) {
			aggFields[i] = struct{}{}
		}
	}
	return aggFields
}

// HasAggFields checks whether has aggregate field.
func HasAggFields(fields []*field.Field) bool {
	aggFields := GetAggFields(fields)
	return len(aggFields) > 0
}

// castIdent returns an Ident expression if e is or nil.
func castIdent(e expression.Expression) *expression.Ident {
	i, ok := e.(*expression.Ident)
	if !ok {
		return nil
	}
	return i
}

// castPosition returns an group/order by Position expression if e is a number.
func castPosition(e expression.Expression, selectList *plans.SelectList, isGroupBy bool) (*expression.Position, error) {
	v, ok := e.(expression.Value)
	if !ok {
		return nil, nil
	}

	var position int
	switch u := v.Val.(type) {
	case int64:
		position = int(u)
	case uint64:
		position = int(u)
	default:
		return nil, nil
	}

	if position < 1 || position > selectList.HiddenFieldOffset {
		if isGroupBy {
			return nil, errors.Errorf("Unknown column '%d' in 'group statement'", position)
		}
		return nil, errors.Errorf("Unknown column '%d' in 'order clause'", position)
	}

	if isGroupBy {
		index := position - 1
		if _, ok := selectList.AggFields[index]; ok {
			return nil, errors.Errorf("Can't group on '%s'", selectList.Fields[index])
		}
	}

	// use Position expression for the associated field.
	return &expression.Position{N: position}, nil
}
