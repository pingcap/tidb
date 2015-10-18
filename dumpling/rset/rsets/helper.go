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
	aggFields := make(map[int]struct{}, len(fields))
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

// TODO: export clause type and move to plan?
type clauseType int

const (
	noneClause clauseType = iota
	onClause
	whereClause
	groupByClause
	fieldListClause
	havingClause
	orderByClause
)

func (clause clauseType) String() string {
	switch clause {
	case onClause:
		return "on clause"
	case fieldListClause:
		return "field list"
	case whereClause:
		return "where clause"
	case groupByClause:
		return "group statement"
	case orderByClause:
		return "order clause"
	case havingClause:
		return "having clause"
	}
	return "none"
}

// castPosition returns an group/order by Position expression if e is a number.
func castPosition(e expression.Expression, selectList *plans.SelectList, clause clauseType) (*expression.Position, error) {
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
		return nil, errors.Errorf("Unknown column '%d' in '%s'", position, clause)
	}

	if clause == groupByClause {
		index := position - 1
		if _, ok := selectList.AggFields[index]; ok {
			return nil, errors.Errorf("Can't group on '%s'", selectList.Fields[index])
		}
	}

	// use Position expression for the associated field.
	return &expression.Position{N: position}, nil
}

func checkIdent(i *expression.Ident, selectList *plans.SelectList, clause clauseType) (int, error) {
	index, err := selectList.GetIndex(i)
	if err != nil {
		return -1, errors.Errorf("Column '%s' in %s is ambiguous", i, clause)
	} else if index == -1 {
		return -1, nil
	}

	if clause == groupByClause {
		// group by can not reference aggregate fields
		if _, ok := selectList.AggFields[index]; ok {
			return -1, errors.Errorf("Reference '%s' not supported (reference to group function)", i)
		}
	}

	return index, nil
}

// fromIdentVisitor can only handle identifier which reference FROM table or outer query.
// like in common select list, where or join on condition.
type fromIdentVisitor struct {
	expression.BaseVisitor
	fromFields []*field.ResultField
	clause     clauseType
}

func (v *fromIdentVisitor) VisitIdent(i *expression.Ident) (expression.Expression, error) {
	idx := field.GetResultFieldIndex(i.L, v.fromFields, field.DefaultFieldFlag)
	if len(idx) == 1 {
		i.ReferScope = expression.IdentReferFromTable
		i.ReferIndex = idx[0]
		return i, nil
	} else if len(idx) > 1 {
		return nil, errors.Errorf("Column '%s' in %s is ambiguous", i, v.clause)
	}

	if v.clause == onClause {
		// on clause can't check outer query.
		return nil, errors.Errorf("Unknown column '%s' in '%s'", i, v.clause)
	}

	// TODO: check in outer query
	return i, nil
}

func newFromIdentVisitor(fromFields []*field.ResultField, clause clauseType) *fromIdentVisitor {
	visitor := &fromIdentVisitor{}
	visitor.BaseVisitor.V = visitor
	visitor.fromFields = fromFields
	visitor.clause = clause

	return visitor
}
