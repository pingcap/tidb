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

// ClauseType is clause type.
// TODO: export clause type and move to plan?
type ClauseType int

// Clause Types.
const (
	NoneClause ClauseType = iota
	OnClause
	WhereClause
	GroupByClause
	FieldListClause
	HavingClause
	OrderByClause
	UpdateClause
)

func (clause ClauseType) String() string {
	switch clause {
	case OnClause:
		return "on clause"
	case WhereClause:
		return "where clause"
	case GroupByClause:
		return "group statement"
	case FieldListClause:
		return "field list"
	case HavingClause:
		return "having clause"
	case OrderByClause:
		return "order clause"
	case UpdateClause:
		return "update clause"
	}
	return "none"
}

// castPosition returns an group/order by Position expression if e is a number.
func castPosition(e expression.Expression, selectList *plans.SelectList, clause ClauseType) (*expression.Position, error) {
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

	if clause == GroupByClause {
		index := position - 1
		if _, ok := selectList.AggFields[index]; ok {
			return nil, errors.Errorf("Can't group on '%s'", selectList.Fields[index])
		}
	}

	// use Position expression for the associated field.
	return &expression.Position{N: position}, nil
}

func checkIdentAmbiguous(i *expression.Ident, selectList *plans.SelectList, clause ClauseType) (int, error) {
	index, err := selectList.CheckAmbiguous(i)
	if err != nil {
		return -1, errors.Errorf("Column '%s' in %s is ambiguous", i, clause)
	} else if index == -1 {
		return -1, nil
	}

	return index, nil
}

// FromIdentVisitor can only handle identifier which reference FROM table or outer query.
// like in common select list, where or join on condition.
type FromIdentVisitor struct {
	expression.BaseVisitor
	FromFields []*field.ResultField
	Clause     ClauseType
}

// VisitIdent implements Visitor interface.
func (v *FromIdentVisitor) VisitIdent(i *expression.Ident) (expression.Expression, error) {
	idx := field.GetResultFieldIndex(i.L, v.FromFields)
	if len(idx) == 1 {
		i.ReferScope = expression.IdentReferFromTable
		i.ReferIndex = idx[0]
		return i, nil
	} else if len(idx) > 1 {
		return nil, errors.Errorf("Column '%s' in %s is ambiguous", i, v.Clause)
	}

	if v.Clause == OnClause {
		// on clause can't check outer query.
		return nil, errors.Errorf("Unknown column '%s' in '%s'", i, v.Clause)
	}

	// TODO: check in outer query
	return i, nil
}

// NewFromIdentVisitor creates a new FromIdentVisitor.
func NewFromIdentVisitor(fromFields []*field.ResultField, clause ClauseType) *FromIdentVisitor {
	visitor := &FromIdentVisitor{}
	visitor.BaseVisitor.V = visitor
	visitor.FromFields = fromFields
	visitor.Clause = clause

	return visitor
}
