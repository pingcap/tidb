// Copyright 2014 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

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
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/plan/plans"
)

var (
	_ plan.Planner = (*GroupByRset)(nil)
)

// GroupByRset is record set for group by fields.
type GroupByRset struct {
	By         []expression.Expression
	Src        plan.Plan
	SelectList *plans.SelectList
}

type groupByVisitor struct {
	expression.BaseVisitor
	selectList *plans.SelectList
	rootIdent  *expression.Ident
}

func (v *groupByVisitor) checkIdent(i *expression.Ident) (int, error) {
	idx, err := v.selectList.CheckReferAmbiguous(i)
	if err != nil {
		return -1, errors.Errorf("Column '%s' in group statement is ambiguous", i)
	} else if len(idx) == 0 {
		return -1, nil
	}

	for _, index := range idx {
		if _, ok := v.selectList.AggFields[index]; ok {
			return -1, errors.Errorf("Reference '%s' not supported (reference to group function)", i)
		}
	}

	// this identifier may reference multi fields.
	// e.g, select c1 as a, c2 + 1 as a from t group by a,
	// we will use the first one which is not an identifer.
	// so, for select c1 as a, c2 + 1 as a from t group by a, we will use c2 + 1.
	for _, index := range idx {
		if castIdent(v.selectList.Fields[index].Expr) == nil {
			return index, nil
		}
	}

	return idx[0], nil
}

func (v *groupByVisitor) VisitIdent(i *expression.Ident) (expression.Expression, error) {
	// Group by ambiguous rule:
	//	select c1 as a, c2 as a from t group by a is ambiguous
	//	select c1 as a, c2 as a from t group by a + 1 is ambiguous
	//	select c1 as c2, c2 from t group by c2 is ambiguous
	//	select c1 as c2, c2 from t group by c2 + 1 is ambiguous

	var (
		index int
		err   error
	)

	if v.rootIdent == i {
		// The group by is an identifier, we must check it first.
		index, err = v.checkIdent(i)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	// first find this identifier in FROM.
	idx := field.GetResultFieldIndex(i.L, v.selectList.FromFields, field.DefaultFieldFlag)
	if len(idx) > 0 {
		i.ReferScope = expression.IdentReferFromTable
		i.ReferIndex = idx[0]
		return i, nil
	}

	if v.rootIdent != i {
		// This identifier is the part of the group by, check ambiguous here.
		index, err = v.checkIdent(i)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	// try to find in select list, we have got index using checkIdent before.
	if index >= 0 {
		// find in select list
		i.ReferScope = expression.IdentReferSelectList
		i.ReferIndex = index
		return i, nil
	}

	// TODO: check in out query
	// TODO: return unknown field error, but now just return directly.
	// Because this may reference outer query.
	return i, nil
}

func (v *groupByVisitor) VisitCall(c *expression.Call) (expression.Expression, error) {
	ok, err := expression.IsAggregateFunc(c.F)
	if err != nil {
		return nil, errors.Trace(err)
	} else if ok {
		return nil, errors.Errorf("group by cannot contain aggregate function %s", c)
	}

	for i, e := range c.Args {
		c.Args[i], err = e.Accept(v)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	return c, nil
}

// Plan gets GroupByDefaultPlan.
func (r *GroupByRset) Plan(ctx context.Context) (plan.Plan, error) {
	fields := r.SelectList.Fields

	r.SelectList.AggFields = GetAggFields(fields)
	visitor := &groupByVisitor{}
	visitor.BaseVisitor.V = visitor
	visitor.selectList = r.SelectList

	for i, e := range r.By {
		pos, err := castPosition(e, r.SelectList, true)
		if err != nil {
			return nil, errors.Trace(err)
		}

		if pos != nil {
			// use Position expression for the associated field.
			r.By[i] = pos
			continue
		}

		visitor.rootIdent = castIdent(e)
		by, err := e.Accept(visitor)
		if err != nil {
			return nil, errors.Trace(err)
		}
		r.By[i] = by
	}

	return &plans.GroupByDefaultPlan{By: r.By, Src: r.Src,
		SelectList: r.SelectList}, nil
}

func (r *GroupByRset) String() string {
	a := make([]string, len(r.By))
	for i, v := range r.By {
		a[i] = v.String()
	}
	return strings.Join(a, ", ")
}
