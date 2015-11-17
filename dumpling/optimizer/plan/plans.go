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

package plan

import (
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/model"
)

// TableScan represents a table scan plan.
type TableScan struct {
	basePlan

	Table *model.TableInfo
	Desc  bool
}

// Accept implements Plan Accept interface.
func (p *TableScan) Accept(v Visitor) (Plan, bool) {
	np, _ := v.Enter(p)
	return v.Leave(np)
}

type bound int

const (
	minNotNullVal bound = 0
	maxVal        bound = 1
)

// String implements fmt.Stringer interface.
func (b bound) String() string {
	if b == minNotNullVal {
		return "-inf"
	} else if b == maxVal {
		return "+inf"
	}
	return ""
}

// IndexRange represents an index range scan plan.
type IndexRange struct {
	basePlan
	// seekVal is different from lowVal, it is casted from lowVal and
	// must be less than or equal to lowVal, used to seek the index.
	SeekVal     interface{}
	LowVal      interface{}
	LowExclude  bool
	HighVal     interface{}
	HighExclude bool
}

// Accept implements Plan Accept interface.
func (p *IndexRange) Accept(v Visitor) (Plan, bool) {
	np, _ := v.Enter(p)
	return v.Leave(np)
}

// IndexScan represents an index scan plan.
type IndexScan struct {
	basePlan

	// The index used.
	Index *model.IndexInfo

	// The table to lookup.
	Table *model.TableInfo

	// Ordered and non-overlapping ranges to be scanned.
	Ranges []*IndexRange
}

// Accept implements Plan Accept interface.
func (p *IndexScan) Accept(v Visitor) (Plan, bool) {
	np, _ := v.Enter(p)
	return v.Leave(np)
}

// Filter represents a filter plan.
type Filter struct {
	basePlan

	Src Plan
	// Originally the WHERE or ON condition is parsed into a single expression,
	// but after we converted to CNF(Conjunctive normal form), it can be
	// split into a list of AND conditions.
	Conditions []ast.ExprNode
}

// Accept implements Plan Accept interface.
func (p *Filter) Accept(v Visitor) (Plan, bool) {
	np, skip := v.Enter(p)
	if skip {
		v.Leave(np)
	}
	p = np.(*Filter)
	var ok bool
	p.Src, ok = p.Src.Accept(v)
	if !ok {
		return p, false
	}
	return v.Leave(p)
}

// SetLimit implements Plan SetLimit interface.
func (p *Filter) SetLimit(limit float64) {
	p.limit = limit
	p.Src.SetLimit(limit)
}

// SelectLock represents a select lock plan.
type SelectLock struct {
	basePlan

	Src  Plan
	Lock ast.SelectLockType
}

// Accept implements Plan Accept interface.
func (p *SelectLock) Accept(v Visitor) (Plan, bool) {
	np, skip := v.Enter(p)
	if skip {
		v.Leave(np)
	}
	p = np.(*SelectLock)
	var ok bool
	p.Src, ok = p.Src.Accept(v)
	if !ok {
		return p, false
	}
	return v.Leave(p)
}

// SetLimit implements Plan SetLimit interface.
func (p *SelectLock) SetLimit(limit float64) {
	p.limit = limit
	p.Src.SetLimit(p.limit)
}

// SelectFields represents a select fields plan.
type SelectFields struct {
	basePlan

	Src Plan
}

// Accept implements Plan Accept interface.
func (p *SelectFields) Accept(v Visitor) (Plan, bool) {
	np, skip := v.Enter(p)
	if skip {
		v.Leave(np)
	}
	p = np.(*SelectFields)
	if p.Src != nil {
		var ok bool
		p.Src, ok = p.Src.Accept(v)
		if !ok {
			return p, false
		}
	}
	return v.Leave(p)
}

// SetLimit implements Plan SetLimit interface.
func (p *SelectFields) SetLimit(limit float64) {
	p.limit = limit
	if p.Src != nil {
		p.Src.SetLimit(limit)
	}
}

// Sort represents a sorting plan.
type Sort struct {
	basePlan

	Src     Plan
	ByItems []*ast.ByItem
	// If the source is already in the same order, the sort process can be by passed.
	// It depends on the Src plan, so if the Src plan has been modified, Bypass needs
	// to be recalculated.
	Bypass bool
}

// Accept implements Plan Accept interface.
func (p *Sort) Accept(v Visitor) (Plan, bool) {
	np, skip := v.Enter(p)
	if skip {
		v.Leave(np)
	}
	p = np.(*Sort)
	var ok bool
	p.Src, ok = p.Src.Accept(v)
	if !ok {
		return p, false
	}
	return v.Leave(p)
}

// SetLimit implements Plan SetLimit interface.
// It set the Src limit only if it is bypassed.
// Bypass has to be determined before this get called.
func (p *Sort) SetLimit(limit float64) {
	p.limit = limit
	if p.Bypass {
		p.Src.SetLimit(limit)
	}
}

// Limit represents offset and limit plan.
type Limit struct {
	basePlan

	Src    Plan
	Offset uint64
	Count  uint64
}

// Accept implements Plan Accept interface.
func (p *Limit) Accept(v Visitor) (Plan, bool) {
	np, skip := v.Enter(p)
	if skip {
		v.Leave(np)
	}
	p = np.(*Limit)
	var ok bool
	p.Src, ok = p.Src.Accept(v)
	if !ok {
		return p, false
	}
	return v.Leave(p)
}

// SetLimit implements Plan SetLimit interface.
// As Limit itself determine the real limit,
// We just ignore the input, and set the real limit.
func (p *Limit) SetLimit(limit float64) {
	p.limit = float64(p.Offset + p.Count)
	p.Src.SetLimit(p.limit)
}
