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

package plans

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/kv/memkv"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/util/format"
	"github.com/pingcap/tidb/util/types"
)

var (
	_ plan.Plan = (*UnionPlan)(nil)
)

// UnionPlan handles UNION query, merges results from different plans.
type UnionPlan struct {
	Srcs      []plan.Plan
	Distincts []bool
	RFields   []*field.ResultField
	rows      []*plan.Row
	cursor    int
}

// Explain implements plan.Plan Explain interface.
func (p *UnionPlan) Explain(w format.Formatter) {
	// TODO: Finish this
}

// GetFields implements plan.Plan GetFields interface.
func (p *UnionPlan) GetFields() []*field.ResultField {
	return p.RFields
}

// Filter implements plan.Plan Filter interface.
func (p *UnionPlan) Filter(ctx context.Context, expr expression.Expression) (plan.Plan, bool, error) {
	return p, false, nil
}

// Next implements plan.Plan Next interface.
func (p *UnionPlan) Next(ctx context.Context) (row *plan.Row, err error) {
	if p.rows == nil {
		err = p.fetchAll(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	if p.cursor == len(p.rows) {
		return
	}
	row = p.rows[p.cursor]
	p.cursor++
	return
}

func (p *UnionPlan) fetchAll(ctx context.Context) error {
	if len(p.Srcs) == 0 {
		return nil
	}
	t, err := memkv.CreateTemp(true)
	if err != nil {
		return err
	}

	defer func() {
		if derr := t.Drop(); derr != nil && err == nil {
			err = derr
		}
	}()

	// Fetch results of the first select statement.
	src := p.Srcs[0]
	for {
		var row *plan.Row
		row, err = src.Next(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		if row == nil {
			break
		}
		p.rows = append(p.rows, row)
		err = t.Set(row.Data, []interface{}{true})
		if err != nil {
			return errors.Trace(err)
		}
	}

	// Update return result types
	// e,g, for select 'abc' union select 'a', we can only get result type after
	// executing first select.
	for i, f := range p.RFields {
		if f.Col.FieldType.Tp == 0 {
			f.Col.FieldType = src.GetFields()[i].Col.FieldType
		}
	}

	// Fetch results of the following select statements.
	for i := range p.Distincts {
		err = p.fetchSrc(ctx, i, t)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (p *UnionPlan) fetchSrc(ctx context.Context, i int, t memkv.Temp) error {
	src := p.Srcs[i+1]
	distinct := p.Distincts[i]

	rfs := p.GetFields()
	if len(src.GetFields()) != len(rfs) {
		return errors.New("The used SELECT statements have a different number of columns")
	}
	for {
		row, err := src.Next(ctx)
		if row == nil || err != nil {
			return errors.Trace(err)
		}

		srcRfs := src.GetFields()
		for i := range row.Data {
			// The column value should be casted as the same type of the first select statement in corresponding position
			srcRf := srcRfs[i]
			rf := rfs[i]
			/*
			 * The lengths of the columns in the UNION result take into account the values retrieved by all of the SELECT statements
			 * SELECT REPEAT('a',1) UNION SELECT REPEAT('b',10);
			 * +---------------+
			 * | REPEAT('a',1) |
			 * +---------------+
			 * | a             |
			 * | bbbbbbbbbb    |
			 * +---------------+
			 */
			if srcRf.Flen > rf.Col.Flen {
				rf.Col.Flen = srcRf.Col.Flen
			}
			// For select nul union select "abc", we should not convert "abc" to nil.
			// And the result field type should be VARCHAR.
			if rf.Col.FieldType.Tp > 0 && rf.Col.FieldType.Tp != mysql.TypeNull {
				row.Data[i], err = types.Convert(row.Data[i], &rf.Col.FieldType)
			} else {
				// First select result doesn't contain enough type information, e,g, select null union select 1.
				// We cannot get the proper data type for select null.
				// Now we just use the first correct return data types with following select.
				// TODO: Try to merge all data types for all select like select null union select 1 union select "abc"
				if tp := srcRf.Col.FieldType.Tp; tp > 0 {
					rf.Col.FieldType.Tp = tp
				}
			}

			if err != nil {
				return errors.Trace(err)
			}
		}

		if distinct {
			// distinct union, check duplicate
			v, getErr := t.Get(row.Data)
			if getErr != nil {
				return errors.Trace(getErr)
			}
			if len(v) > 0 {
				// Find duplicate, ignore it
				continue
			}
		}
		p.rows = append(p.rows, row)
		if err := t.Set(row.Data, []interface{}{true}); err != nil {
			return errors.Trace(err)
		}
	}
}

// Close implements plan.Plan Close interface.
func (p *UnionPlan) Close() error {
	for _, src := range p.Srcs {
		src.Close()
	}
	p.rows = nil
	p.cursor = 0
	return nil
}
