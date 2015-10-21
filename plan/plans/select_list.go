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
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/model"
)

// SelectList contains real select list defined in select statement which will be output to client
// and hidden list which will just be used internally for order by, having clause, etc, why?
// After we do where phase in select, the left flow are group by -> having -> select fields -> order by -> limit -> final.
// for MySQL, order by may use values not in select fields, e.g select c1 from t order by c2, to support this,
// we should add extra fields in select list, and we will use HiddenOffset to control these fields not to be output.
type SelectList struct {
	Fields       []*field.Field
	ResultFields []*field.ResultField
	AggFields    map[int]struct{}

	// HiddenFieldOffset distinguishes select field list and hidden fields for internal use.
	// We will use this to get select filed list and calculate distinct key.
	HiddenFieldOffset int

	// FromFields is the fields from table.
	FromFields []*field.ResultField
}

func (s *SelectList) updateFields(table string, resultFields []*field.ResultField) {
	// TODO: check database name later.
	for _, v := range resultFields {
		if table == "" || table == v.TableName {
			name := field.JoinQualifiedName("", v.TableName, v.Name)

			f := &field.Field{
				Expr: &expression.Ident{
					CIStr: model.NewCIStr(name),
				},
			}

			s.AddField(f, v.Clone())
		}
	}
}

func createEmptyResultField(f *field.Field) *field.ResultField {
	result := &field.ResultField{}
	// Set origin name
	result.ColumnInfo.Name = model.NewCIStr(f.Expr.String())

	if len(f.AsName) > 0 {
		result.Name = f.AsName
	} else {
		result.Name = result.ColumnInfo.Name.O
	}
	return result
}

// AddField adds Field and ResultField objects to SelectList, and if result is nil,
// constructs a new ResultField.
func (s *SelectList) AddField(f *field.Field, result *field.ResultField) {
	if result == nil {
		result = createEmptyResultField(f)
	}

	s.Fields = append(s.Fields, f)
	s.ResultFields = append(s.ResultFields, result)
}

func (s *SelectList) resolveAggFields() {
	for i, v := range s.Fields {
		if expression.ContainAggregateFunc(v.Expr) {
			s.AggFields[i] = struct{}{}
		}
	}
}

// GetFields returns ResultField.
func (s *SelectList) GetFields() []*field.ResultField {
	return s.ResultFields
}

// UpdateAggFields adds aggregate function resultfield to select result field list.
func (s *SelectList) UpdateAggFields(expr expression.Expression) (expression.Expression, error) {
	// We must add aggregate function to hidden select list
	// and use a position expression to fetch its value later.
	name := strings.ToLower(expr.String())
	index := -1
	for i := 0; i < s.HiddenFieldOffset; i++ {
		// only check origin name, e,g. "select sum(c1) as a from t order by sum(c1)"
		// or "select sum(c1) from t order by sum(c1)"
		if s.ResultFields[i].ColumnInfo.Name.L == name {
			index = i
			break
		}
	}

	if index == -1 {
		f := &field.Field{Expr: expr}
		s.AddField(f, nil)

		pos := len(s.Fields)

		s.AggFields[pos-1] = struct{}{}

		return &expression.Position{N: pos, Name: name}, nil
	}

	// select list has this field, use it directly.
	return &expression.Position{N: index + 1, Name: name}, nil
}

// CheckAmbiguous checks whether an identifier reference is ambiguous or not in select list.
// e,g, "select c1 as a, c2 as a from t group by a" is ambiguous,
// but "select c1 as a, c1 as a from t group by a" is not.
// "select c1 as a, c2 + 1 as a from t group by a" is not ambiguous too,
// If no ambiguous, -1 means expr refers none in select list, else an index for first match.
// CheckAmbiguous will break the check when finding first matching which is not an indentifier,
// or an index for an identifier field in the end, -1 means none found.
func (s *SelectList) CheckAmbiguous(expr expression.Expression) (int, error) {
	if _, ok := expr.(*expression.Ident); !ok {
		return -1, nil
	}

	name := expr.String()
	if field.IsQualifiedName(name) {
		// name is qualified, no need to check
		return -1, nil
	}

	//	select c1 as a, 1 as a, c2 as a from t order by a is not ambiguous.
	//	select c1 as a, c2 as a from t order by a is ambiguous.
	//	select 1 as a, c1 as a from t order by a is not ambiguous.
	//	select c1 as a, sum(c1) as a from t group by a is error.
	//	select c1 as a, 1 as a, sum(c1) as a from t group by a is not error.
	// 	so we will break the check if matching a none identifier field.
	lastIndex := -1
	// only check origin select list, no hidden field.
	for i := 0; i < s.HiddenFieldOffset; i++ {
		if !strings.EqualFold(s.ResultFields[i].Name, name) {
			continue
		}

		if _, ok := s.Fields[i].Expr.(*expression.Ident); !ok {
			// not identfier, return directly.
			return i, nil
		}

		if lastIndex == -1 {
			// first match, continue
			lastIndex = i
			continue
		}

		// check origin name, e,g. "select c1 as c2, c2 from t group by c2" is ambiguous.

		if s.ResultFields[i].ColumnInfo.Name.L != s.ResultFields[lastIndex].ColumnInfo.Name.L {
			return -1, errors.Errorf("refer %s is ambiguous", expr)
		}

		// check table name, e.g, "select t.c1, c1 from t group by c1" is not ambiguous.
		if s.ResultFields[i].TableName != s.ResultFields[lastIndex].TableName {
			return -1, errors.Errorf("refer %s is ambiguous", expr)
		}

		// TODO: check database name if possible.
	}

	return lastIndex, nil
}

// ResolveSelectList gets fields and result fields from selectFields and srcFields,
// including field validity check and wildcard field processing.
func ResolveSelectList(selectFields []*field.Field, srcFields []*field.ResultField) (*SelectList, error) {
	selectList := &SelectList{
		Fields:       make([]*field.Field, 0, len(selectFields)),
		ResultFields: make([]*field.ResultField, 0, len(selectFields)),
		AggFields:    make(map[int]struct{}),
		FromFields:   srcFields,
	}

	wildcardNum := 0
	for _, v := range selectFields {
		// Check metioned field.
		names := expression.MentionedColumns(v.Expr)
		if len(names) == 0 {
			selectList.AddField(v, nil)
			continue
		}

		// Check wildcard field.
		name := names[0]
		table, ok, err := field.CheckWildcardField(name)
		if err != nil {
			return nil, err
		}
		if ok {
			// Check unqualified wildcard field number,
			// like `select *, * from t`.
			if table == "" {
				wildcardNum++
				if wildcardNum > 1 {
					return nil, errors.Errorf("wildcard field exist more than once")
				}
			}

			selectList.updateFields(table, srcFields)
			continue
		}

		// TODO: use fromIdentVisitor to cleanup.
		var result *field.ResultField
		for _, name := range names {
			idx := field.GetResultFieldIndex(name, srcFields)
			if len(idx) > 1 {
				return nil, errors.Errorf("ambiguous field %s", name)
			}

			// TODO: must check in outer query too.
			if len(idx) == 0 {
				return nil, errors.Errorf("unknown field %s", name)
			}
		}

		if _, ok := v.Expr.(*expression.Ident); ok {
			// Field is ident.
			if result, err = field.CloneFieldByName(name, srcFields); err != nil {
				return nil, errors.Trace(err)
			}

			// Use alias name
			if len(v.AsName) > 0 {
				result.Name = v.AsName
			} else {
				// use field identifier name directly, but not contain qualified name.
				// e.g, select t.c will only return c as the column name.
				s := v.Expr.String()
				n := strings.LastIndexByte(s, '.')
				if n == -1 {
					result.Name = s
				} else {
					result.Name = s[n+1:]
				}
			}
		}

		selectList.AddField(v, result)
	}

	selectList.HiddenFieldOffset = len(selectList.Fields)
	selectList.resolveAggFields()

	if selectList.HiddenFieldOffset == 0 {
		return nil, errors.Errorf("invalid empty select fields")
	}

	return selectList, nil
}
