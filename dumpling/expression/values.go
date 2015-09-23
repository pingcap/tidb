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

package expression

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"

	"github.com/pingcap/tidb/model"
)

var (
	_ Expression = (*Values)(nil)
)

// Values is the expression used in INSERT VALUES
type Values struct {
	// model.CIStr is column name.
	model.CIStr
}

// Clone implements the Expression Clone interface.
func (i *Values) Clone() Expression {
	newI := *i
	return &newI
}

// IsStatic implements the Expression IsStatic interface, always returns false.
func (i *Values) IsStatic() bool {
	return false
}

// String implements the Expression String interface.
func (i *Values) String() string {
	return i.O
}

// Equal check equation with another x Values using lowercase column name.
func (i *Values) Equal(x *Values) bool {
	return i.L == x.L
}

// Eval implements the Expression Eval interface.
// See: https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
func (i *Values) Eval(ctx context.Context, args map[interface{}]interface{}) (v interface{}, err error) {
	if f, ok := args[ExprEvalValuesFunc]; ok {
		if got, ok := f.(func(string) (interface{}, error)); ok {
			return got(i.L)
		}
	}
	v, ok := args[i.L]
	if !ok {
		err = errors.Errorf("unknown field %s %v", i.O, args)
	}
	return
}

// Accept implements Expression Accept interface.
func (i *Values) Accept(v Visitor) (Expression, error) {
	return v.VisitValues(i)
}
