// Copyright 2013 The ql Authors. All rights reserved.
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

package expression

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression/builtin"

	"github.com/pingcap/tidb/model"
)

var (
	_ Expression = (*Ident)(nil)
)

const (
	// IdentReferSelectList means the identifier reference is in select list.
	IdentReferSelectList = 1
	// IdentReferFromTable means the identifier reference is in FROM table.
	IdentReferFromTable = 2
)

// Ident is the identifier expression.
type Ident struct {
	// model.CIStr contains origin identifier name and its lowercase name.
	model.CIStr

	// ReferScope means where the identifer reference is, select list or from.
	ReferScope int

	// ReferIndex is the index to get the identifer data.
	ReferIndex int
}

// Clone implements the Expression Clone interface.
func (i *Ident) Clone() Expression {
	newI := *i
	return &newI
}

// IsStatic implements the Expression IsStatic interface, always returns false.
func (i *Ident) IsStatic() bool {
	return false
}

// String implements the Expression String interface.
func (i *Ident) String() string {
	return i.O
}

// Equal checks equation with another Ident expression using lowercase identifier name.
func (i *Ident) Equal(x *Ident) bool {
	return i.L == x.L
}

// Eval implements the Expression Eval interface.
func (i *Ident) Eval(ctx context.Context, args map[interface{}]interface{}) (v interface{}, err error) {
	if _, ok := args[builtin.ExprEvalArgAggEmpty]; ok {
		// select c1, max(c1) from t where c1 = null, must get "NULL", "NULL" for empty table
		return nil, nil
	}

	// TODO: we will unify ExprEvalIdentReferFunc and ExprEvalIdentFunc later,
	// now just put them here for refactor step by step.
	if f, ok := args[ExprEvalIdentReferFunc]; ok {
		if got, ok := f.(func(string, int, int) (interface{}, error)); ok {
			return got(i.L, i.ReferScope, i.ReferIndex)
		}
	}

	if f, ok := args[ExprEvalIdentFunc]; ok {
		if got, ok := f.(func(string) (interface{}, error)); ok {
			return got(i.L)
		}
	}

	// defer func() { log.Errorf("Ident %q -> %v %v", i.S, v, err) }()
	v, ok := args[i.L]
	if !ok {
		err = errors.Errorf("unknown field %s %v", i.O, args)
	}
	return
}

// Accept implements Expression Accept interface.
func (i *Ident) Accept(v Visitor) (Expression, error) {
	return v.VisitIdent(i)
}
