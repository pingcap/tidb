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

package expressions

import (
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
)

// ExistsSubQuery is the expression for "exists (select ...)".
// https://dev.mysql.com/doc/refman/5.7/en/exists-and-not-exists-subqueries.html
type ExistsSubQuery struct {
	// Sel is the sub query.
	Sel *SubQuery
	// Not is true, the expression is "not exists".
	Not bool
}

// Clone implements the Expression Clone interface.
func (es *ExistsSubQuery) Clone() (expression.Expression, error) {
	return nil, nil
}

// IsStatic implements the Expression IsStatic interface.
func (es *ExistsSubQuery) IsStatic() bool {
	return es.Sel.IsStatic()
}

// String implements the Expression String interface.
func (es *ExistsSubQuery) String() string {
	return ""
}

// Eval implements the Expression Eval interface.
func (es *ExistsSubQuery) Eval(ctx context.Context, args map[interface{}]interface{}) (interface{}, error) {
	return nil, nil
}

// NewExistsSubQuery creates a ExistsSubQuery object.
func NewExistsSubQuery(sel *SubQuery, not bool) *ExistsSubQuery {
	return &ExistsSubQuery{Sel: sel, Not: not}
}
