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
	"github.com/pingcap/tidb/parser/opcode"
)

// CompareSubQuery is the expression for "expr cmp (select ...)".
type CompareSubQuery struct {
	// L is the left expression
	L expression.Expression
	// Op is the comparison opcode.
	Op opcode.Op
	// R is the sub query for right expression.
	R *SubQuery
	// All is true, we should compare all records in subquery.
	All bool
}

// Clone implements the Expression Clone interface.
func (s *CompareSubQuery) Clone() (expression.Expression, error) {
	return nil, nil
}

// IsStatic implements the Expression IsStatic interface.
func (s *CompareSubQuery) IsStatic() bool {
	return false
}

// String implements the Expression String interface.
func (s *CompareSubQuery) String() string {
	return ""
}

// Eval implements the Expression Eval interface.
func (s *CompareSubQuery) Eval(ctx context.Context, args map[interface{}]interface{}) (interface{}, error) {
	return nil, nil
}

// NewCompareSubQuery creates a CompareSubQuery object.
func NewCompareSubQuery(op opcode.Op, lhs expression.Expression, rhs *SubQuery, all bool) *CompareSubQuery {
	return &CompareSubQuery{
		Op:  op,
		L:   lhs,
		R:   rhs,
		All: all,
	}
}
