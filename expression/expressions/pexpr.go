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

package expressions

import (
	"fmt"

	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
)

var (
	_ expression.Expression = (*PExpr)(nil)
)

// PExpr is the parenthese expression.
type PExpr struct {
	// Expr is the expression in parenthese.
	Expr expression.Expression
}

// Clone implements the Expression Clone interface.
func (p *PExpr) Clone() (expression.Expression, error) {
	expr, err := p.Expr.Clone()
	if err != nil {
		return nil, err
	}

	return &PExpr{Expr: expr}, nil
}

// IsStatic implements the Expression IsStatic interface.
func (p *PExpr) IsStatic() bool {
	return p.Expr.IsStatic()
}

// String implements the Expression String interface.
func (p *PExpr) String() string {
	return fmt.Sprintf("(%s)", p.Expr)
}

// Eval implements the Expression Eval interface.
func (p *PExpr) Eval(ctx context.Context, args map[interface{}]interface{}) (v interface{}, err error) {
	return p.Expr.Eval(ctx, args)
}
