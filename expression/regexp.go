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
	"fmt"
	"regexp"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/util/types"
)

var (
	_ Expression = (*PatternRegexp)(nil)
)

// PatternRegexp is the pattern expression for pattern match.
// TODO: refactor later.
type PatternRegexp struct {
	// Expr is the expression to be checked.
	Expr Expression
	// Pattern is the expression for pattern.
	Pattern Expression
	// Re is the compiled regexp.
	Re *regexp.Regexp
	// Sexpr is the string for Expr expression.
	Sexpr *string
	// Not is true, the expression is "not rlike",
	Not bool
}

// Clone implements the Expression Clone interface.
func (p *PatternRegexp) Clone() Expression {
	expr := p.Expr.Clone()
	pattern := p.Pattern.Clone()
	return &PatternRegexp{
		Expr:    expr,
		Pattern: pattern,
		Re:      p.Re,
		Sexpr:   p.Sexpr,
		Not:     p.Not,
	}
}

// IsStatic implements the Expression IsStatic interface.
func (p *PatternRegexp) IsStatic() bool {
	return p.Expr.IsStatic() && p.Pattern.IsStatic()
}

// String implements the Expression String interface.
func (p *PatternRegexp) String() string {
	return fmt.Sprintf("%s RLIKE %s", p.Expr, p.Pattern)
}

// Eval implements the Expression Eval interface.
func (p *PatternRegexp) Eval(ctx context.Context, args map[interface{}]interface{}) (v interface{}, err error) {
	var sexpr string
	switch {
	case p.Sexpr != nil:
		sexpr = *p.Sexpr
	default:
		expr, err := p.Expr.Eval(ctx, args)
		if err != nil {
			return nil, err
		}

		if types.IsNil(expr) {
			return nil, nil
		}

		sexpr, err = types.ToString(expr)
		if err != nil {
			return nil, errors.Errorf("non-string Expression in LIKE: %v (Value of type %T)", expr, expr)
		}

		if p.Expr.IsStatic() {
			p.Sexpr = new(string)
			*p.Sexpr = sexpr
		}
	}

	re := p.Re
	if re == nil {
		pattern, err := p.Pattern.Eval(ctx, args)
		if err != nil {
			return nil, err
		}

		if types.IsNil(pattern) {
			return nil, nil
		}

		spattern, err := types.ToString(pattern)
		if err != nil {
			return nil, errors.Errorf("non-string pattern in LIKE: %v (Value of type %T)", pattern, pattern)
		}

		if re, err = regexp.Compile(spattern); err != nil {
			return nil, err
		}

		if p.Pattern.IsStatic() {
			p.Re = re
		}
	}
	match := re.MatchString(sexpr)
	if p.Not {
		return !match, nil
	}
	return match, nil
}

// Accept implements Expression Accept interface.
func (p *PatternRegexp) Accept(v Visitor) (Expression, error) {
	return v.VisitPatternRegexp(p)
}
