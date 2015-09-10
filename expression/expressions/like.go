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

	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
)

var (
	_ expression.Expression = (*PatternLike)(nil)
)

const (
	patMatch = iota
	patOne
	patAny
)

// PatternLike is the expression for like operator, e.g, expr like "%123%"
type PatternLike struct {
	// Expr is the expression to be checked.
	Expr expression.Expression
	// Pattern is the like expression.
	Pattern  expression.Expression
	patChars []byte
	patTypes []byte
	// Not is true, the expression is "not like".
	Not bool
}

// Clone implements the Expression Clone interface.
func (p *PatternLike) Clone() (expression.Expression, error) {
	expr, err := p.Expr.Clone()
	if err != nil {
		return nil, err
	}

	pattern, err := p.Pattern.Clone()
	if err != nil {
		return nil, err
	}

	return &PatternLike{
		Expr:     expr,
		Pattern:  pattern,
		patChars: p.patChars,
		patTypes: p.patTypes,
		Not:      p.Not,
	}, nil
}

// IsStatic implements the Expression IsStatic interface.
func (p *PatternLike) IsStatic() bool {
	return p.Expr.IsStatic() && p.Pattern.IsStatic()
}

// String implements the Expression String interface.
func (p *PatternLike) String() string {
	return fmt.Sprintf("%s LIKE %s", p.Expr, p.Pattern)
}

// Eval implements the Expression Eval interface.
func (p *PatternLike) Eval(ctx context.Context, args map[interface{}]interface{}) (v interface{}, err error) {
	expr, err := p.Expr.Eval(ctx, args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if expr == nil {
		return nil, nil
	}
	sexpr, ok := expr.(string)
	if !ok {
		return nil, errors.Errorf("non-string expression.Expression in LIKE: %v (Value of type %T)", expr, expr)
	}
	// We need to compile pattern if it has not been compiled or it is not static.
	var needCompile = len(p.patChars) == 0 || !p.Pattern.IsStatic()
	if needCompile {
		pattern, err := p.Pattern.Eval(ctx, args)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if pattern == nil {
			return nil, nil
		}
		spattern, ok := pattern.(string)
		if !ok {
			return nil, errors.Errorf("non-string pattern in LIKE: %v (Value of type %T)", pattern, pattern)
		}

		p.patChars, p.patTypes = compilePattern(spattern)
	}

	match := doMatch(sexpr, p.patChars, p.patTypes)
	if p.Not {
		return !match, nil
	}
	return match, nil
}

// handle escapes and wild cards convert pattern characters and pattern types,
func compilePattern(pattern string) (patChars, patTypes []byte) {
	var lastAny bool
	patChars = make([]byte, len(pattern))
	patTypes = make([]byte, len(pattern))
	patLen := 0
	for i := 0; i < len(pattern); i++ {
		var tp byte
		var c = pattern[i]
		switch c {
		case '\\':
			lastAny = false
			tp = patMatch
			if i < len(pattern)-1 {
				i++
				c = pattern[i]
				if c == '\\' || c == '_' || c == '%' {
					// valid escape.
				} else {
					// invalid escape, fall back to literal back slash
					i--
					c = '\\'
				}
			}
		case '_':
			lastAny = false
			tp = patOne
		case '%':
			if lastAny {
				continue
			}
			lastAny = true
			tp = patAny
		default:
			lastAny = false
			tp = patMatch
		}
		patChars[patLen] = c
		patTypes[patLen] = tp
		patLen++
	}
	for i := 0; i < patLen-1; i++ {
		if (patTypes[i] == patAny) && (patTypes[i+1] == patOne) {
			patTypes[i] = patOne
			patTypes[i+1] = patAny
		}
	}
	patChars = patChars[:patLen]
	patTypes = patTypes[:patLen]
	return
}

const caseDiff = 'a' - 'A'

func matchByteCI(a, b byte) bool {
	if a == b {
		return true
	}
	if a >= 'a' && a <= 'z' && a-caseDiff == b {
		return true
	}
	return a >= 'A' && a <= 'Z' && a+caseDiff == b
}

func doMatch(str string, patChars, patTypes []byte) bool {
	var sIdx int
	for i := 0; i < len(patChars); i++ {
		switch patTypes[i] {
		case patMatch:
			if sIdx >= len(str) || !matchByteCI(str[sIdx], patChars[i]) {
				return false
			}
			sIdx++
		case patOne:
			sIdx++
			if sIdx > len(str) {
				return false
			}
		case patAny:
			i++
			if i == len(patChars) {
				return true
			}
			for sIdx < len(str) {
				if matchByteCI(patChars[i], str[sIdx]) && doMatch(str[sIdx:], patChars[i:], patTypes[i:]) {
					return true
				}
				sIdx++
			}
			return false
		}
	}
	return sIdx == len(str)
}
