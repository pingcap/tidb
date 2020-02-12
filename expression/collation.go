// Copyright 2020 PingCAP, Inc.
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
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/types"
)

type coercibility struct {
	value Coercibility
	init  bool
}

func (c *coercibility) hasCoercibility() bool {
	return c.init
}

func (c *coercibility) coercibility() Coercibility {
	return c.value
}

// SetCoercibility sets a specified coercibility for this expression.
func (c *coercibility) SetCoercibility(value Coercibility) {
	c.value = value
	c.init = true
}

// CollationExpr contains all interfaces about dealing with collation.
type CollationExpr interface {
	// Coercibility returns the coercibility value which is used to check collations.
	Coercibility() Coercibility

	// SetCoercibility sets a specified coercibility for this expression.
	SetCoercibility(val Coercibility)
}

// Coercibility values are used to check whether the collation of one item can be coerced to
// the collation of other. See https://dev.mysql.com/doc/refman/8.0/en/charset-collation-coercibility.html
type Coercibility int

const (
	// CoercibilityIgnorable: NULL or an expression that is derived from NULL has a coercibility of 6.
	CoercibilityIgnorable Coercibility = 6
	// CoercibilityNumeric: The collation of a numeric or temporal value has a coercibility of 5.
	CoercibilityNumeric Coercibility = 5
	// CoercibilityCoercible: The collation of a literal has a coercibility of 4.
	CoercibilityCoercible Coercibility = 4
	// CoercibilitySysconst: A “system constant” (the string returned by functions such as USER() or VERSION()) has a coercibility of 3.
	CoercibilitySysconst Coercibility = 3
	// CoercibilityImplicit: The collation of a column or a stored routine parameter or local variable has a coercibility of 2.
	CoercibilityImplicit Coercibility = 2
	// CoercibilityNone: The concatenation of two strings with different collations has a coercibility of 1.
	CoercibilityNone Coercibility = 1
	// CoercibilityExplicit: An explicit COLLATE clause has a coercibility of 0 (not coercible at all).
	CoercibilityExplicit Coercibility = 0
)

var (
	sysConstFuncs = map[string]struct{}{
		ast.User:        {},
		ast.Version:     {},
		ast.Database:    {},
		ast.CurrentRole: {},
		ast.CurrentUser: {},
	}
)

func deriveCoercibilityForScarlarFunc(sf *ScalarFunction) Coercibility {
	if _, ok := sysConstFuncs[sf.FuncName.L]; ok {
		return CoercibilitySysconst
	}
	if !types.IsString(sf.RetType.Tp) {
		return CoercibilityNumeric
	}
	coer := CoercibilityIgnorable
	for _, arg := range sf.GetArgs() {
		if arg.Coercibility() < coer {
			coer = arg.Coercibility()
		}
	}
	return coer
}

func deriveCoercibilityForConstant(c *Constant) Coercibility {
	if c.Value.IsNull() {
		return CoercibilityIgnorable
	} else if !types.IsString(c.RetType.Tp) {
		return CoercibilityNumeric
	}
	return CoercibilityCoercible
}

func deriveCoercibilityForColumn(c *Column) Coercibility {
	if !types.IsString(c.RetType.Tp) {
		return CoercibilityNumeric
	}
	return CoercibilityImplicit
}
