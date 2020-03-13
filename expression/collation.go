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
	"strings"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
)

type collationInfo struct {
	coer     Coercibility
	coerInit bool

	charset   string
	collation string
	flen      int
}

func (c *collationInfo) HasCoercibility() bool {
	return c.coerInit
}

func (c *collationInfo) Coercibility() Coercibility {
	return c.coer
}

// SetCoercibility implements CollationInfo SetCoercibility interface.
func (c *collationInfo) SetCoercibility(val Coercibility) {
	c.coer = val
	c.coerInit = true
}

func (c *collationInfo) SetCharsetAndCollation(chs, coll string, flen int) {
	c.charset, c.collation, c.flen = chs, coll, flen
}

func (c *collationInfo) CharsetAndCollation(ctx sessionctx.Context) (string, string, int) {
	if c.charset != "" || c.collation != "" {
		return c.charset, c.collation, c.flen
	}

	if ctx != nil && ctx.GetSessionVars() != nil {
		c.charset, c.collation = ctx.GetSessionVars().GetCharsetInfo()
	}
	if c.charset == "" || c.collation == "" {
		c.charset, c.collation = charset.GetDefaultCharsetAndCollate()
	}
	c.flen = types.UnspecifiedLength
	return c.charset, c.collation, c.flen
}

// CollationInfo contains all interfaces about dealing with collation.
type CollationInfo interface {
	// HasCoercibility returns if the Coercibility value is initialized.
	HasCoercibility() bool

	// Coercibility returns the coercibility value which is used to check collations.
	Coercibility() Coercibility

	// SetCoercibility sets a specified coercibility for this expression.
	SetCoercibility(val Coercibility)

	// CharsetAndCollation ...
	CharsetAndCollation(ctx sessionctx.Context) (string, string, int)

	// SetCharsetAndCollation ...
	SetCharsetAndCollation(chs, coll string, flen int)
}

// Coercibility values are used to check whether the collation of one item can be coerced to
// the collation of other. See https://dev.mysql.com/doc/refman/8.0/en/charset-collation-coercibility.html
type Coercibility int

const (
	// CoercibilityExplicit is derived from an explicit COLLATE clause.
	CoercibilityExplicit Coercibility = 0
	// CoercibilityNone is derived from the concatenation of two strings with different collations.
	CoercibilityNone Coercibility = 1
	// CoercibilityImplicit is derived from a column or a stored routine parameter or local variable.
	CoercibilityImplicit Coercibility = 2
	// CoercibilitySysconst is derived from a “system constant” (the string returned by functions such as USER() or VERSION()).
	CoercibilitySysconst Coercibility = 3
	// CoercibilityCoercible is derived from a literal.
	CoercibilityCoercible Coercibility = 4
	// CoercibilityNumeric is derived from a numeric or temporal value.
	CoercibilityNumeric Coercibility = 5
	// CoercibilityIgnorable is derived from NULL or an expression that is derived from NULL.
	CoercibilityIgnorable Coercibility = 6
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

// DeriveCollationFromExprs derives collation information from these expressions.
func DeriveCollationFromExprs(ctx sessionctx.Context, exprs ...Expression) (dstCharset, dstCollation string, dstFlen int) {
	curCoer := CoercibilityCoercible
	dstCharset, dstCollation = charset.GetDefaultCharsetAndCollate()
	if ctx != nil && ctx.GetSessionVars() != nil {
		dstCharset, dstCollation = ctx.GetSessionVars().GetCharsetInfo()
		if dstCharset == "" || dstCollation == "" {
			dstCharset, dstCollation = charset.GetDefaultCharsetAndCollate()
		}
	}
	dstFlen = types.UnspecifiedLength
	hasStrArg := false
	// see https://dev.mysql.com/doc/refman/8.0/en/charset-collation-coercibility.html
	for _, e := range exprs {
		if e.GetType().EvalType() != types.ETString {
			continue
		}
		hasStrArg = true

		coer := e.Coercibility()
		ft := e.GetType()
		if coer != curCoer {
			if coer < curCoer {
				curCoer, dstCharset, dstCollation, dstFlen = coer, ft.Charset, ft.Collate, ft.Flen
			}
			continue
		}

		isUnicode1 := isUnicodeCharset(ft.Charset)
		isUnicode2 := isUnicodeCharset(dstCharset)
		if isUnicode1 && !isUnicode2 {
			continue
		}
		if (!isUnicode1 && isUnicode2) || // use the unicode charset
			isBinCollation(ft.Collate) { // use the _bin collation
			curCoer, dstCharset, dstCollation, dstFlen = coer, ft.Charset, ft.Collate, ft.Flen
		}
	}
	if !hasStrArg {
		dstCharset, dstCollation, dstFlen = charset.CharsetBin, charset.CollationBin, types.UnspecifiedLength
	}
	return
}

func isUnicodeCharset(charset string) bool {
	charset = strings.ToLower(charset)
	return charset == "utf8" || charset == "utf8mb4"
}

func isBinCollation(collation string) bool {
	return strings.HasSuffix(strings.ToLower(collation), "_bin")
}
