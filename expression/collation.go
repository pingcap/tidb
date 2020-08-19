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
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
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

func (c *collationInfo) SetCharsetAndCollation(chs, coll string) {
	c.charset, c.collation = chs, coll
}

func (c *collationInfo) CharsetAndCollation(ctx sessionctx.Context) (string, string) {
	if c.charset != "" || c.collation != "" {
		return c.charset, c.collation
	}

	if ctx != nil && ctx.GetSessionVars() != nil {
		c.charset, c.collation = ctx.GetSessionVars().GetCharsetInfo()
	}
	if c.charset == "" || c.collation == "" {
		c.charset, c.collation = charset.GetDefaultCharsetAndCollate()
	}
	c.flen = types.UnspecifiedLength
	return c.charset, c.collation
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
	CharsetAndCollation(ctx sessionctx.Context) (string, string)

	// SetCharsetAndCollation ...
	SetCharsetAndCollation(chs, coll string)
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

	// collationPriority is the priority when infer the result collation, the priority of collation a > b iff collationPriority[a] > collationPriority[b]
	// collation a and b are incompatible if collationPriority[a] = collationPriority[b]
	collationPriority = map[string]int{
		charset.CollationASCII:   1,
		charset.CollationLatin1:  2,
		"utf8_general_ci":        3,
		"utf8_unicode_ci":        3,
		charset.CollationUTF8:    4,
		"utf8mb4_general_ci":     5,
		"utf8mb4_unicode_ci":     5,
		charset.CollationUTF8MB4: 6,
		charset.CollationBin:     7,
	}

	// CollationStrictnessGroup group collation by strictness
	CollationStrictnessGroup = map[string]int{
		"utf8_general_ci":        1,
		"utf8mb4_general_ci":     1,
		charset.CollationASCII:   3,
		charset.CollationLatin1:  3,
		charset.CollationUTF8:    3,
		charset.CollationUTF8MB4: 3,
		charset.CollationBin:     4,
	}

	// CollationStrictness indicates the strictness of comparison of the collation. The unequal order in a weak collation also holds in a strict collation.
	// For example, if a != b in a weak collation(e.g. general_ci), then there must be a != b in a strict collation(e.g. _bin).
	// collation group id in value is stricter than collation group id in key
	CollationStrictness = map[int][]int{
		1: {3, 4},
		3: {4},
		4: {},
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

	// it is weird if a ScalarFunction is CoercibilityNumeric but return string type
	if coer == CoercibilityNumeric {
		return CoercibilityCoercible
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
func DeriveCollationFromExprs(ctx sessionctx.Context, exprs ...Expression) (dstCharset, dstCollation string) {
	curCoer := CoercibilityIgnorable
	curCollationPriority := 0
	dstCharset, dstCollation = charset.GetDefaultCharsetAndCollate()
	if ctx != nil && ctx.GetSessionVars() != nil {
		dstCharset, dstCollation = ctx.GetSessionVars().GetCharsetInfo()
		if dstCharset == "" || dstCollation == "" {
			dstCharset, dstCollation = charset.GetDefaultCharsetAndCollate()
		}
	}
	hasStrArg := false
	// see https://dev.mysql.com/doc/refman/8.0/en/charset-collation-coercibility.html
	for _, e := range exprs {
		if e.GetType().EvalType() != types.ETString {
			continue
		}
		hasStrArg = true

		coer := e.Coercibility()
		ft := e.GetType()
		priority := collationPriority[strings.ToLower(ft.Collate)]
		if coer != curCoer {
			if coer < curCoer {
				curCoer, curCollationPriority, dstCharset, dstCollation = coer, priority, ft.Charset, ft.Collate
			}
			continue
		}
		if priority <= curCollationPriority {
			continue
		}
		curCollationPriority, dstCharset, dstCollation = priority, ft.Charset, ft.Collate
	}
	if !hasStrArg {
		dstCharset, dstCollation = charset.CharsetBin, charset.CollationBin
	}
	return
}

func inferCollation(exprs ...Expression) (collation, charset string, coercibility Coercibility, legal bool) {
	firstExplicitCollation := ""
	coercibility = CoercibilityIgnorable
	for _, arg := range exprs {
		logutil.BgLogger().Warn("arg", zap.Any("arg", arg))
		logutil.BgLogger().Warn("arg", zap.Any("coer", arg.Coercibility()))
		logutil.BgLogger().Warn("arg", zap.Any("charset", arg.GetType().Charset))
		logutil.BgLogger().Warn("arg", zap.Any("collate", arg.GetType().Collate))
		if arg.Coercibility() == CoercibilityExplicit {
			if firstExplicitCollation == "" {
				firstExplicitCollation = arg.GetType().Collate
			} else if firstExplicitCollation != arg.GetType().Collate {
				return "", "", CoercibilityIgnorable, false
			}
		} else if arg.Coercibility() < coercibility {
			coercibility, collation, charset = arg.Coercibility(), arg.GetType().Collate, arg.GetType().Charset
		} else if arg.Coercibility() == coercibility && collation != arg.GetType().Collate {
			p1 := collationPriority[collation]
			p2 := collationPriority[arg.GetType().Collate]

			// same priority means this two collation is incompatible
			// it might derive coercibility to CoercibilityNone
			if p1 == p2 {
				coercibility, collation, charset = CoercibilityNone, getBinCollation(arg.GetType().Charset), arg.GetType().Charset
			} else if p1 < p2 {
				collation, charset = arg.GetType().Collate, arg.GetType().Charset
			}
		}
	}
	return collation, charset, coercibility, true
}

func getBinCollation(cs string) string {
	switch cs {
	case charset.CharsetUTF8:
		return charset.CollationUTF8
	case charset.CharsetUTF8MB4:
		return charset.CollationUTF8MB4
	}
	panic("never reachable")
}
