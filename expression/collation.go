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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"
)

type collationInfo struct {
	coer       Coercibility
	coerInit   bool
	repertoire Repertoire
	repeInit   bool

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

func (c *collationInfo) Repertoire() Repertoire {
	return c.repertoire
}

func (c *collationInfo) HasRepertoire() bool {
	return c.repeInit
}

func (c *collationInfo) SetRepertoire(r Repertoire) {
	c.repertoire = r
	c.repeInit = true
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

	// HasRepertoire returns if the Repertoire value is initialized.
	HasRepertoire() bool

	// Repertoire returns the repertoire value which is used to check collations.
	Repertoire() Repertoire

	// SetRepertoire sets a specified repertoire for this expression.
	SetRepertoire(r Repertoire)

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
	// CoercibilityImplicit is derived from a column or a stored routine parameter or local variable or cast() function.
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

// The Repertoire of a character set is the collection of characters in the set.
// See https://dev.mysql.com/doc/refman/8.0/en/charset-repertoire.html.
type Repertoire int

const (
	// ASCII is ascii repertoire.
	ASCII Repertoire = 0x01
	// UNICODE is unicode repertoire.
	UNICODE = ASCII << 1
)

var (
	sysConstFuncs = map[string]struct{}{
		ast.User:        {},
		ast.Version:     {},
		ast.Database:    {},
		ast.CurrentRole: {},
		ast.CurrentUser: {},
	}

	constFuncs = map[string]struct{}{
		ast.Collation: {},
		ast.Charset:   {},
	}

	// CollationStrictnessGroup group collation by strictness
	CollationStrictnessGroup = map[string]int{
		"utf8_general_ci":        1,
		"utf8mb4_general_ci":     1,
		"utf8_unicode_ci":        2,
		"utf8mb4_unicode_ci":     2,
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
		2: {3, 4},
		3: {4},
		4: {},
	}
)

func deriveCoercibilityForScarlarFunc(sf *ScalarFunction) Coercibility {
	if _, ok := sysConstFuncs[sf.FuncName.L]; ok {
		return CoercibilitySysconst
	}

	if _, ok := constFuncs[sf.FuncName.L]; ok {
		return CoercibilityCoercible
	}

	if sf.RetType.EvalType() != types.ETString {
		return CoercibilityNumeric
	}

	_, _, coer, _ := inferCollation(sf.GetArgs()...)

	// it is weird if a ScalarFunction is CoercibilityNumeric but return string type
	if coer == CoercibilityNumeric {
		return CoercibilityCoercible
	}

	return coer
}

func deriveCoercibilityForConstant(c *Constant) Coercibility {
	if c.Value.IsNull() {
		return CoercibilityIgnorable
	} else if c.RetType.EvalType() != types.ETString {
		return CoercibilityNumeric
	}
	return CoercibilityCoercible
}

func deriveCoercibilityForColumn(c *Column) Coercibility {
	// For specified type null, it should return CoercibilityIgnorable, which means it got the lowest priority in DeriveCollationFromExprs.
	if c.RetType.Tp == mysql.TypeNull {
		return CoercibilityIgnorable
	}
	if c.RetType.EvalType() != types.ETString {
		return CoercibilityNumeric
	}
	return CoercibilityImplicit
}

// DeriveCollationFromExprs derives collation information from these expressions.
func DeriveCollationFromExprs(ctx sessionctx.Context, exprs ...Expression) (dstCharset, dstCollation string) {
	dstCollation, dstCharset, _, _ = inferCollation(exprs...)
	return
}

func deriveRepertoireForExprs(sf *ScalarFunction) Repertoire {
	if sf.RetType.EvalType() != types.ETString {
		return ASCII
	}

	for _, arg := range sf.GetArgs() {
		if arg.GetType().EvalType() == types.ETString {
			if arg.Repertoire() != ASCII {
				return UNICODE
			}
		}
	}
	return ASCII
}

// inferCollation infers collation, charset, coercibility and check the legitimacy.
func inferCollation(exprs ...Expression) (dstCollation, dstCharset string, coercibility Coercibility, legal bool) {
	if len(exprs) == 0 {
		dstCharset, dstCollation = charset.GetDefaultCharsetAndCollate()
		return dstCollation, dstCharset, CoercibilityIgnorable, true
	}

	repertoire := exprs[0].Repertoire()
	coercibility = exprs[0].Coercibility()
	dstCharset, dstCollation = exprs[0].GetType().Charset, exprs[0].GetType().Collate
	unknownCS := false

	for _, arg := range exprs[1:] {
		if dstCollation == charset.CollationBin || arg.GetType().Collate == charset.CollationBin {
			if coercibility > arg.Coercibility() || (coercibility == arg.Coercibility() && arg.GetType().Collate == charset.CollationBin) {
				coercibility, dstCharset, dstCollation = arg.Coercibility(), arg.GetType().Charset, arg.GetType().Collate
			}
			repertoire |= arg.Repertoire()
			continue
		}

		if dstCharset != arg.GetType().Charset {
			switch {
			case coercibility < arg.Coercibility():
				if arg.Repertoire() == ASCII || arg.Coercibility() >= CoercibilitySysconst || isUnicodeCollation(dstCharset) {
					repertoire |= arg.Repertoire()
					continue
				}
			case coercibility == arg.Coercibility():
				if (isUnicodeCollation(dstCharset) && !isUnicodeCollation(arg.GetType().Charset)) || (dstCharset == charset.CharsetUTF8MB4 && arg.GetType().Charset == charset.CharsetUTF8) {
					repertoire |= arg.Repertoire()
					continue
				} else if (isUnicodeCollation(arg.GetType().Charset) && !isUnicodeCollation(dstCharset)) || (arg.GetType().Charset == charset.CharsetUTF8MB4 && dstCharset == charset.CharsetUTF8) {
					coercibility, dstCharset, dstCollation = arg.Coercibility(), arg.GetType().Charset, arg.GetType().Collate
					repertoire |= arg.Repertoire()
					continue
				} else if repertoire == ASCII && arg.Repertoire() != ASCII {
					coercibility, dstCharset, dstCollation = arg.Coercibility(), arg.GetType().Charset, arg.GetType().Collate
					repertoire |= arg.Repertoire()
					continue
				} else if repertoire != ASCII && arg.Repertoire() == ASCII {
					repertoire |= arg.Repertoire()
					continue
				}
			case coercibility > arg.Coercibility():
				if repertoire == ASCII || coercibility >= CoercibilitySysconst || isUnicodeCollation(arg.GetType().Charset) {
					coercibility, dstCharset, dstCollation = arg.Coercibility(), arg.GetType().Charset, arg.GetType().Collate
					repertoire |= arg.Repertoire()
					continue
				}
			}

			// Cannot apply conversion.
			repertoire |= arg.Repertoire()
			coercibility, dstCharset, dstCollation = CoercibilityNone, charset.CharsetBin, charset.CollationBin
			unknownCS = true
		} else {
			switch {
			case coercibility == arg.Coercibility():
				if dstCollation == arg.GetType().Collate {
				} else if coercibility == CoercibilityExplicit {
					return "", "", CoercibilityIgnorable, false
				} else if isBinaryCollation(dstCollation) {
				} else if isBinaryCollation(arg.GetType().Collate) {
					coercibility, dstCharset, dstCollation = arg.Coercibility(), arg.GetType().Charset, arg.GetType().Collate
				} else {
					coercibility, dstCollation, dstCharset = CoercibilityNone, getBinCollation(arg.GetType().Charset), arg.GetType().Charset
				}
			case coercibility > arg.Coercibility():
				coercibility, dstCharset, dstCollation = arg.Coercibility(), arg.GetType().Charset, arg.GetType().Collate
			}
			repertoire |= arg.Repertoire()
		}
	}

	if unknownCS && coercibility != CoercibilityExplicit {
		return "", "", CoercibilityIgnorable, false
	}

	return dstCollation, dstCharset, coercibility, true
}

func isUnicodeCollation(ch string) bool {
	return ch == charset.CharsetUTF8 || ch == charset.CharsetUTF8MB4
}

func isBinaryCollation(collation string) bool {
	return collation[len(collation)-3:] == "bin"
}

// getBinCollation get binary collation by charset
func getBinCollation(cs string) string {
	switch cs {
	case charset.CharsetUTF8:
		return charset.CollationUTF8
	case charset.CharsetUTF8MB4:
		return charset.CollationUTF8MB4
	case charset.CharsetGBK:
		return charset.CollationGBKBin
	}

	logutil.BgLogger().Error("unexpected charset " + cs)
	// it must return something, never reachable
	return charset.CollationUTF8MB4
}
