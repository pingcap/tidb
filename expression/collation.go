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
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/logutil"
)

// ExprCollation is a struct that store the collation related information.
type ExprCollation struct {
	Coer      Coercibility
	Repe      Repertoire
	Charset   string
	Collation string
}

type collationInfo struct {
	coer       Coercibility
	coerInit   bool
	repertoire Repertoire

	charset   string
	collation string
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

func (c *collationInfo) SetRepertoire(r Repertoire) {
	c.repertoire = r
}

func (c *collationInfo) SetCharsetAndCollation(chs, coll string) {
	c.charset, c.collation = chs, coll
}

func (c *collationInfo) CharsetAndCollation() (string, string) {
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

	// Repertoire returns the repertoire value which is used to check collations.
	Repertoire() Repertoire

	// SetRepertoire sets a specified repertoire for this expression.
	SetRepertoire(r Repertoire)

	// CharsetAndCollation gets charset and collation.
	CharsetAndCollation() (string, string)

	// SetCharsetAndCollation sets charset and collation.
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

var (
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

// The Repertoire of a character set is the collection of characters in the set.
// See https://dev.mysql.com/doc/refman/8.0/en/charset-repertoire.html.
// Only String expression has Repertoire, for non-string expression, it does not matter what the value it is.
type Repertoire int

const (
	// ASCII is pure ASCII U+0000..U+007F.
	ASCII Repertoire = 0x01
	// EXTENDED is extended characters: U+0080..U+FFFF
	EXTENDED = ASCII << 1
	// UNICODE is ASCII | EXTENDED
	UNICODE = ASCII | EXTENDED
)

func deriveCoercibilityForScalarFunc(sf *ScalarFunction) Coercibility {
	panic("this function should never be called")
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
	if c.RetType.GetType() == mysql.TypeNull {
		return CoercibilityIgnorable
	}

	switch c.RetType.EvalType() {
	case types.ETJson:
	case types.ETString:
		return CoercibilityImplicit
	default:
		return CoercibilityNumeric
	}
	return CoercibilityImplicit
}

func deriveCollation(ctx sessionctx.Context, funcName string, args []Expression, retType types.EvalType, argTps ...types.EvalType) (ec *ExprCollation, err error) {
	switch funcName {
	case ast.Concat, ast.ConcatWS, ast.Lower, ast.Lcase, ast.Reverse, ast.Upper, ast.Ucase, ast.Quote, ast.Coalesce, ast.Greatest, ast.Least:
		return CheckAndDeriveCollationFromExprs(ctx, funcName, retType, args...)
	case ast.Left, ast.Right, ast.Repeat, ast.Trim, ast.LTrim, ast.RTrim, ast.Substr, ast.SubstringIndex, ast.Replace, ast.Substring, ast.Mid, ast.Translate:
		return CheckAndDeriveCollationFromExprs(ctx, funcName, retType, args[0])
	case ast.InsertFunc:
		return CheckAndDeriveCollationFromExprs(ctx, funcName, retType, args[0], args[3])
	case ast.Lpad, ast.Rpad:
		return CheckAndDeriveCollationFromExprs(ctx, funcName, retType, args[0], args[2])
	case ast.Elt, ast.ExportSet, ast.MakeSet:
		return CheckAndDeriveCollationFromExprs(ctx, funcName, retType, args[1:]...)
	case ast.FindInSet, ast.Regexp:
		return CheckAndDeriveCollationFromExprs(ctx, funcName, types.ETInt, args...)
	case ast.Field:
		if argTps[0] == types.ETString {
			return CheckAndDeriveCollationFromExprs(ctx, funcName, retType, args...)
		}
	case ast.Locate, ast.Instr, ast.Position:
		return CheckAndDeriveCollationFromExprs(ctx, funcName, retType, args[0], args[1])
	case ast.GE, ast.LE, ast.GT, ast.LT, ast.EQ, ast.NE, ast.NullEQ, ast.Strcmp:
		// if compare type is string, we should determine which collation should be used.
		if argTps[0] == types.ETString {
			ec, err = CheckAndDeriveCollationFromExprs(ctx, funcName, types.ETInt, args...)
			if err != nil {
				return nil, err
			}
			ec.Coer = CoercibilityNumeric
			ec.Repe = ASCII
			return ec, nil
		}
	case ast.If:
		return CheckAndDeriveCollationFromExprs(ctx, funcName, retType, args[1], args[2])
	case ast.Ifnull:
		return CheckAndDeriveCollationFromExprs(ctx, funcName, retType, args[0], args[1])
	case ast.Like:
		ec, err = CheckAndDeriveCollationFromExprs(ctx, funcName, types.ETInt, args[0], args[1])
		if err != nil {
			return nil, err
		}
		ec.Coer = CoercibilityNumeric
		ec.Repe = ASCII
		return ec, nil
	case ast.In:
		if args[0].GetType().EvalType() == types.ETString {
			return CheckAndDeriveCollationFromExprs(ctx, funcName, types.ETInt, args...)
		}
	case ast.DateFormat, ast.TimeFormat:
		charsetInfo, collation := ctx.GetSessionVars().GetCharsetInfo()
		return &ExprCollation{args[1].Coercibility(), args[1].Repertoire(), charsetInfo, collation}, nil
	case ast.Cast:
		// We assume all the cast are implicit.
		ec = &ExprCollation{args[0].Coercibility(), args[0].Repertoire(), args[0].GetType().GetCharset(), args[0].GetType().GetCollate()}
		// Non-string type cast to string type should use @@character_set_connection and @@collation_connection.
		// String type cast to string type should keep its original charset and collation. It should not happen.
		if retType == types.ETString && argTps[0] != types.ETString {
			ec.Charset, ec.Collation = ctx.GetSessionVars().GetCharsetInfo()
		}
		return ec, nil
	case ast.Case:
		// FIXME: case function aggregate collation is not correct.
		// We should only aggregate the `then expression`,
		// case ... when ... expression will be rewritten to:
		// args:  eq scalar func(args: value, condition1), result1,
		//        eq scalar func(args: value, condition2), result2,
		//        ...
		//        else clause
		// Or
		// args:  condition1, result1,
		//        condition2, result2,
		//        ...
		//        else clause
		// so, arguments with odd index are the `then expression`.
		if argTps[1] == types.ETString {
			fieldArgs := make([]Expression, 0)
			for i := 1; i < len(args); i += 2 {
				fieldArgs = append(fieldArgs, args[i])
			}
			if len(args)%2 == 1 {
				fieldArgs = append(fieldArgs, args[len(args)-1])
			}
			return CheckAndDeriveCollationFromExprs(ctx, funcName, retType, fieldArgs...)
		}
	case ast.Database, ast.User, ast.CurrentUser, ast.Version, ast.CurrentRole, ast.TiDBVersion:
		chs, coll := charset.GetDefaultCharsetAndCollate()
		return &ExprCollation{CoercibilitySysconst, UNICODE, chs, coll}, nil
	case ast.Format, ast.Space, ast.ToBase64, ast.UUID, ast.Hex, ast.MD5, ast.SHA, ast.SHA2:
		// should return ASCII repertoire, MySQL's doc says it depends on character_set_connection, but it not true from its source code.
		ec = &ExprCollation{Coer: CoercibilityCoercible, Repe: ASCII}
		ec.Charset, ec.Collation = ctx.GetSessionVars().GetCharsetInfo()
		return ec, nil
	}

	ec = &ExprCollation{CoercibilityNumeric, ASCII, charset.CharsetBin, charset.CollationBin}
	if retType == types.ETString {
		ec.Charset, ec.Collation = ctx.GetSessionVars().GetCharsetInfo()
		ec.Coer = CoercibilityCoercible
		if ec.Charset != charset.CharsetASCII {
			ec.Repe = UNICODE
		}
	}
	return ec, nil
}

// DeriveCollationFromExprs derives collation information from these expressions.
// Deprecated, use CheckAndDeriveCollationFromExprs instead.
// TODO: remove this function after the all usage is replaced by CheckAndDeriveCollationFromExprs
func DeriveCollationFromExprs(ctx sessionctx.Context, exprs ...Expression) (dstCharset, dstCollation string) {
	collation := inferCollation(exprs...)
	return collation.Charset, collation.Collation
}

// CheckAndDeriveCollationFromExprs derives collation information from these expressions, return error if derives collation error.
func CheckAndDeriveCollationFromExprs(ctx sessionctx.Context, funcName string, evalType types.EvalType, args ...Expression) (et *ExprCollation, err error) {
	ec := inferCollation(args...)
	if ec == nil {
		return nil, illegalMixCollationErr(funcName, args)
	}

	if evalType != types.ETString && ec.Coer == CoercibilityNone {
		return nil, illegalMixCollationErr(funcName, args)
	}

	if evalType == types.ETString && ec.Coer == CoercibilityNumeric {
		ec.Charset, ec.Collation = ctx.GetSessionVars().GetCharsetInfo()
		ec.Coer = CoercibilityCoercible
		ec.Repe = ASCII
	}

	if !safeConvert(ctx, ec, args...) {
		return nil, illegalMixCollationErr(funcName, args)
	}

	return ec, nil
}

func safeConvert(ctx sessionctx.Context, ec *ExprCollation, args ...Expression) bool {
	enc := charset.FindEncodingTakeUTF8AsNoop(ec.Charset)
	for _, arg := range args {
		if arg.GetType().GetCharset() == ec.Charset {
			continue
		}

		// If value has ASCII repertoire, or it is binary string, just skip it.
		if arg.Repertoire() == ASCII || types.IsBinaryStr(arg.GetType()) {
			continue
		}

		if c, ok := arg.(*Constant); ok {
			str, isNull, err := c.EvalString(ctx, chunk.Row{})
			if err != nil {
				return false
			}
			if isNull {
				continue
			}
			if !enc.IsValid(hack.Slice(str)) {
				return false
			}
		} else {
			if arg.GetType().GetCollate() != charset.CharsetBin && ec.Charset != charset.CharsetBin && !isUnicodeCollation(ec.Charset) {
				return false
			}
		}
	}

	return true
}

// inferCollation infers collation, charset, coercibility and check the legitimacy.
func inferCollation(exprs ...Expression) *ExprCollation {
	if len(exprs) == 0 {
		// TODO: see if any function with no arguments could run here.
		dstCharset, dstCollation := charset.GetDefaultCharsetAndCollate()
		return &ExprCollation{
			Coer:      CoercibilityIgnorable,
			Repe:      UNICODE,
			Charset:   dstCharset,
			Collation: dstCollation,
		}
	}

	repertoire := exprs[0].Repertoire()
	coercibility := exprs[0].Coercibility()
	dstCharset, dstCollation := exprs[0].GetType().GetCharset(), exprs[0].GetType().GetCollate()
	if exprs[0].GetType().EvalType() == types.ETJson {
		dstCharset, dstCollation = charset.CharsetUTF8MB4, charset.CollationUTF8MB4
	}
	unknownCS := false

	// Aggregate arguments one by one, agg(a, b, c) := agg(agg(a, b), c).
	for _, arg := range exprs[1:] {
		argCharset, argCollation := arg.GetType().GetCharset(), arg.GetType().GetCollate()
		// The collation of JSON is always utf8mb4_bin in builtin-func which is same as MySQL
		// see details https://github.com/pingcap/tidb/issues/31320#issuecomment-1010599311
		if arg.GetType().EvalType() == types.ETJson {
			argCharset, argCollation = charset.CharsetUTF8MB4, charset.CollationUTF8MB4
		}
		// If one of the arguments is binary charset, we allow it can be used with other charsets.
		// If they have the same coercibility, let the binary charset one to be the winner because binary has more precedence.
		if dstCollation == charset.CollationBin || argCollation == charset.CollationBin {
			if coercibility > arg.Coercibility() || (coercibility == arg.Coercibility() && argCollation == charset.CollationBin) {
				coercibility, dstCharset, dstCollation = arg.Coercibility(), argCharset, argCollation
			}
			repertoire |= arg.Repertoire()
			continue
		}

		// If charset is different, only if conversion without data loss is allowed:
		//		1. ASCII repertoire is always convertible.
		//		2. Non-Unicode charset can convert to Unicode charset.
		//		3. utf8 can convert to utf8mb4.
		//		4. constant value is allowed because we can eval and convert it directly.
		// If we can not aggregate these two collations, we will get CoercibilityNone and wait for an explicit COLLATE clause, if
		// there is no explicit COLLATE clause, we will get an error.
		if dstCharset != argCharset {
			switch {
			case coercibility < arg.Coercibility():
				if arg.Repertoire() == ASCII || arg.Coercibility() >= CoercibilitySysconst || isUnicodeCollation(dstCharset) {
					repertoire |= arg.Repertoire()
					continue
				}
			case coercibility == arg.Coercibility():
				if (isUnicodeCollation(dstCharset) && !isUnicodeCollation(argCharset)) || (dstCharset == charset.CharsetUTF8MB4 && argCharset == charset.CharsetUTF8) {
					repertoire |= arg.Repertoire()
					continue
				} else if (isUnicodeCollation(argCharset) && !isUnicodeCollation(dstCharset)) || (argCharset == charset.CharsetUTF8MB4 && dstCharset == charset.CharsetUTF8) {
					coercibility, dstCharset, dstCollation = arg.Coercibility(), argCharset, argCollation
					repertoire |= arg.Repertoire()
					continue
				} else if repertoire == ASCII && arg.Repertoire() != ASCII {
					coercibility, dstCharset, dstCollation = arg.Coercibility(), argCharset, argCollation
					repertoire |= arg.Repertoire()
					continue
				} else if repertoire != ASCII && arg.Repertoire() == ASCII {
					repertoire |= arg.Repertoire()
					continue
				}
			case coercibility > arg.Coercibility():
				if repertoire == ASCII || coercibility >= CoercibilitySysconst || isUnicodeCollation(argCharset) {
					coercibility, dstCharset, dstCollation = arg.Coercibility(), argCharset, argCollation
					repertoire |= arg.Repertoire()
					continue
				}
			}

			// Cannot apply conversion.
			repertoire |= arg.Repertoire()
			coercibility, dstCharset, dstCollation = CoercibilityNone, charset.CharsetBin, charset.CollationBin
			unknownCS = true
		} else {
			// If charset is the same, use lower coercibility, if coercibility is the same and none of them are _bin,
			// derive to CoercibilityNone and _bin collation.
			switch {
			case coercibility == arg.Coercibility():
				if dstCollation == argCollation {
				} else if coercibility == CoercibilityExplicit {
					return nil
				} else if isBinCollation(dstCollation) {
				} else if isBinCollation(argCollation) {
					coercibility, dstCharset, dstCollation = arg.Coercibility(), argCharset, argCollation
				} else {
					coercibility, dstCharset, dstCollation = CoercibilityNone, argCharset, getBinCollation(argCharset)
				}
			case coercibility > arg.Coercibility():
				coercibility, dstCharset, dstCollation = arg.Coercibility(), argCharset, argCollation
			}
			repertoire |= arg.Repertoire()
		}
	}

	if unknownCS && coercibility != CoercibilityExplicit {
		return nil
	}

	return &ExprCollation{
		Coer:      coercibility,
		Repe:      repertoire,
		Charset:   dstCharset,
		Collation: dstCollation,
	}
}

func isUnicodeCollation(ch string) bool {
	return ch == charset.CharsetUTF8 || ch == charset.CharsetUTF8MB4
}

func isBinCollation(collate string) bool {
	return collate == charset.CollationASCII || collate == charset.CollationLatin1 ||
		collate == charset.CollationUTF8 || collate == charset.CollationUTF8MB4 ||
		collate == charset.CollationGBKBin
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

var (
	coerString = []string{"EXPLICIT", "NONE", "IMPLICIT", "SYSCONST", "COERCIBLE", "NUMERIC", "IGNORABLE"}
)

func illegalMixCollationErr(funcName string, args []Expression) error {
	funcName = GetDisplayName(funcName)

	switch len(args) {
	case 2:
		return collate.ErrIllegalMix2Collation.GenWithStackByArgs(args[0].GetType().GetCollate(), coerString[args[0].Coercibility()], args[1].GetType().GetCollate(), coerString[args[1].Coercibility()], funcName)
	case 3:
		return collate.ErrIllegalMix3Collation.GenWithStackByArgs(args[0].GetType().GetCollate(), coerString[args[0].Coercibility()], args[1].GetType().GetCollate(), coerString[args[1].Coercibility()], args[2].GetType().GetCollate(), coerString[args[2].Coercibility()], funcName)
	default:
		return collate.ErrIllegalMixCollation.GenWithStackByArgs(funcName)
	}
}
