// CAUTION: Generated file - DO NOT EDIT.

// Copyright 2015 The parser Authors. All rights reserved.  Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.
//
// This is a derived work base on the original at
//
// http://pubs.opengroup.org/onlinepubs/009695399/utilities/yacc.html
//
// The original work is
//
// Copyright © 2001-2004 The IEEE and The Open Group, All Rights reserved.
//
// Grammar for the input to yacc.

package parser

import __yyfmt__ "fmt"

import (
	"go/token"
)

type yySymType struct {
	yys   int
	node  Node
	Token *Token
}

type yyXError struct {
	state, xsym int
}

const (
	yyDefault      = 57366
	yyEofCode      = 57344
	COMMENT        = 57346
	C_IDENTIFIER   = 57347
	ERROR_VERBOSE  = 57348
	IDENTIFIER     = 57349
	LCURL          = 57350
	LEFT           = 57351
	MARK           = 57352
	NONASSOC       = 57353
	NUMBER         = 57354
	PREC           = 57355
	PRECEDENCE     = 57356
	RCURL          = 57357
	RIGHT          = 57358
	START          = 57359
	STRING_LITERAL = 57360
	TOKEN          = 57361
	TYPE           = 57362
	UNION          = 57363
	yyErrCode      = 57345

	yyMaxDepth = 200
	yyTabOfs   = -42
)

var (
	yyXLAT = map[int]int{
		57352: 0,  // MARK (43x)
		57349: 1,  // IDENTIFIER (32x)
		57348: 2,  // ERROR_VERBOSE (25x)
		57350: 3,  // LCURL (25x)
		57351: 4,  // LEFT (25x)
		57353: 5,  // NONASSOC (25x)
		57356: 6,  // PRECEDENCE (25x)
		57358: 7,  // RIGHT (25x)
		57359: 8,  // START (25x)
		57361: 9,  // TOKEN (25x)
		57362: 10, // TYPE (25x)
		57363: 11, // UNION (25x)
		57344: 12, // $end (21x)
		57347: 13, // C_IDENTIFIER (19x)
		124:   14, // '|' (18x)
		59:    15, // ';' (16x)
		57360: 16, // STRING_LITERAL (12x)
		123:   17, // '{' (11x)
		57355: 18, // PREC (10x)
		44:    19, // ',' (9x)
		60:    20, // '<' (7x)
		57367: 21, // Action (4x)
		57371: 22, // Name (3x)
		57373: 23, // Precedence (3x)
		57376: 24, // RuleItemList (3x)
		125:   25, // '}' (2x)
		57370: 26, // LiteralStringOpt (2x)
		57357: 27, // RCURL (2x)
		57364: 28, // $@1 (1x)
		57365: 29, // $@2 (1x)
		62:    30, // '>' (1x)
		57368: 31, // Definition (1x)
		57369: 32, // DefinitionList (1x)
		57372: 33, // NameList (1x)
		57354: 34, // NUMBER (1x)
		57374: 35, // ReservedWord (1x)
		57375: 36, // Rule (1x)
		57377: 37, // RuleList (1x)
		57378: 38, // Specification (1x)
		57379: 39, // Tag (1x)
		57380: 40, // Tail (1x)
		57366: 41, // $default (0x)
		57346: 42, // COMMENT (0x)
		57345: 43, // error (0x)
	}

	yySymNames = []string{
		"MARK",
		"IDENTIFIER",
		"ERROR_VERBOSE",
		"LCURL",
		"LEFT",
		"NONASSOC",
		"PRECEDENCE",
		"RIGHT",
		"START",
		"TOKEN",
		"TYPE",
		"UNION",
		"$end",
		"C_IDENTIFIER",
		"'|'",
		"';'",
		"STRING_LITERAL",
		"'{'",
		"PREC",
		"','",
		"'<'",
		"Action",
		"Name",
		"Precedence",
		"RuleItemList",
		"'}'",
		"LiteralStringOpt",
		"RCURL",
		"$@1",
		"$@2",
		"'>'",
		"Definition",
		"DefinitionList",
		"NameList",
		"NUMBER",
		"ReservedWord",
		"Rule",
		"RuleList",
		"Specification",
		"Tag",
		"Tail",
		"$default",
		"COMMENT",
		"error",
	}

	yyTokenLiteralStrings = map[int]string{
		57352: "%%",
		57349: "identifier",
		57348: "%error-verbose",
		57350: "%{",
		57351: "%left",
		57353: "%nonassoc",
		57356: "%precedence",
		57358: "%right",
		57359: "%start",
		57361: "%token",
		57362: "%type",
		57363: "%union",
		57347: "rule name",
		57360: "string literal",
		57355: "%prec",
		57357: "%}",
		57354: "number",
	}

	yyReductions = map[int]struct{ xsym, components int }{
		0:  {0, 1},
		1:  {28, 0},
		2:  {21, 3},
		3:  {31, 2},
		4:  {31, 1},
		5:  {29, 0},
		6:  {31, 3},
		7:  {31, 3},
		8:  {31, 2},
		9:  {31, 1},
		10: {32, 0},
		11: {32, 2},
		12: {26, 0},
		13: {26, 1},
		14: {22, 2},
		15: {22, 3},
		16: {33, 1},
		17: {33, 2},
		18: {33, 3},
		19: {23, 0},
		20: {23, 2},
		21: {23, 3},
		22: {23, 2},
		23: {35, 1},
		24: {35, 1},
		25: {35, 1},
		26: {35, 1},
		27: {35, 1},
		28: {35, 1},
		29: {36, 3},
		30: {36, 3},
		31: {24, 0},
		32: {24, 2},
		33: {24, 2},
		34: {24, 2},
		35: {37, 3},
		36: {37, 2},
		37: {38, 4},
		38: {39, 0},
		39: {39, 3},
		40: {40, 1},
		41: {40, 0},
	}

	yyXErrors = map[yyXError]string{
		yyXError{0, 12}:  "invalid empty input",
		yyXError{1, -1}:  "expected $end",
		yyXError{21, -1}: "expected $end",
		yyXError{22, -1}: "expected $end",
		yyXError{5, -1}:  "expected %}",
		yyXError{53, -1}: "expected %}",
		yyXError{41, -1}: "expected '>'",
		yyXError{24, -1}: "expected '}'",
		yyXError{33, -1}: "expected '}'",
		yyXError{23, -1}: "expected Action or Precedence or one of [$end, %%, %prec, ';', '{', '|', identifier, rule name, string literal]",
		yyXError{35, -1}: "expected Action or Precedence or one of [$end, %%, %prec, ';', '{', '|', identifier, rule name, string literal]",
		yyXError{37, -1}: "expected Action or Precedence or one of [$end, %%, %prec, ';', '{', '|', identifier, rule name, string literal]",
		yyXError{31, -1}: "expected Action or one of [$end, %%, ';', '{', '|', rule name]",
		yyXError{2, -1}:  "expected Definition or one of [%%, %error-verbose, %left, %nonassoc, %precedence, %right, %start, %token, %type, %union, %{]",
		yyXError{44, -1}: "expected LiteralStringOpt or one of [%%, %error-verbose, %left, %nonassoc, %precedence, %right, %start, %token, %type, %union, %{, ',', identifier, number, string literal]",
		yyXError{48, -1}: "expected LiteralStringOpt or one of [%%, %error-verbose, %left, %nonassoc, %precedence, %right, %start, %token, %type, %union, %{, ',', identifier, string literal]",
		yyXError{51, -1}: "expected Name or identifier",
		yyXError{43, -1}: "expected Name or one of [%%, %error-verbose, %left, %nonassoc, %precedence, %right, %start, %token, %type, %union, %{, ',', identifier]",
		yyXError{6, -1}:  "expected NameList or Tag or one of [%%, %error-verbose, %left, %nonassoc, %precedence, %right, %start, %token, %type, %union, %{, '<', identifier]",
		yyXError{39, -1}: "expected NameList or one of [%%, %error-verbose, %left, %nonassoc, %precedence, %right, %start, %token, %type, %union, %{, identifier]",
		yyXError{16, -1}: "expected Precedence or RuleItemList or one of [$end, %%, %prec, ';', '{', '|', identifier, rule name, string literal]",
		yyXError{18, -1}: "expected Precedence or RuleItemList or one of [$end, %%, %prec, ';', '{', '|', identifier, rule name, string literal]",
		yyXError{19, -1}: "expected Precedence or RuleItemList or one of [$end, %%, %prec, ';', '{', '|', identifier, rule name, string literal]",
		yyXError{17, -1}: "expected Rule or Tail or one of [$end, %%, '|', rule name]",
		yyXError{15, -1}: "expected RuleList or rule name",
		yyXError{0, -1}:  "expected Specification or one of [%%, %error-verbose, %left, %nonassoc, %precedence, %right, %start, %token, %type, %union, %{]",
		yyXError{3, -1}:  "expected identifier",
		yyXError{25, -1}: "expected identifier",
		yyXError{40, -1}: "expected identifier",
		yyXError{27, -1}: "expected one of [$end, %%, %prec, ';', '{', '|', identifier, rule name, string literal]",
		yyXError{28, -1}: "expected one of [$end, %%, %prec, ';', '{', '|', identifier, rule name, string literal]",
		yyXError{29, -1}: "expected one of [$end, %%, %prec, ';', '{', '|', identifier, rule name, string literal]",
		yyXError{34, -1}: "expected one of [$end, %%, %prec, ';', '{', '|', identifier, rule name, string literal]",
		yyXError{26, -1}: "expected one of [$end, %%, ';', '|', rule name]",
		yyXError{30, -1}: "expected one of [$end, %%, ';', '|', rule name]",
		yyXError{32, -1}: "expected one of [$end, %%, ';', '|', rule name]",
		yyXError{36, -1}: "expected one of [$end, %%, ';', '|', rule name]",
		yyXError{38, -1}: "expected one of [$end, %%, ';', '|', rule name]",
		yyXError{20, -1}: "expected one of [$end, %%, '|', rule name]",
		yyXError{45, -1}: "expected one of [%%, %error-verbose, %left, %nonassoc, %precedence, %right, %start, %token, %type, %union, %{, ',', identifier]",
		yyXError{46, -1}: "expected one of [%%, %error-verbose, %left, %nonassoc, %precedence, %right, %start, %token, %type, %union, %{, ',', identifier]",
		yyXError{47, -1}: "expected one of [%%, %error-verbose, %left, %nonassoc, %precedence, %right, %start, %token, %type, %union, %{, ',', identifier]",
		yyXError{49, -1}: "expected one of [%%, %error-verbose, %left, %nonassoc, %precedence, %right, %start, %token, %type, %union, %{, ',', identifier]",
		yyXError{50, -1}: "expected one of [%%, %error-verbose, %left, %nonassoc, %precedence, %right, %start, %token, %type, %union, %{, ',', identifier]",
		yyXError{52, -1}: "expected one of [%%, %error-verbose, %left, %nonassoc, %precedence, %right, %start, %token, %type, %union, %{, ',', identifier]",
		yyXError{9, -1}:  "expected one of [%%, %error-verbose, %left, %nonassoc, %precedence, %right, %start, %token, %type, %union, %{, '<', identifier]",
		yyXError{10, -1}: "expected one of [%%, %error-verbose, %left, %nonassoc, %precedence, %right, %start, %token, %type, %union, %{, '<', identifier]",
		yyXError{11, -1}: "expected one of [%%, %error-verbose, %left, %nonassoc, %precedence, %right, %start, %token, %type, %union, %{, '<', identifier]",
		yyXError{12, -1}: "expected one of [%%, %error-verbose, %left, %nonassoc, %precedence, %right, %start, %token, %type, %union, %{, '<', identifier]",
		yyXError{13, -1}: "expected one of [%%, %error-verbose, %left, %nonassoc, %precedence, %right, %start, %token, %type, %union, %{, '<', identifier]",
		yyXError{14, -1}: "expected one of [%%, %error-verbose, %left, %nonassoc, %precedence, %right, %start, %token, %type, %union, %{, '<', identifier]",
		yyXError{42, -1}: "expected one of [%%, %error-verbose, %left, %nonassoc, %precedence, %right, %start, %token, %type, %union, %{, identifier]",
		yyXError{4, -1}:  "expected one of [%%, %error-verbose, %left, %nonassoc, %precedence, %right, %start, %token, %type, %union, %{]",
		yyXError{7, -1}:  "expected one of [%%, %error-verbose, %left, %nonassoc, %precedence, %right, %start, %token, %type, %union, %{]",
		yyXError{8, -1}:  "expected one of [%%, %error-verbose, %left, %nonassoc, %precedence, %right, %start, %token, %type, %union, %{]",
		yyXError{54, -1}: "expected one of [%%, %error-verbose, %left, %nonassoc, %precedence, %right, %start, %token, %type, %union, %{]",
		yyXError{55, -1}: "expected one of [%%, %error-verbose, %left, %nonassoc, %precedence, %right, %start, %token, %type, %union, %{]",
	}

	yyParseTab = [56][]uint8{
		// 0
		{32, 2: 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32: 44, 38: 43},
		{12: 42},
		{57, 2: 49, 47, 52, 54, 56, 53, 45, 51, 55, 46, 31: 50, 35: 48},
		{1: 97},
		{38, 2: 38, 38, 38, 38, 38, 38, 38, 38, 38, 38},
		// 5
		{27: 37, 29: 95},
		{4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 20: 82, 39: 81},
		{33, 2: 33, 33, 33, 33, 33, 33, 33, 33, 33, 33},
		{31, 2: 31, 31, 31, 31, 31, 31, 31, 31, 31, 31},
		{19, 19, 19, 19, 19, 19, 19, 19, 19, 19, 19, 19, 20: 19},
		// 10
		{18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 20: 18},
		{17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 20: 17},
		{16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 20: 16},
		{15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 20: 15},
		{14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 20: 14},
		// 15
		{13: 58, 37: 59},
		{11, 11, 12: 11, 11, 11, 11, 11, 11, 11, 24: 79},
		{64, 12: 1, 60, 61, 36: 62, 40: 63},
		{11, 11, 12: 11, 11, 11, 11, 11, 11, 11, 24: 77},
		{11, 11, 12: 11, 11, 11, 11, 11, 11, 11, 24: 65},
		// 20
		{6, 12: 6, 6, 6},
		{12: 5},
		{12: 2},
		{23, 69, 12: 23, 23, 23, 23, 71, 66, 67, 21: 70, 23: 68},
		{25: 41, 28: 75},
		// 25
		{1: 73},
		{12, 12: 12, 12, 12, 72},
		{10, 10, 12: 10, 10, 10, 10, 10, 10, 10},
		{9, 9, 12: 9, 9, 9, 9, 9, 9, 9},
		{8, 8, 12: 8, 8, 8, 8, 8, 8, 8},
		// 30
		{20, 12: 20, 20, 20, 20},
		{22, 12: 22, 22, 22, 22, 17: 66, 21: 74},
		{21, 12: 21, 21, 21, 21},
		{25: 76},
		{40, 40, 12: 40, 40, 40, 40, 40, 40, 40},
		// 35
		{23, 69, 12: 23, 23, 23, 23, 71, 66, 67, 21: 70, 23: 78},
		{13, 12: 13, 13, 13, 72},
		{23, 69, 12: 23, 23, 23, 23, 71, 66, 67, 21: 70, 23: 80},
		{7, 12: 7, 7, 7, 72},
		{34, 86, 34, 34, 34, 34, 34, 34, 34, 34, 34, 34, 22: 87, 33: 85},
		// 40
		{1: 83},
		{30: 84},
		{3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3},
		{35, 86, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 19: 93, 22: 92},
		{30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 16: 88, 19: 30, 26: 89, 34: 90},
		// 45
		{26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 19: 26},
		{29, 29, 29, 29, 29, 29, 29, 29, 29, 29, 29, 29, 19: 29},
		{28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 19: 28},
		{30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 16: 88, 19: 30, 26: 91},
		{27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 19: 27},
		// 50
		{25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 19: 25},
		{1: 86, 22: 94},
		{24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 19: 24},
		{27: 96},
		{36, 2: 36, 36, 36, 36, 36, 36, 36, 36, 36, 36},
		// 55
		{39, 2: 39, 39, 39, 39, 39, 39, 39, 39, 39, 39},
	}
)

var yyDebug = 0

type yyLexer interface {
	Lex(lval *yySymType) int
	Error(s string)
}

type yyLexerEx interface {
	yyLexer
	Reduced(rule, state int, lval *yySymType) bool
}

func yySymName(c int) (s string) {
	x, ok := yyXLAT[c]
	if ok {
		return yySymNames[x]
	}

	if c < 0x7f {
		return __yyfmt__.Sprintf("'%c'", c)
	}

	return __yyfmt__.Sprintf("%d", c)
}

func yylex1(yylex yyLexer, lval *yySymType) (n int) {
	n = yylex.Lex(lval)
	if n <= 0 {
		n = yyEofCode
	}
	if yyDebug >= 3 {
		__yyfmt__.Printf("\nlex %s(%#x %d), prettyString(lval.Token): %v\n", yySymName(n), n, n, prettyString(lval.Token))
	}
	return n
}

func yyParse(yylex yyLexer) int {
	const yyError = 43

	yyEx, _ := yylex.(yyLexerEx)
	var yyn int
	var yylval yySymType
	var yyVAL yySymType
	yyS := make([]yySymType, 200)

	Nerrs := 0   /* number of errors */
	Errflag := 0 /* error recovery flag */
	yyerrok := func() {
		if yyDebug >= 2 {
			__yyfmt__.Printf("yyerrok()\n")
		}
		Errflag = 0
	}
	_ = yyerrok
	yystate := 0
	yychar := -1
	var yyxchar int
	var yyshift int
	yyp := -1
	goto yystack

ret0:
	return 0

ret1:
	return 1

yystack:
	/* put a state and value onto the stack */
	yyp++
	if yyp >= len(yyS) {
		nyys := make([]yySymType, len(yyS)*2)
		copy(nyys, yyS)
		yyS = nyys
	}
	yyS[yyp] = yyVAL
	yyS[yyp].yys = yystate

yynewstate:
	if yychar < 0 {
		yychar = yylex1(yylex, &yylval)
		var ok bool
		if yyxchar, ok = yyXLAT[yychar]; !ok {
			yyxchar = len(yySymNames) // > tab width
		}
	}
	if yyDebug >= 4 {
		var a []int
		for _, v := range yyS[:yyp+1] {
			a = append(a, v.yys)
		}
		__yyfmt__.Printf("state stack %v\n", a)
	}
	row := yyParseTab[yystate]
	yyn = 0
	if yyxchar < len(row) {
		if yyn = int(row[yyxchar]); yyn != 0 {
			yyn += yyTabOfs
		}
	}
	switch {
	case yyn > 0: // shift
		yychar = -1
		yyVAL = yylval
		yystate = yyn
		yyshift = yyn
		if yyDebug >= 2 {
			__yyfmt__.Printf("shift, and goto state %d\n", yystate)
		}
		if Errflag > 0 {
			Errflag--
		}
		goto yystack
	case yyn < 0: // reduce
	case yystate == 1: // accept
		if yyDebug >= 2 {
			__yyfmt__.Println("accept")
		}
		goto ret0
	}

	if yyn == 0 {
		/* error ... attempt to resume parsing */
		switch Errflag {
		case 0: /* brand new error */
			if yyDebug >= 1 {
				__yyfmt__.Printf("no action for %s in state %d\n", yySymName(yychar), yystate)
			}
			msg, ok := yyXErrors[yyXError{yystate, yyxchar}]
			if !ok {
				msg, ok = yyXErrors[yyXError{yystate, -1}]
			}
			if !ok && yyshift != 0 {
				msg, ok = yyXErrors[yyXError{yyshift, yyxchar}]
			}
			if !ok {
				msg, ok = yyXErrors[yyXError{yyshift, -1}]
			}
			if yychar > 0 {
				ls := yyTokenLiteralStrings[yychar]
				if ls == "" {
					ls = yySymName(yychar)
				}
				if ls != "" {
					switch {
					case msg == "":
						msg = __yyfmt__.Sprintf("unexpected %s", ls)
					default:
						msg = __yyfmt__.Sprintf("unexpected %s, %s", ls, msg)
					}
				}
			}
			if msg == "" {
				msg = "syntax error"
			}
			yylex.Error(msg)
			Nerrs++
			fallthrough

		case 1, 2: /* incompletely recovered error ... try again */
			Errflag = 3

			/* find a state where "error" is a legal shift action */
			for yyp >= 0 {
				row := yyParseTab[yyS[yyp].yys]
				if yyError < len(row) {
					yyn = int(row[yyError]) + yyTabOfs
					if yyn > 0 { // hit
						if yyDebug >= 2 {
							__yyfmt__.Printf("error recovery found error shift in state %d\n", yyS[yyp].yys)
						}
						yystate = yyn /* simulate a shift of "error" */
						goto yystack
					}
				}

				/* the current p has no shift on "error", pop stack */
				if yyDebug >= 2 {
					__yyfmt__.Printf("error recovery pops state %d\n", yyS[yyp].yys)
				}
				yyp--
			}
			/* there is no state on the stack with an error shift ... abort */
			if yyDebug >= 2 {
				__yyfmt__.Printf("error recovery failed\n")
			}
			goto ret1

		case 3: /* no shift yet; clobber input char */
			if yyDebug >= 2 {
				__yyfmt__.Printf("error recovery discards %s\n", yySymName(yychar))
			}
			if yychar == yyEofCode {
				goto ret1
			}

			yychar = -1
			goto yynewstate /* try again in the same state */
		}
	}

	r := -yyn
	x0 := yyReductions[r]
	x, n := x0.xsym, x0.components
	yypt := yyp
	_ = yypt // guard against "declared and not used"

	yyp -= n
	if yyp+1 >= len(yyS) {
		nyys := make([]yySymType, len(yyS)*2)
		copy(nyys, yyS)
		yyS = nyys
	}
	yyVAL = yyS[yyp+1]

	/* consult goto table to find next state */
	exState := yystate
	yystate = int(yyParseTab[yyS[yyp].yys][x]) + yyTabOfs
	/* reduction by production r */
	if yyDebug >= 2 {
		__yyfmt__.Printf("reduce using rule %v (%s), and goto state %d\n", r, yySymNames[x], yystate)
	}

	switch r {
	case 1:
		{
			lx := yylex.(*lexer)
			lx.values2 = append([]string(nil), lx.values...)
			lx.positions2 = append([]token.Pos(nil), lx.positions...)
		}
	case 2:
		{
			lx := yylex.(*lexer)
			lhs := &Action{
				Token:  yyS[yypt-2].Token,
				Token2: yyS[yypt-0].Token,
			}
			yyVAL.node = lhs
			for i, v := range lx.values2 {
				a := lx.parseActionValue(lx.positions2[i], v)
				if a != nil {
					lhs.Values = append(lhs.Values, a)
				}
			}
		}
	case 3:
		{
			yyVAL.node = &Definition{
				Token:  yyS[yypt-1].Token,
				Token2: yyS[yypt-0].Token,
			}
		}
	case 4:
		{
			lx := yylex.(*lexer)
			lhs := &Definition{
				Case:  1,
				Token: yyS[yypt-0].Token,
			}
			yyVAL.node = lhs
			lhs.Value = lx.value
		}
	case 5:
		{
			lx := yylex.(*lexer)
			lx.pos2 = lx.pos
			lx.value2 = lx.value
		}
	case 6:
		{
			lx := yylex.(*lexer)
			lhs := &Definition{
				Case:   2,
				Token:  yyS[yypt-2].Token,
				Token2: yyS[yypt-0].Token,
			}
			yyVAL.node = lhs
			lhs.Value = lx.value2
		}
	case 7:
		{
			lhs := &Definition{
				Case:         3,
				ReservedWord: yyS[yypt-2].node.(*ReservedWord),
				Tag:          yyS[yypt-1].node.(*Tag),
				NameList:     yyS[yypt-0].node.(*NameList).reverse(),
			}
			yyVAL.node = lhs
			for n := lhs.NameList; n != nil; n = n.NameList {
				lhs.Nlist = append(lhs.Nlist, n.Name)
			}
		}
	case 8:
		{
			yyVAL.node = &Definition{
				Case:         4,
				ReservedWord: yyS[yypt-1].node.(*ReservedWord),
				Tag:          yyS[yypt-0].node.(*Tag),
			}
		}
	case 9:
		{
			yyVAL.node = &Definition{
				Case:  5,
				Token: yyS[yypt-0].Token,
			}
		}
	case 10:
		{
			yyVAL.node = (*DefinitionList)(nil)
		}
	case 11:
		{
			lx := yylex.(*lexer)
			lhs := &DefinitionList{
				DefinitionList: yyS[yypt-1].node.(*DefinitionList),
				Definition:     yyS[yypt-0].node.(*Definition),
			}
			yyVAL.node = lhs
			lx.defs = append(lx.defs, lhs.Definition)
		}
	case 12:
		{
			yyVAL.node = (*LiteralStringOpt)(nil)
		}
	case 13:
		{
			yyVAL.node = &LiteralStringOpt{
				Token: yyS[yypt-0].Token,
			}
		}
	case 14:
		{
			lx := yylex.(*lexer)
			lhs := &Name{
				Token:            yyS[yypt-1].Token,
				LiteralStringOpt: yyS[yypt-0].node.(*LiteralStringOpt),
			}
			yyVAL.node = lhs
			lhs.Identifier = lx.ident(lhs.Token)
			lhs.Number = -1
		}
	case 15:
		{
			lx := yylex.(*lexer)
			lhs := &Name{
				Case:             1,
				Token:            yyS[yypt-2].Token,
				Token2:           yyS[yypt-1].Token,
				LiteralStringOpt: yyS[yypt-0].node.(*LiteralStringOpt),
			}
			yyVAL.node = lhs
			lhs.Identifier = lx.ident(lhs.Token)
			lhs.Number = lx.number(lhs.Token2)
		}
	case 16:
		{
			yyVAL.node = &NameList{
				Name: yyS[yypt-0].node.(*Name),
			}
		}
	case 17:
		{
			yyVAL.node = &NameList{
				Case:     1,
				NameList: yyS[yypt-1].node.(*NameList),
				Name:     yyS[yypt-0].node.(*Name),
			}
		}
	case 18:
		{
			yyVAL.node = &NameList{
				Case:     2,
				NameList: yyS[yypt-2].node.(*NameList),
				Token:    yyS[yypt-1].Token,
				Name:     yyS[yypt-0].node.(*Name),
			}
		}
	case 19:
		{
			yyVAL.node = (*Precedence)(nil)
		}
	case 20:
		{
			lx := yylex.(*lexer)
			lhs := &Precedence{
				Case:   1,
				Token:  yyS[yypt-1].Token,
				Token2: yyS[yypt-0].Token,
			}
			yyVAL.node = lhs
			lhs.Identifier = lx.ident(lhs.Token2)
		}
	case 21:
		{
			lx := yylex.(*lexer)
			lhs := &Precedence{
				Case:   2,
				Token:  yyS[yypt-2].Token,
				Token2: yyS[yypt-1].Token,
				Action: yyS[yypt-0].node.(*Action),
			}
			yyVAL.node = lhs
			lhs.Identifier = lx.ident(lhs.Token2)
		}
	case 22:
		{
			yyVAL.node = &Precedence{
				Case:       3,
				Precedence: yyS[yypt-1].node.(*Precedence),
				Token:      yyS[yypt-0].Token,
			}
		}
	case 23:
		{
			yyVAL.node = &ReservedWord{
				Token: yyS[yypt-0].Token,
			}
		}
	case 24:
		{
			yyVAL.node = &ReservedWord{
				Case:  1,
				Token: yyS[yypt-0].Token,
			}
		}
	case 25:
		{
			yyVAL.node = &ReservedWord{
				Case:  2,
				Token: yyS[yypt-0].Token,
			}
		}
	case 26:
		{
			yyVAL.node = &ReservedWord{
				Case:  3,
				Token: yyS[yypt-0].Token,
			}
		}
	case 27:
		{
			yyVAL.node = &ReservedWord{
				Case:  4,
				Token: yyS[yypt-0].Token,
			}
		}
	case 28:
		{
			yyVAL.node = &ReservedWord{
				Case:  5,
				Token: yyS[yypt-0].Token,
			}
		}
	case 29:
		{
			lx := yylex.(*lexer)
			lhs := &Rule{
				Token:        yyS[yypt-2].Token,
				RuleItemList: yyS[yypt-1].node.(*RuleItemList).reverse(),
				Precedence:   yyS[yypt-0].node.(*Precedence),
			}
			yyVAL.node = lhs
			lx.ruleName = lhs.Token
			lhs.Name = lhs.Token
		}
	case 30:
		{
			lx := yylex.(*lexer)
			lhs := &Rule{
				Case:         1,
				Token:        yyS[yypt-2].Token,
				RuleItemList: yyS[yypt-1].node.(*RuleItemList).reverse(),
				Precedence:   yyS[yypt-0].node.(*Precedence),
			}
			yyVAL.node = lhs
			lhs.Name = lx.ruleName
		}
	case 31:
		{
			yyVAL.node = (*RuleItemList)(nil)
		}
	case 32:
		{
			yyVAL.node = &RuleItemList{
				Case:         1,
				RuleItemList: yyS[yypt-1].node.(*RuleItemList),
				Token:        yyS[yypt-0].Token,
			}
		}
	case 33:
		{
			yyVAL.node = &RuleItemList{
				Case:         2,
				RuleItemList: yyS[yypt-1].node.(*RuleItemList),
				Action:       yyS[yypt-0].node.(*Action),
			}
		}
	case 34:
		{
			yyVAL.node = &RuleItemList{
				Case:         3,
				RuleItemList: yyS[yypt-1].node.(*RuleItemList),
				Token:        yyS[yypt-0].Token,
			}
		}
	case 35:
		{
			lx := yylex.(*lexer)
			lhs := &RuleList{
				Token:        yyS[yypt-2].Token,
				RuleItemList: yyS[yypt-1].node.(*RuleItemList).reverse(),
				Precedence:   yyS[yypt-0].node.(*Precedence),
			}
			yyVAL.node = lhs
			lx.ruleName = lhs.Token
			rule := &Rule{
				Token:        lhs.Token,
				Name:         lhs.Token,
				RuleItemList: lhs.RuleItemList,
				Precedence:   lhs.Precedence,
			}
			rule.collect()
			lx.rules = append(lx.rules, rule)
		}
	case 36:
		{
			lx := yylex.(*lexer)
			lhs := &RuleList{
				Case:     1,
				RuleList: yyS[yypt-1].node.(*RuleList),
				Rule:     yyS[yypt-0].node.(*Rule),
			}
			yyVAL.node = lhs
			rule := lhs.Rule
			rule.collect()
			lx.rules = append(lx.rules, rule)
		}
	case 37:
		{
			lx := yylex.(*lexer)
			lhs := &Specification{
				DefinitionList: yyS[yypt-3].node.(*DefinitionList).reverse(),
				Token:          yyS[yypt-2].Token,
				RuleList:       yyS[yypt-1].node.(*RuleList).reverse(),
				Tail:           yyS[yypt-0].node.(*Tail),
			}
			yyVAL.node = lhs
			lhs.Defs = lx.defs
			lhs.Rules = lx.rules
			lx.spec = lhs
		}
	case 38:
		{
			yyVAL.node = (*Tag)(nil)
		}
	case 39:
		{
			yyVAL.node = &Tag{
				Token:  yyS[yypt-2].Token,
				Token2: yyS[yypt-1].Token,
				Token3: yyS[yypt-0].Token,
			}
		}
	case 40:
		{
			lx := yylex.(*lexer)
			lhs := &Tail{
				Token: yyS[yypt-0].Token,
			}
			yyVAL.node = lhs
			lhs.Value = lx.value
		}
	case 41:
		{
			yyVAL.node = (*Tail)(nil)
		}

	}

	if yyEx != nil && yyEx.Reduced(r, exState, &yyVAL) {
		return -1
	}
	goto yystack /* stack new state and value */
}
