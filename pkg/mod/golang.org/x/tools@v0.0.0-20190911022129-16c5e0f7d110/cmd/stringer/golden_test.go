// Copyright 2014 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// This file contains simple golden tests for various examples.
// Besides validating the results when the implementation changes,
// it provides a way to look at the generated code without having
// to execute the print statements in one's head.

package main

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/tools/internal/testenv"
)

// Golden represents a test case.
type Golden struct {
	name        string
	trimPrefix  string
	lineComment bool
	input       string // input; the package clause is provided when running the test.
	output      string // exected output.
}

var golden = []Golden{
	{"day", "", false, day_in, day_out},
	{"offset", "", false, offset_in, offset_out},
	{"gap", "", false, gap_in, gap_out},
	{"num", "", false, num_in, num_out},
	{"unum", "", false, unum_in, unum_out},
	{"prime", "", false, prime_in, prime_out},
	{"prefix", "Type", false, prefix_in, prefix_out},
	{"tokens", "", true, tokens_in, tokens_out},
}

// Each example starts with "type XXX [u]int", with a single space separating them.

// Simple test: enumeration of type int starting at 0.
const day_in = `type Day int
const (
	Monday Day = iota
	Tuesday
	Wednesday
	Thursday
	Friday
	Saturday
	Sunday
)
`

const day_out = `func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[Monday-0]
	_ = x[Tuesday-1]
	_ = x[Wednesday-2]
	_ = x[Thursday-3]
	_ = x[Friday-4]
	_ = x[Saturday-5]
	_ = x[Sunday-6]
}

const _Day_name = "MondayTuesdayWednesdayThursdayFridaySaturdaySunday"

var _Day_index = [...]uint8{0, 6, 13, 22, 30, 36, 44, 50}

func (i Day) String() string {
	if i < 0 || i >= Day(len(_Day_index)-1) {
		return "Day(" + strconv.FormatInt(int64(i), 10) + ")"
	}
	return _Day_name[_Day_index[i]:_Day_index[i+1]]
}
`

// Enumeration with an offset.
// Also includes a duplicate.
const offset_in = `type Number int
const (
	_ Number = iota
	One
	Two
	Three
	AnotherOne = One  // Duplicate; note that AnotherOne doesn't appear below.
)
`

const offset_out = `func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[One-1]
	_ = x[Two-2]
	_ = x[Three-3]
}

const _Number_name = "OneTwoThree"

var _Number_index = [...]uint8{0, 3, 6, 11}

func (i Number) String() string {
	i -= 1
	if i < 0 || i >= Number(len(_Number_index)-1) {
		return "Number(" + strconv.FormatInt(int64(i+1), 10) + ")"
	}
	return _Number_name[_Number_index[i]:_Number_index[i+1]]
}
`

// Gaps and an offset.
const gap_in = `type Gap int
const (
	Two Gap = 2
	Three Gap = 3
	Five Gap = 5
	Six Gap = 6
	Seven Gap = 7
	Eight Gap = 8
	Nine Gap = 9
	Eleven Gap = 11
)
`

const gap_out = `func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[Two-2]
	_ = x[Three-3]
	_ = x[Five-5]
	_ = x[Six-6]
	_ = x[Seven-7]
	_ = x[Eight-8]
	_ = x[Nine-9]
	_ = x[Eleven-11]
}

const (
	_Gap_name_0 = "TwoThree"
	_Gap_name_1 = "FiveSixSevenEightNine"
	_Gap_name_2 = "Eleven"
)

var (
	_Gap_index_0 = [...]uint8{0, 3, 8}
	_Gap_index_1 = [...]uint8{0, 4, 7, 12, 17, 21}
)

func (i Gap) String() string {
	switch {
	case 2 <= i && i <= 3:
		i -= 2
		return _Gap_name_0[_Gap_index_0[i]:_Gap_index_0[i+1]]
	case 5 <= i && i <= 9:
		i -= 5
		return _Gap_name_1[_Gap_index_1[i]:_Gap_index_1[i+1]]
	case i == 11:
		return _Gap_name_2
	default:
		return "Gap(" + strconv.FormatInt(int64(i), 10) + ")"
	}
}
`

// Signed integers spanning zero.
const num_in = `type Num int
const (
	m_2 Num = -2 + iota
	m_1
	m0
	m1
	m2
)
`

const num_out = `func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[m_2 - -2]
	_ = x[m_1 - -1]
	_ = x[m0-0]
	_ = x[m1-1]
	_ = x[m2-2]
}

const _Num_name = "m_2m_1m0m1m2"

var _Num_index = [...]uint8{0, 3, 6, 8, 10, 12}

func (i Num) String() string {
	i -= -2
	if i < 0 || i >= Num(len(_Num_index)-1) {
		return "Num(" + strconv.FormatInt(int64(i+-2), 10) + ")"
	}
	return _Num_name[_Num_index[i]:_Num_index[i+1]]
}
`

// Unsigned integers spanning zero.
const unum_in = `type Unum uint
const (
	m_2 Unum = iota + 253
	m_1
)

const (
	m0 Unum = iota
	m1
	m2
)
`

const unum_out = `func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[m_2-253]
	_ = x[m_1-254]
	_ = x[m0-0]
	_ = x[m1-1]
	_ = x[m2-2]
}

const (
	_Unum_name_0 = "m0m1m2"
	_Unum_name_1 = "m_2m_1"
)

var (
	_Unum_index_0 = [...]uint8{0, 2, 4, 6}
	_Unum_index_1 = [...]uint8{0, 3, 6}
)

func (i Unum) String() string {
	switch {
	case 0 <= i && i <= 2:
		return _Unum_name_0[_Unum_index_0[i]:_Unum_index_0[i+1]]
	case 253 <= i && i <= 254:
		i -= 253
		return _Unum_name_1[_Unum_index_1[i]:_Unum_index_1[i+1]]
	default:
		return "Unum(" + strconv.FormatInt(int64(i), 10) + ")"
	}
}
`

// Enough gaps to trigger a map implementation of the method.
// Also includes a duplicate to test that it doesn't cause problems
const prime_in = `type Prime int
const (
	p2 Prime = 2
	p3 Prime = 3
	p5 Prime = 5
	p7 Prime = 7
	p77 Prime = 7 // Duplicate; note that p77 doesn't appear below.
	p11 Prime = 11
	p13 Prime = 13
	p17 Prime = 17
	p19 Prime = 19
	p23 Prime = 23
	p29 Prime = 29
	p37 Prime = 31
	p41 Prime = 41
	p43 Prime = 43
)
`

const prime_out = `func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[p2-2]
	_ = x[p3-3]
	_ = x[p5-5]
	_ = x[p7-7]
	_ = x[p77-7]
	_ = x[p11-11]
	_ = x[p13-13]
	_ = x[p17-17]
	_ = x[p19-19]
	_ = x[p23-23]
	_ = x[p29-29]
	_ = x[p37-31]
	_ = x[p41-41]
	_ = x[p43-43]
}

const _Prime_name = "p2p3p5p7p11p13p17p19p23p29p37p41p43"

var _Prime_map = map[Prime]string{
	2:  _Prime_name[0:2],
	3:  _Prime_name[2:4],
	5:  _Prime_name[4:6],
	7:  _Prime_name[6:8],
	11: _Prime_name[8:11],
	13: _Prime_name[11:14],
	17: _Prime_name[14:17],
	19: _Prime_name[17:20],
	23: _Prime_name[20:23],
	29: _Prime_name[23:26],
	31: _Prime_name[26:29],
	41: _Prime_name[29:32],
	43: _Prime_name[32:35],
}

func (i Prime) String() string {
	if str, ok := _Prime_map[i]; ok {
		return str
	}
	return "Prime(" + strconv.FormatInt(int64(i), 10) + ")"
}
`

const prefix_in = `type Type int
const (
	TypeInt Type = iota
	TypeString
	TypeFloat
	TypeRune
	TypeByte
	TypeStruct
	TypeSlice
)
`

const prefix_out = `func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[TypeInt-0]
	_ = x[TypeString-1]
	_ = x[TypeFloat-2]
	_ = x[TypeRune-3]
	_ = x[TypeByte-4]
	_ = x[TypeStruct-5]
	_ = x[TypeSlice-6]
}

const _Type_name = "IntStringFloatRuneByteStructSlice"

var _Type_index = [...]uint8{0, 3, 9, 14, 18, 22, 28, 33}

func (i Type) String() string {
	if i < 0 || i >= Type(len(_Type_index)-1) {
		return "Type(" + strconv.FormatInt(int64(i), 10) + ")"
	}
	return _Type_name[_Type_index[i]:_Type_index[i+1]]
}
`

const tokens_in = `type Token int
const (
	And Token = iota // &
	Or               // |
	Add              // +
	Sub              // -
	Ident
	Period // .

	// not to be used
	SingleBefore
	// not to be used
	BeforeAndInline // inline
	InlineGeneral /* inline general */
)
`

const tokens_out = `func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[And-0]
	_ = x[Or-1]
	_ = x[Add-2]
	_ = x[Sub-3]
	_ = x[Ident-4]
	_ = x[Period-5]
	_ = x[SingleBefore-6]
	_ = x[BeforeAndInline-7]
	_ = x[InlineGeneral-8]
}

const _Token_name = "&|+-Ident.SingleBeforeinlineinline general"

var _Token_index = [...]uint8{0, 1, 2, 3, 4, 9, 10, 22, 28, 42}

func (i Token) String() string {
	if i < 0 || i >= Token(len(_Token_index)-1) {
		return "Token(" + strconv.FormatInt(int64(i), 10) + ")"
	}
	return _Token_name[_Token_index[i]:_Token_index[i+1]]
}
`

func TestGolden(t *testing.T) {
	testenv.NeedsTool(t, "go")

	dir, err := ioutil.TempDir("", "stringer")
	if err != nil {
		t.Error(err)
	}
	defer os.RemoveAll(dir)

	for _, test := range golden {
		g := Generator{
			trimPrefix:  test.trimPrefix,
			lineComment: test.lineComment,
		}
		input := "package test\n" + test.input
		file := test.name + ".go"
		absFile := filepath.Join(dir, file)
		err := ioutil.WriteFile(absFile, []byte(input), 0644)
		if err != nil {
			t.Error(err)
		}

		g.parsePackage([]string{absFile}, nil)
		// Extract the name and type of the constant from the first line.
		tokens := strings.SplitN(test.input, " ", 3)
		if len(tokens) != 3 {
			t.Fatalf("%s: need type declaration on first line", test.name)
		}
		g.generate(tokens[1])
		got := string(g.format())
		if got != test.output {
			t.Errorf("%s: got(%d)\n====\n%q====\nexpected(%d)\n====%q", test.name, len(got), got, len(test.output), test.output)
		}
	}
}
