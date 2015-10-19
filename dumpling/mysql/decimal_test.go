// The MIT License (MIT)

// Copyright (c) 2015 Spring, Inc.

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

// - Based on https://github.com/oguzbilgic/fpd, which has the following license:
// """
// The MIT License (MIT)

// Copyright (c) 2013 Oguz Bilgic

// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:

// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
// FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
// IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
// """

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

package mysql

import (
	"encoding/json"
	"encoding/xml"
	"math"
	"strings"
	"testing"
)

var testTable = map[float64]string{
	3.141592653589793:   "3.141592653589793",
	3:                   "3",
	1234567890123456:    "1234567890123456",
	1234567890123456000: "1234567890123456000",
	1234.567890123456:   "1234.567890123456",
	.1234567890123456:   "0.1234567890123456",
	0:                   "0",
	.1111111111111110:   "0.111111111111111",
	.1111111111111111:   "0.1111111111111111",
	.1111111111111119:   "0.1111111111111119",
	.000000000000000001: "0.000000000000000001",
	.000000000000000002: "0.000000000000000002",
	.000000000000000003: "0.000000000000000003",
	.000000000000000005: "0.000000000000000005",
	.000000000000000008: "0.000000000000000008",
	.1000000000000001:   "0.1000000000000001",
	.1000000000000002:   "0.1000000000000002",
	.1000000000000003:   "0.1000000000000003",
	.1000000000000005:   "0.1000000000000005",
	.1000000000000008:   "0.1000000000000008",
}

func init() {
	// add negatives
	for f, s := range testTable {
		if f > 0 {
			testTable[-f] = "-" + s
		}
	}
}

func TestNewFromFloat(t *testing.T) {
	for f, s := range testTable {
		d := NewDecimalFromFloat(f)
		if d.String() != s {
			t.Errorf("expected %s, got %s (%s, %d)",
				s, d.String(),
				d.value.String(), d.exp)
		}
	}

	shouldPanicOn := []float64{
		math.NaN(),
		math.Inf(1),
		math.Inf(-1),
	}

	for _, n := range shouldPanicOn {
		var d Decimal
		if !didPanic(func() { d = NewDecimalFromFloat(n) }) {
			t.Fatalf("Expected panic when creating a Decimal from %v, got %v instead", n, d.String())
		}
	}
}

func TestNewFromString(t *testing.T) {
	for _, s := range testTable {
		d, err := ParseDecimal(s)
		if err != nil {
			t.Errorf("error while parsing %s", s)
		} else if d.String() != s {
			t.Errorf("expected %s, got %s (%s, %d)",
				s, d.String(),
				d.value.String(), d.exp)
		}
	}
}

func TestNewFromStringErrs(t *testing.T) {
	tests := []string{
		"",
		"qwert",
		"-",
		".",
		"-.",
		".-",
		"234-.56",
		"234-56",
		"2-",
		"..",
		"2..",
		"..2",
		".5.2",
		"8..2",
		"8.1.",
	}

	for _, s := range tests {
		_, err := ParseDecimal(s)

		if err == nil {
			t.Errorf("error expected when parsing %s", s)
		}
	}
}

func TestNewFromFloatWithExponent(t *testing.T) {
	type Inp struct {
		float float64
		exp   int32
	}
	tests := map[Inp]string{
		Inp{123.4, -3}:      "123.400",
		Inp{123.4, -1}:      "123.4",
		Inp{123.412345, 1}:  "120",
		Inp{123.412345, 0}:  "123",
		Inp{123.412345, -5}: "123.41235",
		Inp{123.412345, -6}: "123.412345",
		Inp{123.412345, -7}: "123.4123450",
	}

	// add negatives
	for p, s := range tests {
		if p.float > 0 {
			tests[Inp{-p.float, p.exp}] = "-" + s
		}
	}

	for input, s := range tests {
		d := NewDecimalFromFloatWithExponent(input.float, input.exp)
		if d.String() != s {
			t.Errorf("expected %s, got %s (%s, %d)",
				s, d.String(),
				d.value.String(), d.exp)
		}
	}

	shouldPanicOn := []float64{
		math.NaN(),
		math.Inf(1),
		math.Inf(-1),
	}

	for _, n := range shouldPanicOn {
		var d Decimal
		if !didPanic(func() { d = NewDecimalFromFloatWithExponent(n, 0) }) {
			t.Fatalf("Expected panic when creating a Decimal from %v, got %v instead", n, d.String())
		}
	}
}

func TestJSON(t *testing.T) {
	for _, s := range testTable {
		var doc struct {
			Amount Decimal `json:"amount"`
		}
		docStr := `{"amount":"` + s + `"}`
		err := json.Unmarshal([]byte(docStr), &doc)
		if err != nil {
			t.Errorf("error unmarshaling %s: %v", docStr, err)
		} else if doc.Amount.String() != s {
			t.Errorf("expected %s, got %s (%s, %d)",
				s, doc.Amount.String(),
				doc.Amount.value.String(), doc.Amount.exp)
		}

		out, err := json.Marshal(&doc)
		if err != nil {
			t.Errorf("error marshaling %+v: %v", doc, err)
		} else if string(out) != docStr {
			t.Errorf("expected %s, got %s", docStr, string(out))
		}
	}
}

func TestBadJSON(t *testing.T) {
	for _, testCase := range []string{
		"]o_o[",
		"{",
		`{"amount":""`,
		`{"amount":""}`,
		`{"amount":"nope"}`,
		`0.333`,
	} {
		var doc struct {
			Amount Decimal `json:"amount"`
		}
		err := json.Unmarshal([]byte(testCase), &doc)
		if err == nil {
			t.Errorf("expected error, got %+v", doc)
		}
	}
}

func TestXML(t *testing.T) {
	for _, s := range testTable {
		var doc struct {
			XMLName xml.Name `xml:"account"`
			Amount  Decimal  `xml:"amount"`
		}
		docStr := `<account><amount>` + s + `</amount></account>`
		err := xml.Unmarshal([]byte(docStr), &doc)
		if err != nil {
			t.Errorf("error unmarshaling %s: %v", docStr, err)
		} else if doc.Amount.String() != s {
			t.Errorf("expected %s, got %s (%s, %d)",
				s, doc.Amount.String(),
				doc.Amount.value.String(), doc.Amount.exp)
		}

		out, err := xml.Marshal(&doc)
		if err != nil {
			t.Errorf("error marshaling %+v: %v", doc, err)
		} else if string(out) != docStr {
			t.Errorf("expected %s, got %s", docStr, string(out))
		}
	}
}

func TestBadXML(t *testing.T) {
	for _, testCase := range []string{
		"o_o",
		"<abc",
		"<account><amount>7",
		`<html><body></body></html>`,
		`<account><amount></amount></account>`,
		`<account><amount>nope</amount></account>`,
		`0.333`,
	} {
		var doc struct {
			XMLName xml.Name `xml:"account"`
			Amount  Decimal  `xml:"amount"`
		}
		err := xml.Unmarshal([]byte(testCase), &doc)
		if err == nil {
			t.Errorf("expected error, got %+v", doc)
		}
	}
}

func TestDecimal_rescale(t *testing.T) {
	type Inp struct {
		int     int64
		exp     int32
		rescale int32
	}
	tests := map[Inp]string{
		Inp{1234, -3, -5}: "1.234",
		Inp{1234, -3, 0}:  "1.000",
		Inp{1234, 3, 0}:   "1234000",
		Inp{1234, -4, -4}: "0.1234",
	}

	// add negatives
	for p, s := range tests {
		if p.int > 0 {
			tests[Inp{-p.int, p.exp, p.rescale}] = "-" + s
		}
	}

	for input, s := range tests {
		d := NewDecimalFromInt(input.int, input.exp).rescale(input.rescale)

		if d.String() != s {
			t.Errorf("expected %s, got %s (%s, %d)",
				s, d.String(),
				d.value.String(), d.exp)
		}

		// test StringScaled
		s2 := NewDecimalFromInt(input.int, input.exp).StringScaled(input.rescale)
		if s2 != s {
			t.Errorf("expected %s, got %s", s, s2)
		}
	}
}

func TestDecimal_Floor(t *testing.T) {
	type testData struct {
		input    string
		expected string
	}
	tests := []testData{
		{"1.999", "1"},
		{"1", "1"},
		{"1.01", "1"},
		{"0", "0"},
		{"0.9", "0"},
		{"0.1", "0"},
		{"-0.9", "-1"},
		{"-0.1", "-1"},
		{"-1.00", "-1"},
		{"-1.01", "-2"},
		{"-1.999", "-2"},
	}
	for _, test := range tests {
		d, _ := ParseDecimal(test.input)
		expected, _ := ParseDecimal(test.expected)
		got := d.Floor()
		if !got.Equals(expected) {
			t.Errorf("Floor(%s): got %s, expected %s", d, got, expected)
		}
	}
}

func TestDecimal_Ceil(t *testing.T) {
	type testData struct {
		input    string
		expected string
	}
	tests := []testData{
		{"1.999", "2"},
		{"1", "1"},
		{"1.01", "2"},
		{"0", "0"},
		{"0.9", "1"},
		{"0.1", "1"},
		{"-0.9", "0"},
		{"-0.1", "0"},
		{"-1.00", "-1"},
		{"-1.01", "-1"},
		{"-1.999", "-1"},
	}
	for _, test := range tests {
		d, _ := ParseDecimal(test.input)
		expected, _ := ParseDecimal(test.expected)
		got := d.Ceil()
		if !got.Equals(expected) {
			t.Errorf("Ceil(%s): got %s, expected %s", d, got, expected)
		}
	}
}

func TestDecimal_RoundAndStringFixed(t *testing.T) {
	type testData struct {
		input         string
		places        int32
		expected      string
		expectedFixed string
	}
	tests := []testData{
		{"1.454", 0, "1", ""},
		{"1.454", 1, "1.5", ""},
		{"1.454", 2, "1.45", ""},
		{"1.454", 3, "1.454", ""},
		{"1.454", 4, "1.454", "1.4540"},
		{"1.454", 5, "1.454", "1.45400"},
		{"1.554", 0, "2", ""},
		{"1.554", 1, "1.6", ""},
		{"1.554", 2, "1.55", ""},
		{"0.554", 0, "1", ""},
		{"0.454", 0, "0", ""},
		{"0.454", 5, "0.454", "0.45400"},
		{"0", 0, "0", ""},
		{"0", 1, "0", "0.0"},
		{"0", 2, "0", "0.00"},
		{"0", -1, "0", ""},
		{"5", 2, "5", "5.00"},
		{"5", 1, "5", "5.0"},
		{"5", 0, "5", ""},
		{"500", 2, "500", "500.00"},
		{"545", -1, "550", ""},
		{"545", -2, "500", ""},
		{"545", -3, "1000", ""},
		{"545", -4, "0", ""},
		{"499", -3, "0", ""},
		{"499", -4, "0", ""},
	}

	// add negative number tests
	for _, test := range tests {
		expected := test.expected
		if expected != "0" {
			expected = "-" + expected
		}
		expectedStr := test.expectedFixed
		if strings.ContainsAny(expectedStr, "123456789") && expectedStr != "" {
			expectedStr = "-" + expectedStr
		}
		tests = append(tests,
			testData{"-" + test.input, test.places, expected, expectedStr})
	}

	for _, test := range tests {
		d, err := ParseDecimal(test.input)
		if err != nil {
			panic(err)
		}

		// test Round
		expected, err := ParseDecimal(test.expected)
		if err != nil {
			panic(err)
		}
		got := d.Round(test.places)
		if !got.Equals(expected) {
			t.Errorf("Rounding %s to %d places, got %s, expected %s",
				d, test.places, got, expected)
		}
		// test StringFixed
		if test.expectedFixed == "" {
			test.expectedFixed = test.expected
		}
		gotStr := d.StringFixed(test.places)
		if gotStr != test.expectedFixed {
			t.Errorf("(%s).StringFixed(%d): got %s, expected %s",
				d, test.places, gotStr, test.expectedFixed)
		}
	}
}

func TestDecimal_Uninitialized(t *testing.T) {
	a := Decimal{}
	b := Decimal{}

	decs := []Decimal{
		a,
		a.rescale(10),
		a.Abs(),
		a.Add(b),
		a.Sub(b),
		a.Mul(b),
		a.Floor(),
		a.Ceil(),
	}

	for _, d := range decs {
		if d.String() != "0" {
			t.Errorf("expected 0, got %s", d.String())
		}
		if d.StringFixed(3) != "0.000" {
			t.Errorf("expected 0, got %s", d.StringFixed(3))
		}
		if d.StringScaled(-2) != "0" {
			t.Errorf("expected 0, got %s", d.StringScaled(-2))
		}
	}

	if a.Cmp(b) != 0 {
		t.Errorf("a != b")
	}
	if a.Exponent() != 0 {
		t.Errorf("a.Exponent() != 0")
	}
	if a.IntPart() != 0 {
		t.Errorf("a.IntPar() != 0")
	}
	f, _ := a.Float64()
	if f != 0 {
		t.Errorf("a.Float64() != 0")
	}
	if a.Rat().RatString() != "0" {
		t.Errorf("a.Rat() != 0, got %s", a.Rat().RatString())
	}
}

func TestDecimal_Add(t *testing.T) {
	type Inp struct {
		a string
		b string
	}

	inputs := map[Inp]string{
		Inp{"2", "3"}:                     "5",
		Inp{"2454495034", "3451204593"}:   "5905699627",
		Inp{"24544.95034", ".3451204593"}: "24545.2954604593",
		Inp{".1", ".1"}:                   "0.2",
		Inp{".1", "-.1"}:                  "0.0",
		Inp{"0", "1.001"}:                 "1.001",
	}

	for inp, res := range inputs {
		a, err := ParseDecimal(inp.a)
		if err != nil {
			t.FailNow()
		}
		b, err := ParseDecimal(inp.b)
		if err != nil {
			t.FailNow()
		}
		c := a.Add(b)
		if c.String() != res {
			t.Errorf("expected %s, got %s", res, c.String())
		}
	}
}

func TestDecimal_Sub(t *testing.T) {
	type Inp struct {
		a string
		b string
	}

	inputs := map[Inp]string{
		Inp{"2", "3"}:                     "-1",
		Inp{"12", "3"}:                    "9",
		Inp{"-2", "9"}:                    "-11",
		Inp{"2454495034", "3451204593"}:   "-996709559",
		Inp{"24544.95034", ".3451204593"}: "24544.6052195407",
		Inp{".1", "-.1"}:                  "0.2",
		Inp{".1", ".1"}:                   "0.0",
		Inp{"0", "1.001"}:                 "-1.001",
		Inp{"1.001", "0"}:                 "1.001",
		Inp{"2.3", ".3"}:                  "2.0",
	}

	for inp, res := range inputs {
		a, err := ParseDecimal(inp.a)
		if err != nil {
			t.FailNow()
		}
		b, err := ParseDecimal(inp.b)
		if err != nil {
			t.FailNow()
		}
		c := a.Sub(b)
		if c.String() != res {
			t.Errorf("expected %s, got %s", res, c.String())
		}
	}
}

func TestDecimal_Mul(t *testing.T) {
	type Inp struct {
		a string
		b string
	}

	inputs := map[Inp]string{
		Inp{"2", "3"}:                     "6",
		Inp{"2454495034", "3451204593"}:   "8470964534836491162",
		Inp{"24544.95034", ".3451204593"}: "8470.964534836491162",
		Inp{".1", ".1"}:                   "0.01",
		Inp{"0", "1.001"}:                 "0.000",
	}

	for inp, res := range inputs {
		a, err := ParseDecimal(inp.a)
		if err != nil {
			t.FailNow()
		}
		b, err := ParseDecimal(inp.b)
		if err != nil {
			t.FailNow()
		}
		c := a.Mul(b)
		if c.String() != res {
			t.Errorf("expected %s, got %s", res, c.String())
		}
	}

	// positive scale
	c := NewDecimalFromInt(1234, 5).Mul(NewDecimalFromInt(45, -1))
	if c.String() != "555300000.0" {
		t.Errorf("Expected %s, got %s", "555300000.0", c.String())
	}
}

func TestDecimal_Div(t *testing.T) {
	type Inp struct {
		a string
		b string
	}

	inputs := map[Inp]string{
		Inp{"6", "3"}:                            "2.0000",
		Inp{"10", "2"}:                           "5.0000",
		Inp{"2.2", "1.1"}:                        "2.00000",
		Inp{"-2.2", "-1.1"}:                      "2.00000",
		Inp{"12.88", "5.6"}:                      "2.300000",
		Inp{"1023427554493", "43432632"}:         "23563.5629", // rounded
		Inp{"1", "434324545566634"}:              "0.0000",
		Inp{"1", "3"}:                            "0.3333",
		Inp{"2", "3"}:                            "0.6667", // rounded
		Inp{"10000", "3"}:                        "3333.3333",
		Inp{"10234274355545544493", "-3"}:        "-3411424785181848164.3333",
		Inp{"-4612301402398.4753343454", "23.5"}: "-196268144782.91384401469787",
	}

	for inp, expected := range inputs {
		num, err := ParseDecimal(inp.a)
		if err != nil {
			t.FailNow()
		}
		denom, err := ParseDecimal(inp.b)
		if err != nil {
			t.FailNow()
		}
		got := num.Div(denom)
		if got.String() != expected {
			t.Errorf("expected %s when dividing %v by %v, got %v",
				expected, num, denom, got)
		}
	}

	type Inp2 struct {
		n    int64
		exp  int32
		n2   int64
		exp2 int32
	}

	// test code path where exp > 0
	inputs2 := map[Inp2]string{
		Inp2{124, 10, 3, 1}: "41333333333.3333",
		Inp2{124, 10, 3, 0}: "413333333333.3333",
		Inp2{124, 10, 6, 1}: "20666666666.6667",
		Inp2{124, 10, 6, 0}: "206666666666.6667",
		Inp2{10, 10, 10, 1}: "1000000000.0000",
	}

	for inp, expectedAbs := range inputs2 {
		for i := -1; i <= 1; i += 2 {
			for j := -1; j <= 1; j += 2 {
				n := inp.n * int64(i)
				n2 := inp.n2 * int64(j)
				num := NewDecimalFromInt(n, inp.exp)
				denom := NewDecimalFromInt(n2, inp.exp2)
				expected := expectedAbs
				if i != j {
					expected = "-" + expectedAbs
				}
				got := num.Div(denom)
				if got.String() != expected {
					t.Errorf("expected %s when dividing %v by %v, got %v",
						expected, num, denom, got)
				}
			}
		}
	}
}

func TestDecimal_Overflow(t *testing.T) {
	if !didPanic(func() { NewDecimalFromInt(1, math.MinInt32).Mul(NewDecimalFromInt(1, math.MinInt32)) }) {
		t.Fatalf("should have gotten an overflow panic")
	}
	if !didPanic(func() { NewDecimalFromInt(1, math.MaxInt32).Mul(NewDecimalFromInt(1, math.MaxInt32)) }) {
		t.Fatalf("should have gotten an overflow panic")
	}
}

func TestIntPart(t *testing.T) {
	for _, testCase := range []struct {
		Dec     string
		IntPart int64
	}{
		{"0.01", 0},
		{"12.1", 12},
		{"9999.999", 9999},
		{"-32768.01234", -32768},
	} {
		d, err := ParseDecimal(testCase.Dec)
		if err != nil {
			t.Fatal(err)
		}
		if d.IntPart() != testCase.IntPart {
			t.Errorf("expect %d, got %d", testCase.IntPart, d.IntPart())
		}
	}
}

// old tests after this line

func TestDecimal_Scale(t *testing.T) {
	a := NewDecimalFromInt(1234, -3)
	if a.Exponent() != -3 {
		t.Errorf("error")
	}
}

func TestDecimal_Abs1(t *testing.T) {
	a := NewDecimalFromInt(-1234, -4)
	b := NewDecimalFromInt(1234, -4)

	c := a.Abs()
	if c.Cmp(b) != 0 {
		t.Errorf("error")
	}
}

func TestDecimal_Abs2(t *testing.T) {
	a := NewDecimalFromInt(-1234, -4)
	b := NewDecimalFromInt(1234, -4)

	c := b.Abs()
	if c.Cmp(a) == 0 {
		t.Errorf("error")
	}
}

func TestDecimal_Equal(t *testing.T) {
	a := NewDecimalFromInt(1234, 3)
	b := NewDecimalFromInt(1234, 3)

	if !a.Equals(b) {
		t.Errorf("%q should equal %q", a, b)
	}
}

func TestDecimal_ScalesNotEqual(t *testing.T) {
	a := NewDecimalFromInt(1234, 2)
	b := NewDecimalFromInt(1234, 3)
	if a.Equals(b) {
		t.Errorf("%q should not equal %q", a, b)
	}
}

func TestDecimal_Cmp1(t *testing.T) {
	a := NewDecimalFromInt(123, 3)
	b := NewDecimalFromInt(-1234, 2)

	if a.Cmp(b) != 1 {
		t.Errorf("Error")
	}
}

func TestDecimal_Cmp2(t *testing.T) {
	a := NewDecimalFromInt(123, 3)
	b := NewDecimalFromInt(1234, 2)

	if a.Cmp(b) != -1 {
		t.Errorf("Error")
	}
}

func TestDecimal_Cmp3(t *testing.T) {
	a := NewDecimalFromInt(2, 0)
	b := NewDecimalFromInt(3, 0)
	if n := a.Div(b).Mul(b).Cmp(a); n != -1 {
		t.Errorf("Error %d", n)
	}
}

func TestDecimalConvert(t *testing.T) {
	a, err := ConvertToDecimal(1235.3)
	if err != nil || a.String() != "1235.3" {
		t.Error("Error")
	}
	a, err = ConvertToDecimal(1235)
	if err != nil || a.String() != "1235" {
		t.Error("Error")
	}
	a, err = ConvertToDecimal("-234.23")
	if err != nil || a.String() != "-234.23" {
		t.Error("Error")
	}
	a, err = ConvertToDecimal(NewDecimalFromFloat(23.45))
	if err != nil || a.String() != "23.45" {
		t.Error("Error")
	}
}

func TestDecimalComplex(t *testing.T) {
	result := NewDecimalFromFloat(3).Div(NewDecimalFromFloat(5)).Mul(NewDecimalFromFloat(15)).String()
	if result != "9.0000" {
		t.Errorf("got %s", result)
	}
	result = NewDecimalFromFloat(3.553).Mul(NewDecimalFromFloat(2.34)).Div(NewDecimalFromFloat(24.8)).String()
	if result != "0.335242742" {
		t.Errorf("got %s", result)
	}
	result = NewDecimalFromFloat(4.67).Div(NewDecimalFromFloat(32.099)).Div(NewDecimalFromFloat(9.32)).String()
	if result != "0.0156102359" {
		t.Errorf("got %s", result)
	}
	a, _ := ParseDecimal("23.342983749572929843520960646")
	b, _ := ParseDecimal("23.234122323452345412351235351")
	result = a.Mul(b).String()
	if result != "542.353739831937742603572001528416" {
		t.Errorf("got %s", result)
	}
}

func TestDecimalRoundTruncateFragDigits(t *testing.T) {
	a, err := ConvertToDecimal("12.3456")
	if err != nil || a.fracDigits != 4 {
		t.Error("Error")
	}
	a = a.Truncate(3)
	if a.fracDigits != 3 {
		t.Error("Error")
	}
	if a.String() != "12.345" {
		t.Error("Error")
	}
	a = a.Round(2)
	if a.fracDigits != 2 {
		t.Error("Error")
	}
	if a.String() != "12.35" {
		t.Error("Error")
	}
}

func didPanic(f func()) bool {
	ret := false
	func() {

		defer func() {
			if message := recover(); message != nil {
				ret = true
			}
		}()

		// call the target function
		f()

	}()

	return ret
}

func TestDecimalScientificNotation(t *testing.T) {
	tbl := []struct {
		Input    string
		Expected float64
	}{
		{"314e-2", 3.14},
		{"1e2", 100},
		{"2E-1", 0.2},
		{"2E0", 2},
		{"2.2E-1", 0.22},
	}

	for _, c := range tbl {
		n, err := ParseDecimal(c.Input)
		if err != nil {
			t.Error(err)
		}

		f, _ := n.Float64()
		if f != c.Expected {
			t.Errorf("%f != %f", f, c.Expected)
		}
	}

	tblErr := []string{
		"12ee",
		"ae10",
		"12e1a",
		"12e1.2",
		"e1",
	}

	for _, c := range tblErr {
		_, err := ParseDecimal(c)
		if err == nil {
			t.Errorf("%s must be invalid decimal", c)
		}
	}
}
