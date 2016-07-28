// Copyright 2016 PingCAP, Inc.
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
	. "github.com/pingcap/check"
)

var _ = Suite(&testMyDecimalSuite{})

type testMyDecimalSuite struct {
}

func (s *testMyDecimalSuite) TestFromInt(c *C) {
	cases := []struct {
		input  int64
		output string
	}{
		{-12345, "-12345"},
		{-1, "-1"},
		{-9223372036854775807, "-9223372036854775807"},
		{-9223372036854775808, "-9223372036854775808"},
	}
	for _, ca := range cases {
		var dec MyDecimal
		dec.FromInt(ca.input)
		str, ec := dec.ToString(0, 0, 0)
		c.Check(ec, Equals, 0)
		c.Check(string(str), Equals, ca.output)
	}
}

func (s *testMyDecimalSuite) TestFromUint(c *C) {
	cases := []struct {
		input  uint64
		output string
	}{
		{12345, "12345"},
		{0, "0"},
		{18446744073709551615, "18446744073709551615"},
	}
	for _, ca := range cases {
		var dec MyDecimal
		dec.FromUint(ca.input)
		str, ec := dec.ToString(0, 0, 0)
		c.Check(ec, Equals, 0)
		c.Check(string(str), Equals, ca.output)
	}
}

func (s *testMyDecimalSuite) TestToInt(c *C) {
	cases := []struct {
		input   string
		output  int64
		errcode int
	}{
		{"18446744073709551615", 9223372036854775807, 2},
		{"-1", -1, 0},
		{"-1.23", -1, 1},
		{"-9223372036854775807", -9223372036854775807, 0},
		{"-9223372036854775808", -9223372036854775808, 0},
		{"9223372036854775808", 9223372036854775807, 2},
	}
	for _, ca := range cases {
		var dec MyDecimal
		dec.FromString([]byte(ca.input))
		result, ec := dec.ToInt()
		c.Check(ec, Equals, ca.errcode)
		c.Check(result, Equals, ca.output)
	}
}

func (s *testMyDecimalSuite) TestToUint(c *C) {
	cases := []struct {
		input   string
		output  uint64
		errcode int
	}{
		{"12345", 12345, 0},
		{"0", 0, 0},
		/* ULLONG_MAX = 18446744073709551615ULL */
		{"18446744073709551615", 18446744073709551615, 0},
		{"18446744073709551616", 18446744073709551615, 2},
		{"-1", 0, 2},
		{"1.23", 1, 1},
		{"9999999999999999999999999.000", 18446744073709551615, 2},
	}
	for _, ca := range cases {
		var dec MyDecimal
		dec.FromString([]byte(ca.input))
		result, ec := dec.ToUint()
		c.Check(ec, Equals, ca.errcode)
		c.Check(result, Equals, ca.output)
	}
}

func (s *testMyDecimalSuite) TestFromFloat(c *C) {
	cases := []struct {
		s string
		f float64
	}{
		{"12345", 12345},
		{"123.45", 123.45},
		{"-123.45", -123.45},
		{"0.00012345000098765", 0.00012345000098765},
		{"1234500009876.5", 1234500009876.5},
	}
	for _, ca := range cases {
		var dec MyDecimal
		dec.FromFloat64(ca.f)
		str, ec := dec.ToString(0, 0, 0)
		c.Check(ec, Equals, 0)
		c.Check(string(str), Equals, ca.s)
	}
}

func (s *testMyDecimalSuite) TestToFloat(c *C) {
	cases := []struct {
		s string
		f float64
	}{
		{"12345", 12345},
		{"123.45", 123.45},
		{"-123.45", -123.45},
		{"0.00012345000098765", 0.00012345000098765},
		{"1234500009876.5", 1234500009876.5},
	}
	for _, ca := range cases {
		var dec MyDecimal
		dec.FromString([]byte(ca.s))
		f, ec := dec.ToFloat64()
		c.Check(ec, Equals, 0)
		c.Check(f, Equals, ca.f)
	}
}

func (s *testMyDecimalSuite) TestShift(c *C) {
	type tcase struct {
		input   string
		shift   int
		output  string
		errcode int
	}
	var dotest = func(c *C, cases []tcase) {
		for _, ca := range cases {
			var dec MyDecimal
			ec := dec.FromString([]byte(ca.input))
			c.Check(ec, Equals, 0)
			origin := dec
			ec = dec.Shift(ca.shift)
			c.Check(ec, Equals, ca.errcode)
			result, ec := dec.ToString(0, 0, 0)
			c.Check(ec, Equals, 0)
			c.Check(string(result), Equals, ca.output, Commentf("origin:%s\ndec:%s", origin.String(), origin.String()))
		}
	}
	wordBufLen = maxWordBufLen
	cases := []tcase{
		{"123.123", 1, "1231.23", 0},
		{"123457189.123123456789000", 1, "1234571891.23123456789", 0},
		{"123457189.123123456789000", 8, "12345718912312345.6789", 0},
		{"123457189.123123456789000", 9, "123457189123123456.789", 0},
		{"123457189.123123456789000", 10, "1234571891231234567.89", 0},
		{"123457189.123123456789000", 17, "12345718912312345678900000", 0},
		{"123457189.123123456789000", 18, "123457189123123456789000000", 0},
		{"123457189.123123456789000", 19, "1234571891231234567890000000", 0},
		{"123457189.123123456789000", 26, "12345718912312345678900000000000000", 0},
		{"123457189.123123456789000", 27, "123457189123123456789000000000000000", 0},
		{"123457189.123123456789000", 28, "1234571891231234567890000000000000000", 0},
		{"000000000000000000000000123457189.123123456789000", 26, "12345718912312345678900000000000000", 0},
		{"00000000123457189.123123456789000", 27, "123457189123123456789000000000000000", 0},
		{"00000000000000000123457189.123123456789000", 28, "1234571891231234567890000000000000000", 0},
		{"123", 1, "1230", 0},
		{"123", 10, "1230000000000", 0},
		{".123", 1, "1.23", 0},
		{".123", 10, "1230000000", 0},
		{".123", 14, "12300000000000", 0},
		{"000.000", 1000, "0", 0},
		{"000.", 1000, "0", 0},
		{".000", 1000, "0", 0},
		{"1", 1000, "1", 2},
		{"123.123", -1, "12.3123", 0},
		{"123987654321.123456789000", -1, "12398765432.1123456789", 0},
		{"123987654321.123456789000", -2, "1239876543.21123456789", 0},
		{"123987654321.123456789000", -3, "123987654.321123456789", 0},
		{"123987654321.123456789000", -8, "1239.87654321123456789", 0},
		{"123987654321.123456789000", -9, "123.987654321123456789", 0},
		{"123987654321.123456789000", -10, "12.3987654321123456789", 0},
		{"123987654321.123456789000", -11, "1.23987654321123456789", 0},
		{"123987654321.123456789000", -12, "0.123987654321123456789", 0},
		{"123987654321.123456789000", -13, "0.0123987654321123456789", 0},
		{"123987654321.123456789000", -14, "0.00123987654321123456789", 0},
		{"00000087654321.123456789000", -14, "0.00000087654321123456789", 0},
	}
	dotest(c, cases)
	wordBufLen = 2
	cases = []tcase{
		{"123.123", -2, "1.23123", 0},
		{"123.123", -3, "0.123123", 0},
		{"123.123", -6, "0.000123123", 0},
		{"123.123", -7, "0.0000123123", 0},
		{"123.123", -15, "0.000000000000123123", 0},
		{"123.123", -16, "0.000000000000012312", 1},
		{"123.123", -17, "0.000000000000001231", 1},
		{"123.123", -18, "0.000000000000000123", 1},
		{"123.123", -19, "0.000000000000000012", 1},
		{"123.123", -20, "0.000000000000000001", 1},
		{"123.123", -21, "0", 1},
		{".000000000123", -1, "0.0000000000123", 0},
		{".000000000123", -6, "0.000000000000000123", 0},
		{".000000000123", -7, "0.000000000000000012", 1},
		{".000000000123", -8, "0.000000000000000001", 1},
		{".000000000123", -9, "0", 1},
		{".000000000123", 1, "0.00000000123", 0},
		{".000000000123", 8, "0.0123", 0},
		{".000000000123", 9, "0.123", 0},
		{".000000000123", 10, "1.23", 0},
		{".000000000123", 17, "12300000", 0},
		{".000000000123", 18, "123000000", 0},
		{".000000000123", 19, "1230000000", 0},
		{".000000000123", 20, "12300000000", 0},
		{".000000000123", 21, "123000000000", 0},
		{".000000000123", 22, "1230000000000", 0},
		{".000000000123", 23, "12300000000000", 0},
		{".000000000123", 24, "123000000000000", 0},
		{".000000000123", 25, "1230000000000000", 0},
		{".000000000123", 26, "12300000000000000", 0},
		{".000000000123", 27, "123000000000000000", 0},
		{".000000000123", 28, "0.000000000123", 2},
		{"123456789.987654321", -1, "12345678.998765432", 1},
		{"123456789.987654321", -2, "1234567.899876543", 1},
		{"123456789.987654321", -8, "1.234567900", 1},
		{"123456789.987654321", -9, "0.123456789987654321", 0},
		{"123456789.987654321", -10, "0.012345678998765432", 1},
		{"123456789.987654321", -17, "0.000000001234567900", 1},
		{"123456789.987654321", -18, "0.000000000123456790", 1},
		{"123456789.987654321", -19, "0.000000000012345679", 1},
		{"123456789.987654321", -26, "0.000000000000000001", 1},
		{"123456789.987654321", -27, "0", 1},
		{"123456789.987654321", 1, "1234567900", 1},
		{"123456789.987654321", 2, "12345678999", 1},
		{"123456789.987654321", 4, "1234567899877", 1},
		{"123456789.987654321", 8, "12345678998765432", 1},
		{"123456789.987654321", 9, "123456789987654321", 0},
		{"123456789.987654321", 10, "123456789.987654321", 2},
		{"123456789.987654321", 0, "123456789.987654321", 0},
	}
	dotest(c, cases)
	wordBufLen = maxWordBufLen
}

func (s *testMyDecimalSuite) TestRound(c *C) {
	type tcase struct {
		input   string
		scale   int
		mode    roundMode
		output  string
		errcode int
	}
	var doTest = func(c *C, cases []tcase) {
		for _, ca := range cases {
			var dec MyDecimal
			dec.FromString([]byte(ca.input))
			var rounded MyDecimal
			ec := dec.Round(&rounded, ca.scale, ca.mode)
			c.Check(ec, Equals, ca.errcode)
			result, ec := rounded.ToString(0, 0, 0)
			c.Check(string(result), Equals, ca.output)
		}
	}
	cases := []tcase{
		{"5678.123451", -4, truncate, "0", 0},
		{"5678.123451", -3, truncate, "5000", 0},
		{"5678.123451", -2, truncate, "5600", 0},
		{"5678.123451", -1, truncate, "5670", 0},
		{"5678.123451", 0, truncate, "5678", 0},
		{"5678.123451", 1, truncate, "5678.1", 0},
		{"5678.123451", 2, truncate, "5678.12", 0},
		{"5678.123451", 3, truncate, "5678.123", 0},
		{"5678.123451", 4, truncate, "5678.1234", 0},
		{"5678.123451", 5, truncate, "5678.12345", 0},
		{"5678.123451", 6, truncate, "5678.123451", 0},
		{"-5678.123451", -4, truncate, "0", 0},
		{"99999999999999999999999999999999999999", -31, truncate, "99999990000000000000000000000000000000", 0},
		{"123456789.987654321", 1, halfUp, "123456790.0", 0},
		{"15.1", 0, halfUp, "15", 0},
		{"15.5", 0, halfUp, "16", 0},
		{"15.9", 0, halfUp, "16", 0},
		{"-15.1", 0, halfUp, "-15", 0},
		{"-15.5", 0, halfUp, "-16", 0},
		{"-15.9", 0, halfUp, "-16", 0},
		{"15.1", 1, halfUp, "15.1", 0},
		{"-15.1", 1, halfUp, "-15.1", 0},
		{"15.17", 1, halfUp, "15.2", 0},
		{"15.4", -1, halfUp, "20", 0},
		{"-15.4", -1, halfUp, "-20", 0},
		{"5.4", -1, halfUp, "10", 0},
		{".999", 0, halfUp, "1", 0},
		{"999999999", -9, halfUp, "1000000000", 0},
		{"15.1", 0, halfEven, "15", 0},
		{"15.5", 0, halfEven, "16", 0},
		{"14.5", 0, halfEven, "14", 0},
		{"15.9", 0, halfEven, "16", 0},
		{"15.1", 0, ceiling, "16", 0},
		{"-15.1", 0, ceiling, "-15", 0},
		{"15.1", 0, floor, "15", 0},
		{"-15.1", 0, floor, "-16", 0},
		{"999999999999999999999.999", 0, ceiling, "1000000000000000000000", 0},
		{"-999999999999999999999.999", 0, floor, "-1000000000000000000000", 0},
	}
	doTest(c, cases)
}

func (s *testMyDecimalSuite) TestFromString(c *C) {
	type tcase struct {
		input   string
		output  string
		errcode int
	}
	cases := []tcase{
		{"12345", "12345", 0},
		{"12345.", "12345", 0},
		{"123.45.", "123.45", 0},
		{"-123.45.", "-123.45", 0},
		{".00012345000098765", "0.00012345000098765", 0},
		{".12345000098765", "0.12345000098765", 0},
		{"-.000000012345000098765", "-0.000000012345000098765", 0},
		{"1234500009876.5", "1234500009876.5", 0},
		{"123E5", "12300000", 0},
		{"123E-2", "1.23", 0},
	}
	for _, ca := range cases {
		var dec MyDecimal
		ec := dec.FromString([]byte(ca.input))
		c.Check(ec, Equals, ca.errcode)
		result, ec := dec.ToString(0, 0, 0)
		c.Check(ec, Equals, 0)
		c.Check(string(result), Equals, ca.output, Commentf("dec:%s", dec.String()))
	}
	wordBufLen = 1
	cases = []tcase{
		{"123450000098765", "98765", 2},
		{"123450.000098765", "123450", 1},
	}
	for _, ca := range cases {
		var dec MyDecimal
		ec := dec.FromString([]byte(ca.input))
		c.Check(ec, Equals, ca.errcode)
		result, ec := dec.ToString(0, 0, 0)
		c.Check(ec, Equals, 0)
		c.Check(string(result), Equals, ca.output, Commentf("dec:%s", dec.String()))
	}
	wordBufLen = maxWordBufLen
}

func (s *testMyDecimalSuite) TestToString(c *C) {
	type tcase struct {
		input   string
		prec    int
		dec     int
		filler  byte
		output  string
		errcode int
	}
	cases := []tcase{
		{"123.123", 0, 0, 0, "123.123", 0},
		/* For fixed precision, we no longer count the '.' here. */
		{"123.123", 6, 3, '0', "123.123", 0},
		{"123.123", 8, 3, '0', "00123.123", 0},
		{"123.123", 8, 4, '0', "0123.1230", 0},
		{"123.123", 8, 5, '0', "123.12300", 0},
		{"123.123", 8, 2, '0', "000123.12", 1},
		{"123.123", 8, 6, '0', "23.123000", 2},
	}
	for _, ca := range cases {
		var dec MyDecimal
		dec.FromString([]byte(ca.input))
		result, ec := dec.ToString(ca.prec, ca.dec, ca.filler)
		c.Check(ec, Equals, ca.errcode)
		c.Check(string(result), Equals, ca.output)
	}
}
