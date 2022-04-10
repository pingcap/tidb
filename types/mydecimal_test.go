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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package types

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFromInt(t *testing.T) {
	tests := []struct {
		input  int64
		output string
	}{
		{-12345, "-12345"},
		{-1, "-1"},
		{1, "1"},
		{-9223372036854775807, "-9223372036854775807"},
		{-9223372036854775808, "-9223372036854775808"},
	}
	for _, tt := range tests {
		dec := NewDecFromInt(tt.input)
		str := dec.ToString()
		require.Equal(t, tt.output, string(str))
	}
}

func TestFromUint(t *testing.T) {
	tests := []struct {
		input  uint64
		output string
	}{
		{12345, "12345"},
		{0, "0"},
		{18446744073709551615, "18446744073709551615"},
	}
	for _, tt := range tests {
		var dec MyDecimal
		dec.FromUint(tt.input)
		str := dec.ToString()
		require.Equal(t, tt.output, string(str))
	}
}

func TestToInt(t *testing.T) {
	tests := []struct {
		input  string
		output int64
		err    error
	}{
		{"18446744073709551615", 9223372036854775807, ErrOverflow},
		{"-1", -1, nil},
		{"1", 1, nil},
		{"-1.23", -1, ErrTruncated},
		{"-9223372036854775807", -9223372036854775807, nil},
		{"-9223372036854775808", -9223372036854775808, nil},
		{"9223372036854775808", 9223372036854775807, ErrOverflow},
		{"-9223372036854775809", -9223372036854775808, ErrOverflow},
	}
	for _, tt := range tests {
		var dec MyDecimal
		err := dec.FromString([]byte(tt.input))
		require.NoError(t, err)
		result, ec := dec.ToInt()
		require.Equal(t, tt.err, ec)
		require.Equal(t, tt.output, result)
	}
}

func TestToUint(t *testing.T) {
	tests := []struct {
		input  string
		output uint64
		err    error
	}{
		{"12345", 12345, nil},
		{"0", 0, nil},
		/* ULLONG_MAX = 18446744073709551615ULL */
		{"18446744073709551615", 18446744073709551615, nil},
		{"18446744073709551616", 18446744073709551615, ErrOverflow},
		{"-1", 0, ErrOverflow},
		{"1.23", 1, ErrTruncated},
		{"9999999999999999999999999.000", 18446744073709551615, ErrOverflow},
	}
	for _, tt := range tests {
		var dec MyDecimal
		err := dec.FromString([]byte(tt.input))
		require.NoError(t, err)
		result, ec := dec.ToUint()
		require.Equal(t, tt.err, ec)
		require.Equal(t, tt.output, result)
	}
}

func TestFromFloat(t *testing.T) {
	tests := []struct {
		s string
		f float64
	}{
		{"12345", 12345},
		{"123.45", 123.45},
		{"-123.45", -123.45},
		{"0.00012345000098765", 0.00012345000098765},
		{"1234500009876.5", 1234500009876.5},
	}
	for _, tt := range tests {
		dec := NewDecFromFloatForTest(tt.f)
		str := dec.ToString()
		require.Equal(t, tt.s, string(str))
	}
}

func TestToFloat(t *testing.T) {
	tests := []struct {
		in  string
		out string
		err error
	}{
		{"12345", "12345", nil},
		{"123.45", "123.45", nil},
		{"-123.45", "-123.45", nil},
		{"0.00012345000098765", "0.00012345000098765", nil},
		{"1234500009876.5", "1234500009876.5", nil},
		{"1e39", "1e39", nil},
		{"1e-39", "1e-39", nil},
		{"1e00", "1", nil},
		{"1e001", "10", nil},
		{"-9223372036854775807", "-9223372036854775807", nil},
		{"-9223372036854775808", "-9223372036854775808", nil},
		{"18446744073709551615", "18446744073709551615", nil},
		{"123456789.987654321", "123456789.987654321", nil},
		// Testcases from https://github.com/golang/go/blob/master/src/strconv/atof_test.go#L24
		{"1", "1", nil},
		{"+1", "1", nil},
		{"1e23", "1e+23", nil},
		{"1E23", "1e+23", nil},
		{"100000000000000000000000", "1e+23", nil},
		{"123456700", "1.234567e+08", nil},
		{"99999999999999974834176", "9.999999999999997e+22", nil},
		{"100000000000000000000001", "1.0000000000000001e+23", nil},
		{"100000000000000008388608", "1.0000000000000001e+23", nil},
		{"100000000000000016777215", "1.0000000000000001e+23", nil},
		{"100000000000000016777216", "1.0000000000000003e+23", nil},
		{"-1", "-1", nil},
		{"-0.1", "-0.1", nil},
		{"-0", "-0", nil},
		{"1e-20", "1e-20", nil},
		{"625e-3", "0.625", nil},
		{"0", "0", nil},
		{"22.222222222222222", "22.22222222222222", nil},
		{"1.00000000000000011102230246251565404236316680908203125", "1", nil},
		{"1.00000000000000011102230246251565404236316680908203124", "1", nil},
		{"1.00000000000000011102230246251565404236316680908203126", "1.0000000000000002", nil},
		{"1.00000000000000033306690738754696212708950042724609375", "1.0000000000000004", nil},
		{"1090544144181609348671888949248", "1.0905441441816093e+30", nil},
		{"1090544144181609348835077142190", "1.0905441441816094e+30", nil},
	}
	for _, ca := range tests {
		var dec MyDecimal
		err := dec.FromString([]byte(ca.in))
		require.NoError(t, err)
		f, err := dec.ToFloat64()
		require.NoError(t, err)
		std, err := strconv.ParseFloat(ca.out, 64)
		require.NoError(t, err)
		require.Equal(t, std, f)
	}
}

func TestToHashKey(t *testing.T) {
	tests := []struct {
		numbers []string
	}{
		{[]string{"1.1", "1.1000", "1.1000000", "1.10000000000", "01.1", "0001.1", "001.1000000"}},
		{[]string{"-1.1", "-1.1000", "-1.1000000", "-1.10000000000", "-01.1", "-0001.1", "-001.1000000"}},
		{[]string{".1", "0.1", "0.10", "000000.1", ".10000", "0000.10000", "000000000000000000.1"}},
		{[]string{"0", "0000", ".0", ".00000", "00000.00000", "-0", "-0000", "-.0", "-.00000", "-00000.00000"}},
		{[]string{".123456789123456789", ".1234567891234567890", ".12345678912345678900", ".123456789123456789000", ".1234567891234567890000", "0.123456789123456789",
			".1234567891234567890000000000", "0000000.123456789123456789000"}},
		{[]string{"12345", "012345", "0012345", "0000012345", "0000000012345", "00000000000012345", "12345.", "12345.00", "12345.000000000", "000012345.0000"}},
		{[]string{"123E5", "12300000", "00123E5", "000000123E5", "12300000.00000000"}},
		{[]string{"123E-2", "1.23", "00000001.23", "1.2300000000000000", "000000001.23000000000000"}},
	}
	for _, ca := range tests {
		keys := make([]string, 0, len(ca.numbers))
		for _, num := range ca.numbers {
			var dec MyDecimal
			require.NoError(t, dec.FromString([]byte(num)))
			key, err := dec.ToHashKey()
			require.NoError(t, err)
			keys = append(keys, string(key))
		}

		for i := 1; i < len(keys); i++ {
			require.Equal(t, keys[0], keys[i])
		}
	}

	binTests := []struct {
		hashNumbers []string
		binNumbers  []string
	}{
		{[]string{"1.1", "1.1000", "1.1000000", "1.10000000000", "01.1", "0001.1", "001.1000000"},
			[]string{"1.1", "0001.1", "01.1"}},
		{[]string{"-1.1", "-1.1000", "-1.1000000", "-1.10000000000", "-01.1", "-0001.1", "-001.1000000"},
			[]string{"-1.1", "-0001.1", "-01.1"}},
		{[]string{".1", "0.1", "000000.1", ".10000", "0000.10000", "000000000000000000.1"},
			[]string{".1", "0.1", "000000.1", "00.1"}},
		{[]string{"0", "0000", ".0", ".00000", "00000.00000", "-0", "-0000", "-.0", "-.00000", "-00000.00000"},
			[]string{"0", "0000", "00", "-0", "-00", "-000000"}},
		{[]string{".123456789123456789", ".1234567891234567890", ".12345678912345678900", ".123456789123456789000", ".1234567891234567890000", "0.123456789123456789",
			".1234567891234567890000000000", "0000000.123456789123456789000"},
			[]string{".123456789123456789", "0.123456789123456789", "0000.123456789123456789", "0000000.123456789123456789"}},
		{[]string{"12345", "012345", "0012345", "0000012345", "0000000012345", "00000000000012345", "12345.", "12345.00", "12345.000000000", "000012345.0000"},
			[]string{"12345", "012345", "000012345", "000000000000012345"}},
		{[]string{"123E5", "12300000", "00123E5", "000000123E5", "12300000.00000000"},
			[]string{"12300000", "123E5", "00123E5", "0000000000123E5"}},
		{[]string{"123E-2", "1.23", "00000001.23", "1.2300000000000000", "000000001.23000000000000"},
			[]string{"123E-2", "1.23", "000001.23", "0000000000001.23"}},
	}
	for _, ca := range binTests {
		keys := make([]string, 0, len(ca.hashNumbers)+len(ca.binNumbers))
		for _, num := range ca.hashNumbers {
			var dec MyDecimal
			require.NoError(t, dec.FromString([]byte(num)))
			key, err := dec.ToHashKey()
			// remove digit len
			key = key[:len(key)-1]
			require.NoError(t, err)
			keys = append(keys, string(key))
		}
		for _, num := range ca.binNumbers {
			var dec MyDecimal
			require.NoError(t, dec.FromString([]byte(num)))
			prec, frac := dec.PrecisionAndFrac() // remove leading zeros but trailing zeros remain
			key, err := dec.ToBin(prec, frac)
			require.NoError(t, err)
			keys = append(keys, string(key))
		}

		for i := 1; i < len(keys); i++ {
			require.Equal(t, keys[0], keys[i])
		}
	}
}

func TestRemoveTrailingZeros(t *testing.T) {
	tests := []string{
		"0", "0.0", ".0", ".00000000", "0.0000", "0000", "0000.0", "0000.000",
		"-0", "-0.0", "-.0", "-.00000000", "-0.0000", "-0000", "-0000.0", "-0000.000",
		"123123123", "213123.", "21312.000", "21321.123", "213.1230000", "213123.000123000",
		"-123123123", "-213123.", "-21312.000", "-21321.123", "-213.1230000", "-213123.000123000",
		"123E5", "12300E-5", "0.00100E1", "0.001230E-3",
		"123987654321.123456789000", "000000000123", "123456789.987654321", "999.999000",
	}
	for _, ca := range tests {
		var dec MyDecimal
		require.NoError(t, dec.FromString([]byte(ca)))

		// calculate the number of digits after point but trailing zero
		digitsFracExp := 0
		str := string(dec.ToString())
		point := strings.Index(str, ".")
		if point != -1 {
			pos := len(str) - 1
			for pos > point {
				if str[pos] != '0' {
					break
				}
				pos--
			}
			digitsFracExp = pos - point
		}

		_, digitsFrac := dec.removeTrailingZeros()
		require.Equal(t, digitsFracExp, digitsFrac)
	}
}

func TestRoundWithHalfEven(t *testing.T) {
	tests := []struct {
		input  string
		scale  int
		output string
		err    error
	}{
		{"123456789.987654321", 1, "123456790.0", nil},
		{"15.1", 0, "15", nil},
		{"15.5", 0, "16", nil},
		{"15.9", 0, "16", nil},
		{"-15.1", 0, "-15", nil},
		{"-15.5", 0, "-16", nil},
		{"-15.9", 0, "-16", nil},
		{"15.1", 1, "15.1", nil},
		{"-15.1", 1, "-15.1", nil},
		{"15.17", 1, "15.2", nil},
		{"15.4", -1, "20", nil},
		{"-15.4", -1, "-20", nil},
		{"5.4", -1, "10", nil},
		{".999", 0, "1", nil},
		{"999999999", -9, "1000000000", nil},
	}

	for _, ca := range tests {
		var dec MyDecimal
		err := dec.FromString([]byte(ca.input))
		require.NoError(t, err)
		var rounded MyDecimal
		err = dec.Round(&rounded, ca.scale, ModeHalfUp)
		require.Equal(t, ca.err, err)
		result := rounded.ToString()
		require.Equal(t, ca.output, string(result))
	}
}

func TestRoundWithTruncate(t *testing.T) {
	tests := []struct {
		input  string
		scale  int
		output string
		err    error
	}{
		{"123456789.987654321", 1, "123456789.9", nil},
		{"15.1", 0, "15", nil},
		{"15.5", 0, "15", nil},
		{"15.9", 0, "15", nil},
		{"-15.1", 0, "-15", nil},
		{"-15.5", 0, "-15", nil},
		{"-15.9", 0, "-15", nil},
		{"15.1", 1, "15.1", nil},
		{"-15.1", 1, "-15.1", nil},
		{"15.17", 1, "15.1", nil},
		{"15.4", -1, "10", nil},
		{"-15.4", -1, "-10", nil},
		{"5.4", -1, "0", nil},
		{".999", 0, "0", nil},
		{"999999999", -9, "0", nil},
	}
	for _, ca := range tests {
		var dec MyDecimal
		err := dec.FromString([]byte(ca.input))
		require.NoError(t, err)
		var rounded MyDecimal
		err = dec.Round(&rounded, ca.scale, ModeTruncate)
		require.Equal(t, ca.err, err)
		result := rounded.ToString()
		require.Equal(t, ca.output, string(result))
	}
}

func TestRoundWithCeil(t *testing.T) {
	tests := []struct {
		input  string
		scale  int
		output string
		err    error
	}{
		{"123456789.987654321", 1, "123456790.0", nil},
		{"15.1", 0, "16", nil},
		{"15.5", 0, "16", nil},
		{"15.9", 0, "16", nil},
		//TODO:fix me
		{"-15.1", 0, "-16", nil},
		{"-15.5", 0, "-16", nil},
		{"-15.9", 0, "-16", nil},
		{"15.1", 1, "15.1", nil},
		{"-15.1", 1, "-15.1", nil},
		{"15.17", 1, "15.2", nil},
		{"15.4", -1, "20", nil},
		{"-15.4", -1, "-20", nil},
		{"5.4", -1, "10", nil},
		{".999", 0, "1", nil},
		{"999999999", -9, "1000000000", nil},
	}
	for _, ca := range tests {
		var dec MyDecimal
		err := dec.FromString([]byte(ca.input))
		require.NoError(t, err)
		var rounded MyDecimal
		err = dec.Round(&rounded, ca.scale, ModeCeiling)
		require.Equal(t, ca.err, err)
		result := rounded.ToString()
		require.Equal(t, ca.output, string(result))
	}
}

func TestToString(t *testing.T) {
	type tcase struct {
		input  string
		output string
	}
	tests := []tcase{
		{"123.123", "123.123"},
		{"123.1230", "123.1230"},
		{"00123.123", "123.123"},
	}
	for _, ca := range tests {
		var dec MyDecimal
		err := dec.FromString([]byte(ca.input))
		require.NoError(t, err)
		result := dec.ToString()
		require.Equal(t, ca.output, string(result))
	}
}

func TestToBinFromBin(t *testing.T) {
	type tcase struct {
		input     string
		precision int
		frac      int
		output    string
		err       error
	}
	tests := []tcase{
		{"-10.55", 4, 2, "-10.55", nil},
		{"0.0123456789012345678912345", 30, 25, "0.0123456789012345678912345", nil},
		{"12345", 5, 0, "12345", nil},
		{"12345", 10, 3, "12345.000", nil},
		{"123.45", 10, 3, "123.450", nil},
		{"-123.45", 20, 10, "-123.4500000000", nil},
		{".00012345000098765", 15, 14, "0.00012345000098", ErrTruncated},
		{".00012345000098765", 22, 20, "0.00012345000098765000", nil},
		{".12345000098765", 30, 20, "0.12345000098765000000", nil},
		{"-.000000012345000098765", 30, 20, "-0.00000001234500009876", ErrTruncated},
		{"1234500009876.5", 30, 5, "1234500009876.50000", nil},
		{"111111111.11", 10, 2, "11111111.11", ErrOverflow},
		{"000000000.01", 7, 3, "0.010", nil},
		{"123.4", 10, 2, "123.40", nil},
		{"1000", 3, 0, "0", ErrOverflow},
		{"0.1", 1, 1, "0.1", nil},
		{"0.100", 1, 1, "0.1", ErrTruncated},
		{"0.1000", 1, 1, "0.1", ErrTruncated},
		{"0.10000", 1, 1, "0.1", ErrTruncated},
		{"0.100000", 1, 1, "0.1", ErrTruncated},
		{"0.1000000", 1, 1, "0.1", ErrTruncated},
		{"0.10", 1, 1, "0.1", ErrTruncated},
		{"0000000000000000000000000000000000000000000.000000000000123000000000000000", 15, 15, "0.000000000000123", ErrTruncated},
		{"00000000000000000000000000000.00000000000012300", 15, 15, "0.000000000000123", ErrTruncated},
		{"0000000000000000000000000000000000000000000.0000000000001234000000000000000", 16, 16, "0.0000000000001234", ErrTruncated},
		{"00000000000000000000000000000.000000000000123400", 16, 16, "0.0000000000001234", ErrTruncated},
		{"0.1", 2, 2, "0.10", nil},
		{"0.10", 3, 3, "0.100", nil},
		{"0.1", 3, 1, "0.1", nil},
		{"0.0000000000001234", 32, 17, "0.00000000000012340", nil},
		{"0.0000000000001234", 20, 20, "0.00000000000012340000", nil},
	}
	for _, ca := range tests {
		var dec MyDecimal
		err := dec.FromString([]byte(ca.input))
		require.NoError(t, err)
		buf, err := dec.ToBin(ca.precision, ca.frac)
		require.Equal(t, ca.err, err)
		var dec2 MyDecimal
		_, err = dec2.FromBin(buf, ca.precision, ca.frac)
		require.NoError(t, err)
		str := dec2.ToString()
		require.Equal(t, ca.output, string(str))
	}
	var dec MyDecimal
	dec.FromInt(1)
	errTests := []struct {
		prec int
		frac int
	}{
		{82, 1},
		{-1, 1},
		{10, 31},
		{10, -1},
	}
	for _, tt := range errTests {
		_, err := dec.ToBin(tt.prec, tt.frac)
		require.True(t, ErrBadNumber.Equal(err))
	}
}

func TestCompareMyDecimal(t *testing.T) {
	type tcase struct {
		a   string
		b   string
		cmp int
	}
	tests := []tcase{
		{"12", "13", -1},
		{"13", "12", 1},
		{"-10", "10", -1},
		{"10", "-10", 1},
		{"-12", "-13", 1},
		{"0", "12", -1},
		{"-10", "0", -1},
		{"4", "4", 0},
		{"-1.1", "-1.2", 1},
		{"1.2", "1.1", 1},
		{"1.1", "1.2", -1},
	}
	for _, tt := range tests {
		var a, b MyDecimal
		err := a.FromString([]byte(tt.a))
		require.NoError(t, err)
		err = b.FromString([]byte(tt.b))
		require.NoError(t, err)
		require.Equal(t, tt.cmp, a.Compare(&b))
	}
}

func TestMaxDecimal(t *testing.T) {
	type tcase struct {
		prec   int
		frac   int
		result string
	}
	tests := []tcase{
		{1, 1, "0.9"},
		{1, 0, "9"},
		{2, 1, "9.9"},
		{4, 2, "99.99"},
		{6, 3, "999.999"},
		{8, 4, "9999.9999"},
		{10, 5, "99999.99999"},
		{12, 6, "999999.999999"},
		{14, 7, "9999999.9999999"},
		{16, 8, "99999999.99999999"},
		{18, 9, "999999999.999999999"},
		{20, 10, "9999999999.9999999999"},
		{20, 20, "0.99999999999999999999"},
		{20, 0, "99999999999999999999"},
		{40, 20, "99999999999999999999.99999999999999999999"},
	}
	for _, tt := range tests {
		var dec MyDecimal
		maxDecimal(tt.prec, tt.frac, &dec)
		str := dec.ToString()
		require.Equal(t, tt.result, string(str))
	}
}

func TestNegMyDecimal(t *testing.T) {
	type testCase struct {
		a      string
		result string
	}
	tests := []testCase{
		{"-0.0000000000000000000000000000000000000000000000000017382578996420603", "0.0000000000000000000000000000000000000000000000000017382578996420603"},
		{"-13890436710184412000000000000000000000000000000000000000000000000000000000000", "13890436710184412000000000000000000000000000000000000000000000000000000000000"},
		{"0", "0"},
	}
	for _, tt := range tests {
		a := NewDecFromStringForTest(tt.a)
		negResult := DecimalNeg(a)
		result := negResult.ToString()
		require.Equal(t, tt.result, string(result))
	}
}

func TestAddMyDecimal(t *testing.T) {
	type testCase struct {
		a      string
		b      string
		result string
		err    error
	}
	tests := []testCase{
		{".00012345000098765", "123.45", "123.45012345000098765", nil},
		{".1", ".45", "0.55", nil},
		{"1234500009876.5", ".00012345000098765", "1234500009876.50012345000098765", nil},
		{"9999909999999.5", ".555", "9999910000000.055", nil},
		{"99999999", "1", "100000000", nil},
		{"989999999", "1", "990000000", nil},
		{"999999999", "1", "1000000000", nil},
		{"12345", "123.45", "12468.45", nil},
		{"-12345", "-123.45", "-12468.45", nil},
		{"-12345", "123.45", "-12221.55", nil},
		{"12345", "-123.45", "12221.55", nil},
		{"123.45", "-12345", "-12221.55", nil},
		{"-123.45", "12345", "12221.55", nil},
		{"5", "-6.0", "-1.0", nil},
		{"2" + strings.Repeat("1", 71), strings.Repeat("8", 81), "8888888890" + strings.Repeat("9", 71), nil},
		{"-1234.1234", "1234.1234", "0.0000", nil},
	}
	for _, tt := range tests {
		a := NewDecFromStringForTest(tt.a)
		b := NewDecFromStringForTest(tt.b)
		var sum MyDecimal
		err := DecimalAdd(a, b, &sum)
		require.Equal(t, tt.err, err)
		result := sum.ToString()
		require.Equal(t, tt.result, string(result))
	}
}

func TestSubMyDecimal(t *testing.T) {
	type tcase struct {
		a      string
		b      string
		result string
		err    error
	}
	tests := []tcase{
		{".00012345000098765", "123.45", "-123.44987654999901235", nil},
		{"1234500009876.5", ".00012345000098765", "1234500009876.49987654999901235", nil},
		{"9999900000000.5", ".555", "9999899999999.945", nil},
		{"1111.5551", "1111.555", "0.0001", nil},
		{".555", ".555", "0.000", nil},
		{"10000000", "1", "9999999", nil},
		{"1000001000", ".1", "1000000999.9", nil},
		{"1000000000", ".1", "999999999.9", nil},
		{"12345", "123.45", "12221.55", nil},
		{"-12345", "-123.45", "-12221.55", nil},
		{"123.45", "12345", "-12221.55", nil},
		{"-123.45", "-12345", "12221.55", nil},
		{"-12345", "123.45", "-12468.45", nil},
		{"12345", "-123.45", "12468.45", nil},
		{"12.12", "12.12", "0.00", nil},
	}
	for _, tt := range tests {
		var a, b, sum MyDecimal
		err := a.FromString([]byte(tt.a))
		require.NoError(t, err)
		err = b.FromString([]byte(tt.b))
		require.NoError(t, err)
		err = DecimalSub(&a, &b, &sum)
		require.Equal(t, tt.err, err)
		result := sum.ToString()
		require.Equal(t, tt.result, string(result))
	}
}

func TestMulMyDecimal(t *testing.T) {
	type tcase struct {
		a      string
		b      string
		result string
		err    error
	}
	tests := []tcase{
		{"12", "10", "120", nil},
		{"-123.456", "98765.4321", "-12193185.1853376", nil},
		{"-123456000000", "98765432100000", "-12193185185337600000000000", nil},
		{"123456", "987654321", "121931851853376", nil},
		{"123456", "9876543210", "1219318518533760", nil},
		{"123", "0.01", "1.23", nil},
		{"123", "0", "0", nil},
		{"-0.0000000000000000000000000000000000000000000000000017382578996420603", "-13890436710184412000000000000000000000000000000000000000000000000000000000000", "0.000000000000000000000000000000", ErrTruncated},
		{"1" + strings.Repeat("0", 60), "1" + strings.Repeat("0", 60), "0", ErrOverflow},
		{"0.5999991229316", "0.918755041726043", "0.5512522192246113614062276588", nil},
		{"0.5999991229317", "0.918755041726042", "0.5512522192247026369112773314", nil},
		{"0.000", "-1", "0.000", nil},
	}
	for _, tt := range tests {
		var a, b, product MyDecimal
		err := a.FromString([]byte(tt.a))
		require.NoError(t, err)
		err = b.FromString([]byte(tt.b))
		require.NoError(t, err)
		err = DecimalMul(&a, &b, &product)
		require.Equal(t, tt.err, err)
		result := product.String()
		require.Equal(t, tt.result, result)
	}
}

func TestDivModMyDecimal(t *testing.T) {
	type tcase struct {
		a      string
		b      string
		result string
		err    error
	}
	tests := []tcase{
		{"120", "10", "12.000000000", nil},
		{"123", "0.01", "12300.000000000", nil},
		{"120", "100000000000.00000", "0.000000001200000000", nil},
		{"123", "0", "", ErrDivByZero},
		{"0", "0", "", ErrDivByZero},
		{"-12193185.1853376", "98765.4321", "-123.456000000000000000", nil},
		{"121931851853376", "987654321", "123456.000000000", nil},
		{"0", "987", "0.00000", nil},
		{"1", "3", "0.333333333", nil},
		{"1.000000000000", "3", "0.333333333333333333", nil},
		{"1", "1", "1.000000000", nil},
		{"0.0123456789012345678912345", "9999999999", "0.000000000001234567890246913578148141", nil},
		{"10.333000000", "12.34500", "0.837019036046982584042122316", nil},
		{"10.000000000060", "2", "5.000000000030000000", nil},
		{"51", "0.003430", "14868.804664723032069970", nil},
	}
	for _, tt := range tests {
		var a, b, to MyDecimal
		err := a.FromString([]byte(tt.a))
		require.NoError(t, err)
		err = b.FromString([]byte(tt.b))
		require.NoError(t, err)
		err = DecimalDiv(&a, &b, &to, 5)
		require.Equal(t, tt.err, err)
		if tt.err == ErrDivByZero {
			continue
		}
		result := to.ToString()
		require.Equal(t, tt.result, string(result))
	}

	tests = []tcase{
		{"234", "10", "4", nil},
		{"234.567", "10.555", "2.357", nil},
		{"-234.567", "10.555", "-2.357", nil},
		{"234.567", "-10.555", "2.357", nil},
		{"99999999999999999999999999999999999999", "3", "0", nil},
		{"51", "0.003430", "0.002760", nil},
		{"0.0000000001", "1.0", "0.0000000001", nil},
		{"0.000", "0.1", "0.000", nil},
	}
	for _, tt := range tests {
		var a, b, to MyDecimal
		err := a.FromString([]byte(tt.a))
		require.NoError(t, err)
		err = b.FromString([]byte(tt.b))
		require.NoError(t, err)
		ec := DecimalMod(&a, &b, &to)
		require.Equal(t, tt.err, ec)
		if tt.err == ErrDivByZero {
			continue
		}
		result := to.ToString()
		require.Equal(t, tt.result, string(result))
	}

	tests = []tcase{
		{"1", "1", "1.0000", nil},
		{"1.00", "1", "1.000000", nil},
		{"1", "1.000", "1.0000", nil},
		{"2", "3", "0.6667", nil},
		{"51", "0.003430", "14868.8047", nil},
		{"0.000", "0.1", "0.0000000", nil},
	}
	for _, tt := range tests {
		var a, b, to MyDecimal
		err := a.FromString([]byte(tt.a))
		require.NoError(t, err)
		err = b.FromString([]byte(tt.b))
		require.NoError(t, err)
		ec := DecimalDiv(&a, &b, &to, DivFracIncr)
		require.Equal(t, tt.err, ec)
		if tt.err == ErrDivByZero {
			continue
		}
		require.Equal(t, tt.result, to.String())
	}

	tests = []tcase{
		{"1", "2.0", "1.0", nil},
		{"1.0", "2", "1.0", nil},
		{"2.23", "3", "2.23", nil},
		{"51", "0.003430", "0.002760", nil},
	}
	for _, tt := range tests {
		var a, b, to MyDecimal
		err := a.FromString([]byte(tt.a))
		require.NoError(t, err)
		err = b.FromString([]byte(tt.b))
		require.NoError(t, err)
		ec := DecimalMod(&a, &b, &to)
		require.Equal(t, tt.err, ec)
		if tt.err == ErrDivByZero {
			continue
		}
		require.Equal(t, tt.result, to.String())
	}
}

func TestMaxOrMinMyDecimal(t *testing.T) {
	type tcase struct {
		neg    bool
		prec   int
		frac   int
		result string
	}
	tests := []tcase{
		{true, 2, 1, "-9.9"},
		{false, 1, 1, "0.9"},
		{true, 1, 0, "-9"},
		{false, 0, 0, "0"},
		{false, 4, 2, "99.99"},
	}
	for _, tt := range tests {
		dec := NewMaxOrMinDec(tt.neg, tt.prec, tt.frac)
		require.Equal(t, tt.result, dec.String())
	}
}

func TestReset(t *testing.T) {
	var x1, y1, z1 MyDecimal
	require.NoError(t, x1.FromString([]byte("38520.130741106671")))
	require.NoError(t, y1.FromString([]byte("9863.944799797851")))
	require.NoError(t, DecimalAdd(&x1, &y1, &z1))

	var x2, y2, z2 MyDecimal
	require.NoError(t, x2.FromString([]byte("121519.080207244")))
	require.NoError(t, y2.FromString([]byte("54982.444519146")))
	require.NoError(t, DecimalAdd(&x2, &y2, &z2))

	require.NoError(t, DecimalAdd(&x2, &y2, &z1))
	require.Equal(t, z2, z1)
}

// this test will change global variable `wordBufLen`, so it must run in serial
func TestShiftMyDecimal(t *testing.T) {
	type tcase struct {
		input  string
		shift  int
		output string
		err    error
	}

	var dotest = func(tests []tcase) {
		for _, test := range tests {
			t.Run(fmt.Sprintf("%v (shift: %v, wordBufLen: %v)", test.input, test.shift, wordBufLen), func(t *testing.T) {
				var dec MyDecimal
				require.NoError(t, dec.FromString([]byte(test.input)))
				require.Equal(t, test.err, dec.Shift(test.shift))
				require.Equal(t, test.output, string(dec.ToString()))
			})
		}
	}

	wordBufLen = maxWordBufLen
	tests := []tcase{
		{"123.123", 1, "1231.23", nil},
		{"123457189.123123456789000", 1, "1234571891.23123456789", nil},
		{"123457189.123123456789000", 8, "12345718912312345.6789", nil},
		{"123457189.123123456789000", 9, "123457189123123456.789", nil},
		{"123457189.123123456789000", 10, "1234571891231234567.89", nil},
		{"123457189.123123456789000", 17, "12345718912312345678900000", nil},
		{"123457189.123123456789000", 18, "123457189123123456789000000", nil},
		{"123457189.123123456789000", 19, "1234571891231234567890000000", nil},
		{"123457189.123123456789000", 26, "12345718912312345678900000000000000", nil},
		{"123457189.123123456789000", 27, "123457189123123456789000000000000000", nil},
		{"123457189.123123456789000", 28, "1234571891231234567890000000000000000", nil},
		{"000000000000000000000000123457189.123123456789000", 26, "12345718912312345678900000000000000", nil},
		{"00000000123457189.123123456789000", 27, "123457189123123456789000000000000000", nil},
		{"00000000000000000123457189.123123456789000", 28, "1234571891231234567890000000000000000", nil},
		{"123", 1, "1230", nil},
		{"123", 10, "1230000000000", nil},
		{".123", 1, "1.23", nil},
		{".123", 10, "1230000000", nil},
		{".123", 14, "12300000000000", nil},
		{"000.000", 1000, "0", nil},
		{"000.", 1000, "0", nil},
		{".000", 1000, "0", nil},
		{"1", 1000, "1", ErrOverflow},
		{"123.123", -1, "12.3123", nil},
		{"123987654321.123456789000", -1, "12398765432.1123456789", nil},
		{"123987654321.123456789000", -2, "1239876543.21123456789", nil},
		{"123987654321.123456789000", -3, "123987654.321123456789", nil},
		{"123987654321.123456789000", -8, "1239.87654321123456789", nil},
		{"123987654321.123456789000", -9, "123.987654321123456789", nil},
		{"123987654321.123456789000", -10, "12.3987654321123456789", nil},
		{"123987654321.123456789000", -11, "1.23987654321123456789", nil},
		{"123987654321.123456789000", -12, "0.123987654321123456789", nil},
		{"123987654321.123456789000", -13, "0.0123987654321123456789", nil},
		{"123987654321.123456789000", -14, "0.00123987654321123456789", nil},
		{"00000087654321.123456789000", -14, "0.00000087654321123456789", nil},
	}
	dotest(tests)

	wordBufLen = 2
	tests = []tcase{
		{"123.123", -2, "1.23123", nil},
		{"123.123", -3, "0.123123", nil},
		{"123.123", -6, "0.000123123", nil},
		{"123.123", -7, "0.0000123123", nil},
		{"123.123", -15, "0.000000000000123123", nil},
		{"123.123", -16, "0.000000000000012312", ErrTruncated},
		{"123.123", -17, "0.000000000000001231", ErrTruncated},
		{"123.123", -18, "0.000000000000000123", ErrTruncated},
		{"123.123", -19, "0.000000000000000012", ErrTruncated},
		{"123.123", -20, "0.000000000000000001", ErrTruncated},
		{"123.123", -21, "0", ErrTruncated},
		{".000000000123", -1, "0.0000000000123", nil},
		{".000000000123", -6, "0.000000000000000123", nil},
		{".000000000123", -7, "0.000000000000000012", ErrTruncated},
		{".000000000123", -8, "0.000000000000000001", ErrTruncated},
		{".000000000123", -9, "0", ErrTruncated},
		{".000000000123", 1, "0.00000000123", nil},
		{".000000000123", 8, "0.0123", nil},
		{".000000000123", 9, "0.123", nil},
		{".000000000123", 10, "1.23", nil},
		{".000000000123", 17, "12300000", nil},
		{".000000000123", 18, "123000000", nil},
		{".000000000123", 19, "1230000000", nil},
		{".000000000123", 20, "12300000000", nil},
		{".000000000123", 21, "123000000000", nil},
		{".000000000123", 22, "1230000000000", nil},
		{".000000000123", 23, "12300000000000", nil},
		{".000000000123", 24, "123000000000000", nil},
		{".000000000123", 25, "1230000000000000", nil},
		{".000000000123", 26, "12300000000000000", nil},
		{".000000000123", 27, "123000000000000000", nil},
		{".000000000123", 28, "0.000000000123", ErrOverflow},
		{"123456789.987654321", -1, "12345678.998765432", ErrTruncated},
		{"123456789.987654321", -2, "1234567.899876543", ErrTruncated},
		{"123456789.987654321", -8, "1.234567900", ErrTruncated},
		{"123456789.987654321", -9, "0.123456789987654321", nil},
		{"123456789.987654321", -10, "0.012345678998765432", ErrTruncated},
		{"123456789.987654321", -17, "0.000000001234567900", ErrTruncated},
		{"123456789.987654321", -18, "0.000000000123456790", ErrTruncated},
		{"123456789.987654321", -19, "0.000000000012345679", ErrTruncated},
		{"123456789.987654321", -26, "0.000000000000000001", ErrTruncated},
		{"123456789.987654321", -27, "0", ErrTruncated},
		{"123456789.987654321", 1, "1234567900", ErrTruncated},
		{"123456789.987654321", 2, "12345678999", ErrTruncated},
		{"123456789.987654321", 4, "1234567899877", ErrTruncated},
		{"123456789.987654321", 8, "12345678998765432", ErrTruncated},
		{"123456789.987654321", 9, "123456789987654321", nil},
		{"123456789.987654321", 10, "123456789.987654321", ErrOverflow},
		{"123456789.987654321", 0, "123456789.987654321", nil},
	}
	dotest(tests)

	// reset
	wordBufLen = maxWordBufLen
}

// this test will change global variable `wordBufLen`, so it must run in serial
func TestFromStringMyDecimal(t *testing.T) {
	type tcase struct {
		input  string
		output string
		err    error
	}

	var dotest = func(tests []tcase) {
		for _, test := range tests {
			t.Run(fmt.Sprintf("%v (wordBufLen: %v)", test.input, wordBufLen), func(t *testing.T) {
				var dec MyDecimal
				require.Equal(t, test.err, dec.FromString([]byte(test.input)))
				require.Equal(t, test.output, string(dec.ToString()))
			})
		}
	}

	wordBufLen = maxWordBufLen
	tests := []tcase{
		{"12345", "12345", nil},
		{"12345.", "12345", nil},
		{"123.45.", "123.45", ErrTruncated},
		{"-123.45.", "-123.45", ErrTruncated},
		{".00012345000098765", "0.00012345000098765", nil},
		{".12345000098765", "0.12345000098765", nil},
		{"-.000000012345000098765", "-0.000000012345000098765", nil},
		{"1234500009876.5", "1234500009876.5", nil},
		{"123E5", "12300000", nil},
		{"123E-2", "1.23", nil},
		{"1e1073741823", "999999999999999999999999999999999999999999999999999999999999999999999999999999999", ErrOverflow},
		{"-1e1073741823", "-999999999999999999999999999999999999999999999999999999999999999999999999999999999", ErrOverflow},
		{"1e18446744073709551620", "0", ErrBadNumber},
		{"1e", "1", ErrTruncated},
		{"1e001", "10", nil},
		{"1e00", "1", nil},
		{"1eabc", "1", ErrTruncated},
		{"1e 1dddd ", "10", ErrTruncated},
		{"1e - 1", "1", ErrTruncated},
		{"1e -1", "0.1", nil},
		{"0.00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000", "0.000000000000000000000000000000000000000000000000000000000000000000000000", ErrTruncated},
		{"1asf", "1", ErrTruncated},
		{"1.1.1.1.1", "1.1", ErrTruncated},
		{"1  1", "1", ErrTruncated},
		{"1  ", "1", nil},
	}
	dotest(tests)

	wordBufLen = 1
	tests = []tcase{
		{"123450000098765", "98765", ErrOverflow},
		{"123450.000098765", "123450", ErrTruncated},
	}
	dotest(tests)

	// reset
	wordBufLen = maxWordBufLen
}
