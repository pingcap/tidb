// Copyright 2021 PingCAP, Inc.
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
	"testing"

	"github.com/stretchr/testify/require"
)

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
