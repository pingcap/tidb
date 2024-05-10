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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAppendFormatFloat(t *testing.T) {
	infVal, _ := strconv.ParseFloat("+Inf", 64)
	tests := []struct {
		fVal    float64
		out     string
		prec    int
		bitSize int
	}{
		{
			99999999999999999999,
			"1e20",
			-1,
			64,
		},
		{
			1e15,
			"1e15",
			-1,
			64,
		},
		{
			9e14,
			"900000000000000",
			-1,
			64,
		},
		{
			-9999999999999999,
			"-1e16",
			-1,
			64,
		},
		{
			999999999999999,
			"999999999999999",
			-1,
			64,
		},
		{
			0.000000000000001,
			"0.000000000000001",
			-1,
			64,
		},
		{
			0.0000000000000009,
			"9e-16",
			-1,
			64,
		},
		{
			-0.0000000000000009,
			"-9e-16",
			-1,
			64,
		},
		{
			0.11111,
			"0.111",
			3,
			64,
		},
		{
			0.11111,
			"0.111",
			3,
			64,
		},
		{
			0.1111111111111111111,
			"0.11111111",
			-1,
			32,
		},
		{
			0.1111111111111111111,
			"0.1111111111111111",
			-1,
			64,
		},
		{
			0.0000000000000009,
			"9e-16",
			3,
			64,
		},
		{
			0,
			"0",
			-1,
			64,
		},
		{
			-340282346638528860000000000000000000000,
			"-3.40282e38",
			-1,
			32,
		},
		{
			-34028236,
			"-34028236.00",
			2,
			32,
		},
		{
			-17976921.34,
			"-17976921.34",
			2,
			64,
		},
		{
			-3.402823466e+38,
			"-3.40282e38",
			-1,
			32,
		},
		{
			-1.7976931348623157e308,
			"-1.7976931348623157e308",
			-1,
			64,
		},
		{
			10.0e20,
			"1e21",
			-1,
			32,
		},
		{
			1e20,
			"1e20",
			-1,
			32,
		},
		{
			10.0,
			"10",
			-1,
			32,
		},
		{
			999999986991104,
			"1e15",
			-1,
			32,
		},
		{
			1e15,
			"1e15",
			-1,
			32,
		},
		{
			infVal,
			"0",
			-1,
			64,
		},
		{
			-infVal,
			"0",
			-1,
			64,
		},
		{
			1e14,
			"100000000000000",
			-1,
			64,
		},
		{
			1e308,
			"1e308",
			-1,
			64,
		},
	}
	for _, tc := range tests {
		require.Equal(t, tc.out, string(AppendFormatFloat(nil, tc.fVal, tc.prec, tc.bitSize)))
	}
}

func TestParseLengthEncodedInt(t *testing.T) {
	testCases := []struct {
		buffer []byte
		num    uint64
		isNull bool
		n      int
	}{
		{
			[]byte{'\xfb'},
			uint64(0),
			true,
			1,
		},
		{
			[]byte{'\x00'},
			uint64(0),
			false,
			1,
		},
		{
			[]byte{'\xfc', '\x01', '\x02'},
			uint64(513),
			false,
			3,
		},
		{
			[]byte{'\xfd', '\x01', '\x02', '\x03'},
			uint64(197121),
			false,
			4,
		},
		{
			[]byte{'\xfe', '\x01', '\x02', '\x03', '\x04', '\x05', '\x06', '\x07', '\x08'},
			uint64(578437695752307201),
			false,
			9,
		},
	}

	for _, tc := range testCases {
		num, isNull, n := ParseLengthEncodedInt(tc.buffer)
		require.Equal(t, tc.num, num)
		require.Equal(t, tc.isNull, isNull)
		require.Equal(t, tc.n, n)
		require.Equal(t, tc.n, LengthEncodedIntSize(tc.num))
	}
}

func TestParseLengthEncodedBytes(t *testing.T) {
	buffer := []byte{'\xfb'}
	b, isNull, n, err := ParseLengthEncodedBytes(buffer)
	require.Nil(t, b)
	require.True(t, isNull)
	require.Equal(t, 1, n)
	require.NoError(t, err)

	buffer = []byte{0}
	b, isNull, n, err = ParseLengthEncodedBytes(buffer)
	require.Nil(t, b)
	require.False(t, isNull)
	require.Equal(t, 1, n)
	require.NoError(t, err)

	buffer = []byte{'\x01'}
	b, isNull, n, err = ParseLengthEncodedBytes(buffer)
	require.Nil(t, b)
	require.False(t, isNull)
	require.Equal(t, 2, n)
	require.Equal(t, "EOF", err.Error())
}

func TestParseNullTermString(t *testing.T) {
	for _, tc := range []struct {
		input  string
		str    string
		remain string
	}{
		{
			"abc\x00def",
			"abc",
			"def",
		},
		{
			"\x00def",
			"",
			"def",
		},
		{
			"def\x00hig\x00k",
			"def",
			"hig\x00k",
		},
		{
			"abcdef",
			"",
			"abcdef",
		},
	} {
		str, remain := ParseNullTermString([]byte(tc.input))
		require.Equal(t, tc.str, string(str))
		require.Equal(t, tc.remain, string(remain))
	}
}
