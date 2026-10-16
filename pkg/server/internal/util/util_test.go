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
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseLengthEncodedInt(t *testing.T) {
	testCases := []struct {
		buffer []byte
		num    uint64
		isNull bool
		n      int
		err    error
	}{
		{
			[]byte{'\xfb'},
			uint64(0),
			true,
			1,
			nil,
		},
		{
			[]byte{'\x00'},
			uint64(0),
			false,
			1,
			nil,
		},
		{
			[]byte{'\xfc', '\x01', '\x02'},
			uint64(513),
			false,
			3,
			nil,
		},
		{
			[]byte{'\xfd', '\x01', '\x02', '\x03'},
			uint64(197121),
			false,
			4,
			nil,
		},
		{
			[]byte{'\xfe', '\x01', '\x02', '\x03', '\x04', '\x05', '\x06', '\x07', '\x08'},
			uint64(578437695752307201),
			false,
			9,
			nil,
		},
		{
			[]byte{},
			uint64(0),
			false,
			0,
			io.EOF,
		},
		{
			[]byte{'\xfc', '\x01'},
			uint64(0),
			false,
			0,
			io.EOF,
		},
		{
			[]byte{'\xfd', '\x01', '\x02'},
			uint64(0),
			false,
			0,
			io.EOF,
		},
		{
			[]byte{'\xfe'},
			uint64(0),
			false,
			0,
			io.EOF,
		},
	}

	for _, tc := range testCases {
		num, isNull, n, err := ParseLengthEncodedInt(tc.buffer)
		require.Equal(t, tc.num, num)
		require.Equal(t, tc.isNull, isNull)
		require.Equal(t, tc.n, n)
		require.ErrorIs(t, err, tc.err)
		if tc.err == nil {
			require.Equal(t, tc.n, LengthEncodedIntSize(tc.num))
		}
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

	buffer = []byte{'\xfe'}
	b, isNull, n, err = ParseLengthEncodedBytes(buffer)
	require.Nil(t, b)
	require.False(t, isNull)
	require.Equal(t, 0, n)
	require.ErrorIs(t, err, io.EOF)
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
