// Copyright 2024 PingCAP, Inc.
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
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLongestCommonPrefixLen(t *testing.T) {
	cases := []struct {
		s1 string
		s2 string
		l  int
	}{
		{"", "", 0},
		{"", "a", 0},
		{"a", "", 0},
		{"a", "a", 1},
		{"ab", "a", 1},
		{"a", "ab", 1},
		{"b", "ab", 0},
		{"ba", "ab", 0},
	}

	for _, ca := range cases {
		re := longestCommonPrefixLen([]byte(ca.s1), []byte(ca.s2))
		require.Equal(t, ca.l, re)
	}
}

func TestGetStepValue(t *testing.T) {
	cases := []struct {
		lower []byte
		upper []byte
		l     int
		v     uint64
	}{
		{[]byte{}, []byte{}, 0, math.MaxUint64},
		{[]byte{0}, []byte{128}, 0, binary.BigEndian.Uint64([]byte{128, 255, 255, 255, 255, 255, 255, 255})},
		{[]byte{'a'}, []byte{'z'}, 0, binary.BigEndian.Uint64([]byte{'z' - 'a', 255, 255, 255, 255, 255, 255, 255})},
		{[]byte("abc"), []byte{'z'}, 0, binary.BigEndian.Uint64([]byte{'z' - 'a', 255 - 'b', 255 - 'c', 255, 255, 255, 255, 255})},
		{[]byte("abc"), []byte("xyz"), 0, binary.BigEndian.Uint64([]byte{'x' - 'a', 'y' - 'b', 'z' - 'c', 255, 255, 255, 255, 255})},
		{[]byte("abc"), []byte("axyz"), 1, binary.BigEndian.Uint64([]byte{'x' - 'b', 'y' - 'c', 'z', 255, 255, 255, 255, 255})},
		{[]byte("abc0123456"), []byte("xyz01234"), 0, binary.BigEndian.Uint64([]byte{'x' - 'a', 'y' - 'b', 'z' - 'c', 0, 0, 0, 0, 0})},
	}

	for _, ca := range cases {
		l := longestCommonPrefixLen(ca.lower, ca.upper)
		require.Equal(t, ca.l, l)
		v0 := getStepValue(ca.lower[l:], ca.upper[l:], 1)
		require.Equal(t, v0, ca.v)
	}
}
