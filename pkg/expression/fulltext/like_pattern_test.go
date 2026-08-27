// Copyright 2026 PingCAP, Inc.
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

package fulltext

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPathHierarchyLikeLiteral(t *testing.T) {
	for _, test := range []struct {
		pattern string
		literal string
		ok      bool
	}{
		{pattern: "/path/to/%", literal: "/path/to/", ok: true},
		{pattern: `/path/\%/to/%`, literal: "/path/%/to/", ok: true},
		{pattern: "/path/_/%", ok: false},
		{pattern: "%/path/", ok: false},
		{pattern: `/path/\`, ok: false},
	} {
		literal, ok := PathHierarchyLikeLiteral(test.pattern, '\\')
		require.Equal(t, test.ok, ok, test.pattern)
		require.Equal(t, test.literal, literal, test.pattern)
	}
}

func TestNgramLikeLiteral(t *testing.T) {
	for _, test := range []struct {
		pattern string
		literal string
		ok      bool
	}{
		{pattern: "%hello%", literal: "hello", ok: true},
		{pattern: `%a\%bc%`, literal: "a%bc", ok: true},
		{pattern: "%中文词%", literal: "中文词", ok: true},
		{pattern: "%ab_%", ok: false},
		{pattern: "hello%", ok: false},
		{pattern: `%hello\`, ok: false},
	} {
		literal, ok := NgramLikeLiteral(test.pattern, '\\')
		require.Equal(t, test.ok, ok, test.pattern)
		require.Equal(t, test.literal, literal, test.pattern)
	}
}

func TestLikeMandatoryLiterals(t *testing.T) {
	for _, test := range []struct {
		pattern  string
		literals []string
		ok       bool
	}{
		{pattern: "abcd", literals: []string{"abcd"}, ok: true},
		{pattern: "%abc_def%ghi", literals: []string{"abc", "def", "ghi"}, ok: true},
		{pattern: `%a\%bc%`, literals: []string{"a%bc"}, ok: true},
		{pattern: "%中文_词%", literals: []string{"中文", "词"}, ok: true},
		{pattern: "%%%", literals: []string{}, ok: true},
		{pattern: `%abc\`, ok: false},
	} {
		literals, ok := LikeMandatoryLiterals(test.pattern, '\\')
		require.Equal(t, test.ok, ok, test.pattern)
		require.Equal(t, test.literals, literals, test.pattern)
	}
}

func TestPathHierarchyLikePrefilterLiteral(t *testing.T) {
	for _, test := range []struct {
		pattern   string
		delimiter string
		literal   string
		ok        bool
	}{
		{pattern: "/path/to/prefix%", delimiter: "/", literal: "/path/to/", ok: true},
		{pattern: "root::child::prefix%", delimiter: "::", literal: "root::child::", ok: true},
		{pattern: "root::prefix%", delimiter: "::", literal: "root::", ok: true},
		{pattern: "/prefix%", delimiter: "/", ok: false},
		{pattern: "//prefix%", delimiter: "/", ok: false},
		{pattern: "prefix%", delimiter: "/", ok: false},
		{pattern: "/path/to/%", delimiter: "/", ok: false},
		{pattern: "/path/_/prefix%", delimiter: "/", ok: false},
	} {
		literal, ok := PathHierarchyLikePrefilterLiteral(test.pattern, '\\', test.delimiter)
		require.Equal(t, test.ok, ok, test.pattern)
		require.Equal(t, test.literal, literal, test.pattern)
	}
}
