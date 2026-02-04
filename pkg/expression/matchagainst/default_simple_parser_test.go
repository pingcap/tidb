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

package matchagainst

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseSimpleNaturalLanguageMode(t *testing.T) {
	cases := []struct {
		name   string
		input  string
		expect SimpleGroup
	}{
		{
			name:   "empty",
			input:  "",
			expect: SimpleGroup{},
		},
		{
			name:  "terms: split by spaces and punctuations",
			input: " hello,\tworld! ",
			expect: SimpleGroup{
				Terms: []string{"hello", "world"},
			},
		},
		{
			name:  "terms: keep underscore and digits",
			input: "foo_bar 123",
			expect: SimpleGroup{
				Terms: []string{"foo_bar", "123"},
			},
		},
		{
			name:  "terms: non-ascii letters are accepted",
			input: "你好，世界",
			expect: SimpleGroup{
				Terms: []string{"你好", "世界"},
			},
		},
		{
			name:  "terms: boolean operators are treated as delimiters",
			input: "foo-bar ~baz >qux <quux",
			expect: SimpleGroup{
				Terms: []string{"foo", "bar", "baz", "qux", "quux"},
			},
		},
		{
			name:  "terms: boolean operators without spaces are treated as delimiters",
			input: "foo-bar~baz>qux<quux",
			expect: SimpleGroup{
				Terms: []string{"foo", "bar", "baz", "qux", "quux"},
			},
		},
		{
			name:  "terms: repeated '-' still works as delimiters",
			input: "foo---bar",
			expect: SimpleGroup{
				Terms: []string{"foo", "bar"},
			},
		},
		{
			name:  "phrase: basic",
			input: `"hello world"`,
			expect: SimpleGroup{
				Phrase: []string{"hello world"},
			},
		},
		{
			name:  "phrase: normalize separators",
			input: `"foo-bar  baz"`,
			expect: SimpleGroup{
				Phrase: []string{"foo bar baz"},
			},
		},
		{
			name:  "phrase: boolean operators inside quotes are treated as delimiters",
			input: `"foo~bar>baz<qux-quux"`,
			expect: SimpleGroup{
				Phrase: []string{"foo bar baz qux quux"},
			},
		},
		{
			name:  "phrase: boolean operators outside quotes are ignored as delimiters",
			input: `foo-~>"bar baz"<qux`,
			expect: SimpleGroup{
				Terms:  []string{"foo", "qux"},
				Phrase: []string{"bar baz"},
			},
		},
		{
			name:  "phrase: multiple phrases",
			input: `"foo bar" "baz qux"`,
			expect: SimpleGroup{
				Phrase: []string{"foo bar", "baz qux"},
			},
		},
		{
			name:  "phrase: multiple phrases with terms between",
			input: `"foo" mid "bar baz"`,
			expect: SimpleGroup{
				Terms:  []string{"mid"},
				Phrase: []string{"foo", "bar baz"},
			},
		},
		{
			name:  "phrase: adjacent phrases without spaces",
			input: `"foo""bar baz"`,
			expect: SimpleGroup{
				Phrase: []string{"foo", "bar baz"},
			},
		},
		{
			name:  "phrase: empty phrases ignored among multiple phrases",
			input: `"foo" "" "bar"`,
			expect: SimpleGroup{
				Phrase: []string{"foo", "bar"},
			},
		},
		{
			name:  "mix: terms and phrase",
			input: `hello "foo bar" baz`,
			expect: SimpleGroup{
				Terms:  []string{"hello", "baz"},
				Phrase: []string{"foo bar"},
			},
		},
		{
			name:   "quote: empty phrase is ignored",
			input:  `""`,
			expect: SimpleGroup{},
		},
		{
			name:  "quote: unterminated quote is implicitly closed",
			input: `"abc`,
			expect: SimpleGroup{
				Phrase: []string{"abc"},
			},
		},
		{
			name:  "quote: phrase in the middle",
			input: `foo"bar baz"`,
			expect: SimpleGroup{
				Terms:  []string{"foo"},
				Phrase: []string{"bar baz"},
			},
		},
		{
			name:   "quote: single quote char is ignored",
			input:  `"`,
			expect: SimpleGroup{},
		},
		{
			name:   "phrase: no word chars in phrase is ignored",
			input:  `"!!!"`,
			expect: SimpleGroup{},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ParseSimpleNaturalLanguageMode(tc.input)
			require.Equal(t, tc.expect, got)
		})
	}
}
