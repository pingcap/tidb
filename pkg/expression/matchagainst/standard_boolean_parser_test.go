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

func TestParseStandardBooleanMode(t *testing.T) {
	cases := []struct {
		name        string
		input       string
		output      string
		errContains string
	}{
		{
			name:   "empty: empty string",
			input:  "",
			output: "",
		},
		{
			name:   "empty: whitespace only",
			input:  "   \t\r\n",
			output: "",
		},

		{
			name:   `phrase: empty phrase is ignored`,
			input:  `""`,
			output: "",
		},
		{
			name:   "phrase: basic phrase",
			input:  `"hello world"`,
			output: `M[] S["hello world"] N[]`,
		},
		{
			name:   "phrase: '%' allowed inside phrase",
			input:  `"foo%bar"`,
			output: `M[] S["foo%bar"] N[]`,
		},
		{
			name:   `phrase: empty phrase with '+' is still ignored`,
			input:  `+""`,
			output: "",
		},
		{
			name:   "phrase: '+' applies to phrases",
			input:  `+"foo"`,
			output: `M["foo"] S[] N[]`,
		},
		{
			name:   "phrase: '-' applies to phrases",
			input:  `-"foo"`,
			output: `M[] S[] N["foo"]`,
		},
		{
			name:   "phrase: two phrase without spaces",
			input:  `"foo""bar"`,
			output: `M[] S["foo", "bar"] N[]`,
		},
		{
			name:   "term: basic TERM",
			input:  `foo`,
			output: "M[] S[foo] N[]",
		},
		{
			name:   "term: non-ASCII TERM is accepted",
			input:  `你好`,
			output: "M[] S[你好] N[]",
		},
		{
			name:   "term: single quotes are treated as part of TERM",
			input:  `'foo'`,
			output: "M[] S['foo'] N[]",
		},
		{
			name:   "term: NUM token is treated as a term",
			input:  `123`,
			output: "M[] S[123] N[]",
		},
		{
			name:   "term: NUM can also have a wildcard suffix",
			input:  `123*`,
			output: "M[] S[123*] N[]",
		},
		{
			name:   "term: trailing '*' sets wildcard",
			input:  `foo*`,
			output: "M[] S[foo*] N[]",
		},
		{
			name:   "term: wildcard '*' still works with surrounding spaces",
			input:  `foo * bar`,
			output: "M[] S[foo*, bar] N[]",
		},
		{
			name:   "term: trailing '*' at end sets wildcard",
			input:  `foo *`,
			output: "M[] S[foo*] N[]",
		},
		{
			name:   "term: leading '*' before a term is ignored",
			input:  `*foo`,
			output: "M[] S[foo] N[]",
		},
		{
			name:   "term: leading '*' ignored and trailing '*' sets wildcard",
			input:  `*foo*`,
			output: "M[] S[foo*] N[]",
		},
		{
			name:   "term: digits and letters are one TERM",
			input:  "123foo",
			output: "M[] S[123foo] N[]",
		},

		{
			name:   "prefix: '+' and '-' split into MUST / MUST_NOT",
			input:  `+foo -bar`,
			output: "M[foo] S[] N[bar]",
		},
		{
			name:   "prefix: mixed MUST/SHOULD/MUST_NOT in one query",
			input:  `foo +bar baz -qux`,
			output: "M[bar] S[foo, baz] N[qux]",
		},
		{
			name:   "tokenization: operators are tokenized without spaces",
			input:  `+foo+bar-baz`,
			output: "M[foo, bar] S[] N[baz]",
		},
		{
			name:   "tokenization: '-' is always an operator",
			input:  `foo-bar`,
			output: "M[] S[foo] N[bar]",
		},
		{
			name:   "tokenization: '+' is always an operator",
			input:  `a+b`,
			output: "M[b] S[a] N[]",
		},
		{
			name:   "tokenization: prefix '*' is accepted after '+' and ignored",
			input:  `+*foo`,
			output: "M[foo] S[] N[]",
		},
		{
			name:   "mix: empty phrase ignored among terms",
			input:  `foo "" bar`,
			output: "M[] S[foo, bar] N[]",
		},

		{
			name:   `quote: complete quoted phrase is tokenized even without spaces`,
			input:  `foo"bar"baz`,
			output: `M[] S[foo, "bar", baz] N[]`,
		},
		{
			name:   "whitespace: newline is treated as whitespace",
			input:  "foo\nbar",
			output: "M[] S[foo, bar] N[]",
		},
		{
			name:   "underscore: '_' is part of TERM",
			input:  `foo_bar baz_qux`,
			output: "M[] S[foo_bar, baz_qux] N[]",
		},
		{
			name:        "error: lone '*' expects a term",
			input:       `*`,
			errContains: "expected term",
		},
		{
			name:        "error: lone '+' expects an expr",
			input:       `+`,
			errContains: "unexpected token",
		},
		{
			name:        "error: lone '-' expects an expr",
			input:       `-`,
			errContains: "unexpected token",
		},
		{
			name:        "error: '+*' still expects a term",
			input:       `+*`,
			errContains: "expected term",
		},
		{
			name:        "error: standalone '%' is rejected by tokenizer",
			input:       `%`,
			errContains: "unexpected char",
		},
		{
			name:        "error: '%' is not allowed inside TERM",
			input:       `foo%`,
			errContains: "unexpected char",
		},
		{
			name:        "error: '%' is still illegal between terms",
			input:       `foo % bar`,
			errContains: "unexpected char",
		},
		{
			name:        "error: double prefix operator is not allowed",
			input:       `++foo`,
			errContains: "unexpected token",
		},
		{
			name:        "error: opposing prefix operators are not allowed",
			input:       `-+foo`,
			errContains: "unexpected token",
		},
		{
			name:        "error: multiple '*' creates a new clause and fails",
			input:       `foo**`,
			errContains: "expected term",
		},
		{
			name:        "error: dangling '-' at end is invalid",
			input:       `foo-`,
			errContains: "unexpected token",
		},
		{
			name:        `error: missing closing quote is rejected (1)`,
			input:       `"abc`,
			errContains: "unterminated quote",
		},
		{
			name:        `error: missing closing quote is rejected (newline terminator)`,
			input:       "\"abc\n\"",
			errContains: "unterminated quote",
		},
		{
			name:        `error: missing closing quote is rejected in the middle`,
			input:       `foo"bar`,
			errContains: "unterminated quote",
		},
		{
			name:        `error: lone '"' is rejected`,
			input:       `"`,
			errContains: "unterminated quote",
		},

		{
			name:        "error: standalone '(' is rejected",
			input:       `(`,
			errContains: "unexpected token",
		},
		{
			name:        "error: grouping parentheses are rejected",
			input:       `(foo)`,
			errContains: "unexpected token",
		},
		{
			name:        "error: parentheses are rejected even after prefix '+'",
			input:       `+(foo)`,
			errContains: "unexpected token",
		},
		{
			name:        "error: empty parentheses are rejected",
			input:       `()`,
			errContains: "unexpected token",
		},
		{
			name:        "error: parentheses in the middle of a query are rejected",
			input:       `foo () bar`,
			errContains: "unexpected token",
		},
		{
			name:        "error: unexpected ')' at top level",
			input:       `)`,
			errContains: "unexpected ')'",
		},
		{
			name:        "error: unexpected trailing ')'",
			input:       `foo)`,
			errContains: "unexpected ')'",
		},
		{
			name:        "error: '<' is rejected",
			input:       `<foo`,
			errContains: "unexpected token",
		},
		{
			name:        "error: '>' is rejected",
			input:       `>foo`,
			errContains: "unexpected token",
		},
		{
			name:        "error: '~' is rejected",
			input:       `~foo`,
			errContains: "unexpected token",
		},
		{
			name:        "error: '@' cannot follow a TERM token",
			input:       `foo@1`,
			errContains: "unexpected token",
		},
		{
			name:        "error: dangling '@' after a term",
			input:       `foo@`,
			errContains: "unexpected token",
		},
		{
			name:        "error: phrase distance '@NUM' is rejected",
			input:       `"hello world"@10`,
			errContains: "unexpected token",
		},
		{
			name:        "error: phrase distance '@' with whitespace is rejected",
			input:       `"hello" @10`,
			errContains: "unexpected token",
		},
		{
			name:        "error: cannot start with '@'",
			input:       `@1`,
			errContains: "unexpected token",
		},
		{
			name:        "error: empty phrase ignored makes '@' invalid",
			input:       `""@1`,
			errContains: "unexpected token",
		},
		{
			name:        "error: '*' cannot follow a phrase",
			input:       `"foo"*`,
			errContains: "expected term",
		},
		{
			name:        "error: leading '*' only works for terms, not phrases",
			input:       `*"foo"`,
			errContains: "expected term",
		},
		{
			name:        "error: leading '*' only works for terms, not parentheses",
			input:       `*(foo)`,
			errContains: "expected term",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g, err := ParseStandardBooleanMode(tc.input)
			if tc.errContains != "" {
				require.Error(t, err, "input: %q", tc.input)
				require.Contains(t, err.Error(), tc.errContains, "input: %q", tc.input)
				return
			}
			require.NoError(t, err, "input: %q", tc.input)
			require.Equal(t, tc.output, g.DebugString(), "input: %q", tc.input)
		})
	}
}
