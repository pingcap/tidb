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
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func dumpModifier(mod BooleanModifier) string {
	switch mod {
	case BooleanModifierMust:
		return "+"
	case BooleanModifierMustNot:
		return "-"
	case BooleanModifierNegate:
		return "~"
	case BooleanModifierBoost:
		return ">"
	case BooleanModifierDeBoost:
		return "<"
	default:
		return ""
	}
}

func dumpClause(c BooleanClause) string {
	return dumpModifier(c.Modifier) + dumpExpr(c.Expr)
}

func dumpExpr(expr BooleanExpr) string {
	switch x := expr.(type) {
	case *BooleanTerm:
		var b strings.Builder
		b.WriteString(x.text)
		if x.Wildcard {
			b.WriteByte('*')
		}
		if x.Ignored {
			b.WriteString("{ign}")
		}
		return b.String()
	case *BooleanPhrase:
		return fmt.Sprintf("%q", x.text)
	case *BooleanGroup:
		return "(" + dumpGroup(x) + ")"
	default:
		return "<unknown>"
	}
}

func dumpClauses(clauses []BooleanClause) string {
	if len(clauses) == 0 {
		return ""
	}
	out := make([]string, 0, len(clauses))
	for _, c := range clauses {
		out = append(out, dumpClause(c))
	}
	return strings.Join(out, " ")
}

func dumpGroup(g *BooleanGroup) string {
	return fmt.Sprintf("M[%s] S[%s] N[%s]", dumpClauses(g.Must), dumpClauses(g.Should), dumpClauses(g.MustNot))
}

func TestParseNgramBooleanMode(t *testing.T) {
	type testCase struct {
		name            string
		input           string
		want            string
		wantErrContains string
	}

	cases := []testCase{
		{
			name:  "empty",
			input: "",
			want:  "M[] S[] N[]",
		},
		{
			name:  "only whitespace",
			input: "  \t\n  ",
			want:  "M[] S[] N[]",
		},
		{
			name:  "basic buckets",
			input: "foo +bar -baz",
			want:  "M[+bar] S[foo] N[-baz]",
		},
		{
			name:  "prefix only after space",
			input: "a+foo",
			want:  "M[] S[a foo] N[]",
		},
		{
			name:  "prefix after delimiter needs a space",
			input: "foo,+bar",
			want:  "M[] S[foo bar] N[]",
		},
		{
			name:  "prefix after delimiter with space works",
			input: "foo, +bar",
			want:  "M[+bar] S[foo] N[]",
		},
		{
			name:  "trunc word",
			input: "foo*",
			want:  "M[] S[foo*] N[]",
		},
		{
			name:  "non-ASCII term is accepted",
			input: "你好",
			want:  "M[] S[你好] N[]",
		},
		{
			name:  "underscore is part of term",
			input: "foo_bar baz_qux",
			want:  "M[] S[foo_bar baz_qux] N[]",
		},
		{
			name:  "digits and letters are one term",
			input: "123foo",
			want:  "M[] S[123foo] N[]",
		},
		{
			name:  "newline is treated as delimiter",
			input: "foo\nbar",
			want:  "M[] S[foo bar] N[]",
		},
		{
			name:  "leading '*' before a term is ignored",
			input: "*foo",
			want:  "M[] S[foo] N[]",
		},
		{
			name:  "wildcard requires adjacency to the word",
			input: "foo * bar",
			want:  "M[] S[foo bar] N[]",
		},
		{
			name:  "multiple prefix signs are tolerated and the last one wins",
			input: "-+foo",
			want:  "M[+foo] S[] N[]",
		},
		{
			name:  "prefix can be neutralized by delimiter before the term",
			input: "+*foo",
			want:  "M[] S[foo] N[]",
		},
		{
			name:            "reject unsupported operator '>'",
			input:           ">foo",
			wantErrContains: "unsupported operator '>'",
		},
		{
			name:            "reject unsupported operator '<'",
			input:           "<bar",
			wantErrContains: "unsupported operator '<'",
		},
		{
			name:            "reject unsupported operator '~'",
			input:           "~baz",
			wantErrContains: "unsupported operator '~'",
		},
		{
			name:            "reject unsupported operator '('",
			input:           "(foo)",
			wantErrContains: "unsupported operator '('",
		},
		{
			name:            "reject unsupported operator ')'",
			input:           "foo)",
			wantErrContains: "unsupported operator ')'",
		},
		{
			name:  "only negative terms",
			input: "-foo -bar",
			want:  "M[] S[] N[-foo -bar]",
		},
		{
			name:  "quoted phrase",
			input: `"foo bar"`,
			want:  `M[] S["foo bar"] N[]`,
		},
		{
			name:  "empty quoted phrase is ignored",
			input: `""`,
			want:  `M[] S[] N[]`,
		},
		{
			name:  "quoted phrase with prefix",
			input: `+"foo bar"`,
			want:  `M[+"foo bar"] S[] N[]`,
		},
		{
			name:  "quoted phrase can be must-not",
			input: `-"foo bar"`,
			want:  `M[] S[] N[-"foo bar"]`,
		},
		{
			name:  "unclosed quote auto close",
			input: `"foo bar`,
			want:  `M[] S["foo bar"] N[]`,
		},
		{
			name:  "operators inside quote are ignored, words still captured",
			input: `"foo +bar -baz"`,
			want:  `M[] S["foo bar baz"] N[]`,
		},
		{
			name:  "word adjacent to quoted phrase",
			input: `"foo"bar`,
			want:  `M[] S["foo" bar] N[]`,
		},
		{
			name:  "quoted phrase can be adjacent to words on both sides",
			input: `foo"bar"baz`,
			want:  `M[] S[foo "bar" baz] N[]`,
		},
		{
			name:  "unsupported operators inside quote are treated as delimiters",
			input: `"a>b ~c (d)"`,
			want:  `M[] S["a b c d"] N[]`,
		},
		{
			name:  "empty quoted phrase with '+' is ignored",
			input: `+""`,
			want:  `M[] S[] N[]`,
		},
		{
			name:  "empty quoted phrase with '-' is ignored",
			input: `-""`,
			want:  `M[] S[] N[]`,
		},
		{
			name:  "word after quoted phrase can take prefix if separated by space",
			input: `"foo" +bar`,
			want:  `M[+bar] S["foo"] N[]`,
		},
		{
			name:            "reject unsupported operator '@' after phrase",
			input:           `"foo"@2 bar`,
			wantErrContains: "unsupported operator '@'",
		},
		{
			name:            "reject unsupported operator '@'",
			input:           `foo@bar`,
			wantErrContains: "unsupported operator '@'",
		},
		{
			name:            "reject unsupported operator '@' with whitespace before distance",
			input:           `"hello" @10`,
			wantErrContains: "unsupported operator '@'",
		},
		{
			name:            "reject unsupported operator '@' at beginning",
			input:           `@1`,
			wantErrContains: "unsupported operator '@'",
		},
		{
			name:            "unsupported operators mixed with supported prefix",
			input:           "+>~foo",
			wantErrContains: "unsupported operator '>'",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root, err := ParseNgramBooleanMode(tc.input)
			if tc.wantErrContains != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.wantErrContains)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, dumpGroup(root))
		})
	}
}
