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

func dumpUnaryOp(op UnaryOp) string {
	switch op {
	case OpExist:
		return "+"
	case OpIgnore:
		return "-"
	case OpNegate:
		return "~"
	case OpIncrRating:
		return ">"
	case OpDecrRating:
		return "<"
	default:
		return ""
	}
}

func dumpNode(n Node) string {
	return dumpUnaryOp(n.Op) + dumpItem(n.Item)
}

func dumpItem(item Item) string {
	switch x := item.(type) {
	case *Word:
		var b strings.Builder
		b.WriteString(x.Text)
		if x.Trunc {
			b.WriteByte('*')
		}
		if x.Ignored {
			b.WriteString("{ign}")
		}
		return b.String()
	case *Phrase:
		return fmt.Sprintf("%q", x.Text)
	case *Group:
		return "(" + dumpGroup(x) + ")"
	default:
		return "<unknown>"
	}
}

func dumpNodes(nodes []Node) string {
	if len(nodes) == 0 {
		return ""
	}
	out := make([]string, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, dumpNode(n))
	}
	return strings.Join(out, " ")
}

func dumpGroup(g *Group) string {
	return fmt.Sprintf("M[%s] S[%s] N[%s]", dumpNodes(g.Must), dumpNodes(g.Should), dumpNodes(g.MustNot))
}

func TestParseBooleanMode(t *testing.T) {
	type testCase struct {
		name    string
		input   string
		want    string
		wantErr error
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
			name:  "weight and negate kept in should",
			input: ">foo <bar ~baz",
			want:  "M[] S[>foo <bar ~baz] N[]",
		},
		{
			name:  "weight prefix accumulates but only sign kept",
			input: ">>foo <<bar",
			want:  "M[] S[>foo <bar] N[]",
		},
		{
			name:  "weight prefixes can cancel out",
			input: "><foo <>bar",
			want:  "M[] S[foo bar] N[]",
		},
		{
			name:  "multiple prefixes choose by priority yesno over others",
			input: "+>~foo",
			want:  "M[+foo] S[] N[]",
		},
		{
			name:  "multiple prefixes choose by priority weight over negate",
			input: ">~foo",
			want:  "M[] S[>foo] N[]",
		},
		{
			name:  "negate toggle twice cancels",
			input: "~~foo",
			want:  "M[] S[foo] N[]",
		},
		{
			name:  "only negative terms",
			input: "-foo -bar",
			want:  "M[] S[] N[-foo -bar]",
		},
		{
			name:  "paren group",
			input: "+(foo bar)",
			want:  "M[+(M[] S[foo bar] N[])] S[] N[]",
		},
		{
			name:  "paren group without prefix is should",
			input: "(foo)",
			want:  "M[] S[(M[] S[foo] N[])] N[]",
		},
		{
			name:  "prefix before group requires space",
			input: "a+(foo)",
			want:  "M[] S[a (M[] S[foo] N[])] N[]",
		},
		{
			name:  "prefix before group with space works",
			input: "a +(foo)",
			want:  "M[+(M[] S[foo] N[])] S[a] N[]",
		},
		{
			name:  "ignore applies to group",
			input: "-(foo bar)",
			want:  "M[] S[] N[-(M[] S[foo bar] N[])]",
		},
		{
			name:  "nested group and mixed ops",
			input: "+(foo -(bar baz) >qux)",
			want:  "M[+(M[] S[foo >qux] N[-(M[] S[bar baz] N[])])] S[] N[]",
		},
		{
			name:  "quoted phrase",
			input: `"foo bar"`,
			want:  `M[] S["foo bar"] N[]`,
		},
		{
			name:  "empty quoted phrase",
			input: `""`,
			want:  `M[] S[""] N[]`,
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
			name:  "word after quoted phrase can take prefix if separated by space",
			input: `"foo" +bar`,
			want:  `M[+bar] S["foo"] N[]`,
		},
		{
			name:  "@N distance produces ignored word node",
			input: `"foo"@2 bar`,
			want:  `M[] S["foo" 2{ign} bar] N[]`,
		},
		{
			name:  "@non-numeric is not distance and is not ignored",
			input: `"foo"@N bar`,
			want:  `M[] S["foo" N bar] N[]`,
		},
		{
			name:    "unmatched right paren",
			input:   ")",
			wantErr: ErrUnmatchedRightParen,
		},
		{
			name:    "unmatched left paren",
			input:   "(",
			wantErr: ErrUnmatchedLeftParen,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root, err := ParseBooleanMode(tc.input)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, dumpGroup(root))
		})
	}
}
