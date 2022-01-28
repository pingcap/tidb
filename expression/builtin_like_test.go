// Copyright 2017 PingCAP, Inc.
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

package expression

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/testkit/trequire"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/collate"
	"github.com/stretchr/testify/require"
)

func TestLike(t *testing.T) {
	ctx := createContext(t)
	tests := []struct {
		input   string
		pattern string
		match   int
	}{
		{"a", "", 0},
		{"a", "a", 1},
		{"a", "b", 0},
		{"aA", "Aa", 0},
		{"aAb", `Aa%`, 0},
		{"aAb", "aA_", 1},
		{"baab", "b_%b", 1},
		{"baab", "b%_b", 1},
		{"bab", "b_%b", 1},
		{"bab", "b%_b", 1},
		{"bb", "b_%b", 0},
		{"bb", "b%_b", 0},
		{"baabccc", "b_%b%", 1},
		{"a", `\a`, 1},
	}

	for _, tt := range tests {
		comment := fmt.Sprintf(`for input = "%s", pattern = "%s"`, tt.input, tt.pattern)
		fc := funcs[ast.Like]
		f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(tt.input, tt.pattern, int('\\'))))
		require.NoError(t, err, comment)
		r, err := evalBuiltinFuncConcurrent(f, chunk.Row{})
		require.NoError(t, err, comment)
		trequire.DatumEqual(t, types.NewDatum(tt.match), r, comment)
	}
}

func TestRegexp(t *testing.T) {
	ctx := createContext(t)
	tests := []struct {
		pattern string
		input   string
		match   int64
		err     error
	}{
		{"^$", "a", 0, nil},
		{"a", "a", 1, nil},
		{"a", "b", 0, nil},
		{"aA", "aA", 1, nil},
		{".", "a", 1, nil},
		{"^.$", "ab", 0, nil},
		{"..", "b", 0, nil},
		{".ab", "aab", 1, nil},
		{".*", "abcd", 1, nil},
		{"(", "", 0, ErrRegexp},
		{"(*", "", 0, ErrRegexp},
		{"[a", "", 0, ErrRegexp},
		{"\\", "", 0, ErrRegexp},
	}
	for _, tt := range tests {
		fc := funcs[ast.Regexp]
		f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(tt.input, tt.pattern)))
		require.NoError(t, err)
		match, err := evalBuiltinFunc(f, chunk.Row{})
		if tt.err == nil {
			require.NoError(t, err)
			trequire.DatumEqual(t, types.NewDatum(tt.match), match, fmt.Sprintf("%v", tt))
		} else {
			require.True(t, terror.ErrorEqual(err, tt.err))
		}
	}
}

func TestCILike(t *testing.T) {
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)
	ctx := createContext(t)
	tests := []struct {
		input        string
		pattern      string
		generalMatch int
		unicodeMatch int
	}{
		{"a", "", 0, 0},
		{"a", "a", 1, 1},
		{"a", "á", 1, 1},
		{"a", "b", 0, 0},
		{"aA", "Aa", 1, 1},
		{"áAb", `Aa%`, 1, 1},
		{"áAb", `%ab%`, 1, 1},
		{"áAb", `%ab`, 1, 1},
		{"ÀAb", "aA_", 1, 1},
		{"áééá", "a_%a", 1, 1},
		{"áééá", "a%_a", 1, 1},
		{"áéá", "a_%a", 1, 1},
		{"áéá", "a%_a", 1, 1},
		{"áá", "a_%a", 0, 0},
		{"áá", "a%_a", 0, 0},
		{"áééáííí", "a_%a%", 1, 1},

		// performs matching on a per-character basis
		// https://dev.mysql.com/doc/refman/5.7/en/string-comparison-functions.html#operator_like
		{"ß", "s%", 1, 0},
		{"ß", "%s", 1, 0},
		{"ß", "ss", 0, 0},
		{"ß", "s", 1, 0},
		{"ss", "%ß%", 1, 0},
		{"ß", "_", 1, 1},
		{"ß", "__", 0, 0},
	}
	for _, tt := range tests {
		comment := fmt.Sprintf(`for input = "%s", pattern = "%s"`, tt.input, tt.pattern)
		fc := funcs[ast.Like]
		inputs := datumsToConstants(types.MakeDatums(tt.input, tt.pattern, 0))
		f, err := fc.getFunction(ctx, inputs)
		require.NoError(t, err, comment)
		f.setCollator(collate.GetCollator("utf8mb4_general_ci"))
		r, err := evalBuiltinFunc(f, chunk.Row{})
		require.NoError(t, err, comment)
		trequire.DatumEqual(t, types.NewDatum(tt.generalMatch), r, comment)
	}

	for _, tt := range tests {
		comment := fmt.Sprintf(`for input = "%s", pattern = "%s"`, tt.input, tt.pattern)
		fc := funcs[ast.Like]
		inputs := datumsToConstants(types.MakeDatums(tt.input, tt.pattern, 0))
		f, err := fc.getFunction(ctx, inputs)
		require.NoError(t, err, comment)
		f.setCollator(collate.GetCollator("utf8mb4_unicode_ci"))
		r, err := evalBuiltinFunc(f, chunk.Row{})
		require.NoError(t, err, comment)
		trequire.DatumEqual(t, types.NewDatum(tt.unicodeMatch), r, comment)
	}
}
