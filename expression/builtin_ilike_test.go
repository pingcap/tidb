// Copyright 2023 PingCAP, Inc.
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
	"github.com/pingcap/tidb/testkit/testutil"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/stretchr/testify/require"
)

func TestIlike(t *testing.T) {
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
	var charset_and_collation_general = [][]string{{"utf8mb4", "utf8mb4_general_ci"}, {"utf8", "utf8_general_ci"}}

	for _, charset_and_collation := range charset_and_collation_general {
		for _, tt := range tests {
			comment := fmt.Sprintf(`for input = "%s", pattern = "%s"`, tt.input, tt.pattern)
			fc := funcs[ast.Ilike]
			inputs := datumsToConstants(types.MakeDatums(tt.input, tt.pattern, 0))
			f, err := fc.getFunction(ctx, inputs)
			require.NoError(t, err, comment)
			f.SetCharsetAndCollation(charset_and_collation[0], charset_and_collation[1])
			r, err := evalBuiltinFunc(f, chunk.Row{})
			require.NoError(t, err, comment)
			testutil.DatumEqual(t, types.NewDatum(tt.generalMatch), r, comment)
		}
	}

	var charset_and_collation_unicode = [][]string{
		{"utf8mb4", "utf8mb4_bin"},
		{"utf8mb4", "utf8mb4_unicode_ci"},
		{"utf8", "utf8_bin"},
		{"utf8", "utf8_unicode_ci"}}

	for _, charset_and_collation := range charset_and_collation_unicode {
		for _, tt := range tests {
			comment := fmt.Sprintf(`for input = "%s", pattern = "%s"`, tt.input, tt.pattern)
			fc := funcs[ast.Ilike]
			inputs := datumsToConstants(types.MakeDatums(tt.input, tt.pattern, 0))
			f, err := fc.getFunction(ctx, inputs)
			require.NoError(t, err, comment)
			f.SetCharsetAndCollation(charset_and_collation[0], charset_and_collation[1])
			r, err := evalBuiltinFunc(f, chunk.Row{})
			require.NoError(t, err, comment)
			testutil.DatumEqual(t, types.NewDatum(tt.unicodeMatch), r, comment)
		}
	}
}
