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
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/testkit/testutil"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/collate"
	"github.com/stretchr/testify/require"
)

func TestIlike(t *testing.T) {
	ctx := createContext(t)
	tests := []struct {
		input        string
		pattern      string
		escape       int
		generalMatch int
		unicodeMatch int
	}{
		{"a", "", 0, 0, 0},
		{"a", "a", 0, 1, 1},
		{"ü", "Ü", 0, 0, 0},
		{"a", "á", 0, 0, 0},
		{"a", "b", 0, 0, 0},
		{"aA", "Aa", 0, 1, 1},
		{"áAb", `Aa%`, 0, 0, 0},
		{"áAb", `%ab%`, 0, 1, 1},
		{"", "", 0, 1, 1},
		{"ß", "s%", 0, 0, 0},
		{"ß", "%s", 0, 0, 0},
		{"ß", "ss", 0, 0, 0},
		{"ß", "s", 0, 0, 0},
		{"ss", "%ß%", 0, 0, 0},
		{"ß", "_", 0, 1, 1},
		{"ß", "__", 0, 0, 0},
		{"啊aaa啊啊啊aa", "啊aaa啊啊啊aa", 0, 1, 1},

		// escape tests
		{"abc", "ABC", int('a'), 1, 1},
		{"aaz", "Aaaz", int('a'), 1, 1},
		{"AAz", "AAAAz", int('a'), 0, 0},
		{"a", "Aa", int('A'), 1, 1},
		{"a", "AA", int('A'), 1, 1},
		{"Aa", "AAAA", int('A'), 1, 1},
		{"gTp", "AGTAp", int('A'), 1, 1},
		{"gTAp", "AGTAap", int('A'), 1, 1},
		{"A", "aA", int('a'), 1, 1},
		{"a", "aA", int('a'), 1, 1},
		{"aaa", "AAaA", int('a'), 1, 1},
		{"a啊啊a", "a啊啊A", int('A'), 0, 0},
		{"啊aaa啊啊啊aa", "啊aaa啊啊啊aa", int('A'), 1, 1},
		{"啊aAa啊啊啊aA", "啊AAA啊啊啊AA", int('a'), 1, 1},
		{"啊aaa啊啊啊aa", "啊aaa啊啊啊aa", int('a'), 0, 0},
	}
	var charset_and_collation_general = [][]string{{"utf8mb4", "utf8mb4_general_ci"}, {"utf8", "utf8_general_ci"}}

	for _, charset_and_collation := range charset_and_collation_general {
		for _, tt := range tests {
			comment := fmt.Sprintf(`for input = "%s", pattern = "%s", escape = "%s", collation = "%s"`, tt.input, tt.pattern, string(rune(tt.escape)), charset_and_collation[1])
			fc := funcs[ast.Ilike]
			inputs := datumsToConstants(types.MakeDatums(tt.input, tt.pattern, tt.escape))
			f, err := fc.getFunction(ctx, inputs)
			require.NoError(t, err, comment)
			f.SetCharsetAndCollation(charset_and_collation[0], charset_and_collation[1])
			f.setCollator(collate.GetCollator(charset_and_collation[1]))
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

	for _, charsetAndCollation := range charset_and_collation_unicode {
		for _, tt := range tests {
			comment := fmt.Sprintf(`for input = "%s", pattern = "%s", escape = "%s", collation = "%s"`, tt.input, tt.pattern, string(rune(tt.escape)), charsetAndCollation[1])
			fc := funcs[ast.Ilike]
			inputs := datumsToConstants(types.MakeDatums(tt.input, tt.pattern, tt.escape))
			f, err := fc.getFunction(ctx, inputs)
			require.NoError(t, err, comment)
			f.SetCharsetAndCollation(charsetAndCollation[0], charsetAndCollation[1])
			f.setCollator(collate.GetCollator(charsetAndCollation[1]))
			r, err := evalBuiltinFunc(f, chunk.Row{})
			require.NoError(t, err, comment)
			testutil.DatumEqual(t, types.NewDatum(tt.unicodeMatch), r, comment)
		}
	}
}

var vecBuiltinIlikeCases = map[string][]vecExprBenchCase{
	ast.Ilike: {
		{
			retEvalType:   types.ETInt,
			childrenTypes: []types.EvalType{types.ETString, types.ETString, types.ETInt},
			geners: []dataGenerator{
				&selectStringGener{
					candidates: []string{"aaa", "abc", "aAa", "AaA", "a啊啊Aa啊", "啊啊啊啊", "üÜ", "Ü", "a", "A"},
					randGen:    newDefaultRandGen(),
				},
				&selectStringGener{
					candidates: []string{"aaa", "ABC", "啊啊啊啊", "üÜ", "ü", "a", "A"},
					randGen:    newDefaultRandGen(),
				},
				newRangeInt64Gener(65, 122)},
			childrenFieldTypes: []*types.FieldType{types.NewFieldTypeBuilder().SetType(mysql.TypeString).SetFlag(mysql.BinaryFlag).SetCharset(charset.CharsetBin).SetCollate(charset.CollationBin).BuildP()},
		},
	},
}

func TestVectorizedBuiltinIlikeFunc(t *testing.T) {
	testVectorizedBuiltinFunc(t, vecBuiltinIlikeCases)
}
