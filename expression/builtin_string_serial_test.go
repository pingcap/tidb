// Copyright 2021 PingCAP, Inc.
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
	"testing"
	"unicode/utf8"

	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/collate"
	"github.com/stretchr/testify/require"
)

func TestCIWeightString(t *testing.T) {
	ctx := createContext(t)
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)

	type weightStringTest struct {
		str     string
		padding string
		length  int
		expect  interface{}
	}

	checkResult := func(collation string, tests []weightStringTest) {
		fc := funcs[ast.WeightString]
		for _, test := range tests {
			str := types.NewCollationStringDatum(test.str, collation, utf8.RuneCountInString(test.str))
			var f builtinFunc
			var err error
			if test.padding == "NONE" {
				f, err = fc.getFunction(ctx, datumsToConstants([]types.Datum{str}))
			} else {
				padding := types.NewDatum(test.padding)
				length := types.NewDatum(test.length)
				f, err = fc.getFunction(ctx, datumsToConstants([]types.Datum{str, padding, length}))
			}
			require.NoError(t, err)
			result, err := evalBuiltinFunc(f, chunk.Row{})
			require.NoError(t, err)
			if result.IsNull() {
				require.Nil(t, test.expect)
				continue
			}
			res, err := result.ToString()
			require.NoError(t, err)
			require.Equal(t, test.expect, res)
		}
	}

	generalTests := []weightStringTest{
		{"aAÁàãăâ", "NONE", 0, "\x00A\x00A\x00A\x00A\x00A\x00A\x00A"},
		{"中", "NONE", 0, "\x4E\x2D"},
		{"a", "CHAR", 5, "\x00A"},
		{"a ", "CHAR", 5, "\x00A"},
		{"中", "CHAR", 5, "\x4E\x2D"},
		{"中 ", "CHAR", 5, "\x4E\x2D"},
		{"a", "BINARY", 1, "a"},
		{"ab", "BINARY", 1, "a"},
		{"a", "BINARY", 5, "a\x00\x00\x00\x00"},
		{"a ", "BINARY", 5, "a \x00\x00\x00"},
		{"中", "BINARY", 1, "\xe4"},
		{"中", "BINARY", 2, "\xe4\xb8"},
		{"中", "BINARY", 3, "中"},
		{"中", "BINARY", 5, "中\x00\x00"},
	}

	unicodeTests := []weightStringTest{
		{"aAÁàãăâ", "NONE", 0, "\x0e3\x0e3\x0e3\x0e3\x0e3\x0e3\x0e3"},
		{"中", "NONE", 0, "\xfb\x40\xce\x2d"},
		{"a", "CHAR", 5, "\x0e3"},
		{"a ", "CHAR", 5, "\x0e3"},
		{"中", "CHAR", 5, "\xfb\x40\xce\x2d"},
		{"中 ", "CHAR", 5, "\xfb\x40\xce\x2d"},
		{"a", "BINARY", 1, "a"},
		{"ab", "BINARY", 1, "a"},
		{"a", "BINARY", 5, "a\x00\x00\x00\x00"},
		{"a ", "BINARY", 5, "a \x00\x00\x00"},
		{"中", "BINARY", 1, "\xe4"},
		{"中", "BINARY", 2, "\xe4\xb8"},
		{"中", "BINARY", 3, "中"},
		{"中", "BINARY", 5, "中\x00\x00"},
	}

	checkResult("utf8mb4_general_ci", generalTests)
	checkResult("utf8mb4_unicode_ci", unicodeTests)
}
