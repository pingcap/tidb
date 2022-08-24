// Copyright 2022 PingCAP, Inc.
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
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/testkit/testutil"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/stretchr/testify/require"
)

// test Regexp_like function when all parameters are constant
func TestRegexpLikeConst(t *testing.T) {
	ctx := createContext(t)

	// test Regexp_like without match type
	testsExcludeMatchType := []struct {
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

	for _, tt := range testsExcludeMatchType {
		fc := funcs[ast.Regexp]
		f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(tt.input, tt.pattern)))
		require.NoError(t, err)
		match, err := evalBuiltinFunc(f, chunk.Row{})
		if tt.err == nil {
			require.NoError(t, err)
			testutil.DatumEqual(t, types.NewDatum(tt.match), match, fmt.Sprintf("%v", tt))
		} else {
			require.True(t, terror.ErrorEqual(err, tt.err))
		}
	}

	// test Regexp_like with match type
	testsIncludeMatchType := []struct {
		pattern   string
		input     string
		matchType string
		match     int64
		err       error
	}{
		{"^$", "a", "", 0, nil},
		{"a", "a", "", 1, nil},
		{"a", "b", "", 0, nil},
		{"aA", "aA", "", 1, nil},
		{".", "a", "", 1, nil},
		{"^.$", "ab", "", 0, nil},
		{"..", "b", "", 0, nil},
		{".ab", "aab", "", 1, nil},
		{".*", "abcd", "", 1, nil},
		// Test case-insensitive
		{"AbC", "abc", "", 0, nil},
		{"AbC", "abc", "i", 1, nil},
		// Test multiple-line mode
		{"23$", "123\n321", "", 0, nil},
		{"23$", "123\n321", "m", 1, nil},
		{"^day", "good\nday", "m", 1, nil},
		// Test n flag
		{".", "\n", "", 0, nil},
		{".", "\n", "n", 1, nil},
		// Test rightmost rule
		{"aBc", "abc", "ic", 0, nil},
		{"aBc", "abc", "ci", 1, nil},
		// Test invalid match type
		{"abc", "abc", "p", 0, ErrRegexp},
		{"abc", "abc", "cpi", 0, ErrRegexp},
	}

	for _, tt := range testsIncludeMatchType {
		fc := funcs[ast.RegexpLike]
		f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(tt.input, tt.pattern, tt.matchType)))
		require.NoError(t, err)
		match, err := evalBuiltinFunc(f, chunk.Row{})
		if tt.err == nil {
			require.NoError(t, err)
			testutil.DatumEqual(t, types.NewDatum(tt.match), match, fmt.Sprintf("%v", tt))
		} else {
			require.True(t, terror.ErrorEqual(err, tt.err))
		}
	}
}

func getVecExprBenchCaseForRegexpLike(inputs ...[]string) vecExprBenchCase {
	gens := make([]dataGenerator, 0, 3)
	paramTypes := make([]types.EvalType, 0, 3)
	for _, input := range inputs {
		gens = append(gens, &selectStringGener{
			candidates: input,
			randGen:    newDefaultRandGen(),
		})
		paramTypes = append(paramTypes, types.ETString)
	}

	return vecExprBenchCase{
		retEvalType:   types.ETInt,
		childrenTypes: paramTypes,
		geners:        gens,
	}
}

func getStringConstant(value string) *Constant {
	return &Constant{
		Value:   types.NewStringDatum(value),
		RetType: types.NewFieldType(mysql.TypeVarchar),
	}
}

func TestRegexpLikeFunctionVec(t *testing.T) {
	var expr []string = []string{"abc", "aBc", "Good\nday", "\n"}
	var pattern []string = []string{"abc", "od$", "^day", "day$", "."}
	var matchType []string = []string{"m", "i", "icc", "cii", "n", "mni"}

	constants := make([]*Constant, 3)

	// Prepare data: expr is constant
	constants[0] = getStringConstant("abc")
	constants[1] = nil
	constants[2] = nil
	exprConstCase := getVecExprBenchCaseForRegexpLike(expr, pattern, matchType)
	exprConstCase.constants = constants

	// Prepare data: pattern is constant
	constants[0] = nil
	constants[1] = getStringConstant("abc")
	patConstCase := getVecExprBenchCaseForRegexpLike(expr, pattern)
	patConstCase.constants = constants

	// Prepare data: matchType is constant
	constants[0] = nil
	constants[1] = nil
	constants[2] = getStringConstant("imn")
	matchTypeConstCase := getVecExprBenchCaseForRegexpLike(expr, pattern, matchType)
	matchTypeConstCase.constants = constants

	// Prepare data: pattern and matchType are constant
	constants[0] = nil
	constants[1] = getStringConstant("abc")
	constants[2] = getStringConstant("imn")
	patAndMatchTypeConstCase := getVecExprBenchCaseForRegexpLike(expr, pattern, matchType)
	patAndMatchTypeConstCase.constants = constants

	// Build vecBuiltinRegexpLikeCases
	var vecBuiltinRegexpLikeCases = map[string][]vecExprBenchCase{
		ast.RegexpLike: {
			getVecExprBenchCaseForRegexpLike(expr, pattern),                         // without match type
			getVecExprBenchCaseForRegexpLike(expr, pattern, matchType),              // with match type
			getVecExprBenchCaseForRegexpLike(make([]string, 0), pattern, matchType), // Test expr == null
			getVecExprBenchCaseForRegexpLike(expr, make([]string, 0), matchType),    // Test pattern == null
			getVecExprBenchCaseForRegexpLike(expr, pattern, make([]string, 0)),      // Test matchType == null
			exprConstCase,
			patConstCase,
			matchTypeConstCase,
			patAndMatchTypeConstCase,
		},
	}

	testVectorizedBuiltinFunc(t, vecBuiltinRegexpLikeCases)
}

func setBinCollation(tp *types.FieldType) {
	tp.SetType(mysql.TypeVarString)
	tp.SetCharset(charset.CharsetBin)
	tp.SetCollate(charset.CollationBin)
	tp.SetFlen(types.UnspecifiedLength)
	tp.SetFlag(mysql.BinaryFlag)
}

func TestRegexpSubstrConst(t *testing.T) {
	ctx := createContext(t)

	// test regexp_substr(expr, pat)
	testParam2 := []struct {
		input    string
		pattern  string
		match    interface{}
		matchBin interface{} // bin result
		err      error
	}{
		// {"abc", "bc", "bc", "0x6263", nil},
		// {"你好", "好", "好", "0xE5A5BD", nil},
	}

	for isBin := 0; isBin <= 1; isBin++ {
		for _, tt := range testParam2 {
			fc := funcs[ast.RegexpSubstr]
			expectMatch := tt.match
			args := datumsToConstants(types.MakeDatums(tt.input, tt.pattern))
			if isBin == 1 {
				setBinCollation(args[0].GetType())
				expectMatch = tt.matchBin
			}
			f, err := fc.getFunction(ctx, args)
			require.NoError(t, err)

			actualMatch, err := evalBuiltinFunc(f, chunk.Row{})
			if tt.err == nil {
				require.NoError(t, err)
				testutil.DatumEqual(t, types.NewDatum(expectMatch), actualMatch, fmt.Sprintf("%v", tt))
			} else {
				require.True(t, terror.ErrorEqual(err, tt.err))
			}
		}
	}

	// test regexp_substr(expr, pat, pos)
	testParam3 := []struct {
		input    string
		pattern  string
		pos      int64
		match    interface{}
		matchBin interface{} // bin result
		err      error
	}{
		// {"abc", "bc", 2, "bc", "0x6263", nil},
		// {"你好", "好", 2, "好", "0xE5A5BD", nil},
		// {"abc", "bc", 3, nil, nil, nil},
		{"你好啊", "好", 3, nil, nil, nil},
		// {"abc", "bc", -1, nil, nil, ErrRegexp},
	}

	for isBin := 0; isBin <= 1; isBin++ {
		for i, tt := range testParam3 {
			fc := funcs[ast.RegexpSubstr]
			expectMatch := tt.match
			args := datumsToConstants(types.MakeDatums(tt.input, tt.pattern, tt.pos))
			if isBin == 1 {
				setBinCollation(args[0].GetType())
				expectMatch = tt.matchBin
			}
			f, err := fc.getFunction(ctx, args)
			require.NoError(t, err)

			fmt.Println("index: ", i)
			actualMatch, err := evalBuiltinFunc(f, chunk.Row{})
			fmt.Println("actualMatch: ", actualMatch)
			if expectMatch != nil {
				fmt.Println("expectMatch: ", expectMatch)
			} else {
				fmt.Println("expectMatch: null")
			}
			if tt.err == nil {
				require.NoError(t, err)
				testutil.DatumEqual(t, types.NewDatum(expectMatch), actualMatch, fmt.Sprintf("%v", tt))
			} else {
				require.True(t, terror.ErrorEqual(err, tt.err))
			}
		}
	}
}
