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
		input    interface{} // string
		pattern  interface{} // string
		match    interface{} // string
		matchBin interface{} // bin result
		err      error
	}{
		{"abc", "bc", "bc", "0x6263", nil},
		{"你好", "好", "好", "0xE5A5BD", nil},
		{"abc", nil, nil, nil, nil},
		{nil, "bc", nil, nil, nil},
		{nil, nil, nil, nil, nil},
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
		input    interface{} // string
		pattern  interface{} // string
		pos      interface{} // int64
		match    interface{} // string
		matchBin interface{} // bin result
		err      error
	}{
		{"abc", "bc", int64(2), "bc", "0x6263", nil},
		{"你好", "好", int64(2), "好", "0xE5A5BD", nil},
		{"abc", "bc", int64(3), nil, nil, nil},
		{"你好啊", "好", int64(3), nil, "0xE5A5BD", nil},
		{"", "^$", int64(1), "", "0x", nil},
		// Invalid position index tests
		{"abc", "bc", int64(-1), nil, nil, ErrRegexp},
		{"abc", "bc", int64(4), nil, nil, ErrRegexp},
		{"", "bc", int64(0), nil, nil, ErrRegexp},
		// Some nullable input tests
		{"", "^$", nil, nil, nil, nil},
		{nil, "^$", nil, nil, nil, nil},
		{"", nil, nil, nil, nil, nil},
		{nil, nil, int64(1), nil, nil, nil},
		{nil, nil, nil, nil, nil, nil},
	}

	for isBin := 0; isBin <= 1; isBin++ {
		for _, tt := range testParam3 {
			fc := funcs[ast.RegexpSubstr]
			expectMatch := tt.match
			args := datumsToConstants(types.MakeDatums(tt.input, tt.pattern, tt.pos))
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

	// test regexp_substr(expr, pat, pos, occurrence)
	testParam4 := []struct {
		input    interface{} // string
		pattern  interface{} // string
		pos      interface{} // int64
		occur    interface{} // int64
		match    interface{} // string
		matchBin interface{} // bin result
		err      error
	}{
		{"abc abd abe", "ab.", int64(1), int64(1), "abc", "0x616263", nil},
		{"abc abd abe", "ab.", int64(1), int64(0), "abc", "0x616263", nil},
		{"abc abd abe", "ab.", int64(1), int64(-1), "abc", "0x616263", nil},
		{"abc abd abe", "ab.", int64(1), int64(2), "abd", "0x616264", nil},
		{"abc abd abe", "ab.", int64(3), int64(1), "abd", "0x616264", nil},
		{"abc abd abe", "ab.", int64(3), int64(2), "abe", "0x616265", nil}, // index 5
		{"abc abd abe", "ab.", int64(6), int64(1), "abe", "0x616265", nil},
		{"abc abd abe", "ab.", int64(6), int64(100), nil, nil, nil},
		{"嗯嗯 嗯好 嗯呐", "嗯.", int64(1), int64(1), "嗯嗯", "0xE597AFE597AF", nil},
		{"嗯嗯 嗯好 嗯呐", "嗯.", int64(1), int64(2), "嗯好", "0xE597AFE5A5BD", nil},
		{"嗯嗯 嗯好 嗯呐", "嗯.", int64(5), int64(1), "嗯呐", "0xE597AFE5A5BD", nil}, // index 10
		{"嗯嗯 嗯好 嗯呐", "嗯.", int64(5), int64(2), nil, "0xE597AFE59190", nil},
		{"嗯嗯 嗯好 嗯呐", "嗯.", int64(1), int64(100), nil, nil, nil},
		// Some nullable input tests
		{"", "^$", int64(1), nil, nil, nil, nil},
		{nil, "^$", int64(1), nil, nil, nil, nil},
		{nil, "^$", nil, int64(1), nil, nil, nil}, // index 15
		{"", nil, nil, int64(1), nil, nil, nil},
		{nil, nil, nil, nil, nil, nil, nil},
	}

	for isBin := 0; isBin <= 1; isBin++ {
		for _, tt := range testParam4 {
			fc := funcs[ast.RegexpSubstr]
			expectMatch := tt.match
			args := datumsToConstants(types.MakeDatums(tt.input, tt.pattern, tt.pos, tt.occur))
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

	// test regexp_substr(expr, pat, pos, occurrence, matchType)
	testParam5 := []struct {
		input     interface{} // string
		pattern   interface{} // string
		pos       interface{} // int64
		occur     interface{} // int64
		matchType interface{} // string
		match     interface{} // string
		matchBin  interface{} // bin result
		err       error
	}{
		{"abc", "ab.", int64(1), int64(1), "", "abc", "0x616263", nil},
		{"abc", "aB.", int64(1), int64(1), "", nil, nil, nil},
		{"abc", "aB.", int64(1), int64(1), "i", "abc", "0x616263", nil},
		{"good\nday", "od", int64(1), int64(1), "m", "od", "0x6F64", nil},
		{"\n", ".", int64(1), int64(1), "n", "\n", "0x0A", nil},
		// Test invalid matchType
		{"abc", "ab.", int64(1), int64(1), "p", nil, nil, ErrRegexp}, // index 5
		// Some nullable input tests
		{"abc", "ab.", int64(1), int64(1), nil, nil, nil, nil},
		{"abc", "ab.", nil, int64(1), nil, nil, nil, nil},
		{nil, "ab.", nil, int64(1), nil, nil, nil, nil},
	}

	for isBin := 0; isBin <= 1; isBin++ {
		for _, tt := range testParam5 {
			fc := funcs[ast.RegexpSubstr]
			expectMatch := tt.match
			args := datumsToConstants(types.MakeDatums(tt.input, tt.pattern, tt.pos, tt.occur, tt.matchType))
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
}
