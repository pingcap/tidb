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
	"log"
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

func getStringConstant(value string) *Constant {
	return &Constant{
		Value:   types.NewStringDatum(value),
		RetType: types.NewFieldType(mysql.TypeVarchar),
	}
}

func getIntConstant(num int64) *Constant {
	return &Constant{
		Value:   types.NewIntDatum(num),
		RetType: types.NewFieldType(mysql.TypeLong),
	}
}

func getVecExprBenchCaseForRegexp(retType types.EvalType, inputs ...interface{}) vecExprBenchCase {
	gens := make([]dataGenerator, 0, 6)
	paramTypes := make([]types.EvalType, 0, 6)

	for _, input := range inputs {
		switch input.(type) {
		case []int:
			actualInput := input.([]int)
			gens = append(gens, &rangeInt64Gener{
				begin:   actualInput[0],
				end:     actualInput[1],
				randGen: newDefaultRandGen(),
			})
			paramTypes = append(paramTypes, types.ETInt)
		case []string:
			strs := make([]string, 0)
			actualInput := input.([]string)
			for _, elem := range actualInput {
				strs = append(strs, elem)
			}
			gens = append(gens, &selectStringGener{
				candidates: strs,
				randGen:    newDefaultRandGen(),
			})
			paramTypes = append(paramTypes, types.ETString)
		default:
			panic("Invalid type")
		}
	}

	return vecExprBenchCase{
		retEvalType:   retType,
		childrenTypes: paramTypes,
		geners:        gens,
	}
}

func setBinCollation(tp *types.FieldType) {
	tp.SetType(mysql.TypeVarString)
	tp.SetCharset(charset.CharsetBin)
	tp.SetCollate(charset.CollationBin)
	tp.SetFlen(types.UnspecifiedLength)
	tp.SetFlag(mysql.BinaryFlag)
}

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
		{".", "\n", "s", 1, nil},
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

func TestRegexpLikeFunctionVec(t *testing.T) {
	var expr []string = []string{"abc", "aBc", "Good\nday", "\n"}
	var pattern []string = []string{"abc", "od$", "^day", "day$", "."}
	var matchType []string = []string{"m", "i", "icc", "cii", "s", "msi"}

	constants := make([]*Constant, 3)
	for i := 0; i < 3; i++ {
		constants[i] = nil
	}

	// Prepare data: expr is constant
	constants[0] = getStringConstant("abc")
	exprConstCase := getVecExprBenchCaseForRegexp(types.ETInt, expr, pattern, matchType)
	exprConstCase.constants = make([]*Constant, 3)
	copy(exprConstCase.constants, constants)
	constants[0] = nil

	// Prepare data: pattern is constant
	constants[1] = getStringConstant("abc")
	patConstCase := getVecExprBenchCaseForRegexp(types.ETInt, expr, pattern, matchType)
	patConstCase.constants = make([]*Constant, 3)
	copy(patConstCase.constants, constants)
	constants[1] = nil

	// Prepare data: matchType is constant
	constants[2] = getStringConstant("ims")
	matchTypeConstCase := getVecExprBenchCaseForRegexp(types.ETInt, expr, pattern, matchType)
	matchTypeConstCase.constants = make([]*Constant, 3)
	copy(matchTypeConstCase.constants, constants)
	constants[2] = nil

	// Prepare data: test memorization
	constants[1] = getStringConstant("abc")
	constants[2] = getStringConstant("ims")
	patAndMatchTypeConstCase := getVecExprBenchCaseForRegexp(types.ETInt, expr, pattern, matchType)
	patAndMatchTypeConstCase.constants = make([]*Constant, 3)
	copy(patAndMatchTypeConstCase.constants, constants)
	constants[1] = nil
	constants[2] = nil

	constants[1] = getStringConstant("abc")
	patConstMatchTpIgnoredCase := getVecExprBenchCaseForRegexp(types.ETInt, expr, pattern)
	patConstMatchTpIgnoredCase.constants = make([]*Constant, 2)
	copy(patConstMatchTpIgnoredCase.constants, constants[:2])
	constants[1] = nil

	// constants[1] = getStringConstant("abc")
	// patConstCase := getVecExprBenchCaseForRegexp(types.ETInt, expr, pattern)
	// patConstCase.constants = make([]*Constant, 3)
	// copy(patConstCase.constants, constants)
	// constants[1] = nil

	// Build vecBuiltinRegexpLikeCases
	var vecBuiltinRegexpLikeCases = map[string][]vecExprBenchCase{
		ast.RegexpLike: {
			getVecExprBenchCaseForRegexp(types.ETInt, expr, pattern),                         // without match type
			getVecExprBenchCaseForRegexp(types.ETInt, expr, pattern, matchType),              // with match type
			getVecExprBenchCaseForRegexp(types.ETInt, make([]string, 0), pattern, matchType), // Test expr == null
			getVecExprBenchCaseForRegexp(types.ETInt, expr, make([]string, 0), matchType),    // Test pattern == null
			getVecExprBenchCaseForRegexp(types.ETInt, expr, pattern, make([]string, 0)),      // Test matchType == null
			exprConstCase,
			patConstCase,
			matchTypeConstCase,
			// patAndMatchTypeConstCase,
		},
	}

	var should = map[string][]vecExprBenchCase{
		ast.RegexpLike: {
			patAndMatchTypeConstCase,
			patConstMatchTpIgnoredCase,
		},
	}

	log.Println("here1")
	testVectorizedBuiltinFunc(t, vecBuiltinRegexpLikeCases)
	log.Println("here2")
	testVectorizedBuiltinFunc(t, should)
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
		{"", "^$", int64(2), nil, nil, ErrRegexp},
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
		{"\n", ".", int64(1), int64(1), "s", "\n", "0x0A", nil},
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

func TestRegexpSubstrVec(t *testing.T) {
	var expr []string = []string{"abc abd abe", "你好啊啊啊啊啊", "好的 好滴 好~", "Good\nday", "\n\n\n\n\n\n"}
	var pattern []string = []string{"^$", "ab.", "aB.", "abc", "好", "好.", "od$", "^day", "day$", "."}
	var position []int = []int{1, 5}
	var occurrence []int = []int{-1, 10}
	var matchType []string = []string{"m", "i", "icc", "cii", "s", "msi"}

	args := make([]interface{}, 0)
	args = append(args, interface{}(expr))
	args = append(args, interface{}(pattern))
	args = append(args, interface{}(position))
	args = append(args, interface{}(occurrence))
	args = append(args, interface{}(matchType))

	constants := make([]*Constant, 5)
	for i := 0; i < 5; i++ {
		constants[i] = nil
	}

	// Prepare data: expr is constant
	constants[0] = getStringConstant("好的 好滴 好~")
	exprConstCase := getVecExprBenchCaseForRegexp(types.ETString, args...)
	exprConstCase.constants = make([]*Constant, 5)
	copy(exprConstCase.constants, constants)
	constants[0] = nil

	// Prepare data: pattern is constant
	constants[1] = getStringConstant("aB.")
	patConstCase := getVecExprBenchCaseForRegexp(types.ETString, args...)
	patConstCase.constants = make([]*Constant, 5)
	copy(patConstCase.constants, constants)
	constants[1] = nil

	// Prepare data: position is constant
	constants[2] = getIntConstant(2)
	posConstCase := getVecExprBenchCaseForRegexp(types.ETString, args...)
	posConstCase.constants = make([]*Constant, 5)
	copy(posConstCase.constants, constants)
	constants[2] = nil

	// Prepare data: occurrence is constant
	constants[3] = getIntConstant(2)
	occurConstCase := getVecExprBenchCaseForRegexp(types.ETString, args...)
	occurConstCase.constants = make([]*Constant, 5)
	copy(occurConstCase.constants, constants)
	constants[3] = nil

	// Prepare data: match type is constant
	constants[4] = getStringConstant("msi")
	matchTpConstCase := getVecExprBenchCaseForRegexp(types.ETString, args...)
	matchTpConstCase.constants = make([]*Constant, 5)
	copy(matchTpConstCase.constants, constants)
	constants[4] = nil

	// Prepare data: test memorization
	constants[1] = getStringConstant("aB.")
	constants[4] = getStringConstant("msi")
	patAndMatchTypeConstCase := getVecExprBenchCaseForRegexp(types.ETString, args...)
	patAndMatchTypeConstCase.constants = make([]*Constant, 5)
	copy(patAndMatchTypeConstCase.constants, constants)
	constants[1] = nil
	constants[4] = nil

	constants[1] = getStringConstant("aB.")
	patConstMatchTypeIgnoredCase := getVecExprBenchCaseForRegexp(types.ETString, args[:4]...)
	patConstMatchTypeIgnoredCase.constants = make([]*Constant, 4)
	copy(patConstMatchTypeIgnoredCase.constants, constants[:4])
	constants[1] = nil

	// Build vecBuiltinRegexpSubstrCases
	var vecBuiltinRegexpSubstrCases = map[string][]vecExprBenchCase{
		ast.RegexpSubstr: {
			getVecExprBenchCaseForRegexp(types.ETString, args...),
			exprConstCase,
			patConstCase,
			posConstCase,
			occurConstCase,
			matchTpConstCase,
			// patAndMatchTypeConstCase,
		},
	}

	var should = map[string][]vecExprBenchCase{
		ast.RegexpSubstr: {
			patAndMatchTypeConstCase,
			patConstMatchTypeIgnoredCase,
		},
	}

	log.Println("here1")
	testVectorizedBuiltinFunc(t, vecBuiltinRegexpSubstrCases)
	log.Println("here2")
	testVectorizedBuiltinFunc(t, should)
}

func TestRegexpInStrConst(t *testing.T) {
	ctx := createContext(t)

	// test regexp_instr(expr, pat)
	testParam2 := []struct {
		input    interface{} // string
		pattern  interface{} // string
		match    interface{} // int64
		matchBin interface{} // bin result
		err      error
	}{
		{"abc", "bc", int64(2), int64(2), nil},
		{"你好", "好", int64(2), int64(4), nil},
		{"", "^$", int64(1), int64(1), nil},
		{"abc", nil, nil, nil, nil},
		{nil, "bc", nil, nil, nil},
		{nil, nil, nil, nil, nil},
	}

	for isBin := 0; isBin <= 1; isBin++ {
		for _, tt := range testParam2 {
			fc := funcs[ast.RegexpInStr]
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

	// test regexp_instr(expr, pat, pos)
	testParam3 := []struct {
		input    interface{} // string
		pattern  interface{} // string
		pos      interface{} // int64
		match    interface{} // int64
		matchBin interface{} // bin result
		err      error
	}{
		{"abc", "bc", int64(2), int64(2), int64(2), nil},
		{"你好", "好", int64(2), int64(2), int64(4), nil},
		{"abc", "bc", int64(3), int64(0), int64(0), nil},
		{"你好啊", "好", int64(3), int64(0), int64(4), nil},
		{"", "^$", int64(1), 1, 1, nil},
		// Invalid position index tests
		{"", "^$", int64(2), 0, 0, ErrRegexp},
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
			fc := funcs[ast.RegexpInStr]
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

	// test regexp_instr(expr, pat, pos, occurrence)
	testParam4 := []struct {
		input      interface{} // string
		pattern    interface{} // string
		pos        interface{} // int64
		occurrence interface{} // int64
		match      interface{} // int64
		matchBin   interface{} // bin result
		err        error
	}{
		{"abc abd abe", "ab.", int64(1), int64(1), 1, 1, nil},
		{"abc abd abe", "ab.", int64(1), int64(0), 1, 1, nil},
		{"abc abd abe", "ab.", int64(1), int64(-1), 1, 1, nil},
		{"abc abd abe", "ab.", int64(1), int64(2), 5, 5, nil},
		{"abc abd abe", "ab.", int64(3), int64(1), 5, 5, nil},
		{"abc abd abe", "ab.", int64(3), int64(2), 9, 9, nil}, // index 5
		{"abc abd abe", "ab.", int64(6), int64(1), 9, 9, nil},
		{"abc abd abe", "ab.", int64(6), int64(100), 0, 0, nil},
		{"嗯嗯 嗯好 嗯呐", "嗯.", int64(1), int64(1), 1, 1, nil},
		{"嗯嗯 嗯好 嗯呐", "嗯.", int64(1), int64(2), 4, 8, nil},
		{"嗯嗯 嗯好 嗯呐", "嗯.", int64(5), int64(1), 7, 8, nil}, // index 10
		{"嗯嗯 嗯好 嗯呐", "嗯.", int64(5), int64(2), 0, 15, nil},
		{"嗯嗯 嗯好 嗯呐", "嗯.", int64(1), int64(100), 0, 0, nil},
		// Some nullable input tests
		{"", "^$", int64(1), nil, nil, nil, nil},
		{nil, "^$", int64(1), nil, nil, nil, nil},
		{nil, "^$", nil, int64(1), nil, nil, nil}, // index 15
		{"", nil, nil, int64(1), nil, nil, nil},
		{nil, nil, nil, nil, nil, nil, nil},
	}

	for isBin := 0; isBin <= 1; isBin++ {
		for _, tt := range testParam4 {
			fc := funcs[ast.RegexpInStr]
			expectMatch := tt.match
			args := datumsToConstants(types.MakeDatums(tt.input, tt.pattern, tt.pos, tt.occurrence))
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

	// test regexp_instr(expr, pat, pos, occurrence, return_option)
	testParam5 := []struct {
		input      interface{} // string
		pattern    interface{} // string
		pos        interface{} // int64
		occurrence interface{} // int64
		retOpt     interface{} // int64
		match      interface{} // int64
		matchBin   interface{} // bin result
		err        error
	}{
		{"abc abd abe", "ab.", int64(1), int64(1), int64(0), 1, 1, nil},
		{"abc abd abe", "ab.", int64(1), int64(1), int64(1), 4, 4, nil},
		{"嗯嗯 嗯好 嗯呐", "嗯.", int64(1), int64(1), int64(0), 1, 1, nil},
		{"嗯嗯 嗯好 嗯呐", "嗯.", int64(1), int64(1), int64(1), 3, 7, nil},
		{"", "^$", int64(1), int64(1), int64(0), 1, 1, nil},
		{"", "^$", int64(1), int64(1), int64(1), 1, 1, nil},
		// Some nullable input tests
		{"", "^$", int64(1), nil, nil, nil, nil, nil},
		{nil, "^$", int64(1), nil, nil, nil, nil, nil},
		{nil, "^$", nil, int64(1), nil, nil, nil, nil},
		{"", nil, nil, int64(1), nil, nil, nil, nil},
		{nil, nil, nil, nil, nil, nil, nil, nil},
	}

	for isBin := 0; isBin <= 1; isBin++ {
		for _, tt := range testParam5 {
			fc := funcs[ast.RegexpInStr]
			expectMatch := tt.match
			args := datumsToConstants(types.MakeDatums(tt.input, tt.pattern, tt.pos, tt.occurrence, tt.retOpt))
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

	// test regexp_instr(expr, pat, pos, occurrence, return_option, match_type)
	testParam6 := []struct {
		input      interface{} // string
		pattern    interface{} // string
		pos        interface{} // int64
		occurrence interface{} // int64
		retOpt     interface{} // int64
		matchType  interface{} // string
		match      interface{} // int64
		matchBin   interface{} // bin result
		err        error
	}{
		{"abc", "ab.", int64(1), int64(1), int64(0), "", 1, 1, nil},
		{"abc", "aB.", int64(1), int64(1), int64(0), "", 0, 0, nil},
		{"abc", "aB.", int64(1), int64(1), int64(0), "i", 1, 1, nil},
		{"good\nday", "od$", int64(1), int64(1), int64(0), "m", 3, 3, nil},
		{"good\nday", "oD$", int64(1), int64(1), int64(0), "mi", 3, 3, nil},
		{"\n", ".", int64(1), int64(1), int64(0), "s", 1, 1, nil}, // index 6
		// Test invalid matchType
		{"abc", "ab.", int64(1), int64(1), int64(0), "p", nil, nil, ErrRegexp},
		// Some nullable input tests
		{"abc", "ab.", int64(1), int64(1), int64(0), nil, nil, nil, nil},
		{"abc", "ab.", nil, int64(1), int64(0), nil, nil, nil, nil},
		{nil, "ab.", nil, int64(1), int64(0), nil, nil, nil, nil},
	}

	for isBin := 0; isBin <= 1; isBin++ {
		for _, tt := range testParam6 {
			fc := funcs[ast.RegexpInStr]
			expectMatch := tt.match
			args := datumsToConstants(types.MakeDatums(tt.input, tt.pattern, tt.pos, tt.occurrence, tt.retOpt, tt.matchType))
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

func TestRegexpInStrVec(t *testing.T) {
	var expr []string = []string{"abc abd abe", "你好啊啊啊啊啊", "好的 好滴 好~", "Good\nday", "\n\n\n\n\n\n"}
	var pattern []string = []string{"^$", "ab.", "aB.", "abc", "好", "好.", "od$", "^day", "day$", "."}
	var position []int = []int{1, 5}
	var occurrence []int = []int{-1, 10}
	var retOpt []int = []int{0, 1}
	var matchType []string = []string{"m", "i", "icc", "cii", "s", "msi"}

	args := make([]interface{}, 0)
	args = append(args, interface{}(expr))
	args = append(args, interface{}(pattern))
	args = append(args, interface{}(position))
	args = append(args, interface{}(occurrence))
	args = append(args, interface{}(retOpt))
	args = append(args, interface{}(matchType))

	constants := make([]*Constant, 6)
	for i := 0; i < 6; i++ {
		constants[i] = nil
	}

	// Prepare data: expr is constant
	constants[0] = getStringConstant("好的 好滴 好~")
	exprConstCase := getVecExprBenchCaseForRegexp(types.ETInt, args...)
	exprConstCase.constants = make([]*Constant, 6)
	copy(exprConstCase.constants, constants)
	constants[0] = nil

	// Prepare data: pattern is constant
	constants[1] = getStringConstant("aB.")
	patConstCase := getVecExprBenchCaseForRegexp(types.ETInt, args...)
	patConstCase.constants = make([]*Constant, 6)
	copy(patConstCase.constants, constants)
	constants[1] = nil

	// Prepare data: position is constant
	constants[2] = getIntConstant(2)
	posConstCase := getVecExprBenchCaseForRegexp(types.ETInt, args...)
	posConstCase.constants = make([]*Constant, 6)
	copy(posConstCase.constants, constants)
	constants[2] = nil

	// Prepare data: occurrence is constant
	constants[3] = getIntConstant(2)
	occurConstCase := getVecExprBenchCaseForRegexp(types.ETInt, args...)
	occurConstCase.constants = make([]*Constant, 6)
	copy(occurConstCase.constants, constants)
	constants[3] = nil

	// Prepare data: return_option is constant
	constants[4] = getIntConstant(1)
	retOptConstCase := getVecExprBenchCaseForRegexp(types.ETInt, args...)
	retOptConstCase.constants = make([]*Constant, 6)
	copy(retOptConstCase.constants, constants)
	constants[4] = nil

	// Prepare data: match type is constant
	constants[5] = getStringConstant("msi")
	matchTypeConstCase := getVecExprBenchCaseForRegexp(types.ETInt, args...)
	matchTypeConstCase.constants = make([]*Constant, 6)
	copy(matchTypeConstCase.constants, constants)
	constants[5] = nil

	// Prepare data: test memorization
	constants[1] = getStringConstant("aB.")
	constants[5] = getStringConstant("msi")
	patAndMatchTypeConstCase := getVecExprBenchCaseForRegexp(types.ETInt, args...)
	patAndMatchTypeConstCase.constants = make([]*Constant, 6)
	copy(patAndMatchTypeConstCase.constants, constants)
	constants[1] = nil
	constants[5] = nil

	constants[1] = getStringConstant("aB.")
	patConstMatchTypeIgnoredCase := getVecExprBenchCaseForRegexp(types.ETInt, args[:5]...)
	patConstMatchTypeIgnoredCase.constants = make([]*Constant, 5)
	copy(patConstMatchTypeIgnoredCase.constants, constants[:5])
	constants[1] = nil

	// Build vecBuiltinRegexpSubstrCases
	var vecBuiltinRegexpInStrCases = map[string][]vecExprBenchCase{
		ast.RegexpInStr: {
			getVecExprBenchCaseForRegexp(types.ETInt, args...),
			exprConstCase,
			patConstCase,
			posConstCase,
			occurConstCase,
			retOptConstCase,
			matchTypeConstCase,
			// patAndMatchTypeConstCase,
		},
	}

	var should = map[string][]vecExprBenchCase{
		ast.RegexpInStr: {
			patAndMatchTypeConstCase,
			patConstMatchTypeIgnoredCase,
		},
	}

	log.Println("here1")
	testVectorizedBuiltinFunc(t, vecBuiltinRegexpInStrCases)
	log.Println("here1")
	testVectorizedBuiltinFunc(t, should)
}

func TestRegexpReplaceConst(t *testing.T) {
	ctx := createContext(t)

	// test regexp_replace(expr, pat, repl)
	testParam3 := []struct {
		input    interface{} // string
		pattern  interface{} // string
		replace  interface{} // string
		match    interface{} // string
		matchBin interface{} // bin result
		err      error
	}{
		{"abc abd abe", "ab.", "cz", "cz cz cz", "0x637A20637A20637A", nil},
		{"你好 好的", "好", "逸", "你逸 逸的", "0xE4BDA0E980B820E980B8E79A84", nil},
		{"", "^$", "123", "", "0x", nil},
		{"abc", nil, nil, nil, nil, nil},
		{nil, "bc", nil, nil, nil, nil},
		{nil, nil, nil, nil, nil, nil},
	}

	for isBin := 0; isBin <= 1; isBin++ {
		for _, tt := range testParam3 {
			fc := funcs[ast.RegexpReplace]
			expectMatch := tt.match
			args := datumsToConstants(types.MakeDatums(tt.input, tt.pattern, tt.replace))
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

	// test regexp_replace(expr, pat, repl, pos)
	testParam4 := []struct {
		input    interface{} // string
		pattern  interface{} // string
		replace  interface{} // string
		pos      interface{} // int64
		match    interface{} // string
		matchBin interface{} // bin result
		err      error
	}{
		{"abc", "ab.", "cc", int64(1), "cc", "0x6363", nil},
		{"abc", "bc", "cc", int64(3), "abc", "0x616263", nil},
		{"你好", "好", "的", int64(2), "你的", "0xE4BDA0E79A84", nil},
		{"你好啊", "好", "的", int64(3), "你好啊", "0xE4BDA0E79A84E5958A", nil},
		{"", "^$", "cc", int64(1), "", "0x", nil},
		// Invalid position index tests
		{"", "^$", "a", int64(2), "", "", ErrRegexp},
		{"", "^&", "a", int64(0), "", "", ErrRegexp},
		{"abc", "bc", "a", int64(-1), "", "", ErrRegexp},
		{"abc", "bc", "a", int64(4), "", "", ErrRegexp},
		// Some nullable input tests
		{"", "^$", "a", nil, nil, nil, nil},
		{nil, "^$", "a", nil, nil, nil, nil},
		{"", nil, nil, nil, nil, nil, nil},
		{nil, nil, nil, int64(1), nil, nil, nil},
		{nil, nil, nil, nil, nil, nil, nil},
	}

	for isBin := 0; isBin <= 1; isBin++ {
		for _, tt := range testParam4 {
			fc := funcs[ast.RegexpReplace]
			expectMatch := tt.match
			args := datumsToConstants(types.MakeDatums(tt.input, tt.pattern, tt.replace, tt.pos))
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

	// test regexp_replace(expr, pat, repl, pos, occurrence)
	testParam5 := []struct {
		input      interface{} // string
		pattern    interface{} // string
		replace    interface{} // string
		pos        interface{} // int64
		occurrence interface{} // int64
		match      interface{} // string
		matchBin   interface{} // bin result
		err        error
	}{
		{"abc abd", "ab.", "cc", int64(1), int64(1), "cc abd", "0x636320616264", nil},
		{"abc abd", "ab.", "cc", int64(1), int64(2), "abc cc", "0x616263206363", nil},
		{"abc abd", "ab.", "cc", int64(1), int64(0), "cc cc", "0x6363206363", nil},
		{"abc abd abe", "ab.", "cc", int64(3), int64(2), "abc abd cc", "0x61626320616264206363", nil},
		{"abc abd abe", "ab.", "cc", int64(3), int64(10), "abc abd abe", "0x6162632061626420616265", nil},
		{"你好 好啊", "好", "的", int64(1), int64(1), "你的 好啊", "0xE4BDA0E79A8420E5A5BDE5958A", nil}, // index 5
		{"你好 好啊", "好", "的", int64(3), int64(1), "你好 的啊", "0xE4BDA0E79A8420E5A5BDE5958A", nil},
		{"", "^$", "cc", int64(1), int64(1), "", "0x", nil},
		{"", "^$", "cc", int64(1), int64(2), "", "0x", nil},
		{"", "^$", "cc", int64(1), int64(-1), "", "0x", nil},
		// Some nullable input tests
		{"", "^$", "a", nil, int64(1), nil, nil, nil}, // index 10
		{nil, "^$", "a", nil, nil, nil, nil, nil},
		{"", nil, nil, nil, int64(1), nil, nil, nil},
		{nil, nil, nil, int64(1), int64(1), nil, nil, nil},
		{nil, nil, nil, nil, nil, nil, nil, nil},
	}

	for isBin := 0; isBin <= 1; isBin++ {
		for _, tt := range testParam5 {
			fc := funcs[ast.RegexpReplace]
			expectMatch := tt.match
			args := datumsToConstants(types.MakeDatums(tt.input, tt.pattern, tt.replace, tt.pos, tt.occurrence))
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

	// test regexp_replace(expr, pat, repl, pos, occurrence, match_type)
	testParam6 := []struct {
		input      interface{} // string
		pattern    interface{} // string
		replace    interface{} // string
		pos        interface{} // int64
		occurrence interface{} // int64
		matchType  interface{} // string
		match      interface{} // string
		matchBin   interface{} // bin result
		err        error
	}{
		{"abc", "ab.", "cc", int64(1), int64(0), "", "cc", "0x6363", nil},
		{"abc", "aB.", "cc", int64(1), int64(0), "i", "cc", "0x6363", nil},
		{"good\nday", "od$", "cc", int64(1), int64(0), "m", "gocc\nday", "0x676F63630A646179", nil},
		{"good\nday", "oD$", "cc", int64(1), int64(0), "mi", "gocc\nday", "0x676F63630A646179", nil},
		{"\n", ".", "cc", int64(1), int64(0), "s", "cc", "0x6363", nil},
		{"好的 好滴 好~", ".", "的", int64(1), int64(0), "msi", "的的的的的的的的", "0xE79A84E79A84E79A84E79A84E79A84E79A84E79A84E79A84", nil},
		// Test invalid matchType
		{"abc", "ab.", "cc", int64(1), int64(0), "p", nil, nil, ErrRegexp},
		// Some nullable input tests
		{"abc", "ab.", "cc", int64(1), int64(0), nil, nil, nil, nil},
		{"abc", "ab.", nil, int64(1), int64(0), nil, nil, nil, nil},
		{nil, "ab.", nil, int64(1), int64(0), nil, nil, nil, nil},
	}

	for isBin := 0; isBin <= 1; isBin++ {
		for _, tt := range testParam6 {
			fc := funcs[ast.RegexpReplace]
			expectMatch := tt.match
			args := datumsToConstants(types.MakeDatums(tt.input, tt.pattern, tt.replace, tt.pos, tt.occurrence, tt.matchType))
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

func TestRegexpReplaceVec(t *testing.T) {
	var expr []string = []string{"abc abd abe", "你好啊啊啊啊啊", "好的 好滴 好~", "Good\nday", "\n\n\n\n\n\n"}
	var pattern []string = []string{"^$", "ab.", "aB.", "abc", "好", "好.", "od$", "^day", "day$", "."}
	var repl []string = []string{"cc", "的"}
	var position []int = []int{1, 5}
	var occurrence []int = []int{-1, 5}
	var matchType []string = []string{"m", "i", "icc", "cii", "s", "msi"}

	args := make([]interface{}, 0)
	args = append(args, interface{}(expr))
	args = append(args, interface{}(pattern))
	args = append(args, interface{}(repl))
	args = append(args, interface{}(position))
	args = append(args, interface{}(occurrence))
	args = append(args, interface{}(matchType))

	constants := make([]*Constant, 6)
	for i := 0; i < 6; i++ {
		constants[i] = nil
	}

	// Prepare data: expr is constant
	constants[0] = getStringConstant("好的 好滴 好~")
	exprConstCase := getVecExprBenchCaseForRegexp(types.ETString, args...)
	exprConstCase.constants = make([]*Constant, 6)
	copy(exprConstCase.constants, constants)
	constants[0] = nil

	// Prepare data: pattern is constant
	constants[1] = getStringConstant("aB.")
	patConstCase := getVecExprBenchCaseForRegexp(types.ETString, args...)
	patConstCase.constants = make([]*Constant, 6)
	copy(patConstCase.constants, constants)
	constants[1] = nil

	// Prepare data: repl is constant
	constants[2] = getStringConstant("cc")
	replConstCase := getVecExprBenchCaseForRegexp(types.ETString, args...)
	replConstCase.constants = make([]*Constant, 6)
	copy(replConstCase.constants, constants)
	constants[2] = nil

	// Prepare data: position is constant
	constants[3] = getIntConstant(2)
	posConstCase := getVecExprBenchCaseForRegexp(types.ETString, args...)
	posConstCase.constants = make([]*Constant, 6)
	copy(posConstCase.constants, constants)
	constants[3] = nil

	// Prepare data: occurrence is constant
	constants[4] = getIntConstant(2)
	occurConstCase := getVecExprBenchCaseForRegexp(types.ETString, args...)
	occurConstCase.constants = make([]*Constant, 6)
	copy(occurConstCase.constants, constants)
	constants[4] = nil

	// Prepare data: match type is constant
	constants[5] = getStringConstant("msi")
	matchTypeConstCase := getVecExprBenchCaseForRegexp(types.ETString, args...)
	matchTypeConstCase.constants = make([]*Constant, 6)
	copy(matchTypeConstCase.constants, constants)
	constants[5] = nil

	// Prepare data: test memorization
	constants[1] = getStringConstant("aB.")
	constants[5] = getStringConstant("msi")
	patAndMatchTypeConstCase := getVecExprBenchCaseForRegexp(types.ETString, args...)
	patAndMatchTypeConstCase.constants = make([]*Constant, 6)
	copy(patAndMatchTypeConstCase.constants, constants)
	constants[1] = nil
	constants[5] = nil

	constants[1] = getStringConstant("aB.")
	patConstMatchTypeIgnoredCase := getVecExprBenchCaseForRegexp(types.ETString, args[:5]...)
	patConstMatchTypeIgnoredCase.constants = make([]*Constant, 5)
	copy(patConstMatchTypeIgnoredCase.constants, constants[:5])
	constants[1] = nil

	// Build vecBuiltinRegexpSubstrCases
	var vecBuiltinRegexpReplaceCases = map[string][]vecExprBenchCase{
		ast.RegexpReplace: {
			getVecExprBenchCaseForRegexp(types.ETString, args...),
			exprConstCase,
			patConstCase,
			replConstCase,
			posConstCase,
			occurConstCase,
			matchTypeConstCase,
			// patAndMatchTypeConstCase,
		},
	}

	var should = map[string][]vecExprBenchCase{
		ast.RegexpReplace: {
			patAndMatchTypeConstCase,
			patConstMatchTypeIgnoredCase,
		},
	}

	log.Println("here1")
	testVectorizedBuiltinFunc(t, vecBuiltinRegexpReplaceCases)
	log.Println("here2")
	testVectorizedBuiltinFunc(t, should)
}
