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

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/testkit/testutil"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

// should be 5
// We will raise error for binary collation so far,
// so we have to suppress the binary collation tests.
const testCharsetAndCollateTpNum = 5 - 1
const binaryTpIdx = 4

func getStringConstNull() *Constant {
	c := getStringConstant("", false)
	c.Value.SetNull()
	return c
}

func getIntConstNull() *Constant {
	c := getIntConstant(0)
	c.Value.SetNull()
	return c
}

func getStringConstant(value string, isBin bool) *Constant {
	c := &Constant{
		Value: types.NewStringDatum(value),
	}

	if isBin {
		c.RetType = types.NewFieldTypeBuilder().SetType(mysql.TypeString).SetFlag(mysql.BinaryFlag).SetCharset(charset.CharsetBin).SetCollate(charset.CollationBin).BuildP()
	} else {
		c.RetType = types.NewFieldType(mysql.TypeVarchar)
	}

	return c
}

func getIntConstant(num int64) *Constant {
	return &Constant{
		Value:   types.NewIntDatum(num),
		RetType: types.NewFieldType(mysql.TypeLong),
	}
}

func setConstants(isNull bool, isBin bool, constVals map[int]any, constants []*Constant) {
	for i, val := range constVals {
		switch v := val.(type) {
		case string:
			if isNull {
				constants[i] = getStringConstNull()
			} else {
				constants[i] = getStringConstant(v, isBin)
			}
		case int64:
			if isNull {
				constants[i] = getIntConstNull()
			} else {
				constants[i] = getIntConstant(v)
			}
		default:
			panic("Unsupport type")
		}
	}
}

func getVecExprBenchCaseForRegexpIncludeConst(retType types.EvalType, isBin bool, isNull bool, constVals map[int]any, paramNum int, constants []*Constant, inputs ...any) vecExprBenchCase {
	setConstants(isNull, isBin, constVals, constants)

	defer func() {
		// reset constants, so that following cases could reuse this constant slice
		for i := range constVals {
			constants[i] = nil
		}
	}()

	retCase := getVecExprBenchCaseForRegexp(retType, isBin, inputs[:paramNum]...)
	retCase.constants = make([]*Constant, paramNum)
	copy(retCase.constants, constants[:paramNum])
	return retCase
}

func getVecExprBenchCaseForRegexp(retType types.EvalType, isBin bool, inputs ...any) vecExprBenchCase {
	gens := make([]dataGenerator, 0, 6)
	paramTypes := make([]types.EvalType, 0, 6)

	for _, input := range inputs {
		switch input := input.(type) {
		case []int:
			gens = append(gens, &rangeInt64Gener{
				begin:   input[0],
				end:     input[1],
				randGen: newDefaultRandGen(),
			})
			paramTypes = append(paramTypes, types.ETInt)
		case []string:
			strs := make([]string, 0)
			strs = append(strs, input...)
			gens = append(gens, &selectStringGener{
				candidates: strs,
				randGen:    newDefaultRandGen(),
			})
			paramTypes = append(paramTypes, types.ETString)
		default:
			panic("Invalid type")
		}
	}

	ret := vecExprBenchCase{
		retEvalType:   retType,
		childrenTypes: paramTypes,
		geners:        gens,
	}

	if isBin {
		length := len(inputs)
		ft := make([]*types.FieldType, length)
		ft[0] = types.NewFieldTypeBuilder().SetType(mysql.TypeString).SetFlag(mysql.BinaryFlag).SetCharset(charset.CharsetBin).SetCollate(charset.CollationBin).BuildP()
		ret.childrenFieldTypes = ft
	}
	return ret
}

func setCharsetAndCollation(id int, tps ...*types.FieldType) {
	switch id {
	case 0:
		for _, tp := range tps {
			setUtf8mb4CICollation(tp)
		}
	case 1:
		for _, tp := range tps {
			setUtf8mb4BinCollation(tp)
		}
	case 2:
		for _, tp := range tps {
			setGBKCICollation(tp)
		}
	case 3:
		for _, tp := range tps {
			setGBKBinCollation(tp)
		}
	case binaryTpIdx:
		for _, tp := range tps {
			setBinaryCollation(tp)
		}
	default:
		panic("Invalid index")
	}
}

func setUtf8mb4CICollation(tp *types.FieldType) {
	tp.SetType(mysql.TypeVarString)
	tp.SetCharset(charset.CharsetUTF8MB4)
	tp.SetCollate("utf8mb4_general_ci")
	tp.SetFlen(types.UnspecifiedLength)
}

func setUtf8mb4BinCollation(tp *types.FieldType) {
	tp.SetType(mysql.TypeVarString)
	tp.SetCharset(charset.CharsetUTF8MB4)
	tp.SetCollate(charset.CollationUTF8MB4)
	tp.SetFlen(types.UnspecifiedLength)
}

func setGBKCICollation(tp *types.FieldType) {
	tp.SetType(mysql.TypeVarString)
	tp.SetCharset(charset.CharsetGBK)
	tp.SetCollate(charset.CollationGBKChineseCI)
	tp.SetFlen(types.UnspecifiedLength)
}

func setGBKBinCollation(tp *types.FieldType) {
	tp.SetType(mysql.TypeVarString)
	tp.SetCharset(charset.CharsetGBK)
	tp.SetCollate(charset.CollationGBKBin)
	tp.SetFlen(types.UnspecifiedLength)
}

func setBinaryCollation(tp *types.FieldType) {
	tp.SetFlag(mysql.BinaryFlag)
	tp.SetType(mysql.TypeVarString)
	tp.SetCharset(charset.CharsetBin)
	tp.SetCollate(charset.CollationBin)
}

func TestRegexpLike(t *testing.T) {
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
		{"^.$", "ab", 0, nil}, // index 5
		{"..", "b", 0, nil},
		{".ab", "aab", 1, nil},
		{".*", "abcd", 1, nil},
		{"", "a", 0, ErrRegexp}, // issue 37988
		{"(", "", 0, ErrRegexp}, // index 10
		{"(*", "", 0, ErrRegexp},
		{"[a", "", 0, ErrRegexp},
		{"\\", "", 0, ErrRegexp},
	}

	for _, tt := range testsExcludeMatchType {
		fc := funcs[ast.Regexp]
		f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(tt.input, tt.pattern)))
		require.NoError(t, err)
		match, err := evalBuiltinFunc(f, ctx, chunk.Row{})
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
		match, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		if tt.err == nil {
			require.NoError(t, err)
			testutil.DatumEqual(t, types.NewDatum(tt.match), match, fmt.Sprintf("%v", tt))
		} else {
			require.True(t, terror.ErrorEqual(err, tt.err))
		}
	}
}

func TestRegexpLikeVec(t *testing.T) {
	var expr []string = []string{"abc", "aBc", "Good\nday", "\n"}
	var pattern []string = []string{"abc", "od$", "^day", "day$", "."}
	var matchType []string = []string{"m", "i", "icc", "cii", "s", "msi"}

	constants := make([]*Constant, 3)
	for i := 0; i < 3; i++ {
		constants[i] = nil
	}

	args := make([]any, 0)
	args = append(args, any(expr))
	args = append(args, any(pattern))
	args = append(args, any(matchType))

	cases := make([]vecExprBenchCase, 0, 30)

	cases = append(cases, getVecExprBenchCaseForRegexp(types.ETInt, false, expr, pattern))                         // without match type
	cases = append(cases, getVecExprBenchCaseForRegexp(types.ETInt, false, expr, pattern))                         // without match type, with BinCollation
	cases = append(cases, getVecExprBenchCaseForRegexp(types.ETInt, false, expr, pattern, matchType))              // with match type
	cases = append(cases, getVecExprBenchCaseForRegexp(types.ETInt, false, expr, pattern, matchType))              // with match type, with BinCollation
	cases = append(cases, getVecExprBenchCaseForRegexp(types.ETInt, false, make([]string, 0), pattern, matchType)) // Test expr == null
	cases = append(cases, getVecExprBenchCaseForRegexp(types.ETInt, false, make([]string, 0), pattern, matchType)) // Test expr == null, with BinCollation
	cases = append(cases, getVecExprBenchCaseForRegexp(types.ETInt, false, expr, make([]string, 0), matchType))    // Test pattern == null
	cases = append(cases, getVecExprBenchCaseForRegexp(types.ETInt, false, expr, make([]string, 0), matchType))    // Test pattern == null, with BinCollation
	cases = append(cases, getVecExprBenchCaseForRegexp(types.ETInt, false, expr, pattern, make([]string, 0)))      // Test matchType == null
	cases = append(cases, getVecExprBenchCaseForRegexp(types.ETInt, false, expr, pattern, make([]string, 0)))      // Test matchType == null, with BinCollation

	// Prepare data: expr is constant
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETInt, false, false, map[int]any{0: any("abc")}, len(args), constants, args...)) // index 10
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETInt, false, true, map[int]any{0: any("abc")}, len(args), constants, args...))
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETInt, false, false, map[int]any{0: any("abc")}, len(args), constants, args...))
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETInt, false, true, map[int]any{0: any("abc")}, len(args), constants, args...))

	// Prepare data: pattern is constant
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETInt, false, false, map[int]any{1: any("ab.")}, len(args), constants, args...))
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETInt, false, true, map[int]any{1: any("ab.")}, len(args), constants, args...))
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETInt, false, false, map[int]any{1: any("ab.")}, len(args), constants, args...))
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETInt, false, true, map[int]any{1: any("ab.")}, len(args), constants, args...))

	// Prepare data: matchType is constant
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETInt, false, false, map[int]any{2: any("msi")}, len(args), constants, args...))
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETInt, false, true, map[int]any{2: any("msi")}, len(args), constants, args...))
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETInt, false, false, map[int]any{2: any("msi")}, len(args), constants, args...)) // index 20
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETInt, false, true, map[int]any{2: any("msi")}, len(args), constants, args...))

	// Prepare data: test memorization
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETInt, false, false, map[int]any{1: any("abc"), 2: any("msi")}, len(args), constants, args...))
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETInt, false, false, map[int]any{1: any("abc")}, len(args)-1, constants, args...))

	// Build vecBuiltinRegexpLikeCases
	var vecBuiltinRegexpLikeCases = map[string][]vecExprBenchCase{
		ast.RegexpLike: cases,
	}

	testVectorizedBuiltinFunc(t, vecBuiltinRegexpLikeCases)
}

func TestRegexpSubstr(t *testing.T) {
	ctx := createContext(t)

	// test regexp_substr(expr, pat)
	testParam2 := []struct {
		input    any // string
		pattern  any // string
		match    any // string
		matchBin any // bin result
		err      error
	}{
		{"abc", "bc", "bc", "0x6263", nil},
		{"你好", "好", "好", "0xE5A5BD", nil},
		{"abc", nil, nil, nil, nil},
		{nil, "bc", nil, nil, nil},
		{nil, nil, nil, nil, nil},
		{"a", "", nil, nil, ErrRegexp}, // issue 37988
	}

	for charsetAndCollateTp := 0; charsetAndCollateTp < testCharsetAndCollateTpNum; charsetAndCollateTp++ {
		for _, tt := range testParam2 {
			fc := funcs[ast.RegexpSubstr]
			expectMatch := tt.match
			args := datumsToConstants(types.MakeDatums(tt.input, tt.pattern))
			setCharsetAndCollation(charsetAndCollateTp, args[0].GetType(ctx), args[1].GetType(ctx))
			if charsetAndCollateTp == binaryTpIdx {
				expectMatch = tt.matchBin
			}
			f, err := fc.getFunction(ctx, args)
			require.NoError(t, err)

			actualMatch, err := evalBuiltinFunc(f, ctx, chunk.Row{})
			if tt.err == nil {
				require.NoError(t, err)
				testutil.DatumEqual(t, types.NewDatum(expectMatch), actualMatch, fmt.Sprintf("%v", tt))
			} else {
				require.True(t, terror.ErrorEqual(err, tt.err))
			}
		}
	}

	// // test regexp_substr(expr, pat, pos)
	testParam3 := []struct {
		input    any // string
		pattern  any // string
		pos      any // int64
		match    any // string
		matchBin any // bin result
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

	for charsetAndCollateTp := 0; charsetAndCollateTp < testCharsetAndCollateTpNum; charsetAndCollateTp++ {
		for _, tt := range testParam3 {
			fc := funcs[ast.RegexpSubstr]
			expectMatch := tt.match
			args := datumsToConstants(types.MakeDatums(tt.input, tt.pattern, tt.pos))
			setCharsetAndCollation(charsetAndCollateTp, args[0].GetType(ctx), args[1].GetType(ctx))
			if charsetAndCollateTp == binaryTpIdx {
				expectMatch = tt.matchBin
			}
			f, err := fc.getFunction(ctx, args)
			require.NoError(t, err)

			actualMatch, err := evalBuiltinFunc(f, ctx, chunk.Row{})
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
		input    any // string
		pattern  any // string
		pos      any // int64
		occur    any // int64
		match    any // string
		matchBin any // bin result
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

	for charsetAndCollateTp := 0; charsetAndCollateTp < testCharsetAndCollateTpNum; charsetAndCollateTp++ {
		for _, tt := range testParam4 {
			fc := funcs[ast.RegexpSubstr]
			expectMatch := tt.match
			args := datumsToConstants(types.MakeDatums(tt.input, tt.pattern, tt.pos, tt.occur))
			setCharsetAndCollation(charsetAndCollateTp, args[0].GetType(ctx), args[1].GetType(ctx))
			if charsetAndCollateTp == binaryTpIdx {
				expectMatch = tt.matchBin
			}
			f, err := fc.getFunction(ctx, args)
			require.NoError(t, err)

			actualMatch, err := evalBuiltinFunc(f, ctx, chunk.Row{})
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
		input     any // string
		pattern   any // string
		pos       any // int64
		occur     any // int64
		matchType any // string
		match     any // string
		matchBin  any // bin result
		err       error
	}{
		{"abc", "ab.", int64(1), int64(1), "", "abc", "0x616263", nil},
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

	for charsetAndCollateTp := 0; charsetAndCollateTp < testCharsetAndCollateTpNum; charsetAndCollateTp++ {
		for _, tt := range testParam5 {
			fc := funcs[ast.RegexpSubstr]
			expectMatch := tt.match
			args := datumsToConstants(types.MakeDatums(tt.input, tt.pattern, tt.pos, tt.occur, tt.matchType))
			setCharsetAndCollation(charsetAndCollateTp, args[0].GetType(ctx), args[1].GetType(ctx))
			if charsetAndCollateTp == binaryTpIdx {
				expectMatch = tt.matchBin
			}
			f, err := fc.getFunction(ctx, args)
			require.NoError(t, err)

			actualMatch, err := evalBuiltinFunc(f, ctx, chunk.Row{})
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

	args := make([]any, 0)
	args = append(args, any(expr))
	args = append(args, any(pattern))
	args = append(args, any(position))
	args = append(args, any(occurrence))
	args = append(args, any(matchType))

	constants := make([]*Constant, 5)
	for i := 0; i < 5; i++ {
		constants[i] = nil
	}

	cases := make([]vecExprBenchCase, 0, 50)

	cases = append(cases, getVecExprBenchCaseForRegexp(types.ETString, false, args...))
	cases = append(cases, getVecExprBenchCaseForRegexp(types.ETString, false, args...))

	// Prepare data: expr is constant
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETString, false, false, map[int]any{0: any("好的 好滴 好~")}, len(args), constants, args...))
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETString, false, false, map[int]any{0: any("好的 好滴 好~")}, len(args), constants, args...))
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETString, false, true, map[int]any{0: any("好的 好滴 好~")}, len(args), constants, args...))
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETString, false, true, map[int]any{0: any("好的 好滴 好~")}, len(args), constants, args...)) // index 5

	// Prepare data: pattern is constant
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETString, false, false, map[int]any{1: any("aB.")}, len(args), constants, args...))
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETString, false, false, map[int]any{1: any("aB.")}, len(args), constants, args...))
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETString, false, true, map[int]any{1: any("aB.")}, len(args), constants, args...))
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETString, false, true, map[int]any{1: any("aB.")}, len(args), constants, args...))

	// Prepare data: position is constant
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETString, false, false, map[int]any{2: any(int64(2))}, len(args), constants, args...)) // index 10
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETString, false, false, map[int]any{2: any(int64(2))}, len(args), constants, args...))
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETString, false, true, map[int]any{2: any(int64(2))}, len(args), constants, args...))
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETString, false, true, map[int]any{2: any(int64(2))}, len(args), constants, args...))

	// Prepare data: occurrence is constant
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETString, false, false, map[int]any{3: any(int64(2))}, len(args), constants, args...))
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETString, false, false, map[int]any{3: any(int64(2))}, len(args), constants, args...)) // index 15
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETString, false, true, map[int]any{3: any(int64(2))}, len(args), constants, args...))
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETString, false, true, map[int]any{3: any(int64(2))}, len(args), constants, args...))

	// Prepare data: match type is constant
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETString, false, false, map[int]any{4: any("msi")}, len(args), constants, args...))
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETString, false, false, map[int]any{4: any("msi")}, len(args), constants, args...))
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETString, false, true, map[int]any{4: any("msi")}, len(args), constants, args...)) // index 20
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETString, false, true, map[int]any{4: any("msi")}, len(args), constants, args...))

	// Prepare data: test memorization
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETString, false, false, map[int]any{1: any("aB."), 4: any("msi")}, len(args), constants, args...))
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETString, false, false, map[int]any{1: any("aB.")}, len(args)-1, constants, args...))

	// Build vecBuiltinRegexpSubstrCases
	var vecBuiltinRegexpSubstrCases = map[string][]vecExprBenchCase{
		ast.RegexpSubstr: cases,
	}

	testVectorizedBuiltinFunc(t, vecBuiltinRegexpSubstrCases)
}

func TestRegexpInStr(t *testing.T) {
	ctx := createContext(t)

	// test regexp_instr(expr, pat)
	testParam2 := []struct {
		input    any // string
		pattern  any // string
		match    any // int64
		matchBin any // bin result
		err      error
	}{
		{"abc", "bc", int64(2), int64(2), nil},
		{"你好", "好", int64(2), int64(4), nil},
		{"", "^$", int64(1), int64(1), nil},
		{"abc", nil, nil, nil, nil},
		{nil, "bc", nil, nil, nil},
		{nil, nil, nil, nil, nil},
		{"a", "", nil, nil, ErrRegexp}, // issue 37988
	}

	for charsetAndCollateTp := 0; charsetAndCollateTp < testCharsetAndCollateTpNum; charsetAndCollateTp++ {
		for _, tt := range testParam2 {
			fc := funcs[ast.RegexpInStr]
			expectMatch := tt.match
			args := datumsToConstants(types.MakeDatums(tt.input, tt.pattern))
			setCharsetAndCollation(charsetAndCollateTp, args[0].GetType(ctx), args[1].GetType(ctx))
			if charsetAndCollateTp == binaryTpIdx {
				expectMatch = tt.matchBin
			}
			f, err := fc.getFunction(ctx, args)
			require.NoError(t, err)

			actualMatch, err := evalBuiltinFunc(f, ctx, chunk.Row{})
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
		input    any // string
		pattern  any // string
		pos      any // int64
		match    any // int64
		matchBin any // bin result
		err      error
	}{
		{"abc", "bc", int64(2), int64(2), int64(2), nil},
		{"你好", "好", int64(2), int64(2), int64(4), nil},
		{"abc", "bc", int64(3), int64(0), int64(0), nil},
		{"你好啊", "好", int64(3), int64(0), int64(4), nil},
		{"", "^$", int64(1), 1, 1, nil},
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

	for charsetAndCollateTp := 0; charsetAndCollateTp < testCharsetAndCollateTpNum; charsetAndCollateTp++ {
		for _, tt := range testParam3 {
			fc := funcs[ast.RegexpInStr]
			expectMatch := tt.match
			args := datumsToConstants(types.MakeDatums(tt.input, tt.pattern, tt.pos))
			setCharsetAndCollation(charsetAndCollateTp, args[0].GetType(ctx), args[1].GetType(ctx))
			if charsetAndCollateTp == binaryTpIdx {
				expectMatch = tt.matchBin
			}
			f, err := fc.getFunction(ctx, args)
			require.NoError(t, err)

			actualMatch, err := evalBuiltinFunc(f, ctx, chunk.Row{})
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
		input      any // string
		pattern    any // string
		pos        any // int64
		occurrence any // int64
		match      any // int64
		matchBin   any // bin result
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

	for charsetAndCollateTp := 0; charsetAndCollateTp < testCharsetAndCollateTpNum; charsetAndCollateTp++ {
		for _, tt := range testParam4 {
			fc := funcs[ast.RegexpInStr]
			expectMatch := tt.match
			args := datumsToConstants(types.MakeDatums(tt.input, tt.pattern, tt.pos, tt.occurrence))
			setCharsetAndCollation(charsetAndCollateTp, args[0].GetType(ctx), args[1].GetType(ctx))
			if charsetAndCollateTp == binaryTpIdx {
				expectMatch = tt.matchBin
			}
			f, err := fc.getFunction(ctx, args)
			require.NoError(t, err)

			actualMatch, err := evalBuiltinFunc(f, ctx, chunk.Row{})
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
		input      any // string
		pattern    any // string
		pos        any // int64
		occurrence any // int64
		retOpt     any // int64
		match      any // int64
		matchBin   any // bin result
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

	for charsetAndCollateTp := 0; charsetAndCollateTp < testCharsetAndCollateTpNum; charsetAndCollateTp++ {
		for _, tt := range testParam5 {
			fc := funcs[ast.RegexpInStr]
			expectMatch := tt.match
			args := datumsToConstants(types.MakeDatums(tt.input, tt.pattern, tt.pos, tt.occurrence, tt.retOpt))
			setCharsetAndCollation(charsetAndCollateTp, args[0].GetType(ctx), args[1].GetType(ctx))
			if charsetAndCollateTp == binaryTpIdx {
				expectMatch = tt.matchBin
			}
			f, err := fc.getFunction(ctx, args)
			require.NoError(t, err)

			actualMatch, err := evalBuiltinFunc(f, ctx, chunk.Row{})
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
		input      any // string
		pattern    any // string
		pos        any // int64
		occurrence any // int64
		retOpt     any // int64
		matchType  any // string
		match      any // int64
		matchBin   any // bin result
		err        error
	}{
		{"abc", "ab.", int64(1), int64(1), int64(0), "", 1, 1, nil},
		{"abc", "aB.", int64(1), int64(1), int64(0), "i", 1, 1, nil},
		{"good\nday", "od$", int64(1), int64(1), int64(0), "m", 3, 3, nil},
		{"good\nday", "oD$", int64(1), int64(1), int64(0), "mi", 3, 3, nil},
		{"\n", ".", int64(1), int64(1), int64(0), "s", 1, 1, nil}, // index 4
		// Test invalid matchType
		{"abc", "ab.", int64(1), int64(1), int64(0), "p", nil, nil, ErrRegexp},
		// Some nullable input tests
		{"abc", "ab.", int64(1), int64(1), int64(0), nil, nil, nil, nil},
		{"abc", "ab.", nil, int64(1), int64(0), nil, nil, nil, nil},
		{nil, "ab.", nil, int64(1), int64(0), nil, nil, nil, nil},
	}

	for charsetAndCollateTp := 0; charsetAndCollateTp < testCharsetAndCollateTpNum; charsetAndCollateTp++ {
		for _, tt := range testParam6 {
			fc := funcs[ast.RegexpInStr]
			expectMatch := tt.match
			args := datumsToConstants(types.MakeDatums(tt.input, tt.pattern, tt.pos, tt.occurrence, tt.retOpt, tt.matchType))
			setCharsetAndCollation(charsetAndCollateTp, args[0].GetType(ctx), args[1].GetType(ctx))
			if charsetAndCollateTp == binaryTpIdx {
				expectMatch = tt.matchBin
			}
			f, err := fc.getFunction(ctx, args)
			require.NoError(t, err)

			actualMatch, err := evalBuiltinFunc(f, ctx, chunk.Row{})
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

	args := make([]any, 0)
	args = append(args, any(expr))
	args = append(args, any(pattern))
	args = append(args, any(position))
	args = append(args, any(occurrence))
	args = append(args, any(retOpt))
	args = append(args, any(matchType))

	constants := make([]*Constant, 6)
	for i := 0; i < 6; i++ {
		constants[i] = nil
	}

	cases := make([]vecExprBenchCase, 0, 50)

	cases = append(cases, getVecExprBenchCaseForRegexp(types.ETInt, false, args...))
	cases = append(cases, getVecExprBenchCaseForRegexp(types.ETInt, false, args...))

	// Prepare data: expr is constant
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETInt, false, false, map[int]any{0: any("好的 好滴 好~")}, len(args), constants, args...))
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETInt, false, false, map[int]any{0: any("好的 好滴 好~")}, len(args), constants, args...))
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETInt, false, true, map[int]any{0: any("好的 好滴 好~")}, len(args), constants, args...))
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETInt, false, true, map[int]any{0: any("好的 好滴 好~")}, len(args), constants, args...))

	// Prepare data: pattern is constant
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETInt, false, false, map[int]any{1: any("aB.")}, len(args), constants, args...))
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETInt, false, false, map[int]any{1: any("aB.")}, len(args), constants, args...))
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETInt, false, true, map[int]any{1: any("aB.")}, len(args), constants, args...))
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETInt, false, true, map[int]any{1: any("aB.")}, len(args), constants, args...))

	// Prepare data: position is constant
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETInt, false, false, map[int]any{2: any(int64(2))}, len(args), constants, args...))
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETInt, false, false, map[int]any{2: any(int64(2))}, len(args), constants, args...))
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETInt, false, true, map[int]any{2: any(int64(2))}, len(args), constants, args...))
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETInt, false, true, map[int]any{2: any(int64(2))}, len(args), constants, args...))

	// Prepare data: occurrence is constant
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETInt, false, false, map[int]any{3: any(int64(2))}, len(args), constants, args...))
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETInt, false, false, map[int]any{3: any(int64(2))}, len(args), constants, args...))
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETInt, false, true, map[int]any{3: any(int64(2))}, len(args), constants, args...))
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETInt, false, true, map[int]any{3: any(int64(2))}, len(args), constants, args...))

	// Prepare data: return_option is constant
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETInt, false, false, map[int]any{4: any(int64(1))}, len(args), constants, args...))
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETInt, false, false, map[int]any{4: any(int64(1))}, len(args), constants, args...))
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETInt, false, true, map[int]any{4: any(int64(1))}, len(args), constants, args...))
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETInt, false, true, map[int]any{4: any(int64(1))}, len(args), constants, args...))

	// Prepare data: match type is constant
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETInt, false, false, map[int]any{5: any("msi")}, len(args), constants, args...))
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETInt, false, false, map[int]any{5: any("msi")}, len(args), constants, args...))
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETInt, false, true, map[int]any{5: any("msi")}, len(args), constants, args...))
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETInt, false, true, map[int]any{5: any("msi")}, len(args), constants, args...))

	// Prepare data: test memorization
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETInt, false, false, map[int]any{1: any("aB."), 5: any("msi")}, len(args), constants, args...))
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETInt, false, false, map[int]any{1: any("aB.")}, len(args)-1, constants, args...))

	// Build vecBuiltinRegexpSubstrCases
	var vecBuiltinRegexpInStrCases = map[string][]vecExprBenchCase{
		ast.RegexpInStr: cases,
	}

	testVectorizedBuiltinFunc(t, vecBuiltinRegexpInStrCases)
}

func TestRegexpReplace(t *testing.T) {
	ctx := createContext(t)

	url1 := "https://go.mail/folder-1/online/ru-en/#lingvo/#1О 50000&price_ashka/rav4/page=/check.xml"
	url2 := "http://saint-peters-total=меньше 1000-rublyayusche/catalogue/kolasuryat-v-2-kadyirovka-personal/serial_id=0&input_state/apartments/mokrotochki.net/upravda.ru/yandex.ru/GameMain.aspx?mult]/on/orders/50195&text=мыс и орелка в Балаш смотреть онлайн бесплатно в хорошем камбалакс&lr=20030393833539353862643188&op_promo=C-Teaser_id=06d162.html"

	url1Repl := "a\\12\\13"
	url1Res := "ago.mail2go.mail3"
	url1BinRes := "0x61676F2E6D61696C32676F2E6D61696C33"

	url2Repl := "aaa\\1233"
	url2Res := "aaasaint-peters-total=меньше 1000-rublyayusche233"
	url2BinRes := "0x6161617361696E742D7065746572732D746F74616C3DC390C2BCC390C2B5C390C2BDC391C592C391CB86C390C2B520313030302D7275626C7961797573636865323333"

	urlPat := "^https?://(?:www\\.)?([^/]+)/.*$"

	// test regexp_replace(expr, pat, repl)
	testParam3 := []struct {
		input    any // string
		pattern  any // string
		replace  any // string
		match    any // string
		matchBin any // bin result
		err      error
	}{
		{"abc abd abe", "ab.", "cz", "cz cz cz", "0x637A20637A20637A", nil},
		{"你好 好的", "好", "逸", "你逸 逸的", "0xE4BDA0E980B820E980B8E79A84", nil},
		{"", "^$", "123", "123", "0x313233", nil},
		{"stackoverflow", "(.{5})(.*)", `\\+\2+\1+\2+\1\`, `\+overflow+stack+overflow+stack`, "0x5C2B6F766572666C6F772B737461636B2B6F766572666C6F772B737461636B", nil},
		{"fooabcdefghij fooABCDEFGHIJ", "foo(.)(.)(.)(.)(.)(.)(.)(.)(.)(.)", `\\\9\\\8-\7\\\6-\5\\\4-\3\\\2-\1\\`, `\i\h-g\f-e\d-c\b-a\ \I\H-G\F-E\D-C\B-A\`, "0x5C395C382D375C362D355C342D335C322D315C205C395C382D375C362D355C342D335C322D315C", nil},
		{"fool food foo", "foo(.?)", `\0+\1`, "fool+l food+d foo+", "0x5C302B5C31205C302B5C31205C302B5C31", nil},
		{url1, urlPat, url1Repl, url1Res, url1BinRes, nil},
		{url2, urlPat, url2Repl, url2Res, url2BinRes, nil},
		{"abc", nil, nil, nil, nil, nil},
		{nil, "bc", nil, nil, nil, nil},
		{nil, nil, nil, nil, nil, nil},
		{"abc", "\\d*", "d", "dadbdcd", "0x64616462646364", nil},
		{"abc", "\\d*$", "d", "abcd", "0x64616462646364", nil},
		{"我们", "\\d*", "d", "d我d们d", "0x64C3A664CB8664E2809864C3A464C2BB64C2AC64", nil},
		{"a", "", "a", nil, nil, ErrRegexp}, // issue 37988
	}

	for charsetAndCollateTp := 0; charsetAndCollateTp < testCharsetAndCollateTpNum; charsetAndCollateTp++ {
		for _, tt := range testParam3 {
			fc := funcs[ast.RegexpReplace]
			expectMatch := tt.match
			args := datumsToConstants(types.MakeDatums(tt.input, tt.pattern, tt.replace))
			setCharsetAndCollation(charsetAndCollateTp, args[0].GetType(ctx), args[1].GetType(ctx), args[2].GetType(ctx))
			if charsetAndCollateTp == binaryTpIdx {
				expectMatch = tt.matchBin
			}
			f, err := fc.getFunction(ctx, args)
			require.NoError(t, err)

			actualMatch, err := evalBuiltinFunc(f, ctx, chunk.Row{})
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
		input    any // string
		pattern  any // string
		replace  any // string
		pos      any // int64
		match    any // string
		matchBin any // bin result
		err      error
	}{
		{"abc", "ab.", "cc", int64(1), "cc", "0x6363", nil},
		{"abc", "bc", "cc", int64(3), "abc", "0x616263", nil},
		{"你好", "好", "的", int64(2), "你的", "0xE4BDA0E79A84", nil},
		{"你好啊", "好", "的", int64(3), "你好啊", "0xE4BDA0E79A84E5958A", nil},
		{"", "^$", "cc", int64(1), "cc", "0x6363", nil},
		{"seafood fool", "foo(.?)", "123", int64(3), "sea123 123", "0x73656131323320313233", nil}, // index 5
		{"seafood fool", "foo(.?)", "123", int64(5), "seafood 123", "0x736561666F6F6420313233", nil},
		{"seafood fool", "foo(.?)", "123", int64(10), "seafood fool", "0x736561666F6F6420666F6F6C", nil},
		{"seafood fool", "foo(.?)", "z\\12", int64(3), "seazd2 zl2", "0x7365617A6432207A6C32", nil},
		{"seafood fool", "foo(.?)", "z\\12", int64(5), "seafood zl2", "0x736561666F6F64207A6C32", nil},
		// Invalid position index tests
		{"", "^$", "a", int64(2), "", "", ErrRegexp}, // index 10
		{"", "^&", "a", int64(0), "", "", ErrRegexp},
		{"abc", "bc", "a", int64(-1), "", "", ErrRegexp},
		{"abc", "bc", "a", int64(4), "", "", ErrRegexp},
		// Some nullable input tests
		{"", "^$", "a", nil, nil, nil, nil},
		{nil, "^$", "a", nil, nil, nil, nil}, // index 15
		{"", nil, nil, nil, nil, nil, nil},
		{nil, nil, nil, int64(1), nil, nil, nil},
		{nil, nil, nil, nil, nil, nil, nil},
	}

	for charsetAndCollateTp := 0; charsetAndCollateTp < testCharsetAndCollateTpNum; charsetAndCollateTp++ {
		for _, tt := range testParam4 {
			fc := funcs[ast.RegexpReplace]
			expectMatch := tt.match
			args := datumsToConstants(types.MakeDatums(tt.input, tt.pattern, tt.replace, tt.pos))
			setCharsetAndCollation(charsetAndCollateTp, args[0].GetType(ctx), args[1].GetType(ctx), args[2].GetType(ctx))
			if charsetAndCollateTp == binaryTpIdx {
				expectMatch = tt.matchBin
			}
			f, err := fc.getFunction(ctx, args)
			require.NoError(t, err)

			actualMatch, err := evalBuiltinFunc(f, ctx, chunk.Row{})
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
		input      any // string
		pattern    any // string
		replace    any // string
		pos        any // int64
		occurrence any // int64
		match      any // string
		matchBin   any // bin result
		err        error
	}{
		{"abc abd", "ab.", "cc", int64(1), int64(1), "cc abd", "0x636320616264", nil},
		{"abc abd", "ab.", "cc", int64(1), int64(2), "abc cc", "0x616263206363", nil},
		{"abc abd", "ab.", "cc", int64(1), int64(0), "cc cc", "0x6363206363", nil},
		{"abc abd abe", "ab.", "cc", int64(3), int64(2), "abc abd cc", "0x61626320616264206363", nil},
		{"abc abd abe", "ab.", "cc", int64(3), int64(10), "abc abd abe", "0x6162632061626420616265", nil},
		{"你好 好啊", "好", "的", int64(1), int64(1), "你的 好啊", "0xE4BDA0E79A8420E5A5BDE5958A", nil}, // index 5
		{"你好 好啊", "好", "的", int64(3), int64(1), "你好 的啊", "0xE4BDA0E79A8420E5A5BDE5958A", nil},
		{"seafood fool", "foo(.?)", "123", int64(1), int(1), "sea123 fool", "0x73656131323320666F6F6C", nil},
		{"seafood fool", "foo(.?)", "123", int64(1), int(2), "seafood 123", "0x736561666F6F6420313233", nil},
		{"seafood fool", "foo(.?)", "123", int64(1), int(10), "seafood fool", "0x736561666F6F6420666F6F6C", nil},
		{"seafood fool", "foo(.?)", "z\\12", int64(1), int(1), "seazd2 fool", "0x7365617A643220666F6F6C", nil}, // index 10
		{"seafood fool", "foo(.?)", "z\\12", int64(1), int(2), "seafood zl2", "0x736561666F6F64207A6C32", nil},
		{"", "^$", "cc", int64(1), int64(1), "cc", "0x6363", nil},
		{"", "^$", "cc", int64(1), int64(2), "", "0x", nil},
		{"", "^$", "cc", int64(1), int64(-1), "cc", "0x6363", nil},
		{"abc", "\\d*", "p", 1, 2, "apbc", "0x61706263", nil}, // index 15
		{"abc", "\\d*$", "p", 1, 1, "abcp", "0x61626370", nil},
		{"我们", "\\d*", "p", 1, 2, "我p们", "0xC3A670CB86E28098C3A4C2BBC2AC", nil},
		// Some nullable input tests
		{"", "^$", "a", nil, int64(1), nil, nil, nil},
		{nil, "^$", "a", nil, nil, nil, nil, nil},
		{"", nil, nil, nil, int64(1), nil, nil, nil}, // index 20
		{nil, nil, nil, int64(1), int64(1), nil, nil, nil},
		{nil, nil, nil, nil, nil, nil, nil, nil},
	}

	for charsetAndCollateTp := 0; charsetAndCollateTp < testCharsetAndCollateTpNum; charsetAndCollateTp++ {
		for _, tt := range testParam5 {
			fc := funcs[ast.RegexpReplace]
			expectMatch := tt.match
			args := datumsToConstants(types.MakeDatums(tt.input, tt.pattern, tt.replace, tt.pos, tt.occurrence))
			setCharsetAndCollation(charsetAndCollateTp, args[0].GetType(ctx), args[1].GetType(ctx), args[2].GetType(ctx))
			if charsetAndCollateTp == binaryTpIdx {
				expectMatch = tt.matchBin
			}
			f, err := fc.getFunction(ctx, args)
			require.NoError(t, err)

			actualMatch, err := evalBuiltinFunc(f, ctx, chunk.Row{})
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
		input      any // string
		pattern    any // string
		replace    any // string
		pos        any // int64
		occurrence any // int64
		matchType  any // string
		match      any // string
		matchBin   any // bin result
		err        error
	}{
		{"abc", "ab.", "cc", int64(1), int64(0), "", "cc", "0x6363", nil},
		{"abc", "aB.", "cc", int64(1), int64(0), "i", "cc", "0x6363", nil},
		{"good\nday", "od$", "cc", int64(1), int64(0), "m", "gocc\nday", "0x676F63630A646179", nil},
		{"good\nday", "oD$", "cc", int64(1), int64(0), "mi", "gocc\nday", "0x676F63630A646179", nil},
		{"Good\nday", "a(B)", "a\\12", int64(2), int64(0), "msi", "Good\nday", "0x476F6F640A646179", nil},
		{"Good\nday", "(.)", "cc", int64(1), int64(3), "ci", "Goccd\nday", "0x476F6363640A646179", nil},
		{"seafood fool", "foo(.?)", "的", int64(1), int64(2), "m", "seafood 的", "0x736561666F6F6420C3A7C5A1E2809E", nil},
		{"abc abd abe", "(.)", "cc", int64(4), int64(1), "cii", "abcccabd abe", "0x616263636361626420616265", nil},
		{"\n", ".", "cc", int64(1), int64(0), "s", "cc", "0x6363", nil},
		{"好的 好滴 好~", ".", "的", int64(1), int64(0), "msi", "的的的的的的的的", "0xE79A84E79A84E79A84E79A84E79A84E79A84E79A84E79A84", nil},
		// Test invalid matchType
		{"abc", "ab.", "cc", int64(1), int64(0), "p", nil, nil, ErrRegexp},
		// Some nullable input tests
		{"abc", "ab.", "cc", int64(1), int64(0), nil, nil, nil, nil},
		{"abc", "ab.", nil, int64(1), int64(0), nil, nil, nil, nil},
		{nil, "ab.", nil, int64(1), int64(0), nil, nil, nil, nil},
	}

	for charsetAndCollateTp := 0; charsetAndCollateTp < testCharsetAndCollateTpNum; charsetAndCollateTp++ {
		for _, tt := range testParam6 {
			fc := funcs[ast.RegexpReplace]
			expectMatch := tt.match
			args := datumsToConstants(types.MakeDatums(tt.input, tt.pattern, tt.replace, tt.pos, tt.occurrence, tt.matchType))
			setCharsetAndCollation(charsetAndCollateTp, args[0].GetType(ctx), args[1].GetType(ctx), args[2].GetType(ctx))
			if charsetAndCollateTp == binaryTpIdx {
				expectMatch = tt.matchBin
			}
			f, err := fc.getFunction(ctx, args)
			require.NoError(t, err)

			actualMatch, err := evalBuiltinFunc(f, ctx, chunk.Row{})
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
	var expr []string = []string{"abc abd abe", "你好啊啊啊啊啊", "好的 好滴 好~", "Good\nday", "seafood fool"} // , "\n\n\n\n\n\n"
	var pattern []string = []string{"(^$)", "(a)b.", "a(B).", "(ab)c", "(好)", "(好).", "(o)d$", "^da(y)", "(d)ay$", "(.)", "foo(.?)", "foo(d|l)"}
	var repl []string = []string{"cc", "的", "a\\12"}
	var position []int = []int{1, 5}
	var occurrence []int = []int{-1, 5}
	var matchType []string = []string{"m", "i", "icc", "cii", "s", "msi"}

	args := make([]any, 0)
	args = append(args, any(expr))
	args = append(args, any(pattern))
	args = append(args, any(repl))
	args = append(args, any(position))
	args = append(args, any(occurrence))
	args = append(args, any(matchType))

	constants := make([]*Constant, 6)
	for i := 0; i < 6; i++ {
		constants[i] = nil
	}

	cases := make([]vecExprBenchCase, 0, 50)

	cases = append(cases, getVecExprBenchCaseForRegexp(types.ETString, false, args...))
	cases = append(cases, getVecExprBenchCaseForRegexp(types.ETString, false, args...))

	// Prepare data: expr is constant
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETString, false, false, map[int]any{0: any("好的 好滴 好~")}, len(args), constants, args...))
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETString, false, false, map[int]any{0: any("好的 好滴 好~")}, len(args), constants, args...))
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETString, false, true, map[int]any{0: any("好的 好滴 好~")}, len(args), constants, args...))
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETString, false, true, map[int]any{0: any("好的 好滴 好~")}, len(args), constants, args...)) // index 5

	// Prepare data: pattern is constant
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETString, false, false, map[int]any{1: any("(a)B.")}, len(args), constants, args...))
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETString, false, false, map[int]any{1: any("(a)B.")}, len(args), constants, args...))
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETString, false, true, map[int]any{1: any("(a)B.")}, len(args), constants, args...))
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETString, false, true, map[int]any{1: any("(a)B.")}, len(args), constants, args...))

	// Prepare data: repl is constant
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETString, false, false, map[int]any{2: any("cc")}, len(args), constants, args...)) // index 10
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETString, false, false, map[int]any{2: any("cc")}, len(args), constants, args...))
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETString, false, true, map[int]any{2: any("cc")}, len(args), constants, args...))
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETString, false, true, map[int]any{2: any("cc")}, len(args), constants, args...))

	// Prepare data: position is constant
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETString, false, false, map[int]any{3: any(int64(2))}, len(args), constants, args...))
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETString, false, false, map[int]any{3: any(int64(2))}, len(args), constants, args...)) // index 15
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETString, false, true, map[int]any{3: any(int64(2))}, len(args), constants, args...))
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETString, false, true, map[int]any{3: any(int64(2))}, len(args), constants, args...))

	// Prepare data: occurrence is constant
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETString, false, false, map[int]any{4: any(int64(2))}, len(args), constants, args...))
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETString, false, false, map[int]any{4: any(int64(2))}, len(args), constants, args...))
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETString, false, true, map[int]any{4: any(int64(2))}, len(args), constants, args...)) // index 20
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETString, false, true, map[int]any{4: any(int64(2))}, len(args), constants, args...))

	// Prepare data: match type is constant
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETString, false, false, map[int]any{5: any("msi")}, len(args), constants, args...))
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETString, false, false, map[int]any{5: any("msi")}, len(args), constants, args...))
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETString, false, true, map[int]any{5: any("msi")}, len(args), constants, args...))
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETString, false, true, map[int]any{5: any("msi")}, len(args), constants, args...)) // index 25

	// Prepare data: test memorization
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETString, false, false, map[int]any{1: any("a(B)."), 5: any("msi")}, len(args), constants, args...))
	cases = append(cases, getVecExprBenchCaseForRegexpIncludeConst(types.ETString, false, false, map[int]any{1: any("a(B).")}, len(args)-1, constants, args...))

	// Build vecBuiltinRegexpSubstrCases
	var vecBuiltinRegexpReplaceCases = map[string][]vecExprBenchCase{
		ast.RegexpReplace: cases,
	}

	testVectorizedBuiltinFunc(t, vecBuiltinRegexpReplaceCases)
}

func TestRegexpCache(t *testing.T) {
	ctx := createContext(t)

	// if the pattern or match type is not constant, it should not be cached
	sig := regexpBaseFuncSig{}
	sig.args = []Expression{&Column{}, &Column{}, &Constant{}}
	reg, err := sig.getRegexp(ctx, "abc", "", 2)
	require.NoError(t, err)
	require.Equal(t, "abc", reg.String())

	reg, err = sig.getRegexp(ctx, "def", "", 2)
	require.NoError(t, err)
	require.Equal(t, "def", reg.String())

	reg, ok, err := sig.tryVecMemorizedRegexp(ctx, []*funcParam{
		{defaultStrVal: "x"},
		{defaultStrVal: "aaa"},
		{defaultStrVal: ""},
	}, 2, 1)
	require.Nil(t, reg)
	require.False(t, ok)
	require.NoError(t, err)

	_, ok = sig.memorizedRegexp.getCache(ctx.GetSessionVars().StmtCtx.CtxID())
	require.False(t, ok)

	sig.args = []Expression{&Column{}, &Constant{}, &Column{}}
	reg, err = sig.getRegexp(ctx, "bbb", "", 2)
	require.NoError(t, err)
	require.Equal(t, "bbb", reg.String())

	reg, ok, err = sig.tryVecMemorizedRegexp(ctx, []*funcParam{
		{defaultStrVal: "x"},
		{defaultStrVal: "aaa"},
		{defaultStrVal: ""},
	}, 2, 1)
	require.Nil(t, reg)
	require.False(t, ok)
	require.NoError(t, err)

	_, ok = sig.memorizedRegexp.getCache(ctx.GetSessionVars().StmtCtx.CtxID())
	require.False(t, ok)

	// if pattern and match type are both constant, it should be cached
	sig = regexpBaseFuncSig{}
	sig.args = []Expression{&Column{}, &Constant{ParamMarker: &ParamMarker{}}, &Constant{ParamMarker: &ParamMarker{}}}
	reg, err = sig.getRegexp(ctx, "ccc", "", 2)
	require.NoError(t, err)
	require.Equal(t, "ccc", reg.String())

	reg2, err := sig.getRegexp(ctx, "ddd", "", 2)
	require.NoError(t, err)
	require.Same(t, reg, reg2)
	require.Equal(t, "ccc", reg2.String())

	sig = regexpBaseFuncSig{}
	sig.args = []Expression{&Column{}, &Constant{ParamMarker: &ParamMarker{}}, &Constant{ParamMarker: &ParamMarker{}}}
	reg, ok, err = sig.tryVecMemorizedRegexp(ctx, []*funcParam{
		{defaultStrVal: "x"},
		{defaultStrVal: "ddd"},
		{defaultStrVal: ""},
	}, 2, 1)
	require.Equal(t, "ddd", reg.String())
	require.True(t, ok)
	require.NoError(t, err)

	reg2, ok, err = sig.tryVecMemorizedRegexp(ctx, []*funcParam{
		{defaultStrVal: "x"},
		{defaultStrVal: "eee"},
		{defaultStrVal: ""},
	}, 2, 1)
	require.Same(t, reg, reg2)
	require.Equal(t, "ddd", reg2.String())
	require.True(t, ok)
	require.NoError(t, err)
}
