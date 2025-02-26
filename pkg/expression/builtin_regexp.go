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
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/set"
	"github.com/pingcap/tidb/pkg/util/stringutil"
	"github.com/pingcap/tipb/go-tipb"
)

type empty struct{}

const patternIdx = 1
const replacementIdx = 2
const regexpLikeMatchTypeIdx = 2
const regexpSubstrMatchTypeIdx = 4
const regexpInstrMatchTypeIdx = 5
const regexpReplaceMatchTypeIdx = 5

// Valid flags in match type
const (
	flagI = "i"
	flagC = "c"
	flagM = "m"
	flagS = "s"
)

const (
	invalidMatchType    = "Invalid match type"
	invalidIndex        = "Index out of bounds in regular expression search"
	invalidReturnOption = "Incorrect arguments to regexp_instr: return_option must be 1 or 0"
	invalidSubstitution = "Substitution number is out of range"
	binaryCollateErr    = "Not support binary collation so far"
	emptyPatternErr     = "Empty pattern is invalid"
)

var validMatchType = set.NewStringSet(
	flagI, // Case-insensitive matching
	flagC, // Case-sensitive matching
	flagM, // Multiple-line mode
	flagS, // The . character matches line terminators
)

type regexpBaseFuncSig struct {
	baseBuiltinFunc
	memorizedRegexp builtinFuncCache[regexpMemorizedSig]
}

// check binary collation, not xxx_bin collation!
func (re *regexpBaseFuncSig) isBinaryCollation() bool {
	return re.collation == charset.CollationBin && re.charset == charset.CharsetBin
}

func (re *regexpBaseFuncSig) clone() *regexpBaseFuncSig {
	newSig := &regexpBaseFuncSig{}
	newSig.cloneFrom(&re.baseBuiltinFunc)
	return newSig
}

// we can memorize the regexp when:
//  1. pattern and match type are constant
//  2. pattern is const and there is no match type argument
//
// return true: need, false: needless
func (re *regexpBaseFuncSig) canMemorizeRegexp(matchTypeIdx int) bool {
	// If the pattern and match type are both constants, we can cache the regexp into memory.
	// Notice that the above two arguments are not required to be constant across contexts because the cache is only
	// valid when the two context ids are the same.
	return re.args[patternIdx].ConstLevel() >= ConstOnlyInContext &&
		(len(re.args) <= matchTypeIdx || re.args[matchTypeIdx].ConstLevel() >= ConstOnlyInContext)
}

// buildRegexp builds a new `*regexp.Regexp` from the pattern and matchType
func (re *regexpBaseFuncSig) buildRegexp(pattern string, matchType string) (reg *regexp.Regexp, err error) {
	if len(pattern) == 0 {
		return nil, ErrRegexp.GenWithStackByArgs(emptyPatternErr)
	}

	matchType, err = getRegexpMatchType(matchType, re.collation)
	if err != nil {
		return nil, err
	}

	if len(matchType) == 0 {
		reg, err = regexp.Compile(pattern)
	} else {
		reg, err = regexp.Compile(fmt.Sprintf("(?%s)%s", matchType, pattern))
	}

	if err != nil {
		return nil, ErrRegexp.GenWithStackByArgs(err)
	}

	return reg, nil
}

// getRegexp returns the Regexp which can be used by the current function.
// If the pattern and matchType arguments are both constant, the `*regexp.Regexp` object will be cached in memory.
// The next call of `getRegexp` will return the cached regexp if it is present and the context id is equal
func (re *regexpBaseFuncSig) getRegexp(ctx EvalContext, pattern string, matchType string, matchTypeIdx int) (*regexp.Regexp, error) {
	if !re.canMemorizeRegexp(matchTypeIdx) {
		return re.buildRegexp(pattern, matchType)
	}

	sig, err := re.memorizedRegexp.getOrInitCache(ctx, func() (ret regexpMemorizedSig, err error) {
		ret.memorizedRegexp, ret.memorizedErr = re.buildRegexp(pattern, matchType)
		return
	})

	if err != nil {
		return nil, err
	}

	return sig.memorizedRegexp, sig.memorizedErr
}

func (re *regexpBaseFuncSig) tryVecMemorizedRegexp(ctx EvalContext, params []*funcParam, matchTypeIdx int, nRows int) (*regexp.Regexp, bool, error) {
	// Check memorization
	if nRows == 0 || !re.canMemorizeRegexp(matchTypeIdx) {
		return nil, false, nil
	}

	pattern := params[patternIdx].getStringVal(0)
	if len(pattern) == 0 {
		return nil, false, ErrRegexp.GenWithStackByArgs(emptyPatternErr)
	}

	matchType := params[matchTypeIdx].getStringVal(0)
	sig, err := re.memorizedRegexp.getOrInitCache(ctx, func() (ret regexpMemorizedSig, err error) {
		ret.memorizedRegexp, ret.memorizedErr = re.buildRegexp(pattern, matchType)
		return
	})

	if err != nil {
		return nil, false, err
	}

	return sig.memorizedRegexp, true, sig.memorizedErr
}

// If characters specifying contradictory options are specified
// within match_type, the rightmost one takes precedence.
func getRegexpMatchType(userInputMatchType string, collation string) (string, error) {
	flag := ""
	matchTypeSet := set.NewStringSet()

	if collate.IsCICollation(collation) {
		matchTypeSet.Insert(flagI)
	}

	for _, val := range userInputMatchType {
		c := string(val)

		// Check validation of the flag
		_, err := validMatchType[c]
		if !err {
			return "", ErrRegexp.GenWithStackByArgs(invalidMatchType)
		}

		if c == flagC {
			// re2 is case-sensitive by default, so we only need to delete 'i' flag
			// to enable the case-sensitive for the regexp
			delete(matchTypeSet, flagI)
			continue
		}

		matchTypeSet[c] = empty{} // add this flag
	}

	// generate flag
	for key := range matchTypeSet {
		flag += key
	}

	return flag, nil
}

// https://dev.mysql.com/doc/refman/8.0/en/regexp.html#function_regexp-like
type regexpLikeFunctionClass struct {
	baseFunctionClass
}

func (c *regexpLikeFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	argTp := []types.EvalType{types.ETString, types.ETString}
	if len(args) == 3 {
		argTp = append(argTp, types.ETString)
	}

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, argTp...)
	if err != nil {
		return nil, err
	}

	bf.tp.SetFlen(1)
	sig := builtinRegexpLikeFuncSig{
		regexpBaseFuncSig: regexpBaseFuncSig{baseBuiltinFunc: bf},
	}

	sig.setPbCode(tipb.ScalarFuncSig_RegexpLikeSig)

	return &sig, nil
}

type builtinRegexpLikeFuncSig struct {
	regexpBaseFuncSig
}

func (re *builtinRegexpLikeFuncSig) Clone() builtinFunc {
	newSig := &builtinRegexpLikeFuncSig{}
	newSig.regexpBaseFuncSig = *re.regexpBaseFuncSig.clone()
	return newSig
}

func (re *builtinRegexpLikeFuncSig) vectorized() bool {
	return true
}

func (re *builtinRegexpLikeFuncSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	expr, isNull, err := re.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, true, err
	}

	pat, isNull, err := re.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, true, err
	} else if len(pat) == 0 {
		return 0, true, ErrRegexp.GenWithStackByArgs(emptyPatternErr)
	}

	matchType := ""
	if len(re.args) == 3 {
		matchType, isNull, err = re.args[2].EvalString(ctx, row)
		if isNull || err != nil {
			return 0, true, err
		}
	}

	reg, err := re.getRegexp(ctx, pat, matchType, regexpLikeMatchTypeIdx)
	if err != nil {
		return 0, true, err
	}

	return boolToInt64(reg.MatchString(expr)), false, nil
}

// REGEXP_LIKE(expr, pat[, match_type])
func (re *builtinRegexpLikeFuncSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	params := make([]*funcParam, 0, 3)
	defer releaseBuffers(&re.baseBuiltinFunc, params)

	for i := 0; i < 2; i++ {
		param, isConstNull, err := buildStringParam(ctx, &re.baseBuiltinFunc, i, input, false)
		if err != nil {
			return ErrRegexp.GenWithStackByArgs(err)
		}
		if isConstNull {
			result.ResizeInt64(n, true)
			return nil
		}
		params = append(params, param)
	}

	// user may ignore match type parameter
	hasMatchType := (len(re.args) == 3)
	param, isConstNull, err := buildStringParam(ctx, &re.baseBuiltinFunc, 2, input, !hasMatchType)
	params = append(params, param)
	if err != nil {
		return ErrRegexp.GenWithStackByArgs(err)
	}

	if isConstNull {
		result.ResizeInt64(n, true)
		return nil
	}

	reg, memorized, err := re.tryVecMemorizedRegexp(ctx, params, regexpLikeMatchTypeIdx, n)
	if err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	result.MergeNulls(getBuffers(params)...)
	i64s := result.Int64s()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}

		if !memorized {
			matchType := params[2].getStringVal(i)
			pattern := params[1].getStringVal(i)
			reg, err = re.buildRegexp(pattern, matchType)
			if err != nil {
				return err
			}
		}
		i64s[i] = boolToInt64(reg.MatchString(params[0].getStringVal(i)))
	}
	return nil
}

// https://dev.mysql.com/doc/refman/8.0/en/regexp.html#function_regexp-substr
type regexpSubstrFunctionClass struct {
	baseFunctionClass
}

func (c *regexpSubstrFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	argTp := []types.EvalType{types.ETString, types.ETString}
	switch len(args) {
	case 3:
		argTp = append(argTp, types.ETInt)
	case 4:
		argTp = append(argTp, types.ETInt, types.ETInt)
	case 5:
		argTp = append(argTp, types.ETInt, types.ETInt, types.ETString)
	}

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, argTp...)
	if err != nil {
		return nil, err
	}

	argType := args[0].GetType(ctx.GetEvalCtx())
	bf.tp.SetFlen(argType.GetFlen())
	sig := builtinRegexpSubstrFuncSig{
		regexpBaseFuncSig: regexpBaseFuncSig{baseBuiltinFunc: bf},
	}
	sig.setPbCode(tipb.ScalarFuncSig_RegexpSubstrSig)

	if sig.isBinaryCollation() {
		return nil, ErrRegexp.GenWithStackByArgs(binaryCollateErr)
	}

	return &sig, nil
}

type builtinRegexpSubstrFuncSig struct {
	regexpBaseFuncSig
}

func (re *builtinRegexpSubstrFuncSig) vectorized() bool {
	return true
}

func (re *builtinRegexpSubstrFuncSig) Clone() builtinFunc {
	newSig := &builtinRegexpSubstrFuncSig{}
	newSig.regexpBaseFuncSig = *re.regexpBaseFuncSig.clone()
	return newSig
}

func (re *builtinRegexpSubstrFuncSig) findString(reg *regexp.Regexp, expr string, occurrence int64) (string, bool, error) {
	matches := reg.FindAllString(expr, -1)
	length := int64(len(matches))
	if length == 0 || occurrence > length {
		return "", true, nil
	}
	return matches[occurrence-1], false, nil
}

func (re *builtinRegexpSubstrFuncSig) findBinString(reg *regexp.Regexp, bexpr []byte, occurrence int64) (string, bool, error) {
	matches := reg.FindAll(bexpr, -1)
	length := int64(len(matches))
	if length == 0 || occurrence > length {
		return "", true, nil
	}

	return fmt.Sprintf("0x%s", strings.ToUpper(hex.EncodeToString(matches[occurrence-1]))), false, nil
}

func (re *builtinRegexpSubstrFuncSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	expr, isNull, err := re.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	pat, isNull, err := re.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	} else if len(pat) == 0 {
		return "", true, ErrRegexp.GenWithStackByArgs(emptyPatternErr)
	}

	occurrence := int64(1)
	matchType := ""
	argNum := len(re.args)
	var bexpr []byte

	if re.isBinaryCollation() {
		bexpr = []byte(expr)
	}

	if argNum >= 3 {
		pos, isNull, err := re.args[2].EvalInt(ctx, row)
		if isNull || err != nil {
			return "", true, err
		}

		// Check position and trim expr
		if re.isBinaryCollation() {
			if pos < 1 || pos > int64(len(bexpr)) {
				if checkOutRangePos(len(bexpr), pos) {
					return "", true, ErrRegexp.GenWithStackByArgs(invalidIndex)
				}
			}

			bexpr = bexpr[pos-1:] // Trim
		} else {
			if pos < 1 || pos > int64(utf8.RuneCountInString(expr)) {
				if checkOutRangePos(len(expr), pos) {
					return "", true, ErrRegexp.GenWithStackByArgs(invalidIndex)
				}
			}

			stringutil.TrimUtf8String(&expr, pos-1) // Trim
		}
	}

	if argNum >= 4 {
		occurrence, isNull, err = re.args[3].EvalInt(ctx, row)
		if isNull || err != nil {
			return "", true, err
		}

		if occurrence < 1 {
			occurrence = 1
		}
	}

	if argNum == 5 {
		matchType, isNull, err = re.args[4].EvalString(ctx, row)
		if isNull || err != nil {
			return "", true, err
		}
	}

	reg, err := re.getRegexp(ctx, pat, matchType, regexpSubstrMatchTypeIdx)
	if err != nil {
		return "", true, err
	}

	if re.isBinaryCollation() {
		return re.findBinString(reg, bexpr, occurrence)
	}
	return re.findString(reg, expr, occurrence)
}

// REGEXP_SUBSTR(expr, pat[, pos[, occurrence[, match_type]]])
func (re *builtinRegexpSubstrFuncSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	params := make([]*funcParam, 0, 5)
	defer releaseBuffers(&re.baseBuiltinFunc, params)

	for i := 0; i < 2; i++ {
		param, isConstNull, err := buildStringParam(ctx, &re.baseBuiltinFunc, i, input, false)
		if err != nil {
			return err
		}
		if isConstNull {
			fillNullStringIntoResult(result, n)
			return nil
		}
		params = append(params, param)
	}

	paramLen := len(re.args)

	// Handle position parameter
	hasPosition := (paramLen >= 3)
	param, isConstNull, err := buildIntParam(ctx, &re.baseBuiltinFunc, 2, input, !hasPosition, 1)
	params = append(params, param)

	if err != nil {
		return ErrRegexp.GenWithStackByArgs(err)
	}
	if isConstNull {
		fillNullStringIntoResult(result, n)
		return nil
	}

	// Handle occurrence parameter
	hasOccur := (paramLen >= 4)
	param, isConstNull, err = buildIntParam(ctx, &re.baseBuiltinFunc, 3, input, !hasOccur, 1)
	params = append(params, param)

	if err != nil {
		return ErrRegexp.GenWithStackByArgs(err)
	}
	if isConstNull {
		fillNullStringIntoResult(result, n)
		return nil
	}

	// Handle match type
	hasMatchType := (paramLen == 5)
	param, isConstNull, err = buildStringParam(ctx, &re.baseBuiltinFunc, 4, input, !hasMatchType)
	params = append(params, param)

	if err != nil {
		return ErrRegexp.GenWithStackByArgs(err)
	}
	if isConstNull {
		fillNullStringIntoResult(result, n)
		return nil
	}

	// Check memorization
	reg, memorized, err := re.tryVecMemorizedRegexp(ctx, params, regexpSubstrMatchTypeIdx, n)
	if err != nil {
		return err
	}

	result.ReserveString(n)
	buffers := getBuffers(params)

	// Start to calculate
	for i := 0; i < n; i++ {
		if isResultNull(buffers, i) {
			result.AppendNull()
			continue
		}

		expr := params[0].getStringVal(i)
		var bexpr []byte

		if re.isBinaryCollation() {
			bexpr = []byte(expr)
		}

		// Check position and trim expr
		pos := params[2].getIntVal(i)
		if re.isBinaryCollation() {
			if pos < 1 || pos > int64(len(bexpr)) {
				if checkOutRangePos(len(bexpr), pos) {
					return ErrRegexp.GenWithStackByArgs(invalidIndex)
				}
			}

			bexpr = bexpr[pos-1:] // Trim
		} else {
			if pos < 1 || pos > int64(utf8.RuneCountInString(expr)) {
				if checkOutRangePos(len(expr), pos) {
					return ErrRegexp.GenWithStackByArgs(invalidIndex)
				}
			}

			stringutil.TrimUtf8String(&expr, pos-1) // Trim
		}

		// Get occurrence
		occurrence := params[3].getIntVal(i)
		if occurrence < 1 {
			occurrence = 1
		}

		if !memorized {
			// Get pattern and match type and then generate regexp
			matchType := params[4].getStringVal(i)
			pattern := params[1].getStringVal(i)
			if reg, err = re.buildRegexp(pattern, matchType); err != nil {
				return err
			}
		}

		// Find string
		if re.isBinaryCollation() {
			matches := reg.FindAll(bexpr, -1)
			length := int64(len(matches))
			if length == 0 || occurrence > length {
				result.AppendNull()
				continue
			}

			result.AppendString(fmt.Sprintf("0x%s", strings.ToUpper(hex.EncodeToString(matches[occurrence-1]))))
		} else {
			matches := reg.FindAllString(expr, -1)
			length := int64(len(matches))
			if length == 0 || occurrence > length {
				result.AppendNull()
				continue
			}

			result.AppendString(matches[occurrence-1])
		}
	}
	return nil
}

// https://dev.mysql.com/doc/refman/8.0/en/regexp.html#function_regexp-instr
type regexpInStrFunctionClass struct {
	baseFunctionClass
}

func (c *regexpInStrFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, ErrRegexp.GenWithStackByArgs(err)
	}

	argTp := []types.EvalType{types.ETString, types.ETString}
	switch len(args) {
	case 3:
		argTp = append(argTp, types.ETInt)
	case 4:
		argTp = append(argTp, types.ETInt, types.ETInt)
	case 5:
		argTp = append(argTp, types.ETInt, types.ETInt, types.ETInt)
	case 6:
		argTp = append(argTp, types.ETInt, types.ETInt, types.ETInt, types.ETString)
	}

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, argTp...)
	if err != nil {
		return nil, ErrRegexp.GenWithStackByArgs(err)
	}

	bf.tp.SetFlen(mysql.MaxIntWidth)
	sig := builtinRegexpInStrFuncSig{
		regexpBaseFuncSig: regexpBaseFuncSig{baseBuiltinFunc: bf},
	}
	sig.setPbCode(tipb.ScalarFuncSig_RegexpInStrSig)

	if sig.isBinaryCollation() {
		return nil, ErrRegexp.GenWithStackByArgs(binaryCollateErr)
	}

	return &sig, nil
}

type builtinRegexpInStrFuncSig struct {
	regexpBaseFuncSig
}

func (re *builtinRegexpInStrFuncSig) Clone() builtinFunc {
	newSig := &builtinRegexpInStrFuncSig{}
	newSig.regexpBaseFuncSig = *re.regexpBaseFuncSig.clone()
	return newSig
}

func (re *builtinRegexpInStrFuncSig) vectorized() bool {
	return true
}

func (re *builtinRegexpInStrFuncSig) findBinIndex(reg *regexp.Regexp, bexpr []byte, pos int64, occurrence int64, returnOption int64) (int64, bool, error) {
	matches := reg.FindAllIndex(bexpr, -1)
	length := int64(len(matches))
	if length == 0 || occurrence > length {
		return 0, false, nil
	}

	if returnOption == 0 {
		return int64(matches[occurrence-1][0]) + pos, false, nil
	}
	return int64(matches[occurrence-1][1]) + pos, false, nil
}

func (re *builtinRegexpInStrFuncSig) findIndex(reg *regexp.Regexp, expr string, pos int64, occurrence int64, returnOption int64) (int64, bool, error) {
	matches := reg.FindAllStringIndex(expr, -1)
	length := int64(len(matches))
	if length == 0 || occurrence > length {
		return 0, false, nil
	}

	if returnOption == 0 {
		return stringutil.ConvertPosInUtf8(&expr, int64(matches[occurrence-1][0])) + pos - 1, false, nil
	}
	return stringutil.ConvertPosInUtf8(&expr, int64(matches[occurrence-1][1])) + pos - 1, false, nil
}

func (re *builtinRegexpInStrFuncSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	expr, isNull, err := re.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, true, err
	}

	pat, isNull, err := re.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, true, err
	} else if len(pat) == 0 {
		return 0, true, ErrRegexp.GenWithStackByArgs(emptyPatternErr)
	}

	pos := int64(1)
	occurrence := int64(1)
	returnOption := int64(0)
	matchType := ""
	argNum := len(re.args)
	var bexpr []byte

	if re.isBinaryCollation() {
		bexpr = []byte(expr)
	}

	if argNum >= 3 {
		pos, isNull, err = re.args[2].EvalInt(ctx, row)
		if isNull || err != nil {
			return 0, true, err
		}

		// Check position and trim expr
		if re.isBinaryCollation() {
			bexprLen := int64(len(bexpr))
			if (pos < 1 || pos > bexprLen) && bexprLen != 0 {
				return 0, true, ErrRegexp.GenWithStackByArgs(invalidIndex)
			}

			if bexprLen != 0 {
				bexpr = bexpr[pos-1:] // Trim
			}
		} else {
			exprLen := int64(len(expr))
			if pos < 1 || pos > int64(utf8.RuneCountInString(expr)) && exprLen != 0 {
				return 0, true, ErrRegexp.GenWithStackByArgs(invalidIndex)
			}

			if exprLen != 0 {
				stringutil.TrimUtf8String(&expr, pos-1) // Trim
			}
		}
	}

	if argNum >= 4 {
		occurrence, isNull, err = re.args[3].EvalInt(ctx, row)
		if isNull || err != nil {
			return 0, true, err
		}

		if occurrence < 1 {
			occurrence = 1
		}
	}

	if argNum >= 5 {
		returnOption, isNull, err = re.args[4].EvalInt(ctx, row)
		if isNull || err != nil {
			return 0, true, err
		}

		if returnOption != 0 && returnOption != 1 {
			return 0, true, ErrRegexp.GenWithStackByArgs(invalidReturnOption)
		}
	}

	if argNum == 6 {
		matchType, isNull, err = re.args[5].EvalString(ctx, row)
		if isNull || err != nil {
			return 0, true, err
		}
	}

	reg, err := re.getRegexp(ctx, pat, matchType, regexpInstrMatchTypeIdx)
	if err != nil {
		return 0, true, err
	}

	if re.isBinaryCollation() {
		return re.findBinIndex(reg, bexpr, pos, occurrence, returnOption)
	}

	return re.findIndex(reg, expr, pos, occurrence, returnOption)
}

// REGEXP_INSTR(expr, pat[, pos[, occurrence[, return_option[, match_type]]]])
func (re *builtinRegexpInStrFuncSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	params := make([]*funcParam, 0, 5)
	defer releaseBuffers(&re.baseBuiltinFunc, params)

	for i := 0; i < 2; i++ {
		param, isConstNull, err := buildStringParam(ctx, &re.baseBuiltinFunc, i, input, false)
		if err != nil {
			return ErrRegexp.GenWithStackByArgs(err)
		}
		if isConstNull {
			result.ResizeInt64(n, true)
			return nil
		}
		params = append(params, param)
	}

	paramLen := len(re.args)

	// Handle position parameter
	hasPosition := (paramLen >= 3)
	param, isConstNull, err := buildIntParam(ctx, &re.baseBuiltinFunc, 2, input, !hasPosition, 1)
	params = append(params, param)

	if err != nil {
		return ErrRegexp.GenWithStackByArgs(err)
	}
	if isConstNull {
		result.ResizeInt64(n, true)
		return nil
	}

	// Handle occurrence parameter
	hasOccur := (paramLen >= 4)
	param, isConstNull, err = buildIntParam(ctx, &re.baseBuiltinFunc, 3, input, !hasOccur, 1)
	params = append(params, param)

	if err != nil {
		return ErrRegexp.GenWithStackByArgs(err)
	}
	if isConstNull {
		result.ResizeInt64(n, true)
		return nil
	}

	// Handle return_option parameter
	hasRetOpt := (paramLen >= 5)
	param, isConstNull, err = buildIntParam(ctx, &re.baseBuiltinFunc, 4, input, !hasRetOpt, 0)
	params = append(params, param)

	if err != nil {
		return ErrRegexp.GenWithStackByArgs(err)
	}
	if isConstNull {
		result.ResizeInt64(n, true)
		return nil
	}

	// Handle match type
	hasMatchType := (paramLen == 6)
	param, isConstNull, err = buildStringParam(ctx, &re.baseBuiltinFunc, 5, input, !hasMatchType)
	params = append(params, param)

	if err != nil {
		return ErrRegexp.GenWithStackByArgs(err)
	}
	if isConstNull {
		result.ResizeInt64(n, true)
		return nil
	}

	reg, memorized, err := re.tryVecMemorizedRegexp(ctx, params, regexpInstrMatchTypeIdx, n)
	if err != nil {
		return err
	}

	// Start to calculate
	result.ResizeInt64(n, false)
	result.MergeNulls(getBuffers(params)...)
	i64s := result.Int64s()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}

		expr := params[0].getStringVal(i)
		var bexpr []byte

		if re.isBinaryCollation() {
			bexpr = []byte(expr)
		}

		// Check position and trim expr
		pos := params[2].getIntVal(i)
		if re.isBinaryCollation() {
			bexprLen := int64(len(bexpr))
			if pos < 1 || pos > bexprLen {
				return ErrRegexp.GenWithStackByArgs(invalidIndex)
			}

			if bexprLen != 0 {
				bexpr = bexpr[pos-1:] // Trim
			}
		} else {
			if pos < 1 || pos > int64(utf8.RuneCountInString(expr)) {
				return ErrRegexp.GenWithStackByArgs(invalidIndex)
			}

			if len(expr) != 0 {
				stringutil.TrimUtf8String(&expr, pos-1) // Trim
			}
		}

		// Get occurrence
		occurrence := params[3].getIntVal(i)
		if occurrence < 1 {
			occurrence = 1
		}

		returnOption := params[4].getIntVal(i)
		if returnOption != 0 && returnOption != 1 {
			return ErrRegexp.GenWithStackByArgs(invalidReturnOption)
		}

		// Get match type and generate regexp
		if !memorized {
			matchType := params[5].getStringVal(i)
			pattern := params[1].getStringVal(i)
			reg, err = re.buildRegexp(pattern, matchType)
			if err != nil {
				return err
			}
		}

		// Find index
		if re.isBinaryCollation() {
			matches := reg.FindAllIndex(bexpr, -1)
			length := int64(len(matches))
			if length == 0 || occurrence > length {
				i64s[i] = 0
				continue
			}

			if returnOption == 0 {
				i64s[i] = int64(matches[occurrence-1][0]) + pos
			} else {
				i64s[i] = int64(matches[occurrence-1][1]) + pos
			}
		} else {
			matches := reg.FindAllStringIndex(expr, -1)
			length := int64(len(matches))
			if length == 0 || occurrence > length {
				i64s[i] = 0
				continue
			}

			if returnOption == 0 {
				i64s[i] = stringutil.ConvertPosInUtf8(&expr, int64(matches[occurrence-1][0])) + pos - 1
			} else {
				i64s[i] = stringutil.ConvertPosInUtf8(&expr, int64(matches[occurrence-1][1])) + pos - 1
			}
		}
	}
	return nil
}

// https://dev.mysql.com/doc/refman/8.0/en/regexp.html#function_regexp-replace
type regexpReplaceFunctionClass struct {
	baseFunctionClass
}

func (c *regexpReplaceFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, ErrRegexp.GenWithStackByArgs(err)
	}

	argTp := []types.EvalType{types.ETString, types.ETString}
	switch len(args) {
	case 3:
		argTp = append(argTp, types.ETString)
	case 4:
		argTp = append(argTp, types.ETString, types.ETInt)
	case 5:
		argTp = append(argTp, types.ETString, types.ETInt, types.ETInt)
	case 6:
		argTp = append(argTp, types.ETString, types.ETInt, types.ETInt, types.ETString)
	}

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, argTp...)
	if err != nil {
		return nil, ErrRegexp.GenWithStackByArgs(err)
	}

	argType := args[0].GetType(ctx.GetEvalCtx())
	bf.tp.SetFlen(argType.GetFlen())
	sig := builtinRegexpReplaceFuncSig{
		regexpBaseFuncSig: regexpBaseFuncSig{baseBuiltinFunc: bf},
	}
	sig.setPbCode(tipb.ScalarFuncSig_RegexpReplaceSig)

	if sig.isBinaryCollation() {
		return nil, ErrRegexp.GenWithStackByArgs(binaryCollateErr)
	}

	return &sig, nil
}

// Instruction contains content that used to replace the origin string
type Instruction struct {
	// If not negative, perform substitution of n-th subpattern from the regexp match.
	SubstitutionNum int

	// Otherwise, paste this literal string verbatim.
	Literal []byte
}

func (ins *Instruction) getCaptureGroupStr(str []byte, matchedRes []int) ([]byte, error) {
	if ins.SubstitutionNum*2 >= len(matchedRes)-1 {
		return []byte(""), ErrRegexp.GenWithStackByArgs(invalidSubstitution)
	}
	return str[matchedRes[ins.SubstitutionNum*2]:matchedRes[ins.SubstitutionNum*2+1]], nil
}

type builtinRegexpReplaceFuncSig struct {
	regexpBaseFuncSig
	instCache builtinFuncCache[[]Instruction]
}

func (re *builtinRegexpReplaceFuncSig) copyReplacement(replacedBStr *[]byte, matchedBexpr *[]byte, res []int, instructions []Instruction) error {
	for _, instruction := range instructions {
		if instruction.SubstitutionNum != -1 {
			capturedStr, err := instruction.getCaptureGroupStr(*matchedBexpr, res)
			if err != nil {
				return err
			}
			*replacedBStr = append(*replacedBStr, capturedStr...)
		} else {
			*replacedBStr = append(*replacedBStr, instruction.Literal...)
		}
	}
	return nil
}

func (re *builtinRegexpReplaceFuncSig) vectorized() bool {
	return true
}

func (re *builtinRegexpReplaceFuncSig) Clone() builtinFunc {
	newSig := &builtinRegexpReplaceFuncSig{}
	newSig.regexpBaseFuncSig = *re.regexpBaseFuncSig.clone()
	return newSig
}

func (re *builtinRegexpReplaceFuncSig) replaceAllMatchedBinStr(reg *regexp.Regexp, bexpr []byte, trimmedBexpr []byte, instructions []Instruction, pos int64) ([]byte, error) {
	replacedBStr := make([]byte, 0)
	allResults := reg.FindAllSubmatchIndex(trimmedBexpr, -1)
	firstNotCopiedPos := 0
	for _, res := range allResults {
		if firstNotCopiedPos < res[0] {
			replacedBStr = append(replacedBStr, trimmedBexpr[firstNotCopiedPos:res[0]]...) // Copy prefix
		}

		// Put the replace string into expression
		err := re.copyReplacement(&replacedBStr, &trimmedBexpr, res, instructions)
		if err != nil {
			return []byte(""), err
		}

		firstNotCopiedPos = res[1]
	}

	replacedBStr = append(replacedBStr, trimmedBexpr[firstNotCopiedPos:]...) // Copy suffix
	return append(bexpr[:pos-1], replacedBStr...), nil
}

func (re *builtinRegexpReplaceFuncSig) replaceOneMatchedBinStr(reg *regexp.Regexp, bexpr []byte, trimmedBexpr []byte, instructions []Instruction, pos int64, occurrence int64) ([]byte, error) {
	replacedBStr := make([]byte, 0)
	allResults := reg.FindAllSubmatchIndex(trimmedBexpr, int(occurrence))
	if int(occurrence) > len(allResults) {
		replacedBStr = trimmedBexpr
	} else {
		res := allResults[occurrence-1]
		replacedBStr = append(replacedBStr, trimmedBexpr[:res[0]]...) // Copy prefix
		err := re.copyReplacement(&replacedBStr, &trimmedBexpr, res, instructions)
		if err != nil {
			return []byte(""), err
		}

		trimmedBexpr = trimmedBexpr[res[1]:]
		replacedBStr = append(replacedBStr, trimmedBexpr...) // Copy suffix
	}

	return append(bexpr[:pos-1], replacedBStr...), nil
}

func (re *builtinRegexpReplaceFuncSig) replaceAllMatchedStr(reg *regexp.Regexp, expr string, trimmedExpr string, instructions []Instruction, pos int64) (string, bool, error) {
	retBStr, err := re.replaceAllMatchedBinStr(reg, []byte(expr), []byte(trimmedExpr), instructions, pos)
	if err != nil {
		return "", false, err
	}
	return string(retBStr), false, nil
}

func (re *builtinRegexpReplaceFuncSig) replaceOneMatchedStr(reg *regexp.Regexp, expr string, trimmedExpr string, instructions []Instruction, pos int64, occurrence int64) (string, bool, error) {
	retBStr, err := re.replaceOneMatchedBinStr(reg, []byte(expr), []byte(trimmedExpr), instructions, pos, occurrence)
	if err != nil {
		return "", false, err
	}
	return string(retBStr), false, nil
}

func (re *builtinRegexpReplaceFuncSig) getReplacedBinStr(reg *regexp.Regexp, bexpr []byte, trimmedBexpr []byte, instructions []Instruction, pos int64, occurrence int64) (string, bool, error) {
	if occurrence == 0 {
		replacedStr, err := re.replaceAllMatchedBinStr(reg, bexpr, trimmedBexpr, instructions, pos)
		if err != nil {
			return "", false, err
		}
		return fmt.Sprintf("0x%s", strings.ToUpper(hex.EncodeToString(replacedStr))), false, nil
	}

	replacedStr, err := re.replaceOneMatchedBinStr(reg, bexpr, trimmedBexpr, instructions, pos, occurrence)
	if err != nil {
		return "", false, err
	}
	return fmt.Sprintf("0x%s", strings.ToUpper(hex.EncodeToString(replacedStr))), false, nil
}

func (re *builtinRegexpReplaceFuncSig) getReplacedStr(reg *regexp.Regexp, expr string, trimmedExpr string, instructions []Instruction, pos int64, occurrence int64) (string, bool, error) {
	if occurrence == 0 {
		return re.replaceAllMatchedStr(reg, expr, trimmedExpr, instructions, pos)
	}

	return re.replaceOneMatchedStr(reg, expr, trimmedExpr, instructions, pos, occurrence)
}

func getInstructions(repl []byte) []Instruction {
	instructions := make([]Instruction, 0)
	var literals []byte

	replLen := len(repl)
	for i := 0; i < replLen; i += 1 {
		if repl[i] == '\\' {
			if i+1 >= replLen {
				// This slash is in the end. Ignore it and break the loop.
				break
			}

			if stringutil.IsNumericASCII(repl[i+1]) { // Substitution
				if len(literals) != 0 {
					instructions = append(instructions, Instruction{SubstitutionNum: -1, Literal: literals})
					literals = []byte{}
				}
				instructions = append(instructions, Instruction{SubstitutionNum: int(repl[i+1] - '0')})
			} else {
				literals = append(literals, repl[i+1]) // Escaping
			}
			i += 1
		} else {
			literals = append(literals, repl[i]) // Plain character
		}
	}
	if len(literals) != 0 {
		instructions = append(instructions, Instruction{SubstitutionNum: -1, Literal: literals})
	}
	return instructions
}

func (re *builtinRegexpReplaceFuncSig) canInstructionsMemorized() bool {
	return re.args[replacementIdx].ConstLevel() >= ConstOnlyInContext
}

func (re *builtinRegexpReplaceFuncSig) getInstructions(ctx EvalContext, repl string) ([]Instruction, error) {
	if !re.canInstructionsMemorized() {
		return getInstructions([]byte(repl)), nil
	}

	instructions, err := re.instCache.getOrInitCache(ctx, func() ([]Instruction, error) {
		return getInstructions([]byte(repl)), nil
	})

	intest.AssertNoError(err)
	return instructions, err
}

func (re *builtinRegexpReplaceFuncSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	expr, isNull, err := re.args[0].EvalString(ctx, row)
	trimmedExpr := expr
	if isNull || err != nil {
		return "", true, err
	}

	pat, isNull, err := re.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	} else if len(pat) == 0 {
		return "", true, ErrRegexp.GenWithStackByArgs(emptyPatternErr)
	}

	repl, isNull, err := re.args[2].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	pos := int64(1)
	occurrence := int64(0)
	matchType := ""
	argNum := len(re.args)
	var bexpr []byte
	var trimmedBexpr []byte

	if re.isBinaryCollation() {
		bexpr = []byte(expr)
		trimmedBexpr = bexpr
	}

	trimmedLen := int64(0)
	if argNum >= 4 {
		pos, isNull, err = re.args[3].EvalInt(ctx, row)
		if isNull || err != nil {
			return "", true, err
		}

		// Check position and trim expr
		if re.isBinaryCollation() {
			if pos < 1 || pos > int64(len(trimmedBexpr)) {
				if checkOutRangePos(len(trimmedBexpr), pos) {
					return "", true, ErrRegexp.GenWithStackByArgs(invalidIndex)
				}
			}

			trimmedBexpr = bexpr[pos-1:] // Trim
		} else {
			if pos < 1 || pos > int64(utf8.RuneCountInString(trimmedExpr)) {
				if checkOutRangePos(len(trimmedExpr), pos) {
					return "", true, ErrRegexp.GenWithStackByArgs(invalidIndex)
				}
			}

			trimmedLen = stringutil.TrimUtf8String(&trimmedExpr, pos-1) // Trim
		}
	}

	if argNum >= 5 {
		occurrence, isNull, err = re.args[4].EvalInt(ctx, row)
		if isNull || err != nil {
			return "", true, err
		}

		if occurrence < 0 {
			occurrence = 1
		}
	}

	if argNum == 6 {
		matchType, isNull, err = re.args[5].EvalString(ctx, row)
		if isNull || err != nil {
			return "", true, err
		}
	}

	reg, err := re.getRegexp(ctx, pat, matchType, regexpReplaceMatchTypeIdx)
	if err != nil {
		return "", true, err
	}

	instructions, err := re.getInstructions(ctx, repl)
	if err != nil {
		return "", true, err
	}

	if re.isBinaryCollation() {
		return re.getReplacedBinStr(reg, bexpr, trimmedBexpr, instructions, pos, occurrence)
	}
	return re.getReplacedStr(reg, expr, trimmedExpr, instructions, trimmedLen+1, occurrence)
}

// REGEXP_REPLACE(expr, pat, repl[, pos[, occurrence[, match_type]]])
func (re *builtinRegexpReplaceFuncSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	params := make([]*funcParam, 0, 6)
	defer releaseBuffers(&re.baseBuiltinFunc, params)

	for i := 0; i < 2; i++ {
		param, isConstNull, err := buildStringParam(ctx, &re.baseBuiltinFunc, i, input, false)
		if err != nil {
			return ErrRegexp.GenWithStackByArgs(err)
		}
		if isConstNull {
			fillNullStringIntoResult(result, n)
			return nil
		}
		params = append(params, param)
	}

	paramLen := len(re.args)

	// Handle repl parameter
	hasRepl := (paramLen >= 3)
	param, isConstNull, err := buildStringParam(ctx, &re.baseBuiltinFunc, 2, input, !hasRepl)
	params = append(params, param)

	if err != nil {
		return ErrRegexp.GenWithStackByArgs(err)
	}
	if isConstNull {
		fillNullStringIntoResult(result, n)
		return nil
	}

	// Handle position parameter
	hasPosition := (paramLen >= 4)
	param, isConstNull, err = buildIntParam(ctx, &re.baseBuiltinFunc, 3, input, !hasPosition, 1)
	params = append(params, param)

	if err != nil {
		return ErrRegexp.GenWithStackByArgs(err)
	}
	if isConstNull {
		fillNullStringIntoResult(result, n)
		return nil
	}

	// Handle occurrence parameter
	hasOccur := (paramLen >= 5)
	param, isConstNull, err = buildIntParam(ctx, &re.baseBuiltinFunc, 4, input, !hasOccur, 0)
	params = append(params, param)

	if err != nil {
		return ErrRegexp.GenWithStackByArgs(err)
	}
	if isConstNull {
		fillNullStringIntoResult(result, n)
		return nil
	}

	// Handle match type
	hasMatchType := (paramLen == 6)
	param, isConstNull, err = buildStringParam(ctx, &re.baseBuiltinFunc, 5, input, !hasMatchType)
	params = append(params, param)
	if err != nil {
		return ErrRegexp.GenWithStackByArgs(err)
	}

	if isConstNull {
		fillNullStringIntoResult(result, n)
		return nil
	}

	reg, memorized, err := re.tryVecMemorizedRegexp(ctx, params, regexpReplaceMatchTypeIdx, n)
	if err != nil {
		return err
	}

	result.ReserveString(n)
	buffers := getBuffers(params)

	instructions := make([]Instruction, 0)
	canMemorizeRepl := re.canInstructionsMemorized() && n > 0
	if canMemorizeRepl {
		instructions, err = re.getInstructions(ctx, params[replacementIdx].getStringVal(0))
		if err != nil {
			return err
		}
	}

	// Start to calculate
	for i := 0; i < n; i++ {
		if isResultNull(buffers, i) {
			result.AppendNull()
			continue
		}

		expr := params[0].getStringVal(i)
		trimmedExpr := expr
		var bexpr []byte
		var trimmedBexpr []byte

		if re.isBinaryCollation() {
			bexpr = []byte(expr)
			trimmedBexpr = bexpr
		}

		repl := params[2].getStringVal(i)

		// Check position and trim expr
		pos := params[3].getIntVal(i)
		trimmedLen := int64(0)
		if re.isBinaryCollation() {
			if pos < 1 || pos > int64(len(trimmedBexpr)) {
				if checkOutRangePos(len(trimmedBexpr), pos) {
					return ErrRegexp.GenWithStackByArgs(invalidIndex)
				}
			}

			trimmedBexpr = bexpr[pos-1:] // Trim
		} else {
			if pos < 1 || pos > int64(utf8.RuneCountInString(trimmedExpr)) {
				if checkOutRangePos(len(trimmedExpr), pos) {
					return ErrRegexp.GenWithStackByArgs(invalidIndex)
				}
			}

			trimmedLen = stringutil.TrimUtf8String(&trimmedExpr, pos-1) // Trim
		}

		// Get occurrence
		occurrence := params[4].getIntVal(i)
		if occurrence < 0 {
			occurrence = 1
		}

		// Get match type and generate regexp
		if !memorized {
			matchType := params[5].getStringVal(i)
			pattern := params[1].getStringVal(i)
			reg, err = re.buildRegexp(pattern, matchType)
			if err != nil {
				return err
			}
		}

		if !canMemorizeRepl {
			instructions = getInstructions([]byte(repl))
		}

		// Start to replace
		if re.isBinaryCollation() {
			var replacedBStr string
			replacedBStr, _, err = re.getReplacedBinStr(reg, bexpr, trimmedBexpr, instructions, pos, occurrence)
			if err != nil {
				return ErrRegexp.GenWithStackByArgs(err)
			}

			result.AppendString(fmt.Sprintf("0x%s", strings.ToUpper(hex.EncodeToString([]byte(replacedBStr)))))
		} else {
			var replacedStr string
			replacedStr, _, err = re.getReplacedStr(reg, expr, trimmedExpr, instructions, trimmedLen+1, occurrence)
			if err != nil {
				return ErrRegexp.GenWithStackByArgs(err)
			}

			result.AppendString(replacedStr)
		}
	}
	return nil
}
