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
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
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

	for i := range 2 {
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
	for i := range n {
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

	for i := range 2 {
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
	for i := range n {
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
		occurrence := max(params[3].getIntVal(i), 1)

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

