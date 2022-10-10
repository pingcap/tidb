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
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/set"
	"github.com/pingcap/tidb/util/stringutil"
	"github.com/pingcap/tipb/go-tipb"
)

type empty struct{}

const patternIdx = 1
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
	regexpMemorizedSig
	once sync.Once
}

// check binary collation, not xxx_bin collation!
func (re *regexpBaseFuncSig) isBinaryCollation() bool {
	return re.collation == charset.CollationBin && re.charset == charset.CharsetBin
}

func (re *regexpBaseFuncSig) clone() *regexpBaseFuncSig {
	newSig := &regexpBaseFuncSig{once: sync.Once{}}
	if re.memorizedRegexp != nil {
		newSig.memorizedRegexp = re.memorizedRegexp
	}
	newSig.memorizedErr = re.memorizedErr
	newSig.cloneFrom(&re.baseBuiltinFunc)
	return newSig
}

// If characters specifying contradictory options are specified
// within match_type, the rightmost one takes precedence.
func (re *regexpBaseFuncSig) getMatchType(userInputMatchType string) (string, error) {
	flag := ""
	matchTypeSet := set.NewStringSet()

	if collate.IsCICollation(re.baseBuiltinFunc.collation) {
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

// To get a unified compile interface in initMemoizedRegexp, we need to process many things in genCompile
func (re *regexpBaseFuncSig) genCompile(matchType string) (func(string) (*regexp.Regexp, error), error) {
	matchType, err := re.getMatchType(matchType)
	if err != nil {
		return nil, err
	}

	return func(pat string) (*regexp.Regexp, error) {
		if len(matchType) == 0 {
			return regexp.Compile(pat)
		}
		return regexp.Compile(fmt.Sprintf("(?%s)%s", matchType, pat))
	}, nil
}

func (re *regexpBaseFuncSig) genRegexp(pat string, matchType string) (*regexp.Regexp, error) {
	if len(pat) == 0 {
		return nil, ErrRegexp.GenWithStackByArgs(emptyPatternErr)
	}

	if re.isMemorizedRegexpInitialized() {
		return re.memorizedRegexp, re.memorizedErr
	}

	var err error

	// Generate compiler first
	compile, err := re.genCompile(matchType)
	if err != nil {
		return nil, err
	}

	return compile(pat)
}

// we can memorize the regexp when:
//  1. pattern and match type are constant
//  2. pattern is const and there is no match type argument
//
// return true: need, false: needless
func (re *regexpBaseFuncSig) canMemorize(matchTypeIdx int) bool {
	return re.args[patternIdx].ConstItem(re.ctx.GetSessionVars().StmtCtx) && (len(re.args) <= matchTypeIdx || re.args[matchTypeIdx].ConstItem(re.ctx.GetSessionVars().StmtCtx))
}

func (re *regexpBaseFuncSig) initMemoizedRegexp(params []*regexpParam, matchTypeIdx int) error {
	pat := params[patternIdx].getStringVal(0)
	if len(pat) == 0 {
		return ErrRegexp.GenWithStackByArgs(emptyPatternErr)
	}

	// Generate compile
	compile, err := re.genCompile(params[matchTypeIdx].getStringVal(0))
	if err != nil {
		return ErrRegexp.GenWithStackByArgs(err)
	}

	// Compile this constant pattern, so that we can avoid this repeated work
	re.memorize(compile, pat)

	return re.memorizedErr
}

// As multiple threads may memorize regexp and cause data race, only the first thread
// who gets the lock is permitted to do the memorization and others should wait for him
// until the memorization has been finished.
func (re *regexpBaseFuncSig) tryToMemorize(params []*regexpParam, matchTypeIdx int, n int) error {
	// Check memorization
	if n == 0 || !re.canMemorize(matchTypeIdx) {
		return nil
	}

	var err error
	memorize := func() {
		if re.isMemorizedRegexpInitialized() {
			err = nil
			return
		}

		err = re.initMemoizedRegexp(params, matchTypeIdx)
	}

	re.once.Do(memorize)

	return err
}

// https://dev.mysql.com/doc/refman/8.0/en/regexp.html#function_regexp-like
type regexpLikeFunctionClass struct {
	baseFunctionClass
}

func (c *regexpLikeFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
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

func (re *builtinRegexpLikeFuncSig) evalInt(row chunk.Row) (int64, bool, error) {
	expr, isNull, err := re.args[0].EvalString(re.ctx, row)
	if isNull || err != nil {
		return 0, true, err
	}

	pat, isNull, err := re.args[1].EvalString(re.ctx, row)
	if isNull || err != nil {
		return 0, true, err
	} else if len(pat) == 0 {
		return 0, true, ErrRegexp.GenWithStackByArgs(emptyPatternErr)
	}

	matchType := ""
	if len(re.args) == 3 {
		matchType, isNull, err = re.args[2].EvalString(re.ctx, row)
		if isNull || err != nil {
			return 0, true, err
		}
	}

	memorize := func() {
		compile, err := re.genCompile(matchType)
		if err != nil {
			re.memorizedErr = err
			return
		}
		re.memorize(compile, pat)
	}

	if re.canMemorize(regexpLikeMatchTypeIdx) {
		re.once.Do(memorize) // Avoid data race
	}

	if !re.isMemorizedRegexpInitialized() {
		compile, err := re.genCompile(matchType)
		if err != nil {
			return 0, true, ErrRegexp.GenWithStackByArgs(err)
		}
		reg, err := compile(pat)
		if err != nil {
			return 0, true, ErrRegexp.GenWithStackByArgs(err)
		}
		return boolToInt64(reg.MatchString(expr)), false, nil
	}

	if re.memorizedErr != nil {
		return 0, true, ErrRegexp.GenWithStackByArgs(re.memorizedErr)
	}

	return boolToInt64(re.memorizedRegexp.MatchString(expr)), false, nil
}

// REGEXP_LIKE(expr, pat[, match_type])
func (re *builtinRegexpLikeFuncSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	params := make([]*regexpParam, 0, 3)
	defer releaseBuffers(&re.baseBuiltinFunc, params)

	for i := 0; i < 2; i++ {
		param, isConstNull, err := buildStringParam(&re.baseBuiltinFunc, i, input, false)
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
	param, isConstNull, err := buildStringParam(&re.baseBuiltinFunc, 2, input, !hasMatchType)
	params = append(params, param)
	if err != nil {
		return ErrRegexp.GenWithStackByArgs(err)
	}

	if isConstNull {
		result.ResizeInt64(n, true)
		return nil
	}

	err = re.tryToMemorize(params, regexpLikeMatchTypeIdx, n)
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

		matchType := params[2].getStringVal(i)
		re, err := re.genRegexp(params[1].getStringVal(i), matchType)
		if err != nil {
			return ErrRegexp.GenWithStackByArgs(err)
		}
		i64s[i] = boolToInt64(re.MatchString(params[0].getStringVal(i)))
	}
	return nil
}

// https://dev.mysql.com/doc/refman/8.0/en/regexp.html#function_regexp-substr
type regexpSubstrFunctionClass struct {
	baseFunctionClass
}

func (c *regexpSubstrFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
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

	argType := args[0].GetType()
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

func (re *builtinRegexpSubstrFuncSig) evalString(row chunk.Row) (string, bool, error) {
	expr, isNull, err := re.args[0].EvalString(re.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	pat, isNull, err := re.args[1].EvalString(re.ctx, row)
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
		pos, isNull, err := re.args[2].EvalInt(re.ctx, row)
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
		occurrence, isNull, err = re.args[3].EvalInt(re.ctx, row)
		if isNull || err != nil {
			return "", true, err
		}

		if occurrence < 1 {
			occurrence = 1
		}
	}

	if argNum == 5 {
		matchType, isNull, err = re.args[4].EvalString(re.ctx, row)
		if isNull || err != nil {
			return "", true, err
		}
	}

	memorize := func() {
		compile, err := re.genCompile(matchType)
		if err != nil {
			re.memorizedErr = err
			return
		}
		re.memorize(compile, pat)
	}

	if re.canMemorize(regexpSubstrMatchTypeIdx) {
		re.once.Do(memorize) // Avoid data race
	}

	if !re.isMemorizedRegexpInitialized() {
		compile, err := re.genCompile(matchType)
		if err != nil {
			return "", true, ErrRegexp.GenWithStackByArgs(err)
		}
		reg, err := compile(pat)
		if err != nil {
			return "", true, ErrRegexp.GenWithStackByArgs(err)
		}

		if re.isBinaryCollation() {
			return re.findBinString(reg, bexpr, occurrence)
		}
		return re.findString(reg, expr, occurrence)
	}

	if re.memorizedErr != nil {
		return "", true, ErrRegexp.GenWithStackByArgs(re.memorizedErr)
	}

	if re.isBinaryCollation() {
		return re.findBinString(re.memorizedRegexp, bexpr, occurrence)
	}

	return re.findString(re.memorizedRegexp, expr, occurrence)
}

// REGEXP_SUBSTR(expr, pat[, pos[, occurrence[, match_type]]])
func (re *builtinRegexpSubstrFuncSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	params := make([]*regexpParam, 0, 5)
	defer releaseBuffers(&re.baseBuiltinFunc, params)

	for i := 0; i < 2; i++ {
		param, isConstNull, err := buildStringParam(&re.baseBuiltinFunc, i, input, false)
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
	param, isConstNull, err := buildIntParam(&re.baseBuiltinFunc, 2, input, !hasPosition, 1)
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
	param, isConstNull, err = buildIntParam(&re.baseBuiltinFunc, 3, input, !hasOccur, 1)
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
	param, isConstNull, err = buildStringParam(&re.baseBuiltinFunc, 4, input, !hasMatchType)
	params = append(params, param)

	if err != nil {
		return ErrRegexp.GenWithStackByArgs(err)
	}
	if isConstNull {
		fillNullStringIntoResult(result, n)
		return nil
	}

	// Check memorization
	err = re.tryToMemorize(params, regexpSubstrMatchTypeIdx, n)
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

		// Get match type and generate regexp
		matchType := params[4].getStringVal(i)
		reg, err := re.genRegexp(params[1].getStringVal(i), matchType)
		if err != nil {
			return err
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

func (c *regexpInStrFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
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

func (re *builtinRegexpInStrFuncSig) evalInt(row chunk.Row) (int64, bool, error) {
	expr, isNull, err := re.args[0].EvalString(re.ctx, row)
	if isNull || err != nil {
		return 0, true, err
	}

	pat, isNull, err := re.args[1].EvalString(re.ctx, row)
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
		pos, isNull, err = re.args[2].EvalInt(re.ctx, row)
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
		occurrence, isNull, err = re.args[3].EvalInt(re.ctx, row)
		if isNull || err != nil {
			return 0, true, err
		}

		if occurrence < 1 {
			occurrence = 1
		}
	}

	if argNum >= 5 {
		returnOption, isNull, err = re.args[4].EvalInt(re.ctx, row)
		if isNull || err != nil {
			return 0, true, err
		}

		if returnOption != 0 && returnOption != 1 {
			return 0, true, ErrRegexp.GenWithStackByArgs(invalidReturnOption)
		}
	}

	if argNum == 6 {
		matchType, isNull, err = re.args[5].EvalString(re.ctx, row)
		if isNull || err != nil {
			return 0, true, err
		}
	}

	memorize := func() {
		compile, err := re.genCompile(matchType)
		if err != nil {
			re.memorizedErr = err
			return
		}
		re.memorize(compile, pat)
	}

	if re.canMemorize(regexpInstrMatchTypeIdx) {
		re.once.Do(memorize) // Avoid data race
	}

	if !re.isMemorizedRegexpInitialized() {
		compile, err := re.genCompile(matchType)
		if err != nil {
			return 0, true, ErrRegexp.GenWithStackByArgs(err)
		}
		reg, err := compile(pat)
		if err != nil {
			return 0, true, ErrRegexp.GenWithStackByArgs(err)
		}

		if re.isBinaryCollation() {
			return re.findBinIndex(reg, bexpr, pos, occurrence, returnOption)
		}
		return re.findIndex(reg, expr, pos, occurrence, returnOption)
	}

	if re.memorizedErr != nil {
		return 0, true, ErrRegexp.GenWithStackByArgs(re.memorizedErr)
	}

	if re.isBinaryCollation() {
		return re.findBinIndex(re.memorizedRegexp, bexpr, pos, occurrence, returnOption)
	}

	return re.findIndex(re.memorizedRegexp, expr, pos, occurrence, returnOption)
}

// REGEXP_INSTR(expr, pat[, pos[, occurrence[, return_option[, match_type]]]])
func (re *builtinRegexpInStrFuncSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	params := make([]*regexpParam, 0, 5)
	defer releaseBuffers(&re.baseBuiltinFunc, params)

	for i := 0; i < 2; i++ {
		param, isConstNull, err := buildStringParam(&re.baseBuiltinFunc, i, input, false)
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
	param, isConstNull, err := buildIntParam(&re.baseBuiltinFunc, 2, input, !hasPosition, 1)
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
	param, isConstNull, err = buildIntParam(&re.baseBuiltinFunc, 3, input, !hasOccur, 1)
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
	param, isConstNull, err = buildIntParam(&re.baseBuiltinFunc, 4, input, !hasRetOpt, 0)
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
	param, isConstNull, err = buildStringParam(&re.baseBuiltinFunc, 5, input, !hasMatchType)
	params = append(params, param)

	if err != nil {
		return ErrRegexp.GenWithStackByArgs(err)
	}
	if isConstNull {
		result.ResizeInt64(n, true)
		return nil
	}

	err = re.tryToMemorize(params, regexpInstrMatchTypeIdx, n)
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
		matchType := params[5].getStringVal(i)
		reg, err := re.genRegexp(params[1].getStringVal(i), matchType)
		if err != nil {
			return ErrRegexp.GenWithStackByArgs(err)
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

func (c *regexpReplaceFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
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

	argType := args[0].GetType()
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

type builtinRegexpReplaceFuncSig struct {
	regexpBaseFuncSig
}

func (re *builtinRegexpReplaceFuncSig) vectorized() bool {
	return true
}

func (re *builtinRegexpReplaceFuncSig) Clone() builtinFunc {
	newSig := &builtinRegexpReplaceFuncSig{}
	newSig.regexpBaseFuncSig = *re.regexpBaseFuncSig.clone()
	return newSig
}

func (re *builtinRegexpReplaceFuncSig) getReplacedBinStr(reg *regexp.Regexp, bexpr []byte, trimmedBexpr []byte, repl string, pos int64, occurrence int64) (string, bool, error) {
	count := occurrence
	repFunc := func(matchedStr []byte) []byte {
		if occurrence == 0 {
			return []byte(repl)
		}

		count--
		if count == 0 {
			return []byte(repl)
		}

		return matchedStr
	}

	replacedBStr := reg.ReplaceAllFunc(trimmedBexpr, repFunc)

	return fmt.Sprintf("0x%s", strings.ToUpper(hex.EncodeToString(append(bexpr[:pos-1], replacedBStr...)))), false, nil
}

func (re *builtinRegexpReplaceFuncSig) getReplacedStr(reg *regexp.Regexp, expr string, trimmedExpr string, repl string, trimmedLen int64, occurrence int64) (string, bool, error) {
	count := occurrence
	repFunc := func(matchedStr string) string {
		if occurrence == 0 {
			return repl
		}

		count--
		if count == 0 {
			return repl
		}

		return matchedStr
	}

	replacedStr := reg.ReplaceAllStringFunc(trimmedExpr, repFunc)
	return expr[:trimmedLen] + replacedStr, false, nil
}

func (re *builtinRegexpReplaceFuncSig) evalString(row chunk.Row) (string, bool, error) {
	expr, isNull, err := re.args[0].EvalString(re.ctx, row)
	trimmedExpr := expr
	if isNull || err != nil {
		return "", true, err
	}

	pat, isNull, err := re.args[1].EvalString(re.ctx, row)
	if isNull || err != nil {
		return "", true, err
	} else if len(pat) == 0 {
		return "", true, ErrRegexp.GenWithStackByArgs(emptyPatternErr)
	}

	repl, isNull, err := re.args[2].EvalString(re.ctx, row)
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
		pos, isNull, err = re.args[3].EvalInt(re.ctx, row)
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
		occurrence, isNull, err = re.args[4].EvalInt(re.ctx, row)
		if isNull || err != nil {
			return "", true, err
		}

		if occurrence < 0 {
			occurrence = 1
		}
	}

	if argNum == 6 {
		matchType, isNull, err = re.args[5].EvalString(re.ctx, row)
		if isNull || err != nil {
			return "", true, err
		}
	}

	memorize := func() {
		compile, err := re.genCompile(matchType)
		if err != nil {
			re.memorizedErr = err
			return
		}
		re.memorize(compile, pat)
	}

	if re.canMemorize(regexpReplaceMatchTypeIdx) {
		re.once.Do(memorize) // Avoid data race
	}

	if !re.isMemorizedRegexpInitialized() {
		compile, err := re.genCompile(matchType)
		if err != nil {
			return "", true, ErrRegexp.GenWithStackByArgs(err)
		}
		reg, err := compile(pat)
		if err != nil {
			return "", true, ErrRegexp.GenWithStackByArgs(err)
		}

		if re.isBinaryCollation() {
			return re.getReplacedBinStr(reg, bexpr, trimmedBexpr, repl, pos, occurrence)
		}
		return re.getReplacedStr(reg, expr, trimmedExpr, repl, trimmedLen, occurrence)
	}

	if re.memorizedErr != nil {
		return "", true, ErrRegexp.GenWithStackByArgs(re.memorizedErr)
	}

	if re.isBinaryCollation() {
		return re.getReplacedBinStr(re.memorizedRegexp, bexpr, trimmedBexpr, repl, pos, occurrence)
	}
	return re.getReplacedStr(re.memorizedRegexp, expr, trimmedExpr, repl, trimmedLen, occurrence)
}

// REGEXP_REPLACE(expr, pat, repl[, pos[, occurrence[, match_type]]])
func (re *builtinRegexpReplaceFuncSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	params := make([]*regexpParam, 0, 6)
	defer releaseBuffers(&re.baseBuiltinFunc, params)

	for i := 0; i < 2; i++ {
		param, isConstNull, err := buildStringParam(&re.baseBuiltinFunc, i, input, false)
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
	param, isConstNull, err := buildStringParam(&re.baseBuiltinFunc, 2, input, !hasRepl)
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
	param, isConstNull, err = buildIntParam(&re.baseBuiltinFunc, 3, input, !hasPosition, 1)
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
	param, isConstNull, err = buildIntParam(&re.baseBuiltinFunc, 4, input, !hasOccur, 0)
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
	param, isConstNull, err = buildStringParam(&re.baseBuiltinFunc, 5, input, !hasMatchType)
	params = append(params, param)
	if err != nil {
		return ErrRegexp.GenWithStackByArgs(err)
	}

	if isConstNull {
		fillNullStringIntoResult(result, n)
		return nil
	}

	err = re.tryToMemorize(params, regexpReplaceMatchTypeIdx, n)
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
		matchType := params[5].getStringVal(i)
		reg, err := re.genRegexp(params[1].getStringVal(i), matchType)
		if err != nil {
			return ErrRegexp.GenWithStackByArgs(err)
		}

		// Start to replace
		count := occurrence
		if re.isBinaryCollation() {
			repFunc := func(matchedStr []byte) []byte {
				if occurrence == 0 {
					return []byte(repl)
				}

				count--
				if count == 0 {
					return []byte(repl)
				}

				return matchedStr
			}

			replacedBStr := reg.ReplaceAllFunc(trimmedBexpr, repFunc)
			result.AppendString(fmt.Sprintf("0x%s", strings.ToUpper(hex.EncodeToString(append(bexpr[:pos-1], replacedBStr...)))))
		} else {
			repFunc := func(matchedStr string) string {
				if occurrence == 0 {
					return repl
				}

				count--
				if count == 0 {
					return repl
				}

				return matchedStr
			}

			replacedStr := reg.ReplaceAllStringFunc(trimmedExpr, repFunc)
			result.AppendString(expr[:trimmedLen] + replacedStr)
		}
	}
	return nil
}
