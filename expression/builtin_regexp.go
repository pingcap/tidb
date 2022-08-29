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
	"unicode/utf8"

	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tipb/go-tipb"
)

type empty struct{}

// Valid flags in mysql's match type
const (
	flag_i = "i"
	flag_c = "c"
	flag_m = "m"
	flag_n = "n"
)

const (
	invalidMatchType = "Invalid match type"
	invalidIndex     = "Index out of bounds in regular expression search"
)

var validMatchType = map[string]empty{
	flag_i: {}, // Case-insensitive matching
	flag_c: {}, // Case-sensitive matching
	flag_m: {}, // Multiple-line mode
	flag_n: {}, // The . character matches line terminators
}

type regexpBaseFuncSig struct {
	baseBuiltinFunc
	regexpMemorizedSig
	compile func(string) (*regexp.Regexp, error)
}

func (re *regexpBaseFuncSig) clone(from *regexpBaseFuncSig) {
	if from.memorizedRegexp != nil {
		re.memorizedRegexp = from.memorizedRegexp.Copy()
	}
	re.memorizedErr = from.memorizedErr
	re.compile = from.compile
	re.cloneFrom(&from.baseBuiltinFunc)
}

// Convert mysql match type format to re2 format
//
// If characters specifying contradictory options are specified
// within match_type, the rightmost one takes precedence.
func (re *regexpBaseFuncSig) getMatchType(bf *baseBuiltinFunc, userInputMatchType string) (string, error) {
	flag := ""
	matchTypeSet := make(map[string]empty)

	if bf.collation != charset.CollationBin && collate.IsCICollation(bf.collation) {
		matchTypeSet[flag_i] = empty{}
	}

	for _, val := range userInputMatchType {
		c := string(val)

		// Check validation of the flag
		_, err := validMatchType[c]
		if err == false {
			return "", ErrRegexp.GenWithStackByArgs(invalidMatchType)
		}

		if c == flag_c {
			// re2 is case-sensitive by default, so we only need to delete 'i' flag
			// to enable the case-sensitive for the regexp
			delete(matchTypeSet, flag_i)
			continue
		}

		if c == "n" {
			matchTypeSet["s"] = empty{} // convert mysql's match type to the re2
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
	matchType, err := re.getMatchType(&re.baseBuiltinFunc, matchType)
	if err != nil {
		return nil, err
	}

	return func(pat string) (*regexp.Regexp, error) {
		return regexp.Compile(fmt.Sprintf("(?%s)%s", matchType, pat))
	}, nil
}

func (re *regexpBaseFuncSig) genRegexp(pat string, matchType string) (*regexp.Regexp, error) {
	if re.isMemorizedRegexpInitialized() {
		return re.memorizedRegexp, re.memorizedErr
	}

	var err error

	// Generate compiler first
	re.compile, err = re.genCompile(matchType)
	if err != nil {
		return nil, err
	}

	return re.compile(pat)
}

// ---------------------------------- regexp_like ----------------------------------

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

	bf.tp.SetFlen(mysql.MaxIntWidth)
	sig := regexpLikeFuncSig{
		regexpBaseFuncSig: regexpBaseFuncSig{baseBuiltinFunc: bf},
	}
	if bf.collation == charset.CollationBin {
		sig.setPbCode(tipb.ScalarFuncSig_RegexpLikeSig)
	} else {
		sig.setPbCode(tipb.ScalarFuncSig_RegexpLikeUTF8Sig)
	}

	return &sig, nil
}

type regexpLikeFuncSig struct {
	regexpBaseFuncSig
}

func (re *regexpLikeFuncSig) Clone() builtinFunc {
	newSig := &regexpLikeFuncSig{}
	newSig.cloneFrom(&re.baseBuiltinFunc)
	newSig.clone(&re.regexpBaseFuncSig)
	return newSig
}

func (re *regexpLikeFuncSig) vectorized() bool {
	return true
}

// Call this function when all args are constant
func (re *regexpLikeFuncSig) evalInt(row chunk.Row) (int64, bool, error) {
	expr, isNull, err := re.args[0].EvalString(re.ctx, row)
	if isNull || err != nil {
		return 0, true, err
	}

	pat, isNull, err := re.args[1].EvalString(re.ctx, row)
	if isNull || err != nil {
		return 0, true, err
	}

	matchType := ""
	if len(re.args) == 3 {
		matchType, isNull, err = re.args[2].EvalString(re.ctx, row)
		if isNull || err != nil {
			return 0, true, err
		}
	}

	re.compile, err = re.genCompile(matchType)
	if err != nil {
		return 0, true, ErrRegexp.GenWithStackByArgs(err.Error())
	}

	reg, err := re.compile(pat)
	if err != nil {
		return 0, true, ErrRegexp.GenWithStackByArgs(err.Error())
	}

	return boolToInt64(reg.MatchString(expr)), false, nil
}

// we need to memorize the regexp when:
//  1. pattern and match type are constant
//  2. pattern is const and match type is null
//
// return true: need, false: needless
func (re *regexpLikeFuncSig) needMemorization() bool {
	return (re.args[1].ConstItem(re.ctx.GetSessionVars().StmtCtx) && (len(re.args) >= 2 || re.args[2].ConstItem(re.ctx.GetSessionVars().StmtCtx))) && !re.isMemorizedRegexpInitialized()
}

// Call this function when at least one of the args is vector
// REGEXP_LIKE(expr, pat[, match_type])
func (re *regexpLikeFuncSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	params := make([]*regexpParam, 0, 3)

	for i := 0; i < 2; i++ {
		param, err := buildStringParam(&re.baseBuiltinFunc, i, input, false)
		if err != nil {
			return err
		}
		params = append(params, param)
	}

	// user may ignore match type parameter
	hasMatchType := (len(re.args) == 3)
	param, err := buildStringParam(&re.baseBuiltinFunc, 2, input, !hasMatchType)
	params = append(params, param)
	defer releaseBuffers(&re.baseBuiltinFunc, params)

	if err != nil {
		return err
	}

	// Check memorization
	if re.needMemorization() {
		// matchType must be const or null
		matchType := params[2].getStringVal(0)

		re.compile, err = re.genCompile(matchType)
		if err != nil {
			return err
		}

		re.initMemoizedRegexp(re.compile, params[1].getCol(), n)
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
			return err
		}
		i64s[i] = boolToInt64(re.MatchString(params[0].getStringVal(i)))
	}
	return nil
}

// ---------------------------------- regexp_substr ----------------------------------

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
	sig := regexpSubstrFuncSig{
		regexpBaseFuncSig: regexpBaseFuncSig{baseBuiltinFunc: bf},
	}

	if bf.collation == charset.CollationBin {
		sig.setPbCode(tipb.ScalarFuncSig_RegexpSubstrSig)
		sig.isBinCollation = true
	} else {
		sig.setPbCode(tipb.ScalarFuncSig_RegexpSubstrUTF8Sig)
		sig.isBinCollation = false
	}

	return &sig, nil
}

type regexpSubstrFuncSig struct {
	regexpBaseFuncSig
	isBinCollation bool
}

func (re *regexpSubstrFuncSig) vectorized() bool {
	return true
}

func (re *regexpSubstrFuncSig) Clone() builtinFunc {
	newSig := &regexpSubstrFuncSig{}
	newSig.cloneFrom(&re.baseBuiltinFunc)
	newSig.clone(&re.regexpBaseFuncSig)
	newSig.isBinCollation = re.isBinCollation
	return newSig
}

// Call this function when all args are constant
func (re *regexpSubstrFuncSig) evalString(row chunk.Row) (string, bool, error) {
	expr, isNull, err := re.args[0].EvalString(re.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	pat, isNull, err := re.args[1].EvalString(re.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	pos := int64(1)
	occurrence := int64(1)
	matchType := ""
	arg_num := len(re.args)
	var bexpr []byte

	if re.isBinCollation {
		bexpr = []byte(expr)
	}

	if arg_num >= 3 {
		pos, isNull, err = re.args[2].EvalInt(re.ctx, row)
		if isNull || err != nil {
			return "", true, err
		}

		// Check position and trim expr
		if re.isBinCollation {
			bexpr = []byte(expr)
			if pos < 1 || pos > int64(len(bexpr)) {
				if len(expr) != 0 || (len(expr) == 0 && pos != 1) {
					return "", true, ErrRegexp.GenWithStackByArgs(invalidIndex)
				}
			}

			bexpr = bexpr[pos-1:] // Trim
		} else {
			if pos < 1 || pos > int64(utf8.RuneCountInString(expr)) {
				if len(expr) != 0 || (len(expr) == 0 && pos != 1) {
					return "", true, ErrRegexp.GenWithStackByArgs(invalidIndex)
				}
			}

			trimUtf8String(&expr, pos-1) // Trim
		}
	}

	if arg_num >= 4 {
		occurrence, isNull, err = re.args[3].EvalInt(re.ctx, row)
		if isNull || err != nil {
			return "", true, err
		}

		if occurrence < 1 {
			occurrence = 1
		}
	}

	if arg_num == 5 {
		matchType, isNull, err = re.args[4].EvalString(re.ctx, row)
		if isNull || err != nil {
			return "", true, err
		}
	}

	re.compile, err = re.genCompile(matchType)
	if err != nil {
		return "", true, ErrRegexp.GenWithStackByArgs(err.Error())
	}

	reg, err := re.compile(pat)
	if err != nil {
		return "", true, ErrRegexp.GenWithStackByArgs(err.Error())
	}

	if re.isBinCollation {
		matches := reg.FindAll(bexpr, -1)
		length := int64(len(matches))
		if length == 0 || occurrence > length {
			return "", true, nil
		}

		return fmt.Sprintf("0x%s", strings.ToUpper(hex.EncodeToString(matches[occurrence-1]))), false, nil
	}

	matches := reg.FindAllString(expr, -1)
	length := int64(len(matches))
	if length == 0 || occurrence > length {
		return "", true, nil
	}

	return matches[occurrence-1], false, nil
}

// we need to memorize the regexp when:
//  1. pattern and match type are constant
//  2. pattern is const and match type is null
//
// return true: need, false: needless
func (re *regexpSubstrFuncSig) needMemorization() bool {
	return (re.args[1].ConstItem(re.ctx.GetSessionVars().StmtCtx) && (len(re.args) == 5 && re.args[4].ConstItem(re.ctx.GetSessionVars().StmtCtx))) && !re.isMemorizedRegexpInitialized()
}

// Call this function when at least one of the args is vector
// REGEXP_SUBSTR(expr, pat[, pos[, occurrence[, match_type]]])
func (re *regexpSubstrFuncSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	params := make([]*regexpParam, 0, 5)

	for i := 0; i < 2; i++ {
		param, err := buildStringParam(&re.baseBuiltinFunc, i, input, false)
		if err != nil {
			return err
		}
		params = append(params, param)
	}

	paramLen := len(re.args)

	// Handle position parameter
	hasPosition := (paramLen >= 3)
	param, err := buildIntParam(&re.baseBuiltinFunc, 2, input, !hasPosition, 1)
	params = append(params, param)
	defer releaseBuffers(&re.baseBuiltinFunc, params)

	if err != nil {
		return err
	}

	// Handle occurrence parameter
	hasOccur := (paramLen >= 4)
	param, err = buildIntParam(&re.baseBuiltinFunc, 3, input, !hasOccur, 1)
	params = append(params, param)

	if err != nil {
		return err
	}

	// Handle match type
	hasMatchType := (paramLen == 5)
	param, err = buildStringParam(&re.baseBuiltinFunc, 4, input, !hasMatchType)
	params = append(params, param)

	if err != nil {
		return err
	}

	// Check memorization
	if re.needMemorization() {
		// matchType must be const or null
		matchType := params[4].getStringVal(0)

		re.compile, err = re.genCompile(matchType)
		if err != nil {
			return err
		}

		re.initMemoizedRegexp(re.compile, params[1].getCol(), n)
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

		if re.isBinCollation {
			bexpr = []byte(expr)
		}

		// Check position and trim expr
		pos := params[2].getIntVal(i)
		if re.isBinCollation {
			bexpr = []byte(expr)
			if pos < 1 || pos > int64(len(bexpr)) {
				if len(bexpr) != 0 || (len(bexpr) == 0 && pos != 1) {
					return ErrRegexp.GenWithStackByArgs(invalidIndex)
				}
			}

			bexpr = bexpr[pos-1:] // Trim
		} else {
			if pos < 1 || pos > int64(utf8.RuneCountInString(expr)) {
				if len(expr) != 0 || (len(expr) == 0 && pos != 1) {
					return ErrRegexp.GenWithStackByArgs(invalidIndex)
				}
			}

			trimUtf8String(&expr, pos-1) // Trim
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
		if re.isBinCollation {
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
