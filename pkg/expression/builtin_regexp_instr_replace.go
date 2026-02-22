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
	"regexp"
	"unicode/utf8"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/stringutil"
	"github.com/pingcap/tipb/go-tipb"
)

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
	for i := range n {
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
		occurrence := max(params[3].getIntVal(i), 1)

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
