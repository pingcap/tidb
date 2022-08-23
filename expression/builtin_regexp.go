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
	"regexp"

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

type InvalidMatchType struct{}

func (i *InvalidMatchType) Error() string {
	return "Invalid match type"
}

var validMatchType = map[string]empty{
	flag_i: {}, // Case-insensitive matching
	flag_c: {}, // Case-sensitive matching
	flag_m: {}, // Multiple-line mode
	flag_n: {}, // The . character matches line terminators
}

type regexpBaseFuncSig struct {
	regexpMemorizedSig
	compile func(string) (*regexp.Regexp, error)
}

func (re *regexpBaseFuncSig) cloneBase(from *regexpBaseFuncSig) {
	if from.memorizedRegexp != nil {
		re.memorizedRegexp = from.memorizedRegexp.Copy()
	}
	re.memorizedErr = from.memorizedErr
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
			return "", &InvalidMatchType{}
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
	sig := regexpLikeFuncSig{baseBuiltinFunc: bf}
	if bf.collation == charset.CollationBin {
		sig.setPbCode(tipb.ScalarFuncSig_RegexpLikeSig)
	} else {
		sig.setPbCode(tipb.ScalarFuncSig_RegexpLikeUTF8Sig)
	}

	return &sig, nil
}

type regexpLikeFuncSig struct {
	baseBuiltinFunc
	regexpBaseFuncSig
}

func (re *regexpLikeFuncSig) vectorized() bool {
	return true
}

func (re *regexpLikeFuncSig) clone(from *regexpLikeFuncSig) {
	re.cloneFrom(&from.baseBuiltinFunc)
	re.cloneBase(&from.regexpBaseFuncSig)
}

// To get a unified compile interface in initMemoizedRegexp, we need to process many things in genCompile
func (re *regexpLikeFuncSig) genCompile(matchType string) (func(string) (*regexp.Regexp, error), error) {
	matchType, err := re.getMatchType(&re.baseBuiltinFunc, matchType)
	if err != nil {
		return nil, err
	}

	return func(pat string) (*regexp.Regexp, error) {
		return regexp.Compile(fmt.Sprintf("(?%s)%s", matchType, pat))
	}, nil
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
//   1. pattern and match type are constant
//   2. pattern is const and match type is null
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
	if err != nil {
		return err
	}

	params = append(params, param)
	defer releaseBuffers(&re.baseBuiltinFunc, params)

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

	getRegexp := func(pat string, matchType string) (*regexp.Regexp, error) {
		if re.isMemorizedRegexpInitialized() {
			return re.memorizedRegexp, re.memorizedErr
		}

		// Generate compiler first
		re.compile, err = re.genCompile(matchType)
		if err != nil {
			return nil, err
		}

		return re.compile(pat)
	}

	result.ResizeInt64(n, false)
	result.MergeNulls(getBuffers(params)...)
	i64s := result.Int64s()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}

		matchType := params[2].getStringVal(i)
		re, err := getRegexp(params[1].getStringVal(i), matchType)
		if err != nil {
			return err
		}
		i64s[i] = boolToInt64(re.MatchString(params[0].getStringVal(i)))
	}
	return nil
}
