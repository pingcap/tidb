// Copyright 2015 PingCAP, Inc.
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
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

func (c *timestampDiffFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString, types.ETDatetime, types.ETDatetime)
	if err != nil {
		return nil, err
	}
	sig := &builtinTimestampDiffSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_TimestampDiff)
	return sig, nil
}

type builtinTimestampDiffSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinTimestampDiffSig) Clone() builtinFunc {
	newSig := &builtinTimestampDiffSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinTimestampDiffSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timestampdiff
func (b *builtinTimestampDiffSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	unit, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	lhs, isNull, err := b.args[1].EvalTime(ctx, row)
	if isNull || err != nil {
		return 0, isNull, handleInvalidTimeError(ctx, err)
	}
	rhs, isNull, err := b.args[2].EvalTime(ctx, row)
	if isNull || err != nil {
		return 0, isNull, handleInvalidTimeError(ctx, err)
	}
	if invalidLHS, invalidRHS := lhs.InvalidZero(), rhs.InvalidZero(); invalidLHS || invalidRHS {
		if invalidLHS {
			err = handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, lhs.String()))
		}
		if invalidRHS {
			err = handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, rhs.String()))
		}
		return 0, true, err
	}
	return types.TimestampDiff(unit, lhs, rhs), false, nil
}

type unixTimestampFunctionClass struct {
	baseFunctionClass
}

func (c *unixTimestampFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	var (
		argTps              []types.EvalType
		retTp               types.EvalType
		retFLen, retDecimal int
	)

	if len(args) == 0 {
		retTp, retDecimal = types.ETInt, 0
	} else {
		argTps = []types.EvalType{types.ETDatetime}
		argType := args[0].GetType(ctx.GetEvalCtx())
		argEvaltp := argType.EvalType()
		if argEvaltp == types.ETString {
			// Treat types.ETString as unspecified decimal.
			retDecimal = types.UnspecifiedLength
			if cnst, ok := args[0].(*Constant); ok {
				tmpStr, _, err := cnst.EvalString(ctx.GetEvalCtx(), chunk.Row{})
				if err != nil {
					return nil, err
				}
				retDecimal = 0
				if dotIdx := strings.LastIndex(tmpStr, "."); dotIdx >= 0 {
					retDecimal = len(tmpStr) - dotIdx - 1
				}
			}
		} else {
			retDecimal = argType.GetDecimal()
		}
		if retDecimal > 6 || retDecimal == types.UnspecifiedLength {
			retDecimal = 6
		}
		if retDecimal == 0 {
			retTp = types.ETInt
		} else {
			retTp = types.ETDecimal
		}
	}
	if retTp == types.ETInt {
		retFLen = 11
	} else if retTp == types.ETDecimal {
		retFLen = 12 + retDecimal
	} else {
		panic("Unexpected retTp")
	}

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, retTp, argTps...)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlenUnderLimit(retFLen)
	bf.tp.SetDecimalUnderLimit(retDecimal)

	var sig builtinFunc
	if len(args) == 0 {
		sig = &builtinUnixTimestampCurrentSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_UnixTimestampCurrent)
	} else if retTp == types.ETInt {
		sig = &builtinUnixTimestampIntSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_UnixTimestampInt)
	} else if retTp == types.ETDecimal {
		sig = &builtinUnixTimestampDecSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_UnixTimestampDec)
	}
	return sig, nil
}

// goTimeToMysqlUnixTimestamp converts go time into MySQL's Unix timestamp.
// MySQL's Unix timestamp ranges from '1970-01-01 00:00:01.000000' UTC to '3001-01-18 23:59:59.999999' UTC. Values out of range should be rewritten to 0.
// https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_unix-timestamp
func goTimeToMysqlUnixTimestamp(t time.Time, decimal int) (*types.MyDecimal, error) {
	microSeconds := t.UnixMicro()
	// Prior to MySQL 8.0.28 (or any 32-bit platform), the valid range of argument values is the same as for the TIMESTAMP data type:
	// '1970-01-01 00:00:01.000000' UTC to '2038-01-19 03:14:07.999999' UTC.
	// After 8.0.28, the range has been extended to '1970-01-01 00:00:01.000000' UTC to '3001-01-18 23:59:59.999999' UTC
	// The magic value of '3001-01-18 23:59:59.999999' comes from the maximum supported timestamp on windows. Though TiDB
	// doesn't support windows, this value is used here to keep the compatibility with MySQL
	if microSeconds < 1e6 || microSeconds > 32536771199999999 {
		return new(types.MyDecimal), nil
	}
	dec := new(types.MyDecimal)
	// Here we don't use float to prevent precision lose.
	dec.FromUint(uint64(microSeconds))
	err := dec.Shift(-6)
	if err != nil {
		return nil, err
	}

	// In MySQL's implementation, unix_timestamp() will truncate the result instead of rounding it.
	// Results below are from MySQL 5.7, which can prove it.
	//	mysql> select unix_timestamp(), unix_timestamp(now(0)), now(0), unix_timestamp(now(3)), now(3), now(6);
	//	+------------------+------------------------+---------------------+------------------------+-------------------------+----------------------------+
	//	| unix_timestamp() | unix_timestamp(now(0)) | now(0)              | unix_timestamp(now(3)) | now(3)                  | now(6)                     |
	//	+------------------+------------------------+---------------------+------------------------+-------------------------+----------------------------+
	//	|       1553503194 |             1553503194 | 2019-03-25 16:39:54 |         1553503194.992 | 2019-03-25 16:39:54.992 | 2019-03-25 16:39:54.992969 |
	//	+------------------+------------------------+---------------------+------------------------+-------------------------+----------------------------+
	err = dec.Round(dec, decimal, types.ModeTruncate)
	return dec, err
}

type builtinUnixTimestampCurrentSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinUnixTimestampCurrentSig) Clone() builtinFunc {
	newSig := &builtinUnixTimestampCurrentSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a UNIX_TIMESTAMP().
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_unix-timestamp
func (b *builtinUnixTimestampCurrentSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	nowTs, err := getStmtTimestamp(ctx)
	if err != nil {
		return 0, true, err
	}
	dec, err := goTimeToMysqlUnixTimestamp(nowTs, 1)
	if err != nil {
		return 0, true, err
	}
	intVal, err := dec.ToInt()
	if !terror.ErrorEqual(err, types.ErrTruncated) {
		terror.Log(err)
	}
	return intVal, false, nil
}

type builtinUnixTimestampIntSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinUnixTimestampIntSig) Clone() builtinFunc {
	newSig := &builtinUnixTimestampIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a UNIX_TIMESTAMP(time).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_unix-timestamp
func (b *builtinUnixTimestampIntSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	val, isNull, err := b.args[0].EvalTime(ctx, row)
	if err != nil && terror.ErrorEqual(types.ErrWrongValue.GenWithStackByArgs(types.TimeStr, val), err) {
		// Return 0 for invalid date time.
		return 0, false, nil
	}
	if isNull {
		return 0, true, nil
	}

	tz := location(ctx)
	t, err := val.AdjustedGoTime(tz)
	if err != nil {
		return 0, false, nil
	}
	dec, err := goTimeToMysqlUnixTimestamp(t, 1)
	if err != nil {
		return 0, true, err
	}
	intVal, err := dec.ToInt()
	if !terror.ErrorEqual(err, types.ErrTruncated) {
		terror.Log(err)
	}
	return intVal, false, nil
}

type builtinUnixTimestampDecSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinUnixTimestampDecSig) Clone() builtinFunc {
	newSig := &builtinUnixTimestampDecSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDecimal evals a UNIX_TIMESTAMP(time).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_unix-timestamp
func (b *builtinUnixTimestampDecSig) evalDecimal(ctx EvalContext, row chunk.Row) (*types.MyDecimal, bool, error) {
	val, isNull, err := b.args[0].EvalTime(ctx, row)
	if isNull || err != nil {
		// Return 0 for invalid date time.
		return new(types.MyDecimal), isNull, nil
	}
	t, err := val.GoTime(getTimeZone(ctx))
	if err != nil {
		return new(types.MyDecimal), false, nil
	}
	result, err := goTimeToMysqlUnixTimestamp(t, b.tp.GetDecimal())
	return result, err != nil, err
}

type timestampFunctionClass struct {
	baseFunctionClass
}

func (c *timestampFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	evalTps, argLen := []types.EvalType{types.ETString}, len(args)
	if argLen == 2 {
		evalTps = append(evalTps, types.ETString)
	}
	fsp, err := getExpressionFsp(ctx, args[0])
	if err != nil {
		return nil, err
	}
	if argLen == 2 {
		fsp2, err := getExpressionFsp(ctx, args[1])
		if err != nil {
			return nil, err
		}
		if fsp2 > fsp {
			fsp = fsp2
		}
	}
	isFloat := false
	switch args[0].GetType(ctx.GetEvalCtx()).GetType() {
	case mysql.TypeFloat, mysql.TypeDouble, mysql.TypeNewDecimal, mysql.TypeLonglong:
		isFloat = true
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDatetime, evalTps...)
	if err != nil {
		return nil, err
	}
	bf.setDecimalAndFlenForDatetime(fsp)
	var sig builtinFunc
	if argLen == 2 {
		sig = &builtinTimestamp2ArgsSig{bf, isFloat}
		sig.setPbCode(tipb.ScalarFuncSig_Timestamp2Args)
	} else {
		sig = &builtinTimestamp1ArgSig{bf, isFloat}
		sig.setPbCode(tipb.ScalarFuncSig_Timestamp1Arg)
	}
	return sig, nil
}

type builtinTimestamp1ArgSig struct {
	baseBuiltinFunc

	isFloat bool
}

func (b *builtinTimestamp1ArgSig) Clone() builtinFunc {
	newSig := &builtinTimestamp1ArgSig{isFloat: b.isFloat}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals a builtinTimestamp1ArgSig.
// See https://dev.mysql.com/doc/refman/5.5/en/date-and-time-functions.html#function_timestamp
func (b *builtinTimestamp1ArgSig) evalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
	s, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, isNull, err
	}
	var tm types.Time
	tc := typeCtx(ctx)
	if b.isFloat {
		tm, err = types.ParseTimeFromFloatString(tc, s, mysql.TypeDatetime, types.GetFsp(s))
	} else {
		tm, err = types.ParseTime(tc, s, mysql.TypeDatetime, types.GetFsp(s))
	}
	if err != nil {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, err)
	}
	return tm, false, nil
}

type builtinTimestamp2ArgsSig struct {
	baseBuiltinFunc

	isFloat bool
}

func (b *builtinTimestamp2ArgsSig) Clone() builtinFunc {
	newSig := &builtinTimestamp2ArgsSig{isFloat: b.isFloat}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals a builtinTimestamp2ArgsSig.
// See https://dev.mysql.com/doc/refman/5.5/en/date-and-time-functions.html#function_timestamp
func (b *builtinTimestamp2ArgsSig) evalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
	arg0, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, isNull, err
	}
	var tm types.Time
	tc := typeCtx(ctx)
	if b.isFloat {
		tm, err = types.ParseTimeFromFloatString(tc, arg0, mysql.TypeDatetime, types.GetFsp(arg0))
	} else {
		tm, err = types.ParseTime(tc, arg0, mysql.TypeDatetime, types.GetFsp(arg0))
	}
	if err != nil {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, err)
	}
	if tm.Year() == 0 {
		// MySQL won't evaluate add for date with zero year.
		// See https://github.com/mysql/mysql-server/blob/5.7/sql/item_timefunc.cc#L2805
		return types.ZeroTime, true, nil
	}
	arg1, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, isNull, err
	}
	if !isDuration(arg1) {
		return types.ZeroTime, true, nil
	}
	duration, _, err := types.ParseDuration(tc, arg1, types.GetFsp(arg1))
	if err != nil {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, err)
	}
	tmp, err := tm.Add(tc, duration)
	if err != nil {
		return types.ZeroTime, true, err
	}
	return tmp, false, nil
}

type timestampLiteralFunctionClass struct {
	baseFunctionClass
}

func (c *timestampLiteralFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	con, ok := args[0].(*Constant)
	if !ok {
		panic("Unexpected parameter for timestamp literal")
	}
	dt, err := con.Eval(ctx.GetEvalCtx(), chunk.Row{})
	if err != nil {
		return nil, err
	}
	str, err := dt.ToString()
	if err != nil {
		return nil, err
	}
	if !timestampPattern.MatchString(str) {
		return nil, types.ErrWrongValue2.GenWithStackByArgs(types.DateTimeStr, str)
	}
	tm, err := types.ParseTime(ctx.GetEvalCtx().TypeCtx(), str, mysql.TypeDatetime, types.GetFsp(str))
	if err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, []Expression{}, types.ETDatetime)
	if err != nil {
		return nil, err
	}
	bf.setDecimalAndFlenForDatetime(tm.Fsp())
	sig := &builtinTimestampLiteralSig{bf, tm}
	return sig, nil
}

type builtinTimestampLiteralSig struct {
	baseBuiltinFunc
	tm types.Time
}

func (b *builtinTimestampLiteralSig) Clone() builtinFunc {
	newSig := &builtinTimestampLiteralSig{tm: b.tm}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals TIMESTAMP 'stringLit'.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-literals.html
func (b *builtinTimestampLiteralSig) evalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
	return b.tm, false, nil
}

// getFsp4TimeAddSub is used to in function 'ADDTIME' and 'SUBTIME' to evaluate `fsp` for the
// second parameter. It's used only if the second parameter is of string type. It's different
// from getFsp in that the result of getFsp4TimeAddSub is either 6 or 0.
func getFsp4TimeAddSub(s string) int {
	if len(s)-strings.Index(s, ".")-1 == len(s) {
		return types.MinFsp
	}
	for _, c := range s[strings.Index(s, ".")+1:] {
		if c != '0' {
			return types.MaxFsp
		}
	}
	return types.MinFsp
}

// getBf4TimeAddSub parses input types, generates baseBuiltinFunc and set related attributes for
// builtin function 'ADDTIME' and 'SUBTIME'
func getBf4TimeAddSub(ctx BuildContext, funcName string, args []Expression) (tp1, tp2 *types.FieldType, bf baseBuiltinFunc, err error) {
	tp1, tp2 = args[0].GetType(ctx.GetEvalCtx()), args[1].GetType(ctx.GetEvalCtx())
	var argTp1, argTp2, retTp types.EvalType
	switch tp1.GetType() {
	case mysql.TypeDatetime, mysql.TypeTimestamp:
		argTp1, retTp = types.ETDatetime, types.ETDatetime
	case mysql.TypeDuration:
		argTp1, retTp = types.ETDuration, types.ETDuration
	case mysql.TypeDate:
		argTp1, retTp = types.ETDatetime, types.ETString
	default:
		argTp1, retTp = types.ETString, types.ETString
	}
	switch tp2.GetType() {
	case mysql.TypeDatetime, mysql.TypeDuration:
		argTp2 = types.ETDuration
	default:
		argTp2 = types.ETString
	}
	arg0Dec, err := getExpressionFsp(ctx, args[0])
	if err != nil {
		return
	}
	arg1Dec, err := getExpressionFsp(ctx, args[1])
	if err != nil {
		return
	}

	bf, err = newBaseBuiltinFuncWithTp(ctx, funcName, args, retTp, argTp1, argTp2)
	if err != nil {
		return
	}
	switch retTp {
	case types.ETDatetime:
		bf.setDecimalAndFlenForDatetime(min(max(arg0Dec, arg1Dec), types.MaxFsp))
	case types.ETDuration:
		bf.setDecimalAndFlenForTime(min(max(arg0Dec, arg1Dec), types.MaxFsp))
	case types.ETString:
		bf.tp.SetType(mysql.TypeString)
		bf.tp.SetFlen(mysql.MaxDatetimeWidthWithFsp)
		bf.tp.SetDecimal(types.UnspecifiedLength)
	}
	return
}

func getTimeZone(ctx EvalContext) *time.Location {
	ret := location(ctx)
	if ret == nil {
		ret = time.Local
	}
	return ret
}

// isDuration returns a boolean indicating whether the str matches the format of duration.
// See https://dev.mysql.com/doc/refman/5.7/en/time.html
func isDuration(str string) bool {
	return durationPattern.MatchString(str)
}

// strDatetimeAddDuration adds duration to datetime string, returns a string value.
func strDatetimeAddDuration(tc types.Context, d string, arg1 types.Duration) (result string, isNull bool, err error) {
	arg0, err := types.ParseTime(tc, d, mysql.TypeDatetime, types.MaxFsp)
	if err != nil {
		// Return a warning regardless of the sql_mode, this is compatible with MySQL.
		tc.AppendWarning(err)
		return "", true, nil
	}
	ret, err := arg0.Add(tc, arg1)
	if err != nil {
		return "", false, err
	}
	fsp := types.MaxFsp
	if ret.Microsecond() == 0 {
		fsp = types.MinFsp
	}
	ret.SetFsp(fsp)
	return ret.String(), false, nil
}

// strDurationAddDuration adds duration to duration string, returns a string value.
func strDurationAddDuration(tc types.Context, d string, arg1 types.Duration) (string, error) {
	arg0, _, err := types.ParseDuration(tc, d, types.MaxFsp)
	if err != nil {
		return "", err
	}
	tmpDuration, err := arg0.Add(arg1)
	if err != nil {
		return "", err
	}
	tmpDuration.Fsp = types.MaxFsp
	if tmpDuration.MicroSecond() == 0 {
		tmpDuration.Fsp = types.MinFsp
	}
	return tmpDuration.String(), nil
}

// strDatetimeSubDuration subtracts duration from datetime string, returns a string value.
func strDatetimeSubDuration(tc types.Context, d string, arg1 types.Duration) (result string, isNull bool, err error) {
	arg0, err := types.ParseTime(tc, d, mysql.TypeDatetime, types.MaxFsp)
	if err != nil {
		// Return a warning regardless of the sql_mode, this is compatible with MySQL.
		tc.AppendWarning(err)
		return "", true, nil
	}
	resultTime, err := arg0.Add(tc, arg1.Neg())
	if err != nil {
		return "", false, err
	}
	fsp := types.MaxFsp
	if resultTime.Microsecond() == 0 {
		fsp = types.MinFsp
	}
	resultTime.SetFsp(fsp)
	return resultTime.String(), false, nil
}

// strDurationSubDuration subtracts duration from duration string, returns a string value.
func strDurationSubDuration(tc types.Context, d string, arg1 types.Duration) (string, error) {
	arg0, _, err := types.ParseDuration(tc, d, types.MaxFsp)
	if err != nil {
		return "", err
	}
	tmpDuration, err := arg0.Sub(arg1)
	if err != nil {
		return "", err
	}
	tmpDuration.Fsp = types.MaxFsp
	if tmpDuration.MicroSecond() == 0 {
		tmpDuration.Fsp = types.MinFsp
	}
	return tmpDuration.String(), nil
}
