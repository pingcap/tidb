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
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

type funcTimeOpForDateAddSub func(da *baseDateArithmetical, ctx EvalContext, date types.Time, interval, unit string, resultFsp int) (types.Time, bool, error)

func addTime(da *baseDateArithmetical, ctx EvalContext, date types.Time, interval, unit string, resultFsp int) (types.Time, bool, error) {
	return da.add(ctx, date, interval, unit, resultFsp)
}

func subTime(da *baseDateArithmetical, ctx EvalContext, date types.Time, interval, unit string, resultFsp int) (types.Time, bool, error) {
	return da.sub(ctx, date, interval, unit, resultFsp)
}

type funcDurationOpForDateAddSub func(da *baseDateArithmetical, ctx EvalContext, d types.Duration, interval, unit string, resultFsp int) (types.Duration, bool, error)

func addDuration(da *baseDateArithmetical, ctx EvalContext, d types.Duration, interval, unit string, resultFsp int) (types.Duration, bool, error) {
	return da.addDuration(ctx, d, interval, unit, resultFsp)
}

func subDuration(da *baseDateArithmetical, ctx EvalContext, d types.Duration, interval, unit string, resultFsp int) (types.Duration, bool, error) {
	return da.subDuration(ctx, d, interval, unit, resultFsp)
}

type funcSetPbCodeOp func(b builtinFunc, add, sub tipb.ScalarFuncSig)

func setAdd(b builtinFunc, add, sub tipb.ScalarFuncSig) {
	b.setPbCode(add)
}

func setSub(b builtinFunc, add, sub tipb.ScalarFuncSig) {
	b.setPbCode(sub)
}

type funcGetDateForDateAddSub func(da *baseDateArithmetical, ctx EvalContext, args []Expression, row chunk.Row, unit string) (types.Time, bool, error)

func getDateFromString(da *baseDateArithmetical, ctx EvalContext, args []Expression, row chunk.Row, unit string) (types.Time, bool, error) {
	return da.getDateFromString(ctx, args, row, unit)
}

func getDateFromInt(da *baseDateArithmetical, ctx EvalContext, args []Expression, row chunk.Row, unit string) (types.Time, bool, error) {
	return da.getDateFromInt(ctx, args, row, unit)
}

func getDateFromReal(da *baseDateArithmetical, ctx EvalContext, args []Expression, row chunk.Row, unit string) (types.Time, bool, error) {
	return da.getDateFromReal(ctx, args, row, unit)
}

func getDateFromDecimal(da *baseDateArithmetical, ctx EvalContext, args []Expression, row chunk.Row, unit string) (types.Time, bool, error) {
	return da.getDateFromDecimal(ctx, args, row, unit)
}

type funcVecGetDateForDateAddSub func(da *baseDateArithmetical, ctx EvalContext, b *baseBuiltinFunc, input *chunk.Chunk, unit string, result *chunk.Column) error

func vecGetDateFromString(da *baseDateArithmetical, ctx EvalContext, b *baseBuiltinFunc, input *chunk.Chunk, unit string, result *chunk.Column) error {
	return da.vecGetDateFromString(b, ctx, input, unit, result)
}

func vecGetDateFromInt(da *baseDateArithmetical, ctx EvalContext, b *baseBuiltinFunc, input *chunk.Chunk, unit string, result *chunk.Column) error {
	return da.vecGetDateFromInt(b, ctx, input, unit, result)
}

func vecGetDateFromReal(da *baseDateArithmetical, ctx EvalContext, b *baseBuiltinFunc, input *chunk.Chunk, unit string, result *chunk.Column) error {
	return da.vecGetDateFromReal(b, ctx, input, unit, result)
}

func vecGetDateFromDecimal(da *baseDateArithmetical, ctx EvalContext, b *baseBuiltinFunc, input *chunk.Chunk, unit string, result *chunk.Column) error {
	return da.vecGetDateFromDecimal(b, ctx, input, unit, result)
}

type funcGetIntervalForDateAddSub func(da *baseDateArithmetical, ctx EvalContext, args []Expression, row chunk.Row, unit string) (string, bool, error)

func getIntervalFromString(da *baseDateArithmetical, ctx EvalContext, args []Expression, row chunk.Row, unit string) (string, bool, error) {
	return da.getIntervalFromString(ctx, args, row, unit)
}

func getIntervalFromInt(da *baseDateArithmetical, ctx EvalContext, args []Expression, row chunk.Row, unit string) (string, bool, error) {
	return da.getIntervalFromInt(ctx, args, row, unit)
}

func getIntervalFromReal(da *baseDateArithmetical, ctx EvalContext, args []Expression, row chunk.Row, unit string) (string, bool, error) {
	return da.getIntervalFromReal(ctx, args, row, unit)
}

func getIntervalFromDecimal(da *baseDateArithmetical, ctx EvalContext, args []Expression, row chunk.Row, unit string) (string, bool, error) {
	return da.getIntervalFromDecimal(ctx, args, row, unit)
}

type funcVecGetIntervalForDateAddSub func(da *baseDateArithmetical, ctx EvalContext, b *baseBuiltinFunc, input *chunk.Chunk, unit string, result *chunk.Column) error

func vecGetIntervalFromString(da *baseDateArithmetical, ctx EvalContext, b *baseBuiltinFunc, input *chunk.Chunk, unit string, result *chunk.Column) error {
	return da.vecGetIntervalFromString(b, ctx, input, unit, result)
}

func vecGetIntervalFromInt(da *baseDateArithmetical, ctx EvalContext, b *baseBuiltinFunc, input *chunk.Chunk, unit string, result *chunk.Column) error {
	return da.vecGetIntervalFromInt(b, ctx, input, unit, result)
}

func vecGetIntervalFromReal(da *baseDateArithmetical, ctx EvalContext, b *baseBuiltinFunc, input *chunk.Chunk, unit string, result *chunk.Column) error {
	return da.vecGetIntervalFromReal(b, ctx, input, unit, result)
}

func vecGetIntervalFromDecimal(da *baseDateArithmetical, ctx EvalContext, b *baseBuiltinFunc, input *chunk.Chunk, unit string, result *chunk.Column) error {
	return da.vecGetIntervalFromDecimal(b, ctx, input, unit, result)
}

type addSubDateFunctionClass struct {
	baseFunctionClass
	timeOp      funcTimeOpForDateAddSub
	durationOp  funcDurationOpForDateAddSub
	setPbCodeOp funcSetPbCodeOp
}

func (c *addSubDateFunctionClass) getFunction(ctx BuildContext, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}

	dateEvalTp := args[0].GetType(ctx.GetEvalCtx()).EvalType()
	// Some special evaluation type treatment.
	// Note that it could be more elegant if we always evaluate datetime for int, real, decimal and string, by leveraging existing implicit casts.
	// However, MySQL has a weird behavior for date_add(string, ...), whose result depends on the content of the first argument.
	// E.g., date_add('2000-01-02 00:00:00', interval 1 day) evaluates to '2021-01-03 00:00:00' (which is normal),
	// whereas date_add('2000-01-02', interval 1 day) evaluates to '2000-01-03' instead of '2021-01-03 00:00:00'.
	// This requires a customized parsing of the content of the first argument, by recognizing if it is a pure date format or contains HMS part.
	// So implicit casts are not viable here.
	if dateEvalTp == types.ETTimestamp {
		dateEvalTp = types.ETDatetime
	} else if dateEvalTp == types.ETJson {
		dateEvalTp = types.ETString
	}

	intervalEvalTp := args[1].GetType(ctx.GetEvalCtx()).EvalType()
	if intervalEvalTp == types.ETJson {
		intervalEvalTp = types.ETString
	} else if intervalEvalTp != types.ETString && intervalEvalTp != types.ETDecimal && intervalEvalTp != types.ETReal {
		intervalEvalTp = types.ETInt
	}

	unit, _, err := args[2].EvalString(ctx.GetEvalCtx(), chunk.Row{})
	if err != nil {
		return nil, err
	}

	resultTp := mysql.TypeVarString
	resultEvalTp := types.ETString
	if args[0].GetType(ctx.GetEvalCtx()).GetType() == mysql.TypeDate {
		if !types.IsClockUnit(unit) {
			// First arg is date and unit contains no HMS, return date.
			resultTp = mysql.TypeDate
			resultEvalTp = types.ETDatetime
		} else {
			// First arg is date and unit contains HMS, return datetime.
			resultTp = mysql.TypeDatetime
			resultEvalTp = types.ETDatetime
		}
	} else if dateEvalTp == types.ETDuration {
		if types.IsDateUnit(unit) && unit != "DAY_MICROSECOND" {
			// First arg is time and unit contains YMD (except DAY_MICROSECOND), return datetime.
			resultTp = mysql.TypeDatetime
			resultEvalTp = types.ETDatetime
		} else {
			// First arg is time and unit contains no YMD or is DAY_MICROSECOND, return time.
			resultTp = mysql.TypeDuration
			resultEvalTp = types.ETDuration
		}
	} else if dateEvalTp == types.ETDatetime {
		// First arg is datetime or timestamp, return datetime.
		resultTp = mysql.TypeDatetime
		resultEvalTp = types.ETDatetime
	}

	argTps := []types.EvalType{dateEvalTp, intervalEvalTp, types.ETString}
	var bf baseBuiltinFunc
	bf, err = newBaseBuiltinFuncWithTp(ctx, c.funcName, args, resultEvalTp, argTps...)
	if err != nil {
		return nil, err
	}
	bf.tp.SetType(resultTp)

	var resultFsp int
	if types.IsMicrosecondUnit(unit) {
		resultFsp = types.MaxFsp
	} else {
		intervalFsp := types.MinFsp
		if unit == "SECOND" {
			if intervalEvalTp == types.ETString || intervalEvalTp == types.ETReal {
				intervalFsp = types.MaxFsp
			} else {
				intervalFsp = min(types.MaxFsp, args[1].GetType(ctx.GetEvalCtx()).GetDecimal())
			}
		}
		resultFsp = min(types.MaxFsp, max(args[0].GetType(ctx.GetEvalCtx()).GetDecimal(), intervalFsp))
	}
	switch resultTp {
	case mysql.TypeDate:
		bf.setDecimalAndFlenForDate()
	case mysql.TypeDuration:
		bf.setDecimalAndFlenForTime(resultFsp)
	case mysql.TypeDatetime:
		bf.setDecimalAndFlenForDatetime(resultFsp)
	case mysql.TypeVarString:
		bf.tp.SetFlen(mysql.MaxDatetimeFullWidth)
		bf.tp.SetDecimal(types.MinFsp)
	}

	switch {
	case dateEvalTp == types.ETString && intervalEvalTp == types.ETString:
		sig = &builtinAddSubDateAsStringSig{
			baseBuiltinFunc:      bf,
			baseDateArithmetical: newDateArithmeticalUtil(),
			getDate:              getDateFromString,
			vecGetDate:           vecGetDateFromString,
			getInterval:          getIntervalFromString,
			vecGetInterval:       vecGetIntervalFromString,
			timeOp:               c.timeOp,
		}
		c.setPbCodeOp(sig, tipb.ScalarFuncSig_AddDateStringString, tipb.ScalarFuncSig_SubDateStringString)
	case dateEvalTp == types.ETString && intervalEvalTp == types.ETInt:
		sig = &builtinAddSubDateAsStringSig{
			baseBuiltinFunc:      bf,
			baseDateArithmetical: newDateArithmeticalUtil(),
			getDate:              getDateFromString,
			vecGetDate:           vecGetDateFromString,
			getInterval:          getIntervalFromInt,
			vecGetInterval:       vecGetIntervalFromInt,
			timeOp:               c.timeOp,
		}
		c.setPbCodeOp(sig, tipb.ScalarFuncSig_AddDateStringInt, tipb.ScalarFuncSig_SubDateStringInt)
	case dateEvalTp == types.ETString && intervalEvalTp == types.ETReal:
		sig = &builtinAddSubDateAsStringSig{
			baseBuiltinFunc:      bf,
			baseDateArithmetical: newDateArithmeticalUtil(),
			getDate:              getDateFromString,
			vecGetDate:           vecGetDateFromString,
			getInterval:          getIntervalFromReal,
			vecGetInterval:       vecGetIntervalFromReal,
			timeOp:               c.timeOp,
		}
		c.setPbCodeOp(sig, tipb.ScalarFuncSig_AddDateStringReal, tipb.ScalarFuncSig_SubDateStringReal)
	case dateEvalTp == types.ETString && intervalEvalTp == types.ETDecimal:
		sig = &builtinAddSubDateAsStringSig{
			baseBuiltinFunc:      bf,
			baseDateArithmetical: newDateArithmeticalUtil(),
			getDate:              getDateFromString,
			vecGetDate:           vecGetDateFromString,
			getInterval:          getIntervalFromDecimal,
			vecGetInterval:       vecGetIntervalFromDecimal,
			timeOp:               c.timeOp,
		}
		c.setPbCodeOp(sig, tipb.ScalarFuncSig_AddDateStringDecimal, tipb.ScalarFuncSig_SubDateStringDecimal)
	case dateEvalTp == types.ETInt && intervalEvalTp == types.ETString:
		sig = &builtinAddSubDateAsStringSig{
			baseBuiltinFunc:      bf,
			baseDateArithmetical: newDateArithmeticalUtil(),
			getDate:              getDateFromInt,
			vecGetDate:           vecGetDateFromInt,
			getInterval:          getIntervalFromString,
			vecGetInterval:       vecGetIntervalFromString,
			timeOp:               c.timeOp,
		}
		c.setPbCodeOp(sig, tipb.ScalarFuncSig_AddDateIntString, tipb.ScalarFuncSig_SubDateIntString)
	case dateEvalTp == types.ETInt && intervalEvalTp == types.ETInt:
		sig = &builtinAddSubDateAsStringSig{
			baseBuiltinFunc:      bf,
			baseDateArithmetical: newDateArithmeticalUtil(),
			getDate:              getDateFromInt,
			vecGetDate:           vecGetDateFromInt,
			getInterval:          getIntervalFromInt,
			vecGetInterval:       vecGetIntervalFromInt,
			timeOp:               c.timeOp,
		}
		c.setPbCodeOp(sig, tipb.ScalarFuncSig_AddDateIntInt, tipb.ScalarFuncSig_SubDateIntInt)
	case dateEvalTp == types.ETInt && intervalEvalTp == types.ETReal:
		sig = &builtinAddSubDateAsStringSig{
			baseBuiltinFunc:      bf,
			baseDateArithmetical: newDateArithmeticalUtil(),
			getDate:              getDateFromInt,
			vecGetDate:           vecGetDateFromInt,
			getInterval:          getIntervalFromReal,
			vecGetInterval:       vecGetIntervalFromReal,
			timeOp:               c.timeOp,
		}
		c.setPbCodeOp(sig, tipb.ScalarFuncSig_AddDateIntReal, tipb.ScalarFuncSig_SubDateIntReal)
	case dateEvalTp == types.ETInt && intervalEvalTp == types.ETDecimal:
		sig = &builtinAddSubDateAsStringSig{
			baseBuiltinFunc:      bf,
			baseDateArithmetical: newDateArithmeticalUtil(),
			getDate:              getDateFromInt,
			vecGetDate:           vecGetDateFromInt,
			getInterval:          getIntervalFromDecimal,
			vecGetInterval:       vecGetIntervalFromDecimal,
			timeOp:               c.timeOp,
		}
		c.setPbCodeOp(sig, tipb.ScalarFuncSig_AddDateIntDecimal, tipb.ScalarFuncSig_SubDateIntDecimal)
	case dateEvalTp == types.ETReal && intervalEvalTp == types.ETString:
		sig = &builtinAddSubDateAsStringSig{
			baseBuiltinFunc:      bf,
			baseDateArithmetical: newDateArithmeticalUtil(),
			getDate:              getDateFromReal,
			vecGetDate:           vecGetDateFromReal,
			getInterval:          getIntervalFromString,
			vecGetInterval:       vecGetIntervalFromString,
			timeOp:               c.timeOp,
		}
		c.setPbCodeOp(sig, tipb.ScalarFuncSig_AddDateRealString, tipb.ScalarFuncSig_SubDateRealString)
	case dateEvalTp == types.ETReal && intervalEvalTp == types.ETInt:
		sig = &builtinAddSubDateAsStringSig{
			baseBuiltinFunc:      bf,
			baseDateArithmetical: newDateArithmeticalUtil(),
			getDate:              getDateFromReal,
			vecGetDate:           vecGetDateFromReal,
			getInterval:          getIntervalFromInt,
			vecGetInterval:       vecGetIntervalFromInt,
			timeOp:               c.timeOp,
		}
		c.setPbCodeOp(sig, tipb.ScalarFuncSig_AddDateRealInt, tipb.ScalarFuncSig_SubDateRealInt)
	case dateEvalTp == types.ETReal && intervalEvalTp == types.ETReal:
		sig = &builtinAddSubDateAsStringSig{
			baseBuiltinFunc:      bf,
			baseDateArithmetical: newDateArithmeticalUtil(),
			getDate:              getDateFromReal,
			vecGetDate:           vecGetDateFromReal,
			getInterval:          getIntervalFromReal,
			vecGetInterval:       vecGetIntervalFromReal,
			timeOp:               c.timeOp,
		}
		c.setPbCodeOp(sig, tipb.ScalarFuncSig_AddDateRealReal, tipb.ScalarFuncSig_SubDateRealReal)
	case dateEvalTp == types.ETReal && intervalEvalTp == types.ETDecimal:
		sig = &builtinAddSubDateAsStringSig{
			baseBuiltinFunc:      bf,
			baseDateArithmetical: newDateArithmeticalUtil(),
			getDate:              getDateFromReal,
			vecGetDate:           vecGetDateFromReal,
			getInterval:          getIntervalFromDecimal,
			vecGetInterval:       vecGetIntervalFromDecimal,
			timeOp:               c.timeOp,
		}
		c.setPbCodeOp(sig, tipb.ScalarFuncSig_AddDateRealDecimal, tipb.ScalarFuncSig_SubDateRealDecimal)
	case dateEvalTp == types.ETDecimal && intervalEvalTp == types.ETString:
		sig = &builtinAddSubDateAsStringSig{
			baseBuiltinFunc:      bf,
			baseDateArithmetical: newDateArithmeticalUtil(),
			getDate:              getDateFromDecimal,
			vecGetDate:           vecGetDateFromDecimal,
			getInterval:          getIntervalFromString,
			vecGetInterval:       vecGetIntervalFromString,
			timeOp:               c.timeOp,
		}
		c.setPbCodeOp(sig, tipb.ScalarFuncSig_AddDateDecimalString, tipb.ScalarFuncSig_SubDateDecimalString)
	case dateEvalTp == types.ETDecimal && intervalEvalTp == types.ETInt:
		sig = &builtinAddSubDateAsStringSig{
			baseBuiltinFunc:      bf,
			baseDateArithmetical: newDateArithmeticalUtil(),
			getDate:              getDateFromDecimal,
			vecGetDate:           vecGetDateFromDecimal,
			getInterval:          getIntervalFromInt,
			vecGetInterval:       vecGetIntervalFromInt,
			timeOp:               c.timeOp,
		}
		c.setPbCodeOp(sig, tipb.ScalarFuncSig_AddDateDecimalInt, tipb.ScalarFuncSig_SubDateDecimalInt)
	case dateEvalTp == types.ETDecimal && intervalEvalTp == types.ETReal:
		sig = &builtinAddSubDateAsStringSig{
			baseBuiltinFunc:      bf,
			baseDateArithmetical: newDateArithmeticalUtil(),
			getDate:              getDateFromDecimal,
			vecGetDate:           vecGetDateFromDecimal,
			getInterval:          getIntervalFromReal,
			vecGetInterval:       vecGetIntervalFromReal,
			timeOp:               c.timeOp,
		}
		c.setPbCodeOp(sig, tipb.ScalarFuncSig_AddDateDecimalReal, tipb.ScalarFuncSig_SubDateDecimalReal)
	case dateEvalTp == types.ETDecimal && intervalEvalTp == types.ETDecimal:
		sig = &builtinAddSubDateAsStringSig{
			baseBuiltinFunc:      bf,
			baseDateArithmetical: newDateArithmeticalUtil(),
			getDate:              getDateFromDecimal,
			vecGetDate:           vecGetDateFromDecimal,
			getInterval:          getIntervalFromDecimal,
			vecGetInterval:       vecGetIntervalFromDecimal,
			timeOp:               c.timeOp,
		}
		c.setPbCodeOp(sig, tipb.ScalarFuncSig_AddDateDecimalDecimal, tipb.ScalarFuncSig_SubDateDecimalDecimal)
	case dateEvalTp == types.ETDatetime && intervalEvalTp == types.ETString:
		sig = &builtinAddSubDateDatetimeAnySig{
			baseBuiltinFunc:      bf,
			baseDateArithmetical: newDateArithmeticalUtil(),
			getInterval:          getIntervalFromString,
			vecGetInterval:       vecGetIntervalFromString,
			timeOp:               c.timeOp,
		}
		c.setPbCodeOp(sig, tipb.ScalarFuncSig_AddDateDatetimeString, tipb.ScalarFuncSig_SubDateDatetimeString)
	case dateEvalTp == types.ETDatetime && intervalEvalTp == types.ETInt:
		sig = &builtinAddSubDateDatetimeAnySig{
			baseBuiltinFunc:      bf,
			baseDateArithmetical: newDateArithmeticalUtil(),
			getInterval:          getIntervalFromInt,
			vecGetInterval:       vecGetIntervalFromInt,
			timeOp:               c.timeOp,
		}
		c.setPbCodeOp(sig, tipb.ScalarFuncSig_AddDateDatetimeInt, tipb.ScalarFuncSig_SubDateDatetimeInt)
	case dateEvalTp == types.ETDatetime && intervalEvalTp == types.ETReal:
		sig = &builtinAddSubDateDatetimeAnySig{
			baseBuiltinFunc:      bf,
			baseDateArithmetical: newDateArithmeticalUtil(),
			getInterval:          getIntervalFromReal,
			vecGetInterval:       vecGetIntervalFromReal,
			timeOp:               c.timeOp,
		}
		c.setPbCodeOp(sig, tipb.ScalarFuncSig_AddDateDatetimeReal, tipb.ScalarFuncSig_SubDateDatetimeReal)
	case dateEvalTp == types.ETDatetime && intervalEvalTp == types.ETDecimal:
		sig = &builtinAddSubDateDatetimeAnySig{
			baseBuiltinFunc:      bf,
			baseDateArithmetical: newDateArithmeticalUtil(),
			getInterval:          getIntervalFromDecimal,
			vecGetInterval:       vecGetIntervalFromDecimal,
			timeOp:               c.timeOp,
		}
		c.setPbCodeOp(sig, tipb.ScalarFuncSig_AddDateDatetimeDecimal, tipb.ScalarFuncSig_SubDateDatetimeDecimal)
	case dateEvalTp == types.ETDuration && intervalEvalTp == types.ETString && resultTp == mysql.TypeDuration:
		sig = &builtinAddSubDateDurationAnySig{
			baseBuiltinFunc:      bf,
			baseDateArithmetical: newDateArithmeticalUtil(),
			getInterval:          getIntervalFromString,
			vecGetInterval:       vecGetIntervalFromString,
			timeOp:               c.timeOp,
			durationOp:           c.durationOp,
		}
		c.setPbCodeOp(sig, tipb.ScalarFuncSig_AddDateDurationString, tipb.ScalarFuncSig_SubDateDurationString)
	case dateEvalTp == types.ETDuration && intervalEvalTp == types.ETInt && resultTp == mysql.TypeDuration:
		sig = &builtinAddSubDateDurationAnySig{
			baseBuiltinFunc:      bf,
			baseDateArithmetical: newDateArithmeticalUtil(),
			getInterval:          getIntervalFromInt,
			vecGetInterval:       vecGetIntervalFromInt,
			timeOp:               c.timeOp,
			durationOp:           c.durationOp,
		}
		c.setPbCodeOp(sig, tipb.ScalarFuncSig_AddDateDurationInt, tipb.ScalarFuncSig_SubDateDurationInt)
	case dateEvalTp == types.ETDuration && intervalEvalTp == types.ETReal && resultTp == mysql.TypeDuration:
		sig = &builtinAddSubDateDurationAnySig{
			baseBuiltinFunc:      bf,
			baseDateArithmetical: newDateArithmeticalUtil(),
			getInterval:          getIntervalFromReal,
			vecGetInterval:       vecGetIntervalFromReal,
			timeOp:               c.timeOp,
			durationOp:           c.durationOp,
		}
		c.setPbCodeOp(sig, tipb.ScalarFuncSig_AddDateDurationReal, tipb.ScalarFuncSig_SubDateDurationReal)
	case dateEvalTp == types.ETDuration && intervalEvalTp == types.ETDecimal && resultTp == mysql.TypeDuration:
		sig = &builtinAddSubDateDurationAnySig{
			baseBuiltinFunc:      bf,
			baseDateArithmetical: newDateArithmeticalUtil(),
			getInterval:          getIntervalFromDecimal,
			vecGetInterval:       vecGetIntervalFromDecimal,
			timeOp:               c.timeOp,
			durationOp:           c.durationOp,
		}
		c.setPbCodeOp(sig, tipb.ScalarFuncSig_AddDateDurationDecimal, tipb.ScalarFuncSig_SubDateDurationDecimal)
	case dateEvalTp == types.ETDuration && intervalEvalTp == types.ETString && resultTp == mysql.TypeDatetime:
		sig = &builtinAddSubDateDurationAnySig{
			baseBuiltinFunc:      bf,
			baseDateArithmetical: newDateArithmeticalUtil(),
			getInterval:          getIntervalFromString,
			vecGetInterval:       vecGetIntervalFromString,
			timeOp:               c.timeOp,
			durationOp:           c.durationOp,
		}
		c.setPbCodeOp(sig, tipb.ScalarFuncSig_AddDateDurationStringDatetime, tipb.ScalarFuncSig_SubDateDurationStringDatetime)
	case dateEvalTp == types.ETDuration && intervalEvalTp == types.ETInt && resultTp == mysql.TypeDatetime:
		sig = &builtinAddSubDateDurationAnySig{
			baseBuiltinFunc:      bf,
			baseDateArithmetical: newDateArithmeticalUtil(),
			getInterval:          getIntervalFromInt,
			vecGetInterval:       vecGetIntervalFromInt,
			timeOp:               c.timeOp,
			durationOp:           c.durationOp,
		}
		c.setPbCodeOp(sig, tipb.ScalarFuncSig_AddDateDurationIntDatetime, tipb.ScalarFuncSig_SubDateDurationIntDatetime)
	case dateEvalTp == types.ETDuration && intervalEvalTp == types.ETReal && resultTp == mysql.TypeDatetime:
		sig = &builtinAddSubDateDurationAnySig{
			baseBuiltinFunc:      bf,
			baseDateArithmetical: newDateArithmeticalUtil(),
			getInterval:          getIntervalFromReal,
			vecGetInterval:       vecGetIntervalFromReal,
			timeOp:               c.timeOp,
			durationOp:           c.durationOp,
		}
		c.setPbCodeOp(sig, tipb.ScalarFuncSig_AddDateDurationRealDatetime, tipb.ScalarFuncSig_SubDateDurationRealDatetime)
	case dateEvalTp == types.ETDuration && intervalEvalTp == types.ETDecimal && resultTp == mysql.TypeDatetime:
		sig = &builtinAddSubDateDurationAnySig{
			baseBuiltinFunc:      bf,
			baseDateArithmetical: newDateArithmeticalUtil(),
			getInterval:          getIntervalFromDecimal,
			vecGetInterval:       vecGetIntervalFromDecimal,
			timeOp:               c.timeOp,
			durationOp:           c.durationOp,
		}
		c.setPbCodeOp(sig, tipb.ScalarFuncSig_AddDateDurationDecimalDatetime, tipb.ScalarFuncSig_SubDateDurationDecimalDatetime)
	}
	return sig, nil
}

type builtinAddSubDateAsStringSig struct {
	baseBuiltinFunc
	baseDateArithmetical
	getDate        funcGetDateForDateAddSub
	vecGetDate     funcVecGetDateForDateAddSub
	getInterval    funcGetIntervalForDateAddSub
	vecGetInterval funcVecGetIntervalForDateAddSub
	timeOp         funcTimeOpForDateAddSub
}

func (b *builtinAddSubDateAsStringSig) Clone() builtinFunc {
	newSig := &builtinAddSubDateAsStringSig{
		baseDateArithmetical: b.baseDateArithmetical,
		getDate:              b.getDate,
		vecGetDate:           b.vecGetDate,
		getInterval:          b.getInterval,
		vecGetInterval:       b.vecGetInterval,
		timeOp:               b.timeOp,
	}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinAddSubDateAsStringSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	unit, isNull, err := b.args[2].EvalString(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime.String(), true, err
	}

	date, isNull, err := b.getDate(&b.baseDateArithmetical, ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroTime.String(), true, err
	}
	if date.InvalidZero() {
		return types.ZeroTime.String(), true, handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, date.String()))
	}

	interval, isNull, err := b.getInterval(&b.baseDateArithmetical, ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroTime.String(), true, err
	}

	result, isNull, err := b.timeOp(&b.baseDateArithmetical, ctx, date, interval, unit, b.tp.GetDecimal())
	if result.Microsecond() == 0 {
		result.SetFsp(types.MinFsp)
	} else {
		result.SetFsp(types.MaxFsp)
	}

	return result.String(), isNull, err
}

type builtinAddSubDateDatetimeAnySig struct {
	baseBuiltinFunc
	baseDateArithmetical
	getInterval    funcGetIntervalForDateAddSub
	vecGetInterval funcVecGetIntervalForDateAddSub
	timeOp         funcTimeOpForDateAddSub
}

func (b *builtinAddSubDateDatetimeAnySig) Clone() builtinFunc {
	newSig := &builtinAddSubDateDatetimeAnySig{
		baseDateArithmetical: b.baseDateArithmetical,
		getInterval:          b.getInterval,
		vecGetInterval:       b.vecGetInterval,
		timeOp:               b.timeOp,
	}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinAddSubDateDatetimeAnySig) evalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
	unit, isNull, err := b.args[2].EvalString(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	date, isNull, err := b.getDateFromDatetime(ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	interval, isNull, err := b.getInterval(&b.baseDateArithmetical, ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	result, isNull, err := b.timeOp(&b.baseDateArithmetical, ctx, date, interval, unit, b.tp.GetDecimal())
	return result, isNull || err != nil, err
}

type builtinAddSubDateDurationAnySig struct {
	baseBuiltinFunc
	baseDateArithmetical
	getInterval    funcGetIntervalForDateAddSub
	vecGetInterval funcVecGetIntervalForDateAddSub
	timeOp         funcTimeOpForDateAddSub
	durationOp     funcDurationOpForDateAddSub
}

func (b *builtinAddSubDateDurationAnySig) Clone() builtinFunc {
	newSig := &builtinAddSubDateDurationAnySig{
		baseDateArithmetical: b.baseDateArithmetical,
		getInterval:          b.getInterval,
		vecGetInterval:       b.vecGetInterval,
		timeOp:               b.timeOp,
		durationOp:           b.durationOp,
	}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinAddSubDateDurationAnySig) evalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
	unit, isNull, err := b.args[2].EvalString(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	d, isNull, err := b.args[0].EvalDuration(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	interval, isNull, err := b.getInterval(&b.baseDateArithmetical, ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	tc := typeCtx(ctx)
	t, err := d.ConvertToTime(tc, mysql.TypeDatetime)
	if err != nil {
		return types.ZeroTime, true, err
	}
	result, isNull, err := b.timeOp(&b.baseDateArithmetical, ctx, t, interval, unit, b.tp.GetDecimal())
	return result, isNull || err != nil, err
}

func (b *builtinAddSubDateDurationAnySig) evalDuration(ctx EvalContext, row chunk.Row) (types.Duration, bool, error) {
	unit, isNull, err := b.args[2].EvalString(ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, true, err
	}

	dur, isNull, err := b.args[0].EvalDuration(ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, true, err
	}

	interval, isNull, err := b.getInterval(&b.baseDateArithmetical, ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroDuration, true, err
	}

	result, isNull, err := b.durationOp(&b.baseDateArithmetical, ctx, dur, interval, unit, b.tp.GetDecimal())
	return result, isNull || err != nil, err
}

