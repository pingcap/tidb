// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"strconv"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

// Vectorizable checks whether a list of expressions can employ vectorized execution.
func Vectorizable(exprs []Expression) bool {
	for _, expr := range exprs {
		if hasUnVectorizableFunc(expr) {
			return false
		}
	}
	return true
}

// hasUnVectorizableFunc checks whether an expression contains functions that can not utilize the vectorized execution.
func hasUnVectorizableFunc(expr Expression) bool {
	scalaFunc, ok := expr.(*ScalarFunction)
	if !ok {
		return false
	}
	if scalaFunc.FuncName.L == ast.SetVar {
		return true
	}
	for _, arg := range scalaFunc.GetArgs() {
		if hasUnVectorizableFunc(arg) {
			return true
		}
	}
	return false
}

// VectorizedExecute evaluates a list of expressions column by column and append their results to "output" Chunk.
func VectorizedExecute(ctx context.Context, exprs []Expression, input, output *chunk.Chunk) error {
	sc := ctx.GetSessionVars().StmtCtx
	for colID, expr := range exprs {
		err := evalOneColumn(sc, expr, input, output, colID)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func evalOneColumn(sc *stmtctx.StatementContext, expr Expression, input, output *chunk.Chunk, colID int) (err error) {
	switch fieldType, evalType := expr.GetType(), expr.GetType().EvalType(); evalType {
	case types.ETInt:
		for row := input.Begin(); err == nil && row != input.End(); row = row.Next() {
			err = executeToInt(sc, expr, fieldType, row, output, colID)
		}
	case types.ETReal:
		for row := input.Begin(); err == nil && row != input.End(); row = row.Next() {
			err = executeToReal(sc, expr, fieldType, row, output, colID)
		}
	case types.ETDecimal:
		for row := input.Begin(); err == nil && row != input.End(); row = row.Next() {
			err = executeToDecimal(sc, expr, fieldType, row, output, colID)
		}
	case types.ETDatetime, types.ETTimestamp:
		for row := input.Begin(); err == nil && row != input.End(); row = row.Next() {
			err = executeToDatetime(sc, expr, fieldType, row, output, colID)
		}
	case types.ETDuration:
		for row := input.Begin(); err == nil && row != input.End(); row = row.Next() {
			err = executeToDuration(sc, expr, fieldType, row, output, colID)
		}
	case types.ETJson:
		for row := input.Begin(); err == nil && row != input.End(); row = row.Next() {
			err = executeToJSON(sc, expr, fieldType, row, output, colID)
		}
	case types.ETString:
		for row := input.Begin(); err == nil && row != input.End(); row = row.Next() {
			err = executeToString(sc, expr, fieldType, row, output, colID)
		}
	}
	return errors.Trace(err)
}

// UnVectorizedExecute evaluates a list of expressions row by row and append their results to "output" Chunk.
func UnVectorizedExecute(ctx context.Context, exprs []Expression, input, output *chunk.Chunk) error {
	sc := ctx.GetSessionVars().StmtCtx
	for row := input.Begin(); row != input.End(); row = row.Next() {
		for colID, expr := range exprs {
			err := evalOneCell(sc, expr, row, output, colID)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

func evalOneCell(sc *stmtctx.StatementContext, expr Expression, row chunk.Row, output *chunk.Chunk, colID int) (err error) {
	switch fieldType, evalType := expr.GetType(), expr.GetType().EvalType(); evalType {
	case types.ETInt:
		err = executeToInt(sc, expr, fieldType, row, output, colID)
	case types.ETReal:
		err = executeToReal(sc, expr, fieldType, row, output, colID)
	case types.ETDecimal:
		err = executeToDecimal(sc, expr, fieldType, row, output, colID)
	case types.ETDatetime, types.ETTimestamp:
		err = executeToDatetime(sc, expr, fieldType, row, output, colID)
	case types.ETDuration:
		err = executeToDuration(sc, expr, fieldType, row, output, colID)
	case types.ETJson:
		err = executeToJSON(sc, expr, fieldType, row, output, colID)
	case types.ETString:
		err = executeToString(sc, expr, fieldType, row, output, colID)
	}
	return errors.Trace(err)
}

func executeToInt(sc *stmtctx.StatementContext, expr Expression, fieldType *types.FieldType, row chunk.Row, output *chunk.Chunk, colID int) error {
	res, isNull, err := expr.EvalInt(row, sc)
	if err != nil {
		return errors.Trace(err)
	}
	if isNull {
		output.AppendNull(colID)
	} else if fieldType.Tp == mysql.TypeBit {
		output.AppendBytes(colID, strconv.AppendUint(make([]byte, 0, 8), uint64(res), 10))
	} else if mysql.HasUnsignedFlag(fieldType.Flag) {
		output.AppendUint64(colID, uint64(res))
	} else {
		output.AppendInt64(colID, res)
	}
	return nil
}

func executeToReal(sc *stmtctx.StatementContext, expr Expression, fieldType *types.FieldType, row chunk.Row, output *chunk.Chunk, colID int) error {
	res, isNull, err := expr.EvalReal(row, sc)
	if err != nil {
		return errors.Trace(err)
	}
	if isNull {
		output.AppendNull(colID)
	} else if fieldType.Tp == mysql.TypeFloat {
		output.AppendFloat32(colID, float32(res))
	} else {
		output.AppendFloat64(colID, res)
	}
	return nil
}

func executeToDecimal(sc *stmtctx.StatementContext, expr Expression, fieldType *types.FieldType, row chunk.Row, output *chunk.Chunk, colID int) error {
	res, isNull, err := expr.EvalDecimal(row, sc)
	if err != nil {
		return errors.Trace(err)
	}
	if isNull {
		output.AppendNull(colID)
	} else {
		output.AppendMyDecimal(colID, res)
	}
	return nil
}

func executeToDatetime(sc *stmtctx.StatementContext, expr Expression, fieldType *types.FieldType, row chunk.Row, output *chunk.Chunk, colID int) error {
	res, isNull, err := expr.EvalTime(row, sc)
	if err != nil {
		return errors.Trace(err)
	}
	if isNull {
		output.AppendNull(colID)
	} else {
		output.AppendTime(colID, res)
	}
	return nil
}

func executeToDuration(sc *stmtctx.StatementContext, expr Expression, fieldType *types.FieldType, row chunk.Row, output *chunk.Chunk, colID int) error {
	res, isNull, err := expr.EvalDuration(row, sc)
	if err != nil {
		return errors.Trace(err)
	}
	if isNull {
		output.AppendNull(colID)
	} else {
		output.AppendDuration(colID, res)
	}
	return nil
}

func executeToJSON(sc *stmtctx.StatementContext, expr Expression, fieldType *types.FieldType, row chunk.Row, output *chunk.Chunk, colID int) error {
	res, isNull, err := expr.EvalJSON(row, sc)
	if err != nil {
		return errors.Trace(err)
	}
	if isNull {
		output.AppendNull(colID)
	} else {
		output.AppendJSON(colID, res)
	}
	return nil
}

func executeToString(sc *stmtctx.StatementContext, expr Expression, fieldType *types.FieldType, row chunk.Row, output *chunk.Chunk, colID int) error {
	res, isNull, err := expr.EvalString(row, sc)
	if err != nil {
		return errors.Trace(err)
	}
	if isNull {
		output.AppendNull(colID)
	} else if fieldType.Tp == mysql.TypeEnum {
		val := types.Enum{Value: uint64(0), Name: res}
		output.AppendEnum(colID, val)
	} else if fieldType.Tp == mysql.TypeSet {
		val := types.Set{Value: uint64(0), Name: res}
		output.AppendSet(colID, val)
	} else {
		output.AppendString(colID, res)
	}
	return nil
}

// VectorizedFilter applys a list of filters to a Chunk and returns a bool slice, which indicates whether a row is passed the filters.
// Filters is executed vectorized.
func VectorizedFilter(ctx context.Context, filters []Expression, input *chunk.Chunk, selected []bool) ([]bool, error) {
	selected = selected[:0]
	for i, numRows := 0, input.NumRows(); i < numRows; i++ {
		selected = append(selected, true)
	}
	for _, filter := range filters {
		isIntType := true
		if filter.GetType().EvalType() != types.ETInt {
			isIntType = false
		}
		for row := input.Begin(); row != input.End(); row = row.Next() {
			if !selected[row.Idx()] {
				continue
			}
			if isIntType {
				filterResult, isNull, err := filter.EvalInt(row, ctx.GetSessionVars().StmtCtx)
				if err != nil {
					return nil, errors.Trace(err)
				}
				selected[row.Idx()] = selected[row.Idx()] && !isNull && (filterResult != 0)
			} else {
				// TODO: should rewrite the filter to `cast(expr as SIGNED) != 0` and always use `EvalInt`.
				bVal, err := EvalBool([]Expression{filter}, row, ctx)
				if err != nil {
					return nil, errors.Trace(err)
				}
				selected[row.Idx()] = selected[row.Idx()] && bVal
			}
		}
	}
	return selected, nil
}
