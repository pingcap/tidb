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
func VectorizedExecute(ctx context.Context, exprs []Expression, input *chunk.Chunk, isValid []bool, output *chunk.Chunk) error {
	isValid = input.GetIsValid(isValid)
	for colID, expr := range exprs {
		err := evalOneColumn(ctx, expr, input, isValid, output, colID)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func evalOneColumn(ctx context.Context, expr Expression, input *chunk.Chunk, isValid []bool, output *chunk.Chunk, colID int) (err error) {
	switch fieldType, evalType := expr.GetType(), expr.GetType().EvalType(); evalType {
	case types.ETInt:
		for i, length := 0, input.NumAllRows(); i < length; i++ {
			if !isValid[i] {
				output.AppendNull(colID)
				continue
			}
			err = executeToInt(ctx, expr, fieldType, input.GetRow(i), output, colID)
		}
	case types.ETReal:
		for i, length := 0, input.NumAllRows(); i < length; i++ {
			if !isValid[i] {
				output.AppendNull(colID)
				continue
			}
			err = executeToReal(ctx, expr, fieldType, input.GetRow(i), output, colID)
		}
	case types.ETDecimal:
		for i, length := 0, input.NumAllRows(); i < length; i++ {
			if !isValid[i] {
				output.AppendNull(colID)
				continue
			}
			err = executeToDecimal(ctx, expr, fieldType, input.GetRow(i), output, colID)
		}
	case types.ETDatetime, types.ETTimestamp:
		for i, length := 0, input.NumAllRows(); i < length; i++ {
			if !isValid[i] {
				output.AppendNull(colID)
				continue
			}
			err = executeToDatetime(ctx, expr, fieldType, input.GetRow(i), output, colID)
		}
	case types.ETDuration:
		for i, length := 0, input.NumAllRows(); i < length; i++ {
			if !isValid[i] {
				output.AppendNull(colID)
				continue
			}
			err = executeToDuration(ctx, expr, fieldType, input.GetRow(i), output, colID)
		}
	case types.ETJson:
		for i, length := 0, input.NumAllRows(); i < length; i++ {
			if !isValid[i] {
				output.AppendNull(colID)
				continue
			}
			err = executeToJSON(ctx, expr, fieldType, input.GetRow(i), output, colID)
		}
	case types.ETString:
		for i, length := 0, input.NumAllRows(); i < length; i++ {
			if !isValid[i] {
				output.AppendNull(colID)
				continue
			}
			err = executeToString(ctx, expr, fieldType, input.GetRow(i), output, colID)
		}
	}
	return errors.Trace(err)
}

// UnVectorizedExecute evaluates a list of expressions row by row and append their results to "output" Chunk.
func UnVectorizedExecute(ctx context.Context, exprs []Expression, iterator *chunk.Iterator4Chunk, output *chunk.Chunk) error {
	for row := iterator.Begin(); row != iterator.End(); row = iterator.Next() {
		for colID, expr := range exprs {
			err := evalOneCell(ctx, expr, row, output, colID)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

func evalOneCell(ctx context.Context, expr Expression, row chunk.Row, output *chunk.Chunk, colID int) (err error) {
	switch fieldType, evalType := expr.GetType(), expr.GetType().EvalType(); evalType {
	case types.ETInt:
		err = executeToInt(ctx, expr, fieldType, row, output, colID)
	case types.ETReal:
		err = executeToReal(ctx, expr, fieldType, row, output, colID)
	case types.ETDecimal:
		err = executeToDecimal(ctx, expr, fieldType, row, output, colID)
	case types.ETDatetime, types.ETTimestamp:
		err = executeToDatetime(ctx, expr, fieldType, row, output, colID)
	case types.ETDuration:
		err = executeToDuration(ctx, expr, fieldType, row, output, colID)
	case types.ETJson:
		err = executeToJSON(ctx, expr, fieldType, row, output, colID)
	case types.ETString:
		err = executeToString(ctx, expr, fieldType, row, output, colID)
	}
	return errors.Trace(err)
}

func executeToInt(ctx context.Context, expr Expression, fieldType *types.FieldType, row chunk.Row, output *chunk.Chunk, colID int) error {
	res, isNull, err := expr.EvalInt(ctx, row)
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

func executeToReal(ctx context.Context, expr Expression, fieldType *types.FieldType, row chunk.Row, output *chunk.Chunk, colID int) error {
	res, isNull, err := expr.EvalReal(ctx, row)
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

func executeToDecimal(ctx context.Context, expr Expression, fieldType *types.FieldType, row chunk.Row, output *chunk.Chunk, colID int) error {
	res, isNull, err := expr.EvalDecimal(ctx, row)
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

func executeToDatetime(ctx context.Context, expr Expression, fieldType *types.FieldType, row chunk.Row, output *chunk.Chunk, colID int) error {
	res, isNull, err := expr.EvalTime(ctx, row)
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

func executeToDuration(ctx context.Context, expr Expression, fieldType *types.FieldType, row chunk.Row, output *chunk.Chunk, colID int) error {
	res, isNull, err := expr.EvalDuration(ctx, row)
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

func executeToJSON(ctx context.Context, expr Expression, fieldType *types.FieldType, row chunk.Row, output *chunk.Chunk, colID int) error {
	res, isNull, err := expr.EvalJSON(ctx, row)
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

func executeToString(ctx context.Context, expr Expression, fieldType *types.FieldType, row chunk.Row, output *chunk.Chunk, colID int) error {
	res, isNull, err := expr.EvalString(ctx, row)
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

// VectorizedFilter applies a list of filters to a Chunk and
// returns a bool slice, which indicates whether a row is passed the filters.
// Filters is executed vectorized.
func VectorizedFilter(ctx context.Context, filters []Expression, iterator *chunk.Iterator4Chunk) error {
	validIdx := iterator.GetChunk().GetValid()

	for _, filter := range filters {
		// Reset validIdx before execution.
		validIdx = validIdx[:0]

		isIntType := true
		if filter.GetType().EvalType() != types.ETInt {
			isIntType = false
		}

		// Iterate on the valid rows within the Chunk.
		for row := iterator.Begin(); row != iterator.End(); row = iterator.Next() {
			if isIntType {
				filterResult, isNull, err := filter.EvalInt(ctx, row)
				if err != nil {
					return errors.Trace(err)
				}
				if isNull || filterResult == 0 {
					continue
				}
			} else {
				// TODO: should rewrite the filter to `cast(expr as SIGNED) != 0` and always use `EvalInt`.
				selected, err := EvalBool([]Expression{filter}, row, ctx)
				if err != nil {
					return errors.Trace(err)
				}
				if !selected {
					continue
				}
			}
			validIdx = append(validIdx, int32(row.Idx()))
		}

		// Reset valid row indeies in Chunk with the evaluated "validIdx".
		iterator.GetChunk().SetValid(validIdx)
	}
	return nil
}
