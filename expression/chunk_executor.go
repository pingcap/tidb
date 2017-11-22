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
		for rowID, length := 0, input.NumRows(); err == nil && rowID < length; rowID++ {
			err = executeToInt(sc, expr, fieldType, input, output, rowID, colID)
		}
	case types.ETReal:
		for rowID, length := 0, input.NumRows(); err == nil && rowID < length; rowID++ {
			err = executeToReal(sc, expr, fieldType, input, output, rowID, colID)
		}
	case types.ETDecimal:
		for rowID, length := 0, input.NumRows(); err == nil && rowID < length; rowID++ {
			err = executeToDecimal(sc, expr, fieldType, input, output, rowID, colID)
		}
	case types.ETDatetime, types.ETTimestamp:
		for rowID, length := 0, input.NumRows(); err == nil && rowID < length; rowID++ {
			err = executeToDatetime(sc, expr, fieldType, input, output, rowID, colID)
		}
	case types.ETDuration:
		for rowID, length := 0, input.NumRows(); err == nil && rowID < length; rowID++ {
			err = executeToDuration(sc, expr, fieldType, input, output, rowID, colID)
		}
	case types.ETJson:
		for rowID, length := 0, input.NumRows(); err == nil && rowID < length; rowID++ {
			err = executeToJSON(sc, expr, fieldType, input, output, rowID, colID)
		}
	case types.ETString:
		for rowID, length := 0, input.NumRows(); err == nil && rowID < length; rowID++ {
			err = executeToString(sc, expr, fieldType, input, output, rowID, colID)
		}
	}
	return errors.Trace(err)
}

// UnVectorizedExecute evaluates a list of expressions row by row and append their results to "output" Chunk.
func UnVectorizedExecute(ctx context.Context, exprs []Expression, input, output *chunk.Chunk) error {
	sc := ctx.GetSessionVars().StmtCtx
	for rowID, length := 0, input.NumRows(); rowID < length; rowID++ {
		for colID, expr := range exprs {
			err := evalOneCell(sc, expr, input, output, rowID, colID)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

func evalOneCell(sc *stmtctx.StatementContext, expr Expression, input, output *chunk.Chunk, rowID, colID int) (err error) {
	switch fieldType, evalType := expr.GetType(), expr.GetType().EvalType(); evalType {
	case types.ETInt:
		err = executeToInt(sc, expr, fieldType, input, output, rowID, colID)
	case types.ETReal:
		err = executeToReal(sc, expr, fieldType, input, output, rowID, colID)
	case types.ETDecimal:
		err = executeToDecimal(sc, expr, fieldType, input, output, rowID, colID)
	case types.ETDatetime, types.ETTimestamp:
		err = executeToDatetime(sc, expr, fieldType, input, output, rowID, colID)
	case types.ETDuration:
		err = executeToDuration(sc, expr, fieldType, input, output, rowID, colID)
	case types.ETJson:
		err = executeToJSON(sc, expr, fieldType, input, output, rowID, colID)
	case types.ETString:
		err = executeToString(sc, expr, fieldType, input, output, rowID, colID)
	}
	return errors.Trace(err)
}

func executeToInt(sc *stmtctx.StatementContext, expr Expression, fieldType *types.FieldType, input, output *chunk.Chunk, rowID, colID int) error {
	res, isNull, err := expr.EvalInt(input.GetRow(rowID), sc)
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

func executeToReal(sc *stmtctx.StatementContext, expr Expression, fieldType *types.FieldType, input, output *chunk.Chunk, rowID, colID int) error {
	res, isNull, err := expr.EvalReal(input.GetRow(rowID), sc)
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

func executeToDecimal(sc *stmtctx.StatementContext, expr Expression, fieldType *types.FieldType, input, output *chunk.Chunk, rowID, colID int) error {
	res, isNull, err := expr.EvalDecimal(input.GetRow(rowID), sc)
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

func executeToDatetime(sc *stmtctx.StatementContext, expr Expression, fieldType *types.FieldType, input, output *chunk.Chunk, rowID, colID int) error {
	res, isNull, err := expr.EvalTime(input.GetRow(rowID), sc)
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

func executeToDuration(sc *stmtctx.StatementContext, expr Expression, fieldType *types.FieldType, input, output *chunk.Chunk, rowID, colID int) error {
	res, isNull, err := expr.EvalDuration(input.GetRow(rowID), sc)
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

func executeToJSON(sc *stmtctx.StatementContext, expr Expression, fieldType *types.FieldType, input, output *chunk.Chunk, rowID, colID int) error {
	res, isNull, err := expr.EvalJSON(input.GetRow(rowID), sc)
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

func executeToString(sc *stmtctx.StatementContext, expr Expression, fieldType *types.FieldType, input, output *chunk.Chunk, rowID, colID int) error {
	res, isNull, err := expr.EvalString(input.GetRow(rowID), sc)
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
