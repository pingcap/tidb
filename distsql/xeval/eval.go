// Copyright 2016 PingCAP, Inc.
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

package xeval

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-tipb"
)

// Error instances.
var (
	ErrInvalid = terror.ClassXEval.New(CodeInvalid, "invalid operation")
)

// Error codes.
const (
	CodeInvalid = 3
)

const (
	compareResultNull = -2
)

// Flags are used by tipb.SelectRequest.Flags to handle execution mode, like how to handle truncate error.
const (
	// FlagIgnoreTruncate indicates if truncate error should be ignored.
	// Read-only statements should ignore truncate error, write statements should not ignore truncate error.
	FlagIgnoreTruncate uint64 = 1
	// FlagTruncateAsWarning indicates if truncate error should be returned as warning.
	// This flag only matters if FlagIgnoreTruncate is not set, in strict sql mode, truncate error should
	// be returned as error, in non-strict sql mode, truncate error should be saved as warning.
	FlagTruncateAsWarning uint64 = 1 << 1
)

// Evaluator evaluates tipb.Expr.
type Evaluator struct {
	Row map[int64]types.Datum // TODO: Remove this field after refactor cop_handler.

	ColVals      []types.Datum
	ColIDs       map[int64]int
	ColumnInfos  []*tipb.ColumnInfo
	fieldTps     []*types.FieldType
	valueLists   map[*tipb.Expr]*decodedValueList
	StatementCtx *variable.StatementContext
}

// NewEvaluator creates a new Evaluator instance.
func NewEvaluator(sc *variable.StatementContext) *Evaluator {
	return &Evaluator{
		Row:          make(map[int64]types.Datum),
		ColIDs:       make(map[int64]int),
		StatementCtx: sc,
	}
}

// SetColumnInfos sets ColumnInfos.
func (e *Evaluator) SetColumnInfos(cols []*tipb.ColumnInfo) {
	e.ColumnInfos = make([]*tipb.ColumnInfo, len(cols))
	copy(e.ColumnInfos, cols)

	e.ColVals = make([]types.Datum, len(e.ColumnInfos))
	for i, col := range e.ColumnInfos {
		e.ColIDs[col.GetColumnId()] = i
	}

	e.fieldTps = make([]*types.FieldType, 0, len(e.ColumnInfos))
	for _, col := range e.ColumnInfos {
		ft := distsql.FieldTypeFromPBColumn(col)
		e.fieldTps = append(e.fieldTps, ft)
	}
}

// SetRowValue puts row value into evaluator, the values will be used for expr evaluation.
func (e *Evaluator) SetRowValue(handle int64, row [][]byte, relatedColIDs map[int64]int) error {
	for _, offset := range relatedColIDs {
		col := e.ColumnInfos[offset]
		if col.GetPkHandle() {
			if mysql.HasUnsignedFlag(uint(col.GetFlag())) {
				e.ColVals[offset] = types.NewUintDatum(uint64(handle))
			} else {
				e.ColVals[offset] = types.NewIntDatum(handle)
			}
		} else {
			data := row[offset]
			ft := e.fieldTps[offset]
			datum, err := tablecodec.DecodeColumnValue(data, ft)
			if err != nil {
				return errors.Trace(err)
			}
			e.ColVals[offset] = datum
		}
	}
	return nil
}

type decodedValueList struct {
	values  []types.Datum
	hasNull bool
}

// Eval evaluates expr to a Datum.
func (e *Evaluator) Eval(expr *tipb.Expr) (types.Datum, error) {
	switch expr.GetTp() {
	// data type
	case tipb.ExprType_Null, tipb.ExprType_Int64, tipb.ExprType_Uint64,
		tipb.ExprType_String, tipb.ExprType_Bytes, tipb.ExprType_Float32,
		tipb.ExprType_Float64, tipb.ExprType_MysqlDecimal,
		tipb.ExprType_MysqlDuration, tipb.ExprType_ColumnRef:
		return e.evalDataType(expr)
	// compare operator
	case tipb.ExprType_LT, tipb.ExprType_LE, tipb.ExprType_EQ,
		tipb.ExprType_NE, tipb.ExprType_GE, tipb.ExprType_GT,
		tipb.ExprType_NullEQ, tipb.ExprType_Like, tipb.ExprType_In:
		return e.evalCompareOps(expr)
	// logic operator
	case tipb.ExprType_And, tipb.ExprType_Or, tipb.ExprType_Xor, tipb.ExprType_Not:
		return e.evalLogicOps(expr)
	// arithmetic operator
	case tipb.ExprType_Plus, tipb.ExprType_Div, tipb.ExprType_Minus,
		tipb.ExprType_Mul, tipb.ExprType_IntDiv, tipb.ExprType_Mod:
		return e.evalArithmeticOps(expr)
	// bit operator
	case tipb.ExprType_BitAnd, tipb.ExprType_BitOr, tipb.ExprType_BitNeg,
		tipb.ExprType_BitXor, tipb.ExprType_LeftShift, tipb.ExprType_RighShift:
		return e.evalBitOps(expr)
	// control functions
	case tipb.ExprType_Case, tipb.ExprType_If, tipb.ExprType_IfNull, tipb.ExprType_NullIf:
		return e.evalControlFuncs(expr)
	case tipb.ExprType_Coalesce:
		return e.evalCoalesce(expr)
	case tipb.ExprType_IsNull:
		return e.evalIsNull(expr)
	}
	return types.Datum{}, nil
}

func (e *Evaluator) evalTwoChildren(expr *tipb.Expr) (left, right types.Datum, err error) {
	if len(expr.Children) != 2 {
		err = ErrInvalid.Gen("%s needs 2 operands but got %d", tipb.ExprType_name[int32(expr.GetTp())], len(expr.Children))
		return
	}
	left, err = e.Eval(expr.Children[0])
	if err != nil {
		return types.Datum{}, types.Datum{}, errors.Trace(err)
	}
	right, err = e.Eval(expr.Children[1])
	if err != nil {
		return types.Datum{}, types.Datum{}, errors.Trace(err)
	}
	return
}

// getTwoChildren makes sure that expr has and only has two children.
func (e *Evaluator) getTwoChildren(expr *tipb.Expr) (left, right *tipb.Expr, err error) {
	if len(expr.Children) != 2 {
		err = ErrInvalid.Gen("%s needs 2 operands but got %d", tipb.ExprType_name[int32(expr.GetTp())], len(expr.Children))
		return
	}
	return expr.Children[0], expr.Children[1], nil
}

func (e *Evaluator) evalIsNull(expr *tipb.Expr) (types.Datum, error) {
	if len(expr.Children) != 1 {
		return types.Datum{}, ErrInvalid.Gen("ISNULL need 1 operand, got %d", len(expr.Children))
	}
	d, err := e.Eval(expr.Children[0])
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	if d.IsNull() {
		return types.NewIntDatum(1), nil
	}
	return types.NewIntDatum(0), nil
}

// FlagsToStatementContext creates a StatementContext from a `tipb.SelectRequest.Flags`.
func FlagsToStatementContext(flags uint64) *variable.StatementContext {
	sc := new(variable.StatementContext)
	sc.IgnoreTruncate = (flags & FlagIgnoreTruncate) > 0
	sc.TruncateAsWarning = (flags & FlagTruncateAsWarning) > 0
	return sc
}
