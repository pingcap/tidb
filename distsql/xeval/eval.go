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

// Evaluator evaluates tipb.Expr.
type Evaluator struct {
	Row        map[int64]types.Datum // column values.
	valueLists map[*tipb.Expr]*decodedValueList
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
	case tipb.ExprType_And, tipb.ExprType_Or, tipb.ExprType_Not:
		return e.evalLogicOps(expr)
	// arithmetic operator
	case tipb.ExprType_Plus, tipb.ExprType_Div, tipb.ExprType_Minus,
		tipb.ExprType_Mul, tipb.ExprType_IntDiv, tipb.ExprType_Mod:
		return e.evalArithmeticOps(expr)
	case tipb.ExprType_Case:
		return e.evalCaseWhen(expr)
	case tipb.ExprType_Coalesce:
		return e.evalCoalesce(expr)
	}
	return types.Datum{}, nil
}

func (e *Evaluator) evalTwoChildren(expr *tipb.Expr) (left, right types.Datum, err error) {
	if len(expr.Children) != 2 {
		err = ErrInvalid.Gen("need 2 operands but got %d", len(expr.Children))
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
