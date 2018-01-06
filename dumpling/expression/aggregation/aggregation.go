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

package aggregation

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	tipb "github.com/pingcap/tipb/go-tipb"
)

// Aggregation stands for aggregate functions.
type Aggregation interface {
	fmt.Stringer
	json.Marshaler

	// Update during executing.
	Update(ctx *AggEvaluateContext, sc *stmtctx.StatementContext, row types.Row) error

	// GetPartialResult will called by coprocessor to get partial results. For avg function, partial results will return
	// sum and count values at the same time.
	GetPartialResult(ctx *AggEvaluateContext) []types.Datum

	// SetMode sets aggFunctionMode for aggregate function.
	SetMode(mode AggFunctionMode)

	// GetMode gets aggFunctionMode from aggregate function.
	GetMode() AggFunctionMode

	// GetResult will be called when all data have been processed.
	GetResult(ctx *AggEvaluateContext) types.Datum

	// GetArgs stands for getting all arguments.
	GetArgs() []expression.Expression

	// GetName gets the aggregation function name.
	GetName() string

	// SetArgs sets argument by index.
	SetArgs(args []expression.Expression)

	// Create a new AggEvaluateContext for the aggregation function.
	CreateContext() *AggEvaluateContext

	// IsDistinct indicates if the aggregate function contains distinct attribute.
	IsDistinct() bool

	// Equal checks whether two aggregation functions are equal.
	Equal(agg Aggregation, ctx context.Context) bool

	// Clone copies an aggregate function totally.
	Clone() Aggregation

	// GetType gets field type of aggregate function.
	GetType() *types.FieldType

	// CalculateDefaultValue gets the default value when the aggregate function's input is null.
	// The input stands for the schema of Aggregation's child. If the function can't produce a default value, the second
	// return value will be false.
	CalculateDefaultValue(schema *expression.Schema, ctx context.Context) (types.Datum, bool)
}

// NewAggFunction creates a new Aggregation.
func NewAggFunction(funcType string, funcArgs []expression.Expression, distinct bool) Aggregation {
	switch tp := strings.ToLower(funcType); tp {
	case ast.AggFuncSum:
		return &sumFunction{aggFunction: newAggFunc(tp, funcArgs, distinct)}
	case ast.AggFuncCount:
		return &countFunction{aggFunction: newAggFunc(tp, funcArgs, distinct)}
	case ast.AggFuncAvg:
		return &avgFunction{aggFunction: newAggFunc(tp, funcArgs, distinct)}
	case ast.AggFuncGroupConcat:
		return &concatFunction{aggFunction: newAggFunc(tp, funcArgs, distinct)}
	case ast.AggFuncMax:
		return &maxMinFunction{aggFunction: newAggFunc(tp, funcArgs, distinct), isMax: true}
	case ast.AggFuncMin:
		return &maxMinFunction{aggFunction: newAggFunc(tp, funcArgs, distinct), isMax: false}
	case ast.AggFuncFirstRow:
		return &firstRowFunction{aggFunction: newAggFunc(tp, funcArgs, distinct)}
	case ast.AggFuncBitOr:
		return &bitOrFunction{aggFunction: newAggFunc(tp, funcArgs, distinct)}
	case ast.AggFuncBitXor:
		return &bitXorFunction{aggFunction: newAggFunc(tp, funcArgs, distinct)}
	case ast.AggFuncBitAnd:
		return &bitAndFunction{aggFunction: newAggFunc(tp, funcArgs, distinct)}
	}
	return nil
}

// NewDistAggFunc creates new Aggregate function for mock tikv.
func NewDistAggFunc(expr *tipb.Expr, fieldTps []*types.FieldType, sc *stmtctx.StatementContext) (Aggregation, error) {
	args := make([]expression.Expression, 0, len(expr.Children))
	for _, child := range expr.Children {
		arg, err := expression.PBToExpr(child, fieldTps, sc)
		if err != nil {
			return nil, errors.Trace(err)
		}
		args = append(args, arg)
	}
	switch expr.Tp {
	case tipb.ExprType_Sum:
		return &sumFunction{aggFunction: newAggFunc(ast.AggFuncSum, args, false)}, nil
	case tipb.ExprType_Count:
		return &countFunction{aggFunction: newAggFunc(ast.AggFuncCount, args, false)}, nil
	case tipb.ExprType_Avg:
		return &avgFunction{aggFunction: newAggFunc(ast.AggFuncAvg, args, false)}, nil
	case tipb.ExprType_GroupConcat:
		return &concatFunction{aggFunction: newAggFunc(ast.AggFuncGroupConcat, args, false)}, nil
	case tipb.ExprType_Max:
		return &maxMinFunction{aggFunction: newAggFunc(ast.AggFuncMax, args, false), isMax: true}, nil
	case tipb.ExprType_Min:
		return &maxMinFunction{aggFunction: newAggFunc(ast.AggFuncMin, args, false)}, nil
	case tipb.ExprType_First:
		return &firstRowFunction{aggFunction: newAggFunc(ast.AggFuncFirstRow, args, false)}, nil
	case tipb.ExprType_Agg_BitOr:
		return &bitOrFunction{aggFunction: newAggFunc(ast.AggFuncBitOr, args, false)}, nil
	case tipb.ExprType_Agg_BitXor:
		return &bitXorFunction{aggFunction: newAggFunc(ast.AggFuncBitXor, args, false)}, nil
	case tipb.ExprType_Agg_BitAnd:
		return &bitAndFunction{aggFunction: newAggFunc(ast.AggFuncBitAnd, args, false)}, nil
	}
	return nil, errors.Errorf("Unknown aggregate function type %v", expr.Tp)
}

// AggEvaluateContext is used to store intermediate result when calculating aggregate functions.
type AggEvaluateContext struct {
	DistinctChecker *distinctChecker
	Count           int64
	Value           types.Datum
	Buffer          *bytes.Buffer // Buffer is used for group_concat.
	GotFirstRow     bool          // It will check if the agg has met the first row key.
}

// AggFunctionMode stands for the aggregation function's mode.
type AggFunctionMode int

const (
	// CompleteMode function accepts origin data.
	CompleteMode AggFunctionMode = iota
	// FinalMode function accepts partial data.
	FinalMode
)

type aggFunction struct {
	name     string
	mode     AggFunctionMode
	Args     []expression.Expression
	Distinct bool
}

// Equal implements Aggregation interface.
func (af *aggFunction) Equal(b Aggregation, ctx context.Context) bool {
	if af.GetName() != b.GetName() {
		return false
	}
	if af.Distinct != b.IsDistinct() {
		return false
	}
	if len(af.GetArgs()) != len(b.GetArgs()) {
		return false
	}
	for i, argA := range af.GetArgs() {
		if !argA.Equal(b.GetArgs()[i], ctx) {
			return false
		}
	}
	return true
}

// String implements fmt.Stringer interface.
func (af *aggFunction) String() string {
	result := af.name + "("
	for i, arg := range af.Args {
		result += arg.String()
		if i+1 != len(af.Args) {
			result += ", "
		}
	}
	result += ")"
	return result
}

// MarshalJSON implements json.Marshaler interface.
func (af *aggFunction) MarshalJSON() ([]byte, error) {
	buffer := bytes.NewBufferString(fmt.Sprintf("\"%s\"", af))
	return buffer.Bytes(), nil
}

func newAggFunc(name string, args []expression.Expression, dist bool) aggFunction {
	return aggFunction{
		name:     name,
		Args:     args,
		Distinct: dist,
	}
}

// CalculateDefaultValue implements Aggregation interface.
func (af *aggFunction) CalculateDefaultValue(schema *expression.Schema, ctx context.Context) (types.Datum, bool) {
	return types.Datum{}, false
}

// IsDistinct implements Aggregation interface.
func (af *aggFunction) IsDistinct() bool {
	return af.Distinct
}

// GetName implements Aggregation interface.
func (af *aggFunction) GetName() string {
	return af.name
}

// SetMode implements Aggregation interface.
func (af *aggFunction) SetMode(mode AggFunctionMode) {
	af.mode = mode
}

// GetMode implements Aggregation interface.
func (af *aggFunction) GetMode() AggFunctionMode {
	return af.mode
}

// GetArgs implements Aggregation interface.
func (af *aggFunction) GetArgs() []expression.Expression {
	return af.Args
}

// SetArgs implements Aggregation interface.
func (af *aggFunction) SetArgs(args []expression.Expression) {
	af.Args = args
}

// CreateContext implements Aggregation interface.
func (af *aggFunction) CreateContext() *AggEvaluateContext {
	ctx := &AggEvaluateContext{}
	if af.Distinct {
		ctx.DistinctChecker = createDistinctChecker()
	}
	return ctx
}

func (af *aggFunction) updateSum(ctx *AggEvaluateContext, sc *stmtctx.StatementContext, row types.Row) error {
	a := af.Args[0]
	value, err := a.Eval(row)
	if err != nil {
		return errors.Trace(err)
	}
	if value.IsNull() {
		return nil
	}
	if af.Distinct {
		d, err1 := ctx.DistinctChecker.Check([]types.Datum{value})
		if err1 != nil {
			return errors.Trace(err1)
		}
		if !d {
			return nil
		}
	}
	ctx.Value, err = calculateSum(sc, ctx.Value, value)
	if err != nil {
		return errors.Trace(err)
	}
	ctx.Count++
	return nil
}
