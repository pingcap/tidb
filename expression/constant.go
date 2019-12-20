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
	"fmt"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

var (
	// One stands for a number 1.
	One = &Constant{
		Value:   types.NewDatum(1),
		RetType: types.NewFieldType(mysql.TypeTiny),
	}

	// Zero stands for a number 0.
	Zero = &Constant{
		Value:   types.NewDatum(0),
		RetType: types.NewFieldType(mysql.TypeTiny),
	}

	// Null stands for null constant.
	Null = &Constant{
		Value:   types.NewDatum(nil),
		RetType: types.NewFieldType(mysql.TypeTiny),
	}
)

// Constant stands for a constant value.
type Constant struct {
	Value   types.Datum
	RetType *types.FieldType
	// DeferredExpr holds deferred function in PlanCache cached plan.
	// it's only used to represent non-deterministic functions(see expression.DeferredFunctions)
	// in PlanCache cached plan, so let them can be evaluated until cached item be used.
	DeferredExpr Expression
	// ParamMarker holds param index inside sessionVars.PreparedParams.
	// It's only used to reference a user variable provided in the `EXECUTE` statement or `COM_EXECUTE` binary protocol.
	ParamMarker *ParamMarker
	hashcode    []byte
}

// ParamMarker indicates param provided by COM_STMT_EXECUTE.
type ParamMarker struct {
	ctx   sessionctx.Context
	order int
	tp    types.FieldType
}

// GetUserVar returns the corresponding user variable presented in the `EXECUTE` statement or `COM_EXECUTE` command.
func (d *ParamMarker) GetUserVar() types.Datum {
	sessionVars := d.ctx.GetSessionVars()
	return sessionVars.PreparedParams[d.order]
}

// String implements fmt.Stringer interface.
func (c *Constant) String() string {
	if c.ParamMarker != nil {
		dt := c.ParamMarker.GetUserVar()
		c.Value.SetValue(dt.GetValue())
	} else if c.DeferredExpr != nil {
		dt, err := c.Eval(chunk.Row{})
		if err != nil {
			logutil.BgLogger().Error("eval constant failed", zap.Error(err))
			return ""
		}
		c.Value.SetValue(dt.GetValue())
	}
	return fmt.Sprintf("%v", c.Value.GetValue())
}

// MarshalJSON implements json.Marshaler interface.
func (c *Constant) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("\"%s\"", c)), nil
}

// Clone implements Expression interface.
func (c *Constant) Clone() Expression {
	if c.DeferredExpr != nil || c.ParamMarker != nil {
		con := *c
		return &con
	}
	return c
}

var unspecifiedTp = types.NewFieldType(mysql.TypeUnspecified)

// GetType implements Expression interface.
func (c *Constant) GetType() *types.FieldType {
	if c.ParamMarker != nil {
		tp := &c.ParamMarker.tp
		*tp = *unspecifiedTp
		dt := c.ParamMarker.GetUserVar()
		types.DefaultParamTypeForValue(dt.GetValue(), tp)
		return tp
	}
	return c.RetType
}

// VecEvalInt evaluates this expression in a vectorized manner.
func (c *Constant) VecEvalInt(ctx sessionctx.Context, input *chunk.Chunk, result *chunk.Column) error {
	if c.DeferredExpr == nil {
		return genVecFromConstExpr(ctx, c, types.ETInt, input, result)
	}
	return c.DeferredExpr.VecEvalInt(ctx, input, result)
}

// VecEvalReal evaluates this expression in a vectorized manner.
func (c *Constant) VecEvalReal(ctx sessionctx.Context, input *chunk.Chunk, result *chunk.Column) error {
	if c.DeferredExpr == nil {
		return genVecFromConstExpr(ctx, c, types.ETReal, input, result)
	}
	return c.DeferredExpr.VecEvalReal(ctx, input, result)
}

// VecEvalString evaluates this expression in a vectorized manner.
func (c *Constant) VecEvalString(ctx sessionctx.Context, input *chunk.Chunk, result *chunk.Column) error {
	if c.DeferredExpr == nil {
		return genVecFromConstExpr(ctx, c, types.ETString, input, result)
	}
	return c.DeferredExpr.VecEvalString(ctx, input, result)
}

// VecEvalDecimal evaluates this expression in a vectorized manner.
func (c *Constant) VecEvalDecimal(ctx sessionctx.Context, input *chunk.Chunk, result *chunk.Column) error {
	if c.DeferredExpr == nil {
		return genVecFromConstExpr(ctx, c, types.ETDecimal, input, result)
	}
	return c.DeferredExpr.VecEvalDecimal(ctx, input, result)
}

// VecEvalTime evaluates this expression in a vectorized manner.
func (c *Constant) VecEvalTime(ctx sessionctx.Context, input *chunk.Chunk, result *chunk.Column) error {
	if c.DeferredExpr == nil {
		return genVecFromConstExpr(ctx, c, types.ETTimestamp, input, result)
	}
	return c.DeferredExpr.VecEvalTime(ctx, input, result)
}

// VecEvalDuration evaluates this expression in a vectorized manner.
func (c *Constant) VecEvalDuration(ctx sessionctx.Context, input *chunk.Chunk, result *chunk.Column) error {
	if c.DeferredExpr == nil {
		return genVecFromConstExpr(ctx, c, types.ETDuration, input, result)
	}
	return c.DeferredExpr.VecEvalDuration(ctx, input, result)
}

// VecEvalJSON evaluates this expression in a vectorized manner.
func (c *Constant) VecEvalJSON(ctx sessionctx.Context, input *chunk.Chunk, result *chunk.Column) error {
	if c.DeferredExpr == nil {
		return genVecFromConstExpr(ctx, c, types.ETJson, input, result)
	}
	return c.DeferredExpr.VecEvalJSON(ctx, input, result)
}

func (c *Constant) getLazyDatum() (dt types.Datum, isLazy bool, err error) {
	if c.ParamMarker != nil {
		dt = c.ParamMarker.GetUserVar()
		isLazy = true
		return
	} else if c.DeferredExpr != nil {
		dt, err = c.DeferredExpr.Eval(chunk.Row{})
		isLazy = true
		return
	}
	return
}

// Eval implements Expression interface.
func (c *Constant) Eval(_ chunk.Row) (types.Datum, error) {
	if dt, lazy, err := c.getLazyDatum(); lazy {
		if err != nil {
			return c.Value, err
		}
		if dt.IsNull() {
			c.Value.SetNull()
			return c.Value, nil
		}
		if c.DeferredExpr != nil {
			sf, sfOk := c.DeferredExpr.(*ScalarFunction)
			if sfOk {
				val, err := dt.ConvertTo(sf.GetCtx().GetSessionVars().StmtCtx, c.RetType)
				if err != nil {
					return dt, err
				}
				return val, nil
			}
		}
		return dt, nil
	}
	return c.Value, nil
}

// EvalInt returns int representation of Constant.
func (c *Constant) EvalInt(ctx sessionctx.Context, _ chunk.Row) (int64, bool, error) {
	if dt, lazy, err := c.getLazyDatum(); lazy {
		if err != nil {
			return 0, true, err
		}
		if dt.IsNull() {
			return 0, true, nil
		}
		val, err := dt.ToInt64(ctx.GetSessionVars().StmtCtx)
		if err != nil {
			return 0, true, err
		}
		c.Value.SetInt64(val)
	} else {
		if c.GetType().Tp == mysql.TypeNull || c.Value.IsNull() {
			return 0, true, nil
		}
	}
	if c.GetType().Hybrid() || c.Value.Kind() == types.KindBinaryLiteral || c.Value.Kind() == types.KindString {
		res, err := c.Value.ToInt64(ctx.GetSessionVars().StmtCtx)
		return res, err != nil, err
	}
	return c.Value.GetInt64(), false, nil
}

// EvalReal returns real representation of Constant.
func (c *Constant) EvalReal(ctx sessionctx.Context, _ chunk.Row) (float64, bool, error) {
	if dt, lazy, err := c.getLazyDatum(); lazy {
		if err != nil {
			return 0, true, err
		}
		if dt.IsNull() {
			return 0, true, nil
		}
		val, err := dt.ToFloat64(ctx.GetSessionVars().StmtCtx)
		if err != nil {
			return 0, true, err
		}
		c.Value.SetFloat64(val)
	} else {
		if c.GetType().Tp == mysql.TypeNull || c.Value.IsNull() {
			return 0, true, nil
		}
	}
	if c.GetType().Hybrid() || c.Value.Kind() == types.KindBinaryLiteral || c.Value.Kind() == types.KindString {
		res, err := c.Value.ToFloat64(ctx.GetSessionVars().StmtCtx)
		return res, err != nil, err
	}
	return c.Value.GetFloat64(), false, nil
}

// EvalString returns string representation of Constant.
func (c *Constant) EvalString(ctx sessionctx.Context, _ chunk.Row) (string, bool, error) {
	if dt, lazy, err := c.getLazyDatum(); lazy {
		if err != nil {
			return "", true, err
		}
		if dt.IsNull() {
			return "", true, nil
		}
		val, err := dt.ToString()
		if err != nil {
			return "", true, err
		}
		c.Value.SetString(val)
	} else {
		if c.GetType().Tp == mysql.TypeNull || c.Value.IsNull() {
			return "", true, nil
		}
	}
	res, err := c.Value.ToString()
	return res, err != nil, err
}

// EvalDecimal returns decimal representation of Constant.
func (c *Constant) EvalDecimal(ctx sessionctx.Context, _ chunk.Row) (*types.MyDecimal, bool, error) {
	if dt, lazy, err := c.getLazyDatum(); lazy {
		if err != nil {
			return nil, true, err
		}
		if dt.IsNull() {
			return nil, true, nil
		}
		c.Value.SetValue(dt.GetValue())
	} else {
		if c.GetType().Tp == mysql.TypeNull || c.Value.IsNull() {
			return nil, true, nil
		}
	}
	res, err := c.Value.ToDecimal(ctx.GetSessionVars().StmtCtx)
	return res, err != nil, err
}

// EvalTime returns DATE/DATETIME/TIMESTAMP representation of Constant.
func (c *Constant) EvalTime(ctx sessionctx.Context, _ chunk.Row) (val types.Time, isNull bool, err error) {
	if dt, lazy, err := c.getLazyDatum(); lazy {
		if err != nil {
			return types.Time{}, true, err
		}
		if dt.IsNull() {
			return types.Time{}, true, nil
		}
		val, err := dt.ToString()
		if err != nil {
			return types.Time{}, true, err
		}
		tim, err := types.ParseDatetime(ctx.GetSessionVars().StmtCtx, val)
		if err != nil {
			return types.Time{}, true, err
		}
		c.Value.SetMysqlTime(tim)
	} else {
		if c.GetType().Tp == mysql.TypeNull || c.Value.IsNull() {
			return types.Time{}, true, nil
		}
	}
	return c.Value.GetMysqlTime(), false, nil
}

// EvalDuration returns Duration representation of Constant.
func (c *Constant) EvalDuration(ctx sessionctx.Context, _ chunk.Row) (val types.Duration, isNull bool, err error) {
	if dt, lazy, err := c.getLazyDatum(); lazy {
		if err != nil {
			return types.Duration{}, true, err
		}
		if dt.IsNull() {
			return types.Duration{}, true, nil
		}
		val, err := dt.ToString()
		if err != nil {
			return types.Duration{}, true, err
		}
		dur, err := types.ParseDuration(ctx.GetSessionVars().StmtCtx, val, types.MaxFsp)
		if err != nil {
			return types.Duration{}, true, err
		}
		c.Value.SetMysqlDuration(dur)
	} else {
		if c.GetType().Tp == mysql.TypeNull || c.Value.IsNull() {
			return types.Duration{}, true, nil
		}
	}
	return c.Value.GetMysqlDuration(), false, nil
}

// EvalJSON returns JSON representation of Constant.
func (c *Constant) EvalJSON(ctx sessionctx.Context, _ chunk.Row) (json.BinaryJSON, bool, error) {
	if dt, lazy, err := c.getLazyDatum(); lazy {
		if err != nil {
			return json.BinaryJSON{}, true, err
		}
		if dt.IsNull() {
			return json.BinaryJSON{}, true, nil
		}
		val, err := dt.ConvertTo(ctx.GetSessionVars().StmtCtx, types.NewFieldType(mysql.TypeJSON))
		if err != nil {
			return json.BinaryJSON{}, true, err
		}
		c.Value.SetMysqlJSON(val.GetMysqlJSON())
		c.GetType().Tp = mysql.TypeJSON
	} else {
		if c.GetType().Tp == mysql.TypeNull || c.Value.IsNull() {
			return json.BinaryJSON{}, true, nil
		}
	}
	return c.Value.GetMysqlJSON(), false, nil
}

// Equal implements Expression interface.
func (c *Constant) Equal(ctx sessionctx.Context, b Expression) bool {
	y, ok := b.(*Constant)
	if !ok {
		return false
	}
	_, err1 := y.Eval(chunk.Row{})
	_, err2 := c.Eval(chunk.Row{})
	if err1 != nil || err2 != nil {
		return false
	}
	con, err := c.Value.CompareDatum(ctx.GetSessionVars().StmtCtx, &y.Value)
	if err != nil || con != 0 {
		return false
	}
	return true
}

// IsCorrelated implements Expression interface.
func (c *Constant) IsCorrelated() bool {
	return false
}

// ConstItem implements Expression interface.
func (c *Constant) ConstItem() bool {
	return true
}

// Decorrelate implements Expression interface.
func (c *Constant) Decorrelate(_ *Schema) Expression {
	return c
}

// HashCode implements Expression interface.
func (c *Constant) HashCode(sc *stmtctx.StatementContext) []byte {
	if len(c.hashcode) > 0 {
		return c.hashcode
	}
	_, err := c.Eval(chunk.Row{})
	if err != nil {
		terror.Log(err)
	}
	c.hashcode = append(c.hashcode, constantFlag)
	c.hashcode, err = codec.EncodeValue(sc, c.hashcode, c.Value)
	if err != nil {
		terror.Log(err)
	}
	return c.hashcode
}

// ResolveIndices implements Expression interface.
func (c *Constant) ResolveIndices(_ *Schema) (Expression, error) {
	return c, nil
}

func (c *Constant) resolveIndices(_ *Schema) error {
	return nil
}

// Vectorized returns if this expression supports vectorized evaluation.
func (c *Constant) Vectorized() bool {
	if c.DeferredExpr != nil {
		return c.DeferredExpr.Vectorized()
	}
	return true
}

// SupportReverseEval checks whether the builtinFunc support reverse evaluation.
func (c *Constant) SupportReverseEval() bool {
	if c.DeferredExpr != nil {
		return c.DeferredExpr.SupportReverseEval()
	}
	return true
}

// ReverseEval evaluates the only one column value with given function result.
func (c *Constant) ReverseEval(sc *stmtctx.StatementContext, res types.Datum, rType types.RoundingType) (val types.Datum, err error) {
	return c.Value, nil
}
