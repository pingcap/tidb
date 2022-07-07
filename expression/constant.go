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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"fmt"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/collate"
)

// NewOne stands for a number 1.
func NewOne() *Constant {
	retT := types.NewFieldType(mysql.TypeTiny)
	retT.AddFlag(mysql.UnsignedFlag) // shrink range to avoid integral promotion
	retT.SetFlen(1)
	retT.SetDecimal(0)
	return &Constant{
		Value:   types.NewDatum(1),
		RetType: retT,
	}
}

// NewZero stands for a number 0.
func NewZero() *Constant {
	retT := types.NewFieldType(mysql.TypeTiny)
	retT.AddFlag(mysql.UnsignedFlag) // shrink range to avoid integral promotion
	retT.SetFlen(1)
	retT.SetDecimal(0)
	return &Constant{
		Value:   types.NewDatum(0),
		RetType: retT,
	}
}

// NewNull stands for null constant.
func NewNull() *Constant {
	retT := types.NewFieldType(mysql.TypeTiny)
	retT.SetFlen(1)
	retT.SetDecimal(0)
	return &Constant{
		Value:   types.NewDatum(nil),
		RetType: retT,
	}
}

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

	collationInfo
}

// ParamMarker indicates param provided by COM_STMT_EXECUTE.
type ParamMarker struct {
	ctx   sessionctx.Context
	order int
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
		c.Value.SetValue(dt.GetValue(), c.RetType)
	} else if c.DeferredExpr != nil {
		return c.DeferredExpr.String()
	}
	return fmt.Sprintf("%v", c.Value.GetValue())
}

// MarshalJSON implements json.Marshaler interface.
func (c *Constant) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("%q", c)), nil
}

// Clone implements Expression interface.
func (c *Constant) Clone() Expression {
	con := *c
	return &con
}

// GetType implements Expression interface.
func (c *Constant) GetType() *types.FieldType {
	if c.ParamMarker != nil {
		// GetType() may be called in multi-threaded context, e.g, in building inner executors of IndexJoin,
		// so it should avoid data race. We achieve this by returning different FieldType pointer for each call.
		tp := types.NewFieldType(mysql.TypeUnspecified)
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

func (c *Constant) getLazyDatum(row chunk.Row) (dt types.Datum, isLazy bool, err error) {
	if c.ParamMarker != nil {
		return c.ParamMarker.GetUserVar(), true, nil
	} else if c.DeferredExpr != nil {
		dt, err = c.DeferredExpr.Eval(row)
		return dt, true, err
	}
	return types.Datum{}, false, nil
}

// Eval implements Expression interface.
func (c *Constant) Eval(row chunk.Row) (types.Datum, error) {
	if dt, lazy, err := c.getLazyDatum(row); lazy {
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
func (c *Constant) EvalInt(ctx sessionctx.Context, row chunk.Row) (int64, bool, error) {
	dt, lazy, err := c.getLazyDatum(row)
	if err != nil {
		return 0, false, err
	}
	if !lazy {
		dt = c.Value
	}
	if c.GetType().GetType() == mysql.TypeNull || dt.IsNull() {
		return 0, true, nil
	} else if dt.Kind() == types.KindBinaryLiteral {
		val, err := dt.GetBinaryLiteral().ToInt(ctx.GetSessionVars().StmtCtx)
		return int64(val), err != nil, err
	} else if c.GetType().Hybrid() || dt.Kind() == types.KindString {
		res, err := dt.ToInt64(ctx.GetSessionVars().StmtCtx)
		return res, false, err
	} else if dt.Kind() == types.KindMysqlBit {
		uintVal, err := dt.GetBinaryLiteral().ToInt(ctx.GetSessionVars().StmtCtx)
		return int64(uintVal), false, err
	}
	return dt.GetInt64(), false, nil
}

// EvalReal returns real representation of Constant.
func (c *Constant) EvalReal(ctx sessionctx.Context, row chunk.Row) (float64, bool, error) {
	dt, lazy, err := c.getLazyDatum(row)
	if err != nil {
		return 0, false, err
	}
	if !lazy {
		dt = c.Value
	}
	if c.GetType().GetType() == mysql.TypeNull || dt.IsNull() {
		return 0, true, nil
	}
	if c.GetType().Hybrid() || dt.Kind() == types.KindBinaryLiteral || dt.Kind() == types.KindString {
		res, err := dt.ToFloat64(ctx.GetSessionVars().StmtCtx)
		return res, false, err
	}
	return dt.GetFloat64(), false, nil
}

// EvalString returns string representation of Constant.
func (c *Constant) EvalString(ctx sessionctx.Context, row chunk.Row) (string, bool, error) {
	dt, lazy, err := c.getLazyDatum(row)
	if err != nil {
		return "", false, err
	}
	if !lazy {
		dt = c.Value
	}
	if c.GetType().GetType() == mysql.TypeNull || dt.IsNull() {
		return "", true, nil
	}
	res, err := dt.ToString()
	return res, false, err
}

// EvalDecimal returns decimal representation of Constant.
func (c *Constant) EvalDecimal(ctx sessionctx.Context, row chunk.Row) (*types.MyDecimal, bool, error) {
	dt, lazy, err := c.getLazyDatum(row)
	if err != nil {
		return nil, false, err
	}
	if !lazy {
		dt = c.Value
	}
	if c.GetType().GetType() == mysql.TypeNull || dt.IsNull() {
		return nil, true, nil
	}
	res, err := dt.ToDecimal(ctx.GetSessionVars().StmtCtx)
	if err != nil {
		return nil, false, err
	}
	// The decimal may be modified during plan building.
	_, frac := res.PrecisionAndFrac()
	if frac < c.GetType().GetDecimal() {
		err = res.Round(res, c.GetType().GetDecimal(), types.ModeHalfUp)
	}
	return res, false, err
}

// EvalTime returns DATE/DATETIME/TIMESTAMP representation of Constant.
func (c *Constant) EvalTime(ctx sessionctx.Context, row chunk.Row) (val types.Time, isNull bool, err error) {
	dt, lazy, err := c.getLazyDatum(row)
	if err != nil {
		return types.ZeroTime, false, err
	}
	if !lazy {
		dt = c.Value
	}
	if c.GetType().GetType() == mysql.TypeNull || dt.IsNull() {
		return types.ZeroTime, true, nil
	}
	return dt.GetMysqlTime(), false, nil
}

// EvalDuration returns Duration representation of Constant.
func (c *Constant) EvalDuration(ctx sessionctx.Context, row chunk.Row) (val types.Duration, isNull bool, err error) {
	dt, lazy, err := c.getLazyDatum(row)
	if err != nil {
		return types.Duration{}, false, err
	}
	if !lazy {
		dt = c.Value
	}
	if c.GetType().GetType() == mysql.TypeNull || dt.IsNull() {
		return types.Duration{}, true, nil
	}
	return dt.GetMysqlDuration(), false, nil
}

// EvalJSON returns JSON representation of Constant.
func (c *Constant) EvalJSON(ctx sessionctx.Context, row chunk.Row) (json.BinaryJSON, bool, error) {
	dt, lazy, err := c.getLazyDatum(row)
	if err != nil {
		return json.BinaryJSON{}, false, err
	}
	if !lazy {
		dt = c.Value
	}
	if c.GetType().GetType() == mysql.TypeNull || dt.IsNull() {
		return json.BinaryJSON{}, true, nil
	}
	return dt.GetMysqlJSON(), false, nil
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
	con, err := c.Value.Compare(ctx.GetSessionVars().StmtCtx, &y.Value, collate.GetBinaryCollator())
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
func (c *Constant) ConstItem(sc *stmtctx.StatementContext) bool {
	return !sc.UseCache || (c.DeferredExpr == nil && c.ParamMarker == nil)
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

	if c.DeferredExpr != nil {
		c.hashcode = c.DeferredExpr.HashCode(sc)
		return c.hashcode
	}

	if c.ParamMarker != nil {
		c.hashcode = append(c.hashcode, parameterFlag)
		c.hashcode = codec.EncodeInt(c.hashcode, int64(c.ParamMarker.order))
		return c.hashcode
	}

	_, err := c.Eval(chunk.Row{})
	if err != nil {
		terror.Log(err)
	}
	c.hashcode = append(c.hashcode, constantFlag)
	c.hashcode = codec.HashCode(c.hashcode, c.Value)
	return c.hashcode
}

// ResolveIndices implements Expression interface.
func (c *Constant) ResolveIndices(_ *Schema) (Expression, error) {
	return c, nil
}

func (c *Constant) resolveIndices(_ *Schema) error {
	return nil
}

// ResolveIndicesByVirtualExpr implements Expression interface.
func (c *Constant) ResolveIndicesByVirtualExpr(_ *Schema) (Expression, bool) {
	return c, true
}

func (c *Constant) resolveIndicesByVirtualExpr(_ *Schema) bool {
	return true
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

// Coercibility returns the coercibility value which is used to check collations.
func (c *Constant) Coercibility() Coercibility {
	if !c.HasCoercibility() {
		c.SetCoercibility(deriveCoercibilityForConstant(c))
	}
	return c.collationInfo.Coercibility()
}
