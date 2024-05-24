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
	"unsafe"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/intest"
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

// NewUInt64Const stands for constant of a given number.
func NewUInt64Const(num int) *Constant {
	retT := types.NewFieldType(mysql.TypeLonglong)
	retT.AddFlag(mysql.UnsignedFlag) // shrink range to avoid integral promotion
	retT.SetFlen(mysql.MaxIntWidth)
	retT.SetDecimal(0)
	return &Constant{
		Value:   types.NewDatum(num),
		RetType: retT,
	}
}

// NewUInt64ConstWithFieldType stands for constant of a given number with specified fieldType.
func NewUInt64ConstWithFieldType(num uint64, fieldType *types.FieldType) *Constant {
	return &Constant{
		Value:   types.NewDatum(num),
		RetType: fieldType,
	}
}

// NewInt64Const stands for constant of a given number.
func NewInt64Const(num int64) *Constant {
	retT := types.NewFieldType(mysql.TypeLonglong)
	retT.SetFlen(mysql.MaxIntWidth)
	retT.SetDecimal(0)
	return &Constant{
		Value:   types.NewDatum(num),
		RetType: retT,
	}
}

// NewStrConst stands for constant of a given string.
// used in test only now.
func NewStrConst(str string) *Constant {
	retT := types.NewFieldType(mysql.TypeVarString)
	retT.SetFlen(len(str))
	return &Constant{
		Value:   types.NewDatum(str),
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

// NewNullWithFieldType stands for null constant with specified fieldType.
func NewNullWithFieldType(fieldType *types.FieldType) *Constant {
	return &Constant{
		Value:   types.NewDatum(nil),
		RetType: fieldType,
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
	ctx   variable.SessionVarsProvider
	order int
}

// GetUserVar returns the corresponding user variable presented in the `EXECUTE` statement or `COM_EXECUTE` command.
func (d *ParamMarker) GetUserVar(ctx EvalContext) types.Datum {
	return ctx.GetParamValue(d.order)
}

func (d *ParamMarker) getUserVarWithInternalCtx() types.Datum {
	// TODO: remove this function in the future
	sessionVars := d.ctx.GetSessionVars()
	return sessionVars.PlanCacheParams.GetParamValue(d.order)
}

// String implements fmt.Stringer interface.
func (c *Constant) String() string {
	if c.ParamMarker != nil {
		dt := c.ParamMarker.getUserVarWithInternalCtx()
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
func (c *Constant) GetType(ctx EvalContext) *types.FieldType {
	if c.ParamMarker != nil {
		// GetType() may be called in multi-threaded context, e.g, in building inner executors of IndexJoin,
		// so it should avoid data race. We achieve this by returning different FieldType pointer for each call.
		tp := types.NewFieldType(mysql.TypeUnspecified)
		dt := c.ParamMarker.GetUserVar(ctx)
		types.InferParamTypeFromDatum(&dt, tp)
		return tp
	}
	return c.RetType
}

// VecEvalInt evaluates this expression in a vectorized manner.
func (c *Constant) VecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	if c.DeferredExpr == nil {
		return genVecFromConstExpr(ctx, c, types.ETInt, input, result)
	}
	return c.DeferredExpr.VecEvalInt(ctx, input, result)
}

// VecEvalReal evaluates this expression in a vectorized manner.
func (c *Constant) VecEvalReal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	if c.DeferredExpr == nil {
		return genVecFromConstExpr(ctx, c, types.ETReal, input, result)
	}
	return c.DeferredExpr.VecEvalReal(ctx, input, result)
}

// VecEvalString evaluates this expression in a vectorized manner.
func (c *Constant) VecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	if c.DeferredExpr == nil {
		return genVecFromConstExpr(ctx, c, types.ETString, input, result)
	}
	return c.DeferredExpr.VecEvalString(ctx, input, result)
}

// VecEvalDecimal evaluates this expression in a vectorized manner.
func (c *Constant) VecEvalDecimal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	if c.DeferredExpr == nil {
		return genVecFromConstExpr(ctx, c, types.ETDecimal, input, result)
	}
	return c.DeferredExpr.VecEvalDecimal(ctx, input, result)
}

// VecEvalTime evaluates this expression in a vectorized manner.
func (c *Constant) VecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	if c.DeferredExpr == nil {
		return genVecFromConstExpr(ctx, c, types.ETTimestamp, input, result)
	}
	return c.DeferredExpr.VecEvalTime(ctx, input, result)
}

// VecEvalDuration evaluates this expression in a vectorized manner.
func (c *Constant) VecEvalDuration(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	if c.DeferredExpr == nil {
		return genVecFromConstExpr(ctx, c, types.ETDuration, input, result)
	}
	return c.DeferredExpr.VecEvalDuration(ctx, input, result)
}

// VecEvalJSON evaluates this expression in a vectorized manner.
func (c *Constant) VecEvalJSON(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	if c.DeferredExpr == nil {
		return genVecFromConstExpr(ctx, c, types.ETJson, input, result)
	}
	return c.DeferredExpr.VecEvalJSON(ctx, input, result)
}

func (c *Constant) getLazyDatum(ctx EvalContext, row chunk.Row) (dt types.Datum, isLazy bool, err error) {
	if c.ParamMarker != nil {
		return c.ParamMarker.GetUserVar(ctx), true, nil
	} else if c.DeferredExpr != nil {
		dt, err = c.DeferredExpr.Eval(ctx, row)
		return dt, true, err
	}
	return types.Datum{}, false, nil
}

// Traverse implements the TraverseDown interface.
func (c *Constant) Traverse(action TraverseAction) Expression {
	return action.Transform(c)
}

// Eval implements Expression interface.
func (c *Constant) Eval(ctx EvalContext, row chunk.Row) (types.Datum, error) {
	intest.AssertNotNil(ctx)
	if dt, lazy, err := c.getLazyDatum(ctx, row); lazy {
		if err != nil {
			return c.Value, err
		}
		if dt.IsNull() {
			c.Value.SetNull()
			return c.Value, nil
		}
		if c.DeferredExpr != nil {
			if dt.Kind() != types.KindMysqlDecimal {
				val, err := dt.ConvertTo(typeCtx(ctx), c.RetType)
				if err != nil {
					return dt, err
				}
				return val, nil
			}
			if err := c.adjustDecimal(ctx, dt.GetMysqlDecimal()); err != nil {
				return dt, err
			}
		}
		return dt, nil
	}
	return c.Value, nil
}

// EvalInt returns int representation of Constant.
func (c *Constant) EvalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	dt, lazy, err := c.getLazyDatum(ctx, row)
	if err != nil {
		return 0, false, err
	}
	if !lazy {
		dt = c.Value
	}
	if c.GetType(ctx).GetType() == mysql.TypeNull || dt.IsNull() {
		return 0, true, nil
	} else if dt.Kind() == types.KindBinaryLiteral {
		val, err := dt.GetBinaryLiteral().ToInt(typeCtx(ctx))
		return int64(val), err != nil, err
	} else if c.GetType(ctx).Hybrid() || dt.Kind() == types.KindString {
		res, err := dt.ToInt64(typeCtx(ctx))
		return res, false, err
	} else if dt.Kind() == types.KindMysqlBit {
		uintVal, err := dt.GetBinaryLiteral().ToInt(typeCtx(ctx))
		return int64(uintVal), false, err
	}
	return dt.GetInt64(), false, nil
}

// EvalReal returns real representation of Constant.
func (c *Constant) EvalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	dt, lazy, err := c.getLazyDatum(ctx, row)
	if err != nil {
		return 0, false, err
	}
	if !lazy {
		dt = c.Value
	}
	if c.GetType(ctx).GetType() == mysql.TypeNull || dt.IsNull() {
		return 0, true, nil
	}
	if c.GetType(ctx).Hybrid() || dt.Kind() == types.KindBinaryLiteral || dt.Kind() == types.KindString {
		res, err := dt.ToFloat64(typeCtx(ctx))
		return res, false, err
	}
	return dt.GetFloat64(), false, nil
}

// EvalString returns string representation of Constant.
func (c *Constant) EvalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	dt, lazy, err := c.getLazyDatum(ctx, row)
	if err != nil {
		return "", false, err
	}
	if !lazy {
		dt = c.Value
	}
	if c.GetType(ctx).GetType() == mysql.TypeNull || dt.IsNull() {
		return "", true, nil
	}
	res, err := dt.ToString()
	return res, false, err
}

// EvalDecimal returns decimal representation of Constant.
func (c *Constant) EvalDecimal(ctx EvalContext, row chunk.Row) (*types.MyDecimal, bool, error) {
	dt, lazy, err := c.getLazyDatum(ctx, row)
	if err != nil {
		return nil, false, err
	}
	if !lazy {
		dt = c.Value
	}
	if c.GetType(ctx).GetType() == mysql.TypeNull || dt.IsNull() {
		return nil, true, nil
	}
	res, err := dt.ToDecimal(typeCtx(ctx))
	if err != nil {
		return nil, false, err
	}
	if err := c.adjustDecimal(ctx, res); err != nil {
		return nil, false, err
	}
	return res, false, nil
}

func (c *Constant) adjustDecimal(ctx EvalContext, d *types.MyDecimal) error {
	// Decimal Value's precision and frac may be modified during plan building.
	_, frac := d.PrecisionAndFrac()
	if frac < c.GetType(ctx).GetDecimal() {
		return d.Round(d, c.GetType(ctx).GetDecimal(), types.ModeHalfUp)
	}
	return nil
}

// EvalTime returns DATE/DATETIME/TIMESTAMP representation of Constant.
func (c *Constant) EvalTime(ctx EvalContext, row chunk.Row) (val types.Time, isNull bool, err error) {
	dt, lazy, err := c.getLazyDatum(ctx, row)
	if err != nil {
		return types.ZeroTime, false, err
	}
	if !lazy {
		dt = c.Value
	}
	if c.GetType(ctx).GetType() == mysql.TypeNull || dt.IsNull() {
		return types.ZeroTime, true, nil
	}
	return dt.GetMysqlTime(), false, nil
}

// EvalDuration returns Duration representation of Constant.
func (c *Constant) EvalDuration(ctx EvalContext, row chunk.Row) (val types.Duration, isNull bool, err error) {
	dt, lazy, err := c.getLazyDatum(ctx, row)
	if err != nil {
		return types.Duration{}, false, err
	}
	if !lazy {
		dt = c.Value
	}
	if c.GetType(ctx).GetType() == mysql.TypeNull || dt.IsNull() {
		return types.Duration{}, true, nil
	}
	return dt.GetMysqlDuration(), false, nil
}

// EvalJSON returns JSON representation of Constant.
func (c *Constant) EvalJSON(ctx EvalContext, row chunk.Row) (types.BinaryJSON, bool, error) {
	dt, lazy, err := c.getLazyDatum(ctx, row)
	if err != nil {
		return types.BinaryJSON{}, false, err
	}
	if !lazy {
		dt = c.Value
	}
	if c.GetType(ctx).GetType() == mysql.TypeNull || dt.IsNull() {
		return types.BinaryJSON{}, true, nil
	}
	return dt.GetMysqlJSON(), false, nil
}

// Equal implements Expression interface.
func (c *Constant) Equal(ctx EvalContext, b Expression) bool {
	y, ok := b.(*Constant)
	if !ok {
		return false
	}
	_, err1 := y.Eval(ctx, chunk.Row{})
	_, err2 := c.Eval(ctx, chunk.Row{})
	if err1 != nil || err2 != nil {
		return false
	}
	con, err := c.Value.Compare(typeCtx(ctx), &y.Value, collate.GetBinaryCollator())
	if err != nil || con != 0 {
		return false
	}
	return true
}

// IsCorrelated implements Expression interface.
func (c *Constant) IsCorrelated() bool {
	return false
}

// ConstLevel returns the const level for the expression
func (c *Constant) ConstLevel() ConstLevel {
	if c.DeferredExpr != nil || c.ParamMarker != nil {
		return ConstOnlyInContext
	}
	return ConstStrict
}

// Decorrelate implements Expression interface.
func (c *Constant) Decorrelate(_ *Schema) Expression {
	return c
}

// HashCode implements Expression interface.
func (c *Constant) HashCode() []byte {
	return c.getHashCode(false)
}

// CanonicalHashCode implements Expression interface.
func (c *Constant) CanonicalHashCode() []byte {
	return c.getHashCode(true)
}

func (c *Constant) getHashCode(canonical bool) []byte {
	if len(c.hashcode) > 0 {
		return c.hashcode
	}

	if c.DeferredExpr != nil {
		if canonical {
			c.hashcode = c.DeferredExpr.CanonicalHashCode()
		} else {
			c.hashcode = c.DeferredExpr.HashCode()
		}
		return c.hashcode
	}

	if c.ParamMarker != nil {
		c.hashcode = append(c.hashcode, parameterFlag)
		c.hashcode = codec.EncodeInt(c.hashcode, int64(c.ParamMarker.order))
		return c.hashcode
	}

	intest.Assert(c.DeferredExpr == nil && c.ParamMarker == nil)
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
func (c *Constant) ResolveIndicesByVirtualExpr(_ EvalContext, _ *Schema) (Expression, bool) {
	return c, true
}

func (c *Constant) resolveIndicesByVirtualExpr(_ EvalContext, _ *Schema) bool {
	return true
}

// RemapColumn remaps columns with provided mapping and returns new expression
func (c *Constant) RemapColumn(_ map[int64]*Column) (Expression, error) {
	return c, nil
}

// Vectorized returns if this expression supports vectorized evaluation.
func (c *Constant) Vectorized() bool {
	if c.DeferredExpr != nil {
		return c.DeferredExpr.Vectorized()
	}
	return true
}

// Coercibility returns the coercibility value which is used to check collations.
func (c *Constant) Coercibility() Coercibility {
	if !c.HasCoercibility() {
		c.SetCoercibility(deriveCoercibilityForConstant(c))
	}
	return c.collationInfo.Coercibility()
}

const emptyConstantSize = int64(unsafe.Sizeof(Constant{}))

// MemoryUsage return the memory usage of Constant
func (c *Constant) MemoryUsage() (sum int64) {
	if c == nil {
		return
	}

	sum = emptyConstantSize + c.Value.MemUsage() + int64(cap(c.hashcode))
	if c.RetType != nil {
		sum += c.RetType.MemoryUsage()
	}
	return
}
