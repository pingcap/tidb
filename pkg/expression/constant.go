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

	perrors "github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/cascades/base"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

var _ base.HashEquals = &Constant{}

// NewOne stands for an unsigned number 1.
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

// NewSignedOne stands for a signed number 1.
func NewSignedOne() *Constant {
	retT := types.NewFieldType(mysql.TypeTiny)
	retT.SetFlen(1)
	retT.SetDecimal(0)
	return &Constant{
		Value:   types.NewDatum(1),
		RetType: retT,
	}
}

// NewZero stands for an unsigned number 0.
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

// NewSignedZero stands for a signed number 0.
func NewSignedZero() *Constant {
	retT := types.NewFieldType(mysql.TypeTiny)
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
	RetType *types.FieldType `plan-cache-clone:"shallow"`
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
	order int
}

// SafeToShareAcrossSession returns if the function can be shared across different sessions.
func (c *Constant) SafeToShareAcrossSession() bool {
	if c.DeferredExpr != nil {
		return c.DeferredExpr.SafeToShareAcrossSession()
	}
	return true
}

// GetUserVar returns the corresponding user variable presented in the `EXECUTE` statement or `COM_EXECUTE` command.
func (d *ParamMarker) GetUserVar(ctx ParamValues) (types.Datum, error) {
	return ctx.GetParamValue(d.order)
}

// StringWithCtx implements Expression interface.
func (c *Constant) StringWithCtx(ctx ParamValues, redact string) string {
	v := c.Value
	if c.ParamMarker != nil {
		dt, err := c.ParamMarker.GetUserVar(ctx)
		intest.AssertNoError(err, "fail to get param")
		if err != nil {
			return "?"
		}
		v = dt
	} else if c.DeferredExpr != nil {
		return c.DeferredExpr.StringWithCtx(ctx, redact)
	}
	if redact == perrors.RedactLogDisable {
		return v.TruncatedStringify()
	} else if redact == perrors.RedactLogMarker {
		return fmt.Sprintf("‹%s›", v.TruncatedStringify())
	}
	return "?"
}

// Clone implements Expression interface.
func (c *Constant) Clone() Expression {
	con := *c
	if c.ParamMarker != nil {
		con.ParamMarker = &ParamMarker{order: c.ParamMarker.order}
	}
	if c.DeferredExpr != nil {
		con.DeferredExpr = c.DeferredExpr.Clone()
	}
	if c.hashcode != nil {
		con.hashcode = make([]byte, len(c.hashcode))
		copy(con.hashcode, c.hashcode)
	}
	return &con
}

// GetType implements Expression interface.
func (c *Constant) GetType(ctx EvalContext) *types.FieldType {
	if c.ParamMarker != nil {
		// GetType() may be called in multi-threaded context, e.g, in building inner executors of IndexJoin,
		// so it should avoid data race. We achieve this by returning different FieldType pointer for each call.
		tp := types.NewFieldType(mysql.TypeUnspecified)
		dt, err := c.ParamMarker.GetUserVar(ctx)
		intest.AssertNoError(err, "fail to get param")
		if err != nil {
			logutil.BgLogger().Warn("fail to get param", zap.Error(err))
			return nil
		}
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

// VecEvalVectorFloat32 evaluates this expression in a vectorized manner.
func (c *Constant) VecEvalVectorFloat32(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	if c.DeferredExpr == nil {
		return genVecFromConstExpr(ctx, c, types.ETVectorFloat32, input, result)
	}
	return c.DeferredExpr.VecEvalVectorFloat32(ctx, input, result)
}

func (c *Constant) getLazyDatum(ctx EvalContext, row chunk.Row) (dt types.Datum, isLazy bool, err error) {
	if c.ParamMarker != nil {
		val, err := c.ParamMarker.GetUserVar(ctx)
		intest.AssertNoError(err, "fail to get param")
		if err != nil {
			return val, true, err
		}
		return val, true, nil
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
			return dt, nil
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

// EvalVectorFloat32 returns VectorFloat32 representation of Constant.
func (c *Constant) EvalVectorFloat32(ctx EvalContext, row chunk.Row) (types.VectorFloat32, bool, error) {
	dt, lazy, err := c.getLazyDatum(ctx, row)
	if err != nil {
		return types.ZeroVectorFloat32, false, err
	}
	if !lazy {
		dt = c.Value
	}
	if c.GetType(ctx).GetType() == mysql.TypeNull || dt.IsNull() {
		return types.ZeroVectorFloat32, true, nil
	}
	return dt.GetVectorFloat32(), false, nil
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

// Hash64 implements HashEquals.<0th> interface.
func (c *Constant) Hash64(h base.Hasher) {
	if c.RetType == nil {
		h.HashByte(base.NilFlag)
	} else {
		h.HashByte(base.NotNilFlag)
		c.RetType.Hash64(h)
	}
	c.collationInfo.Hash64(h)
	if c.DeferredExpr != nil {
		c.DeferredExpr.Hash64(h)
		return
	}
	if c.ParamMarker != nil {
		h.HashByte(parameterFlag)
		h.HashInt64(int64(c.ParamMarker.order))
		return
	}
	intest.Assert(c.DeferredExpr == nil && c.ParamMarker == nil)
	h.HashByte(constantFlag)
	c.Value.Hash64(h)
}

// Equals implements HashEquals.<1st> interface.
func (c *Constant) Equals(other any) bool {
	c2, ok := other.(*Constant)
	if !ok {
		return false
	}
	if c == nil {
		return c2 == nil
	}
	if c2 == nil {
		return false
	}
	ok = c.RetType == nil && c2.RetType == nil || c.RetType != nil && c2.RetType != nil && c.RetType.Equals(c2.RetType)
	ok = ok && c.collationInfo.Equals(c2.collationInfo)
	ok = ok && (c.DeferredExpr == nil && c2.DeferredExpr == nil || c.DeferredExpr != nil && c2.DeferredExpr != nil && c.DeferredExpr.Equals(c2.DeferredExpr))
	ok = ok && (c.ParamMarker == nil && c2.ParamMarker == nil || c.ParamMarker != nil && c2.ParamMarker != nil && c.ParamMarker.order == c2.ParamMarker.order)
	return ok && c.Value.Equals(c2.Value)
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
