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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/planner/cascades/base"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/size"
)

var (
	_ base.HashEquals = &Column{}
	_ base.HashEquals = &CorrelatedColumn{}
)

// CorrelatedColumn stands for a column in a correlated sub query.
type CorrelatedColumn struct {
	Column

	Data *types.Datum
}

// SafeToShareAcrossSession returns if the function can be shared across different sessions.
func (col *CorrelatedColumn) SafeToShareAcrossSession() bool {
	// TODO: optimize this to make it's safe.
	return false // due to col.Data
}

// Clone implements Expression interface.
func (col *CorrelatedColumn) Clone() Expression {
	return &CorrelatedColumn{
		Column: col.Column,
		Data:   col.Data,
	}
}

// VecEvalInt evaluates this expression in a vectorized manner.
func (col *CorrelatedColumn) VecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	return genVecFromConstExpr(ctx, col, types.ETInt, input, result)
}

// VecEvalReal evaluates this expression in a vectorized manner.
func (col *CorrelatedColumn) VecEvalReal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	return genVecFromConstExpr(ctx, col, types.ETReal, input, result)
}

// VecEvalString evaluates this expression in a vectorized manner.
func (col *CorrelatedColumn) VecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	return genVecFromConstExpr(ctx, col, types.ETString, input, result)
}

// VecEvalDecimal evaluates this expression in a vectorized manner.
func (col *CorrelatedColumn) VecEvalDecimal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	return genVecFromConstExpr(ctx, col, types.ETDecimal, input, result)
}

// VecEvalTime evaluates this expression in a vectorized manner.
func (col *CorrelatedColumn) VecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	return genVecFromConstExpr(ctx, col, types.ETTimestamp, input, result)
}

// VecEvalDuration evaluates this expression in a vectorized manner.
func (col *CorrelatedColumn) VecEvalDuration(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	return genVecFromConstExpr(ctx, col, types.ETDuration, input, result)
}

// VecEvalJSON evaluates this expression in a vectorized manner.
func (col *CorrelatedColumn) VecEvalJSON(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	return genVecFromConstExpr(ctx, col, types.ETJson, input, result)
}

// VecEvalVectorFloat32 evaluates this expression in a vectorized manner.
func (col *CorrelatedColumn) VecEvalVectorFloat32(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	return genVecFromConstExpr(ctx, col, types.ETVectorFloat32, input, result)
}

// Traverse implements the TraverseDown interface.
func (col *CorrelatedColumn) Traverse(action TraverseAction) Expression {
	return action.Transform(col)
}

// Eval implements Expression interface.
func (col *CorrelatedColumn) Eval(_ EvalContext, _ chunk.Row) (types.Datum, error) {
	return *col.Data, nil
}

// EvalInt returns int representation of CorrelatedColumn.
func (col *CorrelatedColumn) EvalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	if col.Data.IsNull() {
		return 0, true, nil
	}
	if col.GetType(ctx).Hybrid() {
		res, err := col.Data.ToInt64(typeCtx(ctx))
		return res, err != nil, err
	}
	return col.Data.GetInt64(), false, nil
}

// EvalReal returns real representation of CorrelatedColumn.
func (col *CorrelatedColumn) EvalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	if col.Data.IsNull() {
		return 0, true, nil
	}
	return col.Data.GetFloat64(), false, nil
}

// EvalString returns string representation of CorrelatedColumn.
func (col *CorrelatedColumn) EvalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	if col.Data.IsNull() {
		return "", true, nil
	}
	res, err := col.Data.ToString()
	return res, err != nil, err
}

// EvalDecimal returns decimal representation of CorrelatedColumn.
func (col *CorrelatedColumn) EvalDecimal(ctx EvalContext, row chunk.Row) (*types.MyDecimal, bool, error) {
	if col.Data.IsNull() {
		return nil, true, nil
	}
	return col.Data.GetMysqlDecimal(), false, nil
}

// EvalTime returns DATE/DATETIME/TIMESTAMP representation of CorrelatedColumn.
func (col *CorrelatedColumn) EvalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
	if col.Data.IsNull() {
		return types.ZeroTime, true, nil
	}
	return col.Data.GetMysqlTime(), false, nil
}

// EvalDuration returns Duration representation of CorrelatedColumn.
func (col *CorrelatedColumn) EvalDuration(ctx EvalContext, row chunk.Row) (types.Duration, bool, error) {
	if col.Data.IsNull() {
		return types.Duration{}, true, nil
	}
	return col.Data.GetMysqlDuration(), false, nil
}

// EvalJSON returns JSON representation of CorrelatedColumn.
func (col *CorrelatedColumn) EvalJSON(ctx EvalContext, row chunk.Row) (types.BinaryJSON, bool, error) {
	if col.Data.IsNull() {
		return types.BinaryJSON{}, true, nil
	}
	return col.Data.GetMysqlJSON(), false, nil
}

// EvalVectorFloat32 returns VectorFloat32 representation of CorrelatedColumn.
func (col *CorrelatedColumn) EvalVectorFloat32(ctx EvalContext, row chunk.Row) (types.VectorFloat32, bool, error) {
	if col.Data.IsNull() {
		return types.ZeroVectorFloat32, true, nil
	}
	return col.Data.GetVectorFloat32(), false, nil
}

// Equal implements Expression interface.
func (col *CorrelatedColumn) Equal(_ EvalContext, expr Expression) bool {
	return col.EqualColumn(expr)
}

// EqualColumn returns whether two column is equal
func (col *CorrelatedColumn) EqualColumn(expr Expression) bool {
	if cc, ok := expr.(*CorrelatedColumn); ok {
		return col.Column.EqualColumn(&cc.Column)
	}
	return false
}

// IsCorrelated implements Expression interface.
func (col *CorrelatedColumn) IsCorrelated() bool {
	return true
}

// ConstLevel returns the const level for the expression
func (col *CorrelatedColumn) ConstLevel() ConstLevel {
	return ConstNone
}

// Decorrelate implements Expression interface.
func (col *CorrelatedColumn) Decorrelate(schema *Schema) Expression {
	if !schema.Contains(&col.Column) {
		return col
	}
	return &col.Column
}

// ResolveIndices implements Expression interface.
func (col *CorrelatedColumn) ResolveIndices(_ *Schema) (Expression, error) {
	return col, nil
}

func (col *CorrelatedColumn) resolveIndices(_ *Schema) error {
	return nil
}

// ResolveIndicesByVirtualExpr implements Expression interface.
func (col *CorrelatedColumn) ResolveIndicesByVirtualExpr(_ EvalContext, _ *Schema) (Expression, bool) {
	return col, true
}

func (col *CorrelatedColumn) resolveIndicesByVirtualExpr(_ EvalContext, _ *Schema) bool {
	return true
}

// MemoryUsage return the memory usage of CorrelatedColumn
func (col *CorrelatedColumn) MemoryUsage() (sum int64) {
	if col == nil {
		return
	}

	sum = col.Column.MemoryUsage() + size.SizeOfPointer
	if col.Data != nil {
		sum += col.Data.MemUsage()
	}
	return sum
}

// RemapColumn remaps columns with provided mapping and returns new expression
func (col *CorrelatedColumn) RemapColumn(m map[int64]*Column) (Expression, error) {
	mapped := m[(&col.Column).UniqueID]
	if mapped == nil {
		return nil, errors.Errorf("Can't remap column for %s", col)
	}
	return &CorrelatedColumn{
		Column: *mapped,
		Data:   col.Data,
	}, nil
}

// Hash64 implements HashEquals.<0th> interface.
func (col *CorrelatedColumn) Hash64(h base.Hasher) {
	// correlatedColumn flag here is used to distinguish correlatedColumn and Column.
	h.HashByte(correlatedColumn)
	col.Column.Hash64(h)
	// since col.Datum is filled in the runtime, we can't use it to calculate hash now, correlatedColumn flag + column is enough.
}

// Equals implements HashEquals.<1st> interface.
func (col *CorrelatedColumn) Equals(other any) bool {
	col2, ok := other.(*CorrelatedColumn)
	if !ok {
		return false
	}
	if col == nil {
		return col2 == nil
	}
	if col2 == nil {
		return false
	}
	return col.Column.Equals(&col2.Column)
}

