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
	"cmp"
	"fmt"
	"slices"
	"strings"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/size"
)

// CorrelatedColumn stands for a column in a correlated sub query.
type CorrelatedColumn struct {
	Column

	Data *types.Datum
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

// Column represents a column.
type Column struct {
	RetType *types.FieldType
	// ID is used to specify whether this column is ExtraHandleColumn or to access histogram.
	// We'll try to remove it in the future.
	ID int64
	// UniqueID is the unique id of this column.
	UniqueID int64

	// Index is used for execution, to tell the column's position in the given row.
	Index int

	hashcode []byte

	// VirtualExpr is used to save expression for virtual column
	VirtualExpr Expression

	OrigName string
	IsHidden bool

	// IsPrefix indicates whether this column is a prefix column in index.
	//
	// for example:
	// 	pk(col1, col2), index(col1(10)), key: col1(10)_col1_col2 => index's col1 will be true
	// 	pk(col1(10), col2), index(col1), key: col1_col1(10)_col2 => pk's col1 will be true
	IsPrefix bool

	// InOperand indicates whether this column is the inner operand of column equal condition converted
	// from `[not] in (subq)`.
	InOperand bool

	collationInfo

	CorrelatedColUniqueID int64
}

// Equal implements Expression interface.
func (col *Column) Equal(_ EvalContext, expr Expression) bool {
	return col.EqualColumn(expr)
}

// EqualColumn returns whether two column is equal
func (col *Column) EqualColumn(expr Expression) bool {
	if newCol, ok := expr.(*Column); ok {
		return newCol.UniqueID == col.UniqueID
	}
	return false
}

// EqualByExprAndID extends Equal by comparing virtual expression
func (col *Column) EqualByExprAndID(ctx EvalContext, expr Expression) bool {
	if newCol, ok := expr.(*Column); ok {
		expr, isOk := col.VirtualExpr.(*ScalarFunction)
		isVirExprMatched := isOk && expr.Equal(ctx, newCol.VirtualExpr) && col.RetType.Equal(newCol.RetType)
		return (newCol.UniqueID == col.UniqueID) || isVirExprMatched
	}
	return false
}

// VecEvalInt evaluates this expression in a vectorized manner.
func (col *Column) VecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	if col.RetType.Hybrid() {
		it := chunk.NewIterator4Chunk(input)
		result.ResizeInt64(0, false)
		for row := it.Begin(); row != it.End(); row = it.Next() {
			v, null, err := col.EvalInt(ctx, row)
			if err != nil {
				return err
			}
			if null {
				result.AppendNull()
			} else {
				result.AppendInt64(v)
			}
		}
		return nil
	}
	input.Column(col.Index).CopyReconstruct(input.Sel(), result)
	return nil
}

// VecEvalReal evaluates this expression in a vectorized manner.
func (col *Column) VecEvalReal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	src := input.Column(col.Index)
	if col.GetType(ctx).GetType() == mysql.TypeFloat {
		result.ResizeFloat64(n, false)
		f32s := src.Float32s()
		f64s := result.Float64s()
		sel := input.Sel()
		if sel != nil {
			for i, j := range sel {
				if src.IsNull(j) {
					result.SetNull(i, true)
				} else {
					f64s[i] = float64(f32s[j])
				}
			}
			return nil
		}
		result.MergeNulls(src)
		for i := range f32s {
			if result.IsNull(i) {
				continue
			}
			f64s[i] = float64(f32s[i])
		}
		return nil
	}
	input.Column(col.Index).CopyReconstruct(input.Sel(), result)
	return nil
}

// VecEvalString evaluates this expression in a vectorized manner.
func (col *Column) VecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	if col.RetType.Hybrid() {
		it := chunk.NewIterator4Chunk(input)
		result.ReserveString(input.NumRows())
		for row := it.Begin(); row != it.End(); row = it.Next() {
			v, null, err := col.EvalString(ctx, row)
			if err != nil {
				return err
			}
			if null {
				result.AppendNull()
			} else {
				result.AppendString(v)
			}
		}
		return nil
	}
	input.Column(col.Index).CopyReconstruct(input.Sel(), result)
	return nil
}

// VecEvalDecimal evaluates this expression in a vectorized manner.
func (col *Column) VecEvalDecimal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	input.Column(col.Index).CopyReconstruct(input.Sel(), result)
	return nil
}

// VecEvalTime evaluates this expression in a vectorized manner.
func (col *Column) VecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	input.Column(col.Index).CopyReconstruct(input.Sel(), result)
	return nil
}

// VecEvalDuration evaluates this expression in a vectorized manner.
func (col *Column) VecEvalDuration(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	input.Column(col.Index).CopyReconstruct(input.Sel(), result)
	return nil
}

// VecEvalJSON evaluates this expression in a vectorized manner.
func (col *Column) VecEvalJSON(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	input.Column(col.Index).CopyReconstruct(input.Sel(), result)
	return nil
}

const columnPrefix = "Column#"

// String implements Stringer interface.
func (col *Column) String() string {
	if col.IsHidden && col.VirtualExpr != nil {
		// A hidden column without virtual expression indicates it's a stored type.
		return col.VirtualExpr.String()
	}
	if col.OrigName != "" {
		return col.OrigName
	}
	var builder strings.Builder
	fmt.Fprintf(&builder, "%s%d", columnPrefix, col.UniqueID)
	return builder.String()
}

// MarshalJSON implements json.Marshaler interface.
func (col *Column) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("%q", col)), nil
}

// GetType implements Expression interface.
func (col *Column) GetType(_ EvalContext) *types.FieldType {
	return col.GetStaticType()
}

// GetStaticType returns the type without considering the context.
func (col *Column) GetStaticType() *types.FieldType {
	return col.RetType
}

// Traverse implements the TraverseDown interface.
func (col *Column) Traverse(action TraverseAction) Expression {
	return action.Transform(col)
}

// Eval implements Expression interface.
func (col *Column) Eval(_ EvalContext, row chunk.Row) (types.Datum, error) {
	return row.GetDatum(col.Index, col.RetType), nil
}

// EvalInt returns int representation of Column.
func (col *Column) EvalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	if col.GetType(ctx).Hybrid() {
		val := row.GetDatum(col.Index, col.RetType)
		if val.IsNull() {
			return 0, true, nil
		}
		if val.Kind() == types.KindMysqlBit {
			val, err := val.GetBinaryLiteral().ToInt(typeCtx(ctx))
			return int64(val), err != nil, err
		}
		res, err := val.ToInt64(typeCtx(ctx))
		return res, err != nil, err
	}
	if row.IsNull(col.Index) {
		return 0, true, nil
	}
	return row.GetInt64(col.Index), false, nil
}

// EvalReal returns real representation of Column.
func (col *Column) EvalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	if row.IsNull(col.Index) {
		return 0, true, nil
	}
	if col.GetType(ctx).GetType() == mysql.TypeFloat {
		return float64(row.GetFloat32(col.Index)), false, nil
	}
	return row.GetFloat64(col.Index), false, nil
}

// EvalString returns string representation of Column.
func (col *Column) EvalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	if row.IsNull(col.Index) {
		return "", true, nil
	}

	// Specially handle the ENUM/SET/BIT input value.
	if col.GetType(ctx).Hybrid() {
		val := row.GetDatum(col.Index, col.RetType)
		res, err := val.ToString()
		return res, err != nil, err
	}

	val := row.GetString(col.Index)
	return val, false, nil
}

// EvalDecimal returns decimal representation of Column.
func (col *Column) EvalDecimal(ctx EvalContext, row chunk.Row) (*types.MyDecimal, bool, error) {
	if row.IsNull(col.Index) {
		return nil, true, nil
	}
	return row.GetMyDecimal(col.Index), false, nil
}

// EvalTime returns DATE/DATETIME/TIMESTAMP representation of Column.
func (col *Column) EvalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
	if row.IsNull(col.Index) {
		return types.ZeroTime, true, nil
	}
	return row.GetTime(col.Index), false, nil
}

// EvalDuration returns Duration representation of Column.
func (col *Column) EvalDuration(ctx EvalContext, row chunk.Row) (types.Duration, bool, error) {
	if row.IsNull(col.Index) {
		return types.Duration{}, true, nil
	}
	duration := row.GetDuration(col.Index, col.RetType.GetDecimal())
	return duration, false, nil
}

// EvalJSON returns JSON representation of Column.
func (col *Column) EvalJSON(ctx EvalContext, row chunk.Row) (types.BinaryJSON, bool, error) {
	if row.IsNull(col.Index) {
		return types.BinaryJSON{}, true, nil
	}
	return row.GetJSON(col.Index), false, nil
}

// Clone implements Expression interface.
func (col *Column) Clone() Expression {
	newCol := *col
	return &newCol
}

// IsCorrelated implements Expression interface.
func (col *Column) IsCorrelated() bool {
	return false
}

// ConstLevel returns the const level for the expression
func (col *Column) ConstLevel() ConstLevel {
	return ConstNone
}

// Decorrelate implements Expression interface.
func (col *Column) Decorrelate(_ *Schema) Expression {
	return col
}

// HashCode implements Expression interface.
func (col *Column) HashCode() []byte {
	if len(col.hashcode) != 0 {
		return col.hashcode
	}
	col.hashcode = make([]byte, 0, 9)
	col.hashcode = append(col.hashcode, columnFlag)
	col.hashcode = codec.EncodeInt(col.hashcode, col.UniqueID)
	return col.hashcode
}

// CanonicalHashCode implements Expression interface.
func (col *Column) CanonicalHashCode() []byte {
	return col.HashCode()
}

// CleanHashCode will clean the hashcode you may be cached before. It's used especially in schema-cloned & reallocated-uniqueID's cases.
func (col *Column) CleanHashCode() {
	col.hashcode = make([]byte, 0, 9)
}

// ResolveIndices implements Expression interface.
func (col *Column) ResolveIndices(schema *Schema) (Expression, error) {
	newCol := col.Clone()
	err := newCol.resolveIndices(schema)
	return newCol, err
}

func (col *Column) resolveIndices(schema *Schema) error {
	col.Index = schema.ColumnIndex(col)
	if col.Index == -1 {
		return errors.Errorf("Can't find column %s in schema %s", col, schema)
	}
	return nil
}

// ResolveIndicesByVirtualExpr implements Expression interface.
func (col *Column) ResolveIndicesByVirtualExpr(ctx EvalContext, schema *Schema) (Expression, bool) {
	newCol := col.Clone()
	isOk := newCol.resolveIndicesByVirtualExpr(ctx, schema)
	return newCol, isOk
}

func (col *Column) resolveIndicesByVirtualExpr(ctx EvalContext, schema *Schema) bool {
	for i, c := range schema.Columns {
		if c.EqualByExprAndID(ctx, col) {
			col.Index = i
			return true
		}
	}
	return false
}

// RemapColumn remaps columns with provided mapping and returns new expression
func (col *Column) RemapColumn(m map[int64]*Column) (Expression, error) {
	mapped := m[col.UniqueID]
	if mapped == nil {
		return nil, errors.Errorf("Can't remap column for %s", col)
	}
	return mapped, nil
}

// Vectorized returns if this expression supports vectorized evaluation.
func (col *Column) Vectorized() bool {
	return true
}

// ToInfo converts the expression.Column to model.ColumnInfo for casting values,
// beware it doesn't fill all the fields of the model.ColumnInfo.
func (col *Column) ToInfo() *model.ColumnInfo {
	return &model.ColumnInfo{
		ID:        col.ID,
		FieldType: *col.RetType,
	}
}

// Column2Exprs will transfer column slice to expression slice.
func Column2Exprs(cols []*Column) []Expression {
	result := make([]Expression, 0, len(cols))
	for _, col := range cols {
		result = append(result, col)
	}
	return result
}

// ColInfo2Col finds the corresponding column of the ColumnInfo in a column slice.
func ColInfo2Col(cols []*Column, col *model.ColumnInfo) *Column {
	for _, c := range cols {
		if c.ID == col.ID {
			return c
		}
	}
	return nil
}

// IndexCol2Col finds the corresponding column of the IndexColumn in a column slice.
func IndexCol2Col(colInfos []*model.ColumnInfo, cols []*Column, col *model.IndexColumn) *Column {
	for i, info := range colInfos {
		if info.Name.L == col.Name.L {
			if col.Length > 0 && info.FieldType.GetFlen() > col.Length {
				c := *cols[i]
				c.IsPrefix = true
				return &c
			}
			return cols[i]
		}
	}
	return nil
}

// IndexInfo2PrefixCols gets the corresponding []*Column of the indexInfo's []*IndexColumn,
// together with a []int containing their lengths.
// If this index has three IndexColumn that the 1st and 3rd IndexColumn has corresponding *Column,
// the return value will be only the 1st corresponding *Column and its length.
// TODO: Use a struct to represent {*Column, int}. And merge IndexInfo2PrefixCols and IndexInfo2Cols.
func IndexInfo2PrefixCols(colInfos []*model.ColumnInfo, cols []*Column, index *model.IndexInfo) ([]*Column, []int) {
	retCols := make([]*Column, 0, len(index.Columns))
	lengths := make([]int, 0, len(index.Columns))
	for _, c := range index.Columns {
		col := IndexCol2Col(colInfos, cols, c)
		if col == nil {
			return retCols, lengths
		}
		retCols = append(retCols, col)
		if c.Length != types.UnspecifiedLength && c.Length == col.RetType.GetFlen() {
			lengths = append(lengths, types.UnspecifiedLength)
		} else {
			lengths = append(lengths, c.Length)
		}
	}
	return retCols, lengths
}

// IndexInfo2Cols gets the corresponding []*Column of the indexInfo's []*IndexColumn,
// together with a []int containing their lengths.
// If this index has three IndexColumn that the 1st and 3rd IndexColumn has corresponding *Column,
// the return value will be [col1, nil, col2].
func IndexInfo2Cols(colInfos []*model.ColumnInfo, cols []*Column, index *model.IndexInfo) ([]*Column, []int) {
	retCols := make([]*Column, 0, len(index.Columns))
	lens := make([]int, 0, len(index.Columns))
	for _, c := range index.Columns {
		col := IndexCol2Col(colInfos, cols, c)
		if col == nil {
			retCols = append(retCols, col)
			lens = append(lens, types.UnspecifiedLength)
			continue
		}
		retCols = append(retCols, col)
		if c.Length != types.UnspecifiedLength && c.Length == col.RetType.GetFlen() {
			lens = append(lens, types.UnspecifiedLength)
		} else {
			lens = append(lens, c.Length)
		}
	}
	return retCols, lens
}

// FindPrefixOfIndex will find columns in index by checking the unique id.
// So it will return at once no matching column is found.
func FindPrefixOfIndex(cols []*Column, idxColIDs []int64) []*Column {
	retCols := make([]*Column, 0, len(idxColIDs))
idLoop:
	for _, id := range idxColIDs {
		for _, col := range cols {
			if col.UniqueID == id {
				retCols = append(retCols, col)
				continue idLoop
			}
		}
		// If no matching column is found, just return.
		return retCols
	}
	return retCols
}

// EvalVirtualColumn evals the virtual column
func (col *Column) EvalVirtualColumn(ctx EvalContext, row chunk.Row) (types.Datum, error) {
	return col.VirtualExpr.Eval(ctx, row)
}

// Coercibility returns the coercibility value which is used to check collations.
func (col *Column) Coercibility() Coercibility {
	if !col.HasCoercibility() {
		col.SetCoercibility(deriveCoercibilityForColumn(col))
	}
	return col.collationInfo.Coercibility()
}

// Repertoire returns the repertoire value which is used to check collations.
func (col *Column) Repertoire() Repertoire {
	if col.repertoire != 0 {
		return col.repertoire
	}
	switch col.RetType.EvalType() {
	case types.ETJson:
		return UNICODE
	case types.ETString:
		if col.RetType.GetCharset() == charset.CharsetASCII {
			return ASCII
		}
		return UNICODE
	default:
		return ASCII
	}
}

// SortColumns sort columns based on UniqueID.
func SortColumns(cols []*Column) []*Column {
	sorted := make([]*Column, len(cols))
	copy(sorted, cols)
	slices.SortFunc(sorted, func(i, j *Column) int {
		return cmp.Compare(i.UniqueID, j.UniqueID)
	})
	return sorted
}

// InColumnArray check whether the col is in the cols array
func (col *Column) InColumnArray(cols []*Column) bool {
	for _, c := range cols {
		if col.EqualColumn(c) {
			return true
		}
	}
	return false
}

// GcColumnExprIsTidbShard check whether the expression is tidb_shard()
func GcColumnExprIsTidbShard(virtualExpr Expression) bool {
	if virtualExpr == nil {
		return false
	}

	f, ok := virtualExpr.(*ScalarFunction)
	if !ok {
		return false
	}

	if f.FuncName.L != ast.TiDBShard {
		return false
	}

	return true
}

const emptyColumnSize = int64(unsafe.Sizeof(Column{}))

// MemoryUsage return the memory usage of Column
func (col *Column) MemoryUsage() (sum int64) {
	if col == nil {
		return
	}

	sum = emptyColumnSize + int64(cap(col.hashcode)) + int64(len(col.OrigName)+len(col.charset)+len(col.collation))

	if col.RetType != nil {
		sum += col.RetType.MemoryUsage()
	}
	if col.VirtualExpr != nil {
		sum += col.VirtualExpr.MemoryUsage()
	}
	return
}
