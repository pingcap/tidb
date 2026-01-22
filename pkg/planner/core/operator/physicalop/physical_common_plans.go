// Copyright 2025 PingCAP, Inc.
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

package physicalop

import (
	"cmp"
	"slices"
	"sort"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/baseimpl"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/size"
)

// InsertGeneratedColumns is for completing generated columns in Insert.
// We resolve generation expressions in plan, and eval those in executor.
type InsertGeneratedColumns struct {
	Exprs        []expression.Expression
	OnDuplicates []*expression.Assignment
}

func (i InsertGeneratedColumns) cloneForPlanCache() InsertGeneratedColumns {
	return InsertGeneratedColumns{
		Exprs:        utilfuncp.CloneExpressionsForPlanCache(i.Exprs, nil),
		OnDuplicates: util.CloneAssignments(i.OnDuplicates),
	}
}

// MemoryUsage return the memory usage of InsertGeneratedColumns
func (i *InsertGeneratedColumns) MemoryUsage() (sum int64) {
	if i == nil {
		return
	}
	sum = size.SizeOfSlice*3 + int64(cap(i.OnDuplicates))*size.SizeOfPointer + int64(cap(i.Exprs))*size.SizeOfInterface

	for _, expr := range i.Exprs {
		sum += expr.MemoryUsage()
	}
	for _, as := range i.OnDuplicates {
		sum += as.MemoryUsage()
	}
	return
}

// Insert represents an insert plan.
type Insert struct {
	SimpleSchemaProducer

	Table         table.Table        `plan-cache-clone:"shallow"`
	TableSchema   *expression.Schema `plan-cache-clone:"shallow"`
	TableColNames types.NameSlice    `plan-cache-clone:"shallow"`
	Columns       []*ast.ColumnName  `plan-cache-clone:"shallow"`
	Lists         [][]expression.Expression

	OnDuplicate        []*expression.Assignment
	Schema4OnDuplicate *expression.Schema `plan-cache-clone:"shallow"`
	Names4OnDuplicate  types.NameSlice    `plan-cache-clone:"shallow"`

	GenCols InsertGeneratedColumns

	SelectPlan base.PhysicalPlan

	IsReplace bool
	IgnoreErr bool

	// NeedFillDefaultValue is true when expr in value list reference other column.
	NeedFillDefaultValue bool

	AllAssignmentsAreConstant bool

	RowLen int

	FKChecks   []*FKCheck   `plan-cache-clone:"must-nil"`
	FKCascades []*FKCascade `plan-cache-clone:"must-nil"`

	// Returning stores the RETURNING clause expression list.
	Returning       []expression.Expression
	ReturningSchema *expression.Schema `plan-cache-clone:"shallow"`
	ReturningNames  types.NameSlice    `plan-cache-clone:"shallow"`
}

// Init initializes Insert.
func (p Insert) Init(ctx base.PlanContext) *Insert {
	p.Plan = baseimpl.NewBasePlan(ctx, plancodec.TypeInsert, 0)
	return &p
}

// MemoryUsage return the memory usage of Insert
func (p *Insert) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.SimpleSchemaProducer.MemoryUsage() + size.SizeOfInterface + size.SizeOfSlice*7 + int64(cap(p.TableColNames)+
		cap(p.Columns)+cap(p.OnDuplicate)+cap(p.Names4OnDuplicate)+cap(p.FKChecks))*size.SizeOfPointer +
		p.GenCols.MemoryUsage() + size.SizeOfInterface + size.SizeOfBool*4 + size.SizeOfInt
	if p.TableSchema != nil {
		sum += p.TableSchema.MemoryUsage()
	}
	if p.Schema4OnDuplicate != nil {
		sum += p.Schema4OnDuplicate.MemoryUsage()
	}
	if p.SelectPlan != nil {
		sum += p.SelectPlan.MemoryUsage()
	}

	for _, name := range p.TableColNames {
		sum += name.MemoryUsage()
	}
	for _, exprs := range p.Lists {
		sum += size.SizeOfSlice + int64(cap(exprs))*size.SizeOfInterface
		for _, expr := range exprs {
			sum += expr.MemoryUsage()
		}
	}
	for _, as := range p.OnDuplicate {
		sum += as.MemoryUsage()
	}
	for _, name := range p.Names4OnDuplicate {
		sum += name.MemoryUsage()
	}
	for _, fkC := range p.FKChecks {
		sum += fkC.MemoryUsage()
	}

	return
}

// Update represents Update plan.
type Update struct {
	SimpleSchemaProducer

	OrderedList []*expression.Assignment

	AllAssignmentsAreConstant bool

	IgnoreError bool

	VirtualAssignmentsOffset int

	SelectPlan base.PhysicalPlan

	// TblColPosInfos is for multi-table update statement.
	// It records the column position of each related table.
	TblColPosInfos TblColPosInfoSlice `plan-cache-clone:"shallow"`

	// Used when partition sets are given.
	// e.g. update t partition(p0) set a = 1;
	PartitionedTable []table.PartitionedTable `plan-cache-clone:"shallow"`

	// TblID2Table stores related tables' info of this Update statement.
	TblID2Table map[int64]table.Table `plan-cache-clone:"shallow"`

	FKChecks   map[int64][]*FKCheck   `plan-cache-clone:"must-nil"`
	FKCascades map[int64][]*FKCascade `plan-cache-clone:"must-nil"`

	// Returning stores the RETURNING clause expression list.
	Returning       []expression.Expression
	ReturningSchema *expression.Schema `plan-cache-clone:"shallow"`
	ReturningNames  types.NameSlice    `plan-cache-clone:"shallow"`
}

// Init initializes Update.
func (p Update) Init(ctx base.PlanContext) *Update {
	p.Plan = baseimpl.NewBasePlan(ctx, plancodec.TypeUpdate, 0)
	return &p
}

// MemoryUsage return the memory usage of Update
func (p *Update) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.SimpleSchemaProducer.MemoryUsage() + size.SizeOfSlice*3 + int64(cap(p.OrderedList))*size.SizeOfPointer +
		size.SizeOfBool + size.SizeOfInt + size.SizeOfInterface + int64(cap(p.PartitionedTable))*size.SizeOfInterface +
		int64(len(p.TblID2Table))*(size.SizeOfInt64+size.SizeOfInterface)
	if p.SelectPlan != nil {
		sum += p.SelectPlan.MemoryUsage()
	}

	for _, as := range p.OrderedList {
		sum += as.MemoryUsage()
	}
	for _, colInfo := range p.TblColPosInfos {
		sum += colInfo.MemoryUsage()
	}
	for _, v := range p.FKChecks {
		sum += size.SizeOfInt64 + size.SizeOfSlice + int64(cap(v))*size.SizeOfPointer
		for _, fkc := range v {
			sum += fkc.MemoryUsage()
		}
	}
	return
}

// Delete represents a delete plan.
type Delete struct {
	SimpleSchemaProducer

	IsMultiTable bool

	SelectPlan base.PhysicalPlan

	TblColPosInfos TblColPosInfoSlice `plan-cache-clone:"shallow"`

	FKChecks   map[int64][]*FKCheck   `plan-cache-clone:"must-nil"`
	FKCascades map[int64][]*FKCascade `plan-cache-clone:"must-nil"`

	IgnoreErr bool

	// Returning stores the RETURNING clause expression list.
	Returning       []expression.Expression
	ReturningSchema *expression.Schema `plan-cache-clone:"shallow"`
	ReturningNames  types.NameSlice    `plan-cache-clone:"shallow"`
}

// Init initializes Delete.
func (p Delete) Init(ctx base.PlanContext) *Delete {
	p.Plan = baseimpl.NewBasePlan(ctx, plancodec.TypeDelete, 0)
	return &p
}

// MemoryUsage return the memory usage of Delete
func (p *Delete) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.SimpleSchemaProducer.MemoryUsage() + size.SizeOfBool + size.SizeOfInterface + size.SizeOfSlice
	if p.SelectPlan != nil {
		sum += p.SelectPlan.MemoryUsage()
	}
	for _, colInfo := range p.TblColPosInfos {
		sum += colInfo.MemoryUsage()
	}
	return
}

// CleanTblID2HandleMap cleans the tblID2Handle map by removing the handles
func (p *Delete) CleanTblID2HandleMap(
	tablesToDelete map[int64][]*resolve.TableNameW,
	tblID2Handle map[int64][]util.HandleCols,
	outputNames []*types.FieldName,
) map[int64][]util.HandleCols {
	for id, cols := range tblID2Handle {
		names, ok := tablesToDelete[id]
		if !ok {
			delete(tblID2Handle, id)
			continue
		}
		cols = slices.DeleteFunc(cols, func(hCols util.HandleCols) bool {
			for col := range hCols.IterColumns() {
				if p.matchingDeletingTable(names, outputNames[col.Index]) {
					return false
				}
			}
			return true
		})
		if len(cols) == 0 {
			delete(tblID2Handle, id)
			continue
		}
		tblID2Handle[id] = cols
	}
	return tblID2Handle
}

// matchingDeletingTable checks whether this column is from the table which is in the deleting list.
func (*Delete) matchingDeletingTable(names []*resolve.TableNameW, name *types.FieldName) bool {
	for _, n := range names {
		if (name.DBName.L == "" || name.DBName.L == n.DBInfo.Name.L) && name.TblName.L == n.Name.L {
			return true
		}
	}
	return false
}

// TblColPosInfo represents an mapper from column index to handle index.
type TblColPosInfo struct {
	TblID int64
	// Start and End represent the ordinal range [Start, End) of the consecutive columns.
	Start, End int
	// HandleOrdinal represents the ordinal of the handle column.
	HandleCols util.HandleCols

	// IndexesRowLayout store the row layout of indexes. We need it if column pruning happens.
	// If it's nil, means no column pruning happens.
	IndexesRowLayout table.IndexesLayout
}

// MemoryUsage return the memory usage of TblColPosInfo
func (t *TblColPosInfo) MemoryUsage() (sum int64) {
	if t == nil {
		return
	}

	sum = size.SizeOfInt64 + size.SizeOfInt*2
	if t.HandleCols != nil {
		sum += t.HandleCols.MemoryUsage()
	}
	return
}

// Cmp compares two TblColPosInfo by their Start field.
func (t *TblColPosInfo) Cmp(a TblColPosInfo) int {
	return cmp.Compare(t.Start, a.Start)
}

// TblColPosInfoSlice attaches the methods of sort.Interface to []TblColPosInfos sorting in increasing order.
type TblColPosInfoSlice []TblColPosInfo

// Len implements sort.Interface#Len.
func (c TblColPosInfoSlice) Len() int {
	return len(c)
}

// FindTblIdx finds the ordinal of the corresponding access column.
func (c TblColPosInfoSlice) FindTblIdx(colOrdinal int) (int, bool) {
	if len(c) == 0 {
		return 0, false
	}
	// find the smallest index of the range that its start great than colOrdinal.
	// @see https://godoc.org/sort#Search
	rangeBehindOrdinal := sort.Search(len(c), func(i int) bool { return c[i].Start > colOrdinal })
	if rangeBehindOrdinal == 0 {
		return 0, false
	}
	return rangeBehindOrdinal - 1, true
}

// ResolveIndices implements Plan interface.
func (p *Update) ResolveIndices() (err error) {
	err = p.SimpleSchemaProducer.ResolveIndices()
	if err != nil {
		return err
	}
	schema := p.SelectPlan.Schema()
	for _, assign := range p.OrderedList {
		newCol, err := assign.Col.ResolveIndices(schema)
		if err != nil {
			return err
		}
		assign.Col = newCol.(*expression.Column)
		assign.Expr, err = assign.Expr.ResolveIndices(schema)
		if err != nil {
			return err
		}
	}
	return
}

// ResolveIndices implements Plan interface.
func (p *Insert) ResolveIndices() (err error) {
	err = p.SimpleSchemaProducer.ResolveIndices()
	if err != nil {
		return err
	}
	for _, asgn := range p.OnDuplicate {
		newCol, err := asgn.Col.ResolveIndices(p.TableSchema)
		if err != nil {
			return err
		}
		asgn.Col = newCol.(*expression.Column)
		// Once the asgn.lazyErr exists, asgn.Expr here is nil.
		if asgn.Expr != nil {
			asgn.Expr, err = asgn.Expr.ResolveIndices(p.Schema4OnDuplicate)
			if err != nil {
				return err
			}
		}
	}
	for i, expr := range p.GenCols.Exprs {
		p.GenCols.Exprs[i], err = expr.ResolveIndices(p.TableSchema)
		if err != nil {
			return err
		}
	}
	for _, asgn := range p.GenCols.OnDuplicates {
		newCol, err := asgn.Col.ResolveIndices(p.TableSchema)
		if err != nil {
			return err
		}
		asgn.Col = newCol.(*expression.Column)
		asgn.Expr, err = asgn.Expr.ResolveIndices(p.Schema4OnDuplicate)
		if err != nil {
			return err
		}
	}
	return
}

// IsDefaultExprSameColumn - DEFAULT or col = DEFAULT(col)
func IsDefaultExprSameColumn(names types.NameSlice, node ast.ExprNode) bool {
	if expr, ok := node.(*ast.DefaultExpr); ok {
		if expr.Name == nil {
			// col = DEFAULT
			return true
		}
		refIdx, err := expression.FindFieldName(names, expr.Name)
		if refIdx == 0 && err == nil {
			// col = DEFAULT(col)
			return true
		}
	}
	return false
}

// ExtractDefaultExpr extract a `DefaultExpr` from `ExprNode`,
// If it is a `DEFAULT` function like `DEFAULT(a)`, return nil.
// Only if it is `DEFAULT` keyword, it will return the `DefaultExpr`.
func ExtractDefaultExpr(node ast.ExprNode) *ast.DefaultExpr {
	if expr, ok := node.(*ast.DefaultExpr); ok && expr.Name == nil {
		return expr
	}
	return nil
}

// ResolveOnDuplicate resolves the OnDuplicate field of Insert.
func (p *Insert) ResolveOnDuplicate(onDup []*ast.Assignment, tblInfo *model.TableInfo, yield func(ast.ExprNode) (expression.Expression, error)) (map[string]struct{}, error) {
	onDupColSet := make(map[string]struct{}, len(onDup))
	colMap := make(map[string]*table.Column, len(p.Table.Cols()))
	for _, col := range p.Table.Cols() {
		colMap[col.Name.L] = col
	}
	for _, assign := range onDup {
		// Check whether the column to be updated exists in the source table.
		idx, err := expression.FindFieldName(p.TableColNames, assign.Column)
		if err != nil {
			return nil, err
		} else if idx < 0 {
			return nil, plannererrors.ErrUnknownColumn.GenWithStackByArgs(assign.Column.OrigColName(), "field list")
		}

		column := colMap[assign.Column.Name.L]
		if column.Hidden {
			return nil, plannererrors.ErrUnknownColumn.GenWithStackByArgs(column.Name, "field list")
		}
		// Check whether the column to be updated is the generated column.
		// Note: For INSERT, REPLACE, and UPDATE, if a generated column is inserted into, replaced, or updated explicitly, the only permitted value is DEFAULT.
		// see https://dev.mysql.com/doc/refman/8.0/en/create-table-generated-columns.html
		if column.IsGenerated() {
			if IsDefaultExprSameColumn(p.TableColNames[idx:idx+1], assign.Expr) {
				continue
			}
			return nil, plannererrors.ErrBadGeneratedColumn.GenWithStackByArgs(assign.Column.Name.O, tblInfo.Name.O)
		}
		defaultExpr := ExtractDefaultExpr(assign.Expr)
		if defaultExpr != nil {
			defaultExpr.Name = assign.Column
		}

		onDupColSet[column.Name.L] = struct{}{}

		expr, err := yield(assign.Expr)
		if err != nil {
			// Throw other error as soon as possible exceptplannererrors.ErrSubqueryMoreThan1Row which need duplicate in insert in triggered.
			// Refer to https://github.com/pingcap/tidb/issues/29260 for more information.
			if terr, ok := errors.Cause(err).(*terror.Error); !(ok && plannererrors.ErrSubqueryMoreThan1Row.Code() == terr.Code()) {
				return nil, err
			}
		}

		p.OnDuplicate = append(p.OnDuplicate, &expression.Assignment{
			Col:     p.TableSchema.Columns[idx],
			ColName: p.TableColNames[idx].ColName,
			Expr:    expr,
			LazyErr: err,
		})
	}
	return onDupColSet, nil
}
