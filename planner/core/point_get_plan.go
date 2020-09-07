// Copyright 2018 PingCAP, Inc.
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

package core

import (
	"bytes"
	"fmt"
	math2 "math"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/opcode"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/planner/util"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/parser_driver"
	"github.com/pingcap/tidb/util/math"
	"github.com/pingcap/tidb/util/plancodec"
	"github.com/pingcap/tipb/go-tipb"
)

// PointGetPlan is a fast plan for simple point get.
// When we detect that the statement has a unique equal access condition, this plan is used.
// This plan is much faster to build and to execute because it avoid the optimization and coprocessor cost.
type PointGetPlan struct {
	basePlan
	dbName             string
	schema             *expression.Schema
	TblInfo            *model.TableInfo
	IndexInfo          *model.IndexInfo
	PartitionInfo      *model.PartitionDefinition
	Handle             int64
	HandleParam        *driver.ParamMarkerExpr
	IndexValues        []types.Datum
	IndexValueParams   []*driver.ParamMarkerExpr
	expr               expression.Expression
	ctx                sessionctx.Context
	UnsignedHandle     bool
	IsTableDual        bool
	Lock               bool
	outputNames        []*types.FieldName
	LockWaitTime       int64
	partitionColumnPos int
	Columns            []*model.ColumnInfo

	Path *util.AccessPath
}

type nameValuePair struct {
	colName string
	value   types.Datum
	param   *driver.ParamMarkerExpr
}

// Schema implements the Plan interface.
func (p *PointGetPlan) Schema() *expression.Schema {
	return p.schema
}

// attach2Task makes the current physical plan as the father of task's physicalPlan and updates the cost of
// current task. If the child's task is cop task, some operator may close this task and return a new rootTask.
func (p *PointGetPlan) attach2Task(...task) task {
	return nil
}

// ToPB converts physical plan to tipb executor.
func (p *PointGetPlan) ToPB(ctx sessionctx.Context, _ kv.StoreType) (*tipb.Executor, error) {
	return nil, nil
}

// ExplainInfo implements Plan interface.
func (p *PointGetPlan) ExplainInfo() string {
	accessObject, operatorInfo := p.AccessObject(), p.OperatorInfo(false)
	if len(operatorInfo) == 0 {
		return accessObject
	}
	return accessObject + ", " + operatorInfo
}

// ExplainNormalizedInfo implements Plan interface.
func (p *PointGetPlan) ExplainNormalizedInfo() string {
	accessObject, operatorInfo := p.AccessObject(), p.OperatorInfo(true)
	if len(operatorInfo) == 0 {
		return accessObject
	}
	return accessObject + ", " + operatorInfo
}

// AccessObject implements dataAccesser interface.
func (p *PointGetPlan) AccessObject() string {
	buffer := bytes.NewBufferString("")
	tblName := p.TblInfo.Name.O
	fmt.Fprintf(buffer, "table:%s", tblName)
	if p.PartitionInfo != nil {
		fmt.Fprintf(buffer, ", partition:%s", p.PartitionInfo.Name.L)
	}
	if p.IndexInfo != nil {
		buffer.WriteString(", index:" + p.IndexInfo.Name.O + "(")
		for i, idxCol := range p.IndexInfo.Columns {
			buffer.WriteString(idxCol.Name.O)
			if i+1 < len(p.IndexInfo.Columns) {
				buffer.WriteString(", ")
			}
		}
		buffer.WriteString(")")
	}
	return buffer.String()
}

// OperatorInfo implements dataAccesser interface.
func (p *PointGetPlan) OperatorInfo(normalized bool) string {
	buffer := bytes.NewBufferString("")
	if p.IndexInfo == nil {
		if normalized {
			fmt.Fprintf(buffer, "handle:?, ")
		} else {
			if p.UnsignedHandle {
				fmt.Fprintf(buffer, "handle:%d, ", uint64(p.Handle))
			} else {
				fmt.Fprintf(buffer, "handle:%d, ", p.Handle)
			}
		}
	}
	if p.Lock {
		fmt.Fprintf(buffer, "lock, ")
	}
	if buffer.Len() >= 2 {
		buffer.Truncate(buffer.Len() - 2)
	}
	return buffer.String()
}

// GetChildReqProps gets the required property by child index.
func (p *PointGetPlan) GetChildReqProps(idx int) *property.PhysicalProperty {
	return nil
}

// StatsCount will return the the RowCount of property.StatsInfo for this plan.
func (p *PointGetPlan) StatsCount() float64 {
	return 1
}

// statsInfo will return the the RowCount of property.StatsInfo for this plan.
func (p *PointGetPlan) statsInfo() *property.StatsInfo {
	if p.stats == nil {
		p.stats = &property.StatsInfo{}
	}
	p.stats.RowCount = 1
	return p.stats
}

// Children gets all the children.
func (p *PointGetPlan) Children() []PhysicalPlan {
	return nil
}

// SetChildren sets the children for the plan.
func (p *PointGetPlan) SetChildren(...PhysicalPlan) {}

// SetChild sets a specific child for the plan.
func (p *PointGetPlan) SetChild(i int, child PhysicalPlan) {}

// ResolveIndices resolves the indices for columns. After doing this, the columns can evaluate the rows by their indices.
func (p *PointGetPlan) ResolveIndices() error {
	return resolveIndicesForVirtualColumn(p.schema.Columns, p.schema)
}

// OutputNames returns the outputting names of each column.
func (p *PointGetPlan) OutputNames() types.NameSlice {
	return p.outputNames
}

// SetOutputNames sets the outputting name by the given slice.
func (p *PointGetPlan) SetOutputNames(names types.NameSlice) {
	p.outputNames = names
}

// GetCost returns cost of the PointGetPlan.
func (p *PointGetPlan) GetCost(cols []*expression.Column) float64 {
	sessVars := p.ctx.GetSessionVars()
	var rowSize float64
	cost := 0.0
	if p.IndexInfo == nil {
		rowSize = p.stats.HistColl.GetTableAvgRowSize(p.ctx, cols, kv.TiKV, true)
	} else {
		rowSize = p.stats.HistColl.GetIndexAvgRowSize(p.ctx, cols, p.IndexInfo.Unique)
	}
	cost += rowSize * sessVars.NetworkFactor
	cost += sessVars.SeekFactor
	cost /= float64(sessVars.DistSQLScanConcurrency)
	return cost
}

// BatchPointGetPlan represents a physical plan which contains a bunch of
// keys reference the same table and use the same `unique key`
type BatchPointGetPlan struct {
	baseSchemaProducer

	ctx              sessionctx.Context
	dbName           string
	TblInfo          *model.TableInfo
	IndexInfo        *model.IndexInfo
	Handles          []int64
	HandleParams     []*driver.ParamMarkerExpr
	IndexValues      [][]types.Datum
	IndexValueParams [][]*driver.ParamMarkerExpr
	PartitionColPos  int
	KeepOrder        bool
	Desc             bool
	Lock             bool
	LockWaitTime     int64
	Columns          []*model.ColumnInfo

	Path *util.AccessPath
}

// attach2Task makes the current physical plan as the father of task's physicalPlan and updates the cost of
// current task. If the child's task is cop task, some operator may close this task and return a new rootTask.
func (p *BatchPointGetPlan) attach2Task(...task) task {
	return nil
}

// ToPB converts physical plan to tipb executor.
func (p *BatchPointGetPlan) ToPB(ctx sessionctx.Context, _ kv.StoreType) (*tipb.Executor, error) {
	return nil, nil
}

// ExplainInfo implements Plan interface.
func (p *BatchPointGetPlan) ExplainInfo() string {
	return p.AccessObject() + ", " + p.OperatorInfo(false)
}

// ExplainNormalizedInfo implements Plan interface.
func (p *BatchPointGetPlan) ExplainNormalizedInfo() string {
	return p.AccessObject() + ", " + p.OperatorInfo(true)
}

// AccessObject implements physicalScan interface.
func (p *BatchPointGetPlan) AccessObject() string {
	buffer := bytes.NewBufferString("")
	tblName := p.TblInfo.Name.O
	fmt.Fprintf(buffer, "table:%s", tblName)
	if p.IndexInfo != nil {
		buffer.WriteString(", index:" + p.IndexInfo.Name.O + "(")
		for i, idxCol := range p.IndexInfo.Columns {
			buffer.WriteString(idxCol.Name.O)
			if i+1 < len(p.IndexInfo.Columns) {
				buffer.WriteString(", ")
			}
		}
		buffer.WriteString(")")
	}
	return buffer.String()
}

// OperatorInfo implements dataAccesser interface.
func (p *BatchPointGetPlan) OperatorInfo(normalized bool) string {
	buffer := bytes.NewBufferString("")
	if p.IndexInfo == nil {
		if normalized {
			fmt.Fprintf(buffer, "handle:?, ")
		} else {
			fmt.Fprintf(buffer, "handle:%v, ", p.Handles)
		}
	}
	fmt.Fprintf(buffer, "keep order:%v, ", p.KeepOrder)
	fmt.Fprintf(buffer, "desc:%v, ", p.Desc)
	if p.Lock {
		fmt.Fprintf(buffer, "lock, ")
	}
	if buffer.Len() >= 2 {
		buffer.Truncate(buffer.Len() - 2)
	}
	return buffer.String()
}

// GetChildReqProps gets the required property by child index.
func (p *BatchPointGetPlan) GetChildReqProps(idx int) *property.PhysicalProperty {
	return nil
}

// StatsCount will return the the RowCount of property.StatsInfo for this plan.
func (p *BatchPointGetPlan) StatsCount() float64 {
	return p.statsInfo().RowCount
}

// statsInfo will return the the RowCount of property.StatsInfo for this plan.
func (p *BatchPointGetPlan) statsInfo() *property.StatsInfo {
	return p.stats
}

// Children gets all the children.
func (p *BatchPointGetPlan) Children() []PhysicalPlan {
	return nil
}

// SetChildren sets the children for the plan.
func (p *BatchPointGetPlan) SetChildren(...PhysicalPlan) {}

// SetChild sets a specific child for the plan.
func (p *BatchPointGetPlan) SetChild(i int, child PhysicalPlan) {}

// ResolveIndices resolves the indices for columns. After doing this, the columns can evaluate the rows by their indices.
func (p *BatchPointGetPlan) ResolveIndices() error {
	return resolveIndicesForVirtualColumn(p.schema.Columns, p.schema)
}

// OutputNames returns the outputting names of each column.
func (p *BatchPointGetPlan) OutputNames() types.NameSlice {
	return p.names
}

// SetOutputNames sets the outputting name by the given slice.
func (p *BatchPointGetPlan) SetOutputNames(names types.NameSlice) {
	p.names = names
}

// GetCost returns cost of the PointGetPlan.
func (p *BatchPointGetPlan) GetCost(cols []*expression.Column) float64 {
	sessVars := p.ctx.GetSessionVars()
	var rowSize, rowCount float64
	cost := 0.0
	if p.IndexInfo == nil {
		rowCount = float64(len(p.Handles))
		rowSize = p.stats.HistColl.GetTableAvgRowSize(p.ctx, cols, kv.TiKV, true)
	} else {
		rowCount = float64(len(p.IndexValues))
		rowSize = p.stats.HistColl.GetIndexAvgRowSize(p.ctx, cols, p.IndexInfo.Unique)
	}
	cost += rowCount * rowSize * sessVars.NetworkFactor
	cost += rowCount * sessVars.SeekFactor
	cost /= float64(sessVars.DistSQLScanConcurrency)
	return cost
}

// TryFastPlan tries to use the PointGetPlan for the query.
func TryFastPlan(ctx sessionctx.Context, node ast.Node) (p Plan) {
	ctx.GetSessionVars().PlanID = 0
	ctx.GetSessionVars().PlanColumnID = 0
	switch x := node.(type) {
	case *ast.SelectStmt:
		defer func() {
			if ctx.GetSessionVars().SelectLimit != math2.MaxUint64 && p != nil {
				ctx.GetSessionVars().StmtCtx.AppendWarning(errors.New("sql_select_limit is set, so point get plan is not activated"))
				p = nil
			}
		}()
		// Try to convert the `SELECT a, b, c FROM t WHERE (a, b, c) in ((1, 2, 4), (1, 3, 5))` to
		// `PhysicalUnionAll` which children are `PointGet` if exists an unique key (a, b, c) in table `t`
		if fp := tryWhereIn2BatchPointGet(ctx, x); fp != nil {
			if checkFastPlanPrivilege(ctx, fp.dbName, fp.TblInfo.Name.L, mysql.SelectPriv) != nil {
				return
			}
			fp.Lock, fp.LockWaitTime = getLockWaitTime(ctx, x.LockInfo)
			p = fp
			return
		}
		if fp := tryPointGetPlan(ctx, x); fp != nil {
			if checkFastPlanPrivilege(ctx, fp.dbName, fp.TblInfo.Name.L, mysql.SelectPriv) != nil {
				return nil
			}
			if fp.IsTableDual {
				tableDual := PhysicalTableDual{}
				tableDual.names = fp.outputNames
				tableDual.SetSchema(fp.Schema())
				p = tableDual.Init(ctx, &property.StatsInfo{}, 0)
				return
			}
			fp.Lock, fp.LockWaitTime = getLockWaitTime(ctx, x.LockInfo)
			p = fp
			return
		}
	case *ast.UpdateStmt:
		return tryUpdatePointPlan(ctx, x)
	case *ast.DeleteStmt:
		return tryDeletePointPlan(ctx, x)
	}
	return nil
}

// IsSelectForUpdateLockType checks if the select lock type is for update type.
func IsSelectForUpdateLockType(lockType ast.SelectLockType) bool {
	if lockType == ast.SelectLockForUpdate ||
		lockType == ast.SelectLockForUpdateNoWait ||
		lockType == ast.SelectLockForUpdateWaitN {
		return true
	}
	return true
}

func getLockWaitTime(ctx sessionctx.Context, lockInfo *ast.SelectLockInfo) (lock bool, waitTime int64) {
	if lockInfo != nil {
		if IsSelectForUpdateLockType(lockInfo.LockType) {
			// Locking of rows for update using SELECT FOR UPDATE only applies when autocommit
			// is disabled (either by beginning transaction with START TRANSACTION or by setting
			// autocommit to 0. If autocommit is enabled, the rows matching the specification are not locked.
			// See https://dev.mysql.com/doc/refman/5.7/en/innodb-locking-reads.html
			sessVars := ctx.GetSessionVars()
			if !sessVars.IsAutocommit() || sessVars.InTxn() {
				lock = true
				waitTime = sessVars.LockWaitTimeout
				if lockInfo.LockType == ast.SelectLockForUpdateWaitN {
					waitTime = int64(lockInfo.WaitSec * 1000)
				} else if lockInfo.LockType == ast.SelectLockForUpdateNoWait {
					waitTime = kv.LockNoWait
				}
			}
		}
	}
	return
}

func newBatchPointGetPlan(
	ctx sessionctx.Context, patternInExpr *ast.PatternInExpr,
	handleCol *model.ColumnInfo, tbl *model.TableInfo, schema *expression.Schema,
	names []*types.FieldName, whereColNames []string,
) *BatchPointGetPlan {
	statsInfo := &property.StatsInfo{RowCount: float64(len(patternInExpr.List))}
	var partitionColName *ast.ColumnName
	if tbl.GetPartitionInfo() != nil {
		partitionColName = getHashPartitionColumnName(ctx, tbl)
		if partitionColName == nil {
			return nil
		}
	}
	if handleCol != nil {
		var handles = make([]int64, len(patternInExpr.List))
		var handleParams = make([]*driver.ParamMarkerExpr, len(patternInExpr.List))
		for i, item := range patternInExpr.List {
			// SELECT * FROM t WHERE (key) in ((1), (2))
			if p, ok := item.(*ast.ParenthesesExpr); ok {
				item = p.Expr
			}
			var d types.Datum
			var param *driver.ParamMarkerExpr
			switch x := item.(type) {
			case *driver.ValueExpr:
				d = x.Datum
			case *driver.ParamMarkerExpr:
				d = x.Datum
				param = x
			default:
				return nil
			}
			if d.IsNull() {
				return nil
			}
			intDatum, err := d.ConvertTo(ctx.GetSessionVars().StmtCtx, &handleCol.FieldType)
			if err != nil {
				return nil
			}
			// The converted result must be same as original datum
			cmp, err := intDatum.CompareDatum(ctx.GetSessionVars().StmtCtx, &d)
			if err != nil || cmp != 0 {
				return nil
			}
			handles[i] = intDatum.GetInt64()
			handleParams[i] = param
		}
		return BatchPointGetPlan{
			TblInfo:      tbl,
			Handles:      handles,
			HandleParams: handleParams,
		}.Init(ctx, statsInfo, schema, names, 0)
	}

	// The columns in where clause should be covered by unique index
	var matchIdxInfo *model.IndexInfo
	permutations := make([]int, len(whereColNames))
	for _, idxInfo := range tbl.Indices {
		if !idxInfo.Unique || idxInfo.State != model.StatePublic {
			continue
		}
		if len(idxInfo.Columns) != len(whereColNames) || idxInfo.HasPrefixIndex() {
			continue
		}
		// TODO: not sure is there any function to reuse
		matched := true
		for whereColIndex, innerCol := range whereColNames {
			var found bool
			for i, col := range idxInfo.Columns {
				if innerCol == col.Name.L {
					permutations[whereColIndex] = i
					found = true
					break
				}
			}
			if !found {
				matched = false
				break
			}
		}
		if matched {
			matchIdxInfo = idxInfo
			break
		}
	}
	if matchIdxInfo == nil {
		return nil
	}
	indexValues := make([][]types.Datum, len(patternInExpr.List))
	indexValueParams := make([][]*driver.ParamMarkerExpr, len(patternInExpr.List))
	for i, item := range patternInExpr.List {
		// SELECT * FROM t WHERE (key) in ((1), (2))
		if p, ok := item.(*ast.ParenthesesExpr); ok {
			item = p.Expr
		}
		var values []types.Datum
		var valuesParams []*driver.ParamMarkerExpr
		switch x := item.(type) {
		case *ast.RowExpr:
			// The `len(values) == len(valuesParams)` should be satisfied in this mode
			values = make([]types.Datum, len(x.Values))
			valuesParams = make([]*driver.ParamMarkerExpr, len(x.Values))
			for index, inner := range x.Values {
				permIndex := permutations[index]
				switch innerX := inner.(type) {
				case *driver.ValueExpr:
					values[permIndex] = innerX.Datum
				case *driver.ParamMarkerExpr:
					values[permIndex] = innerX.Datum
					valuesParams[permIndex] = innerX
				default:
					return nil
				}
			}
		case *driver.ValueExpr:
			values = []types.Datum{x.Datum}
		case *driver.ParamMarkerExpr:
			values = []types.Datum{x.Datum}
			valuesParams = []*driver.ParamMarkerExpr{x}
		default:
			return nil
		}
		indexValues[i] = values
		indexValueParams[i] = valuesParams
	}
	return BatchPointGetPlan{
		TblInfo:          tbl,
		IndexInfo:        matchIdxInfo,
		IndexValues:      indexValues,
		IndexValueParams: indexValueParams,
		PartitionColPos:  getPartitionColumnPos(matchIdxInfo, partitionColName),
	}.Init(ctx, statsInfo, schema, names, 0)
}

func tryWhereIn2BatchPointGet(ctx sessionctx.Context, selStmt *ast.SelectStmt) *BatchPointGetPlan {
	if selStmt.OrderBy != nil || selStmt.GroupBy != nil ||
		selStmt.Limit != nil || selStmt.Having != nil ||
		len(selStmt.WindowSpecs) > 0 {
		return nil
	}
	in, ok := selStmt.Where.(*ast.PatternInExpr)
	if !ok || in.Not || len(in.List) < 1 {
		return nil
	}

	tblName, tblAlias := getSingleTableNameAndAlias(selStmt.From)
	if tblName == nil {
		return nil
	}
	tbl := tblName.TableInfo
	if tbl == nil {
		return nil
	}
	// Skip the optimization with partition selection.
	if len(tblName.PartitionNames) > 0 {
		return nil
	}

	for _, col := range tbl.Columns {
		if col.IsGenerated() || col.State != model.StatePublic {
			return nil
		}
	}

	schema, names := buildSchemaFromFields(tblName.Schema, tbl, tblAlias, selStmt.Fields.Fields)
	if schema == nil {
		return nil
	}

	var (
		handleCol     *model.ColumnInfo
		whereColNames []string
	)

	// SELECT * FROM t WHERE (key) in ((1), (2))
	colExpr := in.Expr
	if p, ok := colExpr.(*ast.ParenthesesExpr); ok {
		colExpr = p.Expr
	}
	switch colName := colExpr.(type) {
	case *ast.ColumnNameExpr:
		if name := colName.Name.Table.L; name != "" && name != tblAlias.L {
			return nil
		}
		// Try use handle
		if tbl.PKIsHandle {
			for _, col := range tbl.Columns {
				if mysql.HasPriKeyFlag(col.Flag) && col.Name.L == colName.Name.Name.L {
					handleCol = col
					whereColNames = append(whereColNames, col.Name.L)
					break
				}
			}
		}
		if handleCol == nil {
			// Downgrade to use unique index
			whereColNames = append(whereColNames, colName.Name.Name.L)
		}

	case *ast.RowExpr:
		for _, col := range colName.Values {
			c, ok := col.(*ast.ColumnNameExpr)
			if !ok {
				return nil
			}
			if name := c.Name.Table.L; name != "" && name != tblAlias.L {
				return nil
			}
			whereColNames = append(whereColNames, c.Name.Name.L)
		}
	default:
		return nil
	}

	p := newBatchPointGetPlan(ctx, in, handleCol, tbl, schema, names, whereColNames)
	if p == nil {
		return nil
	}
	p.dbName = tblName.Schema.L
	if p.dbName == "" {
		p.dbName = ctx.GetSessionVars().CurrentDB
	}
	return p
}

// tryPointGetPlan determine if the SelectStmt can use a PointGetPlan.
// Returns nil if not applicable.
// To use the PointGetPlan the following rules must be satisfied:
// 1. For the limit clause, the count should at least 1 and the offset is 0.
// 2. It must be a single table select.
// 3. All the columns must be public and generated.
// 4. The condition is an access path that the range is a unique key.
func tryPointGetPlan(ctx sessionctx.Context, selStmt *ast.SelectStmt) *PointGetPlan {
	if selStmt.Having != nil {
		return nil
	} else if selStmt.Limit != nil {
		count, offset, err := extractLimitCountOffset(ctx, selStmt.Limit)
		if err != nil || count == 0 || offset > 0 {
			return nil
		}
	}
	tblName, tblAlias := getSingleTableNameAndAlias(selStmt.From)
	if tblName == nil {
		return nil
	}
	tbl := tblName.TableInfo
	if tbl == nil {
		return nil
	}
	pi := tbl.GetPartitionInfo()
	if pi != nil && pi.Type != model.PartitionTypeHash {
		return nil
	}
	for _, col := range tbl.Columns {
		// Do not handle generated columns.
		if col.IsGenerated() {
			return nil
		}
		// Only handle tables that all columns are public.
		if col.State != model.StatePublic {
			return nil
		}
	}
	schema, names := buildSchemaFromFields(tblName.Schema, tbl, tblAlias, selStmt.Fields.Fields)
	if schema == nil {
		return nil
	}
	dbName := tblName.Schema.L
	if dbName == "" {
		dbName = ctx.GetSessionVars().CurrentDB
	}

	pairs := make([]nameValuePair, 0, 4)
	pairs, isTableDual := getNameValuePairs(ctx.GetSessionVars().StmtCtx, tbl, tblAlias, pairs, selStmt.Where)
	if pairs == nil && !isTableDual {
		return nil
	}

	var partitionInfo *model.PartitionDefinition
	var pos int
	if pi != nil {
		partitionInfo, pos = getPartitionInfo(ctx, tbl, pairs)
		if partitionInfo == nil {
			return nil
		}
		// Take partition selection into consideration.
		if len(tblName.PartitionNames) > 0 {
			if !partitionNameInSet(partitionInfo.Name, tblName.PartitionNames) {
				p := newPointGetPlan(ctx, tblName.Schema.O, schema, tbl, names)
				p.IsTableDual = true
				return p
			}
		}
	}

	handlePair, fieldType := findPKHandle(tbl, pairs)
	if handlePair.value.Kind() != types.KindNull && len(pairs) == 1 {
		if isTableDual {
			p := newPointGetPlan(ctx, tblName.Schema.O, schema, tbl, names)
			p.IsTableDual = true
			return p
		}

		p := newPointGetPlan(ctx, dbName, schema, tbl, names)
		p.Handle = handlePair.value.GetInt64()
		p.UnsignedHandle = mysql.HasUnsignedFlag(fieldType.Flag)
		p.HandleParam = handlePair.param
		p.PartitionInfo = partitionInfo
		return p
	}

	for _, idxInfo := range tbl.Indices {
		if !idxInfo.Unique {
			continue
		}
		if idxInfo.State != model.StatePublic {
			continue
		}
		if isTableDual {
			p := newPointGetPlan(ctx, tblName.Schema.O, schema, tbl, names)
			p.IsTableDual = true
			return p
		}

		idxValues, idxValueParams := getIndexValues(idxInfo, pairs)
		if idxValues == nil {
			continue
		}
		p := newPointGetPlan(ctx, dbName, schema, tbl, names)
		p.IndexInfo = idxInfo
		p.IndexValues = idxValues
		p.IndexValueParams = idxValueParams
		p.PartitionInfo = partitionInfo
		if p.PartitionInfo != nil {
			p.partitionColumnPos = findPartitionIdx(idxInfo, pos, pairs)
		}
		return p
	}
	return nil
}

func partitionNameInSet(name model.CIStr, pnames []model.CIStr) bool {
	for _, pname := range pnames {
		// Case insensitive, create table partition p0, query using P0 is OK.
		if name.L == pname.L {
			return true
		}
	}
	return false
}

func newPointGetPlan(ctx sessionctx.Context, dbName string, schema *expression.Schema, tbl *model.TableInfo, names []*types.FieldName) *PointGetPlan {
	p := &PointGetPlan{
		basePlan:     newBasePlan(ctx, plancodec.TypePointGet, 0),
		dbName:       dbName,
		schema:       schema,
		TblInfo:      tbl,
		outputNames:  names,
		LockWaitTime: ctx.GetSessionVars().LockWaitTimeout,
	}
	ctx.GetSessionVars().StmtCtx.Tables = []stmtctx.TableEntry{{DB: dbName, Table: tbl.Name.L}}
	return p
}

func checkFastPlanPrivilege(ctx sessionctx.Context, dbName, tableName string, checkTypes ...mysql.PrivilegeType) error {
	pm := privilege.GetPrivilegeManager(ctx)
	if pm == nil {
		return nil
	}
	for _, checkType := range checkTypes {
		if !pm.RequestVerification(ctx.GetSessionVars().ActiveRoles, dbName, tableName, "", checkType) {
			return errors.New("privilege check fail")
		}
	}
	return nil
}

func buildSchemaFromFields(
	dbName model.CIStr,
	tbl *model.TableInfo,
	tblName model.CIStr,
	fields []*ast.SelectField,
) (
	*expression.Schema,
	[]*types.FieldName,
) {
	columns := make([]*expression.Column, 0, len(tbl.Columns)+1)
	names := make([]*types.FieldName, 0, len(tbl.Columns)+1)
	if len(fields) > 0 {
		for _, field := range fields {
			if field.WildCard != nil {
				if field.WildCard.Table.L != "" && field.WildCard.Table.L != tblName.L {
					return nil, nil
				}
				for _, col := range tbl.Columns {
					names = append(names, &types.FieldName{
						DBName:      dbName,
						OrigTblName: tbl.Name,
						TblName:     tblName,
						ColName:     col.Name,
					})
					columns = append(columns, colInfoToColumn(col, len(columns)))
				}
				continue
			}
			colNameExpr, ok := field.Expr.(*ast.ColumnNameExpr)
			if !ok {
				return nil, nil
			}
			if colNameExpr.Name.Table.L != "" && colNameExpr.Name.Table.L != tblName.L {
				return nil, nil
			}
			col := findCol(tbl, colNameExpr.Name)
			if col == nil {
				return nil, nil
			}
			asName := col.Name
			if field.AsName.L != "" {
				asName = field.AsName
			}
			names = append(names, &types.FieldName{
				DBName:      dbName,
				OrigTblName: tbl.Name,
				TblName:     tblName,
				ColName:     asName,
			})
			columns = append(columns, colInfoToColumn(col, len(columns)))
		}
		return expression.NewSchema(columns...), names
	}
	// fields len is 0 for update and delete.
	for _, col := range tbl.Columns {
		names = append(names, &types.FieldName{
			DBName:      dbName,
			OrigTblName: tbl.Name,
			TblName:     tblName,
			ColName:     col.Name,
		})
		column := colInfoToColumn(col, len(columns))
		columns = append(columns, column)
	}
	schema := expression.NewSchema(columns...)
	return schema, names
}

// getSingleTableNameAndAlias return the ast node of queried table name and the alias string.
// `tblName` is `nil` if there are multiple tables in the query.
// `tblAlias` will be the real table name if there is no table alias in the query.
func getSingleTableNameAndAlias(tableRefs *ast.TableRefsClause) (tblName *ast.TableName, tblAlias model.CIStr) {
	if tableRefs == nil || tableRefs.TableRefs == nil || tableRefs.TableRefs.Right != nil {
		return nil, tblAlias
	}
	tblSrc, ok := tableRefs.TableRefs.Left.(*ast.TableSource)
	if !ok {
		return nil, tblAlias
	}
	tblName, ok = tblSrc.Source.(*ast.TableName)
	if !ok {
		return nil, tblAlias
	}
	tblAlias = tblSrc.AsName
	if tblSrc.AsName.L == "" {
		tblAlias = tblName.Name
	}
	return tblName, tblAlias
}

// getNameValuePairs extracts `column = constant/paramMarker` conditions from expr as name value pairs.
func getNameValuePairs(stmtCtx *stmtctx.StatementContext, tbl *model.TableInfo, tblName model.CIStr, nvPairs []nameValuePair, expr ast.ExprNode) (
	pairs []nameValuePair, isTableDual bool) {
	binOp, ok := expr.(*ast.BinaryOperationExpr)
	if !ok {
		return nil, false
	}
	if binOp.Op == opcode.LogicAnd {
		nvPairs, isTableDual = getNameValuePairs(stmtCtx, tbl, tblName, nvPairs, binOp.L)
		if nvPairs == nil || isTableDual {
			return nil, isTableDual
		}
		nvPairs, isTableDual = getNameValuePairs(stmtCtx, tbl, tblName, nvPairs, binOp.R)
		if nvPairs == nil || isTableDual {
			return nil, isTableDual
		}
		return nvPairs, isTableDual
	} else if binOp.Op == opcode.EQ {
		var d types.Datum
		var colName *ast.ColumnNameExpr
		var param *driver.ParamMarkerExpr
		var ok bool
		if colName, ok = binOp.L.(*ast.ColumnNameExpr); ok {
			switch x := binOp.R.(type) {
			case *driver.ValueExpr:
				d = x.Datum
			case *driver.ParamMarkerExpr:
				d = x.Datum
				param = x
			}
		} else if colName, ok = binOp.R.(*ast.ColumnNameExpr); ok {
			switch x := binOp.L.(type) {
			case *driver.ValueExpr:
				d = x.Datum
			case *driver.ParamMarkerExpr:
				d = x.Datum
				param = x
			}
		} else {
			return nil, false
		}
		if d.IsNull() {
			return nil, false
		}
		// Views' columns have no FieldType.
		if tbl.IsView() {
			return nil, false
		}
		if colName.Name.Table.L != "" && colName.Name.Table.L != tblName.L {
			return nil, false
		}
		col := model.FindColumnInfo(tbl.Cols(), colName.Name.Name.L)
		if col == nil || // Handling the case when the column is _tidb_rowid.
			(col.Tp == mysql.TypeString && col.Collate == charset.CollationBin) { // This type we needn't to pad `\0` in here.
			return append(nvPairs, nameValuePair{colName: colName.Name.Name.L, value: d, param: param}), false
		}
		dVal, err := d.ConvertTo(stmtCtx, &col.FieldType)
		if err != nil {
			if terror.ErrorEqual(types.ErrOverflow, err) {
				return append(nvPairs, nameValuePair{colName: colName.Name.Name.L, value: d, param: param}), true
			}
			// Some scenarios cast to int with error, but we may use this value in point get.
			if !terror.ErrorEqual(types.ErrTruncatedWrongVal, err) {
				return nil, false
			}
		}
		// The converted result must be same as original datum.
		cmp, err := d.CompareDatum(stmtCtx, &dVal)
		if err != nil {
			return nil, false
		} else if cmp != 0 {
			return append(nvPairs, nameValuePair{colName: colName.Name.Name.L, value: dVal, param: param}), true
		}

		return append(nvPairs, nameValuePair{colName: colName.Name.Name.L, value: dVal, param: param}), false
	}
	return nil, false
}

func findPKHandle(tblInfo *model.TableInfo, pairs []nameValuePair) (handlePair nameValuePair, fieldType *types.FieldType) {
	if !tblInfo.PKIsHandle {
		rowIDIdx := findInPairs("_tidb_rowid", pairs)
		if rowIDIdx != -1 {
			return pairs[rowIDIdx], types.NewFieldType(mysql.TypeLonglong)
		}
		return handlePair, nil
	}
	for _, col := range tblInfo.Columns {
		if mysql.HasPriKeyFlag(col.Flag) {
			i := findInPairs(col.Name.L, pairs)
			if i == -1 {
				return handlePair, nil
			}
			return pairs[i], &col.FieldType
		}
	}
	return handlePair, nil
}

func getIndexValues(idxInfo *model.IndexInfo, pairs []nameValuePair) ([]types.Datum, []*driver.ParamMarkerExpr) {
	idxValues := make([]types.Datum, 0, 4)
	idxValueParams := make([]*driver.ParamMarkerExpr, 0, 4)
	if len(idxInfo.Columns) != len(pairs) {
		return nil, nil
	}
	if idxInfo.HasPrefixIndex() {
		return nil, nil
	}
	for _, idxCol := range idxInfo.Columns {
		i := findInPairs(idxCol.Name.L, pairs)
		if i == -1 {
			return nil, nil
		}
		idxValues = append(idxValues, pairs[i].value)
		idxValueParams = append(idxValueParams, pairs[i].param)
	}
	if len(idxValues) > 0 {
		return idxValues, idxValueParams
	}
	return nil, nil
}

func findInPairs(colName string, pairs []nameValuePair) int {
	for i, pair := range pairs {
		if pair.colName == colName {
			return i
		}
	}
	return -1
}

func tryUpdatePointPlan(ctx sessionctx.Context, updateStmt *ast.UpdateStmt) Plan {
	selStmt := &ast.SelectStmt{
		Fields:  &ast.FieldList{},
		From:    updateStmt.TableRefs,
		Where:   updateStmt.Where,
		OrderBy: updateStmt.Order,
		Limit:   updateStmt.Limit,
	}
	pointGet := tryPointGetPlan(ctx, selStmt)
	if pointGet != nil {
		if pointGet.IsTableDual {
			return PhysicalTableDual{
				names: pointGet.outputNames,
			}.Init(ctx, &property.StatsInfo{}, 0)
		}
		if ctx.GetSessionVars().TxnCtx.IsPessimistic {
			pointGet.Lock, pointGet.LockWaitTime = getLockWaitTime(ctx, &ast.SelectLockInfo{LockType: ast.SelectLockForUpdate})
		}
		return buildPointUpdatePlan(ctx, pointGet, pointGet.dbName, pointGet.TblInfo, updateStmt)
	}
	batchPointGet := tryWhereIn2BatchPointGet(ctx, selStmt)
	if batchPointGet != nil {
		if ctx.GetSessionVars().TxnCtx.IsPessimistic {
			batchPointGet.Lock, batchPointGet.LockWaitTime = getLockWaitTime(ctx, &ast.SelectLockInfo{LockType: ast.SelectLockForUpdate})
		}
		return buildPointUpdatePlan(ctx, batchPointGet, batchPointGet.dbName, batchPointGet.TblInfo, updateStmt)
	}
	return nil
}

func buildPointUpdatePlan(ctx sessionctx.Context, pointPlan PhysicalPlan, dbName string, tbl *model.TableInfo, updateStmt *ast.UpdateStmt) Plan {
	if checkFastPlanPrivilege(ctx, dbName, tbl.Name.L, mysql.SelectPriv, mysql.UpdatePriv) != nil {
		return nil
	}
	orderedList, allAssignmentsAreConstant := buildOrderedList(ctx, pointPlan, updateStmt.List)
	if orderedList == nil {
		return nil
	}
	handleCol := findHandleCol(tbl, pointPlan.Schema())
	updatePlan := Update{
		SelectPlan:  pointPlan,
		OrderedList: orderedList,
		TblColPosInfos: TblColPosInfoSlice{
			TblColPosInfo{
				TblID:         tbl.ID,
				Start:         0,
				End:           pointPlan.Schema().Len(),
				HandleOrdinal: handleCol.Index,
			},
		},
		AllAssignmentsAreConstant: allAssignmentsAreConstant,
	}.Init(ctx)
	updatePlan.names = pointPlan.OutputNames()
	return updatePlan
}

func buildOrderedList(ctx sessionctx.Context, plan Plan, list []*ast.Assignment,
) (orderedList []*expression.Assignment, allAssignmentsAreConstant bool) {
	orderedList = make([]*expression.Assignment, 0, len(list))
	allAssignmentsAreConstant = true
	for _, assign := range list {
		idx, err := expression.FindFieldName(plan.OutputNames(), assign.Column)
		if idx == -1 || err != nil {
			return nil, true
		}
		col := plan.Schema().Columns[idx]
		newAssign := &expression.Assignment{
			Col:     col,
			ColName: plan.OutputNames()[idx].ColName,
		}
		expr, err := expression.RewriteSimpleExprWithNames(ctx, assign.Expr, plan.Schema(), plan.OutputNames())
		if err != nil {
			return nil, true
		}
		expr = expression.BuildCastFunction(ctx, expr, col.GetType())
		if allAssignmentsAreConstant {
			_, isConst := expr.(*expression.Constant)
			allAssignmentsAreConstant = isConst
		}

		newAssign.Expr, err = expr.ResolveIndices(plan.Schema())
		if err != nil {
			return nil, true
		}
		orderedList = append(orderedList, newAssign)
	}
	return orderedList, allAssignmentsAreConstant
}

func tryDeletePointPlan(ctx sessionctx.Context, delStmt *ast.DeleteStmt) Plan {
	if delStmt.IsMultiTable {
		return nil
	}
	selStmt := &ast.SelectStmt{
		Fields:  &ast.FieldList{},
		From:    delStmt.TableRefs,
		Where:   delStmt.Where,
		OrderBy: delStmt.Order,
		Limit:   delStmt.Limit,
	}
	if pointGet := tryPointGetPlan(ctx, selStmt); pointGet != nil {
		if pointGet.IsTableDual {
			return PhysicalTableDual{
				names: pointGet.outputNames,
			}.Init(ctx, &property.StatsInfo{}, 0)
		}
		if ctx.GetSessionVars().TxnCtx.IsPessimistic {
			pointGet.Lock, pointGet.LockWaitTime = getLockWaitTime(ctx, &ast.SelectLockInfo{LockType: ast.SelectLockForUpdate})
		}
		return buildPointDeletePlan(ctx, pointGet, pointGet.dbName, pointGet.TblInfo)
	}
	if batchPointGet := tryWhereIn2BatchPointGet(ctx, selStmt); batchPointGet != nil {
		if ctx.GetSessionVars().TxnCtx.IsPessimistic {
			batchPointGet.Lock, batchPointGet.LockWaitTime = getLockWaitTime(ctx, &ast.SelectLockInfo{LockType: ast.SelectLockForUpdate})
		}
		return buildPointDeletePlan(ctx, batchPointGet, batchPointGet.dbName, batchPointGet.TblInfo)
	}
	return nil
}

func buildPointDeletePlan(ctx sessionctx.Context, pointPlan PhysicalPlan, dbName string, tbl *model.TableInfo) Plan {
	if checkFastPlanPrivilege(ctx, dbName, tbl.Name.L, mysql.SelectPriv, mysql.DeletePriv) != nil {
		return nil
	}
	handleCol := findHandleCol(tbl, pointPlan.Schema())
	delPlan := Delete{
		SelectPlan: pointPlan,
		TblColPosInfos: TblColPosInfoSlice{
			TblColPosInfo{
				TblID:         tbl.ID,
				Start:         0,
				End:           pointPlan.Schema().Len(),
				HandleOrdinal: handleCol.Index,
			},
		},
	}.Init(ctx)
	return delPlan
}

func findCol(tbl *model.TableInfo, colName *ast.ColumnName) *model.ColumnInfo {
	for _, col := range tbl.Columns {
		if col.Name.L == colName.Name.L {
			return col
		}
	}
	return nil
}

func colInfoToColumn(col *model.ColumnInfo, idx int) *expression.Column {
	return &expression.Column{
		RetType:  &col.FieldType,
		ID:       col.ID,
		UniqueID: int64(col.Offset),
		Index:    idx,
		OrigName: col.Name.L,
	}
}

func findHandleCol(tbl *model.TableInfo, schema *expression.Schema) *expression.Column {
	// fields len is 0 for update and delete.
	var handleCol *expression.Column
	if tbl.PKIsHandle {
		for i, col := range tbl.Columns {
			if mysql.HasPriKeyFlag(col.Flag) && tbl.PKIsHandle {
				handleCol = schema.Columns[i]
			}
		}
	}
	if handleCol == nil {
		handleCol = colInfoToColumn(model.NewExtraHandleColInfo(), schema.Len())
		schema.Append(handleCol)
	}
	return handleCol
}

func getPartitionInfo(ctx sessionctx.Context, tbl *model.TableInfo, pairs []nameValuePair) (*model.PartitionDefinition, int) {
	partitionColName := getHashPartitionColumnName(ctx, tbl)
	if partitionColName == nil {
		return nil, 0
	}
	pi := tbl.Partition
	for i, pair := range pairs {
		if partitionColName.Name.L == pair.colName {
			val := pair.value.GetInt64()
			pos := math.Abs(val % int64(pi.Num))
			return &pi.Definitions[pos], i
		}
	}
	return nil, 0
}

func findPartitionIdx(idxInfo *model.IndexInfo, pos int, pairs []nameValuePair) int {
	for i, idxCol := range idxInfo.Columns {
		if idxCol.Name.L == pairs[pos].colName {
			return i
		}
	}
	return 0
}

// getPartitionColumnPos gets the partition column's position in the index.
func getPartitionColumnPos(idx *model.IndexInfo, partitionColName *ast.ColumnName) int {
	if partitionColName == nil {
		return 0
	}
	for i, idxCol := range idx.Columns {
		if partitionColName.Name.L == idxCol.Name.L {
			return i
		}
	}
	panic("unique index must include all partition columns")
}

func getHashPartitionColumnName(ctx sessionctx.Context, tbl *model.TableInfo) *ast.ColumnName {
	pi := tbl.GetPartitionInfo()
	if pi == nil {
		return nil
	}
	if pi.Type != model.PartitionTypeHash {
		return nil
	}
	is := infoschema.GetInfoSchema(ctx)
	table, ok := is.TableByID(tbl.ID)
	if !ok {
		return nil
	}
	// PartitionExpr don't need columns and names for hash partition.
	partitionExpr, err := table.(partitionTable).PartitionExpr()
	if err != nil {
		return nil
	}
	expr := partitionExpr.OrigExpr
	col, ok := expr.(*ast.ColumnNameExpr)
	if !ok {
		return nil
	}
	return col.Name
}
