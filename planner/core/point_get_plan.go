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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	math2 "math"
	"sort"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/parser/terror"
	ptypes "github.com/pingcap/tidb/parser/types"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/types"
	driver "github.com/pingcap/tidb/types/parser_driver"
	tidbutil "github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/pingcap/tidb/util/plancodec"
	"github.com/pingcap/tidb/util/stringutil"
	"github.com/pingcap/tidb/util/tracing"
	"github.com/pingcap/tipb/go-tipb"
	tikvstore "github.com/tikv/client-go/v2/kv"
	"go.uber.org/zap"
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
	Handle             kv.Handle
	HandleConstant     *expression.Constant
	handleFieldType    *types.FieldType
	IndexValues        []types.Datum
	IndexConstants     []*expression.Constant
	ColsFieldType      []*types.FieldType
	IdxCols            []*expression.Column
	IdxColLens         []int
	AccessConditions   []expression.Expression
	ctx                sessionctx.Context
	UnsignedHandle     bool
	IsTableDual        bool
	Lock               bool
	outputNames        []*types.FieldName
	LockWaitTime       int64
	partitionColumnPos int
	Columns            []*model.ColumnInfo
	cost               float64

	// required by cost model
	planCostInit bool
	planCost     float64
	// accessCols represents actual columns the PointGet will access, which are used to calculate row-size
	accessCols []*expression.Column
}

type nameValuePair struct {
	colName      string
	colFieldType *types.FieldType
	value        types.Datum
	con          *expression.Constant
}

// Schema implements the Plan interface.
func (p *PointGetPlan) Schema() *expression.Schema {
	return p.schema
}

// Cost implements PhysicalPlan interface
func (p *PointGetPlan) Cost() float64 {
	return p.cost
}

// SetCost implements PhysicalPlan interface
func (p *PointGetPlan) SetCost(cost float64) {
	p.cost = cost
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

// Clone implements PhysicalPlan interface.
func (p *PointGetPlan) Clone() (PhysicalPlan, error) {
	return nil, errors.Errorf("%T doesn't support cloning", p)
}

// ExplainInfo implements Plan interface.
func (p *PointGetPlan) ExplainInfo() string {
	accessObject, operatorInfo := p.AccessObject(false), p.OperatorInfo(false)
	if len(operatorInfo) == 0 {
		return accessObject
	}
	return accessObject + ", " + operatorInfo
}

// ExplainNormalizedInfo implements Plan interface.
func (p *PointGetPlan) ExplainNormalizedInfo() string {
	accessObject, operatorInfo := p.AccessObject(true), p.OperatorInfo(true)
	if len(operatorInfo) == 0 {
		return accessObject
	}
	return accessObject + ", " + operatorInfo
}

// AccessObject implements dataAccesser interface.
func (p *PointGetPlan) AccessObject(normalized bool) string {
	var buffer strings.Builder
	tblName := p.TblInfo.Name.O
	buffer.WriteString("table:")
	buffer.WriteString(tblName)
	if p.PartitionInfo != nil {
		if normalized {
			buffer.WriteString(", partition:?")
		} else {
			buffer.WriteString(", partition:")
			buffer.WriteString(p.PartitionInfo.Name.O)
		}
	}
	if p.IndexInfo != nil {
		if p.IndexInfo.Primary && p.TblInfo.IsCommonHandle {
			buffer.WriteString(", clustered index:")
			buffer.WriteString(p.IndexInfo.Name.O)
			buffer.WriteString("(")
		} else {
			buffer.WriteString(", index:")
			buffer.WriteString(p.IndexInfo.Name.O)
			buffer.WriteString("(")
		}
		for i, idxCol := range p.IndexInfo.Columns {
			if tblCol := p.TblInfo.Columns[idxCol.Offset]; tblCol.Hidden {
				buffer.WriteString(tblCol.GeneratedExprString)
			} else {
				buffer.WriteString(idxCol.Name.O)
			}
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
	if p.Handle == nil && !p.Lock {
		return ""
	}
	var buffer strings.Builder
	if p.Handle != nil {
		if normalized {
			buffer.WriteString("handle:?")
		} else {
			buffer.WriteString("handle:")
			if p.UnsignedHandle {
				buffer.WriteString(strconv.FormatUint(uint64(p.Handle.IntValue()), 10))
			} else {
				buffer.WriteString(p.Handle.String())
			}
		}
	}
	if p.Lock {
		if p.Handle != nil {
			buffer.WriteString(", lock")
		} else {
			buffer.WriteString("lock")
		}
	}
	return buffer.String()
}

// ExtractCorrelatedCols implements PhysicalPlan interface.
func (p *PointGetPlan) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	return nil
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

// BatchPointGetPlan represents a physical plan which contains a bunch of
// keys reference the same table and use the same `unique key`
type BatchPointGetPlan struct {
	baseSchemaProducer

	ctx              sessionctx.Context
	dbName           string
	TblInfo          *model.TableInfo
	IndexInfo        *model.IndexInfo
	Handles          []kv.Handle
	HandleType       *types.FieldType
	HandleParams     []*expression.Constant // record all Parameters for Plan-Cache
	IndexValues      [][]types.Datum
	IndexValueParams [][]*expression.Constant // record all Parameters for Plan-Cache
	IndexColTypes    []*types.FieldType
	AccessConditions []expression.Expression
	IdxCols          []*expression.Column
	IdxColLens       []int
	PartitionColPos  int
	PartitionExpr    *tables.PartitionExpr
	KeepOrder        bool
	Desc             bool
	Lock             bool
	LockWaitTime     int64
	Columns          []*model.ColumnInfo
	cost             float64

	// SinglePart indicates whether this BatchPointGetPlan is just for a single partition, instead of the whole partition table.
	// If the BatchPointGetPlan is built in fast path, this value if false; if the plan is generated in physical optimization for a partition,
	// this value would be true. This value would decide the behavior of BatchPointGetExec, i.e, whether to compute the table ID of the partition
	// on the fly.
	SinglePart bool
	// PartTblID is the table ID for the specific table partition.
	PartTblID int64

	// required by cost model
	planCostInit bool
	planCost     float64
	// accessCols represents actual columns the PointGet will access, which are used to calculate row-size
	accessCols []*expression.Column
}

// Cost implements PhysicalPlan interface
func (p *BatchPointGetPlan) Cost() float64 {
	return p.cost
}

// SetCost implements PhysicalPlan interface
func (p *BatchPointGetPlan) SetCost(cost float64) {
	p.cost = cost
}

// Clone implements PhysicalPlan interface.
func (p *BatchPointGetPlan) Clone() (PhysicalPlan, error) {
	return nil, errors.Errorf("%T doesn't support cloning", p)
}

// ExtractCorrelatedCols implements PhysicalPlan interface.
func (p *BatchPointGetPlan) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	return nil
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
	return p.AccessObject(false) + ", " + p.OperatorInfo(false)
}

// ExplainNormalizedInfo implements Plan interface.
func (p *BatchPointGetPlan) ExplainNormalizedInfo() string {
	return p.AccessObject(true) + ", " + p.OperatorInfo(true)
}

// AccessObject implements physicalScan interface.
func (p *BatchPointGetPlan) AccessObject(_ bool) string {
	var buffer strings.Builder
	tblName := p.TblInfo.Name.O
	buffer.WriteString("table:")
	buffer.WriteString(tblName)
	if p.IndexInfo != nil {
		if p.IndexInfo.Primary && p.TblInfo.IsCommonHandle {
			buffer.WriteString(", clustered index:" + p.IndexInfo.Name.O + "(")
		} else {
			buffer.WriteString(", index:" + p.IndexInfo.Name.O + "(")
		}
		for i, idxCol := range p.IndexInfo.Columns {
			if tblCol := p.TblInfo.Columns[idxCol.Offset]; tblCol.Hidden {
				buffer.WriteString(tblCol.GeneratedExprString)
			} else {
				buffer.WriteString(idxCol.Name.O)
			}
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
	var buffer strings.Builder
	if p.IndexInfo == nil {
		if normalized {
			buffer.WriteString("handle:?, ")
		} else {
			buffer.WriteString("handle:[")
			for i, handle := range p.Handles {
				if i != 0 {
					buffer.WriteString(" ")
				}
				buffer.WriteString(handle.String())
			}
			buffer.WriteString("], ")
		}
	}
	buffer.WriteString("keep order:")
	buffer.WriteString(strconv.FormatBool(p.KeepOrder))
	buffer.WriteString(", desc:")
	buffer.WriteString(strconv.FormatBool(p.Desc))
	if p.Lock {
		buffer.WriteString(", lock")
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

// PointPlanKey is used to get point plan that is pre-built for multi-statement query.
const PointPlanKey = stringutil.StringerStr("pointPlanKey")

// PointPlanVal is used to store point plan that is pre-built for multi-statement query.
// Save the plan in a struct so even if the point plan is nil, we don't need to try again.
type PointPlanVal struct {
	Plan Plan
}

// TryFastPlan tries to use the PointGetPlan for the query.
func TryFastPlan(ctx sessionctx.Context, node ast.Node) (p Plan) {
	if checkStableResultMode(ctx) {
		// the rule of stabilizing results has not taken effect yet, so cannot generate a plan here in this mode
		return nil
	}

	ctx.GetSessionVars().PlanID = 0
	ctx.GetSessionVars().PlanColumnID = 0
	switch x := node.(type) {
	case *ast.SelectStmt:
		defer func() {
			vars := ctx.GetSessionVars()
			if vars.SelectLimit != math2.MaxUint64 && p != nil {
				ctx.GetSessionVars().StmtCtx.AppendWarning(errors.New("sql_select_limit is set, so point get plan is not activated"))
				p = nil
			}
			if vars.StmtCtx.EnableOptimizeTrace && p != nil {
				if vars.StmtCtx.OptimizeTracer == nil {
					vars.StmtCtx.OptimizeTracer = &tracing.OptimizeTracer{}
				}
				vars.StmtCtx.OptimizeTracer.SetFastPlan(p.buildPlanTrace())
			}
		}()
		// Try to convert the `SELECT a, b, c FROM t WHERE (a, b, c) in ((1, 2, 4), (1, 3, 5))` to
		// `PhysicalUnionAll` which children are `PointGet` if exists an unique key (a, b, c) in table `t`
		if fp := tryWhereIn2BatchPointGet(ctx, x); fp != nil {
			if checkFastPlanPrivilege(ctx, fp.dbName, fp.TblInfo.Name.L, mysql.SelectPriv) != nil {
				return
			}
			if tidbutil.IsMemDB(fp.dbName) {
				return nil
			}
			fp.Lock, fp.LockWaitTime = getLockWaitTime(ctx, x.LockInfo)
			p = fp
			return
		}
		if fp := tryPointGetPlan(ctx, x, isForUpdateReadSelectLock(x.LockInfo)); fp != nil {
			if checkFastPlanPrivilege(ctx, fp.dbName, fp.TblInfo.Name.L, mysql.SelectPriv) != nil {
				return nil
			}
			if tidbutil.IsMemDB(fp.dbName) {
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
		lockType == ast.SelectLockForShare ||
		lockType == ast.SelectLockForUpdateNoWait ||
		lockType == ast.SelectLockForUpdateWaitN {
		return true
	}
	return false
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
					waitTime = tikvstore.LockNoWait
				}
			}
		}
	}
	return
}

func newBatchPointGetPlan(
	ctx sessionctx.Context, patternInExpr *ast.PatternInExpr,
	handleCol *model.ColumnInfo, tbl *model.TableInfo, schema *expression.Schema,
	names []*types.FieldName, whereColNames []string, indexHints []*ast.IndexHint,
) *BatchPointGetPlan {
	stmtCtx := ctx.GetSessionVars().StmtCtx
	statsInfo := &property.StatsInfo{RowCount: float64(len(patternInExpr.List))}
	var partitionExpr *tables.PartitionExpr
	if tbl.GetPartitionInfo() != nil {
		partitionExpr = getPartitionExpr(ctx, tbl)
		if partitionExpr == nil {
			return nil
		}

		if partitionExpr.Expr == nil {
			return nil
		}
		if _, ok := partitionExpr.Expr.(*expression.Column); !ok {
			return nil
		}
	}
	if handleCol != nil {
		var handles = make([]kv.Handle, len(patternInExpr.List))
		var handleParams = make([]*expression.Constant, len(patternInExpr.List))
		for i, item := range patternInExpr.List {
			// SELECT * FROM t WHERE (key) in ((1), (2))
			if p, ok := item.(*ast.ParenthesesExpr); ok {
				item = p.Expr
			}
			var d types.Datum
			var con *expression.Constant
			switch x := item.(type) {
			case *driver.ValueExpr:
				d = x.Datum
			case *driver.ParamMarkerExpr:
				var err error
				con, err = expression.ParamMarkerExpression(ctx, x, true)
				if err != nil {
					return nil
				}
				d, err = con.Eval(chunk.Row{})
				if err != nil {
					return nil
				}
			default:
				return nil
			}
			if d.IsNull() {
				return nil
			}
			intDatum := getPointGetValue(stmtCtx, handleCol, &d)
			if intDatum == nil {
				return nil
			}
			handles[i] = kv.IntHandle(intDatum.GetInt64())
			handleParams[i] = con
		}
		return BatchPointGetPlan{
			TblInfo:       tbl,
			Handles:       handles,
			HandleParams:  handleParams,
			HandleType:    &handleCol.FieldType,
			PartitionExpr: partitionExpr,
		}.Init(ctx, statsInfo, schema, names, 0)
	}

	// The columns in where clause should be covered by unique index
	var matchIdxInfo *model.IndexInfo
	permutations := make([]int, len(whereColNames))
	colInfos := make([]*model.ColumnInfo, len(whereColNames))
	for i, innerCol := range whereColNames {
		for _, col := range tbl.Columns {
			if col.Name.L == innerCol {
				colInfos[i] = col
			}
		}
	}
	for _, idxInfo := range tbl.Indices {
		if !idxInfo.Unique || idxInfo.State != model.StatePublic || idxInfo.Invisible ||
			!indexIsAvailableByHints(idxInfo, indexHints) {
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

	pos, err := getPartitionColumnPos(matchIdxInfo, partitionExpr, tbl)
	if err != nil {
		return nil
	}

	indexValues := make([][]types.Datum, len(patternInExpr.List))
	indexValueParams := make([][]*expression.Constant, len(patternInExpr.List))
	var indexTypes []*types.FieldType
	for i, item := range patternInExpr.List {
		// SELECT * FROM t WHERE (key) in ((1), (2))
		if p, ok := item.(*ast.ParenthesesExpr); ok {
			item = p.Expr
		}
		var values []types.Datum
		var valuesParams []*expression.Constant
		switch x := item.(type) {
		case *ast.RowExpr:
			// The `len(values) == len(valuesParams)` should be satisfied in this mode
			if len(x.Values) != len(whereColNames) {
				return nil
			}
			values = make([]types.Datum, len(x.Values))
			valuesParams = make([]*expression.Constant, len(x.Values))
			initTypes := false
			if indexTypes == nil { // only init once
				indexTypes = make([]*types.FieldType, len(x.Values))
				initTypes = true
			}
			for index, inner := range x.Values {
				permIndex := permutations[index]
				switch innerX := inner.(type) {
				case *driver.ValueExpr:
					dval := getPointGetValue(stmtCtx, colInfos[index], &innerX.Datum)
					if dval == nil {
						return nil
					}
					values[permIndex] = innerX.Datum
				case *driver.ParamMarkerExpr:
					con, err := expression.ParamMarkerExpression(ctx, innerX, true)
					if err != nil {
						return nil
					}
					d, err := con.Eval(chunk.Row{})
					if err != nil {
						return nil
					}
					dval := getPointGetValue(stmtCtx, colInfos[index], &d)
					if dval == nil {
						return nil
					}
					values[permIndex] = innerX.Datum
					valuesParams[permIndex] = con
					if initTypes {
						indexTypes[permIndex] = &colInfos[index].FieldType
					}
				default:
					return nil
				}
			}
		case *driver.ValueExpr:
			// if any item is `ValueExpr` type, `Expr` should contain only one column,
			// otherwise column count doesn't match and no plan can be built.
			if len(whereColNames) != 1 {
				return nil
			}
			dval := getPointGetValue(stmtCtx, colInfos[0], &x.Datum)
			if dval == nil {
				return nil
			}
			values = []types.Datum{*dval}
		case *driver.ParamMarkerExpr:
			if len(whereColNames) != 1 {
				return nil
			}
			con, err := expression.ParamMarkerExpression(ctx, x, true)
			if err != nil {
				return nil
			}
			d, err := con.Eval(chunk.Row{})
			if err != nil {
				return nil
			}
			dval := getPointGetValue(stmtCtx, colInfos[0], &d)
			if dval == nil {
				return nil
			}
			values = []types.Datum{*dval}
			valuesParams = []*expression.Constant{con}
			if indexTypes == nil { // only init once
				indexTypes = []*types.FieldType{&colInfos[0].FieldType}
			}
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
		IndexColTypes:    indexTypes,
		PartitionColPos:  pos,
		PartitionExpr:    partitionExpr,
	}.Init(ctx, statsInfo, schema, names, 0)
}

func tryWhereIn2BatchPointGet(ctx sessionctx.Context, selStmt *ast.SelectStmt) *BatchPointGetPlan {
	if selStmt.OrderBy != nil || selStmt.GroupBy != nil ||
		selStmt.Limit != nil || selStmt.Having != nil || selStmt.Distinct ||
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
				if mysql.HasPriKeyFlag(col.GetFlag()) && col.Name.L == colName.Name.Name.L {
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

	p := newBatchPointGetPlan(ctx, in, handleCol, tbl, schema, names, whereColNames, tblName.IndexHints)
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
// 3. All the columns must be public and not generated.
// 4. The condition is an access path that the range is a unique key.
func tryPointGetPlan(ctx sessionctx.Context, selStmt *ast.SelectStmt, check bool) *PointGetPlan {
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
	pairs, isTableDual := getNameValuePairs(ctx, tbl, tblAlias, pairs, selStmt.Where)
	if pairs == nil && !isTableDual {
		return nil
	}

	var partitionInfo *model.PartitionDefinition
	var pos int
	if pi != nil {
		partitionInfo, pos, isTableDual = getPartitionInfo(ctx, tbl, pairs)
		if isTableDual {
			p := newPointGetPlan(ctx, tblName.Schema.O, schema, tbl, names)
			p.IsTableDual = true
			return p
		}
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
	if handlePair.value.Kind() != types.KindNull && len(pairs) == 1 && indexIsAvailableByHints(nil, tblName.IndexHints) {
		if isTableDual {
			p := newPointGetPlan(ctx, tblName.Schema.O, schema, tbl, names)
			p.IsTableDual = true
			return p
		}

		p := newPointGetPlan(ctx, dbName, schema, tbl, names)
		p.Handle = kv.IntHandle(handlePair.value.GetInt64())
		p.UnsignedHandle = mysql.HasUnsignedFlag(fieldType.GetFlag())
		p.handleFieldType = fieldType
		p.HandleConstant = handlePair.con
		p.PartitionInfo = partitionInfo
		return p
	} else if handlePair.value.Kind() != types.KindNull {
		return nil
	}

	check = check || ctx.GetSessionVars().IsIsolation(ast.ReadCommitted)
	check = check && ctx.GetSessionVars().ConnectionID > 0
	var latestIndexes map[int64]*model.IndexInfo
	var err error

	for _, idxInfo := range tbl.Indices {
		if !idxInfo.Unique || idxInfo.State != model.StatePublic || idxInfo.Invisible ||
			!indexIsAvailableByHints(idxInfo, tblName.IndexHints) {
			continue
		}
		if isTableDual {
			if check && latestIndexes == nil {
				latestIndexes, check, err = getLatestIndexInfo(ctx, tbl.ID, 0)
				if err != nil {
					logutil.BgLogger().Warn("get information schema failed", zap.Error(err))
					return nil
				}
			}
			if check {
				if latestIndex, ok := latestIndexes[idxInfo.ID]; !ok || latestIndex.State != model.StatePublic {
					continue
				}
			}
			p := newPointGetPlan(ctx, tblName.Schema.O, schema, tbl, names)
			p.IsTableDual = true
			return p
		}
		idxValues, idxConstant, colsFieldType := getIndexValues(idxInfo, pairs)
		if idxValues == nil {
			continue
		}
		if check && latestIndexes == nil {
			latestIndexes, check, err = getLatestIndexInfo(ctx, tbl.ID, 0)
			if err != nil {
				logutil.BgLogger().Warn("get information schema failed", zap.Error(err))
				return nil
			}
		}
		if check {
			if latestIndex, ok := latestIndexes[idxInfo.ID]; !ok || latestIndex.State != model.StatePublic {
				continue
			}
		}
		p := newPointGetPlan(ctx, dbName, schema, tbl, names)
		p.IndexInfo = idxInfo
		p.IndexValues = idxValues
		p.IndexConstants = idxConstant
		p.ColsFieldType = colsFieldType
		p.PartitionInfo = partitionInfo
		if p.PartitionInfo != nil {
			p.partitionColumnPos = findPartitionIdx(idxInfo, pos, pairs)
		}
		return p
	}
	return nil
}

// indexIsAvailableByHints checks whether this index is filtered by these specified index hints.
// idxInfo is PK if it's nil
func indexIsAvailableByHints(idxInfo *model.IndexInfo, idxHints []*ast.IndexHint) bool {
	if len(idxHints) == 0 {
		return true
	}
	match := func(name model.CIStr) bool {
		if idxInfo == nil {
			return name.L == "primary"
		}
		return idxInfo.Name.L == name.L
	}
	// NOTICE: it's supposed that ignore hints and use/force hints will not be applied together since the effect of
	// the former will be eliminated by the latter.
	isIgnore := false
	for _, hint := range idxHints {
		if hint.HintScope != ast.HintForScan {
			continue
		}
		if hint.HintType == ast.HintIgnore && hint.IndexNames != nil {
			isIgnore = true
			for _, name := range hint.IndexNames {
				if match(name) {
					return false
				}
			}
		}
		if (hint.HintType == ast.HintForce || hint.HintType == ast.HintUse) && hint.IndexNames != nil {
			for _, name := range hint.IndexNames {
				if match(name) {
					return true
				}
			}
		}
	}
	return isIgnore
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
	visitInfos := make([]visitInfo, 0, len(checkTypes))
	for _, checkType := range checkTypes {
		if pm != nil && !pm.RequestVerification(ctx.GetSessionVars().ActiveRoles, dbName, tableName, "", checkType) {
			return ErrPrivilegeCheckFail.GenWithStackByArgs(checkType.String())
		}
		// This visitInfo is only for table lock check, so we do not need column field,
		// just fill it empty string.
		visitInfos = append(visitInfos, visitInfo{
			privilege: checkType,
			db:        dbName,
			table:     tableName,
			column:    "",
			err:       nil,
		})
	}

	infoSchema := ctx.GetInfoSchema().(infoschema.InfoSchema)
	return CheckTableLock(ctx, infoSchema, visitInfos)
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
			asName := colNameExpr.Name.Name
			if field.AsName.L != "" {
				asName = field.AsName
			}
			names = append(names, &types.FieldName{
				DBName:      dbName,
				OrigTblName: tbl.Name,
				TblName:     tblName,
				OrigColName: col.Name,
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
func getNameValuePairs(ctx sessionctx.Context, tbl *model.TableInfo, tblName model.CIStr, nvPairs []nameValuePair, expr ast.ExprNode) (
	pairs []nameValuePair, isTableDual bool) {
	stmtCtx := ctx.GetSessionVars().StmtCtx
	binOp, ok := expr.(*ast.BinaryOperationExpr)
	if !ok {
		return nil, false
	}
	if binOp.Op == opcode.LogicAnd {
		nvPairs, isTableDual = getNameValuePairs(ctx, tbl, tblName, nvPairs, binOp.L)
		if nvPairs == nil || isTableDual {
			return nil, isTableDual
		}
		nvPairs, isTableDual = getNameValuePairs(ctx, tbl, tblName, nvPairs, binOp.R)
		if nvPairs == nil || isTableDual {
			return nil, isTableDual
		}
		return nvPairs, isTableDual
	} else if binOp.Op == opcode.EQ {
		var (
			d       types.Datum
			colName *ast.ColumnNameExpr
			ok      bool
			con     *expression.Constant
			err     error
		)
		if colName, ok = binOp.L.(*ast.ColumnNameExpr); ok {
			switch x := binOp.R.(type) {
			case *driver.ValueExpr:
				d = x.Datum
			case *driver.ParamMarkerExpr:
				con, err = expression.ParamMarkerExpression(ctx, x, true)
				if err != nil {
					return nil, false
				}
				d, err = con.Eval(chunk.Row{})
				if err != nil {
					return nil, false
				}
			}
		} else if colName, ok = binOp.R.(*ast.ColumnNameExpr); ok {
			switch x := binOp.L.(type) {
			case *driver.ValueExpr:
				d = x.Datum
			case *driver.ParamMarkerExpr:
				con, err = expression.ParamMarkerExpression(ctx, x, true)
				if err != nil {
					return nil, false
				}
				d, err = con.Eval(chunk.Row{})
				if err != nil {
					return nil, false
				}
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
		if col == nil { // Handling the case when the column is _tidb_rowid.
			return append(nvPairs, nameValuePair{colName: colName.Name.Name.L, colFieldType: types.NewFieldType(mysql.TypeLonglong), value: d, con: con}), false
		} else if col.GetType() == mysql.TypeString && col.GetCollate() == charset.CollationBin { // This type we needn't to pad `\0` in here.
			return append(nvPairs, nameValuePair{colName: colName.Name.Name.L, colFieldType: &col.FieldType, value: d, con: con}), false
		}
		if !checkCanConvertInPointGet(col, d) {
			return nil, false
		}
		dVal, err := d.ConvertTo(stmtCtx, &col.FieldType)
		if err != nil {
			if terror.ErrorEqual(types.ErrOverflow, err) {
				return append(nvPairs, nameValuePair{colName: colName.Name.Name.L, colFieldType: &col.FieldType, value: d, con: con}), true
			}
			// Some scenarios cast to int with error, but we may use this value in point get.
			if !terror.ErrorEqual(types.ErrTruncatedWrongVal, err) {
				return nil, false
			}
		}
		// The converted result must be same as original datum.
		cmp, err := dVal.Compare(stmtCtx, &d, collate.GetCollator(col.GetCollate()))
		if err != nil || cmp != 0 {
			return nil, false
		}
		return append(nvPairs, nameValuePair{colName: colName.Name.Name.L, colFieldType: &col.FieldType, value: dVal, con: con}), false
	}
	return nil, false
}

func getPointGetValue(stmtCtx *stmtctx.StatementContext, col *model.ColumnInfo, d *types.Datum) *types.Datum {
	if !checkCanConvertInPointGet(col, *d) {
		return nil
	}
	dVal, err := d.ConvertTo(stmtCtx, &col.FieldType)
	if err != nil {
		return nil
	}
	// The converted result must be same as original datum.
	cmp, err := dVal.Compare(stmtCtx, d, collate.GetCollator(col.GetCollate()))
	if err != nil || cmp != 0 {
		return nil
	}
	return &dVal
}

func checkCanConvertInPointGet(col *model.ColumnInfo, d types.Datum) bool {
	kind := d.Kind()
	switch col.FieldType.EvalType() {
	case ptypes.ETString:
		switch kind {
		case types.KindInt64, types.KindUint64,
			types.KindFloat32, types.KindFloat64, types.KindMysqlDecimal:
			// column type is String and constant type is numeric
			return false
		}
	}
	switch col.FieldType.GetType() {
	case mysql.TypeBit:
		switch kind {
		case types.KindString:
			// column type is Bit and constant type is string
			return false
		}
	}
	return true
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
		if mysql.HasPriKeyFlag(col.GetFlag()) {
			i := findInPairs(col.Name.L, pairs)
			if i == -1 {
				return handlePair, nil
			}
			return pairs[i], &col.FieldType
		}
	}
	return handlePair, nil
}

func getIndexValues(idxInfo *model.IndexInfo, pairs []nameValuePair) ([]types.Datum, []*expression.Constant, []*types.FieldType) {
	idxValues := make([]types.Datum, 0, 4)
	idxConstants := make([]*expression.Constant, 0, 4)
	colsFieldType := make([]*types.FieldType, 0, 4)
	if len(idxInfo.Columns) != len(pairs) {
		return nil, nil, nil
	}
	if idxInfo.HasPrefixIndex() {
		return nil, nil, nil
	}
	for _, idxCol := range idxInfo.Columns {
		i := findInPairs(idxCol.Name.L, pairs)
		if i == -1 {
			return nil, nil, nil
		}
		idxValues = append(idxValues, pairs[i].value)
		idxConstants = append(idxConstants, pairs[i].con)
		colsFieldType = append(colsFieldType, pairs[i].colFieldType)
	}
	if len(idxValues) > 0 {
		return idxValues, idxConstants, colsFieldType
	}
	return nil, nil, nil
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
	// avoid using the point_get when assignment_list contains the subquery in the UPDATE.
	for _, list := range updateStmt.List {
		if _, ok := list.Expr.(*ast.SubqueryExpr); ok {
			return nil
		}
	}
	selStmt := &ast.SelectStmt{
		Fields:  &ast.FieldList{},
		From:    updateStmt.TableRefs,
		Where:   updateStmt.Where,
		OrderBy: updateStmt.Order,
		Limit:   updateStmt.Limit,
	}
	pointGet := tryPointGetPlan(ctx, selStmt, true)
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
	handleCols := buildHandleCols(ctx, tbl, pointPlan.Schema())
	updatePlan := Update{
		SelectPlan:  pointPlan,
		OrderedList: orderedList,
		TblColPosInfos: TblColPosInfoSlice{
			TblColPosInfo{
				TblID:      tbl.ID,
				Start:      0,
				End:        pointPlan.Schema().Len(),
				HandleCols: handleCols,
			},
		},
		AllAssignmentsAreConstant: allAssignmentsAreConstant,
		VirtualAssignmentsOffset:  len(orderedList),
	}.Init(ctx)
	updatePlan.names = pointPlan.OutputNames()
	is := ctx.GetInfoSchema().(infoschema.InfoSchema)
	t, _ := is.TableByID(tbl.ID)
	updatePlan.tblID2Table = map[int64]table.Table{
		tbl.ID: t,
	}
	if tbl.GetPartitionInfo() != nil {
		pt := t.(table.PartitionedTable)
		var updateTableList []*ast.TableName
		updateTableList = extractTableList(updateStmt.TableRefs.TableRefs, updateTableList, true)
		updatePlan.PartitionedTable = make([]table.PartitionedTable, 0, len(updateTableList))
		for _, updateTable := range updateTableList {
			if len(updateTable.PartitionNames) > 0 {
				pids := make(map[int64]struct{}, len(updateTable.PartitionNames))
				for _, name := range updateTable.PartitionNames {
					pid, err := tables.FindPartitionByName(tbl, name.L)
					if err != nil {
						return updatePlan
					}
					pids[pid] = struct{}{}
				}
				pt = tables.NewPartitionTableWithGivenSets(pt, pids)
			}
			updatePlan.PartitionedTable = append(updatePlan.PartitionedTable, pt)
		}
	}
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
		defaultExpr := extractDefaultExpr(assign.Expr)
		if defaultExpr != nil {
			defaultExpr.Name = assign.Column
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
	if pointGet := tryPointGetPlan(ctx, selStmt, true); pointGet != nil {
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
	handleCols := buildHandleCols(ctx, tbl, pointPlan.Schema())
	delPlan := Delete{
		SelectPlan: pointPlan,
		TblColPosInfos: TblColPosInfoSlice{
			TblColPosInfo{
				TblID:      tbl.ID,
				Start:      0,
				End:        pointPlan.Schema().Len(),
				HandleCols: handleCols,
			},
		},
	}.Init(ctx)
	return delPlan
}

func findCol(tbl *model.TableInfo, colName *ast.ColumnName) *model.ColumnInfo {
	if colName.Name.L == model.ExtraHandleName.L && !tbl.PKIsHandle {
		colInfo := model.NewExtraHandleColInfo()
		colInfo.Offset = len(tbl.Columns) - 1
		return colInfo
	}
	for _, col := range tbl.Columns {
		if col.Name.L == colName.Name.L {
			return col
		}
	}
	return nil
}

func colInfoToColumn(col *model.ColumnInfo, idx int) *expression.Column {
	return &expression.Column{
		RetType:  col.FieldType.Clone(),
		ID:       col.ID,
		UniqueID: int64(col.Offset),
		Index:    idx,
		OrigName: col.Name.L,
	}
}

func buildHandleCols(ctx sessionctx.Context, tbl *model.TableInfo, schema *expression.Schema) HandleCols {
	// fields len is 0 for update and delete.
	if tbl.PKIsHandle {
		for i, col := range tbl.Columns {
			if mysql.HasPriKeyFlag(col.GetFlag()) {
				return &IntHandleCols{col: schema.Columns[i]}
			}
		}
	}

	if tbl.IsCommonHandle {
		pkIdx := tables.FindPrimaryIndex(tbl)
		return NewCommonHandleCols(ctx.GetSessionVars().StmtCtx, tbl, pkIdx, schema.Columns)
	}

	handleCol := colInfoToColumn(model.NewExtraHandleColInfo(), schema.Len())
	schema.Append(handleCol)
	return &IntHandleCols{col: handleCol}
}

func getPartitionInfo(ctx sessionctx.Context, tbl *model.TableInfo, pairs []nameValuePair) (*model.PartitionDefinition, int, bool) {
	partitionExpr := getPartitionExpr(ctx, tbl)
	if partitionExpr == nil {
		return nil, 0, false
	}

	pi := tbl.GetPartitionInfo()
	if pi == nil {
		return nil, 0, false
	}

	switch pi.Type {
	case model.PartitionTypeHash:
		expr := partitionExpr.OrigExpr
		col, ok := expr.(*ast.ColumnNameExpr)
		if !ok {
			return nil, 0, false
		}

		partitionColName := col.Name
		if partitionColName == nil {
			return nil, 0, false
		}

		for i, pair := range pairs {
			if partitionColName.Name.L == pair.colName {
				val := pair.value.GetInt64()
				pos := mathutil.Abs(val % int64(pi.Num))
				return &pi.Definitions[pos], i, false
			}
		}
	case model.PartitionTypeRange:
		// left range columns partition for future development
		if len(pi.Columns) == 0 {
			if col, ok := partitionExpr.Expr.(*expression.Column); ok {
				colInfo := findColNameByColID(tbl.Columns, col)
				for i, pair := range pairs {
					if colInfo.Name.L == pair.colName {
						val := pair.value.GetInt64() // val cannot be Null, we've check this in func getNameValuePairs
						unsigned := mysql.HasUnsignedFlag(col.GetType().GetFlag())
						ranges := partitionExpr.ForRangePruning
						length := len(ranges.LessThan)
						pos := sort.Search(length, func(i int) bool {
							return ranges.Compare(i, val, unsigned) > 0
						})
						if pos >= 0 && pos < length {
							return &pi.Definitions[pos], i, false
						}
						return nil, 0, true
					}
				}
			}
		}
	case model.PartitionTypeList:
		// left list columns partition for future development
		if partitionExpr.ForListPruning.ColPrunes == nil {
			locateExpr := partitionExpr.ForListPruning.LocateExpr
			if locateExpr, ok := locateExpr.(*expression.Column); ok {
				colInfo := findColNameByColID(tbl.Columns, locateExpr)
				for i, pair := range pairs {
					if colInfo.Name.L == pair.colName {
						val := pair.value.GetInt64() // val cannot be Null, we've check this in func getNameValuePairs
						isNull := false
						pos := partitionExpr.ForListPruning.LocatePartition(val, isNull)
						if pos >= 0 {
							return &pi.Definitions[pos], i, false
						}
						return nil, 0, true
					}
				}
			}
		}
	}
	return nil, 0, false
}

func findPartitionIdx(idxInfo *model.IndexInfo, pos int, pairs []nameValuePair) int {
	for i, idxCol := range idxInfo.Columns {
		if idxCol.Name.L == pairs[pos].colName {
			return i
		}
	}
	return 0
}

// getPartitionColumnPos gets the partition column's position in the unique index.
func getPartitionColumnPos(idx *model.IndexInfo, partitionExpr *tables.PartitionExpr, tbl *model.TableInfo) (int, error) {
	// regular table
	if partitionExpr == nil {
		return 0, nil
	}
	pi := tbl.GetPartitionInfo()
	if pi == nil {
		return 0, nil
	}

	var partitionName model.CIStr
	switch pi.Type {
	case model.PartitionTypeHash:
		if col, ok := partitionExpr.OrigExpr.(*ast.ColumnNameExpr); ok {
			partitionName = col.Name.Name
		} else {
			return 0, errors.Errorf("unsupported partition type in BatchGet")
		}
	case model.PartitionTypeRange:
		// left range columns partition for future development
		if col, ok := partitionExpr.Expr.(*expression.Column); ok && len(pi.Columns) == 0 {
			colInfo := findColNameByColID(tbl.Columns, col)
			partitionName = colInfo.Name
		} else {
			return 0, errors.Errorf("unsupported partition type in BatchGet")
		}
	case model.PartitionTypeList:
		// left list columns partition for future development
		if locateExpr, ok := partitionExpr.ForListPruning.LocateExpr.(*expression.Column); ok && partitionExpr.ForListPruning.ColPrunes == nil {
			colInfo := findColNameByColID(tbl.Columns, locateExpr)
			partitionName = colInfo.Name
		} else {
			return 0, errors.Errorf("unsupported partition type in BatchGet")
		}
	}

	for i, idxCol := range idx.Columns {
		if partitionName.L == idxCol.Name.L {
			return i, nil
		}
	}
	panic("unique index must include all partition columns")
}

// getHashPartitionColumnPos gets the hash partition column's position in the unique index.
func getHashPartitionColumnPos(idx *model.IndexInfo, partitionColName *ast.ColumnName) int {
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

func getPartitionExpr(ctx sessionctx.Context, tbl *model.TableInfo) *tables.PartitionExpr {
	is := ctx.GetInfoSchema().(infoschema.InfoSchema)
	table, ok := is.TableByID(tbl.ID)
	if !ok {
		return nil
	}

	partTable, ok := table.(partitionTable)
	if !ok {
		return nil
	}

	// PartitionExpr don't need columns and names for hash partition.
	partitionExpr, err := partTable.PartitionExpr()
	if err != nil {
		return nil
	}

	return partitionExpr
}

func getHashPartitionColumnName(ctx sessionctx.Context, tbl *model.TableInfo) *ast.ColumnName {
	pi := tbl.GetPartitionInfo()
	if pi == nil {
		return nil
	}
	if pi.Type != model.PartitionTypeHash {
		return nil
	}
	is := ctx.GetInfoSchema().(infoschema.InfoSchema)
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

func findColNameByColID(cols []*model.ColumnInfo, col *expression.Column) *model.ColumnInfo {
	for _, c := range cols {
		if c.ID == col.ID {
			return c
		}
	}
	return nil
}
