// Copyright 2015 PingCAP, Inc.
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
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/costusage"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/pingcap/tidb/pkg/util/texttree"
	"github.com/pingcap/tipb/go-tipb"
)

// ShowDDL is for showing DDL information.
type ShowDDL struct {
	baseSchemaProducer
}

// ShowSlow is for showing slow queries.
type ShowSlow struct {
	baseSchemaProducer

	*ast.ShowSlow
}

// ShowDDLJobQueries is for showing DDL job queries sql.
type ShowDDLJobQueries struct {
	baseSchemaProducer

	JobIDs []int64
}

// ShowDDLJobQueriesWithRange is for showing DDL job queries sql with specified limit and offset.
type ShowDDLJobQueriesWithRange struct {
	baseSchemaProducer

	Limit  uint64
	Offset uint64
}

// ShowNextRowID is for showing the next global row ID.
type ShowNextRowID struct {
	baseSchemaProducer
	TableName *ast.TableName
}

// CheckTable is used for checking table data, built from the 'admin check table' statement.
type CheckTable struct {
	baseSchemaProducer

	DBName             string
	Table              table.Table
	IndexInfos         []*model.IndexInfo
	IndexLookUpReaders []*PhysicalIndexLookUpReader
	CheckIndex         bool
}

// RecoverIndex is used for backfilling corrupted index data.
type RecoverIndex struct {
	baseSchemaProducer

	Table     *ast.TableName
	IndexName string
}

// CleanupIndex is used to delete dangling index data.
type CleanupIndex struct {
	baseSchemaProducer

	Table     *ast.TableName
	IndexName string
}

// CheckIndexRange is used for checking index data, output the index values that handle within begin and end.
type CheckIndexRange struct {
	baseSchemaProducer

	Table     *ast.TableName
	IndexName string

	HandleRanges []ast.HandleRange
}

// ChecksumTable is used for calculating table checksum, built from the `admin checksum table` statement.
type ChecksumTable struct {
	baseSchemaProducer

	Tables []*ast.TableName
}

// CancelDDLJobs represents a cancel DDL jobs plan.
type CancelDDLJobs struct {
	baseSchemaProducer

	JobIDs []int64
}

// PauseDDLJobs indicates a plan to pause the Running DDL Jobs.
type PauseDDLJobs struct {
	baseSchemaProducer

	JobIDs []int64
}

// ResumeDDLJobs indicates a plan to resume the Paused DDL Jobs.
type ResumeDDLJobs struct {
	baseSchemaProducer

	JobIDs []int64
}

// ReloadExprPushdownBlacklist reloads the data from expr_pushdown_blacklist table.
type ReloadExprPushdownBlacklist struct {
	baseSchemaProducer
}

// ReloadOptRuleBlacklist reloads the data from opt_rule_blacklist table.
type ReloadOptRuleBlacklist struct {
	baseSchemaProducer
}

// AdminPluginsAction indicate action will be taken on plugins.
type AdminPluginsAction int

const (
	// Enable indicates enable plugins.
	Enable AdminPluginsAction = iota + 1
	// Disable indicates disable plugins.
	Disable
)

// AdminPlugins administrates tidb plugins.
type AdminPlugins struct {
	baseSchemaProducer
	Action  AdminPluginsAction
	Plugins []string
}

// Change represents a change plan.
type Change struct {
	baseSchemaProducer
	*ast.ChangeStmt
}

// Prepare represents prepare plan.
type Prepare struct {
	baseSchemaProducer

	Name    string
	SQLText string
}

// Execute represents prepare plan.
type Execute struct {
	baseSchemaProducer

	Name     string
	Params   []expression.Expression
	PrepStmt *PlanCacheStmt
	Stmt     ast.StmtNode
	Plan     base.Plan
}

// Check if result of GetVar expr is BinaryLiteral
// Because GetVar use String to represent BinaryLiteral, here we need to convert string back to BinaryLiteral.
func isGetVarBinaryLiteral(sctx base.PlanContext, expr expression.Expression) (res bool) {
	scalarFunc, ok := expr.(*expression.ScalarFunction)
	if ok && scalarFunc.FuncName.L == ast.GetVar {
		name, isNull, err := scalarFunc.GetArgs()[0].EvalString(sctx.GetExprCtx().GetEvalCtx(), chunk.Row{})
		if err != nil || isNull {
			res = false
		} else if dt, ok2 := sctx.GetSessionVars().GetUserVarVal(name); ok2 {
			res = dt.Kind() == types.KindBinaryLiteral
		}
	}
	return res
}

// Deallocate represents deallocate plan.
type Deallocate struct {
	baseSchemaProducer

	Name string
}

// Set represents a plan for set stmt.
type Set struct {
	baseSchemaProducer

	VarAssigns []*expression.VarAssignment
}

// SetConfig represents a plan for set config stmt.
type SetConfig struct {
	baseSchemaProducer

	Type     string
	Instance string
	Name     string
	Value    expression.Expression
}

// SQLBindOpType repreents the SQL bind type
type SQLBindOpType int

const (
	// OpSQLBindCreate represents the operation to create a SQL bind.
	OpSQLBindCreate SQLBindOpType = iota
	// OpSQLBindDrop represents the operation to drop a SQL bind.
	OpSQLBindDrop
	// OpFlushBindings is used to flush plan bindings.
	OpFlushBindings
	// OpCaptureBindings is used to capture plan bindings.
	OpCaptureBindings
	// OpEvolveBindings is used to evolve plan binding.
	OpEvolveBindings
	// OpReloadBindings is used to reload plan binding.
	OpReloadBindings
	// OpSetBindingStatus is used to set binding status.
	OpSetBindingStatus
	// OpSQLBindDropByDigest is used to drop SQL binds by digest
	OpSQLBindDropByDigest
	// OpSetBindingStatusByDigest represents the operation to set SQL binding status by sql digest.
	OpSetBindingStatusByDigest
)

// SQLBindPlan represents a plan for SQL bind.
type SQLBindPlan struct {
	baseSchemaProducer

	SQLBindOp    SQLBindOpType
	NormdOrigSQL string
	BindSQL      string
	IsGlobal     bool
	BindStmt     ast.StmtNode
	Db           string
	Charset      string
	Collation    string
	NewStatus    string
	Source       string // Source indicate how this binding was created, eg: bindinfo.Manual or bindinfo.History
	SQLDigest    string
	PlanDigest   string
}

// Simple represents a simple statement plan which doesn't need any optimization.
type Simple struct {
	baseSchemaProducer

	Statement ast.StmtNode

	// IsFromRemote indicates whether the statement IS FROM REMOTE TiDB instance in cluster,
	//   and executing in co-processor.
	//   Used for `global kill`. See https://github.com/pingcap/tidb/blob/master/docs/design/2020-06-01-global-kill.md.
	IsFromRemote bool

	// StaleTxnStartTS is the StartTS that is used to build a staleness transaction by 'START TRANSACTION READ ONLY' statement.
	StaleTxnStartTS uint64
}

// MemoryUsage return the memory usage of Simple
func (s *Simple) MemoryUsage() (sum int64) {
	if s == nil {
		return
	}

	sum = s.baseSchemaProducer.MemoryUsage() + size.SizeOfInterface + size.SizeOfBool + size.SizeOfUint64
	return
}

// PhysicalSimpleWrapper is a wrapper of `Simple` to implement physical plan interface.
//
//	Used for simple statements executing in coprocessor.
type PhysicalSimpleWrapper struct {
	basePhysicalPlan
	Inner Simple
}

// MemoryUsage return the memory usage of PhysicalSimpleWrapper
func (p *PhysicalSimpleWrapper) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.basePhysicalPlan.MemoryUsage() + p.Inner.MemoryUsage()
	return
}

// InsertGeneratedColumns is for completing generated columns in Insert.
// We resolve generation expressions in plan, and eval those in executor.
type InsertGeneratedColumns struct {
	Columns      []*ast.ColumnName
	Exprs        []expression.Expression
	OnDuplicates []*expression.Assignment
}

// MemoryUsage return the memory usage of InsertGeneratedColumns
func (i *InsertGeneratedColumns) MemoryUsage() (sum int64) {
	if i == nil {
		return
	}
	sum = size.SizeOfSlice*3 + int64(cap(i.Columns)+cap(i.OnDuplicates))*size.SizeOfPointer + int64(cap(i.Exprs))*size.SizeOfInterface

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
	baseSchemaProducer

	Table         table.Table
	tableSchema   *expression.Schema
	tableColNames types.NameSlice
	Columns       []*ast.ColumnName
	Lists         [][]expression.Expression

	OnDuplicate        []*expression.Assignment
	Schema4OnDuplicate *expression.Schema
	names4OnDuplicate  types.NameSlice

	GenCols InsertGeneratedColumns

	SelectPlan base.PhysicalPlan

	IsReplace bool

	// NeedFillDefaultValue is true when expr in value list reference other column.
	NeedFillDefaultValue bool

	AllAssignmentsAreConstant bool

	RowLen int

	FKChecks   []*FKCheck
	FKCascades []*FKCascade
}

// MemoryUsage return the memory usage of Insert
func (p *Insert) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.baseSchemaProducer.MemoryUsage() + size.SizeOfInterface + size.SizeOfSlice*7 + int64(cap(p.tableColNames)+
		cap(p.Columns)+cap(p.OnDuplicate)+cap(p.names4OnDuplicate)+cap(p.FKChecks))*size.SizeOfPointer +
		p.GenCols.MemoryUsage() + size.SizeOfInterface + size.SizeOfBool*4 + size.SizeOfInt
	if p.tableSchema != nil {
		sum += p.tableSchema.MemoryUsage()
	}
	if p.Schema4OnDuplicate != nil {
		sum += p.Schema4OnDuplicate.MemoryUsage()
	}
	if p.SelectPlan != nil {
		sum += p.SelectPlan.MemoryUsage()
	}

	for _, name := range p.tableColNames {
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
	for _, name := range p.names4OnDuplicate {
		sum += name.MemoryUsage()
	}
	for _, fkC := range p.FKChecks {
		sum += fkC.MemoryUsage()
	}

	return
}

// Update represents Update plan.
type Update struct {
	baseSchemaProducer

	OrderedList []*expression.Assignment

	AllAssignmentsAreConstant bool

	VirtualAssignmentsOffset int

	SelectPlan base.PhysicalPlan

	TblColPosInfos TblColPosInfoSlice

	// Used when partition sets are given.
	// e.g. update t partition(p0) set a = 1;
	PartitionedTable []table.PartitionedTable

	tblID2Table map[int64]table.Table

	FKChecks   map[int64][]*FKCheck
	FKCascades map[int64][]*FKCascade
}

// MemoryUsage return the memory usage of Update
func (p *Update) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.baseSchemaProducer.MemoryUsage() + size.SizeOfSlice*3 + int64(cap(p.OrderedList))*size.SizeOfPointer +
		size.SizeOfBool + size.SizeOfInt + size.SizeOfInterface + int64(cap(p.PartitionedTable))*size.SizeOfInterface +
		int64(len(p.tblID2Table))*(size.SizeOfInt64+size.SizeOfInterface)
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
	baseSchemaProducer

	IsMultiTable bool

	SelectPlan base.PhysicalPlan

	TblColPosInfos TblColPosInfoSlice

	FKChecks   map[int64][]*FKCheck
	FKCascades map[int64][]*FKCascade
}

// MemoryUsage return the memory usage of Delete
func (p *Delete) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.baseSchemaProducer.MemoryUsage() + size.SizeOfBool + size.SizeOfInterface + size.SizeOfSlice
	if p.SelectPlan != nil {
		sum += p.SelectPlan.MemoryUsage()
	}
	for _, colInfo := range p.TblColPosInfos {
		sum += colInfo.MemoryUsage()
	}
	return
}

// AnalyzeInfo is used to store the database name, table name and partition name of analyze task.
type AnalyzeInfo struct {
	DBName        string
	TableName     string
	PartitionName string
	TableID       statistics.AnalyzeTableID
	StatsVersion  int
	V2Options     *V2AnalyzeOptions
}

// V2AnalyzeOptions is used to hold analyze options information.
type V2AnalyzeOptions struct {
	PhyTableID  int64
	RawOpts     map[ast.AnalyzeOptionType]uint64
	FilledOpts  map[ast.AnalyzeOptionType]uint64
	ColChoice   model.ColumnChoice
	ColumnList  []*model.ColumnInfo
	IsPartition bool
}

// AnalyzeColumnsTask is used for analyze columns.
type AnalyzeColumnsTask struct {
	HandleCols       util.HandleCols
	CommonHandleInfo *model.IndexInfo
	ColsInfo         []*model.ColumnInfo
	TblInfo          *model.TableInfo
	Indexes          []*model.IndexInfo
	AnalyzeInfo
}

// AnalyzeIndexTask is used for analyze index.
type AnalyzeIndexTask struct {
	IndexInfo *model.IndexInfo
	TblInfo   *model.TableInfo
	AnalyzeInfo
}

// Analyze represents an analyze plan
type Analyze struct {
	baseSchemaProducer

	ColTasks []AnalyzeColumnsTask
	IdxTasks []AnalyzeIndexTask
	Opts     map[ast.AnalyzeOptionType]uint64
	// OptionsMap is used to store the options for each partition.
	OptionsMap map[int64]V2AnalyzeOptions
}

// LoadData represents a loaddata plan.
type LoadData struct {
	baseSchemaProducer

	FileLocRef  ast.FileLocRefTp
	OnDuplicate ast.OnDuplicateKeyHandlingType
	Path        string
	Format      *string
	Table       *ast.TableName
	Charset     *string
	Columns     []*ast.ColumnName
	FieldsInfo  *ast.FieldsClause
	LinesInfo   *ast.LinesClause
	IgnoreLines *uint64

	ColumnAssignments  []*ast.Assignment
	ColumnsAndUserVars []*ast.ColumnNameOrUserVar
	Options            []*LoadDataOpt

	GenCols InsertGeneratedColumns
}

// LoadDataOpt represents load data option.
type LoadDataOpt struct {
	// Name is the name of the option, converted to lower case during parse.
	Name  string
	Value expression.Expression
}

// ImportInto represents a ingest into plan.
type ImportInto struct {
	baseSchemaProducer

	Table              *ast.TableName
	ColumnAssignments  []*ast.Assignment
	ColumnsAndUserVars []*ast.ColumnNameOrUserVar
	Path               string
	Format             *string
	Options            []*LoadDataOpt

	GenCols InsertGeneratedColumns
	Stmt    string

	SelectPlan base.PhysicalPlan
}

// LoadStats represents a load stats plan.
type LoadStats struct {
	baseSchemaProducer

	Path string
}

// LockStats represents a lock stats for table
type LockStats struct {
	baseSchemaProducer

	Tables []*ast.TableName
}

// UnlockStats represents a unlock stats for table
type UnlockStats struct {
	baseSchemaProducer

	Tables []*ast.TableName
}

// PlanReplayer represents a plan replayer plan.
type PlanReplayer struct {
	baseSchemaProducer
	ExecStmt          ast.StmtNode
	Analyze           bool
	Load              bool
	File              string
	HistoricalStatsTS uint64

	Capture    bool
	Remove     bool
	SQLDigest  string
	PlanDigest string
}

// IndexAdvise represents a index advise plan.
type IndexAdvise struct {
	baseSchemaProducer

	IsLocal     bool
	Path        string
	MaxMinutes  uint64
	MaxIndexNum *ast.MaxIndexNumClause
	LineFieldsInfo
}

// SplitRegion represents a split regions plan.
type SplitRegion struct {
	baseSchemaProducer

	TableInfo      *model.TableInfo
	PartitionNames []model.CIStr
	IndexInfo      *model.IndexInfo
	Lower          []types.Datum
	Upper          []types.Datum
	Num            int
	ValueLists     [][]types.Datum
}

// SplitRegionStatus represents a split regions status plan.
type SplitRegionStatus struct {
	baseSchemaProducer

	Table     table.Table
	IndexInfo *model.IndexInfo
}

// CompactTable represents a "ALTER TABLE [NAME] COMPACT ..." plan.
type CompactTable struct {
	baseSchemaProducer

	ReplicaKind    ast.CompactReplicaKind
	TableInfo      *model.TableInfo
	PartitionNames []model.CIStr
}

// DDL represents a DDL statement plan.
type DDL struct {
	baseSchemaProducer

	Statement ast.DDLNode
}

// SelectInto represents a select-into plan.
type SelectInto struct {
	baseSchemaProducer

	TargetPlan base.Plan
	IntoOpt    *ast.SelectIntoOption
	LineFieldsInfo
}

// LineFieldsInfo used in load-data/select-into/index-advise stmt.
type LineFieldsInfo struct {
	FieldsTerminatedBy string
	FieldsEnclosedBy   string // length always <= 1, see parser.y
	FieldsEscapedBy    string // length always <= 1, see parser.y
	FieldsOptEnclosed  bool
	LinesStartingBy    string
	LinesTerminatedBy  string
}

// NewLineFieldsInfo new LineFieldsInfo from FIELDS/LINES info.
func NewLineFieldsInfo(fieldsInfo *ast.FieldsClause, linesInfo *ast.LinesClause) LineFieldsInfo {
	e := LineFieldsInfo{
		FieldsTerminatedBy: "\t",
		FieldsEnclosedBy:   "",
		FieldsEscapedBy:    "\\",
		FieldsOptEnclosed:  false,
		LinesStartingBy:    "",
		LinesTerminatedBy:  "\n",
	}

	if fieldsInfo != nil {
		if fieldsInfo.Terminated != nil {
			e.FieldsTerminatedBy = *fieldsInfo.Terminated
		}
		if fieldsInfo.Enclosed != nil {
			e.FieldsEnclosedBy = *fieldsInfo.Enclosed
		}
		if fieldsInfo.Escaped != nil {
			e.FieldsEscapedBy = *fieldsInfo.Escaped
		}
		e.FieldsOptEnclosed = fieldsInfo.OptEnclosed
	}
	if linesInfo != nil {
		if linesInfo.Starting != nil {
			e.LinesStartingBy = *linesInfo.Starting
		}
		if linesInfo.Terminated != nil {
			e.LinesTerminatedBy = *linesInfo.Terminated
		}
	}
	return e
}

// ExplainInfoForEncode store explain info for JSON encode
type ExplainInfoForEncode struct {
	ID                  string                  `json:"id"`
	EstRows             string                  `json:"estRows"`
	ActRows             string                  `json:"actRows,omitempty"`
	TaskType            string                  `json:"taskType"`
	AccessObject        string                  `json:"accessObject,omitempty"`
	ExecuteInfo         string                  `json:"executeInfo,omitempty"`
	OperatorInfo        string                  `json:"operatorInfo,omitempty"`
	EstCost             string                  `json:"estCost,omitempty"`
	CostFormula         string                  `json:"costFormula,omitempty"`
	MemoryInfo          string                  `json:"memoryInfo,omitempty"`
	DiskInfo            string                  `json:"diskInfo,omitempty"`
	TotalMemoryConsumed string                  `json:"totalMemoryConsumed,omitempty"`
	SubOperators        []*ExplainInfoForEncode `json:"subOperators,omitempty"`
}

// JSONToString convert json to string
func JSONToString(j []*ExplainInfoForEncode) (string, error) {
	byteBuffer := bytes.NewBuffer([]byte{})
	encoder := json.NewEncoder(byteBuffer)
	// avoid wrongly embedding
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "    ")
	err := encoder.Encode(j)
	if err != nil {
		return "", err
	}
	return byteBuffer.String(), nil
}

// Explain represents a explain plan.
type Explain struct {
	baseSchemaProducer

	TargetPlan       base.Plan
	Format           string
	Analyze          bool
	ExecStmt         ast.StmtNode
	RuntimeStatsColl *execdetails.RuntimeStatsColl

	Rows        [][]string
	ExplainRows [][]string
}

// GetExplainRowsForPlan get explain rows for plan.
func GetExplainRowsForPlan(plan base.Plan) (rows [][]string) {
	explain := &Explain{
		TargetPlan: plan,
		Format:     types.ExplainFormatROW,
		Analyze:    false,
	}
	if err := explain.RenderResult(); err != nil {
		return rows
	}
	return explain.Rows
}

// GetExplainAnalyzeRowsForPlan get explain rows for plan.
func GetExplainAnalyzeRowsForPlan(plan *Explain) (rows [][]string) {
	if err := plan.prepareSchema(); err != nil {
		return rows
	}
	if err := plan.RenderResult(); err != nil {
		return rows
	}
	return plan.Rows
}

// prepareSchema prepares explain's result schema.
func (e *Explain) prepareSchema() error {
	var fieldNames []string
	format := strings.ToLower(e.Format)
	if format == types.ExplainFormatTraditional {
		format = types.ExplainFormatROW
		e.Format = types.ExplainFormatROW
	}
	switch {
	case (format == types.ExplainFormatROW || format == types.ExplainFormatBrief || format == types.ExplainFormatPlanCache) && (!e.Analyze && e.RuntimeStatsColl == nil):
		fieldNames = []string{"id", "estRows", "task", "access object", "operator info"}
	case format == types.ExplainFormatVerbose:
		if e.Analyze || e.RuntimeStatsColl != nil {
			fieldNames = []string{"id", "estRows", "estCost", "actRows", "task", "access object", "execution info", "operator info", "memory", "disk"}
		} else {
			fieldNames = []string{"id", "estRows", "estCost", "task", "access object", "operator info"}
		}
	case format == types.ExplainFormatTrueCardCost:
		fieldNames = []string{"id", "estRows", "estCost", "costFormula", "actRows", "task", "access object", "execution info", "operator info", "memory", "disk"}
	case format == types.ExplainFormatCostTrace:
		if e.Analyze || e.RuntimeStatsColl != nil {
			fieldNames = []string{"id", "estRows", "estCost", "costFormula", "actRows", "task", "access object", "execution info", "operator info", "memory", "disk"}
		} else {
			fieldNames = []string{"id", "estRows", "estCost", "costFormula", "task", "access object", "operator info"}
		}
	case (format == types.ExplainFormatROW || format == types.ExplainFormatBrief || format == types.ExplainFormatPlanCache) && (e.Analyze || e.RuntimeStatsColl != nil):
		fieldNames = []string{"id", "estRows", "actRows", "task", "access object", "execution info", "operator info", "memory", "disk"}
	case format == types.ExplainFormatDOT:
		fieldNames = []string{"dot contents"}
	case format == types.ExplainFormatHint:
		fieldNames = []string{"hint"}
	case format == types.ExplainFormatBinary:
		fieldNames = []string{"binary plan"}
	case format == types.ExplainFormatTiDBJSON:
		fieldNames = []string{"TiDB_JSON"}
	default:
		return errors.Errorf("explain format '%s' is not supported now", e.Format)
	}

	cwn := &columnsWithNames{
		cols:  make([]*expression.Column, 0, len(fieldNames)),
		names: make([]*types.FieldName, 0, len(fieldNames)),
	}

	for _, fieldName := range fieldNames {
		cwn.Append(buildColumnWithName("", fieldName, mysql.TypeString, mysql.MaxBlobWidth))
	}
	e.SetSchema(cwn.col2Schema())
	e.names = cwn.names
	return nil
}

// RenderResult renders the explain result as specified format.
func (e *Explain) RenderResult() error {
	if e.TargetPlan == nil {
		return nil
	}

	if e.Analyze && strings.ToLower(e.Format) == types.ExplainFormatTrueCardCost {
		// true_card_cost mode is used to calibrate the cost model.
		pp, ok := e.TargetPlan.(base.PhysicalPlan)
		if ok {
			if _, err := getPlanCost(pp, property.RootTaskType,
				optimizetrace.NewDefaultPlanCostOption().WithCostFlag(costusage.CostFlagRecalculate|costusage.CostFlagUseTrueCardinality|costusage.CostFlagTrace)); err != nil {
				return err
			}
			if pp.SCtx().GetSessionVars().CostModelVersion == modelVer2 {
				// output cost formula and factor costs through warning under model ver2 and true_card_cost mode for cost calibration.
				cost, _ := pp.GetPlanCostVer2(property.RootTaskType, optimizetrace.NewDefaultPlanCostOption())
				if cost.GetTrace() != nil {
					trace := cost.GetTrace()
					pp.SCtx().GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackErrorf("cost formula: %v", trace.GetFormula()))
					data, err := json.Marshal(trace.GetFactorCosts())
					if err != nil {
						pp.SCtx().GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackErrorf("marshal factor costs error %v", err))
					}
					pp.SCtx().GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackErrorf("factor costs: %v", string(data)))

					// output cost factor weights for cost calibration
					factors := defaultVer2Factors.tolist()
					weights := make(map[string]float64)
					for _, factor := range factors {
						if factorCost, ok := trace.GetFactorCosts()[factor.Name]; ok && factor.Value > 0 {
							weights[factor.Name] = factorCost / factor.Value // cost = [factors] * [weights]
						}
					}
					if wstr, err := json.Marshal(weights); err != nil {
						pp.SCtx().GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackErrorf("marshal weights error %v", err))
					} else {
						pp.SCtx().GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackErrorf("factor weights: %v", string(wstr)))
					}
				}
			}
		} else {
			e.SCtx().GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackError("'explain format=true_card_cost' cannot support this plan"))
		}
	}

	if strings.ToLower(e.Format) == types.ExplainFormatCostTrace {
		if pp, ok := e.TargetPlan.(base.PhysicalPlan); ok {
			// trigger getPlanCost again with CostFlagTrace to record all cost formulas
			if _, err := getPlanCost(pp, property.RootTaskType,
				optimizetrace.NewDefaultPlanCostOption().WithCostFlag(costusage.CostFlagRecalculate|costusage.CostFlagTrace)); err != nil {
				return err
			}
		}
	}

	switch strings.ToLower(e.Format) {
	case types.ExplainFormatROW, types.ExplainFormatBrief, types.ExplainFormatVerbose, types.ExplainFormatTrueCardCost, types.ExplainFormatCostTrace, types.ExplainFormatPlanCache:
		if e.Rows == nil || e.Analyze {
			flat := FlattenPhysicalPlan(e.TargetPlan, true)
			e.explainFlatPlanInRowFormat(flat)
			if e.Analyze &&
				e.SCtx().GetSessionVars().MemoryDebugModeMinHeapInUse != 0 &&
				e.SCtx().GetSessionVars().MemoryDebugModeAlarmRatio > 0 {
				row := e.Rows[0]
				tracker := e.SCtx().GetSessionVars().MemTracker
				row[7] = row[7] + "(Total: " + tracker.FormatBytes(tracker.MaxConsumed()) + ")"
			}
		}
	case types.ExplainFormatDOT:
		if physicalPlan, ok := e.TargetPlan.(base.PhysicalPlan); ok {
			e.prepareDotInfo(physicalPlan)
		}
	case types.ExplainFormatHint:
		flat := FlattenPhysicalPlan(e.TargetPlan, false)
		hints := GenHintsFromFlatPlan(flat)
		hints = append(hints, hint.ExtractTableHintsFromStmtNode(e.ExecStmt, nil)...)
		e.Rows = append(e.Rows, []string{hint.RestoreOptimizerHints(hints)})
	case types.ExplainFormatBinary:
		flat := FlattenPhysicalPlan(e.TargetPlan, false)
		str := BinaryPlanStrFromFlatPlan(e.SCtx(), flat)
		e.Rows = append(e.Rows, []string{str})
	case types.ExplainFormatTiDBJSON:
		flat := FlattenPhysicalPlan(e.TargetPlan, true)
		encodes := e.explainFlatPlanInJSONFormat(flat)
		if e.Analyze && len(encodes) > 0 &&
			e.SCtx().GetSessionVars().MemoryDebugModeMinHeapInUse != 0 &&
			e.SCtx().GetSessionVars().MemoryDebugModeAlarmRatio > 0 {
			encodeRoot := encodes[0]
			tracker := e.SCtx().GetSessionVars().MemTracker
			encodeRoot.TotalMemoryConsumed = tracker.FormatBytes(tracker.MaxConsumed())
		}
		str, err := JSONToString(encodes)
		if err != nil {
			return err
		}
		e.Rows = append(e.Rows, []string{str})
	default:
		return errors.Errorf("explain format '%s' is not supported now", e.Format)
	}
	return nil
}

func (e *Explain) explainFlatPlanInRowFormat(flat *FlatPhysicalPlan) {
	if flat == nil || len(flat.Main) == 0 || flat.InExplain {
		return
	}
	for _, flatOp := range flat.Main {
		e.explainFlatOpInRowFormat(flatOp)
	}
	for _, cte := range flat.CTEs {
		for _, flatOp := range cte {
			e.explainFlatOpInRowFormat(flatOp)
		}
	}
	for _, subQ := range flat.ScalarSubQueries {
		for _, flatOp := range subQ {
			e.explainFlatOpInRowFormat(flatOp)
		}
	}
}

func (e *Explain) explainFlatPlanInJSONFormat(flat *FlatPhysicalPlan) (encodes []*ExplainInfoForEncode) {
	if flat == nil || len(flat.Main) == 0 || flat.InExplain {
		return
	}
	// flat.Main[0] must be the root node of tree
	encodes = append(encodes, e.explainOpRecursivelyInJSONFormat(flat.Main[0], flat.Main))

	for _, cte := range flat.CTEs {
		encodes = append(encodes, e.explainOpRecursivelyInJSONFormat(cte[0], cte))
	}
	for _, subQ := range flat.ScalarSubQueries {
		encodes = append(encodes, e.explainOpRecursivelyInJSONFormat(subQ[0], subQ))
	}
	return
}

func (e *Explain) explainOpRecursivelyInJSONFormat(flatOp *FlatOperator, flats FlatPlanTree) *ExplainInfoForEncode {
	taskTp := ""
	if flatOp.IsRoot {
		taskTp = "root"
	} else {
		taskTp = flatOp.ReqType.Name() + "[" + flatOp.StoreType.Name() + "]"
	}
	explainID := flatOp.Origin.ExplainID().String() + flatOp.Label.String()
	textTreeExplainID := texttree.PrettyIdentifier(explainID, flatOp.TextTreeIndent, flatOp.IsLastChild)

	cur := e.prepareOperatorInfoForJSONFormat(flatOp.Origin, taskTp, textTreeExplainID, explainID)

	for _, idx := range flatOp.ChildrenIdx {
		cur.SubOperators = append(cur.SubOperators,
			e.explainOpRecursivelyInJSONFormat(flats[idx], flats))
	}
	return cur
}

func (e *Explain) explainFlatOpInRowFormat(flatOp *FlatOperator) {
	taskTp := ""
	if flatOp.IsRoot {
		taskTp = "root"
	} else {
		taskTp = flatOp.ReqType.Name() + "[" + flatOp.StoreType.Name() + "]"
	}
	textTreeExplainID := texttree.PrettyIdentifier(flatOp.Origin.ExplainID().String()+flatOp.Label.String(),
		flatOp.TextTreeIndent,
		flatOp.IsLastChild)
	e.prepareOperatorInfo(flatOp.Origin, taskTp, textTreeExplainID)
}

func getRuntimeInfoStr(ctx base.PlanContext, p base.Plan, runtimeStatsColl *execdetails.RuntimeStatsColl) (actRows, analyzeInfo, memoryInfo, diskInfo string) {
	if runtimeStatsColl == nil {
		runtimeStatsColl = ctx.GetSessionVars().StmtCtx.RuntimeStatsColl
		if runtimeStatsColl == nil {
			return
		}
	}
	rootStats, copStats, memTracker, diskTracker := getRuntimeInfo(ctx, p, runtimeStatsColl)
	actRows = "0"
	memoryInfo = "N/A"
	diskInfo = "N/A"
	if rootStats != nil {
		actRows = strconv.FormatInt(rootStats.GetActRows(), 10)
		analyzeInfo = rootStats.String()
	}
	if copStats != nil {
		if len(analyzeInfo) > 0 {
			analyzeInfo += ", "
		}
		analyzeInfo += copStats.String()
		actRows = strconv.FormatInt(copStats.GetActRows(), 10)
	}
	if memTracker != nil {
		memoryInfo = memTracker.FormatBytes(memTracker.MaxConsumed())
	}
	if diskTracker != nil {
		diskInfo = diskTracker.FormatBytes(diskTracker.MaxConsumed())
	}
	return
}

func getRuntimeInfo(ctx base.PlanContext, p base.Plan, runtimeStatsColl *execdetails.RuntimeStatsColl) (
	rootStats *execdetails.RootRuntimeStats,
	copStats *execdetails.CopRuntimeStats,
	memTracker *memory.Tracker,
	diskTracker *memory.Tracker,
) {
	if runtimeStatsColl == nil {
		runtimeStatsColl = ctx.GetSessionVars().StmtCtx.RuntimeStatsColl
	}
	explainID := p.ID()
	// There maybe some mock information for cop task to let runtimeStatsColl.Exists(p.ExplainID()) is true.
	// So check copTaskExecDetail first and print the real cop task information if it's not empty.
	if runtimeStatsColl != nil && runtimeStatsColl.ExistsRootStats(explainID) {
		rootStats = runtimeStatsColl.GetRootStats(explainID)
	}
	if runtimeStatsColl != nil && runtimeStatsColl.ExistsCopStats(explainID) {
		copStats = runtimeStatsColl.GetCopStats(explainID)
	}
	memTracker = ctx.GetSessionVars().StmtCtx.MemTracker.SearchTrackerWithoutLock(p.ID())
	diskTracker = ctx.GetSessionVars().StmtCtx.DiskTracker.SearchTrackerWithoutLock(p.ID())
	return
}

// prepareOperatorInfo generates the following information for every plan:
// operator id, estimated rows, task type, access object and other operator info.
func (e *Explain) prepareOperatorInfo(p base.Plan, taskType, id string) {
	if p.ExplainID().String() == "_0" {
		return
	}

	estRows, estCost, costFormula, accessObject, operatorInfo := e.getOperatorInfo(p, id)

	var row []string
	if e.Analyze || e.RuntimeStatsColl != nil {
		row = []string{id, estRows}
		if strings.ToLower(e.Format) == types.ExplainFormatVerbose || strings.ToLower(e.Format) == types.ExplainFormatTrueCardCost || strings.ToLower(e.Format) == types.ExplainFormatCostTrace {
			row = append(row, estCost)
		}
		if strings.ToLower(e.Format) == types.ExplainFormatTrueCardCost || strings.ToLower(e.Format) == types.ExplainFormatCostTrace {
			row = append(row, costFormula)
		}
		actRows, analyzeInfo, memoryInfo, diskInfo := getRuntimeInfoStr(e.SCtx(), p, e.RuntimeStatsColl)
		row = append(row, actRows, taskType, accessObject, analyzeInfo, operatorInfo, memoryInfo, diskInfo)
	} else {
		row = []string{id, estRows}
		if strings.ToLower(e.Format) == types.ExplainFormatVerbose || strings.ToLower(e.Format) == types.ExplainFormatTrueCardCost ||
			strings.ToLower(e.Format) == types.ExplainFormatCostTrace {
			row = append(row, estCost)
		}
		if strings.ToLower(e.Format) == types.ExplainFormatCostTrace {
			row = append(row, costFormula)
		}
		row = append(row, taskType, accessObject, operatorInfo)
	}
	e.Rows = append(e.Rows, row)
}

func (e *Explain) prepareOperatorInfoForJSONFormat(p base.Plan, taskType, id string, explainID string) *ExplainInfoForEncode {
	if p.ExplainID().String() == "_0" {
		return nil
	}

	estRows, _, _, accessObject, operatorInfo := e.getOperatorInfo(p, id)
	jsonRow := &ExplainInfoForEncode{
		ID:           explainID,
		EstRows:      estRows,
		TaskType:     taskType,
		AccessObject: accessObject,
		OperatorInfo: operatorInfo,
		SubOperators: make([]*ExplainInfoForEncode, 0),
	}

	if e.Analyze || e.RuntimeStatsColl != nil {
		jsonRow.ActRows, jsonRow.ExecuteInfo, jsonRow.MemoryInfo, jsonRow.DiskInfo = getRuntimeInfoStr(e.SCtx(), p, e.RuntimeStatsColl)
	}
	return jsonRow
}

func (e *Explain) getOperatorInfo(p base.Plan, id string) (estRows, estCost, costFormula, accessObject, operatorInfo string) {
	// For `explain for connection` statement, `e.ExplainRows` will be set.
	for _, row := range e.ExplainRows {
		if len(row) < 5 {
			panic("should never happen")
		}
		if row[0] == id {
			return row[1], "N/A", "N/A", row[3], row[4]
		}
	}

	pp, isPhysicalPlan := p.(base.PhysicalPlan)
	estRows = "N/A"
	estCost = "N/A"
	costFormula = "N/A"
	if isPhysicalPlan {
		estRows = strconv.FormatFloat(pp.GetEstRowCountForDisplay(), 'f', 2, 64)
		if e.SCtx() != nil && e.SCtx().GetSessionVars().CostModelVersion == modelVer2 {
			costVer2, _ := pp.GetPlanCostVer2(property.RootTaskType, optimizetrace.NewDefaultPlanCostOption())
			estCost = strconv.FormatFloat(costVer2.GetCost(), 'f', 2, 64)
			if costVer2.GetTrace() != nil {
				costFormula = costVer2.GetTrace().GetFormula()
			}
		} else {
			planCost, _ := getPlanCost(pp, property.RootTaskType, optimizetrace.NewDefaultPlanCostOption())
			estCost = strconv.FormatFloat(planCost, 'f', 2, 64)
		}
	} else if si := p.StatsInfo(); si != nil {
		estRows = strconv.FormatFloat(si.RowCount, 'f', 2, 64)
	}

	if plan, ok := p.(dataAccesser); ok {
		accessObject = plan.AccessObject().String()
		operatorInfo = plan.OperatorInfo(false)
	} else {
		if pa, ok := p.(partitionAccesser); ok && e.SCtx() != nil {
			accessObject = pa.accessObject(e.SCtx()).String()
		}
		operatorInfo = p.ExplainInfo()
	}
	return estRows, estCost, costFormula, accessObject, operatorInfo
}

// BinaryPlanStrFromFlatPlan generates the compressed and encoded binary plan from a FlatPhysicalPlan.
func BinaryPlanStrFromFlatPlan(explainCtx base.PlanContext, flat *FlatPhysicalPlan) string {
	binary := binaryDataFromFlatPlan(explainCtx, flat)
	if binary == nil {
		return ""
	}
	proto, err := binary.Marshal()
	if err != nil {
		return ""
	}
	str := plancodec.Compress(proto)
	return str
}

func binaryDataFromFlatPlan(explainCtx base.PlanContext, flat *FlatPhysicalPlan) *tipb.ExplainData {
	if len(flat.Main) == 0 {
		return nil
	}
	// Please see comments in EncodeFlatPlan() for this case.
	// We keep consistency with EncodeFlatPlan() here.
	if flat.InExecute {
		return nil
	}
	res := &tipb.ExplainData{}
	for _, op := range flat.Main {
		// We assume that runtime stats are available to this plan tree if any operator in the "Main" has runtime stats.
		rootStats, copStats, _, _ := getRuntimeInfo(explainCtx, op.Origin, nil)
		if rootStats != nil || copStats != nil {
			res.WithRuntimeStats = true
			break
		}
	}
	res.Main = binaryOpTreeFromFlatOps(explainCtx, flat.Main)
	for _, explainedCTE := range flat.CTEs {
		res.Ctes = append(res.Ctes, binaryOpTreeFromFlatOps(explainCtx, explainedCTE))
	}
	return res
}

func binaryOpTreeFromFlatOps(explainCtx base.PlanContext, ops FlatPlanTree) *tipb.ExplainOperator {
	s := make([]tipb.ExplainOperator, len(ops))
	for i, op := range ops {
		binaryOpFromFlatOp(explainCtx, op, &s[i])
		for _, idx := range op.ChildrenIdx {
			s[i].Children = append(s[i].Children, &s[idx])
		}
	}
	return &s[0]
}

func binaryOpFromFlatOp(explainCtx base.PlanContext, fop *FlatOperator, out *tipb.ExplainOperator) {
	out.Name = fop.Origin.ExplainID().String()
	switch fop.Label {
	case BuildSide:
		out.Labels = []tipb.OperatorLabel{tipb.OperatorLabel_buildSide}
	case ProbeSide:
		out.Labels = []tipb.OperatorLabel{tipb.OperatorLabel_probeSide}
	case SeedPart:
		out.Labels = []tipb.OperatorLabel{tipb.OperatorLabel_seedPart}
	case RecursivePart:
		out.Labels = []tipb.OperatorLabel{tipb.OperatorLabel_recursivePart}
	}
	switch fop.StoreType {
	case kv.TiDB:
		out.StoreType = tipb.StoreType_tidb
	case kv.TiKV:
		out.StoreType = tipb.StoreType_tikv
	case kv.TiFlash:
		out.StoreType = tipb.StoreType_tiflash
	}
	if fop.IsRoot {
		out.TaskType = tipb.TaskType_root
	} else {
		switch fop.ReqType {
		case Cop:
			out.TaskType = tipb.TaskType_cop
		case BatchCop:
			out.TaskType = tipb.TaskType_batchCop
		case MPP:
			out.TaskType = tipb.TaskType_mpp
		}
	}

	if fop.IsPhysicalPlan {
		p := fop.Origin.(base.PhysicalPlan)
		out.Cost, _ = getPlanCost(p, property.RootTaskType, optimizetrace.NewDefaultPlanCostOption())
		out.EstRows = p.GetEstRowCountForDisplay()
	} else if statsInfo := fop.Origin.StatsInfo(); statsInfo != nil {
		out.EstRows = statsInfo.RowCount
	}

	// Runtime info
	rootStats, copStats, memTracker, diskTracker := getRuntimeInfo(explainCtx, fop.Origin, nil)
	if rootStats != nil {
		basic, groups := rootStats.MergeStats()
		if basic != nil {
			out.RootBasicExecInfo = basic.String()
		}
		for _, group := range groups {
			str := group.String()
			if len(str) > 0 {
				out.RootGroupExecInfo = append(out.RootGroupExecInfo, str)
			}
		}
		out.ActRows = uint64(rootStats.GetActRows())
	}
	if copStats != nil {
		out.CopExecInfo = copStats.String()
		out.ActRows = uint64(copStats.GetActRows())
	}
	if memTracker != nil {
		out.MemoryBytes = memTracker.MaxConsumed()
	} else {
		out.MemoryBytes = -1
	}
	if diskTracker != nil {
		out.DiskBytes = diskTracker.MaxConsumed()
	} else {
		out.DiskBytes = -1
	}

	// Operator info
	if plan, ok := fop.Origin.(dataAccesser); ok {
		out.OperatorInfo = plan.OperatorInfo(false)
	} else {
		out.OperatorInfo = fop.Origin.ExplainInfo()
	}

	// Access object
	switch p := fop.Origin.(type) {
	case dataAccesser:
		ao := p.AccessObject()
		if ao != nil {
			ao.SetIntoPB(out)
		}
	case partitionAccesser:
		ao := p.accessObject(explainCtx)
		if ao != nil {
			ao.SetIntoPB(out)
		}
	}
}

func (e *Explain) prepareDotInfo(p base.PhysicalPlan) {
	buffer := bytes.NewBufferString("")
	fmt.Fprintf(buffer, "\ndigraph %s {\n", p.ExplainID())
	e.prepareTaskDot(p, "root", buffer)
	buffer.WriteString("}\n")

	e.Rows = append(e.Rows, []string{buffer.String()})
}

func (e *Explain) prepareTaskDot(p base.PhysicalPlan, taskTp string, buffer *bytes.Buffer) {
	fmt.Fprintf(buffer, "subgraph cluster%v{\n", p.ID())
	buffer.WriteString("node [style=filled, color=lightgrey]\n")
	buffer.WriteString("color=black\n")
	fmt.Fprintf(buffer, "label = \"%s\"\n", taskTp)

	if len(p.Children()) == 0 {
		if taskTp == "cop" {
			fmt.Fprintf(buffer, "\"%s\"\n}\n", p.ExplainID())
			return
		}
		fmt.Fprintf(buffer, "\"%s\"\n", p.ExplainID())
	}

	var copTasks []base.PhysicalPlan
	var pipelines []string

	for planQueue := []base.PhysicalPlan{p}; len(planQueue) > 0; planQueue = planQueue[1:] {
		curPlan := planQueue[0]
		switch copPlan := curPlan.(type) {
		case *PhysicalTableReader:
			pipelines = append(pipelines, fmt.Sprintf("\"%s\" -> \"%s\"\n", copPlan.ExplainID(), copPlan.tablePlan.ExplainID()))
			copTasks = append(copTasks, copPlan.tablePlan)
		case *PhysicalIndexReader:
			pipelines = append(pipelines, fmt.Sprintf("\"%s\" -> \"%s\"\n", copPlan.ExplainID(), copPlan.indexPlan.ExplainID()))
			copTasks = append(copTasks, copPlan.indexPlan)
		case *PhysicalIndexLookUpReader:
			pipelines = append(pipelines, fmt.Sprintf("\"%s\" -> \"%s\"\n", copPlan.ExplainID(), copPlan.tablePlan.ExplainID()))
			pipelines = append(pipelines, fmt.Sprintf("\"%s\" -> \"%s\"\n", copPlan.ExplainID(), copPlan.indexPlan.ExplainID()))
			copTasks = append(copTasks, copPlan.tablePlan)
			copTasks = append(copTasks, copPlan.indexPlan)
		case *PhysicalIndexMergeReader:
			for i := 0; i < len(copPlan.partialPlans); i++ {
				pipelines = append(pipelines, fmt.Sprintf("\"%s\" -> \"%s\"\n", copPlan.ExplainID(), copPlan.partialPlans[i].ExplainID()))
				copTasks = append(copTasks, copPlan.partialPlans[i])
			}
			if copPlan.tablePlan != nil {
				pipelines = append(pipelines, fmt.Sprintf("\"%s\" -> \"%s\"\n", copPlan.ExplainID(), copPlan.tablePlan.ExplainID()))
				copTasks = append(copTasks, copPlan.tablePlan)
			}
		}
		for _, child := range curPlan.Children() {
			fmt.Fprintf(buffer, "\"%s\" -> \"%s\"\n", curPlan.ExplainID(), child.ExplainID())
			planQueue = append(planQueue, child)
		}
	}
	buffer.WriteString("}\n")

	for _, cop := range copTasks {
		e.prepareTaskDot(cop, "cop", buffer)
	}

	for i := range pipelines {
		buffer.WriteString(pipelines[i])
	}
}

// IsPointGetWithPKOrUniqueKeyByAutoCommit returns true when meets following conditions:
//  1. ctx is auto commit tagged
//  2. session is not InTxn
//  3. plan is point get by pk, or point get by unique index (no double read)
func IsPointGetWithPKOrUniqueKeyByAutoCommit(vars *variable.SessionVars, p base.Plan) bool {
	if !IsAutoCommitTxn(vars) {
		return false
	}

	// check plan
	if proj, ok := p.(*PhysicalProjection); ok {
		p = proj.Children()[0]
	}

	switch v := p.(type) {
	case *PhysicalIndexReader:
		indexScan := v.IndexPlans[0].(*PhysicalIndexScan)
		return indexScan.IsPointGetByUniqueKey(vars.StmtCtx.TypeCtx())
	case *PhysicalTableReader:
		tableScan, ok := v.TablePlans[0].(*PhysicalTableScan)
		if !ok {
			return false
		}
		isPointRange := len(tableScan.Ranges) == 1 && tableScan.Ranges[0].IsPointNonNullable(vars.StmtCtx.TypeCtx())
		if !isPointRange {
			return false
		}
		pkLength := 1
		if tableScan.Table.IsCommonHandle {
			pkIdx := tables.FindPrimaryIndex(tableScan.Table)
			pkLength = len(pkIdx.Columns)
		}
		return len(tableScan.Ranges[0].LowVal) == pkLength
	case *PointGetPlan:
		// If the PointGetPlan needs to read data using unique index (double read), we
		// can't use max uint64, because using math.MaxUint64 can't guarantee repeatable-read
		// and the data and index would be inconsistent!
		// If the PointGetPlan needs to read data from Cache Table, we can't use max uint64,
		// because math.MaxUint64 always make cacheData invalid.
		noSecondRead := v.IndexInfo == nil || (v.IndexInfo.Primary && v.TblInfo.IsCommonHandle)
		if !noSecondRead {
			return false
		}
		if v.TblInfo != nil && (v.TblInfo.TableCacheStatusType != model.TableCacheStatusDisable) {
			return false
		}
		return true
	default:
		return false
	}
}

// IsAutoCommitTxn checks if session is in autocommit mode and not InTxn
// used for fast plan like point get
func IsAutoCommitTxn(vars *variable.SessionVars) bool {
	return vars.IsAutocommit() && !vars.InTxn()
}

// AdminShowBDRRole represents a show bdr role plan.
type AdminShowBDRRole struct {
	baseSchemaProducer
}
