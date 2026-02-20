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
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/costusage"
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
	"github.com/pingcap/tidb/pkg/util/texttree"
	"github.com/pingcap/tipb/go-tipb"
)

// ShowDDL is for showing DDL information.
type ShowDDL struct {
	physicalop.SimpleSchemaProducer
}

// ShowSlow is for showing slow queries.
type ShowSlow struct {
	physicalop.SimpleSchemaProducer

	*ast.ShowSlow
}

// ShowDDLJobQueries is for showing DDL job queries sql.
type ShowDDLJobQueries struct {
	physicalop.SimpleSchemaProducer

	JobIDs []int64
}

// ShowDDLJobQueriesWithRange is for showing DDL job queries sql with specified limit and offset.
type ShowDDLJobQueriesWithRange struct {
	physicalop.SimpleSchemaProducer

	Limit  uint64
	Offset uint64
}

// ShowNextRowID is for showing the next global row ID.
type ShowNextRowID struct {
	physicalop.SimpleSchemaProducer
	TableName *ast.TableName
}

// CheckTable is used for checking table data, built from the 'admin check table' statement.
type CheckTable struct {
	physicalop.SimpleSchemaProducer

	DBName             string
	Table              table.Table
	IndexInfos         []*model.IndexInfo
	IndexLookUpReaders []*physicalop.PhysicalIndexLookUpReader
	CheckIndex         bool
}

// RecoverIndex is used for backfilling corrupted index data.
type RecoverIndex struct {
	physicalop.SimpleSchemaProducer

	Table     *resolve.TableNameW
	IndexName string
}

// CleanupIndex is used to delete dangling index data.
type CleanupIndex struct {
	physicalop.SimpleSchemaProducer

	Table     *resolve.TableNameW
	IndexName string
}

// CheckIndexRange is used for checking index data, output the index values that handle within begin and end.
type CheckIndexRange struct {
	physicalop.SimpleSchemaProducer

	Table     *ast.TableName
	IndexName string

	HandleRanges []ast.HandleRange
}

// ChecksumTable is used for calculating table checksum, built from the `admin checksum table` statement.
type ChecksumTable struct {
	physicalop.SimpleSchemaProducer

	Tables []*resolve.TableNameW
}

// CancelDDLJobs represents a cancel DDL jobs plan.
type CancelDDLJobs struct {
	physicalop.SimpleSchemaProducer

	JobIDs []int64
}

// PauseDDLJobs indicates a plan to pause the Running DDL Jobs.
type PauseDDLJobs struct {
	physicalop.SimpleSchemaProducer

	JobIDs []int64
}

// ResumeDDLJobs indicates a plan to resume the Paused DDL Jobs.
type ResumeDDLJobs struct {
	physicalop.SimpleSchemaProducer

	JobIDs []int64
}

const (
	// AlterDDLJobThread alter reorg worker count
	AlterDDLJobThread = "thread"
	// AlterDDLJobBatchSize alter reorg batch size
	AlterDDLJobBatchSize = "batch_size"
	// AlterDDLJobMaxWriteSpeed alter reorg max write speed
	AlterDDLJobMaxWriteSpeed = "max_write_speed"
)

var allowedAlterDDLJobParams = map[string]struct{}{
	AlterDDLJobThread:        {},
	AlterDDLJobBatchSize:     {},
	AlterDDLJobMaxWriteSpeed: {},
}

// AlterDDLJobOpt represents alter ddl job option.
type AlterDDLJobOpt struct {
	Name  string
	Value expression.Expression
}

// AlterDDLJob is the plan of admin alter ddl job
type AlterDDLJob struct {
	physicalop.SimpleSchemaProducer

	JobID   int64
	Options []*AlterDDLJobOpt
}

// WorkloadRepoCreate is the plan of admin create workload snapshot.
type WorkloadRepoCreate struct {
	physicalop.SimpleSchemaProducer
}

// ReloadExprPushdownBlacklist reloads the data from expr_pushdown_blacklist table.
type ReloadExprPushdownBlacklist struct {
	physicalop.SimpleSchemaProducer
}

// ReloadOptRuleBlacklist reloads the data from opt_rule_blacklist table.
type ReloadOptRuleBlacklist struct {
	physicalop.SimpleSchemaProducer
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
	physicalop.SimpleSchemaProducer
	Action  AdminPluginsAction
	Plugins []string
}

// Prepare represents prepare plan.
type Prepare struct {
	physicalop.SimpleSchemaProducer

	Name    string
	SQLText string
}

// Execute represents prepare plan.
type Execute struct {
	physicalop.SimpleSchemaProducer

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
	physicalop.SimpleSchemaProducer

	Name string
}

// Set represents a plan for set stmt.
type Set struct {
	physicalop.SimpleSchemaProducer

	VarAssigns []*expression.VarAssignment
}

// SetConfig represents a plan for set config stmt.
type SetConfig struct {
	physicalop.SimpleSchemaProducer

	Type     string
	Instance string
	Name     string
	Value    expression.Expression
}

// RecommendIndexPlan represents a plan for recommend index stmt.
type RecommendIndexPlan struct {
	physicalop.SimpleSchemaProducer

	Action   string
	SQL      string
	AdviseID int64
	Options  []ast.RecommendIndexOption
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
	// OpReloadBindings is used to reload plan binding in this node.
	OpReloadBindings
	// OpReloadClusterBindings is used to reload plan binding in the entire cluster.
	OpReloadClusterBindings
	// OpSetBindingStatus is used to set binding status.
	OpSetBindingStatus
	// OpSQLBindDropByDigest is used to drop SQL binds by digest
	OpSQLBindDropByDigest
	// OpSetBindingStatusByDigest represents the operation to set SQL binding status by sql digest.
	OpSetBindingStatusByDigest
)

// SQLBindPlan represents a plan for SQL bind.
// One SQLBindPlan can be either global or session, and can only contain one type of operation, but can contain multiple
// operations of that type.
type SQLBindPlan struct {
	physicalop.SimpleSchemaProducer

	IsGlobal     bool
	SQLBindOp    SQLBindOpType
	Details      []*SQLBindOpDetail
	IsFromRemote bool
}

// SQLBindOpDetail represents the detail of an operation on a single binding.
// Different SQLBindOpType use different fields in this struct.
type SQLBindOpDetail struct {
	NormdOrigSQL string
	BindSQL      string
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
	physicalop.SimpleSchemaProducer

	Statement ast.StmtNode

	// IsFromRemote indicates whether the statement IS FROM REMOTE TiDB instance in cluster,
	//   and executing in co-processor.
	//   Used for `global kill`. See https://github.com/pingcap/tidb/blob/master/docs/design/2020-06-01-global-kill.md.
	IsFromRemote bool

	// StaleTxnStartTS is the StartTS that is used to build a staleness transaction by 'START TRANSACTION READ ONLY' statement.
	StaleTxnStartTS uint64

	ResolveCtx *resolve.Context
}

// PhysicalPlanWrapper is a wrapper to wrap any Plan to a PhysicalPlan.
//
//	Used for simple statements executing in coprocessor.
type PhysicalPlanWrapper struct {
	physicalop.BasePhysicalPlan
	Inner base.Plan
}

// MemoryUsage return the memory usage of PhysicalSimpleWrapper
func (p *PhysicalPlanWrapper) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	// base.Plan doesn't have MemoryUsage method, so we just use a fixed size.
	planMem := int64(256)
	sum = p.BasePhysicalPlan.MemoryUsage() + planMem
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
	ColChoice   ast.ColumnChoice
	ColumnList  []*model.ColumnInfo
	IsPartition bool
}

// AnalyzeColumnsTask is used for analyze columns.
type AnalyzeColumnsTask struct {
	HandleCols       util.HandleCols
	CommonHandleInfo *model.IndexInfo
	ColsInfo         []*model.ColumnInfo
	SkipColsInfo     []*model.ColumnInfo
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
	physicalop.SimpleSchemaProducer

	ColTasks []AnalyzeColumnsTask
	IdxTasks []AnalyzeIndexTask
	Opts     map[ast.AnalyzeOptionType]uint64
	// OptionsMap is used to store the options for each partition.
	OptionsMap map[int64]V2AnalyzeOptions
}

// LoadData represents a loaddata plan.
type LoadData struct {
	physicalop.SimpleSchemaProducer

	FileLocRef  ast.FileLocRefTp
	OnDuplicate ast.OnDuplicateKeyHandlingType
	Path        string
	Format      *string
	Table       *resolve.TableNameW
	Charset     *string
	Columns     []*ast.ColumnName
	FieldsInfo  *ast.FieldsClause
	LinesInfo   *ast.LinesClause
	IgnoreLines *uint64

	ColumnAssignments  []*ast.Assignment
	ColumnsAndUserVars []*ast.ColumnNameOrUserVar
	Options            []*LoadDataOpt

	GenCols physicalop.InsertGeneratedColumns
}

// LoadDataOpt represents load data option.
type LoadDataOpt struct {
	// Name is the name of the option, converted to lower case during parse.
	Name  string
	Value expression.Expression
}

// ImportInto represents a ingest into plan.
type ImportInto struct {
	physicalop.SimpleSchemaProducer

	Table              *resolve.TableNameW
	ColumnAssignments  []*ast.Assignment
	ColumnsAndUserVars []*ast.ColumnNameOrUserVar
	Path               string
	Format             *string
	Options            []*LoadDataOpt

	GenCols physicalop.InsertGeneratedColumns
	Stmt    string

	SelectPlan base.PhysicalPlan
}

// LoadStats represents a load stats plan.
type LoadStats struct {
	physicalop.SimpleSchemaProducer

	Path string
}

// LockStats represents a lock stats for table
type LockStats struct {
	physicalop.SimpleSchemaProducer

	Tables []*ast.TableName
}

// UnlockStats represents a unlock stats for table
type UnlockStats struct {
	physicalop.SimpleSchemaProducer

	Tables []*ast.TableName
}

// PlanReplayer represents a plan replayer plan.
type PlanReplayer struct {
	physicalop.SimpleSchemaProducer
	ExecStmt          ast.StmtNode
	StmtList          []string // For PLAN REPLAYER DUMP EXPLAIN ( "sql1", "sql2", ... )
	Analyze           bool
	Load              bool
	File              string
	HistoricalStatsTS uint64

	Capture    bool
	Remove     bool
	SQLDigest  string
	PlanDigest string
}

// Traffic represents a traffic plan.
type Traffic struct {
	physicalop.SimpleSchemaProducer
	OpType  ast.TrafficOpType
	Options []*ast.TrafficOption
	Dir     string
}

// DistributeTable represents a distribute table plan.
type DistributeTable struct {
	physicalop.SimpleSchemaProducer
	TableInfo      *model.TableInfo
	PartitionNames []ast.CIStr
	Engine         string
	Rule           string
	Timeout        string
}

// SplitRegion represents a split regions plan.
type SplitRegion struct {
	physicalop.SimpleSchemaProducer

	TableInfo      *model.TableInfo
	PartitionNames []ast.CIStr
	IndexInfo      *model.IndexInfo
	Lower          []types.Datum
	Upper          []types.Datum
	Num            int
	ValueLists     [][]types.Datum
}

// SplitRegionStatus represents a split regions status plan.
type SplitRegionStatus struct {
	physicalop.SimpleSchemaProducer

	Table     table.Table
	IndexInfo *model.IndexInfo
}

// CompactTable represents a "ALTER TABLE [NAME] COMPACT ..." plan.
type CompactTable struct {
	physicalop.SimpleSchemaProducer

	ReplicaKind    ast.CompactReplicaKind
	TableInfo      *model.TableInfo
	PartitionNames []ast.CIStr
}

// DDL represents a DDL statement plan.
type DDL struct {
	physicalop.SimpleSchemaProducer

	Statement ast.DDLNode
}

// SelectInto represents a select-into plan.
type SelectInto struct {
	physicalop.SimpleSchemaProducer

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


// IsPointGetWithPKOrUniqueKeyByAutoCommit returns true when meets following conditions:
//  1. ctx is auto commit tagged
//  2. session is not InTxn
//  3. plan is point get by pk, or point get by unique index (no double read)
func IsPointGetWithPKOrUniqueKeyByAutoCommit(vars *variable.SessionVars, p base.Plan) bool {
	if !IsAutoCommitTxn(vars) {
		return false
	}

	// check plan
	if proj, ok := p.(*physicalop.PhysicalProjection); ok {
		p = proj.Children()[0]
	}

	switch v := p.(type) {
	case *physicalop.PhysicalIndexReader:
		indexScan := v.IndexPlans[0].(*physicalop.PhysicalIndexScan)
		return indexScan.IsPointGetByUniqueKey(vars.StmtCtx.TypeCtx())
	case *physicalop.PhysicalTableReader:
		tableScan, ok := v.TablePlans[0].(*physicalop.PhysicalTableScan)
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
	case *physicalop.PointGetPlan:
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
	physicalop.SimpleSchemaProducer
}
