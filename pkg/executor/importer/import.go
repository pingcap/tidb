// Copyright 2023 PingCAP, Inc.
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

package importer

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	tidb "github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/disttask/framework/handle"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler"
	"github.com/pingcap/tidb/pkg/expression"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/config"
	litlog "github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	pformat "github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/planctx"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/cpu"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/filter"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/naming"
	sem "github.com/pingcap/tidb/pkg/util/sem/compat"
	"github.com/pingcap/tidb/pkg/util/stringutil"
	pd "github.com/tikv/pd/client"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	// DataFormatCSV represents the data source file of IMPORT INTO is csv.
	DataFormatCSV = "csv"
	// DataFormatDelimitedData delimited data.
	DataFormatDelimitedData = "delimited data"
	// DataFormatSQL represents the data source file of IMPORT INTO is mydumper-format DML file.
	DataFormatSQL = "sql"
	// DataFormatParquet represents the data source file of IMPORT INTO is parquet.
	DataFormatParquet = "parquet"
	// DataFormatAuto represents format is not set in IMPORT INTO, we will determine format automatically.
	DataFormatAuto = "auto"

	// DefaultDiskQuota is the default disk quota for IMPORT INTO
	DefaultDiskQuota = config.ByteSize(50 << 30) // 50GiB

	// 0 means no limit
	unlimitedWriteSpeed = config.ByteSize(0)

	characterSetOption        = "character_set"
	fieldsTerminatedByOption  = "fields_terminated_by"
	fieldsEnclosedByOption    = "fields_enclosed_by"
	fieldsEscapedByOption     = "fields_escaped_by"
	fieldsDefinedNullByOption = "fields_defined_null_by"
	linesTerminatedByOption   = "lines_terminated_by"
	skipRowsOption            = "skip_rows"
	groupKeyOption            = "group_key"
	splitFileOption           = "split_file"
	diskQuotaOption           = "disk_quota"
	threadOption              = "thread"
	maxWriteSpeedOption       = "max_write_speed"
	checksumTableOption       = "checksum_table"
	recordErrorsOption        = "record_errors"
	detachedOption            = "detached"
	// if 'import mode' enabled, TiKV will:
	//  - set level0_stop_writes_trigger = max(old, 1 << 30)
	//  - set level0_slowdown_writes_trigger = max(old, 1 << 30)
	//  - set soft_pending_compaction_bytes_limit = 0,
	//  - set hard_pending_compaction_bytes_limit = 0,
	//  - will not trigger flow control when SST count in L0 is large
	//  - will not trigger region split, it might cause some region became
	//    very large and be a hotspot, might cause latency spike.
	//
	// default false for local sort, true for global sort.
	disableTiKVImportModeOption = "disable_tikv_import_mode"
	cloudStorageURIOption       = ast.CloudStorageURI
	disablePrecheckOption       = "disable_precheck"
	// used for test
	maxEngineSizeOption  = "__max_engine_size"
	forceMergeStep       = "__force_merge_step"
	manualRecoveryOption = "__manual_recovery"
)

var (
	// all supported options.
	// name -> whether the option has value
	supportedOptions = map[string]bool{
		characterSetOption:          true,
		fieldsTerminatedByOption:    true,
		fieldsEnclosedByOption:      true,
		fieldsEscapedByOption:       true,
		fieldsDefinedNullByOption:   true,
		linesTerminatedByOption:     true,
		skipRowsOption:              true,
		groupKeyOption:              true,
		splitFileOption:             false,
		diskQuotaOption:             true,
		threadOption:                true,
		maxWriteSpeedOption:         true,
		checksumTableOption:         true,
		recordErrorsOption:          true,
		detachedOption:              false,
		disableTiKVImportModeOption: false,
		maxEngineSizeOption:         true,
		forceMergeStep:              false,
		manualRecoveryOption:        false,
		cloudStorageURIOption:       true,
		disablePrecheckOption:       false,
	}

	csvOnlyOptions = map[string]struct{}{
		characterSetOption:        {},
		fieldsTerminatedByOption:  {},
		fieldsEnclosedByOption:    {},
		fieldsEscapedByOption:     {},
		fieldsDefinedNullByOption: {},
		linesTerminatedByOption:   {},
		skipRowsOption:            {},
		splitFileOption:           {},
	}

	// we only support global sort on nextgen cluster when SEM enabled, and doesn't
	// allow set separate cloud storage URI.
	disallowedOptionsOfNextGen = map[string]struct{}{
		diskQuotaOption:       {},
		maxWriteSpeedOption:   {},
		cloudStorageURIOption: {},
		threadOption:          {},
		checksumTableOption:   {},
		recordErrorsOption:    {},
	}

	disallowedOptionsForSEM = map[string]struct{}{
		maxEngineSizeOption:  {},
		forceMergeStep:       {},
		manualRecoveryOption: {},
	}

	allowedOptionsOfImportFromQuery = map[string]struct{}{
		threadOption:          {},
		disablePrecheckOption: {},
	}

	// LoadDataReadBlockSize is exposed for test.
	LoadDataReadBlockSize = int64(config.ReadBlockSize)

	supportedSuffixForServerDisk = []string{
		".csv", ".sql", ".parquet",
		".gz", ".gzip",
		".zstd", ".zst",
		".snappy",
	}

	// default character set
	defaultCharacterSet = "utf8mb4"
	// default field null def
	defaultFieldNullDef = []string{`\N`}
)

// DataSourceType indicates the data source type of IMPORT INTO.
type DataSourceType string

const (
	// DataSourceTypeFile represents the data source of IMPORT INTO is file.
	// exported for test.
	DataSourceTypeFile DataSourceType = "file"
	// DataSourceTypeQuery represents the data source of IMPORT INTO is query.
	DataSourceTypeQuery DataSourceType = "query"
)

func (t DataSourceType) String() string {
	return string(t)
}

var (
	// NewClientWithContext returns a kv.Client.
	NewClientWithContext = pd.NewClientWithContext
)

// FieldMapping indicates the relationship between input field and table column or user variable
type FieldMapping struct {
	Column  *table.Column
	UserVar *ast.VariableExpr
}

// LoadDataReaderInfo provides information for a data reader of LOAD DATA.
type LoadDataReaderInfo struct {
	// Opener can be called at needed to get a io.ReadSeekCloser. It will only
	// be called once.
	Opener func(ctx context.Context) (io.ReadSeekCloser, error)
	// Remote is not nil only if load from cloud storage.
	Remote *mydump.SourceFileMeta
}

// Plan describes the plan of LOAD DATA and IMPORT INTO.
type Plan struct {
	DBName string
	DBID   int64
	// TableInfo is the table info we used during import, we might change it
	// if add index by SQL is enabled(it's disabled now).
	TableInfo *model.TableInfo
	// DesiredTableInfo is the table info before import, and the desired table info
	// after import.
	DesiredTableInfo *model.TableInfo

	Path string
	// only effective when data source is file.
	Format string
	// Data interpretation is restrictive if the SQL mode is restrictive and neither
	// the IGNORE nor the LOCAL modifier is specified. Errors terminate the load
	// operation.
	// ref https://dev.mysql.com/doc/refman/8.0/en/load-data.html#load-data-column-assignments
	Restrictive bool

	// Location is used to convert time type for parquet, see
	// https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#timestamp
	Location *time.Location

	SQLMode mysql.SQLMode
	// Charset is the charset of the data file when file is CSV or TSV.
	// it might be nil when using LOAD DATA and no charset is specified.
	// for IMPORT INTO, it is always non-nil and default to be defaultCharacterSet.
	Charset          *string
	ImportantSysVars map[string]string

	// used for LOAD DATA and CSV format of IMPORT INTO
	FieldNullDef []string
	// this is not used in IMPORT INTO
	NullValueOptEnclosed bool
	// LinesStartingBy is not used in IMPORT INTO
	// FieldsOptEnclosed is not used in either IMPORT INTO or LOAD DATA
	plannercore.LineFieldsInfo
	IgnoreLines uint64

	DiskQuota             config.ByteSize
	Checksum              config.PostOpLevel
	ThreadCnt             int
	MaxNodeCnt            int
	MaxWriteSpeed         config.ByteSize
	SplitFile             bool
	MaxRecordedErrors     int64
	Detached              bool
	DisableTiKVImportMode bool
	MaxEngineSize         config.ByteSize
	CloudStorageURI       string
	DisablePrecheck       bool
	GroupKey              string

	// used for checksum in physical mode
	DistSQLScanConcurrency int

	// todo: remove it when load data code is reverted.
	InImportInto   bool
	DataSourceType DataSourceType
	// only initialized for IMPORT INTO, used when creating job.
	Parameters *ImportParameters `json:"-"`
	// only initialized for IMPORT INTO, used when format is detected automatically
	specifiedOptions map[string]*plannercore.LoadDataOpt
	// the user who executes the statement, in the form of user@host
	// only initialized for IMPORT INTO
	User string `json:"-"`

	IsRaftKV2 bool
	// total data file size in bytes.
	TotalFileSize int64
	// used in tests to force enable merge-step when using global sort.
	ForceMergeStep bool
	// see ManualRecovery in proto.ExtraParams
	ManualRecovery bool
	// the keyspace name when submitting this job, only for import-into
	Keyspace string
}

// ASTArgs is the arguments for ast.LoadDataStmt.
// TODO: remove this struct and use the struct which can be serialized.
type ASTArgs struct {
	FileLocRef         ast.FileLocRefTp
	ColumnsAndUserVars []*ast.ColumnNameOrUserVar
	ColumnAssignments  []*ast.Assignment
	OnDuplicate        ast.OnDuplicateKeyHandlingType
	FieldsInfo         *ast.FieldsClause
	LinesInfo          *ast.LinesClause
}

// StepSummary records the number of data involved in each step.
// The data stored might be inaccurate, such as the number of rows in encode step.
type StepSummary struct {
	Bytes  int64 `json:"input-bytes,omitempty"`
	RowCnt int64 `json:"input-rows,omitempty"`
}

// Summary records the amount of data needed to be processed in each step of the import job.
// And this information will be saved into tidb_import_jobs table after the job is finished.
type Summary struct {
	// EncodeSummary stores the bytes and rows needed to be processed in encode step.
	// Same for other summaries.
	EncodeSummary StepSummary `json:"encode-summary,omitempty"`

	MergeSummary StepSummary `json:"merge-summary,omitempty"`

	IngestSummary StepSummary `json:"ingest-summary,omitempty"`

	// ImportedRows is the number of rows imported into TiKV.
	// conflicted rows are excluded from this count if using global-sort.
	ImportedRows   int64  `json:"row-count,omitempty"`
	ConflictRowCnt uint64 `json:"conflict-row-count,omitempty"`
	// TooManyConflicts indicates there are too many conflicted rows that we
	// cannot deduplicate during collecting its checksum, so we will skip later
	// checksum step.
	TooManyConflicts bool `json:"too-many-conflicts,omitempty"`
}

// LoadDataController load data controller.
// todo: need a better name
type LoadDataController struct {
	*Plan
	*ASTArgs

	// used for sync column assignment expression generation.
	colAssignMu sync.Mutex

	Table table.Table

	// how input field(or input column) from data file is mapped, either to a column or variable.
	// if there's NO column list clause in SQL statement, then it's table's columns
	// else it's user defined list.
	FieldMappings []*FieldMapping
	// InsertColumns the columns stated in the SQL statement to insert.
	// as IMPORT INTO have 2 place to state columns, in column-vars and in set clause,
	// so it's computed from both clauses:
	//  - append columns from column-vars to InsertColumns
	//  - append columns from left hand of set clause to InsertColumns
	// it's similar to InsertValues.InsertColumns.
	// Note: our behavior is different with mysql. such as for table t(a,b)
	// - "...(a,a) set a=100" is allowed in mysql, but not in tidb
	// - "...(a,b) set b=100" will set b=100 in mysql, but in tidb the set is ignored.
	// - ref columns in set clause is allowed in mysql, but not in tidb
	InsertColumns []*table.Column

	logger    *zap.Logger
	dataStore storage.ExternalStorage
	dataFiles []*mydump.SourceFileMeta
	// globalSortStore is used to store sorted data when using global sort.
	globalSortStore storage.ExternalStorage
	// ExecuteNodesCnt is the count of execute nodes.
	ExecuteNodesCnt int
}

func getImportantSysVars(sctx sessionctx.Context) map[string]string {
	res := map[string]string{}
	for k, defVal := range common.DefaultImportantVariables {
		if val, ok := sctx.GetSessionVars().GetSystemVar(k); ok {
			res[k] = val
		} else {
			res[k] = defVal
		}
	}
	for k, defVal := range common.DefaultImportVariablesTiDB {
		if val, ok := sctx.GetSessionVars().GetSystemVar(k); ok {
			res[k] = val
		} else {
			res[k] = defVal
		}
	}
	return res
}

// NewPlanFromLoadDataPlan creates a import plan from LOAD DATA.
func NewPlanFromLoadDataPlan(userSctx sessionctx.Context, plan *plannercore.LoadData) (*Plan, error) {
	fullTableName := common.UniqueTable(plan.Table.Schema.L, plan.Table.Name.L)
	logger := log.L().With(zap.String("table", fullTableName))
	charset := plan.Charset
	if charset == nil {
		// https://dev.mysql.com/doc/refman/8.0/en/load-data.html#load-data-character-set
		d, err2 := userSctx.GetSessionVars().GetSessionOrGlobalSystemVar(
			context.Background(), vardef.CharsetDatabase)
		if err2 != nil {
			logger.Error("LOAD DATA get charset failed", zap.Error(err2))
		} else {
			charset = &d
		}
	}
	restrictive := userSctx.GetSessionVars().SQLMode.HasStrictMode() &&
		plan.OnDuplicate != ast.OnDuplicateKeyHandlingIgnore

	var ignoreLines uint64
	if plan.IgnoreLines != nil {
		ignoreLines = *plan.IgnoreLines
	}

	var (
		nullDef              []string
		nullValueOptEnclosed = false
	)

	lineFieldsInfo := plannercore.NewLineFieldsInfo(plan.FieldsInfo, plan.LinesInfo)
	// todo: move null defined into plannercore.LineFieldsInfo
	// in load data, there maybe multiple null def, but in SELECT ... INTO OUTFILE there's only one
	if plan.FieldsInfo != nil && plan.FieldsInfo.DefinedNullBy != nil {
		nullDef = append(nullDef, *plan.FieldsInfo.DefinedNullBy)
		nullValueOptEnclosed = plan.FieldsInfo.NullValueOptEnclosed
	} else if len(lineFieldsInfo.FieldsEnclosedBy) != 0 {
		nullDef = append(nullDef, "NULL")
	}
	if len(lineFieldsInfo.FieldsEscapedBy) != 0 {
		nullDef = append(nullDef, string([]byte{lineFieldsInfo.FieldsEscapedBy[0], 'N'}))
	}

	return &Plan{
		DBName: plan.Table.Schema.L,
		DBID:   plan.Table.DBInfo.ID,

		Path:                 plan.Path,
		Format:               DataFormatDelimitedData,
		Restrictive:          restrictive,
		FieldNullDef:         nullDef,
		NullValueOptEnclosed: nullValueOptEnclosed,
		LineFieldsInfo:       lineFieldsInfo,
		IgnoreLines:          ignoreLines,

		SQLMode:          userSctx.GetSessionVars().SQLMode,
		Charset:          charset,
		ImportantSysVars: getImportantSysVars(userSctx),

		DistSQLScanConcurrency: userSctx.GetSessionVars().DistSQLScanConcurrency(),
		DataSourceType:         DataSourceTypeFile,
	}, nil
}

// NewImportPlan creates a new import into plan.
func NewImportPlan(ctx context.Context, userSctx sessionctx.Context, plan *plannercore.ImportInto, tbl table.Table) (*Plan, error) {
	failpoint.InjectCall("NewImportPlan", plan)
	var format string
	if plan.Format != nil {
		format = strings.ToLower(*plan.Format)
	} else {
		format = DataFormatAuto
	}
	restrictive := userSctx.GetSessionVars().SQLMode.HasStrictMode()
	lineFieldsInfo := newDefaultLineFieldsInfo()

	p := &Plan{
		TableInfo:        tbl.Meta(),
		DesiredTableInfo: tbl.Meta(),
		DBName:           plan.Table.Schema.L,
		DBID:             plan.Table.DBInfo.ID,

		Path:           plan.Path,
		Format:         format,
		Restrictive:    restrictive,
		FieldNullDef:   defaultFieldNullDef,
		LineFieldsInfo: lineFieldsInfo,

		Location:         userSctx.GetSessionVars().Location(),
		SQLMode:          userSctx.GetSessionVars().SQLMode,
		ImportantSysVars: getImportantSysVars(userSctx),

		DistSQLScanConcurrency: userSctx.GetSessionVars().DistSQLScanConcurrency(),
		InImportInto:           true,
		DataSourceType:         getDataSourceType(plan),
		User:                   userSctx.GetSessionVars().User.String(),
		Keyspace:               userSctx.GetStore().GetKeyspace(),
	}
	if err := p.initOptions(ctx, userSctx, plan.Options); err != nil {
		return nil, err
	}
	if err := p.initParameters(plan); err != nil {
		return nil, err
	}
	return p, nil
}

func newDefaultLineFieldsInfo() plannercore.LineFieldsInfo {
	// those are the default values for lightning CSV format too
	return plannercore.LineFieldsInfo{
		FieldsTerminatedBy: `,`,
		FieldsEnclosedBy:   `"`,
		FieldsEscapedBy:    `\`,
		LinesStartingBy:    ``,
		// csv_parser will determine it automatically(either '\r' or '\n' or '\r\n')
		// But user cannot set this to empty explicitly.
		LinesTerminatedBy: ``,
	}
}

// ASTArgsFromPlan creates ASTArgs from plan.
func ASTArgsFromPlan(plan *plannercore.LoadData) *ASTArgs {
	return &ASTArgs{
		FileLocRef:         plan.FileLocRef,
		ColumnsAndUserVars: plan.ColumnsAndUserVars,
		ColumnAssignments:  plan.ColumnAssignments,
		OnDuplicate:        plan.OnDuplicate,
		FieldsInfo:         plan.FieldsInfo,
		LinesInfo:          plan.LinesInfo,
	}
}

// ASTArgsFromImportPlan creates ASTArgs from plan.
func ASTArgsFromImportPlan(plan *plannercore.ImportInto) *ASTArgs {
	// FileLocRef are not used in ImportIntoStmt, OnDuplicate not used now.
	return &ASTArgs{
		FileLocRef:         ast.FileLocServerOrRemote,
		ColumnsAndUserVars: plan.ColumnsAndUserVars,
		ColumnAssignments:  plan.ColumnAssignments,
		OnDuplicate:        ast.OnDuplicateKeyHandlingReplace,
	}
}

// ASTArgsFromStmt creates ASTArgs from statement.
func ASTArgsFromStmt(stmt string) (*ASTArgs, error) {
	stmtNode, err := parser.New().ParseOneStmt(stmt, "", "")
	if err != nil {
		return nil, err
	}
	importIntoStmt, ok := stmtNode.(*ast.ImportIntoStmt)
	if !ok {
		return nil, errors.Errorf("stmt %s is not import into stmt", stmt)
	}
	// FileLocRef are not used in ImportIntoStmt, OnDuplicate not used now.
	return &ASTArgs{
		FileLocRef:         ast.FileLocServerOrRemote,
		ColumnsAndUserVars: importIntoStmt.ColumnsAndUserVars,
		ColumnAssignments:  importIntoStmt.ColumnAssignments,
		OnDuplicate:        ast.OnDuplicateKeyHandlingReplace,
	}, nil
}

// Option is used to set optional parameters for LoadDataController.
type Option func(c *LoadDataController)

// WithLogger sets the logger for LoadDataController.
func WithLogger(logger *zap.Logger) Option {
	return func(c *LoadDataController) {
		c.logger = logger
	}
}

// NewLoadDataController create new controller.
func NewLoadDataController(plan *Plan, tbl table.Table, astArgs *ASTArgs, options ...Option) (*LoadDataController, error) {
	fullTableName := tbl.Meta().Name.String()
	logger := log.L().With(zap.String("table", fullTableName))
	c := &LoadDataController{
		Plan:            plan,
		ASTArgs:         astArgs,
		Table:           tbl,
		logger:          logger,
		ExecuteNodesCnt: 1,
	}
	for _, opt := range options {
		opt(c)
	}

	if err := c.checkFieldParams(); err != nil {
		return nil, err
	}

	columnNames := c.initFieldMappings()
	if err := c.initLoadColumns(columnNames); err != nil {
		return nil, err
	}
	return c, nil
}

// InitTiKVConfigs initializes some TiKV related configs.
func (e *LoadDataController) InitTiKVConfigs(ctx context.Context, sctx sessionctx.Context) error {
	isRaftKV2, err := util.IsRaftKv2(ctx, sctx)
	if err != nil {
		return err
	}
	e.Plan.IsRaftKV2 = isRaftKV2
	return nil
}

func (e *LoadDataController) checkFieldParams() error {
	if e.DataSourceType == DataSourceTypeFile && e.Path == "" {
		return exeerrors.ErrLoadDataEmptyPath
	}
	if e.InImportInto {
		if e.Format != DataFormatCSV && e.Format != DataFormatParquet && e.Format != DataFormatSQL && e.Format != DataFormatAuto {
			return exeerrors.ErrLoadDataUnsupportedFormat.GenWithStackByArgs(e.Format)
		}
	} else {
		if e.NullValueOptEnclosed && len(e.FieldsEnclosedBy) == 0 {
			return exeerrors.ErrLoadDataWrongFormatConfig.GenWithStackByArgs("must specify FIELDS [OPTIONALLY] ENCLOSED BY when use NULL DEFINED BY OPTIONALLY ENCLOSED")
		}
		// NOTE: IMPORT INTO also don't support user set empty LinesTerminatedBy or FieldsTerminatedBy,
		// but it's check in initOptions.
		if len(e.LinesTerminatedBy) == 0 {
			return exeerrors.ErrLoadDataWrongFormatConfig.GenWithStackByArgs("LINES TERMINATED BY is empty")
		}
		// see https://github.com/pingcap/tidb/issues/33298
		if len(e.FieldsTerminatedBy) == 0 {
			return exeerrors.ErrLoadDataWrongFormatConfig.GenWithStackByArgs("load data with empty field terminator")
		}
	}
	if len(e.FieldsEnclosedBy) > 0 &&
		(strings.HasPrefix(e.FieldsEnclosedBy, e.FieldsTerminatedBy) || strings.HasPrefix(e.FieldsTerminatedBy, e.FieldsEnclosedBy)) {
		return exeerrors.ErrLoadDataWrongFormatConfig.GenWithStackByArgs("FIELDS ENCLOSED BY and TERMINATED BY must not be prefix of each other")
	}

	return nil
}

func (p *Plan) initDefaultOptions(ctx context.Context, targetNodeCPUCnt int, store tidbkv.Storage) {
	var threadCnt int
	threadCnt = int(math.Max(1, float64(targetNodeCPUCnt)*0.5))
	if p.DataSourceType == DataSourceTypeQuery {
		threadCnt = 2
	}
	p.Checksum = config.OpLevelRequired
	p.ThreadCnt = threadCnt
	p.MaxWriteSpeed = unlimitedWriteSpeed
	p.SplitFile = false
	p.MaxRecordedErrors = 100
	p.Detached = false
	p.DisableTiKVImportMode = false
	p.MaxEngineSize = getDefMaxEngineSize()
	p.CloudStorageURI = handle.GetCloudStorageURI(ctx, store)

	v := defaultCharacterSet
	p.Charset = &v
}

func getDefMaxEngineSize() config.ByteSize {
	if kerneltype.IsNextGen() {
		return config.DefaultBatchSize
	}
	return config.ByteSize(defaultMaxEngineSize)
}

func (p *Plan) initOptions(ctx context.Context, seCtx sessionctx.Context, options []*plannercore.LoadDataOpt) error {
	targetNodeCPUCnt, err := GetTargetNodeCPUCnt(ctx, p.DataSourceType, p.Path)
	if err != nil {
		return err
	}
	p.initDefaultOptions(ctx, targetNodeCPUCnt, seCtx.GetStore())

	specifiedOptions := map[string]*plannercore.LoadDataOpt{}
	for _, opt := range options {
		hasValue, ok := supportedOptions[opt.Name]
		if !ok {
			return exeerrors.ErrUnknownOption.FastGenByArgs(opt.Name)
		}
		if hasValue && opt.Value == nil || !hasValue && opt.Value != nil {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		if _, ok = specifiedOptions[opt.Name]; ok {
			return exeerrors.ErrDuplicateOption.FastGenByArgs(opt.Name)
		}
		specifiedOptions[opt.Name] = opt
	}
	p.specifiedOptions = specifiedOptions

	if kerneltype.IsNextGen() && sem.IsEnabled() {
		if p.DataSourceType == DataSourceTypeQuery {
			return plannererrors.ErrNotSupportedWithSem.GenWithStackByArgs("IMPORT INTO from select")
		}
		// we put the check here, not in planner, to make sure the cloud_storage_uri
		// won't change in between.
		if p.IsLocalSort() {
			return plannererrors.ErrNotSupportedWithSem.GenWithStackByArgs("IMPORT INTO with local sort")
		}
		for k := range disallowedOptionsOfNextGen {
			if _, ok := specifiedOptions[k]; ok {
				return exeerrors.ErrLoadDataUnsupportedOption.GenWithStackByArgs(k, "nextgen kernel")
			}
		}
	}

	if sem.IsEnabled() {
		for k := range disallowedOptionsForSEM {
			if _, ok := specifiedOptions[k]; ok {
				return exeerrors.ErrLoadDataUnsupportedOption.GenWithStackByArgs(k, "SEM enabled")
			}
		}
	}

	// DataFormatAuto means format is unspecified from stmt,
	// will validate below CSV options when init data files.
	if p.Format != DataFormatCSV && p.Format != DataFormatAuto {
		for k := range csvOnlyOptions {
			if _, ok := specifiedOptions[k]; ok {
				return exeerrors.ErrLoadDataUnsupportedOption.FastGenByArgs(k, "non-CSV format")
			}
		}
	}
	if p.DataSourceType == DataSourceTypeQuery {
		for k := range specifiedOptions {
			if _, ok := allowedOptionsOfImportFromQuery[k]; !ok {
				return exeerrors.ErrLoadDataUnsupportedOption.FastGenByArgs(k, "import from query")
			}
		}
	}

	optAsString := func(opt *plannercore.LoadDataOpt) (string, error) {
		if opt.Value.GetType(seCtx.GetExprCtx().GetEvalCtx()).GetType() != mysql.TypeVarString {
			return "", exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		val, isNull, err2 := opt.Value.EvalString(seCtx.GetExprCtx().GetEvalCtx(), chunk.Row{})
		if err2 != nil || isNull {
			return "", exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		return val, nil
	}
	optAsInt64 := func(opt *plannercore.LoadDataOpt) (int64, error) {
		// current parser takes integer and bool as mysql.TypeLonglong
		if opt.Value.GetType(seCtx.GetExprCtx().GetEvalCtx()).GetType() != mysql.TypeLonglong || mysql.HasIsBooleanFlag(opt.Value.GetType(seCtx.GetExprCtx().GetEvalCtx()).GetFlag()) {
			return 0, exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		val, isNull, err2 := opt.Value.EvalInt(seCtx.GetExprCtx().GetEvalCtx(), chunk.Row{})
		if err2 != nil || isNull {
			return 0, exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		return val, nil
	}
	if opt, ok := specifiedOptions[characterSetOption]; ok {
		v, err := optAsString(opt)
		if err != nil || v == "" {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		_, err = config.ParseCharset(v)
		if err != nil {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		p.Charset = &v
	}
	if opt, ok := specifiedOptions[fieldsTerminatedByOption]; ok {
		v, err := optAsString(opt)
		if err != nil || v == "" {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		p.FieldsTerminatedBy = v
	}
	if opt, ok := specifiedOptions[fieldsEnclosedByOption]; ok {
		v, err := optAsString(opt)
		if err != nil || len(v) > 1 {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		p.FieldsEnclosedBy = v
	}
	if opt, ok := specifiedOptions[fieldsEscapedByOption]; ok {
		v, err := optAsString(opt)
		if err != nil || len(v) > 1 {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		p.FieldsEscapedBy = v
	}
	if opt, ok := specifiedOptions[fieldsDefinedNullByOption]; ok {
		v, err := optAsString(opt)
		if err != nil {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		p.FieldNullDef = []string{v}
	}
	if opt, ok := specifiedOptions[linesTerminatedByOption]; ok {
		v, err := optAsString(opt)
		// cannot set terminator to empty string explicitly
		if err != nil || v == "" {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		p.LinesTerminatedBy = v
	}
	if opt, ok := specifiedOptions[skipRowsOption]; ok {
		vInt, err := optAsInt64(opt)
		if err != nil || vInt < 0 {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		p.IgnoreLines = uint64(vInt)
	}
	if _, ok := specifiedOptions[splitFileOption]; ok {
		p.SplitFile = true
	}
	if opt, ok := specifiedOptions[diskQuotaOption]; ok {
		v, err := optAsString(opt)
		if err != nil {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		if err = p.DiskQuota.UnmarshalText([]byte(v)); err != nil || p.DiskQuota <= 0 {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
	}
	if opt, ok := specifiedOptions[threadOption]; ok {
		vInt, err := optAsInt64(opt)
		if err != nil || vInt <= 0 {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		p.ThreadCnt = int(vInt)
	}
	if opt, ok := specifiedOptions[maxWriteSpeedOption]; ok {
		v, err := optAsString(opt)
		if err != nil {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		if err = p.MaxWriteSpeed.UnmarshalText([]byte(v)); err != nil || p.MaxWriteSpeed < 0 {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
	}
	if opt, ok := specifiedOptions[checksumTableOption]; ok {
		v, err := optAsString(opt)
		if err != nil {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		if err = p.Checksum.FromStringValue(v); err != nil {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
	}
	if opt, ok := specifiedOptions[groupKeyOption]; ok {
		v, err := optAsString(opt)
		if err != nil || v == "" || naming.CheckWithMaxLen(v, 256) != nil {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		p.GroupKey = v
	}
	if opt, ok := specifiedOptions[recordErrorsOption]; ok {
		vInt, err := optAsInt64(opt)
		if err != nil || vInt < -1 {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		p.MaxRecordedErrors = vInt
	}
	if _, ok := specifiedOptions[detachedOption]; ok {
		p.Detached = true
	}
	if _, ok := specifiedOptions[disableTiKVImportModeOption]; ok {
		p.DisableTiKVImportMode = true
	}
	if opt, ok := specifiedOptions[cloudStorageURIOption]; ok {
		v, err := optAsString(opt)
		if err != nil {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		// set cloud storage uri to empty string to force uses local sort when
		// the global variable is set.
		if v != "" {
			b, err := storage.ParseBackend(v, nil)
			if err != nil {
				return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
			}
			// only support s3 and gcs now.
			if b.GetS3() == nil && b.GetGcs() == nil {
				return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
			}
		}
		p.CloudStorageURI = v
	}
	if opt, ok := specifiedOptions[maxEngineSizeOption]; ok {
		v, err := optAsString(opt)
		if err != nil {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		if err = p.MaxEngineSize.UnmarshalText([]byte(v)); err != nil || p.MaxEngineSize < 0 {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
	}
	if _, ok := specifiedOptions[disablePrecheckOption]; ok {
		p.DisablePrecheck = true
	}
	if _, ok := specifiedOptions[forceMergeStep]; ok {
		p.ForceMergeStep = true
	}
	if _, ok := specifiedOptions[manualRecoveryOption]; ok {
		p.ManualRecovery = true
	}

	if kerneltype.IsClassic() {
		if sv, ok := seCtx.GetSessionVars().GetSystemVar(vardef.TiDBMaxDistTaskNodes); ok {
			p.MaxNodeCnt = variable.TidbOptInt(sv, 0)
			if p.MaxNodeCnt == -1 { // -1 means calculate automatically
				p.MaxNodeCnt = scheduler.CalcMaxNodeCountByStoresNum(ctx, seCtx.GetStore())
			}
		}
	}

	// when split-file is set, data file will be split into chunks of 256 MiB.
	// skip_rows should be 0 or 1, we add this restriction to simplify skip_rows
	// logic, so we only need to skip on the first chunk for each data file.
	// CSV parser limit each row size to LargestEntryLimit(120M), the first row
	// will NOT cross file chunk.
	if p.SplitFile && p.IgnoreLines > 1 {
		return exeerrors.ErrInvalidOptionVal.FastGenByArgs("skip_rows, should be <= 1 when split-file is enabled")
	}

	if p.SplitFile && len(p.LinesTerminatedBy) == 0 {
		return exeerrors.ErrInvalidOptionVal.FastGenByArgs("lines_terminated_by, should not be empty when use split_file")
	}

	p.adjustOptions(targetNodeCPUCnt)
	return nil
}

func (p *Plan) adjustOptions(targetNodeCPUCnt int) {
	limit := targetNodeCPUCnt
	if p.DataSourceType == DataSourceTypeQuery {
		// for query, row is produced using 1 thread, the max cpu used is much
		// lower than import from file, so we set limit to 2*targetNodeCPUCnt.
		// TODO: adjust after spec is ready.
		limit *= 2
	}
	// max value is cpu-count
	if p.ThreadCnt > limit {
		log.L().Info("adjust IMPORT INTO thread count",
			zap.Int("before", p.ThreadCnt), zap.Int("after", limit))
		p.ThreadCnt = limit
	}
	if p.IsGlobalSort() {
		p.DisableTiKVImportMode = true
	}
}

func (p *Plan) initParameters(plan *plannercore.ImportInto) error {
	redactURL := ast.RedactURL(p.Path)
	var columnsAndVars, setClause string
	var sb strings.Builder
	formatCtx := pformat.NewRestoreCtx(pformat.DefaultRestoreFlags, &sb)
	if len(plan.ColumnsAndUserVars) > 0 {
		sb.WriteString("(")
		for i, col := range plan.ColumnsAndUserVars {
			if i > 0 {
				sb.WriteString(", ")
			}
			_ = col.Restore(formatCtx)
		}
		sb.WriteString(")")
		columnsAndVars = sb.String()
	}
	if len(plan.ColumnAssignments) > 0 {
		sb.Reset()
		for i, assign := range plan.ColumnAssignments {
			if i > 0 {
				sb.WriteString(", ")
			}
			_ = assign.Restore(formatCtx)
		}
		setClause = sb.String()
	}
	optionMap := make(map[string]any, len(plan.Options))
	for _, opt := range plan.Options {
		if opt.Value != nil {
			// The option attached to the import statement here are all
			// parameters entered by the user. TiDB will process the
			// parameters entered by the user as constant. so we can
			// directly convert it to constant.
			cons := opt.Value.(*expression.Constant)
			val := fmt.Sprintf("%v", cons.Value.GetValue())
			if opt.Name == cloudStorageURIOption {
				val = ast.RedactURL(val)
			}
			optionMap[opt.Name] = val
		} else {
			optionMap[opt.Name] = nil
		}
	}
	p.Parameters = &ImportParameters{
		ColumnsAndVars: columnsAndVars,
		SetClause:      setClause,
		FileLocation:   redactURL,
		Format:         p.Format,
		Options:        optionMap,
	}
	return nil
}

func (e *LoadDataController) tableVisCols2FieldMappings() ([]*FieldMapping, []string) {
	tableCols := e.Table.VisibleCols()
	mappings := make([]*FieldMapping, 0, len(tableCols))
	names := make([]string, 0, len(tableCols))
	for _, v := range tableCols {
		// Data for generated column is generated from the other rows rather than from the parsed data.
		fieldMapping := &FieldMapping{
			Column: v,
		}
		mappings = append(mappings, fieldMapping)
		names = append(names, v.Name.O)
	}
	return mappings, names
}

// initFieldMappings make a field mapping slice to implicitly map input field to table column or user defined variable
// the slice's order is the same as the order of the input fields.
// Returns a slice of same ordered column names without user defined variable names.
func (e *LoadDataController) initFieldMappings() []string {
	columns := make([]string, 0, len(e.ColumnsAndUserVars)+len(e.ColumnAssignments))
	tableCols := e.Table.VisibleCols()

	if len(e.ColumnsAndUserVars) == 0 {
		e.FieldMappings, columns = e.tableVisCols2FieldMappings()

		return columns
	}

	var column *table.Column

	for _, v := range e.ColumnsAndUserVars {
		if v.ColumnName != nil {
			column = table.FindCol(tableCols, v.ColumnName.Name.O)
			columns = append(columns, v.ColumnName.Name.O)
		} else {
			column = nil
		}

		fieldMapping := &FieldMapping{
			Column:  column,
			UserVar: v.UserVar,
		}
		e.FieldMappings = append(e.FieldMappings, fieldMapping)
	}

	return columns
}

// initLoadColumns sets columns which the input fields loaded to.
func (e *LoadDataController) initLoadColumns(columnNames []string) error {
	var cols []*table.Column
	var missingColName string
	var err error
	tableCols := e.Table.VisibleCols()

	if len(columnNames) != len(tableCols) {
		for _, v := range e.ColumnAssignments {
			columnNames = append(columnNames, v.Column.Name.O)
		}
	}

	cols, missingColName = table.FindCols(tableCols, columnNames, e.Table.Meta().PKIsHandle)
	if missingColName != "" {
		return dbterror.ErrBadField.GenWithStackByArgs(missingColName, "field list")
	}

	e.InsertColumns = append(e.InsertColumns, cols...)

	// e.InsertColumns is appended according to the original tables' column sequence.
	// We have to reorder it to follow the use-specified column order which is shown in the columnNames.
	if err = e.reorderColumns(columnNames); err != nil {
		return err
	}

	// Check column whether is specified only once.
	err = table.CheckOnce(cols)
	if err != nil {
		return err
	}

	return nil
}

// reorderColumns reorder the e.InsertColumns according to the order of columnNames
// Note: We must ensure there must be one-to-one mapping between e.InsertColumns and columnNames in terms of column name.
func (e *LoadDataController) reorderColumns(columnNames []string) error {
	cols := e.InsertColumns

	if len(cols) != len(columnNames) {
		return exeerrors.ErrColumnsNotMatched
	}

	reorderedColumns := make([]*table.Column, len(cols))

	if columnNames == nil {
		return nil
	}

	mapping := make(map[string]int)
	for idx, colName := range columnNames {
		mapping[strings.ToLower(colName)] = idx
	}

	for _, col := range cols {
		idx := mapping[col.Name.L]
		reorderedColumns[idx] = col
	}

	e.InsertColumns = reorderedColumns

	return nil
}

// GetFieldCount get field count.
func (e *LoadDataController) GetFieldCount() int {
	return len(e.FieldMappings)
}

// GenerateCSVConfig generates a CSV config for parser from LoadDataWorker.
func (e *LoadDataController) GenerateCSVConfig() *config.CSVConfig {
	csvConfig := &config.CSVConfig{
		FieldsTerminatedBy: e.FieldsTerminatedBy,
		// ignore optionally enclosed
		FieldsEnclosedBy:   e.FieldsEnclosedBy,
		LinesTerminatedBy:  e.LinesTerminatedBy,
		NotNull:            false,
		FieldNullDefinedBy: e.FieldNullDef,
		Header:             false,
		TrimLastEmptyField: false,
		FieldsEscapedBy:    e.FieldsEscapedBy,
		LinesStartingBy:    e.LinesStartingBy,
	}
	if !e.InImportInto {
		// for load data
		csvConfig.AllowEmptyLine = true
		csvConfig.QuotedNullIsText = !e.NullValueOptEnclosed
		csvConfig.UnescapedQuote = true
	}
	return csvConfig
}

// InitDataStore initializes the data store.
func (e *LoadDataController) InitDataStore(ctx context.Context) error {
	u, err2 := storage.ParseRawURL(e.Path)
	if err2 != nil {
		return exeerrors.ErrLoadDataInvalidURI.GenWithStackByArgs(plannercore.ImportIntoDataSource,
			err2.Error())
	}

	if storage.IsLocal(u) {
		u.Path = filepath.Dir(e.Path)
	} else {
		u.Path = ""
	}
	s, err := initExternalStore(ctx, u, plannercore.ImportIntoDataSource)
	if err != nil {
		return err
	}
	e.dataStore = s

	if e.IsGlobalSort() {
		store, err3 := GetSortStore(ctx, e.Plan.CloudStorageURI)
		if err3 != nil {
			return err3
		}
		e.globalSortStore = store
	}
	return nil
}

// Close closes all the resources.
func (e *LoadDataController) Close() {
	if e.dataStore != nil {
		e.dataStore.Close()
	}
	if e.globalSortStore != nil {
		e.globalSortStore.Close()
	}
}

// GetSortStore gets the sort store.
func GetSortStore(ctx context.Context, url string) (storage.ExternalStorage, error) {
	u, err := storage.ParseRawURL(url)
	target := "cloud storage"
	if err != nil {
		return nil, exeerrors.ErrLoadDataInvalidURI.GenWithStackByArgs(target, err.Error())
	}
	return initExternalStore(ctx, u, target)
}

func initExternalStore(ctx context.Context, u *url.URL, target string) (storage.ExternalStorage, error) {
	b, err2 := storage.ParseBackendFromURL(u, nil)
	if err2 != nil {
		return nil, exeerrors.ErrLoadDataInvalidURI.GenWithStackByArgs(target, errors.GetErrStackMsg(err2))
	}

	s, err := storage.NewWithDefaultOpt(ctx, b)
	if err != nil {
		return nil, exeerrors.ErrLoadDataCantAccess.GenWithStackByArgs(target, errors.GetErrStackMsg(err))
	}
	return s, nil
}

// maxSampledCompressedFiles indicates the max number of files we used to sample
// compression ratio for each compression type. Consider the extreme case that
// user data contains all 3 compression types. Then we need to sample about 1,500
// files. Suppose each file costs 0.5 second (for example, cross region access),
// we still can finish in one minute with 16 concurrency.
const maxSampledCompressedFiles = 512

// compressionEstimator estimates compression ratio for different compression types.
// It uses harmonic mean to get the average compression ratio.
type compressionEstimator struct {
	mu      sync.Mutex
	records map[mydump.Compression][]float64
	ratio   sync.Map
}

func newCompressionRecorder() *compressionEstimator {
	return &compressionEstimator{
		records: make(map[mydump.Compression][]float64),
	}
}

func getHarmonicMean(rs []float64) float64 {
	if len(rs) == 0 {
		return 1.0
	}
	var (
		sumInverse float64
		count      int
	)
	for _, r := range rs {
		if r > 0 {
			sumInverse += 1.0 / r
			count++
		}
	}

	if count == 0 {
		return 1.0
	}
	return float64(count) / sumInverse
}

func (r *compressionEstimator) estimate(
	ctx context.Context,
	fileMeta mydump.SourceFileMeta,
	store storage.ExternalStorage,
) float64 {
	compressTp := mydump.ParseCompressionOnFileExtension(fileMeta.Path)
	if compressTp == mydump.CompressionNone {
		return 1.0
	}
	if v, ok := r.ratio.Load(compressTp); ok {
		return v.(float64)
	}

	compressRatio, err := mydump.SampleFileCompressRatio(ctx, fileMeta, store)
	if err != nil {
		logutil.Logger(ctx).Error("fail to calculate data file compress ratio",
			zap.String("category", "loader"),
			zap.String("path", fileMeta.Path),
			zap.Stringer("type", fileMeta.Type), zap.Error(err),
		)
		return 1.0
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.ratio.Load(compressTp); ok {
		return compressRatio
	}

	if r.records[compressTp] == nil {
		r.records[compressTp] = make([]float64, 0, 256)
	}
	if len(r.records[compressTp]) < maxSampledCompressedFiles {
		r.records[compressTp] = append(r.records[compressTp], compressRatio)
	}
	if len(r.records[compressTp]) >= maxSampledCompressedFiles {
		// Using harmonic mean can better handle outlier values.
		compressRatio = getHarmonicMean(r.records[compressTp])
		r.ratio.Store(compressTp, compressRatio)
	}
	return compressRatio
}

// InitDataFiles initializes the data store and files.
// it will call InitDataStore internally.
func (e *LoadDataController) InitDataFiles(ctx context.Context) error {
	u, err2 := storage.ParseRawURL(e.Path)
	if err2 != nil {
		return exeerrors.ErrLoadDataInvalidURI.GenWithStackByArgs(plannercore.ImportIntoDataSource,
			err2.Error())
	}

	var fileNameKey string
	if storage.IsLocal(u) {
		// LOAD DATA don't support server file.
		if !e.InImportInto {
			return exeerrors.ErrLoadDataFromServerDisk.GenWithStackByArgs(e.Path)
		}

		if !filepath.IsAbs(e.Path) {
			return exeerrors.ErrLoadDataInvalidURI.GenWithStackByArgs(plannercore.ImportIntoDataSource,
				"file location should be absolute path when import from server disk")
		}
		// we add this check for security, we don't want user import any sensitive system files,
		// most of which is readable text file and don't have a suffix, such as /etc/passwd
		if !slices.Contains(supportedSuffixForServerDisk, strings.ToLower(filepath.Ext(e.Path))) {
			return exeerrors.ErrLoadDataInvalidURI.GenWithStackByArgs(plannercore.ImportIntoDataSource,
				"the file suffix is not supported when import from server disk")
		}
		dir := filepath.Dir(e.Path)
		_, err := os.Stat(dir)
		if err != nil {
			// permission denied / file not exist error, etc.
			return exeerrors.ErrLoadDataInvalidURI.GenWithStackByArgs(plannercore.ImportIntoDataSource,
				err.Error())
		}

		fileNameKey = filepath.Base(e.Path)
	} else {
		fileNameKey = strings.Trim(u.Path, "/")
	}
	// try to find pattern error in advance
	_, err2 = filepath.Match(stringutil.EscapeGlobQuestionMark(fileNameKey), "")
	if err2 != nil {
		return exeerrors.ErrLoadDataInvalidURI.GenWithStackByArgs(plannercore.ImportIntoDataSource,
			"Glob pattern error: "+err2.Error())
	}

	if err2 = e.InitDataStore(ctx); err2 != nil {
		return err2
	}

	s := e.dataStore
	var (
		totalSize  int64
		sourceType mydump.SourceType
		// sizeExpansionRatio is the estimated size expansion for parquet format.
		// For non-parquet format, it's always 1.0.
		sizeExpansionRatio = 1.0
	)

	checkFirstFile := func(s storage.ExternalStorage, path string) error {
		var (
			err      error
			checkRes *mydump.ParquetPrecheckResult
		)

		e.detectAndUpdateFormat(path)
		sourceType := e.getSourceType()
		if sourceType != mydump.SourceTypeParquet {
			return nil
		}

		checkRes, err = mydump.PrecheckParquet(ctx, s, path)
		if err != nil {
			return err
		}
		sizeExpansionRatio = checkRes.SizeExpansionRatio
		return nil
	}

	dataFiles := []*mydump.SourceFileMeta{}
	isAutoDetectingFormat := e.Format == DataFormatAuto
	// check glob pattern is present in filename.
	idx := strings.IndexAny(fileNameKey, "*[")
	// simple path when the path represent one file
	if idx == -1 {
		fileReader, err2 := s.Open(ctx, fileNameKey, nil)
		if err2 != nil {
			return exeerrors.ErrLoadDataCantRead.GenWithStackByArgs(errors.GetErrStackMsg(err2), "Please check the file location is correct")
		}
		defer func() {
			terror.Log(fileReader.Close())
		}()
		size, err3 := fileReader.Seek(0, io.SeekEnd)
		if err3 != nil {
			return exeerrors.ErrLoadDataCantRead.GenWithStackByArgs(errors.GetErrStackMsg(err3), "failed to read file size by seek")
		}
		if err := checkFirstFile(s, fileNameKey); err != nil {
			return errors.Trace(err)
		}
		compressTp := mydump.ParseCompressionOnFileExtension(fileNameKey)
		fileMeta := mydump.SourceFileMeta{
			Path:        fileNameKey,
			FileSize:    size,
			Compression: compressTp,
			Type:        sourceType,
		}
		fileMeta.RealSize = mydump.EstimateRealSizeForFile(ctx, fileMeta, s)
		fileMeta.RealSize = int64(float64(fileMeta.RealSize) * sizeExpansionRatio)
		dataFiles = append(dataFiles, &fileMeta)
		totalSize = size
	} else {
		var commonPrefix string
		if !storage.IsLocal(u) {
			// for local directory, we're walking the parent directory,
			// so we don't have a common prefix as cloud storage do.
			commonPrefix = fileNameKey[:idx]
		}
		// when import from server disk, all entries in parent directory should have READ
		// access, else walkDir will fail
		// we only support '*', in order to reuse glob library manually escape the path
		escapedPath := stringutil.EscapeGlobQuestionMark(fileNameKey)

		allFiles := make([]mydump.RawFile, 0, 16)
		if err := s.WalkDir(ctx, &storage.WalkOption{ObjPrefix: commonPrefix, SkipSubDir: true},
			func(remotePath string, size int64) error {
				allFiles = append(allFiles, mydump.RawFile{Path: remotePath, Size: size})
				return nil
			}); err != nil {
			return exeerrors.ErrLoadDataCantRead.GenWithStackByArgs(errors.GetErrStackMsg(err), "failed to walk dir")
		}

		var err error
		var processedFiles []*mydump.SourceFileMeta
		var once sync.Once

		ce := newCompressionRecorder()

		if processedFiles, err = mydump.ParallelProcess(ctx, allFiles, e.ThreadCnt*2,
			func(ctx context.Context, f mydump.RawFile) (*mydump.SourceFileMeta, error) {
				// we have checked in LoadDataExec.Next
				//nolint: errcheck
				match, _ := filepath.Match(escapedPath, f.Path)
				if !match {
					return nil, nil
				}
				path, size := f.Path, f.Size
				// pick arbitrary one file to detect the format.
				var err2 error
				once.Do(func() {
					err2 = checkFirstFile(s, path)
				})
				if err2 != nil {
					return nil, err2
				}
				compressTp := mydump.ParseCompressionOnFileExtension(path)
				fileMeta := mydump.SourceFileMeta{
					Path:        path,
					FileSize:    size,
					Compression: compressTp,
					Type:        sourceType,
				}
				fileMeta.RealSize = int64(ce.estimate(ctx, fileMeta, s) * float64(fileMeta.FileSize))
				fileMeta.RealSize = int64(float64(fileMeta.RealSize) * sizeExpansionRatio)
				return &fileMeta, nil
			}); err != nil {
			return err
		}
		// filter unmatch files
		for _, f := range processedFiles {
			if f != nil {
				dataFiles = append(dataFiles, f)
				totalSize += f.FileSize
			}
		}
	}
	if e.InImportInto && isAutoDetectingFormat && e.Format != DataFormatCSV {
		if err2 = e.checkNonCSVFormatOptions(); err2 != nil {
			return err2
		}
	}

	e.dataFiles = dataFiles
	e.TotalFileSize = totalSize

	return nil
}

// CalResourceParams calculates resource related parameters according to the total
// file size and target node cpu count.
func (e *LoadDataController) CalResourceParams(ctx context.Context, ksCodec []byte) error {
	start := time.Now()
	targetNodeCPUCnt, err := handle.GetCPUCountOfNode(ctx)
	if err != nil {
		return err
	}
	factors, err := handle.GetScheduleTuneFactors(ctx, e.Keyspace)
	if err != nil {
		return err
	}
	totalSize := e.TotalFileSize
	failpoint.InjectCall("mockImportDataSize", &totalSize)
	numOfIndexGenKV := GetNumOfIndexGenKV(e.TableInfo)
	var indexSizeRatio float64
	if numOfIndexGenKV > 0 {
		indexSizeRatio, err = e.sampleIndexSizeRatio(ctx, ksCodec)
		if err != nil {
			e.logger.Warn("meet error when sampling index size ratio", zap.Error(err))
		}
	}
	cal := scheduler.NewRCCalc(totalSize, targetNodeCPUCnt, indexSizeRatio, factors)
	e.ThreadCnt = cal.CalcRequiredSlots()
	e.MaxNodeCnt = cal.CalcMaxNodeCountForImportInto()
	e.DistSQLScanConcurrency = scheduler.CalcDistSQLConcurrency(e.ThreadCnt, e.MaxNodeCnt, targetNodeCPUCnt)
	e.logger.Info("auto calculate resource related params",
		zap.Int("thread", e.ThreadCnt),
		zap.Int("maxNode", e.MaxNodeCnt),
		zap.Int("distsqlScanConcurrency", e.DistSQLScanConcurrency),
		zap.Int("targetNodeCPU", targetNodeCPUCnt),
		zap.String("totalFileSize", units.BytesSize(float64(totalSize))),
		zap.Int("fileCount", len(e.dataFiles)),
		zap.Int("numOfIndexGenKV", numOfIndexGenKV),
		zap.Float64("indexSizeRatio", indexSizeRatio),
		zap.Float64("amplifyFactor", factors.AmplifyFactor),
		zap.Duration("costTime", time.Since(start)),
	)
	return nil
}

// update format of the validated file by its extension.
func (e *LoadDataController) detectAndUpdateFormat(path string) {
	if e.Format == DataFormatAuto {
		e.Format = parseFileType(path)
		e.logger.Info("detect and update import plan format based on file extension",
			zap.String("file", path), zap.String("detected format", e.Format))
		e.Parameters.Format = e.Format
	}
}

func parseFileType(path string) string {
	path = strings.ToLower(path)
	ext := filepath.Ext(path)
	// avoid duplicate compress extension
	if ext == ".gz" || ext == ".gzip" || ext == ".zstd" || ext == ".zst" || ext == ".snappy" {
		path = strings.TrimSuffix(path, ext)
		ext = filepath.Ext(path)
	}
	switch ext {
	case ".sql":
		return DataFormatSQL
	case ".parquet":
		return DataFormatParquet
	default:
		// if file do not contain file extension, use ".csv" as default format
		return DataFormatCSV
	}
}

func (e *LoadDataController) getSourceType() mydump.SourceType {
	switch e.Format {
	case DataFormatParquet:
		return mydump.SourceTypeParquet
	case DataFormatDelimitedData, DataFormatCSV:
		return mydump.SourceTypeCSV
	default:
		// DataFormatSQL
		return mydump.SourceTypeSQL
	}
}

// GetLoadDataReaderInfos returns the LoadDataReaderInfo for each data file.
func (e *LoadDataController) GetLoadDataReaderInfos() []LoadDataReaderInfo {
	result := make([]LoadDataReaderInfo, 0, len(e.dataFiles))
	for i := range e.dataFiles {
		f := e.dataFiles[i]
		result = append(result, LoadDataReaderInfo{
			Opener: func(ctx context.Context) (io.ReadSeekCloser, error) {
				fileReader, err2 := mydump.OpenReader(ctx, f, e.dataStore, storage.DecompressConfig{})
				if err2 != nil {
					return nil, exeerrors.ErrLoadDataCantRead.GenWithStackByArgs(errors.GetErrStackMsg(err2), "Please check the INFILE path is correct")
				}
				return fileReader, nil
			},
			Remote: f,
		})
	}
	return result
}

// GetParser returns a parser for the data file.
func (e *LoadDataController) GetParser(
	ctx context.Context,
	dataFileInfo LoadDataReaderInfo,
) (parser mydump.Parser, err error) {
	reader, err2 := dataFileInfo.Opener(ctx)
	if err2 != nil {
		return nil, err2
	}
	defer func() {
		if err != nil {
			if err3 := reader.Close(); err3 != nil {
				e.logger.Warn("failed to close reader", zap.Error(err3))
			}
		}
	}()
	switch e.Format {
	case DataFormatDelimitedData, DataFormatCSV:
		var charsetConvertor *mydump.CharsetConvertor
		if e.Charset != nil {
			charsetConvertor, err = mydump.NewCharsetConvertor(*e.Charset, string(utf8.RuneError))
			if err != nil {
				return nil, err
			}
		}
		if err != nil {
			return nil, err
		}
		parser, err = mydump.NewCSVParser(
			ctx,
			e.GenerateCSVConfig(),
			reader,
			LoadDataReadBlockSize,
			nil,
			false,
			charsetConvertor)
	case DataFormatSQL:
		parser = mydump.NewChunkParser(
			ctx,
			e.SQLMode,
			reader,
			LoadDataReadBlockSize,
			nil,
		)
	case DataFormatParquet:
		parser, err = mydump.NewParquetParser(
			ctx,
			e.dataStore,
			reader,
			dataFileInfo.Remote.Path,
			dataFileInfo.Remote.ParquetMeta,
		)
	}
	if err != nil {
		return nil, exeerrors.ErrLoadDataWrongFormatConfig.GenWithStack(err.Error())
	}
	parser.SetLogger(litlog.Logger{Logger: logutil.Logger(ctx)})

	return parser, nil
}

// HandleSkipNRows skips the first N rows of the data file.
func (e *LoadDataController) HandleSkipNRows(parser mydump.Parser) error {
	// handle IGNORE N LINES
	ignoreOneLineFn := parser.ReadRow
	if csvParser, ok := parser.(*mydump.CSVParser); ok {
		ignoreOneLineFn = func() error {
			_, _, err3 := csvParser.ReadUntilTerminator()
			return err3
		}
	}

	ignoreLineCnt := e.IgnoreLines
	for ignoreLineCnt > 0 {
		err := ignoreOneLineFn()
		if err != nil {
			if errors.Cause(err) == io.EOF {
				return nil
			}
			return err
		}

		ignoreLineCnt--
	}
	return nil
}

func (e *LoadDataController) toMyDumpFiles() []mydump.FileInfo {
	tbl := filter.Table{
		Schema: e.DBName,
		Name:   e.Table.Meta().Name.O,
	}
	res := []mydump.FileInfo{}
	for _, f := range e.dataFiles {
		res = append(res, mydump.FileInfo{
			TableName: tbl,
			FileMeta:  *f,
		})
	}
	return res
}

// IsLocalSort returns true if we sort data on local disk.
func (p *Plan) IsLocalSort() bool {
	return p.CloudStorageURI == ""
}

// IsGlobalSort returns true if we sort data on global storage.
func (p *Plan) IsGlobalSort() bool {
	return !p.IsLocalSort()
}

// non CSV format should not specify CSV only options, we check it again if the
// format is detected automatically.
func (p *Plan) checkNonCSVFormatOptions() error {
	for k := range csvOnlyOptions {
		if _, ok := p.specifiedOptions[k]; ok {
			return exeerrors.ErrLoadDataUnsupportedOption.FastGenByArgs(k, "non-CSV format")
		}
	}
	return nil
}

// CreateColAssignExprs creates the column assignment expressions using session context.
// RewriteAstExpr will write ast node in place(due to xxNode.Accept), but it doesn't change node content,
// so we sync it.
func (e *LoadDataController) CreateColAssignExprs(planCtx planctx.PlanContext) (
	_ []expression.Expression,
	_ []contextutil.SQLWarn,
	retErr error,
) {
	e.colAssignMu.Lock()
	defer e.colAssignMu.Unlock()
	res := make([]expression.Expression, 0, len(e.ColumnAssignments))
	allWarnings := []contextutil.SQLWarn{}
	for _, assign := range e.ColumnAssignments {
		newExpr, err := plannerutil.RewriteAstExprWithPlanCtx(planCtx, assign.Expr, nil, nil, false)
		// col assign expr warnings is static, we should generate it for each row processed.
		// so we save it and clear it here.
		allWarnings = append(allWarnings, planCtx.GetSessionVars().StmtCtx.GetWarnings()...)
		planCtx.GetSessionVars().StmtCtx.SetWarnings(nil)
		if err != nil {
			return nil, nil, err
		}
		res = append(res, newExpr)
	}
	return res, allWarnings, nil
}

// CreateColAssignSimpleExprs creates the column assignment expressions using `expression.BuildContext`.
// This method does not support:
//   - Subquery
//   - System Variables (e.g. `@@tidb_enable_async_commit`)
//   - Window functions
//   - Aggregate functions
//   - Other special functions used in some specified queries such as `GROUPING`, `VALUES` ...
func (e *LoadDataController) CreateColAssignSimpleExprs(ctx expression.BuildContext) (_ []expression.Expression, _ []contextutil.SQLWarn, retErr error) {
	e.colAssignMu.Lock()
	defer e.colAssignMu.Unlock()
	res := make([]expression.Expression, 0, len(e.ColumnAssignments))
	var allWarnings []contextutil.SQLWarn
	for _, assign := range e.ColumnAssignments {
		newExpr, err := expression.BuildSimpleExpr(ctx, assign.Expr)
		// col assign expr warnings is static, we should generate it for each row processed.
		// so we save it and clear it here.
		if ctx.GetEvalCtx().WarningCount() > 0 {
			allWarnings = append(allWarnings, ctx.GetEvalCtx().TruncateWarnings(0)...)
		}
		if err != nil {
			return nil, nil, err
		}
		res = append(res, newExpr)
	}
	return res, allWarnings, nil
}

func (e *LoadDataController) getLocalBackendCfg(keyspace, pdAddr, dataDir string) local.BackendConfig {
	backendConfig := local.BackendConfig{
		PDAddr:                 pdAddr,
		LocalStoreDir:          dataDir,
		MaxConnPerStore:        config.DefaultRangeConcurrency,
		ConnCompressType:       config.CompressionNone,
		WorkerConcurrency:      *atomic.NewInt32(int32(e.ThreadCnt)),
		KVWriteBatchSize:       config.KVWriteBatchSize,
		RegionSplitBatchSize:   config.DefaultRegionSplitBatchSize,
		RegionSplitConcurrency: runtime.GOMAXPROCS(0),
		// enable after we support checkpoint
		CheckpointEnabled:           false,
		MemTableSize:                config.DefaultEngineMemCacheSize,
		LocalWriterMemCacheSize:     int64(config.DefaultLocalWriterMemCacheSize),
		ShouldCheckTiKV:             true,
		DupeDetectEnabled:           false,
		DuplicateDetectOpt:          common.DupDetectOpt{ReportErrOnDup: false},
		TiKVWorkerURL:               tidb.GetGlobalConfig().TiKVWorkerURL,
		StoreWriteBWLimit:           int(e.MaxWriteSpeed),
		MaxOpenFiles:                int(tidbutil.GenRLimit("table_import")),
		KeyspaceName:                keyspace,
		PausePDSchedulerScope:       config.PausePDSchedulerScopeTable,
		DisableAutomaticCompactions: true,
		BlockSize:                   config.DefaultBlockSize,
	}
	if e.IsRaftKV2 {
		backendConfig.RaftKV2SwitchModeDuration = config.DefaultSwitchTiKVModeInterval
	}
	return backendConfig
}

// FullTableName return FQDN of the table.
func (e *LoadDataController) FullTableName() string {
	return common.UniqueTable(e.DBName, e.Table.Meta().Name.O)
}

func getDataSourceType(p *plannercore.ImportInto) DataSourceType {
	if p.SelectPlan != nil {
		return DataSourceTypeQuery
	}
	return DataSourceTypeFile
}

// GetTargetNodeCPUCnt get cpu count of target node where the import into job will be executed.
// target node is current node if it's server-disk import, import from query or disttask is disabled,
// else it's the node managed by disttask.
// exported for testing.
func GetTargetNodeCPUCnt(ctx context.Context, sourceType DataSourceType, path string) (int, error) {
	if sourceType == DataSourceTypeQuery {
		return cpu.GetCPUCount(), nil
	}

	u, err2 := storage.ParseRawURL(path)
	if err2 != nil {
		return 0, exeerrors.ErrLoadDataInvalidURI.GenWithStackByArgs(plannercore.ImportIntoDataSource,
			err2.Error())
	}

	serverDiskImport := storage.IsLocal(u)
	if serverDiskImport || !vardef.EnableDistTask.Load() {
		return cpu.GetCPUCount(), nil
	}
	return handle.GetCPUCountOfNode(ctx)
}
