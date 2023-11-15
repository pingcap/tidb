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
	"io"
	"math"
	"runtime"
	"strings"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/executor/asyncloaddata"
	"github.com/pingcap/tidb/expression"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	pformat "github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/dbterror/exeerrors"
	kvconfig "github.com/tikv/client-go/v2/config"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type (
	dataSourceType string
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

	// DataSourceTypeFile represents the data source of IMPORT INTO is file.
	// exported for test.
	DataSourceTypeFile dataSourceType = "file"
	// DataSourceTypeQuery represents the data source of IMPORT INTO is query.
	DataSourceTypeQuery dataSourceType = "query"

	// DefaultDiskQuota is the default disk quota for IMPORT INTO
	DefaultDiskQuota = config.ByteSize(50 << 30) // 50GiB

	// 0 means no limit
	unlimitedWriteSpeed = config.ByteSize(0)

	characterSetOption          = "character_set"
	fieldsTerminatedByOption    = "fields_terminated_by"
	fieldsEnclosedByOption      = "fields_enclosed_by"
	fieldsEscapedByOption       = "fields_escaped_by"
	fieldsDefinedNullByOption   = "fields_defined_null_by"
	linesTerminatedByOption     = "lines_terminated_by"
	skipRowsOption              = "skip_rows"
	splitFileOption             = "split_file"
	diskQuotaOption             = "disk_quota"
	threadOption                = "thread"
	maxWriteSpeedOption         = "max_write_speed"
	checksumTableOption         = "checksum_table"
	recordErrorsOption          = "record_errors"
	detachedOption              = "detached"
	disableTiKVImportModeOption = "disable_tikv_import_mode"
	cloudStorageURIOption       = "cloud_storage_uri"
	// used for test
	maxEngineSizeOption = "__max_engine_size"
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
		splitFileOption:             false,
		diskQuotaOption:             true,
		threadOption:                true,
		maxWriteSpeedOption:         true,
		checksumTableOption:         true,
		recordErrorsOption:          true,
		detachedOption:              false,
		disableTiKVImportModeOption: false,
		maxEngineSizeOption:         true,
		cloudStorageURIOption:       true,
	}

	// options that can only be used when import from CSV files.
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

	// options that can be used when import from query.
	importFromQueryOptions = map[string]struct{}{
		threadOption: {},
	}

	// LoadDataReadBlockSize is exposed for test.
	LoadDataReadBlockSize = int64(config.ReadBlockSize)

	supportedSuffixForServerDisk = []string{
		".csv", ".sql", ".parquet",
		".gz", ".gzip",
		".zstd", ".zst",
		".snappy",
	}
)

// GetKVStore returns a kv.Storage.
// kv encoder of physical mode needs it.
var GetKVStore func(path string, tls kvconfig.Security) (tidbkv.Storage, error)

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

	SQLMode mysql.SQLMode
	// Charset is the charset of the data file when file is CSV or TSV.
	// it might be nil when using LOAD DATA and no charset is specified.
	// for IMPORT INTO, it is always non-nil.
	Charset          *string
	ImportantSysVars map[string]string

	// used for LOAD DATA and CSV format of IMPORT INTO
	FieldNullDef []string
	// this is not used in IMPORT INTO
	NullValueOptEnclosed bool
	// LinesStartingBy is not used in IMPORT INTO
	// FieldsOptEnclosed is not used in either IMPORT INTO or LOAD DATA
	IgnoreLines uint64

	DiskQuota             config.ByteSize
	Checksum              config.PostOpLevel
	ThreadCnt             int64
	MaxWriteSpeed         config.ByteSize
	SplitFile             bool
	MaxRecordedErrors     int64
	Detached              bool
	DisableTiKVImportMode bool
	MaxEngineSize         config.ByteSize
	CloudStorageURI       string

	// used for checksum in physical mode
	DistSQLScanConcurrency int

	// todo: remove it when load data code is reverted.
	InImportInto   bool
	DataSourceType dataSourceType
	// only initialized for IMPORT INTO, used when creating job.
	Parameters *ImportParameters `json:"-"`
	// the user who executes the statement, in the form of user@host
	// only initialized for IMPORT INTO
	User string `json:"-"`

	IsRaftKV2 bool
	// total data file size in bytes.
	TotalFileSize int64
}

// ASTArgs is the arguments for ast.LoadDataStmt.
// TODO: remove this struct and use the struct which can be serialized.
type ASTArgs struct {
	ColumnsAndUserVars []*ast.ColumnNameOrUserVar
	ColumnAssignments  []*ast.Assignment
	OnDuplicate        ast.OnDuplicateKeyHandlingType
	FieldsInfo         *ast.FieldsClause
	LinesInfo          *ast.LinesClause
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
	// see InsertValues.InsertColumns
	// Note: our behavior is different with mysql. such as for table t(a,b)
	// - "...(a,a) set a=100" is allowed in mysql, but not in tidb
	// - "...(a,b) set b=100" will set b=100 in mysql, but in tidb the set is ignored.
	// - ref columns in set clause is allowed in mysql, but not in tidb
	InsertColumns []*table.Column

	logger    *zap.Logger
	dataStore storage.ExternalStorage
	dataFiles []*mydump.SourceFileMeta
	// GlobalSortStore is used to store sorted data when using global sort.
	GlobalSortStore storage.ExternalStorage
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

// NewImportPlan creates a new import into plan.
func NewImportPlan(userSctx sessionctx.Context, plan *plannercore.ImportInto, tbl table.Table) (*Plan, error) {
	var format string
	if plan.Format != nil {
		format = strings.ToLower(*plan.Format)
	} else {
		// without FORMAT 'xxx' clause, default to CSV
		format = DataFormatCSV
	}
	restrictive := userSctx.GetSessionVars().SQLMode.HasStrictMode()

	p := &Plan{
		TableInfo:        tbl.Meta(),
		DesiredTableInfo: tbl.Meta(),
		DBName:           plan.Table.Schema.O,
		DBID:             plan.Table.DBInfo.ID,

		Path:         plan.Path,
		Format:       format,
		Restrictive:  restrictive,
		FieldNullDef: []string{`\N`},

		SQLMode:          userSctx.GetSessionVars().SQLMode,
		ImportantSysVars: getImportantSysVars(userSctx),

		DistSQLScanConcurrency: userSctx.GetSessionVars().DistSQLScanConcurrency(),
		InImportInto:           true,
		DataSourceType:         getDataSourceType(plan),
		User:                   userSctx.GetSessionVars().User.String(),
	}
	if err := p.initOptions(userSctx, plan.Options); err != nil {
		return nil, err
	}
	if err := p.initParameters(plan); err != nil {
		return nil, err
	}
	return p, nil
}

// ASTArgsFromImportPlan creates ASTArgs from plan.
func ASTArgsFromImportPlan(plan *plannercore.ImportInto) *ASTArgs {
	// FileLocRef are not used in ImportIntoStmt, OnDuplicate not used now.
	return &ASTArgs{
		ColumnsAndUserVars: plan.ColumnsAndUserVars,
		ColumnAssignments:  plan.ColumnAssignments,
		OnDuplicate:        ast.OnDuplicateKeyHandlingReplace,
	}
}

// NewLoadDataController create new controller.
func NewLoadDataController(plan *Plan, tbl table.Table, astArgs *ASTArgs) (*LoadDataController, error) {
	fullTableName := tbl.Meta().Name.String()
	logger := log.L().With(zap.String("table", fullTableName))
	c := &LoadDataController{
		Plan:            plan,
		ASTArgs:         astArgs,
		Table:           tbl,
		logger:          logger,
		ExecuteNodesCnt: 1,
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

func (e *LoadDataController) checkFieldParams() error {
	if e.DataSourceType == DataSourceTypeFile && e.Path == "" {
		return exeerrors.ErrLoadDataEmptyPath
	}
	if e.InImportInto {
		if e.Format != DataFormatCSV && e.Format != DataFormatParquet && e.Format != DataFormatSQL {
			return exeerrors.ErrLoadDataUnsupportedFormat.GenWithStackByArgs(e.Format)
		}
	}

	return nil
}

func (p *Plan) initDefaultOptions() {
	threadCnt := runtime.GOMAXPROCS(0)
	failpoint.Inject("mockNumCpu", func(val failpoint.Value) {
		threadCnt = val.(int)
	})
	if p.DataSourceType == DataSourceTypeFile {
		threadCnt = int(math.Max(1, float64(threadCnt)*0.5))
	} else {
		// if we import from query, we use 1 thread to do encode&deliver on default.
		threadCnt = 1
	}

	p.Checksum = config.OpLevelRequired
	p.ThreadCnt = int64(threadCnt)
	p.MaxWriteSpeed = unlimitedWriteSpeed
	p.SplitFile = false
	p.MaxRecordedErrors = 100
	p.Detached = false
	p.DisableTiKVImportMode = false
	p.MaxEngineSize = config.ByteSize(defaultMaxEngineSize)

	v := "utf8mb4"
	p.Charset = &v
}

func (p *Plan) initOptions(seCtx sessionctx.Context, options []*plannercore.LoadDataOpt) error {
	p.initDefaultOptions()

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

	if p.Format != DataFormatCSV {
		for k := range csvOnlyOptions {
			if _, ok := specifiedOptions[k]; ok {
				return exeerrors.ErrLoadDataUnsupportedOption.FastGenByArgs(k, "non-CSV format")
			}
		}
	}
	if p.DataSourceType == DataSourceTypeQuery {
		for k := range specifiedOptions {
			if _, ok := importFromQueryOptions[k]; !ok {
				return exeerrors.ErrLoadDataUnsupportedOption.FastGenByArgs(k, "import from query")
			}
		}
	}

	optAsString := func(opt *plannercore.LoadDataOpt) (string, error) {
		if opt.Value.GetType().GetType() != mysql.TypeVarString {
			return "", exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		val, isNull, err2 := opt.Value.EvalString(seCtx, chunk.Row{})
		if err2 != nil || isNull {
			return "", exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		return val, nil
	}
	optAsInt64 := func(opt *plannercore.LoadDataOpt) (int64, error) {
		// current parser takes integer and bool as mysql.TypeLonglong
		if opt.Value.GetType().GetType() != mysql.TypeLonglong || mysql.HasIsBooleanFlag(opt.Value.GetType().GetFlag()) {
			return 0, exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		val, isNull, err2 := opt.Value.EvalInt(seCtx, chunk.Row{})
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
	if opt, ok := specifiedOptions[fieldsDefinedNullByOption]; ok {
		v, err := optAsString(opt)
		if err != nil {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		p.FieldNullDef = []string{v}
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
		p.ThreadCnt = vInt
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

	// when split-file is set, data file will be split into chunks of 256 MiB.
	// skip_rows should be 0 or 1, we add this restriction to simplify skip_rows
	// logic, so we only need to skip on the first chunk for each data file.
	// CSV parser limit each row size to LargestEntryLimit(120M), the first row
	// will NOT cross file chunk.
	if p.SplitFile && p.IgnoreLines > 1 {
		return exeerrors.ErrInvalidOptionVal.FastGenByArgs("skip_rows, should be <= 1 when split-file is enabled")
	}

	p.adjustOptions()
	return nil
}

func (p *Plan) adjustOptions() {
	// max value is cpu-count
	numCPU := int64(runtime.GOMAXPROCS(0))
	if p.ThreadCnt > numCPU {
		log.L().Info("IMPORT INTO thread count is larger than cpu-count, set to cpu-count")
		p.ThreadCnt = numCPU
	}
}

func (p *Plan) initParameters(plan *plannercore.ImportInto) error {
	redactURL := p.Path
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
	optionMap := make(map[string]interface{}, len(plan.Options))
	for _, opt := range plan.Options {
		if opt.Value != nil {
			val := opt.Value.String()
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

// initFieldMappings make a field mapping slice to implicitly map input field to table column or user defined variable
// the slice's order is the same as the order of the input fields.
// Returns a slice of same ordered column names without user defined variable names.
func (e *LoadDataController) initFieldMappings() []string {
	columns := make([]string, 0, len(e.ColumnsAndUserVars)+len(e.ColumnAssignments))
	tableCols := e.Table.VisibleCols()

	if len(e.ColumnsAndUserVars) == 0 {
		for _, v := range tableCols {
			// Data for generated column is generated from the other rows rather than from the parsed data.
			fieldMapping := &FieldMapping{
				Column: v,
			}
			e.FieldMappings = append(e.FieldMappings, fieldMapping)
			columns = append(columns, v.Name.O)
		}

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

// IsLocalSort returns true if we sort data on local disk.
func (e *LoadDataController) IsLocalSort() bool {
	return e.Plan.CloudStorageURI == ""
}

// IsGlobalSort returns true if we sort data on global storage.
func (e *LoadDataController) IsGlobalSort() bool {
	return !e.IsLocalSort()
}

// CreateColAssignExprs creates the column assignment expressions using session context.
// RewriteAstExpr will write ast node in place(due to xxNode.Accept), but it doesn't change node content,
// so we sync it.
func (e *LoadDataController) CreateColAssignExprs(sctx sessionctx.Context) ([]expression.Expression, []stmtctx.SQLWarn, error) {
	e.colAssignMu.Lock()
	defer e.colAssignMu.Unlock()
	res := make([]expression.Expression, 0, len(e.ColumnAssignments))
	allWarnings := []stmtctx.SQLWarn{}
	for _, assign := range e.ColumnAssignments {
		newExpr, err := expression.RewriteAstExpr(sctx, assign.Expr, nil, nil)
		// col assign expr warnings is static, we should generate it for each row processed.
		// so we save it and clear it here.
		allWarnings = append(allWarnings, sctx.GetSessionVars().StmtCtx.GetWarnings()...)
		sctx.GetSessionVars().StmtCtx.SetWarnings(nil)
		if err != nil {
			return nil, nil, err
		}
		res = append(res, newExpr)
	}
	return res, allWarnings, nil
}

func getDataSourceType(p *plannercore.ImportInto) dataSourceType {
	if p.SelectPlan != nil {
		return DataSourceTypeQuery
	}
	return DataSourceTypeFile
}

// JobImportParam is the param of the job import.
type JobImportParam struct {
	Job      *asyncloaddata.Job
	Group    *errgroup.Group
	GroupCtx context.Context
	// should be closed in the end of the job.
	Done chan struct{}

	Progress *asyncloaddata.Progress
}

// JobImportResult is the result of the job import.
type JobImportResult struct {
	Affected   uint64
	Warnings   []stmtctx.SQLWarn
	ColSizeMap map[int64]int64
}

// GetMsgFromBRError get msg from BR error.
// TODO: add GetMsg() to errors package to replace this function.
// see TestGetMsgFromBRError for more details.
func GetMsgFromBRError(err error) string {
	if err == nil {
		return ""
	}
	if berr, ok := err.(*errors.Error); ok {
		return berr.GetMsg()
	}
	raw := err.Error()
	berrMsg := errors.Cause(err).Error()
	if len(raw) <= len(berrMsg)+len(": ") {
		return raw
	}
	return raw[:len(raw)-len(berrMsg)-len(": ")]
}

// TestSyncCh is used in unit test to synchronize the execution.
var TestSyncCh = make(chan struct{})
