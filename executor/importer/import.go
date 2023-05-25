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
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	litlog "github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/executor/asyncloaddata"
	"github.com/pingcap/tidb/expression"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/util/filter"
	"github.com/pingcap/tidb/util/intest"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/stringutil"
	kvconfig "github.com/tikv/client-go/v2/config"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	// LoadDataFormatDelimitedData delimited data.
	LoadDataFormatDelimitedData = "delimited data"
	// LoadDataFormatSQLDump represents the data source file of LOAD DATA is mydumper-format DML file.
	LoadDataFormatSQLDump = "sql file"
	// LoadDataFormatParquet represents the data source file of LOAD DATA is parquet.
	LoadDataFormatParquet = "parquet"

	// LogicalImportMode represents the import mode is SQL-like.
	LogicalImportMode = "logical"
	// PhysicalImportMode represents the import mode is KV-like.
	PhysicalImportMode = "physical"
	// 0 means no limit
	unlimitedWriteSpeed = config.ByteSize(0)
	minDiskQuota        = config.ByteSize(10 << 30) // 10GiB

	importModeOption    = "import_mode"
	diskQuotaOption     = "disk_quota"
	checksumOption      = "checksum_table"
	addIndexOption      = "add_index"
	analyzeOption       = "analyze_table"
	threadOption        = "thread"
	maxWriteSpeedOption = "max_write_speed"
	splitFileOption     = "split_file"
	recordErrorsOption  = "record_errors"
	detachedOption      = plannercore.DetachedOption

	// test option, not for user
	distributedOption = "__distributed"
)

var (
	// name -> whether the option has value
	supportedOptions = map[string]bool{
		importModeOption:    true,
		diskQuotaOption:     true,
		checksumOption:      true,
		addIndexOption:      true,
		analyzeOption:       true,
		threadOption:        true,
		maxWriteSpeedOption: true,
		splitFileOption:     true,
		recordErrorsOption:  true,
		detachedOption:      false,
		distributedOption:   true,
	}

	// options only allowed when import mode is physical
	optionsForPhysicalImport = map[string]struct{}{
		diskQuotaOption:   {},
		checksumOption:    {},
		addIndexOption:    {},
		analyzeOption:     {},
		distributedOption: {},
	}

	// LoadDataReadBlockSize is exposed for test.
	LoadDataReadBlockSize = int64(config.ReadBlockSize)
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

// Plan describes the plan of LOAD DATA.
type Plan struct {
	DBName           string
	DBID             int64
	TableInfo        *model.TableInfo
	DesiredTableInfo *model.TableInfo

	Path   string
	Format string
	// Data interpretation is restrictive if the SQL mode is restrictive and neither
	// the IGNORE nor the LOCAL modifier is specified. Errors terminate the load
	// operation.
	// ref https://dev.mysql.com/doc/refman/8.0/en/load-data.html#load-data-column-assignments
	Restrictive bool
	IgnoreLines *uint64

	SQLMode          mysql.SQLMode
	Charset          *string
	ImportantSysVars map[string]string

	ImportMode        string
	DiskQuota         config.ByteSize
	Checksum          config.PostOpLevel
	AddIndex          bool
	Analyze           config.PostOpLevel
	ThreadCnt         int64
	MaxWriteSpeed     config.ByteSize
	SplitFile         bool
	MaxRecordedErrors int64
	Detached          bool

	// used for checksum in physical mode
	DistSQLScanConcurrency int

	// test
	Distributed bool `json:"-"`
	// todo: remove it when load data code is reverted.
	InImportInto bool
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

// LoadDataController load data controller.
// todo: need a better name
type LoadDataController struct {
	*Plan
	*ASTArgs

	// used for sync column assignment expression generation.
	colAssignMu sync.Mutex

	Table table.Table

	// how input field(or input column) from data file is mapped, either to a column or variable.
	// if there's NO column list clause in load data statement, then it's table's columns
	// else it's user defined list.
	FieldMappings []*FieldMapping
	// see InsertValues.InsertColumns
	// todo: our behavior is different with mysql. such as for table t(a,b)
	// - "...(a,a) set a=100" is allowed in mysql, but not in tidb
	// - "...(a,b) set b=100" will set b=100 in mysql, but in tidb the set is ignored.
	// - ref columns in set clause is allowed in mysql, but not in tidb
	InsertColumns []*table.Column

	// used for DELIMITED DATA format
	FieldNullDef         []string
	NullValueOptEnclosed bool
	plannercore.LineFieldsInfo
	IgnoreLines uint64

	logger    *zap.Logger
	dataStore storage.ExternalStorage
	dataFiles []*mydump.SourceFileMeta
	// total data file size in bytes, only initialized when load from remote.
	TotalFileSize int64
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
func NewPlanFromLoadDataPlan(userSctx sessionctx.Context, plan *plannercore.LoadData, tbl table.Table) (*Plan, error) {
	fullTableName := common.UniqueTable(plan.Table.Schema.L, plan.Table.Name.L)
	logger := log.L().With(zap.String("table", fullTableName))
	charset := plan.Charset
	if charset == nil {
		// https://dev.mysql.com/doc/refman/8.0/en/load-data.html#load-data-character-set
		d, err2 := userSctx.GetSessionVars().GetSessionOrGlobalSystemVar(
			context.Background(), variable.CharsetDatabase)
		if err2 != nil {
			logger.Error("LOAD DATA get charset failed", zap.Error(err2))
		} else {
			charset = &d
		}
	}
	restrictive := userSctx.GetSessionVars().SQLMode.HasStrictMode() &&
		plan.OnDuplicate != ast.OnDuplicateKeyHandlingIgnore

	p := &Plan{
		DBName:      plan.Table.Schema.O,
		Path:        plan.Path,
		Format:      LoadDataFormatDelimitedData,
		Restrictive: restrictive,
		IgnoreLines: plan.IgnoreLines,
		Charset:     charset,
	}
	return p, nil
}

// NewImportPlan creates a new import into plan.
func NewImportPlan(userSctx sessionctx.Context, plan *plannercore.ImportInto, tbl table.Table) (*Plan, error) {
	fullTableName := common.UniqueTable(plan.Table.Schema.L, plan.Table.Name.L)
	logger := log.L().With(zap.String("table", fullTableName))
	var format string
	if plan.Format != nil {
		format = strings.ToLower(*plan.Format)
	} else {
		// without FORMAT 'xxx' clause, default to DELIMITED DATA
		format = LoadDataFormatDelimitedData
	}
	// todo: use charset in options
	var charset *string
	// https://dev.mysql.com/doc/refman/8.0/en/load-data.html#load-data-character-set
	d, err2 := userSctx.GetSessionVars().GetSessionOrGlobalSystemVar(
		context.Background(), variable.CharsetDatabase)
	if err2 != nil {
		logger.Error("LOAD DATA get charset failed", zap.Error(err2))
	} else {
		charset = &d
	}
	restrictive := userSctx.GetSessionVars().SQLMode.HasStrictMode()

	p := &Plan{
		TableInfo:        tbl.Meta(),
		DesiredTableInfo: tbl.Meta(),
		DBName:           plan.Table.Schema.O,
		DBID:             plan.Table.DBInfo.ID,
		Path:             plan.Path,
		Format:           format,
		Restrictive:      restrictive,

		SQLMode:          userSctx.GetSessionVars().SQLMode,
		Charset:          charset,
		ImportantSysVars: getImportantSysVars(userSctx),

		DistSQLScanConcurrency: userSctx.GetSessionVars().DistSQLScanConcurrency(),
		InImportInto:           true,
	}
	if err := p.initOptions(userSctx, plan.Options); err != nil {
		return nil, err
	}
	return p, nil
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

// NewLoadDataController create new controller.
func NewLoadDataController(plan *Plan, tbl table.Table, astArgs *ASTArgs) (*LoadDataController, error) {
	fullTableName := tbl.Meta().Name.String()
	logger := log.L().With(zap.String("table", fullTableName))
	lineFieldsInfo := plannercore.NewLineFieldsInfo(astArgs.FieldsInfo, astArgs.LinesInfo)
	if plan.InImportInto {
		// todo: refactor this part when load data reverted
		// those are the default values for lightning CSV format too
		lineFieldsInfo = plannercore.LineFieldsInfo{
			FieldsTerminatedBy: `,`,
			FieldsEnclosedBy:   `"`,
			FieldsEscapedBy:    `\`,
			FieldsOptEnclosed:  false,
			LinesStartingBy:    ``,
			// csv_parser will determine it automatically(either '\r' or '\n' or '\r\n')
			LinesTerminatedBy: ``,
		}
	}
	c := &LoadDataController{
		Plan:           plan,
		ASTArgs:        astArgs,
		Table:          tbl,
		LineFieldsInfo: lineFieldsInfo,
		logger:         logger,
	}
	if err := c.initFieldParams(plan); err != nil {
		return nil, err
	}

	columnNames := c.initFieldMappings()
	if err := c.initLoadColumns(columnNames); err != nil {
		return nil, err
	}
	return c, nil
}

func (e *LoadDataController) initFieldParams(plan *Plan) error {
	if e.Path == "" {
		return exeerrors.ErrLoadDataEmptyPath
	}
	if e.Format != LoadDataFormatDelimitedData && e.Format != LoadDataFormatParquet && e.Format != LoadDataFormatSQLDump {
		return exeerrors.ErrLoadDataUnsupportedFormat.GenWithStackByArgs(e.Format)
	}

	if e.FileLocRef == ast.FileLocClient {
		if e.Detached {
			return exeerrors.ErrLoadDataLocalUnsupportedOption.FastGenByArgs("DETACHED")
		}
		if e.Format == LoadDataFormatParquet {
			// parquet parser need seek around, it's not supported for client local file
			return exeerrors.ErrLoadParquetFromLocal
		}
		if e.ImportMode == PhysicalImportMode {
			return exeerrors.ErrLoadDataLocalUnsupportedOption.FastGenByArgs("import_mode='physical'")
		}
		if e.Distributed {
			return exeerrors.ErrLoadDataLocalUnsupportedOption.FastGenByArgs("__distributed=true")
		}
	}

	if e.Format != LoadDataFormatDelimitedData {
		if e.FieldsInfo != nil || e.LinesInfo != nil || plan.IgnoreLines != nil {
			return exeerrors.ErrLoadDataWrongFormatConfig.GenWithStackByArgs(fmt.Sprintf("cannot specify FIELDS ... or LINES ... or IGNORE N LINES for format '%s'", e.Format))
		}
		// no need to init those param for sql/parquet
		return nil
	}

	if plan.IgnoreLines != nil {
		e.IgnoreLines = *plan.IgnoreLines
	}

	var (
		nullDef              []string
		nullValueOptEnclosed = false
	)

	// todo: move null defined into plannercore.LineFieldsInfo
	// in load data, there maybe multiple null def, but in SELECT ... INTO OUTFILE there's only one
	if e.FieldsInfo != nil && e.FieldsInfo.DefinedNullBy != nil {
		nullDef = append(nullDef, *e.FieldsInfo.DefinedNullBy)
		nullValueOptEnclosed = e.FieldsInfo.NullValueOptEnclosed
	} else if len(e.FieldsEnclosedBy) != 0 {
		nullDef = append(nullDef, "NULL")
	}
	if len(e.FieldsEscapedBy) != 0 {
		nullDef = append(nullDef, string([]byte{e.FieldsEscapedBy[0], 'N'}))
	}

	e.FieldNullDef = nullDef
	e.NullValueOptEnclosed = nullValueOptEnclosed

	if nullValueOptEnclosed && len(e.FieldsEnclosedBy) == 0 {
		return exeerrors.ErrLoadDataWrongFormatConfig.GenWithStackByArgs("must specify FIELDS [OPTIONALLY] ENCLOSED BY when use NULL DEFINED BY OPTIONALLY ENCLOSED")
	}
	// moved from planerbuilder.buildLoadData
	// see https://github.com/pingcap/tidb/issues/33298
	if len(e.FieldsTerminatedBy) == 0 {
		return exeerrors.ErrLoadDataWrongFormatConfig.GenWithStackByArgs("load data with empty field terminator")
	}
	// TODO: support lines terminated is "".
	// todo: remove !e.InImportInto when load data is reverted
	if len(e.LinesTerminatedBy) == 0 && !e.InImportInto {
		return exeerrors.ErrLoadDataWrongFormatConfig.GenWithStackByArgs("LINES TERMINATED BY is empty")
	}
	if len(e.FieldsEnclosedBy) > 0 &&
		(strings.HasPrefix(e.FieldsEnclosedBy, e.FieldsTerminatedBy) || strings.HasPrefix(e.FieldsTerminatedBy, e.FieldsEnclosedBy)) {
		return exeerrors.ErrLoadDataWrongFormatConfig.GenWithStackByArgs("FIELDS ENCLOSED BY and TERMINATED BY must not be prefix of each other")
	}

	return nil
}

func (p *Plan) initDefaultOptions() {
	threadCnt := runtime.NumCPU()
	if p.Format == LoadDataFormatParquet {
		threadCnt = int(math.Max(1, float64(threadCnt)*0.75))
	}

	p.ImportMode = LogicalImportMode
	_ = p.DiskQuota.UnmarshalText([]byte("50GiB")) // todo confirm with pm
	p.Checksum = config.OpLevelRequired
	p.AddIndex = true
	p.Analyze = config.OpLevelOptional
	p.ThreadCnt = int64(threadCnt)
	p.MaxWriteSpeed = unlimitedWriteSpeed
	p.SplitFile = false
	p.MaxRecordedErrors = 100
	p.Detached = false
	p.Distributed = false

	// todo: remove it when load data is reverted
	if p.InImportInto {
		p.ImportMode = PhysicalImportMode
	}
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

	var (
		v      string
		err    error
		isNull bool
	)
	if opt, ok := specifiedOptions[importModeOption]; ok {
		v, isNull, err = opt.Value.EvalString(seCtx, chunk.Row{})
		if err != nil || isNull {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		v = strings.ToLower(v)
		if v != LogicalImportMode && v != PhysicalImportMode {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		p.ImportMode = v
	}

	if p.ImportMode == LogicalImportMode {
		// some options are only allowed in physical mode
		for _, opt := range specifiedOptions {
			if _, ok := optionsForPhysicalImport[opt.Name]; ok {
				return exeerrors.ErrLoadDataUnsupportedOption.FastGenByArgs(opt.Name, p.ImportMode)
			}
		}
	}
	if opt, ok := specifiedOptions[diskQuotaOption]; ok {
		v, isNull, err = opt.Value.EvalString(seCtx, chunk.Row{})
		if err != nil || isNull {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		if err = p.DiskQuota.UnmarshalText([]byte(v)); err != nil || p.DiskQuota <= 0 {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
	}
	if opt, ok := specifiedOptions[checksumOption]; ok {
		v, isNull, err = opt.Value.EvalString(seCtx, chunk.Row{})
		if err != nil || isNull {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		if err = p.Checksum.FromStringValue(v); err != nil {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
	}
	if opt, ok := specifiedOptions[addIndexOption]; ok {
		var vInt int64
		if !mysql.HasIsBooleanFlag(opt.Value.GetType().GetFlag()) {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		vInt, isNull, err = opt.Value.EvalInt(seCtx, chunk.Row{})
		if err != nil || isNull {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		p.AddIndex = vInt == 1
	}
	if opt, ok := specifiedOptions[analyzeOption]; ok {
		v, isNull, err = opt.Value.EvalString(seCtx, chunk.Row{})
		if err != nil || isNull {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		if err = p.Analyze.FromStringValue(v); err != nil {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
	}
	if opt, ok := specifiedOptions[threadOption]; ok {
		// boolean true will be taken as 1
		p.ThreadCnt, isNull, err = opt.Value.EvalInt(seCtx, chunk.Row{})
		if err != nil || isNull || p.ThreadCnt <= 0 {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
	}
	if opt, ok := specifiedOptions[maxWriteSpeedOption]; ok {
		v, isNull, err = opt.Value.EvalString(seCtx, chunk.Row{})
		if err != nil || isNull {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		if err = p.MaxWriteSpeed.UnmarshalText([]byte(v)); err != nil || p.MaxWriteSpeed < 0 {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
	}
	if opt, ok := specifiedOptions[splitFileOption]; ok {
		if !mysql.HasIsBooleanFlag(opt.Value.GetType().GetFlag()) {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		var vInt int64
		vInt, isNull, err = opt.Value.EvalInt(seCtx, chunk.Row{})
		if err != nil || isNull {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		p.SplitFile = vInt == 1
	}
	if opt, ok := specifiedOptions[recordErrorsOption]; ok {
		p.MaxRecordedErrors, isNull, err = opt.Value.EvalInt(seCtx, chunk.Row{})
		if err != nil || isNull || p.MaxRecordedErrors < -1 {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		// todo: set a max value for this param?
	}
	if _, ok := specifiedOptions[detachedOption]; ok {
		p.Detached = true
	}

	// test
	if opt, ok := specifiedOptions[distributedOption]; ok {
		var vInt int64
		if !mysql.HasIsBooleanFlag(opt.Value.GetType().GetFlag()) {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		vInt, isNull, err = opt.Value.EvalInt(seCtx, chunk.Row{})
		if err != nil || isNull {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		p.Distributed = vInt == 1
	}

	p.adjustOptions()
	return nil
}

func (p *Plan) adjustOptions() {
	if p.DiskQuota < minDiskQuota {
		p.DiskQuota = minDiskQuota
	}
	// max value is cpu-count
	numCPU := int64(runtime.NumCPU())
	if p.ThreadCnt > numCPU {
		p.ThreadCnt = numCPU
	}
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

// GenerateCSVConfig generates a CSV config for parser from LoadDataWorker.
func (e *LoadDataController) GenerateCSVConfig() *config.CSVConfig {
	csvConfig := &config.CSVConfig{
		Separator: e.FieldsTerminatedBy,
		// ignore optionally enclosed
		Delimiter:   e.FieldsEnclosedBy,
		Terminator:  e.LinesTerminatedBy,
		NotNull:     false,
		Null:        e.FieldNullDef,
		Header:      false,
		TrimLastSep: false,
		EscapedBy:   e.FieldsEscapedBy,
		StartingBy:  e.LinesStartingBy,
	}
	if !e.InImportInto {
		// for load data
		csvConfig.AllowEmptyLine = true
		csvConfig.QuotedNullIsText = !e.NullValueOptEnclosed
		csvConfig.UnescapedQuote = true
	}
	return csvConfig
}

// InitDataFiles initializes the data store and load data files.
func (e *LoadDataController) InitDataFiles(ctx context.Context) error {
	u, err2 := storage.ParseRawURL(e.Path)
	if err2 != nil {
		return exeerrors.ErrLoadDataInvalidURI.GenWithStackByArgs(err2.Error())
	}
	path := strings.Trim(u.Path, "/")
	u.Path = ""
	b, err2 := storage.ParseBackendFromURL(u, nil)
	if err2 != nil {
		return exeerrors.ErrLoadDataInvalidURI.GenWithStackByArgs(GetMsgFromBRError(err2))
	}
	if b.GetLocal() != nil {
		return exeerrors.ErrLoadDataFromServerDisk.GenWithStackByArgs(e.Path)
	}
	// try to find pattern error in advance
	_, err2 = filepath.Match(stringutil.EscapeGlobExceptAsterisk(path), "")
	if err2 != nil {
		return exeerrors.ErrLoadDataInvalidURI.GenWithStackByArgs("Glob pattern error: " + err2.Error())
	}

	opt := &storage.ExternalStorageOptions{}
	if intest.InTest {
		opt.NoCredentials = true
	}
	s, err := storage.New(ctx, b, opt)
	if err != nil {
		return exeerrors.ErrLoadDataCantAccess.GenWithStackByArgs(GetMsgFromBRError(err))
	}

	var totalSize int64
	dataFiles := []*mydump.SourceFileMeta{}
	idx := strings.IndexByte(path, '*')
	// simple path when the INFILE represent one file
	sourceType := e.getSourceType()
	if idx == -1 {
		fileReader, err2 := s.Open(ctx, path)
		if err2 != nil {
			return exeerrors.ErrLoadDataCantRead.GenWithStackByArgs(GetMsgFromBRError(err2), "Please check the INFILE path is correct")
		}
		defer func() {
			terror.Log(fileReader.Close())
		}()
		size, err3 := fileReader.Seek(0, io.SeekEnd)
		if err3 != nil {
			return exeerrors.ErrLoadDataCantRead.GenWithStackByArgs(GetMsgFromBRError(err2), "failed to read file size by seek in LOAD DATA")
		}
		compressTp := mydump.ParseCompressionOnFileExtension(path)
		dataFiles = append(dataFiles, &mydump.SourceFileMeta{
			Path:        path,
			FileSize:    size,
			Compression: compressTp,
			Type:        sourceType,
			// todo: if we support compression for physical mode, should set it to size * compressRatio to better split
			// engines
			RealSize: size,
		})
		totalSize = size
	} else {
		commonPrefix := path[:idx]
		// we only support '*', in order to reuse glob library manually escape the path
		escapedPath := stringutil.EscapeGlobExceptAsterisk(path)
		err = s.WalkDir(ctx, &storage.WalkOption{ObjPrefix: commonPrefix},
			func(remotePath string, size int64) error {
				// we have checked in LoadDataExec.Next
				//nolint: errcheck
				match, _ := filepath.Match(escapedPath, remotePath)
				if !match {
					return nil
				}
				compressTp := mydump.ParseCompressionOnFileExtension(remotePath)
				dataFiles = append(dataFiles, &mydump.SourceFileMeta{
					Path:        remotePath,
					FileSize:    size,
					Compression: compressTp,
					Type:        sourceType,
					RealSize:    size,
				})
				totalSize += size
				return nil
			})
		if err != nil {
			return err
		}
	}

	e.dataStore = s
	e.dataFiles = dataFiles
	e.TotalFileSize = totalSize
	return nil
}

func (e *LoadDataController) getSourceType() mydump.SourceType {
	switch e.Format {
	case LoadDataFormatParquet:
		return mydump.SourceTypeParquet
	case LoadDataFormatDelimitedData:
		return mydump.SourceTypeCSV
	default:
		// LoadDataFormatSQLDump
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
				fileReader, err2 := mydump.OpenReader(ctx, f, e.dataStore)
				if err2 != nil {
					return nil, exeerrors.ErrLoadDataCantRead.GenWithStackByArgs(GetMsgFromBRError(err2), "Please check the INFILE path is correct")
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
	case LoadDataFormatDelimitedData:
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
	case LoadDataFormatSQLDump:
		parser = mydump.NewChunkParser(
			ctx,
			e.SQLMode,
			reader,
			LoadDataReadBlockSize,
			nil,
		)
	case LoadDataFormatParquet:
		parser, err = mydump.NewParquetParser(
			ctx,
			e.dataStore,
			reader,
			dataFileInfo.Remote.Path,
		)
	}
	if err != nil {
		return nil, exeerrors.ErrLoadDataWrongFormatConfig.GenWithStack(err.Error())
	}
	parser.SetLogger(litlog.Logger{Logger: logutil.Logger(ctx)})

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
		err = ignoreOneLineFn()
		if err != nil {
			if errors.Cause(err) == io.EOF {
				return parser, nil
			}
			return nil, err
		}

		ignoreLineCnt--
	}
	return parser, nil
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

// CreateColAssignExprs creates the column assignment expressions using session context.
// RewriteAstExpr will write ast node in place(due to xxNode.Accept), but it doesn't change node content,
// so we sync it.
func (e *LoadDataController) CreateColAssignExprs(sctx sessionctx.Context) ([]expression.Expression, []stmtctx.SQLWarn, error) {
	e.colAssignMu.Lock()
	defer e.colAssignMu.Unlock()
	res := make([]expression.Expression, 0, len(e.ColumnAssignments))
	allWarnings := []stmtctx.SQLWarn{}
	for _, assign := range e.ColumnAssignments {
		newExpr, err := expression.RewriteAstExpr(sctx, assign.Expr, nil, nil, false)
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
	Msg          string
	LastInsertID uint64
	Affected     uint64
	Warnings     []stmtctx.SQLWarn
}

// JobImporter is the interface for importing a job.
type JobImporter interface {
	// Param returns the param of the job import.
	Param() *JobImportParam
	// Import imports the job.
	// import should run in routines using param.Group, when import finished, it should close param.Done.
	// during import, we should use param.GroupCtx, so this method has no context param.
	Import()
	// Result returns the result of the job import.
	Result() JobImportResult
	io.Closer
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

// TestSyncCh is used in unit test to synchronize the execution of LOAD DATA.
var TestSyncCh = make(chan struct{})
