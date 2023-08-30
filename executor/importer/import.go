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
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	litlog "github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/executor/asyncloaddata"
	"github.com/pingcap/tidb/expression"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	pformat "github.com/pingcap/tidb/parser/format"
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
	// DataFormatCSV represents the data source file of IMPORT INTO is csv.
	DataFormatCSV = "csv"
	// DataFormatDelimitedData delimited data.
	DataFormatDelimitedData = "delimited data"
	// DataFormatSQL represents the data source file of IMPORT INTO is mydumper-format DML file.
	DataFormatSQL = "sql"
	// DataFormatParquet represents the data source file of IMPORT INTO is parquet.
	DataFormatParquet = "parquet"

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
	// used for test
	maxEngineSizeOption = "__max_engine_size"
)

var (
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
	DBName string
	DBID   int64
	// TableInfo is the table info we used during import, we might change it
	// if add index by SQL is enabled(it's disabled now).
	TableInfo *model.TableInfo
	// DesiredTableInfo is the table info before import, and the desired table info
	// after import.
	DesiredTableInfo *model.TableInfo

	Path   string
	Format string
	// Data interpretation is restrictive if the SQL mode is restrictive and neither
	// the IGNORE nor the LOCAL modifier is specified. Errors terminate the load
	// operation.
	// ref https://dev.mysql.com/doc/refman/8.0/en/load-data.html#load-data-column-assignments
	Restrictive bool

	SQLMode          mysql.SQLMode
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
	ThreadCnt             int64
	MaxWriteSpeed         config.ByteSize
	SplitFile             bool
	MaxRecordedErrors     int64
	Detached              bool
	DisableTiKVImportMode bool
	MaxEngineSize         config.ByteSize

	// used for checksum in physical mode
	DistSQLScanConcurrency int

	// todo: remove it when load data code is reverted.
	InImportInto bool
	// only initialized for IMPORT INTO, used when creating job.
	Parameters *ImportParameters `json:"-"`
	// the user who executes the statement, in the form of user@host
	// only initialized for IMPORT INTO
	User string `json:"-"`

	IsRaftKV2 bool
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
func NewPlanFromLoadDataPlan(userSctx sessionctx.Context, plan *plannercore.LoadData) (*Plan, error) {
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
		DBName: plan.Table.Schema.O,
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
	}, nil
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
	// those are the default values for lightning CSV format too
	lineFieldsInfo := plannercore.LineFieldsInfo{
		FieldsTerminatedBy: `,`,
		FieldsEnclosedBy:   `"`,
		FieldsEscapedBy:    `\`,
		LinesStartingBy:    ``,
		// csv_parser will determine it automatically(either '\r' or '\n' or '\r\n')
		// But user cannot set this to empty explicitly.
		LinesTerminatedBy: ``,
	}

	p := &Plan{
		TableInfo:        tbl.Meta(),
		DesiredTableInfo: tbl.Meta(),
		DBName:           plan.Table.Schema.O,
		DBID:             plan.Table.DBInfo.ID,

		Path:           plan.Path,
		Format:         format,
		Restrictive:    restrictive,
		FieldNullDef:   []string{`\N`},
		LineFieldsInfo: lineFieldsInfo,

		SQLMode:          userSctx.GetSessionVars().SQLMode,
		ImportantSysVars: getImportantSysVars(userSctx),

		DistSQLScanConcurrency: userSctx.GetSessionVars().DistSQLScanConcurrency(),
		InImportInto:           true,
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

// InitTiKVConfigs initializes some TiKV related configs.
func (p *Plan) InitTiKVConfigs(ctx context.Context, sctx sessionctx.Context) error {
	isRaftKV2, err := util.IsRaftKv2(ctx, sctx)
	if err != nil {
		return err
	}
	p.IsRaftKV2 = isRaftKV2
	return nil
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
	c := &LoadDataController{
		Plan:    plan,
		ASTArgs: astArgs,
		Table:   tbl,
		logger:  logger,
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
	if e.Path == "" {
		return exeerrors.ErrLoadDataEmptyPath
	}
	if e.InImportInto {
		if e.Format != DataFormatCSV && e.Format != DataFormatParquet && e.Format != DataFormatSQL {
			return exeerrors.ErrLoadDataUnsupportedFormat.GenWithStackByArgs(e.Format)
		}
	} else {
		if e.NullValueOptEnclosed && len(e.FieldsEnclosedBy) == 0 {
			return exeerrors.ErrLoadDataWrongFormatConfig.GenWithStackByArgs("must specify FIELDS [OPTIONALLY] ENCLOSED BY when use NULL DEFINED BY OPTIONALLY ENCLOSED")
		}
		// NOTE: IMPORT INTO also don't support user set empty LinesTerminatedBy or FieldsTerminatedBy,
		// but it's check in initOptions.
		// TODO: support lines terminated is "".
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

func (p *Plan) initDefaultOptions() {
	threadCnt := runtime.GOMAXPROCS(0)
	failpoint.Inject("mockNumCpu", func(val failpoint.Value) {
		threadCnt = val.(int)
	})
	threadCnt = int(math.Max(1, float64(threadCnt)*0.5))

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
		// todo: set a max value for this param?
	}
	if _, ok := specifiedOptions[detachedOption]; ok {
		p.Detached = true
	}
	if _, ok := specifiedOptions[disableTiKVImportModeOption]; ok {
		p.DisableTiKVImportMode = true
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
	optionMap := make(map[string]interface{}, len(plan.Options))
	for _, opt := range plan.Options {
		if opt.Value != nil {
			optionMap[opt.Name] = opt.Value.String()
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

	var fileNameKey string
	if storage.IsLocal(u) {
		// LOAD DATA don't support server file.
		if !e.InImportInto {
			return exeerrors.ErrLoadDataFromServerDisk.GenWithStackByArgs(e.Path)
		}

		if !filepath.IsAbs(e.Path) {
			return exeerrors.ErrLoadDataInvalidURI.GenWithStackByArgs("file location should be absolute path when import from server disk")
		}
		// we add this check for security, we don't want user import any sensitive system files,
		// most of which is readable text file and don't have a suffix, such as /etc/passwd
		if !slices.Contains([]string{".csv", ".sql", ".parquet"}, strings.ToLower(filepath.Ext(e.Path))) {
			return exeerrors.ErrLoadDataInvalidURI.GenWithStackByArgs("the file suffix is not supported when import from server disk")
		}
		dir := filepath.Dir(e.Path)
		_, err := os.Stat(dir)
		if err != nil {
			// permission denied / file not exist error, etc.
			return exeerrors.ErrLoadDataInvalidURI.GenWithStackByArgs(err.Error())
		}

		fileNameKey = filepath.Base(e.Path)
		u.Path = dir
	} else {
		fileNameKey = strings.Trim(u.Path, "/")
		u.Path = ""
	}
	b, err2 := storage.ParseBackendFromURL(u, nil)
	if err2 != nil {
		return exeerrors.ErrLoadDataInvalidURI.GenWithStackByArgs(GetMsgFromBRError(err2))
	}
	// try to find pattern error in advance
	_, err2 = filepath.Match(stringutil.EscapeGlobExceptAsterisk(fileNameKey), "")
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
	idx := strings.IndexByte(fileNameKey, '*')
	// simple path when the path represent one file
	sourceType := e.getSourceType()
	if idx == -1 {
		fileReader, err2 := s.Open(ctx, fileNameKey)
		if err2 != nil {
			return exeerrors.ErrLoadDataCantRead.GenWithStackByArgs(GetMsgFromBRError(err2), "Please check the file location is correct")
		}
		defer func() {
			terror.Log(fileReader.Close())
		}()
		size, err3 := fileReader.Seek(0, io.SeekEnd)
		if err3 != nil {
			return exeerrors.ErrLoadDataCantRead.GenWithStackByArgs(GetMsgFromBRError(err2), "failed to read file size by seek")
		}
		compressTp := mydump.ParseCompressionOnFileExtension(fileNameKey)
		dataFiles = append(dataFiles, &mydump.SourceFileMeta{
			Path:        fileNameKey,
			FileSize:    size,
			Compression: compressTp,
			Type:        sourceType,
			// todo: if we support compression for physical mode, should set it to size * compressRatio to better split
			// engines
			RealSize: size,
		})
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
		escapedPath := stringutil.EscapeGlobExceptAsterisk(fileNameKey)
		err = s.WalkDir(ctx, &storage.WalkOption{ObjPrefix: commonPrefix, SkipSubDir: true},
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
			return exeerrors.ErrLoadDataCantRead.GenWithStackByArgs(GetMsgFromBRError(err), "failed to walk dir")
		}
	}

	e.dataStore = s
	e.dataFiles = dataFiles
	e.TotalFileSize = totalSize
	return nil
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
	Msg        string
	Affected   uint64
	Warnings   []stmtctx.SQLWarn
	ColSizeMap map[int64]int64
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
