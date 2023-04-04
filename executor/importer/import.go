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
	"unicode/utf8"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	litlog "github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	"github.com/pingcap/tidb/br/pkg/storage"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/util/intest"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/stringutil"
	kvconfig "github.com/tikv/client-go/v2/config"
	"go.uber.org/zap"
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
	PhysicalImportMode  = "physical"
	unlimitedWriteSpeed = config.ByteSize(math.MaxInt64)
	minDiskQuota        = config.ByteSize(10 << 30) // 10GiB
	minWriteSpeed       = config.ByteSize(1 << 10)  // 1KiB/s

	importModeOption    = "import_mode"
	diskQuotaOption     = "disk_quota"
	checksumOption      = "checksum_table"
	addIndexOption      = "add_index"
	analyzeOption       = "analyze_table"
	threadOption        = "thread"
	batchSizeOption     = "batch_size"
	maxWriteSpeedOption = "max_write_speed"
	splitFileOption     = "split_file"
	recordErrorsOption  = "record_errors"
)

var (
	detachedOption = plannercore.DetachedOption

	// name -> whether the option has value
	supportedOptions = map[string]bool{
		importModeOption:    true,
		diskQuotaOption:     true,
		checksumOption:      true,
		addIndexOption:      true,
		analyzeOption:       true,
		threadOption:        true,
		batchSizeOption:     true,
		maxWriteSpeedOption: true,
		splitFileOption:     true,
		recordErrorsOption:  true,
		detachedOption:      false,
	}

	// options only allowed when import mode is physical
	optionsForPhysicalImport = map[string]struct{}{
		diskQuotaOption: {},
		checksumOption:  {},
		addIndexOption:  {},
		analyzeOption:   {},
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

// LoadDataController load data controller.
// todo: need a better name
type LoadDataController struct {
	FileLocRef         ast.FileLocRefTp
	Path               string
	Format             string
	ColumnsAndUserVars []*ast.ColumnNameOrUserVar
	ColumnAssignments  []*ast.Assignment
	OnDuplicate        ast.OnDuplicateKeyHandlingType

	Table  table.Table
	DBName string
	DBID   int64

	// how input field(or input column) from data file is mapped, either to a column or variable.
	// if there's NO column list clause in load data statement, then it's table's columns
	// else it's user defined list.
	FieldMappings []*FieldMapping
	// see InsertValues.InsertColumns
	// todo: our behavior is different with mysql. such as for table t(a,b)
	// - "...(a,a) set a=100" is allowed in mysql, but not in tidb
	// - "...(a,b) set b=100" will set b=100 in mysql, but in tidb the set is ignored.
	InsertColumns []*table.Column
	// Data interpretation is restrictive if the SQL mode is restrictive and neither
	// the IGNORE nor the LOCAL modifier is specified. Errors terminate the load
	// operation.
	// ref https://dev.mysql.com/doc/refman/8.0/en/load-data.html#load-data-column-assignments
	Restrictive bool

	// used for DELIMITED DATA format
	FieldNullDef         []string
	NullValueOptEnclosed bool
	plannercore.LineFieldsInfo
	IgnoreLines uint64

	// import options
	ImportMode        string
	diskQuota         config.ByteSize
	checksum          config.PostOpLevel
	addIndex          bool
	analyze           config.PostOpLevel
	ThreadCnt         int64
	BatchSize         int64
	maxWriteSpeed     config.ByteSize // per second
	splitFile         bool
	maxRecordedErrors int64 // -1 means record all error
	Detached          bool

	logger           *zap.Logger
	sqlMode          mysql.SQLMode
	charset          *string
	importantSysVars map[string]string
	dataStore        storage.ExternalStorage
	dataFiles        []*mydump.SourceFileMeta
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

// NewLoadDataController create new controller.
func NewLoadDataController(userSctx sessionctx.Context, plan *plannercore.LoadData, tbl table.Table) (*LoadDataController, error) {
	fullTableName := common.UniqueTable(plan.Table.Schema.L, plan.Table.Name.L)
	logger := log.L().With(zap.String("table", fullTableName))
	var format string
	if plan.Format != nil {
		format = strings.ToLower(*plan.Format)
	} else {
		// without FORMAT 'xxx' clause, default to DELIMITED DATA
		format = LoadDataFormatDelimitedData
	}
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
	c := &LoadDataController{
		FileLocRef:         plan.FileLocRef,
		Path:               plan.Path,
		Format:             format,
		ColumnsAndUserVars: plan.ColumnsAndUserVars,
		ColumnAssignments:  plan.ColumnAssignments,
		OnDuplicate:        plan.OnDuplicate,
		DBName:             plan.Table.Schema.O,
		DBID:               plan.Table.DBInfo.ID,
		Table:              tbl,
		LineFieldsInfo:     plannercore.NewLineFieldsInfo(plan.FieldsInfo, plan.LinesInfo),
		Restrictive:        restrictive,

		logger:           logger,
		sqlMode:          userSctx.GetSessionVars().SQLMode,
		charset:          charset,
		importantSysVars: getImportantSysVars(userSctx),
	}
	if err := c.initFieldParams(plan); err != nil {
		return nil, err
	}
	if err := c.initOptions(userSctx, plan.Options); err != nil {
		return nil, err
	}

	columnNames := c.initFieldMappings()
	if err := c.initLoadColumns(columnNames); err != nil {
		return nil, err
	}
	return c, nil
}

func (e *LoadDataController) initFieldParams(plan *plannercore.LoadData) error {
	if e.Path == "" {
		return exeerrors.ErrLoadDataEmptyPath
	}
	if e.Format != LoadDataFormatDelimitedData && e.Format != LoadDataFormatParquet && e.Format != LoadDataFormatSQLDump {
		return exeerrors.ErrLoadDataUnsupportedFormat.GenWithStackByArgs(e.Format)
	}

	if e.FileLocRef == ast.FileLocClient && e.Format == LoadDataFormatParquet {
		// parquet parser need seek around, it's not supported for client local file
		return exeerrors.ErrLoadParquetFromLocal
	}

	if e.Format != LoadDataFormatDelimitedData {
		if plan.FieldsInfo != nil || plan.LinesInfo != nil || plan.IgnoreLines != nil {
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
	if plan.FieldsInfo != nil && plan.FieldsInfo.DefinedNullBy != nil {
		nullDef = append(nullDef, *plan.FieldsInfo.DefinedNullBy)
		nullValueOptEnclosed = plan.FieldsInfo.NullValueOptEnclosed
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
	if len(e.LinesTerminatedBy) == 0 {
		return exeerrors.ErrLoadDataWrongFormatConfig.GenWithStackByArgs("LINES TERMINATED BY is empty")
	}
	if len(e.FieldsEnclosedBy) > 0 &&
		(strings.HasPrefix(e.FieldsEnclosedBy, e.FieldsTerminatedBy) || strings.HasPrefix(e.FieldsTerminatedBy, e.FieldsEnclosedBy)) {
		return exeerrors.ErrLoadDataWrongFormatConfig.GenWithStackByArgs("FIELDS ENCLOSED BY and TERMINATED BY must not be prefix of each other")
	}

	return nil
}

var ignoreInTest = false

func (e *LoadDataController) initDefaultOptions() {
	threadCnt := runtime.NumCPU()
	if intest.InTest && !ignoreInTest {
		threadCnt = 1
	}
	if e.Format == LoadDataFormatParquet {
		threadCnt = int(math.Max(1, float64(threadCnt)*0.75))
	}

	e.ImportMode = LogicalImportMode
	_ = e.diskQuota.UnmarshalText([]byte("50GiB")) // todo confirm with pm
	e.checksum = config.OpLevelRequired
	e.addIndex = true
	e.analyze = config.OpLevelOptional
	e.ThreadCnt = int64(threadCnt)
	e.BatchSize = 1000
	e.maxWriteSpeed = unlimitedWriteSpeed
	e.splitFile = false
	e.maxRecordedErrors = 100
	e.Detached = false
}

func (e *LoadDataController) initOptions(seCtx sessionctx.Context, options []*plannercore.LoadDataOpt) error {
	e.initDefaultOptions()

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
		e.ImportMode = v
	}

	if e.ImportMode == LogicalImportMode {
		// some options are only allowed in physical mode
		for _, opt := range specifiedOptions {
			if _, ok := optionsForPhysicalImport[opt.Name]; ok {
				return exeerrors.ErrLoadDataUnsupportedOption.FastGenByArgs(opt.Name, e.ImportMode)
			}
		}
	}
	if opt, ok := specifiedOptions[diskQuotaOption]; ok {
		v, isNull, err = opt.Value.EvalString(seCtx, chunk.Row{})
		if err != nil || isNull {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		if err = e.diskQuota.UnmarshalText([]byte(v)); err != nil || e.diskQuota <= 0 {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
	}
	if opt, ok := specifiedOptions[checksumOption]; ok {
		v, isNull, err = opt.Value.EvalString(seCtx, chunk.Row{})
		if err != nil || isNull {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		if err = e.checksum.FromStringValue(v); err != nil {
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
		e.addIndex = vInt == 1
	}
	if opt, ok := specifiedOptions[analyzeOption]; ok {
		v, isNull, err = opt.Value.EvalString(seCtx, chunk.Row{})
		if err != nil || isNull {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		if err = e.analyze.FromStringValue(v); err != nil {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
	}
	if opt, ok := specifiedOptions[threadOption]; ok {
		// boolean true will be taken as 1
		e.ThreadCnt, isNull, err = opt.Value.EvalInt(seCtx, chunk.Row{})
		if err != nil || isNull || e.ThreadCnt <= 0 {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
	}
	if opt, ok := specifiedOptions[batchSizeOption]; ok {
		e.BatchSize, isNull, err = opt.Value.EvalInt(seCtx, chunk.Row{})
		if err != nil || isNull || e.BatchSize < 0 {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
	}
	if opt, ok := specifiedOptions[maxWriteSpeedOption]; ok {
		v, isNull, err = opt.Value.EvalString(seCtx, chunk.Row{})
		if err != nil || isNull {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		if err = e.maxWriteSpeed.UnmarshalText([]byte(v)); err != nil || e.maxWriteSpeed <= 0 {
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
		e.splitFile = vInt == 1
	}
	if opt, ok := specifiedOptions[recordErrorsOption]; ok {
		e.maxRecordedErrors, isNull, err = opt.Value.EvalInt(seCtx, chunk.Row{})
		if err != nil || isNull || e.maxRecordedErrors < -1 {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		// todo: set a max value for this param?
	}
	if _, ok := specifiedOptions[detachedOption]; ok {
		e.Detached = true
	}

	e.adjustOptions()
	return nil
}

func (e *LoadDataController) adjustOptions() {
	if e.diskQuota < minDiskQuota {
		e.diskQuota = minDiskQuota
	}
	// max value is cpu-count
	numCPU := int64(runtime.NumCPU())
	if e.ThreadCnt > numCPU {
		e.ThreadCnt = numCPU
	}
	if e.maxWriteSpeed < minWriteSpeed {
		e.maxWriteSpeed = minWriteSpeed
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

	for _, col := range cols {
		if !col.IsGenerated() {
			// todo: should report error here, since in reorderColumns we report error if en(cols) != len(columnNames)
			e.InsertColumns = append(e.InsertColumns, col)
		}
	}

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
	return &config.CSVConfig{
		Separator: e.FieldsTerminatedBy,
		// ignore optionally enclosed
		Delimiter:        e.FieldsEnclosedBy,
		Terminator:       e.LinesTerminatedBy,
		NotNull:          false,
		Null:             e.FieldNullDef,
		Header:           false,
		TrimLastSep:      false,
		EscapedBy:        e.FieldsEscapedBy,
		StartingBy:       e.LinesStartingBy,
		AllowEmptyLine:   true,
		QuotedNullIsText: !e.NullValueOptEnclosed,
		UnescapedQuote:   true,
	}
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

	dataFiles := []*mydump.SourceFileMeta{}
	idx := strings.IndexByte(path, '*')
	// simple path when the INFILE represent one file
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
		dataFiles = append(dataFiles, &mydump.SourceFileMeta{
			Path:     path,
			FileSize: size,
		})
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
				dataFiles = append(dataFiles, &mydump.SourceFileMeta{
					Path:     remotePath,
					FileSize: size,
				})
				return nil
			})
		if err != nil {
			return err
		}
	}

	e.dataStore = s
	e.dataFiles = dataFiles
	return nil
}

// GetLoadDataReaderInfos returns the LoadDataReaderInfo for each data file.
func (e *LoadDataController) GetLoadDataReaderInfos() []LoadDataReaderInfo {
	result := make([]LoadDataReaderInfo, 0, len(e.dataFiles))
	for i := range e.dataFiles {
		f := e.dataFiles[i]
		result = append(result, LoadDataReaderInfo{
			Opener: func(ctx context.Context) (io.ReadSeekCloser, error) {
				fileReader, err2 := e.dataStore.Open(ctx, f.Path)
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
		if e.charset != nil {
			charsetConvertor, err = mydump.NewCharsetConvertor(*e.charset, string(utf8.RuneError))
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
			e.sqlMode,
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

// PhysicalImport do physical import.
func (e *LoadDataController) PhysicalImport(ctx context.Context) (int64, error) {
	// todo: implement it
	return 0, nil
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
