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
	"fmt"
	"math"
	"runtime"
	"strings"

	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/dbterror/exeerrors"
)

const (
	// LoadDataFormatDelimitedData delimited data.
	LoadDataFormatDelimitedData = "delimited data"
	// LoadDataFormatSQLDump represents the data source file of LOAD DATA is mydumper-format DML file.
	LoadDataFormatSQLDump = "sql file"
	// LoadDataFormatParquet represents the data source file of LOAD DATA is parquet.
	LoadDataFormatParquet = "parquet"

	// LogicalImportMode represents the import mode is SQL-like.
	LogicalImportMode   = "logical"  // tidb backend
	physicalImportMode  = "physical" // local backend
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

// FieldMapping indicates the relationship between input field and table column or user variable
type FieldMapping struct {
	Column  *table.Column
	UserVar *ast.VariableExpr
}

// LoadDataController load data controller.
// todo: need a better name
type LoadDataController struct {
	Path string

	Format             string
	ColumnsAndUserVars []*ast.ColumnNameOrUserVar
	ColumnAssignments  []*ast.Assignment
	OnDuplicate        ast.OnDuplicateKeyHandlingType

	Table      table.Table
	SchemaName string

	// how input field(or input column) from data file is mapped, either to a column or variable.
	// if there's NO column list clause in load data statement, then it's table's columns
	// else it's user defined list.
	fieldMappings []*FieldMapping
	// see InsertValues.insertColumns
	// todo: our behavior is different with mysql. such as for table t(a,b)
	// - "...(a,a) set a=100" is allowed in mysql, but not in tidb
	// - "...(a,b) set b=100" will set b=100 in mysql, but in tidb the set is ignored.
	insertColumns []*table.Column

	// used for DELIMITED DATA format
	FieldNullDef         []string
	NullValueOptEnclosed bool
	plannercore.LineFieldsInfo
	IgnoreLines uint64

	// import options
	importMode        string
	diskQuota         config.ByteSize
	checksum          config.PostOpLevel
	addIndex          bool
	analyze           config.PostOpLevel
	threadCnt         int64
	batchSize         int64
	maxWriteSpeed     config.ByteSize // per second
	splitFile         bool
	maxRecordedErrors int64 // -1 means record all error
	Detached          bool
}

// NewLoadDataController create new controller.
func NewLoadDataController(sctx sessionctx.Context, plan *plannercore.LoadData, tbl table.Table) (*LoadDataController, error) {
	var format string
	if plan.Format != nil {
		format = strings.ToLower(*plan.Format)
	} else {
		// without FORMAT 'xxx' clause, default to DELIMITED DATA
		format = LoadDataFormatDelimitedData
	}
	c := &LoadDataController{
		Path:               plan.Path,
		Format:             format,
		ColumnsAndUserVars: plan.ColumnsAndUserVars,
		ColumnAssignments:  plan.ColumnAssignments,
		OnDuplicate:        plan.OnDuplicate,
		SchemaName:         plan.Table.Schema.O,
		Table:              tbl,
		LineFieldsInfo:     plannercore.NewLineFieldsInfo(plan.FieldsInfo, plan.LinesInfo),
	}
	if err := c.initFieldParams(plan); err != nil {
		return nil, err
	}
	if err := c.initOptions(sctx, plan.Options); err != nil {
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

func (e *LoadDataController) initDefaultOptions() {
	threadCnt := runtime.NumCPU()
	if e.Format == LoadDataFormatParquet {
		threadCnt = int(math.Max(1, float64(threadCnt)*0.75))
	}

	e.importMode = LogicalImportMode
	_ = e.diskQuota.UnmarshalText([]byte("50GiB")) // todo confirm with pm
	e.checksum = config.OpLevelRequired
	e.addIndex = true
	e.analyze = config.OpLevelOptional
	e.threadCnt = int64(threadCnt)
	e.batchSize = 1000
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
		if v != LogicalImportMode && v != physicalImportMode {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		e.importMode = v
	}

	if e.importMode == LogicalImportMode {
		// some options are only allowed in physical mode
		for _, opt := range specifiedOptions {
			if _, ok := optionsForPhysicalImport[opt.Name]; ok {
				return exeerrors.ErrLoadDataUnsupportedOption.FastGenByArgs(opt.Name, e.importMode)
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
		e.threadCnt, isNull, err = opt.Value.EvalInt(seCtx, chunk.Row{})
		if err != nil || isNull || e.threadCnt <= 0 {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
	}
	if opt, ok := specifiedOptions[batchSizeOption]; ok {
		e.batchSize, isNull, err = opt.Value.EvalInt(seCtx, chunk.Row{})
		if err != nil || isNull || e.batchSize < 0 {
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
	if e.threadCnt > numCPU {
		e.threadCnt = numCPU
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
	tableCols := e.Table.Cols()

	if len(e.ColumnsAndUserVars) == 0 {
		for _, v := range tableCols {
			fieldMapping := &FieldMapping{
				Column: v,
			}
			e.fieldMappings = append(e.fieldMappings, fieldMapping)
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
		e.fieldMappings = append(e.fieldMappings, fieldMapping)
	}

	return columns
}

// initLoadColumns sets columns which the input fields loaded to.
func (e *LoadDataController) initLoadColumns(columnNames []string) error {
	var cols []*table.Column
	var missingColName string
	var err error
	tableCols := e.Table.Cols()

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
			e.insertColumns = append(e.insertColumns, col)
		}
	}

	// e.insertColumns is appended according to the original tables' column sequence.
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

// reorderColumns reorder the e.insertColumns according to the order of columnNames
// Note: We must ensure there must be one-to-one mapping between e.insertColumns and columnNames in terms of column name.
func (e *LoadDataController) reorderColumns(columnNames []string) error {
	cols := e.insertColumns

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

	e.insertColumns = reorderedColumns

	return nil
}

// GetInsertColumns get column list need to insert into target table.
// this list include all columns and in the same order as in fieldMappings and ColumnAssignments
func (e *LoadDataController) GetInsertColumns() []*table.Column {
	return e.insertColumns
}

// GetFieldMapping get field mapping.
func (e *LoadDataController) GetFieldMapping() []*FieldMapping {
	return e.fieldMappings
}

// GetFieldCount get field count.
func (e *LoadDataController) GetFieldCount() int {
	return len(e.fieldMappings)
}

// GetBatchSize get batch size.
func (e *LoadDataController) GetBatchSize() int64 {
	return e.batchSize
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
