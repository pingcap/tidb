// Copyright 2026 PingCAP, Inc.
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

package importinto

import (
	"encoding/json"
	"fmt"

	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/importdef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
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
)

// OnDupKeyMode controls the behavior when IMPORT INTO finds conflicted rows.
type OnDupKeyMode string

const (
	// OnDupKeyModeCapture keeps current behavior, i.e. remove conflicted
	// rows and capture them for later inspection.
	OnDupKeyModeCapture OnDupKeyMode = "capture"
	// OnDupKeyModeError means fail on first conflict.
	OnDupKeyModeError OnDupKeyMode = "error"
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

// Plan describes the plan of LOAD DATA and IMPORT INTO.
type Plan struct {
	QuerySource *QuerySource `json:",omitempty"`
	DBName      string
	DBID        int64
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

	// LocationID is used to convert time type for parquet, see
	// https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#timestamp
	LocationID string

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
	importdef.LineFieldsInfo
	IgnoreLines uint64

	DiskQuota             config.ByteSize
	Checksum              config.PostOpLevel
	ThreadCnt             int
	MaxNodeCnt            int
	MaxWriteSpeed         config.ByteSize
	SplitFile             bool
	MaxRecordedErrors     int64
	OnDupKey              OnDupKeyMode
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
	SpecifiedOptionNames map[string]struct{} `json:",omitempty"`
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
	// UseNewCollate captures whether the new collation implementation was enabled
	// in the submitting keyspace. Import execution may happen in another keyspace,
	// so key and expression encoding must use this captured value instead of the
	// executor process default. Nil means old metadata and should fall back to the
	// caller-provided default.
	UseNewCollate *bool `json:"use_new_collate,omitempty"`
}

// GetOnDupKeyMode returns the conflict handling mode.
// For task metadata generated before this option was introduced, the value is
// empty.
// Note: currently it's not possible to have other unknown values, so we don't
// handle that case here.
func (p *Plan) GetOnDupKeyMode() OnDupKeyMode {
	if p.OnDupKey == "" {
		return OnDupKeyModeError
	}
	return p.OnDupKey
}

// GetUseNewCollateOrDefault returns the captured new-collation mode, or
// defaultVal for import metadata generated before the field existed.
func (p *Plan) GetUseNewCollateOrDefault(defaultVal bool) bool {
	if p.UseNewCollate == nil {
		return defaultVal
	}
	return *p.UseNewCollate
}

// ImportParameters is the parameters for import into statement.
// it's a minimal meta info to store in tidb_import_jobs for diagnose.
// for detailed info, see tidb_global_tasks.
type ImportParameters struct {
	ColumnsAndVars string `json:"columns-and-vars,omitempty"`
	SetClause      string `json:"set-clause,omitempty"`
	// for s3 URL, AK/SK is redacted for security
	FileLocation string `json:"file-location"`
	Format       string `json:"format"`
	// only include what user specified, not include default value.
	Options map[string]any `json:"options,omitempty"`
}

var _ fmt.Stringer = &ImportParameters{}

// String implements fmt.Stringer interface.
func (ip *ImportParameters) String() string {
	b, _ := json.Marshal(ip)
	return string(b)
}

// IsLocalSort returns true if we sort data on local disk.
func (p *Plan) IsLocalSort() bool {
	return p.CloudStorageURI == ""
}

// IsGlobalSort returns true if we sort data on global storage.
func (p *Plan) IsGlobalSort() bool {
	return !p.IsLocalSort()
}

// CheckNonCSVFormatOptions non CSV format should not specify CSV only options,
// we check it again if the format is detected automatically.
func (p *Plan) CheckNonCSVFormatOptions() error {
	for k := range p.SpecifiedOptionNames {
		if IsCSVOnlyOption(k) {
			return exeerrors.ErrLoadDataUnsupportedOption.FastGenByArgs(k, "non-CSV format")
		}
	}
	return nil
}

// IsCSVOnlyOption reports whether an option requires a delimited input format.
func IsCSVOnlyOption(name string) bool {
	switch name {
	case "character_set", "fields_terminated_by", "fields_enclosed_by", "fields_escaped_by", "fields_defined_null_by", "lines_terminated_by", "skip_rows", "split_file":
		return true
	}
	return false
}
