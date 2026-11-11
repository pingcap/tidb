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

package importdef

import "github.com/pingcap/tidb/pkg/parser/ast"

// ImportIntoDataSource names the data source of IMPORT INTO in error messages.
const ImportIntoDataSource = "data source"

// LineFieldsInfo describes field and line formatting for data import and export.
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
