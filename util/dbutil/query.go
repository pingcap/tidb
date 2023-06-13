// Copyright 2022 PingCAP, Inc.
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

package dbutil

import (
	"database/sql"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/format"
)

// ScanRowsToInterfaces scans rows to interface array.
func ScanRowsToInterfaces(rows *sql.Rows) ([][]interface{}, error) {
	var rowsData [][]interface{}
	cols, err := rows.Columns()
	if err != nil {
		return nil, errors.Trace(err)
	}

	for rows.Next() {
		colVals := make([]interface{}, len(cols))

		err = rows.Scan(colVals...)
		if err != nil {
			return nil, errors.Trace(err)
		}
		rowsData = append(rowsData, colVals)
	}

	return rowsData, nil
}

// ColumnData saves column's data.
type ColumnData struct {
	Data   []byte
	IsNull bool
}

// ScanRow scans rows into a map.
func ScanRow(rows *sql.Rows) (map[string]*ColumnData, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, errors.Trace(err)
	}

	colVals := make([][]byte, len(cols))
	colValsI := make([]interface{}, len(colVals))
	for i := range colValsI {
		colValsI[i] = &colVals[i]
	}

	err = rows.Scan(colValsI...)
	if err != nil {
		return nil, errors.Trace(err)
	}

	result := make(map[string]*ColumnData)
	for i := range colVals {
		data := &ColumnData{
			Data:   colVals[i],
			IsNull: colVals[i] == nil,
		}
		result[cols[i]] = data
	}

	return result, nil
}

// GetRedactedSQL returns the redacted SQL of the stmtNode.
func GetRedactedSQL(stmtNode ast.StmtNode) string {
	if st, ok := stmtNode.(*ast.ImportIntoStmt); ok {
		newNode := *st
		// path might contain sensitive information, redact it
		// if there's err during parsing, just use the original path
		if redactURL, err := storage.RedactURL(newNode.Path); err != nil {
			return newNode.Text()
		} else {
			newNode.Path = redactURL
		}
		var sb strings.Builder
		if err := newNode.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)); err != nil {
			return newNode.Text()
		}
		return sb.String()
	}
	return stmtNode.Text()
}
