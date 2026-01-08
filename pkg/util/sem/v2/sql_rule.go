// Copyright 2025 PingCAP, Inc.
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

package sem

import (
	"net/url"
	"strings"

	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/parser/ast"
)

// SQLRule is a function to decide whether a SQL statement should be restricted
type SQLRule func(stmt ast.StmtNode) bool

func checkTTLOptions(options []*ast.TableOption) bool {
	for _, option := range options {
		if option.Tp == ast.TableOptionTTL ||
			option.Tp == ast.TableOptionTTLEnable ||
			option.Tp == ast.TableOptionTTLJobInterval {
			return true
		}
	}
	return false
}

var sqlRuleNameMap = map[string]SQLRule{
	"time_to_live":            TimeToLiveSQLRule,
	"alter_table_attributes":  AlterTableAttributesRule,
	"import_with_external_id": ImportWithExternalIDRule,
	"select_into_file":        SelectIntoFileRule,
	"import_from_local":       ImportFromLocalRule,
}

// TimeToLiveSQLRule returns true if the SQL statement is related to Time To Live (TTL) options.
var TimeToLiveSQLRule SQLRule = func(stmt ast.StmtNode) bool {
	switch ddlStmt := stmt.(type) {
	case *ast.CreateTableStmt:
		return checkTTLOptions(ddlStmt.Options)
	case *ast.AlterTableStmt:
		for _, spec := range ddlStmt.Specs {
			if spec.Tp == ast.AlterTableRemoveTTL {
				return true
			}
			if spec.Tp == ast.AlterTableOption {
				if checkTTLOptions(spec.Options) {
					return true
				}
			}
		}
	default:
		return false
	}

	return false
}

// AlterTableAttributesRule SQLRule returns true if the SQL statement is related to altering table attributes.
var AlterTableAttributesRule SQLRule = func(stmt ast.StmtNode) bool {
	switch ddlStmt := stmt.(type) {
	case *ast.AlterTableStmt:
		for _, spec := range ddlStmt.Specs {
			if spec.Tp == ast.AlterTableAttributes || spec.Tp == ast.AlterTablePartitionAttributes {
				return true
			}
		}
	default:
		return false
	}

	return false
}

// ImportWithExternalIDRule SQLRule returns true if the SQL statement is related to importing data with an external ID.
var ImportWithExternalIDRule SQLRule = func(stmt ast.StmtNode) bool {
	switch importStmt := stmt.(type) {
	case *ast.ImportIntoStmt:
		u, err := url.Parse(importStmt.Path)
		if err != nil {
			return false
		}
		if objstore.IsS3(u) {
			values := u.Query()
			for k := range values {
				lowerK := strings.ToLower(k)
				if lowerK == objstore.S3ExternalID {
					return true
				}
			}
		}
	default:
		return false
	}

	return false
}

// SelectIntoFileRule SQLRule returns true if the SQL statement is a SELECT INTO OUTFILE statement.
var SelectIntoFileRule SQLRule = func(stmt ast.StmtNode) bool {
	if selectStmt, ok := stmt.(*ast.SelectStmt); ok {
		return selectStmt.SelectIntoOpt != nil
	}
	return false
}

// ImportFromLocalRule SQLRule returns true if the SQL statement used `IMPORT INTO` or `LOAD DATA ... INFILE ...` with a local file.
var ImportFromLocalRule SQLRule = func(stmt ast.StmtNode) bool {
	switch stmt := stmt.(type) {
	case *ast.ImportIntoStmt:
		// Allow `IMPORT INTO ... FROM SELECT ...`, whose path is empty.
		if stmt.Select != nil {
			return false
		}
		u, err := url.Parse(stmt.Path)
		if err != nil {
			return false
		}
		return objstore.IsLocal(u)
	case *ast.LoadDataStmt:
		if stmt.FileLocRef == ast.FileLocClient {
			return false
		}

		u, err := url.Parse(stmt.Path)
		if err != nil {
			return false
		}

		return objstore.IsLocal(u)
	}

	return false
}
