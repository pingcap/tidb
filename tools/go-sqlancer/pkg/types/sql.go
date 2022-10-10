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

package types

// SQLType enums for SQL types
type SQLType int

// SQLTypeDMLSelect
const (
	SQLTypeUnknown SQLType = iota
	SQLTypeReloadSchema
	SQLTypeDMLSelect
	SQLTypeDMLSelectForUpdate
	SQLTypeDMLUpdate
	SQLTypeDMLInsert
	SQLTypeDMLDelete
	SQLTypeDDLCreateTable
	SQLTypeDDLAlterTable
	SQLTypeDDLCreateIndex
	SQLTypeTxnBegin
	SQLTypeTxnCommit
	SQLTypeTxnRollback
	SQLTypeExec
	SQLTypeExit
	SQLTypeSleep
	SQLTypeCreateDatabase
	SQLTypeDropDatabase
)

// SQL struct
type SQL struct {
	SQLType  SQLType
	SQLStmt  string
	SQLTable string
	// ExecTime is for avoid lock watched interference before time out
	// useful for sleep statement
	ExecTime int
}

func (t SQLType) String() string {
	switch t {
	case SQLTypeReloadSchema:
		return "SQLTypeReloadSchema"
	case SQLTypeDMLSelect:
		return "SQLTypeDMLSelect"
	case SQLTypeDMLSelectForUpdate:
		return "SQLTypeDMLSelectForUpdate"
	case SQLTypeDMLUpdate:
		return "SQLTypeDMLUpdate"
	case SQLTypeDMLInsert:
		return "SQLTypeDMLInsert"
	case SQLTypeDMLDelete:
		return "SQLTypeDMLDelete"
	case SQLTypeDDLCreateTable:
		return "SQLTypeDDLCreateTable"
	case SQLTypeDDLAlterTable:
		return "SQLTypeDDLAlterTable"
	case SQLTypeDDLCreateIndex:
		return "SQLTypeDDLCreateIndex"
	case SQLTypeTxnBegin:
		return "SQLTypeTxnBegin"
	case SQLTypeTxnCommit:
		return "SQLTypeTxnCommit"
	case SQLTypeTxnRollback:
		return "SQLTypeTxnRollback"
	case SQLTypeExec:
		return "SQLTypeExec"
	case SQLTypeExit:
		return "SQLTypeExit"
	case SQLTypeSleep:
		return "SQLTypeSleep"
	case SQLTypeCreateDatabase:
		return "SQLTypeCreateDatabase"
	case SQLTypeDropDatabase:
		return "SQLTypeDropDatabase"
	default:
		return "SQLTypeUnknown"
	}
}
