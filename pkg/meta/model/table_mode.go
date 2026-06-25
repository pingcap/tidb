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

package model

import "github.com/pingcap/tidb/pkg/parser/ast"

// TableMode is the state for table mode, it's a table level metadata for prevent
// table read/write during importing(import into) or BR restoring.
// when table mode isn't TableModeNormal, DMLs or DDLs that change the table will
// return error.
// To modify table mode, only internal DDL operations(AlterTableMode) are permitted.
// Now allow switching between the same table modes, and not allow convert between
// TableModeImport and TableModeRestore
type TableMode byte

const (
	// TableModeNormal means the table is in normal mode.
	TableModeNormal TableMode = iota
	// TableModeImport means the table is in import mode.
	TableModeImport
	// TableModeRestore means the table is in restore mode.
	TableModeRestore
)

// String implements fmt.Stringer interface.
func (t TableMode) String() string {
	switch t {
	case TableModeNormal:
		return "Normal"
	case TableModeImport:
		return "Import"
	case TableModeRestore:
		return "Restore"
	default:
		return ""
	}
}

// CanTransitionTo returns whether the table mode can transition from t to target.
// Now only block import/restore to convert to each other.
// TODO: Now allow switching between the same table modes, but additional validation will be added later
// to verify that only the same modification source can perform ALTER same table mode.
func (t TableMode) CanTransitionTo(target TableMode) bool {
	if t == TableModeImport && target == TableModeRestore {
		return false
	}
	if t == TableModeRestore && target == TableModeImport {
		return false
	}
	return true
}

// AlterTableModeTarget is the resolved target metadata for an AlterTableMode job.
type AlterTableModeTarget struct {
	SchemaID    int64
	SchemaName  ast.CIStr
	TableID     int64
	TableName   ast.CIStr
	CurrentMode TableMode
	TargetMode  TableMode
}
