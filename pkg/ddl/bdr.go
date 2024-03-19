// Copyright 2024 PingCAP, Inc.
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

package ddl

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/types"
)

// In BDR mode(primary role), we allow add a new column to table if it is nullable
// or not null with default value.
func deniedByBDRWhenAddColumn(options []*ast.ColumnOption) bool {
	var (
		nullable     bool
		notNull      bool
		defaultValue bool
		comment      int
		generated    int
	)
	for _, opt := range options {
		switch opt.Tp {
		case ast.ColumnOptionDefaultValue:
			defaultValue = true
		case ast.ColumnOptionComment:
			comment = 1
		case ast.ColumnOptionGenerated:
			generated = 1
		case ast.ColumnOptionNotNull:
			notNull = true
		case ast.ColumnOptionNull:
			nullable = true
		}
	}
	tpLen := len(options) - comment - generated

	if tpLen == 0 || (tpLen == 1 && nullable) || (tpLen == 1 && !notNull && defaultValue) ||
		(tpLen == 2 && notNull && defaultValue) {
		return false
	}

	return true
}

// In BDR mode(primary role), we allow add or update comment for column, change default
// value of one particular column. Other modify column operations are denied.
func deniedByBDRWhenModifyColumn(newFieldType, oldFieldType types.FieldType, options []*ast.ColumnOption) bool {
	if !newFieldType.Equal(&oldFieldType) {
		return true
	}
	var (
		defaultValue bool
		comment      bool
	)
	for _, opt := range options {
		if opt.Tp == ast.ColumnOptionDefaultValue {
			defaultValue = true
		}
		if opt.Tp == ast.ColumnOptionComment {
			comment = true
		}
	}

	if len(options) == 1 && defaultValue {
		return false
	}

	if len(options) == 2 && defaultValue && comment {
		return false
	}

	return true
}
