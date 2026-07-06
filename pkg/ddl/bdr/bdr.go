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

package bdr

import (
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/types"
)

// IsAddColumnDenied checks whether BDR should reject an
// add-column operation with the given column options. It returns false for
// any role other than ast.BDRRolePrimary without evaluating the options. It
// allows adding a nullable column, or a not-null column with a default value.
func IsAddColumnDenied(role ast.BDRRole, options []*ast.ColumnOption) bool {
	if role != ast.BDRRolePrimary {
		return false
	}

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

// IsModifyColumnDenied checks whether BDR should reject a
// modify-column operation with the given field types and column options. It
// returns false for any role other than ast.BDRRolePrimary without evaluating
// the options. It allows changing the default value, or changing the default
// value together with the column comment, when the field type is unchanged.
func IsModifyColumnDenied(role ast.BDRRole, newFieldType, oldFieldType types.FieldType, options []*ast.ColumnOption) bool {
	if role != ast.BDRRolePrimary {
		return false
	}

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

// IsDenied checks whether the DDL is denied by BDR.
func IsDenied(role ast.BDRRole, action model.ActionType, args model.JobArgs) (denied bool) {
	ddlType, ok := model.ActionBDRMap[action]
	switch role {
	case ast.BDRRolePrimary:
		if !ok {
			return true
		}

		// Can't add unique index on primary role.
		if args != nil && (action == model.ActionAddIndex || action == model.ActionAddPrimaryKey) {
			if args.(*model.ModifyIndexArgs).IndexArgs[0].Unique {
				return true
			}
		}

		if ddlType == model.SafeDDL || ddlType == model.UnmanagementDDL {
			return false
		}
	case ast.BDRRoleSecondary:
		if !ok {
			return true
		}
		if ddlType == model.UnmanagementDDL {
			return false
		}
	default:
		// If user do not set bdr role, we will not deny any ddl as `none`.
		return false
	}

	return true
}
