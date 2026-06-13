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

package ddl

import (
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/util/dbterror"
)

func pkdbCheckModifyColumnEnableAutoIncrement(
	sctx sessionctx.Context,
	t table.Table,
	originalColName pmodel.CIStr,
	newCol *table.Column,
) error {
	// Multi-schema change validates the pairing (ADD/MODIFY AUTO_INCREMENT + ADD PRIMARY KEY) at statement level.
	if sctx.GetSessionVars().StmtCtx.MultiSchemaInfo != nil {
		return nil
	}

	tbInfo := t.Meta()
	if tbInfo.GetAutoIncrementColInfo() != nil {
		// TiDB supports at most one AUTO_INCREMENT column.
		return dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs("can't set auto_increment")
	}
	// Keep it simple: don't allow renaming while enabling AUTO_INCREMENT.
	if newCol.Name.L != originalColName.L {
		return dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs("can't set auto_increment")
	}
	// AUTO_INCREMENT must be on an integer, and it is treated as NOT NULL.
	if !mysql.IsIntegerType(newCol.GetType()) || !mysql.HasNotNullFlag(newCol.GetFlag()) {
		return dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs("can't set auto_increment")
	}

	// Only allow enabling AUTO_INCREMENT on an existing NONCLUSTERED PRIMARY KEY column.
	if tbInfo.PKIsHandle || tbInfo.IsCommonHandle {
		return dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs("can't set auto_increment")
	}
	pk := tables.FindPrimaryIndex(tbInfo)
	if pk == nil || len(pk.Columns) != 1 || pk.Columns[0].Name.L != originalColName.L {
		return dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs("can't set auto_increment")
	}
	return nil
}
