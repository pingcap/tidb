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
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util/dbterror"
)

// pkdbAllowedRelativeDupColsForAutoIncrementPK returns the set of column names that are allowed to
// appear in both ModifyColumns/AddColumns and RelativeColumns within a multi-schema change.
//
// This is needed for the supported pattern:
//   - ADD/MODIFY COLUMN (AUTO_INCREMENT) ... then ADD PRIMARY KEY(col) ... in the same statement.
func pkdbAllowedRelativeDupColsForAutoIncrementPK(info *model.MultiSchemaInfo) map[string]struct{} {
	keep := make(map[string]struct{})
	for _, sub := range info.SubJobs {
		switch sub.Type {
		case model.ActionAddColumn:
			args := sub.JobArgs.(*model.TableColumnArgs)
			if mysql.HasAutoIncrementFlag(args.Col.GetFlag()) {
				keep[args.Col.Name.L] = struct{}{}
			}
		case model.ActionModifyColumn:
			args := sub.JobArgs.(*model.ModifyColumnArgs)
			if mysql.HasAutoIncrementFlag(args.Column.GetFlag()) {
				keep[args.Column.Name.L] = struct{}{}
			}
		}
	}
	if len(keep) == 0 {
		return nil
	}

	allowed := make(map[string]struct{}, len(keep))
	for _, sub := range info.SubJobs {
		if sub.Type != model.ActionAddPrimaryKey {
			continue
		}
		args := sub.JobArgs.(*model.ModifyIndexArgs)
		if len(args.IndexArgs) != 1 {
			continue
		}
		for _, idxPart := range args.IndexArgs[0].IndexPartSpecifications {
			if idxPart.Column == nil {
				continue
			}
			if _, ok := keep[idxPart.Column.Name.L]; ok {
				allowed[idxPart.Column.Name.L] = struct{}{}
			}
		}
	}
	if len(allowed) == 0 {
		return nil
	}
	return allowed
}

func pkdbCheckMultiSchemaAutoIncrementWithNonclusteredPK(info *model.MultiSchemaInfo, t table.Table) error {
	if err := pkdbCheckModifyColumnAddAutoIncrementWithNonclusteredPK(info, t); err != nil {
		return err
	}
	return pkdbCheckAddColumnAddAutoIncrementWithNonclusteredPK(info, t)
}

func pkdbCheckModifyColumnAddAutoIncrementWithNonclusteredPK(info *model.MultiSchemaInfo, t table.Table) error {
	tblInfo := t.Meta()

	// Only validate the new support: enabling AUTO_INCREMENT via MODIFY COLUMN.
	// (ADD COLUMN + AUTO_INCREMENT is handled separately.)
	var targetColName string
	for _, sub := range info.SubJobs {
		if sub.Type != model.ActionModifyColumn {
			continue
		}
		args := sub.JobArgs.(*model.ModifyColumnArgs)

		oldCol := model.FindColumnInfo(tblInfo.Columns, args.OldColumnName.L)
		if oldCol == nil {
			continue
		}
		if mysql.HasAutoIncrementFlag(oldCol.GetFlag()) || !mysql.HasAutoIncrementFlag(args.Column.GetFlag()) {
			continue
		}

		// Only support modifying the same column; don't allow renaming here.
		if args.Column.Name.L != args.OldColumnName.L {
			return dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs("can't set auto_increment")
		}
		// AUTO_INCREMENT must be on an integer, and it is treated as NOT NULL.
		if !mysql.IsIntegerType(args.Column.GetType()) || !mysql.HasNotNullFlag(args.Column.GetFlag()) {
			return dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs("can't set auto_increment")
		}

		if targetColName != "" && targetColName != args.Column.Name.L {
			// Keep it simple for now; TiDB supports at most one auto_increment column anyway.
			return dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs("can't set auto_increment")
		}
		targetColName = args.Column.Name.L
	}
	if targetColName == "" {
		return nil
	}

	// TiDB supports at most one AUTO_INCREMENT column.
	if tblInfo.GetAutoIncrementColInfo() != nil {
		return dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs("can't set auto_increment")
	}

	// If the table already has a NONCLUSTERED PRIMARY KEY on this column, no need to add it in the same statement.
	// (This is the MODIFY COLUMN + existing PK pattern.)
	if !tblInfo.PKIsHandle && !tblInfo.IsCommonHandle {
		if pk := tblInfo.FindIndexByName(pmodel.NewCIStr(mysql.PrimaryKeyName).L); pk != nil &&
			len(pk.Columns) == 1 && pk.Columns[0].Name.L == targetColName {
			return nil
		}
	}

	// Require: ADD PRIMARY KEY(targetColName) NONCLUSTERED in the same multi-schema change.
	for _, sub := range info.SubJobs {
		if sub.Type != model.ActionAddPrimaryKey {
			continue
		}
		args := sub.JobArgs.(*model.ModifyIndexArgs)
		if len(args.IndexArgs) != 1 {
			continue
		}
		idxArg := args.IndexArgs[0]
		if len(idxArg.IndexPartSpecifications) != 1 {
			continue
		}
		idxPart := idxArg.IndexPartSpecifications[0]
		if idxPart.Column == nil || idxPart.Column.Name.L != targetColName {
			continue
		}
		// Adding clustered primary key via ALTER TABLE is not supported, so any primary key
		// here is effectively nonclustered. Still, reject an explicit `CLUSTERED` option.
		if idxArg.IndexOption != nil && idxArg.IndexOption.PrimaryKeyTp == pmodel.PrimaryKeyTypeClustered {
			return dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs(
				"can't set auto_increment with ADD PRIMARY KEY(" + targetColName + ") CLUSTERED",
			)
		}
		return nil
	}
	return dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs("can't set auto_increment")
}

func pkdbCheckAddColumnAddAutoIncrementWithNonclusteredPK(info *model.MultiSchemaInfo, t table.Table) error {
	tblInfo := t.Meta()

	// Only validate the new support: adding AUTO_INCREMENT via ADD COLUMN.
	var targetCol *model.ColumnInfo
	for _, sub := range info.SubJobs {
		if sub.Type != model.ActionAddColumn {
			continue
		}
		args := sub.JobArgs.(*model.TableColumnArgs)
		if !mysql.HasAutoIncrementFlag(args.Col.GetFlag()) {
			continue
		}
		// AUTO_INCREMENT must be on an integer, and it is treated as NOT NULL.
		if !mysql.IsIntegerType(args.Col.GetType()) || !mysql.HasNotNullFlag(args.Col.GetFlag()) {
			return dbterror.ErrUnsupportedAddColumn.GenWithStack(
				"unsupported add column '%s' constraint AUTO_INCREMENT", args.Col.Name.L,
			)
		}
		// Keep consistent with CREATE TABLE preprocessing: AUTO_INCREMENT column can't have a non-NULL DEFAULT value.
		if args.Col.GetDefaultValue() != nil {
			return dbterror.ErrUnsupportedAddColumn.GenWithStack(
				"unsupported add column '%s' constraint AUTO_INCREMENT", args.Col.Name.L,
			)
		}
		// Keep it simple for now; TiDB supports at most one auto_increment column anyway.
		if targetCol != nil && targetCol.Name.L != args.Col.Name.L {
			return dbterror.ErrUnsupportedAddColumn.GenWithStack(
				"unsupported add column '%s' constraint AUTO_INCREMENT", args.Col.Name.L,
			)
		}
		targetCol = args.Col
	}
	if targetCol == nil {
		return nil
	}
	targetColName := targetCol.Name.L

	if tblInfo.GetAutoIncrementColInfo() != nil {
		return dbterror.ErrUnsupportedAddColumn.GenWithStack(
			"unsupported add column '%s' constraint AUTO_INCREMENT when table already has an auto_increment column",
			targetColName,
		)
	}

	// Require: ADD PRIMARY KEY(targetColName) NONCLUSTERED in the same multi-schema change.
	for _, sub := range info.SubJobs {
		if sub.Type != model.ActionAddPrimaryKey {
			continue
		}
		args := sub.JobArgs.(*model.ModifyIndexArgs)
		if len(args.IndexArgs) != 1 {
			continue
		}
		idxArg := args.IndexArgs[0]
		if len(idxArg.IndexPartSpecifications) != 1 {
			continue
		}
		idxPart := idxArg.IndexPartSpecifications[0]
		if idxPart.Column == nil || idxPart.Column.Name.L != targetColName {
			continue
		}
		// Adding clustered primary key via ALTER TABLE is not supported, so any primary key
		// here is effectively nonclustered. Still, reject an explicit `CLUSTERED` option.
		if idxArg.IndexOption != nil && idxArg.IndexOption.PrimaryKeyTp == pmodel.PrimaryKeyTypeClustered {
			return dbterror.ErrUnsupportedAddColumn.GenWithStack(
				"unsupported add column '%s' constraint AUTO_INCREMENT with ADD PRIMARY KEY(%s) CLUSTERED",
				targetColName, targetColName,
			)
		}
		return nil
	}

	// If we get here, it means there is an AUTO_INCREMENT column being added but no NONCLUSTERED PK on it.
	// We don't want to expand support beyond the intended "auto_increment + nonclustered primary key" feature.
	return dbterror.ErrUnsupportedAddColumn.GenWithStack(
		"unsupported add column '%s' constraint AUTO_INCREMENT without ADD PRIMARY KEY(%s) NONCLUSTERED in the same statement",
		targetColName, targetColName,
	)
}
