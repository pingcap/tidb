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
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/expression/exprctx"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/filter"
	"github.com/pingcap/tidb/pkg/util/intest"
)

func checkModifyColumnWithGeneratedColumnsConstraint(allCols []*table.Column, oldColName ast.CIStr) error {
	for _, col := range allCols {
		if col.GeneratedExpr == nil {
			continue
		}
		dependedColNames := FindColumnNamesInExpr(col.GeneratedExpr.Internal())
		for _, name := range dependedColNames {
			if name.Name.L == oldColName.L {
				if col.Hidden {
					return dbterror.ErrDependentByFunctionalIndex.GenWithStackByArgs(oldColName.O)
				}
				return dbterror.ErrDependentByGeneratedColumn.GenWithStackByArgs(oldColName.O)
			}
		}
	}
	return nil
}

func preCheckPartitionModifiableColumn(sctx sessionctx.Context, t table.Table, col, newCol *table.Column) error {
	// Check that the column change does not affect the partitioning column
	// It must keep the same type, int [unsigned], [var]char, date[time]
	if t.Meta().Partition != nil {
		pt, ok := t.(table.PartitionedTable)
		if !ok {
			// Should never happen!
			return dbterror.ErrNotAllowedTypeInPartition.GenWithStackByArgs(newCol.Name.O)
		}
		for _, name := range pt.GetPartitionColumnNames() {
			if strings.EqualFold(name.L, col.Name.L) {
				return checkPartitionColumnModifiable(sctx, t.Meta(), col.ColumnInfo, newCol.ColumnInfo)
			}
		}
	}
	return nil
}

func postCheckPartitionModifiableColumn(w *worker, tblInfo *model.TableInfo, col, newCol *model.ColumnInfo) error {
	// Check that the column change does not affect the partitioning column
	// It must keep the same type, int [unsigned], [var]char, date[time]
	if tblInfo.Partition != nil {
		sctx, err := w.sessPool.Get()
		if err != nil {
			return err
		}
		defer w.sessPool.Put(sctx)
		if len(tblInfo.Partition.Columns) > 0 {
			for _, pc := range tblInfo.Partition.Columns {
				if strings.EqualFold(pc.L, col.Name.L) {
					err := checkPartitionColumnModifiable(sctx, tblInfo, col, newCol)
					if err != nil {
						return errors.Trace(err)
					}
				}
			}
			return nil
		}
		partCols, err := extractPartitionColumns(tblInfo.Partition.Expr, tblInfo)
		if err != nil {
			return errors.Trace(err)
		}
		var partCol *model.ColumnInfo
		for _, pc := range partCols {
			if strings.EqualFold(pc.Name.L, col.Name.L) {
				partCol = pc
				break
			}
		}
		if partCol != nil {
			return checkPartitionColumnModifiable(sctx, tblInfo, col, newCol)
		}
	}
	return nil
}

func checkPartitionColumnModifiable(sctx sessionctx.Context, tblInfo *model.TableInfo, col, newCol *model.ColumnInfo) error {
	if col.Name.L != newCol.Name.L {
		return dbterror.ErrDependentByPartitionFunctional.GenWithStackByArgs(col.Name.L)
	}
	if !isColTypeAllowedAsPartitioningCol(tblInfo.Partition.Type, newCol.FieldType) {
		return dbterror.ErrNotAllowedTypeInPartition.GenWithStackByArgs(newCol.Name.O)
	}
	pi := tblInfo.GetPartitionInfo()
	if len(pi.Columns) == 0 {
		// non COLUMNS partitioning, only checks INTs, not their actual range
		// There are many edge cases, like when truncating SQL Mode is allowed
		// which will change the partitioning expression value resulting in a
		// different partition. Better be safe and not allow decreasing of length.
		// TODO: Should we allow it in strict mode? Wait for a use case / request.
		if newCol.FieldType.GetFlen() < col.FieldType.GetFlen() {
			return dbterror.ErrUnsupportedModifyColumn.GenWithStack("Unsupported modify column, decreasing length of int may result in truncation and change of partition")
		}
	}
	// Basically only allow changes of the length/decimals for the column
	// Note that enum is not allowed, so elems are not checked
	// TODO: support partition by ENUM
	if newCol.FieldType.EvalType() != col.FieldType.EvalType() ||
		newCol.FieldType.GetFlag() != col.FieldType.GetFlag() ||
		newCol.FieldType.GetCollate() != col.FieldType.GetCollate() ||
		newCol.FieldType.GetCharset() != col.FieldType.GetCharset() {
		return dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs("can't change the partitioning column, since it would require reorganize all partitions")
	}
	// Generate a new PartitionInfo and validate it together with the new column definition
	// Checks if all partition definition values are compatible.
	// Similar to what buildRangePartitionDefinitions would do in terms of checks.

	newTblInfo := *tblInfo
	// Replace col with newCol and see if we can generate a new SHOW CREATE TABLE
	// and reparse it and build new partition definitions (which will do additional
	// checks columns vs partition definition values
	newCols := make([]*model.ColumnInfo, 0, len(newTblInfo.Columns))
	for _, c := range newTblInfo.Columns {
		if c.ID == col.ID {
			newCols = append(newCols, newCol)
			continue
		}
		newCols = append(newCols, c)
	}
	newTblInfo.Columns = newCols

	var buf bytes.Buffer
	AppendPartitionInfo(tblInfo.GetPartitionInfo(), &buf, mysql.ModeNone)
	// Ignoring warnings
	stmt, _, err := parser.New().ParseSQL("ALTER TABLE t " + buf.String())
	if err != nil {
		// Should never happen!
		return dbterror.ErrUnsupportedModifyColumn.GenWithStack("cannot parse generated PartitionInfo")
	}
	at, ok := stmt[0].(*ast.AlterTableStmt)
	if !ok || len(at.Specs) != 1 || at.Specs[0].Partition == nil {
		return dbterror.ErrUnsupportedModifyColumn.GenWithStack("cannot parse generated PartitionInfo")
	}
	pAst := at.Specs[0].Partition
	_, err = buildPartitionDefinitionsInfo(
		exprctx.CtxWithHandleTruncateErrLevel(sctx.GetExprCtx(), errctx.LevelError),
		pAst.Definitions, &newTblInfo, uint64(len(newTblInfo.Partition.Definitions)),
	)
	if err != nil {
		return dbterror.ErrUnsupportedModifyColumn.GenWithStack("New column does not match partition definitions: %s", err.Error())
	}
	return nil
}

var colStateOrd = map[model.SchemaState]int{
	model.StateNone:                 0,
	model.StateDeleteOnly:           1,
	model.StateDeleteReorganization: 2,
	model.StateWriteOnly:            3,
	model.StateWriteReorganization:  4,
	model.StatePublic:               5,
}

func checkTableInfo(tblInfo *model.TableInfo) {
	if !intest.InTest {
		return
	}

	// Check columns' order by state.
	minState := model.StatePublic
	for _, col := range tblInfo.Columns {
		if colStateOrd[col.State] < colStateOrd[minState] {
			minState = col.State
		} else if colStateOrd[col.State] > colStateOrd[minState] {
			intest.Assert(false, fmt.Sprintf("column %s state %s is not in order, expect at least %s", col.Name, col.State, minState))
		}
		if col.ChangeStateInfo != nil {
			offset := col.ChangeStateInfo.DependencyColumnOffset
			intest.Assert(offset >= 0 && offset < len(tblInfo.Columns))
			depCol := tblInfo.Columns[offset]
			switch {
			case col.IsChanging():
				name := col.GetChangingOriginName()
				intest.Assert(name == depCol.Name.O, "%s != %s", name, depCol.Name.O)
			case depCol.IsRemoving():
				name := depCol.GetRemovingOriginName()
				intest.Assert(name == col.Name.O, "%s != %s", name, col.Name.O)
			}
		}
	}

	// Check index names' uniqueness.
	allNames := make(map[string]struct{})
	for _, idx := range tblInfo.Indices {
		_, exists := allNames[idx.Name.O]
		intest.Assert(!exists, "duplicate index name %s", idx.Name.O)
		allNames[idx.Name.O] = struct{}{}
	}
}

// GetModifiableColumnJob returns a DDL job of model.ActionModifyColumn.
func GetModifiableColumnJob(
	ctx context.Context,
	sctx sessionctx.Context,
	is infoschema.InfoSchema, // WARN: is maybe nil here.
	ident ast.Ident,
	originalColName ast.CIStr,
	schema *model.DBInfo,
	t table.Table,
	spec *ast.AlterTableSpec,
) (*JobWrapper, error) {
	var err error
	specNewColumn := spec.NewColumns[0]

	col := table.FindCol(t.Cols(), originalColName.L)
	if col == nil {
		return nil, infoschema.ErrColumnNotExists.GenWithStackByArgs(originalColName, ident.Name)
	}
	err = checkColumnReferencedByPartialCondition(t.Meta(), col.ColumnInfo.Name)
	if err != nil {
		return nil, errors.Trace(err)
	}
	newColName := specNewColumn.Name.Name
	if newColName.L == model.ExtraHandleName.L {
		return nil, dbterror.ErrWrongColumnName.GenWithStackByArgs(newColName.L)
	}
	errG := checkModifyColumnWithGeneratedColumnsConstraint(t.Cols(), originalColName)

	// If we want to rename the column name, we need to check whether it already exists.
	if newColName.L != originalColName.L {
		c := table.FindCol(t.Cols(), newColName.L)
		if c != nil {
			return nil, infoschema.ErrColumnExists.GenWithStackByArgs(newColName)
		}

		// And also check the generated columns dependency, if some generated columns
		// depend on this column, we can't rename the column name.
		if errG != nil {
			return nil, errors.Trace(errG)
		}
	}

	// Constraints in the new column means adding new constraints. Errors should thrown,
	// which will be done by `processColumnOptions` later.
	if specNewColumn.Tp == nil {
		// Make sure the column definition is simple field type.
		return nil, errors.Trace(dbterror.ErrUnsupportedModifyColumn)
	}

	if err = checkColumnAttributes(specNewColumn.Name.OrigColName(), specNewColumn.Tp); err != nil {
		return nil, errors.Trace(err)
	}

	newCol := table.ToColumn(&model.ColumnInfo{
		ID: col.ID,
		// We use this PR(https://github.com/pingcap/tidb/pull/6274) as the dividing line to define whether it is a new version or an old version TiDB.
		// The old version TiDB initializes the column's offset and state here.
		// The new version TiDB doesn't initialize the column's offset and state, and it will do the initialization in run DDL function.
		// When we do the rolling upgrade the following may happen:
		// a new version TiDB builds the DDL job that doesn't be set the column's offset and state,
		// and the old version TiDB is the DDL owner, it doesn't get offset and state from the store. Then it will encounter errors.
		// So here we set offset and state to support the rolling upgrade.
		Offset:                col.Offset,
		State:                 col.State,
		OriginDefaultValue:    col.OriginDefaultValue,
		OriginDefaultValueBit: col.OriginDefaultValueBit,
		FieldType:             *specNewColumn.Tp,
		Name:                  newColName,
		Version:               col.Version,
	})

	if err = ProcessColumnCharsetAndCollation(NewMetaBuildContextWithSctx(sctx), col, newCol, t.Meta(), specNewColumn, schema); err != nil {
		return nil, err
	}

	if err = checkModifyColumnWithForeignKeyConstraint(is, schema.Name.L, t.Meta(), col.ColumnInfo, newCol.ColumnInfo); err != nil {
		return nil, errors.Trace(err)
	}

	// Copy index related options to the new spec.
	indexFlags := col.FieldType.GetFlag() & (mysql.PriKeyFlag | mysql.UniqueKeyFlag | mysql.MultipleKeyFlag)
	newCol.FieldType.AddFlag(indexFlags)
	if mysql.HasPriKeyFlag(col.FieldType.GetFlag()) {
		newCol.FieldType.AddFlag(mysql.NotNullFlag)
		// TODO: If user explicitly set NULL, we should throw error ErrPrimaryCantHaveNull.
	}

	if err = ProcessModifyColumnOptions(sctx, newCol, specNewColumn.Options); err != nil {
		return nil, errors.Trace(err)
	}

	if err = checkModifyTypes(col.ColumnInfo, newCol.ColumnInfo, isColumnWithIndex(col.Name.L, t.Meta().Indices)); err != nil {
		return nil, errors.Trace(err)
	}
	mayNeedChangeColData := !noReorgDataStrict(t.Meta(), col.ColumnInfo, newCol.ColumnInfo)
	if mayNeedChangeColData {
		if err = isGeneratedRelatedColumn(t.Meta(), newCol.ColumnInfo, col.ColumnInfo); err != nil {
			return nil, errors.Trace(err)
		}
		if isColumnarIndexColumn(t.Meta(), col.ColumnInfo) {
			return nil, dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs("columnar indexes on the column")
		}
		// new col's origin default value be the same as the new default value.
		originDefVal, err := generateOriginDefaultValue(newCol.ColumnInfo, sctx, false)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if err = newCol.ColumnInfo.SetOriginDefaultValue(originDefVal); err != nil {
			return nil, dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs("new column set origin default value failed")
		}
	}

	err = preCheckPartitionModifiableColumn(sctx, t, col, newCol)
	if err != nil {
		return nil, err
	}

	// We don't support modifying column from not_auto_increment to auto_increment.
	if !mysql.HasAutoIncrementFlag(col.GetFlag()) && mysql.HasAutoIncrementFlag(newCol.GetFlag()) {
		return nil, dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs("can't set auto_increment")
	}
	// Not support auto id with default value.
	if mysql.HasAutoIncrementFlag(newCol.GetFlag()) && newCol.GetDefaultValue() != nil {
		return nil, dbterror.ErrInvalidDefaultValue.GenWithStackByArgs(newCol.Name)
	}
	// Disallow modifying column from auto_increment to not auto_increment if the session variable `AllowRemoveAutoInc` is false.
	//nolint:forbidigo
	if !sctx.GetSessionVars().AllowRemoveAutoInc && mysql.HasAutoIncrementFlag(col.GetFlag()) && !mysql.HasAutoIncrementFlag(newCol.GetFlag()) {
		return nil, dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs("can't remove auto_increment without @@tidb_allow_remove_auto_inc enabled")
	}

	// We support modifying the type definitions of 'null' to 'not null' now.
	if !mysql.HasNotNullFlag(col.GetFlag()) && mysql.HasNotNullFlag(newCol.GetFlag()) {
		if err = checkForNullValue(ctx, sctx, true, ident.Schema, ident.Name, newCol.ColumnInfo, col.ColumnInfo); err != nil {
			return nil, errors.Trace(err)
		}
	}

	if err = checkColumnWithIndexConstraint(t.Meta(), col.ColumnInfo, newCol.ColumnInfo); err != nil {
		return nil, err
	}

	// As same with MySQL, we don't support modifying the stored status for generated columns.
	if err = checkModifyGeneratedColumn(sctx, schema.Name, t, col, newCol, specNewColumn, spec.Position); err != nil {
		return nil, errors.Trace(err)
	}
	if errG != nil {
		// According to issue https://github.com/pingcap/tidb/issues/24321,
		// changing the type of a column involving generating a column is prohibited.
		return nil, dbterror.ErrUnsupportedOnGeneratedColumn.GenWithStackByArgs(errG.Error())
	}

	if t.Meta().TTLInfo != nil {
		// the column referenced by TTL should be a time type
		if t.Meta().TTLInfo.ColumnName.L == originalColName.L && !types.IsTypeTime(newCol.ColumnInfo.FieldType.GetType()) {
			return nil, errors.Trace(dbterror.ErrUnsupportedColumnInTTLConfig.GenWithStackByArgs(newCol.ColumnInfo.Name.O))
		}
	}

	var newAutoRandBits uint64
	if newAutoRandBits, err = checkAutoRandom(t.Meta(), col, specNewColumn); err != nil {
		return nil, errors.Trace(err)
	}

	txn, err := sctx.Txn(true)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bdrRole, err := meta.NewMutator(txn).GetBDRRole()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if bdrRole == string(ast.BDRRolePrimary) &&
		deniedByBDRWhenModifyColumn(newCol.FieldType, col.FieldType, specNewColumn.Options) && !filter.IsSystemSchema(schema.Name.L) {
		return nil, dbterror.ErrBDRRestrictedDDL.FastGenByArgs(bdrRole)
	}

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        t.Meta().ID,
		SchemaName:     schema.Name.L,
		TableName:      t.Meta().Name.L,
		Type:           model.ActionModifyColumn,
		BinlogInfo:     &model.HistoryInfo{},
		NeedReorg:      mayNeedChangeColData,
		CDCWriteSource: sctx.GetSessionVars().CDCWriteSource,
		SQLMode:        sctx.GetSessionVars().SQLMode,
		SessionVars:    make(map[string]string),
	}
	err = initJobReorgMetaFromVariables(ctx, job, t, sctx)
	if err != nil {
		return nil, errors.Trace(err)
	}

	args := &model.ModifyColumnArgs{
		Column:        newCol.ColumnInfo,
		OldColumnName: originalColName,
		Position:      spec.Position,
		NewShardBits:  newAutoRandBits,
	}
	return NewJobWrapperWithArgs(job, args, false), nil
}

// noReorgDataStrict is a strong check to decide whether we need to change the column data.
// If it returns true, it means we don't need the reorg no matter what the data is.
