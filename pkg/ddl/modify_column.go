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
	"github.com/pingcap/tidb/pkg/expression"
	exprctx "github.com/pingcap/tidb/pkg/expression/context"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror"
)

func checkModifyColumnWithGeneratedColumnsConstraint(allCols []*table.Column, oldColName model.CIStr) error {
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

// GetModifiableColumnJob returns a DDL job of model.ActionModifyColumn.
func GetModifiableColumnJob(
	ctx context.Context,
	sctx sessionctx.Context,
	is infoschema.InfoSchema, // WARN: is maybe nil here.
	ident ast.Ident,
	originalColName model.CIStr,
	schema *model.DBInfo,
	t table.Table,
	spec *ast.AlterTableSpec,
) (*model.Job, error) {
	var err error
	specNewColumn := spec.NewColumns[0]

	col := table.FindCol(t.Cols(), originalColName.L)
	if col == nil {
		return nil, infoschema.ErrColumnNotExists.GenWithStackByArgs(originalColName, ident.Name)
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

	if err = ProcessColumnCharsetAndCollation(sctx, col, newCol, t.Meta(), specNewColumn, schema); err != nil {
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

	if err = checkModifyTypes(&col.FieldType, &newCol.FieldType, isColumnWithIndex(col.Name.L, t.Meta().Indices)); err != nil {
		if strings.Contains(err.Error(), "Unsupported modifying collation") {
			colErrMsg := "Unsupported modifying collation of column '%s' from '%s' to '%s' when index is defined on it."
			err = dbterror.ErrUnsupportedModifyCollation.GenWithStack(colErrMsg, col.Name.L, col.GetCollate(), newCol.GetCollate())
		}
		return nil, errors.Trace(err)
	}
	needChangeColData := needChangeColumnData(col.ColumnInfo, newCol.ColumnInfo)
	if needChangeColData {
		if err = isGeneratedRelatedColumn(t.Meta(), newCol.ColumnInfo, col.ColumnInfo); err != nil {
			return nil, errors.Trace(err)
		}
		if t.Meta().Partition != nil {
			return nil, dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs("table is partition table")
		}
	}

	// Check that the column change does not affect the partitioning column
	// It must keep the same type, int [unsigned], [var]char, date[time]
	if t.Meta().Partition != nil {
		pt, ok := t.(table.PartitionedTable)
		if !ok {
			// Should never happen!
			return nil, dbterror.ErrNotAllowedTypeInPartition.GenWithStackByArgs(newCol.Name.O)
		}
		isPartitioningColumn := false
		for _, name := range pt.GetPartitionColumnNames() {
			if strings.EqualFold(name.L, col.Name.L) {
				isPartitioningColumn = true
				break
			}
		}
		if isPartitioningColumn {
			// TODO: update the partitioning columns with new names if column is renamed
			// Would be an extension from MySQL which does not support it.
			if col.Name.L != newCol.Name.L {
				return nil, dbterror.ErrDependentByPartitionFunctional.GenWithStackByArgs(col.Name.L)
			}
			if !isColTypeAllowedAsPartitioningCol(t.Meta().Partition.Type, newCol.FieldType) {
				return nil, dbterror.ErrNotAllowedTypeInPartition.GenWithStackByArgs(newCol.Name.O)
			}
			pi := pt.Meta().GetPartitionInfo()
			if len(pi.Columns) == 0 {
				// non COLUMNS partitioning, only checks INTs, not their actual range
				// There are many edge cases, like when truncating SQL Mode is allowed
				// which will change the partitioning expression value resulting in a
				// different partition. Better be safe and not allow decreasing of length.
				// TODO: Should we allow it in strict mode? Wait for a use case / request.
				if newCol.FieldType.GetFlen() < col.FieldType.GetFlen() {
					return nil, dbterror.ErrUnsupportedModifyCollation.GenWithStack("Unsupported modify column, decreasing length of int may result in truncation and change of partition")
				}
			}
			// Basically only allow changes of the length/decimals for the column
			// Note that enum is not allowed, so elems are not checked
			// TODO: support partition by ENUM
			if newCol.FieldType.EvalType() != col.FieldType.EvalType() ||
				newCol.FieldType.GetFlag() != col.FieldType.GetFlag() ||
				newCol.FieldType.GetCollate() != col.FieldType.GetCollate() ||
				newCol.FieldType.GetCharset() != col.FieldType.GetCharset() {
				return nil, dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs("can't change the partitioning column, since it would require reorganize all partitions")
			}
			// Generate a new PartitionInfo and validate it together with the new column definition
			// Checks if all partition definition values are compatible.
			// Similar to what buildRangePartitionDefinitions would do in terms of checks.

			tblInfo := pt.Meta()
			newTblInfo := *tblInfo
			// Replace col with newCol and see if we can generate a new SHOW CREATE TABLE
			// and reparse it and build new partition definitions (which will do additional
			// checks columns vs partition definition values
			newCols := make([]*model.ColumnInfo, 0, len(newTblInfo.Columns))
			for _, c := range newTblInfo.Columns {
				if c.ID == col.ID {
					newCols = append(newCols, newCol.ColumnInfo)
					continue
				}
				newCols = append(newCols, c)
			}
			newTblInfo.Columns = newCols

			var buf bytes.Buffer
			AppendPartitionInfo(tblInfo.GetPartitionInfo(), &buf, mysql.ModeNone)
			// The parser supports ALTER TABLE ... PARTITION BY ... even if the ddl code does not yet :)
			// Ignoring warnings
			stmt, _, err := parser.New().ParseSQL("ALTER TABLE t " + buf.String())
			if err != nil {
				// Should never happen!
				return nil, dbterror.ErrUnsupportedModifyColumn.GenWithStack("cannot parse generated PartitionInfo")
			}
			at, ok := stmt[0].(*ast.AlterTableStmt)
			if !ok || len(at.Specs) != 1 || at.Specs[0].Partition == nil {
				return nil, dbterror.ErrUnsupportedModifyColumn.GenWithStack("cannot parse generated PartitionInfo")
			}
			pAst := at.Specs[0].Partition
			_, err = buildPartitionDefinitionsInfo(
				exprctx.CtxWithHandleTruncateErrLevel(sctx.GetExprCtx(), errctx.LevelError),
				pAst.Definitions, &newTblInfo, uint64(len(newTblInfo.Partition.Definitions)),
			)
			if err != nil {
				return nil, dbterror.ErrUnsupportedModifyColumn.GenWithStack("New column does not match partition definitions: %s", err.Error())
			}
		}
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
	if !sctx.GetSessionVars().AllowRemoveAutoInc && mysql.HasAutoIncrementFlag(col.GetFlag()) && !mysql.HasAutoIncrementFlag(newCol.GetFlag()) {
		return nil, dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs("can't remove auto_increment without @@tidb_allow_remove_auto_inc enabled")
	}

	// We support modifying the type definitions of 'null' to 'not null' now.
	var modifyColumnTp byte
	if !mysql.HasNotNullFlag(col.GetFlag()) && mysql.HasNotNullFlag(newCol.GetFlag()) {
		if err = checkForNullValue(ctx, sctx, true, ident.Schema, ident.Name, newCol.ColumnInfo, col.ColumnInfo); err != nil {
			return nil, errors.Trace(err)
		}
		// `modifyColumnTp` indicates that there is a type modification.
		modifyColumnTp = mysql.TypeNull
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
	bdrRole, err := meta.NewMeta(txn).GetBDRRole()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if bdrRole == string(ast.BDRRolePrimary) &&
		deniedByBDRWhenModifyColumn(newCol.FieldType, col.FieldType, specNewColumn.Options) {
		return nil, dbterror.ErrBDRRestrictedDDL.FastGenByArgs(bdrRole)
	}

	job := &model.Job{
		SchemaID:       schema.ID,
		TableID:        t.Meta().ID,
		SchemaName:     schema.Name.L,
		TableName:      t.Meta().Name.L,
		Type:           model.ActionModifyColumn,
		BinlogInfo:     &model.HistoryInfo{},
		ReorgMeta:      NewDDLReorgMeta(sctx),
		CtxVars:        []any{needChangeColData},
		Args:           []any{&newCol.ColumnInfo, originalColName, spec.Position, modifyColumnTp, newAutoRandBits},
		CDCWriteSource: sctx.GetSessionVars().CDCWriteSource,
		SQLMode:        sctx.GetSessionVars().SQLMode,
	}
	return job, nil
}

// ProcessColumnCharsetAndCollation process column charset and collation
func ProcessColumnCharsetAndCollation(sctx sessionctx.Context, col *table.Column, newCol *table.Column, meta *model.TableInfo, specNewColumn *ast.ColumnDef, schema *model.DBInfo) error {
	var chs, coll string
	var err error
	// TODO: Remove it when all table versions are greater than or equal to TableInfoVersion1.
	// If newCol's charset is empty and the table's version less than TableInfoVersion1,
	// we will not modify the charset of the column. This behavior is not compatible with MySQL.
	if len(newCol.FieldType.GetCharset()) == 0 && meta.Version < model.TableInfoVersion1 {
		chs = col.FieldType.GetCharset()
		coll = col.FieldType.GetCollate()
	} else {
		chs, coll, err = getCharsetAndCollateInColumnDef(sctx.GetSessionVars(), specNewColumn)
		if err != nil {
			return errors.Trace(err)
		}
		chs, coll, err = ResolveCharsetCollation(sctx.GetSessionVars(),
			ast.CharsetOpt{Chs: chs, Col: coll},
			ast.CharsetOpt{Chs: meta.Charset, Col: meta.Collate},
			ast.CharsetOpt{Chs: schema.Charset, Col: schema.Collate},
		)
		chs, coll = OverwriteCollationWithBinaryFlag(sctx.GetSessionVars(), specNewColumn, chs, coll)
		if err != nil {
			return errors.Trace(err)
		}
	}

	if err = setCharsetCollationFlenDecimal(&newCol.FieldType, newCol.Name.O, chs, coll, sctx.GetSessionVars()); err != nil {
		return errors.Trace(err)
	}
	decodeEnumSetBinaryLiteralToUTF8(&newCol.FieldType, chs)
	return nil
}

// checkColumnWithIndexConstraint is used to check the related index constraint of the modified column.
// Index has a max-prefix-length constraint. eg: a varchar(100), index idx(a), modifying column a to a varchar(4000)
// will cause index idx to break the max-prefix-length constraint.
func checkColumnWithIndexConstraint(tbInfo *model.TableInfo, originalCol, newCol *model.ColumnInfo) error {
	columns := make([]*model.ColumnInfo, 0, len(tbInfo.Columns))
	columns = append(columns, tbInfo.Columns...)
	// Replace old column with new column.
	for i, col := range columns {
		if col.Name.L != originalCol.Name.L {
			continue
		}
		columns[i] = newCol.Clone()
		columns[i].Name = originalCol.Name
		break
	}

	pkIndex := tables.FindPrimaryIndex(tbInfo)

	checkOneIndex := func(indexInfo *model.IndexInfo) (err error) {
		var modified bool
		for _, col := range indexInfo.Columns {
			if col.Name.L == originalCol.Name.L {
				modified = true
				break
			}
		}
		if !modified {
			return
		}
		err = checkIndexInModifiableColumns(columns, indexInfo.Columns)
		if err != nil {
			return
		}
		err = checkIndexPrefixLength(columns, indexInfo.Columns)
		return
	}

	// Check primary key first.
	var err error

	if pkIndex != nil {
		err = checkOneIndex(pkIndex)
		if err != nil {
			return err
		}
	}

	// Check secondary indexes.
	for _, indexInfo := range tbInfo.Indices {
		if indexInfo.Primary {
			continue
		}
		// the second param should always be set to true, check index length only if it was modified
		// checkOneIndex needs one param only.
		err = checkOneIndex(indexInfo)
		if err != nil {
			return err
		}
	}
	return nil
}

func checkIndexInModifiableColumns(columns []*model.ColumnInfo, idxColumns []*model.IndexColumn) error {
	for _, ic := range idxColumns {
		col := model.FindColumnInfo(columns, ic.Name.L)
		if col == nil {
			return dbterror.ErrKeyColumnDoesNotExits.GenWithStack("column does not exist: %s", ic.Name)
		}

		prefixLength := types.UnspecifiedLength
		if types.IsTypePrefixable(col.FieldType.GetType()) && col.FieldType.GetFlen() > ic.Length {
			// When the index column is changed, prefix length is only valid
			// if the type is still prefixable and larger than old prefix length.
			prefixLength = ic.Length
		}
		if err := checkIndexColumn(nil, col, prefixLength); err != nil {
			return err
		}
	}
	return nil
}

// checkModifyTypes checks if the 'origin' type can be modified to 'to' type no matter directly change
// or change by reorg. It returns error if the two types are incompatible and correlated change are not
// supported. However, even the two types can be change, if the "origin" type contains primary key, error will be returned.
func checkModifyTypes(origin *types.FieldType, to *types.FieldType, needRewriteCollationData bool) error {
	canReorg, err := types.CheckModifyTypeCompatible(origin, to)
	if err != nil {
		if !canReorg {
			return errors.Trace(dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs(err.Error()))
		}
		if mysql.HasPriKeyFlag(origin.GetFlag()) {
			msg := "this column has primary key flag"
			return dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs(msg)
		}
	}

	err = checkModifyCharsetAndCollation(to.GetCharset(), to.GetCollate(), origin.GetCharset(), origin.GetCollate(), needRewriteCollationData)

	if err != nil {
		if to.GetCharset() == charset.CharsetGBK || origin.GetCharset() == charset.CharsetGBK {
			return errors.Trace(err)
		}
		// column type change can handle the charset change between these two types in the process of the reorg.
		if dbterror.ErrUnsupportedModifyCharset.Equal(err) && canReorg {
			return nil
		}
	}
	return errors.Trace(err)
}

// ProcessModifyColumnOptions process column options.
func ProcessModifyColumnOptions(ctx sessionctx.Context, col *table.Column, options []*ast.ColumnOption) error {
	var sb strings.Builder
	restoreFlags := format.RestoreStringSingleQuotes | format.RestoreKeyWordLowercase | format.RestoreNameBackQuotes |
		format.RestoreSpacesAroundBinaryOperation | format.RestoreWithoutSchemaName | format.RestoreWithoutSchemaName
	restoreCtx := format.NewRestoreCtx(restoreFlags, &sb)

	var hasDefaultValue, setOnUpdateNow bool
	var err error
	var hasNullFlag bool
	for _, opt := range options {
		switch opt.Tp {
		case ast.ColumnOptionDefaultValue:
			hasDefaultValue, err = SetDefaultValue(ctx, col, opt)
			if err != nil {
				return errors.Trace(err)
			}
		case ast.ColumnOptionComment:
			err := setColumnComment(ctx, col, opt)
			if err != nil {
				return errors.Trace(err)
			}
		case ast.ColumnOptionNotNull:
			col.AddFlag(mysql.NotNullFlag)
		case ast.ColumnOptionNull:
			hasNullFlag = true
			col.DelFlag(mysql.NotNullFlag)
		case ast.ColumnOptionAutoIncrement:
			col.AddFlag(mysql.AutoIncrementFlag)
		case ast.ColumnOptionPrimaryKey:
			return errors.Trace(dbterror.ErrUnsupportedModifyColumn.GenWithStack("can't change column constraint (PRIMARY KEY)"))
		case ast.ColumnOptionUniqKey:
			return errors.Trace(dbterror.ErrUnsupportedModifyColumn.GenWithStack("can't change column constraint (UNIQUE KEY)"))
		case ast.ColumnOptionOnUpdate:
			// TODO: Support other time functions.
			if !(col.GetType() == mysql.TypeTimestamp || col.GetType() == mysql.TypeDatetime) {
				return dbterror.ErrInvalidOnUpdate.GenWithStackByArgs(col.Name)
			}
			if !expression.IsValidCurrentTimestampExpr(opt.Expr, &col.FieldType) {
				return dbterror.ErrInvalidOnUpdate.GenWithStackByArgs(col.Name)
			}
			col.AddFlag(mysql.OnUpdateNowFlag)
			setOnUpdateNow = true
		case ast.ColumnOptionGenerated:
			sb.Reset()
			err = opt.Expr.Restore(restoreCtx)
			if err != nil {
				return errors.Trace(err)
			}
			col.GeneratedExprString = sb.String()
			col.GeneratedStored = opt.Stored
			col.Dependences = make(map[string]struct{})
			// Only used by checkModifyGeneratedColumn, there is no need to set a ctor for it.
			col.GeneratedExpr = table.NewClonableExprNode(nil, opt.Expr)
			for _, colName := range FindColumnNamesInExpr(opt.Expr) {
				col.Dependences[colName.Name.L] = struct{}{}
			}
		case ast.ColumnOptionCollate:
			col.SetCollate(opt.StrValue)
		case ast.ColumnOptionReference:
			return errors.Trace(dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs("can't modify with references"))
		case ast.ColumnOptionFulltext:
			return errors.Trace(dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs("can't modify with full text"))
		case ast.ColumnOptionCheck:
			return errors.Trace(dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs("can't modify with check"))
		// Ignore ColumnOptionAutoRandom. It will be handled later.
		case ast.ColumnOptionAutoRandom:
		default:
			return errors.Trace(dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs(fmt.Sprintf("unknown column option type: %d", opt.Tp)))
		}
	}

	if err = processAndCheckDefaultValueAndColumn(ctx, col, nil, hasDefaultValue, setOnUpdateNow, hasNullFlag); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func checkAutoRandom(tableInfo *model.TableInfo, originCol *table.Column, specNewColumn *ast.ColumnDef) (uint64, error) {
	var oldShardBits, oldRangeBits uint64
	if isClusteredPKColumn(originCol, tableInfo) {
		oldShardBits = tableInfo.AutoRandomBits
		oldRangeBits = tableInfo.AutoRandomRangeBits
	}
	newShardBits, newRangeBits, err := extractAutoRandomBitsFromColDef(specNewColumn)
	if err != nil {
		return 0, errors.Trace(err)
	}
	switch {
	case oldShardBits == newShardBits:
	case oldShardBits < newShardBits:
		addingAutoRandom := oldShardBits == 0
		if addingAutoRandom {
			convFromAutoInc := mysql.HasAutoIncrementFlag(originCol.GetFlag()) && originCol.IsPKHandleColumn(tableInfo)
			if !convFromAutoInc {
				return 0, dbterror.ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomAlterChangeFromAutoInc)
			}
		}
		if autoid.AutoRandomShardBitsMax < newShardBits {
			errMsg := fmt.Sprintf(autoid.AutoRandomOverflowErrMsg,
				autoid.AutoRandomShardBitsMax, newShardBits, specNewColumn.Name.Name.O)
			return 0, dbterror.ErrInvalidAutoRandom.GenWithStackByArgs(errMsg)
		}
		// increasing auto_random shard bits is allowed.
	case oldShardBits > newShardBits:
		if newShardBits == 0 {
			return 0, dbterror.ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomAlterErrMsg)
		}
		return 0, dbterror.ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomDecreaseBitErrMsg)
	}

	modifyingAutoRandCol := oldShardBits > 0 || newShardBits > 0
	if modifyingAutoRandCol {
		// Disallow changing the column field type.
		if originCol.GetType() != specNewColumn.Tp.GetType() {
			return 0, dbterror.ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomModifyColTypeErrMsg)
		}
		if originCol.GetType() != mysql.TypeLonglong {
			return 0, dbterror.ErrInvalidAutoRandom.GenWithStackByArgs(fmt.Sprintf(autoid.AutoRandomOnNonBigIntColumn, types.TypeStr(originCol.GetType())))
		}
		// Disallow changing from auto_random to auto_increment column.
		if containsColumnOption(specNewColumn, ast.ColumnOptionAutoIncrement) {
			return 0, dbterror.ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomIncompatibleWithAutoIncErrMsg)
		}
		// Disallow specifying a default value on auto_random column.
		if containsColumnOption(specNewColumn, ast.ColumnOptionDefaultValue) {
			return 0, dbterror.ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomIncompatibleWithDefaultValueErrMsg)
		}
	}
	if rangeBitsIsChanged(oldRangeBits, newRangeBits) {
		return 0, dbterror.ErrInvalidAutoRandom.FastGenByArgs(autoid.AutoRandomUnsupportedAlterRangeBits)
	}
	return newShardBits, nil
}

func rangeBitsIsChanged(oldBits, newBits uint64) bool {
	if oldBits == 0 {
		oldBits = autoid.AutoRandomRangeBitsDefault
	}
	if newBits == 0 {
		newBits = autoid.AutoRandomRangeBitsDefault
	}
	return oldBits != newBits
}
