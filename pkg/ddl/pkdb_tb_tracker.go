// Copyright 2026 PingCAP, Inc.

package ddl

import (
	"fmt"
	"sort"

	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/metabuild"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
)

// TableTracker tracks the state of tableInfo across multiple schema change operations
type TableTracker struct {
	tableInfo  *model.TableInfo
	schemaInfo *model.DBInfo
	sctx       sessionctx.Context

	addedColumns     map[string]struct{}
	modifiedColumns  map[string]struct{}
	positionColumns  map[string]struct{}
	addedIndexColumn map[string]map[string]struct{}
}

// NewTableTracker inits a new tracker from tableInfo
func NewTableTracker(tbInfo *model.TableInfo, schemaInfo *model.DBInfo, sctx sessionctx.Context) *TableTracker {
	return &TableTracker{
		tableInfo:        tbInfo.Clone(),
		schemaInfo:       schemaInfo,
		sctx:             sctx,
		addedColumns:     make(map[string]struct{}),
		modifiedColumns:  make(map[string]struct{}),
		positionColumns:  make(map[string]struct{}),
		addedIndexColumn: make(map[string]map[string]struct{}),
	}
}

func (t *TableTracker) checkColumnCanBeDroped(colName ast.CIStr, tableName ast.CIStr) error {
	if _, ok := t.addedColumns[colName.L]; ok {
		return dbterror.ErrCantDropFieldOrKey.GenWithStackByArgs(colName)
	}
	if _, ok := t.modifiedColumns[colName.L]; ok {
		return dbterror.ErrBadField.GenWithStackByArgs(colName, tableName)
	}
	if _, ok := t.positionColumns[colName.L]; ok {
		return dbterror.ErrBadField.GenWithStackByArgs(colName, tableName)
	}
	for _, cols := range t.addedIndexColumn {
		if _, ok := cols[colName.L]; ok {
			return dbterror.ErrKeyColumnDoesNotExits.GenWithStack("column does not exist: %s", colName)
		}
	}
	return nil
}

func (t *TableTracker) checkColumnCanBeModified(colName ast.CIStr, tableName ast.CIStr) error {
	if _, ok := t.addedColumns[colName.L]; ok {
		return dbterror.ErrBadField.GenWithStackByArgs(colName, tableName)
	}
	if _, ok := t.modifiedColumns[colName.L]; ok {
		return dbterror.ErrBadField.GenWithStackByArgs(colName, tableName)
	}
	return nil
}

func (t *TableTracker) checkIndexCanBeDropped(indexName ast.CIStr) (skip bool, err error) {
	if _, ok := t.addedIndexColumn[indexName.L]; !ok {
		return false, nil
	}
	return false, dbterror.ErrCantDropFieldOrKey.GenWithStack("index %s doesn't exist", indexName)
}

func (t *TableTracker) markColumnAdded(colName ast.CIStr) {
	t.addedColumns[colName.L] = struct{}{}
}

func (t *TableTracker) markColumnModified(colNames ...ast.CIStr) {
	for _, colName := range colNames {
		t.modifiedColumns[colName.L] = struct{}{}
	}
}

func (t *TableTracker) addPositionColumn(pos *ast.ColumnPosition) {
	if pos == nil || pos.Tp != ast.ColumnPositionAfter {
		return
	}
	t.positionColumns[pos.RelativeColumn.Name.L] = struct{}{}
}

func (t *TableTracker) markIndexAdded(indexName ast.CIStr, indexPartSpecifications []*ast.IndexPartSpecification, hiddenCols []*model.ColumnInfo) {
	cols := make(map[string]struct{})
	for _, part := range indexPartSpecifications {
		if part == nil || part.Column == nil {
			continue
		}
		cols[part.Column.Name.L] = struct{}{}
	}
	for _, hiddenCol := range hiddenCols {
		for dep := range hiddenCol.Dependences {
			cols[ast.NewCIStr(dep).L] = struct{}{}
		}
	}
	t.addedIndexColumn[indexName.L] = cols
}

// DBInfo returns the schema info used by the table tracker.
func (tracker *TableTracker) DBInfo() *model.DBInfo {
	return tracker.schemaInfo
}

// TableInfo returns the tracked table info after previously applied specs.
func (tracker *TableTracker) TableInfo() *model.TableInfo {
	return tracker.tableInfo
}

func (tracker *TableTracker) metaBuildCtx() *metabuild.Context {
	if tracker.sctx != nil {
		return NewMetaBuildContextWithSctx(tracker.sctx)
	}
	return metabuild.NewContext()
}

// AlterTable applies one spec on tableInfo.
func (tracker *TableTracker) AlterTable(spec *ast.AlterTableSpec) error {
	switch spec.Tp {
	case ast.AlterTableAddColumns:
		return tracker.addColumn(spec)
	case ast.AlterTableModifyColumn:
		return tracker.modifyColumn(spec)
	case ast.AlterTableChangeColumn:
		return tracker.modifyColumn(spec)
	case ast.AlterTableRenameColumn:
		return tracker.modifyColumn(spec)
	case ast.AlterTableAlterColumn:
		return tracker.alterColumn(spec)
	case ast.AlterTableDropColumn:
		return tracker.dropColumn(spec)
	case ast.AlterTableDropIndex:
		return tracker.dropIndex(spec)
	case ast.AlterTableDropPrimaryKey:
		return tracker.dropIndex(spec)
	case ast.AlterTableRenameIndex:
		return tracker.renameIndex(spec)
	case ast.AlterTableIndexInvisible:
		return tracker.alterIndexVisibility(spec)
	case ast.AlterTableAddConstraint:
		return tracker.addConstraint(spec)
	case ast.AlterTableDropForeignKey:
		return tracker.dropForeignKey(spec)
	default:
		return dbterror.ErrRunMultiSchemaChanges
	}
}

func (tracker *TableTracker) addConstraint(spec *ast.AlterTableSpec) error {
	switch spec.Constraint.Tp {
	case ast.ConstraintKey, ast.ConstraintIndex, ast.ConstraintUniq, ast.ConstraintUniqIndex, ast.ConstraintUniqKey:
		return tracker.addIndex(spec)
	case ast.ConstraintPrimaryKey:
		return tracker.addPrimaryKey(spec)
	case ast.ConstraintForeignKey:
		return tracker.addForeignKey(spec)
	default:
		return dbterror.ErrRunMultiSchemaChanges
	}
}

func (tracker *TableTracker) addColumn(spec *ast.AlterTableSpec) error {
	if len(spec.NewColumns) == 0 {
		return nil
	}
	if colInfo := model.FindColumnInfo(tracker.tableInfo.Columns, spec.NewColumns[0].Name.Name.L); colInfo != nil {
		return plannererrors.ErrDupFieldName.GenWithStackByArgs(spec.NewColumns[0].Name.Name.O)
	}

	col, _, err := columnDefToCol(tracker.metaBuildCtx(), len(tracker.tableInfo.Columns), spec.NewColumns[0], nil)
	if err != nil {
		return err
	}
	colInfo := InitAndAddColumnToTable(tracker.tableInfo, col.ColumnInfo)
	offset, err := LocateOffsetToMove(colInfo.Offset, spec.Position, tracker.tableInfo)
	if err != nil {
		return err
	}
	tracker.tableInfo.MoveColumnInfo(colInfo.Offset, offset)
	colInfo.State = model.StatePublic
	tracker.addPositionColumn(spec.Position)
	tracker.markColumnAdded(colInfo.Name)
	return nil
}

func (tracker *TableTracker) addIndex(spec *ast.AlterTableSpec) error {
	if spec.Constraint == nil {
		return nil
	}
	indexName := ast.NewCIStr(spec.Constraint.Name)
	indexParts := cloneIndexPartSpecifications(spec.Constraint.Keys)
	unique := spec.Constraint.Tp == ast.ConstraintUniq ||
		spec.Constraint.Tp == ast.ConstraintUniqIndex ||
		spec.Constraint.Tp == ast.ConstraintUniqKey
	mockTbl := tables.MockTableFromMeta(tracker.tableInfo)
	if len(indexName.L) == 0 {
		colName := ast.NewCIStr("expression_index")
		if len(indexParts) > 0 && indexParts[0] != nil && indexParts[0].Column != nil {
			colName = indexParts[0].Column.Name
		}
		indexName = GetName4AnonymousIndex(mockTbl, colName, ast.NewCIStr(""))
	}
	hiddenCols, shardIndexVersion, err := BuildHiddenColumnInfo(
		tracker.metaBuildCtx(),
		indexParts,
		indexName,
		tracker.tableInfo,
		mockTbl.Cols(),
		unique,
	)
	if err != nil {
		return err
	}
	for _, hiddenCol := range hiddenCols {
		colInfo := InitAndAddColumnToTable(tracker.tableInfo, hiddenCol)
		colInfo.State = model.StatePublic
	}
	indexInfo, err := BuildIndexInfo(
		tracker.metaBuildCtx(),
		tracker.tableInfo,
		indexName,
		false,
		unique,
		model.ColumnarIndexTypeNA,
		indexParts,
		spec.Constraint.Option,
		model.StatePublic,
	)
	if err != nil {
		return err
	}
	indexInfo.ShardIndexVersion = shardIndexVersion
	indexInfo.ID = AllocateIndexID(tracker.tableInfo)
	tracker.tableInfo.Indices = append(tracker.tableInfo.Indices, indexInfo)
	tracker.rebuildIndexColumnFlags()
	tracker.markIndexAdded(indexName, indexParts, hiddenCols)
	return nil
}

func (tracker *TableTracker) addPrimaryKey(spec *ast.AlterTableSpec) error {
	// cannot add clustered pk in alter table, therefore don't need to update PKIsHandle/IsCommonHandle
	if spec.Constraint == nil {
		return nil
	}
	if indexInfo := tracker.tableInfo.FindIndexByName(mysql.PrimaryKeyName); indexInfo != nil || tracker.tableInfo.PKIsHandle {
		return infoschema.ErrMultiplePriKey
	}
	if _, err := CheckPKOnGeneratedColumn(tracker.tableInfo, spec.Constraint.Keys); err != nil {
		return err
	}
	indexColumns, _, err := buildIndexColumns(tracker.metaBuildCtx(), tracker.tableInfo.Columns, spec.Constraint.Keys, model.ColumnarIndexTypeNA)
	if err != nil {
		return err
	}
	if err = checkCreateGlobalIndex(errctx.StrictNoWarningContext, tracker.tableInfo, "PRIMARY", indexColumns, true, spec.Constraint.Option != nil && spec.Constraint.Option.Global); err != nil {
		return err
	}
	indexInfo, err := BuildIndexInfo(
		tracker.metaBuildCtx(),
		tracker.tableInfo,
		ast.NewCIStr(mysql.PrimaryKeyName),
		true,
		true,
		model.ColumnarIndexTypeNA,
		cloneIndexPartSpecifications(spec.Constraint.Keys),
		spec.Constraint.Option,
		model.StatePublic,
	)
	if err != nil {
		return err
	}
	indexInfo.ID = AllocateIndexID(tracker.tableInfo)
	tracker.tableInfo.Indices = append(tracker.tableInfo.Indices, indexInfo)
	if err := UpdateColsNull2NotNull(tracker.tableInfo, indexInfo); err != nil {
		return err
	}
	tracker.rebuildIndexColumnFlags()
	tracker.markIndexAdded(ast.NewCIStr(mysql.PrimaryKeyName), spec.Constraint.Keys, nil)
	return nil
}

func (tracker *TableTracker) addForeignKey(spec *ast.AlterTableSpec) error {
	if spec.Constraint == nil {
		return nil
	}
	fkName := ast.NewCIStr(spec.Constraint.Name)
	if len(fkName.L) == 0 {
		fkName = ast.NewCIStr(fmt.Sprintf("fk_%d", tracker.tableInfo.MaxForeignKeyID+1))
	}
	fkInfo, err := buildFKInfo(fkName, spec.Constraint.Keys, spec.Constraint.Refer, tables.MockTableFromMeta(tracker.tableInfo).Cols())
	if err != nil {
		return err
	}
	tracker.tableInfo.MaxForeignKeyID++
	fkInfo.ID = tracker.tableInfo.MaxForeignKeyID
	fkInfo.State = model.StatePublic
	tracker.tableInfo.ForeignKeys = append(tracker.tableInfo.ForeignKeys, fkInfo)
	if err := addIndexForForeignKey(tracker.metaBuildCtx(), tracker.tableInfo); err != nil {
		return err
	}
	tracker.rebuildIndexColumnFlags()
	return nil
}

func (tracker *TableTracker) modifyColumn(spec *ast.AlterTableSpec) error {
	var err error
	var oldColName ast.CIStr
	var newColName ast.CIStr
	switch spec.Tp {
	case ast.AlterTableRenameColumn:
		oldColName = spec.OldColumnName.Name
		newColName = spec.NewColumnName.Name
		err = tracker.renameColumn(oldColName, newColName, spec.Position)
	case ast.AlterTableModifyColumn:
		if len(spec.NewColumns) == 0 {
			return nil
		}
		oldColName = spec.NewColumns[0].Name.Name
		newColName = oldColName
		err = tracker.modifyExistingColumn(spec.NewColumns[0], spec.Position)
	case ast.AlterTableChangeColumn:
		if len(spec.NewColumns) == 0 {
			return nil
		}
		oldColName = spec.OldColumnName.Name
		newColName = spec.NewColumns[0].Name.Name
		err = tracker.changeExistingColumn(oldColName, spec.NewColumns[0], spec.Position)
	default:
		return nil
	}
	if err != nil {
		return err
	}
	if err := tracker.checkColumnCanBeModified(oldColName, tracker.tableInfo.Name); err != nil {
		return err
	}
	if newColName.L != oldColName.L {
		// can't rename a column being used as position after
		if _, ok := tracker.positionColumns[oldColName.L]; ok {
			return dbterror.ErrBadField.GenWithStackByArgs(oldColName, tracker.tableInfo.Name)
		}
		tracker.markColumnModified(newColName)
	}
	tracker.markColumnModified(oldColName)
	tracker.addPositionColumn(spec.Position)
	return nil
}

func (tracker *TableTracker) renameColumn(oldColName, newColName ast.CIStr, pos *ast.ColumnPosition) error {
	oldColInfo := model.FindColumnInfo(tracker.tableInfo.Columns, oldColName.L)
	if oldColInfo == nil {
		return infoschema.ErrColumnNotExists.GenWithStackByArgs(oldColName, tracker.tableInfo.Name)
	}
	if err := IsColumnRenameableWithCheckConstraint(oldColInfo.Name, tracker.tableInfo); err != nil {
		return err
	}
	if oldColName.L == newColName.L {
		return nil
	}
	mockTbl := tables.MockTableFromMeta(tracker.tableInfo)
	if table.FindCol(mockTbl.Cols(), newColName.L) != nil {
		return infoschema.ErrColumnExists.GenWithStackByArgs(newColName)
	}
	if err := checkModifyColumnWithGeneratedColumnsConstraint(mockTbl.Cols(), oldColName); err != nil {
		return err
	}
	if err := checkDropColumnWithPartitionConstraint(mockTbl, oldColName); err != nil {
		return err
	}
	oldColInfo.Name = newColName
	tracker.updateRelatedIndexColumns(oldColName, oldColInfo)
	tracker.renameForeignKeyColumns(oldColName, newColName)
	destOffset, err := LocateOffsetToMove(oldColInfo.Offset, pos, tracker.tableInfo)
	if err != nil {
		return err
	}
	tracker.tableInfo.MoveColumnInfo(oldColInfo.Offset, destOffset)
	return nil
}

func (tracker *TableTracker) updateRelatedIndexColumns(oldColName ast.CIStr, newColInfo *model.ColumnInfo) {
	for _, idx := range tracker.tableInfo.Indices {
		for _, idxCol := range idx.Columns {
			if idxCol.Name.L == oldColName.L {
				UpdateIndexCol(idxCol, newColInfo)
			}
		}
		for _, idxCol := range idx.AffectColumn {
			if idxCol.Name.L == oldColName.L {
				UpdateIndexCol(idxCol, newColInfo)
			}
		}
	}
}

func (tracker *TableTracker) renameForeignKeyColumns(oldColName, newColName ast.CIStr) {
	for _, fk := range tracker.tableInfo.ForeignKeys {
		for i := range fk.Cols {
			if fk.Cols[i].L == oldColName.L {
				fk.Cols[i] = newColName
			}
		}
		if fk.RefTable.L == tracker.tableInfo.Name.L {
			for i := range fk.RefCols {
				if fk.RefCols[i].L == oldColName.L {
					fk.RefCols[i] = newColName
				}
			}
		}
	}
}

func (tracker *TableTracker) modifyExistingColumn(newColDef *ast.ColumnDef, pos *ast.ColumnPosition) error {
	oldColName := newColDef.Name.Name
	oldColInfo := model.FindColumnInfo(tracker.tableInfo.Columns, oldColName.L)
	if oldColInfo == nil {
		return infoschema.ErrColumnNotExists.GenWithStackByArgs(oldColName, tracker.tableInfo.Name)
	}
	mockTbl := tables.MockTableFromMeta(tracker.tableInfo)
	if err := checkModifyColumnWithGeneratedColumnsConstraint(mockTbl.Cols(), oldColName); err != nil {
		return err
	}
	if err := checkDropColumnWithPartitionConstraint(mockTbl, oldColName); err != nil {
		return err
	}
	oldCol := table.ToColumn(oldColInfo.Clone())
	newCol := table.ToColumn(&model.ColumnInfo{
		Offset:                oldCol.Offset,
		State:                 oldCol.State,
		OriginDefaultValue:    oldCol.OriginDefaultValue,
		OriginDefaultValueBit: oldCol.OriginDefaultValueBit,
		FieldType:             *newColDef.Tp,
		Name:                  oldColName,
		Version:               oldCol.Version,
	})
	if err := ProcessColumnCharsetAndCollation(
		tracker.metaBuildCtx(),
		oldCol,
		newCol,
		tracker.tableInfo,
		newColDef,
		tracker.schemaInfo,
	); err != nil {
		return err
	}
	if err := ProcessModifyColumnOptions(tracker.sctx, newCol, newColDef.Options); err != nil {
		return err
	}
	err := checkModifyTypes(oldCol.ColumnInfo, newCol.ColumnInfo, isColumnWithIndex(oldCol.Name.L, tracker.tableInfo.Indices))
	if err != nil {
		return err
	}
	indexFlags := oldColInfo.GetFlag() & (mysql.PriKeyFlag | mysql.UniqueKeyFlag | mysql.MultipleKeyFlag)
	newCol.AddFlag(indexFlags)
	if mysql.HasPriKeyFlag(oldColInfo.GetFlag()) {
		newCol.AddFlag(mysql.NotNullFlag)
	}
	if tracker.tableInfo.TTLInfo != nil &&
		tracker.tableInfo.TTLInfo.ColumnName.L == oldColName.L &&
		!types.IsTypeTime(newCol.ColumnInfo.FieldType.GetType()) {
		return dbterror.ErrUnsupportedColumnInTTLConfig.GenWithStackByArgs(newCol.ColumnInfo.Name.O)
	}
	newColInfo := newCol.ColumnInfo
	newColInfo.ID = AllocateColumnID(tracker.tableInfo)
	newColInfo.Version = oldColInfo.Version
	return adjustTableInfoAfterModifyColumn(tracker.tableInfo, newColInfo, oldColInfo.Clone(), pos)
}

func (tracker *TableTracker) changeExistingColumn(oldColName ast.CIStr, newColDef *ast.ColumnDef, pos *ast.ColumnPosition) error {
	oldColInfo := model.FindColumnInfo(tracker.tableInfo.Columns, oldColName.L)
	if oldColInfo == nil {
		return infoschema.ErrColumnNotExists.GenWithStackByArgs(oldColName, tracker.tableInfo.Name)
	}
	mockTbl := tables.MockTableFromMeta(tracker.tableInfo)
	if newColDef.Name.Name.L != oldColName.L && table.FindCol(mockTbl.Cols(), newColDef.Name.Name.L) != nil {
		return infoschema.ErrColumnExists.GenWithStackByArgs(newColDef.Name.Name)
	}
	errG := checkModifyColumnWithGeneratedColumnsConstraint(mockTbl.Cols(), oldColName)
	if newColDef.Name.Name.L != oldColName.L && errG != nil {
		return errG
	}
	if err := checkDropColumnWithPartitionConstraint(mockTbl, oldColName); err != nil {
		return err
	}
	oldCol := table.ToColumn(oldColInfo.Clone())
	newCol := table.ToColumn(&model.ColumnInfo{
		Offset:                oldCol.Offset,
		State:                 oldCol.State,
		OriginDefaultValue:    oldCol.OriginDefaultValue,
		OriginDefaultValueBit: oldCol.OriginDefaultValueBit,
		FieldType:             *newColDef.Tp,
		Name:                  newColDef.Name.Name,
		Version:               oldCol.Version,
	})
	if err := ProcessColumnCharsetAndCollation(
		tracker.metaBuildCtx(),
		oldCol,
		newCol,
		tracker.tableInfo,
		newColDef,
		tracker.schemaInfo,
	); err != nil {
		return err
	}
	if err := ProcessModifyColumnOptions(tracker.sctx, newCol, newColDef.Options); err != nil {
		return err
	}
	err := checkModifyTypes(oldCol.ColumnInfo, newCol.ColumnInfo, isColumnWithIndex(oldCol.Name.L, tracker.tableInfo.Indices))
	if err != nil {
		return err
	}
	indexFlags := oldColInfo.GetFlag() & (mysql.PriKeyFlag | mysql.UniqueKeyFlag | mysql.MultipleKeyFlag)
	newCol.AddFlag(indexFlags)
	if mysql.HasPriKeyFlag(oldColInfo.GetFlag()) {
		newCol.AddFlag(mysql.NotNullFlag)
	}
	if errG != nil {
		return dbterror.ErrUnsupportedOnGeneratedColumn.GenWithStackByArgs(errG.Error())
	}
	if tracker.tableInfo.TTLInfo != nil &&
		tracker.tableInfo.TTLInfo.ColumnName.L == oldColName.L &&
		!types.IsTypeTime(newCol.ColumnInfo.FieldType.GetType()) {
		return dbterror.ErrUnsupportedColumnInTTLConfig.GenWithStackByArgs(newCol.ColumnInfo.Name.O)
	}
	newColInfo := newCol.ColumnInfo
	newColInfo.ID = AllocateColumnID(tracker.tableInfo)
	newColInfo.Version = oldColInfo.Version
	return adjustTableInfoAfterModifyColumn(tracker.tableInfo, newColInfo, oldColInfo.Clone(), pos)
}

func (tracker *TableTracker) renameIndex(spec *ast.AlterTableSpec) error {
	if _, ok := tracker.addedIndexColumn[spec.FromKey.L]; ok {
		return infoschema.ErrKeyNotExists.GenWithStackByArgs(spec.FromKey.O, tracker.TableInfo().Name)
	}
	duplicate, err := ValidateRenameIndex(spec.FromKey, spec.ToKey, tracker.tableInfo)
	if duplicate {
		return nil
	}
	if err != nil {
		return err
	}
	idx := tracker.tableInfo.FindIndexByName(spec.FromKey.L)
	idx.Name = spec.ToKey
	renameHiddenColumns(tracker.tableInfo, spec.FromKey, spec.ToKey)
	return nil
}

func (tracker *TableTracker) alterColumn(spec *ast.AlterTableSpec) error {
	if len(spec.NewColumns) == 0 {
		return nil
	}
	colName := spec.NewColumns[0].Name.Name
	if err := tracker.checkColumnCanBeModified(colName, tracker.tableInfo.Name); err != nil {
		return err
	}
	oldColInfo := model.FindColumnInfo(tracker.tableInfo.Columns, colName.L)
	if oldColInfo == nil {
		return dbterror.ErrBadField.GenWithStackByArgs(colName, tracker.tableInfo.Name)
	}
	col := table.ToColumn(oldColInfo.Clone())
	col.DelFlag(mysql.NoDefaultValueFlag)
	col.DefaultIsExpr = false
	if len(spec.NewColumns[0].Options) == 0 {
		if err := col.SetDefaultValue(nil); err != nil {
			return err
		}
		col.AddFlag(mysql.NoDefaultValueFlag)
	} else {
		if _, err := SetDefaultValue(tracker.metaBuildCtx().GetExprCtx(), col, spec.NewColumns[0].Options[0]); err != nil {
			return err
		}
	}
	tracker.tableInfo.Columns[oldColInfo.Offset] = col.ColumnInfo
	tracker.markColumnModified(colName)
	return nil
}

func (tracker *TableTracker) dropColumn(spec *ast.AlterTableSpec) error {
	colName := spec.OldColumnName.Name
	if err := tracker.checkColumnCanBeDroped(colName, tracker.tableInfo.Name); err != nil {
		return err
	}
	colInfo := model.FindColumnInfo(tracker.tableInfo.Columns, colName.L)
	if colInfo == nil {
		return dbterror.ErrCantDropFieldOrKey.GenWithStackByArgs(colName)
	}
	if err := isDroppableColumn(tracker.tableInfo, colName); err != nil {
		return err
	}
	if err := checkDropColumnWithTTLConfig(tracker.tableInfo, colName.L); err != nil {
		return err
	}
	if err := checkDropColumnWithPartitionConstraint(tables.MockTableFromMeta(tracker.tableInfo), colName); err != nil {
		return err
	}
	if tracker.tableInfo.PKIsHandle && tracker.tableInfo.GetPkColInfo() != nil &&
		tracker.tableInfo.GetPkColInfo().Name.L == colName.L {
		return dbterror.ErrUnsupportedPKHandle
	}
	if err := tracker.checkDropColumnWithLocalForeignKeyConstraint(colName); err != nil {
		return err
	}
	wasPKHandle := tracker.tableInfo.PKIsHandle && mysql.HasPriKeyFlag(colInfo.GetFlag())
	tracker.tableInfo.MoveColumnInfo(colInfo.Offset, len(tracker.tableInfo.Columns)-1)
	tracker.tableInfo.Columns = tracker.tableInfo.Columns[:len(tracker.tableInfo.Columns)-1]
	droppedIndexes := make([]*model.IndexInfo, 0)
	remainIndexes := make([]*model.IndexInfo, 0, len(tracker.tableInfo.Indices))
	for _, idx := range tracker.tableInfo.Indices {
		if i, _ := model.FindIndexColumnByName(idx.Columns, colName.L); i != -1 {
			droppedIndexes = append(droppedIndexes, idx)
			if idx.Primary {
				tracker.tableInfo.IsCommonHandle = false
				tracker.tableInfo.CommonHandleVersion = 0
			}
			continue
		}
		remainIndexes = append(remainIndexes, idx)
	}
	tracker.tableInfo.Indices = remainIndexes
	if wasPKHandle {
		tracker.tableInfo.PKIsHandle = false
	}
	tracker.removeDependentHiddenColumns(droppedIndexes)
	tracker.rebuildIndexColumnFlags()
	return nil
}

func (tracker *TableTracker) checkDropColumnWithLocalForeignKeyConstraint(colName ast.CIStr) error {
	for _, fkInfo := range tracker.tableInfo.ForeignKeys {
		for _, col := range fkInfo.Cols {
			if col.L == colName.L {
				return dbterror.ErrFkColumnCannotDrop.GenWithStackByArgs(colName.L, fkInfo.Name)
			}
		}
		if fkInfo.RefTable.L == tracker.tableInfo.Name.L {
			for _, refCol := range fkInfo.RefCols {
				if refCol.L == colName.L {
					return dbterror.ErrFkColumnCannotDropChild.GenWithStackByArgs(colName.L, fkInfo.Name, tracker.tableInfo.Name)
				}
			}
		}
	}
	return nil
}

func (tracker *TableTracker) dropIndex(spec *ast.AlterTableSpec) error {
	indexName := ast.NewCIStr(spec.Name)
	if spec.Tp == ast.AlterTableDropPrimaryKey {
		indexName = ast.NewCIStr(mysql.PrimaryKeyName)
	}
	if skip, err := tracker.checkIndexCanBeDropped(indexName); skip || err != nil {
		return err
	}
	mockTbl := tables.MockTableFromMeta(tracker.tableInfo)
	indexInfo := tracker.tableInfo.FindIndexByName(indexName.L)
	if _, err := CheckIsDropPrimaryKey(indexName, indexInfo, mockTbl); err != nil {
		return err
	}
	if indexInfo == nil {
		return dbterror.ErrCantDropFieldOrKey.GenWithStack("index %s doesn't exist", indexName)
	}
	newIndices := make([]*model.IndexInfo, 0, len(tracker.tableInfo.Indices)-1)
	for _, idx := range tracker.tableInfo.Indices {
		if idx.Name.L == indexName.L {
			continue
		}
		newIndices = append(newIndices, idx)
	}
	tracker.tableInfo.Indices = newIndices
	if err := checkInvisibleIndexOnPK(tracker.tableInfo); err != nil {
		return err
	}
	if err := tracker.checkIndexNeededInLocalForeignKey(indexInfo, newIndices); err != nil {
		return err
	}
	droppedIndexes := []*model.IndexInfo{indexInfo}
	if indexInfo.Primary {
		tracker.tableInfo.IsCommonHandle = false
		tracker.tableInfo.CommonHandleVersion = 0
	}
	tracker.removeDependentHiddenColumns(droppedIndexes)
	tracker.rebuildIndexColumnFlags()
	return nil
}

func (tracker *TableTracker) checkIndexNeededInLocalForeignKey(idxInfo *model.IndexInfo, remainIdxs []*model.IndexInfo) error {
	if len(tracker.tableInfo.ForeignKeys) == 0 {
		return nil
	}
	for _, fk := range tracker.tableInfo.ForeignKeys {
		if fk.Version < model.FKVersion1 {
			continue
		}
		if !model.IsIndexPrefixCovered(tracker.tableInfo, idxInfo, fk.Cols...) {
			continue
		}
		if tracker.tableInfo.PKIsHandle && len(fk.Cols) == 1 {
			refColInfo := model.FindColumnInfo(tracker.tableInfo.Columns, fk.Cols[0].L)
			if refColInfo != nil && mysql.HasPriKeyFlag(refColInfo.GetFlag()) {
				continue
			}
		}
		covered := false
		for _, remainIdx := range remainIdxs {
			if model.IsIndexPrefixCovered(tracker.tableInfo, remainIdx, fk.Cols...) {
				covered = true
				break
			}
		}
		if !covered {
			return dbterror.ErrDropIndexNeededInForeignKey.GenWithStackByArgs(idxInfo.Name)
		}
	}
	return nil
}

func (tracker *TableTracker) dropForeignKey(spec *ast.AlterTableSpec) error {
	fkName := ast.NewCIStr(spec.Name)
	fks := make([]*model.FKInfo, 0, len(tracker.tableInfo.ForeignKeys))
	found := false
	for _, fk := range tracker.tableInfo.ForeignKeys {
		if fk.Name.L == fkName.L {
			found = true
			continue
		}
		fks = append(fks, fk)
	}
	if !found {
		return infoschema.ErrForeignKeyNotExists.GenWithStackByArgs(fkName)
	}
	tracker.tableInfo.ForeignKeys = fks
	return nil
}

func (tracker *TableTracker) alterIndexVisibility(spec *ast.AlterTableSpec) error {
	invisible := spec.Visibility == ast.IndexVisibilityInvisible
	skip, err := validateAlterIndexVisibility(nil, spec.IndexName, invisible, tracker.tableInfo)
	if skip {
		return nil
	}
	if err != nil {
		return err
	}
	idx := tracker.tableInfo.FindIndexByName(spec.IndexName.L)
	idx.Invisible = invisible
	return nil
}

func (tracker *TableTracker) removeDependentHiddenColumns(indexes []*model.IndexInfo) {
	if len(indexes) == 0 {
		return
	}
	hiddenColIDs := make(map[int64]struct{})
	for _, idx := range indexes {
		for _, idxCol := range idx.Columns {
			if idxCol.Offset >= len(tracker.tableInfo.Columns) {
				continue
			}
			col := tracker.tableInfo.Columns[idxCol.Offset]
			if col.Hidden {
				hiddenColIDs[col.ID] = struct{}{}
			}
		}
	}
	if len(hiddenColIDs) == 0 {
		return
	}
	offsets := make([]int, 0, len(hiddenColIDs))
	for _, col := range tracker.tableInfo.Columns {
		if _, ok := hiddenColIDs[col.ID]; ok {
			offsets = append(offsets, col.Offset)
		}
	}
	sort.Sort(sort.Reverse(sort.IntSlice(offsets)))
	endOffset := len(tracker.tableInfo.Columns) - 1
	for _, offset := range offsets {
		tracker.tableInfo.MoveColumnInfo(offset, endOffset)
		endOffset--
	}
	tracker.tableInfo.Columns = tracker.tableInfo.Columns[:endOffset+1]
}

func (tracker *TableTracker) rebuildIndexColumnFlags() {
	var pkHandleColID int64
	if tracker.tableInfo.PKIsHandle {
		if pkCol := tracker.tableInfo.GetPkColInfo(); pkCol != nil {
			pkHandleColID = pkCol.ID
		}
	}
	for _, col := range tracker.tableInfo.Columns {
		col.DelFlag(mysql.PriKeyFlag | mysql.UniqueKeyFlag | mysql.MultipleKeyFlag)
	}
	if pkHandleColID != 0 {
		if pkCol := model.FindColumnInfoByID(tracker.tableInfo.Columns, pkHandleColID); pkCol != nil {
			pkCol.AddFlag(mysql.PriKeyFlag | mysql.NotNullFlag)
		}
	}
	for _, idx := range tracker.tableInfo.Indices {
		AddIndexColumnFlag(tracker.tableInfo, idx)
	}
}

func cloneIndexPartSpecifications(parts []*ast.IndexPartSpecification) []*ast.IndexPartSpecification {
	cloned := make([]*ast.IndexPartSpecification, 0, len(parts))
	for _, part := range parts {
		if part == nil {
			cloned = append(cloned, nil)
			continue
		}
		clonedPart := *part
		cloned = append(cloned, &clonedPart)
	}
	return cloned
}
