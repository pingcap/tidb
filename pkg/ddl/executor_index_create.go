// Copyright 2015 PingCAP, Inc.
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
	"context"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror"
)

func (e *executor) createIndex(ctx sessionctx.Context, ti ast.Ident, keyType ast.IndexKeyType, indexName ast.CIStr,
	indexPartSpecifications []*ast.IndexPartSpecification, indexOption *ast.IndexOption, ifNotExists bool) error {
	// not support Spatial and FullText index
	switch keyType {
	case ast.IndexKeyTypeSpatial:
		return dbterror.ErrUnsupportedIndexType.GenWithStack("SPATIAL index is not supported")
	case ast.IndexKeyTypeColumnar:
		return e.createColumnarIndex(ctx, ti, indexName, indexPartSpecifications, indexOption, ifNotExists)
	}
	unique := keyType == ast.IndexKeyTypeUnique
	schema, t, err := e.getSchemaAndTableByIdent(ti)
	if err != nil {
		return errors.Trace(err)
	}

	if t.Meta().TableCacheStatusType != model.TableCacheStatusDisable {
		return errors.Trace(dbterror.ErrOptOnCacheTable.GenWithStackByArgs("Create Index"))
	}

	metaBuildCtx := NewMetaBuildContextWithSctx(ctx)
	indexName, hiddenCols, err := checkIndexNameAndColumns(metaBuildCtx, t, indexName, indexPartSpecifications, model.ColumnarIndexTypeNA, ifNotExists)
	if err != nil {
		return errors.Trace(err)
	}
	if len(indexName.L) == 0 {
		// It means that there is already an index exists with same name
		return nil
	}

	tblInfo := t.Meta()
	finalColumns := make([]*model.ColumnInfo, len(tblInfo.Columns), len(tblInfo.Columns)+len(hiddenCols))
	copy(finalColumns, tblInfo.Columns)
	finalColumns = append(finalColumns, hiddenCols...)
	// Check before the job is put to the queue.
	// This check is redundant, but useful. If DDL check fail before the job is put
	// to job queue, the fail path logic is particularly fast.
	// After DDL job is put to the queue, and if the check fail, TiDB will run the DDL cancel logic.
	// The recover step causes DDL wait a few seconds, makes the unit test painfully slow.
	// For same reason, decide whether index is global here.
	indexColumns, _, err := buildIndexColumns(metaBuildCtx, finalColumns, indexPartSpecifications, model.ColumnarIndexTypeNA)
	if err != nil {
		return errors.Trace(err)
	}

	if err = checkCreateGlobalIndex(ctx.GetSessionVars().StmtCtx.ErrCtx(), tblInfo, indexName.O, indexColumns, unique, indexOption != nil && indexOption.Global); err != nil {
		return err
	}

	// May be truncate comment here, when index comment too long and sql_mode is't strict.
	if indexOption != nil {
		sessionVars := ctx.GetSessionVars()
		if _, err = validateCommentLength(sessionVars.StmtCtx.ErrCtx(), sessionVars.SQLMode, indexName.String(), &indexOption.Comment, dbterror.ErrTooLongIndexComment); err != nil {
			return errors.Trace(err)
		}
	}

	if indexOption != nil && indexOption.Tp == ast.IndexTypeHypo { // for hypo-index
		indexInfo, err := BuildIndexInfo(metaBuildCtx, tblInfo, indexName, false, unique, model.ColumnarIndexTypeNA,
			indexPartSpecifications, indexOption, model.StatePublic)
		if err != nil {
			return err
		}
		return e.addHypoIndexIntoCtx(ctx, ti.Schema, ti.Name, indexInfo)
	}

	splitOpt, err := buildIndexPresplitOpt(indexOption)
	if err != nil {
		return errors.Trace(err)
	}

	// global is set to  'false' is just there to be backwards compatible,
	// to avoid unmarshal issues, it is now part of indexOption.
	global := false
	job := buildAddIndexJobWithoutTypeAndArgs(ctx, schema, t)

	job.Version = model.GetJobVerInUse()
	job.Type = model.ActionAddIndex
	job.CDCWriteSource = ctx.GetSessionVars().CDCWriteSource
	job.AddSystemVars(vardef.TiDBEnableDDLAnalyze, getEnableDDLAnalyze(ctx))
	job.AddSystemVars(vardef.TiDBAnalyzeVersion, getAnalyzeVersion(ctx))

	err = initJobReorgMetaFromVariables(e.ctx, job, t, ctx)
	if err != nil {
		return errors.Trace(err)
	}

	var conditionString string
	if indexOption != nil {
		conditionString, err = CheckAndBuildIndexConditionString(tblInfo, indexOption.Condition)
		if err != nil {
			return errors.Trace(err)
		}
		if len(conditionString) > 0 && !job.ReorgMeta.IsFastReorg {
			return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs("add partial index without fast reorg is not supported")
		}
	}
	args := &model.ModifyIndexArgs{
		IndexArgs: []*model.IndexArg{{
			Unique:                  unique,
			IndexName:               indexName,
			IndexPartSpecifications: indexPartSpecifications,
			IndexOption:             indexOption,
			HiddenCols:              hiddenCols,
			Global:                  global,
			SplitOpt:                splitOpt,
			ConditionString:         conditionString,
		}},
		OpType: model.OpAddIndex,
	}

	err = e.doDDLJob2(ctx, job, args)
	// key exists, but if_not_exists flags is true, so we ignore this error.
	if dbterror.ErrDupKeyName.Equal(err) && ifNotExists {
		ctx.GetSessionVars().StmtCtx.AppendNote(err)
		return nil
	}
	return errors.Trace(err)
}

func buildIndexPresplitOpt(indexOpt *ast.IndexOption) (*model.IndexArgSplitOpt, error) {
	if indexOpt == nil {
		return nil, nil
	}
	opt := indexOpt.SplitOpt
	if opt == nil {
		return nil, nil
	}
	if len(opt.ValueLists) > 0 {
		valLists := make([][]string, 0, len(opt.ValueLists))
		for _, lst := range opt.ValueLists {
			values := make([]string, 0, len(lst))
			for _, exp := range lst {
				var sb strings.Builder
				rCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
				err := exp.Restore(rCtx)
				if err != nil {
					return nil, errors.Trace(err)
				}
				values = append(values, sb.String())
			}
			valLists = append(valLists, values)
		}
		return &model.IndexArgSplitOpt{
			Num:        opt.Num,
			ValueLists: valLists,
		}, nil
	}

	lowers := make([]string, 0, len(opt.Lower))
	for _, expL := range opt.Lower {
		var sb strings.Builder
		rCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
		err := expL.Restore(rCtx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		lowers = append(lowers, sb.String())
	}
	uppers := make([]string, 0, len(opt.Upper))
	for _, expU := range opt.Upper {
		var sb strings.Builder
		rCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
		err := expU.Restore(rCtx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		uppers = append(uppers, sb.String())
	}
	maxSplitRegionNum := int64(config.GetGlobalConfig().SplitRegionMaxNum)
	if opt.Num > maxSplitRegionNum {
		return nil, errors.Errorf("Split index region num exceeded the limit %v", maxSplitRegionNum)
	} else if opt.Num < 1 {
		return nil, errors.Errorf("Split index region num should be greater than 0")
	}
	return &model.IndexArgSplitOpt{
		Lower: lowers,
		Upper: uppers,
		Num:   opt.Num,
	}, nil
}

// LastReorgMetaFastReorgDisabled is used for test.
var LastReorgMetaFastReorgDisabled bool

func buildFKInfo(fkName ast.CIStr, keys []*ast.IndexPartSpecification, refer *ast.ReferenceDef, cols []*table.Column) (*model.FKInfo, error) {
	if len(keys) != len(refer.IndexPartSpecifications) {
		return nil, infoschema.ErrForeignKeyNotMatch.GenWithStackByArgs(fkName, "Key reference and table reference don't match")
	}
	if err := checkTooLongForeignKey(fkName); err != nil {
		return nil, err
	}
	if err := checkTooLongSchema(refer.Table.Schema); err != nil {
		return nil, err
	}
	if err := checkTooLongTable(refer.Table.Name); err != nil {
		return nil, err
	}

	// all base columns of stored generated columns
	baseCols := make(map[string]struct{})
	for _, col := range cols {
		if col.IsGenerated() && col.GeneratedStored {
			for name := range col.Dependences {
				baseCols[name] = struct{}{}
			}
		}
	}

	fkInfo := &model.FKInfo{
		Name:      fkName,
		RefSchema: refer.Table.Schema,
		RefTable:  refer.Table.Name,
		Cols:      make([]ast.CIStr, len(keys)),
	}
	if vardef.EnableForeignKey.Load() {
		fkInfo.Version = model.FKVersion1
	}

	for i, key := range keys {
		// Check add foreign key to generated columns
		// For more detail, see https://dev.mysql.com/doc/refman/8.0/en/innodb-foreign-key-constraints.html#innodb-foreign-key-generated-columns
		for _, col := range cols {
			if col.Name.L != key.Column.Name.L {
				continue
			}
			if col.IsGenerated() {
				// Check foreign key on virtual generated columns
				if !col.GeneratedStored {
					return nil, infoschema.ErrForeignKeyCannotUseVirtualColumn.GenWithStackByArgs(fkInfo.Name.O, col.Name.O)
				}

				// Check wrong reference options of foreign key on stored generated columns
				switch refer.OnUpdate.ReferOpt {
				case ast.ReferOptionCascade, ast.ReferOptionSetNull, ast.ReferOptionSetDefault:
					//nolint: gosec
					return nil, dbterror.ErrWrongFKOptionForGeneratedColumn.GenWithStackByArgs("ON UPDATE " + refer.OnUpdate.ReferOpt.String())
				}
				switch refer.OnDelete.ReferOpt {
				case ast.ReferOptionSetNull, ast.ReferOptionSetDefault:
					//nolint: gosec
					return nil, dbterror.ErrWrongFKOptionForGeneratedColumn.GenWithStackByArgs("ON DELETE " + refer.OnDelete.ReferOpt.String())
				}
				continue
			}
			// Check wrong reference options of foreign key on base columns of stored generated columns
			if _, ok := baseCols[col.Name.L]; ok {
				switch refer.OnUpdate.ReferOpt {
				case ast.ReferOptionCascade, ast.ReferOptionSetNull, ast.ReferOptionSetDefault:
					return nil, infoschema.ErrCannotAddForeign
				}
				switch refer.OnDelete.ReferOpt {
				case ast.ReferOptionCascade, ast.ReferOptionSetNull, ast.ReferOptionSetDefault:
					return nil, infoschema.ErrCannotAddForeign
				}
			}
		}
		col := table.FindCol(cols, key.Column.Name.O)
		if col == nil {
			return nil, dbterror.ErrKeyColumnDoesNotExits.GenWithStackByArgs(key.Column.Name)
		}
		if mysql.HasNotNullFlag(col.GetFlag()) && (refer.OnDelete.ReferOpt == ast.ReferOptionSetNull || refer.OnUpdate.ReferOpt == ast.ReferOptionSetNull) {
			return nil, infoschema.ErrForeignKeyColumnNotNull.GenWithStackByArgs(col.Name.O, fkName)
		}
		fkInfo.Cols[i] = key.Column.Name
	}

	fkInfo.RefCols = make([]ast.CIStr, len(refer.IndexPartSpecifications))
	for i, key := range refer.IndexPartSpecifications {
		if err := checkTooLongColumn(key.Column.Name); err != nil {
			return nil, err
		}
		fkInfo.RefCols[i] = key.Column.Name
	}

	fkInfo.OnDelete = int(refer.OnDelete.ReferOpt)
	fkInfo.OnUpdate = int(refer.OnUpdate.ReferOpt)

	return fkInfo, nil
}

func (e *executor) CreateForeignKey(ctx sessionctx.Context, ti ast.Ident, fkName ast.CIStr, keys []*ast.IndexPartSpecification, refer *ast.ReferenceDef) error {
	is := e.infoCache.GetLatest()
	schema, ok := is.SchemaByName(ti.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ti.Schema)
	}

	t, err := is.TableByName(context.Background(), ti.Schema, ti.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ti.Schema, ti.Name))
	}
	if t.Meta().TempTableType != model.TempTableNone {
		return infoschema.ErrCannotAddForeign
	}

	if fkName.L == "" {
		fkName = ast.NewCIStr(fmt.Sprintf("fk_%d", t.Meta().MaxForeignKeyID+1))
	}
	err = checkFKDupName(t.Meta(), fkName)
	if err != nil {
		return err
	}
	fkInfo, err := buildFKInfo(fkName, keys, refer, t.Cols())
	if err != nil {
		return errors.Trace(err)
	}
	fkCheck := ctx.GetSessionVars().ForeignKeyChecks
	err = checkAddForeignKeyValid(is, schema.Name.L, t.Meta(), fkInfo, fkCheck)
	if err != nil {
		return err
	}
	if model.FindIndexByColumns(t.Meta(), t.Meta().Indices, fkInfo.Cols...) == nil {
		// Need to auto create index for fk cols
		if ctx.GetSessionVars().StmtCtx.MultiSchemaInfo == nil {
			ctx.GetSessionVars().StmtCtx.MultiSchemaInfo = model.NewMultiSchemaInfo()
		}
		indexPartSpecifications := make([]*ast.IndexPartSpecification, 0, len(fkInfo.Cols))
		for _, col := range fkInfo.Cols {
			indexPartSpecifications = append(indexPartSpecifications, &ast.IndexPartSpecification{
				Column: &ast.ColumnName{Name: col},
				Length: types.UnspecifiedLength, // Index prefixes on foreign key columns are not supported.
			})
		}
		indexOption := &ast.IndexOption{}
		err = e.createIndex(ctx, ti, ast.IndexKeyTypeNone, fkInfo.Name, indexPartSpecifications, indexOption, false)
		if err != nil {
			return err
		}
	}

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        t.Meta().ID,
		SchemaName:     schema.Name.L,
		TableName:      t.Meta().Name.L,
		Type:           model.ActionAddForeignKey,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{
			{
				Database: schema.Name.L,
				Table:    t.Meta().Name.L,
			},
			{
				Database: fkInfo.RefSchema.L,
				Table:    fkInfo.RefTable.L,
				Mode:     model.SharedInvolving,
			},
		},
		SQLMode: ctx.GetSessionVars().SQLMode,
	}

	args := &model.AddForeignKeyArgs{
		FkInfo:  fkInfo,
		FkCheck: fkCheck,
	}
	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}
