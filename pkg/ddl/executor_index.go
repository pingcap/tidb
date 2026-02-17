// Copyright 2016 PingCAP, Inc.
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

// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

package ddl

import (
	"context"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/metabuild"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror"
)

// getAnonymousIndexPrefix returns the prefix for anonymous index name.
// Column name of vector index IndexPartSpecifications is nil,
// so we need a different prefix to distinguish between vector index and expression index.
func getAnonymousIndexPrefix(isVector bool) string {
	if isVector {
		return "vector_index"
	}
	return "expression_index"
}

// GetName4AnonymousIndex returns a valid name for anonymous index.
func GetName4AnonymousIndex(t table.Table, colName ast.CIStr, idxName ast.CIStr) ast.CIStr {
	// `id` is used to indicated the index name's suffix.
	id := 2
	l := len(t.Indices())
	indexName := colName
	if idxName.O != "" {
		// Use the provided index name, it only happens when the original index name is too long and be truncated.
		indexName = idxName
		id = 3
	}
	if strings.EqualFold(indexName.L, mysql.PrimaryKeyName) {
		indexName = ast.NewCIStr(fmt.Sprintf("%s_%d", colName.O, id))
		id = 3
	}
	for i := 0; i < l; i++ {
		if t.Indices()[i].Meta().Name.L == indexName.L {
			indexName = ast.NewCIStr(fmt.Sprintf("%s_%d", colName.O, id))
			if err := checkTooLongIndex(indexName); err != nil {
				indexName = GetName4AnonymousIndex(t, ast.NewCIStr(colName.O[:30]), ast.NewCIStr(fmt.Sprintf("%s_%d", colName.O[:30], 2)))
			}
			i = -1
			id++
		}
	}
	return indexName
}

func checkCreateGlobalIndex(ec errctx.Context, tblInfo *model.TableInfo, indexName string, indexColumns []*model.IndexColumn, isUnique bool, isGlobal bool) error {
	pi := tblInfo.GetPartitionInfo()
	if isGlobal && pi == nil {
		return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("Global Index on non-partitioned table")
	}
	if isUnique && pi != nil {
		ck, err := checkPartitionKeysConstraint(tblInfo.GetPartitionInfo(), indexColumns, tblInfo)
		if err != nil {
			return err
		}
		if !ck && !isGlobal {
			// index columns does not contain all partition columns, must be global
			return dbterror.ErrGlobalIndexNotExplicitlySet.GenWithStackByArgs(indexName)
		}
	}
	if isGlobal {
		validateGlobalIndexWithGeneratedColumns(ec, tblInfo, indexName, indexColumns)
	}
	return nil
}

func (e *executor) CreatePrimaryKey(ctx sessionctx.Context, ti ast.Ident, indexName ast.CIStr,
	indexPartSpecifications []*ast.IndexPartSpecification, indexOption *ast.IndexOption) error {
	if indexOption != nil && indexOption.PrimaryKeyTp == ast.PrimaryKeyTypeClustered {
		return dbterror.ErrUnsupportedModifyPrimaryKey.GenWithStack("Adding clustered primary key is not supported. " +
			"Please consider adding NONCLUSTERED primary key instead")
	}
	schema, t, err := e.getSchemaAndTableByIdent(ti)
	if err != nil {
		return errors.Trace(err)
	}

	if err = checkTooLongIndex(indexName); err != nil {
		return dbterror.ErrTooLongIdent.GenWithStackByArgs(mysql.PrimaryKeyName)
	}

	indexName = ast.NewCIStr(mysql.PrimaryKeyName)
	if indexInfo := t.Meta().FindIndexByName(indexName.L); indexInfo != nil ||
		// If the table's PKIsHandle is true, it also means that this table has a primary key.
		t.Meta().PKIsHandle {
		return infoschema.ErrMultiplePriKey
	}

	// Primary keys cannot include expression index parts. A primary key requires the generated column to be stored,
	// but expression index parts are implemented as virtual generated columns, not stored generated columns.
	for _, idxPart := range indexPartSpecifications {
		if idxPart.Expr != nil {
			return dbterror.ErrFunctionalIndexPrimaryKey
		}
	}

	tblInfo := t.Meta()
	// Check before the job is put to the queue.
	// This check is redundant, but useful. If DDL check fail before the job is put
	// to job queue, the fail path logic is particularly fast.
	// After DDL job is put to the queue, and if the check fail, TiDB will run the DDL cancel logic.
	// The recover step causes DDL wait a few seconds, makes the unit test painfully slow.
	// For same reason, decide whether index is global here.
	indexColumns, _, err := buildIndexColumns(NewMetaBuildContextWithSctx(ctx), tblInfo.Columns, indexPartSpecifications, model.ColumnarIndexTypeNA)
	if err != nil {
		return errors.Trace(err)
	}
	if _, err = CheckPKOnGeneratedColumn(tblInfo, indexPartSpecifications); err != nil {
		return err
	}

	if err = checkCreateGlobalIndex(ctx.GetSessionVars().StmtCtx.ErrCtx(), tblInfo, "PRIMARY", indexColumns, true, indexOption != nil && indexOption.Global); err != nil {
		return err
	}

	// May be truncate comment here, when index comment too long and sql_mode is't strict.
	if indexOption != nil {
		sessionVars := ctx.GetSessionVars()
		if _, err = validateCommentLength(sessionVars.StmtCtx.ErrCtx(), sessionVars.SQLMode, indexName.String(), &indexOption.Comment, dbterror.ErrTooLongIndexComment); err != nil {
			return errors.Trace(err)
		}
	}

	splitOpt, err := buildIndexPresplitOpt(indexOption)
	if err != nil {
		return errors.Trace(err)
	}
	sqlMode := ctx.GetSessionVars().SQLMode
	// global is set to  'false' is just there to be backwards compatible,
	// to avoid unmarshal issues, it is now part of indexOption.
	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        t.Meta().ID,
		SchemaName:     schema.Name.L,
		TableName:      t.Meta().Name.L,
		Type:           model.ActionAddPrimaryKey,
		BinlogInfo:     &model.HistoryInfo{},
		ReorgMeta:      nil,
		Priority:       ctx.GetSessionVars().DDLReorgPriority,
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	args := &model.ModifyIndexArgs{
		IndexArgs: []*model.IndexArg{{
			Unique:                  true,
			IndexName:               indexName,
			IndexPartSpecifications: indexPartSpecifications,
			IndexOption:             indexOption,
			SQLMode:                 sqlMode,
			Global:                  false,
			IsPK:                    true,
			SplitOpt:                splitOpt,
		}},
		OpType: model.OpAddIndex,
	}

	err = initJobReorgMetaFromVariables(e.ctx, job, t, ctx)
	if err != nil {
		return err
	}

	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}

func checkIndexNameAndColumns(ctx *metabuild.Context, t table.Table, indexName ast.CIStr,
	indexPartSpecifications []*ast.IndexPartSpecification, columnarIndexType model.ColumnarIndexType, ifNotExists bool) (ast.CIStr, []*model.ColumnInfo, error) {
	// Deal with anonymous index.
	if len(indexName.L) == 0 {
		colName := ast.NewCIStr(getAnonymousIndexPrefix(columnarIndexType == model.ColumnarIndexTypeVector))
		if indexPartSpecifications[0].Column != nil {
			colName = indexPartSpecifications[0].Column.Name
		}
		indexName = GetName4AnonymousIndex(t, colName, ast.NewCIStr(""))
	}

	var err error
	if indexInfo := t.Meta().FindIndexByName(indexName.L); indexInfo != nil {
		if indexInfo.State != model.StatePublic {
			// NOTE: explicit error message. See issue #18363.
			err = dbterror.ErrDupKeyName.GenWithStack("index already exist %s; "+
				"a background job is trying to add the same index, "+
				"please check by `ADMIN SHOW DDL JOBS`", indexName)
		} else {
			err = dbterror.ErrDupKeyName.GenWithStackByArgs(indexName)
		}
		if ifNotExists {
			ctx.AppendNote(err)
			return ast.CIStr{}, nil, nil
		}
		return ast.CIStr{}, nil, err
	}

	if err = checkTooLongIndex(indexName); err != nil {
		return ast.CIStr{}, nil, errors.Trace(err)
	}

	// Build hidden columns if necessary.
	var hiddenCols []*model.ColumnInfo
	if columnarIndexType == model.ColumnarIndexTypeNA {
		hiddenCols, err = buildHiddenColumnInfoWithCheck(ctx, indexPartSpecifications, indexName, t.Meta(), t.Cols())
		if err != nil {
			return ast.CIStr{}, nil, err
		}
	}
	if err = checkAddColumnTooManyColumns(len(t.Cols()) + len(hiddenCols)); err != nil {
		return ast.CIStr{}, nil, errors.Trace(err)
	}

	return indexName, hiddenCols, nil
}

func checkTableTypeForColumnarIndex(tblInfo *model.TableInfo) error {
	if tblInfo.TableCacheStatusType != model.TableCacheStatusDisable {
		return dbterror.ErrOptOnCacheTable.GenWithStackByArgs("create columnar index")
	}
	if tblInfo.TempTableType != model.TempTableNone {
		return dbterror.ErrOptOnTemporaryTable.FastGenByArgs("columnar index")
	}
	if tblInfo.GetPartitionInfo() != nil {
		return dbterror.ErrUnsupportedAddColumnarIndex.FastGenByArgs("unsupported partition table")
	}
	if tblInfo.TiFlashReplica == nil || tblInfo.TiFlashReplica.Count == 0 {
		return dbterror.ErrUnsupportedAddColumnarIndex.FastGenByArgs("columnar replica must exist to create vector index, columnar index or fulltext index")
	}

	return nil
}

func (e *executor) createColumnarIndex(ctx sessionctx.Context, ti ast.Ident, indexName ast.CIStr,
	indexPartSpecifications []*ast.IndexPartSpecification, indexOption *ast.IndexOption, ifNotExists bool) error {
	schema, t, err := e.getSchemaAndTableByIdent(ti)
	if err != nil {
		return errors.Trace(err)
	}

	tblInfo := t.Meta()
	if err := checkTableTypeForColumnarIndex(tblInfo); err != nil {
		return errors.Trace(err)
	}

	var columnarIndexType model.ColumnarIndexType
	switch indexOption.Tp {
	case ast.IndexTypeInverted:
		columnarIndexType = model.ColumnarIndexTypeInverted
	case ast.IndexTypeVector:
		columnarIndexType = model.ColumnarIndexTypeVector
	case ast.IndexTypeFulltext:
		columnarIndexType = model.ColumnarIndexTypeFulltext
	default:
		return dbterror.ErrUnsupportedIndexType.GenWithStackByArgs(indexOption.Tp)
	}

	metaBuildCtx := NewMetaBuildContextWithSctx(ctx)
	indexName, _, err = checkIndexNameAndColumns(metaBuildCtx, t, indexName, indexPartSpecifications, columnarIndexType, ifNotExists)
	if err != nil {
		return errors.Trace(err)
	}

	// Do some checks here to fast fail the DDL job.
	var funcExpr string
	switch columnarIndexType {
	case model.ColumnarIndexTypeInverted:
		if _, err := buildInvertedInfoWithCheck(indexPartSpecifications, tblInfo); err != nil {
			return errors.Trace(err)
		}
	case model.ColumnarIndexTypeVector:
		if _, funcExpr, err = buildVectorInfoWithCheck(indexPartSpecifications, tblInfo); err != nil {
			return errors.Trace(err)
		}
	case model.ColumnarIndexTypeFulltext:
		if _, err = buildFullTextInfoWithCheck(indexPartSpecifications, indexOption, tblInfo); err != nil {
			return errors.Trace(err)
		}
	}

	// Check before the job is put to the queue.
	// This check is redundant, but useful. If DDL check fail before the job is put
	// to job queue, the fail path logic is particularly fast.
	// After DDL job is put to the queue, and if the check fail, TiDB will run the DDL cancel logic.
	// The recover step causes DDL wait a few seconds, makes the unit test painfully slow.
	// For same reason, decide whether index is global here.
	_, _, err = buildIndexColumns(metaBuildCtx, tblInfo.Columns, indexPartSpecifications, columnarIndexType)
	if err != nil {
		return errors.Trace(err)
	}

	// May be truncate comment here, when index comment too long and sql_mode it's strict.
	sessionVars := ctx.GetSessionVars()
	if _, err = validateCommentLength(sessionVars.StmtCtx.ErrCtx(), sessionVars.SQLMode, indexName.String(), &indexOption.Comment, dbterror.ErrTooLongTableComment); err != nil {
		return errors.Trace(err)
	}

	job := buildAddIndexJobWithoutTypeAndArgs(ctx, schema, t)
	job.Version = model.GetJobVerInUse()
	job.Type = model.ActionAddColumnarIndex
	// indexPartSpecifications[0].Expr can not be unmarshaled, so we set it to nil.
	indexPartSpecifications[0].Expr = nil

	// TODO: support CDCWriteSource

	args := &model.ModifyIndexArgs{
		IndexArgs: []*model.IndexArg{{
			IndexName:               indexName,
			IndexPartSpecifications: indexPartSpecifications,
			IndexOption:             indexOption,
			FuncExpr:                funcExpr,
			IsColumnar:              true,
			ColumnarIndexType:       columnarIndexType,
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

func buildAddIndexJobWithoutTypeAndArgs(ctx sessionctx.Context, schema *model.DBInfo, t table.Table) *model.Job {
	charset, collate := ctx.GetSessionVars().GetCharsetInfo()
	job := &model.Job{
		SchemaID:    schema.ID,
		TableID:     t.Meta().ID,
		SchemaName:  schema.Name.L,
		TableName:   t.Meta().Name.L,
		BinlogInfo:  &model.HistoryInfo{},
		Priority:    ctx.GetSessionVars().DDLReorgPriority,
		Charset:     charset,
		Collate:     collate,
		SQLMode:     ctx.GetSessionVars().SQLMode,
		SessionVars: make(map[string]string),
	}
	return job
}

func (e *executor) CreateIndex(ctx sessionctx.Context, stmt *ast.CreateIndexStmt) error {
	ident := ast.Ident{Schema: stmt.Table.Schema, Name: stmt.Table.Name}
	return e.createIndex(ctx, ident, stmt.KeyType, ast.NewCIStr(stmt.IndexName),
		stmt.IndexPartSpecifications, stmt.IndexOption, stmt.IfNotExists)
}

// addHypoIndexIntoCtx adds this index as a hypo-index into this ctx.
func (*executor) addHypoIndexIntoCtx(ctx sessionctx.Context, schemaName, tableName ast.CIStr, indexInfo *model.IndexInfo) error {
	sctx := ctx.GetSessionVars()
	indexName := indexInfo.Name

	if sctx.HypoIndexes == nil {
		sctx.HypoIndexes = make(map[string]map[string]map[string]*model.IndexInfo)
	}
	if sctx.HypoIndexes[schemaName.L] == nil {
		sctx.HypoIndexes[schemaName.L] = make(map[string]map[string]*model.IndexInfo)
	}
	if sctx.HypoIndexes[schemaName.L][tableName.L] == nil {
		sctx.HypoIndexes[schemaName.L][tableName.L] = make(map[string]*model.IndexInfo)
	}
	if _, exist := sctx.HypoIndexes[schemaName.L][tableName.L][indexName.L]; exist {
		return errors.Trace(errors.Errorf("conflict hypo index name %s", indexName.L))
	}

	sctx.HypoIndexes[schemaName.L][tableName.L][indexName.L] = indexInfo
	return nil
}

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

func (e *executor) DropForeignKey(ctx sessionctx.Context, ti ast.Ident, fkName ast.CIStr) error {
	is := e.infoCache.GetLatest()
	schema, ok := is.SchemaByName(ti.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ti.Schema)
	}

	t, err := is.TableByName(context.Background(), ti.Schema, ti.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ti.Schema, ti.Name))
	}

	foundFK := false
	for _, fk := range t.Meta().ForeignKeys {
		if fk.Name.L == fkName.L {
			foundFK = true
			break
		}
	}
	if !foundFK {
		return infoschema.ErrForeignKeyNotExists.GenWithStackByArgs(fkName)
	}

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        t.Meta().ID,
		SchemaName:     schema.Name.L,
		SchemaState:    model.StatePublic,
		TableName:      t.Meta().Name.L,
		Type:           model.ActionDropForeignKey,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	args := &model.DropForeignKeyArgs{FkName: fkName}
	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}

func (e *executor) DropIndex(ctx sessionctx.Context, stmt *ast.DropIndexStmt) error {
	ti := ast.Ident{Schema: stmt.Table.Schema, Name: stmt.Table.Name}
	err := e.dropIndex(ctx, ti, ast.NewCIStr(stmt.IndexName), stmt.IfExists, stmt.IsHypo)
	if (infoschema.ErrDatabaseNotExists.Equal(err) || infoschema.ErrTableNotExists.Equal(err)) && stmt.IfExists {
		err = nil
	}
	return err
}

// dropHypoIndexFromCtx drops this hypo-index from this ctx.
func (*executor) dropHypoIndexFromCtx(ctx sessionctx.Context, schema, table, index ast.CIStr, ifExists bool) error {
	sctx := ctx.GetSessionVars()
	if sctx.HypoIndexes != nil &&
		sctx.HypoIndexes[schema.L] != nil &&
		sctx.HypoIndexes[schema.L][table.L] != nil &&
		sctx.HypoIndexes[schema.L][table.L][index.L] != nil {
		delete(sctx.HypoIndexes[schema.L][table.L], index.L)
		return nil
	}
	if !ifExists {
		return dbterror.ErrCantDropFieldOrKey.GenWithStack("index %s doesn't exist", index)
	}
	return nil
}

// dropIndex drops the specified index.
// isHypo is used to indicate whether this operation is for a hypo-index.
func (e *executor) dropIndex(ctx sessionctx.Context, ti ast.Ident, indexName ast.CIStr, ifExist, isHypo bool) error {
	is := e.infoCache.GetLatest()
	schema, ok := is.SchemaByName(ti.Schema)
	if !ok {
		return errors.Trace(infoschema.ErrDatabaseNotExists)
	}
	t, err := is.TableByName(context.Background(), ti.Schema, ti.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ti.Schema, ti.Name))
	}
	if t.Meta().TableCacheStatusType != model.TableCacheStatusDisable {
		return errors.Trace(dbterror.ErrOptOnCacheTable.GenWithStackByArgs("Drop Index"))
	}

	if isHypo {
		return e.dropHypoIndexFromCtx(ctx, ti.Schema, ti.Name, indexName, ifExist)
	}

	indexInfo := t.Meta().FindIndexByName(indexName.L)

	isPK, err := CheckIsDropPrimaryKey(indexName, indexInfo, t)
	if err != nil {
		return err
	}

	if !ctx.GetSessionVars().InRestrictedSQL && ctx.GetSessionVars().PrimaryKeyRequired && isPK {
		return infoschema.ErrTableWithoutPrimaryKey
	}

	if indexInfo == nil {
		err = dbterror.ErrCantDropFieldOrKey.GenWithStack("index %s doesn't exist", indexName)
		if ifExist {
			ctx.GetSessionVars().StmtCtx.AppendNote(err)
			return nil
		}
		return err
	}

	err = checkIndexNeededInForeignKey(is, schema.Name.L, t.Meta(), indexInfo)
	if err != nil {
		return err
	}

	jobTp := model.ActionDropIndex
	if isPK {
		jobTp = model.ActionDropPrimaryKey
	}

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        t.Meta().ID,
		SchemaName:     schema.Name.L,
		SchemaState:    indexInfo.State,
		TableName:      t.Meta().Name.L,
		Type:           jobTp,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	args := &model.ModifyIndexArgs{
		IndexArgs: []*model.IndexArg{{
			IndexName: indexName,
			IfExist:   ifExist,
		}},
		OpType: model.OpDropIndex,
	}
	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}

// CheckIsDropPrimaryKey checks if we will drop PK, there are many PK implementations so we provide a helper function.
func CheckIsDropPrimaryKey(indexName ast.CIStr, indexInfo *model.IndexInfo, t table.Table) (bool, error) {
	var isPK bool
	if indexName.L == strings.ToLower(mysql.PrimaryKeyName) &&
		// Before we fixed #14243, there might be a general index named `primary` but not a primary key.
		(indexInfo == nil || indexInfo.Primary) {
		isPK = true
	}
	if isPK {
		// If the table's PKIsHandle is true, we can't find the index from the table. So we check the value of PKIsHandle.
		if indexInfo == nil && !t.Meta().PKIsHandle {
			return isPK, dbterror.ErrCantDropFieldOrKey.GenWithStackByArgs("PRIMARY")
		}
		if t.Meta().IsCommonHandle || t.Meta().PKIsHandle {
			return isPK, dbterror.ErrUnsupportedModifyPrimaryKey.GenWithStack("Unsupported drop primary key when the table is using clustered index")
		}
	}

	return isPK, nil
}

// validateCommentLength checks comment length of table, column, or index
// If comment length is more than the standard length truncate it
// and store the comment length upto the standard comment length size.
func validateCommentLength(ec errctx.Context, sqlMode mysql.SQLMode, name string, comment *string, errTooLongComment *terror.Error) (string, error) {
	if comment == nil {
		return "", nil
	}

	maxLen := MaxCommentLength
	// The maximum length of table comment in MySQL 5.7 is 2048
	// Other comment is 1024
	switch errTooLongComment {
	case dbterror.ErrTooLongTableComment:
		maxLen *= 2
	case dbterror.ErrTooLongFieldComment, dbterror.ErrTooLongIndexComment, dbterror.ErrTooLongTablePartitionComment:
	default:
		// add more types of terror.Error if need
	}
	if len(*comment) > maxLen {
		err := errTooLongComment.GenWithStackByArgs(name, maxLen)
		if sqlMode.HasStrictMode() {
			// may be treated like an error.
			return "", err
		}
		ec.AppendWarning(err)
		*comment = (*comment)[:maxLen]
	}
	return *comment, nil
}

func validateGlobalIndexWithGeneratedColumns(ec errctx.Context, tblInfo *model.TableInfo, indexName string, indexColumns []*model.IndexColumn) {
	// Auto analyze is not effective when a global index contains prefix columns or virtual generated columns.
	for _, col := range indexColumns {
		colInfo := tblInfo.Columns[col.Offset]
		isPrefixCol := col.Length != types.UnspecifiedLength
		if colInfo.IsVirtualGenerated() || isPrefixCol {
			ec.AppendWarning(dbterror.ErrWarnGlobalIndexNeedManuallyAnalyze.FastGenByArgs(indexName))
			return
		}
	}
}
