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
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/metabuild"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
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
