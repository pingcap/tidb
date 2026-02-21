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

package ddl

import (
	"context"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror"
)

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
