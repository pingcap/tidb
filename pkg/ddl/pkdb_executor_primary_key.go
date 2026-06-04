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
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/dbterror"
)

// createPrimaryKeyWithTableInfo is used by multi-schema change to build the primary key job when later
// specs depend on earlier specs (e.g. ADD COLUMN then ADD PRIMARY KEY on the new column).
//
// It runs the same prechecks as CreatePrimaryKey, but uses the supplied TableInfo instead of the
// current infoschema table.
func (e *executor) createPrimaryKeyWithTableInfo(
	ctx sessionctx.Context,
	schema *model.DBInfo,
	tblInfo *model.TableInfo,
	indexName pmodel.CIStr,
	indexPartSpecifications []*ast.IndexPartSpecification,
	indexOption *ast.IndexOption,
) error {
	if indexOption != nil && indexOption.PrimaryKeyTp == pmodel.PrimaryKeyTypeClustered {
		return dbterror.ErrUnsupportedModifyPrimaryKey.GenWithStack("Adding clustered primary key is not supported. " +
			"Please consider adding NONCLUSTERED primary key instead")
	}

	if err := checkTooLongIndex(indexName); err != nil {
		return dbterror.ErrTooLongIdent.GenWithStackByArgs(mysql.PrimaryKeyName)
	}

	indexName = pmodel.NewCIStr(mysql.PrimaryKeyName)
	if indexInfo := tblInfo.FindIndexByName(indexName.L); indexInfo != nil ||
		// If the table's PKIsHandle is true, it also means that this table has a primary key.
		tblInfo.PKIsHandle {
		return infoschema.ErrMultiplePriKey
	}

	// Primary keys cannot include expression index parts. A primary key requires the generated column to be stored,
	// but expression index parts are implemented as virtual generated columns, not stored generated columns.
	for _, idxPart := range indexPartSpecifications {
		if idxPart.Expr != nil {
			return dbterror.ErrFunctionalIndexPrimaryKey
		}
	}

	// Do the same prechecks as CreatePrimaryKey. The worker will re-check again at execution time.
	indexColumns, _, err := buildIndexColumns(NewMetaBuildContextWithSctx(ctx), tblInfo.Columns, indexPartSpecifications, false)
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

	sqlMode := ctx.GetSessionVars().SQLMode
	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        tblInfo.ID,
		SchemaName:     schema.Name.L,
		FullSchemaName: schema.Name,
		TableName:      tblInfo.Name.L,
		FullTableName:  tblInfo.Name,
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
		}},
		OpType: model.OpAddIndex,
	}

	if err = initJobReorgMetaFromVariables(job, ctx); err != nil {
		return err
	}
	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}
