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
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
)
func onRenameTable(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	args, err := model.GetRenameTableArgs(job)
	if err != nil {
		// Invalid arguments, cancel this job.
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	jobCtx.jobArgs = args

	oldSchemaID, oldSchemaName, tableName := args.OldSchemaID, args.OldSchemaName, args.NewTableName
	if job.SchemaState == model.StatePublic {
		return finishJobRenameTable(jobCtx, job)
	}
	newSchemaID := job.SchemaID
	err = checkTableNotExists(jobCtx.infoCache, newSchemaID, tableName.L)
	if err != nil {
		if infoschema.ErrDatabaseNotExists.Equal(err) || infoschema.ErrTableExists.Equal(err) {
			job.State = model.JobStateCancelled
		}
		return ver, errors.Trace(err)
	}

	metaMut := jobCtx.metaMut
	tblInfo, err := GetTableInfoAndCancelFaultJob(metaMut, job, oldSchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	oldTableName := tblInfo.Name
	ver, err = checkAndRenameTables(metaMut, job, tblInfo, args)
	if err != nil {
		return ver, errors.Trace(err)
	}
	fkh := newForeignKeyHelper()
	err = adjustForeignKeyChildTableInfoAfterRenameTable(jobCtx.infoCache, metaMut,
		job, &fkh, tblInfo, oldSchemaName, oldTableName, tableName, newSchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	ver, err = updateSchemaVersion(jobCtx, job, fkh.getLoadedTables()...)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.SchemaState = model.StatePublic
	return ver, nil
}

func onRenameTables(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	args, err := model.GetRenameTablesArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	jobCtx.jobArgs = args

	if job.SchemaState == model.StatePublic {
		return finishJobRenameTables(jobCtx, job)
	}

	fkh := newForeignKeyHelper()
	metaMut := jobCtx.metaMut
	for _, info := range args.RenameTableInfos {
		job.TableID = info.TableID
		job.TableName = info.OldTableName.L
		tblInfo, err := GetTableInfoAndCancelFaultJob(metaMut, job, info.OldSchemaID)
		if err != nil {
			return ver, errors.Trace(err)
		}
		ver, err := checkAndRenameTables(metaMut, job, tblInfo, info)
		if err != nil {
			return ver, errors.Trace(err)
		}
		err = adjustForeignKeyChildTableInfoAfterRenameTable(
			jobCtx.infoCache, metaMut, job, &fkh, tblInfo,
			info.OldSchemaName, info.OldTableName, info.NewTableName, info.NewSchemaID)
		if err != nil {
			return ver, errors.Trace(err)
		}
	}

	ver, err = updateSchemaVersion(jobCtx, job, fkh.getLoadedTables()...)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.SchemaState = model.StatePublic
	return ver, nil
}

func checkAndRenameTables(t *meta.Mutator, job *model.Job, tblInfo *model.TableInfo, args *model.RenameTableArgs) (ver int64, _ error) {
	err := t.DropTableOrView(args.OldSchemaID, tblInfo.ID)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	failpoint.Inject("renameTableErr", func(val failpoint.Value) {
		if valStr, ok := val.(string); ok {
			if args.NewTableName.L == valStr {
				job.State = model.JobStateCancelled
				failpoint.Return(ver, errors.New("occur an error after renaming table"))
			}
		}
	})

	oldTableName := tblInfo.Name
	tableRuleID, partRuleIDs, oldRuleIDs, oldRules, err := getOldLabelRules(tblInfo, args.OldSchemaName.L, oldTableName.L)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Wrapf(err, "failed to get old label rules from PD")
	}

	if tblInfo.AutoIDSchemaID == 0 && args.NewSchemaID != args.OldSchemaID {
		// The auto id is referenced by a schema id + table id
		// Table ID is not changed between renames, but schema id can change.
		// To allow concurrent use of the auto id during rename, keep the auto id
		// by always reference it with the schema id it was originally created in.
		tblInfo.AutoIDSchemaID = args.OldSchemaID
	}
	if args.NewSchemaID == tblInfo.AutoIDSchemaID {
		// Back to the original schema id, no longer needed.
		tblInfo.AutoIDSchemaID = 0
	}

	tblInfo.Name = args.NewTableName
	err = t.CreateTableOrView(args.NewSchemaID, tblInfo)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	err = updateLabelRules(job, tblInfo, oldRules, tableRuleID, partRuleIDs, oldRuleIDs, tblInfo.ID)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Wrapf(err, "failed to update the label rule to PD")
	}

	return ver, nil
}

func adjustForeignKeyChildTableInfoAfterRenameTable(
	infoCache *infoschema.InfoCache, t *meta.Mutator, job *model.Job,
	fkh *foreignKeyHelper, tblInfo *model.TableInfo,
	oldSchemaName, oldTableName, newTableName ast.CIStr, newSchemaID int64) error {
	if !vardef.EnableForeignKey.Load() || newTableName.L == oldTableName.L {
		return nil
	}
	is := infoCache.GetLatest()
	newDB, ok := is.SchemaByID(newSchemaID)
	if !ok {
		job.State = model.JobStateCancelled
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(fmt.Sprintf("schema-ID: %v", newSchemaID))
	}
	referredFKs := is.GetTableReferredForeignKeys(oldSchemaName.L, oldTableName.L)
	if len(referredFKs) == 0 {
		return nil
	}
	fkh.addLoadedTable(oldSchemaName.L, oldTableName.L, newDB.ID, tblInfo)
	for _, referredFK := range referredFKs {
		childTableInfo, err := fkh.getTableFromStorage(is, t, referredFK.ChildSchema, referredFK.ChildTable)
		if err != nil {
			if infoschema.ErrTableNotExists.Equal(err) || infoschema.ErrDatabaseNotExists.Equal(err) {
				continue
			}
			return err
		}
		childFKInfo := model.FindFKInfoByName(childTableInfo.tblInfo.ForeignKeys, referredFK.ChildFKName.L)
		if childFKInfo == nil {
			continue
		}
		childFKInfo.RefSchema = newDB.Name
		childFKInfo.RefTable = newTableName
	}
	for _, info := range fkh.loaded {
		err := updateTable(t, info.schemaID, info.tblInfo, false)
		if err != nil {
			return err
		}
	}
	return nil
}

// We split the renaming table job into two steps:
// 1. rename table and update the schema version.
// 2. update the job state to JobStateDone.
// This is the requirement from TiCDC because
//   - it uses the job state to check whether the DDL is finished.
//   - there is a gap between schema reloading and job state updating:
//     when the job state is updated to JobStateDone, before the new schema reloaded,
//     there may be DMLs that use the old schema.
//   - TiCDC cannot handle the DMLs that use the old schema, because
//     the commit TS of the DMLs are greater than the job state updating TS.
func finishJobRenameTable(jobCtx *jobContext, job *model.Job) (int64, error) {
	tblInfo, err := getTableInfo(jobCtx.metaMut, job.TableID, job.SchemaID)
	if err != nil {
		job.State = model.JobStateCancelled
		return 0, errors.Trace(err)
	}
	// Before updating the schema version, we need to reset the old schema ID to new schema ID, so that
	// the table info can be dropped normally in `ApplyDiff`. This is because renaming table requires two
	// schema versions to complete.
	args := jobCtx.jobArgs.(*model.RenameTableArgs)
	args.OldSchemaIDForSchemaDiff = job.SchemaID

	ver, err := updateSchemaVersion(jobCtx, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func finishJobRenameTables(jobCtx *jobContext, job *model.Job) (int64, error) {
	args := jobCtx.jobArgs.(*model.RenameTablesArgs)
	infos := args.RenameTableInfos
	tblSchemaIDs := make(map[int64]int64, len(infos))
	for _, info := range infos {
		tblSchemaIDs[info.TableID] = info.NewSchemaID
	}
	tblInfos := make([]*model.TableInfo, 0, len(infos))
	for _, info := range infos {
		tblID := info.TableID
		tblInfo, err := getTableInfo(jobCtx.metaMut, tblID, tblSchemaIDs[tblID])
		if err != nil {
			job.State = model.JobStateCancelled
			return 0, errors.Trace(err)
		}
		tblInfos = append(tblInfos, tblInfo)
	}
	// Before updating the schema version, we need to reset the old schema ID to new schema ID, so that
	// the table info can be dropped normally in `ApplyDiff`. This is because renaming table requires two
	// schema versions to complete.
	for _, info := range infos {
		info.OldSchemaIDForSchemaDiff = info.NewSchemaID
	}
	ver, err := updateSchemaVersion(jobCtx, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishMultipleTableJob(model.JobStateDone, model.StatePublic, ver, tblInfos)
	return ver, nil
}
