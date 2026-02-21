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

package executor

import (
	"context"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn/staleread"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/gcutil"
)

// executeRecoverTable represents a recover table executor.
// It is built from "recover table" statement,
// is used to recover the table that deleted by mistake.
func (e *DDLExec) executeRecoverTable(s *ast.RecoverTableStmt) error {
	dom := domain.GetDomain(e.Ctx())
	var job *model.Job
	var err error
	var tblInfo *model.TableInfo
	// Let check table first. Related isssue #46296.
	if s.Table != nil {
		job, tblInfo, err = e.getRecoverTableByTableName(s.Table)
	} else {
		job, tblInfo, err = e.getRecoverTableByJobID(s, dom)
	}
	if err != nil {
		return err
	}
	// Check the table ID was not exists.
	tbl, ok := dom.InfoSchema().TableByID(context.Background(), tblInfo.ID)
	if ok {
		return infoschema.ErrTableExists.GenWithStack("Table '%-.192s' already been recover to '%-.192s', can't be recover repeatedly", tblInfo.Name.O, tbl.Meta().Name.O)
	}

	m := domain.GetDomain(e.Ctx()).GetSnapshotMeta(job.StartTS)
	autoIDs, err := m.GetAutoIDAccessors(job.SchemaID, job.TableID).Get()
	if err != nil {
		return err
	}

	recoverInfo := &model.RecoverTableInfo{
		SchemaID:      job.SchemaID,
		TableInfo:     tblInfo,
		DropJobID:     job.ID,
		SnapshotTS:    job.StartTS,
		AutoIDs:       autoIDs,
		OldSchemaName: job.SchemaName,
		OldTableName:  tblInfo.Name.L,
	}
	// Call DDL RecoverTable.
	err = e.ddlExecutor.RecoverTable(e.Ctx(), recoverInfo)
	return err
}

func (e *DDLExec) getRecoverTableByJobID(s *ast.RecoverTableStmt, dom *domain.Domain) (*model.Job, *model.TableInfo, error) {
	se, err := e.GetSysSession()
	if err != nil {
		return nil, nil, err
	}
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	defer e.ReleaseSysSession(ctx, se)
	job, err := ddl.GetHistoryJobByID(se, s.JobID)
	if err != nil {
		return nil, nil, err
	}
	if job == nil {
		return nil, nil, dbterror.ErrDDLJobNotFound.GenWithStackByArgs(s.JobID)
	}
	if job.Type != model.ActionDropTable && job.Type != model.ActionTruncateTable {
		return nil, nil, errors.Errorf("Job %v type is %v, not dropped/truncated table", job.ID, job.Type)
	}

	// Check GC safe point for getting snapshot infoSchema.
	err = gcutil.ValidateSnapshot(e.Ctx(), job.StartTS)
	if err != nil {
		return nil, nil, err
	}

	// Get the snapshot infoSchema before drop table.
	snapInfo, err := dom.GetSnapshotInfoSchema(job.StartTS)
	if err != nil {
		return nil, nil, err
	}
	// Get table meta from snapshot infoSchema.
	table, ok := snapInfo.TableByID(ctx, job.TableID)
	if !ok {
		return nil, nil, infoschema.ErrTableNotExists.GenWithStackByArgs(
			fmt.Sprintf("(Schema ID %d)", job.SchemaID),
			fmt.Sprintf("(Table ID %d)", job.TableID),
		)
	}
	// We can't return the meta directly since it will be modified outside, which may corrupt the infocache.
	// Since only State field is changed, return a shallow copy is enough.
	// see https://github.com/pingcap/tidb/issues/55462
	tblInfo := *table.Meta()
	return job, &tblInfo, nil
}

// GetDropOrTruncateTableInfoFromJobs gets the dropped/truncated table information from DDL jobs,
// it will use the `start_ts` of DDL job as snapshot to get the dropped/truncated table information.
func GetDropOrTruncateTableInfoFromJobs(jobs []*model.Job, gcSafePoint uint64, dom *domain.Domain, fn func(*model.Job, *model.TableInfo) (bool, error)) (bool, error) {
	getTable := func(startTS uint64, schemaID int64, tableID int64) (*model.TableInfo, error) {
		snapMeta := dom.GetSnapshotMeta(startTS)
		tbl, err := snapMeta.GetTable(schemaID, tableID)
		return tbl, err
	}
	return ddl.GetDropOrTruncateTableInfoFromJobsByStore(jobs, gcSafePoint, getTable, fn)
}

func (e *DDLExec) getRecoverTableByTableName(tableName *ast.TableName) (*model.Job, *model.TableInfo, error) {
	txn, err := e.Ctx().Txn(true)
	if err != nil {
		return nil, nil, err
	}
	schemaName := tableName.Schema.L
	if schemaName == "" {
		schemaName = strings.ToLower(e.Ctx().GetSessionVars().CurrentDB)
	}
	if schemaName == "" {
		return nil, nil, errors.Trace(plannererrors.ErrNoDB)
	}
	gcSafePoint, err := gcutil.GetGCSafePoint(e.Ctx())
	if err != nil {
		return nil, nil, err
	}
	var jobInfo *model.Job
	var tableInfo *model.TableInfo
	dom := domain.GetDomain(e.Ctx())
	handleJobAndTableInfo := func(job *model.Job, tblInfo *model.TableInfo) (bool, error) {
		if tblInfo.Name.L != tableName.Name.L {
			return false, nil
		}
		schema, ok := dom.InfoSchema().SchemaByID(job.SchemaID)
		if !ok {
			return false, nil
		}
		if schema.Name.L == schemaName {
			tableInfo = tblInfo
			jobInfo = job
			return true, nil
		}
		return false, nil
	}
	fn := func(jobs []*model.Job) (bool, error) {
		return GetDropOrTruncateTableInfoFromJobs(jobs, gcSafePoint, dom, handleJobAndTableInfo)
	}
	err = ddl.IterHistoryDDLJobs(txn, fn)
	if err != nil {
		if terror.ErrorEqual(variable.ErrSnapshotTooOld, err) {
			return nil, nil, errors.Errorf("Can't find dropped/truncated table '%s' in GC safe point %s", tableName.Name.O, model.TSConvert2Time(gcSafePoint).String())
		}
		return nil, nil, err
	}
	if tableInfo == nil || jobInfo == nil {
		return nil, nil, errors.Errorf("Can't find localTemporary/dropped/truncated table: %v in DDL history jobs", tableName.Name)
	}
	// Dropping local temporary tables won't appear in DDL jobs.
	if tableInfo.TempTableType == model.TempTableGlobal {
		return nil, nil, exeerrors.ErrUnsupportedFlashbackTmpTable
	}

	// We can't return the meta directly since it will be modified outside, which may corrupt the infocache.
	// Since only State field is changed, return a shallow copy is enough.
	// see https://github.com/pingcap/tidb/issues/55462
	tblInfo := *tableInfo
	return jobInfo, &tblInfo, nil
}

func (e *DDLExec) executeFlashBackCluster(s *ast.FlashBackToTimestampStmt) error {
	// Check `TO TSO` clause
	if s.FlashbackTSO > 0 {
		return e.ddlExecutor.FlashbackCluster(e.Ctx(), s.FlashbackTSO)
	}

	// Check `TO TIMESTAMP` clause
	flashbackTS, err := staleread.CalculateAsOfTsExpr(context.Background(), e.Ctx().GetPlanCtx(), s.FlashbackTS)
	if err != nil {
		return err
	}

	return e.ddlExecutor.FlashbackCluster(e.Ctx(), flashbackTS)
}

func (e *DDLExec) executeFlashbackTable(s *ast.FlashBackTableStmt) error {
	job, tblInfo, err := e.getRecoverTableByTableName(s.Table)
	if err != nil {
		return err
	}
	if len(s.NewName) != 0 {
		tblInfo.Name = ast.NewCIStr(s.NewName)
	}
	// Check the table ID was not exists.
	is := domain.GetDomain(e.Ctx()).InfoSchema()
	tbl, ok := is.TableByID(context.Background(), tblInfo.ID)
	if ok {
		return infoschema.ErrTableExists.GenWithStack("Table '%-.192s' already been flashback to '%-.192s', can't be flashback repeatedly", s.Table.Name.O, tbl.Meta().Name.O)
	}

	m := domain.GetDomain(e.Ctx()).GetSnapshotMeta(job.StartTS)
	autoIDs, err := m.GetAutoIDAccessors(job.SchemaID, job.TableID).Get()
	if err != nil {
		return err
	}

	recoverInfo := &model.RecoverTableInfo{
		SchemaID:      job.SchemaID,
		TableInfo:     tblInfo,
		DropJobID:     job.ID,
		SnapshotTS:    job.StartTS,
		AutoIDs:       autoIDs,
		OldSchemaName: job.SchemaName,
		OldTableName:  s.Table.Name.L,
	}
	// Call DDL RecoverTable.
	err = e.ddlExecutor.RecoverTable(e.Ctx(), recoverInfo)
	return err
}

// executeFlashbackDatabase represents a restore schema executor.
// It is built from "flashback schema" statement,
// is used to recover the schema that deleted by mistake.
func (e *DDLExec) executeFlashbackDatabase(s *ast.FlashBackDatabaseStmt) error {
	dbName := s.DBName
	if len(s.NewName) > 0 {
		dbName = ast.NewCIStr(s.NewName)
	}
	// Check the Schema Name was not exists.
	is := domain.GetDomain(e.Ctx()).InfoSchema()
	if is.SchemaExists(dbName) {
		return infoschema.ErrDatabaseExists.GenWithStackByArgs(dbName)
	}
	recoverSchemaInfo, err := e.getRecoverDBByName(s.DBName)
	if err != nil {
		return err
	}
	// Check the Schema ID was not exists.
	if schema, ok := is.SchemaByID(recoverSchemaInfo.ID); ok {
		return infoschema.ErrDatabaseExists.GenWithStack("Schema '%-.192s' already been recover to '%-.192s', can't be recover repeatedly", s.DBName, schema.Name.O)
	}
	recoverSchemaInfo.Name = dbName
	// Call DDL RecoverSchema.
	err = e.ddlExecutor.RecoverSchema(e.Ctx(), recoverSchemaInfo)
	return err
}

func (e *DDLExec) getRecoverDBByName(schemaName ast.CIStr) (recoverSchemaInfo *model.RecoverSchemaInfo, err error) {
	txn, err := e.Ctx().Txn(true)
	if err != nil {
		return nil, err
	}
	gcSafePoint, err := gcutil.GetGCSafePoint(e.Ctx())
	if err != nil {
		return nil, err
	}
	dom := domain.GetDomain(e.Ctx())
	fn := func(jobs []*model.Job) (bool, error) {
		for _, job := range jobs {
			// Check GC safe point for getting snapshot infoSchema.
			err = gcutil.ValidateSnapshotWithGCSafePoint(job.StartTS, gcSafePoint)
			if err != nil {
				return false, err
			}
			if job.Type != model.ActionDropSchema {
				continue
			}
			snapMeta := dom.GetSnapshotMeta(job.StartTS)
			schemaInfo, err := snapMeta.GetDatabase(job.SchemaID)
			if err != nil {
				return false, err
			}
			if schemaInfo == nil {
				// The dropped DDL maybe execute failed that caused by the parallel DDL execution,
				// then can't find the schema from the snapshot info-schema. Should just ignore error here,
				// see more in TestParallelDropSchemaAndDropTable.
				continue
			}
			if schemaInfo.Name.L != schemaName.L {
				continue
			}
			recoverSchemaInfo = &model.RecoverSchemaInfo{
				DBInfo:              schemaInfo,
				LoadTablesOnExecute: true,
				DropJobID:           job.ID,
				SnapshotTS:          job.StartTS,
				OldSchemaName:       schemaName,
			}
			return true, nil
		}
		return false, nil
	}
	err = ddl.IterHistoryDDLJobs(txn, fn)
	if err != nil {
		if terror.ErrorEqual(variable.ErrSnapshotTooOld, err) {
			return nil, errors.Errorf("Can't find dropped database '%s' in GC safe point %s", schemaName.O, model.TSConvert2Time(gcSafePoint).String())
		}
		return nil, err
	}
	if recoverSchemaInfo == nil {
		return nil, errors.Errorf("Can't find dropped database: %v in DDL history jobs", schemaName.O)
	}
	return
}

func (e *DDLExec) executeLockTables(s *ast.LockTablesStmt) error {
	if !config.TableLockEnabled() {
		e.Ctx().GetSessionVars().StmtCtx.AppendWarning(exeerrors.ErrFuncNotEnabled.FastGenByArgs("LOCK TABLES", "enable-table-lock"))
		return nil
	}

	for _, tb := range s.TableLocks {
		_, ok, err := e.getLocalTemporaryTable(tb.Table.Schema, tb.Table.Name)
		if err != nil {
			return errors.Trace(err)
		}
		if ok {
			return dbterror.ErrUnsupportedLocalTempTableDDL.GenWithStackByArgs("LOCK TABLES")
		}
	}

	return e.ddlExecutor.LockTables(e.Ctx(), s)
}

func (e *DDLExec) executeUnlockTables(_ *ast.UnlockTablesStmt) error {
	if !config.TableLockEnabled() {
		e.Ctx().GetSessionVars().StmtCtx.AppendWarning(exeerrors.ErrFuncNotEnabled.FastGenByArgs("UNLOCK TABLES", "enable-table-lock"))
		return nil
	}
	lockedTables := e.Ctx().GetAllTableLocks()
	return e.ddlExecutor.UnlockTables(e.Ctx(), lockedTables)
}

func (e *DDLExec) executeCleanupTableLock(s *ast.CleanupTableLockStmt) error {
	for _, tb := range s.Tables {
		_, ok, err := e.getLocalTemporaryTable(tb.Schema, tb.Name)
		if err != nil {
			return errors.Trace(err)
		}
		if ok {
			return dbterror.ErrUnsupportedLocalTempTableDDL.GenWithStackByArgs("ADMIN CLEANUP TABLE LOCK")
		}
	}
	return e.ddlExecutor.CleanupTableLock(e.Ctx(), s.Tables)
}

func (e *DDLExec) executeRepairTable(s *ast.RepairTableStmt) error {
	return e.ddlExecutor.RepairTable(e.Ctx(), s.CreateStmt)
}

func (e *DDLExec) executeCreateSequence(s *ast.CreateSequenceStmt) error {
	return e.ddlExecutor.CreateSequence(e.Ctx(), s)
}

func (e *DDLExec) executeAlterSequence(s *ast.AlterSequenceStmt) error {
	return e.ddlExecutor.AlterSequence(e.Ctx(), s)
}

func (e *DDLExec) executeCreatePlacementPolicy(s *ast.CreatePlacementPolicyStmt) error {
	return e.ddlExecutor.CreatePlacementPolicy(e.Ctx(), s)
}

func (e *DDLExec) executeDropPlacementPolicy(s *ast.DropPlacementPolicyStmt) error {
	return e.ddlExecutor.DropPlacementPolicy(e.Ctx(), s)
}

func (e *DDLExec) executeAlterPlacementPolicy(s *ast.AlterPlacementPolicyStmt) error {
	return e.ddlExecutor.AlterPlacementPolicy(e.Ctx(), s)
}

func (e *DDLExec) executeCreateResourceGroup(s *ast.CreateResourceGroupStmt) error {
	if !vardef.EnableResourceControl.Load() && !e.Ctx().GetSessionVars().InRestrictedSQL {
		return infoschema.ErrResourceGroupSupportDisabled
	}
	return e.ddlExecutor.AddResourceGroup(e.Ctx(), s)
}

func (e *DDLExec) executeAlterResourceGroup(s *ast.AlterResourceGroupStmt) error {
	if !vardef.EnableResourceControl.Load() && !e.Ctx().GetSessionVars().InRestrictedSQL {
		return infoschema.ErrResourceGroupSupportDisabled
	}
	return e.ddlExecutor.AlterResourceGroup(e.Ctx(), s)
}

func (e *DDLExec) executeDropResourceGroup(s *ast.DropResourceGroupStmt) error {
	if !vardef.EnableResourceControl.Load() && !e.Ctx().GetSessionVars().InRestrictedSQL {
		return infoschema.ErrResourceGroupSupportDisabled
	}
	return e.ddlExecutor.DropResourceGroup(e.Ctx(), s)
}
