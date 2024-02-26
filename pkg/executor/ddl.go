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
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/sessiontxn/staleread"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/temptable"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/gcutil"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// DDLExec represents a DDL executor.
// It grabs a DDL instance from Domain, calling the DDL methods to do the work.
type DDLExec struct {
	exec.BaseExecutor

	stmt         ast.StmtNode
	is           infoschema.InfoSchema
	tempTableDDL temptable.TemporaryTableDDL
	done         bool
}

// toErr converts the error to the ErrInfoSchemaChanged when the schema is outdated.
func (e *DDLExec) toErr(err error) error {
	// The err may be cause by schema changed, here we distinguish the ErrInfoSchemaChanged error from other errors.
	dom := domain.GetDomain(e.Ctx())
	checker := domain.NewSchemaChecker(dom, e.is.SchemaMetaVersion(), nil, true)
	txn, err1 := e.Ctx().Txn(true)
	if err1 != nil {
		logutil.BgLogger().Error("active txn failed", zap.Error(err1))
		return err
	}
	_, schemaInfoErr := checker.Check(txn.StartTS())
	if schemaInfoErr != nil {
		return errors.Trace(schemaInfoErr)
	}
	return err
}

func (e *DDLExec) getLocalTemporaryTable(schema model.CIStr, table model.CIStr) (table.Table, bool) {
	tbl, err := e.Ctx().GetInfoSchema().(infoschema.InfoSchema).TableByName(schema, table)
	if infoschema.ErrTableNotExists.Equal(err) {
		return nil, false
	}

	if tbl.Meta().TempTableType != model.TempTableLocal {
		return nil, false
	}

	return tbl, true
}

// Next implements the Executor Next interface.
func (e *DDLExec) Next(ctx context.Context, _ *chunk.Chunk) (err error) {
	if e.done {
		return nil
	}
	e.done = true

	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnDDL)
	// For each DDL, we should commit the previous transaction and create a new transaction.
	// Following cases are exceptions
	var localTempTablesToDrop []*ast.TableName
	switch s := e.stmt.(type) {
	case *ast.CreateTableStmt:
		if s.TemporaryKeyword == ast.TemporaryLocal {
			return e.createSessionTemporaryTable(s)
		}
	case *ast.DropTableStmt:
		if s.IsView {
			break
		}

		for tbIdx := len(s.Tables) - 1; tbIdx >= 0; tbIdx-- {
			if _, ok := e.getLocalTemporaryTable(s.Tables[tbIdx].Schema, s.Tables[tbIdx].Name); ok {
				localTempTablesToDrop = append(localTempTablesToDrop, s.Tables[tbIdx])
				s.Tables = append(s.Tables[:tbIdx], s.Tables[tbIdx+1:]...)
			}
		}

		// Statement `DROP TEMPORARY TABLE ...` should not have non-local temporary tables
		if s.TemporaryKeyword == ast.TemporaryLocal && len(s.Tables) > 0 {
			nonExistsTables := make([]string, 0, len(s.Tables))
			for _, tn := range s.Tables {
				nonExistsTables = append(nonExistsTables, ast.Ident{Schema: tn.Schema, Name: tn.Name}.String())
			}
			// stackless err once used like note.
			err = infoschema.ErrTableDropExists.FastGenByArgs(strings.Join(nonExistsTables, ","))
			if s.IfExists {
				e.Ctx().GetSessionVars().StmtCtx.AppendNote(err)
				return nil
			}
			// complete and trace stack info.
			return errors.Trace(err)
		}

		// if all tables are local temporary, directly drop those tables.
		if len(s.Tables) == 0 {
			return e.dropLocalTemporaryTables(localTempTablesToDrop)
		}
	}

	if err = sessiontxn.NewTxnInStmt(ctx, e.Ctx()); err != nil {
		return err
	}

	defer func() {
		e.Ctx().GetSessionVars().StmtCtx.IsDDLJobInQueue = false
		e.Ctx().GetSessionVars().StmtCtx.DDLJobID = 0
	}()

	switch x := e.stmt.(type) {
	case *ast.AlterDatabaseStmt:
		err = e.executeAlterDatabase(x)
	case *ast.AlterTableStmt:
		err = e.executeAlterTable(ctx, x)
	case *ast.CreateIndexStmt:
		err = e.executeCreateIndex(x)
	case *ast.CreateDatabaseStmt:
		err = e.executeCreateDatabase(x)
	case *ast.FlashBackDatabaseStmt:
		err = e.executeFlashbackDatabase(x)
	case *ast.CreateTableStmt:
		err = e.executeCreateTable(x)
	case *ast.CreateViewStmt:
		err = e.executeCreateView(ctx, x)
	case *ast.DropIndexStmt:
		err = e.executeDropIndex(x)
	case *ast.DropDatabaseStmt:
		err = e.executeDropDatabase(x)
	case *ast.DropTableStmt:
		if x.IsView {
			err = e.executeDropView(x)
		} else {
			err = e.executeDropTable(x)
			if err == nil {
				err = e.dropLocalTemporaryTables(localTempTablesToDrop)
			}
		}
	case *ast.RecoverTableStmt:
		err = e.executeRecoverTable(x)
	case *ast.FlashBackTableStmt:
		err = e.executeFlashbackTable(x)
	case *ast.FlashBackToTimestampStmt:
		if len(x.Tables) != 0 {
			err = dbterror.ErrGeneralUnsupportedDDL.GenWithStack("Unsupported FLASHBACK table TO TIMESTAMP")
		} else if x.DBName.O != "" {
			err = dbterror.ErrGeneralUnsupportedDDL.GenWithStack("Unsupported FLASHBACK database TO TIMESTAMP")
		} else {
			err = e.executeFlashBackCluster(x)
		}
	case *ast.RenameTableStmt:
		err = e.executeRenameTable(x)
	case *ast.TruncateTableStmt:
		err = e.executeTruncateTable(x)
	case *ast.LockTablesStmt:
		err = e.executeLockTables(x)
	case *ast.UnlockTablesStmt:
		err = e.executeUnlockTables(x)
	case *ast.CleanupTableLockStmt:
		err = e.executeCleanupTableLock(x)
	case *ast.RepairTableStmt:
		err = e.executeRepairTable(x)
	case *ast.CreateSequenceStmt:
		err = e.executeCreateSequence(x)
	case *ast.DropSequenceStmt:
		err = e.executeDropSequence(x)
	case *ast.AlterSequenceStmt:
		err = e.executeAlterSequence(x)
	case *ast.CreatePlacementPolicyStmt:
		err = e.executeCreatePlacementPolicy(x)
	case *ast.DropPlacementPolicyStmt:
		err = e.executeDropPlacementPolicy(x)
	case *ast.AlterPlacementPolicyStmt:
		err = e.executeAlterPlacementPolicy(x)
	case *ast.CreateResourceGroupStmt:
		err = e.executeCreateResourceGroup(x)
	case *ast.DropResourceGroupStmt:
		err = e.executeDropResourceGroup(x)
	case *ast.AlterResourceGroupStmt:
		err = e.executeAlterResourceGroup(x)
	}
	if err != nil {
		// If the owner return ErrTableNotExists error when running this DDL, it may be caused by schema changed,
		// otherwise, ErrTableNotExists can be returned before putting this DDL job to the job queue.
		if (e.Ctx().GetSessionVars().StmtCtx.IsDDLJobInQueue && infoschema.ErrTableNotExists.Equal(err)) ||
			!e.Ctx().GetSessionVars().StmtCtx.IsDDLJobInQueue {
			return e.toErr(err)
		}
		return err
	}

	dom := domain.GetDomain(e.Ctx())
	// Update InfoSchema in TxnCtx, so it will pass schema check.
	is := dom.InfoSchema()
	txnCtx := e.Ctx().GetSessionVars().TxnCtx
	txnCtx.InfoSchema = is
	// DDL will force commit old transaction, after DDL, in transaction status should be false.
	e.Ctx().GetSessionVars().SetInTxn(false)
	return nil
}

func (e *DDLExec) executeTruncateTable(s *ast.TruncateTableStmt) error {
	ident := ast.Ident{Schema: s.Table.Schema, Name: s.Table.Name}
	if _, exist := e.getLocalTemporaryTable(s.Table.Schema, s.Table.Name); exist {
		return e.tempTableDDL.TruncateLocalTemporaryTable(s.Table.Schema, s.Table.Name)
	}
	err := domain.GetDomain(e.Ctx()).DDL().TruncateTable(e.Ctx(), ident)
	return err
}

func (e *DDLExec) executeRenameTable(s *ast.RenameTableStmt) error {
	for _, tables := range s.TableToTables {
		if _, ok := e.getLocalTemporaryTable(tables.OldTable.Schema, tables.OldTable.Name); ok {
			return dbterror.ErrUnsupportedLocalTempTableDDL.GenWithStackByArgs("RENAME TABLE")
		}
	}
	return domain.GetDomain(e.Ctx()).DDL().RenameTable(e.Ctx(), s)
}

func (e *DDLExec) executeCreateDatabase(s *ast.CreateDatabaseStmt) error {
	err := domain.GetDomain(e.Ctx()).DDL().CreateSchema(e.Ctx(), s)
	return err
}

func (e *DDLExec) executeAlterDatabase(s *ast.AlterDatabaseStmt) error {
	err := domain.GetDomain(e.Ctx()).DDL().AlterSchema(e.Ctx(), s)
	return err
}

func (e *DDLExec) executeCreateTable(s *ast.CreateTableStmt) error {
	err := domain.GetDomain(e.Ctx()).DDL().CreateTable(e.Ctx(), s)
	return err
}

func (e *DDLExec) createSessionTemporaryTable(s *ast.CreateTableStmt) error {
	is := e.Ctx().GetInfoSchema().(infoschema.InfoSchema)
	dbInfo, ok := is.SchemaByName(s.Table.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(s.Table.Schema.O)
	}

	_, exists := e.getLocalTemporaryTable(s.Table.Schema, s.Table.Name)
	if exists {
		err := infoschema.ErrTableExists.FastGenByArgs(ast.Ident{Schema: s.Table.Schema, Name: s.Table.Name})
		if s.IfNotExists {
			e.Ctx().GetSessionVars().StmtCtx.AppendNote(err)
			return nil
		}
		return errors.Trace(err)
	}

	tbInfo, err := ddl.BuildSessionTemporaryTableInfo(e.Ctx(), is, s, dbInfo.Charset, dbInfo.Collate, dbInfo.PlacementPolicyRef)
	if err != nil {
		return err
	}

	if err = e.tempTableDDL.CreateLocalTemporaryTable(dbInfo, tbInfo); err != nil {
		return err
	}

	sessiontxn.GetTxnManager(e.Ctx()).OnLocalTemporaryTableCreated()
	return nil
}

func (e *DDLExec) executeCreateView(ctx context.Context, s *ast.CreateViewStmt) error {
	ret := &core.PreprocessorReturn{}
	err := core.Preprocess(ctx, e.Ctx(), s.Select, core.WithPreprocessorReturn(ret))
	if err != nil {
		return errors.Trace(err)
	}
	if ret.IsStaleness {
		return exeerrors.ErrViewInvalid.GenWithStackByArgs(s.ViewName.Schema.L, s.ViewName.Name.L)
	}

	return domain.GetDomain(e.Ctx()).DDL().CreateView(e.Ctx(), s)
}

func (e *DDLExec) executeCreateIndex(s *ast.CreateIndexStmt) error {
	if _, ok := e.getLocalTemporaryTable(s.Table.Schema, s.Table.Name); ok {
		return dbterror.ErrUnsupportedLocalTempTableDDL.GenWithStackByArgs("CREATE INDEX")
	}

	return domain.GetDomain(e.Ctx()).DDL().CreateIndex(e.Ctx(), s)
}

func (e *DDLExec) executeDropDatabase(s *ast.DropDatabaseStmt) error {
	dbName := s.Name

	// Protect important system table from been dropped by a mistake.
	// I can hardly find a case that a user really need to do this.
	if dbName.L == "mysql" {
		return errors.New("Drop 'mysql' database is forbidden")
	}

	err := domain.GetDomain(e.Ctx()).DDL().DropSchema(e.Ctx(), s)
	sessionVars := e.Ctx().GetSessionVars()
	if err == nil && strings.ToLower(sessionVars.CurrentDB) == dbName.L {
		sessionVars.CurrentDB = ""
		err = sessionVars.SetSystemVar(variable.CharsetDatabase, mysql.DefaultCharset)
		if err != nil {
			return err
		}
		err = sessionVars.SetSystemVar(variable.CollationDatabase, mysql.DefaultCollationName)
		if err != nil {
			return err
		}
	}
	return err
}

func (e *DDLExec) executeDropTable(s *ast.DropTableStmt) error {
	return domain.GetDomain(e.Ctx()).DDL().DropTable(e.Ctx(), s)
}

func (e *DDLExec) executeDropView(s *ast.DropTableStmt) error {
	return domain.GetDomain(e.Ctx()).DDL().DropView(e.Ctx(), s)
}

func (e *DDLExec) executeDropSequence(s *ast.DropSequenceStmt) error {
	return domain.GetDomain(e.Ctx()).DDL().DropSequence(e.Ctx(), s)
}

func (e *DDLExec) dropLocalTemporaryTables(localTempTables []*ast.TableName) error {
	if len(localTempTables) == 0 {
		return nil
	}

	for _, tb := range localTempTables {
		err := e.tempTableDDL.DropLocalTemporaryTable(tb.Schema, tb.Name)
		if err != nil {
			return err
		}
	}

	return nil
}

func (e *DDLExec) executeDropIndex(s *ast.DropIndexStmt) error {
	if _, ok := e.getLocalTemporaryTable(s.Table.Schema, s.Table.Name); ok {
		return dbterror.ErrUnsupportedLocalTempTableDDL.GenWithStackByArgs("DROP INDEX")
	}

	return domain.GetDomain(e.Ctx()).DDL().DropIndex(e.Ctx(), s)
}

func (e *DDLExec) executeAlterTable(ctx context.Context, s *ast.AlterTableStmt) error {
	if _, ok := e.getLocalTemporaryTable(s.Table.Schema, s.Table.Name); ok {
		return dbterror.ErrUnsupportedLocalTempTableDDL.GenWithStackByArgs("ALTER TABLE")
	}

	return domain.GetDomain(e.Ctx()).DDL().AlterTable(ctx, e.Ctx(), s)
}

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
	tbl, ok := dom.InfoSchema().TableByID(tblInfo.ID)
	if ok {
		return infoschema.ErrTableExists.GenWithStack("Table '%-.192s' already been recover to '%-.192s', can't be recover repeatedly", s.Table.Name.O, tbl.Meta().Name.O)
	}

	m, err := domain.GetDomain(e.Ctx()).GetSnapshotMeta(job.StartTS)
	if err != nil {
		return err
	}
	autoIDs, err := m.GetAutoIDAccessors(job.SchemaID, job.TableID).Get()
	if err != nil {
		return err
	}

	recoverInfo := &ddl.RecoverInfo{
		SchemaID:      job.SchemaID,
		TableInfo:     tblInfo,
		DropJobID:     job.ID,
		SnapshotTS:    job.StartTS,
		AutoIDs:       autoIDs,
		OldSchemaName: job.SchemaName,
		OldTableName:  tblInfo.Name.L,
	}
	// Call DDL RecoverTable.
	err = domain.GetDomain(e.Ctx()).DDL().RecoverTable(e.Ctx(), recoverInfo)
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
	table, ok := snapInfo.TableByID(job.TableID)
	if !ok {
		return nil, nil, infoschema.ErrTableNotExists.GenWithStackByArgs(
			fmt.Sprintf("(Schema ID %d)", job.SchemaID),
			fmt.Sprintf("(Table ID %d)", job.TableID),
		)
	}
	return job, table.Meta(), nil
}

// GetDropOrTruncateTableInfoFromJobs gets the dropped/truncated table information from DDL jobs,
// it will use the `start_ts` of DDL job as snapshot to get the dropped/truncated table information.
func GetDropOrTruncateTableInfoFromJobs(jobs []*model.Job, gcSafePoint uint64, dom *domain.Domain, fn func(*model.Job, *model.TableInfo) (bool, error)) (bool, error) {
	getTable := func(startTS uint64, schemaID int64, tableID int64) (*model.TableInfo, error) {
		snapMeta, err := dom.GetSnapshotMeta(startTS)
		if err != nil {
			return nil, err
		}
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
	return jobInfo, tableInfo, nil
}

func (e *DDLExec) executeFlashBackCluster(s *ast.FlashBackToTimestampStmt) error {
	// Check `TO TSO` clause
	if s.FlashbackTSO > 0 {
		return domain.GetDomain(e.Ctx()).DDL().FlashbackCluster(e.Ctx(), s.FlashbackTSO)
	}

	// Check `TO TIMESTAMP` clause
	flashbackTS, err := staleread.CalculateAsOfTsExpr(context.Background(), e.Ctx().GetPlanCtx(), s.FlashbackTS)
	if err != nil {
		return err
	}

	return domain.GetDomain(e.Ctx()).DDL().FlashbackCluster(e.Ctx(), flashbackTS)
}

func (e *DDLExec) executeFlashbackTable(s *ast.FlashBackTableStmt) error {
	job, tblInfo, err := e.getRecoverTableByTableName(s.Table)
	if err != nil {
		return err
	}
	if len(s.NewName) != 0 {
		tblInfo.Name = model.NewCIStr(s.NewName)
	}
	// Check the table ID was not exists.
	is := domain.GetDomain(e.Ctx()).InfoSchema()
	tbl, ok := is.TableByID(tblInfo.ID)
	if ok {
		return infoschema.ErrTableExists.GenWithStack("Table '%-.192s' already been flashback to '%-.192s', can't be flashback repeatedly", s.Table.Name.O, tbl.Meta().Name.O)
	}

	m, err := domain.GetDomain(e.Ctx()).GetSnapshotMeta(job.StartTS)
	if err != nil {
		return err
	}
	autoIDs, err := m.GetAutoIDAccessors(job.SchemaID, job.TableID).Get()
	if err != nil {
		return err
	}

	recoverInfo := &ddl.RecoverInfo{
		SchemaID:      job.SchemaID,
		TableInfo:     tblInfo,
		DropJobID:     job.ID,
		SnapshotTS:    job.StartTS,
		AutoIDs:       autoIDs,
		OldSchemaName: job.SchemaName,
		OldTableName:  s.Table.Name.L,
	}
	// Call DDL RecoverTable.
	err = domain.GetDomain(e.Ctx()).DDL().RecoverTable(e.Ctx(), recoverInfo)
	return err
}

// executeFlashbackDatabase represents a restore schema executor.
// It is built from "flashback schema" statement,
// is used to recover the schema that deleted by mistake.
func (e *DDLExec) executeFlashbackDatabase(s *ast.FlashBackDatabaseStmt) error {
	dbName := s.DBName
	if len(s.NewName) > 0 {
		dbName = model.NewCIStr(s.NewName)
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
	err = domain.GetDomain(e.Ctx()).DDL().RecoverSchema(e.Ctx(), recoverSchemaInfo)
	return err
}

func (e *DDLExec) getRecoverDBByName(schemaName model.CIStr) (recoverSchemaInfo *ddl.RecoverSchemaInfo, err error) {
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
			snapMeta, err := dom.GetSnapshotMeta(job.StartTS)
			if err != nil {
				return false, err
			}
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
			tables, err := snapMeta.ListTables(job.SchemaID)
			if err != nil {
				return false, err
			}
			recoverTabsInfo := make([]*ddl.RecoverInfo, 0)
			for _, tblInfo := range tables {
				autoIDs, err := snapMeta.GetAutoIDAccessors(job.SchemaID, tblInfo.ID).Get()
				if err != nil {
					return false, err
				}
				recoverTabsInfo = append(recoverTabsInfo, &ddl.RecoverInfo{
					SchemaID:      job.SchemaID,
					TableInfo:     tblInfo,
					DropJobID:     job.ID,
					SnapshotTS:    job.StartTS,
					AutoIDs:       autoIDs,
					OldSchemaName: schemaName.L,
					OldTableName:  tblInfo.Name.L,
				})
			}
			recoverSchemaInfo = &ddl.RecoverSchemaInfo{DBInfo: schemaInfo, RecoverTabsInfo: recoverTabsInfo, DropJobID: job.ID, SnapshotTS: job.StartTS, OldSchemaName: schemaName}
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
		if _, ok := e.getLocalTemporaryTable(tb.Table.Schema, tb.Table.Name); ok {
			return dbterror.ErrUnsupportedLocalTempTableDDL.GenWithStackByArgs("LOCK TABLES")
		}
	}

	return domain.GetDomain(e.Ctx()).DDL().LockTables(e.Ctx(), s)
}

func (e *DDLExec) executeUnlockTables(_ *ast.UnlockTablesStmt) error {
	if !config.TableLockEnabled() {
		e.Ctx().GetSessionVars().StmtCtx.AppendWarning(exeerrors.ErrFuncNotEnabled.FastGenByArgs("UNLOCK TABLES", "enable-table-lock"))
		return nil
	}
	lockedTables := e.Ctx().GetAllTableLocks()
	return domain.GetDomain(e.Ctx()).DDL().UnlockTables(e.Ctx(), lockedTables)
}

func (e *DDLExec) executeCleanupTableLock(s *ast.CleanupTableLockStmt) error {
	for _, tb := range s.Tables {
		if _, ok := e.getLocalTemporaryTable(tb.Schema, tb.Name); ok {
			return dbterror.ErrUnsupportedLocalTempTableDDL.GenWithStackByArgs("ADMIN CLEANUP TABLE LOCK")
		}
	}
	return domain.GetDomain(e.Ctx()).DDL().CleanupTableLock(e.Ctx(), s.Tables)
}

func (e *DDLExec) executeRepairTable(s *ast.RepairTableStmt) error {
	return domain.GetDomain(e.Ctx()).DDL().RepairTable(e.Ctx(), s.CreateStmt)
}

func (e *DDLExec) executeCreateSequence(s *ast.CreateSequenceStmt) error {
	return domain.GetDomain(e.Ctx()).DDL().CreateSequence(e.Ctx(), s)
}

func (e *DDLExec) executeAlterSequence(s *ast.AlterSequenceStmt) error {
	return domain.GetDomain(e.Ctx()).DDL().AlterSequence(e.Ctx(), s)
}

func (e *DDLExec) executeCreatePlacementPolicy(s *ast.CreatePlacementPolicyStmt) error {
	return domain.GetDomain(e.Ctx()).DDL().CreatePlacementPolicy(e.Ctx(), s)
}

func (e *DDLExec) executeDropPlacementPolicy(s *ast.DropPlacementPolicyStmt) error {
	return domain.GetDomain(e.Ctx()).DDL().DropPlacementPolicy(e.Ctx(), s)
}

func (e *DDLExec) executeAlterPlacementPolicy(s *ast.AlterPlacementPolicyStmt) error {
	return domain.GetDomain(e.Ctx()).DDL().AlterPlacementPolicy(e.Ctx(), s)
}

func (e *DDLExec) executeCreateResourceGroup(s *ast.CreateResourceGroupStmt) error {
	if !variable.EnableResourceControl.Load() && !e.Ctx().GetSessionVars().InRestrictedSQL {
		return infoschema.ErrResourceGroupSupportDisabled
	}
	return domain.GetDomain(e.Ctx()).DDL().AddResourceGroup(e.Ctx(), s)
}

func (e *DDLExec) executeAlterResourceGroup(s *ast.AlterResourceGroupStmt) error {
	if !variable.EnableResourceControl.Load() && !e.Ctx().GetSessionVars().InRestrictedSQL {
		return infoschema.ErrResourceGroupSupportDisabled
	}
	return domain.GetDomain(e.Ctx()).DDL().AlterResourceGroup(e.Ctx(), s)
}

func (e *DDLExec) executeDropResourceGroup(s *ast.DropResourceGroupStmt) error {
	if !variable.EnableResourceControl.Load() && !e.Ctx().GetSessionVars().InRestrictedSQL {
		return infoschema.ErrResourceGroupSupportDisabled
	}
	return domain.GetDomain(e.Ctx()).DDL().DropResourceGroup(e.Ctx(), s)
}
