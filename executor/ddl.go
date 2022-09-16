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
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/sessiontxn/staleread"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/temptable"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/gcutil"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// DDLExec represents a DDL executor.
// It grabs a DDL instance from Domain, calling the DDL methods to do the work.
type DDLExec struct {
	baseExecutor

	stmt         ast.StmtNode
	is           infoschema.InfoSchema
	tempTableDDL temptable.TemporaryTableDDL
	done         bool
}

// toErr converts the error to the ErrInfoSchemaChanged when the schema is outdated.
func (e *DDLExec) toErr(err error) error {
	// The err may be cause by schema changed, here we distinguish the ErrInfoSchemaChanged error from other errors.
	dom := domain.GetDomain(e.ctx)
	checker := domain.NewSchemaChecker(dom, e.is.SchemaMetaVersion(), nil)
	txn, err1 := e.ctx.Txn(true)
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
	tbl, err := e.ctx.GetInfoSchema().(infoschema.InfoSchema).TableByName(schema, table)
	if infoschema.ErrTableNotExists.Equal(err) {
		return nil, false
	}

	if tbl.Meta().TempTableType != model.TempTableLocal {
		return nil, false
	}

	return tbl, true
}

// Next implements the Executor Next interface.
func (e *DDLExec) Next(ctx context.Context, req *chunk.Chunk) (err error) {
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
			err = infoschema.ErrTableDropExists.GenWithStackByArgs(strings.Join(nonExistsTables, ","))
			if s.IfExists {
				e.ctx.GetSessionVars().StmtCtx.AppendNote(err)
				return nil
			}
			return err
		}

		// if all tables are local temporary, directly drop those tables.
		if len(s.Tables) == 0 {
			return e.dropLocalTemporaryTables(localTempTablesToDrop)
		}
	}

	if err = sessiontxn.NewTxnInStmt(ctx, e.ctx); err != nil {
		return err
	}

	defer func() {
		e.ctx.GetSessionVars().StmtCtx.IsDDLJobInQueue = false
		e.ctx.GetSessionVars().StmtCtx.DDLJobID = 0
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
	case *ast.CreateTableStmt:
		err = e.executeCreateTable(x)
	case *ast.CreateViewStmt:
		err = e.executeCreateView(x)
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
	case *ast.FlashBackClusterStmt:
		err = e.executeFlashBackCluster(ctx, x)
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
	}
	if err != nil {
		// If the owner return ErrTableNotExists error when running this DDL, it may be caused by schema changed,
		// otherwise, ErrTableNotExists can be returned before putting this DDL job to the job queue.
		if (e.ctx.GetSessionVars().StmtCtx.IsDDLJobInQueue && infoschema.ErrTableNotExists.Equal(err)) ||
			!e.ctx.GetSessionVars().StmtCtx.IsDDLJobInQueue {
			return e.toErr(err)
		}
		return err
	}

	dom := domain.GetDomain(e.ctx)
	// Update InfoSchema in TxnCtx, so it will pass schema check.
	is := dom.InfoSchema()
	txnCtx := e.ctx.GetSessionVars().TxnCtx
	txnCtx.InfoSchema = is
	// DDL will force commit old transaction, after DDL, in transaction status should be false.
	e.ctx.GetSessionVars().SetInTxn(false)
	return nil
}

func (e *DDLExec) executeTruncateTable(s *ast.TruncateTableStmt) error {
	ident := ast.Ident{Schema: s.Table.Schema, Name: s.Table.Name}
	if _, exist := e.getLocalTemporaryTable(s.Table.Schema, s.Table.Name); exist {
		return e.tempTableDDL.TruncateLocalTemporaryTable(s.Table.Schema, s.Table.Name)
	}
	err := domain.GetDomain(e.ctx).DDL().TruncateTable(e.ctx, ident)
	return err
}

func (e *DDLExec) executeRenameTable(s *ast.RenameTableStmt) error {
	for _, tables := range s.TableToTables {
		if _, ok := e.getLocalTemporaryTable(tables.OldTable.Schema, tables.OldTable.Name); ok {
			return dbterror.ErrUnsupportedLocalTempTableDDL.GenWithStackByArgs("RENAME TABLE")
		}
	}
	return domain.GetDomain(e.ctx).DDL().RenameTable(e.ctx, s)
}

func (e *DDLExec) executeCreateDatabase(s *ast.CreateDatabaseStmt) error {
	err := domain.GetDomain(e.ctx).DDL().CreateSchema(e.ctx, s)
	return err
}

func (e *DDLExec) executeAlterDatabase(s *ast.AlterDatabaseStmt) error {
	err := domain.GetDomain(e.ctx).DDL().AlterSchema(e.ctx, s)
	return err
}

func (e *DDLExec) executeCreateTable(s *ast.CreateTableStmt) error {
	err := domain.GetDomain(e.ctx).DDL().CreateTable(e.ctx, s)
	return err
}

func (e *DDLExec) createSessionTemporaryTable(s *ast.CreateTableStmt) error {
	is := e.ctx.GetInfoSchema().(infoschema.InfoSchema)
	dbInfo, ok := is.SchemaByName(s.Table.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(s.Table.Schema.O)
	}

	_, exists := e.getLocalTemporaryTable(s.Table.Schema, s.Table.Name)
	if exists {
		err := infoschema.ErrTableExists.GenWithStackByArgs(ast.Ident{Schema: s.Table.Schema, Name: s.Table.Name})
		if s.IfNotExists {
			e.ctx.GetSessionVars().StmtCtx.AppendNote(err)
			return nil
		}
		return err
	}

	tbInfo, err := ddl.BuildSessionTemporaryTableInfo(e.ctx, is, s, dbInfo.Charset, dbInfo.Collate, dbInfo.PlacementPolicyRef)
	if err != nil {
		return err
	}

	if err = e.tempTableDDL.CreateLocalTemporaryTable(dbInfo, tbInfo); err != nil {
		return err
	}

	sessiontxn.GetTxnManager(e.ctx).OnLocalTemporaryTableCreated()
	return nil
}

func (e *DDLExec) executeCreateView(s *ast.CreateViewStmt) error {
	ret := &core.PreprocessorReturn{}
	err := core.Preprocess(e.ctx, s.Select, core.WithPreprocessorReturn(ret))
	if err != nil {
		return errors.Trace(err)
	}
	if ret.IsStaleness {
		return ErrViewInvalid.GenWithStackByArgs(s.ViewName.Schema.L, s.ViewName.Name.L)
	}

	return domain.GetDomain(e.ctx).DDL().CreateView(e.ctx, s)
}

func (e *DDLExec) executeCreateIndex(s *ast.CreateIndexStmt) error {
	if _, ok := e.getLocalTemporaryTable(s.Table.Schema, s.Table.Name); ok {
		return dbterror.ErrUnsupportedLocalTempTableDDL.GenWithStackByArgs("CREATE INDEX")
	}

	return domain.GetDomain(e.ctx).DDL().CreateIndex(e.ctx, s)
}

func (e *DDLExec) executeDropDatabase(s *ast.DropDatabaseStmt) error {
	dbName := s.Name

	// Protect important system table from been dropped by a mistake.
	// I can hardly find a case that a user really need to do this.
	if dbName.L == "mysql" {
		return errors.New("Drop 'mysql' database is forbidden")
	}

	err := domain.GetDomain(e.ctx).DDL().DropSchema(e.ctx, s)
	sessionVars := e.ctx.GetSessionVars()
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
	return domain.GetDomain(e.ctx).DDL().DropTable(e.ctx, s)
}

func (e *DDLExec) executeDropView(s *ast.DropTableStmt) error {
	return domain.GetDomain(e.ctx).DDL().DropView(e.ctx, s)
}

func (e *DDLExec) executeDropSequence(s *ast.DropSequenceStmt) error {
	return domain.GetDomain(e.ctx).DDL().DropSequence(e.ctx, s)
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

	return domain.GetDomain(e.ctx).DDL().DropIndex(e.ctx, s)
}

func (e *DDLExec) executeAlterTable(ctx context.Context, s *ast.AlterTableStmt) error {
	if _, ok := e.getLocalTemporaryTable(s.Table.Schema, s.Table.Name); ok {
		return dbterror.ErrUnsupportedLocalTempTableDDL.GenWithStackByArgs("ALTER TABLE")
	}

	return domain.GetDomain(e.ctx).DDL().AlterTable(ctx, e.ctx, s)
}

// executeRecoverTable represents a recover table executor.
// It is built from "recover table" statement,
// is used to recover the table that deleted by mistake.
func (e *DDLExec) executeRecoverTable(s *ast.RecoverTableStmt) error {
	dom := domain.GetDomain(e.ctx)
	var job *model.Job
	var err error
	var tblInfo *model.TableInfo
	if s.JobID != 0 {
		job, tblInfo, err = e.getRecoverTableByJobID(s, dom)
	} else {
		job, tblInfo, err = e.getRecoverTableByTableName(s.Table)
	}
	if err != nil {
		return err
	}
	// Check the table ID was not exists.
	tbl, ok := dom.InfoSchema().TableByID(tblInfo.ID)
	if ok {
		return infoschema.ErrTableExists.GenWithStack("Table '%-.192s' already been recover to '%-.192s', can't be recover repeatedly", s.Table.Name.O, tbl.Meta().Name.O)
	}

	m, err := domain.GetDomain(e.ctx).GetSnapshotMeta(job.StartTS)
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
	err = domain.GetDomain(e.ctx).DDL().RecoverTable(e.ctx, recoverInfo)
	return err
}

func (e *DDLExec) getRecoverTableByJobID(s *ast.RecoverTableStmt, dom *domain.Domain) (*model.Job, *model.TableInfo, error) {
	se, err := e.getSysSession()
	if err != nil {
		return nil, nil, err
	}
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	defer e.releaseSysSession(ctx, se)
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
	err = gcutil.ValidateSnapshot(e.ctx, job.StartTS)
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
	getTable := func(StartTS uint64, SchemaID int64, TableID int64) (*model.TableInfo, error) {
		snapMeta, err := dom.GetSnapshotMeta(StartTS)
		if err != nil {
			return nil, err
		}
		tbl, err := snapMeta.GetTable(SchemaID, TableID)
		return tbl, err
	}
	return ddl.GetDropOrTruncateTableInfoFromJobsByStore(jobs, gcSafePoint, getTable, fn)
}

func (e *DDLExec) getRecoverTableByTableName(tableName *ast.TableName) (*model.Job, *model.TableInfo, error) {
	txn, err := e.ctx.Txn(true)
	if err != nil {
		return nil, nil, err
	}
	schemaName := tableName.Schema.L
	if schemaName == "" {
		schemaName = strings.ToLower(e.ctx.GetSessionVars().CurrentDB)
	}
	if schemaName == "" {
		return nil, nil, errors.Trace(core.ErrNoDB)
	}
	gcSafePoint, err := gcutil.GetGCSafePoint(e.ctx)
	if err != nil {
		return nil, nil, err
	}
	var jobInfo *model.Job
	var tableInfo *model.TableInfo
	dom := domain.GetDomain(e.ctx)
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
		return nil, nil, errUnsupportedFlashbackTmpTable
	}
	return jobInfo, tableInfo, nil
}

func (e *DDLExec) executeFlashBackCluster(ctx context.Context, s *ast.FlashBackClusterStmt) error {
	checker := privilege.GetPrivilegeManager(e.ctx)
	if !checker.RequestVerification(e.ctx.GetSessionVars().ActiveRoles, "", "", "", mysql.SuperPriv) {
		return core.ErrSpecificAccessDenied.GenWithStackByArgs("SUPER")
	}

	tiFlashInfo, err := getTiFlashStores(e.ctx)
	if err != nil {
		return err
	}
	if len(tiFlashInfo) != 0 {
		return errors.Errorf("not support flash back cluster with TiFlash stores")
	}

	flashbackTS, err := staleread.CalculateAsOfTsExpr(e.ctx, s.FlashbackTS)
	if err != nil {
		return err
	}

	return domain.GetDomain(e.ctx).DDL().FlashbackCluster(e.ctx, flashbackTS)
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
	is := domain.GetDomain(e.ctx).InfoSchema()
	tbl, ok := is.TableByID(tblInfo.ID)
	if ok {
		return infoschema.ErrTableExists.GenWithStack("Table '%-.192s' already been flashback to '%-.192s', can't be flashback repeatedly", s.Table.Name.O, tbl.Meta().Name.O)
	}

	m, err := domain.GetDomain(e.ctx).GetSnapshotMeta(job.StartTS)
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
	err = domain.GetDomain(e.ctx).DDL().RecoverTable(e.ctx, recoverInfo)
	return err
}

func (e *DDLExec) executeLockTables(s *ast.LockTablesStmt) error {
	if !config.TableLockEnabled() {
		e.ctx.GetSessionVars().StmtCtx.AppendWarning(ErrFuncNotEnabled.GenWithStackByArgs("LOCK TABLES", "enable-table-lock"))
		return nil
	}

	for _, tb := range s.TableLocks {
		if _, ok := e.getLocalTemporaryTable(tb.Table.Schema, tb.Table.Name); ok {
			return dbterror.ErrUnsupportedLocalTempTableDDL.GenWithStackByArgs("LOCK TABLES")
		}
	}

	return domain.GetDomain(e.ctx).DDL().LockTables(e.ctx, s)
}

func (e *DDLExec) executeUnlockTables(_ *ast.UnlockTablesStmt) error {
	if !config.TableLockEnabled() {
		e.ctx.GetSessionVars().StmtCtx.AppendWarning(ErrFuncNotEnabled.GenWithStackByArgs("UNLOCK TABLES", "enable-table-lock"))
		return nil
	}
	lockedTables := e.ctx.GetAllTableLocks()
	return domain.GetDomain(e.ctx).DDL().UnlockTables(e.ctx, lockedTables)
}

func (e *DDLExec) executeCleanupTableLock(s *ast.CleanupTableLockStmt) error {
	for _, tb := range s.Tables {
		if _, ok := e.getLocalTemporaryTable(tb.Schema, tb.Name); ok {
			return dbterror.ErrUnsupportedLocalTempTableDDL.GenWithStackByArgs("ADMIN CLEANUP TABLE LOCK")
		}
	}
	return domain.GetDomain(e.ctx).DDL().CleanupTableLock(e.ctx, s.Tables)
}

func (e *DDLExec) executeRepairTable(s *ast.RepairTableStmt) error {
	return domain.GetDomain(e.ctx).DDL().RepairTable(e.ctx, s.Table, s.CreateStmt)
}

func (e *DDLExec) executeCreateSequence(s *ast.CreateSequenceStmt) error {
	return domain.GetDomain(e.ctx).DDL().CreateSequence(e.ctx, s)
}

func (e *DDLExec) executeAlterSequence(s *ast.AlterSequenceStmt) error {
	return domain.GetDomain(e.ctx).DDL().AlterSequence(e.ctx, s)
}

func (e *DDLExec) executeCreatePlacementPolicy(s *ast.CreatePlacementPolicyStmt) error {
	return domain.GetDomain(e.ctx).DDL().CreatePlacementPolicy(e.ctx, s)
}

func (e *DDLExec) executeDropPlacementPolicy(s *ast.DropPlacementPolicyStmt) error {
	return domain.GetDomain(e.ctx).DDL().DropPlacementPolicy(e.ctx, s)
}

func (e *DDLExec) executeAlterPlacementPolicy(s *ast.AlterPlacementPolicyStmt) error {
	return domain.GetDomain(e.ctx).DDL().AlterPlacementPolicy(e.ctx, s)
}
