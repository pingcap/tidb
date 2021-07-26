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
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/admin"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/gcutil"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

// DDLExec represents a DDL executor.
// It grabs a DDL instance from Domain, calling the DDL methods to do the work.
type DDLExec struct {
	baseExecutor

	stmt ast.StmtNode
	is   infoschema.InfoSchema
	done bool
}

// toErr converts the error to the ErrInfoSchemaChanged when the schema is outdated.
func (e *DDLExec) toErr(err error) error {
	// The err may be cause by schema changed, here we distinguish the ErrInfoSchemaChanged error from other errors.
	dom := domain.GetDomain(e.ctx)
	checker := domain.NewSchemaChecker(dom, e.is.SchemaMetaVersion(), nil)
	txn, err1 := e.ctx.Txn(true)
	if err1 != nil {
		logutil.BgLogger().Error("active txn failed", zap.Error(err))
		return err1
	}
	_, schemaInfoErr := checker.Check(txn.StartTS())
	if schemaInfoErr != nil {
		return errors.Trace(schemaInfoErr)
	}
	return err
}

// deleteTemporaryTableRecords delete temporary table data.
func deleteTemporaryTableRecords(memData kv.MemBuffer, tblID int64) error {
	if memData == nil {
		return kv.ErrNotExist
	}

	tblPrefix := tablecodec.EncodeTablePrefix(tblID)
	endKey := tablecodec.EncodeTablePrefix(tblID + 1)

	iter, err := memData.Iter(tblPrefix, endKey)
	if err != nil {
		return err
	}
	for iter.Valid() {
		key := iter.Key()
		if !bytes.HasPrefix(key, tblPrefix) {
			break
		}

		err = memData.Delete(key)
		if err != nil {
			return err
		}

		err = iter.Next()
		if err != nil {
			return err
		}
	}

	return nil
}

// Next implements the Executor Next interface.
func (e *DDLExec) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	if e.done {
		return nil
	}
	e.done = true

	// For each DDL, we should commit the previous transaction and create a new transaction.
	// Following cases are exceptions
	var localTempTablesToDrop []*model.TableInfo
	switch s := e.stmt.(type) {
	case *ast.CreateTableStmt:
		if s.TemporaryKeyword == ast.TemporaryLocal {
			return e.createSessionTemporaryTable(s)
		}
	case *ast.DropTableStmt:
		if s.IsView {
			break
		}
		sessVars := e.ctx.GetSessionVars()
		sessVarsTempTable := sessVars.LocalTemporaryTables
		if sessVarsTempTable == nil {
			break
		}
		localTemporaryTables := sessVarsTempTable.(*infoschema.LocalTemporaryTables)
		for tbIdx := len(s.Tables) - 1; tbIdx >= 0; tbIdx-- {
			tb := s.Tables[tbIdx]
			if tableInfo, ok := localTemporaryTables.TableByName(s.Tables[tbIdx].Schema, s.Tables[tbIdx].Name); ok {
				localTempTablesToDrop = append(localTempTablesToDrop, tableInfo.Meta())
				localTemporaryTables.RemoveTable(tb.Schema, tb.Name)
				s.Tables = append(s.Tables[:tbIdx], s.Tables[tbIdx+1:]...)
			}
		}
		// if all tables are local temporary, directly drop those tables.
		if len(s.Tables) == 0 {
			return e.dropLocalTemporaryTables(localTempTablesToDrop)
		}
	}

	if err = e.ctx.NewTxn(ctx); err != nil {
		return err
	}

	defer func() { e.ctx.GetSessionVars().StmtCtx.IsDDLJobInQueue = false }()

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
	err := domain.GetDomain(e.ctx).DDL().TruncateTable(e.ctx, ident)
	return err
}

func (e *DDLExec) executeRenameTable(s *ast.RenameTableStmt) error {
	isAlterTable := false
	var err error
	if len(s.TableToTables) == 1 {
		oldIdent := ast.Ident{Schema: s.TableToTables[0].OldTable.Schema, Name: s.TableToTables[0].OldTable.Name}
		newIdent := ast.Ident{Schema: s.TableToTables[0].NewTable.Schema, Name: s.TableToTables[0].NewTable.Name}
		err = domain.GetDomain(e.ctx).DDL().RenameTable(e.ctx, oldIdent, newIdent, isAlterTable)
	} else {
		oldIdents := make([]ast.Ident, 0, len(s.TableToTables))
		newIdents := make([]ast.Ident, 0, len(s.TableToTables))
		for _, tables := range s.TableToTables {
			oldIdent := ast.Ident{Schema: tables.OldTable.Schema, Name: tables.OldTable.Name}
			newIdent := ast.Ident{Schema: tables.NewTable.Schema, Name: tables.NewTable.Name}
			oldIdents = append(oldIdents, oldIdent)
			newIdents = append(newIdents, newIdent)
		}
		err = domain.GetDomain(e.ctx).DDL().RenameTables(e.ctx, oldIdents, newIdents, isAlterTable)
	}
	return err
}

func (e *DDLExec) executeCreateDatabase(s *ast.CreateDatabaseStmt) error {
	var opt *ast.CharsetOpt
	if len(s.Options) != 0 {
		opt = &ast.CharsetOpt{}
		for _, val := range s.Options {
			switch val.Tp {
			case ast.DatabaseOptionCharset:
				opt.Chs = val.Value
			case ast.DatabaseOptionCollate:
				opt.Col = val.Value
			}
		}
	}
	err := domain.GetDomain(e.ctx).DDL().CreateSchema(e.ctx, model.NewCIStr(s.Name), opt)
	if err != nil {
		if infoschema.ErrDatabaseExists.Equal(err) && s.IfNotExists {
			err = nil
		}
	}
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
	tbInfo, err := ddl.BuildTableInfoWithCheck(e.ctx, s, dbInfo.Charset, dbInfo.Collate)
	if err != nil {
		return err
	}

	dom := domain.GetDomain(e.ctx)
	// Local temporary table uses a real table ID.
	// We could mock a table ID, but the mocked ID might be identical to an existing
	// real table, and then we'll get into trouble.
	err = kv.RunInNewTxn(context.Background(), dom.Store(), true, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		tblID, err := m.GenGlobalID()
		if err != nil {
			return errors.Trace(err)
		}
		tbInfo.ID = tblID
		tbInfo.State = model.StatePublic
		return nil
	})
	if err != nil {
		return err
	}

	// AutoID is allocated in mocked..
	alloc := autoid.NewAllocatorFromTempTblInfo(tbInfo)
	tbl, err := tables.TableFromMeta([]autoid.Allocator{alloc}, tbInfo)
	if err != nil {
		return err
	}

	// Store this temporary table to the session.
	sessVars := e.ctx.GetSessionVars()
	if sessVars.LocalTemporaryTables == nil {
		sessVars.LocalTemporaryTables = infoschema.NewLocalTemporaryTables()
	}
	localTempTables := sessVars.LocalTemporaryTables.(*infoschema.LocalTemporaryTables)

	// Init MemBuffer in session
	if sessVars.TemporaryTableData == nil {
		// Create this txn just for getting a MemBuffer. It's a little tricky
		bufferTxn, err := e.ctx.GetStore().BeginWithOption(tikv.DefaultStartTSOption().SetStartTS(0))
		if err != nil {
			return err
		}

		sessVars.TemporaryTableData = bufferTxn.GetMemBuffer()
	}

	err = localTempTables.AddTable(dbInfo, tbl)

	if err != nil && s.IfNotExists && infoschema.ErrTableExists.Equal(err) {
		e.ctx.GetSessionVars().StmtCtx.AppendNote(err)
		return nil
	}

	return err
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
	ident := ast.Ident{Schema: s.Table.Schema, Name: s.Table.Name}
	err := domain.GetDomain(e.ctx).DDL().CreateIndex(e.ctx, ident, s.KeyType, model.NewCIStr(s.IndexName),
		s.IndexPartSpecifications, s.IndexOption, s.IfNotExists)
	return err
}

func (e *DDLExec) executeDropDatabase(s *ast.DropDatabaseStmt) error {
	dbName := model.NewCIStr(s.Name)

	// Protect important system table from been dropped by a mistake.
	// I can hardly find a case that a user really need to do this.
	if dbName.L == "mysql" {
		return errors.New("Drop 'mysql' database is forbidden")
	}

	err := domain.GetDomain(e.ctx).DDL().DropSchema(e.ctx, dbName)
	if infoschema.ErrDatabaseNotExists.Equal(err) {
		if s.IfExists {
			err = nil
		} else {
			err = infoschema.ErrDatabaseDropExists.GenWithStackByArgs(s.Name)
		}
	}
	sessionVars := e.ctx.GetSessionVars()
	if err == nil && strings.ToLower(sessionVars.CurrentDB) == dbName.L {
		sessionVars.CurrentDB = ""
		err = variable.SetSessionSystemVar(sessionVars, variable.CharsetDatabase, mysql.DefaultCharset)
		if err != nil {
			return err
		}
		err = variable.SetSessionSystemVar(sessionVars, variable.CollationDatabase, mysql.DefaultCollationName)
		if err != nil {
			return err
		}
	}
	return err
}

// If one drop those tables by mistake, it's difficult to recover.
// In the worst case, the whole TiDB cluster fails to bootstrap, so we prevent user from dropping them.
var systemTables = map[string]struct{}{
	"tidb":                 {},
	"gc_delete_range":      {},
	"gc_delete_range_done": {},
}

func isSystemTable(schema, table string) bool {
	if schema != "mysql" {
		return false
	}
	if _, ok := systemTables[table]; ok {
		return true
	}
	return false
}

type objectType int

const (
	tableObject objectType = iota
	viewObject
	sequenceObject
)

func (e *DDLExec) executeDropTable(s *ast.DropTableStmt) error {
	return e.dropTableObject(s.Tables, tableObject, s.IfExists)
}

func (e *DDLExec) executeDropView(s *ast.DropTableStmt) error {
	return e.dropTableObject(s.Tables, viewObject, s.IfExists)
}

func (e *DDLExec) executeDropSequence(s *ast.DropSequenceStmt) error {
	return e.dropTableObject(s.Sequences, sequenceObject, s.IfExists)
}

// dropTableObject actually applies to `tableObject`, `viewObject` and `sequenceObject`.
func (e *DDLExec) dropTableObject(objects []*ast.TableName, obt objectType, ifExists bool) error {
	var notExistTables []string
	sessVars := e.ctx.GetSessionVars()
	for _, tn := range objects {
		fullti := ast.Ident{Schema: tn.Schema, Name: tn.Name}
		_, ok := e.is.SchemaByName(tn.Schema)
		if !ok {
			// TODO: we should return special error for table not exist, checking "not exist" is not enough,
			// because some other errors may contain this error string too.
			notExistTables = append(notExistTables, fullti.String())
			continue
		}
		_, err := e.is.TableByName(tn.Schema, tn.Name)
		if err != nil && infoschema.ErrTableNotExists.Equal(err) {
			notExistTables = append(notExistTables, fullti.String())
			continue
		} else if err != nil {
			return err
		}

		// Protect important system table from been dropped by a mistake.
		// I can hardly find a case that a user really need to do this.
		if isSystemTable(tn.Schema.L, tn.Name.L) {
			return errors.Errorf("Drop tidb system table '%s.%s' is forbidden", tn.Schema.L, tn.Name.L)
		}
		tableInfo, err := e.is.TableByName(tn.Schema, tn.Name)
		if err != nil {
			return err
		}
		tempTableType := tableInfo.Meta().TempTableType
		if obt == tableObject && config.CheckTableBeforeDrop && tempTableType == model.TempTableNone {
			logutil.BgLogger().Warn("admin check table before drop",
				zap.String("database", fullti.Schema.O),
				zap.String("table", fullti.Name.O),
			)
			exec := e.ctx.(sqlexec.RestrictedSQLExecutor)
			stmt, err := exec.ParseWithParams(context.TODO(), "admin check table %n.%n", fullti.Schema.O, fullti.Name.O)
			if err != nil {
				return err
			}
			_, _, err = exec.ExecRestrictedStmt(context.TODO(), stmt)
			if err != nil {
				return err
			}
		}
		switch obt {
		case tableObject:
			err = domain.GetDomain(e.ctx).DDL().DropTable(e.ctx, fullti)
		case viewObject:
			err = domain.GetDomain(e.ctx).DDL().DropView(e.ctx, fullti)
		case sequenceObject:
			err = domain.GetDomain(e.ctx).DDL().DropSequence(e.ctx, fullti, ifExists)
		}
		if infoschema.ErrDatabaseNotExists.Equal(err) || infoschema.ErrTableNotExists.Equal(err) {
			notExistTables = append(notExistTables, fullti.String())
		} else if err != nil {
			return err
		}
	}
	if len(notExistTables) > 0 && !ifExists {
		if obt == sequenceObject {
			return infoschema.ErrSequenceDropExists.GenWithStackByArgs(strings.Join(notExistTables, ","))
		}
		return infoschema.ErrTableDropExists.GenWithStackByArgs(strings.Join(notExistTables, ","))
	}
	// We need add warning when use if exists.
	if len(notExistTables) > 0 && ifExists {
		for _, table := range notExistTables {
			if obt == sequenceObject {
				sessVars.StmtCtx.AppendNote(infoschema.ErrSequenceDropExists.GenWithStackByArgs(table))
			} else {
				sessVars.StmtCtx.AppendNote(infoschema.ErrTableDropExists.GenWithStackByArgs(table))
			}
		}
	}
	return nil
}

func (e *DDLExec) dropLocalTemporaryTables(localTempTables []*model.TableInfo) error {
	if len(localTempTables) == 0 {
		return nil
	}
	sessVars := e.ctx.GetSessionVars()
	for _, tb := range localTempTables {
		err := deleteTemporaryTableRecords(sessVars.TemporaryTableData, tb.ID)
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *DDLExec) executeDropIndex(s *ast.DropIndexStmt) error {
	ti := ast.Ident{Schema: s.Table.Schema, Name: s.Table.Name}
	err := domain.GetDomain(e.ctx).DDL().DropIndex(e.ctx, ti, model.NewCIStr(s.IndexName), s.IfExists)
	if (infoschema.ErrDatabaseNotExists.Equal(err) || infoschema.ErrTableNotExists.Equal(err)) && s.IfExists {
		err = nil
	}
	return err
}

func (e *DDLExec) executeAlterTable(ctx context.Context, s *ast.AlterTableStmt) error {
	ti := ast.Ident{Schema: s.Table.Schema, Name: s.Table.Name}
	err := domain.GetDomain(e.ctx).DDL().AlterTable(ctx, e.ctx, ti, s.Specs)
	return err
}

// executeRecoverTable represents a recover table executor.
// It is built from "recover table" statement,
// is used to recover the table that deleted by mistake.
func (e *DDLExec) executeRecoverTable(s *ast.RecoverTableStmt) error {
	txn, err := e.ctx.Txn(true)
	if err != nil {
		return err
	}
	t := meta.NewMeta(txn)
	dom := domain.GetDomain(e.ctx)
	var job *model.Job
	var tblInfo *model.TableInfo
	if s.JobID != 0 {
		job, tblInfo, err = e.getRecoverTableByJobID(s, t, dom)
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

	autoIncID, autoRandID, err := e.getTableAutoIDsFromSnapshot(job)
	if err != nil {
		return err
	}

	recoverInfo := &ddl.RecoverInfo{
		SchemaID:      job.SchemaID,
		TableInfo:     tblInfo,
		DropJobID:     job.ID,
		SnapshotTS:    job.StartTS,
		CurAutoIncID:  autoIncID,
		CurAutoRandID: autoRandID,
	}
	// Call DDL RecoverTable.
	err = domain.GetDomain(e.ctx).DDL().RecoverTable(e.ctx, recoverInfo)
	return err
}

func (e *DDLExec) getTableAutoIDsFromSnapshot(job *model.Job) (autoIncID, autoRandID int64, err error) {
	// Get table original autoIDs before table drop.
	dom := domain.GetDomain(e.ctx)
	m, err := dom.GetSnapshotMeta(job.StartTS)
	if err != nil {
		return 0, 0, err
	}
	autoIncID, err = m.GetAutoTableID(job.SchemaID, job.TableID)
	if err != nil {
		return 0, 0, errors.Errorf("recover table_id: %d, get original autoIncID from snapshot meta err: %s", job.TableID, err.Error())
	}
	autoRandID, err = m.GetAutoRandomID(job.SchemaID, job.TableID)
	if err != nil {
		return 0, 0, errors.Errorf("recover table_id: %d, get original autoRandID from snapshot meta err: %s", job.TableID, err.Error())
	}
	return autoIncID, autoRandID, nil
}

func (e *DDLExec) getRecoverTableByJobID(s *ast.RecoverTableStmt, t *meta.Meta, dom *domain.Domain) (*model.Job, *model.TableInfo, error) {
	job, err := t.GetHistoryDDLJob(s.JobID)
	if err != nil {
		return nil, nil, err
	}
	if job == nil {
		return nil, nil, admin.ErrDDLJobNotFound.GenWithStackByArgs(s.JobID)
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
	for _, job := range jobs {
		// Check GC safe point for getting snapshot infoSchema.
		err := gcutil.ValidateSnapshotWithGCSafePoint(job.StartTS, gcSafePoint)
		if err != nil {
			return false, err
		}
		if job.Type != model.ActionDropTable && job.Type != model.ActionTruncateTable {
			continue
		}

		snapMeta, err := dom.GetSnapshotMeta(job.StartTS)
		if err != nil {
			return false, err
		}
		tbl, err := snapMeta.GetTable(job.SchemaID, job.TableID)
		if err != nil {
			if meta.ErrDBNotExists.Equal(err) {
				// The dropped/truncated DDL maybe execute failed that caused by the parallel DDL execution,
				// then can't find the table from the snapshot info-schema. Should just ignore error here,
				// see more in TestParallelDropSchemaAndDropTable.
				continue
			}
			return false, err
		}
		if tbl == nil {
			// The dropped/truncated DDL maybe execute failed that caused by the parallel DDL execution,
			// then can't find the table from the snapshot info-schema. Should just ignore error here,
			// see more in TestParallelDropSchemaAndDropTable.
			continue
		}
		finish, err := fn(job, tbl)
		if err != nil || finish {
			return finish, err
		}
	}
	return false, nil
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
	err = admin.IterHistoryDDLJobs(txn, fn)
	if err != nil {
		if terror.ErrorEqual(variable.ErrSnapshotTooOld, err) {
			return nil, nil, errors.Errorf("Can't find dropped/truncated table '%s' in GC safe point %s", tableName.Name.O, model.TSConvert2Time(gcSafePoint).String())
		}
		return nil, nil, err
	}
	if tableInfo == nil || jobInfo == nil {
		return nil, nil, errors.Errorf("Can't find dropped/truncated table: %v in DDL history jobs", tableName.Name)
	}
	// Dropping local temporary tables won't appear in DDL jobs.
	if tableInfo.TempTableType == model.TempTableGlobal {
		return nil, nil, errUnsupportedFlashbackTmpTable
	}
	return jobInfo, tableInfo, nil
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

	autoIncID, autoRandID, err := e.getTableAutoIDsFromSnapshot(job)
	if err != nil {
		return err
	}
	recoverInfo := &ddl.RecoverInfo{
		SchemaID:      job.SchemaID,
		TableInfo:     tblInfo,
		DropJobID:     job.ID,
		SnapshotTS:    job.StartTS,
		CurAutoIncID:  autoIncID,
		CurAutoRandID: autoRandID,
	}
	// Call DDL RecoverTable.
	err = domain.GetDomain(e.ctx).DDL().RecoverTable(e.ctx, recoverInfo)
	return err
}

func (e *DDLExec) executeLockTables(s *ast.LockTablesStmt) error {
	if !config.TableLockEnabled() {
		return nil
	}
	return domain.GetDomain(e.ctx).DDL().LockTables(e.ctx, s)
}

func (e *DDLExec) executeUnlockTables(_ *ast.UnlockTablesStmt) error {
	if !config.TableLockEnabled() {
		return nil
	}
	lockedTables := e.ctx.GetAllTableLocks()
	return domain.GetDomain(e.ctx).DDL().UnlockTables(e.ctx, lockedTables)
}

func (e *DDLExec) executeCleanupTableLock(s *ast.CleanupTableLockStmt) error {
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
