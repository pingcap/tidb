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
	"context"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/admin"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/gcutil"
	"github.com/pingcap/tidb/util/sqlexec"
	log "github.com/sirupsen/logrus"
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
	if e.ctx.GetSessionVars().StmtCtx.IsDDLJobInQueue {
		return errors.Trace(err)
	}

	// Before the DDL job is ready, it encouters an error that may be due to the outdated schema information.
	// After the DDL job is ready, the ErrInfoSchemaChanged error won't happen because we are getting the schema directly from storage.
	// So we needn't to consider this condition.
	// Here we distinguish the ErrInfoSchemaChanged error from other errors.
	dom := domain.GetDomain(e.ctx)
	checker := domain.NewSchemaChecker(dom, e.is.SchemaMetaVersion(), nil)
	txn, err1 := e.ctx.Txn(true)
	if err1 != nil {
		log.Error(err)
		return errors.Trace(err1)
	}
	schemaInfoErr := checker.Check(txn.StartTS())
	if schemaInfoErr != nil {
		return errors.Trace(schemaInfoErr)
	}
	return errors.Trace(err)
}

// Next implements the Executor Next interface.
func (e *DDLExec) Next(ctx context.Context, req *chunk.RecordBatch) (err error) {
	if e.done {
		return nil
	}
	e.done = true

	// For each DDL, we should commit the previous transaction and create a new transaction.
	if err = e.ctx.NewTxn(ctx); err != nil {
		return errors.Trace(err)
	}
	defer func() { e.ctx.GetSessionVars().StmtCtx.IsDDLJobInQueue = false }()

	switch x := e.stmt.(type) {
	case *ast.TruncateTableStmt:
		err = e.executeTruncateTable(x)
	case *ast.CreateDatabaseStmt:
		err = e.executeCreateDatabase(x)
	case *ast.CreateTableStmt:
		err = e.executeCreateTable(x)
	case *ast.CreateViewStmt:
		err = e.executeCreateView(x)
	case *ast.CreateIndexStmt:
		err = e.executeCreateIndex(x)
	case *ast.DropDatabaseStmt:
		err = e.executeDropDatabase(x)
	case *ast.DropTableStmt:
		err = e.executeDropTableOrView(x)
	case *ast.DropIndexStmt:
		err = e.executeDropIndex(x)
	case *ast.AlterTableStmt:
		err = e.executeAlterTable(x)
	case *ast.RenameTableStmt:
		err = e.executeRenameTable(x)
	}
	if err != nil {
		return errors.Trace(e.toErr(err))
	}

	dom := domain.GetDomain(e.ctx)
	// Update InfoSchema in TxnCtx, so it will pass schema check.
	is := dom.InfoSchema()
	txnCtx := e.ctx.GetSessionVars().TxnCtx
	txnCtx.InfoSchema = is
	txnCtx.SchemaVersion = is.SchemaMetaVersion()
	// DDL will force commit old transaction, after DDL, in transaction status should be false.
	e.ctx.GetSessionVars().SetStatusFlag(mysql.ServerStatusInTrans, false)
	return nil
}

func (e *DDLExec) executeTruncateTable(s *ast.TruncateTableStmt) error {
	ident := ast.Ident{Schema: s.Table.Schema, Name: s.Table.Name}
	err := domain.GetDomain(e.ctx).DDL().TruncateTable(e.ctx, ident)
	return errors.Trace(err)
}

func (e *DDLExec) executeRenameTable(s *ast.RenameTableStmt) error {
	if len(s.TableToTables) != 1 {
		// Now we only allow one schema changing at the same time.
		return errors.Errorf("can't run multi schema change")
	}
	oldIdent := ast.Ident{Schema: s.OldTable.Schema, Name: s.OldTable.Name}
	newIdent := ast.Ident{Schema: s.NewTable.Schema, Name: s.NewTable.Name}
	isAlterTable := false
	err := domain.GetDomain(e.ctx).DDL().RenameTable(e.ctx, oldIdent, newIdent, isAlterTable)
	return errors.Trace(err)
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
	return errors.Trace(err)
}

func (e *DDLExec) executeCreateTable(s *ast.CreateTableStmt) error {
	err := domain.GetDomain(e.ctx).DDL().CreateTable(e.ctx, s)
	return errors.Trace(err)
}

func (e *DDLExec) executeCreateView(s *ast.CreateViewStmt) error {
	err := domain.GetDomain(e.ctx).DDL().CreateView(e.ctx, s)
	return errors.Trace(err)
}

func (e *DDLExec) executeCreateIndex(s *ast.CreateIndexStmt) error {
	ident := ast.Ident{Schema: s.Table.Schema, Name: s.Table.Name}
	err := domain.GetDomain(e.ctx).DDL().CreateIndex(e.ctx, ident, s.Unique, model.NewCIStr(s.IndexName), s.IndexColNames, s.IndexOption)
	return errors.Trace(err)
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
		err = variable.SetSessionSystemVar(sessionVars, variable.CharsetDatabase, types.NewStringDatum("utf8"))
		if err != nil {
			return errors.Trace(err)
		}
		err = variable.SetSessionSystemVar(sessionVars, variable.CollationDatabase, types.NewStringDatum("utf8_unicode_ci"))
		if err != nil {
			return errors.Trace(err)
		}
	}
	return errors.Trace(err)
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

func (e *DDLExec) executeDropTableOrView(s *ast.DropTableStmt) error {
	var notExistTables []string
	for _, tn := range s.Tables {
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
			return errors.Trace(err)
		}

		// Protect important system table from been dropped by a mistake.
		// I can hardly find a case that a user really need to do this.
		if isSystemTable(tn.Schema.L, tn.Name.L) {
			return errors.Errorf("Drop tidb system table '%s.%s' is forbidden", tn.Schema.L, tn.Name.L)
		}

		if config.CheckTableBeforeDrop {
			log.Warnf("admin check table `%s`.`%s` before drop.", fullti.Schema.O, fullti.Name.O)
			sql := fmt.Sprintf("admin check table `%s`.`%s`", fullti.Schema.O, fullti.Name.O)
			_, _, err = e.ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(e.ctx, sql)
			if err != nil {
				return errors.Trace(err)
			}
		}

		if s.IsView {
			err = domain.GetDomain(e.ctx).DDL().DropView(e.ctx, fullti)
		} else {
			err = domain.GetDomain(e.ctx).DDL().DropTable(e.ctx, fullti)
		}
		if infoschema.ErrDatabaseNotExists.Equal(err) || infoschema.ErrTableNotExists.Equal(err) {
			notExistTables = append(notExistTables, fullti.String())
		} else if err != nil {
			return errors.Trace(err)
		}
	}
	if len(notExistTables) > 0 && !s.IfExists {
		return infoschema.ErrTableDropExists.GenWithStackByArgs(strings.Join(notExistTables, ","))
	}
	return nil
}

func (e *DDLExec) executeDropIndex(s *ast.DropIndexStmt) error {
	ti := ast.Ident{Schema: s.Table.Schema, Name: s.Table.Name}
	err := domain.GetDomain(e.ctx).DDL().DropIndex(e.ctx, ti, model.NewCIStr(s.IndexName))
	if (infoschema.ErrDatabaseNotExists.Equal(err) || infoschema.ErrTableNotExists.Equal(err)) && s.IfExists {
		err = nil
	}
	return errors.Trace(err)
}

func (e *DDLExec) executeAlterTable(s *ast.AlterTableStmt) error {
	ti := ast.Ident{Schema: s.Table.Schema, Name: s.Table.Name}
	err := domain.GetDomain(e.ctx).DDL().AlterTable(e.ctx, ti, s.Specs)
	return errors.Trace(err)
}

// RestoreTableExec represents a recover table executor.
// It is built from "admin restore table by job" statement,
// is used to recover the table that deleted by mistake.
type RestoreTableExec struct {
	baseExecutor
	jobID  int64
	Table  *ast.TableName
	JobNum int64
}

// Open implements the Executor Open interface.
func (e *RestoreTableExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// Next implements the Executor Open interface.
func (e *RestoreTableExec) Next(ctx context.Context, req *chunk.RecordBatch) (err error) {
	// Should commit the previous transaction and create a new transaction.
	if err = e.ctx.NewTxn(ctx); err != nil {
		return errors.Trace(err)
	}
	defer func() { e.ctx.GetSessionVars().StmtCtx.IsDDLJobInQueue = false }()

	err = e.executeRestoreTable()
	if err != nil {
		return errors.Trace(err)
	}

	dom := domain.GetDomain(e.ctx)
	// Update InfoSchema in TxnCtx, so it will pass schema check.
	is := dom.InfoSchema()
	txnCtx := e.ctx.GetSessionVars().TxnCtx
	txnCtx.InfoSchema = is
	txnCtx.SchemaVersion = is.SchemaMetaVersion()
	// DDL will force commit old transaction, after DDL, in transaction status should be false.
	e.ctx.GetSessionVars().SetStatusFlag(mysql.ServerStatusInTrans, false)
	return nil
}

func (e *RestoreTableExec) executeRestoreTable() error {
	txn, err := e.ctx.Txn(true)
	if err != nil {
		return errors.Trace(err)
	}
	t := meta.NewMeta(txn)
	dom := domain.GetDomain(e.ctx)
	var job *model.Job
	var tblInfo *model.TableInfo
	if e.jobID != 0 {
		job, tblInfo, err = getRestoreTableByJobID(e, t, dom)
	} else {
		job, tblInfo, err = getRestoreTableByTableName(e, t, dom)
	}
	if err != nil {
		return errors.Trace(err)
	}
	// Get table original autoID before table drop.
	m, err := dom.GetSnapshotMeta(job.StartTS)
	if err != nil {
		return errors.Trace(err)
	}
	autoID, err := m.GetAutoTableID(job.SchemaID, job.TableID)
	if err != nil {
		return errors.Errorf("recover table_id: %d, get original autoID from snapshot meta err: %s", job.TableID, err.Error())
	}
	// Call DDL RestoreTable
	err = domain.GetDomain(e.ctx).DDL().RestoreTable(e.ctx, tblInfo, job.SchemaID, autoID, job.ID, job.StartTS)
	return errors.Trace(err)
}

func getRestoreTableByJobID(e *RestoreTableExec, t *meta.Meta, dom *domain.Domain) (*model.Job, *model.TableInfo, error) {
	job, err := t.GetHistoryDDLJob(e.jobID)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	if job == nil {
		return nil, nil, admin.ErrDDLJobNotFound.GenWithStackByArgs(e.jobID)
	}
	if job.Type != model.ActionDropTable {
		return nil, nil, errors.Errorf("Job %v type is %v, not drop table", job.ID, job.Type)
	}

	// Check GC safe point for getting snapshot infoSchema.
	err = gcutil.ValidateSnapshot(e.ctx, job.StartTS)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	// Get the snapshot infoSchema before drop table.
	snapInfo, err := dom.GetSnapshotInfoSchema(job.StartTS)
	if err != nil {
		return nil, nil, errors.Trace(err)
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

func getRestoreTableByTableName(e *RestoreTableExec, t *meta.Meta, dom *domain.Domain) (*model.Job, *model.TableInfo, error) {
	jobs, err := t.GetAllHistoryDDLJobs()
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	var job *model.Job
	var tblInfo *model.TableInfo
	gcSafePoint, err := gcutil.GetGCSafePoint(e.ctx)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	schemaName := e.Table.Schema.L
	if schemaName == "" {
		schemaName = e.ctx.GetSessionVars().CurrentDB
	}
	if schemaName == "" {
		return nil, nil, errors.Trace(core.ErrNoDB)
	}
	// TODO: only search recent `e.JobNum` DDL jobs.
	for i := len(jobs) - 1; i > 0; i-- {
		job = jobs[i]
		if job.Type != model.ActionDropTable {
			continue
		}
		// Check GC safe point for getting snapshot infoSchema.
		err = gcutil.ValidateSnapshotWithGCSafePoint(job.StartTS, gcSafePoint)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		// Get the snapshot infoSchema before drop table.
		snapInfo, err := dom.GetSnapshotInfoSchema(job.StartTS)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		// Get table meta from snapshot infoSchema.
		table, ok := snapInfo.TableByID(job.TableID)
		if !ok {
			return nil, nil, infoschema.ErrTableNotExists.GenWithStackByArgs(
				fmt.Sprintf("(Schema ID %d)", job.SchemaID),
				fmt.Sprintf("(Table ID %d)", job.TableID),
			)
		}
		if table.Meta().Name.L == e.Table.Name.L {
			schema, ok := dom.InfoSchema().SchemaByID(job.SchemaID)
			if !ok {
				return nil, nil, errors.Trace(infoschema.ErrDatabaseNotExists.GenWithStackByArgs(
					fmt.Sprintf("(Schema ID %d)", job.SchemaID),
				))
			}
			if schema.Name.L == schemaName {
				tblInfo = table.Meta()
				break
			}
		}
	}
	if tblInfo == nil {
		return nil, nil, errors.Errorf("Can't found drop table: %v in ddl history jobs", e.Table.Name)
	}
	return job, tblInfo, nil
}
