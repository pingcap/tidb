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
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/temptable"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// DDLExec represents a DDL executor.
// It grabs a DDL instance from Domain, calling the DDL methods to do the work.
type DDLExec struct {
	exec.BaseExecutor

	ddlExecutor  ddl.Executor
	stmt         ast.StmtNode
	is           infoschema.InfoSchema
	tempTableDDL temptable.TemporaryTableDDL
	done         bool
}

// toErr converts the error to the ErrInfoSchemaChanged when the schema is outdated.
func (e *DDLExec) toErr(err error) error {
	// The err may be cause by schema changed, here we distinguish the ErrInfoSchemaChanged error from other errors.
	dom := domain.GetDomain(e.Ctx())
	checker := domain.NewSchemaChecker(dom.GetSchemaValidator(), e.is.SchemaMetaVersion(), nil, true)
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

func (e *DDLExec) getLocalTemporaryTable(schema ast.CIStr, table ast.CIStr) (table.Table, bool, error) {
	tbl, err := e.Ctx().GetInfoSchema().(infoschema.InfoSchema).TableByName(context.Background(), schema, table)
	if infoschema.ErrTableNotExists.Equal(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, errors.Trace(err)
	}

	if tbl.Meta().TempTableType != model.TempTableLocal {
		return nil, false, nil
	}

	return tbl, true, nil
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
			_, ok, err := e.getLocalTemporaryTable(s.Tables[tbIdx].Schema, s.Tables[tbIdx].Name)
			if err != nil {
				return errors.Trace(err)
			}
			if ok {
				localTempTablesToDrop = append(localTempTablesToDrop, s.Tables[tbIdx])
				// TODO: investigate why this does not work instead:
				//s.Tables = slices.Delete(s.Tables, tbIdx, tbIdx+1)
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
		e.Ctx().GetSessionVars().StmtCtx.IsDDLJobInQueue.Store(false)
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
		isDDLJobInQueue := e.Ctx().GetSessionVars().StmtCtx.IsDDLJobInQueue.Load()
		if (isDDLJobInQueue && infoschema.ErrTableNotExists.Equal(err)) || !isDDLJobInQueue {
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
	_, exist, err := e.getLocalTemporaryTable(s.Table.Schema, s.Table.Name)
	if err != nil {
		return errors.Trace(err)
	}
	if exist {
		return e.tempTableDDL.TruncateLocalTemporaryTable(s.Table.Schema, s.Table.Name)
	}
	err = e.ddlExecutor.TruncateTable(e.Ctx(), ident)
	return err
}

func (e *DDLExec) executeRenameTable(s *ast.RenameTableStmt) error {
	for _, tables := range s.TableToTables {
		_, ok, err := e.getLocalTemporaryTable(tables.OldTable.Schema, tables.OldTable.Name)
		if err != nil {
			return errors.Trace(err)
		}
		if ok {
			return dbterror.ErrUnsupportedLocalTempTableDDL.GenWithStackByArgs("RENAME TABLE")
		}
	}
	return e.ddlExecutor.RenameTable(e.Ctx(), s)
}

func (e *DDLExec) executeCreateDatabase(s *ast.CreateDatabaseStmt) error {
	err := e.ddlExecutor.CreateSchema(e.Ctx(), s)
	return err
}

func (e *DDLExec) executeAlterDatabase(s *ast.AlterDatabaseStmt) error {
	err := e.ddlExecutor.AlterSchema(e.Ctx(), s)
	return err
}

func (e *DDLExec) executeCreateTable(s *ast.CreateTableStmt) error {
	err := e.ddlExecutor.CreateTable(e.Ctx(), s)
	return err
}

func (e *DDLExec) createSessionTemporaryTable(s *ast.CreateTableStmt) error {
	is := e.Ctx().GetInfoSchema().(infoschema.InfoSchema)
	dbInfo, ok := is.SchemaByName(s.Table.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(s.Table.Schema.O)
	}

	_, exists, err := e.getLocalTemporaryTable(s.Table.Schema, s.Table.Name)
	if err != nil {
		return errors.Trace(err)
	}
	if exists {
		err := infoschema.ErrTableExists.FastGenByArgs(ast.Ident{Schema: s.Table.Schema, Name: s.Table.Name})
		if s.IfNotExists {
			e.Ctx().GetSessionVars().StmtCtx.AppendNote(err)
			return nil
		}
		return errors.Trace(err)
	}

	tbInfo, err := ddl.BuildSessionTemporaryTableInfo(ddl.NewMetaBuildContextWithSctx(e.Ctx()), e.Ctx().GetStore(), is, s,
		dbInfo.Charset, dbInfo.Collate, dbInfo.PlacementPolicyRef)
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
	nodeW := resolve.NewNodeW(s.Select)
	err := core.Preprocess(ctx, e.Ctx(), nodeW, core.WithPreprocessorReturn(ret))
	if err != nil {
		return errors.Trace(err)
	}
	if ret.IsStaleness {
		return exeerrors.ErrViewInvalid.GenWithStackByArgs(s.ViewName.Schema.L, s.ViewName.Name.L)
	}

	e.Ctx().GetSessionVars().ClearRelatedTableForMDL()
	return e.ddlExecutor.CreateView(e.Ctx(), s)
}

func (e *DDLExec) executeCreateIndex(s *ast.CreateIndexStmt) error {
	_, ok, err := e.getLocalTemporaryTable(s.Table.Schema, s.Table.Name)
	if err != nil {
		return errors.Trace(err)
	}
	if ok {
		return dbterror.ErrUnsupportedLocalTempTableDDL.GenWithStackByArgs("CREATE INDEX")
	}

	return e.ddlExecutor.CreateIndex(e.Ctx(), s)
}

func (e *DDLExec) executeDropDatabase(s *ast.DropDatabaseStmt) error {
	dbName := s.Name

	// Protect important system table from been dropped by a mistake.
	// I can hardly find a case that a user really need to do this.
	if dbName.L == "mysql" {
		return dbterror.ErrForbiddenDDL.FastGenByArgs("Drop 'mysql' database")
	}

	err := e.ddlExecutor.DropSchema(e.Ctx(), s)
	sessionVars := e.Ctx().GetSessionVars()
	if err == nil && strings.ToLower(sessionVars.CurrentDB) == dbName.L {
		sessionVars.CurrentDB = ""
		err = sessionVars.SetSystemVar(vardef.CharsetDatabase, mysql.DefaultCharset)
		if err != nil {
			return err
		}
		err = sessionVars.SetSystemVar(vardef.CollationDatabase, mysql.DefaultCollationName)
		if err != nil {
			return err
		}
	}
	return err
}

func (e *DDLExec) executeDropTable(s *ast.DropTableStmt) error {
	return e.ddlExecutor.DropTable(e.Ctx(), s)
}

func (e *DDLExec) executeDropView(s *ast.DropTableStmt) error {
	return e.ddlExecutor.DropView(e.Ctx(), s)
}

func (e *DDLExec) executeDropSequence(s *ast.DropSequenceStmt) error {
	return e.ddlExecutor.DropSequence(e.Ctx(), s)
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
	_, ok, err := e.getLocalTemporaryTable(s.Table.Schema, s.Table.Name)
	if err != nil {
		return errors.Trace(err)
	}
	if ok {
		return dbterror.ErrUnsupportedLocalTempTableDDL.GenWithStackByArgs("DROP INDEX")
	}

	return e.ddlExecutor.DropIndex(e.Ctx(), s)
}

func (e *DDLExec) executeAlterTable(ctx context.Context, s *ast.AlterTableStmt) error {
	_, ok, err := e.getLocalTemporaryTable(s.Table.Schema, s.Table.Name)
	if err != nil {
		return errors.Trace(err)
	}
	if ok {
		return dbterror.ErrUnsupportedLocalTempTableDDL.GenWithStackByArgs("ALTER TABLE")
	}

	return e.ddlExecutor.AlterTable(ctx, e.Ctx(), s)
}

// executeRecoverTable represents a recover table executor.
// It is built from "recover table" statement,
// is used to recover the table that deleted by mistake.
