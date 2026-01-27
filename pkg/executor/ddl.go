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
	"github.com/pingcap/tidb/pkg/materializedview"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/planner"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/sessiontxn/staleread"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/temptable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/gcutil"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
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
	case *ast.CreateMaterializedViewLogStmt:
		err = e.executeCreateMaterializedViewLog(x)
	case *ast.DropMaterializedViewLogStmt:
		err = e.executeDropMaterializedViewLog(x)
	case *ast.CreateMaterializedViewStmt:
		err = e.executeCreateMaterializedView(ctx, x)
	case *ast.DropMaterializedViewStmt:
		err = e.executeDropMaterializedView(x)
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

	is := e.Ctx().GetInfoSchema().(infoschema.InfoSchema)
	schemaName := s.Table.Schema.L
	if schemaName == "" {
		schemaName = strings.ToLower(e.Ctx().GetSessionVars().CurrentDB)
	}
	if schemaName == "" {
		return errors.Trace(plannererrors.ErrNoDB)
	}
	ident.Schema = ast.NewCIStr(schemaName)
	tbl, err := is.TableByName(context.Background(), ast.NewCIStr(schemaName), s.Table.Name)
	if err != nil {
		return errors.Trace(err)
	}
	meta := tbl.Meta()
	if (meta.IsMaterializedView() || meta.IsMaterializedViewLog()) && !e.Ctx().GetSessionVars().InRestrictedSQL {
		obj := "materialized view"
		if meta.IsMaterializedViewLog() {
			obj = "materialized view log"
		}
		return errors.Errorf("truncate %s %s is not supported now", obj, meta.Name.O)
	}
	if !meta.IsMaterializedView() && !meta.IsMaterializedViewLog() {
		if err := e.checkMVDemoBaseTableDDLGate(is, meta.ID, fmt.Sprintf("truncate table %s", meta.Name.O)); err != nil {
			return err
		}
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

func (e *DDLExec) checkMVDemoBaseTableDDLGate(is infoschema.InfoSchema, baseTableID int64, action string) error {
	logInfo, err := materializedview.FindLogTableInfo(is, baseTableID)
	if err != nil {
		return err
	}
	if logInfo != nil {
		return errors.Errorf("cannot %s: materialized view log exists", action)
	}
	hasMV, err := materializedview.HasDependentMaterializedView(is, baseTableID)
	if err != nil {
		return err
	}
	if hasMV {
		return errors.Errorf("cannot %s: dependent materialized view exists", action)
	}
	return nil
}

func (e *DDLExec) executeCreateMaterializedViewLog(s *ast.CreateMaterializedViewLogStmt) error {
	if !e.Ctx().GetSessionVars().EnableMaterializedViewDemo {
		return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("materialized view demo is disabled")
	}

	schemaName := s.Table.Schema.L
	if schemaName == "" {
		schemaName = strings.ToLower(e.Ctx().GetSessionVars().CurrentDB)
	}
	if schemaName == "" {
		return errors.Trace(plannererrors.ErrNoDB)
	}
	schema := ast.NewCIStr(schemaName)

	is := e.Ctx().GetInfoSchema().(infoschema.InfoSchema)
	baseTbl, err := is.TableByName(context.Background(), schema, s.Table.Name)
	if err != nil {
		return errors.Trace(err)
	}
	baseInfo := baseTbl.Meta()
	if baseInfo.IsView() || baseInfo.IsSequence() || baseInfo.IsMaterializedView() || baseInfo.IsMaterializedViewLog() {
		return dbterror.ErrWrongObject.GenWithStackByArgs(schema.L, baseInfo.Name.O, "BASE TABLE")
	}

	if len(s.Columns) == 0 {
		return errors.New("CREATE MATERIALIZED VIEW LOG requires a non-empty column list")
	}

	existingLog, err := materializedview.FindLogTableInfoInSchema(is, schema, baseInfo.ID)
	if err != nil {
		return errors.Trace(err)
	}
	if existingLog != nil {
		return errors.Trace(infoschema.ErrTableExists.GenWithStackByArgs(ast.Ident{Schema: schema, Name: ast.NewCIStr(existingLog.Name.O)}))
	}

	baseColIDs := make([]int64, 0, len(s.Columns))
	logCols := make([]*model.ColumnInfo, 0, len(s.Columns)+2)
	seen := make(map[string]struct{}, len(s.Columns))
	for _, col := range s.Columns {
		colName := col.Name.L
		if _, ok := seen[colName]; ok {
			return infoschema.ErrColumnExists.GenWithStackByArgs(col.Name.O)
		}
		seen[colName] = struct{}{}
		baseCol := model.FindColumnInfo(baseInfo.Columns, colName)
		if baseCol == nil {
			return infoschema.ErrColumnNotExists.GenWithStackByArgs(col.Name.O, baseInfo.Name.O)
		}
		baseColIDs = append(baseColIDs, baseCol.ID)

		logCol := baseCol.Clone()
		logCol.ID = int64(len(logCols) + 1)
		logCol.Offset = len(logCols)
		logCol.Hidden = false
		logCol.State = model.StatePublic
		logCol.ChangeStateInfo = nil
		logCols = append(logCols, logCol)
	}

	metaCols := []struct {
		name string
	}{
		{name: materializedview.MVLogColumnDMLType},
		{name: materializedview.MVLogColumnOldNew},
	}
	for _, m := range metaCols {
		ft := types.NewFieldType(mysql.TypeString)
		ft.SetFlen(1)
		ft.SetCharset(charset.CharsetBin)
		ft.SetCollate(charset.CollationBin)
		ft.SetFlag(mysql.NotNullFlag)
		logCols = append(logCols, &model.ColumnInfo{
			ID:        int64(len(logCols) + 1),
			Name:      ast.NewCIStr(m.name),
			Offset:    len(logCols),
			FieldType: *ft,
			State:     model.StatePublic,
			Version:   model.CurrLatestColumnInfoVersion,
		})
	}

	logTblInfo := &model.TableInfo{
		Name:    ast.NewCIStr(fmt.Sprintf("__tidb_mvlog_%d", baseInfo.ID)),
		Charset: baseInfo.Charset,
		Collate: baseInfo.Collate,
		Columns: logCols,
		// No explicit indices: relies on implicit _tidb_rowid and commit_ts range scan.
		MaterializedViewLogInfo: &model.MaterializedViewLogInfo{
			BaseTableID: baseInfo.ID,
			ColumnIDs:   baseColIDs,
		},
		Version:     model.CurrLatestTableInfoVersion,
		UpdateTS:    baseInfo.UpdateTS,
		MaxColumnID: int64(len(logCols)),
	}

	e.Ctx().GetSessionVars().ClearRelatedTableForMDL()
	return e.ddlExecutor.CreateTableWithInfo(e.Ctx(), schema, logTblInfo, nil)
}

func (e *DDLExec) executeDropMaterializedViewLog(s *ast.DropMaterializedViewLogStmt) error {
	if !e.Ctx().GetSessionVars().EnableMaterializedViewDemo {
		return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("materialized view demo is disabled")
	}

	schemaName := s.Table.Schema.L
	if schemaName == "" {
		schemaName = strings.ToLower(e.Ctx().GetSessionVars().CurrentDB)
	}
	if schemaName == "" {
		return errors.Trace(plannererrors.ErrNoDB)
	}
	schema := ast.NewCIStr(schemaName)

	is := e.Ctx().GetInfoSchema().(infoschema.InfoSchema)
	baseTbl, err := is.TableByName(context.Background(), schema, s.Table.Name)
	if err != nil {
		return errors.Trace(err)
	}
	baseInfo := baseTbl.Meta()

	logInfo, err := materializedview.FindLogTableInfoInSchema(is, schema, baseInfo.ID)
	if err != nil {
		return errors.Trace(err)
	}
	if logInfo == nil {
		return infoschema.ErrTableNotExists.GenWithStackByArgs(schema, s.Table.Name)
	}

	// Refuse to drop if any MV depends on this base table.
	hasMV, err := materializedview.HasDependentMaterializedView(is, baseInfo.ID)
	if err != nil {
		return errors.Trace(err)
	}
	if hasMV {
		return errors.New("cannot drop materialized view log: dependent materialized view exists")
	}

	e.Ctx().GetSessionVars().ClearRelatedTableForMDL()
	return e.ddlExecutor.DropTable(e.Ctx(), &ast.DropTableStmt{
		IfExists: false,
		Tables: []*ast.TableName{
			{Schema: schema, Name: ast.NewCIStr(logInfo.Name.O)},
		},
	})
}

func (e *DDLExec) executeCreateMaterializedView(ctx context.Context, s *ast.CreateMaterializedViewStmt) error {
	if !e.Ctx().GetSessionVars().EnableMaterializedViewDemo {
		return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("materialized view demo is disabled")
	}

	viewSchemaName := s.ViewName.Schema.L
	if viewSchemaName == "" {
		viewSchemaName = strings.ToLower(e.Ctx().GetSessionVars().CurrentDB)
	}
	if viewSchemaName == "" {
		return errors.Trace(plannererrors.ErrNoDB)
	}
	viewSchema := ast.NewCIStr(viewSchemaName)

	is := e.Ctx().GetInfoSchema().(infoschema.InfoSchema)
	if _, ok := is.SchemaByName(viewSchema); !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(viewSchema)
	}
	if _, err := is.TableByName(ctx, viewSchema, s.ViewName.Name); err == nil {
		return infoschema.ErrTableExists.GenWithStackByArgs(ast.Ident{Schema: viewSchema, Name: s.ViewName.Name})
	}

	ret := &core.PreprocessorReturn{}
	nodeW := resolve.NewNodeW(s.Select)
	if err := core.Preprocess(ctx, e.Ctx(), nodeW, core.WithPreprocessorReturn(ret)); err != nil {
		return errors.Trace(err)
	}
	if ret.IsStaleness {
		return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("materialized view query does not support staleness")
	}

	mvQuery, err := materializedview.ParseMVQuery(is, e.Ctx().GetSessionVars().CurrentDB, s.Select)
	if err != nil {
		return err
	}

	baseTbl, err := is.TableByName(ctx, mvQuery.BaseSchema, mvQuery.BaseTable)
	if err != nil {
		return err
	}
	baseInfo := baseTbl.Meta()

	logInfo, err := materializedview.FindLogTableInfoInSchema(is, mvQuery.BaseSchema, baseInfo.ID)
	if err != nil {
		return err
	}
	if logInfo == nil {
		return errors.New("materialized view base table does not have a materialized view log")
	}

	allowedColIDs := make(map[int64]struct{}, len(logInfo.MaterializedViewLogInfo.ColumnIDs))
	for _, id := range logInfo.MaterializedViewLogInfo.ColumnIDs {
		allowedColIDs[id] = struct{}{}
	}
	for id := range mvQuery.UsedBaseColumnIDs {
		if _, ok := allowedColIDs[id]; !ok {
			return errors.Errorf("materialized view query uses column %d which is not included in materialized view log", id)
		}
	}

	plan, _, err := planner.Optimize(ctx, e.Ctx(), nodeW, is)
	if err != nil {
		return errors.Trace(err)
	}
	if plan == nil || plan.Schema() == nil || len(plan.Schema().Columns) != len(mvQuery.SelectItems) {
		return errors.New("failed to derive materialized view schema from query")
	}

	existingColNames := make(map[string]struct{}, len(mvQuery.SelectItems))
	mvCols := make([]*model.ColumnInfo, 0, len(mvQuery.SelectItems))

	mvInfo := &model.MaterializedViewInfo{
		BaseTableID:            baseInfo.ID,
		LogTableID:             logInfo.ID,
		RefreshMode:            model.MaterializedViewRefreshMode(s.RefreshMode.String()),
		RefreshIntervalSeconds: s.RefreshIntervalSeconds,
	}

	groupByOffsets := make([]int, 0, len(mvQuery.GroupByBaseColumnIDs))
	sel := s.Select.(*ast.SelectStmt)

	for i, item := range mvQuery.SelectItems {
		derivedName, err := materializedview.DeriveMVColumnName(baseInfo, sel.Fields.Fields[i], item)
		if err != nil {
			return err
		}
		colName := materializedview.AllocUniqueColumnName(existingColNames, derivedName)

		colType := plan.Schema().Columns[i].RetType
		if colType == nil {
			return errors.New("failed to derive materialized view column type")
		}
		mvCols = append(mvCols, &model.ColumnInfo{
			ID:        int64(len(mvCols) + 1),
			Name:      colName,
			Offset:    len(mvCols),
			FieldType: *colType,
			State:     model.StatePublic,
			Version:   model.CurrLatestColumnInfoVersion,
		})

		if item.IsGroupBy {
			groupByOffsets = append(groupByOffsets, i)
			mvInfo.GroupByColumnIDs = append(mvInfo.GroupByColumnIDs, item.BaseColumnID)
		}
	}

	restoreFlag := format.RestoreStringSingleQuotes | format.RestoreKeyWordUppercase | format.RestoreNameBackQuotes
	var sb strings.Builder
	if err := s.Select.Restore(format.NewRestoreCtx(restoreFlag, &sb)); err != nil {
		return err
	}
	mvInfo.DefinitionSQL = sb.String()

	idxCols := make([]*model.IndexColumn, 0, len(groupByOffsets))
	for _, off := range groupByOffsets {
		idxCols = append(idxCols, &model.IndexColumn{
			Name:   mvCols[off].Name,
			Offset: off,
			Length: types.UnspecifiedLength,
		})
	}
	mvTblInfo := &model.TableInfo{
		Name:    s.ViewName.Name,
		Charset: baseInfo.Charset,
		Collate: baseInfo.Collate,
		Columns: mvCols,
		Indices: []*model.IndexInfo{
			{
				ID:            1,
				Name:          ast.NewCIStr("uniq_mv_group"),
				Table:         s.ViewName.Name,
				Columns:       idxCols,
				State:         model.StatePublic,
				BackfillState: model.BackfillStateInapplicable,
				Tp:            ast.IndexTypeBtree,
				Unique:        true,
			},
		},
		MaterializedViewInfo: mvInfo,
		Version:              model.CurrLatestTableInfoVersion,
		UpdateTS:             baseInfo.UpdateTS,
		MaxColumnID:          int64(len(mvCols)),
		MaxIndexID:           1,
	}

	e.Ctx().GetSessionVars().ClearRelatedTableForMDL()
	if err := e.ddlExecutor.CreateTableWithInfo(e.Ctx(), viewSchema, mvTblInfo, nil); err != nil {
		return err
	}

	// Initialize refresh info. W3 will turn this into a COMPLETE build and update last_refresh_tso atomically.
	latestIS := domain.GetDomain(e.Ctx()).InfoSchema()
	mvTbl, err := latestIS.TableByName(ctx, viewSchema, s.ViewName.Name)
	if err != nil {
		return err
	}
	internalCtx := kv.WithInternalSourceType(ctx, kv.InternalTxnDDL)
	sqlExec, ok := e.Ctx().(interface{ GetSQLExecutor() sqlexec.SQLExecutor })
	if !ok {
		return errors.New("session does not support internal SQL execution")
	}
	mvID := mvTbl.Meta().ID
	_, err = sqlexec.ExecSQL(internalCtx, sqlExec.GetSQLExecutor(),
		`INSERT INTO mysql.mv_refresh_info (mv_id, base_table_id, log_table_id, refresh_interval_seconds, next_run_time, last_refresh_tso, last_refresh_type, last_refresh_result, last_error)
		 VALUES (%?, %?, %?, %?, NULL, 0, 'COMPLETE', 'FAILED', 'not built yet')`,
		mvID, baseInfo.ID, logInfo.ID, s.RefreshIntervalSeconds,
	)
	if err == nil {
		return nil
	}

	// Best-effort cleanup to avoid leaving an orphan MV table without refresh info.
	_, _ = sqlexec.ExecSQL(internalCtx, sqlExec.GetSQLExecutor(), "DELETE FROM mysql.mv_refresh_info WHERE mv_id = %?", mvID)

	e.Ctx().GetSessionVars().ClearRelatedTableForMDL()
	cleanupErr := e.ddlExecutor.DropTable(e.Ctx(), &ast.DropTableStmt{
		IfExists: true,
		Tables: []*ast.TableName{
			{Schema: viewSchema, Name: s.ViewName.Name},
		},
	})
	if cleanupErr != nil {
		return errors.Annotatef(err, "failed to insert into mysql.mv_refresh_info; cleanup drop materialized view %s failed: %v", s.ViewName.Name.O, cleanupErr)
	}
	return errors.Annotatef(err, "failed to insert into mysql.mv_refresh_info; materialized view %s has been rolled back", s.ViewName.Name.O)
}

func (e *DDLExec) executeDropMaterializedView(s *ast.DropMaterializedViewStmt) error {
	if !e.Ctx().GetSessionVars().EnableMaterializedViewDemo {
		return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("materialized view demo is disabled")
	}

	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)

	schemaName := s.ViewName.Schema.L
	if schemaName == "" {
		schemaName = strings.ToLower(e.Ctx().GetSessionVars().CurrentDB)
	}
	if schemaName == "" {
		return errors.Trace(plannererrors.ErrNoDB)
	}
	schema := ast.NewCIStr(schemaName)

	is := e.Ctx().GetInfoSchema().(infoschema.InfoSchema)
	tbl, err := is.TableByName(ctx, schema, s.ViewName.Name)
	if err != nil {
		return errors.Trace(err)
	}
	if !tbl.Meta().IsMaterializedView() {
		return dbterror.ErrWrongObject.GenWithStackByArgs(schema.L, s.ViewName.Name.O, "MATERIALIZED VIEW")
	}
	mvID := tbl.Meta().ID

	e.Ctx().GetSessionVars().ClearRelatedTableForMDL()
	if err := e.ddlExecutor.DropTable(e.Ctx(), &ast.DropTableStmt{
		IfExists: false,
		Tables: []*ast.TableName{
			{Schema: schema, Name: s.ViewName.Name},
		},
	}); err != nil {
		return err
	}

	sqlExec, ok := e.Ctx().(interface{ GetSQLExecutor() sqlexec.SQLExecutor })
	if !ok {
		return errors.New("session does not support internal SQL execution")
	}
	_, err = sqlexec.ExecSQL(ctx, sqlExec.GetSQLExecutor(), "DELETE FROM mysql.mv_refresh_info WHERE mv_id = %?", mvID)
	if err != nil {
		// Don't fail the DROP MATERIALIZED VIEW since the MV table is already dropped.
		e.Ctx().GetSessionVars().StmtCtx.AppendWarning(errors.Annotatef(err, "failed to cleanup mysql.mv_refresh_info for materialized view %d", mvID))
		return nil
	}
	return nil
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
	is := e.Ctx().GetInfoSchema().(infoschema.InfoSchema)
	currentDB := strings.ToLower(e.Ctx().GetSessionVars().CurrentDB)

	for _, tn := range s.Tables {
		schemaName := tn.Schema.L
		if schemaName == "" {
			schemaName = currentDB
		}
		if schemaName == "" {
			return errors.Trace(plannererrors.ErrNoDB)
		}
		schema := ast.NewCIStr(schemaName)

		tbl, err := is.TableByName(context.Background(), schema, tn.Name)
		if err != nil {
			if infoschema.ErrTableNotExists.Equal(err) && s.IfExists {
				continue
			}
			return errors.Trace(err)
		}
		meta := tbl.Meta()

		if (meta.IsMaterializedView() || meta.IsMaterializedViewLog()) && !e.Ctx().GetSessionVars().InRestrictedSQL {
			obj := "materialized view"
			if meta.IsMaterializedViewLog() {
				obj = "materialized view log"
			}
			return errors.Errorf("drop %s %s is not supported now", obj, meta.Name.O)
		}
		if meta.IsView() || meta.IsSequence() || meta.IsMaterializedView() || meta.IsMaterializedViewLog() {
			continue
		}
		if err := e.checkMVDemoBaseTableDDLGate(is, meta.ID, fmt.Sprintf("drop table %s", meta.Name.O)); err != nil {
			return err
		}
	}

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

	is := e.Ctx().GetInfoSchema().(infoschema.InfoSchema)
	schemaName := s.Table.Schema.L
	if schemaName == "" {
		schemaName = strings.ToLower(e.Ctx().GetSessionVars().CurrentDB)
	}
	if schemaName == "" {
		return errors.Trace(plannererrors.ErrNoDB)
	}
	tbl, err := is.TableByName(ctx, ast.NewCIStr(schemaName), s.Table.Name)
	if err != nil {
		return errors.Trace(err)
	}
	meta := tbl.Meta()
	if meta.IsView() || meta.IsSequence() {
		return errors.Errorf("alter %s %s is not supported now", "table", meta.Name.O)
	}
	if (meta.IsMaterializedView() || meta.IsMaterializedViewLog()) && !e.Ctx().GetSessionVars().InRestrictedSQL {
		obj := "materialized view"
		if meta.IsMaterializedViewLog() {
			obj = "materialized view log"
		}
		return errors.Errorf("alter %s %s is not supported now", obj, meta.Name.O)
	}
	if !meta.IsMaterializedView() && !meta.IsMaterializedViewLog() {
		if err := e.checkMVDemoBaseTableDDLGate(is, meta.ID, fmt.Sprintf("alter table %s", meta.Name.O)); err != nil {
			return err
		}
	}

	return e.ddlExecutor.AlterTable(ctx, e.Ctx(), s)
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
