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
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	ptypes "github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/sessiontxn/staleread"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/temptable"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
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

func (e *DDLExec) getLocalTemporaryTable(schema pmodel.CIStr, table pmodel.CIStr) (table.Table, bool) {
	tbl, err := e.Ctx().GetInfoSchema().(infoschema.InfoSchema).TableByName(context.Background(), schema, table)
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
	case *ast.CreateMaterializedViewStmt:
		err = e.executeCreateMaterializedView(ctx, x)
	case *ast.CreateMaterializedViewLogStmt:
		err = e.executeCreateMaterializedViewLog(ctx, x)
	case *ast.AlterMaterializedViewStmt:
		err = e.executeAlterMaterializedView(ctx, x)
	case *ast.AlterMaterializedViewLogStmt:
		err = e.executeAlterMaterializedViewLog(ctx, x)
	case *ast.DropMaterializedViewStmt:
		err = e.executeDropMaterializedView(ctx, x)
	case *ast.DropMaterializedViewLogStmt:
		err = e.executeDropMaterializedViewLog(ctx, x)
	case *ast.RefreshMaterializedViewStmt:
		err = e.executeRefreshMaterializedView(ctx, x)
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
	if _, exist := e.getLocalTemporaryTable(s.Table.Schema, s.Table.Name); exist {
		return e.tempTableDDL.TruncateLocalTemporaryTable(s.Table.Schema, s.Table.Name)
	}

	dbName := s.Table.Schema.O
	if dbName == "" {
		dbName = e.Ctx().GetSessionVars().CurrentDB
	}
	if dbName != "" {
		is := e.Ctx().GetInfoSchema().(infoschema.InfoSchema)
		tbl, err := is.TableByName(context.Background(), pmodel.NewCIStr(dbName), s.Table.Name)
		if err == nil {
			if tbl.Meta().MaterializedView != nil {
				return errors.Errorf("can't truncate table %s.%s: it is a materialized view", dbName, s.Table.Name.O)
			}
			if tbl.Meta().MaterializedViewLog != nil {
				hint := "DROP MATERIALIZED VIEW LOG"
				if base := materializedViewLogBaseTableIdent(context.Background(), is, tbl.Meta().MaterializedViewLog); base != "" {
					hint = "DROP MATERIALIZED VIEW LOG ON " + base
				}
				return errors.Errorf("can't truncate table %s.%s: it is a materialized view log, use %s", dbName, s.Table.Name.O, hint)
			}
		}
	}

	ident := ast.Ident{Schema: s.Table.Schema, Name: s.Table.Name}
	err := e.ddlExecutor.TruncateTable(e.Ctx(), ident)
	return err
}

func (e *DDLExec) executeRenameTable(s *ast.RenameTableStmt) error {
	for _, tables := range s.TableToTables {
		if _, ok := e.getLocalTemporaryTable(tables.OldTable.Schema, tables.OldTable.Name); ok {
			return dbterror.ErrUnsupportedLocalTempTableDDL.GenWithStackByArgs("RENAME TABLE")
		}

		dbName := tables.OldTable.Schema.O
		if dbName == "" {
			dbName = e.Ctx().GetSessionVars().CurrentDB
		}
		if dbName != "" {
			is := e.Ctx().GetInfoSchema().(infoschema.InfoSchema)
			tbl, err := is.TableByName(context.Background(), pmodel.NewCIStr(dbName), tables.OldTable.Name)
			if err == nil {
				if tbl.Meta().MaterializedView != nil {
					return errors.Errorf("can't rename table %s.%s: it is a materialized view", dbName, tables.OldTable.Name.O)
				}
				if tbl.Meta().MaterializedViewLog != nil {
					hint := "DROP MATERIALIZED VIEW LOG"
					if base := materializedViewLogBaseTableIdent(context.Background(), is, tbl.Meta().MaterializedViewLog); base != "" {
						hint = "DROP MATERIALIZED VIEW LOG ON " + base
					}
					return errors.Errorf("can't rename table %s.%s: it is a materialized view log, use %s", dbName, tables.OldTable.Name.O, hint)
				}
			}
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

	_, exists := e.getLocalTemporaryTable(s.Table.Schema, s.Table.Name)
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

func restoreToString(node ast.Node) (string, error) {
	var sb strings.Builder
	restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
	if err := node.Restore(restoreCtx); err != nil {
		return "", err
	}
	return sb.String(), nil
}

func restoreToCanonicalString(node ast.Node) (string, error) {
	// Keep consistent with ddl.BuildViewInfo.
	restoreFlag := format.RestoreStringSingleQuotes | format.RestoreKeyWordUppercase | format.RestoreNameBackQuotes
	var sb strings.Builder
	restoreCtx := format.NewRestoreCtx(restoreFlag, &sb)
	if err := node.Restore(restoreCtx); err != nil {
		return "", err
	}
	return sb.String(), nil
}

func materializedViewLogBaseTableIdent(ctx context.Context, is infoschema.InfoSchema, mlogInfo *model.MaterializedViewLogInfo) string {
	if mlogInfo == nil {
		return ""
	}
	baseTbl, ok := is.TableByID(ctx, mlogInfo.BaseTableID)
	if !ok {
		return ""
	}
	baseSchema, ok := is.SchemaByID(baseTbl.Meta().DBID)
	if !ok {
		return ""
	}
	return fmt.Sprintf("%s.%s", baseSchema.Name.O, baseTbl.Meta().Name.O)
}

func validateCreateMaterializedViewQuery(
	sctx sessionctx.Context,
	baseTableName *ast.TableName,
	baseTableInfo *model.TableInfo,
	mlogColumns []pmodel.CIStr,
	selectNode ast.ResultSetNode,
) (groupBySelectIdx []int, _ error) {
	sel, ok := selectNode.(*ast.SelectStmt)
	if !ok {
		return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("CREATE MATERIALIZED VIEW only supports SELECT statement")
	}

	// Stage-1: must be a single-table query with no join / no derived table.
	fromTbl, err := extractSingleTableNameFromSelect(sel)
	if err != nil {
		return nil, err
	}
	if fromTbl.Schema.L == "" {
		fromTbl.Schema = baseTableName.Schema
	}
	if fromTbl.Schema.L != baseTableName.Schema.L || fromTbl.Name.L != baseTableName.Name.L {
		return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("CREATE MATERIALIZED VIEW only supports a single base table")
	}

	if sel.With != nil {
		return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("CREATE MATERIALIZED VIEW does not support WITH clause")
	}
	if sel.Distinct {
		return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("CREATE MATERIALIZED VIEW does not support DISTINCT")
	}
	if sel.Having != nil {
		return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("CREATE MATERIALIZED VIEW does not support HAVING clause")
	}
	if sel.OrderBy != nil {
		return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("CREATE MATERIALIZED VIEW does not support ORDER BY clause")
	}
	if sel.Limit != nil {
		return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("CREATE MATERIALIZED VIEW does not support LIMIT clause")
	}
	if len(sel.WindowSpecs) > 0 {
		return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("CREATE MATERIALIZED VIEW does not support window functions")
	}

	if sel.GroupBy == nil || len(sel.GroupBy.Items) == 0 {
		return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("CREATE MATERIALIZED VIEW requires GROUP BY clause")
	}

	mlogColSet := make(map[string]struct{}, len(mlogColumns))
	for _, c := range mlogColumns {
		mlogColSet[c.L] = struct{}{}
	}

	groupBySet := make(map[string]struct{}, len(sel.GroupBy.Items))
	usedCols := make(map[string]struct{}, 8)

	for _, item := range sel.GroupBy.Items {
		colExpr, ok := item.Expr.(*ast.ColumnNameExpr)
		if !ok {
			return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("GROUP BY expression is not supported in CREATE MATERIALIZED VIEW")
		}
		colName := colExpr.Name.Name.L
		if _, exists := groupBySet[colName]; exists {
			return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("duplicate GROUP BY column is not supported in CREATE MATERIALIZED VIEW")
		}
		groupBySet[colName] = struct{}{}
		usedCols[colName] = struct{}{}
	}

	if sel.Where != nil {
		expr, err := expression.BuildSimpleExpr(
			sctx.GetExprCtx(),
			sel.Where,
			expression.WithTableInfo(baseTableName.Schema.O, baseTableInfo),
		)
		if err != nil {
			return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("CREATE MATERIALIZED VIEW WHERE clause is not supported")
		}
		if expression.CheckNonDeterministic(expr) {
			return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("CREATE MATERIALIZED VIEW WHERE clause must be deterministic")
		}
		for colName := range collectColumnNamesInExpr(sel.Where) {
			usedCols[colName] = struct{}{}
		}
	}

	selectColIdx := make(map[string]int, len(sel.Fields.Fields))
	hasCountStarOrOne := false
	for i, f := range sel.Fields.Fields {
		if f.WildCard != nil {
			return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("CREATE MATERIALIZED VIEW does not support wildcard select field")
		}
		switch expr := f.Expr.(type) {
		case *ast.ColumnNameExpr:
			colName := expr.Name.Name.L
			if _, ok := groupBySet[colName]; !ok {
				return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("non-aggregated column must appear in GROUP BY clause")
			}
			if _, exists := selectColIdx[colName]; exists {
				return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("duplicate GROUP BY column in SELECT list is not supported in CREATE MATERIALIZED VIEW")
			}
			selectColIdx[colName] = i
			usedCols[colName] = struct{}{}
		case *ast.AggregateFuncExpr:
			if expr.Distinct {
				return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("aggregate DISTINCT is not supported in CREATE MATERIALIZED VIEW")
			}
			if expr.Order != nil {
				return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("aggregate ORDER BY is not supported in CREATE MATERIALIZED VIEW")
			}
			switch strings.ToLower(expr.F) {
			case ast.AggFuncCount:
				if len(expr.Args) != 1 || !isCountStarOrOne(expr.Args[0]) {
					return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("CREATE MATERIALIZED VIEW only supports count(*)/count(1)")
				}
				hasCountStarOrOne = true
			case ast.AggFuncSum, ast.AggFuncMin, ast.AggFuncMax:
				if len(expr.Args) != 1 {
					return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("aggregate function must have exactly one argument in CREATE MATERIALIZED VIEW")
				}
				argCol, ok := expr.Args[0].(*ast.ColumnNameExpr)
				if !ok {
					return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("aggregate function only supports column argument in CREATE MATERIALIZED VIEW")
				}
				usedCols[argCol.Name.Name.L] = struct{}{}
			default:
				return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("unsupported aggregate function in CREATE MATERIALIZED VIEW")
			}
		default:
			return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("unsupported SELECT expression in CREATE MATERIALIZED VIEW")
		}
	}
	if !hasCountStarOrOne {
		return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("CREATE MATERIALIZED VIEW must contain count(*)/count(1)")
	}

	groupBySelectIdx = make([]int, 0, len(sel.GroupBy.Items))
	for _, item := range sel.GroupBy.Items {
		colExpr := item.Expr.(*ast.ColumnNameExpr)
		idx, ok := selectColIdx[colExpr.Name.Name.L]
		if !ok {
			return nil, errors.Errorf("GROUP BY column %s must appear in SELECT list", colExpr.Name.Name.O)
		}
		groupBySelectIdx = append(groupBySelectIdx, idx)
	}

	for colName := range usedCols {
		if _, ok := mlogColSet[colName]; !ok {
			return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack(fmt.Sprintf("materialized view log does not contain column %s", colName))
		}
	}

	return groupBySelectIdx, nil
}

func extractSingleTableNameFromSelect(sel *ast.SelectStmt) (*ast.TableName, error) {
	if sel.From == nil || sel.From.TableRefs == nil || sel.From.TableRefs.Left == nil || sel.From.TableRefs.Right != nil {
		return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("CREATE MATERIALIZED VIEW only supports a single base table")
	}
	ts, ok := sel.From.TableRefs.Left.(*ast.TableSource)
	if !ok {
		return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("CREATE MATERIALIZED VIEW only supports a single base table")
	}
	tbl, ok := ts.Source.(*ast.TableName)
	if !ok {
		return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("CREATE MATERIALIZED VIEW only supports a single base table")
	}
	return tbl, nil
}

func collectColumnNamesInExpr(expr ast.ExprNode) map[string]struct{} {
	collector := &columnNameCollector{cols: make(map[string]struct{}, 8)}
	expr.Accept(collector)
	return collector.cols
}

type columnNameCollector struct {
	cols map[string]struct{}
}

func (c *columnNameCollector) Enter(n ast.Node) (ast.Node, bool) {
	if x, ok := n.(*ast.ColumnNameExpr); ok {
		c.cols[x.Name.Name.L] = struct{}{}
	}
	return n, false
}

func (*columnNameCollector) Leave(n ast.Node) (ast.Node, bool) { return n, true }

func isCountStarOrOne(arg ast.ExprNode) bool {
	v, ok := arg.(*driver.ValueExpr)
	return ok && v.Kind() == types.KindInt64 && v.GetInt64() == 1
}

func findMaterializedViewLogByBaseTableID(ctx context.Context, is infoschema.InfoSchema, baseSchema pmodel.CIStr, baseTableID int64) (*model.TableInfo, error) {
	var found *model.TableInfo
	tblInfos, err := is.SchemaTableInfos(ctx, baseSchema)
	if err != nil {
		return nil, err
	}
	for _, tblInfo := range tblInfos {
		if tblInfo.MaterializedViewLog == nil || tblInfo.MaterializedViewLog.BaseTableID != baseTableID {
			continue
		}
		if found != nil {
			return nil, errors.Errorf("multiple materialized view logs exist for base table id %d", baseTableID)
		}
		found = tblInfo
	}
	return found, nil
}

func hasMaterializedViewDependsOnMLog(ctx context.Context, is infoschema.InfoSchema, schema pmodel.CIStr, mlogID int64) (bool, error) {
	tblInfos, err := is.SchemaTableInfos(ctx, schema)
	if err != nil {
		return false, err
	}
	for _, tblInfo := range tblInfos {
		if tblInfo.MaterializedView == nil {
			continue
		}
		if tblInfo.MaterializedView.MLogID == mlogID {
			return true, nil
		}
	}
	return false, nil
}

func (e *DDLExec) executeCreateMaterializedView(ctx context.Context, s *ast.CreateMaterializedViewStmt) error {
	dbName := s.ViewName.Schema.O
	if dbName == "" {
		dbName = e.Ctx().GetSessionVars().CurrentDB
		if dbName == "" {
			return plannererrors.ErrNoDB
		}
		s.ViewName.Schema = pmodel.NewCIStr(dbName)
	}
	is := e.Ctx().GetInfoSchema().(infoschema.InfoSchema)
	dbInfo, ok := is.SchemaByName(pmodel.NewCIStr(dbName))
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(dbName)
	}
	if s.TiFlashReplicas > 0 {
		return dbterror.ErrGeneralUnsupportedDDL.GenWithStack("CREATE MATERIALIZED VIEW does not support TIFLASH REPLICA yet")
	}

	// Validate MV query.
	ret := &core.PreprocessorReturn{}
	nodeW := resolve.NewNodeW(s.Select)
	if err := core.Preprocess(ctx, e.Ctx(), nodeW, core.WithPreprocessorReturn(ret)); err != nil {
		return errors.Trace(err)
	}
	if ret.IsStaleness {
		return exeerrors.ErrViewInvalid.GenWithStackByArgs(s.ViewName.Schema.L, s.ViewName.Name.L)
	}

	// Stage-1: only allow a single base table.
	tables := core.ExtractTableList(nodeW, false)
	if len(tables) != 1 {
		return dbterror.ErrGeneralUnsupportedDDL.GenWithStack("CREATE MATERIALIZED VIEW only supports a single base table")
	}
	baseTableName := tables[0]
	if baseTableName.Schema.L == "" {
		baseTableName.Schema = pmodel.NewCIStr(dbName)
	}
	if baseTableName.Schema.L != s.ViewName.Schema.L {
		return dbterror.ErrGeneralUnsupportedDDL.GenWithStack("CREATE MATERIALIZED VIEW requires the base table in the same schema")
	}

	dom := domain.GetDomain(e.Ctx())
	baseTable, err := dom.InfoSchema().TableByName(ctx, baseTableName.Schema, baseTableName.Name)
	if err != nil {
		return err
	}
	baseTableID := baseTable.Meta().ID

	mlogInfo, err := findMaterializedViewLogByBaseTableID(ctx, dom.InfoSchema(), baseTableName.Schema, baseTableID)
	if err != nil {
		return err
	}
	if mlogInfo == nil {
		return errors.Errorf("materialized view log on %s.%s does not exist", baseTableName.Schema.O, baseTableName.Name.O)
	}

	groupBySelectIdx, err := validateCreateMaterializedViewQuery(
		e.Ctx(),
		baseTableName,
		baseTable.Meta(),
		mlogInfo.MaterializedViewLog.Columns,
		s.Select,
	)
	if err != nil {
		return err
	}

	selectSQL, err := restoreToCanonicalString(s.Select)
	if err != nil {
		return err
	}

	exec := e.Ctx().GetRestrictedSQLExecutor()
	kctx := kv.WithInternalSourceType(ctx, kv.InternalTxnDDL)
	/* #nosec G202: selectSQL is restored from AST (single statement, no user-provided placeholders). */
	_, resultFields, err := exec.ExecRestrictedSQL(kctx, nil, "SELECT * FROM ("+selectSQL+") AS `tidb_mv_query` LIMIT 0")
	if err != nil {
		return err
	}
	if len(resultFields) != len(s.Cols) {
		return errors.Errorf("materialized view column count %d does not match query output %d", len(s.Cols), len(resultFields))
	}

	colDefs := make([]*ast.ColumnDef, 0, len(resultFields))
	for i, rf := range resultFields {
		ft := rf.Column.FieldType
		colDefs = append(colDefs, &ast.ColumnDef{
			Name: &ast.ColumnName{Name: s.Cols[i]},
			Tp:   &ft,
		})
	}

	keys := make([]*ast.IndexPartSpecification, 0, len(groupBySelectIdx))
	for _, idx := range groupBySelectIdx {
		keys = append(keys, &ast.IndexPartSpecification{
			Column: &ast.ColumnName{Name: s.Cols[idx]},
			Length: ptypes.UnspecifiedLength,
		})
	}
	constraints := []*ast.Constraint{{Tp: ast.ConstraintUniq, Keys: keys}}

	createTableStmt := &ast.CreateTableStmt{
		Table:       s.ViewName,
		Cols:        colDefs,
		Constraints: constraints,
	}
	mvTableInfo, err := ddl.BuildTableInfoWithStmt(
		ddl.NewMetaBuildContextWithSctx(e.Ctx()),
		createTableStmt,
		dbInfo.Charset,
		dbInfo.Collate,
		dbInfo.PlacementPolicyRef,
	)
	if err != nil {
		return err
	}
	mvTableInfo.Comment = s.Comment
	mvTableInfo.MaterializedView = &model.MaterializedViewInfo{
		BaseTableID: baseTableID,
		MLogID:      mlogInfo.ID,
		SQLContent:  selectSQL,
	}

	if err := e.ddlExecutor.CreateTableWithInfo(e.Ctx(), pmodel.NewCIStr(dbName), mvTableInfo, nil); err != nil {
		return err
	}

	mvTable, err := dom.InfoSchema().TableByName(ctx, pmodel.NewCIStr(dbName), s.ViewName.Name)
	if err != nil {
		return err
	}
	mviewID := mvTable.Meta().ID

	refresh := s.Refresh
	if refresh == nil {
		refresh = &ast.MViewRefreshClause{Method: ast.MViewRefreshMethodFast}
	}
	refreshMethod := refresh.Method.String()
	var startWith, next any
	if refresh.Method == ast.MViewRefreshMethodFast {
		const defaultNextSeconds = 300
		startWithStr := "NOW()"
		if refresh.StartWith != nil {
			startWithStr, err = restoreToString(refresh.StartWith)
			if err != nil {
				return err
			}
		}
		nextStr := fmt.Sprintf("%d", defaultNextSeconds)
		if refresh.Next != nil {
			nextStr, err = restoreToString(refresh.Next)
			if err != nil {
				return err
			}
		}
		startWith, next = startWithStr, nextStr
	}

	_, _, err = exec.ExecRestrictedSQL(kctx, nil,
		`INSERT INTO mysql.tidb_mview_refresh (
				MVIEW_ID, REFRESH_METHOD, START_WITH, NEXT,
				LAST_REFRESH_RESULT, LAST_REFRESH_TYPE, LAST_REFRESH_TIME, LAST_REFRESH_READ_TSO, LAST_REFRESH_FAILED_REASON
			) VALUES (%?, %?, %?, %?, NULL, NULL, NULL, NULL, NULL)`,
		mviewID, refreshMethod, startWith, next)
	if err != nil {
		dropStmt := &ast.DropTableStmt{Tables: []*ast.TableName{{Schema: pmodel.NewCIStr(dbName), Name: s.ViewName.Name}}}
		if dropErr := e.ddlExecutor.DropTable(e.Ctx(), dropStmt); dropErr != nil {
			return errors.Annotatef(err, "failed to insert mview refresh metadata, and failed to rollback materialized view table: %v", dropErr)
		}
	}
	return err
}

func (e *DDLExec) executeCreateMaterializedViewLog(ctx context.Context, s *ast.CreateMaterializedViewLogStmt) error {
	dbName := s.Table.Schema.O
	if dbName == "" {
		dbName = e.Ctx().GetSessionVars().CurrentDB
		if dbName == "" {
			return plannererrors.ErrNoDB
		}
		s.Table.Schema = pmodel.NewCIStr(dbName)
	}

	dom := domain.GetDomain(e.Ctx())
	is := dom.InfoSchema()
	dbInfo, ok := is.SchemaByName(pmodel.NewCIStr(dbName))
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(dbName)
	}

	baseTable, err := is.TableByName(ctx, pmodel.NewCIStr(dbName), s.Table.Name)
	if err != nil {
		return err
	}
	baseTableID := baseTable.Meta().ID

	for _, col := range s.Cols {
		if col.L == "dml_type" || col.L == "old_new" {
			return errors.Errorf("column name %s is reserved for materialized view log", col.O)
		}
	}
	colMap := make(map[string]*model.ColumnInfo, len(baseTable.Meta().Columns))
	for _, col := range baseTable.Meta().Columns {
		colMap[col.Name.L] = col
	}

	existingMLog, err := findMaterializedViewLogByBaseTableID(ctx, is, pmodel.NewCIStr(dbName), baseTableID)
	if err != nil {
		return err
	}
	if existingMLog != nil {
		return infoschema.ErrTableExists.GenWithStackByArgs("materialized view log on " + fmt.Sprintf("%s.%s", dbName, s.Table.Name.O))
	}

	mlogName := "$mlog$" + s.Table.Name.O
	if len(mlogName) > mysql.MaxTableNameLength {
		return errors.Errorf("materialized view log table name too long: %s", mlogName)
	}
	_, err = is.TableByName(ctx, pmodel.NewCIStr(dbName), pmodel.NewCIStr(mlogName))
	if err == nil {
		return infoschema.ErrTableExists.GenWithStackByArgs(ast.Ident{Schema: pmodel.NewCIStr(dbName), Name: pmodel.NewCIStr(mlogName)})
	}
	if !infoschema.ErrTableNotExists.Equal(err) {
		return err
	}

	colDefs := make([]*ast.ColumnDef, 0, len(s.Cols)+2)
	colNames := make([]pmodel.CIStr, 0, len(s.Cols))
	for _, c := range s.Cols {
		baseCol := colMap[c.L]
		if baseCol == nil {
			return infoschema.ErrColumnNotExists.GenWithStackByArgs(c.O, s.Table.Name)
		}
		ft := baseCol.FieldType
		colDefs = append(colDefs, &ast.ColumnDef{
			Name: &ast.ColumnName{Name: c},
			Tp:   &ft,
		})
		colNames = append(colNames, c)
	}
	metaCols := []struct {
		name string
		ft   byte
		flen int
	}{
		{name: "dml_type", ft: mysql.TypeVarchar, flen: 1},
		// old_new uses -1/1 to represent deleted/added, to simplify later delta computation.
		{name: "old_new", ft: mysql.TypeTiny, flen: 2},
	}
	for _, metaCol := range metaCols {
		ft := ptypes.NewFieldType(metaCol.ft)
		ft.SetFlen(metaCol.flen)
		ft.SetFlag(mysql.NotNullFlag)
		colDefs = append(colDefs, &ast.ColumnDef{
			Name: &ast.ColumnName{Name: pmodel.NewCIStr(metaCol.name)},
			Tp:   ft,
		})
	}

	createTableStmt := &ast.CreateTableStmt{
		Table: &ast.TableName{Schema: pmodel.NewCIStr(dbName), Name: pmodel.NewCIStr(mlogName)},
		Cols:  colDefs,
	}
	mlogTableInfo, err := ddl.BuildTableInfoWithStmt(
		ddl.NewMetaBuildContextWithSctx(e.Ctx()),
		createTableStmt,
		dbInfo.Charset,
		dbInfo.Collate,
		dbInfo.PlacementPolicyRef,
	)
	if err != nil {
		return err
	}
	mlogTableInfo.MaterializedViewLog = &model.MaterializedViewLogInfo{
		BaseTableID: baseTableID,
		Columns:     colNames,
	}
	if err := e.ddlExecutor.CreateTableWithInfo(e.Ctx(), pmodel.NewCIStr(dbName), mlogTableInfo, nil); err != nil {
		return err
	}

	mlogTable, err := dom.InfoSchema().TableByName(ctx, pmodel.NewCIStr(dbName), pmodel.NewCIStr(mlogName))
	if err != nil {
		return err
	}
	mlogID := mlogTable.Meta().ID

	purgeMethod := "IMMEDIATE"
	purgeStartExpr := "NOW()"
	purgeIntervalExpr := "0"
	if s.Purge != nil {
		if s.Purge.Immediate {
			purgeMethod = "IMMEDIATE"
			purgeStartExpr = "NOW()"
			purgeIntervalExpr = "0"
		} else {
			purgeMethod = "DEFERRED"
			nextExpr, err := restoreToString(s.Purge.Next)
			if err != nil {
				return err
			}
			purgeIntervalExpr = fmt.Sprintf("CAST((%s) AS SIGNED)", nextExpr)
			if s.Purge.StartWith != nil {
				startExpr, err := restoreToString(s.Purge.StartWith)
				if err != nil {
					return err
				}
				purgeStartExpr = fmt.Sprintf("CAST((%s) AS DATETIME)", startExpr)
			} else {
				purgeStartExpr = fmt.Sprintf("DATE_ADD(NOW(), INTERVAL %s SECOND)", purgeIntervalExpr)
			}
		}
	}

	exec := e.Ctx().GetRestrictedSQLExecutor()
	kctx := kv.WithInternalSourceType(ctx, kv.InternalTxnDDL)
	/* #nosec G202: purgeStartExpr/purgeIntervalExpr are restored from AST (single expression), not raw SQL. */
	sql := fmt.Sprintf(
		`INSERT INTO mysql.tidb_mlog_purge (
				MLOG_ID, PURGE_METHOD, PURGE_START, PURGE_INTERVAL,
				LAST_PURGE_TIME, LAST_PURGE_ROWS, LAST_PURGE_DURATION
			) VALUES (
				%%?, %%?, %s, %s, NULL, NULL, NULL
			)`,
		purgeStartExpr, purgeIntervalExpr,
	)
	_, _, err = exec.ExecRestrictedSQL(kctx, nil, sql, mlogID, purgeMethod)
	if err != nil {
		dropStmt := &ast.DropTableStmt{Tables: []*ast.TableName{{Schema: pmodel.NewCIStr(dbName), Name: pmodel.NewCIStr(mlogName)}}}
		if dropErr := e.ddlExecutor.DropTable(e.Ctx(), dropStmt); dropErr != nil {
			return errors.Annotatef(err, "failed to insert mlog purge metadata, and failed to rollback materialized view log table: %v", dropErr)
		}
	}
	return err
}

func (e *DDLExec) executeAlterMaterializedView(ctx context.Context, s *ast.AlterMaterializedViewStmt) error {
	dbName := s.ViewName.Schema.O
	if dbName == "" {
		dbName = e.Ctx().GetSessionVars().CurrentDB
		if dbName == "" {
			return plannererrors.ErrNoDB
		}
		s.ViewName.Schema = pmodel.NewCIStr(dbName)
	}
	dom := domain.GetDomain(e.Ctx())
	if _, ok := dom.InfoSchema().SchemaByName(pmodel.NewCIStr(dbName)); !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(dbName)
	}
	tbl, err := dom.InfoSchema().TableByName(ctx, pmodel.NewCIStr(dbName), s.ViewName.Name)
	if err != nil {
		return err
	}
	if tbl.Meta().MaterializedView == nil {
		return dbterror.ErrWrongObject.GenWithStackByArgs(dbName, s.ViewName.Name, "MATERIALIZED VIEW")
	}

	mviewID := tbl.Meta().ID
	exec := e.Ctx().GetRestrictedSQLExecutor()
	kctx := kv.WithInternalSourceType(ctx, kv.InternalTxnDDL)

	for _, action := range s.Actions {
		switch action.Tp {
		case ast.AlterMaterializedViewActionComment:
			alterStmt := &ast.AlterTableStmt{
				Table: &ast.TableName{Schema: pmodel.NewCIStr(dbName), Name: s.ViewName.Name},
				Specs: []*ast.AlterTableSpec{{
					Tp: ast.AlterTableOption,
					Options: []*ast.TableOption{{
						Tp:       ast.TableOptionComment,
						StrValue: action.Comment,
					}},
				}},
			}
			if err := e.ddlExecutor.AlterTable(ctx, e.Ctx(), alterStmt); err != nil {
				return err
			}
		case ast.AlterMaterializedViewActionRefresh:
			if action.Refresh == nil || (action.Refresh.StartWith == nil && action.Refresh.Next == nil) {
				_, _, err := exec.ExecRestrictedSQL(kctx, nil,
					"INSERT INTO mysql.tidb_mview_refresh (MVIEW_ID, REFRESH_METHOD, START_WITH, NEXT) VALUES (%?, 'REFRESH FAST', NULL, NULL) "+
						"ON DUPLICATE KEY UPDATE START_WITH=NULL, NEXT=NULL",
					mviewID,
				)
				if err != nil {
					return err
				}
				continue
			}

			sets := make([]string, 0, 2)
			var startWith, next any
			if action.Refresh.StartWith != nil {
				startWithStr, err := restoreToString(action.Refresh.StartWith)
				if err != nil {
					return err
				}
				sets = append(sets, "START_WITH=VALUES(START_WITH)")
				startWith = startWithStr
			}
			if action.Refresh.Next != nil {
				nextStr, err := restoreToString(action.Refresh.Next)
				if err != nil {
					return err
				}
				sets = append(sets, "NEXT=VALUES(NEXT)")
				next = nextStr
			}
			if len(sets) == 0 {
				continue
			}
			/* #nosec G202: SQL string concatenation */
			sql := "INSERT INTO mysql.tidb_mview_refresh (MVIEW_ID, REFRESH_METHOD, START_WITH, NEXT) VALUES (%?, 'REFRESH FAST', %?, %?) " +
				"ON DUPLICATE KEY UPDATE " + strings.Join(sets, ", ")
			_, _, err := exec.ExecRestrictedSQL(kctx, nil, sql, mviewID, startWith, next)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *DDLExec) executeAlterMaterializedViewLog(ctx context.Context, s *ast.AlterMaterializedViewLogStmt) error {
	dbName := s.Table.Schema.O
	if dbName == "" {
		dbName = e.Ctx().GetSessionVars().CurrentDB
		if dbName == "" {
			return plannererrors.ErrNoDB
		}
		s.Table.Schema = pmodel.NewCIStr(dbName)
	}

	dom := domain.GetDomain(e.Ctx())
	is := dom.InfoSchema()
	baseTable, err := is.TableByName(ctx, pmodel.NewCIStr(dbName), s.Table.Name)
	if err != nil {
		return err
	}
	baseTableID := baseTable.Meta().ID

	mlogInfo, err := findMaterializedViewLogByBaseTableID(ctx, is, pmodel.NewCIStr(dbName), baseTableID)
	if err != nil {
		return err
	}
	if mlogInfo == nil {
		return infoschema.ErrTableNotExists.GenWithStackByArgs("materialized view log on " + fmt.Sprintf("%s.%s", dbName, s.Table.Name.O))
	}

	exec := e.Ctx().GetRestrictedSQLExecutor()
	kctx := kv.WithInternalSourceType(ctx, kv.InternalTxnDDL)
	for _, action := range s.Actions {
		if action.Purge == nil {
			continue
		}
		purgeMethod := "IMMEDIATE"
		purgeStartExpr := "NOW()"
		purgeIntervalExpr := "0"
		if !action.Purge.Immediate {
			purgeMethod = "DEFERRED"
			nextExpr, err := restoreToString(action.Purge.Next)
			if err != nil {
				return err
			}
			purgeIntervalExpr = fmt.Sprintf("CAST((%s) AS SIGNED)", nextExpr)
			if action.Purge.StartWith != nil {
				startExpr, err := restoreToString(action.Purge.StartWith)
				if err != nil {
					return err
				}
				purgeStartExpr = fmt.Sprintf("CAST((%s) AS DATETIME)", startExpr)
			} else {
				purgeStartExpr = fmt.Sprintf("DATE_ADD(NOW(), INTERVAL %s SECOND)", purgeIntervalExpr)
			}
		}
		/* #nosec G202: purgeStartExpr/purgeIntervalExpr are restored from AST (single expression), not raw SQL. */
		sql := fmt.Sprintf(
			"INSERT INTO mysql.tidb_mlog_purge (MLOG_ID, PURGE_METHOD, PURGE_START, PURGE_INTERVAL, LAST_PURGE_TIME, LAST_PURGE_ROWS, LAST_PURGE_DURATION) "+
				"VALUES (%%?, %%?, %s, %s, NULL, NULL, NULL) "+
				"ON DUPLICATE KEY UPDATE PURGE_METHOD=VALUES(PURGE_METHOD), PURGE_START=VALUES(PURGE_START), PURGE_INTERVAL=VALUES(PURGE_INTERVAL)",
			purgeStartExpr, purgeIntervalExpr,
		)
		_, _, err := exec.ExecRestrictedSQL(kctx, nil, sql, mlogInfo.ID, purgeMethod)
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *DDLExec) executeDropMaterializedView(ctx context.Context, s *ast.DropMaterializedViewStmt) error {
	dbName := s.ViewName.Schema.O
	if dbName == "" {
		dbName = e.Ctx().GetSessionVars().CurrentDB
		if dbName == "" {
			return plannererrors.ErrNoDB
		}
		s.ViewName.Schema = pmodel.NewCIStr(dbName)
	}

	dom := domain.GetDomain(e.Ctx())
	if _, ok := dom.InfoSchema().SchemaByName(pmodel.NewCIStr(dbName)); !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(dbName)
	}
	tbl, err := dom.InfoSchema().TableByName(ctx, pmodel.NewCIStr(dbName), s.ViewName.Name)
	if err != nil {
		return err
	}
	if tbl.Meta().MaterializedView == nil {
		return dbterror.ErrWrongObject.GenWithStackByArgs(dbName, s.ViewName.Name, "MATERIALIZED VIEW")
	}
	mviewID := tbl.Meta().ID

	dropStmt := &ast.DropTableStmt{Tables: []*ast.TableName{{Schema: pmodel.NewCIStr(dbName), Name: s.ViewName.Name}}}
	if err := e.ddlExecutor.DropTable(e.Ctx(), dropStmt); err != nil {
		return err
	}

	exec := e.Ctx().GetRestrictedSQLExecutor()
	kctx := kv.WithInternalSourceType(ctx, kv.InternalTxnDDL)
	if _, _, err := exec.ExecRestrictedSQL(kctx, nil, "DELETE FROM mysql.tidb_mview_refresh WHERE MVIEW_ID=%?", mviewID); err != nil {
		logutil.BgLogger().Warn("failed to cleanup mysql.tidb_mview_refresh for materialized view", zap.Error(err))
	}
	if _, _, err := exec.ExecRestrictedSQL(kctx, nil, "DELETE FROM mysql.tidb_mview_refresh_hist WHERE MVIEW_ID=%?", mviewID); err != nil {
		logutil.BgLogger().Warn("failed to cleanup mysql.tidb_mview_refresh_hist for materialized view", zap.Error(err))
	}
	return nil
}

func (e *DDLExec) executeDropMaterializedViewLog(ctx context.Context, s *ast.DropMaterializedViewLogStmt) error {
	dbName := s.Table.Schema.O
	if dbName == "" {
		dbName = e.Ctx().GetSessionVars().CurrentDB
		if dbName == "" {
			return plannererrors.ErrNoDB
		}
		s.Table.Schema = pmodel.NewCIStr(dbName)
	}

	dom := domain.GetDomain(e.Ctx())
	is := dom.InfoSchema()
	baseTable, err := is.TableByName(ctx, pmodel.NewCIStr(dbName), s.Table.Name)
	if err != nil {
		return err
	}
	baseTableID := baseTable.Meta().ID

	mlogInfo, err := findMaterializedViewLogByBaseTableID(ctx, is, pmodel.NewCIStr(dbName), baseTableID)
	if err != nil {
		return err
	}
	if mlogInfo == nil {
		return infoschema.ErrTableNotExists.GenWithStackByArgs("materialized view log on " + fmt.Sprintf("%s.%s", dbName, s.Table.Name.O))
	}
	if depends, err := hasMaterializedViewDependsOnMLog(ctx, is, pmodel.NewCIStr(dbName), mlogInfo.ID); err != nil {
		return err
	} else if depends {
		return errors.Errorf("can't drop materialized view log on %s.%s: dependent materialized views exist", dbName, s.Table.Name.O)
	}

	mlogSchema, ok := infoschema.SchemaByTable(is, mlogInfo)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(dbName)
	}
	dropStmt := &ast.DropTableStmt{Tables: []*ast.TableName{{Schema: mlogSchema.Name, Name: mlogInfo.Name}}}
	if err := e.ddlExecutor.DropTable(e.Ctx(), dropStmt); err != nil {
		return err
	}

	exec := e.Ctx().GetRestrictedSQLExecutor()
	kctx := kv.WithInternalSourceType(ctx, kv.InternalTxnDDL)
	if _, _, err := exec.ExecRestrictedSQL(kctx, nil, "DELETE FROM mysql.tidb_mlog_purge WHERE MLOG_ID=%?", mlogInfo.ID); err != nil {
		logutil.BgLogger().Warn("failed to cleanup mysql.tidb_mlog_purge for materialized view log", zap.Error(err))
	}
	if _, _, err := exec.ExecRestrictedSQL(kctx, nil, "DELETE FROM mysql.tidb_mlog_purge_hist WHERE MLOG_ID=%?", mlogInfo.ID); err != nil {
		logutil.BgLogger().Warn("failed to cleanup mysql.tidb_mlog_purge_hist for materialized view log", zap.Error(err))
	}
	return nil
}

func (*DDLExec) executeRefreshMaterializedView(context.Context, *ast.RefreshMaterializedViewStmt) error {
	return dbterror.ErrGeneralUnsupportedDDL.GenWithStack("REFRESH MATERIALIZED VIEW is not supported")
}

func (e *DDLExec) executeCreateIndex(s *ast.CreateIndexStmt) error {
	if _, ok := e.getLocalTemporaryTable(s.Table.Schema, s.Table.Name); ok {
		return dbterror.ErrUnsupportedLocalTempTableDDL.GenWithStackByArgs("CREATE INDEX")
	}

	dbName := s.Table.Schema.O
	if dbName == "" {
		dbName = e.Ctx().GetSessionVars().CurrentDB
	}
	if dbName != "" {
		is := domain.GetDomain(e.Ctx()).InfoSchema()
		tbl, err := is.TableByName(context.Background(), pmodel.NewCIStr(dbName), s.Table.Name)
		if err == nil {
			if tbl.Meta().MaterializedView != nil {
				return errors.Errorf("can't create index on table %s.%s: it is a materialized view", dbName, s.Table.Name.O)
			}
			if tbl.Meta().MaterializedViewLog != nil {
				hint := "DROP MATERIALIZED VIEW LOG"
				if base := materializedViewLogBaseTableIdent(context.Background(), is, tbl.Meta().MaterializedViewLog); base != "" {
					hint = "DROP MATERIALIZED VIEW LOG ON " + base
				}
				return errors.Errorf("can't create index on table %s.%s: it is a materialized view log, use %s", dbName, s.Table.Name.O, hint)
			}
		}
	}

	return e.ddlExecutor.CreateIndex(e.Ctx(), s)
}

func (e *DDLExec) executeDropDatabase(s *ast.DropDatabaseStmt) error {
	dbName := s.Name

	// Protect important system table from been dropped by a mistake.
	// I can hardly find a case that a user really need to do this.
	if dbName.L == "mysql" {
		return errors.New("Drop 'mysql' database is forbidden")
	}

	// It's mandatory to drop materialized view logs before dropping a base table.
	// Dropping a database can drop base tables too, so we guard it here as well.
	dom := domain.GetDomain(e.Ctx())
	is := dom.InfoSchema()
	if _, ok := is.SchemaByName(dbName); ok {
		tblInfos, err := is.SchemaTableInfos(context.Background(), dbName)
		if err != nil {
			return err
		}
		for _, tblInfo := range tblInfos {
			if tblInfo.MaterializedViewLog != nil {
				return errors.Errorf("can't drop database %s: materialized view log exists, drop it first", dbName.O)
			}
		}
	}

	err := e.ddlExecutor.DropSchema(e.Ctx(), s)
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
	// It's mandatory to drop materialized view logs before dropping a base table.
	// See spec: "It is mandatory that dropping materialized view log before dropping a base table."
	dom := domain.GetDomain(e.Ctx())
	is := dom.InfoSchema()
	for _, tableName := range s.Tables {
		dbName := tableName.Schema.O
		if dbName == "" {
			dbName = e.Ctx().GetSessionVars().CurrentDB
		}
		if dbName == "" {
			continue
		}
		tbl, err := is.TableByName(context.Background(), pmodel.NewCIStr(dbName), tableName.Name)
		if err != nil {
			// If it's a DROP TABLE IF EXISTS and the table doesn't exist, skip it.
			if s.IfExists && infoschema.ErrTableNotExists.Equal(err) {
				continue
			}
			continue
		}
		if tbl.Meta().MaterializedView != nil {
			return errors.Errorf(
				"can't drop table %s.%s: it is a materialized view, use DROP MATERIALIZED VIEW %s.%s",
				dbName, tableName.Name.O,
				dbName, tableName.Name.O,
			)
		}
		if tbl.Meta().MaterializedViewLog != nil {
			hint := "DROP MATERIALIZED VIEW LOG"
			if base := materializedViewLogBaseTableIdent(context.Background(), is, tbl.Meta().MaterializedViewLog); base != "" {
				hint = "DROP MATERIALIZED VIEW LOG ON " + base
			}
			return errors.Errorf("can't drop table %s.%s: it is a materialized view log, use %s", dbName, tableName.Name.O, hint)
		}

		mlogInfo, err := findMaterializedViewLogByBaseTableID(context.Background(), is, pmodel.NewCIStr(dbName), tbl.Meta().ID)
		if err != nil {
			return err
		}
		if mlogInfo != nil {
			return errors.Errorf("can't drop table %s.%s: materialized view log exists, drop it first", dbName, tableName.Name.O)
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
	if _, ok := e.getLocalTemporaryTable(s.Table.Schema, s.Table.Name); ok {
		return dbterror.ErrUnsupportedLocalTempTableDDL.GenWithStackByArgs("DROP INDEX")
	}

	dbName := s.Table.Schema.O
	if dbName == "" {
		dbName = e.Ctx().GetSessionVars().CurrentDB
	}
	if dbName != "" {
		is := domain.GetDomain(e.Ctx()).InfoSchema()
		tbl, err := is.TableByName(context.Background(), pmodel.NewCIStr(dbName), s.Table.Name)
		if err == nil {
			if tbl.Meta().MaterializedView != nil {
				return errors.Errorf("can't drop index on table %s.%s: it is a materialized view", dbName, s.Table.Name.O)
			}
			if tbl.Meta().MaterializedViewLog != nil {
				hint := "DROP MATERIALIZED VIEW LOG"
				if base := materializedViewLogBaseTableIdent(context.Background(), is, tbl.Meta().MaterializedViewLog); base != "" {
					hint = "DROP MATERIALIZED VIEW LOG ON " + base
				}
				return errors.Errorf("can't drop index on table %s.%s: it is a materialized view log, use %s", dbName, s.Table.Name.O, hint)
			}
		}
	}

	return e.ddlExecutor.DropIndex(e.Ctx(), s)
}

func (e *DDLExec) executeAlterTable(ctx context.Context, s *ast.AlterTableStmt) error {
	if _, ok := e.getLocalTemporaryTable(s.Table.Schema, s.Table.Name); ok {
		return dbterror.ErrUnsupportedLocalTempTableDDL.GenWithStackByArgs("ALTER TABLE")
	}

	dbName := s.Table.Schema.O
	if dbName == "" {
		dbName = e.Ctx().GetSessionVars().CurrentDB
	}
	if dbName != "" {
		is := domain.GetDomain(e.Ctx()).InfoSchema()
		tbl, err := is.TableByName(ctx, pmodel.NewCIStr(dbName), s.Table.Name)
		if err == nil {
			if tbl.Meta().MaterializedView != nil {
				return errors.Errorf("can't alter table %s.%s: it is a materialized view", dbName, s.Table.Name.O)
			}
			if tbl.Meta().MaterializedViewLog != nil {
				hint := "DROP MATERIALIZED VIEW LOG"
				if base := materializedViewLogBaseTableIdent(ctx, is, tbl.Meta().MaterializedViewLog); base != "" {
					hint = "DROP MATERIALIZED VIEW LOG ON " + base
				}
				return errors.Errorf("can't alter table %s.%s: it is a materialized view log, use %s", dbName, s.Table.Name.O, hint)
			}
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
		tblInfo.Name = pmodel.NewCIStr(s.NewName)
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
		dbName = pmodel.NewCIStr(s.NewName)
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

func (e *DDLExec) getRecoverDBByName(schemaName pmodel.CIStr) (recoverSchemaInfo *model.RecoverSchemaInfo, err error) {
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
		if _, ok := e.getLocalTemporaryTable(tb.Table.Schema, tb.Table.Name); ok {
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
		if _, ok := e.getLocalTemporaryTable(tb.Schema, tb.Name); ok {
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
	if !variable.EnableResourceControl.Load() && !e.Ctx().GetSessionVars().InRestrictedSQL {
		return infoschema.ErrResourceGroupSupportDisabled
	}
	return e.ddlExecutor.AddResourceGroup(e.Ctx(), s)
}

func (e *DDLExec) executeAlterResourceGroup(s *ast.AlterResourceGroupStmt) error {
	if !variable.EnableResourceControl.Load() && !e.Ctx().GetSessionVars().InRestrictedSQL {
		return infoschema.ErrResourceGroupSupportDisabled
	}
	return e.ddlExecutor.AlterResourceGroup(e.Ctx(), s)
}

func (e *DDLExec) executeDropResourceGroup(s *ast.DropResourceGroupStmt) error {
	if !variable.EnableResourceControl.Load() && !e.Ctx().GetSessionVars().InRestrictedSQL {
		return infoschema.ErrResourceGroupSupportDisabled
	}
	return e.ddlExecutor.DropResourceGroup(e.Ctx(), s)
}
