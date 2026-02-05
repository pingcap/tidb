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

package executor

import (
	"context"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	ptypes "github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
)

func restoreToCanonicalSQL(node ast.Node) (string, error) {
	var sb strings.Builder
	flags := format.DefaultRestoreFlags | format.RestoreStringWithoutCharset
	rctx := format.NewRestoreCtx(flags, &sb)
	if err := node.Restore(rctx); err != nil {
		return "", err
	}
	return sb.String(), nil
}

func (e *DDLExec) executeCreateMaterializedView(ctx context.Context, s *ast.CreateMaterializedViewStmt) error {
	is := e.Ctx().GetInfoSchema().(infoschema.InfoSchema)
	schemaName := s.ViewName.Schema
	if schemaName.O == "" {
		if e.Ctx().GetSessionVars().CurrentDB == "" {
			return errors.Trace(plannererrors.ErrNoDB)
		}
		schemaName = pmodel.NewCIStr(e.Ctx().GetSessionVars().CurrentDB)
		s.ViewName.Schema = schemaName
	}
	dbInfo, ok := is.SchemaByName(schemaName)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(schemaName.O)
	}

	sel, ok := s.Select.(*ast.SelectStmt)
	if !ok {
		return dbterror.ErrGeneralUnsupportedDDL.GenWithStack("CREATE MATERIALIZED VIEW only supports SELECT statement")
	}
	baseTableName, err := extractSingleTableNameFromSelect(sel)
	if err != nil {
		return err
	}
	if baseTableName.Schema.L == "" {
		baseTableName.Schema = schemaName
	}
	if baseTableName.Schema.L != schemaName.L {
		return dbterror.ErrGeneralUnsupportedDDL.GenWithStack("CREATE MATERIALIZED VIEW only supports base table in the same schema")
	}

	baseTable, err := is.TableByName(ctx, baseTableName.Schema, baseTableName.Name)
	if err != nil {
		return err
	}
	baseTableID := baseTable.Meta().ID

	mlogName := "$mlog$" + baseTable.Meta().Name.O
	mlogTable, err := is.TableByName(ctx, baseTableName.Schema, pmodel.NewCIStr(mlogName))
	if err != nil {
		if infoschema.ErrTableNotExists.Equal(err) {
			return errors.Errorf("materialized view log does not exist for base table %s.%s", baseTableName.Schema.O, baseTableName.Name.O)
		}
		return err
	}
	if mlogTable.Meta().MaterializedViewLog == nil || mlogTable.Meta().MaterializedViewLog.BaseTableID != baseTableID {
		return errors.Errorf("table %s.%s is not a materialized view log for base table %s.%s", baseTableName.Schema.O, mlogName, baseTableName.Schema.O, baseTableName.Name.O)
	}

	groupBySelectIdx, err := validateCreateMaterializedViewQuery(
		e.Ctx(),
		baseTableName,
		baseTable.Meta(),
		mlogTable.Meta().MaterializedViewLog.Columns,
		s.Select,
	)
	if err != nil {
		return err
	}

	selectSQL, err := restoreToCanonicalSQL(s.Select)
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

	refreshMethod, refreshStartWith, refreshNext, err := buildMViewRefreshMeta(s.Refresh)
	if err != nil {
		return err
	}
	mvTableInfo.MaterializedView = &model.MaterializedViewInfo{
		BaseTableIDs:     []int64{baseTableID},
		SQLContent:       selectSQL,
		RefreshMethod:    refreshMethod,
		RefreshStartWith: refreshStartWith,
		RefreshNext:      refreshNext,
	}

	if err := e.ddlExecutor.CreateTableWithInfo(e.Ctx(), schemaName, mvTableInfo, nil); err != nil {
		return err
	}
	return nil
}

func buildMViewRefreshMeta(refresh *ast.MViewRefreshClause) (method, startWith, next string, _ error) {
	const defaultNextSeconds = 300
	if refresh == nil {
		return "FAST", "NOW()", fmt.Sprintf("%d", defaultNextSeconds), nil
	}
	switch refresh.Method {
	case ast.MViewRefreshMethodNever:
		return "NEVER", "", "", nil
	case ast.MViewRefreshMethodFast:
		method = "FAST"
		startWith = "NOW()"
		if refresh.StartWith != nil {
			s, err := restoreExprToCanonicalSQL(refresh.StartWith)
			if err != nil {
				return "", "", "", err
			}
			startWith = s
		}
		next = fmt.Sprintf("%d", defaultNextSeconds)
		if refresh.Next != nil {
			s, err := restoreExprToCanonicalSQL(refresh.Next)
			if err != nil {
				return "", "", "", err
			}
			next = s
		}
		return method, startWith, next, nil
	default:
		return "", "", "", errors.New("unknown refresh method")
	}
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

	is := e.Ctx().GetInfoSchema().(infoschema.InfoSchema)
	if _, ok := is.SchemaByName(pmodel.NewCIStr(dbName)); !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(dbName)
	}
	tbl, err := is.TableByName(ctx, pmodel.NewCIStr(dbName), s.ViewName.Name)
	if err != nil {
		return err
	}
	if tbl.Meta().MaterializedView == nil {
		return dbterror.ErrWrongObject.GenWithStackByArgs(dbName, s.ViewName.Name, "MATERIALIZED VIEW")
	}

	dropStmt := &ast.DropTableStmt{Tables: []*ast.TableName{{Schema: pmodel.NewCIStr(dbName), Name: s.ViewName.Name}}}
	return e.ddlExecutor.DropTable(e.Ctx(), dropStmt)
}

func (e *DDLExec) executeDropMaterializedViewLog(ctx context.Context, s *ast.DropMaterializedViewLogStmt) error {
	is := e.Ctx().GetInfoSchema().(infoschema.InfoSchema)
	schemaName := s.Table.Schema
	if schemaName.O == "" {
		if e.Ctx().GetSessionVars().CurrentDB == "" {
			return errors.Trace(plannererrors.ErrNoDB)
		}
		schemaName = pmodel.NewCIStr(e.Ctx().GetSessionVars().CurrentDB)
	}
	if _, ok := is.SchemaByName(schemaName); !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(schemaName.O)
	}
	baseTable, err := is.TableByName(ctx, schemaName, s.Table.Name)
	if err != nil {
		return err
	}
	baseTableID := baseTable.Meta().ID

	// One base table : one MV LOG, name is fixed as $mlog$<base_table_name>.
	mlogName := "$mlog$" + baseTable.Meta().Name.O
	mlogTable, err := is.TableByName(ctx, schemaName, pmodel.NewCIStr(mlogName))
	if err != nil {
		return err
	}
	if mlogTable.Meta().MaterializedViewLog == nil || mlogTable.Meta().MaterializedViewLog.BaseTableID != baseTableID {
		return dbterror.ErrWrongObject.GenWithStackByArgs(schemaName.O, mlogName, "MATERIALIZED VIEW LOG")
	}

	depends, err := hasMaterializedViewDependsOnBaseTable(ctx, is, schemaName, baseTableID)
	if err != nil {
		return err
	}
	if depends {
		return errors.Errorf("cannot drop materialized view log on %s.%s: dependent materialized views exist", schemaName.O, s.Table.Name.O)
	}

	dropStmt := &ast.DropTableStmt{Tables: []*ast.TableName{{Schema: schemaName, Name: pmodel.NewCIStr(mlogName)}}}
	return e.ddlExecutor.DropTable(e.Ctx(), dropStmt)
}

func hasMaterializedViewDependsOnBaseTable(ctx context.Context, is infoschema.InfoSchema, schema pmodel.CIStr, baseTableID int64) (bool, error) {
	tblInfos, err := is.SchemaTableInfos(ctx, schema)
	if err != nil {
		return false, err
	}
	for _, tblInfo := range tblInfos {
		if tblInfo.MaterializedView == nil {
			continue
		}
		for _, id := range tblInfo.MaterializedView.BaseTableIDs {
			if id == baseTableID {
				return true, nil
			}
		}
	}
	return false, nil
}

func (e *DDLExec) executeAlterMaterializedView(ctx context.Context, s *ast.AlterMaterializedViewStmt) error {
	// Keep the execution atomic: if there are unsupported actions, fail fast before any DDL.
	for _, action := range s.Actions {
		switch action.Tp {
		case ast.AlterMaterializedViewActionComment:
		case ast.AlterMaterializedViewActionRefresh:
			return dbterror.ErrGeneralUnsupportedDDL.GenWithStack("ALTER MATERIALIZED VIEW ... REFRESH is not supported")
		default:
			return errors.Errorf("unknown alter materialized view action type: %d", action.Tp)
		}
	}

	is := e.Ctx().GetInfoSchema().(infoschema.InfoSchema)
	schemaName := s.ViewName.Schema
	if schemaName.O == "" {
		if e.Ctx().GetSessionVars().CurrentDB == "" {
			return errors.Trace(plannererrors.ErrNoDB)
		}
		schemaName = pmodel.NewCIStr(e.Ctx().GetSessionVars().CurrentDB)
		s.ViewName.Schema = schemaName
	}
	if _, ok := is.SchemaByName(schemaName); !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(schemaName.O)
	}
	tbl, err := is.TableByName(ctx, schemaName, s.ViewName.Name)
	if err != nil {
		return err
	}
	if tbl.Meta().MaterializedView == nil {
		return dbterror.ErrWrongObject.GenWithStackByArgs(schemaName.O, s.ViewName.Name, "MATERIALIZED VIEW")
	}

	for _, action := range s.Actions {
		switch action.Tp {
		case ast.AlterMaterializedViewActionComment:
			alterStmt := &ast.AlterTableStmt{
				Table: &ast.TableName{Schema: schemaName, Name: s.ViewName.Name},
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
		default:
			return errors.Errorf("unknown alter materialized view action type: %d", action.Tp)
		}
	}
	return nil
}

func (e *DDLExec) executeAlterMaterializedViewLog(ctx context.Context, s *ast.AlterMaterializedViewLogStmt) error {
	// Keep the execution atomic: if there are unsupported actions, fail fast before any DDL.
	for _, action := range s.Actions {
		if action.Tp == ast.AlterMaterializedViewLogActionPurge {
			return dbterror.ErrGeneralUnsupportedDDL.GenWithStack("ALTER MATERIALIZED VIEW LOG ... PURGE is not supported")
		}
	}

	is := e.Ctx().GetInfoSchema().(infoschema.InfoSchema)
	schemaName := s.Table.Schema
	if schemaName.O == "" {
		if e.Ctx().GetSessionVars().CurrentDB == "" {
			return errors.Trace(plannererrors.ErrNoDB)
		}
		schemaName = pmodel.NewCIStr(e.Ctx().GetSessionVars().CurrentDB)
	}
	if _, ok := is.SchemaByName(schemaName); !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(schemaName.O)
	}

	baseTable, err := is.TableByName(ctx, schemaName, s.Table.Name)
	if err != nil {
		return err
	}
	mlogName := "$mlog$" + baseTable.Meta().Name.O
	mlogTable, err := is.TableByName(ctx, schemaName, pmodel.NewCIStr(mlogName))
	if err != nil {
		return err
	}
	if mlogTable.Meta().MaterializedViewLog == nil || mlogTable.Meta().MaterializedViewLog.BaseTableID != baseTable.Meta().ID {
		return dbterror.ErrWrongObject.GenWithStackByArgs(schemaName.O, mlogName, "MATERIALIZED VIEW LOG")
	}

	for _, action := range s.Actions {
		switch action.Tp {
		case ast.AlterMaterializedViewLogActionTiFlashReplica:
			alterStmt := &ast.AlterTableStmt{
				Table: &ast.TableName{Schema: schemaName, Name: pmodel.NewCIStr(mlogName)},
				Specs: []*ast.AlterTableSpec{{
					Tp:             ast.AlterTableSetTiFlashReplica,
					TiFlashReplica: &ast.TiFlashReplicaSpec{Count: action.TiFlashReplicas},
				}},
			}
			if err := e.ddlExecutor.AlterTable(ctx, e.Ctx(), alterStmt); err != nil {
				return err
			}
		default:
			return errors.Errorf("unknown alter materialized view log action type: %d", action.Tp)
		}
	}
	return nil
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
				return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("CREATE MATERIALIZED VIEW does not support DISTINCT aggregate function")
			}
			if expr.F != ast.AggFuncCount && expr.F != ast.AggFuncSum && expr.F != ast.AggFuncMin && expr.F != ast.AggFuncMax {
				return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("unsupported aggregate function in CREATE MATERIALIZED VIEW")
			}
			switch expr.F {
			case ast.AggFuncCount:
				if len(expr.Args) != 1 {
					return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("count(*)/count(1) must have exactly one argument in CREATE MATERIALIZED VIEW")
				}
				if expr.Args[0] == nil {
					hasCountStarOrOne = true
					continue
				}
				if _, ok := expr.Args[0].(*ast.ColumnNameExpr); ok {
					return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("CREATE MATERIALIZED VIEW only supports count(*)/count(1)")
				}
				if !isCountStarOrOne(expr.Args[0]) {
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

// enforceMySQLDDLRestrictionsOnMViewOrMLog blocks unsupported DDLs on materialized view/log tables.
func enforceMySQLDDLRestrictionsOnMViewOrMLog(ctx context.Context, is infoschema.InfoSchema, dbName string, tblName pmodel.CIStr) error {
	if dbName == "" {
		return nil
	}
	tbl, err := is.TableByName(ctx, pmodel.NewCIStr(dbName), tblName)
	if err != nil {
		return nil
	}
	if tbl.Meta().MaterializedView != nil {
		return errors.Errorf("can't operate on table %s.%s: it is a materialized view", dbName, tblName.O)
	}
	if tbl.Meta().MaterializedViewLog != nil {
		return errors.Errorf("can't operate on table %s.%s: it is a materialized view log", dbName, tblName.O)
	}
	return nil
}

func (e *DDLExec) executeRefreshMaterializedView(context.Context, *ast.RefreshMaterializedViewStmt) error {
	return dbterror.ErrGeneralUnsupportedDDL.GenWithStack("REFRESH MATERIALIZED VIEW is not supported")
}
