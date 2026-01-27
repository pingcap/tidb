package executor

import (
	"context"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/materializedview"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/sessionctx"
)

const mvHasDeleteColumn = "__mv_has_delete"

type mvColumnMap struct {
	BaseCol ast.CIStr
	MVCol   ast.CIStr
}

type mvAggMap struct {
	BaseArgCol ast.CIStr
	MVCol      ast.CIStr
}

type mvExprQualifierRewriter struct {
	qualifier ast.CIStr
}

func (v *mvExprQualifierRewriter) Enter(n ast.Node) (ast.Node, bool) {
	colExpr, ok := n.(*ast.ColumnNameExpr)
	if !ok || colExpr.Name == nil {
		return n, false
	}
	colExpr.Name.Schema = ast.NewCIStr("")
	if v.qualifier.L != "" {
		colExpr.Name.Table = v.qualifier
	}
	return n, false
}

func (*mvExprQualifierRewriter) Leave(n ast.Node) (ast.Node, bool) {
	return n, true
}

func mvRestoreExprWithQualifier(expr ast.ExprNode, qualifier ast.CIStr) (string, error) {
	if expr == nil {
		return "", nil
	}
	// Rewrite column qualifiers in-place. This is OK because we only use the AST in this refresh call.
	expr.Accept(&mvExprQualifierRewriter{qualifier: qualifier})

	restoreFlag := format.RestoreStringSingleQuotes | format.RestoreKeyWordUppercase | format.RestoreNameBackQuotes
	var sb strings.Builder
	if err := expr.Restore(format.NewRestoreCtx(restoreFlag, &sb)); err != nil {
		return "", err
	}
	return sb.String(), nil
}

func mvParseDefinitionSQL(is infoschema.InfoSchema, currentDB, definitionSQL string) (*ast.SelectStmt, *materializedview.MVQuery, error) {
	p := parser.New()
	st, err := p.ParseOneStmt(definitionSQL, "", "")
	if err != nil {
		return nil, nil, err
	}
	sel, ok := st.(*ast.SelectStmt)
	if !ok {
		return nil, nil, errors.New("materialized view definition is not a SELECT statement")
	}
	mvQuery, err := materializedview.ParseMVQuery(is, currentDB, sel)
	if err != nil {
		return nil, nil, err
	}
	return sel, mvQuery, nil
}

func mvGetTableNameByID(is infoschema.InfoSchema, tableID int64) (schema ast.CIStr, table ast.CIStr, err error) {
	tbl, ok := is.TableByID(context.Background(), tableID)
	if !ok || tbl.Meta() == nil {
		return ast.CIStr{}, ast.CIStr{}, errors.Errorf("table %d not found in infoschema", tableID)
	}
	dbInfo, ok := infoschema.SchemaByTable(is, tbl.Meta())
	if !ok || dbInfo == nil {
		return ast.CIStr{}, ast.CIStr{}, errors.Errorf("schema for table %d not found in infoschema", tableID)
	}
	return dbInfo.Name, tbl.Meta().Name, nil
}

func mvBuildDeltaQuery(
	logFullName string,
	qualifier ast.CIStr,
	whereSQL string,
	groupCols []mvColumnMap,
	countCol ast.CIStr,
	sumCols []mvAggMap,
	minCols []mvAggMap,
	maxCols []mvAggMap,
) (string, error) {
	qHasDelete := mvQuoteIdent(ast.NewCIStr(mvHasDeleteColumn))

	qualified := func(col ast.CIStr) string {
		return mvQuoteIdent(qualifier) + "." + mvQuoteIdent(col)
	}

	groupByExprs := make([]string, 0, len(groupCols))
	for _, c := range groupCols {
		groupByExprs = append(groupByExprs, qualified(c.BaseCol))
	}
	if len(groupByExprs) == 0 {
		return "", errors.New("materialized view GROUP BY is empty")
	}

	// Build SELECT list for the INSERT (new) part.
	insSelect := make([]string, 0, len(groupCols)+1+len(sumCols)+len(minCols)+len(maxCols)+1)
	for _, c := range groupCols {
		insSelect = append(insSelect, fmt.Sprintf("%s AS %s", qualified(c.BaseCol), mvQuoteIdent(c.MVCol)))
	}
	insSelect = append(insSelect, fmt.Sprintf("COUNT(1) AS %s", mvQuoteIdent(countCol)))
	for _, a := range sumCols {
		insSelect = append(insSelect, fmt.Sprintf("SUM(%s) AS %s", qualified(a.BaseArgCol), mvQuoteIdent(a.MVCol)))
	}
	for _, a := range minCols {
		insSelect = append(insSelect, fmt.Sprintf("MIN(%s) AS %s", qualified(a.BaseArgCol), mvQuoteIdent(a.MVCol)))
	}
	for _, a := range maxCols {
		insSelect = append(insSelect, fmt.Sprintf("MAX(%s) AS %s", qualified(a.BaseArgCol), mvQuoteIdent(a.MVCol)))
	}
	insSelect = append(insSelect, fmt.Sprintf("0 AS %s", qHasDelete))

	// Build SELECT list for the DELETE (old) part.
	delSelect := make([]string, 0, len(insSelect))
	for _, c := range groupCols {
		delSelect = append(delSelect, fmt.Sprintf("%s AS %s", qualified(c.BaseCol), mvQuoteIdent(c.MVCol)))
	}
	delSelect = append(delSelect, fmt.Sprintf("-COUNT(1) AS %s", mvQuoteIdent(countCol)))
	for _, a := range sumCols {
		delSelect = append(delSelect, fmt.Sprintf("-SUM(%s) AS %s", qualified(a.BaseArgCol), mvQuoteIdent(a.MVCol)))
	}
	for _, a := range minCols {
		delSelect = append(delSelect, fmt.Sprintf("NULL AS %s", mvQuoteIdent(a.MVCol)))
	}
	for _, a := range maxCols {
		delSelect = append(delSelect, fmt.Sprintf("NULL AS %s", mvQuoteIdent(a.MVCol)))
	}
	delSelect = append(delSelect, fmt.Sprintf("1 AS %s", qHasDelete))

	whereParts := func(oldNew string) string {
		parts := []string{
			fmt.Sprintf("%s.%s > %%? AND %s.%s < %%?",
				mvQuoteIdent(qualifier), mvQuoteIdent(model.ExtraCommitTsName),
				mvQuoteIdent(qualifier), mvQuoteIdent(model.ExtraCommitTsName),
			),
			fmt.Sprintf("%s.%s = '%s'", mvQuoteIdent(qualifier), mvQuoteIdent(ast.NewCIStr(materializedview.MVLogColumnOldNew)), oldNew),
		}
		if whereSQL != "" {
			parts = append(parts, "("+whereSQL+")")
		}
		return strings.Join(parts, " AND ")
	}

	insSQL := fmt.Sprintf(
		"SELECT %s FROM %s AS %s WHERE %s GROUP BY %s",
		strings.Join(insSelect, ", "),
		logFullName, mvQuoteIdent(qualifier),
		whereParts(materializedview.MVLogOldNewNew),
		strings.Join(groupByExprs, ", "),
	)
	delSQL := fmt.Sprintf(
		"SELECT %s FROM %s AS %s WHERE %s GROUP BY %s",
		strings.Join(delSelect, ", "),
		logFullName, mvQuoteIdent(qualifier),
		whereParts(materializedview.MVLogOldNewOld),
		strings.Join(groupByExprs, ", "),
	)

	// Outer aggregation over the UNION ALL of INSERT/DELETE parts.
	outerSelect := make([]string, 0, len(groupCols)+1+len(sumCols)+len(minCols)+len(maxCols)+1)
	for _, c := range groupCols {
		outerSelect = append(outerSelect, fmt.Sprintf("u.%s AS %s", mvQuoteIdent(c.MVCol), mvQuoteIdent(c.MVCol)))
	}
	outerSelect = append(outerSelect, fmt.Sprintf("SUM(u.%s) AS %s", mvQuoteIdent(countCol), mvQuoteIdent(countCol)))
	for _, a := range sumCols {
		outerSelect = append(outerSelect, fmt.Sprintf("SUM(u.%s) AS %s", mvQuoteIdent(a.MVCol), mvQuoteIdent(a.MVCol)))
	}
	for _, a := range minCols {
		outerSelect = append(outerSelect, fmt.Sprintf("MIN(u.%s) AS %s", mvQuoteIdent(a.MVCol), mvQuoteIdent(a.MVCol)))
	}
	for _, a := range maxCols {
		outerSelect = append(outerSelect, fmt.Sprintf("MAX(u.%s) AS %s", mvQuoteIdent(a.MVCol), mvQuoteIdent(a.MVCol)))
	}
	outerSelect = append(outerSelect, fmt.Sprintf("MAX(u.%s) AS %s", qHasDelete, qHasDelete))

	groupByAliases := make([]string, 0, len(groupCols))
	for _, c := range groupCols {
		groupByAliases = append(groupByAliases, "u."+mvQuoteIdent(c.MVCol))
	}

	return fmt.Sprintf(
		"(SELECT %s FROM (%s UNION ALL %s) AS u GROUP BY %s)",
		strings.Join(outerSelect, ", "),
		insSQL, delSQL,
		strings.Join(groupByAliases, ", "),
	), nil
}

func mvFastRefresh(
	ctx context.Context,
	sctx sessionctx.Context,
	mvSchema, mvName ast.CIStr,
	mvID int64,
	mvInfo *model.MaterializedViewInfo,
) (retErr error) {
	if mvInfo == nil {
		return errors.New("materialized view info is nil")
	}
	is := sctx.GetInfoSchema().(infoschema.InfoSchema)

	internalCtx := kv.WithInternalSourceType(ctx, kv.InternalTxnAdmin)
	if err := mvBeginPessimistic(internalCtx, sctx); err != nil {
		return err
	}
	defer func() {
		if retErr != nil {
			_ = mvRollback(internalCtx, sctx)
		}
	}()

	txn, err := sctx.Txn(true)
	if err != nil {
		return err
	}
	readTS := txn.StartTS()

	// Refresh mutex + fetch last_refresh_tso.
	rows, err := mvQueryInternal(internalCtx, sctx,
		"SELECT last_refresh_tso FROM mysql.mv_refresh_info WHERE mv_id = %? FOR UPDATE",
		mvID,
	)
	if err != nil {
		return err
	}
	if len(rows) != 1 {
		return errors.Errorf("mv_refresh_info row not found for mv_id=%d", mvID)
	}
	lastRefreshTSO := rows[0].GetUint64(0)
	if lastRefreshTSO == 0 {
		return errors.New("materialized view is not built yet")
	}

	if mvInfo.DefinitionSQL == "" {
		return errors.New("materialized view definition SQL is empty")
	}
	sel, mvQuery, err := mvParseDefinitionSQL(is, sctx.GetSessionVars().CurrentDB, mvInfo.DefinitionSQL)
	if err != nil {
		return err
	}
	qualifier := mvQuery.BaseAlias
	if qualifier.L == "" {
		qualifier = mvQuery.BaseTable
	}
	whereSQL, err := mvRestoreExprWithQualifier(sel.Where, qualifier)
	if err != nil {
		return err
	}

	// Resolve names.
	logSchema, logName, err := mvGetTableNameByID(is, mvInfo.LogTableID)
	if err != nil {
		return err
	}
	logFullName := mvQuoteFullName(logSchema, logName)

	baseSchema, baseName, err := mvGetTableNameByID(is, mvInfo.BaseTableID)
	if err != nil {
		return err
	}
	baseFullName := mvQuoteFullName(baseSchema, baseName)

	mvTbl, ok := is.TableByID(context.Background(), mvID)
	if !ok || mvTbl.Meta() == nil {
		return errors.Errorf("materialized view table %d not found in infoschema", mvID)
	}
	mvCols := mvTbl.Meta().Columns
	if len(mvCols) != len(mvQuery.SelectItems) {
		return errors.New("materialized view schema mismatch with definition")
	}

	// Build mappings from definition to MV columns.
	var (
		groupCols []mvColumnMap
		countCol  ast.CIStr
		sumCols   []mvAggMap
		minCols   []mvAggMap
		maxCols   []mvAggMap
	)

	baseTbl, ok := is.TableByID(context.Background(), mvInfo.BaseTableID)
	if !ok || baseTbl.Meta() == nil {
		return errors.Errorf("base table %d not found in infoschema", mvInfo.BaseTableID)
	}
	baseInfo := baseTbl.Meta()
	if baseInfo == nil {
		return errors.Errorf("base table %d meta not found", mvInfo.BaseTableID)
	}

	for i, item := range mvQuery.SelectItems {
		mvColName := mvCols[i].Name
		if item.IsGroupBy {
			colInfo := model.FindColumnInfoByID(baseInfo.Columns, item.BaseColumnID)
			if colInfo == nil {
				return errors.Errorf("base column %d not found", item.BaseColumnID)
			}
			groupCols = append(groupCols, mvColumnMap{BaseCol: colInfo.Name, MVCol: mvColName})
			continue
		}
		switch item.AggKind {
		case materializedview.MVAggKindCountStar:
			countCol = mvColName
		case materializedview.MVAggKindSum:
			colInfo := model.FindColumnInfoByID(baseInfo.Columns, item.ArgBaseColumnID)
			if colInfo == nil {
				return errors.Errorf("base column %d not found", item.ArgBaseColumnID)
			}
			sumCols = append(sumCols, mvAggMap{BaseArgCol: colInfo.Name, MVCol: mvColName})
		case materializedview.MVAggKindMin:
			colInfo := model.FindColumnInfoByID(baseInfo.Columns, item.ArgBaseColumnID)
			if colInfo == nil {
				return errors.Errorf("base column %d not found", item.ArgBaseColumnID)
			}
			minCols = append(minCols, mvAggMap{BaseArgCol: colInfo.Name, MVCol: mvColName})
		case materializedview.MVAggKindMax:
			colInfo := model.FindColumnInfoByID(baseInfo.Columns, item.ArgBaseColumnID)
			if colInfo == nil {
				return errors.Errorf("base column %d not found", item.ArgBaseColumnID)
			}
			maxCols = append(maxCols, mvAggMap{BaseArgCol: colInfo.Name, MVCol: mvColName})
		default:
			return errors.New("invalid aggregate kind")
		}
	}
	if countCol.L == "" {
		return errors.New("materialized view count(*) column not found")
	}

	deltaSQL, err := mvBuildDeltaQuery(logFullName, qualifier, whereSQL, groupCols, countCol, sumCols, minCols, maxCols)
	if err != nil {
		return err
	}
	deltaArgs := []any{lastRefreshTSO, readTS, lastRefreshTSO, readTS}

	failpoint.Inject("mvFastRefreshFailAfterDeltaQuery", func(_ failpoint.Value) {
		failpoint.Return(errors.New("failpoint: mvFastRefreshFailAfterDeltaQuery"))
	})

	mvFullName := mvQuoteFullName(mvSchema, mvName)

	// Build join condition between MV table and delta/recompute derived tables.
	joinOn := make([]string, 0, len(groupCols))
	for _, c := range groupCols {
		col := mvQuoteIdent(c.MVCol)
		joinOn = append(joinOn, fmt.Sprintf("mv.%s <=> d.%s", col, col))
	}
	joinCondMvDelta := strings.Join(joinOn, " AND ")

	// 1) Apply count/sum and insert-only min/max updates to existing groups.
	setClauses := make([]string, 0, 1+len(sumCols)+len(minCols)+len(maxCols))
	qCnt := mvQuoteIdent(countCol)
	setClauses = append(setClauses, fmt.Sprintf("mv.%s = mv.%s + d.%s", qCnt, qCnt, qCnt))
	for _, a := range sumCols {
		q := mvQuoteIdent(a.MVCol)
		setClauses = append(setClauses, fmt.Sprintf(
			"mv.%s = CASE WHEN d.%s IS NULL THEN mv.%s WHEN mv.%s IS NULL THEN d.%s ELSE mv.%s + d.%s END",
			q, q, q, q, q, q, q,
		))
	}
	qHasDelete := mvQuoteIdent(ast.NewCIStr(mvHasDeleteColumn))
	for _, a := range minCols {
		q := mvQuoteIdent(a.MVCol)
		setClauses = append(setClauses, fmt.Sprintf(
			`mv.%s = CASE
			           WHEN d.%s = 0 THEN
			             CASE
			               WHEN d.%s IS NULL THEN mv.%s
			               WHEN mv.%s IS NULL THEN d.%s
			               ELSE LEAST(mv.%s, d.%s)
			             END
			           ELSE mv.%s
			         END`,
			q, qHasDelete, q, q, q, q, q, q, q,
		))
	}
	for _, a := range maxCols {
		q := mvQuoteIdent(a.MVCol)
		setClauses = append(setClauses, fmt.Sprintf(
			`mv.%s = CASE
			           WHEN d.%s = 0 THEN
			             CASE
			               WHEN d.%s IS NULL THEN mv.%s
			               WHEN mv.%s IS NULL THEN d.%s
			               ELSE GREATEST(mv.%s, d.%s)
			             END
			           ELSE mv.%s
			         END`,
			q, qHasDelete, q, q, q, q, q, q, q,
		))
	}
	updateSQL := fmt.Sprintf("UPDATE %s AS mv JOIN %s AS d ON %s SET %s", mvFullName, deltaSQL, joinCondMvDelta, strings.Join(setClauses, ", "))
	if err := mvExecInternal(internalCtx, sctx, updateSQL, deltaArgs...); err != nil {
		return err
	}

	// 2) Insert new groups (matched by NULL-safe equality via LEFT JOIN).
	mvColList := make([]string, 0, len(mvCols))
	dColList := make([]string, 0, len(mvCols))
	for _, c := range mvCols {
		q := mvQuoteIdent(c.Name)
		mvColList = append(mvColList, q)
		dColList = append(dColList, "d."+q)
	}
	insertJoinOn := make([]string, 0, len(groupCols))
	for _, c := range groupCols {
		col := mvQuoteIdent(c.MVCol)
		insertJoinOn = append(insertJoinOn, fmt.Sprintf("mv.%s <=> d.%s", col, col))
	}
	insertSQL := fmt.Sprintf(
		"INSERT INTO %s (%s) SELECT %s FROM %s AS d LEFT JOIN %s AS mv ON %s WHERE mv._tidb_rowid IS NULL AND d.%s > 0",
		mvFullName,
		strings.Join(mvColList, ", "),
		strings.Join(dColList, ", "),
		deltaSQL,
		mvFullName,
		strings.Join(insertJoinOn, " AND "),
		qCnt,
	)
	if err := mvExecInternal(internalCtx, sctx, insertSQL, deltaArgs...); err != nil {
		return err
	}

	// 3) Remove empty groups.
	if err := mvExecInternal(internalCtx, sctx, "DELETE FROM "+mvFullName+" WHERE "+qCnt+" = 0"); err != nil {
		return err
	}

	failpoint.Inject("mvFastRefreshFailAfterApply", func(_ failpoint.Value) {
		failpoint.Return(errors.New("failpoint: mvFastRefreshFailAfterApply"))
	})

	// 4) Recompute MIN/MAX for groups with delete.
	if len(minCols) > 0 || len(maxCols) > 0 {
		groupSel := make([]string, 0, len(groupCols))
		groupGB := make([]string, 0, len(groupCols))
		for _, c := range groupCols {
			q := mvQuoteIdent(c.MVCol)
			groupSel = append(groupSel, "d."+q+" AS "+q)
			groupGB = append(groupGB, "g."+q)
		}
		groupListSQL := fmt.Sprintf("SELECT %s FROM %s AS d WHERE d.%s = 1", strings.Join(groupSel, ", "), deltaSQL, qHasDelete)

		baseJoinOn := make([]string, 0, len(groupCols))
		for _, c := range groupCols {
			baseJoinOn = append(baseJoinOn, fmt.Sprintf(
				"%s.%s <=> g.%s",
				mvQuoteIdent(qualifier), mvQuoteIdent(c.BaseCol),
				mvQuoteIdent(c.MVCol),
			))
		}

		recomputeSelect := make([]string, 0, len(groupCols)+len(minCols)+len(maxCols))
		for _, c := range groupCols {
			q := mvQuoteIdent(c.MVCol)
			recomputeSelect = append(recomputeSelect, "g."+q+" AS "+q)
		}
		for _, a := range minCols {
			recomputeSelect = append(recomputeSelect, fmt.Sprintf("MIN(%s.%s) AS %s", mvQuoteIdent(qualifier), mvQuoteIdent(a.BaseArgCol), mvQuoteIdent(a.MVCol)))
		}
		for _, a := range maxCols {
			recomputeSelect = append(recomputeSelect, fmt.Sprintf("MAX(%s.%s) AS %s", mvQuoteIdent(qualifier), mvQuoteIdent(a.BaseArgCol), mvQuoteIdent(a.MVCol)))
		}

		recomputeWhere := ""
		if whereSQL != "" {
			recomputeWhere = " WHERE " + whereSQL
		}

		recomputeSQL := fmt.Sprintf(
			"(SELECT %s FROM %s AS %s JOIN (%s) AS g ON %s%s GROUP BY %s)",
			strings.Join(recomputeSelect, ", "),
			baseFullName, mvQuoteIdent(qualifier),
			groupListSQL,
			strings.Join(baseJoinOn, " AND "),
			recomputeWhere,
			strings.Join(groupGB, ", "),
		)

		updateJoin := make([]string, 0, len(groupCols))
		for _, c := range groupCols {
			col := mvQuoteIdent(c.MVCol)
			updateJoin = append(updateJoin, fmt.Sprintf("mv.%s <=> r.%s", col, col))
		}

		recomputeSet := make([]string, 0, len(minCols)+len(maxCols))
		for _, a := range minCols {
			q := mvQuoteIdent(a.MVCol)
			recomputeSet = append(recomputeSet, fmt.Sprintf("mv.%s = r.%s", q, q))
		}
		for _, a := range maxCols {
			q := mvQuoteIdent(a.MVCol)
			recomputeSet = append(recomputeSet, fmt.Sprintf("mv.%s = r.%s", q, q))
		}
		updateMinMaxSQL := fmt.Sprintf("UPDATE %s AS mv JOIN %s AS r ON %s SET %s", mvFullName, recomputeSQL, strings.Join(updateJoin, " AND "), strings.Join(recomputeSet, ", "))
		if err := mvExecInternal(internalCtx, sctx, updateMinMaxSQL, deltaArgs...); err != nil {
			return err
		}
	}

	// 5) Update mv_refresh_info and commit.
	failpoint.Inject("mvFastRefreshFailBeforeRefreshInfoUpdate", func(_ failpoint.Value) {
		failpoint.Return(errors.New("failpoint: mvFastRefreshFailBeforeRefreshInfoUpdate"))
	})
	if err := mvExecInternal(internalCtx, sctx,
		`UPDATE mysql.mv_refresh_info
		   SET last_refresh_tso = %?,
		       last_refresh_type = 'FAST',
		       last_refresh_result = 'SUCCESS',
		       last_refresh_time = NOW(),
		       next_run_time = DATE_ADD(NOW(), INTERVAL refresh_interval_seconds SECOND),
		       last_error = NULL
		 WHERE mv_id = %?`,
		readTS, mvID,
	); err != nil {
		return err
	}
	return mvCommit(internalCtx, sctx)
}
