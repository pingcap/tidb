package executor

import (
	"context"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/materializedview"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/sessionctx"
)

const mvDemoHasDeleteColumn = "__mv_has_delete"

type mvDemoColumnMap struct {
	BaseCol ast.CIStr
	MVCol   ast.CIStr
}

type mvDemoAggMap struct {
	BaseArgCol ast.CIStr
	MVCol      ast.CIStr
}

type mvDemoExprQualifierRewriter struct {
	qualifier ast.CIStr
}

func (v *mvDemoExprQualifierRewriter) Enter(n ast.Node) (ast.Node, bool) {
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

func (*mvDemoExprQualifierRewriter) Leave(n ast.Node) (ast.Node, bool) {
	return n, true
}

func mvDemoRestoreExprWithQualifier(expr ast.ExprNode, qualifier ast.CIStr) (string, error) {
	if expr == nil {
		return "", nil
	}
	// Rewrite column qualifiers in-place. This is OK because we only use the AST in this refresh call.
	expr.Accept(&mvDemoExprQualifierRewriter{qualifier: qualifier})

	restoreFlag := format.RestoreStringSingleQuotes | format.RestoreKeyWordUppercase | format.RestoreNameBackQuotes
	var sb strings.Builder
	if err := expr.Restore(format.NewRestoreCtx(restoreFlag, &sb)); err != nil {
		return "", err
	}
	return sb.String(), nil
}

func mvDemoParseDefinitionSQL(is infoschema.InfoSchema, currentDB, definitionSQL string) (*ast.SelectStmt, *materializedview.MVQuery, error) {
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

func mvDemoGetTableNameByID(is infoschema.InfoSchema, tableID int64) (schema ast.CIStr, table ast.CIStr, err error) {
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

func mvDemoBuildDeltaQuery(
	logFullName string,
	qualifier ast.CIStr,
	whereSQL string,
	groupCols []mvDemoColumnMap,
	countCol ast.CIStr,
	sumCols []mvDemoAggMap,
	minCols []mvDemoAggMap,
	maxCols []mvDemoAggMap,
) (string, error) {
	qHasDelete := mvDemoQuoteIdent(ast.NewCIStr(mvDemoHasDeleteColumn))

	qualified := func(col ast.CIStr) string {
		return mvDemoQuoteIdent(qualifier) + "." + mvDemoQuoteIdent(col)
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
		insSelect = append(insSelect, fmt.Sprintf("%s AS %s", qualified(c.BaseCol), mvDemoQuoteIdent(c.MVCol)))
	}
	insSelect = append(insSelect, fmt.Sprintf("COUNT(1) AS %s", mvDemoQuoteIdent(countCol)))
	for _, a := range sumCols {
		insSelect = append(insSelect, fmt.Sprintf("SUM(%s) AS %s", qualified(a.BaseArgCol), mvDemoQuoteIdent(a.MVCol)))
	}
	for _, a := range minCols {
		insSelect = append(insSelect, fmt.Sprintf("MIN(%s) AS %s", qualified(a.BaseArgCol), mvDemoQuoteIdent(a.MVCol)))
	}
	for _, a := range maxCols {
		insSelect = append(insSelect, fmt.Sprintf("MAX(%s) AS %s", qualified(a.BaseArgCol), mvDemoQuoteIdent(a.MVCol)))
	}
	insSelect = append(insSelect, fmt.Sprintf("0 AS %s", qHasDelete))

	// Build SELECT list for the DELETE (old) part.
	delSelect := make([]string, 0, len(insSelect))
	for _, c := range groupCols {
		delSelect = append(delSelect, fmt.Sprintf("%s AS %s", qualified(c.BaseCol), mvDemoQuoteIdent(c.MVCol)))
	}
	delSelect = append(delSelect, fmt.Sprintf("-COUNT(1) AS %s", mvDemoQuoteIdent(countCol)))
	for _, a := range sumCols {
		delSelect = append(delSelect, fmt.Sprintf("-SUM(%s) AS %s", qualified(a.BaseArgCol), mvDemoQuoteIdent(a.MVCol)))
	}
	for _, a := range minCols {
		delSelect = append(delSelect, fmt.Sprintf("NULL AS %s", mvDemoQuoteIdent(a.MVCol)))
	}
	for _, a := range maxCols {
		delSelect = append(delSelect, fmt.Sprintf("NULL AS %s", mvDemoQuoteIdent(a.MVCol)))
	}
	delSelect = append(delSelect, fmt.Sprintf("1 AS %s", qHasDelete))

	whereParts := func(oldNew string) string {
		parts := []string{
			fmt.Sprintf("%s.%s > %%? AND %s.%s < %%?",
				mvDemoQuoteIdent(qualifier), mvDemoQuoteIdent(model.ExtraCommitTsName),
				mvDemoQuoteIdent(qualifier), mvDemoQuoteIdent(model.ExtraCommitTsName),
			),
			fmt.Sprintf("%s.%s = '%s'", mvDemoQuoteIdent(qualifier), mvDemoQuoteIdent(ast.NewCIStr(materializedview.MVLogColumnOldNew)), oldNew),
		}
		if whereSQL != "" {
			parts = append(parts, "("+whereSQL+")")
		}
		return strings.Join(parts, " AND ")
	}

	insSQL := fmt.Sprintf(
		"SELECT %s FROM %s AS %s WHERE %s GROUP BY %s",
		strings.Join(insSelect, ", "),
		logFullName, mvDemoQuoteIdent(qualifier),
		whereParts(materializedview.MVLogOldNewNew),
		strings.Join(groupByExprs, ", "),
	)
	delSQL := fmt.Sprintf(
		"SELECT %s FROM %s AS %s WHERE %s GROUP BY %s",
		strings.Join(delSelect, ", "),
		logFullName, mvDemoQuoteIdent(qualifier),
		whereParts(materializedview.MVLogOldNewOld),
		strings.Join(groupByExprs, ", "),
	)

	// Outer aggregation over the UNION ALL of INSERT/DELETE parts.
	outerSelect := make([]string, 0, len(groupCols)+1+len(sumCols)+len(minCols)+len(maxCols)+1)
	for _, c := range groupCols {
		outerSelect = append(outerSelect, fmt.Sprintf("u.%s AS %s", mvDemoQuoteIdent(c.MVCol), mvDemoQuoteIdent(c.MVCol)))
	}
	outerSelect = append(outerSelect, fmt.Sprintf("SUM(u.%s) AS %s", mvDemoQuoteIdent(countCol), mvDemoQuoteIdent(countCol)))
	for _, a := range sumCols {
		outerSelect = append(outerSelect, fmt.Sprintf("SUM(u.%s) AS %s", mvDemoQuoteIdent(a.MVCol), mvDemoQuoteIdent(a.MVCol)))
	}
	for _, a := range minCols {
		outerSelect = append(outerSelect, fmt.Sprintf("MIN(u.%s) AS %s", mvDemoQuoteIdent(a.MVCol), mvDemoQuoteIdent(a.MVCol)))
	}
	for _, a := range maxCols {
		outerSelect = append(outerSelect, fmt.Sprintf("MAX(u.%s) AS %s", mvDemoQuoteIdent(a.MVCol), mvDemoQuoteIdent(a.MVCol)))
	}
	outerSelect = append(outerSelect, fmt.Sprintf("MAX(u.%s) AS %s", qHasDelete, qHasDelete))

	groupByAliases := make([]string, 0, len(groupCols))
	for _, c := range groupCols {
		groupByAliases = append(groupByAliases, "u."+mvDemoQuoteIdent(c.MVCol))
	}

	return fmt.Sprintf(
		"(SELECT %s FROM (%s UNION ALL %s) AS u GROUP BY %s)",
		strings.Join(outerSelect, ", "),
		insSQL, delSQL,
		strings.Join(groupByAliases, ", "),
	), nil
}

func mvDemoFastRefresh(
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
	if err := mvDemoBeginPessimistic(internalCtx, sctx); err != nil {
		return err
	}
	defer func() {
		if retErr != nil {
			_ = mvDemoRollback(internalCtx, sctx)
		}
	}()

	txn, err := sctx.Txn(true)
	if err != nil {
		return err
	}
	readTS := txn.StartTS()

	// Refresh mutex + fetch last_refresh_tso.
	rows, err := mvDemoQueryInternal(internalCtx, sctx,
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
	sel, mvQuery, err := mvDemoParseDefinitionSQL(is, sctx.GetSessionVars().CurrentDB, mvInfo.DefinitionSQL)
	if err != nil {
		return err
	}
	qualifier := mvQuery.BaseAlias
	if qualifier.L == "" {
		qualifier = mvQuery.BaseTable
	}
	whereSQL, err := mvDemoRestoreExprWithQualifier(sel.Where, qualifier)
	if err != nil {
		return err
	}

	// Resolve names.
	logSchema, logName, err := mvDemoGetTableNameByID(is, mvInfo.LogTableID)
	if err != nil {
		return err
	}
	logFullName := mvDemoQuoteFullName(logSchema, logName)

	baseSchema, baseName, err := mvDemoGetTableNameByID(is, mvInfo.BaseTableID)
	if err != nil {
		return err
	}
	baseFullName := mvDemoQuoteFullName(baseSchema, baseName)

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
		groupCols []mvDemoColumnMap
		countCol  ast.CIStr
		sumCols   []mvDemoAggMap
		minCols   []mvDemoAggMap
		maxCols   []mvDemoAggMap
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
			groupCols = append(groupCols, mvDemoColumnMap{BaseCol: colInfo.Name, MVCol: mvColName})
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
			sumCols = append(sumCols, mvDemoAggMap{BaseArgCol: colInfo.Name, MVCol: mvColName})
		case materializedview.MVAggKindMin:
			colInfo := model.FindColumnInfoByID(baseInfo.Columns, item.ArgBaseColumnID)
			if colInfo == nil {
				return errors.Errorf("base column %d not found", item.ArgBaseColumnID)
			}
			minCols = append(minCols, mvDemoAggMap{BaseArgCol: colInfo.Name, MVCol: mvColName})
		case materializedview.MVAggKindMax:
			colInfo := model.FindColumnInfoByID(baseInfo.Columns, item.ArgBaseColumnID)
			if colInfo == nil {
				return errors.Errorf("base column %d not found", item.ArgBaseColumnID)
			}
			maxCols = append(maxCols, mvDemoAggMap{BaseArgCol: colInfo.Name, MVCol: mvColName})
		default:
			return errors.New("invalid aggregate kind")
		}
	}
	if countCol.L == "" {
		return errors.New("materialized view count(*) column not found")
	}

	deltaSQL, err := mvDemoBuildDeltaQuery(logFullName, qualifier, whereSQL, groupCols, countCol, sumCols, minCols, maxCols)
	if err != nil {
		return err
	}
	deltaArgs := []any{lastRefreshTSO, readTS, lastRefreshTSO, readTS}

	mvFullName := mvDemoQuoteFullName(mvSchema, mvName)

	// Build join condition between MV table and delta/recompute derived tables.
	joinOn := make([]string, 0, len(groupCols))
	for _, c := range groupCols {
		col := mvDemoQuoteIdent(c.MVCol)
		joinOn = append(joinOn, fmt.Sprintf("mv.%s <=> d.%s", col, col))
	}
	joinCondMvDelta := strings.Join(joinOn, " AND ")

	// 1) Apply count/sum and insert-only min/max updates to existing groups.
	setClauses := make([]string, 0, 1+len(sumCols)+len(minCols)+len(maxCols))
	qCnt := mvDemoQuoteIdent(countCol)
	setClauses = append(setClauses, fmt.Sprintf("mv.%s = mv.%s + d.%s", qCnt, qCnt, qCnt))
	for _, a := range sumCols {
		q := mvDemoQuoteIdent(a.MVCol)
		setClauses = append(setClauses, fmt.Sprintf(
			"mv.%s = CASE WHEN d.%s IS NULL THEN mv.%s WHEN mv.%s IS NULL THEN d.%s ELSE mv.%s + d.%s END",
			q, q, q, q, q, q, q,
		))
	}
	qHasDelete := mvDemoQuoteIdent(ast.NewCIStr(mvDemoHasDeleteColumn))
	for _, a := range minCols {
		q := mvDemoQuoteIdent(a.MVCol)
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
		q := mvDemoQuoteIdent(a.MVCol)
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
	if err := mvDemoExecInternal(internalCtx, sctx, updateSQL, deltaArgs...); err != nil {
		return err
	}

	// 2) Insert new groups (matched by NULL-safe equality via LEFT JOIN).
	mvColList := make([]string, 0, len(mvCols))
	dColList := make([]string, 0, len(mvCols))
	for _, c := range mvCols {
		q := mvDemoQuoteIdent(c.Name)
		mvColList = append(mvColList, q)
		dColList = append(dColList, "d."+q)
	}
	insertJoinOn := make([]string, 0, len(groupCols))
	for _, c := range groupCols {
		col := mvDemoQuoteIdent(c.MVCol)
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
	if err := mvDemoExecInternal(internalCtx, sctx, insertSQL, deltaArgs...); err != nil {
		return err
	}

	// 3) Remove empty groups.
	if err := mvDemoExecInternal(internalCtx, sctx, "DELETE FROM "+mvFullName+" WHERE "+qCnt+" = 0"); err != nil {
		return err
	}

	// 4) Recompute MIN/MAX for groups with delete.
	if len(minCols) > 0 || len(maxCols) > 0 {
		groupSel := make([]string, 0, len(groupCols))
		groupGB := make([]string, 0, len(groupCols))
		for _, c := range groupCols {
			q := mvDemoQuoteIdent(c.MVCol)
			groupSel = append(groupSel, "d."+q+" AS "+q)
			groupGB = append(groupGB, "g."+q)
		}
		groupListSQL := fmt.Sprintf("SELECT %s FROM %s AS d WHERE d.%s = 1", strings.Join(groupSel, ", "), deltaSQL, qHasDelete)

		baseJoinOn := make([]string, 0, len(groupCols))
		for _, c := range groupCols {
			baseJoinOn = append(baseJoinOn, fmt.Sprintf(
				"%s.%s <=> g.%s",
				mvDemoQuoteIdent(qualifier), mvDemoQuoteIdent(c.BaseCol),
				mvDemoQuoteIdent(c.MVCol),
			))
		}

		recomputeSelect := make([]string, 0, len(groupCols)+len(minCols)+len(maxCols))
		for _, c := range groupCols {
			q := mvDemoQuoteIdent(c.MVCol)
			recomputeSelect = append(recomputeSelect, "g."+q+" AS "+q)
		}
		for _, a := range minCols {
			recomputeSelect = append(recomputeSelect, fmt.Sprintf("MIN(%s.%s) AS %s", mvDemoQuoteIdent(qualifier), mvDemoQuoteIdent(a.BaseArgCol), mvDemoQuoteIdent(a.MVCol)))
		}
		for _, a := range maxCols {
			recomputeSelect = append(recomputeSelect, fmt.Sprintf("MAX(%s.%s) AS %s", mvDemoQuoteIdent(qualifier), mvDemoQuoteIdent(a.BaseArgCol), mvDemoQuoteIdent(a.MVCol)))
		}

		recomputeWhere := ""
		if whereSQL != "" {
			recomputeWhere = " WHERE " + whereSQL
		}

		recomputeSQL := fmt.Sprintf(
			"(SELECT %s FROM %s AS %s JOIN (%s) AS g ON %s%s GROUP BY %s)",
			strings.Join(recomputeSelect, ", "),
			baseFullName, mvDemoQuoteIdent(qualifier),
			groupListSQL,
			strings.Join(baseJoinOn, " AND "),
			recomputeWhere,
			strings.Join(groupGB, ", "),
		)

		updateJoin := make([]string, 0, len(groupCols))
		for _, c := range groupCols {
			col := mvDemoQuoteIdent(c.MVCol)
			updateJoin = append(updateJoin, fmt.Sprintf("mv.%s <=> r.%s", col, col))
		}

		recomputeSet := make([]string, 0, len(minCols)+len(maxCols))
		for _, a := range minCols {
			q := mvDemoQuoteIdent(a.MVCol)
			recomputeSet = append(recomputeSet, fmt.Sprintf("mv.%s = r.%s", q, q))
		}
		for _, a := range maxCols {
			q := mvDemoQuoteIdent(a.MVCol)
			recomputeSet = append(recomputeSet, fmt.Sprintf("mv.%s = r.%s", q, q))
		}
		updateMinMaxSQL := fmt.Sprintf("UPDATE %s AS mv JOIN %s AS r ON %s SET %s", mvFullName, recomputeSQL, strings.Join(updateJoin, " AND "), strings.Join(recomputeSet, ", "))
		if err := mvDemoExecInternal(internalCtx, sctx, updateMinMaxSQL, deltaArgs...); err != nil {
			return err
		}
	}

	// 5) Update mv_refresh_info and commit.
	if err := mvDemoExecInternal(internalCtx, sctx,
		`UPDATE mysql.mv_refresh_info
		   SET last_refresh_tso = %?,
		       last_refresh_type = 'FAST',
		       last_refresh_result = 'SUCCESS',
		       last_refresh_time = NOW(),
		       last_error = NULL
		 WHERE mv_id = %?`,
		readTS, mvID,
	); err != nil {
		return err
	}
	return mvDemoCommit(internalCtx, sctx)
}
