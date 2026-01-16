// Copyright 2018 PingCAP, Inc.
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

package core

import (
	"context"
	math2 "math"
	"slices"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/opcode"
	"github.com/pingcap/tidb/pkg/parser/terror"
	ptypes "github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/baseimpl"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/domainmisc"
	"github.com/pingcap/tidb/pkg/planner/util/fixcontrol"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/stringutil"
	tikvstore "github.com/tikv/client-go/v2/kv"
	"go.uber.org/zap"
)

// GlobalWithoutColumnPos marks the index has no partition column.
const GlobalWithoutColumnPos = -1

type nameValuePair struct {
	colName      string
	colFieldType *types.FieldType
	value        types.Datum
	con          *expression.Constant
}

// PointPlanKey is used to get point plan that is pre-built for multi-statement query.
const PointPlanKey = stringutil.StringerStr("pointPlanKey")

// PointPlanVal is used to store point plan that is pre-built for multi-statement query.
// Save the plan in a struct so even if the point plan is nil, we don't need to try again.
type PointPlanVal struct {
	Plan base.Plan
}

// TryFastPlan tries to use the PointGetPlan for the query.
func TryFastPlan(ctx base.PlanContext, node *resolve.NodeW) (p base.Plan) {
	if checkStableResultMode(ctx) || fixcontrol.GetBoolWithDefault(ctx.GetSessionVars().OptimizerFixControl, fixcontrol.Fix52592, false) {
		// the rule of stabilizing results has not taken effect yet, so cannot generate a plan here in this mode
		// or Fix52592 is turn on to disable fast path for select, update and delete
		return nil
	}

	ctx.GetSessionVars().PlanID.Store(0)
	ctx.GetSessionVars().PlanColumnID.Store(0)
	switch x := node.Node.(type) {
	case *ast.SelectStmt:
		if x.SelectIntoOpt != nil {
			return nil
		}
		defer func() {
			vars := ctx.GetSessionVars()
			if vars.SelectLimit != math2.MaxUint64 && p != nil {
				ctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackError("sql_select_limit is set, so point get plan is not activated"))
				p = nil
			}
		}()
		// Try to convert the `SELECT a, b, c FROM t WHERE (a, b, c) in ((1, 2, 4), (1, 3, 5))` to
		// `PhysicalUnionAll` which children are `PointGet` if exists an unique key (a, b, c) in table `t`
		if fp := tryWhereIn2BatchPointGet(ctx, x, node.GetResolveContext()); fp != nil {
			if checkFastPlanPrivilege(ctx, fp.DBName, fp.TblInfo.Name.L, mysql.SelectPriv) != nil {
				return
			}
			if metadef.IsMemDB(fp.DBName) {
				return nil
			}
			fp.Lock, fp.LockWaitTime = getLockWaitTime(ctx, x.LockInfo)
			p = fp
			return
		}
		if fp := tryPointGetPlan(ctx, x, node.GetResolveContext(), isForUpdateReadSelectLock(x.LockInfo)); fp != nil {
			if checkFastPlanPrivilege(ctx, fp.DBName, fp.TblInfo.Name.L, mysql.SelectPriv) != nil {
				return nil
			}
			if metadef.IsMemDB(fp.DBName) {
				return nil
			}
			if fp.IsTableDual {
				tableDual := physicalop.PhysicalTableDual{}
				tableDual.SetOutputNames(fp.OutputNames())
				tableDual.SetSchema(fp.Schema())
				p = tableDual.Init(ctx, &property.StatsInfo{}, 0)
				return
			}
			fp.Lock, fp.LockWaitTime = getLockWaitTime(ctx, x.LockInfo)
			p = fp
			return
		}
	case *ast.UpdateStmt:
		return tryUpdatePointPlan(ctx, x, node.GetResolveContext())
	case *ast.DeleteStmt:
		return tryDeletePointPlan(ctx, x, node.GetResolveContext())
	}
	return nil
}

func getLockWaitTime(ctx base.PlanContext, lockInfo *ast.SelectLockInfo) (lock bool, waitTime int64) {
	if lockInfo != nil {
		if logicalop.IsSupportedSelectLockType(lockInfo.LockType) {
			// Locking of rows for update using SELECT FOR UPDATE only applies when autocommit
			// is disabled (either by beginning transaction with START TRANSACTION or by setting
			// autocommit to 0. If autocommit is enabled, the rows matching the specification are not locked.
			// See https://dev.mysql.com/doc/refman/5.7/en/innodb-locking-reads.html
			sessVars := ctx.GetSessionVars()
			if sessVars.PessimisticLockEligible() {
				lock = true
				waitTime = sessVars.LockWaitTimeout
				if lockInfo.LockType == ast.SelectLockForUpdateWaitN {
					waitTime = int64(lockInfo.WaitSec * 1000)
				} else if lockInfo.LockType == ast.SelectLockForUpdateNoWait || lockInfo.LockType == ast.SelectLockForShareNoWait {
					waitTime = tikvstore.LockNoWait
				}
			}
		}
	}
	return
}

func newBatchPointGetPlan(
	ctx base.PlanContext, patternInExpr *ast.PatternInExpr,
	handleCol *model.ColumnInfo, tbl *model.TableInfo, schema *expression.Schema,
	names []*types.FieldName, whereColNames []string, indexHints []*ast.IndexHint,
	dbName string, tblAlias string, tblHints []*ast.TableOptimizerHint,
) *physicalop.BatchPointGetPlan {
	stmtCtx := ctx.GetSessionVars().StmtCtx
	statsInfo := &property.StatsInfo{RowCount: float64(len(patternInExpr.List))}
	if tbl.GetPartitionInfo() != nil {
		// TODO: remove this limitation
		// Only keeping it for now to limit impact of
		// enable plan cache for partitioned tables PR.
		is := ctx.GetInfoSchema().(infoschema.InfoSchema)
		table, ok := is.TableByID(context.Background(), tbl.ID)
		if !ok {
			return nil
		}

		partTable, ok := table.(base.PartitionTable)
		if !ok {
			return nil
		}

		// PartitionExpr don't need columns and names for hash partition.
		partExpr := partTable.PartitionExpr()
		if partExpr == nil || partExpr.Expr == nil {
			return nil
		}
		if _, ok := partExpr.Expr.(*expression.Column); !ok {
			return nil
		}
	}

	if handleCol != nil {
		// condition key of where is primary key
		var handles = make([]kv.Handle, len(patternInExpr.List))
		var handleParams = make([]*expression.Constant, len(patternInExpr.List))
		for i, item := range patternInExpr.List {
			// SELECT * FROM t WHERE (key) in ((1), (2))
			if p, ok := item.(*ast.ParenthesesExpr); ok {
				item = p.Expr
			}
			var d types.Datum
			var con *expression.Constant
			switch x := item.(type) {
			case *driver.ValueExpr:
				d = x.Datum
			case *driver.ParamMarkerExpr:
				var err error
				con, err = expression.ParamMarkerExpression(ctx.GetExprCtx(), x, true)
				if err != nil {
					return nil
				}
				d, err = con.Eval(ctx.GetExprCtx().GetEvalCtx(), chunk.Row{})
				if err != nil {
					return nil
				}
			default:
				return nil
			}
			if d.IsNull() {
				return nil
			}
			intDatum := getPointGetValue(stmtCtx, handleCol, &d)
			if intDatum == nil {
				return nil
			}
			handles[i] = kv.IntHandle(intDatum.GetInt64())
			handleParams[i] = con
		}

		p := &physicalop.BatchPointGetPlan{
			TblInfo:         tbl,
			Handles:         handles,
			HandleParams:    handleParams,
			HandleType:      &handleCol.FieldType,
			HandleColOffset: handleCol.Offset,
		}

		return p.Init(ctx, statsInfo, schema, names, 0)
	}

	// The columns in where clause should be covered by unique index
	var matchIdxInfo *model.IndexInfo
	permutations := make([]int, len(whereColNames))
	colInfos := make([]*model.ColumnInfo, len(whereColNames))
	for i, innerCol := range whereColNames {
		for _, col := range tbl.Columns {
			if col.Name.L == innerCol {
				colInfos[i] = col
			}
		}
	}
	for _, idxInfo := range tbl.Indices {
		if !idxInfo.Unique || idxInfo.State != model.StatePublic || (idxInfo.Invisible && !ctx.GetSessionVars().OptimizerUseInvisibleIndexes) || idxInfo.MVIndex ||
			!indexIsAvailableByHints(
				ctx.GetSessionVars().CurrentDB,
				dbName,
				tblAlias,
				idxInfo,
				tblHints,
				indexHints,
			) {
			continue
		}
		if len(idxInfo.Columns) != len(whereColNames) || idxInfo.HasPrefixIndex() {
			continue
		}
		// TODO: not sure is there any function to reuse
		matched := true
		for whereColIndex, innerCol := range whereColNames {
			var found bool
			for i, col := range idxInfo.Columns {
				if innerCol == col.Name.L {
					permutations[whereColIndex] = i
					found = true
					break
				}
			}
			if !found {
				matched = false
				break
			}
		}
		if matched {
			matchIdxInfo = idxInfo
			break
		}
	}
	if matchIdxInfo == nil {
		return nil
	}

	indexValues := make([][]types.Datum, len(patternInExpr.List))
	indexValueParams := make([][]*expression.Constant, len(patternInExpr.List))

	var indexTypes []*types.FieldType
	for i, item := range patternInExpr.List {
		// SELECT * FROM t WHERE (key) in ((1), (2)) or SELECT * FROM t WHERE (key1, key2) in ((1, 1), (2, 2))
		if p, ok := item.(*ast.ParenthesesExpr); ok {
			item = p.Expr
		}
		var values []types.Datum
		var valuesParams []*expression.Constant
		switch x := item.(type) {
		case *ast.RowExpr:
			// The `len(values) == len(valuesParams)` should be satisfied in this mode
			if len(x.Values) != len(whereColNames) {
				return nil
			}
			values = make([]types.Datum, len(x.Values))
			valuesParams = make([]*expression.Constant, len(x.Values))
			initTypes := false
			if indexTypes == nil { // only init once
				indexTypes = make([]*types.FieldType, len(x.Values))
				initTypes = true
			}
			for index, inner := range x.Values {
				// permutations is used to match column and value.
				permIndex := permutations[index]
				switch innerX := inner.(type) {
				case *driver.ValueExpr:
					dval := getPointGetValue(stmtCtx, colInfos[index], &innerX.Datum)
					if dval == nil {
						return nil
					}
					values[permIndex] = *dval
				case *driver.ParamMarkerExpr:
					con, err := expression.ParamMarkerExpression(ctx.GetExprCtx(), innerX, true)
					if err != nil {
						return nil
					}
					d, err := con.Eval(ctx.GetExprCtx().GetEvalCtx(), chunk.Row{})
					if err != nil {
						return nil
					}
					dval := getPointGetValue(stmtCtx, colInfos[index], &d)
					if dval == nil {
						return nil
					}
					values[permIndex] = *dval
					valuesParams[permIndex] = con
					if initTypes {
						indexTypes[permIndex] = &colInfos[index].FieldType
					}
				default:
					return nil
				}
			}
		case *driver.ValueExpr:
			// if any item is `ValueExpr` type, `Expr` should contain only one column,
			// otherwise column count doesn't match and no plan can be built.
			if len(whereColNames) != 1 {
				return nil
			}
			dval := getPointGetValue(stmtCtx, colInfos[0], &x.Datum)
			if dval == nil {
				return nil
			}
			values = []types.Datum{*dval}
			valuesParams = []*expression.Constant{nil}
		case *driver.ParamMarkerExpr:
			if len(whereColNames) != 1 {
				return nil
			}
			con, err := expression.ParamMarkerExpression(ctx.GetExprCtx(), x, true)
			if err != nil {
				return nil
			}
			d, err := con.Eval(ctx.GetExprCtx().GetEvalCtx(), chunk.Row{})
			if err != nil {
				return nil
			}
			dval := getPointGetValue(stmtCtx, colInfos[0], &d)
			if dval == nil {
				return nil
			}
			values = []types.Datum{*dval}
			valuesParams = []*expression.Constant{con}
			if indexTypes == nil { // only init once
				indexTypes = []*types.FieldType{&colInfos[0].FieldType}
			}

		default:
			return nil
		}
		indexValues[i] = values
		indexValueParams[i] = valuesParams
	}

	p := &physicalop.BatchPointGetPlan{
		TblInfo:          tbl,
		IndexInfo:        matchIdxInfo,
		IndexValues:      indexValues,
		IndexValueParams: indexValueParams,
		IndexColTypes:    indexTypes,
	}

	return p.Init(ctx, statsInfo, schema, names, 0)
}

func tryWhereIn2BatchPointGet(ctx base.PlanContext, selStmt *ast.SelectStmt, resolveCtx *resolve.Context) *physicalop.BatchPointGetPlan {
	if selStmt.OrderBy != nil || selStmt.GroupBy != nil ||
		selStmt.Limit != nil || selStmt.Having != nil || selStmt.Distinct ||
		len(selStmt.WindowSpecs) > 0 {
		return nil
	}
	// `expr1 in (1, 2) and expr2 in (1, 2)` isn't PatternInExpr, so it can't use tryWhereIn2BatchPointGet.
	// (expr1, expr2) in ((1, 1), (2, 2)) can hit it.
	in, ok := selStmt.Where.(*ast.PatternInExpr)
	if !ok || in.Not || len(in.List) < 1 {
		return nil
	}

	tblName, tblAlias := getSingleTableNameAndAlias(selStmt.From)
	if tblName == nil {
		return nil
	}
	// tnW might be nil, in some ut, query is directly 'optimized' without pre-process
	tnW := resolveCtx.GetTableName(tblName)
	if tnW == nil {
		return nil
	}
	tbl := tnW.TableInfo
	// Skip the optimization with partition selection.
	// TODO: Add test and remove this!
	if len(tblName.PartitionNames) > 0 {
		return nil
	}

	for _, col := range tbl.Columns {
		if col.IsGenerated() || col.State != model.StatePublic {
			return nil
		}
	}

	schema, names := buildSchemaFromFields(tblName.Schema, tbl, tblAlias, selStmt.Fields.Fields)
	if schema == nil {
		return nil
	}

	var (
		handleCol     *model.ColumnInfo
		whereColNames []string
	)

	// SELECT * FROM t WHERE (key) in ((1), (2))
	colExpr := in.Expr
	if p, ok := colExpr.(*ast.ParenthesesExpr); ok {
		colExpr = p.Expr
	}
	switch colName := colExpr.(type) {
	case *ast.ColumnNameExpr:
		if name := colName.Name.Table.L; name != "" && name != tblAlias.L {
			return nil
		}
		// Try use handle
		if tbl.PKIsHandle {
			for _, col := range tbl.Columns {
				if mysql.HasPriKeyFlag(col.GetFlag()) && col.Name.L == colName.Name.Name.L {
					handleCol = col
					whereColNames = append(whereColNames, col.Name.L)
					break
				}
			}
		}
		if handleCol == nil {
			// Downgrade to use unique index
			whereColNames = append(whereColNames, colName.Name.Name.L)
		}

	case *ast.RowExpr:
		for _, col := range colName.Values {
			c, ok := col.(*ast.ColumnNameExpr)
			if !ok {
				return nil
			}
			if name := c.Name.Table.L; name != "" && name != tblAlias.L {
				return nil
			}
			whereColNames = append(whereColNames, c.Name.Name.L)
		}
	default:
		return nil
	}

	dbName := getLowerDB(tblName.Schema, ctx.GetSessionVars())
	p := newBatchPointGetPlan(
		ctx,
		in,
		handleCol,
		tbl,
		schema,
		names,
		whereColNames,
		tblName.IndexHints,
		dbName,
		tblAlias.L,
		selStmt.TableHints,
	)
	if p == nil {
		return nil
	}
	p.DBName = dbName

	return p
}

// tryPointGetPlan determine if the SelectStmt can use a PointGetPlan.
// Returns nil if not applicable.
// To use the PointGetPlan the following rules must be satisfied:
// 1. For the limit clause, the count should at least 1 and the offset is 0.
// 2. It must be a single table select.
// 3. All the columns must be public and not generated.
// 4. The condition is an access path that the range is a unique key.
func tryPointGetPlan(ctx base.PlanContext, selStmt *ast.SelectStmt, resolveCtx *resolve.Context, check bool) *physicalop.PointGetPlan {
	if selStmt.Having != nil || selStmt.OrderBy != nil {
		return nil
	} else if selStmt.Limit != nil {
		count, offset, err := extractLimitCountOffset(ctx.GetExprCtx(), selStmt.Limit)
		if err != nil || count == 0 || offset > 0 {
			return nil
		}
	}
	tblName, tblAlias := getSingleTableNameAndAlias(selStmt.From)
	if tblName == nil {
		return nil
	}
	// tnW might be nil, in some ut, query is directly 'optimized' without pre-process
	tnW := resolveCtx.GetTableName(tblName)
	if tnW == nil {
		return nil
	}
	tbl := tnW.TableInfo

	var pkColOffset int
	for i, col := range tbl.Columns {
		// Do not handle generated columns.
		if col.IsGenerated() {
			return nil
		}
		// Only handle tables that all columns are public.
		if col.State != model.StatePublic {
			return nil
		}
		if mysql.HasPriKeyFlag(col.GetFlag()) {
			pkColOffset = i
		}
	}
	schema, names := buildSchemaFromFields(tblName.Schema, tbl, tblAlias, selStmt.Fields.Fields)
	if schema == nil {
		return nil
	}

	pairs := make([]nameValuePair, 0, 4)
	pairs, isTableDual := getNameValuePairs(ctx.GetExprCtx(), tbl, tblAlias, pairs, selStmt.Where)
	if pairs == nil && !isTableDual {
		return nil
	}

	handlePair, fieldType := findPKHandle(tbl, pairs)
	dbName := getLowerDB(tblName.Schema, ctx.GetSessionVars())
	if handlePair.value.Kind() != types.KindNull && len(pairs) == 1 &&
		indexIsAvailableByHints(
			ctx.GetSessionVars().CurrentDB,
			dbName,
			tblAlias.L,
			nil,
			selStmt.TableHints,
			tblName.IndexHints,
		) {
		if isTableDual {
			p := newPointGetPlan(ctx, dbName, schema, tbl, names)
			p.IsTableDual = true
			return p
		}

		p := newPointGetPlan(ctx, dbName, schema, tbl, names)
		p.Handle = kv.IntHandle(handlePair.value.GetInt64())
		p.UnsignedHandle = mysql.HasUnsignedFlag(fieldType.GetFlag())
		p.HandleFieldType = fieldType
		p.HandleConstant = handlePair.con
		p.HandleColOffset = pkColOffset
		p.PartitionNames = tblName.PartitionNames
		return p
	} else if handlePair.value.Kind() != types.KindNull {
		return nil
	}

	return checkTblIndexForPointPlan(ctx, tnW, schema, tblAlias.L, selStmt.TableHints, names, pairs, isTableDual, check)
}

func checkTblIndexForPointPlan(ctx base.PlanContext, tblName *resolve.TableNameW, schema *expression.Schema,
	tblAlias string, tblHints []*ast.TableOptimizerHint,
	names []*types.FieldName, pairs []nameValuePair, isTableDual, check bool) *physicalop.PointGetPlan {
	check = check || ctx.GetSessionVars().IsIsolation(ast.ReadCommitted)
	check = check && ctx.GetSessionVars().ConnectionID > 0
	var latestIndexes map[int64]*model.IndexInfo
	var err error

	tbl := tblName.TableInfo
	dbName := getLowerDB(tblName.Schema, ctx.GetSessionVars())
	for _, idxInfo := range tbl.Indices {
		if !idxInfo.Unique || idxInfo.State != model.StatePublic || (idxInfo.Invisible && !ctx.GetSessionVars().OptimizerUseInvisibleIndexes) || idxInfo.MVIndex ||
			!indexIsAvailableByHints(
				ctx.GetSessionVars().CurrentDB,
				dbName,
				tblAlias,
				idxInfo,
				tblHints,
				tblName.IndexHints,
			) {
			continue
		}
		if idxInfo.Global {
			if tblName.TableInfo == nil ||
				len(tbl.GetPartitionInfo().AddingDefinitions) > 0 ||
				len(tbl.GetPartitionInfo().NewPartitionIDs) > 0 ||
				len(tbl.GetPartitionInfo().DroppingDefinitions) > 0 {
				continue
			}
		}
		if isTableDual {
			if check && latestIndexes == nil {
				latestIndexes, check, err = domainmisc.GetLatestIndexInfo(ctx, tbl.ID, 0)
				if err != nil {
					logutil.BgLogger().Warn("get information schema failed", zap.Error(err))
					return nil
				}
			}
			if check {
				if latestIndex, ok := latestIndexes[idxInfo.ID]; !ok || latestIndex.State != model.StatePublic {
					continue
				}
			}
			p := newPointGetPlan(ctx, dbName, schema, tbl, names)
			p.IsTableDual = true
			return p
		}
		idxValues, idxConstant, colsFieldType := getIndexValues(idxInfo, pairs)
		if idxValues == nil {
			continue
		}
		if check && latestIndexes == nil {
			latestIndexes, check, err = domainmisc.GetLatestIndexInfo(ctx, tbl.ID, 0)
			if err != nil {
				logutil.BgLogger().Warn("get information schema failed", zap.Error(err))
				return nil
			}
		}
		if check {
			if latestIndex, ok := latestIndexes[idxInfo.ID]; !ok || latestIndex.State != model.StatePublic {
				continue
			}
		}
		p := newPointGetPlan(ctx, dbName, schema, tbl, names)
		p.IndexInfo = idxInfo
		p.IndexValues = idxValues
		p.IndexConstants = idxConstant
		p.ColsFieldType = colsFieldType
		p.PartitionNames = tblName.PartitionNames
		return p
	}
	return nil
}

// indexIsAvailableByHints checks whether this index is filtered by these specified index hints.
// idxInfo is PK if it's nil
func indexIsAvailableByHints(
	currentDB string,
	dbName string,
	tblAlias string,
	idxInfo *model.IndexInfo,
	tblHints []*ast.TableOptimizerHint,
	idxHints []*ast.IndexHint,
) bool {
	combinedHints := idxHints

	// Handle *ast.TableOptimizerHint, which is from the comment style hint in the SQL.
	// In normal planner code path, it's processed by ParsePlanHints() + getPossibleAccessPaths().
	// ParsePlanHints() converts the use/force/ignore index hints in []*ast.TableOptimizerHint into *ast.IndexHint, and
	// put it into *hint.PlanHints.
	// getPossibleAccessPaths() receives the *hint.PlanHints, matches the table names, and combines the hints with the
	// USE/IGNORE/FORCE INDEX () syntax hint.
	// Here we try to remove the logic that is not needed in the fast path, and implement a faster and simpler version
	// to fit the fast path.
	for _, h := range tblHints {
		var hintType ast.IndexHintType
		// The parsing logic from ParsePlanHints()
		switch h.HintName.L {
		case hint.HintUseIndex:
			hintType = ast.HintUse
		case hint.HintIgnoreIndex:
			hintType = ast.HintIgnore
		case hint.HintForceIndex:
			hintType = ast.HintForce
		default:
			continue
		}
		if len(h.Tables) != 1 {
			// This should not happen. See HintIndexList in hintparser.y.
			intest.Assert(false)
			continue
		}
		hintDBName := h.Tables[0].DBName
		if hintDBName.L == "" {
			hintDBName = ast.NewCIStr(currentDB)
		}
		// The table name matching logic from getPossibleAccessPaths()
		if h.Tables[0].TableName.L == tblAlias &&
			(hintDBName.L == dbName || hintDBName.L == "*") {
			combinedHints = append(combinedHints, &ast.IndexHint{
				IndexNames: h.Indexes,
				HintType:   hintType,
				HintScope:  ast.HintForScan,
			})
		}
	}

	if len(combinedHints) == 0 {
		return true
	}
	match := func(name ast.CIStr) bool {
		if idxInfo == nil {
			return name.L == "primary"
		}
		return idxInfo.Name.L == name.L
	}
	// NOTICE: it's supposed that ignore hints and use/force hints will not be applied together since the effect of
	// the former will be eliminated by the latter.
	isIgnore := false
	for _, hint := range combinedHints {
		if hint.HintScope != ast.HintForScan {
			continue
		}
		if hint.HintType == ast.HintIgnore && hint.IndexNames != nil {
			isIgnore = true
			if slices.ContainsFunc(hint.IndexNames, match) {
				return false
			}
		}
		if (hint.HintType == ast.HintForce || hint.HintType == ast.HintUse) && hint.IndexNames != nil {
			if slices.ContainsFunc(hint.IndexNames, match) {
				return true
			}
		}
	}
	return isIgnore
}

func newPointGetPlan(ctx base.PlanContext, dbName string, schema *expression.Schema, tbl *model.TableInfo, names []*types.FieldName) *physicalop.PointGetPlan {
	p := &physicalop.PointGetPlan{
		Plan:         baseimpl.NewBasePlan(ctx, plancodec.TypePointGet, 0),
		DBName:       dbName,
		TblInfo:      tbl,
		LockWaitTime: ctx.GetSessionVars().LockWaitTimeout,
	}
	p.SetSchema(schema)
	p.SetOutputNames(names)

	p.Plan.SetStats(&property.StatsInfo{RowCount: 1})
	ctx.GetSessionVars().StmtCtx.Tables = []stmtctx.TableEntry{{DB: dbName, Table: tbl.Name.L}}
	return p
}

func checkFastPlanPrivilege(ctx base.PlanContext, dbName, tableName string, checkTypes ...mysql.PrivilegeType) error {
	pm := privilege.GetPrivilegeManager(ctx)
	visitInfos := make([]visitInfo, 0, len(checkTypes))
	for _, checkType := range checkTypes {
		if pm != nil && !pm.RequestVerification(ctx.GetSessionVars().ActiveRoles, dbName, tableName, "", checkType) {
			return plannererrors.ErrPrivilegeCheckFail.GenWithStackByArgs(checkType.String())
		}
		// This visitInfo is only for table lock check, so we do not need column field,
		// just fill it empty string.
		visitInfos = append(visitInfos, visitInfo{
			privilege: checkType,
			db:        dbName,
			table:     tableName,
			column:    "",
			err:       nil,
		})
	}

	infoSchema := ctx.GetInfoSchema().(infoschema.InfoSchema)
	return CheckTableLock(ctx, infoSchema, visitInfos)
}

func buildSchemaFromFields(
	dbName ast.CIStr,
	tbl *model.TableInfo,
	tblName ast.CIStr,
	fields []*ast.SelectField,
) (
	*expression.Schema,
	[]*types.FieldName,
) {
	columns := make([]*expression.Column, 0, len(tbl.Columns)+1)
	names := make([]*types.FieldName, 0, len(tbl.Columns)+1)
	if len(fields) > 0 {
		for _, field := range fields {
			if field.WildCard != nil {
				if field.WildCard.Table.L != "" && field.WildCard.Table.L != tblName.L {
					return nil, nil
				}
				for _, col := range tbl.Columns {
					names = append(names, &types.FieldName{
						DBName:      dbName,
						OrigTblName: tbl.Name,
						TblName:     tblName,
						ColName:     col.Name,
					})
					columns = append(columns, colInfoToColumn(col, len(columns)))
				}
				continue
			}
			if name, column, ok := tryExtractRowChecksumColumn(field, len(columns)); ok {
				names = append(names, name)
				columns = append(columns, column)
				continue
			}
			colNameExpr, ok := field.Expr.(*ast.ColumnNameExpr)
			if !ok {
				return nil, nil
			}
			if colNameExpr.Name.Table.L != "" && colNameExpr.Name.Table.L != tblName.L {
				return nil, nil
			}
			col := findCol(tbl, colNameExpr.Name)
			if col == nil {
				return nil, nil
			}
			asName := colNameExpr.Name.Name
			if field.AsName.L != "" {
				asName = field.AsName
			}
			names = append(names, &types.FieldName{
				DBName:      dbName,
				OrigTblName: tbl.Name,
				TblName:     tblName,
				OrigColName: col.Name,
				ColName:     asName,
			})
			columns = append(columns, colInfoToColumn(col, len(columns)))
		}
		return expression.NewSchema(columns...), names
	}
	// fields len is 0 for update and delete.
	for _, col := range tbl.Columns {
		names = append(names, &types.FieldName{
			DBName:      dbName,
			OrigTblName: tbl.Name,
			TblName:     tblName,
			ColName:     col.Name,
		})
		column := colInfoToColumn(col, len(columns))
		columns = append(columns, column)
	}
	schema := expression.NewSchema(columns...)
	return schema, names
}

func tryExtractRowChecksumColumn(field *ast.SelectField, idx int) (*types.FieldName, *expression.Column, bool) {
	f, ok := field.Expr.(*ast.FuncCallExpr)
	if !ok || f.FnName.L != ast.TiDBRowChecksum || len(f.Args) != 0 {
		return nil, nil, false
	}
	origName := f.FnName
	origName.L += "()"
	origName.O += "()"
	asName := origName
	if field.AsName.L != "" {
		asName = field.AsName
	}
	cs, cl := types.DefaultCharsetForType(mysql.TypeString)
	ftype := ptypes.NewFieldType(mysql.TypeString)
	ftype.SetCharset(cs)
	ftype.SetCollate(cl)
	ftype.SetFlen(mysql.MaxBlobWidth)
	ftype.SetDecimal(0)
	name := &types.FieldName{
		OrigColName: origName,
		ColName:     asName,
	}
	column := &expression.Column{
		RetType:  ftype,
		ID:       model.ExtraRowChecksumID,
		UniqueID: model.ExtraRowChecksumID,
		Index:    idx,
		OrigName: origName.L,
	}
	return name, column, true
}

// getSingleTableNameAndAlias return the ast node of queried table name and the alias string.
// `tblName` is `nil` if there are multiple tables in the query.
// `tblAlias` will be the real table name if there is no table alias in the query.
func getSingleTableNameAndAlias(tableRefs *ast.TableRefsClause) (tblName *ast.TableName, tblAlias ast.CIStr) {
	if tableRefs == nil || tableRefs.TableRefs == nil || tableRefs.TableRefs.Right != nil {
		return nil, tblAlias
	}
	tblSrc, ok := tableRefs.TableRefs.Left.(*ast.TableSource)
	if !ok {
		return nil, tblAlias
	}
	tblName, ok = tblSrc.Source.(*ast.TableName)
	if !ok {
		return nil, tblAlias
	}
	tblAlias = tblSrc.AsName
	if tblSrc.AsName.L == "" {
		tblAlias = tblName.Name
	}
	return tblName, tblAlias
}

// getNameValuePairs extracts `column = constant/paramMarker` conditions from expr as name value pairs.
func getNameValuePairs(ctx expression.BuildContext, tbl *model.TableInfo, tblName ast.CIStr, nvPairs []nameValuePair, expr ast.ExprNode) (
	pairs []nameValuePair, isTableDual bool) {
	evalCtx := ctx.GetEvalCtx()
	binOp, ok := expr.(*ast.BinaryOperationExpr)
	if !ok {
		return nil, false
	}
	switch binOp.Op {
	case opcode.LogicAnd:
		nvPairs, isTableDual = getNameValuePairs(ctx, tbl, tblName, nvPairs, binOp.L)
		if nvPairs == nil || isTableDual {
			return nil, isTableDual
		}
		nvPairs, isTableDual = getNameValuePairs(ctx, tbl, tblName, nvPairs, binOp.R)
		if nvPairs == nil || isTableDual {
			return nil, isTableDual
		}
		return nvPairs, isTableDual
	case opcode.EQ:
		var (
			d       types.Datum
			colName *ast.ColumnNameExpr
			ok      bool
			con     *expression.Constant
			err     error
		)
		if colName, ok = binOp.L.(*ast.ColumnNameExpr); ok {
			switch x := binOp.R.(type) {
			case *driver.ValueExpr:
				d = x.Datum
			case *driver.ParamMarkerExpr:
				con, err = expression.ParamMarkerExpression(ctx, x, false)
				if err != nil {
					return nil, false
				}
				d, err = con.Eval(evalCtx, chunk.Row{})
				if err != nil {
					return nil, false
				}
			}
		} else if colName, ok = binOp.R.(*ast.ColumnNameExpr); ok {
			switch x := binOp.L.(type) {
			case *driver.ValueExpr:
				d = x.Datum
			case *driver.ParamMarkerExpr:
				con, err = expression.ParamMarkerExpression(ctx, x, false)
				if err != nil {
					return nil, false
				}
				d, err = con.Eval(evalCtx, chunk.Row{})
				if err != nil {
					return nil, false
				}
			}
		} else {
			return nil, false
		}
		if d.IsNull() {
			return nil, false
		}
		// Views' columns have no FieldType.
		if tbl.IsView() {
			return nil, false
		}
		if colName.Name.Table.L != "" && colName.Name.Table.L != tblName.L {
			return nil, false
		}
		col := model.FindColumnInfo(tbl.Cols(), colName.Name.Name.L)
		if col == nil {
			// Partition table can't use `_tidb_rowid` to generate PointGet Plan.
			if tbl.GetPartitionInfo() != nil && colName.Name.Name.L == model.ExtraHandleName.L {
				return nil, false
			}
			return append(nvPairs, nameValuePair{colName: colName.Name.Name.L, colFieldType: types.NewFieldType(mysql.TypeLonglong), value: d, con: con}), false
		}

		// As in buildFromBinOp in util/ranger, when we build key from the expression to do range scan or point get on
		// a string column, we should set the collation of the string datum to collation of the column.
		if col.FieldType.EvalType() == types.ETString && (d.Kind() == types.KindString || d.Kind() == types.KindBinaryLiteral) {
			d.SetString(d.GetString(), col.FieldType.GetCollate())
		}

		if !checkCanConvertInPointGet(col, d) {
			return nil, false
		}
		if col.GetType() == mysql.TypeString && col.GetCollate() == charset.CollationBin { // This type we needn't to pad `\0` in here.
			return append(nvPairs, nameValuePair{colName: colName.Name.Name.L, colFieldType: &col.FieldType, value: d, con: con}), false
		}
		dVal, err := d.ConvertTo(evalCtx.TypeCtx(), &col.FieldType)
		if err != nil {
			if terror.ErrorEqual(types.ErrOverflow, err) {
				return append(nvPairs, nameValuePair{colName: colName.Name.Name.L, colFieldType: &col.FieldType, value: d, con: con}), true
			}
			// Some scenarios cast to int with error, but we may use this value in point get.
			if !terror.ErrorEqual(types.ErrTruncatedWrongVal, err) {
				return nil, false
			}
		}
		// The converted result must be same as original datum.
		cmp, err := dVal.Compare(evalCtx.TypeCtx(), &d, collate.GetCollator(col.GetCollate()))
		if err != nil || cmp != 0 {
			return nil, false
		}
		return append(nvPairs, nameValuePair{colName: colName.Name.Name.L, colFieldType: &col.FieldType, value: dVal, con: con}), false
	}
	return nil, false
}

func getPointGetValue(stmtCtx *stmtctx.StatementContext, col *model.ColumnInfo, d *types.Datum) *types.Datum {
	if !checkCanConvertInPointGet(col, *d) {
		return nil
	}
	// As in buildFromBinOp in util/ranger, when we build key from the expression to do range scan or point get on
	// a string column, we should set the collation of the string datum to collation of the column.
	if col.FieldType.EvalType() == types.ETString && (d.Kind() == types.KindString || d.Kind() == types.KindBinaryLiteral) {
		d.SetString(d.GetString(), col.FieldType.GetCollate())
	}
	dVal, err := d.ConvertTo(stmtCtx.TypeCtx(), &col.FieldType)
	if err != nil {
		return nil
	}
	// The converted result must be same as original datum.
	cmp, err := dVal.Compare(stmtCtx.TypeCtx(), d, collate.GetCollator(col.GetCollate()))
	if err != nil || cmp != 0 {
		return nil
	}
	return &dVal
}

func checkCanConvertInPointGet(col *model.ColumnInfo, d types.Datum) bool {
	kind := d.Kind()
	if col.FieldType.EvalType() == ptypes.ETString {
		switch kind {
		case types.KindInt64, types.KindUint64,
			types.KindFloat32, types.KindFloat64, types.KindMysqlDecimal:
			// column type is String and constant type is numeric
			return false
		}
	}
	if col.FieldType.GetType() == mysql.TypeBit {
		if kind == types.KindString {
			// column type is Bit and constant type is string
			return false
		}
	}
	return true
}

func findPKHandle(tblInfo *model.TableInfo, pairs []nameValuePair) (handlePair nameValuePair, fieldType *types.FieldType) {
	if !tblInfo.PKIsHandle {
		rowIDIdx := findInPairs("_tidb_rowid", pairs)
		if rowIDIdx != -1 {
			return pairs[rowIDIdx], types.NewFieldType(mysql.TypeLonglong)
		}
		return handlePair, nil
	}
	for _, col := range tblInfo.Columns {
		if mysql.HasPriKeyFlag(col.GetFlag()) {
			i := findInPairs(col.Name.L, pairs)
			if i == -1 {
				return handlePair, nil
			}
			return pairs[i], &col.FieldType
		}
	}
	return handlePair, nil
}

func getIndexValues(idxInfo *model.IndexInfo, pairs []nameValuePair) ([]types.Datum, []*expression.Constant, []*types.FieldType) {
	idxValues := make([]types.Datum, 0, 4)
	idxConstants := make([]*expression.Constant, 0, 4)
	colsFieldType := make([]*types.FieldType, 0, 4)
	if len(idxInfo.Columns) != len(pairs) {
		return nil, nil, nil
	}
	if idxInfo.HasPrefixIndex() {
		return nil, nil, nil
	}
	for _, idxCol := range idxInfo.Columns {
		i := findInPairs(idxCol.Name.L, pairs)
		if i == -1 {
			return nil, nil, nil
		}
		idxValues = append(idxValues, pairs[i].value)
		idxConstants = append(idxConstants, pairs[i].con)
		colsFieldType = append(colsFieldType, pairs[i].colFieldType)
	}
	if len(idxValues) > 0 {
		return idxValues, idxConstants, colsFieldType
	}
	return nil, nil, nil
}

func findInPairs(colName string, pairs []nameValuePair) int {
	for i, pair := range pairs {
		if pair.colName == colName {
			return i
		}
	}
	return -1
}

// Use cache to avoid allocating memory every time.
var subQueryCheckerPool = &sync.Pool{New: func() any { return &subQueryChecker{} }}

type subQueryChecker struct {
	hasSubQuery bool
}

func (s *subQueryChecker) Enter(in ast.Node) (node ast.Node, skipChildren bool) {
	if s.hasSubQuery {
		return in, true
	}

	if _, ok := in.(*ast.SubqueryExpr); ok {
		s.hasSubQuery = true
		return in, true
	}

	return in, false
}

func (s *subQueryChecker) Leave(in ast.Node) (ast.Node, bool) {
	// Before we enter the sub-query, we should keep visiting its children.
	return in, !s.hasSubQuery
}

func isExprHasSubQuery(expr ast.Node) bool {
	checker := subQueryCheckerPool.Get().(*subQueryChecker)
	defer func() {
		// Do not forget to reset the flag.
		checker.hasSubQuery = false
		subQueryCheckerPool.Put(checker)
	}()
	expr.Accept(checker)
	return checker.hasSubQuery
}

func checkIfAssignmentListHasSubQuery(list []*ast.Assignment) bool {
	return slices.ContainsFunc(list, func(assignment *ast.Assignment) bool {
		return isExprHasSubQuery(assignment.Expr)
	})
}

func tryUpdatePointPlan(ctx base.PlanContext, updateStmt *ast.UpdateStmt, resolveCtx *resolve.Context) base.Plan {
	// Avoid using the point_get when RETURNING clause is present (not yet supported).
	if updateStmt.Returning != nil {
		return nil
	}
	// Avoid using the point_get when assignment_list contains the sub-query in the UPDATE.
	if checkIfAssignmentListHasSubQuery(updateStmt.List) {
		return nil
	}

	selStmt := &ast.SelectStmt{
		TableHints: updateStmt.TableHints,
		Fields:     &ast.FieldList{},
		From:       updateStmt.TableRefs,
		Where:      updateStmt.Where,
		OrderBy:    updateStmt.Order,
		Limit:      updateStmt.Limit,
	}
	pointGet := tryPointGetPlan(ctx, selStmt, resolveCtx, true)
	if pointGet != nil {
		if pointGet.IsTableDual {
			dual := physicalop.PhysicalTableDual{}.Init(ctx, &property.StatsInfo{}, 0)
			dual.SetOutputNames(pointGet.OutputNames())
			return dual
		}
		if ctx.GetSessionVars().TxnCtx.IsPessimistic {
			pointGet.Lock, pointGet.LockWaitTime = getLockWaitTime(ctx, &ast.SelectLockInfo{LockType: ast.SelectLockForUpdate})
		}
		return buildPointUpdatePlan(ctx, pointGet, pointGet.DBName, pointGet.TblInfo, updateStmt, resolveCtx)
	}
	batchPointGet := tryWhereIn2BatchPointGet(ctx, selStmt, resolveCtx)
	if batchPointGet != nil {
		if ctx.GetSessionVars().TxnCtx.IsPessimistic {
			batchPointGet.Lock, batchPointGet.LockWaitTime = getLockWaitTime(ctx, &ast.SelectLockInfo{LockType: ast.SelectLockForUpdate})
		}
		return buildPointUpdatePlan(ctx, batchPointGet, batchPointGet.DBName, batchPointGet.TblInfo, updateStmt, resolveCtx)
	}
	return nil
}

func buildPointUpdatePlan(ctx base.PlanContext, pointPlan base.PhysicalPlan, dbName string, tbl *model.TableInfo, updateStmt *ast.UpdateStmt, resolveCtx *resolve.Context) base.Plan {
	if checkFastPlanPrivilege(ctx, dbName, tbl.Name.L, mysql.SelectPriv, mysql.UpdatePriv) != nil {
		return nil
	}
	orderedList, allAssignmentsAreConstant := buildOrderedList(ctx, pointPlan, updateStmt.List)
	if orderedList == nil {
		return nil
	}
	handleCols := buildHandleCols(dbName, tbl, pointPlan)
	updatePlan := physicalop.Update{
		SelectPlan:  pointPlan,
		OrderedList: orderedList,
		TblColPosInfos: physicalop.TblColPosInfoSlice{
			physicalop.TblColPosInfo{
				TblID:      tbl.ID,
				Start:      0,
				End:        pointPlan.Schema().Len(),
				HandleCols: handleCols,
			},
		},
		AllAssignmentsAreConstant: allAssignmentsAreConstant,
		VirtualAssignmentsOffset:  len(orderedList),
		IgnoreError:               updateStmt.IgnoreErr,
	}.Init(ctx)
	updatePlan.SetOutputNames(pointPlan.OutputNames())
	is := ctx.GetInfoSchema().(infoschema.InfoSchema)
	t, _ := is.TableByID(context.Background(), tbl.ID)
	updatePlan.TblID2Table = map[int64]table.Table{
		tbl.ID: t,
	}
	if tbl.GetPartitionInfo() != nil {
		pt := t.(table.PartitionedTable)
		nodeW := resolve.NewNodeWWithCtx(updateStmt.TableRefs.TableRefs, resolveCtx)
		updateTableList := ExtractTableList(nodeW, true)
		updatePlan.PartitionedTable = make([]table.PartitionedTable, 0, len(updateTableList))
		for _, updateTable := range updateTableList {
			if len(updateTable.PartitionNames) > 0 {
				pids := make(map[int64]struct{}, len(updateTable.PartitionNames))
				for _, name := range updateTable.PartitionNames {
					pid, err := tables.FindPartitionByName(tbl, name.L)
					if err != nil {
						return updatePlan
					}
					pids[pid] = struct{}{}
				}
				pt = tables.NewPartitionTableWithGivenSets(pt, pids)
			}
			updatePlan.PartitionedTable = append(updatePlan.PartitionedTable, pt)
		}
	}
	err := updatePlan.BuildOnUpdateFKTriggers(ctx, is, updatePlan.TblID2Table)
	if err != nil {
		return nil
	}
	return updatePlan
}

func buildOrderedList(ctx base.PlanContext, plan base.Plan, list []*ast.Assignment,
) (orderedList []*expression.Assignment, allAssignmentsAreConstant bool) {
	orderedList = make([]*expression.Assignment, 0, len(list))
	allAssignmentsAreConstant = true
	for _, assign := range list {
		idx, err := expression.FindFieldName(plan.OutputNames(), assign.Column)
		if idx == -1 || err != nil {
			return nil, true
		}
		col := plan.Schema().Columns[idx]
		newAssign := &expression.Assignment{
			Col:     col,
			ColName: plan.OutputNames()[idx].ColName,
		}
		defaultExpr := physicalop.ExtractDefaultExpr(assign.Expr)
		if defaultExpr != nil {
			defaultExpr.Name = assign.Column
		}
		expr, err := rewriteAstExprWithPlanCtx(ctx, assign.Expr, plan.Schema(), plan.OutputNames(), false)
		if err != nil {
			return nil, true
		}
		castToTP := col.GetStaticType()
		if castToTP.GetType() == mysql.TypeEnum && assign.Expr.GetType().EvalType() == types.ETInt {
			castToTP.AddFlag(mysql.EnumSetAsIntFlag)
		}
		expr = expression.BuildCastFunction(ctx.GetExprCtx(), expr, castToTP)
		if allAssignmentsAreConstant {
			_, isConst := expr.(*expression.Constant)
			allAssignmentsAreConstant = isConst
		}

		newAssign.Expr, err = expr.ResolveIndices(plan.Schema())
		if err != nil {
			return nil, true
		}
		orderedList = append(orderedList, newAssign)
	}
	return orderedList, allAssignmentsAreConstant
}

func tryDeletePointPlan(ctx base.PlanContext, delStmt *ast.DeleteStmt, resolveCtx *resolve.Context) base.Plan {
	if delStmt.IsMultiTable {
		return nil
	}
	// Avoid using the point_get when RETURNING clause is present (not yet supported).
	if delStmt.Returning != nil {
		return nil
	}
	selStmt := &ast.SelectStmt{
		TableHints: delStmt.TableHints,
		Fields:     &ast.FieldList{},
		From:       delStmt.TableRefs,
		Where:      delStmt.Where,
		OrderBy:    delStmt.Order,
		Limit:      delStmt.Limit,
	}
	if pointGet := tryPointGetPlan(ctx, selStmt, resolveCtx, true); pointGet != nil {
		if pointGet.IsTableDual {
			dual := physicalop.PhysicalTableDual{}.Init(ctx, &property.StatsInfo{}, 0)
			dual.SetOutputNames(pointGet.OutputNames())
			return dual
		}
		if ctx.GetSessionVars().TxnCtx.IsPessimistic {
			pointGet.Lock, pointGet.LockWaitTime = getLockWaitTime(ctx, &ast.SelectLockInfo{LockType: ast.SelectLockForUpdate})
		}
		return buildPointDeletePlan(ctx, pointGet, pointGet.DBName, pointGet.TblInfo, delStmt.IgnoreErr)
	}
	if batchPointGet := tryWhereIn2BatchPointGet(ctx, selStmt, resolveCtx); batchPointGet != nil {
		if ctx.GetSessionVars().TxnCtx.IsPessimistic {
			batchPointGet.Lock, batchPointGet.LockWaitTime = getLockWaitTime(ctx, &ast.SelectLockInfo{LockType: ast.SelectLockForUpdate})
		}
		return buildPointDeletePlan(ctx, batchPointGet, batchPointGet.DBName, batchPointGet.TblInfo, delStmt.IgnoreErr)
	}
	return nil
}

func buildPointDeletePlan(ctx base.PlanContext, pointPlan base.PhysicalPlan, dbName string, tbl *model.TableInfo, ignoreErr bool) base.Plan {
	if checkFastPlanPrivilege(ctx, dbName, tbl.Name.L, mysql.SelectPriv, mysql.DeletePriv) != nil {
		return nil
	}
	handleCols := buildHandleCols(dbName, tbl, pointPlan)
	var err error
	is := ctx.GetInfoSchema().(infoschema.InfoSchema)
	t, _ := is.TableByID(context.Background(), tbl.ID)
	intest.Assert(t != nil, "The point get executor is accessing a table without meta info.")
	colPosInfo, err := initColPosInfo(tbl.ID, pointPlan.OutputNames(), handleCols)
	if err != nil {
		return nil
	}
	err = buildSingleTableColPosInfoForDelete(t, &colPosInfo, 0)
	if err != nil {
		return nil
	}
	delPlan := physicalop.Delete{
		SelectPlan:     pointPlan,
		TblColPosInfos: []physicalop.TblColPosInfo{colPosInfo},
		IgnoreErr:      ignoreErr,
	}.Init(ctx)
	tblID2Table := map[int64]table.Table{tbl.ID: t}
	err = delPlan.BuildOnDeleteFKTriggers(ctx, is, tblID2Table)
	if err != nil {
		return nil
	}
	return delPlan
}

func findCol(tbl *model.TableInfo, colName *ast.ColumnName) *model.ColumnInfo {
	if colName.Name.L == model.ExtraHandleName.L && !tbl.PKIsHandle {
		colInfo := model.NewExtraHandleColInfo()
		colInfo.Offset = len(tbl.Columns) - 1
		return colInfo
	}
	for _, col := range tbl.Columns {
		if col.Name.L == colName.Name.L {
			return col
		}
	}
	return nil
}

func colInfoToColumn(col *model.ColumnInfo, idx int) *expression.Column {
	return &expression.Column{
		RetType:  col.FieldType.Clone(),
		ID:       col.ID,
		UniqueID: int64(col.Offset),
		Index:    idx,
		OrigName: col.Name.L,
	}
}

func buildHandleCols(dbName string, tbl *model.TableInfo, pointget base.PhysicalPlan) util.HandleCols {
	schema := pointget.Schema()
	// fields len is 0 for update and delete.
	if tbl.PKIsHandle {
		for i, col := range tbl.Columns {
			if mysql.HasPriKeyFlag(col.GetFlag()) {
				return util.NewIntHandleCols(schema.Columns[i])
			}
		}
	}

	if tbl.IsCommonHandle {
		pkIdx := tables.FindPrimaryIndex(tbl)
		return util.NewCommonHandleCols(tbl, pkIdx, schema.Columns)
	}

	handleCol := colInfoToColumn(model.NewExtraHandleColInfo(), schema.Len())
	schema.Append(handleCol)
	newOutputNames := pointget.OutputNames().Shallow()
	tableAliasName := tbl.Name
	if schema.Len() > 0 {
		tableAliasName = pointget.OutputNames()[0].TblName
	}
	newOutputNames = append(newOutputNames, &types.FieldName{
		DBName:      ast.NewCIStr(dbName),
		TblName:     tableAliasName,
		OrigTblName: tbl.Name,
		ColName:     model.ExtraHandleName,
	})
	pointget.SetOutputNames(newOutputNames)
	return util.NewIntHandleCols(handleCol)
}

// TODO: Remove this, by enabling all types of partitioning
// and update/add tests
func getHashOrKeyPartitionColumnName(ctx base.PlanContext, tbl *model.TableInfo) *ast.CIStr {
	pi := tbl.GetPartitionInfo()
	if pi == nil {
		return nil
	}
	if pi.Type != ast.PartitionTypeHash && pi.Type != ast.PartitionTypeKey {
		return nil
	}
	is := ctx.GetInfoSchema().(infoschema.InfoSchema)
	table, ok := is.TableByID(context.Background(), tbl.ID)
	if !ok {
		return nil
	}
	// PartitionExpr don't need columns and names for hash partition.
	partitionExpr := table.(base.PartitionTable).PartitionExpr()
	if pi.Type == ast.PartitionTypeKey {
		// used to judge whether the key partition contains only one field
		if len(pi.Columns) != 1 {
			return nil
		}
		return &pi.Columns[0]
	}
	expr := partitionExpr.OrigExpr
	col, ok := expr.(*ast.ColumnNameExpr)
	if !ok {
		return nil
	}
	return &col.Name.Name
}
