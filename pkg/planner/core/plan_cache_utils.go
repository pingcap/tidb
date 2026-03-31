// Copyright 2017 PingCAP, Inc.
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
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"math"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/bindinfo"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	metamodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/opcode"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/planner/core/rule"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/fixcontrol"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/pingcap/tidb/pkg/util/zeropool"
	atomic2 "go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	// MaxCacheableLimitCount is the max limit count for cacheable query.
	MaxCacheableLimitCount = 10000
)

var (
	// PreparedPlanCacheMaxMemory stores the max memory size defined in the global config "performance-server-memory-quota".
	PreparedPlanCacheMaxMemory = *atomic2.NewUint64(math.MaxUint64)
)

type paramMarkerExtractor struct {
	markers []ast.ParamMarkerExpr
}

func (*paramMarkerExtractor) Enter(in ast.Node) (ast.Node, bool) {
	return in, false
}

func (e *paramMarkerExtractor) Leave(in ast.Node) (ast.Node, bool) {
	if x, ok := in.(*driver.ParamMarkerExpr); ok {
		e.markers = append(e.markers, x)
	}
	return in, true
}

// GeneratePlanCacheStmtWithAST generates the PlanCacheStmt structure for this AST.
// paramSQL is the corresponding parameterized sql like 'select * from t where a<? and b>?'.
// paramStmt is the Node of paramSQL.
func GeneratePlanCacheStmtWithAST(ctx context.Context, sctx sessionctx.Context, isPrepStmt bool,
	paramSQL string, paramStmt ast.StmtNode, is infoschema.InfoSchema) (*PlanCacheStmt, base.Plan, int, error) {
	vars := sctx.GetSessionVars()
	var extractor paramMarkerExtractor
	paramStmt.Accept(&extractor)

	// DDL Statements can not accept parameters
	if _, ok := paramStmt.(ast.DDLNode); ok && len(extractor.markers) > 0 {
		return nil, nil, 0, plannererrors.ErrPrepareDDL
	}

	switch stmt := paramStmt.(type) {
	case *ast.ImportIntoStmt, *ast.LoadDataStmt, *ast.PrepareStmt, *ast.ExecuteStmt, *ast.DeallocateStmt, *ast.NonTransactionalDMLStmt, *ast.CreateProcedureInfo,
		*ast.AlterProcedureStmt, *ast.DropProcedureStmt, *ast.Signal, *ast.GetDiagnosticsStmt:
		return nil, nil, 0, plannererrors.ErrUnsupportedPs
	case *ast.SelectStmt:
		if stmt.SelectIntoOpt != nil && stmt.SelectIntoOpt.Tp == ast.SelectIntoOutfile {
			return nil, nil, 0, plannererrors.ErrUnsupportedPs
		}
	}

	// Prepare parameters should NOT over 2 bytes(MaxUint16)
	// https://dev.mysql.com/doc/internals/en/com-stmt-prepare-response.html#packet-COM_STMT_PREPARE_OK.
	if len(extractor.markers) > math.MaxUint16 {
		return nil, nil, 0, plannererrors.ErrPsManyParam
	}

	ret := &PreprocessorReturn{InfoSchema: is} // is can be nil, and
	nodeW := resolve.NewNodeW(paramStmt)
	err := Preprocess(ctx, sctx, nodeW, InPrepare, WithPreprocessorReturn(ret))
	if err != nil {
		return nil, nil, 0, err
	}

	// The parameter markers are appended in visiting order, which may not
	// be the same as the position order in the query string. We need to
	// sort it by position.
	slices.SortFunc(extractor.markers, func(i, j ast.ParamMarkerExpr) int {
		return cmp.Compare(i.(*driver.ParamMarkerExpr).Offset, j.(*driver.ParamMarkerExpr).Offset)
	})
	paramCount := len(extractor.markers)
	for i := 0; i < paramCount; i++ {
		extractor.markers[i].SetOrder(i)
	}

	prepared := &ast.Prepared{
		Stmt:       paramStmt,
		StmtType:   stmtctx.GetStmtLabel(ctx, paramStmt),
		IsReadOnly: ast.IsReadOnly(paramStmt),
	}
	normalizedSQL, digest := parser.NormalizeDigest(prepared.Stmt.Text())

	var (
		cacheable bool
		reason    string
	)
	if (isPrepStmt && !vars.EnablePreparedPlanCache) || // prepared statement
		(!isPrepStmt && !vars.EnableNonPreparedPlanCache) { // non-prepared statement
		cacheable = false
		reason = "plan cache is disabled"
	} else {
		if isPrepStmt {
			cacheable, reason = IsASTCacheable(ctx, sctx.GetPlanCtx(), paramStmt, ret.InfoSchema)
		} else {
			cacheable = true // it is already checked here
		}

		if !cacheable && fixcontrol.GetBoolWithDefault(vars.OptimizerFixControl, fixcontrol.Fix49736, false) {
			sctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackErrorf("force plan-cache: may use risky cached plan: %s", reason))
			cacheable = true
			reason = ""
		}

		if !cacheable {
			sctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackError("skip prepared plan-cache: " + reason))
		}
	}

	// For prepared statements like `prepare st from 'select * from t where a<?'`,
	// parameters are unknown here, so regard them all as NULL.
	// For non-prepared statements, all parameters are already initialized at `ParameterizeAST`, so no need to set NULL.
	if isPrepStmt {
		for i := range extractor.markers {
			param := extractor.markers[i].(*driver.ParamMarkerExpr)
			param.Datum.SetNull()
			param.InExecute = false
		}
	}

	var p base.Plan
	destBuilder, _ := NewPlanBuilder().Init(sctx.GetPlanCtx(), ret.InfoSchema, hint.NewQBHintHandler(nil))
	p, err = destBuilder.Build(ctx, nodeW)
	if err != nil {
		return nil, nil, 0, err
	}

	if cacheable && destBuilder.optFlag&rule.FlagPartitionProcessor > 0 {
		// dynamic prune mode is not used, could be that global statistics not yet available!
		cacheable = false
		reason = "static partition prune mode used"
		sctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackError("skip prepared plan-cache: " + reason))
	}

	// Collect information for metadata lock.
	dbName := make([]model.CIStr, 0, len(vars.StmtCtx.RelatedTableIDs))
	tbls := make([]table.Table, 0, len(vars.StmtCtx.RelatedTableIDs))
	relateVersion := make(map[int64]uint64, len(vars.StmtCtx.RelatedTableIDs))
	for id := range vars.StmtCtx.RelatedTableIDs {
		tbl, ok := is.TableByID(ctx, id)
		if !ok {
			logutil.BgLogger().Error("table not found in info schema", zap.Int64("tableID", id))
			return nil, nil, 0, errors.New("table not found in info schema")
		}
		db, ok := is.SchemaByID(tbl.Meta().DBID)
		if !ok {
			logutil.BgLogger().Error("database not found in info schema", zap.Int64("dbID", tbl.Meta().DBID))
			return nil, nil, 0, errors.New("database not found in info schema")
		}
		dbName = append(dbName, db.Name)
		tbls = append(tbls, tbl)
		relateVersion[id] = tbl.Meta().Revision
	}

	preparedObj := &PlanCacheStmt{
		PreparedAst:         prepared,
		ResolveCtx:          nodeW.GetResolveContext(),
		StmtDB:              vars.CurrentDB,
		StmtText:            paramSQL,
		VisitInfos:          destBuilder.GetVisitInfo(),
		NormalizedSQL:       normalizedSQL,
		SQLDigest:           digest,
		ForUpdateRead:       destBuilder.GetIsForUpdateRead(),
		SnapshotTSEvaluator: ret.SnapshotTSEvaluator,
		StmtCacheable:       cacheable,
		UncacheableReason:   reason,
		dbName:              dbName,
		tbls:                tbls,
		SchemaVersion:       ret.InfoSchema.SchemaMetaVersion(),
		RelateVersion:       relateVersion,
		Params:              extractor.markers,
	}

	stmtProcessor := &planCacheStmtProcessor{ctx: ctx, is: is, stmt: preparedObj}
	paramStmt.Accept(stmtProcessor)

	// Extract early location info during PREPARE phase.
	// This allows early forwarding decision in COM_EXECUTE without needing to compile.
	preparedObj.EarlyLocationInfo = extractEarlyLocationInfo(ctx, is, paramStmt, tbls)

	if err = checkPreparedPriv(ctx, sctx, preparedObj, ret.InfoSchema); err != nil {
		return nil, nil, 0, err
	}
	return preparedObj, p, paramCount, nil
}

// extractEarlyLocationInfo extracts location info from AST during PREPARE phase.
// This is used for early forwarding decision before compile.
// For statements without partitioned tables (including multi-table queries), we can determine
// the location immediately by resolving all table locations.
// For statements that reference partitioned tables, we only mark that partition pruning is
// needed and rely on CachedLocationInfo populated after the first plan generation.
func extractEarlyLocationInfo(ctx context.Context, is infoschema.InfoSchema, stmt ast.StmtNode, tbls []table.Table) *EarlyLocationInfo {
	if len(tbls) == 0 {
		return nil
	}

	tableIDs := make([]int64, 0, len(tbls))
	tableIDSet := make(map[int64]struct{}, len(tbls))
	hasPartitionTable := false
	for _, tbl := range tbls {
		tblMeta := tbl.Meta()
		if tblMeta == nil {
			return nil
		}
		// Ensure the DB exists in this infoschema snapshot.
		if _, ok := is.SchemaByID(tblMeta.DBID); !ok {
			return nil
		}
		if tblMeta.Partition != nil {
			hasPartitionTable = true
		}
		if _, ok := tableIDSet[tblMeta.ID]; ok {
			continue
		}
		tableIDSet[tblMeta.ID] = struct{}{}
		tableIDs = append(tableIDs, tblMeta.ID)
	}
	if len(tableIDs) == 0 {
		return nil
	}

	info := &EarlyLocationInfo{
		TableIDs:            tableIDs,
		HasPartitionTable:   hasPartitionTable,
		ForceRemotePlanHint: hasForceRemotePlanHint(stmt),
	}

	// Check if this is a DML statement
	switch s := stmt.(type) {
	case *ast.SelectStmt:
		info.StatementTraits.IsDML = false
		hasPointGet, hasIndexLookup := extractEarlySelectTraits(s, tbls)
		info.StatementTraits.HasPointGet = hasPointGet
		info.StatementTraits.HasIndexLookup = hasIndexLookup
	case *ast.UpdateStmt:
		info.StatementTraits.IsDML = true
		info.StatementTraits.DMLType = DMLTypeUpdate
	case *ast.DeleteStmt:
		info.StatementTraits.IsDML = true
		info.StatementTraits.DMLType = DMLTypeDelete
	case *ast.InsertStmt:
		info.StatementTraits.IsDML = true
		info.StatementTraits.DMLType = DMLTypeInsert
	}

	return info
}

func hasForceRemotePlanHint(stmt ast.StmtNode) bool {
	if stmt == nil {
		return false
	}
	for _, tableHint := range hint.ExtractTableHintsFromStmtNode(stmt, nil) {
		if tableHint == nil {
			continue
		}
		if tableHint.HintName.L == hint.HintTiDBXRemotePlanForce {
			return true
		}
	}
	return false
}

// tableIDSlicePool is a pool for int64 slices used in hashInt64Uint64Map.
var tableIDSlicePool = zeropool.New[[]int64](func() []int64 {
	return make([]int64, 0, 8)
})

type earlySelectAnalysis struct {
	where          ast.ExprNode
	tblInfo        *metamodel.TableInfo
	hasWhere       bool
	isSimpleSelect bool
	isSimpleKind   bool
	noOrderLimit   bool
	noLock         bool
	noSetOrWith    bool
	simpleFields   bool
}

// extractEarlySelectTraits evaluates a SELECT once to derive early location hints.
func extractEarlySelectTraits(sel *ast.SelectStmt, tbls []table.Table) (bool, bool) {
	analysis, ok := buildEarlySelectAnalysis(sel, tbls)
	if !ok {
		return false, false
	}

	hasPointGet := analysis.isSimpleSelect && analysis.hasWhere &&
		isPointGetPredicateForTable(analysis.where, analysis.tblInfo)
	hasIndexLookup := analysis.isSimpleSelect && analysis.hasWhere && analysis.isSimpleKind &&
		analysis.noOrderLimit && analysis.noLock && analysis.noSetOrWith && analysis.simpleFields
	return hasPointGet, hasIndexLookup
}

func buildEarlySelectAnalysis(sel *ast.SelectStmt, tbls []table.Table) (earlySelectAnalysis, bool) {
	var analysis earlySelectAnalysis
	if sel == nil || !isSingleTableFromClause(sel.From) {
		return analysis, false
	}
	tblInfo := singleTableInfoFromTbls(tbls)
	if tblInfo == nil {
		return analysis, false
	}

	analysis.where = sel.Where
	analysis.tblInfo = tblInfo
	analysis.hasWhere = sel.Where != nil
	analysis.isSimpleSelect = !sel.Distinct && sel.GroupBy == nil &&
		sel.Having == nil && len(sel.WindowSpecs) == 0
	analysis.isSimpleKind = sel.Kind == ast.SelectStmtKindSelect
	analysis.noOrderLimit = sel.OrderBy == nil && sel.Limit == nil
	analysis.noLock = sel.LockInfo == nil || sel.LockInfo.LockType == ast.SelectLockNone
	analysis.noSetOrWith = sel.SelectIntoOpt == nil && sel.AfterSetOperator == nil && sel.With == nil
	analysis.simpleFields = isSimpleSelectFields(sel.Fields)
	return analysis, true
}

func isPointGetPredicateForTable(where ast.ExprNode, tblInfo *metamodel.TableInfo) bool {
	if where == nil || tblInfo == nil {
		return false
	}
	// Common handle primary key point-get: all primary key columns have equality predicates.
	if tblInfo.IsCommonHandle {
		if pk := tblInfo.GetPrimaryKey(); pk != nil && len(pk.Columns) > 0 {
			pkCols := make([]string, 0, len(pk.Columns))
			for _, ic := range pk.Columns {
				pkCols = append(pkCols, ic.Name.L)
			}
			if isConjunctivePointPredicate(where, pkCols) {
				return true
			}
		}
	}

	candidates := make(map[string]struct{}, 4)
	if tblInfo.PKIsHandle {
		if pk := tblInfo.GetPkColInfo(); pk != nil {
			candidates[pk.Name.L] = struct{}{}
		}
	}
	// Also consider single-column unique indexes (simple point-get by unique key).
	for _, idx := range tblInfo.Indices {
		if idx == nil || !idx.Unique || len(idx.Columns) != 1 {
			continue
		}
		candidates[idx.Columns[0].Name.L] = struct{}{}
	}
	if len(candidates) == 0 {
		return false
	}
	for col := range candidates {
		if isSimplePointPredicate(where, col) {
			return true
		}
	}
	return false
}

func isSimpleSelectFields(fields *ast.FieldList) bool {
	if fields == nil || len(fields.Fields) == 0 {
		return false
	}
	for _, field := range fields.Fields {
		if field == nil {
			return false
		}
		if field.WildCard != nil {
			continue
		}
		if field.Expr == nil {
			return false
		}
		if _, ok := columnName(field.Expr); ok {
			continue
		}
		return false
	}
	return true
}

func singleTableInfoFromTbls(tbls []table.Table) *metamodel.TableInfo {
	var info *metamodel.TableInfo
	for _, tbl := range tbls {
		if tbl == nil {
			return nil
		}
		tblInfo := tbl.Meta()
		if tblInfo == nil {
			return nil
		}
		if info == nil {
			info = tblInfo
			continue
		}
		if info.ID != tblInfo.ID {
			return nil
		}
	}
	return info
}

func isSingleTableFromClause(from *ast.TableRefsClause) bool {
	if from == nil || from.TableRefs == nil {
		return false
	}
	join := from.TableRefs
	if join.Right != nil || join.On != nil || len(join.Using) > 0 || join.NaturalJoin {
		return false
	}
	ts, ok := join.Left.(*ast.TableSource)
	if !ok || ts == nil {
		return false
	}
	_, ok = ts.Source.(*ast.TableName)
	return ok
}

func isConjunctivePointPredicate(where ast.ExprNode, columns []string) bool {
	if where == nil || len(columns) == 0 {
		return false
	}
	conds := make([]ast.ExprNode, 0, len(columns))
	collectAndConditions(where, &conds)
	if len(conds) != len(columns) {
		return false
	}
	seen := make(map[string]struct{}, len(columns))
	for _, cond := range conds {
		for {
			if p, ok := cond.(*ast.ParenthesesExpr); ok && p != nil {
				cond = p.Expr
				continue
			}
			break
		}
		expr, ok := cond.(*ast.BinaryOperationExpr)
		if !ok || expr == nil || expr.Op != opcode.EQ {
			return false
		}

		var name string
		if n, ok := columnName(expr.L); ok && isParamOrValue(expr.R) {
			name = n
		} else if n, ok := columnName(expr.R); ok && isParamOrValue(expr.L) {
			name = n
		} else {
			return false
		}

		if _, ok := seen[name]; ok {
			return false
		}
		seen[name] = struct{}{}
	}
	for _, col := range columns {
		if _, ok := seen[col]; !ok {
			return false
		}
	}
	return true
}

func collectAndConditions(expr ast.ExprNode, conds *[]ast.ExprNode) {
	if expr == nil {
		return
	}
	for {
		if p, ok := expr.(*ast.ParenthesesExpr); ok && p != nil {
			expr = p.Expr
			continue
		}
		break
	}
	if bo, ok := expr.(*ast.BinaryOperationExpr); ok && bo != nil && bo.Op == opcode.LogicAnd {
		collectAndConditions(bo.L, conds)
		collectAndConditions(bo.R, conds)
		return
	}
	*conds = append(*conds, expr)
}

func isSimplePointPredicate(where ast.ExprNode, column string) bool {
	if where == nil {
		return false
	}
	for {
		if p, ok := where.(*ast.ParenthesesExpr); ok && p != nil {
			where = p.Expr
			continue
		}
		break
	}
	switch expr := where.(type) {
	case *ast.BinaryOperationExpr:
		if expr.Op != opcode.EQ {
			return false
		}
		if name, ok := columnName(expr.L); ok && name == column && isParamOrValue(expr.R) {
			return true
		}
		if name, ok := columnName(expr.R); ok && name == column && isParamOrValue(expr.L) {
			return true
		}
		return false
	case *ast.PatternInExpr:
		if expr.Not || expr.Sel != nil || len(expr.List) == 0 {
			return false
		}
		name, ok := columnName(expr.Expr)
		if !ok || name != column {
			return false
		}
		for _, item := range expr.List {
			if !isParamOrValue(item) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func columnName(expr ast.ExprNode) (string, bool) {
	if expr == nil {
		return "", false
	}
	for {
		if p, ok := expr.(*ast.ParenthesesExpr); ok && p != nil {
			expr = p.Expr
			continue
		}
		break
	}
	col, ok := expr.(*ast.ColumnNameExpr)
	if !ok || col == nil || col.Name == nil {
		return "", false
	}
	return col.Name.Name.L, true
}

func isParamOrValue(expr ast.ExprNode) bool {
	switch expr.(type) {
	case *driver.ParamMarkerExpr, *driver.ValueExpr:
		return true
	default:
		return false
	}
}

func hashInt64Uint64Map(b []byte, m map[int64]uint64) []byte {
	n := len(m)

	// Fast path for common cases (covers most scenarios)
	if n == 0 {
		return b
	}
	if n == 1 {
		// Single table: no need for allocation or sorting
		for k, v := range m {
			b = codec.EncodeInt(b, k)
			b = codec.EncodeUint(b, v)
			return b
		}
	}
	if n == 2 {
		// Two tables: direct comparison without array allocation
		var k1, k2 int64
		var v1, v2 uint64
		i := 0
		for k, v := range m {
			if i == 0 {
				k1, v1 = k, v
			} else {
				k2, v2 = k, v
			}
			i++
		}
		// Ensure sorted order
		if k1 > k2 {
			k1, k2 = k2, k1
			v1, v2 = v2, v1
		}
		b = codec.EncodeInt(b, k1)
		b = codec.EncodeUint(b, v1)
		b = codec.EncodeInt(b, k2)
		b = codec.EncodeUint(b, v2)
		return b
	}

	// Slow path for multiple tables
	keys := tableIDSlicePool.Get()[:0]
	defer tableIDSlicePool.Put(keys)

	// Ensure sufficient capacity
	if cap(keys) < n {
		keys = make([]int64, 0, n)
	}

	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	for _, k := range keys {
		v := m[k]
		b = codec.EncodeInt(b, k)
		b = codec.EncodeUint(b, v)
	}
	return b
}

// NewPlanCacheKey creates the plan cache key for this statement.
// Note: lastUpdatedSchemaVersion will only be set in the case of rc or for update read in order to
// differentiate the cache key. In other cases, it will be 0.
// All information that might affect the plan should be considered in this function.
func NewPlanCacheKey(sctx sessionctx.Context, stmt *PlanCacheStmt) (key, binding string, cacheable bool, reason string, err error) {
	binding, ignored := bindinfo.MatchSQLBindingForPlanCache(sctx, stmt.PreparedAst.Stmt, &stmt.BindingInfo)
	if ignored {
		return "", binding, false, "ignore plan cache by binding", nil
	}

	// In rc or for update read, we need the latest schema version to decide whether we need to
	// rebuild the plan. So we set this value in rc or for update read. In other cases, let it be 0.
	var latestSchemaVersion int64
	if sctx.GetSessionVars().IsIsolation(ast.ReadCommitted) || stmt.ForUpdateRead {
		// In Rc or ForUpdateRead, we should check if the information schema has been changed since
		// last time. If it changed, we should rebuild the plan. Here, we use a different and more
		// up-to-date schema version which can lead plan cache miss and thus, the plan will be rebuilt.
		latestSchemaVersion = domain.GetDomain(sctx).InfoSchema().SchemaMetaVersion()
	}

	// rebuild key to exclude kv.TiFlash when stmt is not read only
	vars := sctx.GetSessionVars()
	if _, isolationReadContainTiFlash := vars.IsolationReadEngines[kv.TiFlash]; isolationReadContainTiFlash && !IsReadOnly(stmt.PreparedAst.Stmt, vars) {
		delete(vars.IsolationReadEngines, kv.TiFlash)
		defer func() {
			vars.IsolationReadEngines[kv.TiFlash] = struct{}{}
		}()
	}

	if stmt.StmtText == "" {
		return "", "", false, "", errors.New("no statement text")
	}
	if stmt.SchemaVersion == 0 && !intest.InTest {
		return "", "", false, "", errors.New("Schema version uninitialized")
	}
	stmtDB := stmt.StmtDB
	if stmtDB == "" {
		stmtDB = vars.CurrentDB
	}
	timezoneOffset := 0
	if vars.TimeZone != nil {
		_, timezoneOffset = time.Now().In(vars.TimeZone).Zone()
	}
	connCharset, connCollation := vars.GetCharsetInfo()

	// not allow to share the same plan among different users for safety.
	var userName, hostName string
	if sctx.GetSessionVars().User != nil { // might be nil if in test
		userName = sctx.GetSessionVars().User.AuthUsername
		hostName = sctx.GetSessionVars().User.AuthHostname
	}

	// the user might switch the prune mode dynamically
	pruneMode := sctx.GetSessionVars().PartitionPruneMode.Load()

	// Get buffer from pool and ensure it has enough capacity
	hash := planCacheKeyBufPool.Get()[:0]
	if cap(hash) < len(stmt.StmtText)*2 {
		hash = make([]byte, 0, len(stmt.StmtText)*2)
	}
	defer func() {
		planCacheKeyBufPool.Put(hash)
	}()
	hash = append(hash, hack.Slice(userName)...)
	hash = append(hash, hack.Slice(hostName)...)
	hash = append(hash, hack.Slice(stmtDB)...)
	hash = append(hash, hack.Slice(stmt.StmtText)...)
	hash = codec.EncodeInt(hash, stmt.SchemaVersion)
	hash = hashInt64Uint64Map(hash, stmt.RelateVersion)
	hash = append(hash, pruneMode...)
	// Only be set in rc or for update read and leave it default otherwise.
	// In Rc or ForUpdateRead, we should check whether the information schema has been changed when using plan cache.
	// If it changed, we should rebuild the plan. lastUpdatedSchemaVersion help us to decide whether we should rebuild
	// the plan in rc or for update read.
	hash = codec.EncodeInt(hash, latestSchemaVersion)
	hash = codec.EncodeInt(hash, int64(vars.SQLMode))
	hash = codec.EncodeInt(hash, int64(timezoneOffset))
	if _, ok := vars.IsolationReadEngines[kv.TiDB]; ok {
		hash = append(hash, kv.TiDB.Name()...)
	}
	if _, ok := vars.IsolationReadEngines[kv.TiKV]; ok {
		hash = append(hash, kv.TiKV.Name()...)
	}
	if _, ok := vars.IsolationReadEngines[kv.TiFlash]; ok {
		hash = append(hash, kv.TiFlash.Name()...)
	}
	hash = codec.EncodeInt(hash, int64(vars.SelectLimit))
	hash = append(hash, hack.Slice(binding)...)
	hash = append(hash, hack.Slice(connCharset)...)
	hash = append(hash, hack.Slice(connCollation)...)
	hash = append(hash, bool2Byte(vars.InRestrictedSQL))
	hash = append(hash, bool2Byte(variable.RestrictedReadOnly.Load()))
	hash = append(hash, bool2Byte(variable.VarTiDBSuperReadOnly.Load()))
	// expr-pushdown-blacklist can affect query optimization, so we need to consider it in plan cache.
	hash = codec.EncodeInt(hash, expression.ExprPushDownBlackListReloadTimeStamp.Load())

	// whether this query has sub-query
	if stmt.hasSubquery {
		if !vars.EnablePlanCacheForSubquery {
			return "", "", false, "the switch 'tidb_enable_plan_cache_for_subquery' is off", nil
		}
		hash = append(hash, '1')
	} else {
		hash = append(hash, '0')
	}

	// this variable might affect the plan
	hash = append(hash, bool2Byte(vars.ForeignKeyChecks))
	hash = append(hash, bool2Byte(vars.EnablePlanCacheGenericRewrite))

	// "limit ?" can affect the cached plan: "limit 1" and "limit 10000" should use different plans.
	if len(stmt.limits) > 0 {
		if !vars.EnablePlanCacheForParamLimit {
			return "", "", false, "the switch 'tidb_enable_plan_cache_for_param_limit' is off", nil
		}
		hash = append(hash, '|')
		for _, node := range stmt.limits {
			for _, valNode := range []ast.ExprNode{node.Count, node.Offset} {
				if valNode == nil {
					continue
				}
				if param, isParam := valNode.(*driver.ParamMarkerExpr); isParam {
					typeExpected, val := CheckParamTypeInt64orUint64(param)
					if !typeExpected {
						return "", "", false, "unexpected value after LIMIT", nil
					}
					if val > MaxCacheableLimitCount {
						return "", "", false, "limit count is too large", nil
					}
					hash = codec.EncodeUint(hash, val)
				}
			}
		}
		hash = append(hash, '|')
	}

	// stats ver can affect cached plan
	if sctx.GetSessionVars().PlanCacheInvalidationOnFreshStats {
		var statsVerHash uint64
		for _, t := range stmt.tables {
			statsVerHash += getLatestVersionFromStatsTable(sctx, t.Meta(), t.Meta().ID) // use '+' as the hash function for simplicity
		}
		hash = codec.EncodeUint(hash, statsVerHash)
	}

	// handle dirty tables
	dirtyTables := vars.StmtCtx.TblInfo2UnionScan
	if len(dirtyTables) > 0 {
		// Get int64 slice from pool
		dirtyTableIDs := dirtyTableIDsPool.Get()[:0]
		if cap(dirtyTableIDs) < len(dirtyTables) {
			dirtyTableIDs = make([]int64, 0, len(dirtyTables))
		}
		for t, dirty := range dirtyTables {
			if !dirty {
				continue
			}
			dirtyTableIDs = append(dirtyTableIDs, t.ID)
		}
		sort.Slice(dirtyTableIDs, func(i, j int) bool { return dirtyTableIDs[i] < dirtyTableIDs[j] })
		for _, id := range dirtyTableIDs {
			hash = codec.EncodeInt(hash, id)
		}
		// Return slice to pool
		dirtyTableIDsPool.Put(dirtyTableIDs)
	}

	// txn status
	hash = append(hash, '|')
	hash = append(hash, bool2Byte(vars.InTxn()))
	hash = append(hash, bool2Byte(vars.IsAutocommit()))
	hash = append(hash, bool2Byte(config.GetGlobalConfig().PessimisticTxn.PessimisticAutoCommit.Load()))
	hash = append(hash, bool2Byte(vars.StmtCtx.ForShareLockEnabledByNoop))
	hash = append(hash, bool2Byte(vars.SharedLockPromotion))

	return string(hash), binding, true, "", nil
}

func bool2Byte(flag bool) byte {
	if flag {
		return '1'
	}
	return '0'
}

// PlanCacheValue stores the cached Statement and StmtNode.
type PlanCacheValue struct {
	// Meta Info, all are READ-ONLY once initialized.
	SQLDigest        string
	SQLText          string
	StmtType         string             // select, update, insert, delete, etc.
	ParseUser        string             // the user who parses/compiles this plan.
	Binding          string             // the binding of this plan.
	OptimizerEnvHash string             // other environment information that might affect the plan like "time_zone", "sql_mode".
	ParseValues      string             // the actual values used when parsing/compiling this plan.
	PlanDigest       string             // digest of the plan, used to identify the plan in the cache.
	BinaryPlan       string             // binary of this Plan, use tidb_decode_binary_plan to decode this.
	Memory           int64              // the memory usage of this plan, in bytes.
	LoadTime         time.Time          // the time when this plan is loaded into the cache.
	Plan             base.Plan          // READ-ONLY for Instance Cache, READ-WRITE for Session Cache.
	OutputColumns    types.NameSlice    // output column names of this plan
	ParamTypes       []*types.FieldType // all parameters' types, different parameters may share same plan
	StmtHints        *hint.StmtHints    // related hints of this plan, like 'max_execution_time'.
	Stmt             *PlanCacheStmt     // read-only, the original PlanCacheStmt for ResetContextOfStmt

	// Runtime Info, all are READ-WRITE, use UpdateRuntimeInfo() and RuntimeInfo() to access them.
	executions         int64 // the execution times.
	processedKeys      int64 // the total number of processed keys in TiKV.
	totalKeys          int64 // the total number of returned keys in TiKV.
	sumLatency         int64 // the total latency of this plan, in nanoseconds.
	lastUsedTimeInUnix int64 // the last time when this plan is used, in Unix timestamp.

	testKey int64 // test-only
}

// CloneForInstancePlanCache clones a PlanCacheValue for instance plan cache.
// Since PlanCacheValue.Plan is not read-only, to solve the concurrency problem when sharing the same PlanCacheValue
// across multiple sessions, we need to clone the PlanCacheValue for each session.
func (v *PlanCacheValue) CloneForInstancePlanCache(ctx context.Context, newCtx base.PlanContext) (*PlanCacheValue, bool) {
	clonedPlan, ok := v.Plan.CloneForPlanCache(newCtx)
	if !ok {
		return nil, false
	}
	if intest.InTest && ctx.Value(PlanCacheKeyTestClone{}) != nil {
		ctx.Value(PlanCacheKeyTestClone{}).(func(plan, cloned base.Plan))(v.Plan, clonedPlan)
	}
	cloned := new(PlanCacheValue)
	*cloned = *v
	cloned.Plan = clonedPlan
	return cloned, true
}

// unKnownMemoryUsage represent the memory usage of uncounted structure, maybe need implement later
// 100 KiB is approximate consumption of a plan from our internal tests
const unKnownMemoryUsage = int64(50 * size.KB)

// UpdateRuntimeInfo accumulates the runtime information of the plan.
func (v *PlanCacheValue) UpdateRuntimeInfo(proKeys, totKeys, latency int64) {
	atomic.AddInt64(&v.executions, 1)
	atomic.AddInt64(&v.processedKeys, proKeys)
	atomic.AddInt64(&v.totalKeys, totKeys)
	atomic.AddInt64(&v.sumLatency, latency)
	atomic.StoreInt64(&v.lastUsedTimeInUnix, time.Now().Unix())
}

// RuntimeInfo returns the runtime information of the plan.
func (v *PlanCacheValue) RuntimeInfo() (exec, procKeys, totKeys, sumLat int64, lastUsedTime time.Time) {
	exec = atomic.LoadInt64(&v.executions)
	procKeys = atomic.LoadInt64(&v.processedKeys)
	totKeys = atomic.LoadInt64(&v.totalKeys)
	sumLat = atomic.LoadInt64(&v.sumLatency)
	lastUsedTime = time.Unix(atomic.LoadInt64(&v.lastUsedTimeInUnix), 0)
	return
}

// MemoryUsage return the memory usage of PlanCacheValue
func (v *PlanCacheValue) MemoryUsage() (sum int64) {
	if v == nil {
		return
	}

	if v.Memory > 0 {
		return v.Memory
	}
	switch x := v.Plan.(type) {
	case base.PhysicalPlan:
		sum = x.MemoryUsage()
	case *Insert:
		sum = x.MemoryUsage()
	case *Update:
		sum = x.MemoryUsage()
	case *Delete:
		sum = x.MemoryUsage()
	default:
		sum = unKnownMemoryUsage
	}

	sum += size.SizeOfInterface + size.SizeOfSlice*2 + int64(cap(v.OutputColumns))*size.SizeOfPointer +
		size.SizeOfMap + size.SizeOfInt64*2
	if v.ParamTypes != nil {
		sum += int64(cap(v.ParamTypes)) * size.SizeOfPointer
		for _, ft := range v.ParamTypes {
			sum += ft.MemoryUsage()
		}
	}

	for _, name := range v.OutputColumns {
		sum += name.MemoryUsage()
	}
	sum += int64(len(v.SQLDigest)) + int64(len(v.SQLText)) + int64(len(v.StmtType)) + int64(len(v.BinaryPlan)) +
		int64(len(v.ParseUser)) + int64(len(v.Binding)) + int64(len(v.OptimizerEnvHash)) + int64(len(v.ParseValues))

	// Runtime Info Size
	sum += size.SizeOfFloat64 * 7

	v.Memory = sum
	return
}

var planCacheHasherPool = sync.Pool{
	New: func() any {
		return sha256.New()
	},
}

// planCacheKeyBufPool is a pool for byte slices used in NewPlanCacheKey.
var planCacheKeyBufPool = zeropool.New[[]byte](func() []byte {
	return make([]byte, 0, 512)
})

// dirtyTableIDsPool is a pool for int64 slices used in NewPlanCacheKey.
var dirtyTableIDsPool = zeropool.New[[]int64](func() []int64 {
	return make([]int64, 0, 8)
})

// NewPlanCacheValue creates a SQLCacheValue.
func NewPlanCacheValue(
	sctx sessionctx.Context,
	stmt *PlanCacheStmt,
	cacheKey string,
	binding string,
	plan base.Plan, // the cached plan,
	names []*types.FieldName, // output column names of this plan,
	paramTypes []*types.FieldType, // corresponding parameter types of this plan,
	stmtHints *hint.StmtHints, // corresponding hints of this plan,
) *PlanCacheValue {
	userParamTypes := make([]*types.FieldType, len(paramTypes))
	for i, tp := range paramTypes {
		userParamTypes[i] = tp.Clone()
	}
	var userName string
	if sctx.GetSessionVars().User != nil { // might be nil if in test
		userName = sctx.GetSessionVars().User.AuthUsername
	}

	flat := FlattenPhysicalPlan(plan, false)
	binaryPlan := BinaryPlanStrFromFlatPlan(sctx.GetPlanCtx(), flat)

	// calculate opt env hash using cacheKey and paramTypes
	// (cacheKey, paramTypes) contains all factors that can affect the plan
	// use the same hash algo with SQLDigest: sha256 + hex
	hasher := planCacheHasherPool.Get().(hash.Hash)
	hasher.Write(hack.Slice(cacheKey))
	for _, tp := range paramTypes {
		hasher.Write(hack.Slice(tp.String()))
	}
	optEnvHash := hex.EncodeToString(hasher.Sum(nil))
	hasher.Reset()
	planCacheHasherPool.Put(hasher)

	pcv := &PlanCacheValue{
		SQLDigest:        stmt.SQLDigest.String(),
		SQLText:          stmt.StmtText,
		StmtType:         stmt.PreparedAst.StmtType,
		ParseUser:        userName,
		Binding:          binding,
		OptimizerEnvHash: optEnvHash,
		ParseValues:      types.DatumsToStrNoErr(sctx.GetSessionVars().PlanCacheParams.AllParamValues()),
		PlanDigest:       stmt.PlanDigest.String(),
		BinaryPlan:       binaryPlan,

		LoadTime:      time.Now(),
		Plan:          plan,
		OutputColumns: names,
		ParamTypes:    userParamTypes,
		StmtHints:     stmtHints.Clone(),
		Stmt:          stmt,
	}
	pcv.MemoryUsage() // initialize the memory usage field
	return pcv
}

// planCacheStmtProcessor records all query features which may affect plan selection.
type planCacheStmtProcessor struct {
	ctx  context.Context
	is   infoschema.InfoSchema
	stmt *PlanCacheStmt
}

// Enter implements Visitor interface.
func (f *planCacheStmtProcessor) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	switch node := in.(type) {
	case *ast.Limit:
		f.stmt.limits = append(f.stmt.limits, node)
	case *ast.SubqueryExpr, *ast.ExistsSubqueryExpr:
		f.stmt.hasSubquery = true
	case *ast.TableName:
		t, err := f.is.TableByName(f.ctx, node.Schema, node.Name)
		if err == nil {
			f.stmt.tables = append(f.stmt.tables, t)
		}
	}
	return in, false
}

// Leave implements Visitor interface.
func (*planCacheStmtProcessor) Leave(in ast.Node) (out ast.Node, ok bool) {
	return in, true
}

// PointGetExecutorCache caches the PointGetExecutor to further improve its performance.
// Don't forget to reset this executor when the prior plan is invalid.
type PointGetExecutorCache struct {
	ColumnInfos any
	// Executor is only used for point get scene.
	// Notice that we should only cache the PointGetExecutor that have a snapshot with MaxTS in it.
	// If the current plan is not PointGet or does not use MaxTS optimization, this value should be nil here.
	Executor any

	// FastPlan is only used for instance plan cache.
	// To ensure thread-safe, we have to clone each plan before reusing if using instance plan cache.
	// To reduce the memory allocation and increase performance, we cache the FastPlan here.
	FastPlan *PointGetPlan
}

// PlanCacheStmt store prepared ast from PrepareExec and other related fields
type PlanCacheStmt struct {
	PreparedAst *ast.Prepared
	ResolveCtx  *resolve.Context
	StmtDB      string // which DB the statement will be processed over
	VisitInfos  []visitInfo
	Params      []ast.ParamMarkerExpr

	PointGet PointGetExecutorCache
	StmtPlan CacheStmtPlan

	// EarlyLocationInfo stores location info extracted during PREPARE phase.
	// This allows early forwarding decision in COM_EXECUTE without needing to compile.
	EarlyLocationInfo *EarlyLocationInfo

	// below fields are for PointGet short path
	SchemaVersion int64

	// RelateVersion stores the true cache plan table schema version, since each table schema can be updated separately in transaction.
	RelateVersion map[int64]uint64

	StmtCacheable     bool   // Whether this stmt is cacheable.
	UncacheableReason string // Why this stmt is uncacheable.

	limits      []*ast.Limit
	hasSubquery bool
	tables      []table.Table // to capture table stats changes

	NormalizedSQL       string
	NormalizedPlan      string
	SQLDigest           *parser.Digest
	PlanDigest          *parser.Digest
	ForUpdateRead       bool
	SnapshotTSEvaluator func(context.Context, sessionctx.Context) (uint64, error)

	BindingInfo bindinfo.BindingMatchInfo

	// the different between NormalizedSQL, NormalizedSQL4PC and StmtText:
	//  for the query `select * from t where a>1 and b<?`, then
	//  NormalizedSQL: select * from `t` where `a` > ? and `b` < ? --> constants are normalized to '?',
	//  NormalizedSQL4PC: select * from `test` . `t` where `a` > ? and `b` < ? --> schema name is added,
	//  StmtText: select * from t where a>1 and b <? --> just format the original query;
	StmtText string

	// InUse is used to mark plan cache is used.
	// Avoid stored procedures being recursively called in prepare.
	InUse bool

	// dbName and tbls are used to add metadata lock.
	dbName []model.CIStr
	tbls   []table.Table
}

// HasSubquery returns whether the statement contains a subquery.
func (stmt *PlanCacheStmt) HasSubquery() bool {
	return stmt.hasSubquery
}

// HasLimit returns whether the statement contains a LIMIT clause.
func (stmt *PlanCacheStmt) HasLimit() bool {
	return len(stmt.limits) > 0
}

// GetLimitValues extracts limit values (count, offset) from the statement's LIMIT clauses.
// This is used for plan cache key generation in remote execution scenarios.
func (stmt *PlanCacheStmt) GetLimitValues() []uint64 {
	if len(stmt.limits) == 0 {
		return nil
	}
	var values []uint64
	for _, node := range stmt.limits {
		for _, valNode := range []ast.ExprNode{node.Count, node.Offset} {
			if valNode == nil {
				continue
			}
			if param, isParam := valNode.(*driver.ParamMarkerExpr); isParam {
				typeExpected, val := CheckParamTypeInt64orUint64(param)
				if typeExpected && val <= MaxCacheableLimitCount {
					values = append(values, val)
				}
			}
		}
	}
	return values
}

// GetStatsVerHash computes the stats version hash for the statement's tables.
// This is used for plan cache key generation when PlanCacheInvalidationOnFreshStats is enabled.
func (stmt *PlanCacheStmt) GetStatsVerHash(sctx sessionctx.Context) uint64 {
	if len(stmt.tables) == 0 {
		return 0
	}
	var statsVerHash uint64
	for _, t := range stmt.tables {
		statsVerHash += getLatestVersionFromStatsTable(sctx, t.Meta(), t.Meta().ID)
	}
	return statsVerHash
}

// CacheStmtPlan stores the cached plan, hints and result fields.
type CacheStmtPlan struct {
	Plan      base.Plan
	StmtHints *hint.StmtHints
	Fields    []*resolve.ResultField
}

// SetCachedPlan sets the cached plan.
func (c *CacheStmtPlan) SetCachedPlan(plan base.Plan, hints *hint.StmtHints) {
	c.Plan = plan
	c.StmtHints = hints
}

// Reset resets the cached plan.
func (c *CacheStmtPlan) Reset() {
	c.Plan = nil
	c.StmtHints = nil
	c.Fields = nil
}

// GetPreparedStmt extract the prepared statement from the execute statement.
func GetPreparedStmt(stmt *ast.ExecuteStmt, vars *variable.SessionVars) (*PlanCacheStmt, error) {
	if stmt.PrepStmt != nil {
		return stmt.PrepStmt.(*PlanCacheStmt), nil
	}
	if stmt.Name != "" {
		prepStmt, err := vars.GetPreparedStmtByName(stmt.Name)
		if err != nil {
			return nil, err
		}
		stmt.PrepStmt = prepStmt
		return prepStmt.(*PlanCacheStmt), nil
	}
	return nil, plannererrors.ErrStmtNotFound
}

// CheckTypesCompatibility4PC compares FieldSlice with []*types.FieldType
// Currently this is only used in plan cache to check whether the types of parameters are compatible.
// If the types of parameters are compatible, we can use the cached plan.
// tpsExpected is types from cached plan
func checkTypesCompatibility4PC(expected, actual any) bool {
	if expected == nil || actual == nil {
		return true // no need to compare types
	}
	tpsExpected := expected.([]*types.FieldType)
	tpsActual := actual.([]*types.FieldType)
	if len(tpsExpected) != len(tpsActual) {
		return false
	}
	for i := range tpsActual {
		// We only use part of logic of `func (ft *FieldType) Equal(other *FieldType)` here because (1) only numeric and
		// string types will show up here, and (2) we don't need flen and decimal to be matched exactly to use plan cache
		tpEqual := (tpsExpected[i].GetType() == tpsActual[i].GetType()) ||
			(tpsExpected[i].GetType() == mysql.TypeVarchar && tpsActual[i].GetType() == mysql.TypeVarString) ||
			(tpsExpected[i].GetType() == mysql.TypeVarString && tpsActual[i].GetType() == mysql.TypeVarchar)
		if !tpEqual || tpsExpected[i].GetCharset() != tpsActual[i].GetCharset() || tpsExpected[i].GetCollate() != tpsActual[i].GetCollate() ||
			(tpsExpected[i].EvalType() == types.ETInt && mysql.HasUnsignedFlag(tpsExpected[i].GetFlag()) != mysql.HasUnsignedFlag(tpsActual[i].GetFlag())) {
			return false
		}
		// When the type is decimal, we should compare the Flen and Decimal.
		// We can only use the plan when both Flen and Decimal should less equal than the cached one.
		// We assume here that there is no correctness problem when the precision of the parameters is less than the precision of the parameters in the cache.
		if tpEqual && tpsExpected[i].GetType() == mysql.TypeNewDecimal && !(tpsExpected[i].GetFlen() >= tpsActual[i].GetFlen() && tpsExpected[i].GetDecimal() >= tpsActual[i].GetDecimal()) {
			return false
		}
	}
	return true
}

func isSafePointGetPath4PlanCache(sctx base.PlanContext, path *util.AccessPath) bool {
	// PointGet might contain some over-optimized assumptions, like `a>=1 and a<=1` --> `a=1`, but
	// these assumptions may be broken after parameters change.

	if isSafePointGetPath4PlanCacheScenario1(path) {
		return true
	}

	// TODO: enable this fix control switch by default after more test cases are added.
	if sctx != nil && sctx.GetSessionVars() != nil && sctx.GetSessionVars().OptimizerFixControl != nil {
		fixControlOK := fixcontrol.GetBoolWithDefault(sctx.GetSessionVars().GetOptimizerFixControlMap(), fixcontrol.Fix44830, false)
		if fixControlOK && (isSafePointGetPath4PlanCacheScenario2(path) || isSafePointGetPath4PlanCacheScenario3(path)) {
			return true
		}
	}

	return false
}

func isSafePointGetPath4PlanCacheScenario1(path *util.AccessPath) bool {
	// safe scenario 1: each column corresponds to a single EQ, `a=1 and b=2 and c=3` --> `[1, 2, 3]`
	if len(path.Ranges) <= 0 || path.Ranges[0].Width() != len(path.AccessConds) {
		return false
	}
	for _, accessCond := range path.AccessConds {
		f, ok := accessCond.(*expression.ScalarFunction)
		if !ok || f.FuncName.L != ast.EQ { // column = constant
			return false
		}
	}
	return true
}

func isSafePointGetPath4PlanCacheScenario2(path *util.AccessPath) bool {
	// safe scenario 2: this Batch or PointGet is simply from a single IN predicate, `key in (...)`
	if len(path.Ranges) <= 0 || len(path.AccessConds) != 1 {
		return false
	}
	f, ok := path.AccessConds[0].(*expression.ScalarFunction)
	if !ok || f.FuncName.L != ast.In {
		return false
	}
	return len(path.Ranges) == len(f.GetArgs())-1 // no duplicated values in this in-list for safety.
}

func isSafePointGetPath4PlanCacheScenario3(path *util.AccessPath) bool {
	// safe scenario 3: this Batch or PointGet is simply from a simple DNF like `key=? or key=? or key=?`
	if len(path.Ranges) <= 0 || len(path.AccessConds) != 1 {
		return false
	}
	f, ok := path.AccessConds[0].(*expression.ScalarFunction)
	if !ok || f.FuncName.L != ast.LogicOr {
		return false
	}

	dnfExprs := expression.FlattenDNFConditions(f)
	if len(path.Ranges) != len(dnfExprs) {
		// no duplicated values in this in-list for safety.
		// e.g. `k=1 or k=2 or k=1` --> [[1, 1], [2, 2]]
		return false
	}

	for _, expr := range dnfExprs {
		f, ok := expr.(*expression.ScalarFunction)
		if !ok {
			return false
		}
		switch f.FuncName.L {
		case ast.EQ: // (k=1 or k=2) --> [k=1, k=2]
		case ast.LogicAnd: // ((k1=1 and k2=1) or (k1=2 and k2=2)) --> [k1=1 and k2=1, k2=2 and k2=2]
			cnfExprs := expression.FlattenCNFConditions(f)
			if path.Ranges[0].Width() != len(cnfExprs) { // not all key columns are specified
				return false
			}
			for _, expr := range cnfExprs { // k1=1 and k2=1
				f, ok := expr.(*expression.ScalarFunction)
				if !ok || f.FuncName.L != ast.EQ {
					return false
				}
			}
		default:
			return false
		}
	}
	return true
}

// parseParamTypes get parameters' types in PREPARE statement
func parseParamTypes(sctx sessionctx.Context, params []expression.Expression) (paramTypes []*types.FieldType) {
	ectx := sctx.GetExprCtx().GetEvalCtx()
	paramTypes = make([]*types.FieldType, 0, len(params))
	for _, param := range params {
		if c, ok := param.(*expression.Constant); ok { // from binary protocol
			paramTypes = append(paramTypes, c.GetType(ectx))
			continue
		}

		// from text protocol, there must be a GetVar function
		name := param.(*expression.ScalarFunction).GetArgs()[0].StringWithCtx(ectx, errors.RedactLogDisable)
		tp, ok := sctx.GetSessionVars().GetUserVarType(name)
		if !ok {
			tp = types.NewFieldType(mysql.TypeNull)
		}
		paramTypes = append(paramTypes, tp)
	}
	return
}

// PlanCacheLookupParams contains parameters for plan cache lookup.
// This struct is used to pass version information from the control side to the remote side
// via forward pb, avoiding the need to parse SQL AST on the remote side.
type PlanCacheLookupParams struct {
	// ParamSQL is the parameterized SQL with ? placeholders
	ParamSQL string
	// ParamTypes are the types of the parameters
	ParamTypes []*types.FieldType
	// SchemaVersion is the schema version from control side
	SchemaVersion int64
	// RelateVersion is the table versions map from control side (table ID -> table revision)
	RelateVersion map[int64]uint64
	// LatestSchemaVersion is the latest schema version (for RC isolation)
	LatestSchemaVersion int64
	// IsReadOnly indicates whether the statement is read-only. Nil means unknown.
	IsReadOnly *bool
	// HasSubquery indicates whether the SQL contains subquery
	HasSubquery bool
	// HasLimit indicates whether the SQL contains LIMIT clause
	HasLimit bool
	// LimitValues contains the limit values (count, offset) for LIMIT clause
	LimitValues []uint64
	// StatsVerHash is the hash of stats versions for PlanCacheInvalidationOnFreshStats
	// This is computed on the control side and passed to remote side
	StatsVerHash uint64
}

// NewPlanCacheKeyBySQLWithParams creates a plan cache key directly from parameterized SQL
// with pre-computed version information from the control side.
// This is optimized for remote execution scenarios where version info is passed via forward pb.
//
// Parameters:
//   - sctx: session context
//   - params: the lookup parameters containing SQL and version info
//
// Returns:
//   - key: the cache key string
//   - cacheable: whether this SQL is cacheable
//   - reason: if not cacheable, the reason why
//   - err: any error that occurred
func NewPlanCacheKeyBySQLWithParams(sctx sessionctx.Context, params *PlanCacheLookupParams) (key string, cacheable bool, reason string, err error) {
	paramSQL := params.ParamSQL
	if paramSQL == "" {
		return "", false, "", errors.New("empty SQL text")
	}

	vars := sctx.GetSessionVars()
	stmtDB := vars.CurrentDB

	isReadOnly := true
	if params.IsReadOnly != nil {
		isReadOnly = *params.IsReadOnly
	}

	timezoneOffset := 0
	if vars.TimeZone != nil {
		_, timezoneOffset = time.Now().In(vars.TimeZone).Zone()
	}
	connCharset, connCollation := vars.GetCharsetInfo()

	// not allow to share the same plan among different users for safety.
	var userName, hostName string
	if vars.User != nil {
		userName = vars.User.AuthUsername
		hostName = vars.User.AuthHostname
	}

	// the user might switch the prune mode dynamically
	pruneMode := vars.PartitionPruneMode.Load()

	// Build the cache key hash (same format as NewPlanCacheKey)
	hash := make([]byte, 0, len(paramSQL)*2)
	hash = append(hash, hack.Slice(userName)...)
	hash = append(hash, hack.Slice(hostName)...)
	hash = append(hash, hack.Slice(stmtDB)...)
	hash = append(hash, hack.Slice(paramSQL)...)
	hash = codec.EncodeInt(hash, params.SchemaVersion)
	hash = hashInt64Uint64Map(hash, params.RelateVersion)
	hash = append(hash, pruneMode...)
	hash = codec.EncodeInt(hash, params.LatestSchemaVersion)
	hash = codec.EncodeInt(hash, int64(vars.SQLMode))
	hash = codec.EncodeInt(hash, int64(timezoneOffset))
	if _, ok := vars.IsolationReadEngines[kv.TiDB]; ok {
		hash = append(hash, kv.TiDB.Name()...)
	}
	if _, ok := vars.IsolationReadEngines[kv.TiKV]; ok {
		hash = append(hash, kv.TiKV.Name()...)
	}
	if isReadOnly {
		if _, ok := vars.IsolationReadEngines[kv.TiFlash]; ok {
			hash = append(hash, kv.TiFlash.Name()...)
		}
	}
	hash = codec.EncodeInt(hash, int64(vars.SelectLimit))
	// No binding for SQL-based lookup
	hash = append(hash, hack.Slice(connCharset)...)
	hash = append(hash, hack.Slice(connCollation)...)
	hash = append(hash, bool2Byte(vars.InRestrictedSQL))
	hash = append(hash, bool2Byte(variable.RestrictedReadOnly.Load()))
	hash = append(hash, bool2Byte(variable.VarTiDBSuperReadOnly.Load()))
	hash = codec.EncodeInt(hash, expression.ExprPushDownBlackListReloadTimeStamp.Load())

	// subquery flag
	if params.HasSubquery {
		if !vars.EnablePlanCacheForSubquery {
			return "", false, "the switch 'tidb_enable_plan_cache_for_subquery' is off", nil
		}
		hash = append(hash, '1')
	} else {
		hash = append(hash, '0')
	}

	// foreign key checks
	hash = append(hash, bool2Byte(vars.ForeignKeyChecks))
	hash = append(hash, bool2Byte(vars.EnablePlanCacheGenericRewrite))

	// "limit ?" can affect the cached plan: "limit 1" and "limit 10000" should use different plans.
	if params.HasLimit || len(params.LimitValues) > 0 {
		if !vars.EnablePlanCacheForParamLimit {
			return "", false, "the switch 'tidb_enable_plan_cache_for_param_limit' is off", nil
		}
		hash = append(hash, '|')
		for _, val := range params.LimitValues {
			hash = codec.EncodeUint(hash, val)
		}
		hash = append(hash, '|')
	}

	// stats ver can affect cached plan
	if sctx.GetSessionVars().PlanCacheInvalidationOnFreshStats {
		// Use the pre-computed stats version hash from control side
		hash = codec.EncodeUint(hash, params.StatsVerHash)
	}

	// handle dirty tables
	dirtyTables := vars.StmtCtx.TblInfo2UnionScan
	if len(dirtyTables) > 0 {
		dirtyTableIDs := make([]int64, 0, len(dirtyTables))
		for t, dirty := range dirtyTables {
			if !dirty {
				continue
			}
			dirtyTableIDs = append(dirtyTableIDs, t.ID)
		}
		sort.Slice(dirtyTableIDs, func(i, j int) bool { return dirtyTableIDs[i] < dirtyTableIDs[j] })
		for _, id := range dirtyTableIDs {
			hash = codec.EncodeInt(hash, id)
		}
	}

	// txn status
	hash = append(hash, '|')
	hash = append(hash, bool2Byte(vars.InTxn()))
	hash = append(hash, bool2Byte(vars.IsAutocommit()))
	hash = append(hash, bool2Byte(config.GetGlobalConfig().PessimisticTxn.PessimisticAutoCommit.Load()))
	hash = append(hash, bool2Byte(vars.StmtCtx.ForShareLockEnabledByNoop))
	hash = append(hash, bool2Byte(vars.SharedLockPromotion))

	return string(hash), true, "", nil
}

// LookupPlanCacheBySQLWithParams looks up the plan cache using a parameterized SQL
// with pre-computed version information from the control side.
// This is optimized for remote execution scenarios where version info is passed via forward pb.
//
// Parameters:
//   - ctx: context
//   - sctx: session context
//   - params: the lookup parameters containing SQL, param types, and version info
//
// Returns:
//   - cachedVal: the cached plan value if found
//   - cacheKey: the cache key used for lookup
//   - hit: whether the cache was hit
func LookupPlanCacheBySQLWithParams(ctx context.Context, sctx sessionctx.Context, params *PlanCacheLookupParams) (cachedVal *PlanCacheValue, cacheKey string, hit bool) {
	key, cacheable, _, err := NewPlanCacheKeyBySQLWithParams(sctx, params)
	if err != nil || !cacheable {
		return nil, "", false
	}

	return lookupPlanCacheByKey(ctx, sctx, key, params.ParamTypes, false)
}

// lookupPlanCacheByKey is the common implementation for looking up plan cache by key.
func lookupPlanCacheByKey(ctx context.Context, sctx sessionctx.Context, key string, paramTypes []*types.FieldType, noCopy bool) (cachedVal *PlanCacheValue, cacheKey string, hit bool) {
	if instancePlanCacheEnabled(ctx) {
		if v, hit := domain.GetDomain(sctx).GetInstancePlanCache().Get(key, paramTypes); hit {
			cachedVal = v.(*PlanCacheValue)
			if noCopy {
				return cachedVal, key, true
			}
			cloned, _ := cachedVal.CloneForInstancePlanCache(ctx, sctx.GetPlanCtx())
			return cloned, key, true
		}
	} else {
		if v, hit := sctx.GetSessionPlanCache().Get(key, paramTypes); hit {
			return v.(*PlanCacheValue), key, true
		}
	}
	return nil, key, false
}
