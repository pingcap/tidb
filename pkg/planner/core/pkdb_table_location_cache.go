// Copyright 2024 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0

package core

import (
	"context"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// DMLType represents the type of DML operation
type DMLType int

const (
	// DMLTypeNone indicates no DML operation
	DMLTypeNone DMLType = iota
	// DMLTypeInsert indicates INSERT operation
	DMLTypeInsert
	// DMLTypeUpdate indicates UPDATE operation
	DMLTypeUpdate
	// DMLTypeDelete indicates DELETE operation
	DMLTypeDelete
)

// String returns the string representation of DMLType
func (t DMLType) String() string {
	switch t {
	case DMLTypeInsert:
		return "INSERT"
	case DMLTypeUpdate:
		return "UPDATE"
	case DMLTypeDelete:
		return "DELETE"
	default:
		return "NONE"
	}
}

// StatementTraits summarizes statement access patterns used for location decisions.
// For EarlyLocationInfo, these are extracted from AST and are conservative hints.
// For CachedPlanLocationInfo, these are derived from the physical plan.
type StatementTraits struct {
	// HasPointGet indicates if the statement/plan contains PointGet or BatchPointGet.
	// For read-only queries, these plans are usually executed locally because KV access is already efficient.
	// For DML statements, PointGet can still be beneficial to forward (e.g. reducing commit RPCs).
	HasPointGet bool

	// HasIndexLookup indicates if the statement/plan is a simple index-lookup pushdown candidate.
	HasIndexLookup bool

	// DML related fields.
	IsDML   bool
	DMLType DMLType
}

// TableLocationInfo uses PhysPlanPartInfo and PartitionPruning for partition calculation.
type TableLocationInfo struct {
	TableID       int64
	DBName        string
	TableName     string
	IsPartitioned bool
	PlanPartInfo  *PhysPlanPartInfo
	PartitionIDs  []int64

	// IsPointGet indicates this is a PointGet or BatchPointGet plan
	// For read-only queries, these plans are usually executed locally because KV access is already efficient.
	// For DML statements, PointGet can still be beneficial to forward (e.g. reducing commit RPCs).
	IsPointGet bool

	IsIndexLookupPushDown bool

	// PartitionByRowInfo provides enough information to locate the target partition IDs for
	// some plans on partitioned tables using execution parameters (e.g. PointGet/BatchPointGet,
	// and INSERT/REPLACE ... VALUES when the partition columns are available as constants or
	// parameters).
	//
	// NOTE: PointGet/BatchPointGet do not carry PhysPlanPartInfo, and in the fast-path mode
	// (AccessConditions == nil) we still need to locate the partition by evaluating the key values,
	// similar to the logic in executor build (PrunePartitions/PrunePartitionsAndValues).
	PartitionByRowInfo *PartitionByRowInfo
}

// PartitionByRowInfo stores key values/param bindings for locating partition IDs by evaluating row values.
// It is used by PointGet/BatchPointGet and INSERT/REPLACE ... VALUES.
//
// It supports two modes:
//   - Handle-based: use HandleColOffset + HandleValues/HandleParamOrders.
//   - Index-based: use IndexColOffsets + IndexValues/IndexValueParamOrders.
//
// For constant values, the corresponding param order is -1 and the value is taken from the values slice.
// For parameterized values, the param order is >= 0 and the value is taken from params[paramOrder].
type PartitionByRowInfo struct {
	PartitionNames []pmodel.CIStr

	// Handle-based point get/batch point get.
	HandleColOffset   int
	HandleValues      []types.Datum
	HandleParamOrders []int

	// Index-based point get/batch point get.
	IndexColOffsets       []int
	IndexValues           [][]types.Datum
	IndexValueParamOrders [][]int
}

// CachedPlanLocationInfo stores location info in plan cache.
type CachedPlanLocationInfo struct {
	TableLocations    []*TableLocationInfo
	HasPartitionTable bool
	// StatementTraits summarizes access patterns derived from the physical plan.
	StatementTraits StatementTraits
}

// EarlyLocationInfo stores location info extracted during PREPARE phase.
// This allows early forwarding decision in COM_EXECUTE without needing to compile.
//
// For non-partitioned tables (including multi-table queries), we can determine the location
// immediately by resolving all table locations.
//
// For queries that reference partitioned tables, we defer the decision until CachedLocationInfo
// is populated after the first plan generation (so we can do partition pruning with actual
// parameters).
type EarlyLocationInfo struct {
	// TableIDs are the logical table IDs referenced by this statement.
	TableIDs []int64

	// HasPartitionTable indicates whether this statement references any partitioned table.
	HasPartitionTable bool

	// StatementTraits summarizes access patterns for this statement.
	StatementTraits StatementTraits

	// ForceRemotePlanHint indicates this statement has the TIDBX_REMOTE_PLAN_FORCE hint.
	ForceRemotePlanHint bool

	// CachedLocationInfo is set after the first plan generation for partitioned tables.
	// This allows subsequent executions to use partition pruning without full planning.
	CachedLocationInfo *CachedPlanLocationInfo

	// RemotePlanFeedback tracks observed forwarded result sizes for this prepared statement.
	// It can temporarily disable forwarding when the forwarded result is consistently "large",
	// to avoid regression from an extra TiDB hop and re-encoding overhead.
	RemotePlanFeedback RemotePlanFeedback
}

// HasCachedLocationInfo returns true if CachedLocationInfo is available.
// This is used to check if we can do partition pruning without full planning.
func (e *EarlyLocationInfo) HasCachedLocationInfo() bool {
	return e != nil && e.CachedLocationInfo != nil
}

// DetermineLocation determines if the query should be executed locally or remotely.
// For statements without partitioned tables, it resolves all table locations and can forward
// when all tables are located on the same remote store.
// For statements that reference partitioned tables, it uses CachedLocationInfo (when available)
// for partition pruning with parameters; otherwise it returns nil to indicate full planning
// is needed.
func (e *EarlyLocationInfo) DetermineLocation(
	ctx base.PlanContext, is infoschema.InfoSchema, params []expression.Expression,
) *PlanLocationInfo {
	return e.determineLocation(ctx, is, params, false)
}

// DetermineLocationWithOptions determines the location while allowing callers to ignore local preferences.
func (e *EarlyLocationInfo) DetermineLocationWithOptions(
	ctx base.PlanContext, is infoschema.InfoSchema, params []expression.Expression, ignoreLocalPreference bool,
) *PlanLocationInfo {
	return e.determineLocation(ctx, is, params, ignoreLocalPreference)
}

func (e *EarlyLocationInfo) determineLocation(
	ctx base.PlanContext, is infoschema.InfoSchema, params []expression.Expression, ignoreLocalPreference bool,
) *PlanLocationInfo {
	if e == nil {
		return nil
	}

	resolver := GetLocationResolver()
	if resolver == nil {
		return &PlanLocationInfo{PlanType: PhyPlanLocal, Reason: "no location resolver"}
	}

	// For read-only point-get/batch-point-get and simple index-lookup pushdown, prefer local execution.
	if !ignoreLocalPreference && !e.StatementTraits.IsDML &&
		(e.StatementTraits.HasPointGet || e.StatementTraits.HasIndexLookup) &&
		len(e.TableIDs) == 1 {
		return &PlanLocationInfo{PlanType: PhyPlanLocal, Reason: "point get/index lookup - KV access is efficient"}
	}

	// Prefer CachedLocationInfo (extracted from the compiled plan) when available.
	if e.CachedLocationInfo != nil {
		return e.CachedLocationInfo.determineLocationWithParams(ctx, is, params, ignoreLocalPreference)
	}

	// For statements that reference partitioned tables, we need CachedLocationInfo for
	// partition pruning, otherwise we cannot reliably determine the target store.
	if e.HasPartitionTable {
		return nil
	}

	if len(e.TableIDs) == 0 {
		return nil
	}

	storeAddrs := make(map[string]struct{}, 1)
	for _, tableID := range e.TableIDs {
		addr := resolver.ResolveTableLocation(tableID)
		if addr == "" {
			return &PlanLocationInfo{PlanType: PhyPlanLocal, Reason: "no store address for table"}
		}
		storeAddrs[addr] = struct{}{}
		if len(storeAddrs) > 1 {
			return &PlanLocationInfo{PlanType: PhyPlanDistributed, Reason: "multiple stores"}
		}
	}

	var targetStore string
	for addr := range storeAddrs {
		targetStore = addr
		break
	}
	if resolver.IsLocalStore(targetStore) {
		return &PlanLocationInfo{PlanType: PhyPlanLocal, TargetStore: targetStore, Reason: "local"}
	}
	return &PlanLocationInfo{PlanType: PhyPlanRemote, TargetStore: targetStore, Reason: "remote"}
}

// SetCachedLocationInfo sets the CachedLocationInfo after the first plan generation.
// This is called after compile to cache the location info for subsequent executions.
func (e *EarlyLocationInfo) SetCachedLocationInfo(info *CachedPlanLocationInfo) {
	if e != nil {
		e.CachedLocationInfo = info
	}
}

// CalculatePartitionIDs calculates partition IDs using PartitionPruning.
func (loc *TableLocationInfo) CalculatePartitionIDs(
	ctx base.PlanContext, is infoschema.InfoSchema, params []expression.Expression,
) ([]int64, bool, error) {
	if !loc.IsPartitioned {
		return []int64{loc.TableID}, false, nil
	}

	// For some plans we can locate the target partition IDs by evaluating key values with parameters
	// (e.g. PointGet/BatchPointGet, and INSERT/REPLACE ... VALUES when the partition columns are available
	// as constants or parameters).
	if loc.PartitionByRowInfo != nil {
		partIDs, allParts, ok := loc.calculatePartitionIDsByRow(ctx, is, params)
		if ok {
			return partIDs, allParts, nil
		}
		// Fallback to "all partitions" so the caller won't forward incorrectly.
		return loc.PartitionIDs, true, nil
	}

	if loc.PlanPartInfo == nil {
		return loc.PartitionIDs, len(loc.PartitionIDs) > 1, nil
	}
	tbl, err := is.TableByName(context.Background(), pmodel.NewCIStr(loc.DBName), pmodel.NewCIStr(loc.TableName))
	if err != nil {
		return loc.PartitionIDs, true, nil
	}
	partTbl, ok := tbl.(table.PartitionedTable)
	if !ok {
		return []int64{loc.TableID}, false, nil
	}
	// Get evalCtx for evaluating GetVar expressions
	evalCtx := ctx.GetExprCtx().GetEvalCtx()
	pruningConds := substituteParamsInConds(loc.PlanPartInfo.PruningConds, params, evalCtx)
	idxArr, err := PartitionPruning(ctx, partTbl, pruningConds,
		loc.PlanPartInfo.PartitionNames, loc.PlanPartInfo.Columns, loc.PlanPartInfo.ColumnNames)
	if err != nil {
		return loc.PartitionIDs, true, nil
	}
	if len(idxArr) == 1 && idxArr[0] == FullRange {
		return loc.PartitionIDs, true, nil
	}
	pi := partTbl.Meta().GetPartitionInfo()
	result := make([]int64, 0, len(idxArr))
	for _, idx := range idxArr {
		if idx >= 0 && idx < len(pi.Definitions) {
			result = append(result, pi.Definitions[idx].ID)
		}
	}
	return result, false, nil
}

func (loc *TableLocationInfo) calculatePartitionIDsByRow(
	ctx base.PlanContext, is infoschema.InfoSchema, params []expression.Expression,
) ([]int64, bool, bool) {
	if loc == nil || loc.PartitionByRowInfo == nil || ctx == nil || is == nil {
		return nil, false, false
	}
	info := loc.PartitionByRowInfo

	tbl, ok := is.TableByID(context.Background(), loc.TableID)
	if !ok || tbl == nil {
		return nil, false, false
	}
	pt := tbl.GetPartitionedTable()
	if pt == nil || pt.Meta() == nil || pt.Meta().Partition == nil {
		return nil, false, false
	}
	pi := pt.Meta().Partition
	evalCtx := ctx.GetExprCtx().GetEvalCtx()
	typeCtx := ctx.GetSessionVars().StmtCtx.TypeCtx()

	evalParam := func(paramIdx int, target *types.FieldType) (types.Datum, bool) {
		if paramIdx < 0 || paramIdx >= len(params) {
			return types.Datum{}, false
		}
		expr := params[paramIdx]
		if expr == nil {
			return types.Datum{}, false
		}
		// Materialize GetVar(@var) when possible so pruning can use the actual constant.
		if sf, ok := expr.(*expression.ScalarFunction); ok && sf.FuncName.L == ast.GetVar {
			if constExpr := tryEvalGetVarToConstant(sf, evalCtx); constExpr != nil {
				expr = constExpr
			}
		}
		val, err := expr.Eval(evalCtx, chunk.Row{})
		if err != nil || val.IsNull() {
			return types.Datum{}, false
		}
		if target == nil {
			return val, true
		}
		converted, err := val.ConvertTo(typeCtx, target)
		if err != nil {
			return types.Datum{}, false
		}
		return converted, true
	}

	hasHandle := len(info.HandleValues) > 0 || len(info.HandleParamOrders) > 0
	hasIndex := len(info.IndexColOffsets) > 0
	if hasHandle && hasIndex {
		return nil, false, false
	}
	if !hasHandle && !hasIndex {
		return nil, false, false
	}

	if hasHandle {
		n := len(info.HandleValues)
		if len(info.HandleParamOrders) > 0 {
			n = len(info.HandleParamOrders)
		}
		if n == 0 {
			return nil, false, false
		}
		if len(info.HandleValues) > 0 && len(info.HandleValues) != n {
			return nil, false, false
		}
		if info.HandleColOffset < 0 || info.HandleColOffset >= len(pt.Meta().Columns) {
			return nil, false, false
		}
		targetFT := &pt.Meta().Columns[info.HandleColOffset].FieldType

		resultSet := make(map[int64]struct{}, 1)
		for i := 0; i < n; i++ {
			row := make([]types.Datum, len(pt.Meta().Columns))

			paramOrder := -1
			if len(info.HandleParamOrders) > 0 {
				paramOrder = info.HandleParamOrders[i]
			}

			var d types.Datum
			if paramOrder >= 0 {
				var ok bool
				d, ok = evalParam(paramOrder, targetFT)
				if !ok {
					return nil, false, false
				}
			} else {
				d = info.HandleValues[i]
				// Normalize datum kind for unsigned handle columns.
				if mysql.HasUnsignedFlag(targetFT.GetFlag()) && d.Kind() == types.KindInt64 && d.GetInt64() >= 0 {
					d = types.NewUintDatum(uint64(d.GetInt64()))
				}
				converted, err := d.ConvertTo(typeCtx, targetFT)
				if err != nil {
					return nil, false, false
				}
				d = converted
			}

			d.Copy(&row[info.HandleColOffset])
			partIdx, err := pt.GetPartitionIdxByRow(evalCtx, row)
			partIdx, err = pi.ReplaceWithOverlappingPartitionIdx(partIdx, err)
			if err != nil || partIdx < 0 || partIdx >= len(pi.Definitions) || !isInExplicitPartitions(pi, partIdx, info.PartitionNames) {
				return nil, false, false
			}
			resultSet[pi.Definitions[partIdx].ID] = struct{}{}
		}

		partIDs := make([]int64, 0, len(resultSet))
		for pid := range resultSet {
			partIDs = append(partIDs, pid)
		}
		allParts := len(loc.PartitionIDs) > 0 && len(partIDs) == len(loc.PartitionIDs)
		return partIDs, allParts, true
	}

	// Index-based mode
	rows := len(info.IndexValues)
	if rows == 0 {
		return nil, false, false
	}
	cols := len(info.IndexColOffsets)
	if cols == 0 {
		return nil, false, false
	}
	if len(info.IndexValueParamOrders) != 0 && len(info.IndexValueParamOrders) != rows {
		return nil, false, false
	}

	resultSet := make(map[int64]struct{}, 1)
	for rowIdx := 0; rowIdx < rows; rowIdx++ {
		if len(info.IndexValues[rowIdx]) != cols {
			return nil, false, false
		}
		if len(info.IndexValueParamOrders) != 0 && len(info.IndexValueParamOrders[rowIdx]) != cols {
			return nil, false, false
		}

		row := make([]types.Datum, len(pt.Meta().Columns))
		for colIdx := 0; colIdx < cols; colIdx++ {
			offset := info.IndexColOffsets[colIdx]
			if offset < 0 || offset >= len(pt.Meta().Columns) {
				return nil, false, false
			}
			targetFT := &pt.Meta().Columns[offset].FieldType

			paramOrder := -1
			if len(info.IndexValueParamOrders) != 0 {
				paramOrder = info.IndexValueParamOrders[rowIdx][colIdx]
			}

			var d types.Datum
			if paramOrder >= 0 {
				var ok bool
				d, ok = evalParam(paramOrder, targetFT)
				if !ok {
					return nil, false, false
				}
			} else {
				d = info.IndexValues[rowIdx][colIdx]
				converted, err := d.ConvertTo(typeCtx, targetFT)
				if err != nil {
					return nil, false, false
				}
				d = converted
			}

			d.Copy(&row[offset])
		}

		partIdx, err := pt.GetPartitionIdxByRow(evalCtx, row)
		partIdx, err = pi.ReplaceWithOverlappingPartitionIdx(partIdx, err)
		if err != nil || partIdx < 0 || partIdx >= len(pi.Definitions) || !isInExplicitPartitions(pi, partIdx, info.PartitionNames) {
			return nil, false, false
		}
		resultSet[pi.Definitions[partIdx].ID] = struct{}{}
	}

	partIDs := make([]int64, 0, len(resultSet))
	for pid := range resultSet {
		partIDs = append(partIDs, pid)
	}
	allParts := len(loc.PartitionIDs) > 0 && len(partIDs) == len(loc.PartitionIDs)
	return partIDs, allParts, true
}

func substituteParamsInConds(conds []expression.Expression, params []expression.Expression, evalCtx expression.EvalContext) []expression.Expression {
	if len(params) == 0 {
		return conds
	}
	result := make([]expression.Expression, len(conds))
	for i, cond := range conds {
		result[i] = substituteParamInExpr(cond, params, evalCtx)
	}
	return result
}

func substituteParamInExpr(expr expression.Expression, params []expression.Expression, evalCtx expression.EvalContext) expression.Expression {
	if expr == nil {
		return nil
	}
	switch e := expr.(type) {
	case *expression.ScalarFunction:
		newArgs := make([]expression.Expression, len(e.GetArgs()))
		changed := false
		for i, arg := range e.GetArgs() {
			newArg := substituteParamInExpr(arg, params, evalCtx)
			newArgs[i] = newArg
			if newArg != arg {
				changed = true
			}
		}
		if changed {
			newFunc := e.Clone()
			if sf, ok := newFunc.(*expression.ScalarFunction); ok {
				for i := 0; i < len(newArgs); i++ {
					sf.GetArgs()[i] = newArgs[i]
				}
			}
			return newFunc
		}
		return e
	case *expression.Constant:
		if e.ParamMarker != nil {
			paramIdx := e.ParamMarker.Order()
			if paramIdx >= 0 && paramIdx < len(params) {
				param := params[paramIdx]
				// Check if the param is a GetVar expression (user-defined variable like @var)
				// If so, evaluate it to get the actual constant value
				if sf, ok := param.(*expression.ScalarFunction); ok && sf.FuncName.L == ast.GetVar {
					if constExpr := tryEvalGetVarToConstant(sf, evalCtx); constExpr != nil {
						return constExpr
					}
				}
				return param
			}
		}
		return e
	default:
		return expr
	}
}

// tryEvalGetVarToConstant tries to evaluate a GetVar expression to a constant.
// GetVar expressions represent user-defined variables like @var.
// For partition pruning, we need the actual constant value, not the GetVar expression.
func tryEvalGetVarToConstant(sf *expression.ScalarFunction, evalCtx expression.EvalContext) *expression.Constant {
	if evalCtx == nil {
		return nil
	}
	// Evaluate the GetVar expression to get the actual value
	val, err := sf.Eval(evalCtx, chunk.Row{})
	if err != nil {
		return nil
	}
	// Create a new Constant with the evaluated value
	return &expression.Constant{
		Value:   val,
		RetType: sf.GetType(evalCtx),
	}
}

// DetermineLocationWithParams determines if the query should be executed locally or remotely.
func (c *CachedPlanLocationInfo) DetermineLocationWithParams(
	ctx base.PlanContext, is infoschema.InfoSchema, params []expression.Expression,
) *PlanLocationInfo {
	return c.determineLocationWithParams(ctx, is, params, false)
}

func (c *CachedPlanLocationInfo) determineLocationWithParams(
	ctx base.PlanContext, is infoschema.InfoSchema, params []expression.Expression, ignoreLocalPreference bool,
) *PlanLocationInfo {
	if c == nil || len(c.TableLocations) == 0 {
		return &PlanLocationInfo{PlanType: PhyPlanLocal, Reason: "no table location info"}
	}
	resolver := GetLocationResolver()
	if resolver == nil {
		return &PlanLocationInfo{PlanType: PhyPlanLocal, Reason: "no location resolver"}
	}

	// For read-only queries, PointGet/BatchPointGet and simple index-lookup pushdown are usually executed locally
	// because KV access is already efficient. For DML statements, PointGet can still be beneficial to forward
	// (e.g. reducing commit RPCs).
	if !ignoreLocalPreference && !c.StatementTraits.IsDML &&
		(c.StatementTraits.HasPointGet || c.StatementTraits.HasIndexLookup) &&
		len(c.TableLocations) == 1 {
		return &PlanLocationInfo{PlanType: PhyPlanLocal, Reason: "point get/index lookup - KV access is efficient"}
	}

	storeAddrs := make(map[string]bool)
	for _, loc := range c.TableLocations {
		if !loc.IsPartitioned {
			addr := resolver.ResolveTableLocation(loc.TableID)
			if addr != "" {
				storeAddrs[addr] = true
			}
			continue
		}
		partIDs, allParts, err := loc.CalculatePartitionIDs(ctx, is, params)
		if err != nil {
			return &PlanLocationInfo{PlanType: PhyPlanUncertain, Reason: "error: " + err.Error()}
		}
		logutil.BgLogger().Debug("DetermineLocationWithParams",
			zap.Int64("tableID", loc.TableID),
			zap.Bool("isPartitioned", loc.IsPartitioned),
			zap.Int64s("partIDs", partIDs),
			zap.Bool("allParts", allParts),
		)
		for _, partID := range partIDs {
			addr := resolver.ResolvePartitionLocation(loc.TableID, partID)
			if addr != "" {
				storeAddrs[addr] = true
				if len(storeAddrs) > 1 {
					return &PlanLocationInfo{PlanType: PhyPlanDistributed, Reason: "multiple stores"}
				}
			}
		}
	}
	if len(storeAddrs) == 0 {
		return &PlanLocationInfo{PlanType: PhyPlanLocal, Reason: "no store addresses"}
	}
	if len(storeAddrs) == 1 {
		var targetStore string
		for addr := range storeAddrs {
			targetStore = addr
			break
		}
		if resolver.IsLocalStore(targetStore) {
			return &PlanLocationInfo{PlanType: PhyPlanLocal, TargetStore: targetStore, Reason: "local"}
		}
		return &PlanLocationInfo{PlanType: PhyPlanRemote, TargetStore: targetStore, Reason: "remote"}
	}
	return &PlanLocationInfo{PlanType: PhyPlanDistributed, Reason: "multiple stores"}
}

// ExtractTableLocationInfo extracts location info from a physical plan.
func ExtractTableLocationInfo(plan base.Plan) *CachedPlanLocationInfo {
	info := &CachedPlanLocationInfo{TableLocations: make([]*TableLocationInfo, 0)}
	extractLocFromPlanTree(plan, info)
	return info
}

func extractLocFromPlanTree(plan base.Plan, info *CachedPlanLocationInfo) {
	if plan == nil {
		return
	}
	switch p := plan.(type) {
	case *PhysicalTableReader:
		if loc := buildLocFromTableReader(p); loc != nil {
			info.TableLocations = append(info.TableLocations, loc)
			if loc.IsPartitioned {
				info.HasPartitionTable = true
			}
		}
	case *PhysicalIndexReader:
		if loc := buildLocFromIndexReader(p); loc != nil {
			info.TableLocations = append(info.TableLocations, loc)
			if loc.IsPartitioned {
				info.HasPartitionTable = true
			}
		}
	case *PhysicalIndexLookUpReader:
		if loc := buildLocFromIndexLookUpReader(p); loc != nil {
			info.TableLocations = append(info.TableLocations, loc)
			if loc.IsPartitioned {
				info.HasPartitionTable = true
			}
			if loc.IsIndexLookupPushDown {
				info.StatementTraits.HasIndexLookup = true
			}
		}
	case *PhysicalIndexMergeReader:
		if loc := buildLocFromIndexMergeReader(p); loc != nil {
			info.TableLocations = append(info.TableLocations, loc)
			if loc.IsPartitioned {
				info.HasPartitionTable = true
			}
		}
	case *PointGetPlan:
		if loc := buildLocFromPointGet(p); loc != nil {
			info.TableLocations = append(info.TableLocations, loc)
			info.StatementTraits.HasPointGet = true // Mark that this plan has PointGet/BatchPointGet
			if loc.IsPartitioned {
				info.HasPartitionTable = true
			}
		}
	case *BatchPointGetPlan:
		if loc := buildLocFromBatchPointGet(p); loc != nil {
			info.TableLocations = append(info.TableLocations, loc)
			info.StatementTraits.HasPointGet = true // Mark that this plan has PointGet/BatchPointGet
			if loc.IsPartitioned {
				info.HasPartitionTable = true
			}
		}
	// Handle DML statements: Update and Delete
	case *Update:
		info.StatementTraits.IsDML = true
		info.StatementTraits.DMLType = DMLTypeUpdate
		// Extract location info from the SelectPlan (the read part of UPDATE)
		// UPDATE's SelectPlan contains the TableReader/IndexReader that reads the rows to be updated
		// The partition pruning conditions are in the SelectPlan's TableScan/IndexScan
		if p.SelectPlan != nil {
			extractLocFromPlanTree(p.SelectPlan, info)
		}
		return // Don't recurse into children again
	case *Delete:
		info.StatementTraits.IsDML = true
		info.StatementTraits.DMLType = DMLTypeDelete
		// Extract location info from the SelectPlan (the read part of DELETE)
		// DELETE's SelectPlan contains the TableReader/IndexReader that reads the rows to be deleted
		// The partition pruning conditions are in the SelectPlan's TableScan/IndexScan
		if p.SelectPlan != nil {
			extractLocFromPlanTree(p.SelectPlan, info)
		}
		return // Don't recurse into children again
	case *Insert:
		info.StatementTraits.IsDML = true
		info.StatementTraits.DMLType = DMLTypeInsert
		// For INSERT ... SELECT, extract from SelectPlan
		if p.SelectPlan != nil {
			extractLocFromPlanTree(p.SelectPlan, info)
		} else if p.Table != nil {
			// For INSERT VALUES, extract table location info
			// For non-partitioned tables, we can directly determine the location
			// For partitioned tables, partition pruning requires runtime evaluation
			if loc := buildLocFromInsert(p); loc != nil {
				info.TableLocations = append(info.TableLocations, loc)
				if loc.IsPartitioned {
					info.HasPartitionTable = true
				}
			}
		}
		return // Don't recurse into children again
	case *Execute:
		// For COM_QUERY execute, extract executed plan
		extractLocFromPlanTree(p.Plan, info)
	}
	if physPlan, ok := plan.(base.PhysicalPlan); ok {
		for _, child := range physPlan.Children() {
			extractLocFromPlanTree(child, info)
		}
	}
}

// buildLocFromTableInfo is a helper function that builds TableLocationInfo from table metadata.
// This extracts the common logic from buildLocFromTableReader, buildLocFromIndexReader,
// buildLocFromIndexLookUpReader, and buildLocFromIndexMergeReader.
func buildLocFromTableInfo(tableInfo *model.TableInfo, dbName string, planPartInfo *PhysPlanPartInfo) *TableLocationInfo {
	if tableInfo == nil {
		return nil
	}
	loc := &TableLocationInfo{
		TableID:       tableInfo.ID,
		DBName:        dbName,
		TableName:     tableInfo.Name.L,
		IsPartitioned: tableInfo.Partition != nil,
	}
	if loc.IsPartitioned && tableInfo.Partition != nil {
		pi := tableInfo.Partition
		loc.PartitionIDs = make([]int64, len(pi.Definitions))
		for i, def := range pi.Definitions {
			loc.PartitionIDs[i] = def.ID
		}
		if planPartInfo != nil {
			loc.PlanPartInfo = planPartInfo.cloneForPlanCache()
		}
	}
	return loc
}

func buildLocFromTableReader(p *PhysicalTableReader) *TableLocationInfo {
	tableScans := p.GetTableScans()
	if len(tableScans) == 0 {
		return nil
	}
	ts := tableScans[0]
	return buildLocFromTableInfo(ts.Table, ts.DBName.L, p.PlanPartInfo)
}

func buildLocFromIndexReader(p *PhysicalIndexReader) *TableLocationInfo {
	if len(p.IndexPlans) == 0 {
		return nil
	}
	is, ok := p.IndexPlans[0].(*PhysicalIndexScan)
	if !ok {
		return nil
	}
	return buildLocFromTableInfo(is.Table, is.DBName.L, p.PlanPartInfo)
}

func buildLocFromIndexLookUpReader(p *PhysicalIndexLookUpReader) *TableLocationInfo {
	if len(p.TablePlans) == 0 {
		return nil
	}
	ts, ok := p.TablePlans[0].(*PhysicalTableScan)
	if !ok {
		return nil
	}
	loc := buildLocFromTableInfo(ts.Table, ts.DBName.L, p.PlanPartInfo)
	loc.IsIndexLookupPushDown = p.IndexLookUpPushDown
	return loc
}

func buildLocFromIndexMergeReader(p *PhysicalIndexMergeReader) *TableLocationInfo {
	if len(p.TablePlans) == 0 {
		return nil
	}
	ts, ok := p.TablePlans[0].(*PhysicalTableScan)
	if !ok {
		return nil
	}
	return buildLocFromTableInfo(ts.Table, ts.DBName.L, p.PlanPartInfo)
}

// buildLocFromPointGet builds location info for PointGet plan.
// PointGet plans are marked as IsPointGet=true. For read-only queries, these plans are usually executed locally
// because KV access is already very efficient; for DML statements, forwarding can still be beneficial.
func buildLocFromPointGet(p *PointGetPlan) *TableLocationInfo {
	if p.TblInfo == nil {
		return nil
	}
	loc := &TableLocationInfo{
		TableID:       p.TblInfo.ID,
		DBName:        p.dbName,
		TableName:     p.TblInfo.Name.L,
		IsPartitioned: p.TblInfo.Partition != nil,
		IsPointGet:    true,
	}
	// Skip partition-based forwarding decision for global index reads.
	if p.IndexInfo != nil && p.IndexInfo.Global {
		loc.IsPartitioned = false
		return loc
	}
	if !loc.IsPartitioned || p.TblInfo.Partition == nil {
		return loc
	}

	pi := p.TblInfo.Partition
	loc.PartitionIDs = make([]int64, len(pi.Definitions))
	for i, def := range pi.Definitions {
		loc.PartitionIDs[i] = def.ID
	}

	// Extract enough information to locate the target partition using parameters.
	info := &PartitionByRowInfo{PartitionNames: p.PartitionNames}
	if p.IndexInfo == nil {
		// Handle-based PointGet.
		d := types.NewIntDatum(p.Handle.IntValue())
		if p.UnsignedHandle && d.GetInt64() >= 0 {
			d = types.NewUintDatum(uint64(d.GetInt64()))
		}
		paramOrder := -1
		if p.HandleConstant != nil && p.HandleConstant.ParamMarker != nil {
			paramOrder = p.HandleConstant.ParamMarker.Order()
		}
		info.HandleColOffset = p.HandleColOffset
		info.HandleValues = []types.Datum{d}
		info.HandleParamOrders = []int{paramOrder}
	} else {
		// Index-based PointGet.
		cols := len(p.IndexInfo.Columns)
		offsets := make([]int, 0, cols)
		values := make([]types.Datum, 0, cols)
		orders := make([]int, 0, cols)
		for i, col := range p.IndexInfo.Columns {
			offsets = append(offsets, col.Offset)
			if i < len(p.IndexValues) {
				values = append(values, p.IndexValues[i])
			} else {
				values = append(values, types.Datum{})
			}
			order := -1
			if i < len(p.IndexConstants) && p.IndexConstants[i] != nil && p.IndexConstants[i].ParamMarker != nil {
				order = p.IndexConstants[i].ParamMarker.Order()
			}
			orders = append(orders, order)
		}
		info.IndexColOffsets = offsets
		info.IndexValues = [][]types.Datum{values}
		info.IndexValueParamOrders = [][]int{orders}
	}
	loc.PartitionByRowInfo = info
	return loc
}

// buildLocFromBatchPointGet builds location info for BatchPointGet plan.
// BatchPointGet plans are marked as IsPointGet=true. For read-only queries, these plans are usually executed locally
// because KV access is already very efficient with batching; for DML statements, forwarding can still be beneficial.
func buildLocFromBatchPointGet(p *BatchPointGetPlan) *TableLocationInfo {
	if p.TblInfo == nil {
		return nil
	}
	loc := &TableLocationInfo{
		TableID:       p.TblInfo.ID,
		DBName:        p.dbName,
		TableName:     p.TblInfo.Name.L,
		IsPartitioned: p.TblInfo.Partition != nil,
		IsPointGet:    true,
	}
	// Skip partition-based forwarding decision for global index reads.
	if p.IndexInfo != nil && p.IndexInfo.Global {
		loc.IsPartitioned = false
		return loc
	}
	if !loc.IsPartitioned || p.TblInfo.Partition == nil {
		return loc
	}

	pi := p.TblInfo.Partition
	loc.PartitionIDs = make([]int64, len(pi.Definitions))
	for i, def := range pi.Definitions {
		loc.PartitionIDs[i] = def.ID
	}

	info := &PartitionByRowInfo{PartitionNames: p.PartitionNames}
	if p.IndexInfo == nil {
		// Handle-based BatchPointGet.
		unsigned := false
		if p.HandleColOffset >= 0 && p.HandleColOffset < len(p.TblInfo.Columns) {
			unsigned = mysql.HasUnsignedFlag(p.TblInfo.Columns[p.HandleColOffset].GetFlag())
		}
		handleValues := make([]types.Datum, 0, len(p.Handles))
		handleOrders := make([]int, 0, len(p.Handles))
		for i, h := range p.Handles {
			d := types.NewIntDatum(h.IntValue())
			if unsigned && d.GetInt64() >= 0 {
				d = types.NewUintDatum(uint64(d.GetInt64()))
			}
			handleValues = append(handleValues, d)

			order := -1
			if i < len(p.HandleParams) && p.HandleParams[i] != nil && p.HandleParams[i].ParamMarker != nil {
				order = p.HandleParams[i].ParamMarker.Order()
			}
			handleOrders = append(handleOrders, order)
		}
		info.HandleColOffset = p.HandleColOffset
		info.HandleValues = handleValues
		info.HandleParamOrders = handleOrders
	} else {
		// Index-based BatchPointGet.
		cols := len(p.IndexInfo.Columns)
		offsets := make([]int, 0, cols)
		for _, col := range p.IndexInfo.Columns {
			offsets = append(offsets, col.Offset)
		}
		indexValues := make([][]types.Datum, len(p.IndexValues))
		for i := range p.IndexValues {
			indexValues[i] = append([]types.Datum(nil), p.IndexValues[i]...)
		}
		orders := make([][]int, 0, len(p.IndexValues))
		for rowIdx := range p.IndexValues {
			rowOrders := make([]int, 0, cols)
			for colIdx := 0; colIdx < cols; colIdx++ {
				order := -1
				if rowIdx < len(p.IndexValueParams) && colIdx < len(p.IndexValueParams[rowIdx]) {
					if c := p.IndexValueParams[rowIdx][colIdx]; c != nil && c.ParamMarker != nil {
						order = c.ParamMarker.Order()
					}
				}
				rowOrders = append(rowOrders, order)
			}
			orders = append(orders, rowOrders)
		}
		info.IndexColOffsets = offsets
		info.IndexValues = indexValues
		info.IndexValueParamOrders = orders
	}
	loc.PartitionByRowInfo = info
	return loc
}

// buildLocFromInsert builds location info for Insert plan.
// For non-partitioned tables, we can directly determine the location.
// For partitioned tables, we try to extract enough information from VALUES expressions
// (constants/parameters) to locate the target partitions. If we can't do so safely, we
// fall back to scanning all partitions when determining the target store.
func buildLocFromInsert(p *Insert) *TableLocationInfo {
	if p.Table == nil {
		return nil
	}
	tblMeta := p.Table.Meta()
	if tblMeta == nil {
		return nil
	}
	loc := &TableLocationInfo{
		TableID:       tblMeta.ID,
		TableName:     tblMeta.Name.L,
		IsPartitioned: tblMeta.Partition != nil,
	}
	// Try to get DB name from OutputNames
	if names := p.OutputNames(); len(names) > 0 && names[0] != nil {
		loc.DBName = names[0].DBName.L
	}

	if !loc.IsPartitioned || tblMeta.Partition == nil {
		return loc
	}

	pt, ok := p.Table.(table.PartitionedTable)
	if !ok || pt == nil {
		return loc
	}
	loc.PartitionIDs = pt.GetAllPartitionIDs()

	if len(p.Lists) == 0 {
		return loc
	}

	// Build a mapping from VALUES expressions to partition columns so we can locate the target partitions
	// without full planning on subsequent executions.
	tableCols := pt.Cols()
	var cols []*table.Column
	if len(p.Columns) > 0 {
		names := make([]string, 0, len(p.Columns))
		for _, v := range p.Columns {
			names = append(names, v.Name.L)
		}
		// missingColIdx can only happen if the plan is invalid; be conservative and skip pruning.
		var missingColIdx int
		cols, missingColIdx = table.FindColumns(tableCols, names, tblMeta.PKIsHandle)
		if missingColIdx >= 0 {
			return loc
		}
	} else {
		cols = tableCols
	}

	insertCols := make([]*table.Column, 0, len(cols))
	for _, col := range cols {
		if !col.IsGenerated() {
			insertCols = append(insertCols, col)
		}
	}

	partColIDs := pt.GetPartitionColumnIDs()
	if len(partColIDs) == 0 || len(insertCols) == 0 {
		return loc
	}

	idToOffset := make(map[int64]int, len(tableCols))
	for _, col := range tableCols {
		idToOffset[col.ID] = col.Offset
	}
	partOffsets := make([]int, 0, len(partColIDs))
	for _, colID := range partColIDs {
		offset, ok := idToOffset[colID]
		if !ok {
			return loc
		}
		partOffsets = append(partOffsets, offset)
	}

	offsetToInsertIdx := make(map[int]int, len(insertCols))
	for i, col := range insertCols {
		offsetToInsertIdx[col.Offset] = i
	}
	partExprIdx := make([]int, 0, len(partOffsets))
	for _, offset := range partOffsets {
		idx, ok := offsetToInsertIdx[offset]
		if !ok {
			// Partition columns not fully provided in VALUES; fallback to scanning all partitions.
			return loc
		}
		partExprIdx = append(partExprIdx, idx)
	}

	info := &PartitionByRowInfo{
		IndexColOffsets:       partOffsets,
		IndexValues:           make([][]types.Datum, 0, len(p.Lists)),
		IndexValueParamOrders: make([][]int, 0, len(p.Lists)),
	}
	for _, list := range p.Lists {
		// The VALUES list is aligned with insertCols (non-generated columns).
		if len(list) < len(insertCols) {
			return loc
		}
		vals := make([]types.Datum, 0, len(partExprIdx))
		orders := make([]int, 0, len(partExprIdx))
		for _, exprIdx := range partExprIdx {
			expr := list[exprIdx]
			con, ok := expr.(*expression.Constant)
			if !ok || con == nil {
				// Unsupported expression for pruning; be conservative.
				return loc
			}
			if con.ParamMarker != nil {
				vals = append(vals, types.Datum{})
				orders = append(orders, con.ParamMarker.Order())
				continue
			}
			vals = append(vals, con.Value)
			orders = append(orders, -1)
		}
		info.IndexValues = append(info.IndexValues, vals)
		info.IndexValueParamOrders = append(info.IndexValueParamOrders, orders)
	}
	if len(info.IndexValues) > 0 {
		loc.PartitionByRowInfo = info
	}

	return loc
}
