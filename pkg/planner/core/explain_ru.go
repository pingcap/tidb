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

package core

import (
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/stmtsummary"
	rmclient "github.com/tikv/pd/client/resource_group/controller"
)

type explainRUStatus string

const (
	explainRUStatusSuccess                  explainRUStatus                  = "success"
	explainRUStatusUnsupportedNonAnalyze    explainRUStatus                  = "unsupported_non_analyze"
	explainRUStatusUnsupportedNonSelect     explainRUStatus                  = "unsupported_non_select"
	explainRUStatusUnsupportedSideEffecting explainRUStatus                  = "unsupported_side_effecting_select"
	explainRUStatusUnsupportedLockingSelect explainRUStatus                  = "unsupported_locking_select"
	explainRUStatusUnsupportedForConnection explainRUStatus                  = "unsupported_for_connection"
	explainRUStatusError                    explainRUStatus                  = "error"
	explainRUComponentSnapshotOK            explainRUComponentSnapshotStatus = "ok"
	explainRUComponentSnapshotMissing       explainRUComponentSnapshotStatus = "missing"
	explainRUComponentSnapshotNonV2         explainRUComponentSnapshotStatus = "non_v2"
	explainRUComponentSnapshotNilMetrics    explainRUComponentSnapshotStatus = "nil_metrics"
	explainRUComponentSnapshotBypassed      explainRUComponentSnapshotStatus = "bypassed"
	explainRUSectionSummary                                                  = "summary"
	explainRUSectionPlan                                                     = "plan"
	explainRUSourceSummaryTotal                                              = "summary_total"

	explainRUWidthSourceRuntimeChunkAvg         = "runtime_chunk_avg"
	explainRUWidthSourceScanDetailProcessedAvg  = "scan_detail_processed_key_avg"
	explainRUWidthSourceNotApplicable           = "not_applicable"
	readBillingDemoModelVersion                 = "v2"
	readBillingDemoWeightVersion                = "v1"
	readBillingDemoStatusSuccess                = "success"
	readBillingDemoStatusUnsupported            = "unsupported"
	readBillingDemoStatusUnknownInput           = "unknown_input"
	readBillingDemoStatusError                  = "error"
	readBillingDemoStatusOperatorOK             = "ok"
	readBillingDemoReasonNone                   = "none"
	readBillingDemoReasonStatementError         = "statement_error"
	readBillingDemoReasonMissingPlan            = "missing_plan"
	readBillingDemoReasonMissingRuntimeStats    = "missing_runtime_stats"
	readBillingDemoReasonMissingRuntimeRows     = "missing_runtime_rows"
	readBillingDemoReasonMissingRuntimeBytes    = "missing_runtime_bytes"
	readBillingDemoReasonMissingInputBytes      = "missing_input_bytes"
	readBillingDemoReasonMissingScanDetail      = "missing_scan_detail"
	readBillingDemoReasonUnsupportedOperator    = "unsupported_operator"
	readBillingDemoReasonUnsupportedTiFlash     = "unsupported_tiflash"
	readBillingDemoReasonUnsupportedMPP         = "unsupported_mpp"
	readBillingDemoReasonUnsupportedIndexMerge  = "unsupported_index_merge"
	readBillingDemoReasonUnsupportedLock        = "unsupported_lock"
	readBillingDemoReasonNonBillable            = "non_billable"
	readBillingDemoSiteStatement                = "statement"
	readBillingDemoSiteTiDB                     = "tidb"
	readBillingDemoSiteTiKV                     = "tikv"
	readBillingDemoOpClassStatement             = "statement"
	readBillingDemoOpClassFilter                = "filter_eval"
	readBillingDemoOpClassProjection            = "projection_eval"
	readBillingDemoOpClassLimit                 = "row_limit"
	readBillingDemoOpClassTopN                  = "bounded_topn"
	readBillingDemoOpClassSort                  = "full_ordering"
	readBillingDemoOpClassWindow                = "window_eval"
	readBillingDemoOpClassHashAgg               = "agg_hash"
	readBillingDemoOpClassStreamAgg             = "agg_stream"
	readBillingDemoOpClassHashJoin              = "join_hash"
	readBillingDemoOpClassMergeJoin             = "join_merge"
	readBillingDemoOpClassLookupJoin            = "join_lookup"
	readBillingDemoOpClassReaderReceive         = "reader_receive"
	readBillingDemoOpClassLookupReader          = "lookup_reader"
	readBillingDemoOpClassOverlayReader         = "overlay_reader"
	readBillingDemoOpClassMetadataReader        = "metadata_reader"
	readBillingDemoOpClassPointLookup           = "kv_point_lookup"
	readBillingDemoOpClassRangeScan             = "kv_range_scan"
	readBillingDemoOpClassWrapper               = "wrapper"
	readBillingDemoOpClassSynthetic             = "synthetic_source"
	readBillingDemoOperatorStatement            = "statement"
	readBillingDemoUnitFixedEvents              = "fixed_events"
	readBillingDemoUnitInputRows                = "input_rows"
	readBillingDemoUnitInputBytes               = "input_bytes"
	readBillingDemoInputSourceRuntimeChunkBytes = "runtime_chunk_bytes"
	readBillingDemoInputSourceScanDetail        = "scan_detail"
	readBillingDemoInputSideAll                 = "all"
	readBillingDemoInputSideBuild               = "build"
	readBillingDemoInputSideProbe               = "probe"
	readBillingDemoInputSideLeft                = "left"
	readBillingDemoInputSideRight               = "right"
)

type explainRUComponentSnapshotStatus string

type readBillingDemoUnit struct {
	unit        string
	source      string
	side        string
	value       float64
	rowWidth    float64
	widthSource string
}

type readBillingDemoOperatorResult struct {
	id           string
	site         string
	opClass      string
	operatorKind string
	status       string
	reason       string
	actRows      int64
	hasActRows   bool
	units        []readBillingDemoUnit
}

type readBillingDemoResult struct {
	status    string
	reason    string
	operators []readBillingDemoOperatorResult
}

type readBillingDemoOperatorWeights struct {
	fixedEvent float64
	row        float64
	byte       float64
}

type readBillingDemoWeightKey struct {
	site    string
	opClass string
	version string
}

var readBillingDemoWeights = map[readBillingDemoWeightKey]readBillingDemoOperatorWeights{
	{readBillingDemoSiteTiKV, readBillingDemoOpClassRangeScan, readBillingDemoWeightVersion}:     {fixedEvent: 0.070, row: 0.000045, byte: 0.000020},
	{readBillingDemoSiteTiKV, readBillingDemoOpClassFilter, readBillingDemoWeightVersion}:        {fixedEvent: 0.020, row: 0.000040, byte: 0.000006},
	{readBillingDemoSiteTiKV, readBillingDemoOpClassProjection, readBillingDemoWeightVersion}:    {fixedEvent: 0.020, row: 0.000030, byte: 0.000006},
	{readBillingDemoSiteTiKV, readBillingDemoOpClassLimit, readBillingDemoWeightVersion}:         {fixedEvent: 0.010, row: 0.000008, byte: 0.000002},
	{readBillingDemoSiteTiKV, readBillingDemoOpClassTopN, readBillingDemoWeightVersion}:          {fixedEvent: 0.060, row: 0.000075, byte: 0.000012},
	{readBillingDemoSiteTiKV, readBillingDemoOpClassHashAgg, readBillingDemoWeightVersion}:       {fixedEvent: 0.080, row: 0.000100, byte: 0.000014},
	{readBillingDemoSiteTiKV, readBillingDemoOpClassStreamAgg, readBillingDemoWeightVersion}:     {fixedEvent: 0.060, row: 0.000065, byte: 0.000010},
	{readBillingDemoSiteTiKV, readBillingDemoOpClassPointLookup, readBillingDemoWeightVersion}:   {fixedEvent: 0.045, row: 0.000030, byte: 0.000012},
	{readBillingDemoSiteTiDB, readBillingDemoOpClassFilter, readBillingDemoWeightVersion}:        {fixedEvent: 0.020, row: 0.000030, byte: 0.000005},
	{readBillingDemoSiteTiDB, readBillingDemoOpClassProjection, readBillingDemoWeightVersion}:    {fixedEvent: 0.020, row: 0.000020, byte: 0.000004},
	{readBillingDemoSiteTiDB, readBillingDemoOpClassLimit, readBillingDemoWeightVersion}:         {fixedEvent: 0.010, row: 0.000006, byte: 0.000001},
	{readBillingDemoSiteTiDB, readBillingDemoOpClassTopN, readBillingDemoWeightVersion}:          {fixedEvent: 0.060, row: 0.000060, byte: 0.000010},
	{readBillingDemoSiteTiDB, readBillingDemoOpClassSort, readBillingDemoWeightVersion}:          {fixedEvent: 0.080, row: 0.000070, byte: 0.000012},
	{readBillingDemoSiteTiDB, readBillingDemoOpClassWindow, readBillingDemoWeightVersion}:        {fixedEvent: 0.070, row: 0.000070, byte: 0.000010},
	{readBillingDemoSiteTiDB, readBillingDemoOpClassHashAgg, readBillingDemoWeightVersion}:       {fixedEvent: 0.070, row: 0.000085, byte: 0.000012},
	{readBillingDemoSiteTiDB, readBillingDemoOpClassStreamAgg, readBillingDemoWeightVersion}:     {fixedEvent: 0.050, row: 0.000055, byte: 0.000008},
	{readBillingDemoSiteTiDB, readBillingDemoOpClassHashJoin, readBillingDemoWeightVersion}:      {fixedEvent: 0.110, row: 0.000115, byte: 0.000020},
	{readBillingDemoSiteTiDB, readBillingDemoOpClassMergeJoin, readBillingDemoWeightVersion}:     {fixedEvent: 0.090, row: 0.000075, byte: 0.000012},
	{readBillingDemoSiteTiDB, readBillingDemoOpClassLookupJoin, readBillingDemoWeightVersion}:    {fixedEvent: 0.120, row: 0.000120, byte: 0.000020},
	{readBillingDemoSiteTiDB, readBillingDemoOpClassReaderReceive, readBillingDemoWeightVersion}: {fixedEvent: 0.040, row: 0.000025, byte: 0.000014},
	{readBillingDemoSiteTiDB, readBillingDemoOpClassLookupReader, readBillingDemoWeightVersion}:  {fixedEvent: 0.070, row: 0.000045, byte: 0.000016},
	{readBillingDemoSiteTiDB, readBillingDemoOpClassOverlayReader, readBillingDemoWeightVersion}: {fixedEvent: 0.050, row: 0.000035, byte: 0.000012},
	{readBillingDemoSiteTiDB, readBillingDemoOpClassMetadataReader, readBillingDemoWeightVersion}: {
		fixedEvent: 0.020,
		row:        0.000008,
		byte:       0.000002,
	},
}

type explainRURow struct {
	section        string
	id             string
	component      string
	operatorClass  string
	actRows        int64
	hasActRows     bool
	inputRows      int64
	hasInputRows   bool
	outputRows     int64
	hasOutputRows  bool
	rowWidth       float64
	hasRowWidth    bool
	rowWidthSource string
	workRows       int64
	hasWorkRows    bool
	workBytes      float64
	hasWorkBytes   bool
	unit           string
	count          int64
	hasCount       bool
	weight         float64
	hasWeight      bool
	previewRU      float64
	hasPreviewRU   bool
	source         string
	note           string
}

// RecordReadBillingDemoForStatement emits coefficient-free read billing demo
// metrics for a completed statement and returns the structured statement
// summary stats. It is intentionally independent from RU v2 billing/reporting
// and never calls resource-control reporters.
func RecordReadBillingDemoForStatement(sctx sessionctx.Context, plan base.Plan, stmt ast.StmtNode, execErr error) stmtsummary.ReadBillingDemoStatementStats {
	if sctx == nil || sctx.GetSessionVars() == nil || !sctx.GetSessionVars().EnableReadBillingDemo {
		return stmtsummary.ReadBillingDemoStatementStats{}
	}
	// Restricted/internal SQL is not external workload calibration input.
	if sctx.GetSessionVars().InRestrictedSQL || sctx.GetSessionVars().StmtCtx.InRestrictedSQL {
		return stmtsummary.ReadBillingDemoStatementStats{}
	}
	planCtx := readBillingDemoPlanContext(plan)
	if planCtx == nil {
		planCtx = sctx.GetPlanCtx()
	}
	result := buildReadBillingDemoResult(planCtx, plan, stmt, execErr)
	recordReadBillingDemoResult(result)
	return buildReadBillingDemoStatementStats(result)
}

func buildReadBillingDemoResult(sctx base.PlanContext, plan base.Plan, stmt ast.StmtNode, execErr error) readBillingDemoResult {
	if execErr != nil {
		return readBillingDemoFailure(readBillingDemoStatusError, readBillingDemoReasonStatementError)
	}
	if plan == nil {
		return readBillingDemoFailure(readBillingDemoStatusUnknownInput, readBillingDemoReasonMissingPlan)
	}
	if gateStatus := explainRUSelectGateStatus(stmt); gateStatus != explainRUStatusSuccess {
		return readBillingDemoFailure(readBillingDemoStatusUnsupported, string(gateStatus))
	}
	if sctx == nil || sctx.GetSessionVars() == nil || sctx.GetSessionVars().StmtCtx.RuntimeStatsColl == nil {
		return readBillingDemoFailure(readBillingDemoStatusUnknownInput, readBillingDemoReasonMissingRuntimeStats)
	}
	flat := FlattenPhysicalPlan(plan, true)
	if flat == nil || len(flat.Main) == 0 || flat.InExplain || flat.InExecute {
		return readBillingDemoFailure(readBillingDemoStatusUnknownInput, readBillingDemoReasonMissingPlan)
	}

	result := readBillingDemoResult{
		status: readBillingDemoStatusSuccess,
		reason: readBillingDemoReasonNone,
	}
	planCtx := readBillingDemoPlanContext(plan)
	runtimeStats := sctx.GetSessionVars().StmtCtx.RuntimeStatsColl
	if status, op := appendReadBillingDemoTree(&result, planCtx, runtimeStats, flat.Main); status != readBillingDemoStatusSuccess {
		return readBillingDemoFailedOperator(status, op)
	}
	for _, tree := range flat.CTEs {
		if status, op := appendReadBillingDemoTree(&result, planCtx, runtimeStats, tree); status != readBillingDemoStatusSuccess {
			return readBillingDemoFailedOperator(status, op)
		}
	}
	for _, tree := range flat.ScalarSubQueries {
		if status, op := appendReadBillingDemoTree(&result, planCtx, runtimeStats, tree); status != readBillingDemoStatusSuccess {
			return readBillingDemoFailedOperator(status, op)
		}
	}
	return result
}

func readBillingDemoPlanContext(plan base.Plan) base.PlanContext {
	if plan == nil {
		return nil
	}
	return plan.SCtx()
}

func readBillingDemoResolveWeights(site, opClass, version string) (readBillingDemoOperatorWeights, bool) {
	weights, ok := readBillingDemoWeights[readBillingDemoWeightKey{site: site, opClass: opClass, version: version}]
	return weights, ok
}

func readBillingDemoUnitWeight(weights readBillingDemoOperatorWeights, unit string) (float64, bool) {
	switch unit {
	case readBillingDemoUnitFixedEvents:
		return weights.fixedEvent, true
	case readBillingDemoUnitInputRows:
		return weights.row, true
	case readBillingDemoUnitInputBytes:
		return weights.byte, true
	default:
		return 0, false
	}
}

func readBillingDemoUnitPreviewRU(unit readBillingDemoUnit, weights readBillingDemoOperatorWeights) (float64, float64, bool) {
	weight, ok := readBillingDemoUnitWeight(weights, unit.unit)
	if !ok {
		return 0, 0, false
	}
	return weight, unit.value * weight, true
}

func readBillingDemoFailure(status, reason string) readBillingDemoResult {
	return readBillingDemoResult{
		status: status,
		reason: reason,
		operators: []readBillingDemoOperatorResult{{
			site:         readBillingDemoSiteStatement,
			opClass:      readBillingDemoOpClassStatement,
			operatorKind: readBillingDemoOperatorStatement,
			status:       status,
			reason:       reason,
		}},
	}
}

func readBillingDemoFailedOperator(status string, op readBillingDemoOperatorResult) readBillingDemoResult {
	op.status = status
	if op.reason == "" {
		op.reason = readBillingDemoReasonUnsupportedOperator
	}
	return readBillingDemoResult{
		status:    status,
		reason:    op.reason,
		operators: []readBillingDemoOperatorResult{op},
	}
}

func summarizeReadBillingDemoBaseUnits(result readBillingDemoResult) stmtsummary.ReadBillingDemoBaseUnitSummary {
	if result.status != readBillingDemoStatusSuccess {
		return stmtsummary.ReadBillingDemoBaseUnitSummary{}
	}
	var summary stmtsummary.ReadBillingDemoBaseUnitSummary
	for _, op := range result.operators {
		if op.status != readBillingDemoStatusOperatorOK || !readBillingDemoOperatorBillable(op) {
			continue
		}
		for _, unit := range op.units {
			switch unit.unit {
			case readBillingDemoUnitFixedEvents:
				summary.SumReadBillingDemoFixedEvents += unit.value
			case readBillingDemoUnitInputRows:
				summary.SumReadBillingDemoInputRows += unit.value
			case readBillingDemoUnitInputBytes:
				summary.SumReadBillingDemoInputBytes += unit.value
			}
		}
	}
	return summary
}

func buildReadBillingDemoStatementStats(result readBillingDemoResult) stmtsummary.ReadBillingDemoStatementStats {
	stats := stmtsummary.ReadBillingDemoStatementStats{
		ModelVersion:  readBillingDemoModelVersion,
		WeightVersion: readBillingDemoWeightVersion,
	}
	status := result.status
	if status == "" {
		status = readBillingDemoStatusUnknownInput
	}
	reason := result.reason
	if reason == "" {
		reason = readBillingDemoReasonNone
	}
	stats.Statuses = append(stats.Statuses, stmtsummary.ReadBillingDemoStatusSample{
		ModelVersion:  readBillingDemoModelVersion,
		WeightVersion: readBillingDemoWeightVersion,
		Site:          readBillingDemoSiteStatement,
		OpClass:       readBillingDemoOpClassStatement,
		OperatorKind:  readBillingDemoOperatorStatement,
		Status:        status,
		Reason:        reason,
	})
	for _, op := range result.operators {
		opStatus := op.status
		if opStatus == "" {
			opStatus = status
		}
		opReason := op.reason
		if opReason == "" {
			opReason = reason
		}
		if opReason == "" {
			opReason = readBillingDemoReasonNone
		}
		if !(op.site == readBillingDemoSiteStatement &&
			op.opClass == readBillingDemoOpClassStatement &&
			op.operatorKind == readBillingDemoOperatorStatement &&
			opStatus == status &&
			opReason == reason) {
			stats.Statuses = append(stats.Statuses, stmtsummary.ReadBillingDemoStatusSample{
				ModelVersion:  readBillingDemoModelVersion,
				WeightVersion: readBillingDemoWeightVersion,
				Site:          op.site,
				OpClass:       op.opClass,
				OperatorKind:  op.operatorKind,
				Status:        opStatus,
				Reason:        opReason,
			})
		}
		if status != readBillingDemoStatusSuccess || opStatus != readBillingDemoStatusOperatorOK || !readBillingDemoOperatorBillable(op) {
			continue
		}
		for _, unit := range op.units {
			sample := stmtsummary.ReadBillingDemoBaseUnitSample{
				ModelVersion:   readBillingDemoModelVersion,
				WeightVersion:  readBillingDemoWeightVersion,
				Site:           op.site,
				OpClass:        op.opClass,
				OperatorKind:   op.operatorKind,
				Unit:           unit.unit,
				InputSource:    unit.source,
				InputSide:      unit.side,
				RowWidthSource: unit.widthSource,
				Value:          unit.value,
				RowWidth:       unit.rowWidth,
			}
			stats.BaseUnits = append(stats.BaseUnits, sample)
			switch unit.unit {
			case readBillingDemoUnitFixedEvents:
				stats.Totals.SumReadBillingDemoFixedEvents += unit.value
			case readBillingDemoUnitInputRows:
				stats.Totals.SumReadBillingDemoInputRows += unit.value
			case readBillingDemoUnitInputBytes:
				stats.Totals.SumReadBillingDemoInputBytes += unit.value
			}
		}
	}
	return stats
}

func appendReadBillingDemoTree(result *readBillingDemoResult, sctx base.PlanContext, runtimeStats *execdetails.RuntimeStatsColl, tree FlatPlanTree) (string, readBillingDemoOperatorResult) {
	for i, op := range tree {
		if op == nil || op.Origin == nil || op.Origin.ExplainID().String() == "_0" {
			continue
		}
		operator, supported, reason := readBillingDemoClassifyOperator(op)
		if !supported {
			return readBillingDemoStatusUnsupported, operator.withReason(reason)
		}
		operator.id = op.ExplainID().String()
		if actRows, ok := readBillingDemoOperatorActRows(runtimeStats, tree, i, op, operator); ok {
			operator.actRows = actRows
			operator.hasActRows = true
		}
		if !readBillingDemoOperatorBillable(operator) {
			operator.status = readBillingDemoStatusOperatorOK
			result.operators = append(result.operators, operator.withReason(readBillingDemoReasonNonBillable))
			continue
		}
		var units []readBillingDemoUnit
		var missingReason string
		var ok bool
		if op.IsRoot {
			units, missingReason, ok = readBillingDemoRootUnits(runtimeStats, tree, i, op, operator)
		} else {
			units, missingReason, ok = readBillingDemoCopUnits(runtimeStats, tree, i, operator)
		}
		if !ok {
			if missingReason == "" {
				if op.IsRoot {
					missingReason = readBillingDemoReasonMissingRuntimeBytes
				} else {
					missingReason = readBillingDemoReasonMissingScanDetail
				}
			}
			return readBillingDemoStatusUnknownInput, operator.withReason(missingReason)
		}
		operator.status = readBillingDemoStatusOperatorOK
		operator.reason = readBillingDemoReasonNone
		operator.units = units
		result.operators = append(result.operators, operator)
	}
	return readBillingDemoStatusSuccess, readBillingDemoOperatorResult{}
}

func (op readBillingDemoOperatorResult) withReason(reason string) readBillingDemoOperatorResult {
	op.reason = reason
	return op
}

func readBillingDemoOperatorBillable(op readBillingDemoOperatorResult) bool {
	return op.opClass != readBillingDemoOpClassWrapper && op.opClass != readBillingDemoOpClassSynthetic
}

func readBillingDemoOperatorActRows(runtimeStats *execdetails.RuntimeStatsColl, tree FlatPlanTree, idx int, op *FlatOperator, operator readBillingDemoOperatorResult) (int64, bool) {
	if op == nil {
		return 0, false
	}
	if op.IsRoot {
		return readBillingDemoPlanActRows(runtimeStats, op.Origin.ID())
	}
	if copStats := readBillingDemoCopStats(runtimeStats, tree, idx, operator.opClass); copStats != nil {
		return copStats.GetActRows(), true
	}
	return 0, false
}

func readBillingDemoClassifyOperator(op *FlatOperator) (readBillingDemoOperatorResult, bool, string) {
	operatorKind := strings.ToLower(op.Origin.TP())
	if !op.IsRoot {
		if op.StoreType == kv.TiFlash {
			return readBillingDemoOperatorResult{site: readBillingDemoSiteTiKV, opClass: readBillingDemoOpClassRangeScan, operatorKind: operatorKind}, false, readBillingDemoReasonUnsupportedTiFlash
		}
		switch op.Origin.TP() {
		case plancodec.TypeTableScan, plancodec.TypeIdxScan,
			plancodec.TypeTableFullScan, plancodec.TypeTableRangeScan, plancodec.TypeTableRowIDScan,
			plancodec.TypeIndexFullScan, plancodec.TypeIndexRangeScan:
			return readBillingDemoOperatorResult{site: readBillingDemoSiteTiKV, opClass: readBillingDemoOpClassRangeScan, operatorKind: operatorKind}, true, ""
		case plancodec.TypeSel:
			return readBillingDemoOperatorResult{site: readBillingDemoSiteTiKV, opClass: readBillingDemoOpClassFilter, operatorKind: operatorKind}, true, ""
		case plancodec.TypeProj:
			return readBillingDemoOperatorResult{site: readBillingDemoSiteTiKV, opClass: readBillingDemoOpClassProjection, operatorKind: operatorKind}, true, ""
		case plancodec.TypeLimit:
			return readBillingDemoOperatorResult{site: readBillingDemoSiteTiKV, opClass: readBillingDemoOpClassLimit, operatorKind: operatorKind}, true, ""
		case plancodec.TypeTopN:
			return readBillingDemoOperatorResult{site: readBillingDemoSiteTiKV, opClass: readBillingDemoOpClassTopN, operatorKind: operatorKind}, true, ""
		case plancodec.TypeHashAgg:
			return readBillingDemoOperatorResult{site: readBillingDemoSiteTiKV, opClass: readBillingDemoOpClassHashAgg, operatorKind: operatorKind}, true, ""
		case plancodec.TypeStreamAgg:
			return readBillingDemoOperatorResult{site: readBillingDemoSiteTiKV, opClass: readBillingDemoOpClassStreamAgg, operatorKind: operatorKind}, true, ""
		case plancodec.TypeSort, plancodec.TypeHashJoin, plancodec.TypeMergeJoin, plancodec.TypeIndexJoin:
			return readBillingDemoOperatorResult{site: readBillingDemoSiteTiKV, opClass: readBillingDemoOpClassRangeScan, operatorKind: operatorKind}, false, readBillingDemoReasonUnsupportedOperator
		default:
			return readBillingDemoOperatorResult{site: readBillingDemoSiteTiKV, opClass: readBillingDemoOpClassRangeScan, operatorKind: operatorKind}, false, readBillingDemoReasonUnsupportedOperator
		}
	}

	switch op.Origin.TP() {
	case plancodec.TypeExchangeReceiver, plancodec.TypeExchangeSender:
		return readBillingDemoOperatorResult{site: readBillingDemoSiteTiDB, opClass: readBillingDemoOpClassReaderReceive, operatorKind: operatorKind}, false, readBillingDemoReasonUnsupportedMPP
	case plancodec.TypeIndexMerge:
		return readBillingDemoOperatorResult{site: readBillingDemoSiteTiDB, opClass: readBillingDemoOpClassLookupReader, operatorKind: operatorKind}, false, readBillingDemoReasonUnsupportedIndexMerge
	case plancodec.TypeLock:
		return readBillingDemoOperatorResult{site: readBillingDemoSiteTiDB, opClass: readBillingDemoOpClassReaderReceive, operatorKind: operatorKind}, false, readBillingDemoReasonUnsupportedLock
	case plancodec.TypePointGet, plancodec.TypeBatchPointGet:
		return readBillingDemoOperatorResult{site: readBillingDemoSiteTiKV, opClass: readBillingDemoOpClassPointLookup, operatorKind: operatorKind}, true, ""
	case plancodec.TypeSel:
		return readBillingDemoOperatorResult{site: readBillingDemoSiteTiDB, opClass: readBillingDemoOpClassFilter, operatorKind: operatorKind}, true, ""
	case plancodec.TypeProj:
		return readBillingDemoOperatorResult{site: readBillingDemoSiteTiDB, opClass: readBillingDemoOpClassProjection, operatorKind: operatorKind}, true, ""
	case plancodec.TypeLimit:
		return readBillingDemoOperatorResult{site: readBillingDemoSiteTiDB, opClass: readBillingDemoOpClassLimit, operatorKind: operatorKind}, true, ""
	case plancodec.TypeTopN:
		return readBillingDemoOperatorResult{site: readBillingDemoSiteTiDB, opClass: readBillingDemoOpClassTopN, operatorKind: operatorKind}, true, ""
	case plancodec.TypeSort:
		return readBillingDemoOperatorResult{site: readBillingDemoSiteTiDB, opClass: readBillingDemoOpClassSort, operatorKind: operatorKind}, true, ""
	case plancodec.TypeWindow:
		return readBillingDemoOperatorResult{site: readBillingDemoSiteTiDB, opClass: readBillingDemoOpClassWindow, operatorKind: operatorKind}, true, ""
	case plancodec.TypeHashAgg:
		return readBillingDemoOperatorResult{site: readBillingDemoSiteTiDB, opClass: readBillingDemoOpClassHashAgg, operatorKind: operatorKind}, true, ""
	case plancodec.TypeStreamAgg:
		return readBillingDemoOperatorResult{site: readBillingDemoSiteTiDB, opClass: readBillingDemoOpClassStreamAgg, operatorKind: operatorKind}, true, ""
	case plancodec.TypeHashJoin:
		return readBillingDemoOperatorResult{site: readBillingDemoSiteTiDB, opClass: readBillingDemoOpClassHashJoin, operatorKind: operatorKind}, true, ""
	case plancodec.TypeMergeJoin:
		return readBillingDemoOperatorResult{site: readBillingDemoSiteTiDB, opClass: readBillingDemoOpClassMergeJoin, operatorKind: operatorKind}, true, ""
	case plancodec.TypeIndexJoin, plancodec.TypeIndexHashJoin, plancodec.TypeIndexMergeJoin:
		return readBillingDemoOperatorResult{site: readBillingDemoSiteTiDB, opClass: readBillingDemoOpClassLookupJoin, operatorKind: operatorKind}, true, ""
	case plancodec.TypeTableReader, plancodec.TypeIndexReader:
		return readBillingDemoOperatorResult{site: readBillingDemoSiteTiDB, opClass: readBillingDemoOpClassReaderReceive, operatorKind: operatorKind}, true, ""
	case plancodec.TypeIndexLookUp, plancodec.TypeLocalIndexLookUp:
		return readBillingDemoOperatorResult{site: readBillingDemoSiteTiDB, opClass: readBillingDemoOpClassLookupReader, operatorKind: operatorKind}, true, ""
	case plancodec.TypeUnionScan:
		return readBillingDemoOperatorResult{site: readBillingDemoSiteTiDB, opClass: readBillingDemoOpClassOverlayReader, operatorKind: operatorKind}, true, ""
	case plancodec.TypeMemTableScan, plancodec.TypeClusterMemTableReader:
		return readBillingDemoOperatorResult{site: readBillingDemoSiteTiDB, opClass: readBillingDemoOpClassMetadataReader, operatorKind: operatorKind}, true, ""
	case plancodec.TypeUnion:
		return readBillingDemoOperatorResult{site: readBillingDemoSiteTiDB, opClass: readBillingDemoOpClassWrapper, operatorKind: operatorKind}, true, ""
	case plancodec.TypeDual:
		return readBillingDemoOperatorResult{site: readBillingDemoSiteTiDB, opClass: readBillingDemoOpClassSynthetic, operatorKind: operatorKind}, true, ""
	default:
		return readBillingDemoOperatorResult{site: readBillingDemoSiteTiDB, opClass: readBillingDemoOpClassReaderReceive, operatorKind: operatorKind}, false, readBillingDemoReasonUnsupportedOperator
	}
}

func readBillingDemoRootUnits(runtimeStats *execdetails.RuntimeStatsColl, tree FlatPlanTree, idx int, op *FlatOperator, operator readBillingDemoOperatorResult) ([]readBillingDemoUnit, string, bool) {
	if _, _, ok := readBillingDemoRootOutputRowsAndBytes(runtimeStats, op.Origin.ID()); !ok {
		if _, rowsOK := readBillingDemoPlanActRows(runtimeStats, op.Origin.ID()); !rowsOK {
			return nil, readBillingDemoReasonMissingRuntimeRows, false
		}
		return nil, readBillingDemoReasonMissingRuntimeBytes, false
	}
	units := []readBillingDemoUnit{readBillingDemoFixedEventUnit(readBillingDemoInputSourceRuntimeChunkBytes)}
	switch operator.opClass {
	case readBillingDemoOpClassHashJoin:
		return appendReadBillingDemoJoinUnits(units, runtimeStats, tree, idx, true)
	case readBillingDemoOpClassMergeJoin, readBillingDemoOpClassLookupJoin:
		return appendReadBillingDemoJoinUnits(units, runtimeStats, tree, idx, false)
	default:
		inputRows, inputBytes, reason, ok := readBillingDemoDirectLocalInputRowsAndBytes(runtimeStats, tree, idx, operator.opClass)
		if !ok {
			return nil, reason, false
		}
		units = append(units, readBillingDemoRuntimeChunkInputUnits(inputRows, inputBytes, readBillingDemoInputSideAll)...)
		return units, "", true
	}
}

func readBillingDemoFixedEventUnit(inputSource string) readBillingDemoUnit {
	return readBillingDemoUnit{
		unit:        readBillingDemoUnitFixedEvents,
		source:      inputSource,
		side:        readBillingDemoInputSideAll,
		value:       1,
		widthSource: explainRUWidthSourceNotApplicable,
	}
}

func readBillingDemoRuntimeChunkInputUnits(rows, bytes int64, side string) []readBillingDemoUnit {
	rowWidth := readBillingDemoAverageRowWidth(rows, float64(bytes))
	return []readBillingDemoUnit{
		{unit: readBillingDemoUnitInputRows, source: readBillingDemoInputSourceRuntimeChunkBytes, side: side, value: float64(rows), rowWidth: rowWidth, widthSource: explainRUWidthSourceRuntimeChunkAvg},
		{unit: readBillingDemoUnitInputBytes, source: readBillingDemoInputSourceRuntimeChunkBytes, side: side, value: float64(bytes), rowWidth: rowWidth, widthSource: explainRUWidthSourceRuntimeChunkAvg},
	}
}

func readBillingDemoAverageRowWidth(rows int64, bytes float64) float64 {
	if rows <= 0 || bytes <= 0 {
		return 0
	}
	return bytes / float64(rows)
}

func readBillingDemoUseOutputRowsAsInput(opClass string) bool {
	switch opClass {
	case readBillingDemoOpClassReaderReceive, readBillingDemoOpClassLookupReader, readBillingDemoOpClassMetadataReader, readBillingDemoOpClassPointLookup:
		return true
	default:
		return false
	}
}

func appendReadBillingDemoJoinUnits(units []readBillingDemoUnit, runtimeStats *execdetails.RuntimeStatsColl, tree FlatPlanTree, idx int, useBuildProbe bool) ([]readBillingDemoUnit, string, bool) {
	for childOrder, childIdx := range tree[idx].ChildrenIdx {
		if childIdx < 0 || childIdx >= len(tree) || tree[childIdx] == nil || !tree[childIdx].IsRoot {
			continue
		}
		rows, bytes, ok := readBillingDemoRootOutputRowsAndBytes(runtimeStats, tree[childIdx].Origin.ID())
		if !ok {
			return nil, readBillingDemoReasonMissingInputBytes, false
		}
		side := readBillingDemoInputSideAll
		if useBuildProbe {
			switch tree[childIdx].Label {
			case BuildSide:
				side = readBillingDemoInputSideBuild
			case ProbeSide:
				side = readBillingDemoInputSideProbe
			default:
				if childOrder == 0 {
					side = readBillingDemoInputSideBuild
				} else {
					side = readBillingDemoInputSideProbe
				}
			}
		} else if childOrder == 0 {
			side = readBillingDemoInputSideLeft
		} else {
			side = readBillingDemoInputSideRight
		}
		units = append(units, readBillingDemoRuntimeChunkInputUnits(rows, bytes, side)...)
	}
	return units, "", true
}

func readBillingDemoDirectLocalInputRowsAndBytes(runtimeStats *execdetails.RuntimeStatsColl, tree FlatPlanTree, idx int, opClass string) (int64, int64, string, bool) {
	if idx < 0 || idx >= len(tree) || tree[idx] == nil {
		return 0, 0, "", true
	}
	if len(tree[idx].ChildrenIdx) == 0 || readBillingDemoUseOutputRowsAsInput(opClass) {
		rows, bytes, ok := readBillingDemoRootOutputRowsAndBytes(runtimeStats, tree[idx].Origin.ID())
		if !ok {
			return 0, 0, readBillingDemoReasonMissingRuntimeBytes, false
		}
		return rows, bytes, "", true
	}
	var rows, inputBytes int64
	for _, childIdx := range tree[idx].ChildrenIdx {
		if childIdx < 0 || childIdx >= len(tree) || tree[childIdx] == nil || !tree[childIdx].IsRoot {
			continue
		}
		childRows, childBytes, ok := readBillingDemoRootOutputRowsAndBytes(runtimeStats, tree[childIdx].Origin.ID())
		if !ok {
			return 0, 0, readBillingDemoReasonMissingInputBytes, false
		}
		rows += childRows
		inputBytes += childBytes
	}
	return rows, inputBytes, "", true
}

func readBillingDemoPlanActRows(runtimeStats *execdetails.RuntimeStatsColl, planID int) (int64, bool) {
	if runtimeStats == nil || !runtimeStats.ExistsRootStats(planID) {
		return 0, false
	}
	return runtimeStats.GetPlanActRows(planID), true
}

func readBillingDemoRootOutputRowsAndBytes(runtimeStats *execdetails.RuntimeStatsColl, planID int) (int64, int64, bool) {
	if runtimeStats == nil || !runtimeStats.ExistsRootStats(planID) {
		return 0, 0, false
	}
	basic := runtimeStats.GetBasicRuntimeStats(planID, false)
	if basic == nil || !basic.HasBytes() {
		return 0, 0, false
	}
	return basic.GetActRows(), basic.GetOutputBytes(), true
}

func readBillingDemoCopUnits(runtimeStats *execdetails.RuntimeStatsColl, tree FlatPlanTree, idx int, operator readBillingDemoOperatorResult) ([]readBillingDemoUnit, string, bool) {
	if operator.opClass != readBillingDemoOpClassRangeScan {
		return nil, readBillingDemoReasonMissingRuntimeBytes, false
	}
	copStats := readBillingDemoCopStats(runtimeStats, tree, idx, operator.opClass)
	if copStats == nil {
		return nil, readBillingDemoReasonMissingScanDetail, false
	}
	scanDetail := copStats.GetScanDetail()
	scanInputRows, scanInputBytes, ok := readBillingDemoRangeScanInput(scanDetail.TotalKeys, scanDetail.ProcessedKeys, scanDetail.ProcessedKeysSize)
	if !ok {
		return nil, readBillingDemoReasonMissingScanDetail, false
	}
	rowWidth := readBillingDemoAverageRowWidth(scanInputRows, scanInputBytes)
	units := []readBillingDemoUnit{readBillingDemoFixedEventUnit(readBillingDemoInputSourceScanDetail)}
	units = append(units,
		readBillingDemoUnit{unit: readBillingDemoUnitInputRows, source: readBillingDemoInputSourceScanDetail, side: readBillingDemoInputSideAll, value: float64(scanInputRows), rowWidth: rowWidth, widthSource: explainRUWidthSourceScanDetailProcessedAvg},
		readBillingDemoUnit{unit: readBillingDemoUnitInputBytes, source: readBillingDemoInputSourceScanDetail, side: readBillingDemoInputSideAll, value: scanInputBytes, rowWidth: rowWidth, widthSource: explainRUWidthSourceScanDetailProcessedAvg},
	)
	return units, "", true
}

func readBillingDemoRangeScanInput(totalKeys, processedKeys, processedKeysSize int64) (int64, float64, bool) {
	if totalKeys <= 0 || processedKeys <= 0 || processedKeysSize <= 0 {
		return 0, 0, false
	}
	inputBytes := float64(processedKeysSize) / float64(processedKeys) * float64(totalKeys)
	return totalKeys, inputBytes, true
}

func readBillingDemoCopStats(runtimeStats *execdetails.RuntimeStatsColl, tree FlatPlanTree, idx int, opClass string) *execdetails.CopRuntimeStats {
	if runtimeStats == nil || idx < 0 || idx >= len(tree) || tree[idx] == nil || tree[idx].Origin == nil {
		return nil
	}
	exact := runtimeStats.GetCopStats(tree[idx].Origin.ID())
	if readBillingDemoCopStatsUsable(exact, opClass) {
		return exact
	}
	// distsql may attach scan detail to the last cop plan ID in a task, not
	// necessarily to the scan node. Search the same cop subtree before failing.
	if end := tree[idx].ChildrenEndIdx; end > idx {
		if end >= len(tree) {
			end = len(tree) - 1
		}
		for i := end; i > idx; i-- {
			if tree[i] == nil || tree[i].IsRoot || tree[i].Origin == nil || tree[i].StoreType != tree[idx].StoreType {
				continue
			}
			if stats := runtimeStats.GetCopStats(tree[i].Origin.ID()); readBillingDemoCopStatsUsable(stats, opClass) {
				return stats
			}
		}
	}
	for i := idx - 1; i >= 0; i-- {
		if tree[i] == nil || tree[i].IsRoot || tree[i].Origin == nil || tree[i].StoreType != tree[idx].StoreType || tree[i].ChildrenEndIdx < idx {
			continue
		}
		if stats := runtimeStats.GetCopStats(tree[i].Origin.ID()); readBillingDemoCopStatsUsable(stats, opClass) {
			return stats
		}
	}
	return exact
}

func readBillingDemoCopStatsUsable(copStats *execdetails.CopRuntimeStats, opClass string) bool {
	if copStats == nil {
		return false
	}
	if opClass != readBillingDemoOpClassRangeScan {
		return true
	}
	scanDetail := copStats.GetScanDetail()
	return scanDetail.TotalKeys > 0 && scanDetail.ProcessedKeys > 0 && scanDetail.ProcessedKeysSize > 0
}

func recordReadBillingDemoResult(result readBillingDemoResult) {
	status := result.status
	if status == "" {
		status = readBillingDemoStatusUnknownInput
	}
	metrics.RecordReadBillingDemoStatement(status, readBillingDemoModelVersion)
	for _, op := range result.operators {
		opStatus := op.status
		if opStatus == "" {
			opStatus = status
		}
		reason := op.reason
		if reason == "" {
			reason = result.reason
		}
		if reason == "" {
			reason = readBillingDemoReasonNone
		}
		metrics.RecordReadBillingDemoOperatorStatus(op.site, op.opClass, op.operatorKind, opStatus, reason, readBillingDemoModelVersion)
		if status != readBillingDemoStatusSuccess {
			continue
		}
		for _, unit := range op.units {
			metrics.AddReadBillingDemoBaseUnits(op.site, op.opClass, op.operatorKind, unit.unit, unit.source, unit.side, readBillingDemoModelVersion, unit.value)
			metrics.ObserveReadBillingDemoRowWidth(op.site, op.opClass, op.operatorKind, unit.widthSource, readBillingDemoModelVersion, unit.rowWidth)
		}
	}
}

func explainRUError(status explainRUStatus) error {
	return errors.NewNoStackErrorf("EXPLAIN ANALYZE FORMAT='RU' is not supported for this target: %s", status)
}

func recordExplainRUStatus(status explainRUStatus) {
	metrics.RecordExplainRUStatus(string(status))
}

func (e *Explain) recordExplainRUStatus(status explainRUStatus) {
	if e == nil || e.ruStatusRecorded {
		return
	}
	e.ruStatusRecorded = true
	recordExplainRUStatus(status)
}

// explainRUSelectGateStatus is the first-demo pre-execution safety gate. It
// accepts only SELECT keyword surfaces and set operations whose leaves can be
// checked before EXPLAIN ANALYZE can run the target statement.
func explainRUSelectGateStatus(stmt ast.StmtNode) explainRUStatus {
	switch x := stmt.(type) {
	case *ast.SelectStmt:
		return explainRUValidateSelectNode(x)
	case *ast.SetOprStmt:
		if x == nil || x.SelectList == nil {
			return explainRUStatusUnsupportedNonSelect
		}
		return explainRUValidateSetOprSelectList(x.SelectList)
	default:
		return explainRUStatusUnsupportedNonSelect
	}
}

func explainRUValidateSetOprSelectList(list *ast.SetOprSelectList) explainRUStatus {
	if list == nil || len(list.Selects) == 0 {
		return explainRUStatusUnsupportedNonSelect
	}
	for _, sel := range list.Selects {
		switch x := sel.(type) {
		case *ast.SelectStmt:
			if status := explainRUValidateSelectNode(x); status != explainRUStatusSuccess {
				return status
			}
		case *ast.SetOprSelectList:
			if status := explainRUValidateSetOprSelectList(x); status != explainRUStatusSuccess {
				return status
			}
		default:
			// SetOprSelectList is documented as SELECT/TABLE/VALUES capable. Fail
			// closed until non-SELECT leaves have an explicit attribution design.
			return explainRUStatusUnsupportedNonSelect
		}
	}
	visitor := &explainRUSideEffectVisitor{status: explainRUStatusSuccess}
	list.Accept(visitor)
	return visitor.status
}

func explainRUValidateSelectNode(sel *ast.SelectStmt) explainRUStatus {
	if sel == nil || sel.Kind != ast.SelectStmtKindSelect {
		return explainRUStatusUnsupportedNonSelect
	}
	if sel.SelectIntoOpt != nil {
		return explainRUStatusUnsupportedSideEffecting
	}
	if sel.LockInfo != nil && sel.LockInfo.LockType != ast.SelectLockNone {
		return explainRUStatusUnsupportedLockingSelect
	}
	visitor := &explainRUSideEffectVisitor{status: explainRUStatusSuccess}
	sel.Accept(visitor)
	return visitor.status
}

type explainRUSideEffectVisitor struct {
	status explainRUStatus
}

func (v *explainRUSideEffectVisitor) Enter(n ast.Node) (ast.Node, bool) {
	if v.status != explainRUStatusSuccess {
		return n, true
	}
	switch x := n.(type) {
	case *ast.SelectStmt:
		if x.Kind != ast.SelectStmtKindSelect {
			v.status = explainRUStatusUnsupportedNonSelect
			return n, true
		}
		if x.SelectIntoOpt != nil {
			v.status = explainRUStatusUnsupportedSideEffecting
			return n, true
		}
		if x.LockInfo != nil && x.LockInfo.LockType != ast.SelectLockNone {
			v.status = explainRUStatusUnsupportedLockingSelect
			return n, true
		}
	case *ast.VariableExpr:
		// User-variable assignment is syntactically a SELECT expression but
		// mutates session state, so it is outside the side-effect-free demo scope.
		if x.Value != nil {
			v.status = explainRUStatusUnsupportedSideEffecting
			return n, true
		}
	case *ast.FuncCallExpr:
		if explainRUFuncCallHasSideEffect(x) {
			v.status = explainRUStatusUnsupportedSideEffecting
			return n, true
		}
	}
	return n, false
}

func (v *explainRUSideEffectVisitor) Leave(n ast.Node) (ast.Node, bool) {
	return n, v.status == explainRUStatusSuccess
}

func explainRUFuncCallHasSideEffect(fn *ast.FuncCallExpr) bool {
	if fn == nil {
		return false
	}
	switch strings.ToLower(fn.FnName.L) {
	case ast.GetLock, ast.ReleaseLock, ast.ReleaseAllLocks, ast.NextVal, ast.SetVal, ast.Sleep:
		return true
	case ast.LastInsertId:
		return len(fn.Args) > 0
	default:
		return false
	}
}

func (e *Explain) renderRUExplain() (err error) {
	start := time.Now()
	status := explainRUStatusError
	defer func() {
		metrics.ObserveExplainRURenderDuration(string(status), time.Since(start).Seconds())
		e.recordExplainRUStatus(status)
	}()

	if !e.Analyze {
		status = explainRUStatusUnsupportedNonAnalyze
		return explainRUError(explainRUStatusUnsupportedNonAnalyze)
	}
	if gateStatus := explainRUSelectGateStatus(e.ExecStmt); gateStatus != explainRUStatusSuccess {
		status = gateStatus
		return explainRUError(gateStatus)
	}
	flat := FlattenPhysicalPlan(e.TargetPlan, true)
	if flat == nil || len(flat.Main) == 0 || flat.InExplain {
		return errors.NewNoStackError("EXPLAIN ANALYZE FORMAT='RU' cannot render an empty target plan")
	}
	runtimeStats := e.RuntimeStatsColl
	if runtimeStats == nil && e.SCtx() != nil && e.SCtx().GetSessionVars() != nil {
		runtimeStats = e.SCtx().GetSessionVars().StmtCtx.RuntimeStatsColl
	}
	// The snapshot belongs to the target statement execution. Returning this
	// EXPLAIN result can add more result-chunk counters later, so render output
	// and Demo Metrics are derived from this frozen input and the generated rows.
	_, snapshotStatus := explainRUExtractComponentSnapshot(runtimeStats, e.TargetPlan.ID())
	metrics.RecordExplainRUComponentSnapshot(string(snapshotStatus))
	result := buildReadBillingDemoResult(e.SCtx(), e.TargetPlan, e.ExecStmt, nil)
	if result.status != readBillingDemoStatusSuccess {
		operator := ""
		if len(result.operators) > 0 {
			op := result.operators[0]
			operator = " operator=" + op.site + "/" + op.opClass + "/" + op.operatorKind
		}
		status = explainRUStatusError
		return errors.NewNoStackErrorf(
			"EXPLAIN ANALYZE FORMAT='RU' cannot render a complete read billing model result: status=%s reason=%s%s",
			result.status,
			result.reason,
			operator,
		)
	}
	rows := explainRUBuildReadBillingRows(result, snapshotStatus)

	e.Rows = make([][]string, 0, len(rows))
	for _, row := range rows {
		e.Rows = append(e.Rows, row.toStrings())
		explainRUObserveRow(row)
	}
	status = explainRUStatusSuccess
	return nil
}

func explainRUBuildReadBillingRows(result readBillingDemoResult, snapshotStatus explainRUComponentSnapshotStatus) []explainRURow {
	rows := []explainRURow{{
		section:      explainRUSectionSummary,
		component:    "total_preview_ru",
		hasPreviewRU: true,
		source:       explainRUSourceSummaryTotal,
		note:         explainRUReadBillingSummaryNote(snapshotStatus),
	}}
	totalPreviewRU := 0.0
	for _, op := range result.operators {
		if op.status != readBillingDemoStatusOperatorOK || !readBillingDemoOperatorBillable(op) {
			continue
		}
		weights, hasWeights := readBillingDemoResolveWeights(op.site, op.opClass, readBillingDemoWeightVersion)
		for _, unit := range op.units {
			row := explainRUReadBillingUnitRow(op, unit)
			if hasWeights {
				if weight, previewRU, ok := readBillingDemoUnitPreviewRU(unit, weights); ok {
					row.weight = weight
					row.hasWeight = true
					row.previewRU = previewRU
					row.hasPreviewRU = true
					totalPreviewRU += previewRU
				}
			} else {
				row.note = appendExplainRUNote(row.note, "missing_weight")
			}
			rows = append(rows, row)
		}
	}
	rows[0].previewRU = totalPreviewRU
	return rows
}

func explainRUReadBillingSummaryNote(snapshotStatus explainRUComponentSnapshotStatus) string {
	note := "weight_version=" + readBillingDemoWeightVersion
	if snapshotStatus != explainRUComponentSnapshotOK {
		note = appendExplainRUNote(note, "component_snapshot_"+string(snapshotStatus))
	}
	return note
}

func explainRUReadBillingUnitRow(op readBillingDemoOperatorResult, unit readBillingDemoUnit) explainRURow {
	row := explainRURow{
		section:        explainRUSectionPlan,
		id:             op.id,
		component:      op.operatorKind,
		operatorClass:  op.site + "/" + op.opClass,
		rowWidth:       unit.rowWidth,
		hasRowWidth:    unit.rowWidth > 0,
		rowWidthSource: unit.widthSource,
		unit:           unit.unit,
		source:         unit.source,
		note:           "input_side=" + unit.side + ",weight_version=" + readBillingDemoWeightVersion,
	}
	if op.hasActRows {
		row.actRows = op.actRows
		row.hasActRows = true
		row.outputRows = op.actRows
		row.hasOutputRows = true
	}
	switch unit.unit {
	case readBillingDemoUnitFixedEvents:
		row.count = int64(unit.value)
		row.hasCount = true
	case readBillingDemoUnitInputRows:
		row.inputRows = int64(unit.value)
		row.hasInputRows = true
		row.workRows = int64(unit.value)
		row.hasWorkRows = true
		row.count = int64(unit.value)
		row.hasCount = true
	case readBillingDemoUnitInputBytes:
		row.workBytes = unit.value
		row.hasWorkBytes = true
	}
	return row
}

func appendExplainRUNote(note, extra string) string {
	if note == "" {
		return extra
	}
	if extra == "" {
		return note
	}
	return note + "," + extra
}

func explainRUExtractComponentSnapshot(runtimeStats *execdetails.RuntimeStatsColl, targetPlanID int) (*execdetails.RURuntimeStats, explainRUComponentSnapshotStatus) {
	// GetRootStats creates an empty entry for a missing plan ID; check
	// ExistsRootStats first so "missing snapshot" stays observable.
	if runtimeStats == nil || !runtimeStats.ExistsRootStats(targetPlanID) {
		return nil, explainRUComponentSnapshotMissing
	}
	_, groups := runtimeStats.GetRootStats(targetPlanID).MergeStats()
	for _, group := range groups {
		ruStats, ok := group.(*execdetails.RURuntimeStats)
		if !ok {
			continue
		}
		if ruStats.RUVersion != rmclient.RUVersionV2 {
			return ruStats, explainRUComponentSnapshotNonV2
		}
		if ruStats.Metrics == nil {
			return ruStats, explainRUComponentSnapshotNilMetrics
		}
		if ruStats.Metrics.Bypass() {
			return ruStats, explainRUComponentSnapshotBypassed
		}
		return ruStats, explainRUComponentSnapshotOK
	}
	return nil, explainRUComponentSnapshotMissing
}

func (row explainRURow) toStrings() []string {
	return []string{
		row.section,
		row.id,
		row.component,
		row.operatorClass,
		formatOptionalInt(row.actRows, row.hasActRows),
		formatOptionalInt(row.inputRows, row.hasInputRows),
		formatOptionalInt(row.outputRows, row.hasOutputRows),
		formatOptionalFloat(row.rowWidth, row.hasRowWidth),
		row.rowWidthSource,
		formatOptionalInt(row.workRows, row.hasWorkRows),
		formatOptionalFloat(row.workBytes, row.hasWorkBytes),
		row.unit,
		formatOptionalInt(row.count, row.hasCount),
		formatOptionalFloat(row.weight, row.hasWeight),
		formatOptionalFloat(row.previewRU, row.hasPreviewRU),
		row.source,
		row.note,
	}
}

func formatOptionalInt(v int64, ok bool) string {
	if !ok {
		return ""
	}
	return strconv.FormatInt(v, 10)
}

func formatOptionalFloat(v float64, ok bool) string {
	if !ok {
		return ""
	}
	return strconv.FormatFloat(v, 'f', 6, 64)
}

func explainRUObserveRow(row explainRURow) {
	// Metrics are emitted from rendered rows so the Prometheus view matches the
	// SQL output and avoids reading live counters after render-side accounting.
	previewRU := -1.0
	if row.hasPreviewRU {
		previewRU = row.previewRU
	}
	workRows := -1.0
	if row.hasWorkRows {
		workRows = float64(row.workRows)
	}
	workBytes := -1.0
	if row.hasWorkBytes {
		workBytes = row.workBytes
	}
	rowWidth := -1.0
	if row.section == explainRUSectionPlan && row.hasRowWidth {
		rowWidth = row.rowWidth
	}
	component, operator := explainRUMetricComponentOperator(row)
	metrics.ObserveExplainRURow(row.section, component, operator, row.source, row.rowWidthSource, previewRU, workRows, workBytes, rowWidth)
}

func explainRUMetricComponentOperator(row explainRURow) (component, operator string) {
	switch row.section {
	case explainRUSectionPlan:
		return "", row.component
	default:
		return row.component, ""
	}
}

func explainRUUnsupportedFormatError(format string) error {
	return errors.Errorf("'explain format=%v' cannot work without 'analyze', please use 'explain analyze format=%v'", format, format)
}

func isExplainRUFormat(format string) bool {
	return strings.ToLower(format) == types.ExplainFormatRU
}
