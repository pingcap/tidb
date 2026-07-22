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
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/stmtsummary"
	tikvutil "github.com/tikv/client-go/v2/util"
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

	explainRUWidthSourceRuntimeChunkAvg               = "runtime_chunk_avg"
	explainRUWidthSourceRuntimeReaderOutputChunkAvg   = "runtime_reader_output_chunk_avg"
	explainRUWidthSourceScanDetailProcessedAvg        = "scan_detail_processed_key_avg"
	explainRUWidthSourceScanDetailProcessedEstimate   = "scan_detail_processed_key_avg_estimate"
	explainRUWidthSourceNotApplicable                 = "not_applicable"
	readBillingDemoModelVersion                       = "v4"
	readBillingDemoWeightVersion                      = "v3-resource-formula-uncalibrated"
	readBillingDemoStatusSuccess                      = "success"
	readBillingDemoStatusUnsupported                  = "unsupported"
	readBillingDemoStatusUnknownInput                 = "unknown_input"
	readBillingDemoStatusError                        = "error"
	readBillingDemoStatusOperatorOK                   = "ok"
	readBillingDemoStatusPartial                      = "partial"
	readBillingDemoReasonNone                         = "none"
	readBillingDemoReasonStatementError               = "statement_error"
	readBillingDemoReasonMissingPlan                  = "missing_plan"
	readBillingDemoReasonMissingRuntimeStats          = "missing_runtime_stats"
	readBillingDemoReasonMissingRuntimeRows           = "missing_runtime_rows"
	readBillingDemoReasonMissingRuntimeBytes          = "missing_runtime_bytes"
	readBillingDemoReasonMissingInputBytes            = "missing_input_bytes"
	readBillingDemoReasonMissingScanDetail            = "missing_scan_detail"
	readBillingDemoReasonUnsupportedOperator          = "unsupported_operator"
	readBillingDemoReasonUnsupportedTiFlash           = "unsupported_tiflash"
	readBillingDemoReasonUnsupportedMPP               = "unsupported_mpp"
	readBillingDemoReasonUnsupportedIndexMerge        = "unsupported_index_merge"
	readBillingDemoReasonUnsupportedLock              = "unsupported_lock"
	readBillingDemoReasonNonBillable                  = "non_billable"
	readBillingDemoReasonMissingCommitDetail          = "missing_commit_detail"
	readBillingDemoReasonMissingWriteKeys             = "missing_write_keys"
	readBillingDemoReasonMissingWriteByte             = "missing_write_byte"
	readBillingDemoReasonZeroMutation                 = "zero_mutation"
	readBillingDemoReasonMissingPrewriteRegion        = "missing_prewrite_region_num"
	readBillingDemoReasonMissingWriteRPCCount         = "missing_write_rpc_count"
	readBillingDemoReasonMissingMutationRecorder      = "missing_mutation_recorder"
	readBillingDemoReasonUncalibratedMutation         = "uncalibrated_mutation_weights"
	readBillingDemoReasonDMLAncillaryPartial          = "dml_ancillary_work_partial"
	readBillingDemoReasonPipelinedWritePartial        = "pipelined_tikv_payload_unsupported"
	readBillingDemoReasonOptimisticReplayPartial      = "optimistic_replay_attribution_unsupported"
	readBillingDemoReasonMissingCopChildRuntimeRows   = "missing_cop_child_runtime_rows"
	readBillingDemoReasonMissingScanWidthEvidence     = "missing_scan_width_evidence"
	readBillingDemoReasonAmbiguousCopScanWidth        = "ambiguous_cop_scan_width"
	readBillingDemoReasonUnsupportedCopMultiChild     = "unsupported_cop_multi_child"
	readBillingDemoReasonUnsupportedCopWidthTransform = "unsupported_cop_width_transform"
	readBillingDemoReasonUnsupportedCopStructure      = "unsupported_cop_structure"
	readBillingDemoReasonInvalidCopRuntimeRows        = "invalid_cop_runtime_rows"
	readBillingDemoReasonIncompleteCopRuntimeRows     = "incomplete_cop_runtime_rows"
	readBillingDemoReasonDependentCopInputUnavailable = "dependent_cop_input_unavailable"
	readBillingDemoReasonInvalidOrderingWork          = "invalid_ordering_work"
	readBillingDemoReasonMissingOrderingProjection    = "missing_ordering_projection"
	readBillingDemoReasonMissingExpressionCount       = "missing_expression_count"
	readBillingDemoReasonInvalidTopNBound             = "invalid_topn_bound"
	readBillingDemoReasonMissingHashStateRows         = "missing_hash_state_rows"
	readBillingDemoReasonInvalidHashStateRows         = "invalid_hash_state_rows"
	readBillingDemoReasonMissingReaderTransport       = "missing_reader_transport_details"
	readBillingDemoReasonAmbiguousReaderTransport     = "ambiguous_reader_transport_producers"
	readBillingDemoReasonUncalibratedWeights          = "uncalibrated_weights"
	readBillingDemoReasonUnsupportedStatement         = "unsupported_statement"
	readBillingDemoSiteStatement                      = "statement"
	readBillingDemoSiteTiDB                           = "tidb"
	readBillingDemoSiteTiKV                           = "tikv"
	readBillingDemoOpClassStatement                   = "statement"
	readBillingDemoOpClassFilter                      = "filter_eval"
	readBillingDemoOpClassProjection                  = "projection_eval"
	readBillingDemoOpClassLimit                       = "row_limit"
	readBillingDemoOpClassTopN                        = "bounded_topn"
	readBillingDemoOpClassSort                        = "full_ordering"
	readBillingDemoOpClassWindow                      = "window_eval"
	readBillingDemoOpClassHashAgg                     = "agg_hash"
	readBillingDemoOpClassStreamAgg                   = "agg_stream"
	readBillingDemoOpClassHashJoin                    = "join_hash"
	readBillingDemoOpClassMergeJoin                   = "join_merge"
	readBillingDemoOpClassLookupJoin                  = "join_lookup"
	readBillingDemoOpClassReaderReceive               = "reader_receive"
	readBillingDemoOpClassLookupReader                = "lookup_reader"
	readBillingDemoOpClassOverlayReader               = "overlay_reader"
	readBillingDemoOpClassMetadataReader              = "metadata_reader"
	readBillingDemoOpClassPointLookup                 = "kv_point_lookup"
	readBillingDemoOpClassRangeScan                   = "kv_range_scan"
	readBillingDemoOpClassKVMutation                  = "kv_mutation"
	readBillingDemoOpClassKVWrite                     = "kv_write"
	readBillingDemoOpClassReaderTransport             = "reader_transport"
	readBillingDemoOpClassWrapper                     = "wrapper"
	readBillingDemoOpClassSynthetic                   = "synthetic_source"
	readBillingDemoOperatorStatement                  = "statement"
	readBillingDemoOperatorMemDBMutation              = "memdb_mutation"
	readBillingDemoOperatorTxnPrewrite                = "txn_prewrite"
	readBillingDemoOperatorTxnWrite                   = "txn_write"
	readBillingDemoUnitFixedEvents                    = "fixed_events"
	readBillingDemoUnitInputRows                      = "input_rows"
	readBillingDemoUnitInputBytes                     = "input_bytes"
	readBillingDemoUnitOutputRows                     = "output_rows"
	readBillingDemoUnitOutputBytes                    = "output_bytes"
	readBillingDemoUnitOrderWork                      = "order_work"
	readBillingDemoUnitEncodedMutationCount           = "encoded_mutation_count"
	readBillingDemoUnitEncodedMutationBytes           = "encoded_mutation_bytes"
	readBillingDemoUnitSetCount                       = "set_count"
	readBillingDemoUnitDeleteCount                    = "delete_count"
	readBillingDemoUnitKeyBytes                       = "key_bytes"
	readBillingDemoUnitValueBytes                     = "value_bytes"
	readBillingDemoUnitWriteKeys                      = "write_keys"
	readBillingDemoUnitWriteByte                      = "write_byte"
	readBillingDemoUnitPrewriteRegionNum              = "prewrite_region_num"
	readBillingDemoUnitTiKVWriteRPCCount              = "tikv_write_rpc_count"
	readBillingDemoUnitCPUWork                        = "cpu_work"
	readBillingDemoUnitExpressionCount                = "expression_count"
	readBillingDemoUnitScanBytes                      = "scan_bytes"
	readBillingDemoUnitNetBytes                       = "net_bytes"
	readBillingDemoUnitReadRequestCount               = "read_request_count"
	readBillingDemoUnitWriteRequestCount              = "write_request_count"
	readBillingDemoUnitHashStateRows                  = "hash_state_rows"
	readBillingDemoUnitJoinOutputRows                 = "join_output_rows"
	readBillingDemoInputSourceRuntimeChunkBytes       = "runtime_chunk_bytes"
	readBillingDemoInputSourceScanDetail              = "scan_detail"
	readBillingDemoInputSourceRuntimeChildActRows     = "runtime_child_act_rows"
	readBillingDemoInputSourceRuntimeOperatorActRows  = "runtime_operator_act_rows"
	readBillingDemoInputSourceRuntimeReaderOutput     = "runtime_reader_output_chunks"
	readBillingDemoInputSourceRuntimeOrderingWork     = "runtime_ordering_work"
	readBillingDemoInputSourceStmtMemDBMutation       = "stmt_memdb_mutation_calls"
	readBillingDemoInputSourceCommitDetail            = "commit_detail"
	readBillingDemoInputSourceRUV2Metrics             = "ruv2_metrics"
	readBillingDemoInputSourcePhysicalPlan            = "physical_plan"
	readBillingDemoInputSourceHashJoinRuntime         = "hash_join_runtime_stats"
	readBillingDemoInputSideAll                       = "all"
	readBillingDemoInputSideBuild                     = "build"
	readBillingDemoInputSideProbe                     = "probe"
	readBillingDemoInputSideLeft                      = "left"
	readBillingDemoInputSideRight                     = "right"
	readBillingDemoScopeStatementAttempted            = "statement_attempted"
	readBillingDemoScopeTxnPrewritePayload            = "txn_prewrite_payload"
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
	id            string
	site          string
	opClass       string
	operatorKind  string
	dmlKind       string
	scope         string
	uncalibrated  bool
	emitStatusRow bool
	status        string
	reason        string
	actRows       int64
	hasActRows    bool
	units         []readBillingDemoUnit
}

type readBillingDemoResult struct {
	status    string
	reason    string
	operators []readBillingDemoOperatorResult
}

type readBillingDemoCopWidthState uint8

const (
	readBillingDemoCopWidthMissing readBillingDemoCopWidthState = iota
	readBillingDemoCopWidthKnown
	readBillingDemoCopWidthBarrier
	readBillingDemoCopWidthAmbiguous
)

type readBillingDemoCopFailureKind uint8

const (
	readBillingDemoCopFailureCurrent readBillingDemoCopFailureKind = iota
	readBillingDemoCopFailureIntrinsicCause
)

type readBillingDemoCopRowsState uint8

const (
	readBillingDemoCopRowsMissing readBillingDemoCopRowsState = iota
	readBillingDemoCopRowsObserved
	readBillingDemoCopRowsInvalid
)

type readBillingDemoCopFailure struct {
	present    bool
	kind       readBillingDemoCopFailureKind
	status     string
	reason     string
	failingIdx int
}

type readBillingDemoCopRowsEvidence struct {
	state readBillingDemoCopRowsState
	rows  int64
	tasks int32
}

type readBillingDemoCopOutputEstimate struct {
	widthState  readBillingDemoCopWidthState
	avgRowWidth float64
	widthSource string
	scanPlanID  int
	failure     readBillingDemoCopFailure
}

type readBillingDemoCopInputEstimate struct {
	rows        int64
	inputBytes  float64
	avgRowWidth float64
	inputSource string
	widthSource string
	failure     readBillingDemoCopFailure
}

type readBillingDemoCopComponentEvidence struct {
	scanCount               int
	scanIdx                 int
	scanObservedTasks       int32
	scanExpectedTasks       int32
	detailHolderCount       int
	scanDetail              tikvutil.ScanDetail
	scanDetailRecords       int32
	scanDetailExpectedTasks int32
	maxSummaryTasks         int32
}

type readBillingDemoCopEstimator struct {
	tree             FlatPlanTree
	runtimeStats     *execdetails.RuntimeStatsColl
	parentIdx        []int
	componentID      []int
	components       []readBillingDemoCopComponentEvidence
	nodeFailures     map[int]readBillingDemoCopFailure
	treeFailures     []readBillingDemoCopFailure
	treeFailureSeen  map[int]struct{}
	inputMemo        map[int]readBillingDemoCopInputEstimate
	inputMemoSet     map[int]struct{}
	outputMemo       map[int]readBillingDemoCopOutputEstimate
	outputMemoSet    map[int]struct{}
	visiting         map[int]bool
	nodeVisits       int
	edgeVisits       int
	auxiliaryEntries int
}

type readBillingDemoCopUnitOutcome struct {
	success bool
	units   []readBillingDemoUnit
	failure readBillingDemoCopFailure
}

type readBillingDemoAppendOutcome struct {
	success bool
	status  string
	current readBillingDemoOperatorResult
	cause   readBillingDemoCopFailure
}

type readBillingDemoWeights struct {
	Version                 string
	CPUWeight               float64
	ScanWeight              float64
	NetWeight               float64
	ReadRequestWeight       float64
	WriteRequestWeight      float64
	HashTableWeight         float64
	JoinWeight              float64
	MutationBytesPerCPUUnit float64
	Calibrated              bool
}

type readBillingDemoWeightProvider interface {
	valid() bool
	unitWeight(string) (float64, bool)
}

// Production intentionally starts without guessed coefficients. Formula tests
// inject a calibrated private value directly.
var readBillingDemoV4Weights = readBillingDemoWeights{Version: readBillingDemoWeightVersion}

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
	workRows       float64
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
	result := buildReadBillingDemoResult(planCtx, plan, stmt, execErr, nil)
	recordReadBillingDemoResult(result)
	return buildReadBillingDemoStatementStats(result)
}

func buildReadBillingDemoResult(sctx base.PlanContext, plan base.Plan, stmt ast.StmtNode, execErr error, ruv2Metrics *execdetails.RUV2Metrics) readBillingDemoResult {
	if _, ok := stmt.(*ast.CommitStmt); ok {
		return buildTxnCommitBillingDemoResult(sctx, ruv2Metrics, execErr)
	}
	if _, ok := stmt.(*ast.RollbackStmt); ok {
		return readBillingDemoFailure(readBillingDemoStatusUnsupported, readBillingDemoReasonUnsupportedStatement)
	}
	if dmlKind, ok := explainRUWriteDMLKind(stmt); ok {
		return buildWriteBillingDemoResult(sctx, plan, dmlKind, ruv2Metrics, execErr)
	}
	if execErr != nil {
		return readBillingDemoFailure(readBillingDemoStatusError, readBillingDemoReasonStatementError)
	}
	if plan == nil {
		return readBillingDemoFailure(readBillingDemoStatusUnknownInput, readBillingDemoReasonMissingPlan)
	}
	if gateStatus := explainRUTargetGateStatus(stmt); gateStatus != explainRUStatusSuccess {
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
	if ruv2Metrics == nil && sctx.GetSessionVars() != nil {
		ruv2Metrics = sctx.GetSessionVars().RUV2Metrics
	}
	if op, present := readBillingDemoReaderTransport(flat, runtimeStats, ruv2Metrics, false); present {
		if op.status != readBillingDemoStatusOperatorOK {
			return readBillingDemoFailedOperator(readBillingDemoStatusUnknownInput, op)
		}
		result.operators = append(result.operators, op)
	}
	if op, present := readBillingDemoPointLookupTransport(flat, ruv2Metrics, false); present {
		if op.status != readBillingDemoStatusOperatorOK {
			return readBillingDemoFailedOperator(readBillingDemoStatusUnknownInput, op)
		}
		result.operators = append(result.operators, op)
	}
	return result
}

func buildTxnCommitBillingDemoResult(sctx base.PlanContext, ruv2Metrics *execdetails.RUV2Metrics, execErr error) readBillingDemoResult {
	result := readBillingDemoResult{
		status: readBillingDemoStatusSuccess,
		reason: readBillingDemoReasonNone,
	}
	if execErr != nil {
		result.status = readBillingDemoStatusError
		result.reason = readBillingDemoReasonStatementError
	}

	var pipelined bool
	if sctx != nil && sctx.GetSessionVars() != nil {
		vars := sctx.GetSessionVars()
		if ruv2Metrics == nil {
			ruv2Metrics = vars.RUV2Metrics
		}
		if vars.StmtCtx != nil && vars.StmtCtx.PreviewKVMutationRecorder != nil {
			pipelined = vars.StmtCtx.PreviewKVMutationRecorder.Snapshot().Pipelined
		}
	}

	// COMMIT owns only the write requests in its fresh statement snapshot. Keep
	// dml_kind empty because earlier DML statements own their own snapshots.
	result.operators = append(result.operators, buildTiKVWriteBillingDemoOperators("", ruv2Metrics, pipelined)...)
	return result
}

func buildWriteBillingDemoResult(sctx base.PlanContext, plan base.Plan, dmlKind string, ruv2Metrics *execdetails.RUV2Metrics, execErr error) readBillingDemoResult {
	result := readBillingDemoResult{
		status: readBillingDemoStatusSuccess,
		reason: readBillingDemoReasonNone,
	}
	if execErr != nil {
		result.status = readBillingDemoStatusError
		result.reason = readBillingDemoReasonStatementError
	}

	var pipelined bool
	if sctx != nil && sctx.GetSessionVars() != nil {
		if ruv2Metrics == nil {
			ruv2Metrics = sctx.GetSessionVars().RUV2Metrics
		}
		if stmtCtx := sctx.GetSessionVars().StmtCtx; stmtCtx != nil && stmtCtx.PreviewKVMutationRecorder != nil {
			pipelined = stmtCtx.PreviewKVMutationRecorder.Snapshot().Pipelined
		}
	}

	appendReadBillingDemoDMLPlan(&result, sctx, plan, ruv2Metrics)
	appendReadBillingDemoMutation(&result, sctx, dmlKind)
	result.operators = append(result.operators, buildTiKVWriteBillingDemoOperators(dmlKind, ruv2Metrics, pipelined)...)
	return result
}

func appendReadBillingDemoDMLPlan(result *readBillingDemoResult, sctx base.PlanContext, plan base.Plan, ruv2Metrics *execdetails.RUV2Metrics) {
	if plan == nil || sctx == nil || sctx.GetSessionVars() == nil || sctx.GetSessionVars().StmtCtx == nil {
		result.operators = append(result.operators, readBillingDemoPlanDiagnostic(readBillingDemoReasonMissingPlan))
		return
	}
	runtimeStats := sctx.GetSessionVars().StmtCtx.RuntimeStatsColl
	if runtimeStats == nil {
		result.operators = append(result.operators, readBillingDemoPlanDiagnostic(readBillingDemoReasonMissingRuntimeStats))
		return
	}
	flat := FlattenPhysicalPlan(plan, true)
	if flat == nil || len(flat.Main) == 0 || flat.InExplain || flat.InExecute {
		result.operators = append(result.operators, readBillingDemoPlanDiagnostic(readBillingDemoReasonMissingPlan))
		return
	}
	appendTree := func(tree FlatPlanTree) {
		appendReadBillingDemoDMLTree(result, runtimeStats, tree)
	}
	appendTree(flat.Main)
	for _, tree := range flat.CTEs {
		appendTree(tree)
	}
	for _, tree := range flat.ScalarSubQueries {
		appendTree(tree)
	}
	if op, present := readBillingDemoReaderTransport(flat, runtimeStats, ruv2Metrics, true); present {
		result.operators = append(result.operators, op)
	}
	if op, present := readBillingDemoPointLookupTransport(flat, ruv2Metrics, true); present {
		result.operators = append(result.operators, op)
	}
}

func readBillingDemoPlanDiagnostic(reason string) readBillingDemoOperatorResult {
	return readBillingDemoOperatorResult{
		id:            "dml_plan",
		site:          readBillingDemoSiteTiDB,
		opClass:       readBillingDemoOpClassWrapper,
		operatorKind:  "dml_plan",
		emitStatusRow: true,
		status:        readBillingDemoStatusPartial,
		reason:        reason,
	}
}

func appendReadBillingDemoMutation(result *readBillingDemoResult, sctx base.PlanContext, dmlKind string) {
	operator := readBillingDemoOperatorResult{
		id:           "mutation@statement",
		site:         readBillingDemoSiteTiDB,
		opClass:      readBillingDemoOpClassKVMutation,
		operatorKind: readBillingDemoOperatorMemDBMutation,
		dmlKind:      dmlKind,
		scope:        readBillingDemoScopeStatementAttempted,
		uncalibrated: !readBillingDemoWeightsValid(readBillingDemoV4Weights),
	}
	if sctx == nil || sctx.GetSessionVars() == nil || sctx.GetSessionVars().StmtCtx == nil ||
		sctx.GetSessionVars().StmtCtx.PreviewKVMutationRecorder == nil {
		operator.status = readBillingDemoStatusPartial
		operator.reason = readBillingDemoReasonMissingMutationRecorder
		result.operators = append(result.operators, operator)
		return
	}

	snapshot := sctx.GetSessionVars().StmtCtx.PreviewKVMutationRecorder.Snapshot()
	operator.status = readBillingDemoStatusOperatorOK
	operator.reason = readBillingDemoReasonNone
	if snapshot.EncodedMutationCount == 0 {
		operator.reason = readBillingDemoReasonZeroMutation
	}
	operator.units = append(operator.units,
		readBillingDemoUnit{unit: readBillingDemoUnitEncodedMutationCount, source: readBillingDemoInputSourceStmtMemDBMutation, side: readBillingDemoInputSideAll, value: float64(snapshot.EncodedMutationCount), widthSource: explainRUWidthSourceNotApplicable},
		readBillingDemoUnit{unit: readBillingDemoUnitEncodedMutationBytes, source: readBillingDemoInputSourceStmtMemDBMutation, side: readBillingDemoInputSideAll, value: float64(snapshot.EncodedMutationBytes), widthSource: explainRUWidthSourceNotApplicable},
		readBillingDemoUnit{unit: readBillingDemoUnitSetCount, source: readBillingDemoInputSourceStmtMemDBMutation, side: readBillingDemoInputSideAll, value: float64(snapshot.SetCount), widthSource: explainRUWidthSourceNotApplicable},
		readBillingDemoUnit{unit: readBillingDemoUnitDeleteCount, source: readBillingDemoInputSourceStmtMemDBMutation, side: readBillingDemoInputSideAll, value: float64(snapshot.DeleteCount), widthSource: explainRUWidthSourceNotApplicable},
		readBillingDemoUnit{unit: readBillingDemoUnitKeyBytes, source: readBillingDemoInputSourceStmtMemDBMutation, side: readBillingDemoInputSideAll, value: float64(snapshot.KeyBytes), widthSource: explainRUWidthSourceNotApplicable},
		readBillingDemoUnit{unit: readBillingDemoUnitValueBytes, source: readBillingDemoInputSourceStmtMemDBMutation, side: readBillingDemoInputSideAll, value: float64(snapshot.ValueBytes), widthSource: explainRUWidthSourceNotApplicable},
	)
	if readBillingDemoWeightsValid(readBillingDemoV4Weights) {
		normalization := readBillingDemoV4Weights.MutationBytesPerCPUUnit
		work := float64(snapshot.EncodedMutationCount) + float64(snapshot.EncodedMutationBytes)/normalization
		if work >= 0 && !math.IsNaN(work) && !math.IsInf(work, 0) {
			operator.units = append(operator.units, readBillingDemoUnit{unit: readBillingDemoUnitCPUWork, source: readBillingDemoInputSourceStmtMemDBMutation, side: readBillingDemoInputSideAll, value: work, widthSource: explainRUWidthSourceNotApplicable})
		}
	}
	result.operators = append(result.operators, operator)
	if !readBillingDemoWeightsValid(readBillingDemoV4Weights) {
		result.operators = append(result.operators, readBillingDemoMutationDiagnostic(dmlKind, readBillingDemoReasonUncalibratedMutation))
	}
	vars := sctx.GetSessionVars()
	if vars.InTxn() && vars.TxnCtx != nil && vars.TxnCtx.CouldRetry {
		result.operators = append(result.operators, readBillingDemoMutationDiagnostic(dmlKind, readBillingDemoReasonOptimisticReplayPartial))
	}
}

func readBillingDemoMutationDiagnostic(dmlKind, reason string) readBillingDemoOperatorResult {
	return readBillingDemoOperatorResult{
		id:           "mutation@statement",
		site:         readBillingDemoSiteTiDB,
		opClass:      readBillingDemoOpClassKVMutation,
		operatorKind: readBillingDemoOperatorMemDBMutation,
		dmlKind:      dmlKind,
		scope:        readBillingDemoScopeStatementAttempted,
		status:       readBillingDemoStatusPartial,
		reason:       reason,
	}
}

func buildTiKVWriteBillingDemoOperators(dmlKind string, ruv2Metrics *execdetails.RUV2Metrics, pipelined bool) []readBillingDemoOperatorResult {
	operator := readBillingDemoOperatorResult{
		id:            "txn_write@statement",
		site:          readBillingDemoSiteTiKV,
		opClass:       readBillingDemoOpClassKVWrite,
		operatorKind:  readBillingDemoOperatorTxnWrite,
		dmlKind:       dmlKind,
		scope:         readBillingDemoScopeTxnPrewritePayload,
		emitStatusRow: true,
	}
	if pipelined {
		operator.status = readBillingDemoStatusPartial
		operator.reason = readBillingDemoReasonPipelinedWritePartial
		return []readBillingDemoOperatorResult{operator}
	}
	if ruv2Metrics == nil || ruv2Metrics.Bypass() {
		operator.status = readBillingDemoStatusPartial
		operator.reason = readBillingDemoReasonMissingWriteRPCCount
		return []readBillingDemoOperatorResult{operator}
	}
	writeRPCCount := ruv2Metrics.ResourceManagerWriteCnt()
	if writeRPCCount < 0 {
		operator.status = readBillingDemoStatusPartial
		operator.reason = readBillingDemoReasonMissingWriteRPCCount
		return []readBillingDemoOperatorResult{operator}
	}
	operator.status = readBillingDemoStatusOperatorOK
	operator.reason = readBillingDemoReasonNone
	operator.units = []readBillingDemoUnit{{unit: readBillingDemoUnitWriteRequestCount, source: readBillingDemoInputSourceRUV2Metrics, side: readBillingDemoInputSideAll, value: float64(writeRPCCount), widthSource: explainRUWidthSourceNotApplicable}}
	return []readBillingDemoOperatorResult{operator}
}

func readBillingDemoPlanContext(plan base.Plan) base.PlanContext {
	if plan == nil {
		return nil
	}
	return plan.SCtx()
}

func readBillingDemoAllTrees(flat *FlatPhysicalPlan) []FlatPlanTree {
	if flat == nil {
		return nil
	}
	trees := make([]FlatPlanTree, 0, 1+len(flat.CTEs)+len(flat.ScalarSubQueries))
	trees = append(trees, flat.Main)
	trees = append(trees, flat.CTEs...)
	trees = append(trees, flat.ScalarSubQueries...)
	return trees
}

func readBillingDemoReaderTransport(flat *FlatPhysicalPlan, runtimeStats *execdetails.RuntimeStatsColl, ruv2Metrics *execdetails.RUV2Metrics, dml bool) (readBillingDemoOperatorResult, bool) {
	op := readBillingDemoOperatorResult{
		id:            "reader_transport@statement",
		site:          readBillingDemoSiteTiDB,
		opClass:       readBillingDemoOpClassReaderTransport,
		operatorKind:  "mixed_reader",
		emitStatusRow: true,
	}
	kinds := make(map[string]struct{})
	openProducerSet := dml
	hasTasks := false
	allReaderRowsZero := true
	for _, tree := range readBillingDemoAllTrees(flat) {
		for _, node := range tree {
			if node == nil || node.Origin == nil {
				continue
			}
			kind := ""
			switch plan := node.Origin.(type) {
			case *physicalop.PhysicalTableReader:
				if plan.StoreType == kv.TiKV {
					kind = "table_reader"
				} else {
					openProducerSet = true
				}
			case *physicalop.PhysicalIndexReader:
				kind = "index_reader"
			case *physicalop.PhysicalIndexLookUpReader:
				kind = "index_lookup"
			case *physicalop.PhysicalIndexMergeReader:
				kind = "index_merge"
			case *physicalop.PointGetPlan, *physicalop.BatchPointGetPlan, *physicalop.PhysicalExchangeReceiver, *physicalop.PhysicalExchangeSender:
				openProducerSet = true
			}
			if kind != "" {
				kinds[kind] = struct{}{}
				rows, ok := readBillingDemoPlanActRows(runtimeStats, node.Origin.ID())
				if !ok || rows != 0 {
					allReaderRowsZero = false
				}
			}
			if !node.IsRoot && node.StoreType == kv.TiKV && runtimeStats != nil {
				stats := runtimeStats.GetCopStats(node.Origin.ID())
				if (stats != nil && stats.GetTasks() > 0) || runtimeStats.GetExpectedCopTasks(node.Origin.ID()) > 0 {
					hasTasks = true
				}
			}
		}
	}
	if len(kinds) == 0 {
		return readBillingDemoOperatorResult{}, false
	}
	if len(kinds) == 1 {
		for kind := range kinds {
			op.operatorKind = kind
		}
	}
	if openProducerSet {
		op.status = readBillingDemoStatusUnknownInput
		op.reason = readBillingDemoReasonAmbiguousReaderTransport
		return op, true
	}
	if ruv2Metrics == nil || ruv2Metrics.Bypass() {
		op.status = readBillingDemoStatusUnknownInput
		op.reason = readBillingDemoReasonMissingReaderTransport
		return op, true
	}
	netBytes := ruv2Metrics.TiKVCoprocessorResponseBytes()
	requests := ruv2Metrics.ResourceManagerReadCnt()
	if netBytes < 0 || requests < 0 || (netBytes > 0 && requests == 0) || (netBytes == 0 && requests == 0 && (hasTasks || !allReaderRowsZero)) {
		op.status = readBillingDemoStatusUnknownInput
		op.reason = readBillingDemoReasonMissingReaderTransport
		return op, true
	}
	op.status = readBillingDemoStatusOperatorOK
	op.reason = readBillingDemoReasonNone
	op.units = []readBillingDemoUnit{
		{unit: readBillingDemoUnitNetBytes, source: readBillingDemoInputSourceRUV2Metrics, side: readBillingDemoInputSideAll, value: float64(netBytes), widthSource: explainRUWidthSourceNotApplicable},
		{unit: readBillingDemoUnitReadRequestCount, source: readBillingDemoInputSourceRUV2Metrics, side: readBillingDemoInputSideAll, value: float64(requests), widthSource: explainRUWidthSourceNotApplicable},
	}
	return op, true
}

func readBillingDemoPointLookupTransport(flat *FlatPhysicalPlan, ruv2Metrics *execdetails.RUV2Metrics, dml bool) (readBillingDemoOperatorResult, bool) {
	op := readBillingDemoOperatorResult{
		id:            "point_lookup@statement",
		site:          readBillingDemoSiteTiKV,
		opClass:       readBillingDemoOpClassPointLookup,
		operatorKind:  "mixed_point_lookup",
		emitStatusRow: true,
	}
	kinds := make(map[string]struct{})
	openProducerSet := dml
	for _, tree := range readBillingDemoAllTrees(flat) {
		for _, node := range tree {
			if node == nil || node.Origin == nil {
				continue
			}
			switch plan := node.Origin.(type) {
			case *physicalop.PointGetPlan:
				kinds["point_get"] = struct{}{}
				openProducerSet = openProducerSet || plan.Lock
			case *physicalop.BatchPointGetPlan:
				kinds["batch_point_get"] = struct{}{}
				openProducerSet = openProducerSet || plan.Lock
			case *physicalop.PhysicalTableReader:
				openProducerSet = true
			case *physicalop.PhysicalIndexReader, *physicalop.PhysicalIndexLookUpReader,
				*physicalop.PhysicalIndexMergeReader, *physicalop.PhysicalExchangeReceiver,
				*physicalop.PhysicalExchangeSender:
				openProducerSet = true
			}
		}
	}
	if len(kinds) == 0 {
		return readBillingDemoOperatorResult{}, false
	}
	if len(kinds) == 1 {
		for kind := range kinds {
			op.operatorKind = kind
		}
	}
	if openProducerSet {
		op.status = readBillingDemoStatusUnknownInput
		op.reason = readBillingDemoReasonAmbiguousReaderTransport
		return op, true
	}
	if ruv2Metrics == nil || ruv2Metrics.Bypass() {
		op.status = readBillingDemoStatusUnknownInput
		op.reason = readBillingDemoReasonMissingReaderTransport
		return op, true
	}
	requests := ruv2Metrics.ResourceManagerReadCnt()
	if requests < 0 {
		op.status = readBillingDemoStatusUnknownInput
		op.reason = readBillingDemoReasonMissingReaderTransport
		return op, true
	}
	op.status = readBillingDemoStatusOperatorOK
	op.reason = readBillingDemoReasonNone
	op.units = []readBillingDemoUnit{{
		unit: readBillingDemoUnitReadRequestCount, source: readBillingDemoInputSourceRUV2Metrics,
		side: readBillingDemoInputSideAll, value: float64(requests), widthSource: explainRUWidthSourceNotApplicable,
	}}
	return op, true
}

func readBillingDemoWeightsValid(weights readBillingDemoWeights) bool {
	if weights.Version == "" || weights.Version == readBillingDemoWeightVersion || !weights.Calibrated ||
		weights.MutationBytesPerCPUUnit <= 0 || math.IsNaN(weights.MutationBytesPerCPUUnit) || math.IsInf(weights.MutationBytesPerCPUUnit, 0) {
		return false
	}
	for _, weight := range []float64{weights.CPUWeight, weights.ScanWeight, weights.NetWeight, weights.ReadRequestWeight, weights.WriteRequestWeight, weights.HashTableWeight, weights.JoinWeight} {
		if weight < 0 || math.IsNaN(weight) || math.IsInf(weight, 0) {
			return false
		}
	}
	return true
}

func readBillingDemoActiveWeightVersion() string {
	if readBillingDemoV4Weights.Version != "" {
		return readBillingDemoV4Weights.Version
	}
	return readBillingDemoWeightVersion
}

func (weights readBillingDemoWeights) valid() bool {
	return readBillingDemoWeightsValid(weights)
}

func (weights readBillingDemoWeights) unitWeight(unit string) (float64, bool) {
	return readBillingDemoUnitWeight(weights, unit)
}

func readBillingDemoUnitWeight(weights readBillingDemoWeights, unit string) (float64, bool) {
	switch unit {
	case readBillingDemoUnitCPUWork:
		return weights.CPUWeight, true
	case readBillingDemoUnitScanBytes:
		return weights.ScanWeight, true
	case readBillingDemoUnitNetBytes:
		return weights.NetWeight, true
	case readBillingDemoUnitReadRequestCount:
		return weights.ReadRequestWeight, true
	case readBillingDemoUnitWriteRequestCount:
		return weights.WriteRequestWeight, true
	case readBillingDemoUnitHashStateRows:
		return weights.HashTableWeight, true
	case readBillingDemoUnitJoinOutputRows:
		return weights.JoinWeight, true
	default:
		return 0, false
	}
}

func readBillingDemoUnitPreviewRU(unit readBillingDemoUnit, weights readBillingDemoWeightProvider) (float64, float64, bool) {
	if !weights.valid() || unit.value < 0 || math.IsNaN(unit.value) || math.IsInf(unit.value, 0) {
		return 0, 0, false
	}
	weight, ok := weights.unitWeight(unit.unit)
	if !ok {
		return 0, 0, false
	}
	ru := unit.value * weight
	if ru < 0 || math.IsNaN(ru) || math.IsInf(ru, 0) {
		return 0, 0, false
	}
	return weight, ru, true
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
	// The three convenience totals are a v3 schema. V4 detail is preserved in
	// the versioned base-unit table and must not be projected into those fields.
	return stmtsummary.ReadBillingDemoBaseUnitSummary{}
}

func buildReadBillingDemoStatementStats(result readBillingDemoResult) stmtsummary.ReadBillingDemoStatementStats {
	stats := stmtsummary.ReadBillingDemoStatementStats{
		ModelVersion:  readBillingDemoModelVersion,
		WeightVersion: readBillingDemoActiveWeightVersion(),
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
		WeightVersion: readBillingDemoActiveWeightVersion(),
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
				WeightVersion: readBillingDemoActiveWeightVersion(),
				Site:          op.site,
				OpClass:       op.opClass,
				OperatorKind:  op.operatorKind,
				Status:        opStatus,
				Reason:        opReason,
			})
		}
		if opStatus != readBillingDemoStatusOperatorOK || !readBillingDemoOperatorBillable(op) {
			continue
		}
		for _, unit := range op.units {
			sample := stmtsummary.ReadBillingDemoBaseUnitSample{
				ModelVersion:   readBillingDemoModelVersion,
				WeightVersion:  readBillingDemoActiveWeightVersion(),
				Site:           op.site,
				OpClass:        op.opClass,
				OperatorKind:   op.operatorKind,
				DMLKind:        op.dmlKind,
				Unit:           unit.unit,
				InputSource:    unit.source,
				InputSide:      unit.side,
				RowWidthSource: unit.widthSource,
				Value:          unit.value,
				RowWidth:       unit.rowWidth,
			}
			stats.BaseUnits = append(stats.BaseUnits, sample)
		}
	}
	return stats
}

func readBillingDemoCopFailureAt(idx int, kind readBillingDemoCopFailureKind, status, reason string) readBillingDemoCopFailure {
	return readBillingDemoCopFailure{
		present:    true,
		kind:       kind,
		status:     status,
		reason:     reason,
		failingIdx: idx,
	}
}

func readBillingDemoPromoteCopFailure(failure readBillingDemoCopFailure) readBillingDemoCopFailure {
	if failure.present {
		failure.kind = readBillingDemoCopFailureIntrinsicCause
	}
	return failure
}

func readBillingDemoExactCopRowsEvidence(runtimeStats *execdetails.RuntimeStatsColl, planID int) readBillingDemoCopRowsEvidence {
	if runtimeStats == nil {
		return readBillingDemoCopRowsEvidence{state: readBillingDemoCopRowsMissing}
	}
	stats := runtimeStats.GetCopStats(planID)
	if stats == nil || stats.GetTasks() <= 0 {
		return readBillingDemoCopRowsEvidence{state: readBillingDemoCopRowsMissing}
	}
	rows := stats.GetActRows()
	if rows < 0 {
		return readBillingDemoCopRowsEvidence{state: readBillingDemoCopRowsInvalid, rows: rows, tasks: stats.GetTasks()}
	}
	return readBillingDemoCopRowsEvidence{state: readBillingDemoCopRowsObserved, rows: rows, tasks: stats.GetTasks()}
}

func newReadBillingDemoCopEstimator(tree FlatPlanTree, runtimeStats *execdetails.RuntimeStatsColl) *readBillingDemoCopEstimator {
	estimator := &readBillingDemoCopEstimator{
		tree:            tree,
		runtimeStats:    runtimeStats,
		parentIdx:       make([]int, len(tree)),
		componentID:     make([]int, len(tree)),
		nodeFailures:    make(map[int]readBillingDemoCopFailure),
		treeFailureSeen: make(map[int]struct{}),
		inputMemo:       make(map[int]readBillingDemoCopInputEstimate),
		inputMemoSet:    make(map[int]struct{}),
		outputMemo:      make(map[int]readBillingDemoCopOutputEstimate),
		outputMemoSet:   make(map[int]struct{}),
		visiting:        make(map[int]bool),
	}
	for i := range tree {
		estimator.parentIdx[i] = -1
		estimator.componentID[i] = -1
	}
	addStructuralFailure := func(idx int) {
		if idx < 0 || idx >= len(tree) {
			return
		}
		failure := readBillingDemoCopFailureAt(idx, readBillingDemoCopFailureIntrinsicCause, readBillingDemoStatusUnsupported, readBillingDemoReasonUnsupportedCopStructure)
		estimator.nodeFailures[idx] = failure
		if _, ok := estimator.treeFailureSeen[idx]; !ok {
			estimator.treeFailureSeen[idx] = struct{}{}
			estimator.treeFailures = append(estimator.treeFailures, failure)
		}
	}

	// Build the reverse direct-edge index and reject malformed references. This
	// pass is O(n+m), where m is the number of explicit ChildrenIdx entries.
	for idx, node := range tree {
		estimator.nodeVisits++
		if node == nil || node.Origin == nil {
			continue
		}
		previousChild := idx
		for _, childIdx := range node.ChildrenIdx {
			estimator.edgeVisits++
			if childIdx <= previousChild || childIdx <= idx || childIdx >= len(tree) {
				addStructuralFailure(idx)
				continue
			}
			previousChild = childIdx
			if tree[childIdx] == nil || tree[childIdx].Origin == nil {
				addStructuralFailure(idx)
				continue
			}
			if previousParent := estimator.parentIdx[childIdx]; previousParent >= 0 {
				addStructuralFailure(previousParent)
				addStructuralFailure(idx)
				addStructuralFailure(childIdx)
				continue
			}
			estimator.parentIdx[childIdx] = idx
		}
	}

	componentRoots := make([]int, 0)
	for idx, node := range tree {
		estimator.nodeVisits++
		if !readBillingDemoIsTiKVCopNode(node) {
			continue
		}
		parentIdx := estimator.parentIdx[idx]
		if parentIdx < 0 || parentIdx >= len(tree) || tree[parentIdx] == nil || tree[parentIdx].Origin == nil {
			addStructuralFailure(idx)
			continue
		}
		parent := tree[parentIdx]
		if parent.IsRoot {
			componentRoots = append(componentRoots, idx)
			continue
		}
		if !readBillingDemoIsTiKVCopNode(parent) {
			addStructuralFailure(idx)
			addStructuralFailure(parentIdx)
		}
	}

	lastComponentRoot := -1
	lastComponentEnd := -1
	for _, rootIdx := range componentRoots {
		rootEnd := tree[rootIdx].ChildrenEndIdx
		if rootEnd < rootIdx || rootEnd >= len(tree) {
			addStructuralFailure(rootIdx)
			continue
		}
		// Component roots are discovered in preorder. Validate their intervals
		// with one ordered sweep so malformed, overlapping roots cannot make us
		// rescan the same suffix once per sibling component.
		if rootIdx <= lastComponentEnd {
			addStructuralFailure(lastComponentRoot)
			addStructuralFailure(rootIdx)
			continue
		}
		lastComponentRoot = rootIdx
		lastComponentEnd = rootEnd
		estimator.validateReadBillingDemoCopComponent(rootIdx, addStructuralFailure)
	}
	for idx, node := range tree {
		estimator.nodeVisits++
		if readBillingDemoIsTiKVCopNode(node) && estimator.componentID[idx] < 0 {
			addStructuralFailure(idx)
		}
	}

	// Gather component evidence and intrinsic invalid-row failures in one
	// exact-plan-ID pass. Detail ownership and row ownership intentionally stay
	// separate because distsql attaches response ScanDetail to the last plan ID.
	for idx, node := range tree {
		estimator.nodeVisits++
		if !readBillingDemoIsTiKVCopNode(node) {
			continue
		}
		if evidence := readBillingDemoExactCopRowsEvidence(runtimeStats, node.Origin.ID()); evidence.state == readBillingDemoCopRowsInvalid {
			if _, structural := estimator.nodeFailures[idx]; !structural {
				estimator.nodeFailures[idx] = readBillingDemoCopFailureAt(idx, readBillingDemoCopFailureIntrinsicCause, readBillingDemoStatusUnknownInput, readBillingDemoReasonInvalidCopRuntimeRows)
			}
		}
		componentID := estimator.componentID[idx]
		if componentID < 0 || componentID >= len(estimator.components) {
			continue
		}
		component := &estimator.components[componentID]
		operator, supported, _ := readBillingDemoClassifyOperator(node)
		if supported && operator.opClass == readBillingDemoOpClassRangeScan {
			component.scanCount++
			component.scanIdx = idx
		}
		if runtimeStats == nil {
			continue
		}
		detail, detailRecords, observedTasks, expectedTasks := runtimeStats.GetCopScanDetailAndCoverage(node.Origin.ID())
		if observedTasks > component.maxSummaryTasks {
			component.maxSummaryTasks = observedTasks
		}
		if supported && operator.opClass == readBillingDemoOpClassRangeScan {
			component.scanObservedTasks = observedTasks
			component.scanExpectedTasks = expectedTasks
		}
		if detailRecords > 0 {
			component.detailHolderCount++
			component.scanDetail = detail
			component.scanDetailRecords = detailRecords
			component.scanDetailExpectedTasks = expectedTasks
		}
	}
	estimator.auxiliaryEntries = len(estimator.parentIdx) + len(estimator.componentID) + len(estimator.components) + len(estimator.nodeFailures) + len(estimator.treeFailures)
	return estimator
}

func readBillingDemoIsTiKVCopNode(node *FlatOperator) bool {
	return node != nil && node.Origin != nil && !node.IsRoot && node.StoreType == kv.TiKV
}

func (e *readBillingDemoCopEstimator) validateReadBillingDemoCopComponent(rootIdx int, addStructuralFailure func(int)) {
	if rootIdx < 0 || rootIdx >= len(e.tree) || !readBillingDemoIsTiKVCopNode(e.tree[rootIdx]) {
		addStructuralFailure(rootIdx)
		return
	}
	rootEnd := e.tree[rootIdx].ChildrenEndIdx
	if rootEnd < rootIdx || rootEnd >= len(e.tree) {
		addStructuralFailure(rootIdx)
		return
	}
	componentID := len(e.components)
	e.components = append(e.components, readBillingDemoCopComponentEvidence{scanIdx: -1})
	visited := make(map[int]struct{})

	var visit func(int, int) int
	visit = func(idx, expectedParent int) int {
		e.nodeVisits++
		if idx < rootIdx || idx > rootEnd || idx >= len(e.tree) {
			addStructuralFailure(rootIdx)
			return idx
		}
		node := e.tree[idx]
		if !readBillingDemoIsTiKVCopNode(node) {
			addStructuralFailure(idx)
			return idx
		}
		if _, duplicate := visited[idx]; duplicate {
			addStructuralFailure(idx)
			return node.ChildrenEndIdx
		}
		visited[idx] = struct{}{}
		if expectedParent >= 0 && e.parentIdx[idx] != expectedParent {
			addStructuralFailure(expectedParent)
			addStructuralFailure(idx)
		}
		if previousComponent := e.componentID[idx]; previousComponent >= 0 && previousComponent != componentID {
			addStructuralFailure(idx)
			return idx
		}
		e.componentID[idx] = componentID
		nodeEnd := node.ChildrenEndIdx
		if nodeEnd < idx || nodeEnd > rootEnd {
			addStructuralFailure(idx)
			return idx
		}
		if len(node.ChildrenIdx) == 0 {
			if nodeEnd != idx {
				addStructuralFailure(idx)
			}
			return nodeEnd
		}
		expectedChild := idx + 1
		for _, childIdx := range node.ChildrenIdx {
			e.edgeVisits++
			if childIdx != expectedChild {
				addStructuralFailure(idx)
			}
			childEnd := visit(childIdx, idx)
			if childEnd < childIdx {
				addStructuralFailure(childIdx)
				childEnd = childIdx
			}
			expectedChild = childEnd + 1
		}
		if expectedChild-1 != nodeEnd {
			addStructuralFailure(idx)
		}
		return nodeEnd
	}

	if visit(rootIdx, e.parentIdx[rootIdx]) != rootEnd {
		addStructuralFailure(rootIdx)
	}
	for idx := rootIdx; idx <= rootEnd; idx++ {
		e.nodeVisits++
		if e.componentID[idx] != componentID {
			addStructuralFailure(idx)
		}
	}
}

func (e *readBillingDemoCopEstimator) firstTreeFailure() (readBillingDemoCopFailure, bool) {
	if len(e.treeFailures) == 0 {
		return readBillingDemoCopFailure{}, false
	}
	failure := e.treeFailures[0]
	for _, candidate := range e.treeFailures[1:] {
		if candidate.failingIdx < failure.failingIdx {
			failure = candidate
		}
	}
	return failure, true
}

func (e *readBillingDemoCopEstimator) componentOutputWidth(idx int) readBillingDemoCopOutputEstimate {
	if idx < 0 || idx >= len(e.componentID) {
		return readBillingDemoCopOutputEstimate{failure: readBillingDemoCopFailureAt(idx, readBillingDemoCopFailureIntrinsicCause, readBillingDemoStatusUnsupported, readBillingDemoReasonUnsupportedCopStructure)}
	}
	componentID := e.componentID[idx]
	if componentID < 0 || componentID >= len(e.components) {
		return readBillingDemoCopOutputEstimate{failure: readBillingDemoCopFailureAt(idx, readBillingDemoCopFailureIntrinsicCause, readBillingDemoStatusUnsupported, readBillingDemoReasonUnsupportedCopStructure)}
	}
	component := e.components[componentID]
	if component.scanCount > 1 || component.detailHolderCount > 1 {
		return readBillingDemoCopOutputEstimate{widthState: readBillingDemoCopWidthAmbiguous}
	}
	if component.scanCount != 1 || component.detailHolderCount != 1 {
		return readBillingDemoCopOutputEstimate{widthState: readBillingDemoCopWidthMissing}
	}
	rowWidth := float64(component.scanDetail.ProcessedKeysSize) / float64(component.scanDetail.ProcessedKeys)
	if rowWidth <= 0 || math.IsNaN(rowWidth) || math.IsInf(rowWidth, 0) {
		return readBillingDemoCopOutputEstimate{widthState: readBillingDemoCopWidthMissing}
	}
	return readBillingDemoCopOutputEstimate{
		widthState:  readBillingDemoCopWidthKnown,
		avgRowWidth: rowWidth,
		widthSource: explainRUWidthSourceScanDetailProcessedEstimate,
		scanPlanID:  e.tree[component.scanIdx].Origin.ID(),
	}
}

func (e *readBillingDemoCopEstimator) directCopChild(idx int) (int, readBillingDemoCopFailure, bool) {
	if idx < 0 || idx >= len(e.tree) || !readBillingDemoIsTiKVCopNode(e.tree[idx]) {
		return 0, readBillingDemoCopFailureAt(idx, readBillingDemoCopFailureCurrent, readBillingDemoStatusUnsupported, readBillingDemoReasonUnsupportedCopStructure), false
	}
	children := e.tree[idx].ChildrenIdx
	if len(children) == 0 {
		return 0, readBillingDemoCopFailureAt(idx, readBillingDemoCopFailureCurrent, readBillingDemoStatusUnsupported, readBillingDemoReasonUnsupportedCopStructure), false
	}
	if len(children) > 1 {
		return 0, readBillingDemoCopFailureAt(idx, readBillingDemoCopFailureCurrent, readBillingDemoStatusUnsupported, readBillingDemoReasonUnsupportedCopMultiChild), false
	}
	childIdx := children[0]
	if childIdx < 0 || childIdx >= len(e.tree) || !readBillingDemoIsTiKVCopNode(e.tree[childIdx]) || e.componentID[childIdx] != e.componentID[idx] {
		return 0, readBillingDemoCopFailureAt(idx, readBillingDemoCopFailureCurrent, readBillingDemoStatusUnsupported, readBillingDemoReasonUnsupportedCopStructure), false
	}
	return childIdx, readBillingDemoCopFailure{}, true
}

func readBillingDemoCopOperatorPreservesWidth(opClass string) bool {
	switch opClass {
	case readBillingDemoOpClassFilter, readBillingDemoOpClassLimit, readBillingDemoOpClassTopN:
		return true
	default:
		return false
	}
}

func (e *readBillingDemoCopEstimator) inputEstimate(idx int) readBillingDemoCopInputEstimate {
	if _, ok := e.inputMemoSet[idx]; ok {
		return e.inputMemo[idx]
	}
	estimate := readBillingDemoCopInputEstimate{}
	defer func() {
		e.inputMemo[idx] = estimate
		e.inputMemoSet[idx] = struct{}{}
	}()
	if failure, ok := e.nodeFailures[idx]; ok {
		estimate.failure = failure
		return estimate
	}
	if idx < 0 || idx >= len(e.tree) || !readBillingDemoIsTiKVCopNode(e.tree[idx]) {
		estimate.failure = readBillingDemoCopFailureAt(idx, readBillingDemoCopFailureCurrent, readBillingDemoStatusUnsupported, readBillingDemoReasonUnsupportedCopStructure)
		return estimate
	}
	operator, supported, reason := readBillingDemoClassifyOperator(e.tree[idx])
	if !supported {
		estimate.failure = readBillingDemoCopFailureAt(idx, readBillingDemoCopFailureCurrent, readBillingDemoStatusUnsupported, reason)
		return estimate
	}
	if operator.opClass == readBillingDemoOpClassRangeScan {
		estimate.failure = readBillingDemoCopFailureAt(idx, readBillingDemoCopFailureCurrent, readBillingDemoStatusUnsupported, readBillingDemoReasonUnsupportedCopStructure)
		return estimate
	}
	childIdx, failure, ok := e.directCopChild(idx)
	if !ok {
		estimate.failure = failure
		return estimate
	}
	childOutput := e.outputEstimate(childIdx)
	if childOutput.failure.present {
		estimate.failure = readBillingDemoPromoteCopFailure(childOutput.failure)
		return estimate
	}
	rowsEvidence := readBillingDemoExactCopRowsEvidence(e.runtimeStats, e.tree[childIdx].Origin.ID())
	switch rowsEvidence.state {
	case readBillingDemoCopRowsInvalid:
		estimate.failure = readBillingDemoCopFailureAt(childIdx, readBillingDemoCopFailureIntrinsicCause, readBillingDemoStatusUnknownInput, readBillingDemoReasonInvalidCopRuntimeRows)
		return estimate
	case readBillingDemoCopRowsMissing:
		estimate.failure = readBillingDemoCopFailureAt(idx, readBillingDemoCopFailureCurrent, readBillingDemoStatusUnknownInput, readBillingDemoReasonMissingCopChildRuntimeRows)
		return estimate
	}
	componentID := e.componentID[idx]
	if componentID < 0 || componentID >= len(e.components) {
		estimate.failure = readBillingDemoCopFailureAt(idx, readBillingDemoCopFailureCurrent, readBillingDemoStatusUnsupported, readBillingDemoReasonUnsupportedCopStructure)
		return estimate
	}
	if maxTasks := e.components[componentID].maxSummaryTasks; maxTasks > 0 && rowsEvidence.tasks < maxTasks {
		estimate.failure = readBillingDemoCopFailureAt(idx, readBillingDemoCopFailureCurrent, readBillingDemoStatusUnknownInput, readBillingDemoReasonIncompleteCopRuntimeRows)
		return estimate
	}
	switch childOutput.widthState {
	case readBillingDemoCopWidthBarrier:
		estimate.failure = readBillingDemoCopFailureAt(idx, readBillingDemoCopFailureCurrent, readBillingDemoStatusUnknownInput, readBillingDemoReasonUnsupportedCopWidthTransform)
		return estimate
	case readBillingDemoCopWidthAmbiguous:
		estimate.failure = readBillingDemoCopFailureAt(idx, readBillingDemoCopFailureCurrent, readBillingDemoStatusUnknownInput, readBillingDemoReasonAmbiguousCopScanWidth)
		return estimate
	case readBillingDemoCopWidthMissing:
		estimate.failure = readBillingDemoCopFailureAt(idx, readBillingDemoCopFailureCurrent, readBillingDemoStatusUnknownInput, readBillingDemoReasonMissingScanWidthEvidence)
		return estimate
	case readBillingDemoCopWidthKnown:
	default:
		estimate.failure = readBillingDemoCopFailureAt(idx, readBillingDemoCopFailureCurrent, readBillingDemoStatusUnknownInput, readBillingDemoReasonMissingScanWidthEvidence)
		return estimate
	}
	inputBytes := float64(rowsEvidence.rows) * childOutput.avgRowWidth
	if inputBytes < 0 || math.IsNaN(inputBytes) || math.IsInf(inputBytes, 0) {
		estimate.failure = readBillingDemoCopFailureAt(idx, readBillingDemoCopFailureCurrent, readBillingDemoStatusUnknownInput, readBillingDemoReasonMissingScanWidthEvidence)
		return estimate
	}
	estimate.rows = rowsEvidence.rows
	estimate.inputBytes = inputBytes
	estimate.avgRowWidth = childOutput.avgRowWidth
	estimate.inputSource = readBillingDemoInputSourceRuntimeChildActRows
	estimate.widthSource = childOutput.widthSource
	return estimate
}

func (e *readBillingDemoCopEstimator) outputEstimate(idx int) readBillingDemoCopOutputEstimate {
	if _, ok := e.outputMemoSet[idx]; ok {
		return e.outputMemo[idx]
	}
	estimate := readBillingDemoCopOutputEstimate{}
	defer func() {
		e.outputMemo[idx] = estimate
		e.outputMemoSet[idx] = struct{}{}
	}()
	if e.visiting[idx] {
		estimate.failure = readBillingDemoCopFailureAt(idx, readBillingDemoCopFailureIntrinsicCause, readBillingDemoStatusUnsupported, readBillingDemoReasonUnsupportedCopStructure)
		return estimate
	}
	e.visiting[idx] = true
	defer delete(e.visiting, idx)
	if failure, ok := e.nodeFailures[idx]; ok {
		estimate.failure = failure
		return estimate
	}
	if idx < 0 || idx >= len(e.tree) || !readBillingDemoIsTiKVCopNode(e.tree[idx]) {
		estimate.failure = readBillingDemoCopFailureAt(idx, readBillingDemoCopFailureIntrinsicCause, readBillingDemoStatusUnsupported, readBillingDemoReasonUnsupportedCopStructure)
		return estimate
	}
	operator, supported, reason := readBillingDemoClassifyOperator(e.tree[idx])
	if !supported {
		estimate.failure = readBillingDemoCopFailureAt(idx, readBillingDemoCopFailureIntrinsicCause, readBillingDemoStatusUnsupported, reason)
		return estimate
	}
	if operator.opClass == readBillingDemoOpClassRangeScan {
		if len(e.tree[idx].ChildrenIdx) != 0 {
			estimate.failure = readBillingDemoCopFailureAt(idx, readBillingDemoCopFailureIntrinsicCause, readBillingDemoStatusUnsupported, readBillingDemoReasonUnsupportedCopStructure)
			return estimate
		}
		estimate = e.componentOutputWidth(idx)
		return estimate
	}
	input := e.inputEstimate(idx)
	if input.failure.present {
		estimate.failure = readBillingDemoPromoteCopFailure(input.failure)
		return estimate
	}
	if readBillingDemoCopOperatorPreservesWidth(operator.opClass) {
		estimate.widthState = readBillingDemoCopWidthKnown
		estimate.avgRowWidth = input.avgRowWidth
		estimate.widthSource = input.widthSource
		childIdx, _, ok := e.directCopChild(idx)
		if ok {
			estimate.scanPlanID = e.outputEstimate(childIdx).scanPlanID
		}
		return estimate
	}
	estimate.widthState = readBillingDemoCopWidthBarrier
	return estimate
}

func (e *readBillingDemoCopEstimator) auxiliaryEntryCount() int {
	return e.auxiliaryEntries + len(e.inputMemo) + len(e.inputMemoSet) + len(e.outputMemo) + len(e.outputMemoSet) + len(e.visiting)
}

func appendReadBillingDemoTree(result *readBillingDemoResult, sctx base.PlanContext, runtimeStats *execdetails.RuntimeStatsColl, tree FlatPlanTree) (string, readBillingDemoOperatorResult) {
	estimator := newReadBillingDemoCopEstimator(tree, runtimeStats)
	if failure, ok := estimator.firstTreeFailure(); ok {
		return failure.status, readBillingDemoMaterializeCopFailure(runtimeStats, tree, failure)
	}
	for i := range tree {
		outcome := appendReadBillingDemoOperator(result, runtimeStats, tree, estimator, i)
		if outcome.success {
			continue
		}
		if outcome.cause.present {
			return outcome.cause.status, readBillingDemoMaterializeCopFailure(runtimeStats, tree, outcome.cause)
		}
		return outcome.status, outcome.current
	}
	return readBillingDemoStatusSuccess, readBillingDemoOperatorResult{}
}

// appendReadBillingDemoDMLTree keeps every independently usable plan operator.
// A missing or unsupported node makes only that node partial; it must not hide
// supported descendants from the DML read/compute tree.
func appendReadBillingDemoDMLTree(result *readBillingDemoResult, runtimeStats *execdetails.RuntimeStatsColl, tree FlatPlanTree) {
	estimator := newReadBillingDemoCopEstimator(tree, runtimeStats)
	type diagnosticKey struct {
		idx    int
		reason string
	}
	reported := make(map[diagnosticKey]struct{})
	appendDiagnostic := func(idx int, operator readBillingDemoOperatorResult, reason string) {
		key := diagnosticKey{idx: idx, reason: reason}
		if _, ok := reported[key]; ok {
			return
		}
		reported[key] = struct{}{}
		operator.emitStatusRow = true
		operator.status = readBillingDemoStatusPartial
		operator.reason = reason
		result.operators = append(result.operators, operator)
	}
	for _, failure := range estimator.treeFailures {
		appendDiagnostic(failure.failingIdx, readBillingDemoMaterializeCopFailure(runtimeStats, tree, failure), failure.reason)
	}
	for i := range tree {
		outcome := appendReadBillingDemoOperator(result, runtimeStats, tree, estimator, i)
		if outcome.success {
			continue
		}
		if outcome.cause.present && outcome.cause.failingIdx != i {
			appendDiagnostic(i, outcome.current, readBillingDemoReasonDependentCopInputUnavailable)
			appendDiagnostic(
				outcome.cause.failingIdx,
				readBillingDemoMaterializeCopFailure(runtimeStats, tree, outcome.cause),
				outcome.cause.reason,
			)
			continue
		}
		appendDiagnostic(i, outcome.current, outcome.current.reason)
	}
}

func appendReadBillingDemoOperator(result *readBillingDemoResult, runtimeStats *execdetails.RuntimeStatsColl, tree FlatPlanTree, estimator *readBillingDemoCopEstimator, idx int) readBillingDemoAppendOutcome {
	op := tree[idx]
	if op == nil || op.Origin == nil || op.Origin.ExplainID().String() == "_0" {
		return readBillingDemoAppendOutcome{success: true, status: readBillingDemoStatusSuccess}
	}
	if failure, ok := estimator.nodeFailures[idx]; ok {
		operator := readBillingDemoMaterializeCopFailure(runtimeStats, tree, failure)
		return readBillingDemoAppendOutcome{status: failure.status, current: operator, cause: failure}
	}
	operator, supported, reason := readBillingDemoClassifyOperator(op)
	operator.id = op.ExplainID().String()
	if actRows, ok := readBillingDemoOperatorActRows(runtimeStats, op); ok {
		operator.actRows = actRows
		operator.hasActRows = true
	}
	if !supported {
		return readBillingDemoAppendOutcome{status: readBillingDemoStatusUnsupported, current: operator.withReason(reason)}
	}
	if !readBillingDemoOperatorBillable(operator) {
		operator.status = readBillingDemoStatusOperatorOK
		result.operators = append(result.operators, operator.withReason(readBillingDemoReasonNonBillable))
		return readBillingDemoAppendOutcome{success: true, status: readBillingDemoStatusSuccess}
	}
	var units []readBillingDemoUnit
	var missingReason string
	var ok bool
	if op.IsRoot {
		units, missingReason, ok = readBillingDemoRootUnits(runtimeStats, tree, idx, op, operator)
	} else {
		outcome := readBillingDemoCopUnits(estimator, idx, operator)
		if outcome.success {
			units, ok = outcome.units, true
		} else {
			failure := outcome.failure
			currentReason := failure.reason
			cause := readBillingDemoCopFailure{}
			if failure.kind == readBillingDemoCopFailureIntrinsicCause && failure.failingIdx != idx {
				currentReason = readBillingDemoReasonDependentCopInputUnavailable
				cause = failure
			}
			return readBillingDemoAppendOutcome{
				status:  failure.status,
				current: operator.withReason(currentReason),
				cause:   cause,
			}
		}
	}
	if !ok {
		if missingReason == "" {
			if op.IsRoot {
				missingReason = readBillingDemoReasonMissingRuntimeBytes
			} else {
				missingReason = readBillingDemoReasonMissingScanDetail
			}
		}
		return readBillingDemoAppendOutcome{status: readBillingDemoStatusUnknownInput, current: operator.withReason(missingReason)}
	}
	operator.status = readBillingDemoStatusOperatorOK
	operator.reason = readBillingDemoReasonNone
	operator.units = units
	result.operators = append(result.operators, operator)
	return readBillingDemoAppendOutcome{success: true, status: readBillingDemoStatusSuccess}
}

func readBillingDemoMaterializeCopFailure(runtimeStats *execdetails.RuntimeStatsColl, tree FlatPlanTree, failure readBillingDemoCopFailure) readBillingDemoOperatorResult {
	if failure.failingIdx < 0 || failure.failingIdx >= len(tree) || tree[failure.failingIdx] == nil || tree[failure.failingIdx].Origin == nil {
		return readBillingDemoOperatorResult{
			id:           "cop_structure",
			site:         readBillingDemoSiteTiKV,
			opClass:      readBillingDemoOpClassRangeScan,
			operatorKind: "cop_structure",
			reason:       failure.reason,
		}
	}
	op := tree[failure.failingIdx]
	operator, _, _ := readBillingDemoClassifyOperator(op)
	operator.id = op.ExplainID().String()
	operator.reason = failure.reason
	if actRows, ok := readBillingDemoOperatorActRows(runtimeStats, op); ok {
		operator.actRows = actRows
		operator.hasActRows = true
	}
	return operator
}

func (op readBillingDemoOperatorResult) withReason(reason string) readBillingDemoOperatorResult {
	op.reason = reason
	return op
}

func readBillingDemoOperatorBillable(op readBillingDemoOperatorResult) bool {
	switch op.opClass {
	case readBillingDemoOpClassFilter, readBillingDemoOpClassProjection, readBillingDemoOpClassLimit,
		readBillingDemoOpClassTopN, readBillingDemoOpClassSort, readBillingDemoOpClassWindow,
		readBillingDemoOpClassHashAgg, readBillingDemoOpClassStreamAgg, readBillingDemoOpClassHashJoin,
		readBillingDemoOpClassMergeJoin, readBillingDemoOpClassLookupJoin, readBillingDemoOpClassRangeScan,
		readBillingDemoOpClassReaderTransport, readBillingDemoOpClassOverlayReader,
		readBillingDemoOpClassKVMutation, readBillingDemoOpClassKVWrite:
		return true
	case readBillingDemoOpClassPointLookup:
		return op.id == "point_lookup@statement"
	default:
		return false
	}
}

func readBillingDemoOperatorActRows(runtimeStats *execdetails.RuntimeStatsColl, op *FlatOperator) (int64, bool) {
	if op == nil {
		return 0, false
	}
	if op.IsRoot {
		return readBillingDemoPlanActRows(runtimeStats, op.Origin.ID())
	}
	evidence := readBillingDemoExactCopRowsEvidence(runtimeStats, op.Origin.ID())
	if evidence.state == readBillingDemoCopRowsObserved {
		return evidence.rows, true
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
	case plancodec.TypeInsert, plancodec.TypeUpdate, plancodec.TypeDelete:
		return readBillingDemoOperatorResult{site: readBillingDemoSiteTiDB, opClass: readBillingDemoOpClassWrapper, operatorKind: operatorKind}, true, ""
	case plancodec.TypeExchangeReceiver, plancodec.TypeExchangeSender:
		return readBillingDemoOperatorResult{site: readBillingDemoSiteTiDB, opClass: readBillingDemoOpClassReaderReceive, operatorKind: operatorKind}, false, readBillingDemoReasonUnsupportedMPP
	case plancodec.TypeIndexMerge:
		return readBillingDemoOperatorResult{site: readBillingDemoSiteTiDB, opClass: readBillingDemoOpClassLookupReader, operatorKind: operatorKind}, true, ""
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

func readBillingDemoJoinConditionCount(join *physicalop.BasePhysicalJoin) int64 {
	return int64(len(join.LeftConditions) + len(join.RightConditions) + len(join.OtherConditions))
}

func readBillingDemoCompareFilterCount(filters *physicalop.ColWithCmpFuncManager) int64 {
	if filters == nil {
		return 0
	}
	return int64(len(filters.OpType))
}

func readBillingDemoExpressionCount(plan base.Plan) (int64, bool) {
	switch p := plan.(type) {
	case *physicalop.PhysicalSelection:
		return int64(len(p.Conditions)), true
	case *physicalop.PhysicalProjection:
		return int64(len(p.Exprs)), true
	case *physicalop.PhysicalHashAgg:
		return int64(len(p.GroupByItems) + len(p.AggFuncs)), true
	case *physicalop.PhysicalStreamAgg:
		return int64(len(p.GroupByItems) + len(p.AggFuncs)), true
	case *physicalop.PhysicalHashJoin:
		if len(p.LeftJoinKeys) != len(p.RightJoinKeys) || len(p.LeftNAJoinKeys) != len(p.RightNAJoinKeys) {
			return 0, false
		}
		return int64(len(p.EqualConditions)+len(p.NAEqualConditions)) + readBillingDemoJoinConditionCount(&p.BasePhysicalJoin), true
	case *physicalop.PhysicalMergeJoin:
		if len(p.LeftJoinKeys) != len(p.RightJoinKeys) {
			return 0, false
		}
		return int64(len(p.CompareFuncs)) + readBillingDemoJoinConditionCount(&p.BasePhysicalJoin), true
	case *physicalop.PhysicalIndexHashJoin:
		if len(p.OuterHashKeys) != len(p.InnerHashKeys) {
			return 0, false
		}
		return int64(len(p.OuterHashKeys)) + readBillingDemoJoinConditionCount(&p.BasePhysicalJoin) + readBillingDemoCompareFilterCount(p.CompareFilters), true
	case *physicalop.PhysicalIndexMergeJoin:
		if p.NeedOuterSort != (len(p.OuterCompareFuncs) > 0) {
			return 0, false
		}
		return int64(len(p.CompareFuncs)+len(p.OuterCompareFuncs)) + readBillingDemoJoinConditionCount(&p.BasePhysicalJoin) + readBillingDemoCompareFilterCount(p.CompareFilters), true
	case *physicalop.PhysicalIndexJoin:
		if len(p.OuterJoinKeys) != len(p.InnerJoinKeys) {
			return 0, false
		}
		return int64(len(p.OuterJoinKeys)) + readBillingDemoJoinConditionCount(&p.BasePhysicalJoin) + readBillingDemoCompareFilterCount(p.CompareFilters), true
	case *physicalop.PhysicalWindow:
		count := len(p.WindowFuncDescs) + len(p.PartitionBy) + len(p.OrderBy)
		if p.Frame != nil {
			if p.Frame.Start != nil {
				count += len(p.Frame.Start.CalcFuncs)
			}
			if p.Frame.End != nil {
				count += len(p.Frame.End.CalcFuncs)
			}
		}
		return int64(count), true
	default:
		return 0, false
	}
}

func readBillingDemoOrderingMaterialized(op, child *FlatOperator) bool {
	if op == nil || op.Origin == nil || child == nil || child.Origin == nil {
		return false
	}
	childSchema := child.Origin.Schema()
	if projection, ok := child.Origin.(*physicalop.PhysicalProjection); ok &&
		(childSchema == nil || childSchema.Len() != len(projection.Exprs)) {
		return false
	}
	checkExpr := func(expr expression.Expression) bool {
		if expr == nil {
			return false
		}
		_, scalar := expr.(*expression.ScalarFunction)
		if scalar {
			return false
		}
		if col, ok := expr.(*expression.Column); ok {
			return childSchema != nil && childSchema.ColumnIndex(col) >= 0
		}
		return true
	}
	switch p := op.Origin.(type) {
	case *physicalop.PhysicalSort:
		for _, item := range p.ByItems {
			if item == nil || !checkExpr(item.Expr) {
				return false
			}
		}
		return true
	case *physicalop.PhysicalTopN:
		for _, item := range p.ByItems {
			if item == nil || !checkExpr(item.Expr) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func readBillingDemoCheckedWork(rows int64, multiplier float64) (float64, bool) {
	if rows < 0 || multiplier < 0 || math.IsNaN(multiplier) || math.IsInf(multiplier, 0) {
		return 0, false
	}
	work := float64(rows) * multiplier
	return work, work >= 0 && !math.IsNaN(work) && !math.IsInf(work, 0)
}

func readBillingDemoHashStateRows(runtimeStats *execdetails.RuntimeStatsColl, planID int) (int64, bool) {
	if runtimeStats == nil || !runtimeStats.ExistsRootStats(planID) {
		return 0, false
	}
	_, groups := runtimeStats.GetRootStats(planID).MergeStats()
	for _, group := range groups {
		if stats, ok := group.(execdetails.HashTableRuntimeStats); ok {
			return stats.HashTableRows(), true
		}
	}
	return 0, false
}

func readBillingDemoRootUnits(runtimeStats *execdetails.RuntimeStatsColl, tree FlatPlanTree, idx int, op *FlatOperator, operator readBillingDemoOperatorResult) ([]readBillingDemoUnit, string, bool) {
	outputRows, hasOutputRows := readBillingDemoPlanActRows(runtimeStats, op.Origin.ID())
	if !hasOutputRows || outputRows < 0 {
		return nil, readBillingDemoReasonMissingRuntimeRows, false
	}
	units := []readBillingDemoUnit{readBillingDemoFixedEventUnit(readBillingDemoInputSourceRuntimeChunkBytes)}
	appendExpressionCPU := func(rows int64) (string, bool) {
		exprCount, ok := readBillingDemoExpressionCount(op.Origin)
		if !ok || exprCount < 0 {
			return readBillingDemoReasonMissingExpressionCount, false
		}
		work, ok := readBillingDemoCheckedWork(rows, float64(exprCount))
		if !ok {
			return readBillingDemoReasonMissingExpressionCount, false
		}
		units = append(units,
			readBillingDemoUnit{unit: readBillingDemoUnitExpressionCount, source: readBillingDemoInputSourcePhysicalPlan, side: readBillingDemoInputSideAll, value: float64(exprCount), widthSource: explainRUWidthSourceNotApplicable},
			readBillingDemoUnit{unit: readBillingDemoUnitCPUWork, source: readBillingDemoInputSourceRuntimeChildActRows, side: readBillingDemoInputSideAll, value: work, widthSource: explainRUWidthSourceNotApplicable},
		)
		return "", true
	}
	switch operator.opClass {
	case readBillingDemoOpClassHashJoin, readBillingDemoOpClassMergeJoin, readBillingDemoOpClassLookupJoin:
		if idx < 0 || idx >= len(tree) || len(tree[idx].ChildrenIdx) != 2 {
			return nil, readBillingDemoReasonMissingExpressionCount, false
		}
		var inputRows int64
		for childOrder, childIdx := range tree[idx].ChildrenIdx {
			if childIdx < 0 || childIdx >= len(tree) || tree[childIdx] == nil || !tree[childIdx].IsRoot {
				return nil, readBillingDemoReasonMissingRuntimeRows, false
			}
			rows, ok := readBillingDemoPlanActRows(runtimeStats, tree[childIdx].Origin.ID())
			if !ok || rows < 0 || (rows > 0 && inputRows > math.MaxInt64-rows) {
				return nil, readBillingDemoReasonMissingRuntimeRows, false
			}
			inputRows += rows
			side := readBillingDemoInputSideLeft
			if childOrder == 1 {
				side = readBillingDemoInputSideRight
			}
			if operator.opClass == readBillingDemoOpClassHashJoin {
				if tree[childIdx].Label == BuildSide {
					side = readBillingDemoInputSideBuild
				} else if tree[childIdx].Label == ProbeSide {
					side = readBillingDemoInputSideProbe
				}
			}
			units = append(units, readBillingDemoUnit{unit: readBillingDemoUnitInputRows, source: readBillingDemoInputSourceRuntimeChildActRows, side: side, value: float64(rows), widthSource: explainRUWidthSourceNotApplicable})
		}
		if reason, ok := appendExpressionCPU(inputRows); !ok {
			return nil, reason, false
		}
		units = append(units, readBillingDemoUnit{unit: readBillingDemoUnitJoinOutputRows, source: readBillingDemoInputSourceRuntimeOperatorActRows, side: readBillingDemoInputSideAll, value: float64(outputRows), widthSource: explainRUWidthSourceNotApplicable})
		if operator.opClass == readBillingDemoOpClassHashJoin {
			stateRows, ok := readBillingDemoHashStateRows(runtimeStats, op.Origin.ID())
			if !ok {
				return nil, readBillingDemoReasonMissingHashStateRows, false
			}
			if stateRows < 0 {
				return nil, readBillingDemoReasonInvalidHashStateRows, false
			}
			units = append(units, readBillingDemoUnit{unit: readBillingDemoUnitHashStateRows, source: readBillingDemoInputSourceHashJoinRuntime, side: readBillingDemoInputSideBuild, value: float64(stateRows), widthSource: explainRUWidthSourceNotApplicable})
		}
	default:
		if idx < 0 || idx >= len(tree) || len(tree[idx].ChildrenIdx) != 1 {
			return nil, readBillingDemoReasonMissingRuntimeRows, false
		}
		childIdx := tree[idx].ChildrenIdx[0]
		if childIdx < 0 || childIdx >= len(tree) || tree[childIdx] == nil {
			return nil, readBillingDemoReasonMissingRuntimeRows, false
		}
		inputRows, ok := readBillingDemoPlanActRows(runtimeStats, tree[childIdx].Origin.ID())
		if !ok || inputRows < 0 {
			return nil, readBillingDemoReasonMissingRuntimeRows, false
		}
		units = append(units, readBillingDemoUnit{unit: readBillingDemoUnitInputRows, source: readBillingDemoInputSourceRuntimeChildActRows, side: readBillingDemoInputSideAll, value: float64(inputRows), widthSource: explainRUWidthSourceNotApplicable})
		orderWork, ok := readBillingDemoOrderingWorkUnit(op, operator.opClass, inputRows)
		if !ok {
			return nil, readBillingDemoOrderingFailureReason(op, operator.opClass), false
		}
		if orderWork.unit != "" {
			if !readBillingDemoOrderingMaterialized(op, tree[childIdx]) {
				return nil, readBillingDemoReasonMissingOrderingProjection, false
			}
			orderWork.unit = readBillingDemoUnitCPUWork
			units = append(units, orderWork)
		} else if operator.opClass == readBillingDemoOpClassLimit || operator.opClass == readBillingDemoOpClassOverlayReader {
			units = append(units, readBillingDemoUnit{unit: readBillingDemoUnitCPUWork, source: readBillingDemoInputSourceRuntimeChildActRows, side: readBillingDemoInputSideAll, value: float64(inputRows), widthSource: explainRUWidthSourceNotApplicable})
		} else if reason, ok := appendExpressionCPU(inputRows); !ok {
			return nil, reason, false
		}
		if operator.opClass == readBillingDemoOpClassHashAgg {
			units = append(units, readBillingDemoUnit{unit: readBillingDemoUnitHashStateRows, source: readBillingDemoInputSourceRuntimeOperatorActRows, side: readBillingDemoInputSideAll, value: float64(outputRows), widthSource: explainRUWidthSourceNotApplicable})
		}
	}
	if _, outputBytes, ok := readBillingDemoRootOutputRowsAndBytes(runtimeStats, op.Origin.ID()); ok && readBillingDemoOperatorHasOutputShadows(operator.opClass) {
		units = append(units, readBillingDemoRuntimeChunkOutputUnits(outputRows, outputBytes)...)
	}
	return units, "", true
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

func readBillingDemoRuntimeChunkOutputUnits(rows, bytes int64) []readBillingDemoUnit {
	if rows < 0 || bytes < 0 {
		return nil
	}
	return []readBillingDemoUnit{
		{unit: readBillingDemoUnitOutputRows, source: readBillingDemoInputSourceRuntimeChunkBytes, side: readBillingDemoInputSideAll, value: float64(rows), widthSource: explainRUWidthSourceNotApplicable},
		{unit: readBillingDemoUnitOutputBytes, source: readBillingDemoInputSourceRuntimeChunkBytes, side: readBillingDemoInputSideAll, value: float64(bytes), rowWidth: readBillingDemoAverageRowWidth(rows, float64(bytes)), widthSource: explainRUWidthSourceRuntimeChunkAvg},
	}
}

func readBillingDemoIsAggClass(opClass string) bool {
	return opClass == readBillingDemoOpClassHashAgg || opClass == readBillingDemoOpClassStreamAgg
}

func readBillingDemoOperatorHasOutputShadows(opClass string) bool {
	if readBillingDemoIsAggClass(opClass) {
		return true
	}
	switch opClass {
	case readBillingDemoOpClassHashJoin, readBillingDemoOpClassMergeJoin, readBillingDemoOpClassLookupJoin:
		return true
	default:
		return false
	}
}

func readBillingDemoAverageRowWidth(rows int64, bytes float64) float64 {
	if rows <= 0 || bytes <= 0 {
		return 0
	}
	return bytes / float64(rows)
}

func readBillingDemoOrderingWorkUnit(op *FlatOperator, opClass string, inputRows int64) (readBillingDemoUnit, bool) {
	if opClass != readBillingDemoOpClassTopN && opClass != readBillingDemoOpClassSort {
		return readBillingDemoUnit{}, true
	}
	if op == nil || op.Origin == nil || inputRows < 0 {
		return readBillingDemoUnit{}, false
	}

	logWidth := max(float64(inputRows), 2)
	if opClass == readBillingDemoOpClassTopN {
		topN, ok := op.Origin.(*physicalop.PhysicalTopN)
		if !ok {
			return readBillingDemoUnit{}, false
		}
		if topN.Count == 0 {
			return readBillingDemoUnit{
				unit:        readBillingDemoUnitOrderWork,
				source:      readBillingDemoInputSourceRuntimeOrderingWork,
				side:        readBillingDemoInputSideAll,
				value:       0,
				widthSource: explainRUWidthSourceNotApplicable,
			}, true
		}
		if topN.Count > math.MaxUint64-topN.Offset {
			return readBillingDemoUnit{}, false
		}
		effectiveK := min(uint64(inputRows), topN.Offset+topN.Count)
		logWidth = max(float64(effectiveK), 2)
	}
	work := float64(inputRows) * math.Log2(logWidth)
	if work < 0 || math.IsNaN(work) || math.IsInf(work, 0) {
		return readBillingDemoUnit{}, false
	}
	return readBillingDemoUnit{
		unit:        readBillingDemoUnitOrderWork,
		source:      readBillingDemoInputSourceRuntimeOrderingWork,
		side:        readBillingDemoInputSideAll,
		value:       work,
		widthSource: explainRUWidthSourceNotApplicable,
	}, true
}

func readBillingDemoOrderingFailureReason(op *FlatOperator, opClass string) string {
	if opClass == readBillingDemoOpClassTopN && op != nil {
		if topN, ok := op.Origin.(*physicalop.PhysicalTopN); ok && topN.Count > math.MaxUint64-topN.Offset {
			return readBillingDemoReasonInvalidTopNBound
		}
	}
	return readBillingDemoReasonInvalidOrderingWork
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

func (e *readBillingDemoCopEstimator) aggOutputShadowUnits(idx int, opClass string) []readBillingDemoUnit {
	if !readBillingDemoIsAggClass(opClass) || idx < 0 || idx >= len(e.tree) || e.tree[idx] == nil || e.tree[idx].Origin == nil {
		return nil
	}
	rowsEvidence := readBillingDemoExactCopRowsEvidence(e.runtimeStats, e.tree[idx].Origin.ID())
	if rowsEvidence.state != readBillingDemoCopRowsObserved {
		return nil
	}
	expectedTasks := e.runtimeStats.GetExpectedCopTasks(e.tree[idx].Origin.ID())
	if expectedTasks <= 0 || rowsEvidence.tasks != expectedTasks {
		return nil
	}
	componentID := e.componentID[idx]
	if componentID < 0 || componentID >= len(e.components) {
		return nil
	}
	if maxTasks := e.components[componentID].maxSummaryTasks; maxTasks > 0 && rowsEvidence.tasks < maxTasks {
		return nil
	}
	units := []readBillingDemoUnit{{
		unit:        readBillingDemoUnitOutputRows,
		source:      readBillingDemoInputSourceRuntimeOperatorActRows,
		side:        readBillingDemoInputSideAll,
		value:       float64(rowsEvidence.rows),
		widthSource: explainRUWidthSourceNotApplicable,
	}}

	parentIdx := e.parentIdx[idx]
	if parentIdx < 0 || parentIdx >= len(e.tree) {
		return units
	}
	parent := e.tree[parentIdx]
	if parent == nil || parent.Origin == nil || !parent.IsRoot || len(parent.ChildrenIdx) != 1 || parent.ChildrenIdx[0] != idx {
		return units
	}
	parentOperator, supported, _ := readBillingDemoClassifyOperator(parent)
	if !supported || parentOperator.opClass != readBillingDemoOpClassReaderReceive {
		return units
	}
	switch parent.Origin.TP() {
	case plancodec.TypeTableReader, plancodec.TypeIndexReader:
	default:
		return units
	}
	readerRows, readerBytes, ok := readBillingDemoRootOutputRowsAndBytes(e.runtimeStats, parent.Origin.ID())
	if !ok || readerRows != rowsEvidence.rows || readerRows < 0 || readerBytes < 0 {
		return units
	}
	return append(units, readBillingDemoUnit{
		unit:        readBillingDemoUnitOutputBytes,
		source:      readBillingDemoInputSourceRuntimeReaderOutput,
		side:        readBillingDemoInputSideAll,
		value:       float64(readerBytes),
		rowWidth:    readBillingDemoAverageRowWidth(readerRows, float64(readerBytes)),
		widthSource: explainRUWidthSourceRuntimeReaderOutputChunkAvg,
	})
}

func readBillingDemoCopUnits(estimator *readBillingDemoCopEstimator, idx int, operator readBillingDemoOperatorResult) readBillingDemoCopUnitOutcome {
	if failure, ok := estimator.nodeFailures[idx]; ok {
		return readBillingDemoCopUnitOutcome{failure: failure}
	}
	if operator.opClass == readBillingDemoOpClassRangeScan {
		if idx < 0 || idx >= len(estimator.tree) || len(estimator.tree[idx].ChildrenIdx) != 0 {
			return readBillingDemoCopUnitOutcome{failure: readBillingDemoCopFailureAt(idx, readBillingDemoCopFailureCurrent, readBillingDemoStatusUnsupported, readBillingDemoReasonUnsupportedCopStructure)}
		}
		component := estimator.components[estimator.componentID[idx]]
		if component.scanCount != 1 || component.detailHolderCount > 1 {
			return readBillingDemoCopUnitOutcome{failure: readBillingDemoCopFailureAt(idx, readBillingDemoCopFailureCurrent, readBillingDemoStatusUnknownInput, readBillingDemoReasonAmbiguousCopScanWidth)}
		}
		if component.detailHolderCount != 1 || component.scanObservedTasks <= 0 || component.scanExpectedTasks <= 0 {
			return readBillingDemoCopUnitOutcome{failure: readBillingDemoCopFailureAt(idx, readBillingDemoCopFailureCurrent, readBillingDemoStatusUnknownInput, readBillingDemoReasonMissingScanWidthEvidence)}
		}
		if component.scanObservedTasks != component.scanExpectedTasks ||
			component.scanDetailExpectedTasks != component.scanExpectedTasks ||
			component.scanDetailRecords != component.scanDetailExpectedTasks {
			return readBillingDemoCopUnitOutcome{failure: readBillingDemoCopFailureAt(idx, readBillingDemoCopFailureCurrent, readBillingDemoStatusUnknownInput, readBillingDemoReasonIncompleteCopRuntimeRows)}
		}
		scanDetail := component.scanDetail
		scanInputRows, scanInputBytes, ok := readBillingDemoRangeScanInput(scanDetail.TotalKeys, scanDetail.ProcessedKeys, scanDetail.ProcessedKeysSize)
		if !ok {
			return readBillingDemoCopUnitOutcome{failure: readBillingDemoCopFailureAt(idx, readBillingDemoCopFailureCurrent, readBillingDemoStatusUnknownInput, readBillingDemoReasonMissingScanWidthEvidence)}
		}
		rowWidth := readBillingDemoAverageRowWidth(scanInputRows, scanInputBytes)
		units := []readBillingDemoUnit{readBillingDemoFixedEventUnit(readBillingDemoInputSourceScanDetail)}
		units = append(units,
			readBillingDemoUnit{unit: readBillingDemoUnitInputRows, source: readBillingDemoInputSourceScanDetail, side: readBillingDemoInputSideAll, value: float64(scanInputRows), rowWidth: rowWidth, widthSource: explainRUWidthSourceScanDetailProcessedAvg},
			readBillingDemoUnit{unit: readBillingDemoUnitInputBytes, source: readBillingDemoInputSourceScanDetail, side: readBillingDemoInputSideAll, value: scanInputBytes, rowWidth: rowWidth, widthSource: explainRUWidthSourceScanDetailProcessedAvg},
			readBillingDemoUnit{unit: readBillingDemoUnitScanBytes, source: readBillingDemoInputSourceScanDetail, side: readBillingDemoInputSideAll, value: scanInputBytes, rowWidth: rowWidth, widthSource: explainRUWidthSourceScanDetailProcessedEstimate},
		)
		return readBillingDemoCopUnitOutcome{success: true, units: units}
	}
	childIdx, failure, ok := estimator.directCopChild(idx)
	if !ok {
		return readBillingDemoCopUnitOutcome{failure: failure}
	}
	rowsEvidence := readBillingDemoExactCopRowsEvidence(estimator.runtimeStats, estimator.tree[childIdx].Origin.ID())
	if rowsEvidence.state == readBillingDemoCopRowsMissing {
		return readBillingDemoCopUnitOutcome{failure: readBillingDemoCopFailureAt(idx, readBillingDemoCopFailureCurrent, readBillingDemoStatusUnknownInput, readBillingDemoReasonMissingCopChildRuntimeRows)}
	}
	if rowsEvidence.state == readBillingDemoCopRowsInvalid {
		return readBillingDemoCopUnitOutcome{failure: readBillingDemoCopFailureAt(childIdx, readBillingDemoCopFailureIntrinsicCause, readBillingDemoStatusUnknownInput, readBillingDemoReasonInvalidCopRuntimeRows)}
	}
	component := estimator.components[estimator.componentID[idx]]
	if component.maxSummaryTasks > 0 && rowsEvidence.tasks < component.maxSummaryTasks {
		return readBillingDemoCopUnitOutcome{failure: readBillingDemoCopFailureAt(idx, readBillingDemoCopFailureCurrent, readBillingDemoStatusUnknownInput, readBillingDemoReasonIncompleteCopRuntimeRows)}
	}
	units := []readBillingDemoUnit{readBillingDemoFixedEventUnit(readBillingDemoInputSourceRuntimeChildActRows)}
	units = append(units,
		readBillingDemoUnit{unit: readBillingDemoUnitInputRows, source: readBillingDemoInputSourceRuntimeChildActRows, side: readBillingDemoInputSideAll, value: float64(rowsEvidence.rows), widthSource: explainRUWidthSourceNotApplicable},
	)
	orderWork, ok := readBillingDemoOrderingWorkUnit(estimator.tree[idx], operator.opClass, rowsEvidence.rows)
	if !ok {
		return readBillingDemoCopUnitOutcome{failure: readBillingDemoCopFailureAt(idx, readBillingDemoCopFailureCurrent, readBillingDemoStatusUnknownInput, readBillingDemoOrderingFailureReason(estimator.tree[idx], operator.opClass))}
	}
	if orderWork.unit != "" {
		if !readBillingDemoOrderingMaterialized(estimator.tree[idx], estimator.tree[childIdx]) {
			return readBillingDemoCopUnitOutcome{failure: readBillingDemoCopFailureAt(idx, readBillingDemoCopFailureCurrent, readBillingDemoStatusUnknownInput, readBillingDemoReasonMissingOrderingProjection)}
		}
		orderWork.unit = readBillingDemoUnitCPUWork
		units = append(units, orderWork)
	} else if operator.opClass == readBillingDemoOpClassLimit {
		units = append(units, readBillingDemoUnit{unit: readBillingDemoUnitCPUWork, source: readBillingDemoInputSourceRuntimeChildActRows, side: readBillingDemoInputSideAll, value: float64(rowsEvidence.rows), widthSource: explainRUWidthSourceNotApplicable})
	} else {
		exprCount, ok := readBillingDemoExpressionCount(estimator.tree[idx].Origin)
		if !ok || exprCount < 0 {
			return readBillingDemoCopUnitOutcome{failure: readBillingDemoCopFailureAt(idx, readBillingDemoCopFailureCurrent, readBillingDemoStatusUnknownInput, readBillingDemoReasonMissingExpressionCount)}
		}
		work, ok := readBillingDemoCheckedWork(rowsEvidence.rows, float64(exprCount))
		if !ok {
			return readBillingDemoCopUnitOutcome{failure: readBillingDemoCopFailureAt(idx, readBillingDemoCopFailureCurrent, readBillingDemoStatusUnknownInput, readBillingDemoReasonMissingExpressionCount)}
		}
		units = append(units,
			readBillingDemoUnit{unit: readBillingDemoUnitExpressionCount, source: readBillingDemoInputSourcePhysicalPlan, side: readBillingDemoInputSideAll, value: float64(exprCount), widthSource: explainRUWidthSourceNotApplicable},
			readBillingDemoUnit{unit: readBillingDemoUnitCPUWork, source: readBillingDemoInputSourceRuntimeChildActRows, side: readBillingDemoInputSideAll, value: work, widthSource: explainRUWidthSourceNotApplicable},
		)
	}
	aggUnits := estimator.aggOutputShadowUnits(idx, operator.opClass)
	if operator.opClass == readBillingDemoOpClassHashAgg {
		if len(aggUnits) == 0 {
			return readBillingDemoCopUnitOutcome{failure: readBillingDemoCopFailureAt(idx, readBillingDemoCopFailureCurrent, readBillingDemoStatusUnknownInput, readBillingDemoReasonIncompleteCopRuntimeRows)}
		}
		units = append(units, readBillingDemoUnit{unit: readBillingDemoUnitHashStateRows, source: readBillingDemoInputSourceRuntimeOperatorActRows, side: readBillingDemoInputSideAll, value: aggUnits[0].value, widthSource: explainRUWidthSourceNotApplicable})
	}
	units = append(units, aggUnits...)
	return readBillingDemoCopUnitOutcome{success: true, units: units}
}

func readBillingDemoRangeScanInput(totalKeys, processedKeys, processedKeysSize int64) (int64, float64, bool) {
	if totalKeys == 0 && processedKeys == 0 && processedKeysSize == 0 {
		return 0, 0, true
	}
	if totalKeys <= 0 || processedKeys <= 0 || processedKeysSize <= 0 {
		return 0, 0, false
	}
	inputBytes := float64(processedKeysSize) / float64(processedKeys) * float64(totalKeys)
	if inputBytes < 0 || math.IsNaN(inputBytes) || math.IsInf(inputBytes, 0) {
		return 0, 0, false
	}
	return totalKeys, inputBytes, true
}

func recordReadBillingDemoResult(result readBillingDemoResult) {
	status := result.status
	if status == "" {
		status = readBillingDemoStatusUnknownInput
	}
	weightVersion := readBillingDemoActiveWeightVersion()
	metrics.RecordReadBillingDemoStatement(status, readBillingDemoModelVersion, weightVersion)
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
		metrics.RecordReadBillingDemoOperatorStatus(op.site, op.opClass, op.operatorKind, opStatus, reason, readBillingDemoModelVersion, weightVersion)
		if opStatus != readBillingDemoStatusOperatorOK || !readBillingDemoOperatorBillable(op) {
			continue
		}
		for _, unit := range op.units {
			metrics.AddReadBillingDemoBaseUnits(op.site, op.opClass, op.operatorKind, unit.unit, unit.source, unit.side, readBillingDemoModelVersion, weightVersion, unit.value)
			metrics.ObserveReadBillingDemoRowWidth(op.site, op.opClass, op.operatorKind, unit.widthSource, readBillingDemoModelVersion, weightVersion, unit.rowWidth)
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

// explainRUTargetGateStatus is the pre-execution safety gate for FORMAT='RU'.
// SELECT keeps the side-effect-free checks from the read-side demo; write DML
// is limited to foreground statements with statement-local mutation recording.
// TiKV commit detail availability is handled independently after execution.
func explainRUTargetGateStatus(stmt ast.StmtNode) explainRUStatus {
	if _, ok := explainRUWriteDMLKind(stmt); ok {
		return explainRUStatusSuccess
	}
	return explainRUSelectGateStatus(stmt)
}

func explainRUWriteDMLKind(stmt ast.StmtNode) (string, bool) {
	switch x := stmt.(type) {
	case *ast.InsertStmt:
		if x == nil || x.IsReplace {
			return "", false
		}
		if len(x.OnDuplicate) > 0 {
			return "upsert", true
		}
		if x.IgnoreErr {
			return "insert_ignore", true
		}
		return "insert", true
	case *ast.UpdateStmt:
		if x == nil {
			return "", false
		}
		return "update", true
	case *ast.DeleteStmt:
		if x == nil {
			return "", false
		}
		return "delete", true
	default:
		return "", false
	}
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
	if gateStatus := explainRUTargetGateStatus(e.ExecStmt); gateStatus != explainRUStatusSuccess {
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
	snapshot, snapshotStatus := explainRUExtractComponentSnapshot(runtimeStats, e.TargetPlan.ID())
	metrics.RecordExplainRUComponentSnapshot(string(snapshotStatus))
	var snapshotRUV2Metrics *execdetails.RUV2Metrics
	if snapshot != nil {
		snapshotRUV2Metrics = snapshot.Metrics
	}
	result := buildReadBillingDemoResult(e.SCtx(), e.TargetPlan, e.ExecStmt, nil, snapshotRUV2Metrics)
	if result.status != readBillingDemoStatusSuccess {
		operator := ""
		if len(result.operators) > 0 {
			op := result.operators[0]
			operator = " operator=" + op.site + "/" + op.opClass + "/" + op.operatorKind
		}
		status = explainRUStatusError
		return errors.NewNoStackErrorf(
			"EXPLAIN ANALYZE FORMAT='RU' cannot render a complete preview RU model result: status=%s reason=%s%s",
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
		section:   explainRUSectionSummary,
		component: "total_preview_ru",
		source:    explainRUSourceSummaryTotal,
		note:      explainRUReadBillingSummaryNote(snapshotStatus, result),
	}}
	totalPreviewRU := 0.0
	weightsReady := readBillingDemoWeightsValid(readBillingDemoV4Weights)
	completeTotal := weightsReady && result.status == readBillingDemoStatusSuccess
	for _, op := range result.operators {
		if op.status != readBillingDemoStatusOperatorOK {
			if readBillingDemoOperatorBillable(op) {
				completeTotal = false
			}
			if op.emitStatusRow {
				rows = append(rows, explainRUReadBillingStatusRow(op))
			}
			continue
		}
		if !readBillingDemoOperatorBillable(op) {
			continue
		}
		for _, unit := range op.units {
			row := explainRUReadBillingUnitRow(op, unit)
			if _, semantic := readBillingDemoUnitWeight(readBillingDemoV4Weights, unit.unit); semantic {
				if weight, previewRU, ok := readBillingDemoUnitPreviewRU(unit, readBillingDemoV4Weights); ok {
					row.weight = weight
					row.hasWeight = true
					row.previewRU = previewRU
					row.hasPreviewRU = true
					nextTotal := totalPreviewRU + previewRU
					if nextTotal < 0 || math.IsNaN(nextTotal) || math.IsInf(nextTotal, 0) {
						completeTotal = false
					} else {
						totalPreviewRU = nextTotal
					}
				} else {
					completeTotal = false
				}
			}
			rows = append(rows, row)
		}
	}
	if completeTotal {
		rows[0].previewRU = totalPreviewRU
		rows[0].hasPreviewRU = true
	}
	return rows
}

func explainRUReadBillingStatusRow(op readBillingDemoOperatorResult) explainRURow {
	row := explainRURow{
		section:       explainRUSectionPlan,
		id:            op.id,
		component:     op.operatorKind,
		operatorClass: op.site + "/" + op.opClass,
		note:          "weight_version=" + readBillingDemoActiveWeightVersion(),
	}
	row.note = appendExplainRUNote(row.note, "status="+op.status)
	if op.reason != "" {
		row.note = appendExplainRUNote(row.note, "reason="+op.reason)
	}
	if op.scope != "" {
		row.note = appendExplainRUNote(row.note, "scope="+op.scope)
	}
	if op.dmlKind != "" {
		row.note = appendExplainRUNote(row.note, "dml_kind="+op.dmlKind)
	}
	if op.hasActRows {
		row.actRows = op.actRows
		row.hasActRows = true
		row.outputRows = op.actRows
		row.hasOutputRows = true
	}
	return row
}

func explainRUReadBillingSummaryNote(snapshotStatus explainRUComponentSnapshotStatus, result readBillingDemoResult) string {
	note := "weight_version=" + readBillingDemoActiveWeightVersion()
	if !readBillingDemoWeightsValid(readBillingDemoV4Weights) {
		note = appendExplainRUNote(note, readBillingDemoReasonUncalibratedWeights)
	}
	if snapshotStatus != explainRUComponentSnapshotOK {
		note = appendExplainRUNote(note, "component_snapshot_"+string(snapshotStatus))
	}
	for _, op := range result.operators {
		if op.uncalibrated {
			note = appendExplainRUNote(note, "mutation_weights_uncalibrated=true")
		}
		if op.status == readBillingDemoStatusPartial && op.reason != "" {
			note = appendExplainRUNote(note, "partial_"+op.reason)
		}
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
		note:           "input_side=" + unit.side + ",weight_version=" + readBillingDemoActiveWeightVersion(),
	}
	if op.scope != "" {
		row.note = appendExplainRUNote(row.note, "scope="+op.scope)
	}
	if op.dmlKind != "" {
		row.note = appendExplainRUNote(row.note, "dml_kind="+op.dmlKind)
	}
	if op.uncalibrated {
		row.note = appendExplainRUNote(row.note, "uncalibrated=true")
	}
	if unit.unit == readBillingDemoUnitWriteByte {
		row.note = appendExplainRUNote(row.note, "semantic_name=write_bytes")
	}
	if readBillingDemoUnitDiagnosticOnly(unit.unit) {
		row.note = appendExplainRUNote(row.note, "diagnostic_only=true")
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
		row.workRows = unit.value
		row.hasWorkRows = true
		row.count = int64(unit.value)
		row.hasCount = true
	case readBillingDemoUnitInputBytes:
		row.workBytes = unit.value
		row.hasWorkBytes = true
	case readBillingDemoUnitOutputRows:
		row.outputRows = int64(unit.value)
		row.hasOutputRows = true
		row.workRows = unit.value
		row.hasWorkRows = true
		row.count = int64(unit.value)
		row.hasCount = true
	case readBillingDemoUnitOutputBytes:
		row.workBytes = unit.value
		row.hasWorkBytes = true
	case readBillingDemoUnitOrderWork:
		row.workRows = unit.value
		row.hasWorkRows = true
	case readBillingDemoUnitCPUWork:
		row.workRows = unit.value
		row.hasWorkRows = true
	case readBillingDemoUnitScanBytes, readBillingDemoUnitNetBytes:
		row.workBytes = unit.value
		row.hasWorkBytes = true
	case readBillingDemoUnitExpressionCount, readBillingDemoUnitReadRequestCount, readBillingDemoUnitWriteRequestCount,
		readBillingDemoUnitHashStateRows, readBillingDemoUnitJoinOutputRows:
		row.count = int64(unit.value)
		row.hasCount = true
	case readBillingDemoUnitEncodedMutationCount, readBillingDemoUnitSetCount, readBillingDemoUnitDeleteCount,
		readBillingDemoUnitWriteKeys, readBillingDemoUnitPrewriteRegionNum, readBillingDemoUnitTiKVWriteRPCCount:
		row.count = int64(unit.value)
		row.hasCount = true
	case readBillingDemoUnitEncodedMutationBytes, readBillingDemoUnitKeyBytes, readBillingDemoUnitValueBytes,
		readBillingDemoUnitWriteByte:
		row.workBytes = unit.value
		row.hasWorkBytes = true
	}
	return row
}

func readBillingDemoUnitDiagnosticOnly(unit string) bool {
	switch unit {
	case readBillingDemoUnitFixedEvents, readBillingDemoUnitInputRows, readBillingDemoUnitInputBytes,
		readBillingDemoUnitOrderWork, readBillingDemoUnitExpressionCount, readBillingDemoUnitEncodedMutationCount,
		readBillingDemoUnitEncodedMutationBytes, readBillingDemoUnitSetCount, readBillingDemoUnitDeleteCount, readBillingDemoUnitKeyBytes,
		readBillingDemoUnitValueBytes, readBillingDemoUnitPrewriteRegionNum, readBillingDemoUnitTiKVWriteRPCCount,
		readBillingDemoUnitWriteKeys, readBillingDemoUnitWriteByte, readBillingDemoUnitOutputRows, readBillingDemoUnitOutputBytes:
		return true
	default:
		return false
	}
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
		formatOptionalCompactFloat(row.workRows, row.hasWorkRows),
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

func formatOptionalCompactFloat(v float64, ok bool) string {
	if !ok {
		return ""
	}
	return strconv.FormatFloat(v, 'f', -1, 64)
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
		workRows = row.workRows
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
	metrics.ObserveExplainRURow(row.section, component, operator, row.source, row.rowWidthSource, readBillingDemoActiveWeightVersion(), previewRU, workRows, workBytes, rowWidth)
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
