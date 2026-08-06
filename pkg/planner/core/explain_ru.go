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
	"github.com/tikv/client-go/v2/tikvrpc"
	tikvutil "github.com/tikv/client-go/v2/util"
	rmclient "github.com/tikv/pd/client/resource_group/controller"
)

type explainRUStatus string

const (
	explainRUStatusSuccess                  explainRUStatus                  = "success"
	explainRUStatusUnsupportedNonAnalyze    explainRUStatus                  = "unsupported_non_analyze"
	explainRUStatusUnsupportedNonSelect     explainRUStatus                  = "unsupported_non_select"
	explainRUStatusUnsupportedSideEffecting explainRUStatus                  = "unsupported_side_effecting_select"
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
	readBillingDemoModelVersion                       = "v6"
	readBillingDemoWeightVersion                      = "v6-frontend-compile-work-uncalibrated"
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
	readBillingDemoReasonNonBillable                  = "non_billable"
	readBillingDemoReasonMissingCommitDetail          = "missing_commit_detail"
	readBillingDemoReasonMissingWriteKeys             = "missing_write_keys"
	readBillingDemoReasonMissingWriteBytes            = "missing_write_bytes"
	readBillingDemoReasonZeroMutation                 = "zero_mutation"
	readBillingDemoReasonMissingPrewriteRegion        = "missing_prewrite_region_num"
	readBillingDemoReasonMissingMutationRecorder      = "missing_mutation_recorder"
	readBillingDemoReasonUncalibratedMutation         = "uncalibrated_mutation_weights"
	readBillingDemoReasonDMLAncillaryPartial          = "dml_ancillary_work_partial"
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
	readBillingDemoReasonInvalidShuffleStructure      = "invalid_shuffle_structure"
	readBillingDemoReasonInvalidShuffleWork           = "invalid_shuffle_work"
	readBillingDemoReasonMissingOrderingProjection    = "missing_ordering_projection"
	readBillingDemoReasonMissingExpressionCount       = "missing_expression_count"
	readBillingDemoReasonInvalidTopNBound             = "invalid_topn_bound"
	readBillingDemoReasonMissingHashStateRows         = "missing_hash_state_rows"
	readBillingDemoReasonInvalidHashStateRows         = "invalid_hash_state_rows"
	readBillingDemoReasonMissingReaderTransport       = "missing_reader_transport_details"
	readBillingDemoReasonAmbiguousReaderTransport     = "ambiguous_reader_transport_producers"
	readBillingDemoReasonMissingPointScanStats        = "missing_point_scan_stats"
	readBillingDemoReasonIncompletePointScanDetail    = "incomplete_point_scan_detail"
	readBillingDemoReasonInvalidPointScanDetail       = "invalid_point_scan_detail"
	readBillingDemoReasonMissingTiKVWriteCoverage     = "missing_tikv_write_coverage"
	readBillingDemoReasonPipelinedWriteUnmodeled      = "pipelined_tikv_write_work_unmodeled"
	readBillingDemoReasonPipelinedCommitUnmodeled     = "pipelined_tikv_commit_work_unmodeled"
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
	readBillingDemoOpClassShuffle                     = "shuffle"
	readBillingDemoOpClassReaderReceive               = "reader_receive"
	readBillingDemoOpClassLookupReader                = "lookup_reader"
	readBillingDemoOpClassOverlayReader               = "overlay_reader"
	readBillingDemoOpClassMetadataReader              = "metadata_reader"
	readBillingDemoOpClassPointLookup                 = "kv_point_lookup"
	readBillingDemoOpClassRangeScan                   = "kv_range_scan"
	readBillingDemoOpClassKVMutation                  = "kv_mutation"
	readBillingDemoOpClassKVWrite                     = "kv_write"
	readBillingDemoOpClassSQLFrontend                 = "sql_frontend"
	readBillingDemoOpClassReaderTransport             = "reader_transport"
	readBillingDemoOpClassWrapper                     = "wrapper"
	readBillingDemoOpClassSynthetic                   = "synthetic_source"
	readBillingDemoOperatorStatement                  = "statement"
	readBillingDemoOperatorMemDBMutation              = "memdb_mutation"
	readBillingDemoOperatorTxnPrewrite                = "txn_prewrite"
	readBillingDemoOperatorTxnWrite                   = "txn_write"
	readBillingDemoOperatorParserOptimizer            = "parser_optimizer"
	readBillingDemoOperatorHashShuffle                = "hash_shuffle"
	readBillingDemoOperatorRangeShuffle               = "range_shuffle"
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
	readBillingDemoUnitWriteBytes                     = "write_bytes"
	readBillingDemoUnitFrontendCompileBytes           = "frontend_compile_bytes"
	readBillingDemoUnitPrewriteRegionNum              = "prewrite_region_num"
	readBillingDemoUnitTiKVWriteRPCCount              = "tikv_write_rpc_count"
	readBillingDemoUnitCPUWork                        = "cpu_work"
	readBillingDemoUnitExpressionCount                = "expression_count"
	readBillingDemoUnitScanBytes                      = "scan_bytes"
	readBillingDemoUnitNetBytes                       = "net_bytes"
	readBillingDemoUnitHashStateRows                  = "hash_state_rows"
	readBillingDemoUnitJoinOutputRows                 = "join_output_rows"
	readBillingDemoUnitTotalKeys                      = "total_keys"
	readBillingDemoUnitProcessedKeys                  = "processed_keys"
	readBillingDemoUnitProcessedKeysSize              = "processed_keys_size"
	readBillingDemoUnitDetailRecords                  = "detail_records"
	readBillingDemoUnitCompletedResponses             = "completed_responses"
	readBillingDemoInputSourceRuntimeChunkBytes       = "runtime_chunk_bytes"
	readBillingDemoInputSourceScanDetail              = "scan_detail"
	readBillingDemoInputSourceRuntimeChildActRows     = "runtime_child_act_rows"
	readBillingDemoInputSourceRuntimeOperatorActRows  = "runtime_operator_act_rows"
	readBillingDemoInputSourceRuntimeReaderOutput     = "runtime_reader_output_chunks"
	readBillingDemoInputSourceRuntimeOrderingWork     = "runtime_ordering_work"
	readBillingDemoInputSourceStmtMemDBMutation       = "stmt_memdb_mutation_calls"
	readBillingDemoInputSourceCommitDetail            = "commit_detail"
	readBillingDemoInputSourceStatementOriginalSQL    = "statement_original_sql"
	readBillingDemoInputSourceRUV2Metrics             = "ruv2_metrics"
	readBillingDemoInputSourceDistSQLRuntimeStats     = "distsql_runtime_stats"
	readBillingDemoInputSourceSnapshotRuntimeStats    = "snapshot_runtime_stats"
	readBillingDemoInputSourcePhysicalPlan            = "physical_plan"
	readBillingDemoInputSourceHashJoinRuntime         = "hash_join_runtime_stats"
	readBillingDemoInputSourceShuffleDataSourceRows   = "shuffle_data_source_act_rows"
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

type readBillingDemoPlanOccurrence struct {
	treeOrdinal int
	idx         int
	node        *FlatOperator
}

type readBillingDemoSkipCandidate struct {
	treeOrdinal int
	join        readBillingDemoPlanOccurrence
	outer       readBillingDemoPlanOccurrence
	innerRoot   readBillingDemoPlanOccurrence
	innerStart  int
	innerEnd    int
	innerNodes  []*FlatOperator
}

type readBillingDemoIndexLookupSkipCandidate struct {
	treeOrdinal int
	lookup      readBillingDemoPlanOccurrence
	indexRoot   readBillingDemoPlanOccurrence
	tableRoot   readBillingDemoPlanOccurrence
	tableStart  int
	tableEnd    int
	tableNodes  []*FlatOperator
}

type readBillingDemoIndexMergeSkipCandidate struct {
	treeOrdinal  int
	merge        readBillingDemoPlanOccurrence
	partialRoots []readBillingDemoPlanOccurrence
	tableRoot    readBillingDemoPlanOccurrence
	tableStart   int
	tableEnd     int
	tableNodes   []*FlatOperator
}

type readBillingDemoZeroLimitSkipCandidate struct {
	treeOrdinal    int
	root           readBillingDemoPlanOccurrence
	dominatedEnd   int
	dominatedNodes []*FlatOperator
	explicitLimit  bool
	embeddedReader bool
}

type readBillingDemoExecutionMask struct {
	skippedNodes                 map[*FlatOperator]struct{}
	skippedInnerByJoin           map[*FlatOperator]*FlatOperator
	explicitZeroLimits           map[*FlatOperator]struct{}
	suppressedTransportProducers map[*FlatOperator]struct{}
	planIDOccurrences            map[int]int
}

func (m *readBillingDemoExecutionMask) isSkipped(node *FlatOperator) bool {
	if m == nil || node == nil {
		return false
	}
	_, ok := m.skippedNodes[node]
	return ok
}

func (m *readBillingDemoExecutionMask) isSkippedInner(join, child *FlatOperator) bool {
	if m == nil || join == nil || child == nil {
		return false
	}
	return m.skippedInnerByJoin[join] == child
}

func (m *readBillingDemoExecutionMask) hasSkippedInner(join *FlatOperator) bool {
	if m == nil || join == nil {
		return false
	}
	_, ok := m.skippedInnerByJoin[join]
	return ok
}

func (m *readBillingDemoExecutionMask) isExplicitZeroLimit(node *FlatOperator) bool {
	if m == nil || node == nil {
		return false
	}
	_, ok := m.explicitZeroLimits[node]
	return ok
}

func (m *readBillingDemoExecutionMask) suppressesTransportProducer(node *FlatOperator) bool {
	if m == nil || node == nil {
		return false
	}
	_, ok := m.suppressedTransportProducers[node]
	return ok
}

type readBillingDemoCopEstimator struct {
	tree             FlatPlanTree
	runtimeStats     *execdetails.RuntimeStatsColl
	executionMask    *readBillingDemoExecutionMask
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
	ModelVersion            string
	Version                 string
	CPUWeight               float64
	ScanWeight              float64
	NetWeight               float64
	HashTableWeight         float64
	JoinWeight              float64
	WriteKeyWeight          float64
	WriteBytesWeight        float64
	FrontendCompileWeight   float64
	MutationBytesPerCPUUnit float64
	Calibrated              bool
}

type readBillingDemoWeightProvider interface {
	valid() bool
	unitWeight(string) (float64, bool)
}

// Production intentionally starts without guessed coefficients. Formula tests
// inject a calibrated private value directly.
var readBillingDemoV6Weights = readBillingDemoWeights{
	ModelVersion: readBillingDemoModelVersion,
	Version:      readBillingDemoWeightVersion,
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
	result := buildReadBillingDemoExecutionResult(sctx, plan, stmt, execErr, ruv2Metrics)
	appendReadBillingDemoFrontend(&result, sctx, stmt)
	return result
}

func buildReadBillingDemoExecutionResult(sctx base.PlanContext, plan base.Plan, stmt ast.StmtNode, execErr error, ruv2Metrics *execdetails.RUV2Metrics) readBillingDemoResult {
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
	executionMask := buildReadBillingDemoExecutionMask(flat, runtimeStats)
	if status, op := appendReadBillingDemoTree(&result, planCtx, runtimeStats, flat.Main, executionMask); status != readBillingDemoStatusSuccess {
		return readBillingDemoFailedOperator(status, op)
	}
	for _, tree := range flat.CTEs {
		if status, op := appendReadBillingDemoTree(&result, planCtx, runtimeStats, tree, executionMask); status != readBillingDemoStatusSuccess {
			return readBillingDemoFailedOperator(status, op)
		}
	}
	for _, tree := range flat.ScalarSubQueries {
		if status, op := appendReadBillingDemoTree(&result, planCtx, runtimeStats, tree, executionMask); status != readBillingDemoStatusSuccess {
			return readBillingDemoFailedOperator(status, op)
		}
	}
	if ruv2Metrics == nil && sctx.GetSessionVars() != nil {
		ruv2Metrics = sctx.GetSessionVars().RUV2Metrics
	}
	if op, present := readBillingDemoReaderTransport(flat, runtimeStats, ruv2Metrics, false, executionMask); present {
		if op.status != readBillingDemoStatusOperatorOK {
			return readBillingDemoFailedOperator(readBillingDemoStatusUnknownInput, op)
		}
		result.operators = append(result.operators, op)
	}
	if op, present := readBillingDemoPointLookupTransport(flat, runtimeStats, false, executionMask); present {
		if op.status != readBillingDemoStatusOperatorOK {
			return readBillingDemoFailedOperator(readBillingDemoStatusUnknownInput, op)
		}
		result.operators = append(result.operators, op)
	}
	return result
}

func appendReadBillingDemoFrontend(result *readBillingDemoResult, sctx base.PlanContext, stmt ast.StmtNode) {
	if result == nil || result.status != readBillingDemoStatusSuccess || sctx == nil ||
		sctx.GetSessionVars() == nil || sctx.GetSessionVars().FoundInPlanCache {
		return
	}
	originalSQL := ""
	if stmt != nil {
		originalSQL = stmt.OriginalText()
	}
	if originalSQL == "" && sctx.GetSessionVars().StmtCtx != nil {
		originalSQL = sctx.GetSessionVars().StmtCtx.OriginalSQL
	}
	if originalSQL == "" && stmt != nil {
		originalSQL = stmt.Text()
	}
	if originalSQL == "" {
		return
	}
	result.operators = append(result.operators, readBillingDemoOperatorResult{
		id:           "frontend@statement",
		site:         readBillingDemoSiteTiDB,
		opClass:      readBillingDemoOpClassSQLFrontend,
		operatorKind: readBillingDemoOperatorParserOptimizer,
		status:       readBillingDemoStatusOperatorOK,
		units: []readBillingDemoUnit{{
			unit:        readBillingDemoUnitFrontendCompileBytes,
			source:      readBillingDemoInputSourceStatementOriginalSQL,
			side:        readBillingDemoInputSideAll,
			value:       float64(len(originalSQL)),
			widthSource: explainRUWidthSourceNotApplicable,
		}},
	})
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

	// The final COMMIT statement solely owns a non-pipelined explicit
	// transaction's remote write work. Keep dml_kind empty because the commit
	// payload can contain mutations accumulated by multiple DML statements.
	result.operators = append(result.operators, buildTiKVWriteBillingDemoOperator("", ruv2Metrics, pipelined, true))
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

	var pipelined, explicitTxn bool
	if sctx != nil && sctx.GetSessionVars() != nil {
		vars := sctx.GetSessionVars()
		if ruv2Metrics == nil {
			ruv2Metrics = vars.RUV2Metrics
		}
		explicitTxn = vars.InTxn()
		if stmtCtx := vars.StmtCtx; stmtCtx != nil && stmtCtx.PreviewKVMutationRecorder != nil {
			pipelined = stmtCtx.PreviewKVMutationRecorder.Snapshot().Pipelined
		}
	}

	appendReadBillingDemoDMLPlan(&result, sctx, plan, ruv2Metrics)
	appendReadBillingDemoMutation(&result, sctx, dmlKind)
	// Non-pipelined DML in an explicit transaction has not committed remote
	// writes. In particular, pessimistic lock RPCs are not final write payload
	// and remain outside this model. Pipelined DML still needs an explicit
	// partial marker because it may already have flushed remotely.
	if !explicitTxn || pipelined {
		result.operators = append(result.operators, buildTiKVWriteBillingDemoOperator(dmlKind, ruv2Metrics, pipelined, false))
	}
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
	executionMask := buildReadBillingDemoExecutionMask(flat, runtimeStats)
	appendTree := func(tree FlatPlanTree) {
		appendReadBillingDemoDMLTree(result, runtimeStats, tree, executionMask)
	}
	appendTree(flat.Main)
	for _, tree := range flat.CTEs {
		appendTree(tree)
	}
	for _, tree := range flat.ScalarSubQueries {
		appendTree(tree)
	}
	if op, present := readBillingDemoReaderTransport(flat, runtimeStats, ruv2Metrics, true, executionMask); present {
		result.operators = append(result.operators, op)
	}
	if op, present := readBillingDemoPointLookupTransport(flat, runtimeStats, true, executionMask); present {
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
		uncalibrated: !readBillingDemoWeightsValid(readBillingDemoV6Weights),
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
	if readBillingDemoWeightsValid(readBillingDemoV6Weights) {
		normalization := readBillingDemoV6Weights.MutationBytesPerCPUUnit
		work := float64(snapshot.EncodedMutationCount) + float64(snapshot.EncodedMutationBytes)/normalization
		if work >= 0 && !math.IsNaN(work) && !math.IsInf(work, 0) {
			operator.units = append(operator.units, readBillingDemoUnit{unit: readBillingDemoUnitCPUWork, source: readBillingDemoInputSourceStmtMemDBMutation, side: readBillingDemoInputSideAll, value: work, widthSource: explainRUWidthSourceNotApplicable})
		}
	}
	result.operators = append(result.operators, operator)
	if !readBillingDemoWeightsValid(readBillingDemoV6Weights) {
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

func buildTiKVWriteBillingDemoOperator(dmlKind string, ruv2Metrics *execdetails.RUV2Metrics, pipelined, commit bool) readBillingDemoOperatorResult {
	if ruv2Metrics == nil || ruv2Metrics.Bypass() {
		return buildTiKVWriteBillingDemoOperatorFromSnapshot(dmlKind, 0, 0, pipelined, commit, false)
	}
	return buildTiKVWriteBillingDemoOperatorFromSnapshot(
		dmlKind, ruv2Metrics.WriteKeys(), ruv2Metrics.WriteSize(), pipelined, commit, true,
	)
}

func buildTiKVWriteBillingDemoOperatorFromSnapshot(
	dmlKind string,
	writeKeys, writeBytes int64,
	pipelined, commit, coverage bool,
) readBillingDemoOperatorResult {
	operator := readBillingDemoOperatorResult{
		id:            "txn_write@statement",
		site:          readBillingDemoSiteTiKV,
		opClass:       readBillingDemoOpClassKVWrite,
		operatorKind:  readBillingDemoOperatorTxnWrite,
		dmlKind:       dmlKind,
		emitStatusRow: true,
	}
	if !commit {
		operator.scope = readBillingDemoScopeTxnPrewritePayload
	}
	if pipelined {
		operator.status = readBillingDemoStatusPartial
		if commit {
			operator.reason = readBillingDemoReasonPipelinedCommitUnmodeled
		} else {
			operator.reason = readBillingDemoReasonPipelinedWriteUnmodeled
		}
		return operator
	}
	if !coverage {
		operator.status = readBillingDemoStatusPartial
		operator.reason = readBillingDemoReasonMissingTiKVWriteCoverage
		return operator
	}
	if writeKeys < 0 || writeBytes < 0 {
		operator.status = readBillingDemoStatusPartial
		operator.reason = readBillingDemoReasonMissingTiKVWriteCoverage
		return operator
	}
	if writeKeys == 0 && writeBytes > 0 {
		operator.status = readBillingDemoStatusPartial
		operator.reason = readBillingDemoReasonMissingWriteKeys
		return operator
	}
	if writeKeys > 0 && writeBytes == 0 {
		operator.status = readBillingDemoStatusPartial
		operator.reason = readBillingDemoReasonMissingWriteBytes
		return operator
	}
	operator.status = readBillingDemoStatusOperatorOK
	operator.reason = readBillingDemoReasonNone
	// CommitDetails are frozen into the statement-local RUV2Metrics before this
	// constructor runs. Preserve the original data provenance in outward rows.
	operator.units = []readBillingDemoUnit{
		{unit: readBillingDemoUnitWriteKeys, source: readBillingDemoInputSourceCommitDetail, side: readBillingDemoInputSideAll, value: float64(writeKeys), widthSource: explainRUWidthSourceNotApplicable},
		{unit: readBillingDemoUnitWriteBytes, source: readBillingDemoInputSourceCommitDetail, side: readBillingDemoInputSideAll, value: float64(writeBytes), widthSource: explainRUWidthSourceNotApplicable},
	}
	return operator
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

func newReadBillingDemoExecutionMask() *readBillingDemoExecutionMask {
	return &readBillingDemoExecutionMask{
		skippedNodes:                 make(map[*FlatOperator]struct{}),
		skippedInnerByJoin:           make(map[*FlatOperator]*FlatOperator),
		explicitZeroLimits:           make(map[*FlatOperator]struct{}),
		suppressedTransportProducers: make(map[*FlatOperator]struct{}),
		planIDOccurrences:            make(map[int]int),
	}
}

func readBillingDemoOptionalExecutionMask(
	executionMasks []*readBillingDemoExecutionMask,
) (*readBillingDemoExecutionMask, bool) {
	switch len(executionMasks) {
	case 0:
		return nil, true
	case 1:
		return executionMasks[0], true
	default:
		return nil, false
	}
}

func readBillingDemoLookupJoinChildIDs(node *FlatOperator) (innerPlanID, outerPlanID int, ok bool) {
	if node == nil || node.Origin == nil || !node.IsRoot {
		return 0, 0, false
	}
	var plan base.PhysicalPlan
	var innerIdx int
	switch join := node.Origin.(type) {
	case *physicalop.PhysicalIndexJoin:
		plan, innerIdx = join, join.InnerChildIdx
	case *physicalop.PhysicalIndexHashJoin:
		plan, innerIdx = join, join.InnerChildIdx
	case *physicalop.PhysicalIndexMergeJoin:
		plan, innerIdx = join, join.InnerChildIdx
	default:
		return 0, 0, false
	}
	children := plan.Children()
	if len(children) != 2 || innerIdx < 0 || innerIdx >= len(children) ||
		children[innerIdx] == nil || children[1-innerIdx] == nil {
		return 0, 0, false
	}
	innerPlanID = children[innerIdx].ID()
	outerPlanID = children[1-innerIdx].ID()
	return innerPlanID, outerPlanID, innerPlanID > 0 && outerPlanID > 0 && innerPlanID != outerPlanID
}

func readBillingDemoFlatSubtreeValid(tree FlatPlanTree, start, end int) bool {
	if start < 0 || end < start || end >= len(tree) {
		return false
	}
	for idx := start; idx <= end; idx++ {
		node := tree[idx]
		if node == nil || node.Origin == nil || node.Origin.ID() <= 0 ||
			node.ChildrenEndIdx < idx || node.ChildrenEndIdx > end {
			return false
		}
		if len(node.ChildrenIdx) == 0 {
			if node.ChildrenEndIdx != idx {
				return false
			}
			continue
		}
		if node.ChildrenIdx[0] != idx+1 {
			return false
		}
		previousStart := idx
		for childPos, childStart := range node.ChildrenIdx {
			if childStart <= previousStart || childStart <= idx || childStart > node.ChildrenEndIdx ||
				childStart < start || childStart > end || tree[childStart] == nil {
				return false
			}
			childEnd := node.ChildrenEndIdx
			if childPos+1 < len(node.ChildrenIdx) {
				childEnd = node.ChildrenIdx[childPos+1] - 1
			}
			if childEnd < childStart || childEnd > end || tree[childStart].ChildrenEndIdx != childEnd {
				return false
			}
			previousStart = childStart
		}
	}
	return true
}

type readBillingDemoZeroLimitExpectedChild struct {
	planID       int
	isRoot       bool
	label        OperatorLabel
	checkLabel   bool
	isProbe      bool
	checkIsProbe bool
}

func readBillingDemoZeroLimitSkipCandidateAt(
	tree FlatPlanTree,
	treeOrdinal, rootIdx int,
) (readBillingDemoZeroLimitSkipCandidate, bool) {
	candidate := readBillingDemoZeroLimitSkipCandidate{}
	if rootIdx < 0 || rootIdx >= len(tree) || tree[rootIdx] == nil || tree[rootIdx].Origin == nil {
		return candidate, false
	}
	root := tree[rootIdx]
	if !root.IsRoot || root.Origin.ID() <= 0 || root.ChildrenEndIdx <= rootIdx ||
		root.ChildrenEndIdx >= len(tree) || !readBillingDemoFlatSubtreeValid(tree, rootIdx, root.ChildrenEndIdx) {
		return candidate, false
	}

	expectedChildren := make([]readBillingDemoZeroLimitExpectedChild, 0, len(root.ChildrenIdx))
	switch plan := root.Origin.(type) {
	case *physicalop.PhysicalLimit:
		children := plan.Children()
		if plan.Count != 0 || len(children) != 1 || children[0] == nil {
			return candidate, false
		}
		expectedChildren = append(expectedChildren, readBillingDemoZeroLimitExpectedChild{planID: children[0].ID(), isRoot: true})
		candidate.explicitLimit = true
	case *physicalop.PhysicalIndexLookUpReader:
		if plan.PushedLimit == nil || plan.PushedLimit.Count != 0 || plan.IndexPlan == nil || plan.TablePlan == nil {
			return candidate, false
		}
		expectedChildren = append(expectedChildren,
			readBillingDemoZeroLimitExpectedChild{planID: plan.IndexPlan.ID(), label: BuildSide, checkLabel: true, checkIsProbe: true},
			readBillingDemoZeroLimitExpectedChild{planID: plan.TablePlan.ID(), label: ProbeSide, checkLabel: true, isProbe: true, checkIsProbe: true},
		)
		candidate.embeddedReader = true
	case *physicalop.PhysicalIndexMergeReader:
		if plan.PushedLimit == nil || plan.PushedLimit.Count != 0 || len(plan.PartialPlansRaw) == 0 {
			return candidate, false
		}
		for _, partial := range plan.PartialPlansRaw {
			if partial == nil {
				return candidate, false
			}
			expectedChildren = append(expectedChildren,
				readBillingDemoZeroLimitExpectedChild{planID: partial.ID(), label: BuildSide, checkLabel: true, checkIsProbe: true},
			)
		}
		if plan.TablePlan != nil {
			expectedChildren = append(expectedChildren,
				readBillingDemoZeroLimitExpectedChild{planID: plan.TablePlan.ID(), label: ProbeSide, checkLabel: true, isProbe: true, checkIsProbe: true},
			)
		}
		candidate.embeddedReader = true
	default:
		return candidate, false
	}
	if len(root.ChildrenIdx) != len(expectedChildren) {
		return candidate, false
	}
	for childPos, childIdx := range root.ChildrenIdx {
		if childIdx <= rootIdx || childIdx > root.ChildrenEndIdx || childIdx >= len(tree) || tree[childIdx] == nil || tree[childIdx].Origin == nil {
			return candidate, false
		}
		child := tree[childIdx]
		expected := expectedChildren[childPos]
		if child.Origin.ID() != expected.planID || child.IsRoot != expected.isRoot ||
			(expected.checkLabel && child.Label != expected.label) ||
			(expected.checkIsProbe && child.IsINLProbeChild != expected.isProbe) {
			return candidate, false
		}
	}

	candidate.treeOrdinal = treeOrdinal
	candidate.root = readBillingDemoPlanOccurrence{treeOrdinal: treeOrdinal, idx: rootIdx, node: root}
	candidate.dominatedEnd = root.ChildrenEndIdx
	candidate.dominatedNodes = append(candidate.dominatedNodes, tree[rootIdx+1:root.ChildrenEndIdx+1]...)
	return candidate, len(candidate.dominatedNodes) > 0
}

func readBillingDemoSkipCandidateAt(tree FlatPlanTree, treeOrdinal, joinIdx int) (readBillingDemoSkipCandidate, bool) {
	candidate := readBillingDemoSkipCandidate{}
	if joinIdx < 0 || joinIdx >= len(tree) || tree[joinIdx] == nil || tree[joinIdx].Origin == nil {
		return candidate, false
	}
	joinNode := tree[joinIdx]
	innerPlanID, outerPlanID, ok := readBillingDemoLookupJoinChildIDs(joinNode)
	if !ok || joinNode.Origin.ID() <= 0 || len(joinNode.ChildrenIdx) != 2 ||
		joinNode.ChildrenEndIdx < joinIdx || joinNode.ChildrenEndIdx >= len(tree) {
		return candidate, false
	}

	var innerStart, outerStart int
	innerStart, outerStart = -1, -1
	previousStart := joinIdx
	for childPos, childStart := range joinNode.ChildrenIdx {
		if childStart <= previousStart || childStart <= joinIdx || childStart > joinNode.ChildrenEndIdx ||
			childStart >= len(tree) || tree[childStart] == nil || tree[childStart].Origin == nil ||
			!tree[childStart].IsRoot {
			return candidate, false
		}
		childEnd := joinNode.ChildrenEndIdx
		if childPos+1 < len(joinNode.ChildrenIdx) {
			childEnd = joinNode.ChildrenIdx[childPos+1] - 1
		}
		if childEnd < childStart || tree[childStart].ChildrenEndIdx != childEnd {
			return candidate, false
		}
		switch tree[childStart].Origin.ID() {
		case innerPlanID:
			if innerStart >= 0 {
				return candidate, false
			}
			innerStart = childStart
		case outerPlanID:
			if outerStart >= 0 {
				return candidate, false
			}
			outerStart = childStart
		default:
			return candidate, false
		}
		previousStart = childStart
	}
	if innerStart < 0 || outerStart < 0 {
		return candidate, false
	}
	innerEnd := tree[innerStart].ChildrenEndIdx
	if innerEnd < innerStart || innerEnd > joinNode.ChildrenEndIdx {
		return candidate, false
	}
	if !readBillingDemoFlatSubtreeValid(tree, innerStart, innerEnd) {
		return candidate, false
	}
	innerNodes := make([]*FlatOperator, 0, innerEnd-innerStart+1)
	for idx := innerStart; idx <= innerEnd; idx++ {
		innerNodes = append(innerNodes, tree[idx])
	}
	return readBillingDemoSkipCandidate{
		treeOrdinal: treeOrdinal,
		join:        readBillingDemoPlanOccurrence{treeOrdinal: treeOrdinal, idx: joinIdx, node: joinNode},
		outer:       readBillingDemoPlanOccurrence{treeOrdinal: treeOrdinal, idx: outerStart, node: tree[outerStart]},
		innerRoot:   readBillingDemoPlanOccurrence{treeOrdinal: treeOrdinal, idx: innerStart, node: tree[innerStart]},
		innerStart:  innerStart,
		innerEnd:    innerEnd,
		innerNodes:  innerNodes,
	}, true
}

func readBillingDemoIndexLookupSkipCandidateAt(
	tree FlatPlanTree,
	treeOrdinal, lookupIdx int,
) (readBillingDemoIndexLookupSkipCandidate, bool) {
	candidate := readBillingDemoIndexLookupSkipCandidate{}
	if lookupIdx < 0 || lookupIdx >= len(tree) || tree[lookupIdx] == nil || tree[lookupIdx].Origin == nil {
		return candidate, false
	}
	lookupNode := tree[lookupIdx]
	lookup, ok := lookupNode.Origin.(*physicalop.PhysicalIndexLookUpReader)
	if !ok || lookup.IndexLookUpPushDown || !lookupNode.IsRoot || lookupNode.Origin.ID() <= 0 || lookup.IndexPlan == nil || lookup.TablePlan == nil ||
		lookup.IndexPlan.ID() <= 0 || lookup.TablePlan.ID() <= 0 || lookup.IndexPlan.ID() == lookup.TablePlan.ID() ||
		len(lookupNode.ChildrenIdx) != 2 || lookupNode.ChildrenEndIdx < lookupIdx || lookupNode.ChildrenEndIdx >= len(tree) {
		return candidate, false
	}

	indexStart, tableStart := -1, -1
	previousStart := lookupIdx
	for childPos, childStart := range lookupNode.ChildrenIdx {
		if childStart <= previousStart || childStart <= lookupIdx || childStart > lookupNode.ChildrenEndIdx ||
			childStart >= len(tree) || tree[childStart] == nil || tree[childStart].Origin == nil || tree[childStart].IsRoot {
			return candidate, false
		}
		childEnd := lookupNode.ChildrenEndIdx
		if childPos+1 < len(lookupNode.ChildrenIdx) {
			childEnd = lookupNode.ChildrenIdx[childPos+1] - 1
		}
		if childEnd < childStart || tree[childStart].ChildrenEndIdx != childEnd ||
			!readBillingDemoFlatSubtreeValid(tree, childStart, childEnd) {
			return candidate, false
		}
		switch tree[childStart].Origin.ID() {
		case lookup.IndexPlan.ID():
			if indexStart >= 0 || tree[childStart].Label != BuildSide || tree[childStart].IsINLProbeChild {
				return candidate, false
			}
			indexStart = childStart
		case lookup.TablePlan.ID():
			if tableStart >= 0 || tree[childStart].Label != ProbeSide || !tree[childStart].IsINLProbeChild {
				return candidate, false
			}
			tableStart = childStart
		default:
			return candidate, false
		}
		previousStart = childStart
	}
	if indexStart < 0 || tableStart < 0 {
		return candidate, false
	}
	tableEnd := tree[tableStart].ChildrenEndIdx
	tableNodes := make([]*FlatOperator, 0, tableEnd-tableStart+1)
	for idx := tableStart; idx <= tableEnd; idx++ {
		tableNodes = append(tableNodes, tree[idx])
	}
	return readBillingDemoIndexLookupSkipCandidate{
		treeOrdinal: treeOrdinal,
		lookup:      readBillingDemoPlanOccurrence{treeOrdinal: treeOrdinal, idx: lookupIdx, node: lookupNode},
		indexRoot:   readBillingDemoPlanOccurrence{treeOrdinal: treeOrdinal, idx: indexStart, node: tree[indexStart]},
		tableRoot:   readBillingDemoPlanOccurrence{treeOrdinal: treeOrdinal, idx: tableStart, node: tree[tableStart]},
		tableStart:  tableStart,
		tableEnd:    tableEnd,
		tableNodes:  tableNodes,
	}, true
}

func readBillingDemoIndexMergeSkipCandidateAt(
	tree FlatPlanTree,
	treeOrdinal, mergeIdx int,
) (readBillingDemoIndexMergeSkipCandidate, bool) {
	candidate := readBillingDemoIndexMergeSkipCandidate{}
	if mergeIdx < 0 || mergeIdx >= len(tree) || tree[mergeIdx] == nil || tree[mergeIdx].Origin == nil {
		return candidate, false
	}
	mergeNode := tree[mergeIdx]
	merge, ok := mergeNode.Origin.(*physicalop.PhysicalIndexMergeReader)
	if !ok || !mergeNode.IsRoot || mergeNode.Origin.ID() <= 0 || len(merge.PartialPlansRaw) == 0 || merge.TablePlan == nil ||
		merge.TablePlan.ID() <= 0 || len(mergeNode.ChildrenIdx) != len(merge.PartialPlansRaw)+1 ||
		mergeNode.ChildrenEndIdx < mergeIdx || mergeNode.ChildrenEndIdx >= len(tree) ||
		!readBillingDemoFlatSubtreeValid(tree, mergeIdx, mergeNode.ChildrenEndIdx) {
		return candidate, false
	}

	seenPlanIDs := make(map[int]struct{}, len(merge.PartialPlansRaw)+1)
	partialRoots := make([]readBillingDemoPlanOccurrence, 0, len(merge.PartialPlansRaw))
	previousStart := mergeIdx
	for childPos, childStart := range mergeNode.ChildrenIdx {
		if childStart <= previousStart || childStart <= mergeIdx || childStart > mergeNode.ChildrenEndIdx ||
			childStart >= len(tree) || tree[childStart] == nil || tree[childStart].Origin == nil || tree[childStart].IsRoot {
			return candidate, false
		}
		childEnd := mergeNode.ChildrenEndIdx
		if childPos+1 < len(mergeNode.ChildrenIdx) {
			childEnd = mergeNode.ChildrenIdx[childPos+1] - 1
		}
		if childEnd < childStart || tree[childStart].ChildrenEndIdx != childEnd ||
			!readBillingDemoFlatSubtreeValid(tree, childStart, childEnd) {
			return candidate, false
		}

		expectedPlanID := merge.TablePlan.ID()
		expectedLabel, expectedProbe := ProbeSide, true
		if childPos < len(merge.PartialPlansRaw) {
			partial := merge.PartialPlansRaw[childPos]
			if partial == nil || partial.ID() <= 0 {
				return candidate, false
			}
			expectedPlanID = partial.ID()
			expectedLabel, expectedProbe = BuildSide, false
		}
		if tree[childStart].Origin.ID() != expectedPlanID || tree[childStart].Label != expectedLabel ||
			tree[childStart].IsINLProbeChild != expectedProbe {
			return candidate, false
		}
		if _, exists := seenPlanIDs[expectedPlanID]; exists {
			return candidate, false
		}
		seenPlanIDs[expectedPlanID] = struct{}{}
		if childPos < len(merge.PartialPlansRaw) {
			partialRoots = append(partialRoots, readBillingDemoPlanOccurrence{treeOrdinal: treeOrdinal, idx: childStart, node: tree[childStart]})
		} else {
			candidate.tableStart = childStart
			candidate.tableEnd = childEnd
			candidate.tableRoot = readBillingDemoPlanOccurrence{treeOrdinal: treeOrdinal, idx: childStart, node: tree[childStart]}
		}
		previousStart = childStart
	}
	if len(partialRoots) != len(merge.PartialPlansRaw) || candidate.tableRoot.node == nil {
		return readBillingDemoIndexMergeSkipCandidate{}, false
	}
	tableNodes := make([]*FlatOperator, 0, candidate.tableEnd-candidate.tableStart+1)
	for idx := candidate.tableStart; idx <= candidate.tableEnd; idx++ {
		tableNodes = append(tableNodes, tree[idx])
	}
	candidate.treeOrdinal = treeOrdinal
	candidate.merge = readBillingDemoPlanOccurrence{treeOrdinal: treeOrdinal, idx: mergeIdx, node: mergeNode}
	candidate.partialRoots = partialRoots
	candidate.tableNodes = tableNodes
	return candidate, true
}

func readBillingDemoCandidateInvolvedPlanIDs(candidate readBillingDemoSkipCandidate) map[int]struct{} {
	planIDs := make(map[int]struct{}, 2+len(candidate.innerNodes))
	for _, occurrence := range []readBillingDemoPlanOccurrence{candidate.join, candidate.outer} {
		if occurrence.node != nil && occurrence.node.Origin != nil {
			planIDs[occurrence.node.Origin.ID()] = struct{}{}
		}
	}
	for _, node := range candidate.innerNodes {
		if node != nil && node.Origin != nil {
			planIDs[node.Origin.ID()] = struct{}{}
		}
	}
	return planIDs
}

func readBillingDemoOccurrenceInsideCandidateInner(occurrence readBillingDemoPlanOccurrence, candidate readBillingDemoSkipCandidate) bool {
	return occurrence.treeOrdinal == candidate.treeOrdinal &&
		occurrence.idx >= candidate.innerStart &&
		occurrence.idx <= candidate.innerEnd
}

func readBillingDemoSkipCandidatesConflict(left, right readBillingDemoSkipCandidate) bool {
	leftPlanIDs := readBillingDemoCandidateInvolvedPlanIDs(left)
	for planID := range readBillingDemoCandidateInvolvedPlanIDs(right) {
		if _, shared := leftPlanIDs[planID]; shared {
			return true
		}
	}
	if left.treeOrdinal != right.treeOrdinal {
		return false
	}
	overlap := left.innerStart <= right.innerEnd && right.innerStart <= left.innerEnd
	leftContainsRight := left.innerStart <= right.innerStart && left.innerEnd >= right.innerEnd
	rightContainsLeft := right.innerStart <= left.innerStart && right.innerEnd >= left.innerEnd
	if overlap && !leftContainsRight && !rightContainsLeft {
		return true
	}
	return readBillingDemoOccurrenceInsideCandidateInner(left.join, right) ||
		readBillingDemoOccurrenceInsideCandidateInner(left.outer, right) ||
		readBillingDemoOccurrenceInsideCandidateInner(right.join, left) ||
		readBillingDemoOccurrenceInsideCandidateInner(right.outer, left)
}

func readBillingDemoRemoveConflictingSkipCandidates(candidates []readBillingDemoSkipCandidate) []readBillingDemoSkipCandidate {
	conflicting := make([]bool, len(candidates))
	for left := range candidates {
		for right := left + 1; right < len(candidates); right++ {
			if readBillingDemoSkipCandidatesConflict(candidates[left], candidates[right]) {
				conflicting[left], conflicting[right] = true, true
			}
		}
	}
	survivors := make([]readBillingDemoSkipCandidate, 0, len(candidates))
	for idx, candidate := range candidates {
		if !conflicting[idx] {
			survivors = append(survivors, candidate)
		}
	}
	return survivors
}

func readBillingDemoCandidateHasExclusiveOwnership(
	candidate readBillingDemoSkipCandidate,
	ownership map[int][]readBillingDemoPlanOccurrence,
) bool {
	occurrences := make([]readBillingDemoPlanOccurrence, 0, 2+len(candidate.innerNodes))
	occurrences = append(occurrences, candidate.join, candidate.outer)
	for offset, node := range candidate.innerNodes {
		occurrences = append(occurrences, readBillingDemoPlanOccurrence{
			treeOrdinal: candidate.treeOrdinal,
			idx:         candidate.innerStart + offset,
			node:        node,
		})
	}
	return readBillingDemoOccurrencesHaveExclusiveOwnership(occurrences, ownership)
}

func readBillingDemoIndexLookupCandidateHasExclusiveOwnership(
	candidate readBillingDemoIndexLookupSkipCandidate,
	ownership map[int][]readBillingDemoPlanOccurrence,
) bool {
	occurrences := make([]readBillingDemoPlanOccurrence, 0, 2+len(candidate.tableNodes))
	occurrences = append(occurrences, candidate.lookup, candidate.indexRoot)
	for offset, node := range candidate.tableNodes {
		occurrences = append(occurrences, readBillingDemoPlanOccurrence{
			treeOrdinal: candidate.treeOrdinal,
			idx:         candidate.tableStart + offset,
			node:        node,
		})
	}
	return readBillingDemoOccurrencesHaveExclusiveOwnership(occurrences, ownership)
}

func readBillingDemoIndexMergeCandidateHasExclusiveOwnership(
	candidate readBillingDemoIndexMergeSkipCandidate,
	ownership map[int][]readBillingDemoPlanOccurrence,
) bool {
	occurrences := make([]readBillingDemoPlanOccurrence, 0, 1+len(candidate.partialRoots)+len(candidate.tableNodes))
	occurrences = append(occurrences, candidate.merge)
	occurrences = append(occurrences, candidate.partialRoots...)
	for offset, node := range candidate.tableNodes {
		occurrences = append(occurrences, readBillingDemoPlanOccurrence{
			treeOrdinal: candidate.treeOrdinal,
			idx:         candidate.tableStart + offset,
			node:        node,
		})
	}
	return readBillingDemoOccurrencesHaveExclusiveOwnership(occurrences, ownership)
}

func readBillingDemoZeroLimitCandidateHasExclusiveOwnership(
	candidate readBillingDemoZeroLimitSkipCandidate,
	ownership map[int][]readBillingDemoPlanOccurrence,
) bool {
	occurrences := make([]readBillingDemoPlanOccurrence, 0, 1+len(candidate.dominatedNodes))
	occurrences = append(occurrences, candidate.root)
	for offset, node := range candidate.dominatedNodes {
		occurrences = append(occurrences, readBillingDemoPlanOccurrence{
			treeOrdinal: candidate.treeOrdinal,
			idx:         candidate.root.idx + 1 + offset,
			node:        node,
		})
	}
	return readBillingDemoOccurrencesHaveExclusiveOwnership(occurrences, ownership)
}

func readBillingDemoSelectOutermostZeroLimitCandidates(
	candidates []readBillingDemoZeroLimitSkipCandidate,
) []readBillingDemoZeroLimitSkipCandidate {
	conflicting := make([]bool, len(candidates))
	for left := range candidates {
		for right := left + 1; right < len(candidates); right++ {
			if candidates[left].treeOrdinal != candidates[right].treeOrdinal ||
				candidates[left].root.idx > candidates[right].dominatedEnd ||
				candidates[right].root.idx > candidates[left].dominatedEnd {
				continue
			}
			leftContainsRight := candidates[left].root.idx <= candidates[right].root.idx &&
				candidates[left].dominatedEnd >= candidates[right].dominatedEnd
			rightContainsLeft := candidates[right].root.idx <= candidates[left].root.idx &&
				candidates[right].dominatedEnd >= candidates[left].dominatedEnd
			if (!leftContainsRight && !rightContainsLeft) || (leftContainsRight && rightContainsLeft) {
				conflicting[left], conflicting[right] = true, true
			}
		}
	}
	dominated := make([]bool, len(candidates))
	for outer := range candidates {
		if conflicting[outer] {
			continue
		}
		for inner := range candidates {
			if outer == inner || conflicting[inner] || candidates[outer].treeOrdinal != candidates[inner].treeOrdinal {
				continue
			}
			if candidates[outer].root.idx <= candidates[inner].root.idx &&
				candidates[outer].dominatedEnd >= candidates[inner].dominatedEnd {
				dominated[inner] = true
			}
		}
	}
	survivors := make([]readBillingDemoZeroLimitSkipCandidate, 0, len(candidates))
	for idx, candidate := range candidates {
		if !conflicting[idx] && !dominated[idx] {
			survivors = append(survivors, candidate)
		}
	}
	return survivors
}

func readBillingDemoOccurrencesHaveExclusiveOwnership(
	occurrences []readBillingDemoPlanOccurrence,
	ownership map[int][]readBillingDemoPlanOccurrence,
) bool {
	expected := make(map[int]readBillingDemoPlanOccurrence, len(occurrences))
	for _, occurrence := range occurrences {
		if occurrence.node == nil || occurrence.node.Origin == nil || occurrence.node.Origin.ID() <= 0 {
			return false
		}
		planID := occurrence.node.Origin.ID()
		if previous, exists := expected[planID]; exists && previous != occurrence {
			return false
		}
		expected[planID] = occurrence
	}
	for planID, expectedOccurrence := range expected {
		owners := ownership[planID]
		if len(owners) != 1 || owners[0] != expectedOccurrence {
			return false
		}
	}
	return true
}

func readBillingDemoObservedCompletedZero(runtimeStats *execdetails.RuntimeStatsColl, node *FlatOperator) bool {
	if runtimeStats == nil || node == nil || node.Origin == nil || !node.IsRoot {
		return false
	}
	basic := runtimeStats.GetBasicRuntimeStats(node.Origin.ID(), false)
	return basic != nil && basic.GetActRows() == 0 && (basic.HasRuntimeRows() || basic.HasBytes())
}

func readBillingDemoLookupJoinZeroOutputCompatible(runtimeStats *execdetails.RuntimeStatsColl, node *FlatOperator) bool {
	if runtimeStats == nil || node == nil || node.Origin == nil || !node.IsRoot {
		return false
	}
	basic := runtimeStats.GetBasicRuntimeStats(node.Origin.ID(), false)
	if basic == nil || (!basic.HasRuntimeRows() && !basic.HasBytes()) {
		return true
	}
	return basic.GetActRows() == 0
}

func readBillingDemoMaskedRootSemanticZero(runtimeStats *execdetails.RuntimeStatsColl, mask *readBillingDemoExecutionMask, node *FlatOperator) bool {
	if mask == nil || node == nil || node.Origin == nil || !node.IsRoot {
		return false
	}
	return mask.isExplicitZeroLimit(node) ||
		mask.suppressesTransportProducer(node) ||
		mask.hasSkippedInner(node) && readBillingDemoLookupJoinZeroOutputCompatible(runtimeStats, node)
}

func readBillingDemoRootKnownZero(runtimeStats *execdetails.RuntimeStatsColl, mask *readBillingDemoExecutionMask, node *FlatOperator) bool {
	return readBillingDemoObservedCompletedZero(runtimeStats, node) ||
		readBillingDemoMaskedRootSemanticZero(runtimeStats, mask, node)
}

func readBillingDemoZeroLimitRootEvidenceCompatible(runtimeStats *execdetails.RuntimeStatsColl, node *FlatOperator) bool {
	if runtimeStats == nil || node == nil || node.Origin == nil {
		return false
	}
	basic := runtimeStats.GetBasicRuntimeStats(node.Origin.ID(), false)
	if basic != nil && (basic.HasBytes() || basic.HasRuntimeRows()) && basic.GetActRows() != 0 {
		return false
	}
	if !runtimeStats.ExistsRootStats(node.Origin.ID()) {
		return true
	}
	_, groups := runtimeStats.GetRootStats(node.Origin.ID()).MergeStats()
	for _, group := range groups {
		switch group.Tp() {
		case execdetails.TpSelectResultRuntimeStats, execdetails.TpRuntimeStatsWithSnapshot:
			return false
		}
	}
	return true
}

func readBillingDemoInnerHasExecutionEvidence(
	runtimeStats *execdetails.RuntimeStatsColl,
	candidate readBillingDemoSkipCandidate,
) bool {
	return readBillingDemoNodesHaveExecutionEvidence(runtimeStats, candidate.innerNodes)
}

func readBillingDemoInnerHasUnmaskedExecutionEvidence(
	runtimeStats *execdetails.RuntimeStatsColl,
	mask *readBillingDemoExecutionMask,
	candidate readBillingDemoSkipCandidate,
) bool {
	if mask == nil {
		return readBillingDemoInnerHasExecutionEvidence(runtimeStats, candidate)
	}
	for _, node := range candidate.innerNodes {
		if mask.isSkipped(node) {
			continue
		}
		if readBillingDemoNodeHasExecutionEvidence(runtimeStats, node) {
			return true
		}
	}
	return false
}

func readBillingDemoNodeHasExecutionEvidence(runtimeStats *execdetails.RuntimeStatsColl, node *FlatOperator) bool {
	if runtimeStats == nil {
		return false
	}
	if node == nil || node.Origin == nil {
		return true
	}
	planID := node.Origin.ID()
	if basic := runtimeStats.GetBasicRuntimeStats(planID, false); basic != nil && (basic.HasBytes() || basic.HasRuntimeRows()) {
		return true
	}
	if runtimeStats.ExistsRootStats(planID) {
		_, groups := runtimeStats.GetRootStats(planID).MergeStats()
		for _, group := range groups {
			switch group.Tp() {
			case execdetails.TpSelectResultRuntimeStats, execdetails.TpRuntimeStatsWithSnapshot:
				return true
			}
		}
	}
	_, detailRecords, observedTasks, expectedTasks := runtimeStats.GetCopScanDetailAndCoverage(planID)
	return detailRecords != 0 || observedTasks != 0 || expectedTasks != 0
}

func readBillingDemoNodesHaveExecutionEvidence(
	runtimeStats *execdetails.RuntimeStatsColl,
	nodes []*FlatOperator,
) bool {
	for _, node := range nodes {
		if readBillingDemoNodeHasExecutionEvidence(runtimeStats, node) {
			return true
		}
	}
	return false
}

func readBillingDemoObservedCompleteCopRows(runtimeStats *execdetails.RuntimeStatsColl, node *FlatOperator) (int64, bool) {
	if runtimeStats == nil || node == nil || node.Origin == nil || node.IsRoot || node.StoreType != kv.TiKV {
		return 0, false
	}
	evidence := readBillingDemoExactCopRowsEvidence(runtimeStats, node.Origin.ID())
	if evidence.state != readBillingDemoCopRowsObserved || evidence.rows < 0 || evidence.tasks <= 0 {
		return 0, false
	}
	_, _, observedTasks, expectedTasks := runtimeStats.GetCopScanDetailAndCoverage(node.Origin.ID())
	if observedTasks != evidence.tasks || observedTasks != expectedTasks {
		return 0, false
	}
	return evidence.rows, true
}

func readBillingDemoIndexLookupTableLegProvenSkipped(
	runtimeStats *execdetails.RuntimeStatsColl,
	candidate readBillingDemoIndexLookupSkipCandidate,
) bool {
	if runtimeStats == nil || candidate.lookup.node == nil || candidate.lookup.node.Origin == nil {
		return false
	}
	rootStats := runtimeStats.GetBasicRuntimeStats(candidate.lookup.node.Origin.ID(), false)
	if rootStats == nil || (!rootStats.HasRuntimeRows() && !rootStats.HasBytes()) || rootStats.GetActRows() != 0 {
		return false
	}
	indexRows, ok := readBillingDemoObservedCompleteCopRows(runtimeStats, candidate.indexRoot.node)
	return ok && indexRows == 0
}

func readBillingDemoIndexMergeTableLegProvenSkipped(
	runtimeStats *execdetails.RuntimeStatsColl,
	candidate readBillingDemoIndexMergeSkipCandidate,
) bool {
	// The completed zero-row IndexMerge root plus the caller's whole-table-subtree
	// no-evidence check prove that the final table leg was never constructed. The
	// per-partial check below is only a conservative coverage gate for responses
	// that were received; it does not claim that every possible partial range ran.
	if !readBillingDemoObservedCompletedZero(runtimeStats, candidate.merge.node) || len(candidate.partialRoots) == 0 {
		return false
	}
	for _, partial := range candidate.partialRoots {
		if _, ok := readBillingDemoObservedCompleteCopRows(runtimeStats, partial.node); !ok {
			return false
		}
	}
	return true
}

func readBillingDemoExecutionMaskOwnershipValid(
	flat *FlatPhysicalPlan,
	mask *readBillingDemoExecutionMask,
	proofs []readBillingDemoPlanOccurrence,
	ownership map[int][]readBillingDemoPlanOccurrence,
) bool {
	maskedIDs := make(map[int]struct{})
	activeIDs := make(map[int]struct{})
	for _, tree := range readBillingDemoAllTrees(flat) {
		for _, node := range tree {
			if node == nil || node.Origin == nil || node.Origin.ID() <= 0 {
				continue
			}
			if mask.isSkipped(node) {
				maskedIDs[node.Origin.ID()] = struct{}{}
			} else {
				activeIDs[node.Origin.ID()] = struct{}{}
			}
		}
	}
	for planID := range maskedIDs {
		if _, active := activeIDs[planID]; active {
			return false
		}
	}
	for _, proof := range proofs {
		planID := proof.node.Origin.ID()
		if mask.isSkipped(proof.node) {
			return false
		}
		owners := ownership[planID]
		if len(owners) != 1 || owners[0] != proof {
			return false
		}
		if _, masked := maskedIDs[planID]; masked {
			return false
		}
	}
	return true
}

func readBillingDemoMaskUnexecutedCTEParts(
	flat *FlatPhysicalPlan,
	runtimeStats *execdetails.RuntimeStatsColl,
	mask *readBillingDemoExecutionMask,
) {
	for _, tree := range flat.CTEs {
		if len(tree) == 0 || tree[0] == nil || tree[0].Origin == nil {
			continue
		}
		if _, ok := tree[0].Origin.(*physicalop.CTEDefinition); !ok {
			continue
		}
		for _, partRoot := range tree[0].ChildrenIdx {
			if partRoot <= 0 || partRoot >= len(tree) || tree[partRoot] == nil ||
				(tree[partRoot].Label != SeedPart && tree[partRoot].Label != RecursivePart) {
				continue
			}
			partEnd := tree[partRoot].ChildrenEndIdx
			if partEnd < partRoot || partEnd >= len(tree) {
				continue
			}
			partNodes := tree[partRoot : partEnd+1]
			if readBillingDemoNodesHaveExecutionEvidence(runtimeStats, partNodes) {
				continue
			}
			for _, node := range partNodes {
				mask.skippedNodes[node] = struct{}{}
			}
		}
	}
}

func readBillingDemoApplyLookupJoinSkipCandidate(
	mask *readBillingDemoExecutionMask,
	candidate readBillingDemoSkipCandidate,
) bool {
	if mask == nil || mask.isSkipped(candidate.join.node) || mask.isSkipped(candidate.outer.node) {
		return false
	}
	if mask.hasSkippedInner(candidate.join.node) {
		return false
	}
	for _, node := range candidate.innerNodes {
		if mask.isSkipped(node) {
			return false
		}
	}
	mask.skippedInnerByJoin[candidate.join.node] = candidate.innerRoot.node
	for _, node := range candidate.innerNodes {
		mask.skippedNodes[node] = struct{}{}
	}
	return true
}

func buildReadBillingDemoExecutionMask(
	flat *FlatPhysicalPlan,
	runtimeStats *execdetails.RuntimeStatsColl,
) *readBillingDemoExecutionMask {
	emptyMask := newReadBillingDemoExecutionMask()
	if flat == nil || runtimeStats == nil {
		return emptyMask
	}
	trees := readBillingDemoAllTrees(flat)
	ownership := make(map[int][]readBillingDemoPlanOccurrence)
	for treeOrdinal, tree := range trees {
		for idx, node := range tree {
			if node == nil || node.Origin == nil {
				continue
			}
			planID := node.Origin.ID()
			ownership[planID] = append(ownership[planID], readBillingDemoPlanOccurrence{
				treeOrdinal: treeOrdinal,
				idx:         idx,
				node:        node,
			})
		}
	}
	for planID, occurrences := range ownership {
		emptyMask.planIDOccurrences[planID] = len(occurrences)
	}

	joinCandidates := make([]readBillingDemoSkipCandidate, 0)
	indexLookupCandidates := make([]readBillingDemoIndexLookupSkipCandidate, 0)
	indexMergeCandidates := make([]readBillingDemoIndexMergeSkipCandidate, 0)
	zeroLimitCandidates := make([]readBillingDemoZeroLimitSkipCandidate, 0)
	for treeOrdinal, tree := range trees {
		for idx := range tree {
			if candidate, ok := readBillingDemoSkipCandidateAt(tree, treeOrdinal, idx); ok {
				joinCandidates = append(joinCandidates, candidate)
			}
			if candidate, ok := readBillingDemoIndexLookupSkipCandidateAt(tree, treeOrdinal, idx); ok {
				indexLookupCandidates = append(indexLookupCandidates, candidate)
			}
			if candidate, ok := readBillingDemoIndexMergeSkipCandidateAt(tree, treeOrdinal, idx); ok {
				indexMergeCandidates = append(indexMergeCandidates, candidate)
			}
			if candidate, ok := readBillingDemoZeroLimitSkipCandidateAt(tree, treeOrdinal, idx); ok {
				zeroLimitCandidates = append(zeroLimitCandidates, candidate)
			}
		}
	}
	eligibleZeroLimits := make([]readBillingDemoZeroLimitSkipCandidate, 0, len(zeroLimitCandidates))
	for _, candidate := range zeroLimitCandidates {
		if !readBillingDemoZeroLimitCandidateHasExclusiveOwnership(candidate, ownership) ||
			!readBillingDemoZeroLimitRootEvidenceCompatible(runtimeStats, candidate.root.node) ||
			readBillingDemoNodesHaveExecutionEvidence(runtimeStats, candidate.dominatedNodes) {
			continue
		}
		eligibleZeroLimits = append(eligibleZeroLimits, candidate)
	}
	acceptedZeroLimits := readBillingDemoSelectOutermostZeroLimitCandidates(eligibleZeroLimits)

	mask := newReadBillingDemoExecutionMask()
	mask.planIDOccurrences = emptyMask.planIDOccurrences
	readBillingDemoMaskUnexecutedCTEParts(flat, runtimeStats, mask)
	proofs := make([]readBillingDemoPlanOccurrence, 0, len(joinCandidates)*2+len(acceptedZeroLimits)+len(indexLookupCandidates)*2+len(indexMergeCandidates)*3)
	proofNodes := make(map[*FlatOperator]struct{}, cap(proofs))
	addProofs := func(newProofs ...readBillingDemoPlanOccurrence) {
		for _, proof := range newProofs {
			proofs = append(proofs, proof)
			proofNodes[proof.node] = struct{}{}
		}
	}
	nodesContainProof := func(nodes []*FlatOperator) bool {
		for _, node := range nodes {
			if _, ok := proofNodes[node]; ok {
				return true
			}
		}
		return false
	}
	applyLookupJoinSkips := func() bool {
		changed := false
		for _, candidate := range joinCandidates {
			if !readBillingDemoCandidateHasExclusiveOwnership(candidate, ownership) ||
				nodesContainProof(candidate.innerNodes) ||
				!readBillingDemoLookupJoinZeroOutputCompatible(runtimeStats, candidate.join.node) ||
				!readBillingDemoRootKnownZero(runtimeStats, mask, candidate.outer.node) ||
				readBillingDemoInnerHasUnmaskedExecutionEvidence(runtimeStats, mask, candidate) {
				continue
			}
			if readBillingDemoApplyLookupJoinSkipCandidate(mask, candidate) {
				addProofs(candidate.join, candidate.outer)
				changed = true
			}
		}
		return changed
	}
	for applyLookupJoinSkips() {
	}
	for _, candidate := range acceptedZeroLimits {
		if mask.isSkipped(candidate.root.node) {
			continue
		}
		overlapsExistingMask := false
		for _, node := range candidate.dominatedNodes {
			if mask.isSkipped(node) {
				overlapsExistingMask = true
				break
			}
		}
		if overlapsExistingMask || nodesContainProof(candidate.dominatedNodes) {
			continue
		}
		for _, node := range candidate.dominatedNodes {
			mask.skippedNodes[node] = struct{}{}
		}
		if candidate.explicitLimit {
			mask.explicitZeroLimits[candidate.root.node] = struct{}{}
		}
		if candidate.embeddedReader {
			mask.suppressedTransportProducers[candidate.root.node] = struct{}{}
		}
		addProofs(candidate.root)
	}
	for _, candidate := range indexLookupCandidates {
		if !readBillingDemoIndexLookupCandidateHasExclusiveOwnership(candidate, ownership) ||
			!readBillingDemoIndexLookupTableLegProvenSkipped(runtimeStats, candidate) ||
			readBillingDemoNodesHaveExecutionEvidence(runtimeStats, candidate.tableNodes) ||
			mask.isSkipped(candidate.lookup.node) || mask.isSkipped(candidate.indexRoot.node) {
			continue
		}
		overlapsExistingMask := false
		for _, node := range candidate.tableNodes {
			if mask.isSkipped(node) {
				overlapsExistingMask = true
				break
			}
		}
		if overlapsExistingMask || nodesContainProof(candidate.tableNodes) {
			continue
		}
		for _, node := range candidate.tableNodes {
			mask.skippedNodes[node] = struct{}{}
		}
		addProofs(candidate.lookup, candidate.indexRoot)
	}
	for _, candidate := range indexMergeCandidates {
		if !readBillingDemoIndexMergeCandidateHasExclusiveOwnership(candidate, ownership) ||
			!readBillingDemoIndexMergeTableLegProvenSkipped(runtimeStats, candidate) ||
			readBillingDemoNodesHaveExecutionEvidence(runtimeStats, candidate.tableNodes) ||
			mask.isSkipped(candidate.merge.node) {
			continue
		}
		partialMasked := false
		for _, partial := range candidate.partialRoots {
			if mask.isSkipped(partial.node) {
				partialMasked = true
				break
			}
		}
		if partialMasked {
			continue
		}
		overlapsExistingMask := false
		for _, node := range candidate.tableNodes {
			if mask.isSkipped(node) {
				overlapsExistingMask = true
				break
			}
		}
		if overlapsExistingMask || nodesContainProof(candidate.tableNodes) {
			continue
		}
		for _, node := range candidate.tableNodes {
			mask.skippedNodes[node] = struct{}{}
		}
		addProofs(candidate.merge)
		addProofs(candidate.partialRoots...)
	}
	for applyLookupJoinSkips() {
	}
	if !readBillingDemoExecutionMaskOwnershipValid(flat, mask, proofs, ownership) {
		return emptyMask
	}
	return mask
}

func readBillingDemoReaderTransport(
	flat *FlatPhysicalPlan,
	runtimeStats *execdetails.RuntimeStatsColl,
	ruv2Metrics *execdetails.RUV2Metrics,
	dml bool,
	executionMasks ...*readBillingDemoExecutionMask,
) (readBillingDemoOperatorResult, bool) {
	op := readBillingDemoOperatorResult{
		id:            "reader_transport@statement",
		site:          readBillingDemoSiteTiDB,
		opClass:       readBillingDemoOpClassReaderTransport,
		operatorKind:  "mixed_reader",
		emitStatusRow: true,
	}
	executionMask, validMask := readBillingDemoOptionalExecutionMask(executionMasks)
	if !validMask {
		op.status = readBillingDemoStatusUnknownInput
		op.reason = readBillingDemoReasonUnsupportedCopStructure
		return op, true
	}
	kinds := make(map[string]struct{})
	openProducerSet := dml
	hasTasks := false
	allReaderRowsZero := true
	for _, tree := range readBillingDemoAllTrees(flat) {
		for _, node := range tree {
			if node == nil || node.Origin == nil || executionMask.isSkipped(node) || executionMask.suppressesTransportProducer(node) {
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
			case *physicalop.PhysicalExchangeReceiver, *physicalop.PhysicalExchangeSender:
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
	if dml {
		if ruv2Metrics == nil || ruv2Metrics.Bypass() {
			op.status = readBillingDemoStatusUnknownInput
			op.reason = readBillingDemoReasonMissingReaderTransport
			return op, true
		}
		requests, ok := readBillingDemoCopRPCCount(flat, runtimeStats, executionMask)
		if !ok {
			if hasTasks || !allReaderRowsZero {
				op.status = readBillingDemoStatusUnknownInput
				op.reason = readBillingDemoReasonMissingReaderTransport
				return op, true
			}
			requests = 0
		}
		netBytes := ruv2Metrics.TiKVCoprocessorResponseBytes()
		if netBytes < 0 || requests < 0 || (netBytes > 0 && requests == 0) {
			op.status = readBillingDemoStatusUnknownInput
			op.reason = readBillingDemoReasonMissingReaderTransport
			return op, true
		}
		op.status = readBillingDemoStatusOperatorOK
		op.reason = readBillingDemoReasonNone
		op.units = []readBillingDemoUnit{
			{unit: readBillingDemoUnitNetBytes, source: readBillingDemoInputSourceRUV2Metrics, side: readBillingDemoInputSideAll, value: float64(netBytes), widthSource: explainRUWidthSourceNotApplicable},
		}
		return op, true
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
	}
	return op, true
}

type readBillingDemoRPCStats interface {
	GetCmdRPCCount(tikvrpc.CmdType) int64
}

type readBillingDemoPointScanStats interface {
	GetScanDetailAndCoverage() (tikvutil.ScanDetail, uint64, uint64)
}

func readBillingDemoCopRPCCount(
	flat *FlatPhysicalPlan,
	runtimeStats *execdetails.RuntimeStatsColl,
	executionMasks ...*readBillingDemoExecutionMask,
) (int64, bool) {
	if flat == nil || runtimeStats == nil {
		return 0, false
	}
	executionMask, validMask := readBillingDemoOptionalExecutionMask(executionMasks)
	if !validMask {
		return 0, false
	}
	planIDs := make(map[int]struct{})
	for _, tree := range readBillingDemoAllTrees(flat) {
		for _, node := range tree {
			if node != nil && node.Origin != nil && !executionMask.isSkipped(node) {
				planIDs[node.Origin.ID()] = struct{}{}
			}
		}
	}
	var total int64
	found := false
	for planID := range planIDs {
		if !runtimeStats.ExistsRootStats(planID) {
			continue
		}
		_, groups := runtimeStats.GetRootStats(planID).MergeStats()
		for _, group := range groups {
			if group.Tp() != execdetails.TpSelectResultRuntimeStats {
				continue
			}
			rpcStats, ok := group.(readBillingDemoRPCStats)
			if !ok {
				return 0, false
			}
			found = true
			for _, cmd := range []tikvrpc.CmdType{tikvrpc.CmdCop, tikvrpc.CmdCopStream} {
				count := rpcStats.GetCmdRPCCount(cmd)
				if count < 0 || count > math.MaxInt64-total {
					return 0, false
				}
				total += count
			}
		}
	}
	return total, found
}

func readBillingDemoPointLookupTransport(
	flat *FlatPhysicalPlan,
	runtimeStats *execdetails.RuntimeStatsColl,
	dml bool,
	executionMasks ...*readBillingDemoExecutionMask,
) (readBillingDemoOperatorResult, bool) {
	op := readBillingDemoOperatorResult{
		id:            "point_lookup@statement",
		site:          readBillingDemoSiteTiKV,
		opClass:       readBillingDemoOpClassPointLookup,
		operatorKind:  "mixed_point_lookup",
		emitStatusRow: true,
	}
	executionMask, validMask := readBillingDemoOptionalExecutionMask(executionMasks)
	if !validMask {
		op.status = readBillingDemoStatusUnknownInput
		op.reason = readBillingDemoReasonUnsupportedCopStructure
		return op, true
	}
	kinds := make(map[string]struct{})
	pointLookupPlans := make(map[int]struct{})
	for _, tree := range readBillingDemoAllTrees(flat) {
		for _, node := range tree {
			if node == nil || node.Origin == nil || executionMask.isSkipped(node) || executionMask.suppressesTransportProducer(node) {
				continue
			}
			switch plan := node.Origin.(type) {
			case *physicalop.PointGetPlan:
				kinds["point_get"] = struct{}{}
				pointLookupPlans[plan.ID()] = struct{}{}
			case *physicalop.BatchPointGetPlan:
				kinds["batch_point_get"] = struct{}{}
				pointLookupPlans[plan.ID()] = struct{}{}
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
	detail, detailRecords, completedResponses, failureReason := readBillingDemoPointLookupScanDetails(runtimeStats, pointLookupPlans)
	if failureReason != "" {
		op.status = readBillingDemoStatusUnknownInput
		op.reason = failureReason
		return op, true
	}
	op.status = readBillingDemoStatusOperatorOK
	op.reason = readBillingDemoReasonNone
	op.units = []readBillingDemoUnit{
		{unit: readBillingDemoUnitCPUWork, source: readBillingDemoInputSourceSnapshotRuntimeStats, side: readBillingDemoInputSideAll, value: float64(detail.TotalKeys), widthSource: explainRUWidthSourceNotApplicable},
		{unit: readBillingDemoUnitScanBytes, source: readBillingDemoInputSourceSnapshotRuntimeStats, side: readBillingDemoInputSideAll, value: float64(detail.ProcessedKeysSize), widthSource: explainRUWidthSourceNotApplicable},
		{unit: readBillingDemoUnitTotalKeys, source: readBillingDemoInputSourceSnapshotRuntimeStats, side: readBillingDemoInputSideAll, value: float64(detail.TotalKeys), widthSource: explainRUWidthSourceNotApplicable},
		{unit: readBillingDemoUnitProcessedKeys, source: readBillingDemoInputSourceSnapshotRuntimeStats, side: readBillingDemoInputSideAll, value: float64(detail.ProcessedKeys), widthSource: explainRUWidthSourceNotApplicable},
		{unit: readBillingDemoUnitProcessedKeysSize, source: readBillingDemoInputSourceSnapshotRuntimeStats, side: readBillingDemoInputSideAll, value: float64(detail.ProcessedKeysSize), widthSource: explainRUWidthSourceNotApplicable},
		{unit: readBillingDemoUnitDetailRecords, source: readBillingDemoInputSourceSnapshotRuntimeStats, side: readBillingDemoInputSideAll, value: float64(detailRecords), widthSource: explainRUWidthSourceNotApplicable},
		{unit: readBillingDemoUnitCompletedResponses, source: readBillingDemoInputSourceSnapshotRuntimeStats, side: readBillingDemoInputSideAll, value: float64(completedResponses), widthSource: explainRUWidthSourceNotApplicable},
	}
	return op, true
}

func readBillingDemoPointLookupScanDetails(runtimeStats *execdetails.RuntimeStatsColl, plans map[int]struct{}) (tikvutil.ScanDetail, uint64, uint64, string) {
	if runtimeStats == nil {
		return tikvutil.ScanDetail{}, 0, 0, readBillingDemoReasonMissingPointScanStats
	}
	var total tikvutil.ScanDetail
	var totalDetailRecords uint64
	var totalCompletedResponses uint64
	for planID := range plans {
		found := false
		if runtimeStats.ExistsRootStats(planID) {
			_, groups := runtimeStats.GetRootStats(planID).MergeStats()
			for _, group := range groups {
				pointStats, ok := group.(readBillingDemoPointScanStats)
				if !ok {
					continue
				}
				found = true
				detail, detailRecords, completedResponses := pointStats.GetScanDetailAndCoverage()
				if failureReason := readBillingDemoPointScanDetailFailure(detail, detailRecords, completedResponses); failureReason != "" {
					return tikvutil.ScanDetail{}, 0, 0, failureReason
				}
				if detailRecords > math.MaxUint64-totalDetailRecords ||
					completedResponses > math.MaxUint64-totalCompletedResponses ||
					detail.TotalKeys > math.MaxInt64-total.TotalKeys ||
					detail.ProcessedKeys > math.MaxInt64-total.ProcessedKeys ||
					detail.ProcessedKeysSize > math.MaxInt64-total.ProcessedKeysSize {
					return tikvutil.ScanDetail{}, 0, 0, readBillingDemoReasonInvalidPointScanDetail
				}
				total.TotalKeys += detail.TotalKeys
				total.ProcessedKeys += detail.ProcessedKeys
				total.ProcessedKeysSize += detail.ProcessedKeysSize
				totalDetailRecords += detailRecords
				totalCompletedResponses += completedResponses
			}
		}
		if !found && !readBillingDemoPointLookupLocallyShortCircuited(runtimeStats, planID) {
			return tikvutil.ScanDetail{}, 0, 0, readBillingDemoReasonMissingPointScanStats
		}
	}
	return total, totalDetailRecords, totalCompletedResponses, ""
}

// A partition-pruned PointGet can be replaced by TableDualExec after the
// physical plan has been flattened. In that case no SnapshotRuntimeStats is
// created, and completed zero-row BasicRuntimeStats is the only execution
// evidence. It contributes zero point storage work.
func readBillingDemoPointLookupLocallyShortCircuited(runtimeStats *execdetails.RuntimeStatsColl, planID int) bool {
	if runtimeStats == nil || planID <= 0 {
		return false
	}
	basic := runtimeStats.GetBasicRuntimeStats(planID, false)
	return basic != nil && basic.GetActRows() == 0 && (basic.HasRuntimeRows() || basic.HasBytes())
}

func readBillingDemoPointScanDetailFailure(detail tikvutil.ScanDetail, detailRecords, completedResponses uint64) string {
	if detail.TotalKeys < 0 || detail.ProcessedKeys < 0 || detail.ProcessedKeysSize < 0 {
		return readBillingDemoReasonInvalidPointScanDetail
	}
	if completedResponses == 0 {
		if detailRecords == 0 && detail.TotalKeys == 0 && detail.ProcessedKeys == 0 && detail.ProcessedKeysSize == 0 {
			return ""
		}
		return readBillingDemoReasonInvalidPointScanDetail
	}
	if detailRecords != completedResponses {
		return readBillingDemoReasonIncompletePointScanDetail
	}
	return ""
}

func readBillingDemoWeightsValid(weights readBillingDemoWeights) bool {
	if weights.ModelVersion != readBillingDemoModelVersion || weights.Version == "" ||
		weights.Version == readBillingDemoWeightVersion || !weights.Calibrated ||
		weights.MutationBytesPerCPUUnit <= 0 || math.IsNaN(weights.MutationBytesPerCPUUnit) || math.IsInf(weights.MutationBytesPerCPUUnit, 0) {
		return false
	}
	for _, weight := range []float64{
		weights.CPUWeight, weights.ScanWeight, weights.NetWeight, weights.HashTableWeight, weights.JoinWeight,
		weights.WriteKeyWeight, weights.WriteBytesWeight, weights.FrontendCompileWeight,
	} {
		if weight < 0 || math.IsNaN(weight) || math.IsInf(weight, 0) {
			return false
		}
	}
	return true
}

func readBillingDemoActiveWeightVersion() string {
	if readBillingDemoV6Weights.Version != "" {
		return readBillingDemoV6Weights.Version
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
	case readBillingDemoUnitHashStateRows:
		return weights.HashTableWeight, true
	case readBillingDemoUnitJoinOutputRows:
		return weights.JoinWeight, true
	case readBillingDemoUnitWriteKeys:
		return weights.WriteKeyWeight, true
	case readBillingDemoUnitWriteBytes:
		return weights.WriteBytesWeight, true
	case readBillingDemoUnitFrontendCompileBytes:
		return weights.FrontendCompileWeight, true
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
	// The three convenience totals are a v3 schema. V6 detail is preserved in
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

func newReadBillingDemoCopEstimator(
	tree FlatPlanTree,
	runtimeStats *execdetails.RuntimeStatsColl,
	executionMasks ...*readBillingDemoExecutionMask,
) *readBillingDemoCopEstimator {
	executionMask, validMask := readBillingDemoOptionalExecutionMask(executionMasks)
	estimator := &readBillingDemoCopEstimator{
		tree:            tree,
		runtimeStats:    runtimeStats,
		executionMask:   executionMask,
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
	if !validMask {
		addStructuralFailure(0)
		return estimator
	}

	// Build the reverse direct-edge index and reject malformed references. This
	// pass is O(n+m), where m is the number of explicit ChildrenIdx entries.
	for idx, node := range tree {
		estimator.nodeVisits++
		if node == nil || node.Origin == nil || executionMask.isSkipped(node) {
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
			if executionMask.isSkipped(tree[childIdx]) {
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
		if !estimator.isActiveTiKVCopNode(node) {
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
		if !estimator.isActiveTiKVCopNode(parent) {
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
		if estimator.isActiveTiKVCopNode(node) && estimator.componentID[idx] < 0 {
			addStructuralFailure(idx)
		}
	}

	// Gather component evidence and intrinsic invalid-row failures in one
	// exact-plan-ID pass. Detail ownership and row ownership intentionally stay
	// separate because distsql attaches response ScanDetail to the last plan ID.
	for idx, node := range tree {
		estimator.nodeVisits++
		if !estimator.isActiveTiKVCopNode(node) {
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

func (e *readBillingDemoCopEstimator) isActiveTiKVCopNode(node *FlatOperator) bool {
	return e != nil && !e.executionMask.isSkipped(node) && readBillingDemoIsTiKVCopNode(node)
}

func (e *readBillingDemoCopEstimator) validateReadBillingDemoCopComponent(rootIdx int, addStructuralFailure func(int)) {
	if rootIdx < 0 || rootIdx >= len(e.tree) || !e.isActiveTiKVCopNode(e.tree[rootIdx]) {
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
		if !e.isActiveTiKVCopNode(node) {
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
	if idx < 0 || idx >= len(e.tree) || !e.isActiveTiKVCopNode(e.tree[idx]) {
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
	if childIdx < 0 || childIdx >= len(e.tree) || !e.isActiveTiKVCopNode(e.tree[childIdx]) || e.componentID[childIdx] != e.componentID[idx] {
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
	if idx < 0 || idx >= len(e.tree) || !e.isActiveTiKVCopNode(e.tree[idx]) {
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
	if idx < 0 || idx >= len(e.tree) || !e.isActiveTiKVCopNode(e.tree[idx]) {
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

func appendReadBillingDemoTree(
	result *readBillingDemoResult,
	sctx base.PlanContext,
	runtimeStats *execdetails.RuntimeStatsColl,
	tree FlatPlanTree,
	executionMasks ...*readBillingDemoExecutionMask,
) (string, readBillingDemoOperatorResult) {
	estimator := newReadBillingDemoCopEstimator(tree, runtimeStats, executionMasks...)
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
func appendReadBillingDemoDMLTree(
	result *readBillingDemoResult,
	runtimeStats *execdetails.RuntimeStatsColl,
	tree FlatPlanTree,
	executionMasks ...*readBillingDemoExecutionMask,
) {
	estimator := newReadBillingDemoCopEstimator(tree, runtimeStats, executionMasks...)
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
	if estimator.executionMask.isSkipped(op) {
		return readBillingDemoAppendOutcome{success: true, status: readBillingDemoStatusSuccess}
	}
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
		units, missingReason, ok = readBillingDemoRootUnits(runtimeStats, tree, idx, op, operator, estimator.executionMask)
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
		readBillingDemoOpClassMergeJoin, readBillingDemoOpClassLookupJoin, readBillingDemoOpClassShuffle,
		readBillingDemoOpClassRangeScan,
		readBillingDemoOpClassReaderTransport, readBillingDemoOpClassOverlayReader,
		readBillingDemoOpClassKVMutation, readBillingDemoOpClassKVWrite, readBillingDemoOpClassSQLFrontend:
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
	case plancodec.TypeShuffle:
		shuffle, ok := op.Origin.(*physicalop.PhysicalShuffle)
		if !ok {
			return readBillingDemoOperatorResult{site: readBillingDemoSiteTiDB, opClass: readBillingDemoOpClassShuffle, operatorKind: operatorKind}, false, readBillingDemoReasonUnsupportedOperator
		}
		switch shuffle.SplitterType {
		case physicalop.PartitionHashSplitterType:
			operatorKind = readBillingDemoOperatorHashShuffle
		case physicalop.PartitionRangeSplitterType:
			operatorKind = readBillingDemoOperatorRangeShuffle
		default:
			return readBillingDemoOperatorResult{site: readBillingDemoSiteTiDB, opClass: readBillingDemoOpClassShuffle, operatorKind: operatorKind}, false, readBillingDemoReasonUnsupportedOperator
		}
		return readBillingDemoOperatorResult{site: readBillingDemoSiteTiDB, opClass: readBillingDemoOpClassShuffle, operatorKind: operatorKind}, true, ""
	case plancodec.TypeShuffleReceiver:
		return readBillingDemoOperatorResult{site: readBillingDemoSiteTiDB, opClass: readBillingDemoOpClassWrapper, operatorKind: operatorKind}, true, ""
	case plancodec.TypeIndexMerge:
		return readBillingDemoOperatorResult{site: readBillingDemoSiteTiDB, opClass: readBillingDemoOpClassLookupReader, operatorKind: operatorKind}, true, ""
	case plancodec.TypeLock:
		return readBillingDemoOperatorResult{site: readBillingDemoSiteTiDB, opClass: readBillingDemoOpClassWrapper, operatorKind: operatorKind}, true, ""
	case plancodec.TypePointGet, plancodec.TypeBatchPointGet:
		return readBillingDemoOperatorResult{site: readBillingDemoSiteTiKV, opClass: readBillingDemoOpClassPointLookup, operatorKind: operatorKind}, true, ""
	case plancodec.TypeSel:
		return readBillingDemoOperatorResult{site: readBillingDemoSiteTiDB, opClass: readBillingDemoOpClassFilter, operatorKind: operatorKind}, true, ""
	case plancodec.TypeProj:
		return readBillingDemoOperatorResult{site: readBillingDemoSiteTiDB, opClass: readBillingDemoOpClassProjection, operatorKind: operatorKind}, true, ""
	case plancodec.TypeLimit, plancodec.TypeMaxOneRow:
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
	case plancodec.TypeUnion, plancodec.TypeScalarSubQuery, plancodec.TypeApply,
		plancodec.TypeCTE, plancodec.TypeCTETable:
		// Apply and CTE orchestration have no dedicated formulas yet. Keep their
		// executed descendants reportable without charging the wrappers themselves.
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

func readBillingDemoRootUnits(
	runtimeStats *execdetails.RuntimeStatsColl,
	tree FlatPlanTree,
	idx int,
	op *FlatOperator,
	operator readBillingDemoOperatorResult,
	executionMasks ...*readBillingDemoExecutionMask,
) ([]readBillingDemoUnit, string, bool) {
	executionMask, validMask := readBillingDemoOptionalExecutionMask(executionMasks)
	if !validMask {
		return nil, readBillingDemoReasonUnsupportedCopStructure, false
	}
	if operator.opClass == readBillingDemoOpClassShuffle {
		return readBillingDemoShuffleUnits(runtimeStats, tree, op, executionMask)
	}
	if operator.opClass == readBillingDemoOpClassLimit && executionMask.isExplicitZeroLimit(op) {
		limit, ok := op.Origin.(*physicalop.PhysicalLimit)
		if !ok || limit.Count != 0 {
			return nil, readBillingDemoReasonUnsupportedCopStructure, false
		}
		return []readBillingDemoUnit{{
			unit:        readBillingDemoUnitCPUWork,
			source:      readBillingDemoInputSourcePhysicalPlan,
			side:        readBillingDemoInputSideAll,
			value:       0,
			widthSource: explainRUWidthSourceNotApplicable,
		}}, "", true
	}
	outputRows, hasOutputRows := readBillingDemoPlanActRows(runtimeStats, op.Origin.ID())
	outputRowsSource := readBillingDemoInputSourceRuntimeOperatorActRows
	if !hasOutputRows && operator.opClass == readBillingDemoOpClassLookupJoin && executionMask.hasSkippedInner(op) {
		outputRows = 0
		hasOutputRows = true
		outputRowsSource = readBillingDemoInputSourcePhysicalPlan
	}
	if !hasOutputRows || outputRows < 0 {
		return nil, readBillingDemoReasonMissingRuntimeRows, false
	}
	var units []readBillingDemoUnit
	appendExpressionCPU := func(rows int64, source string) (string, bool) {
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
			readBillingDemoUnit{unit: readBillingDemoUnitCPUWork, source: source, side: readBillingDemoInputSideAll, value: work, widthSource: explainRUWidthSourceNotApplicable},
		)
		return "", true
	}
	switch operator.opClass {
	case readBillingDemoOpClassHashJoin, readBillingDemoOpClassMergeJoin, readBillingDemoOpClassLookupJoin:
		if idx < 0 || idx >= len(tree) || len(tree[idx].ChildrenIdx) != 2 {
			return nil, readBillingDemoReasonMissingExpressionCount, false
		}
		var inputRows int64
		allInputsSemanticZero := true
		for _, childIdx := range tree[idx].ChildrenIdx {
			if childIdx < 0 || childIdx >= len(tree) || tree[childIdx] == nil || !tree[childIdx].IsRoot {
				return nil, readBillingDemoReasonMissingRuntimeRows, false
			}
			if operator.opClass == readBillingDemoOpClassLookupJoin && executionMask.isSkippedInner(op, tree[childIdx]) {
				continue
			}
			rows, ok, semanticZero := readBillingDemoDirectChildActRows(runtimeStats, executionMask, tree[childIdx])
			if !ok || rows < 0 || (rows > 0 && inputRows > math.MaxInt64-rows) {
				return nil, readBillingDemoReasonMissingRuntimeRows, false
			}
			inputRows += rows
			allInputsSemanticZero = allInputsSemanticZero && semanticZero
		}
		inputSource := readBillingDemoInputSourceRuntimeChildActRows
		if allInputsSemanticZero {
			inputSource = readBillingDemoInputSourcePhysicalPlan
		}
		if reason, ok := appendExpressionCPU(inputRows, inputSource); !ok {
			return nil, reason, false
		}
		units = append(units, readBillingDemoUnit{unit: readBillingDemoUnitJoinOutputRows, source: outputRowsSource, side: readBillingDemoInputSideAll, value: float64(outputRows), widthSource: explainRUWidthSourceNotApplicable})
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
		inputRows, ok, semanticZero := readBillingDemoDirectChildActRows(runtimeStats, executionMask, tree[childIdx])
		if !ok || inputRows < 0 {
			return nil, readBillingDemoReasonMissingRuntimeRows, false
		}
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
			inputSource := readBillingDemoInputSourceRuntimeChildActRows
			if semanticZero {
				inputSource = readBillingDemoInputSourcePhysicalPlan
			}
			units = append(units, readBillingDemoUnit{unit: readBillingDemoUnitCPUWork, source: inputSource, side: readBillingDemoInputSideAll, value: float64(inputRows), widthSource: explainRUWidthSourceNotApplicable})
		} else {
			inputSource := readBillingDemoInputSourceRuntimeChildActRows
			if semanticZero {
				inputSource = readBillingDemoInputSourcePhysicalPlan
			}
			if reason, ok := appendExpressionCPU(inputRows, inputSource); !ok {
				return nil, reason, false
			}
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

func readBillingDemoShuffleUnits(
	runtimeStats *execdetails.RuntimeStatsColl,
	tree FlatPlanTree,
	op *FlatOperator,
	executionMask *readBillingDemoExecutionMask,
) ([]readBillingDemoUnit, string, bool) {
	if op == nil || op.Origin == nil {
		return nil, readBillingDemoReasonInvalidShuffleStructure, false
	}
	shuffle, ok := op.Origin.(*physicalop.PhysicalShuffle)
	if !ok || len(shuffle.DataSources) == 0 || len(shuffle.DataSources) != len(shuffle.ByItemArrays) {
		return nil, readBillingDemoReasonInvalidShuffleStructure, false
	}

	seenDataSources := make(map[int]struct{}, len(shuffle.DataSources))
	var totalWork int64
	for i, dataSource := range shuffle.DataSources {
		if dataSource == nil || dataSource.ID() <= 0 {
			return nil, readBillingDemoReasonInvalidShuffleStructure, false
		}
		planID := dataSource.ID()
		if _, exists := seenDataSources[planID]; exists {
			return nil, readBillingDemoReasonInvalidShuffleStructure, false
		}
		seenDataSources[planID] = struct{}{}
		if executionMask != nil && executionMask.planIDOccurrences[planID] != 1 {
			return nil, readBillingDemoReasonInvalidShuffleStructure, false
		}

		occurrences := 0
		for _, node := range tree {
			if node == nil || node.Origin == nil {
				continue
			}
			if node.Origin.ID() == planID {
				occurrences++
			}
		}
		if occurrences != 1 {
			return nil, readBillingDemoReasonInvalidShuffleStructure, false
		}

		rows, _, hasRows := readBillingDemoRootOutputRowsAndBytes(runtimeStats, planID)
		if !hasRows {
			return nil, readBillingDemoReasonMissingRuntimeRows, false
		}
		if rows < 0 || len(shuffle.ByItemArrays[i]) > math.MaxInt64-1 {
			return nil, readBillingDemoReasonInvalidShuffleWork, false
		}
		multiplier := int64(len(shuffle.ByItemArrays[i])) + 1
		if rows > math.MaxInt64/multiplier {
			return nil, readBillingDemoReasonInvalidShuffleWork, false
		}
		work := rows * multiplier
		if totalWork > math.MaxInt64-work {
			return nil, readBillingDemoReasonInvalidShuffleWork, false
		}
		totalWork += work
	}

	work := float64(totalWork)
	if work < 0 || math.IsNaN(work) || math.IsInf(work, 0) {
		return nil, readBillingDemoReasonInvalidShuffleWork, false
	}
	return []readBillingDemoUnit{{
		unit:        readBillingDemoUnitCPUWork,
		source:      readBillingDemoInputSourceShuffleDataSourceRows,
		side:        readBillingDemoInputSideAll,
		value:       work,
		widthSource: explainRUWidthSourceNotApplicable,
	}}, "", true
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

func readBillingDemoDirectChildActRows(
	runtimeStats *execdetails.RuntimeStatsColl,
	executionMask *readBillingDemoExecutionMask,
	child *FlatOperator,
) (rows int64, ok, semanticZero bool) {
	if child == nil || child.Origin == nil {
		return 0, false, false
	}
	if readBillingDemoMaskedRootSemanticZero(runtimeStats, executionMask, child) {
		return 0, true, true
	}
	rows, ok = readBillingDemoPlanActRows(runtimeStats, child.Origin.ID())
	return rows, ok, false
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
		units := []readBillingDemoUnit{
			readBillingDemoUnit{unit: readBillingDemoUnitScanBytes, source: readBillingDemoInputSourceScanDetail, side: readBillingDemoInputSideAll, value: scanInputBytes, rowWidth: rowWidth, widthSource: explainRUWidthSourceScanDetailProcessedEstimate},
		}
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
	var units []readBillingDemoUnit
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
	if totalKeys < 0 || processedKeys < 0 || processedKeysSize < 0 {
		return 0, 0, false
	}
	// TiKV can report TotalKeys for a seek that produces no user record. An
	// omitted ProcessedKeys field is decoded as zero, so the paired zero size
	// is the evidence that the scan produced no bytes.
	if processedKeys == 0 && processedKeysSize == 0 {
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
	weightsReady := readBillingDemoWeightsValid(readBillingDemoV6Weights)
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
			if _, semantic := readBillingDemoUnitWeight(readBillingDemoV6Weights, unit.unit); semantic {
				if weight, previewRU, ok := readBillingDemoUnitPreviewRU(unit, readBillingDemoV6Weights); ok {
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
		note:          readBillingDemoVersionNote(),
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
	note := readBillingDemoVersionNote()
	if !readBillingDemoWeightsValid(readBillingDemoV6Weights) {
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
		note:           "input_side=" + unit.side + "," + readBillingDemoVersionNote(),
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
	case readBillingDemoUnitScanBytes, readBillingDemoUnitNetBytes, readBillingDemoUnitWriteBytes,
		readBillingDemoUnitFrontendCompileBytes:
		row.workBytes = unit.value
		row.hasWorkBytes = true
	case readBillingDemoUnitExpressionCount, readBillingDemoUnitHashStateRows, readBillingDemoUnitJoinOutputRows,
		readBillingDemoUnitTotalKeys, readBillingDemoUnitProcessedKeys, readBillingDemoUnitDetailRecords,
		readBillingDemoUnitCompletedResponses:
		row.count = int64(unit.value)
		row.hasCount = true
	case readBillingDemoUnitEncodedMutationCount, readBillingDemoUnitSetCount, readBillingDemoUnitDeleteCount,
		readBillingDemoUnitWriteKeys, readBillingDemoUnitPrewriteRegionNum, readBillingDemoUnitTiKVWriteRPCCount:
		row.count = int64(unit.value)
		row.hasCount = true
	case readBillingDemoUnitEncodedMutationBytes, readBillingDemoUnitKeyBytes, readBillingDemoUnitValueBytes,
		readBillingDemoUnitProcessedKeysSize:
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
		readBillingDemoUnitOutputRows, readBillingDemoUnitOutputBytes,
		readBillingDemoUnitTotalKeys, readBillingDemoUnitProcessedKeys, readBillingDemoUnitProcessedKeysSize,
		readBillingDemoUnitDetailRecords, readBillingDemoUnitCompletedResponses:
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

func readBillingDemoVersionNote() string {
	return "model_version=" + readBillingDemoModelVersion + ",weight_version=" + readBillingDemoActiveWeightVersion()
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
