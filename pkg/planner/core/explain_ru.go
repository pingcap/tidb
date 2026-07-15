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
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
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
	explainRUWidthSourceScanDetailProcessedAvg        = "scan_detail_processed_key_avg"
	explainRUWidthSourceScanDetailProcessedEstimate   = "scan_detail_processed_key_avg_estimate"
	explainRUWidthSourceNotApplicable                 = "not_applicable"
	readBillingDemoModelVersion                       = "v3"
	readBillingDemoWeightVersion                      = "v2"
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
	readBillingDemoOpClassWrapper                     = "wrapper"
	readBillingDemoOpClassSynthetic                   = "synthetic_source"
	readBillingDemoOperatorStatement                  = "statement"
	readBillingDemoOperatorMemDBMutation              = "memdb_mutation"
	readBillingDemoOperatorTxnPrewrite                = "txn_prewrite"
	readBillingDemoUnitFixedEvents                    = "fixed_events"
	readBillingDemoUnitInputRows                      = "input_rows"
	readBillingDemoUnitInputBytes                     = "input_bytes"
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
	readBillingDemoInputSourceRuntimeChunkBytes       = "runtime_chunk_bytes"
	readBillingDemoInputSourceScanDetail              = "scan_detail"
	readBillingDemoInputSourceRuntimeChildActRows     = "runtime_child_act_rows"
	readBillingDemoInputSourceRuntimeOrderingWork     = "runtime_ordering_work"
	readBillingDemoInputSourceStmtMemDBMutation       = "stmt_memdb_mutation_calls"
	readBillingDemoInputSourceCommitDetail            = "commit_detail"
	readBillingDemoInputSourceRUV2Metrics             = "ruv2_metrics"
	readBillingDemoInputSideAll                       = "all"
	readBillingDemoInputSideBuild                     = "build"
	readBillingDemoInputSideProbe                     = "probe"
	readBillingDemoInputSideLeft                      = "left"
	readBillingDemoInputSideRight                     = "right"
	readBillingDemoScopeStatementAttempted            = "statement_attempted"
	readBillingDemoScopeTxnPrewritePayload            = "txn_prewrite_payload"

	// The first write-side preview formula intentionally has no fixed/RPC/region
	// terms: write keys use the current scaled RUv2 write-key coefficient, and
	// write bytes use the existing TiKV range-scan byte coefficient as a
	// calibration seed because RUv2 only shadows commit WriteSize today.
	readBillingDemoWriteRUScale        = 2.01
	readBillingDemoRUV2WriteKeysWeight = 0.330760861554226
	readBillingDemoWriteKeyWeight      = readBillingDemoWriteRUScale * readBillingDemoRUV2WriteKeysWeight
	readBillingDemoWriteByteWeight     = 0.000020
	// Mutation weights are independent preview calibration slots. They stay at
	// zero until mutation-only TiDB CPU calibration supplies a defensible seed;
	// they must never alias RUv2 or TiKV write coefficients.
	readBillingDemoMutationCountWeight  = 0.0
	readBillingDemoMutationByteWeight   = 0.0
	readBillingDemoDiagnosticZeroWeight = 0.0
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
	scanCount         int
	scanIdx           int
	detailHolderCount int
	scanDetail        tikvutil.ScanDetail
	maxSummaryTasks   int32
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

type readBillingDemoOperatorWeights struct {
	fixedEvent    float64
	row           float64
	byte          float64
	orderWork     float64
	mutationCount float64
	mutationByte  float64
	setCount      float64
	deleteCount   float64
	keyByte       float64
	valueByte     float64
	writeKey      float64
	writeByte     float64
	writeRPC      float64
	region        float64
}

type readBillingDemoWeightKey struct {
	site    string
	opClass string
	version string
}

var readBillingDemoWeights = map[readBillingDemoWeightKey]readBillingDemoOperatorWeights{
	{readBillingDemoSiteTiKV, readBillingDemoOpClassRangeScan, readBillingDemoWeightVersion}:   {fixedEvent: 0.070, row: 0.000045, byte: 0.000020},
	{readBillingDemoSiteTiKV, readBillingDemoOpClassKVWrite, readBillingDemoWeightVersion}:     {writeKey: readBillingDemoWriteKeyWeight, writeByte: readBillingDemoWriteByteWeight, writeRPC: readBillingDemoDiagnosticZeroWeight, region: readBillingDemoDiagnosticZeroWeight},
	{readBillingDemoSiteTiKV, readBillingDemoOpClassFilter, readBillingDemoWeightVersion}:      {fixedEvent: 0.020, row: 0.000040, byte: 0.000006},
	{readBillingDemoSiteTiKV, readBillingDemoOpClassProjection, readBillingDemoWeightVersion}:  {fixedEvent: 0.020, row: 0.000030, byte: 0.000006},
	{readBillingDemoSiteTiKV, readBillingDemoOpClassLimit, readBillingDemoWeightVersion}:       {fixedEvent: 0.010, row: 0.000008, byte: 0.000002},
	{readBillingDemoSiteTiKV, readBillingDemoOpClassTopN, readBillingDemoWeightVersion}:        {fixedEvent: 0.060, byte: 0.000012, orderWork: 0.000075},
	{readBillingDemoSiteTiKV, readBillingDemoOpClassHashAgg, readBillingDemoWeightVersion}:     {fixedEvent: 0.080, row: 0.000100, byte: 0.000014},
	{readBillingDemoSiteTiKV, readBillingDemoOpClassStreamAgg, readBillingDemoWeightVersion}:   {fixedEvent: 0.060, row: 0.000065, byte: 0.000010},
	{readBillingDemoSiteTiKV, readBillingDemoOpClassPointLookup, readBillingDemoWeightVersion}: {fixedEvent: 0.045, row: 0.000030, byte: 0.000012},
	{readBillingDemoSiteTiDB, readBillingDemoOpClassKVMutation, readBillingDemoWeightVersion}: {
		mutationCount: readBillingDemoMutationCountWeight,
		mutationByte:  readBillingDemoMutationByteWeight,
		setCount:      readBillingDemoDiagnosticZeroWeight,
		deleteCount:   readBillingDemoDiagnosticZeroWeight,
		keyByte:       readBillingDemoDiagnosticZeroWeight,
		valueByte:     readBillingDemoDiagnosticZeroWeight,
	},
	{readBillingDemoSiteTiDB, readBillingDemoOpClassFilter, readBillingDemoWeightVersion}:        {fixedEvent: 0.020, row: 0.000030, byte: 0.000005},
	{readBillingDemoSiteTiDB, readBillingDemoOpClassProjection, readBillingDemoWeightVersion}:    {fixedEvent: 0.020, row: 0.000020, byte: 0.000004},
	{readBillingDemoSiteTiDB, readBillingDemoOpClassLimit, readBillingDemoWeightVersion}:         {fixedEvent: 0.010, row: 0.000006, byte: 0.000001},
	{readBillingDemoSiteTiDB, readBillingDemoOpClassTopN, readBillingDemoWeightVersion}:          {fixedEvent: 0.060, byte: 0.000010, orderWork: 0.000060},
	{readBillingDemoSiteTiDB, readBillingDemoOpClassSort, readBillingDemoWeightVersion}:          {fixedEvent: 0.080, byte: 0.000012, orderWork: 0.000070},
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

	var commitDetail *tikvutil.CommitDetails
	var pipelined bool
	if sctx != nil && sctx.GetSessionVars() != nil {
		vars := sctx.GetSessionVars()
		if ruv2Metrics == nil {
			ruv2Metrics = vars.RUV2Metrics
		}
		if vars.StmtCtx != nil {
			commitDetail = vars.StmtCtx.GetExecDetails().CommitDetail
			if recorder := vars.StmtCtx.PreviewKVMutationRecorder; recorder != nil {
				pipelined = recorder.Snapshot().Pipelined
			}
		}
	}

	// A final COMMIT owns the transaction-scoped TiKV payload. Keep dml_kind
	// empty rather than guessing how the payload should be split among prior
	// statements in the explicit transaction.
	result.operators = append(result.operators, buildTiKVWriteBillingDemoOperators("", commitDetail, ruv2Metrics, pipelined)...)
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

	var commitDetail *tikvutil.CommitDetails
	var mutationPipelined bool
	if sctx != nil && sctx.GetSessionVars() != nil {
		if ruv2Metrics == nil {
			ruv2Metrics = sctx.GetSessionVars().RUV2Metrics
		}
		if sctx.GetSessionVars().StmtCtx != nil {
			commitDetail = sctx.GetSessionVars().StmtCtx.GetExecDetails().CommitDetail
			if recorder := sctx.GetSessionVars().StmtCtx.PreviewKVMutationRecorder; recorder != nil {
				mutationPipelined = recorder.Snapshot().Pipelined
			}
		}
	}

	appendReadBillingDemoDMLPlan(&result, sctx, plan)
	appendReadBillingDemoMutation(&result, sctx, dmlKind)
	result.operators = append(result.operators, buildTiKVWriteBillingDemoOperators(dmlKind, commitDetail, ruv2Metrics, mutationPipelined)...)
	return result
}

func appendReadBillingDemoDMLPlan(result *readBillingDemoResult, sctx base.PlanContext, plan base.Plan) {
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
		id:           "stmt_memdb_mutation",
		site:         readBillingDemoSiteTiDB,
		opClass:      readBillingDemoOpClassKVMutation,
		operatorKind: readBillingDemoOperatorMemDBMutation,
		dmlKind:      dmlKind,
		scope:        readBillingDemoScopeStatementAttempted,
		uncalibrated: true,
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
	result.operators = append(result.operators, operator)
	result.operators = append(result.operators, readBillingDemoMutationDiagnostic(dmlKind, readBillingDemoReasonUncalibratedMutation))
	// The foreground gate does not currently distinguish simple DML from
	// schemas that require unmodeled row preparation, constraint checks, or FK
	// orchestration. Keep the encoded mutation units available, but report that
	// wider DML CPU as partial for every supported DML kind, including DELETE.
	result.operators = append(result.operators, readBillingDemoMutationDiagnostic(dmlKind, readBillingDemoReasonDMLAncillaryPartial))
	vars := sctx.GetSessionVars()
	if vars.InTxn() && vars.TxnCtx != nil && vars.TxnCtx.CouldRetry {
		result.operators = append(result.operators, readBillingDemoMutationDiagnostic(dmlKind, readBillingDemoReasonOptimisticReplayPartial))
	}
}

func readBillingDemoMutationDiagnostic(dmlKind, reason string) readBillingDemoOperatorResult {
	return readBillingDemoOperatorResult{
		id:           "stmt_memdb_mutation",
		site:         readBillingDemoSiteTiDB,
		opClass:      readBillingDemoOpClassKVMutation,
		operatorKind: readBillingDemoOperatorMemDBMutation,
		dmlKind:      dmlKind,
		scope:        readBillingDemoScopeStatementAttempted,
		status:       readBillingDemoStatusPartial,
		reason:       reason,
	}
}

func buildWriteBillingDemoResultFromDetails(dmlKind string, commitDetail *tikvutil.CommitDetails, ruv2Metrics *execdetails.RUV2Metrics) readBillingDemoResult {
	return readBillingDemoResult{
		status:    readBillingDemoStatusSuccess,
		reason:    readBillingDemoReasonNone,
		operators: buildTiKVWriteBillingDemoOperators(dmlKind, commitDetail, ruv2Metrics, false),
	}
}

func buildTiKVWriteBillingDemoOperators(dmlKind string, commitDetail *tikvutil.CommitDetails, ruv2Metrics *execdetails.RUV2Metrics, pipelined bool) []readBillingDemoOperatorResult {
	operator := readBillingDemoOperatorResult{
		id:            "commit_txn",
		site:          readBillingDemoSiteTiKV,
		opClass:       readBillingDemoOpClassKVWrite,
		operatorKind:  readBillingDemoOperatorTxnPrewrite,
		dmlKind:       dmlKind,
		scope:         readBillingDemoScopeTxnPrewritePayload,
		emitStatusRow: true,
	}
	// Pipelined transactions allocate CommitDetails, but the logical flush
	// payload is not accumulated into WriteKeys/WriteSize. Do not publish those
	// empty fields as a complete zero payload merely because the detail exists.
	if pipelined {
		operator.status = readBillingDemoStatusPartial
		operator.reason = readBillingDemoReasonPipelinedWritePartial
		return []readBillingDemoOperatorResult{operator}
	}
	if commitDetail == nil {
		operator.status = readBillingDemoStatusPartial
		operator.reason = readBillingDemoReasonMissingCommitDetail
		return []readBillingDemoOperatorResult{operator}
	}

	operator.status = readBillingDemoStatusOperatorOK
	operator.reason = readBillingDemoReasonNone
	writeKeys := int64(commitDetail.WriteKeys)
	writeBytes := int64(commitDetail.WriteSize)
	if writeKeys == 0 && writeBytes == 0 {
		operator.reason = readBillingDemoReasonZeroMutation
	}
	operator.units = append(operator.units,
		readBillingDemoUnit{
			unit:        readBillingDemoUnitWriteKeys,
			source:      readBillingDemoInputSourceCommitDetail,
			side:        readBillingDemoInputSideAll,
			value:       float64(writeKeys),
			widthSource: explainRUWidthSourceNotApplicable,
		},
		readBillingDemoUnit{
			unit:        readBillingDemoUnitWriteByte,
			source:      readBillingDemoInputSourceCommitDetail,
			side:        readBillingDemoInputSideAll,
			value:       float64(writeBytes),
			widthSource: explainRUWidthSourceNotApplicable,
		},
	)
	operators := []readBillingDemoOperatorResult{operator}
	if writeKeys == 0 && writeBytes > 0 {
		operators = append(operators, readBillingDemoWriteDiagnosticStatus(dmlKind, readBillingDemoReasonMissingWriteKeys))
	}
	if writeBytes == 0 && writeKeys > 0 {
		operators = append(operators, readBillingDemoWriteDiagnosticStatus(dmlKind, readBillingDemoReasonMissingWriteByte))
	}
	prewriteRegionNum := int64(atomic.LoadInt32(&commitDetail.PrewriteRegionNum))
	if prewriteRegionNum > 0 {
		operator.units = append(operator.units, readBillingDemoUnit{
			unit:        readBillingDemoUnitPrewriteRegionNum,
			source:      readBillingDemoInputSourceCommitDetail,
			side:        readBillingDemoInputSideAll,
			value:       float64(prewriteRegionNum),
			widthSource: explainRUWidthSourceNotApplicable,
		})
	}
	writeRPCCount := int64(0)
	if ruv2Metrics != nil && !ruv2Metrics.Bypass() {
		writeRPCCount = ruv2Metrics.ResourceManagerWriteCnt()
	}
	if writeRPCCount > 0 {
		operator.units = append(operator.units, readBillingDemoUnit{
			unit:        readBillingDemoUnitTiKVWriteRPCCount,
			source:      readBillingDemoInputSourceRUV2Metrics,
			side:        readBillingDemoInputSideAll,
			value:       float64(writeRPCCount),
			widthSource: explainRUWidthSourceNotApplicable,
		})
	}
	if prewriteRegionNum == 0 {
		operators = append(operators, readBillingDemoWriteDiagnosticStatus(dmlKind, readBillingDemoReasonMissingPrewriteRegion))
	}
	if writeRPCCount == 0 {
		operators = append(operators, readBillingDemoWriteDiagnosticStatus(dmlKind, readBillingDemoReasonMissingWriteRPCCount))
	}
	operators[0] = operator
	return operators
}

func readBillingDemoWriteDiagnosticStatus(dmlKind, reason string) readBillingDemoOperatorResult {
	return readBillingDemoOperatorResult{
		id:           "commit_txn",
		site:         readBillingDemoSiteTiKV,
		opClass:      readBillingDemoOpClassKVWrite,
		operatorKind: readBillingDemoOperatorTxnPrewrite,
		dmlKind:      dmlKind,
		scope:        readBillingDemoScopeTxnPrewritePayload,
		status:       readBillingDemoStatusPartial,
		reason:       reason,
	}
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
	case readBillingDemoUnitOrderWork:
		return weights.orderWork, true
	case readBillingDemoUnitEncodedMutationCount:
		return weights.mutationCount, true
	case readBillingDemoUnitEncodedMutationBytes:
		return weights.mutationByte, true
	case readBillingDemoUnitSetCount:
		return weights.setCount, true
	case readBillingDemoUnitDeleteCount:
		return weights.deleteCount, true
	case readBillingDemoUnitKeyBytes:
		return weights.keyByte, true
	case readBillingDemoUnitValueBytes:
		return weights.valueByte, true
	case readBillingDemoUnitWriteKeys:
		return weights.writeKey, true
	case readBillingDemoUnitWriteByte:
		return weights.writeByte, true
	case readBillingDemoUnitPrewriteRegionNum:
		return weights.region, true
	case readBillingDemoUnitTiKVWriteRPCCount:
		return weights.writeRPC, true
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
		if opStatus != readBillingDemoStatusOperatorOK || !readBillingDemoOperatorBillable(op) {
			continue
		}
		for _, unit := range op.units {
			sample := stmtsummary.ReadBillingDemoBaseUnitSample{
				ModelVersion:   readBillingDemoModelVersion,
				WeightVersion:  readBillingDemoWeightVersion,
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
		stats := runtimeStats.GetCopStats(node.Origin.ID())
		if stats == nil {
			continue
		}
		if tasks := stats.GetTasks(); tasks > component.maxSummaryTasks {
			component.maxSummaryTasks = tasks
		}
		detail := stats.GetScanDetail()
		if detail.ProcessedKeys > 0 && detail.ProcessedKeysSize > 0 {
			component.detailHolderCount++
			component.scanDetail = detail
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
	return op.opClass != readBillingDemoOpClassWrapper && op.opClass != readBillingDemoOpClassSynthetic
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
		orderWork, ok := readBillingDemoOrderingWorkUnit(op, operator.opClass, inputRows)
		if !ok {
			return nil, readBillingDemoReasonInvalidOrderingWork, false
		}
		if orderWork.unit != "" {
			units = append(units, orderWork)
		}
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
		if op.IsRoot {
			// Add after conversion so an extreme OFFSET + COUNT cannot wrap uint64.
			logWidth = max(float64(topN.Offset)+float64(topN.Count), 2)
		} else {
			// Pushdown folds the original OFFSET + COUNT into Count. TiKV's TopN
			// protobuf has only Limit, so a non-zero cop Offset is not executable
			// evidence for the heap bound used here.
			if topN.Offset != 0 {
				return readBillingDemoUnit{}, false
			}
			logWidth = max(float64(topN.Count), 2)
		}
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

func readBillingDemoCopUnits(estimator *readBillingDemoCopEstimator, idx int, operator readBillingDemoOperatorResult) readBillingDemoCopUnitOutcome {
	if failure, ok := estimator.nodeFailures[idx]; ok {
		return readBillingDemoCopUnitOutcome{failure: failure}
	}
	if operator.opClass == readBillingDemoOpClassRangeScan {
		if idx < 0 || idx >= len(estimator.tree) || len(estimator.tree[idx].ChildrenIdx) != 0 {
			return readBillingDemoCopUnitOutcome{failure: readBillingDemoCopFailureAt(idx, readBillingDemoCopFailureCurrent, readBillingDemoStatusUnsupported, readBillingDemoReasonUnsupportedCopStructure)}
		}
		width := estimator.componentOutputWidth(idx)
		if width.failure.present {
			return readBillingDemoCopUnitOutcome{failure: width.failure}
		}
		switch width.widthState {
		case readBillingDemoCopWidthAmbiguous:
			return readBillingDemoCopUnitOutcome{failure: readBillingDemoCopFailureAt(idx, readBillingDemoCopFailureCurrent, readBillingDemoStatusUnknownInput, readBillingDemoReasonAmbiguousCopScanWidth)}
		case readBillingDemoCopWidthMissing:
			return readBillingDemoCopUnitOutcome{failure: readBillingDemoCopFailureAt(idx, readBillingDemoCopFailureCurrent, readBillingDemoStatusUnknownInput, readBillingDemoReasonMissingScanWidthEvidence)}
		case readBillingDemoCopWidthKnown:
		default:
			return readBillingDemoCopUnitOutcome{failure: readBillingDemoCopFailureAt(idx, readBillingDemoCopFailureCurrent, readBillingDemoStatusUnknownInput, readBillingDemoReasonMissingScanWidthEvidence)}
		}
		component := estimator.components[estimator.componentID[idx]]
		scanDetail := component.scanDetail
		scanInputRows, scanInputBytes, ok := readBillingDemoRangeScanInput(scanDetail.TotalKeys, scanDetail.ProcessedKeys, scanDetail.ProcessedKeysSize)
		if !ok {
			return readBillingDemoCopUnitOutcome{failure: readBillingDemoCopFailureAt(idx, readBillingDemoCopFailureCurrent, readBillingDemoStatusUnknownInput, readBillingDemoReasonMissingScanWidthEvidence)}
		}
		units := []readBillingDemoUnit{readBillingDemoFixedEventUnit(readBillingDemoInputSourceScanDetail)}
		units = append(units,
			readBillingDemoUnit{unit: readBillingDemoUnitInputRows, source: readBillingDemoInputSourceScanDetail, side: readBillingDemoInputSideAll, value: float64(scanInputRows), rowWidth: width.avgRowWidth, widthSource: explainRUWidthSourceScanDetailProcessedAvg},
			readBillingDemoUnit{unit: readBillingDemoUnitInputBytes, source: readBillingDemoInputSourceScanDetail, side: readBillingDemoInputSideAll, value: scanInputBytes, rowWidth: width.avgRowWidth, widthSource: explainRUWidthSourceScanDetailProcessedAvg},
		)
		return readBillingDemoCopUnitOutcome{success: true, units: units}
	}

	input := estimator.inputEstimate(idx)
	if input.failure.present {
		return readBillingDemoCopUnitOutcome{failure: input.failure}
	}
	units := []readBillingDemoUnit{readBillingDemoFixedEventUnit(readBillingDemoInputSourceRuntimeChildActRows)}
	units = append(units,
		readBillingDemoUnit{unit: readBillingDemoUnitInputRows, source: input.inputSource, side: readBillingDemoInputSideAll, value: float64(input.rows), rowWidth: input.avgRowWidth, widthSource: input.widthSource},
		readBillingDemoUnit{unit: readBillingDemoUnitInputBytes, source: input.inputSource, side: readBillingDemoInputSideAll, value: input.inputBytes, rowWidth: input.avgRowWidth, widthSource: input.widthSource},
	)
	orderWork, ok := readBillingDemoOrderingWorkUnit(estimator.tree[idx], operator.opClass, input.rows)
	if !ok {
		return readBillingDemoCopUnitOutcome{failure: readBillingDemoCopFailureAt(idx, readBillingDemoCopFailureCurrent, readBillingDemoStatusUnknownInput, readBillingDemoReasonInvalidOrderingWork)}
	}
	if orderWork.unit != "" {
		units = append(units, orderWork)
	}
	return readBillingDemoCopUnitOutcome{success: true, units: units}
}

func readBillingDemoRangeScanInput(totalKeys, processedKeys, processedKeysSize int64) (int64, float64, bool) {
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
		if opStatus != readBillingDemoStatusOperatorOK || !readBillingDemoOperatorBillable(op) {
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
		section:      explainRUSectionSummary,
		component:    "total_preview_ru",
		hasPreviewRU: true,
		source:       explainRUSourceSummaryTotal,
		note:         explainRUReadBillingSummaryNote(snapshotStatus, result),
	}}
	totalPreviewRU := 0.0
	for _, op := range result.operators {
		if op.status != readBillingDemoStatusOperatorOK {
			if op.emitStatusRow {
				rows = append(rows, explainRUReadBillingStatusRow(op))
			}
			continue
		}
		if !readBillingDemoOperatorBillable(op) {
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

func explainRUReadBillingStatusRow(op readBillingDemoOperatorResult) explainRURow {
	row := explainRURow{
		section:       explainRUSectionPlan,
		id:            op.id,
		component:     op.operatorKind,
		operatorClass: op.site + "/" + op.opClass,
		note:          "weight_version=" + readBillingDemoWeightVersion,
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
	note := "weight_version=" + readBillingDemoWeightVersion
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
		note:           "input_side=" + unit.side + ",weight_version=" + readBillingDemoWeightVersion,
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
	case readBillingDemoUnitOrderWork:
		row.workRows = unit.value
		row.hasWorkRows = true
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
	case readBillingDemoUnitSetCount, readBillingDemoUnitDeleteCount, readBillingDemoUnitKeyBytes,
		readBillingDemoUnitValueBytes, readBillingDemoUnitPrewriteRegionNum, readBillingDemoUnitTiKVWriteRPCCount:
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
	metrics.ObserveExplainRURow(row.section, component, operator, row.source, row.rowWidthSource, readBillingDemoWeightVersion, previewRU, workRows, workBytes, rowWidth)
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
