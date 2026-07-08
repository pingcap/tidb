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
	"math"
	"strings"

	executor_metrics "github.com/pingcap/tidb/pkg/executor/metrics"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util/earlystopprofile"
)

const (
	adaptiveLimitScanEventLookup  = "lookup"
	adaptiveLimitScanEventCap     = "cap"
	adaptiveLimitScanEventObserve = "observe"

	adaptiveLimitScanResultAccepted              = "accepted"
	adaptiveLimitScanResultChanged               = "changed"
	adaptiveLimitScanResultCustomScanConcurrency = "skipped_custom_scan_concurrency"
	adaptiveLimitScanResultDisabled              = "disabled"
	adaptiveLimitScanResultHit                   = "hit"
	adaptiveLimitScanResultInvalidKey            = "invalid_key"
	adaptiveLimitScanResultMiss                  = "miss"
	adaptiveLimitScanResultSkippedFailed         = "skipped_failed"
	adaptiveLimitScanResultSkippedInternal       = "skipped_internal"
	adaptiveLimitScanResultSkippedMultiCandidate = "skipped_multi_candidate"
	adaptiveLimitScanResultSkippedNoScanDetail   = "skipped_no_scan_detail"
	adaptiveLimitScanResultSkippedNonSingleScan  = "skipped_non_single_scan"
	adaptiveLimitScanResultSkippedZeroKeys       = "skipped_zero_processed_keys"
	adaptiveLimitScanResultUnchanged             = "unchanged"
)

func keepOrderLimitScanConcurrencyCapForIndexReader(
	b *executorBuilder,
	keepOrder bool,
	plans []base.PhysicalPlan,
) int {
	staticCap := keepOrderLimitScanConcurrencyCapFromPlans(keepOrder, plans)
	if b == nil || b.forDataReaderBuilder {
		return staticCap
	}
	return keepOrderLimitScanConcurrencyCapFromPlansWithProfile(
		b.sctx, earlystopprofile.ReaderTypeIndex, keepOrder, plans)
}

func keepOrderLimitScanConcurrencyCapForIndexLookUpReader(
	b *executorBuilder,
	keepOrder bool,
	pushedLimit *physicalop.PushedDownLimit,
	indexLookUpPushDown bool,
) int {
	staticCap := keepOrderLimitScanConcurrencyCapFromPushedLimit(keepOrder, pushedLimit)
	if b == nil || b.forDataReaderBuilder {
		return staticCap
	}
	readerType := earlystopprofile.ReaderTypeIndexLookup
	if indexLookUpPushDown {
		readerType = earlystopprofile.ReaderTypeIndexLookupPushDown
	}
	return keepOrderLimitScanConcurrencyCapFromPushedLimitWithProfile(
		b.sctx, readerType, keepOrder, pushedLimit)
}

func keepOrderLimitScanConcurrencyCapFromPlansWithProfile(
	sctx sessionctx.Context,
	readerType earlystopprofile.ReaderType,
	keepOrder bool,
	plans []base.PhysicalPlan,
) int {
	limitRows, ok := minLimitRowsFromPlans(plans)
	if !ok {
		return 0
	}
	return keepOrderLimitScanConcurrencyCapWithProfile(sctx, readerType, keepOrder, limitRows)
}

func keepOrderLimitScanConcurrencyCapFromPushedLimitWithProfile(
	sctx sessionctx.Context,
	readerType earlystopprofile.ReaderType,
	keepOrder bool,
	pushedLimit *physicalop.PushedDownLimit,
) int {
	if pushedLimit == nil {
		return 0
	}
	return keepOrderLimitScanConcurrencyCapWithProfile(sctx, readerType, keepOrder, pushedLimit.Offset+pushedLimit.Count)
}

func keepOrderLimitScanConcurrencyCapWithProfile(
	sctx sessionctx.Context,
	readerType earlystopprofile.ReaderType,
	keepOrder bool,
	limitRows uint64,
) int {
	legacyCap := keepOrderLimitScanConcurrencyCap(keepOrder, limitRows)
	if !keepOrder || limitRows == 0 {
		return 0
	}
	if sctx == nil {
		return legacyCap
	}
	sessVars := sctx.GetSessionVars()
	if !sessVars.EnableAdaptiveLimitScan {
		recordAdaptiveLimitScanMetric(adaptiveLimitScanEventLookup, readerType, adaptiveLimitScanResultDisabled)
		return legacyCap
	}
	if sessVars.DistSQLScanConcurrency() != vardef.DefDistSQLScanConcurrency {
		recordAdaptiveLimitScanMetric(adaptiveLimitScanEventLookup, readerType, adaptiveLimitScanResultCustomScanConcurrency)
		return 0
	}
	if !isSingleTiKVScanPlan(sessVars.StmtCtx.GetPlan()) {
		recordAdaptiveLimitScanMetric(adaptiveLimitScanEventLookup, readerType, adaptiveLimitScanResultSkippedNonSingleScan)
		return 0
	}

	key, ok := buildEarlyStopProfileKey(sctx, readerType, keepOrder, limitRows)
	if !ok {
		recordAdaptiveLimitScanMetric(adaptiveLimitScanEventLookup, readerType, adaptiveLimitScanResultInvalidKey)
		return 0
	}

	finalCap := 0
	if profileCap, ok := earlystopprofile.LookupCap(key); ok {
		recordAdaptiveLimitScanMetric(adaptiveLimitScanEventLookup, readerType, adaptiveLimitScanResultHit)
		finalCap = mergeAdaptiveProfileCap(profileCap, vardef.DefDistSQLScanConcurrency)
	} else {
		recordAdaptiveLimitScanMetric(adaptiveLimitScanEventLookup, readerType, adaptiveLimitScanResultMiss)
	}
	capResult := adaptiveLimitScanResultUnchanged
	if finalCap != 0 {
		capResult = adaptiveLimitScanResultChanged
	}
	recordAdaptiveLimitScanMetric(adaptiveLimitScanEventCap, readerType, capResult)
	sctx.GetSessionVars().StmtCtx.AddEarlyStopProfileCandidate(earlystopprofile.Candidate{
		Key:       key,
		LimitRows: limitRows,
		BaseCap:   vardef.DefDistSQLScanConcurrency,
		CapUsed:   candidateCapUsed(finalCap, vardef.DefDistSQLScanConcurrency),
	})
	return finalCap
}

func buildEarlyStopProfileKey(
	sctx sessionctx.Context,
	readerType earlystopprofile.ReaderType,
	keepOrder bool,
	limitRows uint64,
) (earlystopprofile.Key, bool) {
	if !keepOrder || limitRows == 0 || sctx == nil {
		return earlystopprofile.Key{}, false
	}
	stmtCtx := sctx.GetSessionVars().StmtCtx
	_, sqlDigest := stmtCtx.SQLDigest()
	_, planDigest := GetPlanDigest(stmtCtx)
	if sqlDigest == nil || planDigest == nil {
		return earlystopprofile.Key{}, false
	}
	return earlystopprofile.Key{
		SchemaName:  strings.ToLower(sctx.GetSessionVars().CurrentDB),
		SQLDigest:   sqlDigest.String(),
		PlanDigest:  planDigest.String(),
		ReaderType:  readerType,
		KeepOrder:   keepOrder,
		LimitBucket: earlystopprofile.LimitBucketForRows(limitRows),
	}, true
}

func mergeAdaptiveProfileCap(profileCap, defaultConcurrency int) int {
	if profileCap <= 0 {
		return 0
	}
	return max(1, min(profileCap, defaultConcurrency))
}

func candidateCapUsed(finalCap, defaultConcurrency int) int {
	if finalCap > 0 {
		return finalCap
	}
	return defaultConcurrency
}

func adaptiveIndexJoinLimitSettings(
	sctx sessionctx.Context,
	indexJoin *physicalop.PhysicalIndexJoin,
) (batchSize int, concurrency int, ok bool) {
	if sctx == nil || indexJoin == nil {
		return 0, 0, false
	}
	sessVars := sctx.GetSessionVars()
	if !sessVars.EnableAdaptiveLimitScan {
		return 0, 0, false
	}
	limitRows, ok := limitRowsAboveIndexJoin(sessVars.StmtCtx.GetPlan(), indexJoin.ID())
	if !ok {
		return 0, 0, false
	}
	return adaptiveIndexJoinLimitSettingsForRows(
		limitRows,
		sessVars.IndexJoinBatchSize,
		sessVars.IndexLookupJoinConcurrency(),
	)
}

func adaptiveIndexJoinLimitSettingsForRows(
	limitRows uint64,
	sessionBatchSize int,
	sessionConcurrency int,
) (batchSize int, concurrency int, ok bool) {
	if limitRows == 0 || sessionBatchSize <= 0 || sessionConcurrency <= 0 {
		return 0, 0, false
	}
	batchSize = sessionBatchSize
	if limitRows < uint64(sessionBatchSize) {
		batchSize = int(limitRows)
	}
	batchSize = max(1, batchSize)
	batchesNeeded := (limitRows-1)/uint64(batchSize) + 1
	concurrency = sessionConcurrency
	if batchesNeeded < uint64(sessionConcurrency) {
		concurrency = int(batchesNeeded)
	}
	concurrency = max(1, concurrency)
	if batchSize == sessionBatchSize && concurrency == sessionConcurrency {
		return 0, 0, false
	}
	return batchSize, concurrency, true
}

func limitRowsAboveIndexJoin(plan any, targetID int) (uint64, bool) {
	root, ok := plan.(base.PhysicalPlan)
	if !ok || root == nil {
		return 0, false
	}
	return findLimitRowsAboveIndexJoin(root, targetID, 0)
}

func findLimitRowsAboveIndexJoin(plan base.PhysicalPlan, targetID int, activeLimit uint64) (uint64, bool) {
	if plan == nil {
		return 0, false
	}
	if plan.ID() == targetID {
		if indexHashJoin, ok := plan.(*physicalop.PhysicalIndexHashJoin); ok && !indexHashJoin.KeepOuterOrder {
			return 0, false
		}
		return activeLimit, activeLimit > 0
	}
	switch p := plan.(type) {
	case *physicalop.PhysicalLimit:
		limitRows := saturatedAddUint64(p.Offset, p.Count)
		if activeLimit == 0 || limitRows < activeLimit {
			activeLimit = limitRows
		}
	case *physicalop.PhysicalProjection:
	default:
		if activeLimit > 0 {
			return 0, false
		}
	}
	for _, child := range plan.Children() {
		if limitRows, ok := findLimitRowsAboveIndexJoin(child, targetID, activeLimit); ok {
			return limitRows, true
		}
	}
	return 0, false
}

func saturatedAddUint64(a, b uint64) uint64 {
	if math.MaxUint64-a < b {
		return math.MaxUint64
	}
	return a + b
}

func isSingleTiKVScanPlan(plan any) bool {
	root, ok := plan.(base.PhysicalPlan)
	if !ok || root == nil {
		return false
	}
	readerCount := 0
	stack := []base.PhysicalPlan{root}
	for len(stack) > 0 {
		last := len(stack) - 1
		current := stack[last]
		stack = stack[:last]
		if current == nil {
			continue
		}
		switch reader := current.(type) {
		case *physicalop.PhysicalTableReader:
			if reader.StoreType != kv.TiKV {
				return false
			}
			readerCount++
		case *physicalop.PhysicalIndexReader, *physicalop.PhysicalIndexLookUpReader:
			readerCount++
		case *physicalop.PhysicalIndexMergeReader:
			return false
		}
		if readerCount > 1 {
			return false
		}
		stack = append(stack, current.Children()...)
	}
	return readerCount == 1
}

func recordAdaptiveLimitScanMetric(event string, readerType earlystopprofile.ReaderType, result string) {
	executor_metrics.AdaptiveLimitScanCounter.WithLabelValues(event, readerType.String(), result).Inc()
}

func recordAdaptiveLimitScanMetricUnknown(event string, result string) {
	executor_metrics.AdaptiveLimitScanCounter.WithLabelValues(event, "unknown", result).Inc()
}
