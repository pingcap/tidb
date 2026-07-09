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

	exec "github.com/pingcap/tidb/pkg/executor/internal/exec"
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
	adaptiveLimitScanResultSkippedNoRowsSignal   = "skipped_no_rows_signal"
	adaptiveLimitScanResultSkippedNonSingleScan  = "skipped_non_single_scan"
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
	return buildEarlyStopProfileKeyWithSingleScanCheck(sctx, readerType, keepOrder, limitRows, true)
}

func buildEarlyStopProfileKeyWithSingleScanCheck(
	sctx sessionctx.Context,
	readerType earlystopprofile.ReaderType,
	keepOrder bool,
	limitRows uint64,
	requireSingleTiKVScan bool,
) (earlystopprofile.Key, bool) {
	if !keepOrder || limitRows == 0 || sctx == nil {
		return earlystopprofile.Key{}, false
	}
	if requireSingleTiKVScan && !isSingleTiKVScanPlan(sctx.GetSessionVars().StmtCtx.GetPlan()) {
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
) (adaptiveIndexJoinLimitSettingsResult, bool) {
	if sctx == nil || indexJoin == nil {
		return adaptiveIndexJoinLimitSettingsResult{}, false
	}
	sessVars := sctx.GetSessionVars()
	if !sessVars.EnableAdaptiveLimitScan {
		return adaptiveIndexJoinLimitSettingsResult{}, false
	}
	limitRows, ok := limitRowsFromIndexJoinOuterProp(indexJoin)
	if !ok {
		return adaptiveIndexJoinLimitSettingsResult{}, false
	}
	settings := adaptiveIndexJoinLimitSettingsForRows(
		limitRows,
		sessVars.IndexJoinBatchSize,
		sessVars.IndexLookupJoinConcurrency(),
	)
	settings.LimitRows = limitRows
	settings.LookupBatchSize = adaptiveIndexJoinLookupBatchSize(limitRows, sessVars.IndexLookupSize)
	settings.LookupConcurrency = adaptiveIndexJoinLookupConcurrency(sessVars.IndexLookupConcurrency())
	if settings.LookupConcurrency > 0 {
		settings.LookupResultChSize = 2
	}
	if settings.Concurrency > 0 {
		settings.ScanConcurrencyCap = settings.Concurrency
	}
	key, hasKey := buildEarlyStopProfileKeyWithSingleScanCheck(
		sctx, earlystopprofile.ReaderTypeIndexJoin, true, limitRows, false)
	if hasKey {
		settings.ProfileKey = key
		settings.HasProfileKey = true
		settings.ProfileBaseCap = vardef.DefDistSQLScanConcurrency
		settings.ProfileCapUsed = vardef.DefDistSQLScanConcurrency
		if profileCap, ok := earlystopprofile.LookupCap(key); ok {
			recordAdaptiveLimitScanMetric(adaptiveLimitScanEventLookup, earlystopprofile.ReaderTypeIndexJoin, adaptiveLimitScanResultHit)
			profileCap = mergeAdaptiveProfileCap(profileCap, vardef.DefDistSQLScanConcurrency)
			settings.ProfileCapUsed = candidateCapUsed(profileCap, vardef.DefDistSQLScanConcurrency)
			if profileCap > 0 {
				if settings.Concurrency == 0 || settings.Concurrency > profileCap {
					settings.Concurrency = profileCap
				}
				if settings.ScanConcurrencyCap == 0 || settings.ScanConcurrencyCap > profileCap {
					settings.ScanConcurrencyCap = profileCap
				}
			}
		} else {
			recordAdaptiveLimitScanMetric(adaptiveLimitScanEventLookup, earlystopprofile.ReaderTypeIndexJoin, adaptiveLimitScanResultMiss)
		}
	} else {
		recordAdaptiveLimitScanMetric(adaptiveLimitScanEventLookup, earlystopprofile.ReaderTypeIndexJoin, adaptiveLimitScanResultInvalidKey)
	}
	if settings.HasProfileKey && settings.ScanConcurrencyCap > 0 {
		settings.ProfileCapUsed = candidateCapUsed(settings.ScanConcurrencyCap, vardef.DefDistSQLScanConcurrency)
	}
	if !settings.Changed() {
		return settings, settings.HasProfileKey
	}
	return settings, true
}

type adaptiveIndexJoinLimitSettingsResult struct {
	LimitRows uint64

	BatchSize          int
	Concurrency        int
	ScanConcurrencyCap int

	LookupBatchSize    int
	LookupConcurrency  int
	LookupResultChSize int

	ProfileKey     earlystopprofile.Key
	HasProfileKey  bool
	ProfileBaseCap int
	ProfileCapUsed int
}

func (s adaptiveIndexJoinLimitSettingsResult) Changed() bool {
	return s.BatchSize > 0 ||
		s.Concurrency > 0 ||
		s.ScanConcurrencyCap > 0 ||
		s.LookupBatchSize > 0 ||
		s.LookupConcurrency > 0 ||
		s.LookupResultChSize > 0
}

func adaptiveIndexJoinLimitSettingsForRows(
	limitRows uint64,
	sessionBatchSize int,
	sessionConcurrency int,
) adaptiveIndexJoinLimitSettingsResult {
	if limitRows == 0 || sessionBatchSize <= 0 || sessionConcurrency <= 0 {
		return adaptiveIndexJoinLimitSettingsResult{}
	}
	settings := adaptiveIndexJoinLimitSettingsResult{}
	batchSize := sessionBatchSize
	if limitRows < uint64(sessionBatchSize) {
		batchSize = int(limitRows)
	}
	batchSize = max(1, batchSize)
	batchesNeeded := (limitRows-1)/uint64(batchSize) + 1
	concurrency := sessionConcurrency
	if batchesNeeded < uint64(sessionConcurrency) {
		concurrency = int(batchesNeeded)
	}
	concurrency = max(1, concurrency)
	if batchSize != sessionBatchSize {
		settings.BatchSize = batchSize
	}
	if concurrency != sessionConcurrency {
		settings.Concurrency = concurrency
	}
	return settings
}

func adaptiveIndexJoinLookupBatchSize(limitRows uint64, sessionLookupSize int) int {
	if limitRows == 0 || sessionLookupSize <= 0 || limitRows >= uint64(sessionLookupSize) {
		return 0
	}
	return max(1, int(limitRows))
}

func adaptiveIndexJoinLookupConcurrency(sessionConcurrency int) int {
	const adaptiveLookupConcurrency = 2
	if sessionConcurrency <= adaptiveLookupConcurrency {
		return 0
	}
	return adaptiveLookupConcurrency
}

func applyAdaptiveIndexJoinLimitSettingsToLookup(root exec.Executor, settings adaptiveIndexJoinLimitSettingsResult) *IndexLookUpExecutor {
	lookup := findIndexLookUpExecutorForAdaptiveIndexJoin(root)
	if lookup == nil {
		return nil
	}
	if settings.ScanConcurrencyCap > 0 {
		lookup.keepOrderLimitScanConcurrencyCap = settings.ScanConcurrencyCap
	}
	if settings.LookupBatchSize > 0 {
		lookup.adaptiveLimitLookupBatchSize = settings.LookupBatchSize
	}
	if settings.LookupConcurrency > 0 {
		lookup.adaptiveLimitLookupConcurrency = settings.LookupConcurrency
	}
	if settings.LookupResultChSize > 0 {
		lookup.adaptiveLimitResultChSize = settings.LookupResultChSize
	}
	return lookup
}

func findIndexLookUpExecutorForAdaptiveIndexJoin(root exec.Executor) *IndexLookUpExecutor {
	for current := root; current != nil; {
		switch e := current.(type) {
		case *IndexLookUpExecutor:
			return e
		case *ProjectionExec:
			if e.ChildrenLen() != 1 {
				return nil
			}
			current = e.Children(0)
		default:
			return nil
		}
	}
	return nil
}

func limitRowsFromIndexJoinOuterProp(indexJoin *physicalop.PhysicalIndexJoin) (uint64, bool) {
	if indexJoin == nil {
		return 0, false
	}
	outerIdx := 1 - indexJoin.InnerChildIdx
	if outerIdx < 0 {
		return 0, false
	}
	if outerIdx >= len(indexJoin.Children()) {
		return 0, false
	}
	prop := indexJoin.GetChildReqProps(outerIdx)
	if prop == nil || !prop.NeedKeepOrder() {
		return 0, false
	}
	if prop.ExpectedCnt <= 0 || math.IsInf(prop.ExpectedCnt, 0) || prop.ExpectedCnt >= float64(math.MaxUint64) {
		return 0, false
	}
	limitRows := uint64(math.Ceil(prop.ExpectedCnt))
	if limitRows == 0 {
		return 0, false
	}
	return limitRows, true
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
