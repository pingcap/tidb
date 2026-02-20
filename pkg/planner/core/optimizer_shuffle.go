// Copyright 2022 PingCAP, Inc.
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
	"strconv"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/diagnosticspb"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tipb/go-tipb"
)

// Only for MPP(Window<-[Sort]<-ExchangeReceiver<-ExchangeSender).
// TiFlashFineGrainedShuffleStreamCount:
// < 0: fine grained shuffle is disabled.
// > 0: use TiFlashFineGrainedShuffleStreamCount as stream count.
// == 0: use TiFlashMaxThreads as stream count when it's greater than 0. Otherwise set status as uninitialized.
func handleFineGrainedShuffle(ctx context.Context, sctx base.PlanContext, plan base.PhysicalPlan) {
	streamCount := sctx.GetSessionVars().TiFlashFineGrainedShuffleStreamCount
	if streamCount < 0 {
		return
	}
	if streamCount == 0 {
		if sctx.GetSessionVars().TiFlashMaxThreads > 0 {
			streamCount = sctx.GetSessionVars().TiFlashMaxThreads
		}
	}
	// use two separate cluster info to avoid grpc calls cost
	tiflashServerCountInfo := tiflashClusterInfo{unInitialized, 0}
	streamCountInfo := tiflashClusterInfo{unInitialized, 0}
	if streamCount != 0 {
		streamCountInfo.itemStatus = initialized
		streamCountInfo.itemValue = uint64(streamCount)
	}
	setupFineGrainedShuffle(ctx, sctx, &streamCountInfo, &tiflashServerCountInfo, plan)
}

func setupFineGrainedShuffle(ctx context.Context, sctx base.PlanContext, streamCountInfo *tiflashClusterInfo, tiflashServerCountInfo *tiflashClusterInfo, plan base.PhysicalPlan) {
	if tableReader, ok := plan.(*physicalop.PhysicalTableReader); ok {
		if _, isExchangeSender := tableReader.TablePlan.(*physicalop.PhysicalExchangeSender); isExchangeSender {
			helper := fineGrainedShuffleHelper{shuffleTarget: unknown, plans: make([]*physicalop.BasePhysicalPlan, 1)}
			setupFineGrainedShuffleInternal(ctx, sctx, tableReader.TablePlan, &helper, streamCountInfo, tiflashServerCountInfo)
		}
	} else {
		for _, child := range plan.Children() {
			setupFineGrainedShuffle(ctx, sctx, streamCountInfo, tiflashServerCountInfo, child)
		}
	}
}

type shuffleTarget uint8

const (
	unknown shuffleTarget = iota
	window
	joinBuild
	hashAgg
)

type fineGrainedShuffleHelper struct {
	shuffleTarget shuffleTarget
	plans         []*physicalop.BasePhysicalPlan
	joinKeys      []*expression.Column
}

type tiflashClusterInfoStatus uint8

const (
	unInitialized tiflashClusterInfoStatus = iota
	initialized
	failed
)

type tiflashClusterInfo struct {
	itemStatus tiflashClusterInfoStatus
	itemValue  uint64
}

func (h *fineGrainedShuffleHelper) clear() {
	h.shuffleTarget = unknown
	h.plans = h.plans[:0]
	h.joinKeys = nil
}

func (h *fineGrainedShuffleHelper) updateTarget(t shuffleTarget, p *physicalop.BasePhysicalPlan) {
	h.shuffleTarget = t
	h.plans = append(h.plans, p)
}

func getTiFlashServerMinLogicalCores(ctx context.Context, sctx base.PlanContext, serversInfo []infoschema.ServerInfo) (bool, uint64) {
	failpoint.Inject("mockTiFlashStreamCountUsingMinLogicalCores", func(val failpoint.Value) {
		intVal, err := strconv.Atoi(val.(string))
		if err == nil {
			failpoint.Return(true, uint64(intVal))
		} else {
			failpoint.Return(false, 0)
		}
	})
	defer func(begin time.Time) {
		// if there are any network jitters, this could take a long time.
		sctx.GetSessionVars().DurationOptimizer.TiFlashInfoFetch = time.Since(begin)
	}(time.Now())
	rows, err := infoschema.FetchClusterServerInfoWithoutPrivilegeCheck(ctx, sctx.GetSessionVars(), serversInfo, diagnosticspb.ServerInfoType_HardwareInfo, false)
	if err != nil {
		return false, 0
	}
	var minLogicalCores = initialMaxCores // set to a large enough value here
	for _, row := range rows {
		if row[4].GetString() == "cpu-logical-cores" {
			logicalCpus, err := strconv.Atoi(row[5].GetString())
			if err == nil && logicalCpus > 0 {
				minLogicalCores = min(minLogicalCores, uint64(logicalCpus))
			}
		}
	}
	// No need to check len(serersInfo) == serverCount here, since missing some servers' info won't affect the correctness
	return true, minLogicalCores
}

// calculateTiFlashStreamCountUsingMinLogicalCores uses minimal logical cpu cores among tiflash servers
// return false, 0 if any err happens
func calculateTiFlashStreamCountUsingMinLogicalCores(ctx context.Context, sctx base.PlanContext, serversInfo []infoschema.ServerInfo) (bool, uint64) {
	valid, minLogicalCores := getTiFlashServerMinLogicalCores(ctx, sctx, serversInfo)
	if !valid {
		return false, 0
	}
	if minLogicalCores != initialMaxCores {
		// use logical core number as the stream count, the same as TiFlash's default max_threads: https://github.com/pingcap/tiflash/blob/v7.5.0/dbms/src/Interpreters/SettingsCommon.h#L166
		return true, minLogicalCores
	}

	return false, 0
}

func checkFineGrainedShuffleForJoinAgg(ctx context.Context, sctx base.PlanContext, streamCountInfo *tiflashClusterInfo, tiflashServerCountInfo *tiflashClusterInfo, exchangeColCount int, splitLimit uint64) (applyFlag bool, streamCount uint64) {
	switch (*streamCountInfo).itemStatus {
	case unInitialized:
		streamCount = 4 // assume 8c node in cluster as minimal, stream count is 8 / 2 = 4
	case initialized:
		streamCount = (*streamCountInfo).itemValue
	case failed:
		return false, 0 // probably won't reach this path
	}

	var tiflashServerCount uint64
	switch (*tiflashServerCountInfo).itemStatus {
	case unInitialized:
		serversInfo, err := infoschema.GetTiFlashServerInfo(sctx.GetStore())
		if err != nil {
			(*tiflashServerCountInfo).itemStatus = failed
			(*tiflashServerCountInfo).itemValue = 0
			if (*streamCountInfo).itemStatus == unInitialized {
				setDefaultStreamCount(streamCountInfo)
			}
			return false, 0
		}
		tiflashServerCount = uint64(len(serversInfo))
		(*tiflashServerCountInfo).itemStatus = initialized
		(*tiflashServerCountInfo).itemValue = tiflashServerCount
	case initialized:
		tiflashServerCount = (*tiflashServerCountInfo).itemValue
	case failed:
		return false, 0
	}

	// if already exceeds splitLimit, no need to fetch actual logical cores
	if tiflashServerCount*uint64(exchangeColCount)*streamCount > splitLimit {
		return false, 0
	}

	// if streamCount already initialized, and can pass splitLimit check
	if (*streamCountInfo).itemStatus == initialized {
		return true, streamCount
	}

	serversInfo, err := infoschema.GetTiFlashServerInfo(sctx.GetStore())
	if err != nil {
		(*tiflashServerCountInfo).itemStatus = failed
		(*tiflashServerCountInfo).itemValue = 0
		return false, 0
	}
	flag, temStreamCount := calculateTiFlashStreamCountUsingMinLogicalCores(ctx, sctx, serversInfo)
	if !flag {
		setDefaultStreamCount(streamCountInfo)
		(*tiflashServerCountInfo).itemStatus = failed
		return false, 0
	}
	streamCount = temStreamCount
	(*streamCountInfo).itemStatus = initialized
	(*streamCountInfo).itemValue = streamCount
	applyFlag = tiflashServerCount*uint64(exchangeColCount)*streamCount <= splitLimit
	return applyFlag, streamCount
}

func inferFineGrainedShuffleStreamCountForWindow(ctx context.Context, sctx base.PlanContext, streamCountInfo *tiflashClusterInfo, tiflashServerCountInfo *tiflashClusterInfo) (streamCount uint64) {
	switch (*streamCountInfo).itemStatus {
	case unInitialized:
		if (*tiflashServerCountInfo).itemStatus == failed {
			setDefaultStreamCount(streamCountInfo)
			streamCount = (*streamCountInfo).itemValue
			break
		}

		serversInfo, err := infoschema.GetTiFlashServerInfo(sctx.GetStore())
		if err != nil {
			setDefaultStreamCount(streamCountInfo)
			streamCount = (*streamCountInfo).itemValue
			(*tiflashServerCountInfo).itemStatus = failed
			break
		}

		if (*tiflashServerCountInfo).itemStatus == unInitialized {
			(*tiflashServerCountInfo).itemStatus = initialized
			(*tiflashServerCountInfo).itemValue = uint64(len(serversInfo))
		}

		flag, temStreamCount := calculateTiFlashStreamCountUsingMinLogicalCores(ctx, sctx, serversInfo)
		if !flag {
			setDefaultStreamCount(streamCountInfo)
			streamCount = (*streamCountInfo).itemValue
			(*tiflashServerCountInfo).itemStatus = failed
			break
		}
		streamCount = temStreamCount
		(*streamCountInfo).itemStatus = initialized
		(*streamCountInfo).itemValue = streamCount
	case initialized:
		streamCount = (*streamCountInfo).itemValue
	case failed:
		setDefaultStreamCount(streamCountInfo)
		streamCount = (*streamCountInfo).itemValue
	}
	return streamCount
}

func setDefaultStreamCount(streamCountInfo *tiflashClusterInfo) {
	(*streamCountInfo).itemStatus = initialized
	(*streamCountInfo).itemValue = vardef.DefStreamCountWhenMaxThreadsNotSet
}

func setupFineGrainedShuffleInternal(ctx context.Context, sctx base.PlanContext, plan base.PhysicalPlan, helper *fineGrainedShuffleHelper, streamCountInfo *tiflashClusterInfo, tiflashServerCountInfo *tiflashClusterInfo) {
	switch x := plan.(type) {
	case *physicalop.PhysicalWindow:
		// Do not clear the plans because window executor will keep the data partition.
		// For non hash partition window function, there will be a passthrough ExchangeSender to collect data,
		// which will break data partition.
		helper.updateTarget(window, &x.BasePhysicalPlan)
		setupFineGrainedShuffleInternal(ctx, sctx, x.Children()[0], helper, streamCountInfo, tiflashServerCountInfo)
	case *physicalop.PhysicalSort:
		if x.IsPartialSort {
			// Partial sort will keep the data partition.
			helper.plans = append(helper.plans, &x.BasePhysicalPlan)
		} else {
			// Global sort will break the data partition.
			helper.clear()
		}
		setupFineGrainedShuffleInternal(ctx, sctx, x.Children()[0], helper, streamCountInfo, tiflashServerCountInfo)
	case *physicalop.PhysicalSelection:
		helper.plans = append(helper.plans, &x.BasePhysicalPlan)
		setupFineGrainedShuffleInternal(ctx, sctx, x.Children()[0], helper, streamCountInfo, tiflashServerCountInfo)
	case *physicalop.PhysicalProjection:
		helper.plans = append(helper.plans, &x.BasePhysicalPlan)
		setupFineGrainedShuffleInternal(ctx, sctx, x.Children()[0], helper, streamCountInfo, tiflashServerCountInfo)
	case *physicalop.PhysicalExchangeReceiver:
		helper.plans = append(helper.plans, &x.BasePhysicalPlan)
		setupFineGrainedShuffleInternal(ctx, sctx, x.Children()[0], helper, streamCountInfo, tiflashServerCountInfo)
	case *physicalop.PhysicalHashAgg:
		// Todo: allow hash aggregation's output still benefits from fine grained shuffle
		aggHelper := fineGrainedShuffleHelper{shuffleTarget: hashAgg, plans: []*physicalop.BasePhysicalPlan{}}
		aggHelper.plans = append(aggHelper.plans, &x.BasePhysicalPlan)
		setupFineGrainedShuffleInternal(ctx, sctx, x.Children()[0], &aggHelper, streamCountInfo, tiflashServerCountInfo)
	case *physicalop.PhysicalHashJoin:
		child0 := x.Children()[0]
		child1 := x.Children()[1]
		buildChild := child0
		probChild := child1
		joinKeys := x.LeftJoinKeys
		if x.InnerChildIdx != 0 {
			// Child1 is build side.
			buildChild = child1
			joinKeys = x.RightJoinKeys
			probChild = child0
		}

		if len(joinKeys) > 0 && !x.CanTiFlashUseHashJoinV2(sctx) { // Not cross join and can not use hash join v2 in tiflash
			buildHelper := fineGrainedShuffleHelper{shuffleTarget: joinBuild, plans: []*physicalop.BasePhysicalPlan{}}
			buildHelper.plans = append(buildHelper.plans, &x.BasePhysicalPlan)
			buildHelper.joinKeys = joinKeys
			setupFineGrainedShuffleInternal(ctx, sctx, buildChild, &buildHelper, streamCountInfo, tiflashServerCountInfo)
		} else {
			buildHelper := fineGrainedShuffleHelper{shuffleTarget: unknown, plans: []*physicalop.BasePhysicalPlan{}}
			setupFineGrainedShuffleInternal(ctx, sctx, buildChild, &buildHelper, streamCountInfo, tiflashServerCountInfo)
		}
		// don't apply fine grained shuffle for probe side
		helper.clear()
		setupFineGrainedShuffleInternal(ctx, sctx, probChild, helper, streamCountInfo, tiflashServerCountInfo)
	case *physicalop.PhysicalExchangeSender:
		if x.ExchangeType == tipb.ExchangeType_Hash {
			// Set up stream count for all plans based on shuffle target type.
			var exchangeColCount = x.Schema().Len()
			switch helper.shuffleTarget {
			case window:
				streamCount := inferFineGrainedShuffleStreamCountForWindow(ctx, sctx, streamCountInfo, tiflashServerCountInfo)
				x.TiFlashFineGrainedShuffleStreamCount = streamCount
				for _, p := range helper.plans {
					p.TiFlashFineGrainedShuffleStreamCount = streamCount
				}
			case hashAgg:
				applyFlag, streamCount := checkFineGrainedShuffleForJoinAgg(ctx, sctx, streamCountInfo, tiflashServerCountInfo, exchangeColCount, 1200) // 1200: performance test result
				if applyFlag {
					x.TiFlashFineGrainedShuffleStreamCount = streamCount
					for _, p := range helper.plans {
						p.TiFlashFineGrainedShuffleStreamCount = streamCount
					}
				}
			case joinBuild:
				// Support hashJoin only when shuffle hash keys equals to join keys due to tiflash implementations
				if len(x.HashCols) != len(helper.joinKeys) {
					break
				}
				// Check the shuffle key should be equal to joinKey, otherwise the shuffle hash code may not be equal to
				// actual join hash code due to type cast
				applyFlag := true
				for i, joinKey := range helper.joinKeys {
					if !x.HashCols[i].Col.EqualColumn(joinKey) {
						applyFlag = false
						break
					}
				}
				if !applyFlag {
					break
				}
				applyFlag, streamCount := checkFineGrainedShuffleForJoinAgg(ctx, sctx, streamCountInfo, tiflashServerCountInfo, exchangeColCount, 600) // 600: performance test result
				if applyFlag {
					x.TiFlashFineGrainedShuffleStreamCount = streamCount
					for _, p := range helper.plans {
						p.TiFlashFineGrainedShuffleStreamCount = streamCount
					}
				}
			}
		}
		// exchange sender will break the data partition.
		helper.clear()
		setupFineGrainedShuffleInternal(ctx, sctx, x.Children()[0], helper, streamCountInfo, tiflashServerCountInfo)
	default:
		for _, child := range x.Children() {
			childHelper := fineGrainedShuffleHelper{shuffleTarget: unknown, plans: []*physicalop.BasePhysicalPlan{}}
			setupFineGrainedShuffleInternal(ctx, sctx, child, &childHelper, streamCountInfo, tiflashServerCountInfo)
		}
	}
}
