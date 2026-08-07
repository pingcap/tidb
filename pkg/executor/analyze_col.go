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

package executor

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"slices"
	"strings"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/distsql"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

const analyzeRegionResolveMaxBackoff = 5000

// AnalyzeColumnsExec represents Analyze columns push down executor.
type AnalyzeColumnsExec struct {
	baseAnalyzeExec

	tableInfo     *model.TableInfo
	colsInfo      []*model.ColumnInfo
	handleCols    plannerutil.HandleCols
	commonHandle  *model.IndexInfo
	resultHandler *tableResultHandler
	indexes       []*model.IndexInfo
	core.AnalyzeInfo

	samplingBuilderWg *notifyErrorWaitGroupWrapper
	samplingMergeWg   *util.WaitGroupWrapper

	schemaForVirtualColEval *expression.Schema
	baseCount               int64
	baseModifyCnt           int64

	// Resolved on the main goroutine; SessionVars.systems is not safe for
	// concurrent lookup across partition workers.
	samplingStatsConcurrency int

	memTracker *memory.Tracker
}

// isColumnCoveredBySingleColUniqueIndex returns true if there exists a public, non-prefix,
// single-column unique index whose only column has the given offset.
func isColumnCoveredBySingleColUniqueIndex(tblInfo *model.TableInfo, colOffset int) bool {
	for _, idx := range tblInfo.Indices {
		if idx.State != model.StatePublic {
			continue
		}
		if isSingleColNonPrefixUniqueIndex(idx) && idx.Columns[0].Offset == colOffset {
			return true
		}
	}
	return false
}

// isSingleColNonPrefixUniqueIndex returns true if the index is public, unique
// (or primary), has exactly one column, and uses neither a prefix nor a
// partial-index condition.
func isSingleColNonPrefixUniqueIndex(idx *model.IndexInfo) bool {
	return idx.State == model.StatePublic &&
		(idx.Unique || idx.Primary) && len(idx.Columns) == 1 &&
		!idx.HasPrefixIndex() && !idx.HasCondition()
}

func (e *AnalyzeColumnsExec) open(ctx context.Context, ranges []*ranger.Range) error {
	e.memTracker = memory.NewTracker(int(e.ctx.GetSessionVars().PlanID.Load()), -1)
	e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)
	e.resultHandler = &tableResultHandler{}
	// Full-sampling analyze restores handle order after collecting samples,
	// so it can scan both sides of the unsigned integer boundary in one request.
	firstPartRanges, secondPartRanges := distsql.SplitRangesAcrossInt64Boundary(ranges, false, false, !hasPkHist(e.handleCols))
	firstResult, err := e.buildResp(ctx, firstPartRanges)
	if err != nil {
		return err
	}
	if len(secondPartRanges) == 0 {
		e.resultHandler.open(nil, firstResult)
		return nil
	}
	var secondResult distsql.SelectResult
	secondResult, err = e.buildResp(ctx, secondPartRanges)
	if err != nil {
		return err
	}
	e.resultHandler.open(firstResult, secondResult)

	return nil
}

func (e *AnalyzeColumnsExec) buildResp(ctx context.Context, ranges []*ranger.Range) (distsql.SelectResult, error) {
	var builder distsql.RequestBuilder
	reqBuilder := builder.SetHandleRangesForTables(e.ctx.GetDistSQLCtx(), []int64{e.TableID.GetStatisticsID()}, e.handleCols != nil && !e.handleCols.IsInt(), ranges)
	builder.SetResourceGroupTagger(e.ctx.GetSessionVars().StmtCtx.GetResourceGroupTagger())
	startTS := uint64(math.MaxUint64)
	isoLevel := kv.RC
	if e.ctx.GetSessionVars().EnableAnalyzeSnapshot {
		startTS = e.snapshot
		isoLevel = kv.SI
	}
	// Full-sampling analyze sorts collected samples by handle before computing
	// correlation, so this request does not need KeepOrder.
	regionCount := countAnalyzeRequestRegions(ctx, e.ctx.GetStore(), reqBuilder.KeyRanges, e.ctx.GetSessionVars().KVVars)
	concurrency, storeBatchSize := analyzeBatchScanBudget(regionCount, e.concurrency)
	kvReq, err := reqBuilder.
		SetAnalyzeRequest(e.analyzePB, isoLevel).
		SetStartTS(startTS).
		SetConcurrency(concurrency).
		SetStoreBatchSize(storeBatchSize).
		SetAllowBatchTaskDataMerge(true).
		SetMemTracker(e.memTracker).
		SetResourceGroupName(e.ctx.GetSessionVars().StmtCtx.ResourceGroupName).
		SetExplicitRequestSourceType(e.ctx.GetSessionVars().ExplicitRequestSourceType).
		Build()
	if err != nil {
		return nil, err
	}
	failpoint.InjectCall("analyzeColumnsRequestBuilt", kvReq)
	result, err := distsql.Analyze(ctx, e.ctx.GetClient(), kvReq, e.ctx.GetSessionVars().KVVars, e.ctx.GetSessionVars().InRestrictedSQL, e.ctx.GetDistSQLCtx())
	if err != nil {
		return nil, err
	}
	return result, nil
}

// countAnalyzeRequestRegions counts distinct Regions, returning zero if they cannot be resolved.
func countAnalyzeRequestRegions(ctx context.Context, store kv.Storage, keyRanges *kv.KeyRanges, vars *tikv.Variables) int {
	tikvStore, ok := store.(tikv.Storage)
	if !ok || keyRanges == nil {
		return 0
	}
	ranges := keyRanges.AppendSelfTo(nil)
	if len(ranges) == 0 {
		return 0
	}
	slices.SortFunc(ranges, func(a, b kv.KeyRange) int {
		if cmp := bytes.Compare(a.StartKey, b.StartKey); cmp != 0 {
			return cmp
		}
		return bytes.Compare(a.EndKey, b.EndKey)
	})
	locateRanges := make([]tikv.KeyRange, len(ranges))
	for i, keyRange := range ranges {
		locateRanges[i] = tikv.KeyRange{StartKey: keyRange.StartKey, EndKey: keyRange.EndKey}
	}
	// Load bucket metadata while populating the shared Region cache because task building reuses cached entries.
	bo := tikv.NewBackofferWithVars(ctx, analyzeRegionResolveMaxBackoff, vars)
	locations, err := tikvStore.GetRegionCache().BatchLocateKeyRanges(bo, locateRanges, tikv.WithNeedBuckets())
	if err != nil {
		statslogutil.StatsLogger().Warn(
			"failed to count regions for analyze batching, falling back to an unbatched request",
			zap.Error(err),
		)
		return 0
	}
	regionIDs := make(map[uint64]struct{}, len(locations))
	for _, location := range locations {
		regionIDs[location.Region.GetID()] = struct{}{}
	}
	return len(regionIDs)
}

const (
	// analyzeBatchMinOuterConcurrency keeps at least four client requests in
	// flight while batching. This limits a transport-level failure or
	// cancellation of one request to at most a quarter of the scan budget.
	analyzeBatchMinOuterConcurrency = 4
	// analyzeBatchMaxRegionsPerRequest limits the deadline and retry exposure
	// of each unary RPC. Sixteen Regions capture most RPC amortization without
	// allowing an unbounded number of Analyze tasks to queue under deadlines
	// that start when TiKV receives the RPC.
	analyzeBatchMaxRegionsPerRequest = 16
)

// analyzeBatchScanBudget disables batching when N <= 2C. Otherwise, it chooses
// B = min(ceil(N/C), C/analyzeBatchMinOuterConcurrency,
// analyzeBatchMaxRegionsPerRequest) and returns concurrency C/B and batch size
// B-1, where N is regionCount and C is scanBudget.
func analyzeBatchScanBudget(regionCount, scanBudget int) (outerConcurrency, storeBatchSize int) {
	scanBudget = max(scanBudget, 1)
	if regionCount-scanBudget <= scanBudget {
		return scanBudget, 0
	}
	batchWidth := (regionCount-1)/scanBudget + 1
	batchWidth = min(batchWidth, scanBudget/analyzeBatchMinOuterConcurrency, analyzeBatchMaxRegionsPerRequest)
	batchWidth = max(batchWidth, 1)
	return scanBudget / batchWidth, batchWidth - 1
}

func hasPkHist(handleCols plannerutil.HandleCols) bool {
	return handleCols != nil && handleCols.IsInt()
}

// prepareColumns prepares the columns for the analyze job.
func prepareColumns(e *AnalyzeColumnsExec, b *strings.Builder) {
	cols := e.colsInfo
	// Ignore the _row_id column.
	if len(cols) > 0 && cols[len(cols)-1].ID == model.ExtraHandleID {
		cols = cols[:len(cols)-1]
	}
	// If there are no columns, skip the process.
	if len(cols) == 0 {
		return
	}

	filteredCols := make([]*model.ColumnInfo, 0, len(cols))
	for _, col := range cols {
		if !col.IsChanging() && !col.IsRemoving() {
			filteredCols = append(filteredCols, col)
		}
	}

	if len(filteredCols) < len(e.tableInfo.GetNonTempColumns()) {
		if len(cols) > 1 {
			b.WriteString(" columns ")
		} else {
			b.WriteString(" column ")
		}
		for i, col := range filteredCols {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(col.Name.O)
		}
	} else {
		b.WriteString(" all columns")
	}
}

// prepareIndexes prepares the indexes for the analyze job.
func prepareIndexes(e *AnalyzeColumnsExec, b *strings.Builder) {
	indexes := e.indexes

	// If there are no indexes, skip the process.
	if len(indexes) == 0 {
		return
	}
	if len(indexes) < len(e.tableInfo.Indices) {
		if len(indexes) > 1 {
			b.WriteString(" indexes ")
		} else {
			b.WriteString(" index ")
		}
		for i, index := range indexes {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(index.Name.O)
		}
	} else {
		b.WriteString(" all indexes")
	}
}

// prepareAnalyzeColumnsJobInfo prepares the job info for the analyze columns job.
func prepareAnalyzeColumnsJobInfo(e *AnalyzeColumnsExec) {
	if e == nil {
		return
	}

	opts := e.opts
	if e.V2Options != nil {
		opts = e.V2Options.FilledOpts
	}
	sampleRate := *e.analyzePB.ColReq.SampleRate
	var b strings.Builder
	// If it is an internal SQL, it means it is triggered by the system itself(auto-analyze).
	if e.ctx.GetSessionVars().InRestrictedSQL {
		b.WriteString("auto ")
	}
	b.WriteString("analyze table")

	prepareIndexes(e, &b)
	if len(e.indexes) > 0 && len(e.colsInfo) > 0 {
		b.WriteString(",")
	}
	prepareColumns(e, &b)

	var needComma bool
	b.WriteString(" with ")
	printOption := func(optType ast.AnalyzeOptionType) {
		if val, ok := opts[optType]; ok {
			if needComma {
				b.WriteString(", ")
			} else {
				needComma = true
			}
			b.WriteString(fmt.Sprintf("%v %s", val, strings.ToLower(ast.AnalyzeOptionString[optType])))
		}
	}
	printOption(ast.AnalyzeOptNumBuckets)
	printOption(ast.AnalyzeOptNumTopN)
	if opts[ast.AnalyzeOptNumSamples] != 0 {
		printOption(ast.AnalyzeOptNumSamples)
	} else {
		if needComma {
			b.WriteString(", ")
		} else {
			needComma = true
		}
		b.WriteString(fmt.Sprintf("%v samplerate", sampleRate))
	}
	e.job.JobInfo = b.String()
}
