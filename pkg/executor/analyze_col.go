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
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/pingcap/tidb/pkg/distsql"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"github.com/tiancaiamao/gp"
)

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

func analyzeColumnsPushDownEntry(gp *gp.Pool, e *AnalyzeColumnsExec) *statistics.AnalyzeResults {
	// TODO: Merge the AnalyzeColumnsExec and AnalyzeColumnsExecV2 together.
	return (&AnalyzeColumnsExecV2{
		AnalyzeColumnsExec: e,
	}).analyzeColumnsPushDownV2(gp)
}

func (e *AnalyzeColumnsExec) open(ranges []*ranger.Range) error {
	e.memTracker = memory.NewTracker(int(e.ctx.GetSessionVars().PlanID.Load()), -1)
	e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)
	e.resultHandler = &tableResultHandler{}
	firstPartRanges, secondPartRanges := distsql.SplitRangesAcrossInt64Boundary(ranges, true, false, !hasPkHist(e.handleCols))
	firstResult, err := e.buildResp(firstPartRanges)
	if err != nil {
		return err
	}
	if len(secondPartRanges) == 0 {
		e.resultHandler.open(nil, firstResult)
		return nil
	}
	var secondResult distsql.SelectResult
	secondResult, err = e.buildResp(secondPartRanges)
	if err != nil {
		return err
	}
	e.resultHandler.open(firstResult, secondResult)

	return nil
}

func (e *AnalyzeColumnsExec) buildResp(ranges []*ranger.Range) (distsql.SelectResult, error) {
	var builder distsql.RequestBuilder
	reqBuilder := builder.SetHandleRangesForTables(e.ctx.GetDistSQLCtx(), []int64{e.TableID.GetStatisticsID()}, e.handleCols != nil && !e.handleCols.IsInt(), ranges)
	builder.SetResourceGroupTagger(e.ctx.GetSessionVars().StmtCtx.GetResourceGroupTagger())
	startTS := uint64(math.MaxUint64)
	isoLevel := kv.RC
	if e.ctx.GetSessionVars().EnableAnalyzeSnapshot {
		startTS = e.snapshot
		isoLevel = kv.SI
	}
	// Always set KeepOrder of the request to be true, in order to compute
	// correct `correlation` of columns.
	kvReq, err := reqBuilder.
		SetAnalyzeRequest(e.analyzePB, isoLevel).
		SetStartTS(startTS).
		SetKeepOrder(true).
		SetConcurrency(e.concurrency).
		SetMemTracker(e.memTracker).
		SetResourceGroupName(e.ctx.GetSessionVars().StmtCtx.ResourceGroupName).
		SetExplicitRequestSourceType(e.ctx.GetSessionVars().ExplicitRequestSourceType).
		Build()
	if err != nil {
		return nil, err
	}
	ctx := context.TODO()
	result, err := distsql.Analyze(ctx, e.ctx.GetClient(), kvReq, e.ctx.GetSessionVars().KVVars, e.ctx.GetSessionVars().InRestrictedSQL, e.ctx.GetDistSQLCtx())
	if err != nil {
		return nil, err
	}
	return result, nil
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

// prepareV2AnalyzeJobInfo prepares the job info for the analyze job.
func prepareV2AnalyzeJobInfo(e *AnalyzeColumnsExec) {
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
