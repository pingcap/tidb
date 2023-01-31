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

	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/set"
	"github.com/pingcap/tidb/util/stmtsummary"
	stmtsummaryv2 "github.com/pingcap/tidb/util/stmtsummary/v2"
)

func buildStmtSummaryRetriever(
	ctx sessionctx.Context,
	table *model.TableInfo,
	columns []*model.ColumnInfo,
	extractor *plannercore.StatementsSummaryExtractor,
) memTableRetriever {
	if ctx.GetSessionVars().StmtSummary.EnablePersistent {
		return &stmtSummaryRetrieverV2{
			stmtSummary: ctx.GetSessionVars().StmtSummary.StmtSummaryV2,
			table:       table,
			columns:     columns,
			extractor:   extractor,
		}
	}
	return &stmtSummaryRetriever{
		table:     table,
		columns:   columns,
		extractor: extractor,
	}
}

// stmtSummaryRetriever is used to retrieve statements summary.
type stmtSummaryRetriever struct {
	dummyCloser
	table     *model.TableInfo
	columns   []*model.ColumnInfo
	extractor *plannercore.StatementsSummaryExtractor
	retrieved bool

	// only for evicted
	initiliazed bool
	rowIdx      int
	rows        [][]types.Datum
}

func (e *stmtSummaryRetriever) retrieve(ctx context.Context, sctx sessionctx.Context) ([][]types.Datum, error) {
	if (e.extractor != nil && e.extractor.SkipRequest) || e.retrieved {
		return nil, nil
	}
	switch e.table.Name.O {
	case infoschema.TableStatementsSummary,
		infoschema.ClusterTableStatementsSummary:
		return e.retrieveCurrent(sctx)
	case infoschema.TableStatementsSummaryHistory,
		infoschema.ClusterTableStatementsSummaryHistory:
		return e.retrieveHistory(sctx)
	case infoschema.TableStatementsSummaryEvicted,
		infoschema.ClusterTableStatementsSummaryEvicted:
		return e.retrieveEvicted(sctx)
	}
	return nil, nil
}

func (e *stmtSummaryRetriever) retrieveCurrent(sctx sessionctx.Context) ([][]types.Datum, error) {
	e.retrieved = true
	var err error
	var instanceAddr string
	if e.table.Name.O == infoschema.ClusterTableStatementsSummary {
		instanceAddr, err = infoschema.GetInstanceAddr(sctx)
		if err != nil {
			return nil, err
		}
	}
	user := sctx.GetSessionVars().User
	reader := stmtsummary.NewStmtSummaryReader(user, hasPriv(sctx, mysql.ProcessPriv), e.columns, instanceAddr, sctx.GetSessionVars().StmtCtx.TimeZone)
	if e.extractor != nil && e.extractor.Enable {
		checker := stmtsummary.NewStmtSummaryChecker(e.extractor.Digests)
		reader.SetChecker(checker)
	}
	return reader.GetStmtSummaryCurrentRows(), nil
}

func (e *stmtSummaryRetriever) retrieveHistory(sctx sessionctx.Context) ([][]types.Datum, error) {
	e.retrieved = true
	var err error
	var instanceAddr string
	if e.table.Name.O == infoschema.ClusterTableStatementsSummaryHistory {
		instanceAddr, err = infoschema.GetInstanceAddr(sctx)
		if err != nil {
			return nil, err
		}
	}
	user := sctx.GetSessionVars().User
	reader := stmtsummary.NewStmtSummaryReader(user, hasPriv(sctx, mysql.ProcessPriv), e.columns, instanceAddr, sctx.GetSessionVars().StmtCtx.TimeZone)
	if e.extractor != nil && e.extractor.Enable {
		checker := stmtsummary.NewStmtSummaryChecker(e.extractor.Digests)
		reader.SetChecker(checker)
	}
	return reader.GetStmtSummaryHistoryRows(), nil
}

func (e *stmtSummaryRetriever) retrieveEvicted(sctx sessionctx.Context) ([][]types.Datum, error) {
	if !e.initiliazed {
		e.initiliazed = true
		if !hasPriv(sctx, mysql.ProcessPriv) {
			return nil, plannercore.ErrSpecificAccessDenied.GenWithStackByArgs("PROCESS")
		}
		e.rows = stmtsummary.StmtSummaryByDigestMap.ToEvictedCountDatum()
		if e.table.Name.O == infoschema.ClusterTableStatementsSummaryEvicted {
			var err error
			e.rows, err = infoschema.AppendHostInfoToRows(sctx, e.rows)
			if err != nil {
				return nil, err
			}
		}
	}
	maxCount := 1024
	retCount := maxCount
	if e.rowIdx+maxCount > len(e.rows) {
		retCount = len(e.rows) - e.rowIdx
		e.retrieved = true
	}
	ret := make([][]types.Datum, retCount)
	for i := e.rowIdx; i < e.rowIdx+retCount; i++ {
		ret[i-e.rowIdx] = e.rows[i]
	}
	e.rowIdx += retCount
	return adjustColumns(ret, e.columns, e.table), nil
}

// stmtSummaryRetriever is used to retrieve statements summary when
// config.GetGlobalConfig().Instance.StmtSummaryEnablePersistent is true
type stmtSummaryRetrieverV2 struct {
	stmtSummary *stmtsummaryv2.StmtSummary
	table       *model.TableInfo
	columns     []*model.ColumnInfo
	extractor   *plannercore.StatementsSummaryExtractor

	retrieved     bool
	memReader     *stmtsummaryv2.MemReader
	historyReader *stmtsummaryv2.HistoryReader
}

func (r *stmtSummaryRetrieverV2) retrieve(ctx context.Context, sctx sessionctx.Context) ([][]types.Datum, error) {
	if r.extractor != nil && r.extractor.SkipRequest {
		return nil, nil
	}
	switch r.table.Name.O {
	case infoschema.TableStatementsSummary,
		infoschema.ClusterTableStatementsSummary:
		return r.retrieveCurrent(sctx)
	case infoschema.TableStatementsSummaryHistory,
		infoschema.ClusterTableStatementsSummaryHistory:
		return r.retrieveHistory(ctx, sctx)
	case infoschema.TableStatementsSummaryEvicted,
		infoschema.ClusterTableStatementsSummaryEvicted:
		return r.retrieveEvicted(sctx)
	}
	return nil, nil
}

func (r *stmtSummaryRetrieverV2) retrieveCurrent(sctx sessionctx.Context) ([][]types.Datum, error) {
	if r.retrieved {
		return nil, nil
	}
	r.retrieved = true
	if r.memReader == nil {
		var err error
		var instanceAddr string
		if r.table.Name.O == infoschema.ClusterTableStatementsSummary {
			instanceAddr, err = infoschema.GetInstanceAddr(sctx)
			if err != nil {
				return nil, err
			}
		}
		timeLocation := sctx.GetSessionVars().TimeZone
		user := sctx.GetSessionVars().User
		hasProcessPriv := hasPriv(sctx, mysql.ProcessPriv)
		digests := r.getDigestsFromExtractor()
		timeRanges := r.getTimeRangesFromExtractor()
		r.memReader = stmtsummaryv2.NewMemReader(r.stmtSummary, r.columns, instanceAddr, timeLocation, user, hasProcessPriv, digests, timeRanges)
	}
	return r.memReader.Rows(), nil
}

func (r *stmtSummaryRetrieverV2) retrieveHistory(ctx context.Context, sctx sessionctx.Context) ([][]types.Datum, error) {
	if r.historyReader == nil {
		var err error
		var instanceAddr string
		if r.table.Name.O == infoschema.ClusterTableStatementsSummaryHistory {
			instanceAddr, err = infoschema.GetInstanceAddr(sctx)
			if err != nil {
				return nil, err
			}
		}
		timeLocation := sctx.GetSessionVars().TimeZone
		user := sctx.GetSessionVars().User
		hasProcessPriv := hasPriv(sctx, mysql.ProcessPriv)
		digests := r.getDigestsFromExtractor()
		timeRanges := r.getTimeRangesFromExtractor()
		r.historyReader, err = stmtsummaryv2.NewHistoryReader(ctx, r.columns, instanceAddr, timeLocation, user, hasProcessPriv, digests, timeRanges)
		if err != nil {
			return nil, err
		}
		if r.memReader == nil {
			r.memReader = stmtsummaryv2.NewMemReader(r.stmtSummary, r.columns, instanceAddr, timeLocation, user, hasProcessPriv, digests, timeRanges)
		}
	}
	rows, err := r.historyReader.Rows()
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		if r.retrieved {
			return nil, nil
		}
		r.retrieved = true
		return r.memReader.Rows(), nil
	}
	return rows, nil
}

func (r *stmtSummaryRetrieverV2) retrieveEvicted(sctx sessionctx.Context) ([][]types.Datum, error) {
	if r.retrieved {
		return nil, nil
	}
	r.retrieved = true
	if !hasPriv(sctx, mysql.ProcessPriv) {
		return nil, plannercore.ErrSpecificAccessDenied.GenWithStackByArgs("PROCESS")
	}
	row := r.stmtSummary.Evicted()
	if row == nil {
		return nil, nil
	}
	rows := [][]types.Datum{row}
	if r.table.Name.O == infoschema.ClusterTableStatementsSummaryEvicted {
		var err error
		rows, err = infoschema.AppendHostInfoToRows(sctx, rows)
		if err != nil {
			return nil, err
		}
	}
	return adjustColumns(rows, r.columns, r.table), nil
}

func (r *stmtSummaryRetrieverV2) close() error {
	if r.historyReader != nil {
		r.historyReader.Close()
	}
	return nil
}

func (r *stmtSummaryRetrieverV2) getRuntimeStats() execdetails.RuntimeStats {
	return nil
}

func (r *stmtSummaryRetrieverV2) getDigestsFromExtractor() set.StringSet {
	if r.extractor != nil && r.extractor.Enable {
		return r.extractor.Digests
	}
	return nil
}

func (r *stmtSummaryRetrieverV2) getTimeRangesFromExtractor() []*stmtsummaryv2.StmtTimeRange {
	var timeRanges []*stmtsummaryv2.StmtTimeRange
	if r.extractor != nil && len(r.extractor.TimeRanges) > 0 {
		timeRanges = make([]*stmtsummaryv2.StmtTimeRange, len(r.extractor.TimeRanges))
		for n, tr := range r.extractor.TimeRanges {
			timeRanges[n] = &stmtsummaryv2.StmtTimeRange{
				Begin: tr.StartTime.Unix(),
				End:   tr.EndTime.Unix(),
			}
		}
	}
	return timeRanges
}
