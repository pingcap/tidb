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

	// lazily initialized
	rowsReader *rowsReader
}

func (e *stmtSummaryRetriever) retrieve(ctx context.Context, sctx sessionctx.Context) ([][]types.Datum, error) {
	if e.extractor != nil && e.extractor.SkipRequest {
		return nil, nil
	}

	if err := e.ensureRowsReader(sctx); err != nil {
		return nil, err
	}
	return e.rowsReader.read(1024), nil
}

func (e *stmtSummaryRetriever) ensureRowsReader(sctx sessionctx.Context) error {
	if e.rowsReader != nil {
		return nil
	}

	var err error
	if isEvictedTable(e.table.Name.O) {
		e.rowsReader, err = e.initEvictedRowsReader(sctx)
	} else {
		e.rowsReader, err = e.initSummaryRowsReader(sctx)
	}

	return err
}

func (e *stmtSummaryRetriever) initEvictedRowsReader(sctx sessionctx.Context) (*rowsReader, error) {
	if err := checkPrivilege(sctx); err != nil {
		return nil, err
	}

	rows := stmtsummary.StmtSummaryByDigestMap.ToEvictedCountDatum()
	if !isClusterTable(e.table.Name.O) {
		// rows are full-columned, so we need to adjust them to the required columns.
		return &rowsReader{rows: adjustColumns(rows, e.columns, e.table)}, nil
	}

	// Additional column `INSTANCE` for cluster table
	rows, err := infoschema.AppendHostInfoToRows(sctx, rows)
	if err != nil {
		return nil, err
	}
	// rows are full-columned, so we need to adjust them to the required columns.
	return &rowsReader{rows: adjustColumns(rows, e.columns, e.table)}, nil
}

func (e *stmtSummaryRetriever) initSummaryRowsReader(sctx sessionctx.Context) (*rowsReader, error) {
	vars := sctx.GetSessionVars()
	user := vars.User
	tz := vars.StmtCtx.TimeZone
	columns := e.columns
	priv := hasPriv(sctx, mysql.ProcessPriv)
	instanceAddr, err := e.clusterTableInstanceAddr(sctx)
	if err != nil {
		return nil, err
	}

	reader := stmtsummary.NewStmtSummaryReader(user, priv, columns, instanceAddr, tz)
	if e.extractor != nil && !e.extractor.Digests.Empty() {
		// set checker to filter out statements not matching the given digests
		checker := stmtsummary.NewStmtSummaryChecker(e.extractor.Digests)
		reader.SetChecker(checker)
	}

	var rows [][]types.Datum
	if isCurrentTable(e.table.Name.O) {
		rows = reader.GetStmtSummaryCurrentRows()
	}
	if isHistoryTable(e.table.Name.O) {
		rows = reader.GetStmtSummaryHistoryRows()
	}
	return &rowsReader{rows: rows}, nil
}

func (e *stmtSummaryRetriever) clusterTableInstanceAddr(sctx sessionctx.Context) (string, error) {
	if isClusterTable(e.table.Name.O) {
		return infoschema.GetInstanceAddr(sctx)
	}
	return "", nil
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
	if r.extractor != nil && !r.extractor.Digests.Empty() {
		return r.extractor.Digests
	}
	return nil
}

func (r *stmtSummaryRetrieverV2) getTimeRangesFromExtractor() []*stmtsummaryv2.StmtTimeRange {
	if r.extractor != nil && r.extractor.CoarseTimeRange != nil {
		return []*stmtsummaryv2.StmtTimeRange{{
			Begin: r.extractor.CoarseTimeRange.StartTime.Unix(),
			End:   r.extractor.CoarseTimeRange.EndTime.Unix(),
		}}
	}
	return nil
}

type rowsReader struct {
	rows [][]types.Datum
}

func (r *rowsReader) read(maxCount int) [][]types.Datum {
	if maxCount >= len(r.rows) {
		ret := r.rows
		r.rows = nil
		return ret
	}
	ret := r.rows[:maxCount]
	r.rows = r.rows[maxCount:]
	return ret
}

func isClusterTable(originalTableName string) bool {
	switch originalTableName {
	case infoschema.ClusterTableStatementsSummary,
		infoschema.ClusterTableStatementsSummaryHistory,
		infoschema.ClusterTableStatementsSummaryEvicted:
		return true
	}

	return false
}

func isCurrentTable(originalTableName string) bool {
	switch originalTableName {
	case infoschema.TableStatementsSummary,
		infoschema.ClusterTableStatementsSummary:
		return true
	}

	return false
}

func isHistoryTable(originalTableName string) bool {
	switch originalTableName {
	case infoschema.TableStatementsSummaryHistory,
		infoschema.ClusterTableStatementsSummaryHistory:
		return true
	}

	return false
}

func isEvictedTable(originalTableName string) bool {
	switch originalTableName {
	case infoschema.TableStatementsSummaryEvicted,
		infoschema.ClusterTableStatementsSummaryEvicted:
		return true
	}

	return false
}

func checkPrivilege(sctx sessionctx.Context) error {
	if !hasPriv(sctx, mysql.ProcessPriv) {
		return plannercore.ErrSpecificAccessDenied.GenWithStackByArgs("PROCESS")
	}
	return nil
}
