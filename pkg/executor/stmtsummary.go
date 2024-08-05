// Copyright 2023 PingCAP, Inc.
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

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/set"
	"github.com/pingcap/tidb/pkg/util/stmtsummary"
	stmtsummaryv2 "github.com/pingcap/tidb/pkg/util/stmtsummary/v2"
)

const (
	defaultRetrieveCount = 1024
)

func buildStmtSummaryRetriever(
	table *model.TableInfo,
	columns []*model.ColumnInfo,
	extractor *plannercore.StatementsSummaryExtractor,
) memTableRetriever {
	if extractor == nil {
		extractor = &plannercore.StatementsSummaryExtractor{}
	}
	if extractor.Digests.Empty() {
		extractor.Digests = nil
	}

	var retriever memTableRetriever
	if extractor.SkipRequest {
		retriever = &dummyRetriever{}
	} else if config.GetGlobalConfig().Instance.StmtSummaryEnablePersistent {
		retriever = &stmtSummaryRetrieverV2{
			stmtSummary: stmtsummaryv2.GlobalStmtSummary,
			table:       table,
			columns:     columns,
			digests:     extractor.Digests,
			timeRanges:  buildTimeRanges(extractor.CoarseTimeRange),
		}
	} else {
		retriever = &stmtSummaryRetriever{
			table:   table,
			columns: columns,
			digests: extractor.Digests,
		}
	}

	return retriever
}

type dummyRetriever struct {
	dummyCloser
}

func (*dummyRetriever) retrieve(_ context.Context, _ sessionctx.Context) ([][]types.Datum, error) {
	return nil, nil
}

// stmtSummaryRetriever is used to retrieve statements summary.
type stmtSummaryRetriever struct {
	table   *model.TableInfo
	columns []*model.ColumnInfo
	digests set.StringSet

	// lazily initialized
	rowsReader *rowsReader
}

func (e *stmtSummaryRetriever) retrieve(_ context.Context, sctx sessionctx.Context) ([][]types.Datum, error) {
	if err := e.ensureRowsReader(sctx); err != nil {
		return nil, err
	}
	return e.rowsReader.read(defaultRetrieveCount)
}

func (e *stmtSummaryRetriever) close() error {
	if e.rowsReader != nil {
		return e.rowsReader.close()
	}
	return nil
}

func (*stmtSummaryRetriever) getRuntimeStats() execdetails.RuntimeStats {
	return nil
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
		return newSimpleRowsReader(adjustColumns(rows, e.columns, e.table)), nil
	}

	// Additional column `INSTANCE` for cluster table
	rows, err := infoschema.AppendHostInfoToRows(sctx, rows)
	if err != nil {
		return nil, err
	}
	// rows are full-columned, so we need to adjust them to the required columns.
	return newSimpleRowsReader(adjustColumns(rows, e.columns, e.table)), nil
}

func (e *stmtSummaryRetriever) initSummaryRowsReader(sctx sessionctx.Context) (*rowsReader, error) {
	vars := sctx.GetSessionVars()
	user := vars.User
	tz := vars.StmtCtx.TimeZone()
	columns := e.columns
	priv := hasPriv(sctx, mysql.ProcessPriv)
	instanceAddr, err := clusterTableInstanceAddr(sctx, e.table.Name.O)
	if err != nil {
		return nil, err
	}

	reader := stmtsummary.NewStmtSummaryReader(user, priv, columns, instanceAddr, tz)
	if e.digests != nil {
		// set checker to filter out statements not matching the given digests
		checker := stmtsummary.NewStmtSummaryChecker(e.digests)
		reader.SetChecker(checker)
	}

	var rows [][]types.Datum
	if isCurrentTable(e.table.Name.O) {
		rows = reader.GetStmtSummaryCurrentRows()
	}
	if isHistoryTable(e.table.Name.O) {
		rows = reader.GetStmtSummaryHistoryRows()
	}
	return newSimpleRowsReader(rows), nil
}

// stmtSummaryRetriever is used to retrieve statements summary when
// config.GetGlobalConfig().Instance.StmtSummaryEnablePersistent is true
type stmtSummaryRetrieverV2 struct {
	stmtSummary *stmtsummaryv2.StmtSummary
	table       *model.TableInfo
	columns     []*model.ColumnInfo
	digests     set.StringSet
	timeRanges  []*stmtsummaryv2.StmtTimeRange

	// lazily initialized
	rowsReader *rowsReader
}

func (r *stmtSummaryRetrieverV2) retrieve(ctx context.Context, sctx sessionctx.Context) ([][]types.Datum, error) {
	if err := r.ensureRowsReader(ctx, sctx); err != nil {
		return nil, err
	}
	return r.rowsReader.read(defaultRetrieveCount)
}

func (r *stmtSummaryRetrieverV2) close() error {
	if r.rowsReader != nil {
		return r.rowsReader.close()
	}
	return nil
}

func (*stmtSummaryRetrieverV2) getRuntimeStats() execdetails.RuntimeStats {
	return nil
}

func (r *stmtSummaryRetrieverV2) ensureRowsReader(ctx context.Context, sctx sessionctx.Context) error {
	if r.rowsReader != nil {
		return nil
	}

	var err error
	if isEvictedTable(r.table.Name.O) {
		r.rowsReader, err = r.initEvictedRowsReader(sctx)
	} else {
		r.rowsReader, err = r.initSummaryRowsReader(ctx, sctx)
	}

	return err
}

func (r *stmtSummaryRetrieverV2) initEvictedRowsReader(sctx sessionctx.Context) (*rowsReader, error) {
	if err := checkPrivilege(sctx); err != nil {
		return nil, err
	}

	var rows [][]types.Datum

	row := r.stmtSummary.Evicted()
	if row != nil {
		rows = append(rows, row)
	}
	if !isClusterTable(r.table.Name.O) {
		// rows are full-columned, so we need to adjust them to the required columns.
		return newSimpleRowsReader(adjustColumns(rows, r.columns, r.table)), nil
	}

	// Additional column `INSTANCE` for cluster table
	rows, err := infoschema.AppendHostInfoToRows(sctx, rows)
	if err != nil {
		return nil, err
	}
	// rows are full-columned, so we need to adjust them to the required columns.
	return newSimpleRowsReader(adjustColumns(rows, r.columns, r.table)), nil
}

func (r *stmtSummaryRetrieverV2) initSummaryRowsReader(ctx context.Context, sctx sessionctx.Context) (*rowsReader, error) {
	vars := sctx.GetSessionVars()
	user := vars.User
	tz := vars.StmtCtx.TimeZone()
	stmtSummary := r.stmtSummary
	columns := r.columns
	timeRanges := r.timeRanges
	digests := r.digests
	priv := hasPriv(sctx, mysql.ProcessPriv)
	instanceAddr, err := clusterTableInstanceAddr(sctx, r.table.Name.O)
	if err != nil {
		return nil, err
	}

	mem := stmtsummaryv2.NewMemReader(stmtSummary, columns, instanceAddr, tz, user, priv, digests, timeRanges)
	memRows := mem.Rows()

	var rowsReader *rowsReader
	if isCurrentTable(r.table.Name.O) {
		rowsReader = newSimpleRowsReader(memRows)
	}
	if isHistoryTable(r.table.Name.O) {
		// history table should return all rows including mem and disk
		concurrent := sctx.GetSessionVars().Concurrency.DistSQLScanConcurrency()
		history, err := stmtsummaryv2.NewHistoryReader(ctx, columns, instanceAddr, tz, user, priv, digests, timeRanges, concurrent)
		if err != nil {
			return nil, err
		}
		rowsReader = newRowsReader(memRows, history)
	}

	return rowsReader, nil
}

type rowsPuller interface {
	Closeable
	Rows() ([][]types.Datum, error)
}

type rowsReader struct {
	puller rowsPuller
	rows   [][]types.Datum
}

func newSimpleRowsReader(rows [][]types.Datum) *rowsReader {
	return &rowsReader{rows: rows}
}

func newRowsReader(rows [][]types.Datum, puller rowsPuller) *rowsReader {
	return &rowsReader{puller: puller, rows: rows}
}

func (r *rowsReader) read(maxCount int) ([][]types.Datum, error) {
	if err := r.pull(); err != nil {
		return nil, err
	}

	if maxCount >= len(r.rows) {
		ret := r.rows
		r.rows = nil
		return ret, nil
	}
	ret := r.rows[:maxCount]
	r.rows = r.rows[maxCount:]
	return ret, nil
}

func (r *rowsReader) pull() error {
	if r.puller == nil {
		return nil
	}
	// there are remaining rows
	if len(r.rows) > 0 {
		return nil
	}

	rows, err := r.puller.Rows()
	if err != nil {
		return err
	}
	// pulled new rows from the puller
	if len(rows) != 0 {
		r.rows = rows
		return nil
	}

	// reach the end of the puller
	err = r.puller.Close()
	if err != nil {
		return err
	}
	r.puller = nil
	return nil
}

func (r *rowsReader) close() error {
	if r.puller != nil {
		return r.puller.Close()
	}
	return nil
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
		return plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("PROCESS")
	}
	return nil
}

func clusterTableInstanceAddr(sctx sessionctx.Context, originalTableName string) (string, error) {
	if isClusterTable(originalTableName) {
		return infoschema.GetInstanceAddr(sctx)
	}
	return "", nil
}

func buildTimeRanges(tr *plannercore.TimeRange) []*stmtsummaryv2.StmtTimeRange {
	if tr == nil {
		return nil
	}

	return []*stmtsummaryv2.StmtTimeRange{{
		Begin: tr.StartTime.Unix(),
		End:   tr.EndTime.Unix(),
	}}
}
