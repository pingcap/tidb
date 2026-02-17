// Copyright 2015 PingCAP, Inc.
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
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/ranger"
)

func (b *executorBuilder) buildCancelDDLJobs(v *plannercore.CancelDDLJobs) exec.Executor {
	e := &CancelDDLJobsExec{
		CommandDDLJobsExec: &CommandDDLJobsExec{
			BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
			jobIDs:       v.JobIDs,
			execute:      ddl.CancelJobs,
		},
	}
	return e
}

func (b *executorBuilder) buildPauseDDLJobs(v *plannercore.PauseDDLJobs) exec.Executor {
	e := &PauseDDLJobsExec{
		CommandDDLJobsExec: &CommandDDLJobsExec{
			BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
			jobIDs:       v.JobIDs,
			execute:      ddl.PauseJobs,
		},
	}
	return e
}

func (b *executorBuilder) buildResumeDDLJobs(v *plannercore.ResumeDDLJobs) exec.Executor {
	e := &ResumeDDLJobsExec{
		CommandDDLJobsExec: &CommandDDLJobsExec{
			BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
			jobIDs:       v.JobIDs,
			execute:      ddl.ResumeJobs,
		},
	}
	return e
}

func (b *executorBuilder) buildAlterDDLJob(v *plannercore.AlterDDLJob) exec.Executor {
	e := &AlterDDLJobExec{
		BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
		jobID:        v.JobID,
		AlterOpts:    v.Options,
	}
	return e
}

func (b *executorBuilder) buildShowNextRowID(v *plannercore.ShowNextRowID) exec.Executor {
	e := &ShowNextRowIDExec{
		BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
		tblName:      v.TableName,
	}
	return e
}

func (b *executorBuilder) buildShowDDL(v *plannercore.ShowDDL) exec.Executor {
	// We get Info here because for Executors that returns result set,
	// next will be called after transaction has been committed.
	// We need the transaction to get Info.
	e := &ShowDDLExec{
		BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
	}

	var err error
	ownerManager := domain.GetDomain(e.Ctx()).DDL().OwnerManager()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	e.ddlOwnerID, err = ownerManager.GetOwnerID(ctx)
	cancel()
	if err != nil {
		b.err = err
		return nil
	}

	session, err := e.GetSysSession()
	if err != nil {
		b.err = err
		return nil
	}
	ddlInfo, err := ddl.GetDDLInfoWithNewTxn(session)
	e.ReleaseSysSession(kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL), session)
	if err != nil {
		b.err = err
		return nil
	}
	e.ddlInfo = ddlInfo
	e.selfID = ownerManager.ID()
	return e
}

func (b *executorBuilder) buildShowDDLJobs(v *physicalop.PhysicalShowDDLJobs) exec.Executor {
	loc := b.ctx.GetSessionVars().Location()
	ddlJobRetriever := DDLJobRetriever{TZLoc: loc}
	e := &ShowDDLJobsExec{
		jobNumber:       int(v.JobNumber),
		is:              b.is,
		BaseExecutor:    exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
		DDLJobRetriever: ddlJobRetriever,
	}
	return e
}

func (b *executorBuilder) buildShowDDLJobQueries(v *plannercore.ShowDDLJobQueries) exec.Executor {
	e := &ShowDDLJobQueriesExec{
		BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
		jobIDs:       v.JobIDs,
	}
	return e
}

func (b *executorBuilder) buildShowDDLJobQueriesWithRange(v *plannercore.ShowDDLJobQueriesWithRange) exec.Executor {
	e := &ShowDDLJobQueriesWithRangeExec{
		BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
		offset:       v.Offset,
		limit:        v.Limit,
	}
	return e
}

func (b *executorBuilder) buildShowSlow(v *plannercore.ShowSlow) exec.Executor {
	e := &ShowSlowExec{
		BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
		ShowSlow:     v.ShowSlow,
	}
	return e
}

// buildIndexLookUpChecker builds check information to IndexLookUpReader.
func buildIndexLookUpChecker(b *executorBuilder, p *physicalop.PhysicalIndexLookUpReader,
	e *IndexLookUpExecutor,
) {
	is := p.IndexPlans[0].(*physicalop.PhysicalIndexScan)
	fullColLen := len(is.Index.Columns) + len(p.CommonHandleCols)
	if !e.isCommonHandle() {
		fullColLen++
	}
	if e.index.Global {
		fullColLen++
	}
	e.dagPB.OutputOffsets = make([]uint32, fullColLen)
	for i := range fullColLen {
		e.dagPB.OutputOffsets[i] = uint32(i)
	}

	ts := p.TablePlans[0].(*physicalop.PhysicalTableScan)
	e.handleIdx = ts.HandleIdx

	e.ranges = ranger.FullRange()

	tps := make([]*types.FieldType, 0, fullColLen)
	for _, col := range is.Columns {
		// tps is used to decode the index, we should use the element type of the array if any.
		tps = append(tps, col.FieldType.ArrayType())
	}

	if !e.isCommonHandle() {
		tps = append(tps, types.NewFieldType(mysql.TypeLonglong))
	}
	if e.index.Global {
		tps = append(tps, types.NewFieldType(mysql.TypeLonglong))
	}

	e.checkIndexValue = &checkIndexValue{idxColTps: tps}

	colNames := make([]string, 0, len(is.IdxCols))
	for i := range is.IdxCols {
		colNames = append(colNames, is.Columns[i].Name.L)
	}
	if cols, missingColOffset := table.FindColumns(e.table.Cols(), colNames, true); missingColOffset >= 0 {
		b.err = plannererrors.ErrUnknownColumn.GenWithStack("Unknown column %s", is.Columns[missingColOffset].Name.O)
	} else {
		e.idxTblCols = cols
	}
}

func (b *executorBuilder) buildCheckTable(v *plannercore.CheckTable) exec.Executor {
	canUseFastCheck := true
	for _, idx := range v.IndexInfos {
		if idx.MVIndex || idx.IsColumnarIndex() {
			canUseFastCheck = false
			break
		}
		for _, col := range idx.Columns {
			if col.Length != types.UnspecifiedLength {
				canUseFastCheck = false
				break
			}
		}
		if !canUseFastCheck {
			break
		}
	}
	if b.ctx.GetSessionVars().FastCheckTable && canUseFastCheck {
		e := &FastCheckTableExec{
			BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
			dbName:       v.DBName,
			table:        v.Table,
			indexInfos:   v.IndexInfos,
			is:           b.is,
		}
		return e
	}

	readerExecs := make([]*IndexLookUpExecutor, 0, len(v.IndexLookUpReaders))
	for _, readerPlan := range v.IndexLookUpReaders {
		readerExec, err := buildNoRangeIndexLookUpReader(b, readerPlan)
		if err != nil {
			b.err = errors.Trace(err)
			return nil
		}
		buildIndexLookUpChecker(b, readerPlan, readerExec)

		readerExecs = append(readerExecs, readerExec)
	}

	e := &CheckTableExec{
		BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
		dbName:       v.DBName,
		table:        v.Table,
		indexInfos:   v.IndexInfos,
		srcs:         readerExecs,
		exitCh:       make(chan struct{}),
		retCh:        make(chan error, len(readerExecs)),
		checkIndex:   v.CheckIndex,
	}
	return e
}

func buildIdxColsConcatHandleCols(tblInfo *model.TableInfo, indexInfo *model.IndexInfo, hasGenedColOrPartialIndex bool) []*model.ColumnInfo {
	var pkCols []*model.IndexColumn
	if tblInfo.IsCommonHandle {
		pkIdx := tables.FindPrimaryIndex(tblInfo)
		pkCols = pkIdx.Columns
	}

	columns := make([]*model.ColumnInfo, 0, len(indexInfo.Columns)+len(pkCols))
	if hasGenedColOrPartialIndex {
		columns = tblInfo.Columns
	} else {
		for _, idxCol := range indexInfo.Columns {
			if tblInfo.PKIsHandle && tblInfo.GetPkColInfo().Offset == idxCol.Offset {
				continue
			}
			columns = append(columns, tblInfo.Columns[idxCol.Offset])
		}
	}

	if tblInfo.IsCommonHandle {
		for _, c := range pkCols {
			if model.FindColumnInfo(columns, c.Name.L) == nil {
				columns = append(columns, tblInfo.Columns[c.Offset])
			}
		}
		return columns
	}
	if tblInfo.PKIsHandle {
		columns = append(columns, tblInfo.Columns[tblInfo.GetPkColInfo().Offset])
		return columns
	}
	handleOffset := len(columns)
	handleColsInfo := &model.ColumnInfo{
		ID:     model.ExtraHandleID,
		Name:   model.ExtraHandleName,
		Offset: handleOffset,
	}
	handleColsInfo.FieldType = *types.NewFieldType(mysql.TypeLonglong)
	columns = append(columns, handleColsInfo)
	return columns
}

func (b *executorBuilder) buildRecoverIndex(v *plannercore.RecoverIndex) exec.Executor {
	tblInfo := v.Table.TableInfo
	t, err := b.is.TableByName(context.Background(), v.Table.Schema, tblInfo.Name)
	if err != nil {
		b.err = err
		return nil
	}
	idxName := strings.ToLower(v.IndexName)
	index := tables.GetWritableIndexByName(idxName, t)
	if index == nil {
		b.err = errors.Errorf("secondary index `%v` is not found in table `%v`", v.IndexName, v.Table.Name.O)
		return nil
	}
	var hasGenedColOrPartialIndex bool
	for _, iCol := range index.Meta().Columns {
		if tblInfo.Columns[iCol.Offset].IsGenerated() {
			hasGenedColOrPartialIndex = true
		}
	}
	if index.Meta().HasCondition() {
		hasGenedColOrPartialIndex = true
	}
	cols := buildIdxColsConcatHandleCols(tblInfo, index.Meta(), hasGenedColOrPartialIndex)
	e := &RecoverIndexExec{
		BaseExecutor:                   exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
		columns:                        cols,
		containsGenedColOrPartialIndex: hasGenedColOrPartialIndex,
		index:                          index,
		table:                          t,
		physicalID:                     t.Meta().ID,
	}
	e.handleCols = buildHandleColsForExec(tblInfo, e.columns)
	return e
}

func buildHandleColsForExec(tblInfo *model.TableInfo, allColInfo []*model.ColumnInfo) plannerutil.HandleCols {
	if !tblInfo.IsCommonHandle {
		extraColPos := len(allColInfo) - 1
		intCol := &expression.Column{
			Index:   extraColPos,
			RetType: types.NewFieldType(mysql.TypeLonglong),
		}
		return plannerutil.NewIntHandleCols(intCol)
	}
	tblCols := make([]*expression.Column, len(tblInfo.Columns))
	for i := range tblInfo.Columns {
		c := tblInfo.Columns[i]
		tblCols[i] = &expression.Column{
			RetType: &c.FieldType,
			ID:      c.ID,
		}
	}
	pkIdx := tables.FindPrimaryIndex(tblInfo)
	for _, c := range pkIdx.Columns {
		for j, colInfo := range allColInfo {
			if colInfo.Name.L == c.Name.L {
				tblCols[c.Offset].Index = j
			}
		}
	}
	return plannerutil.NewCommonHandleCols(tblInfo, pkIdx, tblCols)
}

func (b *executorBuilder) buildCleanupIndex(v *plannercore.CleanupIndex) exec.Executor {
	tblInfo := v.Table.TableInfo
	t, err := b.is.TableByName(context.Background(), v.Table.Schema, tblInfo.Name)
	if err != nil {
		b.err = err
		return nil
	}
	idxName := strings.ToLower(v.IndexName)
	var index table.Index
	for _, idx := range t.Indices() {
		if idx.Meta().State != model.StatePublic {
			continue
		}
		if idxName == idx.Meta().Name.L {
			index = idx
			break
		}
	}

	if index == nil {
		b.err = errors.Errorf("secondary index `%v` is not found in table `%v`", v.IndexName, v.Table.Name.O)
		return nil
	}
	if index.Meta().IsColumnarIndex() {
		b.err = errors.Errorf("columnar index `%v` is not supported for cleanup index", v.IndexName)
		return nil
	}
	e := &CleanupIndexExec{
		BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
		columns:      buildIdxColsConcatHandleCols(tblInfo, index.Meta(), false),
		index:        index,
		table:        t,
		physicalID:   t.Meta().ID,
		batchSize:    20000,
	}
	e.handleCols = buildHandleColsForExec(tblInfo, e.columns)
	if e.index.Meta().Global {
		e.columns = append(e.columns, model.NewExtraPhysTblIDColInfo())
	}
	return e
}

func (b *executorBuilder) buildCheckIndexRange(v *plannercore.CheckIndexRange) exec.Executor {
	tb, err := b.is.TableByName(context.Background(), v.Table.Schema, v.Table.Name)
	if err != nil {
		b.err = err
		return nil
	}
	e := &CheckIndexRangeExec{
		BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
		handleRanges: v.HandleRanges,
		table:        tb.Meta(),
		is:           b.is,
	}
	idxName := strings.ToLower(v.IndexName)
	for _, idx := range tb.Indices() {
		if idx.Meta().Name.L == idxName {
			e.index = idx.Meta()
			e.startKey = make([]types.Datum, len(e.index.Columns))
			break
		}
	}
	return e
}

func (b *executorBuilder) buildChecksumTable(v *plannercore.ChecksumTable) exec.Executor {
	e := &ChecksumTableExec{
		BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
		tables:       make(map[int64]*checksumContext),
		done:         false,
	}
	startTs, err := b.getSnapshotTS()
	if err != nil {
		b.err = err
		return nil
	}
	for _, t := range v.Tables {
		e.tables[t.TableInfo.ID] = newChecksumContext(t.DBInfo, t.TableInfo, startTs)
	}
	return e
}

func (b *executorBuilder) buildReloadExprPushdownBlacklist(_ *plannercore.ReloadExprPushdownBlacklist) exec.Executor {
	base := exec.NewBaseExecutor(b.ctx, nil, 0)
	return &ReloadExprPushdownBlacklistExec{base}
}

func (b *executorBuilder) buildReloadOptRuleBlacklist(_ *plannercore.ReloadOptRuleBlacklist) exec.Executor {
	base := exec.NewBaseExecutor(b.ctx, nil, 0)
	return &ReloadOptRuleBlacklistExec{BaseExecutor: base}
}

func (b *executorBuilder) buildAdminPlugins(v *plannercore.AdminPlugins) exec.Executor {
	base := exec.NewBaseExecutor(b.ctx, nil, 0)
	return &AdminPluginsExec{BaseExecutor: base, Action: v.Action, Plugins: v.Plugins}
}

func (b *executorBuilder) buildDeallocate(v *plannercore.Deallocate) exec.Executor {
	base := exec.NewBaseExecutor(b.ctx, nil, v.ID())
	base.SetInitCap(chunk.ZeroCapacity)
	e := &DeallocateExec{
		BaseExecutor: base,
		Name:         v.Name,
	}
	return e
}

