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
	"bytes"
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/diagnosticspb"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/executor/aggfuncs"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	plannercore "github.com/pingcap/tidb/planner/core"
	plannerutil "github.com/pingcap/tidb/planner/util"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/sessiontxn/legacy"
	"github.com/pingcap/tidb/sessiontxn/staleread"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/store/helper"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/table/temptable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/cteutil"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tidb/util/rowcodec"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/timeutil"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

var (
	executorCounterMergeJoinExec            = metrics.ExecutorCounter.WithLabelValues("MergeJoinExec")
	executorCountHashJoinExec               = metrics.ExecutorCounter.WithLabelValues("HashJoinExec")
	executorCounterHashAggExec              = metrics.ExecutorCounter.WithLabelValues("HashAggExec")
	executorStreamAggExec                   = metrics.ExecutorCounter.WithLabelValues("StreamAggExec")
	executorCounterSortExec                 = metrics.ExecutorCounter.WithLabelValues("SortExec")
	executorCounterTopNExec                 = metrics.ExecutorCounter.WithLabelValues("TopNExec")
	executorCounterNestedLoopApplyExec      = metrics.ExecutorCounter.WithLabelValues("NestedLoopApplyExec")
	executorCounterIndexLookUpJoin          = metrics.ExecutorCounter.WithLabelValues("IndexLookUpJoin")
	executorCounterIndexLookUpExecutor      = metrics.ExecutorCounter.WithLabelValues("IndexLookUpExecutor")
	executorCounterIndexMergeReaderExecutor = metrics.ExecutorCounter.WithLabelValues("IndexMergeReaderExecutor")
)

// executorBuilder builds an Executor from a Plan.
// The InfoSchema must not change during execution.
type executorBuilder struct {
	ctx              sessionctx.Context
	is               infoschema.InfoSchema
	snapshotTS       uint64 // The ts for snapshot-read. A select statement without for update will use this ts
	forUpdateTS      uint64 // The ts should be used by insert/update/delete/select-for-update statement
	snapshotTSCached bool
	err              error // err is set when there is error happened during Executor building process.
	hasLock          bool
	Ti               *TelemetryInfo
	// isStaleness means whether this statement use stale read.
	isStaleness      bool
	readReplicaScope string
	inUpdateStmt     bool
	inDeleteStmt     bool
	inInsertStmt     bool
	inSelectLockStmt bool

	// forDataReaderBuilder indicates whether the builder is used by a dataReaderBuilder.
	// When forDataReader is true, the builder should use the dataReaderTS as the executor read ts. This is because
	// dataReaderBuilder can be used in concurrent goroutines, so we must ensure that getting the ts should be thread safe and
	// can return a correct value even if the session context has already been destroyed
	forDataReaderBuilder bool
	dataReaderTS         uint64
}

// CTEStorages stores resTbl and iterInTbl for CTEExec.
// There will be a map[CTEStorageID]*CTEStorages in StmtCtx,
// which will store all CTEStorages to make all shared CTEs use same the CTEStorages.
type CTEStorages struct {
	ResTbl    cteutil.Storage
	IterInTbl cteutil.Storage
}

func newExecutorBuilder(ctx sessionctx.Context, is infoschema.InfoSchema, ti *TelemetryInfo, replicaReadScope string) *executorBuilder {
	b := &executorBuilder{
		ctx:              ctx,
		is:               is,
		Ti:               ti,
		isStaleness:      staleread.IsStmtStaleness(ctx),
		readReplicaScope: replicaReadScope,
	}

	txnManager := sessiontxn.GetTxnManager(ctx)
	if provider, ok := txnManager.GetContextProvider().(*legacy.SimpleTxnContextProvider); ok {
		provider.GetReadTSFunc = b.getReadTS
		provider.GetForUpdateTSFunc = func() (uint64, error) {
			if b.forUpdateTS != 0 {
				return b.forUpdateTS, nil
			}
			return b.getReadTS()
		}
	}

	return b
}

// MockPhysicalPlan is used to return a specified executor in when build.
// It is mainly used for testing.
type MockPhysicalPlan interface {
	plannercore.PhysicalPlan
	GetExecutor() Executor
}

// MockExecutorBuilder is a wrapper for executorBuilder.
// ONLY used in test.
type MockExecutorBuilder struct {
	*executorBuilder
}

// NewMockExecutorBuilderForTest is ONLY used in test.
func NewMockExecutorBuilderForTest(ctx sessionctx.Context, is infoschema.InfoSchema, ti *TelemetryInfo, replicaReadScope string) *MockExecutorBuilder {
	return &MockExecutorBuilder{
		executorBuilder: newExecutorBuilder(ctx, is, ti, replicaReadScope)}
}

// Build builds an executor tree according to `p`.
func (b *MockExecutorBuilder) Build(p plannercore.Plan) Executor {
	return b.build(p)
}

func (b *executorBuilder) build(p plannercore.Plan) Executor {
	switch v := p.(type) {
	case nil:
		return nil
	case *plannercore.Change:
		return b.buildChange(v)
	case *plannercore.CheckTable:
		return b.buildCheckTable(v)
	case *plannercore.RecoverIndex:
		return b.buildRecoverIndex(v)
	case *plannercore.CleanupIndex:
		return b.buildCleanupIndex(v)
	case *plannercore.CheckIndexRange:
		return b.buildCheckIndexRange(v)
	case *plannercore.ChecksumTable:
		return b.buildChecksumTable(v)
	case *plannercore.ReloadExprPushdownBlacklist:
		return b.buildReloadExprPushdownBlacklist(v)
	case *plannercore.ReloadOptRuleBlacklist:
		return b.buildReloadOptRuleBlacklist(v)
	case *plannercore.AdminPlugins:
		return b.buildAdminPlugins(v)
	case *plannercore.DDL:
		return b.buildDDL(v)
	case *plannercore.Deallocate:
		return b.buildDeallocate(v)
	case *plannercore.Delete:
		return b.buildDelete(v)
	case *plannercore.Execute:
		return b.buildExecute(v)
	case *plannercore.Trace:
		return b.buildTrace(v)
	case *plannercore.Explain:
		return b.buildExplain(v)
	case *plannercore.PointGetPlan:
		return b.buildPointGet(v)
	case *plannercore.BatchPointGetPlan:
		return b.buildBatchPointGet(v)
	case *plannercore.Insert:
		return b.buildInsert(v)
	case *plannercore.LoadData:
		return b.buildLoadData(v)
	case *plannercore.LoadStats:
		return b.buildLoadStats(v)
	case *plannercore.IndexAdvise:
		return b.buildIndexAdvise(v)
	case *plannercore.PlanReplayer:
		return b.buildPlanReplayer(v)
	case *plannercore.PhysicalLimit:
		return b.buildLimit(v)
	case *plannercore.Prepare:
		return b.buildPrepare(v)
	case *plannercore.PhysicalLock:
		return b.buildSelectLock(v)
	case *plannercore.CancelDDLJobs:
		return b.buildCancelDDLJobs(v)
	case *plannercore.ShowNextRowID:
		return b.buildShowNextRowID(v)
	case *plannercore.ShowDDL:
		return b.buildShowDDL(v)
	case *plannercore.PhysicalShowDDLJobs:
		return b.buildShowDDLJobs(v)
	case *plannercore.ShowDDLJobQueries:
		return b.buildShowDDLJobQueries(v)
	case *plannercore.ShowSlow:
		return b.buildShowSlow(v)
	case *plannercore.PhysicalShow:
		return b.buildShow(v)
	case *plannercore.Simple:
		return b.buildSimple(v)
	case *plannercore.PhysicalSimpleWrapper:
		return b.buildSimple(&v.Inner)
	case *plannercore.Set:
		return b.buildSet(v)
	case *plannercore.SetConfig:
		return b.buildSetConfig(v)
	case *plannercore.PhysicalSort:
		return b.buildSort(v)
	case *plannercore.PhysicalTopN:
		return b.buildTopN(v)
	case *plannercore.PhysicalUnionAll:
		return b.buildUnionAll(v)
	case *plannercore.Update:
		return b.buildUpdate(v)
	case *plannercore.PhysicalUnionScan:
		return b.buildUnionScanExec(v)
	case *plannercore.PhysicalHashJoin:
		return b.buildHashJoin(v)
	case *plannercore.PhysicalMergeJoin:
		return b.buildMergeJoin(v)
	case *plannercore.PhysicalIndexJoin:
		return b.buildIndexLookUpJoin(v)
	case *plannercore.PhysicalIndexMergeJoin:
		return b.buildIndexLookUpMergeJoin(v)
	case *plannercore.PhysicalIndexHashJoin:
		return b.buildIndexNestedLoopHashJoin(v)
	case *plannercore.PhysicalSelection:
		return b.buildSelection(v)
	case *plannercore.PhysicalHashAgg:
		return b.buildHashAgg(v)
	case *plannercore.PhysicalStreamAgg:
		return b.buildStreamAgg(v)
	case *plannercore.PhysicalProjection:
		return b.buildProjection(v)
	case *plannercore.PhysicalMemTable:
		return b.buildMemTable(v)
	case *plannercore.PhysicalTableDual:
		return b.buildTableDual(v)
	case *plannercore.PhysicalApply:
		return b.buildApply(v)
	case *plannercore.PhysicalMaxOneRow:
		return b.buildMaxOneRow(v)
	case *plannercore.Analyze:
		return b.buildAnalyze(v)
	case *plannercore.PhysicalTableReader:
		return b.buildTableReader(v)
	case *plannercore.PhysicalTableSample:
		return b.buildTableSample(v)
	case *plannercore.PhysicalIndexReader:
		return b.buildIndexReader(v)
	case *plannercore.PhysicalIndexLookUpReader:
		return b.buildIndexLookUpReader(v)
	case *plannercore.PhysicalWindow:
		return b.buildWindow(v)
	case *plannercore.PhysicalShuffle:
		return b.buildShuffle(v)
	case *plannercore.PhysicalShuffleReceiverStub:
		return b.buildShuffleReceiverStub(v)
	case *plannercore.SQLBindPlan:
		return b.buildSQLBindExec(v)
	case *plannercore.SplitRegion:
		return b.buildSplitRegion(v)
	case *plannercore.PhysicalIndexMergeReader:
		return b.buildIndexMergeReader(v)
	case *plannercore.SelectInto:
		return b.buildSelectInto(v)
	case *plannercore.AdminShowTelemetry:
		return b.buildAdminShowTelemetry(v)
	case *plannercore.AdminResetTelemetryID:
		return b.buildAdminResetTelemetryID(v)
	case *plannercore.PhysicalCTE:
		return b.buildCTE(v)
	case *plannercore.PhysicalCTETable:
		return b.buildCTETableReader(v)
	case *plannercore.CompactTable:
		return b.buildCompactTable(v)
	default:
		if mp, ok := p.(MockPhysicalPlan); ok {
			return mp.GetExecutor()
		}

		b.err = ErrUnknownPlan.GenWithStack("Unknown Plan %T", p)
		return nil
	}
}

func (b *executorBuilder) buildCancelDDLJobs(v *plannercore.CancelDDLJobs) Executor {
	e := &CancelDDLJobsExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
		jobIDs:       v.JobIDs,
	}
	return e
}

func (b *executorBuilder) buildChange(v *plannercore.Change) Executor {
	return &ChangeExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
		ChangeStmt:   v.ChangeStmt,
	}
}

func (b *executorBuilder) buildShowNextRowID(v *plannercore.ShowNextRowID) Executor {
	e := &ShowNextRowIDExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
		tblName:      v.TableName,
	}
	return e
}

func (b *executorBuilder) buildShowDDL(v *plannercore.ShowDDL) Executor {
	// We get Info here because for Executors that returns result set,
	// next will be called after transaction has been committed.
	// We need the transaction to get Info.
	e := &ShowDDLExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
	}

	var err error
	ownerManager := domain.GetDomain(e.ctx).DDL().OwnerManager()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	e.ddlOwnerID, err = ownerManager.GetOwnerID(ctx)
	cancel()
	if err != nil {
		b.err = err
		return nil
	}
	txn, err := e.ctx.Txn(true)
	if err != nil {
		b.err = err
		return nil
	}

	ddlInfo, err := ddl.GetDDLInfo(txn)
	if err != nil {
		b.err = err
		return nil
	}
	e.ddlInfo = ddlInfo
	e.selfID = ownerManager.ID()
	return e
}

func (b *executorBuilder) buildShowDDLJobs(v *plannercore.PhysicalShowDDLJobs) Executor {
	loc := b.ctx.GetSessionVars().Location()
	ddlJobRetriever := DDLJobRetriever{TZLoc: loc}
	e := &ShowDDLJobsExec{
		jobNumber:       int(v.JobNumber),
		is:              b.is,
		baseExecutor:    newBaseExecutor(b.ctx, v.Schema(), v.ID()),
		DDLJobRetriever: ddlJobRetriever,
	}
	return e
}

func (b *executorBuilder) buildShowDDLJobQueries(v *plannercore.ShowDDLJobQueries) Executor {
	e := &ShowDDLJobQueriesExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
		jobIDs:       v.JobIDs,
	}
	return e
}

func (b *executorBuilder) buildShowSlow(v *plannercore.ShowSlow) Executor {
	e := &ShowSlowExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
		ShowSlow:     v.ShowSlow,
	}
	return e
}

// buildIndexLookUpChecker builds check information to IndexLookUpReader.
func buildIndexLookUpChecker(b *executorBuilder, p *plannercore.PhysicalIndexLookUpReader,
	e *IndexLookUpExecutor) {
	is := p.IndexPlans[0].(*plannercore.PhysicalIndexScan)
	fullColLen := len(is.Index.Columns) + len(p.CommonHandleCols)
	if !e.isCommonHandle() {
		fullColLen += 1
	}
	e.dagPB.OutputOffsets = make([]uint32, fullColLen)
	for i := 0; i < fullColLen; i++ {
		e.dagPB.OutputOffsets[i] = uint32(i)
	}

	ts := p.TablePlans[0].(*plannercore.PhysicalTableScan)
	e.handleIdx = ts.HandleIdx

	e.ranges = ranger.FullRange()

	tps := make([]*types.FieldType, 0, fullColLen)
	for _, col := range is.Columns {
		tps = append(tps, &col.FieldType)
	}

	if !e.isCommonHandle() {
		tps = append(tps, types.NewFieldType(mysql.TypeLonglong))
	}

	e.checkIndexValue = &checkIndexValue{idxColTps: tps}

	colNames := make([]string, 0, len(is.IdxCols))
	for i := range is.IdxCols {
		colNames = append(colNames, is.Columns[i].Name.L)
	}
	if cols, missingColOffset := table.FindColumns(e.table.Cols(), colNames, true); missingColOffset >= 0 {
		b.err = plannercore.ErrUnknownColumn.GenWithStack("Unknown column %s", is.Columns[missingColOffset].Name.O)
	} else {
		e.idxTblCols = cols
	}
}

func (b *executorBuilder) buildCheckTable(v *plannercore.CheckTable) Executor {
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
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
		dbName:       v.DBName,
		table:        v.Table,
		indexInfos:   v.IndexInfos,
		is:           b.is,
		srcs:         readerExecs,
		exitCh:       make(chan struct{}),
		retCh:        make(chan error, len(readerExecs)),
		checkIndex:   v.CheckIndex,
	}
	return e
}

func buildIdxColsConcatHandleCols(tblInfo *model.TableInfo, indexInfo *model.IndexInfo) []*model.ColumnInfo {
	handleLen := 1
	var pkCols []*model.IndexColumn
	if tblInfo.IsCommonHandle {
		pkIdx := tables.FindPrimaryIndex(tblInfo)
		pkCols = pkIdx.Columns
		handleLen = len(pkIdx.Columns)
	}
	columns := make([]*model.ColumnInfo, 0, len(indexInfo.Columns)+handleLen)
	for _, idxCol := range indexInfo.Columns {
		columns = append(columns, tblInfo.Columns[idxCol.Offset])
	}
	if tblInfo.IsCommonHandle {
		for _, c := range pkCols {
			columns = append(columns, tblInfo.Columns[c.Offset])
		}
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

func (b *executorBuilder) buildRecoverIndex(v *plannercore.RecoverIndex) Executor {
	tblInfo := v.Table.TableInfo
	t, err := b.is.TableByName(v.Table.Schema, tblInfo.Name)
	if err != nil {
		b.err = err
		return nil
	}
	idxName := strings.ToLower(v.IndexName)
	index := tables.GetWritableIndexByName(idxName, t)
	if index == nil {
		b.err = errors.Errorf("index `%v` is not found in table `%v`", v.IndexName, v.Table.Name.O)
		return nil
	}
	e := &RecoverIndexExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
		columns:      buildIdxColsConcatHandleCols(tblInfo, index.Meta()),
		index:        index,
		table:        t,
		physicalID:   t.Meta().ID,
	}
	sessCtx := e.ctx.GetSessionVars().StmtCtx
	e.handleCols = buildHandleColsForExec(sessCtx, tblInfo, index.Meta(), e.columns)
	return e
}

func buildHandleColsForExec(sctx *stmtctx.StatementContext, tblInfo *model.TableInfo,
	idxInfo *model.IndexInfo, allColInfo []*model.ColumnInfo) plannercore.HandleCols {
	if !tblInfo.IsCommonHandle {
		extraColPos := len(allColInfo) - 1
		intCol := &expression.Column{
			Index:   extraColPos,
			RetType: types.NewFieldType(mysql.TypeLonglong),
		}
		return plannercore.NewIntHandleCols(intCol)
	}
	tblCols := make([]*expression.Column, len(tblInfo.Columns))
	for i := 0; i < len(tblInfo.Columns); i++ {
		c := tblInfo.Columns[i]
		tblCols[i] = &expression.Column{
			RetType: &c.FieldType,
			ID:      c.ID,
		}
	}
	pkIdx := tables.FindPrimaryIndex(tblInfo)
	for i, c := range pkIdx.Columns {
		tblCols[c.Offset].Index = len(idxInfo.Columns) + i
	}
	return plannercore.NewCommonHandleCols(sctx, tblInfo, pkIdx, tblCols)
}

func (b *executorBuilder) buildCleanupIndex(v *plannercore.CleanupIndex) Executor {
	tblInfo := v.Table.TableInfo
	t, err := b.is.TableByName(v.Table.Schema, tblInfo.Name)
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
		b.err = errors.Errorf("index `%v` is not found in table `%v`", v.IndexName, v.Table.Name.O)
		return nil
	}
	e := &CleanupIndexExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
		columns:      buildIdxColsConcatHandleCols(tblInfo, index.Meta()),
		index:        index,
		table:        t,
		physicalID:   t.Meta().ID,
		batchSize:    20000,
	}
	sessCtx := e.ctx.GetSessionVars().StmtCtx
	e.handleCols = buildHandleColsForExec(sessCtx, tblInfo, index.Meta(), e.columns)
	return e
}

func (b *executorBuilder) buildCheckIndexRange(v *plannercore.CheckIndexRange) Executor {
	tb, err := b.is.TableByName(v.Table.Schema, v.Table.Name)
	if err != nil {
		b.err = err
		return nil
	}
	e := &CheckIndexRangeExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
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

func (b *executorBuilder) buildChecksumTable(v *plannercore.ChecksumTable) Executor {
	e := &ChecksumTableExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
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

func (b *executorBuilder) buildReloadExprPushdownBlacklist(v *plannercore.ReloadExprPushdownBlacklist) Executor {
	return &ReloadExprPushdownBlacklistExec{baseExecutor{ctx: b.ctx}}
}

func (b *executorBuilder) buildReloadOptRuleBlacklist(v *plannercore.ReloadOptRuleBlacklist) Executor {
	return &ReloadOptRuleBlacklistExec{baseExecutor{ctx: b.ctx}}
}

func (b *executorBuilder) buildAdminPlugins(v *plannercore.AdminPlugins) Executor {
	return &AdminPluginsExec{baseExecutor: baseExecutor{ctx: b.ctx}, Action: v.Action, Plugins: v.Plugins}
}

func (b *executorBuilder) buildDeallocate(v *plannercore.Deallocate) Executor {
	base := newBaseExecutor(b.ctx, nil, v.ID())
	base.initCap = chunk.ZeroCapacity
	e := &DeallocateExec{
		baseExecutor: base,
		Name:         v.Name,
	}
	return e
}

func (b *executorBuilder) buildSelectLock(v *plannercore.PhysicalLock) Executor {
	if !b.inSelectLockStmt {
		b.inSelectLockStmt = true
		defer func() { b.inSelectLockStmt = false }()
	}
	b.hasLock = true
	if b.err = b.updateForUpdateTSIfNeeded(v.Children()[0]); b.err != nil {
		return nil
	}
	// Build 'select for update' using the 'for update' ts.
	b.forUpdateTS = b.ctx.GetSessionVars().TxnCtx.GetForUpdateTS()

	src := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	if !b.ctx.GetSessionVars().InTxn() {
		// Locking of rows for update using SELECT FOR UPDATE only applies when autocommit
		// is disabled (either by beginning transaction with START TRANSACTION or by setting
		// autocommit to 0. If autocommit is enabled, the rows matching the specification are not locked.
		// See https://dev.mysql.com/doc/refman/5.7/en/innodb-locking-reads.html
		return src
	}
	e := &SelectLockExec{
		baseExecutor:       newBaseExecutor(b.ctx, v.Schema(), v.ID(), src),
		Lock:               v.Lock,
		tblID2Handle:       v.TblID2Handle,
		tblID2PhysTblIDCol: v.TblID2PhysTblIDCol,
	}

	// filter out temporary tables because they do not store any record in tikv and should not write any lock
	is := e.ctx.GetInfoSchema().(infoschema.InfoSchema)
	for tblID := range e.tblID2Handle {
		tblInfo, ok := is.TableByID(tblID)
		if !ok {
			b.err = errors.Errorf("Can not get table %d", tblID)
		}

		if tblInfo.Meta().TempTableType != model.TempTableNone {
			delete(e.tblID2Handle, tblID)
		}
	}

	return e
}

func (b *executorBuilder) buildLimit(v *plannercore.PhysicalLimit) Executor {
	childExec := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	n := int(mathutil.Min(v.Count, uint64(b.ctx.GetSessionVars().MaxChunkSize)))
	base := newBaseExecutor(b.ctx, v.Schema(), v.ID(), childExec)
	base.initCap = n
	e := &LimitExec{
		baseExecutor: base,
		begin:        v.Offset,
		end:          v.Offset + v.Count,
	}

	childUsedSchema := markChildrenUsedCols(v.Schema(), v.Children()[0].Schema())[0]
	e.columnIdxsUsedByChild = make([]int, 0, len(childUsedSchema))
	for i, used := range childUsedSchema {
		if used {
			e.columnIdxsUsedByChild = append(e.columnIdxsUsedByChild, i)
		}
	}
	if len(e.columnIdxsUsedByChild) == len(childUsedSchema) {
		e.columnIdxsUsedByChild = nil // indicates that all columns are used. LimitExec will improve performance for this condition.
	}
	return e
}

func (b *executorBuilder) buildPrepare(v *plannercore.Prepare) Executor {
	base := newBaseExecutor(b.ctx, v.Schema(), v.ID())
	base.initCap = chunk.ZeroCapacity
	return &PrepareExec{
		baseExecutor: base,
		name:         v.Name,
		sqlText:      v.SQLText,
	}
}

func (b *executorBuilder) buildExecute(v *plannercore.Execute) Executor {
	e := &ExecuteExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
		is:           b.is,
		name:         v.Name,
		usingVars:    v.TxtProtoVars,
		id:           v.ExecID,
		stmt:         v.Stmt,
		plan:         v.Plan,
		outputNames:  v.OutputNames(),
	}

	failpoint.Inject("assertStaleReadValuesSameWithExecuteAndBuilder", func() {
		// This fail point is used to assert the behavior after refactoring is exactly the same with the previous implement.
		// Some variables in `plannercore.Execute` is deprecated and only be used for asserting now.
		if b.isStaleness != v.IsStaleness {
			panic(fmt.Sprintf("%v != %v", b.isStaleness, v.IsStaleness))
		}

		if b.readReplicaScope != v.ReadReplicaScope {
			panic(fmt.Sprintf("%s != %s", b.readReplicaScope, v.ReadReplicaScope))
		}

		if v.SnapshotTS != 0 {
			is, err := domain.GetDomain(b.ctx).GetSnapshotInfoSchema(v.SnapshotTS)
			if err != nil {
				panic(err)
			}

			if b.is.SchemaMetaVersion() != is.SchemaMetaVersion() {
				panic(fmt.Sprintf("%d != %d", b.is.SchemaMetaVersion(), is.SchemaMetaVersion()))
			}

			ts, err := sessiontxn.GetTxnManager(b.ctx).GetStmtReadTS()
			if err != nil {
				panic(e)
			}

			if v.SnapshotTS != ts {
				panic(fmt.Sprintf("%d != %d", ts, v.SnapshotTS))
			}
		}
	})

	failpoint.Inject("assertExecutePrepareStatementStalenessOption", func(val failpoint.Value) {
		vs := strings.Split(val.(string), "_")
		assertTS, assertTxnScope := vs[0], vs[1]
		staleread.AssertStmtStaleness(b.ctx, true)
		ts, err := sessiontxn.GetTxnManager(b.ctx).GetStmtReadTS()
		if err != nil {
			panic(e)
		}

		if strconv.FormatUint(ts, 10) != assertTS ||
			assertTxnScope != b.readReplicaScope {
			panic("execute prepare statement have wrong staleness option")
		}
	})

	return e
}

func (b *executorBuilder) buildShow(v *plannercore.PhysicalShow) Executor {
	e := &ShowExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
		Tp:           v.Tp,
		DBName:       model.NewCIStr(v.DBName),
		Table:        v.Table,
		Partition:    v.Partition,
		Column:       v.Column,
		IndexName:    v.IndexName,
		Flag:         v.Flag,
		Roles:        v.Roles,
		User:         v.User,
		is:           b.is,
		Full:         v.Full,
		IfNotExists:  v.IfNotExists,
		GlobalScope:  v.GlobalScope,
		Extended:     v.Extended,
		Extractor:    v.Extractor,
	}
	if e.Tp == ast.ShowMasterStatus {
		// show master status need start ts.
		if _, err := e.ctx.Txn(true); err != nil {
			b.err = err
		}
	}
	return e
}

func (b *executorBuilder) buildSimple(v *plannercore.Simple) Executor {
	switch s := v.Statement.(type) {
	case *ast.GrantStmt:
		return b.buildGrant(s)
	case *ast.RevokeStmt:
		return b.buildRevoke(s)
	case *ast.BRIEStmt:
		return b.buildBRIE(s, v.Schema())
	}
	base := newBaseExecutor(b.ctx, v.Schema(), v.ID())
	base.initCap = chunk.ZeroCapacity
	e := &SimpleExec{
		baseExecutor:    base,
		Statement:       v.Statement,
		IsFromRemote:    v.IsFromRemote,
		is:              b.is,
		staleTxnStartTS: v.StaleTxnStartTS,
	}
	return e
}

func (b *executorBuilder) buildSet(v *plannercore.Set) Executor {
	base := newBaseExecutor(b.ctx, v.Schema(), v.ID())
	base.initCap = chunk.ZeroCapacity
	e := &SetExecutor{
		baseExecutor: base,
		vars:         v.VarAssigns,
	}
	return e
}

func (b *executorBuilder) buildSetConfig(v *plannercore.SetConfig) Executor {
	return &SetConfigExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
		p:            v,
	}
}

func (b *executorBuilder) buildInsert(v *plannercore.Insert) Executor {
	b.inInsertStmt = true
	if v.SelectPlan != nil {
		// Try to update the forUpdateTS for insert/replace into select statements.
		// Set the selectPlan parameter to nil to make it always update the forUpdateTS.
		if b.err = b.updateForUpdateTSIfNeeded(nil); b.err != nil {
			return nil
		}
	}
	b.forUpdateTS = b.ctx.GetSessionVars().TxnCtx.GetForUpdateTS()
	selectExec := b.build(v.SelectPlan)
	if b.err != nil {
		return nil
	}
	var baseExec baseExecutor
	if selectExec != nil {
		baseExec = newBaseExecutor(b.ctx, nil, v.ID(), selectExec)
	} else {
		baseExec = newBaseExecutor(b.ctx, nil, v.ID())
	}
	baseExec.initCap = chunk.ZeroCapacity

	ivs := &InsertValues{
		baseExecutor:              baseExec,
		Table:                     v.Table,
		Columns:                   v.Columns,
		Lists:                     v.Lists,
		SetList:                   v.SetList,
		GenExprs:                  v.GenCols.Exprs,
		allAssignmentsAreConstant: v.AllAssignmentsAreConstant,
		hasRefCols:                v.NeedFillDefaultValue,
		SelectExec:                selectExec,
		rowLen:                    v.RowLen,
	}
	err := ivs.initInsertColumns()
	if err != nil {
		b.err = err
		return nil
	}

	if v.IsReplace {
		return b.buildReplace(ivs)
	}
	insert := &InsertExec{
		InsertValues: ivs,
		OnDuplicate:  append(v.OnDuplicate, v.GenCols.OnDuplicates...),
	}
	return insert
}

func (b *executorBuilder) buildLoadData(v *plannercore.LoadData) Executor {
	tbl, ok := b.is.TableByID(v.Table.TableInfo.ID)
	if !ok {
		b.err = errors.Errorf("Can not get table %d", v.Table.TableInfo.ID)
		return nil
	}
	insertVal := &InsertValues{
		baseExecutor: newBaseExecutor(b.ctx, nil, v.ID()),
		Table:        tbl,
		Columns:      v.Columns,
		GenExprs:     v.GenCols.Exprs,
		isLoadData:   true,
		txnInUse:     sync.Mutex{},
	}
	loadDataInfo := &LoadDataInfo{
		row:                make([]types.Datum, 0, len(insertVal.insertColumns)),
		InsertValues:       insertVal,
		Path:               v.Path,
		Table:              tbl,
		FieldsInfo:         v.FieldsInfo,
		LinesInfo:          v.LinesInfo,
		IgnoreLines:        v.IgnoreLines,
		ColumnAssignments:  v.ColumnAssignments,
		ColumnsAndUserVars: v.ColumnsAndUserVars,
		Ctx:                b.ctx,
	}
	columnNames := loadDataInfo.initFieldMappings()
	err := loadDataInfo.initLoadColumns(columnNames)
	if err != nil {
		b.err = err
		return nil
	}
	loadDataExec := &LoadDataExec{
		baseExecutor: newBaseExecutor(b.ctx, nil, v.ID()),
		IsLocal:      v.IsLocal,
		OnDuplicate:  v.OnDuplicate,
		loadDataInfo: loadDataInfo,
	}
	var defaultLoadDataBatchCnt uint64 = 20000 // TODO this will be changed to variable in another pr
	loadDataExec.loadDataInfo.InitQueues()
	loadDataExec.loadDataInfo.SetMaxRowsInBatch(defaultLoadDataBatchCnt)

	return loadDataExec
}

func (b *executorBuilder) buildLoadStats(v *plannercore.LoadStats) Executor {
	e := &LoadStatsExec{
		baseExecutor: newBaseExecutor(b.ctx, nil, v.ID()),
		info:         &LoadStatsInfo{v.Path, b.ctx},
	}
	return e
}

func (b *executorBuilder) buildIndexAdvise(v *plannercore.IndexAdvise) Executor {
	e := &IndexAdviseExec{
		baseExecutor: newBaseExecutor(b.ctx, nil, v.ID()),
		IsLocal:      v.IsLocal,
		indexAdviseInfo: &IndexAdviseInfo{
			Path:        v.Path,
			MaxMinutes:  v.MaxMinutes,
			MaxIndexNum: v.MaxIndexNum,
			LinesInfo:   v.LinesInfo,
			Ctx:         b.ctx,
		},
	}
	return e
}

func (b *executorBuilder) buildPlanReplayer(v *plannercore.PlanReplayer) Executor {
	if v.Load {
		e := &PlanReplayerLoadExec{
			baseExecutor: newBaseExecutor(b.ctx, nil, v.ID()),
			info:         &PlanReplayerLoadInfo{Path: v.File, Ctx: b.ctx},
		}
		return e
	}
	e := &PlanReplayerSingleExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
		ExecStmt:     v.ExecStmt,
		Analyze:      v.Analyze,
	}
	return e
}

func (b *executorBuilder) buildReplace(vals *InsertValues) Executor {
	replaceExec := &ReplaceExec{
		InsertValues: vals,
	}
	return replaceExec
}

func (b *executorBuilder) buildGrant(grant *ast.GrantStmt) Executor {
	e := &GrantExec{
		baseExecutor: newBaseExecutor(b.ctx, nil, 0),
		Privs:        grant.Privs,
		ObjectType:   grant.ObjectType,
		Level:        grant.Level,
		Users:        grant.Users,
		WithGrant:    grant.WithGrant,
		TLSOptions:   grant.TLSOptions,
		is:           b.is,
	}
	return e
}

func (b *executorBuilder) buildRevoke(revoke *ast.RevokeStmt) Executor {
	e := &RevokeExec{
		baseExecutor: newBaseExecutor(b.ctx, nil, 0),
		ctx:          b.ctx,
		Privs:        revoke.Privs,
		ObjectType:   revoke.ObjectType,
		Level:        revoke.Level,
		Users:        revoke.Users,
		is:           b.is,
	}
	return e
}

func (b *executorBuilder) buildDDL(v *plannercore.DDL) Executor {
	e := &DDLExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
		stmt:         v.Statement,
		is:           b.is,
		tempTableDDL: temptable.GetTemporaryTableDDL(b.ctx),
	}
	return e
}

// buildTrace builds a TraceExec for future executing. This method will be called
// at build().
func (b *executorBuilder) buildTrace(v *plannercore.Trace) Executor {
	t := &TraceExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
		stmtNode:     v.StmtNode,
		builder:      b,
		format:       v.Format,

		optimizerTrace:       v.OptimizerTrace,
		optimizerTraceTarget: v.OptimizerTraceTarget,
	}
	if t.format == plannercore.TraceFormatLog && !t.optimizerTrace {
		return &SortExec{
			baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID(), t),
			ByItems: []*plannerutil.ByItems{
				{Expr: &expression.Column{
					Index:   0,
					RetType: types.NewFieldType(mysql.TypeTimestamp),
				}},
			},
			schema: v.Schema(),
		}
	}
	return t
}

// buildExplain builds a explain executor. `e.rows` collects final result to `ExplainExec`.
func (b *executorBuilder) buildExplain(v *plannercore.Explain) Executor {
	explainExec := &ExplainExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
		explain:      v,
	}
	if v.Analyze {
		if b.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl == nil {
			b.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl = execdetails.NewRuntimeStatsColl(nil)
		}
		explainExec.analyzeExec = b.build(v.TargetPlan)
	}
	return explainExec
}

func (b *executorBuilder) buildSelectInto(v *plannercore.SelectInto) Executor {
	child := b.build(v.TargetPlan)
	if b.err != nil {
		return nil
	}
	return &SelectIntoExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID(), child),
		intoOpt:      v.IntoOpt,
	}
}

func (b *executorBuilder) buildUnionScanExec(v *plannercore.PhysicalUnionScan) Executor {
	reader := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}

	return b.buildUnionScanFromReader(reader, v)
}

// buildUnionScanFromReader builds union scan executor from child executor.
// Note that this function may be called by inner workers of index lookup join concurrently.
// Be careful to avoid data race.
func (b *executorBuilder) buildUnionScanFromReader(reader Executor, v *plannercore.PhysicalUnionScan) Executor {
	// If reader is union, it means a partition table and we should transfer as above.
	if x, ok := reader.(*UnionExec); ok {
		for i, child := range x.children {
			x.children[i] = b.buildUnionScanFromReader(child, v)
			if b.err != nil {
				return nil
			}
		}
		return x
	}
	us := &UnionScanExec{baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID(), reader)}
	// Get the handle column index of the below Plan.
	us.belowHandleCols = v.HandleCols
	us.mutableRow = chunk.MutRowFromTypes(retTypes(us))

	// If the push-downed condition contains virtual column, we may build a selection upon reader
	originReader := reader
	if sel, ok := reader.(*SelectionExec); ok {
		reader = sel.children[0]
	}

	us.collators = make([]collate.Collator, 0, len(us.columns))
	for _, tp := range retTypes(us) {
		us.collators = append(us.collators, collate.GetCollator(tp.GetCollate()))
	}

	startTS, err := b.getSnapshotTS()
	sessionVars := b.ctx.GetSessionVars()
	if err != nil {
		b.err = err
		return nil
	}

	switch x := reader.(type) {
	case *TableReaderExecutor:
		us.desc = x.desc
		us.conditions, us.conditionsWithVirCol = plannercore.SplitSelCondsWithVirtualColumn(v.Conditions)
		us.columns = x.columns
		us.table = x.table
		us.virtualColumnIndex = x.virtualColumnIndex
		us.handleCachedTable(b, x, sessionVars, startTS)
	case *IndexReaderExecutor:
		us.desc = x.desc
		for _, ic := range x.index.Columns {
			for i, col := range x.columns {
				if col.Name.L == ic.Name.L {
					us.usedIndex = append(us.usedIndex, i)
					break
				}
			}
		}
		us.conditions, us.conditionsWithVirCol = plannercore.SplitSelCondsWithVirtualColumn(v.Conditions)
		us.columns = x.columns
		us.table = x.table
		us.handleCachedTable(b, x, sessionVars, startTS)
	case *IndexLookUpExecutor:
		us.desc = x.desc
		for _, ic := range x.index.Columns {
			for i, col := range x.columns {
				if col.Name.L == ic.Name.L {
					us.usedIndex = append(us.usedIndex, i)
					break
				}
			}
		}
		us.conditions, us.conditionsWithVirCol = plannercore.SplitSelCondsWithVirtualColumn(v.Conditions)
		us.columns = x.columns
		us.table = x.table
		us.virtualColumnIndex = buildVirtualColumnIndex(us.Schema(), us.columns)
		us.handleCachedTable(b, x, sessionVars, startTS)
	case *IndexMergeReaderExecutor:
		// IndexMergeReader doesn't care order for now. So we will not set desc and useIndex.
		us.conditions, us.conditionsWithVirCol = plannercore.SplitSelCondsWithVirtualColumn(v.Conditions)
		us.columns = x.columns
		us.table = x.table
		us.virtualColumnIndex = buildVirtualColumnIndex(us.Schema(), us.columns)
	default:
		// The mem table will not be written by sql directly, so we can omit the union scan to avoid err reporting.
		return originReader
	}
	return us
}

type bypassDataSourceExecutor interface {
	dataSourceExecutor
	setDummy()
}

func (us *UnionScanExec) handleCachedTable(b *executorBuilder, x bypassDataSourceExecutor, vars *variable.SessionVars, startTS uint64) {
	tbl := x.Table()
	if tbl.Meta().TableCacheStatusType == model.TableCacheStatusEnable {
		cachedTable := tbl.(table.CachedTable)
		// Determine whether the cache can be used.
		leaseDuration := time.Duration(variable.TableCacheLease.Load()) * time.Second
		cacheData, loading := cachedTable.TryReadFromCache(startTS, leaseDuration)
		if cacheData != nil {
			vars.StmtCtx.ReadFromTableCache = true
			x.setDummy()
			us.cacheTable = cacheData
		} else if loading {
			// continue
		} else {
			if !b.inUpdateStmt && !b.inDeleteStmt && !b.inInsertStmt && !vars.StmtCtx.InExplainStmt {
				store := b.ctx.GetStore()
				cachedTable.UpdateLockForRead(context.Background(), store, startTS, leaseDuration)
			}
		}
	}
}

// buildMergeJoin builds MergeJoinExec executor.
func (b *executorBuilder) buildMergeJoin(v *plannercore.PhysicalMergeJoin) Executor {
	leftExec := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}

	rightExec := b.build(v.Children()[1])
	if b.err != nil {
		return nil
	}

	defaultValues := v.DefaultValues
	if defaultValues == nil {
		if v.JoinType == plannercore.RightOuterJoin {
			defaultValues = make([]types.Datum, leftExec.Schema().Len())
		} else {
			defaultValues = make([]types.Datum, rightExec.Schema().Len())
		}
	}

	e := &MergeJoinExec{
		stmtCtx:      b.ctx.GetSessionVars().StmtCtx,
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID(), leftExec, rightExec),
		compareFuncs: v.CompareFuncs,
		joiner: newJoiner(
			b.ctx,
			v.JoinType,
			v.JoinType == plannercore.RightOuterJoin,
			defaultValues,
			v.OtherConditions,
			retTypes(leftExec),
			retTypes(rightExec),
			markChildrenUsedCols(v.Schema(), v.Children()[0].Schema(), v.Children()[1].Schema()),
		),
		isOuterJoin: v.JoinType.IsOuterJoin(),
		desc:        v.Desc,
	}

	leftTable := &mergeJoinTable{
		childIndex: 0,
		joinKeys:   v.LeftJoinKeys,
		filters:    v.LeftConditions,
	}
	rightTable := &mergeJoinTable{
		childIndex: 1,
		joinKeys:   v.RightJoinKeys,
		filters:    v.RightConditions,
	}

	if v.JoinType == plannercore.RightOuterJoin {
		e.innerTable = leftTable
		e.outerTable = rightTable
	} else {
		e.innerTable = rightTable
		e.outerTable = leftTable
	}
	e.innerTable.isInner = true

	// optimizer should guarantee that filters on inner table are pushed down
	// to tikv or extracted to a Selection.
	if len(e.innerTable.filters) != 0 {
		b.err = errors.Annotate(ErrBuildExecutor, "merge join's inner filter should be empty.")
		return nil
	}

	executorCounterMergeJoinExec.Inc()
	return e
}

func (b *executorBuilder) buildSideEstCount(v *plannercore.PhysicalHashJoin) float64 {
	buildSide := v.Children()[v.InnerChildIdx]
	if v.UseOuterToBuild {
		buildSide = v.Children()[1-v.InnerChildIdx]
	}
	if buildSide.Stats().HistColl == nil || buildSide.Stats().HistColl.Pseudo {
		return 0.0
	}
	return buildSide.StatsCount()
}

func (b *executorBuilder) buildHashJoin(v *plannercore.PhysicalHashJoin) Executor {
	leftExec := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}

	rightExec := b.build(v.Children()[1])
	if b.err != nil {
		return nil
	}

	e := &HashJoinExec{
		baseExecutor:    newBaseExecutor(b.ctx, v.Schema(), v.ID(), leftExec, rightExec),
		concurrency:     v.Concurrency,
		joinType:        v.JoinType,
		isOuterJoin:     v.JoinType.IsOuterJoin(),
		useOuterToBuild: v.UseOuterToBuild,
	}
	defaultValues := v.DefaultValues
	lhsTypes, rhsTypes := retTypes(leftExec), retTypes(rightExec)
	if v.InnerChildIdx == 1 {
		if len(v.RightConditions) > 0 {
			b.err = errors.Annotate(ErrBuildExecutor, "join's inner condition should be empty")
			return nil
		}
	} else {
		if len(v.LeftConditions) > 0 {
			b.err = errors.Annotate(ErrBuildExecutor, "join's inner condition should be empty")
			return nil
		}
	}

	leftIsBuildSide := true

	e.isNullEQ = v.IsNullEQ
	if v.UseOuterToBuild {
		// update the buildSideEstCount due to changing the build side
		if v.InnerChildIdx == 1 {
			e.buildSideExec, e.buildKeys = leftExec, v.LeftJoinKeys
			e.probeSideExec, e.probeKeys = rightExec, v.RightJoinKeys
			e.outerFilter = v.LeftConditions
		} else {
			e.buildSideExec, e.buildKeys = rightExec, v.RightJoinKeys
			e.probeSideExec, e.probeKeys = leftExec, v.LeftJoinKeys
			e.outerFilter = v.RightConditions
			leftIsBuildSide = false
		}
		if defaultValues == nil {
			defaultValues = make([]types.Datum, e.probeSideExec.Schema().Len())
		}
	} else {
		if v.InnerChildIdx == 0 {
			e.buildSideExec, e.buildKeys = leftExec, v.LeftJoinKeys
			e.probeSideExec, e.probeKeys = rightExec, v.RightJoinKeys
			e.outerFilter = v.RightConditions
		} else {
			e.buildSideExec, e.buildKeys = rightExec, v.RightJoinKeys
			e.probeSideExec, e.probeKeys = leftExec, v.LeftJoinKeys
			e.outerFilter = v.LeftConditions
			leftIsBuildSide = false
		}
		if defaultValues == nil {
			defaultValues = make([]types.Datum, e.buildSideExec.Schema().Len())
		}
	}
	e.buildSideEstCount = b.buildSideEstCount(v)
	childrenUsedSchema := markChildrenUsedCols(v.Schema(), v.Children()[0].Schema(), v.Children()[1].Schema())
	e.joiners = make([]joiner, e.concurrency)
	for i := uint(0); i < e.concurrency; i++ {
		e.joiners[i] = newJoiner(b.ctx, v.JoinType, v.InnerChildIdx == 0, defaultValues,
			v.OtherConditions, lhsTypes, rhsTypes, childrenUsedSchema)
	}
	executorCountHashJoinExec.Inc()

	// We should use JoinKey to construct the type information using by hashing, instead of using the child's schema directly.
	// When a hybrid type column is hashed multiple times, we need to distinguish what field types are used.
	// For example, the condition `enum = int and enum = string`, we should use ETInt to hash the first column,
	// and use ETString to hash the second column, although they may be the same column.
	leftExecTypes, rightExecTypes := retTypes(leftExec), retTypes(rightExec)
	leftTypes, rightTypes := make([]*types.FieldType, 0, len(v.LeftJoinKeys)), make([]*types.FieldType, 0, len(v.RightJoinKeys))
	for i, col := range v.LeftJoinKeys {
		leftTypes = append(leftTypes, leftExecTypes[col.Index].Clone())
		leftTypes[i].SetFlag(col.RetType.GetFlag())
	}
	for i, col := range v.RightJoinKeys {
		rightTypes = append(rightTypes, rightExecTypes[col.Index].Clone())
		rightTypes[i].SetFlag(col.RetType.GetFlag())
	}

	// consider collations
	for i := range v.EqualConditions {
		chs, coll := v.EqualConditions[i].CharsetAndCollation()
		leftTypes[i].SetCharset(chs)
		leftTypes[i].SetCollate(coll)
		rightTypes[i].SetCharset(chs)
		rightTypes[i].SetCollate(coll)
	}
	if leftIsBuildSide {
		e.buildTypes, e.probeTypes = leftTypes, rightTypes
	} else {
		e.buildTypes, e.probeTypes = rightTypes, leftTypes
	}
	return e
}

func (b *executorBuilder) buildHashAgg(v *plannercore.PhysicalHashAgg) Executor {
	src := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	sessionVars := b.ctx.GetSessionVars()
	e := &HashAggExec{
		baseExecutor:    newBaseExecutor(b.ctx, v.Schema(), v.ID(), src),
		sc:              sessionVars.StmtCtx,
		PartialAggFuncs: make([]aggfuncs.AggFunc, 0, len(v.AggFuncs)),
		GroupByItems:    v.GroupByItems,
	}
	// We take `create table t(a int, b int);` as example.
	//
	// 1. If all the aggregation functions are FIRST_ROW, we do not need to set the defaultVal for them:
	// e.g.
	// mysql> select distinct a, b from t;
	// 0 rows in set (0.00 sec)
	//
	// 2. If there exists group by items, we do not need to set the defaultVal for them either:
	// e.g.
	// mysql> select avg(a) from t group by b;
	// Empty set (0.00 sec)
	//
	// mysql> select avg(a) from t group by a;
	// +--------+
	// | avg(a) |
	// +--------+
	// |  NULL  |
	// +--------+
	// 1 row in set (0.00 sec)
	if len(v.GroupByItems) != 0 || aggregation.IsAllFirstRow(v.AggFuncs) {
		e.defaultVal = nil
	} else {
		e.defaultVal = chunk.NewChunkWithCapacity(retTypes(e), 1)
	}
	for _, aggDesc := range v.AggFuncs {
		if aggDesc.HasDistinct || len(aggDesc.OrderByItems) > 0 {
			e.isUnparallelExec = true
		}
	}
	// When we set both tidb_hashagg_final_concurrency and tidb_hashagg_partial_concurrency to 1,
	// we do not need to parallelly execute hash agg,
	// and this action can be a workaround when meeting some unexpected situation using parallelExec.
	if finalCon, partialCon := sessionVars.HashAggFinalConcurrency(), sessionVars.HashAggPartialConcurrency(); finalCon <= 0 || partialCon <= 0 || finalCon == 1 && partialCon == 1 {
		e.isUnparallelExec = true
	}
	partialOrdinal := 0
	for i, aggDesc := range v.AggFuncs {
		if e.isUnparallelExec {
			e.PartialAggFuncs = append(e.PartialAggFuncs, aggfuncs.Build(b.ctx, aggDesc, i))
		} else {
			ordinal := []int{partialOrdinal}
			partialOrdinal++
			if aggDesc.Name == ast.AggFuncAvg {
				ordinal = append(ordinal, partialOrdinal+1)
				partialOrdinal++
			}
			partialAggDesc, finalDesc := aggDesc.Split(ordinal)
			partialAggFunc := aggfuncs.Build(b.ctx, partialAggDesc, i)
			finalAggFunc := aggfuncs.Build(b.ctx, finalDesc, i)
			e.PartialAggFuncs = append(e.PartialAggFuncs, partialAggFunc)
			e.FinalAggFuncs = append(e.FinalAggFuncs, finalAggFunc)
			if partialAggDesc.Name == ast.AggFuncGroupConcat {
				// For group_concat, finalAggFunc and partialAggFunc need shared `truncate` flag to do duplicate.
				finalAggFunc.(interface{ SetTruncated(t *int32) }).SetTruncated(
					partialAggFunc.(interface{ GetTruncated() *int32 }).GetTruncated(),
				)
			}
		}
		if e.defaultVal != nil {
			value := aggDesc.GetDefaultValue()
			e.defaultVal.AppendDatum(i, &value)
		}
	}

	executorCounterHashAggExec.Inc()
	return e
}

func (b *executorBuilder) buildStreamAgg(v *plannercore.PhysicalStreamAgg) Executor {
	src := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	e := &StreamAggExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID(), src),
		groupChecker: newVecGroupChecker(b.ctx, v.GroupByItems),
		aggFuncs:     make([]aggfuncs.AggFunc, 0, len(v.AggFuncs)),
	}
	if len(v.GroupByItems) != 0 || aggregation.IsAllFirstRow(v.AggFuncs) {
		e.defaultVal = nil
	} else {
		e.defaultVal = chunk.NewChunkWithCapacity(retTypes(e), 1)
	}
	for i, aggDesc := range v.AggFuncs {
		aggFunc := aggfuncs.Build(b.ctx, aggDesc, i)
		e.aggFuncs = append(e.aggFuncs, aggFunc)
		if e.defaultVal != nil {
			value := aggDesc.GetDefaultValue()
			e.defaultVal.AppendDatum(i, &value)
		}
	}

	executorStreamAggExec.Inc()
	return e
}

func (b *executorBuilder) buildSelection(v *plannercore.PhysicalSelection) Executor {
	childExec := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	e := &SelectionExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID(), childExec),
		filters:      v.Conditions,
	}
	return e
}

func (b *executorBuilder) buildProjection(v *plannercore.PhysicalProjection) Executor {
	childExec := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	e := &ProjectionExec{
		baseExecutor:     newBaseExecutor(b.ctx, v.Schema(), v.ID(), childExec),
		numWorkers:       int64(b.ctx.GetSessionVars().ProjectionConcurrency()),
		evaluatorSuit:    expression.NewEvaluatorSuite(v.Exprs, v.AvoidColumnEvaluator),
		calculateNoDelay: v.CalculateNoDelay,
	}

	// If the calculation row count for this Projection operator is smaller
	// than a Chunk size, we turn back to the un-parallel Projection
	// implementation to reduce the goroutine overhead.
	if int64(v.StatsCount()) < int64(b.ctx.GetSessionVars().MaxChunkSize) {
		e.numWorkers = 0
	}

	// Use un-parallel projection for query that write on memdb to avoid data race.
	// See also https://github.com/pingcap/tidb/issues/26832
	if b.inUpdateStmt || b.inDeleteStmt || b.inInsertStmt || b.hasLock {
		e.numWorkers = 0
	}
	return e
}

func (b *executorBuilder) buildTableDual(v *plannercore.PhysicalTableDual) Executor {
	if v.RowCount != 0 && v.RowCount != 1 {
		b.err = errors.Errorf("buildTableDual failed, invalid row count for dual table: %v", v.RowCount)
		return nil
	}
	base := newBaseExecutor(b.ctx, v.Schema(), v.ID())
	base.initCap = v.RowCount
	e := &TableDualExec{
		baseExecutor: base,
		numDualRows:  v.RowCount,
	}
	return e
}

// `getSnapshotTS` returns for-update-ts if in insert/update/delete/lock statement otherwise the isolation read ts
// Please notice that in RC isolation, the above two ts are the same
func (b *executorBuilder) getSnapshotTS() (uint64, error) {
	if b.forDataReaderBuilder {
		return b.dataReaderTS, nil
	}

	txnManager := sessiontxn.GetTxnManager(b.ctx)
	if b.inInsertStmt || b.inUpdateStmt || b.inDeleteStmt || b.inSelectLockStmt {
		return txnManager.GetStmtForUpdateTS()
	}
	return txnManager.GetStmtReadTS()
}

// getReadTS returns the ts used by select (without for-update clause). The return value is affected by the isolation level
// and some stale/historical read contexts. For example, it will return txn.StartTS in RR and return
// the current timestamp in RC isolation
func (b *executorBuilder) getReadTS() (uint64, error) {
	// `refreshForUpdateTSForRC` should always be invoked before returning the cached value to
	// ensure the correct value is returned even the `snapshotTS` field is already set by other
	// logics. However for `IndexLookUpMergeJoin` and `IndexLookUpHashJoin`, it requires caching the
	// snapshotTS and and may even use it after the txn being destroyed. In this case, mark
	// `snapshotTSCached` to skip `refreshForUpdateTSForRC`.
	failpoint.Inject("assertNotStaleReadForExecutorGetReadTS", func() {
		// after refactoring stale read will use its own context provider
		staleread.AssertStmtStaleness(b.ctx, false)
	})

	if b.snapshotTSCached {
		return b.snapshotTS, nil
	}

	if snapshotTS := b.ctx.GetSessionVars().SnapshotTS; snapshotTS != 0 {
		b.snapshotTS = snapshotTS
		b.snapshotTSCached = true
		return snapshotTS, nil
	}

	if b.ctx.GetSessionVars().IsPessimisticReadConsistency() {
		if err := b.refreshForUpdateTSForRC(); err != nil {
			return 0, err
		}
	}

	if b.snapshotTS != 0 {
		b.snapshotTSCached = true
		// Return the cached value.
		return b.snapshotTS, nil
	}

	txn, err := b.ctx.Txn(true)
	if err != nil {
		return 0, err
	}

	b.snapshotTS = txn.StartTS()
	if b.snapshotTS == 0 {
		return 0, errors.Trace(ErrGetStartTS)
	}
	b.snapshotTSCached = true
	return b.snapshotTS, nil
}

func (b *executorBuilder) buildMemTable(v *plannercore.PhysicalMemTable) Executor {
	switch v.DBName.L {
	case util.MetricSchemaName.L:
		return &MemTableReaderExec{
			baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
			table:        v.Table,
			retriever: &MetricRetriever{
				table:     v.Table,
				extractor: v.Extractor.(*plannercore.MetricTableExtractor),
			},
		}
	case util.InformationSchemaName.L:
		switch v.Table.Name.L {
		case strings.ToLower(infoschema.TableClusterConfig):
			return &MemTableReaderExec{
				baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
				table:        v.Table,
				retriever: &clusterConfigRetriever{
					extractor: v.Extractor.(*plannercore.ClusterTableExtractor),
				},
			}
		case strings.ToLower(infoschema.TableClusterLoad):
			return &MemTableReaderExec{
				baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
				table:        v.Table,
				retriever: &clusterServerInfoRetriever{
					extractor:      v.Extractor.(*plannercore.ClusterTableExtractor),
					serverInfoType: diagnosticspb.ServerInfoType_LoadInfo,
				},
			}
		case strings.ToLower(infoschema.TableClusterHardware):
			return &MemTableReaderExec{
				baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
				table:        v.Table,
				retriever: &clusterServerInfoRetriever{
					extractor:      v.Extractor.(*plannercore.ClusterTableExtractor),
					serverInfoType: diagnosticspb.ServerInfoType_HardwareInfo,
				},
			}
		case strings.ToLower(infoschema.TableClusterSystemInfo):
			return &MemTableReaderExec{
				baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
				table:        v.Table,
				retriever: &clusterServerInfoRetriever{
					extractor:      v.Extractor.(*plannercore.ClusterTableExtractor),
					serverInfoType: diagnosticspb.ServerInfoType_SystemInfo,
				},
			}
		case strings.ToLower(infoschema.TableClusterLog):
			return &MemTableReaderExec{
				baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
				table:        v.Table,
				retriever: &clusterLogRetriever{
					extractor: v.Extractor.(*plannercore.ClusterLogTableExtractor),
				},
			}
		case strings.ToLower(infoschema.TableTiDBHotRegionsHistory):
			return &MemTableReaderExec{
				baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
				table:        v.Table,
				retriever: &hotRegionsHistoryRetriver{
					extractor: v.Extractor.(*plannercore.HotRegionsHistoryTableExtractor),
				},
			}
		case strings.ToLower(infoschema.TableInspectionResult):
			return &MemTableReaderExec{
				baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
				table:        v.Table,
				retriever: &inspectionResultRetriever{
					extractor: v.Extractor.(*plannercore.InspectionResultTableExtractor),
					timeRange: v.QueryTimeRange,
				},
			}
		case strings.ToLower(infoschema.TableInspectionSummary):
			return &MemTableReaderExec{
				baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
				table:        v.Table,
				retriever: &inspectionSummaryRetriever{
					table:     v.Table,
					extractor: v.Extractor.(*plannercore.InspectionSummaryTableExtractor),
					timeRange: v.QueryTimeRange,
				},
			}
		case strings.ToLower(infoschema.TableInspectionRules):
			return &MemTableReaderExec{
				baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
				table:        v.Table,
				retriever: &inspectionRuleRetriever{
					extractor: v.Extractor.(*plannercore.InspectionRuleTableExtractor),
				},
			}
		case strings.ToLower(infoschema.TableMetricSummary):
			return &MemTableReaderExec{
				baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
				table:        v.Table,
				retriever: &MetricsSummaryRetriever{
					table:     v.Table,
					extractor: v.Extractor.(*plannercore.MetricSummaryTableExtractor),
					timeRange: v.QueryTimeRange,
				},
			}
		case strings.ToLower(infoschema.TableMetricSummaryByLabel):
			return &MemTableReaderExec{
				baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
				table:        v.Table,
				retriever: &MetricsSummaryByLabelRetriever{
					table:     v.Table,
					extractor: v.Extractor.(*plannercore.MetricSummaryTableExtractor),
					timeRange: v.QueryTimeRange,
				},
			}
		case strings.ToLower(infoschema.TableTiKVRegionPeers):
			return &MemTableReaderExec{
				baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
				table:        v.Table,
				retriever: &tikvRegionPeersRetriever{
					extractor: v.Extractor.(*plannercore.TikvRegionPeersExtractor),
				},
			}
		case strings.ToLower(infoschema.TableSchemata),
			strings.ToLower(infoschema.TableStatistics),
			strings.ToLower(infoschema.TableTiDBIndexes),
			strings.ToLower(infoschema.TableViews),
			strings.ToLower(infoschema.TableTables),
			strings.ToLower(infoschema.TableReferConst),
			strings.ToLower(infoschema.TableSequences),
			strings.ToLower(infoschema.TablePartitions),
			strings.ToLower(infoschema.TableEngines),
			strings.ToLower(infoschema.TableCollations),
			strings.ToLower(infoschema.TableAnalyzeStatus),
			strings.ToLower(infoschema.TableClusterInfo),
			strings.ToLower(infoschema.TableProfiling),
			strings.ToLower(infoschema.TableCharacterSets),
			strings.ToLower(infoschema.TableKeyColumn),
			strings.ToLower(infoschema.TableUserPrivileges),
			strings.ToLower(infoschema.TableMetricTables),
			strings.ToLower(infoschema.TableCollationCharacterSetApplicability),
			strings.ToLower(infoschema.TableProcesslist),
			strings.ToLower(infoschema.ClusterTableProcesslist),
			strings.ToLower(infoschema.TableTiKVRegionStatus),
			strings.ToLower(infoschema.TableTiDBHotRegions),
			strings.ToLower(infoschema.TableSessionVar),
			strings.ToLower(infoschema.TableConstraints),
			strings.ToLower(infoschema.TableTiFlashReplica),
			strings.ToLower(infoschema.TableTiDBServersInfo),
			strings.ToLower(infoschema.TableTiKVStoreStatus),
			strings.ToLower(infoschema.TableStatementsSummaryEvicted),
			strings.ToLower(infoschema.ClusterTableStatementsSummaryEvicted),
			strings.ToLower(infoschema.TableClientErrorsSummaryGlobal),
			strings.ToLower(infoschema.TableClientErrorsSummaryByUser),
			strings.ToLower(infoschema.TableClientErrorsSummaryByHost),
			strings.ToLower(infoschema.TableAttributes),
			strings.ToLower(infoschema.TablePlacementPolicies):
			return &MemTableReaderExec{
				baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
				table:        v.Table,
				retriever: &memtableRetriever{
					table:     v.Table,
					columns:   v.Columns,
					extractor: v.Extractor,
				},
			}
		case strings.ToLower(infoschema.TableTiDBTrx),
			strings.ToLower(infoschema.ClusterTableTiDBTrx):
			return &MemTableReaderExec{
				baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
				table:        v.Table,
				retriever: &tidbTrxTableRetriever{
					table:   v.Table,
					columns: v.Columns,
				},
			}
		case strings.ToLower(infoschema.TableDataLockWaits):
			return &MemTableReaderExec{
				baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
				table:        v.Table,
				retriever: &dataLockWaitsTableRetriever{
					table:   v.Table,
					columns: v.Columns,
				},
			}
		case strings.ToLower(infoschema.TableDeadlocks),
			strings.ToLower(infoschema.ClusterTableDeadlocks):
			return &MemTableReaderExec{
				baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
				table:        v.Table,
				retriever: &deadlocksTableRetriever{
					table:   v.Table,
					columns: v.Columns,
				},
			}
		case strings.ToLower(infoschema.TableStatementsSummary),
			strings.ToLower(infoschema.TableStatementsSummaryHistory),
			strings.ToLower(infoschema.ClusterTableStatementsSummaryHistory),
			strings.ToLower(infoschema.ClusterTableStatementsSummary):
			return &MemTableReaderExec{
				baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
				table:        v.Table,
				retriever: &stmtSummaryTableRetriever{
					table:     v.Table,
					columns:   v.Columns,
					extractor: v.Extractor.(*plannercore.StatementsSummaryExtractor),
				},
			}
		case strings.ToLower(infoschema.TableColumns):
			return &MemTableReaderExec{
				baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
				table:        v.Table,
				retriever: &hugeMemTableRetriever{
					table:              v.Table,
					columns:            v.Columns,
					extractor:          v.Extractor.(*plannercore.ColumnsTableExtractor),
					viewSchemaMap:      make(map[int64]*expression.Schema),
					viewOutputNamesMap: make(map[int64]types.NameSlice),
				},
			}

		case strings.ToLower(infoschema.TableSlowQuery), strings.ToLower(infoschema.ClusterTableSlowLog):
			memTracker := memory.NewTracker(v.ID(), -1)
			memTracker.AttachTo(b.ctx.GetSessionVars().StmtCtx.MemTracker)
			return &MemTableReaderExec{
				baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
				table:        v.Table,
				retriever: &slowQueryRetriever{
					table:      v.Table,
					outputCols: v.Columns,
					extractor:  v.Extractor.(*plannercore.SlowQueryExtractor),
					memTracker: memTracker,
				},
			}
		case strings.ToLower(infoschema.TableStorageStats):
			return &MemTableReaderExec{
				baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
				table:        v.Table,
				retriever: &tableStorageStatsRetriever{
					table:      v.Table,
					outputCols: v.Columns,
					extractor:  v.Extractor.(*plannercore.TableStorageStatsExtractor),
				},
			}
		case strings.ToLower(infoschema.TableDDLJobs):
			loc := b.ctx.GetSessionVars().Location()
			ddlJobRetriever := DDLJobRetriever{TZLoc: loc}
			return &DDLJobsReaderExec{
				baseExecutor:    newBaseExecutor(b.ctx, v.Schema(), v.ID()),
				is:              b.is,
				DDLJobRetriever: ddlJobRetriever,
			}
		case strings.ToLower(infoschema.TableTiFlashTables),
			strings.ToLower(infoschema.TableTiFlashSegments):
			return &MemTableReaderExec{
				baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
				table:        v.Table,
				retriever: &TiFlashSystemTableRetriever{
					table:      v.Table,
					outputCols: v.Columns,
					extractor:  v.Extractor.(*plannercore.TiFlashSystemTableExtractor),
				},
			}
		}
	}
	tb, _ := b.is.TableByID(v.Table.ID)
	return &TableScanExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
		t:            tb,
		columns:      v.Columns,
	}
}

func (b *executorBuilder) buildSort(v *plannercore.PhysicalSort) Executor {
	childExec := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	sortExec := SortExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID(), childExec),
		ByItems:      v.ByItems,
		schema:       v.Schema(),
	}
	executorCounterSortExec.Inc()
	return &sortExec
}

func (b *executorBuilder) buildTopN(v *plannercore.PhysicalTopN) Executor {
	childExec := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	sortExec := SortExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID(), childExec),
		ByItems:      v.ByItems,
		schema:       v.Schema(),
	}
	executorCounterTopNExec.Inc()
	return &TopNExec{
		SortExec: sortExec,
		limit:    &plannercore.PhysicalLimit{Count: v.Count, Offset: v.Offset},
	}
}

func (b *executorBuilder) buildApply(v *plannercore.PhysicalApply) Executor {
	var (
		innerPlan plannercore.PhysicalPlan
		outerPlan plannercore.PhysicalPlan
	)
	if v.InnerChildIdx == 0 {
		innerPlan = v.Children()[0]
		outerPlan = v.Children()[1]
	} else {
		innerPlan = v.Children()[1]
		outerPlan = v.Children()[0]
	}
	v.OuterSchema = plannercore.ExtractCorColumnsBySchema4PhysicalPlan(innerPlan, outerPlan.Schema())
	leftChild := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	rightChild := b.build(v.Children()[1])
	if b.err != nil {
		return nil
	}
	otherConditions := append(expression.ScalarFuncs2Exprs(v.EqualConditions), v.OtherConditions...)
	defaultValues := v.DefaultValues
	if defaultValues == nil {
		defaultValues = make([]types.Datum, v.Children()[v.InnerChildIdx].Schema().Len())
	}
	outerExec, innerExec := leftChild, rightChild
	outerFilter, innerFilter := v.LeftConditions, v.RightConditions
	if v.InnerChildIdx == 0 {
		outerExec, innerExec = rightChild, leftChild
		outerFilter, innerFilter = v.RightConditions, v.LeftConditions
	}
	tupleJoiner := newJoiner(b.ctx, v.JoinType, v.InnerChildIdx == 0,
		defaultValues, otherConditions, retTypes(leftChild), retTypes(rightChild), nil)
	serialExec := &NestedLoopApplyExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID(), outerExec, innerExec),
		innerExec:    innerExec,
		outerExec:    outerExec,
		outerFilter:  outerFilter,
		innerFilter:  innerFilter,
		outer:        v.JoinType != plannercore.InnerJoin,
		joiner:       tupleJoiner,
		outerSchema:  v.OuterSchema,
		ctx:          b.ctx,
		canUseCache:  v.CanUseCache,
	}
	executorCounterNestedLoopApplyExec.Inc()

	// try parallel mode
	if v.Concurrency > 1 {
		innerExecs := make([]Executor, 0, v.Concurrency)
		innerFilters := make([]expression.CNFExprs, 0, v.Concurrency)
		corCols := make([][]*expression.CorrelatedColumn, 0, v.Concurrency)
		joiners := make([]joiner, 0, v.Concurrency)
		for i := 0; i < v.Concurrency; i++ {
			clonedInnerPlan, err := plannercore.SafeClone(innerPlan)
			if err != nil {
				b.err = nil
				return serialExec
			}
			corCol := plannercore.ExtractCorColumnsBySchema4PhysicalPlan(clonedInnerPlan, outerPlan.Schema())
			clonedInnerExec := b.build(clonedInnerPlan)
			if b.err != nil {
				b.err = nil
				return serialExec
			}
			innerExecs = append(innerExecs, clonedInnerExec)
			corCols = append(corCols, corCol)
			innerFilters = append(innerFilters, innerFilter.Clone())
			joiners = append(joiners, newJoiner(b.ctx, v.JoinType, v.InnerChildIdx == 0,
				defaultValues, otherConditions, retTypes(leftChild), retTypes(rightChild), nil))
		}

		allExecs := append([]Executor{outerExec}, innerExecs...)

		return &ParallelNestedLoopApplyExec{
			baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID(), allExecs...),
			innerExecs:   innerExecs,
			outerExec:    outerExec,
			outerFilter:  outerFilter,
			innerFilter:  innerFilters,
			outer:        v.JoinType != plannercore.InnerJoin,
			joiners:      joiners,
			corCols:      corCols,
			concurrency:  v.Concurrency,
			useCache:     v.CanUseCache,
		}
	}
	return serialExec
}

func (b *executorBuilder) buildMaxOneRow(v *plannercore.PhysicalMaxOneRow) Executor {
	childExec := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	base := newBaseExecutor(b.ctx, v.Schema(), v.ID(), childExec)
	base.initCap = 2
	base.maxChunkSize = 2
	e := &MaxOneRowExec{baseExecutor: base}
	return e
}

func (b *executorBuilder) buildUnionAll(v *plannercore.PhysicalUnionAll) Executor {
	childExecs := make([]Executor, len(v.Children()))
	for i, child := range v.Children() {
		childExecs[i] = b.build(child)
		if b.err != nil {
			return nil
		}
	}
	e := &UnionExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID(), childExecs...),
		concurrency:  b.ctx.GetSessionVars().UnionConcurrency(),
	}
	return e
}

func buildHandleColsForSplit(sc *stmtctx.StatementContext, tbInfo *model.TableInfo) plannercore.HandleCols {
	if tbInfo.IsCommonHandle {
		primaryIdx := tables.FindPrimaryIndex(tbInfo)
		tableCols := make([]*expression.Column, len(tbInfo.Columns))
		for i, col := range tbInfo.Columns {
			tableCols[i] = &expression.Column{
				ID:      col.ID,
				RetType: &col.FieldType,
			}
		}
		for i, pkCol := range primaryIdx.Columns {
			tableCols[pkCol.Offset].Index = i
		}
		return plannercore.NewCommonHandleCols(sc, tbInfo, primaryIdx, tableCols)
	}
	intCol := &expression.Column{
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}
	return plannercore.NewIntHandleCols(intCol)
}

func (b *executorBuilder) buildSplitRegion(v *plannercore.SplitRegion) Executor {
	base := newBaseExecutor(b.ctx, v.Schema(), v.ID())
	base.initCap = 1
	base.maxChunkSize = 1
	if v.IndexInfo != nil {
		return &SplitIndexRegionExec{
			baseExecutor:   base,
			tableInfo:      v.TableInfo,
			partitionNames: v.PartitionNames,
			indexInfo:      v.IndexInfo,
			lower:          v.Lower,
			upper:          v.Upper,
			num:            v.Num,
			valueLists:     v.ValueLists,
		}
	}
	handleCols := buildHandleColsForSplit(b.ctx.GetSessionVars().StmtCtx, v.TableInfo)
	if len(v.ValueLists) > 0 {
		return &SplitTableRegionExec{
			baseExecutor:   base,
			tableInfo:      v.TableInfo,
			partitionNames: v.PartitionNames,
			handleCols:     handleCols,
			valueLists:     v.ValueLists,
		}
	}
	return &SplitTableRegionExec{
		baseExecutor:   base,
		tableInfo:      v.TableInfo,
		partitionNames: v.PartitionNames,
		handleCols:     handleCols,
		lower:          v.Lower,
		upper:          v.Upper,
		num:            v.Num,
	}
}

func (b *executorBuilder) buildUpdate(v *plannercore.Update) Executor {
	b.inUpdateStmt = true
	tblID2table := make(map[int64]table.Table, len(v.TblColPosInfos))
	multiUpdateOnSameTable := make(map[int64]bool)
	for _, info := range v.TblColPosInfos {
		tbl, _ := b.is.TableByID(info.TblID)
		if _, ok := tblID2table[info.TblID]; ok {
			multiUpdateOnSameTable[info.TblID] = true
		}
		tblID2table[info.TblID] = tbl
		if len(v.PartitionedTable) > 0 {
			// The v.PartitionedTable collects the partitioned table.
			// Replace the original table with the partitioned table to support partition selection.
			// e.g. update t partition (p0, p1), the new values are not belong to the given set p0, p1
			// Using the table in v.PartitionedTable returns a proper error, while using the original table can't.
			for _, p := range v.PartitionedTable {
				if info.TblID == p.Meta().ID {
					tblID2table[info.TblID] = p
				}
			}
		}
	}
	if b.err = b.updateForUpdateTSIfNeeded(v.SelectPlan); b.err != nil {
		return nil
	}
	b.forUpdateTS = b.ctx.GetSessionVars().TxnCtx.GetForUpdateTS()
	selExec := b.build(v.SelectPlan)
	if b.err != nil {
		return nil
	}
	base := newBaseExecutor(b.ctx, v.Schema(), v.ID(), selExec)
	base.initCap = chunk.ZeroCapacity
	var assignFlag []int
	assignFlag, b.err = getAssignFlag(b.ctx, v, selExec.Schema().Len())
	if b.err != nil {
		return nil
	}
	// should use the new tblID2table, since the update's schema may have been changed in Execstmt.
	b.err = plannercore.CheckUpdateList(assignFlag, v, tblID2table)
	if b.err != nil {
		return nil
	}
	updateExec := &UpdateExec{
		baseExecutor:              base,
		OrderedList:               v.OrderedList,
		allAssignmentsAreConstant: v.AllAssignmentsAreConstant,
		virtualAssignmentsOffset:  v.VirtualAssignmentsOffset,
		multiUpdateOnSameTable:    multiUpdateOnSameTable,
		tblID2table:               tblID2table,
		tblColPosInfos:            v.TblColPosInfos,
		assignFlag:                assignFlag,
	}
	return updateExec
}

func getAssignFlag(ctx sessionctx.Context, v *plannercore.Update, schemaLen int) ([]int, error) {
	assignFlag := make([]int, schemaLen)
	for i := range assignFlag {
		assignFlag[i] = -1
	}
	for _, assign := range v.OrderedList {
		if !ctx.GetSessionVars().AllowWriteRowID && assign.Col.ID == model.ExtraHandleID {
			return nil, errors.Errorf("insert, update and replace statements for _tidb_rowid are not supported")
		}
		tblIdx, found := v.TblColPosInfos.FindTblIdx(assign.Col.Index)
		if found {
			colIdx := assign.Col.Index
			assignFlag[colIdx] = tblIdx
		}
	}
	return assignFlag, nil
}

func (b *executorBuilder) buildDelete(v *plannercore.Delete) Executor {
	b.inDeleteStmt = true
	tblID2table := make(map[int64]table.Table, len(v.TblColPosInfos))
	for _, info := range v.TblColPosInfos {
		tblID2table[info.TblID], _ = b.is.TableByID(info.TblID)
	}
	if b.err = b.updateForUpdateTSIfNeeded(v.SelectPlan); b.err != nil {
		return nil
	}
	b.forUpdateTS = b.ctx.GetSessionVars().TxnCtx.GetForUpdateTS()
	selExec := b.build(v.SelectPlan)
	if b.err != nil {
		return nil
	}
	base := newBaseExecutor(b.ctx, v.Schema(), v.ID(), selExec)
	base.initCap = chunk.ZeroCapacity
	deleteExec := &DeleteExec{
		baseExecutor:   base,
		tblID2Table:    tblID2table,
		IsMultiTable:   v.IsMultiTable,
		tblColPosInfos: v.TblColPosInfos,
	}
	return deleteExec
}

// updateForUpdateTSIfNeeded updates the ForUpdateTS for a pessimistic transaction if needed.
// PointGet executor will get conflict error if the ForUpdateTS is older than the latest commitTS,
// so we don't need to update now for better latency.
func (b *executorBuilder) updateForUpdateTSIfNeeded(selectPlan plannercore.PhysicalPlan) error {
	txnCtx := b.ctx.GetSessionVars().TxnCtx
	if !txnCtx.IsPessimistic {
		return nil
	}
	if _, ok := selectPlan.(*plannercore.PointGetPlan); ok {
		return nil
	}
	// Activate the invalid txn, use the txn startTS as newForUpdateTS
	txn, err := b.ctx.Txn(false)
	if err != nil {
		return err
	}
	if !txn.Valid() {
		_, err := b.ctx.Txn(true)
		if err != nil {
			return err
		}
		return nil
	}
	// The Repeatable Read transaction use Read Committed level to read data for writing (insert, update, delete, select for update),
	// We should always update/refresh the for-update-ts no matter the isolation level is RR or RC.
	if b.ctx.GetSessionVars().IsPessimisticReadConsistency() {
		return b.refreshForUpdateTSForRC()
	}
	return UpdateForUpdateTS(b.ctx, 0)
}

// refreshForUpdateTSForRC is used to refresh the for-update-ts for reading data at read consistency level in pessimistic transaction.
// It could use the cached tso from the statement future to avoid get tso many times.
func (b *executorBuilder) refreshForUpdateTSForRC() error {
	defer func() {
		b.snapshotTS = b.ctx.GetSessionVars().TxnCtx.GetForUpdateTS()
	}()
	// The first time read-consistency read is executed and `RcReadCheckTS` is enabled, try to use
	// the last valid ts as the for update read ts.
	if b.ctx.GetSessionVars().StmtCtx.RCCheckTS {
		rcReadTS := b.ctx.GetSessionVars().TxnCtx.LastRcReadTs
		if rcReadTS == 0 {
			rcReadTS = b.ctx.GetSessionVars().TxnCtx.StartTS
		}
		return UpdateForUpdateTS(b.ctx, rcReadTS)
	}
	future := b.ctx.GetSessionVars().TxnCtx.GetStmtFutureForRC()
	if future == nil {
		return nil
	}
	newForUpdateTS, waitErr := future.Wait()
	if waitErr != nil {
		logutil.BgLogger().Warn("wait tso failed",
			zap.Uint64("startTS", b.ctx.GetSessionVars().TxnCtx.StartTS),
			zap.Error(waitErr))
	}
	b.ctx.GetSessionVars().TxnCtx.SetStmtFutureForRC(nil)
	// If newForUpdateTS is 0, it will force to get a new for-update-ts from PD.
	return UpdateForUpdateTS(b.ctx, newForUpdateTS)
}

func (b *executorBuilder) buildAnalyzeIndexPushdown(task plannercore.AnalyzeIndexTask, opts map[ast.AnalyzeOptionType]uint64, autoAnalyze string) *analyzeTask {
	job := &statistics.AnalyzeJob{DBName: task.DBName, TableName: task.TableName, PartitionName: task.PartitionName, JobInfo: autoAnalyze + "analyze index " + task.IndexInfo.Name.O}
	_, offset := timeutil.Zone(b.ctx.GetSessionVars().Location())
	sc := b.ctx.GetSessionVars().StmtCtx
	startTS, err := b.getSnapshotTS()
	if err != nil {
		b.err = err
		return nil
	}
	failpoint.Inject("injectAnalyzeSnapshot", func(val failpoint.Value) {
		startTS = uint64(val.(int))
	})
	base := baseAnalyzeExec{
		ctx:         b.ctx,
		tableID:     task.TableID,
		concurrency: b.ctx.GetSessionVars().IndexSerialScanConcurrency(),
		analyzePB: &tipb.AnalyzeReq{
			Tp:             tipb.AnalyzeType_TypeIndex,
			Flags:          sc.PushDownFlags(),
			TimeZoneOffset: offset,
		},
		opts:     opts,
		job:      job,
		snapshot: startTS,
	}
	e := &AnalyzeIndexExec{
		baseAnalyzeExec: base,
		isCommonHandle:  task.TblInfo.IsCommonHandle,
		idxInfo:         task.IndexInfo,
	}
	topNSize := new(int32)
	*topNSize = int32(opts[ast.AnalyzeOptNumTopN])
	statsVersion := new(int32)
	*statsVersion = int32(task.StatsVersion)
	e.analyzePB.IdxReq = &tipb.AnalyzeIndexReq{
		BucketSize: int64(opts[ast.AnalyzeOptNumBuckets]),
		NumColumns: int32(len(task.IndexInfo.Columns)),
		TopNSize:   topNSize,
		Version:    statsVersion,
		SketchSize: maxSketchSize,
	}
	if e.isCommonHandle && e.idxInfo.Primary {
		e.analyzePB.Tp = tipb.AnalyzeType_TypeCommonHandle
	}
	depth := int32(opts[ast.AnalyzeOptCMSketchDepth])
	width := int32(opts[ast.AnalyzeOptCMSketchWidth])
	e.analyzePB.IdxReq.CmsketchDepth = &depth
	e.analyzePB.IdxReq.CmsketchWidth = &width
	return &analyzeTask{taskType: idxTask, idxExec: e, job: job}
}

func (b *executorBuilder) buildAnalyzeIndexIncremental(task plannercore.AnalyzeIndexTask, opts map[ast.AnalyzeOptionType]uint64) *analyzeTask {
	h := domain.GetDomain(b.ctx).StatsHandle()
	statsTbl := h.GetPartitionStats(&model.TableInfo{}, task.TableID.GetStatisticsID())
	analyzeTask := b.buildAnalyzeIndexPushdown(task, opts, "")
	if statsTbl.Pseudo {
		return analyzeTask
	}
	idx, ok := statsTbl.Indices[task.IndexInfo.ID]
	if !ok || idx.Len() == 0 || idx.LastAnalyzePos.IsNull() {
		return analyzeTask
	}
	// If idx got evicted previously, we directly use IndexPushDown task as incremental analyze task will cause inaccuracy
	if idx.IsEvicted() {
		return analyzeTask
	}
	failpoint.Inject("assertEvictIndex", func() {
		if idx.IsEvicted() {
			panic("evicted index shouldn't use analyze incremental task")
		}
	})

	var oldHist *statistics.Histogram
	if statistics.IsAnalyzed(idx.Flag) {
		exec := analyzeTask.idxExec
		if idx.CMSketch != nil {
			width, depth := idx.CMSketch.GetWidthAndDepth()
			exec.analyzePB.IdxReq.CmsketchWidth = &width
			exec.analyzePB.IdxReq.CmsketchDepth = &depth
		}
		oldHist = idx.Histogram.Copy()
	} else {
		_, bktID := idx.LessRowCountWithBktIdx(idx.LastAnalyzePos)
		if bktID == 0 {
			return analyzeTask
		}
		oldHist = idx.TruncateHistogram(bktID)
	}
	var oldTopN *statistics.TopN
	if analyzeTask.idxExec.analyzePB.IdxReq.GetVersion() >= statistics.Version2 {
		oldTopN = idx.TopN.Copy()
		oldTopN.RemoveVal(oldHist.Bounds.GetRow(len(oldHist.Buckets)*2 - 1).GetBytes(0))
	}
	oldHist = oldHist.RemoveUpperBound()
	job := &statistics.AnalyzeJob{DBName: task.DBName, TableName: task.TableName, PartitionName: task.PartitionName, JobInfo: "analyze incremental index " + task.IndexInfo.Name.O}
	exec := analyzeTask.idxExec
	exec.job = job
	analyzeTask.taskType = idxIncrementalTask
	analyzeTask.idxIncrementalExec = &analyzeIndexIncrementalExec{AnalyzeIndexExec: *exec, oldHist: oldHist, oldCMS: idx.CMSketch, oldTopN: oldTopN}
	analyzeTask.job = job
	return analyzeTask
}

func (b *executorBuilder) buildAnalyzeSamplingPushdown(task plannercore.AnalyzeColumnsTask, opts map[ast.AnalyzeOptionType]uint64, schemaForVirtualColEval *expression.Schema) *analyzeTask {
	if task.V2Options != nil {
		opts = task.V2Options.FilledOpts
	}
	availableIdx := make([]*model.IndexInfo, 0, len(task.Indexes))
	colGroups := make([]*tipb.AnalyzeColumnGroup, 0, len(task.Indexes))
	if len(task.Indexes) > 0 {
		for _, idx := range task.Indexes {
			availableIdx = append(availableIdx, idx)
			colGroup := &tipb.AnalyzeColumnGroup{
				ColumnOffsets: make([]int64, 0, len(idx.Columns)),
			}
			for _, col := range idx.Columns {
				colGroup.ColumnOffsets = append(colGroup.ColumnOffsets, int64(col.Offset))
			}
			colGroups = append(colGroups, colGroup)
		}
	}

	_, offset := timeutil.Zone(b.ctx.GetSessionVars().Location())
	sc := b.ctx.GetSessionVars().StmtCtx
	startTS, err := b.getSnapshotTS()
	if err != nil {
		b.err = err
		return nil
	}
	failpoint.Inject("injectAnalyzeSnapshot", func(val failpoint.Value) {
		startTS = uint64(val.(int))
	})
	statsHandle := domain.GetDomain(b.ctx).StatsHandle()
	count, modifyCount, err := statsHandle.StatsMetaCountAndModifyCount(task.TableID.GetStatisticsID())
	if err != nil {
		b.err = err
		return nil
	}
	failpoint.Inject("injectBaseCount", func(val failpoint.Value) {
		count = int64(val.(int))
	})
	failpoint.Inject("injectBaseModifyCount", func(val failpoint.Value) {
		modifyCount = int64(val.(int))
	})
	sampleRate := new(float64)
	if opts[ast.AnalyzeOptNumSamples] == 0 {
		*sampleRate = math.Float64frombits(opts[ast.AnalyzeOptSampleRate])
		if *sampleRate < 0 {
			*sampleRate = b.getAdjustedSampleRate(b.ctx, task)
			if task.PartitionName != "" {
				sc.AppendNote(errors.Errorf(
					"Analyze use auto adjusted sample rate %f for table %s.%s's partition %s",
					*sampleRate,
					task.DBName,
					task.TableName,
					task.PartitionName,
				))
			} else {
				sc.AppendNote(errors.Errorf(
					"Analyze use auto adjusted sample rate %f for table %s.%s",
					*sampleRate,
					task.DBName,
					task.TableName,
				))
			}
		}
	}
	job := &statistics.AnalyzeJob{
		DBName:        task.DBName,
		TableName:     task.TableName,
		PartitionName: task.PartitionName,
	}
	base := baseAnalyzeExec{
		ctx:         b.ctx,
		tableID:     task.TableID,
		concurrency: b.ctx.GetSessionVars().DistSQLScanConcurrency(),
		analyzePB: &tipb.AnalyzeReq{
			Tp:             tipb.AnalyzeType_TypeFullSampling,
			Flags:          sc.PushDownFlags(),
			TimeZoneOffset: offset,
		},
		opts:     opts,
		job:      job,
		snapshot: startTS,
	}
	e := &AnalyzeColumnsExec{
		baseAnalyzeExec:         base,
		tableInfo:               task.TblInfo,
		colsInfo:                task.ColsInfo,
		handleCols:              task.HandleCols,
		indexes:                 availableIdx,
		AnalyzeInfo:             task.AnalyzeInfo,
		schemaForVirtualColEval: schemaForVirtualColEval,
		baseCount:               count,
		baseModifyCnt:           modifyCount,
	}
	e.analyzePB.ColReq = &tipb.AnalyzeColumnsReq{
		BucketSize:   int64(opts[ast.AnalyzeOptNumBuckets]),
		SampleSize:   int64(opts[ast.AnalyzeOptNumSamples]),
		SampleRate:   sampleRate,
		SketchSize:   maxSketchSize,
		ColumnsInfo:  util.ColumnsToProto(task.ColsInfo, task.TblInfo.PKIsHandle),
		ColumnGroups: colGroups,
	}
	if task.TblInfo != nil {
		e.analyzePB.ColReq.PrimaryColumnIds = tables.TryGetCommonPkColumnIds(task.TblInfo)
		if task.TblInfo.IsCommonHandle {
			e.analyzePB.ColReq.PrimaryPrefixColumnIds = tables.PrimaryPrefixColumnIDs(task.TblInfo)
		}
	}
	b.err = plannercore.SetPBColumnsDefaultValue(b.ctx, e.analyzePB.ColReq.ColumnsInfo, task.ColsInfo)
	return &analyzeTask{taskType: colTask, colExec: e, job: job}
}

// getAdjustedSampleRate calculate the sample rate by the table size. If we cannot get the table size. We use the 0.001 as the default sample rate.
func (b *executorBuilder) getAdjustedSampleRate(sctx sessionctx.Context, task plannercore.AnalyzeColumnsTask) float64 {
	statsHandle := domain.GetDomain(sctx).StatsHandle()
	defaultRate := 0.001
	if statsHandle == nil {
		return defaultRate
	}
	var statsTbl *statistics.Table
	tid := task.TableID.GetStatisticsID()
	if tid == task.TblInfo.ID {
		statsTbl = statsHandle.GetTableStats(task.TblInfo)
	} else {
		statsTbl = statsHandle.GetPartitionStats(task.TblInfo, tid)
	}
	approxiCount, hasPD := b.getApproximateTableCountFromStorage(sctx, tid, task)
	// If there's no stats meta and no pd, return the default rate.
	if statsTbl == nil && !hasPD {
		return defaultRate
	}
	// If the count in stats_meta is still 0 and there's no information from pd side, we scan all rows.
	if statsTbl.Count == 0 && !hasPD {
		return 1
	}
	// we have issue https://github.com/pingcap/tidb/issues/29216.
	// To do a workaround for this issue, we check the approxiCount from the pd side to do a comparison.
	// If the count from the stats_meta is extremely smaller than the approximate count from the pd,
	// we think that we meet this issue and use the approximate count to calculate the sample rate.
	if float64(statsTbl.Count*5) < approxiCount {
		// Confirmed by TiKV side, the experience error rate of the approximate count is about 20%.
		// So we increase the number to 150000 to reduce this error rate.
		return math.Min(1, 150000/approxiCount)
	}
	// If we don't go into the above if branch and we still detect the count is zero. Return 1 to prevent the dividing zero.
	if statsTbl.Count == 0 {
		return 1
	}
	// We are expected to scan about 100000 rows or so.
	// Since there's tiny error rate around the count from the stats meta, we use 110000 to get a little big result
	return math.Min(1, config.DefRowsForSampleRate/float64(statsTbl.Count))
}

func (b *executorBuilder) getApproximateTableCountFromStorage(sctx sessionctx.Context, tid int64, task plannercore.AnalyzeColumnsTask) (float64, bool) {
	tikvStore, ok := sctx.GetStore().(helper.Storage)
	if !ok {
		return 0, false
	}
	regionStats := &helper.PDRegionStats{}
	pdHelper := helper.NewHelper(tikvStore)
	err := pdHelper.GetPDRegionStats(tid, regionStats, true)
	failpoint.Inject("calcSampleRateByStorageCount", func() {
		// Force the TiDB thinking that there's PD and the count of region is small.
		err = nil
		regionStats.Count = 1
		// Set a very large approximate count.
		regionStats.StorageKeys = 1000000
	})
	if err != nil {
		return 0, false
	}
	// If this table is not small, we directly use the count from PD,
	// since for a small table, it's possible that it's data is in the same region with part of another large table.
	// Thus, we use the number of the regions of the table's table KV to decide whether the table is small.
	if regionStats.Count > 2 {
		return float64(regionStats.StorageKeys), true
	}
	// Otherwise, we use count(*) to calc it's size, since it's very small, the table data can be filled in no more than 2 regions.
	sql := new(strings.Builder)
	sqlexec.MustFormatSQL(sql, "select count(*) from %n.%n", task.DBName, task.TableName)
	if task.PartitionName != "" {
		sqlexec.MustFormatSQL(sql, " partition(%n)", task.PartitionName)
	}
	rows, _, err := b.ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(context.TODO(), nil, sql.String())
	if err != nil {
		return 0, false
	}
	// If the record set is nil, there's something wrong with the execution. The COUNT(*) would always return one row.
	if len(rows) == 0 || rows[0].Len() == 0 {
		return 0, false
	}
	return float64(rows[0].GetInt64(0)), true
}

func (b *executorBuilder) buildAnalyzeColumnsPushdown(task plannercore.AnalyzeColumnsTask, opts map[ast.AnalyzeOptionType]uint64, autoAnalyze string, schemaForVirtualColEval *expression.Schema) *analyzeTask {
	if task.StatsVersion == statistics.Version2 {
		return b.buildAnalyzeSamplingPushdown(task, opts, schemaForVirtualColEval)
	}
	job := &statistics.AnalyzeJob{DBName: task.DBName, TableName: task.TableName, PartitionName: task.PartitionName, JobInfo: autoAnalyze + "analyze columns"}
	cols := task.ColsInfo
	if hasPkHist(task.HandleCols) {
		colInfo := task.TblInfo.Columns[task.HandleCols.GetCol(0).Index]
		cols = append([]*model.ColumnInfo{colInfo}, cols...)
	} else if task.HandleCols != nil && !task.HandleCols.IsInt() {
		cols = make([]*model.ColumnInfo, 0, len(task.ColsInfo)+task.HandleCols.NumCols())
		for i := 0; i < task.HandleCols.NumCols(); i++ {
			cols = append(cols, task.TblInfo.Columns[task.HandleCols.GetCol(i).Index])
		}
		cols = append(cols, task.ColsInfo...)
		task.ColsInfo = cols
	}

	_, offset := timeutil.Zone(b.ctx.GetSessionVars().Location())
	sc := b.ctx.GetSessionVars().StmtCtx
	startTS, err := b.getSnapshotTS()
	if err != nil {
		b.err = err
		return nil
	}
	failpoint.Inject("injectAnalyzeSnapshot", func(val failpoint.Value) {
		startTS = uint64(val.(int))
	})
	base := baseAnalyzeExec{
		ctx:         b.ctx,
		tableID:     task.TableID,
		concurrency: b.ctx.GetSessionVars().DistSQLScanConcurrency(),
		analyzePB: &tipb.AnalyzeReq{
			Tp:             tipb.AnalyzeType_TypeColumn,
			Flags:          sc.PushDownFlags(),
			TimeZoneOffset: offset,
		},
		opts:     opts,
		job:      job,
		snapshot: startTS,
	}
	e := &AnalyzeColumnsExec{
		baseAnalyzeExec: base,
		colsInfo:        task.ColsInfo,
		handleCols:      task.HandleCols,
		AnalyzeInfo:     task.AnalyzeInfo,
	}
	depth := int32(opts[ast.AnalyzeOptCMSketchDepth])
	width := int32(opts[ast.AnalyzeOptCMSketchWidth])
	e.analyzePB.ColReq = &tipb.AnalyzeColumnsReq{
		BucketSize:    int64(opts[ast.AnalyzeOptNumBuckets]),
		SampleSize:    maxRegionSampleSize,
		SketchSize:    maxSketchSize,
		ColumnsInfo:   util.ColumnsToProto(cols, task.HandleCols != nil && task.HandleCols.IsInt()),
		CmsketchDepth: &depth,
		CmsketchWidth: &width,
	}
	if task.TblInfo != nil {
		e.analyzePB.ColReq.PrimaryColumnIds = tables.TryGetCommonPkColumnIds(task.TblInfo)
		if task.TblInfo.IsCommonHandle {
			e.analyzePB.ColReq.PrimaryPrefixColumnIds = tables.PrimaryPrefixColumnIDs(task.TblInfo)
		}
	}
	if task.CommonHandleInfo != nil {
		topNSize := new(int32)
		*topNSize = int32(opts[ast.AnalyzeOptNumTopN])
		statsVersion := new(int32)
		*statsVersion = int32(task.StatsVersion)
		e.analyzePB.IdxReq = &tipb.AnalyzeIndexReq{
			BucketSize: int64(opts[ast.AnalyzeOptNumBuckets]),
			NumColumns: int32(len(task.CommonHandleInfo.Columns)),
			TopNSize:   topNSize,
			Version:    statsVersion,
		}
		depth := int32(opts[ast.AnalyzeOptCMSketchDepth])
		width := int32(opts[ast.AnalyzeOptCMSketchWidth])
		e.analyzePB.IdxReq.CmsketchDepth = &depth
		e.analyzePB.IdxReq.CmsketchWidth = &width
		e.analyzePB.IdxReq.SketchSize = maxSketchSize
		e.analyzePB.ColReq.PrimaryColumnIds = tables.TryGetCommonPkColumnIds(task.TblInfo)
		e.analyzePB.Tp = tipb.AnalyzeType_TypeMixed
		e.commonHandle = task.CommonHandleInfo
	}
	b.err = plannercore.SetPBColumnsDefaultValue(b.ctx, e.analyzePB.ColReq.ColumnsInfo, cols)
	return &analyzeTask{taskType: colTask, colExec: e, job: job}
}

func (b *executorBuilder) buildAnalyzePKIncremental(task plannercore.AnalyzeColumnsTask, opts map[ast.AnalyzeOptionType]uint64) *analyzeTask {
	h := domain.GetDomain(b.ctx).StatsHandle()
	statsTbl := h.GetPartitionStats(&model.TableInfo{}, task.TableID.GetStatisticsID())
	analyzeTask := b.buildAnalyzeColumnsPushdown(task, opts, "", nil)
	if statsTbl.Pseudo {
		return analyzeTask
	}
	if task.HandleCols == nil || !task.HandleCols.IsInt() {
		return analyzeTask
	}
	col, ok := statsTbl.Columns[task.HandleCols.GetCol(0).ID]
	if !ok || col.Len() == 0 || col.LastAnalyzePos.IsNull() {
		return analyzeTask
	}
	var oldHist *statistics.Histogram
	if statistics.IsAnalyzed(col.Flag) {
		oldHist = col.Histogram.Copy()
	} else {
		d, err := col.LastAnalyzePos.ConvertTo(b.ctx.GetSessionVars().StmtCtx, col.Tp)
		if err != nil {
			b.err = err
			return nil
		}
		_, bktID := col.LessRowCountWithBktIdx(d)
		if bktID == 0 {
			return analyzeTask
		}
		oldHist = col.TruncateHistogram(bktID)
		oldHist.NDV = int64(oldHist.TotalRowCount())
	}
	job := &statistics.AnalyzeJob{DBName: task.DBName, TableName: task.TableName, PartitionName: task.PartitionName, JobInfo: "analyze incremental primary key"}
	exec := analyzeTask.colExec
	exec.job = job
	analyzeTask.taskType = pkIncrementalTask
	analyzeTask.colIncrementalExec = &analyzePKIncrementalExec{AnalyzeColumnsExec: *exec, oldHist: oldHist}
	analyzeTask.job = job
	return analyzeTask
}

func (b *executorBuilder) buildAnalyzeFastColumn(e *AnalyzeExec, task plannercore.AnalyzeColumnsTask, opts map[ast.AnalyzeOptionType]uint64) {
	findTask := false
	for _, eTask := range e.tasks {
		if eTask.fastExec != nil && eTask.fastExec.tableID.Equals(&task.TableID) {
			eTask.fastExec.colsInfo = append(eTask.fastExec.colsInfo, task.ColsInfo...)
			findTask = true
			break
		}
	}
	if !findTask {
		job := &statistics.AnalyzeJob{DBName: task.DBName, TableName: task.TableName, PartitionName: task.PartitionName, JobInfo: "fast analyze columns"}
		var concurrency int
		concurrency, b.err = getBuildStatsConcurrency(e.ctx)
		if b.err != nil {
			return
		}
		startTS, err := b.getSnapshotTS()
		if err != nil {
			b.err = err
			return
		}
		base := baseAnalyzeExec{
			ctx:         b.ctx,
			tableID:     task.TableID,
			opts:        opts,
			concurrency: concurrency,
			job:         job,
			snapshot:    startTS,
		}
		fastExec := &AnalyzeFastExec{
			baseAnalyzeExec: base,
			colsInfo:        task.ColsInfo,
			handleCols:      task.HandleCols,
			tblInfo:         task.TblInfo,
			wg:              &sync.WaitGroup{},
		}
		b.err = fastExec.calculateEstimateSampleStep()
		if b.err != nil {
			return
		}
		e.tasks = append(e.tasks, &analyzeTask{
			taskType: fastTask,
			fastExec: fastExec,
			job:      job,
		})
	}
}

func (b *executorBuilder) buildAnalyzeFastIndex(e *AnalyzeExec, task plannercore.AnalyzeIndexTask, opts map[ast.AnalyzeOptionType]uint64) {
	findTask := false
	for _, eTask := range e.tasks {
		if eTask.fastExec != nil && eTask.fastExec.tableID.Equals(&task.TableID) {
			eTask.fastExec.idxsInfo = append(eTask.fastExec.idxsInfo, task.IndexInfo)
			findTask = true
			break
		}
	}
	if !findTask {
		job := &statistics.AnalyzeJob{DBName: task.DBName, TableName: task.TableName, PartitionName: "fast analyze index " + task.IndexInfo.Name.O}
		var concurrency int
		concurrency, b.err = getBuildStatsConcurrency(e.ctx)
		if b.err != nil {
			return
		}
		startTS, err := b.getSnapshotTS()
		if err != nil {
			b.err = err
			return
		}
		base := baseAnalyzeExec{
			ctx:         b.ctx,
			tableID:     task.TableID,
			opts:        opts,
			concurrency: concurrency,
			job:         job,
			snapshot:    startTS,
		}
		fastExec := &AnalyzeFastExec{
			baseAnalyzeExec: base,
			idxsInfo:        []*model.IndexInfo{task.IndexInfo},
			tblInfo:         task.TblInfo,
			wg:              &sync.WaitGroup{},
		}
		b.err = fastExec.calculateEstimateSampleStep()
		if b.err != nil {
			return
		}
		e.tasks = append(e.tasks, &analyzeTask{
			taskType: fastTask,
			fastExec: fastExec,
			job:      job,
		})
	}
}

func (b *executorBuilder) buildAnalyze(v *plannercore.Analyze) Executor {
	e := &AnalyzeExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
		tasks:        make([]*analyzeTask, 0, len(v.ColTasks)+len(v.IdxTasks)),
		opts:         v.Opts,
		OptionsMap:   v.OptionsMap,
	}
	enableFastAnalyze := b.ctx.GetSessionVars().EnableFastAnalyze
	autoAnalyze := ""
	if b.ctx.GetSessionVars().InRestrictedSQL {
		autoAnalyze = "auto "
	}
	for _, task := range v.ColTasks {
		if task.Incremental {
			e.tasks = append(e.tasks, b.buildAnalyzePKIncremental(task, v.Opts))
		} else {
			if enableFastAnalyze {
				b.buildAnalyzeFastColumn(e, task, v.Opts)
			} else {
				columns, _, err := expression.ColumnInfos2ColumnsAndNames(b.ctx, model.NewCIStr(task.AnalyzeInfo.DBName), task.TblInfo.Name, task.ColsInfo, task.TblInfo)
				if err != nil {
					b.err = err
					return nil
				}
				schema := expression.NewSchema(columns...)
				e.tasks = append(e.tasks, b.buildAnalyzeColumnsPushdown(task, v.Opts, autoAnalyze, schema))
			}
		}
		if b.err != nil {
			return nil
		}
	}
	for _, task := range v.IdxTasks {
		if task.Incremental {
			e.tasks = append(e.tasks, b.buildAnalyzeIndexIncremental(task, v.Opts))
		} else {
			if enableFastAnalyze {
				b.buildAnalyzeFastIndex(e, task, v.Opts)
			} else {
				e.tasks = append(e.tasks, b.buildAnalyzeIndexPushdown(task, v.Opts, autoAnalyze))
			}
		}
		if b.err != nil {
			return nil
		}
	}
	return e
}

func constructDistExec(sctx sessionctx.Context, plans []plannercore.PhysicalPlan) ([]*tipb.Executor, bool, error) {
	streaming := true
	executors := make([]*tipb.Executor, 0, len(plans))
	for _, p := range plans {
		execPB, err := p.ToPB(sctx, kv.TiKV)
		if err != nil {
			return nil, false, err
		}
		if !plannercore.SupportStreaming(p) {
			streaming = false
		}
		executors = append(executors, execPB)
	}
	return executors, streaming, nil
}

// markChildrenUsedCols compares each child with the output schema, and mark
// each column of the child is used by output or not.
func markChildrenUsedCols(outputSchema *expression.Schema, childSchema ...*expression.Schema) (childrenUsed [][]bool) {
	for _, child := range childSchema {
		used := expression.GetUsedList(outputSchema.Columns, child)
		childrenUsed = append(childrenUsed, used)
	}
	return
}

func constructDistExecForTiFlash(sctx sessionctx.Context, p plannercore.PhysicalPlan) ([]*tipb.Executor, bool, error) {
	execPB, err := p.ToPB(sctx, kv.TiFlash)
	return []*tipb.Executor{execPB}, false, err

}

func constructDAGReq(ctx sessionctx.Context, plans []plannercore.PhysicalPlan, storeType kv.StoreType) (dagReq *tipb.DAGRequest, streaming bool, err error) {
	dagReq = &tipb.DAGRequest{}
	dagReq.TimeZoneName, dagReq.TimeZoneOffset = timeutil.Zone(ctx.GetSessionVars().Location())
	sc := ctx.GetSessionVars().StmtCtx
	if sc.RuntimeStatsColl != nil {
		collExec := true
		dagReq.CollectExecutionSummaries = &collExec
	}
	dagReq.Flags = sc.PushDownFlags()
	if storeType == kv.TiFlash {
		var executors []*tipb.Executor
		executors, streaming, err = constructDistExecForTiFlash(ctx, plans[0])
		dagReq.RootExecutor = executors[0]
	} else {
		dagReq.Executors, streaming, err = constructDistExec(ctx, plans)
	}

	distsql.SetEncodeType(ctx, dagReq)
	return dagReq, streaming, err
}

func (b *executorBuilder) corColInDistPlan(plans []plannercore.PhysicalPlan) bool {
	for _, p := range plans {
		x, ok := p.(*plannercore.PhysicalSelection)
		if !ok {
			continue
		}
		for _, cond := range x.Conditions {
			if len(expression.ExtractCorColumns(cond)) > 0 {
				return true
			}
		}
	}
	return false
}

// corColInAccess checks whether there's correlated column in access conditions.
func (b *executorBuilder) corColInAccess(p plannercore.PhysicalPlan) bool {
	var access []expression.Expression
	switch x := p.(type) {
	case *plannercore.PhysicalTableScan:
		access = x.AccessCondition
	case *plannercore.PhysicalIndexScan:
		access = x.AccessCondition
	}
	for _, cond := range access {
		if len(expression.ExtractCorColumns(cond)) > 0 {
			return true
		}
	}
	return false
}

func (b *executorBuilder) newDataReaderBuilder(p plannercore.PhysicalPlan) (*dataReaderBuilder, error) {
	ts, err := b.getSnapshotTS()
	if err != nil {
		return nil, err
	}

	builderForDataReader := *b
	builderForDataReader.forDataReaderBuilder = true
	builderForDataReader.dataReaderTS = ts

	return &dataReaderBuilder{
		Plan:            p,
		executorBuilder: &builderForDataReader,
	}, nil
}

func (b *executorBuilder) buildIndexLookUpJoin(v *plannercore.PhysicalIndexJoin) Executor {
	outerExec := b.build(v.Children()[1-v.InnerChildIdx])
	if b.err != nil {
		return nil
	}
	outerTypes := retTypes(outerExec)
	innerPlan := v.Children()[v.InnerChildIdx]
	innerTypes := make([]*types.FieldType, innerPlan.Schema().Len())
	for i, col := range innerPlan.Schema().Columns {
		innerTypes[i] = col.RetType.Clone()
		// The `innerTypes` would be called for `Datum.ConvertTo` when converting the columns from outer table
		// to build hash map or construct lookup keys. So we need to modify its flen otherwise there would be
		// truncate error. See issue https://github.com/pingcap/tidb/issues/21232 for example.
		if innerTypes[i].EvalType() == types.ETString {
			innerTypes[i].SetFlen(types.UnspecifiedLength)
		}
	}

	// Use the probe table's collation.
	for i, col := range v.OuterHashKeys {
		outerTypes[col.Index] = outerTypes[col.Index].Clone()
		outerTypes[col.Index].SetCollate(innerTypes[v.InnerHashKeys[i].Index].GetCollate())
		outerTypes[col.Index].SetFlag(col.RetType.GetFlag())
	}

	// We should use JoinKey to construct the type information using by hashing, instead of using the child's schema directly.
	// When a hybrid type column is hashed multiple times, we need to distinguish what field types are used.
	// For example, the condition `enum = int and enum = string`, we should use ETInt to hash the first column,
	// and use ETString to hash the second column, although they may be the same column.
	innerHashTypes := make([]*types.FieldType, len(v.InnerHashKeys))
	outerHashTypes := make([]*types.FieldType, len(v.OuterHashKeys))
	for i, col := range v.InnerHashKeys {
		innerHashTypes[i] = innerTypes[col.Index].Clone()
		innerHashTypes[i].SetFlag(col.RetType.GetFlag())
	}
	for i, col := range v.OuterHashKeys {
		outerHashTypes[i] = outerTypes[col.Index].Clone()
		outerHashTypes[i].SetFlag(col.RetType.GetFlag())
	}

	var (
		outerFilter           []expression.Expression
		leftTypes, rightTypes []*types.FieldType
	)

	if v.InnerChildIdx == 0 {
		leftTypes, rightTypes = innerTypes, outerTypes
		outerFilter = v.RightConditions
		if len(v.LeftConditions) > 0 {
			b.err = errors.Annotate(ErrBuildExecutor, "join's inner condition should be empty")
			return nil
		}
	} else {
		leftTypes, rightTypes = outerTypes, innerTypes
		outerFilter = v.LeftConditions
		if len(v.RightConditions) > 0 {
			b.err = errors.Annotate(ErrBuildExecutor, "join's inner condition should be empty")
			return nil
		}
	}
	defaultValues := v.DefaultValues
	if defaultValues == nil {
		defaultValues = make([]types.Datum, len(innerTypes))
	}
	hasPrefixCol := false
	for _, l := range v.IdxColLens {
		if l != types.UnspecifiedLength {
			hasPrefixCol = true
			break
		}
	}

	readerBuilder, err := b.newDataReaderBuilder(innerPlan)
	if err != nil {
		b.err = err
		return nil
	}

	e := &IndexLookUpJoin{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID(), outerExec),
		outerCtx: outerCtx{
			rowTypes:  outerTypes,
			hashTypes: outerHashTypes,
			filter:    outerFilter,
		},
		innerCtx: innerCtx{
			readerBuilder: readerBuilder,
			rowTypes:      innerTypes,
			hashTypes:     innerHashTypes,
			colLens:       v.IdxColLens,
			hasPrefixCol:  hasPrefixCol,
		},
		workerWg:      new(sync.WaitGroup),
		isOuterJoin:   v.JoinType.IsOuterJoin(),
		indexRanges:   v.Ranges,
		keyOff2IdxOff: v.KeyOff2IdxOff,
		lastColHelper: v.CompareFilters,
		finished:      &atomic.Value{},
	}
	childrenUsedSchema := markChildrenUsedCols(v.Schema(), v.Children()[0].Schema(), v.Children()[1].Schema())
	e.joiner = newJoiner(b.ctx, v.JoinType, v.InnerChildIdx == 0, defaultValues, v.OtherConditions, leftTypes, rightTypes, childrenUsedSchema)
	outerKeyCols := make([]int, len(v.OuterJoinKeys))
	for i := 0; i < len(v.OuterJoinKeys); i++ {
		outerKeyCols[i] = v.OuterJoinKeys[i].Index
	}
	innerKeyCols := make([]int, len(v.InnerJoinKeys))
	innerKeyColIDs := make([]int64, len(v.InnerJoinKeys))
	keyCollators := make([]collate.Collator, 0, len(v.InnerJoinKeys))
	for i := 0; i < len(v.InnerJoinKeys); i++ {
		innerKeyCols[i] = v.InnerJoinKeys[i].Index
		innerKeyColIDs[i] = v.InnerJoinKeys[i].ID
		keyCollators = append(keyCollators, collate.GetCollator(v.InnerJoinKeys[i].RetType.GetCollate()))
	}
	e.outerCtx.keyCols = outerKeyCols
	e.innerCtx.keyCols = innerKeyCols
	e.innerCtx.keyColIDs = innerKeyColIDs
	e.innerCtx.keyCollators = keyCollators

	outerHashCols, innerHashCols := make([]int, len(v.OuterHashKeys)), make([]int, len(v.InnerHashKeys))
	hashCollators := make([]collate.Collator, 0, len(v.InnerHashKeys))
	for i := 0; i < len(v.OuterHashKeys); i++ {
		outerHashCols[i] = v.OuterHashKeys[i].Index
	}
	for i := 0; i < len(v.InnerHashKeys); i++ {
		innerHashCols[i] = v.InnerHashKeys[i].Index
		hashCollators = append(hashCollators, collate.GetCollator(v.InnerHashKeys[i].RetType.GetCollate()))
	}
	e.outerCtx.hashCols = outerHashCols
	e.innerCtx.hashCols = innerHashCols
	e.innerCtx.hashCollators = hashCollators

	e.joinResult = newFirstChunk(e)
	executorCounterIndexLookUpJoin.Inc()
	return e
}

func (b *executorBuilder) buildIndexLookUpMergeJoin(v *plannercore.PhysicalIndexMergeJoin) Executor {
	outerExec := b.build(v.Children()[1-v.InnerChildIdx])
	if b.err != nil {
		return nil
	}
	outerTypes := retTypes(outerExec)
	innerPlan := v.Children()[v.InnerChildIdx]
	innerTypes := make([]*types.FieldType, innerPlan.Schema().Len())
	for i, col := range innerPlan.Schema().Columns {
		innerTypes[i] = col.RetType.Clone()
		// The `innerTypes` would be called for `Datum.ConvertTo` when converting the columns from outer table
		// to build hash map or construct lookup keys. So we need to modify its flen otherwise there would be
		// truncate error. See issue https://github.com/pingcap/tidb/issues/21232 for example.
		if innerTypes[i].EvalType() == types.ETString {
			innerTypes[i].SetFlen(types.UnspecifiedLength)
		}
	}
	var (
		outerFilter           []expression.Expression
		leftTypes, rightTypes []*types.FieldType
	)
	if v.InnerChildIdx == 0 {
		leftTypes, rightTypes = innerTypes, outerTypes
		outerFilter = v.RightConditions
		if len(v.LeftConditions) > 0 {
			b.err = errors.Annotate(ErrBuildExecutor, "join's inner condition should be empty")
			return nil
		}
	} else {
		leftTypes, rightTypes = outerTypes, innerTypes
		outerFilter = v.LeftConditions
		if len(v.RightConditions) > 0 {
			b.err = errors.Annotate(ErrBuildExecutor, "join's inner condition should be empty")
			return nil
		}
	}
	defaultValues := v.DefaultValues
	if defaultValues == nil {
		defaultValues = make([]types.Datum, len(innerTypes))
	}
	outerKeyCols := make([]int, len(v.OuterJoinKeys))
	for i := 0; i < len(v.OuterJoinKeys); i++ {
		outerKeyCols[i] = v.OuterJoinKeys[i].Index
	}
	innerKeyCols := make([]int, len(v.InnerJoinKeys))
	keyCollators := make([]collate.Collator, 0, len(v.InnerJoinKeys))
	for i := 0; i < len(v.InnerJoinKeys); i++ {
		innerKeyCols[i] = v.InnerJoinKeys[i].Index
		keyCollators = append(keyCollators, collate.GetCollator(v.InnerJoinKeys[i].RetType.GetCollate()))
	}
	executorCounterIndexLookUpJoin.Inc()

	readerBuilder, err := b.newDataReaderBuilder(innerPlan)
	if err != nil {
		b.err = err
		return nil
	}

	e := &IndexLookUpMergeJoin{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID(), outerExec),
		outerMergeCtx: outerMergeCtx{
			rowTypes:      outerTypes,
			filter:        outerFilter,
			joinKeys:      v.OuterJoinKeys,
			keyCols:       outerKeyCols,
			needOuterSort: v.NeedOuterSort,
			compareFuncs:  v.OuterCompareFuncs,
		},
		innerMergeCtx: innerMergeCtx{
			readerBuilder:           readerBuilder,
			rowTypes:                innerTypes,
			joinKeys:                v.InnerJoinKeys,
			keyCols:                 innerKeyCols,
			keyCollators:            keyCollators,
			compareFuncs:            v.CompareFuncs,
			colLens:                 v.IdxColLens,
			desc:                    v.Desc,
			keyOff2KeyOffOrderByIdx: v.KeyOff2KeyOffOrderByIdx,
		},
		workerWg:      new(sync.WaitGroup),
		isOuterJoin:   v.JoinType.IsOuterJoin(),
		indexRanges:   v.Ranges,
		keyOff2IdxOff: v.KeyOff2IdxOff,
		lastColHelper: v.CompareFilters,
	}
	childrenUsedSchema := markChildrenUsedCols(v.Schema(), v.Children()[0].Schema(), v.Children()[1].Schema())
	joiners := make([]joiner, e.ctx.GetSessionVars().IndexLookupJoinConcurrency())
	for i := 0; i < len(joiners); i++ {
		joiners[i] = newJoiner(b.ctx, v.JoinType, v.InnerChildIdx == 0, defaultValues, v.OtherConditions, leftTypes, rightTypes, childrenUsedSchema)
	}
	e.joiners = joiners
	return e
}

func (b *executorBuilder) buildIndexNestedLoopHashJoin(v *plannercore.PhysicalIndexHashJoin) Executor {
	e := b.buildIndexLookUpJoin(&(v.PhysicalIndexJoin)).(*IndexLookUpJoin)
	idxHash := &IndexNestedLoopHashJoin{
		IndexLookUpJoin: *e,
		keepOuterOrder:  v.KeepOuterOrder,
	}
	concurrency := e.ctx.GetSessionVars().IndexLookupJoinConcurrency()
	idxHash.joiners = make([]joiner, concurrency)
	for i := 0; i < concurrency; i++ {
		idxHash.joiners[i] = e.joiner.Clone()
	}
	return idxHash
}

// containsLimit tests if the execs contains Limit because we do not know whether `Limit` has consumed all of its' source,
// so the feedback may not be accurate.
func containsLimit(execs []*tipb.Executor) bool {
	for _, exec := range execs {
		if exec.Limit != nil {
			return true
		}
	}
	return false
}

func buildNoRangeTableReader(b *executorBuilder, v *plannercore.PhysicalTableReader) (*TableReaderExecutor, error) {
	tablePlans := v.TablePlans
	if v.StoreType == kv.TiFlash {
		tablePlans = []plannercore.PhysicalPlan{v.GetTablePlan()}
	}
	dagReq, streaming, err := constructDAGReq(b.ctx, tablePlans, v.StoreType)
	if err != nil {
		return nil, err
	}
	ts, err := v.GetTableScan()
	if err != nil {
		return nil, err
	}
	if err = b.validCanReadTemporaryOrCacheTable(ts.Table); err != nil {
		return nil, err
	}

	tbl, _ := b.is.TableByID(ts.Table.ID)
	isPartition, physicalTableID := ts.IsPartition()
	if isPartition {
		pt := tbl.(table.PartitionedTable)
		tbl = pt.GetPartition(physicalTableID)
	}
	startTS, err := b.getSnapshotTS()
	if err != nil {
		return nil, err
	}
	e := &TableReaderExecutor{
		baseExecutor:     newBaseExecutor(b.ctx, v.Schema(), v.ID()),
		dagPB:            dagReq,
		startTS:          startTS,
		readReplicaScope: b.readReplicaScope,
		isStaleness:      b.isStaleness,
		table:            tbl,
		keepOrder:        ts.KeepOrder,
		desc:             ts.Desc,
		columns:          ts.Columns,
		streaming:        streaming,
		corColInFilter:   b.corColInDistPlan(v.TablePlans),
		corColInAccess:   b.corColInAccess(v.TablePlans[0]),
		plans:            v.TablePlans,
		tablePlan:        v.GetTablePlan(),
		storeType:        v.StoreType,
		batchCop:         v.ReadReqType == plannercore.BatchCop,
	}
	e.buildVirtualColumnInfo()
	if containsLimit(dagReq.Executors) {
		e.feedback = statistics.NewQueryFeedback(0, nil, 0, ts.Desc)
	} else {
		e.feedback = statistics.NewQueryFeedback(getFeedbackStatsTableID(e.ctx, tbl), ts.Hist, int64(ts.StatsCount()), ts.Desc)
	}
	collect := statistics.CollectFeedback(b.ctx.GetSessionVars().StmtCtx, e.feedback, len(ts.Ranges))
	// Do not collect the feedback when the table is the partition table.
	if collect && tbl.Meta().Partition != nil {
		collect = false
	}
	if !collect {
		e.feedback.Invalidate()
	}
	e.dagPB.CollectRangeCounts = &collect
	if v.StoreType == kv.TiDB && b.ctx.GetSessionVars().User != nil {
		// User info is used to do privilege check. It is only used in TiDB cluster memory table.
		e.dagPB.User = &tipb.UserIdentity{
			UserName: b.ctx.GetSessionVars().User.Username,
			UserHost: b.ctx.GetSessionVars().User.Hostname,
		}
	}

	for i := range v.Schema().Columns {
		dagReq.OutputOffsets = append(dagReq.OutputOffsets, uint32(i))
	}

	return e, nil
}

func (b *executorBuilder) buildMPPGather(v *plannercore.PhysicalTableReader) Executor {
	startTs, err := b.getSnapshotTS()
	if err != nil {
		b.err = err
		return nil
	}
	gather := &MPPGather{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
		is:           b.is,
		originalPlan: v.GetTablePlan(),
		startTS:      startTs,
	}
	return gather
}

// buildTableReader builds a table reader executor. It first build a no range table reader,
// and then update it ranges from table scan plan.
func (b *executorBuilder) buildTableReader(v *plannercore.PhysicalTableReader) Executor {
	if v.StoreType != kv.TiKV && b.isStaleness {
		b.err = errors.New("stale requests require tikv backend")
		return nil
	}
	failpoint.Inject("checkUseMPP", func(val failpoint.Value) {
		if val.(bool) != useMPPExecution(b.ctx, v) {
			if val.(bool) {
				b.err = errors.New("expect mpp but not used")
			} else {
				b.err = errors.New("don't expect mpp but we used it")
			}
			failpoint.Return(nil)
		}
	})
	if useMPPExecution(b.ctx, v) {
		return b.buildMPPGather(v)
	}
	ts, err := v.GetTableScan()
	if err != nil {
		b.err = err
		return nil
	}
	ret, err := buildNoRangeTableReader(b, v)
	if err != nil {
		b.err = err
		return nil
	}
	if err = b.validCanReadTemporaryOrCacheTable(ts.Table); err != nil {
		b.err = err
		return nil
	}

	if ret.table.Meta().TempTableType != model.TempTableNone {
		ret.dummy = true
	}

	ret.ranges = ts.Ranges
	sctx := b.ctx.GetSessionVars().StmtCtx
	sctx.TableIDs = append(sctx.TableIDs, ts.Table.ID)

	if !b.ctx.GetSessionVars().UseDynamicPartitionPrune() {
		return ret
	}
	// When isPartition is set, it means the union rewriting is done, so a partition reader is preferred.
	if ok, _ := ts.IsPartition(); ok {
		return ret
	}

	pi := ts.Table.GetPartitionInfo()
	if pi == nil {
		return ret
	}

	tmp, _ := b.is.TableByID(ts.Table.ID)
	tbl := tmp.(table.PartitionedTable)
	partitions, err := partitionPruning(b.ctx, tbl, v.PartitionInfo.PruningConds, v.PartitionInfo.PartitionNames, v.PartitionInfo.Columns, v.PartitionInfo.ColumnNames)
	if err != nil {
		b.err = err
		return nil
	}
	if v.StoreType == kv.TiFlash {
		sctx.IsTiFlash.Store(true)
	}

	if len(partitions) == 0 {
		return &TableDualExec{baseExecutor: *ret.base()}
	}
	ret.kvRangeBuilder = kvRangeBuilderFromRangeAndPartition{
		sctx:       b.ctx,
		partitions: partitions,
	}

	return ret
}

func buildIndexRangeForEachPartition(ctx sessionctx.Context, usedPartitions []table.PhysicalTable, contentPos []int64,
	lookUpContent []*indexJoinLookUpContent, indexRanges []*ranger.Range, keyOff2IdxOff []int, cwc *plannercore.ColWithCmpFuncManager) (map[int64][]*ranger.Range, error) {
	contentBucket := make(map[int64][]*indexJoinLookUpContent)
	for _, p := range usedPartitions {
		contentBucket[p.GetPhysicalID()] = make([]*indexJoinLookUpContent, 0, 8)
	}
	for i, pos := range contentPos {
		if _, ok := contentBucket[pos]; ok {
			contentBucket[pos] = append(contentBucket[pos], lookUpContent[i])
		}
	}
	nextRange := make(map[int64][]*ranger.Range)
	for _, p := range usedPartitions {
		ranges, err := buildRangesForIndexJoin(ctx, contentBucket[p.GetPhysicalID()], indexRanges, keyOff2IdxOff, cwc)
		if err != nil {
			return nil, err
		}
		nextRange[p.GetPhysicalID()] = ranges
	}
	return nextRange, nil
}

func keyColumnsIncludeAllPartitionColumns(keyColumns []int, pe *tables.PartitionExpr) bool {
	tmp := make(map[int]struct{}, len(keyColumns))
	for _, offset := range keyColumns {
		tmp[offset] = struct{}{}
	}
	for _, offset := range pe.ColumnOffset {
		if _, ok := tmp[offset]; !ok {
			return false
		}
	}
	return true
}

func (builder *dataReaderBuilder) prunePartitionForInnerExecutor(tbl table.Table, schema *expression.Schema, partitionInfo *plannercore.PartitionInfo,
	lookUpContent []*indexJoinLookUpContent) (usedPartition []table.PhysicalTable, canPrune bool, contentPos []int64, err error) {
	partitionTbl := tbl.(table.PartitionedTable)

	// In index join, this is called by multiple goroutines simultaneously, but partitionPruning is not thread-safe.
	// Use once.Do to avoid DATA RACE here.
	// TODO: condition based pruning can be do in advance.
	condPruneResult, err := builder.partitionPruning(partitionTbl, partitionInfo.PruningConds, partitionInfo.PartitionNames, partitionInfo.Columns, partitionInfo.ColumnNames)
	if err != nil {
		return nil, false, nil, err
	}

	// check whether can runtime prune.
	type partitionExpr interface {
		PartitionExpr() (*tables.PartitionExpr, error)
	}
	pe, err := tbl.(partitionExpr).PartitionExpr()
	if err != nil {
		return nil, false, nil, err
	}

	// recalculate key column offsets
	if len(lookUpContent) == 0 {
		return nil, false, nil, nil
	}
	if lookUpContent[0].keyColIDs == nil {
		return nil, false, nil, plannercore.ErrInternal.GenWithStack("cannot get column IDs when dynamic pruning")
	}
	keyColOffsets := make([]int, len(lookUpContent[0].keyColIDs))
	for i, colID := range lookUpContent[0].keyColIDs {
		offset := -1
		for j, col := range partitionTbl.Cols() {
			if colID == col.ID {
				offset = j
				break
			}
		}
		if offset == -1 {
			return nil, false, nil, plannercore.ErrInternal.GenWithStack("invalid column offset when dynamic pruning")
		}
		keyColOffsets[i] = offset
	}

	offsetMap := make(map[int]bool)
	for _, offset := range keyColOffsets {
		offsetMap[offset] = true
	}
	for _, offset := range pe.ColumnOffset {
		if _, ok := offsetMap[offset]; !ok {
			return condPruneResult, false, nil, nil
		}
	}

	locateKey := make([]types.Datum, len(partitionTbl.Cols()))
	partitions := make(map[int64]table.PhysicalTable)
	contentPos = make([]int64, len(lookUpContent))
	for idx, content := range lookUpContent {
		for i, date := range content.keys {
			locateKey[keyColOffsets[i]] = date
		}
		p, err := partitionTbl.GetPartitionByRow(builder.ctx, locateKey)
		if err != nil {
			return nil, false, nil, err
		}
		if _, ok := partitions[p.GetPhysicalID()]; !ok {
			partitions[p.GetPhysicalID()] = p
		}
		contentPos[idx] = p.GetPhysicalID()
	}

	usedPartition = make([]table.PhysicalTable, 0, len(partitions))
	for _, p := range condPruneResult {
		if _, ok := partitions[p.GetPhysicalID()]; ok {
			usedPartition = append(usedPartition, p)
		}
	}
	return usedPartition, true, contentPos, nil
}

func buildNoRangeIndexReader(b *executorBuilder, v *plannercore.PhysicalIndexReader) (*IndexReaderExecutor, error) {
	dagReq, streaming, err := constructDAGReq(b.ctx, v.IndexPlans, kv.TiKV)
	if err != nil {
		return nil, err
	}
	is := v.IndexPlans[0].(*plannercore.PhysicalIndexScan)
	tbl, _ := b.is.TableByID(is.Table.ID)
	isPartition, physicalTableID := is.IsPartition()
	if isPartition {
		pt := tbl.(table.PartitionedTable)
		tbl = pt.GetPartition(physicalTableID)
	} else {
		physicalTableID = is.Table.ID
	}
	startTS, err := b.getSnapshotTS()
	if err != nil {
		return nil, err
	}
	e := &IndexReaderExecutor{
		baseExecutor:     newBaseExecutor(b.ctx, v.Schema(), v.ID()),
		dagPB:            dagReq,
		startTS:          startTS,
		readReplicaScope: b.readReplicaScope,
		isStaleness:      b.isStaleness,
		physicalTableID:  physicalTableID,
		table:            tbl,
		index:            is.Index,
		keepOrder:        is.KeepOrder,
		desc:             is.Desc,
		columns:          is.Columns,
		streaming:        streaming,
		corColInFilter:   b.corColInDistPlan(v.IndexPlans),
		corColInAccess:   b.corColInAccess(v.IndexPlans[0]),
		idxCols:          is.IdxCols,
		colLens:          is.IdxColLens,
		plans:            v.IndexPlans,
		outputColumns:    v.OutputColumns,
	}
	if containsLimit(dagReq.Executors) {
		e.feedback = statistics.NewQueryFeedback(0, nil, 0, is.Desc)
	} else {
		tblID := e.physicalTableID
		if b.ctx.GetSessionVars().UseDynamicPartitionPrune() {
			tblID = e.table.Meta().ID
		}
		e.feedback = statistics.NewQueryFeedback(tblID, is.Hist, int64(is.StatsCount()), is.Desc)
	}
	collect := statistics.CollectFeedback(b.ctx.GetSessionVars().StmtCtx, e.feedback, len(is.Ranges))
	// Do not collect the feedback when the table is the partition table.
	if collect && tbl.Meta().Partition != nil {
		collect = false
	}
	if !collect {
		e.feedback.Invalidate()
	}
	e.dagPB.CollectRangeCounts = &collect

	for _, col := range v.OutputColumns {
		dagReq.OutputOffsets = append(dagReq.OutputOffsets, uint32(col.Index))
	}

	return e, nil
}

func (b *executorBuilder) buildIndexReader(v *plannercore.PhysicalIndexReader) Executor {
	is := v.IndexPlans[0].(*plannercore.PhysicalIndexScan)
	if err := b.validCanReadTemporaryOrCacheTable(is.Table); err != nil {
		b.err = err
		return nil
	}

	ret, err := buildNoRangeIndexReader(b, v)
	if err != nil {
		b.err = err
		return nil
	}

	if ret.table.Meta().TempTableType != model.TempTableNone {
		ret.dummy = true
	}

	ret.ranges = is.Ranges
	sctx := b.ctx.GetSessionVars().StmtCtx
	sctx.IndexNames = append(sctx.IndexNames, is.Table.Name.O+":"+is.Index.Name.O)

	if !b.ctx.GetSessionVars().UseDynamicPartitionPrune() {
		return ret
	}
	// When isPartition is set, it means the union rewriting is done, so a partition reader is preferred.
	if ok, _ := is.IsPartition(); ok {
		return ret
	}

	pi := is.Table.GetPartitionInfo()
	if pi == nil {
		return ret
	}

	if is.Index.Global {
		return ret
	}

	tmp, _ := b.is.TableByID(is.Table.ID)
	tbl := tmp.(table.PartitionedTable)
	partitions, err := partitionPruning(b.ctx, tbl, v.PartitionInfo.PruningConds, v.PartitionInfo.PartitionNames, v.PartitionInfo.Columns, v.PartitionInfo.ColumnNames)
	if err != nil {
		b.err = err
		return nil
	}
	ret.partitions = partitions
	return ret
}

func buildTableReq(b *executorBuilder, schemaLen int, plans []plannercore.PhysicalPlan) (dagReq *tipb.DAGRequest, streaming bool, val table.Table, err error) {
	tableReq, tableStreaming, err := constructDAGReq(b.ctx, plans, kv.TiKV)
	if err != nil {
		return nil, false, nil, err
	}
	for i := 0; i < schemaLen; i++ {
		tableReq.OutputOffsets = append(tableReq.OutputOffsets, uint32(i))
	}
	ts := plans[0].(*plannercore.PhysicalTableScan)
	tbl, _ := b.is.TableByID(ts.Table.ID)
	isPartition, physicalTableID := ts.IsPartition()
	if isPartition {
		pt := tbl.(table.PartitionedTable)
		tbl = pt.GetPartition(physicalTableID)
	}
	return tableReq, tableStreaming, tbl, err
}

func buildIndexReq(ctx sessionctx.Context, schemaLen, handleLen int, plans []plannercore.PhysicalPlan) (dagReq *tipb.DAGRequest, streaming bool, err error) {
	indexReq, indexStreaming, err := constructDAGReq(ctx, plans, kv.TiKV)
	if err != nil {
		return nil, false, err
	}
	indexReq.OutputOffsets = []uint32{}
	for i := 0; i < handleLen; i++ {
		indexReq.OutputOffsets = append(indexReq.OutputOffsets, uint32(schemaLen+i))
	}
	if len(indexReq.OutputOffsets) == 0 {
		indexReq.OutputOffsets = []uint32{uint32(schemaLen)}
	}
	return indexReq, indexStreaming, err
}

func buildNoRangeIndexLookUpReader(b *executorBuilder, v *plannercore.PhysicalIndexLookUpReader) (*IndexLookUpExecutor, error) {
	is := v.IndexPlans[0].(*plannercore.PhysicalIndexScan)
	var handleLen int
	if len(v.CommonHandleCols) != 0 {
		handleLen = len(v.CommonHandleCols)
	} else {
		handleLen = 1
	}
	if is.Index.Global {
		// Should output pid col.
		handleLen++
	}
	indexReq, indexStreaming, err := buildIndexReq(b.ctx, len(is.Index.Columns), handleLen, v.IndexPlans)
	if err != nil {
		return nil, err
	}
	indexPaging := false
	if v.Paging {
		indexPaging = true
		indexStreaming = false
	}
	tableReq, tableStreaming, tbl, err := buildTableReq(b, v.Schema().Len(), v.TablePlans)
	if err != nil {
		return nil, err
	}
	ts := v.TablePlans[0].(*plannercore.PhysicalTableScan)
	startTS, err := b.getSnapshotTS()
	if err != nil {
		return nil, err
	}

	readerBuilder, err := b.newDataReaderBuilder(nil)
	if err != nil {
		return nil, err
	}

	e := &IndexLookUpExecutor{
		baseExecutor:      newBaseExecutor(b.ctx, v.Schema(), v.ID()),
		dagPB:             indexReq,
		startTS:           startTS,
		table:             tbl,
		index:             is.Index,
		keepOrder:         is.KeepOrder,
		desc:              is.Desc,
		tableRequest:      tableReq,
		columns:           ts.Columns,
		indexStreaming:    indexStreaming,
		tableStreaming:    tableStreaming,
		indexPaging:       indexPaging,
		dataReaderBuilder: readerBuilder,
		corColInIdxSide:   b.corColInDistPlan(v.IndexPlans),
		corColInTblSide:   b.corColInDistPlan(v.TablePlans),
		corColInAccess:    b.corColInAccess(v.IndexPlans[0]),
		idxCols:           is.IdxCols,
		colLens:           is.IdxColLens,
		idxPlans:          v.IndexPlans,
		tblPlans:          v.TablePlans,
		PushedLimit:       v.PushedLimit,
	}

	if containsLimit(indexReq.Executors) {
		e.feedback = statistics.NewQueryFeedback(0, nil, 0, is.Desc)
	} else {
		e.feedback = statistics.NewQueryFeedback(getFeedbackStatsTableID(e.ctx, tbl), is.Hist, int64(is.StatsCount()), is.Desc)
	}
	// Do not collect the feedback for table request.
	collectTable := false
	e.tableRequest.CollectRangeCounts = &collectTable
	collectIndex := statistics.CollectFeedback(b.ctx.GetSessionVars().StmtCtx, e.feedback, len(is.Ranges))
	// Do not collect the feedback when the table is the partition table.
	if collectIndex && tbl.Meta().GetPartitionInfo() != nil {
		collectIndex = false
	}
	if !collectIndex {
		e.feedback.Invalidate()
	}
	e.dagPB.CollectRangeCounts = &collectIndex
	if v.ExtraHandleCol != nil {
		e.handleIdx = append(e.handleIdx, v.ExtraHandleCol.Index)
		e.handleCols = []*expression.Column{v.ExtraHandleCol}
	} else {
		for _, handleCol := range v.CommonHandleCols {
			e.handleIdx = append(e.handleIdx, handleCol.Index)
		}
		e.handleCols = v.CommonHandleCols
		e.primaryKeyIndex = tables.FindPrimaryIndex(tbl.Meta())
	}
	return e, nil
}

func (b *executorBuilder) buildIndexLookUpReader(v *plannercore.PhysicalIndexLookUpReader) Executor {
	is := v.IndexPlans[0].(*plannercore.PhysicalIndexScan)
	if err := b.validCanReadTemporaryOrCacheTable(is.Table); err != nil {
		b.err = err
		return nil
	}

	ret, err := buildNoRangeIndexLookUpReader(b, v)
	if err != nil {
		b.err = err
		return nil
	}

	if ret.table.Meta().TempTableType != model.TempTableNone {
		ret.dummy = true
	}

	ts := v.TablePlans[0].(*plannercore.PhysicalTableScan)

	ret.ranges = is.Ranges
	executorCounterIndexLookUpExecutor.Inc()

	sctx := b.ctx.GetSessionVars().StmtCtx
	sctx.IndexNames = append(sctx.IndexNames, is.Table.Name.O+":"+is.Index.Name.O)
	sctx.TableIDs = append(sctx.TableIDs, ts.Table.ID)

	if !b.ctx.GetSessionVars().UseDynamicPartitionPrune() {
		return ret
	}

	if pi := is.Table.GetPartitionInfo(); pi == nil {
		return ret
	}

	if is.Index.Global {
		return ret
	}
	if ok, _ := is.IsPartition(); ok {
		// Already pruned when translated to logical union.
		return ret
	}

	tmp, _ := b.is.TableByID(is.Table.ID)
	tbl := tmp.(table.PartitionedTable)
	partitions, err := partitionPruning(b.ctx, tbl, v.PartitionInfo.PruningConds, v.PartitionInfo.PartitionNames, v.PartitionInfo.Columns, v.PartitionInfo.ColumnNames)
	if err != nil {
		b.err = err
		return nil
	}
	ret.partitionTableMode = true
	ret.prunedPartitions = partitions
	return ret
}

func buildNoRangeIndexMergeReader(b *executorBuilder, v *plannercore.PhysicalIndexMergeReader) (*IndexMergeReaderExecutor, error) {
	partialPlanCount := len(v.PartialPlans)
	partialReqs := make([]*tipb.DAGRequest, 0, partialPlanCount)
	partialStreamings := make([]bool, 0, partialPlanCount)
	indexes := make([]*model.IndexInfo, 0, partialPlanCount)
	descs := make([]bool, 0, partialPlanCount)
	feedbacks := make([]*statistics.QueryFeedback, 0, partialPlanCount)
	ts := v.TablePlans[0].(*plannercore.PhysicalTableScan)
	isCorColInPartialFilters := make([]bool, 0, partialPlanCount)
	isCorColInPartialAccess := make([]bool, 0, partialPlanCount)
	for i := 0; i < partialPlanCount; i++ {
		var tempReq *tipb.DAGRequest
		var tempStreaming bool
		var err error

		feedback := statistics.NewQueryFeedback(0, nil, 0, ts.Desc)
		feedback.Invalidate()
		feedbacks = append(feedbacks, feedback)

		if is, ok := v.PartialPlans[i][0].(*plannercore.PhysicalIndexScan); ok {
			tempReq, tempStreaming, err = buildIndexReq(b.ctx, len(is.Index.Columns), ts.HandleCols.NumCols(), v.PartialPlans[i])
			descs = append(descs, is.Desc)
			indexes = append(indexes, is.Index)
		} else {
			ts := v.PartialPlans[i][0].(*plannercore.PhysicalTableScan)
			tempReq, tempStreaming, _, err = buildTableReq(b, len(ts.Columns), v.PartialPlans[i])
			descs = append(descs, ts.Desc)
			indexes = append(indexes, nil)
		}
		if err != nil {
			return nil, err
		}
		collect := false
		tempReq.CollectRangeCounts = &collect
		partialReqs = append(partialReqs, tempReq)
		partialStreamings = append(partialStreamings, tempStreaming)
		isCorColInPartialFilters = append(isCorColInPartialFilters, b.corColInDistPlan(v.PartialPlans[i]))
		isCorColInPartialAccess = append(isCorColInPartialAccess, b.corColInAccess(v.PartialPlans[i][0]))
	}
	tableReq, tableStreaming, tblInfo, err := buildTableReq(b, v.Schema().Len(), v.TablePlans)
	isCorColInTableFilter := b.corColInDistPlan(v.TablePlans)
	if err != nil {
		return nil, err
	}
	startTS, err := b.getSnapshotTS()
	if err != nil {
		return nil, err
	}

	readerBuilder, err := b.newDataReaderBuilder(nil)
	if err != nil {
		return nil, err
	}

	e := &IndexMergeReaderExecutor{
		baseExecutor:             newBaseExecutor(b.ctx, v.Schema(), v.ID()),
		dagPBs:                   partialReqs,
		startTS:                  startTS,
		table:                    tblInfo,
		indexes:                  indexes,
		descs:                    descs,
		tableRequest:             tableReq,
		columns:                  ts.Columns,
		partialStreamings:        partialStreamings,
		tableStreaming:           tableStreaming,
		partialPlans:             v.PartialPlans,
		tblPlans:                 v.TablePlans,
		dataReaderBuilder:        readerBuilder,
		feedbacks:                feedbacks,
		handleCols:               ts.HandleCols,
		isCorColInPartialFilters: isCorColInPartialFilters,
		isCorColInTableFilter:    isCorColInTableFilter,
		isCorColInPartialAccess:  isCorColInPartialAccess,
	}
	collectTable := false
	e.tableRequest.CollectRangeCounts = &collectTable
	return e, nil
}

func (b *executorBuilder) buildIndexMergeReader(v *plannercore.PhysicalIndexMergeReader) Executor {
	ts := v.TablePlans[0].(*plannercore.PhysicalTableScan)
	if err := b.validCanReadTemporaryOrCacheTable(ts.Table); err != nil {
		b.err = err
		return nil
	}

	ret, err := buildNoRangeIndexMergeReader(b, v)
	if err != nil {
		b.err = err
		return nil
	}
	ret.ranges = make([][]*ranger.Range, 0, len(v.PartialPlans))
	sctx := b.ctx.GetSessionVars().StmtCtx
	for i := 0; i < len(v.PartialPlans); i++ {
		if is, ok := v.PartialPlans[i][0].(*plannercore.PhysicalIndexScan); ok {
			ret.ranges = append(ret.ranges, is.Ranges)
			sctx.IndexNames = append(sctx.IndexNames, is.Table.Name.O+":"+is.Index.Name.O)
		} else {
			ret.ranges = append(ret.ranges, v.PartialPlans[i][0].(*plannercore.PhysicalTableScan).Ranges)
			if ret.table.Meta().IsCommonHandle {
				tblInfo := ret.table.Meta()
				sctx.IndexNames = append(sctx.IndexNames, tblInfo.Name.O+":"+tables.FindPrimaryIndex(tblInfo).Name.O)
			}
		}
	}
	sctx.TableIDs = append(sctx.TableIDs, ts.Table.ID)
	executorCounterIndexMergeReaderExecutor.Inc()

	if !b.ctx.GetSessionVars().UseDynamicPartitionPrune() {
		return ret
	}

	if pi := ts.Table.GetPartitionInfo(); pi == nil {
		return ret
	}

	tmp, _ := b.is.TableByID(ts.Table.ID)
	partitions, err := partitionPruning(b.ctx, tmp.(table.PartitionedTable), v.PartitionInfo.PruningConds, v.PartitionInfo.PartitionNames, v.PartitionInfo.Columns, v.PartitionInfo.ColumnNames)
	if err != nil {
		b.err = err
		return nil
	}
	ret.partitionTableMode, ret.prunedPartitions = true, partitions
	return ret
}

// dataReaderBuilder build an executor.
// The executor can be used to read data in the ranges which are constructed by datums.
// Differences from executorBuilder:
// 1. dataReaderBuilder calculate data range from argument, rather than plan.
// 2. the result executor is already opened.
type dataReaderBuilder struct {
	plannercore.Plan
	*executorBuilder

	selectResultHook // for testing
	once             struct {
		sync.Once
		condPruneResult []table.PhysicalTable
		err             error
	}
}

type mockPhysicalIndexReader struct {
	plannercore.PhysicalPlan

	e Executor
}

func (builder *dataReaderBuilder) buildExecutorForIndexJoin(ctx context.Context, lookUpContents []*indexJoinLookUpContent,
	IndexRanges []*ranger.Range, keyOff2IdxOff []int, cwc *plannercore.ColWithCmpFuncManager, canReorderHandles bool, memTracker *memory.Tracker, interruptSignal *atomic.Value) (Executor, error) {
	return builder.buildExecutorForIndexJoinInternal(ctx, builder.Plan, lookUpContents, IndexRanges, keyOff2IdxOff, cwc, canReorderHandles, memTracker, interruptSignal)
}

func (builder *dataReaderBuilder) buildExecutorForIndexJoinInternal(ctx context.Context, plan plannercore.Plan, lookUpContents []*indexJoinLookUpContent,
	IndexRanges []*ranger.Range, keyOff2IdxOff []int, cwc *plannercore.ColWithCmpFuncManager, canReorderHandles bool, memTracker *memory.Tracker, interruptSignal *atomic.Value) (Executor, error) {
	switch v := plan.(type) {
	case *plannercore.PhysicalTableReader:
		return builder.buildTableReaderForIndexJoin(ctx, v, lookUpContents, IndexRanges, keyOff2IdxOff, cwc, canReorderHandles, memTracker, interruptSignal)
	case *plannercore.PhysicalIndexReader:
		return builder.buildIndexReaderForIndexJoin(ctx, v, lookUpContents, IndexRanges, keyOff2IdxOff, cwc, memTracker, interruptSignal)
	case *plannercore.PhysicalIndexLookUpReader:
		return builder.buildIndexLookUpReaderForIndexJoin(ctx, v, lookUpContents, IndexRanges, keyOff2IdxOff, cwc, memTracker, interruptSignal)
	case *plannercore.PhysicalUnionScan:
		return builder.buildUnionScanForIndexJoin(ctx, v, lookUpContents, IndexRanges, keyOff2IdxOff, cwc, canReorderHandles, memTracker, interruptSignal)
	// The inner child of IndexJoin might be Projection when a combination of the following conditions is true:
	// 	1. The inner child fetch data using indexLookupReader
	// 	2. PK is not handle
	// 	3. The inner child needs to keep order
	// In this case, an extra column tidb_rowid will be appended in the output result of IndexLookupReader(see copTask.doubleReadNeedProj).
	// Then we need a Projection upon IndexLookupReader to prune the redundant column.
	case *plannercore.PhysicalProjection:
		return builder.buildProjectionForIndexJoin(ctx, v, lookUpContents, IndexRanges, keyOff2IdxOff, cwc, memTracker, interruptSignal)
	// Need to support physical selection because after PR 16389, TiDB will push down all the expr supported by TiKV or TiFlash
	// in predicate push down stage, so if there is an expr which only supported by TiFlash, a physical selection will be added after index read
	case *plannercore.PhysicalSelection:
		childExec, err := builder.buildExecutorForIndexJoinInternal(ctx, v.Children()[0], lookUpContents, IndexRanges, keyOff2IdxOff, cwc, canReorderHandles, memTracker, interruptSignal)
		if err != nil {
			return nil, err
		}
		exec := &SelectionExec{
			baseExecutor: newBaseExecutor(builder.ctx, v.Schema(), v.ID(), childExec),
			filters:      v.Conditions,
		}
		err = exec.open(ctx)
		return exec, err
	case *mockPhysicalIndexReader:
		return v.e, nil
	}
	return nil, errors.New("Wrong plan type for dataReaderBuilder")
}

func (builder *dataReaderBuilder) buildUnionScanForIndexJoin(ctx context.Context, v *plannercore.PhysicalUnionScan,
	values []*indexJoinLookUpContent, indexRanges []*ranger.Range, keyOff2IdxOff []int,
	cwc *plannercore.ColWithCmpFuncManager, canReorderHandles bool, memTracker *memory.Tracker, interruptSignal *atomic.Value) (Executor, error) {
	childBuilder, err := builder.newDataReaderBuilder(v.Children()[0])
	if err != nil {
		return nil, err
	}

	reader, err := childBuilder.buildExecutorForIndexJoin(ctx, values, indexRanges, keyOff2IdxOff, cwc, canReorderHandles, memTracker, interruptSignal)
	if err != nil {
		return nil, err
	}

	ret := builder.buildUnionScanFromReader(reader, v)
	if us, ok := ret.(*UnionScanExec); ok {
		err = us.open(ctx)
	}
	return ret, err
}

func (builder *dataReaderBuilder) buildTableReaderForIndexJoin(ctx context.Context, v *plannercore.PhysicalTableReader,
	lookUpContents []*indexJoinLookUpContent, indexRanges []*ranger.Range, keyOff2IdxOff []int,
	cwc *plannercore.ColWithCmpFuncManager, canReorderHandles bool, memTracker *memory.Tracker, interruptSignal *atomic.Value) (Executor, error) {
	e, err := buildNoRangeTableReader(builder.executorBuilder, v)
	if err != nil {
		return nil, err
	}
	tbInfo := e.table.Meta()
	if tbInfo.GetPartitionInfo() == nil || !builder.ctx.GetSessionVars().UseDynamicPartitionPrune() {
		if v.IsCommonHandle {
			kvRanges, err := buildKvRangesForIndexJoin(e.ctx, getPhysicalTableID(e.table), -1, lookUpContents, indexRanges, keyOff2IdxOff, cwc, memTracker, interruptSignal)
			if err != nil {
				return nil, err
			}
			return builder.buildTableReaderFromKvRanges(ctx, e, kvRanges)
		}
		handles, _ := dedupHandles(lookUpContents)
		return builder.buildTableReaderFromHandles(ctx, e, handles, canReorderHandles)
	}
	tbl, _ := builder.is.TableByID(tbInfo.ID)
	pt := tbl.(table.PartitionedTable)
	pe, err := tbl.(interface {
		PartitionExpr() (*tables.PartitionExpr, error)
	}).PartitionExpr()
	if err != nil {
		return nil, err
	}
	partitionInfo := &v.PartitionInfo
	usedPartitionList, err := builder.partitionPruning(pt, partitionInfo.PruningConds, partitionInfo.PartitionNames, partitionInfo.Columns, partitionInfo.ColumnNames)
	if err != nil {
		return nil, err
	}
	usedPartitions := make(map[int64]table.PhysicalTable, len(usedPartitionList))
	for _, p := range usedPartitionList {
		usedPartitions[p.GetPhysicalID()] = p
	}
	var kvRanges []kv.KeyRange
	if v.IsCommonHandle {
		if len(lookUpContents) > 0 && keyColumnsIncludeAllPartitionColumns(lookUpContents[0].keyCols, pe) {
			locateKey := make([]types.Datum, e.Schema().Len())
			kvRanges = make([]kv.KeyRange, 0, len(lookUpContents))
			// lookUpContentsByPID groups lookUpContents by pid(partition) so that kv ranges for same partition can be merged.
			lookUpContentsByPID := make(map[int64][]*indexJoinLookUpContent)
			for _, content := range lookUpContents {
				for i, date := range content.keys {
					locateKey[content.keyCols[i]] = date
				}
				p, err := pt.GetPartitionByRow(e.ctx, locateKey)
				if err != nil {
					return nil, err
				}
				pid := p.GetPhysicalID()
				if _, ok := usedPartitions[pid]; !ok {
					continue
				}
				lookUpContentsByPID[pid] = append(lookUpContentsByPID[pid], content)
			}
			for pid, contents := range lookUpContentsByPID {
				// buildKvRanges for each partition.
				tmp, err := buildKvRangesForIndexJoin(e.ctx, pid, -1, contents, indexRanges, keyOff2IdxOff, cwc, nil, interruptSignal)
				if err != nil {
					return nil, err
				}
				kvRanges = append(kvRanges, tmp...)
			}
		} else {
			kvRanges = make([]kv.KeyRange, 0, len(usedPartitions)*len(lookUpContents))
			for _, p := range usedPartitionList {
				tmp, err := buildKvRangesForIndexJoin(e.ctx, p.GetPhysicalID(), -1, lookUpContents, indexRanges, keyOff2IdxOff, cwc, memTracker, interruptSignal)
				if err != nil {
					return nil, err
				}
				kvRanges = append(tmp, kvRanges...)
			}
		}
		return builder.buildTableReaderFromKvRanges(ctx, e, kvRanges)
	}

	handles, lookUpContents := dedupHandles(lookUpContents)

	if len(lookUpContents) > 0 && keyColumnsIncludeAllPartitionColumns(lookUpContents[0].keyCols, pe) {
		locateKey := make([]types.Datum, e.Schema().Len())
		kvRanges = make([]kv.KeyRange, 0, len(lookUpContents))
		for _, content := range lookUpContents {
			for i, date := range content.keys {
				locateKey[content.keyCols[i]] = date
			}
			p, err := pt.GetPartitionByRow(e.ctx, locateKey)
			if err != nil {
				return nil, err
			}
			pid := p.GetPhysicalID()
			if _, ok := usedPartitions[pid]; !ok {
				continue
			}
			handle := kv.IntHandle(content.keys[0].GetInt64())
			tmp := distsql.TableHandlesToKVRanges(pid, []kv.Handle{handle})
			kvRanges = append(kvRanges, tmp...)
		}
	} else {
		for _, p := range usedPartitionList {
			tmp := distsql.TableHandlesToKVRanges(p.GetPhysicalID(), handles)
			kvRanges = append(kvRanges, tmp...)
		}
	}
	return builder.buildTableReaderFromKvRanges(ctx, e, kvRanges)
}

func dedupHandles(lookUpContents []*indexJoinLookUpContent) ([]kv.Handle, []*indexJoinLookUpContent) {
	handles := make([]kv.Handle, 0, len(lookUpContents))
	validLookUpContents := make([]*indexJoinLookUpContent, 0, len(lookUpContents))
	for _, content := range lookUpContents {
		isValidHandle := true
		handle := kv.IntHandle(content.keys[0].GetInt64())
		for _, key := range content.keys {
			if handle.IntValue() != key.GetInt64() {
				isValidHandle = false
				break
			}
		}
		if isValidHandle {
			handles = append(handles, handle)
			validLookUpContents = append(validLookUpContents, content)
		}
	}
	return handles, validLookUpContents
}

type kvRangeBuilderFromRangeAndPartition struct {
	sctx       sessionctx.Context
	partitions []table.PhysicalTable
}

func (h kvRangeBuilderFromRangeAndPartition) buildKeyRangeSeparately(ranges []*ranger.Range) ([]int64, [][]kv.KeyRange, error) {
	ret := make([][]kv.KeyRange, 0, len(h.partitions))
	pids := make([]int64, 0, len(h.partitions))
	for _, p := range h.partitions {
		pid := p.GetPhysicalID()
		meta := p.Meta()
		kvRange, err := distsql.TableHandleRangesToKVRanges(h.sctx.GetSessionVars().StmtCtx, []int64{pid}, meta != nil && meta.IsCommonHandle, ranges, nil)
		if err != nil {
			return nil, nil, err
		}
		pids = append(pids, pid)
		ret = append(ret, kvRange)
	}
	return pids, ret, nil
}

func (h kvRangeBuilderFromRangeAndPartition) buildKeyRange(ranges []*ranger.Range) ([]kv.KeyRange, error) {
	var ret []kv.KeyRange
	for _, p := range h.partitions {
		pid := p.GetPhysicalID()
		meta := p.Meta()
		kvRange, err := distsql.TableHandleRangesToKVRanges(h.sctx.GetSessionVars().StmtCtx, []int64{pid}, meta != nil && meta.IsCommonHandle, ranges, nil)
		if err != nil {
			return nil, err
		}
		ret = append(ret, kvRange...)
	}
	return ret, nil
}

func (builder *dataReaderBuilder) buildTableReaderBase(ctx context.Context, e *TableReaderExecutor, reqBuilderWithRange distsql.RequestBuilder) (*TableReaderExecutor, error) {
	startTS, err := builder.getSnapshotTS()
	if err != nil {
		return nil, err
	}
	kvReq, err := reqBuilderWithRange.
		SetDAGRequest(e.dagPB).
		SetStartTS(startTS).
		SetDesc(e.desc).
		SetKeepOrder(e.keepOrder).
		SetStreaming(e.streaming).
		SetReadReplicaScope(e.readReplicaScope).
		SetIsStaleness(e.isStaleness).
		SetFromSessionVars(e.ctx.GetSessionVars()).
		SetFromInfoSchema(e.ctx.GetInfoSchema()).
		Build()
	if err != nil {
		return nil, err
	}
	e.kvRanges = append(e.kvRanges, kvReq.KeyRanges...)
	e.resultHandler = &tableResultHandler{}
	result, err := builder.SelectResult(ctx, builder.ctx, kvReq, retTypes(e), e.feedback, getPhysicalPlanIDs(e.plans), e.id)
	if err != nil {
		return nil, err
	}
	e.resultHandler.open(nil, result)
	return e, nil
}

func (builder *dataReaderBuilder) buildTableReaderFromHandles(ctx context.Context, e *TableReaderExecutor, handles []kv.Handle, canReorderHandles bool) (*TableReaderExecutor, error) {
	if canReorderHandles {
		sort.Slice(handles, func(i, j int) bool {
			return handles[i].Compare(handles[j]) < 0
		})
	}
	var b distsql.RequestBuilder
	if len(handles) > 0 {
		if _, ok := handles[0].(kv.PartitionHandle); ok {
			b.SetPartitionsAndHandles(handles)
		} else {
			b.SetTableHandles(getPhysicalTableID(e.table), handles)
		}
	}
	return builder.buildTableReaderBase(ctx, e, b)
}

func (builder *dataReaderBuilder) buildTableReaderFromKvRanges(ctx context.Context, e *TableReaderExecutor, ranges []kv.KeyRange) (Executor, error) {
	var b distsql.RequestBuilder
	b.SetKeyRanges(ranges)
	return builder.buildTableReaderBase(ctx, e, b)
}

func (builder *dataReaderBuilder) buildIndexReaderForIndexJoin(ctx context.Context, v *plannercore.PhysicalIndexReader,
	lookUpContents []*indexJoinLookUpContent, indexRanges []*ranger.Range, keyOff2IdxOff []int, cwc *plannercore.ColWithCmpFuncManager, memoryTracker *memory.Tracker, interruptSignal *atomic.Value) (Executor, error) {
	e, err := buildNoRangeIndexReader(builder.executorBuilder, v)
	if err != nil {
		return nil, err
	}
	tbInfo := e.table.Meta()
	if tbInfo.GetPartitionInfo() == nil || !builder.ctx.GetSessionVars().UseDynamicPartitionPrune() {
		kvRanges, err := buildKvRangesForIndexJoin(e.ctx, e.physicalTableID, e.index.ID, lookUpContents, indexRanges, keyOff2IdxOff, cwc, memoryTracker, interruptSignal)
		if err != nil {
			return nil, err
		}
		err = e.open(ctx, kvRanges)
		return e, err
	}

	tbl, _ := builder.executorBuilder.is.TableByID(tbInfo.ID)
	usedPartition, canPrune, contentPos, err := builder.prunePartitionForInnerExecutor(tbl, e.Schema(), &v.PartitionInfo, lookUpContents)
	if err != nil {
		return nil, err
	}
	if len(usedPartition) != 0 {
		if canPrune {
			rangeMap, err := buildIndexRangeForEachPartition(e.ctx, usedPartition, contentPos, lookUpContents, indexRanges, keyOff2IdxOff, cwc)
			if err != nil {
				return nil, err
			}
			e.partitions = usedPartition
			e.ranges = indexRanges
			e.partRangeMap = rangeMap
		} else {
			e.partitions = usedPartition
			if e.ranges, err = buildRangesForIndexJoin(e.ctx, lookUpContents, indexRanges, keyOff2IdxOff, cwc); err != nil {
				return nil, err
			}
		}
		if err := e.Open(ctx); err != nil {
			return nil, err
		}
		return e, nil
	}
	ret := &TableDualExec{baseExecutor: *e.base()}
	err = ret.Open(ctx)
	return ret, err
}

func (builder *dataReaderBuilder) buildIndexLookUpReaderForIndexJoin(ctx context.Context, v *plannercore.PhysicalIndexLookUpReader,
	lookUpContents []*indexJoinLookUpContent, indexRanges []*ranger.Range, keyOff2IdxOff []int, cwc *plannercore.ColWithCmpFuncManager, memTracker *memory.Tracker, interruptSignal *atomic.Value) (Executor, error) {
	e, err := buildNoRangeIndexLookUpReader(builder.executorBuilder, v)
	if err != nil {
		return nil, err
	}

	tbInfo := e.table.Meta()
	if tbInfo.GetPartitionInfo() == nil || !builder.ctx.GetSessionVars().UseDynamicPartitionPrune() {
		e.kvRanges, err = buildKvRangesForIndexJoin(e.ctx, getPhysicalTableID(e.table), e.index.ID, lookUpContents, indexRanges, keyOff2IdxOff, cwc, memTracker, interruptSignal)
		if err != nil {
			return nil, err
		}
		err = e.open(ctx)
		return e, err
	}
	tbl, _ := builder.executorBuilder.is.TableByID(tbInfo.ID)
	usedPartition, canPrune, contentPos, err := builder.prunePartitionForInnerExecutor(tbl, e.Schema(), &v.PartitionInfo, lookUpContents)
	if err != nil {
		return nil, err
	}
	if len(usedPartition) != 0 {
		if canPrune {
			rangeMap, err := buildIndexRangeForEachPartition(e.ctx, usedPartition, contentPos, lookUpContents, indexRanges, keyOff2IdxOff, cwc)
			if err != nil {
				return nil, err
			}
			e.prunedPartitions = usedPartition
			e.ranges = indexRanges
			e.partitionRangeMap = rangeMap
		} else {
			e.prunedPartitions = usedPartition
			e.ranges, err = buildRangesForIndexJoin(e.ctx, lookUpContents, indexRanges, keyOff2IdxOff, cwc)
			if err != nil {
				return nil, err
			}
		}
		e.partitionTableMode = true
		if err := e.Open(ctx); err != nil {
			return nil, err
		}
		return e, err
	}
	ret := &TableDualExec{baseExecutor: *e.base()}
	err = ret.Open(ctx)
	return ret, err
}

func (builder *dataReaderBuilder) buildProjectionForIndexJoin(ctx context.Context, v *plannercore.PhysicalProjection,
	lookUpContents []*indexJoinLookUpContent, indexRanges []*ranger.Range, keyOff2IdxOff []int, cwc *plannercore.ColWithCmpFuncManager, memTracker *memory.Tracker, interruptSignal *atomic.Value) (Executor, error) {
	var (
		childExec Executor
		err       error
	)
	switch op := v.Children()[0].(type) {
	case *plannercore.PhysicalIndexLookUpReader:
		if childExec, err = builder.buildIndexLookUpReaderForIndexJoin(ctx, op, lookUpContents, indexRanges, keyOff2IdxOff, cwc, memTracker, interruptSignal); err != nil {
			return nil, err
		}
	case *plannercore.PhysicalTableReader:
		if childExec, err = builder.buildTableReaderForIndexJoin(ctx, op, lookUpContents, indexRanges, keyOff2IdxOff, cwc, true, memTracker, interruptSignal); err != nil {
			return nil, err
		}
	default:
		return nil, errors.Errorf("inner child of Projection should be IndexLookupReader/TableReader, but got %T", v.Children()[0])
	}

	e := &ProjectionExec{
		baseExecutor:     newBaseExecutor(builder.ctx, v.Schema(), v.ID(), childExec),
		numWorkers:       int64(builder.ctx.GetSessionVars().ProjectionConcurrency()),
		evaluatorSuit:    expression.NewEvaluatorSuite(v.Exprs, v.AvoidColumnEvaluator),
		calculateNoDelay: v.CalculateNoDelay,
	}

	// If the calculation row count for this Projection operator is smaller
	// than a Chunk size, we turn back to the un-parallel Projection
	// implementation to reduce the goroutine overhead.
	if int64(v.StatsCount()) < int64(builder.ctx.GetSessionVars().MaxChunkSize) {
		e.numWorkers = 0
	}
	err = e.open(ctx)

	return e, err
}

// buildRangesForIndexJoin builds kv ranges for index join when the inner plan is index scan plan.
func buildRangesForIndexJoin(ctx sessionctx.Context, lookUpContents []*indexJoinLookUpContent,
	ranges []*ranger.Range, keyOff2IdxOff []int, cwc *plannercore.ColWithCmpFuncManager) ([]*ranger.Range, error) {
	retRanges := make([]*ranger.Range, 0, len(ranges)*len(lookUpContents))
	lastPos := len(ranges[0].LowVal) - 1
	tmpDatumRanges := make([]*ranger.Range, 0, len(lookUpContents))
	for _, content := range lookUpContents {
		for _, ran := range ranges {
			for keyOff, idxOff := range keyOff2IdxOff {
				ran.LowVal[idxOff] = content.keys[keyOff]
				ran.HighVal[idxOff] = content.keys[keyOff]
			}
		}
		if cwc == nil {
			// A deep copy is need here because the old []*range.Range is overwriten
			for _, ran := range ranges {
				retRanges = append(retRanges, ran.Clone())
			}
			continue
		}
		nextColRanges, err := cwc.BuildRangesByRow(ctx, content.row)
		if err != nil {
			return nil, err
		}
		for _, nextColRan := range nextColRanges {
			for _, ran := range ranges {
				ran.LowVal[lastPos] = nextColRan.LowVal[0]
				ran.HighVal[lastPos] = nextColRan.HighVal[0]
				ran.LowExclude = nextColRan.LowExclude
				ran.HighExclude = nextColRan.HighExclude
				ran.Collators = nextColRan.Collators
				tmpDatumRanges = append(tmpDatumRanges, ran.Clone())
			}
		}
	}

	if cwc == nil {
		return retRanges, nil
	}

	return ranger.UnionRanges(ctx, tmpDatumRanges, true)
}

// buildKvRangesForIndexJoin builds kv ranges for index join when the inner plan is index scan plan.
func buildKvRangesForIndexJoin(ctx sessionctx.Context, tableID, indexID int64, lookUpContents []*indexJoinLookUpContent,
	ranges []*ranger.Range, keyOff2IdxOff []int, cwc *plannercore.ColWithCmpFuncManager, memTracker *memory.Tracker, interruptSignal *atomic.Value) (_ []kv.KeyRange, err error) {
	kvRanges := make([]kv.KeyRange, 0, len(ranges)*len(lookUpContents))
	lastPos := len(ranges[0].LowVal) - 1
	sc := ctx.GetSessionVars().StmtCtx
	tmpDatumRanges := make([]*ranger.Range, 0, len(lookUpContents))
	for _, content := range lookUpContents {
		for _, ran := range ranges {
			for keyOff, idxOff := range keyOff2IdxOff {
				ran.LowVal[idxOff] = content.keys[keyOff]
				ran.HighVal[idxOff] = content.keys[keyOff]
			}
		}
		if cwc == nil {
			// Index id is -1 means it's a common handle.
			var tmpKvRanges []kv.KeyRange
			var err error
			if indexID == -1 {
				tmpKvRanges, err = distsql.CommonHandleRangesToKVRanges(sc, []int64{tableID}, ranges)
			} else {
				tmpKvRanges, err = distsql.IndexRangesToKVRangesWithInterruptSignal(sc, tableID, indexID, ranges, nil, memTracker, interruptSignal)
			}
			if err != nil {
				return nil, err
			}
			kvRanges = append(kvRanges, tmpKvRanges...)
			continue
		}
		nextColRanges, err := cwc.BuildRangesByRow(ctx, content.row)
		if err != nil {
			return nil, err
		}
		for _, nextColRan := range nextColRanges {
			for _, ran := range ranges {
				ran.LowVal[lastPos] = nextColRan.LowVal[0]
				ran.HighVal[lastPos] = nextColRan.HighVal[0]
				ran.LowExclude = nextColRan.LowExclude
				ran.HighExclude = nextColRan.HighExclude
				ran.Collators = nextColRan.Collators
				tmpDatumRanges = append(tmpDatumRanges, ran.Clone())
			}
		}
	}
	if len(kvRanges) != 0 && memTracker != nil {
		memTracker.Consume(int64(2 * cap(kvRanges[0].StartKey) * len(kvRanges)))
	}
	if len(tmpDatumRanges) != 0 && memTracker != nil {
		memTracker.Consume(2 * int64(len(tmpDatumRanges)) * types.EstimatedMemUsage(tmpDatumRanges[0].LowVal, len(tmpDatumRanges)))
	}
	if cwc == nil {
		sort.Slice(kvRanges, func(i, j int) bool {
			return bytes.Compare(kvRanges[i].StartKey, kvRanges[j].StartKey) < 0
		})
		return kvRanges, nil
	}

	tmpDatumRanges, err = ranger.UnionRanges(ctx, tmpDatumRanges, true)
	if err != nil {
		return nil, err
	}
	// Index id is -1 means it's a common handle.
	if indexID == -1 {
		return distsql.CommonHandleRangesToKVRanges(ctx.GetSessionVars().StmtCtx, []int64{tableID}, tmpDatumRanges)
	}
	return distsql.IndexRangesToKVRangesWithInterruptSignal(ctx.GetSessionVars().StmtCtx, tableID, indexID, tmpDatumRanges, nil, memTracker, interruptSignal)
}

func (b *executorBuilder) buildWindow(v *plannercore.PhysicalWindow) Executor {
	childExec := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	base := newBaseExecutor(b.ctx, v.Schema(), v.ID(), childExec)
	groupByItems := make([]expression.Expression, 0, len(v.PartitionBy))
	for _, item := range v.PartitionBy {
		groupByItems = append(groupByItems, item.Col)
	}
	orderByCols := make([]*expression.Column, 0, len(v.OrderBy))
	for _, item := range v.OrderBy {
		orderByCols = append(orderByCols, item.Col)
	}
	windowFuncs := make([]aggfuncs.AggFunc, 0, len(v.WindowFuncDescs))
	partialResults := make([]aggfuncs.PartialResult, 0, len(v.WindowFuncDescs))
	resultColIdx := v.Schema().Len() - len(v.WindowFuncDescs)
	for _, desc := range v.WindowFuncDescs {
		aggDesc, err := aggregation.NewAggFuncDescForWindowFunc(b.ctx, desc, false)
		if err != nil {
			b.err = err
			return nil
		}
		agg := aggfuncs.BuildWindowFunctions(b.ctx, aggDesc, resultColIdx, orderByCols)
		windowFuncs = append(windowFuncs, agg)
		partialResult, _ := agg.AllocPartialResult()
		partialResults = append(partialResults, partialResult)
		resultColIdx++
	}

	if b.ctx.GetSessionVars().EnablePipelinedWindowExec {
		exec := &PipelinedWindowExec{
			baseExecutor:   base,
			groupChecker:   newVecGroupChecker(b.ctx, groupByItems),
			numWindowFuncs: len(v.WindowFuncDescs),
		}

		exec.windowFuncs = windowFuncs
		exec.partialResults = partialResults
		if v.Frame == nil {
			exec.start = &plannercore.FrameBound{
				Type:      ast.Preceding,
				UnBounded: true,
			}
			exec.end = &plannercore.FrameBound{
				Type:      ast.Following,
				UnBounded: true,
			}
		} else {
			exec.start = v.Frame.Start
			exec.end = v.Frame.End
			if v.Frame.Type == ast.Ranges {
				cmpResult := int64(-1)
				if len(v.OrderBy) > 0 && v.OrderBy[0].Desc {
					cmpResult = 1
				}
				exec.orderByCols = orderByCols
				exec.expectedCmpResult = cmpResult
				exec.isRangeFrame = true
			}
		}
		return exec
	}
	var processor windowProcessor
	if v.Frame == nil {
		processor = &aggWindowProcessor{
			windowFuncs:    windowFuncs,
			partialResults: partialResults,
		}
	} else if v.Frame.Type == ast.Rows {
		processor = &rowFrameWindowProcessor{
			windowFuncs:    windowFuncs,
			partialResults: partialResults,
			start:          v.Frame.Start,
			end:            v.Frame.End,
		}
	} else {
		cmpResult := int64(-1)
		if len(v.OrderBy) > 0 && v.OrderBy[0].Desc {
			cmpResult = 1
		}
		processor = &rangeFrameWindowProcessor{
			windowFuncs:       windowFuncs,
			partialResults:    partialResults,
			start:             v.Frame.Start,
			end:               v.Frame.End,
			orderByCols:       orderByCols,
			expectedCmpResult: cmpResult,
		}
	}
	return &WindowExec{baseExecutor: base,
		processor:      processor,
		groupChecker:   newVecGroupChecker(b.ctx, groupByItems),
		numWindowFuncs: len(v.WindowFuncDescs),
	}
}

func (b *executorBuilder) buildShuffle(v *plannercore.PhysicalShuffle) *ShuffleExec {
	base := newBaseExecutor(b.ctx, v.Schema(), v.ID())
	shuffle := &ShuffleExec{
		baseExecutor: base,
		concurrency:  v.Concurrency,
	}

	splitters := make([]partitionSplitter, len(v.ByItemArrays))
	switch v.SplitterType {
	case plannercore.PartitionHashSplitterType:
		for i, byItems := range v.ByItemArrays {
			splitters[i] = buildPartitionHashSplitter(shuffle.concurrency, byItems)
		}
	case plannercore.PartitionRangeSplitterType:
		for i, byItems := range v.ByItemArrays {
			splitters[i] = buildPartitionRangeSplitter(b.ctx, shuffle.concurrency, byItems)
		}
	default:
		panic("Not implemented. Should not reach here.")
	}
	shuffle.splitters = splitters

	shuffle.dataSources = make([]Executor, len(v.DataSources))
	for i, dataSource := range v.DataSources {
		shuffle.dataSources[i] = b.build(dataSource)
		if b.err != nil {
			return nil
		}
	}

	head := v.Children()[0]
	shuffle.workers = make([]*shuffleWorker, shuffle.concurrency)
	for i := range shuffle.workers {
		receivers := make([]*shuffleReceiver, len(v.DataSources))
		for j, dataSource := range v.DataSources {
			receivers[j] = &shuffleReceiver{
				baseExecutor: newBaseExecutor(b.ctx, dataSource.Schema(), dataSource.ID()),
			}
		}

		w := &shuffleWorker{
			receivers: receivers,
		}

		for j, dataSource := range v.DataSources {
			stub := plannercore.PhysicalShuffleReceiverStub{
				Receiver:   (unsafe.Pointer)(receivers[j]),
				DataSource: dataSource,
			}.Init(b.ctx, dataSource.Stats(), dataSource.SelectBlockOffset(), nil)
			stub.SetSchema(dataSource.Schema())
			v.Tails[j].SetChildren(stub)
		}

		w.childExec = b.build(head)
		if b.err != nil {
			return nil
		}

		shuffle.workers[i] = w
	}

	return shuffle
}

func (b *executorBuilder) buildShuffleReceiverStub(v *plannercore.PhysicalShuffleReceiverStub) *shuffleReceiver {
	return (*shuffleReceiver)(v.Receiver)
}

func (b *executorBuilder) buildSQLBindExec(v *plannercore.SQLBindPlan) Executor {
	base := newBaseExecutor(b.ctx, v.Schema(), v.ID())
	base.initCap = chunk.ZeroCapacity

	e := &SQLBindExec{
		baseExecutor: base,
		sqlBindOp:    v.SQLBindOp,
		normdOrigSQL: v.NormdOrigSQL,
		bindSQL:      v.BindSQL,
		charset:      v.Charset,
		collation:    v.Collation,
		db:           v.Db,
		isGlobal:     v.IsGlobal,
		bindAst:      v.BindStmt,
		newStatus:    v.NewStatus,
	}
	return e
}

// NewRowDecoder creates a chunk decoder for new row format row value decode.
func NewRowDecoder(ctx sessionctx.Context, schema *expression.Schema, tbl *model.TableInfo) *rowcodec.ChunkDecoder {
	getColInfoByID := func(tbl *model.TableInfo, colID int64) *model.ColumnInfo {
		for _, col := range tbl.Columns {
			if col.ID == colID {
				return col
			}
		}
		return nil
	}
	var pkCols []int64
	reqCols := make([]rowcodec.ColInfo, len(schema.Columns))
	for i := range schema.Columns {
		idx, col := i, schema.Columns[i]
		isPK := (tbl.PKIsHandle && mysql.HasPriKeyFlag(col.RetType.GetFlag())) || col.ID == model.ExtraHandleID
		if isPK {
			pkCols = append(pkCols, col.ID)
		}
		isGeneratedCol := false
		if col.VirtualExpr != nil {
			isGeneratedCol = true
		}
		reqCols[idx] = rowcodec.ColInfo{
			ID:            col.ID,
			VirtualGenCol: isGeneratedCol,
			Ft:            col.RetType,
		}
	}
	if len(pkCols) == 0 {
		pkCols = tables.TryGetCommonPkColumnIds(tbl)
		if len(pkCols) == 0 {
			pkCols = []int64{-1}
		}
	}
	defVal := func(i int, chk *chunk.Chunk) error {
		ci := getColInfoByID(tbl, reqCols[i].ID)
		d, err := table.GetColOriginDefaultValue(ctx, ci)
		if err != nil {
			return err
		}
		chk.AppendDatum(i, &d)
		return nil
	}
	return rowcodec.NewChunkDecoder(reqCols, pkCols, defVal, ctx.GetSessionVars().Location())
}

func (b *executorBuilder) buildBatchPointGet(plan *plannercore.BatchPointGetPlan) Executor {
	if err := b.validCanReadTemporaryOrCacheTable(plan.TblInfo); err != nil {
		b.err = err
		return nil
	}

	startTS, err := b.getSnapshotTS()
	if err != nil {
		b.err = err
		return nil
	}
	decoder := NewRowDecoder(b.ctx, plan.Schema(), plan.TblInfo)
	e := &BatchPointGetExec{
		baseExecutor:     newBaseExecutor(b.ctx, plan.Schema(), plan.ID()),
		tblInfo:          plan.TblInfo,
		idxInfo:          plan.IndexInfo,
		rowDecoder:       decoder,
		startTS:          startTS,
		readReplicaScope: b.readReplicaScope,
		isStaleness:      b.isStaleness,
		keepOrder:        plan.KeepOrder,
		desc:             plan.Desc,
		lock:             plan.Lock,
		waitTime:         plan.LockWaitTime,
		partExpr:         plan.PartitionExpr,
		partPos:          plan.PartitionColPos,
		singlePart:       plan.SinglePart,
		partTblID:        plan.PartTblID,
		columns:          plan.Columns,
	}
	if plan.TblInfo.TableCacheStatusType == model.TableCacheStatusEnable {
		e.cacheTable = b.getCacheTable(plan.TblInfo, startTS)
	}
	if plan.TblInfo.TempTableType != model.TempTableNone {
		// Temporary table should not do any lock operations
		e.lock = false
		e.waitTime = 0
	}

	if e.lock {
		b.hasLock = true
	}
	var capacity int
	if plan.IndexInfo != nil && !isCommonHandleRead(plan.TblInfo, plan.IndexInfo) {
		e.idxVals = plan.IndexValues
		capacity = len(e.idxVals)
	} else {
		// `SELECT a FROM t WHERE a IN (1, 1, 2, 1, 2)` should not return duplicated rows
		handles := make([]kv.Handle, 0, len(plan.Handles))
		dedup := kv.NewHandleMap()
		if plan.IndexInfo == nil {
			for _, handle := range plan.Handles {
				if _, found := dedup.Get(handle); found {
					continue
				}
				dedup.Set(handle, true)
				handles = append(handles, handle)
			}
		} else {
			for _, value := range plan.IndexValues {
				if datumsContainNull(value) {
					continue
				}
				handleBytes, err := EncodeUniqueIndexValuesForKey(e.ctx, e.tblInfo, plan.IndexInfo, value)
				if err != nil {
					if kv.ErrNotExist.Equal(err) {
						continue
					}
					b.err = err
					return nil
				}
				handle, err := kv.NewCommonHandle(handleBytes)
				if err != nil {
					b.err = err
					return nil
				}
				if _, found := dedup.Get(handle); found {
					continue
				}
				dedup.Set(handle, true)
				handles = append(handles, handle)
			}
		}
		e.handles = handles
		capacity = len(e.handles)
	}
	e.base().initCap = capacity
	e.base().maxChunkSize = capacity
	e.buildVirtualColumnInfo()
	return e
}

func isCommonHandleRead(tbl *model.TableInfo, idx *model.IndexInfo) bool {
	return tbl.IsCommonHandle && idx.Primary
}

func getPhysicalTableID(t table.Table) int64 {
	if p, ok := t.(table.PhysicalTable); ok {
		return p.GetPhysicalID()
	}
	return t.Meta().ID
}

func getFeedbackStatsTableID(ctx sessionctx.Context, t table.Table) int64 {
	if p, ok := t.(table.PhysicalTable); ok && !ctx.GetSessionVars().UseDynamicPartitionPrune() {
		return p.GetPhysicalID()
	}
	return t.Meta().ID
}

func (b *executorBuilder) buildAdminShowTelemetry(v *plannercore.AdminShowTelemetry) Executor {
	return &AdminShowTelemetryExec{baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID())}
}

func (b *executorBuilder) buildAdminResetTelemetryID(v *plannercore.AdminResetTelemetryID) Executor {
	return &AdminResetTelemetryIDExec{baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID())}
}

func (builder *dataReaderBuilder) partitionPruning(tbl table.PartitionedTable, conds []expression.Expression, partitionNames []model.CIStr,
	columns []*expression.Column, columnNames types.NameSlice) ([]table.PhysicalTable, error) {
	builder.once.Do(func() {
		condPruneResult, err := partitionPruning(builder.executorBuilder.ctx, tbl, conds, partitionNames, columns, columnNames)
		builder.once.condPruneResult = condPruneResult
		builder.once.err = err
	})
	return builder.once.condPruneResult, builder.once.err
}

func partitionPruning(ctx sessionctx.Context, tbl table.PartitionedTable, conds []expression.Expression, partitionNames []model.CIStr,
	columns []*expression.Column, columnNames types.NameSlice) ([]table.PhysicalTable, error) {
	idxArr, err := plannercore.PartitionPruning(ctx, tbl, conds, partitionNames, columns, columnNames)
	if err != nil {
		return nil, err
	}

	pi := tbl.Meta().GetPartitionInfo()
	var ret []table.PhysicalTable
	if fullRangePartition(idxArr) {
		ret = make([]table.PhysicalTable, 0, len(pi.Definitions))
		for _, def := range pi.Definitions {
			p := tbl.GetPartition(def.ID)
			ret = append(ret, p)
		}
	} else {
		ret = make([]table.PhysicalTable, 0, len(idxArr))
		for _, idx := range idxArr {
			pid := pi.Definitions[idx].ID
			p := tbl.GetPartition(pid)
			ret = append(ret, p)
		}
	}
	return ret, nil
}

func fullRangePartition(idxArr []int) bool {
	return len(idxArr) == 1 && idxArr[0] == plannercore.FullRange
}

type emptySampler struct{}

func (s *emptySampler) writeChunk(_ *chunk.Chunk) error {
	return nil
}

func (s *emptySampler) finished() bool {
	return true
}

func (b *executorBuilder) buildTableSample(v *plannercore.PhysicalTableSample) *TableSampleExecutor {
	startTS, err := b.getSnapshotTS()
	if err != nil {
		b.err = err
		return nil
	}
	e := &TableSampleExecutor{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
		table:        v.TableInfo,
		startTS:      startTS,
	}

	tblInfo := v.TableInfo.Meta()
	if tblInfo.TempTableType != model.TempTableNone {
		if tblInfo.TempTableType == model.TempTableGlobal {
			e.sampler = &emptySampler{}
		} else {
			b.err = errors.New("TABLESAMPLE clause can not be applied to local temporary tables")
			return nil
		}
	} else if v.TableSampleInfo.AstNode.SampleMethod == ast.SampleMethodTypeTiDBRegion {
		e.sampler = newTableRegionSampler(
			b.ctx, v.TableInfo, startTS, v.TableSampleInfo.Partitions, v.Schema(),
			v.TableSampleInfo.FullSchema, e.retFieldTypes, v.Desc)
	}

	return e
}

func (b *executorBuilder) buildCTE(v *plannercore.PhysicalCTE) Executor {
	// 1. Build seedPlan.
	if b.Ti != nil {
		b.Ti.UseNonRecursive = true
	}
	seedExec := b.build(v.SeedPlan)
	if b.err != nil {
		return nil
	}

	// 2. Build tables to store intermediate results.
	chkSize := b.ctx.GetSessionVars().MaxChunkSize
	tps := seedExec.base().retFieldTypes
	// iterOutTbl will be constructed in CTEExec.Open().
	var resTbl cteutil.Storage
	var iterInTbl cteutil.Storage

	storageMap, ok := b.ctx.GetSessionVars().StmtCtx.CTEStorageMap.(map[int]*CTEStorages)
	if !ok {
		b.err = errors.New("type assertion for CTEStorageMap failed")
		return nil
	}
	storages, ok := storageMap[v.CTE.IDForStorage]
	if ok {
		// Storage already setup.
		resTbl = storages.ResTbl
		iterInTbl = storages.IterInTbl
	} else {
		resTbl = cteutil.NewStorageRowContainer(tps, chkSize)
		if err := resTbl.OpenAndRef(); err != nil {
			b.err = err
			return nil
		}
		iterInTbl = cteutil.NewStorageRowContainer(tps, chkSize)
		if err := iterInTbl.OpenAndRef(); err != nil {
			b.err = err
			return nil
		}
		storageMap[v.CTE.IDForStorage] = &CTEStorages{ResTbl: resTbl, IterInTbl: iterInTbl}
	}

	// 3. Build recursive part.
	if v.RecurPlan != nil && b.Ti != nil {
		b.Ti.UseRecursive = true
	}
	recursiveExec := b.build(v.RecurPlan)
	if b.err != nil {
		return nil
	}

	var sel []int
	if v.CTE.IsDistinct {
		sel = make([]int, chkSize)
		for i := 0; i < chkSize; i++ {
			sel[i] = i
		}
	}

	return &CTEExec{
		baseExecutor:  newBaseExecutor(b.ctx, v.Schema(), v.ID()),
		seedExec:      seedExec,
		recursiveExec: recursiveExec,
		resTbl:        resTbl,
		iterInTbl:     iterInTbl,
		chkIdx:        0,
		isDistinct:    v.CTE.IsDistinct,
		sel:           sel,
		hasLimit:      v.CTE.HasLimit,
		limitBeg:      v.CTE.LimitBeg,
		limitEnd:      v.CTE.LimitEnd,
		isInApply:     v.CTE.IsInApply,
	}
}

func (b *executorBuilder) buildCTETableReader(v *plannercore.PhysicalCTETable) Executor {
	storageMap, ok := b.ctx.GetSessionVars().StmtCtx.CTEStorageMap.(map[int]*CTEStorages)
	if !ok {
		b.err = errors.New("type assertion for CTEStorageMap failed")
		return nil
	}
	storages, ok := storageMap[v.IDForStorage]
	if !ok {
		b.err = errors.Errorf("iterInTbl should already be set up by CTEExec(id: %d)", v.IDForStorage)
		return nil
	}
	return &CTETableReaderExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
		iterInTbl:    storages.IterInTbl,
		chkIdx:       0,
	}
}
func (b *executorBuilder) validCanReadTemporaryOrCacheTable(tbl *model.TableInfo) error {
	err := b.validCanReadTemporaryTable(tbl)
	if err != nil {
		return err
	}
	return b.validCanReadCacheTable(tbl)
}

func (b *executorBuilder) validCanReadCacheTable(tbl *model.TableInfo) error {
	if tbl.TableCacheStatusType == model.TableCacheStatusDisable {
		return nil
	}

	sessionVars := b.ctx.GetSessionVars()

	// Temporary table can't switch into cache table. so the following code will not cause confusion
	if sessionVars.TxnCtx.IsStaleness || b.isStaleness {
		return errors.Trace(errors.New("can not stale read cache table"))
	}

	return nil
}

func (b *executorBuilder) validCanReadTemporaryTable(tbl *model.TableInfo) error {
	if tbl.TempTableType == model.TempTableNone {
		return nil
	}

	// Some tools like dumpling use history read to dump all table's records and will be fail if we return an error.
	// So we do not check SnapshotTS here

	sessionVars := b.ctx.GetSessionVars()

	if tbl.TempTableType == model.TempTableLocal && sessionVars.SnapshotTS != 0 {
		return errors.New("can not read local temporary table when 'tidb_snapshot' is set")
	}

	if sessionVars.TxnCtx.IsStaleness || b.isStaleness {
		return errors.New("can not stale read temporary table")
	}

	return nil
}

func (b *executorBuilder) getCacheTable(tblInfo *model.TableInfo, startTS uint64) kv.MemBuffer {
	tbl, ok := b.is.TableByID(tblInfo.ID)
	if !ok {
		b.err = errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(b.ctx.GetSessionVars().CurrentDB, tblInfo.Name))
		return nil
	}
	sessVars := b.ctx.GetSessionVars()
	leaseDuration := time.Duration(variable.TableCacheLease.Load()) * time.Second
	cacheData, loading := tbl.(table.CachedTable).TryReadFromCache(startTS, leaseDuration)
	if cacheData != nil {
		sessVars.StmtCtx.ReadFromTableCache = true
		return cacheData
	} else if loading {
		// continue
	} else {
		if !b.ctx.GetSessionVars().StmtCtx.InExplainStmt && !b.inDeleteStmt && !b.inUpdateStmt {
			tbl.(table.CachedTable).UpdateLockForRead(context.Background(), b.ctx.GetStore(), startTS, leaseDuration)
		}
	}
	return nil
}

func (b *executorBuilder) buildCompactTable(v *plannercore.CompactTable) Executor {
	if v.ReplicaKind != ast.CompactReplicaKindTiFlash {
		b.err = errors.Errorf("compact %v replica is not supported", strings.ToLower(string(v.ReplicaKind)))
		return nil
	}

	store := b.ctx.GetStore()
	tikvStore, ok := store.(tikv.Storage)
	if !ok {
		b.err = errors.New("compact tiflash replica can only run with tikv compatible storage")
		return nil
	}

	return &CompactTableTiFlashExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
		tableInfo:    v.TableInfo,
		tikvStore:    tikvStore,
	}
}
