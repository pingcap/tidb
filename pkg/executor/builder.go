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
	"cmp"
	"context"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/diagnosticspb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/ddl/placement"
	"github.com/pingcap/tidb/pkg/distsql"
	distsqlctx "github.com/pingcap/tidb/pkg/distsql/context"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor/aggfuncs"
	"github.com/pingcap/tidb/pkg/executor/aggregate"
	"github.com/pingcap/tidb/pkg/executor/internal/builder"
	"github.com/pingcap/tidb/pkg/executor/internal/calibrateresource"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/executor/internal/pdhelper"
	"github.com/pingcap/tidb/pkg/executor/internal/querywatch"
	"github.com/pingcap/tidb/pkg/executor/internal/testutil"
	"github.com/pingcap/tidb/pkg/executor/internal/vecgroupchecker"
	"github.com/pingcap/tidb/pkg/executor/join"
	"github.com/pingcap/tidb/pkg/executor/lockstats"
	executor_metrics "github.com/pingcap/tidb/pkg/executor/metrics"
	"github.com/pingcap/tidb/pkg/executor/sortexec"
	"github.com/pingcap/tidb/pkg/executor/unionexec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/coreusage"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/sessiontxn/staleread"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/table/temptable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/cteutil"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/ranger"
	rangerctx "github.com/pingcap/tidb/pkg/util/ranger/context"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"github.com/pingcap/tidb/pkg/util/tiflash"
	"github.com/pingcap/tidb/pkg/util/timeutil"
	"github.com/pingcap/tipb/go-tipb"
	clientkv "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv"
	"github.com/tikv/client-go/v2/txnkv/txnsnapshot"
)

// executorBuilder builds an Executor from a Plan.
// The InfoSchema must not change during execution.
type executorBuilder struct {
	ctx     sessionctx.Context
	is      infoschema.InfoSchema
	err     error // err is set when there is error happened during Executor building process.
	hasLock bool
	// isStaleness means whether this statement use stale read.
	isStaleness      bool
	txnScope         string
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

	// Used when building MPPGather.
	encounterUnionScan bool
}

// CTEStorages stores resTbl and iterInTbl for CTEExec.
// There will be a map[CTEStorageID]*CTEStorages in StmtCtx,
// which will store all CTEStorages to make all shared CTEs use same the CTEStorages.
type CTEStorages struct {
	ResTbl    cteutil.Storage
	IterInTbl cteutil.Storage
	Producer  *cteProducer
}

func newExecutorBuilder(ctx sessionctx.Context, is infoschema.InfoSchema) *executorBuilder {
	txnManager := sessiontxn.GetTxnManager(ctx)
	return &executorBuilder{
		ctx:              ctx,
		is:               is,
		isStaleness:      staleread.IsStmtStaleness(ctx),
		txnScope:         txnManager.GetTxnScope(),
		readReplicaScope: txnManager.GetReadReplicaScope(),
	}
}

// MockExecutorBuilder is a wrapper for executorBuilder.
// ONLY used in test.
type MockExecutorBuilder struct {
	*executorBuilder
}

// NewMockExecutorBuilderForTest is ONLY used in test.
func NewMockExecutorBuilderForTest(ctx sessionctx.Context, is infoschema.InfoSchema) *MockExecutorBuilder {
	return &MockExecutorBuilder{
		executorBuilder: newExecutorBuilder(ctx, is)}
}

// Build builds an executor tree according to `p`.
func (b *MockExecutorBuilder) Build(p base.Plan) exec.Executor {
	return b.build(p)
}

func (b *executorBuilder) build(p base.Plan) exec.Executor {
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
	case *plannercore.ImportInto:
		return b.buildImportInto(v)
	case *plannercore.LoadData:
		return b.buildLoadData(v)
	case *plannercore.LoadStats:
		return b.buildLoadStats(v)
	case *plannercore.LockStats:
		return b.buildLockStats(v)
	case *plannercore.UnlockStats:
		return b.buildUnlockStats(v)
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
	case *plannercore.PauseDDLJobs:
		return b.buildPauseDDLJobs(v)
	case *plannercore.ResumeDDLJobs:
		return b.buildResumeDDLJobs(v)
	case *plannercore.ShowNextRowID:
		return b.buildShowNextRowID(v)
	case *plannercore.ShowDDL:
		return b.buildShowDDL(v)
	case *plannercore.PhysicalShowDDLJobs:
		return b.buildShowDDLJobs(v)
	case *plannercore.ShowDDLJobQueries:
		return b.buildShowDDLJobQueries(v)
	case *plannercore.ShowDDLJobQueriesWithRange:
		return b.buildShowDDLJobQueriesWithRange(v)
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
	case *plannercore.PhysicalCTE:
		return b.buildCTE(v)
	case *plannercore.PhysicalCTETable:
		return b.buildCTETableReader(v)
	case *plannercore.CompactTable:
		return b.buildCompactTable(v)
	case *plannercore.AdminShowBDRRole:
		return b.buildAdminShowBDRRole(v)
	case *plannercore.PhysicalExpand:
		return b.buildExpand(v)
	case *plannercore.RecommendIndexPlan:
		return b.buildRecommendIndex(v)
	default:
		if mp, ok := p.(testutil.MockPhysicalPlan); ok {
			return mp.GetExecutor()
		}

		b.err = exeerrors.ErrUnknownPlan.GenWithStack("Unknown Plan %T", p)
		return nil
	}
}

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

func (b *executorBuilder) buildChange(v *plannercore.Change) exec.Executor {
	return &ChangeExec{
		BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
		ChangeStmt:   v.ChangeStmt,
	}
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

func (b *executorBuilder) buildShowDDLJobs(v *plannercore.PhysicalShowDDLJobs) exec.Executor {
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
func buildIndexLookUpChecker(b *executorBuilder, p *plannercore.PhysicalIndexLookUpReader,
	e *IndexLookUpExecutor) {
	is := p.IndexPlans[0].(*plannercore.PhysicalIndexScan)
	fullColLen := len(is.Index.Columns) + len(p.CommonHandleCols)
	if !e.isCommonHandle() {
		fullColLen++
	}
	if e.index.Global {
		fullColLen++
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
	noMVIndexOrPrefixIndexOrVectorIndex := true
	for _, idx := range v.IndexInfos {
		if idx.MVIndex || idx.VectorInfo != nil {
			noMVIndexOrPrefixIndexOrVectorIndex = false
			break
		}
		for _, col := range idx.Columns {
			if col.Length != types.UnspecifiedLength {
				noMVIndexOrPrefixIndexOrVectorIndex = false
				break
			}
		}
		if !noMVIndexOrPrefixIndexOrVectorIndex {
			break
		}
	}
	if b.ctx.GetSessionVars().FastCheckTable && noMVIndexOrPrefixIndexOrVectorIndex {
		e := &FastCheckTableExec{
			BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
			dbName:       v.DBName,
			table:        v.Table,
			indexInfos:   v.IndexInfos,
			is:           b.is,
			err:          &atomic.Pointer[error]{},
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
		is:           b.is,
		srcs:         readerExecs,
		exitCh:       make(chan struct{}),
		retCh:        make(chan error, len(readerExecs)),
		checkIndex:   v.CheckIndex,
	}
	return e
}

func buildIdxColsConcatHandleCols(tblInfo *model.TableInfo, indexInfo *model.IndexInfo, hasGenedCol bool) []*model.ColumnInfo {
	var pkCols []*model.IndexColumn
	if tblInfo.IsCommonHandle {
		pkIdx := tables.FindPrimaryIndex(tblInfo)
		pkCols = pkIdx.Columns
	}

	columns := make([]*model.ColumnInfo, 0, len(indexInfo.Columns)+len(pkCols))
	if hasGenedCol {
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
	var hasGenedCol bool
	for _, iCol := range index.Meta().Columns {
		if tblInfo.Columns[iCol.Offset].IsGenerated() {
			hasGenedCol = true
		}
	}
	cols := buildIdxColsConcatHandleCols(tblInfo, index.Meta(), hasGenedCol)
	e := &RecoverIndexExec{
		BaseExecutor:     exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
		columns:          cols,
		containsGenedCol: hasGenedCol,
		index:            index,
		table:            t,
		physicalID:       t.Meta().ID,
	}
	sessCtx := e.Ctx().GetSessionVars().StmtCtx
	e.handleCols = buildHandleColsForExec(sessCtx, tblInfo, e.columns)
	return e
}

func buildHandleColsForExec(sctx *stmtctx.StatementContext, tblInfo *model.TableInfo,
	allColInfo []*model.ColumnInfo) plannerutil.HandleCols {
	if !tblInfo.IsCommonHandle {
		extraColPos := len(allColInfo) - 1
		intCol := &expression.Column{
			Index:   extraColPos,
			RetType: types.NewFieldType(mysql.TypeLonglong),
		}
		return plannerutil.NewIntHandleCols(intCol)
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
	for _, c := range pkIdx.Columns {
		for j, colInfo := range allColInfo {
			if colInfo.Name.L == c.Name.L {
				tblCols[c.Offset].Index = j
			}
		}
	}
	return plannerutil.NewCommonHandleCols(sctx, tblInfo, pkIdx, tblCols)
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
	if index.Meta().VectorInfo != nil {
		b.err = errors.Errorf("vector index `%v` is not supported for cleanup index", v.IndexName)
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
	sessCtx := e.Ctx().GetSessionVars().StmtCtx
	e.handleCols = buildHandleColsForExec(sessCtx, tblInfo, e.columns)
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

func (b *executorBuilder) buildSelectLock(v *plannercore.PhysicalLock) exec.Executor {
	if !b.inSelectLockStmt {
		b.inSelectLockStmt = true
		defer func() { b.inSelectLockStmt = false }()
	}
	if b.err = b.updateForUpdateTS(); b.err != nil {
		return nil
	}

	src := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	if !b.ctx.GetSessionVars().PessimisticLockEligible() {
		// Locking of rows for update using SELECT FOR UPDATE only applies when autocommit
		// is disabled (either by beginning transaction with START TRANSACTION or by setting
		// autocommit to 0. If autocommit is enabled, the rows matching the specification are not locked.
		// See https://dev.mysql.com/doc/refman/5.7/en/innodb-locking-reads.html
		return src
	}
	// If the `PhysicalLock` is not ignored by the above logic, set the `hasLock` flag.
	b.hasLock = true
	e := &SelectLockExec{
		BaseExecutor:       exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID(), src),
		Lock:               v.Lock,
		tblID2Handle:       v.TblID2Handle,
		tblID2PhysTblIDCol: v.TblID2PhysTblIDCol,
	}

	// filter out temporary tables because they do not store any record in tikv and should not write any lock
	is := e.Ctx().GetInfoSchema().(infoschema.InfoSchema)
	for tblID := range e.tblID2Handle {
		tblInfo, ok := is.TableByID(context.Background(), tblID)
		if !ok {
			b.err = errors.Errorf("Can not get table %d", tblID)
		}

		if tblInfo.Meta().TempTableType != model.TempTableNone {
			delete(e.tblID2Handle, tblID)
		}
	}

	return e
}

func (b *executorBuilder) buildLimit(v *plannercore.PhysicalLimit) exec.Executor {
	childExec := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	n := int(min(v.Count, uint64(b.ctx.GetSessionVars().MaxChunkSize)))
	base := exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID(), childExec)
	base.SetInitCap(n)
	e := &LimitExec{
		BaseExecutor: base,
		begin:        v.Offset,
		end:          v.Offset + v.Count,
	}

	childUsedSchemaLen := v.Children()[0].Schema().Len()
	childUsedSchema := markChildrenUsedCols(v.Schema().Columns, v.Children()[0].Schema())[0]
	e.columnIdxsUsedByChild = make([]int, 0, len(childUsedSchema))
	e.columnIdxsUsedByChild = append(e.columnIdxsUsedByChild, childUsedSchema...)
	if len(e.columnIdxsUsedByChild) == childUsedSchemaLen {
		e.columnIdxsUsedByChild = nil // indicates that all columns are used. LimitExec will improve performance for this condition.
	}
	return e
}

func (b *executorBuilder) buildPrepare(v *plannercore.Prepare) exec.Executor {
	base := exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID())
	base.SetInitCap(chunk.ZeroCapacity)
	return &PrepareExec{
		BaseExecutor: base,
		name:         v.Name,
		sqlText:      v.SQLText,
	}
}

func (b *executorBuilder) buildExecute(v *plannercore.Execute) exec.Executor {
	e := &ExecuteExec{
		BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
		is:           b.is,
		name:         v.Name,
		usingVars:    v.Params,
		stmt:         v.Stmt,
		plan:         v.Plan,
		outputNames:  v.OutputNames(),
	}

	failpoint.Inject("assertExecutePrepareStatementStalenessOption", func(val failpoint.Value) {
		vs := strings.Split(val.(string), "_")
		assertTS, assertReadReplicaScope := vs[0], vs[1]
		staleread.AssertStmtStaleness(b.ctx, true)
		ts, err := sessiontxn.GetTxnManager(b.ctx).GetStmtReadTS()
		if err != nil {
			panic(e)
		}

		if strconv.FormatUint(ts, 10) != assertTS ||
			assertReadReplicaScope != b.readReplicaScope {
			panic("execute prepare statement have wrong staleness option")
		}
	})

	return e
}

func (b *executorBuilder) buildShow(v *plannercore.PhysicalShow) exec.Executor {
	e := &ShowExec{
		BaseExecutor:          exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
		Tp:                    v.Tp,
		CountWarningsOrErrors: v.CountWarningsOrErrors,
		DBName:                pmodel.NewCIStr(v.DBName),
		Table:                 v.Table,
		Partition:             v.Partition,
		Column:                v.Column,
		IndexName:             v.IndexName,
		ResourceGroupName:     pmodel.NewCIStr(v.ResourceGroupName),
		Flag:                  v.Flag,
		Roles:                 v.Roles,
		User:                  v.User,
		is:                    b.is,
		Full:                  v.Full,
		IfNotExists:           v.IfNotExists,
		GlobalScope:           v.GlobalScope,
		Extended:              v.Extended,
		Extractor:             v.Extractor,
		ImportJobID:           v.ImportJobID,
	}
	if e.Tp == ast.ShowMasterStatus || e.Tp == ast.ShowBinlogStatus {
		// show master status need start ts.
		if _, err := e.Ctx().Txn(true); err != nil {
			b.err = err
		}
	}
	return e
}

func (b *executorBuilder) buildSimple(v *plannercore.Simple) exec.Executor {
	switch s := v.Statement.(type) {
	case *ast.GrantStmt:
		return b.buildGrant(s)
	case *ast.RevokeStmt:
		return b.buildRevoke(s)
	case *ast.BRIEStmt:
		return b.buildBRIE(s, v.Schema())
	case *ast.CalibrateResourceStmt:
		return &calibrateresource.Executor{
			BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), 0),
			WorkloadType: s.Tp,
			OptionList:   s.DynamicCalibrateResourceOptionList,
		}
	case *ast.AddQueryWatchStmt:
		return &querywatch.AddExecutor{
			BaseExecutor:         exec.NewBaseExecutor(b.ctx, v.Schema(), 0),
			QueryWatchOptionList: s.QueryWatchOptionList,
		}
	case *ast.ImportIntoActionStmt:
		return &ImportIntoActionExec{
			BaseExecutor: exec.NewBaseExecutor(b.ctx, nil, 0),
			tp:           s.Tp,
			jobID:        s.JobID,
		}
	}
	base := exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID())
	base.SetInitCap(chunk.ZeroCapacity)
	e := &SimpleExec{
		BaseExecutor:    base,
		Statement:       v.Statement,
		ResolveCtx:      v.ResolveCtx,
		IsFromRemote:    v.IsFromRemote,
		is:              b.is,
		staleTxnStartTS: v.StaleTxnStartTS,
	}
	return e
}

func (b *executorBuilder) buildSet(v *plannercore.Set) exec.Executor {
	base := exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID())
	base.SetInitCap(chunk.ZeroCapacity)
	e := &SetExecutor{
		BaseExecutor: base,
		vars:         v.VarAssigns,
	}
	return e
}

func (b *executorBuilder) buildSetConfig(v *plannercore.SetConfig) exec.Executor {
	return &SetConfigExec{
		BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
		p:            v,
	}
}

func (b *executorBuilder) buildInsert(v *plannercore.Insert) exec.Executor {
	b.inInsertStmt = true
	if b.err = b.updateForUpdateTS(); b.err != nil {
		return nil
	}

	selectExec := b.build(v.SelectPlan)
	if b.err != nil {
		return nil
	}
	var baseExec exec.BaseExecutor
	if selectExec != nil {
		baseExec = exec.NewBaseExecutor(b.ctx, nil, v.ID(), selectExec)
	} else {
		baseExec = exec.NewBaseExecutor(b.ctx, nil, v.ID())
	}
	baseExec.SetInitCap(chunk.ZeroCapacity)

	ivs := &InsertValues{
		BaseExecutor:              baseExec,
		Table:                     v.Table,
		Columns:                   v.Columns,
		Lists:                     v.Lists,
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
	ivs.fkChecks, b.err = buildFKCheckExecs(b.ctx, ivs.Table, v.FKChecks)
	if b.err != nil {
		return nil
	}
	ivs.fkCascades, b.err = b.buildFKCascadeExecs(ivs.Table, v.FKCascades)
	if b.err != nil {
		return nil
	}

	if v.IsReplace {
		return b.buildReplace(ivs)
	}
	insert := &InsertExec{
		InsertValues: ivs,
		OnDuplicate:  append(v.OnDuplicate, v.GenCols.OnDuplicates...),
		IgnoreErr:    v.IgnoreErr,
	}
	return insert
}

func (b *executorBuilder) buildImportInto(v *plannercore.ImportInto) exec.Executor {
	// see planBuilder.buildImportInto for detail why we use the latest schema here.
	latestIS := b.ctx.GetDomainInfoSchema().(infoschema.InfoSchema)
	tbl, ok := latestIS.TableByID(context.Background(), v.Table.TableInfo.ID)
	if !ok {
		b.err = errors.Errorf("Can not get table %d", v.Table.TableInfo.ID)
		return nil
	}
	if !tbl.Meta().IsBaseTable() {
		b.err = plannererrors.ErrNonUpdatableTable.GenWithStackByArgs(tbl.Meta().Name.O, "IMPORT")
		return nil
	}

	var (
		selectExec exec.Executor
		base       exec.BaseExecutor
	)
	if v.SelectPlan != nil {
		selectExec = b.build(v.SelectPlan)
		if b.err != nil {
			return nil
		}
		base = exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID(), selectExec)
	} else {
		base = exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID())
	}
	executor, err := newImportIntoExec(base, selectExec, b.ctx, v, tbl)
	if err != nil {
		b.err = err
		return nil
	}

	return executor
}

func (b *executorBuilder) buildLoadData(v *plannercore.LoadData) exec.Executor {
	tbl, ok := b.is.TableByID(context.Background(), v.Table.TableInfo.ID)
	if !ok {
		b.err = errors.Errorf("Can not get table %d", v.Table.TableInfo.ID)
		return nil
	}
	if !tbl.Meta().IsBaseTable() {
		b.err = plannererrors.ErrNonUpdatableTable.GenWithStackByArgs(tbl.Meta().Name.O, "LOAD")
		return nil
	}

	base := exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID())
	worker, err := NewLoadDataWorker(b.ctx, v, tbl)
	if err != nil {
		b.err = err
		return nil
	}

	return &LoadDataExec{
		BaseExecutor:   base,
		loadDataWorker: worker,
		FileLocRef:     v.FileLocRef,
	}
}

func (b *executorBuilder) buildLoadStats(v *plannercore.LoadStats) exec.Executor {
	e := &LoadStatsExec{
		BaseExecutor: exec.NewBaseExecutor(b.ctx, nil, v.ID()),
		info:         &LoadStatsInfo{v.Path, b.ctx},
	}
	return e
}

func (b *executorBuilder) buildLockStats(v *plannercore.LockStats) exec.Executor {
	e := &lockstats.LockExec{
		BaseExecutor: exec.NewBaseExecutor(b.ctx, nil, v.ID()),
		Tables:       v.Tables,
	}
	return e
}

func (b *executorBuilder) buildUnlockStats(v *plannercore.UnlockStats) exec.Executor {
	e := &lockstats.UnlockExec{
		BaseExecutor: exec.NewBaseExecutor(b.ctx, nil, v.ID()),
		Tables:       v.Tables,
	}
	return e
}

func (b *executorBuilder) buildPlanReplayer(v *plannercore.PlanReplayer) exec.Executor {
	if v.Load {
		e := &PlanReplayerLoadExec{
			BaseExecutor: exec.NewBaseExecutor(b.ctx, nil, v.ID()),
			info:         &PlanReplayerLoadInfo{Path: v.File, Ctx: b.ctx},
		}
		return e
	}
	if v.Capture {
		e := &PlanReplayerExec{
			BaseExecutor: exec.NewBaseExecutor(b.ctx, nil, v.ID()),
			CaptureInfo: &PlanReplayerCaptureInfo{
				SQLDigest:  v.SQLDigest,
				PlanDigest: v.PlanDigest,
			},
		}
		return e
	}
	if v.Remove {
		e := &PlanReplayerExec{
			BaseExecutor: exec.NewBaseExecutor(b.ctx, nil, v.ID()),
			CaptureInfo: &PlanReplayerCaptureInfo{
				SQLDigest:  v.SQLDigest,
				PlanDigest: v.PlanDigest,
				Remove:     true,
			},
		}
		return e
	}

	e := &PlanReplayerExec{
		BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
		DumpInfo: &PlanReplayerDumpInfo{
			Analyze:           v.Analyze,
			Path:              v.File,
			ctx:               b.ctx,
			HistoricalStatsTS: v.HistoricalStatsTS,
		},
	}
	if v.ExecStmt != nil {
		e.DumpInfo.ExecStmts = []ast.StmtNode{v.ExecStmt}
	} else {
		e.BaseExecutor = exec.NewBaseExecutor(b.ctx, nil, v.ID())
	}
	return e
}

func (*executorBuilder) buildReplace(vals *InsertValues) exec.Executor {
	replaceExec := &ReplaceExec{
		InsertValues: vals,
	}
	return replaceExec
}

func (b *executorBuilder) buildGrant(grant *ast.GrantStmt) exec.Executor {
	e := &GrantExec{
		BaseExecutor:          exec.NewBaseExecutor(b.ctx, nil, 0),
		Privs:                 grant.Privs,
		ObjectType:            grant.ObjectType,
		Level:                 grant.Level,
		Users:                 grant.Users,
		WithGrant:             grant.WithGrant,
		AuthTokenOrTLSOptions: grant.AuthTokenOrTLSOptions,
		is:                    b.is,
	}
	return e
}

func (b *executorBuilder) buildRevoke(revoke *ast.RevokeStmt) exec.Executor {
	e := &RevokeExec{
		BaseExecutor: exec.NewBaseExecutor(b.ctx, nil, 0),
		ctx:          b.ctx,
		Privs:        revoke.Privs,
		ObjectType:   revoke.ObjectType,
		Level:        revoke.Level,
		Users:        revoke.Users,
		is:           b.is,
	}
	return e
}

func (b *executorBuilder) buildDDL(v *plannercore.DDL) exec.Executor {
	e := &DDLExec{
		BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
		ddlExecutor:  domain.GetDomain(b.ctx).DDLExecutor(),
		stmt:         v.Statement,
		is:           b.is,
		tempTableDDL: temptable.GetTemporaryTableDDL(b.ctx),
	}
	return e
}

// buildTrace builds a TraceExec for future executing. This method will be called
// at build().
func (b *executorBuilder) buildTrace(v *plannercore.Trace) exec.Executor {
	t := &TraceExec{
		BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
		stmtNode:     v.StmtNode,
		resolveCtx:   v.ResolveCtx,
		builder:      b,
		format:       v.Format,

		optimizerTrace:       v.OptimizerTrace,
		optimizerTraceTarget: v.OptimizerTraceTarget,
	}
	if t.format == plannercore.TraceFormatLog && !t.optimizerTrace {
		return &sortexec.SortExec{
			BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID(), t),
			ByItems: []*plannerutil.ByItems{
				{Expr: &expression.Column{
					Index:   0,
					RetType: types.NewFieldType(mysql.TypeTimestamp),
				}},
			},
			ExecSchema: v.Schema(),
		}
	}
	return t
}

// buildExplain builds a explain executor. `e.rows` collects final result to `ExplainExec`.
func (b *executorBuilder) buildExplain(v *plannercore.Explain) exec.Executor {
	explainExec := &ExplainExec{
		BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
		explain:      v,
	}
	if v.Analyze {
		if b.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl == nil {
			b.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl = execdetails.NewRuntimeStatsColl(nil)
		}
	}
	// Needs to build the target plan, even if not executing it
	// to get partition pruning.
	explainExec.analyzeExec = b.build(v.TargetPlan)
	return explainExec
}

func (b *executorBuilder) buildSelectInto(v *plannercore.SelectInto) exec.Executor {
	child := b.build(v.TargetPlan)
	if b.err != nil {
		return nil
	}
	return &SelectIntoExec{
		BaseExecutor:   exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID(), child),
		intoOpt:        v.IntoOpt,
		LineFieldsInfo: v.LineFieldsInfo,
	}
}

func (b *executorBuilder) buildUnionScanExec(v *plannercore.PhysicalUnionScan) exec.Executor {
	oriEncounterUnionScan := b.encounterUnionScan
	b.encounterUnionScan = true
	defer func() {
		b.encounterUnionScan = oriEncounterUnionScan
	}()
	reader := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}

	return b.buildUnionScanFromReader(reader, v)
}

// buildUnionScanFromReader builds union scan executor from child executor.
// Note that this function may be called by inner workers of index lookup join concurrently.
// Be careful to avoid data race.
func (b *executorBuilder) buildUnionScanFromReader(reader exec.Executor, v *plannercore.PhysicalUnionScan) exec.Executor {
	// If reader is union, it means a partition table and we should transfer as above.
	if x, ok := reader.(*unionexec.UnionExec); ok {
		for i, child := range x.AllChildren() {
			x.SetChildren(i, b.buildUnionScanFromReader(child, v))
			if b.err != nil {
				return nil
			}
		}
		return x
	}
	us := &UnionScanExec{BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID(), reader)}
	// Get the handle column index of the below Plan.
	us.handleCols = v.HandleCols
	us.mutableRow = chunk.MutRowFromTypes(exec.RetTypes(us))

	// If the push-downed condition contains virtual column, we may build a selection upon reader
	originReader := reader
	if sel, ok := reader.(*SelectionExec); ok {
		reader = sel.Children(0)
	}

	us.collators = make([]collate.Collator, 0, len(us.columns))
	for _, tp := range exec.RetTypes(us) {
		us.collators = append(us.collators, collate.GetCollator(tp.GetCollate()))
	}

	startTS, err := b.getSnapshotTS()
	sessionVars := b.ctx.GetSessionVars()
	if err != nil {
		b.err = err
		return nil
	}

	switch x := reader.(type) {
	case *MPPGather:
		us.desc = false
		us.keepOrder = false
		us.conditions, us.conditionsWithVirCol = plannercore.SplitSelCondsWithVirtualColumn(v.Conditions)
		us.columns = x.columns
		us.table = x.table
		us.virtualColumnIndex = x.virtualColumnIndex
		us.handleCachedTable(b, x, sessionVars, startTS)
	case *TableReaderExecutor:
		us.desc = x.desc
		us.keepOrder = x.keepOrder
		us.conditions, us.conditionsWithVirCol = plannercore.SplitSelCondsWithVirtualColumn(v.Conditions)
		us.columns = x.columns
		us.table = x.table
		us.virtualColumnIndex = x.virtualColumnIndex
		us.handleCachedTable(b, x, sessionVars, startTS)
	case *IndexReaderExecutor:
		us.desc = x.desc
		us.keepOrder = x.keepOrder
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
		us.partitionIDMap = x.partitionIDMap
		us.table = x.table
		us.handleCachedTable(b, x, sessionVars, startTS)
	case *IndexLookUpExecutor:
		us.desc = x.desc
		us.keepOrder = x.keepOrder
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
		us.partitionIDMap = x.partitionIDMap
		us.virtualColumnIndex = buildVirtualColumnIndex(us.Schema(), us.columns)
		us.handleCachedTable(b, x, sessionVars, startTS)
	case *IndexMergeReaderExecutor:
		if len(x.byItems) != 0 {
			us.keepOrder = x.keepOrder
			us.desc = x.byItems[0].Desc
			for _, item := range x.byItems {
				c, ok := item.Expr.(*expression.Column)
				if !ok {
					b.err = errors.Errorf("Not support non-column in orderBy pushed down")
					return nil
				}
				for i, col := range x.columns {
					if col.ID == c.ID {
						us.usedIndex = append(us.usedIndex, i)
						break
					}
				}
			}
		}
		us.partitionIDMap = x.partitionIDMap
		us.conditions, us.conditionsWithVirCol = plannercore.SplitSelCondsWithVirtualColumn(v.Conditions)
		us.columns = x.columns
		us.table = x.table
		us.virtualColumnIndex = buildVirtualColumnIndex(us.Schema(), us.columns)
	case *PointGetExecutor, *BatchPointGetExec, // PointGet and BatchPoint can handle virtual columns and dirty txn data themselves.
		*TableDualExec,       // If TableDual, the result must be empty, so we can skip UnionScan and use TableDual directly here.
		*TableSampleExecutor: // TableSample only supports sampling from disk, don't need to consider in-memory txn data for simplicity.
		return originReader
	default:
		// TODO: consider more operators like Projection.
		b.err = errors.NewNoStackErrorf("unexpected operator %T under UnionScan", reader)
		return nil
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
			return
		} else {
			if !b.inUpdateStmt && !b.inDeleteStmt && !b.inInsertStmt && !vars.StmtCtx.InExplainStmt {
				store := b.ctx.GetStore()
				cachedTable.UpdateLockForRead(context.Background(), store, startTS, leaseDuration)
			}
		}
	}
}

// buildMergeJoin builds MergeJoinExec executor.
func (b *executorBuilder) buildMergeJoin(v *plannercore.PhysicalMergeJoin) exec.Executor {
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
		if v.JoinType == logicalop.RightOuterJoin {
			defaultValues = make([]types.Datum, leftExec.Schema().Len())
		} else {
			defaultValues = make([]types.Datum, rightExec.Schema().Len())
		}
	}

	colsFromChildren := v.Schema().Columns
	if v.JoinType == logicalop.LeftOuterSemiJoin || v.JoinType == logicalop.AntiLeftOuterSemiJoin {
		colsFromChildren = colsFromChildren[:len(colsFromChildren)-1]
	}

	e := &join.MergeJoinExec{
		StmtCtx:      b.ctx.GetSessionVars().StmtCtx,
		BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID(), leftExec, rightExec),
		CompareFuncs: v.CompareFuncs,
		Joiner: join.NewJoiner(
			b.ctx,
			v.JoinType,
			v.JoinType == logicalop.RightOuterJoin,
			defaultValues,
			v.OtherConditions,
			exec.RetTypes(leftExec),
			exec.RetTypes(rightExec),
			markChildrenUsedCols(colsFromChildren, v.Children()[0].Schema(), v.Children()[1].Schema()),
			false,
		),
		IsOuterJoin: v.JoinType.IsOuterJoin(),
		Desc:        v.Desc,
	}

	leftTable := &join.MergeJoinTable{
		ChildIndex: 0,
		JoinKeys:   v.LeftJoinKeys,
		Filters:    v.LeftConditions,
	}
	rightTable := &join.MergeJoinTable{
		ChildIndex: 1,
		JoinKeys:   v.RightJoinKeys,
		Filters:    v.RightConditions,
	}

	if v.JoinType == logicalop.RightOuterJoin {
		e.InnerTable = leftTable
		e.OuterTable = rightTable
	} else {
		e.InnerTable = rightTable
		e.OuterTable = leftTable
	}
	e.InnerTable.IsInner = true

	// optimizer should guarantee that filters on inner table are pushed down
	// to tikv or extracted to a Selection.
	if len(e.InnerTable.Filters) != 0 {
		b.err = errors.Annotate(exeerrors.ErrBuildExecutor, "merge join's inner filter should be empty.")
		return nil
	}

	executor_metrics.ExecutorCounterMergeJoinExec.Inc()
	return e
}

func collectColumnIndexFromExpr(expr expression.Expression, leftColumnSize int, leftColumnIndex []int, rightColumnIndex []int) ([]int, []int) {
	switch x := expr.(type) {
	case *expression.Column:
		colIndex := x.Index
		if colIndex >= leftColumnSize {
			rightColumnIndex = append(rightColumnIndex, colIndex-leftColumnSize)
		} else {
			leftColumnIndex = append(leftColumnIndex, colIndex)
		}
		return leftColumnIndex, rightColumnIndex
	case *expression.Constant:
		return leftColumnIndex, rightColumnIndex
	case *expression.ScalarFunction:
		for _, arg := range x.GetArgs() {
			leftColumnIndex, rightColumnIndex = collectColumnIndexFromExpr(arg, leftColumnSize, leftColumnIndex, rightColumnIndex)
		}
		return leftColumnIndex, rightColumnIndex
	default:
		panic("unsupported expression")
	}
}

func extractUsedColumnsInJoinOtherCondition(expr expression.CNFExprs, leftColumnSize int) ([]int, []int) {
	leftColumnIndex := make([]int, 0, 1)
	rightColumnIndex := make([]int, 0, 1)
	for _, subExpr := range expr {
		leftColumnIndex, rightColumnIndex = collectColumnIndexFromExpr(subExpr, leftColumnSize, leftColumnIndex, rightColumnIndex)
	}
	return leftColumnIndex, rightColumnIndex
}

func (b *executorBuilder) buildHashJoinV2(v *plannercore.PhysicalHashJoin) exec.Executor {
	leftExec := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}

	rightExec := b.build(v.Children()[1])
	if b.err != nil {
		return nil
	}

	e := &join.HashJoinV2Exec{
		BaseExecutor:          exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID(), leftExec, rightExec),
		ProbeSideTupleFetcher: &join.ProbeSideTupleFetcherV2{},
		ProbeWorkers:          make([]*join.ProbeWorkerV2, v.Concurrency),
		BuildWorkers:          make([]*join.BuildWorkerV2, v.Concurrency),
		HashJoinCtxV2: &join.HashJoinCtxV2{
			OtherCondition: v.OtherConditions,
		},
	}
	e.HashJoinCtxV2.SessCtx = b.ctx
	e.HashJoinCtxV2.JoinType = v.JoinType
	e.HashJoinCtxV2.Concurrency = v.Concurrency
	e.HashJoinCtxV2.SetupPartitionInfo()
	e.ChunkAllocPool = e.AllocPool
	e.HashJoinCtxV2.RightAsBuildSide = true
	if v.InnerChildIdx == 1 && v.UseOuterToBuild {
		e.HashJoinCtxV2.RightAsBuildSide = false
	} else if v.InnerChildIdx == 0 && !v.UseOuterToBuild {
		e.HashJoinCtxV2.RightAsBuildSide = false
	}

	lhsTypes, rhsTypes := exec.RetTypes(leftExec), exec.RetTypes(rightExec)
	joinedTypes := make([]*types.FieldType, 0, len(lhsTypes)+len(rhsTypes))
	joinedTypes = append(joinedTypes, lhsTypes...)
	joinedTypes = append(joinedTypes, rhsTypes...)

	if v.InnerChildIdx == 1 {
		if len(v.RightConditions) > 0 {
			b.err = errors.Annotate(exeerrors.ErrBuildExecutor, "join's inner condition should be empty")
			return nil
		}
	} else {
		if len(v.LeftConditions) > 0 {
			b.err = errors.Annotate(exeerrors.ErrBuildExecutor, "join's inner condition should be empty")
			return nil
		}
	}

	var probeKeys, buildKeys []*expression.Column
	var buildSideExec exec.Executor
	if v.UseOuterToBuild {
		if v.InnerChildIdx == 1 {
			buildSideExec, buildKeys = leftExec, v.LeftJoinKeys
			e.ProbeSideTupleFetcher.ProbeSideExec, probeKeys = rightExec, v.RightJoinKeys
			e.HashJoinCtxV2.BuildFilter = v.LeftConditions
		} else {
			buildSideExec, buildKeys = rightExec, v.RightJoinKeys
			e.ProbeSideTupleFetcher.ProbeSideExec, probeKeys = leftExec, v.LeftJoinKeys
			e.HashJoinCtxV2.BuildFilter = v.RightConditions
		}
	} else {
		if v.InnerChildIdx == 0 {
			buildSideExec, buildKeys = leftExec, v.LeftJoinKeys
			e.ProbeSideTupleFetcher.ProbeSideExec, probeKeys = rightExec, v.RightJoinKeys
			e.HashJoinCtxV2.ProbeFilter = v.RightConditions
		} else {
			buildSideExec, buildKeys = rightExec, v.RightJoinKeys
			e.ProbeSideTupleFetcher.ProbeSideExec, probeKeys = leftExec, v.LeftJoinKeys
			e.HashJoinCtxV2.ProbeFilter = v.LeftConditions
		}
	}
	probeKeyColIdx := make([]int, len(probeKeys))
	buildKeyColIdx := make([]int, len(buildKeys))
	for i := range buildKeys {
		buildKeyColIdx[i] = buildKeys[i].Index
	}
	for i := range probeKeys {
		probeKeyColIdx[i] = probeKeys[i].Index
	}

	colsFromChildren := v.Schema().Columns
	if v.JoinType == logicalop.LeftOuterSemiJoin || v.JoinType == logicalop.AntiLeftOuterSemiJoin {
		// the matched column is added inside join
		colsFromChildren = colsFromChildren[:len(colsFromChildren)-1]
	}
	childrenUsedSchema := markChildrenUsedCols(colsFromChildren, v.Children()[0].Schema(), v.Children()[1].Schema())
	if childrenUsedSchema == nil {
		b.err = errors.New("children used should never be nil")
		return nil
	}
	e.LUsed = make([]int, 0, len(childrenUsedSchema[0]))
	e.LUsed = append(e.LUsed, childrenUsedSchema[0]...)
	e.RUsed = make([]int, 0, len(childrenUsedSchema[1]))
	e.RUsed = append(e.RUsed, childrenUsedSchema[1]...)
	if v.OtherConditions != nil {
		leftColumnSize := v.Children()[0].Schema().Len()
		e.LUsedInOtherCondition, e.RUsedInOtherCondition = extractUsedColumnsInJoinOtherCondition(v.OtherConditions, leftColumnSize)
	}
	// todo add partition hash join exec
	executor_metrics.ExecutorCountHashJoinExec.Inc()

	leftExecTypes, rightExecTypes := exec.RetTypes(leftExec), exec.RetTypes(rightExec)
	leftTypes, rightTypes := make([]*types.FieldType, 0, len(v.LeftJoinKeys)+len(v.LeftNAJoinKeys)), make([]*types.FieldType, 0, len(v.RightJoinKeys)+len(v.RightNAJoinKeys))
	for i, col := range v.LeftJoinKeys {
		leftTypes = append(leftTypes, leftExecTypes[col.Index].Clone())
		leftTypes[i].SetFlag(col.RetType.GetFlag())
	}
	offset := len(v.LeftJoinKeys)
	for i, col := range v.LeftNAJoinKeys {
		leftTypes = append(leftTypes, leftExecTypes[col.Index].Clone())
		leftTypes[i+offset].SetFlag(col.RetType.GetFlag())
	}
	for i, col := range v.RightJoinKeys {
		rightTypes = append(rightTypes, rightExecTypes[col.Index].Clone())
		rightTypes[i].SetFlag(col.RetType.GetFlag())
	}
	offset = len(v.RightJoinKeys)
	for i, col := range v.RightNAJoinKeys {
		rightTypes = append(rightTypes, rightExecTypes[col.Index].Clone())
		rightTypes[i+offset].SetFlag(col.RetType.GetFlag())
	}

	// consider collations
	for i := range v.EqualConditions {
		chs, coll := v.EqualConditions[i].CharsetAndCollation()
		leftTypes[i].SetCharset(chs)
		leftTypes[i].SetCollate(coll)
		rightTypes[i].SetCharset(chs)
		rightTypes[i].SetCollate(coll)
	}
	offset = len(v.EqualConditions)
	for i := range v.NAEqualConditions {
		chs, coll := v.NAEqualConditions[i].CharsetAndCollation()
		leftTypes[i+offset].SetCharset(chs)
		leftTypes[i+offset].SetCollate(coll)
		rightTypes[i+offset].SetCharset(chs)
		rightTypes[i+offset].SetCollate(coll)
	}
	if e.RightAsBuildSide {
		e.BuildKeyTypes, e.ProbeKeyTypes = rightTypes, leftTypes
	} else {
		e.BuildKeyTypes, e.ProbeKeyTypes = leftTypes, rightTypes
	}
	for i := uint(0); i < e.Concurrency; i++ {
		e.ProbeWorkers[i] = &join.ProbeWorkerV2{
			HashJoinCtx: e.HashJoinCtxV2,
			JoinProbe:   join.NewJoinProbe(e.HashJoinCtxV2, i, v.JoinType, probeKeyColIdx, joinedTypes, e.ProbeKeyTypes, e.RightAsBuildSide),
		}
		e.ProbeWorkers[i].WorkerID = i

		e.BuildWorkers[i] = join.NewJoinBuildWorkerV2(e.HashJoinCtxV2, i, buildSideExec, buildKeyColIdx, exec.RetTypes(buildSideExec))
	}
	return e
}

func (b *executorBuilder) buildHashJoin(v *plannercore.PhysicalHashJoin) exec.Executor {
	if join.IsHashJoinV2Enabled() && v.CanUseHashJoinV2() {
		return b.buildHashJoinV2(v)
	}
	leftExec := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}

	rightExec := b.build(v.Children()[1])
	if b.err != nil {
		return nil
	}

	e := &join.HashJoinV1Exec{
		BaseExecutor:          exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID(), leftExec, rightExec),
		ProbeSideTupleFetcher: &join.ProbeSideTupleFetcherV1{},
		ProbeWorkers:          make([]*join.ProbeWorkerV1, v.Concurrency),
		BuildWorker:           &join.BuildWorkerV1{},
		HashJoinCtxV1: &join.HashJoinCtxV1{
			IsOuterJoin:     v.JoinType.IsOuterJoin(),
			UseOuterToBuild: v.UseOuterToBuild,
		},
	}
	e.HashJoinCtxV1.SessCtx = b.ctx
	e.HashJoinCtxV1.JoinType = v.JoinType
	e.HashJoinCtxV1.Concurrency = v.Concurrency
	e.HashJoinCtxV1.ChunkAllocPool = e.AllocPool
	defaultValues := v.DefaultValues
	lhsTypes, rhsTypes := exec.RetTypes(leftExec), exec.RetTypes(rightExec)
	if v.InnerChildIdx == 1 {
		if len(v.RightConditions) > 0 {
			b.err = errors.Annotate(exeerrors.ErrBuildExecutor, "join's inner condition should be empty")
			return nil
		}
	} else {
		if len(v.LeftConditions) > 0 {
			b.err = errors.Annotate(exeerrors.ErrBuildExecutor, "join's inner condition should be empty")
			return nil
		}
	}

	leftIsBuildSide := true

	e.IsNullEQ = v.IsNullEQ
	var probeKeys, probeNAKeys, buildKeys, buildNAKeys []*expression.Column
	var buildSideExec exec.Executor
	if v.UseOuterToBuild {
		// update the buildSideEstCount due to changing the build side
		if v.InnerChildIdx == 1 {
			buildSideExec, buildKeys, buildNAKeys = leftExec, v.LeftJoinKeys, v.LeftNAJoinKeys
			e.ProbeSideTupleFetcher.ProbeSideExec, probeKeys, probeNAKeys = rightExec, v.RightJoinKeys, v.RightNAJoinKeys
			e.OuterFilter = v.LeftConditions
		} else {
			buildSideExec, buildKeys, buildNAKeys = rightExec, v.RightJoinKeys, v.RightNAJoinKeys
			e.ProbeSideTupleFetcher.ProbeSideExec, probeKeys, probeNAKeys = leftExec, v.LeftJoinKeys, v.LeftNAJoinKeys
			e.OuterFilter = v.RightConditions
			leftIsBuildSide = false
		}
		if defaultValues == nil {
			defaultValues = make([]types.Datum, e.ProbeSideTupleFetcher.ProbeSideExec.Schema().Len())
		}
	} else {
		if v.InnerChildIdx == 0 {
			buildSideExec, buildKeys, buildNAKeys = leftExec, v.LeftJoinKeys, v.LeftNAJoinKeys
			e.ProbeSideTupleFetcher.ProbeSideExec, probeKeys, probeNAKeys = rightExec, v.RightJoinKeys, v.RightNAJoinKeys
			e.OuterFilter = v.RightConditions
		} else {
			buildSideExec, buildKeys, buildNAKeys = rightExec, v.RightJoinKeys, v.RightNAJoinKeys
			e.ProbeSideTupleFetcher.ProbeSideExec, probeKeys, probeNAKeys = leftExec, v.LeftJoinKeys, v.LeftNAJoinKeys
			e.OuterFilter = v.LeftConditions
			leftIsBuildSide = false
		}
		if defaultValues == nil {
			defaultValues = make([]types.Datum, buildSideExec.Schema().Len())
		}
	}
	probeKeyColIdx := make([]int, len(probeKeys))
	probeNAKeColIdx := make([]int, len(probeNAKeys))
	buildKeyColIdx := make([]int, len(buildKeys))
	buildNAKeyColIdx := make([]int, len(buildNAKeys))
	for i := range buildKeys {
		buildKeyColIdx[i] = buildKeys[i].Index
	}
	for i := range buildNAKeys {
		buildNAKeyColIdx[i] = buildNAKeys[i].Index
	}
	for i := range probeKeys {
		probeKeyColIdx[i] = probeKeys[i].Index
	}
	for i := range probeNAKeys {
		probeNAKeColIdx[i] = probeNAKeys[i].Index
	}
	isNAJoin := len(v.LeftNAJoinKeys) > 0
	colsFromChildren := v.Schema().Columns
	if v.JoinType == logicalop.LeftOuterSemiJoin || v.JoinType == logicalop.AntiLeftOuterSemiJoin {
		colsFromChildren = colsFromChildren[:len(colsFromChildren)-1]
	}
	childrenUsedSchema := markChildrenUsedCols(colsFromChildren, v.Children()[0].Schema(), v.Children()[1].Schema())
	for i := uint(0); i < e.Concurrency; i++ {
		e.ProbeWorkers[i] = &join.ProbeWorkerV1{
			HashJoinCtx:      e.HashJoinCtxV1,
			Joiner:           join.NewJoiner(b.ctx, v.JoinType, v.InnerChildIdx == 0, defaultValues, v.OtherConditions, lhsTypes, rhsTypes, childrenUsedSchema, isNAJoin),
			ProbeKeyColIdx:   probeKeyColIdx,
			ProbeNAKeyColIdx: probeNAKeColIdx,
		}
		e.ProbeWorkers[i].WorkerID = i
	}
	e.BuildWorker.BuildKeyColIdx, e.BuildWorker.BuildNAKeyColIdx, e.BuildWorker.BuildSideExec, e.BuildWorker.HashJoinCtx = buildKeyColIdx, buildNAKeyColIdx, buildSideExec, e.HashJoinCtxV1
	e.HashJoinCtxV1.IsNullAware = isNAJoin
	executor_metrics.ExecutorCountHashJoinExec.Inc()

	// We should use JoinKey to construct the type information using by hashing, instead of using the child's schema directly.
	// When a hybrid type column is hashed multiple times, we need to distinguish what field types are used.
	// For example, the condition `enum = int and enum = string`, we should use ETInt to hash the first column,
	// and use ETString to hash the second column, although they may be the same column.
	leftExecTypes, rightExecTypes := exec.RetTypes(leftExec), exec.RetTypes(rightExec)
	leftTypes, rightTypes := make([]*types.FieldType, 0, len(v.LeftJoinKeys)+len(v.LeftNAJoinKeys)), make([]*types.FieldType, 0, len(v.RightJoinKeys)+len(v.RightNAJoinKeys))
	// set left types and right types for joiner.
	for i, col := range v.LeftJoinKeys {
		leftTypes = append(leftTypes, leftExecTypes[col.Index].Clone())
		leftTypes[i].SetFlag(col.RetType.GetFlag())
	}
	offset := len(v.LeftJoinKeys)
	for i, col := range v.LeftNAJoinKeys {
		leftTypes = append(leftTypes, leftExecTypes[col.Index].Clone())
		leftTypes[i+offset].SetFlag(col.RetType.GetFlag())
	}
	for i, col := range v.RightJoinKeys {
		rightTypes = append(rightTypes, rightExecTypes[col.Index].Clone())
		rightTypes[i].SetFlag(col.RetType.GetFlag())
	}
	offset = len(v.RightJoinKeys)
	for i, col := range v.RightNAJoinKeys {
		rightTypes = append(rightTypes, rightExecTypes[col.Index].Clone())
		rightTypes[i+offset].SetFlag(col.RetType.GetFlag())
	}

	// consider collations
	for i := range v.EqualConditions {
		chs, coll := v.EqualConditions[i].CharsetAndCollation()
		leftTypes[i].SetCharset(chs)
		leftTypes[i].SetCollate(coll)
		rightTypes[i].SetCharset(chs)
		rightTypes[i].SetCollate(coll)
	}
	offset = len(v.EqualConditions)
	for i := range v.NAEqualConditions {
		chs, coll := v.NAEqualConditions[i].CharsetAndCollation()
		leftTypes[i+offset].SetCharset(chs)
		leftTypes[i+offset].SetCollate(coll)
		rightTypes[i+offset].SetCharset(chs)
		rightTypes[i+offset].SetCollate(coll)
	}
	if leftIsBuildSide {
		e.BuildTypes, e.ProbeTypes = leftTypes, rightTypes
	} else {
		e.BuildTypes, e.ProbeTypes = rightTypes, leftTypes
	}
	return e
}

func (b *executorBuilder) buildHashAgg(v *plannercore.PhysicalHashAgg) exec.Executor {
	src := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	return b.buildHashAggFromChildExec(src, v)
}

func (b *executorBuilder) buildHashAggFromChildExec(childExec exec.Executor, v *plannercore.PhysicalHashAgg) *aggregate.HashAggExec {
	sessionVars := b.ctx.GetSessionVars()
	e := &aggregate.HashAggExec{
		BaseExecutor:    exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID(), childExec),
		Sc:              sessionVars.StmtCtx,
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
		e.DefaultVal = nil
	} else {
		if v.IsFinalAgg() {
			e.DefaultVal = e.AllocPool.Alloc(exec.RetTypes(e), 1, 1)
		}
	}
	for _, aggDesc := range v.AggFuncs {
		if aggDesc.HasDistinct || len(aggDesc.OrderByItems) > 0 {
			e.IsUnparallelExec = true
		}
	}
	// When we set both tidb_hashagg_final_concurrency and tidb_hashagg_partial_concurrency to 1,
	// we do not need to parallelly execute hash agg,
	// and this action can be a workaround when meeting some unexpected situation using parallelExec.
	if finalCon, partialCon := sessionVars.HashAggFinalConcurrency(), sessionVars.HashAggPartialConcurrency(); finalCon <= 0 || partialCon <= 0 || finalCon == 1 && partialCon == 1 {
		e.IsUnparallelExec = true
	}
	partialOrdinal := 0
	exprCtx := b.ctx.GetExprCtx()
	for i, aggDesc := range v.AggFuncs {
		if e.IsUnparallelExec {
			e.PartialAggFuncs = append(e.PartialAggFuncs, aggfuncs.Build(exprCtx, aggDesc, i))
		} else {
			ordinal := []int{partialOrdinal}
			partialOrdinal++
			if aggDesc.Name == ast.AggFuncAvg {
				ordinal = append(ordinal, partialOrdinal+1)
				partialOrdinal++
			}
			partialAggDesc, finalDesc := aggDesc.Split(ordinal)
			partialAggFunc := aggfuncs.Build(exprCtx, partialAggDesc, i)
			finalAggFunc := aggfuncs.Build(exprCtx, finalDesc, i)
			e.PartialAggFuncs = append(e.PartialAggFuncs, partialAggFunc)
			e.FinalAggFuncs = append(e.FinalAggFuncs, finalAggFunc)
			if partialAggDesc.Name == ast.AggFuncGroupConcat {
				// For group_concat, finalAggFunc and partialAggFunc need shared `truncate` flag to do duplicate.
				finalAggFunc.(interface{ SetTruncated(t *int32) }).SetTruncated(
					partialAggFunc.(interface{ GetTruncated() *int32 }).GetTruncated(),
				)
			}
		}
		if e.DefaultVal != nil {
			value := aggDesc.GetDefaultValue()
			e.DefaultVal.AppendDatum(i, &value)
		}
	}

	executor_metrics.ExecutorCounterHashAggExec.Inc()
	return e
}

func (b *executorBuilder) buildStreamAgg(v *plannercore.PhysicalStreamAgg) exec.Executor {
	src := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	return b.buildStreamAggFromChildExec(src, v)
}

func (b *executorBuilder) buildStreamAggFromChildExec(childExec exec.Executor, v *plannercore.PhysicalStreamAgg) *aggregate.StreamAggExec {
	exprCtx := b.ctx.GetExprCtx()
	e := &aggregate.StreamAggExec{
		BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID(), childExec),
		GroupChecker: vecgroupchecker.NewVecGroupChecker(exprCtx.GetEvalCtx(), b.ctx.GetSessionVars().EnableVectorizedExpression, v.GroupByItems),
		AggFuncs:     make([]aggfuncs.AggFunc, 0, len(v.AggFuncs)),
	}

	if len(v.GroupByItems) != 0 || aggregation.IsAllFirstRow(v.AggFuncs) {
		e.DefaultVal = nil
	} else {
		// Only do this for final agg, see issue #35295, #30923
		if v.IsFinalAgg() {
			e.DefaultVal = e.AllocPool.Alloc(exec.RetTypes(e), 1, 1)
		}
	}
	for i, aggDesc := range v.AggFuncs {
		aggFunc := aggfuncs.Build(exprCtx, aggDesc, i)
		e.AggFuncs = append(e.AggFuncs, aggFunc)
		if e.DefaultVal != nil {
			value := aggDesc.GetDefaultValue()
			e.DefaultVal.AppendDatum(i, &value)
		}
	}

	executor_metrics.ExecutorStreamAggExec.Inc()
	return e
}

func (b *executorBuilder) buildSelection(v *plannercore.PhysicalSelection) exec.Executor {
	childExec := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	e := &SelectionExec{
		selectionExecutorContext: newSelectionExecutorContext(b.ctx),
		BaseExecutorV2:           exec.NewBaseExecutorV2(b.ctx.GetSessionVars(), v.Schema(), v.ID(), childExec),
		filters:                  v.Conditions,
	}
	return e
}

func (b *executorBuilder) buildExpand(v *plannercore.PhysicalExpand) exec.Executor {
	childExec := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	levelES := make([]*expression.EvaluatorSuite, 0, len(v.LevelExprs))
	for _, exprs := range v.LevelExprs {
		// column evaluator can always refer others inside expand.
		// grouping column's nullability change should be seen as a new column projecting.
		// since input inside expand logic should be targeted and reused for N times.
		// column evaluator's swapping columns logic will pollute the input data.
		levelE := expression.NewEvaluatorSuite(exprs, true)
		levelES = append(levelES, levelE)
	}
	e := &ExpandExec{
		BaseExecutor:        exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID(), childExec),
		numWorkers:          int64(b.ctx.GetSessionVars().ProjectionConcurrency()),
		levelEvaluatorSuits: levelES,
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

func (b *executorBuilder) buildProjection(v *plannercore.PhysicalProjection) exec.Executor {
	childExec := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	e := &ProjectionExec{
		projectionExecutorContext: newProjectionExecutorContext(b.ctx),
		BaseExecutorV2:            exec.NewBaseExecutorV2(b.ctx.GetSessionVars(), v.Schema(), v.ID(), childExec),
		numWorkers:                int64(b.ctx.GetSessionVars().ProjectionConcurrency()),
		evaluatorSuit:             expression.NewEvaluatorSuite(v.Exprs, v.AvoidColumnEvaluator),
		calculateNoDelay:          v.CalculateNoDelay,
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

func (b *executorBuilder) buildTableDual(v *plannercore.PhysicalTableDual) exec.Executor {
	if v.RowCount != 0 && v.RowCount != 1 {
		b.err = errors.Errorf("buildTableDual failed, invalid row count for dual table: %v", v.RowCount)
		return nil
	}
	base := exec.NewBaseExecutorV2(b.ctx.GetSessionVars(), v.Schema(), v.ID())
	base.SetInitCap(v.RowCount)
	e := &TableDualExec{
		BaseExecutorV2: base,
		numDualRows:    v.RowCount,
	}
	return e
}

// `getSnapshotTS` returns for-update-ts if in insert/update/delete/lock statement otherwise the isolation read ts
// Please notice that in RC isolation, the above two ts are the same
func (b *executorBuilder) getSnapshotTS() (ts uint64, err error) {
	if b.forDataReaderBuilder {
		return b.dataReaderTS, nil
	}

	txnManager := sessiontxn.GetTxnManager(b.ctx)
	if b.inInsertStmt || b.inUpdateStmt || b.inDeleteStmt || b.inSelectLockStmt {
		return txnManager.GetStmtForUpdateTS()
	}
	return txnManager.GetStmtReadTS()
}

// getSnapshot get the appropriate snapshot from txnManager and set
// the relevant snapshot options before return.
func (b *executorBuilder) getSnapshot() (kv.Snapshot, error) {
	var snapshot kv.Snapshot
	var err error

	txnManager := sessiontxn.GetTxnManager(b.ctx)
	if b.inInsertStmt || b.inUpdateStmt || b.inDeleteStmt || b.inSelectLockStmt {
		snapshot, err = txnManager.GetSnapshotWithStmtForUpdateTS()
	} else {
		snapshot, err = txnManager.GetSnapshotWithStmtReadTS()
	}
	if err != nil {
		return nil, err
	}

	sessVars := b.ctx.GetSessionVars()
	replicaReadType := sessVars.GetReplicaRead()
	snapshot.SetOption(kv.ReadReplicaScope, b.readReplicaScope)
	snapshot.SetOption(kv.TaskID, sessVars.StmtCtx.TaskID)
	snapshot.SetOption(kv.TiKVClientReadTimeout, sessVars.GetTiKVClientReadTimeout())
	snapshot.SetOption(kv.ResourceGroupName, sessVars.StmtCtx.ResourceGroupName)
	snapshot.SetOption(kv.ExplicitRequestSourceType, sessVars.ExplicitRequestSourceType)

	if replicaReadType.IsClosestRead() && b.readReplicaScope != kv.GlobalTxnScope {
		snapshot.SetOption(kv.MatchStoreLabels, []*metapb.StoreLabel{
			{
				Key:   placement.DCLabelKey,
				Value: b.readReplicaScope,
			},
		})
	}

	return snapshot, nil
}

func (b *executorBuilder) buildMemTable(v *plannercore.PhysicalMemTable) exec.Executor {
	switch v.DBName.L {
	case util.MetricSchemaName.L:
		return &MemTableReaderExec{
			BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
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
				BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
				table:        v.Table,
				retriever: &clusterConfigRetriever{
					extractor: v.Extractor.(*plannercore.ClusterTableExtractor),
				},
			}
		case strings.ToLower(infoschema.TableClusterLoad):
			return &MemTableReaderExec{
				BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
				table:        v.Table,
				retriever: &clusterServerInfoRetriever{
					extractor:      v.Extractor.(*plannercore.ClusterTableExtractor),
					serverInfoType: diagnosticspb.ServerInfoType_LoadInfo,
				},
			}
		case strings.ToLower(infoschema.TableClusterHardware):
			return &MemTableReaderExec{
				BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
				table:        v.Table,
				retriever: &clusterServerInfoRetriever{
					extractor:      v.Extractor.(*plannercore.ClusterTableExtractor),
					serverInfoType: diagnosticspb.ServerInfoType_HardwareInfo,
				},
			}
		case strings.ToLower(infoschema.TableClusterSystemInfo):
			return &MemTableReaderExec{
				BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
				table:        v.Table,
				retriever: &clusterServerInfoRetriever{
					extractor:      v.Extractor.(*plannercore.ClusterTableExtractor),
					serverInfoType: diagnosticspb.ServerInfoType_SystemInfo,
				},
			}
		case strings.ToLower(infoschema.TableClusterLog):
			return &MemTableReaderExec{
				BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
				table:        v.Table,
				retriever: &clusterLogRetriever{
					extractor: v.Extractor.(*plannercore.ClusterLogTableExtractor),
				},
			}
		case strings.ToLower(infoschema.TableTiDBHotRegionsHistory):
			return &MemTableReaderExec{
				BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
				table:        v.Table,
				retriever: &hotRegionsHistoryRetriver{
					extractor: v.Extractor.(*plannercore.HotRegionsHistoryTableExtractor),
				},
			}
		case strings.ToLower(infoschema.TableInspectionResult):
			return &MemTableReaderExec{
				BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
				table:        v.Table,
				retriever: &inspectionResultRetriever{
					extractor: v.Extractor.(*plannercore.InspectionResultTableExtractor),
					timeRange: v.QueryTimeRange,
				},
			}
		case strings.ToLower(infoschema.TableInspectionSummary):
			return &MemTableReaderExec{
				BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
				table:        v.Table,
				retriever: &inspectionSummaryRetriever{
					table:     v.Table,
					extractor: v.Extractor.(*plannercore.InspectionSummaryTableExtractor),
					timeRange: v.QueryTimeRange,
				},
			}
		case strings.ToLower(infoschema.TableInspectionRules):
			return &MemTableReaderExec{
				BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
				table:        v.Table,
				retriever: &inspectionRuleRetriever{
					extractor: v.Extractor.(*plannercore.InspectionRuleTableExtractor),
				},
			}
		case strings.ToLower(infoschema.TableMetricSummary):
			return &MemTableReaderExec{
				BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
				table:        v.Table,
				retriever: &MetricsSummaryRetriever{
					table:     v.Table,
					extractor: v.Extractor.(*plannercore.MetricSummaryTableExtractor),
					timeRange: v.QueryTimeRange,
				},
			}
		case strings.ToLower(infoschema.TableMetricSummaryByLabel):
			return &MemTableReaderExec{
				BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
				table:        v.Table,
				retriever: &MetricsSummaryByLabelRetriever{
					table:     v.Table,
					extractor: v.Extractor.(*plannercore.MetricSummaryTableExtractor),
					timeRange: v.QueryTimeRange,
				},
			}
		case strings.ToLower(infoschema.TableTiKVRegionPeers):
			return &MemTableReaderExec{
				BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
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
			strings.ToLower(infoschema.TableClientErrorsSummaryGlobal),
			strings.ToLower(infoschema.TableClientErrorsSummaryByUser),
			strings.ToLower(infoschema.TableClientErrorsSummaryByHost),
			strings.ToLower(infoschema.TableAttributes),
			strings.ToLower(infoschema.TablePlacementPolicies),
			strings.ToLower(infoschema.TableTrxSummary),
			strings.ToLower(infoschema.TableVariablesInfo),
			strings.ToLower(infoschema.TableUserAttributes),
			strings.ToLower(infoschema.ClusterTableTrxSummary),
			strings.ToLower(infoschema.TableMemoryUsage),
			strings.ToLower(infoschema.TableMemoryUsageOpsHistory),
			strings.ToLower(infoschema.ClusterTableMemoryUsage),
			strings.ToLower(infoschema.ClusterTableMemoryUsageOpsHistory),
			strings.ToLower(infoschema.TableResourceGroups),
			strings.ToLower(infoschema.TableRunawayWatches),
			strings.ToLower(infoschema.TableCheckConstraints),
			strings.ToLower(infoschema.TableTiDBCheckConstraints),
			strings.ToLower(infoschema.TableKeywords),
			strings.ToLower(infoschema.TableTiDBIndexUsage),
			strings.ToLower(infoschema.ClusterTableTiDBIndexUsage):
			memTracker := memory.NewTracker(v.ID(), -1)
			memTracker.AttachTo(b.ctx.GetSessionVars().StmtCtx.MemTracker)
			return &MemTableReaderExec{
				BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
				table:        v.Table,
				retriever: &memtableRetriever{
					table:      v.Table,
					columns:    v.Columns,
					extractor:  v.Extractor,
					memTracker: memTracker,
				},
			}
		case strings.ToLower(infoschema.TableTiDBTrx),
			strings.ToLower(infoschema.ClusterTableTiDBTrx):
			return &MemTableReaderExec{
				BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
				table:        v.Table,
				retriever: &tidbTrxTableRetriever{
					table:   v.Table,
					columns: v.Columns,
				},
			}
		case strings.ToLower(infoschema.TableDataLockWaits):
			return &MemTableReaderExec{
				BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
				table:        v.Table,
				retriever: &dataLockWaitsTableRetriever{
					table:   v.Table,
					columns: v.Columns,
				},
			}
		case strings.ToLower(infoschema.TableDeadlocks),
			strings.ToLower(infoschema.ClusterTableDeadlocks):
			return &MemTableReaderExec{
				BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
				table:        v.Table,
				retriever: &deadlocksTableRetriever{
					table:   v.Table,
					columns: v.Columns,
				},
			}
		case strings.ToLower(infoschema.TableStatementsSummary),
			strings.ToLower(infoschema.TableStatementsSummaryHistory),
			strings.ToLower(infoschema.TableStatementsSummaryEvicted),
			strings.ToLower(infoschema.ClusterTableStatementsSummary),
			strings.ToLower(infoschema.ClusterTableStatementsSummaryHistory),
			strings.ToLower(infoschema.ClusterTableStatementsSummaryEvicted):
			var extractor *plannercore.StatementsSummaryExtractor
			if v.Extractor != nil {
				extractor = v.Extractor.(*plannercore.StatementsSummaryExtractor)
			}
			return &MemTableReaderExec{
				BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
				table:        v.Table,
				retriever:    buildStmtSummaryRetriever(v.Table, v.Columns, extractor),
			}
		case strings.ToLower(infoschema.TableColumns):
			return &MemTableReaderExec{
				BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
				table:        v.Table,
				retriever: &hugeMemTableRetriever{
					table:              v.Table,
					columns:            v.Columns,
					extractor:          v.Extractor.(*plannercore.InfoSchemaColumnsExtractor),
					viewSchemaMap:      make(map[int64]*expression.Schema),
					viewOutputNamesMap: make(map[int64]types.NameSlice),
				},
			}
		case strings.ToLower(infoschema.TableSlowQuery), strings.ToLower(infoschema.ClusterTableSlowLog):
			memTracker := memory.NewTracker(v.ID(), -1)
			memTracker.AttachTo(b.ctx.GetSessionVars().StmtCtx.MemTracker)
			return &MemTableReaderExec{
				BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
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
				BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
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
				BaseExecutor:    exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
				is:              b.is,
				DDLJobRetriever: ddlJobRetriever,
			}
		case strings.ToLower(infoschema.TableTiFlashTables),
			strings.ToLower(infoschema.TableTiFlashSegments),
			strings.ToLower(infoschema.TableTiFlashIndexes):
			return &MemTableReaderExec{
				BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
				table:        v.Table,
				retriever: &TiFlashSystemTableRetriever{
					table:      v.Table,
					outputCols: v.Columns,
					extractor:  v.Extractor.(*plannercore.TiFlashSystemTableExtractor),
				},
			}
		}
	}
	tb, _ := b.is.TableByID(context.Background(), v.Table.ID)
	return &TableScanExec{
		BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
		t:            tb,
		columns:      v.Columns,
	}
}

func (b *executorBuilder) buildSort(v *plannercore.PhysicalSort) exec.Executor {
	childExec := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	sortExec := sortexec.SortExec{
		BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID(), childExec),
		ByItems:      v.ByItems,
		ExecSchema:   v.Schema(),
	}
	executor_metrics.ExecutorCounterSortExec.Inc()
	return &sortExec
}

func (b *executorBuilder) buildTopN(v *plannercore.PhysicalTopN) exec.Executor {
	childExec := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	sortExec := sortexec.SortExec{
		BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID(), childExec),
		ByItems:      v.ByItems,
		ExecSchema:   v.Schema(),
	}
	executor_metrics.ExecutorCounterTopNExec.Inc()
	return &sortexec.TopNExec{
		SortExec:    sortExec,
		Limit:       &plannercore.PhysicalLimit{Count: v.Count, Offset: v.Offset},
		Concurrency: b.ctx.GetSessionVars().Concurrency.ExecutorConcurrency,
	}
}

func (b *executorBuilder) buildApply(v *plannercore.PhysicalApply) exec.Executor {
	var (
		innerPlan base.PhysicalPlan
		outerPlan base.PhysicalPlan
	)
	if v.InnerChildIdx == 0 {
		innerPlan = v.Children()[0]
		outerPlan = v.Children()[1]
	} else {
		innerPlan = v.Children()[1]
		outerPlan = v.Children()[0]
	}
	v.OuterSchema = coreusage.ExtractCorColumnsBySchema4PhysicalPlan(innerPlan, outerPlan.Schema())
	leftChild := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	rightChild := b.build(v.Children()[1])
	if b.err != nil {
		return nil
	}
	// test is in the explain/naaj.test#part5.
	// although we prepared the NAEqualConditions, but for Apply mode, we still need move it to other conditions like eq condition did here.
	otherConditions := append(expression.ScalarFuncs2Exprs(v.EqualConditions), expression.ScalarFuncs2Exprs(v.NAEqualConditions)...)
	otherConditions = append(otherConditions, v.OtherConditions...)
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
	tupleJoiner := join.NewJoiner(b.ctx, v.JoinType, v.InnerChildIdx == 0,
		defaultValues, otherConditions, exec.RetTypes(leftChild), exec.RetTypes(rightChild), nil, false)
	serialExec := &join.NestedLoopApplyExec{
		BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID(), outerExec, innerExec),
		InnerExec:    innerExec,
		OuterExec:    outerExec,
		OuterFilter:  outerFilter,
		InnerFilter:  innerFilter,
		Outer:        v.JoinType != logicalop.InnerJoin,
		Joiner:       tupleJoiner,
		OuterSchema:  v.OuterSchema,
		Sctx:         b.ctx,
		CanUseCache:  v.CanUseCache,
	}
	executor_metrics.ExecutorCounterNestedLoopApplyExec.Inc()

	// try parallel mode
	if v.Concurrency > 1 {
		innerExecs := make([]exec.Executor, 0, v.Concurrency)
		innerFilters := make([]expression.CNFExprs, 0, v.Concurrency)
		corCols := make([][]*expression.CorrelatedColumn, 0, v.Concurrency)
		joiners := make([]join.Joiner, 0, v.Concurrency)
		for i := 0; i < v.Concurrency; i++ {
			clonedInnerPlan, err := plannercore.SafeClone(v.SCtx(), innerPlan)
			if err != nil {
				b.err = nil
				return serialExec
			}
			corCol := coreusage.ExtractCorColumnsBySchema4PhysicalPlan(clonedInnerPlan, outerPlan.Schema())
			clonedInnerExec := b.build(clonedInnerPlan)
			if b.err != nil {
				b.err = nil
				return serialExec
			}
			innerExecs = append(innerExecs, clonedInnerExec)
			corCols = append(corCols, corCol)
			innerFilters = append(innerFilters, innerFilter.Clone())
			joiners = append(joiners, join.NewJoiner(b.ctx, v.JoinType, v.InnerChildIdx == 0,
				defaultValues, otherConditions, exec.RetTypes(leftChild), exec.RetTypes(rightChild), nil, false))
		}

		allExecs := append([]exec.Executor{outerExec}, innerExecs...)

		return &ParallelNestedLoopApplyExec{
			BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID(), allExecs...),
			innerExecs:   innerExecs,
			outerExec:    outerExec,
			outerFilter:  outerFilter,
			innerFilter:  innerFilters,
			outer:        v.JoinType != logicalop.InnerJoin,
			joiners:      joiners,
			corCols:      corCols,
			concurrency:  v.Concurrency,
			useCache:     v.CanUseCache,
		}
	}
	return serialExec
}

func (b *executorBuilder) buildMaxOneRow(v *plannercore.PhysicalMaxOneRow) exec.Executor {
	childExec := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	base := exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID(), childExec)
	base.SetInitCap(2)
	base.SetMaxChunkSize(2)
	e := &MaxOneRowExec{BaseExecutor: base}
	return e
}

func (b *executorBuilder) buildUnionAll(v *plannercore.PhysicalUnionAll) exec.Executor {
	childExecs := make([]exec.Executor, len(v.Children()))
	for i, child := range v.Children() {
		childExecs[i] = b.build(child)
		if b.err != nil {
			return nil
		}
	}
	e := &unionexec.UnionExec{
		BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID(), childExecs...),
		Concurrency:  b.ctx.GetSessionVars().UnionConcurrency(),
	}
	return e
}

func buildHandleColsForSplit(sc *stmtctx.StatementContext, tbInfo *model.TableInfo) plannerutil.HandleCols {
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
		return plannerutil.NewCommonHandleCols(sc, tbInfo, primaryIdx, tableCols)
	}
	intCol := &expression.Column{
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}
	return plannerutil.NewIntHandleCols(intCol)
}

func (b *executorBuilder) buildSplitRegion(v *plannercore.SplitRegion) exec.Executor {
	base := exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID())
	base.SetInitCap(1)
	base.SetMaxChunkSize(1)
	if v.IndexInfo != nil {
		return &SplitIndexRegionExec{
			BaseExecutor:   base,
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
			BaseExecutor:   base,
			tableInfo:      v.TableInfo,
			partitionNames: v.PartitionNames,
			handleCols:     handleCols,
			valueLists:     v.ValueLists,
		}
	}
	return &SplitTableRegionExec{
		BaseExecutor:   base,
		tableInfo:      v.TableInfo,
		partitionNames: v.PartitionNames,
		handleCols:     handleCols,
		lower:          v.Lower,
		upper:          v.Upper,
		num:            v.Num,
	}
}

func (b *executorBuilder) buildUpdate(v *plannercore.Update) exec.Executor {
	b.inUpdateStmt = true
	tblID2table := make(map[int64]table.Table, len(v.TblColPosInfos))
	multiUpdateOnSameTable := make(map[int64]bool)
	for _, info := range v.TblColPosInfos {
		tbl, _ := b.is.TableByID(context.Background(), info.TblID)
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
	if b.err = b.updateForUpdateTS(); b.err != nil {
		return nil
	}

	selExec := b.build(v.SelectPlan)
	if b.err != nil {
		return nil
	}
	base := exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID(), selExec)
	base.SetInitCap(chunk.ZeroCapacity)
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
		BaseExecutor:              base,
		OrderedList:               v.OrderedList,
		allAssignmentsAreConstant: v.AllAssignmentsAreConstant,
		virtualAssignmentsOffset:  v.VirtualAssignmentsOffset,
		multiUpdateOnSameTable:    multiUpdateOnSameTable,
		tblID2table:               tblID2table,
		tblColPosInfos:            v.TblColPosInfos,
		assignFlag:                assignFlag,
		IgnoreError:               v.IgnoreError,
	}
	updateExec.fkChecks, b.err = buildTblID2FKCheckExecs(b.ctx, tblID2table, v.FKChecks)
	if b.err != nil {
		return nil
	}
	updateExec.fkCascades, b.err = b.buildTblID2FKCascadeExecs(tblID2table, v.FKCascades)
	if b.err != nil {
		return nil
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

func (b *executorBuilder) buildDelete(v *plannercore.Delete) exec.Executor {
	b.inDeleteStmt = true
	tblID2table := make(map[int64]table.Table, len(v.TblColPosInfos))
	for _, info := range v.TblColPosInfos {
		tblID2table[info.TblID], _ = b.is.TableByID(context.Background(), info.TblID)
	}

	if b.err = b.updateForUpdateTS(); b.err != nil {
		return nil
	}

	selExec := b.build(v.SelectPlan)
	if b.err != nil {
		return nil
	}
	base := exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID(), selExec)
	base.SetInitCap(chunk.ZeroCapacity)
	deleteExec := &DeleteExec{
		BaseExecutor:   base,
		tblID2Table:    tblID2table,
		IsMultiTable:   v.IsMultiTable,
		tblColPosInfos: v.TblColPosInfos,
	}
	deleteExec.fkChecks, b.err = buildTblID2FKCheckExecs(b.ctx, tblID2table, v.FKChecks)
	if b.err != nil {
		return nil
	}
	deleteExec.fkCascades, b.err = b.buildTblID2FKCascadeExecs(tblID2table, v.FKCascades)
	if b.err != nil {
		return nil
	}
	return deleteExec
}

func (b *executorBuilder) updateForUpdateTS() error {
	// GetStmtForUpdateTS will auto update the for update ts if it is necessary
	_, err := sessiontxn.GetTxnManager(b.ctx).GetStmtForUpdateTS()
	return err
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
	concurrency := adaptiveAnlayzeDistSQLConcurrency(context.Background(), b.ctx)
	base := baseAnalyzeExec{
		ctx:         b.ctx,
		tableID:     task.TableID,
		concurrency: concurrency,
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
		SketchSize: statistics.MaxSketchSize,
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

func (b *executorBuilder) buildAnalyzeSamplingPushdown(
	task plannercore.AnalyzeColumnsTask,
	opts map[ast.AnalyzeOptionType]uint64,
	schemaForVirtualColEval *expression.Schema,
) *analyzeTask {
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
	var sampleRateReason string
	if opts[ast.AnalyzeOptNumSamples] == 0 {
		*sampleRate = math.Float64frombits(opts[ast.AnalyzeOptSampleRate])
		if *sampleRate < 0 {
			*sampleRate, sampleRateReason = b.getAdjustedSampleRate(task)
			if task.PartitionName != "" {
				sc.AppendNote(errors.NewNoStackErrorf(
					`Analyze use auto adjusted sample rate %f for table %s.%s's partition %s, reason to use this rate is "%s"`,
					*sampleRate,
					task.DBName,
					task.TableName,
					task.PartitionName,
					sampleRateReason,
				))
			} else {
				sc.AppendNote(errors.NewNoStackErrorf(
					`Analyze use auto adjusted sample rate %f for table %s.%s, reason to use this rate is "%s"`,
					*sampleRate,
					task.DBName,
					task.TableName,
					sampleRateReason,
				))
			}
		}
	}
	job := &statistics.AnalyzeJob{
		DBName:           task.DBName,
		TableName:        task.TableName,
		PartitionName:    task.PartitionName,
		SampleRateReason: sampleRateReason,
	}
	concurrency := adaptiveAnlayzeDistSQLConcurrency(context.Background(), b.ctx)
	base := baseAnalyzeExec{
		ctx:         b.ctx,
		tableID:     task.TableID,
		concurrency: concurrency,
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
		SketchSize:   statistics.MaxSketchSize,
		ColumnsInfo:  util.ColumnsToProto(task.ColsInfo, task.TblInfo.PKIsHandle, false, false),
		ColumnGroups: colGroups,
	}
	if task.TblInfo != nil {
		e.analyzePB.ColReq.PrimaryColumnIds = tables.TryGetCommonPkColumnIds(task.TblInfo)
		if task.TblInfo.IsCommonHandle {
			e.analyzePB.ColReq.PrimaryPrefixColumnIds = tables.PrimaryPrefixColumnIDs(task.TblInfo)
		}
	}
	b.err = tables.SetPBColumnsDefaultValue(b.ctx.GetExprCtx(), e.analyzePB.ColReq.ColumnsInfo, task.ColsInfo)
	return &analyzeTask{taskType: colTask, colExec: e, job: job}
}

// getAdjustedSampleRate calculate the sample rate by the table size. If we cannot get the table size. We use the 0.001 as the default sample rate.
// From the paper "Random sampling for histogram construction: how much is enough?"'s Corollary 1 to Theorem 5,
// for a table size n, histogram size k, maximum relative error in bin size f, and error probability gamma,
// the minimum random sample size is
//
//	r = 4 * k * ln(2*n/gamma) / f^2
//
// If we take f = 0.5, gamma = 0.01, n =1e6, we would got r = 305.82* k.
// Since the there's log function over the table size n, the r grows slowly when the n increases.
// If we take n = 1e12, a 300*k sample still gives <= 0.66 bin size error with probability 0.99.
// So if we don't consider the top-n values, we can keep the sample size at 300*256.
// But we may take some top-n before building the histogram, so we increase the sample a little.
func (b *executorBuilder) getAdjustedSampleRate(task plannercore.AnalyzeColumnsTask) (sampleRate float64, reason string) {
	statsHandle := domain.GetDomain(b.ctx).StatsHandle()
	defaultRate := 0.001
	if statsHandle == nil {
		return defaultRate, fmt.Sprintf("statsHandler is nil, use the default-rate=%v", defaultRate)
	}
	var statsTbl *statistics.Table
	tid := task.TableID.GetStatisticsID()
	if tid == task.TblInfo.ID {
		statsTbl = statsHandle.GetTableStats(task.TblInfo)
	} else {
		statsTbl = statsHandle.GetPartitionStats(task.TblInfo, tid)
	}
	approxiCount, hasPD := b.getApproximateTableCountFromStorage(tid, task)
	// If there's no stats meta and no pd, return the default rate.
	if statsTbl == nil && !hasPD {
		return defaultRate, fmt.Sprintf("TiDB cannot get the row count of the table, use the default-rate=%v", defaultRate)
	}
	// If the count in stats_meta is still 0 and there's no information from pd side, we scan all rows.
	if statsTbl.RealtimeCount == 0 && !hasPD {
		return 1, "TiDB assumes that the table is empty and cannot get row count from PD, use sample-rate=1"
	}
	// we have issue https://github.com/pingcap/tidb/issues/29216.
	// To do a workaround for this issue, we check the approxiCount from the pd side to do a comparison.
	// If the count from the stats_meta is extremely smaller than the approximate count from the pd,
	// we think that we meet this issue and use the approximate count to calculate the sample rate.
	if float64(statsTbl.RealtimeCount*5) < approxiCount {
		// Confirmed by TiKV side, the experience error rate of the approximate count is about 20%.
		// So we increase the number to 150000 to reduce this error rate.
		sampleRate = math.Min(1, 150000/approxiCount)
		return sampleRate, fmt.Sprintf("Row count in stats_meta is much smaller compared with the row count got by PD, use min(1, 15000/%v) as the sample-rate=%v", approxiCount, sampleRate)
	}
	// If we don't go into the above if branch and we still detect the count is zero. Return 1 to prevent the dividing zero.
	if statsTbl.RealtimeCount == 0 {
		return 1, "TiDB assumes that the table is empty, use sample-rate=1"
	}
	// We are expected to scan about 100000 rows or so.
	// Since there's tiny error rate around the count from the stats meta, we use 110000 to get a little big result
	sampleRate = math.Min(1, config.DefRowsForSampleRate/float64(statsTbl.RealtimeCount))
	return sampleRate, fmt.Sprintf("use min(1, %v/%v) as the sample-rate=%v", config.DefRowsForSampleRate, statsTbl.RealtimeCount, sampleRate)
}

func (b *executorBuilder) getApproximateTableCountFromStorage(tid int64, task plannercore.AnalyzeColumnsTask) (float64, bool) {
	return pdhelper.GlobalPDHelper.GetApproximateTableCountFromStorage(context.Background(), b.ctx, tid, task.DBName, task.TableName, task.PartitionName)
}

func (b *executorBuilder) buildAnalyzeColumnsPushdown(
	task plannercore.AnalyzeColumnsTask,
	opts map[ast.AnalyzeOptionType]uint64,
	autoAnalyze string,
	schemaForVirtualColEval *expression.Schema,
) *analyzeTask {
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
	concurrency := adaptiveAnlayzeDistSQLConcurrency(context.Background(), b.ctx)
	base := baseAnalyzeExec{
		ctx:         b.ctx,
		tableID:     task.TableID,
		concurrency: concurrency,
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
		SampleSize:    MaxRegionSampleSize,
		SketchSize:    statistics.MaxSketchSize,
		ColumnsInfo:   util.ColumnsToProto(cols, task.HandleCols != nil && task.HandleCols.IsInt(), false, false),
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
		e.analyzePB.IdxReq.SketchSize = statistics.MaxSketchSize
		e.analyzePB.ColReq.PrimaryColumnIds = tables.TryGetCommonPkColumnIds(task.TblInfo)
		e.analyzePB.Tp = tipb.AnalyzeType_TypeMixed
		e.commonHandle = task.CommonHandleInfo
	}
	b.err = tables.SetPBColumnsDefaultValue(b.ctx.GetExprCtx(), e.analyzePB.ColReq.ColumnsInfo, cols)
	return &analyzeTask{taskType: colTask, colExec: e, job: job}
}

func (b *executorBuilder) buildAnalyze(v *plannercore.Analyze) exec.Executor {
	gp := domain.GetDomain(b.ctx).StatsHandle().GPool()
	e := &AnalyzeExec{
		BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
		tasks:        make([]*analyzeTask, 0, len(v.ColTasks)+len(v.IdxTasks)),
		opts:         v.Opts,
		OptionsMap:   v.OptionsMap,
		wg:           util.NewWaitGroupPool(gp),
		gp:           gp,
		errExitCh:    make(chan struct{}),
	}
	autoAnalyze := ""
	if b.ctx.GetSessionVars().InRestrictedSQL {
		autoAnalyze = "auto "
	}
	exprCtx := b.ctx.GetExprCtx()
	for _, task := range v.ColTasks {
		columns, _, err := expression.ColumnInfos2ColumnsAndNames(
			exprCtx,
			pmodel.NewCIStr(task.AnalyzeInfo.DBName),
			task.TblInfo.Name,
			task.ColsInfo,
			task.TblInfo,
		)
		if err != nil {
			b.err = err
			return nil
		}
		schema := expression.NewSchema(columns...)
		e.tasks = append(e.tasks, b.buildAnalyzeColumnsPushdown(task, v.Opts, autoAnalyze, schema))
		// Other functions may set b.err, so we need to check it here.
		if b.err != nil {
			return nil
		}
	}
	for _, task := range v.IdxTasks {
		e.tasks = append(e.tasks, b.buildAnalyzeIndexPushdown(task, v.Opts, autoAnalyze))
		if b.err != nil {
			return nil
		}
	}
	return e
}

// markChildrenUsedCols compares each child with the output schema, and mark
// each column of the child is used by output or not.
func markChildrenUsedCols(outputCols []*expression.Column, childSchemas ...*expression.Schema) (childrenUsed [][]int) {
	childrenUsed = make([][]int, 0, len(childSchemas))
	markedOffsets := make(map[int]int)
	// keep the original maybe reversed order.
	for originalIdx, col := range outputCols {
		markedOffsets[col.Index] = originalIdx
	}
	prefixLen := 0
	type intPair struct {
		first  int
		second int
	}
	// for example here.
	// left child schema: [col11]
	// right child schema: [col21, col22]
	// output schema is [col11, col22, col21], if not records the original derived order after physical resolve index.
	// the lused will be [0], the rused will be [0,1], while the actual order is dismissed, [1,0] is correct for rused.
	for _, childSchema := range childSchemas {
		usedIdxPair := make([]intPair, 0, len(childSchema.Columns))
		for i := range childSchema.Columns {
			if originalIdx, ok := markedOffsets[prefixLen+i]; ok {
				usedIdxPair = append(usedIdxPair, intPair{first: originalIdx, second: i})
			}
		}
		// sort the used idxes according their original indexes derived after resolveIndex.
		slices.SortFunc(usedIdxPair, func(a, b intPair) int {
			return cmp.Compare(a.first, b.first)
		})
		usedIdx := make([]int, 0, len(childSchema.Columns))
		for _, one := range usedIdxPair {
			usedIdx = append(usedIdx, one.second)
		}
		childrenUsed = append(childrenUsed, usedIdx)
		prefixLen += childSchema.Len()
	}
	return
}

func (*executorBuilder) corColInDistPlan(plans []base.PhysicalPlan) bool {
	for _, p := range plans {
		switch x := p.(type) {
		case *plannercore.PhysicalSelection:
			for _, cond := range x.Conditions {
				if len(expression.ExtractCorColumns(cond)) > 0 {
					return true
				}
			}
		case *plannercore.PhysicalProjection:
			for _, expr := range x.Exprs {
				if len(expression.ExtractCorColumns(expr)) > 0 {
					return true
				}
			}
		case *plannercore.PhysicalTopN:
			for _, byItem := range x.ByItems {
				if len(expression.ExtractCorColumns(byItem.Expr)) > 0 {
					return true
				}
			}
		case *plannercore.PhysicalTableScan:
			for _, cond := range x.LateMaterializationFilterCondition {
				if len(expression.ExtractCorColumns(cond)) > 0 {
					return true
				}
			}
		}
	}
	return false
}

// corColInAccess checks whether there's correlated column in access conditions.
func (*executorBuilder) corColInAccess(p base.PhysicalPlan) bool {
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

func (b *executorBuilder) newDataReaderBuilder(p base.PhysicalPlan) (*dataReaderBuilder, error) {
	ts, err := b.getSnapshotTS()
	if err != nil {
		return nil, err
	}

	builderForDataReader := *b
	builderForDataReader.forDataReaderBuilder = true
	builderForDataReader.dataReaderTS = ts

	return &dataReaderBuilder{
		plan:            p,
		executorBuilder: &builderForDataReader,
	}, nil
}

func (b *executorBuilder) buildIndexLookUpJoin(v *plannercore.PhysicalIndexJoin) exec.Executor {
	outerExec := b.build(v.Children()[1-v.InnerChildIdx])
	if b.err != nil {
		return nil
	}
	outerTypes := exec.RetTypes(outerExec)
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
			b.err = errors.Annotate(exeerrors.ErrBuildExecutor, "join's inner condition should be empty")
			return nil
		}
	} else {
		leftTypes, rightTypes = outerTypes, innerTypes
		outerFilter = v.LeftConditions
		if len(v.RightConditions) > 0 {
			b.err = errors.Annotate(exeerrors.ErrBuildExecutor, "join's inner condition should be empty")
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

	e := &join.IndexLookUpJoin{
		BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID(), outerExec),
		OuterCtx: join.OuterCtx{
			RowTypes:  outerTypes,
			HashTypes: outerHashTypes,
			Filter:    outerFilter,
		},
		InnerCtx: join.InnerCtx{
			ReaderBuilder: readerBuilder,
			RowTypes:      innerTypes,
			HashTypes:     innerHashTypes,
			ColLens:       v.IdxColLens,
			HasPrefixCol:  hasPrefixCol,
		},
		WorkerWg:      new(sync.WaitGroup),
		IsOuterJoin:   v.JoinType.IsOuterJoin(),
		IndexRanges:   v.Ranges,
		KeyOff2IdxOff: v.KeyOff2IdxOff,
		LastColHelper: v.CompareFilters,
		Finished:      &atomic.Value{},
	}
	colsFromChildren := v.Schema().Columns
	if v.JoinType == logicalop.LeftOuterSemiJoin || v.JoinType == logicalop.AntiLeftOuterSemiJoin {
		colsFromChildren = colsFromChildren[:len(colsFromChildren)-1]
	}
	childrenUsedSchema := markChildrenUsedCols(colsFromChildren, v.Children()[0].Schema(), v.Children()[1].Schema())
	e.Joiner = join.NewJoiner(b.ctx, v.JoinType, v.InnerChildIdx == 0, defaultValues, v.OtherConditions, leftTypes, rightTypes, childrenUsedSchema, false)
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
	e.OuterCtx.KeyCols = outerKeyCols
	e.InnerCtx.KeyCols = innerKeyCols
	e.InnerCtx.KeyColIDs = innerKeyColIDs
	e.InnerCtx.KeyCollators = keyCollators

	outerHashCols, innerHashCols := make([]int, len(v.OuterHashKeys)), make([]int, len(v.InnerHashKeys))
	hashCollators := make([]collate.Collator, 0, len(v.InnerHashKeys))
	for i := 0; i < len(v.OuterHashKeys); i++ {
		outerHashCols[i] = v.OuterHashKeys[i].Index
	}
	for i := 0; i < len(v.InnerHashKeys); i++ {
		innerHashCols[i] = v.InnerHashKeys[i].Index
		hashCollators = append(hashCollators, collate.GetCollator(v.InnerHashKeys[i].RetType.GetCollate()))
	}
	e.OuterCtx.HashCols = outerHashCols
	e.InnerCtx.HashCols = innerHashCols
	e.InnerCtx.HashCollators = hashCollators

	e.JoinResult = exec.TryNewCacheChunk(e)
	executor_metrics.ExecutorCounterIndexLookUpJoin.Inc()
	return e
}

func (b *executorBuilder) buildIndexLookUpMergeJoin(v *plannercore.PhysicalIndexMergeJoin) exec.Executor {
	outerExec := b.build(v.Children()[1-v.InnerChildIdx])
	if b.err != nil {
		return nil
	}
	outerTypes := exec.RetTypes(outerExec)
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
			b.err = errors.Annotate(exeerrors.ErrBuildExecutor, "join's inner condition should be empty")
			return nil
		}
	} else {
		leftTypes, rightTypes = outerTypes, innerTypes
		outerFilter = v.LeftConditions
		if len(v.RightConditions) > 0 {
			b.err = errors.Annotate(exeerrors.ErrBuildExecutor, "join's inner condition should be empty")
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
	executor_metrics.ExecutorCounterIndexLookUpJoin.Inc()

	readerBuilder, err := b.newDataReaderBuilder(innerPlan)
	if err != nil {
		b.err = err
		return nil
	}

	e := &join.IndexLookUpMergeJoin{
		BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID(), outerExec),
		OuterMergeCtx: join.OuterMergeCtx{
			RowTypes:      outerTypes,
			Filter:        outerFilter,
			JoinKeys:      v.OuterJoinKeys,
			KeyCols:       outerKeyCols,
			NeedOuterSort: v.NeedOuterSort,
			CompareFuncs:  v.OuterCompareFuncs,
		},
		InnerMergeCtx: join.InnerMergeCtx{
			ReaderBuilder:           readerBuilder,
			RowTypes:                innerTypes,
			JoinKeys:                v.InnerJoinKeys,
			KeyCols:                 innerKeyCols,
			KeyCollators:            keyCollators,
			CompareFuncs:            v.CompareFuncs,
			ColLens:                 v.IdxColLens,
			Desc:                    v.Desc,
			KeyOff2KeyOffOrderByIdx: v.KeyOff2KeyOffOrderByIdx,
		},
		WorkerWg:      new(sync.WaitGroup),
		IsOuterJoin:   v.JoinType.IsOuterJoin(),
		IndexRanges:   v.Ranges,
		KeyOff2IdxOff: v.KeyOff2IdxOff,
		LastColHelper: v.CompareFilters,
	}
	colsFromChildren := v.Schema().Columns
	if v.JoinType == logicalop.LeftOuterSemiJoin || v.JoinType == logicalop.AntiLeftOuterSemiJoin {
		colsFromChildren = colsFromChildren[:len(colsFromChildren)-1]
	}
	childrenUsedSchema := markChildrenUsedCols(colsFromChildren, v.Children()[0].Schema(), v.Children()[1].Schema())
	joiners := make([]join.Joiner, e.Ctx().GetSessionVars().IndexLookupJoinConcurrency())
	for i := 0; i < len(joiners); i++ {
		joiners[i] = join.NewJoiner(b.ctx, v.JoinType, v.InnerChildIdx == 0, defaultValues, v.OtherConditions, leftTypes, rightTypes, childrenUsedSchema, false)
	}
	e.Joiners = joiners
	return e
}

func (b *executorBuilder) buildIndexNestedLoopHashJoin(v *plannercore.PhysicalIndexHashJoin) exec.Executor {
	joinExec := b.buildIndexLookUpJoin(&(v.PhysicalIndexJoin))
	if b.err != nil {
		return nil
	}
	e := joinExec.(*join.IndexLookUpJoin)
	idxHash := &join.IndexNestedLoopHashJoin{
		IndexLookUpJoin: *e,
		KeepOuterOrder:  v.KeepOuterOrder,
	}
	concurrency := e.Ctx().GetSessionVars().IndexLookupJoinConcurrency()
	idxHash.Joiners = make([]join.Joiner, concurrency)
	for i := 0; i < concurrency; i++ {
		idxHash.Joiners[i] = e.Joiner.Clone()
	}
	return idxHash
}

func buildNoRangeTableReader(b *executorBuilder, v *plannercore.PhysicalTableReader) (*TableReaderExecutor, error) {
	tablePlans := v.TablePlans
	if v.StoreType == kv.TiFlash {
		tablePlans = []base.PhysicalPlan{v.GetTablePlan()}
	}
	dagReq, err := builder.ConstructDAGReq(b.ctx, tablePlans, v.StoreType)
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

	tbl, _ := b.is.TableByID(context.Background(), ts.Table.ID)
	isPartition, physicalTableID := ts.IsPartition()
	if isPartition {
		pt := tbl.(table.PartitionedTable)
		tbl = pt.GetPartition(physicalTableID)
	}
	startTS, err := b.getSnapshotTS()
	if err != nil {
		return nil, err
	}
	paging := b.ctx.GetSessionVars().EnablePaging

	e := &TableReaderExecutor{
		BaseExecutorV2:             exec.NewBaseExecutorV2(b.ctx.GetSessionVars(), v.Schema(), v.ID()),
		tableReaderExecutorContext: newTableReaderExecutorContext(b.ctx),
		indexUsageReporter:         b.buildIndexUsageReporter(v),
		dagPB:                      dagReq,
		startTS:                    startTS,
		txnScope:                   b.txnScope,
		readReplicaScope:           b.readReplicaScope,
		isStaleness:                b.isStaleness,
		netDataSize:                v.GetNetDataSize(),
		table:                      tbl,
		keepOrder:                  ts.KeepOrder,
		desc:                       ts.Desc,
		byItems:                    ts.ByItems,
		columns:                    ts.Columns,
		paging:                     paging,
		corColInFilter:             b.corColInDistPlan(v.TablePlans),
		corColInAccess:             b.corColInAccess(v.TablePlans[0]),
		plans:                      v.TablePlans,
		tablePlan:                  v.GetTablePlan(),
		storeType:                  v.StoreType,
		batchCop:                   v.ReadReqType == plannercore.BatchCop,
	}
	e.buildVirtualColumnInfo()

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

func (b *executorBuilder) buildMPPGather(v *plannercore.PhysicalTableReader) exec.Executor {
	startTs, err := b.getSnapshotTS()
	if err != nil {
		b.err = err
		return nil
	}

	gather := &MPPGather{
		BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
		is:           b.is,
		originalPlan: v.GetTablePlan(),
		startTS:      startTs,
		mppQueryID:   kv.MPPQueryID{QueryTs: getMPPQueryTS(b.ctx), LocalQueryID: getMPPQueryID(b.ctx), ServerID: domain.GetDomain(b.ctx).ServerID()},
		memTracker:   memory.NewTracker(v.ID(), -1),

		columns:                    []*model.ColumnInfo{},
		virtualColumnIndex:         []int{},
		virtualColumnRetFieldTypes: []*types.FieldType{},
	}

	gather.memTracker.AttachTo(b.ctx.GetSessionVars().StmtCtx.MemTracker)

	var hasVirtualCol bool
	for _, col := range v.Schema().Columns {
		if col.VirtualExpr != nil {
			hasVirtualCol = true
			break
		}
	}

	var isSingleDataSource bool
	tableScans := v.GetTableScans()
	if len(tableScans) == 1 {
		isSingleDataSource = true
	}

	// 1. hasVirtualCol: when got virtual column in TableScan, will generate plan like the following,
	//                   and there will be no other operators in the MPP fragment.
	//     MPPGather
	//       ExchangeSender
	//         PhysicalTableScan
	// 2. UnionScan: there won't be any operators like Join between UnionScan and TableScan.
	//               and UnionScan cannot push down to tiflash.
	if !isSingleDataSource {
		if hasVirtualCol || b.encounterUnionScan {
			b.err = errors.Errorf("should only have one TableScan in MPP fragment(hasVirtualCol: %v, encounterUnionScan: %v)", hasVirtualCol, b.encounterUnionScan)
			return nil
		}
		return gather
	}

	// Setup MPPGather.table if isSingleDataSource.
	// Virtual Column or UnionScan need to use it.
	ts := tableScans[0]
	gather.columns = ts.Columns
	if hasVirtualCol {
		gather.virtualColumnIndex, gather.virtualColumnRetFieldTypes = buildVirtualColumnInfo(gather.Schema(), gather.columns)
	}
	tbl, _ := b.is.TableByID(context.Background(), ts.Table.ID)
	isPartition, physicalTableID := ts.IsPartition()
	if isPartition {
		// Only for static pruning partition table.
		pt := tbl.(table.PartitionedTable)
		tbl = pt.GetPartition(physicalTableID)
	}
	gather.table = tbl
	return gather
}

// buildTableReader builds a table reader executor. It first build a no range table reader,
// and then update it ranges from table scan plan.
func (b *executorBuilder) buildTableReader(v *plannercore.PhysicalTableReader) exec.Executor {
	failpoint.Inject("checkUseMPP", func(val failpoint.Value) {
		if !b.ctx.GetSessionVars().InRestrictedSQL && val.(bool) != useMPPExecution(b.ctx, v) {
			if val.(bool) {
				b.err = errors.New("expect mpp but not used")
			} else {
				b.err = errors.New("don't expect mpp but we used it")
			}
			failpoint.Return(nil)
		}
	})
	// https://github.com/pingcap/tidb/issues/50358
	if len(v.Schema().Columns) == 0 && len(v.GetTablePlan().Schema().Columns) > 0 {
		v.SetSchema(v.GetTablePlan().Schema())
	}
	useMPP := useMPPExecution(b.ctx, v)
	useTiFlashBatchCop := v.ReadReqType == plannercore.BatchCop
	useTiFlash := useMPP || useTiFlashBatchCop
	if useTiFlash {
		if _, isTiDBZoneLabelSet := config.GetGlobalConfig().Labels[placement.DCLabelKey]; b.ctx.GetSessionVars().TiFlashReplicaRead != tiflash.AllReplicas && !isTiDBZoneLabelSet {
			b.ctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackErrorf("the variable tiflash_replica_read is ignored, because the entry TiDB[%s] does not set the zone attribute and tiflash_replica_read is '%s'", config.GetGlobalConfig().AdvertiseAddress, tiflash.GetTiFlashReplicaRead(b.ctx.GetSessionVars().TiFlashReplicaRead)))
		}
	}
	if useMPP {
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

	if !b.ctx.GetSessionVars().StmtCtx.UseDynamicPartitionPrune() {
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

	tmp, _ := b.is.TableByID(context.Background(), ts.Table.ID)
	tbl := tmp.(table.PartitionedTable)
	partitions, err := partitionPruning(b.ctx, tbl, v.PlanPartInfo)
	if err != nil {
		b.err = err
		return nil
	}
	if v.StoreType == kv.TiFlash {
		sctx.IsTiFlash.Store(true)
	}

	if len(partitions) == 0 {
		return &TableDualExec{BaseExecutorV2: ret.BaseExecutorV2}
	}

	// Sort the partition is necessary to make the final multiple partition key ranges ordered.
	slices.SortFunc(partitions, func(i, j table.PhysicalTable) int {
		return cmp.Compare(i.GetPhysicalID(), j.GetPhysicalID())
	})
	ret.kvRangeBuilder = kvRangeBuilderFromRangeAndPartition{
		partitions: partitions,
	}

	return ret
}

func buildIndexRangeForEachPartition(rctx *rangerctx.RangerContext, usedPartitions []table.PhysicalTable, contentPos []int64,
	lookUpContent []*join.IndexJoinLookUpContent, indexRanges []*ranger.Range, keyOff2IdxOff []int, cwc *plannercore.ColWithCmpFuncManager) (map[int64][]*ranger.Range, error) {
	contentBucket := make(map[int64][]*join.IndexJoinLookUpContent)
	for _, p := range usedPartitions {
		contentBucket[p.GetPhysicalID()] = make([]*join.IndexJoinLookUpContent, 0, 8)
	}
	for i, pos := range contentPos {
		if _, ok := contentBucket[pos]; ok {
			contentBucket[pos] = append(contentBucket[pos], lookUpContent[i])
		}
	}
	nextRange := make(map[int64][]*ranger.Range)
	for _, p := range usedPartitions {
		ranges, err := buildRangesForIndexJoin(rctx, contentBucket[p.GetPhysicalID()], indexRanges, keyOff2IdxOff, cwc)
		if err != nil {
			return nil, err
		}
		nextRange[p.GetPhysicalID()] = ranges
	}
	return nextRange, nil
}

func getPartitionKeyColOffsets(keyColIDs []int64, pt table.PartitionedTable) []int {
	keyColOffsets := make([]int, len(keyColIDs))
	for i, colID := range keyColIDs {
		offset := -1
		for j, col := range pt.Cols() {
			if colID == col.ID {
				offset = j
				break
			}
		}
		if offset == -1 {
			return nil
		}
		keyColOffsets[i] = offset
	}

	t, ok := pt.(interface {
		PartitionExpr() *tables.PartitionExpr
	})
	if !ok {
		return nil
	}
	pe := t.PartitionExpr()
	if pe == nil {
		return nil
	}

	offsetMap := make(map[int]struct{})
	for _, offset := range keyColOffsets {
		offsetMap[offset] = struct{}{}
	}
	for _, offset := range pe.ColumnOffset {
		if _, ok := offsetMap[offset]; !ok {
			return nil
		}
	}
	return keyColOffsets
}

func (builder *dataReaderBuilder) prunePartitionForInnerExecutor(tbl table.Table, physPlanPartInfo *plannercore.PhysPlanPartInfo,
	lookUpContent []*join.IndexJoinLookUpContent) (usedPartition []table.PhysicalTable, canPrune bool, contentPos []int64, err error) {
	partitionTbl := tbl.(table.PartitionedTable)

	// In index join, this is called by multiple goroutines simultaneously, but partitionPruning is not thread-safe.
	// Use once.Do to avoid DATA RACE here.
	// TODO: condition based pruning can be do in advance.
	condPruneResult, err := builder.partitionPruning(partitionTbl, physPlanPartInfo)
	if err != nil {
		return nil, false, nil, err
	}

	// recalculate key column offsets
	if len(lookUpContent) == 0 {
		return nil, false, nil, nil
	}
	if lookUpContent[0].KeyColIDs == nil {
		return nil, false, nil, plannererrors.ErrInternal.GenWithStack("cannot get column IDs when dynamic pruning")
	}
	keyColOffsets := getPartitionKeyColOffsets(lookUpContent[0].KeyColIDs, partitionTbl)
	if len(keyColOffsets) == 0 {
		return condPruneResult, false, nil, nil
	}

	locateKey := make([]types.Datum, len(partitionTbl.Cols()))
	partitions := make(map[int64]table.PhysicalTable)
	contentPos = make([]int64, len(lookUpContent))
	exprCtx := builder.ctx.GetExprCtx()
	for idx, content := range lookUpContent {
		for i, data := range content.Keys {
			locateKey[keyColOffsets[i]] = data
		}
		p, err := partitionTbl.GetPartitionByRow(exprCtx.GetEvalCtx(), locateKey)
		if table.ErrNoPartitionForGivenValue.Equal(err) {
			continue
		}
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

	// To make the final key ranges involving multiple partitions ordered.
	slices.SortFunc(usedPartition, func(i, j table.PhysicalTable) int {
		return cmp.Compare(i.GetPhysicalID(), j.GetPhysicalID())
	})
	return usedPartition, true, contentPos, nil
}

func buildNoRangeIndexReader(b *executorBuilder, v *plannercore.PhysicalIndexReader) (*IndexReaderExecutor, error) {
	dagReq, err := builder.ConstructDAGReq(b.ctx, v.IndexPlans, kv.TiKV)
	if err != nil {
		return nil, err
	}
	is := v.IndexPlans[0].(*plannercore.PhysicalIndexScan)
	tbl, _ := b.is.TableByID(context.Background(), is.Table.ID)
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
	paging := b.ctx.GetSessionVars().EnablePaging

	e := &IndexReaderExecutor{
		indexReaderExecutorContext: newIndexReaderExecutorContext(b.ctx),
		BaseExecutorV2:             exec.NewBaseExecutorV2(b.ctx.GetSessionVars(), v.Schema(), v.ID()),
		indexUsageReporter:         b.buildIndexUsageReporter(v),
		dagPB:                      dagReq,
		startTS:                    startTS,
		txnScope:                   b.txnScope,
		readReplicaScope:           b.readReplicaScope,
		isStaleness:                b.isStaleness,
		netDataSize:                v.GetNetDataSize(),
		physicalTableID:            physicalTableID,
		table:                      tbl,
		index:                      is.Index,
		keepOrder:                  is.KeepOrder,
		desc:                       is.Desc,
		columns:                    is.Columns,
		byItems:                    is.ByItems,
		paging:                     paging,
		corColInFilter:             b.corColInDistPlan(v.IndexPlans),
		corColInAccess:             b.corColInAccess(v.IndexPlans[0]),
		idxCols:                    is.IdxCols,
		colLens:                    is.IdxColLens,
		plans:                      v.IndexPlans,
		outputColumns:              v.OutputColumns,
	}

	for _, col := range v.OutputColumns {
		dagReq.OutputOffsets = append(dagReq.OutputOffsets, uint32(col.Index))
	}

	return e, nil
}

func (b *executorBuilder) buildIndexReader(v *plannercore.PhysicalIndexReader) exec.Executor {
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

	if !b.ctx.GetSessionVars().StmtCtx.UseDynamicPartitionPrune() {
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
		ret.partitionIDMap, err = getPartitionIDsAfterPruning(b.ctx, ret.table.(table.PartitionedTable), v.PlanPartInfo)
		if err != nil {
			b.err = err
			return nil
		}
		return ret
	}

	tmp, _ := b.is.TableByID(context.Background(), is.Table.ID)
	tbl := tmp.(table.PartitionedTable)
	partitions, err := partitionPruning(b.ctx, tbl, v.PlanPartInfo)
	if err != nil {
		b.err = err
		return nil
	}
	ret.partitions = partitions
	return ret
}

func buildTableReq(b *executorBuilder, schemaLen int, plans []base.PhysicalPlan) (dagReq *tipb.DAGRequest, val table.Table, err error) {
	tableReq, err := builder.ConstructDAGReq(b.ctx, plans, kv.TiKV)
	if err != nil {
		return nil, nil, err
	}
	for i := 0; i < schemaLen; i++ {
		tableReq.OutputOffsets = append(tableReq.OutputOffsets, uint32(i))
	}
	ts := plans[0].(*plannercore.PhysicalTableScan)
	tbl, _ := b.is.TableByID(context.Background(), ts.Table.ID)
	isPartition, physicalTableID := ts.IsPartition()
	if isPartition {
		pt := tbl.(table.PartitionedTable)
		tbl = pt.GetPartition(physicalTableID)
	}
	return tableReq, tbl, err
}

// buildIndexReq is designed to create a DAG for index request.
// If len(ByItems) != 0 means index request should return related columns
// to sort result rows in TiDB side for partition tables.
func buildIndexReq(ctx sessionctx.Context, columns []*model.IndexColumn, handleLen int, plans []base.PhysicalPlan) (dagReq *tipb.DAGRequest, err error) {
	indexReq, err := builder.ConstructDAGReq(ctx, plans, kv.TiKV)
	if err != nil {
		return nil, err
	}

	indexReq.OutputOffsets = []uint32{}
	idxScan := plans[0].(*plannercore.PhysicalIndexScan)
	if len(idxScan.ByItems) != 0 {
		schema := idxScan.Schema()
		for _, item := range idxScan.ByItems {
			c, ok := item.Expr.(*expression.Column)
			if !ok {
				return nil, errors.Errorf("Not support non-column in orderBy pushed down")
			}
			find := false
			for i, schemaColumn := range schema.Columns {
				if schemaColumn.ID == c.ID {
					indexReq.OutputOffsets = append(indexReq.OutputOffsets, uint32(i))
					find = true
					break
				}
			}
			if !find {
				return nil, errors.Errorf("Not found order by related columns in indexScan.schema")
			}
		}
	}

	for i := 0; i < handleLen; i++ {
		indexReq.OutputOffsets = append(indexReq.OutputOffsets, uint32(len(columns)+i))
	}

	if idxScan.NeedExtraOutputCol() {
		// need add one more column for pid or physical table id
		indexReq.OutputOffsets = append(indexReq.OutputOffsets, uint32(len(columns)+handleLen))
	}
	return indexReq, err
}

func buildNoRangeIndexLookUpReader(b *executorBuilder, v *plannercore.PhysicalIndexLookUpReader) (*IndexLookUpExecutor, error) {
	is := v.IndexPlans[0].(*plannercore.PhysicalIndexScan)
	var handleLen int
	if len(v.CommonHandleCols) != 0 {
		handleLen = len(v.CommonHandleCols)
	} else {
		handleLen = 1
	}
	indexReq, err := buildIndexReq(b.ctx, is.Index.Columns, handleLen, v.IndexPlans)
	if err != nil {
		return nil, err
	}
	indexPaging := false
	if v.Paging {
		indexPaging = true
	}
	tableReq, tbl, err := buildTableReq(b, v.Schema().Len(), v.TablePlans)
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
		indexLookUpExecutorContext: newIndexLookUpExecutorContext(b.ctx),
		BaseExecutorV2:             exec.NewBaseExecutorV2(b.ctx.GetSessionVars(), v.Schema(), v.ID()),
		indexUsageReporter:         b.buildIndexUsageReporter(v),
		dagPB:                      indexReq,
		startTS:                    startTS,
		table:                      tbl,
		index:                      is.Index,
		keepOrder:                  is.KeepOrder,
		byItems:                    is.ByItems,
		desc:                       is.Desc,
		tableRequest:               tableReq,
		columns:                    ts.Columns,
		indexPaging:                indexPaging,
		dataReaderBuilder:          readerBuilder,
		corColInIdxSide:            b.corColInDistPlan(v.IndexPlans),
		corColInTblSide:            b.corColInDistPlan(v.TablePlans),
		corColInAccess:             b.corColInAccess(v.IndexPlans[0]),
		idxCols:                    is.IdxCols,
		colLens:                    is.IdxColLens,
		idxPlans:                   v.IndexPlans,
		tblPlans:                   v.TablePlans,
		PushedLimit:                v.PushedLimit,
		idxNetDataSize:             v.GetAvgTableRowSize(),
		avgRowSize:                 v.GetAvgTableRowSize(),
	}

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

func (b *executorBuilder) buildIndexLookUpReader(v *plannercore.PhysicalIndexLookUpReader) exec.Executor {
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
	executor_metrics.ExecutorCounterIndexLookUpExecutor.Inc()

	sctx := b.ctx.GetSessionVars().StmtCtx
	sctx.IndexNames = append(sctx.IndexNames, is.Table.Name.O+":"+is.Index.Name.O)
	sctx.TableIDs = append(sctx.TableIDs, ts.Table.ID)

	if !b.ctx.GetSessionVars().StmtCtx.UseDynamicPartitionPrune() {
		return ret
	}

	if pi := is.Table.GetPartitionInfo(); pi == nil {
		return ret
	}

	if is.Index.Global {
		ret.partitionIDMap, err = getPartitionIDsAfterPruning(b.ctx, ret.table.(table.PartitionedTable), v.PlanPartInfo)
		if err != nil {
			b.err = err
			return nil
		}

		return ret
	}
	if ok, _ := is.IsPartition(); ok {
		// Already pruned when translated to logical union.
		return ret
	}

	tmp, _ := b.is.TableByID(context.Background(), is.Table.ID)
	tbl := tmp.(table.PartitionedTable)
	partitions, err := partitionPruning(b.ctx, tbl, v.PlanPartInfo)
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
	partialDataSizes := make([]float64, 0, partialPlanCount)
	indexes := make([]*model.IndexInfo, 0, partialPlanCount)
	descs := make([]bool, 0, partialPlanCount)
	ts := v.TablePlans[0].(*plannercore.PhysicalTableScan)
	isCorColInPartialFilters := make([]bool, 0, partialPlanCount)
	isCorColInPartialAccess := make([]bool, 0, partialPlanCount)
	hasGlobalIndex := false
	for i := 0; i < partialPlanCount; i++ {
		var tempReq *tipb.DAGRequest
		var err error

		if is, ok := v.PartialPlans[i][0].(*plannercore.PhysicalIndexScan); ok {
			tempReq, err = buildIndexReq(b.ctx, is.Index.Columns, ts.HandleCols.NumCols(), v.PartialPlans[i])
			descs = append(descs, is.Desc)
			indexes = append(indexes, is.Index)
			if is.Index.Global {
				hasGlobalIndex = true
			}
		} else {
			ts := v.PartialPlans[i][0].(*plannercore.PhysicalTableScan)
			tempReq, _, err = buildTableReq(b, len(ts.Columns), v.PartialPlans[i])
			descs = append(descs, ts.Desc)
			indexes = append(indexes, nil)
		}
		if err != nil {
			return nil, err
		}
		collect := false
		tempReq.CollectRangeCounts = &collect
		partialReqs = append(partialReqs, tempReq)
		isCorColInPartialFilters = append(isCorColInPartialFilters, b.corColInDistPlan(v.PartialPlans[i]))
		isCorColInPartialAccess = append(isCorColInPartialAccess, b.corColInAccess(v.PartialPlans[i][0]))
		partialDataSizes = append(partialDataSizes, v.GetPartialReaderNetDataSize(v.PartialPlans[i][0]))
	}
	tableReq, tblInfo, err := buildTableReq(b, v.Schema().Len(), v.TablePlans)
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
		BaseExecutor:             exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
		indexUsageReporter:       b.buildIndexUsageReporter(v),
		dagPBs:                   partialReqs,
		startTS:                  startTS,
		table:                    tblInfo,
		indexes:                  indexes,
		descs:                    descs,
		tableRequest:             tableReq,
		columns:                  ts.Columns,
		partialPlans:             v.PartialPlans,
		tblPlans:                 v.TablePlans,
		partialNetDataSizes:      partialDataSizes,
		dataAvgRowSize:           v.GetAvgTableRowSize(),
		dataReaderBuilder:        readerBuilder,
		handleCols:               v.HandleCols,
		isCorColInPartialFilters: isCorColInPartialFilters,
		isCorColInTableFilter:    isCorColInTableFilter,
		isCorColInPartialAccess:  isCorColInPartialAccess,
		isIntersection:           v.IsIntersectionType,
		byItems:                  v.ByItems,
		pushedLimit:              v.PushedLimit,
		keepOrder:                v.KeepOrder,
		hasGlobalIndex:           hasGlobalIndex,
	}
	collectTable := false
	e.tableRequest.CollectRangeCounts = &collectTable
	return e, nil
}

type tableStatsPreloader interface {
	LoadTableStats(sessionctx.Context)
}

func buildIndexUsageReporter(ctx sessionctx.Context, plan tableStatsPreloader) (indexUsageReporter *exec.IndexUsageReporter) {
	sc := ctx.GetSessionVars().StmtCtx
	if ctx.GetSessionVars().StmtCtx.IndexUsageCollector != nil &&
		sc.RuntimeStatsColl != nil {
		// Preload the table stats. If the statement is a point-get or execute, the planner may not have loaded the
		// stats.
		plan.LoadTableStats(ctx)

		statsMap := sc.GetUsedStatsInfo(false)
		indexUsageReporter = exec.NewIndexUsageReporter(
			sc.IndexUsageCollector,
			sc.RuntimeStatsColl, statsMap)
	}

	return indexUsageReporter
}

func (b *executorBuilder) buildIndexUsageReporter(plan tableStatsPreloader) (indexUsageReporter *exec.IndexUsageReporter) {
	return buildIndexUsageReporter(b.ctx, plan)
}

func (b *executorBuilder) buildIndexMergeReader(v *plannercore.PhysicalIndexMergeReader) exec.Executor {
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
	hasGlobalIndex := false
	for i := 0; i < len(v.PartialPlans); i++ {
		if is, ok := v.PartialPlans[i][0].(*plannercore.PhysicalIndexScan); ok {
			ret.ranges = append(ret.ranges, is.Ranges)
			sctx.IndexNames = append(sctx.IndexNames, is.Table.Name.O+":"+is.Index.Name.O)
			if is.Index.Global {
				hasGlobalIndex = true
			}
		} else {
			ret.ranges = append(ret.ranges, v.PartialPlans[i][0].(*plannercore.PhysicalTableScan).Ranges)
			if ret.table.Meta().IsCommonHandle {
				tblInfo := ret.table.Meta()
				sctx.IndexNames = append(sctx.IndexNames, tblInfo.Name.O+":"+tables.FindPrimaryIndex(tblInfo).Name.O)
			}
		}
	}
	sctx.TableIDs = append(sctx.TableIDs, ts.Table.ID)
	executor_metrics.ExecutorCounterIndexMergeReaderExecutor.Inc()

	if !b.ctx.GetSessionVars().StmtCtx.UseDynamicPartitionPrune() {
		return ret
	}

	if pi := ts.Table.GetPartitionInfo(); pi == nil {
		return ret
	}

	tmp, _ := b.is.TableByID(context.Background(), ts.Table.ID)
	partitions, err := partitionPruning(b.ctx, tmp.(table.PartitionedTable), v.PlanPartInfo)
	if err != nil {
		b.err = err
		return nil
	}
	ret.partitionTableMode, ret.prunedPartitions = true, partitions
	if hasGlobalIndex {
		ret.partitionIDMap = make(map[int64]struct{})
		for _, p := range partitions {
			ret.partitionIDMap[p.GetPhysicalID()] = struct{}{}
		}
	}
	return ret
}

// dataReaderBuilder build an executor.
// The executor can be used to read data in the ranges which are constructed by datums.
// Differences from executorBuilder:
// 1. dataReaderBuilder calculate data range from argument, rather than plan.
// 2. the result executor is already opened.
type dataReaderBuilder struct {
	plan base.Plan
	*executorBuilder

	selectResultHook // for testing
	once             struct {
		sync.Once
		condPruneResult []table.PhysicalTable
		err             error
	}
}

type mockPhysicalIndexReader struct {
	base.PhysicalPlan

	e exec.Executor
}

// MemoryUsage of mockPhysicalIndexReader is only for testing
func (*mockPhysicalIndexReader) MemoryUsage() (sum int64) {
	return
}

func (builder *dataReaderBuilder) BuildExecutorForIndexJoin(ctx context.Context, lookUpContents []*join.IndexJoinLookUpContent,
	indexRanges []*ranger.Range, keyOff2IdxOff []int, cwc *plannercore.ColWithCmpFuncManager, canReorderHandles bool, memTracker *memory.Tracker, interruptSignal *atomic.Value) (exec.Executor, error) {
	return builder.buildExecutorForIndexJoinInternal(ctx, builder.plan, lookUpContents, indexRanges, keyOff2IdxOff, cwc, canReorderHandles, memTracker, interruptSignal)
}

func (builder *dataReaderBuilder) buildExecutorForIndexJoinInternal(ctx context.Context, plan base.Plan, lookUpContents []*join.IndexJoinLookUpContent,
	indexRanges []*ranger.Range, keyOff2IdxOff []int, cwc *plannercore.ColWithCmpFuncManager, canReorderHandles bool, memTracker *memory.Tracker, interruptSignal *atomic.Value) (exec.Executor, error) {
	switch v := plan.(type) {
	case *plannercore.PhysicalTableReader:
		return builder.buildTableReaderForIndexJoin(ctx, v, lookUpContents, indexRanges, keyOff2IdxOff, cwc, canReorderHandles, memTracker, interruptSignal)
	case *plannercore.PhysicalIndexReader:
		return builder.buildIndexReaderForIndexJoin(ctx, v, lookUpContents, indexRanges, keyOff2IdxOff, cwc, memTracker, interruptSignal)
	case *plannercore.PhysicalIndexLookUpReader:
		return builder.buildIndexLookUpReaderForIndexJoin(ctx, v, lookUpContents, indexRanges, keyOff2IdxOff, cwc, memTracker, interruptSignal)
	case *plannercore.PhysicalUnionScan:
		return builder.buildUnionScanForIndexJoin(ctx, v, lookUpContents, indexRanges, keyOff2IdxOff, cwc, canReorderHandles, memTracker, interruptSignal)
	case *plannercore.PhysicalProjection:
		return builder.buildProjectionForIndexJoin(ctx, v, lookUpContents, indexRanges, keyOff2IdxOff, cwc, canReorderHandles, memTracker, interruptSignal)
	// Need to support physical selection because after PR 16389, TiDB will push down all the expr supported by TiKV or TiFlash
	// in predicate push down stage, so if there is an expr which only supported by TiFlash, a physical selection will be added after index read
	case *plannercore.PhysicalSelection:
		childExec, err := builder.buildExecutorForIndexJoinInternal(ctx, v.Children()[0], lookUpContents, indexRanges, keyOff2IdxOff, cwc, canReorderHandles, memTracker, interruptSignal)
		if err != nil {
			return nil, err
		}
		exec := &SelectionExec{
			selectionExecutorContext: newSelectionExecutorContext(builder.ctx),
			BaseExecutorV2:           exec.NewBaseExecutorV2(builder.ctx.GetSessionVars(), v.Schema(), v.ID(), childExec),
			filters:                  v.Conditions,
		}
		err = exec.open(ctx)
		return exec, err
	case *plannercore.PhysicalHashAgg:
		childExec, err := builder.buildExecutorForIndexJoinInternal(ctx, v.Children()[0], lookUpContents, indexRanges, keyOff2IdxOff, cwc, canReorderHandles, memTracker, interruptSignal)
		if err != nil {
			return nil, err
		}
		exec := builder.buildHashAggFromChildExec(childExec, v)
		err = exec.OpenSelf()
		return exec, err
	case *plannercore.PhysicalStreamAgg:
		childExec, err := builder.buildExecutorForIndexJoinInternal(ctx, v.Children()[0], lookUpContents, indexRanges, keyOff2IdxOff, cwc, canReorderHandles, memTracker, interruptSignal)
		if err != nil {
			return nil, err
		}
		exec := builder.buildStreamAggFromChildExec(childExec, v)
		err = exec.OpenSelf()
		return exec, err
	case *mockPhysicalIndexReader:
		return v.e, nil
	}
	return nil, errors.New("Wrong plan type for dataReaderBuilder")
}

func (builder *dataReaderBuilder) buildUnionScanForIndexJoin(ctx context.Context, v *plannercore.PhysicalUnionScan,
	values []*join.IndexJoinLookUpContent, indexRanges []*ranger.Range, keyOff2IdxOff []int,
	cwc *plannercore.ColWithCmpFuncManager, canReorderHandles bool, memTracker *memory.Tracker, interruptSignal *atomic.Value) (exec.Executor, error) {
	childBuilder, err := builder.newDataReaderBuilder(v.Children()[0])
	if err != nil {
		return nil, err
	}

	reader, err := childBuilder.BuildExecutorForIndexJoin(ctx, values, indexRanges, keyOff2IdxOff, cwc, canReorderHandles, memTracker, interruptSignal)
	if err != nil {
		return nil, err
	}

	ret := builder.buildUnionScanFromReader(reader, v)
	if builder.err != nil {
		return nil, builder.err
	}
	if us, ok := ret.(*UnionScanExec); ok {
		err = us.open(ctx)
	}
	return ret, err
}

func (builder *dataReaderBuilder) buildTableReaderForIndexJoin(ctx context.Context, v *plannercore.PhysicalTableReader,
	lookUpContents []*join.IndexJoinLookUpContent, indexRanges []*ranger.Range, keyOff2IdxOff []int,
	cwc *plannercore.ColWithCmpFuncManager, canReorderHandles bool, memTracker *memory.Tracker, interruptSignal *atomic.Value) (exec.Executor, error) {
	e, err := buildNoRangeTableReader(builder.executorBuilder, v)
	if !canReorderHandles {
		// `canReorderHandles` is set to false only in IndexMergeJoin. IndexMergeJoin will trigger a dead loop problem
		// when enabling paging(tidb/issues/35831). But IndexMergeJoin is not visible to the user and is deprecated
		// for now. Thus, we disable paging here.
		e.paging = false
	}
	if err != nil {
		return nil, err
	}
	tbInfo := e.table.Meta()
	if tbInfo.GetPartitionInfo() == nil || !builder.ctx.GetSessionVars().StmtCtx.UseDynamicPartitionPrune() {
		if v.IsCommonHandle {
			kvRanges, err := buildKvRangesForIndexJoin(e.dctx, e.rctx, getPhysicalTableID(e.table), -1, lookUpContents, indexRanges, keyOff2IdxOff, cwc, memTracker, interruptSignal)
			if err != nil {
				return nil, err
			}
			return builder.buildTableReaderFromKvRanges(ctx, e, kvRanges)
		}
		handles, _ := dedupHandles(lookUpContents)
		return builder.buildTableReaderFromHandles(ctx, e, handles, canReorderHandles)
	}
	tbl, _ := builder.is.TableByID(ctx, tbInfo.ID)
	pt := tbl.(table.PartitionedTable)
	usedPartitionList, err := builder.partitionPruning(pt, v.PlanPartInfo)
	if err != nil {
		return nil, err
	}
	usedPartitions := make(map[int64]table.PhysicalTable, len(usedPartitionList))
	for _, p := range usedPartitionList {
		usedPartitions[p.GetPhysicalID()] = p
	}
	var kvRanges []kv.KeyRange
	var keyColOffsets []int
	if len(lookUpContents) > 0 {
		keyColOffsets = getPartitionKeyColOffsets(lookUpContents[0].KeyColIDs, pt)
	}
	if v.IsCommonHandle {
		if len(keyColOffsets) > 0 {
			locateKey := make([]types.Datum, len(pt.Cols()))
			kvRanges = make([]kv.KeyRange, 0, len(lookUpContents))
			// lookUpContentsByPID groups lookUpContents by pid(partition) so that kv ranges for same partition can be merged.
			lookUpContentsByPID := make(map[int64][]*join.IndexJoinLookUpContent)
			exprCtx := e.ectx
			for _, content := range lookUpContents {
				for i, data := range content.Keys {
					locateKey[keyColOffsets[i]] = data
				}
				p, err := pt.GetPartitionByRow(exprCtx.GetEvalCtx(), locateKey)
				if table.ErrNoPartitionForGivenValue.Equal(err) {
					continue
				}
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
				tmp, err := buildKvRangesForIndexJoin(e.dctx, e.rctx, pid, -1, contents, indexRanges, keyOff2IdxOff, cwc, nil, interruptSignal)
				if err != nil {
					return nil, err
				}
				kvRanges = append(kvRanges, tmp...)
			}
		} else {
			kvRanges = make([]kv.KeyRange, 0, len(usedPartitions)*len(lookUpContents))
			for _, p := range usedPartitionList {
				tmp, err := buildKvRangesForIndexJoin(e.dctx, e.rctx, p.GetPhysicalID(), -1, lookUpContents, indexRanges, keyOff2IdxOff, cwc, memTracker, interruptSignal)
				if err != nil {
					return nil, err
				}
				kvRanges = append(tmp, kvRanges...)
			}
		}
		// The key ranges should be ordered.
		slices.SortFunc(kvRanges, func(i, j kv.KeyRange) int {
			return bytes.Compare(i.StartKey, j.StartKey)
		})
		return builder.buildTableReaderFromKvRanges(ctx, e, kvRanges)
	}

	handles, lookUpContents := dedupHandles(lookUpContents)

	if len(keyColOffsets) > 0 {
		locateKey := make([]types.Datum, len(pt.Cols()))
		kvRanges = make([]kv.KeyRange, 0, len(lookUpContents))
		exprCtx := e.ectx
		for _, content := range lookUpContents {
			for i, data := range content.Keys {
				locateKey[keyColOffsets[i]] = data
			}
			p, err := pt.GetPartitionByRow(exprCtx.GetEvalCtx(), locateKey)
			if table.ErrNoPartitionForGivenValue.Equal(err) {
				continue
			}
			if err != nil {
				return nil, err
			}
			pid := p.GetPhysicalID()
			if _, ok := usedPartitions[pid]; !ok {
				continue
			}
			handle := kv.IntHandle(content.Keys[0].GetInt64())
			ranges, _ := distsql.TableHandlesToKVRanges(pid, []kv.Handle{handle})
			kvRanges = append(kvRanges, ranges...)
		}
	} else {
		for _, p := range usedPartitionList {
			ranges, _ := distsql.TableHandlesToKVRanges(p.GetPhysicalID(), handles)
			kvRanges = append(kvRanges, ranges...)
		}
	}

	// The key ranges should be ordered.
	slices.SortFunc(kvRanges, func(i, j kv.KeyRange) int {
		return bytes.Compare(i.StartKey, j.StartKey)
	})
	return builder.buildTableReaderFromKvRanges(ctx, e, kvRanges)
}

func dedupHandles(lookUpContents []*join.IndexJoinLookUpContent) ([]kv.Handle, []*join.IndexJoinLookUpContent) {
	handles := make([]kv.Handle, 0, len(lookUpContents))
	validLookUpContents := make([]*join.IndexJoinLookUpContent, 0, len(lookUpContents))
	for _, content := range lookUpContents {
		isValidHandle := true
		handle := kv.IntHandle(content.Keys[0].GetInt64())
		for _, key := range content.Keys {
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
	partitions []table.PhysicalTable
}

func (h kvRangeBuilderFromRangeAndPartition) buildKeyRangeSeparately(dctx *distsqlctx.DistSQLContext, ranges []*ranger.Range) ([]int64, [][]kv.KeyRange, error) {
	ret := make([][]kv.KeyRange, len(h.partitions))
	pids := make([]int64, 0, len(h.partitions))
	for i, p := range h.partitions {
		pid := p.GetPhysicalID()
		pids = append(pids, pid)
		meta := p.Meta()
		if len(ranges) == 0 {
			continue
		}
		kvRange, err := distsql.TableHandleRangesToKVRanges(dctx, []int64{pid}, meta != nil && meta.IsCommonHandle, ranges)
		if err != nil {
			return nil, nil, err
		}
		ret[i] = kvRange.AppendSelfTo(ret[i])
	}
	return pids, ret, nil
}

func (h kvRangeBuilderFromRangeAndPartition) buildKeyRange(dctx *distsqlctx.DistSQLContext, ranges []*ranger.Range) ([][]kv.KeyRange, error) {
	ret := make([][]kv.KeyRange, len(h.partitions))
	if len(ranges) == 0 {
		return ret, nil
	}
	for i, p := range h.partitions {
		pid := p.GetPhysicalID()
		meta := p.Meta()
		kvRange, err := distsql.TableHandleRangesToKVRanges(dctx, []int64{pid}, meta != nil && meta.IsCommonHandle, ranges)
		if err != nil {
			return nil, err
		}
		ret[i] = kvRange.AppendSelfTo(ret[i])
	}
	return ret, nil
}

// newClosestReadAdjuster let the request be sent to closest replica(within the same zone)
// if response size exceeds certain threshold.
func newClosestReadAdjuster(dctx *distsqlctx.DistSQLContext, req *kv.Request, netDataSize float64) kv.CoprRequestAdjuster {
	if req.ReplicaRead != kv.ReplicaReadClosestAdaptive {
		return nil
	}
	return func(req *kv.Request, copTaskCount int) bool {
		// copTaskCount is the number of coprocessor requests
		if int64(netDataSize/float64(copTaskCount)) >= dctx.ReplicaClosestReadThreshold {
			req.MatchStoreLabels = append(req.MatchStoreLabels, &metapb.StoreLabel{
				Key:   placement.DCLabelKey,
				Value: config.GetTxnScopeFromConfig(),
			})
			return true
		}
		// reset to read from leader when the data size is small.
		req.ReplicaRead = kv.ReplicaReadLeader
		return false
	}
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
		SetTxnScope(e.txnScope).
		SetReadReplicaScope(e.readReplicaScope).
		SetIsStaleness(e.isStaleness).
		SetFromSessionVars(e.dctx).
		SetFromInfoSchema(e.GetInfoSchema()).
		SetClosestReplicaReadAdjuster(newClosestReadAdjuster(e.dctx, &reqBuilderWithRange.Request, e.netDataSize)).
		SetPaging(e.paging).
		SetConnIDAndConnAlias(e.dctx.ConnectionID, e.dctx.SessionAlias).
		Build()
	if err != nil {
		return nil, err
	}
	e.kvRanges = kvReq.KeyRanges.AppendSelfTo(e.kvRanges)
	e.resultHandler = &tableResultHandler{}
	result, err := builder.SelectResult(ctx, builder.ctx.GetDistSQLCtx(), kvReq, exec.RetTypes(e), getPhysicalPlanIDs(e.plans), e.ID())
	if err != nil {
		return nil, err
	}
	e.resultHandler.open(nil, result)
	return e, nil
}

func (builder *dataReaderBuilder) buildTableReaderFromHandles(ctx context.Context, e *TableReaderExecutor, handles []kv.Handle, canReorderHandles bool) (*TableReaderExecutor, error) {
	if canReorderHandles {
		slices.SortFunc(handles, func(i, j kv.Handle) int {
			return i.Compare(j)
		})
	}
	var b distsql.RequestBuilder
	if len(handles) > 0 {
		if _, ok := handles[0].(kv.PartitionHandle); ok {
			b.SetPartitionsAndHandles(handles)
		} else {
			b.SetTableHandles(getPhysicalTableID(e.table), handles)
		}
	} else {
		b.SetKeyRanges(nil)
	}
	return builder.buildTableReaderBase(ctx, e, b)
}

func (builder *dataReaderBuilder) buildTableReaderFromKvRanges(ctx context.Context, e *TableReaderExecutor, ranges []kv.KeyRange) (exec.Executor, error) {
	var b distsql.RequestBuilder
	b.SetKeyRanges(ranges)
	return builder.buildTableReaderBase(ctx, e, b)
}

func (builder *dataReaderBuilder) buildIndexReaderForIndexJoin(ctx context.Context, v *plannercore.PhysicalIndexReader,
	lookUpContents []*join.IndexJoinLookUpContent, indexRanges []*ranger.Range, keyOff2IdxOff []int, cwc *plannercore.ColWithCmpFuncManager, memoryTracker *memory.Tracker, interruptSignal *atomic.Value) (exec.Executor, error) {
	e, err := buildNoRangeIndexReader(builder.executorBuilder, v)
	if err != nil {
		return nil, err
	}
	tbInfo := e.table.Meta()
	if tbInfo.GetPartitionInfo() == nil || !builder.ctx.GetSessionVars().StmtCtx.UseDynamicPartitionPrune() {
		kvRanges, err := buildKvRangesForIndexJoin(e.dctx, e.rctx, e.physicalTableID, e.index.ID, lookUpContents, indexRanges, keyOff2IdxOff, cwc, memoryTracker, interruptSignal)
		if err != nil {
			return nil, err
		}
		err = e.open(ctx, kvRanges)
		return e, err
	}

	is := v.IndexPlans[0].(*plannercore.PhysicalIndexScan)
	if is.Index.Global {
		e.partitionIDMap, err = getPartitionIDsAfterPruning(builder.ctx, e.table.(table.PartitionedTable), v.PlanPartInfo)
		if err != nil {
			return nil, err
		}
		if e.ranges, err = buildRangesForIndexJoin(e.rctx, lookUpContents, indexRanges, keyOff2IdxOff, cwc); err != nil {
			return nil, err
		}
		if err := exec.Open(ctx, e); err != nil {
			return nil, err
		}
		return e, nil
	}

	tbl, _ := builder.executorBuilder.is.TableByID(ctx, tbInfo.ID)
	usedPartition, canPrune, contentPos, err := builder.prunePartitionForInnerExecutor(tbl, v.PlanPartInfo, lookUpContents)
	if err != nil {
		return nil, err
	}
	if len(usedPartition) != 0 {
		if canPrune {
			rangeMap, err := buildIndexRangeForEachPartition(e.rctx, usedPartition, contentPos, lookUpContents, indexRanges, keyOff2IdxOff, cwc)
			if err != nil {
				return nil, err
			}
			e.partitions = usedPartition
			e.ranges = indexRanges
			e.partRangeMap = rangeMap
		} else {
			e.partitions = usedPartition
			if e.ranges, err = buildRangesForIndexJoin(e.rctx, lookUpContents, indexRanges, keyOff2IdxOff, cwc); err != nil {
				return nil, err
			}
		}
		if err := exec.Open(ctx, e); err != nil {
			return nil, err
		}
		return e, nil
	}
	ret := &TableDualExec{BaseExecutorV2: e.BaseExecutorV2}
	err = exec.Open(ctx, ret)
	return ret, err
}

func (builder *dataReaderBuilder) buildIndexLookUpReaderForIndexJoin(ctx context.Context, v *plannercore.PhysicalIndexLookUpReader,
	lookUpContents []*join.IndexJoinLookUpContent, indexRanges []*ranger.Range, keyOff2IdxOff []int, cwc *plannercore.ColWithCmpFuncManager, memTracker *memory.Tracker, interruptSignal *atomic.Value) (exec.Executor, error) {
	e, err := buildNoRangeIndexLookUpReader(builder.executorBuilder, v)
	if err != nil {
		return nil, err
	}

	tbInfo := e.table.Meta()
	if tbInfo.GetPartitionInfo() == nil || !builder.ctx.GetSessionVars().StmtCtx.UseDynamicPartitionPrune() {
		e.kvRanges, err = buildKvRangesForIndexJoin(e.dctx, e.rctx, getPhysicalTableID(e.table), e.index.ID, lookUpContents, indexRanges, keyOff2IdxOff, cwc, memTracker, interruptSignal)
		if err != nil {
			return nil, err
		}
		err = e.open(ctx)
		return e, err
	}

	is := v.IndexPlans[0].(*plannercore.PhysicalIndexScan)
	if is.Index.Global {
		e.partitionIDMap, err = getPartitionIDsAfterPruning(builder.ctx, e.table.(table.PartitionedTable), v.PlanPartInfo)
		if err != nil {
			return nil, err
		}
		e.ranges, err = buildRangesForIndexJoin(e.rctx, lookUpContents, indexRanges, keyOff2IdxOff, cwc)
		if err != nil {
			return nil, err
		}
		if err := exec.Open(ctx, e); err != nil {
			return nil, err
		}
		return e, err
	}

	tbl, _ := builder.executorBuilder.is.TableByID(ctx, tbInfo.ID)
	usedPartition, canPrune, contentPos, err := builder.prunePartitionForInnerExecutor(tbl, v.PlanPartInfo, lookUpContents)
	if err != nil {
		return nil, err
	}
	if len(usedPartition) != 0 {
		if canPrune {
			rangeMap, err := buildIndexRangeForEachPartition(e.rctx, usedPartition, contentPos, lookUpContents, indexRanges, keyOff2IdxOff, cwc)
			if err != nil {
				return nil, err
			}
			e.prunedPartitions = usedPartition
			e.ranges = indexRanges
			e.partitionRangeMap = rangeMap
		} else {
			e.prunedPartitions = usedPartition
			e.ranges, err = buildRangesForIndexJoin(e.rctx, lookUpContents, indexRanges, keyOff2IdxOff, cwc)
			if err != nil {
				return nil, err
			}
		}
		e.partitionTableMode = true
		if err := exec.Open(ctx, e); err != nil {
			return nil, err
		}
		return e, err
	}
	ret := &TableDualExec{BaseExecutorV2: e.BaseExecutorV2}
	err = exec.Open(ctx, ret)
	return ret, err
}

func (builder *dataReaderBuilder) buildProjectionForIndexJoin(
	ctx context.Context,
	v *plannercore.PhysicalProjection,
	lookUpContents []*join.IndexJoinLookUpContent,
	indexRanges []*ranger.Range,
	keyOff2IdxOff []int,
	cwc *plannercore.ColWithCmpFuncManager,
	canReorderHandles bool,
	memTracker *memory.Tracker,
	interruptSignal *atomic.Value,
) (executor exec.Executor, err error) {
	var childExec exec.Executor
	childExec, err = builder.buildExecutorForIndexJoinInternal(ctx, v.Children()[0], lookUpContents, indexRanges, keyOff2IdxOff, cwc, canReorderHandles, memTracker, interruptSignal)
	if err != nil {
		return nil, err
	}
	defer func() {
		if r := recover(); r != nil {
			err = util.GetRecoverError(r)
		}
		if err != nil {
			terror.Log(exec.Close(childExec))
		}
	}()

	e := &ProjectionExec{
		projectionExecutorContext: newProjectionExecutorContext(builder.ctx),
		BaseExecutorV2:            exec.NewBaseExecutorV2(builder.ctx.GetSessionVars(), v.Schema(), v.ID(), childExec),
		numWorkers:                int64(builder.ctx.GetSessionVars().ProjectionConcurrency()),
		evaluatorSuit:             expression.NewEvaluatorSuite(v.Exprs, v.AvoidColumnEvaluator),
		calculateNoDelay:          v.CalculateNoDelay,
	}

	// If the calculation row count for this Projection operator is smaller
	// than a Chunk size, we turn back to the un-parallel Projection
	// implementation to reduce the goroutine overhead.
	if int64(v.StatsCount()) < int64(builder.ctx.GetSessionVars().MaxChunkSize) {
		e.numWorkers = 0
	}
	failpoint.Inject("buildProjectionForIndexJoinPanic", func(val failpoint.Value) {
		if v, ok := val.(bool); ok && v {
			panic("buildProjectionForIndexJoinPanic")
		}
	})
	err = e.open(ctx)
	if err != nil {
		return nil, err
	}
	return e, nil
}

// buildRangesForIndexJoin builds kv ranges for index join when the inner plan is index scan plan.
func buildRangesForIndexJoin(rctx *rangerctx.RangerContext, lookUpContents []*join.IndexJoinLookUpContent,
	ranges []*ranger.Range, keyOff2IdxOff []int, cwc *plannercore.ColWithCmpFuncManager) ([]*ranger.Range, error) {
	retRanges := make([]*ranger.Range, 0, len(ranges)*len(lookUpContents))
	lastPos := len(ranges[0].LowVal) - 1
	tmpDatumRanges := make([]*ranger.Range, 0, len(lookUpContents))
	for _, content := range lookUpContents {
		for _, ran := range ranges {
			for keyOff, idxOff := range keyOff2IdxOff {
				ran.LowVal[idxOff] = content.Keys[keyOff]
				ran.HighVal[idxOff] = content.Keys[keyOff]
			}
		}
		if cwc == nil {
			// A deep copy is need here because the old []*range.Range is overwritten
			for _, ran := range ranges {
				retRanges = append(retRanges, ran.Clone())
			}
			continue
		}
		nextColRanges, err := cwc.BuildRangesByRow(rctx, content.Row)
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

	return ranger.UnionRanges(rctx, tmpDatumRanges, true)
}

// buildKvRangesForIndexJoin builds kv ranges for index join when the inner plan is index scan plan.
func buildKvRangesForIndexJoin(dctx *distsqlctx.DistSQLContext, pctx *rangerctx.RangerContext, tableID, indexID int64, lookUpContents []*join.IndexJoinLookUpContent,
	ranges []*ranger.Range, keyOff2IdxOff []int, cwc *plannercore.ColWithCmpFuncManager, memTracker *memory.Tracker, interruptSignal *atomic.Value) (_ []kv.KeyRange, err error) {
	kvRanges := make([]kv.KeyRange, 0, len(ranges)*len(lookUpContents))
	if len(ranges) == 0 {
		return []kv.KeyRange{}, nil
	}
	lastPos := len(ranges[0].LowVal) - 1
	tmpDatumRanges := make([]*ranger.Range, 0, len(lookUpContents))
	for _, content := range lookUpContents {
		for _, ran := range ranges {
			for keyOff, idxOff := range keyOff2IdxOff {
				ran.LowVal[idxOff] = content.Keys[keyOff]
				ran.HighVal[idxOff] = content.Keys[keyOff]
			}
		}
		if cwc == nil {
			// Index id is -1 means it's a common handle.
			var tmpKvRanges *kv.KeyRanges
			var err error
			if indexID == -1 {
				tmpKvRanges, err = distsql.CommonHandleRangesToKVRanges(dctx, []int64{tableID}, ranges)
			} else {
				tmpKvRanges, err = distsql.IndexRangesToKVRangesWithInterruptSignal(dctx, tableID, indexID, ranges, memTracker, interruptSignal)
			}
			if err != nil {
				return nil, err
			}
			kvRanges = tmpKvRanges.AppendSelfTo(kvRanges)
			continue
		}
		nextColRanges, err := cwc.BuildRangesByRow(pctx, content.Row)
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
		failpoint.Inject("testIssue49033", func() {
			panic("testIssue49033")
		})
		memTracker.Consume(int64(2 * cap(kvRanges[0].StartKey) * len(kvRanges)))
	}
	if len(tmpDatumRanges) != 0 && memTracker != nil {
		memTracker.Consume(2 * types.EstimatedMemUsage(tmpDatumRanges[0].LowVal, len(tmpDatumRanges)))
	}
	if cwc == nil {
		slices.SortFunc(kvRanges, func(i, j kv.KeyRange) int {
			return bytes.Compare(i.StartKey, j.StartKey)
		})
		return kvRanges, nil
	}

	tmpDatumRanges, err = ranger.UnionRanges(pctx, tmpDatumRanges, true)
	if err != nil {
		return nil, err
	}
	// Index id is -1 means it's a common handle.
	if indexID == -1 {
		tmpKeyRanges, err := distsql.CommonHandleRangesToKVRanges(dctx, []int64{tableID}, tmpDatumRanges)
		return tmpKeyRanges.FirstPartitionRange(), err
	}
	tmpKeyRanges, err := distsql.IndexRangesToKVRangesWithInterruptSignal(dctx, tableID, indexID, tmpDatumRanges, memTracker, interruptSignal)
	return tmpKeyRanges.FirstPartitionRange(), err
}

func (b *executorBuilder) buildWindow(v *plannercore.PhysicalWindow) exec.Executor {
	childExec := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	base := exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID(), childExec)
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
	exprCtx := b.ctx.GetExprCtx()
	for _, desc := range v.WindowFuncDescs {
		aggDesc, err := aggregation.NewAggFuncDescForWindowFunc(exprCtx, desc, false)
		if err != nil {
			b.err = err
			return nil
		}
		agg := aggfuncs.BuildWindowFunctions(exprCtx, aggDesc, resultColIdx, orderByCols)
		windowFuncs = append(windowFuncs, agg)
		partialResult, _ := agg.AllocPartialResult()
		partialResults = append(partialResults, partialResult)
		resultColIdx++
	}

	var err error
	if b.ctx.GetSessionVars().EnablePipelinedWindowExec {
		exec := &PipelinedWindowExec{
			BaseExecutor:   base,
			groupChecker:   vecgroupchecker.NewVecGroupChecker(b.ctx.GetExprCtx().GetEvalCtx(), b.ctx.GetSessionVars().EnableVectorizedExpression, groupByItems),
			numWindowFuncs: len(v.WindowFuncDescs),
			windowFuncs:    windowFuncs,
			partialResults: partialResults,
		}
		exec.slidingWindowFuncs = make([]aggfuncs.SlidingWindowAggFunc, len(exec.windowFuncs))
		for i, windowFunc := range exec.windowFuncs {
			if slidingWindowAggFunc, ok := windowFunc.(aggfuncs.SlidingWindowAggFunc); ok {
				exec.slidingWindowFuncs[i] = slidingWindowAggFunc
			}
		}
		if v.Frame == nil {
			exec.start = &logicalop.FrameBound{
				Type:      ast.Preceding,
				UnBounded: true,
			}
			exec.end = &logicalop.FrameBound{
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
				err = exec.start.UpdateCompareCols(b.ctx, exec.orderByCols)
				if err != nil {
					return nil
				}
				err = exec.end.UpdateCompareCols(b.ctx, exec.orderByCols)
				if err != nil {
					return nil
				}
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
		tmpProcessor := &rangeFrameWindowProcessor{
			windowFuncs:       windowFuncs,
			partialResults:    partialResults,
			start:             v.Frame.Start,
			end:               v.Frame.End,
			orderByCols:       orderByCols,
			expectedCmpResult: cmpResult,
		}

		err = tmpProcessor.start.UpdateCompareCols(b.ctx, orderByCols)
		if err != nil {
			return nil
		}
		err = tmpProcessor.end.UpdateCompareCols(b.ctx, orderByCols)
		if err != nil {
			return nil
		}

		processor = tmpProcessor
	}
	return &WindowExec{BaseExecutor: base,
		processor:      processor,
		groupChecker:   vecgroupchecker.NewVecGroupChecker(b.ctx.GetExprCtx().GetEvalCtx(), b.ctx.GetSessionVars().EnableVectorizedExpression, groupByItems),
		numWindowFuncs: len(v.WindowFuncDescs),
	}
}

func (b *executorBuilder) buildShuffle(v *plannercore.PhysicalShuffle) *ShuffleExec {
	base := exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID())
	shuffle := &ShuffleExec{
		BaseExecutor: base,
		concurrency:  v.Concurrency,
	}

	// 1. initialize the splitters
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

	// 2. initialize the data sources (build the data sources from physical plan to executors)
	shuffle.dataSources = make([]exec.Executor, len(v.DataSources))
	for i, dataSource := range v.DataSources {
		shuffle.dataSources[i] = b.build(dataSource)
		if b.err != nil {
			return nil
		}
	}

	// 3. initialize the workers
	head := v.Children()[0]
	// A `PhysicalShuffleReceiverStub` for every worker have the same `DataSource` but different `Receiver`.
	// We preallocate `PhysicalShuffleReceiverStub`s here and reuse them below.
	stubs := make([]*plannercore.PhysicalShuffleReceiverStub, 0, len(v.DataSources))
	for _, dataSource := range v.DataSources {
		stub := plannercore.PhysicalShuffleReceiverStub{
			DataSource: dataSource,
		}.Init(b.ctx.GetPlanCtx(), dataSource.StatsInfo(), dataSource.QueryBlockOffset(), nil)
		stub.SetSchema(dataSource.Schema())
		stubs = append(stubs, stub)
	}
	shuffle.workers = make([]*shuffleWorker, shuffle.concurrency)
	for i := range shuffle.workers {
		receivers := make([]*shuffleReceiver, len(v.DataSources))
		for j, dataSource := range v.DataSources {
			receivers[j] = &shuffleReceiver{
				BaseExecutor: exec.NewBaseExecutor(b.ctx, dataSource.Schema(), stubs[j].ID()),
			}
		}

		w := &shuffleWorker{
			receivers: receivers,
		}

		for j := range v.DataSources {
			stub := stubs[j]
			stub.Receiver = (unsafe.Pointer)(receivers[j])
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

func (*executorBuilder) buildShuffleReceiverStub(v *plannercore.PhysicalShuffleReceiverStub) *shuffleReceiver {
	return (*shuffleReceiver)(v.Receiver)
}

func (b *executorBuilder) buildSQLBindExec(v *plannercore.SQLBindPlan) exec.Executor {
	base := exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID())
	base.SetInitCap(chunk.ZeroCapacity)

	e := &SQLBindExec{
		BaseExecutor: base,
		isGlobal:     v.IsGlobal,
		sqlBindOp:    v.SQLBindOp,
		details:      v.Details,
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
		if reqCols[i].ID < 0 {
			// model.ExtraHandleID, ExtraPhysTblID... etc
			// Don't set the default value for that column.
			chk.AppendNull(i)
			return nil
		}

		ci := getColInfoByID(tbl, reqCols[i].ID)
		d, err := table.GetColOriginDefaultValue(ctx.GetExprCtx(), ci)
		if err != nil {
			return err
		}
		chk.AppendDatum(i, &d)
		return nil
	}
	return rowcodec.NewChunkDecoder(reqCols, pkCols, defVal, ctx.GetSessionVars().Location())
}

func (b *executorBuilder) buildBatchPointGet(plan *plannercore.BatchPointGetPlan) exec.Executor {
	var err error
	if err = b.validCanReadTemporaryOrCacheTable(plan.TblInfo); err != nil {
		b.err = err
		return nil
	}

	if plan.Lock && !b.inSelectLockStmt {
		b.inSelectLockStmt = true
		defer func() {
			b.inSelectLockStmt = false
		}()
	}
	handles, isTableDual := plan.PrunePartitionsAndValues(b.ctx)
	if isTableDual {
		// No matching partitions
		return &TableDualExec{
			BaseExecutorV2: exec.NewBaseExecutorV2(b.ctx.GetSessionVars(), plan.Schema(), plan.ID()),
			numDualRows:    0,
		}
	}

	decoder := NewRowDecoder(b.ctx, plan.Schema(), plan.TblInfo)
	e := &BatchPointGetExec{
		BaseExecutor:       exec.NewBaseExecutor(b.ctx, plan.Schema(), plan.ID()),
		indexUsageReporter: b.buildIndexUsageReporter(plan),
		tblInfo:            plan.TblInfo,
		idxInfo:            plan.IndexInfo,
		rowDecoder:         decoder,
		keepOrder:          plan.KeepOrder,
		desc:               plan.Desc,
		lock:               plan.Lock,
		waitTime:           plan.LockWaitTime,
		columns:            plan.Columns,
		handles:            handles,
		idxVals:            plan.IndexValues,
		partitionNames:     plan.PartitionNames,
	}

	e.snapshot, err = b.getSnapshot()
	if err != nil {
		b.err = err
		return nil
	}
	if e.Ctx().GetSessionVars().IsReplicaReadClosestAdaptive() {
		e.snapshot.SetOption(kv.ReplicaReadAdjuster, newReplicaReadAdjuster(e.Ctx(), plan.GetAvgRowSize()))
	}
	if e.RuntimeStats() != nil {
		snapshotStats := &txnsnapshot.SnapshotRuntimeStats{}
		e.stats = &runtimeStatsWithSnapshot{
			SnapshotRuntimeStats: snapshotStats,
		}
		e.snapshot.SetOption(kv.CollectRuntimeStats, snapshotStats)
	}

	if plan.IndexInfo != nil {
		sctx := b.ctx.GetSessionVars().StmtCtx
		sctx.IndexNames = append(sctx.IndexNames, plan.TblInfo.Name.O+":"+plan.IndexInfo.Name.O)
	}

	failpoint.Inject("assertBatchPointReplicaOption", func(val failpoint.Value) {
		assertScope := val.(string)
		if e.Ctx().GetSessionVars().GetReplicaRead().IsClosestRead() && assertScope != b.readReplicaScope {
			panic("batch point get replica option fail")
		}
	})

	snapshotTS, err := b.getSnapshotTS()
	if err != nil {
		b.err = err
		return nil
	}
	if plan.TblInfo.TableCacheStatusType == model.TableCacheStatusEnable {
		if cacheTable := b.getCacheTable(plan.TblInfo, snapshotTS); cacheTable != nil {
			e.snapshot = cacheTableSnapshot{e.snapshot, cacheTable}
		}
	}

	if plan.TblInfo.TempTableType != model.TempTableNone {
		// Temporary table should not do any lock operations
		e.lock = false
		e.waitTime = 0
	}

	if e.lock {
		b.hasLock = true
	}
	if pi := plan.TblInfo.GetPartitionInfo(); pi != nil && len(plan.PartitionIdxs) > 0 {
		defs := plan.TblInfo.GetPartitionInfo().Definitions
		if plan.SinglePartition {
			e.singlePartID = defs[plan.PartitionIdxs[0]].ID
		} else {
			e.planPhysIDs = make([]int64, len(plan.PartitionIdxs))
			for i, idx := range plan.PartitionIdxs {
				e.planPhysIDs[i] = defs[idx].ID
			}
		}
	}

	capacity := len(e.handles)
	if capacity == 0 {
		capacity = len(e.idxVals)
	}
	e.SetInitCap(capacity)
	e.SetMaxChunkSize(capacity)
	e.buildVirtualColumnInfo()
	return e
}

func newReplicaReadAdjuster(ctx sessionctx.Context, avgRowSize float64) txnkv.ReplicaReadAdjuster {
	return func(count int) (tikv.StoreSelectorOption, clientkv.ReplicaReadType) {
		if int64(avgRowSize*float64(count)) >= ctx.GetSessionVars().ReplicaClosestReadThreshold {
			return tikv.WithMatchLabels([]*metapb.StoreLabel{
				{
					Key:   placement.DCLabelKey,
					Value: config.GetTxnScopeFromConfig(),
				},
			}), clientkv.ReplicaReadMixed
		}
		// fallback to read from leader if the request is small
		return nil, clientkv.ReplicaReadLeader
	}
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

func (builder *dataReaderBuilder) partitionPruning(tbl table.PartitionedTable, planPartInfo *plannercore.PhysPlanPartInfo) ([]table.PhysicalTable, error) {
	builder.once.Do(func() {
		condPruneResult, err := partitionPruning(builder.executorBuilder.ctx, tbl, planPartInfo)
		builder.once.condPruneResult = condPruneResult
		builder.once.err = err
	})
	return builder.once.condPruneResult, builder.once.err
}

func partitionPruning(ctx sessionctx.Context, tbl table.PartitionedTable, planPartInfo *plannercore.PhysPlanPartInfo) ([]table.PhysicalTable, error) {
	var pruningConds []expression.Expression
	var partitionNames []pmodel.CIStr
	var columns []*expression.Column
	var columnNames types.NameSlice
	if planPartInfo != nil {
		pruningConds = planPartInfo.PruningConds
		partitionNames = planPartInfo.PartitionNames
		columns = planPartInfo.Columns
		columnNames = planPartInfo.ColumnNames
	}
	idxArr, err := plannercore.PartitionPruning(ctx.GetPlanCtx(), tbl, pruningConds, partitionNames, columns, columnNames)
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

func getPartitionIDsAfterPruning(ctx sessionctx.Context, tbl table.PartitionedTable, physPlanPartInfo *plannercore.PhysPlanPartInfo) (map[int64]struct{}, error) {
	if physPlanPartInfo == nil {
		return nil, errors.New("physPlanPartInfo in getPartitionIDsAfterPruning must not be nil")
	}
	idxArr, err := plannercore.PartitionPruning(ctx.GetPlanCtx(), tbl, physPlanPartInfo.PruningConds, physPlanPartInfo.PartitionNames, physPlanPartInfo.Columns, physPlanPartInfo.ColumnNames)
	if err != nil {
		return nil, err
	}

	var ret map[int64]struct{}

	pi := tbl.Meta().GetPartitionInfo()
	if fullRangePartition(idxArr) {
		ret = make(map[int64]struct{}, len(pi.Definitions))
		for _, def := range pi.Definitions {
			ret[def.ID] = struct{}{}
		}
	} else {
		ret = make(map[int64]struct{}, len(idxArr))
		for _, idx := range idxArr {
			pid := pi.Definitions[idx].ID
			ret[pid] = struct{}{}
		}
	}
	return ret, nil
}

func fullRangePartition(idxArr []int) bool {
	return len(idxArr) == 1 && idxArr[0] == plannercore.FullRange
}

type emptySampler struct{}

func (*emptySampler) writeChunk(_ *chunk.Chunk) error {
	return nil
}

func (*emptySampler) finished() bool {
	return true
}

func (b *executorBuilder) buildTableSample(v *plannercore.PhysicalTableSample) *TableSampleExecutor {
	startTS, err := b.getSnapshotTS()
	if err != nil {
		b.err = err
		return nil
	}
	e := &TableSampleExecutor{
		BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
		table:        v.TableInfo,
		startTS:      startTS,
	}

	tblInfo := v.TableInfo.Meta()
	if tblInfo.TempTableType != model.TempTableNone {
		if tblInfo.TempTableType != model.TempTableGlobal {
			b.err = errors.New("TABLESAMPLE clause can not be applied to local temporary tables")
			return nil
		}
		e.sampler = &emptySampler{}
	} else if v.TableSampleInfo.AstNode.SampleMethod == ast.SampleMethodTypeTiDBRegion {
		e.sampler = newTableRegionSampler(
			b.ctx, v.TableInfo, startTS, v.PhysicalTableID, v.TableSampleInfo.Partitions, v.Schema(),
			v.TableSampleInfo.FullSchema, e.RetFieldTypes(), v.Desc)
	}

	return e
}

func (b *executorBuilder) buildCTE(v *plannercore.PhysicalCTE) exec.Executor {
	storageMap, ok := b.ctx.GetSessionVars().StmtCtx.CTEStorageMap.(map[int]*CTEStorages)
	if !ok {
		b.err = errors.New("type assertion for CTEStorageMap failed")
		return nil
	}

	chkSize := b.ctx.GetSessionVars().MaxChunkSize
	// iterOutTbl will be constructed in CTEExec.Open().
	var resTbl cteutil.Storage
	var iterInTbl cteutil.Storage
	var producer *cteProducer
	storages, ok := storageMap[v.CTE.IDForStorage]
	if ok {
		// Storage already setup.
		resTbl = storages.ResTbl
		iterInTbl = storages.IterInTbl
		producer = storages.Producer
	} else {
		if v.SeedPlan == nil {
			b.err = errors.New("cte.seedPlan cannot be nil")
			return nil
		}
		// Build seed part.
		corCols := plannercore.ExtractOuterApplyCorrelatedCols(v.SeedPlan)
		seedExec := b.build(v.SeedPlan)
		if b.err != nil {
			return nil
		}

		// Setup storages.
		tps := seedExec.RetFieldTypes()
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

		// Build recursive part.
		var recursiveExec exec.Executor
		if v.RecurPlan != nil {
			recursiveExec = b.build(v.RecurPlan)
			if b.err != nil {
				return nil
			}
			corCols = append(corCols, plannercore.ExtractOuterApplyCorrelatedCols(v.RecurPlan)...)
		}

		var sel []int
		if v.CTE.IsDistinct {
			sel = make([]int, chkSize)
			for i := 0; i < chkSize; i++ {
				sel[i] = i
			}
		}

		var corColHashCodes [][]byte
		for _, corCol := range corCols {
			corColHashCodes = append(corColHashCodes, getCorColHashCode(corCol))
		}

		producer = &cteProducer{
			ctx:             b.ctx,
			seedExec:        seedExec,
			recursiveExec:   recursiveExec,
			resTbl:          resTbl,
			iterInTbl:       iterInTbl,
			isDistinct:      v.CTE.IsDistinct,
			sel:             sel,
			hasLimit:        v.CTE.HasLimit,
			limitBeg:        v.CTE.LimitBeg,
			limitEnd:        v.CTE.LimitEnd,
			corCols:         corCols,
			corColHashCodes: corColHashCodes,
		}
		storageMap[v.CTE.IDForStorage].Producer = producer
	}

	return &CTEExec{
		BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
		producer:     producer,
	}
}

func (b *executorBuilder) buildCTETableReader(v *plannercore.PhysicalCTETable) exec.Executor {
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
		BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
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
	tbl, ok := b.is.TableByID(context.Background(), tblInfo.ID)
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
		return nil
	}
	if !b.ctx.GetSessionVars().StmtCtx.InExplainStmt && !b.inDeleteStmt && !b.inUpdateStmt {
		tbl.(table.CachedTable).UpdateLockForRead(context.Background(), b.ctx.GetStore(), startTS, leaseDuration)
	}
	return nil
}

func (b *executorBuilder) buildCompactTable(v *plannercore.CompactTable) exec.Executor {
	if v.ReplicaKind != ast.CompactReplicaKindTiFlash && v.ReplicaKind != ast.CompactReplicaKindAll {
		b.err = errors.Errorf("compact %v replica is not supported", strings.ToLower(string(v.ReplicaKind)))
		return nil
	}

	store := b.ctx.GetStore()
	tikvStore, ok := store.(tikv.Storage)
	if !ok {
		b.err = errors.New("compact tiflash replica can only run with tikv compatible storage")
		return nil
	}

	var partitionIDs []int64
	if v.PartitionNames != nil {
		if v.TableInfo.Partition == nil {
			b.err = errors.Errorf("table:%s is not a partition table, but user specify partition name list:%+v", v.TableInfo.Name.O, v.PartitionNames)
			return nil
		}
		// use map to avoid FindPartitionDefinitionByName
		partitionMap := map[string]int64{}
		for _, partition := range v.TableInfo.Partition.Definitions {
			partitionMap[partition.Name.L] = partition.ID
		}

		for _, partitionName := range v.PartitionNames {
			partitionID, ok := partitionMap[partitionName.L]
			if !ok {
				b.err = table.ErrUnknownPartition.GenWithStackByArgs(partitionName.O, v.TableInfo.Name.O)
				return nil
			}
			partitionIDs = append(partitionIDs, partitionID)
		}
	}

	return &CompactTableTiFlashExec{
		BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
		tableInfo:    v.TableInfo,
		partitionIDs: partitionIDs,
		tikvStore:    tikvStore,
	}
}

func (b *executorBuilder) buildAdminShowBDRRole(v *plannercore.AdminShowBDRRole) exec.Executor {
	return &AdminShowBDRRoleExec{BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID())}
}

func (b *executorBuilder) buildRecommendIndex(v *plannercore.RecommendIndexPlan) exec.Executor {
	return &RecommendIndexExec{
		BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
		Action:       v.Action,
		SQL:          v.SQL,
		AdviseID:     v.AdviseID,
		Option:       v.Option,
		Value:        v.Value,
	}
}
