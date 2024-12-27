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
	stderrors "errors"
	"runtime/pprof"
	"strings"
	"sync/atomic"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl/schematracker"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/executor/aggregate"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/executor/internal/pdhelper"
	"github.com/pingcap/tidb/pkg/executor/sortexec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/planctx"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/fixcontrol"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/deadlockhistory"
	"github.com/pingcap/tidb/pkg/util/disk"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/syncutil"
	"github.com/pingcap/tidb/pkg/util/topsql"
	topsqlstate "github.com/pingcap/tidb/pkg/util/topsql/state"
	"github.com/pingcap/tidb/pkg/util/tracing"
	tikverr "github.com/tikv/client-go/v2/error"
	tikvstore "github.com/tikv/client-go/v2/kv"
	tikvutil "github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

var (
	_ exec.Executor = &aggregate.HashAggExec{}
	_ exec.Executor = &IndexLookUpExecutor{}
	_ exec.Executor = &IndexReaderExecutor{}
	_ exec.Executor = &LimitExec{}
	_ exec.Executor = &MaxOneRowExec{}
	_ exec.Executor = &ProjectionExec{}
	_ exec.Executor = &SelectionExec{}
	_ exec.Executor = &SelectLockExec{}
	_ exec.Executor = &sortexec.SortExec{}
	_ exec.Executor = &aggregate.StreamAggExec{}
	_ exec.Executor = &TableDualExec{}
	_ exec.Executor = &TableReaderExecutor{}
	_ exec.Executor = &TableScanExec{}
	_ exec.Executor = &sortexec.TopNExec{}

	// GlobalMemoryUsageTracker is the ancestor of all the Executors' memory tracker and GlobalMemory Tracker
	GlobalMemoryUsageTracker *memory.Tracker
	// GlobalDiskUsageTracker is the ancestor of all the Executors' disk tracker
	GlobalDiskUsageTracker *disk.Tracker
	// GlobalAnalyzeMemoryTracker is the ancestor of all the Analyze jobs' memory tracker and child of global Tracker
	GlobalAnalyzeMemoryTracker *memory.Tracker
)

var (
	_ dataSourceExecutor = &TableReaderExecutor{}
	_ dataSourceExecutor = &IndexReaderExecutor{}
	_ dataSourceExecutor = &IndexLookUpExecutor{}
	_ dataSourceExecutor = &IndexMergeReaderExecutor{}

	// CheckTableFastBucketSize is the bucket size of fast check table.
	CheckTableFastBucketSize = atomic.Int64{}
)

// dataSourceExecutor is a table DataSource converted Executor.
// Currently, there are TableReader/IndexReader/IndexLookUp/IndexMergeReader.
// Note, partition reader is special and the caller should handle it carefully.
type dataSourceExecutor interface {
	exec.Executor
	Table() table.Table
}

const (
	// globalPanicStorageExceed represents the panic message when out of storage quota.
	globalPanicStorageExceed string = "Out Of Quota For Local Temporary Space!"
	// globalPanicMemoryExceed represents the panic message when out of memory limit.
	globalPanicMemoryExceed string = "Out Of Global Memory Limit!"
	// globalPanicAnalyzeMemoryExceed represents the panic message when out of analyze memory limit.
	globalPanicAnalyzeMemoryExceed string = "Out Of Global Analyze Memory Limit!"
)

// globalPanicOnExceed panics when GlobalDisTracker storage usage exceeds storage quota.
type globalPanicOnExceed struct {
	memory.BaseOOMAction
	mutex syncutil.Mutex // For synchronization.
}

func init() {
	action := &globalPanicOnExceed{}
	GlobalMemoryUsageTracker = memory.NewGlobalTracker(memory.LabelForGlobalMemory, -1)
	GlobalMemoryUsageTracker.SetActionOnExceed(action)
	GlobalDiskUsageTracker = disk.NewGlobalTrcaker(memory.LabelForGlobalStorage, -1)
	GlobalDiskUsageTracker.SetActionOnExceed(action)
	GlobalAnalyzeMemoryTracker = memory.NewTracker(memory.LabelForGlobalAnalyzeMemory, -1)
	GlobalAnalyzeMemoryTracker.SetActionOnExceed(action)
	// register quota funcs
	variable.SetMemQuotaAnalyze = GlobalAnalyzeMemoryTracker.SetBytesLimit
	variable.GetMemQuotaAnalyze = GlobalAnalyzeMemoryTracker.GetBytesLimit
	// TODO: do not attach now to avoid impact to global, will attach later when analyze memory track is stable
	//GlobalAnalyzeMemoryTracker.AttachToGlobalTracker(GlobalMemoryUsageTracker)

	schematracker.ConstructResultOfShowCreateDatabase = ConstructResultOfShowCreateDatabase
	schematracker.ConstructResultOfShowCreateTable = ConstructResultOfShowCreateTable

	// CheckTableFastBucketSize is used to set the fast analyze bucket size for check table.
	CheckTableFastBucketSize.Store(1024)
}

// Start the backend components
func Start() {
	pdhelper.GlobalPDHelper.Start()
}

// Stop the backend components
func Stop() {
	pdhelper.GlobalPDHelper.Stop()
}

// Action panics when storage usage exceeds storage quota.
func (a *globalPanicOnExceed) Action(t *memory.Tracker) {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	msg := ""
	switch t.Label() {
	case memory.LabelForGlobalStorage:
		msg = globalPanicStorageExceed
	case memory.LabelForGlobalMemory:
		msg = globalPanicMemoryExceed
	case memory.LabelForGlobalAnalyzeMemory:
		msg = globalPanicAnalyzeMemoryExceed
	default:
		msg = "Out of Unknown Resource Quota!"
	}
	// TODO(hawkingrei): should return error instead.
	panic(msg)
}

// GetPriority get the priority of the Action
func (*globalPanicOnExceed) GetPriority() int64 {
	return memory.DefPanicPriority
}

// SelectLockExec represents a select lock executor.
// It is built from the "SELECT .. FOR UPDATE" or the "SELECT .. LOCK IN SHARE MODE" statement.
// For "SELECT .. FOR UPDATE" statement, it locks every row key from source Executor.
// After the execution, the keys are buffered in transaction, and will be sent to KV
// when doing commit. If there is any key already locked by another transaction,
// the transaction will rollback and retry.
type SelectLockExec struct {
	exec.BaseExecutor

	Lock *ast.SelectLockInfo
	keys []kv.Key

	// The children may be a join of multiple tables, so we need a map.
	tblID2Handle map[int64][]plannerutil.HandleCols

	// When SelectLock work on a partition table, we need the partition ID
	// (Physical Table ID) instead of the 'logical' table ID to calculate
	// the lock KV. In that case, the Physical Table ID is extracted
	// from the row key in the store and as an extra column in the chunk row.

	// tblID2PhyTblIDCol is used for partitioned tables.
	// The child executor need to return an extra column containing
	// the Physical Table ID (i.e. from which partition the row came from)
	// Used during building
	tblID2PhysTblIDCol map[int64]*expression.Column

	// Used during execution
	// Map from logic tableID to column index where the physical table id is stored
	// For dynamic prune mode, model.ExtraPhysTblID columns are requested from
	// storage and used for physical table id
	// For static prune mode, model.ExtraPhysTblID is still sent to storage/Protobuf
	// but could be filled in by the partitions TableReaderExecutor
	// due to issues with chunk handling between the TableReaderExecutor and the
	// SelectReader result.
	tblID2PhysTblIDColIdx map[int64]int
}

// Open implements the Executor Open interface.
func (e *SelectLockExec) Open(ctx context.Context) error {
	if len(e.tblID2PhysTblIDCol) > 0 {
		e.tblID2PhysTblIDColIdx = make(map[int64]int)
		cols := e.Schema().Columns
		for i := len(cols) - 1; i >= 0; i-- {
			if cols[i].ID == model.ExtraPhysTblID {
				for tblID, col := range e.tblID2PhysTblIDCol {
					if cols[i].UniqueID == col.UniqueID {
						e.tblID2PhysTblIDColIdx[tblID] = i
						break
					}
				}
			}
		}
	}
	return e.BaseExecutor.Open(ctx)
}

// Next implements the Executor Next interface.
func (e *SelectLockExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.MaxChunkSize())
	err := exec.Next(ctx, e.Children(0), req)
	if err != nil {
		return err
	}
	// If there's no handle or it's not a `SELECT FOR UPDATE` or `SELECT FOR SHARE` statement.
	if len(e.tblID2Handle) == 0 || (!logicalop.IsSupportedSelectLockType(e.Lock.LockType)) {
		return nil
	}

	if req.NumRows() > 0 {
		iter := chunk.NewIterator4Chunk(req)
		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			for tblID, cols := range e.tblID2Handle {
				for _, col := range cols {
					handle, err := col.BuildHandle(row)
					if err != nil {
						return err
					}
					physTblID := tblID
					if physTblColIdx, ok := e.tblID2PhysTblIDColIdx[tblID]; ok {
						physTblID = row.GetInt64(physTblColIdx)
						if physTblID == 0 {
							// select * from t1 left join t2 on t1.c = t2.c for update
							// The join right side might be added NULL in left join
							// In that case, physTblID is 0, so skip adding the lock.
							//
							// Note, we can't distinguish whether it's the left join case,
							// or a bug that TiKV return without correct physical ID column.
							continue
						}
					}
					e.keys = append(e.keys, tablecodec.EncodeRowKeyWithHandle(physTblID, handle))
				}
			}
		}
		return nil
	}
	lockWaitTime := e.Ctx().GetSessionVars().LockWaitTimeout
	if e.Lock.LockType == ast.SelectLockForUpdateNoWait || e.Lock.LockType == ast.SelectLockForShareNoWait {
		lockWaitTime = tikvstore.LockNoWait
	} else if e.Lock.LockType == ast.SelectLockForUpdateWaitN {
		lockWaitTime = int64(e.Lock.WaitSec) * 1000
	}

	for id := range e.tblID2Handle {
		e.UpdateDeltaForTableID(id)
	}
	lockCtx, err := newLockCtx(e.Ctx(), lockWaitTime, len(e.keys))
	if err != nil {
		return err
	}
	return doLockKeys(ctx, e.Ctx(), lockCtx, e.keys...)
}

func newLockCtx(sctx sessionctx.Context, lockWaitTime int64, numKeys int) (*tikvstore.LockCtx, error) {
	seVars := sctx.GetSessionVars()
	forUpdateTS, err := sessiontxn.GetTxnManager(sctx).GetStmtForUpdateTS()
	if err != nil {
		return nil, err
	}
	lockCtx := tikvstore.NewLockCtx(forUpdateTS, lockWaitTime, seVars.StmtCtx.GetLockWaitStartTime())
	lockCtx.Killed = &seVars.SQLKiller.Signal
	lockCtx.PessimisticLockWaited = &seVars.StmtCtx.PessimisticLockWaited
	lockCtx.LockKeysDuration = &seVars.StmtCtx.LockKeysDuration
	lockCtx.LockKeysCount = &seVars.StmtCtx.LockKeysCount
	lockCtx.LockExpired = &seVars.TxnCtx.LockExpire
	lockCtx.ResourceGroupTagger = func(req *kvrpcpb.PessimisticLockRequest) []byte {
		if req == nil {
			return nil
		}
		if len(req.Mutations) == 0 {
			return nil
		}
		if mutation := req.Mutations[0]; mutation != nil {
			normalized, digest := seVars.StmtCtx.SQLDigest()
			if len(normalized) == 0 {
				return nil
			}
			_, planDigest := seVars.StmtCtx.GetPlanDigest()

			return kv.NewResourceGroupTagBuilder().
				SetPlanDigest(planDigest).
				SetSQLDigest(digest).
				EncodeTagWithKey(mutation.Key)
		}
		return nil
	}
	lockCtx.OnDeadlock = func(deadlock *tikverr.ErrDeadlock) {
		cfg := config.GetGlobalConfig()
		if deadlock.IsRetryable && !cfg.PessimisticTxn.DeadlockHistoryCollectRetryable {
			return
		}
		rec := deadlockhistory.ErrDeadlockToDeadlockRecord(deadlock)
		deadlockhistory.GlobalDeadlockHistory.Push(rec)
	}
	if lockCtx.ForUpdateTS > 0 && seVars.AssertionLevel != variable.AssertionLevelOff {
		lockCtx.InitCheckExistence(numKeys)
	}
	return lockCtx, nil
}

// doLockKeys is the main entry for pessimistic lock keys
// waitTime means the lock operation will wait in milliseconds if target key is already
// locked by others. used for (select for update nowait) situation
func doLockKeys(ctx context.Context, se sessionctx.Context, lockCtx *tikvstore.LockCtx, keys ...kv.Key) error {
	sessVars := se.GetSessionVars()
	sctx := sessVars.StmtCtx
	if !sctx.InUpdateStmt && !sctx.InDeleteStmt {
		atomic.StoreUint32(&se.GetSessionVars().TxnCtx.ForUpdate, 1)
	}
	// Lock keys only once when finished fetching all results.
	txn, err := se.Txn(true)
	if err != nil {
		return err
	}

	// Skip the temporary table keys.
	keys = filterTemporaryTableKeys(sessVars, keys)

	keys = filterLockTableKeys(sessVars.StmtCtx, keys)
	var lockKeyStats *tikvutil.LockKeysDetails
	ctx = context.WithValue(ctx, tikvutil.LockKeysDetailCtxKey, &lockKeyStats)
	err = txn.LockKeys(tikvutil.SetSessionID(ctx, se.GetSessionVars().ConnectionID), lockCtx, keys...)
	if lockKeyStats != nil {
		sctx.MergeLockKeysExecDetails(lockKeyStats)
	}
	return err
}

func filterTemporaryTableKeys(vars *variable.SessionVars, keys []kv.Key) []kv.Key {
	txnCtx := vars.TxnCtx
	if txnCtx == nil || txnCtx.TemporaryTables == nil {
		return keys
	}

	newKeys := keys[:0:len(keys)]
	for _, key := range keys {
		tblID := tablecodec.DecodeTableID(key)
		if _, ok := txnCtx.TemporaryTables[tblID]; !ok {
			newKeys = append(newKeys, key)
		}
	}
	return newKeys
}

func filterLockTableKeys(stmtCtx *stmtctx.StatementContext, keys []kv.Key) []kv.Key {
	if len(stmtCtx.LockTableIDs) == 0 {
		return keys
	}
	newKeys := keys[:0:len(keys)]
	for _, key := range keys {
		tblID := tablecodec.DecodeTableID(key)
		if _, ok := stmtCtx.LockTableIDs[tblID]; ok {
			newKeys = append(newKeys, key)
		}
	}
	return newKeys
}

// LimitExec represents limit executor
// It ignores 'Offset' rows from src, then returns 'Count' rows at maximum.
type LimitExec struct {
	exec.BaseExecutor

	begin  uint64
	end    uint64
	cursor uint64

	// meetFirstBatch represents whether we have met the first valid Chunk from child.
	meetFirstBatch bool

	childResult *chunk.Chunk

	// columnIdxsUsedByChild keep column indexes of child executor used for inline projection
	columnIdxsUsedByChild []int
	columnSwapHelper      *chunk.ColumnSwapHelper

	// Log the close time when opentracing is enabled.
	span opentracing.Span
}

// Next implements the Executor Next interface.
func (e *LimitExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.cursor >= e.end {
		return nil
	}
	for !e.meetFirstBatch {
		// transfer req's requiredRows to childResult and then adjust it in childResult
		e.childResult = e.childResult.SetRequiredRows(req.RequiredRows(), e.MaxChunkSize())
		err := exec.Next(ctx, e.Children(0), e.adjustRequiredRows(e.childResult))
		if err != nil {
			return err
		}
		batchSize := uint64(e.childResult.NumRows())
		// no more data.
		if batchSize == 0 {
			return nil
		}
		if newCursor := e.cursor + batchSize; newCursor >= e.begin {
			e.meetFirstBatch = true
			begin, end := e.begin-e.cursor, batchSize
			if newCursor > e.end {
				end = e.end - e.cursor
			}
			e.cursor += end
			if begin == end {
				break
			}
			if e.columnIdxsUsedByChild != nil {
				req.Append(e.childResult.Prune(e.columnIdxsUsedByChild), int(begin), int(end))
			} else {
				req.Append(e.childResult, int(begin), int(end))
			}
			return nil
		}
		e.cursor += batchSize
	}
	e.childResult.Reset()
	e.childResult = e.childResult.SetRequiredRows(req.RequiredRows(), e.MaxChunkSize())
	e.adjustRequiredRows(e.childResult)
	err := exec.Next(ctx, e.Children(0), e.childResult)
	if err != nil {
		return err
	}
	batchSize := uint64(e.childResult.NumRows())
	// no more data.
	if batchSize == 0 {
		return nil
	}
	if e.cursor+batchSize > e.end {
		e.childResult.TruncateTo(int(e.end - e.cursor))
		batchSize = e.end - e.cursor
	}
	e.cursor += batchSize

	if e.columnIdxsUsedByChild != nil {
		err = e.columnSwapHelper.SwapColumns(e.childResult, req)
		if err != nil {
			return err
		}
	} else {
		req.SwapColumns(e.childResult)
	}
	return nil
}

// Open implements the Executor Open interface.
func (e *LimitExec) Open(ctx context.Context) error {
	if err := e.BaseExecutor.Open(ctx); err != nil {
		return err
	}
	e.childResult = exec.TryNewCacheChunk(e.Children(0))
	e.cursor = 0
	e.meetFirstBatch = e.begin == 0
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		e.span = span
	}
	return nil
}

// Close implements the Executor Close interface.
func (e *LimitExec) Close() error {
	start := time.Now()

	e.childResult = nil
	err := e.BaseExecutor.Close()

	elapsed := time.Since(start)
	if elapsed > time.Millisecond {
		logutil.BgLogger().Info("limit executor close takes a long time",
			zap.Duration("elapsed", elapsed))
		if e.span != nil {
			span1 := e.span.Tracer().StartSpan("limitExec.Close", opentracing.ChildOf(e.span.Context()), opentracing.StartTime(start))
			defer span1.Finish()
		}
	}
	return err
}

func (e *LimitExec) adjustRequiredRows(chk *chunk.Chunk) *chunk.Chunk {
	// the limit of maximum number of rows the LimitExec should read
	limitTotal := int(e.end - e.cursor)

	var limitRequired int
	if e.cursor < e.begin {
		// if cursor is less than begin, it have to read (begin-cursor) rows to ignore
		// and then read chk.RequiredRows() rows to return,
		// so the limit is (begin-cursor)+chk.RequiredRows().
		limitRequired = int(e.begin) - int(e.cursor) + chk.RequiredRows()
	} else {
		// if cursor is equal or larger than begin, just read chk.RequiredRows() rows to return.
		limitRequired = chk.RequiredRows()
	}

	return chk.SetRequiredRows(min(limitTotal, limitRequired), e.MaxChunkSize())
}

func init() {
	// While doing optimization in the plan package, we need to execute uncorrelated subquery,
	// but the plan package cannot import the executor package because of the dependency cycle.
	// So we assign a function implemented in the executor package to the plan package to avoid the dependency cycle.
	plannercore.EvalSubqueryFirstRow = func(ctx context.Context, p base.PhysicalPlan, is infoschema.InfoSchema, pctx planctx.PlanContext) ([]types.Datum, error) {
		if fixcontrol.GetBoolWithDefault(pctx.GetSessionVars().OptimizerFixControl, fixcontrol.Fix43817, false) {
			return nil, errors.NewNoStackError("evaluate non-correlated sub-queries during optimization phase is not allowed by fix-control 43817")
		}

		defer func(begin time.Time) {
			s := pctx.GetSessionVars()
			s.StmtCtx.SetSkipPlanCache("query has uncorrelated sub-queries is un-cacheable")
			s.RewritePhaseInfo.PreprocessSubQueries++
			s.RewritePhaseInfo.DurationPreprocessSubQuery += time.Since(begin)
		}(time.Now())

		r, ctx := tracing.StartRegionEx(ctx, "executor.EvalSubQuery")
		defer r.End()

		sctx, err := plannercore.AsSctx(pctx)
		intest.AssertNoError(err)
		if err != nil {
			return nil, err
		}

		e := newExecutorBuilder(sctx, is)
		executor := e.build(p)
		if e.err != nil {
			return nil, e.err
		}
		err = exec.Open(ctx, executor)
		defer func() { terror.Log(exec.Close(executor)) }()
		if err != nil {
			return nil, err
		}
		if pi, ok := sctx.(processinfoSetter); ok {
			// Before executing the sub-query, we need update the processinfo to make the progress bar more accurate.
			// because the sub-query may take a long time.
			pi.UpdateProcessInfo()
		}
		chk := exec.TryNewCacheChunk(executor)
		err = exec.Next(ctx, executor, chk)
		if err != nil {
			return nil, err
		}
		if chk.NumRows() == 0 {
			return nil, nil
		}
		row := chk.GetRow(0).GetDatumRow(exec.RetTypes(executor))
		return row, err
	}
}

// TableDualExec represents a dual table executor.
type TableDualExec struct {
	exec.BaseExecutorV2

	// numDualRows can only be 0 or 1.
	numDualRows int
	numReturned int
}

// Open implements the Executor Open interface.
func (e *TableDualExec) Open(context.Context) error {
	e.numReturned = 0
	return nil
}

// Next implements the Executor Next interface.
func (e *TableDualExec) Next(_ context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.numReturned >= e.numDualRows {
		return nil
	}
	if e.Schema().Len() == 0 {
		req.SetNumVirtualRows(1)
	} else {
		for i := range e.Schema().Columns {
			req.AppendNull(i)
		}
	}
	e.numReturned = e.numDualRows
	return nil
}

type selectionExecutorContext struct {
	stmtMemTracker             *memory.Tracker
	evalCtx                    expression.EvalContext
	enableVectorizedExpression bool
}

func newSelectionExecutorContext(sctx sessionctx.Context) selectionExecutorContext {
	return selectionExecutorContext{
		stmtMemTracker:             sctx.GetSessionVars().StmtCtx.MemTracker,
		evalCtx:                    sctx.GetExprCtx().GetEvalCtx(),
		enableVectorizedExpression: sctx.GetSessionVars().EnableVectorizedExpression,
	}
}

// SelectionExec represents a filter executor.
type SelectionExec struct {
	selectionExecutorContext
	exec.BaseExecutorV2

	batched     bool
	filters     []expression.Expression
	selected    []bool
	inputIter   *chunk.Iterator4Chunk
	inputRow    chunk.Row
	childResult *chunk.Chunk

	memTracker *memory.Tracker
}

// Open implements the Executor Open interface.
func (e *SelectionExec) Open(ctx context.Context) error {
	if err := e.BaseExecutorV2.Open(ctx); err != nil {
		return err
	}
	failpoint.Inject("mockSelectionExecBaseExecutorOpenReturnedError", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(errors.New("mock SelectionExec.baseExecutor.Open returned error"))
		}
	})
	return e.open(ctx)
}

func (e *SelectionExec) open(context.Context) error {
	if e.memTracker != nil {
		e.memTracker.Reset()
	} else {
		e.memTracker = memory.NewTracker(e.ID(), -1)
	}
	e.memTracker.AttachTo(e.stmtMemTracker)
	e.childResult = exec.TryNewCacheChunk(e.Children(0))
	e.memTracker.Consume(e.childResult.MemoryUsage())
	e.batched = expression.Vectorizable(e.filters)
	if e.batched {
		e.selected = make([]bool, 0, chunk.InitialCapacity)
	}
	e.inputIter = chunk.NewIterator4Chunk(e.childResult)
	e.inputRow = e.inputIter.End()
	return nil
}

// Close implements plannercore.Plan Close interface.
func (e *SelectionExec) Close() error {
	if e.childResult != nil {
		e.memTracker.Consume(-e.childResult.MemoryUsage())
		e.childResult = nil
	}
	e.selected = nil
	return e.BaseExecutorV2.Close()
}

// Next implements the Executor Next interface.
func (e *SelectionExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.MaxChunkSize())

	if !e.batched {
		return e.unBatchedNext(ctx, req)
	}

	for {
		for ; e.inputRow != e.inputIter.End(); e.inputRow = e.inputIter.Next() {
			if req.IsFull() {
				return nil
			}

			if !e.selected[e.inputRow.Idx()] {
				continue
			}

			req.AppendRow(e.inputRow)
		}
		mSize := e.childResult.MemoryUsage()
		err := exec.Next(ctx, e.Children(0), e.childResult)
		e.memTracker.Consume(e.childResult.MemoryUsage() - mSize)
		if err != nil {
			return err
		}
		// no more data.
		if e.childResult.NumRows() == 0 {
			return nil
		}
		e.selected, err = expression.VectorizedFilter(e.evalCtx, e.enableVectorizedExpression, e.filters, e.inputIter, e.selected)
		if err != nil {
			return err
		}
		e.inputRow = e.inputIter.Begin()
	}
}

// unBatchedNext filters input rows one by one and returns once an input row is selected.
// For sql with "SETVAR" in filter and "GETVAR" in projection, for example: "SELECT @a FROM t WHERE (@a := 2) > 0",
// we have to set batch size to 1 to do the evaluation of filter and projection.
func (e *SelectionExec) unBatchedNext(ctx context.Context, chk *chunk.Chunk) error {
	evalCtx := e.evalCtx
	for {
		for ; e.inputRow != e.inputIter.End(); e.inputRow = e.inputIter.Next() {
			selected, _, err := expression.EvalBool(evalCtx, e.filters, e.inputRow)
			if err != nil {
				return err
			}
			if selected {
				chk.AppendRow(e.inputRow)
				e.inputRow = e.inputIter.Next()
				return nil
			}
		}
		mSize := e.childResult.MemoryUsage()
		err := exec.Next(ctx, e.Children(0), e.childResult)
		e.memTracker.Consume(e.childResult.MemoryUsage() - mSize)
		if err != nil {
			return err
		}
		e.inputRow = e.inputIter.Begin()
		// no more data.
		if e.childResult.NumRows() == 0 {
			return nil
		}
	}
}

// TableScanExec is a table scan executor without result fields.
type TableScanExec struct {
	exec.BaseExecutor

	t                     table.Table
	columns               []*model.ColumnInfo
	virtualTableChunkList *chunk.List
	virtualTableChunkIdx  int
}

// Next implements the Executor Next interface.
func (e *TableScanExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.MaxChunkSize())
	return e.nextChunk4InfoSchema(ctx, req)
}

func (e *TableScanExec) nextChunk4InfoSchema(ctx context.Context, chk *chunk.Chunk) error {
	chk.GrowAndReset(e.MaxChunkSize())
	if e.virtualTableChunkList == nil {
		e.virtualTableChunkList = chunk.NewList(exec.RetTypes(e), e.InitCap(), e.MaxChunkSize())
		columns := make([]*table.Column, e.Schema().Len())
		for i, colInfo := range e.columns {
			columns[i] = table.ToColumn(colInfo)
		}
		mutableRow := chunk.MutRowFromTypes(exec.RetTypes(e))
		type tableIter interface {
			IterRecords(ctx context.Context, sctx sessionctx.Context, cols []*table.Column, fn table.RecordIterFunc) error
		}
		err := (e.t.(tableIter)).IterRecords(ctx, e.Ctx(), columns, func(_ kv.Handle, rec []types.Datum, _ []*table.Column) (bool, error) {
			mutableRow.SetDatums(rec...)
			e.virtualTableChunkList.AppendRow(mutableRow.ToRow())
			return true, nil
		})
		if err != nil {
			return err
		}
	}
	// no more data.
	if e.virtualTableChunkIdx >= e.virtualTableChunkList.NumChunks() {
		return nil
	}
	virtualTableChunk := e.virtualTableChunkList.GetChunk(e.virtualTableChunkIdx)
	e.virtualTableChunkIdx++
	chk.SwapColumns(virtualTableChunk)
	return nil
}

// Open implements the Executor Open interface.
func (e *TableScanExec) Open(context.Context) error {
	e.virtualTableChunkList = nil
	return nil
}

// MaxOneRowExec checks if the number of rows that a query returns is at maximum one.
// It's built from subquery expression.
type MaxOneRowExec struct {
	exec.BaseExecutor

	evaluated bool
}

// Open implements the Executor Open interface.
func (e *MaxOneRowExec) Open(ctx context.Context) error {
	if err := e.BaseExecutor.Open(ctx); err != nil {
		return err
	}
	e.evaluated = false
	return nil
}

// Next implements the Executor Next interface.
func (e *MaxOneRowExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.evaluated {
		return nil
	}
	e.evaluated = true
	err := exec.Next(ctx, e.Children(0), req)
	if err != nil {
		return err
	}

	if num := req.NumRows(); num == 0 {
		for i := range e.Schema().Columns {
			req.AppendNull(i)
		}
		return nil
	} else if num != 1 {
		return exeerrors.ErrSubqueryMoreThan1Row
	}

	childChunk := exec.TryNewCacheChunk(e.Children(0))
	err = exec.Next(ctx, e.Children(0), childChunk)
	if err != nil {
		return err
	}
	if childChunk.NumRows() != 0 {
		return exeerrors.ErrSubqueryMoreThan1Row
	}

	return nil
}

// ResetContextOfStmt resets the StmtContext and session variables.
// Before every execution, we must clear statement context.
func ResetContextOfStmt(ctx sessionctx.Context, s ast.StmtNode) (err error) {
	defer func() {
		if r := recover(); r != nil {
			logutil.BgLogger().Warn("ResetContextOfStmt panicked", zap.Stack("stack"), zap.Any("recover", r), zap.Error(err))
			if err != nil {
				err = stderrors.Join(err, util.GetRecoverError(r))
			} else {
				err = util.GetRecoverError(r)
			}
		}
	}()
	vars := ctx.GetSessionVars()
	for name, val := range vars.StmtCtx.SetVarHintRestore {
		err := vars.SetSystemVar(name, val)
		if err != nil {
			logutil.BgLogger().Warn("Failed to restore the variable after SET_VAR hint", zap.String("variable name", name), zap.String("expected value", val))
		}
	}
	vars.StmtCtx.SetVarHintRestore = nil
	var sc *stmtctx.StatementContext
	if vars.TxnCtx.CouldRetry || vars.HasStatusFlag(mysql.ServerStatusCursorExists) {
		// Must construct new statement context object, the retry history need context for every statement.
		// TODO: Maybe one day we can get rid of transaction retry, then this logic can be deleted.
		sc = stmtctx.NewStmtCtx()
	} else {
		sc = vars.InitStatementContext()
	}
	sc.SetTimeZone(vars.Location())
	sc.TaskID = stmtctx.AllocateTaskID()
	if sc.CTEStorageMap == nil {
		sc.CTEStorageMap = map[int]*CTEStorages{}
	} else {
		clear(sc.CTEStorageMap.(map[int]*CTEStorages))
	}
	if sc.LockTableIDs == nil {
		sc.LockTableIDs = make(map[int64]struct{})
	} else {
		clear(sc.LockTableIDs)
	}
	if sc.TableStats == nil {
		sc.TableStats = make(map[int64]any)
	} else {
		clear(sc.TableStats)
	}
	if sc.MDLRelatedTableIDs == nil {
		sc.MDLRelatedTableIDs = make(map[int64]struct{})
	} else {
		clear(sc.MDLRelatedTableIDs)
	}
	if sc.TblInfo2UnionScan == nil {
		sc.TblInfo2UnionScan = make(map[*model.TableInfo]bool)
	} else {
		clear(sc.TblInfo2UnionScan)
	}
	sc.IsStaleness = false
	sc.EnableOptimizeTrace = false
	sc.OptimizeTracer = nil
	sc.OptimizerCETrace = nil
	sc.IsSyncStatsFailed = false
	sc.IsExplainAnalyzeDML = false
	sc.ResourceGroupName = vars.ResourceGroupName
	// Firstly we assume that UseDynamicPruneMode can be enabled according session variable, then we will check other conditions
	// in PlanBuilder.buildDataSource
	if ctx.GetSessionVars().IsDynamicPartitionPruneEnabled() {
		sc.UseDynamicPruneMode = true
	} else {
		sc.UseDynamicPruneMode = false
	}

	sc.StatsLoad.Timeout = 0
	sc.StatsLoad.NeededItems = nil
	sc.StatsLoad.ResultCh = nil

	sc.SysdateIsNow = ctx.GetSessionVars().SysdateIsNow

	vars.MemTracker.Detach()
	vars.MemTracker.UnbindActions()
	vars.MemTracker.SetBytesLimit(vars.MemQuotaQuery)
	vars.MemTracker.ResetMaxConsumed()
	vars.DiskTracker.Detach()
	vars.DiskTracker.ResetMaxConsumed()
	vars.MemTracker.SessionID.Store(vars.ConnectionID)
	vars.MemTracker.Killer = &vars.SQLKiller
	vars.DiskTracker.Killer = &vars.SQLKiller
	vars.SQLKiller.Reset()
	vars.SQLKiller.ConnID.Store(vars.ConnectionID)

	isAnalyze := false
	if execStmt, ok := s.(*ast.ExecuteStmt); ok {
		prepareStmt, err := plannercore.GetPreparedStmt(execStmt, vars)
		if err != nil {
			return err
		}
		_, isAnalyze = prepareStmt.PreparedAst.Stmt.(*ast.AnalyzeTableStmt)
	} else if _, ok := s.(*ast.AnalyzeTableStmt); ok {
		isAnalyze = true
	}
	if isAnalyze {
		sc.InitMemTracker(memory.LabelForAnalyzeMemory, -1)
		vars.MemTracker.SetBytesLimit(-1)
		vars.MemTracker.AttachTo(GlobalAnalyzeMemoryTracker)
	} else {
		sc.InitMemTracker(memory.LabelForSQLText, -1)
	}
	logOnQueryExceedMemQuota := domain.GetDomain(ctx).ExpensiveQueryHandle().LogOnQueryExceedMemQuota
	switch variable.OOMAction.Load() {
	case variable.OOMActionCancel:
		action := &memory.PanicOnExceed{ConnID: vars.ConnectionID, Killer: vars.MemTracker.Killer}
		action.SetLogHook(logOnQueryExceedMemQuota)
		vars.MemTracker.SetActionOnExceed(action)
	case variable.OOMActionLog:
		fallthrough
	default:
		action := &memory.LogOnExceed{ConnID: vars.ConnectionID}
		action.SetLogHook(logOnQueryExceedMemQuota)
		vars.MemTracker.SetActionOnExceed(action)
	}
	sc.MemTracker.SessionID.Store(vars.ConnectionID)
	sc.MemTracker.AttachTo(vars.MemTracker)
	sc.InitDiskTracker(memory.LabelForSQLText, -1)
	globalConfig := config.GetGlobalConfig()
	if variable.EnableTmpStorageOnOOM.Load() && sc.DiskTracker != nil {
		sc.DiskTracker.AttachTo(vars.DiskTracker)
		if GlobalDiskUsageTracker != nil {
			vars.DiskTracker.AttachTo(GlobalDiskUsageTracker)
		}
	}
	if execStmt, ok := s.(*ast.ExecuteStmt); ok {
		prepareStmt, err := plannercore.GetPreparedStmt(execStmt, vars)
		if err != nil {
			return err
		}
		s = prepareStmt.PreparedAst.Stmt
		sc.InitSQLDigest(prepareStmt.NormalizedSQL, prepareStmt.SQLDigest)
		// For `execute stmt` SQL, should reset the SQL digest with the prepare SQL digest.
		goCtx := context.Background()
		if variable.EnablePProfSQLCPU.Load() && len(prepareStmt.NormalizedSQL) > 0 {
			goCtx = pprof.WithLabels(goCtx, pprof.Labels("sql", FormatSQL(prepareStmt.NormalizedSQL).String()))
			pprof.SetGoroutineLabels(goCtx)
		}
		if topsqlstate.TopSQLEnabled() && prepareStmt.SQLDigest != nil {
			sc.IsSQLRegistered.Store(true)
			topsql.AttachAndRegisterSQLInfo(goCtx, prepareStmt.NormalizedSQL, prepareStmt.SQLDigest, vars.InRestrictedSQL)
		}
		if s, ok := prepareStmt.PreparedAst.Stmt.(*ast.SelectStmt); ok {
			if s.LockInfo == nil {
				sc.WeakConsistency = isWeakConsistencyRead(ctx, execStmt)
			}
		}
	}
	// execute missed stmtID uses empty sql
	sc.OriginalSQL = s.Text()
	if explainStmt, ok := s.(*ast.ExplainStmt); ok {
		sc.InExplainStmt = true
		sc.ExplainFormat = explainStmt.Format
		sc.InExplainAnalyzeStmt = explainStmt.Analyze
		sc.IgnoreExplainIDSuffix = strings.ToLower(explainStmt.Format) == types.ExplainFormatBrief
		sc.InVerboseExplain = strings.ToLower(explainStmt.Format) == types.ExplainFormatVerbose
		s = explainStmt.Stmt
	} else {
		sc.ExplainFormat = ""
	}
	if explainForStmt, ok := s.(*ast.ExplainForStmt); ok {
		sc.InExplainStmt = true
		sc.InExplainAnalyzeStmt = true
		sc.InVerboseExplain = strings.ToLower(explainForStmt.Format) == types.ExplainFormatVerbose
	}

	// TODO: Many same bool variables here.
	// We should set only two variables (
	// IgnoreErr and StrictSQLMode) to avoid setting the same bool variables and
	// pushing them down to TiKV as flags.

	sc.InRestrictedSQL = vars.InRestrictedSQL
	strictSQLMode := vars.SQLMode.HasStrictMode()

	errLevels := sc.ErrLevels()
	errLevels[errctx.ErrGroupDividedByZero] = errctx.LevelWarn
	switch stmt := s.(type) {
	// `ResetUpdateStmtCtx` and `ResetDeleteStmtCtx` may modify the flags, so we'll need to store them.
	case *ast.UpdateStmt:
		ResetUpdateStmtCtx(sc, stmt, vars)
		errLevels = sc.ErrLevels()
	case *ast.DeleteStmt:
		ResetDeleteStmtCtx(sc, stmt, vars)
		errLevels = sc.ErrLevels()
	case *ast.InsertStmt:
		sc.InInsertStmt = true
		// For insert statement (not for update statement), disabling the StrictSQLMode
		// should make TruncateAsWarning and DividedByZeroAsWarning,
		// but should not make DupKeyAsWarning.
		if stmt.IgnoreErr {
			errLevels[errctx.ErrGroupDupKey] = errctx.LevelWarn
			errLevels[errctx.ErrGroupAutoIncReadFailed] = errctx.LevelWarn
			errLevels[errctx.ErrGroupNoMatchedPartition] = errctx.LevelWarn
		}
		// For single-row INSERT statements, ignore non-strict mode
		// See https://dev.mysql.com/doc/refman/5.7/en/constraint-invalid-data.html
		isSingleInsert := len(stmt.Lists) == 1
		errLevels[errctx.ErrGroupBadNull] = errctx.ResolveErrLevel(false, (!strictSQLMode && !isSingleInsert) || stmt.IgnoreErr)
		errLevels[errctx.ErrGroupNoDefault] = errctx.ResolveErrLevel(false, !strictSQLMode || stmt.IgnoreErr)
		errLevels[errctx.ErrGroupDividedByZero] = errctx.ResolveErrLevel(
			!vars.SQLMode.HasErrorForDivisionByZeroMode(),
			!strictSQLMode || stmt.IgnoreErr,
		)
		sc.Priority = stmt.Priority
		sc.SetTypeFlags(sc.TypeFlags().
			WithTruncateAsWarning(!strictSQLMode || stmt.IgnoreErr).
			WithIgnoreInvalidDateErr(vars.SQLMode.HasAllowInvalidDatesMode()).
			WithIgnoreZeroInDate(!vars.SQLMode.HasNoZeroInDateMode() ||
				!vars.SQLMode.HasNoZeroDateMode() || !strictSQLMode || stmt.IgnoreErr ||
				vars.SQLMode.HasAllowInvalidDatesMode()))
	case *ast.CreateTableStmt, *ast.AlterTableStmt:
		sc.InCreateOrAlterStmt = true
		sc.SetTypeFlags(sc.TypeFlags().
			WithTruncateAsWarning(!strictSQLMode).
			WithIgnoreInvalidDateErr(vars.SQLMode.HasAllowInvalidDatesMode()).
			WithIgnoreZeroInDate(!vars.SQLMode.HasNoZeroInDateMode() || !strictSQLMode ||
				vars.SQLMode.HasAllowInvalidDatesMode()).
			WithIgnoreZeroDateErr(!vars.SQLMode.HasNoZeroDateMode() || !strictSQLMode))

	case *ast.LoadDataStmt:
		sc.InLoadDataStmt = true
		// return warning instead of error when load data meet no partition for value
		errLevels[errctx.ErrGroupNoMatchedPartition] = errctx.LevelWarn
	case *ast.SelectStmt:
		sc.InSelectStmt = true

		// Return warning for truncate error in selection.
		sc.SetTypeFlags(sc.TypeFlags().
			WithTruncateAsWarning(true).
			WithIgnoreZeroInDate(true).
			WithIgnoreInvalidDateErr(vars.SQLMode.HasAllowInvalidDatesMode()))
		if opts := stmt.SelectStmtOpts; opts != nil {
			sc.Priority = opts.Priority
			sc.NotFillCache = !opts.SQLCache
		}
		sc.WeakConsistency = isWeakConsistencyRead(ctx, stmt)
	case *ast.SetOprStmt:
		sc.InSelectStmt = true
		sc.SetTypeFlags(sc.TypeFlags().
			WithTruncateAsWarning(true).
			WithIgnoreZeroInDate(true).
			WithIgnoreInvalidDateErr(vars.SQLMode.HasAllowInvalidDatesMode()))
	case *ast.ShowStmt:
		sc.SetTypeFlags(sc.TypeFlags().
			WithIgnoreTruncateErr(true).
			WithIgnoreZeroInDate(true).
			WithIgnoreInvalidDateErr(vars.SQLMode.HasAllowInvalidDatesMode()))
		if stmt.Tp == ast.ShowWarnings || stmt.Tp == ast.ShowErrors || stmt.Tp == ast.ShowSessionStates {
			sc.InShowWarning = true
			sc.SetWarnings(vars.StmtCtx.GetWarnings())
		}
	case *ast.SplitRegionStmt:
		sc.SetTypeFlags(sc.TypeFlags().
			WithIgnoreTruncateErr(false).
			WithIgnoreZeroInDate(true).
			WithIgnoreInvalidDateErr(vars.SQLMode.HasAllowInvalidDatesMode()))
	case *ast.SetSessionStatesStmt:
		sc.InSetSessionStatesStmt = true
		sc.SetTypeFlags(sc.TypeFlags().
			WithIgnoreTruncateErr(true).
			WithIgnoreZeroInDate(true).
			WithIgnoreInvalidDateErr(vars.SQLMode.HasAllowInvalidDatesMode()))
	default:
		sc.SetTypeFlags(sc.TypeFlags().
			WithIgnoreTruncateErr(true).
			WithIgnoreZeroInDate(true).
			WithIgnoreInvalidDateErr(vars.SQLMode.HasAllowInvalidDatesMode()))
	}

	if errLevels != sc.ErrLevels() {
		sc.SetErrLevels(errLevels)
	}

	sc.SetTypeFlags(sc.TypeFlags().
		WithSkipUTF8Check(vars.SkipUTF8Check).
		WithSkipSACIICheck(vars.SkipASCIICheck).
		WithSkipUTF8MB4Check(!globalConfig.Instance.CheckMb4ValueInUTF8.Load()).
		// WithAllowNegativeToUnsigned with false value indicates values less than 0 should be clipped to 0 for unsigned integer types.
		// This is the case for `insert`, `update`, `alter table`, `create table` and `load data infile` statements, when not in strict SQL mode.
		// see https://dev.mysql.com/doc/refman/5.7/en/out-of-range-and-overflow.html
		WithAllowNegativeToUnsigned(!sc.InInsertStmt && !sc.InLoadDataStmt && !sc.InUpdateStmt && !sc.InCreateOrAlterStmt),
	)

	vars.PlanCacheParams.Reset()
	if priority := mysql.PriorityEnum(atomic.LoadInt32(&variable.ForcePriority)); priority != mysql.NoPriority {
		sc.Priority = priority
	}
	if vars.StmtCtx.LastInsertID > 0 {
		sc.PrevLastInsertID = vars.StmtCtx.LastInsertID
	} else {
		sc.PrevLastInsertID = vars.StmtCtx.PrevLastInsertID
	}
	sc.PrevAffectedRows = 0
	if vars.StmtCtx.InUpdateStmt || vars.StmtCtx.InDeleteStmt || vars.StmtCtx.InInsertStmt || vars.StmtCtx.InSetSessionStatesStmt {
		sc.PrevAffectedRows = int64(vars.StmtCtx.AffectedRows())
	} else if vars.StmtCtx.InSelectStmt {
		sc.PrevAffectedRows = -1
	}
	if globalConfig.Instance.EnableCollectExecutionInfo.Load() {
		// In ExplainFor case, RuntimeStatsColl should not be reset for reuse,
		// because ExplainFor need to display the last statement information.
		reuseObj := vars.StmtCtx.RuntimeStatsColl
		if _, ok := s.(*ast.ExplainForStmt); ok {
			reuseObj = nil
		}
		sc.RuntimeStatsColl = execdetails.NewRuntimeStatsColl(reuseObj)

		// also enable index usage collector
		if sc.IndexUsageCollector == nil {
			sc.IndexUsageCollector = ctx.NewStmtIndexUsageCollector()
		} else {
			sc.IndexUsageCollector.Reset()
		}
	} else {
		// turn off the index usage collector
		sc.IndexUsageCollector = nil
	}

	sc.SetForcePlanCache(fixcontrol.GetBoolWithDefault(vars.OptimizerFixControl, fixcontrol.Fix49736, false))
	sc.SetAlwaysWarnSkipCache(sc.InExplainStmt && sc.ExplainFormat == "plan_cache")
	errCount, warnCount := vars.StmtCtx.NumErrorWarnings()
	vars.SysErrorCount = errCount
	vars.SysWarningCount = warnCount
	vars.ExchangeChunkStatus()
	vars.StmtCtx = sc
	vars.PrevFoundInPlanCache = vars.FoundInPlanCache
	vars.FoundInPlanCache = false
	vars.PrevFoundInBinding = vars.FoundInBinding
	vars.FoundInBinding = false
	vars.DurationWaitTS = 0
	vars.CurrInsertBatchExtraCols = nil
	vars.CurrInsertValues = chunk.Row{}
	ctx.GetPlanCtx().Reset()

	return
}

// ResetUpdateStmtCtx resets statement context for UpdateStmt.
func ResetUpdateStmtCtx(sc *stmtctx.StatementContext, stmt *ast.UpdateStmt, vars *variable.SessionVars) {
	strictSQLMode := vars.SQLMode.HasStrictMode()
	sc.InUpdateStmt = true
	errLevels := sc.ErrLevels()
	errLevels[errctx.ErrGroupDupKey] = errctx.ResolveErrLevel(false, stmt.IgnoreErr)
	errLevels[errctx.ErrGroupBadNull] = errctx.ResolveErrLevel(false, !strictSQLMode || stmt.IgnoreErr)
	errLevels[errctx.ErrGroupNoDefault] = errLevels[errctx.ErrGroupBadNull]
	errLevels[errctx.ErrGroupDividedByZero] = errctx.ResolveErrLevel(
		!vars.SQLMode.HasErrorForDivisionByZeroMode(),
		!strictSQLMode || stmt.IgnoreErr,
	)
	errLevels[errctx.ErrGroupNoMatchedPartition] = errctx.ResolveErrLevel(false, stmt.IgnoreErr)
	sc.SetErrLevels(errLevels)
	sc.Priority = stmt.Priority
	sc.SetTypeFlags(sc.TypeFlags().
		WithTruncateAsWarning(!strictSQLMode || stmt.IgnoreErr).
		WithIgnoreInvalidDateErr(vars.SQLMode.HasAllowInvalidDatesMode()).
		WithIgnoreZeroInDate(!vars.SQLMode.HasNoZeroInDateMode() || !vars.SQLMode.HasNoZeroDateMode() ||
			!strictSQLMode || stmt.IgnoreErr || vars.SQLMode.HasAllowInvalidDatesMode()))
}

// ResetDeleteStmtCtx resets statement context for DeleteStmt.
func ResetDeleteStmtCtx(sc *stmtctx.StatementContext, stmt *ast.DeleteStmt, vars *variable.SessionVars) {
	strictSQLMode := vars.SQLMode.HasStrictMode()
	sc.InDeleteStmt = true
	errLevels := sc.ErrLevels()
	errLevels[errctx.ErrGroupDupKey] = errctx.ResolveErrLevel(false, stmt.IgnoreErr)
	errLevels[errctx.ErrGroupBadNull] = errctx.ResolveErrLevel(false, !strictSQLMode || stmt.IgnoreErr)
	errLevels[errctx.ErrGroupNoDefault] = errLevels[errctx.ErrGroupBadNull]
	errLevels[errctx.ErrGroupDividedByZero] = errctx.ResolveErrLevel(
		!vars.SQLMode.HasErrorForDivisionByZeroMode(),
		!strictSQLMode || stmt.IgnoreErr,
	)
	sc.SetErrLevels(errLevels)
	sc.Priority = stmt.Priority
	sc.SetTypeFlags(sc.TypeFlags().
		WithTruncateAsWarning(!strictSQLMode || stmt.IgnoreErr).
		WithIgnoreInvalidDateErr(vars.SQLMode.HasAllowInvalidDatesMode()).
		WithIgnoreZeroInDate(!vars.SQLMode.HasNoZeroInDateMode() || !vars.SQLMode.HasNoZeroDateMode() ||
			!strictSQLMode || stmt.IgnoreErr || vars.SQLMode.HasAllowInvalidDatesMode()))
}

func setOptionForTopSQL(sc *stmtctx.StatementContext, snapshot kv.Snapshot) {
	if snapshot == nil {
		return
	}
	// pipelined dml may already flush in background, don't touch it to avoid race.
	if txn, ok := snapshot.(kv.Transaction); ok && txn.IsPipelined() {
		return
	}
	snapshot.SetOption(kv.ResourceGroupTagger, sc.GetResourceGroupTagger())
	if sc.KvExecCounter != nil {
		snapshot.SetOption(kv.RPCInterceptor, sc.KvExecCounter.RPCInterceptor())
	}
}

func isWeakConsistencyRead(ctx sessionctx.Context, node ast.Node) bool {
	sessionVars := ctx.GetSessionVars()
	return sessionVars.ConnectionID > 0 && sessionVars.ReadConsistency.IsWeak() &&
		plannercore.IsAutoCommitTxn(sessionVars) && plannercore.IsReadOnly(node, sessionVars)
}
