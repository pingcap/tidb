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

package ddl

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/expression/exprctx"
	"github.com/pingcap/tidb/pkg/expression/exprstatic"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tblctx"
	"github.com/pingcap/tidb/pkg/types"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"github.com/pingcap/tidb/pkg/util/timeutil"
	atomicutil "go.uber.org/atomic"
	"go.uber.org/zap"
)

// reorgCtx is for reorganization.
type reorgCtx struct {
	// doneCh is used to notify.
	// If the reorganization job is done, we will use this channel to notify outer.
	// TODO: Now we use goroutine to simulate reorganization jobs, later we may
	// use a persistent job list.
	doneCh chan reorgFnResult
	// rowCount is used to simulate a job's row count.
	rowCount int64
	// maxProgress is the historical maximum progress to prevent progress regression.
	maxProgress atomicutil.Float64

	mu struct {
		sync.Mutex
		// warnings are used to store the warnings when doing the reorg job under certain SQL modes.
		warnings      map[errors.ErrorID]*terror.Error
		warningsCount map[errors.ErrorID]int64
	}

	references atomicutil.Int32
}

// reorgFnResult records the DDL owner TS before executing reorg function, in order to help
// receiver determine if the result is from reorg function of previous DDL owner in this instance.
type reorgFnResult struct {
	ownerTS int64
	err     error
}

func newReorgExprCtx() *exprstatic.ExprContext {
	evalCtx := exprstatic.NewEvalContext(
		exprstatic.WithSQLMode(mysql.ModeNone),
		exprstatic.WithTypeFlags(types.DefaultStmtFlags),
		exprstatic.WithErrLevelMap(stmtctx.DefaultStmtErrLevels),
	)

	planCacheTracker := contextutil.NewPlanCacheTracker(contextutil.IgnoreWarn)

	return exprstatic.NewExprContext(
		exprstatic.WithEvalCtx(evalCtx),
		exprstatic.WithPlanCacheTracker(&planCacheTracker),
	)
}

func newReorgExprCtxWithReorgMeta(reorgMeta *model.DDLReorgMeta, warnHandler contextutil.WarnHandler) (*exprstatic.ExprContext, error) {
	intest.AssertNotNil(reorgMeta)
	intest.AssertNotNil(warnHandler)
	loc, err := reorgTimeZoneWithTzLoc(reorgMeta.Location)
	if err != nil {
		return nil, errors.Trace(err)
	}

	ctx := newReorgExprCtx()
	evalCtx := ctx.GetStaticEvalCtx().Apply(
		exprstatic.WithSQLMode(reorgMeta.SQLMode),
		exprstatic.WithLocation(loc),
		exprstatic.WithTypeFlags(reorgTypeFlagsWithSQLMode(reorgMeta.SQLMode)),
		exprstatic.WithErrLevelMap(reorgErrLevelsWithSQLMode(reorgMeta.SQLMode)),
		exprstatic.WithWarnHandler(warnHandler),
	)
	return ctx.Apply(exprstatic.WithEvalCtx(evalCtx)), nil
}

// reorgTableMutateContext implements table.MutateContext for reorganization.
type reorgTableMutateContext struct {
	exprCtx            exprctx.ExprContext
	encodingConfig     tblctx.RowEncodingConfig
	mutateBuffers      *tblctx.MutateBuffers
	shardID            *variable.RowIDShardGenerator
	reservedRowIDAlloc stmtctx.ReservedRowIDAlloc
}

// AlternativeAllocators implements table.MutateContext.AlternativeAllocators.
func (*reorgTableMutateContext) AlternativeAllocators(*model.TableInfo) (autoid.Allocators, bool) {
	// No alternative allocators for all tables because temporary tables
	// are not supported (temporary tables do not have any data in TiKV) in reorganization.
	return autoid.Allocators{}, false
}

// GetExprCtx implements table.MutateContext.GetExprCtx.
func (ctx *reorgTableMutateContext) GetExprCtx() exprctx.ExprContext {
	return ctx.exprCtx
}

// ConnectionID implements table.MutateContext.ConnectionID.
func (*reorgTableMutateContext) ConnectionID() uint64 {
	return 0
}

// InRestrictedSQL implements table.MutateContext.InRestrictedSQL.
func (*reorgTableMutateContext) InRestrictedSQL() bool {
	return false
}

// TxnAssertionLevel implements table.MutateContext.TxnAssertionLevel.
func (*reorgTableMutateContext) TxnAssertionLevel() variable.AssertionLevel {
	// Because only `index.Create` and `index.Delete` are invoked in reorganization which does not use this method,
	// we can just return `AssertionLevelOff`.
	return variable.AssertionLevelOff
}

// EnableMutationChecker implements table.MutateContext.EnableMutationChecker.
func (*reorgTableMutateContext) EnableMutationChecker() bool {
	// Because only `index.Create` and `index.Delete` are invoked in reorganization which does not use this method,
	// we can just return false.
	return false
}

// GetRowEncodingConfig implements table.MutateContext.GetRowEncodingConfig.
func (ctx *reorgTableMutateContext) GetRowEncodingConfig() tblctx.RowEncodingConfig {
	return ctx.encodingConfig
}

// GetMutateBuffers implements table.MutateContext.GetMutateBuffers.
func (ctx *reorgTableMutateContext) GetMutateBuffers() *tblctx.MutateBuffers {
	return ctx.mutateBuffers
}

// GetRowIDShardGenerator implements table.MutateContext.GetRowIDShardGenerator.
func (ctx *reorgTableMutateContext) GetRowIDShardGenerator() *variable.RowIDShardGenerator {
	return ctx.shardID
}

// GetReservedRowIDAlloc implements table.MutateContext.GetReservedRowIDAlloc.
func (ctx *reorgTableMutateContext) GetReservedRowIDAlloc() (*stmtctx.ReservedRowIDAlloc, bool) {
	return &ctx.reservedRowIDAlloc, true
}

// GetStatisticsSupport implements table.MutateContext.GetStatisticsSupport.
func (*reorgTableMutateContext) GetStatisticsSupport() (tblctx.StatisticsSupport, bool) {
	// We can just return `(nil, false)` because:
	// - Only `index.Create` and `index.Delete` are invoked in reorganization which does not use this method.
	// - DDL reorg do need to collect statistics in this way.
	return nil, false
}

// GetCachedTableSupport implements table.MutateContext.GetCachedTableSupport.
func (*reorgTableMutateContext) GetCachedTableSupport() (tblctx.CachedTableSupport, bool) {
	// We can just return `(nil, false)` because:
	// - Only `index.Create` and `index.Delete` are invoked in reorganization which does not use this method.
	// - It is not allowed to execute DDL on a cached table.
	return nil, false
}

// GetTemporaryTableSupport implements table.MutateContext.GetTemporaryTableSupport.
func (*reorgTableMutateContext) GetTemporaryTableSupport() (tblctx.TemporaryTableSupport, bool) {
	// We can just return `(nil, false)` because:
	// - Only `index.Create` and `index.Delete` are invoked in reorganization which does not use this method.
	// - Temporary tables do not have any data in TiKV.
	return nil, false
}

// GetExchangePartitionDMLSupport implements table.MutateContext.GetExchangePartitionDMLSupport.
func (*reorgTableMutateContext) GetExchangePartitionDMLSupport() (tblctx.ExchangePartitionDMLSupport, bool) {
	// We can just return `(nil, false)` because:
	// - Only `index.Create` and `index.Delete` are invoked in reorganization which does not use this method.
	return nil, false
}

// newReorgTableMutateContext creates a new table.MutateContext for reorganization.
func newReorgTableMutateContext(exprCtx exprctx.ExprContext) table.MutateContext {
	rowEncoder := &rowcodec.Encoder{
		Enable: vardef.GetDDLReorgRowFormat() != vardef.DefTiDBRowFormatV1,
	}

	encodingConfig := tblctx.RowEncodingConfig{
		IsRowLevelChecksumEnabled: rowEncoder.Enable,
		RowEncoder:                rowEncoder,
	}

	return &reorgTableMutateContext{
		exprCtx:        exprCtx,
		encodingConfig: encodingConfig,
		mutateBuffers:  tblctx.NewMutateBuffers(&variable.WriteStmtBufs{}),
		// Though currently, `RowIDShardGenerator` is not required in DDL reorg,
		// we still provide a valid one to keep the context complete and to avoid panic if it is used in the future.
		shardID: variable.NewRowIDShardGenerator(
			rand.New(rand.NewSource(time.Now().UnixNano())), // #nosec G404
			vardef.DefTiDBShardAllocateStep,
		),
	}
}

func reorgTypeFlagsWithSQLMode(mode mysql.SQLMode) types.Flags {
	return types.StrictFlags.
		WithTruncateAsWarning(!mode.HasStrictMode()).
		WithIgnoreInvalidDateErr(mode.HasAllowInvalidDatesMode()).
		WithIgnoreZeroInDate(!mode.HasStrictMode() || mode.HasAllowInvalidDatesMode()).
		WithCastTimeToYearThroughConcat(true)
}

func reorgErrLevelsWithSQLMode(mode mysql.SQLMode) errctx.LevelMap {
	return errctx.LevelMap{
		errctx.ErrGroupTruncate:  errctx.ResolveErrLevel(false, !mode.HasStrictMode()),
		errctx.ErrGroupBadNull:   errctx.ResolveErrLevel(false, !mode.HasStrictMode()),
		errctx.ErrGroupNoDefault: errctx.ResolveErrLevel(false, !mode.HasStrictMode()),
		errctx.ErrGroupDividedByZero: errctx.ResolveErrLevel(
			!mode.HasErrorForDivisionByZeroMode(),
			!mode.HasStrictMode(),
		),
	}
}

func reorgTimeZoneWithTzLoc(tzLoc *model.TimeZoneLocation) (*time.Location, error) {
	if tzLoc == nil {
		// It is set to SystemLocation to be compatible with nil LocationInfo.
		return timeutil.SystemLocation(), nil
	}
	return tzLoc.GetLocation()
}

func (rc *reorgCtx) setRowCount(count int64) {
	atomic.StoreInt64(&rc.rowCount, count)
}

func (rc *reorgCtx) mergeWarnings(warnings map[errors.ErrorID]*terror.Error, warningsCount map[errors.ErrorID]int64) {
	if len(warnings) == 0 || len(warningsCount) == 0 {
		return
	}
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.mu.warnings, rc.mu.warningsCount = mergeWarningsAndWarningsCount(warnings, rc.mu.warnings, warningsCount, rc.mu.warningsCount)
}

func (rc *reorgCtx) resetWarnings() {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.mu.warnings = make(map[errors.ErrorID]*terror.Error)
	rc.mu.warningsCount = make(map[errors.ErrorID]int64)
}

func (rc *reorgCtx) increaseRowCount(count int64) {
	atomic.AddInt64(&rc.rowCount, count)
}

func (rc *reorgCtx) getRowCount() int64 {
	row := atomic.LoadInt64(&rc.rowCount)
	return row
}

// setMaxProgress updates the maximum progress if the new progress is greater.
// It returns the current maximum progress (which may be unchanged if newProgress <= oldMax).
// This prevents progress regression when statistics change during backfill.
func (rc *reorgCtx) setMaxProgress(newProgress float64) float64 {
	for {
		oldMax := rc.maxProgress.Load()
		if newProgress <= oldMax {
			return oldMax
		}
		if rc.maxProgress.CompareAndSwap(oldMax, newProgress) {
			return newProgress
		}
	}
}

// runReorgJob is used as a portal to do the reorganization work.
// eg:
// 1: add index
// 2: alter column type
// 3: clean global index
// 4: reorganize partitions
/*
 ddl goroutine >---------+
   ^                     |
   |                     |
   |                     |
   |                     | <---(doneCh)--- f()
 HandleDDLQueue(...)     | <---(regular timeout)
   |                     | <---(ctx done)
   |                     |
   |                     |
 A more ddl round  <-----+
*/
// How can we cancel reorg job?
//
// The background reorg is continuously running except for several factors, for instances, ddl owner change,
// logic error (kv duplicate when insert index / cast error when alter column), ctx done, and cancel signal.
//
// When `admin cancel ddl jobs xxx` takes effect, we will give this kind of reorg ddl one more round.
// because we should pull the result from doneCh out, otherwise, the reorg worker will hang on `f()` logic,
// which is a kind of goroutine leak.
//
// That's why we couldn't set the job to rollingback state directly in `convertJob2RollbackJob`, which is a
// cancelling portal for admin cancel action.
//
// In other words, the cancelling signal is informed from the bottom up, we set the atomic cancel variable
// in the cancelling portal to notify the lower worker goroutine, and fetch the cancel error from them in
// the additional ddl round.
//
// After that, we can make sure that the worker goroutine is correctly shut down.
func (w *worker) runReorgJob(
	jobCtx *jobContext,
	reorgInfo *reorgInfo,
	tblInfo *model.TableInfo,
	reorgFn func() error,
) error {
	job := reorgInfo.Job
	d := reorgInfo.jobCtx.oldDDLCtx
	// This is for tests compatible, because most of the early tests try to build the reorg job manually
	// without reorg meta info, which will cause nil pointer in here.
	if job.ReorgMeta == nil {
		job.ReorgMeta = &model.DDLReorgMeta{
			SQLMode:       mysql.ModeNone,
			Warnings:      make(map[errors.ErrorID]*terror.Error),
			WarningsCount: make(map[errors.ErrorID]int64),
			Location:      &model.TimeZoneLocation{Name: time.UTC.String(), Offset: 0},
			Version:       model.CurrentReorgMetaVersion,
		}
	}

	rc := w.getReorgCtx(job.ID)
	if rc == nil {
		// This job is cancelling, we should return ErrCancelledDDLJob directly.
		//
		// Q: Is there any possibility that the job is cancelling and has no reorgCtx?
		// A: Yes, consider the case that :
		// - we cancel the job when backfilling the last batch of data, the cancel txn is commit first,
		// - and then the backfill workers send signal to the `doneCh` of the reorgCtx,
		// - and then the DDL worker will remove the reorgCtx
		// - and update the DDL job to `done`
		// - but at the commit time, the DDL txn will raise a "write conflict" error and retry, and it happens.
		if job.IsCancelling() {
			return dbterror.ErrCancelledDDLJob
		}

		beOwnerTS := w.ddlCtx.reorgCtx.getOwnerTS()
		rc = w.newReorgCtx(reorgInfo.Job.ID, reorgInfo.Job.GetRowCount())
		w.wg.Run(func() {
			err := reorgFn()
			rc.doneCh <- reorgFnResult{ownerTS: beOwnerTS, err: err}
		})
	}

	updateProgressInverval := 5 * time.Second
	failpoint.Inject("updateProgressIntervalInMs", func(val failpoint.Value) {
		if v, ok := val.(int); ok {
			updateProgressInverval = time.Duration(v) * time.Millisecond
		}
	})
	updateProcessTicker := time.NewTicker(updateProgressInverval)
	defer updateProcessTicker.Stop()
	for {
		select {
		case res := <-rc.doneCh:
			err := res.err
			curTS := w.ddlCtx.reorgCtx.getOwnerTS()
			if res.ownerTS != curTS {
				d.removeReorgCtx(job.ID)
				logutil.DDLLogger().Warn("owner ts mismatch, return timeout error and retry",
					zap.Int64("prevTS", res.ownerTS),
					zap.Int64("curTS", curTS))
				return jobCtx.genReorgTimeoutErr()
			}
			// Since job is cancelled，we don't care about its partial counts.
			// TODO(lance6716): should we also do for paused job?
			if terror.ErrorEqual(err, dbterror.ErrCancelledDDLJob) {
				d.removeReorgCtx(job.ID)
				return err
			}
			rowCount := rc.getRowCount()
			job.SetRowCount(rowCount)
			if err != nil {
				logutil.DDLLogger().Warn("run reorg job done",
					zap.Int64("jobID", reorgInfo.ID),
					zap.Int64("handled rows", rowCount), zap.Error(err))
			} else {
				logutil.DDLLogger().Info("run reorg job done",
					zap.Int64("jobID", reorgInfo.ID),
					zap.Int64("handled rows", rowCount))
			}

			// Update a job's warnings.
			w.mergeWarningsIntoJob(job)

			d.removeReorgCtx(job.ID)

			updateBackfillProgress(w, reorgInfo, tblInfo, rowCount)

			// For other errors, even err is not nil here, we still wait the partial counts to be collected.
			// since in the next round, the startKey is brand new which is stored by last time.
			return errors.Trace(err)
		case <-updateProcessTicker.C:
			rowCount := rc.getRowCount()
			job.SetRowCount(rowCount)
			updateBackfillProgress(w, reorgInfo, tblInfo, rowCount)

			// Update a job's warnings.
			w.mergeWarningsIntoJob(job)

			rc.resetWarnings()
			failpoint.InjectCall("onRunReorgJobTimeout")
			return jobCtx.genReorgTimeoutErr()
		}
	}
}

