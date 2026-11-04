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

package util

import (
	"context"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/session/syssession"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/pingcap/tidb/pkg/util/sqlexec/mock"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

const (
	// StatsMetaHistorySourceAnalyze indicates stats history meta source from analyze
	StatsMetaHistorySourceAnalyze = "analyze"
	// StatsMetaHistorySourceLoadStats indicates stats history meta source from load stats
	StatsMetaHistorySourceLoadStats = "load stats"
	// StatsMetaHistorySourceFlushStats indicates stats history meta source from flush stats
	StatsMetaHistorySourceFlushStats = "flush stats"
	// StatsMetaHistorySourceSchemaChange indicates stats history meta source from schema change
	StatsMetaHistorySourceSchemaChange = "schema change"
	// StatsMetaHistorySourceExtendedStats indicates stats history meta source from extended stats
	StatsMetaHistorySourceExtendedStats = "extended stats"
)

var (
	// UseCurrentSessionOpt to make sure the sql is executed in current session.
	UseCurrentSessionOpt = []sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession}

	// StatsCtx is used to mark the request as internal stats foreground priority.
	StatsCtx = kv.WithInternalSourceType(context.Background(), kv.InternalTxnStatsForegroundPriority)
)

// finishTransaction will execute `commit` when error is nil, otherwise `rollback`.
func finishTransaction(sctx sessionctx.Context, err error) error {
	if err == nil {
		_, _, err = ExecRows(sctx, "COMMIT")
	} else {
		_, _, err1 := ExecRows(sctx, "rollback")
		terror.Log(errors.Trace(err1))
	}
	return errors.Trace(err)
}

var (
	// FlagWrapTxn indicates whether to wrap a transaction.
	FlagWrapTxn = 0
)

// CallWithSCtx allocates a sctx from the pool and call the f().
func CallWithSCtx(pool syssession.Pool, f func(sctx sessionctx.Context) error, flags ...int) (err error) {
	defer util.Recover(metrics.LabelStats, "CallWithSCtx", nil, false)
	return pool.WithSession(func(se *syssession.Session) error {
		return se.WithSessionContext(func(sctx sessionctx.Context) error {
			restoreStatsVars, err := UpdateSCtxVarsForStats(sctx)
			if err != nil { // update stats variables automatically
				return errors.Trace(err)
			}
			defer restoreStatsVars()

			wrapTxn := false
			for _, flag := range flags {
				if flag == FlagWrapTxn {
					wrapTxn = true
				}
			}
			if wrapTxn {
				return WrapTxn(sctx, f)
			}
			return f(sctx)
		})
	})
}

type statsSessionVarsSnapshot struct {
	analyzeSkipColumnTypes      map[string]struct{}
	stmtTimeZone                *time.Location
	timeZone                    *time.Location
	partitionPruneMode          string
	timeZoneValue               string
	analyzeStoreBatchSizeValue  string
	lockWaitTimeout             int64
	analyzeVersion              int
	analyzeStoreBatchSize       int
	analyzePartitionConcurrency int
	skipMissingPartitionStats   bool
	enableAnalyzeSnapshot       bool
	enableHistoricalStats       bool
	enableAsyncMergeGlobalStats bool
}

// UpdateSCtxVarsForStats updates all necessary variables that may affect the behavior of statistics.
// It restores the original settings on failure and returns a function to restore them after use on success.
func UpdateSCtxVarsForStats(sctx sessionctx.Context) (func(), error) {
	vars := sctx.GetSessionVars()
	snapshot := statsSessionVarsSnapshot{
		analyzeSkipColumnTypes:      vars.AnalyzeSkipColumnTypes,
		partitionPruneMode:          vars.PartitionPruneMode.Load(),
		enableAsyncMergeGlobalStats: vars.EnableAsyncMergeGlobalStats,
		enableHistoricalStats:       vars.EnableHistoricalStats,
		enableAnalyzeSnapshot:       vars.EnableAnalyzeSnapshot,
		skipMissingPartitionStats:   vars.SkipMissingPartitionStats,
		analyzePartitionConcurrency: vars.AnalyzePartitionConcurrency,
		analyzeVersion:              vars.AnalyzeVersion,
		analyzeStoreBatchSize:       vars.AnalyzeStoreBatchSize,
		lockWaitTimeout:             vars.LockWaitTimeout,
		timeZone:                    vars.TimeZone,
		stmtTimeZone:                vars.StmtCtx.TimeZone(),
	}
	// These setters update both cached system variables and typed fields. Unloaded
	// session variables must retain the session's original values, not current globals.
	var loaded bool
	snapshot.analyzeStoreBatchSizeValue, loaded = vars.GetSystemVar(vardef.TiDBAnalyzeStoreBatchSize)
	if !loaded {
		snapshot.analyzeStoreBatchSizeValue = strconv.Itoa(vars.AnalyzeStoreBatchSize)
	}
	snapshot.timeZoneValue, loaded = vars.GetSystemVar(vardef.TimeZone)
	if !loaded {
		snapshot.timeZoneValue = "SYSTEM"
		if vars.TimeZone != nil {
			snapshot.timeZoneValue = vars.TimeZone.String()
			if snapshot.timeZoneValue == "" {
				snapshot.timeZoneValue = time.Now().In(vars.TimeZone).Format("-07:00")
			}
		}
	}
	restore := func() {
		snapshot.restore(vars)
	}
	restoreOnErr := func(err error) (func(), error) {
		restore()
		return nil, err
	}

	// async merge global stats
	enableAsyncMergeGlobalStats, err := vars.GlobalVarsAccessor.GetGlobalSysVar(vardef.TiDBEnableAsyncMergeGlobalStats)
	if err != nil {
		return restoreOnErr(err)
	}
	vars.EnableAsyncMergeGlobalStats = variable.TiDBOptOn(enableAsyncMergeGlobalStats)

	// concurrency of save stats to storage
	analyzePartitionConcurrency, err := vars.GlobalVarsAccessor.GetGlobalSysVar(vardef.TiDBAnalyzePartitionConcurrency)
	if err != nil {
		return restoreOnErr(err)
	}
	c, err := strconv.ParseInt(analyzePartitionConcurrency, 10, 64)
	if err != nil {
		return restoreOnErr(err)
	}
	vars.AnalyzePartitionConcurrency = int(c)

	// analyzer version
	verInString, err := vars.GlobalVarsAccessor.GetGlobalSysVar(vardef.TiDBAnalyzeVersion)
	if err != nil {
		return restoreOnErr(err)
	}
	ver, err := strconv.ParseInt(verInString, 10, 64)
	if err != nil {
		return restoreOnErr(err)
	}
	vars.AnalyzeVersion = int(ver)

	// Analyze store batch size. Auto Analyze uses the latest global value instead
	// of the potentially stale value held by its pooled internal session.
	analyzeStoreBatchSize, err := vars.GlobalVarsAccessor.GetGlobalSysVar(vardef.TiDBAnalyzeStoreBatchSize)
	if err != nil {
		return restoreOnErr(err)
	}
	if err := vars.SetSystemVar(vardef.TiDBAnalyzeStoreBatchSize, analyzeStoreBatchSize); err != nil {
		return restoreOnErr(err)
	}

	// enable historical stats
	val, err := vars.GlobalVarsAccessor.GetGlobalSysVar(vardef.TiDBEnableHistoricalStats)
	if err != nil {
		return restoreOnErr(err)
	}
	vars.EnableHistoricalStats = variable.TiDBOptOn(val)

	// partition mode
	pruneMode, err := vars.GlobalVarsAccessor.GetGlobalSysVar(vardef.TiDBPartitionPruneMode)
	if err != nil {
		return restoreOnErr(err)
	}
	vars.PartitionPruneMode.Store(pruneMode)

	// enable analyze snapshot
	analyzeSnapshot, err := vars.GlobalVarsAccessor.GetGlobalSysVar(vardef.TiDBEnableAnalyzeSnapshot)
	if err != nil {
		return restoreOnErr(err)
	}
	vars.EnableAnalyzeSnapshot = variable.TiDBOptOn(analyzeSnapshot)

	// enable skip column types
	val, err = vars.GlobalVarsAccessor.GetGlobalSysVar(vardef.TiDBAnalyzeSkipColumnTypes)
	if err != nil {
		return restoreOnErr(err)
	}
	vars.AnalyzeSkipColumnTypes = variable.ParseAnalyzeSkipColumnTypes(val)

	// skip missing partition stats
	val, err = vars.GlobalVarsAccessor.GetGlobalSysVar(vardef.TiDBSkipMissingPartitionStats)
	if err != nil {
		return restoreOnErr(err)
	}
	vars.SkipMissingPartitionStats = variable.TiDBOptOn(val)
	// sync innodb_lock_wait_timeout
	val, err = vars.GlobalVarsAccessor.GetGlobalSysVar(vardef.InnodbLockWaitTimeout)
	if err != nil {
		return restoreOnErr(err)
	}
	lockWaitSec, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return restoreOnErr(err)
	}
	vars.LockWaitTimeout = lockWaitSec * 1000

	// timezone setting
	// timezone used to datetime/timestamp conversion when collecting stats.
	globalTZ, err := vars.GlobalVarsAccessor.GetGlobalSysVar(vardef.TimeZone)
	if err != nil {
		return restoreOnErr(err)
	}
	if err := vars.SetSystemVar(vardef.TimeZone, globalTZ); err != nil {
		return restoreOnErr(err)
	}
	vars.StmtCtx.SetTimeZone(vars.Location())
	return restore, nil
}

func (s statsSessionVarsSnapshot) restore(vars *variable.SessionVars) {
	if err := vars.SetSystemVar(vardef.TiDBAnalyzeStoreBatchSize, s.analyzeStoreBatchSizeValue); err != nil {
		logutil.BgLogger().Warn("restore analyze store batch size failed", zap.Error(err))
	}
	if err := vars.SetSystemVar(vardef.TimeZone, s.timeZoneValue); err != nil {
		logutil.BgLogger().Warn("restore statistics session time zone failed", zap.Error(err))
	}
	vars.AnalyzeSkipColumnTypes = s.analyzeSkipColumnTypes
	vars.PartitionPruneMode.Store(s.partitionPruneMode)
	vars.EnableAsyncMergeGlobalStats = s.enableAsyncMergeGlobalStats
	vars.EnableHistoricalStats = s.enableHistoricalStats
	vars.EnableAnalyzeSnapshot = s.enableAnalyzeSnapshot
	vars.SkipMissingPartitionStats = s.skipMissingPartitionStats
	vars.AnalyzePartitionConcurrency = s.analyzePartitionConcurrency
	vars.AnalyzeVersion = s.analyzeVersion
	vars.AnalyzeStoreBatchSize = s.analyzeStoreBatchSize
	vars.LockWaitTimeout = s.lockWaitTimeout
	vars.TimeZone = s.timeZone
	vars.StmtCtx.SetTimeZone(s.stmtTimeZone)
}

// GetCurrentPruneMode returns the current latest partitioning table prune mode.
func GetCurrentPruneMode(pool syssession.Pool) (mode string, err error) {
	err = CallWithSCtx(pool, func(sctx sessionctx.Context) error {
		mode = sctx.GetSessionVars().PartitionPruneMode.Load()
		return nil
	})
	return
}

// WrapTxn uses a transaction here can let different SQLs in this operation have the same data visibility.
func WrapTxn(sctx sessionctx.Context, f func(sctx sessionctx.Context) error) (err error) {
	// TODO: check whether this sctx is already in a txn
	if _, _, err := ExecRows(sctx, "BEGIN PESSIMISTIC"); err != nil {
		return err
	}
	defer func() {
		err = finishTransaction(sctx, err)
	}()
	err = f(sctx)
	return
}

// GetStartTS gets the start ts from current transaction.
func GetStartTS(sctx sessionctx.Context) (uint64, error) {
	txn, err := sctx.Txn(true)
	if err != nil {
		return 0, err
	}
	return txn.StartTS(), nil
}

// Exec is a helper function to execute sql and return RecordSet.
func Exec(sctx sessionctx.Context, sql string, args ...any) (sqlexec.RecordSet, error) {
	return ExecWithCtx(StatsCtx, sctx, sql, args...)
}

// ExecWithCtx is a helper function to execute sql and return RecordSet.
func ExecWithCtx(
	ctx context.Context,
	sctx sessionctx.Context,
	sql string,
	args ...any,
) (sqlexec.RecordSet, error) {
	sqlExec := sctx.GetSQLExecutor()
	// TODO: use RestrictedSQLExecutor + ExecOptionUseCurSession instead of SQLExecutor
	return sqlExec.ExecuteInternal(ctx, sql, args...)
}

// ExecRows is a helper function to execute sql and return rows and fields.
func ExecRows(sctx sessionctx.Context, sql string, args ...any) (rows []chunk.Row, fields []*resolve.ResultField, err error) {
	return ExecRowsWithCtx(StatsCtx, sctx, sql, args...)
}

// ExecRowsWithCtx is a helper function to execute sql and return rows and fields.
func ExecRowsWithCtx(
	ctx context.Context,
	sctx sessionctx.Context,
	sql string,
	args ...any,
) (rows []chunk.Row, fields []*resolve.ResultField, err error) {
	failpoint.Inject("ExecRowsTimeout", func() {
		failpoint.Return(nil, nil, errors.New("inject timeout error"))
	})
	if intest.InTest {
		if v := sctx.Value(mock.RestrictedSQLExecutorKey{}); v != nil {
			return v.(*mock.MockRestrictedSQLExecutor).ExecRestrictedSQL(
				StatsCtx, UseCurrentSessionOpt, sql, args...,
			)
		}
	}

	sqlExec := sctx.GetRestrictedSQLExecutor()
	return sqlExec.ExecRestrictedSQL(ctx, UseCurrentSessionOpt, sql, args...)
}

// ExecWithOpts is a helper function to execute sql and return rows and fields.
func ExecWithOpts(sctx sessionctx.Context, opts []sqlexec.OptionFuncAlias, sql string, args ...any) (rows []chunk.Row, fields []*resolve.ResultField, err error) {
	return ExecWithOptsWithCtx(StatsCtx, sctx, opts, sql, args...)
}

// ExecWithOptsWithCtx is a helper function to execute sql with context and options and return rows and fields.
func ExecWithOptsWithCtx(ctx context.Context, sctx sessionctx.Context, opts []sqlexec.OptionFuncAlias, sql string, args ...any) (rows []chunk.Row, fields []*resolve.ResultField, err error) {
	sqlExec := sctx.GetRestrictedSQLExecutor()
	return sqlExec.ExecRestrictedSQL(ctx, opts, sql, args...)
}

// DurationToTS converts duration to timestamp.
func DurationToTS(d time.Duration) uint64 {
	return oracle.ComposeTS(d.Nanoseconds()/int64(time.Millisecond), 0)
}

// IsSpecialGlobalIndex checks a index is a special global index or not.
// A special global index is one that is a global index and has virtual generated columns or prefix columns.
func IsSpecialGlobalIndex(idx *model.IndexInfo, tblInfo *model.TableInfo) bool {
	if !idx.Global {
		return false
	}
	for _, col := range idx.Columns {
		colInfo := tblInfo.Columns[col.Offset]
		isPrefixCol := col.Length != types.UnspecifiedLength
		if colInfo.IsVirtualGenerated() || isPrefixCol {
			return true
		}
	}
	return false
}
