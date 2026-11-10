// Copyright 2024 PingCAP, Inc.
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

package exec

import (
	"context"
	"math"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/sysproctrack"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/statistics"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	statsutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)

func warnAnalyzeVersionRewrite(sql string, params ...any) {
	escapedSQL, err := sqlescape.EscapeSQL(sql, params...)
	intest.Assert(err == nil, "sql escaping should not fail")
	// No need to fail the analyze task if the SQL escaping fails, just log the original SQL.
	if err != nil {
		escapedSQL = sql
	}
	statslogutil.StatsLogger().Warn(
		"auto analyze rewrites legacy statistics version 1 to version 2",
		zap.String("sql", escapedSQL),
	)
}

func execOptionForAnalyzeVersion(statsVer int, needVersionRewriteWarn bool, sql string, params ...any) sqlexec.OptionFuncAlias {
	if needVersionRewriteWarn {
		warnAnalyzeVersionRewrite(sql, params...)
	}
	intest.Assert(statsVer == statistics.Version2, "auto analyze should use stats version 2")
	return sqlexec.ExecOptionAnalyzeVer2
}

// AnalyzedTable is the table that an auto-analyze statement analyzes.
type AnalyzedTable struct {
	ID int64
}

// Dispatcher runs an auto-analyze statement on some TiDB node and waits for the result.
type Dispatcher interface {
	// Dispatch runs the statement. sctx is an idle session of the statement's keyspace.
	Dispatch(sctx sessionctx.Context, table AnalyzedTable, sql string, params ...any) error
}

// NewDispatcherFunc creates the Dispatcher of a stats handle from the parts of the handle that the
// dispatcher shares: the stats cache.
type NewDispatcherFunc func(statsCache statstypes.StatsCacheLoader) Dispatcher

// Runner runs the auto-analyze statements of this node.
type Runner struct {
	procIDs        statsutil.AutoAnalyzeProcIDGenerator
	sysProcTracker sysproctrack.Tracker
	dispatcher     Dispatcher
}

// NewRunner creates a Runner that tracks each statement in sysProcTracker under an ID from procIDs.
// When tidb_enable_dist_auto_analyze is ON, it runs the statements through dispatcher instead, if
// dispatcher is not nil.
func NewRunner(procIDs statsutil.AutoAnalyzeProcIDGenerator, sysProcTracker sysproctrack.Tracker, dispatcher Dispatcher) *Runner {
	return &Runner{procIDs: procIDs, sysProcTracker: sysProcTracker, dispatcher: dispatcher}
}

// AutoAnalyze runs one auto-analyze statement of table in sctx and reports whether it succeeded.
func (r *Runner) AutoAnalyze(
	sctx sessionctx.Context,
	statsVer int,
	needVersionRewriteWarn bool,
	table AnalyzedTable,
	sql string,
	params ...any,
) bool {
	startTime := time.Now()
	var err error
	if r.dispatcher != nil && vardef.EnableDistAutoAnalyze.Load() {
		// Log on the owner so DXF metadata does not need a warning-only flag.
		if needVersionRewriteWarn {
			warnAnalyzeVersionRewrite(sql, params...)
		}
		err = r.dispatcher.Dispatch(sctx, table, sql, params...)
	} else {
		_, _, err = RunAnalyzeStmt(sctx, r.procIDs, r.sysProcTracker, statsVer, needVersionRewriteWarn, sql, params...)
	}
	dur := time.Since(startTime)
	metrics.AutoAnalyzeHistogram.Observe(dur.Seconds())
	if err != nil {
		escaped, err1 := sqlescape.EscapeSQL(sql, params...)
		if err1 != nil {
			escaped = ""
		}
		statslogutil.StatsErrVerboseSampleLogger().Error(
			"auto analyze failed",
			zap.String("sql", escaped),
			zap.Duration("cost_time", dur),
			zap.Error(err),
		)
		metrics.AutoAnalyzeCounter.WithLabelValues("failed").Inc()
		return false
	}
	metrics.AutoAnalyzeCounter.WithLabelValues("succ").Inc()
	return true
}

// RunAnalyzeStmt executes the analyze statement.
func RunAnalyzeStmt(
	sctx sessionctx.Context,
	procIDs statsutil.AutoAnalyzeProcIDGenerator,
	sysProcTracker sysproctrack.Tracker,
	statsVer int,
	needVersionRewriteWarn bool,
	sql string,
	params ...any,
) ([]chunk.Row, []*resolve.ResultField, error) {
	defer func() {
		if r := recover(); r != nil {
			logutil.BgLogger().Warn("panic in execAnalyzeStmt", zap.Any("error", r), zap.Stack("stack"))
		}
	}()
	return RunAnalyzeStmtWithCtx(statsutil.StatsCtx, sctx, procIDs, sysProcTracker, statsVer, needVersionRewriteWarn, sql, params...)
}

// RunAnalyzeStmtWithCtx is RunAnalyzeStmt that stops when ctx, which must carry an internal
// source type, is cancelled. It recovers no panic: an unfinished statement must not look finished.
func RunAnalyzeStmtWithCtx(
	ctx context.Context,
	sctx sessionctx.Context,
	procIDs statsutil.AutoAnalyzeProcIDGenerator,
	sysProcTracker sysproctrack.Tracker,
	statsVer int,
	needVersionRewriteWarn bool,
	sql string,
	params ...any,
) ([]chunk.Row, []*resolve.ResultField, error) {
	pruneMode := sctx.GetSessionVars().PartitionPruneMode.Load()
	analyzeSnapshot := sctx.GetSessionVars().EnableAnalyzeSnapshot
	autoAnalyzeTracker := statsutil.NewAutoAnalyzeTracker(sysProcTracker.Track, sysProcTracker.UnTrack)
	autoAnalyzeProcID := procIDs.AutoAnalyzeProcID()
	optFuncs := []sqlexec.OptionFuncAlias{
		execOptionForAnalyzeVersion(statsVer, needVersionRewriteWarn, sql, params...),
		sqlexec.GetAnalyzeSnapshotOption(analyzeSnapshot),
		sqlexec.GetPartitionPruneModeOption(pruneMode),
		sqlexec.ExecOptionUseCurSession,
		sqlexec.ExecOptionWithSysProcTrack(autoAnalyzeProcID, autoAnalyzeTracker.Track, autoAnalyzeTracker.UnTrack),
	}
	// A cancellation of ctx also kills the process: steps like the global stats merge only watch
	// the SQL killer. The session resets a kill from before the statement, but then checks ctx.
	killed := make(chan struct{})
	stopKill := context.AfterFunc(ctx, func() {
		defer close(killed)
		sysProcTracker.KillSysProcess(autoAnalyzeProcID)
	})
	defer func() {
		// The kill must be over before the ID is released: a released ID can be
		// given to another process.
		if !stopKill() {
			<-killed
		}
		procIDs.ReleaseAutoAnalyzeProcID(autoAnalyzeProcID)
	}()
	return statsutil.ExecWithOptsWithCtx(ctx, sctx, optFuncs, sql, params...)
}

// GetAutoAnalyzeParameters gets the auto analyze parameters from mysql.global_variables.
func GetAutoAnalyzeParameters(sctx sessionctx.Context) map[string]string {
	sql := "select variable_name, variable_value from mysql.global_variables where variable_name in (%?, %?, %?)"
	rows, _, err := statsutil.ExecWithOpts(sctx, nil, sql, vardef.TiDBAutoAnalyzeRatio, vardef.TiDBAutoAnalyzeStartTime, vardef.TiDBAutoAnalyzeEndTime)
	if err != nil {
		return map[string]string{}
	}
	parameters := make(map[string]string, len(rows))
	for _, row := range rows {
		parameters[row.GetString(0)] = row.GetString(1)
	}
	return parameters
}

// ParseAutoAnalyzeRatio parses the auto analyze ratio from the string.
func ParseAutoAnalyzeRatio(ratio string) float64 {
	autoAnalyzeRatio, err := strconv.ParseFloat(ratio, 64)
	if err != nil {
		return vardef.DefAutoAnalyzeRatio
	}
	return math.Max(autoAnalyzeRatio, 0)
}

// ParseAutoAnalysisWindow parses the time window for auto analysis.
// It parses the times in UTC location.
func ParseAutoAnalysisWindow(start, end string) (_, _ time.Time, err error) {
	if start == "" {
		start = vardef.DefAutoAnalyzeStartTime
	}
	if end == "" {
		end = vardef.DefAutoAnalyzeEndTime
	}
	s, err := time.ParseInLocation(vardef.FullDayTimeFormat, start, time.UTC)
	if err != nil {
		return s, s, errors.Trace(err)
	}
	e, err := time.ParseInLocation(vardef.FullDayTimeFormat, end, time.UTC)
	return s, e, err
}
