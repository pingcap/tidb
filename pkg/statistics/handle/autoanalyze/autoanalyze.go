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

package autoanalyze

import (
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics"
	statsutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/pingcap/tidb/pkg/util/timeutil"
	"go.uber.org/zap"
)

// statsAnalyze implements util.StatsAnalyze.
// statsAnalyze is used to handle auto-analyze and manage analyze jobs.
type statsAnalyze struct {
	pool statsutil.SessionPool

	// TODO: use interfaces instead of raw function pointers below
	sysProcTracker          sessionctx.SysProcTracker
	getLockedTables         func(tableIDs ...int64) (map[int64]struct{}, error)
	getTableStats           func(tblInfo *model.TableInfo) *statistics.Table
	getPartitionStats       func(tblInfo *model.TableInfo, pid int64) *statistics.Table
	autoAnalyzeProcIDGetter func() uint64
	statsLease              time.Duration
}

// NewStatsAnalyze creates a new StatsAnalyze.
func NewStatsAnalyze(pool statsutil.SessionPool,
	sysProcTracker sessionctx.SysProcTracker,
	getLockedTables func(tableIDs ...int64) (map[int64]struct{}, error),
	getTableStats func(tblInfo *model.TableInfo) *statistics.Table,
	getPartitionStats func(tblInfo *model.TableInfo, pid int64) *statistics.Table,
	autoAnalyzeProcIDGetter func() uint64,
	statsLease time.Duration) statsutil.StatsAnalyze {
	return &statsAnalyze{pool: pool,
		sysProcTracker:          sysProcTracker,
		getLockedTables:         getLockedTables,
		getTableStats:           getTableStats,
		getPartitionStats:       getPartitionStats,
		autoAnalyzeProcIDGetter: autoAnalyzeProcIDGetter,
		statsLease:              statsLease}
}

// InsertAnalyzeJob inserts the analyze job to the storage.
func (sa *statsAnalyze) InsertAnalyzeJob(job *statistics.AnalyzeJob, instance string, procID uint64) error {
	return statsutil.CallWithSCtx(sa.pool, func(sctx sessionctx.Context) error {
		return insertAnalyzeJob(sctx, job, instance, procID)
	})
}

// DeleteAnalyzeJobs deletes the analyze jobs whose update time is earlier than updateTime.
func (sa *statsAnalyze) DeleteAnalyzeJobs(updateTime time.Time) error {
	return statsutil.CallWithSCtx(sa.pool, func(sctx sessionctx.Context) error {
		_, _, err := statsutil.ExecRows(sctx, "DELETE FROM mysql.analyze_jobs WHERE update_time < CONVERT_TZ(%?, '+00:00', @@TIME_ZONE)", updateTime.UTC().Format(types.TimeFormat))
		return err
	})
}

// HandleAutoAnalyze analyzes the newly created table or index.
func (sa *statsAnalyze) HandleAutoAnalyze(is infoschema.InfoSchema) (analyzed bool) {
	_ = statsutil.CallWithSCtx(sa.pool, func(sctx sessionctx.Context) error {
		analyzed = HandleAutoAnalyze(sctx, &Opt{
			StatsLease:              sa.statsLease,
			GetLockedTables:         sa.getLockedTables,
			GetTableStats:           sa.getTableStats,
			GetPartitionStats:       sa.getPartitionStats,
			SysProcTracker:          sa.sysProcTracker,
			AutoAnalyzeProcIDGetter: sa.autoAnalyzeProcIDGetter,
		}, is)
		return nil
	})
	return
}

func parseAutoAnalyzeRatio(ratio string) float64 {
	autoAnalyzeRatio, err := strconv.ParseFloat(ratio, 64)
	if err != nil {
		return variable.DefAutoAnalyzeRatio
	}
	return math.Max(autoAnalyzeRatio, 0)
}

func parseAnalyzePeriod(start, end string) (time.Time, time.Time, error) {
	if start == "" {
		start = variable.DefAutoAnalyzeStartTime
	}
	if end == "" {
		end = variable.DefAutoAnalyzeEndTime
	}
	s, err := time.ParseInLocation(variable.FullDayTimeFormat, start, time.UTC)
	if err != nil {
		return s, s, errors.Trace(err)
	}
	e, err := time.ParseInLocation(variable.FullDayTimeFormat, end, time.UTC)
	return s, e, err
}

func getAutoAnalyzeParameters(sctx sessionctx.Context) map[string]string {
	sql := "select variable_name, variable_value from mysql.global_variables where variable_name in (%?, %?, %?)"
	rows, _, err := statsutil.ExecWithOpts(sctx, nil, sql, variable.TiDBAutoAnalyzeRatio, variable.TiDBAutoAnalyzeStartTime, variable.TiDBAutoAnalyzeEndTime)
	if err != nil {
		return map[string]string{}
	}
	parameters := make(map[string]string, len(rows))
	for _, row := range rows {
		parameters[row.GetString(0)] = row.GetString(1)
	}
	return parameters
}

// Opt is used to hold parameters for auto analyze.
type Opt struct {
	// SysProcTracker is used to track analyze resource consumption.
	SysProcTracker sessionctx.SysProcTracker
	// GetLockedTables is used to look up locked tables which will be skipped in auto analyze.
	GetLockedTables func(tableIDs ...int64) (map[int64]struct{}, error)
	// GetTableStats is used to look up table stats to decide whether to analyze the table.
	GetTableStats func(tblInfo *model.TableInfo) *statistics.Table
	// GetPartitionStats is used to look up partition stats to decide whether to analyze the partition.
	GetPartitionStats func(tblInfo *model.TableInfo, pid int64) *statistics.Table
	// AutoAnalyzeProcIDGetter is used to assign job ID for analyze jobs.
	AutoAnalyzeProcIDGetter func() uint64
	// StatsLease is the current stats lease.
	StatsLease time.Duration
}

// HandleAutoAnalyze analyzes the newly created table or index.
func HandleAutoAnalyze(sctx sessionctx.Context,
	opt *Opt,
	is infoschema.InfoSchema) (analyzed bool) {
	defer func() {
		if r := recover(); r != nil {
			logutil.BgLogger().Error("HandleAutoAnalyze panicked", zap.Any("error", r), zap.Stack("stack"))
		}
	}()
	dbs := is.AllSchemaNames()
	parameters := getAutoAnalyzeParameters(sctx)
	autoAnalyzeRatio := parseAutoAnalyzeRatio(parameters[variable.TiDBAutoAnalyzeRatio])
	start, end, err := parseAnalyzePeriod(parameters[variable.TiDBAutoAnalyzeStartTime], parameters[variable.TiDBAutoAnalyzeEndTime])
	if err != nil {
		logutil.BgLogger().Error("parse auto analyze period failed", zap.String("category", "stats"), zap.Error(err))
		return false
	}
	if !timeutil.WithinDayTimePeriod(start, end, time.Now()) {
		return false
	}

	pruneMode := variable.PartitionPruneMode(sctx.GetSessionVars().PartitionPruneMode.Load())
	rd := rand.New(rand.NewSource(time.Now().UnixNano())) // #nosec G404
	rd.Shuffle(len(dbs), func(i, j int) {
		dbs[i], dbs[j] = dbs[j], dbs[i]
	})
	for _, db := range dbs {
		if util.IsMemOrSysDB(strings.ToLower(db)) {
			continue
		}
		tbls := is.SchemaTables(model.NewCIStr(db))
		// We shuffle dbs and tbls so that the order of iterating tables is random. If the order is fixed and the auto
		// analyze job of one table fails for some reason, it may always analyze the same table and fail again and again
		// when the HandleAutoAnalyze is triggered. Randomizing the order can avoid the problem.
		// TODO: Design a priority queue to place the table which needs analyze most in the front.
		rd.Shuffle(len(tbls), func(i, j int) {
			tbls[i], tbls[j] = tbls[j], tbls[i]
		})

		// We need to check every partition of every table to see if it needs to be analyzed.
		tidsAndPids := make([]int64, 0, len(tbls))
		for _, tbl := range tbls {
			tidsAndPids = append(tidsAndPids, tbl.Meta().ID)
			tblInfo := tbl.Meta()
			pi := tblInfo.GetPartitionInfo()
			if pi != nil {
				for _, def := range pi.Definitions {
					tidsAndPids = append(tidsAndPids, def.ID)
				}
			}
		}

		lockedTables, err := opt.GetLockedTables(tidsAndPids...)
		if err != nil {
			logutil.BgLogger().Error("check table lock failed",
				zap.String("category", "stats"), zap.Error(err))
			continue
		}

		for _, tbl := range tbls {
			// If table locked, skip analyze all partitions of the table.
			// FIXME: This check is not accurate, because other nodes may change the table lock status at any time.
			if _, ok := lockedTables[tbl.Meta().ID]; ok {
				continue
			}
			tblInfo := tbl.Meta()
			if tblInfo.IsView() {
				continue
			}
			pi := tblInfo.GetPartitionInfo()
			if pi == nil {
				statsTbl := opt.GetTableStats(tblInfo)
				sql := "analyze table %n.%n"
				analyzed := autoAnalyzeTable(sctx, opt, tblInfo, statsTbl, autoAnalyzeRatio, sql, db, tblInfo.Name.O)
				if analyzed {
					// analyze one table at a time to let it get the freshest parameters.
					// others will be analyzed next round which is just 3s later.
					return true
				}
				continue
			}
			// Only analyze the partition that has not been locked.
			var partitionDefs []model.PartitionDefinition
			for _, def := range pi.Definitions {
				if _, ok := lockedTables[def.ID]; !ok {
					partitionDefs = append(partitionDefs, def)
				}
			}
			if pruneMode == variable.Dynamic {
				analyzed := autoAnalyzePartitionTableInDynamicMode(sctx, opt, tblInfo, partitionDefs, db, autoAnalyzeRatio)
				if analyzed {
					return true
				}
				continue
			}
			for _, def := range partitionDefs {
				sql := "analyze table %n.%n partition %n"
				statsTbl := opt.GetPartitionStats(tblInfo, def.ID)
				analyzed := autoAnalyzeTable(sctx, opt, tblInfo, statsTbl, autoAnalyzeRatio, sql, db, tblInfo.Name.O, def.Name.O)
				if analyzed {
					return true
				}
			}
		}
	}
	return false
}

// AutoAnalyzeMinCnt means if the count of table is less than this value, we needn't do auto analyze.
var AutoAnalyzeMinCnt int64 = 1000

func autoAnalyzeTable(sctx sessionctx.Context,
	opt *Opt,
	tblInfo *model.TableInfo, statsTbl *statistics.Table,
	ratio float64, sql string, params ...interface{}) bool {
	if statsTbl.Pseudo || statsTbl.RealtimeCount < AutoAnalyzeMinCnt {
		return false
	}
	if needAnalyze, reason := NeedAnalyzeTable(statsTbl, 20*opt.StatsLease, ratio); needAnalyze {
		escaped, err := sqlexec.EscapeSQL(sql, params...)
		if err != nil {
			return false
		}
		logutil.BgLogger().Info("auto analyze triggered", zap.String("category", "stats"), zap.String("sql", escaped), zap.String("reason", reason))
		tableStatsVer := sctx.GetSessionVars().AnalyzeVersion
		statistics.CheckAnalyzeVerOnTable(statsTbl, &tableStatsVer)
		execAutoAnalyze(sctx, opt, tableStatsVer, sql, params...)
		return true
	}
	for _, idx := range tblInfo.Indices {
		if _, ok := statsTbl.Indices[idx.ID]; !ok && idx.State == model.StatePublic {
			sqlWithIdx := sql + " index %n"
			paramsWithIdx := append(params, idx.Name.O)
			escaped, err := sqlexec.EscapeSQL(sqlWithIdx, paramsWithIdx...)
			if err != nil {
				return false
			}
			logutil.BgLogger().Info("auto analyze for unanalyzed", zap.String("category", "stats"), zap.String("sql", escaped))
			tableStatsVer := sctx.GetSessionVars().AnalyzeVersion
			statistics.CheckAnalyzeVerOnTable(statsTbl, &tableStatsVer)
			execAutoAnalyze(sctx, opt, tableStatsVer, sqlWithIdx, paramsWithIdx...)
			return true
		}
	}
	return false
}

// NeedAnalyzeTable checks if we need to analyze the table:
//  1. If the table has never been analyzed, we need to analyze it when it has
//     not been modified for a while.
//  2. If the table had been analyzed before, we need to analyze it when
//     "tbl.ModifyCount/tbl.Count > autoAnalyzeRatio" and the current time is
//     between `start` and `end`.
//
// Exposed for test.
func NeedAnalyzeTable(tbl *statistics.Table, _ time.Duration, autoAnalyzeRatio float64) (bool, string) {
	analyzed := TableAnalyzed(tbl)
	if !analyzed {
		return true, "table unanalyzed"
	}
	// Auto analyze is disabled.
	if autoAnalyzeRatio == 0 {
		return false, ""
	}
	// No need to analyze it.
	tblCnt := float64(tbl.RealtimeCount)
	if histCnt := tbl.GetAnalyzeRowCount(); histCnt > 0 {
		tblCnt = histCnt
	}
	if float64(tbl.ModifyCount)/tblCnt <= autoAnalyzeRatio {
		return false, ""
	}
	return true, fmt.Sprintf("too many modifications(%v/%v>%v)", tbl.ModifyCount, tblCnt, autoAnalyzeRatio)
}

// TableAnalyzed checks if the table is analyzed.
// Exposed for test.
func TableAnalyzed(tbl *statistics.Table) bool {
	for _, col := range tbl.Columns {
		if col.IsAnalyzed() {
			return true
		}
	}
	for _, idx := range tbl.Indices {
		if idx.IsAnalyzed() {
			return true
		}
	}
	return false
}

func autoAnalyzePartitionTableInDynamicMode(sctx sessionctx.Context,
	opt *Opt,
	tblInfo *model.TableInfo, partitionDefs []model.PartitionDefinition,
	db string, ratio float64) bool {
	tableStatsVer := sctx.GetSessionVars().AnalyzeVersion
	analyzePartitionBatchSize := int(variable.AutoAnalyzePartitionBatchSize.Load())
	partitionNames := make([]interface{}, 0, len(partitionDefs))
	for _, def := range partitionDefs {
		partitionStatsTbl := opt.GetPartitionStats(tblInfo, def.ID)
		if partitionStatsTbl.Pseudo || partitionStatsTbl.RealtimeCount < AutoAnalyzeMinCnt {
			continue
		}
		if needAnalyze, _ := NeedAnalyzeTable(partitionStatsTbl, 20*opt.StatsLease, ratio); needAnalyze {
			partitionNames = append(partitionNames, def.Name.O)
			statistics.CheckAnalyzeVerOnTable(partitionStatsTbl, &tableStatsVer)
		}
	}
	getSQL := func(prefix, suffix string, numPartitions int) string {
		var sqlBuilder strings.Builder
		sqlBuilder.WriteString(prefix)
		for i := 0; i < numPartitions; i++ {
			if i != 0 {
				sqlBuilder.WriteString(",")
			}
			sqlBuilder.WriteString(" %n")
		}
		sqlBuilder.WriteString(suffix)
		return sqlBuilder.String()
	}
	if len(partitionNames) > 0 {
		logutil.BgLogger().Info("start to auto analyze", zap.String("category", "stats"),
			zap.String("table", tblInfo.Name.String()),
			zap.Any("partitions", partitionNames),
			zap.Int("analyze partition batch size", analyzePartitionBatchSize))
		statsTbl := opt.GetTableStats(tblInfo)
		statistics.CheckAnalyzeVerOnTable(statsTbl, &tableStatsVer)
		for i := 0; i < len(partitionNames); i += analyzePartitionBatchSize {
			start := i
			end := start + analyzePartitionBatchSize
			if end >= len(partitionNames) {
				end = len(partitionNames)
			}
			sql := getSQL("analyze table %n.%n partition", "", end-start)
			params := append([]interface{}{db, tblInfo.Name.O}, partitionNames[start:end]...)
			logutil.BgLogger().Info("auto analyze triggered", zap.String("category", "stats"),
				zap.String("table", tblInfo.Name.String()),
				zap.Any("partitions", partitionNames[start:end]))
			execAutoAnalyze(sctx, opt, tableStatsVer, sql, params...)
		}
		return true
	}
	for _, idx := range tblInfo.Indices {
		if idx.State != model.StatePublic {
			continue
		}
		for _, def := range partitionDefs {
			partitionStatsTbl := opt.GetPartitionStats(tblInfo, def.ID)
			if _, ok := partitionStatsTbl.Indices[idx.ID]; !ok {
				partitionNames = append(partitionNames, def.Name.O)
				statistics.CheckAnalyzeVerOnTable(partitionStatsTbl, &tableStatsVer)
			}
		}
		if len(partitionNames) > 0 {
			statsTbl := opt.GetTableStats(tblInfo)
			statistics.CheckAnalyzeVerOnTable(statsTbl, &tableStatsVer)
			for i := 0; i < len(partitionNames); i += analyzePartitionBatchSize {
				start := i
				end := start + analyzePartitionBatchSize
				if end >= len(partitionNames) {
					end = len(partitionNames)
				}
				sql := getSQL("analyze table %n.%n partition", " index %n", end-start)
				params := append([]interface{}{db, tblInfo.Name.O}, partitionNames[start:end]...)
				params = append(params, idx.Name.O)
				logutil.BgLogger().Info("auto analyze for unanalyzed", zap.String("category", "stats"),
					zap.String("table", tblInfo.Name.String()),
					zap.String("index", idx.Name.String()),
					zap.Any("partitions", partitionNames[start:end]))
				execAutoAnalyze(sctx, opt, tableStatsVer, sql, params...)
			}
			return true
		}
	}
	return false
}

var execOptionForAnalyze = map[int]sqlexec.OptionFuncAlias{
	statistics.Version0: sqlexec.ExecOptionAnalyzeVer1,
	statistics.Version1: sqlexec.ExecOptionAnalyzeVer1,
	statistics.Version2: sqlexec.ExecOptionAnalyzeVer2,
}

func execAutoAnalyze(sctx sessionctx.Context,
	opt *Opt,
	statsVer int,
	sql string, params ...interface{}) {
	startTime := time.Now()
	_, _, err := execAnalyzeStmt(sctx, opt, statsVer, sql, params...)
	dur := time.Since(startTime)
	metrics.AutoAnalyzeHistogram.Observe(dur.Seconds())
	if err != nil {
		escaped, err1 := sqlexec.EscapeSQL(sql, params...)
		if err1 != nil {
			escaped = ""
		}
		logutil.BgLogger().Error("auto analyze failed", zap.String("category", "stats"), zap.String("sql", escaped), zap.Duration("cost_time", dur), zap.Error(err))
		metrics.AutoAnalyzeCounter.WithLabelValues("failed").Inc()
	} else {
		metrics.AutoAnalyzeCounter.WithLabelValues("succ").Inc()
	}
}

func execAnalyzeStmt(sctx sessionctx.Context,
	opt *Opt,
	statsVer int,
	sql string, params ...interface{}) ([]chunk.Row, []*ast.ResultField, error) {
	pruneMode := sctx.GetSessionVars().PartitionPruneMode.Load()
	analyzeSnapshot := sctx.GetSessionVars().EnableAnalyzeSnapshot
	optFuncs := []sqlexec.OptionFuncAlias{
		execOptionForAnalyze[statsVer],
		sqlexec.GetAnalyzeSnapshotOption(analyzeSnapshot),
		sqlexec.GetPartitionPruneModeOption(pruneMode),
		sqlexec.ExecOptionUseCurSession,
		sqlexec.ExecOptionWithSysProcTrack(opt.AutoAnalyzeProcIDGetter(), opt.SysProcTracker.Track, opt.SysProcTracker.UnTrack),
	}
	return statsutil.ExecWithOpts(sctx, optFuncs, sql, params...)
}

// insertAnalyzeJob inserts analyze job into mysql.analyze_jobs and gets job ID for further updating job.
func insertAnalyzeJob(sctx sessionctx.Context, job *statistics.AnalyzeJob, instance string, procID uint64) (err error) {
	jobInfo := job.JobInfo
	const textMaxLength = 65535
	if len(jobInfo) > textMaxLength {
		jobInfo = jobInfo[:textMaxLength]
	}
	const insertJob = "INSERT INTO mysql.analyze_jobs (table_schema, table_name, partition_name, job_info, state, instance, process_id) VALUES (%?, %?, %?, %?, %?, %?, %?)"
	_, _, err = statsutil.ExecRows(sctx, insertJob, job.DBName, job.TableName, job.PartitionName, jobInfo, statistics.AnalyzePending, instance, procID)
	if err != nil {
		return err
	}
	const getJobID = "SELECT LAST_INSERT_ID()"
	rows, _, err := statsutil.ExecRows(sctx, getJobID)
	if err != nil {
		return err
	}
	job.ID = new(uint64)
	*job.ID = rows[0].GetUint64(0)
	failpoint.Inject("DebugAnalyzeJobOperations", func(val failpoint.Value) {
		if val.(bool) {
			logutil.BgLogger().Info("InsertAnalyzeJob",
				zap.String("table_schema", job.DBName),
				zap.String("table_name", job.TableName),
				zap.String("partition_name", job.PartitionName),
				zap.String("job_info", jobInfo),
				zap.Uint64("job_id", *job.ID),
			)
		}
	})
	return nil
}
