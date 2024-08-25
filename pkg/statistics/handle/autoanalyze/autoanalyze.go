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
	"github.com/pingcap/tidb/pkg/statistics/handle/lockstats"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
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
	statsHandle statsutil.StatsHandle
}

// NewStatsAnalyze creates a new StatsAnalyze.
func NewStatsAnalyze(statsHandle statsutil.StatsHandle) statsutil.StatsAnalyze {
	return &statsAnalyze{statsHandle: statsHandle}
}

// InsertAnalyzeJob inserts the analyze job to the storage.
func (sa *statsAnalyze) InsertAnalyzeJob(job *statistics.AnalyzeJob, instance string, procID uint64) error {
	return statsutil.CallWithSCtx(sa.statsHandle.SPool(), func(sctx sessionctx.Context) error {
		return insertAnalyzeJob(sctx, job, instance, procID)
	})
}

// DeleteAnalyzeJobs deletes the analyze jobs whose update time is earlier than updateTime.
func (sa *statsAnalyze) DeleteAnalyzeJobs(updateTime time.Time) error {
	return statsutil.CallWithSCtx(sa.statsHandle.SPool(), func(sctx sessionctx.Context) error {
		_, _, err := statsutil.ExecRows(sctx, "DELETE FROM mysql.analyze_jobs WHERE update_time < CONVERT_TZ(%?, '+00:00', @@TIME_ZONE)", updateTime.UTC().Format(types.TimeFormat))
		return err
	})
}

// HandleAutoAnalyze analyzes the newly created table or index.
func (sa *statsAnalyze) HandleAutoAnalyze(is infoschema.InfoSchema) (analyzed bool) {
	_ = statsutil.CallWithSCtx(sa.statsHandle.SPool(), func(sctx sessionctx.Context) error {
		analyzed = HandleAutoAnalyze(sctx, sa.statsHandle, is)
		return nil
	})
	return
}

// CheckAnalyzeVersion checks whether all the statistics versions of this table's columns and indexes are the same.
func (sa *statsAnalyze) CheckAnalyzeVersion(tblInfo *model.TableInfo, physicalIDs []int64, version *int) bool {
	// We simply choose one physical id to get its stats.
	var tbl *statistics.Table
	for _, pid := range physicalIDs {
		tbl = sa.statsHandle.GetPartitionStats(tblInfo, pid)
		if !tbl.Pseudo {
			break
		}
	}
	if tbl == nil || tbl.Pseudo {
		return true
	}
	return statistics.CheckAnalyzeVerOnTable(tbl, version)
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

// HandleAutoAnalyze analyzes the newly created table or index.
func HandleAutoAnalyze(sctx sessionctx.Context,
	statsHandle statsutil.StatsHandle,
	is infoschema.InfoSchema) (analyzed bool) {
	defer func() {
		if r := recover(); r != nil {
			statslogutil.StatsLogger().Error(
				"HandleAutoAnalyze panicked",
				zap.Any("recover", r),
				zap.Stack("stack"),
			)
		}
	}()

	parameters := getAutoAnalyzeParameters(sctx)
	autoAnalyzeRatio := parseAutoAnalyzeRatio(parameters[variable.TiDBAutoAnalyzeRatio])
	start, end, err := parseAnalyzePeriod(parameters[variable.TiDBAutoAnalyzeStartTime], parameters[variable.TiDBAutoAnalyzeEndTime])
	if err != nil {
		statslogutil.StatsLogger().Error(
			"parse auto analyze period failed",
			zap.Error(err),
		)
		return false
	}
	if !timeutil.WithinDayTimePeriod(start, end, time.Now()) {
		return false
	}

	pruneMode := variable.PartitionPruneMode(sctx.GetSessionVars().PartitionPruneMode.Load())
	return RandomPickOneTableAndTryAutoAnalyze(
		sctx,
		statsHandle,
		is,
		autoAnalyzeRatio,
		pruneMode,
		start,
		end,
	)
}

// RandomPickOneTableAndTryAutoAnalyze randomly picks one table and tries to analyze it.
// 1. If the table is not analyzed, analyze it.
// 2. If the table is analyzed, analyze it when "tbl.ModifyCount/tbl.Count > autoAnalyzeRatio".
// 3. If the table is analyzed, analyze its indices when the index is not analyzed.
// 4. If the table is locked, skip it.
// Exposed solely for testing.
func RandomPickOneTableAndTryAutoAnalyze(
	sctx sessionctx.Context,
	statsHandle statsutil.StatsHandle,
	is infoschema.InfoSchema,
	autoAnalyzeRatio float64,
	pruneMode variable.PartitionPruneMode,
	start, end time.Time,
) bool {
	dbs := is.AllSchemaNames()
	// Shuffle the database and table slice to randomize the order of analyzing tables.
	rd := rand.New(rand.NewSource(time.Now().UnixNano())) // #nosec G404
	rd.Shuffle(len(dbs), func(i, j int) {
		dbs[i], dbs[j] = dbs[j], dbs[i]
	})
	// Query locked tables once to minimize overhead.
	// Outdated lock info is acceptable as we verify table lock status pre-analysis.
	lockedTables, err := lockstats.QueryLockedTables(sctx)
	if err != nil {
		statslogutil.StatsLogger().Error(
			"check table lock failed",
			zap.Error(err),
		)
		return false
	}

	for _, db := range dbs {
		// Ignore the memory and system database.
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
		for _, tbl := range tbls {
			// Sometimes the tables are too many. Auto-analyze will take too much time on it.
			// so we need to check the available time.
			if !timeutil.WithinDayTimePeriod(start, end, time.Now()) {
				return false
			}
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
			// No partitions, analyze the whole table.
			if pi == nil {
				statsTbl := statsHandle.GetTableStats(tblInfo)
				sql := "analyze table %n.%n"
				analyzed := autoAnalyzeTable(sctx, statsHandle, tblInfo, statsTbl, autoAnalyzeRatio, sql, db, tblInfo.Name.O)
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
				analyzed := autoAnalyzePartitionTableInDynamicMode(sctx, statsHandle, tblInfo, partitionDefs, db, autoAnalyzeRatio)
				if analyzed {
					return true
				}
				continue
			}
			for _, def := range partitionDefs {
				sql := "analyze table %n.%n partition %n"
				statsTbl := statsHandle.GetPartitionStats(tblInfo, def.ID)
				analyzed := autoAnalyzeTable(sctx, statsHandle, tblInfo, statsTbl, autoAnalyzeRatio, sql, db, tblInfo.Name.O, def.Name.O)
				if analyzed {
					return true
				}
			}
		}
	}

	return false
}

func autoAnalyzeTable(sctx sessionctx.Context,
	statsHandle statsutil.StatsHandle,
	tblInfo *model.TableInfo, statsTbl *statistics.Table,
	ratio float64, sql string, params ...interface{}) bool {
	if statsTbl.Pseudo || statsTbl.RealtimeCount < statistics.AutoAnalyzeMinCnt {
		return false
	}
	if needAnalyze, reason := NeedAnalyzeTable(statsTbl, 20*statsHandle.Lease(), ratio); needAnalyze {
		escaped, err := sqlexec.EscapeSQL(sql, params...)
		if err != nil {
			return false
		}
		statslogutil.StatsLogger().Info(
			"auto analyze triggered",
			zap.String("sql", escaped),
			zap.String("reason", reason),
		)

		tableStatsVer := sctx.GetSessionVars().AnalyzeVersion
		statistics.CheckAnalyzeVerOnTable(statsTbl, &tableStatsVer)
		execAutoAnalyze(sctx, statsHandle, tableStatsVer, sql, params...)
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

			statslogutil.StatsLogger().Info(
				"auto analyze for unanalyzed indexes",
				zap.String("sql", escaped),
			)
			tableStatsVer := sctx.GetSessionVars().AnalyzeVersion
			statistics.CheckAnalyzeVerOnTable(statsTbl, &tableStatsVer)
			execAutoAnalyze(sctx, statsHandle, tableStatsVer, sqlWithIdx, paramsWithIdx...)
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
	statsHandle statsutil.StatsHandle,
	tblInfo *model.TableInfo, partitionDefs []model.PartitionDefinition,
	db string, ratio float64) bool {
	tableStatsVer := sctx.GetSessionVars().AnalyzeVersion
	analyzePartitionBatchSize := int(variable.AutoAnalyzePartitionBatchSize.Load())
	partitionNames := make([]interface{}, 0, len(partitionDefs))
	for _, def := range partitionDefs {
		partitionStatsTbl := statsHandle.GetPartitionStats(tblInfo, def.ID)
		if partitionStatsTbl.Pseudo || partitionStatsTbl.RealtimeCount < statistics.AutoAnalyzeMinCnt {
			continue
		}
		if needAnalyze, reason := NeedAnalyzeTable(partitionStatsTbl, 20*statsHandle.Lease(), ratio); needAnalyze {
			partitionNames = append(partitionNames, def.Name.O)
			statslogutil.StatsLogger().Info(
				"need to auto analyze",
				zap.String("database", db),
				zap.String("table", tblInfo.Name.String()),
				zap.String("partition", def.Name.O),
				zap.String("reason", reason))
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
		statslogutil.StatsLogger().Info("start to auto analyze",
			zap.String("database", db),
			zap.String("table", tblInfo.Name.String()),
			zap.Any("partitions", partitionNames),
			zap.Int("analyze partition batch size", analyzePartitionBatchSize))
		statsTbl := statsHandle.GetTableStats(tblInfo)
		statistics.CheckAnalyzeVerOnTable(statsTbl, &tableStatsVer)
		for i := 0; i < len(partitionNames); i += analyzePartitionBatchSize {
			start := i
			end := start + analyzePartitionBatchSize
			if end >= len(partitionNames) {
				end = len(partitionNames)
			}
			sql := getSQL("analyze table %n.%n partition", "", end-start)
			params := append([]interface{}{db, tblInfo.Name.O}, partitionNames[start:end]...)
			statslogutil.StatsLogger().Info(
				"auto analyze triggered",
				zap.String("database", db),
				zap.String("table", tblInfo.Name.String()),
				zap.Any("partitions", partitionNames[start:end]))
			execAutoAnalyze(sctx, statsHandle, tableStatsVer, sql, params...)
		}
		return true
	}
	for _, idx := range tblInfo.Indices {
		if idx.State != model.StatePublic {
			continue
		}
		for _, def := range partitionDefs {
			partitionStatsTbl := statsHandle.GetPartitionStats(tblInfo, def.ID)
			if _, ok := partitionStatsTbl.Indices[idx.ID]; !ok {
				partitionNames = append(partitionNames, def.Name.O)
				statistics.CheckAnalyzeVerOnTable(partitionStatsTbl, &tableStatsVer)
			}
		}
		if len(partitionNames) > 0 {
			statsTbl := statsHandle.GetTableStats(tblInfo)
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
				statslogutil.StatsLogger().Info("auto analyze for unanalyzed",
					zap.String("database", db),
					zap.String("table", tblInfo.Name.String()),
					zap.String("index", idx.Name.String()),
					zap.Any("partitions", partitionNames[start:end]))
				execAutoAnalyze(sctx, statsHandle, tableStatsVer, sql, params...)
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
	statsHandle statsutil.StatsHandle,
	statsVer int,
	sql string, params ...interface{}) {
	startTime := time.Now()
	_, _, err := execAnalyzeStmt(sctx, statsHandle, statsVer, sql, params...)
	dur := time.Since(startTime)
	metrics.AutoAnalyzeHistogram.Observe(dur.Seconds())
	if err != nil {
		escaped, err1 := sqlexec.EscapeSQL(sql, params...)
		if err1 != nil {
			escaped = ""
		}
		statslogutil.StatsLogger().Error(
			"auto analyze failed",
			zap.String("sql", escaped),
			zap.Duration("cost_time", dur),
			zap.Error(err),
		)
		metrics.AutoAnalyzeCounter.WithLabelValues("failed").Inc()
	} else {
		metrics.AutoAnalyzeCounter.WithLabelValues("succ").Inc()
	}
}

func execAnalyzeStmt(sctx sessionctx.Context,
	statsHandle statsutil.StatsHandle,
	statsVer int,
	sql string, params ...interface{}) ([]chunk.Row, []*ast.ResultField, error) {
	pruneMode := sctx.GetSessionVars().PartitionPruneMode.Load()
	analyzeSnapshot := sctx.GetSessionVars().EnableAnalyzeSnapshot
	optFuncs := []sqlexec.OptionFuncAlias{
		execOptionForAnalyze[statsVer],
		sqlexec.GetAnalyzeSnapshotOption(analyzeSnapshot),
		sqlexec.GetPartitionPruneModeOption(pruneMode),
		sqlexec.ExecOptionUseCurSession,
		sqlexec.ExecOptionWithSysProcTrack(statsHandle.AutoAnalyzeProcID(), statsHandle.SysProcTracker().Track, statsHandle.SysProcTracker().UnTrack),
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
