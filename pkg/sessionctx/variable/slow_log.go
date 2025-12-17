// Copyright 2025 PingCAP, Inc.
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

package variable

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"hash/crc64"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx/slowlogrule"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/ppcpuusage"
	"github.com/tikv/client-go/v2/util"
)

const (
	// SlowLogRowPrefixStr is slow log row prefix.
	SlowLogRowPrefixStr = "# "
	// SlowLogSpaceMarkStr is slow log space mark.
	SlowLogSpaceMarkStr = ": "
	// SlowLogSQLSuffixStr is slow log suffix.
	SlowLogSQLSuffixStr = ";"
	// SlowLogTimeStr is slow log field name.
	SlowLogTimeStr = "Time"
	// SlowLogStartPrefixStr is slow log start row prefix.
	SlowLogStartPrefixStr = SlowLogRowPrefixStr + SlowLogTimeStr + SlowLogSpaceMarkStr
	// SlowLogTxnStartTSStr is slow log field name.
	SlowLogTxnStartTSStr = "Txn_start_ts"
	// SlowLogKeyspaceName is slow log field name.
	SlowLogKeyspaceName = "Keyspace_name"
	// SlowLogKeyspaceID is slow log field name.
	SlowLogKeyspaceID = "Keyspace_ID"
	// SlowLogUserAndHostStr is the user and host field name, which is compatible with MySQL.
	SlowLogUserAndHostStr = "User@Host"
	// SlowLogUserStr is slow log field name.
	SlowLogUserStr = "User"
	// SlowLogHostStr only for slow_query table usage.
	SlowLogHostStr = "Host"
	// SlowLogPreprocSubQueriesStr is the number of pre-processed sub-queries.
	SlowLogPreprocSubQueriesStr = "Preproc_subqueries"
	// SlowLogPreProcSubQueryTimeStr is the total time of pre-processing sub-queries.
	SlowLogPreProcSubQueryTimeStr = "Preproc_subqueries_time"

	// SlowLogIndexNamesStr is slow log field name.
	SlowLogIndexNamesStr = "Index_names"
	// SlowLogQuerySQLStr is slow log field name.
	SlowLogQuerySQLStr = "Query" // use for slow log table, slow log will not print this field name but print sql directly.
	// SlowLogStatsInfoStr is plan stats info.
	SlowLogStatsInfoStr = "Stats"
	// SlowLogCopProcAvg is the average process time of all cop-tasks.
	SlowLogCopProcAvg = "Cop_proc_avg"
	// SlowLogCopProcP90 is the p90 process time of all cop-tasks.
	SlowLogCopProcP90 = "Cop_proc_p90"
	// SlowLogCopProcMax is the max process time of all cop-tasks.
	SlowLogCopProcMax = "Cop_proc_max"
	// SlowLogCopProcAddr is the address of TiKV where the cop-task which cost max process time run.
	SlowLogCopProcAddr = "Cop_proc_addr"
	// SlowLogCopWaitAvg is the average wait time of all cop-tasks.
	SlowLogCopWaitAvg = "Cop_wait_avg" // #nosec G101
	// SlowLogCopWaitP90 is the p90 wait time of all cop-tasks.
	SlowLogCopWaitP90 = "Cop_wait_p90" // #nosec G101
	// SlowLogCopWaitMax is the max wait time of all cop-tasks.
	SlowLogCopWaitMax = "Cop_wait_max"
	// SlowLogCopWaitAddr is the address of TiKV where the cop-task which cost wait process time run.
	SlowLogCopWaitAddr = "Cop_wait_addr" // #nosec G101
	// SlowLogCopBackoffPrefix contains backoff information.
	SlowLogCopBackoffPrefix = "Cop_backoff_"
	// SlowLogPrepared is used to indicate whether this sql execute in prepare.
	SlowLogPrepared = "Prepared"
	// SlowLogPlanFromCache is used to indicate whether this plan is from plan cache.
	SlowLogPlanFromCache = "Plan_from_cache"
	// SlowLogPlanFromBinding is used to indicate whether this plan is matched with the hints in the binding.
	SlowLogPlanFromBinding = "Plan_from_binding"
	// SlowLogHasMoreResults is used to indicate whether this sql has more following results.
	SlowLogHasMoreResults = "Has_more_results"
	// SlowLogPrevStmt is used to show the previous executed statement.
	SlowLogPrevStmt = "Prev_stmt"
	// SlowLogPlan is used to record the query plan.
	SlowLogPlan = "Plan"
	// SlowLogBinaryPlan is used to record the binary plan.
	SlowLogBinaryPlan = "Binary_plan"
	// SlowLogPlanPrefix is the prefix of the plan value.
	SlowLogPlanPrefix = ast.TiDBDecodePlan + "('"
	// SlowLogBinaryPlanPrefix is the prefix of the binary plan value.
	SlowLogBinaryPlanPrefix = ast.TiDBDecodeBinaryPlan + "('"
	// SlowLogPlanSuffix is the suffix of the plan value.
	SlowLogPlanSuffix = "')"
	// SlowLogPrevStmtPrefix is the prefix of Prev_stmt in slow log file.
	SlowLogPrevStmtPrefix = SlowLogPrevStmt + SlowLogSpaceMarkStr
	// SlowLogBackoffTotal is the total time doing backoff.
	SlowLogBackoffTotal = "Backoff_total"
	// SlowLogExecRetryTime is the execution retry time.
	SlowLogExecRetryTime = "Exec_retry_time"
	// SlowLogBackoffDetail is the detail of backoff.
	SlowLogBackoffDetail = "Backoff_Detail"
	// SlowLogResultRows is the row count of the SQL result.
	SlowLogResultRows = "Result_rows"
	// SlowLogWarnings is the warnings generated during executing the statement.
	// Note that some extra warnings would also be printed through slow log.
	SlowLogWarnings = "Warnings"
	// SlowLogIsExplicitTxn is used to indicate whether this sql execute in explicit transaction or not.
	SlowLogIsExplicitTxn = "IsExplicitTxn"
	// SlowLogIsWriteCacheTable is used to indicate whether writing to the cache table need to wait for the read lock to expire.
	SlowLogIsWriteCacheTable = "IsWriteCacheTable"
	// SlowLogIsSyncStatsFailed is used to indicate whether any failure happen during sync stats
	SlowLogIsSyncStatsFailed = "IsSyncStatsFailed"
	// SlowLogRRU is the read request_unit(RU) cost
	SlowLogRRU = "Request_unit_read"
	// SlowLogWRU is the write request_unit(RU) cost
	SlowLogWRU = "Request_unit_write"
	// SlowLogWaitRUDuration is the total duration for kv requests to wait available request-units.
	SlowLogWaitRUDuration = "Time_queued_by_rc"
	// SlowLogTidbCPUUsageDuration is the total tidb cpu usages.
	SlowLogTidbCPUUsageDuration = "Tidb_cpu_time"
	// SlowLogTikvCPUUsageDuration is the total tikv cpu usages.
	SlowLogTikvCPUUsageDuration = "Tikv_cpu_time"
	// SlowLogStorageFromKV is used to indicate whether the statement read data from TiKV.
	SlowLogStorageFromKV = "Storage_from_kv"
	// SlowLogStorageFromMPP is used to indicate whether the statement read data from TiFlash.
	SlowLogStorageFromMPP = "Storage_from_mpp"

	// The following constants define the set of fields for SlowQueryLogItems
	// that are relevant to evaluating and triggering SlowLogRules.

	// SlowLogConnIDStr is slow log field name.
	SlowLogConnIDStr = "Conn_ID"
	// SlowLogSessAliasStr is the session alias set by user
	SlowLogSessAliasStr = "Session_alias"
	// SlowLogQueryTimeStr is slow log field name.
	SlowLogQueryTimeStr = "Query_time"
	// SlowLogParseTimeStr is the parse sql time.
	SlowLogParseTimeStr = "Parse_time"
	// SlowLogCompileTimeStr is the compile plan time.
	SlowLogCompileTimeStr = "Compile_time"
	// SlowLogRewriteTimeStr is the rewrite time.
	SlowLogRewriteTimeStr = "Rewrite_time"
	// SlowLogOptimizeTimeStr is the optimization time.
	SlowLogOptimizeTimeStr = "Optimize_time"
	// SlowLogWaitTSTimeStr is the time of waiting TS.
	SlowLogWaitTSTimeStr = "Wait_TS"
	// SlowLogDBStr is slow log field name.
	SlowLogDBStr = "DB"
	// SlowLogIsInternalStr is slow log field name.
	SlowLogIsInternalStr = "Is_internal"
	// SlowLogDigestStr is slow log field name.
	SlowLogDigestStr = "Digest"
	// SlowLogNumCopTasksStr is the number of cop-tasks.
	SlowLogNumCopTasksStr = "Num_cop_tasks"
	// SlowLogMemMax is the max number bytes of memory used in this statement.
	SlowLogMemMax = "Mem_max"
	// SlowLogMemArbitration is the total wait time(ns) of mem arbitration
	SlowLogMemArbitration = "Mem_arbitration"
	// SlowLogDiskMax is the max number bytes of disk used in this statement.
	SlowLogDiskMax = "Disk_max"
	// SlowLogKVTotal is the total time waiting for kv.
	SlowLogKVTotal = "KV_total"
	// SlowLogPDTotal is the total time waiting for pd.
	SlowLogPDTotal = "PD_total"
	// SlowLogUnpackedBytesSentTiKVTotal is the total bytes sent by tikv.
	SlowLogUnpackedBytesSentTiKVTotal = "Unpacked_bytes_sent_tikv_total"
	// SlowLogUnpackedBytesReceivedTiKVTotal is the total bytes received by tikv.
	SlowLogUnpackedBytesReceivedTiKVTotal = "Unpacked_bytes_received_tikv_total"
	// SlowLogUnpackedBytesSentTiKVCrossZone is the cross zone bytes sent by tikv.
	SlowLogUnpackedBytesSentTiKVCrossZone = "Unpacked_bytes_sent_tikv_cross_zone"
	// SlowLogUnpackedBytesReceivedTiKVCrossZone is the cross zone bytes received by tikv.
	SlowLogUnpackedBytesReceivedTiKVCrossZone = "Unpacked_bytes_received_tikv_cross_zone"
	// SlowLogUnpackedBytesSentTiFlashTotal is the total bytes sent by tiflash.
	SlowLogUnpackedBytesSentTiFlashTotal = "Unpacked_bytes_sent_tiflash_total"
	// SlowLogUnpackedBytesReceivedTiFlashTotal is the total bytes received by tiflash.
	SlowLogUnpackedBytesReceivedTiFlashTotal = "Unpacked_bytes_received_tiflash_total"
	// SlowLogUnpackedBytesSentTiFlashCrossZone is the cross zone bytes sent by tiflash.
	SlowLogUnpackedBytesSentTiFlashCrossZone = "Unpacked_bytes_sent_tiflash_cross_zone"
	// SlowLogUnpackedBytesReceivedTiFlashCrossZone is the cross zone bytes received by tiflash.
	SlowLogUnpackedBytesReceivedTiFlashCrossZone = "Unpacked_bytes_received_tiflash_cross_zone"
	// SlowLogWriteSQLRespTotal is the total time used to write response to client.
	SlowLogWriteSQLRespTotal = "Write_sql_response_total"
	// SlowLogSucc is used to indicate whether this sql execute successfully.
	SlowLogSucc = "Succ"
	// SlowLogPlanDigest is used to record the query plan digest.
	SlowLogPlanDigest = "Plan_digest"
	// SlowLogExecRetryCount is the execution retry count.
	SlowLogExecRetryCount = "Exec_retry_count"
	// SlowLogResourceGroup is the resource group name that the current session bind.
	SlowLogResourceGroup = "Resource_group"
)

// JSONSQLWarnForSlowLog helps to print the SQLWarn through the slow log in JSON format.
type JSONSQLWarnForSlowLog struct {
	Level   string
	Message string
	// IsExtra means this SQL Warn is expected to be recorded only under some conditions (like in EXPLAIN) and should
	// haven't been recorded as a warning now, but we recorded it anyway to help diagnostics.
	IsExtra bool `json:",omitempty"`
}

func extractMsgFromSQLWarn(sqlWarn *contextutil.SQLWarn) string {
	// Currently, this function is only used in collectWarningsForSlowLog.
	// collectWarningsForSlowLog can make sure SQLWarn is not nil so no need to add a nil check here.
	warn := errors.Cause(sqlWarn.Err)
	if x, ok := warn.(*terror.Error); ok && x != nil {
		sqlErr := terror.ToSQLError(x)
		return sqlErr.Message
	}
	return warn.Error()
}

// CollectWarningsForSlowLog collects warnings from the statement context and formats them for slow log output.
func CollectWarningsForSlowLog(stmtCtx *stmtctx.StatementContext) []JSONSQLWarnForSlowLog {
	warnings := stmtCtx.GetWarnings()
	extraWarnings := stmtCtx.GetExtraWarnings()
	res := make([]JSONSQLWarnForSlowLog, len(warnings)+len(extraWarnings))
	for i := range warnings {
		res[i].Level = warnings[i].Level
		res[i].Message = extractMsgFromSQLWarn(&warnings[i])
	}
	for i := range extraWarnings {
		res[len(warnings)+i].Level = extraWarnings[i].Level
		res[len(warnings)+i].Message = extractMsgFromSQLWarn(&extraWarnings[i])
		res[len(warnings)+i].IsExtra = true
	}
	return res
}

// SlowQueryLogItems is a collection of items that should be included in the
// slow query log.
type SlowQueryLogItems struct {
	TxnTS             uint64
	KeyspaceName      string
	KeyspaceID        uint32
	SQL               string
	Digest            string
	TimeTotal         time.Duration
	IndexNames        string
	Succ              bool
	IsExplicitTxn     bool
	IsWriteCacheTable bool
	IsSyncStatsFailed bool
	Prepared          bool
	// plan information
	PlanFromCache   bool
	PlanFromBinding bool
	HasMoreResults  bool
	PrevStmt        string
	Plan            string
	PlanDigest      string
	BinaryPlan      string
	// execution detail information
	UsedStats         *stmtctx.UsedStatsInfo
	CopTasks          *execdetails.CopTasksDetails
	RewriteInfo       RewritePhaseInfo
	WriteSQLRespTotal time.Duration
	KVExecDetail      *util.ExecDetails
	ExecDetail        *execdetails.ExecDetails
	ExecRetryCount    uint64
	ExecRetryTime     time.Duration
	ResultRows        int64
	Warnings          []JSONSQLWarnForSlowLog
	// resource information
	ResourceGroupName string
	RUDetails         *util.RUDetails
	MemMax            int64
	DiskMax           int64
	CPUUsages         ppcpuusage.CPUUsages
	StorageKV         bool // query read from TiKV
	StorageMPP        bool // query read from TiFlash
	MemArbitration    float64
}

const zeroStr = "0"

func kvExecDetailFormat(buf *bytes.Buffer, kvExecDetail *util.ExecDetails) {
	if kvExecDetail == nil {
		writeSlowLogItem(buf, SlowLogKVTotal, zeroStr)
		writeSlowLogItem(buf, SlowLogPDTotal, zeroStr)
		writeSlowLogItem(buf, SlowLogBackoffTotal, zeroStr)
		writeSlowLogItem(buf, SlowLogUnpackedBytesSentTiKVTotal, zeroStr)
		writeSlowLogItem(buf, SlowLogUnpackedBytesReceivedTiKVTotal, zeroStr)
		writeSlowLogItem(buf, SlowLogUnpackedBytesSentTiKVCrossZone, zeroStr)
		writeSlowLogItem(buf, SlowLogUnpackedBytesReceivedTiKVCrossZone, zeroStr)
		writeSlowLogItem(buf, SlowLogUnpackedBytesSentTiFlashTotal, zeroStr)
		writeSlowLogItem(buf, SlowLogUnpackedBytesReceivedTiFlashTotal, zeroStr)
		writeSlowLogItem(buf, SlowLogUnpackedBytesSentTiFlashCrossZone, zeroStr)
		writeSlowLogItem(buf, SlowLogUnpackedBytesReceivedTiFlashCrossZone, zeroStr)
		return
	}
	writeSlowLogItem(buf, SlowLogKVTotal, strconv.FormatFloat(time.Duration(kvExecDetail.WaitKVRespDuration).Seconds(), 'f', -1, 64))
	writeSlowLogItem(buf, SlowLogPDTotal, strconv.FormatFloat(time.Duration(kvExecDetail.WaitPDRespDuration).Seconds(), 'f', -1, 64))
	writeSlowLogItem(buf, SlowLogBackoffTotal, strconv.FormatFloat(time.Duration(kvExecDetail.BackoffDuration).Seconds(), 'f', -1, 64))
	writeSlowLogItem(buf, SlowLogUnpackedBytesSentTiKVTotal, strconv.FormatInt(kvExecDetail.UnpackedBytesSentKVTotal, 10))
	writeSlowLogItem(buf, SlowLogUnpackedBytesReceivedTiKVTotal, strconv.FormatInt(kvExecDetail.UnpackedBytesReceivedKVTotal, 10))
	writeSlowLogItem(buf, SlowLogUnpackedBytesSentTiKVCrossZone, strconv.FormatInt(kvExecDetail.UnpackedBytesSentKVCrossZone, 10))
	writeSlowLogItem(buf, SlowLogUnpackedBytesReceivedTiKVCrossZone, strconv.FormatInt(kvExecDetail.UnpackedBytesReceivedKVCrossZone, 10))
	writeSlowLogItem(buf, SlowLogUnpackedBytesSentTiFlashTotal, strconv.FormatInt(kvExecDetail.UnpackedBytesSentMPPTotal, 10))
	writeSlowLogItem(buf, SlowLogUnpackedBytesReceivedTiFlashTotal, strconv.FormatInt(kvExecDetail.UnpackedBytesReceivedMPPTotal, 10))
	writeSlowLogItem(buf, SlowLogUnpackedBytesSentTiFlashCrossZone, strconv.FormatInt(kvExecDetail.UnpackedBytesSentMPPCrossZone, 10))
	writeSlowLogItem(buf, SlowLogUnpackedBytesReceivedTiFlashCrossZone, strconv.FormatInt(kvExecDetail.UnpackedBytesReceivedMPPCrossZone, 10))
}

// SlowLogFormat uses for formatting slow log.
// The slow log output is like below:
// # Time: 2019-04-28T15:24:04.309074+08:00
// # Txn_start_ts: 406315658548871171
// # Keyspace_name: keyspace_a
// # Keyspace_ID: 1
// # User@Host: root[root] @ localhost [127.0.0.1]
// # Conn_ID: 6
// # Query_time: 4.895492
// # Process_time: 0.161 Request_count: 1 Total_keys: 100001 Processed_keys: 100000
// # DB: test
// # Index_names: [t1.idx1,t2.idx2]
// # Is_internal: false
// # Digest: 42a1c8aae6f133e934d4bf0147491709a8812ea05ff8819ec522780fe657b772
// # Stats: t1:1,t2:2
// # Num_cop_tasks: 10
// # Cop_process: Avg_time: 1s P90_time: 2s Max_time: 3s Max_addr: 10.6.131.78
// # Cop_wait: Avg_time: 10ms P90_time: 20ms Max_time: 30ms Max_Addr: 10.6.131.79
// # Memory_max: 4096
// # Disk_max: 65535
// # Succ: true
// # Prev_stmt: begin;
// select * from t_slim;
func (s *SessionVars) SlowLogFormat(logItems *SlowQueryLogItems) string {
	var buf bytes.Buffer

	writeSlowLogItem(&buf, SlowLogTxnStartTSStr, strconv.FormatUint(logItems.TxnTS, 10))
	if logItems.KeyspaceName != "" {
		writeSlowLogItem(&buf, SlowLogKeyspaceName, logItems.KeyspaceName)
		writeSlowLogItem(&buf, SlowLogKeyspaceID, fmt.Sprintf("%d", logItems.KeyspaceID))
	}

	if s.User != nil {
		hostAddress := s.User.Hostname
		if s.ConnectionInfo != nil {
			hostAddress = s.ConnectionInfo.ClientIP
		}
		writeSlowLogItem(&buf, SlowLogUserAndHostStr, fmt.Sprintf("%s[%s] @ %s [%s]", s.User.Username, s.User.Username, s.User.Hostname, hostAddress))
	}
	if s.ConnectionID != 0 {
		writeSlowLogItem(&buf, SlowLogConnIDStr, strconv.FormatUint(s.ConnectionID, 10))
	}
	if s.SessionAlias != "" {
		writeSlowLogItem(&buf, SlowLogSessAliasStr, s.SessionAlias)
	}
	if logItems.ExecRetryCount > 0 {
		buf.WriteString(SlowLogRowPrefixStr)
		buf.WriteString(SlowLogExecRetryTime)
		buf.WriteString(SlowLogSpaceMarkStr)
		buf.WriteString(strconv.FormatFloat(logItems.ExecRetryTime.Seconds(), 'f', -1, 64))
		buf.WriteString(" ")
		buf.WriteString(SlowLogExecRetryCount)
		buf.WriteString(SlowLogSpaceMarkStr)
		buf.WriteString(strconv.Itoa(int(logItems.ExecRetryCount)))
		buf.WriteString("\n")
	}
	writeSlowLogItem(&buf, SlowLogQueryTimeStr, strconv.FormatFloat(logItems.TimeTotal.Seconds(), 'f', -1, 64))
	writeSlowLogItem(&buf, SlowLogParseTimeStr, strconv.FormatFloat(s.DurationParse.Seconds(), 'f', -1, 64))
	writeSlowLogItem(&buf, SlowLogCompileTimeStr, strconv.FormatFloat(s.DurationCompile.Seconds(), 'f', -1, 64))

	buf.WriteString(SlowLogRowPrefixStr + fmt.Sprintf("%v%v%v", SlowLogRewriteTimeStr,
		SlowLogSpaceMarkStr, strconv.FormatFloat(logItems.RewriteInfo.DurationRewrite.Seconds(), 'f', -1, 64)))
	if logItems.RewriteInfo.PreprocessSubQueries > 0 {
		buf.WriteString(fmt.Sprintf(" %v%v%v %v%v%v", SlowLogPreprocSubQueriesStr, SlowLogSpaceMarkStr, logItems.RewriteInfo.PreprocessSubQueries,
			SlowLogPreProcSubQueryTimeStr, SlowLogSpaceMarkStr, strconv.FormatFloat(logItems.RewriteInfo.DurationPreprocessSubQuery.Seconds(), 'f', -1, 64)))
	}
	buf.WriteString("\n")

	writeSlowLogItem(&buf, SlowLogOptimizeTimeStr, strconv.FormatFloat(s.DurationOptimization.Seconds(), 'f', -1, 64))
	writeSlowLogItem(&buf, SlowLogWaitTSTimeStr, strconv.FormatFloat(s.DurationWaitTS.Seconds(), 'f', -1, 64))

	if execDetailStr := logItems.ExecDetail.String(); len(execDetailStr) > 0 {
		buf.WriteString(SlowLogRowPrefixStr + execDetailStr + "\n")
	}

	if len(s.CurrentDB) > 0 {
		writeSlowLogItem(&buf, SlowLogDBStr, strings.ToLower(s.CurrentDB))
	}
	if len(logItems.IndexNames) > 0 {
		writeSlowLogItem(&buf, SlowLogIndexNamesStr, logItems.IndexNames)
	}

	writeSlowLogItem(&buf, SlowLogIsInternalStr, strconv.FormatBool(s.InRestrictedSQL))
	if len(logItems.Digest) > 0 {
		writeSlowLogItem(&buf, SlowLogDigestStr, logItems.Digest)
	}
	keys := logItems.UsedStats.Keys()
	if len(keys) > 0 {
		buf.WriteString(SlowLogRowPrefixStr + SlowLogStatsInfoStr + SlowLogSpaceMarkStr)
		firstComma := false
		slices.Sort(keys)
		for _, id := range keys {
			usedStatsForTbl := logItems.UsedStats.GetUsedInfo(id)
			if usedStatsForTbl == nil {
				continue
			}
			if firstComma {
				buf.WriteString(",")
			}
			usedStatsForTbl.WriteToSlowLog(&buf)
			firstComma = true
		}

		buf.WriteString("\n")
	}
	if logItems.CopTasks != nil {
		writeSlowLogItem(&buf, SlowLogNumCopTasksStr, strconv.FormatInt(int64(logItems.CopTasks.NumCopTasks), 10))
		if logItems.CopTasks.NumCopTasks > 0 {
			// make the result stable
			backoffs := make([]string, 0, 3)
			for backoff := range logItems.CopTasks.TotBackoffTimes {
				backoffs = append(backoffs, backoff)
			}
			slices.Sort(backoffs)

			taskNum := logItems.CopTasks.NumCopTasks
			buf.WriteString(SlowLogRowPrefixStr +
				logItems.CopTasks.ProcessTimeStats.String(taskNum, SlowLogSpaceMarkStr, SlowLogCopProcAvg, SlowLogCopProcP90, SlowLogCopProcMax, SlowLogCopProcAddr) + "\n")
			buf.WriteString(SlowLogRowPrefixStr +
				logItems.CopTasks.WaitTimeStats.String(taskNum, SlowLogSpaceMarkStr, SlowLogCopWaitAvg, SlowLogCopWaitP90, SlowLogCopWaitMax, SlowLogCopWaitAddr) + "\n")

			if taskNum == 1 {
				for _, backoff := range backoffs {
					backoffPrefix := SlowLogCopBackoffPrefix + backoff + "_"
					buf.WriteString(SlowLogRowPrefixStr + fmt.Sprintf("%v%v%v %v%v%v\n",
						backoffPrefix+"total_times", SlowLogSpaceMarkStr, logItems.CopTasks.TotBackoffTimes[backoff],
						backoffPrefix+"total_time", SlowLogSpaceMarkStr, logItems.CopTasks.BackoffTimeStatsMap[backoff].TotTime.Seconds(),
					))
				}
			} else {
				for _, backoff := range backoffs {
					backoffPrefix := SlowLogCopBackoffPrefix + backoff + "_"
					backoffTimeStats := logItems.CopTasks.BackoffTimeStatsMap[backoff]
					buf.WriteString(SlowLogRowPrefixStr + fmt.Sprintf("%v%v%v %v%v%v %v%v%v %v%v%v %v%v%v %v%v%v\n",
						backoffPrefix+"total_times", SlowLogSpaceMarkStr, logItems.CopTasks.TotBackoffTimes[backoff],
						backoffPrefix+"total_time", SlowLogSpaceMarkStr, backoffTimeStats.TotTime.Seconds(),
						backoffPrefix+"max_time", SlowLogSpaceMarkStr, backoffTimeStats.MaxTime.Seconds(),
						backoffPrefix+"max_addr", SlowLogSpaceMarkStr, backoffTimeStats.MaxAddress,
						backoffPrefix+"avg_time", SlowLogSpaceMarkStr, backoffTimeStats.AvgTime.Seconds(),
						backoffPrefix+"p90_time", SlowLogSpaceMarkStr, backoffTimeStats.P90Time.Seconds(),
					))
				}
			}
		}
	}
	if logItems.MemMax > 0 {
		writeSlowLogItem(&buf, SlowLogMemMax, strconv.FormatInt(logItems.MemMax, 10))
	}
	if logItems.MemArbitration > 0 {
		writeSlowLogItem(&buf, SlowLogMemArbitration, strconv.FormatFloat(logItems.MemArbitration, 'f', -1, 64))
	}
	if logItems.DiskMax > 0 {
		writeSlowLogItem(&buf, SlowLogDiskMax, strconv.FormatInt(logItems.DiskMax, 10))
	}

	writeSlowLogItem(&buf, SlowLogPrepared, strconv.FormatBool(logItems.Prepared))
	writeSlowLogItem(&buf, SlowLogPlanFromCache, strconv.FormatBool(logItems.PlanFromCache))
	writeSlowLogItem(&buf, SlowLogPlanFromBinding, strconv.FormatBool(logItems.PlanFromBinding))
	writeSlowLogItem(&buf, SlowLogHasMoreResults, strconv.FormatBool(logItems.HasMoreResults))
	kvExecDetailFormat(&buf, logItems.KVExecDetail)
	writeSlowLogItem(&buf, SlowLogWriteSQLRespTotal, strconv.FormatFloat(logItems.WriteSQLRespTotal.Seconds(), 'f', -1, 64))
	writeSlowLogItem(&buf, SlowLogResultRows, strconv.FormatInt(logItems.ResultRows, 10))
	if len(logItems.Warnings) > 0 {
		buf.WriteString(SlowLogRowPrefixStr + SlowLogWarnings + SlowLogSpaceMarkStr)
		jsonEncoder := json.NewEncoder(&buf)
		jsonEncoder.SetEscapeHTML(false)
		// Note that the Encode() will append a '\n' so we don't need to add another.
		err := jsonEncoder.Encode(logItems.Warnings)
		if err != nil {
			buf.WriteString(err.Error())
		}
	}
	writeSlowLogItem(&buf, SlowLogSucc, strconv.FormatBool(logItems.Succ))
	writeSlowLogItem(&buf, SlowLogIsExplicitTxn, strconv.FormatBool(logItems.IsExplicitTxn))
	writeSlowLogItem(&buf, SlowLogIsSyncStatsFailed, strconv.FormatBool(logItems.IsSyncStatsFailed))
	if s.StmtCtx.WaitLockLeaseTime > 0 {
		writeSlowLogItem(&buf, SlowLogIsWriteCacheTable, strconv.FormatBool(logItems.IsWriteCacheTable))
	}
	if len(logItems.Plan) != 0 {
		writeSlowLogItem(&buf, SlowLogPlan, logItems.Plan)
	}
	if len(logItems.PlanDigest) != 0 {
		writeSlowLogItem(&buf, SlowLogPlanDigest, logItems.PlanDigest)
	}
	if len(logItems.BinaryPlan) != 0 {
		writeSlowLogItem(&buf, SlowLogBinaryPlan, logItems.BinaryPlan)
	}

	if logItems.ResourceGroupName != "" {
		writeSlowLogItem(&buf, SlowLogResourceGroup, logItems.ResourceGroupName)
	}
	if rru := logItems.RUDetails.RRU(); rru > 0.0 {
		writeSlowLogItem(&buf, SlowLogRRU, strconv.FormatFloat(rru, 'f', -1, 64))
	}
	if wru := logItems.RUDetails.WRU(); wru > 0.0 {
		writeSlowLogItem(&buf, SlowLogWRU, strconv.FormatFloat(wru, 'f', -1, 64))
	}
	if waitRUDuration := logItems.RUDetails.RUWaitDuration(); waitRUDuration > time.Duration(0) {
		writeSlowLogItem(&buf, SlowLogWaitRUDuration, strconv.FormatFloat(waitRUDuration.Seconds(), 'f', -1, 64))
	}
	if logItems.CPUUsages.TidbCPUTime > time.Duration(0) {
		writeSlowLogItem(&buf, SlowLogTidbCPUUsageDuration, strconv.FormatFloat(logItems.CPUUsages.TidbCPUTime.Seconds(), 'f', -1, 64))
	}
	if logItems.CPUUsages.TikvCPUTime > time.Duration(0) {
		writeSlowLogItem(&buf, SlowLogTikvCPUUsageDuration, strconv.FormatFloat(logItems.CPUUsages.TikvCPUTime.Seconds(), 'f', -1, 64))
	}
	writeSlowLogItem(&buf, SlowLogStorageFromKV, strconv.FormatBool(logItems.StorageKV))
	writeSlowLogItem(&buf, SlowLogStorageFromMPP, strconv.FormatBool(logItems.StorageMPP))
	if logItems.PrevStmt != "" {
		writeSlowLogItem(&buf, SlowLogPrevStmt, logItems.PrevStmt)
	}

	if s.CurrentDBChanged {
		buf.WriteString(fmt.Sprintf("use %s;\n", strings.ToLower(s.CurrentDB)))
		s.CurrentDBChanged = false
	}

	buf.WriteString(logItems.SQL)
	if len(logItems.SQL) == 0 || logItems.SQL[len(logItems.SQL)-1] != ';' {
		buf.WriteString(";")
	}

	return buf.String()
}

// writeSlowLogItem writes a slow log item in the form of: "# ${key}:${value}"
func writeSlowLogItem(buf *bytes.Buffer, key, value string) {
	buf.WriteString(SlowLogRowPrefixStr + key + SlowLogSpaceMarkStr + value + "\n")
}

// SlowLogFieldAccessor defines how to get or set a specific field in SlowQueryLogItems.
// - Parse converts a user-provided threshold string into the proper type for comparison.
// - Setter is optional and pre-fills the field before matching if it needs explicit preparation.
// - Match evaluates whether the field in SlowQueryLogItems meets a specific threshold.
//   - threshold is the value to compare against when determining a match.
type SlowLogFieldAccessor struct {
	Parse  func(string) (any, error)
	Setter func(ctx context.Context, seVars *SessionVars, items *SlowQueryLogItems)
	Match  func(seVars *SessionVars, items *SlowQueryLogItems, threshold any) bool
}

func makeExecDetailAccessor(parse func(string) (any, error),
	match func(*execdetails.ExecDetails, any) bool) SlowLogFieldAccessor {
	return SlowLogFieldAccessor{
		Parse: parse,
		Setter: func(_ context.Context, seVars *SessionVars, items *SlowQueryLogItems) {
			if items.ExecDetail == nil {
				execDetail := seVars.StmtCtx.GetExecDetails()
				items.ExecDetail = &execDetail
			}
		},
		Match: func(_ *SessionVars, items *SlowQueryLogItems, threshold any) bool {
			if items.ExecDetail == nil {
				return matchZero(threshold)
			}
			return match(items.ExecDetail, threshold)
		},
	}
}

func makeKVExecDetailAccessor(parse func(string) (any, error),
	match func(*util.ExecDetails, any) bool) SlowLogFieldAccessor {
	return SlowLogFieldAccessor{
		Parse: parse,
		Setter: func(ctx context.Context, _ *SessionVars, items *SlowQueryLogItems) {
			if items.KVExecDetail == nil {
				tikvExecDetailRaw := ctx.Value(util.ExecDetailsKey)
				if tikvExecDetailRaw != nil {
					items.KVExecDetail = tikvExecDetailRaw.(*util.ExecDetails)
				}
			}
		},
		Match: func(_ *SessionVars, items *SlowQueryLogItems, threshold any) bool {
			if items.KVExecDetail == nil {
				return matchZero(threshold)
			}
			return match(items.KVExecDetail, threshold)
		},
	}
}

// numericComparable defines a set of numeric types that support ordering operations (like >=).
type numericComparable interface {
	~int | ~int64 | ~uint64 | ~float64
}

// MatchEqual compares a value `v` with a threshold and returns true if they are equal.
func MatchEqual[T comparable](threshold any, v T) bool {
	tv, ok := threshold.(T)
	return ok && v == tv
}

func matchGE[T numericComparable](threshold any, v T) bool {
	tv, ok := threshold.(T)
	return ok && v >= tv
}

func matchZero(threshold any) bool {
	switch v := threshold.(type) {
	case int:
		return v == 0
	case uint64:
		return v == 0
	case int64:
		return v == 0
	case float64:
		return v == 0
	default:
		return false
	}
}

// ParseString converts the input string to lowercase and returns it.
func ParseString(v string) (any, error)  { return v, nil }
func parseInt64(v string) (any, error)   { return strconv.ParseInt(v, 10, 64) }
func parseUint64(v string) (any, error)  { return strconv.ParseUint(v, 10, 64) }
func parseFloat64(v string) (any, error) { return strconv.ParseFloat(v, 64) }
func parseBool(v string) (any, error)    { return strconv.ParseBool(v) }

// SlowLogRuleFieldAccessors defines the set of field accessors for SlowQueryLogItems
// that are relevant to evaluating and triggering SlowLogRules.
// It's exporting for testing.
var SlowLogRuleFieldAccessors = map[string]SlowLogFieldAccessor{
	strings.ToLower(SlowLogConnIDStr): {
		Parse: parseUint64,
		Match: func(seVars *SessionVars, _ *SlowQueryLogItems, threshold any) bool {
			return matchGE(threshold, seVars.ConnectionID)
		},
	},
	strings.ToLower(SlowLogSessAliasStr): {
		Parse: ParseString,
		Match: func(seVars *SessionVars, _ *SlowQueryLogItems, threshold any) bool {
			return MatchEqual(threshold, seVars.SessionAlias)
		},
	},
	strings.ToLower(SlowLogDBStr): {
		Parse: ParseString,
		Match: func(seVars *SessionVars, _ *SlowQueryLogItems, threshold any) bool {
			return MatchEqual(strings.ToLower(threshold.(string)), strings.ToLower(seVars.CurrentDB))
		},
	},
	strings.ToLower(SlowLogExecRetryCount): {
		Parse: parseUint64,
		Setter: func(_ context.Context, seVars *SessionVars, items *SlowQueryLogItems) {
			items.ExecRetryCount = seVars.StmtCtx.ExecRetryCount
		},
		Match: func(_ *SessionVars, items *SlowQueryLogItems, threshold any) bool {
			return matchGE(threshold, items.ExecRetryCount)
		},
	},
	strings.ToLower(SlowLogQueryTimeStr): {
		Parse: parseFloat64,
		Setter: func(_ context.Context, seVars *SessionVars, items *SlowQueryLogItems) {
			items.TimeTotal = seVars.GetTotalCostDuration()
		},
		Match: func(_ *SessionVars, items *SlowQueryLogItems, threshold any) bool {
			return matchGE(threshold, items.TimeTotal.Seconds())
		},
	},
	strings.ToLower(SlowLogParseTimeStr): {
		Parse: parseFloat64,
		Match: func(seVars *SessionVars, _ *SlowQueryLogItems, threshold any) bool {
			return matchGE(threshold, seVars.DurationParse.Seconds())
		},
	},
	strings.ToLower(SlowLogCompileTimeStr): {
		Parse: parseFloat64,
		Match: func(seVars *SessionVars, _ *SlowQueryLogItems, threshold any) bool {
			return matchGE(threshold, seVars.DurationCompile.Seconds())
		},
	},
	strings.ToLower(SlowLogRewriteTimeStr): {
		Parse: parseFloat64,
		Setter: func(_ context.Context, seVars *SessionVars, items *SlowQueryLogItems) {
			items.RewriteInfo = seVars.RewritePhaseInfo
		},
		Match: func(_ *SessionVars, items *SlowQueryLogItems, threshold any) bool {
			return matchGE(threshold, items.RewriteInfo.DurationRewrite.Seconds())
		},
	},
	strings.ToLower(SlowLogOptimizeTimeStr): {
		Parse: parseFloat64,
		Match: func(seVars *SessionVars, _ *SlowQueryLogItems, threshold any) bool {
			return matchGE(threshold, seVars.DurationOptimization.Seconds())
		},
	},
	strings.ToLower(SlowLogWaitTSTimeStr): {
		Parse: parseFloat64,
		Match: func(seVars *SessionVars, _ *SlowQueryLogItems, threshold any) bool {
			return matchGE(threshold, seVars.DurationWaitTS.Seconds())
		},
	},
	strings.ToLower(SlowLogIsInternalStr): {
		Parse: parseBool,
		Match: func(seVars *SessionVars, _ *SlowQueryLogItems, threshold any) bool {
			return MatchEqual(threshold, seVars.InRestrictedSQL)
		},
	},
	strings.ToLower(SlowLogDigestStr): {
		Parse: ParseString,
		Setter: func(_ context.Context, seVars *SessionVars, items *SlowQueryLogItems) {
			_, digest := seVars.StmtCtx.SQLDigest()
			items.Digest = digest.String()
		},
		Match: func(_ *SessionVars, items *SlowQueryLogItems, threshold any) bool {
			return MatchEqual(threshold, items.Digest)
		},
	},
	strings.ToLower(SlowLogNumCopTasksStr): {
		Parse: parseInt64,
		Setter: func(_ context.Context, seVars *SessionVars, items *SlowQueryLogItems) {
			copTasksDetail := seVars.StmtCtx.CopTasksDetails()
			items.CopTasks = copTasksDetail
		},
		Match: func(_ *SessionVars, items *SlowQueryLogItems, threshold any) bool {
			if items.CopTasks == nil {
				return matchZero(threshold)
			}
			return matchGE(threshold, int64(items.CopTasks.NumCopTasks))
		},
	},
	strings.ToLower(SlowLogMemMax): {
		Parse: parseInt64,
		Setter: func(_ context.Context, seVars *SessionVars, items *SlowQueryLogItems) {
			items.MemMax = seVars.MemTracker.MaxConsumed()
		},
		Match: func(_ *SessionVars, items *SlowQueryLogItems, threshold any) bool {
			return matchGE(threshold, items.MemMax)
		},
	},
	strings.ToLower(SlowLogMemArbitration): {
		Parse: parseFloat64,
		Setter: func(_ context.Context, seVars *SessionVars, items *SlowQueryLogItems) {
			items.MemArbitration = seVars.StmtCtx.MemTracker.MemArbitration().Seconds()
		},
		Match: func(_ *SessionVars, items *SlowQueryLogItems, threshold any) bool {
			return matchGE(threshold, items.MemArbitration)
		},
	},
	strings.ToLower(SlowLogDiskMax): {
		Parse: parseInt64,
		Setter: func(_ context.Context, seVars *SessionVars, items *SlowQueryLogItems) {
			items.DiskMax = seVars.DiskTracker.MaxConsumed()
		},
		Match: func(_ *SessionVars, items *SlowQueryLogItems, threshold any) bool {
			return matchGE(threshold, items.DiskMax)
		},
	},
	strings.ToLower(SlowLogWriteSQLRespTotal): {
		Parse: parseFloat64,
		Setter: func(ctx context.Context, _ *SessionVars, items *SlowQueryLogItems) {
			stmtDetailRaw := ctx.Value(execdetails.StmtExecDetailKey)
			if stmtDetailRaw != nil {
				stmtDetail := *(stmtDetailRaw.(*execdetails.StmtExecDetails))
				items.WriteSQLRespTotal = stmtDetail.WriteSQLRespDuration
			}
		},
		Match: func(_ *SessionVars, items *SlowQueryLogItems, threshold any) bool {
			return matchGE(threshold, items.WriteSQLRespTotal.Seconds())
		},
	},
	strings.ToLower(SlowLogSucc): {
		Parse: parseBool,
		Setter: func(_ context.Context, seVars *SessionVars, items *SlowQueryLogItems) {
			items.Succ = seVars.StmtCtx.ExecSuccess
		},
		Match: func(_ *SessionVars, items *SlowQueryLogItems, threshold any) bool {
			return MatchEqual(threshold, items.Succ)
		},
	},
	strings.ToLower(SlowLogResourceGroup): {
		Parse: ParseString,
		Setter: func(_ context.Context, seVars *SessionVars, items *SlowQueryLogItems) {
			items.ResourceGroupName = seVars.StmtCtx.ResourceGroupName
		},
		Match: func(_ *SessionVars, items *SlowQueryLogItems, threshold any) bool {
			return MatchEqual(strings.ToLower(threshold.(string)), strings.ToLower(items.ResourceGroupName))
		},
	},
	// The following fields are related to util.ExecDetails.
	strings.ToLower(SlowLogKVTotal): makeKVExecDetailAccessor(
		parseFloat64,
		func(d *util.ExecDetails, threshold any) bool {
			return matchGE(threshold, time.Duration(d.WaitKVRespDuration).Seconds())
		},
	),
	strings.ToLower(SlowLogPDTotal): makeKVExecDetailAccessor(
		parseFloat64,
		func(d *util.ExecDetails, threshold any) bool {
			return matchGE(threshold, time.Duration(d.WaitPDRespDuration).Seconds())
		},
	),
	strings.ToLower(SlowLogUnpackedBytesSentTiKVTotal): makeKVExecDetailAccessor(
		parseInt64,
		func(d *util.ExecDetails, threshold any) bool {
			return matchGE(threshold, d.UnpackedBytesSentKVTotal)
		},
	),
	strings.ToLower(SlowLogUnpackedBytesReceivedTiKVTotal): makeKVExecDetailAccessor(
		parseInt64,
		func(d *util.ExecDetails, threshold any) bool {
			return matchGE(threshold, d.UnpackedBytesReceivedKVTotal)
		},
	),
	strings.ToLower(SlowLogUnpackedBytesSentTiKVCrossZone): makeKVExecDetailAccessor(
		parseInt64,
		func(d *util.ExecDetails, threshold any) bool {
			return matchGE(threshold, d.UnpackedBytesSentKVCrossZone)
		},
	),
	strings.ToLower(SlowLogUnpackedBytesReceivedTiKVCrossZone): makeKVExecDetailAccessor(
		parseInt64,
		func(d *util.ExecDetails, threshold any) bool {
			return matchGE(threshold, d.UnpackedBytesReceivedKVCrossZone)
		},
	),
	strings.ToLower(SlowLogUnpackedBytesSentTiFlashTotal): makeKVExecDetailAccessor(
		parseInt64,
		func(d *util.ExecDetails, threshold any) bool {
			return matchGE(threshold, d.UnpackedBytesSentMPPTotal)
		},
	),
	strings.ToLower(SlowLogUnpackedBytesReceivedTiFlashTotal): makeKVExecDetailAccessor(
		parseInt64,
		func(d *util.ExecDetails, threshold any) bool {
			return matchGE(threshold, d.UnpackedBytesReceivedMPPTotal)
		},
	),
	strings.ToLower(SlowLogUnpackedBytesSentTiFlashCrossZone): makeKVExecDetailAccessor(
		parseInt64,
		func(d *util.ExecDetails, threshold any) bool {
			return matchGE(threshold, d.UnpackedBytesSentMPPCrossZone)
		},
	),
	strings.ToLower(SlowLogUnpackedBytesReceivedTiFlashCrossZone): makeKVExecDetailAccessor(
		parseInt64,
		func(d *util.ExecDetails, threshold any) bool {
			return matchGE(threshold, d.UnpackedBytesReceivedMPPCrossZone)
		},
	),
	// The following fields are related to execdetails.ExecDetails.
	strings.ToLower(execdetails.ProcessTimeStr): makeExecDetailAccessor(
		parseFloat64,
		func(d *execdetails.ExecDetails, threshold any) bool {
			return matchGE(threshold, d.TimeDetail.ProcessTime.Seconds())
		}),
	strings.ToLower(execdetails.BackoffTimeStr): makeExecDetailAccessor(
		parseFloat64,
		func(d *execdetails.ExecDetails, threshold any) bool {
			return matchGE(threshold, d.BackoffTime.Seconds())
		}),
	strings.ToLower(execdetails.TotalKeysStr): makeExecDetailAccessor(
		parseUint64,
		func(d *execdetails.ExecDetails, threshold any) bool {
			if d.ScanDetail == nil {
				return matchZero(threshold)
			}
			return matchGE(threshold, d.ScanDetail.TotalKeys)
		}),
	strings.ToLower(execdetails.ProcessKeysStr): makeExecDetailAccessor(
		parseUint64,
		func(d *execdetails.ExecDetails, threshold any) bool {
			if d.ScanDetail == nil {
				return matchZero(threshold)
			}
			return matchGE(threshold, d.ScanDetail.ProcessedKeys)
		}),
	strings.ToLower(execdetails.PreWriteTimeStr): makeExecDetailAccessor(
		parseFloat64,
		func(d *execdetails.ExecDetails, threshold any) bool {
			if d.CommitDetail == nil {
				return matchZero(threshold)
			}
			return matchGE(threshold, d.CommitDetail.PrewriteTime.Seconds())
		}),
	strings.ToLower(execdetails.CommitTimeStr): makeExecDetailAccessor(
		parseFloat64,
		func(d *execdetails.ExecDetails, threshold any) bool {
			if d.CommitDetail == nil {
				return matchZero(threshold)
			}
			return matchGE(threshold, d.CommitDetail.CommitTime.Seconds())
		}),
	strings.ToLower(execdetails.WriteKeysStr): makeExecDetailAccessor(
		parseUint64,
		func(d *execdetails.ExecDetails, threshold any) bool {
			if d.CommitDetail == nil {
				return matchZero(threshold)
			}
			return matchGE(threshold, int64(d.CommitDetail.WriteKeys))
		}),
	strings.ToLower(execdetails.WriteSizeStr): makeExecDetailAccessor(
		parseUint64,
		func(d *execdetails.ExecDetails, threshold any) bool {
			if d.CommitDetail == nil {
				return matchZero(threshold)
			}
			return matchGE(threshold, int64(d.CommitDetail.WriteSize))
		}),
	strings.ToLower(execdetails.PrewriteRegionStr): makeExecDetailAccessor(
		parseUint64,
		func(d *execdetails.ExecDetails, threshold any) bool {
			if d.CommitDetail == nil {
				return matchZero(threshold)
			}
			return matchGE(threshold, int64(atomic.LoadInt32(&d.CommitDetail.PrewriteRegionNum)))
		}),
}

// slowLogFieldRe is uses to compile field:value
var slowLogFieldRe = regexp.MustCompile(`\s*(\w+)\s*:\s*([^,]+)\s*`)

// UnsetConnID is a sentinel value (-1) for slow log rules without an explicit connection binding.
//
// Semantics:
//   - Session scope: represents the current session.
//   - Global scope: means no specific connection ID is set, i.e. the rule applies globally.
const UnsetConnID = int64(-1)

// ParseSlowLogFieldValue is exporting for testing.
func ParseSlowLogFieldValue(fieldName string, value string) (any, error) {
	parser, ok := SlowLogRuleFieldAccessors[strings.ToLower(fieldName)]
	if !ok {
		return nil, errors.Errorf("unknown slow log field name:%s", fieldName)
	}

	return parser.Parse(value)
}

func parseSlowLogRuleEntry(rawRule string, allowConnID bool) (int64, *slowlogrule.SlowLogRule, error) {
	connID := UnsetConnID
	rawRule = strings.TrimSpace(rawRule)
	if rawRule == "" {
		return connID, nil, nil
	}

	matches := slowLogFieldRe.FindAllStringSubmatch(rawRule, -1)
	if len(matches) == 0 {
		return connID, nil, fmt.Errorf("invalid slow log rule format:%s", rawRule)
	}
	fieldMap := make(map[string]any, len(matches))
	for _, match := range matches {
		if len(match) != 3 {
			return connID, nil, errors.Errorf("invalid slow log condition format:%s", match)
		}

		fieldName := strings.ToLower(strings.TrimSpace(match[1]))
		value := strings.TrimSpace(match[2])
		fieldValue, err := ParseSlowLogFieldValue(fieldName, strings.Trim(value, "\"'"))
		if err != nil {
			return connID, nil, errors.Errorf("invalid slow log format, value:%s, err:%s", value, err)
		}

		if strings.EqualFold(fieldName, SlowLogConnIDStr) {
			if !allowConnID {
				return connID, nil, errors.Errorf("do not allow ConnID value:%s", value)
			}

			connID = int64(fieldValue.(uint64))
		}

		fieldMap[fieldName] = fieldValue
	}

	slowLogRule := &slowlogrule.SlowLogRule{Conditions: make([]slowlogrule.SlowLogCondition, 0, len(fieldMap))}
	for fieldName, fieldValue := range fieldMap {
		slowLogRule.Conditions = append(slowLogRule.Conditions, slowlogrule.SlowLogCondition{
			Field:     fieldName,
			Threshold: fieldValue,
		})
	}

	return connID, slowLogRule, nil
}

// parseSlowLogRuleSet parses a raw slow log rules string into a map keyed by ConnID.
// Input format:
//   - Multiple rules are separated by semicolons (';').
//   - Inside each rule, fields are expressed as key:value pairs, separated by commas (',').
//   - Example: "field1:val1,field2:val2;field3:val3"
//
// Behavior:
//   - Returns a map where the key is ConnID, and the value is a set of rules for that ConnID.
//   - UnsetConnID (-1) is used for rules not bound to a specific connection.
//   - If allowConnID is false, rules containing an explicit ConnID will be rejected.
func parseSlowLogRuleSet(rawRules string, allowConnID bool) (map[int64]*slowlogrule.SlowLogRules, error) {
	rawRules = strings.TrimSpace(rawRules)
	if rawRules == "" {
		return nil, nil
	}
	rules := strings.Split(rawRules, ";")
	if len(rules) > 10 {
		return nil, errors.Errorf("invalid slow log rules count:%d, limit is 10", len(rules))
	}

	result := make(map[int64]*slowlogrule.SlowLogRules)
	for _, raw := range rules {
		connID, slowLogRule, err := parseSlowLogRuleEntry(raw, allowConnID)
		if err != nil {
			return nil, err
		}
		if slowLogRule == nil {
			continue
		}

		slowLogRules, ok := result[connID]
		if !ok {
			slowLogRules = &slowlogrule.SlowLogRules{
				Fields: make(map[string]struct{}),
				Rules:  make([]*slowlogrule.SlowLogRule, 0, len(rules)),
			}
			result[connID] = slowLogRules
		}
		for _, cond := range slowLogRule.Conditions {
			slowLogRules.Fields[cond.Field] = struct{}{}
		}
		slowLogRules.Rules = append(slowLogRules.Rules, slowLogRule)
	}
	return result, nil
}

// ParseSessionSlowLogRules parses raw rules into the default (UnsetConnID) slow log rules.
// Returns nil if no rules for UnsetConnID are found.
func ParseSessionSlowLogRules(rawRules string) (*slowlogrule.SlowLogRules, error) {
	globalRules, err := parseSlowLogRuleSet(rawRules, false)
	if err != nil {
		return nil, err
	}
	if globalRules == nil || globalRules[UnsetConnID] == nil {
		return nil, nil
	}

	globalRules[UnsetConnID].RawRules = encodeRules(globalRules[UnsetConnID])

	return globalRules[UnsetConnID], nil
}

func encodeRules(rules *slowlogrule.SlowLogRules) string {
	if rules == nil || len(rules.Rules) == 0 {
		return ""
	}

	var strB strings.Builder
	for i, rule := range rules.Rules {
		for j, cond := range rule.Conditions {
			if j > 0 {
				strB.WriteByte(',')
			}
			strB.WriteString(cond.Field)
			strB.WriteByte(':')
			strB.WriteString(fmt.Sprintf("%v", cond.Threshold))
		}

		if i < len(rules.Rules)-1 {
			strB.WriteByte(';')
		}
	}

	return strB.String()
}

var crc64Table = crc64.MakeTable(crc64.ECMA)

// ParseGlobalSlowLogRules parses raw rules and constructs a GlobalSlowLogRules object.
// The result contains both the raw string and the rules map keyed by ConnID.
// allowConnID = true is used here to support both ConnID-bound and default rules.
func ParseGlobalSlowLogRules(rawRules string) (*slowlogrule.GlobalSlowLogRules, error) {
	rulesMap, err := parseSlowLogRuleSet(rawRules, true)
	if err != nil {
		return nil, err
	}

	if rulesMap == nil {
		rulesMap = make(map[int64]*slowlogrule.SlowLogRules)
	}

	rawSlice := make([]string, 0, len(rulesMap))
	for _, rules := range rulesMap {
		rawSlice = append(rawSlice, encodeRules(rules))
	}

	rawRules = strings.Join(rawSlice, ";")
	return &slowlogrule.GlobalSlowLogRules{
		RawRules:     rawRules,
		RawRulesHash: crc64.Checksum([]byte(rawRules), crc64Table),
		RulesMap:     rulesMap,
	}, nil
}
