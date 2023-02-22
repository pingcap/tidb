// Copyright 2018 PingCAP, Inc.
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

package execdetails

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/tipb/go-tipb"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

// ExecDetails contains execution detail information.
type ExecDetails struct {
	DetailsNeedP90
	CommitDetail     *util.CommitDetails
	LockKeysDetail   *util.LockKeysDetails
	ScanDetail       *util.ScanDetail
	CopTime          time.Duration
	BackoffTime      time.Duration
	LockKeysDuration time.Duration
	RequestCount     int
}

// DetailsNeedP90 contains execution detail information which need calculate P90.
type DetailsNeedP90 struct {
	BackoffSleep  map[string]time.Duration
	BackoffTimes  map[string]int
	CalleeAddress string
	TimeDetail    util.TimeDetail
}

type stmtExecDetailKeyType struct{}

// StmtExecDetailKey used to carry StmtExecDetail info in context.Context.
var StmtExecDetailKey = stmtExecDetailKeyType{}

// StmtExecDetails contains stmt level execution detail info.
type StmtExecDetails struct {
	WriteSQLRespDuration time.Duration
}

const (
	// CopTimeStr represents the sum of cop-task time spend in TiDB distSQL.
	CopTimeStr = "Cop_time"
	// ProcessTimeStr represents the sum of process time of all the coprocessor tasks.
	ProcessTimeStr = "Process_time"
	// WaitTimeStr means the time of all coprocessor wait.
	WaitTimeStr = "Wait_time"
	// BackoffTimeStr means the time of all back-off.
	BackoffTimeStr = "Backoff_time"
	// LockKeysTimeStr means the time interval between pessimistic lock wait start and lock got obtain
	LockKeysTimeStr = "LockKeys_time"
	// RequestCountStr means the request count.
	RequestCountStr = "Request_count"
	// TotalKeysStr means the total scan keys.
	TotalKeysStr = "Total_keys"
	// ProcessKeysStr means the total processed keys.
	ProcessKeysStr = "Process_keys"
	// PreWriteTimeStr means the time of pre-write.
	PreWriteTimeStr = "Prewrite_time"
	// WaitPrewriteBinlogTimeStr means the time of waiting prewrite binlog finished when transaction committing.
	WaitPrewriteBinlogTimeStr = "Wait_prewrite_binlog_time"
	// CommitTimeStr means the time of commit.
	CommitTimeStr = "Commit_time"
	// GetCommitTSTimeStr means the time of getting commit ts.
	GetCommitTSTimeStr = "Get_commit_ts_time"
	// GetLatestTsTimeStr means the time of getting latest ts in async commit and 1pc.
	GetLatestTsTimeStr = "Get_latest_ts_time"
	// CommitBackoffTimeStr means the time of commit backoff.
	CommitBackoffTimeStr = "Commit_backoff_time"
	// BackoffTypesStr means the backoff type.
	BackoffTypesStr = "Backoff_types"
	// SlowestPrewriteRPCDetailStr means the details of the slowest RPC during the transaction 2pc prewrite process.
	SlowestPrewriteRPCDetailStr = "Slowest_prewrite_rpc_detail"
	// CommitPrimaryRPCDetailStr means the details of the slowest RPC during the transaction 2pc commit process.
	CommitPrimaryRPCDetailStr = "Commit_primary_rpc_detail"
	// ResolveLockTimeStr means the time of resolving lock.
	ResolveLockTimeStr = "Resolve_lock_time"
	// LocalLatchWaitTimeStr means the time of waiting in local latch.
	LocalLatchWaitTimeStr = "Local_latch_wait_time"
	// WriteKeysStr means the count of keys in the transaction.
	WriteKeysStr = "Write_keys"
	// WriteSizeStr means the key/value size in the transaction.
	WriteSizeStr = "Write_size"
	// PrewriteRegionStr means the count of region when pre-write.
	PrewriteRegionStr = "Prewrite_region"
	// TxnRetryStr means the count of transaction retry.
	TxnRetryStr = "Txn_retry"
	// GetSnapshotTimeStr means the time spent on getting an engine snapshot.
	GetSnapshotTimeStr = "Get_snapshot_time"
	// RocksdbDeleteSkippedCountStr means the count of rocksdb delete skipped count.
	RocksdbDeleteSkippedCountStr = "Rocksdb_delete_skipped_count"
	// RocksdbKeySkippedCountStr means the count of rocksdb key skipped count.
	RocksdbKeySkippedCountStr = "Rocksdb_key_skipped_count"
	// RocksdbBlockCacheHitCountStr means the count of rocksdb block cache hit.
	RocksdbBlockCacheHitCountStr = "Rocksdb_block_cache_hit_count"
	// RocksdbBlockReadCountStr means the count of rocksdb block read.
	RocksdbBlockReadCountStr = "Rocksdb_block_read_count"
	// RocksdbBlockReadByteStr means the bytes of rocksdb block read.
	RocksdbBlockReadByteStr = "Rocksdb_block_read_byte"
	// RocksdbBlockReadTimeStr means the time spent on rocksdb block read.
	RocksdbBlockReadTimeStr = "Rocksdb_block_read_time"
)

// String implements the fmt.Stringer interface.
func (d ExecDetails) String() string {
	parts := make([]string, 0, 8)
	if d.CopTime > 0 {
		parts = append(parts, CopTimeStr+": "+strconv.FormatFloat(d.CopTime.Seconds(), 'f', -1, 64))
	}
	if d.TimeDetail.ProcessTime > 0 {
		parts = append(parts, ProcessTimeStr+": "+strconv.FormatFloat(d.TimeDetail.ProcessTime.Seconds(), 'f', -1, 64))
	}
	if d.TimeDetail.WaitTime > 0 {
		parts = append(parts, WaitTimeStr+": "+strconv.FormatFloat(d.TimeDetail.WaitTime.Seconds(), 'f', -1, 64))
	}
	if d.BackoffTime > 0 {
		parts = append(parts, BackoffTimeStr+": "+strconv.FormatFloat(d.BackoffTime.Seconds(), 'f', -1, 64))
	}
	if d.LockKeysDuration > 0 {
		parts = append(parts, LockKeysTimeStr+": "+strconv.FormatFloat(d.LockKeysDuration.Seconds(), 'f', -1, 64))
	}
	if d.RequestCount > 0 {
		parts = append(parts, RequestCountStr+": "+strconv.FormatInt(int64(d.RequestCount), 10))
	}
	commitDetails := d.CommitDetail
	if commitDetails != nil {
		if commitDetails.PrewriteTime > 0 {
			parts = append(parts, PreWriteTimeStr+": "+strconv.FormatFloat(commitDetails.PrewriteTime.Seconds(), 'f', -1, 64))
		}
		if commitDetails.WaitPrewriteBinlogTime > 0 {
			parts = append(parts, WaitPrewriteBinlogTimeStr+": "+strconv.FormatFloat(commitDetails.WaitPrewriteBinlogTime.Seconds(), 'f', -1, 64))
		}
		if commitDetails.CommitTime > 0 {
			parts = append(parts, CommitTimeStr+": "+strconv.FormatFloat(commitDetails.CommitTime.Seconds(), 'f', -1, 64))
		}
		if commitDetails.GetCommitTsTime > 0 {
			parts = append(parts, GetCommitTSTimeStr+": "+strconv.FormatFloat(commitDetails.GetCommitTsTime.Seconds(), 'f', -1, 64))
		}
		if commitDetails.GetLatestTsTime > 0 {
			parts = append(parts, GetLatestTsTimeStr+": "+strconv.FormatFloat(commitDetails.GetLatestTsTime.Seconds(), 'f', -1, 64))
		}
		commitDetails.Mu.Lock()
		commitBackoffTime := commitDetails.Mu.CommitBackoffTime
		if commitBackoffTime > 0 {
			parts = append(parts, CommitBackoffTimeStr+": "+strconv.FormatFloat(time.Duration(commitBackoffTime).Seconds(), 'f', -1, 64))
		}
		if len(commitDetails.Mu.PrewriteBackoffTypes) > 0 {
			parts = append(parts, "Prewrite_"+BackoffTypesStr+": "+fmt.Sprintf("%v", commitDetails.Mu.PrewriteBackoffTypes))
		}
		if len(commitDetails.Mu.CommitBackoffTypes) > 0 {
			parts = append(parts, "Commit_"+BackoffTypesStr+": "+fmt.Sprintf("%v", commitDetails.Mu.CommitBackoffTypes))
		}
		if commitDetails.Mu.SlowestPrewrite.ReqTotalTime > 0 {
			parts = append(parts, SlowestPrewriteRPCDetailStr+": {total:"+strconv.FormatFloat(commitDetails.Mu.SlowestPrewrite.ReqTotalTime.Seconds(), 'f', 3, 64)+
				"s, region_id: "+strconv.FormatUint(commitDetails.Mu.SlowestPrewrite.Region, 10)+
				", store: "+commitDetails.Mu.SlowestPrewrite.StoreAddr+
				", "+commitDetails.Mu.SlowestPrewrite.ExecDetails.String()+"}")
		}
		if commitDetails.Mu.CommitPrimary.ReqTotalTime > 0 {
			parts = append(parts, CommitPrimaryRPCDetailStr+": {total:"+strconv.FormatFloat(commitDetails.Mu.SlowestPrewrite.ReqTotalTime.Seconds(), 'f', 3, 64)+
				"s, region_id: "+strconv.FormatUint(commitDetails.Mu.SlowestPrewrite.Region, 10)+
				", store: "+commitDetails.Mu.SlowestPrewrite.StoreAddr+
				", "+commitDetails.Mu.SlowestPrewrite.ExecDetails.String()+"}")
		}
		commitDetails.Mu.Unlock()
		resolveLockTime := atomic.LoadInt64(&commitDetails.ResolveLock.ResolveLockTime)
		if resolveLockTime > 0 {
			parts = append(parts, ResolveLockTimeStr+": "+strconv.FormatFloat(time.Duration(resolveLockTime).Seconds(), 'f', -1, 64))
		}
		if commitDetails.LocalLatchTime > 0 {
			parts = append(parts, LocalLatchWaitTimeStr+": "+strconv.FormatFloat(commitDetails.LocalLatchTime.Seconds(), 'f', -1, 64))
		}
		if commitDetails.WriteKeys > 0 {
			parts = append(parts, WriteKeysStr+": "+strconv.FormatInt(int64(commitDetails.WriteKeys), 10))
		}
		if commitDetails.WriteSize > 0 {
			parts = append(parts, WriteSizeStr+": "+strconv.FormatInt(int64(commitDetails.WriteSize), 10))
		}
		prewriteRegionNum := atomic.LoadInt32(&commitDetails.PrewriteRegionNum)
		if prewriteRegionNum > 0 {
			parts = append(parts, PrewriteRegionStr+": "+strconv.FormatInt(int64(prewriteRegionNum), 10))
		}
		if commitDetails.TxnRetry > 0 {
			parts = append(parts, TxnRetryStr+": "+strconv.FormatInt(int64(commitDetails.TxnRetry), 10))
		}
	}
	scanDetail := d.ScanDetail
	if scanDetail != nil {
		if scanDetail.ProcessedKeys > 0 {
			parts = append(parts, ProcessKeysStr+": "+strconv.FormatInt(scanDetail.ProcessedKeys, 10))
		}
		if scanDetail.TotalKeys > 0 {
			parts = append(parts, TotalKeysStr+": "+strconv.FormatInt(scanDetail.TotalKeys, 10))
		}
		if scanDetail.GetSnapshotDuration > 0 {
			parts = append(parts, GetSnapshotTimeStr+": "+strconv.FormatFloat(scanDetail.GetSnapshotDuration.Seconds(), 'f', 3, 64))
		}
		if scanDetail.RocksdbDeleteSkippedCount > 0 {
			parts = append(parts, RocksdbDeleteSkippedCountStr+": "+strconv.FormatUint(scanDetail.RocksdbDeleteSkippedCount, 10))
		}
		if scanDetail.RocksdbKeySkippedCount > 0 {
			parts = append(parts, RocksdbKeySkippedCountStr+": "+strconv.FormatUint(scanDetail.RocksdbKeySkippedCount, 10))
		}
		if scanDetail.RocksdbBlockCacheHitCount > 0 {
			parts = append(parts, RocksdbBlockCacheHitCountStr+": "+strconv.FormatUint(scanDetail.RocksdbBlockCacheHitCount, 10))
		}
		if scanDetail.RocksdbBlockReadCount > 0 {
			parts = append(parts, RocksdbBlockReadCountStr+": "+strconv.FormatUint(scanDetail.RocksdbBlockReadCount, 10))
		}
		if scanDetail.RocksdbBlockReadByte > 0 {
			parts = append(parts, RocksdbBlockReadByteStr+": "+strconv.FormatUint(scanDetail.RocksdbBlockReadByte, 10))
		}
		if scanDetail.RocksdbBlockReadDuration > 0 {
			parts = append(parts, RocksdbBlockReadTimeStr+": "+strconv.FormatFloat(scanDetail.RocksdbBlockReadDuration.Seconds(), 'f', 3, 64))
		}
	}
	return strings.Join(parts, " ")
}

// ToZapFields wraps the ExecDetails as zap.Fields.
func (d ExecDetails) ToZapFields() (fields []zap.Field) {
	fields = make([]zap.Field, 0, 16)
	if d.CopTime > 0 {
		fields = append(fields, zap.String(strings.ToLower(CopTimeStr), strconv.FormatFloat(d.CopTime.Seconds(), 'f', -1, 64)+"s"))
	}
	if d.TimeDetail.ProcessTime > 0 {
		fields = append(fields, zap.String(strings.ToLower(ProcessTimeStr), strconv.FormatFloat(d.TimeDetail.ProcessTime.Seconds(), 'f', -1, 64)+"s"))
	}
	if d.TimeDetail.WaitTime > 0 {
		fields = append(fields, zap.String(strings.ToLower(WaitTimeStr), strconv.FormatFloat(d.TimeDetail.WaitTime.Seconds(), 'f', -1, 64)+"s"))
	}
	if d.BackoffTime > 0 {
		fields = append(fields, zap.String(strings.ToLower(BackoffTimeStr), strconv.FormatFloat(d.BackoffTime.Seconds(), 'f', -1, 64)+"s"))
	}
	if d.RequestCount > 0 {
		fields = append(fields, zap.String(strings.ToLower(RequestCountStr), strconv.FormatInt(int64(d.RequestCount), 10)))
	}
	if d.ScanDetail != nil && d.ScanDetail.TotalKeys > 0 {
		fields = append(fields, zap.String(strings.ToLower(TotalKeysStr), strconv.FormatInt(d.ScanDetail.TotalKeys, 10)))
	}
	if d.ScanDetail != nil && d.ScanDetail.ProcessedKeys > 0 {
		fields = append(fields, zap.String(strings.ToLower(ProcessKeysStr), strconv.FormatInt(d.ScanDetail.ProcessedKeys, 10)))
	}
	commitDetails := d.CommitDetail
	if commitDetails != nil {
		if commitDetails.PrewriteTime > 0 {
			fields = append(fields, zap.String("prewrite_time", fmt.Sprintf("%v", strconv.FormatFloat(commitDetails.PrewriteTime.Seconds(), 'f', -1, 64)+"s")))
		}
		if commitDetails.CommitTime > 0 {
			fields = append(fields, zap.String("commit_time", fmt.Sprintf("%v", strconv.FormatFloat(commitDetails.CommitTime.Seconds(), 'f', -1, 64)+"s")))
		}
		if commitDetails.GetCommitTsTime > 0 {
			fields = append(fields, zap.String("get_commit_ts_time", fmt.Sprintf("%v", strconv.FormatFloat(commitDetails.GetCommitTsTime.Seconds(), 'f', -1, 64)+"s")))
		}
		commitDetails.Mu.Lock()
		commitBackoffTime := commitDetails.Mu.CommitBackoffTime
		if commitBackoffTime > 0 {
			fields = append(fields, zap.String("commit_backoff_time", fmt.Sprintf("%v", strconv.FormatFloat(time.Duration(commitBackoffTime).Seconds(), 'f', -1, 64)+"s")))
		}
		if len(commitDetails.Mu.PrewriteBackoffTypes) > 0 {
			fields = append(fields, zap.String("Prewrite_"+BackoffTypesStr, fmt.Sprintf("%v", commitDetails.Mu.PrewriteBackoffTypes)))
		}
		if len(commitDetails.Mu.CommitBackoffTypes) > 0 {
			fields = append(fields, zap.String("Commit_"+BackoffTypesStr, fmt.Sprintf("%v", commitDetails.Mu.CommitBackoffTypes)))
		}
		if commitDetails.Mu.SlowestPrewrite.ReqTotalTime > 0 {
			fields = append(fields, zap.String(SlowestPrewriteRPCDetailStr, "total:"+strconv.FormatFloat(commitDetails.Mu.SlowestPrewrite.ReqTotalTime.Seconds(), 'f', 3, 64)+
				"s, region_id: "+strconv.FormatUint(commitDetails.Mu.SlowestPrewrite.Region, 10)+
				", store: "+commitDetails.Mu.SlowestPrewrite.StoreAddr+
				", "+commitDetails.Mu.SlowestPrewrite.ExecDetails.String()+"}"))
		}
		if commitDetails.Mu.CommitPrimary.ReqTotalTime > 0 {
			fields = append(fields, zap.String(CommitPrimaryRPCDetailStr, "{total:"+strconv.FormatFloat(commitDetails.Mu.SlowestPrewrite.ReqTotalTime.Seconds(), 'f', 3, 64)+
				"s, region_id: "+strconv.FormatUint(commitDetails.Mu.SlowestPrewrite.Region, 10)+
				", store: "+commitDetails.Mu.SlowestPrewrite.StoreAddr+
				", "+commitDetails.Mu.SlowestPrewrite.ExecDetails.String()+"}"))
		}
		commitDetails.Mu.Unlock()
		resolveLockTime := atomic.LoadInt64(&commitDetails.ResolveLock.ResolveLockTime)
		if resolveLockTime > 0 {
			fields = append(fields, zap.String("resolve_lock_time", fmt.Sprintf("%v", strconv.FormatFloat(time.Duration(resolveLockTime).Seconds(), 'f', -1, 64)+"s")))
		}
		if commitDetails.LocalLatchTime > 0 {
			fields = append(fields, zap.String("local_latch_wait_time", fmt.Sprintf("%v", strconv.FormatFloat(commitDetails.LocalLatchTime.Seconds(), 'f', -1, 64)+"s")))
		}
		if commitDetails.WriteKeys > 0 {
			fields = append(fields, zap.Int("write_keys", commitDetails.WriteKeys))
		}
		if commitDetails.WriteSize > 0 {
			fields = append(fields, zap.Int("write_size", commitDetails.WriteSize))
		}
		prewriteRegionNum := atomic.LoadInt32(&commitDetails.PrewriteRegionNum)
		if prewriteRegionNum > 0 {
			fields = append(fields, zap.Int32("prewrite_region", prewriteRegionNum))
		}
		if commitDetails.TxnRetry > 0 {
			fields = append(fields, zap.Int("txn_retry", commitDetails.TxnRetry))
		}
	}
	return fields
}

type basicCopRuntimeStats struct {
	storeType string
	BasicRuntimeStats
	threads    int32
	totalTasks int32
	procTimes  []time.Duration
}

// String implements the RuntimeStats interface.
func (e *basicCopRuntimeStats) String() string {
	if e.storeType == "tiflash" {
		return fmt.Sprintf("time:%v, loops:%d, threads:%d, ", FormatDuration(time.Duration(e.consume)), e.loop, e.threads) + e.BasicRuntimeStats.tiflashScanContext.String()
	}
	return fmt.Sprintf("time:%v, loops:%d", FormatDuration(time.Duration(e.consume)), e.loop)
}

// Clone implements the RuntimeStats interface.
func (e *basicCopRuntimeStats) Clone() RuntimeStats {
	return &basicCopRuntimeStats{
		BasicRuntimeStats: BasicRuntimeStats{loop: e.loop, consume: e.consume, rows: e.rows, tiflashScanContext: e.tiflashScanContext.Clone()},
		threads:           e.threads,
		storeType:         e.storeType,
		totalTasks:        e.totalTasks,
		procTimes:         e.procTimes,
	}
}

// Merge implements the RuntimeStats interface.
func (e *basicCopRuntimeStats) Merge(rs RuntimeStats) {
	tmp, ok := rs.(*basicCopRuntimeStats)
	if !ok {
		return
	}
	e.loop += tmp.loop
	e.consume += tmp.consume
	e.rows += tmp.rows
	e.threads += tmp.threads
	e.totalTasks += tmp.totalTasks
	if len(tmp.procTimes) > 0 {
		e.procTimes = append(e.procTimes, tmp.procTimes...)
	} else {
		e.procTimes = append(e.procTimes, time.Duration(tmp.consume))
	}
	e.tiflashScanContext.Merge(tmp.tiflashScanContext)
}

// Tp implements the RuntimeStats interface.
func (*basicCopRuntimeStats) Tp() int {
	return TpBasicCopRunTimeStats
}

// CopRuntimeStats collects cop tasks' execution info.
type CopRuntimeStats struct {
	// stats stores the runtime statistics of coprocessor tasks.
	// The key of the map is the tikv-server address. Because a tikv-server can
	// have many region leaders, several coprocessor tasks can be sent to the
	// same tikv-server instance. We have to use a list to maintain all tasks
	// executed on each instance.
	stats      map[string]*basicCopRuntimeStats
	scanDetail *util.ScanDetail
	// do not use kv.StoreType because it will meet cycle import error
	storeType string
	sync.Mutex
}

// RecordOneCopTask records a specific cop tasks's execution detail.
func (crs *CopRuntimeStats) RecordOneCopTask(address string, summary *tipb.ExecutorExecutionSummary) {
	crs.Lock()
	defer crs.Unlock()

	if crs.stats[address] == nil {
		crs.stats[address] = &basicCopRuntimeStats{
			storeType: crs.storeType,
		}
	}
	crs.stats[address].Merge(&basicCopRuntimeStats{
		storeType: crs.storeType,
		BasicRuntimeStats: BasicRuntimeStats{loop: int32(*summary.NumIterations),
			consume: int64(*summary.TimeProcessedNs),
			rows:    int64(*summary.NumProducedRows),
			tiflashScanContext: TiFlashScanContext{
				totalDmfileScannedPacks:            summary.GetTiflashScanContext().GetTotalDmfileScannedPacks(),
				totalDmfileSkippedPacks:            summary.GetTiflashScanContext().GetTotalDmfileSkippedPacks(),
				totalDmfileScannedRows:             summary.GetTiflashScanContext().GetTotalDmfileScannedRows(),
				totalDmfileSkippedRows:             summary.GetTiflashScanContext().GetTotalDmfileSkippedRows(),
				totalDmfileRoughSetIndexLoadTimeMs: summary.GetTiflashScanContext().GetTotalDmfileRoughSetIndexLoadTimeMs(),
				totalDmfileReadTimeMs:              summary.GetTiflashScanContext().GetTotalDmfileReadTimeMs(),
				totalCreateSnapshotTimeMs:          summary.GetTiflashScanContext().GetTotalCreateSnapshotTimeMs(),
				totalLocalRegionNum:                summary.GetTiflashScanContext().GetTotalLocalRegionNum(),
				totalRemoteRegionNum:               summary.GetTiflashScanContext().GetTotalRemoteRegionNum()}}, threads: int32(summary.GetConcurrency()),
		totalTasks: 1,
	})
}

// GetActRows return total rows of CopRuntimeStats.
func (crs *CopRuntimeStats) GetActRows() (totalRows int64) {
	for _, instanceStats := range crs.stats {
		totalRows += instanceStats.rows
	}
	return totalRows
}

// MergeBasicStats traverses basicCopRuntimeStats in the CopRuntimeStats and collects some useful information.
func (crs *CopRuntimeStats) MergeBasicStats() (procTimes []time.Duration, totalTime time.Duration, totalTasks, totalLoops, totalThreads int32, totalTiFlashScanContext TiFlashScanContext) {
	procTimes = make([]time.Duration, 0, 32)
	totalTiFlashScanContext = TiFlashScanContext{}
	for _, instanceStats := range crs.stats {
		procTimes = append(procTimes, instanceStats.procTimes...)
		totalTime += time.Duration(instanceStats.consume)
		totalLoops += instanceStats.loop
		totalThreads += instanceStats.threads
		totalTiFlashScanContext.Merge(instanceStats.tiflashScanContext)
		totalTasks += instanceStats.totalTasks
	}
	return
}

func (crs *CopRuntimeStats) String() string {
	if len(crs.stats) == 0 {
		return ""
	}

	procTimes, totalTime, totalTasks, totalLoops, totalThreads, totalTiFlashScanContext := crs.MergeBasicStats()
	avgTime := time.Duration(totalTime.Nanoseconds() / int64(totalTasks))
	isTiFlashCop := crs.storeType == "tiflash"

	buf := bytes.NewBuffer(make([]byte, 0, 16))
	if totalTasks == 1 {
		buf.WriteString(fmt.Sprintf("%v_task:{time:%v, loops:%d", crs.storeType, FormatDuration(procTimes[0]), totalLoops))
		if isTiFlashCop {
			buf.WriteString(fmt.Sprintf(", threads:%d}", totalThreads))
			if !totalTiFlashScanContext.Empty() {
				buf.WriteString(", " + totalTiFlashScanContext.String())
			}
		} else {
			buf.WriteString("}")
		}
	} else {
		n := len(procTimes)
		slices.Sort(procTimes)
		buf.WriteString(fmt.Sprintf("%v_task:{proc max:%v, min:%v, avg: %v, p80:%v, p95:%v, iters:%v, tasks:%v",
			crs.storeType, FormatDuration(procTimes[n-1]), FormatDuration(procTimes[0]), FormatDuration(avgTime),
			FormatDuration(procTimes[n*4/5]), FormatDuration(procTimes[n*19/20]), totalLoops, totalTasks))
		if isTiFlashCop {
			buf.WriteString(fmt.Sprintf(", threads:%d}", totalThreads))
			if !totalTiFlashScanContext.Empty() {
				buf.WriteString(", " + totalTiFlashScanContext.String())
			}
		} else {
			buf.WriteString("}")
		}
	}
	if !isTiFlashCop && crs.scanDetail != nil {
		detail := crs.scanDetail.String()
		if detail != "" {
			buf.WriteString(", ")
			buf.WriteString(detail)
		}
	}
	return buf.String()
}

const (
	// TpBasicRuntimeStats is the tp for BasicRuntimeStats.
	TpBasicRuntimeStats int = iota
	// TpRuntimeStatsWithCommit is the tp for RuntimeStatsWithCommit.
	TpRuntimeStatsWithCommit
	// TpRuntimeStatsWithConcurrencyInfo is the tp for RuntimeStatsWithConcurrencyInfo.
	TpRuntimeStatsWithConcurrencyInfo
	// TpSnapshotRuntimeStats is the tp for SnapshotRuntimeStats.
	TpSnapshotRuntimeStats
	// TpHashJoinRuntimeStats is the tp for HashJoinRuntimeStats.
	TpHashJoinRuntimeStats
	// TpIndexLookUpJoinRuntimeStats is the tp for IndexLookUpJoinRuntimeStats.
	TpIndexLookUpJoinRuntimeStats
	// TpRuntimeStatsWithSnapshot is the tp for RuntimeStatsWithSnapshot.
	TpRuntimeStatsWithSnapshot
	// TpJoinRuntimeStats is the tp for JoinRuntimeStats.
	TpJoinRuntimeStats
	// TpSelectResultRuntimeStats is the tp for SelectResultRuntimeStats.
	TpSelectResultRuntimeStats
	// TpInsertRuntimeStat is the tp for InsertRuntimeStat
	TpInsertRuntimeStat
	// TpIndexLookUpRunTimeStats is the tp for IndexLookUpRunTimeStats
	TpIndexLookUpRunTimeStats
	// TpSlowQueryRuntimeStat is the tp for SlowQueryRuntimeStat
	TpSlowQueryRuntimeStat
	// TpHashAggRuntimeStat is the tp for HashAggRuntimeStat
	TpHashAggRuntimeStat
	// TpIndexMergeRunTimeStats is the tp for IndexMergeRunTimeStats
	TpIndexMergeRunTimeStats
	// TpBasicCopRunTimeStats is the tp for BasicCopRunTimeStats
	TpBasicCopRunTimeStats
	// TpUpdateRuntimeStats is the tp for UpdateRuntimeStats
	TpUpdateRuntimeStats
	// TpFKCheckRuntimeStats is the tp for FKCheckRuntimeStats
	TpFKCheckRuntimeStats
	// TpFKCascadeRuntimeStats is the tp for FKCascadeRuntimeStats
	TpFKCascadeRuntimeStats
)

// RuntimeStats is used to express the executor runtime information.
type RuntimeStats interface {
	String() string
	Merge(RuntimeStats)
	Clone() RuntimeStats
	Tp() int
}

// TiFlashScanContext is used to express the table scan information in tiflash
type TiFlashScanContext struct {
	totalDmfileScannedPacks            uint64
	totalDmfileScannedRows             uint64
	totalDmfileSkippedPacks            uint64
	totalDmfileSkippedRows             uint64
	totalDmfileRoughSetIndexLoadTimeMs uint64
	totalDmfileReadTimeMs              uint64
	totalCreateSnapshotTimeMs          uint64
	totalLocalRegionNum                uint64
	totalRemoteRegionNum               uint64
}

// Clone implements the deep copy of * TiFlashshScanContext
func (context *TiFlashScanContext) Clone() TiFlashScanContext {
	return TiFlashScanContext{
		totalDmfileScannedPacks:            context.totalDmfileScannedPacks,
		totalDmfileScannedRows:             context.totalDmfileScannedRows,
		totalDmfileSkippedPacks:            context.totalDmfileSkippedPacks,
		totalDmfileSkippedRows:             context.totalDmfileSkippedRows,
		totalDmfileRoughSetIndexLoadTimeMs: context.totalDmfileRoughSetIndexLoadTimeMs,
		totalDmfileReadTimeMs:              context.totalDmfileReadTimeMs,
		totalCreateSnapshotTimeMs:          context.totalCreateSnapshotTimeMs,
		totalLocalRegionNum:                context.totalLocalRegionNum,
		totalRemoteRegionNum:               context.totalRemoteRegionNum,
	}
}
func (context *TiFlashScanContext) String() string {
	return fmt.Sprintf("tiflash_scan:{dtfile:{total_scanned_packs:%d, total_skipped_packs:%d, total_scanned_rows:%d, total_skipped_rows:%d, total_rs_index_load_time: %dms, total_read_time: %dms}, total_create_snapshot_time: %dms, total_local_region_num: %d, total_remote_region_num: %d}", context.totalDmfileScannedPacks, context.totalDmfileSkippedPacks, context.totalDmfileScannedRows, context.totalDmfileSkippedRows, context.totalDmfileRoughSetIndexLoadTimeMs, context.totalDmfileReadTimeMs, context.totalCreateSnapshotTimeMs, context.totalLocalRegionNum, context.totalRemoteRegionNum)
}

// Merge make sum to merge the information in TiFlashScanContext
func (context *TiFlashScanContext) Merge(other TiFlashScanContext) {
	context.totalDmfileScannedPacks += other.totalDmfileScannedPacks
	context.totalDmfileScannedRows += other.totalDmfileScannedRows
	context.totalDmfileSkippedPacks += other.totalDmfileSkippedPacks
	context.totalDmfileSkippedRows += other.totalDmfileSkippedRows
	context.totalDmfileRoughSetIndexLoadTimeMs += other.totalDmfileRoughSetIndexLoadTimeMs
	context.totalDmfileReadTimeMs += other.totalDmfileReadTimeMs
	context.totalCreateSnapshotTimeMs += other.totalCreateSnapshotTimeMs
	context.totalLocalRegionNum += other.totalLocalRegionNum
	context.totalRemoteRegionNum += other.totalRemoteRegionNum
}

// Empty check whether TiFlashScanContext is Empty, if scan no pack and skip no pack, we regard it as empty
func (context *TiFlashScanContext) Empty() bool {
	res := (context.totalDmfileScannedPacks == 0 && context.totalDmfileSkippedPacks == 0)
	return res
}

// BasicRuntimeStats is the basic runtime stats.
type BasicRuntimeStats struct {
	// executor's Next() called times.
	loop int32
	// executor consume time.
	consume int64
	// executor return row count.
	rows int64
	// executor extra infos
	tiflashScanContext TiFlashScanContext
}

// GetActRows return total rows of BasicRuntimeStats.
func (e *BasicRuntimeStats) GetActRows() int64 {
	return e.rows
}

// Clone implements the RuntimeStats interface.
func (e *BasicRuntimeStats) Clone() RuntimeStats {
	return &BasicRuntimeStats{
		loop:               e.loop,
		consume:            e.consume,
		rows:               e.rows,
		tiflashScanContext: e.tiflashScanContext.Clone(),
	}
}

// Merge implements the RuntimeStats interface.
func (e *BasicRuntimeStats) Merge(rs RuntimeStats) {
	tmp, ok := rs.(*BasicRuntimeStats)
	if !ok {
		return
	}
	e.loop += tmp.loop
	e.consume += tmp.consume
	e.rows += tmp.rows
	e.tiflashScanContext.Merge(tmp.tiflashScanContext)
}

// Tp implements the RuntimeStats interface.
func (*BasicRuntimeStats) Tp() int {
	return TpBasicRuntimeStats
}

// RootRuntimeStats is the executor runtime stats that combine with multiple runtime stats.
type RootRuntimeStats struct {
	basic    *BasicRuntimeStats
	groupRss []RuntimeStats
}

// NewRootRuntimeStats returns a new RootRuntimeStats
func NewRootRuntimeStats() *RootRuntimeStats {
	return &RootRuntimeStats{}
}

// GetActRows return total rows of RootRuntimeStats.
func (e *RootRuntimeStats) GetActRows() int64 {
	if e.basic == nil {
		return 0
	}
	return e.basic.rows
}

// MergeStats merges stats in the RootRuntimeStats and return the stats suitable for display directly.
func (e *RootRuntimeStats) MergeStats() (basic *BasicRuntimeStats, groups []RuntimeStats) {
	return e.basic, e.groupRss
}

// String implements the RuntimeStats interface.
func (e *RootRuntimeStats) String() string {
	basic, groups := e.MergeStats()
	strs := make([]string, 0, len(groups)+1)
	if basic != nil {
		strs = append(strs, basic.String())
	}
	for _, group := range groups {
		str := group.String()
		if len(str) > 0 {
			strs = append(strs, group.String())
		}
	}
	return strings.Join(strs, ", ")
}

// Record records executor's execution.
func (e *BasicRuntimeStats) Record(d time.Duration, rowNum int) {
	atomic.AddInt32(&e.loop, 1)
	atomic.AddInt64(&e.consume, int64(d))
	atomic.AddInt64(&e.rows, int64(rowNum))
}

// SetRowNum sets the row num.
func (e *BasicRuntimeStats) SetRowNum(rowNum int64) {
	atomic.StoreInt64(&e.rows, rowNum)
}

// String implements the RuntimeStats interface.
func (e *BasicRuntimeStats) String() string {
	if e == nil {
		return ""
	}
	var str strings.Builder
	str.WriteString("time:")
	str.WriteString(FormatDuration(time.Duration(e.consume)))
	str.WriteString(", loops:")
	str.WriteString(strconv.FormatInt(int64(e.loop), 10))
	return str.String()
}

// GetTime get the int64 total time
func (e *BasicRuntimeStats) GetTime() int64 {
	return e.consume
}

// RuntimeStatsColl collects executors's execution info.
type RuntimeStatsColl struct {
	rootStats map[int]*RootRuntimeStats
	copStats  map[int]*CopRuntimeStats
	mu        sync.Mutex
}

// NewRuntimeStatsColl creates new executor collector.
// Reuse the object to reduce allocation when *RuntimeStatsColl is not nil.
func NewRuntimeStatsColl(reuse *RuntimeStatsColl) *RuntimeStatsColl {
	if reuse != nil {
		// Reuse map is cheaper than create a new map object.
		// Go compiler optimize this cleanup code pattern to a clearmap() function.
		for k := range reuse.rootStats {
			delete(reuse.rootStats, k)
		}
		for k := range reuse.copStats {
			delete(reuse.copStats, k)
		}
		return reuse
	}
	return &RuntimeStatsColl{
		rootStats: make(map[int]*RootRuntimeStats),
		copStats:  make(map[int]*CopRuntimeStats),
	}
}

// RegisterStats register execStat for a executor.
func (e *RuntimeStatsColl) RegisterStats(planID int, info RuntimeStats) {
	e.mu.Lock()
	stats, ok := e.rootStats[planID]
	if !ok {
		stats = NewRootRuntimeStats()
		e.rootStats[planID] = stats
	}
	tp := info.Tp()
	found := false
	for _, rss := range stats.groupRss {
		if rss.Tp() == tp {
			rss.Merge(info)
			found = true
			break
		}
	}
	if !found {
		stats.groupRss = append(stats.groupRss, info.Clone())
	}
	e.mu.Unlock()
}

// GetBasicRuntimeStats gets basicRuntimeStats for a executor.
func (e *RuntimeStatsColl) GetBasicRuntimeStats(planID int) *BasicRuntimeStats {
	e.mu.Lock()
	defer e.mu.Unlock()
	stats, ok := e.rootStats[planID]
	if !ok {
		stats = NewRootRuntimeStats()
		e.rootStats[planID] = stats
	}
	if stats.basic == nil {
		stats.basic = &BasicRuntimeStats{}
	}
	return stats.basic
}

// GetRootStats gets execStat for a executor.
func (e *RuntimeStatsColl) GetRootStats(planID int) *RootRuntimeStats {
	e.mu.Lock()
	defer e.mu.Unlock()
	runtimeStats, exists := e.rootStats[planID]
	if !exists {
		runtimeStats = NewRootRuntimeStats()
		e.rootStats[planID] = runtimeStats
	}
	return runtimeStats
}

// GetCopStats gets the CopRuntimeStats specified by planID.
func (e *RuntimeStatsColl) GetCopStats(planID int) *CopRuntimeStats {
	e.mu.Lock()
	defer e.mu.Unlock()
	copStats, ok := e.copStats[planID]
	if !ok {
		return nil
	}
	return copStats
}

// GetOrCreateCopStats gets the CopRuntimeStats specified by planID, if not exists a new one will be created.
func (e *RuntimeStatsColl) GetOrCreateCopStats(planID int, storeType string) *CopRuntimeStats {
	e.mu.Lock()
	defer e.mu.Unlock()
	copStats, ok := e.copStats[planID]
	if !ok {
		copStats = &CopRuntimeStats{
			stats:      make(map[string]*basicCopRuntimeStats),
			scanDetail: &util.ScanDetail{},
			storeType:  storeType,
		}
		e.copStats[planID] = copStats
	}
	return copStats
}

func getPlanIDFromExecutionSummary(summary *tipb.ExecutorExecutionSummary) (int, bool) {
	if summary.GetExecutorId() != "" {
		strs := strings.Split(summary.GetExecutorId(), "_")
		if id, err := strconv.Atoi(strs[len(strs)-1]); err == nil {
			return id, true
		}
	}
	return 0, false
}

// RecordOneCopTask records a specific cop tasks's execution detail.
func (e *RuntimeStatsColl) RecordOneCopTask(planID int, storeType string, address string, summary *tipb.ExecutorExecutionSummary) int {
	// for TiFlash cop response, ExecutorExecutionSummary contains executor id, so if there is a valid executor id in
	// summary, use it overwrite the planID
	if id, valid := getPlanIDFromExecutionSummary(summary); valid {
		planID = id
	}
	copStats := e.GetOrCreateCopStats(planID, storeType)
	copStats.RecordOneCopTask(address, summary)
	return planID
}

// RecordScanDetail records a specific cop tasks's cop detail.
func (e *RuntimeStatsColl) RecordScanDetail(planID int, storeType string, detail *util.ScanDetail) {
	copStats := e.GetOrCreateCopStats(planID, storeType)
	copStats.scanDetail.Merge(detail)
}

// ExistsRootStats checks if the planID exists in the rootStats collection.
func (e *RuntimeStatsColl) ExistsRootStats(planID int) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	_, exists := e.rootStats[planID]
	return exists
}

// ExistsCopStats checks if the planID exists in the copStats collection.
func (e *RuntimeStatsColl) ExistsCopStats(planID int) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	_, exists := e.copStats[planID]
	return exists
}

// ConcurrencyInfo is used to save the concurrency information of the executor operator
type ConcurrencyInfo struct {
	concurrencyName string
	concurrencyNum  int
}

// NewConcurrencyInfo creates new executor's concurrencyInfo.
func NewConcurrencyInfo(name string, num int) *ConcurrencyInfo {
	return &ConcurrencyInfo{name, num}
}

// RuntimeStatsWithConcurrencyInfo is the BasicRuntimeStats with ConcurrencyInfo.
type RuntimeStatsWithConcurrencyInfo struct {
	// executor concurrency information
	concurrency []*ConcurrencyInfo
	// protect concurrency
	sync.Mutex
}

// Tp implements the RuntimeStats interface.
func (*RuntimeStatsWithConcurrencyInfo) Tp() int {
	return TpRuntimeStatsWithConcurrencyInfo
}

// SetConcurrencyInfo sets the concurrency informations.
// We must clear the concurrencyInfo first when we call the SetConcurrencyInfo.
// When the num <= 0, it means the exector operator is not executed parallel.
func (e *RuntimeStatsWithConcurrencyInfo) SetConcurrencyInfo(infos ...*ConcurrencyInfo) {
	e.Lock()
	defer e.Unlock()
	e.concurrency = e.concurrency[:0]
	e.concurrency = append(e.concurrency, infos...)
}

// Clone implements the RuntimeStats interface.
func (e *RuntimeStatsWithConcurrencyInfo) Clone() RuntimeStats {
	newRs := &RuntimeStatsWithConcurrencyInfo{
		concurrency: make([]*ConcurrencyInfo, 0, len(e.concurrency)),
	}
	newRs.concurrency = append(newRs.concurrency, e.concurrency...)
	return newRs
}

// String implements the RuntimeStats interface.
func (e *RuntimeStatsWithConcurrencyInfo) String() string {
	var result string
	if len(e.concurrency) > 0 {
		for i, concurrency := range e.concurrency {
			if i > 0 {
				result += ", "
			}
			if concurrency.concurrencyNum > 0 {
				result += fmt.Sprintf("%s:%d", concurrency.concurrencyName, concurrency.concurrencyNum)
			} else {
				result += fmt.Sprintf("%s:OFF", concurrency.concurrencyName)
			}
		}
	}
	return result
}

// Merge implements the RuntimeStats interface.
func (*RuntimeStatsWithConcurrencyInfo) Merge(RuntimeStats) {}

// RuntimeStatsWithCommit is the RuntimeStats with commit detail.
type RuntimeStatsWithCommit struct {
	Commit   *util.CommitDetails
	LockKeys *util.LockKeysDetails
	TxnCnt   int
}

// Tp implements the RuntimeStats interface.
func (*RuntimeStatsWithCommit) Tp() int {
	return TpRuntimeStatsWithCommit
}

// MergeCommitDetails merges the commit details.
func (e *RuntimeStatsWithCommit) MergeCommitDetails(detail *util.CommitDetails) {
	if detail == nil {
		return
	}
	if e.Commit == nil {
		e.Commit = detail
		e.TxnCnt = 1
		return
	}
	e.Commit.Merge(detail)
	e.TxnCnt++
}

// Merge implements the RuntimeStats interface.
func (e *RuntimeStatsWithCommit) Merge(rs RuntimeStats) {
	tmp, ok := rs.(*RuntimeStatsWithCommit)
	if !ok {
		return
	}
	e.TxnCnt += tmp.TxnCnt
	if tmp.Commit != nil {
		if e.Commit == nil {
			e.Commit = &util.CommitDetails{}
		}
		e.Commit.Merge(tmp.Commit)
	}

	if tmp.LockKeys != nil {
		if e.LockKeys == nil {
			e.LockKeys = &util.LockKeysDetails{}
		}
		e.LockKeys.Merge(tmp.LockKeys)
	}
}

// Clone implements the RuntimeStats interface.
func (e *RuntimeStatsWithCommit) Clone() RuntimeStats {
	newRs := RuntimeStatsWithCommit{
		TxnCnt: e.TxnCnt,
	}
	if e.Commit != nil {
		newRs.Commit = e.Commit.Clone()
	}
	if e.LockKeys != nil {
		newRs.LockKeys = e.LockKeys.Clone()
	}
	return &newRs
}

// String implements the RuntimeStats interface.
func (e *RuntimeStatsWithCommit) String() string {
	buf := bytes.NewBuffer(make([]byte, 0, 32))
	if e.Commit != nil {
		buf.WriteString("commit_txn: {")
		// Only print out when there are more than 1 transaction.
		if e.TxnCnt > 1 {
			buf.WriteString("count: ")
			buf.WriteString(strconv.Itoa(e.TxnCnt))
			buf.WriteString(", ")
		}
		if e.Commit.PrewriteTime > 0 {
			buf.WriteString("prewrite:")
			buf.WriteString(FormatDuration(e.Commit.PrewriteTime))
		}
		if e.Commit.WaitPrewriteBinlogTime > 0 {
			buf.WriteString(", wait_prewrite_binlog:")
			buf.WriteString(FormatDuration(e.Commit.WaitPrewriteBinlogTime))
		}
		if e.Commit.GetCommitTsTime > 0 {
			buf.WriteString(", get_commit_ts:")
			buf.WriteString(FormatDuration(e.Commit.GetCommitTsTime))
		}
		if e.Commit.CommitTime > 0 {
			buf.WriteString(", commit:")
			buf.WriteString(FormatDuration(e.Commit.CommitTime))
		}
		e.Commit.Mu.Lock()
		commitBackoffTime := e.Commit.Mu.CommitBackoffTime
		if commitBackoffTime > 0 {
			buf.WriteString(", backoff: {time: ")
			buf.WriteString(FormatDuration(time.Duration(commitBackoffTime)))
			if len(e.Commit.Mu.PrewriteBackoffTypes) > 0 {
				buf.WriteString(", prewrite type: ")
				buf.WriteString(e.formatBackoff(e.Commit.Mu.PrewriteBackoffTypes))
			}
			if len(e.Commit.Mu.CommitBackoffTypes) > 0 {
				buf.WriteString(", commit type: ")
				buf.WriteString(e.formatBackoff(e.Commit.Mu.CommitBackoffTypes))
			}
			buf.WriteString("}")
		}
		if e.Commit.Mu.SlowestPrewrite.ReqTotalTime > 0 {
			buf.WriteString(", slowest_prewrite_rpc: {total: ")
			buf.WriteString(strconv.FormatFloat(e.Commit.Mu.SlowestPrewrite.ReqTotalTime.Seconds(), 'f', 3, 64))
			buf.WriteString("s, region_id: ")
			buf.WriteString(strconv.FormatUint(e.Commit.Mu.SlowestPrewrite.Region, 10))
			buf.WriteString(", store: ")
			buf.WriteString(e.Commit.Mu.SlowestPrewrite.StoreAddr)
			buf.WriteString(", ")
			buf.WriteString(e.Commit.Mu.SlowestPrewrite.ExecDetails.String())
			buf.WriteString("}")
		}
		if e.Commit.Mu.CommitPrimary.ReqTotalTime > 0 {
			buf.WriteString(", commit_primary_rpc: {total: ")
			buf.WriteString(strconv.FormatFloat(e.Commit.Mu.CommitPrimary.ReqTotalTime.Seconds(), 'f', 3, 64))
			buf.WriteString("s, region_id: ")
			buf.WriteString(strconv.FormatUint(e.Commit.Mu.CommitPrimary.Region, 10))
			buf.WriteString(", store: ")
			buf.WriteString(e.Commit.Mu.CommitPrimary.StoreAddr)
			buf.WriteString(", ")
			buf.WriteString(e.Commit.Mu.CommitPrimary.ExecDetails.String())
			buf.WriteString("}")
		}
		e.Commit.Mu.Unlock()
		if e.Commit.ResolveLock.ResolveLockTime > 0 {
			buf.WriteString(", resolve_lock: ")
			buf.WriteString(FormatDuration(time.Duration(e.Commit.ResolveLock.ResolveLockTime)))
		}

		prewriteRegionNum := atomic.LoadInt32(&e.Commit.PrewriteRegionNum)
		if prewriteRegionNum > 0 {
			buf.WriteString(", region_num:")
			buf.WriteString(strconv.FormatInt(int64(prewriteRegionNum), 10))
		}
		if e.Commit.WriteKeys > 0 {
			buf.WriteString(", write_keys:")
			buf.WriteString(strconv.FormatInt(int64(e.Commit.WriteKeys), 10))
		}
		if e.Commit.WriteSize > 0 {
			buf.WriteString(", write_byte:")
			buf.WriteString(strconv.FormatInt(int64(e.Commit.WriteSize), 10))
		}
		if e.Commit.TxnRetry > 0 {
			buf.WriteString(", txn_retry:")
			buf.WriteString(strconv.FormatInt(int64(e.Commit.TxnRetry), 10))
		}
		buf.WriteString("}")
	}
	if e.LockKeys != nil {
		if buf.Len() > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString("lock_keys: {")
		if e.LockKeys.TotalTime > 0 {
			buf.WriteString("time:")
			buf.WriteString(FormatDuration(e.LockKeys.TotalTime))
		}
		if e.LockKeys.RegionNum > 0 {
			buf.WriteString(", region:")
			buf.WriteString(strconv.FormatInt(int64(e.LockKeys.RegionNum), 10))
		}
		if e.LockKeys.LockKeys > 0 {
			buf.WriteString(", keys:")
			buf.WriteString(strconv.FormatInt(int64(e.LockKeys.LockKeys), 10))
		}
		if e.LockKeys.ResolveLock.ResolveLockTime > 0 {
			buf.WriteString(", resolve_lock:")
			buf.WriteString(FormatDuration(time.Duration(e.LockKeys.ResolveLock.ResolveLockTime)))
		}
		e.LockKeys.Mu.Lock()
		if e.LockKeys.BackoffTime > 0 {
			buf.WriteString(", backoff: {time: ")
			buf.WriteString(FormatDuration(time.Duration(e.LockKeys.BackoffTime)))
			if len(e.LockKeys.Mu.BackoffTypes) > 0 {
				buf.WriteString(", type: ")
				buf.WriteString(e.formatBackoff(e.LockKeys.Mu.BackoffTypes))
			}
			buf.WriteString("}")
		}
		if e.LockKeys.Mu.SlowestReqTotalTime > 0 {
			buf.WriteString(", slowest_rpc: {total: ")
			buf.WriteString(strconv.FormatFloat(e.LockKeys.Mu.SlowestReqTotalTime.Seconds(), 'f', 3, 64))
			buf.WriteString("s, region_id: ")
			buf.WriteString(strconv.FormatUint(e.LockKeys.Mu.SlowestRegion, 10))
			buf.WriteString(", store: ")
			buf.WriteString(e.LockKeys.Mu.SlowestStoreAddr)
			buf.WriteString(", ")
			buf.WriteString(e.LockKeys.Mu.SlowestExecDetails.String())
			buf.WriteString("}")
		}
		e.LockKeys.Mu.Unlock()
		if e.LockKeys.LockRPCTime > 0 {
			buf.WriteString(", lock_rpc:")
			buf.WriteString(time.Duration(e.LockKeys.LockRPCTime).String())
		}
		if e.LockKeys.LockRPCCount > 0 {
			buf.WriteString(", rpc_count:")
			buf.WriteString(strconv.FormatInt(e.LockKeys.LockRPCCount, 10))
		}
		if e.LockKeys.RetryCount > 0 {
			buf.WriteString(", retry_count:")
			buf.WriteString(strconv.FormatInt(int64(e.LockKeys.RetryCount), 10))
		}

		buf.WriteString("}")
	}
	return buf.String()
}

func (*RuntimeStatsWithCommit) formatBackoff(backoffTypes []string) string {
	if len(backoffTypes) == 0 {
		return ""
	}
	tpMap := make(map[string]struct{})
	tpArray := []string{}
	for _, tpStr := range backoffTypes {
		_, ok := tpMap[tpStr]
		if ok {
			continue
		}
		tpMap[tpStr] = struct{}{}
		tpArray = append(tpArray, tpStr)
	}
	slices.Sort(tpArray)
	return fmt.Sprintf("%v", tpArray)
}

// FormatDuration uses to format duration, this function will prune precision before format duration.
// Pruning precision is for human readability. The prune rule is:
//  1. if the duration was less than 1us, return the original string.
//  2. readable value >=10, keep 1 decimal, otherwise, keep 2 decimal. such as:
//     9.412345ms  -> 9.41ms
//     10.412345ms -> 10.4ms
//     5.999s      -> 6s
//     100.45µs    -> 100.5µs
func FormatDuration(d time.Duration) string {
	if d <= time.Microsecond {
		return d.String()
	}
	unit := getUnit(d)
	if unit == time.Nanosecond {
		return d.String()
	}
	integer := (d / unit) * unit //nolint:durationcheck
	decimal := float64(d%unit) / float64(unit)
	if d < 10*unit {
		decimal = math.Round(decimal*100) / 100
	} else {
		decimal = math.Round(decimal*10) / 10
	}
	d = integer + time.Duration(decimal*float64(unit))
	return d.String()
}

func getUnit(d time.Duration) time.Duration {
	if d >= time.Second {
		return time.Second
	} else if d >= time.Millisecond {
		return time.Millisecond
	} else if d >= time.Microsecond {
		return time.Microsecond
	}
	return time.Nanosecond
}
