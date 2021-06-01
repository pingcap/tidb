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
// See the License for the specific language governing permissions and
// limitations under the License.

package execdetails

import (
	"bytes"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/store/tikv/util"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

// ExecDetails contains execution detail information.
type ExecDetails struct {
	CalleeAddress    string
	CopTime          time.Duration
	BackoffTime      time.Duration
	LockKeysDuration time.Duration
	BackoffSleep     map[string]time.Duration
	BackoffTimes     map[string]int
	RequestCount     int
	CommitDetail     *util.CommitDetails
	LockKeysDetail   *util.LockKeysDetails
	ScanDetail       *util.ScanDetail
	TimeDetail       util.TimeDetail
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
	// CommitBackoffTimeStr means the time of commit backoff.
	CommitBackoffTimeStr = "Commit_backoff_time"
	// BackoffTypesStr means the backoff type.
	BackoffTypesStr = "Backoff_types"
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
		commitBackoffTime := atomic.LoadInt64(&commitDetails.CommitBackoffTime)
		if commitBackoffTime > 0 {
			parts = append(parts, CommitBackoffTimeStr+": "+strconv.FormatFloat(time.Duration(commitBackoffTime).Seconds(), 'f', -1, 64))
		}
		commitDetails.Mu.Lock()
		if len(commitDetails.Mu.BackoffTypes) > 0 {
			parts = append(parts, BackoffTypesStr+": "+fmt.Sprintf("%v", commitDetails.Mu.BackoffTypes))
		}
		commitDetails.Mu.Unlock()
		resolveLockTime := atomic.LoadInt64(&commitDetails.ResolveLockTime)
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
		commitBackoffTime := atomic.LoadInt64(&commitDetails.CommitBackoffTime)
		if commitBackoffTime > 0 {
			fields = append(fields, zap.String("commit_backoff_time", fmt.Sprintf("%v", strconv.FormatFloat(time.Duration(commitBackoffTime).Seconds(), 'f', -1, 64)+"s")))
		}
		commitDetails.Mu.Lock()
		if len(commitDetails.Mu.BackoffTypes) > 0 {
			fields = append(fields, zap.String("backoff_types", fmt.Sprintf("%v", commitDetails.Mu.BackoffTypes)))
		}
		commitDetails.Mu.Unlock()
		resolveLockTime := atomic.LoadInt64(&commitDetails.ResolveLockTime)
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
	BasicRuntimeStats
	threads   int32
	storeType string
}

// String implements the RuntimeStats interface.
func (e *basicCopRuntimeStats) String() string {
	if e.storeType == "tiflash" {
		return fmt.Sprintf("time:%v, loops:%d, threads:%d", FormatDuration(time.Duration(e.consume)), e.loop, e.threads)
	}
	return fmt.Sprintf("time:%v, loops:%d", FormatDuration(time.Duration(e.consume)), e.loop)
}

// Clone implements the RuntimeStats interface.
func (e *basicCopRuntimeStats) Clone() RuntimeStats {
	return &basicCopRuntimeStats{
		BasicRuntimeStats: BasicRuntimeStats{loop: e.loop, consume: e.consume, rows: e.rows},
		threads:           e.threads,
		storeType:         e.storeType,
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
}

// Tp implements the RuntimeStats interface.
func (e *basicCopRuntimeStats) Tp() int {
	return TpBasicCopRunTimeStats
}

// CopRuntimeStats collects cop tasks' execution info.
type CopRuntimeStats struct {
	sync.Mutex

	// stats stores the runtime statistics of coprocessor tasks.
	// The key of the map is the tikv-server address. Because a tikv-server can
	// have many region leaders, several coprocessor tasks can be sent to the
	// same tikv-server instance. We have to use a list to maintain all tasks
	// executed on each instance.
	stats      map[string][]*basicCopRuntimeStats
	scanDetail *util.ScanDetail
	// do not use kv.StoreType because it will meet cycle import error
	storeType string
}

// RecordOneCopTask records a specific cop tasks's execution detail.
func (crs *CopRuntimeStats) RecordOneCopTask(address string, summary *tipb.ExecutorExecutionSummary) {
	crs.Lock()
	defer crs.Unlock()
	crs.stats[address] = append(crs.stats[address],
		&basicCopRuntimeStats{BasicRuntimeStats: BasicRuntimeStats{loop: int32(*summary.NumIterations),
			consume: int64(*summary.TimeProcessedNs),
			rows:    int64(*summary.NumProducedRows)},
			threads:   int32(summary.GetConcurrency()),
			storeType: crs.storeType})
}

// GetActRows return total rows of CopRuntimeStats.
func (crs *CopRuntimeStats) GetActRows() (totalRows int64) {
	for _, instanceStats := range crs.stats {
		for _, stat := range instanceStats {
			totalRows += stat.rows
		}
	}
	return totalRows
}

func (crs *CopRuntimeStats) String() string {
	if len(crs.stats) == 0 {
		return ""
	}

	var totalTasks int64
	var totalIters int32
	var totalThreads int32
	procTimes := make([]time.Duration, 0, 32)
	for _, instanceStats := range crs.stats {
		for _, stat := range instanceStats {
			procTimes = append(procTimes, time.Duration(stat.consume)*time.Nanosecond)
			totalIters += stat.loop
			totalThreads += stat.threads
			totalTasks++
		}
	}
	isTiFlashCop := crs.storeType == "tiflash"

	buf := bytes.NewBuffer(make([]byte, 0, 16))
	if totalTasks == 1 {
		buf.WriteString(fmt.Sprintf("%v_task:{time:%v, loops:%d", crs.storeType, FormatDuration(procTimes[0]), totalIters))
		if isTiFlashCop {
			buf.WriteString(fmt.Sprintf(", threads:%d}", totalThreads))
		} else {
			buf.WriteString("}")
		}
	} else {
		n := len(procTimes)
		sort.Slice(procTimes, func(i, j int) bool { return procTimes[i] < procTimes[j] })
		buf.WriteString(fmt.Sprintf("%v_task:{proc max:%v, min:%v, p80:%v, p95:%v, iters:%v, tasks:%v",
			crs.storeType, FormatDuration(procTimes[n-1]), FormatDuration(procTimes[0]),
			FormatDuration(procTimes[n*4/5]), FormatDuration(procTimes[n*19/20]), totalIters, totalTasks))
		if isTiFlashCop {
			buf.WriteString(fmt.Sprintf(", threads:%d}", totalThreads))
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
	// TpIndexLookUpRunTimeStats is the tp for TpIndexLookUpRunTimeStats
	TpIndexLookUpRunTimeStats
	// TpSlowQueryRuntimeStat is the tp for TpSlowQueryRuntimeStat
	TpSlowQueryRuntimeStat
	// TpHashAggRuntimeStat is the tp for HashAggRuntimeStat
	TpHashAggRuntimeStat
	// TpIndexMergeRunTimeStats is the tp for TpIndexMergeRunTimeStats
	TpIndexMergeRunTimeStats
	// TpBasicCopRunTimeStats is the tp for TpBasicCopRunTimeStats
	TpBasicCopRunTimeStats
)

// RuntimeStats is used to express the executor runtime information.
type RuntimeStats interface {
	String() string
	Merge(RuntimeStats)
	Clone() RuntimeStats
	Tp() int
}

// BasicRuntimeStats is the basic runtime stats.
type BasicRuntimeStats struct {
	// executor's Next() called times.
	loop int32
	// executor consume time.
	consume int64
	// executor return row count.
	rows int64
}

// GetActRows return total rows of BasicRuntimeStats.
func (e *BasicRuntimeStats) GetActRows() int64 {
	return e.rows
}

// Clone implements the RuntimeStats interface.
func (e *BasicRuntimeStats) Clone() RuntimeStats {
	return &BasicRuntimeStats{
		loop:    e.loop,
		consume: e.consume,
		rows:    e.rows,
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
}

// Tp implements the RuntimeStats interface.
func (e *BasicRuntimeStats) Tp() int {
	return TpBasicRuntimeStats
}

// RootRuntimeStats is the executor runtime stats that combine with multiple runtime stats.
type RootRuntimeStats struct {
	basics   []*BasicRuntimeStats
	groupRss [][]RuntimeStats
}

// GetActRows return total rows of RootRuntimeStats.
func (e *RootRuntimeStats) GetActRows() int64 {
	num := int64(0)
	for _, basic := range e.basics {
		num += basic.GetActRows()
	}
	return num
}

// String implements the RuntimeStats interface.
func (e *RootRuntimeStats) String() string {
	buf := bytes.NewBuffer(make([]byte, 0, 32))
	if len(e.basics) > 0 {
		if len(e.basics) == 1 {
			buf.WriteString(e.basics[0].String())
		} else {
			basic := e.basics[0].Clone()
			for i := 1; i < len(e.basics); i++ {
				basic.Merge(e.basics[i])
			}
			buf.WriteString(basic.String())
		}
	}
	if len(e.groupRss) > 0 {
		if buf.Len() > 0 {
			buf.WriteString(", ")
		}
		for i, rss := range e.groupRss {
			if i > 0 {
				buf.WriteString(", ")
			}
			if len(rss) == 1 {
				buf.WriteString(rss[0].String())
				continue
			}
			rs := rss[0].Clone()
			for i := 1; i < len(rss); i++ {
				rs.Merge(rss[i])
			}
			buf.WriteString(rs.String())
		}
	}
	return buf.String()
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
	return fmt.Sprintf("time:%v, loops:%d", FormatDuration(time.Duration(e.consume)), e.loop)
}

// GetTime get the int64 total time
func (e *BasicRuntimeStats) GetTime() int64 {
	return e.consume
}

// RuntimeStatsColl collects executors's execution info.
type RuntimeStatsColl struct {
	mu        sync.Mutex
	rootStats map[int]*RootRuntimeStats
	copStats  map[int]*CopRuntimeStats
}

// NewRuntimeStatsColl creates new executor collector.
func NewRuntimeStatsColl() *RuntimeStatsColl {
	return &RuntimeStatsColl{rootStats: make(map[int]*RootRuntimeStats),
		copStats: make(map[int]*CopRuntimeStats)}
}

// RegisterStats register execStat for a executor.
func (e *RuntimeStatsColl) RegisterStats(planID int, info RuntimeStats) {
	e.mu.Lock()
	stats, ok := e.rootStats[planID]
	if !ok {
		stats = &RootRuntimeStats{}
		e.rootStats[planID] = stats
	}
	if basic, ok := info.(*BasicRuntimeStats); ok {
		stats.basics = append(stats.basics, basic)
	} else {
		tp := info.Tp()
		found := false
		for i, rss := range stats.groupRss {
			if len(rss) == 0 {
				continue
			}
			if rss[0].Tp() == tp {
				stats.groupRss[i] = append(stats.groupRss[i], info)
				found = true
				break
			}
		}
		if !found {
			stats.groupRss = append(stats.groupRss, []RuntimeStats{info})
		}
	}
	e.mu.Unlock()
}

// GetRootStats gets execStat for a executor.
func (e *RuntimeStatsColl) GetRootStats(planID int) *RootRuntimeStats {
	e.mu.Lock()
	defer e.mu.Unlock()
	runtimeStats, exists := e.rootStats[planID]
	if !exists {
		runtimeStats = &RootRuntimeStats{}
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
			stats:      make(map[string][]*basicCopRuntimeStats),
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
func (e *RuntimeStatsColl) RecordOneCopTask(planID int, storeType string, address string, summary *tipb.ExecutorExecutionSummary) {
	// for TiFlash cop response, ExecutorExecutionSummary contains executor id, so if there is a valid executor id in
	// summary, use it overwrite the planID
	if id, valid := getPlanIDFromExecutionSummary(summary); valid {
		planID = id
	}
	copStats := e.GetOrCreateCopStats(planID, storeType)
	copStats.RecordOneCopTask(address, summary)
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
	// protect concurrency
	sync.Mutex
	// executor concurrency information
	concurrency []*ConcurrencyInfo
}

// Tp implements the RuntimeStats interface.
func (e *RuntimeStatsWithConcurrencyInfo) Tp() int {
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
func (e *RuntimeStatsWithConcurrencyInfo) Merge(_ RuntimeStats) {
	return
}

// RuntimeStatsWithCommit is the RuntimeStats with commit detail.
type RuntimeStatsWithCommit struct {
	Commit   *util.CommitDetails
	LockKeys *util.LockKeysDetails
}

// Tp implements the RuntimeStats interface.
func (e *RuntimeStatsWithCommit) Tp() int {
	return TpRuntimeStatsWithCommit
}

// Merge implements the RuntimeStats interface.
func (e *RuntimeStatsWithCommit) Merge(rs RuntimeStats) {
	tmp, ok := rs.(*RuntimeStatsWithCommit)
	if !ok {
		return
	}
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
	newRs := RuntimeStatsWithCommit{}
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
		commitBackoffTime := atomic.LoadInt64(&e.Commit.CommitBackoffTime)
		if commitBackoffTime > 0 {
			buf.WriteString(", backoff: {time: ")
			buf.WriteString(FormatDuration(time.Duration(commitBackoffTime)))
			e.Commit.Mu.Lock()
			if len(e.Commit.Mu.BackoffTypes) > 0 {
				buf.WriteString(", type: ")
				buf.WriteString(e.formatBackoff(e.Commit.Mu.BackoffTypes))
			}
			e.Commit.Mu.Unlock()
			buf.WriteString("}")
		}
		if e.Commit.ResolveLockTime > 0 {
			buf.WriteString(", resolve_lock: ")
			buf.WriteString(FormatDuration(time.Duration(e.Commit.ResolveLockTime)))
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
		if e.LockKeys.ResolveLockTime > 0 {
			buf.WriteString(", resolve_lock:")
			buf.WriteString(FormatDuration(time.Duration(e.LockKeys.ResolveLockTime)))
		}
		if e.LockKeys.BackoffTime > 0 {
			buf.WriteString(", backoff: {time: ")
			buf.WriteString(FormatDuration(time.Duration(e.LockKeys.BackoffTime)))
			e.LockKeys.Mu.Lock()
			if len(e.LockKeys.Mu.BackoffTypes) > 0 {
				buf.WriteString(", type: ")
				buf.WriteString(e.formatBackoff(e.LockKeys.Mu.BackoffTypes))
			}
			e.LockKeys.Mu.Unlock()
			buf.WriteString("}")
		}
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

func (e *RuntimeStatsWithCommit) formatBackoff(backoffTypes []string) string {
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
	sort.Strings(tpArray)
	return fmt.Sprintf("%v", tpArray)
}

// FormatDuration uses to format duration, this function will prune precision before format duration.
// Pruning precision is for human readability. The prune rule is:
// 1. if the duration was less than 1us, return the original string.
// 2. readable value >=10, keep 1 decimal, otherwise, keep 2 decimal. such as:
//    9.412345ms  -> 9.41ms
//    10.412345ms -> 10.4ms
//    5.999s      -> 6s
//    100.45µs    -> 100.5µs
func FormatDuration(d time.Duration) string {
	if d <= time.Microsecond {
		return d.String()
	}
	unit := getUnit(d)
	if unit == time.Nanosecond {
		return d.String()
	}
	integer := (d / unit) * unit
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
