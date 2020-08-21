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
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

type commitDetailCtxKeyType struct{}

// CommitDetailCtxKey presents CommitDetail info key in context.
var CommitDetailCtxKey = commitDetailCtxKeyType{}

// ExecDetails contains execution detail information.
type ExecDetails struct {
	CalleeAddress    string
	CopTime          time.Duration
	ProcessTime      time.Duration
	WaitTime         time.Duration
	BackoffTime      time.Duration
	LockKeysDuration time.Duration
	BackoffSleep     map[string]time.Duration
	BackoffTimes     map[string]int
	RequestCount     int
	TotalKeys        int64
	ProcessedKeys    int64
	CommitDetail     *CommitDetails
}

type stmtExecDetailKeyType struct{}

// StmtExecDetailKey used to carry StmtExecDetail info in context.Context.
var StmtExecDetailKey = stmtExecDetailKeyType{}

// StmtExecDetails contains stmt level execution detail info.
type StmtExecDetails struct {
	BackoffCount         int64
	BackoffDuration      int64
	WaitKVRespDuration   int64
	WaitPDRespDuration   int64
	WriteSQLRespDuration time.Duration
}

// CommitDetails contains commit detail information.
type CommitDetails struct {
	GetCommitTsTime        time.Duration
	PrewriteTime           time.Duration
	WaitPrewriteBinlogTime time.Duration
	CommitTime             time.Duration
	LocalLatchTime         time.Duration
	CommitBackoffTime      int64
	Mu                     struct {
		sync.Mutex
		BackoffTypes []fmt.Stringer
	}
	ResolveLockTime   int64
	WriteKeys         int
	WriteSize         int
	PrewriteRegionNum int32
	TxnRetry          int
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
)

// String implements the fmt.Stringer interface.
func (d ExecDetails) String() string {
	parts := make([]string, 0, 8)
	if d.CopTime > 0 {
		parts = append(parts, CopTimeStr+": "+strconv.FormatFloat(d.CopTime.Seconds(), 'f', -1, 64))
	}
	if d.ProcessTime > 0 {
		parts = append(parts, ProcessTimeStr+": "+strconv.FormatFloat(d.ProcessTime.Seconds(), 'f', -1, 64))
	}
	if d.WaitTime > 0 {
		parts = append(parts, WaitTimeStr+": "+strconv.FormatFloat(d.WaitTime.Seconds(), 'f', -1, 64))
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
	if d.TotalKeys > 0 {
		parts = append(parts, TotalKeysStr+": "+strconv.FormatInt(d.TotalKeys, 10))
	}
	if d.ProcessedKeys > 0 {
		parts = append(parts, ProcessKeysStr+": "+strconv.FormatInt(d.ProcessedKeys, 10))
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
	return strings.Join(parts, " ")
}

// ToZapFields wraps the ExecDetails as zap.Fields.
func (d ExecDetails) ToZapFields() (fields []zap.Field) {
	fields = make([]zap.Field, 0, 16)
	if d.CopTime > 0 {
		fields = append(fields, zap.String(strings.ToLower(CopTimeStr), strconv.FormatFloat(d.CopTime.Seconds(), 'f', -1, 64)+"s"))
	}
	if d.ProcessTime > 0 {
		fields = append(fields, zap.String(strings.ToLower(ProcessTimeStr), strconv.FormatFloat(d.ProcessTime.Seconds(), 'f', -1, 64)+"s"))
	}
	if d.WaitTime > 0 {
		fields = append(fields, zap.String(strings.ToLower(WaitTimeStr), strconv.FormatFloat(d.WaitTime.Seconds(), 'f', -1, 64)+"s"))
	}
	if d.BackoffTime > 0 {
		fields = append(fields, zap.String(strings.ToLower(BackoffTimeStr), strconv.FormatFloat(d.BackoffTime.Seconds(), 'f', -1, 64)+"s"))
	}
	if d.RequestCount > 0 {
		fields = append(fields, zap.String(strings.ToLower(RequestCountStr), strconv.FormatInt(int64(d.RequestCount), 10)))
	}
	if d.TotalKeys > 0 {
		fields = append(fields, zap.String(strings.ToLower(TotalKeysStr), strconv.FormatInt(d.TotalKeys, 10)))
	}
	if d.ProcessedKeys > 0 {
		fields = append(fields, zap.String(strings.ToLower(ProcessKeysStr), strconv.FormatInt(d.ProcessedKeys, 10)))
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

// CopRuntimeStats collects cop tasks' execution info.
type CopRuntimeStats struct {
	sync.Mutex

	// stats stores the runtime statistics of coprocessor tasks.
	// The key of the map is the tikv-server address. Because a tikv-server can
	// have many region leaders, several coprocessor tasks can be sent to the
	// same tikv-server instance. We have to use a list to maintain all tasks
	// executed on each instance.
	stats map[string][]*BasicRuntimeStats
}

// RecordOneCopTask records a specific cop tasks's execution detail.
func (crs *CopRuntimeStats) RecordOneCopTask(address string, summary *tipb.ExecutorExecutionSummary) {
	crs.Lock()
	defer crs.Unlock()
	crs.stats[address] = append(crs.stats[address],
		&BasicRuntimeStats{loop: int32(*summary.NumIterations),
			consume: int64(*summary.TimeProcessedNs),
			rows:    int64(*summary.NumProducedRows)})
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
	procTimes := make([]time.Duration, 0, 32)
	for _, instanceStats := range crs.stats {
		for _, stat := range instanceStats {
			procTimes = append(procTimes, time.Duration(stat.consume)*time.Nanosecond)
			totalIters += stat.loop
			totalTasks++
		}
	}

	if totalTasks == 1 {
		return fmt.Sprintf("time:%v, loops:%d", procTimes[0], totalIters)
	}

	n := len(procTimes)
	sort.Slice(procTimes, func(i, j int) bool { return procTimes[i] < procTimes[j] })
	return fmt.Sprintf("proc max:%v, min:%v, p80:%v, p95:%v, iters:%v, tasks:%v",
		procTimes[n-1], procTimes[0], procTimes[n*4/5], procTimes[n*19/20], totalIters, totalTasks)
}

// RuntimeStats is used to express the executor runtime information.
type RuntimeStats interface {
	GetActRows() int64
	String() string
	GetBasicRuntimeStats() *BasicRuntimeStats
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

// GetActRows implements the RuntimeStats interface.
func (e *BasicRuntimeStats) GetActRows() int64 {
	return e.rows
}

// Record records executor's execution.
func (e *BasicRuntimeStats) Record(d time.Duration, rowNum int) {
	atomic.AddInt32(&e.loop, 1)
	atomic.AddInt64(&e.consume, int64(d))
	atomic.AddInt64(&e.rows, int64(rowNum))
}

// AddConsume adds executor's consume time.
func (e *BasicRuntimeStats) GetBasicRuntimeStats() *BasicRuntimeStats {
	return e
}

// SetRowNum sets the row num.
func (e *BasicRuntimeStats) SetRowNum(rowNum int64) {
	atomic.StoreInt64(&e.rows, rowNum)
}

// String implements the RuntimeStats interface.
func (e *BasicRuntimeStats) String() string {
	return fmt.Sprintf("time:%v, loops:%d", time.Duration(e.consume), e.loop)
}

// RuntimeStatsColl collects executors's execution info.
type RuntimeStatsColl struct {
	mu        sync.Mutex
	rootStats map[int]RuntimeStats
	copStats  map[int]*CopRuntimeStats
}

// NewRuntimeStatsColl creates new executor collector.
func NewRuntimeStatsColl() *RuntimeStatsColl {
	return &RuntimeStatsColl{rootStats: make(map[int]RuntimeStats),
		copStats: make(map[int]*CopRuntimeStats)}
}

// RegisterStats register execStat for a executor.
func (e *RuntimeStatsColl) RegisterStats(planID int, info RuntimeStats) {
	e.mu.Lock()
	e.rootStats[planID] = info
	e.mu.Unlock()
}

// GetRootStats gets execStat for a executor.
func (e *RuntimeStatsColl) GetRootStats(planID int) RuntimeStats {
	e.mu.Lock()
	defer e.mu.Unlock()
	runtimeStats, exists := e.rootStats[planID]
	if !exists {
		runtimeStats = &BasicRuntimeStats{}
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
		copStats = &CopRuntimeStats{stats: make(map[string][]*BasicRuntimeStats)}
		e.copStats[planID] = copStats
	}
	return copStats
}

// RecordOneCopTask records a specific cop tasks's execution detail.
func (e *RuntimeStatsColl) RecordOneCopTask(planID int, address string, summary *tipb.ExecutorExecutionSummary) {
	copStats := e.GetCopStats(planID)
	copStats.RecordOneCopTask(address, summary)
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
	*BasicRuntimeStats

	// protect concurrency
	sync.Mutex
	// executor concurrency information
	concurrency []*ConcurrencyInfo
}

// SetConcurrencyInfo sets the concurrency informations.
// We must clear the concurrencyInfo first when we call the SetConcurrencyInfo.
// When the num <= 0, it means the exector operator is not executed parallel.
func (e *RuntimeStatsWithConcurrencyInfo) SetConcurrencyInfo(infos ...*ConcurrencyInfo) {
	e.Lock()
	defer e.Unlock()
	e.concurrency = e.concurrency[:0]
	for _, info := range infos {
		e.concurrency = append(e.concurrency, info)
	}
}

func (e *RuntimeStatsWithConcurrencyInfo) String() string {
	var result string
	if e.BasicRuntimeStats != nil {
		result = fmt.Sprintf("time:%v, loops:%d", time.Duration(e.consume), e.loop)
	}
	if len(e.concurrency) > 0 {
		for _, concurrency := range e.concurrency {
			if concurrency.concurrencyNum > 0 {
				result += fmt.Sprintf(", %s:%d", concurrency.concurrencyName, concurrency.concurrencyNum)
			} else {
				result += fmt.Sprintf(", %s:OFF", concurrency.concurrencyName)
			}
		}
	}
	return result
}

type RuntimeStatsWithCommit struct {
	RuntimeStats
	Commit *CommitDetails
}

func (e *RuntimeStatsWithCommit) String() string {
	var result string
	if e.RuntimeStats != nil {
		basic := e.RuntimeStats.GetBasicRuntimeStats()
		newBasic := *basic
		if e.Commit != nil {
			newBasic.consume += int64(e.Commit.PrewriteTime + e.Commit.GetCommitTsTime + e.Commit.CommitTime)
		}
		result = newBasic.String()
	}
	if e.Commit == nil {
		return result
	}
	buf := bytes.NewBuffer(make([]byte, 0, len(result)+32))
	buf.WriteString(result)
	if e.Commit.PrewriteTime > 0 {
		buf.WriteString(", prewrite:")
		buf.WriteString(e.Commit.PrewriteTime.String())
	}
	if e.Commit.WaitPrewriteBinlogTime > 0 {
		buf.WriteString(", wait_prewrite_binlog:")
		buf.WriteString(e.Commit.WaitPrewriteBinlogTime.String())
	}
	if e.Commit.GetCommitTsTime > 0 {
		buf.WriteString(", get_commit_ts:")
		buf.WriteString(e.Commit.GetCommitTsTime.String())
	}
	if e.Commit.CommitTime > 0 {
		buf.WriteString(", commit:")
		buf.WriteString(e.Commit.CommitTime.String())
	}
	commitBackoffTime := atomic.LoadInt64(&e.Commit.CommitBackoffTime)
	if commitBackoffTime > 0 {
		buf.WriteString(", commit_backoff: {time: ")
		buf.WriteString(time.Duration(commitBackoffTime).String())
		tpMap := make(map[string]struct{})
		tpArray := []string{}
		e.Commit.Mu.Lock()
		if len(e.Commit.Mu.BackoffTypes) > 0 {
			for _, tp := range e.Commit.Mu.BackoffTypes {
				tpStr := tp.String()
				_, ok := tpMap[tpStr]
				if ok {
					continue
				}
				tpMap[tpStr] = struct{}{}
				tpArray = append(tpArray, tpStr)
			}
			buf.WriteString(", type: ")
			sort.Strings(tpArray)
			buf.WriteString(fmt.Sprintf("%v", tpArray))
		}
		e.Commit.Mu.Unlock()
		buf.WriteString("}")
	}
	if e.Commit.ResolveLockTime > 0 {
		buf.WriteString(", resolve_lock: ")
		buf.WriteString(time.Duration(e.Commit.ResolveLockTime).String())
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
	return buf.String()
}
