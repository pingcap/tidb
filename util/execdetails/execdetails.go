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

// CommitDetails contains commit detail information.
type CommitDetails struct {
	GetCommitTsTime    time.Duration
	PrewriteTime       time.Duration
	BinlogPrewriteTime time.Duration
	CommitTime         time.Duration
	LocalLatchTime     time.Duration
	CommitBackoffTime  int64
	Mu                 struct {
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
	// BinlogPrewriteTimeStr means the time of binlog prewrite
	BinlogPrewriteTimeStr = "Binlog_prewrite_time"
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
	parts := make([]string, 0, 6)
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
		if commitDetails.BinlogPrewriteTime > 0 {
			parts = append(parts, BinlogPrewriteTimeStr+": "+strconv.FormatFloat(commitDetails.BinlogPrewriteTime.Seconds(), 'f', -1, 64))
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
	if d.ProcessTime > 0 {
		fields = append(fields, zap.String(strings.ToLower(ProcessTimeStr), strconv.FormatFloat(d.ProcessTime.Seconds(), 'f', -1, 64)+"s"))
	}
	if d.WaitTime > 0 {
		fields = append(fields, zap.String(strings.ToLower(WaitTimeStr), strconv.FormatFloat(d.ProcessTime.Seconds(), 'f', -1, 64)+"s"))
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
	stats map[string][]*RuntimeStats
}

// RecordOneCopTask records a specific cop tasks's execution detail.
func (crs *CopRuntimeStats) RecordOneCopTask(address string, summary *tipb.ExecutorExecutionSummary) {
	crs.Lock()
	defer crs.Unlock()
	crs.stats[address] = append(crs.stats[address],
		&RuntimeStats{loop: int32(*summary.NumIterations),
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

	var totalRows, totalTasks int64
	var totalIters int32
	procTimes := make([]time.Duration, 0, 32)
	for _, instanceStats := range crs.stats {
		for _, stat := range instanceStats {
			procTimes = append(procTimes, time.Duration(stat.consume)*time.Nanosecond)
			totalRows += stat.rows
			totalIters += stat.loop
			totalTasks++
		}
	}

	if totalTasks == 1 {
		return fmt.Sprintf("time:%v, loops:%d, rows:%d", procTimes[0], totalIters, totalRows)
	}

	n := len(procTimes)
	sort.Slice(procTimes, func(i, j int) bool { return procTimes[i] < procTimes[j] })
	return fmt.Sprintf("proc max:%v, min:%v, p80:%v, p95:%v, rows:%v, iters:%v, tasks:%v",
		procTimes[n-1], procTimes[0], procTimes[n*4/5], procTimes[n*19/20], totalRows, totalIters, totalTasks)
}

// ReaderRuntimeStats collects stats for TableReader, IndexReader and IndexLookupReader
type ReaderRuntimeStats struct {
	sync.Mutex

	copRespTime []time.Duration
	procKeys    []int64
}

// recordOneCopTask record once cop response time to update maxcopRespTime
func (rrs *ReaderRuntimeStats) recordOneCopTask(t time.Duration, detail *ExecDetails) {
	rrs.Lock()
	defer rrs.Unlock()
	rrs.copRespTime = append(rrs.copRespTime, t)
	rrs.procKeys = append(rrs.procKeys, detail.ProcessedKeys)
}

func (rrs *ReaderRuntimeStats) String() string {
	size := len(rrs.copRespTime)
	if size == 0 {
		return ""
	}
	if size == 1 {
		return fmt.Sprintf("rpc num: 1, rpc time:%v, proc keys:%v", rrs.copRespTime[0], rrs.procKeys[0])
	}
	sort.Slice(rrs.copRespTime, func(i, j int) bool {
		return rrs.copRespTime[i] < rrs.copRespTime[j]
	})
	vMax, vMin := rrs.copRespTime[size-1], rrs.copRespTime[0]
	vP80, vP95 := rrs.copRespTime[size*4/5], rrs.copRespTime[size*19/20]
	sum := 0.0
	for _, t := range rrs.copRespTime {
		sum += float64(t)
	}
	vAvg := time.Duration(sum / float64(size))

	sort.Slice(rrs.procKeys, func(i, j int) bool {
		return rrs.procKeys[i] < rrs.procKeys[j]
	})
	keyMax := rrs.procKeys[size-1]
	keyP95 := rrs.procKeys[size*19/20]
	return fmt.Sprintf("rpc num: %v, rpc max:%v, min:%v, avg:%v, p80:%v, p95:%v, proc keys max:%v, p95:%v", size, vMax, vMin, vAvg, vP80, vP95, keyMax, keyP95)
}

// RuntimeStatsColl collects executors's execution info.
type RuntimeStatsColl struct {
	mu          sync.Mutex
	rootStats   map[string]*RuntimeStats
	copStats    map[string]*CopRuntimeStats
	readerStats map[string]*ReaderRuntimeStats
}

// concurrencyInfo is used to save the concurrency information of the executor operator
type concurrencyInfo struct {
	concurrencyName string
	concurrencyNum  int
}

// RuntimeStats collects one executor's execution info.
type RuntimeStats struct {
	// executor's Next() called times.
	loop int32
	// executor consume time.
	consume int64
	// executor return row count.
	rows int64

	// protect concurrency
	mu sync.Mutex
	// executor concurrency information
	concurrency []concurrencyInfo

	// additional information for executors
	additionalInfo string
}

// NewRuntimeStatsColl creates new executor collector.
func NewRuntimeStatsColl() *RuntimeStatsColl {
	return &RuntimeStatsColl{rootStats: make(map[string]*RuntimeStats),
		copStats: make(map[string]*CopRuntimeStats), readerStats: make(map[string]*ReaderRuntimeStats)}
}

// GetRootStats gets execStat for a executor.
func (e *RuntimeStatsColl) GetRootStats(planID string) *RuntimeStats {
	e.mu.Lock()
	defer e.mu.Unlock()
	runtimeStats, exists := e.rootStats[planID]
	if !exists {
		runtimeStats = &RuntimeStats{}
		e.rootStats[planID] = runtimeStats
	}
	return runtimeStats
}

// GetCopStats gets the CopRuntimeStats specified by planID.
func (e *RuntimeStatsColl) GetCopStats(planID string) *CopRuntimeStats {
	e.mu.Lock()
	defer e.mu.Unlock()
	copStats, ok := e.copStats[planID]
	if !ok {
		copStats = &CopRuntimeStats{stats: make(map[string][]*RuntimeStats)}
		e.copStats[planID] = copStats
	}
	return copStats
}

// RecordOneCopTask records a specific cop tasks's execution detail.
func (e *RuntimeStatsColl) RecordOneCopTask(planID, address string, summary *tipb.ExecutorExecutionSummary) {
	copStats := e.GetCopStats(planID)
	copStats.RecordOneCopTask(address, summary)
}

// RecordOneReaderStats records a specific stats for TableReader, IndexReader and IndexLookupReader.
func (e *RuntimeStatsColl) RecordOneReaderStats(planID string, copRespTime time.Duration, detail *ExecDetails) {
	readerStats := e.GetReaderStats(planID)
	readerStats.recordOneCopTask(copRespTime, detail)
}

// ExistsRootStats checks if the planID exists in the rootStats collection.
func (e *RuntimeStatsColl) ExistsRootStats(planID string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	_, exists := e.rootStats[planID]
	return exists
}

// ExistsCopStats checks if the planID exists in the copStats collection.
func (e *RuntimeStatsColl) ExistsCopStats(planID string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	_, exists := e.copStats[planID]
	return exists
}

// GetReaderStats gets the ReaderRuntimeStats specified by planID.
func (e *RuntimeStatsColl) GetReaderStats(planID string) *ReaderRuntimeStats {
	e.mu.Lock()
	defer e.mu.Unlock()
	stats, exists := e.readerStats[planID]
	if !exists {
		stats = &ReaderRuntimeStats{copRespTime: make([]time.Duration, 0, 20)}
		e.readerStats[planID] = stats
	}
	return stats
}

// Record records executor's execution.
func (e *RuntimeStats) Record(d time.Duration, rowNum int) {
	atomic.AddInt32(&e.loop, 1)
	atomic.AddInt64(&e.consume, int64(d))
	atomic.AddInt64(&e.rows, int64(rowNum))
}

// SetRowNum sets the row num.
func (e *RuntimeStats) SetRowNum(rowNum int64) {
	atomic.StoreInt64(&e.rows, rowNum)
}

// SetConcurrencyInfo sets the concurrency information.
// When the num <= 0, it means the exector operator is not executed parallel.
func (e *RuntimeStats) SetConcurrencyInfo(name string, num int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.concurrency = append(e.concurrency, concurrencyInfo{concurrencyName: name, concurrencyNum: num})
}

// SetAdditionalInfo sets the additional information.
func (e *RuntimeStats) SetAdditionalInfo(info string) {
	e.mu.Lock()
	e.additionalInfo = info
	e.mu.Unlock()
}

// GetActRows return rows of CopRuntimeStats.
func (e *RuntimeStats) GetActRows() int64 {
	return e.rows
}

func (e *RuntimeStats) String() string {
	result := fmt.Sprintf("time:%v, loops:%d, rows:%d", time.Duration(e.consume), e.loop, e.rows)
	if len(e.concurrency) > 0 {
		for _, concurrency := range e.concurrency {
			if concurrency.concurrencyNum > 0 {
				result += fmt.Sprintf(", %s:%d", concurrency.concurrencyName, concurrency.concurrencyNum)
			} else {
				result += fmt.Sprintf(", %s:OFF", concurrency.concurrencyName)
			}
		}
	}
	if len(e.additionalInfo) > 0 {
		result += ", " + e.additionalInfo
	}
	return result
}
