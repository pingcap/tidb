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

type (
	commitDetailCtxKeyType   struct{}
	snapshotDetailCtxKeyType struct{}
)

var (
	// CommitDetailCtxKey presents CommitDetail info key in context.
	CommitDetailCtxKey = commitDetailCtxKeyType{}
	// SnapshotDetailCtxKey presents SnapshotDetail info key in context.
	SnapshotDetailCtxKey = snapshotDetailCtxKeyType{}
)

const (
	// ProcessTimeStr represents the sum of process time of all the coprocessor tasks.
	ProcessTimeStr = "Process_time"
	// WaitTimeStr means the time of all coprocessor wait.
	WaitTimeStr = "Wait_time"
	// BackoffTimeStr means the time of all back-off.
	BackoffTimeStr = "Backoff_time"
	// RequestCountStr means the request count.
	RequestCountStr = "Request_count"
	// TotalKeysStr means the total scan keys.
	TotalKeysStr = "Total_keys"
	// ProcessKeysStr means the total processed keys.
	ProcessKeysStr = "Process_keys"
	// PreWriteTimeStr means the time of pre-write.
	PreWriteTimeStr = "Prewrite_time"
	// CommitTimeStr means the time of commit.
	CommitTimeStr = "Commit_time"
	// GetCommitTSTimeStr means the time of getting commit ts.
	GetCommitTSTimeStr = "Get_commit_ts_time"
	// CommitBackoffTimeStr means the time of commit backoff.
	CommitBackoffTimeStr = "Commit_backoff_time"
	// BackoffTypesStr means the backoff type.
	BackoffTypesStr = "Backoff_types"
	// WaitLockTimeStr means the time of resolving lock.
	WaitLockTimeStr = "Wait_lock_time"
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
	// SnapshotGetTimeStr means the time of total get/batch-get.
	SnapshotGetTimeStr = "Get_time"
	// SnapshotGetKeyStr means the count of keys be get.
	SnapshotGetKeyStr = "Get_key"
	// SnapshotWaitLockStr means the time of waiting in resolving lock.
	SnapshotWaitLockStr = "Wait_lock_time"
	// SnapshotRPCCountStr means the RPC count used to get.
	SnapshotRPCCountStr = "RPC_count"
)

// SQLExecDetails contains SQL execution detail information.
type SQLExecDetails struct {
	CopExecDetails *CopExecDetails
	SnapshotDetail *SnapshotExecDetails
	CommitDetail   *CommitExecDetails
}

// ToZapFields wraps the SQLExecDetails as zap.Fields.
func (s *SQLExecDetails) ToZapFields(fields []zap.Field) []zap.Field {
	if s.CopExecDetails != nil {
		fields = s.CopExecDetails.ToZapFields(fields)
	}
	if s.SnapshotDetail != nil {
		fields = s.SnapshotDetail.ToZapFields(fields)
	}
	if s.CommitDetail != nil {
		fields = s.CommitDetail.ToZapFields(fields)
	}
	return fields
}

// ReadCopStr returns cop read phase exec details.
func (s *SQLExecDetails) ReadCopStr() string {
	var details []string
	if s.CopExecDetails != nil {
		details = s.CopExecDetails.append(details)
	}
	return strings.Join(details, " ")
}

// ReadSnapshotStr returns snapshot read phase exec details.
func (s *SQLExecDetails) ReadSnapshotStr() string {
	var details []string
	if s.SnapshotDetail != nil {
		details = s.SnapshotDetail.append(details)
	}
	return strings.Join(details, " ")
}

// WriteStr returns write phase exec details.
func (s *SQLExecDetails) WriteStr() string {
	var details []string
	if s.CommitDetail != nil {
		details = s.CommitDetail.append(details)
	}
	return strings.Join(details, " ")
}

// CopExecDetails contains execution detail information.
type CopExecDetails struct {
	CalleeAddress string
	ProcessTime   time.Duration
	WaitTime      time.Duration
	BackoffTime   time.Duration
	RequestCount  int
	TotalKeys     int64
	ProcessedKeys int64
}

func (d *CopExecDetails) append(parts []string) []string {
	if d.ProcessTime > 0 {
		parts = append(parts, ProcessTimeStr+": "+strconv.FormatFloat(d.ProcessTime.Seconds(), 'f', -1, 64))
	}
	if d.WaitTime > 0 {
		parts = append(parts, WaitTimeStr+": "+strconv.FormatFloat(d.WaitTime.Seconds(), 'f', -1, 64))
	}
	if d.BackoffTime > 0 {
		parts = append(parts, BackoffTimeStr+": "+strconv.FormatFloat(d.BackoffTime.Seconds(), 'f', -1, 64))
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
	return parts
}

// ToZapFields wraps the CopExecDetails as zap.Fields.
func (d CopExecDetails) ToZapFields(fields []zap.Field) []zap.Field {
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
	return fields
}

// CommitExecDetails contains commit detail information.
type CommitExecDetails struct {
	GetCommitTsTime   time.Duration
	PrewriteTime      time.Duration
	CommitTime        time.Duration
	LocalLatchTime    time.Duration
	CommitBackoffTime int64
	Mu                struct {
		sync.Mutex
		BackoffTypes []fmt.Stringer
	}
	WaitLockTime      int64
	WriteKeys         int
	WriteSize         int
	PrewriteRegionNum int32
	TxnRetry          int
}

// ToZapFields wraps the CommitExecDetails as zap.Fields.
func (c *CommitExecDetails) ToZapFields(fields []zap.Field) []zap.Field {
	if c.PrewriteTime > 0 {
		fields = append(fields, zap.String("prewrite_time", fmt.Sprintf("%v", strconv.FormatFloat(c.PrewriteTime.Seconds(), 'f', -1, 64)+"s")))
	}
	if c.CommitTime > 0 {
		fields = append(fields, zap.String("commit_time", fmt.Sprintf("%v", strconv.FormatFloat(c.CommitTime.Seconds(), 'f', -1, 64)+"s")))
	}
	if c.GetCommitTsTime > 0 {
		fields = append(fields, zap.String("get_commit_ts_time", fmt.Sprintf("%v", strconv.FormatFloat(c.GetCommitTsTime.Seconds(), 'f', -1, 64)+"s")))
	}
	commitBackoffTime := atomic.LoadInt64(&c.CommitBackoffTime)
	if commitBackoffTime > 0 {
		fields = append(fields, zap.String("commit_backoff_time", fmt.Sprintf("%v", strconv.FormatFloat(time.Duration(commitBackoffTime).Seconds(), 'f', -1, 64)+"s")))
	}
	c.Mu.Lock()
	if len(c.Mu.BackoffTypes) > 0 {
		fields = append(fields, zap.String("backoff_types", fmt.Sprintf("%v", c.Mu.BackoffTypes)))
	}
	c.Mu.Unlock()
	waitLockTime := atomic.LoadInt64(&c.WaitLockTime)
	if waitLockTime > 0 {
		fields = append(fields, zap.String("wait_lock_time", fmt.Sprintf("%v", strconv.FormatFloat(time.Duration(waitLockTime).Seconds(), 'f', -1, 64)+"s")))
	}
	if c.LocalLatchTime > 0 {
		fields = append(fields, zap.String("local_latch_wait_time", fmt.Sprintf("%v", strconv.FormatFloat(c.LocalLatchTime.Seconds(), 'f', -1, 64)+"s")))
	}
	if c.WriteKeys > 0 {
		fields = append(fields, zap.Int("write_keys", c.WriteKeys))
	}
	if c.WriteSize > 0 {
		fields = append(fields, zap.Int("write_size", c.WriteSize))
	}
	prewriteRegionNum := atomic.LoadInt32(&c.PrewriteRegionNum)
	if prewriteRegionNum > 0 {
		fields = append(fields, zap.Int32("prewrite_region", prewriteRegionNum))
	}
	if c.TxnRetry > 0 {
		fields = append(fields, zap.Int("txn_retry", c.TxnRetry))
	}
	return fields
}

func (c *CommitExecDetails) append(parts []string) []string {
	if c.PrewriteTime > 0 {
		parts = append(parts, PreWriteTimeStr+": "+strconv.FormatFloat(c.PrewriteTime.Seconds(), 'f', -1, 64))
	}
	if c.CommitTime > 0 {
		parts = append(parts, CommitTimeStr+": "+strconv.FormatFloat(c.CommitTime.Seconds(), 'f', -1, 64))
	}
	if c.GetCommitTsTime > 0 {
		parts = append(parts, GetCommitTSTimeStr+": "+strconv.FormatFloat(c.GetCommitTsTime.Seconds(), 'f', -1, 64))
	}
	commitBackoffTime := atomic.LoadInt64(&c.CommitBackoffTime)
	if commitBackoffTime > 0 {
		parts = append(parts, CommitBackoffTimeStr+": "+strconv.FormatFloat(time.Duration(commitBackoffTime).Seconds(), 'f', -1, 64))
	}
	c.Mu.Lock()
	if len(c.Mu.BackoffTypes) > 0 {
		parts = append(parts, BackoffTypesStr+": "+fmt.Sprintf("%v", c.Mu.BackoffTypes))
	}
	c.Mu.Unlock()
	waitLockTime := atomic.LoadInt64(&c.WaitLockTime)
	if waitLockTime > 0 {
		parts = append(parts, WaitLockTimeStr+": "+strconv.FormatFloat(time.Duration(waitLockTime).Seconds(), 'f', -1, 64))
	}
	if c.LocalLatchTime > 0 {
		parts = append(parts, LocalLatchWaitTimeStr+": "+strconv.FormatFloat(c.LocalLatchTime.Seconds(), 'f', -1, 64))
	}
	if c.WriteKeys > 0 {
		parts = append(parts, WriteKeysStr+": "+strconv.FormatInt(int64(c.WriteKeys), 10))
	}
	if c.WriteSize > 0 {
		parts = append(parts, WriteSizeStr+": "+strconv.FormatInt(int64(c.WriteSize), 10))
	}
	prewriteRegionNum := atomic.LoadInt32(&c.PrewriteRegionNum)
	if prewriteRegionNum > 0 {
		parts = append(parts, PrewriteRegionStr+": "+strconv.FormatInt(int64(prewriteRegionNum), 10))
	}
	if c.TxnRetry > 0 {
		parts = append(parts, TxnRetryStr+": "+strconv.FormatInt(int64(c.TxnRetry), 10))
	}
	return parts
}

// SnapshotExecDetails contains snapshot execution detail information.
type SnapshotExecDetails struct {
	ReqCount     int
	GetTime      time.Duration
	GetKey       int
	WaitLockTime int64
	RPCCount     uint32
}

// ToZapFields wraps the SnapshotExecDetails as zap.Fields.
func (s *SnapshotExecDetails) ToZapFields(fields []zap.Field) []zap.Field {
	if s.GetTime > 0 {
		fields = append(fields, zap.String("get_time", strconv.FormatFloat(s.GetTime.Seconds(), 'f', -1, 64)))
	}
	if s.GetKey > 0 {
		fields = append(fields, zap.String("get_key", strconv.FormatInt(int64(s.GetKey), 10)))
	}
	if s.WaitLockTime > 0 {
		fields = append(fields, zap.String("wait_lock_time", strconv.FormatFloat(time.Duration(s.WaitLockTime).Seconds(), 'f', -1, 64)))
	}
	if s.RPCCount > 0 {
		fields = append(fields, zap.String("rpc_count", strconv.FormatInt(int64(s.RPCCount), 10)))
	}
	return fields
}

func (s *SnapshotExecDetails) append(parts []string) []string {
	if s.GetTime > 0 {
		parts = append(parts, SnapshotGetTimeStr+": "+strconv.FormatFloat(s.GetTime.Seconds(), 'f', -1, 64))
	}
	if s.GetKey > 0 {
		parts = append(parts, SnapshotGetKeyStr+": "+strconv.FormatInt(int64(s.GetKey), 10))
	}
	if s.WaitLockTime > 0 {
		parts = append(parts, SnapshotWaitLockStr+": "+strconv.FormatFloat(time.Duration(s.WaitLockTime).Seconds(), 'f', -1, 64))
	}
	if s.RPCCount > 0 {
		parts = append(parts, SnapshotRPCCountStr+": "+strconv.FormatInt(int64(s.RPCCount), 10))
	}
	return parts
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
		&RuntimeStats{int32(*summary.NumIterations), int64(*summary.TimeProcessedNs), int64(*summary.NumProducedRows)})
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
}

// recordOneCopTask record once cop response time to update maxcopRespTime
func (rrs *ReaderRuntimeStats) recordOneCopTask(t time.Duration) {
	rrs.Lock()
	defer rrs.Unlock()
	rrs.copRespTime = append(rrs.copRespTime, t)
}

func (rrs *ReaderRuntimeStats) String() string {
	size := len(rrs.copRespTime)
	if size == 0 {
		return ""
	}
	if size == 1 {
		return fmt.Sprintf("rpc time:%v", rrs.copRespTime[0])
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
	return fmt.Sprintf("rpc max:%v, min:%v, avg:%v, p80:%v, p95:%v", vMax, vMin, vAvg, vP80, vP95)
}

// RuntimeStatsColl collects executors's execution info.
type RuntimeStatsColl struct {
	mu          sync.Mutex
	rootStats   map[string]*RuntimeStats
	copStats    map[string]*CopRuntimeStats
	readerStats map[string]*ReaderRuntimeStats
}

// RuntimeStats collects one executor's execution info.
type RuntimeStats struct {
	// executor's Next() called times.
	loop int32
	// executor consume time.
	consume int64
	// executor return row count.
	rows int64
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
func (e *RuntimeStatsColl) RecordOneReaderStats(planID string, copRespTime time.Duration) {
	readerStats := e.GetReaderStats(planID)
	readerStats.recordOneCopTask(copRespTime)
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

func (e *RuntimeStats) String() string {
	return fmt.Sprintf("time:%v, loops:%d, rows:%d", time.Duration(e.consume), e.loop, e.rows)
}
