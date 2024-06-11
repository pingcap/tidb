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

package stmtsummary

import (
	"bytes"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/stmtsummary"
	"github.com/tikv/client-go/v2/util"
)

// MaxEncodedPlanSizeInBytes is the upper limit of the size of the plan and the binary plan in the stmt summary.
var MaxEncodedPlanSizeInBytes = 1024 * 1024

// StmtRecord represents a statement statistics record.
// StmtRecord is addable and mergable.
type StmtRecord struct {
	// Each record is summarized between [Begin, End).
	Begin int64 `json:"begin"`
	End   int64 `json:"end"`
	// Immutable
	SchemaName    string `json:"schema_name"`
	Digest        string `json:"digest"`
	PlanDigest    string `json:"plan_digest"`
	StmtType      string `json:"stmt_type"`
	NormalizedSQL string `json:"normalized_sql"`
	TableNames    string `json:"table_names"`
	IsInternal    bool   `json:"is_internal"`
	// Basic
	SampleSQL        string   `json:"sample_sql"`
	Charset          string   `json:"charset"`
	Collation        string   `json:"collation"`
	PrevSQL          string   `json:"prev_sql"`
	SamplePlan       string   `json:"sample_plan"`
	SampleBinaryPlan string   `json:"sample_binary_plan"`
	PlanHint         string   `json:"plan_hint"`
	IndexNames       []string `json:"index_names"`
	ExecCount        int64    `json:"exec_count"`
	SumErrors        int      `json:"sum_errors"`
	SumWarnings      int      `json:"sum_warnings"`
	// Latency
	SumLatency        time.Duration `json:"sum_latency"`
	MaxLatency        time.Duration `json:"max_latency"`
	MinLatency        time.Duration `json:"min_latency"`
	SumParseLatency   time.Duration `json:"sum_parse_latency"`
	MaxParseLatency   time.Duration `json:"max_parse_latency"`
	SumCompileLatency time.Duration `json:"sum_compile_latency"`
	MaxCompileLatency time.Duration `json:"max_compile_latency"`
	// Coprocessor
	SumNumCopTasks       int64         `json:"sum_num_cop_tasks"`
	MaxCopProcessTime    time.Duration `json:"max_cop_process_time"`
	MaxCopProcessAddress string        `json:"max_cop_process_address"`
	MaxCopWaitTime       time.Duration `json:"max_cop_wait_time"`
	MaxCopWaitAddress    string        `json:"max_cop_wait_address"`
	// TiKV
	SumProcessTime               time.Duration `json:"sum_process_time"`
	MaxProcessTime               time.Duration `json:"max_process_time"`
	SumWaitTime                  time.Duration `json:"sum_wait_time"`
	MaxWaitTime                  time.Duration `json:"max_wait_time"`
	SumBackoffTime               time.Duration `json:"sum_backoff_time"`
	MaxBackoffTime               time.Duration `json:"max_backoff_time"`
	SumTotalKeys                 int64         `json:"sum_total_keys"`
	MaxTotalKeys                 int64         `json:"max_total_keys"`
	SumProcessedKeys             int64         `json:"sum_processed_keys"`
	MaxProcessedKeys             int64         `json:"max_processed_keys"`
	SumRocksdbDeleteSkippedCount uint64        `json:"sum_rocksdb_delete_skipped_count"`
	MaxRocksdbDeleteSkippedCount uint64        `json:"max_rocksdb_delete_skipped_count"`
	SumRocksdbKeySkippedCount    uint64        `json:"sum_rocksdb_key_skipped_count"`
	MaxRocksdbKeySkippedCount    uint64        `json:"max_rocksdb_key_skipped_count"`
	SumRocksdbBlockCacheHitCount uint64        `json:"sum_rocksdb_block_cache_hit_count"`
	MaxRocksdbBlockCacheHitCount uint64        `json:"max_rocksdb_block_cache_hit_count"`
	SumRocksdbBlockReadCount     uint64        `json:"sum_rocksdb_block_read_count"`
	MaxRocksdbBlockReadCount     uint64        `json:"max_rocksdb_block_read_count"`
	SumRocksdbBlockReadByte      uint64        `json:"sum_rocksdb_block_read_byte"`
	MaxRocksdbBlockReadByte      uint64        `json:"max_rocksdb_block_read_byte"`
	// Txn
	CommitCount          int64               `json:"commit_count"`
	SumGetCommitTsTime   time.Duration       `json:"sum_get_commit_ts_time"`
	MaxGetCommitTsTime   time.Duration       `json:"max_get_commit_ts_time"`
	SumPrewriteTime      time.Duration       `json:"sum_prewrite_time"`
	MaxPrewriteTime      time.Duration       `json:"max_prewrite_time"`
	SumCommitTime        time.Duration       `json:"sum_commit_time"`
	MaxCommitTime        time.Duration       `json:"max_commit_time"`
	SumLocalLatchTime    time.Duration       `json:"sum_local_latch_time"`
	MaxLocalLatchTime    time.Duration       `json:"max_local_latch_time"`
	SumCommitBackoffTime int64               `json:"sum_commit_backoff_time"`
	MaxCommitBackoffTime int64               `json:"max_commit_backoff_time"`
	SumResolveLockTime   int64               `json:"sum_resolve_lock_time"`
	MaxResolveLockTime   int64               `json:"max_resolve_lock_time"`
	SumWriteKeys         int64               `json:"sum_write_keys"`
	MaxWriteKeys         int                 `json:"max_write_keys"`
	SumWriteSize         int64               `json:"sum_write_size"`
	MaxWriteSize         int                 `json:"max_write_size"`
	SumPrewriteRegionNum int64               `json:"sum_prewrite_region_num"`
	MaxPrewriteRegionNum int32               `json:"max_prewrite_region_num"`
	SumTxnRetry          int64               `json:"sum_txn_retry"`
	MaxTxnRetry          int                 `json:"max_txn_retry"`
	SumBackoffTimes      int64               `json:"sum_backoff_times"`
	BackoffTypes         map[string]int      `json:"backoff_types"`
	AuthUsers            map[string]struct{} `json:"auth_users"`
	// Other
	SumMem               int64         `json:"sum_mem"`
	MaxMem               int64         `json:"max_mem"`
	SumDisk              int64         `json:"sum_disk"`
	MaxDisk              int64         `json:"max_disk"`
	SumAffectedRows      uint64        `json:"sum_affected_rows"`
	SumKVTotal           time.Duration `json:"sum_kv_total"`
	SumPDTotal           time.Duration `json:"sum_pd_total"`
	SumBackoffTotal      time.Duration `json:"sum_backoff_total"`
	SumWriteSQLRespTotal time.Duration `json:"sum_write_sql_resp_total"`
	SumResultRows        int64         `json:"sum_result_rows"`
	MaxResultRows        int64         `json:"max_result_rows"`
	MinResultRows        int64         `json:"min_result_rows"`
	Prepared             bool          `json:"prepared"`
	// The first time this type of SQL executes.
	FirstSeen time.Time `json:"first_seen"`
	// The last time this type of SQL executes.
	LastSeen time.Time `json:"last_seen"`
	// Plan cache
	PlanInCache   bool  `json:"plan_in_cache"`
	PlanCacheHits int64 `json:"plan_cache_hits"`
	PlanInBinding bool  `json:"plan_in_binding"`
	// Pessimistic execution retry information.
	ExecRetryCount uint          `json:"exec_retry_count"`
	ExecRetryTime  time.Duration `json:"exec_retry_time"`

	KeyspaceName string `json:"keyspace_name,omitempty"`
	KeyspaceID   uint32 `json:"keyspace_id,omitempty"`
	// request units(RU)
	ResourceGroupName string `json:"resource_group_name"`
	stmtsummary.StmtRUSummary

	PlanCacheUnqualifiedCount int64  `json:"plan_cache_unqualified_count"`
	LastPlanCacheUnqualified  string `json:"last_plan_cache_unqualified"` // the reason why this query is unqualified for the plan cache
}

// NewStmtRecord creates a new StmtRecord from StmtExecInfo.
// StmtExecInfo is only used to initialize the basic information
// of StmtRecord. Next we need to call StmtRecord.Add to add the
// statistics of the StmtExecInfo into the StmtRecord.
func NewStmtRecord(info *stmtsummary.StmtExecInfo) *StmtRecord {
	// Use "," to separate table names to support FIND_IN_SET.
	var buffer bytes.Buffer
	for i, value := range info.StmtCtx.Tables {
		// In `create database` statement, DB name is not empty but table name is empty.
		if len(value.Table) == 0 {
			continue
		}
		buffer.WriteString(strings.ToLower(value.DB))
		buffer.WriteString(".")
		buffer.WriteString(strings.ToLower(value.Table))
		if i < len(info.StmtCtx.Tables)-1 {
			buffer.WriteString(",")
		}
	}
	tableNames := buffer.String()
	planDigest := info.PlanDigest
	if info.PlanDigestGen != nil && len(planDigest) == 0 {
		// It comes here only when the plan is 'Point_Get'.
		planDigest = info.PlanDigestGen()
	}
	// sampleSQL / authUsers(sampleUser) / samplePlan / prevSQL / indexNames store the values shown at the first time,
	// because it compacts performance to update every time.
	samplePlan, planHint, _ := info.PlanGenerator()
	if len(samplePlan) > MaxEncodedPlanSizeInBytes {
		samplePlan = plancodec.PlanDiscardedEncoded
	}
	binPlan := ""
	if info.BinaryPlanGenerator != nil {
		binPlan = info.BinaryPlanGenerator()
		if len(binPlan) > MaxEncodedPlanSizeInBytes {
			binPlan = plancodec.BinaryPlanDiscardedEncoded
		}
	}
	return &StmtRecord{
		SchemaName:    info.SchemaName,
		Digest:        info.Digest,
		PlanDigest:    planDigest,
		StmtType:      info.StmtCtx.StmtType,
		NormalizedSQL: info.NormalizedSQL,
		TableNames:    tableNames,
		IsInternal:    info.IsInternal,
		SampleSQL:     formatSQL(info.OriginalSQL),
		Charset:       info.Charset,
		Collation:     info.Collation,
		// PrevSQL is already truncated to cfg.Log.QueryLogMaxLen.
		PrevSQL: info.PrevSQL,
		// SamplePlan needs to be decoded so it can't be truncated.
		SamplePlan:        samplePlan,
		SampleBinaryPlan:  binPlan,
		PlanHint:          planHint,
		IndexNames:        info.StmtCtx.IndexNames,
		MinLatency:        info.TotalLatency,
		BackoffTypes:      make(map[string]int),
		AuthUsers:         make(map[string]struct{}),
		MinResultRows:     math.MaxInt64,
		Prepared:          info.Prepared,
		FirstSeen:         info.StartTime,
		LastSeen:          info.StartTime,
		KeyspaceName:      info.KeyspaceName,
		KeyspaceID:        info.KeyspaceID,
		ResourceGroupName: info.ResourceGroupName,
	}
}

// Add adds the statistics of StmtExecInfo to StmtRecord.
func (r *StmtRecord) Add(info *stmtsummary.StmtExecInfo) {
	r.IsInternal = r.IsInternal && info.IsInternal
	// Add user to auth users set
	if len(info.User) > 0 {
		r.AuthUsers[info.User] = struct{}{}
	}
	r.ExecCount++
	if !info.Succeed {
		r.SumErrors++
	}
	r.SumWarnings += int(info.StmtCtx.WarningCount())
	// Latency
	r.SumLatency += info.TotalLatency
	if info.TotalLatency > r.MaxLatency {
		r.MaxLatency = info.TotalLatency
	}
	if info.TotalLatency < r.MinLatency {
		r.MinLatency = info.TotalLatency
	}
	r.SumParseLatency += info.ParseLatency
	if info.ParseLatency > r.MaxParseLatency {
		r.MaxParseLatency = info.ParseLatency
	}
	r.SumCompileLatency += info.CompileLatency
	if info.CompileLatency > r.MaxCompileLatency {
		r.MaxCompileLatency = info.CompileLatency
	}
	// Coprocessor
	numCopTasks := int64(info.CopTasks.NumCopTasks)
	r.SumNumCopTasks += numCopTasks
	if info.CopTasks.MaxProcessTime > r.MaxCopProcessTime {
		r.MaxCopProcessTime = info.CopTasks.MaxProcessTime
		r.MaxCopProcessAddress = info.CopTasks.MaxProcessAddress
	}
	if info.CopTasks.MaxWaitTime > r.MaxCopWaitTime {
		r.MaxCopWaitTime = info.CopTasks.MaxWaitTime
		r.MaxCopWaitAddress = info.CopTasks.MaxWaitAddress
	}
	// TiKV
	r.SumProcessTime += info.ExecDetail.TimeDetail.ProcessTime
	if info.ExecDetail.TimeDetail.ProcessTime > r.MaxProcessTime {
		r.MaxProcessTime = info.ExecDetail.TimeDetail.ProcessTime
	}
	r.SumWaitTime += info.ExecDetail.TimeDetail.WaitTime
	if info.ExecDetail.TimeDetail.WaitTime > r.MaxWaitTime {
		r.MaxWaitTime = info.ExecDetail.TimeDetail.WaitTime
	}
	r.SumBackoffTime += info.ExecDetail.BackoffTime
	if info.ExecDetail.BackoffTime > r.MaxBackoffTime {
		r.MaxBackoffTime = info.ExecDetail.BackoffTime
	}
	if info.ExecDetail.ScanDetail != nil {
		r.SumTotalKeys += info.ExecDetail.ScanDetail.TotalKeys
		if info.ExecDetail.ScanDetail.TotalKeys > r.MaxTotalKeys {
			r.MaxTotalKeys = info.ExecDetail.ScanDetail.TotalKeys
		}
		r.SumProcessedKeys += info.ExecDetail.ScanDetail.ProcessedKeys
		if info.ExecDetail.ScanDetail.ProcessedKeys > r.MaxProcessedKeys {
			r.MaxProcessedKeys = info.ExecDetail.ScanDetail.ProcessedKeys
		}
		r.SumRocksdbDeleteSkippedCount += info.ExecDetail.ScanDetail.RocksdbDeleteSkippedCount
		if info.ExecDetail.ScanDetail.RocksdbDeleteSkippedCount > r.MaxRocksdbDeleteSkippedCount {
			r.MaxRocksdbDeleteSkippedCount = info.ExecDetail.ScanDetail.RocksdbDeleteSkippedCount
		}
		r.SumRocksdbKeySkippedCount += info.ExecDetail.ScanDetail.RocksdbKeySkippedCount
		if info.ExecDetail.ScanDetail.RocksdbKeySkippedCount > r.MaxRocksdbKeySkippedCount {
			r.MaxRocksdbKeySkippedCount = info.ExecDetail.ScanDetail.RocksdbKeySkippedCount
		}
		r.SumRocksdbBlockCacheHitCount += info.ExecDetail.ScanDetail.RocksdbBlockCacheHitCount
		if info.ExecDetail.ScanDetail.RocksdbBlockCacheHitCount > r.MaxRocksdbBlockCacheHitCount {
			r.MaxRocksdbBlockCacheHitCount = info.ExecDetail.ScanDetail.RocksdbBlockCacheHitCount
		}
		r.SumRocksdbBlockReadCount += info.ExecDetail.ScanDetail.RocksdbBlockReadCount
		if info.ExecDetail.ScanDetail.RocksdbBlockReadCount > r.MaxRocksdbBlockReadCount {
			r.MaxRocksdbBlockReadCount = info.ExecDetail.ScanDetail.RocksdbBlockReadCount
		}
		r.SumRocksdbBlockReadByte += info.ExecDetail.ScanDetail.RocksdbBlockReadByte
		if info.ExecDetail.ScanDetail.RocksdbBlockReadByte > r.MaxRocksdbBlockReadByte {
			r.MaxRocksdbBlockReadByte = info.ExecDetail.ScanDetail.RocksdbBlockReadByte
		}
	}
	// Txn
	commitDetails := info.ExecDetail.CommitDetail
	if commitDetails != nil {
		r.CommitCount++
		r.SumPrewriteTime += commitDetails.PrewriteTime
		if commitDetails.PrewriteTime > r.MaxPrewriteTime {
			r.MaxPrewriteTime = commitDetails.PrewriteTime
		}
		r.SumCommitTime += commitDetails.CommitTime
		if commitDetails.CommitTime > r.MaxCommitTime {
			r.MaxCommitTime = commitDetails.CommitTime
		}
		r.SumGetCommitTsTime += commitDetails.GetCommitTsTime
		if commitDetails.GetCommitTsTime > r.MaxGetCommitTsTime {
			r.MaxGetCommitTsTime = commitDetails.GetCommitTsTime
		}
		resolveLockTime := atomic.LoadInt64(&commitDetails.ResolveLock.ResolveLockTime)
		r.SumResolveLockTime += resolveLockTime
		if resolveLockTime > r.MaxResolveLockTime {
			r.MaxResolveLockTime = resolveLockTime
		}
		r.SumLocalLatchTime += commitDetails.LocalLatchTime
		if commitDetails.LocalLatchTime > r.MaxLocalLatchTime {
			r.MaxLocalLatchTime = commitDetails.LocalLatchTime
		}
		r.SumWriteKeys += int64(commitDetails.WriteKeys)
		if commitDetails.WriteKeys > r.MaxWriteKeys {
			r.MaxWriteKeys = commitDetails.WriteKeys
		}
		r.SumWriteSize += int64(commitDetails.WriteSize)
		if commitDetails.WriteSize > r.MaxWriteSize {
			r.MaxWriteSize = commitDetails.WriteSize
		}
		prewriteRegionNum := atomic.LoadInt32(&commitDetails.PrewriteRegionNum)
		r.SumPrewriteRegionNum += int64(prewriteRegionNum)
		if prewriteRegionNum > r.MaxPrewriteRegionNum {
			r.MaxPrewriteRegionNum = prewriteRegionNum
		}
		r.SumTxnRetry += int64(commitDetails.TxnRetry)
		if commitDetails.TxnRetry > r.MaxTxnRetry {
			r.MaxTxnRetry = commitDetails.TxnRetry
		}
		commitDetails.Mu.Lock()
		commitBackoffTime := commitDetails.Mu.CommitBackoffTime
		r.SumCommitBackoffTime += commitBackoffTime
		if commitBackoffTime > r.MaxCommitBackoffTime {
			r.MaxCommitBackoffTime = commitBackoffTime
		}
		r.SumBackoffTimes += int64(len(commitDetails.Mu.PrewriteBackoffTypes))
		for _, backoffType := range commitDetails.Mu.PrewriteBackoffTypes {
			r.BackoffTypes[backoffType]++
		}
		r.SumBackoffTimes += int64(len(commitDetails.Mu.CommitBackoffTypes))
		for _, backoffType := range commitDetails.Mu.CommitBackoffTypes {
			r.BackoffTypes[backoffType]++
		}
		commitDetails.Mu.Unlock()
	}
	// Plan cache
	if info.PlanInCache {
		r.PlanInCache = true
		r.PlanCacheHits++
	} else {
		r.PlanInCache = false
	}
	if info.PlanCacheUnqualified != "" {
		r.PlanCacheUnqualifiedCount++
		r.LastPlanCacheUnqualified = info.PlanCacheUnqualified
	}
	// SPM
	if info.PlanInBinding {
		r.PlanInBinding = true
	} else {
		r.PlanInBinding = false
	}
	// Other
	r.SumAffectedRows += info.StmtCtx.AffectedRows()
	r.SumMem += info.MemMax
	if info.MemMax > r.MaxMem {
		r.MaxMem = info.MemMax
	}
	r.SumDisk += info.DiskMax
	if info.DiskMax > r.MaxDisk {
		r.MaxDisk = info.DiskMax
	}
	if info.StartTime.Before(r.FirstSeen) {
		r.FirstSeen = info.StartTime
	}
	if r.LastSeen.Before(info.StartTime) {
		r.LastSeen = info.StartTime
	}
	if info.ExecRetryCount > 0 {
		r.ExecRetryCount += info.ExecRetryCount
		r.ExecRetryTime += info.ExecRetryTime
	}
	if info.ResultRows > 0 {
		r.SumResultRows += info.ResultRows
		if r.MaxResultRows < info.ResultRows {
			r.MaxResultRows = info.ResultRows
		}
		if r.MinResultRows > info.ResultRows {
			r.MinResultRows = info.ResultRows
		}
	} else {
		r.MinResultRows = 0
	}
	r.SumKVTotal += time.Duration(atomic.LoadInt64(&info.TiKVExecDetails.WaitKVRespDuration))
	r.SumPDTotal += time.Duration(atomic.LoadInt64(&info.TiKVExecDetails.WaitPDRespDuration))
	r.SumBackoffTotal += time.Duration(atomic.LoadInt64(&info.TiKVExecDetails.BackoffDuration))
	r.SumWriteSQLRespTotal += info.StmtExecDetails.WriteSQLRespDuration
	// RU
	r.StmtRUSummary.Add(info.RUDetail)
}

// Merge merges the statistics of another StmtRecord to this StmtRecord.
func (r *StmtRecord) Merge(other *StmtRecord) {
	// User
	for user := range other.AuthUsers {
		r.AuthUsers[user] = struct{}{}
	}
	// ExecCount and SumWarnings
	r.ExecCount += other.ExecCount
	r.SumWarnings += other.SumWarnings
	// Latency
	r.SumLatency += other.SumLatency
	if r.MaxLatency < other.MaxLatency {
		r.MaxLatency = other.MaxLatency
	}
	if r.MinLatency > other.MinLatency {
		r.MinLatency = other.MinLatency
	}
	r.SumParseLatency += other.SumParseLatency
	if r.MaxParseLatency < other.MaxParseLatency {
		r.MaxParseLatency = other.MaxParseLatency
	}
	r.SumCompileLatency += other.SumCompileLatency
	if r.MaxCompileLatency < other.MaxCompileLatency {
		r.MaxCompileLatency = other.MaxCompileLatency
	}
	// Coprocessor
	r.SumNumCopTasks += other.SumNumCopTasks
	if r.MaxCopProcessTime < other.MaxCopProcessTime {
		r.MaxCopProcessTime = other.MaxCopProcessTime
		r.MaxCopProcessAddress = other.MaxCopProcessAddress
	}
	if r.MaxCopWaitTime < other.MaxCopWaitTime {
		r.MaxCopWaitTime = other.MaxCopWaitTime
		r.MaxCopWaitAddress = other.MaxCopWaitAddress
	}
	// TiKV
	r.SumProcessTime += other.SumProcessTime
	if r.MaxProcessTime < other.MaxProcessTime {
		r.MaxProcessTime = other.MaxProcessTime
	}
	r.SumWaitTime += other.SumWaitTime
	if r.MaxWaitTime < other.MaxWaitTime {
		r.MaxWaitTime = other.MaxWaitTime
	}
	r.SumBackoffTime += other.SumBackoffTime
	if r.MaxBackoffTime < other.MaxBackoffTime {
		r.MaxBackoffTime = other.MaxBackoffTime
	}
	r.SumTotalKeys += other.SumTotalKeys
	if r.MaxTotalKeys < other.MaxTotalKeys {
		r.MaxTotalKeys = other.MaxTotalKeys
	}
	r.SumProcessedKeys += other.SumProcessedKeys
	if r.MaxProcessedKeys < other.MaxProcessedKeys {
		r.MaxProcessedKeys = other.MaxProcessedKeys
	}
	r.SumRocksdbDeleteSkippedCount += other.SumRocksdbDeleteSkippedCount
	if r.MaxRocksdbDeleteSkippedCount < other.MaxRocksdbDeleteSkippedCount {
		r.MaxRocksdbDeleteSkippedCount = other.MaxRocksdbDeleteSkippedCount
	}
	r.SumRocksdbKeySkippedCount += other.SumRocksdbKeySkippedCount
	if r.MaxRocksdbKeySkippedCount < other.MaxRocksdbKeySkippedCount {
		r.MaxRocksdbKeySkippedCount = other.MaxRocksdbKeySkippedCount
	}
	r.SumRocksdbBlockCacheHitCount += other.SumRocksdbBlockCacheHitCount
	if r.MaxRocksdbBlockCacheHitCount < other.MaxRocksdbBlockCacheHitCount {
		r.MaxRocksdbBlockCacheHitCount = other.MaxRocksdbBlockCacheHitCount
	}
	r.SumRocksdbBlockReadCount += other.SumRocksdbBlockReadCount
	if r.MaxRocksdbBlockReadCount < other.MaxRocksdbBlockReadCount {
		r.MaxRocksdbBlockReadCount = other.MaxRocksdbBlockReadCount
	}
	r.SumRocksdbBlockReadByte += other.SumRocksdbBlockReadByte
	if r.MaxRocksdbBlockReadByte < other.MaxRocksdbBlockReadByte {
		r.MaxRocksdbBlockReadByte = other.MaxRocksdbBlockReadByte
	}
	// Txn
	r.CommitCount += other.CommitCount
	r.SumPrewriteTime += other.SumPrewriteTime
	if r.MaxPrewriteTime < other.MaxPrewriteTime {
		r.MaxPrewriteTime = other.MaxPrewriteTime
	}
	r.SumCommitTime += other.SumCommitTime
	if r.MaxCommitTime < other.MaxCommitTime {
		r.MaxCommitTime = other.MaxCommitTime
	}
	r.SumGetCommitTsTime += other.SumGetCommitTsTime
	if r.MaxGetCommitTsTime < other.MaxGetCommitTsTime {
		r.MaxGetCommitTsTime = other.MaxGetCommitTsTime
	}
	r.SumCommitBackoffTime += other.SumCommitBackoffTime
	if r.MaxCommitBackoffTime < other.MaxCommitBackoffTime {
		r.MaxCommitBackoffTime = other.MaxCommitBackoffTime
	}
	r.SumResolveLockTime += other.SumResolveLockTime
	if r.MaxResolveLockTime < other.MaxResolveLockTime {
		r.MaxResolveLockTime = other.MaxResolveLockTime
	}
	r.SumLocalLatchTime += other.SumLocalLatchTime
	if r.MaxLocalLatchTime < other.MaxLocalLatchTime {
		r.MaxLocalLatchTime = other.MaxLocalLatchTime
	}
	r.SumWriteKeys += other.SumWriteKeys
	if r.MaxWriteKeys < other.MaxWriteKeys {
		r.MaxWriteKeys = other.MaxWriteKeys
	}
	r.SumWriteSize += other.SumWriteSize
	if r.MaxWriteSize < other.MaxWriteSize {
		r.MaxWriteSize = other.MaxWriteSize
	}
	r.SumPrewriteRegionNum += other.SumPrewriteRegionNum
	if r.MaxPrewriteRegionNum < other.MaxPrewriteRegionNum {
		r.MaxPrewriteRegionNum = other.MaxPrewriteRegionNum
	}
	r.SumTxnRetry += other.SumTxnRetry
	if r.MaxTxnRetry < other.MaxTxnRetry {
		r.MaxTxnRetry = other.MaxTxnRetry
	}
	r.SumBackoffTimes += other.SumBackoffTimes
	for backoffType, backoffValue := range other.BackoffTypes {
		_, ok := r.BackoffTypes[backoffType]
		if ok {
			r.BackoffTypes[backoffType] += backoffValue
		} else {
			r.BackoffTypes[backoffType] = backoffValue
		}
	}
	// Plan cache
	r.PlanCacheHits += other.PlanCacheHits
	r.PlanCacheUnqualifiedCount += other.PlanCacheUnqualifiedCount
	if other.LastPlanCacheUnqualified != "" {
		r.LastPlanCacheUnqualified = other.LastPlanCacheUnqualified
	}
	// Other
	r.SumAffectedRows += other.SumAffectedRows
	r.SumMem += other.SumMem
	if r.MaxMem < other.MaxMem {
		r.MaxMem = other.MaxMem
	}
	r.SumDisk += other.SumDisk
	if r.MaxDisk < other.MaxDisk {
		r.MaxDisk = other.MaxDisk
	}
	if r.FirstSeen.After(other.FirstSeen) {
		r.FirstSeen = other.FirstSeen
	}
	if r.LastSeen.Before(other.LastSeen) {
		r.LastSeen = other.LastSeen
	}
	r.ExecRetryCount += other.ExecRetryCount
	r.ExecRetryTime += other.ExecRetryTime
	r.SumKVTotal += other.SumKVTotal
	r.SumPDTotal += other.SumPDTotal
	r.SumBackoffTotal += other.SumBackoffTotal
	r.SumWriteSQLRespTotal += other.SumWriteSQLRespTotal
	r.SumErrors += other.SumErrors
	r.StmtRUSummary.Merge(&other.StmtRUSummary)
}

// Truncate SQL to maxSQLLength.
func formatSQL(sql string) string {
	maxSQLLength := int(maxSQLLength())
	length := len(sql)
	if length > maxSQLLength {
		var result strings.Builder
		result.WriteString(sql[:maxSQLLength])
		fmt.Fprintf(&result, "(len:%d)", length)
		return result.String()
	}
	return sql
}

func maxSQLLength() uint32 {
	if GlobalStmtSummary != nil {
		return GlobalStmtSummary.MaxSQLLength()
	}
	return 4096
}

// GenerateStmtExecInfo4Test generates a new StmtExecInfo for testing purposes.
func GenerateStmtExecInfo4Test(digest string) *stmtsummary.StmtExecInfo {
	tables := []stmtctx.TableEntry{{DB: "db1", Table: "tb1"}, {DB: "db2", Table: "tb2"}}
	indexes := []string{"a"}
	sc := stmtctx.NewStmtCtx()
	sc.StmtType = "Select"
	sc.Tables = tables
	sc.IndexNames = indexes

	stmtExecInfo := &stmtsummary.StmtExecInfo{
		SchemaName:     "schema_name",
		OriginalSQL:    "original_sql1",
		NormalizedSQL:  "normalized_sql",
		Digest:         digest,
		PlanDigest:     "plan_digest",
		PlanGenerator:  func() (string, string, any) { return "", "", nil },
		User:           "user",
		TotalLatency:   10000,
		ParseLatency:   100,
		CompileLatency: 1000,
		CopTasks: &execdetails.CopTasksDetails{
			NumCopTasks:       10,
			AvgProcessTime:    1000,
			P90ProcessTime:    10000,
			MaxProcessAddress: "127",
			MaxProcessTime:    15000,
			AvgWaitTime:       100,
			P90WaitTime:       1000,
			MaxWaitAddress:    "128",
			MaxWaitTime:       1500,
		},
		ExecDetail: &execdetails.ExecDetails{
			BackoffTime:  80,
			RequestCount: 10,
			CommitDetail: &util.CommitDetails{
				GetCommitTsTime: 100,
				PrewriteTime:    10000,
				CommitTime:      1000,
				LocalLatchTime:  10,
				Mu: struct {
					sync.Mutex
					CommitBackoffTime    int64
					PrewriteBackoffTypes []string
					CommitBackoffTypes   []string
					SlowestPrewrite      util.ReqDetailInfo
					CommitPrimary        util.ReqDetailInfo
				}{
					CommitBackoffTime:    200,
					PrewriteBackoffTypes: []string{"txnlock"},
					CommitBackoffTypes:   []string{},
					SlowestPrewrite:      util.ReqDetailInfo{},
					CommitPrimary:        util.ReqDetailInfo{},
				},
				WriteKeys:         20000,
				WriteSize:         200000,
				PrewriteRegionNum: 20,
				TxnRetry:          2,
				ResolveLock: util.ResolveLockDetail{
					ResolveLockTime: 2000,
				},
			},
			ScanDetail: &util.ScanDetail{
				TotalKeys:                 1000,
				ProcessedKeys:             500,
				RocksdbDeleteSkippedCount: 100,
				RocksdbKeySkippedCount:    10,
				RocksdbBlockCacheHitCount: 10,
				RocksdbBlockReadCount:     10,
				RocksdbBlockReadByte:      1000,
			},
			DetailsNeedP90: execdetails.DetailsNeedP90{
				TimeDetail: util.TimeDetail{
					ProcessTime: 500,
					WaitTime:    50,
				},
				CalleeAddress: "129",
			},
		},
		StmtCtx:           sc,
		MemMax:            10000,
		DiskMax:           10000,
		StartTime:         time.Date(2019, 1, 1, 10, 10, 10, 10, time.UTC),
		Succeed:           true,
		KeyspaceName:      "keyspace_a",
		KeyspaceID:        1,
		ResourceGroupName: "rg1",
		RUDetail:          util.NewRUDetailsWith(1.2, 3.4, 2*time.Millisecond),
	}
	stmtExecInfo.StmtCtx.AddAffectedRows(10000)
	return stmtExecInfo
}
