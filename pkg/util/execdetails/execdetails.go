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
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/tdigest"
	"github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
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

// P90BackoffSummary contains execution summary for a backoff type.
type P90BackoffSummary struct {
	ReqTimes          int
	BackoffPercentile Percentile[DurationWithAddr]
	TotBackoffTime    time.Duration
	TotBackoffTimes   int
}

// P90Summary contains execution summary for cop tasks.
type P90Summary struct {
	NumCopTasks int

	ProcessTimePercentile Percentile[DurationWithAddr]
	WaitTimePercentile    Percentile[DurationWithAddr]

	BackoffInfo map[string]*P90BackoffSummary
}

// MaxDetailsNumsForOneQuery is the max number of details to keep for P90 for one query.
const MaxDetailsNumsForOneQuery = 1000

// Reset resets all fields in DetailsNeedP90Summary.
func (d *P90Summary) Reset() {
	d.NumCopTasks = 0
	d.ProcessTimePercentile = Percentile[DurationWithAddr]{}
	d.WaitTimePercentile = Percentile[DurationWithAddr]{}
	d.BackoffInfo = make(map[string]*P90BackoffSummary)
}

// Merge merges DetailsNeedP90 into P90Summary.
func (d *P90Summary) Merge(detail *DetailsNeedP90) {
	if d.BackoffInfo == nil {
		d.Reset()
	}
	d.NumCopTasks++
	d.ProcessTimePercentile.Add(DurationWithAddr{detail.TimeDetail.ProcessTime, detail.CalleeAddress})
	d.WaitTimePercentile.Add(DurationWithAddr{detail.TimeDetail.WaitTime, detail.CalleeAddress})

	var info *P90BackoffSummary
	var ok bool
	for backoff, timeItem := range detail.BackoffTimes {
		if info, ok = d.BackoffInfo[backoff]; !ok {
			d.BackoffInfo[backoff] = &P90BackoffSummary{}
			info = d.BackoffInfo[backoff]
		}
		sleepItem := detail.BackoffSleep[backoff]
		info.ReqTimes++
		info.TotBackoffTime += sleepItem
		info.TotBackoffTimes += timeItem

		info.BackoffPercentile.Add(DurationWithAddr{sleepItem, detail.CalleeAddress})
	}
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
			parts = append(parts, CommitPrimaryRPCDetailStr+": {total:"+strconv.FormatFloat(commitDetails.Mu.CommitPrimary.ReqTotalTime.Seconds(), 'f', 3, 64)+
				"s, region_id: "+strconv.FormatUint(commitDetails.Mu.CommitPrimary.Region, 10)+
				", store: "+commitDetails.Mu.CommitPrimary.StoreAddr+
				", "+commitDetails.Mu.CommitPrimary.ExecDetails.String()+"}")
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
			fields = append(fields, zap.String(CommitPrimaryRPCDetailStr, "{total:"+strconv.FormatFloat(commitDetails.Mu.CommitPrimary.ReqTotalTime.Seconds(), 'f', 3, 64)+
				"s, region_id: "+strconv.FormatUint(commitDetails.Mu.CommitPrimary.Region, 10)+
				", store: "+commitDetails.Mu.CommitPrimary.StoreAddr+
				", "+commitDetails.Mu.CommitPrimary.ExecDetails.String()+"}"))
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

// SyncExecDetails is a synced version of `ExecDetails` and its `P90Summary`
type SyncExecDetails struct {
	mu sync.Mutex

	execDetails    ExecDetails
	detailsSummary P90Summary
}

// MergeExecDetails merges a single region execution details into self, used to print
// the information in slow query log.
func (s *SyncExecDetails) MergeExecDetails(details *ExecDetails, commitDetails *util.CommitDetails) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if details != nil {
		s.execDetails.CopTime += details.CopTime
		s.execDetails.BackoffTime += details.BackoffTime
		s.execDetails.RequestCount++
		s.mergeScanDetail(details.ScanDetail)
		s.mergeTimeDetail(details.TimeDetail)
		detail := &DetailsNeedP90{
			BackoffSleep:  details.BackoffSleep,
			BackoffTimes:  details.BackoffTimes,
			CalleeAddress: details.CalleeAddress,
			TimeDetail:    details.TimeDetail,
		}
		s.detailsSummary.Merge(detail)
	}
	if commitDetails != nil {
		if s.execDetails.CommitDetail == nil {
			s.execDetails.CommitDetail = commitDetails
		} else {
			s.execDetails.CommitDetail.Merge(commitDetails)
		}
	}
}

// mergeScanDetail merges scan details into self.
func (s *SyncExecDetails) mergeScanDetail(scanDetail *util.ScanDetail) {
	// Currently TiFlash cop task does not fill scanDetail, so need to skip it if scanDetail is nil
	if scanDetail == nil {
		return
	}
	if s.execDetails.ScanDetail == nil {
		s.execDetails.ScanDetail = &util.ScanDetail{}
	}
	s.execDetails.ScanDetail.Merge(scanDetail)
}

// MergeTimeDetail merges time details into self.
func (s *SyncExecDetails) mergeTimeDetail(timeDetail util.TimeDetail) {
	s.execDetails.TimeDetail.ProcessTime += timeDetail.ProcessTime
	s.execDetails.TimeDetail.WaitTime += timeDetail.WaitTime
}

// MergeLockKeysExecDetails merges lock keys execution details into self.
func (s *SyncExecDetails) MergeLockKeysExecDetails(lockKeys *util.LockKeysDetails) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.execDetails.LockKeysDetail == nil {
		s.execDetails.LockKeysDetail = lockKeys
	} else {
		s.execDetails.LockKeysDetail.Merge(lockKeys)
	}
}

// Reset resets the content inside
func (s *SyncExecDetails) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.execDetails = ExecDetails{}
	s.detailsSummary.Reset()
}

// GetExecDetails returns the exec details inside.
// It's actually not safe, because the `ExecDetails` still contains some reference, which is not protected after returning
// outside.
func (s *SyncExecDetails) GetExecDetails() ExecDetails {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.execDetails
}

// CopTasksDetails returns some useful information of cop-tasks during execution.
func (s *SyncExecDetails) CopTasksDetails() *CopTasksDetails {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.detailsSummary.NumCopTasks
	if n == 0 {
		return nil
	}
	d := &CopTasksDetails{NumCopTasks: n}
	d.TotProcessTime = s.execDetails.TimeDetail.ProcessTime
	d.AvgProcessTime = d.TotProcessTime / time.Duration(n)

	d.TotWaitTime = s.execDetails.TimeDetail.WaitTime
	d.AvgWaitTime = d.TotWaitTime / time.Duration(n)

	d.P90ProcessTime = time.Duration((s.detailsSummary.ProcessTimePercentile.GetPercentile(0.9)))
	d.MaxProcessTime = s.detailsSummary.ProcessTimePercentile.GetMax().D
	d.MaxProcessAddress = s.detailsSummary.ProcessTimePercentile.GetMax().Addr

	d.P90WaitTime = time.Duration((s.detailsSummary.WaitTimePercentile.GetPercentile(0.9)))
	d.MaxWaitTime = s.detailsSummary.WaitTimePercentile.GetMax().D
	d.MaxWaitAddress = s.detailsSummary.WaitTimePercentile.GetMax().Addr

	if len(s.detailsSummary.BackoffInfo) > 0 {
		d.MaxBackoffTime = make(map[string]time.Duration)
		d.AvgBackoffTime = make(map[string]time.Duration)
		d.P90BackoffTime = make(map[string]time.Duration)
		d.TotBackoffTime = make(map[string]time.Duration)
		d.TotBackoffTimes = make(map[string]int)
		d.MaxBackoffAddress = make(map[string]string)
	}
	for backoff, items := range s.detailsSummary.BackoffInfo {
		if items == nil {
			continue
		}
		n := items.ReqTimes
		d.MaxBackoffAddress[backoff] = items.BackoffPercentile.GetMax().Addr
		d.MaxBackoffTime[backoff] = items.BackoffPercentile.GetMax().D
		d.P90BackoffTime[backoff] = time.Duration(items.BackoffPercentile.GetPercentile(0.9))

		d.AvgBackoffTime[backoff] = items.TotBackoffTime / time.Duration(n)
		d.TotBackoffTime[backoff] = items.TotBackoffTime
		d.TotBackoffTimes[backoff] = items.TotBackoffTimes
	}
	return d
}

// CopTasksSummary returns some summary information of cop-tasks for statement summary.
func (s *SyncExecDetails) CopTasksSummary() *CopTasksSummary {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.detailsSummary.NumCopTasks
	if n == 0 {
		return nil
	}
	return &CopTasksSummary{
		NumCopTasks:       n,
		MaxProcessAddress: s.detailsSummary.ProcessTimePercentile.GetMax().Addr,
		MaxProcessTime:    s.detailsSummary.ProcessTimePercentile.GetMax().D,
		TotProcessTime:    s.execDetails.TimeDetail.ProcessTime,
		MaxWaitAddress:    s.detailsSummary.WaitTimePercentile.GetMax().Addr,
		MaxWaitTime:       s.detailsSummary.WaitTimePercentile.GetMax().D,
		TotWaitTime:       s.execDetails.TimeDetail.WaitTime,
	}
}

// CopTasksDetails collects some useful information of cop-tasks during execution.
type CopTasksDetails struct {
	NumCopTasks int

	AvgProcessTime    time.Duration
	P90ProcessTime    time.Duration
	MaxProcessAddress string
	MaxProcessTime    time.Duration
	TotProcessTime    time.Duration

	AvgWaitTime    time.Duration
	P90WaitTime    time.Duration
	MaxWaitAddress string
	MaxWaitTime    time.Duration
	TotWaitTime    time.Duration

	MaxBackoffTime    map[string]time.Duration
	MaxBackoffAddress map[string]string
	AvgBackoffTime    map[string]time.Duration
	P90BackoffTime    map[string]time.Duration
	TotBackoffTime    map[string]time.Duration
	TotBackoffTimes   map[string]int
}

// CopTasksSummary collects some summary information of cop-tasks for statement summary.
type CopTasksSummary struct {
	NumCopTasks       int
	MaxProcessAddress string
	MaxProcessTime    time.Duration
	TotProcessTime    time.Duration
	MaxWaitAddress    string
	MaxWaitTime       time.Duration
	TotWaitTime       time.Duration
}

// ToZapFields wraps the CopTasksDetails as zap.Fileds.
func (d *CopTasksDetails) ToZapFields() (fields []zap.Field) {
	if d == nil || d.NumCopTasks == 0 {
		return
	}
	fields = make([]zap.Field, 0, 10)
	fields = append(fields, zap.Int("num_cop_tasks", d.NumCopTasks))
	fields = append(fields, zap.String("process_avg_time", strconv.FormatFloat(d.AvgProcessTime.Seconds(), 'f', -1, 64)+"s"))
	fields = append(fields, zap.String("process_p90_time", strconv.FormatFloat(d.P90ProcessTime.Seconds(), 'f', -1, 64)+"s"))
	fields = append(fields, zap.String("process_max_time", strconv.FormatFloat(d.MaxProcessTime.Seconds(), 'f', -1, 64)+"s"))
	fields = append(fields, zap.String("process_max_addr", d.MaxProcessAddress))
	fields = append(fields, zap.String("wait_avg_time", strconv.FormatFloat(d.AvgWaitTime.Seconds(), 'f', -1, 64)+"s"))
	fields = append(fields, zap.String("wait_p90_time", strconv.FormatFloat(d.P90WaitTime.Seconds(), 'f', -1, 64)+"s"))
	fields = append(fields, zap.String("wait_max_time", strconv.FormatFloat(d.MaxWaitTime.Seconds(), 'f', -1, 64)+"s"))
	fields = append(fields, zap.String("wait_max_addr", d.MaxWaitAddress))
	return fields
}

type basicCopRuntimeStats struct {
	storeType kv.StoreType
	BasicRuntimeStats
	threads    int32
	totalTasks int32
	procTimes  Percentile[Duration]
	// executor extra infos
	tiflashScanContext TiFlashScanContext
	tiflashWaitSummary TiFlashWaitSummary
}

type canGetFloat64 interface {
	GetFloat64() float64
}

// Int64 is a wrapper of int64 to implement the canGetFloat64 interface.
type Int64 int64

// GetFloat64 implements the canGetFloat64 interface.
func (i Int64) GetFloat64() float64 { return float64(i) }

// Duration is a wrapper of time.Duration to implement the canGetFloat64 interface.
type Duration time.Duration

// GetFloat64 implements the canGetFloat64 interface.
func (d Duration) GetFloat64() float64 { return float64(d) }

// DurationWithAddr is a wrapper of time.Duration and string to implement the canGetFloat64 interface.
type DurationWithAddr struct {
	D    time.Duration
	Addr string
}

// GetFloat64 implements the canGetFloat64 interface.
func (d DurationWithAddr) GetFloat64() float64 { return float64(d.D) }

// Percentile is a struct to calculate the percentile of a series of values.
type Percentile[valueType canGetFloat64] struct {
	values   []valueType
	size     int
	isSorted bool

	minVal valueType
	maxVal valueType
	sumVal float64
	dt     *tdigest.TDigest
}

// Add adds a value to calculate the percentile.
func (p *Percentile[valueType]) Add(value valueType) {
	p.isSorted = false
	p.sumVal += value.GetFloat64()
	p.size++
	if p.dt == nil && len(p.values) == 0 {
		p.minVal = value
		p.maxVal = value
	} else {
		if value.GetFloat64() < p.minVal.GetFloat64() {
			p.minVal = value
		}
		if value.GetFloat64() > p.maxVal.GetFloat64() {
			p.maxVal = value
		}
	}
	if p.dt == nil {
		p.values = append(p.values, value)
		if len(p.values) >= MaxDetailsNumsForOneQuery {
			p.dt = tdigest.New()
			for _, v := range p.values {
				p.dt.Add(v.GetFloat64(), 1)
			}
			p.values = nil
		}
		return
	}
	p.dt.Add(value.GetFloat64(), 1)
}

// GetPercentile returns the percentile `f` of the values.
func (p *Percentile[valueType]) GetPercentile(f float64) float64 {
	if p.dt == nil {
		if !p.isSorted {
			p.isSorted = true
			sort.Slice(p.values, func(i, j int) bool {
				return p.values[i].GetFloat64() < p.values[j].GetFloat64()
			})
		}
		return p.values[int(float64(len(p.values))*f)].GetFloat64()
	}
	return p.dt.Quantile(f)
}

// GetMax returns the max value.
func (p *Percentile[valueType]) GetMax() valueType {
	return p.maxVal
}

// GetMin returns the min value.
func (p *Percentile[valueType]) GetMin() valueType {
	return p.minVal
}

// MergePercentile merges two Percentile.
func (p *Percentile[valueType]) MergePercentile(p2 *Percentile[valueType]) {
	p.isSorted = false
	if p2.dt == nil {
		for _, v := range p2.values {
			p.Add(v)
		}
		return
	}
	p.sumVal += p2.sumVal
	p.size += p2.size
	if p.dt == nil {
		p.dt = tdigest.New()
		for _, v := range p.values {
			p.dt.Add(v.GetFloat64(), 1)
		}
		p.values = nil
	}
	p.dt.AddCentroidList(p2.dt.Centroids())
}

// Size returns the size of the values.
func (p *Percentile[valueType]) Size() int {
	return p.size
}

// Sum returns the sum of the values.
func (p *Percentile[valueType]) Sum() float64 {
	return p.sumVal
}

// String implements the RuntimeStats interface.
func (e *basicCopRuntimeStats) String() string {
	buf := bytes.NewBuffer(make([]byte, 0, 16))
	buf.WriteString("time:")
	buf.WriteString(FormatDuration(time.Duration(e.consume.Load())))
	buf.WriteString(", loops:")
	buf.WriteString(strconv.Itoa(int(e.loop.Load())))
	if e.storeType == kv.TiFlash {
		buf.WriteString(", threads:")
		buf.WriteString(strconv.Itoa(int(e.threads)))
		buf.WriteString(", ")
		if e.tiflashWaitSummary.CanBeIgnored() {
			buf.WriteString(e.tiflashScanContext.String())
		} else {
			buf.WriteString(e.tiflashWaitSummary.String())
			buf.WriteString(", ")
			buf.WriteString(e.tiflashScanContext.String())
		}
	}
	return buf.String()
}

// Clone implements the RuntimeStats interface.
func (e *basicCopRuntimeStats) Clone() RuntimeStats {
	stats := &basicCopRuntimeStats{
		BasicRuntimeStats: BasicRuntimeStats{},
		threads:           e.threads,
		storeType:         e.storeType,
		totalTasks:        e.totalTasks,
		procTimes:         e.procTimes,
	}
	stats.loop.Store(e.loop.Load())
	stats.consume.Store(e.consume.Load())
	stats.rows.Store(e.rows.Load())
	stats.tiflashScanContext = e.tiflashScanContext.Clone()
	stats.tiflashWaitSummary = e.tiflashWaitSummary.Clone()
	return stats
}

// Merge implements the RuntimeStats interface.
func (e *basicCopRuntimeStats) Merge(rs RuntimeStats) {
	tmp, ok := rs.(*basicCopRuntimeStats)
	if !ok {
		return
	}
	e.loop.Add(tmp.loop.Load())
	e.consume.Add(tmp.consume.Load())
	e.rows.Add(tmp.rows.Load())
	e.threads += tmp.threads
	e.totalTasks += tmp.totalTasks
	if tmp.procTimes.Size() == 0 {
		e.procTimes.Add(Duration(tmp.consume.Load()))
	} else {
		e.procTimes.MergePercentile(&tmp.procTimes)
	}
	e.tiflashScanContext.Merge(tmp.tiflashScanContext)
	e.tiflashWaitSummary.Merge(tmp.tiflashWaitSummary)
}

// mergeExecSummary likes Merge, but it merges ExecutorExecutionSummary directly.
func (e *basicCopRuntimeStats) mergeExecSummary(summary *tipb.ExecutorExecutionSummary) {
	e.loop.Add(int32(*summary.NumIterations))
	e.consume.Add(int64(*summary.TimeProcessedNs))
	e.rows.Add(int64(*summary.NumProducedRows))
	e.threads += int32(summary.GetConcurrency())
	e.totalTasks++
	e.procTimes.Add(Duration(int64(*summary.TimeProcessedNs)))
	if tiflashScanContext := summary.GetTiflashScanContext(); tiflashScanContext != nil {
		var regionsOfInstance map[string]uint64
		if len(tiflashScanContext.GetRegionsOfInstance()) > 0 {
			regionsOfInstance = make(map[string]uint64)
			for _, instance := range tiflashScanContext.GetRegionsOfInstance() {
				regionsOfInstance[instance.GetInstanceId()] = instance.GetRegionNum()
			}
		}
		e.tiflashScanContext.Merge(TiFlashScanContext{
			dmfileDataScannedRows:     tiflashScanContext.GetDmfileDataScannedRows(),
			dmfileDataSkippedRows:     tiflashScanContext.GetDmfileDataSkippedRows(),
			dmfileMvccScannedRows:     tiflashScanContext.GetDmfileMvccScannedRows(),
			dmfileMvccSkippedRows:     tiflashScanContext.GetDmfileMvccSkippedRows(),
			dmfileLmFilterScannedRows: tiflashScanContext.GetDmfileLmFilterScannedRows(),
			dmfileLmFilterSkippedRows: tiflashScanContext.GetDmfileLmFilterSkippedRows(),
			totalDmfileRsCheckMs:      tiflashScanContext.GetTotalDmfileRsCheckMs(),
			totalDmfileReadMs:         tiflashScanContext.GetTotalDmfileReadMs(),
			totalBuildSnapshotMs:      tiflashScanContext.GetTotalBuildSnapshotMs(),
			localRegions:              tiflashScanContext.GetLocalRegions(),
			remoteRegions:             tiflashScanContext.GetRemoteRegions(),
			totalLearnerReadMs:        tiflashScanContext.GetTotalLearnerReadMs(),
			disaggReadCacheHitBytes:   tiflashScanContext.GetDisaggReadCacheHitBytes(),
			disaggReadCacheMissBytes:  tiflashScanContext.GetDisaggReadCacheMissBytes(),
			segments:                  tiflashScanContext.GetSegments(),
			readTasks:                 tiflashScanContext.GetReadTasks(),
			deltaRows:                 tiflashScanContext.GetDeltaRows(),
			deltaBytes:                tiflashScanContext.GetDeltaBytes(),
			mvccInputRows:             tiflashScanContext.GetMvccInputRows(),
			mvccInputBytes:            tiflashScanContext.GetMvccInputBytes(),
			mvccOutputRows:            tiflashScanContext.GetMvccOutputRows(),
			totalBuildBitmapMs:        tiflashScanContext.GetTotalBuildBitmapMs(),
			totalBuildInputStreamMs:   tiflashScanContext.GetTotalBuildInputstreamMs(),
			staleReadRegions:          tiflashScanContext.GetStaleReadRegions(),
			minLocalStreamMs:          tiflashScanContext.GetMinLocalStreamMs(),
			maxLocalStreamMs:          tiflashScanContext.GetMaxLocalStreamMs(),
			minRemoteStreamMs:         tiflashScanContext.GetMinRemoteStreamMs(),
			maxRemoteStreamMs:         tiflashScanContext.GetMaxRemoteStreamMs(),
			regionsOfInstance:         regionsOfInstance,

			totalVectorIdxLoadFromS3:           tiflashScanContext.GetTotalVectorIdxLoadFromS3(),
			totalVectorIdxLoadFromDisk:         tiflashScanContext.GetTotalVectorIdxLoadFromDisk(),
			totalVectorIdxLoadFromCache:        tiflashScanContext.GetTotalVectorIdxLoadFromCache(),
			totalVectorIdxLoadTimeMs:           tiflashScanContext.GetTotalVectorIdxLoadTimeMs(),
			totalVectorIdxSearchTimeMs:         tiflashScanContext.GetTotalVectorIdxSearchTimeMs(),
			totalVectorIdxSearchVisitedNodes:   tiflashScanContext.GetTotalVectorIdxSearchVisitedNodes(),
			totalVectorIdxSearchDiscardedNodes: tiflashScanContext.GetTotalVectorIdxSearchDiscardedNodes(),
			totalVectorIdxReadVecTimeMs:        tiflashScanContext.GetTotalVectorIdxReadVecTimeMs(),
			totalVectorIdxReadOthersTimeMs:     tiflashScanContext.GetTotalVectorIdxReadOthersTimeMs(),
		})
	}
	if tiflashWaitSummary := summary.GetTiflashWaitSummary(); tiflashWaitSummary != nil {
		e.tiflashWaitSummary.Merge(TiFlashWaitSummary{
			executionTime:           *summary.TimeProcessedNs,
			minTSOWaitTime:          tiflashWaitSummary.GetMinTSOWaitNs(),
			pipelineBreakerWaitTime: tiflashWaitSummary.GetPipelineBreakerWaitNs(),
			pipelineQueueWaitTime:   tiflashWaitSummary.GetPipelineQueueWaitNs(),
		})
	}
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
	timeDetail *util.TimeDetail
	storeType  kv.StoreType
	sync.Mutex
}

// RecordOneCopTask records a specific cop tasks's execution detail.
func (crs *CopRuntimeStats) RecordOneCopTask(address string, summary *tipb.ExecutorExecutionSummary) {
	crs.Lock()
	defer crs.Unlock()

	stats := crs.stats[address]
	if stats == nil {
		stats = &basicCopRuntimeStats{storeType: crs.storeType}
		crs.stats[address] = stats
	}
	stats.mergeExecSummary(summary)
}

// GetActRows return total rows of CopRuntimeStats.
func (crs *CopRuntimeStats) GetActRows() (totalRows int64) {
	for _, instanceStats := range crs.stats {
		totalRows += instanceStats.rows.Load()
	}
	return totalRows
}

// GetTasks return total tasks of CopRuntimeStats
func (crs *CopRuntimeStats) GetTasks() (totalTasks int32) {
	for _, instanceStats := range crs.stats {
		totalTasks += instanceStats.totalTasks
	}
	return totalTasks
}

// MergeBasicStats traverses basicCopRuntimeStats in the CopRuntimeStats and collects some useful information.
func (crs *CopRuntimeStats) MergeBasicStats() (procTimes Percentile[Duration], totalTime time.Duration, totalTasks, totalLoops, totalThreads int32, totalTiFlashScanContext TiFlashScanContext, totalTiFlashWaitSummary TiFlashWaitSummary) {
	totalTiFlashScanContext = TiFlashScanContext{
		regionsOfInstance: make(map[string]uint64),
	}
	for _, instanceStats := range crs.stats {
		procTimes.MergePercentile(&instanceStats.procTimes)
		totalTime += time.Duration(instanceStats.consume.Load())
		totalLoops += instanceStats.loop.Load()
		totalThreads += instanceStats.threads
		totalTiFlashScanContext.Merge(instanceStats.tiflashScanContext)
		totalTiFlashWaitSummary.Merge(instanceStats.tiflashWaitSummary)
		totalTasks += instanceStats.totalTasks
	}
	return
}

func (crs *CopRuntimeStats) String() string {
	if len(crs.stats) == 0 {
		return ""
	}

	procTimes, totalTime, totalTasks, totalLoops, totalThreads, totalTiFlashScanContext, totalTiFlashWaitSummary := crs.MergeBasicStats()
	avgTime := time.Duration(totalTime.Nanoseconds() / int64(totalTasks))
	isTiFlashCop := crs.storeType == kv.TiFlash

	buf := bytes.NewBuffer(make([]byte, 0, 16))
	{
		printTiFlashSpecificInfo := func() {
			if isTiFlashCop {
				buf.WriteString(", ")
				buf.WriteString("threads:")
				buf.WriteString(strconv.Itoa(int(totalThreads)))
				buf.WriteString("}")
				if !totalTiFlashWaitSummary.CanBeIgnored() {
					buf.WriteString(", ")
					buf.WriteString(totalTiFlashWaitSummary.String())
				}
				if !totalTiFlashScanContext.Empty() {
					buf.WriteString(", ")
					buf.WriteString(totalTiFlashScanContext.String())
				}
			} else {
				buf.WriteString("}")
			}
		}
		if totalTasks == 1 {
			buf.WriteString(crs.storeType.Name())
			buf.WriteString("_task:{time:")
			buf.WriteString(FormatDuration(time.Duration(procTimes.GetPercentile(0))))
			buf.WriteString(", loops:")
			buf.WriteString(strconv.Itoa(int(totalLoops)))
			printTiFlashSpecificInfo()
		} else {
			buf.WriteString(crs.storeType.Name())
			buf.WriteString("_task:{proc max:")
			buf.WriteString(FormatDuration(time.Duration(procTimes.GetMax().GetFloat64())))
			buf.WriteString(", min:")
			buf.WriteString(FormatDuration(time.Duration(procTimes.GetMin().GetFloat64())))
			buf.WriteString(", avg: ")
			buf.WriteString(FormatDuration(avgTime))
			buf.WriteString(", p80:")
			buf.WriteString(FormatDuration(time.Duration(procTimes.GetPercentile(0.8))))
			buf.WriteString(", p95:")
			buf.WriteString(FormatDuration(time.Duration(procTimes.GetPercentile(0.95))))
			buf.WriteString(", iters:")
			buf.WriteString(strconv.Itoa(int(totalLoops)))
			buf.WriteString(", tasks:")
			buf.WriteString(strconv.Itoa(int(totalTasks)))
			printTiFlashSpecificInfo()
		}
	}
	if !isTiFlashCop {
		if crs.scanDetail != nil {
			detail := crs.scanDetail.String()
			if detail != "" {
				buf.WriteString(", ")
				buf.WriteString(detail)
			}
		}
		if crs.timeDetail != nil {
			timeDetailStr := crs.timeDetail.String()
			if timeDetailStr != "" {
				buf.WriteString(", ")
				buf.WriteString(timeDetailStr)
			}
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
	// TpRURuntimeStats is the tp for RURuntimeStats
	TpRURuntimeStats
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
	dmfileDataScannedRows     uint64
	dmfileDataSkippedRows     uint64
	dmfileMvccScannedRows     uint64
	dmfileMvccSkippedRows     uint64
	dmfileLmFilterScannedRows uint64
	dmfileLmFilterSkippedRows uint64
	totalDmfileRsCheckMs      uint64
	totalDmfileReadMs         uint64
	totalBuildSnapshotMs      uint64
	localRegions              uint64
	remoteRegions             uint64
	totalLearnerReadMs        uint64
	disaggReadCacheHitBytes   uint64
	disaggReadCacheMissBytes  uint64
	segments                  uint64
	readTasks                 uint64
	deltaRows                 uint64
	deltaBytes                uint64
	mvccInputRows             uint64
	mvccInputBytes            uint64
	mvccOutputRows            uint64
	totalBuildBitmapMs        uint64
	totalBuildInputStreamMs   uint64
	staleReadRegions          uint64
	minLocalStreamMs          uint64
	maxLocalStreamMs          uint64
	minRemoteStreamMs         uint64
	maxRemoteStreamMs         uint64
	regionsOfInstance         map[string]uint64

	totalVectorIdxLoadFromS3           uint64
	totalVectorIdxLoadFromDisk         uint64
	totalVectorIdxLoadFromCache        uint64
	totalVectorIdxLoadTimeMs           uint64
	totalVectorIdxSearchTimeMs         uint64
	totalVectorIdxSearchVisitedNodes   uint64
	totalVectorIdxSearchDiscardedNodes uint64
	totalVectorIdxReadVecTimeMs        uint64
	totalVectorIdxReadOthersTimeMs     uint64
}

// Clone implements the deep copy of * TiFlashshScanContext
func (context *TiFlashScanContext) Clone() TiFlashScanContext {
	newContext := TiFlashScanContext{
		dmfileDataScannedRows:     context.dmfileDataScannedRows,
		dmfileDataSkippedRows:     context.dmfileDataSkippedRows,
		dmfileMvccScannedRows:     context.dmfileMvccScannedRows,
		dmfileMvccSkippedRows:     context.dmfileMvccSkippedRows,
		dmfileLmFilterScannedRows: context.dmfileLmFilterScannedRows,
		dmfileLmFilterSkippedRows: context.dmfileLmFilterSkippedRows,
		totalDmfileRsCheckMs:      context.totalDmfileRsCheckMs,
		totalDmfileReadMs:         context.totalDmfileReadMs,
		totalBuildSnapshotMs:      context.totalBuildSnapshotMs,
		localRegions:              context.localRegions,
		remoteRegions:             context.remoteRegions,
		totalLearnerReadMs:        context.totalLearnerReadMs,
		disaggReadCacheHitBytes:   context.disaggReadCacheHitBytes,
		disaggReadCacheMissBytes:  context.disaggReadCacheMissBytes,
		segments:                  context.segments,
		readTasks:                 context.readTasks,
		deltaRows:                 context.deltaRows,
		deltaBytes:                context.deltaBytes,
		mvccInputRows:             context.mvccInputRows,
		mvccInputBytes:            context.mvccInputBytes,
		mvccOutputRows:            context.mvccOutputRows,
		totalBuildBitmapMs:        context.totalBuildBitmapMs,
		totalBuildInputStreamMs:   context.totalBuildInputStreamMs,
		staleReadRegions:          context.staleReadRegions,
		minLocalStreamMs:          context.minLocalStreamMs,
		maxLocalStreamMs:          context.maxLocalStreamMs,
		minRemoteStreamMs:         context.minRemoteStreamMs,
		maxRemoteStreamMs:         context.maxRemoteStreamMs,
		regionsOfInstance:         make(map[string]uint64),

		totalVectorIdxLoadFromS3:           context.totalVectorIdxLoadFromS3,
		totalVectorIdxLoadFromDisk:         context.totalVectorIdxLoadFromDisk,
		totalVectorIdxLoadFromCache:        context.totalVectorIdxLoadFromCache,
		totalVectorIdxLoadTimeMs:           context.totalVectorIdxLoadTimeMs,
		totalVectorIdxSearchTimeMs:         context.totalVectorIdxSearchTimeMs,
		totalVectorIdxSearchVisitedNodes:   context.totalVectorIdxSearchVisitedNodes,
		totalVectorIdxSearchDiscardedNodes: context.totalVectorIdxSearchDiscardedNodes,
		totalVectorIdxReadVecTimeMs:        context.totalVectorIdxReadVecTimeMs,
		totalVectorIdxReadOthersTimeMs:     context.totalVectorIdxReadOthersTimeMs,
	}
	for k, v := range context.regionsOfInstance {
		newContext.regionsOfInstance[k] = v
	}
	return newContext
}

func (context *TiFlashScanContext) String() string {
	var output []string
	if context.totalVectorIdxLoadFromS3+context.totalVectorIdxLoadFromDisk+context.totalVectorIdxLoadFromCache > 0 {
		var items []string
		items = append(items, fmt.Sprintf("load:{total:%dms,from_s3:%d,from_disk:%d,from_cache:%d}", context.totalVectorIdxLoadTimeMs, context.totalVectorIdxLoadFromS3, context.totalVectorIdxLoadFromDisk, context.totalVectorIdxLoadFromCache))
		items = append(items, fmt.Sprintf("search:{total:%dms,visited_nodes:%d,discarded_nodes:%d}", context.totalVectorIdxSearchTimeMs, context.totalVectorIdxSearchVisitedNodes, context.totalVectorIdxSearchDiscardedNodes))
		items = append(items, fmt.Sprintf("read:{vec_total:%dms,others_total:%dms}", context.totalVectorIdxReadVecTimeMs, context.totalVectorIdxReadOthersTimeMs))
		output = append(output, "vector_idx:{"+strings.Join(items, ",")+"}")
	}

	regionBalanceInfo := "none"
	if len(context.regionsOfInstance) > 0 {
		maxNum := uint64(0)
		minNum := uint64(math.MaxUint64)
		for _, v := range context.regionsOfInstance {
			if v > maxNum {
				maxNum = v
			}
			if v > 0 && v < minNum {
				minNum = v
			}
		}
		regionBalanceInfo = fmt.Sprintf("{instance_num: %d, max/min: %d/%d=%f}",
			len(context.regionsOfInstance),
			maxNum,
			minNum,
			float64(maxNum)/float64(minNum))
	}
	dmfileDisaggInfo := ""
	if context.disaggReadCacheHitBytes != 0 || context.disaggReadCacheMissBytes != 0 {
		dmfileDisaggInfo = fmt.Sprintf(", disagg_cache_hit_bytes: %d, disagg_cache_miss_bytes: %d",
			context.disaggReadCacheHitBytes,
			context.disaggReadCacheMissBytes)
	}
	remoteStreamInfo := ""
	if context.minRemoteStreamMs != 0 || context.maxRemoteStreamMs != 0 {
		remoteStreamInfo = fmt.Sprintf("min_remote_stream:%dms, max_remote_stream:%dms, ", context.minRemoteStreamMs, context.maxRemoteStreamMs)
	}

	// note: "tot" is short for "total"
	output = append(output, fmt.Sprintf("tiflash_scan:{"+
		"mvcc_input_rows:%d, "+
		"mvcc_input_bytes:%d, "+
		"mvcc_output_rows:%d, "+
		"local_regions:%d, "+
		"remote_regions:%d, "+
		"tot_learner_read:%dms, "+
		"region_balance:%s, "+
		"delta_rows:%d, "+
		"delta_bytes:%d, "+
		"segments:%d, "+
		"stale_read_regions:%d, "+
		"tot_build_snapshot:%dms, "+
		"tot_build_bitmap:%dms, "+
		"tot_build_inputstream:%dms, "+
		"min_local_stream:%dms, "+
		"max_local_stream:%dms, "+
		"%s"+ // remote stream info
		"dtfile:{"+
		"data_scanned_rows:%d, "+
		"data_skipped_rows:%d, "+
		"mvcc_scanned_rows:%d, "+
		"mvcc_skipped_rows:%d, "+
		"lm_filter_scanned_rows:%d, "+
		"lm_filter_skipped_rows:%d, "+
		"tot_rs_index_check:%dms, "+
		"tot_read:%dms"+
		"%s}"+ // Disagg cache info of DMFile
		"}",
		context.mvccInputRows,
		context.mvccInputBytes,
		context.mvccOutputRows,
		context.localRegions,
		context.remoteRegions,
		context.totalLearnerReadMs,
		regionBalanceInfo,
		context.deltaRows,
		context.deltaBytes,
		context.segments,
		context.staleReadRegions,
		context.totalBuildSnapshotMs,
		context.totalBuildBitmapMs,
		context.totalBuildInputStreamMs,
		context.minLocalStreamMs,
		context.maxLocalStreamMs,
		remoteStreamInfo,
		context.dmfileDataScannedRows,
		context.dmfileDataSkippedRows,
		context.dmfileMvccScannedRows,
		context.dmfileMvccSkippedRows,
		context.dmfileLmFilterScannedRows,
		context.dmfileLmFilterSkippedRows,
		context.totalDmfileRsCheckMs,
		context.totalDmfileReadMs,
		dmfileDisaggInfo,
	))

	return strings.Join(output, ", ")
}

// Merge make sum to merge the information in TiFlashScanContext
func (context *TiFlashScanContext) Merge(other TiFlashScanContext) {
	context.dmfileDataScannedRows += other.dmfileDataScannedRows
	context.dmfileDataSkippedRows += other.dmfileDataSkippedRows
	context.dmfileMvccScannedRows += other.dmfileMvccScannedRows
	context.dmfileMvccSkippedRows += other.dmfileMvccSkippedRows
	context.dmfileLmFilterScannedRows += other.dmfileLmFilterScannedRows
	context.dmfileLmFilterSkippedRows += other.dmfileLmFilterSkippedRows
	context.totalDmfileRsCheckMs += other.totalDmfileRsCheckMs
	context.totalDmfileReadMs += other.totalDmfileReadMs
	context.totalBuildSnapshotMs += other.totalBuildSnapshotMs
	context.localRegions += other.localRegions
	context.remoteRegions += other.remoteRegions
	context.totalLearnerReadMs += other.totalLearnerReadMs
	context.disaggReadCacheHitBytes += other.disaggReadCacheHitBytes
	context.disaggReadCacheMissBytes += other.disaggReadCacheMissBytes
	context.segments += other.segments
	context.readTasks += other.readTasks
	context.deltaRows += other.deltaRows
	context.deltaBytes += other.deltaBytes
	context.mvccInputRows += other.mvccInputRows
	context.mvccInputBytes += other.mvccInputBytes
	context.mvccOutputRows += other.mvccOutputRows
	context.totalBuildBitmapMs += other.totalBuildBitmapMs
	context.totalBuildInputStreamMs += other.totalBuildInputStreamMs
	context.staleReadRegions += other.staleReadRegions

	context.totalVectorIdxLoadFromS3 += other.totalVectorIdxLoadFromS3
	context.totalVectorIdxLoadFromDisk += other.totalVectorIdxLoadFromDisk
	context.totalVectorIdxLoadFromCache += other.totalVectorIdxLoadFromCache
	context.totalVectorIdxLoadTimeMs += other.totalVectorIdxLoadTimeMs
	context.totalVectorIdxSearchTimeMs += other.totalVectorIdxSearchTimeMs
	context.totalVectorIdxSearchVisitedNodes += other.totalVectorIdxSearchVisitedNodes
	context.totalVectorIdxSearchDiscardedNodes += other.totalVectorIdxSearchDiscardedNodes
	context.totalVectorIdxReadVecTimeMs += other.totalVectorIdxReadVecTimeMs
	context.totalVectorIdxReadOthersTimeMs += other.totalVectorIdxReadOthersTimeMs

	if context.minLocalStreamMs == 0 || other.minLocalStreamMs < context.minLocalStreamMs {
		context.minLocalStreamMs = other.minLocalStreamMs
	}
	if other.maxLocalStreamMs > context.maxLocalStreamMs {
		context.maxLocalStreamMs = other.maxLocalStreamMs
	}
	if context.minRemoteStreamMs == 0 || other.minRemoteStreamMs < context.minRemoteStreamMs {
		context.minRemoteStreamMs = other.minRemoteStreamMs
	}
	if other.maxRemoteStreamMs > context.maxRemoteStreamMs {
		context.maxRemoteStreamMs = other.maxRemoteStreamMs
	}

	if context.regionsOfInstance == nil {
		context.regionsOfInstance = make(map[string]uint64)
	}
	for k, v := range other.regionsOfInstance {
		context.regionsOfInstance[k] += v
	}
}

// Empty check whether TiFlashScanContext is Empty, if scan no pack and skip no pack, we regard it as empty
func (context *TiFlashScanContext) Empty() bool {
	res := context.dmfileDataScannedRows == 0 &&
		context.dmfileDataSkippedRows == 0 &&
		context.dmfileMvccScannedRows == 0 &&
		context.dmfileMvccSkippedRows == 0 &&
		context.dmfileLmFilterScannedRows == 0 &&
		context.dmfileLmFilterSkippedRows == 0 &&
		context.localRegions == 0 &&
		context.remoteRegions == 0 &&
		context.totalVectorIdxLoadFromDisk == 0 &&
		context.totalVectorIdxLoadFromCache == 0 &&
		context.totalVectorIdxLoadFromS3 == 0
	return res
}

// TiFlashWaitSummary is used to express all kinds of wait information in tiflash
type TiFlashWaitSummary struct {
	// keep execution time to do merge work, always record the wait time with largest execution time
	executionTime           uint64
	minTSOWaitTime          uint64
	pipelineBreakerWaitTime uint64
	pipelineQueueWaitTime   uint64
}

// Clone implements the deep copy of * TiFlashWaitSummary
func (waitSummary *TiFlashWaitSummary) Clone() TiFlashWaitSummary {
	newSummary := TiFlashWaitSummary{
		executionTime:           waitSummary.executionTime,
		minTSOWaitTime:          waitSummary.minTSOWaitTime,
		pipelineBreakerWaitTime: waitSummary.pipelineBreakerWaitTime,
		pipelineQueueWaitTime:   waitSummary.pipelineQueueWaitTime,
	}
	return newSummary
}

// String dumps TiFlashWaitSummary info as string
func (waitSummary *TiFlashWaitSummary) String() string {
	if waitSummary.CanBeIgnored() {
		return ""
	}
	buf := bytes.NewBuffer(make([]byte, 0, 32))
	buf.WriteString("tiflash_wait: {")
	empty := true
	if waitSummary.minTSOWaitTime >= uint64(time.Millisecond) {
		buf.WriteString("minTSO_wait: ")
		buf.WriteString(strconv.FormatInt(time.Duration(waitSummary.minTSOWaitTime).Milliseconds(), 10))
		buf.WriteString("ms")
		empty = false
	}
	if waitSummary.pipelineBreakerWaitTime >= uint64(time.Millisecond) {
		if !empty {
			buf.WriteString(", ")
		}
		buf.WriteString("pipeline_breaker_wait: ")
		buf.WriteString(strconv.FormatInt(time.Duration(waitSummary.pipelineBreakerWaitTime).Milliseconds(), 10))
		buf.WriteString("ms")
		empty = false
	}
	if waitSummary.pipelineQueueWaitTime >= uint64(time.Millisecond) {
		if !empty {
			buf.WriteString(", ")
		}
		buf.WriteString("pipeline_queue_wait: ")
		buf.WriteString(strconv.FormatInt(time.Duration(waitSummary.pipelineQueueWaitTime).Milliseconds(), 10))
		buf.WriteString("ms")
	}
	buf.WriteString("}")
	return buf.String()
}

// Merge make sum to merge the information in TiFlashWaitSummary
func (waitSummary *TiFlashWaitSummary) Merge(other TiFlashWaitSummary) {
	if waitSummary.executionTime < other.executionTime {
		waitSummary.executionTime = other.executionTime
		waitSummary.minTSOWaitTime = other.minTSOWaitTime
		waitSummary.pipelineBreakerWaitTime = other.pipelineBreakerWaitTime
		waitSummary.pipelineQueueWaitTime = other.pipelineQueueWaitTime
	}
}

// CanBeIgnored check whether TiFlashWaitSummary can be ignored, not all tidb executors have significant tiflash wait summary
func (waitSummary *TiFlashWaitSummary) CanBeIgnored() bool {
	res := waitSummary.minTSOWaitTime < uint64(time.Millisecond) &&
		waitSummary.pipelineBreakerWaitTime < uint64(time.Millisecond) &&
		waitSummary.pipelineQueueWaitTime < uint64(time.Millisecond)
	return res
}

// BasicRuntimeStats is the basic runtime stats.
type BasicRuntimeStats struct {
	// the count of executors with the same id
	executorCount atomic.Int32
	// executor's Next() called times.
	loop atomic.Int32
	// executor consume time, including open, next, and close time.
	consume atomic.Int64
	// executor open time.
	open atomic.Int64
	// executor close time.
	close atomic.Int64
	// executor return row count.
	rows atomic.Int64
}

// GetActRows return total rows of BasicRuntimeStats.
func (e *BasicRuntimeStats) GetActRows() int64 {
	return e.rows.Load()
}

// Clone implements the RuntimeStats interface.
func (e *BasicRuntimeStats) Clone() RuntimeStats {
	result := &BasicRuntimeStats{}
	result.executorCount.Store(e.executorCount.Load())
	result.loop.Store(e.loop.Load())
	result.consume.Store(e.consume.Load())
	result.open.Store(e.open.Load())
	result.close.Store(e.close.Load())
	result.rows.Store(e.rows.Load())
	return result
}

// Merge implements the RuntimeStats interface.
func (e *BasicRuntimeStats) Merge(rs RuntimeStats) {
	tmp, ok := rs.(*BasicRuntimeStats)
	if !ok {
		return
	}
	e.loop.Add(tmp.loop.Load())
	e.consume.Add(tmp.consume.Load())
	e.open.Add(tmp.open.Load())
	e.close.Add(tmp.close.Load())
	e.rows.Add(tmp.rows.Load())
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
	return e.basic.rows.Load()
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
			strs = append(strs, str)
		}
	}
	return strings.Join(strs, ", ")
}

// Record records executor's execution.
func (e *BasicRuntimeStats) Record(d time.Duration, rowNum int) {
	e.loop.Add(1)
	e.consume.Add(int64(d))
	e.rows.Add(int64(rowNum))
}

// RecordOpen records executor's open time.
func (e *BasicRuntimeStats) RecordOpen(d time.Duration) {
	e.consume.Add(int64(d))
	e.open.Add(int64(d))
}

// RecordClose records executor's close time.
func (e *BasicRuntimeStats) RecordClose(d time.Duration) {
	e.consume.Add(int64(d))
	e.close.Add(int64(d))
}

// SetRowNum sets the row num.
func (e *BasicRuntimeStats) SetRowNum(rowNum int64) {
	e.rows.Store(rowNum)
}

// String implements the RuntimeStats interface.
func (e *BasicRuntimeStats) String() string {
	if e == nil {
		return ""
	}
	var str strings.Builder
	timePrefix := ""
	if e.executorCount.Load() > 1 {
		timePrefix = "total_"
	}
	totalTime := e.consume.Load()
	openTime := e.open.Load()
	closeTime := e.close.Load()
	str.WriteString(timePrefix)
	str.WriteString("time:")
	str.WriteString(FormatDuration(time.Duration(totalTime)))
	str.WriteString(", ")
	str.WriteString(timePrefix)
	str.WriteString("open:")
	str.WriteString(FormatDuration(time.Duration(openTime)))
	str.WriteString(", ")
	str.WriteString(timePrefix)
	str.WriteString("close:")
	str.WriteString(FormatDuration(time.Duration(closeTime)))
	str.WriteString(", loops:")
	str.WriteString(strconv.FormatInt(int64(e.loop.Load()), 10))
	return str.String()
}

// GetTime get the int64 total time
func (e *BasicRuntimeStats) GetTime() int64 {
	return e.consume.Load()
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
		reuse.mu.Lock()
		defer reuse.mu.Unlock()
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
	defer e.mu.Unlock()
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
		stats.groupRss = append(stats.groupRss, info)
	}
}

// GetBasicRuntimeStats gets basicRuntimeStats for a executor
// When rootStat/rootStat's basicRuntimeStats is nil, the behavior is decided by initNewExecutorStats argument:
// 1. If true, it created a new one, and increase basicRuntimeStats' executorCount
// 2. Else, it returns nil
func (e *RuntimeStatsColl) GetBasicRuntimeStats(planID int, initNewExecutorStats bool) *BasicRuntimeStats {
	e.mu.Lock()
	defer e.mu.Unlock()
	stats, ok := e.rootStats[planID]
	if !ok && initNewExecutorStats {
		stats = NewRootRuntimeStats()
		e.rootStats[planID] = stats
	}
	if stats == nil {
		return nil
	}

	if stats.basic == nil && initNewExecutorStats {
		stats.basic = &BasicRuntimeStats{}
		stats.basic.executorCount.Add(1)
	} else if stats.basic != nil && initNewExecutorStats {
		stats.basic.executorCount.Add(1)
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

// GetPlanActRows returns the actual rows of the plan.
func (e *RuntimeStatsColl) GetPlanActRows(planID int) int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	runtimeStats, exists := e.rootStats[planID]
	if !exists {
		return 0
	}
	return runtimeStats.GetActRows()
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
func (e *RuntimeStatsColl) GetOrCreateCopStats(planID int, storeType kv.StoreType) *CopRuntimeStats {
	e.mu.Lock()
	defer e.mu.Unlock()
	copStats, ok := e.copStats[planID]
	if !ok {
		copStats = &CopRuntimeStats{
			stats:      make(map[string]*basicCopRuntimeStats),
			scanDetail: &util.ScanDetail{},
			timeDetail: &util.TimeDetail{},
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
func (e *RuntimeStatsColl) RecordOneCopTask(planID int, storeType kv.StoreType, address string, summary *tipb.ExecutorExecutionSummary) int {
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
func (e *RuntimeStatsColl) RecordScanDetail(planID int, storeType kv.StoreType, detail *util.ScanDetail) {
	copStats := e.GetOrCreateCopStats(planID, storeType)
	copStats.scanDetail.Merge(detail)
}

// RecordTimeDetail records a specific cop tasks's time detail.
func (e *RuntimeStatsColl) RecordTimeDetail(planID int, storeType kv.StoreType, detail *util.TimeDetail) {
	copStats := e.GetOrCreateCopStats(planID, storeType)
	if detail != nil {
		copStats.timeDetail.Merge(detail)
	}
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
	buf := bytes.NewBuffer(make([]byte, 0, 8))
	if len(e.concurrency) > 0 {
		for i, concurrency := range e.concurrency {
			if i > 0 {
				buf.WriteString(", ")
			}
			if concurrency.concurrencyNum > 0 {
				buf.WriteString(concurrency.concurrencyName)
				buf.WriteByte(':')
				buf.WriteString(strconv.Itoa(concurrency.concurrencyNum))
			} else {
				buf.WriteString(concurrency.concurrencyName)
				buf.WriteString(":OFF")
			}
		}
	}
	return buf.String()
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
				e.formatBackoff(buf, e.Commit.Mu.PrewriteBackoffTypes)
			}
			if len(e.Commit.Mu.CommitBackoffTypes) > 0 {
				buf.WriteString(", commit type: ")
				e.formatBackoff(buf, e.Commit.Mu.CommitBackoffTypes)
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
				e.formatBackoff(buf, e.LockKeys.Mu.BackoffTypes)
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

func (*RuntimeStatsWithCommit) formatBackoff(buf *bytes.Buffer, backoffTypes []string) {
	if len(backoffTypes) == 0 {
		return
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
	buf.WriteByte('[')
	for i, tp := range tpArray {
		if i > 0 {
			buf.WriteString(" ")
		}
		buf.WriteString(tp)
	}
	buf.WriteByte(']')
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

// MergeTiFlashRUConsumption merge execution summaries from selectResponse into ruDetails.
func MergeTiFlashRUConsumption(executionSummaries []*tipb.ExecutorExecutionSummary, ruDetails *util.RUDetails) error {
	newRUDetails := util.NewRUDetails()
	for _, summary := range executionSummaries {
		if summary != nil && summary.GetRuConsumption() != nil {
			tiflashRU := new(resource_manager.Consumption)
			if err := tiflashRU.Unmarshal(summary.GetRuConsumption()); err != nil {
				return err
			}
			newRUDetails.Update(tiflashRU, 0)
		}
	}
	ruDetails.Merge(newRUDetails)
	return nil
}

// RURuntimeStats is a wrapper of util.RUDetails,
// which implements the RuntimeStats interface.
type RURuntimeStats struct {
	*util.RUDetails
}

// String implements the RuntimeStats interface.
func (e *RURuntimeStats) String() string {
	if e.RUDetails != nil {
		buf := bytes.NewBuffer(make([]byte, 0, 8))
		buf.WriteString("RU:")
		buf.WriteString(strconv.FormatFloat(e.RRU()+e.WRU(), 'f', 2, 64))
		return buf.String()
	}
	return ""
}

// Clone implements the RuntimeStats interface.
func (e *RURuntimeStats) Clone() RuntimeStats {
	return &RURuntimeStats{RUDetails: e.RUDetails.Clone()}
}

// Merge implements the RuntimeStats interface.
func (e *RURuntimeStats) Merge(other RuntimeStats) {
	if tmp, ok := other.(*RURuntimeStats); ok {
		if e.RUDetails != nil {
			e.RUDetails.Merge(tmp.RUDetails)
		} else {
			e.RUDetails = tmp.RUDetails.Clone()
		}
	}
}

// Tp implements the RuntimeStats interface.
func (*RURuntimeStats) Tp() int {
	return TpRURuntimeStats
}
