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
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

// ExecDetails contains execution detail information.
type ExecDetails struct {
	CopExecDetails
	CommitDetail         *util.CommitDetails
	LockKeysDetail       *util.LockKeysDetails
	SharedLockKeysDetail *util.LockKeysDetails
	CopTime              time.Duration
	LockKeysDuration     time.Duration
	RequestCount         int
}

// CopExecDetails contains cop execution detail information.
type CopExecDetails struct {
	ScanDetail    *util.ScanDetail
	TimeDetail    util.TimeDetail
	CalleeAddress string
	BackoffTime   time.Duration
	BackoffSleep  map[string]time.Duration
	BackoffTimes  map[string]int
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
func (d *P90Summary) Merge(backoffSleep map[string]time.Duration, backoffTimes map[string]int, calleeAddress string, timeDetail util.TimeDetail) {
	if d.BackoffInfo == nil {
		d.Reset()
	}
	d.NumCopTasks++
	d.ProcessTimePercentile.Add(DurationWithAddr{timeDetail.ProcessTime, calleeAddress})
	d.WaitTimePercentile.Add(DurationWithAddr{timeDetail.WaitTime, calleeAddress})

	var info *P90BackoffSummary
	var ok bool
	for backoff, timeItem := range backoffTimes {
		if info, ok = d.BackoffInfo[backoff]; !ok {
			d.BackoffInfo[backoff] = &P90BackoffSummary{}
			info = d.BackoffInfo[backoff]
		}
		sleepItem := backoffSleep[backoff]
		info.ReqTimes++
		info.TotBackoffTime += sleepItem
		info.TotBackoffTimes += timeItem

		info.BackoffPercentile.Add(DurationWithAddr{sleepItem, calleeAddress})
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
	// WaitTimeStr means the time of all coprocessor wait.
	WaitTimeStr = "Wait_time"
	// LockKeysTimeStr means the time interval between pessimistic lock wait start and lock got obtain
	LockKeysTimeStr = "LockKeys_time"
	// RequestCountStr means the request count.
	RequestCountStr = "Request_count"
	// WaitPrewriteBinlogTimeStr means the time of waiting prewrite binlog finished when transaction committing.
	WaitPrewriteBinlogTimeStr = "Wait_prewrite_binlog_time"
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

	// The following constants define the set of fields for SlowQueryLogItems
	// that are relevant to evaluating and triggering SlowLogRules.

	// ProcessTimeStr represents the sum of process time of all the coprocessor tasks.
	ProcessTimeStr = "Process_time"
	// BackoffTimeStr means the time of all back-off.
	BackoffTimeStr = "Backoff_time"
	// TotalKeysStr means the total scan keys.
	TotalKeysStr = "Total_keys"
	// ProcessKeysStr means the total processed keys.
	ProcessKeysStr = "Process_keys"
	// PreWriteTimeStr means the time of pre-write.
	PreWriteTimeStr = "Prewrite_time"
	// CommitTimeStr means the time of commit.
	CommitTimeStr = "Commit_time"
	// WriteKeysStr means the count of keys in the transaction.
	WriteKeysStr = "Write_keys"
	// WriteSizeStr means the key/value size in the transaction.
	WriteSizeStr = "Write_size"
	// PrewriteRegionStr means the count of region when pre-write.
	PrewriteRegionStr = "Prewrite_region"
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
	lockKeyDetails := d.LockKeysDetail
	if lockKeyDetails != nil {
		if lockKeyDetails.TotalTime > 0 {
			parts = append(parts, LockKeysTimeStr+": "+strconv.FormatFloat(lockKeyDetails.TotalTime.Seconds(), 'f', -1, 64))
		}
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
func (s *SyncExecDetails) MergeExecDetails(commitDetails *util.CommitDetails) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if commitDetails != nil {
		if s.execDetails.CommitDetail == nil {
			s.execDetails.CommitDetail = commitDetails
		} else {
			s.execDetails.CommitDetail.Merge(commitDetails)
		}
	}
}

// MergeCopExecDetails merges a CopExecDetails into self.
func (s *SyncExecDetails) MergeCopExecDetails(details *CopExecDetails, copTime time.Duration) {
	if details == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.execDetails.CopTime += copTime
	s.execDetails.BackoffTime += details.BackoffTime
	s.execDetails.RequestCount++
	s.mergeScanDetail(details.ScanDetail)
	s.mergeTimeDetail(details.TimeDetail)
	s.detailsSummary.Merge(details.BackoffSleep, details.BackoffTimes, details.CalleeAddress, details.TimeDetail)
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

// MergeSharedLockKeysExecDetails merges shared lock keys execution details into self.
func (s *SyncExecDetails) MergeSharedLockKeysExecDetails(lockKeys *util.LockKeysDetails) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.execDetails.SharedLockKeysDetail == nil {
		s.execDetails.SharedLockKeysDetail = lockKeys
	} else {
		s.execDetails.SharedLockKeysDetail.Merge(lockKeys)
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
	d.ProcessTimeStats = TaskTimeStats{
		TotTime:    s.execDetails.TimeDetail.ProcessTime,
		AvgTime:    s.execDetails.TimeDetail.ProcessTime / time.Duration(n),
		P90Time:    time.Duration((s.detailsSummary.ProcessTimePercentile.GetPercentile(0.9))),
		MaxTime:    s.detailsSummary.ProcessTimePercentile.GetMax().D,
		MaxAddress: s.detailsSummary.ProcessTimePercentile.GetMax().Addr,
	}

	d.WaitTimeStats = TaskTimeStats{
		TotTime:    s.execDetails.TimeDetail.WaitTime,
		AvgTime:    s.execDetails.TimeDetail.WaitTime / time.Duration(n),
		P90Time:    time.Duration((s.detailsSummary.WaitTimePercentile.GetPercentile(0.9))),
		MaxTime:    s.detailsSummary.WaitTimePercentile.GetMax().D,
		MaxAddress: s.detailsSummary.WaitTimePercentile.GetMax().Addr,
	}

	if len(s.detailsSummary.BackoffInfo) > 0 {
		d.BackoffTimeStatsMap = make(map[string]TaskTimeStats)
		d.TotBackoffTimes = make(map[string]int)
	}
	for backoff, items := range s.detailsSummary.BackoffInfo {
		if items == nil {
			continue
		}
		n := items.ReqTimes
		d.BackoffTimeStatsMap[backoff] = TaskTimeStats{
			MaxAddress: items.BackoffPercentile.GetMax().Addr,
			MaxTime:    items.BackoffPercentile.GetMax().D,
			P90Time:    time.Duration(items.BackoffPercentile.GetPercentile(0.9)),
			AvgTime:    items.TotBackoffTime / time.Duration(n),
			TotTime:    items.TotBackoffTime,
		}

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

	ProcessTimeStats TaskTimeStats
	WaitTimeStats    TaskTimeStats

	BackoffTimeStatsMap map[string]TaskTimeStats
	TotBackoffTimes     map[string]int
}

// TaskTimeStats is used for recording time-related statistical metrics, including dimensions such as average values, percentile values, maximum values, etc.
// It is suitable for scenarios involving latency statistics, wait time analysis, and similar use cases.
type TaskTimeStats struct {
	AvgTime    time.Duration
	P90Time    time.Duration
	MaxAddress string
	MaxTime    time.Duration
	TotTime    time.Duration
}

// String returns the TaskTimeStats fields as a string.
func (s TaskTimeStats) String(numCopTasks int, spaceMarkStr, avgStr, p90Str, maxStr, addrStr string) string {
	if numCopTasks == 1 {
		return fmt.Sprintf("%v%v%v %v%v%v",
			avgStr, spaceMarkStr, s.AvgTime.Seconds(),
			addrStr, spaceMarkStr, s.MaxAddress)
	}

	return fmt.Sprintf("%v%v%v %v%v%v %v%v%v %v%v%v",
		avgStr, spaceMarkStr, s.AvgTime.Seconds(),
		p90Str, spaceMarkStr, s.P90Time.Seconds(),
		maxStr, spaceMarkStr, s.MaxTime.Seconds(),
		addrStr, spaceMarkStr, s.MaxAddress)
}

// FormatFloatFields returns the AvgTime, P90Time and MaxTime in float format.
func (s TaskTimeStats) FormatFloatFields() (avgStr, p90Str, maxStr string) {
	return strconv.FormatFloat(s.AvgTime.Seconds(), 'f', -1, 64),
		strconv.FormatFloat(s.P90Time.Seconds(), 'f', -1, 64),
		strconv.FormatFloat(s.MaxTime.Seconds(), 'f', -1, 64)
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
	avgStr, p90Str, maxStr := d.ProcessTimeStats.FormatFloatFields()
	fields = append(fields, zap.String("process_avg_time", avgStr+"s"))
	fields = append(fields, zap.String("process_p90_time", p90Str+"s"))
	fields = append(fields, zap.String("process_max_time", maxStr+"s"))
	fields = append(fields, zap.String("process_max_addr", d.ProcessTimeStats.MaxAddress))
	avgStr, p90Str, maxStr = d.WaitTimeStats.FormatFloatFields()
	fields = append(fields, zap.String("wait_avg_time", avgStr+"s"))
	fields = append(fields, zap.String("wait_p90_time", p90Str+"s"))
	fields = append(fields, zap.String("wait_max_time", maxStr+"s"))
	fields = append(fields, zap.String("wait_max_addr", d.WaitTimeStats.MaxAddress))
	return fields
}
