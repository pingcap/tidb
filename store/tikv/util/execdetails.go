// Copyright 2021 PingCAP, Inc.
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

package util

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
)

type commitDetailCtxKeyType struct{}
type lockKeysDetailCtxKeyType struct{}
type execDetailsCtxKeyType struct{}

var (
	// CommitDetailCtxKey presents CommitDetail info key in context.
	CommitDetailCtxKey = commitDetailCtxKeyType{}

	// LockKeysDetailCtxKey presents LockKeysDetail info key in context.
	LockKeysDetailCtxKey = lockKeysDetailCtxKeyType{}

	// ExecDetailsKey presents ExecDetail info key in context.
	ExecDetailsKey = execDetailsCtxKeyType{}
)

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

// Merge merges commit details into itself.
func (cd *CommitDetails) Merge(other *CommitDetails) {
	cd.GetCommitTsTime += other.GetCommitTsTime
	cd.PrewriteTime += other.PrewriteTime
	cd.WaitPrewriteBinlogTime += other.WaitPrewriteBinlogTime
	cd.CommitTime += other.CommitTime
	cd.LocalLatchTime += other.LocalLatchTime
	cd.CommitBackoffTime += other.CommitBackoffTime
	cd.ResolveLockTime += other.ResolveLockTime
	cd.WriteKeys += other.WriteKeys
	cd.WriteSize += other.WriteSize
	cd.PrewriteRegionNum += other.PrewriteRegionNum
	cd.TxnRetry += other.TxnRetry
	cd.Mu.BackoffTypes = append(cd.Mu.BackoffTypes, other.Mu.BackoffTypes...)
}

// Clone returns a deep copy of itself.
func (cd *CommitDetails) Clone() *CommitDetails {
	commit := &CommitDetails{
		GetCommitTsTime:        cd.GetCommitTsTime,
		PrewriteTime:           cd.PrewriteTime,
		WaitPrewriteBinlogTime: cd.WaitPrewriteBinlogTime,
		CommitTime:             cd.CommitTime,
		LocalLatchTime:         cd.LocalLatchTime,
		CommitBackoffTime:      cd.CommitBackoffTime,
		ResolveLockTime:        cd.ResolveLockTime,
		WriteKeys:              cd.WriteKeys,
		WriteSize:              cd.WriteSize,
		PrewriteRegionNum:      cd.PrewriteRegionNum,
		TxnRetry:               cd.TxnRetry,
	}
	commit.Mu.BackoffTypes = append([]fmt.Stringer{}, cd.Mu.BackoffTypes...)
	return commit
}

// LockKeysDetails contains pessimistic lock keys detail information.
type LockKeysDetails struct {
	TotalTime       time.Duration
	RegionNum       int32
	LockKeys        int32
	ResolveLockTime int64
	BackoffTime     int64
	Mu              struct {
		sync.Mutex
		BackoffTypes []fmt.Stringer
	}
	LockRPCTime  int64
	LockRPCCount int64
	RetryCount   int
}

// Merge merges lock keys execution details into self.
func (ld *LockKeysDetails) Merge(lockKey *LockKeysDetails) {
	ld.TotalTime += lockKey.TotalTime
	ld.RegionNum += lockKey.RegionNum
	ld.LockKeys += lockKey.LockKeys
	ld.ResolveLockTime += lockKey.ResolveLockTime
	ld.BackoffTime += lockKey.BackoffTime
	ld.LockRPCTime += lockKey.LockRPCTime
	ld.LockRPCCount += ld.LockRPCCount
	ld.Mu.BackoffTypes = append(ld.Mu.BackoffTypes, lockKey.Mu.BackoffTypes...)
	ld.RetryCount++
}

// Clone returns a deep copy of itself.
func (ld *LockKeysDetails) Clone() *LockKeysDetails {
	lock := &LockKeysDetails{
		TotalTime:       ld.TotalTime,
		RegionNum:       ld.RegionNum,
		LockKeys:        ld.LockKeys,
		ResolveLockTime: ld.ResolveLockTime,
		BackoffTime:     ld.BackoffTime,
		LockRPCTime:     ld.LockRPCTime,
		LockRPCCount:    ld.LockRPCCount,
		RetryCount:      ld.RetryCount,
	}
	lock.Mu.BackoffTypes = append([]fmt.Stringer{}, ld.Mu.BackoffTypes...)
	return lock
}

// ExecDetails contains execution detail info.
type ExecDetails struct {
	BackoffCount       int64
	BackoffDuration    int64
	WaitKVRespDuration int64
	WaitPDRespDuration int64
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

// ScanDetail contains coprocessor scan detail information.
type ScanDetail struct {
	// TotalKeys is the approximate number of MVCC keys meet during scanning. It includes
	// deleted versions, but does not include RocksDB tombstone keys.
	TotalKeys int64
	// ProcessedKeys is the number of user keys scanned from the storage.
	// It does not include deleted version or RocksDB tombstone keys.
	// For Coprocessor requests, it includes keys that has been filtered out by Selection.
	ProcessedKeys int64
	// RocksdbDeleteSkippedCount is the total number of deletes and single deletes skipped over during
	// iteration, i.e. how many RocksDB tombstones are skipped.
	RocksdbDeleteSkippedCount uint64
	// RocksdbKeySkippedCount it the total number of internal keys skipped over during iteration.
	RocksdbKeySkippedCount uint64
	// RocksdbBlockCacheHitCount is the total number of RocksDB block cache hits.
	RocksdbBlockCacheHitCount uint64
	// RocksdbBlockReadCount is the total number of block reads (with IO).
	RocksdbBlockReadCount uint64
	// RocksdbBlockReadByte is the total number of bytes from block reads.
	RocksdbBlockReadByte uint64
}

// Merge merges scan detail execution details into self.
func (sd *ScanDetail) Merge(scanDetail *ScanDetail) {
	atomic.AddInt64(&sd.TotalKeys, scanDetail.TotalKeys)
	atomic.AddInt64(&sd.ProcessedKeys, scanDetail.ProcessedKeys)
	atomic.AddUint64(&sd.RocksdbDeleteSkippedCount, scanDetail.RocksdbDeleteSkippedCount)
	atomic.AddUint64(&sd.RocksdbKeySkippedCount, scanDetail.RocksdbKeySkippedCount)
	atomic.AddUint64(&sd.RocksdbBlockCacheHitCount, scanDetail.RocksdbBlockCacheHitCount)
	atomic.AddUint64(&sd.RocksdbBlockReadCount, scanDetail.RocksdbBlockReadCount)
	atomic.AddUint64(&sd.RocksdbBlockReadByte, scanDetail.RocksdbBlockReadByte)
}

var zeroScanDetail = ScanDetail{}

// String implements the fmt.Stringer interface.
func (sd *ScanDetail) String() string {
	if sd == nil || *sd == zeroScanDetail {
		return ""
	}
	buf := bytes.NewBuffer(make([]byte, 0, 16))
	buf.WriteString("scan_detail: {")
	buf.WriteString("total_process_keys: ")
	buf.WriteString(strconv.FormatInt(sd.ProcessedKeys, 10))
	buf.WriteString(", total_keys: ")
	buf.WriteString(strconv.FormatInt(sd.TotalKeys, 10))
	buf.WriteString(", rocksdb: {")
	buf.WriteString("delete_skipped_count: ")
	buf.WriteString(strconv.FormatUint(sd.RocksdbDeleteSkippedCount, 10))
	buf.WriteString(", key_skipped_count: ")
	buf.WriteString(strconv.FormatUint(sd.RocksdbKeySkippedCount, 10))
	buf.WriteString(", block: {")
	buf.WriteString("cache_hit_count: ")
	buf.WriteString(strconv.FormatUint(sd.RocksdbBlockCacheHitCount, 10))
	buf.WriteString(", read_count: ")
	buf.WriteString(strconv.FormatUint(sd.RocksdbBlockReadCount, 10))
	buf.WriteString(", read_byte: ")
	buf.WriteString(FormatBytes(int64(sd.RocksdbBlockReadByte)))
	buf.WriteString("}}}")
	return buf.String()
}

// MergeFromScanDetailV2 merges scan detail from pb into itself.
func (sd *ScanDetail) MergeFromScanDetailV2(scanDetail *kvrpcpb.ScanDetailV2) {
	if scanDetail != nil {
		sd.TotalKeys += int64(scanDetail.TotalVersions)
		sd.ProcessedKeys += int64(scanDetail.ProcessedVersions)
		sd.RocksdbDeleteSkippedCount += scanDetail.RocksdbDeleteSkippedCount
		sd.RocksdbKeySkippedCount += scanDetail.RocksdbKeySkippedCount
		sd.RocksdbBlockCacheHitCount += scanDetail.RocksdbBlockCacheHitCount
		sd.RocksdbBlockReadCount += scanDetail.RocksdbBlockReadCount
		sd.RocksdbBlockReadByte += scanDetail.RocksdbBlockReadByte
	}
}

// TimeDetail contains coprocessor time detail information.
type TimeDetail struct {
	// WaitWallTimeMs is the off-cpu wall time which is elapsed in TiKV side. Usually this includes queue waiting time and
	// other kind of waitings in series.
	ProcessTime time.Duration
	// Off-cpu and on-cpu wall time elapsed to actually process the request payload. It does not
	// include `wait_wall_time`.
	// This field is very close to the CPU time in most cases. Some wait time spend in RocksDB
	// cannot be excluded for now, like Mutex wait time, which is included in this field, so that
	// this field is called wall time instead of CPU time.
	WaitTime time.Duration
}

// String implements the fmt.Stringer interface.
func (td *TimeDetail) String() string {
	if td == nil {
		return ""
	}
	buf := bytes.NewBuffer(make([]byte, 0, 16))
	if td.ProcessTime > 0 {
		buf.WriteString("total_process_time: ")
		buf.WriteString(FormatDuration(td.ProcessTime))
	}
	if td.WaitTime > 0 {
		if buf.Len() > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString("total_wait_time: ")
		buf.WriteString(FormatDuration(td.WaitTime))
	}
	return buf.String()
}

// MergeFromTimeDetail merges time detail from pb into itself.
func (td *TimeDetail) MergeFromTimeDetail(timeDetail *kvrpcpb.TimeDetail) {
	if timeDetail != nil {
		td.WaitTime += time.Duration(timeDetail.WaitWallTimeMs) * time.Millisecond
		td.ProcessTime += time.Duration(timeDetail.ProcessWallTimeMs) * time.Millisecond
	}
}
