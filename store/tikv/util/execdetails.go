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
	"fmt"
	"math"
	"sync"
	"time"
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
