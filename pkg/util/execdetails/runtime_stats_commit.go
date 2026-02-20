// Copyright 2025 PingCAP, Inc.
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
	"slices"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/tikv/client-go/v2/util"
)

// RuntimeStatsWithCommit is the RuntimeStats with commit detail.
type RuntimeStatsWithCommit struct {
	Commit         *util.CommitDetails
	LockKeys       *util.LockKeysDetails
	SharedLockKeys *util.LockKeysDetails
	TxnCnt         int
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

	if tmp.SharedLockKeys != nil {
		if e.SharedLockKeys == nil {
			e.SharedLockKeys = &util.LockKeysDetails{}
		}
		e.SharedLockKeys.Merge(tmp.SharedLockKeys)
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
	if e.SharedLockKeys != nil {
		newRs.SharedLockKeys = e.SharedLockKeys.Clone()
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
	e.formatLockKeysDetails(buf, "lock_keys", e.LockKeys)
	e.formatLockKeysDetails(buf, "shared_lock_keys", e.SharedLockKeys)
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

func (e *RuntimeStatsWithCommit) formatLockKeysDetails(buf *bytes.Buffer, label string, lockKeys *util.LockKeysDetails) {
	if lockKeys == nil {
		return
	}
	if buf.Len() > 0 {
		buf.WriteString(", ")
	}
	buf.WriteString(label)
	buf.WriteString(": {")
	if lockKeys.TotalTime > 0 {
		buf.WriteString("time:")
		buf.WriteString(FormatDuration(lockKeys.TotalTime))
	}
	if lockKeys.RegionNum > 0 {
		buf.WriteString(", region:")
		buf.WriteString(strconv.FormatInt(int64(lockKeys.RegionNum), 10))
	}
	if lockKeys.LockKeys > 0 {
		buf.WriteString(", keys:")
		buf.WriteString(strconv.FormatInt(int64(lockKeys.LockKeys), 10))
	}
	if lockKeys.ResolveLock.ResolveLockTime > 0 {
		buf.WriteString(", resolve_lock:")
		buf.WriteString(FormatDuration(time.Duration(lockKeys.ResolveLock.ResolveLockTime)))
	}
	lockKeys.Mu.Lock()
	if lockKeys.BackoffTime > 0 {
		buf.WriteString(", backoff: {time: ")
		buf.WriteString(FormatDuration(time.Duration(lockKeys.BackoffTime)))
		if len(lockKeys.Mu.BackoffTypes) > 0 {
			buf.WriteString(", type: ")
			e.formatBackoff(buf, lockKeys.Mu.BackoffTypes)
		}
		buf.WriteString("}")
	}
	if lockKeys.Mu.SlowestReqTotalTime > 0 {
		buf.WriteString(", slowest_rpc: {total: ")
		buf.WriteString(strconv.FormatFloat(lockKeys.Mu.SlowestReqTotalTime.Seconds(), 'f', 3, 64))
		buf.WriteString("s, region_id: ")
		buf.WriteString(strconv.FormatUint(lockKeys.Mu.SlowestRegion, 10))
		buf.WriteString(", store: ")
		buf.WriteString(lockKeys.Mu.SlowestStoreAddr)
		buf.WriteString(", ")
		buf.WriteString(lockKeys.Mu.SlowestExecDetails.String())
		buf.WriteString("}")
	}
	lockKeys.Mu.Unlock()
	if lockKeys.LockRPCTime > 0 {
		buf.WriteString(", lock_rpc:")
		buf.WriteString(time.Duration(lockKeys.LockRPCTime).String())
	}
	if lockKeys.LockRPCCount > 0 {
		buf.WriteString(", rpc_count:")
		buf.WriteString(strconv.FormatInt(lockKeys.LockRPCCount, 10))
	}
	if lockKeys.RetryCount > 0 {
		buf.WriteString(", retry_count:")
		buf.WriteString(strconv.FormatInt(int64(lockKeys.RetryCount), 10))
	}
	buf.WriteString("}")
}
