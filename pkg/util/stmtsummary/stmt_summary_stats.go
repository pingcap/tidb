// Copyright 2019 PingCAP, Inc.
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
	"sync/atomic"
	"time"
)
func (ssStats *stmtSummaryStats) add(sei *StmtExecInfo, warningCount int, affectedRows uint64) {
	// add user to auth users set
	if len(sei.User) > 0 {
		ssStats.authUsers[sei.User] = struct{}{}
	}

	ssStats.execCount++
	if !sei.Succeed {
		ssStats.sumErrors++
	}
	ssStats.sumWarnings += warningCount

	// latency
	ssStats.sumLatency += sei.TotalLatency
	if sei.TotalLatency > ssStats.maxLatency {
		ssStats.maxLatency = sei.TotalLatency
	}
	if sei.TotalLatency < ssStats.minLatency {
		ssStats.minLatency = sei.TotalLatency
	}
	ssStats.sumParseLatency += sei.ParseLatency
	if sei.ParseLatency > ssStats.maxParseLatency {
		ssStats.maxParseLatency = sei.ParseLatency
	}
	ssStats.sumCompileLatency += sei.CompileLatency
	if sei.CompileLatency > ssStats.maxCompileLatency {
		ssStats.maxCompileLatency = sei.CompileLatency
	}

	// coprocessor
	if sei.CopTasks != nil {
		ssStats.sumNumCopTasks += int64(sei.CopTasks.NumCopTasks)
		ssStats.sumCopProcessTime += sei.CopTasks.TotProcessTime
		if sei.CopTasks.MaxProcessTime > ssStats.maxCopProcessTime {
			ssStats.maxCopProcessTime = sei.CopTasks.MaxProcessTime
			ssStats.maxCopProcessAddress = sei.CopTasks.MaxProcessAddress
		}
		ssStats.sumCopWaitTime += sei.CopTasks.TotWaitTime
		if sei.CopTasks.MaxWaitTime > ssStats.maxCopWaitTime {
			ssStats.maxCopWaitTime = sei.CopTasks.MaxWaitTime
			ssStats.maxCopWaitAddress = sei.CopTasks.MaxWaitAddress
		}
	}

	// TiKV
	ssStats.sumProcessTime += sei.ExecDetail.TimeDetail.ProcessTime
	if sei.ExecDetail.TimeDetail.ProcessTime > ssStats.maxProcessTime {
		ssStats.maxProcessTime = sei.ExecDetail.TimeDetail.ProcessTime
	}
	ssStats.sumWaitTime += sei.ExecDetail.TimeDetail.WaitTime
	if sei.ExecDetail.TimeDetail.WaitTime > ssStats.maxWaitTime {
		ssStats.maxWaitTime = sei.ExecDetail.TimeDetail.WaitTime
	}
	ssStats.sumBackoffTime += sei.ExecDetail.BackoffTime
	if sei.ExecDetail.BackoffTime > ssStats.maxBackoffTime {
		ssStats.maxBackoffTime = sei.ExecDetail.BackoffTime
	}

	if sei.ExecDetail.ScanDetail != nil {
		ssStats.sumTotalKeys += sei.ExecDetail.ScanDetail.TotalKeys
		if sei.ExecDetail.ScanDetail.TotalKeys > ssStats.maxTotalKeys {
			ssStats.maxTotalKeys = sei.ExecDetail.ScanDetail.TotalKeys
		}
		ssStats.sumProcessedKeys += sei.ExecDetail.ScanDetail.ProcessedKeys
		if sei.ExecDetail.ScanDetail.ProcessedKeys > ssStats.maxProcessedKeys {
			ssStats.maxProcessedKeys = sei.ExecDetail.ScanDetail.ProcessedKeys
		}
		ssStats.sumRocksdbDeleteSkippedCount += sei.ExecDetail.ScanDetail.RocksdbDeleteSkippedCount
		if sei.ExecDetail.ScanDetail.RocksdbDeleteSkippedCount > ssStats.maxRocksdbDeleteSkippedCount {
			ssStats.maxRocksdbDeleteSkippedCount = sei.ExecDetail.ScanDetail.RocksdbDeleteSkippedCount
		}
		ssStats.sumRocksdbKeySkippedCount += sei.ExecDetail.ScanDetail.RocksdbKeySkippedCount
		if sei.ExecDetail.ScanDetail.RocksdbKeySkippedCount > ssStats.maxRocksdbKeySkippedCount {
			ssStats.maxRocksdbKeySkippedCount = sei.ExecDetail.ScanDetail.RocksdbKeySkippedCount
		}
		ssStats.sumRocksdbBlockCacheHitCount += sei.ExecDetail.ScanDetail.RocksdbBlockCacheHitCount
		if sei.ExecDetail.ScanDetail.RocksdbBlockCacheHitCount > ssStats.maxRocksdbBlockCacheHitCount {
			ssStats.maxRocksdbBlockCacheHitCount = sei.ExecDetail.ScanDetail.RocksdbBlockCacheHitCount
		}
		ssStats.sumRocksdbBlockReadCount += sei.ExecDetail.ScanDetail.RocksdbBlockReadCount
		if sei.ExecDetail.ScanDetail.RocksdbBlockReadCount > ssStats.maxRocksdbBlockReadCount {
			ssStats.maxRocksdbBlockReadCount = sei.ExecDetail.ScanDetail.RocksdbBlockReadCount
		}
		ssStats.sumRocksdbBlockReadByte += sei.ExecDetail.ScanDetail.RocksdbBlockReadByte
		if sei.ExecDetail.ScanDetail.RocksdbBlockReadByte > ssStats.maxRocksdbBlockReadByte {
			ssStats.maxRocksdbBlockReadByte = sei.ExecDetail.ScanDetail.RocksdbBlockReadByte
		}
	}

	// txn
	commitDetails := sei.ExecDetail.CommitDetail
	if commitDetails != nil {
		ssStats.commitCount++
		ssStats.sumPrewriteTime += commitDetails.PrewriteTime
		if commitDetails.PrewriteTime > ssStats.maxPrewriteTime {
			ssStats.maxPrewriteTime = commitDetails.PrewriteTime
		}
		ssStats.sumCommitTime += commitDetails.CommitTime
		if commitDetails.CommitTime > ssStats.maxCommitTime {
			ssStats.maxCommitTime = commitDetails.CommitTime
		}
		ssStats.sumGetCommitTsTime += commitDetails.GetCommitTsTime
		if commitDetails.GetCommitTsTime > ssStats.maxGetCommitTsTime {
			ssStats.maxGetCommitTsTime = commitDetails.GetCommitTsTime
		}
		resolveLockTime := atomic.LoadInt64(&commitDetails.ResolveLock.ResolveLockTime)
		ssStats.sumResolveLockTime += resolveLockTime
		if resolveLockTime > ssStats.maxResolveLockTime {
			ssStats.maxResolveLockTime = resolveLockTime
		}
		ssStats.sumLocalLatchTime += commitDetails.LocalLatchTime
		if commitDetails.LocalLatchTime > ssStats.maxLocalLatchTime {
			ssStats.maxLocalLatchTime = commitDetails.LocalLatchTime
		}
		ssStats.sumWriteKeys += int64(commitDetails.WriteKeys)
		if commitDetails.WriteKeys > ssStats.maxWriteKeys {
			ssStats.maxWriteKeys = commitDetails.WriteKeys
		}
		ssStats.sumWriteSize += int64(commitDetails.WriteSize)
		if commitDetails.WriteSize > ssStats.maxWriteSize {
			ssStats.maxWriteSize = commitDetails.WriteSize
		}
		prewriteRegionNum := atomic.LoadInt32(&commitDetails.PrewriteRegionNum)
		ssStats.sumPrewriteRegionNum += int64(prewriteRegionNum)
		if prewriteRegionNum > ssStats.maxPrewriteRegionNum {
			ssStats.maxPrewriteRegionNum = prewriteRegionNum
		}
		ssStats.sumTxnRetry += int64(commitDetails.TxnRetry)
		if commitDetails.TxnRetry > ssStats.maxTxnRetry {
			ssStats.maxTxnRetry = commitDetails.TxnRetry
		}
		commitDetails.Mu.Lock()
		commitBackoffTime := commitDetails.Mu.CommitBackoffTime
		ssStats.sumCommitBackoffTime += commitBackoffTime
		if commitBackoffTime > ssStats.maxCommitBackoffTime {
			ssStats.maxCommitBackoffTime = commitBackoffTime
		}
		ssStats.sumBackoffTimes += int64(len(commitDetails.Mu.PrewriteBackoffTypes))
		for _, backoffType := range commitDetails.Mu.PrewriteBackoffTypes {
			ssStats.backoffTypes[backoffType]++
		}
		ssStats.sumBackoffTimes += int64(len(commitDetails.Mu.CommitBackoffTypes))
		for _, backoffType := range commitDetails.Mu.CommitBackoffTypes {
			ssStats.backoffTypes[backoffType]++
		}
		commitDetails.Mu.Unlock()
	}

	// plan cache
	if sei.PlanInCache {
		ssStats.planInCache = true
		ssStats.planCacheHits++
	} else {
		ssStats.planInCache = false
	}
	if sei.PlanCacheUnqualified != "" {
		ssStats.planCacheUnqualifiedCount++
		ssStats.lastPlanCacheUnqualified = sei.PlanCacheUnqualified
	}

	// SPM
	if sei.PlanInBinding {
		ssStats.planInBinding = true
	} else {
		ssStats.planInBinding = false
	}

	// other
	ssStats.sumAffectedRows += affectedRows
	ssStats.sumMem += sei.MemMax
	if sei.MemMax > ssStats.maxMem {
		ssStats.maxMem = sei.MemMax
	}

	ssStats.sumMemArbitration += sei.MemArbitration
	if sei.MemArbitration > ssStats.maxMemArbitration {
		ssStats.maxMemArbitration = sei.MemArbitration
	}

	ssStats.sumDisk += sei.DiskMax
	if sei.DiskMax > ssStats.maxDisk {
		ssStats.maxDisk = sei.DiskMax
	}
	if sei.StartTime.Before(ssStats.firstSeen) {
		ssStats.firstSeen = sei.StartTime
	}
	if ssStats.lastSeen.Before(sei.StartTime) {
		ssStats.lastSeen = sei.StartTime
	}
	if sei.ExecRetryCount > 0 {
		ssStats.execRetryCount += sei.ExecRetryCount
		ssStats.execRetryTime += sei.ExecRetryTime
	}
	if sei.ResultRows > 0 {
		ssStats.sumResultRows += sei.ResultRows
		if ssStats.maxResultRows < sei.ResultRows {
			ssStats.maxResultRows = sei.ResultRows
		}
		if ssStats.minResultRows > sei.ResultRows {
			ssStats.minResultRows = sei.ResultRows
		}
	} else {
		ssStats.minResultRows = 0
	}
	ssStats.sumKVTotal += time.Duration(atomic.LoadInt64(&sei.TiKVExecDetails.WaitKVRespDuration))
	ssStats.sumPDTotal += time.Duration(atomic.LoadInt64(&sei.TiKVExecDetails.WaitPDRespDuration))
	ssStats.sumBackoffTotal += time.Duration(atomic.LoadInt64(&sei.TiKVExecDetails.BackoffDuration))
	ssStats.sumWriteSQLRespTotal += sei.StmtExecDetails.WriteSQLRespDuration
	ssStats.sumTidbCPU += sei.CPUUsages.TidbCPUTime
	ssStats.sumTikvCPU += sei.CPUUsages.TikvCPUTime

	// network traffic
	ssStats.StmtNetworkTrafficSummary.Add(sei.TiKVExecDetails)

	// request-units
	ssStats.StmtRUSummary.Add(sei.RUDetail)

	ssStats.storageKV = sei.StmtCtx.IsTiKV.Load()
	ssStats.storageMPP = sei.StmtCtx.IsTiFlash.Load()
}
