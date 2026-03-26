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
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
	rmclient "github.com/tikv/pd/client/resource_group/controller"
)

func defaultRUV2WeightsForTest() RUV2Weights {
	cfg := config.DefaultRUV2Config()
	return RUV2Weights{
		RUScale:                 cfg.RUScale,
		ResultChunkCells:        cfg.ResultChunkCells,
		ExecutorL1:              cfg.ExecutorL1,
		ExecutorL2:              cfg.ExecutorL2,
		ExecutorL3:              cfg.ExecutorL3,
		ExecutorL5InsertRows:    cfg.ExecutorL5InsertRows,
		PlanCnt:                 cfg.PlanCnt,
		PlanDeriveStatsPaths:    cfg.PlanDeriveStatsPaths,
		ResourceManagerReadCnt:  cfg.ResourceManagerReadCnt,
		ResourceManagerWriteCnt: cfg.ResourceManagerWriteCnt,
		SessionParserTotal:      cfg.SessionParserTotal,
		TxnCnt:                  cfg.TxnCnt,
	}
}

func TestString(t *testing.T) {
	detail := &ExecDetails{
		CopTime:      time.Second + 3*time.Millisecond,
		RequestCount: 1,
		LockKeysDetail: &util.LockKeysDetails{
			TotalTime:   time.Second,
			RegionNum:   2,
			LockKeys:    10,
			BackoffTime: int64(time.Second * 3),
			Mu: struct {
				sync.Mutex
				BackoffTypes        []string
				SlowestReqTotalTime time.Duration
				SlowestRegion       uint64
				SlowestStoreAddr    string
				SlowestExecDetails  util.TiKVExecDetails
			}{
				BackoffTypes: []string{
					"backoff4",
					"backoff5",
					"backoff5",
				},
				SlowestReqTotalTime: time.Second,
				SlowestRegion:       1000,
				SlowestStoreAddr:    "tikv-1:20160",
				SlowestExecDetails: util.TiKVExecDetails{
					TimeDetail: &util.TimeDetail{
						TotalRPCWallTime: 500 * time.Millisecond,
					},
					ScanDetail: &util.ScanDetail{
						ProcessedKeys:             10,
						TotalKeys:                 100,
						RocksdbDeleteSkippedCount: 1,
						RocksdbKeySkippedCount:    1,
						RocksdbBlockCacheHitCount: 1,
						RocksdbBlockReadCount:     1,
						RocksdbBlockReadByte:      100,
						RocksdbBlockReadDuration:  20 * time.Millisecond,
					},
					WriteDetail: &util.WriteDetail{
						StoreBatchWaitDuration:        10 * time.Microsecond,
						ProposeSendWaitDuration:       20 * time.Microsecond,
						PersistLogDuration:            30 * time.Microsecond,
						RaftDbWriteLeaderWaitDuration: 40 * time.Microsecond,
						RaftDbSyncLogDuration:         45 * time.Microsecond,
						RaftDbWriteMemtableDuration:   50 * time.Microsecond,
						CommitLogDuration:             60 * time.Microsecond,
						ApplyBatchWaitDuration:        70 * time.Microsecond,
						ApplyLogDuration:              80 * time.Microsecond,
						ApplyMutexLockDuration:        90 * time.Microsecond,
						ApplyWriteLeaderWaitDuration:  100 * time.Microsecond,
						ApplyWriteWalDuration:         101 * time.Microsecond,
						ApplyWriteMemtableDuration:    102 * time.Microsecond,
					},
				}},
			LockRPCTime:  int64(time.Second * 5),
			LockRPCCount: 50,
			RetryCount:   2,
			ResolveLock: util.ResolveLockDetail{
				ResolveLockTime: int64(time.Second * 2),
			},
		},
		CommitDetail: &util.CommitDetails{
			GetCommitTsTime: time.Second,
			GetLatestTsTime: time.Second,
			PrewriteTime:    time.Second,
			CommitTime:      time.Second,
			LocalLatchTime:  time.Second,

			Mu: struct {
				sync.Mutex
				CommitBackoffTime    int64
				PrewriteBackoffTypes []string
				CommitBackoffTypes   []string
				SlowestPrewrite      util.ReqDetailInfo
				CommitPrimary        util.ReqDetailInfo
			}{
				CommitBackoffTime: int64(time.Second),
				PrewriteBackoffTypes: []string{
					"backoff1",
					"backoff2",
				},
				CommitBackoffTypes: []string{
					"commit1",
					"commit2",
				},
				SlowestPrewrite: util.ReqDetailInfo{
					ReqTotalTime: time.Second,
					Region:       1000,
					StoreAddr:    "tikv-1:20160",
					ExecDetails: util.TiKVExecDetails{
						TimeDetail: &util.TimeDetail{
							TotalRPCWallTime: 500 * time.Millisecond,
						},
						ScanDetail: &util.ScanDetail{
							ProcessedKeys:             10,
							TotalKeys:                 100,
							RocksdbDeleteSkippedCount: 1,
							RocksdbKeySkippedCount:    1,
							RocksdbBlockCacheHitCount: 1,
							RocksdbBlockReadCount:     1,
							RocksdbBlockReadByte:      100,
							RocksdbBlockReadDuration:  20 * time.Millisecond,
						},
						WriteDetail: &util.WriteDetail{
							StoreBatchWaitDuration:        10 * time.Microsecond,
							ProposeSendWaitDuration:       20 * time.Microsecond,
							PersistLogDuration:            30 * time.Microsecond,
							RaftDbWriteLeaderWaitDuration: 40 * time.Microsecond,
							RaftDbSyncLogDuration:         45 * time.Microsecond,
							RaftDbWriteMemtableDuration:   50 * time.Microsecond,
							CommitLogDuration:             60 * time.Microsecond,
							ApplyBatchWaitDuration:        70 * time.Microsecond,
							ApplyLogDuration:              80 * time.Microsecond,
							ApplyMutexLockDuration:        90 * time.Microsecond,
							ApplyWriteLeaderWaitDuration:  100 * time.Microsecond,
							ApplyWriteWalDuration:         101 * time.Microsecond,
							ApplyWriteMemtableDuration:    102 * time.Microsecond,
						},
					},
				},
				CommitPrimary: util.ReqDetailInfo{
					ReqTotalTime: 2 * time.Second,
					Region:       2000,
					StoreAddr:    "tikv-2:20160",
					ExecDetails: util.TiKVExecDetails{
						TimeDetail: &util.TimeDetail{
							TotalRPCWallTime: 1000 * time.Millisecond,
						},
						ScanDetail: &util.ScanDetail{
							ProcessedKeys:             20,
							TotalKeys:                 200,
							RocksdbDeleteSkippedCount: 2,
							RocksdbKeySkippedCount:    2,
							RocksdbBlockCacheHitCount: 2,
							RocksdbBlockReadCount:     2,
							RocksdbBlockReadByte:      200,
							RocksdbBlockReadDuration:  40 * time.Millisecond,
						},
						WriteDetail: &util.WriteDetail{
							StoreBatchWaitDuration:        110 * time.Microsecond,
							ProposeSendWaitDuration:       120 * time.Microsecond,
							PersistLogDuration:            130 * time.Microsecond,
							RaftDbWriteLeaderWaitDuration: 140 * time.Microsecond,
							RaftDbSyncLogDuration:         145 * time.Microsecond,
							RaftDbWriteMemtableDuration:   150 * time.Microsecond,
							CommitLogDuration:             160 * time.Microsecond,
							ApplyBatchWaitDuration:        170 * time.Microsecond,
							ApplyLogDuration:              180 * time.Microsecond,
							ApplyMutexLockDuration:        190 * time.Microsecond,
							ApplyWriteLeaderWaitDuration:  200 * time.Microsecond,
							ApplyWriteWalDuration:         201 * time.Microsecond,
							ApplyWriteMemtableDuration:    202 * time.Microsecond,
						},
					},
				},
			},
			WriteKeys:         1,
			WriteSize:         1,
			PrewriteRegionNum: 1,
			TxnRetry:          1,
			ResolveLock: util.ResolveLockDetail{
				ResolveLockTime: 1000000000, // 10^9 ns = 1s
			},
		},
		CopExecDetails: CopExecDetails{
			BackoffTime: time.Second,
			ScanDetail: &util.ScanDetail{
				ProcessedKeys:             10,
				TotalKeys:                 100,
				RocksdbDeleteSkippedCount: 1,
				RocksdbKeySkippedCount:    1,
				RocksdbBlockCacheHitCount: 1,
				RocksdbBlockReadCount:     1,
				RocksdbBlockReadByte:      100,
				RocksdbBlockReadDuration:  time.Millisecond,
			},
			TimeDetail: util.TimeDetail{
				ProcessTime: 2*time.Second + 5*time.Millisecond,
				WaitTime:    time.Second,
			}},
	}
	expected := "Cop_time: 1.003 Process_time: 2.005 Wait_time: 1 Backoff_time: 1 LockKeys_time: 1 Request_count: 1 Prewrite_time: 1 Commit_time: " +
		"1 Get_commit_ts_time: 1 Get_latest_ts_time: 1 Commit_backoff_time: 1 " +
		"Prewrite_Backoff_types: [backoff1 backoff2] Commit_Backoff_types: [commit1 commit2] " +
		"Slowest_prewrite_rpc_detail: {total:1.000s, region_id: 1000, " +
		"store: tikv-1:20160, time_detail: {tikv_wall_time: 500ms}, scan_detail: {total_process_keys: 10, total_keys: 100, " +
		"rocksdb: {delete_skipped_count: 1, key_skipped_count: 1, block: {cache_hit_count: 1, read_count: 1, " +
		"read_byte: 100 Bytes, read_time: 20ms}}}, write_detail: {store_batch_wait: 10µs, propose_send_wait: 20µs, " +
		"persist_log: {total: 30µs, write_leader_wait: 40µs, sync_log: 45µs, write_memtable: 50µs}, " +
		"commit_log: 60µs, apply_batch_wait: 70µs, apply: {total:80µs, mutex_lock: 90µs, write_leader_wait: 100µs, " +
		"write_wal: 101µs, write_memtable: 102µs}, scheduler: {process: 0s}}} " +
		"Commit_primary_rpc_detail: {total:2.000s, region_id: 2000, " +
		"store: tikv-2:20160, time_detail: {tikv_wall_time: 1s}, scan_detail: {total_process_keys: 20, total_keys: 200, " +
		"rocksdb: {delete_skipped_count: 2, key_skipped_count: 2, block: {cache_hit_count: 2, read_count: 2, " +
		"read_byte: 200 Bytes, read_time: 40ms}}}, write_detail: {store_batch_wait: 110µs, propose_send_wait: 120µs, " +
		"persist_log: {total: 130µs, write_leader_wait: 140µs, sync_log: 145µs, write_memtable: 150µs}, " +
		"commit_log: 160µs, apply_batch_wait: 170µs, apply: {total:180µs, mutex_lock: 190µs, write_leader_wait: 200µs, " +
		"write_wal: 201µs, write_memtable: 202µs}, scheduler: {process: 0s}}} " +
		"Resolve_lock_time: 1 Local_latch_wait_time: 1 Write_keys: 1 Write_size: " +
		"1 Prewrite_region: 1 Txn_retry: 1 Process_keys: 10 Total_keys: 100 Rocksdb_delete_skipped_count: 1 Rocksdb_key_skipped_count: " +
		"1 Rocksdb_block_cache_hit_count: 1 Rocksdb_block_read_count: 1 Rocksdb_block_read_byte: 100 Rocksdb_block_read_time: 0.001"
	require.Equal(t, expected, detail.String())
	detail = &ExecDetails{}
	require.Equal(t, "", detail.String())
}

func mockExecutorExecutionSummary(TimeProcessedNs, NumProducedRows, NumIterations uint64) *tipb.ExecutorExecutionSummary {
	return &tipb.ExecutorExecutionSummary{TimeProcessedNs: &TimeProcessedNs, NumProducedRows: &NumProducedRows,
		NumIterations: &NumIterations}
}

func mockExecutorExecutionSummaryForTiFlash(TimeProcessedNs, NumProducedRows, NumIterations, Concurrency, dmfileScannedRows, dmfileSkippedRows, totalDmfileRsCheckMs, totalDmfileReadTimeMs, totalBuildSnapshotMs, localRegions, remoteRegions, totalLearnerReadMs, disaggReadCacheHitBytes, disaggReadCacheMissBytes, minTSOWaitTime, pipelineBreakerWaitTime, pipelineQueueTime uint64, innerZoneSendBytes uint64, interZoneSendBytes uint64, innerZoneReceiveBytes uint64, interZoneReceiveBytes uint64, ExecutorID string) *tipb.ExecutorExecutionSummary {
	tiflashScanContext := tipb.TiFlashScanContext{
		DmfileDataScannedRows:    &dmfileScannedRows,
		DmfileDataSkippedRows:    &dmfileSkippedRows,
		TotalDmfileRsCheckMs:     &totalDmfileRsCheckMs,
		TotalDmfileReadMs:        &totalDmfileReadTimeMs,
		TotalBuildSnapshotMs:     &totalBuildSnapshotMs,
		LocalRegions:             &localRegions,
		RemoteRegions:            &remoteRegions,
		TotalLearnerReadMs:       &totalLearnerReadMs,
		DisaggReadCacheHitBytes:  &disaggReadCacheHitBytes,
		DisaggReadCacheMissBytes: &disaggReadCacheMissBytes,
	}
	tiflashWaitSummary := tipb.TiFlashWaitSummary{
		MinTSOWaitNs:          &minTSOWaitTime,
		PipelineQueueWaitNs:   &pipelineBreakerWaitTime,
		PipelineBreakerWaitNs: &pipelineQueueTime,
	}
	tiflashNetworkSummary := tipb.TiFlashNetWorkSummary{
		InnerZoneSendBytes:    &innerZoneSendBytes,
		InterZoneSendBytes:    &interZoneSendBytes,
		InnerZoneReceiveBytes: &innerZoneReceiveBytes,
		InterZoneReceiveBytes: &interZoneReceiveBytes,
	}
	return &tipb.ExecutorExecutionSummary{TimeProcessedNs: &TimeProcessedNs, NumProducedRows: &NumProducedRows,
		NumIterations: &NumIterations, Concurrency: &Concurrency, ExecutorId: &ExecutorID, DetailInfo: &tipb.ExecutorExecutionSummary_TiflashScanContext{TiflashScanContext: &tiflashScanContext}, TiflashWaitSummary: &tiflashWaitSummary, TiflashNetworkSummary: &tiflashNetworkSummary}
}

func TestCopRuntimeStats(t *testing.T) {
	stats := NewRuntimeStatsColl(nil)
	tableScanID := 1
	aggID := 2
	tableReaderID := 3
	stats.RecordOneCopTask(tableScanID, kv.TiKV, mockExecutorExecutionSummary(1, 1, 1))
	stats.RecordOneCopTask(tableScanID, kv.TiKV, mockExecutorExecutionSummary(2, 2, 2))
	stats.RecordOneCopTask(aggID, kv.TiKV, mockExecutorExecutionSummary(3, 3, 3))
	stats.RecordOneCopTask(aggID, kv.TiKV, mockExecutorExecutionSummary(4, 4, 4))
	scanDetail := &util.ScanDetail{
		TotalKeys:                 15,
		ProcessedKeys:             10,
		ProcessedKeysSize:         10,
		RocksdbDeleteSkippedCount: 5,
		RocksdbKeySkippedCount:    1,
		RocksdbBlockCacheHitCount: 10,
		RocksdbBlockReadCount:     20,
		RocksdbBlockReadByte:      100,
	}
	stats.RecordCopStats(tableScanID, kv.TiKV, scanDetail, util.TimeDetail{}, nil)
	require.True(t, stats.ExistsCopStats(tableScanID))

	cop := stats.GetCopStats(tableScanID)
	expected := "tikv_task:{proc max:2ns, min:1ns, avg: 1ns, p80:2ns, p95:2ns, iters:3, tasks:2}, " +
		"scan_detail: {total_process_keys: 10, total_process_keys_size: 10, total_keys: 15, rocksdb: {delete_skipped_count: 5, key_skipped_count: 1, block: {cache_hit_count: 10, read_count: 20, read_byte: 100 Bytes}}}"
	require.Equal(t, expected, cop.String())

	require.NotNil(t, cop.stats)
	require.Equal(t, "time:3ns, loops:3", cop.stats.String())
	require.Equal(t, "tikv_task:{proc max:4ns, min:3ns, avg: 3ns, p80:4ns, p95:4ns, iters:7, tasks:2}", stats.GetCopStats(aggID).String())

	rootStats := stats.GetRootStats(tableReaderID)
	require.NotNil(t, rootStats)
	require.True(t, stats.ExistsRootStats(tableReaderID))

	cop.scanDetail.ProcessedKeys = 0
	cop.scanDetail.ProcessedKeysSize = 0
	cop.scanDetail.RocksdbKeySkippedCount = 0
	cop.scanDetail.RocksdbBlockReadCount = 0
	// Print all fields even though the value of some fields is 0.
	str := "tikv_task:{proc max:2ns, min:1ns, avg: 1ns, p80:2ns, p95:2ns, iters:3, tasks:2}, scan_detail: {total_keys: 15, rocksdb: {delete_skipped_count: 5, block: {cache_hit_count: 10, read_byte: 100 Bytes}}}"
	require.Equal(t, str, cop.String())
	zeroScanDetail := util.ScanDetail{}
	zeroCopStats := CopRuntimeStats{}
	require.Equal(t, "", zeroScanDetail.String())
	require.Equal(t, "", zeroTimeDetail.String())
	require.Equal(t, "", zeroCopStats.String())
}

func TestRUV2MetricsSnapshotCalculateRUValues(t *testing.T) {
	weights := defaultRUV2WeightsForTest()
	metrics := NewRUV2Metrics()
	metrics.AddResultChunkCells(1000)
	metrics.AddExecutorMetric(1, "TableReader", 5)
	metrics.AddExecutorMetric(1, "Projection", 7)
	metrics.AddExecutorMetric(2, "Selection", 11)
	metrics.AddExecutorMetric(3, "HashJoin", 13)
	metrics.AddExecutorL5InsertRows(17)
	metrics.AddPlanCnt(19)
	metrics.AddPlanDeriveStatsPaths(23)
	metrics.AddResourceManagerReadCnt(29)
	metrics.AddResourceManagerWriteCnt(31)
	metrics.AddSessionParserTotal(37)
	metrics.AddTxnCnt(41)
	metrics.AddTiKVKVEngineCacheMiss(43)
	metrics.AddTiKVCoprocessorWorkTotal("BatchSelection", 53)
	metrics.AddTiKVCoprocessorWorkTotal("BatchTopN", 59)
	metrics.AddTiKVCoprocessorExecutorIterations(61)
	metrics.AddTiKVCoprocessorResponseBytes(67)
	metrics.AddTiKVRaftstoreStoreWriteTriggerWB(71)
	metrics.AddTiKVStorageProcessedKeysBatchGet(73)
	metrics.AddTiKVStorageProcessedKeysGet(79)

	tidbRU := metrics.CalculateRUValues(weights)
	tikvRU := float64(157258)
	tiflashRU := float64(24680)
	totalRU := metrics.TotalRU(weights, tikvRU, tiflashRU)
	require.InEpsilon(t, 114198.0, tidbRU, 0.01)
	require.InEpsilon(t, 157258.0, tikvRU, 0.01)
	require.InEpsilon(t, 24680.0, tiflashRU, 0.01)
	require.InEpsilon(t, 296136.0, totalRU, 0.01)

	t.Run("zero scale stays zero", func(t *testing.T) {
		zeroScaleWeights := weights
		zeroScaleWeights.RUScale = 0
		require.Zero(t, metrics.CalculateRUValues(zeroScaleWeights))
		require.Equal(t, tikvRU+tiflashRU, metrics.TotalRU(zeroScaleWeights, tikvRU, tiflashRU))
	})
}

func TestRUV2MetricsSnapshotFreezesRUValues(t *testing.T) {
	weights := defaultRUV2WeightsForTest()
	metrics := NewRUV2Metrics()
	metrics.AddResultChunkCells(1000)
	metrics.AddPlanCnt(2)

	baseline := metrics.CalculateRUValues(weights)

	updated := weights
	updated.ResultChunkCells *= 10
	updated.PlanCnt *= 10

	require.NotEqual(t, baseline, metrics.CalculateRUValues(updated))
}

func TestFormatRUV2MetricsIncludesRUValuesFirst(t *testing.T) {
	weights := defaultRUV2WeightsForTest()
	metrics := NewRUV2Metrics()
	metrics.AddResultChunkCells(1000)
	metrics.AddResourceManagerWriteCnt(20)
	metrics.AddTiKVCoprocessorWorkTotal("BatchTopN", 10)
	total, formatted := FormatRUV2Summary(metrics, weights, 10987, 246)

	require.Equal(t, "19983.42", total)
	require.Equal(t, total, FormatRUV2Total(metrics, weights, 10987, 246))
	require.Equal(t, formatted, FormatRUV2Metrics(metrics, weights, 10987, 246))
	require.Contains(t, formatted, "tidb_ru:")
	require.Contains(t, formatted, "tikv_ru:")
	require.Contains(t, formatted, "tiflash_ru:")
	require.Contains(t, formatted, "total_ru:")
	require.True(t, strings.HasPrefix(formatted, "total_ru:"))

	parts := strings.Split(formatted, ", ")
	require.Len(t, parts, 7)
	require.Equal(t, "total_ru:19983.42", parts[0])
	require.Equal(t, "tidb_ru:8750.42", parts[1])
	require.Equal(t, "tikv_ru:10987.00", parts[2])
	require.Equal(t, "tiflash_ru:246.00", parts[3])
}

func TestRURuntimeStatsStringIncludesTiFlashRU(t *testing.T) {
	stats := &RURuntimeStats{
		RUDetails: util.NewRUDetails(),
		Metrics:   NewRUV2Metrics(),
		Weights:   defaultRUV2WeightsForTest(),
		RUVersion: rmclient.RUVersionV2,
	}
	stats.RUDetails.AddTiKVRUV2(200)
	stats.RUDetails.UpdateTiFlash(&rmpb.Consumption{RRU: 100, WRU: 200})

	require.Equal(t, "RU:500.00", stats.String())
}

func TestCopRuntimeStatsForTiFlash(t *testing.T) {
	stats := NewRuntimeStatsColl(nil)
	tableScanID := 1
	aggID := 2
	tableReaderID := 3
	stats.RecordOneCopTask(tableScanID, kv.TiFlash, mockExecutorExecutionSummaryForTiFlash(1, 1, 1, 1, 8192, 0, 15, 200, 40, 10, 4, 1, 100, 50, 30000000, 20000000, 10000000, 1000, 2000, 3000, 4000, "tablescan_"+strconv.Itoa(tableScanID)))
	stats.RecordOneCopTask(tableScanID, kv.TiFlash, mockExecutorExecutionSummaryForTiFlash(2, 2, 2, 1, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 20000000, 10000000, 5000000, 10000, 20000, 30000, 40000, "tablescan_"+strconv.Itoa(tableScanID)))
	stats.RecordOneCopTask(aggID, kv.TiFlash, mockExecutorExecutionSummaryForTiFlash(3, 3, 3, 1, 12000, 6000, 60, 1000, 20, 5, 1, 0, 20, 0, 0, 0, 0, 0, 0, 0, 0, "aggregation_"+strconv.Itoa(aggID)))
	stats.RecordOneCopTask(aggID, kv.TiFlash, mockExecutorExecutionSummaryForTiFlash(4, 4, 4, 1, 8192, 80000, 40, 2000, 30, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "aggregation_"+strconv.Itoa(aggID)))
	scanDetail := &util.ScanDetail{
		TotalKeys:                 10,
		ProcessedKeys:             10,
		RocksdbDeleteSkippedCount: 10,
		RocksdbKeySkippedCount:    1,
		RocksdbBlockCacheHitCount: 10,
		RocksdbBlockReadCount:     10,
		RocksdbBlockReadByte:      100,
	}
	stats.RecordCopStats(tableScanID, kv.TiFlash, scanDetail, util.TimeDetail{}, nil)
	require.True(t, stats.ExistsCopStats(tableScanID))

	cop := stats.GetCopStats(tableScanID)
	require.Equal(t, "tiflash_task:{proc max:2ns, min:1ns, avg: 1ns, p80:2ns, p95:2ns, iters:3, tasks:2, threads:2}, tiflash_wait: {minTSO_wait: 20ms, pipeline_breaker_wait: 5ms, pipeline_queue_wait: 10ms}, tiflash_network: {inner_zone_send_bytes: 11000, inter_zone_send_bytes: 22000, inner_zone_receive_bytes: 33000, inter_zone_receive_bytes: 44000}, tiflash_scan:{mvcc_input_rows:0, mvcc_input_bytes:0, mvcc_output_rows:0, local_regions:10, remote_regions:4, tot_learner_read:1ms, region_balance:none, delta_rows:0, delta_bytes:0, segments:0, stale_read_regions:0, tot_build_snapshot:40ms, tot_build_bitmap:0ms, tot_build_inputstream:0ms, min_local_stream:0ms, max_local_stream:0ms, dtfile:{data_scanned_rows:8192, data_skipped_rows:0, mvcc_scanned_rows:0, mvcc_skipped_rows:0, lm_filter_scanned_rows:0, lm_filter_skipped_rows:0, tot_rs_index_check:15ms, tot_read:202ms, disagg_cache_hit_bytes: 100, disagg_cache_miss_bytes: 50}}", cop.String())

	copStats := cop.stats
	require.NotNil(t, copStats)

	require.Equal(t, "time:3ns, loops:3, threads:2, tiflash_wait: {minTSO_wait: 20ms, pipeline_breaker_wait: 5ms, pipeline_queue_wait: 10ms}, tiflash_network: {inner_zone_send_bytes: 11000, inter_zone_send_bytes: 22000, inner_zone_receive_bytes: 33000, inter_zone_receive_bytes: 44000}, tiflash_scan:{mvcc_input_rows:0, mvcc_input_bytes:0, mvcc_output_rows:0, local_regions:10, remote_regions:4, tot_learner_read:1ms, region_balance:none, delta_rows:0, delta_bytes:0, segments:0, stale_read_regions:0, tot_build_snapshot:40ms, tot_build_bitmap:0ms, tot_build_inputstream:0ms, min_local_stream:0ms, max_local_stream:0ms, dtfile:{data_scanned_rows:8192, data_skipped_rows:0, mvcc_scanned_rows:0, mvcc_skipped_rows:0, lm_filter_scanned_rows:0, lm_filter_skipped_rows:0, tot_rs_index_check:15ms, tot_read:202ms, disagg_cache_hit_bytes: 100, disagg_cache_miss_bytes: 50}}", copStats.String())
	expected := "tiflash_task:{proc max:4ns, min:3ns, avg: 3ns, p80:4ns, p95:4ns, iters:7, tasks:2, threads:2}, tiflash_scan:{mvcc_input_rows:0, mvcc_input_bytes:0, mvcc_output_rows:0, local_regions:6, remote_regions:2, tot_learner_read:0ms, region_balance:none, delta_rows:0, delta_bytes:0, segments:0, stale_read_regions:0, tot_build_snapshot:50ms, tot_build_bitmap:0ms, tot_build_inputstream:0ms, min_local_stream:0ms, max_local_stream:0ms, dtfile:{data_scanned_rows:20192, data_skipped_rows:86000, mvcc_scanned_rows:0, mvcc_skipped_rows:0, lm_filter_scanned_rows:0, lm_filter_skipped_rows:0, tot_rs_index_check:100ms, tot_read:3000ms, disagg_cache_hit_bytes: 20, disagg_cache_miss_bytes: 0}}"
	require.Equal(t, expected, stats.GetCopStats(aggID).String())

	rootStats := stats.GetRootStats(tableReaderID)
	require.NotNil(t, rootStats)
	require.True(t, stats.ExistsRootStats(tableReaderID))

	stmtNetworkStats := stats.GetStmtCopRuntimeStats().TiflashNetworkStats
	require.Equal(t, stmtNetworkStats.innerZoneSendBytes, uint64(11000))
	require.Equal(t, stmtNetworkStats.interZoneSendBytes, uint64(22000))
	require.Equal(t, stmtNetworkStats.innerZoneReceiveBytes, uint64(33000))
	require.Equal(t, stmtNetworkStats.interZoneReceiveBytes, uint64(44000))
}

func TestVectorSearchStats(t *testing.T) {
	stats := NewRuntimeStatsColl(nil)

	var v uint64 = 1

	execSummary := mockExecutorExecutionSummaryForTiFlash(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "")
	execSummary.DetailInfo.(*tipb.ExecutorExecutionSummary_TiflashScanContext).TiflashScanContext.VectorIdxLoadFromS3 = &v
	stats.RecordOneCopTask(1, kv.TiFlash, execSummary)
	s := stats.GetCopStats(1)
	require.Equal(t, "tiflash_task:{time:0s, loops:0, threads:0}, vector_idx:{load:{total:0ms,from_s3:1,from_disk:0,from_cache:0},search:{total:0ms,visited_nodes:0,discarded_nodes:0},read:{vec_total:0ms,others_total:0ms}}, tiflash_scan:{mvcc_input_rows:0, mvcc_input_bytes:0, mvcc_output_rows:0, local_regions:0, remote_regions:0, tot_learner_read:0ms, region_balance:none, delta_rows:0, delta_bytes:0, segments:0, stale_read_regions:0, tot_build_snapshot:0ms, tot_build_bitmap:0ms, tot_build_inputstream:0ms, min_local_stream:0ms, max_local_stream:0ms, dtfile:{data_scanned_rows:0, data_skipped_rows:0, mvcc_scanned_rows:0, mvcc_skipped_rows:0, lm_filter_scanned_rows:0, lm_filter_skipped_rows:0, tot_rs_index_check:0ms, tot_read:0ms}}", s.String())
}

func TestRuntimeStatsWithCommit(t *testing.T) {
	commitDetail := &util.CommitDetails{
		GetCommitTsTime: time.Second,
		PrewriteTime:    time.Second,
		CommitTime:      time.Second,
		Mu: struct {
			sync.Mutex
			CommitBackoffTime    int64
			PrewriteBackoffTypes []string
			CommitBackoffTypes   []string
			SlowestPrewrite      util.ReqDetailInfo
			CommitPrimary        util.ReqDetailInfo
		}{
			CommitBackoffTime:    int64(time.Second),
			PrewriteBackoffTypes: []string{"backoff1", "backoff2", "backoff1"},
			CommitBackoffTypes:   []string{},
			SlowestPrewrite: util.ReqDetailInfo{
				ReqTotalTime: time.Second,
				Region:       1000,
				StoreAddr:    "tikv-1:20160",
				ExecDetails: util.TiKVExecDetails{
					TimeDetail: &util.TimeDetail{
						TotalRPCWallTime:  500 * time.Millisecond,
						KvGrpcWaitTime:    100 * time.Millisecond,
						KvGrpcProcessTime: 200 * time.Millisecond,
					},
					ScanDetail: &util.ScanDetail{
						ProcessedKeys:             10,
						TotalKeys:                 100,
						RocksdbDeleteSkippedCount: 1,
						RocksdbKeySkippedCount:    1,
						RocksdbBlockCacheHitCount: 1,
						RocksdbBlockReadCount:     1,
						RocksdbBlockReadByte:      100,
						RocksdbBlockReadDuration:  20 * time.Millisecond,
					},
					WriteDetail: &util.WriteDetail{
						StoreBatchWaitDuration:               10 * time.Microsecond,
						ProposeSendWaitDuration:              20 * time.Microsecond,
						PersistLogDuration:                   30 * time.Microsecond,
						RaftDbWriteLeaderWaitDuration:        40 * time.Microsecond,
						RaftDbSyncLogDuration:                45 * time.Microsecond,
						RaftDbWriteMemtableDuration:          50 * time.Microsecond,
						CommitLogDuration:                    60 * time.Microsecond,
						ApplyBatchWaitDuration:               70 * time.Microsecond,
						ApplyLogDuration:                     80 * time.Microsecond,
						ApplyMutexLockDuration:               90 * time.Microsecond,
						ApplyWriteLeaderWaitDuration:         100 * time.Microsecond,
						ApplyWriteWalDuration:                101 * time.Microsecond,
						ApplyWriteMemtableDuration:           102 * time.Microsecond,
						SchedulerLatchWaitDuration:           103 * time.Microsecond,
						SchedulerProcessDuration:             104 * time.Microsecond,
						SchedulerThrottleDuration:            105 * time.Microsecond,
						SchedulerPessimisticLockWaitDuration: 106 * time.Microsecond,
					},
				},
			},
			CommitPrimary: util.ReqDetailInfo{},
		},
		WriteKeys:         3,
		WriteSize:         66,
		PrewriteRegionNum: 5,
		TxnRetry:          2,
		ResolveLock: util.ResolveLockDetail{
			ResolveLockTime: int64(time.Second),
		},
	}
	stats := &RuntimeStatsWithCommit{
		Commit: commitDetail,
	}
	expect := "commit_txn: {prewrite:1s, get_commit_ts:1s, commit:1s, backoff: {time: 1s, prewrite type: [backoff1 backoff2]}, " +
		"slowest_prewrite_rpc: {total: 1.000s, region_id: 1000, store: tikv-1:20160, " +
		"time_detail: {tikv_grpc_process_time: 200ms, tikv_grpc_wait_time: 100ms, tikv_wall_time: 500ms}, " +
		"scan_detail: {total_process_keys: 10, total_keys: 100, rocksdb: {delete_skipped_count: 1, key_skipped_count: 1, " +
		"block: {cache_hit_count: 1, read_count: 1, read_byte: 100 Bytes, read_time: 20ms}}}, " +
		"write_detail: {store_batch_wait: 10µs, propose_send_wait: 20µs, persist_log: {total: 30µs, write_leader_wait: 40µs, " +
		"sync_log: 45µs, write_memtable: 50µs}, commit_log: 60µs, apply_batch_wait: 70µs, apply: {total:80µs, mutex_lock: 90µs, " +
		"write_leader_wait: 100µs, write_wal: 101µs, write_memtable: 102µs}, scheduler: {process: 104µs, latch_wait: 103µs, " +
		"pessimistic_lock_wait: 106µs, throttle: 105µs}}}, resolve_lock: 1s, region_num:5, write_keys:3" +
		", write_byte:66, txn_retry:2}"
	require.Equal(t, expect, stats.String())

	lockDetail := &util.LockKeysDetails{
		TotalTime:   time.Second,
		RegionNum:   2,
		LockKeys:    10,
		BackoffTime: int64(time.Second * 3),
		Mu: struct {
			sync.Mutex
			BackoffTypes        []string
			SlowestReqTotalTime time.Duration
			SlowestRegion       uint64
			SlowestStoreAddr    string
			SlowestExecDetails  util.TiKVExecDetails
		}{
			BackoffTypes: []string{
				"backoff4",
				"backoff5",
				"backoff5",
			},
			SlowestReqTotalTime: time.Second,
			SlowestRegion:       1000,
			SlowestStoreAddr:    "tikv-1:20160",
			SlowestExecDetails: util.TiKVExecDetails{
				TimeDetail: &util.TimeDetail{
					TotalRPCWallTime: 500 * time.Millisecond,
				},
				ScanDetail: &util.ScanDetail{
					ProcessedKeys:             10,
					TotalKeys:                 100,
					RocksdbDeleteSkippedCount: 1,
					RocksdbKeySkippedCount:    1,
					RocksdbBlockCacheHitCount: 1,
					RocksdbBlockReadCount:     1,
					RocksdbBlockReadByte:      100,
					RocksdbBlockReadDuration:  20 * time.Millisecond,
				},
				WriteDetail: &util.WriteDetail{
					StoreBatchWaitDuration:        10 * time.Microsecond,
					ProposeSendWaitDuration:       20 * time.Microsecond,
					PersistLogDuration:            30 * time.Microsecond,
					RaftDbWriteLeaderWaitDuration: 40 * time.Microsecond,
					RaftDbSyncLogDuration:         45 * time.Microsecond,
					RaftDbWriteMemtableDuration:   50 * time.Microsecond,
					CommitLogDuration:             60 * time.Microsecond,
					ApplyBatchWaitDuration:        70 * time.Microsecond,
					ApplyLogDuration:              80 * time.Microsecond,
					ApplyMutexLockDuration:        90 * time.Microsecond,
					ApplyWriteLeaderWaitDuration:  100 * time.Microsecond,
					ApplyWriteWalDuration:         101 * time.Microsecond,
					ApplyWriteMemtableDuration:    102 * time.Microsecond,
				},
			}},
		LockRPCTime:  int64(time.Second * 5),
		LockRPCCount: 50,
		RetryCount:   2,
		ResolveLock: util.ResolveLockDetail{
			ResolveLockTime: int64(time.Second * 2),
		},
	}
	stats = &RuntimeStatsWithCommit{
		LockKeys: lockDetail,
	}
	expect = "lock_keys: {time:1s, region:2, keys:10, resolve_lock:2s, backoff: {time: 3s, type: [backoff4 backoff5]}, " +
		"slowest_rpc: {total: 1.000s, region_id: 1000, store: tikv-1:20160, time_detail: {tikv_wall_time: 500ms}, scan_detail: " +
		"{total_process_keys: 10, total_keys: 100, rocksdb: {delete_skipped_count: 1, key_skipped_count: 1, block: " +
		"{cache_hit_count: 1, read_count: 1, read_byte: 100 Bytes, read_time: 20ms}}}, write_detail: " +
		"{store_batch_wait: 10µs, propose_send_wait: 20µs, persist_log: {total: 30µs, write_leader_wait: 40µs, sync_log: 45µs, write_memtable: 50µs}, " +
		"commit_log: 60µs, apply_batch_wait: 70µs, apply: {total:80µs, mutex_lock: 90µs, write_leader_wait: 100µs, write_wal: 101µs, write_memtable: 102µs}, " +
		"scheduler: {process: 0s}}}, lock_rpc:5s, rpc_count:50, retry_count:2}"
	require.Equal(t, expect, stats.String())

	stats.SharedLockKeys = lockDetail.Clone()
	require.Equal(t, expect+", shared_"+expect, stats.String())

	// Test Clone with SharedLockKeys
	clonedStats := stats.Clone().(*RuntimeStatsWithCommit)
	require.Equal(t, stats.String(), clonedStats.String())
	require.NotNil(t, clonedStats.SharedLockKeys)
	require.Equal(t, stats.SharedLockKeys.LockKeys, clonedStats.SharedLockKeys.LockKeys)

	// Test Merge with SharedLockKeys
	stats2 := &RuntimeStatsWithCommit{
		SharedLockKeys: &util.LockKeysDetails{
			TotalTime: time.Second,
			RegionNum: 3,
			LockKeys:  5,
		},
	}
	stats.Merge(stats2)
	require.Equal(t, int32(5), stats.SharedLockKeys.RegionNum)
	require.Equal(t, int32(15), stats.SharedLockKeys.LockKeys)

	// Test Merge into empty SharedLockKeys
	stats3 := &RuntimeStatsWithCommit{}
	stats3.Merge(stats2)
	require.NotNil(t, stats3.SharedLockKeys)
	require.Equal(t, int32(3), stats3.SharedLockKeys.RegionNum)
	require.Equal(t, int32(5), stats3.SharedLockKeys.LockKeys)
}

func TestRootRuntimeStats(t *testing.T) {
	pid := 1
	stmtStats := NewRuntimeStatsColl(nil)
	basic1 := stmtStats.GetBasicRuntimeStats(pid, true)
	basic2 := stmtStats.GetBasicRuntimeStats(pid, true)
	basic1.RecordOpen(time.Millisecond * 10)
	basic1.Record(time.Second, 20)
	basic2.Record(time.Second*2, 30)
	basic2.RecordClose(time.Millisecond * 100)
	concurrency := &RuntimeStatsWithConcurrencyInfo{}
	concurrency.SetConcurrencyInfo(NewConcurrencyInfo("worker", 15))
	commitDetail := &util.CommitDetails{
		GetCommitTsTime:   time.Second,
		PrewriteTime:      time.Second,
		CommitTime:        time.Second,
		WriteKeys:         3,
		WriteSize:         66,
		PrewriteRegionNum: 5,
		TxnRetry:          2,
	}
	stmtStats.RegisterStats(pid, concurrency)
	stmtStats.RegisterStats(pid, &RuntimeStatsWithCommit{
		Commit: commitDetail,
	})
	stats := stmtStats.GetRootStats(1)
	expect := "total_time:3.11s, total_open:10ms, total_close:100ms, loops:2, worker:15, commit_txn: {prewrite:1s, get_commit_ts:1s, commit:1s, region_num:5, write_keys:3, write_byte:66, txn_retry:2}"
	require.Equal(t, expect, stats.String())
}

func TestFormatDurationForExplain(t *testing.T) {
	cases := []struct {
		t string
		s string
	}{
		{"0s", "0s"},
		{"1ns", "1ns"},
		{"9ns", "9ns"},
		{"10ns", "10ns"},
		{"999ns", "999ns"},
		{"1µs", "1µs"},
		{"1.123µs", "1.12µs"},
		{"1.023µs", "1.02µs"},
		{"1.003µs", "1µs"},
		{"10.456µs", "10.5µs"},
		{"10.956µs", "11µs"},
		{"999.056µs", "999.1µs"},
		{"999.988µs", "1ms"},
		{"1.123ms", "1.12ms"},
		{"1.023ms", "1.02ms"},
		{"1.003ms", "1ms"},
		{"10.456ms", "10.5ms"},
		{"10.956ms", "11ms"},
		{"999.056ms", "999.1ms"},
		{"999.988ms", "1s"},
		{"1.123s", "1.12s"},
		{"1.023s", "1.02s"},
		{"1.003s", "1s"},
		{"10.456s", "10.5s"},
		{"10.956s", "11s"},
		{"16m39.056s", "16m39.1s"},
		{"16m39.988s", "16m40s"},
		{"24h16m39.388662s", "24h16m39.4s"},
		{"9.412345ms", "9.41ms"},
		{"10.412345ms", "10.4ms"},
		{"5.999s", "6s"},
		{"100.45µs", "100.5µs"},
	}
	for _, ca := range cases {
		d, err := time.ParseDuration(ca.t)
		require.NoError(t, err)

		result := FormatDuration(d)
		require.Equal(t, ca.s, result)
	}
}

func TestCopRuntimeStats2(t *testing.T) {
	stats := NewRuntimeStatsColl(nil)
	tableScanID := 1
	scanDetail := &util.ScanDetail{
		TotalKeys:                 15,
		ProcessedKeys:             10,
		ProcessedKeysSize:         10,
		RocksdbDeleteSkippedCount: 5,
		RocksdbKeySkippedCount:    1,
		RocksdbBlockCacheHitCount: 10,
		RocksdbBlockReadCount:     20,
		RocksdbBlockReadByte:      100,
	}
	timeDetail := util.TimeDetail{
		ProcessTime:      10 * time.Millisecond,
		SuspendTime:      20 * time.Millisecond,
		WaitTime:         30 * time.Millisecond,
		KvReadWallTime:   5 * time.Millisecond,
		TotalRPCWallTime: 50 * time.Millisecond,
	}
	stats.RecordCopStats(tableScanID, kv.TiKV, scanDetail, util.TimeDetail{}, nil)
	for range 1005 {
		stats.RecordCopStats(tableScanID, kv.TiKV, scanDetail, timeDetail, mockExecutorExecutionSummary(2, 2, 2))
	}

	cop := stats.GetCopStats(tableScanID)
	expected := "tikv_task:{proc max:2ns, min:2ns, avg: 2ns, p80:2ns, p95:2ns, iters:2010, tasks:1005}, " +
		"scan_detail: {total_process_keys: 10060, total_process_keys_size: 10060, total_keys: 15090, " +
		"rocksdb: {delete_skipped_count: 5030, key_skipped_count: 1006, " +
		"block: {cache_hit_count: 10060, read_count: 20120, read_byte: 98.2 KB}}}, " +
		"time_detail: {total_process_time: 10.1s, total_suspend_time: 20.1s, total_wait_time: 30.2s, " +
		"total_kv_read_wall_time: 5.03s, tikv_wall_time: 50.3s}"
	require.Equal(t, expected, cop.String())
	require.Equal(t, expected, cop.String())
}

func TestRURuntimeStatsStringV1(t *testing.T) {
	stats := &RURuntimeStats{
		RUDetails: util.NewRUDetailsWith(10.5, 20.3, 0),
		Metrics:   NewRUV2Metrics(),
		Weights:   defaultRUV2WeightsForTest(),
		RUVersion: rmclient.RUVersionV1,
	}
	// v1: shows RRU + WRU
	require.Equal(t, "RU:30.80", stats.String())
}

func TestRURuntimeStatsStringV1NilDetails(t *testing.T) {
	stats := &RURuntimeStats{
		Metrics:   NewRUV2Metrics(),
		Weights:   defaultRUV2WeightsForTest(),
		RUVersion: rmclient.RUVersionV1,
	}
	// v1 with nil RUDetails returns empty
	require.Equal(t, "", stats.String())
}

func TestRURuntimeStatsStringV2(t *testing.T) {
	stats := &RURuntimeStats{
		RUDetails: util.NewRUDetails(),
		Metrics:   NewRUV2Metrics(),
		Weights:   defaultRUV2WeightsForTest(),
		RUVersion: rmclient.RUVersionV2,
	}
	stats.RUDetails.AddTiKVRUV2(200)
	stats.RUDetails.UpdateTiFlash(&rmpb.Consumption{RRU: 100, WRU: 200})
	// v2: shows total RU from v2 metrics (tikvRU + tiflashRU + tidbRU)
	require.Equal(t, "RU:500.00", stats.String())
}

func TestRURuntimeStatsStringV2ZeroRU(t *testing.T) {
	stats := &RURuntimeStats{
		Metrics:   NewRUV2Metrics(),
		Weights:   defaultRUV2WeightsForTest(),
		RUVersion: rmclient.RUVersionV2,
	}
	// v2 with zero total RU returns empty
	require.Equal(t, "", stats.String())
}

func TestRURuntimeStatsStringDefaultVersion(t *testing.T) {
	// RUVersion=0 (zero value) should default to v1 for backward compatibility
	stats := &RURuntimeStats{
		RUDetails: util.NewRUDetailsWith(10.5, 20.3, 0),
		Metrics:   NewRUV2Metrics(),
		Weights:   defaultRUV2WeightsForTest(),
	}
	// default (v1): shows RRU + WRU
	require.Equal(t, "RU:30.80", stats.String())
}

func TestRURuntimeStatsClonePreservesRUVersion(t *testing.T) {
	stats := &RURuntimeStats{
		RUDetails: util.NewRUDetailsWith(10, 20, 0),
		Metrics:   NewRUV2Metrics(),
		Weights:   defaultRUV2WeightsForTest(),
		RUVersion: rmclient.RUVersionV1,
	}
	cloned := stats.Clone().(*RURuntimeStats)
	require.Equal(t, rmclient.RUVersionV1, cloned.RUVersion)
	// Verify the clone produces the same output
	require.Equal(t, stats.String(), cloned.String())
}

func TestRURuntimeStatsCloneNilPreservesZeroVersion(t *testing.T) {
	var stats *RURuntimeStats
	cloned := stats.Clone().(*RURuntimeStats)
	require.Equal(t, rmclient.RUVersion(0), cloned.RUVersion)
}

func TestRURuntimeStatsMergeRUVersion(t *testing.T) {
	// Merge takes RUVersion from other when receiver has zero value
	dst := &RURuntimeStats{
		Metrics: NewRUV2Metrics(),
		Weights: defaultRUV2WeightsForTest(),
	}
	src := &RURuntimeStats{
		Metrics:   NewRUV2Metrics(),
		Weights:   defaultRUV2WeightsForTest(),
		RUVersion: rmclient.RUVersionV2,
	}
	dst.Merge(src)
	require.Equal(t, rmclient.RUVersionV2, dst.RUVersion)
}

func TestRURuntimeStatsMergeKeepsExistingRUVersion(t *testing.T) {
	// Merge does NOT override a non-zero RUVersion
	dst := &RURuntimeStats{
		Metrics:   NewRUV2Metrics(),
		Weights:   defaultRUV2WeightsForTest(),
		RUVersion: rmclient.RUVersionV1,
	}
	src := &RURuntimeStats{
		Metrics:   NewRUV2Metrics(),
		Weights:   defaultRUV2WeightsForTest(),
		RUVersion: rmclient.RUVersionV2,
	}
	dst.Merge(src)
	require.Equal(t, rmclient.RUVersionV1, dst.RUVersion)
}
