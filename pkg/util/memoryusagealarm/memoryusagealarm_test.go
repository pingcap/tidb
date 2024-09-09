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

package memoryusagealarm

import (
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/stretchr/testify/assert"
)

func TestIfNeedDoRecord(t *testing.T) {
	record := memoryUsageAlarm{}
	record.initMemoryUsageAlarmRecord()

	// mem usage ratio < 70% will not be recorded
	memUsed := 0.69 * float64(record.serverMemoryLimit)
	needRecord, reason := record.needRecord(uint64(memUsed))
	assert.False(t, needRecord)
	assert.Equal(t, NoReason, reason)

	// mem usage ratio > 70% will not be recorded
	memUsed = 0.71 * float64(record.serverMemoryLimit)
	needRecord, reason = record.needRecord(uint64(memUsed))
	assert.True(t, needRecord)
	assert.Equal(t, ExceedAlarmRatio, reason)
	record.lastCheckTime = time.Now()
	record.lastRecordMemUsed = uint64(memUsed)

	// check time - last record time < 60s will not be recorded
	memUsed = 0.71 * float64(record.serverMemoryLimit)
	needRecord, reason = record.needRecord(uint64(memUsed))
	assert.False(t, needRecord)
	assert.Equal(t, NoReason, reason)

	// check time - last record time > 60s will be recorded
	record.lastCheckTime = record.lastCheckTime.Add(-60 * time.Second)
	memUsed = 0.71 * float64(record.serverMemoryLimit)
	needRecord, reason = record.needRecord(uint64(memUsed))
	assert.True(t, needRecord)
	assert.Equal(t, ExceedAlarmRatio, reason)
	record.lastCheckTime = time.Now()
	record.lastRecordMemUsed = uint64(memUsed)

	// mem usage ratio - last mem usage ratio < 10% will not be recorded
	memUsed = 0.80 * float64(record.serverMemoryLimit)
	needRecord, reason = record.needRecord(uint64(memUsed))
	assert.False(t, needRecord)
	assert.Equal(t, NoReason, reason)

	// mem usage ratio - last mem usage ratio > 10% will not be recorded even though check time - last record time
	memUsed = 0.82 * float64(record.serverMemoryLimit)
	needRecord, reason = record.needRecord(uint64(memUsed))
	assert.True(t, needRecord)
	assert.Equal(t, GrowTooFast, reason)
}

func genTime(sec int64) time.Time {
	minStartTime := time.Date(1970, 1, 0, 0, 0, 0, 0, time.UTC).Unix()
	return time.Unix(minStartTime+sec, 0)
}

func TestGetTop10Sql(t *testing.T) {
	record := memoryUsageAlarm{}
	record.initMemoryUsageAlarmRecord()
	record.lastCheckTime = genTime(123456)

	processInfoList := genMockProcessInfoList([]int64{1000, 87263523, 34223},
		[]time.Time{genTime(1234), genTime(123456), genTime(12)},
		3)
	actual := record.getTop10SqlInfoByMemoryUsage(processInfoList)
	assert.Equal(t, "SQL 0: \ncost_time: 0s\ntxn_start_ts: 0\nmem_max: 87263523 Bytes (83.2 MB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 1: \ncost_time: 123444s\ntxn_start_ts: 0\nmem_max: 34223 Bytes (33.4 KB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 2: \ncost_time: 122222s\ntxn_start_ts: 0\nmem_max: 1000 Bytes (1000 Bytes)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\n\n",
		actual.String())
	actual = record.getTop10SqlInfoByCostTime(processInfoList)
	assert.Equal(t, "SQL 0: \ncost_time: 123444s\ntxn_start_ts: 0\nmem_max: 34223 Bytes (33.4 KB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 1: \ncost_time: 122222s\ntxn_start_ts: 0\nmem_max: 1000 Bytes (1000 Bytes)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 2: \ncost_time: 0s\ntxn_start_ts: 0\nmem_max: 87263523 Bytes (83.2 MB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\n\n", actual.String())

	processInfoList = genMockProcessInfoList([]int64{1000, 87263523, 34223, 532355, 123225151, 231231515, 12312, 12515134234, 232, 12414, 15263236, 123123123, 15},
		[]time.Time{genTime(1234), genTime(123456), genTime(12), genTime(3241), genTime(12515), genTime(3215), genTime(61314), genTime(12234), genTime(1123), genTime(512), genTime(11111), genTime(22222), genTime(5512)},
		13)
	actual = record.getTop10SqlInfoByMemoryUsage(processInfoList)
	assert.Equal(t, "SQL 0: \ncost_time: 111222s\ntxn_start_ts: 0\nmem_max: 12515134234 Bytes (11.7 GB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 1: \ncost_time: 120241s\ntxn_start_ts: 0\nmem_max: 231231515 Bytes (220.5 MB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 2: \ncost_time: 110941s\ntxn_start_ts: 0\nmem_max: 123225151 Bytes (117.5 MB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 3: \ncost_time: 101234s\ntxn_start_ts: 0\nmem_max: 123123123 Bytes (117.4 MB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 4: \ncost_time: 0s\ntxn_start_ts: 0\nmem_max: 87263523 Bytes (83.2 MB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 5: \ncost_time: 112345s\ntxn_start_ts: 0\nmem_max: 15263236 Bytes (14.6 MB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 6: \ncost_time: 120215s\ntxn_start_ts: 0\nmem_max: 532355 Bytes (519.9 KB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 7: \ncost_time: 123444s\ntxn_start_ts: 0\nmem_max: 34223 Bytes (33.4 KB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 8: \ncost_time: 122944s\ntxn_start_ts: 0\nmem_max: 12414 Bytes (12.1 KB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 9: \ncost_time: 62142s\ntxn_start_ts: 0\nmem_max: 12312 Bytes (12.0 KB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\n\n", actual.String())
	actual = record.getTop10SqlInfoByCostTime(processInfoList)
	assert.Equal(t, "SQL 0: \ncost_time: 123444s\ntxn_start_ts: 0\nmem_max: 34223 Bytes (33.4 KB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 1: \ncost_time: 122944s\ntxn_start_ts: 0\nmem_max: 12414 Bytes (12.1 KB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 2: \ncost_time: 122333s\ntxn_start_ts: 0\nmem_max: 232 Bytes (232 Bytes)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 3: \ncost_time: 122222s\ntxn_start_ts: 0\nmem_max: 1000 Bytes (1000 Bytes)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 4: \ncost_time: 120241s\ntxn_start_ts: 0\nmem_max: 231231515 Bytes (220.5 MB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 5: \ncost_time: 120215s\ntxn_start_ts: 0\nmem_max: 532355 Bytes (519.9 KB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 6: \ncost_time: 117944s\ntxn_start_ts: 0\nmem_max: 15 Bytes (15 Bytes)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 7: \ncost_time: 112345s\ntxn_start_ts: 0\nmem_max: 15263236 Bytes (14.6 MB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 8: \ncost_time: 111222s\ntxn_start_ts: 0\nmem_max: 12515134234 Bytes (11.7 GB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 9: \ncost_time: 110941s\ntxn_start_ts: 0\nmem_max: 123225151 Bytes (117.5 MB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\n\n", actual.String())
}

func genMockProcessInfoList(memConsumeList []int64, startTimeList []time.Time, size int) []*util.ProcessInfo {
	processInfoList := make([]*util.ProcessInfo, 0, size)
	for i := 0; i < size; i++ {
		tracker := memory.NewTracker(0, 0)
		tracker.Consume(memConsumeList[i])
		var stmtCtxRefCount stmtctx.ReferenceCount = 0
		processInfo := util.ProcessInfo{Time: startTimeList[i],
			StmtCtx:    stmtctx.NewStmtCtx(),
			MemTracker: tracker,
			StatsInfo: func(any) map[string]uint64 {
				return map[string]uint64{}
			},
			RefCountOfStmtCtx: &stmtCtxRefCount,
		}
		processInfoList = append(processInfoList, &processInfo)
	}
	return processInfoList
}

func TestUpdateVariables(t *testing.T) {
	variable.MemoryUsageAlarmRatio.Store(0.3)
	variable.MemoryUsageAlarmKeepRecordNum.Store(3)
	memory.ServerMemoryLimit.Store(1024)

	record := memoryUsageAlarm{}

	record.initMemoryUsageAlarmRecord()
	assert.Equal(t, 0.3, record.memoryUsageAlarmRatio)
	assert.Equal(t, int64(3), record.memoryUsageAlarmKeepRecordNum)
	assert.Equal(t, uint64(1024), record.serverMemoryLimit)
	variable.MemoryUsageAlarmRatio.Store(0.6)
	variable.MemoryUsageAlarmKeepRecordNum.Store(6)
	memory.ServerMemoryLimit.Store(2048)

	record.updateVariable()
	assert.Equal(t, 0.3, record.memoryUsageAlarmRatio)
	assert.Equal(t, int64(3), record.memoryUsageAlarmKeepRecordNum)
	assert.Equal(t, uint64(1024), record.serverMemoryLimit)
	record.lastUpdateVariableTime = record.lastUpdateVariableTime.Add(-60 * time.Second)
	record.updateVariable()
	assert.Equal(t, 0.6, record.memoryUsageAlarmRatio)
	assert.Equal(t, int64(6), record.memoryUsageAlarmKeepRecordNum)
	assert.Equal(t, uint64(2048), record.serverMemoryLimit)
}
