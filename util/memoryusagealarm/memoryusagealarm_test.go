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

	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/memory"
	"github.com/stretchr/testify/assert"
)

func TestIfNeedDoRecord(t *testing.T) {
	record := memoryUsageAlarm{}
	record.initMemoryUsageAlarmRecord()

	// mem usage ratio < 70% will not be recorded
	memUsed := 0.69 * float64(record.serverMemoryQuota)
	needRecord, reason := record.needRecord(uint64(memUsed))
	assert.False(t, needRecord)
	assert.Equal(t, NoReason, reason)

	// mem usage ratio > 70% will not be recorded
	memUsed = 0.71 * float64(record.serverMemoryQuota)
	needRecord, reason = record.needRecord(uint64(memUsed))
	assert.True(t, needRecord)
	assert.Equal(t, ExceedAlarmRatio, reason)
	record.lastCheckTime = time.Now()
	record.lastRecordMemUsed = uint64(memUsed)

	// check time - last record time < 60s will not be recorded
	memUsed = 0.71 * float64(record.serverMemoryQuota)
	needRecord, reason = record.needRecord(uint64(memUsed))
	assert.False(t, needRecord)
	assert.Equal(t, NoReason, reason)

	// check time - last record time > 60s will be recorded
	record.lastCheckTime = record.lastCheckTime.Add(-60 * time.Second)
	memUsed = 0.71 * float64(record.serverMemoryQuota)
	needRecord, reason = record.needRecord(uint64(memUsed))
	assert.True(t, needRecord)
	assert.Equal(t, ExceedAlarmRatio, reason)
	record.lastCheckTime = time.Now()
	record.lastRecordMemUsed = uint64(memUsed)

	// mem usage ratio - last mem usage ratio < 10% will not be recorded
	memUsed = 0.80 * float64(record.serverMemoryQuota)
	needRecord, reason = record.needRecord(uint64(memUsed))
	assert.False(t, needRecord)
	assert.Equal(t, NoReason, reason)

	// mem usage ratio - last mem usage ratio > 10% will not be recorded even though check time - last record time
	memUsed = 0.82 * float64(record.serverMemoryQuota)
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
	assert.Equal(t, "SQL 0: \ncost_time: 0s\ntxn_start_ts: 0\nmem_max: 87263523 Bytes (83.2 MB)\nsql: \nanalyze-version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|actRows|task|access object|execution info|operator info|memory|disk|\nSQL 1: \ncost_time: 123444s\ntxn_start_ts: 0\nmem_max: 34223 Bytes (33.4 KB)\nsql: \nanalyze-version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|actRows|task|access object|execution info|operator info|memory|disk|\nSQL 2: \ncost_time: 122222s\ntxn_start_ts: 0\nmem_max: 1000 Bytes (1000 Bytes)\nsql: \nanalyze-version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|actRows|task|access object|execution info|operator info|memory|disk|\n\n",
		actual.String())
	actual = record.getTop10SqlInfoByCostTime(processInfoList)
	assert.Equal(t, "SQL 0: \ncost_time: 123444s\ntxn_start_ts: 0\nmem_max: 34223 Bytes (33.4 KB)\nsql: \nanalyze-version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|actRows|task|access object|execution info|operator info|memory|disk|\nSQL 1: \ncost_time: 122222s\ntxn_start_ts: 0\nmem_max: 1000 Bytes (1000 Bytes)\nsql: \nanalyze-version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|actRows|task|access object|execution info|operator info|memory|disk|\nSQL 2: \ncost_time: 0s\ntxn_start_ts: 0\nmem_max: 87263523 Bytes (83.2 MB)\nsql: \nanalyze-version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|actRows|task|access object|execution info|operator info|memory|disk|\n\n", actual.String())

	processInfoList = genMockProcessInfoList([]int64{1000, 87263523, 34223, 532355, 123225151, 231231515, 12312, 12515134234, 232, 12414, 15263236, 123123123, 15},
		[]time.Time{genTime(1234), genTime(123456), genTime(12), genTime(3241), genTime(12515), genTime(3215), genTime(61314), genTime(12234), genTime(1123), genTime(512), genTime(11111), genTime(22222), genTime(5512)},
		13)
	actual = record.getTop10SqlInfoByMemoryUsage(processInfoList)
	assert.Equal(t, "SQL 0: \ncost_time: 111222s\ntxn_start_ts: 0\nmem_max: 12515134234 Bytes (11.7 GB)\nsql: \nanalyze-version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|actRows|task|access object|execution info|operator info|memory|disk|\nSQL 1: \ncost_time: 120241s\ntxn_start_ts: 0\nmem_max: 231231515 Bytes (220.5 MB)\nsql: \nanalyze-version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|actRows|task|access object|execution info|operator info|memory|disk|\nSQL 2: \ncost_time: 110941s\ntxn_start_ts: 0\nmem_max: 123225151 Bytes (117.5 MB)\nsql: \nanalyze-version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|actRows|task|access object|execution info|operator info|memory|disk|\nSQL 3: \ncost_time: 101234s\ntxn_start_ts: 0\nmem_max: 123123123 Bytes (117.4 MB)\nsql: \nanalyze-version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|actRows|task|access object|execution info|operator info|memory|disk|\nSQL 4: \ncost_time: 0s\ntxn_start_ts: 0\nmem_max: 87263523 Bytes (83.2 MB)\nsql: \nanalyze-version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|actRows|task|access object|execution info|operator info|memory|disk|\nSQL 5: \ncost_time: 112345s\ntxn_start_ts: 0\nmem_max: 15263236 Bytes (14.6 MB)\nsql: \nanalyze-version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|actRows|task|access object|execution info|operator info|memory|disk|\nSQL 6: \ncost_time: 120215s\ntxn_start_ts: 0\nmem_max: 532355 Bytes (519.9 KB)\nsql: \nanalyze-version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|actRows|task|access object|execution info|operator info|memory|disk|\nSQL 7: \ncost_time: 123444s\ntxn_start_ts: 0\nmem_max: 34223 Bytes (33.4 KB)\nsql: \nanalyze-version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|actRows|task|access object|execution info|operator info|memory|disk|\nSQL 8: \ncost_time: 122944s\ntxn_start_ts: 0\nmem_max: 12414 Bytes (12.1 KB)\nsql: \nanalyze-version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|actRows|task|access object|execution info|operator info|memory|disk|\nSQL 9: \ncost_time: 62142s\ntxn_start_ts: 0\nmem_max: 12312 Bytes (12.0 KB)\nsql: \nanalyze-version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|actRows|task|access object|execution info|operator info|memory|disk|\n\n", actual.String())
	actual = record.getTop10SqlInfoByCostTime(processInfoList)
	assert.Equal(t, "SQL 0: \ncost_time: 123444s\ntxn_start_ts: 0\nmem_max: 34223 Bytes (33.4 KB)\nsql: \nanalyze-version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|actRows|task|access object|execution info|operator info|memory|disk|\nSQL 1: \ncost_time: 122944s\ntxn_start_ts: 0\nmem_max: 12414 Bytes (12.1 KB)\nsql: \nanalyze-version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|actRows|task|access object|execution info|operator info|memory|disk|\nSQL 2: \ncost_time: 122333s\ntxn_start_ts: 0\nmem_max: 232 Bytes (232 Bytes)\nsql: \nanalyze-version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|actRows|task|access object|execution info|operator info|memory|disk|\nSQL 3: \ncost_time: 122222s\ntxn_start_ts: 0\nmem_max: 1000 Bytes (1000 Bytes)\nsql: \nanalyze-version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|actRows|task|access object|execution info|operator info|memory|disk|\nSQL 4: \ncost_time: 120241s\ntxn_start_ts: 0\nmem_max: 231231515 Bytes (220.5 MB)\nsql: \nanalyze-version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|actRows|task|access object|execution info|operator info|memory|disk|\nSQL 5: \ncost_time: 120215s\ntxn_start_ts: 0\nmem_max: 532355 Bytes (519.9 KB)\nsql: \nanalyze-version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|actRows|task|access object|execution info|operator info|memory|disk|\nSQL 6: \ncost_time: 117944s\ntxn_start_ts: 0\nmem_max: 15 Bytes (15 Bytes)\nsql: \nanalyze-version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|actRows|task|access object|execution info|operator info|memory|disk|\nSQL 7: \ncost_time: 112345s\ntxn_start_ts: 0\nmem_max: 15263236 Bytes (14.6 MB)\nsql: \nanalyze-version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|actRows|task|access object|execution info|operator info|memory|disk|\nSQL 8: \ncost_time: 111222s\ntxn_start_ts: 0\nmem_max: 12515134234 Bytes (11.7 GB)\nsql: \nanalyze-version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|actRows|task|access object|execution info|operator info|memory|disk|\nSQL 9: \ncost_time: 110941s\ntxn_start_ts: 0\nmem_max: 123225151 Bytes (117.5 MB)\nsql: \nanalyze-version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|actRows|task|access object|execution info|operator info|memory|disk|\n\n", actual.String())
}

func genMockProcessInfoList(memConsumeList []int64, startTimeList []time.Time, size int) []*util.ProcessInfo {
	processInfoList := make([]*util.ProcessInfo, 0, size)
	for i := 0; i < size; i++ {
		tracker := memory.NewTracker(0, 0)
		tracker.Consume(memConsumeList[i])
		processInfo := util.ProcessInfo{Time: startTimeList[i],
			StmtCtx: &stmtctx.StatementContext{MemTracker: tracker},
			StatsInfo: func(interface{}) map[string]uint64 {
				return map[string]uint64{}
			},
			CurrentAnalyzeRows: func(interface{}, *execdetails.RuntimeStatsColl) [][]string {
				return [][]string{}
			},
		}
		processInfoList = append(processInfoList, &processInfo)
	}
	return processInfoList
}
