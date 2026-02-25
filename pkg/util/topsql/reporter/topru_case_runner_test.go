// Copyright 2026 PingCAP, Inc.
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

package reporter

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/util/topsql/stmtstats"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

type caseSpec struct {
	GoalID             string
	Level              string
	Description        string
	RequireSend        bool
	RURecordsMin       int
	ExecCountMin       uint64
	ExecCountSumMin    uint64
	TotalRUMin         float64
	SQLMetaMatchMarker string
	PlanMetaRequired   *bool
}

type topRUCaseSink struct {
	ch chan *ReportData
}

func (s *topRUCaseSink) TrySend(data *ReportData, _ time.Time) error {
	s.ch <- data
	return nil
}

func (s *topRUCaseSink) OnReporterClosing() {}

func boolPtr(v bool) *bool {
	return &v
}

func runTopRUCase(t *testing.T, cs caseSpec) {
	t.Helper()
	// This runner intentionally builds one deterministic closed 60s window so each
	// generated case validates semantic contracts without clock/ticker flakiness.

	tsr := NewRemoteTopSQLReporter(mockPlanBinaryDecoderFunc, mockPlanBinaryCompressFunc)
	t.Cleanup(tsr.Close)

	sink := &topRUCaseSink{ch: make(chan *ReportData, 1)}
	require.NoError(t, tsr.Register(sink))

	recordCount := cs.RURecordsMin
	if recordCount < 1 {
		recordCount = 1
	}
	marker := cs.SQLMetaMatchMarker
	if marker == "" {
		marker = fmt.Sprintf("topru_gen_%s", strings.ToLower(cs.GoalID))
	}
	totalRUBaseline := cs.TotalRUMin
	if totalRUBaseline <= 0 {
		totalRUBaseline = 0.001
	}
	requiredSum := int(cs.ExecCountSumMin)
	if requiredSum < recordCount {
		requiredSum = recordCount
	}

	execCounts := make([]uint64, recordCount)
	for i := 0; i < recordCount; i++ {
		execCounts[i] = 1
	}
	execCounts[0] += uint64(requiredSum - recordCount)
	if execCounts[0] < cs.ExecCountMin {
		execCounts[0] = cs.ExecCountMin
	}

	const sampleTs = uint64(1700000000)
	switch cs.GoalID {
	case "G7_key_aggregation_by_user_sql_plan":
		// Same SQL/plan under different users must remain isolated by RUKey.User.
		sqlDigest := []byte("S_G7")
		planDigest := []byte("P_G7")
		tsr.RegisterSQL(sqlDigest, fmt.Sprintf("/* %s */ select 7", marker), false)
		tsr.RegisterPlan(planDigest, fmt.Sprintf("plan_%s_7", marker), false)
		tsr.ruAggregator.addBatchToBucket(sampleTs, stmtstats.RUIncrementMap{
			{
				User:       "u1",
				SQLDigest:  stmtstats.BinaryDigest(sqlDigest),
				PlanDigest: stmtstats.BinaryDigest(planDigest),
			}: {TotalRU: 7, ExecCount: 1, ExecDuration: 1000},
			{
				User:       "u2",
				SQLDigest:  stmtstats.BinaryDigest(sqlDigest),
				PlanDigest: stmtstats.BinaryDigest(planDigest),
			}: {TotalRU: 9, ExecCount: 1, ExecDuration: 1000},
		})
	case "G8_same_timestamp_multiple_finish_accumulate":
		// Same key and same bucket timestamp should accumulate RU/ExecCount/Duration.
		sqlDigest := []byte("S_G8")
		planDigest := []byte("P_G8")
		tsr.RegisterSQL(sqlDigest, fmt.Sprintf("/* %s */ select 8", marker), false)
		tsr.RegisterPlan(planDigest, fmt.Sprintf("plan_%s_8", marker), false)
		key := stmtstats.RUKey{
			User:       "root",
			SQLDigest:  stmtstats.BinaryDigest(sqlDigest),
			PlanDigest: stmtstats.BinaryDigest(planDigest),
		}
		tsr.ruAggregator.addBatchToBucket(sampleTs, stmtstats.RUIncrementMap{
			key: {TotalRU: 3, ExecCount: 1, ExecDuration: 1000},
		})
		tsr.ruAggregator.addBatchToBucket(sampleTs, stmtstats.RUIncrementMap{
			key: {TotalRU: 4, ExecCount: 2, ExecDuration: 2000},
		})
	case "G10_internal_sql_empty_user_handling":
		// Empty user is valid and should not be rewritten to "<others>".
		sqlDigest := []byte("S_G10")
		planDigest := []byte("P_G10")
		tsr.RegisterSQL(sqlDigest, fmt.Sprintf("/* %s */ select 10", marker), false)
		tsr.RegisterPlan(planDigest, fmt.Sprintf("plan_%s_10", marker), false)
		tsr.ruAggregator.addBatchToBucket(sampleTs, stmtstats.RUIncrementMap{
			{
				User:       "",
				SQLDigest:  stmtstats.BinaryDigest(sqlDigest),
				PlanDigest: stmtstats.BinaryDigest(planDigest),
			}: {TotalRU: 10, ExecCount: 1, ExecDuration: 1000},
		})
	case "G11_short_exec_time_lt_1s_handling":
		// Sub-second duration is kept in nanos and reported as-is.
		sqlDigest := []byte("S_G11")
		planDigest := []byte("P_G11")
		tsr.RegisterSQL(sqlDigest, fmt.Sprintf("/* %s */ select 11", marker), false)
		tsr.RegisterPlan(planDigest, fmt.Sprintf("plan_%s_11", marker), false)
		tsr.ruAggregator.addBatchToBucket(sampleTs, stmtstats.RUIncrementMap{
			{
				User:       "root",
				SQLDigest:  stmtstats.BinaryDigest(sqlDigest),
				PlanDigest: stmtstats.BinaryDigest(planDigest),
			}: {TotalRU: 11, ExecCount: 1, ExecDuration: uint64((500 * time.Millisecond).Nanoseconds())},
		})
	default:
		batch := make(stmtstats.RUIncrementMap, recordCount)
		for i := 0; i < recordCount; i++ {
			sqlDigest := []byte(fmt.Sprintf("S_%s_%d", cs.GoalID, i))
			planDigest := []byte(fmt.Sprintf("P_%s_%d", cs.GoalID, i))

			tsr.RegisterSQL(sqlDigest, fmt.Sprintf("/* %s */ select %d", marker, i), false)
			tsr.RegisterPlan(planDigest, fmt.Sprintf("plan_%s_%d", marker, i), false)

			batch[stmtstats.RUKey{
				User:       "root",
				SQLDigest:  stmtstats.BinaryDigest(sqlDigest),
				PlanDigest: stmtstats.BinaryDigest(planDigest),
			}] = &stmtstats.RUIncrement{
				TotalRU:      totalRUBaseline + float64(i+1),
				ExecCount:    execCounts[i],
				ExecDuration: uint64(1000 + i*100),
			}
		}
		tsr.ruAggregator.addBatchToBucket(sampleTs, batch)
	}

	// Emit exactly one aligned closed [start,start+60) window.
	reportTs := alignToInterval(sampleTs, ruReportWindowSeconds) + ruReportWindowSeconds
	tsr.doReport(&ReportData{
		RURecords: tsr.ruAggregator.takeReportRecords(reportTs, 60, []byte("topru-gen-keyspace")),
		SQLMetas:  tsr.normalizedSQLMap.take().toProto([]byte("topru-gen-keyspace")),
		PlanMetas: tsr.normalizedPlanMap.take().toProto(
			[]byte("topru-gen-keyspace"), tsr.decodePlan, tsr.compressPlan,
		),
	})

	select {
	case payload := <-sink.ch:
		if cs.RequireSend {
			require.NotNil(t, payload)
		}
		if cs.RURecordsMin > 0 {
			require.GreaterOrEqual(t, len(payload.RURecords), cs.RURecordsMin)
		}

		var maxExecCount uint64
		var sumExecCount uint64
		var maxTotalRU float64
		for _, rec := range payload.RURecords {
			for _, item := range rec.Items {
				if item.ExecCount > maxExecCount {
					maxExecCount = item.ExecCount
				}
				sumExecCount += item.ExecCount
				if item.TotalRu > maxTotalRU {
					maxTotalRU = item.TotalRu
				}
			}
		}
		if cs.ExecCountMin > 0 {
			require.GreaterOrEqual(t, maxExecCount, cs.ExecCountMin)
		}
		if cs.ExecCountSumMin > 0 {
			require.GreaterOrEqual(t, sumExecCount, cs.ExecCountSumMin)
		}
		if cs.TotalRUMin > 0 {
			require.GreaterOrEqual(t, maxTotalRU, cs.TotalRUMin)
		}
		if cs.SQLMetaMatchMarker != "" {
			matched := false
			for _, meta := range payload.SQLMetas {
				if strings.Contains(meta.NormalizedSql, cs.SQLMetaMatchMarker) {
					matched = true
					break
				}
			}
			require.True(t, matched, "missing SQLMeta marker: %s", cs.SQLMetaMatchMarker)
		}
		if cs.PlanMetaRequired != nil && *cs.PlanMetaRequired {
			require.NotEmpty(t, payload.PlanMetas)
		}
		assertTopRUCasePayload(t, cs.GoalID, payload)
	case <-time.After(3 * time.Second):
		if cs.RequireSend {
			t.Fatalf("timeout waiting for payload for goal %s", cs.GoalID)
		}
	}
}

func assertTopRUCasePayload(t *testing.T, goalID string, payload *ReportData) {
	t.Helper()
	switch goalID {
	case "G7_key_aggregation_by_user_sql_plan":
		users := map[string]struct{}{}
		for _, rec := range payload.RURecords {
			if string(rec.SqlDigest) == "S_G7" && string(rec.PlanDigest) == "P_G7" {
				users[rec.User] = struct{}{}
			}
		}
		require.Len(t, users, 2)
		_, ok := users["u1"]
		require.True(t, ok)
		_, ok = users["u2"]
		require.True(t, ok)
	case "G8_same_timestamp_multiple_finish_accumulate":
		rec := findRURecordByDigest(payload.RURecords, "root", "S_G8", "P_G8")
		require.NotNil(t, rec)
		require.Len(t, rec.Items, 1)
		require.InDelta(t, 7.0, rec.Items[0].TotalRu, 1e-9)
		require.Equal(t, uint64(3), rec.Items[0].ExecCount)
		require.Equal(t, uint64(3000), rec.Items[0].ExecDuration)
	case "G10_internal_sql_empty_user_handling":
		rec := findRURecordByDigest(payload.RURecords, "", "S_G10", "P_G10")
		require.NotNil(t, rec)
		require.NotEmpty(t, rec.Items)
	case "G11_short_exec_time_lt_1s_handling":
		rec := findRURecordByDigest(payload.RURecords, "root", "S_G11", "P_G11")
		require.NotNil(t, rec)
		require.NotEmpty(t, rec.Items)
		require.Equal(t, uint64((500 * time.Millisecond).Nanoseconds()), rec.Items[0].ExecDuration)
	}
}

func findRURecordByDigest(records []tipb.TopRURecord, user, sqlDigest, planDigest string) *tipb.TopRURecord {
	for i := range records {
		rec := &records[i]
		if rec.User == user && string(rec.SqlDigest) == sqlDigest && string(rec.PlanDigest) == planDigest {
			return rec
		}
	}
	return nil
}
