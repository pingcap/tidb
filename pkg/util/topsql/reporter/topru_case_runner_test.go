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

	const sampleTs = uint64(1700000000)
	tsr.ruAggregator.addBatchToBucket(sampleTs, batch)
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
	case <-time.After(3 * time.Second):
		if cs.RequireSend {
			t.Fatalf("timeout waiting for payload for goal %s", cs.GoalID)
		}
	}
}
