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

package stmtsummary

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadBillingDemoAggregationCaps(t *testing.T) {
	stats := ReadBillingDemoStatementStats{
		ModelVersion:  "v1",
		WeightVersion: "v1",
	}
	for i := 0; i < MaxReadBillingDemoBaseUnitKeysPerRecord+2; i++ {
		stats.BaseUnits = append(stats.BaseUnits, ReadBillingDemoBaseUnitSample{
			ModelVersion:   "v1",
			WeightVersion:  "v1",
			Site:           "tidb",
			OpClass:        fmt.Sprintf("op_%03d", i),
			OperatorKind:   "projection",
			Unit:           "input_rows",
			InputSource:    "runtime_act_rows",
			InputSide:      "all",
			RowWidthSource: "operator_helper",
			Value:          float64(i + 1),
			RowWidth:       8,
		})
	}

	baseAggs, statusAggs, acceptedSummary := AddReadBillingDemoStatementStatsToMaps(nil, nil, &stats)
	require.Len(t, baseAggs, MaxReadBillingDemoBaseUnitKeysPerRecord)
	require.Equal(t, uint64(2), requireReadBillingDemoStatusReason(t, ReadBillingDemoStatusEntriesFromMap(statusAggs), readBillingDemoReasonAggregation).Count)
	require.Equal(t, float64(MaxReadBillingDemoBaseUnitKeysPerRecord*(MaxReadBillingDemoBaseUnitKeysPerRecord+1)/2), acceptedSummary.SumReadBillingDemoInputRows)

	baseEntries, statusEntries, acceptedSummary := AddReadBillingDemoStatementStatsToEntries(nil, nil, &stats)
	require.Len(t, baseEntries, MaxReadBillingDemoBaseUnitKeysPerRecord)
	require.Equal(t, uint64(2), requireReadBillingDemoStatusReason(t, statusEntries, readBillingDemoReasonAggregation).Count)
	require.Equal(t, float64(MaxReadBillingDemoBaseUnitKeysPerRecord*(MaxReadBillingDemoBaseUnitKeysPerRecord+1)/2), acceptedSummary.SumReadBillingDemoInputRows)

	statusOnly := ReadBillingDemoStatementStats{
		ModelVersion:  "v1",
		WeightVersion: "v1",
	}
	for i := 0; i < MaxReadBillingDemoStatusKeysPerRecord+2; i++ {
		statusOnly.Statuses = append(statusOnly.Statuses, ReadBillingDemoStatusSample{
			ModelVersion:  "v1",
			WeightVersion: "v1",
			Site:          "tidb",
			OpClass:       fmt.Sprintf("op_%03d", i),
			OperatorKind:  "projection",
			Status:        "unsupported",
			Reason:        "unsupported_operator",
		})
	}

	_, statusAggs, acceptedSummary = AddReadBillingDemoStatementStatsToMaps(nil, nil, &statusOnly)
	require.Equal(t, MaxReadBillingDemoStatusKeysPerRecord, readBillingDemoNonReservedStatusKeyCount(statusAggs))
	require.Equal(t, uint64(2), requireReadBillingDemoStatusReason(t, ReadBillingDemoStatusEntriesFromMap(statusAggs), readBillingDemoReasonStatusAggregation).Count)
	require.Zero(t, acceptedSummary.SumReadBillingDemoFixedEvents)
	require.Zero(t, acceptedSummary.SumReadBillingDemoInputRows)
	require.Zero(t, acceptedSummary.SumReadBillingDemoInputBytes)

	_, statusEntries, acceptedSummary = AddReadBillingDemoStatementStatsToEntries(nil, nil, &statusOnly)
	require.Equal(t, MaxReadBillingDemoStatusKeysPerRecord, readBillingDemoNonReservedStatusEntryCount(statusEntries))
	require.Equal(t, uint64(2), requireReadBillingDemoStatusReason(t, statusEntries, readBillingDemoReasonStatusAggregation).Count)
	require.Zero(t, acceptedSummary.SumReadBillingDemoFixedEvents)
	require.Zero(t, acceptedSummary.SumReadBillingDemoInputRows)
	require.Zero(t, acceptedSummary.SumReadBillingDemoInputBytes)
}

func TestReadBillingDemoDMLKindAggregation(t *testing.T) {
	stats := ReadBillingDemoStatementStats{ModelVersion: "v2", WeightVersion: "v1"}
	for _, dmlKind := range []string{"insert", "update"} {
		stats.BaseUnits = append(stats.BaseUnits, ReadBillingDemoBaseUnitSample{
			ModelVersion:   "v2",
			WeightVersion:  "v1",
			Site:           "tidb",
			OpClass:        "kv_mutation",
			OperatorKind:   "memdb_mutation",
			DMLKind:        dmlKind,
			Unit:           "encoded_mutation_count",
			InputSource:    "stmt_memdb_mutation_calls",
			InputSide:      "all",
			RowWidthSource: "not_applicable",
			Value:          1,
		})
	}

	aggs, _, _ := AddReadBillingDemoStatementStatsToMaps(nil, nil, &stats)
	require.Len(t, aggs, 2)
	entries := ReadBillingDemoBaseUnitEntriesFromMap(aggs)
	require.Equal(t, "insert", entries[0].DMLKind)
	require.Equal(t, "update", entries[1].DMLKind)
	for _, entry := range entries {
		require.Equal(t, "kv_mutation", entry.OpClass)
		require.Equal(t, "memdb_mutation", entry.OperatorKind)
		require.Equal(t, "encoded_mutation_count", entry.Unit)
	}
}

func TestReadBillingDemoReservedStatusMergeBypassesStatusCap(t *testing.T) {
	fullStatusAggs := make(map[ReadBillingDemoStatusKey]ReadBillingDemoStatusAgg)
	fullStatusEntries := make([]ReadBillingDemoStatusAggEntry, 0, MaxReadBillingDemoStatusKeysPerRecord)
	for i := 0; i < MaxReadBillingDemoStatusKeysPerRecord; i++ {
		key := ReadBillingDemoStatusKey{
			ModelVersion:  "v1",
			WeightVersion: "v1",
			Site:          "tidb",
			OpClass:       fmt.Sprintf("op_%03d", i),
			OperatorKind:  "projection",
			Status:        "unsupported",
			Reason:        "unsupported_operator",
		}
		fullStatusAggs[key] = ReadBillingDemoStatusAgg{Count: 1}
		fullStatusEntries = append(fullStatusEntries, readBillingDemoStatusEntry(key, ReadBillingDemoStatusAgg{Count: 1}))
	}

	srcStatusAggs := map[ReadBillingDemoStatusKey]ReadBillingDemoStatusAgg{
		makeReadBillingDemoStatusKey(readBillingDemoReservedStatusSample("v1", "v1", readBillingDemoReasonAggregation)): {
			Count: 7,
		},
		makeReadBillingDemoStatusKey(readBillingDemoReservedStatusSample("v1", "v1", readBillingDemoReasonStatusAggregation)): {
			Count: 11,
		},
	}
	_, mergedStatusAggs, _ := MergeReadBillingDemoAggMaps(nil, fullStatusAggs, nil, srcStatusAggs)
	require.Equal(t, MaxReadBillingDemoStatusKeysPerRecord, readBillingDemoNonReservedStatusKeyCount(mergedStatusAggs))
	require.Equal(t, uint64(7), requireReadBillingDemoStatusReason(t, ReadBillingDemoStatusEntriesFromMap(mergedStatusAggs), readBillingDemoReasonAggregation).Count)
	require.Equal(t, uint64(11), requireReadBillingDemoStatusReason(t, ReadBillingDemoStatusEntriesFromMap(mergedStatusAggs), readBillingDemoReasonStatusAggregation).Count)

	srcStatusEntries := []ReadBillingDemoStatusAggEntry{
		readBillingDemoStatusEntry(
			makeReadBillingDemoStatusKey(readBillingDemoReservedStatusSample("v1", "v1", readBillingDemoReasonAggregation)),
			ReadBillingDemoStatusAgg{Count: 7},
		),
		readBillingDemoStatusEntry(
			makeReadBillingDemoStatusKey(readBillingDemoReservedStatusSample("v1", "v1", readBillingDemoReasonStatusAggregation)),
			ReadBillingDemoStatusAgg{Count: 11},
		),
	}
	_, mergedStatusEntries, _ := MergeReadBillingDemoEntrySlices(nil, fullStatusEntries, nil, srcStatusEntries)
	require.Equal(t, MaxReadBillingDemoStatusKeysPerRecord, readBillingDemoNonReservedStatusEntryCount(mergedStatusEntries))
	require.Equal(t, uint64(7), requireReadBillingDemoStatusReason(t, mergedStatusEntries, readBillingDemoReasonAggregation).Count)
	require.Equal(t, uint64(11), requireReadBillingDemoStatusReason(t, mergedStatusEntries, readBillingDemoReasonStatusAggregation).Count)
}

func requireReadBillingDemoStatusReason(t *testing.T, entries []ReadBillingDemoStatusAggEntry, reason string) ReadBillingDemoStatusAggEntry {
	t.Helper()
	for _, entry := range entries {
		if entry.Reason == reason {
			return entry
		}
	}
	require.Failf(t, "missing read billing status reason", "reason=%s entries=%v", reason, entries)
	return ReadBillingDemoStatusAggEntry{}
}
