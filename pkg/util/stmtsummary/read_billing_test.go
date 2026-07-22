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
		ModelVersion:  "v3",
		WeightVersion: "v1",
	}
	for i := 0; i < MaxReadBillingDemoBaseUnitKeysPerRecord+2; i++ {
		stats.BaseUnits = append(stats.BaseUnits, ReadBillingDemoBaseUnitSample{
			ModelVersion:   "v3",
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

	v4Stats := ReadBillingDemoStatementStats{
		ModelVersion:  "v4",
		WeightVersion: "v3-resource-formula-uncalibrated",
		BaseUnits: []ReadBillingDemoBaseUnitSample{{
			ModelVersion:  "v4",
			WeightVersion: "v3-resource-formula-uncalibrated",
			Site:          "tidb",
			OpClass:       "projection_eval",
			OperatorKind:  "projection",
			Unit:          "input_rows",
			InputSource:   "runtime_child_act_rows",
			InputSide:     "all",
			Value:         10,
		}},
	}
	_, _, v4Summary := AddReadBillingDemoStatementStatsToMaps(nil, nil, &v4Stats)
	require.Zero(t, v4Summary.SumReadBillingDemoFixedEvents)
	require.Zero(t, v4Summary.SumReadBillingDemoInputRows)
	require.Zero(t, v4Summary.SumReadBillingDemoInputBytes)

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
	stats := ReadBillingDemoStatementStats{ModelVersion: "v4", WeightVersion: "test-v4-calibrated"}
	for _, dmlKind := range []string{"insert", "update"} {
		for _, unit := range []string{"cpu_work", "encoded_mutation_count"} {
			stats.BaseUnits = append(stats.BaseUnits, ReadBillingDemoBaseUnitSample{
				ModelVersion:   "v4",
				WeightVersion:  "test-v4-calibrated",
				Site:           "tidb",
				OpClass:        "kv_mutation",
				OperatorKind:   "memdb_mutation",
				DMLKind:        dmlKind,
				Unit:           unit,
				InputSource:    "stmt_memdb_mutation_calls",
				InputSide:      "all",
				RowWidthSource: "not_applicable",
				Value:          1,
			})
		}
	}
	stats.BaseUnits = append(stats.BaseUnits,
		ReadBillingDemoBaseUnitSample{
			ModelVersion: "v4", WeightVersion: "test-v4-calibrated", Site: "tidb", OpClass: "reader_transport", OperatorKind: "mixed_reader",
			Unit: "read_request_count", InputSource: "ruv2_metrics", InputSide: "all", RowWidthSource: "not_applicable", Value: 4,
		},
		ReadBillingDemoBaseUnitSample{
			ModelVersion: "v4", WeightVersion: "test-v4-calibrated", Site: "tikv", OpClass: "kv_write", OperatorKind: "txn_write", DMLKind: "update",
			Unit: "write_request_count", InputSource: "ruv2_metrics", InputSide: "all", RowWidthSource: "not_applicable", Value: 2,
		},
	)

	aggs, _, _ := AddReadBillingDemoStatementStatsToMaps(nil, nil, &stats)
	require.Len(t, aggs, 6)
	entries := ReadBillingDemoBaseUnitEntriesFromMap(aggs)
	seenUnits := make(map[string]int)
	for _, entry := range entries {
		require.Equal(t, "all", entry.InputSide)
		switch entry.Unit {
		case "cpu_work", "encoded_mutation_count":
			require.Equal(t, "kv_mutation", entry.OpClass)
			require.Equal(t, "memdb_mutation", entry.OperatorKind)
			require.Equal(t, "stmt_memdb_mutation_calls", entry.InputSource)
			require.Contains(t, []string{"insert", "update"}, entry.DMLKind)
		case "read_request_count":
			require.Equal(t, "reader_transport", entry.OpClass)
			require.Equal(t, "mixed_reader", entry.OperatorKind)
			require.Equal(t, "ruv2_metrics", entry.InputSource)
		case "write_request_count":
			require.Equal(t, "kv_write", entry.OpClass)
			require.Equal(t, "txn_write", entry.OperatorKind)
			require.Equal(t, "ruv2_metrics", entry.InputSource)
		default:
			require.Failf(t, "unexpected unit", "entry=%+v", entry)
		}
		seenUnits[entry.Unit]++
	}
	require.Equal(t, map[string]int{"cpu_work": 2, "encoded_mutation_count": 2, "read_request_count": 1, "write_request_count": 1}, seenUnits)
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
