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

import "testing"

// topRUGeneratedCaseSpecs defines all TopRU generated test cases.
// Each case validates a specific semantic contract of the TopRU reporting pipeline.
var topRUGeneratedCaseSpecs = []caseSpec{
	// sqlmeta_present: payload includes SQLMetas for the triggered marker
	{
		GoalID:             "sqlmeta_present",
		Level:              "should",
		Description:        "payload includes SQLMetas for the triggered marker",
		RequireSend:        false,
		RURecordsMin:       0,
		ExecCountMin:       0,
		ExecCountSumMin:    0,
		TotalRUMin:         0.0,
		SQLMetaMatchMarker: "topru_gen_sqlmeta",
		PlanMetaRequired:   nil,
	},
	// planmeta_present: payload includes PlanMetas for the triggered marker
	{
		GoalID:             "planmeta_present",
		Level:              "should",
		Description:        "payload includes PlanMetas for the triggered marker",
		RequireSend:        false,
		RURecordsMin:       0,
		ExecCountMin:       0,
		ExecCountSumMin:    0,
		TotalRUMin:         0.0,
		SQLMetaMatchMarker: "",
		PlanMetaRequired:   boolPtr(true),
	},
	// multi_records_batch: payload batches multiple RURecords and preserves counts
	{
		GoalID:             "multi_records_batch",
		Level:              "should",
		Description:        "payload batches multiple RURecords and preserves counts",
		RequireSend:        false,
		RURecordsMin:       2,
		ExecCountMin:       0,
		ExecCountSumMin:    2,
		TotalRUMin:         0.0,
		SQLMetaMatchMarker: "",
		PlanMetaRequired:   nil,
	},
	// total_ru_threshold: payload contains a record with RU above a threshold
	{
		GoalID:             "total_ru_threshold",
		Level:              "should",
		Description:        "payload contains a record with RU above a threshold",
		RequireSend:        false,
		RURecordsMin:       0,
		ExecCountMin:       0,
		ExecCountSumMin:    0,
		TotalRUMin:         1.5,
		SQLMetaMatchMarker: "",
		PlanMetaRequired:   nil,
	},
	// key_aggregation_by_user_sql_plan: aggregation key is (user, sql_digest, plan_digest)
	{
		GoalID:             "key_aggregation_by_user_sql_plan",
		Level:              "must",
		Description:        "aggregation key is (user, sql_digest, plan_digest); different users same SQL are separated",
		RequireSend:        true,
		RURecordsMin:       0,
		ExecCountMin:       0,
		ExecCountSumMin:    0,
		TotalRUMin:         0.0,
		SQLMetaMatchMarker: "",
		PlanMetaRequired:   nil,
	},
	// same_timestamp_multiple_finish_accumulate: multiple finishes for same key accumulate
	{
		GoalID:             "same_timestamp_multiple_finish_accumulate",
		Level:              "should",
		Description:        "within same timestamp, multiple finishes for same key accumulate ruIncrement into TopN",
		RequireSend:        true,
		RURecordsMin:       1,
		ExecCountMin:       0,
		ExecCountSumMin:    2,
		TotalRUMin:         0.0,
		SQLMetaMatchMarker: "",
		PlanMetaRequired:   nil,
	},
	// internal_sql_empty_user_handling: empty user (internal SQL) is handled deterministically
	{
		GoalID:             "internal_sql_empty_user_handling",
		Level:              "should",
		Description:        "empty user (internal SQL) is handled deterministically (no panic, stable key)",
		RequireSend:        true,
		RURecordsMin:       0,
		ExecCountMin:       0,
		ExecCountSumMin:    0,
		TotalRUMin:         0.0,
		SQLMetaMatchMarker: "",
		PlanMetaRequired:   nil,
	},
	// short_exec_time_lt_1s_handling: exec_duration < 1s still produces correct RU record
	{
		GoalID:             "short_exec_time_lt_1s_handling",
		Level:              "should",
		Description:        "exec_duration < 1s still produces correct RU record (or explicitly skipped by design)",
		RequireSend:        true,
		RURecordsMin:       0,
		ExecCountMin:       0,
		ExecCountSumMin:    0,
		TotalRUMin:         0.0,
		SQLMetaMatchMarker: "",
		PlanMetaRequired:   nil,
	},
}

// TestTopRUGeneratedCases runs all TopRU generated test cases using table-driven subtests.
// Each subtest validates a specific semantic contract of the TopRU reporting pipeline.
func TestTopRUGeneratedCases(t *testing.T) {
	for _, cs := range topRUGeneratedCaseSpecs {
		t.Run(cs.GoalID, func(t *testing.T) {
			runTopRUCase(t, cs)
		})
	}
}
