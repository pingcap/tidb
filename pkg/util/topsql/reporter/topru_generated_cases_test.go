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

// TestTopRUSqlmetaPresent verifies a TopRU payload keeps SQL meta for the
// target digest marker, even when RURecords are not required by this case.
func TestTopRUSqlmetaPresent(t *testing.T) {
	runTopRUCase(t, caseSpec{
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
	})
}

// TestTopRUPlanmetaPresent verifies a TopRU payload keeps PlanMeta for the
// target digest marker, independent from RURecords count.
func TestTopRUPlanmetaPresent(t *testing.T) {
	runTopRUCase(t, caseSpec{
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
	})
}

// TestTopRUMultiRecordsBatch verifies the report can carry multiple
// RURecords in one payload and preserves the expected exec-count sum.
func TestTopRUMultiRecordsBatch(t *testing.T) {
	runTopRUCase(t, caseSpec{
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
	})
}

// TestTopRUTotalRuThreshold verifies at least one record crosses the RU
// threshold, so filtering does not drop all heavy entries.
func TestTopRUTotalRuThreshold(t *testing.T) {
	runTopRUCase(t, caseSpec{
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
	})
}

// TestTopRUKey verifies the aggregation key and guards against regressions in begin-based RU accounting.
func TestTopRUKey(t *testing.T) {
	runTopRUCase(t, caseSpec{
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
	})
}

// TestTopRUSameTimestampMultipleFinishAccumulate verifies multiple finish
// events at one timestamp accumulate into the same TopRU key deterministically.
func TestTopRUSameTimestampMultipleFinishAccumulate(t *testing.T) {
	runTopRUCase(t, caseSpec{
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
	})
}

// TestTopRUInternalSqlEmptyUserHandling verifies empty-user internal SQL
// is keyed consistently and never panics during RU aggregation.
func TestTopRUInternalSqlEmptyUserHandling(t *testing.T) {
	runTopRUCase(t, caseSpec{
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
	})
}

// TestTopRUShort verifies short exec time handling and guards against regressions in begin-based RU accounting.
func TestTopRUShort(t *testing.T) {
	runTopRUCase(t, caseSpec{
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
	})
}
