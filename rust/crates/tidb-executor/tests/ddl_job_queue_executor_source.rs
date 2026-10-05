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

//! Behavioral tests retained from Go. Removed documentary entries are
//! indexed in rust/docs/parity/current-audit/comment-test-cleanup-validation.json.

use tidb_model::{ActionType, Job, SchemaState};

// --- TestIsJobRollbackable (pkg/ddl/executor_test.go:128) ---
//
// Go walks four (action, state) cases over a bare job and requires
// `job.IsRollbackable()` to agree with each: DROP INDEX is rollbackable at
// StateNone but not once the index half-exists (StateDeleteOnly);
// DROP SCHEMA and DROP COLUMN at StateDeleteOnly are already past their
// cancel point (they only revert at StatePublic, i.e. before the job starts
// writing).
#[test]
fn is_job_rollbackable_matches_the_go_matrix() {
    let cases = [
        (ActionType::ACTION_DROP_INDEX, SchemaState::NONE, true),
        (ActionType::ACTION_DROP_INDEX, SchemaState::DELETE_ONLY, false),
        (ActionType::ACTION_DROP_SCHEMA, SchemaState::DELETE_ONLY, false),
        (ActionType::ACTION_DROP_COLUMN, SchemaState::DELETE_ONLY, false),
    ];
    for (action_type, schema_state, expected) in cases {
        let mut job = Job::default();
        job.type_ = action_type;
        job.schema_state = schema_state;
        assert_eq!(
            job.is_rollbackable(),
            expected,
            "Go case {action_type:?}@{schema_state:?} (pkg/meta/model/job.go:864 IsRollbackable)"
        );
    }
}
