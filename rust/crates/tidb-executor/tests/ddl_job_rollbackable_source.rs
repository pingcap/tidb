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

//! GO PORT of `pkg/ddl/executor_test.go:128 TestIsJobRollbackable`
//! (`pkg/ddl.part6` batch b105).
//!
//! The Go test drives `Job.IsRollbackable()` (pkg/meta/model/job.go:864)
//! through four action/state rows straight out of a live DDL worker's
//! lifecycle: a DROP INDEX has not destroyed anything while the job is only
//! queued (StateNone), so cancelling it is fine; the same action is not
//! rollbackable once it reached StateDeleteOnly, and dropping a schema or a
//! column is equally past the point of no return at its first destructive
//! state. The transcreated predicate lives at
//! `tidb_model::Job::is_rollbackable` (crates/tidb-model/src/job.rs:1270),
//! so the four rows run against the model type directly.

use tidb_model::action_type::ActionType;
use tidb_model::job::Job;
use tidb_model::schema_state::SchemaState;

/// GO PORT of `pkg/ddl/executor_test.go:128 TestIsJobRollbackable`.
///
/// Each row is one `job.Type`/`job.SchemaState` pair with the verdict Go's
/// `require.Equal(t, ca.result, job.IsRollbackable())` pins for it:
///
/// * `ActionDropIndex` at `StateNone` -> rollbackable. Nothing has been
///   written yet (job.go:870's `!IsStateDeleteOnly-like` arm);
/// * `ActionDropIndex` at `StateDeleteOnly` -> NOT rollbackable: indexes are
///   already being removed (job.go:870-874);
/// * `ActionDropSchema` at `StateDeleteOnly` -> NOT rollbackable
///   (job.go:875-879: the whole schema's objects are being dropped);
/// * `ActionDropColumn` at `StateDeleteOnly` -> NOT rollbackable
///   (job.go:880-884, same family as DropSchema).
#[test]
fn job_rollbackable_matches_the_four_source_rows() {
    let cases = [
        (ActionType::ACTION_DROP_INDEX, SchemaState::NONE, true),
        (
            ActionType::ACTION_DROP_INDEX,
            SchemaState::DELETE_ONLY,
            false,
        ),
        (
            ActionType::ACTION_DROP_SCHEMA,
            SchemaState::DELETE_ONLY,
            false,
        ),
        (
            ActionType::ACTION_DROP_COLUMN,
            SchemaState::DELETE_ONLY,
            false,
        ),
    ];
    for (action, state, expected) in cases {
        // `mu`/`args` are private to the crate, so the job is built from
        // `Default` and the two public fields are set in place.
        let mut job = Job::default();
        job.type_ = action;
        job.schema_state = state;
        assert_eq!(
            job.is_rollbackable(),
            expected,
            "action {action:?} at state {state:?}"
        );
    }
}
