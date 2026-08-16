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

//! Go `step.go`: the step vocabulary of every task type and its rendering.

use serde::{Deserialize, Serialize};

use crate::task::TaskType;
use crate::task_type::{BACKFILL, IMPORT_INTO, TASK_TYPE_EXAMPLE};

/// Go `Step`: the step of a task.
///
/// Go declares it as a bare `int64` and steps are numbered per task type, so
/// arbitrary values must survive; this is a newtype rather than an enum for
/// that reason. `#[serde(transparent)]` keeps `ExtraParams.target_steps`
/// encoding as a JSON number array, as Go's does.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct Step(pub i64);

/// Go `StepInit`. DO NOT change the value, it would break backward
/// compatibility.
pub const STEP_INIT: Step = Step(-1);
/// Go `StepDone`. DO NOT change the value, it would break backward
/// compatibility.
pub const STEP_DONE: Step = Step(-2);
/// Go `StepPrepared`: framework prepare logic has finished while the task
/// state is still pending.
pub const STEP_PREPARED: Step = Step(-3);

/// Go `unknownStepPrefix`.
const UNKNOWN_STEP_PREFIX: &str = "unknown step";

/// Go `Step2Str`: converts a step to a string.
///
/// It is too bad that Go defines step as an int 🙃.
#[must_use]
pub fn step2str(t: &TaskType, s: Step) -> String {
    // StepInit, StepDone and StepPrepared are special steps, we don't check
    // task type for them.
    match s {
        STEP_INIT => return "init".to_owned(),
        STEP_DONE => return "done".to_owned(),
        STEP_PREPARED => return "prepared".to_owned(),
        _ => {}
    }
    if *t == BACKFILL {
        backfill_step2str(s)
    } else if *t == IMPORT_INTO {
        import_into_step2str(s)
    } else if *t == TASK_TYPE_EXAMPLE {
        example_step2str(s)
    } else {
        format!("unknown type {t}")
    }
}

/// Go `IsValidStep`: whether the step is valid for the task type.
#[must_use]
pub fn is_valid_step(t: &TaskType, s: Step) -> bool {
    !step2str(t, s).contains(UNKNOWN_STEP_PREFIX)
}

/// Go `IsValidBusinessStep`: whether the step is a business step valid for the
/// task type. Framework marker steps are excluded.
#[must_use]
pub fn is_valid_business_step(t: &TaskType, s: Step) -> bool {
    if s == STEP_INIT || s == STEP_DONE || s == STEP_PREPARED {
        return false;
    }
    is_valid_step(t, s)
}

/// Go `StepOne`: a step of the example task type.
pub const STEP_ONE: Step = Step(1);
/// Go `StepTwo`: a step of the example task type.
pub const STEP_TWO: Step = Step(2);
/// Go `StepThree`: a step of the example task type.
pub const STEP_THREE: Step = Step(3);

fn example_step2str(s: Step) -> String {
    match s {
        STEP_ONE => "one".to_owned(),
        STEP_TWO => "two".to_owned(),
        STEP_THREE => "three".to_owned(),
        _ => unknown_step_str(s),
    }
}

// Steps of IMPORT INTO, each step is represented by one or multiple subtasks.
// The initial step is StepInit(-1); steps are processed in this order:
//
//   - local sort:
//     StepInit -> ImportStepImport -> ImportStepPostProcess -> StepDone
//   - global sort:
//     StepInit -> ImportStepEncodeAndSort -> ImportStepMergeSort (optional)
//     -> ImportStepWriteAndIngest -> ImportStepCollectConflicts (optional)
//     -> ImportStepConflictResolution (optional) -> ImportStepPostProcess
//     -> StepDone

/// Go `ImportStepImport`: sort source data and ingest it into TiKV.
pub const IMPORT_STEP_IMPORT: Step = Step(1);
/// Go `ImportStepPostProcess`: verify checksum and add index.
pub const IMPORT_STEP_POST_PROCESS: Step = Step(2);
/// Go `ImportStepEncodeAndSort`: encode source data and write sorted kv into
/// global storage.
pub const IMPORT_STEP_ENCODE_AND_SORT: Step = Step(3);
/// Go `ImportStepMergeSort`: merge sorted kv from global storage so that
/// [`IMPORT_STEP_WRITE_AND_INGEST`] reads faster. Depending on how much the kv
/// files overlap, this step may have 0 subtasks.
pub const IMPORT_STEP_MERGE_SORT: Step = Step(4);
/// Go `ImportStepWriteAndIngest`: write sorted kv into TiKV and ingest it.
pub const IMPORT_STEP_WRITE_AND_INGEST: Step = Step(5);
/// Go `ImportStepCollectConflicts`: collect conflict info.
///
/// This step does not mutate downstream data, so it is idempotent and can
/// collect a correct checksum for the conflicted rows; doing it together with
/// [`IMPORT_STEP_CONFLICT_RESOLUTION`] would lose that on a mid-step retry. It
/// also deduplicates conflicted rows arising from multiple unique indexes, in
/// memory, so with too many conflicts the later checksum step is skipped.
pub const IMPORT_STEP_COLLECT_CONFLICTS: Step = Step(6);
/// Go `ImportStepConflictResolution`: resolve detected conflicts.
///
/// Other global-sort steps record detected conflicts in external storage; this
/// step resolves them, and so may have 0 subtasks.
pub const IMPORT_STEP_CONFLICT_RESOLUTION: Step = Step(7);

fn import_into_step2str(s: Step) -> String {
    match s {
        IMPORT_STEP_IMPORT => "import".to_owned(),
        IMPORT_STEP_POST_PROCESS => "post-process".to_owned(),
        IMPORT_STEP_ENCODE_AND_SORT => "encode".to_owned(),
        IMPORT_STEP_MERGE_SORT => "merge-sort".to_owned(),
        IMPORT_STEP_WRITE_AND_INGEST => "ingest".to_owned(),
        IMPORT_STEP_COLLECT_CONFLICTS => "collect-conflicts".to_owned(),
        IMPORT_STEP_CONFLICT_RESOLUTION => "conflict-resolution".to_owned(),
        _ => unknown_step_str(s),
    }
}

// Steps of Add Index, each step is represented by one or multiple subtasks.
// The initial step is StepInit(-1); steps are processed in this order:
// - local sort:
//   StepInit -> BackfillStepReadIndex -> StepDone
// - global sort:
//   StepInit -> BackfillStepReadIndex -> BackfillStepMergeSort
//   -> BackfillStepWriteAndIngest -> StepDone

/// Go `BackfillStepReadIndex`.
pub const BACKFILL_STEP_READ_INDEX: Step = Step(1);
/// Go `BackfillStepMergeSort`, used only in global sort: merge sorted kv from
/// global storage so that [`BACKFILL_STEP_WRITE_AND_INGEST`] reads faster.
/// When kv files overlap less than the merge-sort overlap threshold, this step
/// has no subtasks.
pub const BACKFILL_STEP_MERGE_SORT: Step = Step(2);
/// Go `BackfillStepWriteAndIngest`: write sorted kv into TiKV and ingest it.
pub const BACKFILL_STEP_WRITE_AND_INGEST: Step = Step(3);
/// Go `BackfillStepMergeTempIndex`: merge the temp index into the original
/// index.
pub const BACKFILL_STEP_MERGE_TEMP_INDEX: Step = Step(4);

fn backfill_step2str(s: Step) -> String {
    match s {
        BACKFILL_STEP_READ_INDEX => "read-index".to_owned(),
        BACKFILL_STEP_MERGE_SORT => "merge-sort".to_owned(),
        BACKFILL_STEP_WRITE_AND_INGEST => "ingest".to_owned(),
        BACKFILL_STEP_MERGE_TEMP_INDEX => "merge-temp-index".to_owned(),
        _ => unknown_step_str(s),
    }
}

fn unknown_step_str(s: Step) -> String {
    format!("{UNKNOWN_STEP_PREFIX} {}", s.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go `TestStep`.
    #[test]
    fn test_step() {
        // backfill
        assert_eq!(step2str(&BACKFILL, STEP_INIT), "init");
        assert_eq!(step2str(&BACKFILL, BACKFILL_STEP_READ_INDEX), "read-index");
        assert_eq!(step2str(&BACKFILL, BACKFILL_STEP_MERGE_SORT), "merge-sort");
        assert_eq!(
            step2str(&BACKFILL, BACKFILL_STEP_WRITE_AND_INGEST),
            "ingest"
        );
        assert_eq!(step2str(&BACKFILL, STEP_PREPARED), "prepared");
        assert_eq!(step2str(&BACKFILL, STEP_DONE), "done");
        assert_eq!(step2str(&BACKFILL, Step(111)), "unknown step 111");

        // import into
        assert_eq!(step2str(&IMPORT_INTO, STEP_INIT), "init");
        assert_eq!(step2str(&IMPORT_INTO, IMPORT_STEP_IMPORT), "import");
        assert_eq!(
            step2str(&IMPORT_INTO, IMPORT_STEP_POST_PROCESS),
            "post-process"
        );
        assert_eq!(step2str(&IMPORT_INTO, IMPORT_STEP_MERGE_SORT), "merge-sort");
        assert_eq!(
            step2str(&IMPORT_INTO, IMPORT_STEP_ENCODE_AND_SORT),
            "encode"
        );
        assert_eq!(
            step2str(&IMPORT_INTO, IMPORT_STEP_WRITE_AND_INGEST),
            "ingest"
        );
        assert_eq!(
            step2str(&IMPORT_INTO, IMPORT_STEP_COLLECT_CONFLICTS),
            "collect-conflicts"
        );
        assert_eq!(
            step2str(&IMPORT_INTO, IMPORT_STEP_CONFLICT_RESOLUTION),
            "conflict-resolution"
        );
        assert_eq!(step2str(&IMPORT_INTO, STEP_PREPARED), "prepared");
        assert_eq!(step2str(&IMPORT_INTO, STEP_DONE), "done");
        assert_eq!(step2str(&IMPORT_INTO, Step(123)), "unknown step 123");

        // example type
        assert_eq!(step2str(&TASK_TYPE_EXAMPLE, STEP_INIT), "init");
        assert_eq!(step2str(&TASK_TYPE_EXAMPLE, STEP_ONE), "one");
        assert_eq!(step2str(&TASK_TYPE_EXAMPLE, STEP_TWO), "two");
        assert_eq!(step2str(&TASK_TYPE_EXAMPLE, STEP_PREPARED), "prepared");
        assert_eq!(step2str(&TASK_TYPE_EXAMPLE, STEP_DONE), "done");
        assert_eq!(step2str(&TASK_TYPE_EXAMPLE, Step(333)), "unknown step 333");

        // unknown type
        assert_eq!(
            step2str(&TaskType::new("123"), Step(123)),
            "unknown type 123"
        );
    }

    /// Go `TestIsValidStep`.
    #[test]
    fn test_is_valid_step() {
        assert!(is_valid_step(&BACKFILL, BACKFILL_STEP_READ_INDEX));
        assert!(!is_valid_step(&BACKFILL, Step(123)));
        assert!(is_valid_step(&BACKFILL, STEP_PREPARED));
        assert!(is_valid_step(&IMPORT_INTO, IMPORT_STEP_WRITE_AND_INGEST));
        assert!(!is_valid_step(&IMPORT_INTO, Step(456)));

        assert!(is_valid_business_step(&BACKFILL, BACKFILL_STEP_READ_INDEX));
        assert!(!is_valid_business_step(&BACKFILL, STEP_INIT));
        assert!(!is_valid_business_step(&BACKFILL, STEP_DONE));
        assert!(!is_valid_business_step(&BACKFILL, STEP_PREPARED));
        assert!(!is_valid_business_step(&BACKFILL, Step(123)));
    }
}
