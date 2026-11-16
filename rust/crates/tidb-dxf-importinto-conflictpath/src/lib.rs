// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/dxf/importinto/conflictpath`: the object-store path layout of
//! IMPORT INTO conflict rows.

/// Go `storageDir`: the top-level directory under the global-sort URI where
/// IMPORT INTO stores conflict rows.
const STORAGE_DIR: &str = "conflicted-rows";

/// Go `StoragePrefix`: [`STORAGE_DIR`] with a trailing slash, matching the
/// object keys `WalkDir` returns.
pub const STORAGE_PREFIX: &str = "conflicted-rows/";

/// Go `NewFileNamePrefix`: a new file name prefix for the conflict rows of
/// the task's subtask. The files stay for the user to inspect, so they are
/// not under `<task-id>/`, which the global-sort cleanup deletes.
#[must_use]
pub fn new_file_name_prefix(task_id: i64, subtask_id: i64) -> String {
    format!("{STORAGE_DIR}/{task_id}/{subtask_id}-{}", uuid::Uuid::new_v4())
}

#[cfg(test)]
mod tests {
    #[test]
    fn prefix_layout() {
        let p = super::new_file_name_prefix(7, 30);
        let rest = p.strip_prefix(super::STORAGE_PREFIX).unwrap().strip_prefix("7/30-").unwrap();
        assert!(uuid::Uuid::parse_str(rest).is_ok(), "{p}");
    }
}
