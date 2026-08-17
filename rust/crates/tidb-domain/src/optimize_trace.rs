// Copyright 2025 PingCAP, Inc.
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

//! Go `pkg/domain/optimize_trace.go` lands complete.
//!
//! The file has exactly one production symbol, `GetOptimizerTraceDirName`
//! ([`get_optimizer_trace_dir_name`]): the relative directory, under
//! external storage, that this TiDB instance writes optimizer traces into.
//! It is `optimizer_trace/<instance id>`, where the instance id is the
//! registered server info ID, falling back to the process ID when that is
//! unavailable or empty.
//!
//! The fallback is the point of the function: two instances must not share a
//! trace directory, and an empty ID would collapse them all into
//! `optimizer_trace/`. The PID is not globally unique either, but it is
//! unique among processes on one host, which is as far as the Go goes.
//!
//! Narrowings, all named:
//!
//! - `// boundary:` Go `pkg/domain/infosync.GetServerInfo()` — the global
//!   info syncer has no Rust home yet. Go treats both `err != nil` and a
//!   `nil` info as "no ID", so the two collapse into a single
//!   `Option<&str>` parameter here: `None` covers both, and `Some("")`
//!   stays distinct because Go checks the empty string separately (it
//!   reaches the same fallback, but through the other branch).
//! - `// boundary:` Go `os.Getpid()` — [`get_optimizer_trace_dir_name`]
//!   reads the live PID exactly as Go does;
//!   [`optimizer_trace_dir_name_with`] takes it explicitly so the composition
//!   is testable without spawning processes.
//!
//! `filepath.Join` is reproduced with [`std::path::Path::join`], which uses
//! the same platform separator. Both also normalize, but neither component
//! here can contain a separator or a `..`, so no normalization is
//! observable.

use std::path::Path;

/// Go's literal first path component.
pub const OPTIMIZER_TRACE_DIR: &str = "optimizer_trace";

/// Go `GetOptimizerTraceDirName`.
///
/// `server_info_id` is the ID from `infosync.GetServerInfo()`; pass `None`
/// when the call errored or returned a nil info.
#[must_use]
pub fn get_optimizer_trace_dir_name(server_info_id: Option<&str>) -> String {
    // boundary: Go `os.Getpid()`.
    optimizer_trace_dir_name_with(server_info_id, std::process::id())
}

/// [`get_optimizer_trace_dir_name`] with Go's `os.Getpid()` supplied.
#[must_use]
pub fn optimizer_trace_dir_name_with(server_info_id: Option<&str>, pid: u32) -> String {
    let mut instance_id = server_info_id.unwrap_or("");
    // If instanceID is empty, use the process ID as the instance ID. This is
    // a fallback for the case where the instance ID is not set.
    let pid_string;
    if instance_id.is_empty() {
        pid_string = pid.to_string();
        instance_id = &pid_string;
    }
    Path::new(OPTIMIZER_TRACE_DIR)
        .join(instance_id)
        .to_string_lossy()
        .into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    // `pkg/domain` has no upstream test for `GetOptimizerTraceDirName` —
    // `optimize_trace.go` has no `_test.go` counterpart and no other test in
    // the package names the symbol — so these are written, not transcreated.

    #[test]
    fn a_registered_id_is_used_verbatim() {
        assert_eq!(
            optimizer_trace_dir_name_with(Some("s1"), 4242),
            format!("optimizer_trace{}s1", std::path::MAIN_SEPARATOR)
        );
    }

    #[test]
    fn a_missing_server_info_falls_back_to_the_pid() {
        assert_eq!(
            optimizer_trace_dir_name_with(None, 4242),
            format!("optimizer_trace{}4242", std::path::MAIN_SEPARATOR)
        );
    }

    #[test]
    fn an_empty_id_takes_the_same_fallback() {
        // Go reaches the fallback through the `instanceID == ""` check
        // rather than the error check, but lands on the same path.
        assert_eq!(
            optimizer_trace_dir_name_with(Some(""), 7),
            optimizer_trace_dir_name_with(None, 7)
        );
    }

    #[test]
    fn the_live_pid_is_what_the_no_arg_form_uses() {
        assert_eq!(
            get_optimizer_trace_dir_name(None),
            optimizer_trace_dir_name_with(None, std::process::id())
        );
        assert!(get_optimizer_trace_dir_name(Some("abc")).starts_with(OPTIMIZER_TRACE_DIR));
    }
}
