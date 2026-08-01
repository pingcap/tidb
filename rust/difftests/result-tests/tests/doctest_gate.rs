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

//! The doctest gate, enforced from inside the workspace suite.
//!
//! `cargo nextest` CANNOT run doctests -- the doctest harness is rustdoc's,
//! not libtest's, so there is nothing for nextest to execute. The workspace
//! gate is a nextest run, so until this test existed every example inside a
//! `///` comment was unverified, and `cargo test -p tidb-executor --doc` had
//! been failing with nobody watching.
//!
//! Documenting `cargo test --doc` as a separate step is what we had, and it
//! is what failed: a step a human has to remember is a step that gets
//! skipped. This is the same shape as `source_size_ratchet`, and the same
//! fix -- `rust/scripts/run-doctests.sh` owns the logic and can be run
//! instantly on its own, while this three-line wrapper makes
//! `cargo test --workspace` / `cargo nextest run --workspace` fail when a
//! doctest fails or quietly stops existing.
//!
//! Shelling out to `cargo` from within a test is safe here, and that was
//! checked rather than assumed: cargo releases the build-directory lock
//! before it runs test binaries, so the nested `cargo test --doc` acquires
//! it and completes instead of deadlocking against its own parent. The
//! nested run compiles no doctest twice either -- `--doc` runs only
//! doctests, so it never re-enters this test.

use std::process::Command;

#[test]
fn every_doc_example_is_still_checked() {
    let script = difftest::parser_oracle::repo_root()
        .join("rust")
        .join("scripts")
        .join("run-doctests.sh");
    let output = Command::new(&script)
        .output()
        .expect("run the doctest gate");
    assert!(
        output.status.success(),
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
