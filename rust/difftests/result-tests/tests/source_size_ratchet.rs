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

//! The source-file size ratchet, enforced in the workspace suite.
//!
//! The check itself is `rust/scripts/check-source-size.sh` with its bounds in
//! `rust/scripts/source_size_bounds.txt` -- ONE owner, runnable instantly
//! with no build. This test only shells out to it so `cargo test --workspace`
//! still fails when a file grows past its bound, a new file exceeds the soft
//! limit, or a shrunken file's entry is left unretired.

use std::process::Command;

#[test]
fn no_source_file_grows_huge_silently() {
    let script = difftest::parser_oracle::repo_root()
        .join("rust")
        .join("scripts")
        .join("check-source-size.sh");
    let output = Command::new(&script).output().expect("run the ratchet");
    assert!(
        output.status.success(),
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
