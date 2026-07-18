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

//! Guards the complete Go parser source-to-Rust ownership manifest.

use std::process::Command;

#[test]
fn parser_translation_manifest_is_current() {
    let output = Command::new(std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into()))
        .current_dir(
            difftest::difftest_root()
                .parent()
                .expect("difftest lives below the Rust workspace"),
        )
        .args([
            "run",
            "-q",
            "-p",
            "difftest",
            "--bin",
            "parser_translation_manifest",
            "--",
            "--check",
        ])
        .output()
        .expect("run parser translation manifest checker");
    assert!(
        output.status.success(),
        "parser translation manifest is stale or incomplete:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
}
