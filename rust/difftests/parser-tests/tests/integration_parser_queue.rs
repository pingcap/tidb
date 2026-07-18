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

//! Verifies that the static parser porting queue can be generated without Go.

use std::process::Command;

#[test]
fn integration_parser_queue_is_current_and_static() {
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
            "integration_parser_queue",
            "--",
            "--check",
        ])
        .output()
        .expect("run the static integration parser queue");
    assert!(
        output.status.success(),
        "parser porting queue failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report = String::from_utf8(output.stdout).expect("queue report is UTF-8");
    assert!(report.starts_with("# integration parser porting queue"));
    assert!(report.contains("summary\trust_matched\t"));
    assert!(report.contains("summary\trust_rejected_as_expected\t"));
    assert!(report.contains("summary\trust_multi_statement_matched\t"));
}
