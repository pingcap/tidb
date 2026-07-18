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
// See the License for the specific language governing permissions and
// limitations under the License.

//! Makes the upstream-test inventory part of the normal differential gate.

use std::process::Command;

#[test]
fn upstream_go_test_inventory_is_current() {
    let output = Command::new(env!("CARGO_BIN_EXE_go_test_ledger"))
        .arg("--check")
        .output()
        .expect("run the Go test ledger generator");
    assert!(
        output.status.success(),
        "upstream test ledger is stale:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
}
