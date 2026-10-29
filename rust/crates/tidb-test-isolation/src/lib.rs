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

//! Test-harness isolation for tests that change process-global state.

/// Runs the calling test in its own harness process when it touches
/// process-global hooks (sysvar hooks, the global config) that other
/// simulated nodes in the same test process would share; threads created
/// inside the body still exercise the real publication races. Returns `true`
/// in the parent, which then skips the body, and `false` in the child, which
/// runs it.
pub fn isolate_process_globals() -> bool {
    const MARKER: &str = "TIDB_SERVER_ISOLATED_GLOBALS_TEST";
    let thread = std::thread::current();
    let name = thread.name().expect("named Rust test thread");
    if std::env::var(MARKER).as_deref() == Ok(name) {
        return false;
    }
    let output = std::process::Command::new(std::env::current_exe().expect("test executable"))
        .args(["--exact", name, "--nocapture"])
        .env(MARKER, name)
        .output()
        .expect("start isolated global-state test");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success() && stdout.contains("1 passed; 0 failed"),
        "isolated test {name} failed or was not selected: {}\n{stdout}\n{}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    true
}
