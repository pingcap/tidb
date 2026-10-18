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

//! `install_as_default_thread_stack` must run before the first thread spawn,
//! so this check owns `main` (`harness = false`): it re-runs itself without
//! `RUST_MIN_STACK`, as a server started by TiUP runs, and the child spawns an
//! unsized thread that needs about 8 MiB of stack.

const CHILD: &str = "TIDB_WORKER_STACK_CHILD";

/// About 8 MiB of stack: far past a 2 MB default, inside the worker stack.
#[inline(never)]
fn deep(depth: usize) -> u64 {
    let frame = std::hint::black_box([depth as u8; 64 * 1024]);
    if depth == 0 {
        u64::from(frame[0])
    } else {
        deep(depth - 1) + u64::from(frame[depth % frame.len()])
    }
}

fn main() {
    // Answer the test runner's listing protocol (`--list`, as nextest asks):
    // one test, never ignored.
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    if arguments.iter().any(|argument| argument == "--list") {
        if !arguments.iter().any(|argument| argument == "--ignored") {
            println!("default_thread_stack: test");
        }
        return;
    }
    if std::env::var_os(CHILD).is_some() {
        tidb_util::worker_stack::install_as_default_thread_stack();
        let used = std::thread::spawn(|| deep(128))
            .join()
            .expect("deep thread");
        assert!(used > 0);
        return;
    }
    let status = std::process::Command::new(std::env::current_exe().expect("test binary"))
        .env_remove("RUST_MIN_STACK")
        .env(CHILD, "1")
        .stderr(std::process::Stdio::null())
        .status()
        .expect("run child");
    assert!(
        status.success(),
        "an unsized thread overflowed without RUST_MIN_STACK: {status:?}"
    );
    println!("default_thread_stack: ok");
}
