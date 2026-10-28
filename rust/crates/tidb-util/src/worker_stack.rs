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

//! The stack a dedicated worker thread gets when it does what Go runs on a
//! goroutine: plan or execute SQL, or commit a transaction.

/// A goroutine's stack grows on demand and keeps its size for the
/// goroutine's life; a reserved thread stack of this size is that semantics
/// for a thread (address space, committed only as touched). The default
/// 2 MB thread stack does not hold a debug build's commit path: the client's
/// prewrite futures overflowed it and aborted the process.
pub const WORKER_STACK_BYTES: usize = 32 * 1024 * 1024;

/// Makes [`WORKER_STACK_BYTES`] the default stack of every thread this
/// process spawns without naming a size (`std::thread::spawn`, scoped
/// threads, a `Builder` with no `stack_size`), unless `RUST_MIN_STACK` is
/// already set. The standard library reads that variable once, at the first
/// spawn, so a binary must call this at the top of `main`, before any thread
/// exists. Cargo's `[env]` gives `cargo run` and `cargo test` the same value;
/// a binary started any other way (TiUP, a playground, a copied test binary)
/// would otherwise run its background workers on 2 MB stacks.
pub fn install_as_default_thread_stack() {
    const VARIABLE: &str = "RUST_MIN_STACK";
    if std::env::var_os(VARIABLE).is_none() {
        std::env::set_var(VARIABLE, WORKER_STACK_BYTES.to_string());
    }
}

// A connection worker runs the planner, whose recursion is guarded by
// `stacker::maybe_grow(red_zone = 2 MB, segment = 16 MB)`. On the default
// 2 MB thread stack the red-zone check fails on EVERY select, so each
// statement mmap'ed and munmap'ed a fresh 16 MB segment -- measured at ~11%
// of the serving thread under sysbench point selects. Go never pays this
// because a goroutine's grown stack PERSISTS for the goroutine's life; a
// large reserved stack is that semantics for a dedicated thread (reserved
// address space, committed only as touched), leaving `maybe_grow` as the
// safety net for genuinely deep plans.
pub const SQL_WORKER_STACK_BYTES: usize = WORKER_STACK_BYTES;

/// A named thread with the SQL worker's stack, for every background worker
/// that runs a session, a transaction or a catalog reload. Go runs these on
/// goroutines whose stacks grow; a debug build's commit path alone (the
/// client's prewrite futures) outgrows the default 2 MB thread stack, which
/// aborted the whole process from `tidb-binding-maintenance`'s GC commit.
pub fn sql_worker_thread(name: impl Into<String>) -> std::thread::Builder {
    std::thread::Builder::new()
        .name(name.into())
        .stack_size(SQL_WORKER_STACK_BYTES)
}
