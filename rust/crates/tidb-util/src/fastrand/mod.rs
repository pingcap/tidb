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

//! Complete transcreation of `pkg/util/fastrand`.
//!
//! `random.go` and `runtime.go` retain separate recognizable modules. The
//! original `TestRand` and all four benchmarks have executable Rust
//! translations. The Go-only `TestMain` installs TiDB's global Go test setup
//! and ignores known third-party goroutines during goleak inspection; this
//! Rust module starts no background workers and needs neither hook nor ignore
//! list. Bazel's library/test declarations map to this Cargo module, its unit
//! tests, and the `fastrand` benchmark target.

mod random;
mod runtime;

pub use random::{buf, uint32_n, uint64_n};
pub use runtime::uint32;
