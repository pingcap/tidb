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

//! Disjoint-set package transcreated from `pkg/util/disjointset`.
//!
//! The package has two production files, two test files, one test-process
//! setup file, and one Bazel target. Rust's test harness has no package-owned
//! background workers, so Go's `goleak.VerifyTestMain` needs no runtime
//! analogue.

mod int_set;
mod set;

pub use int_set::SimpleIntSet;
pub use set::Set;
