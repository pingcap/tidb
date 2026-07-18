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

//! Statistics handle pool boundary from
//! `pkg/statistics/handle/util/pool.go`.
//!
//! The Go interface couples its goroutine and session pools to `gp.Pool` and
//! `syssession.Pool`.  This Rust trait keeps those resources opaque while
//! preserving the dynamic access and close lifecycle.  Concrete pool creation,
//! worker limits, session checkout, and shutdown behavior remain external
//! owners.

/// Access to the goroutine and session pools used by the statistics handle.
pub trait StatsPool<G, S> {
    /// Returns the goroutine-pool resource.
    fn gpool(&self) -> &G;

    /// Returns the advanced-session-pool resource.
    fn spool(&self) -> &S;

    /// Closes pool-owned resources.
    fn close(&mut self);
}
