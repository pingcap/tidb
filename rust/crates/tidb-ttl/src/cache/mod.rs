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

//! Go `pkg/ttl/cache`: the TTL worker's view of which physical tables
//! have TTL, what state their jobs and tasks are in, and how a table's key
//! space splits into scan ranges.
//!
//! File mapping (one Rust module per Go file):
//! - [`base`] <- `base.go` — complete.
//! - [`infoschema`] <- `infoschema.go`
//! - [`table`] <- `table.go` — complete.
//! - [`task`] <- `task.go`
//! - [`ttlstatus`] <- `ttlstatus.go`
//!
//! This remains a partial package because the two cache `Update` methods are
//! transcreated against trait boundaries (`TtlInfoSchema`, `Session`) rather
//! than a real `infoschema.InfoSchema` transcreation, which this workspace
//! does not have to depend on. The reason is *not* that most of Go's tests
//! need `testkit`: `base.go`'s and `table.go`'s whole content, both row
//! decoders, and every SQL statement builder that does not encode datums came
//! across, and they are covered by direct assertions here. The boundary is:
//!
//! - The `Update` methods of [`infoschema::InfoSchemaCache`] and
//!   [`ttlstatus::TableStatusCache`] drive their info-schema traversal through
//!   those traits; the traversal logic itself is ported.
//! The task statement builder now uses `tidb-codec` for scan-range encoding,
//! and `RowToTTLTask` decodes both ranges and `TTLTaskState` with the same
//! persisted contracts as Go.
//!
//! Every narrowing is named at its own definition site with a `// boundary:`
//! comment identifying the Go symbol it stands for.

pub mod base;
pub mod infoschema;
pub mod table;
pub mod task;
pub mod ttlstatus;

/// This package's error, standing in for Go's `errors.Errorf`/`error`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheError(pub String);

impl std::fmt::Display for CacheError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for CacheError {}

/// This package's `Result` alias.
pub type Result<T> = std::result::Result<T, CacheError>;

pub(crate) fn error(text: impl Into<String>) -> CacheError {
    CacheError(text.into())
}
