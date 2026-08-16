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

//! SEED of Go `pkg/ttl/cache`: the TTL worker's view of which physical tables
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
//! This is labelled a SEED rather than a complete package, and the reason is
//! *not* that most of Go's tests need `testkit`: `base.go`'s and `table.go`'s
//! whole content, both row decoders, and every SQL statement builder that does
//! not encode datums came across, and they are covered by direct assertions
//! here. It is a seed because two pieces of production behaviour are still
//! missing:
//!
//! - The `Update` methods of [`infoschema::InfoSchemaCache`] and
//!   [`ttlstatus::TableStatusCache`], whose info-schema traversal is expressed
//!   against trait boundaries rather than the real `infoschema.InfoSchema`.
//!   This is a genuine boundary: `pkg/infoschema` has no transcreation to
//!   depend on.
//! - `task.go`'s `InsertIntoTTLTask`, which memcomparable-encodes the scan
//!   range bounds through `codec.EncodeKey`, and the `encoding/json` decode of
//!   `TTLTaskState`. These are NOT blocked any more — `tidb-codec` is now a
//!   dependency of this crate — they are simply unported work in a module this
//!   change did not touch, and [`task`]'s own header still describes them as
//!   dependency-blocked.
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
