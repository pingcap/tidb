// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The ONE place the cluster tier decides whether it can serve an
//! `AUTO_INCREMENT` table -- read by the `CREATE TABLE` admission path and by
//! the catalog loader, so the two cannot disagree about what a valid table is.
//!
//! # Why the answer is "no", and what it would take to make it "yes"
//!
//! Go does not keep the auto-increment counter inside `TableInfo`. It lives in
//! meta keys of its own -- `pkg/meta/meta.go`'s `autoTableIDKey` (`TID:<id>`)
//! and `autoIncrementIDKey` (`IID:<id>`), bumped with `HInc` -- and
//! `pkg/meta/autoid/autoid.go`'s `Allocator` hands ids out of a range it
//! reserved there in a transaction of its OWN, so an id is burned the moment
//! it is issued and is never returned by a rollback.
//!
//! This node has no such allocator. Its counter (`tidb_executor`'s
//! `kv_table::auto_id::AutoIdAllocator`) is process-local and starts at zero,
//! which is exactly right for the in-process tier that creates every row it
//! serves, and exactly wrong against shared cluster storage: a table loaded
//! with a fresh counter re-issues ids that already exist in TiKV, and a Go
//! `tidb-server` on the same cluster would be handing out ids from the same
//! range at the same time. Loading such a table would trade an honest refusal
//! for duplicate primary keys.
//!
//! So the table is refused, and refused in BOTH directions: `CREATE TABLE`
//! will not write a shape the loader cannot serve -- which used to leave a
//! table whose own creator answered `table not found in catalog` to every
//! INSERT and SELECT -- and the loader still refuses one that a Go node
//! created, because that half is not this node's to admit.
//!
//! Making the answer "yes" is a unit of its own: give the counter the same
//! separate-key home Go gives it, allocate through it in a transaction that is
//! not the row's, and prove the counter survives a node restart.

use tidb_datatype::FieldTypeFlags;
use tidb_model::table_info::TableInfo;

/// Why this node cannot serve `table`'s auto-increment column, if it has one.
///
/// The text is a sentence fragment beginning "its column ...", so both callers
/// can prefix it with their own subject the way their other refusals read.
///
/// Every stored column is examined, not just the public ones: a column still
/// coming up through the schema states is one this node would have to serve a
/// moment later, and `CREATE TABLE`'s template is inspected before its columns
/// are published at all.
#[must_use]
pub fn auto_increment_refusal(table: &TableInfo) -> Option<String> {
    table
        .columns
        .iter()
        .find(|column| column.field_type.has_flag(FieldTypeFlags::AUTO_INCREMENT))
        .map(|column| {
            format!(
                "its column {} is AUTO_INCREMENT, whose ids come from the cluster's own \
                 autoid allocator, which this node does not consume",
                column.name.original()
            )
        })
}
