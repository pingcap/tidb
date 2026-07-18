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

//! Physical statistics IDs selected for DDL events from
//! `pkg/statistics/handle/ddl/subscriber.go`.
//!
//! The Go subscriber emits partition IDs for partitioned tables and, under
//! dynamic pruning, appends the global table ID. A nil partition descriptor
//! means a non-partitioned table and must remain distinct from an empty list of
//! partition definitions. This leaf owns only that ordered ID selection;
//! TableInfo decoding, session-variable reads, DDL event handling, and stats
//! storage remain external.

/// Selects the physical IDs whose statistics a DDL event should update.
///
/// `partition_ids == None` represents a table without partition metadata and
/// therefore returns only `table_id`. `Some(ids)` preserves the source's
/// caller order, including an empty partition-definition list. Dynamic prune
/// mode appends the global table ID to the partition IDs in either case.
#[must_use]
pub fn physical_ids_for_stats_ddl(
    table_id: i64,
    partition_ids: Option<&[i64]>,
    dynamic_prune: bool,
) -> Vec<i64> {
    let Some(partition_ids) = partition_ids else {
        return vec![table_id];
    };

    let mut ids = partition_ids.to_vec();
    if dynamic_prune {
        ids.push(table_id);
    }
    ids
}
