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

//! Pinned Go `pkg/planner/util/domainmisc`.
//!
//! The package has one operation: compare a caller's schema version with the
//! latest domain infoschema and, when it changed, return the latest index
//! metadata for one logical table. The caller decides whether a statement
//! needs the comparison.

use std::collections::BTreeMap;

use tidb_infoschema::table_source::SourceIndex;

use tidb_infoschema::table_source::LatestIndexSchema;


/// Go's `errors.New("domain not found for ctx")`: the caller has no domain.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DomainNotFound;

impl std::fmt::Display for DomainNotFound {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("domain not found for ctx")
    }
}

impl std::error::Error for DomainNotFound {}

/// Go `domainmisc.GetLatestIndexInfo`.
///
/// `None` is Go's nil domain. A missing table in a newer schema produces an
/// empty map, not an error, exactly like ranging over a nil `latestTblInfo`.
pub fn get_latest_index_info(
    latest: Option<&LatestIndexSchema>,
    table_id: i64,
    start_version: u64,
) -> Result<(Option<BTreeMap<i64, SourceIndex>>, bool), DomainNotFound> {
    let latest = latest.ok_or(DomainNotFound)?;
    if latest.schema_meta_version == start_version {
        return Ok((None, false));
    }

    let indexes = latest
        .table_indexes
        .get(&table_id)
        .into_iter()
        .flatten()
        .map(|index| (index.read().id, index.clone()))
        .collect();
    Ok((Some(indexes), true))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unchanged_schema_does_not_build_an_index_map() {
        let latest = LatestIndexSchema {
            schema_meta_version: 7,
            ..LatestIndexSchema::default()
        };
        let (indexes, changed) = get_latest_index_info(Some(&latest), 42, 7).unwrap();
        assert!(!changed);
        assert!(indexes.is_none());
    }

    #[test]
    fn missing_table_in_changed_schema_returns_an_empty_map() {
        let latest = LatestIndexSchema {
            schema_meta_version: 8,
            ..LatestIndexSchema::default()
        };
        let (indexes, changed) = get_latest_index_info(Some(&latest), 42, 7).unwrap();
        assert!(changed);
        assert!(indexes.unwrap().is_empty());
    }

    #[test]
    fn missing_domain_matches_go_error() {
        let error = get_latest_index_info(None, 42, 7).unwrap_err();
        assert!(error.to_string().contains("domain not found for ctx"));
    }
}
