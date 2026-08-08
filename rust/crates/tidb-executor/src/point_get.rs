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

//! Exact executor-side point-get helpers from `pkg/executor/point_get.go`.
//!
//! This module is deliberately smaller than Go's `PointGetExecutor`. The
//! current native executor has table metadata and partition definitions, so
//! the five pure metadata rules below are reachable and ported. It does not
//! expose Go's `sessionctx.Context`, pessimistic lock cache, `kv.Snapshot`,
//! chunk decoder, row-checksum writer, or snapshot runtime-stat interfaces.
//! Those obligations remain explicit declines in the adjacent lockdown
//! inventory; this module is a file-lockdown seed, not a package-completion
//! claim.

use crate::{kv_table::KvColumn, PartitionDef};
use tidb_model::column::EXTRA_ROW_CHECKSUM_ID;

/// Go `GetPhysID` for every representable Rust partition ordinal.
///
/// `None` selects the logical table id. A supplied ordinal selects that
/// partition when partition metadata exists, otherwise it also falls back to
/// the logical table id. Indexing intentionally panics for an out-of-range
/// ordinal, as Go's direct `Definitions[*idx]` access does. Go's negative
/// `*int` assertion is structurally absent because Rust uses `usize`.
#[must_use]
pub fn physical_table_id(
    table_id: i64,
    definitions: Option<&[PartitionDef]>,
    partition_ordinal: Option<usize>,
) -> i64 {
    if let Some(ordinal) = partition_ordinal {
        if let Some(definitions) = definitions {
            return definitions[ordinal].id;
        }
    }
    table_id
}

/// Go `matchPartitionNames`.
///
/// An empty requested-name list accepts every physical id without touching
/// partition metadata. Otherwise only the first definition with `pid` is
/// considered, and names compare through the same Unicode-lowercase shape as
/// Go `ast.CIStr.L`.
#[must_use]
pub fn partition_name_matches(
    pid: i64,
    partition_names: &[String],
    definitions: Option<&[PartitionDef]>,
) -> bool {
    if partition_names.is_empty() {
        return true;
    }
    let definitions = definitions.expect("non-empty partition names require partition metadata");
    for definition in definitions {
        if definition.id == pid {
            let definition_name = definition.name.to_lowercase();
            for name in partition_names {
                if definition_name == name.to_lowercase() {
                    return true;
                }
            }
            return false;
        }
    }
    false
}

/// Go `shouldFillRowChecksum`: the first checksum pseudo-column, if present.
#[must_use]
pub fn row_checksum_column(columns: &[KvColumn]) -> Option<usize> {
    columns
        .iter()
        .position(|column| column.id == EXTRA_ROW_CHECKSUM_ID)
}

/// Go `notPKPrefixCol`.
#[must_use]
pub fn not_primary_prefix_column(column_id: i64, prefix_column_ids: &[i64]) -> bool {
    !prefix_column_ids.contains(&column_id)
}

/// Go `getColInfoByID`: the first column with the requested id.
#[must_use]
pub fn column_by_id(columns: &[KvColumn], column_id: i64) -> Option<&KvColumn> {
    columns.iter().find(|column| column.id == column_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::{FieldType, FieldTypeCode};

    fn definitions() -> Vec<PartitionDef> {
        vec![
            PartitionDef {
                id: 101,
                name: "P0".to_owned(),
            },
            PartitionDef {
                id: 202,
                name: "PÜNICODE".to_owned(),
            },
        ]
    }

    fn column(id: i64, name: &str) -> KvColumn {
        KvColumn {
            name: name.to_owned(),
            id,
            field_type: FieldType::new(FieldTypeCode::LongLong),
            default_value: None,
            origin_default: None,
            generated: None,
        }
    }

    #[test]
    fn physical_id_uses_the_logical_or_selected_partition_id() {
        let definitions = definitions();
        assert_eq!(physical_table_id(7, Some(&definitions), None), 7);
        assert_eq!(physical_table_id(7, None, Some(0)), 7);
        assert_eq!(physical_table_id(7, Some(&definitions), Some(0)), 101);
        assert_eq!(physical_table_id(7, Some(&definitions), Some(1)), 202);
    }

    #[test]
    #[should_panic]
    fn physical_id_preserves_the_out_of_range_definition_panic() {
        let definitions = definitions();
        let _ = physical_table_id(7, Some(&definitions), Some(2));
    }

    #[test]
    fn partition_names_preserve_empty_casefold_and_first_pid_rules() {
        assert!(partition_name_matches(9, &[], None));
        let mut definitions = definitions();
        definitions.push(PartitionDef {
            id: 202,
            name: "later".to_owned(),
        });
        assert!(partition_name_matches(
            202,
            &["pünicode".to_owned()],
            Some(&definitions)
        ));
        assert!(!partition_name_matches(
            202,
            &["later".to_owned()],
            Some(&definitions)
        ));
        assert!(!partition_name_matches(
            303,
            &["p0".to_owned()],
            Some(&definitions)
        ));
    }

    #[test]
    #[should_panic(expected = "non-empty partition names require partition metadata")]
    fn nonempty_partition_names_require_metadata() {
        let _ = partition_name_matches(101, &["p0".to_owned()], None);
    }

    #[test]
    fn checksum_and_column_lookup_choose_the_first_matching_id() {
        let columns = vec![
            column(1, "a"),
            column(EXTRA_ROW_CHECKSUM_ID, "first"),
            column(EXTRA_ROW_CHECKSUM_ID, "second"),
        ];
        assert_eq!(row_checksum_column(&[]), None);
        assert_eq!(row_checksum_column(&columns), Some(1));
        assert_eq!(
            column_by_id(&[], 1).map(|column| column.name.as_str()),
            None
        );
        assert_eq!(
            column_by_id(&columns, EXTRA_ROW_CHECKSUM_ID).map(|column| column.name.as_str()),
            Some("first")
        );
        assert_eq!(
            column_by_id(&columns, 99).map(|column| column.name.as_str()),
            None
        );
    }

    #[test]
    fn primary_prefix_membership_covers_empty_hit_and_miss() {
        assert!(not_primary_prefix_column(7, &[]));
        assert!(!not_primary_prefix_column(7, &[5, 7, 9]));
        assert!(not_primary_prefix_column(8, &[5, 7, 9]));
    }
}

#[cfg(test)]
mod lockdown {
    use super::*;
    use sha2::{Digest, Sha256};
    use std::{
        collections::{BTreeMap, BTreeSet},
        fs,
        path::{Path, PathBuf},
    };

    const ARTIFACTS: &str = include_str!("point_get.artifacts.tsv");
    const INVENTORY: &str = include_str!("point_get.inventory.tsv");
    const MUTATION_PLAN: &str = include_str!("point_get.mutation-plan.tsv");
    const MUTATION_RESULTS: &str = include_str!("point_get.mutation-results.tsv");
    const RECEIPT: &str = include_str!("point_get.receipt.json");

    fn repository_root() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../..")
    }

    fn data_rows(contents: &str) -> Vec<Vec<&str>> {
        contents
            .lines()
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
            .skip(1)
            .map(|line| line.split('\t').collect())
            .collect()
    }

    fn sha256(bytes: impl AsRef<[u8]>) -> String {
        format!("{:x}", Sha256::digest(bytes.as_ref()))
    }

    fn source_lines(bytes: &[u8]) -> usize {
        bytes.iter().filter(|byte| **byte == b'\n').count()
            + usize::from(!bytes.is_empty() && !bytes.ends_with(b"\n"))
    }

    fn assert_relative_source(row: &[&str], source: usize, hash: usize, kind: &str) {
        let sources: Vec<_> = row[source].split('|').collect();
        let hashes: Vec<_> = row[hash].split('|').collect();
        assert_eq!(sources.len(), hashes.len(), "{kind} hash width: {row:?}");
        for (source, expected_hash) in sources.into_iter().zip(hashes) {
            let relative = Path::new(source);
            assert!(!relative.is_absolute(), "absolute {kind} path: {source}");
            assert!(
                !relative.components().any(|part| part.as_os_str() == ".."),
                "parent traversal in {kind} path: {source}"
            );
            let bytes = fs::read(repository_root().join(relative))
                .unwrap_or_else(|error| panic!("read {kind} source {source}: {error}"));
            assert_eq!(
                sha256(bytes),
                expected_hash,
                "{kind} source drift: {source}"
            );
        }
    }

    #[test]
    fn point_get_lockdown_symbols_compile_and_inventory_is_exact() {
        let _physical: fn(i64, Option<&[PartitionDef]>, Option<usize>) -> i64 = physical_table_id;
        let _partition: fn(i64, &[String], Option<&[PartitionDef]>) -> bool =
            partition_name_matches;
        let _checksum: fn(&[KvColumn]) -> Option<usize> = row_checksum_column;
        let _prefix: fn(i64, &[i64]) -> bool = not_primary_prefix_column;
        let _column: for<'a> fn(&'a [KvColumn], i64) -> Option<&'a KvColumn> = column_by_id;

        let artifacts = data_rows(ARTIFACTS);
        assert_eq!(artifacts.len(), 6);
        assert!(artifacts.iter().all(|row| row.len() == 5));
        for row in artifacts {
            let relative = Path::new(row[0]);
            assert!(!relative.is_absolute());
            assert!(!relative.components().any(|part| part.as_os_str() == ".."));
            let bytes = fs::read(repository_root().join(relative)).expect("read owned Go artifact");
            assert_eq!(sha256(&bytes), row[2], "artifact hash drift: {}", row[0]);
            assert_eq!(
                bytes.len().to_string(),
                row[3],
                "artifact size drift: {}",
                row[0]
            );
            assert_eq!(
                source_lines(&bytes).to_string(),
                row[4],
                "artifact line drift: {}",
                row[0]
            );
        }

        let inventory = data_rows(INVENTORY);
        assert_eq!(inventory.len(), 763);
        assert!(inventory.iter().all(|row| row.len() == 11));
        let mut sources = BTreeMap::new();
        let mut statuses = BTreeMap::new();
        let mut symbols = BTreeSet::new();
        for row in inventory {
            *sources.entry(row[2]).or_insert(0usize) += 1;
            *statuses.entry(row[6]).or_insert(0usize) += 1;
            assert!(row[8].contains(&format!("@sha256:{}", row[4])));
            assert!(!row[9].is_empty());
            match row[6] {
                "PORTED" => {
                    symbols.insert(row[7]);
                    assert!(row[8].contains("rust-compile-anchor:"));
                    assert!(row[8].contains("mutation-suite:"));
                    assert_eq!(row[10], "boundary-mutation-killed");
                }
                "DECLINED" => {
                    assert_eq!(row[7], "-");
                    assert!(row[8].contains("measured-gap:"));
                }
                "UNREACHABLE" => {
                    assert_eq!(row[7], "-");
                    assert!(row[8].contains("structural-proof:"));
                }
                verdict => panic!("invalid or blank verdict {verdict:?}: {}", row[0]),
            }
        }
        assert_eq!(
            sources,
            BTreeMap::from([
                ("pkg/executor/point_get.go", 358usize),
                ("pkg/executor/point_get_test.go", 100usize),
                ("pkg/executor/executor_failpoint_test.go", 11usize),
                ("pkg/executor/internal/exec/executor_test.go", 116usize),
                ("pkg/executor/internal/exec/indexusage_test.go", 144usize),
                ("tests/realtikvtest/txntest/stale_read_test.go", 34usize),
            ])
        );
        assert_eq!(
            statuses,
            BTreeMap::from([
                ("DECLINED", 734usize),
                ("PORTED", 28usize),
                ("UNREACHABLE", 1usize),
            ])
        );
        assert_eq!(
            symbols,
            BTreeSet::from([
                "point_get::column_by_id",
                "point_get::not_primary_prefix_column",
                "point_get::partition_name_matches",
                "point_get::physical_table_id",
                "point_get::row_checksum_column",
            ])
        );
    }

    #[test]
    fn point_get_lockdown_mutations_are_killed_and_sources_restored() {
        let plan = data_rows(MUTATION_PLAN);
        let results = data_rows(MUTATION_RESULTS);
        assert_eq!(plan.len(), 12);
        assert_eq!(results.len(), 22);
        assert!(plan.iter().all(|row| row.len() == 8));
        assert!(results.iter().all(|row| row.len() == 9));

        let expected_counts = plan
            .iter()
            .map(|row| (row[0], row[2].parse::<usize>().expect("mutation count")))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(expected_counts.values().sum::<usize>(), 22);
        let baselines = plan
            .iter()
            .map(|row| (row[0], row[3]))
            .collect::<BTreeMap<_, _>>();
        for row in &plan {
            assert_relative_source(row, 4, 5, "mutation plan");
        }

        let mut actual_counts = expected_counts
            .keys()
            .map(|suite| (*suite, 0usize))
            .collect::<BTreeMap<_, _>>();
        let mut ids = BTreeSet::new();
        for row in results {
            assert!(ids.insert(row[0]), "duplicate mutation id: {}", row[0]);
            assert_eq!(row[2], "KILLED", "surviving mutation: {row:?}");
            assert_eq!(row[3], baselines[row[1]], "baseline drift: {row:?}");
            assert!(!row[4].is_empty());
            assert_ne!(row[5], "0");
            assert_eq!(row[8], "PASS");
            assert_relative_source(&row, 6, 7, "mutation result");
            *actual_counts.entry(row[1]).or_insert(0usize) += 1;
        }
        assert_eq!(actual_counts, expected_counts);
    }

    #[test]
    fn point_get_lockdown_receipt_keeps_the_file_seed_boundary() {
        assert!(
            RECEIPT.contains("\"claim_boundary\": \"file-lockdown-seed-not-package-completion\"")
        );
        assert!(RECEIPT.contains("\"obligation_count\": 763"));
        assert!(RECEIPT.contains("\"ported_obligation_count\": 28"));
        assert!(RECEIPT.contains("\"ported_symbol_count\": 5"));
        assert!(RECEIPT.contains("\"reachable_ported_rule_count\": 28"));
        assert!(RECEIPT.contains("\"whole_go_package_complete\": false"));
    }
}
