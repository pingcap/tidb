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

//! Go `br/pkg/restore/utils/merge.go`: fuses a backup's many small file ranges
//! into region-sized ranges before restore splits regions.

use std::collections::BTreeMap;
use std::sync::Arc;

use super::misc::{DEFAULT_CF_NAME, WRITE_CF_NAME};
use super::rewrite_rule::{rewrite_range, RestoreError, RestoreErrorKind, RewriteRules};
use super::File;
use crate::rtree::{Range, RangeStats, RangeStatsTree};

/// Go `utils.MergeRangesStat`: statistics for `MergeAndRewriteFileRanges`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MergeRangesStat {
    /// Go `MergeRangesStat.TotalFiles`.
    pub total_files: usize,
    /// Go `MergeRangesStat.TotalWriteCFFile`.
    pub total_write_cf_file: usize,
    /// Go `MergeRangesStat.TotalDefaultCFFile`.
    pub total_default_cf_file: usize,
    /// Go `MergeRangesStat.TotalRegions`.
    pub total_regions: usize,
    /// Go `MergeRangesStat.RegionKeysAvg`.
    pub region_keys_avg: usize,
    /// Go `MergeRangesStat.RegionBytesAvg`.
    pub region_bytes_avg: usize,
    /// Go `MergeRangesStat.MergedRegions`.
    pub merged_regions: usize,
    /// Go `MergeRangesStat.MergedRegionKeysAvg`.
    pub merged_region_keys_avg: usize,
    /// Go `MergeRangesStat.MergedRegionBytesAvg`.
    pub merged_region_bytes_avg: usize,
}

/// Go `MergeAndRewriteFileRanges`: the file ranges merged according to
/// `split_size_bytes` and `split_key_count`.
///
/// Merging small ranges speeds up restoring a backup that contains many small
/// ranges (regions), because it reduces region splitting and scattering.
///
/// The empty-input case returns `(vec![], Some(default), ...)`, matching Go's
/// `([]rtree.RangeStats{}, &MergeRangesStat{}, nil)`; the "unknown CF" case
/// returns an error while Go additionally returns a `nil` stat, which no caller
/// can read.
///
/// # Errors
///
/// - [`RestoreErrorKind::RestoreInvalidBackup`] when no file belongs to either
///   the write or the default column family.
/// - [`RestoreErrorKind::InvalidRange`] when a range cannot be rewritten, or
///   when two distinct ranges collide.
///
/// # Panics
///
/// Panics (Go: `log.Panic`) when two files share a start key but disagree on
/// the end key.
pub fn merge_and_rewrite_file_ranges(
    files: &[Arc<File>],
    rewrite_rules: Option<&RewriteRules>,
    split_size_bytes: u64,
    split_key_count: u64,
) -> Result<(Vec<RangeStats>, MergeRangesStat), RestoreError> {
    if files.is_empty() {
        return Ok((Vec::new(), MergeRangesStat::default()));
    }
    let mut total_bytes = 0u64;
    let mut total_kvs = 0u64;
    let total_files = files.len();
    let mut write_cf_file = 0usize;
    let mut default_cf_file = 0usize;

    // Go uses a `map[string][]*File`; a `BTreeMap` only makes the iteration
    // order deterministic, which the result does not otherwise depend on.
    let mut files_map: BTreeMap<Vec<u8>, Vec<Arc<File>>> = BTreeMap::new();
    for file in files {
        let entry = files_map.entry(file.start_key.clone()).or_default();
        entry.push(file.clone());

        // Assert that it has the same end key.
        assert!(
            entry[0].end_key == file.end_key,
            "there are two files having the same start key, but different end key: \
             start key {:?}, file 1 end key {:?}, file 2 end key {:?}",
            file.start_key,
            file.end_key,
            entry[0].end_key
        );
        // All default-CF files are skipped because their ranges do not overlap.
        if file.cf == WRITE_CF_NAME || file.name.contains(WRITE_CF_NAME) {
            write_cf_file += 1;
        } else if file.cf == DEFAULT_CF_NAME || file.name.contains(DEFAULT_CF_NAME) {
            default_cf_file += 1;
        }
        total_bytes += file.total_bytes;
        total_kvs += file.total_kvs;
    }
    if write_cf_file == 0 && default_cf_file == 0 {
        return Err(RestoreError::annotate(
            RestoreErrorKind::RestoreInvalidBackup,
            "unknown backup data from neither Wrtie CF nor Default CF",
        ));
    }

    // RawKV does not have data in the write CF.
    let total_regions = default_cf_file.max(write_cf_file);

    // Check whether the files overlap.
    let mut range_tree: RangeStatsTree = RangeStatsTree::new();
    for grouped in files_map.values() {
        let mut range_size = 0u64;
        let mut range_count = 0u64;
        for f in grouped {
            range_size += f.total_bytes;
            range_count += f.total_kvs;
        }
        let rg = Range::with_files(
            grouped[0].start_key.clone(),
            grouped[0].end_key.clone(),
            grouped.clone(),
        );
        // Rewrite the range for the split, so that `splitRanges` no longer has
        // to handle rewrite rules.
        let tmp_rng = rewrite_range(rg, rewrite_rules).map_err(|_| {
            RestoreError::annotate(
                RestoreErrorKind::InvalidRange,
                format!("unable to rewrite range files {grouped:+?}"),
            )
        })?;
        if let Some(out) = range_tree.insert_range(&tmp_rng, range_size, range_count) {
            return Err(RestoreError::annotate(
                RestoreErrorKind::InvalidRange,
                format!(
                    "duplicate range {:?} files {grouped:+?}",
                    out.range.key_range
                ),
            ));
        }
    }

    let sorted_ranges = range_tree.merged_ranges(split_size_bytes, split_key_count);
    let total_regions_u64 = u64::try_from(total_regions).expect("region count fits");
    let merged_len = u64::try_from(sorted_ranges.len()).expect("range count fits");
    let region_bytes_avg = total_bytes / total_regions_u64;
    let region_keys_avg = total_kvs / total_regions_u64;
    let merged_region_bytes_avg = total_bytes / merged_len;
    let merged_region_keys_avg = total_kvs / merged_len;

    let merged_regions = sorted_ranges.len();
    Ok((
        sorted_ranges,
        MergeRangesStat {
            total_files,
            total_write_cf_file: write_cf_file,
            total_default_cf_file: default_cf_file,
            total_regions,
            region_keys_avg: usize::try_from(region_keys_avg).expect("average fits"),
            region_bytes_avg: usize::try_from(region_bytes_avg).expect("average fits"),
            merged_regions,
            merged_region_keys_avg: usize::try_from(merged_region_keys_avg).expect("average fits"),
            merged_region_bytes_avg: usize::try_from(merged_region_bytes_avg)
                .expect("average fits"),
        },
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tidb_codec::table_key::encode_index_seek_key;
    use tidb_codec::table_key::RecordHandle;
    use tidb_codec::{encode_int, encode_row_key, INT_FLAG};

    use super::*;

    #[test]
    fn merged_ranges_preserve_complete_shared_backup_files() {
        use tidb_proto::backup::{File as BackupFile, TableMeta};

        let files: Vec<_> = (0..3)
            .map(|index| {
                Arc::new(BackupFile {
                    name: format!("{index}_write.sst"),
                    sha256: vec![index as u8; 32],
                    start_key: encode_row_key(1, &RecordHandle::Int(index).encoded()),
                    end_key: encode_row_key(1, &RecordHandle::Int(index + 1).encoded()),
                    start_version: 41,
                    end_version: 79,
                    crc64xor: 17,
                    total_kvs: 11,
                    total_bytes: 23,
                    cf: "write".into(),
                    size: 101,
                    cipher_iv: vec![index as u8; 16],
                    table_metas: vec![TableMeta {
                        physical_id: 1,
                        ..Default::default()
                    }],
                })
            })
            .collect();
        let rules = super::super::get_rewrite_rule_of_table(1, 2, &BTreeMap::new(), false);
        let (ranges, stat) = merge_and_rewrite_file_ranges(&files, Some(&rules), 100, 100)
            .expect("the shared generated file is the restore payload");
        assert_eq!(stat.total_files, 3);
        assert_eq!(ranges.len(), 1);
        assert_eq!(
            ranges[0].start_key(),
            encode_row_key(2, &RecordHandle::Int(0).encoded())
        );
        assert_eq!(
            ranges[0].end_key(),
            encode_row_key(2, &RecordHandle::Int(3).encoded())
        );
        for (actual, expected) in ranges[0].files().iter().zip(&files) {
            assert!(
                Arc::ptr_eq(actual, expected),
                "merging retains Go's file references"
            );
            assert_eq!(actual.as_ref(), expected.as_ref());
        }
        assert_eq!(ranges[0].files().len(), files.len());
    }

    /// Go `conn.DefaultMergeRegionSizeBytes`: the default region split size,
    /// 96 MiB.
    const DEFAULT_MERGE_REGION_SIZE_BYTES: u64 = 96 * 1024 * 1024;
    /// Go `conn.DefaultMergeRegionKeyCount`: the default region key count.
    const DEFAULT_MERGE_REGION_KEY_COUNT: u64 = 960_000;

    /// Go's `codec.EncodeKey(tz, nil, types.NewIntDatum(v))` for a signed
    /// integer datum: the `intFlag` byte followed by the ascending
    /// mem-comparable encoding.
    fn encode_int_datum_key(value: i64) -> Vec<u8> {
        let mut out = vec![INT_FLAG];
        encode_int(&mut out, value);
        out
    }

    /// Go `fileBulder` (`merge_test.go`). The random file names Go uses only
    /// have to be unique and to carry the `_write`/`_default` marker; a
    /// counter is enough and keeps the test deterministic.
    #[derive(Default)]
    struct FileBuilder {
        table_id: i64,
        start_key_offset: i64,
        serial: u64,
    }

    impl FileBuilder {
        fn build(
            &mut self,
            table_id: i64,
            index_id: i64,
            num: i64,
            bytes: i64,
            kv: i64,
        ) -> Vec<Arc<File>> {
            assert!(num == 1 || num == 2, "num must be 1 or 2");

            // Rotate the table ID.
            if self.table_id != table_id {
                self.table_id = table_id;
                self.start_key_offset = 0;
            }

            let mut low = Vec::new();
            encode_int(&mut low, self.start_key_offset);
            self.start_key_offset += 10;
            let mut high = Vec::new();
            encode_int(&mut high, self.start_key_offset);

            let mut start_key = encode_row_key(self.table_id, &low);
            let mut end_key = encode_row_key(self.table_id, &high);
            if index_id != 0 {
                let low_value = encode_int_datum_key(self.start_key_offset - 10);
                let high_value = encode_int_datum_key(self.start_key_offset);
                start_key = encode_index_seek_key(table_id, index_id, &low_value);
                end_key = encode_index_seek_key(table_id, index_id, &high_value);
            }

            self.serial += 1;
            let mut files = vec![File {
                name: format!("{}_write.sst", self.serial),
                start_key,
                end_key,
                total_kvs: u64::try_from(kv).expect("kv fits"),
                total_bytes: u64::try_from(bytes).expect("bytes fits"),
                cf: "write".to_owned(),
                crc64xor: 0,
                ..Default::default()
            }];
            if num == 1 {
                return files.into_iter().map(Arc::new).collect();
            }

            // To match TiKV's behavior.
            files[0].total_kvs = 0;
            files[0].total_bytes = 0;
            self.serial += 1;
            files.push(File {
                name: format!("{}_default.sst", self.serial),
                start_key: encode_row_key(self.table_id, &low),
                end_key: encode_row_key(self.table_id, &high),
                total_kvs: u64::try_from(kv).expect("kv fits"),
                total_bytes: u64::try_from(bytes).expect("bytes fits"),
                cf: "default".to_owned(),
                crc64xor: 0,
                ..Default::default()
            });
            files.into_iter().map(Arc::new).collect()
        }
    }

    /// Go `TestMergeRanges` (`merge_test.go`).
    #[test]
    fn merge_ranges() {
        struct Case {
            // tableID, indexID, num, bytes, kv
            files: Vec<[i64; 5]>,
            // Number of files in each merged range.
            merged: Vec<usize>,
            total_regions: usize,
            merged_regions: usize,
        }

        let split_size_bytes =
            i64::try_from(DEFAULT_MERGE_REGION_SIZE_BYTES).expect("split size fits");
        let split_key_count =
            i64::try_from(DEFAULT_MERGE_REGION_KEY_COUNT).expect("split keys fit");

        let cases = vec![
            // Empty backup.
            Case {
                files: vec![],
                merged: vec![],
                total_regions: 0,
                merged_regions: 0,
            },
            // Do not merge a big range.
            Case {
                files: vec![[1, 0, 1, split_size_bytes, 1], [1, 0, 1, 1, 1]],
                merged: vec![1, 1],
                total_regions: 2,
                merged_regions: 2,
            },
            Case {
                files: vec![[1, 0, 1, 1, 1], [1, 0, 1, split_size_bytes, 1]],
                merged: vec![1, 1],
                total_regions: 2,
                merged_regions: 2,
            },
            Case {
                files: vec![[1, 0, 1, 1, split_key_count], [1, 0, 1, 1, 1]],
                merged: vec![1, 1],
                total_regions: 2,
                merged_regions: 2,
            },
            Case {
                files: vec![[1, 0, 1, 1, 1], [1, 0, 1, 1, split_key_count]],
                merged: vec![1, 1],
                total_regions: 2,
                merged_regions: 2,
            },
            // 3 -> 1
            Case {
                files: vec![[1, 0, 1, 1, 1], [1, 0, 1, 1, 1], [1, 0, 1, 1, 1]],
                merged: vec![3],
                total_regions: 3,
                merged_regions: 1,
            },
            // 3 -> 2, size: [split*1/3, split*1/3, split*1/2] -> [split*2/3, split*1/2]
            Case {
                files: vec![
                    [1, 0, 1, split_size_bytes / 3, 1],
                    [1, 0, 1, split_size_bytes / 3, 1],
                    [1, 0, 1, split_size_bytes / 2, 1],
                ],
                merged: vec![2, 1],
                total_regions: 3,
                merged_regions: 2,
            },
            // 4 -> 2
            Case {
                files: vec![
                    [1, 0, 1, split_size_bytes / 3, 1],
                    [1, 0, 1, split_size_bytes / 3, 1],
                    [1, 0, 1, split_size_bytes / 2, 1],
                    [1, 0, 1, 1, 1],
                ],
                merged: vec![2, 2],
                total_regions: 4,
                merged_regions: 2,
            },
            // 5 -> 3
            Case {
                files: vec![
                    [1, 0, 1, split_size_bytes / 3, 1],
                    [1, 0, 1, split_size_bytes / 3, 1],
                    [1, 0, 1, split_size_bytes, 1],
                    [1, 0, 1, split_size_bytes / 2, 1],
                    [1, 0, 1, 1, 1],
                ],
                merged: vec![2, 1, 2],
                total_regions: 5,
                merged_regions: 3,
            },
            // Do not merge ranges from different tables.
            Case {
                files: vec![[1, 0, 1, 1, 1], [2, 0, 1, 1, 1]],
                merged: vec![1, 1],
                total_regions: 2,
                merged_regions: 2,
            },
            Case {
                files: vec![
                    [1, 0, 1, split_size_bytes / 3, 1],
                    [2, 0, 1, split_size_bytes / 3, 1],
                    [2, 0, 1, split_size_bytes / 2, 1],
                ],
                merged: vec![1, 2],
                total_regions: 3,
                merged_regions: 2,
            },
            // Do not merge ranges from different indexes.
            Case {
                files: vec![[1, 1, 1, 1, 1], [1, 2, 1, 1, 1]],
                merged: vec![1, 1],
                total_regions: 2,
                merged_regions: 2,
            },
            // Index ID out of order.
            Case {
                files: vec![[1, 2, 1, 1, 1], [1, 1, 1, 1, 1]],
                merged: vec![1, 1],
                total_regions: 2,
                merged_regions: 2,
            },
            Case {
                files: vec![[1, 0, 1, 1, 1], [2, 1, 1, 1, 1], [2, 2, 1, 1, 1]],
                merged: vec![1, 1, 1],
                total_regions: 3,
                merged_regions: 3,
            },
            Case {
                files: vec![
                    [1, 0, 1, 1, 1],
                    [2, 1, 1, 1, 1],
                    [2, 0, 1, 1, 1],
                    [2, 0, 1, 1, 1],
                ],
                merged: vec![1, 1, 2],
                total_regions: 4,
                merged_regions: 3,
            },
            // Merge the same table ID and index ID.
            Case {
                files: vec![
                    [1, 0, 1, 1, 1],
                    [2, 1, 1, 1, 1],
                    [2, 1, 1, 1, 1],
                    [2, 0, 1, 1, 1],
                ],
                merged: vec![1, 2, 1],
                total_regions: 4,
                merged_regions: 3,
            },
        ];

        for (index, cs) in cases.iter().enumerate() {
            let mut files = Vec::new();
            let mut fb = FileBuilder::default();
            for f in &cs.files {
                files.extend(fb.build(f[0], f[1], f[2], f[3], f[4]));
            }
            let (rngs, stat) = merge_and_rewrite_file_ranges(
                &files,
                None,
                DEFAULT_MERGE_REGION_SIZE_BYTES,
                DEFAULT_MERGE_REGION_KEY_COUNT,
            )
            .unwrap_or_else(|err| panic!("case {index}: {err}"));
            assert_eq!(cs.total_regions, stat.total_regions, "case {index}");
            assert_eq!(cs.merged_regions, stat.merged_regions, "case {index}");
            assert_eq!(cs.merged.len(), rngs.len(), "case {index}");
            for (i, rg) in rngs.iter().enumerate() {
                assert_eq!(cs.merged[i], rg.files().len(), "case {index}");
                // The files' ranges must be within [Range.StartKey, Range.EndKey].
                for f in rg.files() {
                    assert!(rg.start_key() <= f.start_key.as_slice());
                    assert!(rg.end_key() >= f.end_key.as_slice());
                }
            }
        }
    }

    /// Go `TestMergeRawKVRanges` (`merge_test.go`).
    #[test]
    fn merge_raw_kv_ranges() {
        let mut fb = FileBuilder::default();
        let mut files = fb.build(1, 0, 2, 1, 1);
        // RawKV does not have a write CF.
        files.remove(0);
        let (_, stat) = merge_and_rewrite_file_ranges(
            &files,
            None,
            DEFAULT_MERGE_REGION_SIZE_BYTES,
            DEFAULT_MERGE_REGION_KEY_COUNT,
        )
        .expect("raw kv merges");
        assert_eq!(1, stat.total_regions);
        assert_eq!(1, stat.merged_regions);
    }

    /// Go `TestInvalidRanges` (`merge_test.go`).
    #[test]
    fn invalid_ranges() {
        let mut fb = FileBuilder::default();
        let mut files = fb.build(1, 0, 1, 1, 1);
        let file = Arc::get_mut(&mut files[0]).expect("unshared fixture");
        file.name = "invalid.sst".to_owned();
        file.cf = "invalid".to_owned();
        let err = merge_and_rewrite_file_ranges(
            &files,
            None,
            DEFAULT_MERGE_REGION_SIZE_BYTES,
            DEFAULT_MERGE_REGION_KEY_COUNT,
        )
        .expect_err("neither write nor default CF");
        assert_eq!(RestoreErrorKind::RestoreInvalidBackup, err.kind());
    }

    /// Execute each original Go benchmark workload once. This checks that
    /// large merges retain every shared file; it is not a timing comparison.
    #[test]
    #[ignore = "Go BenchmarkMergeRanges* workloads; run explicitly"]
    fn benchmark_merge_ranges() {
        for count in [100, 1_000, 10_000, 50_000, 100_000] {
            let mut builder = FileBuilder::default();
            let files: Vec<_> = (0..count)
                .flat_map(|_| builder.build(1, 0, 1, 1, 1))
                .collect();
            let (ranges, stats) = merge_and_rewrite_file_ranges(
                &files,
                None,
                DEFAULT_MERGE_REGION_SIZE_BYTES,
                DEFAULT_MERGE_REGION_KEY_COUNT,
            )
            .expect("original merge benchmark workload");
            assert_eq!(stats.total_files, count);
            let actual: Vec<_> = ranges.iter().flat_map(|range| range.files()).collect();
            assert_eq!(actual.len(), files.len());
            assert!(actual
                .iter()
                .zip(&files)
                .all(|(actual, expected)| Arc::ptr_eq(actual, expected)));
        }
    }

    /// Keeps the record-handle import honest: `encode_row_key` frames an
    /// already-encoded handle, which is what Go's `EncodeRowKey` does.
    #[test]
    fn row_key_framing() {
        let handle = RecordHandle::Int(7);
        assert_eq!(
            encode_row_key(1, &handle.encoded()),
            tidb_codec::table_key::encode_row_key_with_handle(1, &handle)
        );
    }
}
