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

//! Reading a table's statistics out of a cluster's `mysql.stats_*`.
//!
//! This is the supply line for [`tidb_stats::histogram`]: `ANALYZE TABLE` runs
//! on a Go node and writes rows; this reads those rows back and rebuilds the
//! `Histogram`/`TopN`/`CMSketch` triple the estimator consumes. Go source of
//! truth is `pkg/statistics/handle/storage/read.go`, whose
//! `HistogramFromStorageWithPriority`, `CMSketchAndTopNFromStorageWithHighPriority`,
//! and `StatsMetaCountAndModifyCount` this file follows statement for
//! statement, minus the `SELECT` — the rows come from the record range, the
//! way every `mysql.*` read on this node does (see
//! [`crate::mysql_system_tables`]).
//!
//! # The bound encoding, which is the whole correctness question
//!
//! `mysql.stats_buckets.lower_bound`/`upper_bound` are `LONGBLOB`, and what
//! those blobs hold depends on `is_index`. Go's writer is
//! `storage/save.go::convertBoundToBlob`, which is
//! `Datum.ConvertTo(TypeBlob)` — *not* the datum codec — and its reader is
//! `read.go::convertBoundFromBlob`. So:
//!
//! * **`is_index = 1`**: the histogram's values already *are* index-key bytes
//!   (`codec.EncodeKey` output). Converting those bytes to a blob is the
//!   identity, and Go reads them straight back as a blob without decoding.
//!   The bound stays raw bytes here too, because that is the domain every
//!   index-histogram comparison happens in.
//! * **`is_index = 0`**: the blob is the column value's *text* form — a
//!   `BIGINT` bound is stored as `"90"`, a `DECIMAL(10,2)` bound as `"11.00"`,
//!   a `DATETIME` bound as `"2024-05-05 05:05:05"`. Reading it back is a
//!   string-to-type conversion at the column's declared type. Verified
//!   against a live v8.5.7 cluster (see the fixtures in this module's tests,
//!   captured from its `mysql.stats_buckets`).
//!
//! Two exceptions Go carves out of that second rule, both reproduced here:
//!
//! * **String-evaluated columns** (but not `ENUM`/`SET`): Go replaces the
//!   target type with `TypeBlob` before converting. Under a new collation the
//!   stored bound is the *collation sort key*, not the original text, and it
//!   can be longer than the column's `flen` and not valid in the column's
//!   charset; converting at the declared type would truncate or reject it.
//!   The bound therefore stays raw bytes.
//! * **`BIT`**: converting `BIT` to a blob formats it as a decimal integer,
//!   so the blob is parsed back as one.

use std::collections::BTreeMap;

use tidb_datatype::{
    BinaryLiteral, BinaryLiteralWidth, ConversionFlags, Datum, EvalType, FieldType, FieldTypeCode,
};
use tidb_model::table_info::TableInfo;
use tidb_stats::cmsketch::{decode_cmsketch_and_topn, CmsSketch, TopN};
use tidb_stats::histogram::{Bucket, Histogram};

use crate::cluster_catalog::{ClusterCatalog, MetaSnapshot};
use crate::mysql_system_tables::{
    scan_system_table, scan_system_table_prefixed, SystemRow, SystemTableError, SystemTableView,
};

/// Go `statistics.UTCWithAllowInvalidDateCtx`, as conversion flags.
///
/// A stored bound is a value a previous `ANALYZE` already accepted; refusing
/// to read it back because it is, say, a zero date would lose the whole
/// histogram over a value the table legitimately contains.
const BOUND_CONVERSION_FLAGS: ConversionFlags = ConversionFlags::from_bits(
    ConversionFlags::ALLOW_NEGATIVE_TO_UNSIGNED
        | ConversionFlags::IGNORE_ZERO_DATE_ERR
        | ConversionFlags::IGNORE_ZERO_IN_DATE_ERR
        | ConversionFlags::IGNORE_INVALID_DATE_ERR,
);

/// One column's or one index's statistics, as one cluster snapshot holds them.
#[derive(Clone, Debug)]
pub struct ClusterStatsItem {
    /// `hist_id`: the column ID, or the index ID when [`Self::is_index`].
    pub id: i64,
    /// Whether this is an index histogram rather than a column histogram.
    pub is_index: bool,
    /// `stats_ver`: which `ANALYZE` generation wrote this.
    pub stats_ver: i64,
    /// `flag`, Go's `statistics.AnalyzeFlag` bitset.
    pub flag: i64,
    /// The histogram, with its buckets in ascending bound order and its
    /// counts made cumulative the way Go's in-memory `Bucket.Count` is.
    pub histogram: Histogram,
    /// `mysql.stats_top_n`'s rows, when any.
    pub topn: Option<TopN>,
    /// `mysql.stats_histograms.cm_sketch`, when it holds one.
    pub cms: Option<CmsSketch>,
}

/// Everything one cluster snapshot says about one table's statistics.
#[derive(Clone, Debug)]
pub struct ClusterTableStats {
    /// The physical table ID these rows key on.
    pub table_id: i64,
    /// `mysql.stats_meta.version`, the TSO the stats were last written at.
    pub version: u64,
    /// `mysql.stats_meta.modify_count`: rows changed since that write.
    pub modify_count: i64,
    /// `mysql.stats_meta.count`: the table's estimated row count.
    pub row_count: u64,
    /// Column histograms, in `hist_id` order.
    pub columns: Vec<ClusterStatsItem>,
    /// Index histograms, in `hist_id` order.
    pub indexes: Vec<ClusterStatsItem>,
}

impl ClusterTableStats {
    /// The column histogram for one column ID.
    #[must_use]
    pub fn column(&self, id: i64) -> Option<&ClusterStatsItem> {
        self.columns.iter().find(|item| item.id == id)
    }

    /// The index histogram for one index ID.
    #[must_use]
    pub fn index(&self, id: i64) -> Option<&ClusterStatsItem> {
        self.indexes.iter().find(|item| item.id == id)
    }
}

/// The declared type of each column a table's column histograms are keyed by.
///
/// A column histogram's `hist_id` is the stored column ID, and the bound
/// decode needs that column's declared type — so this is exactly the map a
/// caller must hand [`ClusterStatsLoader::load_table`], and building it from
/// the analyzed table's own `TableInfo` is how a caller gets the cluster's
/// types rather than a guess at them.
#[must_use]
pub fn column_types_of(table: &TableInfo) -> BTreeMap<i64, FieldType> {
    table
        .cols()
        .into_iter()
        .map(|column| (column.id, column.field_type.clone()))
        .collect()
}

/// `mysql.stats_top_n`'s `(value, count)` rows, keyed by
/// `(is_index, hist_id)` — the pair that names one histogram within a table.
type TopNRowsByHistogram = BTreeMap<(bool, i64), Vec<(Vec<u8>, u64)>>;

/// The `mysql.stats_*` tables located once in a loaded catalog.
#[derive(Clone, Debug)]
pub struct ClusterStatsLoader {
    meta: SystemTableView,
    histograms: SystemTableView,
    buckets: SystemTableView,
    topn: SystemTableView,
}

impl ClusterStatsLoader {
    /// Locates the statistics tables in one loaded catalog.
    pub fn locate(catalog: &ClusterCatalog) -> Result<Self, SystemTableError> {
        Ok(Self {
            meta: SystemTableView::locate(
                catalog,
                "stats_meta",
                &["table_id", "version", "modify_count", "count"],
            )?,
            histograms: SystemTableView::locate(
                catalog,
                "stats_histograms",
                &[
                    "table_id",
                    "is_index",
                    "hist_id",
                    "distinct_count",
                    "null_count",
                    "tot_col_size",
                    "version",
                    "cm_sketch",
                    "stats_ver",
                    "flag",
                    "correlation",
                ],
            )?,
            buckets: SystemTableView::locate(
                catalog,
                "stats_buckets",
                &[
                    "table_id",
                    "is_index",
                    "hist_id",
                    "bucket_id",
                    "count",
                    "repeats",
                    "lower_bound",
                    "upper_bound",
                    "ndv",
                ],
            )?,
            topn: SystemTableView::locate(
                catalog,
                "stats_top_n",
                &["table_id", "is_index", "hist_id", "value", "count"],
            )?,
        })
    }

    /// Reads every histogram one table has, at one snapshot.
    ///
    /// `None` means the table has no `mysql.stats_meta` row at all — it was
    /// never analyzed and never had a row-count delta flushed — which is not
    /// the same as an analyzed table with no histograms, and the planner must
    /// be able to tell the two apart before it falls back to pseudo stats.
    pub fn load_table<S: MetaSnapshot>(
        &self,
        snapshot: &mut S,
        table_id: i64,
        column_types: &BTreeMap<i64, FieldType>,
    ) -> Result<Option<ClusterTableStats>, SystemTableError> {
        let Some((version, modify_count, row_count)) = self.load_meta(snapshot, table_id)? else {
            return Ok(None);
        };
        let buckets = self.load_buckets(snapshot, table_id, column_types)?;
        let topn = self.load_topn(snapshot, table_id)?;

        let mut stats = ClusterTableStats {
            table_id,
            version,
            modify_count,
            row_count,
            columns: Vec::new(),
            indexes: Vec::new(),
        };
        let key_prefix = [Datum::Int(table_id)];
        for (key, value) in scan_system_table_prefixed(snapshot, &self.histograms, &key_prefix)? {
            let row = SystemRow::parse(&self.histograms, &key, &value)?;
            if row.i64("table_id")?.unwrap_or_default() != table_id {
                continue;
            }
            let is_index = row.i64("is_index")?.unwrap_or_default() != 0;
            let id = row.i64("hist_id")?.unwrap_or_default();
            let mut histogram = Histogram {
                id,
                ndv: row.i64("distinct_count")?.unwrap_or_default(),
                null_count: row.i64("null_count")?.unwrap_or_default(),
                last_update_version: row.u64("version")?.unwrap_or_default(),
                tot_col_size: row.i64("tot_col_size")?.unwrap_or_default(),
                correlation: row.f64("correlation")?.unwrap_or_default(),
                buckets: buckets.get(&(is_index, id)).cloned().unwrap_or_default(),
            };
            // Go stores `tot_col_size`/`correlation` only for columns and
            // passes literal zeros for an index (`read.go::LoadHistogram`).
            if is_index {
                histogram.tot_col_size = 0;
                histogram.correlation = 0.0;
            }
            let (cms, top) = decode_cmsketch_and_topn(
                row.bytes("cm_sketch")?.as_deref(),
                topn.get(&(is_index, id)).map_or(&[][..], Vec::as_slice),
            )
            .map_err(|error| SystemTableError::Decode {
                name: self.histograms.name().to_owned(),
                detail: error.to_string(),
            })?;
            let item = ClusterStatsItem {
                id,
                is_index,
                stats_ver: row.i64("stats_ver")?.unwrap_or_default(),
                flag: row.i64("flag")?.unwrap_or_default(),
                histogram,
                topn: top,
                cms,
            };
            if is_index {
                stats.indexes.push(item);
            } else {
                stats.columns.push(item);
            }
        }
        stats.columns.sort_by_key(|item| item.id);
        stats.indexes.sort_by_key(|item| item.id);
        Ok(Some(stats))
    }

    /// Go `StatsMetaCountAndModifyCount`.
    fn load_meta<S: MetaSnapshot>(
        &self,
        snapshot: &mut S,
        table_id: i64,
    ) -> Result<Option<(u64, i64, u64)>, SystemTableError> {
        // `mysql.stats_meta` declares `PRIMARY KEY (table_id) CLUSTERED`, so
        // this prefix is the row's exact key: one point read, not a scan.
        let rows = scan_system_table_prefixed(snapshot, &self.meta, &[Datum::Int(table_id)])?;
        for (key, value) in rows {
            let row = SystemRow::parse(&self.meta, &key, &value)?;
            if row.i64("table_id")?.unwrap_or_default() != table_id {
                continue;
            }
            return Ok(Some((
                row.u64("version")?.unwrap_or_default(),
                row.i64("modify_count")?.unwrap_or_default(),
                row.u64("count")?.unwrap_or_default(),
            )));
        }
        Ok(None)
    }

    /// Reads one table's buckets, grouped by `(is_index, hist_id)` and left in
    /// `bucket_id` order with cumulative counts.
    fn load_buckets<S: MetaSnapshot>(
        &self,
        snapshot: &mut S,
        table_id: i64,
        column_types: &BTreeMap<i64, FieldType>,
    ) -> Result<BTreeMap<(bool, i64), Vec<Bucket>>, SystemTableError> {
        // `(bucket_id, lower, upper, repeats, ndv, per-bucket count)` keyed by
        // histogram; `bucket_id` leads so the sort restores Go's
        // `order by bucket_id`, which is what makes the running total below
        // reproduce Go's cumulative `Bucket.Count`.
        let mut collected: BTreeMap<(bool, i64), Vec<(i64, Bucket)>> = BTreeMap::new();
        let key_prefix = [Datum::Int(table_id)];
        for (key, value) in scan_system_table_prefixed(snapshot, &self.buckets, &key_prefix)? {
            let row = SystemRow::parse(&self.buckets, &key, &value)?;
            if row.i64("table_id")?.unwrap_or_default() != table_id {
                continue;
            }
            let is_index = row.i64("is_index")?.unwrap_or_default() != 0;
            let id = row.i64("hist_id")?.unwrap_or_default();
            let field_type = column_types.get(&id);
            let lower = decode_bound(
                row.bytes("lower_bound")?.unwrap_or_default(),
                is_index,
                field_type,
                self.buckets.name(),
            )?;
            let upper = decode_bound(
                row.bytes("upper_bound")?.unwrap_or_default(),
                is_index,
                field_type,
                self.buckets.name(),
            )?;
            collected.entry((is_index, id)).or_default().push((
                row.i64("bucket_id")?.unwrap_or_default(),
                Bucket {
                    count: row.i64("count")?.unwrap_or_default(),
                    repeat: row.i64("repeats")?.unwrap_or_default(),
                    ndv: row.i64("ndv")?.unwrap_or_default(),
                    lower_bound: lower,
                    upper_bound: upper,
                },
            ));
        }
        Ok(collected
            .into_iter()
            .map(|(histogram, mut rows)| {
                rows.sort_by_key(|(bucket_id, _)| *bucket_id);
                // The stored `count` is the bucket's *own* row count
                // (`save.go` subtracts the previous bucket's cumulative count
                // before writing), so reading it back means adding them up
                // again. A loader that skipped this would report the last
                // bucket as holding one bucket's worth of rows and make every
                // range estimate over the histogram far too small.
                let mut running = 0_i64;
                let buckets = rows
                    .into_iter()
                    .map(|(_, mut bucket)| {
                        running += bucket.count;
                        bucket.count = running;
                        bucket
                    })
                    .collect();
                (histogram, buckets)
            })
            .collect())
    }

    /// Reads one table's TopN rows, grouped by `(is_index, hist_id)`.
    fn load_topn<S: MetaSnapshot>(
        &self,
        snapshot: &mut S,
        table_id: i64,
    ) -> Result<TopNRowsByHistogram, SystemTableError> {
        // `mysql.stats_top_n` has no clustered handle — only a secondary
        // `INDEX tbl(table_id, is_index, hist_id)` — so its record range
        // cannot be narrowed by key here and every row is read and filtered.
        // That is a real cost on a cluster with many analyzed tables, and the
        // honest fix is an index read, not a narrower prefix that does not
        // exist.
        let mut collected = TopNRowsByHistogram::new();
        for (key, value) in scan_system_table(snapshot, &self.topn)? {
            let row = SystemRow::parse(&self.topn, &key, &value)?;
            if row.i64("table_id")?.unwrap_or_default() != table_id {
                continue;
            }
            let is_index = row.i64("is_index")?.unwrap_or_default() != 0;
            let id = row.i64("hist_id")?.unwrap_or_default();
            collected.entry((is_index, id)).or_default().push((
                row.bytes("value")?.unwrap_or_default(),
                row.u64("count")?.unwrap_or_default(),
            ));
        }
        Ok(collected)
    }
}

/// Go `read.go::convertBoundFromBlob`, plus the `is_index`/string-type
/// branches its caller `HistogramFromStorageWithPriority` wraps it in.
///
/// See this module's doc for why each branch exists.
fn decode_bound(
    stored: Vec<u8>,
    is_index: bool,
    field_type: Option<&FieldType>,
    table: &str,
) -> Result<Datum, SystemTableError> {
    // An index bound is already index-key bytes, and a column whose type the
    // caller could not name is one this node has no conversion target for;
    // in both cases the stored bytes are the most faithful answer available.
    let Some(field_type) = field_type.filter(|_| !is_index) else {
        return Ok(Datum::Bytes(stored));
    };
    match field_type.code() {
        // Converting BIT to a blob formats it as a decimal integer, so that
        // is what comes back.
        FieldTypeCode::Bit => {
            let text = String::from_utf8_lossy(&stored);
            let Ok(mut value) = text.parse::<u64>() else {
                // Go falls back to the blob's own bytes as the literal.
                return Ok(Datum::BinaryLiteral(BinaryLiteral::from(stored)));
            };
            let flen = field_type.flen();
            // Go saturates a bound wider than the column rather than dropping
            // the histogram; a `BIT(n)` value can only ever be `2^n - 1`.
            if (1..64).contains(&flen) && value >= 1 << flen {
                value = (1 << flen) - 1;
            }
            let byte_size = ((flen.clamp(1, 64) + 7) >> 3) as u8;
            let width = BinaryLiteralWidth::try_from(byte_size).map_err(|error| {
                SystemTableError::Decode {
                    name: table.to_owned(),
                    detail: format!("a BIT bound has an impossible width: {error}"),
                }
            })?;
            Ok(Datum::Bit(BinaryLiteral::from_uint(value, Some(width))))
        }
        // Go swaps the target for `TypeBlob` here: under a new collation the
        // stored bound is a collation sort key, which the declared string type
        // would truncate or reject.
        code if field_type.eval_type() == EvalType::String
            && code != FieldTypeCode::Enum
            && code != FieldTypeCode::Set =>
        {
            Ok(Datum::Bytes(stored))
        }
        _ => Datum::Bytes(stored)
            .convert_to(field_type, BOUND_CONVERSION_FLAGS)
            .map(|converted| converted.value)
            .map_err(|error| SystemTableError::Decode {
                name: table.to_owned(),
                detail: format!("a histogram bound did not convert: {error}"),
            }),
    }
}

#[cfg(test)]
mod tests {
    use tidb_datatype::FieldTypeCode;

    use super::*;

    /// Every bound fixture below is the exact `mysql.stats_buckets` blob a
    /// live v8.5.7 playground wrote for `ANALYZE TABLE` over a known
    /// distribution, read back with `hex(lower_bound)`. They are the reason
    /// this module claims to know the encoding rather than assume it.
    fn field_type(code: FieldTypeCode) -> FieldType {
        FieldType::new(code)
    }

    #[test]
    fn a_column_bound_is_the_values_text_form_not_its_datum_encoding() {
        // `id BIGINT`: the cluster stored bucket 2's bounds as "11" and "12".
        let bound = decode_bound(
            b"11".to_vec(),
            false,
            Some(&field_type(FieldTypeCode::LongLong)),
            "t",
        )
        .expect("the bound converts");
        assert_eq!(bound, Datum::Int(11));
        // Read as a datum-codec value instead, `0x31 0x31` would be a
        // nonsense flag byte — which is exactly the silent misread this
        // fixture exists to rule out.
    }

    #[test]
    fn a_decimal_bound_keeps_its_stored_scale() {
        // `c DECIMAL(10,2)`: stored as "11.00", not as 1100 or 11.
        let mut decimal = field_type(FieldTypeCode::NewDecimal);
        decimal.set_flen(10);
        decimal.set_decimal(2);
        let bound = decode_bound(b"11.00".to_vec(), false, Some(&decimal), "t")
            .expect("the bound converts");
        assert_eq!(bound.sql_string().unwrap(), "11.00");
    }

    #[test]
    fn a_datetime_bound_parses_back_from_its_printed_form() {
        // `d DATETIME`: stored as "2024-05-05 05:05:05".
        let bound = decode_bound(
            b"2024-05-05 05:05:05".to_vec(),
            false,
            Some(&field_type(FieldTypeCode::Datetime)),
            "t",
        )
        .expect("the bound converts");
        assert_eq!(bound.sql_string().unwrap(), "2024-05-05 05:05:05");
    }

    #[test]
    fn a_string_column_bound_stays_raw_bytes_because_it_may_be_a_collation_key() {
        // `b VARCHAR(32)`: the cluster stored "charlie". Under a new
        // collation the same slot holds a sort key that the declared VARCHAR
        // would truncate, so neither is converted.
        let mut varchar = field_type(FieldTypeCode::Varchar);
        varchar.set_flen(32);
        let bound = decode_bound(b"charlie".to_vec(), false, Some(&varchar), "t")
            .expect("the bound converts");
        assert_eq!(bound, Datum::Bytes(b"charlie".to_vec()));
    }

    #[test]
    fn an_index_bound_stays_the_index_key_bytes_the_cluster_stored() {
        // Index `ia(a)`, bucket 0 upper bound, exactly as the cluster wrote
        // it: `0x03` is the datum codec's INT flag and the eight bytes are
        // 50 with its sign bit flipped. Converting this at column `a`'s INT
        // type would read the flag byte as text and produce garbage, so an
        // index bound is never converted.
        let stored = vec![0x03, 0x80, 0, 0, 0, 0, 0, 0, 0x32];
        let bound = decode_bound(
            stored.clone(),
            true,
            Some(&field_type(FieldTypeCode::Long)),
            "t",
        )
        .expect("an index bound never converts");
        assert_eq!(bound, Datum::Bytes(stored));
    }

    #[test]
    fn a_bound_for_a_column_the_caller_could_not_type_stays_bytes() {
        let bound = decode_bound(b"90".to_vec(), false, None, "t").expect("the bound is kept");
        assert_eq!(bound, Datum::Bytes(b"90".to_vec()));
    }
}
