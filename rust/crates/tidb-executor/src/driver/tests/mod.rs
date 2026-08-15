//! The SQL-through-the-driver tests, grouped by the statement surface each
//! one exercises. Every test runs real SQL text end to end -- parse, plan,
//! execute over TiKV-format bytes -- so a group's name says which part of
//! that path it is holding still.
//!
//! This module owns only the two fixtures every group shares:
//! [`test_catalog`], the three-table catalog the queries run against, and
//! [`datum_text_for_test`], the printer an assertion compares against. The
//! assertions themselves live in the submodules.
//!
//! The groups mirror how Go splits `pkg/executor`'s own tests: statement
//! clauses, scan pushdown, aggregates, DML, joins, subqueries, key and index
//! behaviour, point gets, range scans, defaults, and set operations.

mod aggregates;
mod catalog_version;
mod column_defaults;
mod column_type_flags;
mod create_table_like;
mod dml;
mod index_prefix_lengths;
mod index_prefix_reads;
mod index_ranges;
mod indexes;
mod join_reorder;
mod joins;
mod mem_quota;
mod point_get;
mod predicate_pushdown;
mod primary_keys;
mod select_clauses;
mod set_operations;
mod subqueries;
mod table_round_trip;
mod through_proj;

use super::*;

fn test_catalog() -> Catalog {
    use tidb_datatype::FieldTypeCode;
    let mut catalog = Catalog::default();
    catalog.register(
        "t",
        MemTable {
            columns: vec![
                ("a".to_owned(), FieldType::new(FieldTypeCode::LongLong)),
                ("b".to_owned(), FieldType::new(FieldTypeCode::LongLong)),
            ],
            rows: vec![
                vec![Datum::Int(1), Datum::Int(30)],
                vec![Datum::Int(2), Datum::Int(20)],
                vec![Datum::Int(3), Datum::Int(10)],
            ],
        },
    );
    catalog
}

/// The text of a string datum, however the codec chose to represent it.
fn datum_text_for_test(value: &Datum) -> String {
    match value {
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        Datum::String(text) => String::from_utf8_lossy(text.bytes()).into_owned(),
        other => panic!("expected a string datum, got {other:?}"),
    }
}

/// Analyze representative TPCC rows, then replace the sample cardinalities
/// with the ten-warehouse statistics the Go planner reads.
///
/// The fixture rows provide histogram bounds, not a statistically valid
/// sample: scaling their two or three observed NDVs by millions of rows would
/// claim millions of warehouses and districts. Keeping the analyzed count
/// equal to the real-time count makes Go's `EstimateColumnNDV` increase factor
/// one, so the explicit column NDVs below remain the source statistics.
fn scale_analyzed_tpcc_table(
    catalog: &mut Catalog,
    table_name: &str,
    row_count: i64,
    column_ndvs: &[(&str, i64)],
    ctx: &crate::StmtContext,
) {
    fn scale_histogram(histogram: &mut tidb_stats::Histogram, row_count: i64, ndv: i64) {
        let original_total = histogram.total_row_count().max(1.0);
        let original_non_null = histogram.not_null_count().max(1.0);
        let null_count =
            ((histogram.null_count as f64 / original_total) * row_count as f64).round() as i64;
        let non_null_count = row_count.saturating_sub(null_count);
        let bucket_count = histogram.buckets.len() as i64;
        let bounded_ndv = ndv.max(1).min(non_null_count.max(1));
        let uniform_repeat = (non_null_count / bounded_ndv).max(1);
        let mut previous_count = 0;
        for (position, bucket) in histogram.buckets.iter_mut().enumerate() {
            let count = if position + 1 == bucket_count as usize {
                non_null_count
            } else {
                ((bucket.count as f64 / original_non_null) * non_null_count as f64).round() as i64
            }
            .max(previous_count)
            .min(non_null_count);
            let rows_in_bucket = count - previous_count;
            bucket.count = count;
            bucket.repeat = if rows_in_bucket == 0 {
                0
            } else {
                uniform_repeat.min(rows_in_bucket)
            };
            bucket.ndv = if bucket_count == 0 {
                0
            } else {
                bounded_ndv / bucket_count
                    + i64::from((position as i64) < bounded_ndv % bucket_count)
            };
            previous_count = count;
        }
        histogram.ndv = if non_null_count == 0 { 0 } else { bounded_ndv };
        histogram.null_count = null_count;
        histogram.tot_col_size =
            ((histogram.tot_col_size as f64 / original_total) * row_count as f64).round() as i64;
    }

    let (table_id, column_ndvs, indexes, mut statistics) = {
        let TableEntry::Kv(table) = catalog.get_mut_in("test", table_name).unwrap() else {
            panic!("{table_name} is not a KV table");
        };
        let table_id = table.table_id;
        let requested = column_ndvs
            .iter()
            .copied()
            .collect::<std::collections::BTreeMap<_, _>>();
        let column_ndvs = table
            .visible_columns()
            .iter()
            .filter_map(|column| {
                requested
                    .get(column.name.as_str())
                    .map(|ndv| (column.id, *ndv))
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(
            column_ndvs.len(),
            requested.len(),
            "unknown TPCC column NDV for {table_name}"
        );
        let indexes = table
            .indexes()
            .iter()
            .map(|index| {
                let columns = index
                    .column_offsets
                    .iter()
                    .filter_map(|offset| table.visible_columns().get(*offset))
                    .map(|column| column.id)
                    .collect::<Vec<_>>();
                (index.id, columns)
            })
            .collect::<Vec<_>>();
        let mut options = crate::analyze::AnalyzeOptions::default();
        options.num_topn = 0;
        let statistics = crate::analyze::kv::analyze_kv_table(table, &options, None, ctx).unwrap();
        (table_id, column_ndvs, indexes, statistics)
    };
    for (column_id, column) in &mut statistics.columns {
        let ndv = column_ndvs
            .get(column_id)
            .copied()
            .unwrap_or(column.histogram.ndv.max(1));
        scale_histogram(&mut column.histogram, row_count, ndv);
    }
    for (index_id, index_columns) in indexes {
        let index = statistics
            .indexes
            .get_mut(&index_id)
            .expect("analyzed TPCC index statistics");
        let index_ndv = index_columns
            .iter()
            .try_fold(1_i64, |ndv, column_id| {
                column_ndvs
                    .get(column_id)
                    .and_then(|column_ndv| ndv.checked_mul(*column_ndv))
            })
            .unwrap_or(row_count)
            .min(row_count)
            .max(1);
        scale_histogram(&mut index.histogram, row_count, index_ndv);
    }
    statistics.row_count = row_count;
    catalog.set_table_statistics(table_id, std::sync::Arc::new(statistics));
}
