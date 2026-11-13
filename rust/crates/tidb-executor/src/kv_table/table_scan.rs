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

//! Reading a stored table back: the record-key/value decoder, the cursors
//! that walk a key range, and the [`TableScanExec`] executor those feed.
//!
//! Inside: [`capture_decoded_column_ids`], the per-thread probe that makes
//! column pruning checkable; [`RowCursor`] and
//! [`IndexRangeCursor`] over the in-process store, [`RemoteRowCursor`] over a
//! coprocessor stream; and [`TableScanExec`], which turns any of them into
//! chunk rows and also carries the `TableAccess` plan surface (pruning,
//! pushdown, index choice).
//!
//! Mirrors Go `pkg/executor`'s table reader over `pkg/tablecodec`'s
//! `DecodeRowToDatumMap`/`DecodeRecordKey`: the writes live with the table in
//! the parent module, and this file is only the read direction.

use super::row_decoder::{PreparedPointGetRowDecoder, RowDecoder};
use tidb_ranger::types::Range;
use super::{
    index_entry_handle, Index, TableCommon, KvTableError, PreparedPointGetDecodeContext,
    RowDecodeContext, TableHandle,
};

use tidb_kv::storage::{StorageIterator, TableStorage};
use std::collections::BinaryHeap;
use tidb_tablecodec::table_key::{
    cut_index_prefix, cut_row_key_prefix, decode_table_id, encode_index_seek_key,
    encode_row_key_with_handle, get_table_handle_key_range, RecordHandle,
};
use tidb_codec::Encoder;
use tidb_datatype::{Datum, SessionTimeZone};
use tidb_txnkv::Key;

/// The read direction of [`TableCommon`]: the entry points that open a decoder
/// or a cursor over the stored bytes.
/// Go's reader executors over a table (`TableReader`, `IndexReader`, `IndexLookUp`, `PointGet`): the executor's reads of a [`TableCommon`].
pub trait KvTableReader {
    /// Legacy zone-only point read retained for unmigrated DML/FK callers.
    /// Origin defaults use the exact former `DEFAULT_STATEMENT_FLAGS` behavior.
    fn get_row_by_handle(
        &mut self,
        store: &mut dyn TableStorage,
        handle: &TableHandle,
        zone: &SessionTimeZone,
    ) -> Result<Option<Vec<Datum>>, KvTableError>;

    /// The row-decoding metadata, detached from the table and optionally
    /// narrowed to the columns at `keep` (offsets into [`TableCommon::columns`],
    /// ascending and unique).
    ///
    /// A cursor holds the storage iterator and must decode without borrowing
    /// the table it came from, so it carries its own copy of everything
    /// decoding reads. The copy is a snapshot of the schema and statement
    /// context at cursor-open time. The row codec is asked for only the kept
    /// columns' ids, so an unreferenced column is never decoded; `None` keeps
    /// the whole schema.
    fn row_decoder_projected(
        &self,
        keep: Option<&[usize]>,
        context: &RowDecodeContext,
    ) -> Result<RowDecoder, KvTableError>;

    fn row_decoder_recomputed(
        &self,
        context: &RowDecodeContext,
    ) -> Result<RowDecoder, KvTableError>;

    /// A forward cursor over the table's record-key range, in key order.
    ///
    /// This is the streaming form of [`TableCommon::scan_rows_with_handles_with_context`]: the
    /// storage iterator stays open and one row is *decoded* per pull, so the
    /// decoded rows alive at once are the caller's chunk rather than the whole
    /// relation. (How far the laziness reaches below the storage seam is a
    /// property of the backend's `iter`; see [`crate::access_path`].) The seam
    /// returns an owned iterator, so the cursor does not borrow the table and
    /// a caller may hold it across chunk boundaries.
    fn row_cursor_with_context(
        &mut self,
        store: &mut dyn TableStorage,
        context: &RowDecodeContext,
    ) -> Result<RowCursor, KvTableError>;

    /// [`TableCommon::row_cursor_with_context`] narrowed to the columns at `keep`: the cursor
    /// decodes and yields exactly those columns, in `keep`'s order.
    ///
    /// `handle_ranges` narrows which RECORDS are read, as
    /// [`tidb_executor_internal_exec::table_access::TableAccess::accept_handle_ranges`] describes:
    /// `None` reads the whole table.
    fn row_cursor_projected_with_context(
        &mut self,
        store: &mut dyn TableStorage,
        keep: Option<&[usize]>,
        handle_ranges: Option<&[Range]>,
        context: &RowDecodeContext,
    ) -> Result<RowCursor, KvTableError>;

    /// [`Self::row_cursor_projected_with_context`] with a walk direction:
    /// `descending` reads the ranges last-to-first, each backwards -- the
    /// local half of Go's `desc` table scan.
    fn row_cursor_projected_directed_with_context(
        &mut self,
        store: &mut dyn TableStorage,
        keep: Option<&[usize]>,
        handle_ranges: Option<&[Range]>,
        descending: bool,
        ordered: bool,
        context: &RowDecodeContext,
    ) -> Result<RowCursor, KvTableError>;

    /// Looks up several record handles through one storage batch and decodes
    /// them with one row decoder. The returned slots preserve input handle
    /// order and use `None` for an index entry whose record disappeared.
    fn get_rows_by_handles_projected_with_context(
        &mut self,
        store: &mut dyn TableStorage,
        handles: &[TableHandle],
        keep: Option<&[usize]>,
        context: &RowDecodeContext,
    ) -> Result<Vec<Option<Vec<Datum>>>, KvTableError>;

    /// Decodes one record per handle in its known physical keyspace. The
    /// partition IDs come from the index entry, not from probing for a match.
    fn get_rows_by_routed_handles_projected_with_context(
        &mut self,
        store: &mut dyn TableStorage,
        handles: &[TableHandle],
        routes: Option<&[i64]>,
        keep: Option<&[usize]>,
        context: &RowDecodeContext,
    ) -> Result<Vec<Option<Vec<Datum>>>, KvTableError>;

    /// [`Self::get_rows_by_routed_handles_projected_with_context`] that also
    /// answers the physical table each found row was read from, so a reader
    /// reporting Go's `_tidb_tid` knows the partition of a handle it probed
    /// for in every partition. A slot with no row has no physical id.
    fn get_located_rows_by_routed_handles_projected_with_context(
        &mut self,
        store: &mut dyn TableStorage,
        handles: &[TableHandle],
        routes: Option<&[i64]>,
        keep: Option<&[usize]>,
        context: &RowDecodeContext,
    ) -> Result<(Vec<Option<Vec<Datum>>>, Vec<Option<i64>>), KvTableError>;

    fn row_cursor_with_decoder(
        &mut self,
        store: &mut dyn TableStorage,
        decoder: RowDecoder,
        handle_ranges: Option<&[Range]>,
        descending: bool,
        ordered: bool,
        zone: &SessionTimeZone,
    ) -> Result<RowCursor, KvTableError>;

    /// The record ranges this scan reads, as the storage seam's half-open
    /// `[start, end)` pairs in ascending key order.
    ///
    /// With no handle ranges this is the ONE range the whole relation lives
    /// in ([`TableCommon::record_key_range`]). With them it is the intervals
    /// [`crate::handle_range::record_key_ranges`] encodes, which is what
    /// makes a `TableRangeScan` read less than the table. `keep_order`
    /// selects Go's ordered-read half order versus his merged unordered wire
    /// order, exactly as [`crate::handle_range::record_key_ranges`] documents.
    fn record_key_ranges(
        &self,
        handle_ranges: Option<&[Range]>,
        zone: &SessionTimeZone,
        keep_order: bool,
    ) -> Result<Vec<(Key, Key)>, KvTableError>;

    /// The same intervals as [`Self::record_key_ranges], split into the
    /// VALUE-ordered halves an ORDERED reader consumes one after the other --
    /// Go's `firstPartGroupedRanges then `secondPartGroupedRanges
    /// (`table_reader.go). An unsigned handle whose domain wraps the int64
    /// boundary yields `[signed_half, unsigned_half]; a descending read
    /// reverses the pair, exactly Go's `desc = true answer. Every other
    /// shape is one group, and an unencodable bound falls back to the whole
    /// record range as one group, reading a correct superset.
    fn record_key_range_groups(
        &self,
        handle_ranges: Option<&[Range]>,
        zone: &SessionTimeZone,
        descending: bool,
    ) -> Result<Vec<Vec<(Key, Key)>>, KvTableError>;

    /// The record ranges this table's rows live in, as the storage seam's
    /// half-open `[start, end)` pairs in ascending key order.
    ///
    /// One range PER physical id it reads. An unpartitioned table and a
    /// whole-table scan of a partitioned one both give a contiguous block --
    /// `CREATE TABLE` allocates the partition ids together, and this table's
    /// index entries sit under the (smaller) table id, below the block
    /// entirely -- but a PRUNED or explicitly SELECTED read gives a set with
    /// holes in it, and the holes are the whole point: a single spanning
    /// range would read the partitions pruning just proved cannot match.
    fn record_key_range(&self) -> Vec<(Key, Key)>;











    /// Scans the table's record-key range in key order, decoding each value.
    /// Returns rows as `Datum`s in schema order (a missing column decodes
    /// NULL, and the handle columns come from the key).
    #[cfg(test)]
    fn scan_rows_with_context(
        &mut self,
        store: &mut dyn TableStorage,
        context: &RowDecodeContext,
    ) -> Result<Vec<Vec<Datum>>, KvTableError>;

    /// Like [`TableCommon::scan_rows_with_context`], but each row carries the record handle its
    /// key encodes, which `UPDATE`/`DELETE` need to address the row again.
    ///
    /// Uses the backend's table reader, merging staged writes, or the local
    /// [`RowCursor`] when the backend cannot serve the table's shape. A caller
    /// that does not need the whole relation in memory should hold a cursor
    /// instead (see [`TableCommon::row_cursor_with_context`]).
    #[cfg(test)]
    fn scan_rows_with_handles_with_context(
        &mut self,
        store: &mut dyn TableStorage,
        context: &RowDecodeContext,
    ) -> Result<Vec<(TableHandle, Vec<Datum>)>, KvTableError>;



    /// [`TableCommon::scan_rows_with_handles_with_context`] narrowed to `handle_ranges`: the
    /// same intervals the read side offers a `TableRangeScan` through
    /// [`tidb_executor_internal_exec::table_access::TableAccess::accept_handle_ranges`]. `None`
    /// reads the whole table.
    ///
    /// This is the WRITE path's form of that narrowing, and it narrows only
    /// WHICH RECORDS ARE FETCHED: the ranges are a superset of the rows the
    /// `WHERE` admits, and the caller still evaluates that `WHERE` per row, so
    /// the set of rows a statement acts on is the same set the full scan
    /// produced.
    #[cfg(test)]
    fn scan_rows_with_handles_in_with_context(
        &mut self,
        store: &mut dyn TableStorage,
        handle_ranges: Option<&[Range]>,
        context: &RowDecodeContext,
    ) -> Result<Vec<(TableHandle, Vec<Datum>)>, KvTableError>;


    /// Go `sampleFetcher.run` over one sampled range: the first record of
    /// the read snapshot in `[start, end)`, scanned forward whatever the
    /// sample's order, decoded with generated columns recomputed.
    fn first_snapshot_row_in_range(
        &mut self,
        store: &mut dyn TableStorage,
        start: &[u8],
        end: &[u8],
        context: &RowDecodeContext,
    ) -> Result<Option<(TableHandle, Vec<Datum>)>, KvTableError>;

    /// The handles an index range covers, in index order.
    ///
    /// Go turns a range into a key interval in `IndexRangesToKVRanges`: the
    /// low key is the encoded low bound, advanced to its `PrefixNext` when the
    /// bound is excluded; the high key is the encoded high bound, advanced to
    /// its `PrefixNext` when the bound is INCLUDED, because the scan's upper
    /// end is exclusive. This walks that interval and reads the handle out of
    /// each entry.
    fn scan_index_range(
        &mut self,
        store: &mut dyn TableStorage,
        index_id: i64,
        range: &Range,
        zone: &SessionTimeZone,
    ) -> Result<Vec<TableHandle>, KvTableError>;

    /// A forward cursor over one index range, in index order -- the streaming
    /// form of [`TableCommon::scan_index_range`].
    fn index_range_cursor(
        &mut self,
        store: &mut dyn TableStorage,
        index_id: i64,
        range: &Range,
        zone: &SessionTimeZone,
    ) -> Result<IndexRangeCursor, KvTableError>;

    /// A cursor over one index range in the physical scan direction Go put
    /// on `PhysicalIndexScan`.
    fn index_range_cursor_with_direction(
        &mut self,
        store: &mut dyn TableStorage,
        index_id: i64,
        range: &Range,
        zone: &SessionTimeZone,
        descending: bool,
        ordered: bool,
    ) -> Result<IndexRangeCursor, KvTableError>;




    fn index_ranges_cursor_for_physical_ids(
        &mut self,
        store: &mut dyn TableStorage,
        index_id: i64,
        ranges: &[Range],
        zone: &SessionTimeZone,
        physical_ids: &[i64],
        descending: bool,
        ordered: bool,
    ) -> Result<IndexRangeCursor, KvTableError>;
}

impl KvTableReader for TableCommon {
    fn get_row_by_handle(
        &mut self,
        store: &mut dyn TableStorage,
        handle: &TableHandle,
        zone: &SessionTimeZone,
    ) -> Result<Option<Vec<Datum>>, KvTableError> {
        self.read_row(store, handle, &RowDecodeContext::legacy_default(zone))
    }

    fn row_decoder_projected(
        &self,
        keep: Option<&[usize]>,
        context: &RowDecodeContext,
    ) -> Result<RowDecoder, KvTableError> {
        let decoder = RowDecoder::for_table_read(
            self.columns_arc().clone(),
            self.pk_handle_offset(),
            self.common_handle_offsets().to_vec(),
            self.common_handle_prefix_lengths().to_vec(),
            keep,
            self.use_new_collation(),
            context.clone(),
        )?;
        note_decoded_column_ids(decoder.decoded_column_ids());
        Ok(decoder)
    }

    fn row_decoder_recomputed(
        &self,
        context: &RowDecodeContext,
    ) -> Result<RowDecoder, KvTableError> {
        RowDecoder::for_recomputed_read(
            self.columns_arc().clone(),
            self.pk_handle_offset(),
            self.common_handle_offsets().to_vec(),
            self.common_handle_prefix_lengths().to_vec(),
            self.use_new_collation(),
            context.clone(),
        )
    }

    fn row_cursor_with_context(
        &mut self,
        store: &mut dyn TableStorage,
        context: &RowDecodeContext,
    ) -> Result<RowCursor, KvTableError> {
        self.row_cursor_projected_with_context(store, None, None, context)
    }

    fn row_cursor_projected_with_context(
        &mut self,
        store: &mut dyn TableStorage,
        keep: Option<&[usize]>,
        handle_ranges: Option<&[Range]>,
        context: &RowDecodeContext,
    ) -> Result<RowCursor, KvTableError> {
        self.row_cursor_projected_directed_with_context(store, keep, handle_ranges, false, false, context)
    }

    fn row_cursor_projected_directed_with_context(
        &mut self,
        store: &mut dyn TableStorage,
        keep: Option<&[usize]>,
        handle_ranges: Option<&[Range]>,
        descending: bool,
        ordered: bool,
        context: &RowDecodeContext,
    ) -> Result<RowCursor, KvTableError> {
        let decoder = self.row_decoder_projected(keep, context)?;
        self.row_cursor_with_decoder(store, decoder, handle_ranges, descending, ordered, context.zone())
    }

    fn get_rows_by_handles_projected_with_context(
        &mut self,
        store: &mut dyn TableStorage,
        handles: &[TableHandle],
        keep: Option<&[usize]>,
        context: &RowDecodeContext,
    ) -> Result<Vec<Option<Vec<Datum>>>, KvTableError> {
        self.get_rows_by_routed_handles_projected_with_context(store, handles, None, keep, context)
    }

    fn get_rows_by_routed_handles_projected_with_context(
        &mut self,
        store: &mut dyn TableStorage,
        handles: &[TableHandle],
        routes: Option<&[i64]>,
        keep: Option<&[usize]>,
        context: &RowDecodeContext,
    ) -> Result<Vec<Option<Vec<Datum>>>, KvTableError> {
        self.get_located_rows_by_routed_handles_projected_with_context(store, 
            handles, routes, keep, context,
        )
        .map(|(rows, _)| rows)
    }

    fn get_located_rows_by_routed_handles_projected_with_context(
        &mut self,
        store: &mut dyn TableStorage,
        handles: &[TableHandle],
        routes: Option<&[i64]>,
        keep: Option<&[usize]>,
        context: &RowDecodeContext,
    ) -> Result<(Vec<Option<Vec<Datum>>>, Vec<Option<i64>>), KvTableError> {
        if routes.is_some_and(|ids| ids.len() != handles.len()) {
            return Err(KvTableError::Decode(
                "index lookup partition routes lost alignment".to_owned(),
            ));
        }
        if handles.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }
        let physical_ids = self.record_physical_ids();
        if physical_ids.is_empty() {
            return Ok((vec![None; handles.len()], vec![None; handles.len()]));
        }
        let candidates = if routes.is_some() {
            1
        } else {
            physical_ids.len()
        };
        let mut keys = Vec::with_capacity(handles.len() * candidates);
        for (position, handle) in handles.iter().enumerate() {
            let record_handle = handle.record_handle();
            let ids = routes.map_or(physical_ids.as_slice(), |ids| &ids[position..position + 1]);
            for physical_id in ids {
                keys.push(Key::from_bytes(encode_row_key_with_handle(
                    *physical_id,
                    &record_handle,
                )));
            }
        }
        let entries = store.batch_get(&keys).map_err(KvTableError::from)?;
        // Each handle owns a contiguous window of physical keys. Preserve
        // handle order and the first matching partition without a second
        // key/handle collection or a tree lookup for every output row.
        // The ordinary index-lookup shape has no generated or origin-default
        // columns. Use the same projection-only decoder as Go's prepared
        // point path so each returned record avoids rebuilding the general
        // RowDecoder's dependency maps and full table row.
        let prepared = self
            .pk_handle_offset()
            .filter(|_| self.common_handle_offsets().is_empty())
            .and_then(|pk_offset| {
                let keep = keep?;
                if keep.iter().any(|offset| {
                    self.columns()[*offset].generated.is_some()
                        || self.columns()[*offset].origin_default.is_some()
                }) {
                    return None;
                }
                PreparedPointGetRowDecoder::new_with_handles(
                    &self.columns(),
                    Some(pk_offset),
                    &[],
                    keep,
                )
                .ok()
            });
        let prepared_context = prepared
            .as_ref()
            .map(|_| PreparedPointGetDecodeContext::for_query(false, context.zone().clone()));
        let decoder = if prepared.is_none() {
            Some(self.row_decoder_projected(keep, context)?)
        } else {
            None
        };
        let mut rows = Vec::with_capacity(handles.len());
        let mut located = Vec::with_capacity(handles.len());
        for (handle_index, keys) in keys.chunks_exact(candidates).enumerate() {
            let entry = if candidates == 1 {
                entries.get(&keys[0]).map(|entry| (0, &keys[0], entry))
            } else {
                keys.iter().enumerate().find_map(|(candidate, key)| {
                    entries.get(key).map(|entry| (candidate, key, entry))
                })
            };
            let Some((candidate, key, entry)) = entry else {
                rows.push(None);
                located.push(None);
                continue;
            };
            located.push(Some(match routes {
                Some(ids) => ids[handle_index],
                None => physical_ids[candidate],
            }));
            let values = if let (Some(prepared), Some(prepared_context)) =
                (prepared.as_ref(), prepared_context.as_ref())
            {
                prepared.decode(&handles[handle_index], entry, prepared_context)?
            } else {
                decoder
                    .as_ref()
                    .expect("general decoder exists when prepared decoder is absent")
                    .decode_record(key.as_bytes(), entry)?
                    .1
            };
            rows.push(Some(values));
        }
        Ok((rows, located))
    }

    fn row_cursor_with_decoder(
        &mut self,
        store: &mut dyn TableStorage,
        decoder: RowDecoder,
        handle_ranges: Option<&[Range]>,
        descending: bool,
        ordered: bool,
        zone: &SessionTimeZone,
    ) -> Result<RowCursor, KvTableError> {
        // Go's `byItems` half of `needMergeSort` is set because the table is
        // PARTITIONED (`find_best_task.go:2960`), so the count that decides
        // the merge is the number of physical tables, not the number of key
        // ranges: `WHERE id IN (1,5,9)` on an unpartitioned table opens three
        // ranges that are already ascending, and Go concatenates them. A
        // `SetTableHandles` request may additionally contain equal point
        // ranges for duplicate index entries; those are intentionally
        // overlapping and must remain duplicated.
        let physical_count = self.record_physical_ids().len();
        let mut iterators = Vec::new();
        for (low, upper) in self.record_key_ranges(handle_ranges, zone, ordered || descending)? {
            iterators.push(
                self.scan_iterator(store, &low, &upper, descending)
                    .map_err(KvTableError::from)?,
            );
        }
        if descending {
            iterators.reverse();
        }
        // Go `needMergeSort` (`executor/distsql.go`):
        //
        //     len(byItems) > 0 && kvRangesCount > 1
        //
        // BOTH halves, for the reason the index path states: testing the
        // range count alone merges every partitioned read, and Go drains one
        // partition to exhaustion before the next when no order is required
        // -- which is what a partitioned `LIMIT` depends on.
        //
        // `ordered` is this tier's `len(byItems) > 0`. Go sets those items on
        // a partitioned table scan whose order matched
        // (`find_best_task.go:2959`, "Add sort items for table scan for
        // merge-sort operation between partitions") and its reader then hands
        // the per-partition results to `NewSortedSelectResults`. Without the
        // merge the ranges were CONCATENATED, so `ORDER BY id` over
        // `PARTITION BY HASH (id) PARTITIONS 2` answered each partition in
        // order and the partitions in id order: 2, 4, 1, 3.
        let merge_by_record_key = ordered && physical_count > 1;
        let unsigned_handle = self.unsigned_pk_handle();
        let mut merge_heap = IndexMergeHeap::new(descending);
        if merge_by_record_key {
            for (position, iterator) in iterators.iter().enumerate() {
                if iterator.valid() {
                    merge_heap.push(
                        record_merge_key(iterator.key().as_bytes(), unsigned_handle),
                        position,
                    );
                }
            }
        }
        Ok(RowCursor {
            iterators,
            next_iterator: 0,
            merge_by_record_key,
            unsigned_handle,
            merge_heap,
            decoder,
        })
    }

    fn record_key_ranges(
        &self,
        handle_ranges: Option<&[Range]>,
        zone: &SessionTimeZone,
        keep_order: bool,
    ) -> Result<Vec<(Key, Key)>, KvTableError> {
        let full_common_handle = tidb_ranger::points::full_not_null_range();
        // Go `ranger.FullIntRange(isUnsigned)`: a scan with no `WHERE` still
        // carries the handle domain as ONE range, and the table reader splits
        // it like any other (`table_reader.go:295`). For an UNSIGNED handle
        // that split is what puts the read in VALUE order -- the block above
        // `i64::MAX` encodes negative and would otherwise be walked first --
        // and it is what lets the shared planner promise handle order at the
        // reader boundary.
        //
        // A SIGNED handle needs none of this: its whole domain is one key
        // interval already, and building it here instead of through the range
        // encoder would only re-derive the same bytes.
        let full_unsigned_handle = tidb_ranger::points::full_int_range(true);
        let handle_ranges = if handle_ranges.is_some() {
            handle_ranges
        } else if tidb_table::tables::common_handle_primary(self).is_some() {
            Some(full_common_handle.as_slice())
        } else if self.unsigned_pk_handle() {
            Some(full_unsigned_handle.as_slice())
        } else {
            handle_ranges
        };
        let encoded = match handle_ranges {
            Some(ranges) => {
                { crate::handle_range::record_key_ranges(self, ranges, zone, keep_order) }
                    .map_err(|error| KvTableError::Encode(format!("{error:?}")))?
            }
            None => None,
        };
        Ok(encoded.unwrap_or_else(|| self.record_key_range()))
    }

    fn record_key_range_groups(
        &self,
        handle_ranges: Option<&[Range]>,
        zone: &SessionTimeZone,
        descending: bool,
    ) -> Result<Vec<Vec<(Key, Key)>>, KvTableError> {
        let full_common_handle = tidb_ranger::points::full_not_null_range();
        // See [`Self::record_key_ranges]: the full-domain defaults exist so
        // the boundary split can see the handle's own width.
        let full_unsigned_handle = tidb_ranger::points::full_int_range(true);
        let handle_ranges = if handle_ranges.is_some() {
            handle_ranges
        } else if tidb_table::tables::common_handle_primary(self).is_some() {
            Some(full_common_handle.as_slice())
        } else if self.unsigned_pk_handle() {
            Some(full_unsigned_handle.as_slice())
        } else {
            handle_ranges
        };
        let encoded = match handle_ranges {
            Some(ranges) => crate::handle_range::record_key_range_value_halves(self, ranges, zone)
                .map_err(|error| KvTableError::Encode(format!("{error:?}")))?,
            None => None,
        };
        let mut groups = match encoded {
            Some([signed_half, unsigned_half]) => vec![signed_half, unsigned_half],
            None => vec![self.record_key_range()],
        };
        groups.retain(|group| !group.is_empty());
        if descending {
            groups.reverse();
        }
        Ok(groups)
    }

    fn record_key_range(&self) -> Vec<(Key, Key)> {
        self.record_physical_ids()
            .into_iter()
            .map(|id| {
                let (low, high) = get_table_handle_key_range(id);
                // `get_table_handle_key_range` returns an inclusive upper
                // bound, while the seam's is exclusive, so the range runs to
                // the key just past it.
                let mut upper = high;
                upper.push(0);
                (Key::from_bytes(low), Key::from_bytes(upper))
            })
            .collect()
    }











    #[cfg(test)]
    fn scan_rows_with_context(
        &mut self,
        store: &mut dyn TableStorage,
        context: &RowDecodeContext,
    ) -> Result<Vec<Vec<Datum>>, KvTableError> {
        Ok(self
            .scan_rows_with_handles_with_context(store, context)?
            .into_iter()
            .map(|(_, row)| row)
            .collect())
    }

    #[cfg(test)]
    fn scan_rows_with_handles_with_context(
        &mut self,
        store: &mut dyn TableStorage,
        context: &RowDecodeContext,
    ) -> Result<Vec<(TableHandle, Vec<Datum>)>, KvTableError> {
        self.scan_rows_with_handles_in_with_context(store, None, context)
    }



    #[cfg(test)]
    fn scan_rows_with_handles_in_with_context(
        &mut self,
        store: &mut dyn TableStorage,
        handle_ranges: Option<&[Range]>,
        context: &RowDecodeContext,
    ) -> Result<Vec<(TableHandle, Vec<Datum>)>, KvTableError> {
        // Go `tables.IterRecords`: the transaction's record keys, each row
        // decoded with the table's decoder.
        let mut cursor = self.row_cursor_projected_with_context(store, None, handle_ranges, context)?;
        let mut rows = Vec::new();
        while let Some(entry) = cursor.next_row()? {
            rows.push(entry);
        }
        Ok(rows)
    }


    fn first_snapshot_row_in_range(
        &mut self,
        store: &mut dyn TableStorage,
        start: &[u8],
        end: &[u8],
        context: &RowDecodeContext,
    ) -> Result<Option<(TableHandle, Vec<Datum>)>, KvTableError> {
        let Some((key, value)) = store.snapshot_first(
            &Key::from_bytes(start.to_vec()),
            &Key::from_bytes(end.to_vec()),
        )?
        else {
            return Ok(None);
        };
        self.row_decoder_recomputed(context)?
            .decode_record(key.as_bytes(), &value)
            .map(Some)
    }

    fn scan_index_range(
        &mut self,
        store: &mut dyn TableStorage,
        index_id: i64,
        range: &Range,
        zone: &SessionTimeZone,
    ) -> Result<Vec<TableHandle>, KvTableError> {
        let mut cursor = self.index_range_cursor(store, index_id, range, zone)?;
        let mut handles = Vec::new();
        while let Some(handle) = cursor.next_handle()? {
            handles.push(handle);
        }
        Ok(handles)
    }

    fn index_range_cursor(
        &mut self,
        store: &mut dyn TableStorage,
        index_id: i64,
        range: &Range,
        zone: &SessionTimeZone,
    ) -> Result<IndexRangeCursor, KvTableError> {
        // `ordered`: this entry point serves reads whose answer is the index
        // walk itself, so it keeps the cross-partition merge.
        self.index_range_cursor_with_direction(store, index_id, range, zone, false, true)
    }

    fn index_range_cursor_with_direction(
        &mut self,
        store: &mut dyn TableStorage,
        index_id: i64,
        range: &Range,
        zone: &SessionTimeZone,
        descending: bool,
        ordered: bool,
    ) -> Result<IndexRangeCursor, KvTableError> {
        self.index_ranges_cursor_for_physical_ids(store, 
            index_id,
            std::slice::from_ref(range),
            zone,
            &self.record_physical_ids(),
            descending,
            ordered,
        )
    }




    fn index_ranges_cursor_for_physical_ids(
        &mut self,
        store: &mut dyn TableStorage,
        index_id: i64,
        ranges: &[Range],
        zone: &SessionTimeZone,
        physical_ids: &[i64],
        descending: bool,
        ordered: bool,
    ) -> Result<IndexRangeCursor, KvTableError> {
        let Some(index) = self
            .indexes()
            .iter()
            .find(|index| index.read().id == index_id)
            .cloned()
        else {
            return Err(KvTableError::Decode("no such index".to_owned()));
        };
        // Go `index.GenIndexKey` files a GLOBAL index under TableInfo.ID,
        // while local indexes remain under each physical partition id.
        let scan_physical_ids = if index.read().global {
            vec![self.table_id()]
        } else {
            physical_ids.to_vec()
        };
        let encoder = Encoder::new(self.use_new_collation());
        let encode = |values: &[Datum]| -> Result<Vec<u8>, KvTableError> {
            encoder
                .encode_key_in_timezone(zone, values)
                .map_err(|e| KvTableError::Encode(format!("{e:?}")))
        };
        let mut iterators = Vec::new();
        // Which pruned-partition ordinal each iterator reads, parallel to
        // `iterators`. The unordered lookup cuts its handle batches on this:
        // Go's `lookupTableTask` is built per partition SelectResult
        // (`buildAndDispatchLookupTasks` tags it
        // `prunedPartitions[curResultIdx]`), so the consumer must see WHERE
        // one partition ends and the next begins.
        let mut partition_of_iterator = Vec::new();
        for (ordinal, physical_id) in scan_physical_ids.iter().copied().enumerate() {
            let mut key_ranges = Vec::with_capacity(ranges.len());
            for range in ranges {
                let mut low = Key::from_bytes(encode_index_seek_key(
                    physical_id,
                    index_id,
                    &encode(&range.low_val)?,
                ));
                if range.low_exclude {
                    low = low.prefix_next();
                }
                let mut high = Key::from_bytes(encode_index_seek_key(
                    physical_id,
                    index_id,
                    &encode(&range.high_val)?,
                ));
                if !range.high_exclude {
                    high = high.prefix_next();
                }
                key_ranges.push((low, high));
            }
            if ordered {
                key_ranges.sort_by(|left, right| left.0.cmp(&right.0));
            }
            for (low, high) in key_ranges {
                partition_of_iterator.push(ordinal);
                let iterator = self
                    .scan_iterator(store, &low, &high, descending)
                    .map_err(KvTableError::from)?;
                iterators.push(iterator);
            }
        }
        if descending {
            iterators.reverse();
            partition_of_iterator.reverse();
        }
        // Go `needMergeSort` (`executor/distsql.go`):
        //
        //     len(byItems) > 0 && kvRangesCount > 1
        //
        // BOTH halves. `byItems` is the read's own sort items, non-empty only
        // when the answer has to come back in index order; `ordered` is this
        // tier's spelling of it. Testing the range count ALONE merged every
        // partitioned read, so a partitioned `LIMIT` took the globally
        // smallest index keys where Go takes the first partitions' rows:
        // `select * from tp2 where a > 33 limit 5` over
        // `PARTITION BY RANGE COLUMNS(id1)` answered rows b..f where TiDB
        // records a..e. Unordered, Go drains one partition's result to
        // exhaustion before the next (its `for i := 0; i < len(results);`
        // loop), which is what concatenating these iterators does.
        let merge_by_index_key = ordered && scan_physical_ids.len() > 1;
        let mut merge_heap = IndexMergeHeap::new(descending);
        if merge_by_index_key {
            for (position, iterator) in iterators.iter().enumerate() {
                if iterator.valid() {
                    merge_heap.push(
                        cut_index_prefix(iterator.key().as_bytes()).to_vec(),
                        position,
                    );
                }
            }
        }
        let global_partition_ids = index.read().global.then(|| physical_ids.to_vec());
        Ok(IndexRangeCursor {
            iterators,
            partition_of_iterator,
            next_iterator: 0,
            merge_by_index_key,
            merge_heap,
            index,
            common_handle: !self.common_handle_offsets().is_empty(),
            global_partition_ids,
        })
    }
}

enum IndexMergeHeap {
    Ascending(BinaryHeap<std::cmp::Reverse<(Vec<u8>, usize)>>),
    Descending(BinaryHeap<(Vec<u8>, usize)>),
}

impl IndexMergeHeap {
    fn new(descending: bool) -> Self {
        if descending {
            Self::Descending(BinaryHeap::new())
        } else {
            Self::Ascending(BinaryHeap::new())
        }
    }

    fn push(&mut self, key: Vec<u8>, position: usize) {
        match self {
            Self::Ascending(heap) => heap.push(std::cmp::Reverse((key, position))),
            Self::Descending(heap) => heap.push((key, position)),
        }
    }

    fn pop(&mut self) -> Option<usize> {
        match self {
            Self::Ascending(heap) => heap.pop().map(|std::cmp::Reverse((_, position))| position),
            Self::Descending(heap) => heap.pop().map(|(_, position)| position),
        }
    }
}

thread_local! {
    /// The column ids the row codec has been asked for while a
    /// [`capture_decoded_column_ids`] call is on this thread's stack.
    static DECODE_PROBE: std::cell::RefCell<Option<std::collections::BTreeSet<i64>>> =
        const { std::cell::RefCell::new(None) };
}

/// Runs `f` while recording every column id a table scan asks the row codec
/// to decode, and returns that set alongside `f`'s value.
///
/// This is the instrument that makes column pruning *checkable* rather than
/// merely plausible: the set is the exact `columns` map handed to
/// `tidb_tablecodec::decode_table_row_to_map`, which decodes an id if and
/// only if the map holds it. A column absent from the set was never read out
/// of the stored row bytes.
///
/// Capture is per-thread and does not nest: an inner call takes the recorded
/// set with it and leaves the outer one empty.
pub fn capture_decoded_column_ids<R>(
    f: impl FnOnce() -> R,
) -> (R, std::collections::BTreeSet<i64>) {
    DECODE_PROBE.with(|probe| {
        *probe.borrow_mut() = Some(std::collections::BTreeSet::new());
    });
    let value = f();
    let ids = DECODE_PROBE
        .with(|probe| probe.borrow_mut().take())
        .unwrap_or_default();
    (value, ids)
}

/// Records the ids one decode round asked for, when a capture is active.
pub(crate) fn note_decoded_column_ids(ids: impl Iterator<Item = i64>) {
    DECODE_PROBE.with(|probe| {
        if let Some(recorded) = probe.borrow_mut().as_mut() {
            recorded.extend(ids);
        }
    });
}

/// A forward cursor over a table's record range, decoding one row per pull.
///
/// See [`TableCommon::row_cursor_with_context`]. Over
/// [`ClusterTableStorage`](tidb_store_driver_txn::cluster_storage::ClusterTableStorage) the
/// iterator is the merged stream (snapshot plus the session's staged mutation
/// buffer), so a cursor sees exactly the rows a materializing scan saw.
pub struct RowCursor {
    /// The ranges to read, in ascending key order. A whole-table scan
    /// holds exactly one; a `TableRangeScan` holds one per handle range (per
    /// partition, for a partitioned table).
    iterators: Vec<Box<dyn StorageIterator>>,
    next_iterator: usize,
    /// Go `needMergeSort` (`executor/distsql.go`): with more than one range
    /// AND an order to keep, the ranges are merged rather than concatenated.
    merge_by_record_key: bool,
    /// Whether the handle the merge orders by is UNSIGNED; see
    /// [`record_merge_key`].
    unsigned_handle: bool,
    merge_heap: IndexMergeHeap,
    decoder: RowDecoder,
}

/// The bytes a record key sorts by inside the partition merge, in the
/// HANDLE's own domain order.
///
/// `cut_row_key_prefix`, NOT `cut_index_prefix`: a record key is
/// `t{id}_r{handle}` and is exactly `PREFIX_LEN + ID_LEN` long, which is the
/// whole of what the index cut removes. Cut that way every record key
/// compares EMPTY, all keys tie, and the heap falls back to the position --
/// draining one partition before the next, which is the concatenation this
/// merge exists to replace. What is left is the 8 encoded handle bytes.
///
/// Those bytes carry the SIGNED integer codec, which flips the sign bit so
/// that byte order is signed order. An UNSIGNED handle above `i64::MAX`
/// therefore sorts FIRST in key order while its VALUE is the largest, and a
/// merge that trusted the raw bytes answered `PARTITION BY HASH (id)` with
/// `2^63, u64::MAX, 1, i64::MAX` for `ORDER BY id`. Flipping the bit back
/// gives the bytes the unsigned codec would have written, whose order is the
/// value order.
///
/// Go reaches the same order from the other side: its partitioned keep-order
/// table scan carries `byItems` and merges through `NewSortedSelectResults`,
/// which compares the ORDER BY expressions' decoded VALUES. Flipping one bit
/// is that comparison without decoding a row to make it.
fn record_merge_key(key: &[u8], unsigned_handle: bool) -> Vec<u8> {
    record_merge_bytes(key, unsigned_handle).collect()
}

/// The same ordering without allocating a key for a streaming comparison.
fn record_merge_bytes(key: &[u8], unsigned_handle: bool) -> impl Iterator<Item = u8> + '_ {
    cut_row_key_prefix(key)
        .iter()
        .copied()
        .enumerate()
        .map(move |(offset, byte)| {
            if unsigned_handle && offset == 0 {
                byte ^ 0x80
            } else {
                byte
            }
        })
}

impl RowCursor {
    /// The next row in key order, or `None` at the end of the last range.
    pub fn next_row(&mut self) -> Result<Option<(TableHandle, Vec<Datum>)>, KvTableError> {
        Ok(self
            .next_physical_row()?
            .map(|(_, handle, row)| (handle, row)))
    }

    /// The next row plus the physical table id its record key carries.
    pub(crate) fn next_physical_row(
        &mut self,
    ) -> Result<Option<(i64, TableHandle, Vec<Datum>)>, KvTableError> {
        if self.merge_by_record_key {
            let Some(position) = self.merge_heap.pop() else {
                return Ok(None);
            };
            let iterator = &mut self.iterators[position];
            let physical_id = decode_table_id(iterator.key().as_bytes());
            let (handle, row) = self
                .decoder
                .decode_record(iterator.key().as_bytes(), iterator.value())?;
            iterator.next().map_err(KvTableError::from)?;
            if iterator.valid() {
                self.merge_heap.push(
                    record_merge_key(iterator.key().as_bytes(), self.unsigned_handle),
                    position,
                );
            }
            return Ok(Some((physical_id, handle, row)));
        }
        while self.next_iterator < self.iterators.len() {
            let iterator = &mut self.iterators[self.next_iterator];
            if !iterator.valid() {
                iterator.close();
                self.next_iterator += 1;
                continue;
            }
            let physical_id = decode_table_id(iterator.key().as_bytes());
            let (handle, row) = self
                .decoder
                .decode_record(iterator.key().as_bytes(), iterator.value())?;
            iterator.next().map_err(KvTableError::from)?;
            return Ok(Some((physical_id, handle, row)));
        }
        Ok(None)
    }
}

impl Drop for RowCursor {
    /// An abandoned cursor (an early-stopping `LIMIT`) must still release
    /// every iterator, which a drained loop's explicit `close` would have
    /// done -- including the ranges it never reached.
    fn drop(&mut self) {
        // A MERGED cursor leaves every range open until it is exhausted, so
        // the whole list is closed; a sequential one has already closed the
        // ranges it finished with.
        let from = if self.merge_by_record_key {
            0
        } else {
            self.next_iterator
        };
        for iterator in &mut self.iterators[from..] {
            iterator.close();
        }
    }
}

/// A forward cursor over one index range, yielding row handles in index order.
///
/// See [`TableCommon::index_range_cursor`].
pub struct IndexRangeCursor {
    iterators: Vec<Box<dyn StorageIterator>>,
    /// The pruned-partition ordinal each iterator reads, parallel to
    /// `iterators`. Always `0` for an unpartitioned table.
    partition_of_iterator: Vec<usize>,
    next_iterator: usize,
    merge_by_index_key: bool,
    merge_heap: IndexMergeHeap,
    index: Index,
    common_handle: bool,
    /// Selected physical partition ids for a GLOBAL index. Its one logical
    /// keyspace carries the actual id in every value rather than in the key.
    global_partition_ids: Option<Vec<i64>>,
}

impl IndexRangeCursor {
    fn global_partition_ordinal(
        selected: Option<&[i64]>,
        value: &[u8],
    ) -> Result<Option<usize>, KvTableError> {
        let Some(selected) = selected else {
            return Ok(None);
        };
        let segments = tidb_tablecodec::split_index_value(value)
            .map_err(|error| KvTableError::Decode(format!("{error:?}")))?;
        let encoded = segments.partition_id.ok_or_else(|| {
            KvTableError::Decode("a global index value contains no partition id".to_owned())
        })?;
        let (_, partition_id) = tidb_codec::decode_int(&encoded)
            .map_err(|error| KvTableError::Decode(format!("{error:?}")))?;
        Ok(selected
            .iter()
            .position(|candidate| *candidate == partition_id))
    }

    /// The next row handle in index order, or `None` at the end of the range.
    pub fn next_handle(&mut self) -> Result<Option<TableHandle>, KvTableError> {
        Ok(self
            .next_handle_in_partition()?
            .map(|(handle, _partition)| handle))
    }

    /// The next row handle together with the pruned-partition ordinal it was
    /// read from.
    ///
    /// The ordinal is what lets an unordered index LOOKUP cut its handle
    /// batches at partition boundaries: Go's `indexWorker` builds one
    /// `lookupTableTask` per per-partition `SelectResult`
    /// (`buildAndDispatchLookupTasks` tags it
    /// `prunedPartitions[curResultIdx]`), so no task -- and therefore no
    /// handle sort and no table read -- ever spans two partitions. A caller
    /// that ignored the ordinal would sort one storage batch GLOBALLY and
    /// answer the partitions interleaved by handle, where Go answers them
    /// one after another.
    pub fn next_handle_in_partition(
        &mut self,
    ) -> Result<Option<(TableHandle, usize)>, KvTableError> {
        self.next_handle_in_partition_if(|_, _, _, _| Ok(std::ops::ControlFlow::Continue(true)))
    }

    /// Applies an index-side Selection/Limit while the entry bytes are borrowed
    /// from the iterator, before issuing any table lookup. Ordinary reads use
    /// the no-op callback above and do not allocate or decode extra key values.
    pub(crate) fn next_handle_in_partition_if<E: From<KvTableError>>(
        &mut self,
        mut admit: impl FnMut(
            &TableHandle,
            &[u8],
            &[u8],
            usize,
        ) -> Result<std::ops::ControlFlow<(), bool>, E>,
    ) -> Result<Option<(TableHandle, usize)>, E> {
        if self.merge_by_index_key {
            loop {
                let Some(position) = self.merge_heap.pop() else {
                    return Ok(None);
                };
                let iterator = &mut self.iterators[position];
                let handle = index_entry_handle(
                    &self.index,
                    iterator.key().as_bytes(),
                    iterator.value(),
                    self.common_handle,
                )?;
                let global_partition = Self::global_partition_ordinal(
                    self.global_partition_ids.as_deref(),
                    iterator.value(),
                )?;
                let decision = if self.global_partition_ids.is_none() || global_partition.is_some()
                {
                    admit(
                        &handle,
                        iterator.key().as_bytes(),
                        iterator.value(),
                        global_partition.unwrap_or_else(|| {
                            self.partition_of_iterator
                                .get(position)
                                .copied()
                                .unwrap_or(0)
                        }),
                    )?
                } else {
                    std::ops::ControlFlow::Continue(false)
                };
                iterator.next().map_err(KvTableError::from)?;
                if iterator.valid() {
                    self.merge_heap.push(
                        cut_index_prefix(iterator.key().as_bytes()).to_vec(),
                        position,
                    );
                }
                match decision {
                    std::ops::ControlFlow::Break(()) => return Ok(None),
                    std::ops::ControlFlow::Continue(false) => continue,
                    std::ops::ControlFlow::Continue(true) => {}
                }
                let partition = if self.global_partition_ids.is_some() {
                    let Some(partition) = global_partition else {
                        continue;
                    };
                    partition
                } else {
                    self.partition_of_iterator
                        .get(position)
                        .copied()
                        .unwrap_or(0)
                };
                return Ok(Some((handle, partition)));
            }
        }
        while self.next_iterator < self.iterators.len() {
            let iterator = &mut self.iterators[self.next_iterator];
            if !iterator.valid() {
                iterator.close();
                self.next_iterator += 1;
                continue;
            }
            let handle = index_entry_handle(
                &self.index,
                iterator.key().as_bytes(),
                iterator.value(),
                self.common_handle,
            )?;
            let global_partition = Self::global_partition_ordinal(
                self.global_partition_ids.as_deref(),
                iterator.value(),
            )?;
            let decision = if self.global_partition_ids.is_none() || global_partition.is_some() {
                admit(
                    &handle,
                    iterator.key().as_bytes(),
                    iterator.value(),
                    global_partition.unwrap_or_else(|| {
                        self.partition_of_iterator
                            .get(self.next_iterator)
                            .copied()
                            .unwrap_or(0)
                    }),
                )?
            } else {
                std::ops::ControlFlow::Continue(false)
            };
            iterator.next().map_err(KvTableError::from)?;
            match decision {
                std::ops::ControlFlow::Break(()) => return Ok(None),
                std::ops::ControlFlow::Continue(false) => continue,
                std::ops::ControlFlow::Continue(true) => {}
            }
            let partition = if self.global_partition_ids.is_some() {
                let Some(partition) = global_partition else {
                    continue;
                };
                partition
            } else {
                self.partition_of_iterator
                    .get(self.next_iterator)
                    .copied()
                    .unwrap_or(0)
            };
            return Ok(Some((handle, partition)));
        }
        Ok(None)
    }
}

impl Drop for IndexRangeCursor {
    fn drop(&mut self) {
        for iterator in &mut self.iterators[self.next_iterator..] {
            iterator.close();
        }
    }
}

pub(crate) fn insert_extra_handle(
    mut row: Vec<Datum>,
    slot: usize,
    handle: &TableHandle,
) -> Vec<Datum> {
    let value = match integer_record_handle(&handle.record_handle()) {
        Some(value) => Datum::Int(value),
        // Fail-closed rather than inventing a rowid a heap table cannot have.
        None => Datum::Null,
    };
    if slot == row.len() {
        row.push(value);
    } else if let Some(cell) = row.get_mut(slot) {
        *cell = value;
    }
    row
}

/// The integer inside a record handle, through a partition's wrapper.
///
/// A partitioned heap table's rows are keyed by `(partition_id, handle)`, and
/// Go's `_tidb_rowid` reports the handle -- which is why the same rowid
/// recurs across partitions, as the source corpus's own comments note.
fn integer_record_handle(handle: &RecordHandle) -> Option<i64> {
    match handle {
        RecordHandle::Int(value) => Some(*value),
        RecordHandle::Partition { handle, .. } => integer_record_handle(handle),
        RecordHandle::Common(_) => None,
    }
}

/// The executor's constructors of a [`RowDecodeContext`], over its own
/// statement context: a zone-only context for callers not yet migrated to a
/// caller class, and the test fixture's.
/// The executor's constructors of a [`RowDecodeContext`], over its own
/// statement context: a zone-only context for callers not yet migrated to a
/// caller class, and the test fixture's.
pub trait RowDecodeContextExt {
    /// The pre-migration row-decoder contract: caller supplies only a zone,
    /// and origin defaults use `DEFAULT_STATEMENT_FLAGS`.
    fn legacy_default(zone: &SessionTimeZone) -> RowDecodeContext;

    /// Query-class UTC decoding for unit fixtures with no session.
    fn for_test_query_utc() -> RowDecodeContext;
}

impl RowDecodeContextExt for RowDecodeContext {
    fn legacy_default(zone: &SessionTimeZone) -> RowDecodeContext {
        RowDecodeContext::new(
            tidb_datatype::DEFAULT_STATEMENT_FLAGS,
            zone.clone(),
            std::sync::Arc::new(crate::StmtContext::for_query().with_time_zone(zone.clone())),
        )
    }

    fn for_test_query_utc() -> RowDecodeContext {
        RowDecodeContext::for_query(&crate::StmtContext::for_query())
    }
}
