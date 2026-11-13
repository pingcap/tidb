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
use tidb_executor_internal_exec::Executor;

use tidb_distsql::remote_scan::EXTRA_HANDLE_COLUMN_ID;
use tidb_distsql::remote_scan::PushdownTopN;
use tidb_distsql::remote_scan::PushdownStatementContext;
use tidb_distsql::remote_scan::PushdownScanRequest;
use tidb_distsql::remote_scan::PushdownScanColumn;
use tidb_distsql::remote_scan::PushdownRowStream;
use tidb_kv::storage::{StorageError, StorageIterator, TableStorage};
use std::collections::{BTreeMap, BinaryHeap, HashMap, HashSet};
use tidb_chunk::chunk::Chunk;
use tidb_tablecodec::table_key::{
    cut_index_prefix, cut_row_key_prefix, decode_table_id, encode_index_seek_key,
    encode_row_key_with_handle, get_table_handle_key_range, RecordHandle,
};
use tidb_codec::Encoder;
use tidb_datatype::{Datum, FieldType, SessionTimeZone};
use tidb_expr::expression::Expression;
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

    /// A cursor that reads this table's rows through the backend's
    /// coprocessor -- predicate, row cap and projection evaluated at the
    /// region -- with the session's staged writes merged back in, or `None`
    /// when the backend has none or this table's shape is outside it.
    ///
    /// `keep` is the projected column set, in output order; `predicates`
    /// describe the conjuncts the caller applies to every emitted row anyway,
    /// so the remote filter is a pre-filter and cannot change the answer.
    ///
    /// A common-handle (clustered non-integer primary key) table is served
    /// without a client-side handle merge. Its clustered primary metadata is
    /// synthesized when the catalog omits the physical index entry, matching
    /// the table's record-key layout and Go's always-present `IndexInfo`.
    ///
    /// `retain_identity` asks for the record handle beside `keep` even when
    /// the projection omits it: writers, `_tidb_rowid` readers and a dirty
    /// table's staged merge need it. A clean reader passes `false` and gets
    /// Go's TableReader wire schema, exactly the pruned columns.
    #[allow(clippy::too_many_arguments)]
    fn pushdown_row_cursor_with_context(
        &mut self,
        store: &mut dyn TableStorage,
        keep: &[usize],
        predicates: &[Expression],
        output_offsets: Option<&[usize]>,
        topn: Option<&PushdownTopN>,
        limit: Option<u64>,
        handle_ranges: Option<&[Range]>,
        range_hints: Option<&[usize]>,
        descending: bool,
        keep_order: bool,
        retain_identity: bool,
        context: &RowDecodeContext,
        statement: &PushdownStatementContext,
    ) -> Result<Option<RemoteRowCursor>, KvTableError>;

    /// [`Self::pushdown_row_cursor_with_context`] for a scan whose schema
    /// carries Go's `ExtraPhysTblID`: a partitioned read then requests that
    /// column LAST, as Go's TableScan does, and storage fills it from each
    /// row's key, so a row's record key names its partition.
    #[allow(clippy::too_many_arguments)]
    fn pushdown_row_cursor_with_physical_id(
        &mut self,
        store: &mut dyn TableStorage,
        keep: &[usize],
        predicates: &[Expression],
        output_offsets: Option<&[usize]>,
        topn: Option<&PushdownTopN>,
        limit: Option<u64>,
        handle_ranges: Option<&[Range]>,
        range_hints: Option<&[usize]>,
        descending: bool,
        keep_order: bool,
        retain_identity: bool,
        physical_id: bool,
        context: &RowDecodeContext,
        statement: &PushdownStatementContext,
    ) -> Result<Option<RemoteRowCursor>, KvTableError>;

    fn pushdown_row_cursor_with_context_and_key_ranges(
        &mut self,
        store: &mut dyn TableStorage,
        keep: &[usize],
        predicates: &[Expression],
        output_offsets: Option<&[usize]>,
        topn: Option<&PushdownTopN>,
        limit: Option<u64>,
        handle_ranges: Option<&[Range]>,
        range_hints: Option<&[usize]>,
        descending: bool,
        keep_order: bool,
        retain_identity: bool,
        physical_id: bool,
        context: &RowDecodeContext,
        statement: &PushdownStatementContext,
        precomputed_ranges: Option<&[(Key, Key)]>,
    ) -> Result<Option<RemoteRowCursor>, KvTableError>;

    /// Builds table tasks with Go's partition-handle identity. Routes, when
    /// present, are aligned with input handles and validated before any I/O.
    #[allow(clippy::too_many_arguments)]
    fn build_table_reader_from_routed_handles(
        &mut self,
        store: &mut dyn TableStorage,
        handles: &[TableHandle],
        physical_ids: Option<&[i64]>,
        scan_keep: &[usize],
        predicates: &[Expression],
        context: &RowDecodeContext,
        statement: &PushdownStatementContext,
        required_rows: usize,
    ) -> Result<Option<StagedHandlesLookup>, KvTableError>;

    #[allow(clippy::too_many_arguments)]
    fn build_physical_table_reader(
        &mut self,
        store: &mut dyn TableStorage,
        scan_keep: &[usize],
        predicates: &[Expression],
        context: &RowDecodeContext,
        statement: &PushdownStatementContext,
        required_rows: usize,
        key_ranges: &[(Key, Key)],
        range_hints: &[usize],
        handle_partitions: Option<Vec<i64>>,
    ) -> Result<Option<StagedPhysicalLookup>, KvTableError>;

    /// Converts sorted handles using the direct KV range builder shared with
    /// Go's RequestBuilder.SetTableHandles, avoiding an Range round trip.
    fn table_reader_handle_key_ranges(
        table_id: i64,
        handles: &[TableHandle],
    ) -> Result<(Vec<(Key, Key)>, Vec<usize>), KvTableError>;

    /// Returns original request positions, retaining route identity across
    /// missing rows, filters and equal handles in different partitions.
    fn finish_routed_rows_by_handles(
        handles: &[TableHandle],
        staged: StagedHandlesLookup,
    ) -> Result<Option<(Vec<(TableHandle, Vec<Datum>)>, bool, u64, Vec<usize>)>, KvTableError>;

    /// The rows of one physical part, with Go's `chk.MemoryUsage()` of every
    /// chunk its reader filled.
    fn finish_physical_rows(
        handles: &[TableHandle],
        staged: StagedPhysicalLookup,
    ) -> Result<Option<(Vec<(TableHandle, Vec<Datum>)>, bool, u64, Vec<usize>, i64)>, KvTableError>;








    /// Scans the table's record-key range in key order, decoding each value.
    /// Returns rows as `Datum`s in schema order (a missing column decodes
    /// NULL, and the handle columns come from the key).
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
    fn scan_rows_with_handles_in_with_context(
        &mut self,
        store: &mut dyn TableStorage,
        handle_ranges: Option<&[Range]>,
        context: &RowDecodeContext,
    ) -> Result<Vec<(TableHandle, Vec<Datum>)>, KvTableError>;

    fn scan_rows_with_handles_recomputed(
        &mut self,
        store: &mut dyn TableStorage,
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

    #[allow(clippy::too_many_arguments)]
    fn pushdown_row_cursor_with_context(
        &mut self,
        store: &mut dyn TableStorage,
        keep: &[usize],
        predicates: &[Expression],
        output_offsets: Option<&[usize]>,
        topn: Option<&PushdownTopN>,
        limit: Option<u64>,
        handle_ranges: Option<&[Range]>,
        range_hints: Option<&[usize]>,
        descending: bool,
        keep_order: bool,
        retain_identity: bool,
        context: &RowDecodeContext,
        statement: &PushdownStatementContext,
    ) -> Result<Option<RemoteRowCursor>, KvTableError> {
        self.pushdown_row_cursor_with_physical_id(store, 
            keep,
            predicates,
            output_offsets,
            topn,
            limit,
            handle_ranges,
            range_hints,
            descending,
            keep_order,
            retain_identity,
            false,
            context,
            statement,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn pushdown_row_cursor_with_physical_id(
        &mut self,
        store: &mut dyn TableStorage,
        keep: &[usize],
        predicates: &[Expression],
        output_offsets: Option<&[usize]>,
        topn: Option<&PushdownTopN>,
        limit: Option<u64>,
        handle_ranges: Option<&[Range]>,
        range_hints: Option<&[usize]>,
        descending: bool,
        keep_order: bool,
        retain_identity: bool,
        physical_id: bool,
        context: &RowDecodeContext,
        statement: &PushdownStatementContext,
    ) -> Result<Option<RemoteRowCursor>, KvTableError> {
        self.pushdown_row_cursor_with_context_and_key_ranges(store, 
            keep,
            predicates,
            output_offsets,
            topn,
            limit,
            handle_ranges,
            range_hints,
            descending,
            keep_order,
            retain_identity,
            physical_id,
            context,
            statement,
            None,
        )
    }

    fn pushdown_row_cursor_with_context_and_key_ranges(
        &mut self,
        store: &mut dyn TableStorage,
        keep: &[usize],
        predicates: &[Expression],
        output_offsets: Option<&[usize]>,
        topn: Option<&PushdownTopN>,
        limit: Option<u64>,
        handle_ranges: Option<&[Range]>,
        range_hints: Option<&[usize]>,
        descending: bool,
        keep_order: bool,
        retain_identity: bool,
        physical_id: bool,
        context: &RowDecodeContext,
        statement: &PushdownStatementContext,
        precomputed_ranges: Option<&[(Key, Key)]>,
    ) -> Result<Option<RemoteRowCursor>, KvTableError> {
        let common_handle = !self.common_handle_offsets().is_empty();
        let common_primary = tidb_table::tables::clustered_primary_metadata(self);
        if common_handle && common_primary.is_none() {
            return Ok(None);
        }
        // A TiKV TableScan names one PHYSICAL table id.  A partitioned table
        // therefore needs one request per selected partition; sending the
        // logical table id (or a range spanning several partition ids) makes
        // the region scan read the wrong record namespace.  The requests are
        // assembled below after the handle ranges have been split.
        // An ORDERED read of an unsigned handle whose domain wraps the int64
        // boundary cannot travel as one ascending range list: Go opens TWO
        // results -- `firstPartGroupedRanges then `secondPartGroupedRanges
        // (`table_reader.go) -- and reads them one after the other through
        // `resultHandler.open(firstResult, secondResult). The groups below are
        // exactly those parts in read order (a descending read reverses the
        // pair); every shape without a straddle is one group and takes the
        // single-request path unchanged. An UNORDERED read keeps Go's merged
        // ascending wire order from [`Self::record_key_ranges].
        let groups = if let Some(ranges) = precomputed_ranges {
            vec![ranges.to_vec()]
        } else if keep_order || descending {
            self.record_key_range_groups(handle_ranges, context.zone(), descending)?
        } else {
            vec![self.record_key_ranges(handle_ranges, context.zone(), false)?]
        };
        // No range at all is a read of NOTHING -- `id > 100 AND id < 100, or a
        // bound that is NULL -- and a coprocessor request has no way to say
        // that: its `Ranges list is what the transport turns into region
        // tasks, so an empty one is a malformed request rather than an empty
        // answer (`tidb_distsql's `metadata_region_ranges rejects it as
        // `missing_ranges). The local cursor states it exactly, by opening no
        // iterator, so the read goes there. Go plans a `TableDual` for the same
        // shape and sends no request either.
        if groups.iter().all(|group| group.is_empty()) {
            return Ok(None);
        }
        let mut columns: Vec<PushdownScanColumn> = keep
            .iter()
            .map(|offset| {
                let column = &self.columns()[*offset];
                PushdownScanColumn {
                    id: column.read().id,
                    field_type: column.read().field_type.clone(),
                    is_handle: self.pk_handle_offset() == Some(*offset),
                    origin_default: column.origin_default.clone(),
                }
            })
            .collect();
        // A merged row retains its record identity independently of the
        // visible projection. Integer and composite handles use the same
        // staged replacement/tombstone merge below. A read with no handle
        // consumer requests exactly `keep`, as Go's TableReader does: its
        // TableScan columns are the pruned schema, and the UnionScan that
        // needs a handle is planned only over a dirty table. An ordered
        // partitioned integer read still merges its per-partition streams by
        // handle, so it keeps the column whatever the caller asked.
        let retain_identity = retain_identity || (self.partition().is_some() && keep_order);
        let mut common_identity =
            common_primary
                .as_ref()
                .filter(|_| retain_identity)
                .map(|primary| {
                    let parts = primary
                        .column_offsets
                        .iter()
                        .enumerate()
                        .map(|(part, offset)| {
                            let column = &self.columns()[*offset];
                            let position = columns
                                .iter()
                                .position(|kept| kept.id == column.read().id)
                                .unwrap_or_else(|| {
                                    columns.push(PushdownScanColumn {
                                        id: column.read().id,
                                        field_type: column.read().field_type.clone(),
                                        is_handle: false,
                                        origin_default: column.origin_default.clone(),
                                    });
                                    columns.len() - 1
                                });
                            (
                                position,
                                primary.prefix_length(part),
                                column.read().field_type.clone(),
                            )
                        })
                        .collect();
                    RemoteCommonHandle {
                        parts,
                        zone: context.zone().clone(),
                        use_new_collation: self.use_new_collation(),
                    }
                });
        let handle_index = if common_handle {
            None
        } else {
            match self
                .pk_handle_offset()
                .and_then(|offset| keep.iter().position(|kept| *kept == offset))
            {
                Some(index) => Some(index),
                None if !retain_identity => None,
                None => {
                    columns.push(match self.pk_handle_offset() {
                        Some(offset) => PushdownScanColumn {
                            id: self.columns()[offset].read().id,
                            field_type: self.columns()[offset].read().field_type.clone(),
                            is_handle: true,
                            origin_default: None,
                        },
                        None => PushdownScanColumn {
                            id: EXTRA_HANDLE_COLUMN_ID,
                            field_type: FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
                            is_handle: true,
                            origin_default: None,
                        },
                    });
                    Some(columns.len() - 1)
                }
            }
        };
        // Go's sorted partition results retain their ordering columns in the
        // wire schema. An output projection may otherwise drop or reorder a
        // key part even though the scan requested it. Remap identity against
        // the response and append only the missing transport columns.
        let mut response_offsets = output_offsets.map(<[usize]>::to_vec);
        let response_handle_index = if let Some(offsets) = &mut response_offsets {
            let mut identity_position = |position| {
                offsets
                    .iter()
                    .position(|offset| *offset == position)
                    .or_else(|| {
                        retain_identity.then(|| {
                            offsets.push(position);
                            offsets.len() - 1
                        })
                    })
            };
            if let Some(identity) = &mut common_identity {
                for (position, _, _) in &mut identity.parts {
                    *position = identity_position(*position).expect("common identity is retained");
                }
            }
            handle_index.and_then(identity_position)
        } else {
            handle_index
        };
        // A dirty partitioned read also needs each row's physical table id:
        // Go's UnionScan keys a snapshot row by its own partition
        // (`getSnapshotRow` encodes the record key under `_tidb_tid`), and so
        // does the staged-row merge below.
        let dirty_partitioned =
            self.partition().is_some() && self.has_dirty_content(&statement.staged_writes);
        // A request over precomputed ranges may span partitions (Go's
        // `SetPartitionsAndHandles`): storage fills the column from each
        // row's key all the same.
        let response_physical_id_index = (dirty_partitioned
            || (physical_id && (self.partition().is_some() || precomputed_ranges.is_some())))
        .then(|| {
                columns.push(PushdownScanColumn {
                    id: tidb_model::column::EXTRA_PHYS_TBL_ID,
                    field_type: FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
                    is_handle: false,
                    origin_default: None,
                });
                let position = columns.len() - 1;
                match &mut response_offsets {
                    Some(offsets) => {
                        offsets.push(position);
                        offsets.len() - 1
                    }
                    None => position,
                }
            });
        let primary_column_ids: Vec<i64> = common_primary
            .as_ref()
            .into_iter()
            .flat_map(|index| index.column_offsets.iter())
            .filter_map(|offset| self.columns().get(*offset))
            .map(|column| column.read().id)
            .collect();
        let primary_prefix_column_ids: Vec<i64> = common_primary
            .as_ref()
            .into_iter()
            .flat_map(|index| {
                index
                    .column_offsets
                    .iter()
                    .enumerate()
                    .filter_map(|(position, offset)| {
                        let column = self.columns().get(*offset)?;
                        let prefix = index.prefix_length(position);
                        (prefix > 0 && column.read().field_type.flen() > prefix).then_some(column.read().id)
                    })
            })
            .collect();
        // One request PER physical table and unsigned-handle group. Both open
        // up front -- Go builds all partition/part responses before reading
        // either (`buildRespForGroupedRanges`) -- and the rows are consumed
        // partition by partition (or merged for an ordered integer-handle
        // read).
        // Go's `SetTableHandles` supplies hints only for the single grouped
        // handle request. A straddling unsigned range is split into two
        // requests above, so do not attach hints to those unrelated groups.
        let partitioned = self.partition().is_some();
        let physical_ids = self.record_physical_ids();
        let request_range_hints = if !partitioned && groups.len() == 1 {
            range_hints.filter(|hints| hints.len() == groups[0].len())
        } else {
            None
        };
        let build_request = |table_id: i64, ranges: Vec<(Key, Key)>| PushdownScanRequest {
            // `table_id` is the physical partition id for a partitioned read
            // and the logical id for an ordinary table.
            table_id,
            index_lookup: None,
            index: None,
            columns: columns.clone(),
            handle_index,
            primary_column_ids: primary_column_ids.clone(),
            primary_prefix_column_ids: primary_prefix_column_ids.clone(),
            conditions: predicates.to_vec(),
            output_offsets: response_offsets.clone(),
            topn: topn.cloned(),
            limit,
            prefix_limit: None,
            paging_min_size: None,
            aggregate: None,
            desc: descending,
            keep_order,
            allow_unordered_response: !keep_order && !descending,
            // The storage that owns the snapshot fills this in; the table has
            // no timestamp of its own.
            snapshot_ts: 0,
            ranges,
            range_hints: request_range_hints.map_or_else(Vec::new, <[usize]>::to_vec),
            statement: statement.clone(),
        };
        // Keep the physical id beside each opened stream. A clean remote
        // stream does not need record-key reconstruction, but an ordered
        // partitioned read still has to merge the per-partition streams by
        // their decoded integer handles.
        let mut partition_scans: Vec<(i64, Vec<tidb_distsql::remote_scan::PushdownScan>)> = Vec::new();
        let request_groups: Vec<(i64, Vec<(Key, Key)>)> = if partitioned {
            let mut requests = Vec::new();
            // `groups` is in value order (and is reversed for DESC). Keep
            // that order inside each partition so an unsigned handle's two
            // halves can be chained exactly like Go's two SelectResults.
            for physical_id in &physical_ids {
                for group in &groups {
                    let ranges = group
                        .iter()
                        .filter(|(start, _)| decode_table_id(start.as_bytes()) == *physical_id)
                        .cloned()
                        .collect::<Vec<_>>();
                    if !ranges.is_empty() {
                        requests.push((*physical_id, ranges));
                    }
                }
            }
            requests
        } else {
            groups
                .into_iter()
                .map(|ranges| (self.table_id(), ranges))
                .collect()
        };
        for (physical_id, ranges) in request_groups {
            let Some(scan) = crate::remote_scan::open_table_remote_scan(self, &*store, &build_request(physical_id, ranges))
            else {
                for (_, opened_parts) in partition_scans.drain(..) {
                    for mut opened in opened_parts {
                        opened.stream.close();
                    }
                }
                return Ok(None);
            };
            let mut scan = scan.map_err(KvTableError::from)?;
            if (output_offsets.is_some() || !retain_identity) && !scan.staged.is_empty() {
                // A projected or identity-free remote row may no longer
                // contain the handle needed by UnionScan's staged-row merge.
                // The caller normally refuses both as soon as the table is
                // dirty; retain this storage-boundary guard for a concurrent
                // staged write or a backend that reports staged content late.
                scan.stream.close();
                for (_, opened_parts) in partition_scans.drain(..) {
                    for mut opened in opened_parts {
                        opened.stream.close();
                    }
                }
                return Ok(None);
            }
            // An unordered read merges the staged rows by record key, which
            // now carries each row's own partition. An ordered read merges
            // the partitions by handle (`PartitionMergedPushdownStream`),
            // whose order the partition-major staged slice does not follow.
            if partitioned
                && !scan.staged.is_empty()
                && (response_physical_id_index.is_none() || keep_order || descending)
            {
                scan.stream.close();
                for (_, opened_parts) in partition_scans.drain(..) {
                    for mut opened in opened_parts {
                        opened.stream.close();
                    }
                }
                return Ok(None);
            }
            // A narrowed response can drop a column the local filter would
            // need to re-check. That re-check is not optional: a backend may
            // accept a predicate description without fully applying it (the
            // "never a wrong answer, only slower" contract), so an
            // unconfirmed narrowed row can never be trusted verbatim whenever
            // there is a predicate to re-check in the first place. A bare
            // range/projection read with no pushed predicate has nothing to
            // re-verify, so it keeps the coprocessor request either way.
            if output_offsets.is_some()
                && !predicates.is_empty()
                && !scan.stream.predicates_applied()
            {
                scan.stream.close();
                for (_, opened_parts) in partition_scans.drain(..) {
                    for mut opened in opened_parts {
                        opened.stream.close();
                    }
                }
                return Ok(None);
            }
            // One request reached a region. Counted here rather than at the
            // storage seam so a backend that REFUSED the shape (and returned
            // an `Unsupported` the caller turned into a byte-level cursor) is
            // not recorded as a coprocessor read.
            tidb_kv::storage::note_storage_op(|ops| ops.cop_scans += 1);
            if let Some((_, parts)) = partition_scans
                .iter_mut()
                .find(|(id, _)| *id == physical_id)
            {
                parts.push(scan);
            } else {
                partition_scans.push((physical_id, vec![scan]));
            }
        }
        let decoder = self.row_decoder_projected(Some(keep), context)?;
        let mut staged = Vec::new();
        // Each part's staged slice covers only that part's ranges and arrives
        // in part order, so concatenating them preserves the key order the
        // merge below walks -- ascending for an ascending read, descending for
        // a descending one (`open_remote_scan already reverses to match).
        for (_, parts) in &mut partition_scans {
            for scan in parts {
                for (key, value) in std::mem::take(&mut scan.staged) {
                    let row = match value {
                        Some(value) => Some(decoder.decode_record(key.as_bytes(), &value)?.1),
                        None => None,
                    };
                    staged.push((key.into_bytes(), row));
                }
            }
        }
        let merge_staged = !staged.is_empty();
        let unordered_shadowed = (merge_staged && !keep_order && !descending)
            .then(|| staged.iter().map(|(key, _)| key.clone()).collect());
        let predicates_applied = partition_scans
            .iter()
            .all(|(_, parts)| parts.iter().all(|scan| scan.stream.predicates_applied()));
        let stream: Box<dyn tidb_distsql::remote_scan::PushdownRowStream> = if partitioned {
            let mut per_partition = Vec::with_capacity(partition_scans.len());
            for (physical_id, mut parts) in partition_scans {
                let streams = parts.drain(..).map(|scan| scan.stream).collect::<Vec<_>>();
                let stream = if streams.len() == 1 {
                    streams.into_iter().next().expect("one partition stream")
                } else {
                    tidb_distsql::remote_scan::ChainedPushdownStream::new(streams)
                };
                per_partition.push((physical_id, stream));
            }
            if keep_order {
                Box::new(PartitionMergedPushdownStream::new(
                    per_partition,
                    response_handle_index,
                    common_identity.clone(),
                    self.unsigned_pk_handle(),
                    descending,
                ))
            } else if per_partition.len() == 1 {
                per_partition
                    .into_iter()
                    .next()
                    .expect("one partition stream")
                    .1
            } else {
                tidb_distsql::remote_scan::ChainedPushdownStream::new(
                    per_partition
                        .into_iter()
                        .map(|(_, stream)| stream)
                        .collect(),
                )
            }
        } else {
            let streams = partition_scans
                .into_iter()
                .flat_map(|(_, parts)| parts.into_iter().map(|scan| scan.stream))
                .collect::<Vec<_>>();
            if streams.len() == 1 {
                streams.into_iter().next().expect("one scan stream")
            } else {
                tidb_distsql::remote_scan::ChainedPushdownStream::new(streams)
            }
        };
        // A computing Projection returns its own rows, which carry no
        // identity and cannot be re-filtered here.
        if let Some((_, types)) = &statement.cop_projection_exprs {
            if !predicates_applied || retain_identity || response_physical_id_index.is_some() {
                let mut stream = stream;
                stream.close();
                return Ok(None);
            }
            return Ok(Some(RemoteRowCursor {
                stream,
                staged: staged.into_iter(),
                pending_staged: None,
                pending_remote: None,
                pending_chunk: None,
                pending_chunk_row: 0,
                field_types: types.clone(),
                width: types.len(),
                handle_index: None,
                common_identity: None,
                table_id: self.table_id(),
                physical_id_index: None,
                merge_staged,
                unordered_shadowed,
                unsigned_handle_order: false,
                noted_rows: 0,
                descending,
                predicates_applied,
            }));
        }
        Ok(Some(RemoteRowCursor {
            stream,
            staged: staged.into_iter(),
            pending_staged: None,
            pending_remote: None,
            pending_chunk: None,
            pending_chunk_row: 0,
            // Chunk identity extraction uses the actual response layout.
            field_types: response_offsets.map_or_else(
                || {
                    columns
                        .iter()
                        .map(|column| column.field_type.clone())
                        .collect()
                },
                |offsets| {
                    offsets
                        .into_iter()
                        .map(|offset| columns[offset].field_type.clone())
                        .collect()
                },
            ),
            width: output_offsets.map_or(keep.len(), <[usize]>::len),
            handle_index: response_handle_index,
            common_identity,
            table_id: self.table_id(),
            physical_id_index: response_physical_id_index,
            merge_staged,
            unordered_shadowed,
            unsigned_handle_order: self.unsigned_pk_handle() && (keep_order || descending),
            noted_rows: 0,
            descending,
            predicates_applied,
        }))
    }

    #[allow(clippy::too_many_arguments)]
    fn build_table_reader_from_routed_handles(
        &mut self,
        store: &mut dyn TableStorage,
        handles: &[TableHandle],
        physical_ids: Option<&[i64]>,
        scan_keep: &[usize],
        predicates: &[Expression],
        context: &RowDecodeContext,
        statement: &PushdownStatementContext,
        required_rows: usize,
    ) -> Result<Option<StagedHandlesLookup>, KvTableError> {
        if physical_ids.is_some_and(|ids| ids.len() != handles.len()) {
            return Err(KvTableError::Encode(
                "table reader partition routes lost alignment".into(),
            ));
        }
        if handles.is_empty() || self.has_dirty_content(&statement.staged_writes) {
            return Ok(None);
        }
        let allowed = self.record_physical_ids();
        let inferred;
        let physical_ids = match physical_ids {
            Some(ids) => ids,
            None if allowed.len() == 1 => {
                inferred = vec![allowed[0]; handles.len()];
                &inferred
            }
            None => return Ok(None),
        };
        let mut groups = BTreeMap::<i64, (Vec<TableHandle>, Vec<usize>)>::new();
        for (position, (handle, id)) in handles.iter().zip(physical_ids).enumerate() {
            if !allowed.contains(id) {
                return Err(KvTableError::Encode(
                    "table reader route outside selected partitions".into(),
                ));
            }
            let (group_handles, positions) = groups.entry(*id).or_default();
            group_handles.push(handle.clone());
            positions.push(position);
        }
        // Validate every group's key domain before opening the first stream.
        let groups = groups
            .into_iter()
            .map(|(id, (handles, positions))| {
                Self::table_reader_handle_key_ranges(id, &handles)
                    .map(|ranges| (id, handles, positions, ranges))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let kv_ranges_mem_usage = handle_kv_ranges_mem_usage(
            groups.iter().map(|(_, _, _, (key_ranges, _))| key_ranges.as_slice()),
        );
        // Go `buildTableReaderFromHandles` over a global index's
        // `kv.PartitionHandle`s: one `TableReaderExecutor` of the logical
        // table, `SetPartitionsAndHandles` putting every partition's ranges
        // (`PartitionHandlesToKVRanges`) in its one request. Its unordered
        // table worker forwards the rows as returned, so no partition's rows
        // need telling apart.
        // Its keep-order worker matches each row to its handle's partition
        // by the `ExtraPhysTblID` the reader returns.
        let (groups, handle_partitions) = if groups.len() > 1 {
            let mut merged = (self.table_id(), Vec::new(), Vec::new(), (Vec::new(), Vec::new()));
            let mut partitions = Vec::new();
            for (id, handles, positions, (key_ranges, range_hints)) in groups {
                partitions.extend(std::iter::repeat(id).take(handles.len()));
                merged.1.extend(handles);
                merged.2.extend(positions);
                merged.3 .0.extend(key_ranges);
                merged.3 .1.extend(range_hints);
            }
            (vec![merged], (!statement.lookup_without_identity).then_some(partitions))
        } else {
            (groups, None)
        };
        let statement = statement.for_lookup_batch(handles.len());
        let mut parts = Vec::with_capacity(groups.len());
        for (id, handles, positions, (key_ranges, range_hints)) in groups {
            // The ordinary table keeps its original mutable storage owner.
            // Partition readers need distinct physical metadata, with storage
            // cloning retaining the backend's shared transaction/snapshot.
            let mut physical;
            let table = if self.partition().is_none() && id == self.table_id() {
                &mut *self
            } else {
                physical = self.clone();
                physical.set_table_id(id);
                *physical.partition_mut() = None;
                *physical.read_partitions_mut() = None;
                &mut physical
            };
            let Some(reader) = table.build_physical_table_reader(
                store,
                scan_keep,
                predicates,
                context,
                &statement,
                required_rows,
                &key_ranges,
                &range_hints,
                handle_partitions.clone(),
            )?
            else {
                // Dropping all previously staged cursors closes their streams.
                return Ok(None);
            };
            parts.push((handles, positions, reader));
        }
        Ok(Some(StagedHandlesLookup {
            parts,
            kv_ranges_mem_usage,
        }))
    }

    #[allow(clippy::too_many_arguments)]
    fn build_physical_table_reader(
        &mut self,
        store: &mut dyn TableStorage,
        scan_keep: &[usize],
        predicates: &[Expression],
        context: &RowDecodeContext,
        statement: &PushdownStatementContext,
        required_rows: usize,
        key_ranges: &[(Key, Key)],
        range_hints: &[usize],
        handle_partitions: Option<Vec<i64>>,
    ) -> Result<Option<StagedPhysicalLookup>, KvTableError> {
        // Each cursor retains transport identity after the requested projection.
        let keep = scan_keep;
        let materialization = if keep
            .iter()
            .any(|offset| tidb_table::generated_column::is_virtual(&self.columns()[*offset]))
        {
            Some(RemoteRowMaterialization::new(
                self,
                keep,
                context,
                self.pk_handle_offset().is_none() && self.common_handle_offsets().is_empty(),
            )?)
        } else {
            None
        };
        let physical_predicates = materialization
            .as_ref()
            .map(|projection| projection.remap_predicates(keep, predicates));
        let Some(cursor) = self.pushdown_row_cursor_with_context_and_key_ranges(store, 
            materialization
                .as_ref()
                .map_or(keep, |projection| projection.offsets.as_slice()),
            physical_predicates.as_deref().unwrap_or(predicates),
            None,
            // A virtual column's materialization keeps the TopN's keys
            // above the request; the root TopN still orders the rows.
            statement
                .lookup_table_topn
                .as_ref()
                .filter(|_| materialization.is_none()),
            statement.lookup_table_limit,
            None,
            Some(&range_hints),
            false,
            false,
            // A virtual column's projection reads a heap's row id beside the
            // physical columns, so it keeps the identity.
            !statement.lookup_without_identity || materialization.is_some(),
            handle_partitions.is_some(),
            context,
            &statement,
            Some(&key_ranges),
        )?
        else {
            return Ok(None);
        };
        // The table plan's Limit counts the rows its Selection admits, so a
        // request that could not apply every predicate cannot carry it.
        if (statement.lookup_table_limit.is_some() || statement.lookup_table_topn.is_some())
            && !predicates.is_empty()
            && !cursor.predicates_applied()
        {
            return Ok(None);
        }
        Ok(Some(StagedPhysicalLookup {
            cursor,
            output_width: scan_keep.len(),
            materialization,
            required_rows: required_rows.max(1),
            init_cap: statement.init_chunk_size,
            alloc_pool: statement.alloc_pool.clone(),
            handle_partitions,
        }))
    }

    fn table_reader_handle_key_ranges(
        table_id: i64,
        handles: &[TableHandle],
    ) -> Result<(Vec<(Key, Key)>, Vec<usize>), KvTableError> {
        // TableHandlesToKVRanges requires one handle domain. Reject a corrupt
        // mixed batch before its integer-run coalescer can assert.
        let common = matches!(handles.first(), Some(TableHandle::Common(_)));
        if handles
            .iter()
            .any(|handle| matches!(handle, TableHandle::Common(_)) != common)
        {
            return Err(KvTableError::Encode(
                "table reader mixed handle kinds".to_owned(),
            ));
        }
        let mut sorted = handles.iter().collect::<Vec<_>>();
        sorted.sort_unstable();
        let typed = sorted
            .into_iter()
            .map(|handle| match handle {
                TableHandle::Int(value) => Ok(tidb_txnkv::IntHandle::new(*value).into()),
                TableHandle::Common(bytes) => tidb_txnkv::CommonHandle::new(bytes.clone())
                    .map(Into::into)
                    .map_err(|error| KvTableError::Encode(format!("{error:?}"))),
            })
            .collect::<Result<Vec<tidb_txnkv::Handle>, KvTableError>>()?;
        let (ranges, hints) = tidb_distsql::table_handles_to_kv_ranges(table_id, &typed);
        Ok((
            ranges
                .into_iter()
                .map(|range| (range.start_key, range.end_key))
                .collect(),
            hints,
        ))
    }

    fn finish_routed_rows_by_handles(
        handles: &[TableHandle],
        staged: StagedHandlesLookup,
    ) -> Result<Option<(Vec<(TableHandle, Vec<Datum>)>, bool, u64, Vec<usize>)>, KvTableError> {
        Ok(finish_routed_rows(handles, staged)?
            .map(|(rows, applied, wire_rows, indices, _)| (rows, applied, wire_rows, indices)))
    }

    fn finish_physical_rows(
        handles: &[TableHandle],
        mut staged: StagedPhysicalLookup,
    ) -> Result<Option<(Vec<(TableHandle, Vec<Datum>)>, bool, u64, Vec<usize>, i64)>, KvTableError> {
        let wire_rows = staged.cursor.rows_returned();
        let predicates_applied =
            staged.materialization.is_none() && staged.cursor.predicates_applied();
        let mut rows = Vec::with_capacity(handles.len());
        let handle_index = staged.cursor.handle_index;
        let common_identity = staged.cursor.common_identity.clone();
        // Go's unordered table worker keeps the rows in the order the reader
        // returns them and never reads their handles back.
        let identity = handle_index.is_some() || common_identity.is_some();
        let wire_width = staged.cursor.width;
        let physical_id_index = staged.handle_partitions.as_ref().and(staged.cursor.physical_id_index);
        let mut row_partitions = Vec::new();
        let mut append_row = |mut row: Vec<Datum>| -> Result<(), KvTableError> {
            if let Some(index) = physical_id_index {
                row_partitions.push(match row.get(index) {
                    Some(Datum::Int(id)) => *id,
                    other => {
                        return Err(KvTableError::Decode(format!(
                            "a partition-spanning row carried no partition id, got {other:?}"
                        )))
                    }
                });
            }
            let handle = if identity {
                remote_record_handle(handle_index, common_identity.as_ref(), |position| {
                    row.get(position).cloned()
                })?
            } else {
                handles.get(rows.len()).cloned().ok_or_else(|| {
                    KvTableError::Decode("table reader returned more rows than handles".into())
                })?
            };
            if let Some(projection) = &staged.materialization {
                // Virtual-column evaluation reads physical columns. Only a
                // heap's synthetic rowid belongs beside that projection;
                // common/PK identity has already been reconstructed above.
                row.truncate(wire_width + usize::from(projection.synthetic_handle));
                row = projection.project_row(row)?;
            }
            row.truncate(staged.output_width);
            rows.push((handle, row));
            Ok(())
        };
        // A real coprocessor stream transfers decoded chunks. Drain each
        // chunk once so the stream's row counter and channel are touched once
        // per batch; fakes and row-only backends keep the original contract.
        let mut chunk_memory = 0;
        if staged.cursor.supports_lookup_chunks() {
            loop {
                let Some(batch) = staged
                    .cursor
                    .next_raw_chunk_with_handle(staged.init_cap, staged.required_rows, staged.alloc_pool.as_ref())?
                else {
                    break;
                };
                if batch.num_rows() == 0 {
                    break;
                }
                chunk_memory += batch.memory_usage();
                // Go's tableWorker iterates the decoded chunk and calls
                // GetDatum on each cell; the response decoder has already
                // validated the wire schema.
                let field_types = staged.cursor.field_types();
                let rows = (0..batch.num_rows())
                    .map(|row| batch.get_row(row).get_datum_row(field_types))
                    .collect::<Vec<_>>();
                for row in rows {
                    append_row(row)?;
                }
            }
        } else {
            while let Some(row) = staged.cursor.next_row_with_handle()? {
                append_row(row)?;
            }
        }
        let wire_rows = staged.cursor.rows_returned().saturating_sub(wire_rows);
        if !identity {
            let indices = (0..rows.len()).collect();
            return Ok(Some((rows, predicates_applied, wire_rows, indices, chunk_memory)));
        }
        // A partition-spanning request's handles repeat across partitions.
        let partition_of = |partitions: Option<&Vec<i64>>, position: usize| {
            partitions.and_then(|partitions| partitions.get(position).copied())
        };
        let positions = handles
            .iter()
            .enumerate()
            .map(|(position, handle)| {
                ((partition_of(staged.handle_partitions.as_ref(), position), handle.clone()), position)
            })
            .collect::<HashMap<_, _>>();
        let row_partitions = (!row_partitions.is_empty()).then_some(row_partitions);
        let mut ordered = rows
            .into_iter()
            .enumerate()
            .map(|(ordinal, row)| {
                positions
                    .get(&(partition_of(row_partitions.as_ref(), ordinal), row.0.clone()))
                    .copied()
                    .map(|position| (position, row))
                    .ok_or_else(|| {
                        KvTableError::Decode("table reader returned an unrequested handle".into())
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        ordered.sort_by_key(|(position, _)| *position);
        let (indices, rows) = ordered.into_iter().unzip();
        Ok(Some((rows, predicates_applied, wire_rows, indices, chunk_memory)))
    }








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

    fn scan_rows_with_handles_with_context(
        &mut self,
        store: &mut dyn TableStorage,
        context: &RowDecodeContext,
    ) -> Result<Vec<(TableHandle, Vec<Datum>)>, KvTableError> {
        self.scan_rows_with_handles_in_with_context(store, None, context)
    }



    fn scan_rows_with_handles_in_with_context(
        &mut self,
        store: &mut dyn TableStorage,
        handle_ranges: Option<&[Range]>,
        context: &RowDecodeContext,
    ) -> Result<Vec<(TableHandle, Vec<Datum>)>, KvTableError> {
        // UPDATE/DELETE use the same table reader as SELECT in Go. Keep the
        // record identity beside its values rather than opening a separate
        // snapshot iterator (and RPC) for every range on the write path.
        let keep: Vec<usize> = self
            .columns()
            .iter()
            .enumerate()
            .filter_map(|(offset, column)| {
                (!tidb_table::generated_column::is_virtual(column)).then_some(offset)
            })
            .collect();
        if let Some(mut cursor) = self.pushdown_row_cursor_with_context(store, 
            &keep,
            &[],
            None,
            None,
            None,
            handle_ranges,
            None,
            false,
            false,
            true,
            context,
            &crate::remote_scan::pushdown_statement_context(tidb_sessionctx::stmt_context_of(context.expression())),
        )? {
            let decoder = self.row_decoder_projected(None, context)?;
            let mut rows = Vec::new();
            loop {
                let Some((key, projected)) = cursor.next_keyed_row()? else {
                    break;
                };
                let handle = decoder.record_handle(&key)?;
                let row = if keep.len() == self.columns().len() {
                    projected
                } else {
                    decoder.project_remote_row(&keep, projected)?
                };
                rows.push((handle, row));
            }
            return Ok(rows);
        }
        let mut cursor = self.row_cursor_projected_with_context(store, None, handle_ranges, context)?;
        let mut rows = Vec::new();
        while let Some(entry) = cursor.next_row()? {
            rows.push(entry);
        }
        Ok(rows)
    }

    fn scan_rows_with_handles_recomputed(
        &mut self,
        store: &mut dyn TableStorage,
        context: &RowDecodeContext,
    ) -> Result<Vec<(TableHandle, Vec<Datum>)>, KvTableError> {
        let decoder = self.row_decoder_recomputed(context)?;
        let mut cursor =
            self.row_cursor_with_decoder(store, decoder, None, false, false, context.zone())?;
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

/// One row of a merge side, addressed by its record key.
type KeyedRow = (Vec<u8>, Vec<Datum>);

/// One staged write of the same range: `None` is a staged delete.
type StagedRow = (Vec<u8>, Option<Vec<Datum>>);

/// A staged routed table task. The request builder opens its physical streams;
/// the shared row/chunk completion path retains original handle positions.
/// Dropping an unfinished task closes every cursor.
pub struct StagedHandlesLookup {
    parts: Vec<(Vec<TableHandle>, Vec<usize>, StagedPhysicalLookup)>,
    /// Go `kv.KeyRangeSliceMemUsage(tableReader.kvRanges)`: every part's
    /// handle ranges appended into one slice (`KeyRanges.AppendSelfTo`).
    kv_ranges_mem_usage: i64,
}

struct StagedPhysicalLookup {
    cursor: RemoteRowCursor,
    output_width: usize,
    materialization: Option<RemoteRowMaterialization>,
    required_rows: usize,
    /// Go `TableReaderExecutor.InitCap()`: the capacity each chunk the
    /// task's reader fills starts with.
    init_cap: usize,
    /// Go `TableReaderExecutor.AllocPool`, which each of those chunks comes
    /// from.
    alloc_pool: Option<tidb_distsql::remote_scan::AllocPool>,
    /// The partition of each requested handle when the request spans
    /// partitions (Go's `kv.PartitionHandle`s): its rows carry
    /// `ExtraPhysTblID` and match by partition and handle.
    handle_partitions: Option<Vec<i64>>,
}

/// [`TableCommon::finish_routed_rows_by_handles`] with Go's
/// `chk.MemoryUsage()` of every chunk the parts' readers filled.
#[allow(clippy::type_complexity)]
fn finish_routed_rows(
    handles: &[TableHandle],
    staged: StagedHandlesLookup,
) -> Result<Option<(Vec<(TableHandle, Vec<Datum>)>, bool, u64, Vec<usize>, i64)>, KvTableError> {
    let mut ordered = Vec::with_capacity(handles.len());
    let mut applied = true;
    let mut wire_rows = 0;
    let mut chunk_memory = 0;
    for (part_handles, positions, reader) in staged.parts {
        let Some((rows, part_applied, count, indices, chunks)) =
            TableCommon::finish_physical_rows(&part_handles, reader)?
        else {
            return Ok(None);
        };
        applied &= part_applied;
        wire_rows += count;
        chunk_memory += chunks;
        ordered.extend(
            rows.into_iter()
                .zip(indices)
                .map(|(row, index)| (positions[index], row)),
        );
    }
    ordered.sort_by_key(|(index, _)| *index);
    let (indices, rows) = ordered.into_iter().unzip();
    Ok(Some((rows, applied, wire_rows, indices, chunk_memory)))
}

/// Go `kv.KeyRangeSliceMemUsage` of the slice `KeyRanges.AppendSelfTo(nil)`
/// builds from `groups` -- one `append` per group, growing as Go's `append`
/// does. Go allocates each handle key exactly (`tablecodec.EncodeRowKey`),
/// so a key's capacity is its length.
fn handle_kv_ranges_mem_usage<'a>(groups: impl IntoIterator<Item = &'a [(Key, Key)]>) -> i64 {
    // Go `unsafe.Sizeof(kv.KeyRange{})`: two slice headers.
    const SIZEOF_KEY_RANGE: usize = 48;
    let mut len = 0_usize;
    let mut capacity = 0_usize;
    let mut key_bytes = 0_usize;
    for ranges in groups {
        if len + ranges.len() > capacity {
            capacity = tidb_goruntime::go_runtime::go_64_next_slice_capacity_for_element(
                len + ranges.len(),
                capacity,
                SIZEOF_KEY_RANGE,
                tidb_goruntime::go_runtime::GoSliceElementLayout::PointerBearing,
            );
        }
        len += ranges.len();
        key_bytes += ranges
            .iter()
            .map(|(start, end)| start.as_bytes().len() + end.as_bytes().len())
            .sum::<usize>();
    }
    i64::try_from(SIZEOF_KEY_RANGE * capacity + key_bytes).unwrap_or(i64::MAX)
}

/// The logical row above a physical table scan, shared by table readers and
/// index-lookup table probes. Transport-only identity stays outside the schema.
struct RemoteRowMaterialization {
    decoder: RowDecoder,
    /// The scan's columns, virtual ones included: Go's `TableScan.Columns`
    /// carries them, the store answers their default, and
    /// `FillVirtualColumnValue` computes them above the reader. A table
    /// whose only read column is virtual still scans one column, so its rows
    /// are counted.
    offsets: Vec<usize>,
    /// The stored columns among `offsets`, which the decoder takes as read.
    physical: Vec<usize>,
    synthetic_handle: bool,
}

impl RemoteRowMaterialization {
    fn new(
        table: &TableCommon,
        keep: &[usize],
        context: &RowDecodeContext,
        synthetic_handle: bool,
    ) -> Result<Self, KvTableError> {
        let decoder = table.row_decoder_projected(Some(keep), context)?;
        let offsets = decoder.decoded_offsets();
        let physical = decoder.physical_offsets();
        Ok(Self {
            decoder,
            offsets,
            physical,
            synthetic_handle,
        })
    }

    /// The conditions in the materialized row space. One that reads a column
    /// the wire does not carry keeps the whole list local: the request never
    /// carries a subset of a Selection it then reports on.
    fn remap_predicates(&self, keep: &[usize], predicates: &[Expression]) -> Vec<Expression> {
        // A virtual column's wire value is only its default, so a predicate
        // reading one stays above the materialization.
        let layout: Vec<_> = self
            .offsets
            .iter()
            .map(|offset| {
                if !self.physical.contains(offset) {
                    return usize::MAX;
                }
                keep.iter()
                    .position(|kept| kept == offset)
                    .unwrap_or(usize::MAX)
            })
            .collect();
        predicates
            .iter()
            .map(|predicate| {
                let mut predicate = predicate.clone();
                tidb_executor_internal_exec::predicate_pushdown::remap_expression(&mut predicate, &layout)
                    .map(|()| predicate)
            })
            .collect::<Option<Vec<_>>>()
            .unwrap_or_default()
    }

    fn project_row(&self, mut row: Vec<Datum>) -> Result<Vec<Datum>, KvTableError> {
        let handle = if self.synthetic_handle {
            Some(row.pop().ok_or_else(|| {
                KvTableError::Decode("remote row omitted its synthetic handle".to_owned())
            })?)
        } else {
            None
        };
        // Drop the virtual columns' wire defaults; the decoder computes them.
        let row = if self.physical.len() == self.offsets.len() {
            row
        } else {
            self.offsets
                .iter()
                .zip(row)
                .filter(|(offset, _)| self.physical.contains(offset))
                .map(|(_, value)| value)
                .collect()
        };
        let mut row = self.decoder.project_remote_row(&self.physical, row)?;
        if let Some(handle) = handle {
            row.push(handle);
        }
        Ok(row)
    }
}

#[derive(Clone)]
struct RemoteCommonHandle {
    /// Wire position, declared prefix length and type of each key part.
    parts: Vec<(usize, i64, FieldType)>,
    zone: SessionTimeZone,
    use_new_collation: bool,
}

impl RemoteCommonHandle {
    #[cfg(test)]
    fn record_handle(&self, row: &[Datum]) -> Result<TableHandle, KvTableError> {
        self.record_handle_with(|position| row.get(position).cloned())
    }

    fn record_handle_with(
        &self,
        mut datum: impl FnMut(usize) -> Option<Datum>,
    ) -> Result<TableHandle, KvTableError> {
        // Go CommonHandleCols.buildHandleByDatumsBuffer truncates key parts
        // before EncodeKey, then NewCommonHandle pads short encodings.
        let values = self
            .parts
            .iter()
            .map(|(position, prefix, field_type)| {
                let mut value = datum(*position).ok_or_else(|| {
                    KvTableError::Decode("remote row omitted a common handle column".to_owned())
                })?;
                tidb_tablecodec::cut_datum_by_prefix_len(&mut value, *prefix, field_type);
                Ok(value)
            })
            .collect::<Result<Vec<_>, KvTableError>>()?;
        let bytes = Encoder::new(self.use_new_collation)
            .encode_key_in_timezone(&self.zone, &values)
            .map_err(|error| KvTableError::Encode(format!("{error:?}")))?;
        let handle = tidb_txnkv::CommonHandle::new(bytes)
            .map_err(|error| KvTableError::Encode(format!("{error:?}")))?;
        Ok(TableHandle::Common(handle.encoded().to_vec()))
    }
}

/// Reconstructs the shared transport identity without materializing a whole
/// chunk row. CommonHandleCols and IntHandleCols supply this contract in Go.
fn remote_record_handle(
    handle_index: Option<usize>,
    common_identity: Option<&RemoteCommonHandle>,
    mut datum: impl FnMut(usize) -> Option<Datum>,
) -> Result<TableHandle, KvTableError> {
    if let Some(identity) = common_identity {
        return identity.record_handle_with(datum);
    }
    match handle_index.and_then(&mut datum) {
        Some(Datum::Int(value)) => Ok(TableHandle::Int(value)),
        Some(Datum::UInt(value)) => Ok(TableHandle::Int(value as i64)),
        _ => Err(KvTableError::Decode(
            "remote row omitted its integer handle".to_owned(),
        )),
    }
}

/// Merges clean partition streams by their encoded handle, independently of
/// the physical table ID. The common tuple encoder preserves key collation
/// and prefix rules; unsigned integer handles retain value ordering.
struct PartitionMergedPushdownStream {
    parts: Vec<(i64, Box<dyn PushdownRowStream>)>,
    pending: Vec<Option<(Vec<u8>, Vec<Datum>)>>,
    handle_index: Option<usize>,
    common_identity: Option<RemoteCommonHandle>,
    unsigned_handle: bool,
    descending: bool,
}

impl PartitionMergedPushdownStream {
    fn new(
        parts: Vec<(i64, Box<dyn PushdownRowStream>)>,
        handle_index: Option<usize>,
        common_identity: Option<RemoteCommonHandle>,
        unsigned_handle: bool,
        descending: bool,
    ) -> Self {
        let pending = (0..parts.len()).map(|_| None).collect();
        Self {
            parts,
            pending,
            handle_index,
            common_identity,
            unsigned_handle,
            descending,
        }
    }

    fn row_key(&self, physical_id: i64, row: &[Datum]) -> Result<Vec<u8>, StorageError> {
        let handle = remote_record_handle(
            self.handle_index,
            self.common_identity.as_ref(),
            |position| row.get(position).cloned(),
        )
        .map_err(|error| StorageError::Backend(format!("{error:?}")))?;
        Ok(record_merge_key(
            &encode_row_key_with_handle(physical_id, &handle.record_handle()),
            self.unsigned_handle,
        ))
    }
}

impl PushdownRowStream for PartitionMergedPushdownStream {
    fn next_row(&mut self) -> Result<Option<Vec<Datum>>, StorageError> {
        for index in 0..self.parts.len() {
            if self.pending[index].is_none() {
                if let Some(row) = self.parts[index].1.next_row()? {
                    let key = self.row_key(self.parts[index].0, &row)?;
                    self.pending[index] = Some((key, row));
                }
            }
        }
        let mut selected: Option<usize> = None;
        for (index, pending) in self.pending.iter().enumerate() {
            let Some((key, _)) = pending else {
                continue;
            };
            let better = selected.is_none_or(|current| {
                let current_key = self.pending[current]
                    .as_ref()
                    .expect("selected partition has a pending row")
                    .0
                    .as_slice();
                if self.descending {
                    key.as_slice() > current_key
                } else {
                    key.as_slice() < current_key
                }
            });
            if better {
                selected = Some(index);
            }
        }
        let Some(index) = selected else {
            return Ok(None);
        };
        Ok(self.pending[index].take().map(|(_, row)| row))
    }

    fn rows_returned(&self) -> u64 {
        self.parts
            .iter()
            .map(|(_, stream)| stream.rows_returned())
            .sum()
    }

    fn predicates_applied(&self) -> bool {
        self.parts
            .iter()
            .all(|(_, stream)| stream.predicates_applied())
    }

    fn close(&mut self) {
        for (_, stream) in &mut self.parts {
            stream.close();
        }
        self.pending.fill(None);
    }
}

/// A remote table stream with its staged overlay, merged by record identity.
///
/// Like Go's UnionScan, staged replacements and tombstones win over the
/// snapshot. Ordered reads merge by key; unordered reads suppress shadowed
/// snapshot keys and append staged rows, without ordering region responses.
/// The caller evaluates residual predicates after merging. Clean reads keep
/// the direct row/chunk path without reconstructing a key for every row.
pub struct RemoteRowCursor {
    stream: Box<dyn PushdownRowStream>,
    staged: std::vec::IntoIter<StagedRow>,
    pending_staged: Option<StagedRow>,
    pending_remote: Option<KeyedRow>,
    /// Unconsumed rows from one clean, decoded columnar response batch.
    pending_chunk: Option<Chunk>,
    pending_chunk_row: usize,
    /// Field types of the decoded wire columns. Empty for hand-built test
    /// cursors and row-only backends, which retain the old row path.
    field_types: Vec<FieldType>,
    /// Number of projected columns, which the remote row may exceed by the
    /// appended identity columns.
    width: usize,
    /// Where the integer handle sits in a row to be merged. Composite-key
    /// rows instead carry all key parts in `common_identity`.
    handle_index: Option<usize>,
    common_identity: Option<RemoteCommonHandle>,
    table_id: i64,
    /// Where the storage-filled `ExtraPhysTblID` sits in a remote row, when
    /// it was requested: the row's record key then names that partition
    /// instead of the logical table.
    physical_id_index: Option<usize>,
    /// Whether the cursor must merge staged rows into the snapshot stream.
    /// Clean reads can consume projected remote rows directly without
    /// reconstructing an encoded record key for every row.
    merge_staged: bool,
    /// All staged identities, including tombstones. Kept for the entire
    /// unordered scan so a late snapshot row can never revive an old value.
    unordered_shadowed: Option<HashSet<Vec<u8>>>,
    /// Ordered unsigned range groups follow handle VALUE order, not the
    /// signed order of their record-key bytes. Both merge inputs use this
    /// domain; unordered reads retain physical key order.
    unsigned_handle_order: bool,
    /// How much of [`PushdownRowStream::rows_returned`] has already been
    /// reported to the storage probe, so each row is counted once. See
    /// [`note_wire_rows`].
    noted_rows: u64,
    /// Both merge inputs arrive in DESCENDING key order (a `desc` request's
    /// remote stream, and the staged slice the storage reversed to match),
    /// so the winner of each step is the GREATER key.
    descending: bool,
    /// Whether the remote backend lowered every requested predicate.
    predicates_applied: bool,
}

impl RemoteRowCursor {
    /// Go `RuntimeStatsColl.GetCopCountAndRows` for this remote scan.
    #[must_use]
    pub fn get_cop_count_and_rows(&self) -> (u64, u64) {
        self.stream.cop_count_and_rows()
    }

    /// How many rows have crossed the network so far: the wire receipt.
    #[must_use]
    pub fn rows_returned(&self) -> u64 {
        self.stream.rows_returned()
    }

    /// Whether the remote backend evaluated every requested predicate.
    #[must_use]
    pub fn predicates_applied(&self) -> bool {
        self.predicates_applied
    }

    /// Whether this cursor can preserve Go's chunk.Row handoff through a
    /// table-lookup worker. A staged merge must stay on the row path because
    /// its snapshot/staged ordering is resolved one row at a time.
    pub(crate) fn supports_lookup_chunks(&self) -> bool {
        !self.merge_staged && self.stream.supports_chunks() && !self.field_types.is_empty()
    }
    /// Field types of the decoded wire columns (empty for row-only backends).
    pub(crate) fn field_types(&self) -> &[FieldType] {
        &self.field_types
    }

    /// Reports rows received since the last report to
    /// [`tidb_kv::storage::capture_storage_ops`]'s probe.
    ///
    /// The stream's own counter is the authority on what crossed the network
    /// -- a batching transport may already hold rows this cursor has not
    /// pulled -- so the probe takes its DELTA rather than counting pulls. The
    /// drop below takes the last delta, which is how an early-stopping
    /// `LIMIT` still reports the batch it abandoned.
    fn note_wire_rows(&mut self) {
        let returned = self.stream.rows_returned();
        let fresh = returned.saturating_sub(self.noted_rows);
        self.noted_rows = returned;
        if fresh > 0 {
            tidb_kv::storage::note_storage_op(|ops| ops.cop_rows += fresh);
        }
    }

    /// The next remote row, as its record key and its projected columns.
    fn next_remote(&mut self) -> Result<Option<KeyedRow>, KvTableError> {
        if self.pending_remote.is_none() {
            self.pending_remote = self.read_remote()?;
        }
        Ok(self.pending_remote.clone())
    }

    fn read_remote(&mut self) -> Result<Option<KeyedRow>, KvTableError> {
        let next = self.stream.next_row().map_err(KvTableError::from)?;
        self.note_wire_rows();
        let Some(mut row) = next else {
            return Ok(None);
        };
        let handle = remote_record_handle(
            self.handle_index,
            self.common_identity.as_ref(),
            |position| row.get(position).cloned(),
        )?;
        let table_id = match self.physical_id_index.and_then(|index| row.get(index)) {
            Some(Datum::Int(id)) => *id,
            _ => self.table_id,
        };
        row.truncate(self.width);
        let key = encode_row_key_with_handle(table_id, &handle.record_handle());
        Ok(Some((key, row)))
    }

    fn next_staged(&mut self) -> Option<StagedRow> {
        if self.pending_staged.is_none() {
            self.pending_staged = self.staged.next();
        }
        self.pending_staged.clone()
    }

    /// The next projected row, directly from a clean stream or from the
    /// shared integer/composite-handle snapshot/staged merge.
    pub fn next_row(&mut self) -> Result<Option<Vec<Datum>>, KvTableError> {
        if !self.merge_staged {
            let next = self.stream.next_row().map_err(KvTableError::from)?;
            self.note_wire_rows();
            return Ok(next.map(|mut row| {
                row.truncate(self.width);
                row
            }));
        }
        self.next_keyed_row().map(|row| row.map(|(_, row)| row))
    }

    /// The shared keyed merge with record identity retained for writers.
    /// Its ordering and tombstone rules are shared with projected readers.
    fn next_keyed_row(&mut self) -> Result<Option<KeyedRow>, KvTableError> {
        if self.unordered_shadowed.is_some() {
            // Go UnionScan's snapshot getter drops every snapshot row whose
            // key is in the mutation buffer, whatever order the rows arrive
            // in -- replacement rows and inserts come from the added rows,
            // including when their old snapshot row was filtered. What
            // remains is merged with the added rows exactly as
            // `UnionScanExec.getOneRow` does: the snapshot row is emitted
            // while it compares below the added row, so a replaced row that
            // sorts before the next snapshot row comes out before it.
            loop {
                let remote = loop {
                    let Some(entry) = self.next_remote()? else {
                        break None;
                    };
                    if self.unordered_shadowed.as_ref().unwrap().contains(&entry.0) {
                        self.pending_remote = None;
                        continue;
                    }
                    break Some(entry);
                };
                // A tombstone contributes no row.
                let staged = loop {
                    match self.next_staged() {
                        Some((_, None)) => self.pending_staged = None,
                        other => break other,
                    }
                };
                match (remote, staged) {
                    (None, None) => return Ok(None),
                    (Some(entry), None) => {
                        self.pending_remote = None;
                        return Ok(Some(entry));
                    }
                    (None, Some((key, row))) => {
                        self.pending_staged = None;
                        let row = row.expect("tombstones were skipped above");
                        return Ok(Some((key, row)));
                    }
                    (Some(entry), Some((staged_key, row))) => {
                        let ordering = if self.unsigned_handle_order {
                            record_merge_bytes(&entry.0, true)
                                .cmp(record_merge_bytes(&staged_key, true))
                        } else {
                            entry.0.as_slice().cmp(staged_key.as_slice())
                        };
                        if ordering == std::cmp::Ordering::Less {
                            self.pending_remote = None;
                            return Ok(Some(entry));
                        }
                        self.pending_staged = None;
                        let row = row.expect("tombstones were skipped above");
                        return Ok(Some((staged_key, row)));
                    }
                }
            }
        }
        loop {
            let remote = self.next_remote()?;
            let staged = self.next_staged();
            match (remote, staged) {
                (None, None) => return Ok(None),
                (Some(entry), None) => {
                    self.pending_remote = None;
                    return Ok(Some(entry));
                }
                (remote, Some((staged_key, staged_row))) => {
                    // A staged write of the same key is the transaction's own
                    // newer version of that row, so it replaces the snapshot's
                    // and a tombstone drops it entirely.
                    if let Some((remote_key, _)) = &remote {
                        let ordering = if self.unsigned_handle_order {
                            record_merge_bytes(remote_key, true)
                                .cmp(record_merge_bytes(&staged_key, true))
                        } else {
                            remote_key.as_slice().cmp(staged_key.as_slice())
                        };
                        let ordering = if self.descending {
                            ordering.reverse()
                        } else {
                            ordering
                        };
                        match ordering {
                            std::cmp::Ordering::Less => {
                                return Ok(self.pending_remote.take());
                            }
                            std::cmp::Ordering::Equal => self.pending_remote = None,
                            std::cmp::Ordering::Greater => {}
                        }
                    }
                    self.pending_staged = None;
                    if let Some(row) = staged_row {
                        return Ok(Some((staged_key, row)));
                    }
                }
            }
        }
    }

    /// Returns the next clean remote row without truncating the synthetic
    /// integer handle column.  Index-lookup table fetches use this to restore
    /// handle-to-row association; normal scan consumers must use `next_row`
    /// so transport-only columns never escape the projected schema.
    pub fn next_row_with_handle(&mut self) -> Result<Option<Vec<Datum>>, KvTableError> {
        debug_assert!(!self.merge_staged);
        let next = self.stream.next_row().map_err(KvTableError::from)?;
        self.note_wire_rows();
        Ok(next)
    }

    /// Pulls one decoded response chunk without materializing its rows.
    ///
    /// The response decoder already validated the wire schema. This is the
    /// chunk.Row equivalent of Go's `exec.Next` result handed to
    /// `tableWorker.executeTask`; the caller decides whether to retain the
    /// chunk or use the compatibility row conversion above.
    ///
    /// Go `tableWorker.executeTask`'s loop: each call is one
    /// `chk := exec.TryNewCacheChunk(tableReader)` -- a chunk starting at
    /// `init_cap` -- filled by `exec.Next(ctx, tableReader, chk)`, so the
    /// chunk's `MemoryUsage` is the one Go's worker charges.
    fn next_raw_chunk_with_handle(
        &mut self,
        init_cap: usize,
        required_rows: usize,
        alloc_pool: Option<&tidb_distsql::remote_scan::AllocPool>,
    ) -> Result<Option<Chunk>, KvTableError> {
        if !self.supports_lookup_chunks() {
            return Ok(None);
        }
        let required_rows = required_rows.max(1);
        let mut batch = tidb_executor_internal_exec::new_chunk_with_alloc_pool(
            alloc_pool.map(|pool| &pool.0),
            &self.field_types,
            init_cap.min(required_rows),
            required_rows,
        );
        let filled = self
            .stream
            .next_chunk_into(required_rows, &mut batch)
            .map_err(KvTableError::from)?;
        self.note_wire_rows();
        if !filled {
            return Ok(Some(Chunk::new_chunk_with_capacity(
                &self.field_types[..self.width.min(self.field_types.len())],
                0,
            )));
        }
        if batch.num_cols() > self.field_types.len() {
            return Err(KvTableError::Decode(format!(
                "a coprocessor row carried {} columns but only {} field types were requested",
                batch.num_cols(),
                self.field_types.len()
            )));
        }
        Ok(Some(batch))
    }
}

impl Drop for RemoteRowCursor {
    /// An abandoned cursor (an early-stopping `LIMIT`) must still release the
    /// request, which a drained stream's explicit `close` would have done.
    fn drop(&mut self) {
        self.note_wire_rows();
        self.stream.close();
    }
}

/// A handle-only view of a remote TiKV index scan.

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
