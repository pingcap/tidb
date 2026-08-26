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

use super::row_decoder::RowDecoder;
use super::{
    index_entry_handle, IndexRange, KvIndex, KvTable, KvTableError, RowDecodeContext, TableHandle,
};
use crate::executor::{ExecError, Executor, ExecutorMeta};
use crate::predicate_pushdown::ScanPredicate;
use crate::remote_scan::{
    PushdownAggregateKind, PushdownIndexScan, PushdownPartialAggregate, PushdownRowStream,
    PushdownScanColumn, PushdownScanRequest, PushdownStatementContext, PushdownTopN,
    EXTRA_HANDLE_COLUMN_ID,
};
use crate::storage::StorageIterator;
use std::collections::{BTreeMap, BinaryHeap, HashMap, HashSet};
use tidb_chunk::chunk::Chunk;
use tidb_codec::table_key::{
    cut_index_prefix, cut_row_key_prefix, decode_table_id, encode_index_seek_key,
    encode_row_key_with_handle, get_table_handle_key_range, RecordHandle,
};
use tidb_codec::Encoder;
use tidb_datatype::{Datum, Decimal, FieldType, SessionTimeZone};
use tidb_expr::schema::Schema;
use tidb_txnkv::Key;

/// The read direction of [`KvTable`]: the entry points that open a decoder
/// or a cursor over the stored bytes.
impl KvTable {
    /// The row-decoding metadata, detached from the table and optionally
    /// narrowed to the columns at `keep` (offsets into [`KvTable::columns`],
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
    ) -> Result<RowDecoder, KvTableError> {
        let decoder = RowDecoder::for_table_read(
            self.columns.clone(),
            self.pk_handle_offset,
            self.common_handle_offsets.clone(),
            keep,
            self.use_new_collation,
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
            self.columns.clone(),
            self.pk_handle_offset,
            self.common_handle_offsets.clone(),
            self.use_new_collation,
            context.clone(),
        )
    }

    /// A forward cursor over the table's record-key range, in key order.
    ///
    /// This is the streaming form of [`KvTable::scan_rows_with_handles`]: the
    /// storage iterator stays open and one row is *decoded* per pull, so the
    /// decoded rows alive at once are the caller's chunk rather than the whole
    /// relation. (How far the laziness reaches below the storage seam is a
    /// property of the backend's `iter`; see [`crate::access_path`].) The seam
    /// returns an owned iterator, so the cursor does not borrow the table and
    /// a caller may hold it across chunk boundaries.
    pub fn row_cursor_with_context(
        &mut self,
        context: &RowDecodeContext,
    ) -> Result<RowCursor, KvTableError> {
        self.row_cursor_projected_with_context(None, None, context)
    }

    /// Legacy zone-only cursor retained while write callers await an explicit
    /// statement-class migration. Origin defaults use the exact former
    /// `DEFAULT_STATEMENT_FLAGS` behavior.
    pub fn row_cursor(&mut self, zone: &SessionTimeZone) -> Result<RowCursor, KvTableError> {
        self.row_cursor_with_context(&RowDecodeContext::legacy_default(zone))
    }

    /// [`KvTable::row_cursor`] narrowed to the columns at `keep`: the cursor
    /// decodes and yields exactly those columns, in `keep`'s order.
    ///
    /// `handle_ranges` narrows which RECORDS are read, as
    /// [`crate::table_access::TableAccess::accept_handle_ranges`] describes:
    /// `None` reads the whole table.
    pub fn row_cursor_projected_with_context(
        &mut self,
        keep: Option<&[usize]>,
        handle_ranges: Option<&[IndexRange]>,
        context: &RowDecodeContext,
    ) -> Result<RowCursor, KvTableError> {
        self.row_cursor_projected_directed_with_context(keep, handle_ranges, false, false, context)
    }

    /// [`Self::row_cursor_projected_with_context`] with a walk direction:
    /// `descending` reads the ranges last-to-first, each backwards -- the
    /// local half of Go's `desc` table scan.
    pub fn row_cursor_projected_directed_with_context(
        &mut self,
        keep: Option<&[usize]>,
        handle_ranges: Option<&[IndexRange]>,
        descending: bool,
        ordered: bool,
        context: &RowDecodeContext,
    ) -> Result<RowCursor, KvTableError> {
        let decoder = self.row_decoder_projected(keep, context)?;
        self.row_cursor_with_decoder(decoder, handle_ranges, descending, ordered, context.zone())
    }

    /// Looks up several record handles through one storage batch and decodes
    /// them with one row decoder. The returned slots preserve input handle
    /// order and use `None` for an index entry whose record disappeared.
    pub(crate) fn get_rows_by_handles_projected_with_context(
        &mut self,
        handles: &[TableHandle],
        keep: Option<&[usize]>,
        context: &RowDecodeContext,
    ) -> Result<Vec<Option<Vec<Datum>>>, KvTableError> {
        if handles.is_empty() {
            return Ok(Vec::new());
        }
        let physical_ids = self.record_physical_ids();
        let mut probes = Vec::with_capacity(handles.len() * physical_ids.len());
        for handle in handles {
            for physical_id in &physical_ids {
                probes.push((
                    Key::from_bytes(encode_row_key_with_handle(
                        *physical_id,
                        &handle.record_handle(),
                    )),
                    handle.clone(),
                ));
            }
        }
        let keys: Vec<Key> = probes.iter().map(|(key, _)| key.clone()).collect();
        let entries = self
            .store
            .batch_get(&keys)
            .map_err(|error| KvTableError::Storage(format!("{error:?}")))?;
        // Keep the batch lookup linear. Scanning `probes` for every handle
        // makes a 20k-row index batch quadratic before row decoding starts;
        // Go's batch table reader indexes each returned record once.
        let mut probe_indices = BTreeMap::<TableHandle, Vec<usize>>::new();
        for (index, (_, handle)) in probes.iter().enumerate() {
            probe_indices.entry(handle.clone()).or_default().push(index);
        }
        let decoder = self.row_decoder_projected(keep, context)?;
        let mut rows = Vec::with_capacity(handles.len());
        for handle in handles {
            let entry = probe_indices
                .get(handle)
                .into_iter()
                .flatten()
                .find_map(|index| entries.get(&probes[*index].0));
            let Some(entry) = entry else {
                rows.push(None);
                continue;
            };
            let (mut values, _) = decoder.decode_and_eval(handle, entry)?.into_parts();
            if let Some(keep) = keep {
                values = keep
                    .iter()
                    .map(|offset| std::mem::replace(&mut values[*offset], Datum::Null))
                    .collect();
            }
            rows.push(Some(values));
        }
        Ok(rows)
    }

    fn row_cursor_with_decoder(
        &mut self,
        decoder: RowDecoder,
        handle_ranges: Option<&[IndexRange]>,
        descending: bool,
        ordered: bool,
        zone: &SessionTimeZone,
    ) -> Result<RowCursor, KvTableError> {
        // Go's `byItems` half of `needMergeSort` is set because the table is
        // PARTITIONED (`find_best_task.go:2960`), so the count that decides
        // the merge is the number of physical tables, not the number of key
        // ranges: `WHERE id IN (1,5,9)` on an unpartitioned table opens three
        // ranges that are already disjoint and ascending, and Go concatenates
        // them. This is the same rule the index path states as
        // `physical_ids.len() > 1`.
        let physical_count = self.record_physical_ids().len();
        let mut iterators = Vec::new();
        for (low, upper) in self.record_key_ranges(handle_ranges, zone, ordered || descending)? {
            iterators.push(
                if descending {
                    self.store.iter_reverse(Some(&upper), Some(&low))
                } else {
                    self.store.iter(Some(&low), Some(&upper))
                }
                .map_err(|e| KvTableError::Storage(format!("{e:?}")))?,
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

    /// Legacy zone-only projected cursor; see [`KvTable::row_cursor`].
    pub fn row_cursor_projected(
        &mut self,
        keep: Option<&[usize]>,
        handle_ranges: Option<&[IndexRange]>,
        zone: &SessionTimeZone,
    ) -> Result<RowCursor, KvTableError> {
        self.row_cursor_projected_with_context(
            keep,
            handle_ranges,
            &RowDecodeContext::legacy_default(zone),
        )
    }

    /// Reads only the first row covered by `handle_ranges`, using the
    /// storage backend's bounded primitive when it has one. This is the
    /// byte-level counterpart of a `TableRangeScan` with `LIMIT 1`.
    pub fn first_row_in_handle_ranges(
        &mut self,
        keep: Option<&[usize]>,
        handle_ranges: &[IndexRange],
        zone: &SessionTimeZone,
    ) -> Result<Option<(TableHandle, Vec<Datum>)>, KvTableError> {
        let context = RowDecodeContext::legacy_default(zone);
        let decoder = self.row_decoder_projected(keep, &context)?;
        for (low, upper) in self.record_key_ranges(Some(handle_ranges), zone, false)? {
            let Some((key, value)) = self
                .store
                .first(Some(&low), Some(&upper))
                .map_err(|error| KvTableError::Storage(format!("{error:?}")))?
            else {
                continue;
            };
            return decoder.decode_record(key.as_bytes(), &value).map(Some);
        }
        Ok(None)
    }

    /// The record ranges this scan reads, as the storage seam's half-open
    /// `[start, end)` pairs in ascending key order.
    ///
    /// With no handle ranges this is the ONE range the whole relation lives
    /// in ([`KvTable::record_key_range`]). With them it is the intervals
    /// [`crate::handle_range::record_key_ranges`] encodes, which is what
    /// makes a `TableRangeScan` read less than the table. `keep_order`
    /// selects Go's ordered-read half order versus his merged unordered wire
    /// order, exactly as [`crate::handle_range::record_key_ranges`] documents.
    fn record_key_ranges(
        &self,
        handle_ranges: Option<&[IndexRange]>,
        zone: &SessionTimeZone,
        keep_order: bool,
    ) -> Result<Vec<(Key, Key)>, KvTableError> {
        let full_common_handle = [IndexRange {
            low: vec![Datum::MinNotNull],
            high: vec![Datum::MaxValue],
            low_exclusive: false,
            high_exclusive: false,
        }];
        // Go `ranger.FullIntRange(isUnsigned)`: a scan with no `WHERE` still
        // carries the handle domain as ONE range, and the table reader splits
        // it like any other (`table_reader.go:295`). For an UNSIGNED handle
        // that split is what puts the read in VALUE order -- the block above
        // `i64::MAX` encodes negative and would otherwise be walked first --
        // and it is what lets [`full_table_handle_order`] promise the order at
        // all.
        //
        // A SIGNED handle needs none of this: its whole domain is one key
        // interval already, and building it here instead of through the range
        // encoder would only re-derive the same bytes.
        let full_unsigned_handle = [IndexRange {
            low: vec![Datum::UInt(0)],
            high: vec![Datum::UInt(u64::MAX)],
            low_exclusive: false,
            high_exclusive: false,
        }];
        let handle_ranges = if handle_ranges.is_some() {
            handle_ranges
        } else if crate::handle_range::common_handle_primary(self).is_some() {
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
        handle_ranges: Option<&[IndexRange]>,
        zone: &SessionTimeZone,
        descending: bool,
    ) -> Result<Vec<Vec<(Key, Key)>>, KvTableError> {
        let full_common_handle = [IndexRange {
            low: vec![Datum::MinNotNull],
            high: vec![Datum::MaxValue],
            low_exclusive: false,
            high_exclusive: false,
        }];
        // See [`Self::record_key_ranges]: the full-domain defaults exist so
        // the boundary split can see the handle's own width.
        let full_unsigned_handle = [IndexRange {
            low: vec![Datum::UInt(0)],
            high: vec![Datum::UInt(u64::MAX)],
            low_exclusive: false,
            high_exclusive: false,
        }];
        let handle_ranges = if handle_ranges.is_some() {
            handle_ranges
        } else if crate::handle_range::common_handle_primary(self).is_some() {
            Some(full_common_handle.as_slice())
        } else if self.unsigned_pk_handle() {
            Some(full_unsigned_handle.as_slice())
        } else {
            handle_ranges
        };
        let encoded = match handle_ranges {
            Some(ranges) => crate::handle_range::record_key_range_value_halves(
                self,
                ranges,
                zone,
            )
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
    #[allow(clippy::too_many_arguments)]
    pub fn pushdown_row_cursor_with_context(
        &mut self,
        keep: &[usize],
        predicates: &[ScanPredicate],
        output_offsets: Option<&[usize]>,
        topn: Option<&PushdownTopN>,
        limit: Option<u64>,
        handle_ranges: Option<&[IndexRange]>,
        range_hints: Option<&[usize]>,
        descending: bool,
        keep_order: bool,
        read_ahead_batches: usize,
        context: &RowDecodeContext,
        statement: &PushdownStatementContext,
    ) -> Result<Option<RemoteRowCursor>, KvTableError> {
        let common_handle = !self.common_handle_offsets.is_empty();
        let common_primary = crate::handle_range::clustered_primary_metadata(self);
        if common_handle && (self.has_dirty_content() || common_primary.is_none()) {
            return Ok(None);
        }
        // A pushdown request names ONE physical table id. A partitioned table
        // has several, so the request cannot describe it and the local scan
        // (which spans the whole partition block) serves the read instead.
        if self.partition.is_some() {
            return Ok(None);
        }
        // An ORDERED read of an unsigned handle whose domain wraps the int64
        // boundary cannot travel as one ascending range list: Go opens TWO
        // results -- `firstPartGroupedRanges then `secondPartGroupedRanges
        // (`table_reader.go) -- and reads them one after the other through
        // `resultHandler.open(firstResult, secondResult). The groups below are
        // exactly those parts in read order (a descending read reverses the
        // pair); every shape without a straddle is one group and takes the
        // single-request path unchanged. An UNORDERED read keeps Go's merged
        // ascending wire order from [`Self::record_key_ranges].
        let groups = if keep_order || descending {
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
                let column = &self.columns[*offset];
                PushdownScanColumn {
                    id: column.id,
                    field_type: column.field_type.clone(),
                    is_handle: self.pk_handle_offset == Some(*offset),
                    origin_default: column.origin_default.clone(),
                }
            })
            .collect();
        // An integer-handle merge needs every remote row's handle. A projected
        // primary key already carries it; otherwise `_tidb_rowid` is appended
        // and dropped before emission. A clean common-handle scan has no
        // client-side merge, so it appends no synthetic handle.
        let handle_index = if common_handle {
            None
        } else {
            Some(
                match self
                    .pk_handle_offset
                    .and_then(|offset| keep.iter().position(|kept| *kept == offset))
                {
                    Some(index) => index,
                    None => {
                        columns.push(match self.pk_handle_offset {
                            Some(offset) => PushdownScanColumn {
                                id: self.columns[offset].id,
                                field_type: self.columns[offset].field_type.clone(),
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
                        columns.len() - 1
                    }
                },
            )
        };
        let primary_column_ids: Vec<i64> = common_primary
            .as_ref()
            .into_iter()
            .flat_map(|index| index.column_offsets.iter())
            .filter_map(|offset| self.columns.get(*offset))
            .map(|column| column.id)
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
                        let column = self.columns.get(*offset)?;
                        let prefix = index.prefix_length(position);
                        (prefix > 0 && column.field_type.flen() > prefix).then_some(column.id)
                    })
            })
            .collect();
        // One request PER group. Both open up front -- Go builds both parts'
        // responses before reading either (`buildRespForGroupedRanges) -- and
        // the rows are consumed strictly part by part.
        // Go's `SetTableHandles` supplies hints only for the single grouped
        // handle request. A straddling unsigned range is split into two
        // requests above, so do not attach hints to those unrelated groups.
        let request_range_hints = if groups.len() == 1 {
            range_hints.filter(|hints| hints.len() == groups[0].len())
        } else {
            None
        };
        let build_request = |ranges: Vec<(Key, Key)>| PushdownScanRequest {
            table_id: self.table_id,
            index: None,
            columns: columns.clone(),
            handle_index,
            primary_column_ids: primary_column_ids.clone(),
            primary_prefix_column_ids: primary_prefix_column_ids.clone(),
            predicates: predicates.to_vec(),
            output_offsets: output_offsets.map(<[usize]>::to_vec),
            topn: topn.cloned(),
            limit,
            aggregate: None,
            desc: descending,
            keep_order,
            allow_unordered_response: false,
            read_ahead_batches,
            // The storage that owns the snapshot fills this in; the table has
            // no timestamp of its own.
            snapshot_ts: 0,
            ranges,
            range_hints: request_range_hints.map_or_else(Vec::new, <[usize]>::to_vec),
            statement: statement.clone(),
        };
        let mut scans: Vec<crate::remote_scan::PushdownScan> =
            Vec::with_capacity(groups.len());
        for ranges in &groups {
            let Some(scan) = self.store.open_remote_scan(&build_request(ranges.clone())) else {
                for mut opened in scans.drain(..) {
                    opened.stream.close();
                }
                return Ok(None);
            };
            let mut scan = scan.map_err(|e| KvTableError::Storage(format!("{e:?}")))?;
            if common_handle && !scan.staged.is_empty() {
                scan.stream.close();
                for mut opened in scans.drain(..) {
                    opened.stream.close();
                }
                return Ok(None);
            }
            // One request reached a region. Counted here rather than at the
            // storage seam so a backend that REFUSED the shape (and returned
            // an `Unsupported` the caller turned into a byte-level cursor) is
            // not recorded as a coprocessor read.
            crate::storage::note_storage_op(|ops| ops.cop_scans += 1);
            scans.push(scan);
        }
        let decoder = self.row_decoder_projected(Some(keep), context)?;
        let mut staged = Vec::new();
        // Each part's staged slice covers only that part's ranges and arrives
        // in part order, so concatenating them preserves the key order the
        // merge below walks -- ascending for an ascending read, descending for
        // a descending one (`open_remote_scan already reverses to match).
        for scan in &mut scans {
            for (key, value) in std::mem::take(&mut scan.staged) {
                let row = match value {
                    Some(value) => Some(decoder.decode_record(key.as_bytes(), &value)?.1),
                    None => None,
                };
                staged.push((key.into_bytes(), row));
            }
        }
        let merge_staged = !staged.is_empty();
        let predicates_applied = scans.iter().all(|scan| scan.stream.predicates_applied());
        let stream: Box<dyn crate::remote_scan::PushdownRowStream> = if scans.len() == 1 {
            scans.pop().expect("exactly one scan").stream
        } else {
            crate::remote_scan::ChainedPushdownStream::new(
                scans.into_iter().map(|scan| scan.stream).collect(),
            )
        };
        Ok(Some(RemoteRowCursor {
            stream,
            staged: staged.into_iter(),
            pending_staged: None,
            pending_remote: None,
            pending_chunk: None,
            pending_chunk_row: 0,
            // Handle lookups keep the complete request schema on the wire
            // (`output_offsets` is None), so retain its types for the
            // columnar drain below. Ordinary projected scans may narrow the
            // wire schema; their existing chunk handoff remains unchanged.
            field_types: columns
                .iter()
                .map(|column| column.field_type.clone())
                .collect(),
            width: output_offsets.map_or(keep.len(), <[usize]>::len),
            handle_index,
            table_id: self.table_id,
            merge_staged,
            noted_rows: 0,
            descending,
            predicates_applied,
        }))
    }

    /// Legacy zone-only coprocessor cursor; see [`KvTable::row_cursor`].
    #[allow(clippy::too_many_arguments)]
    pub fn pushdown_row_cursor(
        &mut self,
        keep: &[usize],
        predicates: &[ScanPredicate],
        limit: Option<u64>,
        handle_ranges: Option<&[IndexRange]>,
        zone: &SessionTimeZone,
        statement: &PushdownStatementContext,
    ) -> Result<Option<RemoteRowCursor>, KvTableError> {
        self.pushdown_row_cursor_with_context(
            keep,
            predicates,
            None,
            None,
            limit,
            handle_ranges,
            None,
            false,
            false,
            crate::remote_scan::DEFAULT_SCAN_READ_AHEAD_BATCHES,
            &RowDecodeContext::legacy_default(zone),
            statement,
        )
    }

    /// Reads a batch of integer handles through one coprocessor table request,
    /// applying the supplied residual predicates before rows cross back. The
    /// caller still keeps its local probe for the staged/fallback path.
    pub fn pushdown_rows_by_handles_filtered(
        &mut self,
        handles: &[TableHandle],
        scan_keep: &[usize],
        predicates: &[ScanPredicate],
        zone: &SessionTimeZone,
        statement: &PushdownStatementContext,
    ) -> Result<Option<(Vec<(TableHandle, Vec<Datum>)>, bool)>, KvTableError> {
        let Some(staged) = self.stage_rows_by_handles_filtered(
            handles, scan_keep, predicates, zone, statement,
        )?
        else {
            return Ok(None);
        };
        Self::finish_rows_by_handles(handles, staged)
            .map(|answer| answer.map(|(rows, applied, _wire)| (rows, applied)))
    }

    /// Everything [`Self::pushdown_rows_by_handles_filtered`] does BEFORE any
    /// row crosses back: the refusal gates, the record-range grouping, the
    /// region request OPEN. Split from the drain so a caller may run the
    /// (network-bound) drain on another thread while this thread keeps
    /// issuing the next requests -- go's table-worker pool. Staging runs on
    /// the CALLER's thread on purpose: the storage-operation probe it feeds
    /// is thread-local, so the read is counted exactly where a serial walk
    /// would count it.
    #[allow(clippy::too_many_arguments)]
    pub fn stage_rows_by_handles_filtered(
        &mut self,
        handles: &[TableHandle],
        scan_keep: &[usize],
        predicates: &[ScanPredicate],
        zone: &SessionTimeZone,
        statement: &PushdownStatementContext,
    ) -> Result<Option<StagedHandlesLookup>, KvTableError> {
        if handles.is_empty() {
            return Ok(None);
        }
        if self.has_dirty_content()
            || self.partition.is_some()
            || handles
                .iter()
                .any(|handle| !matches!(handle, TableHandle::Int(_)))
        {
            return Ok(None);
        }
        let mut keep = scan_keep.to_vec();
        let (handle_position, appended_handle) = match self.pk_handle_offset {
            Some(handle_offset) => {
                let appended = !keep.contains(&handle_offset);
                let position = keep
                    .iter()
                    .position(|offset| *offset == handle_offset)
                    .unwrap_or_else(|| {
                        keep.push(handle_offset);
                        keep.len() - 1
                    });
                (position, appended)
            }
            // Tables without an integer primary key use Go's hidden
            // `_tidb_rowid`. `pushdown_row_cursor_with_context` appends that
            // synthetic handle column itself; keep it out of `scan_keep` so
            // predicate offsets still describe the visible row.
            None => (keep.len(), true),
        };
        let layout = keep
            .iter()
            .map(|offset| {
                scan_keep
                    .iter()
                    .position(|kept| kept == offset)
                    .unwrap_or(usize::MAX)
            })
            .collect::<Vec<_>>();
        let Some(predicates) = predicates
            .iter()
            .map(|predicate| predicate.remapped_columns(&layout))
            .collect::<Option<Vec<_>>>()
        else {
            return Ok(None);
        };
        // TiKV requires record ranges in ascending, disjoint order. The
        // index worker may deliberately hand us a descending index-order
        // batch, so sort only the request copy and restore index order below.
        let mut request_handles = handles.to_vec();
        request_handles.sort();
        request_handles.dedup();
        // Go's `buildKeyRanges` over a sorted handle batch: consecutive
        // handles collapse into one closed interval, so a bulk-loaded window
        // arrives as a handful of wide ranges instead of one seek per row --
        // TiKV scans the record span instead of seeking every key apart. A
        // fragmented batch keeps its point intervals and pays only what it
        // must; the gap rowids a merged interval spans match no stored row
        // and answer nothing.
        let mut ranges: Vec<IndexRange> = Vec::with_capacity(request_handles.len());
        let mut range_hints: Vec<usize> = Vec::with_capacity(request_handles.len());
        for handle in &request_handles {
            let TableHandle::Int(value) = handle else {
                unreachable!("handle kind checked above")
            };
            match (ranges.last_mut(), value.checked_sub(1)) {
                // Consecutive handle: widen the open interval's high end.
                (Some(range), Some(prev)) if range.high.first() == Some(&Datum::Int(prev)) => {
                    range.high = vec![Datum::Int(*value)];
                    *range_hints.last_mut().expect("every range has a hint") += 1;
                }
                _ => {
                    ranges.push(IndexRange {
                        low: vec![Datum::Int(*value)],
                        high: vec![Datum::Int(*value)],
                        low_exclusive: false,
                        high_exclusive: false,
                    });
                    range_hints.push(1);
                }
            }
        }
        let context = RowDecodeContext::legacy_default(zone);
        let Some(cursor) = self.pushdown_row_cursor_with_context(
            &keep,
            &predicates,
            None,
            None,
            None,
            Some(&ranges),
            Some(&range_hints),
            false,
            false,
            crate::remote_scan::DEFAULT_SCAN_READ_AHEAD_BATCHES,
            &context,
            statement,
        )?
        else {
            return Ok(None);
        };
        Ok(Some(StagedHandlesLookup {
            cursor,
            handle_position,
            appended_handle,
        }))
    }

    /// Drains a staged handle lookup and pairs each returned row back to the
    /// CALLER's handle order -- index order for a keep-order read, whatever
    /// order the windows were collected in otherwise -- so the answer never
    /// depends on how the drain was scheduled. The trailing count is what
    /// crossed the wire, for the caller's storage probe (a worker thread has
    /// no probe of its own).
    #[must_use]
    pub fn finish_rows_by_handles(
        handles: &[TableHandle],
        mut staged: StagedHandlesLookup,
    ) -> Result<Option<(Vec<(TableHandle, Vec<Datum>)>, bool, u64)>, KvTableError> {
        let wire_rows = staged.cursor.rows_returned();
        let predicates_applied = staged.cursor.predicates_applied();
        let mut rows = Vec::with_capacity(handles.len());
        // This helper asks the remote cursor to retain the synthetic
        // `_tidb_rowid` appended by `pushdown_row_cursor_with_context`.
        // Ordinary consumers intentionally truncate that transport-only
        // column before returning a projected row, but the lookup caller
        // needs it to associate each fetched row with its index handle.
        let mut append_row = |mut row: Vec<Datum>| -> Result<(), KvTableError> {
            let Some(Datum::Int(handle)) = row.get(staged.handle_position) else {
                return Err(KvTableError::Decode(
                    "a coprocessor row carried no integer handle".to_owned(),
                ));
            };
            let handle = TableHandle::Int(*handle);
            if staged.appended_handle {
                // pushdown_row_cursor_with_context appends a synthetic
                // handle after every requested column. Go's table worker
                // keeps that handle in a side field rather than shifting the
                // row; pop the trailing slot to avoid an O(width) move for
                // every fetched row.
                debug_assert_eq!(staged.handle_position, row.len().saturating_sub(1));
                row.pop();
            }
            rows.push((handle, row));
            Ok(())
        };
        // A real coprocessor stream transfers decoded chunks. Drain each
        // chunk once so the stream's row counter and channel are touched once
        // per batch; fakes and row-only backends keep the original contract.
        if let Some(batch) = staged.cursor.next_chunk_with_handle()? {
            for row in batch {
                append_row(row)?;
            }
            loop {
                let Some(batch) = staged.cursor.next_chunk_with_handle()? else {
                    break;
                };
                if batch.is_empty() {
                    break;
                }
                for row in batch {
                    append_row(row)?;
                }
            }
        } else {
            while let Some(row) = staged.cursor.next_row_with_handle()? {
                append_row(row)?;
            }
        }
        let wire_rows = staged.cursor.rows_returned().saturating_sub(wire_rows);
        // The coprocessor returns rows in record-key order, while the lookup
        // executor must restore the index window's handle order. Unordered
        // lookup windows are sorted before the request is opened, so their
        // caller order already is record-key order. Avoid building a map and
        // sorting that common path; keep the map for keep-order windows, where
        // the index order is intentionally different from record-key order.
        if !handles.windows(2).all(|window| window[0] <= window[1]) {
            // Keep the source order in a map once instead of scanning
            // `handles` for every returned row (the old position lookup was
            // O(n²) for a large window).
            // Go's `kv.HandleMap` is hash-backed; preserve the same O(1)
            // handle-to-index lookup for keep-order restoration instead of
            // paying the tree comparison cost for every returned row.
            let positions = handles
                .iter()
                .enumerate()
                .map(|(position, handle)| (handle, position))
                .collect::<HashMap<_, _>>();
            // Go's `tableWorker.executeTask` computes `rowIdx` once per row
            // and sorts that integer field. Decorate before sorting for the
            // same O(n) handle-map work; calling `HashMap::get` from a
            // comparison key would repeat the hash lookup O(n log n) times.
            let mut ordered = rows
                .into_iter()
                .map(|row| {
                    let position = positions.get(&row.0).copied().unwrap_or(usize::MAX);
                    (position, row)
                })
                .collect::<Vec<_>>();
            ordered.sort_by_key(|(position, _)| *position);
            rows = ordered.into_iter().map(|(_, row)| row).collect();
        }
        Ok(Some((rows, predicates_applied, wire_rows)))
    }

    /// Finishes one staged table lookup using the same chunk-backed handoff as
    /// Go's `tableWorker.executeTask`, falling back to the row contract for
    /// sources that cannot transfer typed chunks.
    pub(crate) fn finish_lookup_by_handles(
        handles: &[TableHandle],
        staged: StagedHandlesLookup,
    ) -> Result<Option<FinishedLookup>, KvTableError> {
        if !staged.cursor.supports_lookup_chunks() || !staged.cursor.predicates_applied() {
            return Self::finish_rows_by_handles(handles, staged).map(|answer| {
                answer.map(|(rows, applied, wire_rows)| {
                    FinishedLookup::Rows(rows, applied, wire_rows)
                })
            });
        }
        Self::finish_lookup_chunks_by_handles(handles, staged)
            .map(|answer| answer.map(FinishedLookup::Chunk))
    }

    /// Retains decoded table batches and row positions instead of converting
    /// each row to `Vec<Datum>` or copying rows into a merged chunk. Go stores
    /// `chunk.Row` references in the table task and computes the integer
    /// `rowIdx` once per row before sorting; this implementation follows that
    /// contract with `(batch,row)` positions.
    fn finish_lookup_chunks_by_handles(
        handles: &[TableHandle],
        mut staged: StagedHandlesLookup,
    ) -> Result<Option<FinishedLookupChunk>, KvTableError> {
        let wire_rows = staged.cursor.rows_returned();
        let predicates_applied = staged.cursor.predicates_applied();
        let mut batches = Vec::new();
        let mut rows = Vec::with_capacity(handles.len());
        loop {
            let Some(batch) = staged.cursor.next_raw_chunk_with_handle()? else {
                break;
            };
            if batch.num_rows() == 0 {
                break;
            }
            if staged.handle_position >= batch.num_cols() {
                return Err(KvTableError::Decode(
                    "a coprocessor row carried no integer handle".to_owned(),
                ));
            }
            let batch_index = batches.len();
            for row_index in 0..batch.num_rows() {
                let row = batch.get_row(row_index);
                let handle = match row.get_datum(
                    staged.handle_position,
                    &staged.cursor.field_types[staged.handle_position],
                ) {
                    Datum::Int(handle) => handle,
                    Datum::UInt(handle) => handle as i64,
                    _ => {
                        return Err(KvTableError::Decode(
                            "a coprocessor row carried no integer handle".to_owned(),
                        ));
                    }
                };
                rows.push((TableHandle::Int(handle), batch_index, row_index));
            }
            batches.push(batch);
        }
        let wire_rows = staged.cursor.rows_returned().saturating_sub(wire_rows);
        let mut ordered = rows;
        if !handles.windows(2).all(|window| window[0] <= window[1]) {
            let positions = handles
                .iter()
                .enumerate()
                .map(|(position, handle)| (handle, position))
                .collect::<HashMap<_, _>>();
            ordered
                .sort_by_key(|(handle, _, _)| positions.get(handle).copied().unwrap_or(usize::MAX));
        }
        Ok(Some(FinishedLookupChunk {
            batches,
            row_positions: ordered
                .into_iter()
                .map(|(_, batch_index, row_index)| (batch_index, row_index))
                .collect(),
            handle_position: staged.handle_position,
            appended_handle: staged.appended_handle,
            predicates_applied,
            wire_rows,
        }))
    }

    /// Opens a coprocessor partial aggregation over this table, or returns
    /// `None` so the executor computes the same partial result locally.
    ///
    /// An aggregate result cannot be merged with staged rows. A dirty table
    /// therefore refuses before sending a request, and a backend-reported
    /// staged buffer closes the request and falls back as a second guard.
    #[allow(clippy::too_many_arguments)]
    pub fn pushdown_partial_aggregate_cursor(
        &mut self,
        keep: &[usize],
        predicates: &[ScanPredicate],
        handle_ranges: Option<&[IndexRange]>,
        aggregate: &PushdownPartialAggregate,
        zone: &SessionTimeZone,
        statement: &PushdownStatementContext,
    ) -> Result<Option<Box<dyn PushdownRowStream>>, KvTableError> {
        if self.has_dirty_content() || self.partition.is_some() {
            return Ok(None);
        }
        let ranges = self.record_key_ranges(handle_ranges, zone, false)?;
        if ranges.is_empty() {
            return Ok(None);
        }
        let columns = keep
            .iter()
            .map(|offset| {
                let column = &self.columns[*offset];
                PushdownScanColumn {
                    id: column.id,
                    field_type: column.field_type.clone(),
                    is_handle: self.pk_handle_offset == Some(*offset),
                    origin_default: column.origin_default.clone(),
                }
            })
            .collect();
        // Go's PhysicalTableScan.ToPB names the clustered primary's column
        // ids on every table scan over a common-handle table, whether or not
        // the catalog stores that key as an index. This tier stores no KvIndex
        // for a clustered key, so the stored-index lookup answers None for
        // exactly the tables TiKV then cannot decode: the PK columns live in
        // the record KEY, and with no primary_column_ids TiKV leaves their
        // slots unfilled and rejects the NOT NULL row as corrupted. The
        // synthesized metadata (see handle_range::clustered_primary_metadata)
        // is the same reconstruction the row-cursor builder above sends.
        let common_primary = crate::handle_range::clustered_primary_metadata(self);
        let primary_column_ids = common_primary
            .as_ref()
            .into_iter()
            .flat_map(|index| index.column_offsets.iter())
            .filter_map(|offset| self.columns.get(*offset))
            .map(|column| column.id)
            .collect();
        let primary_prefix_column_ids = common_primary
            .as_ref()
            .into_iter()
            .flat_map(|index| {
                index
                    .column_offsets
                    .iter()
                    .enumerate()
                    .filter_map(|(position, offset)| {
                        let column = self.columns.get(*offset)?;
                        let prefix = index.prefix_length(position);
                        (prefix > 0 && column.field_type.flen() > prefix).then_some(column.id)
                    })
            })
            .collect();
        let request = PushdownScanRequest {
            table_id: self.table_id,
            index: None,
            columns,
            handle_index: None,
            primary_column_ids,
            primary_prefix_column_ids,
            predicates: predicates.to_vec(),
            output_offsets: None,
            topn: None,
            limit: None,
            aggregate: Some(aggregate.clone()),
            desc: false,
            keep_order: false,
            // Order-free responses are opted into per call site below.
            allow_unordered_response: false,
            read_ahead_batches: crate::remote_scan::DEFAULT_SCAN_READ_AHEAD_BATCHES,
            snapshot_ts: 0,
            ranges,
            range_hints: Vec::new(),
            statement: statement.clone(),
        };
        let Some(scan) = self.store.open_remote_scan(&request) else {
            return Ok(None);
        };
        let mut scan = scan.map_err(|error| KvTableError::Storage(format!("{error:?}")))?;
        if !scan.staged.is_empty() {
            scan.stream.close();
            return Ok(None);
        }
        crate::storage::note_storage_op(|ops| ops.cop_scans += 1);
        Ok(Some(scan.stream))
    }

    /// Index-scan counterpart of [`Self::pushdown_partial_aggregate_cursor`].
    /// The bounded path is the covering `COUNT(k)` shape used by Sysbench:
    /// the aggregate input must be the index's leading key column.
    pub fn pushdown_index_partial_aggregate_cursor(
        &mut self,
        index_id: i64,
        ranges: &[IndexRange],
        scan_keep: &[usize],
        predicates: &[ScanPredicate],
        aggregate: &PushdownPartialAggregate,
        zone: &SessionTimeZone,
        statement: &PushdownStatementContext,
    ) -> Result<Option<Box<dyn PushdownRowStream>>, KvTableError> {
        // Go's `PhysicalIndexReader.ToPB` names the clustered primary's
        // column ids on the index scan and appends those key columns to the
        // executor schema (`is.InitSchema(append(path.FullIdxCols,
        // ds.CommonHandleCols...))`); TiKV reads the executor as
        // `[index datums..., handle datums...]` by subtracting
        // `primary_column_ids.len()` from the column count. This builder once
        // sent empty ids, which real TiKV answered with
        // `Expect to decode index values with common handles in
        // `DecodeCommonHandle` mode` for every CLUSTERED-PK table whose
        // covering index the planner picks post-ANALYZE (`count(*) FROM
        // bmsql_customer`), refusing into the local partial-aggregate walk.
        // Build Go's schema instead.
        if self.has_dirty_content() || self.partition.is_some() {
            return Ok(None);
        }
        if self.has_dirty_content() || self.partition.is_some() {
            return Ok(None);
        }
        let Some(index) = self.indexes.iter().find(|index| index.id == index_id) else {
            return Ok(None);
        };
        if index.column_offsets.is_empty()
            || !matches!(
                aggregate,
                PushdownPartialAggregate::Count { .. }
                    | PushdownPartialAggregate::Global { .. }
                    | PushdownPartialAggregate::Grouped { .. }
            )
        {
            return Ok(None);
        }
        let mut columns: Vec<PushdownScanColumn> = index
            .column_offsets
            .iter()
            .filter_map(|offset| self.columns.get(*offset))
            .map(|column| PushdownScanColumn {
                id: column.id,
                field_type: column.field_type.clone(),
                is_handle: false,
                origin_default: column.origin_default.clone(),
            })
            .collect();
        if columns.len() != index.column_offsets.len() {
            return Ok(None);
        }
        // The handle columns ride AFTER the indexed columns -- duplicates
        // included, exactly like `pushdown_index_handle_cursor`'s schema --
        // because TiKV subtracts their count from the executor width before
        // decoding the record handle.
        for offset in &self.common_handle_offsets {
            let Some(column) = self.columns.get(*offset) else {
                return Ok(None);
            };
            columns.push(PushdownScanColumn {
                id: column.id,
                field_type: column.field_type.clone(),
                is_handle: false,
                origin_default: column.origin_default.clone(),
            });
        }
        // Go `checkCoverIndex` again: a unique flag over PARTIAL-key ranges
        // asks TiKV for a unique get the range does not name.
        let declared_unique = index.unique
            && ranges.iter().all(|range| {
                range.low.len() == index.column_offsets.len()
                    && !range.low.iter().any(|datum| matches!(datum, Datum::Null))
                    && !range.high.iter().any(|datum| matches!(datum, Datum::Null))
            });
        let encode = |values: &[Datum]| -> Result<Vec<u8>, KvTableError> {
            tidb_codec::encode_key_in_timezone(zone, values)
                .map_err(|error| KvTableError::Encode(format!("{error:?}")))
        };
        let mut key_ranges = Vec::with_capacity(ranges.len());
        for range in ranges {
            let mut low = Key::from_bytes(encode_index_seek_key(
                self.table_id,
                index_id,
                &encode(&range.low)?,
            ));
            if range.low_exclusive {
                low = low.prefix_next();
            }
            let mut high = Key::from_bytes(encode_index_seek_key(
                self.table_id,
                index_id,
                &encode(&range.high)?,
            ));
            if !range.high_exclusive {
                high = high.prefix_next();
            }
            key_ranges.push((low, high));
        }
        if key_ranges.is_empty() {
            return Ok(None);
        }
        let mut remote_aggregate = aggregate.clone();
        // The planner's aggregate offsets name the scan's pruned schema;
        // the covering index request exposes index columns in index order.
        // Keep the same remapping Go's `PhysicalIndexReader.ToPB` applies to
        // aggregate arguments and group keys.
        // Keep one slot per physical index column. A sentinel represents an
        // index-only column absent from the pruned source schema; it preserves
        // the physical position while still letting offset remapping find
        // every aggregate/predicate column that IS present.
        let index_keep = index
            .column_offsets
            .iter()
            .map(|offset| {
                scan_keep
                    .iter()
                    .position(|kept| kept == offset)
                    .unwrap_or(usize::MAX)
            })
            .collect::<Vec<_>>();
        let mut remote_predicates = predicates.to_vec();
        for predicate in &mut remote_predicates {
            if crate::predicate_pushdown::remap_scan_predicate(predicate, &index_keep).is_none() {
                return Ok(None);
            }
        }
        let remap = |offset: &mut usize| {
            *offset = index_keep.iter().position(|kept| *kept == *offset)?;
            Some(())
        };
        match &mut remote_aggregate {
            PushdownPartialAggregate::Count { input_offset, .. } => {
                if input_offset.is_some() {
                    let offset = input_offset.as_mut().expect("checked above");
                    if remap(offset).is_none() {
                        return Ok(None);
                    }
                }
            }
            PushdownPartialAggregate::Global { functions } => {
                for function in functions {
                    if let Some(input) = function.input.as_mut() {
                        // An argument naming a column the index does not
                        // carry (sysbench's checksum sums the handle under
                        // `USE INDEX`) is not THIS cursor's shape -- Go
                        // answers it through a lookup that reads the row --
                        // so the scan falls back to the local path instead
                        // of failing a statement Go accepts.
                        if crate::predicate_pushdown::remap_expression(input, &index_keep).is_none()
                        {
                            return Ok(None);
                        }
                    }
                }
            }
            PushdownPartialAggregate::Grouped {
                group_offsets,
                functions,
                ..
            } => {
                for offset in group_offsets {
                    remap(offset).ok_or_else(|| {
                        KvTableError::Encode(
                            "a grouped aggregate key is not covered by the index".to_owned(),
                        )
                    })?;
                }
                for function in functions {
                    if let Some(input) = function.input.as_mut() {
                        if crate::predicate_pushdown::remap_expression(input, &index_keep)
                            .is_none()
                        {
                            return Ok(None);
                        }
                    }
                }
            }
            _ => unreachable!("index partial aggregate shape checked above"),
        }
        let request = PushdownScanRequest {
            table_id: self.table_id,
            index: Some(PushdownIndexScan {
                index_id,
                declared_unique,
                index_column_count: index.column_offsets.len(),
                desc: false,
            }),
            columns,
            handle_index: None,
            primary_column_ids: self
                .common_handle_offsets
                .iter()
                .filter_map(|offset| self.columns.get(*offset).map(|column| column.id))
                .collect(),
            primary_prefix_column_ids: Vec::new(),
            predicates: remote_predicates,
            output_offsets: None,
            topn: None,
            limit: None,
            aggregate: Some(remote_aggregate),
            desc: false,
            keep_order: false,
            // Order-free responses are opted into per call site below.
            allow_unordered_response: false,
            read_ahead_batches: crate::remote_scan::DEFAULT_SCAN_READ_AHEAD_BATCHES,
            snapshot_ts: 0,
            ranges: key_ranges,
            range_hints: Vec::new(),
            statement: statement.clone(),
        };
        let Some(scan) = self.store.open_remote_scan(&request) else {
            return Ok(None);
        };
        let mut scan = scan.map_err(|error| KvTableError::Storage(format!("{error:?}")))?;
        if !scan.staged.is_empty() {
            scan.stream.close();
            return Ok(None);
        }
        crate::storage::note_storage_op(|ops| ops.cop_scans += 1);
        Ok(Some(scan.stream))
    }

    /// Opens a coprocessor index scan whose rows carry only the indexed
    /// columns and the table handle. The access source consumes those rows as
    /// an ordered handle stream before issuing its table lookup batch.
    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::too_many_arguments)]
    pub fn pushdown_index_handle_cursor(
        &mut self,
        index_id: i64,
        ranges: &[IndexRange],
        scan_keep: &[usize],
        predicates: &[ScanPredicate],
        topn: Option<&PushdownTopN>,
        zone: &SessionTimeZone,
        statement: &PushdownStatementContext,
        desc: bool,
        index_limit: Option<u64>,
        unordered: bool,
        handle_only: bool,
        projected_keep: Option<&[usize]>,
    ) -> Result<Option<RemoteIndexHandleCursor>, KvTableError> {
        // The wire contract this request once broke -- reordered and
        // truncated columns on real TiKV -- came from the executor schema
        // DEDUPING clustered-handle columns the index already carried:
        // TiKV reads [index datums..., handle datums...] by subtracting
        // primary_column_ids.len() from the column count, so the shrunken
        // layout decoded bytes where ints were expected. The schema below
        // appends every primary-key column (duplicates included) per Go's
        // `is.InitSchema(append(path.FullIdxCols, ds.CommonHandleCols...))`,
        // and a covering prefix read of bmsql_oorder_idx1 plus the ecasdb
        // max(dtlno) stream answer correctly against the real backend again.
        if self.has_dirty_content() || self.partition.is_some() || ranges.is_empty() {
            return Ok(None);
        }
        let Some(index) = self.indexes.iter().find(|index| index.id == index_id) else {
            return Ok(None);
        };
        // The compact layout is the source's current schema after column
        // pruning. An absent value is represented by usize::MAX and causes a
        // predicate/order key that needs it to fail closed below.
        //
        // A common-handle table appends EVERY primary-key column after the
        // indexed columns, duplicates included: Go builds the same executor
        // schema with `is.InitSchema(append(path.FullIdxCols,
        // ds.CommonHandleCols...), ...)` (`exhaust_physical_plans.go`), and
        // TiKV reads the executor as [index datums..., handle datums...] by
        // subtracting `primary_column_ids.len()` from the column count
        // (`initIdxScanCtx`). Deduplicating a key column that the index
        // already carries shrinks that layout -- an executor naming
        // [a, b, d, c] over PRIMARY(a, b, c) makes TiKV cut one index datum
        // and decode `b, d` as the handle, so an int read from a bytes datum
        // fails the whole region with "Unsupported datum flag 1 for Int
        // vector".
        let mut table_offsets = index
            .column_offsets
            .iter()
            .copied()
            .map(Some)
            .collect::<Vec<_>>();
        for offset in &self.common_handle_offsets {
            table_offsets.push(Some(*offset));
        }
        if let Some(offset) = self.pk_handle_offset {
            if !table_offsets.contains(&Some(offset)) {
                table_offsets.push(Some(offset));
            }
        }
        if self.pk_handle_offset.is_none() && self.common_handle_offsets.is_empty() {
            table_offsets.push(None);
        }
        let layout = table_offsets
            .iter()
            .map(|offset| {
                offset
                    .and_then(|offset| scan_keep.iter().position(|kept| *kept == offset))
                    .unwrap_or(usize::MAX)
            })
            .collect::<Vec<_>>();
        let Some(predicates) = predicates
            .iter()
            .map(|predicate| predicate.remapped_columns(&layout))
            .collect::<Option<Vec<_>>>()
        else {
            return Ok(None);
        };
        let topn = if let Some(topn) = topn {
            let order_by = topn
                .order_by
                .iter()
                .map(|item| {
                    let column_offset = layout.iter().position(|offset| *offset == item.offset)?;
                    Some(crate::remote_scan::PushdownTopNOrder {
                        offset: column_offset,
                        desc: item.desc,
                    })
                })
                .collect::<Option<Vec<_>>>();
            let Some(order_by) = order_by else {
                return Ok(None);
            };
            Some(PushdownTopN {
                order_by,
                limit: topn.limit,
            })
        } else {
            None
        };
        let columns = table_offsets
            .iter()
            .enumerate()
            .map(|(_position, offset)| {
                if let Some(offset) = offset {
                    let column = self.columns.get(*offset)?;
                    Some(PushdownScanColumn {
                        id: column.id,
                        field_type: column.field_type.clone(),
                        is_handle: self.pk_handle_offset == Some(*offset),
                        origin_default: column.origin_default.clone(),
                    })
                } else {
                    Some(PushdownScanColumn {
                        id: EXTRA_HANDLE_COLUMN_ID,
                        field_type: FieldType::new(tidb_datatype::FieldTypeCode::LongLong)
                            .with_flags(
                                tidb_datatype::FieldTypeFlags::NOT_NULL
                                    | tidb_datatype::FieldTypeFlags::PRI_KEY,
                            ),
                        is_handle: true,
                        origin_default: None,
                    })
                }
            })
            .collect::<Option<Vec<_>>>();
        let Some(columns) = columns else {
            return Ok(None);
        };
        let handle_indices = if self.common_handle_offsets.is_empty() {
            vec![table_offsets
                .iter()
                .position(|offset| *offset == self.pk_handle_offset || offset.is_none())
                .unwrap_or(0)]
        } else {
            self.common_handle_offsets
                .iter()
                .filter_map(|offset| {
                    // The trailing duplicate is the HANDLE copy of the key
                    // column; an indexed copy can differ from it under a
                    // prefix length or new-collation sort-key encoding.
                    table_offsets
                        .iter()
                        .rposition(|candidate| *candidate == Some(*offset))
                })
                .collect()
        };
        if handle_indices.is_empty() {
            return Ok(None);
        }
        // An UNORDERED double read (go's rolling fetchHandles over a
        // keepOrder:false request) neither needs nor gets stream ordering:
        // its windows are re-sorted per batch and nothing consumes cross-
        // window order. Letting the regions answer out of order is what
        // unlocks go's `2 x DistSQLConcurrency` in-flight window.
        let keep_order = if unordered { false } else { !desc || topn.is_none() };
        let encoder = Encoder::new(self.use_new_collation);
        let encode = |values: &[Datum]| -> Result<Vec<u8>, KvTableError> {
            encoder
                .encode_key_in_timezone(zone, values)
                .map_err(|error| KvTableError::Encode(format!("{error:?}")))
        };
        let mut key_ranges = Vec::with_capacity(ranges.len());
        for range in ranges {
            let mut low = Key::from_bytes(encode_index_seek_key(
                self.table_id,
                index_id,
                &encode(&range.low)?,
            ));
            if range.low_exclusive {
                low = low.prefix_next();
            }
            let mut high = Key::from_bytes(encode_index_seek_key(
                self.table_id,
                index_id,
                &encode(&range.high)?,
            ));
            if !range.high_exclusive {
                high = high.prefix_next();
            }
            key_ranges.push((low, high));
        }
        // Go `checkCoverIndex` (`physical_index_scan.go`): the coprocessor's
        // Unique flag travels only when EVERY range names the FULL index key
        // -- a prefix range of a unique index is not a unique get, and
        // telling TiKV it is makes the region treat the partial-key span as
        // a unique lookup that binds no handle and answer nothing.
        let declared_unique = index.unique
            && ranges.iter().all(|range| {
                range.low.len() == index.column_offsets.len()
                    && !range.low.iter().any(|datum| matches!(datum, Datum::Null))
                    && !range.high.iter().any(|datum| matches!(datum, Datum::Null))
            });
        // `output_offsets` addresses the full executor schema, while the
        // cursor consumes the projected response schema. Keep both mappings
        // explicit: a plain double read projects the original handle slots
        // and then sees them densely at positions 0..handle_count.
        let output_offsets = handle_only.then(|| handle_indices.clone());
        let returned_handle_indices = if handle_only {
            (0..handle_indices.len()).collect::<Vec<_>>()
        } else {
            handle_indices.clone()
        };
        let handle_is_unsigned = if handle_only && self.common_handle_offsets.is_empty() {
            handle_indices
                .first()
                .and_then(|index| columns.get(*index))
                .map(|column| column.field_type.is_unsigned())
        } else {
            None
        };
        // Go's covering `PhysicalIndexReader` returns the requested table
        // columns directly from the index executor; it never constructs a
        // table-handle lookup task. Keep the complete wire schema for the
        // normal handle consumer, but remember the dense positions needed to
        // project a covering row back into the pruned table schema. A primary
        // key column uses the trailing handle copy, not an indexed sort-key
        // copy (prefix/collation encodings may differ).
        let projected_indices = projected_keep
            .map(|keep| {
                keep.iter()
                    .map(|offset| {
                        if self.pk_handle_offset == Some(*offset) {
                            table_offsets
                                .iter()
                                .rposition(|candidate| *candidate == Some(*offset))
                        } else {
                            table_offsets
                                .iter()
                                .position(|candidate| *candidate == Some(*offset))
                        }
                        .ok_or_else(|| {
                            KvTableError::Decode(
                                "covering index omitted a requested table column".to_owned(),
                            )
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()?;
        let request = PushdownScanRequest {
            table_id: self.table_id,
            index: Some(PushdownIndexScan {
                index_id,
                declared_unique,
                index_column_count: index.column_offsets.len(),
                desc,
            }),
            columns,
            handle_index: if self.common_handle_offsets.is_empty() {
                returned_handle_indices.first().copied()
            } else {
                None
            },
            primary_column_ids: self
                .common_handle_offsets
                .iter()
                .filter_map(|offset| self.columns.get(*offset).map(|column| column.id))
                .collect(),
            primary_prefix_column_ids: Vec::new(),
            predicates,
            // A plain double read does not consume indexed values. Select
            // only the trailing handle columns after TiKV evaluates the
            // index scan; predicates and TopN retain the complete schema.
            output_offsets,
            topn,
            limit: index_limit,
            aggregate: None,
            desc,
            keep_order,
            allow_unordered_response: unordered,
            read_ahead_batches: crate::remote_scan::DEFAULT_SCAN_READ_AHEAD_BATCHES,
            snapshot_ts: 0,
            ranges: key_ranges,
            range_hints: Vec::new(),
            statement: statement.clone(),
        };
        let Some(scan) = self.store.open_remote_scan(&request) else {
            return Ok(None);
        };
        let mut scan = scan.map_err(|error| KvTableError::Storage(format!("{error:?}")))?;
        if !scan.staged.is_empty() {
            scan.stream.close();
            return Ok(None);
        }
        crate::storage::note_storage_op(|ops| ops.cop_scans += 1);
        Ok(Some(RemoteIndexHandleCursor {
            inner: scan.stream,
            handle_indices: returned_handle_indices,
            projected_indices,
            common_handle: !self.common_handle_offsets.is_empty(),
            zone: zone.clone(),
            use_new_collation: self.use_new_collation,
            noted_rows: 0,
            handle_is_unsigned,
            pending_chunk: None,
            pending_chunk_row: 0,
        }))
    }

    /// Scans the table's record-key range in key order, decoding each value.
    /// Returns rows as `Datum`s in schema order (a missing column decodes
    /// NULL, and the handle columns come from the key).
    pub fn scan_rows_with_context(
        &mut self,
        context: &RowDecodeContext,
    ) -> Result<Vec<Vec<Datum>>, KvTableError> {
        Ok(self
            .scan_rows_with_handles_with_context(context)?
            .into_iter()
            .map(|(_, row)| row)
            .collect())
    }

    /// Legacy zone-only materializing scan; see [`KvTable::row_cursor`].
    pub fn scan_rows(&mut self, zone: &SessionTimeZone) -> Result<Vec<Vec<Datum>>, KvTableError> {
        self.scan_rows_with_context(&RowDecodeContext::legacy_default(zone))
    }

    /// Like [`KvTable::scan_rows`], but each row carries the record handle its
    /// key encodes, which `UPDATE`/`DELETE` need to address the row again.
    ///
    /// This drains a [`RowCursor`]; a caller that does not need the whole
    /// relation in memory should hold the cursor instead (see
    /// [`KvTable::row_cursor`]).
    pub fn scan_rows_with_handles_with_context(
        &mut self,
        context: &RowDecodeContext,
    ) -> Result<Vec<(TableHandle, Vec<Datum>)>, KvTableError> {
        self.scan_rows_with_handles_in_with_context(None, context)
    }

    /// Like [`KvTable::scan_rows_with_handles_with_context`], retaining the
    /// physical table id encoded in every record key.
    ///
    /// DDL rewrites need this identity: a partitioned table can contain the
    /// same local handle in more than one physical partition, and rewriting
    /// either row under the logical table id would collapse the partitions.
    pub(crate) fn scan_physical_rows_with_handles_with_context(
        &mut self,
        context: &RowDecodeContext,
    ) -> Result<Vec<(i64, TableHandle, Vec<Datum>)>, KvTableError> {
        let decoder = self.row_decoder_projected(None, context)?;
        let mut cursor =
            self.row_cursor_with_decoder(decoder, None, false, false, context.zone())?;
        let mut rows = Vec::new();
        while let Some(entry) = cursor.next_physical_row()? {
            rows.push(entry);
        }
        Ok(rows)
    }

    /// Legacy zone-only handle scan; see [`KvTable::row_cursor`].
    pub fn scan_rows_with_handles(
        &mut self,
        zone: &SessionTimeZone,
    ) -> Result<Vec<(TableHandle, Vec<Datum>)>, KvTableError> {
        self.scan_rows_with_handles_with_context(&RowDecodeContext::legacy_default(zone))
    }

    /// [`KvTable::scan_rows_with_handles`] narrowed to `handle_ranges`: the
    /// same intervals the read side offers a `TableRangeScan` through
    /// [`crate::table_access::TableAccess::accept_handle_ranges`]. `None`
    /// reads the whole table.
    ///
    /// This is the WRITE path's form of that narrowing, and it narrows only
    /// WHICH RECORDS ARE FETCHED: the ranges are a superset of the rows the
    /// `WHERE` admits, and the caller still evaluates that `WHERE` per row, so
    /// the set of rows a statement acts on is the same set the full scan
    /// produced.
    pub fn scan_rows_with_handles_in_with_context(
        &mut self,
        handle_ranges: Option<&[IndexRange]>,
        context: &RowDecodeContext,
    ) -> Result<Vec<(TableHandle, Vec<Datum>)>, KvTableError> {
        let mut cursor = self.row_cursor_projected_with_context(None, handle_ranges, context)?;
        let mut rows = Vec::new();
        while let Some(entry) = cursor.next_row()? {
            rows.push(entry);
        }
        Ok(rows)
    }

    pub(crate) fn scan_rows_with_handles_recomputed(
        &mut self,
        context: &RowDecodeContext,
    ) -> Result<Vec<(TableHandle, Vec<Datum>)>, KvTableError> {
        let decoder = self.row_decoder_recomputed(context)?;
        let mut cursor =
            self.row_cursor_with_decoder(decoder, None, false, false, context.zone())?;
        let mut rows = Vec::new();
        while let Some(entry) = cursor.next_row()? {
            rows.push(entry);
        }
        Ok(rows)
    }

    /// Legacy zone-only ranged handle scan; see [`KvTable::row_cursor`].
    pub fn scan_rows_with_handles_in(
        &mut self,
        handle_ranges: Option<&[IndexRange]>,
        zone: &SessionTimeZone,
    ) -> Result<Vec<(TableHandle, Vec<Datum>)>, KvTableError> {
        self.scan_rows_with_handles_in_with_context(
            handle_ranges,
            &RowDecodeContext::legacy_default(zone),
        )
    }

    /// [`KvTable::scan_rows_with_handles`] narrowed to the columns at `keep`
    /// (offsets into [`KvTable::columns`], ascending and unique): the row
    /// codec is asked for **only** those columns' ids, and each returned row
    /// holds exactly them, in `keep`'s order.
    ///
    /// This drains a projected [`RowCursor`]; the projection lives in the
    /// cursor so the streaming path prunes too.
    pub fn scan_rows_with_handles_projected_with_context(
        &mut self,
        keep: &[usize],
        context: &RowDecodeContext,
    ) -> Result<Vec<(TableHandle, Vec<Datum>)>, KvTableError> {
        let mut cursor = self.row_cursor_projected_with_context(Some(keep), None, context)?;
        let mut rows = Vec::new();
        while let Some(entry) = cursor.next_row()? {
            rows.push(entry);
        }
        Ok(rows)
    }

    /// Legacy zone-only projected handle scan; see [`KvTable::row_cursor`].
    pub fn scan_rows_with_handles_projected(
        &mut self,
        keep: &[usize],
        zone: &SessionTimeZone,
    ) -> Result<Vec<(TableHandle, Vec<Datum>)>, KvTableError> {
        self.scan_rows_with_handles_projected_with_context(
            keep,
            &RowDecodeContext::legacy_default(zone),
        )
    }

    /// The handles an index range covers, in index order.
    ///
    /// Go turns a range into a key interval in `IndexRangesToKVRanges`: the
    /// low key is the encoded low bound, advanced to its `PrefixNext` when the
    /// bound is excluded; the high key is the encoded high bound, advanced to
    /// its `PrefixNext` when the bound is INCLUDED, because the scan's upper
    /// end is exclusive. This walks that interval and reads the handle out of
    /// each entry.
    pub fn scan_index_range(
        &mut self,
        index_id: i64,
        range: &IndexRange,
        zone: &SessionTimeZone,
    ) -> Result<Vec<TableHandle>, KvTableError> {
        let mut cursor = self.index_range_cursor(index_id, range, zone)?;
        let mut handles = Vec::new();
        while let Some(handle) = cursor.next_handle()? {
            handles.push(handle);
        }
        Ok(handles)
    }

    /// A forward cursor over one index range, in index order -- the streaming
    /// form of [`KvTable::scan_index_range`].
    pub fn index_range_cursor(
        &mut self,
        index_id: i64,
        range: &IndexRange,
        zone: &SessionTimeZone,
    ) -> Result<IndexRangeCursor, KvTableError> {
        // `ordered`: this entry point serves reads whose answer is the index
        // walk itself, so it keeps the cross-partition merge.
        self.index_range_cursor_with_direction(index_id, range, zone, false, true)
    }

    /// A cursor over one index range in the physical scan direction Go put
    /// on `PhysicalIndexScan`.
    pub fn index_range_cursor_with_direction(
        &mut self,
        index_id: i64,
        range: &IndexRange,
        zone: &SessionTimeZone,
        descending: bool,
        ordered: bool,
    ) -> Result<IndexRangeCursor, KvTableError> {
        self.index_ranges_cursor_for_physical_ids(
            index_id,
            std::slice::from_ref(range),
            zone,
            &self.record_physical_ids(),
            descending,
            ordered,
        )
    }

    /// A forward cursor over all `ranges`, partition by partition. Go builds
    /// one index request per physical partition and puts every range for that
    /// partition in the request before advancing to the next partition.
    pub fn index_ranges_cursor(
        &mut self,
        index_id: i64,
        ranges: &[IndexRange],
        zone: &SessionTimeZone,
    ) -> Result<IndexRangeCursor, KvTableError> {
        let physical_ids = self.record_physical_ids();
        self.index_ranges_cursor_for_physical_ids(
            index_id,
            ranges,
            zone,
            &physical_ids,
            false,
            true,
        )
    }

    /// The UNORDERED cursor an index LOOKUP walks: every range, partition by
    /// partition, no cross-partition merge.
    ///
    /// Go's `IndexLookUpExecutor.buildTableKeyRanges` builds ONE index
    /// request per pruned partition holding EVERY range for that partition
    /// (`buildKeyRanges` is called once with all ranges and all `tableIDs`),
    /// and `indexWorker.fetchHandles` drains request `i` to exhaustion
    /// before touching `i + 1` -- so the handle stream of an unordered
    /// partitioned lookup is PARTITION-major. Opening one cursor per range
    /// instead (each concatenating every partition) would make the stream
    /// RANGE-major, and a multi-range lookup such as `b IN (...)` over a
    /// partitioned table would interleave partitions where Go finishes each
    /// partition's tasks first.
    ///
    /// Always ascending: an unordered lookup has no `Desc` -- Go sets
    /// `PhysicalIndexScan.Desc` only for a keep-order plan, and a keep-order
    /// lookup answers through the cross-partition merge
    /// ([`Self::index_range_cursor_with_direction`] with `ordered`), never
    /// through this cursor.
    pub fn index_lookup_ranges_cursor(
        &mut self,
        index_id: i64,
        ranges: &[IndexRange],
        zone: &SessionTimeZone,
    ) -> Result<IndexRangeCursor, KvTableError> {
        let physical_ids = self.record_physical_ids();
        self.index_ranges_cursor_for_physical_ids(
            index_id,
            ranges,
            zone,
            &physical_ids,
            false,
            false,
        )
    }

    fn index_ranges_cursor_for_physical_ids(
        &mut self,
        index_id: i64,
        ranges: &[IndexRange],
        zone: &SessionTimeZone,
        physical_ids: &[i64],
        descending: bool,
        ordered: bool,
    ) -> Result<IndexRangeCursor, KvTableError> {
        let Some(index) = self
            .indexes
            .iter()
            .find(|index| index.id == index_id)
            .cloned()
        else {
            return Err(KvTableError::Decode("no such index".to_owned()));
        };
        let encoder = Encoder::new(self.use_new_collation);
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
        for (ordinal, physical_id) in physical_ids.iter().copied().enumerate() {
            for range in ranges {
                partition_of_iterator.push(ordinal);
                let mut low = Key::from_bytes(encode_index_seek_key(
                    physical_id,
                    index_id,
                    &encode(&range.low)?,
                ));
                if range.low_exclusive {
                    low = low.prefix_next();
                }
                let mut high = Key::from_bytes(encode_index_seek_key(
                    physical_id,
                    index_id,
                    &encode(&range.high)?,
                ));
                if !range.high_exclusive {
                    high = high.prefix_next();
                }
                let iterator = if descending {
                    self.store.iter_reverse(Some(&high), Some(&low))
                } else {
                    self.store.iter(Some(&low), Some(&high))
                }
                .map_err(|e| KvTableError::Storage(format!("{e:?}")))?;
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
        let merge_by_index_key = ordered && physical_ids.len() > 1;
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
        Ok(IndexRangeCursor {
            iterators,
            partition_of_iterator,
            next_iterator: 0,
            merge_by_index_key,
            merge_heap,
            index,
            common_handle: !self.common_handle_offsets.is_empty(),
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
/// See [`KvTable::row_cursor`]. Over
/// [`ClusterTableStorage`](crate::cluster_storage::ClusterTableStorage) the
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
    let mut merge_key = cut_row_key_prefix(key).to_vec();
    if unsigned_handle {
        if let Some(high_byte) = merge_key.first_mut() {
            *high_byte ^= 0x80;
        }
    }
    merge_key
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
            iterator
                .next()
                .map_err(|e| KvTableError::Storage(format!("{e:?}")))?;
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
            iterator
                .next()
                .map_err(|e| KvTableError::Storage(format!("{e:?}")))?;
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

/// A forward cursor over a table's record range served by the backend's
/// coprocessor.
///
/// Integer-handle scans merge the session's staged writes back in. A
/// common-handle scan reaches this cursor only while the table is clean, so
/// it consumes the remote stream directly.
///
/// # Why the merge is here and not at the backend
///
/// A coprocessor answers from the snapshot. Inside an explicit transaction
/// the session's own uncommitted writes are client-side, so this cursor is
/// Go's `UnionScan` over a distsql reader: the staged rows win over the
/// snapshot rows they shadow, a staged delete hides the snapshot row, and the
/// merged stream stays in record-key order -- which is the order the remote
/// stream and the staged buffer already arrive in, so the merge is one linear
/// pass with one row of each side alive at a time.
///
/// The caller applies its pushed predicate to *every* row this yields, staged
/// or remote, so a staged row that no longer satisfies the `WHERE` is dropped
/// by the same test the snapshot rows passed at TiKV.
/// One OPEN remote handle lookup: [`KvTable::stage_rows_by_handles_filtered`]
/// built the request and the region is already streaming; draining it is
/// [`KvTable::finish_rows_by_handles`]. `Send` so a bounded-concurrency
/// lookup pipeline can drain it off the executor thread.
pub struct StagedHandlesLookup {
    cursor: RemoteRowCursor,
    handle_position: usize,
    appended_handle: bool,
}

/// A table-lookup response kept in its decoded columnar form.
///
/// Go's `tableWorker.executeTask` retains `chunk.Row` values until the parent
/// executor consumes them. Keep the source chunks and row positions instead
/// of copying every row into a second merged chunk; the parent performs the
/// one required append into its output chunk, just as Go does.
pub(crate) struct FinishedLookupChunk {
    /// Decoded response batches retained until the parent consumes them.
    pub(crate) batches: Vec<Chunk>,
    /// `(batch index, row index)` entries in the caller's index-handle order.
    pub(crate) row_positions: Vec<(usize, usize)>,
    /// The source column carrying the integer handle.
    pub(crate) handle_position: usize,
    /// Whether the handle column was appended only for lookup association.
    pub(crate) appended_handle: bool,
    /// Whether the remote response evaluated every requested predicate.
    pub(crate) predicates_applied: bool,
    /// Number of rows that crossed the remote response boundary.
    pub(crate) wire_rows: u64,
}

/// One completed table lookup, preserving Go's chunk-backed worker result
/// whenever the remote stream supports it and retaining the row fallback for
/// hand-built or row-only sources.
pub(crate) enum FinishedLookup {
    /// A chunk-backed response from a real coprocessor stream.
    Chunk(FinishedLookupChunk),
    /// The compatibility path for row-only streams.
    Rows(Vec<(TableHandle, Vec<Datum>)>, bool, u64),
}

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
    /// appended handle column.
    width: usize,
    /// Where the integer handle sits in a row to be merged. `None` is a
    /// clean common-handle stream consumed without a staged overlay.
    handle_index: Option<usize>,
    table_id: i64,
    /// Whether the cursor must merge staged rows into the snapshot stream.
    /// Clean reads can consume projected remote rows directly without
    /// reconstructing an encoded record key for every row.
    merge_staged: bool,
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
    fn supports_lookup_chunks(&self) -> bool {
        !self.merge_staged && self.stream.supports_chunks() && !self.field_types.is_empty()
    }

    /// Appends clean remote rows without materializing an owned datum vector
    /// for every row. `None` means this cursor must use the row path;
    /// `Some(0)` means the chunk-capable stream is exhausted.
    fn append_clean_chunk(
        &mut self,
        output: &mut Chunk,
        target_rows: usize,
        allow_oversized: bool,
    ) -> Result<Option<usize>, KvTableError> {
        if self.merge_staged || !self.stream.supports_chunks() {
            return Ok(None);
        }
        let before = output.num_rows();
        while output.num_rows() < target_rows {
            if self.pending_chunk.is_none() {
                let next = self
                    .stream
                    .next_chunk()
                    .map_err(|error| KvTableError::Storage(format!("{error:?}")))?;
                if output.num_rows() == 0 {
                    if let Some(batch) = next.as_ref() {
                        // TiKV commonly terminates a page a few rows below
                        // the executor chunk cap. Returning that owned batch
                        // still preserves the source boundary and avoids
                        // copying a million-row ordered scan one cell at a
                        // time; tiny pages are coalesced below.
                        let direct_min_rows = target_rows.saturating_mul(3) / 4;
                        if batch.num_cols() == self.width
                            && batch.num_rows() >= direct_min_rows
                            && (allow_oversized || batch.num_rows() <= target_rows)
                        {
                            let rows = batch.num_rows();
                            *output = next.expect("the exact batch was just inspected");
                            self.note_wire_rows();
                            return Ok(Some(rows));
                        }
                    }
                }
                self.pending_chunk = next;
                self.pending_chunk_row = 0;
                self.note_wire_rows();
                if self.pending_chunk.is_none() {
                    break;
                }
            }
            let batch = self.pending_chunk.as_ref().expect("just installed");
            let available = batch.num_rows().saturating_sub(self.pending_chunk_row);
            let take = available.min(target_rows.saturating_sub(output.num_rows()));
            let projection =
                (self.width != batch.num_cols()).then(|| (0..self.width).collect::<Vec<_>>());
            for row in self.pending_chunk_row..self.pending_chunk_row + take {
                output.append_row_by_col_idxs(batch.get_row(row), projection.as_deref());
            }
            self.pending_chunk_row += take;
            if self.pending_chunk_row == batch.num_rows() {
                self.pending_chunk = None;
                self.pending_chunk_row = 0;
            }
        }
        Ok(Some(output.num_rows().saturating_sub(before)))
    }

    /// Reports rows received since the last report to
    /// [`crate::storage::capture_storage_ops`]'s probe.
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
            crate::storage::note_storage_op(|ops| ops.cop_rows += fresh);
        }
    }

    /// The next remote row, as its record key and its projected columns.
    fn next_remote(&mut self) -> Result<Option<KeyedRow>, KvTableError> {
        if self.pending_remote.is_some() {
            return Ok(self.pending_remote.clone());
        }
        let next = self
            .stream
            .next_row()
            .map_err(|e| KvTableError::Storage(format!("{e:?}")))?;
        self.note_wire_rows();
        let Some(mut row) = next else {
            return Ok(None);
        };
        let handle_index = self
            .handle_index
            .expect("only an integer-handle merge asks for keyed remote rows");
        let handle = match row.get(handle_index) {
            Some(Datum::Int(value)) => *value,
            Some(Datum::UInt(value)) => *value as i64,
            other => {
                return Err(KvTableError::Decode(format!(
                    "a coprocessor row carried no integer handle, got {other:?}"
                )));
            }
        };
        row.truncate(self.width);
        let key = encode_row_key_with_handle(self.table_id, &RecordHandle::Int(handle));
        self.pending_remote = Some((key, row));
        Ok(self.pending_remote.clone())
    }

    fn next_staged(&mut self) -> Option<StagedRow> {
        if self.pending_staged.is_none() {
            self.pending_staged = self.staged.next();
        }
        self.pending_staged.clone()
    }

    /// The next projected row, either directly from a clean common-handle
    /// stream or from the integer-handle snapshot/staged merge.
    pub fn next_row(&mut self) -> Result<Option<Vec<Datum>>, KvTableError> {
        if !self.merge_staged {
            let next = self
                .stream
                .next_row()
                .map_err(|error| KvTableError::Storage(format!("{error:?}")))?;
            self.note_wire_rows();
            return Ok(next.map(|mut row| {
                row.truncate(self.width);
                row
            }));
        }
        if self.handle_index.is_none() {
            debug_assert!(self.staged.len() == 0);
            let next = self
                .stream
                .next_row()
                .map_err(|error| KvTableError::Storage(format!("{error:?}")))?;
            self.note_wire_rows();
            return Ok(next.map(|mut row| {
                row.truncate(self.width);
                row
            }));
        }
        loop {
            let remote = self.next_remote()?;
            let staged = self.next_staged();
            match (remote, staged) {
                (None, None) => return Ok(None),
                (Some((_, row)), None) => {
                    self.pending_remote = None;
                    return Ok(Some(row));
                }
                (remote, Some((staged_key, staged_row))) => {
                    // A staged write of the same key is the transaction's own
                    // newer version of that row, so it replaces the snapshot's
                    // and a tombstone drops it entirely.
                    if let Some((remote_key, _)) = &remote {
                        let ordering = remote_key.as_slice().cmp(staged_key.as_slice());
                        let ordering = if self.descending {
                            ordering.reverse()
                        } else {
                            ordering
                        };
                        match ordering {
                            std::cmp::Ordering::Less => {
                                let (_, row) = self.pending_remote.take().expect("just peeked");
                                return Ok(Some(row));
                            }
                            std::cmp::Ordering::Equal => self.pending_remote = None,
                            std::cmp::Ordering::Greater => {}
                        }
                    }
                    self.pending_staged = None;
                    if let Some(row) = staged_row {
                        return Ok(Some(row));
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
        let next = self
            .stream
            .next_row()
            .map_err(|error| KvTableError::Storage(format!("{error:?}")))?;
        self.note_wire_rows();
        Ok(next)
    }

    /// Drains one decoded response batch for an integer-handle lookup. The
    /// coprocessor already owns a columnar `Chunk`; consuming that batch at
    /// once avoids a `next_row`/wire-counter call for every row while keeping
    /// the public lookup result's owned datum vectors and handle association.
    /// `None` means the stream is row-only (or a hand-built cursor lacks wire
    /// type metadata), so callers must use [`Self::next_row_with_handle`].
    fn next_chunk_with_handle(&mut self) -> Result<Option<Vec<Vec<Datum>>>, KvTableError> {
        let Some(batch) = self.next_raw_chunk_with_handle()? else {
            return Ok(None);
        };
        // Go's tableWorker iterates the decoded chunk and calls GetDatum on
        // each cell; the response decoder has already validated the wire
        // schema. Use the infallible, buffer-oriented equivalent here instead
        // of constructing a Result for every cell in every lookup row.
        let rows = (0..batch.num_rows())
            .map(|row| batch.get_row(row).get_datum_row(&self.field_types))
            .collect();
        Ok(Some(rows))
    }

    /// Pulls one decoded response chunk without materializing its rows.
    ///
    /// The response decoder already validated the wire schema. This is the
    /// chunk.Row equivalent of Go's `exec.Next` result handed to
    /// `tableWorker.executeTask`; the caller decides whether to retain the
    /// chunk or use the compatibility row conversion above.
    fn next_raw_chunk_with_handle(&mut self) -> Result<Option<Chunk>, KvTableError> {
        if !self.supports_lookup_chunks() {
            return Ok(None);
        }
        let Some(batch) = self
            .stream
            .next_chunk()
            .map_err(|error| KvTableError::Storage(format!("{error:?}")))?
        else {
            self.note_wire_rows();
            return Ok(Some(Chunk::new_with_capacity(
                &self.field_types[..self.width.min(self.field_types.len())],
                0,
            )));
        };
        self.note_wire_rows();
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

pub struct RemoteIndexHandleCursor {
    inner: Box<dyn PushdownRowStream>,
    handle_indices: Vec<usize>,
    /// Dense wire positions for a covering index row. `None` means this
    /// cursor is consumed as handles only by an IndexLookUp worker.
    projected_indices: Option<Vec<usize>>,
    common_handle: bool,
    zone: SessionTimeZone,
    use_new_collation: bool,
    noted_rows: u64,
    /// A clean integer-handle response can be consumed directly from the
    /// decoded chunk. None keeps the row path for common handles, where all
    /// key columns must still be encoded together.
    handle_is_unsigned: Option<bool>,
    pending_chunk: Option<Chunk>,
    pending_chunk_row: usize,
}

impl RemoteIndexHandleCursor {
    fn note_rows(&mut self) {
        let returned = self.inner.rows_returned();
        let fresh = returned.saturating_sub(self.noted_rows);
        self.noted_rows = returned;
        if fresh > 0 {
            crate::storage::note_storage_op(|ops| ops.cop_rows += fresh);
        }
    }

    /// Returns one covering index row in the source's pruned table-column
    /// order. Go's `PhysicalIndexReader` emits these values directly from the
    /// index stream, so no table row lookup or handle reordering is involved.
    pub fn next_projected_row(&mut self) -> Result<Option<Vec<Datum>>, KvTableError> {
        let Some(row) = self
            .inner
            .next_row()
            .map_err(|error| KvTableError::Storage(format!("{error:?}")))?
        else {
            self.note_rows();
            return Ok(None);
        };
        self.note_rows();
        let projected_indices = self.projected_indices.as_ref().ok_or_else(|| {
            KvTableError::Decode("index cursor has no covering projection".to_owned())
        })?;
        let mut projected = Vec::with_capacity(projected_indices.len());
        for index in projected_indices {
            projected.push(row.get(*index).cloned().ok_or_else(|| {
                KvTableError::Decode("index row omitted a covering column".to_owned())
            })?);
        }
        Ok(Some(projected))
    }

    /// Whether the remote coprocessor evaluated every requested predicate.
    /// Go's `PhysicalIndexReader` consumes the Selection result directly; the
    /// executor may therefore avoid re-running the same filter once the clean
    /// remote stream confirms the complete pushdown.
    pub fn predicates_applied(&self) -> bool {
        self.inner.predicates_applied()
    }

    /// Reads one integer handle directly from the remote response chunk.
    /// Go's indexWorker iterates the SelectResult chunk and extracts only the
    /// handle column; materializing a one-column Vec<Datum> for every index
    /// entry is unnecessary and especially costly for wide ranges.
    fn next_integer_handle_from_chunk(&mut self) -> Result<Option<TableHandle>, KvTableError> {
        loop {
            if let Some(batch) = self.pending_chunk.as_ref() {
                if self.pending_chunk_row < batch.num_rows() {
                    if batch.num_cols() == 0 {
                        return Err(KvTableError::Decode(
                            "index response omitted its integer handle column".to_owned(),
                        ));
                    }
                    let row = batch.get_row(self.pending_chunk_row);
                    let handle = if self.handle_is_unsigned == Some(true) {
                        row.get_uint64(0) as i64
                    } else {
                        row.get_int64(0)
                    };
                    let last = self.pending_chunk_row + 1 == batch.num_rows();
                    self.pending_chunk_row += 1;
                    if last {
                        self.pending_chunk = None;
                        self.pending_chunk_row = 0;
                    }
                    return Ok(Some(TableHandle::Int(handle)));
                }
                self.pending_chunk = None;
                self.pending_chunk_row = 0;
            }
            let Some(batch) = self
                .inner
                .next_chunk()
                .map_err(|error| KvTableError::Storage(format!("{error:?}")))?
            else {
                self.note_rows();
                return Ok(None);
            };
            self.note_rows();
            self.pending_chunk = Some(batch);
            self.pending_chunk_row = 0;
        }
    }

    /// Returns the next row handle in the remote index order.
    pub fn next_handle(&mut self) -> Result<Option<TableHandle>, KvTableError> {
        if self.handle_is_unsigned.is_some() && self.inner.supports_chunks() {
            return self.next_integer_handle_from_chunk();
        }
        let Some(row) = self
            .inner
            .next_row()
            .map_err(|error| KvTableError::Storage(format!("{error:?}")))?
        else {
            return Ok(None);
        };
        self.note_rows();
        if self.common_handle {
            let values = self
                .handle_indices
                .iter()
                .map(|index| row.get(*index).cloned())
                .collect::<Option<Vec<_>>>()
                .ok_or_else(|| {
                    KvTableError::Decode("an index row omitted a common handle column".to_owned())
                })?;
            let encoded = Encoder::new(self.use_new_collation)
                .encode_key_in_timezone(&self.zone, &values)
                .map_err(|error| KvTableError::Encode(format!("{error:?}")))?;
            return Ok(Some(TableHandle::Common(encoded)));
        }
        match self
            .handle_indices
            .first()
            .and_then(|index| row.get(*index))
        {
            Some(Datum::Int(value)) => Ok(Some(TableHandle::Int(*value))),
            Some(Datum::UInt(value)) => Ok(Some(TableHandle::Int(*value as i64))),
            other => Err(KvTableError::Decode(format!(
                "an index row carried no integer handle, got {other:?}"
            ))),
        }
    }

    /// Releases the remote request before a caller stops consuming it.
    pub fn close(&mut self) {
        self.inner.close();
    }
}

impl Drop for RemoteIndexHandleCursor {
    fn drop(&mut self) {
        self.close();
    }
}

/// A forward cursor over one index range, yielding row handles in index order.
///
/// See [`KvTable::index_range_cursor`].
pub struct IndexRangeCursor {
    iterators: Vec<Box<dyn StorageIterator>>,
    /// The pruned-partition ordinal each iterator reads, parallel to
    /// `iterators`. Always `0` for an unpartitioned table.
    partition_of_iterator: Vec<usize>,
    next_iterator: usize,
    merge_by_index_key: bool,
    merge_heap: IndexMergeHeap,
    index: KvIndex,
    common_handle: bool,
}

impl IndexRangeCursor {
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
        if self.merge_by_index_key {
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
            iterator
                .next()
                .map_err(|e| KvTableError::Storage(format!("{e:?}")))?;
            if iterator.valid() {
                self.merge_heap.push(
                    cut_index_prefix(iterator.key().as_bytes()).to_vec(),
                    position,
                );
            }
            let partition = self
                .partition_of_iterator
                .get(position)
                .copied()
                .unwrap_or(0);
            return Ok(Some((handle, partition)));
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
            iterator
                .next()
                .map_err(|e| KvTableError::Storage(format!("{e:?}")))?;
            let partition = self
                .partition_of_iterator
                .get(self.next_iterator)
                .copied()
                .unwrap_or(0);
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

/// Scans a [`KvTable`]'s record range into chunks -- the storage-backed source
/// (Go's table reader over `tablecodec`, minus distsql/coprocessor).
pub struct TableScanExec {
    meta: ExecutorMeta,
    table: KvTable,
    /// The open storage cursor; `None` before `open` and once exhausted, and
    /// always `None` when the backend served the scan through `remote`.
    cursor: Option<RowCursor>,
    /// The open coprocessor-served cursor, when the backend has one.
    remote: Option<RemoteRowCursor>,
    /// A TiKV partial-aggregation row stream. It is separate from `remote`
    /// because aggregate rows have no record handle and no staged merge.
    partial_remote: Option<Box<dyn PushdownRowStream>>,
    /// A remainder when a chunk-capable partial aggregate batch does not fit
    /// in the caller's output chunk. Keeping the decoded batch here avoids
    /// falling back to row materialisation on the next `next` call.
    partial_pending: Option<(Chunk, usize)>,
    /// Locally computed partial rows when the backend refuses aggregation.
    partial_rows: Option<std::vec::IntoIter<Vec<Datum>>>,
    /// Whether the local partial result has already been fully emitted.
    partial_done: bool,
    /// The partial aggregation this source accepted from the planner.
    partial_aggregate: Option<PushdownPartialAggregate>,
    /// Input schema and statement context retained for an expression-valued
    /// local partial-aggregation fallback after `meta` becomes the partial
    /// output schema.
    partial_input_types: Option<Vec<FieldType>>,
    partial_context: Option<crate::StmtContext>,
    /// Access-path cardinality selected by the optimizer. Go chooses the
    /// partial/final split only above the one-row floor for these workloads.
    estimated_rows: Option<f64>,
    /// Go's `desc` on the `TableScan`: walk the record ranges BACKWARDS.
    /// Set by [`crate::table_access::TableAccess::accept_keep_order`], and
    /// honored on the remote and the local cursor alike -- acceptance is a
    /// guarantee for EVERY path the scan may fall back to.
    descending: bool,
    /// Conjuncts this scan took over from the `Selection` above it.
    filter: Option<crate::predicate_pushdown::ScanFilterProbe>,
    /// The same conjuncts as a description, for a backend that can evaluate
    /// them at the region. They are applied locally regardless.
    pushed: Vec<ScanPredicate>,
    /// Final offsets into the wider scan row, applied only after `filter`.
    /// A remote cursor already returns this projection; the local path
    /// applies it after evaluating the same filter against the wider row.
    post_filter_projection: Option<Vec<usize>>,
    /// A TopN the remote backend may execute after its complete Selection.
    /// The driver retains a local partial TopN as the semantic fallback.
    remote_topn: Option<PushdownTopN>,
    /// Whether the record scan must preserve ascending key order for a parent
    /// MergeJoin or ordered aggregation. Go carries this from the required
    /// physical property into `TableReaderExecutor.keepOrder`.
    keep_order: bool,
    /// The table-column offsets this scan emits, in output order. Every
    /// column of the table until the driver prunes it.
    keep: Vec<usize>,
    /// Rows this scan read before filtering -- what Go's `TableFullScan`
    /// reports as `actRows`, which a filter above it must not change.
    scanned: std::rc::Rc<std::cell::Cell<u64>>,
    /// A pushed row cap (`offset + count` of a `LIMIT`): the scan stops once
    /// it has emitted this many qualifying rows. See
    /// [`Executor::accept_scan_limit`].
    limit: Option<u64>,
    /// Qualifying rows emitted so far, against `limit`.
    emitted: u64,
    /// The clustered-handle ranges this scan reads, when the driver offered
    /// them and this scan took them ([`Executor::accept_handle_ranges`]).
    /// `None` reads the whole table, which is every scan until the offer.
    handle_ranges: Option<Vec<IndexRange>>,
    /// The statement class and session zone this scan decodes under. Captured
    /// when the scan is BUILT, because `Executor::open` has no statement
    /// context of its own.
    decode_context: RowDecodeContext,
    /// The statement's coprocessor seam -- `DAGRequest.flags` plus the sink
    /// TiKV's warnings must reach. Captured beside `decode_context` and for
    /// the same reason: `Executor::open`, where the request is built, has no
    /// statement context of its own.
    statement: PushdownStatementContext,
    /// The output slot carrying `_tidb_rowid`, Go's extra handle column.
    ///
    /// Go appends it to a heap table's `DataSource` schema
    /// (`buildDataSource`'s `NewExtraHandleSchemaCol`) and it is the row's
    /// HANDLE, not a stored column -- so it has no entry in `keep`, and the
    /// local cursor's `(TableHandle, Vec<Datum>)` already carries its value.
    /// `None` for every scan the statement did not ask for it, which is all
    /// of them until the leaf sees the name.
    extra_handle_slot: Option<usize>,
}

/// A partial `SUM` in progress.
///
/// Go compiles `SUM` into one of two signatures chosen by the argument's
/// eval type (`pkg/executor/aggfuncs/func_sum.go`): `sum4Decimal` for
/// integer and decimal inputs, `sum4Float64` for real ones. Only the first
/// existed here, so `SUM` over a `FLOAT`/`DOUBLE` column refused the scan
/// instead of answering. Which arm this becomes is decided by the first
/// non-NULL value, exactly as the argument's type decides it in Go; an
/// all-NULL group stays `Empty` and answers NULL.
#[derive(Debug, Default, Clone)]
enum PartialSum {
    #[default]
    Empty,
    Decimal(Decimal),
    Real(f64),
}

impl PartialSum {
    /// Folds one scanned value in, skipping NULLs the way both Go signatures
    /// do. A value from the other eval family than the arm already chosen
    /// promotes the running total to real: Go cannot reach that case because
    /// the argument carries a single type, and real is the family MySQL
    /// would have merged such a column into.
    fn accumulate(&mut self, value: &Datum) -> Result<(), ExecError> {
        let addend = match value {
            Datum::Null => return Ok(()),
            Datum::Int(value) => PartialSum::Decimal(Decimal::from_int(*value)),
            Datum::UInt(value) => PartialSum::Decimal(Decimal::from_uint(*value)),
            Datum::Decimal(value) => PartialSum::Decimal(value.clone()),
            Datum::Real(value) | Datum::Float32(value) => PartialSum::Real(*value),
            _ => {
                return Err(ExecError::unsupported(
                    "partial SUM requires a numeric input",
                ));
            }
        };
        *self = match (std::mem::take(self), addend) {
            (PartialSum::Empty, addend) => addend,
            (PartialSum::Decimal(current), PartialSum::Decimal(addend)) => {
                PartialSum::Decimal(current.add(&addend))
            }
            (PartialSum::Real(current), PartialSum::Real(addend)) => {
                PartialSum::Real(current + addend)
            }
            (PartialSum::Decimal(current), PartialSum::Real(addend)) => {
                PartialSum::Real(current.to_f64() + addend)
            }
            (PartialSum::Real(current), PartialSum::Decimal(addend)) => {
                PartialSum::Real(current + addend.to_f64())
            }
            // `addend` is built from a non-NULL value, so it is never `Empty`.
            (current, PartialSum::Empty) => current,
        };
        Ok(())
    }

    /// The partial row's value: NULL when nothing non-NULL was folded in,
    /// which is what Go's aggregate answers for an empty or all-NULL group.
    fn into_datum(self) -> Datum {
        match self {
            PartialSum::Empty => Datum::Null,
            PartialSum::Decimal(value) => Datum::Decimal(value),
            PartialSum::Real(value) => Datum::Real(value),
        }
    }
}

impl TableScanExec {
    /// Builds a scan over `table` with an explicit row-decode context.
    #[must_use]
    pub fn new_with_context(
        meta: ExecutorMeta,
        table: KvTable,
        decode_context: RowDecodeContext,
        statement: PushdownStatementContext,
    ) -> Self {
        // A scan emits the VISIBLE columns: the schema its rows are appended
        // into is the visible one, and a hidden expression-index column's
        // value is only ever needed to write an index entry, never to answer
        // a read. It is still DECODED and filled -- `keep` is applied after
        // the virtual columns are materialized -- so an index built from the
        // scanned row still sees it.
        let keep = (0..table.visible_column_count()).collect();
        TableScanExec {
            meta,
            table,
            cursor: None,
            remote: None,
            partial_remote: None,
            partial_pending: None,
            partial_rows: None,
            partial_done: false,
            partial_aggregate: None,
            partial_input_types: None,
            partial_context: None,
            estimated_rows: None,
            descending: false,
            filter: None,
            pushed: Vec::new(),
            post_filter_projection: None,
            remote_topn: None,
            keep_order: false,
            keep,
            scanned: std::rc::Rc::new(std::cell::Cell::new(0)),
            limit: None,
            emitted: 0,
            handle_ranges: None,
            decode_context,
            statement,
            extra_handle_slot: None,
        }
    }

    /// Legacy zone-only constructor retained for unmigrated write/server
    /// callers. Origin defaults use the exact former
    /// `DEFAULT_STATEMENT_FLAGS` behavior.
    #[must_use]
    pub fn new(
        meta: ExecutorMeta,
        table: KvTable,
        zone: SessionTimeZone,
        statement: PushdownStatementContext,
    ) -> Self {
        Self::new_with_context(
            meta,
            table,
            RowDecodeContext::legacy_default(&zone),
            statement,
        )
    }

    /// The live count of rows read from storage, before any pushed filter.
    #[must_use]
    pub fn scanned_rows(&self) -> std::rc::Rc<std::cell::Cell<u64>> {
        std::rc::Rc::clone(&self.scanned)
    }

    /// How many rows the backend's coprocessor has sent across the network for
    /// this scan, or `None` when the scan is reading raw key/value bytes.
    ///
    /// This is the wire receipt: against a lowered predicate it is smaller
    /// than the table holds, which a byte-level scan can never be.
    #[must_use]
    pub fn rows_crossing_the_wire(&self) -> Option<u64> {
        self.remote.as_ref().map(RemoteRowCursor::rows_returned)
    }

    /// Opens the byte-level cursor over this scan's projection and ranges.
    /// The initial open and the fall-back from a refused pushdown share
    /// it, so the two paths cannot drift apart.
    fn open_local_cursor(&mut self) -> Result<(), ExecError> {
        // The pruned column set is the cursor's projection, so an
        // unreferenced column is never decoded on the streaming path either.
        let projection: Option<&[usize]> = if self.keep.len() == self.table.columns.len() {
            None
        } else {
            Some(&self.keep)
        };
        let handle_ranges = self.handle_ranges.clone();
        self.cursor = Some(
            self.table
                .row_cursor_projected_directed_with_context(
                    projection,
                    handle_ranges.as_deref(),
                    self.descending,
                    // Go carries the required physical property into
                    // `TableReaderExecutor.keepOrder`, and a partitioned
                    // reader with it set merge-sorts its per-partition
                    // results rather than concatenating them.
                    self.keep_order,
                    &self.decode_context,
                )
                .map_err(|error| {
                    ExecError::unsupported(format!("table bytes failed to decode: {error:?}"))
                })?,
        );
        Ok(())
    }
    /// The next row of whichever cursor is open, remote or local.
    ///
    /// A backend may REFUSE a request shape it cannot evaluate -- the
    /// embedded coprocessor answers `other_error` for a scalar signature its
    /// evaluator has not grown yet -- and that refusal reaches the caller as
    /// the first read rather than at open, because nothing is evaluated
    /// until a batch is asked for. `PushdownScannerError::Unsupported` names
    /// the contract for exactly this case: "never a wrong answer, only a
    /// slower one". So a remote that fails before yielding ANY row is
    /// abandoned for the byte-level cursor, which evaluates every conjunct
    /// locally. A remote that fails after yielding rows cannot be retried
    /// this way -- those rows are already emitted -- so it stays an error.
    fn next_source_row(&mut self) -> Result<Option<Vec<Datum>>, ExecError> {
        if let Some(remote) = self.remote.as_mut() {
            match remote.next_row() {
                Ok(Some(row)) => return Ok(Some(row)),
                Ok(None) => {
                    self.remote = None;
                    self.cursor = None;
                    return Ok(None);
                }
                Err(error) if remote.rows_returned() == 0 => {
                    // Nothing crossed the wire, so nothing is lost by
                    // scanning locally instead.
                    let refused = format!("{error:?}");
                    self.remote = None;
                    self.open_local_cursor().map_err(|open| {
                        ExecError::unsupported(format!(
                            "the backend refused the pushed-down scan ({refused}) \
                             and the local scan could not open: {open:?}"
                        ))
                    })?;
                }
                Err(error) => {
                    return Err(ExecError::unsupported(format!(
                        "table bytes failed to decode: {error:?}"
                    )))
                }
            }
        }
        let next = match (self.remote.as_mut(), self.cursor.as_mut()) {
            (Some(remote), _) => remote.next_row(),
            (None, Some(cursor)) => cursor.next_row().map(|row| {
                row.map(|(handle, projected)| match self.extra_handle_slot {
                    // Go's extra handle column IS the record handle, so the
                    // value the cursor already carries beside the row is the
                    // one `_tidb_rowid` reports.
                    Some(slot) => insert_extra_handle(projected, slot, &handle),
                    None => projected,
                })
            }),
            (None, None) => return Ok(None),
        }
        .map_err(|error| {
            ExecError::unsupported(format!("table bytes failed to decode: {error:?}"))
        })?;
        match next {
            Some(row) => Ok(Some(row)),
            None => {
                self.remote = None;
                self.cursor = None;
                Ok(None)
            }
        }
    }

    fn build_local_partial_rows(
        &mut self,
        aggregate: &PushdownPartialAggregate,
    ) -> Result<Vec<Vec<Datum>>, ExecError> {
        match aggregate {
            PushdownPartialAggregate::Count { input_offset, .. } => {
                let mut count = 0_i64;
                while let Some(row) = self.next_source_row()? {
                    self.scanned.set(self.scanned.get() + 1);
                    if let Some(filter) = self.filter.as_mut() {
                        if !filter.admits(&row)? {
                            continue;
                        }
                    }
                    if input_offset
                        .is_none_or(|offset| !matches!(row.get(offset), None | Some(Datum::Null)))
                    {
                        count += 1;
                    }
                }
                Ok(vec![vec![Datum::Int(count)]])
            }
            PushdownPartialAggregate::Sum { input_offset, .. } => {
                let mut sum = PartialSum::default();
                while let Some(row) = self.next_source_row()? {
                    self.scanned.set(self.scanned.get() + 1);
                    if let Some(filter) = self.filter.as_mut() {
                        if !filter.admits(&row)? {
                            continue;
                        }
                    }
                    let Some(value) = row.get(*input_offset) else {
                        return Err(ExecError::unsupported(
                            "partial SUM input is outside the scan row",
                        ));
                    };
                    sum.accumulate(value)?;
                }
                Ok(vec![vec![sum.into_datum()]])
            }
            PushdownPartialAggregate::Global { functions } => {
                enum PartialValue {
                    Count(i64),
                    SumDecimal(Option<Decimal>),
                    SumReal(Option<f64>),
                    Extreme {
                        value: Option<Datum>,
                        is_max: bool,
                        collation: tidb_datatype::Collation,
                    },
                }

                let input_types = self.partial_input_types.clone().ok_or_else(|| {
                    ExecError::unsupported("global partial aggregation lost its input schema")
                })?;
                let context = self.partial_context.clone().ok_or_else(|| {
                    ExecError::unsupported("global partial aggregation lost its statement context")
                })?;
                let mut values = functions
                    .iter()
                    .map(|function| match function.kind {
                        PushdownAggregateKind::Count => PartialValue::Count(0),
                        PushdownAggregateKind::Sum
                            if function.output_type.eval_type()
                                == tidb_datatype::EvalType::Real =>
                        {
                            PartialValue::SumReal(None)
                        }
                        PushdownAggregateKind::Sum => PartialValue::SumDecimal(None),
                        PushdownAggregateKind::Min => PartialValue::Extreme {
                            value: None,
                            is_max: false,
                            collation: crate::remote_scan::extreme_collation(
                                function.input.as_ref(),
                            ),
                        },
                        PushdownAggregateKind::Max => PartialValue::Extreme {
                            value: None,
                            is_max: true,
                            collation: crate::remote_scan::extreme_collation(
                                function.input.as_ref(),
                            ),
                        },
                    })
                    .collect::<Vec<_>>();
                while let Some(row) = self.next_source_row()? {
                    self.scanned.set(self.scanned.get() + 1);
                    if let Some(filter) = self.filter.as_mut() {
                        if !filter.admits(&row)? {
                            continue;
                        }
                    }
                    for (function, value) in functions.iter().zip(values.iter_mut()) {
                        let input = function
                            .input
                            .as_ref()
                            .map(|expression| {
                                crate::generated_column::eval_over_row(
                                    expression,
                                    &input_types,
                                    &row,
                                    &context,
                                )
                                .map_err(ExecError::Eval)
                            })
                            .transpose()?;
                        match (value, input) {
                            (PartialValue::Count(count), None) => *count += 1,
                            (PartialValue::Count(_), Some(Datum::Null)) => {}
                            (PartialValue::Count(count), Some(_)) => *count += 1,
                            (PartialValue::SumDecimal(_), None)
                            | (PartialValue::SumReal(_), None)
                            | (PartialValue::Extreme { .. }, None) => {
                                return Err(ExecError::unsupported(
                                    "only COUNT may omit a global partial aggregate input",
                                ));
                            }
                            (PartialValue::SumDecimal(_), Some(Datum::Null))
                            | (PartialValue::SumReal(_), Some(Datum::Null))
                            | (PartialValue::Extreme { .. }, Some(Datum::Null)) => {}
                            (PartialValue::SumDecimal(sum), Some(input)) => {
                                let addend = match input {
                                    Datum::Int(value) => Decimal::from_int(value),
                                    Datum::UInt(value) => Decimal::from_uint(value),
                                    Datum::Decimal(value) => value,
                                    _ => {
                                        return Err(ExecError::unsupported(
                                            "global partial SUM requires numeric input",
                                        ));
                                    }
                                };
                                *sum = Some(match sum.take() {
                                    Some(current) => current.add(&addend),
                                    None => addend,
                                });
                            }
                            (PartialValue::SumReal(sum), Some(input)) => {
                                let addend = input.to_f64().map_err(|_| {
                                    ExecError::unsupported(
                                        "global partial SUM requires numeric input",
                                    )
                                })?;
                                *sum = Some(sum.unwrap_or(0.0) + addend.value);
                            }
                            (
                                PartialValue::Extreme {
                                    value,
                                    is_max,
                                    collation,
                                },
                                Some(candidate),
                            ) => {
                                let replace = value.as_ref().is_none_or(|current| {
                                    crate::remote_scan::extreme_replaces(
                                        &candidate, current, *is_max, *collation,
                                    )
                                });
                                if replace {
                                    *value = Some(candidate);
                                }
                            }
                        }
                    }
                }
                Ok(vec![values
                    .into_iter()
                    .map(|value| match value {
                        PartialValue::Count(count) => Datum::Int(count),
                        PartialValue::SumDecimal(sum) => sum.map_or(Datum::Null, Datum::Decimal),
                        PartialValue::SumReal(sum) => sum.map_or(Datum::Null, Datum::Real),
                        PartialValue::Extreme { value, .. } => value.unwrap_or(Datum::Null),
                    })
                    .collect()])
            }
            PushdownPartialAggregate::GroupBy {
                input_offset,
                output_type,
            } => {
                let mut seen = HashSet::new();
                let mut rows = Vec::new();
                while let Some(row) = self.next_source_row()? {
                    self.scanned.set(self.scanned.get() + 1);
                    if let Some(filter) = self.filter.as_mut() {
                        if !filter.admits(&row)? {
                            continue;
                        }
                    }
                    let Some(value) = row.get(*input_offset).cloned() else {
                        return Err(ExecError::unsupported(
                            "partial GROUP BY input is outside the scan row",
                        ));
                    };
                    let key = crate::hash_agg::group_key_part(&output_type.collation(), &value);
                    if seen.insert(key) {
                        rows.push(vec![value]);
                    }
                }
                Ok(rows)
            }
            PushdownPartialAggregate::GroupBySum {
                group_offset,
                sum_offset,
                sum_type: _,
                group_type,
            } => {
                let mut groups: BTreeMap<Vec<u8>, (Datum, PartialSum)> = BTreeMap::new();
                while let Some(row) = self.next_source_row()? {
                    self.scanned.set(self.scanned.get() + 1);
                    if let Some(filter) = self.filter.as_mut() {
                        if !filter.admits(&row)? {
                            continue;
                        }
                    }
                    let Some(group) = row.get(*group_offset).cloned() else {
                        return Err(ExecError::unsupported(
                            "partial GROUP BY input is outside the scan row",
                        ));
                    };
                    let Some(value) = row.get(*sum_offset) else {
                        return Err(ExecError::unsupported(
                            "partial SUM input is outside the scan row",
                        ));
                    };
                    let key = crate::hash_agg::group_key_part(&group_type.collation(), &group);
                    let (_, sum) = groups.entry(key).or_insert((group, PartialSum::default()));
                    sum.accumulate(value)?;
                }
                Ok(groups
                    .into_values()
                    .map(|(group, sum)| vec![sum.into_datum(), group])
                    .collect())
            }
            PushdownPartialAggregate::Grouped {
                group_offsets,
                group_types,
                functions,
                streamed,
            } => {
                if group_offsets.len() != group_types.len() || group_offsets.is_empty() {
                    return Err(ExecError::unsupported(
                        "partial grouped aggregation requires typed group keys",
                    ));
                }

                let input_types = self.partial_input_types.clone().ok_or_else(|| {
                    ExecError::unsupported("partial aggregation lost its input schema")
                })?;
                let context = self.partial_context.clone().ok_or_else(|| {
                    ExecError::unsupported("partial aggregation lost its statement context")
                })?;

                enum PartialValue {
                    Count(i64),
                    Sum(PartialSum),
                    Extreme {
                        value: Option<Datum>,
                        is_max: bool,
                        collation: tidb_datatype::Collation,
                    },
                }

                let new_values = || {
                    functions
                        .iter()
                        .map(|function| match function.kind {
                            PushdownAggregateKind::Count => PartialValue::Count(0),
                            PushdownAggregateKind::Sum => PartialValue::Sum(PartialSum::default()),
                            PushdownAggregateKind::Min => PartialValue::Extreme {
                                value: None,
                                is_max: false,
                                collation: crate::remote_scan::extreme_collation(
                                    function.input.as_ref(),
                                ),
                            },
                            PushdownAggregateKind::Max => PartialValue::Extreme {
                                value: None,
                                is_max: true,
                                collation: crate::remote_scan::extreme_collation(
                                    function.input.as_ref(),
                                ),
                            },
                        })
                        .collect::<Vec<_>>()
                };
                let finish = |groups: Vec<Datum>, values: Vec<PartialValue>| {
                    values
                        .into_iter()
                        .map(|value| match value {
                            PartialValue::Count(count) => Datum::Int(count),
                            PartialValue::Sum(sum) => sum.into_datum(),
                            PartialValue::Extreme { value, .. } => value.unwrap_or(Datum::Null),
                        })
                        .chain(groups)
                        .collect::<Vec<_>>()
                };
                let group = |row: &[Datum]| -> Result<(Vec<u8>, Vec<Datum>), ExecError> {
                    let groups = group_offsets
                        .iter()
                        .map(|offset| {
                            row.get(*offset).cloned().ok_or_else(|| {
                                ExecError::unsupported(
                                    "partial GROUP BY input is outside the scan row",
                                )
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    let mut key = Vec::new();
                    for (group, field_type) in groups.iter().zip(group_types) {
                        key.extend_from_slice(&crate::hash_agg::group_key_part(
                            &field_type.collation(),
                            group,
                        ));
                        key.push(0xff);
                    }
                    Ok((key, groups))
                };
                let update = |values: &mut [PartialValue],
                              row: &[Datum]|
                 -> Result<(), ExecError> {
                    for (function, value) in functions.iter().zip(values.iter_mut()) {
                        let input = function
                            .input
                            .as_ref()
                            .map(|expression| {
                                crate::generated_column::eval_over_row(
                                    expression,
                                    &input_types,
                                    row,
                                    &context,
                                )
                                .map_err(ExecError::Eval)
                            })
                            .transpose()?;
                        match (value, input) {
                            (PartialValue::Count(count), None) => *count += 1,
                            (PartialValue::Count(_), Some(Datum::Null)) => {}
                            (PartialValue::Count(count), Some(_)) => *count += 1,
                            (PartialValue::Sum(_), None) | (PartialValue::Extreme { .. }, None) => {
                                return Err(ExecError::unsupported(
                                    "only COUNT may omit a partial aggregate input",
                                ));
                            }
                            (PartialValue::Sum(_), Some(Datum::Null))
                            | (PartialValue::Extreme { .. }, Some(Datum::Null)) => {}
                            (PartialValue::Sum(sum), Some(input)) => sum.accumulate(&input)?,
                            (
                                PartialValue::Extreme {
                                    value,
                                    is_max,
                                    collation,
                                },
                                Some(candidate),
                            ) => {
                                let replace = value.as_ref().is_none_or(|current| {
                                    crate::remote_scan::extreme_replaces(
                                        &candidate, current, *is_max, *collation,
                                    )
                                });
                                if replace {
                                    *value = Some(candidate);
                                }
                            }
                        }
                    }
                    Ok(())
                };

                if !streamed {
                    let mut grouped = BTreeMap::<Vec<u8>, (Vec<Datum>, Vec<PartialValue>)>::new();
                    while let Some(row) = self.next_source_row()? {
                        self.scanned.set(self.scanned.get() + 1);
                        if let Some(filter) = self.filter.as_mut() {
                            if !filter.admits(&row)? {
                                continue;
                            }
                        }
                        let (key, groups) = group(&row)?;
                        let (_, values) =
                            grouped.entry(key).or_insert_with(|| (groups, new_values()));
                        update(values, &row)?;
                    }
                    return Ok(grouped
                        .into_values()
                        .map(|(groups, values)| finish(groups, values))
                        .collect());
                }

                let mut rows = Vec::new();
                let mut current: Option<(Vec<u8>, Vec<Datum>, Vec<PartialValue>)> = None;
                while let Some(row) = self.next_source_row()? {
                    self.scanned.set(self.scanned.get() + 1);
                    if let Some(filter) = self.filter.as_mut() {
                        if !filter.admits(&row)? {
                            continue;
                        }
                    }
                    let (key, groups) = group(&row)?;
                    if current
                        .as_ref()
                        .is_some_and(|(current_key, _, _)| current_key != &key)
                    {
                        let (_, previous_groups, previous_values) =
                            current.take().expect("current group exists");
                        rows.push(finish(previous_groups, previous_values));
                    }
                    let (_, _, values) = current.get_or_insert_with(|| (key, groups, new_values()));
                    update(values, &row)?;
                }
                if let Some((_, groups, values)) = current {
                    rows.push(finish(groups, values));
                }
                Ok(rows)
            }
        }
    }
}

impl Executor for TableScanExec {
    fn open(&mut self) -> Result<(), ExecError> {
        self.scanned.set(0);
        self.emitted = 0;
        self.cursor = None;
        self.partial_remote = None;
        self.partial_pending = None;
        self.partial_rows = None;
        self.partial_done = false;
        if let Some(aggregate) = self.partial_aggregate.clone() {
            self.partial_remote = self
                .table
                .pushdown_partial_aggregate_cursor(
                    &self.keep.clone(),
                    &self.pushed,
                    self.handle_ranges.as_deref(),
                    &aggregate,
                    self.decode_context.zone(),
                    &self.statement,
                )
                .map_err(|error| {
                    ExecError::unsupported(format!("table bytes failed to decode: {error:?}"))
                })?;
            if self.partial_remote.is_some() {
                self.remote = None;
                return Ok(());
            }
        }
        // A backend with a coprocessor evaluates the predicate, the cap and
        // the projection at the region, so only the surviving rows cross the
        // network. Nothing about the answer depends on it succeeding: the
        // conjuncts and the cap are applied below either way, which is what
        // makes the fall-through a performance choice rather than a semantic
        // one.
        // A remote cursor answers the projected STORED columns and carries no
        // record handle beside them, so a scan that owes `_tidb_rowid` reads
        // records itself. This is the same performance-only choice the
        // comment above describes, taken for a slot the wire cannot fill.
        if self.extra_handle_slot.is_some() {
            return self.open_local_cursor();
        }
        self.remote = self
            .table
            .pushdown_row_cursor_with_context(
                &self.keep.clone(),
                &self.pushed,
                self.post_filter_projection.as_deref(),
                self.remote_topn.as_ref(),
                self.limit,
                self.handle_ranges.as_deref(),
                None,
                self.descending,
                self.keep_order,
                crate::remote_scan::DEFAULT_SCAN_READ_AHEAD_BATCHES,
                &self.decode_context,
                &self.statement,
            )
            .map_err(|error| {
                ExecError::unsupported(format!("table bytes failed to decode: {error:?}"))
            })?;
        if self.remote.is_some() {
            // A clean remote stream whose backend lowered every predicate is
            // already exact. Keep the local filter for residuals and staged
            // rows, but avoid evaluating the same expression once per wire
            // row on the common coprocessor path.
            if self
                .remote
                .as_ref()
                .is_some_and(|remote| remote.predicates_applied() && !remote.merge_staged)
            {
                self.filter = None;
            }
            return Ok(());
        }
        self.open_local_cursor()
    }

    /// Pulls rows from the open cursor until the chunk is full, the pushed
    /// row cap is reached, or the range is exhausted.
    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        let cap = self.meta.max_chunk_size();
        if let Some(remote) = self.partial_remote.as_mut() {
            if remote.supports_chunks() {
                if let Some(rows) = append_partial_remote_chunk(
                    remote.as_mut(),
                    &mut self.partial_pending,
                    req,
                    cap,
                )? {
                    self.emitted = self.emitted.saturating_add(rows as u64);
                    return Ok(());
                }
                self.partial_remote = None;
                self.partial_done = true;
                return Ok(());
            }
            while req.num_rows() < cap {
                let Some(row) = remote.next_row().map_err(|error| {
                    ExecError::unsupported(format!("partial aggregate response failed: {error:?}"))
                })?
                else {
                    self.partial_remote = None;
                    self.partial_done = true;
                    break;
                };
                for (column, value) in row.iter().enumerate() {
                    req.append_datum(column, value);
                }
            }
            return Ok(());
        }
        if let Some(aggregate) = self.partial_aggregate.clone() {
            if self.partial_done {
                return Ok(());
            }
            if self.partial_rows.is_none() {
                self.partial_rows = Some(self.build_local_partial_rows(&aggregate)?.into_iter());
            }
            let rows = self.partial_rows.as_mut().expect("just initialized");
            while req.num_rows() < cap {
                let Some(row) = rows.next() else {
                    self.partial_rows = None;
                    self.partial_done = true;
                    break;
                };
                for (column, value) in row.iter().enumerate() {
                    req.append_datum(column, value);
                }
            }
            return Ok(());
        }
        // A clean remote scan whose predicates were fully lowered can hand
        // off the decoded columnar batch directly. Keep the existing row
        // path for residual filters, staged overlays, and local fallbacks.
        if self.filter.is_none() {
            if let Some(remote) = self.remote.as_mut() {
                let target = self.limit.map_or(cap, |limit| {
                    usize::try_from(limit.saturating_sub(self.emitted))
                        .unwrap_or(usize::MAX)
                        .min(cap)
                });
                if target == 0 {
                    self.remote = None;
                    return Ok(());
                }
                if let Some(appended) = remote
                    .append_clean_chunk(req, target, self.limit.is_none())
                    .map_err(|error| {
                        ExecError::unsupported(format!("table bytes failed to decode: {error:?}"))
                    })?
                {
                    self.scanned
                        .set(self.scanned.get().saturating_add(appended as u64));
                    self.emitted = self.emitted.saturating_add(appended as u64);
                    if appended == 0 {
                        self.remote = None;
                    }
                    return Ok(());
                }
            }
        }
        while req.num_rows() < cap {
            if self.limit.is_some_and(|limit| self.emitted >= limit) {
                // Early stop: the cursor is dropped, so nothing past the
                // batch the cap fell in is read, and no row past the cap is
                // decoded. The backend cursor pulls the snapshot one batch at
                // a time, so "dropped" is what actually stops the reading --
                // it did not when the cursor materialized its whole range.
                self.cursor = None;
                self.remote = None;
                return Ok(());
            }
            let remote_projected = self.remote.is_some() && self.post_filter_projection.is_some();
            let Some(row) = self.next_source_row()? else {
                return Ok(());
            };
            self.scanned.set(self.scanned.get() + 1);
            if !remote_projected {
                if let Some(filter) = self.filter.as_mut() {
                    if !filter.admits(&row)? {
                        continue;
                    }
                }
            }
            if remote_projected {
                for (column, value) in row.iter().enumerate() {
                    req.append_datum(column, value);
                }
            } else if let Some(projection) = &self.post_filter_projection {
                for (column, offset) in projection.iter().enumerate() {
                    let Some(value) = row.get(*offset) else {
                        return Err(ExecError::unsupported(
                            "post-filter projection is outside the scan row",
                        ));
                    };
                    req.append_datum(column, value);
                }
            } else {
                for (column, value) in row.iter().enumerate() {
                    req.append_datum(column, value);
                }
            }
            self.emitted += 1;
        }
        Ok(())
    }

    fn close(&mut self) -> Result<(), ExecError> {
        self.cursor = None;
        self.remote = None;
        self.partial_remote = None;
        self.partial_pending = None;
        self.partial_rows = None;
        self.partial_done = false;
        Ok(())
    }

    fn schema(&self) -> &Schema {
        self.meta.schema()
    }

    fn ret_field_types(&self) -> &[FieldType] {
        self.meta.ret_field_types()
    }

    fn init_cap(&self) -> usize {
        self.meta.init_cap()
    }

    fn table_access(&mut self) -> Option<&mut dyn crate::table_access::TableAccess> {
        Some(self)
    }

    fn max_chunk_size(&self) -> usize {
        self.meta.max_chunk_size()
    }

    fn new_chunk(&self) -> Chunk {
        self.meta.new_chunk()
    }
}

/// Transfers one chunk-capable partial-aggregate stream into `req` without
/// converting every row to an owned `Vec<Datum>`. A decoded response batch may
/// be larger than the executor chunk cap; in that case the unconsumed suffix
/// stays in `pending` for the next call.
fn append_partial_remote_chunk(
    remote: &mut dyn PushdownRowStream,
    pending: &mut Option<(Chunk, usize)>,
    req: &mut Chunk,
    cap: usize,
) -> Result<Option<usize>, ExecError> {
    let before = req.num_rows();
    loop {
        let (batch, start) = if let Some((batch, start)) = pending.take() {
            (batch, start)
        } else {
            let batch = remote.next_chunk().map_err(|error| {
                ExecError::unsupported(format!("partial aggregate response failed: {error:?}"))
            })?;
            let Some(batch) = batch else {
                return Ok((req.num_rows() > before).then_some(req.num_rows() - before));
            };
            (batch, 0)
        };
        if start >= batch.num_rows() {
            continue;
        }
        let remaining = cap.saturating_sub(req.num_rows());
        if req.num_rows() == 0
            && start == 0
            && batch.num_cols() == req.num_cols()
            && batch.num_rows() >= cap.saturating_mul(3) / 4
        {
            let rows = batch.num_rows();
            *req = batch;
            return Ok(Some(rows));
        }
        let take = (batch.num_rows() - start).min(remaining);
        req.append_range_from(&batch, start, start + take);
        if start + take < batch.num_rows() {
            *pending = Some((batch, start + take));
        }
        if req.num_rows() >= cap {
            return Ok(Some(req.num_rows() - before));
        }
    }
}

impl crate::table_access::TableAccess for TableScanExec {
    fn accept_scan_estimate(&mut self, rows: f64) {
        self.estimated_rows = Some(rows);
    }

    /// Go `checkColCanUseIndex`: a record-key walk ranks by the clustered
    /// handle, or by the single integer handle column, and by nothing else --
    /// so the MaxMinEliminate bounded reverse read is only offered for those
    /// columns. A clustered handle's later columns need every earlier one
    /// pinned to one value across the read's ranges.
    fn accept_extreme_boundary(&mut self, order_offset: usize, desc: bool) -> bool {
        if self.limit.is_some()
            || self.partial_aggregate.is_some()
            || self.remote_topn.is_some()
            || !self.pushed.is_empty()
            || self.handle_ranges.as_ref().is_some_and(|ranges| ranges.len() != 1)
        {
            return false;
        }
        let Some(physical) = self.keep.get(order_offset) else {
            return false;
        };
        let common = self.table.common_handle_offsets();
        if common.is_empty() {
            // Go's int-handle arm accepts ONLY the handle column itself.
            if self.table.pk_handle_offset() != Some(*physical) {
                return false;
            }
        } else {
            let Some(rank) = common.iter().position(|offset| offset == physical) else {
                return false;
            };
            if rank > 0 {
                let Some(ranges) = self.handle_ranges.as_ref() else {
                    return false;
                };
                let range = &ranges[0];
                let fixed = range.low.len() >= rank
                    && range.high.len() >= rank
                    && !range.low_exclusive
                    && !range.high_exclusive
                    && (0..rank).all(|i| range.low[i] == range.high[i]);
                if !fixed {
                    return false;
                }
            }
        }
        self.accept_keep_order(desc) && self.accept_scan_limit(1)
    }

    fn accept_partial_aggregate(
        &mut self,
        aggregate: &PushdownPartialAggregate,
        ctx: &crate::StmtContext,
    ) -> bool {
        // Go `CheckAggPushDown` ends with `IsPushDownEnabled(aggFunc.Name,
        // storeType)`, so `mysql.expr_pushdown_blacklist` refuses an
        // aggregate by its own name exactly as it refuses a scalar function.
        if !crate::pushdown_blacklist::aggregate_admits(aggregate, ctx) {
            return false;
        }
        if self.estimated_rows.is_none_or(|rows| rows <= 1.0)
            || aggregate
                .input_offsets()
                .into_iter()
                .any(|offset| offset >= self.keep.len())
            || self.limit.is_some()
            || self.partial_aggregate.is_some()
        {
            return false;
        }
        let input_types = self.meta.ret_field_types().to_vec();
        let columns = aggregate
            .output_types()
            .into_iter()
            .enumerate()
            .map(|(offset, field_type)| {
                let mut column = tidb_expr::column::Column::new((offset + 1) as i64, field_type);
                column.index = offset as i64;
                column
            })
            .collect();
        self.meta = ExecutorMeta::new(
            Schema::new(columns),
            self.meta.id(),
            self.meta.init_cap(),
            self.meta.max_chunk_size(),
        );
        self.partial_aggregate = Some(aggregate.clone());
        self.partial_input_types = Some(input_types);
        self.partial_context = Some(ctx.clone());
        true
    }

    /// `scan_rows` reads the storage seam's merged stream -- the statement
    /// snapshot with the session's staged mutation buffer already merged in
    /// (`ClusterTableStorage`) -- and every row of it is tested here, whether
    /// it came from the byte cursor, from the coprocessor's answer, or from
    /// the staged overlay merged in below. That is what makes the promise in
    /// [`crate::table_access`] hold on both cursors.
    fn accept_scan_filter(
        &mut self,
        filter: &crate::predicate_pushdown::PushedScanFilter,
        ctx: &crate::StmtContext,
    ) -> bool {
        if filter.is_empty() {
            return false;
        }
        self.pushed = filter.predicates().to_vec();
        self.filter = Some(crate::predicate_pushdown::ScanFilterProbe::new(
            filter.clone(),
            ctx.clone(),
            self.meta.new_chunk(),
        ));
        true
    }

    fn accept_post_filter_projection(&mut self, keep: &[usize]) -> bool {
        // The only current consumer is a clean clustered common-handle range.
        // An integer-handle remote scan needs its handle column after the
        // projection in order to merge staged rows, so it must keep using the
        // unchanged wider-row contract until that handle is modelled as a
        // separate transport field.
        if self.filter.is_none()
            || self.table.common_handle_offsets.is_empty()
            || self.partial_aggregate.is_some()
            || keep.is_empty()
            || keep.iter().any(|offset| *offset >= self.keep.len())
        {
            return false;
        }
        let columns: Vec<tidb_expr::column::Column> = keep
            .iter()
            .enumerate()
            .map(|(index, offset)| {
                let mut column = self.meta.schema().columns[*offset].clone();
                column.index = index as i64;
                column.id = index as i64 + 1;
                column
            })
            .collect();
        self.meta = ExecutorMeta::new(
            Schema::new(columns),
            self.meta.id(),
            self.meta.init_cap(),
            self.meta.max_chunk_size(),
        );
        self.post_filter_projection = Some(keep.to_vec());
        true
    }

    fn accept_remote_topn(&mut self, topn: &PushdownTopN) -> bool {
        if topn.order_by.is_empty()
            || topn.limit == 0
            || topn
                .order_by
                .iter()
                .any(|item| item.offset >= self.keep.len())
            || self.partial_aggregate.is_some()
            || self.post_filter_projection.is_some()
            || self.limit.is_some()
            || self.remote_topn.is_some()
        {
            return false;
        }
        // Go's TopN-over-table-scan reader sets `KeepOrder: true` and walks
        // BACKWARD for a descending order (`table_reader.go` builds a cop
        // task whose TableFullScan carries keepOrder:true, desc under its
        // Limit), so the cop-side Limit reads only the boundary rows.
        // Without flipping the walk direction the region streams every row
        // ascending into the bound -- correct, but a full-scan worth of work
        // for LIMIT 1. All items must agree on the direction for the flip to
        // be valid; mixed directions keep the default forward walk and lean
        // on the cop's own TopN executor.
        let desc = topn.order_by[0].desc;
        if topn.order_by.iter().all(|item| item.desc == desc) {
            self.keep_order = true;
            self.descending = desc;
        }
        self.remote_topn = Some(topn.clone());
        true
    }

    /// The scan reads its range in one forward pass and emits rows in that
    /// order, so stopping after `cap` of them yields the same prefix a
    /// `LimitExec` above would have kept.
    fn accept_scan_limit(&mut self, cap: u64) -> bool {
        self.limit = Some(cap);
        true
    }

    /// Go's `keep order:true` on a table scan. ASC preserves the forward
    /// record-key walk; DESC reverses both the local and remote cursors.
    fn accept_keep_order(&mut self, descending: bool) -> bool {
        self.keep_order = true;
        self.descending = descending;
        true
    }

    fn scanned_rows_counter(&self) -> Option<std::rc::Rc<std::cell::Cell<u64>>> {
        Some(self.scanned_rows())
    }

    /// The scan reads the record keys the ranges cover and nothing else, on
    /// both cursors: the byte cursor opens one storage iterator per range,
    /// and the coprocessor request carries them as its key ranges.
    ///
    /// A shape [`crate::handle_range::record_key_ranges`] cannot encode falls
    /// back to the whole record range, which is a SUPERSET of the ranges and
    /// therefore still every row the statement admits -- the weaker half of
    /// the promise in [`crate::table_access`], and the only one that matters
    /// for correctness.
    fn accept_handle_ranges(&mut self, ranges: &[IndexRange]) -> bool {
        self.handle_ranges = Some(ranges.to_vec());
        true
    }

    /// Names the output slot that carries `_tidb_rowid`.
    ///
    /// Refused unless this scan reads records with the LOCAL cursor: the
    /// value is the record handle, and only that path carries one beside the
    /// row. A refusal leaves the leaf to decline the column rather than
    /// answer a slot it cannot fill.
    fn accept_extra_handle(&mut self, slot: usize) -> bool {
        // The slot sits immediately after the stored columns this scan
        // emits; anywhere else and the row would not line up with the schema
        // the leaf built. A partial aggregate produces no record handles at
        // all, so it cannot make the promise.
        if self.partial_aggregate.is_some()
            || self.remote_topn.is_some()
            || self.post_filter_projection.is_some()
            || slot != self.meta.schema().columns.len()
        {
            return false;
        }
        let mut columns = self.meta.schema().columns.clone();
        let mut handle = tidb_expr::column::Column::new(
            slot as i64 + 1,
            tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong)
                .with_flags(tidb_datatype::FieldTypeFlags::NOT_NULL),
        );
        handle.index = slot as i64;
        columns.push(handle);
        self.meta = ExecutorMeta::new(
            Schema::new(columns),
            self.meta.id(),
            self.meta.init_cap(),
            self.meta.max_chunk_size(),
        );
        self.extra_handle_slot = Some(slot);
        true
    }

    /// The scan reads the named partitions and no others: the restriction
    /// goes onto the table handle, which is where every one of this scan's
    /// key ranges -- whole-relation and handle-narrowed alike -- gets its id
    /// list from. Narrowing is cumulative, so a `PARTITION (p)` restriction
    /// already on the handle survives this.
    fn accept_partition_pruning(&mut self, ids: &[i64]) -> bool {
        self.table.restrict_read_to_partitions(ids);
        true
    }

    /// The scan narrows both what it decodes and what it emits, so the
    /// promise `accept_column_prune` makes holds for every row.
    ///
    /// A join may derive and push a leaf-local predicate before its outermost
    /// two-table pruning gate runs. The gate includes every predicate column
    /// in `keep`, so both executable and coprocessor descriptions can be
    /// remapped to the narrowed row without changing the predicate.
    fn accept_column_prune(&mut self, keep: &[usize]) -> bool {
        if keep.is_empty() {
            return false;
        }
        // `_tidb_rowid` sits beside the stored columns rather than among
        // them, so a projection expressed in stored offsets can no longer
        // describe this row. The leaf offers the handle slot only after it
        // has finished pruning, so this cannot refuse a prune that matters.
        if self.extra_handle_slot.is_some() {
            return false;
        }
        if keep.iter().any(|offset| *offset >= self.keep.len()) {
            return false;
        }
        let columns: Vec<tidb_expr::column::Column> = keep
            .iter()
            .enumerate()
            .map(|(index, offset)| {
                let mut column = self.meta.schema().columns[*offset].clone();
                column.index = index as i64;
                // The driver's scope resolver hands expressions the unique id
                // `index + 1`, so the schema must renumber with it or the two
                // would disagree about which column is which.
                column.id = index as i64 + 1;
                column
            })
            .collect();
        let meta = ExecutorMeta::new(
            Schema::new(columns),
            self.meta.id(),
            self.meta.init_cap(),
            self.meta.max_chunk_size(),
        );
        let remapped_filter = match &self.filter {
            Some(filter) => {
                let Some(filter) = filter.remapped_columns(keep, meta.new_chunk()) else {
                    return false;
                };
                Some(filter)
            }
            None => None,
        };
        self.meta = meta;
        self.filter = remapped_filter;
        if let Some(filter) = &self.filter {
            self.pushed = filter.predicates().to_vec();
        }
        // `keep` indexes the CURRENT output, which a previous prune may
        // already have narrowed, so the table offsets compose rather than
        // replace.
        self.keep = keep.iter().map(|offset| self.keep[*offset]).collect();
        true
    }
}

#[cfg(test)]
mod remote_cursor_tests {
    use super::*;
    use crate::ddl::index_prefix::UNSPECIFIED_LENGTH;
    use crate::kv_table::KvColumn;
    use crate::storage::{StorageError, TableStorage};

    /// Records the one request a builder sends, then declines to serve it so
    /// the test can assert on the wire shape itself.
    #[derive(Debug)]
    struct RequestCapture {
        captured: std::sync::Arc<
            std::sync::Mutex<Option<crate::remote_scan::PushdownScanRequest>>,
        >,
    }

    impl TableStorage for RequestCapture {
        fn get(&mut self, _key: &Key) -> Result<Vec<u8>, StorageError> {
            Err(StorageError::NotFound)
        }

        fn set(&mut self, _key: Key, _value: Vec<u8>) -> Result<(), StorageError> {
            Err(StorageError::Backend("unused".to_owned()))
        }

        fn delete(&mut self, _key: Key) -> Result<(), StorageError> {
            Err(StorageError::Backend("unused".to_owned()))
        }

        fn iter(
            &mut self,
            _start: Option<&Key>,
            _upper_bound: Option<&Key>,
        ) -> Result<Box<dyn StorageIterator>, StorageError> {
            Err(StorageError::Backend("unused".to_owned()))
        }

        fn open_remote_scan(
            &mut self,
            request: &crate::remote_scan::PushdownScanRequest,
        ) -> Option<Result<crate::remote_scan::PushdownScan, StorageError>> {
            *self
                .captured
                .lock()
                .unwrap_or_else(|poison| poison.into_inner()) = Some(request.clone());
            None
        }

        fn key_count(&self) -> usize {
            0
        }

        fn clear(&mut self) {}

        fn clone_box(&self) -> Box<dyn TableStorage> {
            Box::new(RequestCapture {
                captured: std::sync::Arc::clone(&self.captured),
            })
        }
    }

    fn bigint_column(id: i64, name: &str) -> KvColumn {
        KvColumn {
            name: name.to_owned(),
            id,
            field_type: FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
            column_info_version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
            default_value: None,
            origin_default: None,
            comment: String::new(),
            generated: None,
        }
    }

    /// Go's `PhysicalIndexReader.ToPB` names the clustered primary's column
    /// ids on a covering-index aggregate and appends those key columns to the
    /// executor schema after the indexed ones -- TiKV decodes the executor as
    /// `[index datums..., handle datums...]` by subtracting their count from
    /// the width. A common-handle table's `count(*)` through this builder
    /// must build exactly that shape instead of refusing into the local walk.
    #[test]
    fn an_index_aggregate_over_a_common_handle_table_names_its_primary_columns() {
        let captured = std::sync::Arc::new(std::sync::Mutex::new(None));
        let mut table = KvTable::with_storage(
            91,
            vec![
                bigint_column(1, "a"),
                bigint_column(2, "b"),
                bigint_column(3, "c"),
            ],
            Box::new(RequestCapture {
                captured: std::sync::Arc::clone(&captured),
            }),
        );
        table.set_common_handle_offsets(vec![0, 1]);
        table.add_index(crate::kv_table::table_meta::KvIndex {
            id: 5,
            name: "idx_c".to_owned(),
            comment: String::new(),
            unique: false,
            prefix_lengths: vec![UNSPECIFIED_LENGTH],
            column_offsets: vec![2],
            visible: true,
            global: false,
            clustered_primary: false,
        }, false);

        let aggregate = crate::remote_scan::PushdownPartialAggregate::Global {
            functions: vec![crate::remote_scan::PushdownGlobalAggregateFunction {
                kind: crate::remote_scan::PushdownAggregateKind::Count,
                input: None,
                output_type: FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
            }],
        };
        let ranges = [IndexRange {
            low: vec![Datum::Int(1)],
            high: vec![Datum::Int(9)],
            low_exclusive: false,
            high_exclusive: false,
        }];
        let statement = PushdownStatementContext::default();
        let stream = table.pushdown_index_partial_aggregate_cursor(
            5,
            &ranges,
            &[0, 1, 2],
            &[],
            &aggregate,
            &tidb_datatype::SessionTimeZone::utc(),
            &statement,
        );
        // The capture store declines to serve, so the builder reports no
        // cursor -- but only AFTER the request was built and recorded.
        assert!(stream.unwrap().is_none());

        let request = captured
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone()
            .expect("the aggregate request was built");
        let index = request.index.as_ref().expect("an index scan request");
        assert_eq!(index.index_id, 5);
        assert_eq!(
            request.primary_column_ids,
            vec![1, 2],
            "the clustered primary travels for TiKV's common-handle decode"
        );
        let schema_ids: Vec<i64> =
            request.columns.iter().map(|column| column.id).collect();
        assert_eq!(
            schema_ids,
            vec![3, 1, 2],
            "handle columns ride after the indexed columns, Go InitSchema order"
        );
    }

    struct VecStream {
        rows: std::collections::VecDeque<Vec<Datum>>,
        returned: u64,
        predicates_applied: bool,
    }

    impl PushdownRowStream for VecStream {
        fn next_row(&mut self) -> Result<Option<Vec<Datum>>, StorageError> {
            let row = self.rows.pop_front();
            if row.is_some() {
                self.returned += 1;
            }
            Ok(row)
        }

        fn rows_returned(&self) -> u64 {
            self.returned
        }

        fn predicates_applied(&self) -> bool {
            self.predicates_applied
        }

        fn close(&mut self) {}
    }

    /// Go's covering `PhysicalIndexReader` returns the requested projection
    /// from the index response itself; it must not turn the row into a table
    /// handle lookup. This is the direct cursor-level regression for that
    /// contract (the integration receipt exercises the live TiKV stream).
    #[test]
    fn covering_cursor_projects_index_rows_without_lookup() {
        let mut cursor = RemoteIndexHandleCursor {
            inner: Box::new(VecStream {
                rows: std::collections::VecDeque::from([vec![
                    Datum::Int(11),
                    Datum::Int(22),
                    Datum::Int(33),
                ]]),
                returned: 0,
                predicates_applied: true,
            }),
            handle_indices: vec![2],
            projected_indices: Some(vec![1, 0]),
            common_handle: false,
            zone: tidb_datatype::SessionTimeZone::utc(),
            use_new_collation: false,
            noted_rows: 0,
            handle_is_unsigned: None,
            pending_chunk: None,
            pending_chunk_row: 0,
        };
        assert_eq!(
            cursor.next_projected_row().unwrap(),
            Some(vec![Datum::Int(22), Datum::Int(11)])
        );
        assert!(
            cursor.predicates_applied(),
            "covering readers may skip a duplicate local Selection only after the cop confirms it"
        );
        assert!(cursor.next_projected_row().unwrap().is_none());
    }

    struct ChunkStream {
        chunks: std::collections::VecDeque<Chunk>,
        returned: u64,
    }

    impl PushdownRowStream for ChunkStream {
        fn next_row(&mut self) -> Result<Option<Vec<Datum>>, StorageError> {
            panic!("the clean cursor must use the columnar handoff")
        }

        fn supports_chunks(&self) -> bool {
            true
        }

        fn next_chunk(&mut self) -> Result<Option<Chunk>, StorageError> {
            let chunk = self.chunks.pop_front();
            self.returned += chunk.as_ref().map_or(0, |chunk| chunk.num_rows() as u64);
            Ok(chunk)
        }

        fn rows_returned(&self) -> u64 {
            self.returned
        }

        fn close(&mut self) {}
    }

    #[test]
    fn integer_handle_cursor_reads_chunk_without_row_materialization() {
        let field_types = vec![FieldType::new(tidb_datatype::FieldTypeCode::LongLong)];
        let mut batch = Chunk::new_with_capacity(&field_types, 2);
        batch.append_int64(0, 7);
        batch.append_int64(0, 8);
        let mut cursor = RemoteIndexHandleCursor {
            inner: Box::new(ChunkStream {
                chunks: std::collections::VecDeque::from([batch]),
                returned: 0,
            }),
            handle_indices: vec![0],
            projected_indices: None,
            common_handle: false,
            zone: tidb_datatype::SessionTimeZone::utc(),
            use_new_collation: false,
            noted_rows: 0,
            handle_is_unsigned: Some(false),
            pending_chunk: None,
            pending_chunk_row: 0,
        };

        assert_eq!(
            cursor.next_handle().unwrap(),
            Some(TableHandle::Int(7))
        );
        assert_eq!(
            cursor.next_handle().unwrap(),
            Some(TableHandle::Int(8))
        );
        assert_eq!(cursor.next_handle().unwrap(), None);
    }

    #[test]
    fn clean_remote_cursor_skips_record_key_reconstruction() {
        let mut cursor = RemoteRowCursor {
            stream: Box::new(VecStream {
                rows: std::collections::VecDeque::from([
                    vec![Datum::Int(7), Datum::Int(70)],
                    vec![Datum::Int(8), Datum::Int(80)],
                ]),
                returned: 0,
                predicates_applied: false,
            }),
            staged: Vec::new().into_iter(),
            pending_staged: None,
            pending_remote: None,
            pending_chunk: None,
            pending_chunk_row: 0,
            field_types: Vec::new(),
            width: 1,
            handle_index: Some(0),
            table_id: 0,
            merge_staged: false,
            descending: false,
            noted_rows: 0,
            predicates_applied: false,
        };

        assert_eq!(cursor.next_row().unwrap(), Some(vec![Datum::Int(7)]));
        assert_eq!(cursor.next_row().unwrap(), Some(vec![Datum::Int(8)]));
        assert_eq!(cursor.next_row().unwrap(), None);
    }

    #[test]
    fn clean_remote_cursor_appends_columnar_batches_without_row_materialization() {
        let source_types = vec![
            FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
            FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
        ];
        let mut batch = Chunk::new_with_capacity(&source_types, 2);
        batch.append_int64(0, 7);
        batch.append_int64(1, 70);
        batch.append_int64(0, 8);
        batch.append_int64(1, 80);
        let mut cursor = RemoteRowCursor {
            stream: Box::new(ChunkStream {
                chunks: std::collections::VecDeque::from([batch]),
                returned: 0,
            }),
            staged: Vec::new().into_iter(),
            pending_staged: None,
            pending_remote: None,
            pending_chunk: None,
            pending_chunk_row: 0,
            field_types: Vec::new(),
            width: 1,
            handle_index: Some(1),
            table_id: 0,
            merge_staged: false,
            descending: false,
            noted_rows: 0,
            predicates_applied: true,
        };
        let mut output = Chunk::new_with_capacity(&source_types[..1], 2);

        assert_eq!(
            cursor.append_clean_chunk(&mut output, 2, false).unwrap(),
            Some(2)
        );
        assert_eq!(output.num_rows(), 2);
        assert_eq!(output.get_row(0).get_int64(0), 7);
        assert_eq!(output.get_row(1).get_int64(0), 8);
        assert_eq!(
            cursor.append_clean_chunk(&mut output, 3, false).unwrap(),
            Some(0)
        );
    }

    #[test]
    fn handle_lookup_groups_ranges_with_go_row_count_hints() {
        let captured = std::sync::Arc::new(std::sync::Mutex::new(None));
        let mut table = KvTable::with_storage(
            91,
            vec![bigint_column(1, "v")],
            Box::new(RequestCapture {
                captured: std::sync::Arc::clone(&captured),
            }),
        );
        let handles = vec![
            TableHandle::Int(8),
            TableHandle::Int(5),
            TableHandle::Int(7),
            TableHandle::Int(5),
        ];

        assert!(
            table
                .stage_rows_by_handles_filtered(
                    &handles,
                    &[0],
                    &[],
                    &tidb_datatype::SessionTimeZone::utc(),
                    &PushdownStatementContext::default(),
                )
                .unwrap()
                .is_none(),
            "the recording store intentionally refuses the remote scan"
        );
        let request = captured
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone()
            .expect("handle lookup request");
        assert_eq!(request.ranges.len(), 2);
        assert_eq!(request.range_hints, vec![1, 2]);
    }

    #[test]
    fn handle_lookup_drains_columnar_batches_and_restores_index_order() {
        let source_types = vec![
            FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
            FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
        ];
        let mut first_batch = Chunk::new_with_capacity(&source_types, 1);
        first_batch.append_int64(0, 70);
        first_batch.append_int64(1, 7);
        let mut second_batch = Chunk::new_with_capacity(&source_types, 1);
        second_batch.append_int64(0, 80);
        second_batch.append_int64(1, 8);
        let cursor = RemoteRowCursor {
            stream: Box::new(ChunkStream {
                chunks: std::collections::VecDeque::from([first_batch, second_batch]),
                returned: 0,
            }),
            staged: Vec::new().into_iter(),
            pending_staged: None,
            pending_remote: None,
            pending_chunk: None,
            pending_chunk_row: 0,
            field_types: source_types,
            width: 1,
            handle_index: Some(1),
            table_id: 0,
            merge_staged: false,
            descending: false,
            noted_rows: 0,
            predicates_applied: true,
        };
        let staged = StagedHandlesLookup {
            cursor,
            handle_position: 1,
            appended_handle: true,
        };
        let handles = vec![TableHandle::Int(8), TableHandle::Int(7)];
        let Some(FinishedLookup::Chunk(finished)) =
            KvTable::finish_lookup_by_handles(&handles, staged).unwrap()
        else {
            panic!("columnar handle lookup unexpectedly refused");
        };
        assert!(finished.predicates_applied);
        assert_eq!(finished.wire_rows, 2);
        let rows = finished
            .row_positions
            .iter()
            .map(|(batch, row)| {
                (
                    finished.batches[*batch].get_row(*row).get_int64(1),
                    finished.batches[*batch].get_row(*row).get_int64(0),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(rows, vec![(8, 80), (7, 70)]);
        assert_eq!(finished.batches.len(), 2);
        assert_eq!(finished.row_positions, vec![(1, 0), (0, 0)]);
    }

    #[test]
    fn clean_remote_cursor_moves_an_exact_width_batch_into_the_output() {
        let source_types = vec![FieldType::new(tidb_datatype::FieldTypeCode::LongLong)];
        let mut batch = Chunk::new_with_capacity(&source_types, 2);
        batch.append_int64(0, 7);
        batch.append_int64(0, 8);
        let mut cursor = RemoteRowCursor {
            stream: Box::new(ChunkStream {
                chunks: std::collections::VecDeque::from([batch]),
                returned: 0,
            }),
            staged: Vec::new().into_iter(),
            pending_staged: None,
            pending_remote: None,
            pending_chunk: None,
            pending_chunk_row: 0,
            field_types: Vec::new(),
            width: 1,
            handle_index: None,
            table_id: 0,
            merge_staged: false,
            descending: false,
            noted_rows: 0,
            predicates_applied: true,
        };
        let mut output = Chunk::new_with_capacity(&source_types, 2);
        assert_eq!(
            cursor.append_clean_chunk(&mut output, 2, false).unwrap(),
            Some(2)
        );
        assert_eq!(output.get_row(0).get_int64(0), 7);
        assert_eq!(output.get_row(1).get_int64(0), 8);
    }

    #[test]
    fn clean_remote_cursor_transfers_an_oversized_batch_without_a_limit() {
        let source_types = vec![FieldType::new(tidb_datatype::FieldTypeCode::LongLong)];
        let mut batch = Chunk::new_with_capacity(&source_types, 4);
        for value in 7..11 {
            batch.append_int64(0, value);
        }
        let mut cursor = RemoteRowCursor {
            stream: Box::new(ChunkStream {
                chunks: std::collections::VecDeque::from([batch]),
                returned: 0,
            }),
            staged: Vec::new().into_iter(),
            pending_staged: None,
            pending_remote: None,
            pending_chunk: None,
            pending_chunk_row: 0,
            field_types: Vec::new(),
            width: 1,
            handle_index: None,
            table_id: 0,
            merge_staged: false,
            descending: false,
            noted_rows: 0,
            predicates_applied: true,
        };
        let mut output = Chunk::new_with_capacity(&source_types, 2);
        assert_eq!(
            cursor.append_clean_chunk(&mut output, 2, true).unwrap(),
            Some(4)
        );
        assert_eq!(output.num_rows(), 4);
        assert_eq!(output.get_row(3).get_int64(0), 10);
    }

    #[test]
    fn partial_remote_chunk_handoff_preserves_remainder_without_row_materialization() {
        let source_types = vec![FieldType::new(tidb_datatype::FieldTypeCode::LongLong)];
        let mut small = Chunk::new_with_capacity(&source_types, 1);
        small.append_int64(0, 7);
        let mut large = Chunk::new_with_capacity(&source_types, 5);
        for value in 8..13 {
            large.append_int64(0, value);
        }
        let mut stream = ChunkStream {
            chunks: std::collections::VecDeque::from([small, large]),
            returned: 0,
        };
        let mut pending = None;
        let mut output = Chunk::new_with_capacity(&source_types, 4);
        assert_eq!(
            append_partial_remote_chunk(&mut stream, &mut pending, &mut output, 4).unwrap(),
            Some(4)
        );
        assert_eq!(
            (0..output.num_rows())
                .map(|row| output.get_row(row).get_int64(0))
                .collect::<Vec<_>>(),
            vec![7, 8, 9, 10]
        );
        output.reset();
        assert_eq!(
            append_partial_remote_chunk(&mut stream, &mut pending, &mut output, 4).unwrap(),
            Some(2)
        );
        assert_eq!(output.get_row(0).get_int64(0), 11);
        assert_eq!(output.get_row(1).get_int64(0), 12);
        output.reset();
        assert_eq!(
            append_partial_remote_chunk(&mut stream, &mut pending, &mut output, 4).unwrap(),
            None
        );
    }
}

/// Places the record handle in the output slot Go gives `_tidb_rowid`.
///
/// A common-handle table has no extra handle column at all -- Go builds its
/// `HandleCols` from the primary index instead -- so only the integer form
/// can reach here, and an unsigned one keeps the value it was stored under.
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
