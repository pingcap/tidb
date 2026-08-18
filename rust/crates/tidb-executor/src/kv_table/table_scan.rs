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
use std::collections::{BTreeMap, BinaryHeap, HashSet};
use tidb_chunk::chunk::Chunk;
use tidb_codec::table_key::{
    cut_index_prefix, decode_table_id, encode_index_seek_key, encode_row_key_with_handle,
    get_table_handle_key_range, RecordHandle,
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
        let decoder = self.row_decoder_projected(keep, context)?;
        self.row_cursor_with_decoder(decoder, handle_ranges, context.zone())
    }

    /// One handle lookup decoded with the same physical-column projection as
    /// a table cursor. This is the point-read half of an index lookup join.
    pub(crate) fn get_row_by_handle_projected_with_context(
        &mut self,
        handle: &TableHandle,
        keep: Option<&[usize]>,
        context: &RowDecodeContext,
    ) -> Result<Option<Vec<Datum>>, KvTableError> {
        let Some((_, entry)) = self.stored_record(handle)? else {
            return Ok(None);
        };
        let decoder = self.row_decoder_projected(keep, context)?;
        let (mut values, _) = decoder.decode_and_eval(handle, &entry)?.into_parts();
        if let Some(keep) = keep {
            values = keep
                .iter()
                .map(|offset| std::mem::replace(&mut values[*offset], Datum::Null))
                .collect();
        }
        Ok(Some(values))
    }

    fn row_cursor_with_decoder(
        &mut self,
        decoder: RowDecoder,
        handle_ranges: Option<&[IndexRange]>,
        zone: &SessionTimeZone,
    ) -> Result<RowCursor, KvTableError> {
        let mut iterators = Vec::new();
        for (low, upper) in self.record_key_ranges(handle_ranges, zone)? {
            iterators.push(
                self.store
                    .iter(Some(&low), Some(&upper))
                    .map_err(|e| KvTableError::Storage(format!("{e:?}")))?,
            );
        }
        Ok(RowCursor {
            iterators: iterators.into_iter(),
            current: None,
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

    /// The record ranges this scan reads, as the storage seam's half-open
    /// `[start, end)` pairs in ascending key order.
    ///
    /// With no handle ranges this is the ONE range the whole relation lives
    /// in ([`KvTable::record_key_range`]). With them it is the intervals
    /// [`crate::handle_range::record_key_ranges`] encodes, which is what
    /// makes a `TableRangeScan` read less than the table.
    fn record_key_ranges(
        &self,
        handle_ranges: Option<&[IndexRange]>,
        zone: &SessionTimeZone,
    ) -> Result<Vec<(Key, Key)>, KvTableError> {
        let full_common_handle = [IndexRange {
            low: vec![Datum::MinNotNull],
            high: vec![Datum::MaxValue],
            low_exclusive: false,
            high_exclusive: false,
        }];
        let handle_ranges = if handle_ranges.is_none()
            && crate::handle_range::common_handle_primary(self).is_some()
        {
            Some(full_common_handle.as_slice())
        } else {
            handle_ranges
        };
        let encoded = match handle_ranges {
            Some(ranges) => crate::handle_range::record_key_ranges(self, ranges, zone)
                .map_err(|error| KvTableError::Encode(format!("{error:?}")))?,
            None => None,
        };
        Ok(encoded.unwrap_or_else(|| self.record_key_range()))
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
    /// A common-handle (clustered non-integer primary key) table is refused:
    /// the merge below addresses rows by their integer handle, so a handle
    /// this cursor cannot compare is a shape it must not claim to serve. So is
    /// a scan whose handle ranges cover no record at all, which a coprocessor
    /// request cannot express.
    #[allow(clippy::too_many_arguments)]
    pub fn pushdown_row_cursor_with_context(
        &mut self,
        keep: &[usize],
        predicates: &[ScanPredicate],
        output_offsets: Option<&[usize]>,
        topn: Option<&PushdownTopN>,
        limit: Option<u64>,
        handle_ranges: Option<&[IndexRange]>,
        context: &RowDecodeContext,
        statement: &PushdownStatementContext,
    ) -> Result<Option<RemoteRowCursor>, KvTableError> {
        let common_handle = !self.common_handle_offsets.is_empty();
        let common_primary = crate::handle_range::common_handle_primary(self);
        if common_handle && (self.has_dirty_content() || common_primary.is_none()) {
            return Ok(None);
        }
        // A pushdown request names ONE physical table id. A partitioned table
        // has several, so the request cannot describe it and the local scan
        // (which spans the whole partition block) serves the read instead.
        if self.partition.is_some() {
            return Ok(None);
        }
        let ranges = self.record_key_ranges(handle_ranges, context.zone())?;
        // No range at all is a read of NOTHING -- `id > 100 AND id < 100`, or a
        // bound that is NULL -- and a coprocessor request has no way to say
        // that: its `Ranges` list is what the transport turns into region
        // tasks, so an empty one is a malformed request rather than an empty
        // answer (`tidb_distsql`'s `metadata_region_ranges` rejects it as
        // `missing_ranges`). The local cursor states it exactly, by opening no
        // iterator, so the read goes there. Go plans a `TableDual` for the same
        // shape and sends no request either.
        if ranges.is_empty() {
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
            .into_iter()
            .flat_map(|index| index.column_offsets.iter())
            .filter_map(|offset| self.columns.get(*offset))
            .map(|column| column.id)
            .collect();
        let primary_prefix_column_ids: Vec<i64> = common_primary
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
            handle_index,
            primary_column_ids,
            primary_prefix_column_ids,
            predicates: predicates.to_vec(),
            output_offsets: output_offsets.map(<[usize]>::to_vec),
            topn: topn.cloned(),
            limit,
            aggregate: None,
            // The storage that owns the snapshot fills this in; the table has
            // no timestamp of its own.
            snapshot_ts: 0,
            ranges,
            statement: statement.clone(),
        };
        let Some(scan) = self.store.open_remote_scan(&request) else {
            return Ok(None);
        };
        let mut scan = scan.map_err(|e| KvTableError::Storage(format!("{e:?}")))?;
        if common_handle && !scan.staged.is_empty() {
            scan.stream.close();
            return Ok(None);
        }
        // One request reached a region. Counted here rather than at the
        // storage seam so a backend that REFUSED the shape (and returned an
        // `Unsupported` the caller turned into a byte-level cursor) is not
        // recorded as a coprocessor read.
        crate::storage::note_storage_op(|ops| ops.cop_scans += 1);
        let decoder = self.row_decoder_projected(Some(keep), context)?;
        let mut staged = Vec::with_capacity(scan.staged.len());
        for (key, value) in scan.staged {
            let row = match value {
                Some(value) => Some(decoder.decode_record(key.as_bytes(), &value)?.1),
                None => None,
            };
            staged.push((key.into_bytes(), row));
        }
        Ok(Some(RemoteRowCursor {
            stream: scan.stream,
            staged: staged.into_iter(),
            pending_staged: None,
            pending_remote: None,
            width: output_offsets.map_or(keep.len(), <[usize]>::len),
            handle_index,
            table_id: self.table_id,
            noted_rows: 0,
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
            &RowDecodeContext::legacy_default(zone),
            statement,
        )
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
        let ranges = self.record_key_ranges(handle_ranges, zone)?;
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
        let common_primary = crate::handle_range::common_handle_primary(self);
        let primary_column_ids = common_primary
            .into_iter()
            .flat_map(|index| index.column_offsets.iter())
            .filter_map(|offset| self.columns.get(*offset))
            .map(|column| column.id)
            .collect();
        let primary_prefix_column_ids = common_primary
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
            snapshot_ts: 0,
            ranges,
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
        aggregate: &PushdownPartialAggregate,
        zone: &SessionTimeZone,
        statement: &PushdownStatementContext,
    ) -> Result<Option<Box<dyn PushdownRowStream>>, KvTableError> {
        if self.has_dirty_content() || self.partition.is_some() {
            return Ok(None);
        }
        let Some(index) = self.indexes.iter().find(|index| index.id == index_id) else {
            return Ok(None);
        };
        if index.column_offsets.is_empty()
            || !matches!(aggregate, PushdownPartialAggregate::Count { .. })
        {
            return Ok(None);
        }
        let columns: Vec<PushdownScanColumn> = index
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
        if let PushdownPartialAggregate::Count { input_offset, .. } = &mut remote_aggregate {
            *input_offset = 0;
        }
        let request = PushdownScanRequest {
            table_id: self.table_id,
            index: Some(PushdownIndexScan {
                index_id,
                declared_unique: index.unique,
                index_column_count: index.column_offsets.len(),
            }),
            columns,
            handle_index: None,
            primary_column_ids: Vec::new(),
            primary_prefix_column_ids: Vec::new(),
            predicates: Vec::new(),
            output_offsets: None,
            topn: None,
            limit: None,
            aggregate: Some(remote_aggregate),
            snapshot_ts: 0,
            ranges: key_ranges,
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
        let mut cursor = self.row_cursor_with_decoder(decoder, None, context.zone())?;
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
        let mut cursor = self.row_cursor_with_decoder(decoder, None, context.zone())?;
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
        self.index_range_cursor_with_direction(index_id, range, zone, false)
    }

    /// A cursor over one index range in the physical scan direction Go put
    /// on `PhysicalIndexScan`.
    pub fn index_range_cursor_with_direction(
        &mut self,
        index_id: i64,
        range: &IndexRange,
        zone: &SessionTimeZone,
        descending: bool,
    ) -> Result<IndexRangeCursor, KvTableError> {
        self.index_ranges_cursor_for_physical_ids(
            index_id,
            std::slice::from_ref(range),
            zone,
            &self.record_physical_ids(),
            descending,
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
        self.index_ranges_cursor_for_physical_ids(index_id, ranges, zone, &physical_ids, false)
    }

    fn index_ranges_cursor_for_physical_ids(
        &mut self,
        index_id: i64,
        ranges: &[IndexRange],
        zone: &SessionTimeZone,
        physical_ids: &[i64],
        descending: bool,
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
        for physical_id in physical_ids.iter().copied() {
            for range in ranges {
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
        }
        let merge_by_index_key = physical_ids.len() > 1;
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
    /// The ranges left to read, in ascending key order. A whole-table scan
    /// holds exactly one; a `TableRangeScan` holds one per handle range (per
    /// partition, for a partitioned table).
    iterators: std::vec::IntoIter<Box<dyn StorageIterator>>,
    current: Option<Box<dyn StorageIterator>>,
    decoder: RowDecoder,
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
        loop {
            let Some(iterator) = self.current.as_mut() else {
                // Every range is opened when the cursor is, so advancing is
                // only ever moving to the next already-open one.
                self.current = self.iterators.next();
                if self.current.is_none() {
                    return Ok(None);
                }
                continue;
            };
            if !iterator.valid() {
                self.current.take().expect("just borrowed").close();
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
    }
}

impl Drop for RowCursor {
    /// An abandoned cursor (an early-stopping `LIMIT`) must still release
    /// every iterator, which a drained loop's explicit `close` would have
    /// done -- including the ranges it never reached.
    fn drop(&mut self) {
        if let Some(current) = self.current.as_mut() {
            current.close();
        }
        for mut iterator in self.iterators.by_ref() {
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
pub struct RemoteRowCursor {
    stream: Box<dyn PushdownRowStream>,
    staged: std::vec::IntoIter<StagedRow>,
    pending_staged: Option<StagedRow>,
    pending_remote: Option<KeyedRow>,
    /// Number of projected columns, which the remote row may exceed by the
    /// appended handle column.
    width: usize,
    /// Where the integer handle sits in a row to be merged. `None` is a
    /// clean common-handle stream consumed without a staged overlay.
    handle_index: Option<usize>,
    table_id: i64,
    /// How much of [`PushdownRowStream::rows_returned`] has already been
    /// reported to the storage probe, so each row is counted once. See
    /// [`note_wire_rows`].
    noted_rows: u64,
}

impl RemoteRowCursor {
    /// How many rows have crossed the network so far: the wire receipt.
    #[must_use]
    pub fn rows_returned(&self) -> u64 {
        self.stream.rows_returned()
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
                        match remote_key.as_slice().cmp(staged_key.as_slice()) {
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
}

impl Drop for RemoteRowCursor {
    /// An abandoned cursor (an early-stopping `LIMIT`) must still release the
    /// request, which a drained stream's explicit `close` would have done.
    fn drop(&mut self) {
        self.note_wire_rows();
        self.stream.close();
    }
}

/// A forward cursor over one index range, yielding row handles in index order.
///
/// See [`KvTable::index_range_cursor`].
pub struct IndexRangeCursor {
    iterators: Vec<Box<dyn StorageIterator>>,
    next_iterator: usize,
    merge_by_index_key: bool,
    merge_heap: IndexMergeHeap,
    index: KvIndex,
    common_handle: bool,
}

impl IndexRangeCursor {
    /// The next row handle in index order, or `None` at the end of the range.
    pub fn next_handle(&mut self) -> Result<Option<TableHandle>, KvTableError> {
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
            return Ok(Some(handle));
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
            return Ok(Some(handle));
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
    /// Locally computed partial rows when the backend refuses aggregation.
    partial_rows: Option<std::vec::IntoIter<Vec<Datum>>>,
    /// Whether the local partial result has already been fully emitted.
    partial_done: bool,
    /// The partial aggregation this source accepted from the planner.
    partial_aggregate: Option<PushdownPartialAggregate>,
    /// Access-path cardinality selected by the optimizer. Go chooses the
    /// partial/final split only above the one-row floor for these workloads.
    estimated_rows: Option<f64>,
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
            partial_rows: None,
            partial_done: false,
            partial_aggregate: None,
            estimated_rows: None,
            filter: None,
            pushed: Vec::new(),
            post_filter_projection: None,
            remote_topn: None,
            keep,
            scanned: std::rc::Rc::new(std::cell::Cell::new(0)),
            limit: None,
            emitted: 0,
            handle_ranges: None,
            decode_context,
            statement,
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

    /// The next row of whichever cursor is open, remote or local.
    fn next_source_row(&mut self) -> Result<Option<Vec<Datum>>, ExecError> {
        let next = match (self.remote.as_mut(), self.cursor.as_mut()) {
            (Some(remote), _) => remote.next_row(),
            (None, Some(cursor)) => cursor
                .next_row()
                .map(|row| row.map(|(_, projected)| projected)),
            (None, None) => return Ok(None),
        }
        .map_err(|_| ExecError::unsupported("table bytes failed to decode"))?;
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
                    if !matches!(row.get(*input_offset), None | Some(Datum::Null)) {
                        count += 1;
                    }
                }
                Ok(vec![vec![Datum::Int(count)]])
            }
            PushdownPartialAggregate::Sum { input_offset, .. } => {
                let mut sum: Option<Decimal> = None;
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
                    let addend = match value {
                        Datum::Null => continue,
                        Datum::Int(value) => Decimal::from_int(*value),
                        Datum::UInt(value) => Decimal::from_uint(*value),
                        Datum::Decimal(value) => value.clone(),
                        _ => {
                            return Err(ExecError::unsupported(
                                "partial SUM requires an integer or decimal input",
                            ));
                        }
                    };
                    sum = Some(match sum.take() {
                        Some(current) => current.add(&addend),
                        None => addend,
                    });
                }
                Ok(vec![vec![sum.map_or(Datum::Null, Datum::Decimal)]])
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
                let mut groups: BTreeMap<Vec<u8>, (Datum, Option<Decimal>)> = BTreeMap::new();
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
                    let addend = match value {
                        Datum::Null => None,
                        Datum::Int(value) => Some(Decimal::from_int(*value)),
                        Datum::UInt(value) => Some(Decimal::from_uint(*value)),
                        Datum::Decimal(value) => Some(value.clone()),
                        _ => {
                            return Err(ExecError::unsupported(
                                "partial SUM requires an integer or decimal input",
                            ));
                        }
                    };
                    let key = crate::hash_agg::group_key_part(&group_type.collation(), &group);
                    let (_, sum) = groups.entry(key).or_insert((group, None));
                    if let Some(addend) = addend {
                        *sum = Some(match sum.take() {
                            Some(current) => current.add(&addend),
                            None => addend,
                        });
                    }
                }
                Ok(groups
                    .into_values()
                    .map(|(group, sum)| vec![sum.map_or(Datum::Null, Datum::Decimal), group])
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

                enum PartialValue {
                    Count(i64),
                    Sum(Option<Decimal>),
                    Extreme { value: Option<Datum>, is_max: bool },
                }

                let new_values = || {
                    functions
                        .iter()
                        .map(|function| match function.kind {
                            PushdownAggregateKind::Count => PartialValue::Count(0),
                            PushdownAggregateKind::Sum => PartialValue::Sum(None),
                            PushdownAggregateKind::Min => PartialValue::Extreme {
                                value: None,
                                is_max: false,
                            },
                            PushdownAggregateKind::Max => PartialValue::Extreme {
                                value: None,
                                is_max: true,
                            },
                        })
                        .collect::<Vec<_>>()
                };
                let finish = |groups: Vec<Datum>, values: Vec<PartialValue>| {
                    values
                        .into_iter()
                        .map(|value| match value {
                            PartialValue::Count(count) => Datum::Int(count),
                            PartialValue::Sum(sum) => sum.map_or(Datum::Null, Datum::Decimal),
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
                            .input_offset
                            .map(|offset| {
                                row.get(offset).cloned().ok_or_else(|| {
                                    ExecError::unsupported(
                                        "partial aggregate input is outside the scan row",
                                    )
                                })
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
                            (PartialValue::Sum(sum), Some(input)) => {
                                let addend = match input {
                                    Datum::Int(value) => Decimal::from_int(value),
                                    Datum::UInt(value) => Decimal::from_uint(value),
                                    Datum::Decimal(value) => value,
                                    _ => {
                                        return Err(ExecError::unsupported(
                                            "partial SUM requires an integer or decimal input",
                                        ));
                                    }
                                };
                                *sum = Some(match sum.take() {
                                    Some(current) => current.add(&addend),
                                    None => addend,
                                });
                            }
                            (PartialValue::Extreme { value, is_max }, Some(candidate)) => {
                                let replace = value.as_ref().is_none_or(|current| {
                                    tidb_expr::compare_datums(&candidate, current).is_ok_and(
                                        |ordering| {
                                            if *is_max {
                                                ordering.is_gt()
                                            } else {
                                                ordering.is_lt()
                                            }
                                        },
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
                .map_err(|_| ExecError::unsupported("table bytes failed to decode"))?;
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
        self.remote = self
            .table
            .pushdown_row_cursor_with_context(
                &self.keep.clone(),
                &self.pushed,
                self.post_filter_projection.as_deref(),
                self.remote_topn.as_ref(),
                self.limit,
                self.handle_ranges.as_deref(),
                &self.decode_context,
                &self.statement,
            )
            .map_err(|_| ExecError::unsupported("table bytes failed to decode"))?;
        if self.remote.is_some() {
            return Ok(());
        }
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
                .row_cursor_projected_with_context(
                    projection,
                    handle_ranges.as_deref(),
                    &self.decode_context,
                )
                .map_err(|_| ExecError::unsupported("table bytes failed to decode"))?,
        );
        Ok(())
    }

    /// Pulls rows from the open cursor until the chunk is full, the pushed
    /// row cap is reached, or the range is exhausted.
    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        let cap = self.meta.max_chunk_size();
        if let Some(remote) = self.partial_remote.as_mut() {
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

impl crate::table_access::TableAccess for TableScanExec {
    fn accept_scan_estimate(&mut self, rows: f64) {
        self.estimated_rows = Some(rows);
    }

    fn accept_partial_aggregate(&mut self, aggregate: &PushdownPartialAggregate) -> bool {
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
