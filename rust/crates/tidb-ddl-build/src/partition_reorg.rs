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

//! Go `pkg/ddl/partition.go`'s row work over a partitioned table:
//! `reorgPartitionWorker.BackfillData` copies a batch of a reorganized
//! partition's rows to the partitions the new scheme routes them to, and
//! `checkExchangePartitionRecordValidation` tests a row of the table being
//! exchanged in against the partition's condition.

use tidb_datatype::Datum;
use tidb_table::kv_table::{KvTable, KvTableError, RowDecodeContext, TableHandle};
use tidb_table::partition_routing::PartitionKind;
use tidb_txnkv::Key;

use crate::backfilling::ReorgBatch;

/// The partition reorganization's row work over a [`KvTable`].
pub trait KvTablePartitionReorg {
    /// Go `reorgPartitionWorker.BackfillData` for one batch of the
    /// reorganized partition `physical_id`'s rows in `[start, end)`: each
    /// stored record is copied, byte for byte, to the partition the new
    /// scheme routes it to, under the same handle. A heap table's copy that
    /// is already there with the same bytes (double written by DML since)
    /// is skipped; one holding another row (two partitions shared a
    /// `_tidb_rowid` after an EXCHANGE PARTITION) moves the source row to a
    /// fresh handle first, in its own partition, so the double writes keep
    /// finding it. Every source record not rewritten is locked.
    fn reorg_partition_batch(
        &mut self,
        physical_id: i64,
        start: &[u8],
        end: &[u8],
        batch_count: usize,
        ctx: &dyn tidb_table::context::MutateContext,
    ) -> Result<ReorgBatch, KvTableError>;

    /// Go `checkExchangePartitionRecordValidation`'s partition condition for
    /// one row of the table being exchanged in: whether the row does NOT
    /// belong to the partition at `ordinal`.
    ///
    /// HASH is Go's literal `mod(expr, num) != index`, which refuses a
    /// negative value that routing would place by its absolute value, and a
    /// NULL anywhere but the first partition. The other methods compare the
    /// row's route, and a row no partition accepts does not match.
    fn exchange_row_mismatches(
        &self,
        ordinal: usize,
        row: &[Datum],
        ctx: &dyn tidb_expr::Columns,
    ) -> Result<bool, KvTableError>;
}

impl KvTablePartitionReorg for KvTable {
    fn reorg_partition_batch(
        &mut self,
        physical_id: i64,
        start: &[u8],
        end: &[u8],
        batch_count: usize,
        ctx: &dyn tidb_table::context::MutateContext,
    ) -> Result<ReorgBatch, KvTableError> {
        let decode_context = RowDecodeContext::for_write(ctx);
        let decoder = crate::table_backfill::reorg_row_decoder(self, &decode_context)?;
        // Go `fetchRowColVals`: the records with their raw values.
        let mut records: Vec<(Vec<u8>, TableHandle, Vec<Datum>, Vec<u8>)> = Vec::new();
        let mut task_done = false;
        {
            let mut iter = self.store.iter(
                Some(&Key::from_bytes(start.to_vec())),
                Some(&Key::from_bytes(end.to_vec())),
            )?;
            loop {
                if !iter.valid() {
                    task_done = true;
                    break;
                }
                if records.len() >= batch_count {
                    break;
                }
                let key = iter.key().as_bytes().to_vec();
                if key.as_slice() >= end {
                    task_done = true;
                    break;
                }
                let value = iter.value().to_vec();
                let (handle, row) = decoder.decode_record(&key, &value)?;
                records.push((key, handle, row, value));
                iter.next()?;
            }
            iter.close();
        }
        if records.is_empty() {
            task_done = true;
        }
        let next_key = if task_done {
            end.to_vec()
        } else {
            let mut next = records.last().expect("a full batch has rows").0.clone();
            next.push(0);
            next
        };
        let mut batch = ReorgBatch {
            next_key,
            done: task_done,
            ..ReorgBatch::default()
        };
        let zone = ctx.session_zone();
        let clustered = self.clustered();
        for (source_key, handle, row, value) in records {
            batch.scanned += 1;
            let target = self.locate_reorg_partition(&row, ctx)?.ok_or_else(|| {
                KvTableError::Decode("the table is not being reorganized".to_owned())
            })?;
            let mut handle = handle;
            let mut rewritten = false;
            if !clustered {
                if let Some(copy) = self.partition_copy(target, &handle)? {
                    if copy == value {
                        // Already copied, or double written by DML.
                        batch.locks.push(source_key);
                        continue;
                    }
                    // Go `RemoveRecord` + `AddRecord` in the source
                    // partition, which gives the row a fresh handle.
                    self.remove_partition_copy(physical_id, &row, &handle, &zone)?;
                    let shard = if self.shard_row_id_bits() > 0 {
                        ctx.next_row_id_shard(1) as i64
                    } else {
                        0
                    };
                    handle = self.handle_of_row(&row, &zone, shard)?;
                    self.add_partition_copy(
                        physical_id,
                        &row,
                        &handle,
                        &zone,
                        false,
                        false,
                        false,
                    )?;
                    rewritten = true;
                }
            }
            if !rewritten {
                batch.locks.push(source_key);
            }
            // The copy's indexes are built by the next phase, or by any DML
            // that touches the row.
            let key = Key::from_bytes(tidb_codec::table_key::encode_row_key_with_handle(
                target,
                &handle.record_handle(),
            ));
            self.store.set(key, value).map_err(KvTableError::from)?;
            batch.added += 1;
        }
        Ok(batch)
    }

    fn exchange_row_mismatches(
        &self,
        ordinal: usize,
        row: &[Datum],
        ctx: &dyn tidb_expr::Columns,
    ) -> Result<bool, KvTableError> {
        let partition = self.partition.as_ref().expect("validated by DDL");
        if let PartitionKind::Hash = partition.kind {
            let num = partition.num();
            if num == 1 {
                return Ok(false);
            }
            let value = tidb_table::generated_column::eval_over_dependencies(
                &partition.expr,
                &partition.dependencies,
                &*self.columns,
                row,
                ctx,
            )
            .map_err(|error| KvTableError::Decode(format!("{error:?}")))?;
            let remainder = match value {
                Datum::Null => return Ok(ordinal != 0),
                Datum::UInt(value) => (value % num) as i64,
                Datum::Int(value) => value % num as i64,
                other => {
                    let index = tidb_table::partition_routing::hash_partition_index(&other, num)
                        .map_err(|error| KvTableError::Decode(format!("{error:?}")))?;
                    index as i64
                }
            };
            return Ok(remainder != ordinal as i64);
        }
        match self.record_physical_id(row, ctx) {
            Ok(physical_id) => Ok(physical_id != partition.definitions[ordinal].id),
            Err(KvTableError::Mysql(error))
                if error.equal(&tidb_error::catalog::table::ERR_NO_PARTITION_FOR_GIVEN_VALUE) =>
            {
                Ok(true)
            }
            Err(error) => Err(error),
        }
    }
}
