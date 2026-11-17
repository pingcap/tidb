//! Go `sql2kv.go`: `tableKVEncoder.Encode` and the encoded rows.

use tidb_datatype::Datum;
use tidb_lightning_backend_encode::{EncodingConfig, Encoder};
use tidb_lightning_verification::{KvChecksum, KvPair};

use crate::allocator::{AllocatorType, PanickingAllocators};
use crate::base::{datum_int64, BaseKVEncoder, EncodeError};

/// Go `Pairs`: one or more encoded rows' KV pairs.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Pairs {
    /// Go `Pairs.Pairs`.
    pub pairs: Vec<KvPair>,
}

impl Pairs {
    /// Go `MakeRowsFromKvPairs` / `MakeRowFromKvPairs`.
    #[must_use]
    pub fn from_kv_pairs(pairs: Vec<KvPair>) -> Self {
        Self { pairs }
    }
}

impl tidb_lightning_backend_encode::Rows for Pairs {
    /// Go `Pairs.Clear`.
    fn clear(&mut self) {
        self.pairs.clear();
    }
}

impl tidb_lightning_backend_encode::Row for Pairs {
    type Rows = Pairs;

    /// Go `Pairs.ClassifyAndAppend`: a record key (`t<id>_r`) is data,
    /// anything else an index entry.
    fn classify_and_append(
        self,
        data: &mut Pairs,
        data_checksum: &mut KvChecksum,
        indices: &mut Pairs,
        index_checksum: &mut KvChecksum,
    ) {
        for kv in self.pairs {
            if kv.key[tidb_tablecodec::table_key::TABLE_SPLIT_KEY_LEN + 1] == b'r' {
                data_checksum.update_one(&kv);
                data.pairs.push(kv);
            } else {
                index_checksum.update_one(&kv);
                indices.pairs.push(kv);
            }
        }
    }

    /// Go `Pairs.Size`.
    fn size(&self) -> u64 {
        self.pairs.iter().map(|kv| (kv.key.len() + kv.val.len()) as u64).sum()
    }
}

/// Go `tableKVEncoder`.
pub struct TableKVEncoder {
    base: BaseKVEncoder,
}

impl TableKVEncoder {
    /// Go `NewTableKVEncoder`; `allocators` are the table's
    /// (`NewPanickingAllocators` for a fresh import).
    pub fn new(config: &EncodingConfig, allocators: PanickingAllocators) -> Result<Self, EncodeError> {
        Ok(Self { base: BaseKVEncoder::new(config, allocators)? })
    }

    /// The embedded `BaseKVEncoder`.
    #[must_use]
    pub fn base(&self) -> &BaseKVEncoder {
        &self.base
    }

    /// Go `GetEncoderIncrementalID`.
    pub fn incremental_id(&mut self, id: i64) -> i64 {
        self.base.auto_id_fn.apply(id)
    }

    /// Go `GetActualDatum`.
    pub fn actual_datum(&mut self, column: usize, row_id: i64, input: Option<&Datum>) -> Result<Datum, EncodeError> {
        let col = self.base.columns[column].clone();
        self.base.get_actual_datum(&col, row_id, input, true)
    }

    fn encode_row(&mut self, row: &[Datum], row_id: i64, perm: &[isize]) -> Result<Pairs, EncodeError> {
        let base = &mut self.base;
        let mut record = Vec::with_capacity(base.columns.len() + 1);
        let columns = base.columns.clone();
        for (i, col) in columns.iter().enumerate() {
            let j = perm[i];
            let datum = usize::try_from(j).ok().and_then(|j| row.get(j));
            match base.process_col_datum(col, row_id, datum, true) {
                Ok(value) => record.push(value),
                Err(error) => return Err(base.log_kv_convert_failed(row, j, &col.read(), error)),
            }
        }
        let meta = base.table.meta();
        let mut record_id = None;
        if tidb_lightning_common::table_has_auto_row_id(&meta.read()) {
            let j = perm[columns.len()];
            let (value, row_value) = match usize::try_from(j).ok().and_then(|j| row.get(j)) {
                Some(input) => {
                    let extra = tidb_model::ColumnInfo::new_extra_handle_col_info();
                    let value = base
                        .cast(input.clone(), &extra)
                        .map_err(|error| base.log_kv_convert_failed(row, j, &extra, error))?;
                    let row_value = datum_int64(&value);
                    (value, row_value)
                }
                None => (Datum::Int(base.auto_id_fn.apply(row_id)), row_id),
            };
            record_id = Some(datum_int64(&value));
            record.push(value);
            base.allocators.get(AllocatorType::RowId).rebase(row_value);
        }
        if base.has_generated {
            base.eval_generated_columns(&mut record)
                .map_err(|(column, error)| base.log_eval_gen_expr_failed(&column, error))?;
        }
        let pairs = base.record_to_kv(&record, record_id, row_id)?;
        Ok(Pairs { pairs })
    }
}

impl Encoder for TableKVEncoder {
    type Row = Pairs;
    type Error = EncodeError;

    /// Go `tableKVEncoder.Encode`: each column's value from its permuted
    /// input (or its auto id, default or NULL handling), the hidden row id,
    /// the generated columns, then the table layer's KV pairs. Warnings are
    /// dropped afterwards, as Go truncates them.
    fn encode(&mut self, row: &[Datum], row_id: i64, column_permutation: &[isize], _offset: i64) -> Result<Pairs, EncodeError> {
        let result = self.encode_row(row, row_id, column_permutation);
        self.base.truncate_warns();
        result
    }
}
