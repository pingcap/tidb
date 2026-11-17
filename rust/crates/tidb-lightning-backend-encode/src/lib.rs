//! Go `pkg/lightning/backend/encode`: what an encoder is configured with
//! and what it produces.

use std::collections::BTreeMap;
use tidb_datatype::Datum;
use tidb_lightning_verification::KvChecksum;

/// Go `SessionOptions`: the session an encoder evaluates rows under.
#[derive(Clone, Debug, Default)]
pub struct SessionOptions {
    /// Go `SQLMode`.
    pub sql_mode: i64,
    /// Go `Timestamp`: the clock's fixed value in seconds, or 0 for the
    /// wall clock.
    pub timestamp: i64,
    /// Go `SysVars`.
    pub sys_vars: BTreeMap<String, String>,
    /// Go `LogicalImportPrepStmt`, read by the TiDB (logical) backend.
    pub logical_import_prep_stmt: bool,
    /// Go `AutoRandomSeed`: seeds the AUTO_RANDOM and SHARD_ROW_ID_BITS
    /// shards.
    pub auto_random_seed: i64,
    /// Go `IndexID`: set when only one index is encoded (ADD INDEX).
    pub index_id: i64,
    /// Go `MinCommitTS`.
    pub min_commit_ts: u64,
}

/// Go `EncodingConfig`. Go carries the `table.Table`; this carries the
/// stored `TableInfo` the encoder opens its table from.
#[derive(Clone, Debug)]
pub struct EncodingConfig {
    /// Go's embedded `SessionOptions`.
    pub session_options: SessionOptions,
    /// Go `Path`: the data file the rows come from.
    pub path: String,
    /// Go `Table.Meta()`.
    pub table: tidb_model::TableInfo,
    /// Go `UseIdentityAutoRowID`: keep the given row id for AUTO_RANDOM
    /// and shard row ids instead of composing a shard into it.
    pub use_identity_auto_row_id: bool,
}

/// Go `Encoder`.
pub trait Encoder {
    /// The encoded row.
    type Row: Row;
    /// The error an encoding reports.
    type Error;

    /// Go `Encoder.Close`.
    fn close(&mut self) {}

    /// Go `Encoder.Encode`: `column_permutation[i]` is the input position of
    /// the table's column `i` (and, last, of `_tidb_rowid`), or negative
    /// when the input does not provide it. `offset` is the row's position in
    /// its file.
    fn encode(
        &mut self,
        row: &[Datum],
        row_id: i64,
        column_permutation: &[isize],
        offset: i64,
    ) -> Result<Self::Row, Self::Error>;
}

/// Go `Rows`: encoded rows ready for the backend.
pub trait Rows {
    /// Go `Rows.Clear`: drops the rows, keeping the allocation.
    fn clear(&mut self);
}

/// Go `Row`: one encoded row.
pub trait Row {
    /// The rows this row is appended to.
    type Rows: Rows;

    /// Go `Row.ClassifyAndAppend`: data (record) KVs to `data` and the
    /// index KVs to `indices`, each counted in its checksum.
    fn classify_and_append(
        self,
        data: &mut Self::Rows,
        data_checksum: &mut KvChecksum,
        indices: &mut Self::Rows,
        index_checksum: &mut KvChecksum,
    );

    /// Go `Row.Size`: the bytes of every key and value.
    fn size(&self) -> u64;
}
