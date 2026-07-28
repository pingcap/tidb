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

//! Bound configured writes lowered onto the real optimistic transaction.
//!
//! This owns the two executor contracts Go keeps in `pkg/executor/insert.go`
//! and `pkg/executor/update.go`: which mutations a statement publishes, and how
//! many rows it reports as affected. The transaction itself stays in
//! `tidb-txnkv`; the in-memory `Database` DML path is a separate owner and is
//! never called from here.
//!
//! Affected rows follow the source exactly:
//!
//! * INSERT counts one row per successfully added record
//!   (`pkg/executor/insert_common.go` `addRecordWithAutoIDHint` calls
//!   `AddAffectedRows(1)` only after `AddRecord` returns).
//! * UPDATE counts a row only when the new value differs from the old one
//!   (`pkg/executor/write.go`: an unchanged row takes `AddTouchedRows(1)` and
//!   adds an affected row only under `ClientFoundRows`, which this bounded path
//!   does not negotiate).
//! * A missing UPDATE row matches nothing and publishes nothing.

use std::fmt;
use std::time::Duration;

use tidb_codec::table_key::{
    encode_non_unique_index_key, encode_row_key_with_handle, non_unique_index_value, RecordHandle,
};
use tidb_datatype::{
    parse_duration, parse_time, produce_char_value, Datum, Decimal, MySqlDuration, TimeType,
};
use tidb_planner::{
    prepared_dml::{
        lower_prepared_write, lower_text_write, ConfiguredAssignment, ConfiguredInsertRow,
        ConfiguredPreparedWrite, ConfiguredPreparedWriteTemplate, PreparedBindValue,
        PreparedWritePlanError,
    },
    read_only_scan::{
        configured_catalog::ConfiguredCatalog, ConfiguredColumn, ConfiguredColumnKind,
        ConfiguredIndex, ConfiguredScalarType, ConfiguredTable,
    },
};
use tidb_tablecodec::{decode_table_row_to_map, encode_table_row, TableRowError};
use tidb_txnkv::{
    lock::{LockRecoveryClient, TimestampSource},
    region::RegionRecoveryLoader,
    rpc::UnaryCallContext,
    transaction::{
        MutationSetError, OptimisticCommitOutcome, OptimisticCoordinatorError, OptimisticMutation,
        ReadOnlyTransaction, RealOptimisticTransaction, RealOptimisticTransactionOpener,
        TransactionCommandClient,
    },
};

/// Why a bound configured write cannot produce mutations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConfiguredWriteError {
    /// The row could not be encoded. Carries the `tidb_tablecodec::TableRowError`
    /// display message (that type has no `Eq`/`Clone`, so it is rendered here
    /// rather than wrapped).
    RowWrite(String),
    /// The stored value read at `start_ts` could not be decoded, or the row
    /// carries no entry for a requested column.
    RowRead(String),
    /// Signed `BIGINT` addition left the domain, exactly as Go's
    /// `types.ErrOverflow` "BIGINT value is out of range" case.
    Overflow {
        /// Configured column being assigned.
        column: String,
        /// Value stored at the transaction's start timestamp.
        current: i64,
        /// Bound addend.
        addend: i64,
    },
    /// A bound or computed value is outside the column's signed integer domain,
    /// exactly as Go's `ConvertIntToInt` rejects an out-of-range store with
    /// `types.ErrOverflow`.
    ValueOutOfRange {
        /// Configured column that rejected the value.
        column: String,
        /// The offending signed value.
        value: i64,
        /// The column's integer domain.
        scalar_type: ConfiguredScalarType,
    },
    /// One statement repeats a clustered handle, so its mutations would
    /// collide inside a single transaction.
    DuplicateHandle(i64),
    /// The transaction rejected the assembled mutation set.
    Mutations(MutationSetError),
    /// The real transaction coordinator failed before or during publication.
    Transaction(OptimisticCoordinatorError),
    /// The statement is not admitted SQL for the configured write boundary.
    Plan(PreparedWritePlanError),
    /// The prepared statement text did not parse.
    Parse(String),
    /// Publication finished in a terminal state that is not a commit, so no
    /// affected-row count may be reported.
    NotCommitted(String),
    /// A bound parameter's kind (integer vs string) did not match its target
    /// column's type — a string into an integer column or the reverse.
    ColumnTypeMismatch {
        /// Configured column that rejected the value.
        column: String,
        /// The column's declared scalar type.
        scalar_type: ConfiguredScalarType,
        /// Whether the supplied value was string bytes (`true`) or an integer.
        value_is_bytes: bool,
    },
    /// A configured index shape the write path does not yet maintain (a unique
    /// index, or an index over a non-integer column). Failing closed keeps the
    /// index from silently drifting out of sync with the row data.
    UnsupportedIndex {
        /// Why the index could not be maintained.
        reason: &'static str,
    },
    /// A bound string exceeds its `CHAR(N)` column's character length in strict
    /// `sql_mode`, matching Go `types.ErrDataTooLong`.
    DataTooLong {
        /// Configured column that rejected the value.
        column: String,
        /// The column's declared character length.
        max_length: usize,
        /// The value's actual character length.
        char_length: usize,
    },
    /// A bound value is outside `BIGINT UNSIGNED`'s domain (negative, or
    /// larger than `u64::MAX`), matching Go `types.ErrOverflow` /
    /// `[types:1264]Out of range value for column`.
    UnsignedOutOfRange {
        /// Configured column that rejected the value.
        column: String,
    },
    /// A bound decimal string does not fit `DECIMAL(precision, scale)` after
    /// rounding to `scale`, matching Go `types.ErrOverflow` /
    /// `[types:1264]Out of range value for column`.
    DecimalOutOfRange {
        /// Configured column that rejected the value.
        column: String,
    },
    /// A bound temporal string is not a valid literal for its column's type,
    /// matching Go `types.ErrWrongValue` / `[table:1292]Incorrect ... value`.
    InvalidTemporal {
        /// Configured column that rejected the value.
        column: String,
        /// The underlying parse failure.
        message: String,
    },
    /// `NULL` was bound to a `NOT NULL` configured column, matching Go
    /// `[table:1048]Column '<name>' cannot be null`.
    NullNotAllowed {
        /// Configured column that rejected `NULL`.
        column: String,
    },
}

impl fmt::Display for ConfiguredWriteError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RowWrite(error) => write!(formatter, "configured write encoding: {error}"),
            Self::RowRead(error) => write!(formatter, "configured write snapshot read: {error}"),
            Self::Overflow {
                column,
                current,
                addend,
            } => write!(
                formatter,
                "BIGINT value is out of range in '({column} + {addend})' at stored value {current}"
            ),
            Self::ValueOutOfRange {
                column,
                value,
                scalar_type,
            } => {
                // Only integer columns produce this error (the range check is a
                // no-op for others), but the name stays exhaustive.
                let type_name = match scalar_type {
                    ConfiguredScalarType::BigInt => "BIGINT",
                    ConfiguredScalarType::UnsignedBigInt => "BIGINT UNSIGNED",
                    ConfiguredScalarType::Int => "INT",
                    ConfiguredScalarType::Double => "DOUBLE",
                    ConfiguredScalarType::Char { .. } => "CHAR",
                    ConfiguredScalarType::Varchar { .. } => "VARCHAR",
                    ConfiguredScalarType::Decimal { .. } => "DECIMAL",
                    ConfiguredScalarType::Date => "DATE",
                    ConfiguredScalarType::Datetime { .. } => "DATETIME",
                    ConfiguredScalarType::Timestamp { .. } => "TIMESTAMP",
                    ConfiguredScalarType::Duration { .. } => "TIME",
                };
                write!(
                    formatter,
                    "{type_name} value {value} is out of range in column '{column}'"
                )
            }
            Self::DuplicateHandle(handle) => write!(
                formatter,
                "configured write repeats clustered handle {handle}"
            ),
            Self::Mutations(error) => write!(formatter, "configured write mutations: {error}"),
            Self::Transaction(error) => write!(formatter, "configured write transaction: {error}"),
            Self::Plan(error) => write!(formatter, "{error}"),
            Self::Parse(message) => write!(formatter, "SQL parse error: {message}"),
            Self::NotCommitted(state) => {
                write!(formatter, "configured write did not commit: {state}")
            }
            Self::ColumnTypeMismatch {
                column,
                scalar_type,
                value_is_bytes,
            } => {
                let supplied = if *value_is_bytes { "string" } else { "integer" };
                let type_name = match scalar_type {
                    ConfiguredScalarType::BigInt => "BIGINT",
                    ConfiguredScalarType::UnsignedBigInt => "BIGINT UNSIGNED",
                    ConfiguredScalarType::Int => "INT",
                    ConfiguredScalarType::Double => "DOUBLE",
                    ConfiguredScalarType::Char { .. } => "CHAR",
                    ConfiguredScalarType::Varchar { .. } => "VARCHAR",
                    ConfiguredScalarType::Decimal { .. } => "DECIMAL",
                    ConfiguredScalarType::Date => "DATE",
                    ConfiguredScalarType::Datetime { .. } => "DATETIME",
                    ConfiguredScalarType::Timestamp { .. } => "TIMESTAMP",
                    ConfiguredScalarType::Duration { .. } => "TIME",
                };
                write!(
                    formatter,
                    "{supplied} value does not match {type_name} column '{column}'"
                )
            }
            Self::UnsupportedIndex { reason } => {
                write!(formatter, "configured index is not maintained: {reason}")
            }
            Self::DataTooLong {
                column,
                max_length,
                char_length,
            } => write!(
                formatter,
                "Data too long for column '{column}' (field len {max_length}, data len {char_length})"
            ),
            Self::UnsignedOutOfRange { column } => write!(
                formatter,
                "Out of range value for column '{column}'"
            ),
            Self::DecimalOutOfRange { column } => write!(
                formatter,
                "Out of range value for column '{column}'"
            ),
            Self::InvalidTemporal { column, message } => write!(
                formatter,
                "Incorrect value: '{message}' for column '{column}'"
            ),
            Self::NullNotAllowed { column } => {
                write!(formatter, "Column '{column}' cannot be null")
            }
        }
    }
}

impl std::error::Error for ConfiguredWriteError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Mutations(error) => Some(error),
            Self::Transaction(error) => Some(error),
            Self::Plan(error) => Some(error),
            Self::RowWrite(_)
            | Self::RowRead(_)
            | Self::Overflow { .. }
            | Self::ValueOutOfRange { .. }
            | Self::DuplicateHandle(_)
            | Self::Parse(_)
            | Self::NotCommitted(_)
            | Self::ColumnTypeMismatch { .. }
            | Self::UnsupportedIndex { .. }
            | Self::DataTooLong { .. }
            | Self::UnsignedOutOfRange { .. }
            | Self::DecimalOutOfRange { .. }
            | Self::InvalidTemporal { .. }
            | Self::NullNotAllowed { .. } => None,
        }
    }
}

impl From<TableRowError> for ConfiguredWriteError {
    fn from(error: TableRowError) -> Self {
        // `TableRowError` carries no `Display` impl of its own beyond `Debug`
        // (it is a leaf codec error), so its debug rendering is the message.
        Self::RowWrite(format!("{error:?}"))
    }
}

impl From<MutationSetError> for ConfiguredWriteError {
    fn from(error: MutationSetError) -> Self {
        Self::Mutations(error)
    }
}

impl From<OptimisticCoordinatorError> for ConfiguredWriteError {
    fn from(error: OptimisticCoordinatorError) -> Self {
        Self::Transaction(error)
    }
}

/// What a bound statement publishes and reports.
///
/// `NoWrite` is not an error: a point UPDATE whose row is missing or already
/// holds the assigned value publishes nothing and reports zero affected rows.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConfiguredWritePlan {
    /// Publish these mutations and report `affected_rows` after a determinate
    /// commit.
    Write {
        /// Mutations in statement order; the transaction sorts and batches.
        mutations: Vec<OptimisticMutation>,
        /// Rows to report in the MySQL OK packet.
        affected_rows: u64,
    },
    /// Publish nothing and report zero affected rows.
    NoWrite {
        /// Why nothing is published, for the receipt.
        reason: NoWriteReason,
    },
}

/// Why a bound statement publishes no mutation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NoWriteReason {
    /// No row exists at the clustered handle at `start_ts`.
    MissingRow,
    /// The stored value already equals the assigned value.
    UnchangedRow,
}

/// Encodes one INSERT statement's rows into typed mutations.
///
/// TiKV enforces absence through the `Insert` operation's `NotExist`
/// assertion, so this performs no preliminary existence read.
pub fn plan_insert(
    table: &ConfiguredTable,
    rows: &[ConfiguredInsertRow],
) -> Result<ConfiguredWritePlan, ConfiguredWriteError> {
    let mut mutations = Vec::with_capacity(rows.len());
    let mut handles = Vec::with_capacity(rows.len());
    for row in rows {
        let (handle, columns) = split_row(table, row)?;
        if handles.contains(&handle) {
            return Err(ConfiguredWriteError::DuplicateHandle(handle));
        }
        handles.push(handle);
        let key = encode_row_key_with_handle(table.table_id(), &RecordHandle::Int(handle));
        let value = encode_row_value(&columns)?;
        mutations.push(OptimisticMutation::insert(key, value)?);
        // Every configured index gains one entry for the new row, committed in
        // the same 2PC as the record so the index can never lag the row.
        for index in table.indexes() {
            let indexed = indexed_insert_value(index, &columns)?;
            mutations.push(index_put_mutation(
                table.table_id(),
                index,
                indexed,
                handle,
            )?);
        }
    }
    // One affected row per record, independent of how many index entries each
    // record also wrote.
    let affected_rows = handles.len() as u64;
    Ok(ConfiguredWritePlan::Write {
        mutations,
        affected_rows,
    })
}

/// Decides one point UPDATE from the row observed at the transaction's own
/// start timestamp.
///
/// `stored` is exactly what TiKV returned for the record key: `None` is
/// `not_found`.
pub fn plan_update(
    table: &ConfiguredTable,
    handle: i64,
    column_index: usize,
    assignment: ConfiguredAssignment,
    stored: Option<&[u8]>,
) -> Result<ConfiguredWritePlan, ConfiguredWriteError> {
    let Some(stored) = stored else {
        return Ok(ConfiguredWritePlan::NoWrite {
            reason: NoWriteReason::MissingRow,
        });
    };
    let assigned = &table.columns()[column_index];
    // Decode the whole stored row once: the unchanged-row check, the moved
    // index entry, and every carried-forward column all read from it.
    let stored_row = decode_stored_row(table, stored)?;
    let stored_value = stored_row.get(&assigned.id()).cloned().ok_or_else(|| {
        ConfiguredWriteError::RowRead(format!(
            "configured row is missing column ID {}",
            assigned.id()
        ))
    })?;

    // Resolve the assigned column's new value by assignment kind. An integer
    // assignment applies the column's range (the i64 arithmetic runs first, then
    // storing the result applies the column's own domain, so an INT that leaves
    // the i32 range is an overflow even when the i64 addition did not wrap); a
    // CHAR assignment carries raw bytes with no range check.
    let new_value = match assignment {
        ConfiguredAssignment::Set(value) => {
            check_column_value(assigned, value)?;
            Datum::Int(value)
        }
        ConfiguredAssignment::Add(addend) => {
            let current = stored_value.as_int().ok_or_else(|| {
                ConfiguredWriteError::RowRead(format!(
                    "configured column ID {} is not a signed integer",
                    assigned.id()
                ))
            })?;
            let replacement =
                current
                    .checked_add(addend)
                    .ok_or_else(|| ConfiguredWriteError::Overflow {
                        column: assigned.name().to_owned(),
                        current,
                        addend,
                    })?;
            check_column_value(assigned, replacement)?;
            Datum::Int(replacement)
        }
        ConfiguredAssignment::SetBytes(bytes) => {
            // A bytes assignment targets a CHAR column (the lowering resolves the
            // shape from the column type); enforce its character-length limit.
            let ConfiguredScalarType::Char { max_length } = assigned.scalar_type() else {
                return Err(ConfiguredWriteError::ColumnTypeMismatch {
                    column: assigned.name().to_owned(),
                    scalar_type: assigned.scalar_type(),
                    value_is_bytes: true,
                });
            };
            Datum::new_collation_string(
                admit_char_value(assigned, bytes, max_length)?,
                tidb_datatype::Collation::Utf8Mb4Bin,
            )
        }
    };
    // An UPDATE whose value does not change writes nothing (Go `AddTouchedRows`
    // with no affected row on this non-`ClientFoundRows` path).
    if new_value == stored_value {
        return Ok(ConfiguredWritePlan::NoWrite {
            reason: NoWriteReason::UnchangedRow,
        });
    }

    // The replacement row is complete, not a partial overwrite: every other
    // stored column keeps the value observed at `start_ts`, decoded at its OWN
    // type so a CHAR column survives the rewrite as its raw bytes rather than
    // being misread as an integer. Columns are ordered by id before encoding,
    // exactly as the INSERT path emits them.
    let mut columns: Vec<(i64, Datum)> = Vec::new();
    for column in table.columns() {
        if column.kind() == ConfiguredColumnKind::ClusteredPrimaryKey {
            continue;
        }
        let value = if column.id() == assigned.id() {
            new_value.clone()
        } else {
            stored_row.get(&column.id()).cloned().ok_or_else(|| {
                ConfiguredWriteError::RowRead(format!(
                    "configured row is missing column ID {}",
                    column.id()
                ))
            })?
        };
        columns.push((column.id(), value));
    }
    columns.sort_by_key(|(id, _)| *id);
    let key = encode_row_key_with_handle(table.table_id(), &RecordHandle::Int(handle));
    let value = encode_row_value(&columns)?;
    let mut mutations = vec![OptimisticMutation::put_existing(key, value)?];
    // A single-column update moves only the entry of an index on that column;
    // any other index is untouched because its column value did not change. The
    // index path maintains integer columns only, so a moved entry over a CHAR
    // column, or a NULL value, fails closed rather than writing a malformed key.
    for index in table.indexes() {
        if index.is_unique() {
            return Err(ConfiguredWriteError::UnsupportedIndex {
                reason: "unique index",
            });
        }
        if index.column_id() == assigned.id() {
            match (&stored_value, &new_value) {
                (Datum::Int(old), Datum::Int(new)) => {
                    mutations.push(index_delete_mutation(
                        table.table_id(),
                        index,
                        *old,
                        handle,
                    )?);
                    mutations.push(index_put_mutation(table.table_id(), index, *new, handle)?);
                }
                (Datum::Null, _) | (_, Datum::Null) => {
                    return Err(ConfiguredWriteError::UnsupportedIndex {
                        reason: "index over a nullable column",
                    })
                }
                _ => {
                    return Err(ConfiguredWriteError::UnsupportedIndex {
                        reason: "index over a non-integer column",
                    })
                }
            }
        }
    }
    Ok(ConfiguredWritePlan::Write {
        mutations,
        affected_rows: 1,
    })
}

/// Plans a point DELETE. If the clustered handle's row exists at `start_ts`, one
/// delete mutation removes its record key; otherwise no write is published.
/// Mirrors Go `pkg/executor/delete.go`'s single-table point delete, which
/// removes the record key via `tables.RemoveRecord` (`Op_Del` + `AssertExist`).
pub fn plan_delete(
    table: &ConfiguredTable,
    handle: i64,
    stored: Option<&[u8]>,
) -> Result<ConfiguredWritePlan, ConfiguredWriteError> {
    let Some(stored) = stored else {
        return Ok(ConfiguredWritePlan::NoWrite {
            reason: NoWriteReason::MissingRow,
        });
    };
    let key = encode_row_key_with_handle(table.table_id(), &RecordHandle::Int(handle));
    let mut mutations = vec![OptimisticMutation::delete(key)?];
    // The removed row's index entries go with it, keyed by the values it stored
    // at `start_ts`.
    if !table.indexes().is_empty() {
        let stored_row = decode_stored_row(table, stored)?;
        for index in table.indexes() {
            let indexed = indexed_stored_value(index, &stored_row)?;
            mutations.push(index_delete_mutation(
                table.table_id(),
                index,
                indexed,
                handle,
            )?);
        }
    }
    Ok(ConfiguredWritePlan::Write {
        mutations,
        affected_rows: 1,
    })
}

/// A generous upper bound on one integer index entry's key + value bytes, used
/// only to size the transaction's pre-open publication budget (an over-estimate
/// is safe; the commit only rejects an *under*-provisioned mutation set).
const MAX_INT_INDEX_ENTRY_BYTES: usize = 64;

/// Admits a bound string into a `CHAR(max_length)` column, enforcing the
/// character-length limit (Go `types.ProduceStrWithSpecifiedTp`): a value within
/// the limit passes unchanged, an over-length value whose overflow is only
/// trailing whitespace is truncated, and anything longer is `DataTooLong`.
fn admit_char_value(
    column: &ConfiguredColumn,
    value: Vec<u8>,
    max_length: u32,
) -> Result<Vec<u8>, ConfiguredWriteError> {
    produce_char_value(&value, max_length as usize).map_err(|error| {
        ConfiguredWriteError::DataTooLong {
            column: column.name().to_owned(),
            max_length: error.flen,
            char_length: error.char_len,
        }
    })
}

/// Admits a bound string into a `VARCHAR(max_length)` column.
///
/// A `binary`-collation column (`VARCHAR ... CHARACTER SET binary`/
/// `VARBINARY`) counts length in bytes with no character-boundary or
/// whitespace-trim exception, matching Go's byte-length check for a binary
/// string. A `utf8mb4` column shares the exact same length/truncation rule a
/// `CHAR` column uses (`admit_char_value`); MySQL/TiDB's over-length
/// whitespace exception applies identically to both.
fn admit_varchar_value(
    column: &ConfiguredColumn,
    value: Vec<u8>,
    max_length: u32,
    binary: bool,
) -> Result<Vec<u8>, ConfiguredWriteError> {
    if binary {
        if value.len() > max_length as usize {
            return Err(ConfiguredWriteError::DataTooLong {
                column: column.name().to_owned(),
                max_length: max_length as usize,
                char_length: value.len(),
            });
        }
        return Ok(value);
    }
    admit_char_value(column, value, max_length)
}

/// Admits a bound decimal-text literal into a `DECIMAL(precision, scale)`
/// column: parses it (Go `types.ParseDecimal`), then rounds to `scale` and
/// checks the rounded value's integer digits fit `precision - scale`
/// (`Decimal::fit_precision_scale`, already the exact call
/// `pkg/types/convert.go`'s `ProduceDecWithSpecifiedTp` performs). An integer
/// part that only fits after rounding is accepted — MySQL/TiDB round before
/// checking overflow, matching `fit_precision_scale`'s own contract.
fn admit_decimal_value(
    column: &ConfiguredColumn,
    bytes: &[u8],
    precision: u32,
    scale: u32,
) -> Result<Decimal, ConfiguredWriteError> {
    let (parsed, _warning) = Decimal::parse_mysql(&String::from_utf8_lossy(bytes));
    parsed.fit_precision_scale(precision, scale).ok_or_else(|| {
        ConfiguredWriteError::DecimalOutOfRange {
            column: column.name().to_owned(),
        }
    })
}

/// Admits a bound literal into a `DATE`/`DATETIME`/`TIMESTAMP`/`TIME` column,
/// rounding to the column's own declared fractional-seconds precision
/// (`fsp`) exactly as `parse_time`/`parse_duration` already do for the read
/// path's literal folding.
///
/// `TIMESTAMP` is parsed and stored as the literal wall-clock value with no
/// session-timezone-to-UTC conversion: this bounded write boundary has no
/// session timezone threaded to it yet (see `ConfiguredScalarType::Timestamp`'s
/// own doc, which notes the conversion happens server-side on read). This is a
/// known, narrower behavior than Go's `TIMESTAMP`, tracked as a follow-up
/// rather than silently guessed.
fn admit_temporal_value(
    column: &ConfiguredColumn,
    bytes: &[u8],
    scalar_type: ConfiguredScalarType,
) -> Result<Datum, ConfiguredWriteError> {
    let invalid = |message: String| ConfiguredWriteError::InvalidTemporal {
        column: column.name().to_owned(),
        message,
    };
    let text = std::str::from_utf8(bytes)
        .map_err(|_| invalid(String::from_utf8_lossy(bytes).into_owned()))?;
    match scalar_type {
        ConfiguredScalarType::Date => {
            let parsed = parse_time(text, TimeType::Date, 0, false, false, false, &chrono::Utc)
                .map_err(|error| invalid(format!("{text}: {error}")))?;
            Ok(Datum::Time(parsed.time))
        }
        ConfiguredScalarType::Datetime { fsp } => {
            let parsed = parse_time(
                text,
                TimeType::DateTime,
                i64::from(fsp),
                false,
                false,
                false,
                &chrono::Utc,
            )
            .map_err(|error| invalid(format!("{text}: {error}")))?;
            Ok(Datum::Time(parsed.time))
        }
        ConfiguredScalarType::Timestamp { fsp } => {
            let parsed = parse_time(
                text,
                TimeType::Timestamp,
                i64::from(fsp),
                false,
                false,
                false,
                &chrono::Utc,
            )
            .map_err(|error| invalid(format!("{text}: {error}")))?;
            Ok(Datum::Time(parsed.time))
        }
        ConfiguredScalarType::Duration { fsp } => {
            let parsed = parse_duration(bytes, i64::from(fsp))
                .map_err(|error| invalid(format!("{text}: {error:?}")))?;
            let duration = MySqlDuration::from_nanoseconds(parsed.nanoseconds(), parsed.fsp())
                .map_err(|error| invalid(format!("{text}: {error:?}")))?;
            Ok(Datum::Duration(duration))
        }
        ConfiguredScalarType::BigInt
        | ConfiguredScalarType::UnsignedBigInt
        | ConfiguredScalarType::Int
        | ConfiguredScalarType::Double
        | ConfiguredScalarType::Char { .. }
        | ConfiguredScalarType::Varchar { .. }
        | ConfiguredScalarType::Decimal { .. } => {
            unreachable!("configured_stored_value only calls this for temporal scalar types")
        }
    }
}

/// Encodes the new-format row value for a complete set of stored columns,
/// through the same `Datum`-based codec (`tidb_tablecodec::encode_table_row`)
/// the in-process `KvTable` store and the real-TiKV read path both already
/// use. Routing through one shared codec (rather than growing a bespoke
/// integer/bytes-only encoder) is what makes every `Datum` variant — `UInt`,
/// `Real`, `Decimal`, `Time`, `Duration`, `Null` — writable with no per-type
/// encoding logic of this crate's own.
fn encode_row_value(columns: &[(i64, Datum)]) -> Result<Vec<u8>, ConfiguredWriteError> {
    let ids: Vec<i64> = columns.iter().map(|(id, _)| *id).collect();
    let values: Vec<Datum> = columns.iter().map(|(_, value)| value.clone()).collect();
    Ok(encode_table_row(None, &values, &ids, true, None)?)
}

/// Decodes every stored column of a persisted row in one pass, keyed by
/// column ID.
///
/// A row rewrite (a point UPDATE) or a point DELETE's index pre-image must
/// read every stored column at its own configured type — decoding once into a
/// map (rather than once per column) is both simpler and cheaper than the
/// prior per-column re-parse. An explicit SQL `NULL` decodes to [`Datum::Null`]
/// (`tidb_tablecodec::decode_table_row_to_map` already distinguishes a
/// present-but-NULL column from an absent one), so a nullable column's value
/// carries forward exactly, with no special case here.
fn decode_stored_row(
    table: &ConfiguredTable,
    stored: &[u8],
) -> Result<std::collections::BTreeMap<i64, Datum>, ConfiguredWriteError> {
    let field_types: std::collections::BTreeMap<i64, tidb_datatype::FieldType> = table
        .columns()
        .iter()
        .filter(|column| column.kind() == ConfiguredColumnKind::Stored)
        .map(|column| (column.id(), column.scalar_type().chunk_field_type()))
        .collect();
    decode_table_row_to_map(stored, &field_types, None)
        .map_err(|error| ConfiguredWriteError::RowRead(error.to_string()))
}

/// Resolves a non-unique integer index's column value for a newly inserted row.
///
/// Fails closed on the index shapes this path does not maintain: a unique
/// index, an index over a non-integer or nullable column, or an index whose
/// column is not a stored column of the row.
fn indexed_insert_value(
    index: &ConfiguredIndex,
    columns: &[(i64, Datum)],
) -> Result<i64, ConfiguredWriteError> {
    if index.is_unique() {
        return Err(ConfiguredWriteError::UnsupportedIndex {
            reason: "unique index",
        });
    }
    match columns.iter().find(|(id, _)| *id == index.column_id()) {
        Some((_, Datum::Int(value))) => Ok(*value),
        Some((_, Datum::Null)) => Err(ConfiguredWriteError::UnsupportedIndex {
            reason: "index over a nullable column",
        }),
        Some(_) => Err(ConfiguredWriteError::UnsupportedIndex {
            reason: "index over a non-integer column",
        }),
        None => Err(ConfiguredWriteError::UnsupportedIndex {
            reason: "indexed column is not a stored column",
        }),
    }
}

/// Resolves a non-unique integer index's column value from a decoded stored
/// row (the pre-image for a delete or an update).
fn indexed_stored_value(
    index: &ConfiguredIndex,
    stored_row: &std::collections::BTreeMap<i64, Datum>,
) -> Result<i64, ConfiguredWriteError> {
    if index.is_unique() {
        return Err(ConfiguredWriteError::UnsupportedIndex {
            reason: "unique index",
        });
    }
    match stored_row.get(&index.column_id()) {
        Some(Datum::Int(value)) => Ok(*value),
        Some(Datum::Null) => Err(ConfiguredWriteError::UnsupportedIndex {
            reason: "index over a nullable column",
        }),
        Some(_) => Err(ConfiguredWriteError::UnsupportedIndex {
            reason: "index over a non-integer column",
        }),
        None => Err(ConfiguredWriteError::UnsupportedIndex {
            reason: "indexed column is not a stored column",
        }),
    }
}

/// Builds the PUT mutation adding a non-unique index entry for `(value, handle)`.
fn index_put_mutation(
    table_id: i64,
    index: &ConfiguredIndex,
    value: i64,
    handle: i64,
) -> Result<OptimisticMutation, ConfiguredWriteError> {
    let key =
        encode_non_unique_index_key(table_id, index.index_id(), &[Datum::new_int(value)], handle)
            .map_err(|_| ConfiguredWriteError::UnsupportedIndex {
            reason: "index column value is not encodable",
        })?;
    OptimisticMutation::index_put(key, non_unique_index_value())
        .map_err(ConfiguredWriteError::Mutations)
}

/// Builds the DELETE mutation removing the non-unique index entry for
/// `(value, handle)`.
fn index_delete_mutation(
    table_id: i64,
    index: &ConfiguredIndex,
    value: i64,
    handle: i64,
) -> Result<OptimisticMutation, ConfiguredWriteError> {
    let key =
        encode_non_unique_index_key(table_id, index.index_id(), &[Datum::new_int(value)], handle)
            .map_err(|_| ConfiguredWriteError::UnsupportedIndex {
            reason: "index column value is not encodable",
        })?;
    OptimisticMutation::index_delete(key).map_err(ConfiguredWriteError::Mutations)
}

/// Splits a bound INSERT row into its clustered handle and stored columns.
///
/// Each stored value is range-checked against its column's integer domain
/// before any row bytes exist, so an out-of-range `INT` fails closed exactly
/// where Go's `ConvertIntToInt` would.
fn split_row(
    table: &ConfiguredTable,
    row: &ConfiguredInsertRow,
) -> Result<(i64, Vec<(i64, Datum)>), ConfiguredWriteError> {
    let mut handle = 0;
    let mut columns = Vec::with_capacity(row.values().len());
    for (column_index, value) in row.values() {
        let column = &table.columns()[*column_index];
        match column.kind() {
            // A clustered handle is always a signed integer column.
            ConfiguredColumnKind::ClusteredPrimaryKey => {
                handle = expect_integer_column_value(column, value)?;
            }
            ConfiguredColumnKind::Stored => {
                columns.push((column.id(), configured_stored_value(column, value)?));
            }
        }
    }
    // Stored columns are emitted in configured order so one logical row always
    // produces one byte sequence regardless of the statement's column order.
    columns.sort_by_key(|(id, _)| *id);
    Ok((handle, columns))
}

/// Resolves a bound value against an integer column (the clustered handle),
/// range-checking the integer and rejecting string bytes.
fn expect_integer_column_value(
    column: &ConfiguredColumn,
    value: &PreparedBindValue,
) -> Result<i64, ConfiguredWriteError> {
    match value {
        PreparedBindValue::Int(value) => {
            check_column_value(column, *value)?;
            Ok(*value)
        }
        PreparedBindValue::UInt(_) | PreparedBindValue::Float(_) | PreparedBindValue::Null => {
            Err(ConfiguredWriteError::ColumnTypeMismatch {
                column: column.name().to_owned(),
                scalar_type: column.scalar_type(),
                value_is_bytes: false,
            })
        }
        PreparedBindValue::Bytes(_) => Err(ConfiguredWriteError::ColumnTypeMismatch {
            column: column.name().to_owned(),
            scalar_type: column.scalar_type(),
            value_is_bytes: true,
        }),
    }
}

/// Builds the "wrong kind of value for this column" error `configured_stored_value`
/// reports for every scalar type it refuses at a given position.
fn type_mismatch(column: &ConfiguredColumn, value_is_bytes: bool) -> ConfiguredWriteError {
    ConfiguredWriteError::ColumnTypeMismatch {
        column: column.name().to_owned(),
        scalar_type: column.scalar_type(),
        value_is_bytes,
    }
}

/// Maps a bound value to a stored column's codec value (a [`Datum`] the shared
/// row codec can encode), requiring the parameter kind to match the column's
/// type and admitting `NULL` only into a nullable column.
///
/// `VARCHAR`, `DECIMAL`, and every temporal type bind as raw string bytes (the
/// same wire shape a `CHAR` column already used): the target column's own type
/// picks how those bytes are parsed, exactly as Go's parameter binding
/// converts a string parameter through `types.Datum.ConvertTo` the target
/// column's type describes.
fn configured_stored_value(
    column: &ConfiguredColumn,
    value: &PreparedBindValue,
) -> Result<Datum, ConfiguredWriteError> {
    if matches!(value, PreparedBindValue::Null) {
        return if column.is_nullable() {
            Ok(Datum::Null)
        } else {
            Err(ConfiguredWriteError::NullNotAllowed {
                column: column.name().to_owned(),
            })
        };
    }
    match column.scalar_type() {
        ConfiguredScalarType::BigInt | ConfiguredScalarType::Int => {
            let PreparedBindValue::Int(signed) = value else {
                return Err(type_mismatch(
                    column,
                    !matches!(
                        value,
                        PreparedBindValue::UInt(_) | PreparedBindValue::Float(_)
                    ),
                ));
            };
            check_column_value(column, *signed)?;
            Ok(Datum::Int(*signed))
        }
        ConfiguredScalarType::UnsignedBigInt => match value {
            PreparedBindValue::Int(signed) if *signed >= 0 => Ok(Datum::UInt(*signed as u64)),
            PreparedBindValue::Int(_) => Err(ConfiguredWriteError::UnsignedOutOfRange {
                column: column.name().to_owned(),
            }),
            PreparedBindValue::UInt(unsigned) => Ok(Datum::UInt(*unsigned)),
            PreparedBindValue::Float(_) | PreparedBindValue::Bytes(_) => Err(type_mismatch(
                column,
                matches!(value, PreparedBindValue::Bytes(_)),
            )),
            PreparedBindValue::Null => unreachable!("NULL is admitted above"),
        },
        ConfiguredScalarType::Double => match value {
            PreparedBindValue::Float(real) => Ok(Datum::Real(*real)),
            // An integer literal (`INSERT ... VALUES (3)`) is a valid DOUBLE
            // value, exactly as Go's implicit numeric-to-numeric conversion.
            PreparedBindValue::Int(signed) => Ok(Datum::Real(*signed as f64)),
            PreparedBindValue::UInt(unsigned) => Ok(Datum::Real(*unsigned as f64)),
            PreparedBindValue::Bytes(_) => Err(type_mismatch(column, true)),
            PreparedBindValue::Null => unreachable!("NULL is admitted above"),
        },
        ConfiguredScalarType::Char { max_length } => {
            let PreparedBindValue::Bytes(bytes) = value else {
                return Err(type_mismatch(column, false));
            };
            // A collation-tagged `Datum::String`, not a bare `Datum::Bytes`:
            // this must be the exact `Datum` shape
            // `tidb_tablecodec::decode_table_row_to_map` reconstructs for this
            // column (`unflatten_datum`'s `String`/`Varchar`/... arm), so the
            // point UPDATE unchanged-value comparison compares like with like.
            Ok(Datum::new_collation_string(
                admit_char_value(column, bytes.clone(), max_length)?,
                tidb_datatype::Collation::Utf8Mb4Bin,
            ))
        }
        ConfiguredScalarType::Varchar { max_length, binary } => {
            let PreparedBindValue::Bytes(bytes) = value else {
                return Err(type_mismatch(column, false));
            };
            let collation = if binary {
                tidb_datatype::Collation::Binary
            } else {
                tidb_datatype::Collation::Utf8Mb4Bin
            };
            Ok(Datum::new_collation_string(
                admit_varchar_value(column, bytes.clone(), max_length, binary)?,
                collation,
            ))
        }
        ConfiguredScalarType::Decimal { precision, scale } => {
            let PreparedBindValue::Bytes(bytes) = value else {
                return Err(type_mismatch(column, false));
            };
            Ok(Datum::Decimal(admit_decimal_value(
                column, bytes, precision, scale,
            )?))
        }
        scalar_type @ (ConfiguredScalarType::Date
        | ConfiguredScalarType::Datetime { .. }
        | ConfiguredScalarType::Timestamp { .. }
        | ConfiguredScalarType::Duration { .. }) => {
            let PreparedBindValue::Bytes(bytes) = value else {
                return Err(type_mismatch(column, false));
            };
            admit_temporal_value(column, bytes, scalar_type)
        }
    }
}

/// Rejects a signed value outside its column's integer domain.
///
/// A non-integer column (e.g. `CHAR`) has no integer range; the integer write
/// path never targets one, so this is a no-op there rather than a false
/// rejection.
fn check_column_value(column: &ConfiguredColumn, value: i64) -> Result<(), ConfiguredWriteError> {
    let Some((min, max)) = column.scalar_type().integer_range() else {
        return Ok(());
    };
    if value < min || value > max {
        return Err(ConfiguredWriteError::ValueOutOfRange {
            column: column.name().to_owned(),
            value,
            scalar_type: column.scalar_type(),
        });
    }
    Ok(())
}

/// Upper bounds on what a bound statement can publish.
///
/// `RealOptimisticTransactionOpener::begin` validates the plan before it spends
/// a real PD timestamp, so these must be known without reading storage. An
/// INSERT encodes its rows exactly; a point UPDATE cannot know its replacement
/// row until it reads, so it declares one mutation and the widest row its
/// configured table can produce.
pub fn planned_publication_bounds(
    write: &ConfiguredPreparedWrite,
) -> Result<(usize, usize), ConfiguredWriteError> {
    match write {
        ConfiguredPreparedWrite::InsertRows { table, rows } => {
            let ConfiguredWritePlan::Write { mutations, .. } = plan_insert(table, rows)? else {
                return Err(ConfiguredWriteError::Mutations(MutationSetError::Empty));
            };
            let bytes = mutations
                .iter()
                .try_fold(0usize, |total, mutation| {
                    total
                        .checked_add(mutation.key().len())?
                        .checked_add(mutation.value().len())
                })
                .unwrap_or(usize::MAX);
            Ok((mutations.len(), bytes))
        }
        ConfiguredPreparedWrite::UpdatePoint { table, handle, .. } => {
            let key = encode_row_key_with_handle(table.table_id(), &RecordHandle::Int(*handle));
            // At most one index (the one on the assigned column) moves its entry
            // (delete + put). Over-provision two entries per index so the pre-open
            // budget never under-counts the actual mutation set.
            let index_entries = table.indexes().len().saturating_mul(2);
            Ok((
                1 + index_entries,
                key.len()
                    .saturating_add(max_configured_row_value_len(table))
                    .saturating_add(index_entries.saturating_mul(MAX_INT_INDEX_ENTRY_BYTES)),
            ))
        }
        ConfiguredPreparedWrite::DeletePoint { table, handle } => {
            // A delete publishes one record mutation plus one entry per index.
            let key = encode_row_key_with_handle(table.table_id(), &RecordHandle::Int(*handle));
            let index_entries = table.indexes().len();
            Ok((
                1 + index_entries,
                key.len()
                    .saturating_add(index_entries.saturating_mul(MAX_INT_INDEX_ENTRY_BYTES)),
            ))
        }
    }
}

/// Widest new-format row a configured table can persist.
///
/// Row format v2 spends a five-byte header, then per not-null column a column
/// ID, an end offset, and the payload. The large-row layout is the worst case at
/// four bytes each for ID and offset. The payload is type-dependent: a signed
/// integer never exceeds eight bytes, while a `CHAR(N)` utf8mb4 value stores up
/// to four bytes per character. Counting a `CHAR` column as an integer (as an
/// earlier BIGINT-only version did) under-provisions the transaction's byte
/// budget, so an UPDATE that rewrites a wide string row is wrongly rejected as
/// `TransactionTooLarge` at commit.
fn max_configured_row_value_len(table: &ConfiguredTable) -> usize {
    const ROW_HEADER_LEN: usize = 5;
    const MAX_COLUMN_METADATA_LEN: usize = 4 + 4;
    const MAX_INT_PAYLOAD_LEN: usize = 8;
    const UTF8MB4_MAX_BYTES_PER_CHAR: usize = 4;
    table
        .columns()
        .iter()
        .filter(|column| column.kind() == ConfiguredColumnKind::Stored)
        .fold(ROW_HEADER_LEN, |total, column| {
            let payload = match column.scalar_type() {
                ConfiguredScalarType::BigInt
                | ConfiguredScalarType::Int
                | ConfiguredScalarType::UnsignedBigInt
                | ConfiguredScalarType::Double
                // `Date`/`Datetime`/`Timestamp` persist as the packed 8-byte
                // `types.Time`; `Duration` persists as an 8-byte `int64`
                // nanosecond count (`tidb_codec::column`'s temporal decode).
                | ConfiguredScalarType::Date
                | ConfiguredScalarType::Datetime { .. }
                | ConfiguredScalarType::Timestamp { .. }
                | ConfiguredScalarType::Duration { .. } => MAX_INT_PAYLOAD_LEN,
                ConfiguredScalarType::Char { max_length } => {
                    (max_length as usize).saturating_mul(UTF8MB4_MAX_BYTES_PER_CHAR)
                }
                ConfiguredScalarType::Varchar {
                    max_length,
                    binary: true,
                } => max_length as usize,
                ConfiguredScalarType::Varchar {
                    max_length,
                    binary: false,
                } => (max_length as usize).saturating_mul(UTF8MB4_MAX_BYTES_PER_CHAR),
                // Fixed-width `MyDecimal` binary encoding
                // (`tidb_codec::column::MY_DECIMAL_BYTES`).
                ConfiguredScalarType::Decimal { .. } => 40,
            };
            total
                .saturating_add(MAX_COLUMN_METADATA_LEN)
                .saturating_add(payload)
        })
}

/// What one bound statement did to storage.
#[derive(Debug)]
pub enum ConfiguredWriteOutcome {
    /// Mutations were published; `affected_rows` is reportable only once
    /// `outcome` is [`OptimisticCommitOutcome::Committed`].
    Published {
        /// Terminal transaction outcome and its receipt. Boxed because a
        /// committed receipt is an order of magnitude larger than the
        /// no-publication variant.
        outcome: Box<OptimisticCommitOutcome>,
        /// Rows this statement would report on a determinate commit.
        affected_rows: u64,
    },
    /// Nothing was published; the statement reports zero affected rows.
    NoPublication {
        /// The read-only transaction that observed the row.
        transaction: ReadOnlyTransaction,
        /// Why nothing was published.
        reason: NoWriteReason,
    },
}

/// Parses and admits one prepared write template.
///
/// This mirrors `prepare_configured_point_read`: the server never parses SQL
/// or names transaction types itself, so the dependency direction stays
/// `tidb-server -> tidb-exec -> tidb-txnkv`.
pub fn prepare_configured_write(
    sql: &str,
    catalog: &ConfiguredCatalog,
) -> Result<ConfiguredPreparedWriteTemplate, ConfiguredWriteError> {
    let statement = tidb_parser::parse(sql).map_err(|error| {
        ConfiguredWriteError::Parse(format!("{} at byte {}", error.message, error.offset))
    })?;
    lower_prepared_write(&statement, catalog).map_err(ConfiguredWriteError::Plan)
}

/// Parses and admits one text-protocol `COM_QUERY` statement as a write.
///
/// Returns `None` when the statement is not a single-table INSERT/UPDATE/DELETE
/// — including when it does not parse at all, so an unparsable statement is
/// still reported by the read path that owns the same text. A statement that
/// *is* one of those three is lowered through the prepared path's own admission
/// rules with its literals in place of markers, so text and prepared writes
/// admit and refuse exactly the same shapes.
pub fn prepare_text_write(
    sql: &str,
    catalog: &ConfiguredCatalog,
) -> Result<Option<ConfiguredPreparedWriteTemplate>, ConfiguredWriteError> {
    let Ok(statement) = tidb_parser::parse(sql) else {
        return Ok(None);
    };
    let tidb_ast::Stmt::Dml(dml) = &statement else {
        return Ok(None);
    };
    if !matches!(
        dml.as_ref(),
        tidb_ast::DmlStmt::Insert(_) | tidb_ast::DmlStmt::Update(_) | tidb_ast::DmlStmt::Delete(_)
    ) {
        return Ok(None);
    }
    lower_text_write(&statement, catalog)
        .map(Some)
        .map_err(ConfiguredWriteError::Plan)
}

/// What one committed statement reports to the MySQL client.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConfiguredWriteReport {
    /// Rows to report in the OK packet.
    pub affected_rows: u64,
    /// Set when the statement matched nothing or changed nothing.
    pub no_write: Option<NoWriteReason>,
}

/// Opens one transaction on the shared authority, publishes a bound write, and
/// reports affected rows only for a determinate commit.
///
/// Every other terminal state — rolled back, cleanup failed, or undetermined —
/// is an error here, because an OK packet asserts durable rows.
pub fn commit_configured_write(
    opener: &RealOptimisticTransactionOpener,
    write: &ConfiguredPreparedWrite,
    timeout: Duration,
) -> Result<ConfiguredWriteReport, ConfiguredWriteError> {
    // Bounds are computed before the transaction opens so an oversized
    // statement never spends a real PD timestamp.
    let (planned_mutations, planned_bytes) = planned_publication_bounds(write)?;
    let transaction = opener.begin(planned_mutations, planned_bytes)?;
    let call = UnaryCallContext::with_timeout(timeout);
    match execute_configured_write(transaction, write, &call)? {
        ConfiguredWriteOutcome::Published {
            outcome,
            affected_rows,
        } => match *outcome {
            OptimisticCommitOutcome::Committed { .. } => Ok(ConfiguredWriteReport {
                affected_rows,
                no_write: None,
            }),
            other => Err(ConfiguredWriteError::NotCommitted(format!("{other:?}"))),
        },
        ConfiguredWriteOutcome::NoPublication { reason, .. } => Ok(ConfiguredWriteReport {
            affected_rows: 0,
            no_write: Some(reason),
        }),
    }
}

/// The one transaction capability write planning needs: a point Get at the
/// transaction's start timestamp, reading the row a point `UPDATE`/`DELETE`
/// rewrites (a row `INSERT` needs no read). Abstracting the Get lets planning
/// be exercised without a live coordinator; the production path is the impl for
/// [`RealOptimisticTransaction`].
pub trait WritePlanningSnapshot {
    /// Reads the value stored at `key` at the transaction start timestamp,
    /// `None` when TiKV holds no value there.
    fn read_at_snapshot(
        &mut self,
        key: &[u8],
        call: &UnaryCallContext,
    ) -> Result<Option<Vec<u8>>, ConfiguredWriteError>;
}

impl<C, L, T> WritePlanningSnapshot for RealOptimisticTransaction<C, L, T>
where
    C: TransactionCommandClient + LockRecoveryClient,
    L: RegionRecoveryLoader,
    T: TimestampSource,
{
    fn read_at_snapshot(
        &mut self,
        key: &[u8],
        call: &UnaryCallContext,
    ) -> Result<Option<Vec<u8>>, ConfiguredWriteError> {
        Ok(self.snapshot_get(key, call)?.value)
    }
}

/// Plans one bound write against `snapshot` without publishing it.
///
/// A point `UPDATE`/`DELETE` reads its own row at the transaction start
/// timestamp through `snapshot` — the same coordinator that will publish the
/// mutation — so planning can never observe a later snapshot; an `INSERT` needs
/// no read. Returning the [`ConfiguredWritePlan`] rather than committing lets a
/// multi-statement transaction stage the mutations into its buffer and commit
/// them together at `COMMIT`, while [`execute_configured_write`] plans then
/// commits in one step.
pub fn plan_configured_write<S: WritePlanningSnapshot>(
    snapshot: &mut S,
    write: &ConfiguredPreparedWrite,
    call: &UnaryCallContext,
) -> Result<ConfiguredWritePlan, ConfiguredWriteError> {
    match write {
        ConfiguredPreparedWrite::InsertRows { table, rows } => plan_insert(table, rows),
        ConfiguredPreparedWrite::UpdatePoint {
            table,
            handle,
            column_index,
            assignment,
        } => {
            let key = encode_row_key_with_handle(table.table_id(), &RecordHandle::Int(*handle));
            let observed = snapshot.read_at_snapshot(&key, call)?;
            plan_update(
                table,
                *handle,
                *column_index,
                assignment.clone(),
                observed.as_deref(),
            )
        }
        ConfiguredPreparedWrite::DeletePoint { table, handle } => {
            let key = encode_row_key_with_handle(table.table_id(), &RecordHandle::Int(*handle));
            let observed = snapshot.read_at_snapshot(&key, call)?;
            plan_delete(table, *handle, observed.as_deref())
        }
    }
}

/// Runs one bound write on the shared real optimistic transaction, committing it
/// as its own single-statement transaction.
///
/// Planning ([`plan_configured_write`]) reads any UPDATE/DELETE row at this
/// transaction's start timestamp through the same coordinator that publishes the
/// mutation, so it can never observe a later snapshot; an INSERT needs no read.
pub fn execute_configured_write<C, L, T>(
    mut transaction: RealOptimisticTransaction<C, L, T>,
    write: &ConfiguredPreparedWrite,
    call: &UnaryCallContext,
) -> Result<ConfiguredWriteOutcome, ConfiguredWriteError>
where
    C: TransactionCommandClient + LockRecoveryClient,
    L: RegionRecoveryLoader,
    T: TimestampSource,
{
    match plan_configured_write(&mut transaction, write, call)? {
        ConfiguredWritePlan::Write {
            mutations,
            affected_rows,
        } => Ok(ConfiguredWriteOutcome::Published {
            outcome: Box::new(transaction.commit(mutations, call)?),
            affected_rows,
        }),
        ConfiguredWritePlan::NoWrite { reason } => Ok(ConfiguredWriteOutcome::NoPublication {
            transaction: transaction.finish_without_writes()?,
            reason,
        }),
    }
}
