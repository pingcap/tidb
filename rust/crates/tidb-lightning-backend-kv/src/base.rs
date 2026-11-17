//! Go `base.go`: `BaseKVEncoder`, which turns one input row into the
//! table's record through the table layer.

use tidb_datatype::{Datum, FieldType, FieldTypeFlags};
use tidb_expr::EvalErrorSql as _;
use tidb_lightning_backend_encode::EncodingConfig;
use tidb_lightning_verification::KvPair;
use tidb_model::ColumnInfo;
use tidb_table::column::Column;
use tidb_table::tables::TableCommon;
use tidb_terror::TerrorError;

use crate::allocator::{AllocatorType, PanickingAllocators};
use crate::session::Session;

/// Go `RowIDBitLength`.
const ROW_ID_BIT_LENGTH: u64 = 64;

/// Why a row did not encode: Go's error, as `Error()` prints it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EncodeError {
    /// A Go `*errors.Error` such as `ErrCastValue`.
    Terror(TerrorError),
    /// Any other Go error text.
    Other(String),
}

impl std::fmt::Display for EncodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Terror(error) => error.fmt(f),
            Self::Other(text) => f.write_str(text),
        }
    }
}

impl std::error::Error for EncodeError {}

/// Go `AutoIDConverterFn`.
pub(crate) enum AutoIdFn {
    /// The id as is.
    Identity,
    /// `shardFmt.Compose(shard, id)` for an AUTO_RANDOM handle.
    AutoRandom { format: ShardIdFormat, shard: i64 },
    /// SHARD_ROW_ID_BITS: a shard drawn from a generator re-seeded by id.
    ShardRowId { rand: Box<tidb_go_rand::Rand>, mask: i64, shift: u64 },
}

impl AutoIdFn {
    pub(crate) fn apply(&mut self, id: i64) -> i64 {
        match self {
            Self::Identity => id,
            Self::AutoRandom { format, shard } => format.compose(*shard, id),
            Self::ShardRowId { rand, mask, shift } => {
                rand.seed(id);
                let shard_bits = (i64::from(rand.uint32()) & *mask) << *shift;
                shard_bits | id
            }
        }
    }
}

/// Go `autoid.ShardIDFormat`.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ShardIdFormat {
    shard_bits: u64,
    incremental_bits: u64,
}

impl ShardIdFormat {
    /// Go `NewShardIDFormat`.
    pub(crate) fn new(field_type: &FieldType, shard_bits: u64, range_bits: u64) -> Self {
        let mut incremental_bits = if range_bits == 0 {
            ROW_ID_BIT_LENGTH - shard_bits
        } else {
            range_bits - shard_bits
        };
        if field_type.flags() & FieldTypeFlags::UNSIGNED == 0 {
            incremental_bits -= 1;
        }
        Self { shard_bits, incremental_bits }
    }

    /// Go `IncrementalMask`.
    pub(crate) fn incremental_mask(self) -> i64 {
        ((1_u64 << self.incremental_bits) - 1) as i64
    }

    /// Go `Compose`.
    fn compose(self, shard: i64, id: i64) -> i64 {
        ((shard & ((1_i64 << self.shard_bits) - 1)) << self.incremental_bits) | id
    }
}

/// Go `BaseKVEncoder`.
pub struct BaseKVEncoder {
    pub(crate) session: Session,
    pub(crate) table: TableCommon,
    pub(crate) allocators: PanickingAllocators,
    /// Go `Columns`: the public columns, in offset order.
    pub(crate) columns: Vec<Column>,
    pub(crate) has_generated: bool,
    auto_random_col_id: i64,
    pub(crate) auto_id_fn: AutoIdFn,
}

impl BaseKVEncoder {
    /// Go `NewBaseKVEncoder`, over allocators the caller shares with the
    /// importer (Go's `table.Table` carries them).
    pub fn new(config: &EncodingConfig, allocators: PanickingAllocators) -> Result<Self, EncodeError> {
        let meta = &config.table;
        let use_new_collation = tidb_datatype::new_collation_enabled();
        let table = tidb_table::table_from_meta::table_from_meta(
            meta,
            &tidb_table::table_from_meta::AutoIdSource::Unavailable,
        )
        .map_err(EncodeError::Other)?
        .with_new_collation_mode(use_new_collation);
        let mut session = Session::new(&config.session_options);
        session.ctx = session.ctx.clone().with_new_collation(use_new_collation);
        let mut auto_random_col_id = 0;
        let mut auto_id_fn = AutoIdFn::Identity;
        if !config.use_identity_auto_row_id {
            if meta.contains_auto_random_bits() {
                if let Some(col) = tidb_lightning_common::get_auto_random_column(meta) {
                    let col = col.read();
                    auto_random_col_id = col.id;
                    let format = ShardIdFormat::new(
                        &col.field_type,
                        meta.auto_random_bits,
                        meta.auto_random_range_bits,
                    );
                    let shard = tidb_go_rand::Rand::new(config.session_options.auto_random_seed).int63();
                    auto_id_fn = AutoIdFn::AutoRandom { format, shard };
                }
            } else if meta.shard_row_id_bits > 0 {
                auto_id_fn = AutoIdFn::ShardRowId {
                    rand: Box::new(tidb_go_rand::Rand::new(config.session_options.auto_random_seed)),
                    mask: (1_i64 << meta.shard_row_id_bits) - 1,
                    shift: ROW_ID_BIT_LENGTH - meta.shard_row_id_bits - 1,
                };
            }
        }
        let columns: Vec<Column> = table
            .columns()
            .iter()
            .filter(|c| c.read().state == tidb_model::SchemaState::PUBLIC)
            .cloned()
            .collect();
        let has_generated = columns.iter().any(|c| c.generated.is_some());
        Ok(Self {
            session,
            table,
            allocators,
            columns,
            has_generated,
            auto_random_col_id,
            auto_id_fn,
        })
    }

    /// Go `TableAllocators`.
    #[must_use]
    pub fn table_allocators(&self) -> &PanickingAllocators {
        &self.allocators
    }

    /// Go `SessionCtx`.
    #[must_use]
    pub fn session(&self) -> &Session {
        &self.session
    }

    /// Go `Columns`: the public columns, in offset order.
    #[must_use]
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    /// Go `GenCols` is non-empty.
    #[must_use]
    pub fn has_generated_columns(&self) -> bool {
        self.has_generated
    }

    /// Go `AutoIDFn`.
    pub fn auto_id(&mut self, id: i64) -> i64 {
        self.auto_id_fn.apply(id)
    }

    /// Go `TableMeta()`.
    #[must_use]
    pub fn table_meta(&self) -> tidb_model::TableInfo {
        self.table.meta().read().clone()
    }

    /// Go `Record2KV`: `AddRecord` with `DupKeyCheckSkip`, then every pair
    /// tagged with the row id.
    pub fn record_to_kv(&mut self, record: &[Datum], record_id: Option<i64>, row_id: i64) -> Result<Vec<KvPair>, EncodeError> {
        let columns = self.columns.len();
        self.table
            .add_record_with_record_id(
                &mut self.session.mem_buf.clone(),
                &record[..columns],
                record_id,
                true,
                &self.session.ctx,
            )
            .map_err(|error| mysql_error(error.into_mysql_error()))?;
        let mut pairs = self.session.take_kv_pairs();
        let encoded = tidb_lightning_common::encode_int_row_id(row_id);
        for pair in &mut pairs {
            pair.row_id.clone_from(&encoded);
        }
        Ok(pairs)
    }

    /// Go `ProcessColDatum`: the column's value, with the AUTO_RANDOM and
    /// AUTO_INCREMENT allocators rebased past it.
    pub fn process_col_datum(
        &mut self,
        col: &Column,
        row_id: i64,
        input: Option<&Datum>,
        need_cast: bool,
    ) -> Result<Datum, EncodeError> {
        let value = self.get_actual_datum(col, row_id, input, need_cast)?;
        let info = col.read();
        if self.is_auto_random_col(&info) {
            let meta = self.table.meta();
            let meta = meta.read();
            let format = ShardIdFormat::new(&info.field_type, meta.auto_random_bits, meta.auto_random_range_bits);
            self.allocators
                .get(AllocatorType::AutoRandom)
                .rebase(datum_int64(&value) & format.incremental_mask());
        }
        if is_auto_inc_col(&info) {
            self.allocators
                .get(AllocatorType::AutoIncrement)
                .rebase(get_auto_record_id(&value, &info.field_type));
        }
        Ok(value)
    }

    /// Go `getActualDatum`.
    pub fn get_actual_datum(
        &mut self,
        col: &Column,
        row_id: i64,
        input: Option<&Datum>,
        need_cast: bool,
    ) -> Result<Datum, EncodeError> {
        let info = col.read().clone();
        let ctx = &self.session.ctx;
        let mut value = Datum::Null;
        let mut is_bad_null_value = false;
        if let Some(input) = input {
            value = if need_cast {
                self.cast(input.clone(), &info)?
            } else {
                input.clone()
            };
            if !(value.is_null() && refuses_null(&info.field_type)) {
                return Ok(value);
            }
            is_bad_null_value = true;
        }
        if is_auto_inc_col(&info) {
            // A conversion still catches an overflow, e.g. of a TINYINT.
            return self.cast(Datum::Int(row_id), &info);
        }
        if self.is_auto_random_col(&info) {
            let real_row_id = self.auto_id_fn.apply(row_id);
            let datum = if info.field_type.flags() & FieldTypeFlags::UNSIGNED != 0 {
                Datum::UInt(real_row_id as u64)
            } else {
                Datum::Int(real_row_id)
            };
            return self.cast(datum, &info);
        }
        if col.generated.is_some() {
            // A dummy value, replaced when the generated columns are evaluated.
            return Ok(tidb_datatype::get_min_value(&info.field_type));
        }
        if is_bad_null_value {
            let level = match tidb_exprctx::Columns::bad_null_level(ctx) {
                tidb_exprctx::ErrorLevel::Error => tidb_table::bad_null::NullLevel::Error,
                _ => tidb_table::bad_null::NullLevel::Warn,
            };
            tidb_table::bad_null::handle_bad_null(&mut value, &info.field_type, info.name.original(), level, ctx)
                .map_err(column_error)?;
            return Ok(value);
        }
        tidb_table::column_default::get_col_default_value(
            col.default_value.as_ref(),
            &info.field_type,
            info.name.original(),
            info.version,
            refuses_null(&info.field_type),
            ctx,
            tidb_chunk::row::Row::empty(),
        )
        .map_err(column_error)
    }

    /// Go `table.CastColumnValue(exprCtx, value, col, false, false)`.
    pub fn cast(&self, value: Datum, info: &ColumnInfo) -> Result<Datum, EncodeError> {
        tidb_table::column::cast_value(value, &info.field_type, info.name.original(), &self.session.ctx, false)
            .map_err(|error| mysql_error(error.to_mysql_error()))
    }

    /// Go `IsAutoRandomCol`.
    #[must_use]
    pub fn is_auto_random_col(&self, col: &ColumnInfo) -> bool {
        self.table.meta().read().contains_auto_random_bits() && col.id == self.auto_random_col_id
    }

    /// Go `EvalGeneratedColumns`: every generated column, in offset order,
    /// evaluated over the record and cast into its column.
    pub fn eval_generated_columns(&self, record: &mut [Datum]) -> Result<(), (String, EncodeError)> {
        let ctx = &self.session.ctx;
        let columns = &self.columns;
        tidb_table::generated_column::materialize_with(
            columns,
            &mut record[..columns.len()],
            false,
            ctx,
            |col: &Column, value: Datum| -> Result<Datum, GenError> {
                let info = col.read();
                self.cast(value, &info)
                    .map_err(|error| GenError(info.name.original().to_owned(), error))
            },
        )
        .map_err(|GenError(name, error)| (name, error))
    }

    /// Go `LogKVConvertFailed`: `ErrCastValue` naming the column, its type,
    /// the input value at `j` (NULL when the input has none) and the cause.
    pub fn log_kv_convert_failed(&self, row: &[Datum], j: isize, col: &ColumnInfo, error: EncodeError) -> EncodeError {
        let original = usize::try_from(j).ok().and_then(|j| row.get(j)).cloned().unwrap_or(Datum::Null);
        let bad_value = datum_to_value_string_for_cast_error(&original);
        EncodeError::Terror(tidb_lightning_common::errors::ERR_CAST_VALUE.gen_with_stack_by_args(&[
            col.name.original().into(),
            col.field_type.source_string().into(),
            bad_value.into(),
            error.to_string().into(),
        ]))
    }

    /// Go `LogEvalGenExprFailed`.
    pub fn log_eval_gen_expr_failed(&self, column: &str, error: EncodeError) -> EncodeError {
        EncodeError::Other(format!(
            "failed to evaluate generated column expression for column `{column}`: {error}"
        ))
    }

    /// Go `TruncateWarns`.
    pub fn truncate_warns(&self) {
        tidb_exprctx::Columns::truncate_warnings(&self.session.ctx, 0);
    }
}

/// A generated column's failure, carrying its name out of the evaluation.
struct GenError(String, EncodeError);

impl From<tidb_table::generated_column::GenerationError> for GenError {
    fn from(error: tidb_table::generated_column::GenerationError) -> Self {
        let text = match &error.eval {
            Some(eval) => eval.clone().to_mysql_error().error_string(),
            None => error.detail.clone(),
        };
        GenError(error.column, EncodeError::Other(text))
    }
}

/// Go `datumToValueStringForCastError`: `DatumsToStringSmart` of one datum.
fn datum_to_value_string_for_cast_error(datum: &Datum) -> String {
    tidb_datatype::datums_to_string(std::slice::from_ref(datum), true, true)
        .unwrap_or_else(|_| "<unprintable>".to_owned())
}

fn mysql_error(error: tidb_terror::MysqlError) -> EncodeError {
    EncodeError::Other(error.error_string())
}

fn column_error(error: tidb_table::column::ColumnError) -> EncodeError {
    mysql_error(tidb_table::tables::KvTableError::from(error).into_mysql_error())
}

/// Go `mysql.HasNotNullFlag || HasPreventNullInsertFlag`.
fn refuses_null(field_type: &FieldType) -> bool {
    field_type.flags() & (FieldTypeFlags::NOT_NULL | FieldTypeFlags::PREVENT_NULL_INSERT) != 0
}

/// Go `IsAutoIncCol`.
#[must_use]
pub fn is_auto_inc_col(col: &ColumnInfo) -> bool {
    col.field_type.flags() & FieldTypeFlags::AUTO_INCREMENT != 0
}

/// Go `Datum.GetInt64`.
pub(crate) fn datum_int64(datum: &Datum) -> i64 {
    match datum {
        Datum::Int(v) => *v,
        Datum::UInt(v) => *v as i64,
        _ => 0,
    }
}

/// Go `GetAutoRecordID`: an auto-increment column's value as an id.
#[must_use]
pub fn get_auto_record_id(datum: &Datum, target: &FieldType) -> i64 {
    use tidb_datatype::FieldTypeCode as T;
    match target.code() {
        T::Float | T::Double => match datum {
            Datum::Float32(v) => v.round() as i64,
            Datum::Real(v) => v.round() as i64,
            other => datum_int64(other),
        },
        T::Tiny | T::Short | T::Int24 | T::Long | T::LongLong => datum_int64(datum),
        other => panic!("unsupported auto-increment field type '{other:?}'"),
    }
}
