//! Go `pkg/dumpformat/parquetfile`: the Parquet data-file parser IMPORT
//! INTO and Lightning read rows with (`parser.go`, `type_converter.go`,
//! `spark_rebase.go`, `reader_wrapper.go`).
//!
//! Go reads with arrow-go's `parquet/file`; this port reads with the Apache
//! `parquet` crate's typed column readers. The file is read through
//! `tidb-objstore` range requests, so a remote file is never downloaded
//! whole. The writer (`writer.go`, used by dump tools) is not ported.

mod spark_rebase;
#[rustfmt::skip]
mod spark_rebase_micros_generated;
mod type_converter;

use std::sync::Arc;

use bytes::Bytes;
use parquet::basic::{ConvertedType, LogicalType as PqLogicalType, TimeUnit as PqTimeUnit, Type as PhysicalType};
use parquet::column::reader::{get_typed_column_reader, ColumnReader, ColumnReaderImpl};
use parquet::data_type::{
    BoolType, ByteArrayType, DataType, DoubleType, FixedLenByteArrayType, FloatType, Int32Type, Int64Type, Int96Type,
};
use parquet::file::reader::{ChunkReader, FileReader, Length, SerializedFileReader};
use tidb_datatype::{Datum, SessionTimeZone};
use tidb_gocontext::Context;
use tidb_objstore::storeapi::Storage;

use spark_rebase::{
    FileInfo, SparkRebaseMicrosLookup, SPARK_DATETIME_REBASE_CUTOFF, SPARK_INT96_REBASE_CUTOFF,
    SPARK_LEGACY_DATE_TIME_METADATA_KEY, SPARK_LEGACY_INT96_METADATA_KEY,
};
use type_converter::{LogicalType, ParquetColumnType, TimeUnit};

/// Go `readBatchSize`.
const READ_BATCH_SIZE: usize = 128;
/// Go `maxRowSize` (`config.MaxTxnEntrySizeLimit`).
const MAX_ROW_SIZE: usize = 120 * 1024 * 1024;

/// Go `FileMeta`: what a parser is opened with.
#[derive(Clone, Debug, Default)]
pub struct FileMeta {
    /// Go `Loc`: the zone UTC-adjusted times are read in; the system
    /// location when `None`.
    pub loc: Option<SessionTimeZone>,
}

/// Go `parsedef.ErrRowTooLarge`'s text.
pub const ERR_ROW_TOO_LARGE: &str = "size of row cannot exceed the max value of txn-entry-size-limit";

/// A storage file read by range (Go `readerWrapper`).
#[derive(Clone)]
struct StorageFile {
    ctx: Context,
    store: Arc<dyn Storage>,
    path: String,
    size: u64,
}

impl Length for StorageFile {
    fn len(&self) -> u64 {
        self.size
    }
}

impl ChunkReader for StorageFile {
    type T = std::io::Cursor<Bytes>;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        let len = self.size.saturating_sub(start) as usize;
        Ok(std::io::Cursor::new(self.get_bytes(start, len)?))
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        let mut buf = vec![0; length];
        if length > 0 {
            tidb_objstore::read_data_in_range(&self.ctx, self.store.as_ref(), &self.path, start as i64, &mut buf)
                .map_err(|e| parquet::errors::ParquetError::External(Box::new(e)))?;
        }
        Ok(Bytes::from(buf))
    }
}

fn pq_err(e: parquet::errors::ParquetError) -> String {
    e.to_string()
}

fn time_unit(unit: &PqTimeUnit) -> TimeUnit {
    match unit {
        PqTimeUnit::MILLIS => TimeUnit::Millis,
        PqTimeUnit::MICROS => TimeUnit::Micros,
        PqTimeUnit::NANOS => TimeUnit::Nanos,
    }
}

/// The column's logical type as Go sees it: the Thrift logical type, else
/// Go `ConvertedType.ToLogicalType(decimalMeta)`.
fn logical_type_of(desc: &parquet::schema::types::ColumnDescriptor) -> LogicalType {
    if let Some(lt) = desc.logical_type_ref().cloned() {
        return match lt {
            PqLogicalType::String => LogicalType::String,
            PqLogicalType::Map => LogicalType::Map,
            PqLogicalType::List => LogicalType::List,
            PqLogicalType::Enum => LogicalType::Enum,
            PqLogicalType::Decimal { scale, precision } => LogicalType::Decimal { precision, scale },
            PqLogicalType::Date => LogicalType::Date,
            PqLogicalType::Time { is_adjusted_to_u_t_c, unit } => {
                LogicalType::Time { adjusted_to_utc: is_adjusted_to_u_t_c, unit: time_unit(&unit) }
            }
            PqLogicalType::Timestamp { is_adjusted_to_u_t_c, unit } => {
                LogicalType::Timestamp { adjusted_to_utc: is_adjusted_to_u_t_c, unit: time_unit(&unit) }
            }
            PqLogicalType::Integer { bit_width, is_signed } => LogicalType::Int { bit_width, signed: is_signed },
            PqLogicalType::Unknown => LogicalType::Null,
            PqLogicalType::Json => LogicalType::Json,
            PqLogicalType::Bson => LogicalType::Bson,
            PqLogicalType::Uuid => LogicalType::Uuid,
            PqLogicalType::Float16 => LogicalType::Float16,
            other => LogicalType::Unknown(format!("{other:?}")),
        };
    }
    let int = |bit_width, signed| LogicalType::Int { bit_width, signed };
    match desc.converted_type() {
        ConvertedType::NONE => LogicalType::None,
        ConvertedType::UTF8 => LogicalType::String,
        ConvertedType::MAP | ConvertedType::MAP_KEY_VALUE => LogicalType::Map,
        ConvertedType::LIST => LogicalType::List,
        ConvertedType::ENUM => LogicalType::Enum,
        ConvertedType::DECIMAL => LogicalType::Decimal { precision: desc.type_precision(), scale: desc.type_scale() },
        ConvertedType::DATE => LogicalType::Date,
        ConvertedType::TIME_MILLIS => LogicalType::Time { adjusted_to_utc: true, unit: TimeUnit::Millis },
        ConvertedType::TIME_MICROS => LogicalType::Time { adjusted_to_utc: true, unit: TimeUnit::Micros },
        ConvertedType::TIMESTAMP_MILLIS => LogicalType::Timestamp { adjusted_to_utc: true, unit: TimeUnit::Millis },
        ConvertedType::TIMESTAMP_MICROS => LogicalType::Timestamp { adjusted_to_utc: true, unit: TimeUnit::Micros },
        ConvertedType::UINT_8 => int(8, false),
        ConvertedType::UINT_16 => int(16, false),
        ConvertedType::UINT_32 => int(32, false),
        ConvertedType::UINT_64 => int(64, false),
        ConvertedType::INT_8 => int(8, true),
        ConvertedType::INT_16 => int(16, true),
        ConvertedType::INT_32 => int(32, true),
        ConvertedType::INT_64 => int(64, true),
        ConvertedType::JSON => LogicalType::Json,
        ConvertedType::BSON => LogicalType::Bson,
        ConvertedType::INTERVAL => LogicalType::Interval,
    }
}

/// Go `validateParquetLogicalType`.
fn validate_logical_type(lt: &LogicalType, physical: PhysicalType, type_length: i32) -> Result<(), String> {
    match lt {
        LogicalType::List
        | LogicalType::Map
        | LogicalType::Interval
        | LogicalType::Unknown(_)
        | LogicalType::Float16
        | LogicalType::Variant => return Err(format!("unsupported parquet logical type {}", lt.go_string())),
        _ => {}
    }
    let applicable = match lt {
        LogicalType::None | LogicalType::Null => true,
        LogicalType::String | LogicalType::Enum | LogicalType::Json | LogicalType::Bson => physical == PhysicalType::BYTE_ARRAY,
        LogicalType::Decimal { precision, .. } => match physical {
            PhysicalType::INT32 => (1..=9).contains(precision),
            PhysicalType::INT64 => (1..=18).contains(precision),
            PhysicalType::FIXED_LEN_BYTE_ARRAY => {
                f64::from(*precision) <= (2f64.powf(8.0 * f64::from(type_length) - 1.0)).log10().floor()
            }
            PhysicalType::BYTE_ARRAY => true,
            _ => false,
        },
        LogicalType::Date => physical == PhysicalType::INT32,
        LogicalType::Time { unit, .. } => {
            (physical == PhysicalType::INT32 && *unit == TimeUnit::Millis)
                || (physical == PhysicalType::INT64 && matches!(unit, TimeUnit::Micros | TimeUnit::Nanos))
        }
        LogicalType::Timestamp { .. } => physical == PhysicalType::INT64,
        LogicalType::Int { bit_width, .. } => {
            (physical == PhysicalType::INT32 && *bit_width <= 32) || (physical == PhysicalType::INT64 && *bit_width == 64)
        }
        LogicalType::Uuid => physical == PhysicalType::FIXED_LEN_BYTE_ARRAY && type_length == 16,
        _ => false,
    };
    if !applicable {
        return Err(format!("logical type {} is not applicable to physical type {physical}", lt.go_string()));
    }
    Ok(())
}

/// One column's batched reader (Go `columnIterator`).
struct ColumnIter<T: DataType> {
    reader: ColumnReaderImpl<T>,
    max_def_level: i16,
    values: Vec<T::T>,
    defs: Vec<i16>,
    level_pos: usize,
    value_pos: usize,
}

impl<T: DataType> ColumnIter<T> {
    fn new(reader: ColumnReaderImpl<T>, max_def_level: i16) -> Self {
        Self { reader, max_def_level, values: Vec::new(), defs: Vec::new(), level_pos: 0, value_pos: 0 }
    }

    /// The next value, `None` for NULL.
    fn next(&mut self) -> Result<Option<T::T>, String> {
        if self.level_pos >= self.defs.len() {
            self.values.clear();
            self.defs.clear();
            self.level_pos = 0;
            self.value_pos = 0;
            let (records, _, _) = self
                .reader
                .read_records(READ_BATCH_SIZE, Some(&mut self.defs), None, &mut self.values)
                .map_err(pq_err)?;
            if records == 0 {
                return Err("parquet column has fewer values than rows".to_owned());
            }
            if self.max_def_level == 0 {
                self.defs = vec![0; records];
            }
        }
        let def = self.defs[self.level_pos];
        self.level_pos += 1;
        if def < self.max_def_level {
            return Ok(None);
        }
        let v = self.values[self.value_pos].clone();
        self.value_pos += 1;
        Ok(Some(v))
    }
}

/// A column reader of any physical type.
enum AnyIter {
    Bool(ColumnIter<BoolType>),
    Int32(ColumnIter<Int32Type>),
    Int64(ColumnIter<Int64Type>),
    Int96(ColumnIter<Int96Type>),
    Float(ColumnIter<FloatType>),
    Double(ColumnIter<DoubleType>),
    ByteArray(ColumnIter<ByteArrayType>),
    Fixed(ColumnIter<FixedLenByteArrayType>),
}

impl AnyIter {
    fn new(reader: ColumnReader, max_def_level: i16) -> Self {
        match reader {
            r @ ColumnReader::BoolColumnReader(_) => Self::Bool(ColumnIter::new(get_typed_column_reader(r), max_def_level)),
            r @ ColumnReader::Int32ColumnReader(_) => Self::Int32(ColumnIter::new(get_typed_column_reader(r), max_def_level)),
            r @ ColumnReader::Int64ColumnReader(_) => Self::Int64(ColumnIter::new(get_typed_column_reader(r), max_def_level)),
            r @ ColumnReader::Int96ColumnReader(_) => Self::Int96(ColumnIter::new(get_typed_column_reader(r), max_def_level)),
            r @ ColumnReader::FloatColumnReader(_) => Self::Float(ColumnIter::new(get_typed_column_reader(r), max_def_level)),
            r @ ColumnReader::DoubleColumnReader(_) => Self::Double(ColumnIter::new(get_typed_column_reader(r), max_def_level)),
            r @ ColumnReader::ByteArrayColumnReader(_) => {
                Self::ByteArray(ColumnIter::new(get_typed_column_reader(r), max_def_level))
            }
            r @ ColumnReader::FixedLenByteArrayColumnReader(_) => {
                Self::Fixed(ColumnIter::new(get_typed_column_reader(r), max_def_level))
            }
        }
    }

    /// Go `columnIterator.Next`: the next value converted by the column's
    /// setter.
    fn next(&mut self, col: &ParquetColumnType, loc: &SessionTimeZone) -> Result<Datum, String> {
        use type_converter as tc;
        Ok(match self {
            Self::Bool(it) => it.next()?.map_or(Datum::Null, tc::bool_datum),
            Self::Int32(it) => match it.next()? {
                Some(v) => tc::int32_datum(col, loc, v)?,
                None => Datum::Null,
            },
            Self::Int64(it) => match it.next()? {
                Some(v) => tc::int64_datum(col, loc, v)?,
                None => Datum::Null,
            },
            Self::Int96(it) => match it.next()? {
                Some(v) => {
                    let d = v.data();
                    tc::int96_datum(col, loc, [d[0], d[1], d[2]])?
                }
                None => Datum::Null,
            },
            Self::Float(it) => it.next()?.map_or(Datum::Null, tc::float32_datum),
            Self::Double(it) => it.next()?.map_or(Datum::Null, tc::float64_datum),
            Self::ByteArray(it) => match it.next()? {
                Some(v) => tc::byte_array_datum(col, v.data())?,
                None => Datum::Null,
            },
            Self::Fixed(it) => match it.next()? {
                Some(v) => tc::fixed_len_byte_array_datum(col, v.data())?,
                None => Datum::Null,
            },
        })
    }
}

/// Go `rowGroupParser`.
struct RowGroupParser {
    read_rows: i64,
    total_rows: i64,
    iterators: Vec<AnyIter>,
}

/// Go `parsedef.Row`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Row {
    pub row_id: i64,
    pub row: Vec<Datum>,
    pub length: usize,
}

/// Go `Parser`.
pub struct Parser {
    reader: SerializedFileReader<StorageFile>,
    col_types: Vec<ParquetColumnType>,
    col_names: Vec<String>,
    max_def_levels: Vec<i16>,
    loc: SessionTimeZone,
    file_size: i64,
    row_group: Option<RowGroupParser>,
    cur_row_group: usize,
    total_row_group: usize,
    total_rows: i64,
    total_read_rows: i64,
    last_row: Row,
}

/// Go's `*time.Location` as the zone the datum layer reads.
#[must_use]
pub fn session_zone(zone: tidb_timeutil::TimeZone) -> SessionTimeZone {
    match zone {
        tidb_timeutil::TimeZone::Local => SessionTimeZone::Local,
        tidb_timeutil::TimeZone::Named(zone) => SessionTimeZone::Named(zone),
        tidb_timeutil::TimeZone::Fixed { name, offset_secs } => SessionTimeZone::Fixed { name, offset_secs },
    }
}

/// Go `estimateRowSize`.
fn estimate_row_size(row: &[Datum]) -> usize {
    row.iter()
        .map(|d| match d {
            Datum::Null => 0,
            Datum::String(s) => s.bytes().len(),
            _ => 8,
        })
        .sum()
}

impl Parser {
    /// Go `NewParser`: opens the file's footer and the first row group.
    pub fn new(ctx: &Context, store: Arc<dyn Storage>, path: &str, file_size: i64, meta: FileMeta) -> Result<Self, String> {
        let size = if file_size > 0 {
            file_size as u64
        } else {
            let mut r = store.open(ctx, path, None).map_err(|e| e.to_string())?;
            let size = r.get_file_size().map_err(|e| e.to_string())? as u64;
            let _ = r.close();
            size
        };
        let file = StorageFile { ctx: ctx.clone(), store, path: path.to_owned(), size };
        let reader = SerializedFileReader::new(file).map_err(pq_err)?;
        let file_meta = reader.metadata().file_metadata();
        let schema = file_meta.schema_descr();
        if schema.columns().iter().any(|c| c.max_rep_level() > 0) {
            return Err("nested or repeated Parquet fields are unsupported".to_owned());
        }
        let loc = meta.loc.unwrap_or_else(|| session_zone(tidb_timeutil::system_location()));
        // Go `fallback.String()`: the location's name.
        let fallback = match &loc {
            SessionTimeZone::Named(tz) => tz.name().to_owned(),
            SessionTimeZone::Fixed { name, .. } => name.clone(),
            SessionTimeZone::Local => "Local".to_owned(),
        };
        let kv: Vec<(&str, Option<&str>)> = file_meta
            .key_value_metadata()
            .map(|kv| kv.iter().map(|e| (e.key.as_str(), e.value.as_deref())).collect())
            .unwrap_or_default();
        let info = FileInfo { kv, created_by: file_meta.created_by() };
        let mut col_types = Vec::with_capacity(schema.num_columns());
        let mut col_names = Vec::with_capacity(schema.num_columns());
        let mut max_def_levels = Vec::with_capacity(schema.num_columns());
        for desc in schema.columns() {
            col_names.push(desc.name().to_lowercase());
            max_def_levels.push(desc.max_def_level());
            let logical_type = logical_type_of(desc);
            validate_logical_type(&logical_type, desc.physical_type(), desc.type_length())
                .map_err(|e| format!("column {:?}: {e}", desc.name()))?;
            let spark_rebase_micros = match desc.physical_type() {
                PhysicalType::INT32 if logical_type == LogicalType::Date => SparkRebaseMicrosLookup::from_metadata(
                    &info,
                    SPARK_DATETIME_REBASE_CUTOFF,
                    SPARK_LEGACY_DATE_TIME_METADATA_KEY,
                    &fallback,
                )?,
                PhysicalType::INT64
                    if matches!(logical_type, LogicalType::Timestamp { unit, .. } if unit != TimeUnit::Nanos) =>
                {
                    SparkRebaseMicrosLookup::from_metadata(
                        &info,
                        SPARK_DATETIME_REBASE_CUTOFF,
                        SPARK_LEGACY_DATE_TIME_METADATA_KEY,
                        &fallback,
                    )?
                }
                PhysicalType::INT96 => SparkRebaseMicrosLookup::from_metadata(
                    &info,
                    SPARK_INT96_REBASE_CUTOFF,
                    SPARK_LEGACY_INT96_METADATA_KEY,
                    &fallback,
                )?,
                _ => SparkRebaseMicrosLookup::default(),
            };
            col_types.push(ParquetColumnType { logical_type, spark_rebase_micros });
        }
        let total_row_group = reader.metadata().num_row_groups();
        let total_rows = file_meta.num_rows();
        let mut parser = Self {
            reader,
            col_types,
            col_names,
            max_def_levels,
            loc,
            file_size: size as i64,
            row_group: None,
            cur_row_group: 0,
            total_row_group,
            total_rows,
            total_read_rows: 0,
            last_row: Row::default(),
        };
        if total_row_group > 0 {
            parser.build_row_group_parser()?;
        }
        Ok(parser)
    }

    /// Go `buildRowGroupParser`.
    fn build_row_group_parser(&mut self) -> Result<(), String> {
        let rg = self.reader.get_row_group(self.cur_row_group).map_err(pq_err)?;
        let mut iterators = Vec::with_capacity(self.col_types.len());
        for (i, max_def) in self.max_def_levels.iter().enumerate() {
            iterators.push(AnyIter::new(rg.get_column_reader(i).map_err(pq_err)?, *max_def));
        }
        let total_rows = self.reader.metadata().row_group(self.cur_row_group).num_rows();
        self.row_group = Some(RowGroupParser { read_rows: 0, total_rows, iterators });
        Ok(())
    }

    /// Go `moveToNextRowGroup`; `Ok(false)` at the end of the file.
    fn move_to_next_row_group(&mut self) -> Result<bool, String> {
        self.row_group = None;
        self.cur_row_group += 1;
        if self.cur_row_group >= self.total_row_group {
            return Ok(false);
        }
        self.build_row_group_parser()?;
        Ok(true)
    }

    /// Go `readSingleRow`; `Ok(None)` at the end of the file.
    fn read_single_row(&mut self) -> Result<Option<Vec<Datum>>, String> {
        loop {
            match &self.row_group {
                Some(rg) if rg.read_rows < rg.total_rows => break,
                None if self.total_row_group == 0 => return Ok(None),
                _ => {
                    if !self.move_to_next_row_group()? {
                        return Ok(None);
                    }
                }
            }
        }
        let rg = self.row_group.as_mut().expect("a row group");
        let mut row = Vec::with_capacity(rg.iterators.len());
        for (col, iter) in rg.iterators.iter_mut().enumerate() {
            row.push(iter.next(&self.col_types[col], &self.loc).map_err(|e| format!("parquet read column failed: {e}"))?);
        }
        rg.read_rows += 1;
        self.total_read_rows += 1;
        Ok(Some(row))
    }

    /// Go `ReadRow`; `Ok(false)` at the end of the file (Go's `io.EOF`).
    pub fn read_row(&mut self) -> Result<bool, String> {
        self.last_row.row_id += 1;
        self.last_row.length = 0;
        let Some(row) = self.read_single_row()? else { return Ok(false) };
        let size = estimate_row_size(&row);
        if size > MAX_ROW_SIZE {
            return Err(ERR_ROW_TOO_LARGE.to_owned());
        }
        self.last_row.row = row;
        self.last_row.length = size;
        Ok(true)
    }

    /// Go `LastRow`.
    #[must_use]
    pub fn last_row(&self) -> &Row {
        &self.last_row
    }

    /// Go `Columns`: lowercased column names.
    #[must_use]
    pub fn columns(&self) -> &[String] {
        &self.col_names
    }

    /// Go `Pos`: rows read, and the last row ID.
    #[must_use]
    pub fn pos(&self) -> (i64, i64) {
        (self.total_read_rows, self.last_row.row_id)
    }

    /// Go `SetPos`: skips to row `pos`.
    pub fn set_pos(&mut self, pos: i64, row_id: i64) -> Result<(), String> {
        let to_read = pos - self.last_row.row_id;
        for _ in 0..to_read.max(0) {
            if self.read_single_row()?.is_none() {
                return Err("EOF".to_owned());
            }
        }
        self.last_row.row_id = row_id;
        Ok(())
    }

    /// Go `SetRowID`.
    pub fn set_row_id(&mut self, row_id: i64) {
        self.last_row.row_id = row_id;
    }

    /// Go `ScannedPos`: the read progress scaled to the file size.
    #[must_use]
    pub fn scanned_pos(&self) -> i64 {
        if self.total_rows <= 0 || self.total_read_rows == self.total_rows {
            return self.file_size;
        }
        (self.total_read_rows as f64 / self.total_rows as f64 * self.file_size as f64) as i64
    }
}

/// Go `ReadRowCount`.
pub fn read_row_count(ctx: &Context, store: Arc<dyn Storage>, path: &str) -> Result<i64, String> {
    let mut r = store.open(ctx, path, None).map_err(|e| e.to_string())?;
    let size = r.get_file_size().map_err(|e| e.to_string())? as u64;
    let _ = r.close();
    let file = StorageFile { ctx: ctx.clone(), store, path: path.to_owned(), size };
    let reader = SerializedFileReader::new(file).map_err(pq_err)?;
    Ok(reader.metadata().file_metadata().num_rows())
}

/// Go `SampleStatisticsFromParquet`: the file's row count and the average
/// size of its first (at most 1024) rows.
pub fn sample_statistics_from_parquet(ctx: &Context, path: &str, store: Arc<dyn Storage>) -> Result<(i64, f64), String> {
    let mut parser = Parser::new(ctx, store, path, 0, FileMeta::default())?;
    if parser.total_row_group == 0 || parser.reader.metadata().row_group(0).num_rows() == 0 {
        return Ok((0, 0.0));
    }
    let total_read_rows = parser.total_rows;
    let read_rows = total_read_rows.min(1024);
    let mut row_size = 0usize;
    let mut row_count = 0i64;
    for _ in 0..read_rows {
        if !parser.read_row()? {
            break;
        }
        row_size += parser.last_row.length;
        row_count += 1;
    }
    Ok((total_read_rows, row_size as f64 / row_count as f64))
}

#[cfg(test)]
mod tests;
