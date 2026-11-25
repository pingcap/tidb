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

use std::io::{self, BufReader, Cursor, Read, Seek, SeekFrom};
use std::ops::Range;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use parquet::basic::{ConvertedType, LogicalType as PqLogicalType, TimeUnit as PqTimeUnit, Type as PhysicalType};
use parquet::column::reader::{get_column_reader, get_typed_column_reader, ColumnReader, ColumnReaderImpl};
use parquet::data_type::{
    BoolType, ByteArrayType, DataType, DoubleType, FixedLenByteArrayType, FloatType, Int32Type, Int64Type, Int96Type,
};
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader, RowGroupMetaData};
use parquet::file::properties::ReaderProperties;
use parquet::file::reader::{ChunkReader, Length, RowGroupReader};
use parquet::file::serialized_reader::{SerializedPageReader, SerializedRowGroupReader};
use tidb_datatype::{Datum, SessionTimeZone};
use tidb_gocontext::Context;
use tidb_objstore::objectio::Reader;
use tidb_objstore::storeapi::{ReaderOption, Storage};

use spark_rebase::{
    FileInfo, SparkRebaseMicrosLookup, SPARK_DATETIME_REBASE_CUTOFF, SPARK_INT96_REBASE_CUTOFF,
    SPARK_LEGACY_DATE_TIME_METADATA_KEY, SPARK_LEGACY_INT96_METADATA_KEY,
};
use type_converter::{LogicalType, ParquetColumnType, TimeUnit};

/// Go `readBatchSize`.
const READ_BATCH_SIZE: usize = 128;
/// Go `maxRowSize` (`config.MaxTxnEntrySizeLimit`).
const MAX_ROW_SIZE: usize = 120 * 1024 * 1024;
/// Go `rowGroupInMemoryThreshold`.
const ROW_GROUP_BUFFER_LIMIT: u64 = 128 * 1024 * 1024;
const PAGE_HEADER_READ_BUFFER: u64 = 8 * 1024;
/// Go limits concurrent column reader construction in `buildRowGroupParser`.
const COLUMN_OPEN_LIMIT: usize = 8;

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
    path: Arc<str>,
    size: u64,
    buffered: Option<(u64, Bytes)>,
}

impl StorageFile {
    fn with_row_group(&self, group: &RowGroupMetaData) -> Result<Self, String> {
        let mut start = self.size;
        let mut end = 0;
        for col in group.columns() {
            let offset = col.dictionary_page_offset().unwrap_or_else(|| col.data_page_offset());
            let offset = u64::try_from(offset).map_err(|_| "negative Parquet column offset")?;
            let length = u64::try_from(col.compressed_size()).map_err(|_| "negative Parquet column length")?;
            let col_end = offset
                .checked_add(length)
                .filter(|&col_end| col_end <= self.size)
                .ok_or("Parquet column range exceeds file size")?;
            start = start.min(offset);
            end = end.max(col_end);
        }
        let mut file = self.clone();
        if let Some(length) = end.checked_sub(start).filter(|&length| length > 0 && length <= ROW_GROUP_BUFFER_LIMIT) {
            file.buffered = Some((start, self.get_bytes(start, length as usize).map_err(pq_err)?));
        }
        Ok(file)
    }

    fn buffered_from(&self, start: u64) -> Option<Bytes> {
        let (offset, bytes) = self.buffered.as_ref()?;
        let within = usize::try_from(start.checked_sub(*offset)?).ok()?;
        (within < bytes.len()).then(|| bytes.slice(within..))
    }
}

/// A page header reader that reuses the active row group or fetches a
/// bounded range on demand for larger groups.
struct StorageRead {
    file: StorageFile,
    position: u64,
    buffer: Cursor<Bytes>,
}

impl Read for StorageRead {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() || self.position >= self.file.size {
            return Ok(0);
        }
        if self.buffer.position() >= self.buffer.get_ref().len() as u64 {
            let length = (self.file.size - self.position).min(PAGE_HEADER_READ_BUFFER) as usize;
            let bytes = self.file.get_bytes(self.position, length).map_err(io::Error::other)?;
            self.buffer = Cursor::new(bytes);
        }
        let n = self.buffer.read(out)?;
        self.position += n as u64;
        Ok(n)
    }
}

impl Length for StorageFile {
    fn len(&self) -> u64 {
        self.size
    }
}

impl ChunkReader for StorageFile {
    type T = StorageRead;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        Ok(StorageRead {
            file: self.clone(),
            position: start,
            buffer: Cursor::new(self.buffered_from(start).unwrap_or_default()),
        })
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        if start.checked_add(length as u64).is_none_or(|end| end > self.size) {
            return Err(parquet::errors::ParquetError::EOF("Parquet range exceeds file size".to_owned()));
        }
        if let Some(buffer) = self.buffered_from(start).filter(|buffer| length <= buffer.len()) {
            return Ok(buffer.slice(..length));
        }
        let mut buf = vec![0; length];
        if length > 0 {
            tidb_objstore::read_data_in_range(&self.ctx, self.store.as_ref(), &self.path, start as i64, &mut buf)
                .map_err(|e| parquet::errors::ParquetError::External(Box::new(e)))?;
        }
        Ok(Bytes::from(buf))
    }
}

/// A bounded storage stream shared by reads of file metadata or one
/// column chunk.
#[derive(Clone)]
struct StorageRange {
    range: Range<u64>,
    stream: Arc<Mutex<RangeStream>>,
}

struct RangeStream {
    reader: BufReader<Box<dyn Reader>>,
    position: u64,
}

impl Drop for RangeStream {
    fn drop(&mut self) {
        let _ = self.reader.get_mut().close();
    }
}

impl StorageRange {
    fn new(range: Range<u64>, reader: Box<dyn Reader>) -> Self {
        let stream = Arc::new(Mutex::new(RangeStream {
            reader: BufReader::new(reader),
            position: range.start,
        }));
        Self { range, stream }
    }

    fn open(file: &StorageFile, range: Range<u64>) -> Result<Self, String> {
        let reader = file
            .store
            .open(
                &file.ctx,
                &file.path,
                Some(&ReaderOption {
                    start_offset: Some(range.start as i64),
                    end_offset: Some(range.end as i64),
                    prefetch_size: 0,
                }),
            )
            .map_err(|error| error.to_string())?;
        Ok(Self::new(range, reader))
    }
}

struct RangeRead {
    file: StorageRange,
    position: u64,
}

impl Read for RangeRead {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        let length = out.len().min(self.file.range.end.saturating_sub(self.position) as usize);
        if length == 0 {
            return Ok(0);
        }
        let mut stream = self
            .file
            .stream
            .lock()
            .map_err(|_| io::Error::other("Parquet range stream lock poisoned"))?;
        if stream.position != self.position {
            stream.reader.seek(SeekFrom::Start(self.position))?;
            stream.position = self.position;
        }
        let n = stream.reader.read(&mut out[..length])?;
        stream.position += n as u64;
        self.position += n as u64;
        Ok(n)
    }
}

impl Length for StorageRange {
    fn len(&self) -> u64 {
        self.range.end
    }
}

impl ChunkReader for StorageRange {
    type T = RangeRead;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        if start < self.range.start || start > self.range.end {
            return Err(parquet::errors::ParquetError::EOF(
                "Parquet read starts outside storage range".to_owned(),
            ));
        }
        Ok(RangeRead { file: self.clone(), position: start })
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        if start.checked_add(length as u64).is_none_or(|end| end > self.range.end) {
            return Err(parquet::errors::ParquetError::EOF(
                "Parquet read exceeds storage range".to_owned(),
            ));
        }
        let mut reader = self.get_read(start)?;
        let mut bytes = vec![0; length];
        reader.read_exact(&mut bytes)?;
        Ok(Bytes::from(bytes))
    }
}

fn open_columns(file: &StorageFile, metadata: &RowGroupMetaData) -> Result<Vec<StorageRange>, String> {
    let mut columns = Vec::with_capacity(metadata.num_columns());
    for batch in metadata.columns().chunks(COLUMN_OPEN_LIMIT) {
        let opened = std::thread::scope(|scope| {
            let handles: Vec<_> = batch
                .iter()
                .map(|column| {
                    // StorageFile already validated these bounds for the row group.
                    let (start, length) = column.byte_range();
                    scope.spawn(move || StorageRange::open(file, start..start + length))
                })
                .collect();
            handles
                .into_iter()
                .map(|handle| {
                    handle
                        .join()
                        .map_err(|_| "Parquet column open panicked".to_owned())?
                })
                .collect::<Vec<_>>()
        });
        for column in opened {
            columns.push(column?);
        }
    }
    Ok(columns)
}

fn read_metadata(ctx: &Context, store: &dyn Storage, path: &str, file_size: i64) -> Result<(u64, ParquetMetaData), String> {
    let mut reader = store.open(ctx, path, None).map_err(|error| error.to_string())?;
    let size = if file_size > 0 {
        file_size as u64
    } else {
        match reader.get_file_size() {
            Ok(size) if size >= 0 => size as u64,
            Ok(_) => {
                let _ = reader.close();
                return Err("negative Parquet file size".to_owned());
            }
            Err(error) => {
                let _ = reader.close();
                return Err(error.to_string());
            }
        }
    };
    let file = StorageRange::new(0..size, reader);
    let metadata = ParquetMetaDataReader::new().parse_and_finish(&file).map_err(pq_err)?;
    Ok((size, metadata))
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
    file: StorageFile,
    metadata: ParquetMetaData,
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
    row_buffer: Vec<Datum>,
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

impl Parser {
    /// Go `NewParser`: opens the file's footer and the first row group.
    pub fn new(ctx: &Context, store: Arc<dyn Storage>, path: &str, file_size: i64, meta: FileMeta) -> Result<Self, String> {
        let (size, metadata) = read_metadata(ctx, store.as_ref(), path, file_size)?;
        let file = StorageFile { ctx: ctx.clone(), store, path: Arc::from(path), size, buffered: None };
        let file_meta = metadata.file_metadata();
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
        let total_row_group = metadata.num_row_groups();
        let total_rows = file_meta.num_rows();
        let mut parser = Self {
            file,
            metadata,
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
            row_buffer: Vec::new(),
        };
        if total_row_group > 0 {
            parser.build_row_group_parser()?;
        }
        Ok(parser)
    }

    /// Go `buildRowGroupParser`.
    fn build_row_group_parser(&mut self) -> Result<(), String> {
        let metadata = self.metadata.row_group(self.cur_row_group);
        let file = self.file.with_row_group(metadata)?;
        let props = Arc::new(ReaderProperties::builder().build());
        let total_rows = metadata.num_rows();
        let mut iterators = Vec::with_capacity(self.col_types.len());
        if file.buffered.is_some() || total_rows == 0 {
            let rg = SerializedRowGroupReader::new(Arc::new(file), metadata, None, props).map_err(pq_err)?;
            for (i, max_def) in self.max_def_levels.iter().enumerate() {
                iterators.push(AnyIter::new(rg.get_column_reader(i).map_err(pq_err)?, *max_def));
            }
        } else {
            let rows = usize::try_from(total_rows).map_err(|_| "invalid Parquet row-group row count")?;
            let streams = open_columns(&file, metadata)?;
            for ((column, max_def), stream) in metadata.columns().iter().zip(&self.max_def_levels).zip(streams) {
                let page_reader = SerializedPageReader::new_with_properties(
                    Arc::new(stream), column, rows, None, Arc::clone(&props),
                ).map_err(pq_err)?;
                let reader = get_column_reader(column.column_descr_ptr(), Box::new(page_reader));
                iterators.push(AnyIter::new(reader, *max_def));
            }
        }
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

    /// Go `readSingleRow`; decodes into the spare row buffer and returns its
    /// estimated byte length. `Ok(None)` means end of file.
    fn read_single_row(&mut self) -> Result<Option<usize>, String> {
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
        self.row_buffer.reserve(rg.iterators.len());
        let mut size = 0;
        for (col, iter) in rg.iterators.iter_mut().enumerate() {
            let datum = iter
                .next(&self.col_types[col], &self.loc)
                .map_err(|e| format!("parquet read column failed: {e}"))?;
            size += match &datum {
                Datum::Null => 0,
                Datum::String(s) => s.bytes().len(),
                _ => 8,
            };
            self.row_buffer.push(datum);
        }
        rg.read_rows += 1;
        self.total_read_rows += 1;
        Ok(Some(size))
    }

    /// Go `ReadRow`; `Ok(false)` at the end of the file (Go's `io.EOF`).
    pub fn read_row(&mut self) -> Result<bool, String> {
        self.last_row.row_id += 1;
        self.last_row.length = 0;
        let result = match self.read_single_row() {
            Ok(Some(size)) if size <= MAX_ROW_SIZE => {
                std::mem::swap(&mut self.last_row.row, &mut self.row_buffer);
                self.last_row.length = size;
                Ok(true)
            }
            Ok(Some(_)) => Err(ERR_ROW_TOO_LARGE.to_owned()),
            Ok(None) => Ok(false),
            Err(error) => Err(error),
        };
        self.row_buffer.clear();
        result
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
            let result = self.read_single_row();
            self.row_buffer.clear();
            if result?.is_none() {
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
    let (_, metadata) = read_metadata(ctx, store.as_ref(), path, 0)?;
    Ok(metadata.file_metadata().num_rows())
}

/// Go `SampleStatisticsFromParquet`: the file's row count and the average
/// size of its first (at most 1024) rows.
pub fn sample_statistics_from_parquet(ctx: &Context, path: &str, store: Arc<dyn Storage>) -> Result<(i64, f64), String> {
    let mut parser = Parser::new(ctx, store, path, 0, FileMeta::default())?;
    if parser.total_row_group == 0 || parser.metadata.row_group(0).num_rows() == 0 {
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
