//! Go `pkg/lightning/mydump/region.go`: data files split into chunks
//! (table regions) and grouped into engines.

use std::sync::Arc;

use tidb_gocontext::Context;
use tidb_objstore::storeapi::Storage;

use crate::{Chunk, Compression, CsvConfig, CsvParser, FileInfo, MDTableMeta, SourceFileMeta, SourceType};

/// Go `tableRegionSizeWarningThreshold`.
const TABLE_REGION_SIZE_WARNING_THRESHOLD: i64 = 1024 * 1024 * 1024;
/// Go `largeCSVLowerThresholdRation`.
const LARGE_CSV_LOWER_THRESHOLD_RATION: i64 = 10;
/// Go `TableFileSizeINF`: the end offset of a compressed file's region.
pub const TABLE_FILE_SIZE_INF: i64 = 10 * 1024 * TABLE_REGION_SIZE_WARNING_THRESHOLD;
/// Go `CompressSizeFactor`.
pub const COMPRESS_SIZE_FACTOR: i64 = 5;
/// Go `config.DefaultBatchSize`.
pub const DEFAULT_BATCH_SIZE: i64 = 100 * 1024 * 1024 * 1024;
/// Go `config.MaxRegionSize`.
pub const MAX_REGION_SIZE: i64 = 256 * 1024 * 1024;

/// Go `TableRegion`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TableRegion {
    pub engine_id: i32,
    pub db: String,
    pub table: String,
    pub file_meta: SourceFileMeta,
    pub extend_data: crate::ExtendColumnData,
    pub chunk: Chunk,
}

impl TableRegion {
    /// Go `RowIDMin`.
    #[must_use]
    pub fn row_id_min(&self) -> i64 {
        self.chunk.prev_row_id_max + 1
    }

    /// Go `Rows`.
    #[must_use]
    pub fn rows(&self) -> i64 {
        self.chunk.row_id_max - self.chunk.prev_row_id_max
    }

    /// Go `Offset`.
    #[must_use]
    pub fn offset(&self) -> i64 {
        self.chunk.offset
    }

    /// Go `Size`.
    #[must_use]
    pub fn size(&self) -> i64 {
        self.chunk.end_offset - self.chunk.offset
    }
}

/// Go `math.Lgamma`'s value (the sign is not needed for positive inputs).
fn lgamma(x: f64) -> f64 {
    // Lanczos approximation (g = 7, n = 9), accurate to ~1e-15 for x > 0,
    // which is where Go calls it.
    const G: f64 = 7.0;
    const COEF: [f64; 9] = [
        0.999_999_999_999_809_9,
        676.520_368_121_885_1,
        -1_259.139_216_722_402_8,
        771.323_428_777_653_1,
        -176.615_029_162_140_6,
        12.507_343_278_686_905,
        -0.138_571_095_265_720_12,
        9.984_369_578_019_572e-6,
        1.505_632_735_149_311_6e-7,
    ];
    if x < 0.5 {
        let pi = std::f64::consts::PI;
        return (pi / (pi * x).sin()).abs().ln() - lgamma(1.0 - x);
    }
    let x = x - 1.0;
    let mut a = COEF[0];
    let t = x + G + 0.5;
    for (i, c) in COEF.iter().enumerate().skip(1) {
        a += c / (x + i as f64);
    }
    0.5 * (2.0 * std::f64::consts::PI).ln() + (x + 0.5) * t.ln() - t + a.ln()
}

/// Go `AllocateEngineIDs`.
pub fn allocate_engine_ids(
    files_regions: &mut [TableRegion],
    data_file_sizes: &[f64],
    batch_size: f64,
    batch_import_ratio: f64,
    engine_concurrency: f64,
) {
    let total_data_file_size: f64 = data_file_sizes.iter().sum();
    if total_data_file_size <= batch_size {
        return;
    }
    let mut cur_engine_id = 0i32;
    let mut cur_engine_size = 0.0;
    let mut cur_batch_size = batch_size;
    let ratio = total_data_file_size * (1.0 - batch_import_ratio) / batch_size;
    let mut n = ratio.ceil();
    let mut inv_beta_nr = (lgamma(n + batch_import_ratio) - lgamma(n) - lgamma(batch_import_ratio)).exp();
    loop {
        if n <= 0.0 || n > engine_concurrency {
            n = engine_concurrency;
            break;
        }
        let real_ratio = n - inv_beta_nr;
        if real_ratio >= ratio {
            cur_batch_size = total_data_file_size * (1.0 - batch_import_ratio) / real_ratio;
            break;
        }
        inv_beta_nr *= 1.0 + batch_import_ratio / n;
        n += 1.0;
    }
    for (i, data_file_size) in data_file_sizes.iter().enumerate() {
        files_regions[i].engine_id = cur_engine_id;
        cur_engine_size += data_file_size;
        if cur_engine_size >= cur_batch_size {
            cur_engine_size = 0.0;
            cur_engine_id += 1;
            let i = f64::from(cur_engine_id);
            if i >= n {
                cur_batch_size = batch_size;
            } else {
                cur_batch_size *= batch_import_ratio / (n - i) + 1.0;
            }
        }
    }
}

/// Go `DataDivideConfig`.
#[derive(Clone)]
pub struct DataDivideConfig {
    pub column_cnt: usize,
    pub engine_data_size: i64,
    pub max_chunk_size: i64,
    pub concurrency: usize,
    pub engine_concurrency: usize,
    pub batch_import_ratio: f64,
    pub store: Arc<dyn Storage>,
    pub table_meta: MDTableMeta,
    pub skip_parquet_row_count: bool,
    pub strict_format: bool,
    pub data_character_set: String,
    pub data_invalid_char_replace: String,
    pub read_block_size: i64,
    pub csv: CsvConfig,
}

/// Go `CalculateBatchSize`.
#[must_use]
pub fn calculate_batch_size(mydumper_batch_size: f64, is_row_ordered: bool, total_size: f64) -> f64 {
    if mydumper_batch_size > 0.0 {
        return mydumper_batch_size;
    }
    if is_row_ordered {
        DEFAULT_BATCH_SIZE as f64
    } else {
        (DEFAULT_BATCH_SIZE as f64).max(total_size)
    }
}

type Regions = (Vec<TableRegion>, Vec<f64>);

/// Go `MakeTableRegions`.
pub fn make_table_regions(ctx: &Context, cfg: &DataDivideConfig) -> Result<Vec<TableRegion>, String> {
    let concurrency = cfg.concurrency.max(2);
    let meta = &cfg.table_meta;
    let parallel_split_threshold = cfg.max_chunk_size * concurrency as i64;
    // `None` marks a large CSV split afterwards with parallel readers.
    let results: Vec<Option<Regions>> = crate::parallel_process(ctx, &meta.data_files, concurrency, |ctx, info| {
        let fm = &info.file_meta;
        if fm.r#type == SourceType::Parquet {
            make_parquet_file_region(ctx, cfg, info).map(Some)
        } else if fm.r#type == SourceType::CSV
            && cfg.strict_format
            && fm.compression == Compression::None
            && fm.file_size > cfg.max_chunk_size + cfg.max_chunk_size / LARGE_CSV_LOWER_THRESHOLD_RATION
        {
            if fm.file_size > parallel_split_threshold {
                return Ok(None);
            }
            split_large_csv(ctx, cfg, info, false).map(Some)
        } else {
            make_source_file_region(ctx, cfg, info).map(Some)
        }
    })?;
    let mut files_regions = Vec::with_capacity(meta.data_files.len());
    let mut data_file_sizes = Vec::with_capacity(meta.data_files.len());
    let mut row_id_base = 0;
    for (data_file, result) in meta.data_files.iter().zip(results) {
        let (mut regions, sizes) = match result {
            Some(r) => r,
            None => split_large_csv(ctx, cfg, data_file, true)?,
        };
        for region in &mut regions {
            region.chunk.prev_row_id_max += row_id_base;
            region.chunk.row_id_max += row_id_base;
        }
        if let Some(last) = regions.last() {
            row_id_base = last.chunk.row_id_max;
        }
        files_regions.extend(regions);
        data_file_sizes.extend(sizes);
    }
    let batch_size = calculate_batch_size(cfg.engine_data_size as f64, meta.is_row_ordered, meta.total_size as f64);
    allocate_engine_ids(
        &mut files_regions,
        &data_file_sizes,
        batch_size,
        cfg.batch_import_ratio,
        cfg.engine_concurrency as f64,
    );
    Ok(files_regions)
}

/// Go `MakeSourceFileRegion`: one region for the whole file.
pub fn make_source_file_region(_ctx: &Context, cfg: &DataDivideConfig, fi: &FileInfo) -> Result<Regions, String> {
    let mut divisor = cfg.column_cnt as i64;
    if fi.file_meta.r#type != SourceType::CSV {
        divisor += 2;
    }
    let mut file_size = fi.file_meta.file_size;
    let mut row_id_max = file_size / divisor;
    if fi.file_meta.compression != Compression::None {
        row_id_max = fi.file_meta.real_size * COMPRESS_SIZE_FACTOR / divisor;
        file_size = TABLE_FILE_SIZE_INF;
    }
    let region = TableRegion {
        db: cfg.table_meta.db.clone(),
        table: cfg.table_meta.name.clone(),
        file_meta: fi.file_meta.clone(),
        chunk: Chunk { offset: 0, end_offset: file_size, real_offset: 0, prev_row_id_max: 0, row_id_max, columns: Vec::new() },
        ..TableRegion::default()
    };
    Ok((vec![region], vec![fi.file_meta.real_size as f64]))
}

/// Go `makeParquetFileRegion`.
fn make_parquet_file_region(ctx: &Context, cfg: &DataDivideConfig, data_file: &FileInfo) -> Result<Regions, String> {
    let mut number_rows = data_file.file_meta.rows;
    if !cfg.skip_parquet_row_count {
        number_rows = crate::parquet_row_count(ctx, cfg.store.as_ref(), &data_file.file_meta.path)?;
    } else {
        if number_rows <= 0 {
            number_rows = data_file.file_meta.file_size;
        }
        if let Some(v) = tidb_failpoint::eval("github.com/pingcap/tidb/pkg/lightning/mydump/mockParquetRowCount") {
            if let Some(v) = v.as_int() {
                number_rows = v;
            }
        }
    }
    let region = TableRegion {
        db: cfg.table_meta.db.clone(),
        table: cfg.table_meta.name.clone(),
        file_meta: data_file.file_meta.clone(),
        chunk: Chunk {
            offset: 0,
            end_offset: i64::MAX,
            real_offset: 0,
            prev_row_id_max: 0,
            row_id_max: number_rows,
            columns: Vec::new(),
        },
        ..TableRegion::default()
    };
    Ok((vec![region], vec![data_file.file_meta.real_size as f64]))
}

/// Go `openCSVParser`: a CSV parser positioned at `offset`.
fn open_csv_parser(ctx: &Context, cfg: &DataDivideConfig, path: &str, offset: i64) -> Result<CsvParser, String> {
    let r = cfg.store.open(ctx, path, None).map_err(|e| e.to_string())?;
    let convertor = crate::CharsetConvertor::new(&cfg.data_character_set, &cfg.data_invalid_char_replace)?;
    let mut parser = CsvParser::new(
        &cfg.csv,
        Box::new(crate::ParserReader(r)),
        cfg.read_block_size as usize,
        true,
        Some(convertor),
    )
    .map_err(|e| e.to_string())?;
    parser.set_pos(offset, 0).map_err(|e| e.to_string())?;
    Ok(parser)
}

/// Go `getHeaderColumn`.
fn get_header_column(ctx: &Context, cfg: &DataDivideConfig, path: &str) -> Result<(Vec<String>, i64), String> {
    if !cfg.csv.header || !cfg.csv.header_schema_match {
        return Ok((Vec::new(), 0));
    }
    let mut parser = open_csv_parser(ctx, cfg, path, 0)?;
    parser.read_columns().map_err(|e| e.to_string())?;
    let (start_offset, _) = parser.pos();
    Ok((parser.columns().to_vec(), start_offset))
}

/// Go `mathutil.Divide2Batches`.
fn divide_2_batches(mut total: i64, batches: i64) -> Vec<i64> {
    let mut result = Vec::with_capacity(batches.max(0) as usize);
    if batches <= 0 {
        return result;
    }
    let quotient = total / batches;
    let mut remainder = total % batches;
    while total > 0 {
        let mut size = quotient;
        if remainder > 0 {
            size += 1;
            remainder -= 1;
        }
        result.push(size);
        total -= size;
    }
    result
}

/// Go `SplitLargeCSV`: regions of about `MaxChunkSize`, each ending after a
/// line terminator.
pub fn split_large_csv(ctx: &Context, cfg: &DataDivideConfig, data_file: &FileInfo, parallel: bool) -> Result<Regions, String> {
    let max_region_size = cfg.max_chunk_size;
    let (header_columns, data_start) = get_header_column(ctx, cfg, &data_file.file_meta.path)?;
    let file_size = data_file.file_meta.file_size;
    let region_cnt = (file_size - data_start + max_region_size - 1) / max_region_size;
    let mut split_points = divide_2_batches(file_size - data_start, region_cnt);
    split_points.pop();
    for i in 0..split_points.len() {
        if i == 0 {
            split_points[i] += data_start;
        } else {
            split_points[i] += split_points[i - 1];
        }
    }
    let concurrency = if parallel { cfg.concurrency.max(1) } else { 1 };
    let path = data_file.file_meta.path.clone();
    let split_points = crate::parallel_process(ctx, &split_points, concurrency, |ctx, split_point| {
        let mut parser = open_csv_parser(ctx, cfg, &path, *split_point)?;
        match parser.read_until_terminator() {
            Ok(pos) => Ok(pos),
            Err(crate::CsvError::Eof) => Ok(file_size),
            Err(e) => Err(e.to_string()),
        }
    })?;
    let mut split_points = split_points;
    split_points.push(file_size);
    let divisor = cfg.column_cnt as i64;
    let mut prev_row_idx_max = 0;
    let mut regions = Vec::new();
    let mut data_file_sizes = Vec::new();
    for i in 0..split_points.len() {
        let start_offset = if i > 0 { split_points[i - 1] } else { data_start };
        let end_offset = split_points[i];
        if start_offset == end_offset {
            continue;
        }
        let cur_rows_cnt = (end_offset - start_offset) / divisor;
        let row_id_max = prev_row_idx_max + cur_rows_cnt;
        regions.push(TableRegion {
            db: cfg.table_meta.db.clone(),
            table: cfg.table_meta.name.clone(),
            file_meta: data_file.file_meta.clone(),
            chunk: Chunk {
                offset: start_offset,
                end_offset,
                real_offset: 0,
                prev_row_id_max: prev_row_idx_max,
                row_id_max,
                columns: header_columns.clone(),
            },
            ..TableRegion::default()
        });
        data_file_sizes.push((end_offset - start_offset) as f64);
        prev_row_idx_max = row_id_max;
    }
    Ok((regions, data_file_sizes))
}
