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

//! Go `pkg/executor/slow_query.go`'s `slowQueryRetriever`: the rows of
//! `INFORMATION_SCHEMA.SLOW_QUERY`, parsed from the instance's slow-log file
//! and its rotated siblings (`SessionVars.SlowQueryFile`).
//!
//! The files are located the way Go's `getAllFiles` does (same directory,
//! same name prefix, ordered by their first `# Time:`, pruned by the
//! extractor's time ranges), and every entry is parsed by Go's `parseLog`
//! state machine and `getColumnValueFactoryByName` column factories. Go parses
//! batches of 64 entries concurrently and, for a descending scan, reads the
//! files backwards; both only change the order and the pace of the same rows,
//! so this reader parses the files once, front to back, and the cluster scan
//! that pushes a descending Limit orders its rows itself.

use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::Path;
use std::time::Duration;

use chrono::{DateTime, FixedOffset, Utc};
use tidb_datatype::{
    core_time_from_datetime, BinaryJSON, Datum, FieldType, SessionTimeZone, Time, TimeType,
};

/// Go `variable.SlowLogStartPrefixStr`.
const SLOW_LOG_START_PREFIX_STR: &str = "# Time: ";
/// Go `variable.SlowLogRowPrefixStr`.
const SLOW_LOG_ROW_PREFIX_STR: &str = "# ";
/// Go `variable.SlowLogSQLSuffixStr`.
const SLOW_LOG_SQL_SUFFIX_STR: &str = ";";
/// Go `variable.SlowLogPrevStmtPrefix`.
const SLOW_LOG_PREV_STMT_PREFIX: &str = "Prev_stmt: ";
/// Go `variable.SlowLogPlanPrefix`.
const SLOW_LOG_PLAN_PREFIX: &str = "tidb_decode_plan('";
/// Go `variable.SlowLogBinaryPlanPrefix`.
const SLOW_LOG_BINARY_PLAN_PREFIX: &str = "tidb_decode_binary_plan('";
/// Go `variable.SlowLogPlanSuffix`.
const SLOW_LOG_PLAN_SUFFIX: &str = "')";

/// Go `slowLogTimeRangeInternalTolerance`: widens only the file pruning; rows
/// are still filtered by the original ranges.
const SLOW_LOG_TIME_RANGE_INTERNAL_TOLERANCE: Duration = Duration::from_secs(1);

/// Go `vardef.MaxOfMaxAllowedPacket`, the longest slow-log line read.
const MAX_LINE_SIZE: usize = 1 << 30;

/// Go `slowLogChecker`: who may see an entry and when it must have run.
#[derive(Clone, Debug, Default)]
pub struct SlowLogChecker {
    /// The viewer holds `PROCESS`, which shows every user's entries.
    pub has_process_priv: bool,
    /// The viewer's user name; `None` (Go's nil user) sees every entry.
    pub user: Option<String>,
    /// Go `SlowQueryExtractor.Enable`: the time ranges filter entries and
    /// prune files.
    pub enable_time_check: bool,
    /// Go `SlowQueryExtractor.TimeRanges`, inclusive on both ends.
    pub time_ranges: Vec<(DateTime<Utc>, DateTime<Utc>)>,
}

impl SlowLogChecker {
    /// Go `hasPrivilege`.
    fn has_privilege(&self, user_name: &str) -> bool {
        self.has_process_priv || self.user.as_deref().is_none_or(|user| user == user_name)
    }

    /// Go `isTimeValid`.
    fn is_time_valid(&self, t: DateTime<Utc>) -> bool {
        if !self.enable_time_check {
            return true;
        }
        self.time_ranges
            .iter()
            .any(|(start, end)| t >= *start && t <= *end)
    }

    /// Go `slowLogMayOverlapTimeRangeWithTolerance`.
    fn may_overlap(&self, start: DateTime<Utc>, end: DateTime<Utc>) -> bool {
        self.time_ranges.iter().any(|(range_start, range_end)| {
            let range_start = *range_start - SLOW_LOG_TIME_RANGE_INTERNAL_TOLERANCE;
            let range_end = *range_end + SLOW_LOG_TIME_RANGE_INTERNAL_TOLERANCE;
            !(start > range_end || end < range_start)
        })
    }
}

/// Go `logFile`.
struct LogFile {
    path: String,
    start: DateTime<Utc>,
    compressed: bool,
}

/// Go `ParseTime`: RFC3339Nano, then the old `2006-01-02-15:04:05.999999999
/// -0700` layout.
pub fn parse_time(s: &str) -> Result<DateTime<FixedOffset>, String> {
    if let Ok(t) = DateTime::parse_from_rfc3339(s) {
        return Ok(t);
    }
    DateTime::parse_from_str(s, "%Y-%m-%d-%H:%M:%S%.f %z").map_err(|error| {
        format!(
            "string \"{s}\" doesn't has a prefix that matches format \"2006-01-02T15:04:05.999999999Z07:00\", err: {error}"
        )
    })
}

/// Go `util.ReadLine` through `getOneLine`: one line without its `\n` or
/// `\r\n`; `None` at the end of the input.
fn read_line(reader: &mut dyn BufRead) -> Result<Option<String>, String> {
    let mut buf = Vec::new();
    let read = reader
        .read_until(b'\n', &mut buf)
        .map_err(|error| error.to_string())?;
    if read == 0 {
        return Ok(None);
    }
    if buf.last() == Some(&b'\n') {
        buf.pop();
        if buf.last() == Some(&b'\r') {
            buf.pop();
        }
    }
    if buf.len() > MAX_LINE_SIZE {
        return Err(format!("single line length exceeds limit: {MAX_LINE_SIZE}"));
    }
    Ok(Some(String::from_utf8_lossy(&buf).into_owned()))
}

/// Opens a log file for line reading, decompressing a `.gz` rotation.
fn open_reader(path: &str, compressed: bool) -> Result<Box<dyn BufRead>, String> {
    let file = std::fs::File::open(path).map_err(|error| go_open_error(path, &error))?;
    if compressed {
        Ok(Box::new(BufReader::new(flate2::read::GzDecoder::new(file))))
    } else {
        Ok(Box::new(BufReader::new(file)))
    }
}

/// Go's `*PathError` text for a failed open or read-dir.
fn go_open_error(path: &str, error: &std::io::Error) -> String {
    let text = match error.kind() {
        std::io::ErrorKind::NotFound => "no such file or directory".to_owned(),
        std::io::ErrorKind::PermissionDenied => "permission denied".to_owned(),
        _ => error.to_string(),
    };
    format!("open {path}: {text}")
}

/// Go `getFileStartTime`: the first `# Time:` among the file's first 128
/// lines.
fn file_start_time(path: &str, compressed: bool) -> Result<Option<DateTime<FixedOffset>>, String> {
    let mut reader = open_reader(path, compressed)?;
    for _ in 0..128 {
        let Some(line) = read_line(reader.as_mut())? else {
            // Go's `io.EOF`, which the caller skips without a warning.
            return Ok(None);
        };
        if let Some(value) = line.strip_prefix(SLOW_LOG_START_PREFIX_STR) {
            return parse_time(value).map(Some);
        }
    }
    Err(format!("malform slow query file {path}"))
}

/// Go `maxReadCacheSize`.
const MAX_READ_CACHE_SIZE: u64 = 1024 * 1024 * 64;

/// Go `readLastLines`: the whole lines that end at `end_cursor`, growing the
/// window backwards until it holds a line break that starts a non-empty
/// line, and the bytes they span (0 at the file's start).
fn read_last_lines(file: &mut std::fs::File, end_cursor: u64) -> Result<(Vec<String>, usize), String> {
    let mut lines: Vec<u8> = Vec::new();
    let mut first_non_newline = 0usize;
    let mut cursor = end_cursor;
    let mut size: u64 = 2048;
    loop {
        // stop if we are at the beginning
        if cursor == 0 {
            break;
        }
        if size < MAX_READ_CACHE_SIZE {
            size *= 2;
        }
        if cursor < size {
            size = cursor;
        }
        cursor -= size;
        file.seek(SeekFrom::Start(cursor))
            .map_err(|error| error.to_string())?;
        let mut chars = vec![0u8; size as usize];
        file.read_exact(&mut chars)
            .map_err(|error| error.to_string())?;
        let scanned = chars.len();
        chars.extend_from_slice(&lines);
        lines = chars;
        // find first '\n' or '\r'
        for i in 0..scanned.saturating_sub(1) {
            if (lines[i] == b'\n' || lines[i] == b'\r') && lines[i + 1] != b'\n' && lines[i + 1] != b'\r' {
                first_non_newline = i + 1;
                break;
            }
        }
        if first_non_newline > 0 {
            break;
        }
    }
    let tail = &lines[first_non_newline..];
    Ok((split_slow_log_lines(&String::from_utf8_lossy(tail)), tail.len()))
}

/// Go `splitSlowLogLines`.
fn split_slow_log_lines(s: &str) -> Vec<String> {
    let mut lines: Vec<String> = s.replace("\r\n", "\n").split('\n').map(str::to_owned).collect();
    // strip the last empty string if the ending is new line symbol.
    if lines.last().is_some_and(String::is_empty) {
        lines.pop();
    }
    lines
}

/// Go `getFileEndTime`: the last `# Time:` among the file's last 128 lines.
fn file_end_time(path: &str) -> Result<DateTime<FixedOffset>, String> {
    let mut file = std::fs::File::open(path).map_err(|error| go_open_error(path, &error))?;
    let mut end_cursor = file.metadata().map_err(|error| error.to_string())?.len();
    let mut tried = 0usize;
    loop {
        let (lines, read_bytes) = read_last_lines(&mut file, end_cursor)?;
        // read out the file
        if read_bytes == 0 {
            break;
        }
        end_cursor -= read_bytes as u64;
        for line in lines.iter().rev() {
            if let Some(value) = line.strip_prefix(SLOW_LOG_START_PREFIX_STR) {
                return parse_time(value);
            }
        }
        tried += lines.len();
        if tried >= 128 {
            break;
        }
    }
    Err(format!("invalid slow query file {path}"))
}

/// Go `filepath.Dir` for the paths a slow-log file setting takes.
fn go_dir(path: &str) -> String {
    match Path::new(path).parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.to_string_lossy().into_owned(),
        _ => {
            if path.starts_with('/') {
                "/".to_owned()
            } else {
                ".".to_owned()
            }
        }
    }
}

/// Go `filepath.Join(dir, name)` for a cleaned directory.
fn go_join(dir: &str, name: &str) -> String {
    if dir == "." {
        name.to_owned()
    } else if dir.ends_with('/') {
        format!("{dir}{name}")
    } else {
        format!("{dir}/{name}")
    }
}

/// Go `getAllFiles`: the slow-log file and its rotations that may hold
/// entries in the checker's time ranges, ordered by start time. A file that
/// cannot be read is skipped with a warning (an empty one silently).
fn get_all_files(
    checker: &SlowLogChecker,
    log_file_path: &str,
    warnings: &mut Vec<String>,
    stats: &mut SlowQueryRuntimeStats,
) -> Result<Vec<LogFile>, String> {
    let started = std::time::Instant::now();
    let files = get_all_files_counted(checker, log_file_path, warnings, &mut stats.total_file_num);
    stats.initialize = started.elapsed();
    files
}

fn get_all_files_counted(
    checker: &SlowLogChecker,
    log_file_path: &str,
    warnings: &mut Vec<String>,
    total_file_num: &mut usize,
) -> Result<Vec<LogFile>, String> {
    let log_dir = go_dir(log_file_path);
    let ext = Path::new(log_file_path)
        .extension()
        .map(|ext| format!(".{}", ext.to_string_lossy()))
        .unwrap_or_default();
    let prefix = &log_file_path[..log_file_path.len() - ext.len()];
    let entries = std::fs::read_dir(&log_dir).map_err(|error| go_open_error(&log_dir, &error))?;
    let mut names: Vec<(String, bool)> = entries
        .filter_map(Result::ok)
        .map(|entry| {
            let is_dir = entry.file_type().is_ok_and(|kind| kind.is_dir());
            (entry.file_name().to_string_lossy().into_owned(), is_dir)
        })
        .collect();
    // Go `os.ReadDir` returns the entries sorted by name.
    names.sort();
    let mut log_files = Vec::new();
    for (name, is_dir) in names {
        if is_dir {
            continue;
        }
        let path = go_join(&log_dir, &name);
        // All rotated log files have the same prefix with the original file.
        if !path.starts_with(prefix) {
            continue;
        }
        *total_file_num += 1;
        let compressed = path.ends_with(".gz");
        let start = match file_start_time(&path, compressed) {
            Ok(Some(start)) => start.with_timezone(&Utc),
            Ok(None) => continue,
            Err(error) => {
                warnings.push(error);
                continue;
            }
        };
        if checker.enable_time_check {
            let not_in_all_time_ranges = checker
                .time_ranges
                .iter()
                .all(|(_, end)| start > *end + SLOW_LOG_TIME_RANGE_INTERNAL_TOLERANCE);
            if not_in_all_time_ranges {
                continue;
            }
        }
        // Go reads a plain file's end time; a compressed one would need the
        // whole file inflated.
        if !compressed {
            let end = match file_end_time(&path) {
                Ok(end) => end.with_timezone(&Utc),
                Err(error) => {
                    warnings.push(error);
                    continue;
                }
            };
            if checker.enable_time_check && !checker.may_overlap(start, end) {
                continue;
            }
        }
        log_files.push(LogFile {
            path,
            start,
            compressed,
        });
    }
    log_files.sort_by(|left, right| left.start.cmp(&right.start));
    // Assume no time range overlap in log files and remove unnecessary log
    // files for compressed files.
    let mut kept = Vec::with_capacity(log_files.len());
    for index in 0..log_files.len() {
        let file = &log_files[index];
        if index == log_files.len() - 1 || !file.compressed || !checker.enable_time_check {
            kept.push(index);
            continue;
        }
        if checker.may_overlap(file.start, log_files[index + 1].start) {
            kept.push(index);
        }
    }
    let mut kept_files = Vec::with_capacity(kept.len());
    for (index, file) in log_files.into_iter().enumerate() {
        if kept.contains(&index) {
            kept_files.push(file);
        }
    }
    Ok(kept_files)
}

/// Go `isLetterOrNumeric`.
fn is_letter_or_numeric(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
}

/// Go `findMatchedRightBracket`.
fn find_matched_right_bracket(line: &[u8], left_bracket_idx: usize) -> Option<usize> {
    let left_bracket = line[left_bracket_idx];
    let right_bracket = match left_bracket {
        b'[' => b']',
        b'{' => b'}',
        _ => return None,
    };
    let mut current = left_bracket_idx;
    let mut left_bracket_cnt = 0i64;
    while current < line.len() {
        let byte = line[current];
        if byte == left_bracket {
            left_bracket_cnt += 1;
            current += 1;
        } else if byte == right_bracket {
            left_bracket_cnt -= 1;
            if left_bracket_cnt > 0 {
                current += 1;
            } else if left_bracket_cnt == 0 {
                if current + 1 < line.len() && line[current + 1] != b' ' {
                    return None;
                }
                return Some(current);
            } else {
                return None;
            }
        } else {
            current += 1;
        }
    }
    None
}

/// Go `splitByColon`: a `field: value field: value ...` line, where a value
/// in `[]` or `{}` may hold spaces. A malformed line yields nothing.
fn split_by_colon(line: &str) -> (Vec<String>, Vec<String>) {
    let bytes = line.as_bytes();
    let length = bytes.len();
    let mut fields = Vec::new();
    let mut values = Vec::new();
    let mut parse_key = true;
    let mut current = 0usize;
    let mut failed = false;
    let slice = |start: usize, end: usize| String::from_utf8_lossy(&bytes[start..end]).into_owned();
    while current < length {
        if parse_key {
            while current < length && !is_letter_or_numeric(bytes[current]) {
                current += 1;
            }
            let start = current;
            if current >= length {
                break;
            }
            while current < length && bytes[current] != b':' {
                current += 1;
            }
            fields.push(slice(start, current));
            parse_key = false;
            current += 2;
            if current >= length {
                values.push(String::new());
            }
        } else {
            let start = current;
            if current < length && (bytes[current] == b'{' || bytes[current] == b'[') {
                let Some(right) = find_matched_right_bracket(bytes, current) else {
                    failed = true;
                    break;
                };
                current = right + 1;
            } else {
                while current < length && bytes[current] != b' ' {
                    current += 1;
                }
                // Meet empty value cases: "Key: Key:".
                if current > 0 && bytes[current - 1] == b':' {
                    values.push(String::new());
                    current = start;
                    parse_key = true;
                    continue;
                }
            }
            values.push(slice(start, current.min(length)));
            parse_key = true;
        }
    }
    if failed || fields.len() != values.len() {
        return (Vec::new(), Vec::new());
    }
    (fields, values)
}

/// Go `parseUserOrHostValue`: the part before `[`, trimmed.
fn parse_user_or_host_value(value: &str) -> String {
    value
        .split('[')
        .next()
        .unwrap_or_default()
        .trim()
        .to_owned()
}

/// Go `parsePlan`: the decoded `tidb_decode_plan('...')` payload, or the raw
/// text when it does not decode.
fn parse_plan(plan: &str) -> String {
    if plan.len() <= SLOW_LOG_PLAN_PREFIX.len() + SLOW_LOG_PLAN_SUFFIX.len() {
        return plan.to_owned();
    }
    let inner = &plan[SLOW_LOG_PLAN_PREFIX.len()..plan.len() - SLOW_LOG_PLAN_SUFFIX.len()];
    match tidb_plancodec::decode_plan(inner) {
        Ok(decoded) => String::from_utf8_lossy(&decoded).into_owned(),
        Err(_) => inner.to_owned(),
    }
}

/// The value kind of one SLOW_QUERY column (Go
/// `getColumnValueFactoryByName`'s arms).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ColumnKind {
    Time,
    BackoffDetail,
    Plan,
    BinaryPlan,
    Uint,
    Float,
    Text,
    Int,
    Bool,
    Json,
}

/// Go `getColumnValueFactoryByName`.
fn column_kind(name: &str) -> Option<ColumnKind> {
    Some(match name {
        "Time" => ColumnKind::Time,
        "Backoff_Detail" => ColumnKind::BackoffDetail,
        "Plan" => ColumnKind::Plan,
        "Binary_plan" => ColumnKind::BinaryPlan,
        "Conn_ID"
        | "Exec_retry_count"
        | "Preproc_subqueries"
        | "Write_keys"
        | "Write_size"
        | "Prewrite_region"
        | "Txn_retry"
        | "Request_count"
        | "Total_keys"
        | "Process_keys"
        | "Rocksdb_delete_skipped_count"
        | "Rocksdb_key_skipped_count"
        | "Rocksdb_block_cache_hit_count"
        | "Rocksdb_block_read_count"
        | "Txn_start_ts"
        | "Rocksdb_block_read_byte"
        | "IA_remote_read_segment_count"
        | "IA_remote_read_segment_size" => ColumnKind::Uint,
        "Exec_retry_time"
        | "Query_time"
        | "Parse_time"
        | "Compile_time"
        | "Rewrite_time"
        | "Preproc_subqueries_time"
        | "Optimize_time"
        | "Wait_TS"
        | "Prewrite_time"
        | "Wait_prewrite_binlog_time"
        | "Commit_time"
        | "Get_commit_ts_time"
        | "Commit_backoff_time"
        | "Resolve_lock_time"
        | "Local_latch_wait_time"
        | "Cop_time"
        | "Process_time"
        | "Wait_time"
        | "Backoff_time"
        | "LockKeys_time"
        | "Cop_proc_avg"
        | "Cop_proc_p90"
        | "Cop_proc_max"
        | "Cop_wait_avg"
        | "Cop_wait_p90"
        | "Cop_wait_max"
        | "KV_total"
        | "PD_total"
        | "Backoff_total"
        | "Write_sql_response_total"
        | "Request_unit_read"
        | "Request_unit_write"
        | "Time_queued_by_rc"
        | "Tidb_cpu_time"
        | "Tikv_cpu_time"
        | "Mem_arbitration"
        | "IA_remote_read_segment_wait_time"
        | "Request_unit_v2" => ColumnKind::Float,
        "User"
        | "Host"
        | "Backoff_types"
        | "DB"
        | "Index_names"
        | "Digest"
        | "Stats"
        | "Cop_proc_addr"
        | "Cop_wait_addr"
        | "Plan_digest"
        | "Prev_stmt"
        | "Query"
        | "Warnings"
        | "Session_alias"
        | "Resource_group"
        | "Request_unit_v2_detail" => ColumnKind::Text,
        "Mem_max"
        | "Disk_max"
        | "Result_rows"
        | "Unpacked_bytes_sent_tikv_total"
        | "Unpacked_bytes_received_tikv_total"
        | "Unpacked_bytes_sent_tikv_cross_zone"
        | "Unpacked_bytes_received_tikv_cross_zone"
        | "Unpacked_bytes_sent_tiflash_total"
        | "Unpacked_bytes_received_tiflash_total"
        | "Unpacked_bytes_sent_tiflash_cross_zone"
        | "Unpacked_bytes_received_tiflash_cross_zone" => ColumnKind::Int,
        "Prepared" | "Succ" | "Plan_from_cache" | "Plan_from_binding" | "Is_internal"
        | "IsExplicitTxn" | "IsWriteCacheTable" | "Has_more_results" | "Storage_from_kv"
        | "Storage_from_mpp" => ColumnKind::Bool,
        "Session_connect_attrs" => ColumnKind::Json,
        _ => return None,
    })
}

/// Go `strconv.ParseBool`.
fn parse_go_bool(value: &str) -> Result<bool, String> {
    match value {
        "1" | "t" | "T" | "TRUE" | "true" | "True" => Ok(true),
        "0" | "f" | "F" | "FALSE" | "false" | "False" => Ok(false),
        _ => Err(format!(
            "strconv.ParseBool: parsing {value:?}: invalid syntax"
        )),
    }
}

/// The rows of one SLOW_QUERY read, and the warnings Go appends to the
/// statement while reading.
pub struct SlowQueryRows {
    /// The rows, one datum per column of `columns`.
    pub rows: Vec<Vec<Datum>>,
    /// Go's `StmtCtx.AppendWarning` texts, in order.
    pub warnings: Vec<String>,
    /// Go `slowQueryRetriever.stats`, which `MemTableReaderExec.Close`
    /// registers for the scan.
    pub stats: SlowQueryRuntimeStats,
}

/// Go `slowQueryRuntimeStats`.
#[derive(Clone, Debug, Default)]
pub struct SlowQueryRuntimeStats {
    total_file_num: usize,
    read_file_num: usize,
    read_file: Duration,
    initialize: Duration,
    read_file_size: i64,
    parse_log: Duration,
    /// The parse workers (`DistSQLScanConcurrency`); unset under a row
    /// limit, which parses serially.
    concurrent: usize,
}

impl tidb_execdetails::RuntimeStats for SlowQueryRuntimeStats {
    /// Go `slowQueryRuntimeStats.String`.
    fn string(&self) -> String {
        format!(
            "initialize: {}, read_file: {}, parse_log: {{time:{}, concurrency:{}}}, total_file: {}, read_file: {}, read_size: {}",
            tidb_execdetails::format_duration(self.initialize),
            tidb_execdetails::format_duration(self.read_file),
            tidb_execdetails::format_duration(self.parse_log),
            self.concurrent,
            self.total_file_num,
            self.read_file_num,
            tidb_memory::format_bytes(self.read_file_size),
        )
    }

    /// Go `slowQueryRuntimeStats.Merge`.
    fn merge(&mut self, other: &dyn tidb_execdetails::RuntimeStats) {
        let Some(tmp) = other.as_any().downcast_ref::<Self>() else {
            return;
        };
        self.total_file_num += tmp.total_file_num;
        self.read_file_num += tmp.read_file_num;
        self.read_file += tmp.read_file;
        self.initialize += tmp.initialize;
        self.read_file_size += tmp.read_file_size;
        self.parse_log += tmp.parse_log;
    }

    fn clone_box(&self) -> Box<dyn tidb_execdetails::RuntimeStats> {
        Box::new(self.clone())
    }

    fn tp(&self) -> i32 {
        tidb_execdetails::TP_SLOW_QUERY_RUNTIME_STAT
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

struct Parser<'a> {
    columns: &'a [(String, FieldType)],
    kinds: Vec<Option<ColumnKind>>,
    checker: &'a SlowLogChecker,
    tz: &'a SessionTimeZone,
    warnings: Vec<String>,
}

impl Parser<'_> {
    fn column(&self, field: &str) -> Option<(usize, ColumnKind)> {
        self.columns
            .iter()
            .position(|(name, _)| name == field)
            .and_then(|index| self.kinds[index].map(|kind| (index, kind)))
    }

    /// Go `setColumnValue`: `false` invalidates the entry.
    fn set_column_value(
        &self,
        warnings: &mut Vec<String>,
        row: &mut [Option<Datum>],
        field: &str,
        value: &str,
        line_num: usize,
    ) -> bool {
        let Some((index, kind)) = self.column(field) else {
            // Go issue 34320: the time filter applies even when the Time
            // column is not read.
            if field == "Time" {
                return match parse_time(value) {
                    Ok(t) => self.checker.is_time_valid(t.with_timezone(&Utc)),
                    Err(error) => {
                        warnings.push(format!(
                            "Parse slow log at line {line_num}, failed field is {field}, failed value is {value}, error is {error}"
                        ));
                        false
                    }
                };
            }
            return true;
        };
        match self.factory(row, index, kind, value) {
            Ok(valid) => valid,
            Err(error) => {
                warnings.push(format!(
                    "Parse slow log at line {line_num}, failed field is {field}, failed value is {value}, error is {error}"
                ));
                true
            }
        }
    }

    /// Go's column value factories.
    fn factory(
        &self,
        row: &mut [Option<Datum>],
        index: usize,
        kind: ColumnKind,
        value: &str,
    ) -> Result<bool, String> {
        let datum =
            match kind {
                ColumnKind::Time => {
                    let t = parse_time(value)?.with_timezone(&Utc);
                    if !self.checker.is_time_valid(t) {
                        return Ok(false);
                    }
                    let local = t.with_timezone(self.tz);
                    Datum::new_time(
                        Time::new(core_time_from_datetime(local), TimeType::Timestamp, 6)
                            .map_err(|error| error.to_string())?,
                    )
                }
                ColumnKind::BackoffDetail => {
                    let mut detail = match &row[index] {
                        Some(Datum::String(existing)) => {
                            String::from_utf8_lossy(existing.bytes()).into_owned()
                        }
                        _ => String::new(),
                    };
                    if !detail.is_empty() {
                        detail.push(' ');
                    }
                    detail.push_str(value);
                    Datum::new_string_datum(detail)
                }
                ColumnKind::Plan => Datum::new_string_datum(parse_plan(value)),
                ColumnKind::BinaryPlan => {
                    let value = if value.starts_with(SLOW_LOG_BINARY_PLAN_PREFIX)
                        && value.len()
                            >= SLOW_LOG_BINARY_PLAN_PREFIX.len() + SLOW_LOG_PLAN_SUFFIX.len()
                    {
                        &value[SLOW_LOG_BINARY_PLAN_PREFIX.len()
                            ..value.len() - SLOW_LOG_PLAN_SUFFIX.len()]
                    } else {
                        value
                    };
                    Datum::new_string_datum(value)
                }
                ColumnKind::Uint => Datum::UInt(value.parse::<u64>().map_err(|_| {
                    format!("strconv.ParseUint: parsing {value:?}: invalid syntax")
                })?),
                ColumnKind::Float => Datum::Real(value.parse::<f64>().map_err(|_| {
                    format!("strconv.ParseFloat: parsing {value:?}: invalid syntax")
                })?),
                ColumnKind::Text => Datum::new_string_datum(value),
                ColumnKind::Int => {
                    Datum::Int(value.parse::<i64>().map_err(|_| {
                        format!("strconv.ParseInt: parsing {value:?}: invalid syntax")
                    })?)
                }
                ColumnKind::Bool => Datum::Int(i64::from(parse_go_bool(value)?)),
                ColumnKind::Json => {
                    if value.is_empty() {
                        return Ok(true);
                    }
                    Datum::new_json(BinaryJSON::parse(value).map_err(|error| error.to_string())?)
                }
            };
        row[index] = Some(datum);
        Ok(true)
    }

    /// Go `parseSlowLog` with `parseSlowLogByBatchGetter`, one batch after
    /// another: `getBatchLog` reads up to [`PARSE_SLOW_LOG_BATCH_SIZE`] entries
    /// across the files, `parseLog` turns the batch into rows, and the reader's
    /// `memConsume` charges what Go's does -- the batch's log size while it is
    /// parsed (`calculateLogSize`), each row's `EstimatedMemUsage`, and the
    /// previous batch's rows released when the next is fetched
    /// (`dataForSlowLog`'s `lastFetchSize`).
    fn parse(
        &mut self,
        next_batch: &mut dyn FnMut(usize, &mut SlowQueryRuntimeStats) -> Result<Vec<(String, usize)>, String>,
        mem_tracker: Option<&std::sync::Arc<tidb_memory::Tracker>>,
        stats: &mut SlowQueryRuntimeStats,
        limit: u64,
        concurrency: usize,
    ) -> Result<Vec<Vec<Datum>>, String> {
        let mem_consume = |bytes: i64| {
            if let Some(tracker) = mem_tracker {
                tracker.consume(bytes);
            }
        };
        let mut data = Vec::new();
        let mut last_fetch_size = 0;
        if limit > 0 {
            // Go `parseSlowLogByBatchGetterWithLimit`: serial, `batchSize =
            // min(ParseSlowLogBatchSize, limit)`, stopping once `limit` rows
            // are produced.
            let limit = usize::try_from(limit).unwrap_or(usize::MAX);
            while data.len() < limit {
                let remaining = limit - data.len();
                let started = std::time::Instant::now();
                let log = next_batch(PARSE_SLOW_LOG_BATCH_SIZE.min(remaining), stats)?;
                stats.read_file += started.elapsed();
                if log.is_empty() {
                    break;
                }
                let log_size = calculate_log_size(&log);
                mem_consume(log_size);
                let started = std::time::Instant::now();
                let mut warnings = Vec::new();
                let mut rows = self.parse_log(&log, &mem_consume, &mut warnings);
                stats.parse_log += started.elapsed();
                self.warnings.extend(warnings);
                rows.truncate(remaining);
                mem_consume(-log_size);
                mem_consume(-last_fetch_size);
                last_fetch_size = calculate_datums_size(&rows);
                data.extend(rows);
            }
            mem_consume(-last_fetch_size);
            return Ok(data);
        }
        // Go `parseSlowLogByBatchGetter`: the sender reads batches in order
        // and `concurrency` workers parse them; `dataForSlowLog` takes the
        // results in the order the tasks were sent.
        stats.concurrent = concurrency;
        let concurrency = concurrency.max(1);
        loop {
            let started = std::time::Instant::now();
            let mut batches = Vec::with_capacity(concurrency);
            while batches.len() < concurrency {
                let log = next_batch(PARSE_SLOW_LOG_BATCH_SIZE, stats)?;
                if log.is_empty() {
                    break;
                }
                batches.push(log);
            }
            stats.read_file += started.elapsed();
            if batches.is_empty() {
                break;
            }
            let parser = &*self;
            let mem_consume = &mem_consume;
            let parsed: Vec<(Vec<Vec<Datum>>, Vec<String>, Duration)> = std::thread::scope(|scope| {
                let workers: Vec<_> = batches
                    .iter()
                    .map(|log| {
                        scope.spawn(move || {
                            // Go `parseLog`: the log size is held while parsing.
                            let started = std::time::Instant::now();
                            let log_size = calculate_log_size(log);
                            mem_consume(log_size);
                            let mut warnings = Vec::new();
                            let rows = parser.parse_log(log, mem_consume, &mut warnings);
                            mem_consume(-log_size);
                            (rows, warnings, started.elapsed())
                        })
                    })
                    .collect();
                workers
                    .into_iter()
                    .map(|worker| worker.join().expect("a slow-log parse worker panicked"))
                    .collect()
            });
            for (rows, warnings, elapsed) in parsed {
                // Go adds each worker's `parseLog` time.
                stats.parse_log += elapsed;
                self.warnings.extend(warnings);
                mem_consume(-last_fetch_size);
                last_fetch_size = calculate_datums_size(&rows);
                data.extend(rows);
            }
        }
        mem_consume(-last_fetch_size);
        Ok(data)
    }

    /// Go `parseLog`: one batch of lines, each with its line number in its
    /// file, into rows.
    fn parse_log(
        &self,
        log: &[(String, usize)],
        mem_consume: &(dyn Fn(i64) + Sync),
        warnings: &mut Vec<String>,
    ) -> Vec<Vec<Datum>> {
        let mut data = Vec::new();
        let mut row: Vec<Option<Datum>> = Vec::new();
        let mut user = String::new();
        let mut start_flag = false;
        for (line, line_num) in log {
            let (line, line_num) = (line.as_str(), *line_num);
            if !start_flag {
                if let Some(value) = line.strip_prefix(SLOW_LOG_START_PREFIX_STR) {
                    row = vec![None; self.columns.len()];
                    user.clear();
                    if self.set_column_value(warnings, &mut row, "Time", value, line_num) {
                        start_flag = true;
                    }
                }
                continue;
            }
                if let Some(rest) = line.strip_prefix(SLOW_LOG_ROW_PREFIX_STR) {
                    let mut valid = true;
                    if let Some(value) = rest.strip_prefix(SLOW_LOG_PREV_STMT_PREFIX) {
                        valid = self.set_column_value(warnings, &mut row, "Prev_stmt", value, line_num);
                    } else if let Some(value) = rest.strip_prefix("User@Host: ") {
                        let Some((user_part, host_part)) = value.split_once('@') else {
                            continue;
                        };
                        user = parse_user_or_host_value(user_part);
                        if !self.checker.has_privilege(&user) {
                            start_flag = false;
                            continue;
                        }
                        let user_value = user.clone();
                        if !self.set_column_value(warnings, &mut row, "User", &user_value, line_num) {
                            start_flag = false;
                            continue;
                        }
                        let host = parse_user_or_host_value(host_part);
                        valid = self.set_column_value(warnings, &mut row, "Host", &host, line_num);
                    } else if rest.starts_with("Cop_backoff_") {
                        valid = self.set_column_value(warnings, &mut row, "Backoff_Detail", rest, line_num);
                    } else if let Some(value) = rest.strip_prefix("Warnings: ") {
                        valid = self.set_column_value(warnings, &mut row, "Warnings", value, line_num);
                    } else if let Some(value) = rest.strip_prefix("Session_connect_attrs: ") {
                        valid = self.set_column_value(
                            warnings,
                            &mut row,
                            "Session_connect_attrs",
                            value,
                            line_num,
                        );
                    } else if let Some(value) = rest.strip_prefix("Request_unit_v2_detail: ") {
                        valid = self.set_column_value(
                            warnings,
                            &mut row,
                            "Request_unit_v2_detail",
                            value,
                            line_num,
                        );
                    } else if let Some(value) = rest.strip_prefix("DB: ") {
                        valid = self.set_column_value(warnings, &mut row, "DB", value, line_num);
                    } else {
                        let (fields, values) = split_by_colon(rest);
                        for (field, value) in fields.iter().zip(values.iter()) {
                            if !self.set_column_value(warnings, &mut row, field, value, line_num) {
                                start_flag = false;
                                break;
                            }
                        }
                    }
                    if !valid {
                        start_flag = false;
                    }
                } else if line.ends_with(SLOW_LOG_SQL_SUFFIX_STR) {
                    // `use DB` lines keep the log MySQL-compatible; the `# DB`
                    // field already carries the database (tidb#17846).
                    if line.starts_with("use") {
                        continue;
                    }
                    if !self.checker.has_privilege(&user) {
                        start_flag = false;
                        continue;
                    }
                    let _ = self.set_column_value(warnings, &mut row, "Query", line, line_num);
                    // Go `setDefaultValue`: an unset column takes its type's
                    // zero value.
                    let finished = row
                        .iter_mut()
                        .zip(self.columns.iter())
                        .map(|(value, (_, field_type))| {
                            value
                                .take()
                                .unwrap_or_else(|| crate::bad_null::get_zero_value(field_type))
                        })
                        .collect::<Vec<_>>();
                    mem_consume(tidb_datatype::estimated_mem_usage(&finished, 1));
                    data.push(finished);
                    start_flag = false;
                } else {
                    start_flag = false;
                }
        }
        data
    }
}

/// Go `ParseSlowLogBatchSize`: the slow-log entries one batch holds.
const PARSE_SLOW_LOG_BATCH_SIZE: usize = 64;

/// Go `calculateLogSize`.
fn calculate_log_size(log: &[(String, usize)]) -> i64 {
    log.iter().map(|(line, _)| line.len() as i64).sum()
}

/// Go `calculateDatumsSize`.
fn calculate_datums_size(rows: &[Vec<Datum>]) -> i64 {
    rows.iter().map(|row| tidb_datatype::estimated_mem_usage(row, 1)).sum()
}

/// Go `slowQueryRetriever`'s reader state over the selected files: the open
/// file (`getNextReader`) and the line within it (`fileLine`).
struct BatchReader<'a> {
    files: std::slice::Iter<'a, LogFile>,
    reader: Option<Box<dyn BufRead>>,
    file_line: usize,
}

impl<'a> BatchReader<'a> {
    fn new(files: &'a [LogFile]) -> Self {
        Self {
            files: files.iter(),
            reader: None,
            file_line: 0,
        }
    }

    /// Go `getBatchLog`: up to `batch_size` entries, each ending at a line
    /// with the SQL suffix that is not a `use` or `# ` line; at a file's end
    /// the next file continues the batch.
    fn get_batch_log(
        &mut self,
        batch_size: usize,
        stats: &mut SlowQueryRuntimeStats,
    ) -> Result<Vec<(String, usize)>, String> {
        let mut log = Vec::with_capacity(batch_size);
        for _ in 0..batch_size {
            loop {
                if self.reader.is_none() {
                    let Some(file) = self.files.next() else {
                        return Ok(log);
                    };
                    count_next_file(file, stats);
                    self.reader = Some(open_reader(&file.path, file.compressed)?);
                    self.file_line = 0;
                }
                let reader = self.reader.as_mut().expect("opened above");
                let Some(line) = read_line(reader.as_mut())? else {
                    self.reader = None;
                    continue;
                };
                self.file_line += 1;
                let ends = ends_entry(&line);
                log.push((line, self.file_line));
                if ends {
                    break;
                }
            }
        }
        Ok(log)
    }
}

/// Go `getNextFile`'s `readFileSize` / `readFileNum`.
fn count_next_file(file: &LogFile, stats: &mut SlowQueryRuntimeStats) {
    if let Ok(metadata) = std::fs::metadata(&file.path) {
        stats.read_file_size += i64::try_from(metadata.len()).unwrap_or(i64::MAX);
        stats.read_file_num += 1;
    }
}

/// Go's test for a line that ends an entry: the SQL suffix on a line that is
/// neither a `use` nor a `# ` line.
fn ends_entry(line: &str) -> bool {
    line.ends_with(SLOW_LOG_SQL_SUFFIX_STR) && !line.starts_with("use") && !line.starts_with(SLOW_LOG_ROW_PREFIX_STR)
}

/// Go `slowLogReverseScanner`: the entries of the selected files (newest
/// file first) from the last to the first, stopping at the first entry that
/// started before every time range (less `slowLogTimeRangeInternalTolerance`).
struct ReverseScanner<'a> {
    files: std::slice::Iter<'a, LogFile>,
    file: Option<std::fs::File>,
    end_cursor: u64,
    /// A compressed file can only be read forward: its blocks are loaded
    /// whole and handed out from the last.
    compressed_blocks: Option<Vec<Vec<String>>>,
    /// The block being scanned backwards, its lines in reverse order.
    pending_block: Vec<String>,
    pending_has_sql_suffix: bool,
    /// The lines `readLastLines` returned, handed out from the last.
    cached_lines: Vec<String>,
    min_start_time: Option<DateTime<Utc>>,
    finished: bool,
}

impl<'a> ReverseScanner<'a> {
    /// Go `newSlowLogReverseScanner`.
    fn new(files: &'a [LogFile], checker: &SlowLogChecker) -> Self {
        let min_start_time = if checker.enable_time_check {
            checker
                .time_ranges
                .iter()
                .map(|(start, _)| *start)
                .min()
                .map(|start| start - SLOW_LOG_TIME_RANGE_INTERNAL_TOLERANCE)
        } else {
            None
        };
        Self {
            files: files.iter(),
            file: None,
            end_cursor: 0,
            compressed_blocks: None,
            pending_block: Vec::with_capacity(8),
            pending_has_sql_suffix: false,
            cached_lines: Vec::new(),
            min_start_time,
            finished: false,
        }
    }

    /// Go `slowLogReverseScanner.nextBatch`: up to `batch_size` entries. A
    /// line's number is its place in the batch (Go parses it at offset 0).
    fn next_batch(
        &mut self,
        batch_size: usize,
        stats: &mut SlowQueryRuntimeStats,
    ) -> Result<Vec<(String, usize)>, String> {
        if self.finished {
            return Ok(Vec::new());
        }
        let mut blocks = Vec::with_capacity(batch_size);
        while blocks.len() < batch_size {
            let Some(block) = self.next_block(stats)? else {
                break;
            };
            let started_before = self.min_start_time.is_some_and(|min_start_time| {
                block
                    .first()
                    .and_then(|line| line.strip_prefix(SLOW_LOG_START_PREFIX_STR))
                    .and_then(|value| parse_time(value).ok())
                    .is_some_and(|t| t.with_timezone(&Utc) < min_start_time)
            });
            if started_before {
                self.finished = true;
                break;
            }
            blocks.push(block);
        }
        Ok(blocks
            .into_iter()
            .flatten()
            .enumerate()
            .map(|(index, line)| (line, index + 1))
            .collect())
    }

    /// Go `slowLogReverseScanner.nextBlock`: the previous entry, `None` once
    /// every file is read.
    fn next_block(&mut self, stats: &mut SlowQueryRuntimeStats) -> Result<Option<Vec<String>>, String> {
        loop {
            if let Some(blocks) = &mut self.compressed_blocks {
                let Some(block) = blocks.pop() else {
                    self.compressed_blocks = None;
                    self.file = None;
                    continue;
                };
                if !block.iter().any(|line| ends_entry(line)) {
                    if !self.pending_has_sql_suffix {
                        continue;
                    }
                    let mut merged = block;
                    merged.extend(self.pending_block.drain(..).rev());
                    self.pending_has_sql_suffix = false;
                    return Ok(Some(merged));
                }
                return Ok(Some(block));
            }
            if self.file.is_none() {
                let Some(file) = self.files.next() else {
                    return Ok(None);
                };
                count_next_file(file, stats);
                self.cached_lines.clear();
                if file.compressed {
                    self.compressed_blocks = Some(load_compressed_blocks(&file.path)?);
                    continue;
                }
                let opened = std::fs::File::open(&file.path).map_err(|error| go_open_error(&file.path, &error))?;
                self.end_cursor = opened.metadata().map_err(|error| error.to_string())?.len();
                self.file = Some(opened);
            }
            let Some(line) = self.cached_lines.pop() else {
                let file = self.file.as_mut().expect("opened above");
                let (lines, read_bytes) = read_last_lines(file, self.end_cursor)?;
                if read_bytes == 0 {
                    // Reach the beginning of the current file.
                    self.file = None;
                    self.end_cursor = 0;
                    continue;
                }
                self.end_cursor -= read_bytes as u64;
                self.cached_lines = lines;
                continue;
            };
            if self.pending_block.is_empty() && line.is_empty() {
                continue;
            }
            let is_start = line.starts_with(SLOW_LOG_START_PREFIX_STR);
            if ends_entry(&line) {
                self.pending_has_sql_suffix = true;
            }
            self.pending_block.push(line);
            // In reverse scanning, the start prefix ends a block.
            if is_start {
                if !self.pending_has_sql_suffix {
                    self.pending_block.clear();
                    continue;
                }
                let block = self.pending_block.drain(..).rev().collect();
                self.pending_has_sql_suffix = false;
                return Ok(Some(block));
            }
        }
    }
}

/// Go `loadCompressedBlocks`: a compressed file's entries, each from a
/// `# Time:` line to the line that ends it, plus a trailing unfinished one.
fn load_compressed_blocks(path: &str) -> Result<Vec<Vec<String>>, String> {
    let mut reader = open_reader(path, true)?;
    let mut blocks = Vec::new();
    let mut block = Vec::new();
    let mut has_start_flag = false;
    while let Some(line) = read_line(reader.as_mut())? {
        if line.starts_with(SLOW_LOG_START_PREFIX_STR) {
            if has_start_flag {
                block.clear();
            } else {
                has_start_flag = true;
            }
        }
        if has_start_flag {
            let ends = ends_entry(&line);
            block.push(line);
            if ends {
                blocks.push(std::mem::replace(&mut block, Vec::with_capacity(8)));
                has_start_flag = false;
            }
        }
    }
    if !block.is_empty() {
        blocks.push(block);
    }
    Ok(blocks)
}

/// Go `slowQueryRetriever.retrieve` for one read of `SLOW_QUERY`: the rows
/// of the slow-log file `log_file_path` and its rotations, one datum per
/// column of `columns` (`SLOW_QUERY`'s columns, in table order), with times
/// shown in `tz`.
///
/// # Errors
/// The directory of `log_file_path` cannot be listed, or a selected file
/// cannot be read.
pub fn retrieve_slow_query_rows(
    columns: &[(String, FieldType)],
    checker: &SlowLogChecker,
    tz: &SessionTimeZone,
    log_file_path: &str,
    mem_tracker: Option<&std::sync::Arc<tidb_memory::Tracker>>,
    limit: u64,
    desc: bool,
    concurrency: usize,
) -> Result<SlowQueryRows, String> {
    let mut warnings = Vec::new();
    let mut stats = SlowQueryRuntimeStats::default();
    let mut files = get_all_files(checker, log_file_path, &mut warnings, &mut stats)?;
    let mut parser = Parser {
        columns,
        kinds: columns.iter().map(|(name, _)| column_kind(name)).collect(),
        checker,
        tz,
        warnings,
    };
    let rows = if desc {
        // Go `initialize` reverses the files for `parseSlowLogReversed`.
        files.reverse();
        let mut scanner = ReverseScanner::new(&files, checker);
        parser.parse(&mut |batch_size, stats| scanner.next_batch(batch_size, stats), mem_tracker, &mut stats, limit, concurrency)?
    } else {
        let mut reader = BatchReader::new(&files);
        parser.parse(&mut |batch_size, stats| reader.get_batch_log(batch_size, stats), mem_tracker, &mut stats, limit, concurrency)?
    };
    Ok(SlowQueryRows {
        rows,
        warnings: parser.warnings,
        stats,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go `slowQueryRetriever.memConsume`: a batch charges its log size while
    /// it is parsed and each row it yields; the rows are released when the
    /// next batch is fetched, so one batch peaks at both and ends at zero.
    #[test]
    fn a_slow_log_read_charges_its_log_and_rows_like_go() {
        let dir = std::env::temp_dir().join(format!("slow-query-mem-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("tidb-slow.log");
        let log = "# Time: 2026-11-05T10:00:00.000000+08:00\n# Query_time: 1.5\nselect 1;\n\
                   # Time: 2026-11-05T10:00:01.000000+08:00\n# Query_time: 2.5\nselect 2;\n";
        std::fs::write(&path, log).unwrap();
        let columns = tidb_infoschema::infoschema_meta::table_schema("SLOW_QUERY").unwrap();
        let checker = SlowLogChecker {
            has_process_priv: true,
            ..Default::default()
        };
        let tracker = tidb_memory::Tracker::new(1, -1);
        let read = retrieve_slow_query_rows(
            &columns,
            &checker,
            &SessionTimeZone::utc(),
            path.to_str().unwrap(),
            Some(&tracker),
            0,
            false,
            1,
        )
        .unwrap();
        assert_eq!(read.rows.len(), 2);
        let log_size: i64 = log.lines().map(|line| line.len() as i64).sum();
        assert_eq!(tracker.max_consumed(), log_size + calculate_datums_size(&read.rows));
        assert_eq!(tracker.bytes_consumed(), 0);
        // Go `slowQueryRuntimeStats`: the files the directory holds with the
        // log's prefix, the ones read and their bytes.
        let stats = tidb_execdetails::RuntimeStats::string(&read.stats);
        assert!(
            stats.ends_with(&format!(
                "concurrency:1}}, total_file: 1, read_file: 1, read_size: {}",
                tidb_memory::format_bytes(log.len() as i64)
            )),
            "{stats}"
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// Go `TestSplitbyColon` (`slow_query_test.go`).
    #[test]
    fn split_by_colon_matches_go() {
        let cases: &[(&str, &[&str], &[&str])] = &[
            ("", &[], &[]),
            ("123a", &["123a"], &[""]),
            ("1a: 2b", &["1a"], &["2b"]),
            ("1a: [2b 3c] 4d: 5e", &["1a", "4d"], &["[2b 3c]", "5e"]),
            ("1a: [2b,3c] 4d: 5e", &["1a", "4d"], &["[2b,3c]", "5e"]),
            (
                "1a: [2b,[3c: 3cc]] 4d: 5e",
                &["1a", "4d"],
                &["[2b,[3c: 3cc]]", "5e"],
            ),
            ("1a: {2b 3c} 4d: 5e", &["1a", "4d"], &["{2b 3c}", "5e"]),
            ("1a: {2b,3c} 4d: 5e", &["1a", "4d"], &["{2b,3c}", "5e"]),
            (
                "1a: {2b,{3c: 3cc}} 4d: 5e",
                &["1a", "4d"],
                &["{2b,{3c: 3cc}}", "5e"],
            ),
            ("1a: {{{2b,{3c: 3cc}} 4d: 5e", &[], &[]),
            ("1a: [2b,[3c: 3cc]]]] 4d: 5e", &[], &[]),
            (
                "Time: 2021-09-08T14:39:54.506967433+08:00",
                &["Time"],
                &["2021-09-08T14:39:54.506967433+08:00"],
            ),
            (
                "Cop_proc_avg: 0 Cop_proc_addr: Cop_proc_max: Cop_proc_min: ",
                &[
                    "Cop_proc_avg",
                    "Cop_proc_addr",
                    "Cop_proc_max",
                    "Cop_proc_min",
                ],
                &["0", "", "", ""],
            ),
        ];
        for (line, fields, values) in cases {
            let (got_fields, got_values) = split_by_colon(line);
            assert_eq!(got_fields, *fields, "{line}");
            assert_eq!(got_values, *values, "{line}");
        }
    }

    #[test]
    fn parse_time_accepts_go_layouts() {
        assert!(parse_time("2019-04-28T15:24:04.309074+08:00").is_ok());
        assert!(parse_time("2020-04-15T18:00:01Z").is_ok());
        assert!(parse_time("2019-04-28-15:24:04.309074 +0800").is_ok());
        assert!(parse_time("xx").is_err());
    }

    /// Go `parseSlowLogByBatchGetterWithLimit`: a row-limit hint stops the
    /// read once that many rows are produced.
    #[test]
    fn a_slow_log_read_stops_at_its_row_limit() {
        let dir = std::env::temp_dir().join(format!("slow-query-limit-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("tidb-slow.log");
        let log: String = (0..3)
            .map(|i| format!("# Time: 2026-11-05T10:00:0{i}.000000+08:00\nselect {i};\n"))
            .collect();
        std::fs::write(&path, &log).unwrap();
        let columns = tidb_infoschema::infoschema_meta::table_schema("SLOW_QUERY").unwrap();
        let checker = SlowLogChecker {
            has_process_priv: true,
            ..Default::default()
        };
        let read = |limit| {
            retrieve_slow_query_rows(&columns, &checker, &SessionTimeZone::utc(), path.to_str().unwrap(), None, limit, false, 4)
                .unwrap()
                .rows
                .len()
        };
        assert_eq!(read(0), 3);
        assert_eq!(read(2), 2);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    fn utc(rfc3339: &str) -> DateTime<Utc> {
        parse_time(rfc3339).unwrap().with_timezone(&Utc)
    }

    /// The `Query` column of `rows`.
    fn queries(columns: &[(String, FieldType)], rows: &[Vec<Datum>]) -> Vec<String> {
        let query = columns.iter().position(|(name, _)| name == "Query").unwrap();
        rows.iter().map(|row| row[query].sql_string().unwrap()).collect()
    }

    /// Go `TestBatchLogForReversedScan`.
    #[test]
    fn a_reverse_batch_reads_the_newest_entries_first() {
        let dir = std::env::temp_dir().join(format!("slow-query-reverse-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let logs = [
            ("tidb-slow-reverse-scan-2020-02-14T19-04-05.01.log", ""),
            (
                "tidb-slow-reverse-scan-2020-02-15T19-04-05.01.log",
                "\n# Time: 2020-02-15T18:00:01.000000+08:00\nselect 1;\n# Time: 2020-02-15T19:00:05.000000+08:00\nselect 2;\n# Time: 2020-02-15T20:00:05.000000+08:00",
            ),
            (
                "tidb-slow-reverse-scan-2020-02-16T19-04-05.01.log",
                "select 3;\n# Time: 2020-02-16T18:00:01.000000+08:00\nselect 4;\n# Time: 2020-02-16T18:00:05.000000+08:00\nselect 5;",
            ),
            (
                "tidb-slow-reverse-scan-2020-02-17T19-04-05.01.log",
                "\n# Time: 2020-02-16T19:00:00.000000+08:00\nselect 6;\n# Time: 2020-02-17T18:00:05.000000+08:00\nselect 7;\n# Time: 2020-04-15T18:00:05.299063744+08:00",
            ),
            (
                "tidb-slow-reverse-scan.log",
                "select 8;\n# Time: 2020-04-15T19:00:05.299063744+08:00\nselect 9;",
            ),
        ];
        for (name, data) in logs {
            std::fs::write(dir.join(name), data).unwrap();
        }
        let path = dir.join("tidb-slow-reverse-scan.log");
        let cases: [(&str, &str, &[&str]); 4] = [
            // `select 4;` started more than the tolerance before the range,
            // which ends the scan.
            (
                "2020-02-16T18:00:03.000000+08:00",
                "2020-02-16T18:00:06.000000+08:00",
                &["# Time: 2020-02-16T18:00:05.000000+08:00", "select 5;", "", ""],
            ),
            (
                "2020-02-15T18:00:00.000000+08:00",
                "2020-02-15T19:00:00.000000+08:00",
                &[
                    "# Time: 2020-02-15T19:00:05.000000+08:00",
                    "select 2;",
                    "# Time: 2020-02-15T18:00:01.000000+08:00",
                    "select 1;",
                ],
            ),
            (
                "2020-02-15T20:00:05.000000+08:00",
                "2020-02-17T19:00:00.000000+08:00",
                &[
                    "# Time: 2020-02-17T18:00:05.000000+08:00",
                    "select 7;",
                    "# Time: 2020-02-16T19:00:00.000000+08:00",
                    "select 6;",
                    "# Time: 2020-02-16T18:00:05.000000+08:00",
                    "select 5;",
                    "# Time: 2020-02-16T18:00:01.000000+08:00",
                    "select 4;",
                    "# Time: 2020-02-15T20:00:05.000000+08:00",
                    "select 3;",
                ],
            ),
            (
                "2020-02-16T19:00:00.000000+08:00",
                "2020-04-15T20:00:00.000000+08:00",
                &[
                    "# Time: 2020-04-15T19:00:05.299063744+08:00",
                    "select 9;",
                    "# Time: 2020-04-15T18:00:05.299063744+08:00",
                    "select 8;",
                    "# Time: 2020-02-17T18:00:05.000000+08:00",
                    "select 7;",
                    "# Time: 2020-02-16T19:00:00.000000+08:00",
                    "select 6;",
                ],
            ),
        ];
        for (start, end, want) in cases {
            let checker = SlowLogChecker {
                has_process_priv: true,
                enable_time_check: true,
                time_ranges: vec![(utc(start), utc(end))],
                ..Default::default()
            };
            let mut stats = SlowQueryRuntimeStats::default();
            let mut files = get_all_files(&checker, path.to_str().unwrap(), &mut Vec::new(), &mut stats).unwrap();
            files.reverse();
            let mut scanner = ReverseScanner::new(&files, &checker);
            let got: Vec<String> = scanner
                .next_batch(want.len() / 2, &mut stats)
                .unwrap()
                .into_iter()
                .map(|(line, _)| line)
                .collect();
            let want: Vec<&str> = want.iter().copied().filter(|line| !line.is_empty()).collect();
            assert_eq!(got, want, "{start}");
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// Go `TestSlowQueryRetrieverReversedScanWithLimit` and
    /// `TestSlowQueryRetrieverReversedScanWithTimeJitter`.
    #[test]
    fn a_descending_read_takes_its_limit_from_the_end() {
        let dir = std::env::temp_dir().join(format!("slow-query-desc-limit-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let columns = tidb_infoschema::infoschema_meta::table_schema("SLOW_QUERY").unwrap();
        let read = |name: &str, data: &str, checker: &SlowLogChecker, limit| {
            let path = dir.join(name);
            std::fs::write(&path, data).unwrap();
            let read = retrieve_slow_query_rows(
                &columns,
                checker,
                &SessionTimeZone::utc(),
                path.to_str().unwrap(),
                None,
                limit,
                true,
                4,
            )
            .unwrap();
            queries(&columns, &read.rows)
        };
        let all = SlowLogChecker {
            has_process_priv: true,
            ..Default::default()
        };
        let log = "# Time: 2020-02-15T18:00:01.000000+08:00\nselect 1;\n# Time: 2020-02-15T19:00:05.000000+08:00\nselect 2;\n# Time: 2020-02-15T20:00:05.000000+08:00\nselect 3;\n# Time: 2020-02-15T21:00:05.000000+08:00\nselect 4;\n# Time: 2020-02-15T22:00:05.000000+08:00\nselect 5;";
        assert_eq!(read("limit.log", log, &all, 2), ["select 5;", "select 4;"]);
        assert_eq!(read("limit.log", log, &all, 0), ["select 5;", "select 4;", "select 3;", "select 2;", "select 1;"]);
        // The long DB line makes the middle block cross `readLastLines` chunks.
        let cross_chunk = format!(
            "# Time: 2020-02-15T18:00:01.000000+08:00\nselect 1;\n# Time: 2020-02-15T19:00:05.000000+08:00\n# DB: {}\nselect 2;\n# Time: 2020-02-15T20:00:05.000000+08:00\nselect 3;",
            "x".repeat(5000)
        );
        assert_eq!(read("cross-chunk.log", &cross_chunk, &all, 3), ["select 3;", "select 2;", "select 1;"]);
        // A block just before the lower bound does not stop the scan before
        // earlier in-range blocks.
        let jitter = "# Time: 2020-02-15T18:00:00.000500+08:00\nselect in-window-1;\n# Time: 2020-02-15T18:00:00.001000+08:00\nselect in-window-2;\n# Time: 2020-02-15T17:59:59.999700+08:00\nselect just-before-window;\n# Time: 2020-02-15T18:00:00.001500+08:00\nselect in-window-3;";
        let window = SlowLogChecker {
            has_process_priv: true,
            enable_time_check: true,
            time_ranges: vec![(
                utc("2020-02-15T18:00:00.000000+08:00"),
                utc("2020-02-15T18:00:00.002000+08:00"),
            )],
            ..Default::default()
        };
        assert_eq!(
            read("jitter.log", jitter, &window, 3),
            ["select in-window-3;", "select in-window-2;", "select in-window-1;"]
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn parse_workers_keep_the_logs_order() {
        let dir = std::env::temp_dir().join(format!("slow-query-workers-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("tidb-slow.log");
        // 300 entries are five batches of `ParseSlowLogBatchSize`; every
        // 40th has a bad Query_time, which warns.
        let log: String = (0..300)
            .map(|i| {
                let query_time = if i % 40 == 0 { "bad".to_string() } else { format!("0.{i:03}") };
                format!(
                    "# Time: 2026-11-05T10:{:02}:{:02}.000000+08:00\n# Query_time: {query_time}\nselect {i};\n",
                    i / 60,
                    i % 60
                )
            })
            .collect();
        std::fs::write(&path, &log).unwrap();
        let columns = tidb_infoschema::infoschema_meta::table_schema("SLOW_QUERY").unwrap();
        let checker = SlowLogChecker {
            has_process_priv: true,
            ..Default::default()
        };
        let read = |concurrency| {
            let read = retrieve_slow_query_rows(
                &columns,
                &checker,
                &SessionTimeZone::utc(),
                path.to_str().unwrap(),
                None,
                0,
                false,
                concurrency,
            )
            .unwrap();
            let rows: Vec<Vec<String>> =
                read.rows.iter().map(|row| row.iter().map(Datum::string).collect()).collect();
            (rows, read.warnings, read.stats.concurrent)
        };
        let (serial_rows, serial_warnings, _) = read(1);
        let (rows, warnings, concurrent) = read(4);
        assert_eq!(serial_rows.len(), 300);
        assert_eq!(serial_warnings.len(), 8);
        assert_eq!(rows, serial_rows);
        assert_eq!(warnings, serial_warnings);
        assert_eq!(concurrent, 4);
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
