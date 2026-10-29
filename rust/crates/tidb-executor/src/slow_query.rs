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

/// Go `getFileEndTime` over `readLastLines`: the last `# Time:` among the
/// file's last 128 lines.
fn file_end_time(path: &str) -> Result<DateTime<FixedOffset>, String> {
    const MAX_READ_CACHE_SIZE: u64 = 1024 * 1024 * 64;
    let mut file = std::fs::File::open(path).map_err(|error| go_open_error(path, &error))?;
    let mut end_cursor = file.metadata().map_err(|error| error.to_string())?.len();
    let mut tried = 0usize;
    loop {
        // Go `readLastLines`: grow the window backwards until it holds a line
        // break that starts a non-empty line.
        let mut lines: Vec<u8> = Vec::new();
        let mut first_non_newline = 0usize;
        let mut cursor = end_cursor;
        let mut size: u64 = 2048;
        loop {
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
            chars.extend_from_slice(&lines);
            lines = chars;
            let scanned = size as usize;
            for i in 0..scanned.saturating_sub(1) {
                if (lines[i] == b'\n' || lines[i] == b'\r')
                    && lines[i + 1] != b'\n'
                    && lines[i + 1] != b'\r'
                {
                    first_non_newline = i + 1;
                    break;
                }
            }
            if first_non_newline > 0 {
                break;
            }
        }
        let tail = String::from_utf8_lossy(&lines[first_non_newline..]).into_owned();
        let read_bytes = lines.len() - first_non_newline;
        if read_bytes == 0 {
            break;
        }
        end_cursor -= read_bytes as u64;
        let mut split: Vec<&str> = Vec::new();
        let normalized = tail.replace("\r\n", "\n");
        split.extend(normalized.split('\n'));
        if split.last() == Some(&"") {
            split.pop();
        }
        for line in split.iter().rev() {
            if let Some(value) = line.strip_prefix(SLOW_LOG_START_PREFIX_STR) {
                return parse_time(value);
            }
        }
        tried += split.len();
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
        &mut self,
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
                        self.warnings.push(format!(
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
                self.warnings.push(format!(
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
                    Datum::new_string(detail)
                }
                ColumnKind::Plan => Datum::new_string(parse_plan(value)),
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
                    Datum::new_string(value)
                }
                ColumnKind::Uint => Datum::UInt(value.parse::<u64>().map_err(|_| {
                    format!("strconv.ParseUint: parsing {value:?}: invalid syntax")
                })?),
                ColumnKind::Float => Datum::Real(value.parse::<f64>().map_err(|_| {
                    format!("strconv.ParseFloat: parsing {value:?}: invalid syntax")
                })?),
                ColumnKind::Text => Datum::new_string(value),
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

    /// Go `parseLog` over every line of the files, in order.
    fn parse(&mut self, files: &[LogFile]) -> Result<Vec<Vec<Datum>>, String> {
        let mut data = Vec::new();
        let mut row: Vec<Option<Datum>> = Vec::new();
        let mut user = String::new();
        let mut start_flag = false;
        for file in files {
            let mut reader = open_reader(&file.path, file.compressed)?;
            let mut line_num = 0usize;
            while let Some(line) = read_line(reader.as_mut())? {
                line_num += 1;
                if !start_flag {
                    if let Some(value) = line.strip_prefix(SLOW_LOG_START_PREFIX_STR) {
                        row = vec![None; self.columns.len()];
                        user.clear();
                        if self.set_column_value(&mut row, "Time", value, line_num) {
                            start_flag = true;
                        }
                    }
                    continue;
                }
                if let Some(rest) = line.strip_prefix(SLOW_LOG_ROW_PREFIX_STR) {
                    let mut valid = true;
                    if let Some(value) = rest.strip_prefix(SLOW_LOG_PREV_STMT_PREFIX) {
                        valid = self.set_column_value(&mut row, "Prev_stmt", value, line_num);
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
                        if !self.set_column_value(&mut row, "User", &user_value, line_num) {
                            start_flag = false;
                            continue;
                        }
                        let host = parse_user_or_host_value(host_part);
                        valid = self.set_column_value(&mut row, "Host", &host, line_num);
                    } else if rest.starts_with("Cop_backoff_") {
                        valid = self.set_column_value(&mut row, "Backoff_Detail", rest, line_num);
                    } else if let Some(value) = rest.strip_prefix("Warnings: ") {
                        valid = self.set_column_value(&mut row, "Warnings", value, line_num);
                    } else if let Some(value) = rest.strip_prefix("Session_connect_attrs: ") {
                        valid = self.set_column_value(
                            &mut row,
                            "Session_connect_attrs",
                            value,
                            line_num,
                        );
                    } else if let Some(value) = rest.strip_prefix("Request_unit_v2_detail: ") {
                        valid = self.set_column_value(
                            &mut row,
                            "Request_unit_v2_detail",
                            value,
                            line_num,
                        );
                    } else if let Some(value) = rest.strip_prefix("DB: ") {
                        valid = self.set_column_value(&mut row, "DB", value, line_num);
                    } else {
                        let (fields, values) = split_by_colon(rest);
                        for (field, value) in fields.iter().zip(values.iter()) {
                            if !self.set_column_value(&mut row, field, value, line_num) {
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
                    let _ = self.set_column_value(&mut row, "Query", &line, line_num);
                    // Go `setDefaultValue`: an unset column takes its type's
                    // zero value.
                    let finished = row
                        .iter_mut()
                        .zip(self.columns.iter())
                        .map(|(value, (_, field_type))| {
                            value
                                .take()
                                .unwrap_or_else(|| crate::bad_null::zero_value(field_type))
                        })
                        .collect();
                    data.push(finished);
                    start_flag = false;
                } else {
                    start_flag = false;
                }
            }
        }
        Ok(data)
    }
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
) -> Result<SlowQueryRows, String> {
    let mut warnings = Vec::new();
    let files = get_all_files(checker, log_file_path, &mut warnings)?;
    let mut parser = Parser {
        columns,
        kinds: columns.iter().map(|(name, _)| column_kind(name)).collect(),
        checker,
        tz,
        warnings,
    };
    let rows = parser.parse(&files)?;
    Ok(SlowQueryRows {
        rows,
        warnings: parser.warnings,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
