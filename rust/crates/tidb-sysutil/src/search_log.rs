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

//! Go `search_log.go` and the `SearchLog` loop of `service.go`.
//!
//! Log lines are bytes; they become protobuf strings, which must be UTF-8 in
//! Rust, through a lossy conversion (Go's gogoproto passes invalid bytes
//! through unchanged).

use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::Path;

use flate2::read::MultiGzDecoder;
use regex::Regex;
use tidb_proto::diagnosticspb as pb;

/// Why a search stopped early.
#[derive(Debug)]
pub(crate) enum SearchLogError {
    /// The client went away (Go `ctx.Err()`), so nothing more is sent.
    Canceled,
    /// Go's returned error.
    Failed(String),
}

impl From<String> for SearchLogError {
    fn from(message: String) -> Self {
        SearchLogError::Failed(message)
    }
}

/// Go's `ctx.Done()` probe.
pub(crate) type Done<'a> = &'a dyn Fn() -> bool;

/// Go `DiagnosticsServer.SearchLog` after its panic guard: resolve the
/// files, then send batches of at most 1024 matching messages; the drained
/// batch is sent too, so a search always sends at least one response.
pub(crate) fn search_log(
    log_file: &str,
    req: &pb::SearchLogRequest,
    done: Done<'_>,
    send: &mut dyn FnMut(pb::SearchLogResponse) -> Result<(), ()>,
) -> Result<(), SearchLogError> {
    let begin_time = req.start_time;
    let mut end_time = req.end_time;
    if end_time == 0 {
        end_time = i64::MAX;
    }

    let log_files = resolve_files(done, log_file, begin_time, end_time)?;

    let mut level_flag: i64 = 0;
    for l in &req.levels {
        // Go `1 << l` on an int64 shift count; counts past 63 yield zero.
        level_flag |= u32::try_from(*l)
            .ok()
            .and_then(|l| 1i64.checked_shl(l))
            .unwrap_or(0);
    }
    let mut patterns = Vec::with_capacity(req.patterns.len());
    for p in &req.patterns {
        patterns.push(Regex::new(p).map_err(|error| SearchLogError::Failed(error.to_string()))?);
    }
    let mut iter = LogIterator {
        begin: begin_time,
        end: end_time,
        level_flag,
        patterns,
        file_index: 0,
        reader: None,
        pending: log_files,
        pre_log: None,
    };

    loop {
        let mut messages = Vec::new();
        let mut drained = false;
        for _ in 0..1024 {
            match iter.next(done)? {
                None => {
                    drained = true;
                    break;
                }
                Some(item) => messages.push(item),
            }
        }
        if send(pb::SearchLogResponse { messages }).is_err() {
            return Err(SearchLogError::Canceled);
        }
        if drained {
            break;
        }
    }
    Ok(())
}

/// Go `logFile`: an open log file and the times of its first and last
/// valid lines, in Unix milliseconds.
#[derive(Debug)]
pub(crate) struct LogFile {
    file: File,
    begin: i64,
    // Read only through `end_time`, which Go exports for its tests.
    #[cfg_attr(not(test), allow(dead_code))]
    end: i64,
    compressed: bool,
}

#[cfg_attr(not(test), allow(dead_code))]
impl LogFile {
    /// Go `logFile.BeginTime`.
    pub(crate) fn begin_time(&self) -> i64 {
        self.begin
    }

    /// Go `logFile.EndTime`.
    pub(crate) fn end_time(&self) -> i64 {
        self.end
    }
}

const COMPRESS_SUFFIX: &str = ".gz";

/// Go's `os.PathError` text: `op path: errno text`.
fn path_error(op: &str, path: &str, error: &std::io::Error) -> String {
    let text = match error.raw_os_error() {
        Some(errno) => {
            let desc = nix::errno::Errno::from_raw(errno).desc();
            let mut chars = desc.chars();
            match chars.next() {
                Some(first) => first.to_lowercase().chain(chars).collect(),
                None => String::new(),
            }
        }
        None => error.to_string(),
    };
    format!("{op} {path}: {text}")
}

/// Go `filepath.Dir`, `filepath.Ext` over a slash-separated path.
fn dir_and_ext(path: &str) -> (String, &str) {
    let dir = match path.rfind('/') {
        Some(0) => "/".to_owned(),
        Some(index) => clean(&path[..index]),
        None => ".".to_owned(),
    };
    let ext = match path.rfind(['.', '/']) {
        Some(index) if path.as_bytes()[index] == b'.' => &path[index..],
        _ => "",
    };
    (dir, ext)
}

/// Go `filepath.Clean`.
fn clean(path: &str) -> String {
    if path.is_empty() {
        return ".".to_owned();
    }
    let rooted = path.starts_with('/');
    let mut parts: Vec<&str> = Vec::new();
    for part in path.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                if parts.last().is_some_and(|last| *last != "..") {
                    parts.pop();
                } else if !rooted {
                    parts.push("..");
                }
            }
            part => parts.push(part),
        }
    }
    let joined = parts.join("/");
    match (rooted, joined.is_empty()) {
        (true, _) => format!("/{joined}"),
        (false, true) => ".".to_owned(),
        (false, false) => joined,
    }
}

/// Go `filepath.Join(dir, name)` for one plain directory entry name.
fn join(dir: &str, name: &str) -> String {
    clean(&format!("{dir}/{name}"))
}

/// Go `resolveFiles`: every rotated sibling of the log file (same prefix,
/// same extension, optionally gzipped) whose valid lines overlap the range,
/// sorted by first line; leading files wholly before `begin_time` are
/// dropped, keeping the one that may still hold it.
pub(crate) fn resolve_files(
    done: Done<'_>,
    log_file_path: &str,
    begin_time: i64,
    end_time: i64,
) -> Result<Vec<LogFile>, SearchLogError> {
    if log_file_path.is_empty() {
        return Err("empty log file location configuration".to_owned().into());
    }

    let mut log_files = Vec::new();
    let (log_dir, ext) = dir_and_ext(log_file_path);
    let file_prefix = &log_file_path[..log_file_path.len() - ext.len()];
    let mut names = std::fs::read_dir(&log_dir)
        .and_then(|entries| {
            entries
                .map(|entry| {
                    entry.and_then(|entry| Ok((entry.file_name(), entry.file_type()?.is_dir())))
                })
                .collect::<std::io::Result<Vec<_>>>()
        })
        .map_err(|error| path_error("open", &log_dir, &error))?;
    // Go `os.ReadDir` sorts by name.
    names.sort();
    for (name, is_dir) in names {
        let path = join(&log_dir, &name.to_string_lossy());
        if is_dir {
            continue;
        }
        // All rotated log files have the same prefix and extension with the original file
        if !path.starts_with(file_prefix) {
            continue;
        }
        let compressed = path.ends_with(COMPRESS_SUFFIX);
        if !path.ends_with(ext) && !path.ends_with(&format!("{ext}{COMPRESS_SUFFIX}")) {
            continue;
        }
        if done() {
            return Err(SearchLogError::Canceled);
        }
        // A file that cannot be opened is skipped rather than failing the
        // whole search.
        let Ok(mut file) = File::open(Path::new(&path)) else {
            continue;
        };
        let first_item = if compressed {
            let Some(decoder) = gzip_reader(&file) else {
                continue;
            };
            read_first_valid_log(done, &mut BufReader::new(decoder), 10)
        } else {
            read_first_valid_log(done, &mut BufReader::new(&file), 10)
        };
        let Ok(first_item) = first_item else {
            continue;
        };
        let first_item_time = first_item.time;

        let last_item_time = if !compressed {
            match read_last_valid_log(done, &mut file, 10) {
                Ok(item) => item.time,
                Err(_) => continue,
            }
        } else {
            // For compressed file, it's hard to get last item,
            // and to avoid decompression, we assume lastTime equals to `math.MaxInt64`.
            i64::MAX
        };
        // Reset position to the start and skip this file if cannot seek to start
        if file.seek(SeekFrom::Start(0)).is_err() {
            continue;
        }

        if begin_time > last_item_time || end_time < first_item_time {
            continue;
        }
        log_files.push(LogFile {
            file,
            begin: first_item_time,
            end: last_item_time,
            compressed,
        });
    }

    // Sort by start time
    log_files.sort_by_key(|file| file.begin);

    // Assume no time range overlap in log files and remove unnecessary log files for compressed files.
    // When logFiles[i-1].end < begin < logFiles[i].begin, it will return one more logFiles[i-1].
    let mut idx = 0;
    for (i, file) in log_files.iter().enumerate().skip(1) {
        if file.begin < begin_time {
            idx = i;
        } else {
            break;
        }
    }
    Ok(log_files.split_off(idx))
}

/// Go `gzip.NewReader`, which reads and checks the header eagerly.
fn gzip_reader<R: Read>(reader: R) -> Option<MultiGzDecoder<R>> {
    let decoder = MultiGzDecoder::new(reader);
    decoder.header()?;
    Some(decoder)
}

/// Go `readFirstValidLog`: the first of up to `try_lines` lines that parses.
fn read_first_valid_log(
    done: Done<'_>,
    reader: &mut dyn BufRead,
    try_lines: i64,
) -> Result<pb::LogMessage, SearchLogError> {
    let mut tried = 0;
    loop {
        let Some(line) = read_line(reader).map_err(SearchLogError::Failed)? else {
            return Err("EOF".to_owned().into());
        };
        if let Ok(item) = parse_log_item(&line) {
            return Ok(item);
        }
        tried += 1;
        if tried >= try_lines {
            break;
        }
        if done() {
            return Err(SearchLogError::Canceled);
        }
    }
    Err("not a valid log file".to_owned().into())
}

/// Go `readLastValidLog`: the last parsing line among roughly the last
/// `try_lines` lines.
fn read_last_valid_log(
    done: Done<'_>,
    file: &mut File,
    try_lines: usize,
) -> Result<pb::LogMessage, SearchLogError> {
    let mut tried = 0;
    let mut end_cursor = file.metadata().map_or(0, |stat| stat.len() as i64);
    loop {
        let (lines, read_bytes) = read_last_lines(done, file, end_cursor)?;
        // read out the file
        if read_bytes == 0 {
            break;
        }
        end_cursor -= read_bytes as i64;
        for line in lines.iter().rev() {
            if let Ok(item) = parse_log_item(line) {
                return Ok(item);
            }
        }
        tried += lines.len();
        if tried >= try_lines {
            break;
        }
    }
    Err("not a valid log file".to_owned().into())
}

/// Go `readLine`: one line without its `\n` or `\r\n`; `None` at EOF.
/// A read error after part of a line returns the part first, as
/// `bufio.Reader.ReadLine` does.
fn read_line(reader: &mut dyn BufRead) -> Result<Option<String>, String> {
    let mut line = Vec::new();
    loop {
        let available = match reader.fill_buf() {
            Ok(available) => available,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => {
                if line.is_empty() {
                    return Err(error.to_string());
                }
                break;
            }
        };
        if available.is_empty() {
            if line.is_empty() {
                return Ok(None);
            }
            break;
        }
        match available.iter().position(|&b| b == b'\n') {
            Some(end) => {
                line.extend_from_slice(&available[..end]);
                reader.consume(end + 1);
                if line.last() == Some(&b'\r') {
                    line.pop();
                }
                return Ok(Some(String::from_utf8_lossy(&line).into_owned()));
            }
            None => {
                let len = available.len();
                line.extend_from_slice(available);
                reader.consume(len);
            }
        }
    }
    Ok(Some(String::from_utf8_lossy(&line).into_owned()))
}

const MAX_READ_CACHE_SIZE: i64 = 1024 * 1024 * 16;

/// Go `readLastLines`: the complete lines at the end of the file before
/// `end_cursor` (initially the file size), and how many bytes they span.
pub(crate) fn read_last_lines(
    done: Done<'_>,
    file: &mut File,
    end_cursor: i64,
) -> Result<(Vec<String>, usize), SearchLogError> {
    let mut lines: Vec<u8> = Vec::new();
    let mut first_non_newline_pos = 0;
    let mut cursor = end_cursor;
    let mut size: i64 = 256;
    loop {
        // stop if we are at the begining
        // check it in the start to avoid read beyond the size
        if cursor <= 0 {
            break;
        }

        // enlarge the read cache to avoid too many memory move.
        size *= 2;
        if size > MAX_READ_CACHE_SIZE {
            size = MAX_READ_CACHE_SIZE;
        }
        if cursor < size {
            size = cursor;
        }
        cursor -= size;

        // Go returns `ctx.Err()` here, which is nil unless canceled: a
        // failed seek or read ends with no lines and no error.
        if file.seek(SeekFrom::Start(cursor as u64)).is_err() {
            return if done() {
                Err(SearchLogError::Canceled)
            } else {
                Ok((Vec::new(), 0))
            };
        }
        let mut chars = vec![0u8; size as usize];
        if read_once(file, &mut chars).is_err() {
            return if done() {
                Err(SearchLogError::Canceled)
            } else {
                Ok((Vec::new(), 0))
            };
        }
        let mut joined = chars.clone();
        joined.extend_from_slice(&lines);
        lines = joined;

        // find first '\n' or '\r'
        for i in 0..chars.len().saturating_sub(1) {
            // reach the line end
            // the first newline may be in the line end at the first round
            if i >= lines.len() - 1 {
                break;
            }
            if (chars[i] == 10 || chars[i] == 13) && chars[i + 1] != 10 && chars[i + 1] != 13 {
                first_non_newline_pos = i + 1;
                break;
            }
        }
        if first_non_newline_pos > 0 {
            break;
        }
        if done() {
            return Err(SearchLogError::Canceled);
        }
    }
    let final_bytes = &lines[first_non_newline_pos..];
    let final_str = String::from_utf8_lossy(final_bytes).replace("\r\n", "\n");
    Ok((
        final_str.split('\n').map(str::to_owned).collect(),
        final_bytes.len(),
    ))
}

/// Go `os.File.Read`: one read call; a short read leaves the rest zero, and
/// reading at end of file is `io.EOF`.
fn read_once(file: &mut File, buf: &mut [u8]) -> std::io::Result<()> {
    loop {
        match file.read(buf) {
            Ok(0) if !buf.is_empty() => {
                return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof))
            }
            Ok(_) => return Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
}

/// Go `ParseLogLevel`: the level named by `s`, `UNKNOWN` for anything else.
#[must_use]
pub fn parse_log_level(s: &str) -> pb::LogLevel {
    match s {
        "debug" | "DEBUG" => pb::LogLevel::Debug,
        "info" | "INFO" => pb::LogLevel::Info,
        "warn" | "WARN" => pb::LogLevel::Warn,
        "trace" | "TRACE" => pb::LogLevel::Trace,
        "critical" | "CRITICAL" => pb::LogLevel::Critical,
        "error" | "ERROR" => pb::LogLevel::Error,
        _ => pb::LogLevel::Unknown,
    }
}

/// Go `parseLogItem`: `[time] [LEVEL] message...` in the unified log format.
///
/// ```text
/// [2019/08/26 06:19:13.011 -04:00] [INFO] [printer.go:41] ["Welcome to TiDB."] ["Release Version"=v2.1.14]...
/// [2019/08/21 01:43:01.460 -04:00] [INFO] [util.go:60] [PD] [release-version=v3.0.2]
/// [2019/08/26 07:20:23.815 -04:00] [INFO] [mod.rs:28] ["Release Version:   3.0.2"]
/// ```
pub(crate) fn parse_log_item(s: &str) -> Result<pb::LogMessage, String> {
    let invalid = || format!("invalid log string: {s}");
    let (Some(time_left_bound), Some(time_right_bound)) = (s.find('['), s.find(']')) else {
        return Err(invalid());
    };
    if time_left_bound > time_right_bound {
        return Err(invalid());
    }
    let time = parse_time_stamp(&s[time_left_bound + 1..time_right_bound])?;
    let rest = &s[time_right_bound + 1..];
    let (Some(level_left_bound), Some(level_right_bound)) = (rest.find('['), rest.find(']')) else {
        return Err(invalid());
    };
    if level_left_bound > level_right_bound {
        return Err(invalid());
    }
    let level = parse_log_level(&rest[level_left_bound + 1..level_right_bound]);
    Ok(pb::LogMessage {
        time,
        level: level as i32,
        message: s[time_right_bound + level_right_bound + 2..]
            .trim()
            .to_owned(),
    })
}

/// Go `TimeStampLayout`, accessed by the dashboard.
pub const TIME_STAMP_LAYOUT: &str = "2006/01/02 15:04:05.000 -07:00";
const TIME_STAMP_LAYOUT_LEN: usize = TIME_STAMP_LAYOUT.len();

/// Go `parseTimeStamp`: `time.Parse(TimeStampLayout, s)` in Unix
/// milliseconds.
pub(crate) fn parse_time_stamp(s: &str) -> Result<i64, String> {
    let (unix_secs, nanos) = parse_layout(s.as_bytes()).map_err(|(value, elem)| {
        format!(
            "parsing time {:?} as {:?}: cannot parse {:?} as {:?}",
            s,
            TIME_STAMP_LAYOUT,
            String::from_utf8_lossy(value),
            elem
        )
    })?;
    // Go `t.UnixNano() / int64(time.Millisecond)`, wrapping like Go.
    Ok(unix_secs.wrapping_mul(1_000_000_000).wrapping_add(nanos) / 1_000_000)
}

/// Go `getnum`: one or two digits; `fixed` requires two.
fn getnum(value: &[u8], fixed: bool) -> Option<(i64, &[u8])> {
    if value.first().is_none_or(|c| !c.is_ascii_digit()) {
        return None;
    }
    if value.get(1).is_none_or(|c| !c.is_ascii_digit()) {
        if fixed {
            return None;
        }
        return Some((i64::from(value[0] - b'0'), &value[1..]));
    }
    Some((
        i64::from(value[0] - b'0') * 10 + i64::from(value[1] - b'0'),
        &value[2..],
    ))
}

/// Go `skip`: match the layout literal; a space matches one or more spaces.
fn skip<'a>(mut value: &'a [u8], mut prefix: &[u8]) -> Option<&'a [u8]> {
    fn cutspace(text: &[u8]) -> &[u8] {
        let n = text.iter().take_while(|&&b| b == b' ').count();
        &text[n..]
    }
    while let Some(&first) = prefix.first() {
        if first == b' ' {
            if value.first().is_some_and(|&b| b != b' ') {
                return None;
            }
            prefix = cutspace(prefix);
            value = cutspace(value);
            continue;
        }
        if value.first() != Some(&first) {
            return None;
        }
        prefix = &prefix[1..];
        value = &value[1..];
    }
    Some(value)
}

/// Go `atoi`: an optionally signed decimal that must consume the input.
fn atoi(s: &[u8]) -> Option<i64> {
    let (neg, digits) = match s.first() {
        Some(b'-') => (true, &s[1..]),
        Some(b'+') => (false, &s[1..]),
        _ => (false, s),
    };
    let mut x: i64 = 0;
    for &b in digits {
        if !b.is_ascii_digit() {
            return None;
        }
        x = x.checked_mul(10)?.checked_add(i64::from(b - b'0'))?;
    }
    Some(if neg { -x } else { x })
}

type LayoutError<'a> = (&'a [u8], &'static str);

/// `time.Parse` over [`TIME_STAMP_LAYOUT`] alone: the layout elements in
/// order, with Go's literal, digit-count and range rules. The result is the
/// Unix second and nanosecond. Errors carry the unparsed value and the
/// layout element.
fn parse_layout(value: &[u8]) -> Result<(i64, i64), LayoutError<'_>> {
    let v = value;
    // "2006"
    if v.len() < 4 || !v[0].is_ascii_digit() {
        return Err((v, "2006"));
    }
    let year = atoi(&v[..4]).ok_or((v, "2006"))?;
    let v = &v[4..];
    let v = skip(v, b"/").ok_or((v, "/"))?;
    // "01"
    let (month, v) = getnum(v, true).ok_or((v, "01"))?;
    if !(1..=12).contains(&month) {
        return Err((v, "01"));
    }
    let v = skip(v, b"/").ok_or((v, "/"))?;
    // "02": any two-digit day, validated against the month at the end.
    let (day, v) = getnum(v, true).ok_or((v, "02"))?;
    let v = skip(v, b" ").ok_or((v, " "))?;
    // "15"
    let (hour, v) = getnum(v, false).ok_or((v, "15"))?;
    if !(0..24).contains(&hour) {
        return Err((v, "15"));
    }
    let v = skip(v, b":").ok_or((v, ":"))?;
    // "04"
    let (min, v) = getnum(v, true).ok_or((v, "04"))?;
    if !(0..60).contains(&min) {
        return Err((v, "04"));
    }
    let v = skip(v, b":").ok_or((v, ":"))?;
    // "05"
    let (sec, v) = getnum(v, true).ok_or((v, "05"))?;
    if !(0..60).contains(&sec) {
        return Err((v, "05"));
    }
    // ".000": exactly three fractional digits after '.' or ','.
    if v.len() < 4 || !matches!(v[0], b'.' | b',') {
        return Err((v, ".000"));
    }
    let frac = atoi(&v[1..4]).ok_or((v, ".000"))?;
    if frac < 0 {
        return Err((v, ".000"));
    }
    let nsec = frac * 1_000_000;
    let v = &v[4..];
    let v = skip(v, b" ").ok_or((v, " "))?;
    // "-07:00"
    if v.len() < 6 || v[3] != b':' {
        return Err((v, "-07:00"));
    }
    let (hr, _) = getnum(&v[1..3], true).ok_or((v, "-07:00"))?;
    let (mn, _) = getnum(&v[4..6], true).ok_or((v, "-07:00"))?;
    // Go's range test uses > rather than >=.
    if hr > 24 || mn > 60 {
        return Err((v, "-07:00"));
    }
    let mut zone_offset = (hr * 60 + mn) * 60;
    match v[0] {
        b'+' => {}
        b'-' => zone_offset = -zone_offset,
        _ => return Err((v, "-07:00")),
    }
    let v = &v[6..];
    if !v.is_empty() {
        return Err((v, ""));
    }
    if day < 1 || day > days_in(month, year) {
        return Err((value, "02"));
    }
    let days = days_from_civil(year, month, day);
    let unix = days * 86_400 + hour * 3600 + min * 60 + sec - zone_offset;
    Ok((unix, nsec))
}

fn is_leap(year: i64) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

fn days_in(month: i64, year: i64) -> i64 {
    match month {
        2 if is_leap(year) => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

/// Days since 1970-01-01 in the proleptic Gregorian calendar.
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (month + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// Go `logIterator`: reads the pending files in order, yielding the
/// messages in range that pass the level and pattern filters. A line that
/// does not parse continues the previous message with its time and level.
struct LogIterator {
    // filters
    begin: i64,
    end: i64,
    level_flag: i64,
    patterns: Vec<Regex>,

    // inner state
    file_index: usize,
    reader: Option<Box<dyn BufRead + Send>>,
    pending: Vec<LogFile>,
    pre_log: Option<pb::LogMessage>,
}

impl LogIterator {
    /// Go `updateToNextReader`.
    fn update_to_next_reader(&mut self) -> Result<(), SearchLogError> {
        let pending = &self.pending[self.file_index];
        let file = pending
            .file
            .try_clone()
            .map_err(|error| SearchLogError::Failed(error.to_string()))?;
        self.reader = Some(if !pending.compressed {
            Box::new(BufReader::new(file))
        } else {
            let decoder = gzip_reader(file)
                .ok_or_else(|| SearchLogError::Failed("gzip: invalid header".to_owned()))?;
            Box::new(BufReader::new(decoder))
        });
        Ok(())
    }

    /// Go `logIterator.next`; `None` is Go's `io.EOF`.
    fn next(&mut self, done: Done<'_>) -> Result<Option<pb::LogMessage>, SearchLogError> {
        // initial state
        if self.reader.is_none() {
            if self.pending.is_empty() {
                return Ok(None);
            }
            self.update_to_next_reader()?;
        }

        'next_line: loop {
            if done() {
                return Err(SearchLogError::Canceled);
            }
            let reader = self.reader.as_mut().expect("reader installed above");
            let line = match read_line(reader.as_mut()) {
                // Switch to next log file
                Ok(None) => {
                    self.file_index += 1;
                    if self.file_index >= self.pending.len() {
                        return Ok(None);
                    }
                    self.update_to_next_reader()?;
                    continue;
                }
                Ok(Some(line)) => line,
                // Go keeps going with the empty line a failed read returns.
                Err(_) => String::new(),
            };
            let line = line.trim();
            if self.pre_log.is_none() && line.len() < TIME_STAMP_LAYOUT_LEN {
                continue;
            }
            let item = match parse_log_item(line) {
                Ok(item) => {
                    self.pre_log = Some(item.clone());
                    item
                }
                Err(_) => {
                    let Some(pre_log) = &self.pre_log else {
                        continue;
                    };
                    // handle invalid log
                    // make whole line as log message with pre time and pre log_level
                    pb::LogMessage {
                        time: pre_log.time,
                        level: pre_log.level,
                        message: line.to_owned(),
                    }
                }
            };
            // It assumes no time range overlap for log files.
            if item.time > self.end {
                return Ok(None);
            }
            if item.time < self.begin {
                continue;
            }
            // always keep unknown log_level
            if item.level > pb::LogLevel::Unknown as i32
                && self.level_flag != 0
                && self.level_flag & (1i64 << item.level) == 0
            {
                continue;
            }
            for p in &self.patterns {
                if !p.is_match(&item.message) {
                    continue 'next_line;
                }
            }
            return Ok(Some(item));
        }
    }
}
