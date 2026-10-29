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

//! Go `importer.NewImportPlan` option validation (`initOptions`) followed by
//! `LoadDataController.checkFieldParams`' path and format checks: the part of
//! `IMPORT INTO` that decides, before any file is touched, whether the
//! statement's `FORMAT` and `WITH` options are acceptable.

use tidb_ast::{Expr, ImportIntoStmt, ImportSource, LoadDataOption, UnaryOp};

use crate::DriverError;

/// Go `supportedOptions`: whether the option takes a value.
const SUPPORTED_OPTIONS: &[(&str, bool)] = &[
    ("character_set", true),
    ("fields_terminated_by", true),
    ("fields_enclosed_by", true),
    ("fields_escaped_by", true),
    ("fields_defined_null_by", true),
    ("lines_terminated_by", true),
    ("skip_rows", true),
    ("group_key", true),
    ("split_file", false),
    ("disk_quota", true),
    ("thread", true),
    ("max_write_speed", true),
    ("checksum_table", true),
    ("record_errors", true),
    ("on_duplicate_key", true),
    ("detached", false),
    ("disable_tikv_import_mode", false),
    ("__max_engine_size", true),
    ("__force_merge_step", false),
    ("__manual_recovery", false),
    ("cloud_storage_uri", true),
    ("disable_precheck", false),
];

/// Go `csvOnlyOptions`.
const CSV_ONLY_OPTIONS: &[&str] = &[
    "character_set",
    "fields_terminated_by",
    "fields_enclosed_by",
    "fields_escaped_by",
    "fields_defined_null_by",
    "lines_terminated_by",
    "skip_rows",
    "split_file",
];

/// Go `allowedOptionsOfImportFromQuery`.
const QUERY_ALLOWED_OPTIONS: &[&str] = &["thread", "disable_precheck"];

/// What the constant an option's literal folds to is, in the terms Go's
/// `optAsString` / `optAsInt64` test.
enum OptionValue {
    Null,
    /// `TypeVarString`.
    Text(String),
    /// `TypeLonglong` without the boolean flag.
    Int(i64),
    /// An integer-typed `TRUE` / `FALSE` (`IsBooleanFlag`).
    Bool,
    /// Any other constant type (decimal, double, bit).
    Other,
}

fn code(proto: &'static tidb_terror::TerrorError, message: String) -> DriverError {
    DriverError::from((proto).generate_with_stack(message))
}

fn invalid(name: &str) -> DriverError {
    code(
        &tidb_dbterror::exeerrors::ERR_INVALID_OPTION_VAL,
        format!("Invalid option value for {name}"),
    )
}

fn unsupported_option(name: &str, reason: &str) -> DriverError {
    code(
        &tidb_dbterror::exeerrors::ERR_LOAD_DATA_UNSUPPORTED_OPTION,
        format!("Unsupported option {name} for {reason}"),
    )
}

fn classify(value: &Expr) -> OptionValue {
    match value {
        Expr::Null => OptionValue::Null,
        Expr::Bool(_) => OptionValue::Bool,
        Expr::String(text) => OptionValue::Text(text.clone()),
        Expr::CharsetString { value, .. } => OptionValue::Text(value.clone()),
        Expr::Hex(digits) => OptionValue::Text(digits.clone()),
        Expr::Int(text) => text.parse::<i64>().map_or(OptionValue::Other, OptionValue::Int),
        Expr::Unary(UnaryOp::Plus, inner) => match classify(inner) {
            OptionValue::Int(value) => OptionValue::Int(value),
            _ => OptionValue::Other,
        },
        Expr::Unary(UnaryOp::Minus, inner) => match classify(inner) {
            OptionValue::Int(value) => value.checked_neg().map_or(OptionValue::Other, OptionValue::Int),
            _ => OptionValue::Other,
        },
        _ => OptionValue::Other,
    }
}

/// Go `optAsString`.
fn as_string(option: &LoadDataOption) -> Result<String, DriverError> {
    match option.value.as_ref().map(classify) {
        Some(OptionValue::Text(text)) => Ok(text),
        _ => Err(invalid(&option.name)),
    }
}

/// Go `optAsInt64`.
fn as_int(option: &LoadDataOption) -> Result<i64, DriverError> {
    match option.value.as_ref().map(classify) {
        Some(OptionValue::Int(value)) => Ok(value),
        _ => Err(invalid(&option.name)),
    }
}

/// `units.RAMInBytes`: `^(\d+(\.\d+)*) ?([kKmMgGtTpP])?[iI]?[bB]?$`, binary
/// multiples, fractions truncated.
fn ram_in_bytes(text: &str) -> Option<i64> {
    let number_end = {
        let bytes = text.as_bytes();
        let mut end = 0;
        while end < bytes.len() && bytes[end].is_ascii_digit() {
            end += 1;
        }
        if end == 0 {
            return None;
        }
        // `(\.\d+)*`: any number of dot-digit groups.
        loop {
            if end < bytes.len() && bytes[end] == b'.' {
                let mut next = end + 1;
                while next < bytes.len() && bytes[next].is_ascii_digit() {
                    next += 1;
                }
                if next == end + 1 {
                    break;
                }
                end = next;
            } else {
                break;
            }
        }
        end
    };
    let mut rest = &text[number_end..];
    rest = rest.strip_prefix(' ').unwrap_or(rest);
    let mut multiplier = 1.0_f64;
    if let Some(unit) = rest.chars().next() {
        let power = match unit.to_ascii_lowercase() {
            'k' => Some(1),
            'm' => Some(2),
            'g' => Some(3),
            't' => Some(4),
            'p' => Some(5),
            _ => None,
        };
        if let Some(power) = power {
            multiplier = 1024_f64.powi(power);
            rest = &rest[unit.len_utf8()..];
        }
    }
    rest = rest.strip_prefix(['i', 'I']).unwrap_or(rest);
    rest = rest.strip_prefix(['b', 'B']).unwrap_or(rest);
    if !rest.is_empty() {
        return None;
    }
    // The number text is `\d+(\.\d+)*`, which `ParseFloat` rejects once it has
    // a second fractional group.
    let number: f64 = text[..number_end].parse().ok()?;
    Some((number * multiplier) as i64)
}

/// `objstore.ParseBackend` + `isSupportedCloudStorageBackend`: only the S3,
/// GCS and Azure families are cloud storage backends.
fn is_supported_cloud_storage_uri(uri: &str) -> bool {
    let Some((scheme, rest)) = uri.split_once("://") else {
        return false;
    };
    if scheme.is_empty() || !scheme.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'+' || byte == b'-' || byte == b'.') {
        return false;
    }
    let bucket = rest.split(['/', '?']).next().unwrap_or_default();
    matches!(
        scheme.to_ascii_lowercase().as_str(),
        "s3" | "ks3" | "oss" | "gcs" | "gs" | "azure" | "azblob"
    ) && !bucket.is_empty()
}

/// Go `config.ParseCharset`.
fn is_supported_data_charset(charset: &str) -> bool {
    matches!(
        charset.to_ascii_lowercase().as_str(),
        "" | "binary" | "utf8" | "utf8mb4" | "gb18030" | "gbk" | "latin1" | "ascii"
    )
}

/// Go `naming.CheckWithMaxLen`: letters, digits, `-` and `_`.
fn is_valid_name(name: &str, max_len: usize) -> bool {
    name.len() <= max_len
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
}

/// Validates an `IMPORT INTO` statement the way Go's planner and executor do
/// before reading its source: `initOptions` in its order, then the path and
/// format checks of `checkFieldParams`.
pub fn validate_import_into_options(stmt: &ImportIntoStmt) -> Result<(), DriverError> {
    let (format, path) = match &stmt.source {
        ImportSource::File { path, format } => (
            format
                .as_deref()
                .map_or_else(|| "auto".to_owned(), str::to_ascii_lowercase),
            Some(path.as_str()),
        ),
        ImportSource::Select { .. } => ("auto".to_owned(), None),
    };

    let mut specified: Vec<&LoadDataOption> = Vec::with_capacity(stmt.options.len());
    for option in &stmt.options {
        let Some((_, has_value)) = SUPPORTED_OPTIONS
            .iter()
            .find(|(name, _)| *name == option.name)
        else {
            return Err(code(
                &tidb_dbterror::exeerrors::ERR_UNKNOWN_OPTION,
                format!("Unknown option {}", option.name),
            ));
        };
        if *has_value != option.value.is_some() {
            return Err(invalid(&option.name));
        }
        if specified.iter().any(|seen| seen.name == option.name) {
            return Err(code(
                &tidb_dbterror::exeerrors::ERR_DUPLICATE_OPTION,
                format!("Option {} specified more than once", option.name),
            ));
        }
        specified.push(option);
    }
    let find = |name: &str| specified.iter().copied().find(|option| option.name == name);

    if format != "csv" && format != "auto" {
        for name in CSV_ONLY_OPTIONS {
            if find(name).is_some() {
                return Err(unsupported_option(name, "non-CSV format"));
            }
        }
    }
    if path.is_none() {
        for option in &specified {
            if !QUERY_ALLOWED_OPTIONS.contains(&option.name.as_str()) {
                return Err(unsupported_option(&option.name, "import from query"));
            }
        }
    }

    if let Some(option) = find("character_set") {
        let value = as_string(option)?;
        if value.is_empty() || !is_supported_data_charset(&value) {
            return Err(invalid(&option.name));
        }
    }
    if let Some(option) = find("fields_terminated_by") {
        if as_string(option)?.is_empty() {
            return Err(invalid(&option.name));
        }
    }
    for name in ["fields_enclosed_by", "fields_escaped_by"] {
        if let Some(option) = find(name) {
            if as_string(option)?.len() > 1 {
                return Err(invalid(&option.name));
            }
        }
    }
    if let Some(option) = find("fields_defined_null_by") {
        as_string(option)?;
    }
    // `newDefaultLineFieldsInfo` leaves IMPORT INTO's line terminator empty
    // (the CSV parser detects it).
    let mut lines_terminated_by = String::new();
    if let Some(option) = find("lines_terminated_by") {
        lines_terminated_by = as_string(option)?;
        if lines_terminated_by.is_empty() {
            return Err(invalid(&option.name));
        }
    }
    let mut skip_rows = 0;
    if let Some(option) = find("skip_rows") {
        skip_rows = as_int(option)?;
        if skip_rows < 0 {
            return Err(invalid(&option.name));
        }
    }
    let split_file = find("split_file").is_some();
    if let Some(option) = find("disk_quota") {
        match ram_in_bytes(&as_string(option)?) {
            Some(bytes) if bytes > 0 => {}
            _ => return Err(invalid(&option.name)),
        }
    }
    if let Some(option) = find("thread") {
        if as_int(option)? <= 0 {
            return Err(invalid(&option.name));
        }
    }
    if let Some(option) = find("max_write_speed") {
        match ram_in_bytes(&as_string(option)?) {
            Some(bytes) if bytes >= 0 => {}
            _ => return Err(invalid(&option.name)),
        }
    }
    if let Some(option) = find("checksum_table") {
        let level = as_string(option)?.to_ascii_lowercase();
        if !matches!(
            level.as_str(),
            "off" | "false" | "required" | "true" | "optional"
        ) {
            return Err(invalid(&option.name));
        }
    }
    let mut on_duplicate_key = false;
    if let Some(option) = find("on_duplicate_key") {
        on_duplicate_key = true;
        let mode = as_string(option)?.to_ascii_lowercase();
        if mode != "capture" && mode != "error" {
            return Err(invalid(&option.name));
        }
    }
    if let Some(option) = find("group_key") {
        let value = as_string(option)?;
        if value.is_empty() || !is_valid_name(&value, 256) {
            return Err(invalid(&option.name));
        }
    }
    if let Some(option) = find("record_errors") {
        if as_int(option)? < -1 {
            return Err(invalid(&option.name));
        }
    }
    // An empty `cloud_storage_uri` forces local sort; otherwise the global
    // `tidb_cloud_storage_uri` (empty by default) decides.
    let mut cloud_storage_uri = String::new();
    if let Some(option) = find("cloud_storage_uri") {
        cloud_storage_uri = as_string(option)?;
        if !cloud_storage_uri.is_empty() && !is_supported_cloud_storage_uri(&cloud_storage_uri) {
            return Err(invalid(&option.name));
        }
    }
    if let Some(option) = find("__max_engine_size") {
        match ram_in_bytes(&as_string(option)?) {
            Some(bytes) if bytes >= 0 => {}
            _ => return Err(invalid(&option.name)),
        }
    }
    if on_duplicate_key && cloud_storage_uri.is_empty() {
        return Err(unsupported_option("on_duplicate_key", "local sort"));
    }
    if split_file && skip_rows > 1 {
        return Err(invalid(
            "skip_rows, should be <= 1 when split-file is enabled",
        ));
    }
    if split_file && lines_terminated_by.is_empty() {
        return Err(invalid(
            "lines_terminated_by, should not be empty when use split_file",
        ));
    }

    // Go `checkFieldParams`.
    if let Some("") = path {
        return Err(code(
            &tidb_dbterror::exeerrors::ERR_LOAD_DATA_EMPTY_PATH,
            "The value of INFILE must not be empty when LOAD DATA from LOCAL".to_owned(),
        ));
    }
    if !matches!(format.as_str(), "csv" | "parquet" | "sql" | "auto") {
        return Err(code(
            &tidb_dbterror::exeerrors::ERR_LOAD_DATA_UNSUPPORTED_FORMAT,
            format!("The FORMAT '{format}' is not supported"),
        ));
    }
    Ok(())
}
