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

//! Go `pkg/executor/select_into.go`: `SELECT ... INTO OUTFILE`, which drains
//! the target query into a file instead of answering rows.

use std::io::Write;

use tidb_chunk::chunk::Chunk;
use tidb_datatype::{FieldType, FieldTypeCode, FieldTypeFlags};
use tidb_datatype::EvalType;
use tidb_planner::explain::LineFieldsInfo;

use crate::driver::RecordSet;
use crate::DriverError;

/// Go `SelectIntoExec`.
pub struct SelectIntoExec {
    file_name: String,
    line_fields_info: LineFieldsInfo,
    line_buf: Vec<u8>,
    real_buf: Vec<u8>,
    field_buf: Vec<u8>,
    escape_buf: Vec<u8>,
    enclosed: bool,
    writer: Option<std::io::BufWriter<std::fs::File>>,
    /// Go `StmtCtx.AddAffectedRows`: the rows written.
    affected_rows: u64,
}

impl SelectIntoExec {
    /// Go `buildSelectInto`: the target query's `IntoOpt` and its
    /// `NewLineFieldsInfo(intoOpt.FieldsInfo, intoOpt.LinesInfo)`.
    #[must_use]
    pub fn new(into: &tidb_ast::SelectIntoOption) -> Self {
        Self {
            file_name: into.file_name.clone(),
            line_fields_info: tidb_planner::explain::new_line_fields_info(
                Some(&into.fields),
                Some(&into.lines),
            ),
            line_buf: Vec::new(),
            real_buf: Vec::new(),
            field_buf: Vec::new(),
            escape_buf: Vec::new(),
            enclosed: false,
            writer: None,
            affected_rows: 0,
        }
    }

    /// Go `SelectIntoExec.Open`: the file must not exist; MySQL-compatible
    /// behavior lets it be group-readable.
    pub fn open(&mut self) -> Result<(), DriverError> {
        #[cfg(unix)]
        let file = {
            use std::os::unix::fs::OpenOptionsExt;
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .mode(0o640)
                .open(&self.file_name)
        };
        #[cfg(not(unix))]
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&self.file_name);
        let file = file.map_err(|error| {
            driver_unknown(format!("open {}: {}", self.file_name, go_errno_text(&error)))
        })?;
        self.writer = Some(std::io::BufWriter::new(file));
        self.line_buf = Vec::with_capacity(1024);
        self.field_buf = Vec::with_capacity(64);
        self.escape_buf = Vec::with_capacity(64);
        Ok(())
    }

    /// Go `SelectIntoExec.Next` over the target's record set: every chunk is
    /// dumped until the target is drained, its rows added to the statement's
    /// affected rows (`StmtCtx.AddAffectedRows`).
    pub fn next(&mut self, child: &mut RecordSet, ctx: &crate::StmtContext) -> Result<(), DriverError> {
        let columns: Vec<FieldType> = child.columns().iter().map(|(_, ty)| ty.clone()).collect();
        let mut chk = child.new_chunk();
        loop {
            child.next(&mut chk)?;
            if chk.num_rows() == 0 {
                break;
            }
            self.dump_to_outfile(&chk, &columns)?;
            ctx.add_affected_rows(chk.num_rows() as u64);
        }
        Ok(())
    }

    /// Go `considerEncloseOpt`.
    fn consider_enclose_opt(et: EvalType) -> bool {
        matches!(
            et,
            EvalType::String
                | EvalType::Duration
                | EvalType::Timestamp
                | EvalType::Datetime
                | EvalType::Json
        )
    }

    /// Go `escapeField`.
    fn escape_field(&mut self, f: &[u8]) -> Vec<u8> {
        let info = &self.line_fields_info;
        let escaped_by = info.fields_escaped_by.as_bytes();
        if escaped_by.is_empty() {
            return f.to_vec();
        }
        let enclosed_by = info.fields_enclosed_by.as_bytes();
        let terminated_by = info.fields_terminated_by.as_bytes();
        let lines_terminated_by = info.lines_terminated_by.as_bytes();
        self.escape_buf.clear();
        for &byte in f {
            let mut b = byte;
            let escape = if b == 0 {
                // we always escape 0
                b = b'0';
                true
            } else if b == escaped_by[0] || (!enclosed_by.is_empty() && b == enclosed_by[0]) {
                true
            } else if !self.enclosed && !terminated_by.is_empty() && b == terminated_by[0] {
                // if field is enclosed, we only escape line terminator,
                // otherwise both field and line terminator will be escaped
                true
            } else {
                // we always escape line terminator
                !lines_terminated_by.is_empty() && b == lines_terminated_by[0]
            };
            if escape {
                self.escape_buf.push(escaped_by[0]);
            }
            self.escape_buf.push(b);
        }
        self.escape_buf.clone()
    }

    /// Go `dumpToOutfile`: writes the current chunk using the configured
    /// OUTFILE format. Line prefixes are written literally, outside field
    /// escaping and enclosure.
    fn dump_to_outfile(&mut self, chk: &Chunk, cols: &[FieldType]) -> Result<(), DriverError> {
        let info = self.line_fields_info.clone();
        let (enclose_flag, enclose_byte, enclose_opt) = match info.fields_enclosed_by.as_bytes().first() {
            Some(&byte) => (true, byte, info.fields_opt_enclosed),
            None => (false, 0, false),
        };
        let null_term: Vec<u8> = match info.fields_escaped_by.as_bytes().first() {
            Some(&escape) => vec![escape, b'N'],
            None => b"NULL".to_vec(),
        };
        let starting_by = info.lines_starting_by.as_bytes();
        for i in 0..chk.num_rows() {
            let row = chk.get_row(i);
            self.line_buf.clear();
            self.line_buf.extend_from_slice(starting_by);
            for (j, col) in cols.iter().enumerate() {
                if j != 0 {
                    self.line_buf.extend_from_slice(info.fields_terminated_by.as_bytes());
                }
                if row.is_null(j) {
                    self.line_buf.extend_from_slice(&null_term);
                    continue;
                }
                let et = col.eval_type();
                let enclose = (enclose_flag && !enclose_opt)
                    || (enclose_flag && enclose_opt && Self::consider_enclose_opt(et));
                if enclose {
                    self.line_buf.push(enclose_byte);
                    self.enclosed = true;
                } else {
                    self.enclosed = false;
                }
                self.field_buf.clear();
                match col.code() {
                    FieldTypeCode::Tiny
                    | FieldTypeCode::Short
                    | FieldTypeCode::Int24
                    | FieldTypeCode::Long
                    | FieldTypeCode::Year => {
                        self.field_buf.extend_from_slice(row.get_int64(j).to_string().as_bytes());
                    }
                    FieldTypeCode::LongLong => {
                        if col.has_flag(FieldTypeFlags::UNSIGNED) {
                            self.field_buf.extend_from_slice(row.get_uint64(j).to_string().as_bytes());
                        } else {
                            self.field_buf.extend_from_slice(row.get_int64(j).to_string().as_bytes());
                        }
                    }
                    FieldTypeCode::Float => {
                        dump_real_outfile(&mut self.real_buf, &mut self.field_buf, f64::from(row.get_float32(j)), col);
                    }
                    FieldTypeCode::Double => {
                        dump_real_outfile(&mut self.real_buf, &mut self.field_buf, row.get_float64(j), col);
                    }
                    FieldTypeCode::NewDecimal => {
                        self.field_buf.extend_from_slice(&row.get_my_decimal(j).to_string_bytes());
                    }
                    FieldTypeCode::String
                    | FieldTypeCode::VarString
                    | FieldTypeCode::Varchar
                    | FieldTypeCode::TinyBlob
                    | FieldTypeCode::MediumBlob
                    | FieldTypeCode::LongBlob
                    | FieldTypeCode::Blob => {
                        self.field_buf.extend_from_slice(&row.get_bytes(j));
                    }
                    // bit value won't be escaped anyway (verified on MySQL)
                    FieldTypeCode::Bit => self.line_buf.extend_from_slice(&row.get_bytes(j)),
                    FieldTypeCode::Date | FieldTypeCode::Datetime | FieldTypeCode::Timestamp => {
                        self.field_buf.extend_from_slice(row.get_time(j).to_string().as_bytes());
                    }
                    FieldTypeCode::Duration => {
                        self.field_buf.extend_from_slice(
                            row.get_duration(j, col.decimal()).to_string().as_bytes(),
                        );
                    }
                    FieldTypeCode::Enum => {
                        self.field_buf.extend_from_slice(row.get_enum(j).to_string().as_bytes());
                    }
                    FieldTypeCode::Set => {
                        self.field_buf.extend_from_slice(row.get_set(j).to_string().as_bytes());
                    }
                    FieldTypeCode::Json => {
                        self.field_buf.extend_from_slice(row.get_json(j).to_string().as_bytes());
                    }
                    FieldTypeCode::VectorFloat32 => {
                        self.field_buf
                            .extend_from_slice(row.get_vector_float32(j).to_string().as_bytes());
                    }
                    _ => {}
                }
                match et {
                    EvalType::String | EvalType::Json => {
                        let field = std::mem::take(&mut self.field_buf);
                        let escaped = self.escape_field(&field);
                        self.field_buf = field;
                        self.line_buf.extend_from_slice(&escaped);
                    }
                    _ => self.line_buf.extend_from_slice(&self.field_buf),
                }
                if enclose {
                    self.line_buf.push(enclose_byte);
                }
            }
            self.line_buf.extend_from_slice(info.lines_terminated_by.as_bytes());
            self.writer
                .as_mut()
                .expect("SelectIntoExec writes after Open")
                .write_all(&self.line_buf)
                .map_err(|error| driver_unknown(error.to_string()))?;
        }
        self.affected_rows += chk.num_rows() as u64;
        Ok(())
    }

    /// Go `SelectIntoExec.Close`: flushes, then closes the file.
    pub fn close(&mut self) -> Result<u64, DriverError> {
        if let Some(writer) = self.writer.take() {
            writer
                .into_inner()
                .map_err(|error| driver_unknown(error.error().to_string()))?
                .sync_all()
                .ok();
        }
        Ok(self.affected_rows)
    }
}

/// Go `os.OpenFile`'s error text without the Go path prefix: the errno's
/// lowercase description (`file exists`, `no such file or directory`).
fn go_errno_text(error: &std::io::Error) -> String {
    match error.kind() {
        std::io::ErrorKind::AlreadyExists => "file exists".to_owned(),
        std::io::ErrorKind::NotFound => "no such file or directory".to_owned(),
        std::io::ErrorKind::PermissionDenied => "permission denied".to_owned(),
        _ => {
            let text = error.to_string();
            // Rust's `Display` appends ` (os error N)`; Go's does not.
            let text = text.split(" (os error").next().unwrap_or(&text);
            let mut chars = text.chars();
            chars
                .next()
                .map(|first| first.to_lowercase().chain(chars).collect())
                .unwrap_or_default()
        }
    }
}

fn driver_unknown(message: String) -> DriverError {
    DriverError::Mysql(crate::MysqlError::unknown(message))
}

const EXP_FORMAT_BIG: f64 = 1e15;
const EXP_FORMAT_SMALL: f64 = 1e-15;

/// Go `DumpRealOutfile`: dumps a real number to `line_buf`.
pub fn dump_real_outfile(real_buf: &mut Vec<u8>, line_buf: &mut Vec<u8>, v: f64, tp: &FieldType) {
    let mut prec = tidb_parser_types::field_type::UNSPECIFIED_LENGTH as i32;
    if tp.decimal() > 0 && tp.decimal() != i64::from(crate::adapter::NOT_FIXED_DEC) {
        prec = tp.decimal() as i32;
    }
    let abs_v = v.abs();
    if prec == tidb_parser_types::field_type::UNSPECIFIED_LENGTH as i32
        && (abs_v >= EXP_FORMAT_BIG || (abs_v != 0.0 && abs_v < EXP_FORMAT_SMALL))
    {
        real_buf.clear();
        tidb_goruntime::go_strconv::append_float(real_buf, v, b'e', prec, 64);
        if let Some(idx) = real_buf.iter().position(|&b| b == b'+') {
            line_buf.extend_from_slice(&real_buf[..idx]);
            line_buf.extend_from_slice(&real_buf[idx + 1..]);
        } else {
            line_buf.extend_from_slice(real_buf);
        }
    } else {
        tidb_goruntime::go_strconv::append_float(line_buf, v, b'f', prec, 64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dump_real() {
        // Go `TestDumpReal` (`pkg/executor/select_into_test.go`).
        let unspecified = tidb_parser_types::field_type::UNSPECIFIED_LENGTH;
        let cases: &[(f64, i64, &str)] = &[
            (1.2, 1, "1.2"),
            (1.2, 2, "1.20"),
            (2.0, 2, "2.00"),
            (2.333, unspecified, "2.333"),
            (1e14, unspecified, "100000000000000"),
            (1e15, unspecified, "1e15"),
            (1e-15, unspecified, "0.000000000000001"),
            (1e-16, unspecified, "1e-16"),
        ];
        for &(val, dec, result) in cases {
            let mut tp = FieldType::new(FieldTypeCode::Double);
            tp.set_decimal(dec);
            let (mut real_buf, mut buf) = (Vec::new(), Vec::new());
            dump_real_outfile(&mut real_buf, &mut buf, val, &tp);
            assert_eq!(String::from_utf8(buf).unwrap(), result, "{val} {dec}");
        }
    }
}
