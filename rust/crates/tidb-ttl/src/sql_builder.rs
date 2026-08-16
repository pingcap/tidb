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

//! Transcreation of Go `pkg/ttl/sqlbuilder/sql.go`: the SQL text TTL sends to
//! scan expired rows and to delete them.

use std::fmt::Write as _;

use tidb_ast::{RestoreCtx, RestoreFlags, RestoreWriter};
use tidb_datatype::{Datum, FieldType};
use tidb_model::ColumnInfo;
use tidb_util::sqlescape::escape_string;

pub use crate::cache::table::PhysicalTable;

/// This package's error, standing in for Go's `errors.New`/`errors.Errorf`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlBuilderError(pub String);

impl std::fmt::Display for SqlBuilderError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for SqlBuilderError {}

/// The package's `Result` alias.
pub type Result<T> = std::result::Result<T, SqlBuilderError>;

fn error(text: impl Into<String>) -> SqlBuilderError {
    SqlBuilderError(text.into())
}

/// Go's `sqlbuilder` imports `PhysicalTable` from `pkg/ttl/cache`; that package
/// is transcreated in this crate, so the real type is used here rather than a
/// local redeclaration.
impl From<crate::cache::CacheError> for SqlBuilderError {
    fn from(error: crate::cache::CacheError) -> Self {
        Self(error.0)
    }
}

/// Go `writeHex`.
fn write_hex<W: RestoreWriter>(ctx: &mut RestoreCtx<W>, datum: &Datum) {
    let mut text = String::from("x'");
    for byte in datum.go_bytes() {
        let _ = write!(&mut text, "{byte:02x}");
    }
    text.push('\'');
    ctx.write_plain(&text);
}

/// Go `writeDatum`.
///
/// The escaped-string branch is where Go writes raw bytes into a
/// `strings.Builder`. Rust's `RestoreCtx` writes through `std::fmt::Write`, so
/// a value that is not valid UTF-8 after escaping — reachable only for a
/// non-binary `latin1`-style column holding arbitrary bytes — is reported as an
/// error here instead of being emitted verbatim. Every binary-flagged and
/// blob-typed column already takes the hex branch above, so no TTL key column
/// that the scan/delete paths build reaches this case.
fn write_datum<W: RestoreWriter>(
    ctx: &mut RestoreCtx<W>,
    datum: &Datum,
    field_type: &FieldType,
) -> Result<()> {
    match field_type.code().mysql_type() {
        tidb_mysql::TypeBit
        | tidb_mysql::TypeBlob
        | tidb_mysql::TypeLongBlob
        | tidb_mysql::TypeTinyBlob => {
            write_hex(ctx, datum);
            return Ok(());
        }
        tidb_mysql::TypeString
        | tidb_mysql::TypeVarString
        | tidb_mysql::TypeVarchar
        | tidb_mysql::TypeEnum
        | tidb_mysql::TypeSet => {
            if tidb_mysql::has_binary_flag(field_type.flags() as usize) {
                write_hex(ctx, datum);
                return Ok(());
            }
            let escaped = escape_string(datum.go_bytes());
            let escaped = String::from_utf8(escaped).map_err(|_| {
                error("the datum is not valid UTF-8 after escaping and cannot be written as text")
            })?;
            ctx.write_plain(&format!("'{escaped}'"));
            return Ok(());
        }
        _ => {}
    }
    write_value_expr(ctx, datum, field_type.charset_name())
}

/// Go `ast.NewValueExpr(d.GetValue(), charset, collate).Restore(ctx)`.
///
/// `boundary:` Go builds a `parser_driver.ValueExpr` — an AST node carrying the
/// datum plus the field type `types.DefaultTypeForValue` derives from it — and
/// calls its `Restore`. This workspace's `tidb-ast` has no `ValueExpr` node, so
/// the restore body is transcreated directly from
/// `pkg/types/parser_driver/value_expr.go`. The collation argument is dropped
/// because that `Restore` never reads it, and the boolean-flag branch is
/// dropped because `DefaultTypeForValue` never sets `IsBooleanFlag`.
fn write_value_expr<W: RestoreWriter>(
    ctx: &mut RestoreCtx<W>,
    datum: &Datum,
    charset: &str,
) -> Result<()> {
    match datum {
        Datum::Null => ctx.write_keyword("NULL"),
        Datum::Int(value) => ctx.write_plain(&value.to_string()),
        Datum::UInt(value) => ctx.write_plain(&value.to_string()),
        Datum::Float32(value) | Datum::Real(value) => {
            ctx.write_plain(&tidb_util::sqlescape::format_go_float64(*value));
        }
        Datum::String(_) => {
            // Go writes the `_charset` introducer unless the restore flags
            // suppress it.
            if !charset.is_empty() && !ctx.flags.contains(RestoreFlags::STRING_WITHOUT_CHARSET) {
                ctx.write_plain("_");
                ctx.write_keyword(charset);
            }
            let text = datum_utf8(datum)?;
            // Go replaces `\` with `\\` regardless of NO_BACKSLASH_ESCAPES.
            ctx.write_string(&text.replace('\\', "\\\\"));
        }
        Datum::Bytes(_) => {
            let text = datum_utf8(datum)?;
            ctx.write_string(&text);
        }
        Datum::Decimal(value) => ctx.write_plain(&value.to_string()),
        Datum::Time(value) => ctx.write_plain(&format!("'{value}'")),
        Datum::Duration(value) => ctx.write_plain(&format!("'{value}'")),
        Datum::BinaryLiteral(_)
        | Datum::Bit(_)
        | Datum::Enum(_, _)
        | Datum::Set(_, _)
        | Datum::Json(_)
        | Datum::Raw(_)
        | Datum::VectorFloat32(_)
        | Datum::MinNotNull
        | Datum::MaxValue => return Err(error("Not implemented")),
    }
    Ok(())
}

fn datum_utf8(datum: &Datum) -> Result<String> {
    String::from_utf8(datum.go_bytes().to_vec())
        .map_err(|_| error("the datum is not valid UTF-8 and cannot be written as text"))
}

/// Go `FormatSQLDatum`.
pub fn format_sql_datum(datum: &Datum, field_type: &FieldType) -> Result<String> {
    let mut ctx = RestoreCtx::new(RestoreFlags::DEFAULT, String::new());
    write_datum(&mut ctx, datum, field_type)?;
    Ok(ctx.into_inner())
}

/// Go's unexported `sqlBuilderState`. Go prefixes every constant with `write`
/// (`writeBegin`, `writeSelOrDel`, ...); the prefix is dropped here because the
/// enum name already carries it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SqlBuilderState {
    Begin,
    SelOrDel,
    Where,
    OrderBy,
    Limit,
    Done,
}

impl SqlBuilderState {
    /// Go formats the state with `%v` on an untyped `int`-backed constant, so
    /// the error text carries the ordinal.
    fn ordinal(self) -> u8 {
        match self {
            Self::Begin => 0,
            Self::SelOrDel => 1,
            Self::Where => 2,
            Self::OrderBy => 3,
            Self::Limit => 4,
            Self::Done => 5,
        }
    }
}

/// Go `SQLBuilder`: builds SQLs for TTL.
pub struct SqlBuilder {
    tbl: PhysicalTable,
    restore_ctx: RestoreCtx<String>,
    state: SqlBuilderState,

    is_read_only: bool,
    has_write_expire_cond: bool,
}

impl SqlBuilder {
    /// Go `NewSQLBuilder`.
    ///
    /// Go keeps `sb` and a `restoreCtx` pointing into it; Rust's `RestoreCtx`
    /// owns its writer, so the single `String` lives inside the context.
    pub fn new(tbl: &PhysicalTable) -> Self {
        Self {
            tbl: tbl.clone(),
            restore_ctx: RestoreCtx::new(RestoreFlags::DEFAULT, String::new()),
            state: SqlBuilderState::Begin,
            is_read_only: false,
            has_write_expire_cond: false,
        }
    }

    fn invalid_state(&self) -> SqlBuilderError {
        error(format!("invalid state: {}", self.state.ordinal()))
    }

    /// Go `Build`.
    pub fn build(&mut self) -> Result<String> {
        if self.state == SqlBuilderState::Begin {
            return Err(self.invalid_state());
        }

        if !self.is_read_only && !self.has_write_expire_cond {
            // check whether the `timeRow < expire_time` condition has been
            // written to make sure this SQL is safe.
            return Err(error("expire condition not write"));
        }

        if self.state != SqlBuilderState::Done {
            self.state = SqlBuilderState::Done;
        }

        Ok(self.restore_ctx.writer.clone())
    }

    /// Go `WriteSelect`.
    pub fn write_select(&mut self) -> Result<()> {
        if self.state != SqlBuilderState::Begin {
            return Err(self.invalid_state());
        }
        self.restore_ctx
            .write_plain("SELECT LOW_PRIORITY SQL_NO_CACHE ");
        let key_columns = self.tbl.key_columns.clone();
        self.write_col_names(&key_columns, false);
        self.restore_ctx.write_plain(" FROM ");
        self.write_tbl_name();
        if let Some(partition) = self.tbl.partition_def.clone() {
            self.restore_ctx.write_plain(" PARTITION(");
            self.restore_ctx.write_name(partition.name.original());
            self.restore_ctx.write_plain(")");
        }
        self.state = SqlBuilderState::SelOrDel;
        self.is_read_only = true;
        Ok(())
    }

    /// Go `WriteDelete`.
    pub fn write_delete(&mut self) -> Result<()> {
        if self.state != SqlBuilderState::Begin {
            return Err(self.invalid_state());
        }
        self.restore_ctx.write_plain("DELETE LOW_PRIORITY FROM ");
        self.write_tbl_name();
        if let Some(partition) = self.tbl.partition_def.clone() {
            self.restore_ctx.write_plain(" PARTITION(");
            self.restore_ctx.write_name(partition.name.original());
            self.restore_ctx.write_plain(")");
        }
        self.state = SqlBuilderState::SelOrDel;
        Ok(())
    }

    fn open_condition(&mut self) -> Result<()> {
        match self.state {
            SqlBuilderState::SelOrDel => {
                self.restore_ctx.write_plain(" WHERE ");
                self.state = SqlBuilderState::Where;
                Ok(())
            }
            SqlBuilderState::Where => {
                self.restore_ctx.write_plain(" AND ");
                Ok(())
            }
            _ => Err(self.invalid_state()),
        }
    }

    /// Go `WriteCommonCondition`.
    pub fn write_common_condition(
        &mut self,
        cols: &[ColumnInfo],
        op: &str,
        dp: &[Datum],
    ) -> Result<()> {
        self.open_condition()?;

        self.write_col_names(cols, cols.len() > 1);
        self.restore_ctx.write_plain(" ");
        self.restore_ctx.write_plain(op);
        self.restore_ctx.write_plain(" ");
        self.write_data_point(cols, dp)
    }

    /// Go `WriteExpireCondition`; `expire` is the Unix second the time column
    /// is compared against.
    pub fn write_expire_condition(&mut self, expire_unix: i64) -> Result<()> {
        self.open_condition()?;

        let time_column = self.tbl.time_column_ref()?.clone();
        self.write_col_names(std::slice::from_ref(&time_column), false);
        self.restore_ctx.write_plain(" < ");
        self.restore_ctx.write_plain("FROM_UNIXTIME(");
        self.restore_ctx.write_plain(&expire_unix.to_string());
        self.restore_ctx.write_plain(")");
        self.has_write_expire_cond = true;
        Ok(())
    }

    /// Go `WriteInCondition`.
    pub fn write_in_condition(&mut self, cols: &[ColumnInfo], dps: &[Vec<Datum>]) -> Result<()> {
        self.open_condition()?;

        self.write_col_names(cols, cols.len() > 1);
        self.restore_ctx.write_plain(" IN ");
        self.restore_ctx.write_plain("(");
        let mut first = true;
        for value in dps {
            if first {
                first = false;
            } else {
                self.restore_ctx.write_plain(", ");
            }
            self.write_data_point(cols, value)?;
        }
        self.restore_ctx.write_plain(")");
        Ok(())
    }

    /// Go `WriteOrderBy`.
    pub fn write_order_by(&mut self, cols: &[ColumnInfo], desc: bool) -> Result<()> {
        if self.state != SqlBuilderState::SelOrDel && self.state != SqlBuilderState::Where {
            return Err(self.invalid_state());
        }
        self.state = SqlBuilderState::OrderBy;
        self.restore_ctx.write_plain(" ORDER BY ");
        self.write_col_names(cols, false);
        if desc {
            self.restore_ctx.write_plain(" DESC");
        } else {
            self.restore_ctx.write_plain(" ASC");
        }
        Ok(())
    }

    /// Go `WriteLimit`.
    pub fn write_limit(&mut self, n: i64) -> Result<()> {
        if self.state != SqlBuilderState::SelOrDel
            && self.state != SqlBuilderState::Where
            && self.state != SqlBuilderState::OrderBy
        {
            return Err(self.invalid_state());
        }
        self.state = SqlBuilderState::Limit;
        self.restore_ctx.write_plain(" LIMIT ");
        self.restore_ctx.write_plain(&n.to_string());
        Ok(())
    }

    /// Go `writeTblName`, which restores an `ast.TableName{Schema, Name}`.
    ///
    /// `boundary:` this workspace's `tidb-ast` has no `TableName` node, so the
    /// two names are written directly — which is exactly what `TableName`'s own
    /// `restoreName` does for a node with no index hints or partition names.
    fn write_tbl_name(&mut self) {
        let schema = self.tbl.schema.original().to_string();
        let name = self.tbl.name_original();
        if !schema.is_empty() {
            self.restore_ctx.write_name(&schema);
            self.restore_ctx.write_plain(".");
        }
        self.restore_ctx.write_name(&name);
    }

    /// Go `writeColName`.
    fn write_col_name(&mut self, col: &ColumnInfo) {
        self.restore_ctx.write_name(col.name.original());
    }

    /// Go `writeColNames`.
    fn write_col_names(&mut self, cols: &[ColumnInfo], write_brackets: bool) {
        if write_brackets {
            self.restore_ctx.write_plain("(");
        }

        let mut first = true;
        for col in cols {
            if first {
                first = false;
            } else {
                self.restore_ctx.write_plain(", ");
            }
            self.write_col_name(col);
        }

        if write_brackets {
            self.restore_ctx.write_plain(")");
        }
    }

    /// Go `writeDataPoint`.
    fn write_data_point(&mut self, cols: &[ColumnInfo], dp: &[Datum]) -> Result<()> {
        let write_brackets = cols.len() > 1;
        if cols.len() != dp.len() {
            return Err(error(format!(
                "col count not match {} != {}",
                cols.len(),
                dp.len()
            )));
        }

        if write_brackets {
            self.restore_ctx.write_plain("(");
        }

        let mut first = true;
        for (index, datum) in dp.iter().enumerate() {
            if first {
                first = false;
            } else {
                self.restore_ctx.write_plain(", ");
            }
            write_datum(&mut self.restore_ctx, datum, &cols[index].field_type)?;
        }

        if write_brackets {
            self.restore_ctx.write_plain(")");
        }

        Ok(())
    }
}

/// Go `ScanQueryGenerator`: generates SQLs for a scan task.
pub struct ScanQueryGenerator {
    tbl: PhysicalTable,
    expire_unix: i64,
    key_range_start: Vec<Datum>,
    key_range_end: Vec<Datum>,
    stack: Vec<Vec<Datum>>,
    limit: usize,
    first_build: bool,
    exhausted: bool,
}

impl ScanQueryGenerator {
    /// Go `NewScanQueryGenerator`; `expire_unix` is the Unix second of Go's
    /// `expire time.Time`, the only thing the generator reads from it.
    pub fn new(
        tbl: &PhysicalTable,
        expire_unix: i64,
        range_start: &[Datum],
        range_end: &[Datum],
    ) -> Result<Self> {
        tbl.validate_key_prefix(range_start)?;
        tbl.validate_key_prefix(range_end)?;

        Ok(Self {
            tbl: tbl.clone(),
            expire_unix,
            key_range_start: range_start.to_vec(),
            key_range_end: range_end.to_vec(),
            stack: Vec::new(),
            limit: 0,
            first_build: true,
            exhausted: false,
        })
    }

    /// Go `NextSQL`.
    pub fn next_sql(
        &mut self,
        continue_from_result: &[Vec<Datum>],
        next_limit: i64,
    ) -> Result<String> {
        if self.exhausted {
            return Err(error("generator is exhausted"));
        }

        if next_limit <= 0 {
            return Err(error(format!("invalid limit '{next_limit}'")));
        }

        if continue_from_result.len() >= self.limit {
            let continue_from_key = continue_from_result.last().cloned();
            self.set_stack(continue_from_key.as_deref())?;
        } else {
            self.stack.pop();
            if self.stack.is_empty() {
                self.exhausted = true;
            }
        }
        self.limit = next_limit as usize;
        let sql = self.build_sql();
        self.first_build = false;
        sql
    }

    /// Go `IsExhausted`.
    pub fn is_exhausted(&self) -> bool {
        self.exhausted
    }

    /// Go `setStack`. Go's nil `key` selects the range start; an empty slice
    /// carries the same meaning here, since nothing constructs a non-nil empty
    /// key.
    fn set_stack(&mut self, key: Option<&[Datum]>) -> Result<()> {
        let key = match key.filter(|key| !key.is_empty()) {
            Some(key) => key.to_vec(),
            None => self.key_range_start.clone(),
        };

        if key.is_empty() {
            self.stack.clear();
            return Ok(());
        }

        self.tbl.validate_key_prefix(&key)?;

        self.stack = (0..key.len()).map(|i| key[..=i].to_vec()).collect();
        Ok(())
    }

    /// Go `buildSQL`.
    fn build_sql(&mut self) -> Result<String> {
        if self.limit == 0 {
            return Err(error("invalid limit '0'"));
        }

        if self.exhausted {
            return Ok(String::new());
        }

        let mut builder = SqlBuilder::new(&self.tbl);
        builder.write_select()?;
        if let Some(top) = self.stack.last().cloned() {
            let depth = self.stack.len();
            for (index, datum) in top.iter().enumerate() {
                let col = vec![self.tbl.key_columns[index].clone()];
                let val = vec![datum.clone()];
                if index < depth - 1 {
                    builder.write_common_condition(&col, "=", &val)?;
                } else if self.first_build {
                    // When this is the first build we are querying rows after
                    // the range start; the range is `[start, end)`, so `>=`
                    // keeps the start key itself.
                    builder.write_common_condition(&col, ">=", &val)?;
                } else {
                    // Otherwise we are continuing from the previous result, so
                    // `>` excludes the row already returned.
                    builder.write_common_condition(&col, ">", &val)?;
                }
            }
        }

        if !self.key_range_end.is_empty() {
            let cols = self.tbl.key_columns[0..self.key_range_end.len()].to_vec();
            builder.write_common_condition(&cols, "<", &self.key_range_end)?;
        }

        builder.write_expire_condition(self.expire_unix)?;
        let key_columns = self.tbl.key_columns.clone();
        builder.write_order_by(&key_columns, false)?;
        builder.write_limit(self.limit as i64)?;

        builder.build()
    }
}

/// Go `BuildDeleteSQL`; `expire_unix` is the Unix second of Go's `expire`.
pub fn build_delete_sql(
    tbl: &PhysicalTable,
    rows: &[Vec<Datum>],
    expire_unix: i64,
) -> Result<String> {
    if rows.is_empty() {
        return Err(error("Cannot build delete SQL with empty rows"));
    }

    let mut builder = SqlBuilder::new(tbl);
    builder.write_delete()?;
    builder.write_in_condition(&tbl.key_columns, rows)?;
    builder.write_expire_condition(expire_unix)?;
    builder.write_limit(rows.len() as i64)?;
    builder.build()
}
