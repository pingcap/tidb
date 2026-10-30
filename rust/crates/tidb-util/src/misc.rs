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

//! Go `pkg/util/misc.go`: the statement-level helpers of the root `util`
//! package.

use tidb_datatype::DateModes;

/// Go `GetTypeFlagsForInsert` / `ResetUpdateStmtCtx`, in the two bits that
/// decide whether a zero-in or invalid date PARSES at all.
///
/// The `IgnoreZeroInDate` expression is Go's verbatim, and its shape is the
/// point: the zero-in-date is let through unless `NO_ZERO_IN_DATE` **and**
/// `NO_ZERO_DATE` **and** strict mode are all present and
/// `ALLOW_INVALID_DATES` is absent. Any one of those missing and
/// `'2024-00-01'` reaches Go `handleZeroDatetime` as a real value instead of
/// a conversion failure -- which is what makes `NO_ZERO_IN_DATE` alone a
/// warning rather than an error.
#[must_use]
pub fn write_date_flags(
    base: tidb_datatype::ConversionFlags,
    modes: DateModes,
    strict: bool,
) -> tidb_datatype::ConversionFlags {
    base.with_ignore_invalid_date_err(modes.allow_invalid_dates)
        .with_ignore_zero_in_date_err(
            !modes.no_zero_in_date || !modes.no_zero_date || !strict || modes.allow_invalid_dates,
        )
}

/// Go `SyntaxErrorPrefix`: the common prefix for SQL syntax errors in TiDB.
pub const SYNTAX_ERROR_PREFIX: &str = "You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use";

/// Go `SyntaxError` for a parser failure that is not already a
/// `*terror.Error`: `parser.ErrParse.GenWithStackByArgs(SyntaxErrorPrefix,
/// err.Error())`.
pub fn syntax_error(message: impl Into<String>) -> tidb_terror::TerrorError {
    tidb_parser::parser_errors::ERR_PARSE
        .generate_with_stack_by_args(&[SYNTAX_ERROR_PREFIX.into(), message.into().into()])
}

/// Go `util.SequenceTable`: the sequence operations a table object of a
/// `CREATE SEQUENCE` provides, so expression evaluation can drive one without
/// depending on the table layer. `ctx` is Go's session argument; the
/// database and sequence names are what its errors name.
pub trait SequenceTable: std::fmt::Debug + Send + Sync {
    /// Go `GetSequenceID`.
    fn get_sequence_id(&self) -> i64;

    /// Go `GetSequenceNextVal`.
    fn get_sequence_next_val(
        &self,
        ctx: &dyn std::any::Any,
        db_name: &str,
        seq_name: &str,
    ) -> Result<i64, tidb_terror::TerrorError>;

    /// Go `SetSequenceVal`: the value set, and whether the counter was
    /// already at or past `new_val` (Go's `alreadySatisfied`, nothing set).
    fn set_sequence_val(
        &self,
        ctx: &dyn std::any::Any,
        new_val: i64,
        db_name: &str,
        seq_name: &str,
    ) -> Result<(i64, bool), tidb_terror::TerrorError>;
}

/// Go `util.GetSequenceByName` over a statement's infoschema: the
/// sequences its `NEXTVAL` / `LASTVAL` / `SETVAL` resolve by name.
pub trait SequenceSchema: std::fmt::Debug + Send + Sync {
    /// The session's current database, lowercase: the schema of a bare name.
    fn current_db(&self) -> &str;

    /// Go `infoschema.GetSequenceByName`: the sequence `schema.sequence`
    /// names, or the infoschema's error for a missing object or an object
    /// that is not a sequence.
    fn get_sequence_by_name(
        &self,
        schema: &str,
        sequence: &str,
    ) -> Result<&dyn SequenceTable, tidb_terror::TerrorError>;

    /// The `(database, name)` a one- or two-part sequence path names.
    fn schema_and_name<'a>(&'a self, path: &'a [String]) -> Option<(&'a str, &'a str)> {
        match path {
            [name] => Some((self.current_db(), name.as_str())),
            [database, name] => Some((database.as_str(), name.as_str())),
            _ => None,
        }
    }

    /// The lowercase `(database, name)` the lookup and its errors name for
    /// `path`.
    fn key(&self, path: &[String]) -> (String, String) {
        use tidb_hack::GoToLower;
        match path {
            [name] => (self.current_db().to_string(), name.go_to_lower()),
            [database, name] => (database.go_to_lower(), name.go_to_lower()),
            [] => (String::new(), String::new()),
            [leading @ .., name] => (
                leading
                    .iter()
                    .map(|part| part.go_to_lower())
                    .collect::<Vec<_>>()
                    .join("."),
                name.go_to_lower(),
            ),
        }
    }
}

/// The sequences of a context with no infoschema: none, so every name is
/// Go's `infoschema.ErrTableNotExists` (`ClassSchema.NewStd(ErrNoSuchTable)`).
#[derive(Debug)]
pub struct NoSequences;

impl SequenceSchema for NoSequences {
    fn current_db(&self) -> &str {
        ""
    }

    fn get_sequence_by_name(
        &self,
        schema: &str,
        sequence: &str,
    ) -> Result<&dyn SequenceTable, tidb_terror::TerrorError> {
        Err(tidb_dbterror::catalog_support::std_err(
            tidb_dbterror::catalog_support::Class::Schema,
            tidb_errno::errcode::ErrNoSuchTable,
        )
        .generate_with_stack_by_args(&[schema.into(), sequence.into()]))
    }
}
