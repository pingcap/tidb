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
/// depending on the table layer.
pub trait SequenceTable: std::fmt::Debug + Send + Sync {
    /// Go `GetSequenceID`.
    fn sequence_id(&self) -> i64;

    /// Go `GetSequenceNextVal`.
    fn sequence_next_val(&self) -> Result<i64, Box<dyn std::error::Error + Send + Sync>>;

    /// Go `SetSequenceVal`: `None` when the counter was already at or past
    /// `new_val` (Go's `alreadySatisfied`).
    fn set_sequence_val(
        &self,
        new_val: i64,
    ) -> Result<Option<i64>, Box<dyn std::error::Error + Send + Sync>>;
}

/// Go `util.GetSequenceByName` over a statement's infoschema: the
/// sequences its `NEXTVAL` / `LASTVAL` / `SETVAL` resolve by name.
pub trait SequenceSchema: std::fmt::Debug + Send + Sync {
    /// The session's current database, lowercase: the schema of a bare name.
    fn current_db(&self) -> &str;

    /// The object `(database, name)` names: `Some(Some(_))` a sequence,
    /// `Some(None)` another kind of object, `None` nothing.
    fn object(&self, database: &str, name: &str) -> Option<Option<&dyn SequenceTable>>;

    /// The `(database, name)` a one- or two-part sequence path names.
    fn schema_and_name<'a>(&'a self, path: &'a [String]) -> Option<(&'a str, &'a str)> {
        match path {
            [name] => Some((self.current_db(), name.as_str())),
            [database, name] => Some((database.as_str(), name.as_str())),
            _ => None,
        }
    }

    /// The lowercase `(database, name)` an error names for `path`.
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

/// The sequences of a context with no infoschema: none.
#[derive(Debug)]
pub struct NoSequences;

impl SequenceSchema for NoSequences {
    fn current_db(&self) -> &str {
        ""
    }

    fn object(&self, _database: &str, _name: &str) -> Option<Option<&dyn SequenceTable>> {
        None
    }
}
