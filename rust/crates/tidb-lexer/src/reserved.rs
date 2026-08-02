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
// See the License for the specific language governing permissions and
// limitations under the License.

//! Real MySQL/TiDB distinguishes RESERVED keywords (never usable as a bare,
//! unquoted identifier) from the much larger set of NON-RESERVED keywords
//! (usable as a table/column/alias name whenever the grammar isn't
//! otherwise ambiguous there — e.g. `SELECT uuid FROM t` is valid, `uuid`
//! being a non-reserved keyword). `RESERVED_KEYWORDS` is this crate's own
//! copy of that reserved set, sorted for binary search.
//!
//! Derived (not hand-transcribed) from `pkg/parser/reserved_words.go`'s
//! `IsReserved` — that function switches on internal Go token-constant
//! names, not keyword strings, so each was cross-referenced against
//! `pkg/parser/misc.go`'s `tokenMap` (string -> token constant) to recover
//! the keyword spelling; confirmed to be a subset of this crate's own
//! `keywords::GENERAL_KEYWORDS`.
//!
//! `tokenMap` is many-to-one: four token constants each have TWO string
//! spellings mapping onto them, so a naive "invert the map" derivation keeps
//! only one spelling per constant and silently drops the other. All four
//! pairs need BOTH spellings listed here, independently of which one the
//! derivation script happened to keep:
//!
//! - `distinct` <- `"DISTINCT"`, `"DISTINCTROW"`
//! - `database` <- `"DATABASE"`, `"SCHEMA"`
//! - `databases` <- `"DATABASES"`, `"SCHEMAS"`
//! - `decimalType` <- `"DECIMAL"`, `"DEC"`
//!
//! Each pair's reserved status is confirmed independently by `pkg/parser/
//! keywords.go`'s own catalog entries (`{"DISTINCT", true, "reserved"}`,
//! `{"DISTINCTROW", true, "reserved"}`, `{"DATABASE", true, "reserved"}`,
//! `{"DATABASES", true, "reserved"}`, `{"DECIMAL", true, "reserved"}`;
//! `SCHEMA`/`SCHEMAS`/`DEC` aren't in that catalog at all — MySQL's
//! `information_schema.KEYWORDS` doesn't list them as separate words either
//! — but `tokenMap` still routes them onto the reserved `database`/
//! `databases`/`decimalType` constants, so they belong here too).
//!
//! This list is hand-maintained (there is no codegen step deriving it from
//! Go), so nothing mechanically prevents a future shared-token-constant pair
//! from dropping a spelling the same way. `tests::reserved_keyword_pairs_are_
//! complete` below pins all four known pairs directly, so at least these
//! collisions can't silently regress; a genuinely new collision would still
//! need a human to notice it against `pkg/parser/misc.go`'s `tokenMap`.

/// RESERVED_KEYWORDS: 236 keywords, sorted for binary search. Derived from
/// `pkg/parser/reserved_words.go` — see this module's own doc.
pub static RESERVED_KEYWORDS: &[&str] = &[
    "ADD",
    "ALL",
    "ALTER",
    "ANALYZE",
    "AND",
    "ARRAY",
    "AS",
    "ASC",
    "BETWEEN",
    "BIGINT",
    "BINARY",
    "BLOB",
    "BOTH",
    "BY",
    "CALL",
    "CASCADE",
    "CASE",
    "CHANGE",
    "CHAR",
    "CHARACTER",
    "CHECK",
    "COLLATE",
    "COLUMN",
    "CONSTRAINT",
    "CONTINUE",
    "CONVERT",
    "CREATE",
    "CROSS",
    "CUME_DIST",
    "CURRENT_DATE",
    "CURRENT_ROLE",
    "CURRENT_TIME",
    "CURRENT_TIMESTAMP",
    "CURRENT_USER",
    "CURSOR",
    "DATABASE",
    "DATABASES",
    "DAY_HOUR",
    "DAY_MICROSECOND",
    "DAY_MINUTE",
    "DAY_SECOND",
    "DEC",
    "DECIMAL",
    "DEFAULT",
    "DELAYED",
    "DELETE",
    "DENSE_RANK",
    "DESC",
    "DESCRIBE",
    "DISTINCT",
    "DISTINCTROW",
    "DIV",
    "DOUBLE",
    "DROP",
    "DUAL",
    "ELSE",
    "ELSEIF",
    "ENCLOSED",
    "ESCAPED",
    "EXCEPT",
    "EXISTS",
    "EXIT",
    "EXPLAIN",
    "FALSE",
    "FETCH",
    "FIRST_VALUE",
    "FLOAT",
    "FLOAT4",
    "FLOAT8",
    "FOR",
    "FORCE",
    "FOREIGN",
    "FROM",
    "FULLTEXT",
    "GENERATED",
    "GRANT",
    "GROUP",
    "GROUPS",
    "HAVING",
    "HIGH_PRIORITY",
    "HOUR_MICROSECOND",
    "HOUR_MINUTE",
    "HOUR_SECOND",
    "IF",
    "IGNORE",
    "ILIKE",
    "IN",
    "INDEX",
    "INFILE",
    "INNER",
    "INOUT",
    "INSERT",
    "INT",
    "INT1",
    "INT2",
    "INT3",
    "INT4",
    "INT8",
    "INTEGER",
    "INTERSECT",
    "INTERVAL",
    "INTO",
    "IS",
    "ITERATE",
    "JOIN",
    "KEY",
    "KEYS",
    "KILL",
    "LAG",
    "LAST_VALUE",
    "LATERAL",
    "LEAD",
    "LEADING",
    "LEAVE",
    "LEFT",
    "LIKE",
    "LIMIT",
    "LINEAR",
    "LINES",
    "LOAD",
    "LOCALTIME",
    "LOCALTIMESTAMP",
    "LOCK",
    "LONG",
    "LONGBLOB",
    "LONGTEXT",
    "LOW_PRIORITY",
    "MATCH",
    "MAXVALUE",
    "MEDIUMBLOB",
    "MEDIUMINT",
    "MEDIUMTEXT",
    "MIDDLEINT",
    "MINUTE_MICROSECOND",
    "MINUTE_SECOND",
    "MOD",
    "NATURAL",
    "NOT",
    "NO_WRITE_TO_BINLOG",
    "NTH_VALUE",
    "NTILE",
    "NULL",
    "NUMERIC",
    "OF",
    "ON",
    "OPTIMIZE",
    "OPTION",
    "OPTIONALLY",
    "OR",
    "ORDER",
    "OUT",
    "OUTER",
    "OUTFILE",
    "OVER",
    "PARTITION",
    "PERCENT_RANK",
    "PRECISION",
    "PRIMARY",
    "PROCEDURE",
    "RANGE",
    "RANK",
    "READ",
    "REAL",
    "RECURSIVE",
    "REFERENCES",
    "REGEXP",
    "RELEASE",
    "RENAME",
    "REPEAT",
    "REPLACE",
    "REQUIRE",
    "RESTRICT",
    "REVOKE",
    "RIGHT",
    "RLIKE",
    "ROW",
    "ROWS",
    "ROW_NUMBER",
    "SCHEMA",
    "SCHEMAS",
    "SECOND_MICROSECOND",
    "SELECT",
    "SET",
    "SHOW",
    "SMALLINT",
    "SPATIAL",
    "SQL",
    "SQLEXCEPTION",
    "SQLSTATE",
    "SQLWARNING",
    "SQL_BIG_RESULT",
    "SQL_CALC_FOUND_ROWS",
    "SQL_SMALL_RESULT",
    "SSL",
    "STARTING",
    "STORED",
    "STRAIGHT_JOIN",
    "TABLE",
    "TABLESAMPLE",
    "TERMINATED",
    "THEN",
    "TIDB_CURRENT_TSO",
    "TINYBLOB",
    "TINYINT",
    "TINYTEXT",
    "TO",
    "TRAILING",
    "TRIGGER",
    "TRUE",
    "UNION",
    "UNIQUE",
    "UNLOCK",
    "UNSIGNED",
    "UNTIL",
    "UPDATE",
    "USAGE",
    "USE",
    "USING",
    "UTC_DATE",
    "UTC_TIME",
    "UTC_TIMESTAMP",
    "VALUES",
    "VARBINARY",
    "VARCHAR",
    "VARCHARACTER",
    "VARYING",
    "VIRTUAL",
    "WHEN",
    "WHERE",
    "WHILE",
    "WINDOW",
    "WITH",
    "WRITE",
    "XOR",
    "YEAR_MONTH",
    "ZEROFILL",
];

/// Reports whether `word` (matched case-insensitively) is a real
/// MySQL/TiDB RESERVED keyword — see this module's own doc.
pub fn is_reserved(word: &str) -> bool {
    let upper = word.to_ascii_uppercase();
    RESERVED_KEYWORDS.binary_search(&upper.as_str()).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pins the four `pkg/parser/misc.go` `tokenMap` collisions this
    /// module's own doc names: each pair shares one Go token constant, so
    /// `IsReserved` (and therefore this list) must include BOTH spellings,
    /// not just whichever one a naive map-inversion happens to keep. This
    /// is the exact regression that shipped without `DATABASE`/
    /// `DATABASES`/`DISTINCT` — see `rust/docs/parser-lexer-divergence.md`
    /// finding #5 (`DEC`/`DECIMAL` is a fourth pair the same finding
    /// missed, caught by re-running its own machine diff after this fix).
    #[test]
    fn reserved_keyword_pairs_are_complete() {
        for (a, b) in [
            ("DISTINCT", "DISTINCTROW"),
            ("DATABASE", "SCHEMA"),
            ("DATABASES", "SCHEMAS"),
            ("DECIMAL", "DEC"),
        ] {
            assert!(
                is_reserved(a),
                "{a} shares a token with {b}, both must be reserved"
            );
            assert!(
                is_reserved(b),
                "{b} shares a token with {a}, both must be reserved"
            );
        }
    }
}
