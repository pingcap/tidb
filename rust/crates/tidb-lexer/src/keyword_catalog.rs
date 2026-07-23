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

//! Complete TiDB SQL keyword catalog transcreated from `pkg/parser/keywords.go`.

/// One entry in TiDB's public SQL keyword catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Keyword {
    /// Uppercase SQL keyword spelling.
    pub word: &'static str,
    /// Whether the keyword is reserved by TiDB's grammar.
    pub reserved: bool,
    /// Source section used to preserve TiDB's grouped ordering.
    pub section: &'static str,
}

/// All TiDB SQL keywords in the same order and sections as the Go parser.
pub static KEYWORDS: &[Keyword] = &[
    Keyword {
        word: "ADD",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "ALL",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "ALTER",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "ANALYZE",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "AND",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "ARRAY",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "AS",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "ASC",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "BETWEEN",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "BIGINT",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "BINARY",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "BLOB",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "BOTH",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "BY",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "CALL",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "CASCADE",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "CASE",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "CHANGE",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "CHAR",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "CHARACTER",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "CHECK",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "COLLATE",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "COLUMN",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "CONSTRAINT",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "CONTINUE",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "CONVERT",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "CREATE",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "CROSS",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "CUME_DIST",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "CURRENT_DATE",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "CURRENT_ROLE",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "CURRENT_TIME",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "CURRENT_TIMESTAMP",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "CURRENT_USER",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "CURSOR",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "DATABASE",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "DATABASES",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "DAY_HOUR",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "DAY_MICROSECOND",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "DAY_MINUTE",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "DAY_SECOND",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "DECIMAL",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "DEFAULT",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "DELAYED",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "DELETE",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "DENSE_RANK",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "DESC",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "DESCRIBE",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "DISTINCT",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "DISTINCTROW",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "DIV",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "DOUBLE",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "DROP",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "DUAL",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "ELSE",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "ELSEIF",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "ENCLOSED",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "ESCAPED",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "EXCEPT",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "EXISTS",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "EXIT",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "EXPLAIN",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "FALSE",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "FETCH",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "FIRST_VALUE",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "FLOAT",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "FLOAT4",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "FLOAT8",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "FOR",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "FORCE",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "FOREIGN",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "FROM",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "FULLTEXT",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "GENERATED",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "GRANT",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "GROUP",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "GROUPS",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "HAVING",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "HIGH_PRIORITY",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "HOUR_MICROSECOND",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "HOUR_MINUTE",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "HOUR_SECOND",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "IF",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "IGNORE",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "ILIKE",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "IN",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "INDEX",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "INFILE",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "INNER",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "INOUT",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "INSERT",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "INT",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "INT1",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "INT2",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "INT3",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "INT4",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "INT8",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "INTEGER",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "INTERSECT",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "INTERVAL",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "INTO",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "IS",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "ITERATE",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "JOIN",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "KEY",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "KEYS",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "KILL",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "LAG",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "LAST_VALUE",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "LATERAL",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "LEAD",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "LEADING",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "LEAVE",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "LEFT",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "LIKE",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "LIMIT",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "LINEAR",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "LINES",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "LOAD",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "LOCALTIME",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "LOCALTIMESTAMP",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "LOCK",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "LONG",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "LONGBLOB",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "LONGTEXT",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "LOW_PRIORITY",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "MATCH",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "MAXVALUE",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "MEDIUMBLOB",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "MEDIUMINT",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "MEDIUMTEXT",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "MIDDLEINT",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "MINUTE_MICROSECOND",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "MINUTE_SECOND",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "MOD",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "NATURAL",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "NOT",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "NO_WRITE_TO_BINLOG",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "NTH_VALUE",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "NTILE",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "NULL",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "NUMERIC",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "OF",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "ON",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "OPTIMIZE",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "OPTION",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "OPTIONALLY",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "OR",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "ORDER",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "OUT",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "OUTER",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "OUTFILE",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "OVER",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "PARTITION",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "PERCENT_RANK",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "PRECISION",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "PRIMARY",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "PROCEDURE",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "RANGE",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "RANK",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "READ",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "REAL",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "RECURSIVE",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "REFERENCES",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "REGEXP",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "RELEASE",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "RENAME",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "REPEAT",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "REPLACE",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "REQUIRE",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "RESTRICT",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "REVOKE",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "RIGHT",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "RLIKE",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "ROW",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "ROWS",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "ROW_NUMBER",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "SECOND_MICROSECOND",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "SELECT",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "SET",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "SHOW",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "SMALLINT",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "SPATIAL",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "SQL",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "SQLEXCEPTION",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "SQLSTATE",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "SQLWARNING",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "SQL_BIG_RESULT",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "SQL_CALC_FOUND_ROWS",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "SQL_SMALL_RESULT",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "SSL",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "STARTING",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "STORED",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "STRAIGHT_JOIN",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "TABLE",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "TABLESAMPLE",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "TERMINATED",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "THEN",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "TIDB_CURRENT_TSO",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "TINYBLOB",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "TINYINT",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "TINYTEXT",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "TO",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "TRAILING",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "TRIGGER",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "TRUE",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "UNION",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "UNIQUE",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "UNLOCK",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "UNSIGNED",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "UNTIL",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "UPDATE",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "USAGE",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "USE",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "USING",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "UTC_DATE",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "UTC_TIME",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "UTC_TIMESTAMP",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "VALUES",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "VARBINARY",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "VARCHAR",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "VARCHARACTER",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "VARYING",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "VIRTUAL",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "WHEN",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "WHERE",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "WHILE",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "WINDOW",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "WITH",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "WRITE",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "XOR",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "YEAR_MONTH",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "ZEROFILL",
        reserved: true,
        section: "reserved",
    },
    Keyword {
        word: "ACCOUNT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ACTION",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ADD_COLUMNAR_REPLICA_ON_DEMAND",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ADVISE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "AFFINITY",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "AFTER",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "AGAINST",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "AGO",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ALGORITHM",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ALWAYS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ANY",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "APPLY",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ASCII",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ATTRIBUTE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ATTRIBUTES",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "AUTOEXTEND_SIZE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "AUTO_ID_CACHE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "AUTO_INCREMENT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "AUTO_RANDOM",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "AUTO_RANDOM_BASE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "AVG",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "AVG_ROW_LENGTH",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "BACKEND",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "BACKUP",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "BACKUPS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "BDR",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "BEGIN",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "BERNOULLI",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "BINDING",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "BINDINGS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "BINDING_CACHE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "BINLOG",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "BIT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "BLOCK",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "BOOL",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "BOOLEAN",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "BTREE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "BYTE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CACHE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CALIBRATE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CAPTURE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CASCADED",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CAUSAL",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CHAIN",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CHARSET",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CHECKPOINT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CHECKSUM",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CHECKSUM_CONCURRENCY",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CIPHER",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CLEANUP",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CLIENT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CLIENT_ERRORS_SUMMARY",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CLOSE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CLUSTER",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CLUSTERED",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "COALESCE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "COLLATION",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "COLUMNAR",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "COLUMNS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "COLUMN_FORMAT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "COMMENT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "COMMIT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "COMMITTED",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "COMPACT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "COMPRESSED",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "COMPRESSION",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "COMPRESSION_LEVEL",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "COMPRESSION_TYPE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CONCURRENCY",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CONFIG",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CONNECTION",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CONSISTENCY",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CONSISTENT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CONTEXT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CPU",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CSV_BACKSLASH_ESCAPE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CSV_DELIMITER",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CSV_HEADER",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CSV_NOT_NULL",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CSV_NULL",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CSV_SEPARATOR",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CSV_TRIM_LAST_SEPARATORS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CURRENT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CYCLE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "DATA",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "DATE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "DATETIME",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "DAY",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "DEALLOCATE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "DECLARE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "DEFINER",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "DELAY_KEY_WRITE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "DIGEST",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "DIRECTORY",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "DISABLE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "DISABLED",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "DISCARD",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "DISK",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "DO",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "DUPLICATE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "DYNAMIC",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ENABLE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ENABLED",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ENCRYPTION",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ENCRYPTION_KEYFILE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ENCRYPTION_METHOD",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "END",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ENFORCED",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ENGINE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ENGINES",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ENGINE_ATTRIBUTE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ENUM",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ERROR",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ERRORS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ESCAPE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "EVENT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "EVENTS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "EVOLVE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "EXCHANGE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "EXCLUSIVE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "EXECUTE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "EXPANSION",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "EXPIRE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "EXPLORE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "EXTENDED",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "FAILED_LOGIN_ATTEMPTS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "FAULTS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "FIELDS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "FILE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "FIRST",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "FIXED",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "FLUSH",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "FOLLOWING",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "FORMAT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "FOUND",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "FULL",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "FUNCTION",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "GENERAL",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "GLOBAL",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "GRANTS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "HANDLER",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "HASH",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "HELP",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "HISTOGRAM",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "HISTORY",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "HOSTS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "HOUR",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "HYPO",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "IDENTIFIED",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "IETF_QUOTES",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "IGNORE_STATS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "IMPORT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "IMPORTS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "INCREMENT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "INCREMENTAL",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "INDEXES",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "INSERT_METHOD",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "INSTANCE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "INVISIBLE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "INVOKER",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "IO",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "IPC",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ISOLATION",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ISSUER",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "JSON",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "KEY_BLOCK_SIZE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "LABELS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "LANGUAGE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "LAST",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "LASTVAL",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "LAST_BACKUP",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "LESS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "LEVEL",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "LIST",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "LOAD_STATS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "LOCAL",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "LOCATION",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "LOCKED",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "LOGS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "MASKING",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "MASTER",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "MAX_CONNECTIONS_PER_HOUR",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "MAX_IDXNUM",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "MAX_MINUTES",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "MAX_QUERIES_PER_HOUR",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "MAX_ROWS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "MAX_UPDATES_PER_HOUR",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "MAX_USER_CONNECTIONS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "MB",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "MEMBER",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "MEMORY",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "MERGE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "MICROSECOND",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "MINUTE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "MINVALUE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "MIN_ROWS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "MODE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "MODIFY",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "MONTH",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "NAMES",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "NATIONAL",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "NCHAR",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "NEVER",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "NEXT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "NEXTVAL",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "NO",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "NOCACHE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "NOCYCLE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "NODEGROUP",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "NOMAXVALUE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "NOMINVALUE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "NONCLUSTERED",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "NONE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "NOWAIT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "NULLS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "NVARCHAR",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "OFF",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "OFFSET",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "OLD",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "OLTP_READ_ONLY",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "OLTP_READ_WRITE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "OLTP_WRITE_ONLY",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ONLINE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ONLY",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ON_DUPLICATE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "OPEN",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "OPTIONAL",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "PACK_KEYS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "PAGE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "PAGE_CHECKSUM",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "PAGE_COMPRESSED",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "PAGE_COMPRESSION_LEVEL",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "PARSER",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "PARTIAL",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "PARTITIONING",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "PARTITIONS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "PASSWORD",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "PASSWORD_LOCK_TIME",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "PAUSE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "PERCENT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "PER_DB",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "PER_TABLE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "PLUGINS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "POINT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "POLICY",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "PRECEDING",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "PREPARE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "PRESERVE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "PRE_SPLIT_REGIONS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "PRIVILEGES",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "PROCESS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "PROCESSLIST",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "PROFILE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "PROFILES",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "PROXY",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "PURGE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "QUARTER",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "QUERIES",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "QUERY",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "QUICK",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "RATE_LIMIT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "REBUILD",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "RECOMMEND",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "RECOVER",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "REDUNDANT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "REFRESH",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "RELOAD",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "REMOVE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "REORGANIZE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "REPAIR",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "REPEATABLE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "REPLICA",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "REPLICAS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "REPLICATION",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "REQUIRED",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "RESOURCE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "RESPECT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "RESTART",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "RESTORE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "RESTORES",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "RESUME",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "RETAIN",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "RETURNING",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "REUSE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "REVERSE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ROLE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ROLLBACK",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ROLLUP",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ROUTINE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ROW_COUNT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ROW_FORMAT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "RTREE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "RULE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SAN",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SAVEPOINT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SECOND",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SECONDARY",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SECONDARY_ENGINE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SECONDARY_ENGINE_ATTRIBUTE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SECONDARY_LOAD",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SECONDARY_UNLOAD",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SECURITY",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SEND_CREDENTIALS_TO_TIKV",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SEPARATOR",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SEQUENCE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SERIAL",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SERIALIZABLE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SESSION",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SETVAL",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SHARD_ROW_ID_BITS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SHARE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SHARED",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SHUTDOWN",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SIGNED",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SIMPLE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SKIP",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SKIP_SCHEMA_FILES",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SLAVE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SLOW",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SNAPSHOT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SOME",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SOURCE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SQL_BUFFER_RESULT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SQL_CACHE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SQL_NO_CACHE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SQL_TSI_DAY",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SQL_TSI_HOUR",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SQL_TSI_MINUTE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SQL_TSI_MONTH",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SQL_TSI_QUARTER",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SQL_TSI_SECOND",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SQL_TSI_WEEK",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SQL_TSI_YEAR",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "START",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "STATS_AUTO_RECALC",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "STATS_COL_CHOICE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "STATS_COL_LIST",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "STATS_OPTIONS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "STATS_PERSISTENT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "STATS_SAMPLE_PAGES",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "STATS_SAMPLE_RATE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "STATUS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "STORAGE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "STORAGE_CLASS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "STRICT_FORMAT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SUBJECT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SUBPARTITION",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SUBPARTITIONS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SUPER",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SWAPS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SWITCHES",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SYSTEM",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SYSTEM_TIME",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "TABLES",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "TABLESPACE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "TABLE_CHECKSUM",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "TEMPORARY",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "TEMPTABLE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "TEXT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "THAN",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "TIKV_IMPORTER",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "TIME",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "TIMEOUT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "TIMESTAMP",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "TOKEN_ISSUER",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "TPCC",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "TPCH_10",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "TRACE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "TRADITIONAL",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "TRANSACTION",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "TRANSACTIONAL",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "TRIGGERS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "TRUNCATE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "TSO",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "TTL",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "TTL_ENABLE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "TTL_JOB_INTERVAL",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "TYPE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "UNBOUNDED",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "UNCOMMITTED",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "UNDEFINED",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "UNICODE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "UNKNOWN",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "UNSET",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "USER",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "UUID",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "VALIDATION",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "VALUE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "VARIABLES",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "VECTOR",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "VIEW",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "VISIBLE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "WAIT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "WAIT_TIFLASH_READY",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "WARNINGS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "WEEK",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "WEIGHT_STRING",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "WITHOUT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "WITH_SYS_TABLE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "WORKLOAD",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "X509",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "YEAR",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ADMIN",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "BATCH",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "BUCKETS",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "BUILTINS",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "CANCEL",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "CARDINALITY",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "CMSKETCH",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "COLUMN_STATS_USAGE",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "CORRELATION",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "DDL",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "DEPENDENCY",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "DEPTH",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "DISTRIBUTE",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "DISTRIBUTION",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "DISTRIBUTIONS",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "DRY",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "HISTOGRAMS_IN_FLIGHT",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "JOB",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "JOBS",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "LITE",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "NDVRATE",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "NODE_ID",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "NODE_STATE",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "OPTIMISTIC",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "PESSIMISTIC",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "POLICIES",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "RAW",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "REGION",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "REGIONS",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "RESET",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "RUN",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "SAMPLERATE",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "SAMPLES",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "SESSION_STATES",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "SPLIT",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "STATISTICS",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "STATS",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "STATS_BUCKETS",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "STATS_DELTA",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "STATS_EXTENDED",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "STATS_HEALTHY",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "STATS_HISTOGRAMS",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "STATS_LOCKED",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "STATS_META",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "STATS_TOPN",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "TIDB",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "TIFLASH",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "TOPN",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "WIDTH",
        reserved: false,
        section: "tidb",
    },
];
