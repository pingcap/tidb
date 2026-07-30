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

//! `reserved` section of the TiDB SQL keyword catalog (see `keyword_catalog/mod.rs`).

use super::Keyword;

pub(super) static KEYWORDS_RESERVED: &[Keyword] = &[
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
];
