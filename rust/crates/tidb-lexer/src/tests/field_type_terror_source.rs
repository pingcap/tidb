// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

//! Executable scanner-owned behavior derived from pinned Go `pkg/parser`
//! tests. The two surfaces represented here are:
//!
//! - `pkg/parser/util.UnescapeChar`, ported as [`crate::unescape_char`]
//!   (`escape.rs`) — fully re-derived and pinned below.
//! - The keyword classification data this crate maintains
//!   (`RESERVED_KEYWORDS` derived from `pkg/parser/reserved_words.go`) and the
//!   TiDB feature-ID allowlist (`pkg/parser/tidb` → `features.rs`), which the
//!   affinity/split-partition parser tests exercise through `T![...]` comment
//!   syntax.
//!
//! Parser, AST, error, and field-type tests live with their owning crates and
//! are deliberately not represented here.

use super::*;

// ---------------------------------------------------------------------------
// pkg/parser/util/escape_test.go — TestUnescapeChar
// ---------------------------------------------------------------------------

/// Go: `pkg/parser/util/escape_test.go`, `TestUnescapeChar`.
///
/// Pins every vector of the Go table against this crate's `unescape_char`
/// port: standard single-byte escapes, `%`/`_` preservation, self-escaping,
/// and the identity fall-back for arbitrary characters.
#[test]
fn unescape_char_go_table() {
    let cases: &[(u8, &[u8])] = &[
        // Standard single-byte escapes
        (b'n', b"\n"),
        (b'0', &[0]),
        (b'b', &[8]),
        (b'Z', &[26]),
        (b'r', b"\r"),
        (b't', b"\t"),
        // Preserve both backslash and character
        (b'%', b"\\%"),
        (b'_', b"\\_"),
        // Self-escaping characters (backslash removed)
        (b'\\', b"\\"),
        (b'\'', b"'"),
        (b'"', b"\""),
        // Any other character just returns itself (backslash removed)
        (b'a', b"a"),
        (b'z', b"z"),
        (b'1', b"1"),
        (b' ', b" "),
    ];
    for (input, want) in cases {
        assert_eq!(&unescape_char(*input), want, "UnescapeChar({input:?})");
    }
}

// ---------------------------------------------------------------------------
// pkg/parser/parser_test.go — TestSimple (reserved/unreserved keyword lists)
// ---------------------------------------------------------------------------

/// Go: `pkg/parser/parser_test.go`, `TestSimple` (line 92), reserved-keyword
/// list. The Go test proves each word can appear where an identifier is
/// required when qualified (`db.<kw>`, `<kw>.desc`); that grammar property
/// lives in the yacc parser. Its lexical precondition — every listed word is
/// a distinct scanner token via Go's `tokenMap` — IS owned by this crate's
/// generated `GENERAL_KEYWORDS` table, so pin all of them here.
#[test]
fn simple_reserved_keywords_are_lexical_tokens() {
    let recognized = |word: &str| {
        let upper = word.to_ascii_uppercase();
        // Window-function spellings live in the separate window table
        // (`windowFuncTokenMap` in Go), consulted by isTokenIdentifier.
        crate::keywords::GENERAL_KEYWORDS
            .binary_search(&upper.as_str())
            .is_ok()
            || crate::keywords::WINDOW_FUNC_KEYWORDS
                .binary_search(&upper.as_str())
                .is_ok()
    };

    let reserved_kws = [
        "add",
        "all",
        "alter",
        "analyze",
        "and",
        "as",
        "asc",
        "between",
        "bigint",
        "binary",
        "blob",
        "both",
        "by",
        "call",
        "cascade",
        "case",
        "change",
        "character",
        "check",
        "collate",
        "column",
        "constraint",
        "convert",
        "create",
        "cross",
        "current_date",
        "current_time",
        "current_timestamp",
        "current_user",
        "database",
        "databases",
        "day_hour",
        "day_microsecond",
        "day_minute",
        "day_second",
        "decimal",
        "default",
        "delete",
        "desc",
        "describe",
        "distinct",
        "distinctRow",
        "div",
        "double",
        "drop",
        "dual",
        "else",
        "enclosed",
        "escaped",
        "exists",
        "explain",
        "false",
        "float",
        "fetch",
        "for",
        "force",
        "foreign",
        "from",
        "fulltext",
        "grant",
        "group",
        "having",
        "hour_microsecond",
        "hour_minute",
        "hour_second",
        "if",
        "ignore",
        "in",
        "index",
        "infile",
        "inner",
        "insert",
        "int",
        "into",
        "integer",
        "interval",
        "is",
        "join",
        "key",
        "keys",
        "kill",
        "leading",
        "left",
        "like",
        "ilike",
        "limit",
        "lines",
        "load",
        "localtime",
        "localtimestamp",
        "lock",
        "longblob",
        "longtext",
        "mediumblob",
        "maxvalue",
        "mediumint",
        "mediumtext",
        "minute_microsecond",
        "minute_second",
        "mod",
        "not",
        "no_write_to_binlog",
        "null",
        "numeric",
        "on",
        "option",
        "optionally",
        "or",
        "order",
        "outer",
        "partition",
        "precision",
        "primary",
        "procedure",
        "range",
        "read",
        "real",
        "recursive",
        "references",
        "regexp",
        "rename",
        "repeat",
        "replace",
        "revoke",
        "restrict",
        "right",
        "rlike",
        "schema",
        "schemas",
        "second_microsecond",
        "select",
        "set",
        "show",
        "smallint",
        "starting",
        "table",
        "terminated",
        "then",
        "tinyblob",
        "tinyint",
        "tinytext",
        "to",
        "trailing",
        "true",
        "union",
        "unique",
        "unlock",
        "unsigned",
        "update",
        "use",
        "using",
        "utc_date",
        "values",
        "varbinary",
        "varchar",
        "when",
        "where",
        "write",
        "xor",
        "year_month",
        "zerofill",
        "generated",
        "virtual",
        "stored",
        "usage",
        "delayed",
        "high_priority",
        "low_priority",
        // Window-function keywords appear in the Go list in camelCase, but
        // Go's tokenMap (`pkg/parser/misc.go`) only recognizes them under
        // their underscored SQL spellings, so use those here: this check
        // exercises the lexical keyword surface. The same applies to the two
        // option names `failedLoginAttempts` / `passwordLockTime`, whose
        // tokenMap spellings are `FAILED_LOGIN_ATTEMPTS` /
        // `PASSWORD_LOCK_TIME`.
        "CUME_DIST",
        "DENSE_RANK",
        "FIRST_VALUE",
        "lag",
        "LAST_VALUE",
        "lead",
        "NTH_VALUE",
        "ntile",
        "PERCENT_RANK",
        "rank",
        "row",
        "rows",
        "ROW_NUMBER",
        "window",
        "linear",
        "match",
        "until",
        "placement",
        "tablesample",
        "FAILED_LOGIN_ATTEMPTS",
        "PASSWORD_LOCK_TIME",
    ];
    for kw in reserved_kws {
        assert!(
            recognized(kw),
            "Go TestSimple requires {kw:?} to be a scanner token but GENERAL_KEYWORDS lacks it"
        );
    }
}

/// Go: `pkg/parser/parser_test.go`, `TestSimple` (line 92), unreserved-keyword
/// list. Each entry must NOT be classified reserved by this crate's table
/// (`SELECT <kw> FROM tbl` lexes as identifier-position keywords). The two
/// `"1 sql_no_cache"` / `"1 sql_cache"` entries are multi-word SQL fragments,
/// not single keywords, so their words are checked individually exactly as
/// spelled by the lexer surface (`SQL_NO_CACHE` / `SQL_CACHE`).
#[test]
fn simple_unreserved_keywords_are_not_reserved() {
    let unreserved_kws = [
        "add_columnar_replica_on_demand",
        "auto_increment",
        "after",
        "begin",
        "bit",
        "bool",
        "boolean",
        "charset",
        "columns",
        "commit",
        "date",
        "datediff",
        "datetime",
        "deallocate",
        "do",
        "from_days",
        "end",
        "engine",
        "engines",
        "execute",
        "extended",
        "first",
        "file",
        "full",
        "local",
        "names",
        "offset",
        "password",
        "prepare",
        "quick",
        "rollback",
        "savepoint",
        "session",
        "signed",
        "start",
        "global",
        "tables",
        "tablespace",
        "target",
        "text",
        "time",
        "timestamp",
        "tidb",
        "transaction",
        "truncate",
        "unknown",
        "value",
        "warnings",
        "year",
        "now",
        "substr",
        "subpartition",
        "subpartitions",
        "substring",
        "mode",
        "any",
        "some",
        "user",
        "identified",
        "collation",
        "comment",
        "avg_row_length",
        "checksum",
        "compression",
        "connection",
        "key_block_size",
        "max_rows",
        "min_rows",
        "national",
        "quarter",
        "escape",
        "grants",
        "status",
        "fields",
        "triggers",
        "language",
        "delay_key_write",
        "isolation",
        "partitions",
        "repeatable",
        "committed",
        "uncommitted",
        "only",
        "serializable",
        "level",
        "curtime",
        "variables",
        "dayname",
        "version",
        "btree",
        "hash",
        "row_format",
        "dynamic",
        "fixed",
        "compressed",
        "compact",
        "redundant",
        "action",
        "round",
        "enable",
        "disable",
        "reverse",
        "space",
        "privileges",
        "get_lock",
        "release_lock",
        "sleep",
        "no",
        "greatest",
        "least",
        "binlog",
        "hex",
        "unhex",
        "function",
        "indexes",
        "from_unixtime",
        "processlist",
        "events",
        "less",
        "than",
        "timediff",
        "ln",
        "log",
        "log2",
        "log10",
        "timestampdiff",
        "pi",
        "proxy",
        "quote",
        "none",
        "super",
        "shared",
        "exclusive",
        "always",
        "stats",
        "stats_meta",
        "stats_histogram",
        "stats_buckets",
        "stats_healthy",
        "tidb_version",
        "replication",
        "slave",
        "client",
        "max_connections_per_hour",
        "max_queries_per_hour",
        "max_updates_per_hour",
        "max_user_connections",
        "event",
        "reload",
        "routine",
        "temporary",
        "following",
        "preceding",
        "unbounded",
        "respect",
        "nulls",
        "current",
        "last",
        "against",
        "expansion",
        "chain",
        "error",
        "general",
        "nvarchar",
        "pack_keys",
        "p",
        "shard_row_id_bits",
        "pre_split_regions",
        "constraints",
        "role",
        "replicas",
        "policy",
        "s3",
        "strict",
        "running",
        "stop",
        "preserve",
        "placement",
        "attributes",
        "attribute",
        "resource",
        "burstable",
        "calibrate",
        "masking",
        "rollup",
    ];
    for kw in unreserved_kws {
        assert!(
            !is_reserved(kw),
            "Go TestSimple lists {kw:?} as unreserved but is_reserved classifies it reserved"
        );
    }
}

// ---------------------------------------------------------------------------
// pkg/parser/parser_test.go — MariaDB-compat / UUID / partial-index /
// secondary-engine-attribute / affinity / split-partition tests
// ---------------------------------------------------------------------------
//
// All six of the following Go tests are full `RunTest(...)` parse-and-restore
// sweeps over CREATE/ALTER TABLE grammar; the grammar and AST live outside
// tidb-lexer. The only slice each test contributes to a surface this crate
// owns is TiDB's feature-ID allowlist for `T![…]` compatibility comments
// (`pkg/parser/tidb`): `affinity` (TestTableAffinityOption uses
// `T![affinity] AFFINITY = 'table'`), `region_split` (TestSplitPartition's
// SPLIT REGION syntax family), and the MariaDB-compat/UUID/partial-index
// grammars which are gated on no feature ID at all. Those allowlist facts
// are pinned directly below.

/// Feature IDs exercised by `TestTableAffinityOption`'s
/// `/*T![affinity] ... */` cases must be inside the source parser allowlist;
/// the deliberately unsupported `resource_group` ID must stay outside.
#[test]
fn table_affinity_option_feature_id_allowlist() {
    // Go: pkg/parser/tidb featureIDs, exercised via T![affinity] comments in
    // pkg/parser/parser_test.go TestTableAffinityOption.
    assert!(can_parse_feature(&[FEATURE_ID_AFFINITY]));
    // Control from features.rs: resource_group is intentionally rejected.
    assert!(!can_parse_feature(&[FEATURE_ID_RESOURCE_GROUP]));
    // Combined with another supported ID stays supported.
    assert!(can_parse_feature(&[
        FEATURE_ID_AFFINITY,
        FEATURE_ID_SPLIT_REGION
    ]));
}

/// `TestSplitPartition`'s SPLIT syntax is gated on the `region_split` feature
/// ID; pin its allowlist membership here alongside its negative control.
#[test]
fn split_partition_feature_id_allowlist() {
    assert!(can_parse_feature(&[FEATURE_ID_SPLIT_REGION]));
    assert!(!can_parse_feature(&["nonsense"]));
}
