// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

//! Batch b056 port of `pkg/parser` part-6 unit tests (Go tests sorted by file
//! path + line number, items 301–323 on `origin/master`).
//!
//! The range spans `pkg/parser/parser_test.go` (9 tests), `pkg/parser/
//! reserved_words_test.go` (1), `pkg/parser/terror/terror_test.go` (6),
//! `pkg/parser/types/etc_test.go` (1), `pkg/parser/types/field_type_test.go`
//! (5) and `pkg/parser/util/escape_test.go` (1).
//!
//! Only two surfaces are genuinely owned by this crate end-to-end:
//!
//! - `pkg/parser/util.UnescapeChar`, ported as [`crate::unescape_char`]
//!   (`escape.rs`) — fully re-derived and pinned below.
//! - The keyword classification data this crate maintains
//!   (`RESERVED_KEYWORDS` derived from `pkg/parser/reserved_words.go`) and the
//!   TiDB feature-ID allowlist (`pkg/parser/tidb` → `features.rs`), which the
//!   affinity/split-partition parser tests exercise through `T![...]` comment
//!   syntax.
//!
//! Every other Go test exercises the yacc parser, the AST, `terror`, or the
//! field-type package — none owned by tidb-lexer — so they carry explicit
//! `go-parity-gap` ignores rather than approximations.

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

/// Go: `pkg/parser/parser_test.go`, `TestSimple` — the remaining statement-
/// level bodies (prepared-statement placeholders, `--` comments vs unary
/// minus, `/*! */` versioned comments, CONVERT(expr,type), leading comments,
/// column KEY option, NVARCHAR, quoted identifiers, etc.) all require the
/// yacc parser + AST, which are not owned by this crate.
#[test]
#[ignore = "go-parity-gap: statement-level parse assertions need the yacc parser and AST, not owned by tidb-lexer"]
fn simple_statement_level_cases() {}

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

/// Go: `pkg/parser/parser_test.go`, `TestCompatMariaDB`.
#[test]
#[ignore = "go-parity-gap: MariaDB-compat table options require the yacc parser + AST restore, not owned by tidb-lexer"]
fn compat_maria_db() {}

/// Go: `pkg/parser/parser_test.go`, `TestUUIDTypeMariaDBEnabled`.
#[test]
#[ignore = "go-parity-gap: UUID-as-CHAR(36) rewrite requires the yacc parser + AST restore, not owned by tidb-lexer"]
fn uuid_type_maria_db_enabled() {}

/// Go: `pkg/parser/parser_test.go`, `TestUUIDKeywordCompatibility`.
#[test]
#[ignore = "go-parity-gap: uuid-as-identifier grammar requires the yacc parser, not owned by tidb-lexer"]
fn uuid_keyword_compatibility() {}

/// Go: `pkg/parser/parser_test.go`, `TestUUIDTypeMariaDBDisabled`.
#[test]
#[ignore = "go-parity-gap: UUID-type rejection requires the yacc parser error path, not owned by tidb-lexer"]
fn uuid_type_maria_db_disabled() {}

/// Go: `pkg/parser/parser_test.go`, `TestSecondaryEngineAttribute`.
#[test]
#[ignore = "go-parity-gap: partition-level SECONDARY_ENGINE_ATTRIBUTE grammar requires the yacc parser, not owned by tidb-lexer"]
fn secondary_engine_attribute() {}

/// Go: `pkg/parser/parser_test.go`, `TestPartialIndex`.
#[test]
#[ignore = "go-parity-gap: partial-index WHERE-clause grammar requires the yacc parser + AST restore, not owned by tidb-lexer"]
fn partial_index() {}

/// Go: `pkg/parser/parser_test.go`, `TestExplainExplore`-style full-parse
/// coverage also applies to `TestTableAffinityOption` / `TestSplitPartition`
/// statement bodies beyond the feature-ID allowlist pinned above.
#[test]
#[ignore = "go-parity-gap: AFFINITY/SPLIT statement parsing requires the yacc parser, not owned by tidb-lexer"]
fn table_affinity_option_statements() {}

/// See `table_affinity_option_statements`; same gap for SPLIT statements.
#[test]
#[ignore = "go-parity-gap: SPLIT PRIMARY KEY/INDEX statement parsing requires the yacc parser, not owned by tidb-lexer"]
fn split_partition_statements() {}

// ---------------------------------------------------------------------------
// pkg/parser/reserved_words_test.go — TestCompareReservedWordsWithMySQL
// ---------------------------------------------------------------------------

/// Go: `pkg/parser/reserved_words_test.go`, `TestCompareReservedWordsWithMySQL`.
/// Extracts keyword sections from `parser.y` and compares them against a live
/// MySQL 8 server via `do (select 1 as <kw>)`.
#[test]
#[ignore = "go-parity-gap: requires a live MySQL server connection and parser.y extraction; neither is available to tidb-lexer"]
fn compare_reserved_words_with_my_sql() {}

// ---------------------------------------------------------------------------
// pkg/parser/terror/terror_test.go
// ---------------------------------------------------------------------------

// The whole terror package (error classes, codes, SQL-error conversion,
// stack traces, JSON round-trips) is owned by tidb-error, not tidb-lexer;
// none of its surfaces exist here, so each Go test is recorded as a gap.

/// Go: `pkg/parser/terror/terror_test.go`, `TestErrCode`.
#[test]
#[ignore = "go-parity-gap: terror error-code constants live in tidb-error, not owned by tidb-lexer"]
fn err_code() {}

/// Go: `pkg/parser/terror/terror_test.go`, `TestTError`.
#[test]
#[ignore = "go-parity-gap: terror class/error construction and SQLError conversion live in tidb-error, not owned by tidb-lexer"]
fn t_error() {}

/// Go: `pkg/parser/terror/terror_test.go`, `TestJson`.
#[test]
#[ignore = "go-parity-gap: terror JSON round-trip lives in tidb-error, not owned by tidb-lexer"]
fn json_round_trip() {}

/// Go: `pkg/parser/terror/terror_test.go`, `TestErrorEqual`.
#[test]
#[ignore = "go-parity-gap: terror ErrorEqual/ErrorNotEqual live in tidb-error, not owned by tidb-lexer"]
fn error_equal() {}

/// Go: `pkg/parser/terror/terror_test.go`, `TestLog`.
#[test]
#[ignore = "go-parity-gap: terror Log lives in tidb-error, not owned by tidb-lexer"]
fn log_error() {}

/// Go: `pkg/parser/terror/terror_test.go`, `TestTraceAndLocation`.
#[test]
#[ignore = "go-parity-gap: Go runtime stack-trace formatting has no tidb-lexer counterpart"]
fn trace_and_location() {}

// ---------------------------------------------------------------------------
// pkg/parser/types/etc_test.go + types/field_type_test.go
// ---------------------------------------------------------------------------

/// Go: `pkg/parser/types/etc_test.go`, `TestStrToType`.
#[test]
#[ignore = "go-parity-gap: StrToType/type2Str live in the types/mysql packages (tidb-datatype), not owned by tidb-lexer"]
fn str_to_type() {}

/// Go: `pkg/parser/types/field_type_test.go`, `TestFieldType`.
#[test]
#[ignore = "go-parity-gap: FieldType construction/formatting lives in the types package (tidb-datatype), not owned by tidb-lexer"]
fn field_type() {}

/// Go: `pkg/parser/types/field_type_test.go`, `TestHasCharsetFromStmt`.
#[test]
#[ignore = "go-parity-gap: HasCharsetFromStmt inspects parser AST nodes, not owned by tidb-lexer"]
fn has_charset_from_stmt() {}

/// Go: `pkg/parser/types/field_type_test.go`, `TestEnumSetFlen`.
#[test]
#[ignore = "go-parity-gap: enum/set flen computation lives in the types package, not owned by tidb-lexer"]
fn enum_set_flen() {}

/// Go: `pkg/parser/types/field_type_test.go`, `TestFieldTypeEqual`.
#[test]
#[ignore = "go-parity-gap: FieldType equality lives in the types package, not owned by tidb-lexer"]
fn field_type_equal() {}

/// Go: `pkg/parser/types/field_type_test.go`, `TestCompactStr`.
#[test]
#[ignore = "go-parity-gap: CompactStr lives in the types package, not owned by tidb-lexer"]
fn compact_str() {}
