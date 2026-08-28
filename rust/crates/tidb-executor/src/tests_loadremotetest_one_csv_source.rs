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

//! Per-test ports of the nine `mockGCSSuite` methods in Go
//! `pkg/executor/test/loadremotetest/one_csv_test.go` — the final slice
//! (items 1381–1389) of the `pkg/executor` test enumeration, whose tail is
//! the suite-method block appended after the top-level `func Test*` listing
//! (the ordering the landed sibling receipts b119/b121/b136/b139 pin).
//!
//! Every one of these Go tests stands up a fake GCS server
//! (`fakestorage.Server`, `util_test.go:37 SetupSuite`), uploads CSV/TSV
//! objects, and drives `LOAD DATA INFILE 'gs://...?endpoint=...'` through
//! the full executor pipeline: the remote reader over `pkg/objstore`, the
//! `LoadDataController` CSV row loop (`pkg/executor/load_data.go`), charset
//! decoding, generated-column substitution, and index maintenance. On this
//! tier the statement PARSES — `tidb-parser/src/load_data.rs` reproduced the
//! whole `FIELDS`/`LINES`/`IGNORE n LINES`/`CHARACTER SET`/user-var surface
//! (probe-verified this session for every SQL shape below) — but no Rust
//! crate implements `LoadDataExec`/`LoadDataController`
//! (`tests_loaddatatest_source` documents the same executor-level gap), and
//! the `tidb-executor` driver exposes no LOAD DATA entry point at all. Each
//! test below therefore records the gap with the Go behavior it will pin;
//! nothing is approximated to make it pass.

/// Go `pkg/executor/test/loadremotetest/one_csv_test.go:25::TestLoadCSV`.
/// The same quoted CSV (`i,s` header; rows `100,"test100"`, `101,"\""` —
/// an escaped quote is the whole field — `102,"😄😄😄😄😄"`,
/// `104,""` trailing empty) loads identically with and without a trailing
/// newline at EOF, yielding rows `100 test100 / 101 \" / 102 😄😄😄😄😄 /
/// 104 ` under `FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES
/// TERMINATED BY '\n' IGNORE 1 LINES`. The third arm (`one_csv_test.go:83`)
/// loads `/etc/passwd` and must fail with the 8154 message of
/// `pkg/errno/errname.go:1070`, raised at
/// `pkg/executor/importer/import.go:1475`.
#[test]
#[ignore = "go-parity-gap: LOAD DATA remote execution (fake-GCS reader + LoadDataController row loop, pkg/executor/load_data.go) is unported, including the 8154 server-disk rejection at pkg/executor/importer/import.go:1475"]
fn load_csv_loads_quoted_remote_csv_rows() {}

/// Go `one_csv_test.go:88::TestLoadCsvInTransaction`: inside
/// `begin pessimistic`, a plain insert plus the load are visible to the
/// same transaction (`1/100/101/102/104`), `rollback` discards the loaded
/// rows, and a second `begin pessimistic` + load + `commit` persists
/// `100/101/102/104` — the load participates in the transaction instead of
/// auto-committing.
#[test]
#[ignore = "go-parity-gap: no transaction surface (begin pessimistic/rollback) and no LOAD DATA executor on this tier; Go pins in-txn visibility of loaded rows"]
fn load_csv_in_transaction_rolls_back_with_the_txn() {}

/// Go `one_csv_test.go:129::TestIgnoreNLines`: `IGNORE 1 LINES` skips the
/// `"bad syntax"1` header row (`"b",2` / `"c",3` remain), `IGNORE 100
/// LINES` loads nothing, and IGNORE counts raw `\n` terminators without
/// quote-awareness — skipping the first two terminators of a file whose
/// rows `"a\n",1` embed newlines inside quotes leaves `b\n 2` and `c 3`
/// (one.go row keeps its embedded newline: rendered `b\n 2`).
#[test]
#[ignore = "go-parity-gap: LOAD DATA IGNORE n LINES counting lives in the unported LoadDataController; the remote reader seam (fake GCS) is unported too"]
fn ignore_n_lines_counts_terminators_not_quotes() {}

/// Go `one_csv_test.go:188::TestCustomizeNULL`: the `DEFINED NULL BY`
/// matrix over the four-row file `\N,"\N" / !N,"!N" / NULL,"NULL" /
/// mynull,"mynull"` — default FIELDS reads only unquoted `\N` as NULL;
/// `ESCAPED BY '\\'` keeps that; `ESCAPED BY '!'` makes `!N` strip to `N`
/// (row 1 becomes `\N \N`, quoted `!N` reads NULL); `DEFINED NULL BY
/// 'NULL'` (plain and `OPTIONALLY ENCLOSED`) and `DEFINED NULL BY 'mynull'
/// OPTIONALLY ENCLOSED` pin each null-token choice; `DEFINED NULL BY
/// x'00'` accepts the hex literal (with `ESCAPED BY ''` and with `\\`);
/// and the guard arm (`one_csv_test.go:306-309`) fails with
/// `must specify FIELDS [OPTIONALLY] ENCLOSED BY when use NULL DEFINED BY
/// OPTIONALLY ENCLOSED` (`pkg/executor/importer/import.go:719`).
#[test]
#[ignore = "go-parity-gap: DEFINED NULL BY field decoding and its import.go:719 guard live in the unported LoadDataController/importer"]
fn customize_null_defined_null_by_matrix() {}

/// Go `one_csv_test.go:312::TestGeneratedColumns` (with `set @@sql_mode =
/// ''`): loading `1\t2 / 2\t3` into `t_gen1 (a int, b generated ALWAYS AS
/// (a+1))` stores `1 2 / 2 3`; an explicit column list `(a)` behaves the
/// same; the swapped table `t_gen2 (a generated ALWAYS AS (b+1), b int)`
/// stores `3 2 / 4 3` for a full-row load, `2 1 / 3 2` for `(b)`, and two
/// all-NULL rows for `(a)` (the generated column is never assigned from
/// input and its default is NULL).
#[test]
#[ignore = "go-parity-gap: LOAD DATA generated-column substitution (load_data.go's generated column assignment) and its executor are unported"]
fn generated_columns_substitute_on_load() {}

/// Go `one_csv_test.go:356::TestMultiValueIndex`: loading JSON arrays
/// `"[1,2,3]"` / `"[2,3,4]"` into `t (i INT, j JSON, KEY idx ((cast(j as
/// signed array))))` maintains the multi-valued index and selects back as
/// `1 [1, 2, 3] / 2 [2, 3, 4]`.
#[test]
#[ignore = "go-parity-gap: JSON multi-valued index maintenance on LOAD DATA (cast(j as signed array) index) is unported end to end"]
fn multi_value_index_maintained_by_load() {}

/// Go `one_csv_test.go:384::TestGBK`: `CHARACTER SET gbk` decoding of the
/// hand-coded GBK bytes (rows `1 一丁丂七丄丅丆万丈三上下丌不与丏 / 2 丐丑丒专且丕世丗丘丙业丛东丝丞丢`)
/// into gbk and utf8mb4 tables alike, with `SET SESSION
/// character_set_database = 'gbk'` as the no-clause fallback; an utf8mb4
/// emoji file into the gbk table fails client-side with
/// `ERROR 1366 (HY000): Incorrect string value '\xF0\x9F\x98\x80' for
/// column 'j'` (`pkg/table/column.go:227`, checked via
/// `error_test.go:28 checkClientErrorMessage`); the `IGNORE` variant
/// reports `Records: 2  Deleted: 0  Skipped: 0  Warnings: 2`
/// (`one_csv_test.go:474`) and stores `3F3F3F3F` replacement bytes
/// (`D2BBB6A18140C6DF3F3F3F3F`); `CHARACTER SET unknown` is a PARSE-time
/// error containing `Unknown character set: 'unknown'`
/// (`pkg/parser/ddl_load_data_parser.go:74`). MEASURED this session: the
/// Rust parser rejects the same statement (`tidb-parser/src/load_data.rs:77`)
/// but its message is `unknown LOAD DATA character set`, which does not
/// carry Go's `[parser:1115]Unknown character set: '...'` text — a
/// message-level divergence recorded here rather than papered over.
#[test]
#[ignore = "go-parity-gap: LOAD DATA charset decoding, the 1366/warning-count arms, and Go's exact parse-error text (tidb-parser/src/load_data.rs:77 diverges) are unported"]
fn gbk_charset_conversion_on_load() {}

/// Go `one_csv_test.go:486::TestOtherCharset`: utf8 (Myanmar `ကခဂဃ /
/// ငစဆဇ`) loads into utf8 and utf8mb4 tables; latin1 bytes
/// `0x91..0x94`/`0xA1..0xA4` decode to `‘’“”`/`¡¢£¤` into latin1 and
/// utf8mb4 tables; raw control bytes `00..07` load into ascii and binary
/// tables with `HEX(j)` = `0001020304050607`.
#[test]
#[ignore = "go-parity-gap: LOAD DATA per-charset byte decoding (latin1/ascii/binary paths) is unported; no executor to drive"]
fn other_charset_conversions_on_load() {}

/// Go `one_csv_test.go:574::TestColumnsAndUserVars`: the wildcard
/// `gs://test-load/cols_and_vars-*.tsv` matches both uploaded objects (5+4
/// rows) in one statement; the source list `(@V1, @v2, @v3)` binds fields
/// to user variables and `set a=@V1, b=@V2*10, c=123` computes the target
/// columns (case-insensitive variable names), yielding sorted rows
/// `1 110 123 ... 9 990 123`.
#[test]
#[ignore = "go-parity-gap: LOAD DATA wildcard-object expansion and user-variable SET mapping (LoadDataController column assignments) are unported"]
fn columns_and_user_vars_wildcard_mapping() {}
