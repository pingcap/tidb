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

//! Gap tests for Go `pkg/executor/select_into_test.go` (items 566-574).
//! Every test writes a SELECT INTO OUTFILE file and compares the file's
//! bytes; this tier has no SELECT INTO executor
//! (`SelectIntoExec.Open/Next/dumpToOutfile`,
//! pkg/executor/select_into.go:52/:74/:124) and no outfile formatter
//! (`DumpRealOutfile`, pkg/executor/select_into.go:238).

/// Go `pkg/executor/select_into_test.go:43::TestSelectIntoFileExists`:
/// `select 1 into outfile <f>` succeeds once; the second identical statement
/// errors with an "already exists"-style message that names the file. Needs
/// the outfile sink and its exists check.
#[test]
#[ignore = "go-parity-gap: SELECT INTO OUTFILE (SelectIntoExec, pkg/executor/select_into.go:52) and its file-exists error are unported"]
fn select_into_rejects_an_existing_outfile() {}

/// Go `pkg/executor/select_into_test.go:58::TestSelectIntoOutfilePointGet`:
/// `select * from t where id = 1 into outfile` writes exactly `1\n` through
/// the point-get plan on a clustered PK table.
#[test]
#[ignore = "go-parity-gap: SELECT INTO OUTFILE file writing (pkg/executor/select_into.go:124 dumpToOutfile) is unported"]
fn select_into_outfile_writes_a_point_get_result() {}

/// Go `pkg/executor/select_into_test.go:70::TestSelectIntoOutfileTypes`:
/// outfile bytes for BIT (`_binary` literals -> `\x00\x00\n\x001\n…`), ENUM
/// (`value1\nvalue2\n`), JSON (compact one-object-per-line), tinyint
/// unsigned, and `float(16,2)` ordering (`1.00\n2.00\n3.40\n10.10\n`).
#[test]
#[ignore = "go-parity-gap: the outfile value formatter for BIT/ENUM/JSON/FLOAT types (pkg/executor/select_into.go:89 considerEncloseOpt/:95 escapeField/:124) is unported"]
fn select_into_outfile_formats_each_type_like_mysql() {}

/// Go `pkg/executor/select_into_test.go:115::TestSelectIntoOutfileFromTable`:
/// four outfile variants over a mixed-type table pin the default tab/`\N`
/// format, `FIELDS TERMINATED BY ',' ENCLOSED BY '"' ESCAPED BY '#'`,
/// `OPTIONALLY ENCLOSED BY` (strings only), and custom line terminators —
/// each with exact expected file bytes and `AffectedRows() == 4`.
#[test]
#[ignore = "go-parity-gap: outfile field/line terminator options and NULL escaping (pkg/executor/select_into.go:95 escapeField) are unported"]
fn select_into_outfile_honors_field_and_line_options() {}

/// Go `pkg/executor/select_into_test.go:161::TestSelectIntoOutfileConstant`:
/// a FROM-less constant select writes `1\t2\t3\t4\t5\t6\t7.7\t8.8\t9.9\t\N\n`
/// and the float-literal row pins Go's real-dump formatting (`1e20`,
/// `123456700`, `0.123`, full-precision decimal, `0.0123456789`).
#[test]
#[ignore = "go-parity-gap: FROM-less SELECT INTO OUTFILE (pkg/executor/select_into.go:124) is unported"]
fn select_into_outfile_writes_constants() {}

/// Go `pkg/executor/select_into_test.go:177::TestDeliminators`: `ENCLOSED
/// BY '""'` / `ESCAPED BY 'gg'` error with "Field separator argument is not
/// what is expected" and write no file; empty ESCAPED BY leaves NULL
/// unescaped; enclosing/escaping interactions pin exactly which bytes get
/// escaped (encloser, escaper, line terminator's first char, zero byte
/// always) for varbinary/char/bit/blob columns.
#[test]
#[ignore = "go-parity-gap: outfile delimiter validation and escaping rules (pkg/executor/select_into.go:95 escapeField/:217 Close) are unported"]
fn outfile_deliminators_validate_and_escape_like_mysql() {}

/// Go `pkg/executor/select_into_test.go:248::TestDumpReal`:
/// `executor.DumpRealOutfile(nil, nil, val, tp)` formats a float per the
/// field's decimal: `1.2`/dec1 -> `1.2`, `2`/dec2 -> `2.00`,
/// `2.333`/unspecified -> `2.333`, `1e14` -> `100000000000000`, `1e15` ->
/// `1e15`, `1e-15` -> `0.000000000000001`, `1e-16` -> `1e-16`
/// (pkg/executor/select_into.go:238). The function is not transcreated.
#[test]
#[ignore = "go-parity-gap: DumpRealOutfile (pkg/executor/select_into.go:238) has no Rust counterpart"]
fn dump_real_outfile_formats_per_decimal() {}

/// Go `pkg/executor/select_into_test.go:271::TestEscapeType`: with `ESCAPED
/// BY '1'`, every literal `1` in any output value (int, double, varchar,
/// blob, json, set, enum) is doubled: `1,1,11,11,{"key": 11},11,11\n`.
#[test]
#[ignore = "go-parity-gap: outfile escaped-by substitution across types (pkg/executor/select_into.go:95 escapeField) is unported"]
fn outfile_escapes_the_escape_character_everywhere() {}

/// Go `pkg/executor/select_into_test.go:292::TestYearType`: YEAR(4) values
/// dump as bare four-digit lines (`2010\n2011\n2012\n2030\n`, the default
/// '2030' filling the empty insert) with `OPTIONALLY ENCLOSED BY '"'`.
#[test]
#[ignore = "go-parity-gap: outfile formatting of YEAR values (pkg/executor/select_into.go:124) is unported"]
fn outfile_writes_year_values_unquoted() {}
