# `pkg/parser/mysql` — Go-master `OPERATE VIEW` parity boundary receipt

Status: audited as a complete Go package, but not claimed as a package-complete
Rust transcreation. Go master adds `OperateViewPriv` to the MySQL privilege
catalog. The change is consumed by the generated parser, lexer keyword tables,
privilege cache SQL, bootstrap `mysql.user` rows, executor display paths, and
session privilege checks. Rust currently has a hand-written parser and a
session-owned privilege registry; the generated keyword artifacts and full
bootstrap/catalog integration are not a dependency-closed leaf in this
package.

Comparison source: Go `origin/master` at
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01), with the privilege
change from `8cde78af3c` (`session, parser, privilege, executor: add OPERATE
VIEW privilege`).

## Complete Go inventory

The package has exactly 15 tracked artifacts and 9,678 Go lines. There is no
`doc.go`, fixture/testdata tree, generated Go source, platform variant, or
benchmark outside the listed files.

| Artifact | Lines | Role |
| --- | ---: | --- |
| `BUILD.bazel` | 39 | library and test target metadata |
| `charset.go` | 598 | charset, collation, and Unicode tables |
| `const.go` | 725 | protocol, SQL mode, command, and server constants |
| `const_test.go` | 129 | SQL mode/version regression tests |
| `errcode.go` | 980 | MySQL/TiDB error-code constants |
| `errname.go` | 985 | error messages and redaction metadata |
| `error.go` | 74 | typed SQL error helpers |
| `error_test.go` | 34 | SQL error tests |
| `locale_format.go` | 277 | locale-aware numeric formatting |
| `privs.go` | 326 | privilege bits, names, scope lists, and catalog maps |
| `privs_test.go` | 94 | privilege map and scope consistency tests |
| `state.go` | 268 | server state and status constants |
| `type.go` | 173 | MySQL field-type and flag helpers |
| `type_test.go` | 35 | field-type flag tests |
| `util.go` | 102 | field-length and authentication helpers |

All 15 files were read in full, including 146 top-level function/type/const/
var declarations and the 11 package test functions. The Go delta is confined
to `privs.go` in this package: `OperateViewPriv` is added to string/set/user-
column maps, global/database/table privilege lists, and the bit enum.

## Rust ownership and comparison

The nearest Rust owners are:

- `tidb-lexer/src/keywords.rs` and `keyword_catalog/*`, generated from Go
  `misc.go`/`parser.y`; neither currently contains the new `OPERATE` keyword.
- `tidb-parser/src/privilege.rs`, whose hand-written privilege parser has no
  `OPERATE VIEW` branch (the AST stores canonical names as strings).
- `tidb-session/src/privilege/privs.rs`, whose `GlobalPriv` enum and
  `ALL_GLOBAL_PRIVS`/`ALL_DB_PRIVS`/`ALL_TABLE_PRIVS` masks have no
  `OperateView` variant.
- `tidb-session/src/privilege/registry_ops.rs`, `table_privilege.rs`, and
  bootstrap/user-table code, which consume those masks and column names.

Adding just a `GlobalPriv::OperateView` variant would not parse the new grant,
would assign no lexer token, and would leave `mysql.user`/`mysql.db` loading and
`SHOW GRANTS` behavior inconsistent. Adding only a lexer or parser branch would
accept a privilege the registry cannot store or verify. The generated keyword
files also must be regenerated from the Go parser source rather than edited by
hand. For these reasons no Rust-only behavior was removed and no speculative
partial privilege pipeline was added. The correct implementation unit is the
coordinated parser/lexer/session/bootstrap/executor change, with grant/revoke,
scope, persistence, and display regressions in the same batch.

## Validation

Profile: WIP for a continuing boundary audit; no production fix was made, so a
package-complete Ready claim is intentionally not made.

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/parser/mysql -count=1` — package tests passed.
- `cargo +nightly-2026-08-22 test --offline --locked -p tidb-parser --test all parser_privilege -- --test-threads=1` — existing privilege parser tests passed.
- `cargo +nightly-2026-08-22 test --offline --locked -p tidb-session --lib privilege -- --test-threads=1` — 47 passed, 3 ignored; the pre-existing `tests_grants::static_grants::infoschema_privileges_tables_are_header_only` assertion fails because the current result exposes `Column#N` names instead of the expected headers. This is unrelated to `OPERATE VIEW` and remains unverified.
- `git diff --check` — passed.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risks and unverified surfaces

- Correctness risk is concentrated in bit compatibility, scope masks, and
  bootstrap column ordering: a missing column shifts every following `mysql.user`
  value.
- Compatibility risk spans generated parser/lexer inputs, `GRANT`/`REVOKE`,
  `SHOW GRANTS`, information-schema visibility, and materialized-view checks.
- Performance is unchanged because this audit added no production path.
- End-to-end `OPERATE VIEW` behavior remains unverified until all listed owners
  move together.
- The scoped Rust session gate retains the unrelated infoschema header failure
  described above.
