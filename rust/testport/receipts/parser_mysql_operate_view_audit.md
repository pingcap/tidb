# `pkg/parser/mysql` — Go-master `OPERATE VIEW` parity boundary receipt

Status: implemented as a coordinated Rust batch across the parser, lexer,
privilege registry, bootstrap, executor account bridge, and metadata owners.
The Go package inventory is complete, and the executable `OPERATE VIEW`
privilege path now preserves one bit, spelling, scope mask, persisted column,
and `GRANT`/`REVOKE` parser form. This is not a claim that the broader Go
session package or materialized-view maintenance scheduler has been
transcreated; those remain explicit integration boundaries below.

Comparison source: Go `origin/master` at
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01), with the privilege
change from `8cde78af3c` (`session, parser, privilege, executor: add OPERATE
VIEW privilege`) and the materialized-view metadata change from
`d6afc7d991`.

## Complete Go inventory

The package has exactly 15 tracked artifacts and 4,847 Go lines at the
Go-master comparison commit. There is no
`doc.go`, fixture/testdata tree, generated Go source, platform variant, or
benchmark outside the listed files. The adjacent Go `pkg/meta/metadef` owner
was also inventoried in full for the reserved materialized-view IDs and SQL:
seven artifacts (`BUILD.bazel`, `OWNERS`, `db.go`, `db_test.go`, `system.go`,
`system_tables_def.go`, and `system_test.go`) totaling 1,347 lines, with no
generated or platform variant.

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
| `privs.go` | 334 | privilege bits, names, scope lists, and catalog maps |
| `privs_test.go` | 94 | privilege map and scope consistency tests |
| `state.go` | 268 | server state and status constants |
| `type.go` | 173 | MySQL field-type and flag helpers |
| `type_test.go` | 35 | field-type flag tests |
| `util.go` | 102 | field-length and authentication helpers |

All 15 parser/mysql files were read in full, including 75 function
declarations (11 package test functions). The
Go delta is confined to `privs.go` in this package: `OperateViewPriv` is added
to string/set/user-column maps, global/database/table privilege lists, and the
bit enum. The seven metadef files, their tests, and build/ownership metadata
were likewise read before adding the five target-master table definitions and
reserved IDs.

## Rust ownership and comparison

The nearest Rust owners are:

- `tidb-lexer/src/keywords.rs` and `keyword_catalog/*`, generated from Go
  `misc.go`/`parser.y`; these now contain the generated unreserved `OPERATE`
  token and count assertions.
- `tidb-parser/src/privilege.rs`, whose hand-written privilege parser now
  restores `OPERATE VIEW` as a static two-word privilege (the AST stores
  canonical names as strings).
- `tidb-session/src/privilege/privs.rs`, whose `GlobalPriv` enum and
  `ALL_GLOBAL_PRIVS`/`ALL_DB_PRIVS`/`ALL_TABLE_PRIVS` masks now carry the
  `OperateView` variant.
- `tidb-session/src/privilege/registry_ops.rs`, `table_privilege.rs`, and
  bootstrap/user-table code, which consume those masks and column names.
- `tidb-exec/src/cluster_privilege_load.rs`, `cluster_account_write.rs`, and
  `mysql_bootstrap/rows.rs`, which read/write the persisted `mysql.user` and
  `mysql.db` columns; their source-derived table-info JSON fixture was
  regenerated so the inserted column keeps every later offset aligned.
- `tidb-metadef/src/system.rs` and `system_tables_def.rs`, which own the
  target-master materialized-view/log reserved IDs and bootstrap `CREATE
  TABLE` strings alongside the privilege-column additions.

Adding just a `GlobalPriv::OperateView` variant would not parse the new grant,
would assign no lexer token, and would leave `mysql.user`/`mysql.db` loading and
`SHOW GRANTS` behavior inconsistent. The batch therefore synchronizes the
generated/source-derived keyword catalogs, parser restoration, privilege masks
and maps, bootstrap root row, executor account load/write columns, and the
metadef SQL/ID owners. Focused parser, privilege, metadata, and source-catalog
regressions cover those seams. No Rust-only privilege behavior was removed;
the existing native registry remains the ordinary execution path.

## Validation

Profile: Ready for this code batch. The package-complete claim is limited to
the implemented privilege/metadata seams; session versioned upgrade code and
materialized-view execution remain unverified integration boundaries.

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./mysql -count=1` from `pkg/parser` — passed.
- `OPENSSL_DIR=... DYLD_LIBRARY_PATH=... cargo +nightly-2026-08-22 test --offline --locked -p tidb-parser --lib operate_view_privilege_restores_as_a_static_privilege -- --test-threads=1` — passed after the fix; the same test failed before the parser branch was restored (`expected a privilege`).
- `OPENSSL_DIR=... DYLD_LIBRARY_PATH=... cargo +nightly-2026-08-22 test --offline --locked -p tidb-mysql --test parser_mysql_package_source -- --test-threads=1` — 17 passed, including bit 33, map, set, and scope assertions.
- `OPENSSL_DIR=... DYLD_LIBRARY_PATH=... cargo +nightly-2026-08-22 test --offline --locked -p tidb-lexer --test all -- --test-threads=1` — 5 passed, including the target 685-keyword catalog length.
- `OPENSSL_DIR=... DYLD_LIBRARY_PATH=... cargo +nightly-2026-08-22 test --offline --locked -p tidb-metadef --test metadef_contract -- --test-threads=1` — 3 passed, including the five materialized-view table shapes and baseline contract.
- `OPENSSL_DIR=... DYLD_LIBRARY_PATH=... cargo +nightly-2026-08-22 test --offline --locked -p tidb-exec --test all operate_view_table_grant_round_trips_through_the_table_priv_set -- --test-threads=1` — passed; this exercises the persisted SET writer and loader together.
- `OPENSSL_DIR=... DYLD_LIBRARY_PATH=... cargo +nightly-2026-08-22 test --offline --locked -p tidb-exec --test all the_seeded_root_account_loads_back_as_an_unlocked_superuser -- --test-threads=1` — passed with the added bootstrap `OPERATE VIEW` privilege.
- `OPENSSL_DIR=... DYLD_LIBRARY_PATH=... cargo +nightly-2026-08-22 test --offline --locked -p tidb-exec --test all mysql_bootstrap_tableinfo_source -- --test-threads=1` — 6 passed after regenerating the source-derived table-info fixture.
- `OPENSSL_DIR=... DYLD_LIBRARY_PATH=... cargo +nightly-2026-08-22 test --offline --locked -p tidb-session --lib privilege -- --test-threads=1` — 48 passed, 3 ignored; the pre-existing `tests_grants::static_grants::infoschema_privileges_tables_are_header_only` assertion still fails because the current result exposes `Column#N` names instead of expected headers. This is unrelated and remains unverified.
- `rustup run nightly-2026-08-22 rustfmt --edition 2021 --check` on all touched Rust sources — passed.
- `make lint` — Ready gate; passed after the final receipt/plan edits.
- `git diff --check` — passed.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risks and unverified surfaces

- Correctness risk is concentrated in bit compatibility, scope masks, and
  bootstrap column ordering: a missing column shifts every following `mysql.user`
  value.
- Compatibility risk spans generated parser/lexer inputs, `GRANT`/`REVOKE`,
  `SHOW GRANTS`, information-schema visibility, and materialized-view checks.
- The inserted `mysql.user`/`mysql.db` column shifts later TableInfo offsets;
  the regenerated fixture and bootstrap source tests cover the shape, but a
  live mixed-version cluster upgrade was not run.
- Performance impact is limited to one additional privilege bit/column in
  existing linear maps and row projections.
- Rust does not yet implement Go's versioned `version285` schema upgrade or
  materialized-view refresh/log purge scheduling and execution; those are
  intentionally not fabricated in this batch.
- The scoped Rust session gate retains the unrelated infoschema header failure
  described above.
