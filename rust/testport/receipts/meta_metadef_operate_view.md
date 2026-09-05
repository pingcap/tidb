# `pkg/meta/metadef` — `OPERATE VIEW` system-schema parity receipt

Status: bounded source-parity batch; the direct package inventory is complete
for this behavior cluster, but the package is not claimed as a complete
Go-master transcreation. Comparison source: Go `origin/master` at
`78cac443a4f46c13bfe27eb247b5c80657952547` (2026-09-02). This receipt now
covers the materialized-view system-definition follow-up restored in this
package batch.

## Complete direct inventory

All direct production, test, ownership, and build artifacts were read before
editing. The package has seven artifacts and 1,375 lines after the focused
regressions. It has no package `doc.go`, fixture/testdata directory, generated
source/input, platform-specific variant, or nested package boundary.

| Artifact | Lines | Role |
| --- | ---: | --- |
| `BUILD.bazel` | 29 | Go library/test metadata |
| `OWNERS` | 13 | ownership metadata |
| `db.go` | 68 | system-database classification helpers |
| `db_test.go` | 40 | database-classification tests |
| `system.go` | 174 | reserved system IDs and predicates |
| `system_tables_def.go` | 995 | system-table DDL constants |
| `system_test.go` | 56 | reserved-ID and DDL contract tests |

The package contains seven production functions and five test functions. No
benchmarks or generated/platform variants are present.

## Implemented behavior

- `CreateUserTable` and `CreateDBTable` now declare the Go-master
  `Operate_view_priv` column with the normal `ENUM('N','Y')` default.
- `CreateTablesPrivTable` now accepts the `Operate View` table privilege,
  allowing the privilege cache to load the same static privilege set as Go.
- Rust `tidb-metadef` already contains these three definitions and its source
  contract test; no duplicate Rust-only behavior was added.

- Go now declares the five materialized-view maintenance table IDs and their
  exact Go-master CREATE TABLE definitions (`tidb_mview_refresh_info`,
  `tidb_mlog_purge_info`, `tidb_mview_refresh_hist`,
  `tidb_mview_refresh_alert`, and `tidb_mlog_purge_hist`). The Rust
  `tidb-metadef` owner already had these definitions and contract coverage, so
  this batch restores the missing Go source without adding duplicate Rust-only
  behavior.

## Regression and validation

Profile: Ready for this bounded package behavior. The focused regression was
run before and after the production edit.

- Pre-fix `go test ./pkg/meta/metadef -run '^TestPrivilegeTableDefinitionsIncludeOperateView$' -count=1` failed because `CreateUserTable` lacked `Operate_view_priv`.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/meta/metadef -run '^TestPrivilegeTableDefinitionsIncludeOperateView$' -count=1` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/meta/metadef -count=1` — passed.
- A clean HEAD worktree with the new regression test applied before the source
  change failed to compile with undefined materialized-view IDs/DDL constants;
  the same focused test passes after the restoration.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint` — passed.
- `git diff --check` — passed after the package and receipt updates.
- `make bazel_prepare` is required by the new top-level Go test under repository policy; the local gate is recorded with the package commit and is blocked when `bazel` is unavailable.

## Rust-only diagnostic alignment (`2026-09-06`)

The complete seven-artifact inventory above was re-read before this follow-up:
`BUILD.bazel`, `OWNERS`, `db.go`, `db_test.go`, `system.go`,
`system_tables_def.go`, and `system_test.go`, with no fixtures, generated
inputs, or platform variants. The Rust `tidb-metadef` owner and its source
tests were also rechecked.

The Go-shaped `is_reserved_id` predicate carried one Rust-only
`#[must_use]` diagnostic even though Go callers may discard the boolean. The
annotation was removed and `system::tests::reserved_id_return_may_be_ignored_like_go`
now enforces the source contract under `#[deny(unused_must_use)]`.

On detached pre-fix `fd4e0f1c8bfdb9dbc59165b5793f26140874d88d`, the focused
probe failed with exactly one `unused_must_use` diagnostic:

```
CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-metadef --lib reserved_id_return_may_be_ignored_like_go -- --exact --nocapture
```

The corrected fully-qualified focused probe passed:

```
CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-metadef --lib 'system::tests::reserved_id_return_may_be_ignored_like_go' -- --exact --nocapture
```

Ready validation passed:

* `cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-metadef --lib -- --test-threads=1` — 7 passed;
* `cargo +nightly-2026-08-22 check --offline --locked --manifest-path rust/Cargo.toml -p tidb-metadef -p tidb-executor -p tidb-session --all-targets` with the bundled OpenSSL environment — passed;
* pinned Rust formatting, `git diff --check`, and `make lint` — passed.

No Go source was edited. No live TiDB integration was needed for this
constant/predicate diagnostic-only change.

## Risks and boundaries

- Existing upgraded clusters still require the owning `pkg/session` upgrade
  path to add these columns; this receipt does not claim that migration.
- The privilege cache and user-attribute consumer are separate package
  boundaries and are validated in `privilege_privileges_user_attributes.md`.
