# `pkg/util/gcutil` — complete package audit

Status: complete atomic inventory and package implementation; consumer
activation remains dependency-blocked by the unimplemented `mysql.tidb`
bootstrap table and is not claimed.

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

| Artifact | Bytes | Blob |
| --- | ---: | --- |
| `pkg/util/gcutil/gcutil.go` | 3,213 | `42bd02f2d5450a525774baaa6a1e87aa600d23ea` |
| `pkg/util/gcutil/BUILD.bazel` | 528 | `74c823ffa5d4a29b90e8d223df35940490585ac8` |

There is no `doc.go`, test, support file, fixture, benchmark, generated source,
or platform variant.

## Whole-package behavior

The package has six exported functions and one private SQL constant:

- `CheckGCEnable` reads the global `tidb_gc_enable` sysvar and applies Go's
  `TiDBOptOn` interpretation;
- `DisableGC` and `EnableGC` write that global sysvar to exact `OFF`/`ON`
  values using a background context;
- `GetGCSafePoint` executes the HIGH_PRIORITY restricted SQL read of
  `mysql.tidb` under internal source type `InternalTxnGC`, requires exactly one
  row, parses `tikv_gc_safe_point` with client-go's compatible GC-time parser,
  then converts its instant with `oracle.GoTimeToTS`;
- `ValidateSnapshot` composes the safe-point read with validation;
- `ValidateSnapshotWithGCSafePoint` rejects only when `safePointTS` is
  strictly greater than `snapshotTS`, returning TiDB error 8055 with the
  safe-point timestamp rendered through `model.TSConvert2Time(...).String()`.

Pinned production consumers span DDL flashback/recover flows, `SET
tidb_snapshot`, and the TiKV HTTP handler. The package has no private or public
test artifact to reproduce.

## Rust implementation and integration decision

`tidb-gcutil` is the package owner. It implements all six Go functions, the
exact HIGH_PRIORITY parameterized SQL, `TiDBOptOn`, exact `ON`/`OFF` writes,
strict one-row check, client-go-compatible old/new GC-time parsing, physical
millisecond TSO conversion, strict `safePointTS > snapshotTS` comparison, and
the 8055 snapshot error with process-local Go-time rendering. The package has
no Go tests, so no permanent Rust-only test was added.

Go supplies a broad `sessionctx.Context`; Rust represents only the three
capabilities this package actually calls through `tidb_gcutil::Context`:
global sysvar read, global sysvar write, and parameterized restricted SQL with
the exact `gc` internal-source identity. `tidb-session::Session` implements
that boundary using the ordinary global-variable and physical-plan execution
paths. Error 8055 is integrated into the ordinary driver/wire error mapping.

The normal Rust catalog still intentionally contains an empty `mysql` schema;
`mysql.tidb` and all other bootstrap tables are absent. Consequently the real
restricted query correctly returns table-not-found today. Adding a private
safe-point field, caller-supplied safe-point string, PD read, or fabricated
single system row would bypass Go's `mysql.tidb` authority and is rejected as
a workaround. DDL flashback/recover and TiKV HTTP consumers are also absent as
complete owning behaviors, so no disconnected call was added to them. The
package implementation is complete; production consumer activation is not
claimed until the complete owning bootstrap/consumer packages exist.

## Read-only evidence

- `git ls-tree -r --long e2788410d8d696605e8cb002585877a063ccc909 pkg/util/gcutil`
- `git show e2788410d8d696605e8cb002585877a063ccc909:pkg/util/gcutil/gcutil.go`
- `git grep -n 'gcutil\.' e2788410d8d696605e8cb002585877a063ccc909 -- '*.go'`
- `rg -n 'tidb_gc_safe_point|tidb_gc_enable|SnapshotTooOld|GoTimeToTS' rust/crates`

## WIP validation

- `cargo test --manifest-path rust/Cargo.toml -p tidb-gcutil
  temporary_validation::validates_the_complete_package_surface -- --exact`
  passed against a temporary, subsequently removed context probe covering all
  six functions and the compatible suffixed GC-time form;
- `cargo check --manifest-path rust/Cargo.toml -p tidb-gcutil -p tidb-session`
  passed (pre-existing workspace warnings only);
- `cargo fmt --manifest-path rust/Cargo.toml --all -- --check` and final
  `git diff --check` are completion gates for this batch.

No Go or Bazel source changed, so `make bazel_prepare` is not required.
