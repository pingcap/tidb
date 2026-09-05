# `pkg/ddl` ALTER COLUMN index-length parity receipt

Status: bounded Rust-only alignment follow-up. This closes the affected-index
running-byte-sum check in Go's `TestAlterColumn`; it does not claim that the
whole `pkg/ddl` package is complete in this single follow-up. The package-wide
part receipts (`b102`, `b105`–`b118`, and `b109`) remain the inventory ledger
for the other DDL source windows.

## Package inventory and authority

The Go authority is `origin/master` at `f2c346fe4f3`. The complete recursive
`pkg/ddl` inventory contains 293 tracked artifacts: 262 Go files, 30 build or
ownership inputs, and the package `doc.go`; the Go sources total 133,120 lines.
The inventory includes production, unit/benchmark/integration tests, nested
`bdr/`, `copr/`, `ingest/`, `mview/`, `owner/`, `reorg/`, `schematracker/`,
`schemaver/`, and `session/` packages, their BUILD/Bazel inputs, generated or
platform-tagged sources, and fixture/testdata paths. The package doc was read
first: DDL maintains at most two schema versions while online DDL advances the
global schema version. Existing part receipts retain the per-file/function
inventory for the package's 16×60+42 test windows.

The source contract for this fix was reread in:

- `pkg/ddl/doc.go` (online-DDL invariant);
- `pkg/ddl/modify_column.go`, especially `GetModifiableColumnJob`,
  `checkColumnWithIndexConstraint`, `checkIndexInModifiableColumns`, and the
  call to `checkIndexPrefixLength`;
- `pkg/ddl/index.go`, `buildIndexColumns`, `checkIndexPrefixLength`, and the
  running `sumLength`/`ErrTooLongKey` rule;
- `pkg/ddl/db_integration_test.go`'s `TestAlterColumn` cases for widening an
  indexed column;
- the Rust carriers in `tidb-executor::ddl::alter_table`,
  `tidb-executor::ddl::index_prefix`, `tidb-executor::kv_table::KvIndex`, and
  `tidb-session::tests_alter_column`.

## Go behavior restored

Go checks every affected index twice when preparing `MODIFY COLUMN`: each key
part is revalidated against the replacement type, then the complete index is
checked with a running byte sum. A composite index can therefore fail even
when every individual part is legal. The Rust owner already performed the
per-part check but omitted that second aggregate check, so it accepted:

```text
CREATE TABLE wide (..., INDEX ab (a,b), INDEX cd (c,d)) CHARSET=ascii;
ALTER TABLE wide MODIFY COLUMN a VARCHAR(3000); -- Go 1071, sum 3100
ALTER TABLE wide MODIFY COLUMN c BIGINT;         -- Go 1071, sum 3079
```

`set_table_options_action` now rebuilds the affected index's `(FieldType,
prefix length)` stream, substitutes the proposed field type at the changed
offset, and calls the existing `check_index_key_length` helper in strict mode.
Indexes that do not contain the changed column are left untouched. The error
maps through the existing Go-shaped `DriverError::TooLongKey` conversion.

## Regression and validation

The new source-shaped regression is
`tidb-session::tests_alter_column::modify_column_rechecks_the_full_affected_index_key_length`.
It failed before the production change because the first MODIFY succeeded;
after the change it passes with exact 1071 payloads (`3100` and `3079` bytes).
The existing measured-divergence pin was updated to record the now-closed
behavior rather than preserve the obsolete acceptance.

Ready validation for this Rust-only batch:

```text
OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... \
cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-session --lib tests_alter_column -- --test-threads=1
# 13 passed; 0 failed

cargo +nightly-2026-08-22 check --offline --locked \
  --manifest-path rust/Cargo.toml -p tidb-executor -p tidb-session --all-targets
# passed (existing warnings only)

cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml \
  -p tidb-executor -p tidb-session -- --check
git diff --check
PATH=... GOPATH=... TMPDIR=/tmp/tidb-codex make lint
# passed
```

No Go, generated, platform, Bazel, or module file changed, so
`make bazel_prepare` is not required. Broader DDL online-job, PD/GC, and
cross-session integration gaps remain documented by the existing part
receipts; this follow-up only closes the deterministic index-length contract.

## Risks and boundaries

- Correctness: only indexes containing the modified column are rechecked, in
  Go declaration order, with the running sum that reports the first crossing.
- Compatibility: the check is strict, matching Go's ALTER worker path; no
  warning/truncation behavior is introduced for MODIFY.
- Performance: one metadata-only pass over related indexes; no row scan or
  index rebuild is added to the Rust catalog mutation.
