# `pkg/kv` write-conflict retry-marker parity receipt

Status: bounded Rust parity fix implemented in the isolated worktree. Go
`origin/master` is the source oracle; a freshly built Go server is not
available on this host, so the expected wire text is taken from the Go
catalogue and raise sites.

Comparison source: Go `origin/master` at
`6331b8787b4203a91aafe49ee1dc801ee497bf98`.

Final batch commit: this receipt is included in the pushed batch; verify the
exact hash with the final `hparser-integration` remote check.

## Inventory completed before editing

The complete owners were enumerated before editing, including production
files, unit/benchmark/fuzz tests, fixtures, generated/platform data, build
metadata, and other package artifacts:

| Tree | Files | Source lines |
| --- | ---: | ---: |
| `pkg/kv` | 30 | 5,319 Go lines |
| `rust/crates/tidb-executor` | 290 | 195,724 Rust lines |

The behavior-bearing Go artifact is `pkg/kv/error.go`: its
`TxnRetryableMark` constant is the compatibility token, and both
`ErrWriteConflict` (9007) and `ErrWriteConflictInTiDB` (8005) append it to the
catalogue message. Rust's live owner is
`rust/crates/tidb-executor/src/driver/errors/mod.rs`, where
`DriverError::to_mysql_error` renders `TxnErrorKind::WriteConflict`.

## Go behavior restored

Rust now defines one `TXN_RETRYABLE_MARK` constant with Go's exact
`[try again later]` bytes and appends it to the generic 9007 write-conflict
message. This restores the client-visible retry marker and keeps the marker in
the ordinary driver-to-MySQL error path. The focused regression asserts code,
SQLSTATE, and the complete message.

The fix is intentionally bounded: `TxnErrorKind` currently has no structured
conflict fields, and the separate Rust 8005 undetermined-commit pipeline still
needs an independent Go capture/design pass. No claim of complete write-conflict
diagnostic parity is made here.

## Focused regression

`driver::errors::source_tests::write_conflict_preserves_go_retryable_marker`
constructs `DriverError::Txn(TxnErrorKind::WriteConflict)` and pins:

```text
9007 / HY000 / Write conflict, please retry the transaction [try again later]
```

Before this change the marker was absent from the Rust message.

## Ready validation

Commands run from `rust/`:

```text
cargo test --offline --locked -p tidb-executor --lib \
  driver::errors::source_tests::write_conflict_preserves_go_retryable_marker \
  -- --exact --nocapture
cargo test --offline --locked -p tidb-executor --all-targets -- --test-threads=1
cargo check --offline --locked -p tidb-executor --all-targets
cargo fmt --all -- --check
git diff --check
cargo clippy --offline --locked -p tidb-executor --all-targets -- -D warnings
```

Results before the final rebase:

- Focused regression: PASS (1/1).
- Serialized executor owner profile: 1,030 passed, 136 failed, 0 ignored
  (1,166 total). The failures are the existing planner/remote/spill/fixture
  surfaces; none is the new error regression.
- Owner compilation: PASS; emitted warnings are pre-existing workspace/test
  warnings.
- Formatting and whitespace gates: PASS.
- Strict clippy: BLOCKED by unrelated dependency diagnostics: the existing
  `tidb-mysql/src/consts.rs:117-120` `map-or-identity` lint and generated
  `tidb-proto` `large_enum_variant`/`double_must_use` lints. No diagnostic
  points at the changed error code or its test.

## Risks and remaining boundaries

- The marker is a compatibility string; changing its spelling would break
  clients that use it to decide whether a transaction can be retried.
- The structured conflict metadata and 8005 error path remain explicit
  follow-ups in `rust/docs/error-code-parity-audit.md`.
- The complete `pkg/kv` inventory is larger than this one behavior cluster, so
  this receipt is not a package-complete parity claim.
