# `pkg/util/dbterror` standard-error catalogue precedence receipt

Status: bounded Rust parity fix implemented in the isolated worktree and
validated against the fetched Go master. Go source and Bazel files were not
changed.

Comparison source: Go `origin/master` at
`fc7788ff517c3407dc7e000be989ab23e6648211`.

Final batch commit: this receipt is included in the pushed batch; verify the
exact hash with the final `hparser-integration` remote check.

## Inventory completed before editing

The complete owners were enumerated before editing, including every
production file, nested package, test/support harness, BUILD metadata, and
generated/platform artifact:

| Tree | Files | Source lines |
| --- | ---: | ---: |
| `pkg/util/dbterror` (including `exeerrors` and `plannererrors`) | 10 | 1,118 Go lines |
| `rust/crates/tidb-error` | 42 | 34,598 Rust lines |

The Go behavior-bearing source is `pkg/util/dbterror/terror.go`: `ErrClass.NewStd`
looks up the message in `errno.MySQLErrName`, whose entries are the TiDB
catalogue. The Rust owner is `rust/crates/tidb-error/src/terror.rs`; its
`registered_std` constructor feeds every standard prototype in
`plannererrors.rs`, `server_errors.rs`, and dependent crates.

## Go behavior restored

Go's `NewStd` must use the `pkg/errno`/TiDB message when a code exists in both
catalogues. Rust previously queried the parser/MySQL catalogue first, so
overlapping codes silently returned different wording and even different
placeholder types. `registered_std` now checks `tidb::message_by_code` first
and falls back to `mysql::message_by_code` only for codes absent from the TiDB
catalogue. The change preserves the existing dual-catalogue coverage while
restoring Go's precedence in the ordinary prototype path.

## Focused regression

`terror::registered_std_tests::prefers_tidb_errno_messages_for_overlapping_codes`
asserts the exact TiDB catalogue strings for three known overlaps:

```text
3143  Invalid JSON path expression. The error is around character position %d.
1243  Unknown prepared statement handler (%.*s) given to %s
1820  You must reset your password using ALTER USER statement before executing this statement
```

Each case differed from the parser/MySQL catalogue before this change.

## Ready validation

Commands run from `rust/`:

```text
cargo test --offline --locked -p tidb-error --lib \
  terror::registered_std_tests::prefers_tidb_errno_messages_for_overlapping_codes \
  -- --exact --nocapture
cargo test --offline --locked -p tidb-error --all-targets -- --test-threads=1
cargo check --offline --locked -p tidb-error --all-targets
cargo fmt --all -- --check
git diff --check
cargo clippy --offline --locked -p tidb-error --all-targets -- -D warnings
```

Results before the final rebase:

- Focused regression: PASS (1/1).
- Serialized owner profile: PASS (8 unit tests and 31 generated/source
  integration tests).
- Owner compilation: PASS.
- Formatting, whitespace, and strict clippy: PASS for `tidb-error`.

## Risks and remaining boundaries

- This changes the message selected for all overlapping standard prototypes;
  that is intentional Go behavior and is pinned by the focused cases.
- Raise sites that explicitly call `registered_standard` with a hand-selected
  message remain unchanged; those are separate source decisions.
- The complete `pkg/util/dbterror` inventory is larger than this one lookup
  boundary, so this receipt is not a package-complete execution claim.
