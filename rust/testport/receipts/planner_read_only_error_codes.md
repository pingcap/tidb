# `pkg/planner` — read-only refusal error-code parity receipt

Comparison source: Go `origin/master` ordinary planner/error surfaces. This
Rust-only batch closes the typed error contract at the bounded read-only
planner and its real-TiKV server adapter; no Go source was edited.

## Rust owner inventory

The bounded owner files were read before editing:

| Artifact | Role |
| --- | --- |
| `rust/crates/tidb-planner/src/read_only_scan/errors.rs` | complete refusal vocabulary and prepared-plan/bind errors |
| `rust/crates/tidb-planner/src/read_only_scan.rs` | parse, table, and column refusal producers |
| `rust/crates/tidb-planner/src/read_only_scan/prepared.rs` | prepared catalog/marker refusal producers |
| `rust/crates/tidb-planner/src/configured_catalog.rs` | configured table lookup variants |
| `rust/crates/tidb-planner/src/signed_bigint_ranger.rs` | invalid-comparison error input |
| `rust/crates/tidb-planner/src/index_task.rs` | planner-rejection error input |
| `rust/crates/tidb-planner/tests/read_only_scan_source.rs` | source-derived planner tests and new wire-identity regression |
| `rust/crates/tidb-exec/src/real_tikv_read.rs` | `RealTiKvReadError::Plan` execution boundary |
| `rust/crates/tidb-server/src/real_tikv_node/mod.rs` | single-table text, prepared, and direct read-error flattening seams |
| `rust/crates/tidb-server/src/real_tikv_multi_node.rs` | two-table prepared-plan, bind, and direct read execution seams |

Generated/platform fixtures and build variants for this bounded error surface
were absent; the existing aggregate Rust test harness remains unchanged.

## Go contract and Rust fix

Go's ordinary missing-table planner path uses `ErrNoSuchTable` (1146/42S02),
unknown columns use `ErrBadField` (1054/42S22), parse failures use `ErrParse`
(1064/42000), and unsupported bounded read shapes use
`ErrNotSupportedYet` (1235/42000). Internal planner invariants retain the
generic 1105/HY000 contract. Prepared catalog ambiguity uses
the explicit Rust configured-catalog fallback 1105/HY000 (the bounded
prepared planner has no ordinary Go catalog consumer yet), and an invalid
prepared parameter count uses TiDB's `ErrWrongParamCount` (8112/HY000).

`ReadOnlyScanError`, `PreparedPlanError`, and `PreparedBindError` now expose
Go-compatible `(errno, SQLSTATE)` pairs. The single- and multi-table server
adapters convert these pairs to `SqlQueryError` at text lowering, prepared
planning/binding, and `RealTiKvReadError::Plan` seams while retaining the
loaded-table refusal override. Existing non-planner transport/storage errors
remain generic 1105.

## Regression and validation

`read_only_scan_source::read_only_errors_keep_their_go_wire_identity` covers
all ten `ReadOnlyScanError` shapes plus each prepared-plan and prepared-bind
classification. `real_tikv_node::tests::read_only_refusals_reach_the_server_with_go_wire_codes`
covers the server adapter for unsupported, unknown-column, and direct-plan
errors. The pre-fix server regression was run with the flattening seams
temporarily routed through `SqlQueryError::unknown`; it failed on the expected
1235/1054/1146 assertions. Restoring the typed seam made both focused tests
pass. This checkout lacks a system OpenSSL development environment, so those
Rust tests used a temporary vendored test-only workaround; that dependency
change was reverted and is not part of this receipt's source diff.

Ready validation:

```text
LC_ALL=C LANG=C cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-planner --test all \
  read_only_errors_keep_their_go_wire_identity
LC_ALL=C LANG=C cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-server --lib \
  read_only_refusals_reach_the_server_with_go_wire_codes
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
git diff --check
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
TMPDIR=/tmp/tidb-codex make lint
```

Both focused regressions, Rust formatting, diff hygiene, and the repository
`make lint` Ready gate pass.

## Boundary and risk

This change carries error identity only; it does not widen the bounded read
planner or change storage execution. The custom Rust refusal text remains the
message body, so clients receive the corrected code/SQLSTATE without a new
message formatter. Loaded-table refusal diagnostics intentionally remain
generic 1105 because that path reports a cluster admission reason rather than
the planner's ordinary missing-table error.
