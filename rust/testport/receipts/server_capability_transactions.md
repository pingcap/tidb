# `pkg/server` / Rust `tidb-server` transaction capability receipt

## Bounded package inventory

The Go owner package was inventoried before editing with `find pkg/server
-type f`: 43 production `.go` files, 41 Go test files, 25 Bazel build files,
zero fixture/data files, and `pkg/server/AGENTS.md`. The Rust owner crate was
inventoried with `find rust/crates/tidb-server -type f`: 60 production `.rs`
files, 49 source-derived/unit-test `.rs` files, its `Cargo.toml`, zero fixture
files, and no generated/platform variant outside those lists.

The Go capability contract was read from `pkg/server/server.go:116-123` and
the related handshake/packet tests in `pkg/server/conn_test.go` and
`pkg/server/conn_stmt_test.go`. `defaultCapability` includes
`ClientTransactions` (1<<13), alongside the command/status capabilities that
the Rust connection loop already implements.

The Rust owners read for this bounded change were
`tidb-server/src/handshake.rs` (capability constants and negotiation),
`tidb-server/src/mysql_connection.rs` (server capability mask and handshake
state machine), `tidb-server/src/wire_status.rs` (transaction status), and the
source-derived/unit-test inventory for the crate. No Go source was edited.

## Contract and change

Rust now defines `CLIENT_TRANSACTIONS` as the MySQL bit 1<<13 and includes it
in `SERVER_CAPABILITIES`. A client can therefore negotiate the transaction
support that the existing Rust session and command paths provide.
`CLIENT_FOUND_ROWS` was a separate boundary in this batch and is now closed by
`server_client_found_rows.md`; keeping the receipts separate preserves the
fail-before/pass-after evidence for each behavior change.

## Regression evidence

- Fail-before: with the new regression in place but the capability bit removed
  from `SERVER_CAPABILITIES`,
  `server_advertises_go_transaction_capability` failed with `left: 0, right: 0`.
- Pass-after:
  `LC_ALL=C LANG=C cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-server --lib server_advertises_go_transaction_capability`
  (`1 passed`, `409 filtered out`).

## Ready validation

- `git diff --check`
- `cargo fmt --manifest-path rust/Cargo.toml --all -- --check`
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make lint`

The temporary local `openssl` vendored feature used only to run the focused
Rust test was reverted and is not part of the change.

## Boundary

This receipt covers one `pkg/server` handshake capability. The later
`server_client_found_rows.md` receipt covers the found-rows capability and its
executor/session integration. Neither receipt claims the remaining command
refusals or repository-wide Rust/Go parity.
