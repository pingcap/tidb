# `pkg/server` / Rust `tidb-server` unknown-command error receipt

## Bounded package inventory

The Go owner package was inventoried before the edit with `find pkg/server
-type f`: 43 production `.go` files, 41 Go test files, 25 Bazel build files,
zero fixture/data files, and `pkg/server/AGENTS.md`. The Rust owner crate was
inventoried with `find rust/crates/tidb-server -type f`: 60 production `.rs`
files, 49 source-derived/unit-test `.rs` files, its `Cargo.toml`, zero fixture
files, and no generated/platform variant outside those lists.

The behavior-owning Go artifacts read for this bounded fix were:

- `pkg/server/conn.go` — `dispatch`'s default arm formats
  `command %d not supported now` and returns `mysql.ErrUnknown` (1105).
- `pkg/server/conn_test.go` — `TestDispatch` and
  `TestDispatchClientProtocol41` assert the unknown-command error identity for
  `COM_SLEEP`; the same path applies to an arbitrary command byte.
- `pkg/server/server.go` and `pkg/server/internal/packetio.go` — the command
  response is written through the ordinary protocol-41/error framing path.

The Rust artifacts read for this bounded fix were `tidb-protocol`'s
`command.rs`, `tidb-server/src/mysql_connection.rs`,
`tidb-server/src/connection_writers.rs`, `tidb-server/src/sql_node.rs`, and
the `tidb-server` source-derived lifecycle/packet test inventory. Known but
unimplemented commands remain on the separate `ER_UNKNOWN_COM_ERROR` (1047,
`08S01`) refusal path.

## Contract and change

Before this batch, `Command::Unknown` shared the known-command refusal arm and
returned 1047/`08S01` with a Rust-specific message. The Rust command loop now
maps only `Command::Unknown { code, .. }` to 1105/`HY000`, with the exact Go
message `command {code} not supported now`. `COM_FIELD_LIST` and
`COM_RESET_CONNECTION` remain explicit known-command refusals until their
session owners are ported.

## Regression evidence

- Fail-before: temporarily restoring the previous 1047/`08S01` helper made
  `unknown_command_keeps_go_generic_error_identity` fail (`left: 1047`,
  `right: 1105`).
- Pass-after:
  `LC_ALL=C LANG=C cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-server --lib unknown_command_keeps_go_generic_error_identity`
  (`1 passed`, `408 filtered out`).

## Ready validation

- `git diff --check`
- `cargo fmt --manifest-path rust/Cargo.toml --all -- --check`
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make lint`

The temporary local `openssl` vendored feature used only to run the focused
Rust test was reverted and is not part of the change. No Go source was edited.

## Boundary

This receipt aligns the error identity for genuinely unknown command bytes.
It does not claim `COM_FIELD_LIST`, `COM_RESET_CONNECTION`, compression
advertisement, or the remaining protocol-divergence entries are implemented;
those require their own complete package-scoped inventory and regression.
