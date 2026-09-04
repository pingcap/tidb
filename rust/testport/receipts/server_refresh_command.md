# `pkg/server` / Rust `tidb-server` `COM_REFRESH` receipt

## Bounded package inventory

The Go owner package was inventoried before the edit with `find pkg/server
-type f`: 43 production `.go` files, 41 Go test files, 25 Bazel/build files,
zero fixture/data files, and `pkg/server/AGENTS.md`. The Rust command owners
were inventoried with `find rust/crates/tidb-protocol -type f` and
`find rust/crates/tidb-server -type f`: `tidb-protocol` has one manifest, 15
production sources, one benchmark, and 14 source-derived test files;
`tidb-server` has one manifest, 61 production sources, 48 source-derived test
or support files, and 24 integration-test files. No platform-specific source,
hand-edited generated output, or fixture is part of this command path.

The behavior-owning Go artifacts read for this batch were:

- `pkg/server/conn.go:1552-1554` — `COM_REFRESH` dispatches the first payload
  byte to `handleRefresh`.
- `pkg/server/conn.go:2874-2883` — subcommand `0x01` runs `FLUSH PRIVILEGES`
  through `handleQuery` and then writes the command's own OK; all other
  subcommands are no-ops followed by one OK.
- `pkg/server/conn_test.go:774-787,894-907` — protocol-4.1 and legacy dispatch
  vectors assert the two-OK versus one-OK packet shapes.

The Rust artifacts read before editing were `tidb-protocol/src/command.rs`,
`tidb-protocol/src/lib.rs`, `tidb-server/src/mysql_connection.rs`,
`tidb-server/src/sql_node.rs`, `tidb-server/src/pipeline_session.rs`,
`tidb-server/src/real_tikv_multi_node.rs`, `tidb-server/src/connection_writers.rs`,
`tidb-server/tests/server_internal_packetio_source.rs`, the test aggregator,
and the session `show_admin`/flush tests. The session already executes
`FLUSH PRIVILEGES` as a Go-compatible `StmtOutput::Done`; the missing seam was
only command decoding and wire routing.

## Contract and change

`tidb-protocol` now exports `COM_REFRESH` (`0x07`), decodes it as
`Command::Refresh(Vec<u8>)`, and preserves the raw subcommand bytes. The
`tidb-server` command loop follows Go's response shape: `0x01` invokes the
session's `FLUSH PRIVILEGES` write path and emits its OK followed by the
refresh-command OK; every other supplied target emits one OK. A missing
subcommand is rejected with the existing `1105/HY000 "malform packet error"`
path instead of indexing past the payload.

## Regression evidence

- Fail-before: after adding the source-derived decoder/test but before adding
  the server arm, the focused server test failed to compile with the
  non-exhaustive `Command::Refresh(_)` match. The first test attempt also
  exposed this macOS checkout's missing system `pkg-config`/OpenSSL; a
  temporary vendored OpenSSL feature was used only for the focused run and
  reverted immediately afterward.
- Pass-after:
  `LC_ALL=C LANG=C cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-server --test all com_refresh_privileges_keeps_go_two_ok_response_shape`
  (`1 passed`, `209 filtered out`). The loopback client consumed both OK
  packets and then sent `COM_QUIT`, proving the second response did not remain
  queued for the next command.

## Ready validation

- `git diff --check`
- `cargo fmt --manifest-path rust/Cargo.toml --all -- --check`
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make lint`

No Go source was edited, and the temporary OpenSSL manifest change is not part
of the batch.

## Boundary

This receipt covers `COM_REFRESH` command decoding and Go-compatible response
framing. `COM_FIELD_LIST`, `COM_RESET_CONNECTION`, `COM_CHANGE_USER`, and
`COM_SHUTDOWN` retain their separate unsupported-command owners; privilege
reload remains the process-wide watcher/session behavior already covered by
the session tests.
