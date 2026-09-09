# `pkg/server` wire result-set terminal metadata parity receipt

Status: bounded Rust alignment; the complete Go package inventory was
enumerated before editing, and the deprecated-EOF result-set behavior was
traced through its Go packet owner and every Rust writer call site. This is not
a claim that the 110-file `pkg/server` package is otherwise transcreated.

Comparison source: Go `origin/master` at `f2c346fe4f3`.

## Go inventory

The package tree was enumerated with `find pkg/server -type f | sort` before
editing: 110 tracked files consisting of 43 production `.go` files, 41 Go
tests, 25 Bazel build files, and the package-local `AGENTS.md`. No fixture,
generated source, platform-specific variant, benchmark, or fuzz artifact is
present in this tree. The adjacent source packages used by the packet path
were also enumerated in full: `pkg/format/textrow` (5 files: 2 production Go,
2 tests, 1 BUILD) and `pkg/parser/mysql` (15 files: 10 production Go, 4 tests,
1 BUILD).

The behavior owners and support artifacts read for this batch are:

| Go artifact | role in the terminal packet path |
| --- | --- |
| `pkg/server/BUILD.bazel` | server package target and packet dependencies |
| `pkg/server/conn.go` | `writeEOF` → `writeOkWith`, including affected rows, last insert id, and `LastMessage` |
| `pkg/server/conn_stmt.go` | statement and cursor call sites that terminate with `writeEOF` |
| `pkg/server/driver_tidb.go` | TiDB driver/session ownership used by the connection |
| `pkg/server/internal/packetio.go` and `packetio_test.go` | packet framing and sequence behavior |
| `pkg/server/internal/column/{column,convert}.go`, `column_test.go`, `BUILD.bazel` | column metadata consumed by result writers |
| `pkg/format/textrow/{result_encoder.go,result_encoder_test.go,textrow.go,textrow_test.go,BUILD.bazel}` | text row encoding and tests |
| `pkg/parser/mysql/{const.go,charset.go,state.go}`, their tests, and `BUILD.bazel` | protocol constants, charset state, and packet field definitions |

The remaining files in the enumerated `pkg/server` tree (HTTP handlers,
metrics, RPC, TLS, test servers, and nested support packages) have no call
edge into this EOF value decision and were retained unchanged. No Go source,
test, fixture, generated output, platform variant, or Bazel file was edited.

## Go contract

`pkg/server/conn.go:1689-1721` encodes the live statement context values in
`writeOkWith`: affected rows, last insert id, status, warnings, and
`LastMessage`. `conn.go:1771-1777` invokes that helper with `EOFHeader` when
`CLIENT_DEPRECATE_EOF` is negotiated. Legacy EOF packets continue to carry
only warning/status fields.

## Rust ownership and implementation

The Rust ownership inventory covered the protocol packet structs/streams and
all server result writers:

- `rust/crates/tidb-protocol/src/{resultset.rs,resultset_stream.rs,prepared_statement.rs}`
- `rust/crates/tidb-protocol/tests/{resultset_source.rs,resultset_stream_source.rs}`
- `rust/crates/tidb-exec/src/status_result.rs` and
  `rust/crates/tidb-exec/tests/status_result_source.rs`
- `rust/crates/tidb-server/src/{connection_writers.rs,sql_node.rs,mysql_connection.rs,cursor_state.rs,pipeline_session.rs,cluster_session_node/mod.rs,real_tikv_node/mod.rs}`

`ResultSetOptions`, `EofPacket`, `QueryResult`, and the published
`StatusResultSnapshot` now retain affected rows, last insert id, and info
bytes. Text, binary, prepared, and cursor writers forward that snapshot to
the deprecated-EOF encoder. Pipeline and cluster sessions publish their live
statement insert id; row result defaults remain zero/empty where the Go
statement context has no corresponding Rust producer.

## Regression evidence

The focused protocol test
`deprecate_eof_preserves_statement_output_like_go_write_eof` asserts the exact
OK-shaped payload for non-zero affected rows, last insert id, status, and info,
while asserting that legacy EOF remains compact. The executor status regression
also checks the same values survive publication into `ResultSetOptions`. The source guard
`eof_encoder_uses_the_statement_snapshot_instead_of_zero_literals` prevents a
future regression to hardcoded zero values. Against the pre-change source, the
guard's required `affected_rows: packet.affected_rows` expression is absent
(fail-before); it passes after the implementation.

## Validation

Profile: **Ready** for this Rust-only behavior batch.

- `cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -p tidb-protocol --all-targets` — passed.
- `cargo +nightly-2026-08-22 nextest run --manifest-path rust/Cargo.toml --offline --locked -p tidb-protocol --all-targets --no-fail-fast --status-level fail --final-status-level pass` — 120 passed, 0 skipped.
- `OPENSSL_DIR=.../policy/openssl PKG_CONFIG_PATH=.../policy/openssl/lib/pkgconfig cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-exec --test all status` — 18 status/session tests passed.
- `OPENSSL_DIR=.../policy/openssl PKG_CONFIG_PATH=.../policy/openssl/lib/pkgconfig cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -p tidb-server --lib --no-default-features` — passed (pre-existing warnings only).
- `rustfmt +nightly-2026-08-22 --check` on every changed Rust source/test file — passed.
- `make lint` and `git diff --check` — run in the final Ready gate.

## Remaining boundary and risks

Rust has no session-owned equivalent of Go's `StatementContext.LastMessage`,
so current SQL producers intentionally publish an empty info field. The
transport now preserves info whenever a producer supplies it; adding a live
producer requires a separate session-state parity unit. No Rust-only packet
behavior was removed beyond replacing the incorrect hardcoded deprecated-EOF
values. Wire compatibility risk is limited to clients that intentionally
inspect the previously-zero fields; those clients now receive the Go-shaped
statement values.
