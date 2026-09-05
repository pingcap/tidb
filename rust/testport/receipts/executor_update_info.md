# `pkg/executor` / Rust UPDATE info-message alignment receipt

Status: bounded Rust alignment. This closes the live pipeline/cluster UPDATE
summary-message producer and its text-protocol OK packet; it does not claim
that every Go DML info-message producer or all of `pkg/executor` has been
transcreated.

Comparison source: Go `origin/master` at `f2c346fe4f368ff855e17c1f62e28a89ba7f9723`
(refreshed 2026-09-05).

## Inventory read before editing

The complete direct `pkg/executor` tree was enumerated and read before the
Rust edit: 165 tracked artifacts consisting of 87 production Go files, 76 Go
tests, one `BUILD.bazel`, and `OWNERS`. Its recursive tree contains 508
tracked artifacts: 195 production Go files, 235 tests, 60 Bazel files, eight
fixtures/testdata artifacts, and ten ownership/support artifacts. Nested
directories such as `pkg/executor/internal`, `pkg/executor/importer`, and
`pkg/executor/adapter` remain separate Go packages; their production, test,
fixture, generated/platform, and build inputs were included in the count.
No Go file, fixture, generated source, platform variant, or Bazel artifact was
edited.

The behavior owner was read line by line in `pkg/executor/update.go`, including
`UpdateExec.Next`/`Close` and `setMessage`: Go publishes
`Rows matched: %d  Changed: %d  Warnings: %d` after the update completes, while
`client_found_rows` changes only the affected-row count. The adjacent
`pkg/executor/insert.go`, `replace.go`, and `load_data.go` producers were also
checked and remain separate follow-up boundaries. The direct package tests and
`pkg/session/test/common` wire-facing fixtures were inventoried before the
focused Rust test edit.

Rust owners inventoried before editing were:

- `tidb-executor`: `stmt_context.rs`, `driver/dml.rs`, and
  `driver/multi_dml.rs` (single/multi UPDATE counters and warning context).
- `tidb-session`: `lib.rs` statement lifecycle and `dispatch.rs` UPDATE arm.
- `tidb-server`: `sql_node.rs`, `pipeline_session.rs`,
  `cluster_session_node/mod.rs`, `connection_writers.rs`,
  `mysql_connection.rs`, and the source-aggregated TCP fixture
  `tests/pipeline_mysql_client_source.rs`.
- Existing configured real-TiKV `QuerySession` implementations were checked;
  their write reports have no Go-equivalent UPDATE message source and retain
  the default empty info field in this bounded batch.

## Contract and implementation

Go's `UpdateExec.Close` reports matched rows, changed rows, and warning count
independently of whether `CLIENT_FOUND_ROWS` is negotiated. Rust now records
that message in the per-statement `StmtContext`, publishes it from single- and
multi-table UPDATE dispatch into the session, and carries it through the
pipeline and cluster `QuerySession` implementations. The connection writer
adds an info-aware OK helper while retaining the existing empty-info helper;
text and prepared general-write paths pass the session bytes into the
length-encoded OK field. Deprecated-EOF result snapshots also receive the
same info bytes. Command paths that bypass the ordinary executor lifecycle
(`SET`, routed cluster DDL, and local-load handling) clear the prior message so
an UPDATE summary cannot leak into a later OK packet.

## Regression evidence

- Fail-before: after adding the focused assertion but before the producer and
  writer changes,
  `env OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-server --test all client_found_rows_reports_matched_updates_over_the_mysql_wire -- --nocapture`
  failed at the new info assertion because the OK packet's info field was
  empty (`complete length-encoded OK info`).
- Pass-after: the same command passed (`1 passed`, `210 filtered out`) and
  observed affected rows `2` plus exact info
  `Rows matched: 2  Changed: 1  Warnings: 0` over COM_QUERY; the following
  `SET sql_mode = ''` returned an empty info field.

## Ready validation

Profile: **Ready**, because this is a Rust code-changing executor package
batch intended for immediate integration.

- Focused TCP regression above passed.
- `cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline
  --locked -p tidb-executor -p tidb-session -p tidb-server --lib` passed with
  pre-existing warnings.
- `rustfmt +nightly-2026-08-22 --edition 2021 --config skip_children=true
  --check` passed on changed production files; the source-aggregated TCP test
  retains the package's existing formatting to avoid unrelated whole-file
  churn, and its changed helper/test block compiled and passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH
  GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10
  TMPDIR=/tmp/tidb-codex make lint` and `git diff --check` passed.
- The Bazel prepare gate found no Go, Bazel, or Go module changes, so
  `make bazel_prepare` was not required.

The broader `tidb-server --lib` test target remains blocked by the unrelated
existing non-exhaustive materialized-view match in
`cluster_session_node/tests/mock_seams.rs`; this batch does not touch that
mock or the DDL enum.

## Remaining boundary and risk

The change is wire-compatible for clients: an empty info field remains empty
for statements without a producer, while UPDATE clients now receive Go's
summary text. It adds one small per-session string and copies it only at the
statement boundary; no storage or planner behavior changes. INSERT/REPLACE,
LOAD DATA, and configured real-TiKV write paths still need their own Go-derived
message producers and focused regressions. No external MySQL client or
production-cluster packet capture was run.
