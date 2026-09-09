# `pkg/server` / Rust `CLIENT_FOUND_ROWS` alignment receipt

Status: bounded Rust alignment. This closes the negotiated found-rows wire and
affected-row seam; it does not claim that all of `pkg/server` or
`pkg/executor` has been transcreated.

Comparison source: Go `origin/master` at `f2c346fe4f3` (refreshed 2026-09-05).

## Inventory read before editing

The complete tracked `pkg/server` tree was enumerated and read before the Rust
edit: 110 files consisting of 43 production Go files, 41 Go tests, 25
`BUILD.bazel` files, and `pkg/server/AGENTS.md`. It contains no fixture,
generated-source, platform-specific, benchmark, or fuzz artifact. The current
tree is unchanged from the already recorded server receipts and has no diff
against the refreshed Go comparison ref.

Because the capability changes executor output, the direct Go `pkg/executor`
package was also re-enumerated: 165 tracked artifacts (87 production Go, 76 Go
tests, one `BUILD.bazel`, and `OWNERS`). Its recursive tree has 508 artifacts,
including 195 production Go files, 235 Go tests, 60 Bazel files, eight fixture
or testdata artifacts, and ten ownership/support artifacts; nested directories
remain separate Go packages. `pkg/session/test/common` was checked as the
client-visible regression owner (three Go tests plus one Bazel file). No file
in those Go inventories was edited.

The behavior owners read line by line were `pkg/server/server.go`
(`defaultCapability`), `pkg/executor/write.go` (`updateRecord` touched/affected
ordering), `pkg/executor/insert.go` and `insert_common.go` (duplicate-key
affected rows), and `pkg/session/test/common/common_test.go`
(`TestAffectedRows`). The Rust audit covered the handshake/connection, all
`SessionContext` factories, pipeline and cluster session construction,
statement-context creation, single- and multi-table DML, and both configured
real-TiKV write adapters.

## Contract and implementation

Go advertises `ClientFoundRows` and retains the negotiated bit in session
variables. Without it, UPDATE reports changed rows. With it, UPDATE reports
successfully touched rows, including unchanged matches; an unchanged
`ON DUPLICATE KEY UPDATE` reports one, while a changed duplicate-key update
continues to report two.

Rust now advertises bit `1 << 1`, stores only the negotiated bit needed by the
session, and copies it into every statement context. Single-table UPDATE keeps
separate matched, touched, and changed counters so LIMIT/execution accounting
does not leak into affected-row reporting. Multi-table UPDATE counts each
successfully touched target once. Configured point updates translate only the
`UnchangedRow` no-write report to one; missing rows and identical REPLACE keep
their independent semantics. Pipeline, cluster, configured single-node, and
configured multi-node factories all apply the same session value.

## Regression evidence

- Fail-before: with the TCP regression present and before advertising the bit,
  `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline
  --locked -p tidb-server --test all
  client_found_rows_reports_matched_updates_over_the_mysql_wire -- --nocapture`
  failed in the handshake assertion (`left: 0`, `right: 2`).
- Pass-after: the same TCP test passed and proved the advertised bit, one
  changed plus one unchanged UPDATE returning two, and an unchanged
  duplicate-key UPDATE returning one (`1 passed`, `210 filtered out`).
- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline
  --locked -p tidb-session --lib multi_table_update_counts_ -- --nocapture`
  passed the found-rows and default changed-row vectors (`2 passed`, `1836
  filtered out`).
- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline
  --locked -p tidb-exec --test all
  client_found_rows_changes_only_an_unchanged_match_report -- --nocapture`
  passed (`1 passed`, `815 filtered out`).

The Cargo commands used the repository's local OpenSSL runtime through
`OPENSSL_DIR` and `DYLD_FALLBACK_LIBRARY_PATH`; no dependency file changed.

## Ready validation

Profile: **Ready**, because this is a code-changing package batch intended for
immediate integration.

- The three focused test commands above passed.
- `cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline
  --locked -p tidb-executor -p tidb-session -p tidb-exec -p tidb-server --lib`
  passed with pre-existing warnings.
- `rustfmt +nightly-2026-08-22 --edition 2021 --config skip_children=true
  --check` passed on every changed production/module Rust file and the focused
  module-local test. The two source-aggregated integration files retain their
  existing package style to avoid unrelated whole-file churn; their new blocks
  compile in the focused tests.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH
  GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10
  TMPDIR=/tmp/tidb-codex make lint` passed.
- `git diff --check` passed. The Bazel prepare gate found no Go, Bazel, or Go
  module change, so `make bazel_prepare` was not required.

The broader `tidb-server --lib` test target was attempted but is blocked by an
unrelated existing non-exhaustive match in
`cluster_session_node/tests/mock_seams.rs` for four materialized-view no-op DDL
variants. This batch changes neither that mock nor the DDL enum; the production
library check and TCP integration target both pass.

## Remaining boundary and risk

The wire-facing behavior is backward compatible for clients that do not
negotiate the bit. Clients that do negotiate it now receive Go's matched/touched
counts instead of the former Rust-only changed counts. The change adds only
small per-session booleans and counters; no new storage read or write is
introduced. Go's informational OK-packet message producer remains the separate
`StatementContext.LastMessage` boundary recorded by the deprecated-EOF receipt.
