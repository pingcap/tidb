# domain: topn_slow_query / historical_stats / plan_replayer parity audit (baseline a85e0fd5df)

Audit of Go `pkg/domain/{topn_slow_query,historical_stats,plan_replayer}.go`
(+ plan_replayer_dump.go) against the corresponding tidb-domain modules.

## Fixed this batch

- plan_replayer worker loop: Go defers `util.Recover` inside handleTask, so
  a panicking dump is swallowed and the worker keeps draining; the Rust
  `run` now wraps `handle_task` in `catch_unwind` with the same
  log-and-survive semantics.
- Header claims corrected: the three plan-replayer metrics are reproduced
  against the workspace registry (not dropped); the status type has eight
  methods; `GetWorker` is stood in by `take_receiver`.

## Verified matching

- topn_slow_query: heap sift orders (container/heap replication),
  replace-top-only-when-slower, FIFO evict, takeLastN newest-first,
  RemoveExpired strict-cutoff + no-op skip, Query sort, three QueryTop
  scopes, closed latch. No Go dedup exists.
- historical_stats: all four error texts byte-for-byte, -1 sentinel,
  0-after-drain, panic on send-after-close/double-close, capacity 16,
  full-mailbox drop + warn, enabled short-circuit, partition lookup
  order, failed-counter before error.
- plan_replayer: parse_time order, GC inclusive cutoff + capture/default
  retention substring rule, per-deleted clear_finished_task, all four SQL
  strings byte-identical, "unknown" instance + IPv6 bracketing, collector
  replace + per-key abort, all eight status methods, handleTask three
  gates, SendTask remove-only-non-continuous + full-channel discard +
  panic-on-closed; PlanReplayerDirName and the three-branch file-name
  generator (replayer.rs); SendTask analog.

## Open / documented

- `plan_replayer_dump.go` (dump routine: zip layout, sql-meta TOML keys,
  presign URL, extractTableNames, stats fallback, generateRecords) is
  unported and declared; PLAN REPLAYER DUMP executor and HTTP handler
  depend on it. Success/Failed counters therefore only cover the capture
  worker until it lands (Go counts executor dumps too).
- sort.Sort (unstable) vs sort_by (stable): equal-duration ties in
  ADMIN SHOW SLOW may order differently.
- Channel capacities 1000/10 replaced by the run-loop boundary; two
  per-path time.Now calls collapsed to one; walk errors returned instead
  of only logged; failpoints (sendHistoricalStats,
  InjectPlanReplayerFileNameTimeField) omitted (unnamed in replayer.rs).

## Validation

- `cargo test -p tidb-domain` (143 pass), `cargo fmt`, `git diff --check`,
  `make lint`.

## Follow-up: the table-name extractor is ported

`tableNameExtractor` + `findFK` + `handleIsView` land as
`TableNameExtractor` (+ `ExtractSchemaSource` / `ExtractViewParser`
seams): the AST walk collects `db.table` pairs, skips CTE references,
keeps the view flag, follows foreign keys recursively with a
visited-set guard, and re-parses+walks a view's SELECT through the same
extractor. Real-SQL tests: view recursion pulls the referenced table,
CTE references are skipped, FKs pull in their tables.

## 2026-09-06 build-artifact closure

The plan-replayer additions introduced three direct manifest dependencies
without refreshing the `tidb-domain` entry in `rust/Cargo.lock`, making every
frozen Cargo command fail before compilation. The lock entry now includes
`toml`, `tidb-util`, and dev-only `tidb-parser`; no package version or checksum
changed. Frozen metadata resolution, all 158 domain owner tests, all-target
compilation, repository lint, and diff hygiene pass. Exact evidence is in
`../testport/receipts/domain_plan_replayer_retention.md`.
