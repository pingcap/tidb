# Share read consistency across SQL readers


## Purpose / Big Picture


Stale reads must carry their transaction-selected replica scope and stale-read flag through point, batch-point, and coprocessor requests. Changing a transaction back to ordinary reads must clear stale mode while preserving its existing label-reset behavior. This maintenance batch advances S04, O13, and N03 without claiming complete Go packages or local-transaction support.

## Progress


- [x] Refreshed Go master and reviewed the existing session, executor, DistSQL, and native snapshot owners.
- [x] Added grouped regressions: four failures before wiring; one existing sysvar control passed. The first over-broad target command failed on disk space before running tests; narrowed target commands produced the behavioral baseline.
- [x] Connected the statement policy through session, point/batch snapshots, and coprocessor consumers; native mode reset remains intact.
- [x] 114 grouped Rust cases passed (34 executor,37 session,12 coprocessor,31 native), followed by affected all-target checking, make lint and the locked server build.
- [x] Updated both registers, source/log receipt and self-review.
- [ ] Publish through the actual hook and a fresh pre-push build; record final SHA/results in `/workspace/.cloud-setup/read-consistency-batch/final-handoff.json` (outside the commit to avoid self-referential commit IDs).

## Context and Orientation


Work in `/workspace/tidb` on `hparser-integration`, initially `480abecaed1edcda2b93f7010025538104ad26f8`. Native `/workspace/client-rust` remains at `8b752f9638ad157931725b66ffdc57e0465432a9`. Comparison Go master is `7a3dacb52efe58d28db360ae8639d8838c376544`, exported in `/workspace/.cloud-setup/go-master`. Go `pkg/sessiontxn/isolation/base.go:GetReadReplicaScope` selects transaction scope or configured closest scope; `staleread/provider.go` selects the configured scope and marks snapshots stale. `pkg/executor/builder.go:InitSnapshotWithSessCtx` and reader request builders propagate those values.

Rust `tidb-session/src/stmt_ctx.rs` constructs statement context, `tidb-executor/src/stmt_context.rs` produces snapshot options, and `remote_scan.rs` retains coprocessor context. `tidb-exec/src/cop_scan.rs` builds DistSQL requests. `tidb-txnkv/src/transaction/client.rs` configures the maintained native client. Staleness means a read uses an earlier timestamp with TiKV stale-read routing; `tidb_snapshot` selects a historical timestamp but is not itself that routing mode.

## Milestones and Concrete Steps


First extend the existing point, coprocessor, session, and native-request suites with scope/flag propagation and mode-reset cases. Add only the data interface needed to compile those tests, leaving consumers disconnected for the failure receipt. Then connect session-selected policy through both reader paths while retaining the native mode setter's existing reset behavior. Retire their duplicated scope derivation. Do not add another transport or native fork.

Source `/workspace/.cloud-setup/env.sh` and set `CARGO_BUILD_JOBS=1`. Run Cargo from `/workspace/tidb/rust`. Group affected tests in their existing targets; record exact final filters in `rust/docs/parity/current-audit/read-consistency-batch-validation.json`. Run `cargo check --locked --all-targets -p tidb-executor -p tidb-session -p tidb-exec -p tidb-txnkv -p tidb-server`, `make lint` from the repository root, and `cargo build --locked -p tidb-server` from rust. Reuse build artifacts; do not clean caches.

## Validation and Acceptance


Point and batch readers must preserve explicit scope and staleness; coprocessor transport must receive the same metadata. Native Get/BatchGet must carry stale flags and reset them when returning to ordinary reads. Low-level Scan must continue to omit stale flags, as client-go scan.go does. Scope changes must not leak labels from a previous statement. Session tests distinguish ordinary, stale, and snapshot reads. Tests must fail before wiring and pass after. This does not prove live multi-node SafeTS behavior or complete package parity.

## Idempotence and Recovery


Preserve unrelated work, never force-push or bypass hooks. All probes use test fixtures and leave production data untouched. If validation fails, fix only the connected batch and repeat the affected gate. If publishing fails, retain the commit and report its local SHA and sanitized error. External logs belong in `/workspace/.cloud-setup/read-consistency-batch`; committed receipts must remain interpretable without those logs.

## Interfaces and Dependencies


Statement context retains read replica scope and stale mode, snapshot options carry them to the native transaction, and pushdown context carries them to DistSQL. Native setters already exist; no dependency revision change is planned. Transaction-local scope has no composed producer here and remains explicitly outside acceptance.

## Surprises & Discoveries


Both snapshot and coprocessor consumers independently derive scope and omit stale mode. A suspected label-reset defect was ruled out: native Transaction::set_replica_read replaces the whole replica configuration. No label-reset fix is needed. client-go low-level Scan intentionally constructs its request without stale mode; preserve that distinction from SQL coprocessor scans. Historical snapshot and stale transactions share a Rust catalog adapter, so the session producer must distinguish the `tidb_snapshot` mode.

## Decision Log


2026-10-07: repair existing read ownership across three related findings in one batch. Do not implement unrelated auto-ID transport, FK shared locks, or normalized-plan generation during this batch. Preserve broad finding status until its remaining prerequisites are satisfied.

## Outcomes & Retrospective


The batch connects existing owners across S04/O13/N03; no new native implementation is needed. Four baseline regressions fail and 114 grouped cases pass. Exact commands, source hashes and limits are in `parity/current-audit/read-consistency-batch-validation.json`. The initial broad target invocation exhausted the 32GiB disk; selecting only the four affected test targets and retiring identified inactive link outputs recovered validation without purging compiler libraries. Native label reset was already correct, and low-level Scan's non-stale behavior is source-compatible. All three parent findings remain partial; live SafeTS/local-transaction and complete package acceptance are not claimed. Commit/publication gates remain pending.
