# Compose MPP failure detection and safe gather recovery

## Purpose / Big Picture


Follow Go master 1f819a0b4a6cc07f9a8ff07e6777761a770c6d3d to connect failed-node
admission, process probing, and bounded raw-response recovery in the existing MPP
full-scan path. A failed compute node must not receive repeated foreground probes
until background recovery; a memory-limit failure may restart a gather only before
any response is delivered. Retain one query identity and snapshot across attempts.

## Progress


- [x] Inspect Go discovery, prober, dispatch and coordinator/recovery callers and existing Rust owners.
- [x] Reproduce both shutdown/cooldown failures and implement the connected lifecycle batch.
- [x] Pass79 grouped Rust cases,19 live SQL/startup assertions, affected all-target checks, make lint and locked server build.
- [x] Review and update both finding registers and validation evidence.

Commit/push gates and the saved Cloud checkpoint are finalized in
`/workspace/.cloud-setup/mpp-failure-lifecycle-batch/final-handoff.json` after
the actual hook, fresh locked build and remote-SHA verification.

## Context and Orientation


Work in `/workspace/tidb`, branch hparser-integration, baseline
7aa3044431c35340e2bfbc3d0cb4b64e11dfed11. Native `/workspace/client-rust` remains
02880abbab5ed89a4dc603a4ddc7935853870ea6. The process prober is in
`rust/crates/tidb-txnkv/src/mpp_probe.rs`; its shared transport adapter and gather
responses belong to `tidb-exec/src/tiflash_mpp_scan.rs`. Server startup composes
owners in `tidb-server/src/lib.rs`. Statement policy flows from the session through
StmtContext to PushdownStatementContext. A gather is one distributed execution of
a statement's fragment; recovery allocates another gather within the same query.

## Plan of Work and Milestones


First repair detection timing and join ownership, connect startup/shutdown and
foreground admission using the existing store fleet. Then replace decoded task
concatenation with a raw response owner, using Go's two-packet holding capacity and
three memory-limit recovery attempts. Disable recovery when TiFlash fallback is
allowed, after packet delivery, and for local cancellation or unrelated errors.
Close all old task responses before opening a replacement gather. Extend existing
fixtures and retire the superseded decoded-row fanout fixture.

## Concrete Steps and Validation


Source `/workspace/.cloud-setup/env.sh`, export CARGO_BUILD_JOBS=1, then run Cargo
from `rust/`. Capture fail-before/pass-after with `cargo test --locked -p
tidb-txnkv --lib mpp_failure_batch -- --test-threads=1`. Run grouped MPP/topology
and statement-policy tests, all-target checks for affected crates, and root
`make lint`. The actual hook must pass `cd rust && cargo build --locked -p
tidb-server`; repeat this immediately before any push and verify remote SHA.
Evidence lives in `/workspace/.cloud-setup/mpp-failure-lifecycle-batch`.

## Surprises & Discoveries


The SQL process uses its own RegionCache wrapper, while the native compute-store
cache belongs to a different RegionCache instance. Wiring a second native cache
would duplicate ownership. Canonical compute-cache migration needs the storage
owner migration; do not disguise a per-MPP cache as that repair. Go deliberately
sends region-bearing dispatch only once; full fragment rescheduling is a separate
obligation. Go cooldown starts after detection, not before a slow RPC.

## Decision Log


Maintain existing partial copr/coordinator owners; this is not complete package
transcreation. Prioritize connected failure admission, joined shutdown and safe
raw-response recovery in this batch. Keep PD compute-cache migration explicitly
open rather than creating a competing cache. Preserve original Go semantics and
Rust cancellation/memory safety. No native source or generated protocol changes.

## Outcomes & Retrospective


The connected batch passes79 Rust cases and19 live startup/SQL assertions. Two baseline failures are repaired. M04/M05/N03 retain explicitly stated cache/coordinator/configuration residuals;54 broader findings remain unresolved. Raw packet memory transfers once, the retry opener releases its capabilities on close, and replacements keep one schema version. Publication evidence is finalized outside the checkout after the actual hook and fresh locked build.

## Idempotence and Recovery


Preserve concurrent work and exact origins. Do not force push or bypass hooks.
Retire only completed owned executable artifacts for space, retaining caches.
Repeated close/stop must release resources exactly once; errors preserve the
original MPP error if autoscaler recovery or replacement setup fails.

## Interfaces and Dependencies


Reuse QueryResponse, SelectResponseIter, StatementMemory, shared query runtime,
TonicCoprocessorClient and the existing TopoFetcher recovery interface. No new
dependency or alternate connection pool is needed.
