# Share TSO stream retention and timestamp completion


This living ExecPlan follows root PLANS.md.

## Purpose and scope


Consolidate native and TiDB timestamp stream retention, response arithmetic and
ordering checks. TSO allocates monotonically increasing transaction timestamps.
Both consumers must retire failed/cancelled exchanges without discarding healthy
peers, split each batch in Go's plain logical arithmetic and reject regressed
allocations. Pinned Go clients/tso/stream.go ignores response header errors and
cluster IDs; TiDB's additional rejection incorrectly retries valid payloads.
This maintains P03/P06 existing owners; it does not accept complete PD packages.

## Progress

- [x] Confirm clean integration/native branches and refresh Go master at
  7a3dacb52efe58d28db360ae8639d8838c376544, unchanged external PD afa43111d149.
- [x] Capture native regressed allocation and adapter absent-header baseline failures.
- [x] Move stream retention and completion into native shared owners; migrate
  both consumers and remove duplicate TiDB arithmetic/retained-stream code.
- [x] Run grouped relevant lifecycle tests, affected checks, lint and review:76 native and76 adapter tests pass, with zero failed/ignored. Both affected all-target checks and make lint pass.
- [ ] Publish native, maintained sync, actual TiDB commit hook, fresh build and
  normal push; verify both remotes and save the reusable Cloud checkpoint.

## Context and interfaces


Editable repositories are /workspace/client-rust master and /workspace/tidb
hparser-integration. Native src/pd/timestamp.rs owns asynchronous dispatch;
TiDB rust/crates/tidb-pd-client/src/client/worker.rs owns synchronous commands.
Native src/pd/service_discovery.rs already owns wire streams. Move retained
stream collection there, with cancellation-safe removal before an exchange and
reinsertion only on success. Share response batch validation and ordering in
native src/pd/tso_batch.rs; expose it for the synchronous consumer. Keep request
queues and application-specific retry/error projections in existing callers.

## Milestones and plan of work


First extend the existing native and adapter wire fixtures and run combined
baseline filters. Then migrate both owners before final compilation. Preserve
Go count mismatch identity, plain logical allocation and native Rust bounds.
Keep the timestamp high-water mark across native oracle replacement. Remove
adapter-local splitting/model tests after their meaningful coverage moves to
the shared owner. No new harness, dependency or build target is needed.

Finally run grouped tests and all-target checks for native, PD adapter and server,
make lint, the actual locked-build precommit hook and fresh locked prepush builds.
Record counts and limits in both registers and a validation JSON. Full Go package,
live distributed and performance acceptance remain unclaimed.

## Concrete steps and acceptance


Activate /workspace/.cloud-setup/env.sh and CARGO_BUILD_JOBS=1 in every shell.
Native commands run in /workspace/client-rust with --config
/workspace/.cloud-setup/native-cargo.toml; TiDB commands run in /workspace/tidb/rust.
Run cargo test --locked with native --lib pd::timestamp::tests:: and adapter
--lib --test all tso_source:: pd_client_source:: client::worker_lifecycle_tests::.
Baseline must expose native invalid/regressed timestamp acceptance and adapter
header rejection. Final tests must prove corrected behavior and existing stream
reuse, retirement, discovery, cancellation and joined shutdown.

## Idempotence and recovery


Preserve concurrent edits and caches. Do not replay mutation helpers or force
push. Native source enters TiDB only through rust/scripts/sync-tikv-client-rs.sh,
including all patches and protobuf regeneration. Remove only identified inactive
executables when disk space requires it; keep evidence and libraries. Before
every push run cd rust && cargo build --locked -p tidb-server in TiDB. Verify
remote SHA. Recover published mistakes through reviewed revert, not history rewrite.

## Surprises & Discoveries


Native baseline returns (100,0) after (100,1); adapter baseline times out on a valid payload with no header. A first compile found one internal test caller missing its new tracker argument; it was corrected before the grouped test rerun.

A first grouped run passes72 and fails4 failover fixtures that restart timestamp allocation on peer replacement. Their peers now share the server-owned counter; shutdown and transport assertions remain. The unused per-stream counter state is removed.

The adapter's comments describe obsolete suffix shifts while its actual code
already uses plain addition. Its checked_shl tests the shift amount, not lost
physical bits. Native completion currently lacks its sibling's range and
monotonicity guards. Go stream adapters ignore response header errors and cluster
IDs, while its dispatcher checks batch ordering.

## Decision Log


Decision (2026-10-08): consolidate the retained stream and completion owners in
one batch. Preserve Rust's explicit invalid timestamp errors instead of Go's
process panic; do not claim dispatcher/concurrency or whole-package closure.

## Outcomes & Retrospective


Shared completion and stream owners are integrated. Native76 and adapter76 selected tests pass; all-target checks and lint pass. Native 02880abbab5ed89a4dc603a4ddc7935853870ea6 is published and synchronized with all four patches and protobuf regeneration. The adapter loses494 lines across tso.rs and client/worker.rs. No broad root or package is accepted:56 remain unresolved. TiDB publication gates are pending at this precommit freeze; /workspace/.cloud-setup/tso-completion-batch/final-handoff.json records final outcomes.
