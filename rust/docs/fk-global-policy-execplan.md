# Share the foreign-key feature policy across DDL and metadata

This living plan follows `PLANS.md`.

## Purpose / Big Picture

Make `SET GLOBAL tidb_enable_foreign_key` control the DDL consumers Go controls. A constraint built while disabled retains version zero and stays inactive after re-enabling; existing version-one row checks remain active. CREATE, ALTER, parent publication, rename and DDL validation must distinguish this process setting from the session's `foreign_key_checks`.

## Progress

- [x] Read live Go owners at master `7a3dacb52efe58d28db360ae8639d8838c376544` and confirm clean TiDB baseline `ea3cea1cbf65d6385f6ef6a5da274b6c3d4bd1ac`.
- [x] Capture baseline SQL observations: seven of thirteen fail after correcting four admission expectations against the full Go call chain.
- [x] Connect global publication, constraint versions, catalog consumers and DDL checks. Add the missing persistent TRUNCATE/index/database submission callers and legacy SHOW annotation.
- [x] Group validation:138 Rust tests,14 live MySQL/unistore checks, affected all-target checking and make lint pass; self-review and both registers updated.
- [ ] Commit through the actual hook, run a fresh locked server build, push and verify remote; save Cloud checkpoint.

## Context and Orientation

Go owns the atomic in `pkg/sessionctx/vardef/tidb_vars.go`; sysvar hooks publish/read it. `pkg/ddl/executor.go:buildFKInfo` chooses the metadata version. `pkg/ddl/foreign_key.go` gates DDL validation, while infoschema only indexes version-one references. DML uses the persisted version, not the current global switch. Rust's `tidb-vardef`, `tidb-session/src/vars.rs`, `tidb-exec/src/foreign_key_build.rs` and `tidb-executor/src/ddl` are the corresponding existing owners. `KvForeignKey` currently loses version, and the server adapter discards old constraints instead of retaining their metadata.

## Plan of Work

First reproduce failures against the retained baseline server. Then add one shared atomic, publish it only from live global registries (staged cluster images remain private), and retain/read the constraint version in both metadata paths. Migrate CREATE validation/index synthesis, referred-key lookup, DML planning, rename and ALTER validation consumers together. Preserve Go's ungated definition validation and ALTER's supporting-index/existing-row checks. Keep unfinished durable DDL recovery and distributed FK ownership explicit in N03/E02/D11; this is maintenance of existing owners, not complete `pkg/ddl` acceptance.

## Concrete Steps

Activate `/workspace/.cloud-setup/env.sh` in each shell and set `CARGO_BUILD_JOBS=1`. Work in `/workspace/tidb`, branch `hparser-integration`. Baseline and final live checks run with `PYTHONPATH=/workspace/.cloud-setup/python python /workspace/.cloud-setup/fk-global-batch/wire.py`; this helper starts/stops only its own unistore server. From `rust/`, run the isolated `cargo test --locked -p tidb-session --test fk_global_policy`, affected existing FK tests, and `cargo check --locked -p tidb-session -p tidb-exec -p tidb-executor -p tidb-server --all-targets`. Run `make lint` at repository root. Normal commit must execute the selected `hooks/pre-commit` and its locked server build. Immediately before normal push run `cargo build --locked -p tidb-server` from `rust/`, then verify remote SHA.

## Validation and Acceptance

Observe global reads/publication and private staged loads, version-zero metadata/index omission, continued version-one DML enforcement while disabled, enabled/disabled parent validation, and Go's rename/drop/truncate/index policy. Run failing cases before changes and identical cases after. Isolate process-global mutation tests in their own integration-test process so other FK tests cannot observe transient OFF. No full Go suite, multi-node TiKV or performance claim follows from these checks.

## Idempotence and Recovery

Preserve concurrent files. Do not reset branches, bypass hooks or force-push. Preserve baseline receipts outside checkout. Retire only completed owned executables with identity/process receipts if disk capacity requires it; retain dependencies. Native client is unchanged.

## Surprises & Discoveries

The global switch does not disable existing version-one DML. ALTER ADD still creates its supporting index and validates existing rows; CREATE only creates support indexes for version-one constraints. DROP/TRUNCATE/index/column/database admission remains ungated even though their owner rechecks are gated. Preserve admission checks in the existing combined owner; do not bypass them based on the worker helper alone. The first wire attempt omitted environment activation and hit the known small-thread-stack failure; rerun with saved activation, and do not count that attempt as a behavioral baseline.

## Decision Log

Select the connected N03/E02/D11 policy consumers as one batch. Preserve unconditional definition checks and Go's per-operation distinctions instead of introducing a universal FK bypass. Use isolated process tests for shared atomic transitions.

## Outcomes & Retrospective

Implementation complete. Final grouped Rust selections pass138 tests with zero failures/ignored; affected all-target checking and make lint pass. Locked server build and14 live MySQL/unistore checks pass, with owned server exit0. Publication gates remain in progress; their final results are recorded in the external final-handoff receipt. The stale direct-executor rollback assertion now runs through the session transaction owner; the false ignored serial missing-index surrogate and unused render helpers are removed. Online Go concurrency/state obligations remain open. Broad finding closure requires all remaining obligations; do not lower the count based on this partial owner maintenance.

## Artifacts and Notes

Cloud logs and wire receipts: `/workspace/.cloud-setup/fk-global-batch`. The tracked final receipt will be `rust/docs/parity/current-audit/fk-global-policy-validation.json`.

## Interfaces and Dependencies

Add `tidb_vardef::ENABLE_FOREIGN_KEY: AtomicBool`, default true, and `KvForeignKey.version: i64` matching `FKInfo.version`. Existing Go-version checks and DDL owners consume these; no new dependency or alternate execution path is needed.
