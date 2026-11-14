# Port O15 keyspace runtime and next-generation consumers

This ExecPlan follows root `PLANS.md` and is maintained as work proceeds.

## Purpose / Big Picture

Make the Rust runtime select the same keyspace as Go and preserve the isolation and resource lifetime of cross-keyspace work. Consumers include DXF status and metering and plan-replayer external storage. A keyspace is an independent tenant's KV namespace; routing only the name while sharing the original store is incorrect.

## Progress

- [x] Read root instructions and inventory Go keyspace, crossks, store, DXF status/metering and extstore owners.
- [x] Confirm missing production cross-keyspace acquisition and next-generation external storage by current Rust source.
- [x] Port all three named keyspace tests; fix CodecV1 NullspaceID with Go capture and a demonstrated failing Rust regression. Storage/domain codec wiring remains.
- [x] Add Go etcd namespace behavior for the exposed KV/transaction/watch operations and verify Go capture, regression sensitivity, and an isolated real etcd server.
- [ ] Port crossks ownership, creation, schema/session lifecycle and Go cross-keyspace tests.
- [ ] Port named next-generation consumers and their Go tests.
- [x] Run changed-crate suites and update both registers to partial with precise remaining scope.
- [ ] Commit coherent changes: blocked because the worktree Git index is outside writable roots; no escalation is available.

## Surprises & Discoveries

The row predates existing keyspace helpers and DXF status value types. The production `DomainSqlServer::acquire_ks_runtime` still rejects every next-generation target. Its current runtime reports global configuration rather than store identity. `pkg/domain/crossks` now includes submit-only DDL, virtual server registration, schema validation and idle eviction. Those are necessary for package completion.

The Rust extstore is a concrete local-file implementation, not the Go `storeapi.Storage` interface. Only `tidb-objstore-recording` exists, so cloud storage requires an object-storage owner as well as changing the branch in `createGlobalExtStorage`.

## Decision Log

- Follow the user's explicit six-worker build settings over the root twelve-worker default. Never delete shared build artifacts. Date: 2026-11-14.
- Preserve Go and other worktrees. No push. The initial working tree was clean. Date: 2026-11-14.
- Do not remove O15 on the strength of value helpers or mock-only runtime tests. All production consumers and lifecycle dependencies must work before closure. Date: 2026-11-14.

## Context and Orientation

Go owners: `pkg/keyspace`, `pkg/domain/crossks`, `pkg/domain/domain.go`, `pkg/store`, `pkg/dxf/framework/handle/status.go`, `pkg/dxf/framework/metering`, `pkg/planner/extstore`. Rust owners: `tidb-keyspace`, `tidb-domain-core/src/domain/sqlsvr.rs`, `tidb-store-driver-txn`, `tidb-dxf-handle`, `tidb-dxf-metering`, `tidb-planner-extstore`. The vendored TiKV client already supports named keyspaces; inspect its public codec/metadata surface before adding adapters. Do not hand-edit generated protocol files.

## Plan of Work

First preserve the complete keyspace helper behavior and pin it with the three named Go tests. Trace the actual store's codec and metadata through construction, domain, resource groups and etcd namespace. Then implement Go crossks ownership with the existing SQL runtime/session abstractions, preserving failed-creation cleanup, held-runtime eviction exclusion and SYSTEM store ownership. Add the Go integration cases before claiming runtime success. Finish named status, metering and external-storage consumers using their actual dependencies, not fabricated successful stubs.

## Concrete Steps

Run from `rust/` with the environment prefix:

    CARGO_CACHE_AUTO_CLEAN_FREQUENCY=never CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0

Use `cargo check -j6 -p <crate> --tests`, then `cargo nextest run -j6 -p <crate> -E 'test(<name>)'`. Run `cargo nextest run -j6 -p <changed-crate>` once at the end. Use a nextgen feature build to cover the compile-time kernel branch. Record exact executed commands and results below. Stage only authorized Rust changes, commit on the current branch and do not push.

## Validation and Acceptance

Tests must use Go test names in snake case and match source assertions. Demonstrate a failure without each behavioral fix. Keyspace routing needs actual separate namespaces, not a string field; crossks tests must cover lifecycle and schema synchronization. Cloud storage needs read/write behavior and credential redaction. Keep O15 partial until every named surface is verified. Local tests do not establish a live deployment.

## Idempotence and Recovery

Checks and targeted tests can be rerun. Preserve unrelated changes and all shared targets. Failed initialization must release only resources it owns. Never change branches or modify Go source. Keep throwaway Go probes under untracked `zzprobe/` and never stage them.

## Artifacts and Notes

Initial evidence: `DomainSqlServer::acquire_ks_runtime` ends with an unsupported error; `create_global_ext_storage` immediately rejects nextgen; metering explicitly omits `Meter`.

## Interfaces and Dependencies

Use the actual TiKV client's keyspace codec, `tidb-keyspace::KeyspaceCodec`, `tidb-domain-sqlsvrapi::{Runtime,KsRuntimeHandle,Server}`, and existing schema/session owners. Identify missing dependency behavior explicitly instead of inventing a substitute API.

## Outcomes & Retrospective

Keyspace helper regression fixed. Classic and nextgen each passed all 10 tidb-keyspace tests. Etcd namespace support passed 20 enabled pd-client suite tests and its separate real-etcd test. Domain-core baseline check passed. O15 is partial, not closed. Requested commits failed at the Git index permission boundary. Full evidence and exact commands are in `rust/docs/parity/current-audit/o15-verification.md`.

Evidence: `go test --tags=intest -p 6 -run TestCodecV1Identity -v ./zzprobe/o15_keyspace` reported `api=V1 keyspace_id=4294967295 namespace="" namespace_slash=""`. The Rust targeted regression failed before the fix (left 0, right 4294967295). `cargo check -j6 --manifest-path rust/Cargo.toml -p tidb-keyspace --tests` and both `cargo nextest run -j6 --manifest-path rust/Cargo.toml -p tidb-keyspace` variants (classic; `--features tidb-config-kerneltype/nextgen`) passed with the required environment. `cargo check -j6 --manifest-path rust/Cargo.toml -p tidb-domain-core --tests` also passed on unchanged domain sources. An attempted redundant `--test-threads 6` option was rejected by nextest because `-j6` already supplies it; reruns used only `-j6`.

Update: namespace operations and real-etcd verification completed; production domain/crossks and the named consumers remain unimplemented. No successful commit exists because `/Users/qiliu/projects/tidb/.git/worktrees/cx-o15/index.lock` cannot be created under the supplied sandbox.
