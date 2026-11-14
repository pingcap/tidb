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
- [x] Previous pass committed (confirmed by the current clean tracked worktree). The continuation was supplied explicit index write access, but actual Git writes still fail (see below).
- [x] Port handle ownership, creation serialization, explicit release, idle eviction and cancellable GC; all four Go internal tests and target validation pass in nextgen. The concrete session manager initializer is still missing.
- [x] Wire configured TiKV keyspace selection, store-derived identity and etcd namespaces, resource-group keyspace ID, assumed server metadata and minimum-start-TS GC scope. Component tests pass; no live named-keyspace node acceptance run yet.
- [x] Port Meter recorder lifecycle, deltas, retries and flush loops over the SDK writer interface; six Go behavioral tests pass. SDK provider construction and readback remain.
- [x] Run the four component suites in nextgen: 205 passed, one skipped. Combined checks pass, and the session identity regression passes and fails with the old accessor.
- [x] Complete the full classic suites: 5,054 passed, nine failed, eight skipped. Baseline comparison was stopped during compilation as disk space fell below 1 GiB; the nine failures remain unclassified. The added resource-group regression passed cargo check but its runtime and negative runs remain pending.
- [ ] Commit this continuation: blocked by actual index.lock permission denial, despite the configured writable root.

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

## Previous Pass Outcome

Keyspace helper regression fixed. Classic and nextgen each passed all 10 tidb-keyspace tests. Etcd namespace support passed 20 enabled pd-client suite tests and its separate real-etcd test. Domain-core baseline check passed. O15 is partial, not closed. Requested commits failed at the Git index permission boundary. Full evidence and exact commands are in `rust/docs/parity/current-audit/o15-verification.md`.

Evidence: `go test --tags=intest -p 6 -run TestCodecV1Identity -v ./zzprobe/o15_keyspace` reported `api=V1 keyspace_id=4294967295 namespace="" namespace_slash=""`. The Rust targeted regression failed before the fix (left 0, right 4294967295). `cargo check -j6 --manifest-path rust/Cargo.toml -p tidb-keyspace --tests` and both `cargo nextest run -j6 --manifest-path rust/Cargo.toml -p tidb-keyspace` variants (classic; `--features tidb-config-kerneltype/nextgen`) passed with the required environment. `cargo check -j6 --manifest-path rust/Cargo.toml -p tidb-domain-core --tests` also passed on unchanged domain sources. An attempted redundant `--test-threads 6` option was rejected by nextest because `-j6` already supplies it; reruns used only `-j6`.

Update: namespace operations and real-etcd verification completed; production domain/crossks and the named consumers remain unimplemented. No successful commit exists because `/Users/qiliu/projects/tidb/.git/worktrees/cx-o15/index.lock` cannot be created under the supplied sandbox.

## Continuation, 2026-11-14

The tracked worktree starts clean; untracked `zzprobe/` is preserved. The user explicitly authorizes commits and prohibits pushes and artifact deletion. The runtime manager will retain Go's creation-under-lock, explicit once-only release, idle deadline and close-outside-lock semantics. Rust resource traits adapt Go interface fields; a tested manager alone does not close O15. Production target initialization must own the session pool, schema sync, virtual server registration and submit-only DDL.

## Continuation Decisions and Evidence

The Rust `Manager` owns handles and eviction over a `SessionManager` resource-owner interface. That interface is an integration boundary, not an implemented production target runtime: `DomainSqlServer::acquire_ks_runtime` still refuses unsupported target acquisition. Do not treat passing internal manager tests as completing `pkg/domain/crossks`. Its remaining Go files require the actual target session factory, schema coordinator and validator, virtual server registration/cleanup, schema and MDL loops, min-job refresher and submit-only DDL client.

The production TiKV startup now supplies `Config.with_keyspace` for a nonempty configured name and derives its etcd namespace from the transaction client's actual codec ID. Existing client accessors were exposed instead of retaining a second copy of the selected codec. The domain stores the opened store's name and retains an unprefixed etcd handle. Resource-group admission uses that codec ID. Minimum-start-TS reporting deliberately uses the unprefixed handle for unified GC, and the scoped handle only when the metadata selects keyspace-level GC. This follows `InfoSyncer.getEtcdClientForMinStartTS` and avoids moving unified GC records into a tenant namespace.

Go probes under untracked `zzprobe/o15_store` captured null/SYSTEM/tenant/DEFAULT identities and the five GC-metadata cases. The Rust tests use the native client's mocked PD metadata and exercise real store construction and closing, not just a formatted namespace string. They do not exercise a running TiKV cluster or a SQL session in two keyspaces.

`tidb-dxf-metering` now implements recorder reuse and unregister cleanup, delta scraping, immutable timestamped retry batches, the ten-attempt limit, failure metrics, a child write timeout and cancellation/final-flush loops. `MeteringWriter` is the exact Write/Close boundary used from the external Go SDK. No storage provider constructor, configured production Meter installation, or real object-store readback is claimed. The Go synctest one-hour schedule is driven deterministically in Rust and the real loop cancellation is exercised separately.

Regression sensitivity: restoring the previous unsupported acquisition response makes `test_acquire_runtime_handle` and `test_evict_runtime` fail. The implementation was restored before acceptance runs. All 15 selected component tests passed in a nextgen build. The native TiKV dependency's own test target fails with unchanged HEAD sources (missing test dependencies and pre-existing test macro errors); the narrow exposed client accessors are exercised from the TiDB store tests instead.

## Outcomes & Retrospective

O15 remains partial. Internal work still outstanding: concrete cross-keyspace runtime initialization and all domain integration tests, submit-only DDL, HTTP `/dxf/*` and `/ddl/check/*`, `GetScheduleStatus`, production Meter setup, and nextgen plan-replayer external storage. The missing storage-provider/SDK adapters are external-dependency work; the other gaps are internal and must not be described as external blockers. Validation and commits are recorded in `o15-verification.md` as they complete.

## Delivery blocker observed in this continuation

`git add rust/Cargo.toml rust/crates/tidb-domain-crossks`, the index-only Cargo.lock patch, and `git commit -m 'Port Go crossks runtime handle ownership and eviction'` all fail with:

```text
fatal: Unable to create '/Users/qiliu/projects/tidb/.git/worktrees/cx-o15/index.lock': Operation not permitted
```

The supplied permissions declare this Git directory writable, but the actual command is denied. No commit or push was created. The worktree contains all changes; no alternative index, branch, or worktree was used. This is a delivery blocker, not evidence that the remaining internal implementation gaps are external.

## Remaining production integration boundaries

- Go `pkg/session/session.go:createSessionWithOpt` explicitly allows a nil domain, sets `crossKS`, and installs the target store, schema validator and info cache without a DDL owner. Rust `cluster_session_node::ClusterStatsSessionContext` currently obtains those services through a required `Domain`. Calling the ordinary cluster startup for a target would start owning services and bootstrap paths that Go crossks does not start. Port the domain-free constructor before installing a successful Domain acquisition path.
- Go `crossks.createSessionManager` creates a five-session pool, coordinator, validator, scoped etcd client, virtual server registration, schema-version syncer and initial info reload; it seeds server state with `GetGlobalState`, without starting its watch. It starts server-info, schema, MDL and min-job refresh loops. Failure cleanup and ordinary close differ; SYSTEM store lifetime remains external to the manager. Rust `issyncer::catalog_watch::Syncer` currently owns a reload thread but does not expose Go's complete crossks constructor/coordination surface.
- Go `crossks.ddlClient` resolves current database/table metadata transactionally, verifies names, builds ALTER TABLE MODE, refreshes server state before `SubmitBatch`, notifies the owner and polls history every 100 ms. Reusing a target DDL owner would be wrong.
- Go `/dxf/*` registration is conditional on `kv.IsSystemKS(store)`, whereas `/ddl/check/*` registration is conditional on the nextgen kernel. The latter forces fast checking; execution errors produce a JSON `failed` result with HTTP 200, but record-set collection errors use `WriteError`. Preserve that distinction instead of wrapping every SQL error as HTTP 200.
- Go extstore falls back to local storage for classic OR empty cloud URI and applies the keyspace namespace even to that local path. A nextgen blanket error and a cloud-URI branch over Rust's concrete `LocalStorage` both fail to implement the package. Finish the storage interface/provider dependency and credential-safe errors before claiming parity.

Further source confirmation: `pkg/infoschema/issyncer/loader.go` has a separate cross-keyspace loader, omits user-table dependencies, restricts full loads to system databases, and filters schema diffs to system tables. `syncer.go` also has crossKS-specific reload handling. These are part of the target runtime's isolation boundary; loading the normal complete catalog and merely setting a keyspace name is insufficient. Go `session.GetSQLServer` resolves the session's domain; do not substitute the serving domain into a cross-keyspace session simply to satisfy Rust's current required `sql_server()` method.

## Final validation interruption

The baseline comparison was interrupted deliberately when free space fell to 538 MiB (197 MiB on the final check). No shared target artifacts were deleted. The runner restored all 18 temporarily replaced source/manifest/lock files; a separate byte comparison confirmed zero mismatches against the pre-baseline backups. The shell service temporarily disconnected, then recovered before this restoration was verified. The full classic run remains 5,054 passed, nine failed, eight skipped; baseline attribution is not established. The final resource-group test has passed cargo check and its Go capture, but its Rust runtime/sensitivity checks remain pending. Further builds and remaining production work require disk capacity; committing additionally requires fixing the actual Git index permission denial. O15 remains partial, including internal implementation gaps.
