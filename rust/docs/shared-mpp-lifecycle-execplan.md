# Put MPP on the process store connection fleet

This living ExecPlan follows root PLANS.md. Work is in /workspace/tidb hparser-integration from 42a007cf4c0a11ec4dcf5d606dcb3671dd1f8ad0. Incoming e52af04b8c965c7bae2b35e91445c78261885dc0 (schema acknowledgement reconciliation) is merged without an intermediate commit and will be preserved as the second parent of the final hook-gated batch commit. Go master remains 93a01d31f6da205ae4bf376825293903a6899fdb. Native /workspace/client-rust master remains 19a56ccda1e128218cd33c69709038219aced9bc. No pushes or dry runs are authorized.

## Purpose / Big Picture


MPP and ordinary store RPCs must share actual connection ownership, not only endpoint construction. Repeated MPP queries should reuse the configured process fleet; address retirement and process shutdown must end its old streams and reject new ones. Cluster TLS from NodeConfig must reach the single PD/store authority. This is connected existing-owner maintenance of M04/N03/T02, not acceptance of complete Go copr/client/discovery packages.

## Progress


- [x] Read root instructions, refreshed all requested branches, and preserved the nonoverlapping incoming schema repair in an uncommitted merge.
- [x] Extend existing real-socket MPP fixture with process reuse/close/address/TLS regressions, grouped into one baseline target.
- [x] Record runtime baseline failures; exclude compilation or fixture failures.
- [x] Route MPP through the existing worker-owned fleet; wire immutable security at production bootstrap and remove private MPP runtime/channel construction.
- [x] Run combined affected regressions and incoming schema tests, all-target checks, lint and locked server build; self-review.
- [ ] Update both registers/receipts, commit once through the actual hook, and refresh recovery/startup. No push.

## Context and Orientation


rust/crates/tidb-txnkv/src/rpc/transport_runtime.rs owns a connection fleet with per-address round-robin selection and shared version allocation. channel_pool.rs retains each physical channel and its ConnectionTasks join scope; close_address_version protects replacements from delayed failures. TonicCoprocessorClient clones retain only request capability. rust/crates/tidb-exec/src/real_tikv_read.rs retains the unique process PD/cache/transport owners. tiflash_mpp_scan.rs currently constructs channels and a separate two-thread runtime. The previous repair shared policy but did not remove this lifecycle split. Production bootstrap also calls plaintext constructors despite NodeConfig already retaining cluster security.

## Milestones and Plan of Work


First demonstrate repeated socket creation and MPP escape from process/address closure, plus ordinary RPC TLS omission. Then add an opaque store-channel capability returned by the existing command worker's round-robin fleet. It retains channel generation and the existing task scope, with no new join authority or cache. Route MPP dispatch, stream establishment and cancellation through that capability; detect scope retirement during pending setup and receive. Reuse the existing global execution runtime rather than creating MPP threads. Introduce security-aware process constructors while retaining explicit plaintext defaults for programmatic compatibility, and pass NodeConfig's security Arc through the real startup path to both PD and store fleet. Finally validate all connected owners together and include incoming schemaver reconciliation tests.

## Concrete Steps


From /workspace/tidb/rust source /workspace/.cloud-setup/env.sh. Baseline: cargo test --locked -p tidb-exec --lib -- shared_mpp_ shared_store_ --test-threads=1. Final: combine tidb-exec, tidb-txnkv, tidb-pd-client and tidb-schemaver library targets with their MPP/transport/security/etcd_syncer filters in one Cargo invocation. Check the affected crates and tidb-server with --all-targets. From repository root run make lint. The actual hooks/pre-commit must run cd rust && cargo build --locked -p tidb-server for the single normal final commit. Do not bypass it or push.

## Validation and Acceptance


Two MPP opens with a one-connection process fixture must use one accepted socket. Process/address close must interrupt an already-open stalled stream before its delayed remote packet/error, and close must reject new MPP opens. A replacement stream after address retirement must work. Ordinary RPCs must succeed against the existing TLS server when the process receives the shared credentials. Preserve prior cancellation, packet limits, memory, region and natural-completion cases. Prove version-specific retirement does not damage a newer replacement. Full live multi-node, complete Go package and performance gates remain separate obligations.

## Idempotence and Recovery


Preserve the incoming merge and all existing local history. Tests use ephemeral loopback sockets and joined fixture shutdown. Keep baseline logs and the old verified recovery bundle until a replacement verifies. Check disk before builds; with no build active, remove only recorded obsolete outputs, preserving current binaries, sources, dependencies, fingerprints and logs. No reset, force push or hook bypass.

## Interfaces and Dependencies


TonicCoprocessorClient exposes a request-only store channel capability obtained from TransportHandle via WorkerCommand. The existing worker chooses the channel and owns its tasks. ConnectionTasks' existing closed state supplies retirement detection. ProductionReadProcessAuthority exposes a borrowed transport opener, and its security-aware constructor initializes PD and store transport from the same immutable Arc. TiFlashMppScanSource retains only borrowed capabilities and a shared execution runtime handle. No new dependencies or independently maintained MPP pool/retry policy.

## Surprises & Discoveries


Fresh integration advanced to e52af04b8c only in etcd_syncer.rs; preserved without overlap. The previous TLS fixture validated the consumer, but actual production bootstrap still discarded NodeConfig's security and ordinary transport exposed only plaintext constructors. Historical receipts must not be interpreted as complete secure startup acceptance.

## Decision Log


Decision: migrate MPP to the existing fleet and repair the connected startup security handoff together. Rationale: shared policy cannot enforce process close or configuration while channels and bootstrap remain independent. Date: 2026-10-04.

## Outcomes & Retrospective


Five runtime failures establish the baseline; 58 distinct final cases, all-target checks, lint and locked server build pass. Self-review also repairs cleanup after post-dispatch channel acquisition failure. Compilation/fixture failures are excluded. Broad M04/N03/T02 stay partial. Counts remain 86 tracked, 29 repaired, 57 unresolved; other entries retain their previous evidence.
