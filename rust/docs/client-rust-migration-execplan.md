# Replace hand-rolled PD/TiKV client internals with vendored `client-rust`

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`,
`Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` (repository root) governs how this plan must be written
and maintained.

## Purpose / Big Picture

Go TiDB never reimplements the PD/TiKV client protocol: `go.mod` pins
`github.com/tikv/client-go/v2` and every SQL-layer package (`pkg/store/driver`,
`pkg/store/copr`, `pkg/kv`) calls into it. The Rust rewrite under `rust/`
currently does the opposite: `crates/tidb-pd-client` (8.6K lines),
`crates/tidb-txnkv` (66K lines), and `crates/tidb-distsql` (24K lines)
hand-transcreate PD member discovery, TSO, region routing, RPC transport, 2PC,
lock resolution, and coprocessor dispatch from client-go's Go source, instead
of depending on a Rust client library the way Go TiDB depends on client-go.

`github.com/ngaut/client-rust` (a fork of `tikv/client-rust`, the official Rust
TiKV client) is a Rust implementation of the same PD/TiKV wire protocols
(`pdpb`, `kvrpcpb`, transaction 2PC, raw KV). This plan vendors it into the
workspace and rewires `tidb-pd-client`/`tidb-txnkv`/`tidb-distsql` to depend on
it for everything that duplicates client-go functionality, deleting the
duplicate hand-rolled implementation. What is genuinely TiDB-SQL-layer-specific
(the `pkg/kv` contract types, the `pkg/distsql`/`pkg/store/copr` DAG/paging
protocol — neither of which exists in client-go either, since Go TiDB owns
those itself) stays, because there is nothing upstream to replace it with.

After this plan, a reader can observe: `rust/Cargo.toml` lists a
`tikv-client-rs` path dependency (vendored, patched) instead of `tidb-pd-client`
implementing its own PD gRPC calls; `crates/tidb-pd-client`,
`crates/tidb-txnkv`, and `crates/tidb-distsql` are each a small fraction of
their current size, containing only TiDB-specific contract/dispatch code plus
thin adapters onto the vendored crate; and the full existing Rust test suite
(`cargo test` targets under these three crates plus their consumers:
`tidb-distsql`, `tidb-exec`, `tidb-executor`, `tidb-server`, `tidb-stats`,
`tidb-tablecodec`, `tidb-ttl`, `tidb-unistore`) passes.

## Progress

- [x] (2026-08-24) Scoping conversation with user: confirmed full swap,
      confirmed crates keep their documented names/boundaries (gut internals,
      don't rename/split), confirmed the already-parity-verified 2PC engine
      and coprocessor encoding are in scope too (accepting the risk, porting
      forward the documented fixes).
- [x] (2026-08-24) Research: mapped `tidb-pd-client`, `tidb-txnkv`,
      `tidb-distsql` public APIs and consumers against `ngaut/client-rust`'s
      `pd`, `oracle`, `kv`, `transaction`, `raw`, `store`, `request`, `locate`,
      `region`, `region_cache`, `retry`, `backoff` modules. See
      `Surprises & Discoveries` below for the findings that shape every later
      phase.
- [ ] Phase 0: vendor and patch `ngaut/client-rust` into the workspace.
- [ ] Phase 1: replace `tidb-pd-client` internals.
- [ ] Phase 2: replace `tidb-txnkv` internals (region cache, RPC transport,
      2PC/lock-resolver engine, raw KV), porting the documented parity fixes
      from `rust/docs/two-phase-commit-vs-client-go.md`.
- [ ] Phase 3: rewire `tidb-distsql`'s RPC/region-retry layer onto the vendored
      crate's `request::Plan`/`Shardable`/retry framework, porting the
      documented parity fixes from `rust/docs/distsql-coprocessor-parity.md`.
- [ ] Phase 4: full-workspace build, targeted + aggregate test pass, `make
      bazel_prepare` (Go-file-count is unaffected, but Bazel metadata for the
      Rust crates does not apply — confirm scope; see `Concrete Steps`),
      `make lint`, update `rust/README.md` and
      `rust/docs/architecture/workspace.md` crate-size/description tables,
      final commit.

## Surprises & Discoveries

- Observation: `ngaut/client-rust`'s `region.rs`, `region_cache.rs`, and
  `locate.rs` are private (`mod`, not `pub mod`) in `src/lib.rs`, and its
  `mock.rs`/`store/mockserver.rs` in-process backend is gated
  `#[cfg(test)]`-only. Go's client-go deliberately exposes `RegionCache`,
  `RegionRequestSender`, and a mock/unistore backend as public API *because it
  was built to serve TiDB*; `client-rust` was not, so these must be patched to
  `pub` (and the mock backend defeatured out of `#[cfg(test)]`) in our vendored
  copy rather than assumed available.
  Evidence: research agent report on `tidb-txnkv`, section 4; `client-rust
  src/lib.rs` `mod region;` / `mod region_cache;` / `mod locate;` declarations.
- Observation: workspace pins `tonic = "0.14"`, `prost = "0.14"`, `tonic-prost`
  (`rust/Cargo.toml`); `client-rust` pins `tonic = "0.12"`, `prost = "0.13"`,
  no `tonic-prost`. These are semver-incompatible majors — Cargo cannot unify
  them, and `tidb-proto`'s generated `pdpb`/`kvrpcpb` types (built against
  0.14) will not satisfy `client-rust`'s trait bounds (built against 0.13)
  without patching `client-rust`'s own `Cargo.toml`.
  Evidence: research agent report on `tidb-pd-client`, section 5 (dependency
  concerns).
- Observation: `kvproto` pin coincidentally matches at the moment of writing
  (`client-rust/proto/VERSION`: `b41e86365ce0e0fba482005c0dfa5e2ca967d74c`,
  2026-06-22; `tidb-proto/proto/pdpb.proto` diffed byte-identical for
  `GetGCStateResponse`/`GetGCState`). There is no automated mechanism keeping
  them in sync — record this as a maintenance risk, not a blocker.
  Evidence: research agent report on `tidb-pd-client`, section 5.
- Observation: `tidb-pd-client`'s `EtcdClient`/`EtcdWatcher` (etcd KV/lease/
  watch on the PD port, used for DDL schema-version notification and
  sysvar/privilege-update notification) has **no** client-rust equivalent —
  and it should not, because Go TiDB does not get this from client-go either:
  `go.mod` depends directly on `go.etcd.io/etcd/client/v3` (confirmed:
  `go.mod:133-137`). The Rust replacement is the `etcd-client` crate (the
  standard async Rust etcd v3 client), added as a direct workspace dependency,
  matching Go's own dependency shape — not a client-rust gap to route around.
  Evidence: `go.mod:133-137`; research agent report on `tidb-pd-client`,
  section 4 (`EtcdClient`/`EtcdWatcher` row).
- Observation: `scan_regions`/`batch_scan_regions` and the txn-safepoint,
  keyspace-scoped `GetGCState` RPC (as opposed to the legacy
  `UpdateGCSafePoint`) are absent from client-rust's `pd::cluster::Cluster`
  and `pd::PdClient` trait, even though the RPCs exist in the vendored
  `pdpb.proto`. These must be added to the vendored copy (thin additions
  calling the existing generated `pdpb` client stub, following the pattern of
  neighboring methods in `pd/cluster.rs`) rather than reimplemented from
  scratch in `tidb-pd-client`.
  Evidence: research agent report on `tidb-pd-client`, section 4.
- Observation: client-rust's `transaction::Client`/`Transaction` only expose
  high-level 2PC outcomes; no path returning an "undetermined" verdict for a
  primary-batch commit whose RPC failed was found reaching a public API. Our
  existing `tidb-txnkv/src/transaction/coordinator/commit.rs` reproduces
  client-go's `commit.go:127-180`/`2pc.go:2062-2069` undetermined-result state
  machine faithfully (see `rust/docs/two-phase-commit-vs-client-go.md`,
  section "0. The undetermined-commit answer, first"), including suppressing
  post-failure cleanup and marking the transaction `Ambiguous`. This signal
  must be added to the vendored client-rust coordinator (patch, following the
  existing client-go-vs-ours comparison as the spec) before `tidb-exec`'s
  connection-close-on-undetermined behavior can be preserved. Losing it would
  be a silent regression: a network blip on a primary commit would report an
  ordinary SQL error instead of closing the connection, which for a
  distributed database can turn a merely-unknown outcome into an
  application-visible correctness bug (retrying an already-committed write).
  Evidence: `rust/docs/two-phase-commit-vs-client-go.md` lines 13-40; research
  agent report on `tidb-txnkv`, section 4/6.
- Observation: client-rust has **no** coprocessor DAG/paging/MPP/chunk-decode
  layer at all — no `tipb` dependency, no DAG builder, and the one
  `coprocessor::Request` `KvRequest` impl that exists has no `Shardable` impl
  (only the raw-KV coprocessor plugin path does). This mirrors Go: `pkg/distsql`
  and `pkg/store/copr` are TiDB-owned packages built atop client-go's generic
  `RegionCache`/`RegionRequestSender`, not part of client-go itself. So
  "replacing" the coprocessor layer with client-rust cannot mean deleting
  `tidb-distsql`'s DAG/paging/chunk-decode logic (nothing upstream does that
  job); it means rebuilding `tidb-distsql`'s hand-rolled RPC-transport/
  region-retry machinery (`cop_paging/direct_unary*`, `RegionRetryWaiter`) on
  top of client-rust's generic `request::Plan`/`Shardable`/retry/backoff
  framework and `store::client`/`store::command` transport, the same relation
  Go's `pkg/store/copr` has to client-go's sender.
  Evidence: research agent report on `tidb-distsql`, sections 4-6;
  `rust/docs/distsql-coprocessor-parity.md`.
- Observation: `rust/docs/architecture/workspace.md` already documents
  `tidb-txnkv` as the single, deliberate authority for "PD/region/TiKV
  transport and transaction primitives" spanning `pkg/kv`, `pkg/store`,
  client-go, and pd-client together — a documented simplification versus Go's
  own `pkg/kv`/`pkg/store/driver`/client-go layering ("duplicate authorities
  are removed when a complete package takes ownership"). This plan keeps that
  documented boundary: crate names and dependency shape stay as-is; only the
  ~98K lines of hand-rolled client-go-equivalent internals inside them are
  replaced.
  Evidence: `rust/docs/architecture/workspace.md`, "Crate responsibilities"
  table and "Dependency direction" section.

## Decision Log

- Decision: keep `tidb-pd-client`, `tidb-txnkv`, `tidb-distsql` as crate
  names/boundaries; do not split into a Go-shaped `pkg/kv`/`pkg/store/driver`
  layout. "Remove the crates to keep the project clean" is satisfied by
  deleting the ~98K lines of duplicate client-go-equivalent implementation
  inside them, not by renaming or splitting.
  Rationale: `rust/docs/architecture/workspace.md` already documents this as
  the intended, deliberate architecture for the Rust rewrite (single
  authority, not Go's multi-package indirection). Renaming/splitting would
  contradict checked-in architecture docs without a corresponding technical
  reason.
  Date/Author: 2026-08-24, this plan.
- Decision: vendor `ngaut/client-rust` as a re-syncable git checkout under
  `rust/third_party/tikv-client-rs/` (full clone, `.git` kept) plus a maintained
  patch set under `rust/third_party/patches/tikv-client-rs/*.patch`, applied by a
  `rust/scripts/sync-tikv-client-rs.sh` script, rather than a static
  copy-and-hand-edit snapshot. Three patch categories are needed before it can
  serve as this workspace's dependency: (1) `Cargo.toml` version bump
  `tonic 0.12→0.14`, `prost 0.13→0.14`, adopt `tonic-prost`, so its generated
  types unify with `tidb-proto`'s; (2) visibility patches (`mod region`→
  `pub mod region`, same for `region_cache`, `locate`; lift `#[cfg(test)]` off
  the mock backend behind a new `mock` feature) so `tidb-txnkv`/`tidb-distsql`
  can build on its region/RPC/mock layers the way they currently build on
  their own; (3) behavioral additions (`scan_regions`/`batch_scan_regions`,
  keyspace-scoped `GetGCState`, undetermined-commit signal) where client-go/
  our-existing-code has proven behavior client-rust doesn't expose yet.
  Rationale: the user has stated `ngaut/client-rust` master is being actively
  updated by another agent and this workspace must always track the latest
  version, not a frozen commit. A one-time copy-and-hand-edit snapshot would
  silently drift from upstream and could not be re-synced without redoing the
  diffing by hand; it would also collide with upstream fixes landing for the
  exact gaps this plan identified (private modules, missing RPCs, the
  undetermined-commit signal) since there would be no way to tell "still
  needed" from "upstream already did this" apart. A patch-set-over-a-real-git-
  checkout keeps `git diff`/`git log` meaningful against upstream, lets
  `sync-tikv-client-rs.sh` re-fetch master on demand, and makes each patch's
  fate explicit: `git apply` succeeds (patch still needed, unchanged),
  succeeds with fuzz/conflict (patch needs updating — upstream touched the
  same area), or fails because upstream already added equivalent behavior (in
  which case the patch is deleted and the caller code re-checked against
  upstream's actual shape, not assumed identical).
  Date/Author: 2026-08-24, this plan; revised same day after user specified
  master tracks another agent's ongoing updates.
- Decision: `tidb-pd-client`'s `EtcdClient`/`EtcdWatcher` are replaced by a
  direct `etcd-client` crate dependency, not routed through client-rust.
  Rationale: matches Go TiDB's own dependency shape exactly (`go.mod` depends
  on `go.etcd.io/etcd/client/v3` directly, not through client-go) — this is
  not a client-rust gap, it is the correct architecture.
  Date/Author: 2026-08-24, this plan.
- Decision: the already-parity-verified 2PC/lock-resolver engine
  (`tidb-txnkv/src/transaction/**`) and coprocessor request/response encoding
  (`tidb-distsql`) are in scope for replacement, not exempted. The documented
  parity fixes in `rust/docs/two-phase-commit-vs-client-go.md` (undetermined-
  commit/connection-close, and any other fixes recorded there) and
  `rust/docs/distsql-coprocessor-parity.md` (pushdown-flags-from-StmtContext,
  warning-channel routing, and any other fixes recorded there) must be ported
  into the new client-rust-backed code paths as part of Phase 2/Phase 3, not
  silently dropped. Each ported fix gets a `Surprises & Discoveries` entry
  when carried forward, and Phase 2/3 acceptance criteria require re-deriving
  the specific scenarios those two documents describe (not just "tests pass")
  since both documents note their findings were from source reading /
  real-cluster observation, not automated tests alone.
  Rationale: explicit user decision, made after the risk was raised
  (regressing already-verified correctness). Accepted with the mitigation of
  explicitly porting forward every documented fix rather than starting from
  client-rust's un-audited behavior.
  Date/Author: 2026-08-24, user decision via this session.

## Outcomes & Retrospective

(To be filled in as phases complete. Empty at plan creation.)

- Decision: proceed with the migration now, against whatever state
  `ngaut/client-rust` master is in, rather than gating each crate's migration
  on its corresponding upstream ledger row reaching `complete`.
  Rationale: `third_party/tikv-client-rs/doc/client-go-parity-ledger.md`
  (discovered during Phase 0 vendoring) shows `txnkv/transaction`,
  `txnkv/txnlock`, `internal/locate`, and `internal/client` — exactly the
  packages this plan's Phase 2/3 depend on most — marked `unassessed`/`seed`/
  `in-progress`, not `complete`. Explicit user decision, made after this risk
  was raised: proceed anyway, re-verifying/porting the documented
  `tidb-txnkv`/`tidb-distsql` parity fixes ourselves per the existing Decision
  Log entry on that topic, rather than blocking this plan's progress on a
  concurrently-moving upstream ledger.
  Date/Author: 2026-08-24, user decision via this session.

## Context and Orientation

**Repository layout.** `/home/user/tidb` is Go TiDB; `/home/user/tidb/rust` is
a separate Cargo workspace (`rust/Cargo.toml`) implementing a parity Rust SQL
node, never linked into the Go binary. `docs/design/2026-07-11-tidb-rust-rewrite.md`
(repository root) is the overall Rust-rewrite strategy; `rust/docs/architecture/workspace.md`
is the current crate-boundary authority; both stay accurate through this plan.

**The three crates in scope**, all under `rust/crates/`:

- `tidb-pd-client` (`rust/crates/tidb-pd-client/src/`): `client.rs` (PD gRPC:
  member discovery/failover, TSO, region/store lookup, GC-state), `etcd.rs`
  (etcd KV/lease/watch on the PD port), `model.rs` (wire-shaped structs),
  `security.rs` (TLS), `engine.rs` (TiFlash label helpers), `error.rs`.
- `tidb-txnkv` (`rust/crates/tidb-txnkv/src/`, ~45 files plus `driver/`,
  `lock/`, `region/`, `rpc/`, `transaction/` subdirectories): owns both the
  TiDB-specific KV contract types (`handle.rs`, `kv_api.rs`, `assertion.rs`,
  `checker.rs`, `fault_injection.rs`, `mvcc_metadata.rs`, `resource_group.rs`,
  `txn_source.rs`, `mpp.rs`, `key.rs`/`key_flags.rs`/`key_ranges.rs`,
  `unistore.rs`/`mem_storage.rs` in-process mock) and the client-go-equivalent
  protocol implementation (`region/` cache and routing, `rpc/` transport and
  channel pooling, `transaction/` 2PC coordinator, `lock/` resolver,
  `pd_loader.rs`/`pd_capability.rs`, `gc_state.rs`, `retry.rs`).
- `tidb-distsql` (`rust/crates/tidb-distsql/src/`): coprocessor request
  building (`request_builder.rs`, `kv_request.rs`, `coprocessor_request.rs`),
  region-aware task splitting (`region_task.rs`, `region_location.rs`),
  dispatch/retry (`cop_paging/` — `DirectUnaryQueryTransport`,
  `RegionRetryWaiter`, `OptimisticLockRecovery`), and response decoding
  (`chunk_decode.rs`, `stream_decode.rs`, `select_iter.rs`).

**Consumers** (grep counts from research, approximate, re-verify per phase):
`tidb-distsql` (uses `tidb-txnkv::{lock,region,rpc}` and `tidb-pd-client`
transitively), `tidb-exec` (heaviest `tidb-txnkv::transaction::*` consumer),
`tidb-executor` (`Key`/`Handle`/`MemBuffer`/`resource_group` types),
`tidb-server` (`tidb-txnkv::transaction::*`, `tidb-pd-client::{PdClient,
EtcdClient}`), `tidb-unistore` (`SharedReadAuthority`, `region::RegionCache`,
`rpc::{AsyncRequestDispatcher,PendingRequest}`), `tidb-stats`/
`tidb-tablecodec`/`tidb-ttl` (light: `Handle`/`Key` only).

**The vendored dependency.** `ngaut/client-rust`
(`https://github.com/ngaut/client-rust`, `master` branch, a fork of the
official `tikv/client-rust`) will be checked out into
`rust/third_party/tikv-client-rs/` and kept in sync with `origin/master` via
`rust/scripts/sync-tikv-client-rs.sh` (master is actively maintained by
another agent per the user; this workspace always builds against its latest
commit, not a frozen pin). Its crate name is `tikv-client` (`lib` name
`tikv_client`); this plan refers to it by that name once vendored, and by
"client-rust" generically when discussing the upstream project. Relevant modules: `src/pd/`
(`PdClient` trait, `PdRpcClient`, `cluster.rs`, `retry.rs` — PD gRPC),
`src/oracle/` (TSO abstraction), `src/transaction/` (`client.rs`,
`transaction.rs`, `snapshot.rs`, `unionstore.rs`/`art.rs`/`rbt.rs` — mutation
buffer, `lock.rs` — resolver), `src/raw/` (`RawClient`), `src/store/`
(`batch.rs`, `command.rs`, `mockserver.rs`), `src/request/` (`plan.rs`,
`plan_builder.rs`, `shard.rs` — generic region-sharded retry framework),
`src/region.rs`/`src/region_cache.rs`/`src/locate.rs` (currently private),
`src/backoff.rs`/`src/retry.rs`, `src/common/security.rs`, `src/config.rs`,
`src/mock.rs` (currently `#[cfg(test)]`-only).

## Plan of Work

### Phase 0 — vendor and patch `ngaut/client-rust`

`ngaut/client-rust` master is a moving target (another agent updates it
ongoing) — this workspace must always build against the latest master, not a
commit frozen at plan-writing time. Vendor it as a real git checkout under
`rust/third_party/tikv-client-rs/` (`.git` kept, `.gitignore`d from *this*
workspace's own tracked-file expectations is wrong — the checkout's contents
*are* tracked in the TiDB repo the normal way; only its own `.git` directory
is excluded, same as any vendored git checkout) plus a maintained patch set
under `rust/third_party/patches/tikv-client-rs/NNN-description.patch`
(numbered, applied in order), and a `rust/scripts/sync-tikv-client-rs.sh`
that:

1. `git -C rust/third_party/tikv-client-rs fetch origin master && git -C rust/third_party/tikv-client-rs reset --hard origin/master`
2. For each patch in `rust/third_party/patches/tikv-client-rs/` (sorted), run
   `git -C rust/third_party/tikv-client-rs apply --check <patch>`; if it applies
   cleanly, `git apply` it; if it fails, print the patch name and stop rather
   than silently skipping, so a human/agent triages whether upstream already
   absorbed the fix (delete the patch, re-verify the caller code against the
   new upstream shape) or the patch needs a rebase (upstream changed nearby
   code; update the patch).
3. Record the synced commit hash and sync date by appending to
   `rust/third_party/tikv-client-rs-SYNC-LOG.md` (new file), so every sync is an
   auditable, timestamped event rather than a silent floating pointer.

Run this script once at the start of Phase 0, and again at the start of every
later phase (Phase 1/2/3/4) before doing that phase's work, so each phase
starts from the actual current upstream state rather than whatever was latest
when this plan was written.

Author the three patch categories from the Decision Log as actual `.patch`
files (via `git diff` inside the vendored checkout after making the edit,
then `git checkout` the working tree back to clean before moving to the next
patch — or use `git format-patch` on a small local commit per patch), so they
survive being reapplied after a resync:

1. `Cargo.toml`: bump `tonic` to `"0.14"`, `prost` to `"0.14"`, add
   `tonic-prost` matching the workspace pin (`rust/Cargo.toml`
   `[workspace.dependencies]`), and fix any resulting codegen/call-site breaks
   in `src/generated/` build script or `proto-build/`.
2. Visibility: `src/lib.rs` — change `mod region;`, `mod region_cache;`,
   `mod locate;` to `pub mod`; add a `mock` Cargo feature that gates
   `mock.rs`/`store/mockserver.rs` instead of `#[cfg(test)]`, so
   `tidb-unistore`'s production-usable in-process backend need can depend on
   `tikv-client-rs` with `features = ["mock"]`.
3. Behavioral gaps, each following the pattern of a neighboring existing
   method in the same file: add `scan_regions`/`batch_scan_regions` to
   `pd::cluster::Cluster` (calling the already-generated `pdpb` client stub
   for `ScanRegions`/whatever the vendored `pdpb.proto` names the RPC — check
   the proto first), add the keyspace-scoped `GetGCState` RPC alongside the
   existing `update_safepoint`, and add an undetermined-commit signal to the
   transaction coordinator (`src/transaction/`) following exactly the state
   machine in `rust/docs/two-phase-commit-vs-client-go.md` section 0 (publish
   primary batch → RPC error/undetermined region error → mark txn
   `Undetermined`/suppress cleanup → surface to the caller of `commit()`,
   e.g. as a new `Transaction::commit` error variant `Undetermined`).

Add `rust/third_party/tikv-client-rs` to `rust/Cargo.toml`'s `[workspace.members]`
list, and a `tikv-client = { path = "third_party/tikv-client-rs" }` entry under
`[workspace.dependencies]`. Do not yet make any of the three target crates
depend on it — Phase 0's acceptance is that `cargo check -p tikv-client`
succeeds standalone inside the workspace.

### Phase 1 — replace `tidb-pd-client` internals

Add `tikv-client.workspace = true` and `etcd-client = "0.14"` (or latest
compatible; confirm current crates.io version at implementation time) to
`crates/tidb-pd-client/Cargo.toml`. Rewrite `client.rs` so `PdClient`'s public
methods (`connect*`, `get_region*`, `get_prev_region*`, `get_region_by_id*`,
`scan_regions`/`batch_scan_regions`, `get_store`, `get_timestamp*`,
`get_gc_state`, `refresh_members`, `member_set`, `active_endpoint`,
`shutdown`) become thin wrappers delegating to
`tikv_client::pd::{PdRpcClient, cluster::Cluster, retry::RetryClient}` and
`tikv_client::oracle::PdOracle`, keeping `tidb-pd-client`'s existing method
signatures and the `PdOperation`/`PdClientError` error taxonomy so consumers
(`tidb-txnkv`, `tidb-server`) do not need to change at this phase. Rewrite
`etcd.rs`'s `EtcdClient`/`EtcdWatcher` to wrap the `etcd-client` crate's
`Client`/watch APIs instead of hand-rolled etcd gRPC calls, keeping the same
public surface (`put*`, `lease_*`, `get*`, `delete*`, `put_global_schema_version`,
`notify_privilege_update`, `notify_sysvar_update`, `global_schema_version`,
`EtcdWatcher::spawn*`). Keep `model.rs` (wire-shaped structs consumers already
depend on), `engine.rs` (trivial TiFlash label helpers, nothing to replace),
and `security.rs` (rewrite to build a `tikv_client::common::security::SecurityManager`
internally while keeping the existing `ClusterSecurity`/`secure_endpoint` public
API).

Acceptance: `cargo test -p tidb-pd-client` passes unchanged (same test file
names/assertions as before this phase — the crate's own test suite is the
regression net for its public API staying stable), and every direct consumer
(`tidb-txnkv`, `tidb-server`) builds with zero call-site changes, since the
public API shape does not change in this phase — only its implementation.

### Phase 2 — replace `tidb-txnkv` internals

This is the largest phase. Work file group by file group, each independently
buildable and testable:

1. **Raw key/range/handle contract types** (`handle.rs`, `key.rs`,
   `key_flags.rs`, `key_ranges.rs`, `keyspace.rs`, `assertion.rs`, `checker.rs`,
   `fault_injection.rs`, `resource_group.rs`, `txn_source.rs`,
   `mvcc_metadata.rs`, `mpp.rs`, `variables.rs`, `version.rs`, `option.rs`,
   `trxevents.rs`, `tiflash.rs`, `farmhash.rs`, `go_is_print.rs`,
   `prefix_ops.rs`, `cache_db.rs`, `new_txn.rs`, `union_iter.rs`,
   `batch_getter.rs`, `counter.rs`): unchanged. Nothing in client-rust
   replaces TiDB-specific contract types; these stay exactly as they are.
2. **Region cache / routing** (`region/` directory): replace with thin
   wrappers over the now-`pub` `tikv_client::region_cache`/`locate` modules
   (Phase 0 patch), keeping `tidb-txnkv`'s existing `region::RegionCache`,
   `RegionBackoffKind`, `RouteOutcome`, `RouteFeedback` public types (used
   directly by `tidb-distsql`/`tidb-unistore`) as adapters.
3. **RPC transport** (`rpc/` directory including `rpc/batch/`): replace with
   thin wrappers over `tikv_client::store::{client, command}`, keeping
   `TonicCoprocessorClient`, `DirectUnaryClient`, `UnaryCallContext`,
   `AsyncRequestDispatcher`, `PendingRequest`, `CompletionError` as the stable
   consumer-facing types.
4. **Lock resolver** (`lock/` directory): replace `resolver.rs`/`pessimistic.rs`
   internals with `tikv_client::transaction::lock`/`Client::resolve_locks`/
   `scan_locks`/`cleanup_locks`, keeping `LockRecoveryClient`,
   `resolve_optimistic_locks`, `SnapshotLockSet` as the stable public API.
5. **2PC coordinator** (`transaction/` directory including `transaction/coordinator/`):
   replace with `tikv_client::transaction::{Client, Transaction}` (using the
   Phase 0 undetermined-commit patch), keeping the existing
   `RealOptimisticTransaction`, `RealOptimisticTransactionOpener`,
   `CommitProtocol`, `OptimisticMutation`, `CommittedTransaction`,
   `TransactionAttemptResult::Ambiguous` types stable for `tidb-exec`/
   `tidb-server` (the heaviest consumers of this module). Explicitly re-derive
   and record in `Surprises & Discoveries` whether the undetermined-commit
   connection-close scenario from `rust/docs/two-phase-commit-vs-client-go.md`
   section 0 still holds end to end (`tidb-exec` still sees `Ambiguous` and
   still closes the connection rather than reporting a false-negative SQL
   error).
6. **PD loader / GC state / read runtime** (`pd_loader.rs`, `pd_capability.rs`,
   `gc_state.rs`, `read_runtime.rs`, `retry.rs`): thin wrappers over
   `tidb-pd-client` (already migrated in Phase 1) plus `tikv_client::backoff`/
   `retry`.
7. **In-process mock backend** (`unistore.rs`, `mem_storage.rs`): replace with
   the Phase-0-defeatured `tikv_client` `mock`-feature backend where it covers
   the same scenarios; keep whatever `tidb-unistore`-specific behavior (e.g.
   `SharedReadAuthority`) client-rust's mock does not cover, as a thin wrapper
   rather than a full reimplementation.

Acceptance: `cargo test -p tidb-txnkv` (including the `lock_resolver_source`
and `kv` bench targets already declared in `crates/tidb-txnkv/Cargo.toml`)
passes; every consumer crate (`tidb-distsql`, `tidb-exec`, `tidb-executor`,
`tidb-server`, `tidb-stats`, `tidb-tablecodec`, `tidb-ttl`, `tidb-unistore`)
builds with call-site changes limited to what each file group's public API
intentionally changed (most should need none, per the "keep the stable type
names" instruction above).

### Phase 3 — rewire `tidb-distsql`'s transport layer

Replace `cop_paging/`'s hand-rolled `DirectUnaryQueryTransport`/
`RegionRetryWaiter`/transport-failure-classification with a thin layer over
`tikv_client::request::{Plan, PlanBuilder, Shardable}` for region-aware
splitting/retry, and `tikv_client::store::{client, command}` for the actual
RPC send (the same transport Phase 2 wired `tidb-txnkv/rpc` onto — reuse it
rather than duplicating). Keep `envelope.rs`, `request_builder.rs`,
`kv_request.rs`, `coprocessor_request.rs`, `region_task.rs`,
`chunk_decode.rs`, `stream_decode.rs`, `chblock.rs`, `distsql_runtime.rs`,
`copr_cache.rs`, `read_bytes_ema.rs` unchanged — these encode the DAG/paging
wire protocol and TiDB-specific caching/EMA behavior that has no client-rust
equivalent (matching Go's `pkg/distsql`/`pkg/store/copr` being hand-rolled
atop client-go's generic sender, not replaced by it).

Explicitly re-derive and record in `Surprises & Discoveries` whether the two
fixes in `rust/docs/distsql-coprocessor-parity.md` still hold after the
transport swap: `DAGRequest.flags` still sourced from
`StmtContext::push_down_flags()`, and lock/region-error/warning routing still
reaching the statement's own warning sink (`StmtContext::take_warnings`)
rather than a shared/wrong collector.

Acceptance: `cargo test -p tidb-distsql` passes; `tidb-exec`/`tidb-executor`/
`tidb-server`/`tidb-unistore` build with call-site changes limited to what
intentionally changed.

### Phase 4 — workspace-wide validation and doc updates

Run the full validation profile (see `Concrete Steps`), update
`rust/README.md`'s workspace table and `rust/docs/architecture/workspace.md`'s
"Crate responsibilities"/"Dependency direction" sections to mention the
vendored `tikv-client-rs` dependency and the shrunk crate sizes, and make the
final commit(s).

## Concrete Steps

All commands run from `/home/user/tidb/rust` unless stated otherwise.

Phase 0, first-time setup:

    mkdir -p third_party
    git clone https://github.com/ngaut/client-rust.git third_party/tikv-client-rs
    git -C third_party/tikv-client-rs rev-parse HEAD   # append to rust/third_party/tikv-client-rs-SYNC-LOG.md

Phase 0 and start of every later phase, resync:

    bash scripts/sync-tikv-client-rs.sh

(`scripts/sync-tikv-client-rs.sh` implements the fetch/reset/reapply-patches/
log-append sequence described above; write it before the first resync, commit
it once working.)

Edit `third_party/tikv-client-rs/Cargo.toml`, `third_party/tikv-client-rs/src/lib.rs`,
and the behavioral-gap files per the Decision Log; capture each edit as a
`.patch` file under `third_party/patches/tikv-client-rs/` (e.g.
`git -C third_party/tikv-client-rs diff > ../patches/tikv-client-rs/010-visibility.patch`
per logical change, then reset the working tree before the next one); add
`third_party/tikv-client-rs` to `rust/Cargo.toml` `[workspace.members]` and
`[workspace.dependencies]`. Then:

    cargo check -p tikv-client --locked

Expected: clean compile. If proto-generation breaks under the bumped
tonic/prost, the fix is in `third_party/tikv-client-rs/proto-build/` (its build
script), not a workaround in the workspace Cargo.toml.

Per-crate acceptance (Phases 1-3), run after each file group:

    cargo check -p tidb-pd-client --locked
    cargo test -p tidb-pd-client --locked
    cargo check -p tidb-txnkv --locked
    cargo test -p tidb-txnkv --locked
    cargo check -p tidb-distsql --locked
    cargo test -p tidb-distsql --locked

Full workspace (Phase 4):

    cargo check --workspace --locked
    cargo test --workspace --locked
    cargo clippy --workspace --locked   # if this is the project's lint gate; confirm against rust/docs/operations/validation.md at implementation time

Also consult `rust/docs/operations/validation.md` (referenced from
`rust/README.md`) for the exact WIP/Ready validation commands this workspace
expects, and follow AGENTS.md's `Ready` verification profile
(`.agents/skills/tidb-verify-profile`) before any final-status claim.

## Validation and Acceptance

Acceptance is behavioral, not just "compiles": for Phase 2/3, the specific
scenarios in `rust/docs/two-phase-commit-vs-client-go.md` (undetermined
commit) and `rust/docs/distsql-coprocessor-parity.md` (pushdown flags,
warning routing) must be re-checked by source reading against the new
client-rust-backed code paths (both documents were themselves produced by
source reading, not automated tests, per their own text — the safest
apples-to-apples verification is the same method), with the re-check recorded
in this plan's `Surprises & Discoveries`. Where a `RealTiKV` test exists for
the affected path (see `rust/difftests/transaction-tests`,
`docs/agents/testing-flow.md` → RealTiKV tests), run it — a mock-only pass is
not sufficient evidence for transaction-correctness-critical code per this
workspace's own stated policy ("mocks are focused tests, not release
evidence" — `rust/docs/architecture/workspace.md`, Runtime boundaries).

## Idempotence and Recovery

Each phase is committed independently once its acceptance criteria pass,
so a broken phase can be `git revert`ed without losing prior phases.
`scripts/sync-tikv-client-rs.sh` is idempotent (re-running it against an
already-synced checkout with all patches already applied is a no-op past step
1's `reset --hard`, since the working tree already matches
`origin/master` plus applied patches — the `git apply --check` in step 2 will
report "already applied" territory only if the script is re-run without first
resetting; the documented sequence always resets first, so this is safe to
re-run at the start of every phase without manual bookkeeping).
`rust/third_party/tikv-client-rs-SYNC-LOG.md` records every sync's commit hash and
date, so "what upstream commit was this phase built against" is always
answerable. If a resync breaks a previously-applied patch mid-phase, the fix
is either to update that one `.patch` file (upstream changed nearby code) or
delete it (upstream absorbed the fix) — never to pin the checkout back to an
older commit to dodge the conflict, since the whole point of this vendoring
strategy is staying current with the other agent's ongoing updates.

## Artifacts and Notes

(Populate with `cargo test` output snippets, patch diffs, and the
`Surprises & Discoveries` re-derivations as each phase completes.)

## Interfaces and Dependencies

- `rust/Cargo.toml` `[workspace.dependencies]`: add `tikv-client = { path =
  "third_party/tikv-client-rs" }`.
- `crates/tidb-pd-client::client::PdClient`, `crates/tidb-pd-client::etcd::{EtcdClient,
  EtcdWatcher}`: signatures stay stable across Phase 1; internals delegate to
  `tikv_client::pd::*`/`etcd_client::Client`.
- `crates/tidb-txnkv::region::RegionCache`, `crates/tidb-txnkv::rpc::{TonicCoprocessorClient,
  DirectUnaryClient}`, `crates/tidb-txnkv::lock::LockRecoveryClient`,
  `crates/tidb-txnkv::transaction::{RealOptimisticTransaction,
  RealOptimisticTransactionOpener, CommitProtocol}`: signatures stay stable
  across Phase 2; internals delegate to `tikv_client::{region_cache, locate,
  store, transaction}::*`.
- `crates/tidb-distsql::cop_paging::{DirectUnaryQueryTransport,
  RegionRetryWaiter}`: signatures stay stable across Phase 3; internals
  delegate to `tikv_client::request::{Plan, Shardable}`.

## Revision notes

- 2026-08-24: initial plan created after scoping conversation and research
  agent findings (three parallel `Explore` agents mapping `tidb-pd-client`,
  `tidb-txnkv`, `tidb-distsql` against `ngaut/client-rust`), plus discovery of
  the two existing parity-audit docs and `rust/docs/architecture/workspace.md`'s
  documented crate-boundary decision.
