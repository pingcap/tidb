# Own cluster identity and active timestamp reporting

This living ExecPlan follows root `PLANS.md`.

## Purpose / Big Picture


Replace the standalone identity used by clustered connections with Go's leased server identity, and publish the minimum timestamp still protected by live work. Treat allocation, publication, admission, loss/recovery, reporting and joined shutdown as one batch across O01/O09 and their N04 consumer. Never enable MVCC GC merely because a timestamp reporter exists.

## Context and Orientation


Work in `/workspace/tidb`, branch `hparser-integration`, from f4ef95954c11710986ab1d1f359b4b13a12d52a5. Refreshed Go master is 93a01d31f6da205ae4bf376825293903a6899fdb in `/workspace/.cloud-setup/go-master`. Native master remains unchanged. No pushes or push dry runs are authorized; preserve the unmerged remote integration and local history.

`tidb-domain/src/serverinfo_syncer.rs` owns leased server records. `tidb-server/src/serverinfo_etcd.rs` binds its etcd operations to transport. `cluster_session_node/boot.rs` retains background owners, and `sql_node.rs` owns admission and connection IDs. `tidb-util/src/globalconn` already supplies both Go allocators. `tidb-session/src/process.rs` publishes user transactions; `tidb-txnkv/src/inner_txn.rs` supplies the existing internal timestamp registry. Go's source owners are `pkg/domain/domain.go` server-ID methods, `pkg/domain/infosync/info.go::ReportMinStartTS`, `pkg/domain/serverinfo/syncer.go`, and `pkg/server/server.go`. A lease is an expiring etcd grant; losing identity must stop connection admission before another node can reuse it. A TSO encodes physical milliseconds above 18 logical bits.

## Progress


- [x] Refresh comparison refs, inspect the ten structural segments and select shared cluster prerequisites.
- [x] Capture three failing existing-path tests; implement identity allocation/publication/admission/loss/recovery and shared MPP identity.
- [x] Wire physical transaction guards, eager/lazy cursor retention, user/internal minima, current lease publication and joined retirement; retain I04 lazy historical schema and DDL queue exclusion obligations.
- [x] Validate the whole changed segment, update both registers and prepare the normal commit guarded by the executable locked-build hook.
- [x] Prepare the recovery bundle and Cloud startup handoff without pushing; final Git/bundle/draft identities are recorded externally in `/workspace/.cloud-setup/cluster-lifecycle-batch/final-handoff.json` to avoid a self-referential commit.

## Plan of Work


Use one domain identity authority for server-info serialization, configured global/simple connection allocation and admission. Claim IDs with etcd create-revision transactions, retain the lease, renew, reject/kill connections on prolonged loss, reacquire and republish, and stop/join before releasing the lease. Follow Go's 32-bit candidate policy and 64-bit fallback. Add the timestamp reporter to the existing server-info lifetime, consuming production transaction/cursor/internal/schema timestamp owners rather than fabricated timestamps or a second transaction implementation. Keep GC disabled. Route local KILL authorization through the existing internal-process list where present; remote KILL remains a distinct transport obligation unless its entire dispatch path is implemented and validated.

## Milestones


First establish the current allocator/config/publication defects against existing code. Next compose identity and admission with deterministic collision/loss/recovery/shutdown tests. Then connect the reporter and all available timestamp producers, proving publication follows current lease replacement and stops on retirement. Finally check the affected crates together, lint and run the mandatory locked server build in the executable precommit hook. Finding closure requires live-source and behavioral evidence for its full claim; partial producer coverage must remain partial.

## Concrete Steps


Activate `/workspace/.cloud-setup/env.sh`; use CARGO_BUILD_JOBS=1, CARGO_INCREMENTAL=0 and existing build outputs. Run targeted new server regressions together before production edits. Run combined domain/server/session/transaction checks after the batch, plus appropriate real transport/SQL checks. Run `make lint` from repository root and `cargo check --locked --all-targets` for affected crates from `rust/`. Commit normally with TERM=xterm and core.hooksPath=hooks; `cd rust && cargo build --locked -p tidb-server` must pass inside the actual hook. Record exact final commands and outcomes in the audit receipt.

## Validation and Acceptance


Disabled global kill uses simple IDs; 32/64-bit allocation follows config and shares the published server ID. Multiple leased owners cannot claim the same ID. Loss closes admission and live connections; recovery obtains and publishes valid identity; shutdown cannot resurrect a worker. Timestamp reporting selects only values strictly above Go's lower age bound, includes protected work and uses the server-info lease. Reporting errors preserve safe ownership. No full package or live multi-node acceptance is implied by deterministic tests alone.

## Idempotence and Recovery


Keep artifacts under `/workspace/.cloud-setup/cluster-lifecycle-batch`. Clean up only test services started here. Preserve source changes and current build artifacts; disk is constrained. Update the unpublished bundle only after verified local commit, then survey all workspace repositories and save/read back exact Cloud draft pins. Fresh-task restoration remains unverified.

## Surprises & Discoveries


GlobalAllocator is currently hardwired to server ID 1 and 32-bit mode, while published server info has no numeric getter. The existing internal transaction timestamp box has a source-shaped implementation but reporting is not composed into startup. Complete versioned InfoCache and remote KILL remain broader prerequisites, so their absence cannot be hidden by changing finding labels.

## Decision Log


- Decision: segment around cluster ownership and protection rather than adding isolated SQL fields. Rationale: one identity/reporting lifetime supplies multiple consumers and gates future distributed work. Date: 2026-10-04.

## Outcomes & Retrospective


All 58 grouped Rust cases pass, including real etcd identity owners, lease revocation/recovery, cursor/transaction retirement and existing compatibility cases. Affected all-target checking, lint, scoped formatting, six real MySQL/unistore assertions and the locked server build pass. The actual commit hook enforces the same build; its execution and saved configuration outcome are recorded in the external final handoff. O01 is repaired at its finding boundary; O09/N04 remain partial and no whole package is accepted.

## Interfaces and Dependencies


Reuse EtcdOps, globalconn allocators, the existing process registry and inner transaction registry. Add domain-owned lifetime interfaces where needed; bind transport only in tidb-server. Preserve native dependency/lockfile consistency and avoid a parallel allocator or timestamp algorithm.

## Validation discoveries (2026-10-04)


The real etcd test initially failed identity recovery. Pinned etcd-client consumes its initial TTL-zero response and returns a specific LeaseKeepAliveError. The wrapper flattened that to Unreachable, so the keeper could never retire the expired lease. EtcdError now preserves LeaseExpired, existing error-based consumers remain intact, and the numeric owner receives TTL zero. A confirmed expiration stops endpoint retries so another endpoint outage cannot hide it. The same real integration now passes.

ActiveStartTs holds reference-counted physical transaction and retained-cursor timestamps, including independent owners at the same TSO. Go's separate RunInNewTxn registry remains intact. The reporter uses physical PD time, the live GC maximum wait variable, strict age bounds and the current server-info lease. Materialized catalogs keep their real storage transaction protected; the absent lazy historical InfoSchema owner is not simulated. DDL-queue exclusion and internal-session diagnostic classification remain explicit residuals. No MVCC GC worker is enabled.

The Cloud has etcd 3.5.16 from Debian's signed trixie package index. Its integration test is explicitly invoked with --include-ignored; it does not require a running service and cleans up the process and data it creates. Baseline wire probes need the same env.sh activation as compilation because Rust's production worker stack needs RUST_MIN_STACK. The first unactivated wire attempt hit a stack overflow; retain that separate setup-error receipt and use the correctly activated run for before/after comparison.

The final boot teardown keeps identity and minimum-report leases until the status callback releases the last session factory, its workers join, and its statistics flush completes. Claim creation uses Go’s ten-second operation deadline; leased writes share the one-second timeout and thirty-millisecond retry interval. This replaces duplicate new retry loops with one EtcdOps helper.

A final read-only GitHub check observed origin/hparser-integration advance to 219a0de48ec86ab988c536e587e150543c524948 (additional schema-ack alignment). It is fetched but remains unmerged with the earlier concurrent changes. Go master and native master remain unchanged. Do not reset or overwrite this remote work.

Validation outcome: 58 distinct Rust cases, six wire assertions and three clean server exits. O01 is repaired, O09 and N04 remain partial, and the register has 30 repaired and 56 unresolved findings (32 open, 24 partial). No package completeness, Go-peer GC, multi-node TiKV or benchmark claim is made.
