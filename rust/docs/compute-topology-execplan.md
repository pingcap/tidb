# Compose disaggregated TiFlash topology and placement

## Purpose / Big Picture


Disaggregated TiFlash configuration currently leaves MPP selecting classic TiFlash
stores. Complete Go's small `pkg/util/tiflashcompute` owner and connect startup,
session dispatch policy and compute task placement in one batch. Follow Go master
1f819a0b4a6cc07f9a8ff07e6777761a770c6d3d. General physical fragments and coordinator
recovery remain separate obligations; do not claim those complete from topology APIs.

## Progress


- [x] Inspect complete upstream topology package, startup, placement and recovery callers; refresh Go master.
- [x] Capture invalid-policy baseline failure and implement topology, configuration and placement together.
- [x] Pass65 grouped Rust cases, current Go oracle, live SQL/startup probes, affected target checking and make lint.
- [ ] Self-review, update both registers and evidence, run actual hook and fresh locked publication build.

## Context and Orientation


The editable checkout is `/workspace/tidb`, branch `hparser-integration`, baseline
e648074e15f4b05e8204aa5c6162eecd6972c366. Native client remains at02880abbab5ed89a4dc603a4ddc7935853870ea6.
`tidb-exec/src/tiflash_mpp_scan.rs` owns the existing single-fragment scan and borrows
the process transport and region cache. `tidb-server/src/lib.rs` composes process
configuration. `tidb-session/src/sysvar.rs` validates session/global settings.

## Plan of Work and Milestones


Implement all production files of Go `pkg/util/tiflashcompute`: dispatch policy and
mock/AWS/test fetchers, recovery requests, timestamp cache, fixed pool and global
publication. Inventory the original BUILD and absence of package-local tests and
variants. Preserve error and HTTP behavior, without inventing GCP support.

Compose startup before storage selection, carry dispatch policy with statement
state, and group region tasks by compute address. Reuse the existing transport,
decoder and cancellation lifecycle. Keep classic behavior on the classic branch.
Validate empty/error/stale/fixed/recovery topology, policy rejection and fanout.

## Concrete Steps and Validation


Source `/workspace/.cloud-setup/env.sh`, export CARGO_BUILD_JOBS=1 and run Cargo from
`/workspace/tidb/rust`. Capture `cargo test --locked -p tidb-session --lib
compute_topology_batch -- --test-threads=1` before/after. Group topology and MPP
tests, then affected all-target checks. Run `make lint` at repository root. The
actual precommit hook must pass `cargo build --locked -p tidb-server`; rerun that
locked build immediately before normal authorized push and verify remote SHA.

## Surprises & Discoveries


Go's fixed pool bypasses subsequent HTTP calls only after a nonempty topology.
AWS response error flags are decoded but do not reject topology. Recovery is an
HTTP contract distinct from safe coordinator retry after result buffering.

## Decision Log


Use one process topology owner and the existing MPP fleet. No native client edits,
new runtime or generated protocol edits. Parent M01/M04/M05/N03 dispositions must
reflect remaining consumers rather than treating constructor presence as parity.

## Outcomes & Retrospective


The configured topology owner, statement policy and compute-node full-scan fanout are composed. The65 grouped Rust cases, Go oracle and live probes pass. M05 advances to partial;54 broader findings retain incomplete package/lifecycle obligations. Evidence directory:
`/workspace/.cloud-setup/compute-topology-batch`.

## Idempotence and Recovery


Preserve concurrent work and exact remotes. Never bypass hooks or force push.
Retire only completed owned binaries when disk requires it; retain build caches.
HTTP failures must not mutate cached topology. Fanout failures must close already
opened streams and release region leases.

## Interfaces and Dependencies


Use existing reqwest, serde, process config, query runtime, native store channels,
StatementMemory and SelectResponseIter. No new dependency or lockfile edits.

Publication and saved checkpoint outcomes are recorded in `/workspace/.cloud-setup/compute-topology-batch/final-handoff.json` after actual hook, fresh locked build and normal push. See the maintained [receipt](parity/current-audit/compute-topology-validation.json) for exact validation scope and limits.
