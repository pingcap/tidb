# Shared ordinary request forwarding and transport feedback

## Purpose / Big Picture


Repair the connected N03 configuration and T02 ordinary-routing residuals against Go master b36c940a4332c866d8b0e2afde88f5e7c2fd7fed and selected client-go 8edb23f6c7ee. The process configuration must reach point/batch/scan, writes and lock recovery; an unavailable logical leader must retain its peer while a healthy physical proxy carries forwarding metadata. Completed attempts must update the same canonical cache used by coprocessor reads. This is existing-owner maintenance, not complete package acceptance.

## Progress


- [x] Refresh source refs and trace native routing, canonical cache, command transport and lock recovery.
- [x] Reproduce both initial emitted-route/proxy-publication regressions before repair.
- [x] Connect configuration, proxy selection, forwarding metadata and generation-safe failure/success feedback across ordinary commands; native NotLeader accessor published as cfafb1eb01594cecd5926fa63e57e2a6e20c7ee1 and synchronized through the maintained script.
- [x] Run 31 native and 74 TiDB cases, affected all-target checking, lint and self-review; update both finding registers and receipt.
- [ ] Normal commit with actual locked-server hook, fresh locked prepush build, verify exact remote; refresh Cloud recovery/startup.

## Context and Orientation


Base 03f9e989ac280d6d2fa34f05e8b1b01128013773 in /workspace/tidb, hparser-integration. Native /workspace/client-rust master remains a0a6ec32deb8dfb565494b9b803598cd0e7bcef2. ClientPd already consumes native ReplicaRouting but forwarding_enabled is false, proxy lookup returns None, and inherited KvClient methods discard forwarded_host. Production BatchCommands and raw unary already support forwarding. Canonical RegionCache owns preferred proxies, store health and epoch-safe feedback; reuse these owners.

## Milestones


First retain failing emitted-request regressions in the existing snapshot suite. Then connect route metadata through the shared transport and cache feedback through the immutable selected route. Preserve native request attempts and cancellation. Finally validate once at the batch boundary, retaining any broader unresolved obligations. Read client-go internal/locate and internal/client inventories from structural coverage; no package acceptance is inferred from this repair.

## Validation


Source /workspace/.cloud-setup/env.sh in every shell and run Cargo from rust/. Use CARGO_BUILD_JOBS=1 cargo test --locked -p tidb-txnkv --test snapshot_lock_wait_source and focused bridge/transport library suites. Run affected all-target checking and root make lint once after implementation. Required actual commit hook and fresh prepush command are cd rust && cargo build --locked -p tidb-server. Logs and before-images belong in /workspace/.cloud-setup/ordinary-forwarding-batch. No broad Go suite or live multi-node acceptance is claimed.

## Surprises & Discoveries


The raw CheckTxnStatus path emitted extra CheckSecondaryLocks and ResolveLock latency samples. Correct command attribution belongs to the same transport boundary and must be covered with the metadata checks.

Two retained snapshot cases caught invalidated store epochs being handed to a later statement and expired admission being mistaken for store failure. Preserve those assertions: refresh stale snapshots on the next lookup and exclude caller cancellation/deadline exhaustion from health feedback. Read mode also stays attached to the selected endpoint; stale-read leader probes have ordinary wire flags but are not source leader mode.

## Decision Log


Retain shared native retry policy and canonical region/store generations. Never introduce a request-history map in the adapter. Capture an immutable route in each endpoint handle so delayed feedback cannot damage a refreshed topology. Original Go obligations remain open; periodic replica-flow reporting and full cache consolidation are outside this maintenance unit.

Concurrent remote DDL commits ab8cb5a5f8 and c22792ae62 advanced integration during validation. Fast-forward them before the batch commit, preserving their two nonoverlapping files; repeat combined server checks and publication builds.

## Outcomes & Retrospective


Seven new regressions cover the connected forwarding/transport boundary; two original routing cases and the original metrics command fail before repair. All 105 distinct selected cases pass, along with affected all-target checks and lint. T02/N03 evidence is updated in both registers; every status/count remains unchanged because full cache, health and package acceptance remains open. See parity/current-audit/ordinary-forwarding-batch-validation.json. Actual hook, publication and recovery results are recorded in /workspace/.cloud-setup/ordinary-forwarding-batch/final-handoff.json.

## Recovery


Recover individual files from the base commit after inspecting concurrent changes; never reset the checkout or force-push. Existing complete package inventories and original fixtures remain unchanged.
