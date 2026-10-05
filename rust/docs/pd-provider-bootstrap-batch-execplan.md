# Complete PD provider selection and keyspace bootstrap

This living ExecPlan follows repository `PLANS.md`.

## Purpose / Big Picture


Connect an API-v2 client directly to its assigned timestamp group without requiring a default-group service. Select minimum timestamps through Go's current provider policy, and handle optional metadata headers without panics. These connected repairs advance P03/P06 in the native owner consumed by TiDB; they do not claim complete upstream package acceptance.

## Progress


- [x] Refreshed both clean editable branches and Go master: TiDB 85abcb3c23, native bb8206e7d080, Go 93a01d31f6. No concurrent changes were found.
- [x] Four grouped regressions fail on native bb8206e7d080: classic minimum RPC, API minimum/optional header, metadata optional header, and V2 default-group bootstrap. Initial fixture compilation required explicit tonic codec types; the rerun reached all four behavioral failures.
- [x] Implemented initialization metadata handoff, Go provider selection/Unimplemented fallback, optional headers across sixteen metadata adapters, and explicit membership-header validation. All grouped validation passed.
- [x] 167 native PD and 40 keyspace tests plus all-target checking pass. Native bc8cca3fea4f741e68a41b124f28164604a57ee2 published after a fresh locked server build, remote SHA verified, maintained sync completed.
- [x] 27 TiDB unit and 52 integration tests pass (one existing live-PD test ignored); affected all-target checks and make lint pass. Both registers and receipt updated. Publication is prepared with the actual hook and separate fresh prepush build; final outcomes and remote SHA are recorded in Cloud final-handoff.json.

## Context and Orientation


Cloud checkouts are `/workspace/client-rust` on master and `/workspace/tidb` on hparser-integration. Native `src/pd/client.rs` currently loads keyspace metadata after `RetryClient::connect` has already discovered a default timestamp group. `cluster.rs` unconditionally calls GetMinTS and unwraps optional response headers. The existing fixture is `src/pd/timestamp_tests.rs`. Pinned Go PD module is `/workspace/.cloud-setup/gopath/pkg/mod/github.com/tikv/pd/client@v0.0.0-20260805103528-afa43111d149`; its `client.go` loads keyspace metadata in discovery initialization and selects GetTS for the PD provider, with an Unimplemented fallback for API GetMinTS. Generated Go getters permit absent headers.

## Plan of Work


Extend the fixture with keyspace metadata, restricted group discovery, minimum timestamp responses and optional metadata headers. Add regressions using existing production entrypoints before implementation. Load and validate V2 metadata on the metadata connection before opening a timestamp route, retaining that same metadata for codec publication. Select ordinary TSO for classic mode and API GetMinTS for API mode; fallback only for Unimplemented, preserving the existing retry/cancellation owner. Replace unchecked optional header access for every existing metadata response adapter. Keep membership validation explicit. No generated code is edited.

## Concrete Steps


Activate each shell with `source /workspace/.cloud-setup/env.sh`. From client-rust run `CARGO_BUILD_JOBS=1 CARGO_TARGET_DIR=/workspace/tidb/rust/target cargo test --locked -p tikv-client --lib source_provider_` before production edits, then the grouped `pd::` suite and affected API-v2 tests. Run native all-target checking once. Native publication requires a fresh `cargo build --locked -p tidb-server` from tidb/rust, followed immediately by normal push and remote verification. Run `bash rust/scripts/sync-tikv-client-rs.sh` from TiDB after native publication. Finish affected TiDB tests/checks and `make lint`, then the actual precommit hook and separate immediate prepush locked build.

## Validation and Acceptance


Observe successful V2 startup when only the requested group exists; no default-group discovery or allocation is allowed. Classic minimum timestamps use the retained TSO stream; API minimum timestamps use the API response, with compatibility fallback only on Unimplemented. Metadata responses with absent headers preserve their values; explicit error headers remain errors. Cancelled/closed clients cannot allocate timestamps or start workers. Record fail-before/pass-after counts and source pins, without claiming live multi-node or full Go coverage.

## Idempotence and Recovery


Preserve work and exact remotes; never force-push or bypass hooks. Use the maintained vendor sync and reconcile failed patches. Retain validation failures and recoverable build outputs. Failed initialization must not leak an owned timestamp stream or discovery worker.

## Surprises & Discoveries


The prior batch's route selection occurs too late for non-default-only V2 bootstrap. The existing metadata adapter also unwraps optional headers although Go uses nil-safe getters.

## Decision Log


Repair all three at the shared native boundary and validate together; avoid adding a second TiDB policy implementation. Date: 2026-10-05.

## Outcomes & Retrospective


Four behavior regressions fail before and all 286 native/TiDB cases pass after. One existing live-PD test is ignored. Bootstrap no longer needs a default-group route, minimum timestamps follow provider policy, and optional metadata headers do not panic. Counts remain 86 tracked, 30 repaired and 56 unresolved (29 open, 27 partial): whole PD package, follower/forwarding and transport ownership remain explicit residual obligations. Other 54 roots were not freshly reproduced. Publication gate outcomes belong to Cloud final-handoff.json.

## Artifacts and Notes


Store Cloud logs in `/workspace/.cloud-setup/pd-provider-batch`; the durable receipt belongs in `rust/docs/parity/current-audit/`.

## Interfaces and Dependencies


Reuse existing tonic/prost messages, SecurityManager, RetryClient cancellation and TimestampOracle. Initial keyspace metadata flows from Connection through RetryClient into the existing PdRpcClient codec resolver. No runtime dependency is added.
