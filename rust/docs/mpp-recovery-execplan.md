# Share MPP stream setup and storage recovery


This living ExecPlan follows root `PLANS.md`. Maintain every section as implementation and validation proceed.

## Purpose and context


MPP scans must recover a failed connection or first receive before exposing results, without replaying delivered rows or retrying canceled work. Go `pkg/store/copr/mpp.go::EstablishMPPConns`, `pkg/executor/internal/mpp/local_mpp_coordinator.go::receiveResults` and pinned client-go `internal/client/client.go::getMPPStreamResponse` jointly own this contract. Before this batch, Rust `tidb-exec/src/tiflash_mpp_scan.rs` opened only response headers, erased setup status into strings and canceled immediately on any opening error. The ordinary storage transport already retains typed status, timeout and physical channel generation (the identity of a particular pooled connection).

Work against fetched Go `9e298833cf87fe957f146f32d1f1a273356a5d22`, client-go `8edb23f6c7ee` and native client `02880abbab5ed89a4dc603a4ddc7935853870ea6`. This connected T02/M04 maintenance moves MPP onto the existing transport error and native backoff owners. It does not accept complete Go packages or the scan-only MPP fragment planner. All source/build execution is in `/workspace/tidb` Cloud checkout on `hparser-integration`.

## Progress

- [x] Inspect instructions, refresh Go master, verify both remotes and clean integration/native heads.
- [x] Compare Go setup, first receive, retry, cancellation and generation retirement with live Rust callers.
- [x] Extend existing socket fixtures: grouped baseline has 3 failures (headers, first receive, EOF) and 1 preserved cancellation pass.
- [x] Share error projection, native TiFlash backoff and first-packet ownership; migrate both setup and tail receives to common cancellation/deadline handling.
- [x] 33 MPP and 7 shared transport/backoff tests pass, zero failed/ignored; affected all-target checks, root lint, scoped format/diff review pass. Both registers updated.
- [ ] Commit with the actual locked-build hook, run a fresh locked build immediately before authorized push, verify remote and save reusable checkpoint.

## Milestones and plan of work


First extend the existing TiFlash socket fixture with scripted setup and first-packet failures and attempt counters. Run the regressions together against unchanged production code. Reuse the fixture and retain late-stream, TLS, memory and cancellation coverage.

Then expose the ordinary transport's status projection through its borrowed `StoreRpcChannel`; keep timeout provenance and exact generation identity. Add the TiFlash category to the existing `RegionBackoffBudget`, which delegates schedules/accounting to native `RetryBackoffer`. Migrate MPP establishment to typed errors, acquire a fresh fleet selection per retry, receive exactly one initial packet within the per-receive timeout, retry eligible failures under the existing 20-second effective budget, and terminate cancellation without retry. Unlike ordinary region recovery, Go MPP cancellation does not retire the channel generation. Preserve this caller-specific decision; local timeout and statement KILL do not retire it either. Region-bearing dispatch stays single-attempt, following Go. EOF is successful empty completion; errors after the first packet never reopen the stream.

Finally run the affected Rust suites and all-target checks as one batch, root `make lint`, and scoped format/diff review. Update the JSON and readable registers without marking broad roots complete. Publish only through the real hook and fresh pre-push build.

## Concrete steps and validation


Activate `source /workspace/.cloud-setup/env.sh; export CARGO_BUILD_JOBS=1`. From `/workspace/tidb/rust`, run:

    cargo test --locked -p tidb-exec --lib -- tiflash_mpp_scan:: --test-threads=1
    cargo test --locked -p tidb-txnkv --lib -- rpc::unary::tests:: retry::tests:: --test-threads=1
    cargo check --locked -p tidb-exec -p tidb-txnkv -p tidb-server --all-targets

From `/workspace/tidb`, run `make lint`; commit normally with `TERM=xterm`. The selected executable `hooks/pre-commit` must pass `cd rust && cargo build --locked -p tidb-server`. Repeat that build immediately before each push and compare `git ls-remote` against the local commit. No native changes are planned, so maintained sync is unnecessary unless that scope changes.

Acceptance: transient setup and initial-receive failures recover; canceled RPCs remain terminal and preserve the shared fleet; local KILL interrupts setup/wait; empty EOF stays empty; post-delivery errors do not replay. Preserve meaningful socket tests. Full Go suites, distributed TiFlash/TiKV, performance and complete package acceptance are not claimed.

## Idempotence and recovery


Preserve concurrent changes. Never force-push or bypass hooks. Repeated tests are safe; retire only owned completed inactive executables when disk pressure requires it, preserving compiler caches. Source receipts and logs live under `/workspace/.cloud-setup/mpp-recovery-batch`; required durable evidence belongs in `rust/docs/parity/current-audit`. A failed check requires diagnosis before retry. Revert only this batch's edits if its owner migration cannot satisfy the contract.

## Interfaces and dependencies


The existing `StoreRpcChannel` supplies physical identity and typed RPC error conversion to MPP and ordinary calls. `RegionBackoffBudget` continues to delegate to native client-rust; no second jitter engine, channel pool or request worker is introduced. `MppQueryResponse` retains one initial packet and existing statement memory/cancellation ownership until consumed or closed.

## Surprises & Discoveries


Go deliberately does not retry region-bearing dispatch after an RPC failure. Establishment does retry, and its first receive belongs to that retry boundary. Rust previously deferred this receive until after returning the stream. Go MPP does not invoke ordinary region-recovery generation retirement on Canceled; the initial proposed retirement assertion was corrected during source review before the production edit.

## Decision Log


- Decision: migrate typed errors and first-packet/retry lifetime together. Rationale: retrying strings or retrying after row delivery loses cancellation identity or duplicates results. Date/author: 2026-10-08, Codex.
- Decision: retain Rust correctness/socket tests and remove only obsolete setup behavior or duplicate mechanisms. Rationale: absence of an identical Go test name does not make a behavioral regression disposable. Date/author: 2026-10-08, Codex.

## Outcomes & Retrospective


Implementation and grouped validation are complete: 40 selected Rust tests pass with zero failed/ignored, affected all-target checks and make lint pass. One initial compilation exposed the exhaustive storage error mapping for the added TiFlash category; it was repaired before the final run. T02/M04 remain partial; the other 54 unresolved findings carry their existing evidence.


Validation evidence is frozen in `parity/current-audit/mpp-recovery-validation.json`. The actual commit hook and immediately-pre-push locked build run after this freeze; `/workspace/.cloud-setup/mpp-recovery-batch/final-handoff.json` records their final outcomes and remote SHA.
