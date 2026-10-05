# Complete the deferred pessimistic uniqueness lifecycle

This living ExecPlan follows `PLANS.md` at the repository root.

## Purpose and outcome

The session accepts `tidb_constraint_check_in_place_pessimistic=OFF`, but the table writer does not publish the native prewrite-check flag. Complete this configuration consumer (N03) and its table policy (T01) together. Explicit user pessimistic transactions must defer eligible INSERT, UPDATE and ODKU duplicate checks until commit, refuse savepoints and abort after a terminal DML error, following Go. Autocommit, optimistic, restricted and IGNORE paths retain their own policy.

## Progress

- [x] Refresh both remotes and confirm clean integration/native branches and Go source.
- [x] Trace existing session, table and native buffer owners.
- [x] Add grouped failing regressions in the existing real-unistore suite.
- [x] Compose flags and safety policy through shared production owners.
- [x] Validate grouped regressions, affected checks, lint and locked server build.
- [x] Update both audit registers and review the final source diff.
- [ ] Commit through the actual hook, push after a fresh locked build and refresh reusable handoff (post-commit execution receipts live in `/workspace/.cloud-setup/deferred-uniqueness-batch/final-handoff.json`).

## Context and orientation

Work in `/workspace/tidb` on `hparser-integration`, baseline f6a53da43a2702d381bfcc5a3bb8e3dd49d6ecab. Native `/workspace/client-rust` master bc8cca3fea4f741e68a41b124f28164604a57ee2 already owns the required MemDB flags and commit behavior. Go master 93a01d31f6da205ae4bf376825293903a6899fdb is exported at `/workspace/.cloud-setup/go-master`. No native edits are planned. This is maintenance of existing owners; it does not accept a complete transcreated package.

`pkg/executor/insert.go::getPessimisticLazyCheckMode` selects prewrite checking only for explicit user transactions with the setting OFF. `pkg/table/tables/{tables,index}.go` sets NeedConstraintCheckInPrewrite only when a local miss actually presumed absence; local tombstones do not qualify. Native MemDB value writes clear that flag, so it must accompany or follow the value write. `pkg/session/session.go::KeyNeedToLock` excludes these keys from statement locking. `pkg/executor/simple.go` refuses savepoints and `adapter.go::handlePessimisticDML` aborts after a terminal error.

## Plan of work and milestones

First extend `tidb-server/src/cluster_session_node/tests/unistore_cop.rs` with grouped cases using its existing embedded-store fixture. Preserve a baseline failure receipt. Then expose the selected policy in `tidb-session/src/stmt_ctx.rs` and `tidb-executor/src/stmt_context.rs`; compose table-owned absence selection and the native buffer flag through `kv_table`, `storage` and `cluster_storage`. Complete session savepoint and server transaction teardown policy. Every changed key retains its original duplicate diagnostic. Do not duplicate native transactions or mutation buffers.

Finally run the same regressions plus adjacent transaction cases and all-target checking together. Review flags on tombstones, IGNORE, local duplicates, mode switches, autocommit and transaction cleanup. Record exact results and limits in the durable validation JSON and both finding registers.

## Concrete steps and acceptance

Source `/workspace/.cloud-setup/env.sh`, then from `/workspace/tidb/rust` run:

    CARGO_BUILD_JOBS=1 cargo test --locked -p tidb-server --lib -- deferred_uniqueness_batch --test-threads=1
    CARGO_BUILD_JOBS=1 cargo check --locked -p tidb-executor -p tidb-session -p tidb-server --all-targets
    CARGO_BUILD_JOBS=1 cargo build --locked -p tidb-server

From the repository root run `make lint` and `git diff --check`. The real precommit hook must run the locked build. Repeat that build immediately before normal push; verify the remote SHA. Baseline regressions must fail on wrong behavior, then all pass after the fix. Real unistore verifies native prewrite composition; it does not establish multi-node TiKV or performance acceptance.

## Idempotence and recovery

Preserve unrelated changes. Keep logs under `/workspace/.cloud-setup/deferred-uniqueness-batch`. Reuse valid build outputs and one build job. Never force push or bypass hooks. Native sync is unnecessary unless native source changes; if needed use the maintained sync script. Record blockers accurately.

## Surprises & Discoveries

The native client already implements the flag and the shared SQL lock selector already consumes it. The missing producer makes the existing lock-stage setting check incomplete. Native MemDB clears the flag on every value write, requiring careful ordering.

## Decision Log

- Decision: repair N03's selected setting consumer and T01's full deferred-check statement lifecycle together.
  Rationale: enabling only deferred table flags without abort/savepoint rules would weaken transaction safety.

## Interfaces and dependencies

Use the existing `StmtContext`, `TableStorage`, `MutationBuffer`, native MemDB, `Session` and `ClusterServerSession` lifetimes. Preserve all dependency pins. The table policy carries whether checking is deferred until prewrite separately from transaction mode and whether an absence check was actually lazy.

## Outcomes & Retrospective

The three baseline Rust regressions fail before implementation; six new cases and adjacent lifecycle coverage total 63 distinct passing Rust tests. The TCP matrix changes from 13 failures/10 passes to 23 passes. Affected all-target checking, lint and the locked server build pass. T01 and N03 remain partial until their unrelated residuals and complete package obligations are satisfied. Publication evidence is recorded after execution in `/workspace/.cloud-setup/deferred-uniqueness-batch/final-handoff.json`.

The final review preserves Go unistore assertion ordering: STRICT may return 8141 before an Insert duplicate, while OFF returns 1062. SQL and binary-prepared INSERT need the explicit DML classification at the shared abort boundary; a query read-shape classification alone does not identify INSERT.
