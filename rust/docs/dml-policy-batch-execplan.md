# Repair DML buffering and mutation policy together

This living ExecPlan follows repository PLANS.md.

## Purpose / Big Picture

Joined writes must stop at their session memory quota while reading, and every read executor must close after failure. The KV buffer must transport absence flags and assertions independently, leaving the table caller to choose them. E03 and T01 are the connected audit findings. This maintains existing owners; it does not accept a complete transcreated Go package.

## Progress

- [x] Read instructions and reproduce live source gaps; fetch all authorized refs. Go master remains 93a01d31f6da205ae4bf376825293903a6899fdb, native remains 19a56ccda1e128218cd33c69709038219aced9bc; integration remote remains 7b991676da79f044774caf6da4dfffe247160feb.
- [x] Add two policy and three corrected quota/close/consumer fail-before regressions.
- [x] Consume every physical DML caller through the visitor, charge retained rows during growth, close on all Result exits, migrate every generic insert caller to explicit metadata.
- [x] Run scoped Rust tests, all-target checks, lint, and actual locked-build commit hook. Update both registers, durable receipt and cloud draft; no pushes.

## Surprises & Discoveries

The production wire smoke exposed OOM action loss: replacing helper contexts lets the earlier tracker detach after the next action is installed. Two owner-lifetime regressions fail before the repair. Planning/execution share the active authority; SQL, cached prepared and bootstrap boundaries retire it explicitly. Bootstrap must not leave a tracker active before the connection installs spill/arbitration policy. Go ResetContextOfStmt and Tracker.Detach retain their ownership and ordering.

Go table AddRecord asserts Unknown for an optimistic lazy absence check, while checked or pessimistic writes assert NotExist. Current BufferMutation::insert unconditionally asserts NotExist. The retained configured-write executor is explicitly optimistic. System clustered writes receive a snapshot-proven absence. System index code still requires a fuller duplicate-check lifecycle and must not be marked repaired merely because record policy improves.

## Decision Log

- Decision: Repair both existing DML owners in one batch, keep parent findings partial if remaining schema/handle, matrix or index lifecycle is not migrated.
  Rationale: Neither leaf fixes nor passing serial result tests establish full handoff parity.
  Date/Author: 2026-10-03, Codex.
- Decision: Preserve user no-push instruction and existing exact destinations.
  Rationale: Latest authorization overrides earlier publication instructions.
  Date/Author: 2026-10-03, Codex.

## Outcomes & Retrospective

E03 and T01 are partial. Eleven behavioral failures are repaired, while complete planner/matrix/system-index/pessimistic lifecycles remain. Registers agree: 58 unresolved (44 open, fourteen partial), 28 repaired. Wider driver/DDL runs have twelve/five failures reproduced with pre-batch subsystem behavior; they are not deleted or misreported as passing. Affected all-target checking, lint and enabled source-commit hooks passed. Production wire checks exposed and drove the final text-command recovery repair. Final wire cancellation/recovery plus normal-quota controls pass; cloud draft and recovery bundle preserve the local no-push handoff.

## Context and Orientation

The checkout is /workspace/tidb on hparser-integration. PhysicalBuilder builds read executors; driver drain currently retains all datum rows. multi_dml then copies them into joined SourceRows before charging. StatementMemory owns the canonical quota and killer. BufferMutation in tidb-txnkv carries MemDB writes. real_tikv_dml and system_row_write encode record/index mutations in tidb-exec. Go reference owners are pkg/executor/update.go, delete.go and pkg/table/tables/{tables,index}.go in /workspace/.cloud-setup/go-master.

## Plan of Work

First add observable failures. Then make the read helper visit each row before requesting another chunk, and charge each retained joined row before pushing it. Keep runtime stats and explicit error precedence. Close the read on open, next and visitor failure. Replace BufferMutation::insert with independent metadata setters and migrate every Rust caller. Existing table mutation policy chooses optimistic lazy Unknown versus checked/pessimistic NotExist. Record which index/matrix/planner ownership remains.

## Concrete Steps

From /workspace/tidb/rust source /workspace/.cloud-setup/env.sh. Run cargo test --locked -p tidb-executor --lib dml_batch and cargo test --locked -p tidb-exec --lib table_write_policy. Record red results before fixes and green results after. Run cargo check --locked -p tidb-executor -p tidb-exec -p tidb-txnkv -p tidb-server --all-targets. From /workspace/tidb run make lint and git diff --check. Commit locally with hooks enabled; the actual precommit hook must run cd rust && cargo build --locked -p tidb-server.

## Validation and Acceptance

A quota-crossing physical row must fail before draining the whole source, and source close must occur after both next and consumer failures. Optimistic lazy inserts must preserve the duplicate-check flag but assert Unknown; checked/pessimistic cases must not be blanket Unknown. All metadata callers must compile. Do not infer live multi-node behavior from these fixtures.

## Idempotence and Recovery

Tests and checks are repeatable. Preserve changes; never reset, force push or bypass hooks. Only remove obsolete completed generated ELF artifacts if disk space requires it. Keep no-push local commits in the existing recovery bundle; drafts are not published snapshots.

## Artifacts and Notes

Logs and exact baseline failure names go under /workspace/.cloud-setup/dml-batch. Read rust/docs/parity/current-audit/dml-policy-batch-repair.md and dml-policy-batch-validation.json for delivered limits and receipts. Baseline source swaps preserve backups and restore bytes in finally.

## Interfaces and Dependencies

Use existing Executor, StatementMemory, WriteMemory, BufferMutation and AssertionOp. No dependency versions or Go files change. Session adds a direct dependency on the existing tidb-expr owner; its lock entry records that edge only. The row visitor returns DriverError and runs before the next physical chunk. Metadata setters must preserve independent flags and assertions rather than inventing table policy in KV.


The next wire run correctly cancelled DML but could not reset its quota through SET. The text parse boundary now retires the preceding authority even for SET/control routes that bypass execution. SET literal evaluation uses the existing scalar evaluator directly, following Go SetExecutor rather than constructing a SELECT result under the old tiny quota. Complex expression/subquery SET evaluation retains its existing path; full SET owner parity is not claimed. A third owner/recovery regression fails before this final repair.


The existing Go-based user-variable test exposed a skipped bare-word SET policy on the pre-bound multi-assignment path. That policy now lives in the common bound-value evaluator, covering both ordinary and pre-bound assignments without duplicating evaluation semantics. Final quota/lifecycle/user-variable/hooks/global-variable checks run as one libtest invocation with multiple filters.


Expanded SET/config validation discovers two genuine duplicate policies: an early float-only check blocks Go's valid GC-trigger percentage fallback, and an early session packet branch skips canonical upper-bound warnings. Both duplicate branches are removed; shared validation retains Go's parse-error behavior for invalid percentages. N03 remains partial. Two stale/harmful test expectations are corrected against live Go: classic kernels refuse flight recording, and query-info after completed statements includes global txn scope and RU v2 JSON. X-kernel success remains an upstream obligation, not an unsupported feature added here.


Final validation: eleven fail-before/pass-after cases; 698 distinct passing Rust cases; seventeen controlled pre-existing broader failures retained; affected all-target checks, make lint and actual locked-build source hooks pass. Final real MySQL/unistore check verifies three 8175 cancellations with unchanged data and three successful default-quota controls. No pushes, whole-package acceptance, live multi-node TiKV or performance claims.
