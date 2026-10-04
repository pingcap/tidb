# Share observed plans and execution admission

This living ExecPlan follows PLANS.md. All execution is in Codex Cloud; no pushes or push dry runs are authorized.

## Purpose / Big Picture

Completed SQL should expose its actual encoded physical plan through statement summaries, obey the live GLOBAL binary-plan switch, and retain prepared plans without exposing them through the process list. TopSQL counters must follow Go's fast-plan and runtime-toggle rules. These connected repairs advance O11, O18 and N03; complete packages, normalized plan digests, profiling transport and runtime statistics remain outside this maintenance batch.

## Progress

- [x] Read shared owners and Go adapter/encode source at master 93a01d31f6da205ae4bf376825293903a6899fdb; clean parent 229bda1c3ba0bbe7b17a1056ac4eb66ebc5c34a0.
- [x] Add six regressions covering actual SQL execution, binary prepared summaries and runtime toggle windows.
- [x] Record six independent failing Rust regressions and seven failing real-wire assertions; implement shared producers and migrate completion consumers.
- [x] Run grouped regressions: 93 distinct Rust cases and 13 real-wire assertions pass, zero failures/ignored; affected all-target checking, locked server build, make lint and self-review pass.
- [x] Update both finding registers, structural batch map and validation receipt. The local commit must pass the actual locked-build hook; final commit, bundle verification and draft readback are recorded in /workspace/.cloud-setup/observation-plan-batch/final-handoff.json.

## Context and Orientation

`rust/crates/tidb-session/src/observation.rs` owns one observation from compilation through result close. `StmtContext` publishes `ProcessPlanInfo` from `tidb-executor/src/explain.rs` using the retained physical tree. The existing `tidb-util/src/plancodec.rs` owns Go's compressed text and TiPB encodings. Summary stores already apply sample size limits. GLOBAL sysvars are shared across sessions; consume that owner instead of adding an unrelated switch.

## Plan of Work

Extend shared plan metadata with encoded and summary binary plans plus Go fast-plan classification. Keep the existing eager physical-tree capture; avoid a second full-tree render for the text codec. This does not claim Go's lazy sampling cost or complete runtime-plan metadata. Reuse the existing EXPLAIN tree and codec, keeping binary prepared process-list suppression independent of summary samples. Both session context constructors already use this shared publication seam; no duplicate frontend producer is added. Completion reads the live binary switch. Counter begin is unconditional for non-fast execution; fast PointGet/TableDual (through one projection) and SET can omit begin while disabled. Completion independently consults live enable state, allowing unmatched finish across toggle windows as the existing counter contract specifies. Failed physical compilation must remain excluded.

## Milestones

First run the new session regressions together against unchanged production code. Next implement the shared plan and admission changes without inventing normalized digests or runtime measurements. Finally run affected checks once as a group, record exact results and preserve all broader unresolved obligations.

## Concrete Steps

From `/workspace/tidb/rust`, source `/workspace/.cloud-setup/env.sh`. Run `cargo test --locked -p tidb-session --lib observation_plan_batch -- --test-threads=1` before changes. After implementation run the complete existing observation group and related prepared regressions from one compiled test executable. Run `cargo check --locked -p tidb-executor -p tidb-session -p tidb-server --all-targets`. From `/workspace/tidb`, run `make lint`. Commit normally with TERM=xterm so hooks/pre-commit runs `cd rust && cargo build --locked -p tidb-server`.

## Validation and Acceptance

Decoded PLAN names the actual table/operator; prepared PLAN/BINARY_PLAN survive retained parameter execution while process-list BriefBinaryPlan stays empty. Existing sessions see GLOBAL OFF/ON. Non-fast disabled begins count, fast disabled begins do not; enabling before close records duration even without a matching begin. Compile failures and duplicate close remain protected by existing regressions. These are scoped behavioral checks, not whole-package acceptance or performance evidence.

## Idempotence and Recovery

Preserve concurrent remote integration work without merging it blindly. Keep the native checkout unchanged. Logs live under `/workspace/.cloud-setup/observation-plan-batch`. The unpublished Git bundle and draft must be updated only after validation and a clean local commit. No broad cache deletion or test suppression; disk cleanup may remove documented regenerable artifacts only.

## Surprises & Discoveries

Six baseline Rust cases failed independently; seven of thirteen real-wire assertions failed and the server exited cleanly. The first follow-up had 25 passes and a fixture failure: the test needed the explicit binary-protocol flag. With that corrected, the expanded group had 27 passes and one genuine SET failure. A temporary trace established user-variable SET was labeled `other`; the trace was removed after repairing the shared AST mapping. An isolated formatting attempt lacked toolchain activation and failed before editing executor source; rerunning with env.sh resolved it.

The first typed-pattern compile required dereferencing the AST NodeBox wrapper (four E0308 diagnostics); corrected without weakening the classification. An expanded fixture combining SET NAMES with a user-variable assignment hit an existing parser refusal. The final mixed-SET case uses the parser's existing canonical system-variable fixture, SET NAMES utf8mb4, autocommit=1; mixed user-variable parsing remains unaccepted and is not hidden as a passing case.

Go explicitly allows unmatched begin/finish across toggle windows. Brief process-list binary plans and statement-summary binary plans have different prepared-execution policies; sharing their publication gate loses valid summaries.

## Decision Log

- Decision: reuse existing tree renderers and codec; do not label brief output a normalized plan digest. Rationale: Go uses a distinct ExplainNormalizedInfo traversal. Date: 2026-10-04.
- Decision: classify administrative fast plans by typed AST variants, not the display label. Rationale: Go labels SET PASSWORD as Set but builds a separate non-fast plan; user-variable/charset/mixed SET forms map to ast.SetStmt. Date: 2026-10-04.
- Decision: retain full finding statuses as partial. Rationale: complete Go package obligations and other observation producers remain unresolved. Date: 2026-10-04.

## Outcomes & Retrospective

Five connected observation/configuration/label gaps are repaired. All 93 selected Rust cases and 13 real MySQL/unistore assertions pass; all-target checks, make lint and locked server build pass. The real server exits 0. No full finding/package is closed: O11/O18/N03 remain partial, with 57 unresolved roots overall. No push. The receipt records commands, source/log hashes, unsuccessful intermediate attempts and unverified scope. Final local hook, recovery bundle and draft outcomes belong to the Cloud final-handoff.json; fresh restoration remains unverified.

## Interfaces and Dependencies

Extend `ProcessPlanInfo` and `StmtContext` publication options; no dependency or native-client changes. `StatementStats` remains the sole execution counter owner and the summary store remains the sample-size policy owner.
