# Shared DML writable rows and explicit handles


## Purpose and outcome


INSERT must preserve an explicitly supplied `_tidb_rowid` alongside hidden expression-index columns. Joined UPDATE and DELETE must retain the complete writable row, and alias merging must preserve generated values from prior writes. Foreign-key checks and index maintenance consume the same retained rows. This connected E03/K03/E02 maintenance follows Go's writable-column and row ownership; it does not accept complete planner/executor/table packages.

## Context and milestones


Cloud checkouts are /workspace/tidb on hparser-integration at 92031a2954864d54a47df846adeea7151b66c93e and /workspace/client-rust on master at 8b752f9638ad157931725b66ffdc57e0465432a9. Go master refreshed to the unchanged 3ca96b1d5df8da123e7a650512654eedab12c861. Native source is outside this batch. Read root AGENTS.md and PLANS.md before edits.

First reproduce the connected failures using existing session suites and the current real MySQL/unistore server. Then repair rust/crates/tidb-executor/src/driver/dml.rs, multi_dml.rs and their shared metadata/record consumers. Go source owners are pkg/planner/core/logical_plan_builder.go buildUpdateTblColPosInfos/buildDelete, pkg/executor/update.go updateRows/mergeGenerated, delete.go composeTblRowMap, and insert_common.go fillRow/adjustImplicitRowID. Keep visible SQL names separate from complete writable rows, and keep explicit candidate handles separate from stored columns. Go replace.go::replaceRow and insert_common.go::batchCheckAndInsert check the candidate record key before unique-index conflicts.

The final milestone validates all connected callers together, updates both finding registers and their receipt, and publishes through the actual hook plus a fresh locked server build. Preserve concurrent changes; use normal pushes only.

## Progress


- [x] Refresh both remotes and inspect shared ownership; real-server probes reproduce generated-handle corruption, joined row-width errors and partition deduplication.
- [x] Record three original fail-before session regressions and the later failing index-join regression. Two additional forced duplicate-handle probes were excluded after Go source review showed the same bare-handle map.
- [x] Implement complete joined preimages, generated alias merging and separate explicit handles across INSERT, REPLACE, IGNORE, ODKU, FK and shared record consumers.
- [x] Run grouped tests, affected checks, real SQL verification and make lint; update durable evidence.
- [ ] Publish after validation through the actual hook and a fresh locked build; record remote SHA and Cloud readback in /workspace/.cloud-setup/dml-identity-batch/final-handoff.json. The source receipt records pre-publication evidence, and that external handoff records the later publication result.

## Surprises & Discoveries


The explicit handle currently occupies the first hidden generated-column offset. Generation overwrites it and insertion then truncates the stored row at that offset. Joined layouts use visible columns while their retained physical child emits hidden columns too. Forced duplicate handles across partitions also reproduce a deduplication issue, but Go BuildHandleByDatums returns a bare IntHandle and its DML maps use that handle. No Rust-only semantic change is accepted from that probe. The AutoID service finding K01 was inspected but remains outside this repair: its missing production transport and separate allocator ownership cannot be solved by changing a cache step.

## Decision Log


REPLACE also failed on the original server without an expression index: explicit handle 7 returned 1062. The shared conflict owner now checks the candidate physical record key. ODKU evaluates visible values and its handle separately from hidden generated values. Final TCP validation also reproduced an index-join reader that looked up hidden outputs in visible-only metadata. A new session regression fails with the same error. Repair driver/physical_builder.rs schemas/offsets and access_path.rs filter/fork/probe/reconstruction together, following Go builder.go::buildIndexLookUpJoin. Explicit UPDATE of the pseudo handle remains unsupported, matching the existing single UPDATE limitation; rejecting it avoids silently overwriting a generated column. This is a retained parity gap, not a repaired feature.

Reuse existing session tests and server startup. Preserve Go-contract assertions and do not introduce another general harness. Retain memory accounting and statement rollback. Resolve SQL names using visible metadata; hidden generated columns must never become user-addressable merely to repair internal layout.

## Validation and recovery


Activate /workspace/.cloud-setup/env.sh and set CARGO_BUILD_JOBS=1. Run Cargo from /workspace/tidb/rust. Baseline command: cargo test --locked -p tidb-session --lib -- dml_identity_batch --test-threads=1. Group the final multi-table DML, expression-index, foreign-key and statement rollback filters in one invocation. Run affected all-target checking and root make lint once after source review. Reuse the existing MySQL/unistore binary for baseline probes and the final built binary for post-fix checks; stop only owned server processes. Logs are /workspace/.cloud-setup/dml-identity-batch. Actual executable hooks/pre-commit selected by core.hooksPath=hooks must pass cargo build --locked -p tidb-server. Run that exact build again immediately before every push. Recover through retained diffs and commits, never reset another contributor's work.

## Outcomes & Retrospective


The first grouped after-run passed 187 cases and failed one REPLACE assertion; that connected record-key gap is now included. A later compile-time Datum constructor typo was corrected. Final validation: 286 Rust cases and 20 TCP assertions pass; affected all-target checking, make lint and locked server build pass. See parity/current-audit/dml-identity-batch-validation.json for exact commands, hashes and limits. No full Go suite, multi-node TiKV, complete package or performance acceptance is claimed.
