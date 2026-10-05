# Shared write diagnostics and UPDATE source rows

## Purpose


Follow Go master 93a01d31f6da205ae4bf376825293903a6899fdb in the shared datatype/table conversion pipeline and both single-table and joined UPDATE callers. Charset failures must retain Go's converted bytes and error precedence. Binary and temporal numeric conversions must retain typed errors and ordered warnings. UPDATE diagnostics must identify the actual source row. This batch repairs related K03/E03 residuals; it does not accept whole Go packages or close these broad findings.

## Context and milestones


The Cloud checkout is /workspace/tidb on hparser-integration at d521ef775635f0a0c5fc2291a1e13e467611855b. Native /workspace/client-rust master is unchanged at 06b4ccc2735ecf89ed57136241cb7b7d204d6c07. Both refs match their freshly fetched remotes. The Go source export is /workspace/.cloud-setup/go-master. Read pkg/types/datum.go, binary_literal.go, pkg/table/column.go and pkg/executor/update.go; existing Rust source owners are tidb-datatype/src/datum_convert.rs, tidb-executor/src/driver/write_cast.rs and driver/{dml,multi_dml}.rs. No package doc.go exists in the touched upstream packages.

First extend existing tests and capture grouped failures. Then connect all selected conversion/caller paths and remove their obsolete duplicate diagnostic branch. Finally run grouped owner tests and SQL mutation tests, all-target checking and make lint. Source scope is existing-owner maintenance, not package transcreation. Retained handle metadata, partial-row reconstruction, streaming writes, JSON/temporal-target diagnostics and generated-expression context remain separate obligations.

## Progress


- [x] Refresh both remotes and identify Go producer/caller contracts.
- [x] Add grouped regressions in existing suites.
- [x] Capture four Rust and eight MySQL failures; implement selected paths together.
- [x] 71 distinct Rust cases, ten MySQL assertions, all-target checking, lint and server build pass; self-review and both registers updated.
- [ ] Commit through actual locked server build hook, rebuild immediately before normal push and verify remote SHA; save reusable checkpoint.

## Validation and recovery


Source /workspace/.cloud-setup/env.sh and run Cargo from /workspace/tidb/rust with CARGO_BUILD_JOBS=1. Baseline command: cargo test --locked -p tidb-executor --lib write_diagnostics_batch -- --test-threads=1. After repairs run scoped datatype conversion and executor write/UPDATE suites, cargo check --locked for affected crates --all-targets, and make lint from the repository root. Never bypass hooks; cargo build --locked -p tidb-server is mandatory in the actual precommit hook and immediately before every push. Preserve concurrent changes and never force-push. Logs reside in /workspace/.cloud-setup/write-diagnostics-batch. Revert only this batch's changes if interrupted; do not reset the worktree or clean dependency caches.

## Surprises & Discoveries


Go convertToString skips width production after a charset decoding error. The previous adapter incorrectly continued width checks and unconditionally returned charset errors for lenient mutation callers. Both UPDATE callers passed row index zero for assignment conversion despite retaining source-row ordinals.

## Decision Log


Use the existing contextual datatype owner and warning sink, extending existing source tests under the repository test-guidelines skill. Keep completion rules at INSERT/UPDATE/ODKU callers. Do not add a new harness or remove meaningful regressions. Restrict acceptance to reproduced behavior; preserve unrelated evidence.

## Outcomes & Retrospective


The selected write-path failures are repaired and validated in one batch. Four Rust regressions fail before; 71 distinct Rust cases and ten MySQL/unistore assertions pass after. The broader suite exposed an obsolete executor preimage-replay assertion: its CHECK contract now passes in the session rollback suite and over MySQL, and the old harness is removed. The receipt rust/docs/parity/current-audit/write-diagnostics-batch-validation.json records exact commands, source hashes and limits. K03/E03 remain partial; counts remain 86 tracked, 30 repaired, 56 unresolved. No performance or full-package acceptance claim. Publication still requires the actual hook and immediate pre-push locked build.
