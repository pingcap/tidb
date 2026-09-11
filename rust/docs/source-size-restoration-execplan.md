# Restore the original Rust source-size gate

This living plan follows repository `PLANS.md` and implements item 5 of
`BLOCKER_RESOLUTION.md`. Keep its progress and evidence current.

## Purpose / Big Picture


Restore the deleted source-size gate and split oversized files by responsibility
without changing Go-derived semantics or hiding violations in a whitelist.
Success is an executable check returning `source-size ratchet: OK`, with the
same tests and public APIs remaining available after file moves.

## Progress


- [x] (2026-09-11) Restore the original script, empty bounds table and Cargo test entry.
- [x] Run the restored check: 92 NEW-HUGE files, exit 1.
- [x] (2026-09-11) Separate statement-summary, placement-bundle and table-model tests from production modules; 432 crate tests passed.
- [x] (2026-09-11) Split model/job_args.rs into shared codec infrastructure, schema/table/partition arguments and alteration/index arguments; 327 model tests passed.
- [x] (2026-09-11) Split session/show.rs into dispatch/column metadata, CREATE formatting and statistics query modules; baseline comparison retained below.
- [ ] Split remaining named file: session/tests_partition.rs.
- [ ] Split remaining current violations reported by the unchanged gate.
- [x] (2026-09-11) Validate first three affected crates, workspace check and lint.
- [ ] Validate later moves and pass the full source-size gate (87 current violations remain).

## Surprises & Discoveries


The original six violations are still oversized. Commit 431d637dec deleted the
script and its Cargo integration; the current repository has 92 violations.
The bounds file immediately before deletion was empty apart from comments.
Restoration must not convert those violations into exemptions.

## Decision Log


Decision (2026-09-11): restore the exact pre-deletion gate and Cargo integration.
The user explicitly requires that gate; its absence is not a passing result.

Decision (2026-09-11): move inline test modules first where they are an existing
responsibility boundary. Keep their module names, visibility, test bodies and
parent access unchanged using explicit path attributes. This makes both halves
smaller without inventing a production API or changing Go-derived algorithms.

Decision (2026-09-11): retain job_args.rs as the shared V1/V2 decoding and dynamic
argument foundation. Move schema/table/partition arguments to job_args_schema_table.rs
and mutation/index arguments to job_args_alter.rs, re-exporting both through the
existing job_args module. Both moved bodies are byte-identical to the originals;
shared private helpers remain accessible to their child modules.

Decision (2026-09-11): move SHOW CREATE formatting to show_create.rs and SHOW
statistics methods to show_statistics.rs. Only entry points called from the
parent gain pub(super), preserving their privacy outside SHOW. Compare full
session tests to the original file because this crate already has failing tests.

## Context and Orientation


Work from repository root `/tmp/tidb-hparser-current`. Rust workspace is `rust`.
The checker reads all `rust/crates/**/*.rs` and rejects unlisted files over 2200
lines. It is also invoked by the `source_size_ratchet` integration target in
`rust/difftests/result-tests/Cargo.toml` (autotests is false, so registration matters).
Go master source of truth remains fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85.
No whole Go package transcreation claim follows from reorganizing Rust files.

## Plan of Work


Milestone 1 restores scripts/check-source-size.sh, scripts/source_size_bounds.txt
and difftests/result-tests/tests/source_size_ratchet.rs from 431d637dec's parent.
Run the shell checker to retain a real failing inventory.

Milestone 2 moves inline tests from crates/tidb-stmtsummary/src/statement_summary.rs,
crates/tidb-placement/src/bundle.rs and crates/tidb-model/src/table.rs into sibling
files, retaining the original logical modules. Run each affected crate's tests.

Milestone 3 separates session SHOW responsibilities, partition test groups and
model job argument families into modules. Preserve exports and private access
within the owning module, and retain every upstream test and fixture.

Milestone 4 applies the same responsibility-based procedure to the remaining
checker inventory. Do not whitelist new files or raise the 2200-line limit.

## Concrete Steps


From repository root:

    bash rust/scripts/check-source-size.sh
    RUSTFLAGS='' RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-placement -p tidb-stmtsummary -p tidb-model
    RUSTFLAGS='' RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo check --manifest-path rust/Cargo.toml --workspace
    make lint
    git diff --check

Repeat affected crate tests after later moves. The full gate is expected to fail
until every current violation has been split; record remaining counts honestly.

## Validation and Acceptance


File movement must preserve test names, counts, assertions and exported paths.
Compare extracted test names and assertions against the original; rustfmt may
reflow expressions after removal of the enclosing indentation.
All affected crate tests must pass; any unrelated failures remain open and must
be diagnosed. Final acceptance requires the full shell gate and Cargo target to
pass, not just the six originally named files dropping below the threshold.

## Idempotence and Recovery


All check commands are repeatable. Before a move, inspect git status and the
current module boundary. Use patches that require the expected original text.
Do not reset the worktree or discard unrelated changes. If a module no longer
compiles, fix imports and visibility without changing runtime behavior.

## Artifacts and Notes


Initial restored failure: `/tmp/source-size-restored-red.log`, 92 NEW-HUGE entries.
Original restoration source: `git show 431d637dec^:<repository-relative-path>`.
After the first three moves: `/tmp/source-size-after-three.log`, 89 NEW-HUGE entries.
The restored Cargo target ran one test and failed on those violations, exit 101:
`/tmp/source-size-cargo-red.log`. This is expected unfinished work, not a gate pass.
Crate tests: `/tmp/source-size-three-crates.log`, 432 passed, zero failed/ignored.
Workspace check: `/tmp/source-size-workspace-check.log`, exit 0.
Lint: `/tmp/source-size-lint.log`, exit 0.
Job-argument split: `/tmp/job-args-split-test.log`, 327 passed, zero failed/ignored.
Source-size after this split: `/tmp/job-args-source-size.log`, 88 NEW-HUGE entries.
Lint after this split: `/tmp/job-args-lint.log`, exit 0.
Workspace check after this split: `/tmp/job-args-workspace-check.log`, exit 0.
SHOW split: parent/create/statistics sizes are 1820/662/976 lines. Moved code is
identical after ignoring whitespace and pub(super). Workspace check and lint
passed (`/tmp/show-split-workspace.log`, `/tmp/show-split-lint.log`). Full session
tests failed: 1533 passed/148 failed/209 ignored in --lib, and 305 passed/2 failed
in --test all. Restoring the original SHOW file and rerunning produced 1535
passed/146 failed/209 ignored and the same two integration failures
(`/tmp/show-baseline-session-tests.log`). The two extra failures (SLEEP and
global embedding version) passed serially on both versions; logs are
`/tmp/show-baseline-two-serial.log` and `/tmp/show-split-two-serial.log`.
The final worktree restores the split implementation. These are unfinished
session failures, not evidence of a fully passing crate.

## Interfaces and Dependencies


No new dependencies. Parent modules remain public exactly as before. Test modules
retain `#[cfg(test)]`, their original names and visibility via `#[path = ...]`.
Production splits should use explicit re-exports to preserve caller paths.

## Outcomes & Retrospective


Restoration and the first three named splits are complete. Production/test file
sizes are statement_summary 1841/1805, bundle 952/1341 and table 1761/849 lines.
All retain their original logical test-module names and public API paths.
Job arguments now occupy 859 lines in the parent, 1355 in schema/table/partition
arguments and 1418 in alteration/index arguments. The size gate remains failing
on 87 files after the SHOW split. Full session tests expose existing failures
that also need Go-master-based fixes. Module splits and final verification are
in progress. Do not report this work as a completed size gate.
