# Consolidate the parser differential harness

This living ExecPlan follows root PLANS.md.

## Purpose and Context

Continue batched cleanup from 2d76b05d49b36125a4a619210ceb78e75d98ee38.
Go master remains 3ca96b1d5df8da123e7a650512654eedab12c861.
The parser test package adds no dependencies needed beyond the shared difftest
library. A separate integration test runs a source-inventory generator during
normal Cargo validation; it proves no runtime or planner parity.

## Progress

- [x] Inspect dependencies, corpus paths and all callers; reproduce baseline.
- [x] Move all three suites and their aggregate byte-identically to difftest.
- [x] Remove redundant package and automatic inventory test; update commands.
- [x] Compare all behavioral outcomes, validate locked metadata and run lint.
- [ ] Record evidence, commit through hook, build before push and verify remote.

## Milestones and Plan of Work

Move parser-tests/tests/*.rs into difftests/tests. Replace the inventory test
registration with the all target; remove parser-tests manifest/member/lock
entry. Keep all corpus, Go oracle, generator and expected-count bytes intact.
Keep the CR-before-newline harness correctness regression. Update regeneration
and current workflow commands to the shared crate and explicit inventory tool.

## Concrete Steps and Acceptance

Source /workspace/.cloud-setup/env.sh in each build shell. From rust/ run
cargo metadata --locked --no-deps --format-version 1 and
CARGO_BUILD_JOBS=1 cargo test --locked -p difftest --test all -- --test-threads=1.
Baseline under difftest-parser-tests has three passes and two existing failures:
integration_parser_static_go_oracle_reports_rust_outcomes and test_differential.
Compare exact diagnostics normalized only for moved paths and process IDs.
Run make lint from root, bash -n on regen-golden.sh and git diff --check.

Commit normally with the actual precommit hook's locked tidb-server build.
Immediately before authorized push repeat cargo build --locked -p tidb-server
from rust/, then push normally to pingcap/tidb hparser-integration and verify SHA.

## Surprises & Discoveries

The baseline integration replay reports 51,477 matches, 103 parse failures and
eight restore mismatches among 51,598 inputs. The curated corpus has five
existing divergences among 2,121 statements. Preserve these visible failures;
this cleanup does not implement parser fixes or revise expected outcomes.

## Decision Log

One fewer Cargo package and one net fewer integration binary; five behavioral
tests retained, one source-inventory-only automatic test removed. The explicit
generator and its --check/--write commands stay available. No measured speedup,
production behavior change, finding closure or whole-package acceptance.

## Outcomes & Retrospective

Validation complete: three passes and two identical baseline failures, including
all normalized diagnostics. Lint, metadata, continuity and shell checks pass.
Separate parser failures and statistics Go-master deltas
remain open; consolidation cannot establish behavioral parity.

## Recovery, Artifacts and Dependencies

Restore only affected paths from the base if needed, preserving concurrent
work. Evidence is in /workspace/.cloud-setup/parser-harness-cleanup; the durable
receipt is parity/current-audit/parser-harness-cleanup-validation.json.
Publication and Cloud checkpoint results belong in external final-handoff.json.
Saving a draft does not Publish or prove fresh-task restoration.
