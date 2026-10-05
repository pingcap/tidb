# Retire disconnected planner rule models

This living ExecPlan follows root PLANS.md. Earlier cleanup receipts remain
indexed in parity/current-audit/README.md; Git preserves historical source.

## Purpose / Big Picture

Remove eight unused rule models and their nine private harnesses together.
Preserve real logical-plan rules and migrate the useful childless-TableDual
regression into an existing real-plan test. Reduce compiled inputs without
claiming measured speedup or complete Go package acceptance.

## Context and Orientation

Base 3781259475fb31754186f7b86b083bc0d4b656c5 on /workspace/tidb hparser-integration.
Go master is b36c940a4332c866d8b0e2afde88f5e7c2fd7fed; native client remains
cfafb1eb01594cecd5926fa63e57e2a6e20c7ee1. Retire rule_set, rule_type,
topn_push_down, derive_topn_from_window, push_down_sequence, condition_to_dual,
eliminate_empty_selection and eliminate_unionall_dual_item at the crate root.
Keep their real logical/ owners, whose names overlap. Go's rule wrappers invoke
shared LogicalPlan methods; private miniature trees do not implement that path.

## Progress

- [x] Refresh refs and trace scoped imports and owner symbols across tracked Rust sources.
- [x] Remove eight models, nine harnesses and 30 private tests (1,471 source/test lines).
- [x] Migrate the childless-TableDual case and remove stale ownership/coverage claims.
- [x] Grouped all-target check, migrated regression (1 passed), lint, 3,658 unchanged retained inputs and self-review.
- [ ] Normal commit with actual locked-build hook, fresh build immediately before push, remote verification and Cloud handoff.

## Milestones and Plan of Work

Delete the complete private dependency group, including difftests outside the
owning crate. Retain real planner owners and their tests. Replace obsolete
adapter claims in mixed historical receipts without discarding original Go
obligations. Record deletion hashes and upstream anchors in the cleanup receipt.

## Validation and Acceptance

Source /workspace/.cloud-setup/env.sh in each shell. From rust/ run the affected
planner, difftest-planner-tests and server all-target locked check together.
Run the existing a_sequence_is_pushed_through_a_unary_operator test containing
the migrated TableDual case. From root run make lint and git diff --check.
Verify retained inputs: only eight lib declarations, five comment-only source
edits and the additive existing-test extension may differ. Fresh generated
registrations must exclude all nine deleted harnesses. Normal commit must run
hooks/pre-commit through core.hooksPath=hooks and pass cd rust && cargo build
--locked -p tidb-server; repeat immediately before every authorized push.

## Surprises & Discoveries

The real UnionAll owner's comment alleging divergent adapter change flags was
stale: an earlier adapter repair had already fixed the flag. Remove the stale
claim, not the real rule. The childless-TableDual guard exercised a toy tree;
move its useful shape to the real rule's existing unary-sequence regression.

## Decision Log

Keep behavioral and Rust correctness tests on retained implementations. Delete
private tests with their unreachable models. No source behavior, dependencies,
maintained scripts or original Go tests change. Decision date: 2026-10-05 UTC.
The migrated regression establishes structural coverage, not the Go SQL test's
full pipeline coverage. No new harness or permanent script is introduced.

## Outcomes & Retrospective

Evidence: parity/current-audit/rule-model-cleanup-validation.json. Registers
remain 86 tracked / 30 repaired / 56 unresolved: cleanup is not finding closure.
Before-images and final publication handoff: /workspace/.cloud-setup/rule-model-cleanup.
Recover individual files with git show 3781259475fb31754186f7b86b083bc0d4b656c5:<path>
into a temporary file before reviewing restoration. Preserve concurrent work;
never reset or force-push. Full Go suites, live TiKV and performance are unverified.
