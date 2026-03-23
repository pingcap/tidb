# Conclude Record: toomanytests-generated-cases

## Source case

- PR context: `pkg/util/topsql/reporter` hit `toomanytests` (`Too many test cases in one package`).
- Trigger pattern: generated cases and many mechanical tests co-exist in one package.
- Concrete signals:
  - linter output pointed to `topru_generated_cases_test.go`
  - package-level Test count exceeded threshold

## worth_concluding

- `yes`
- This case repeats in test-heavy packages and is suitable for routing guidance.

## destination_kind

- `skill`

## strongest_routing

- primary: `skills/test/test-style.md`
- secondary: `skills/test/test-comment.md`

## new_vs_update

- `update-existing-skills`
- No new capability is required; this is routing + delta refinement over existing testing-style and test-comment skills.

## reusable_deltas

- [Routing rule]: `generated_cases` hit is a weak signal, not a sufficient condition. Route by current package structure first: if tests are already structurally converged, do not force additional merges.
- [Fix strategy]: prioritize structural convergence of top-level `Test*` entries (table-driven / `t.Run` clustering) before any linter threshold or exception path.
- [Scope guard]: when PR scope cannot reasonably touch unrelated legacy tests, allow threshold/exception only as constrained fallback with explicit rationale.
- [Naming rule]: after clustering, keep subtest names scenario-descriptive and directly runnable for triage (`-run Parent/Subcase`).
- [Safety rule]: preserve assertions and behavior; only reorganize test entry structure. Prefer heuristic refinement in tooling (for false positives) over introducing issue-specific one-off skills.

## tooling impact

- Affected analyzer: `build/linter/toomanytests/analyze.go`
- Related heuristic direction: `fix_test_style.py` false-positive refinement should treat `generated_cases` as a hint, then verify whether structure is already converged before suggesting further consolidation.
- Practical decision path in this case:
  - benchmark consolidation alone did not reduce `toomanytests` (analyzer counts `Test*`, not `Benchmark*`);
  - package-specific threshold override was used for `pkg/util/topsql/reporter` due to scope constraints (fallback, not primary recommendation).

## verification summary

- Verified analyzer behavior against source code (`Test*` prefix counting, package threshold rule).
- Confirmed route decision and priority:
  - structural convergence is the primary decision axis;
  - `generated_cases` presence alone does not imply mandatory merge work;
  - style-level guidance reused from existing test-style skill;
  - wording/rationale guidance reused from test-comment skill.
- Outcome: no new issue-specific skill created.

## why not a new skill

- A dedicated `skills/test/toomanytests.md` would mostly duplicate:
  - test organization patterns already covered by test-style guidance;
  - rationale/comment patterns already covered by test-comment guidance.
- This case is a routing composition problem, not a new standalone workflow.
- Creating a new skill here would increase maintenance cost and fragment ownership.

## follow-up

- If 2+ future cases show the same routing friction, add a short subsection in `skills/test/test-style.md`:
  - “Handling toomanytests in package-scoped constraints”.
- Keep this conclude record as the traceable example for that future update.
- If similar cases repeatedly rely on threshold/exception instead of structural convergence, re-evaluate whether guidance should primarily live in workflow/tooling guidance rather than continuously expanding test-style scope.
