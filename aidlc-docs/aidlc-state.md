# AI-DLC State

- Current phase: Construction
- Current unit: runtime adaptive LIMIT scan controller
- Approval: the user explicitly requested implementation and at least ten E2E iterations
- Baseline commit: `8be4bd0`
- Last updated: 2026-07-11

## Scope

Implement an opt-in, statement-local controller for early-stop LIMIT queries. The
first supported shape is a LIMIT above an ordered IndexLookUpJoin whose outer
side is an IndexLookUpExecutor. The controller must react during the current
execution and must not depend on SQL digest history.

## Exit Criteria

- Feature gate defaults to OFF.
- ON bounds speculative outer and lookup work without changing result rows.
- User concurrency and batch settings remain hard ceilings.
- Targeted unit tests and build checks pass.
- At least ten isolated E2E rounds cover the issue shape and adverse cases.
