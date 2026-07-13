# Runtime Adaptive LIMIT Scan Requirements

## Functional

1. Add `tidb_enable_adaptive_limit_scan`, global/session scoped, default OFF.
2. Activate only for an exact LIMIT demand above an ordered IndexLookUpJoin.
3. Track produced, outer fetched, outer consumed, reserved, and stopped state in
   one statement-local controller.
4. Bound future outer and lookup work using current-execution feedback.
5. Stop issuing future work when LIMIT demand is satisfied.
6. Preserve complete SQL results and errors; the controller may delay or cancel
   speculative work but may never truncate required input.

## Non-functional

- No digest key, TTL, cross-statement cache, or persisted profile.
- Session concurrency and batch variables are ceilings.
- Disabled behavior has no controller allocation or scheduling change.
- Low-selectivity scans continue making progress when fetched rows are consumed.
- Runtime state is concurrency-safe and local to one executor tree.

## Acceptance

- Regression unit tests demonstrate reservation, feedback growth, shrink, stop,
  and cancellation behavior.
- The issue-shaped E2E case shows materially lower scan/selection work on the
  first ON execution than OFF while returning identical rows.
- Ten E2E rounds include low selectivity and larger LIMIT safeguards.
