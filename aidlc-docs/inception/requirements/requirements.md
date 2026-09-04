# Requirements: UNION `inUnion` cast parity

This brownfield work unit aligns the Rust expression and planner path with
Go `master`'s `BuildCastFunction4Union` behavior. The user explicitly scopes
implementation to Rust while using Go production/tests/fixtures/generated
and build metadata as the read-only oracle.

## Acceptance criteria

1. A UNION-specific unsigned cast carries an explicit internal marker and is
   not confused with user-written `CAST(... AS UNSIGNED)`.
2. Negative signed integer values reaching an unsigned UNION cast evaluate to
   zero, matching Go's `builtinCastIntAsIntSig` `inUnion` branch.
3. UNION projection planning uses the marked cast helper; ordinary cast
   wrappers retain their existing behavior.
4. Focused regressions cover expression evaluation and planner shape.
5. The Ready validation profile is run and recorded with pre-existing warning
   and failure boundaries.
6. The parity receipt and ExecPlan list complete inventories and retain the
   remaining Go `string AS DECIMAL`/vectorized gaps honestly.
