# Planner Case Map

## pkg/planner/core/casetest/rule/testdata

- predicate_pushdown_suite_in.json
  - TestConstantPropagateWithCollation: predicate simplification + results; includes outer join OR-constant case (`/* issue:65994 */`).

## pkg/planner/core/issuetest

- planner_issue_test.go
  - right-outer-join-view-rollup-runtime-panic: regression for RIGHT JOIN + view + ROLLUP crash (`/* issue:66170 */`).
  - rollup-having-exists-nil-expression: regression for `GROUP BY ... WITH ROLLUP` + `HAVING EXISTS` nil-expression panic (`/* issue:66165 */`).
