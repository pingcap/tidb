# Planner Case Map

## pkg/planner/core/casetest/rule

- predicate_pushdown_suite_in.json
  - TestConstantPropagateWithCollation: predicate simplification + results; includes outer join OR-constant case (`/* issue:65994 */`).

## pkg/planner/core/issuetest

- planner_issue_test.go
  - rollup-having-exists-nil-expression: regression for `GROUP BY ... WITH ROLLUP` + `HAVING EXISTS` nil-expression panic (`/* issue:66165 */`).
