### What problem does this PR solve?

Issue Number: close #69927

Problem Summary:

NOT NOT a.c4 = 50 is logically equivalent to a.c4 = 50, but TiDB does not simplify the double negation before constraint derivation and left-join pruning. This causes a ~7.5x slowdown: the right table is scanned unnecessarily when the WHERE clause already contradicts the join condition.

### What changed and how does it work?

In pushNotAcrossExpr (pkg/expression/util.go), a single-line fix: when stripping an outer NOT (not=false) and recursing into the inner child, the early return condition previously returned expr (unchanged) which preserved the double negation. Now it returns childExpr instead — the simplified inner expression without the outer NOT wrapper.

This is safe because NOT NOT X is logically X (with is_true_with_null semantics already handled by wrapWithIsTrue).

### Check List

Tests

- [x] Unit test — added TestPushDownNotDoubleNegation in pkg/expression/notnot_test.go
- [x] Integration test — existing predicate pushdown testdata covers NOT NOT in join ON conditions
- [ ] Manual test (add detailed scripts or steps below)
- [ ] No need to test

Side effects

- [ ] Performance regression: Consumes more CPU
- [ ] Performance regression: Consumes more Memory
- [ ] Breaking backward compatibility

Documentation

- [ ] Affects user behaviors
- [ ] Contains syntax changes
- [ ] Contains variable changes
- [ ] Contains experimental features
- [ ] Changes MySQL compatibility

### Release note

```release-note
Fix planner simplification of NOT NOT predicates in WHERE clauses, enabling left-join right-side pruning and avoiding ~7.5x query slowdown.
```

🤖 Generated with [Claude Code](https://claude.com/claude-code)
