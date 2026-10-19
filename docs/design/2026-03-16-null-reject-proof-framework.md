# Design: Structural Null-Reject Proof under Three-Valued Logic

- Author(s): [Yiding Cui](https://github.com/winoros)
- Discussion PR: https://github.com/pingcap/tidb/pull/67129
- Tracking Issue: https://github.com/pingcap/tidb/issues/66825

## Table of Contents

* [Introduction](#introduction)
* [Motivation or Background](#motivation-or-background)
* [Detailed Design](#detailed-design)
  * [Core Definitions](#core-definitions)
  * [Proof Rules under Three-Valued Logic](#proof-rules-under-three-valued-logic)
  * [Null-Preserving Builtins and Opaque Functions](#null-preserving-builtins-and-opaque-functions)
  * [`IN` as a Special Form](#in-as-a-special-form)
  * [Stable Folding versus Unstable Folding](#stable-folding-versus-unstable-folding)
  * [Implementation Mapping](#implementation-mapping)
  * [Representative Counterexamples](#representative-counterexamples)
* [Test Design](#test-design)
  * [Functional Tests](#functional-tests)
  * [Scenario Tests](#scenario-tests)
  * [Compatibility Tests](#compatibility-tests)
  * [Benchmark Tests](#benchmark-tests)
* [Impacts & Risks](#impacts--risks)
* [Investigation & Alternatives](#investigation--alternatives)
* [Unresolved Questions](#unresolved-questions)

## Introduction

This document describes a structural proof framework for null-reject checks used by outer join simplification. The goal is to replace evaluation-oriented heuristics with a conservative proof model that remains sound under SQL three-valued logic.

## Motivation or Background

Outer join simplification needs to answer a precise question:

> After every inner-side column is replaced with SQL `NULL`, can a predicate still evaluate to `TRUE`?

If the answer is no, the predicate is null-rejected and the outer join can be safely simplified to an inner join.

Historically, TiDB relied more on evaluating expressions after nullifying the inner schema. That approach is fragile for predicates that are sensitive to three-valued logic, especially expressions involving `IN`, `COALESCE`, `IS TRUE`, `IS FALSE`, and nested boolean operators. It also tends to blur the boundary between:

- "this predicate is proven to be null-rejected", and
- "this predicate could not be evaluated precisely enough".

The framework in this document intentionally favors correctness over aggressiveness:

- if a proof succeeds, the result must be sound;
- if the proof cannot be established, the optimizer should fall back to "do not simplify".

This document does not try to make every builtin provable. It only defines a reliable core that can be extended conservatively.

## Detailed Design

### Core Definitions

Let `E` be a boolean expression and `x` be a variable from the null-producing side of an outer join.

`E` is null-rejected on `x` if `E` cannot evaluate to `TRUE` after substituting `x = NULL`.

To reason soundly under SQL three-valued logic, a single proof target is not enough. The framework tracks two related but distinct properties:

- `nonTrue(E)`: `E` cannot evaluate to `TRUE`; it can only be `FALSE` or `NULL`.
- `mustNull(E)`: `E` must evaluate to `NULL`.

These two properties are not interchangeable. For example:

- `NOT(NULL) = NULL`
- `NOT(FALSE) = TRUE`

Therefore, knowing that a child expression is `nonTrue` does not imply that `NOT(child)` is also `nonTrue`. For `NOT`, the stronger `mustNull` proof is required.

### Proof Rules under Three-Valued Logic

All rules below are intentionally conservative. If a sub-expression cannot be proven, the framework does not guess.

#### Base Cases

- Any inner-side column becomes `NULL` in the nullified world.
  - Therefore an inner column contributes both `nonTrue` and `mustNull`.
- Constants are classified directly by value.
  - Constant `NULL` contributes both `nonTrue` and `mustNull`.
  - Constant `FALSE` contributes `nonTrue`.
  - Constant `TRUE` contributes neither.
- Unclassified or unprovable expressions contribute neither property.

#### `AND`

For `E = A AND B`:

- `nonTrue(E) = nonTrue(A) OR nonTrue(B)`
- `mustNull(E) = mustNull(A) AND mustNull(B)`

Rationale:

- If either side cannot be `TRUE`, the whole conjunction cannot be `TRUE`.
- But `mustNull` is stricter. `NULL AND FALSE = FALSE`, so both sides must be strong enough to force `NULL`.

#### `OR`

For `E = A OR B`:

- `nonTrue(E) = nonTrue(A) AND nonTrue(B)`
- `mustNull(E) = mustNull(A) AND mustNull(B)` as a conservative sufficient condition

Rationale:

- If either side may still be `TRUE`, the disjunction may still be `TRUE`.
- Likewise, `TRUE OR NULL = TRUE`, so `mustNull` only holds when both sides are strong enough.

#### `NOT`

For `E = NOT A`:

- `nonTrue(E) = mustNull(A)`
- `mustNull(E) = mustNull(A)`

This is the main reason `mustNull` must be tracked explicitly.

#### `IS TRUE` / `IS FALSE`-Style Tests

Boolean tests that turn `NULL` into a definite boolean result need explicit classification.

Two useful categories are:

- `f(NULL) = FALSE`
- `f(NULL) = NULL`

Both categories can derive `nonTrue` from the child's `mustNull`. Only the second category can also derive `mustNull` for the parent.

### Null-Preserving Builtins and Opaque Functions

Many builtins are null-preserving: once any argument is `NULL`, the result is `NULL`.

For `E = F(arg1, arg2, ..., argn)`, if `F` is null-preserving, then:

- if any argument is `mustNull`, `E` is `mustNull`;
- once `E` is `mustNull`, it is also `nonTrue`.

This is why many comparisons, arithmetic operators, string functions, and date functions can participate in structural proof.

By contrast, functions such as `COALESCE` and `IFNULL` may hide `NULL`. They require dedicated local rules, or must be treated as opaque.

The implementation follows this policy:

- null-preserving builtins may propagate `mustNull`;
- `COALESCE` / `IFNULL` are handled by dedicated logic;
- unregistered builtins are treated as opaque.

This reduces optimization opportunities, but it prevents incorrect join simplification.

### `IN` as a Special Form

`IN` should not be handled as a generic comparison.

For example:

```sql
a IN (b, c, d)
```

Under three-valued logic:

- if `a` is `NULL`, the result is `NULL`;
- if all candidate comparisons collapse to `NULL`, the result is also `NULL`;
- but if any candidate may still produce a match, `nonTrue` cannot be concluded.

For that reason, `IN` needs its own proof rule rather than being naively expanded into a disjunction of equalities.

### Stable Folding versus Unstable Folding

Another common temptation is to nullify the inner schema and then fold the expression into a constant.

That is only sound when the entire folding path is stable. The proof must explicitly reject nodes such as:

- `ParamMarker`
- `DeferredExpr`
- any other node whose value is not stable at proof time

If a sub-expression cannot be folded safely, the framework should stop folding and fall back to structural proof or conservative failure.

### Implementation Mapping

The current implementation can be read in layers:

- `proveNullRejected(...)`
  - recursively computes `nonTrue` and `mustNull`
- `tryFoldNullifiedConstant(...)`
  - attempts to fold an expression in the nullified inner-schema world
- `nullRejectNullPreservingFunctions`
  - records builtins that preserve `NULL`
- `nullRejectRejectNullTests`
  - records boolean tests such as `IS TRUE` / `IS FALSE`

This structure has several benefits:

- the rules are explicit and reviewable;
- adding builtin support becomes a classification problem instead of scattered ad hoc special cases;
- proof failure naturally degrades to "do not simplify".

### Representative Counterexamples

#### Counterexample 1: `NOT` cannot rely on `nonTrue` alone

```sql
NOT ((x = 1) AND FALSE)
```

When `x = NULL`:

- `(x = 1) = NULL`
- `NULL AND FALSE = FALSE`
- `NOT FALSE = TRUE`

So the expression is not null-rejected.

#### Counterexample 2: a null-preserving outer layer does not help if the inner layer already hides `NULL`

Even when an outer function is null-preserving, that alone does not make the whole expression null-rejected. Some argument must still be provably `NULL`.

If an inner expression has already hidden `NULL` and collapsed to a concrete value, the outer layer cannot recover the missing proof.

## Test Design

### Functional Tests

The implementation should be covered by both unit tests and SQL-level regression tests.

Unit tests should verify:

- core `nonTrue` / `mustNull` propagation rules;
- builtin registry assumptions;
- special handling for `IN`, `COALESCE`, and boolean tests such as `IS TRUE` / `IS FALSE`.

### Scenario Tests

Regression coverage should include real optimizer scenarios where wrong null-reject judgment changes join semantics or plan-cache behavior, including:

- outer join simplification cases that must remain outer joins;
- cases that must simplify to inner joins;
- parameterized prepared statements whose null-reject property is structurally invariant.

### Compatibility Tests

The design primarily affects planner behavior. Validation should check compatibility with:

- outer join simplification in planner rules;
- derived predicates and not-null reasoning that reuse null-reject checks;
- prepared plan cache decisions for parameterized predicates;
- integration tests that observe user-visible result sets.

No parser, DDL, storage, upgrade, or downgrade compatibility changes are expected.

### Benchmark Tests

No dedicated benchmark is required for the design itself.

If the proof is expanded significantly in the future, planner CPU overhead should be measured against the previous evaluation-based approach.

## Impacts & Risks

### Impacts

- Improves correctness for outer join simplification under complex three-valued logic.
- Makes proof rules more explicit and easier to review.
- Provides a foundation for later work on parameter-invariant plan-cache eligibility.

### Risks

- The framework is conservative, so some simplification opportunities will be missed.
- Incorrect builtin classification can still introduce unsound results, so registry growth must remain disciplined.
- More structural proof logic may increase planner complexity and maintenance cost.

## Investigation & Alternatives

The main alternative is to continue using evaluation-based nullification:

- replace inner-side columns with `NULL`;
- evaluate the rewritten predicate;
- decide null-reject from the evaluated result.

That approach is simpler at first glance, but it becomes fragile once expressions involve nested boolean logic, `IN`, `COALESCE`, or nodes whose value cannot be folded stably.

Another alternative is to build a much more aggressive proof engine that understands more builtin semantics. This was not chosen because correctness matters more than optimization coverage in this area. The current design deliberately starts from a small, reviewable sound core.

## Unresolved Questions

- How far should builtin classification be expanded before the maintenance cost outweighs the optimization benefit?
- Should parameter-invariant proof be modeled explicitly as a separate layer for prepared plan cache decisions?
- Are there additional planner subsystems that should share the same proof artifacts instead of re-implementing narrower checks?
