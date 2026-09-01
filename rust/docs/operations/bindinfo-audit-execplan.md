# `pkg/bindinfo` parity audit ExecPlan

## Objective

Maintain a complete Go-master inventory for `pkg/bindinfo` and keep the
dependency-closed Rust binding owner behaviorally aligned. Read every Go
production, test, fixture, generated/platform variant, and build artifact
before editing; record the ownership boundary, measured regression, and Ready
validation in `rust/testport/receipts/bindinfo.md`.

## Completed this batch

1. Inventoried all 25 tracked Go artifacts (7,917 lines): binding model and
   normalization, cache and lifecycle, automatic generation, operators, plan
   evolution/generation, JSON fixtures, package tests, nested SQL integration
   tests, and both Bazel targets. No generated or platform-specific Go
   artifact exists.
2. Compared binding selection and prepared execution in Go and Rust. A matched
   binding was selected in Rust, but nested prepared execution consumed the
   session's `found_in_binding` marker before the outer `EXECUTE` boundary
   published it.
3. Added `a_prepared_binding_is_published_when_the_plan_cache_is_disabled`;
   it failed before the change and now verifies `PrevFoundInBinding = 1`.
4. Re-armed the marker after cached-select, cached-DML, and fallback execution
   whenever a binding matched, including paths returning an error.
5. Keep automatic-binding persistence, manager/session integration, and the Go
   SQL integration harness as explicit boundaries until a dependency-closed
   Rust owner exists; do not add compatibility-only APIs.

## Validation gate

- [x] Focused regression fails before the fix and passes after it.
- [x] `tidb-session` binding owner suite (49 passed, 1 ignored).
- [x] Go unit package and nested integration-package compile check.
- [x] Workspace `cargo check --offline --locked`.
- [x] `cargo fmt --all -- --check`.
- [x] Ready profile `make lint`.
- [ ] Fetch remote, create one meaningful batch commit, push to
      `origin/hparser-integration`, and verify `rev-list` is `0 0`.

## Remaining boundaries

The Go package is broader than the Rust owner. Automatic binding persistence,
full manager/session lifecycle, and nested integration tests are not yet
dependency-closed in Rust. The prepared-plan-cache selector also exposes an
unrelated parallel HashAgg panic in an existing test; it is not part of this
binding fix. The repository package loop continues after this receipt, so this
plan does not claim whole-repository completion.
