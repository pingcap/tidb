# `pkg/ddl/placement` parity audit ExecPlan

This living plan follows `PLANS.md` at the repository root and the DDL
execution guidance in `docs/agents/ddl/README.md`.

## Purpose / Big Picture

`pkg/ddl/placement` turns placement-policy metadata into PD rule bundles. The
13-artifact Go package is one atomic parity unit; its complete Rust owner,
metadata test carrier, YAML/PD boundaries, and builder API must retain the
current Go master behavior.

## Progress

- [x] Re-read current Go master `f2c346fe4f368ff855e17c1f62e28a89ba7f9723`:
  all 13 artifacts and 3,844 lines, including BUILD metadata, every
  production/test declaration, metadata helper, and all support inputs. No
  generated, platform-specific, fixture, benchmark, fuzz, example, or extra
  build-input artifact exists; the tree is byte-identical to the historical
  pin.
- [x] Read the complete 24-line Rust manifest, all 4,663 owner lines, every
  production/private function, inline test, metadata integration test,
  aggregate registration, workspace/lock entry, and direct builder caller
  before editing.
- [x] Reproduce the pre-fix mismatch: the Go-pointer-shaped regression failed
  on the old consuming setters with exactly four `E0382` moved-value
  diagnostics.
- [x] Change the four setters to `&mut self -> &mut Self`, remove the two
  discard-only constructor `#[must_use]` annotations, and update the two
  long-lived bundle callers while retaining fluent support.
- [x] Add and run the focused regression, then run the complete owner tests,
  all-target check, formatting, repository lint, and diff hygiene gates.
- [x] Update the package receipt, the `b050` cross-reference, and the global
  rolling ExecPlan.
- [x] Commit once for `pkg/ddl/placement`, rebase/push to
  `hparser-integration`, and verify the remote SHA `a11714f1504`.
- [x] (2026-09-06) Re-read the complete package owner and removed the lone
  Rust-only `#[must_use]` annotation from Go-shaped `GroupID`; a deny-on-
  discard regression failed pre-fix with one diagnostic and passes afterward.
- [x] (2026-09-06) Focused regression, all 30 owner tests, all-target check,
  formatting, Ready lint, and diff hygiene all pass.
- [ ] Publish the `GroupID` return-contract follow-up as one package-scoped
  commit, then continue the rolling package audit.

## Scope and decision

The implementation change is deliberately limited to the Go API contract:
mutating builder receivers and discardable constructor returns. Go callers can
write either `builder.SetRole(...)` or a fluent chain; both forms now work in
Rust. Bundle construction, role/count defaults, constraint parsing, policy
lookup, rule merging/tidying, key ranges, JSON output, and error identity are
not redesigned. The metadata test continues to use the documented in-memory
`PolicyGetter` boundary rather than inventing a storage implementation.

## Validation gate

Use the commands recorded in `rust/testport/receipts/ddl_placement.md`:
focused regression, full `tidb-placement` tests, all-target check, workspace
format check, `make lint` (Ready profile), and `git diff --check`.

No Go/import/Bazel/Cargo-module file changes, new Go tests, or Go file moves
are in scope, so `make bazel_prepare` is not required.

## Surprises & Discoveries

The old Rust fluent API looked equivalent until a caller ignored a setter:
consuming `self` moved the builder instead of mutating a Go-style pointer. Once
the receiver was corrected, the two long-lived bundle callers could not borrow
a temporary (`E0716`), so they were rewritten as explicit mutable bindings.
The short-lived chain remains valid through the returned mutable reference.

## Decision Log

- 2026-09-06: Treat `NewRuleBuilder` and the four `Set*` methods as pointer
  API counterparts, not Rust ownership conveniences; preserve both ignored
  and fluent call forms.
- 2026-09-06: Remove `#[must_use]` from `RuleBuilder::new` and `new_rule`
  because Go permits discarding those results.
- 2026-09-06: Keep the existing strict YAML, PD DTO, metadata, and bundle test
  carriers; no Go test or fixture changes are needed.
- 2026-09-06: Skip Go execution and `make bazel_prepare` for this Rust-only
  follow-up; the owner tests, compile gates, and Ready lint are proportional.

## Outcomes & Retrospective

Published as one package-scoped commit; after the latest upstream rebase the
remote `hparser-integration` SHA is `a11714f1504`. The rolling audit continues
with the next complete package boundary.
