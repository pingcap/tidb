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
- [x] (2026-09-06) Correct the regression from `8d42bcc7035`: under the
  requested Rust-only scope, read all 11 current Rust owner artifacts (5,066
  lines after the test addition), restore the focused `GroupID` discard test,
  and remove the one restored `#[must_use]` annotation.
- [x] (2026-09-06) Preserve exact fail-before evidence for one diagnostic;
  pass the focused test, all 30 owner tests, all-target compilation,
  standalone rustfmt, Ready lint, and diff hygiene afterward.
- [x] (2026-09-06) Prepare one corrective `pkg/ddl/placement` commit for
  normal rebase and publication to `origin/hparser-integration`.
- [x] (2026-09-07) Reconfirm that the 11-artifact, 5,066-line owner is
  byte-identical to the tree already read in full; re-inventory its manifest,
  lock entry, 30 tests, sole direct dependent and all callers before editing.
- [x] (2026-09-07) Remove the seven remaining direct Go-shaped scalar/struct
  return diagnostics, retain eleven inherent/native contracts, and prove the
  correction with an exact seven-diagnostic fail-before regression.
- [x] (2026-09-07) Pass all 31 owner tests, owner and `tidb-exec` all-target
  checks, scoped formatting, Ready lint, and diff hygiene; update both
  ExecPlans and the package receipt for one package-scoped publication.
- [ ] Continue the rolling Rust-only audit with the next package boundary.

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
- 2026-09-06: Treat `8d42bcc7035` as a restore regression: reapply only the
  discarded `GroupID` return contract and its focused test; do not change the
  already aligned bundle and builder behavior.
- 2026-09-07: Remove only the seven direct scalar/struct return attributes.
  Keep the eleven collection/`Option`, PD DTO conversion, and Rust error
  carrier annotations because those are native type/ownership boundaries.

## Outcomes & Retrospective

Published as one package-scoped commit; after the latest upstream rebase the
remote `hparser-integration` SHA is `a11714f1504`. The rolling audit continues
with the next complete package boundary. The corrective batch restores the
later `GroupID` contract without changing the prior package implementation.
The 2026-09-07 follow-up closes the remaining discardable scalar/struct return
contracts without changing any placement-policy runtime behavior.
