# `pkg/meta/model` reorganization parity audit ExecPlan

This living plan follows `PLANS.md` at the repository root. The package-level
map and DDL semantics are documented in `rust/testport/receipts/meta_model_materialized_view.md`.

## Purpose / Big Picture

`pkg/meta/model/reorg.go` owns the persisted DDL reorganization metadata and
the dynamic worker/batch/write-speed controls consumed by DDL. It is part of
the atomic 23-artifact `pkg/meta/model` package; this follow-up aligns the
remaining Go-shaped return contracts in the complete `tidb-model` owner.

## Progress

- [x] Re-read current Go master `f2c346fe4f368ff855e17c1f62e28a89ba7f9723`:
  all 23 package artifacts and 10,721 lines recorded by the package receipt;
  the tree is unchanged from the prior authority pin and contains no
  generated, fixture, benchmark, fuzz, or platform artifact.
- [x] Read the complete Rust model owner, `reorg.rs` callers and tests,
  aggregate registration, and workspace metadata before editing.
- [x] Add the discard-contract regression and prove the pre-fix owner fails
  with exactly seven `unused_must_use` diagnostics.
- [x] Remove `#[must_use]` only from the seven direct Go-shaped constructor,
  getter, and predicate counterparts; retain Rust-only adapter diagnostics.
- [x] Run the focused regression and complete owner suite.
- [x] Run the all-target check, formatting, Ready lint, and diff hygiene;
  update the global rolling plan.
- [ ] Commit once for `pkg/meta/model`, rebase/push to
  `hparser-integration`, and verify the remote SHA.

## Scope and decision

This is a compile-contract correction only. `DDLReorgMeta` atomics, live
process-default callbacks, shallow-copy aliasing, JSON field merge order,
collation fallback, and `ReorgType` values remain unchanged. The Rust-only
`DDLReorgProcessDefaults::new` callback carrier and nullable receiver helper
are not Go symbols and keep their stricter annotations.

## Validation gate

The Ready gate is the focused regression, the full `tidb-model` owner suite,
`cargo check -p tidb-model --all-targets`, workspace `cargo fmt --check`,
repository `make lint`, and `git diff --check`. No Go/import/Bazel/Cargo-module
file changed, so `make bazel_prepare` is not required.

## Surprises & Discoveries

The existing reorganization tests already covered all runtime and JSON
semantics. The only remaining divergence was Rust's seven discard diagnostics;
the old owner fails at compile time before any test body runs, which makes the
deny-on-discard probe a precise regression.

## Decision Log

- 2026-09-06: Treat the seven listed methods as direct Go API counterparts and
  remove their `#[must_use]` annotations; do not weaken Rust-only adapters.
- 2026-09-06: Keep Go execution out of this Rust-only follow-up and use the
  existing source-derived model suite plus Ready lint for validation.

## Outcomes & Retrospective

Pending publication. After the package-scoped commit is pushed, record its
remote SHA here and mark the final two progress items complete. The rolling
audit continues with the next complete package boundary.
