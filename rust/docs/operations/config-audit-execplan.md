# `pkg/config` parity audit ExecPlan

This living plan follows `PLANS.md` at the repository root.

## Objective

Inventory every configuration production/test/example/build artifact, restore
missing Go-master validation and defaults, run focused regression coverage, and
publish one bounded batch to `hparser-integration`.

## Progress

- [x] (2026-09-02) Pulled the latest branch tip and read all 28 `pkg/config`
  artifacts (production, tests, TOML examples, OWNERS, subpackages, and BUILD
  inputs) before editing.
- [x] (2026-09-02) Restored Starter import-size defaults, Starter bootstrap
  manifest validation, hosted-embedding validation, and the foreign-key
  shared-lock configuration gate from Go master.
- [x] (2026-09-02) Ran focused failpoint-safe config tests and diff hygiene.
- [x] (2026-09-02) Ran the remaining Ready gates, staged only this config
  batch, committed, pushed, verified the remote SHA, and fast-forward pulled.
- [ ] Continue the rolling audit with the next unrecorded Go package.

## Constraints

Configuration changes are compatibility-sensitive: defaults, TOML metadata,
deploy-mode validation, and generated Bazel dependencies must remain aligned.
`make bazel_prepare` is mandatory for this scope but is blocked locally because
`bazel` is unavailable.

## Outcome

Evidence is recorded in `rust/testport/receipts/config.md`; the audit remains
open and no Rust package-completion claim is made.
