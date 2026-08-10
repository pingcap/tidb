# Simplify Go-package semantic gates

This ExecPlan is a living document. Maintain it according to `PLANS.md`.

## Purpose

Package work was spending more time maintaining evidence machinery than
implementing behavior. The replacement keeps only three current inputs:

1. one TOML specification with semantic rules;
2. the Rust implementation and focused tests named by those rules;
3. one generated JSON receipt with current hashes and verification outcomes.

The gate still inventories every accepted Go artifact and AST obligation. It
rejects source drift, overlapping rules, stale Rust ownership, stale receipts,
surviving mutations, and premature whole-package claims. It does not store
derived ledgers, helper-call graphs, probe transcripts, mutation histories, or
Go runtime mechanics that do not affect TiDB behavior.

## Progress

- [x] Implement `rust/scripts/go-package-lockdown.py` with `audit`, `check`, and
  `verify` commands.
- [x] Add focused verifier tests for unmatched and overlapping rules, nested
  package exclusions, killed and surviving mutations, unrelated failures, and
  byte-for-byte restoration.
- [x] Convert `pkg/util/chunk` to one specification and one receipt.
- [x] Preserve the settled `pkg/meta/model` and `pkg/statistics` classifications
  in compact seed specifications and receipts.
- [x] Remove the three superseded receipt trees and obsolete v2 documentation.
- [x] Run focused semantic gates and Ready validation on the exact compact-gate
  checkpoint.

## Contract

Rules are semantic clusters, not Go files or individual branches. A rule may
select obligations by source, owner, anchor, category, or exact ID. Selectors
are conjunctive and shell-style patterns inside one selector are alternatives.
Every obligation matches at most one rule.

`PORTED` rules name production Rust files and one focused `-j12` test. They may
carry one mutation of one invariant. `DECLINED` and `UNREACHABLE` rules state
the semantic reason directly. Unmatched obligations stay visible as
`UNCLASSIFIED`; a `package-seed` may contain them, while `whole-go-package`
may not.

The generated receipt stores accepted artifact hashes, owned Rust hashes,
inventory and unmatched-set hashes, per-rule obligation count/hash, and current
verification outcomes. It intentionally stores no raw command logs or
append-only history.

## Commands

From repository root:

    python3 rust/scripts/go-package-lockdown.py audit --spec <package>.toml --write
    python3 rust/scripts/go-package-lockdown.py verify --spec <package>.toml --rule <rule-id>
    python3 rust/scripts/go-package-lockdown.py check --spec <package>.toml
    python3 -m unittest rust.scripts.tests.test_go_package_lockdown

`audit` and `check` are read-only unless `audit --write` is requested. `verify`
holds original source bytes in memory, restores them in `finally`, verifies the
restored hash, reruns the named test, and writes the receipt only after success.

## Acceptance

The mechanism is complete when the repository has one verifier, one concise
contract, and one TOML/JSON pair per migrated package; the legacy checker,
receipt forests, observation serializers, execution logs, and stale docs are
gone; the verifier tests and each compact receipt check pass; and Ready gates
pass on the exact integration candidate.
