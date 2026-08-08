# Build one generic package-scale Go lockdown gate

This ExecPlan is a living document under `PLANS.md`. The infrastructure unit is
based on accepted integration commit
`bdab0016365e8b1d79b5b11f52ee6fdde90f4c46`. Existing source-specific scripts
and receipts are immutable seed evidence and are not edited here.

## Purpose

`rust/scripts/go-package-lockdown.py` is a standard-library-only frontend for an
atomic complete-Go-package claim. It composes the checked-in Go AST, fixture,
and direct-test helper-call inventory tools; generates one package artifact
census and one ledger per Go file; validates human verdict/evidence joins;
executes coordinator-owned fixed probe and mutation runners; and writes a
content-addressed package receipt.
Package owners no longer copy Python boilerplate or repeatedly compile per file.

The active workflow is deliberately split:

- The coordinator seeds inventories, executes all probes/mutations and
  compilation in one warmed lane, fixes integration issues, batches only
  write-disjoint crates, and is the sole dual-pusher to `hparser-integration`.
- One package owner compares and edits mapped Rust production/tests and package
  ledgers. The owner runs no Cargo, Go package tests, Clippy, `make`, full gate,
  executable evidence, or remote push.

## Progress

- [x] Audited reusable package/file receipts and the existing Go inventory
  binaries without editing locked evidence.
- [x] Added `generate`, `check`, and `write-receipt` with a declarative v2 spec,
  exhaustive artifact/AST ledgers, exact joins, and schema-versioned receipt.
- [x] Closed source-body, source-commit, nested-exclusion, `go:generate`, Rust
  lexical-anchor, executable-mutation, and evidence-artifact false greens.
- [x] Added fixed `run-evidence` / `verify-evidence` coordinator runners with
  exact source restoration and normalized named-test observations.
- [x] Closed later adversarial findings: production declaration proof, exact
  named test-body references, mapped-crate change census, content-addressed
  dynamic-fixture evidence, immutable historical survivor plans, and honest
  implementation-completeness status.
- [x] Bound qualified Rust declarations to full module/type/impl identity,
  inventoried every direct-test call for helper-mediated fixture access,
  required machine-readable probe observations, proved mutation baseline and
  restoration passes, and hash-chained attempt history to a committed
  receipt/history checkpoint.
- [x] Completed Python unit/static/diff checks for the clean local descendant;
  all Cargo/Go package/make/full execution remains coordinator-owned.

## Important findings and decisions

The existing Go AST inventory already parses every direct `.go` artifact with
`go/parser`, independent of active build constraints, and emits stable node
hashes. The frontend reuses it and binds every ledger row to the full owning Go
blob hash too. A straight-line body edit therefore invalidates preserved
verdicts even when an AST obligation ID remains stable.

`source_commit` is not descriptive metadata. The manifest requires the exact
tracked artifact set and Git blob IDs at that commit, which must be an ancestor
of the checkout. Nested exclusions are valid only for a direct tracked Go
package directory with proof
`go-package-dir:<path>#package:<identifier>`; the identifier may equal its
parent, because directory/import path—not the package clause alone—defines a Go
package. `testdata` and arbitrary fixture subtrees cannot be excluded.

Every repository input statically referenced by `go:generate` must be in the
manifest. Shell directives, unresolved variables, globs, or repository paths
that cannot be pinned fail closed. Literal fixture accesses must resolve to a
manifested artifact. Dynamic accesses use content-addressed
`fixture-resolution` JSON bound to the exact source/line/access expression,
source commit, exact resolved set (or explicit no-artifact conclusion), two
boundary cases, and a fixed passing probe. `helper-calls.tsv` additionally
records every `go/ast.CallExpr` in each direct `*_test.go`.
`helper-contracts.tsv` must classify each exact call set as a mechanically
linked direct fixture access, a content-addressed `NO-FIXTURE` structural
proof, or measured `FIXTURE` resolution. Thus helpers such as
`testDataMap.LoadTestSuiteData` cannot sit outside the receipt. Prose containing
`measured:` is not evidence.

The Rust registry separates production definition from test proof. A symbol is
qualified as `crate::...` or `<mapped_crate_name>::...`; its complete
module/type/impl identity must resolve to an actual declaration in a tracked receipt-owned
`rust/crates/<crate>/src/**/*.rs`. The separate anchor must contain an actual
`#[test]` whose exact body references the full token sequence. The lexer drops
nested comments and all Rust string/character forms, so comments, strings,
usage-only tokens, test-only definitions, and unrelated test bodies cannot make
the gate green. Mutation targets obey the same owned production-`src` rule.

Every changed, staged, or untracked path below every mapped crate since
`source_commit` must appear in `owned_rust_files`, including helpers and
`Cargo.toml`. There is no integration-only escape hatch. This closes the common
false green where an owner edits a helper but omits it from the receipt.

Evidence never supplies an executable command. A plan selects:

- `cargo-test`: a mapped crate, integration-test target, and exact (optionally
  module-qualified) test name. The checker runs from `rust/`:
  `cargo test --offline --locked -j12 --quiet -p <crate> --test <target> <name> -- --exact`.
- `go-test`: the exact pinned Go package and exact test name. The checker runs
  from the repository root:
  `go test ./<package> -run ^<name>$ -count=1 -v`.

`run-evidence` and `verify-evidence` each content-address their raw logs. A
measured probe also owns a content-addressed observation JSON whose boundary
cases and conclusion are exact. Its named test must emit the checker-defined
observation-hash and conclusion-hash marker; a passing test without the marker
cannot attest the observation. Raw Cargo/Go logs can differ in elapsed times
and build chatter, so validation
derives and compares a deterministic hash of the exact named-test PASS/FAIL
marker plus exit/outcome. A compilation-only failure has no test marker and is
rejected. Every mutation attempt proves baseline named-test PASS, records the
mutated named-test FAIL or SURVIVED observation, and proves restored named-test
PASS. Mutation installation and restoration are guarded by `finally` and exact
byte comparisons.

Mutation history is immutable. Each attempt owns a content-addressed attempt
plan binding the baseline commit, production source hash, operator, constructed
argv, exact test, and mutated hash. Historical plans are checked against their
own Git baseline and retained operator even after production changes. Historical
survivors remain countable; each current rule plan separately requires a
current-source verified KILLED attempt. No history is rewritten to make the
latest source fit. `mutation-results.tsv` is a contiguous sequence/hash chain.
Every append binds its prior history head and the content hash of the committed
receipt, or the committed history TSV before the first valid receipt. Committed
history must remain an exact prefix, so attempts cannot be deleted, reordered,
or rewritten after a survivor leads to a source fix.

Receipt truth is split deliberately:

- `inventory_complete=true` means every artifact and obligation has an exact
  final classification.
- `implementation_complete=true` requires at least one production PORTED symbol
  and zero DECLINED obligations.
- zero PORTED is `falsification`; mixed PORTED/DECLINED is `classified-gaps`;
  only a gap-free native result is `lockdown`.

## Public interface

From the repository root:

```bash
python3 rust/scripts/go-package-lockdown.py generate --spec <dir>/package.toml
python3 rust/scripts/go-package-lockdown.py run-evidence \
  --spec <dir>/package.toml --kind probe --id <probe-id>
python3 rust/scripts/go-package-lockdown.py verify-evidence \
  --spec <dir>/package.toml --kind probe --id <probe-id>
python3 rust/scripts/go-package-lockdown.py run-evidence \
  --spec <dir>/package.toml --kind mutation --id <mutation-id> --attempt <attempt-id>
python3 rust/scripts/go-package-lockdown.py verify-evidence \
  --spec <dir>/package.toml --kind mutation --id <mutation-id> --attempt <attempt-id>
python3 rust/scripts/go-package-lockdown.py write-receipt --spec <dir>/package.toml
python3 rust/scripts/go-package-lockdown.py check --spec <dir>/package.toml
```

The v2 package specification is repository-relative:

```toml
schema = "go-package-lockdown-spec-v2"
claim = "whole-go-package"
go_package = "pkg/example"
source_commit = "<full accepted SHA>"
primary_rust_crate = "tidb-example"
mapped_rust_crates = ["tidb-example", "tidb-support"]
extra_artifacts = ["pkg/shared/example-fixture.json"]
owned_rust_files = [
  "rust/crates/tidb-example/src/lib.rs",
  "rust/crates/tidb-example/tests/package_lockdown.rs",
  "rust/crates/tidb-support/Cargo.toml",
]
excluded_subpackages = [
  { path = "pkg/example/child", proof = "go-package-dir:pkg/example/child#package:child" },
]

[artifact_roles]
"pkg/example/schema.json" = "generated-input"
"pkg/shared/example-fixture.json" = "fixture"

[unresolved_fixture_evidence]
"pkg/example/example_test.go:42:os.ReadFile:dynamicPath" = \
  "evidence-artifact:<receipt-dir>/fixture-dynamic.json@sha256:<sha256>"
```

Generated `artifacts.tsv` includes path, role, traits, SHA-256, bytes, lines, and
the pinned Git blob OID. Each ledger contains the six raw AST fields, the full
source blob SHA-256, and verdict/symbol/evidence/rule fields. Source hash drift
resets preserved verdicts; removed classified obligations abort generation.
Generated `helper-calls.tsv` binds every direct-test call node, while
`helper-contracts.tsv` joins exact call sets to fixture/no-fixture evidence.

`symbols.tsv` columns are `symbol_id`, `rust_crate`, qualified `rust_symbol`,
`definition_path`, `anchor_path`, and `anchor_name`. `rules.tsv` gives exact
obligation/rule/boundary/mutation joins. `mutation-plan.tsv` contains a current
declarative runner and content-addressed operator. `mutation-results.tsv`
references immutable attempt plans, baseline/mutated/restored run and replay
artifacts, and append-only history hashes. Probe results likewise reference
independent run/verification artifacts plus machine observations. Every
receipt-owned input and execution artifact is SHA-256 recorded in
`receipt.json`; the shared checker is represented by `checker_schema`, not its
file hash.

## Local infrastructure validation

This infrastructure lane is intentionally non-compiling:

```bash
python3 -m py_compile \
  rust/scripts/go-package-lockdown.py \
  rust/scripts/tests/test_go_package_lockdown.py
python3 -m unittest -v rust/scripts/tests/test_go_package_lockdown.py
git diff --check
```

The focused suite uses temporary synthetic repositories and mocks the fixed
Cargo runner when exercising install/restore and run/verify paths. It invokes
the existing Go inventory tooling but runs no TiDB Go package test, Cargo,
Clippy, `make`, lint, or full workspace gate. Those remain coordinator work.

## Idempotence and recovery

`check` is read-only. `generate` and `write-receipt` use atomic replacement.
`run-evidence` refuses an existing attempt; `verify-evidence` refuses an already
verified attempt. Mutation bytes are always restored before artifacts are
accepted. A failed compilation-only mutation writes no result. Historical
attempt/operator artifacts are never overwritten; a production fix receives a
new baseline, operator path, and attempt.

The implementation has no third-party Python dependency, network service, or
developer-local path. It requires Python 3.11+, Git, and the repository's
existing Go tooling. Coordinator evidence execution additionally uses the
already-required Go and Rust toolchains.
