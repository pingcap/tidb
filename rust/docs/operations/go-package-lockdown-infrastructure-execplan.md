# Build one generic package-scale Go lockdown gate

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`,
`Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root. Go source in the isolated worktree at
accepted integration commit `bdab0016365e8b1d79b5b11f52ee6fdde90f4c46` is
authoritative. Existing source-specific lockdown scripts and receipts are locked
evidence and must not be edited by this infrastructure unit.

## Purpose / Big Picture

After this change, a package owner can describe one complete Go package and all
mapped Rust crates in a small `package.toml`, generate an exhaustive artifact
manifest and one AST ledger per Go file, then use one command to reject source,
verdict, Rust-symbol, semantic-rule, mutation, or receipt drift. Future package
units no longer need to copy and modify a Python checker for every Go file.

The observable result is a standard-library-only command at
`rust/scripts/go-package-lockdown.py` with `generate`, `check`, and
`write-receipt` subcommands. Focused integration tests construct a temporary Git
repository, invoke the real checked-in Go AST inventory tool, close a synthetic
package receipt, and deliberately mutate every critical omission boundary.

## Progress

- [x] (2026-08-08) Verified both remotes expose the accepted SHA and created an isolated branch/worktree without claiming a Rust crate.
- [x] (2026-08-08) Audited the package-scale `pkg/types`, source-scale `upgrade_def.go`, and cross-crate server-parse receipts.
- [x] (2026-08-08) Implemented the generic declarative generator and fail-closed checker without editing existing lockdowns.
- [x] (2026-08-08) Added a synthetic lifecycle plus 14 deliberate drift/omission cases and documented the operator contract.
- [x] (2026-08-08) Committed the clean, scoped-validated descendant and preserved its local branch ref for coordinator integration.

## Surprises & Discoveries

- Observation: the existing package AST tool already parses every direct `.go`
  file regardless of build constraints and emits stable content-addressed IDs.
  Evidence: `rust/difftests/tools/go_package_lockdown_inventory/main.go`
  performs one non-recursive directory read and uses `go/parser` rather than
  package loading.
- Observation: hashing a shared checker into every package receipt would make a
  checker improvement reopen every completed package.
  Evidence: the existing `pkg/types` receipt includes both its source-specific
  checker and Go inventory tool hashes. This generic receipt instead records a
  schema version and hashes only package-owned configuration, evidence, and Rust
  anchors.
- Observation: direct test fixtures outside the package directory are otherwise
  easy to omit even when the package file census is recursive.
  Evidence: the focused test changes `os.ReadFile("testdata/cases.txt")` to
  `os.ReadFile("../shared.txt")`; the fixture inventory resolves the external
  path and the generic generator rejects it until it is explicitly manifested.

## Decision Log

- Decision: keep the existing Go inventory binaries and all existing lockdown
  scripts byte-for-byte unchanged.
  Rationale: those files are already content-addressed by locked receipts; a new
  front end can compose them without invalidating prior work.
  Date/Author: 2026-08-08 / Codex
- Decision: `generate` preserves verdict fields by obligation ID and refuses to
  discard an obligation that disappeared from Go.
  Rationale: regeneration must never turn source drift into silent evidence
  deletion. An owner must explicitly remove retired ledger rows before accepting
  a changed Go source.
  Date/Author: 2026-08-08 / Codex
- Decision: untracked package artifacts are enumerated in a hard failure rather
  than recorded in a manifest.
  Rationale: their Git state would change after commit and make the receipt
  unstable. Staging a legitimate new artifact before generation makes the
  inclusion explicit and reproducible.
  Date/Author: 2026-08-08 / Codex
- Decision: keep compilation outside this infrastructure process while requiring
  exact mapped-crate anchor paths and names.
  Rationale: the coordinator explicitly owns all compile/test/fix work. The
  package gate proves the registry join and the package owner's single scoped
  Cargo command proves the anchors compile.
  Date/Author: 2026-08-08 / Codex

## Outcomes & Retrospective

The generic v1 now closes a synthetic package through `generate`, manual verdict
classification, `write-receipt`, and read-only `check`. Sixteen focused tests
pass. They kill artifact-hash drift, untracked and unclassified artifacts,
unknown specification fields, missing ledger obligations, verdict loss during
regeneration, removed-obligation deletion, missing PORTED symbols, weak
DECLINED/UNREACHABLE evidence, missing rule mutations, final surviving
mutations, unresolved and external fixture omissions, and receipt drift.
The second positive lifecycle proves that a package with every obligation
honestly `DECLINED` closes as `completion_kind: falsification` with no fake Rust
symbols, rules, or mutations.

No Cargo, Clippy, Go package test, repository lint, or full workspace command was
run; those gates remain explicitly owned by the coordinator. The implementation
uses only Python's standard library plus the repository's existing Go AST and
fixture inventory tools.

## Context and Orientation

`rust/difftests/tools/go_package_lockdown_inventory` is the syntax-aware Go
authority. It emits one row for declarations, fields, functions, control-flow
outcomes, closures, tests, test rows, and assertions. The new Python front end
runs that tool once for a non-recursive package and partitions its rows into a
ledger beside each owning Go filename.

A semantic rule is a human-reviewed behavior shared by one or more AST
obligations. `rules.tsv` maps every `PORTED` obligation to a rule, its boundary
cases, and one or more independent mutations. `mutation-plan.tsv` identifies the
mutated Rust source and exact named test. `mutation-results.tsv` records every
attempt, including initially surviving mutations, and requires a later killed
attempt before the rule can close.

## Plan of Work

Add `rust/scripts/go-package-lockdown.py`. Use only Python's standard library.
The script reads TOML with `tomllib`, runs Git and the existing Go tool with
argument arrays, writes TSV/JSON atomically, and rejects paths escaping the
repository. Its artifact census covers every tracked file below the package
directory except explicitly proved nested Go packages, plus explicitly listed
external non-Go artifacts. Automatic roles cover Go production/test/generated
files, Bazel metadata, and `testdata`; all other artifacts require an explicit
role in `package.toml`.

`generate` writes the current manifest and per-file ledgers. Existing verdict
columns are joined by obligation ID and never replaced. New obligations are
`UNCLASSIFIED`; disappeared obligations abort generation. `check` regenerates
the Go side in memory, compares it exactly, validates all joins and evidence,
and checks the content-addressed receipt. `write-receipt` performs every check
except the final receipt comparison, then writes the expected receipt.

Add `rust/scripts/tests/test_go_package_lockdown.py`. The tests create an
isolated temporary Git repository and a small Go package, copy in the real AST
tool, generate and classify the package, and then exercise successful closure.
Separate tests mutate an artifact, add an untracked file, delete a ledger row,
remove a symbol, weaken decline/unreachability evidence, break rule/mutation
coverage, and verify regeneration preserves existing verdicts.

Update `rust/scripts/README.md` with the stable command sequence and point to
this document for the complete schema.

## Concrete Steps

Run from repository root:

    python3 -m unittest -v rust/scripts/tests/test_go_package_lockdown.py
    python3 -m py_compile rust/scripts/go-package-lockdown.py rust/scripts/tests/test_go_package_lockdown.py
    git diff --check

No Go, Bazel, module, or Rust source is changed, so `make bazel_prepare`, Cargo
crate gates, Go package tests, repository lint, and a full workspace run are
owned by the coordinator rather than this non-compiling infrastructure unit.

## Validation and Acceptance

Acceptance requires the synthetic package lifecycle to pass through
`generate`, manual classification, `write-receipt`, and `check`. Each deliberate
mutation named in the Plan of Work must make the checker fail with the intended
diagnostic, and restoration must return the lifecycle to green. The final Git
diff must contain only the new generic script, its focused tests, this plan, and
the scripts README update. The coordinator owns compilation and repository-wide
Ready gates after integration.

## Idempotence and Recovery

`check` is read-only. `generate` and `write-receipt` use atomic replacement and
are safe to rerun. A failed validation writes nothing. If Go obligations were
removed, generation stops before updating any file; the owner reviews and
explicitly removes the retired ledger rows before rerunning. Temporary test
repositories are owned by `tempfile.TemporaryDirectory` and are automatically
reclaimed.

## Artifacts and Notes

The accepted parent was verified directly on both `origin` and `ngaut` as
`bdab0016365e8b1d79b5b11f52ee6fdde90f4c46`. This unit owns only generic
infrastructure files and does not reserve or modify a Rust crate.

## Interfaces and Dependencies

The public command is:

    python3 rust/scripts/go-package-lockdown.py [--root <repo>] \
      {generate,check,write-receipt} --spec <package.toml>

It requires Python 3.11 or newer for standard-library `tomllib`, Git, and the Go
toolchain already required by TiDB. It introduces no Python package, Cargo
crate, Go module, network service, or developer-specific path.

The package specification is repository-relative and has this shape:

    schema = "go-package-lockdown-spec-v1"
    claim = "whole-go-package"
    go_package = "pkg/example"
    source_commit = "<full-40-character-accepted-sha>"
    primary_rust_crate = "tidb-example"
    mapped_rust_crates = ["tidb-example", "tidb-support"]
    extra_artifacts = ["pkg/shared/example-fixture.json"]
    owned_rust_files = [
      "rust/crates/tidb-example/tests/pkg_example_lockdown.rs",
      "rust/crates/tidb-support/src/example_anchor.rs",
    ]
    excluded_subpackages = [
      { path = "pkg/example/child", proof = "separate Go package declared as child" },
    ]

    [artifact_roles]
    "pkg/example/schema.json" = "generated-input"
    "pkg/example/README.md" = "support"
    "pkg/shared/example-fixture.json" = "fixture"

    [unresolved_fixture_evidence]
    "pkg/example/example_test.go:42:os.ReadFile:dynamicPath" = "measured: helper resolves only testdata/example.json"

The package directory is non-recursive for Go syntax. Nested directories remain
artifact-owned for fixtures and generator inputs, but any nested Go package must
appear in `excluded_subpackages` with proof. External Go files are separate Go
packages and cannot be smuggled in as support; `extra_artifacts` is for direct
non-Go build, generated, or fixture inputs. Untracked package artifacts always
fail so a legitimate new file must be staged before generation.

`artifacts.tsv` is generated with `path`, `role`, `traits`, `sha256`, `bytes`,
and `lines`. Automatic roles cover direct production/test Go, generated Go,
Bazel build files, and files below `testdata`; an unknown tracked file requires
an `artifact_roles` decision. Traits record build tags, platform suffixes,
generated markers, `go:generate`, `go:embed`, and testdata membership. Direct
literal fixture accesses must resolve to a manifested artifact. Dynamic fixture
expressions require an exact `unresolved_fixture_evidence` key and measured
evidence.

Each generated ledger has the six raw Go fields followed by `status`,
`symbol_id`, `evidence`, and `rule_id`. A `PORTED` row uses
`boundary-test:<anchor_name>` and names a symbol and rule. A `DECLINED` row uses
`-` for symbol/rule and includes the exact
`go-quote:<source>#<anchor>@sha256:<node-hash>` plus `measured:` evidence. An
`UNREACHABLE` row uses the same exact Go quote plus `structural-proof:`.

`symbols.tsv` has `symbol_id`, `rust_crate`, `rust_symbol`, `anchor_path`, and
`anchor_name`. The registry must equal the symbols used by all `PORTED` rows;
each anchor file must live below its mapped crate and contain both names. The
package owner's one scoped Cargo command is what compiles those anchors; this
non-compiling infrastructure checker does not invoke Cargo.

`rules.tsv` has `rule_id`, `cluster_id`, `description`, semicolon-separated
`obligation_ids`, semicolon-separated `boundary_cases`, and semicolon-separated
`mutation_ids`. Every `PORTED` obligation appears in exactly one rule. Each rule
has at least two boundary cases and at least one mutation.

`mutation-plan.tsv` has `mutation_id`, `cluster_id`, `rule_ids`,
`baseline_commit`, `rust_path`, `source_sha256`, `command`, and `named_test`.
One mutation alters exactly one rule; its source hash and baseline commit must
remain current and its command must name the exact boundary test.
`mutation-results.tsv` has `attempt_id`, `mutation_id`, `outcome`, `exit_code`,
`restore_status`, `restored_source_sha256`, and `named_failure`. Every attempt is
retained, `SURVIVED` uses exit zero, `KILLED` uses a nonzero exit, every restore
is `PASS` with exact bytes, and the last attempt for each mutation must be
`KILLED`.

`receipt.json` records counts and SHA-256 values for the specification,
manifest, every per-file ledger, all four evidence matrices, all listed Rust
files, and every compile-anchor file. It records
`go-package-lockdown-checker-v1`, not the checker file hash.
