# Stabilize differential-test selector shards and corpus contracts

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root; this plan must be maintained according to it.

## Purpose / Big Picture

The Rust rewrite depends on many independent parser selectors and source-owned differential corpora. Today Cargo treats each file directly under `rust/difftests/tests/` as a separate integration-test binary. That gives every selector an ownership boundary, but it also creates roughly one hundred almost-identical binaries that repeatedly compile the checked Go parser oracle reader. Contributors need to keep owning one selector at a time without paying that compile and process overhead.

After this plan's first increment, selector modules can live under a stable family shard while retaining their own file and test name. The `admin` shard proves the shape: several independently owned source selectors are compiled and run as one Cargo integration-test binary. A corpus contract validator makes executable corpora self-checking: every executable topic must have exactly one paired source and golden file, and prose evidence must live outside executable namespaces.

## Progress

- [x] (2026-07-15) Mapped the current topology: 111 top-level integration-test files, including 100 parser selector binaries; `expr`, `parser`, and `table` are paired executable corpus namespaces.
- [x] (2026-07-15) Recorded the initial first-shard scope: four independent `ADMIN` selectors, selected to avoid parser/executor implementation files and active feature ownership.
- [x] (2026-07-15) Exported the checked parser-oracle reader through the `difftest` library without duplicating its framing/TSV protocol.
- [x] (2026-07-15) Moved four `ADMIN` selector modules below `tests/selectors/admin/` and added the `selector_admin.rs` shard entrypoint.
- [x] (2026-07-15) Added corpus contract validation plus checked-repository and synthetic failure tests.
- [x] (2026-07-15) Measured direct test entrypoints: 111 before, 108 after; selector entrypoints: 100 before, 96 after. The first shard test took 4.05 seconds warm. A meaningful pre-change runtime comparison could not be captured because the old entrypoints were replaced in the same atomic migration; future family migrations must use a clean-worktree baseline.
- [x] (2026-07-15) Completed the family-wide mechanical migration: Cargo auto-discovery is disabled and its metadata lists 23 explicit test targets, comprising 12 non-selector rings (including the topology contract) plus 11 stable selector shards.
- [x] (2026-07-15) Ran formatter, focused shard/contract tests, the first full 23-target difftest suite, strict Clippy with twelve jobs, and `git diff --check`. The full suite passed in 189.65s; after the validator's Clippy cleanup, focused contracts/rings and final strict Clippy passed.

## Surprises & Discoveries

- Observation: `rust/difftests/tests/` contains 111 top-level `.rs` files and 100 `*_selector.rs` files. Cargo compiles each top-level file as a separate test executable.
  Evidence: `find rust/difftests/tests -maxdepth 1 -name '*.rs' | wc -l` returned `111`; the selector count returned `100`.
- Observation: selector files use `#[path = "../src/bin/integration_parser_golden.rs"]`, so every selector owns an inlined copy of the parser-oracle reader rather than importing a stable library interface.
  Evidence: 100 selector files contain that path attribute.
- Observation: executable topic pairs are currently conventional rather than enforced by one validator.
  Evidence: `difftest::corpus_topics` enumerates source `.txt` names and `load_corpus_dir` unwraps the matching golden path; it does not reject golden-only files or Markdown evidence in `corpus/{expr,parser,table}`.
- Observation: `cargo test -p difftest` runs selector test binaries serially after compilation; at three minutes the active child was one individual selector binary.
  Evidence: process inspection showed `admin_show_next_row_id_selector --quiet`, then `alter_exchange_partition_selector --quiet`, then `alter_user_resource_group_selector --quiet` as the sole Cargo child. This confirms the first shard's binary reduction is a real runtime direction, not merely filesystem organization.
- Observation: Cargo's automatic discovery must be disabled once selector modules move below a non-root directory; otherwise a new root selector creates an unreviewed test binary, while a nested module can be silently skipped.
  Evidence: the explicit topology contract now compares all `tests/selectors/**/*.rs` modules with shard `#[path]` entries and compares all root entrypoints with `Cargo.toml` `[[test]]` paths. It passed with 100 selector modules, 11 shards, and 23 explicit targets.

## Decision Log

- Decision: Introduce shard entrypoints incrementally instead of renaming all one hundred selector files in one change.
  Rationale: A family shard preserves each source selector's module/file ownership while reducing Cargo binaries. The small `ADMIN` migration gives a behavior-preserving proof and avoids conflicts with active parser/AST work.
  Date/Author: 2026-07-15 / Codex
- Decision: Treat `corpus/expr`, `corpus/parser`, and `corpus/table` as executable paired namespaces; leave top-level legacy files and `corpus/coverage` out of that rule.
  Rationale: those three directories are consumed by directory loaders, while coverage holds evidence (`.md`, `.tsv`) and the root files have separate legacy consumers.
  Date/Author: 2026-07-15 / Codex
- Decision: Expose the existing parser-oracle module through the library for new shards, while preserving legacy selector paths during the migration.
  Rationale: this avoids a semantic rewrite of the byte-exact Go framing protocol and permits independently movable selectors. Legacy path modules are removed only as their family is migrated.
  Date/Author: 2026-07-15 / Codex

## Outcomes & Retrospective

The first increment and the family migration are implemented. `tests/selector_admin.rs` compiles its source-owned ADMIN modules below `tests/selectors/admin/`, and every other selector now has the same family directory boundary. `difftest::parser_oracle` exposes the checked reader without a copy of its byte framing implementation. The corpus validator enforces paired topics and rejects Markdown in executable namespaces, while intentionally excluding `corpus/coverage` evidence. The topology contract proves no selector is orphaned, duplicated, or implicitly omitted by Cargo.

The first complete post-shard difftest run passed every target in 189.65 seconds (`user 169.16`, `sys 64.15`). Strict Clippy then identified two `never_loop` findings in the new validator; they were replaced with direct `next()` checks. After the concurrent AST/parser `AlterUser` migration stabilized, formatter, topology, corpus-contract, expression, parser, and table focused rings all passed. The final strict `cargo clippy -j 12 -p difftest --all-targets -- -D warnings` passed after the parser lane repaired its own lint, and `git diff --check` passed.

The family-wide migration grouped every selector into eleven stable semantic shard entrypoints, preserving each existing module and test name. No selector retains `#[path = "../src/bin/integration_parser_golden.rs"]`; importing `difftest::parser_oracle` is now the only selector-oracle route.

## Context and Orientation

`rust/difftests` is the Rust differential-test package. Cargo discovers every direct file under `rust/difftests/tests/` as a separate integration-test binary. A *selector* is a source-owned test that filters records in `rust/difftests/corpus/coverage/integration_parser_golden.tsv`, parses each selected SQL input in Rust, and compares its restored SQL bytes with the checked Go oracle.

`rust/difftests/src/bin/integration_parser_golden.rs` owns the byte-exact framed Go oracle protocol and exposes `read_golden`, `repo_root`, `GoldenRecord`, and `GoOutcome` only through file inclusion today. `rust/difftests/src/lib.rs` owns the shared corpus loaders. The paired executable corpus directories are `rust/difftests/corpus/expr`, `rust/difftests/corpus/parser`, and `rust/difftests/corpus/table`; a topic is `<name>.txt` plus `<name>.golden.txt`. `rust/difftests/corpus/coverage` is an evidence namespace and may contain Markdown.

The first shard moves `admin_bdr_selector.rs`, `admin_checksum_selector.rs`, `admin_recover_index_selector.rs`, and `admin_reload_selector.rs`. Their separate module names and test functions remain the source ownership boundary; only Cargo's binary boundary changes.

## Plan of Work

First, make the existing parser-oracle reader a public `difftest::parser_oracle` module by including its existing source from `src/lib.rs` and making the exact selector-facing record API public. Do not copy or reinterpret its framed Go subprocess protocol. This lets new shard modules import `difftest::parser_oracle` directly.

Second, create `tests/selectors/admin/` and move the four selected files there. Add `tests/selector_admin.rs` whose `#[path]` modules include the four files. Update those modules to import the shared oracle API. They retain their independent filter function, source-count assertion, and test name. Delete the four old top-level entrypoint files so Cargo produces one `selector_admin` binary rather than four separate binaries.

Third, add a `CorpusContractError`-style public validator in `src/lib.rs` or a narrowly named sibling module. It takes a root directory so unit tests can construct temporary trees. It checks each exact executable namespace for normal `.txt` sources paired with a `.golden.txt` file, golden-only files, source-only files, and Markdown files. It does not parse corpus contents or impose a policy on `corpus/coverage`. Add tests for the checked repository and every synthetic failure class; invoke the checked validator in `expr_diff`, `parser_diff`, and `table_diff` before loading their corpus so any normal differential ring rejects malformed inputs.

Finally, measure Cargo test entrypoint count from the filesystem and use `time` around the stable focused selector command. If the active shared worktree prevents full difftest compilation, record the exact external error in this plan, run all independent library/selector checks possible, and rerun the full commands once the concurrent change is complete. Never declare a runtime reduction without before/after command output.

## Concrete Steps

From repository root, establish the baseline:

    find rust/difftests/tests -maxdepth 1 -type f -name '*.rs' | wc -l
    find rust/difftests/tests -maxdepth 1 -type f -name '*_selector.rs' | wc -l
    cd rust && /usr/bin/time -p cargo test -j 12 -p difftest --test admin_bdr_selector -q

Implement the shared oracle export, first ADMIN shard, and contract validator with `apply_patch`. Generate no golden files in this refactor; the existing content is merely validated.

Then run from `rust/`:

    cargo fmt --all -- --check
    cargo test -j 12 -p difftest --lib -q
    cargo test -j 12 -p difftest --test selector_admin -q
    cargo test -j 12 -p difftest --test expr_diff -q
    cargo test -j 12 -p difftest --test parser_diff -q
    cargo test -j 12 -p difftest --test table_diff -q
    cargo test -j 12 -p difftest -q
    cargo clippy -j 12 -p difftest --all-targets -- -D warnings

Use `/usr/bin/time -p` around `selector_admin` and the four legacy selectors before migration if the workspace builds. Count top-level test files after migration; the expected first-step reduction is three binaries (four individual entrypoints become one shard).

## Validation and Acceptance

Acceptance is observable, not a directory convention:

1. `cargo test -p difftest --test selector_admin` runs the same four source-owned tests and their source-count assertions pass.
2. The checked `expr`, `parser`, and `table` corpus trees validate with no error.
3. Synthetic test trees prove the validator rejects a source-only topic, golden-only topic, and Markdown inside an executable namespace, while permitting Markdown under `corpus/coverage`.
4. The direct test entrypoint count falls from 111 to 108: four standalone ADMIN files disappear and one shard file appears.
5. Full difftest tests and strict Clippy pass after the shared worktree is compilable.

## Idempotence and Recovery

The validator is read-only. Moving a selector is safe to retry: the shard only includes files present below `tests/selectors/admin/`, and Cargo ignores nested files unless referenced by the shard. If a moved module fails due to import visibility, restore only that module to its former top-level path and retain the shared oracle export; do not duplicate the oracle reader. If a synthetic contract test leaves a temporary directory, its test cleanup must remove it even after assertion failure where possible.

## Artifacts and Notes

Baseline topology evidence:

    top-level integration test files: 111
    selector entrypoint files: 100
    direct executable artifact names matching selector: 100

The four first-shard selectors select 92 BDR rows, 8 checksum rows, 5 recover-index rows, and 28 reload rows. Their existing assertions make the migration detect checked-oracle drift.

## Interfaces and Dependencies

At the first milestone, `difftest` exposes:

    pub mod parser_oracle
    pub fn validate_executable_corpora(root: &Path) -> Result<(), String>

`parser_oracle` exposes public `repo_root()`, `read_golden(&Path)`, `GoOutcome`, `GoldenRecord`, and the fields used by selector modules. `validate_executable_corpora` must inspect only `root/rust/difftests/corpus/{expr,parser,table}` when given repository root. The three directory differential tests call it before `load_corpus_dir`/`corpus_topics`, preserving current data and evaluation semantics.

Plan created 2026-07-15 for the first incremental selector-shard and corpus-contract refactor.

Updated 2026-07-15: implemented and measured the ADMIN first shard; recorded the serial-binary runtime finding, completed the family-wide migration to eleven shards, and added the durable topology contract. Cargo metadata reports 23 explicit difftest test targets: twelve non-selector rings and eleven selector shards.
