# `pkg/planner/cascades/pattern` — Go-master parity audit receipt

Go authority: `origin/master` at
`aec988ea500de42dd6c8b2cf429dff907ce5bd41`.

## Complete Go inventory

The package contains exactly five tracked artifacts and 449 lines. Every
production file, test file, and build target was read in full before editing:

| Artifact | Lines | Role |
| --- | ---: | --- |
| `BUILD.bazel` | 31 | library and short-test targets |
| `engine.go` | 64 | execution-engine bits, predefined sets, membership, and labels |
| `pattern.go` | 222 | operand classification, matching, and pattern construction |
| `engine_test.go` | 43 | predefined engine-set membership matrix |
| `pattern_test.go` | 89 | operand mapping/matching and pattern construction/children tests |

The production declaration inventory was checked function by function:
`EngineTypeSet.Contains`, `EngineType.String`, `Operand.String`, `GetOperand`,
`Operand.Match`, `Pattern.Match`, `Pattern.MatchOperandAny`, `NewPattern`,
`Pattern.SetChildren`, and `BuildPattern`. The test inventory contains exactly
`TestEngineTypeSet`, `TestGetOperand`, `TestOperandMatch`, `TestNewPattern`, and
`TestPatternSetChildren`; there is no package `doc.go`, `TestMain`, benchmark,
fuzz target, example, fixture/testdata tree, generated source,
platform/build-tag variant, nested package, or other support artifact. The
BUILD file lists exactly the two production and two test files above.

## Rust ownership and parity

The dependency-closed owner is `rust/crates/tidb-planner`, specifically
`pattern_engine.rs` and `pattern.rs`. The crate manifest and module
declarations, the source-derived aggregate-test input
`tests/cascades_pattern_operand_engine_source.rs`, and direct planner/executor
consumers were re-read. Cargo generates the `all` test target from
`scripts/aggregate-tests.rs`; there is no target-specific owner source.

Go permits callers to discard every source-shaped return. Rust previously
added nine explicit `#[must_use]` diagnostics to `EngineType::as_str`,
`EngineTypeSet::contains`, `Operand::as_str`, `Operand::matches`, `get_operand`,
`Pattern::matches`, `Pattern::matches_operand_any`, `new_pattern`, and
`build_pattern`. Those Rust-only diagnostics were removed. The native bit
conversion adapters `EngineType::bits`, `EngineTypeSet::from_bits`, and
`EngineTypeSet::bits` remain annotated because they have no Go call contract.
Engine membership, operand numbering and wildcard symmetry, logical-operator
classification, child replacement, and construction behavior are unchanged.

The focused regression uses `#[deny(unused_must_use)]` and discards all nine
direct Go-shaped results. Before the edit it failed with exactly nine
diagnostics, one for each API above; after the edit it passes.

## Validation (Ready profile)

- Current Go-master five-artifact inventory and complete declaration/test
  mapping — passed.
- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-planner --lib pattern::return_contract_tests::source_return_values_may_be_ignored_like_go -- --exact --nocapture` — passed (1 focused test; exactly nine diagnostics before the edit).
- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-planner --lib pattern:: -- --test-threads=1` — passed (1/1 owner regression).
- `cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -p tidb-planner --lib` — passed.
- `rustfmt --check rust/crates/tidb-planner/src/pattern.rs rust/crates/tidb-planner/src/pattern_engine.rs` and `git diff --check` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make lint` — passed (repository Ready gate).

The source-derived five-test port is part of the generated `all` target. Its
filtered run was attempted and is blocked before execution by an unrelated
existing initializer at
`rust/crates/tidb-planner/tests/core_logical_cte_topn_prune_source.rs:75`,
which omits the pre-existing `RuleContext.allow_agg_push_down` field. A direct
named test target does not exist because these files are aggregated. The
workspace-wide `cargo fmt --all -- --check` is also blocked by pre-existing
formatting in the already-published
`physical_property::required_property_tests` regression; both changed pattern
modules pass a direct rustfmt check. No unrelated file was changed to mask
either boundary.

This is a Rust-only batch: no Go source, import section, Go test function,
Bazel file, or module dependency changed, so `make bazel_prepare` is not
required. The package has no failpoint use. The repository-level Ready lint
gate passed with the receipt and ExecPlan updates included.

## Risks and boundaries

- Correctness: the focused discard regression and planner library check pass;
  runtime matching and construction logic is unchanged.
- Compatibility: only Rust lint policy changed for APIs Go already permits to
  be ignored; no Rust signature or runtime behavior changed.
- Performance: no planner algorithm, allocation, or hot path changed.
- Not verified locally: execution of the generated five-test aggregate,
  workspace-wide rustfmt because of the unrelated committed formatting issue,
  full workspace tests, Bazel execution, and cross-platform planner builds.
