# `pkg/planner/property` — Go-master parity audit receipt

Go authority: `origin/master` at
`aec988ea500de42dd6c8b2cf429dff907ce5bd41`.

## Complete Go inventory

The package contains exactly six tracked artifacts and 1,242 lines. Every
production file, test file, and build target was read in full before editing:

| Artifact | Lines | Role |
| --- | ---: | --- |
| `BUILD.bazel` | 40 | library and flaky short-test targets |
| `logical_property.go` | 39 | logical-property value and constructor |
| `physical_property.go` | 799 | order, task, MPP exchange, join-runtime, partial-order, hashing, and memory contracts |
| `stats_info.go` | 137 | row count, NDV scaling, group NDV, and limit-stat contracts |
| `task_type.go` | 49 | root/cop/MPP task enum and diagnostic string |
| `physical_property_test.go` | 178 | six-row exchange-equivalence source test and three FD helpers |

The production declaration inventory was checked function by function: the
four production files contain `NewLogicalProp`; all `SortItem`, MPP partition,
collation, physical-property, partial-order, index-join, and exchanger methods
from `physical_property.go`; `ToString`, `String`, `Count`, `Scale`,
`ScaleByExpectCnt`, `GetGroupNDV4Cols`, and `DeriveLimitStats` from
`stats_info.go`; and `TaskType.String`. The test inventory contains one
`TestNeedEnforceExchangerWithHashByEquivalence` plus its three helper builders;
there is no `TestMain`, benchmark, fuzz target, example, fixture/testdata
tree, generated source, platform/build-tag variant, nested package, or other
support artifact. The BUILD file lists exactly the four production files and
one test file above.

## Rust ownership and parity

The dependency-closed owner is `rust/crates/tidb-planner`, specifically its
`logical_property.rs`, `physical_property.rs`, `stats_info.rs`, and
`task_type.rs` modules. The complete owner manifest, module declarations,
aggregate-test build input, source-derived owner tests, and direct
`tidb-executor/src/driver/planner_bridge.rs` consumer were re-read. The
planner package's generated `OUT_DIR/all_tests.rs` is a build artifact of
`scripts/aggregate-tests.rs`; no target-specific owner source exists.

Go permits callers to discard every source-shaped return. Rust previously
added 36 explicit `#[must_use]` diagnostics to direct physical-property,
partition, order, collation, and exchange APIs, six to direct `StatsInfo`
scaling/count/group/format/free-function APIs, and one to `LogicalProperty::new`.
Those 43 Rust-only diagnostics were removed. The five native conversion or
construction helpers (`MppPartitionType::from_raw/raw`,
`MppPartitionColumn::new`, and `SortItem::new/from_column`) remain annotated;
they have no Go method/constructor counterpart and are ownership adapters,
not source-visible discard contracts. Runtime ordering, task selection, MPP
collation matching, hash identity, statistics arithmetic, and property clone
behavior are unchanged.

The focused source-shaped regressions use `#[deny(unused_must_use)]` and
discard every direct Go-shaped result. Before the edit, the physical-property
owner emitted exactly 36 errors. The logical-property and stats-info probes
compiled together by Cargo emitted seven errors, partitioned as one for
`LogicalProperty::new` and six for the stats APIs. After the edit all three
focused regressions pass.

## Validation (Ready profile)

- Current Go-master six-artifact inventory and complete declaration/test
  mapping — passed.
- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-planner --lib physical_property::required_property_tests::source_return_values_may_be_ignored_like_go -- --exact --nocapture` — passed (1 focused test; 36 diagnostics before the edit).
- The equivalent `logical_property::tests::logical_property_return_may_be_ignored_like_go` and `stats_info::tests::stats_info_returns_may_be_ignored_like_go` commands — passed (1 focused test each; one and six diagnostics respectively before the edit).
- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-planner --lib logical_property::tests -- --test-threads=1` — passed (1/1).
- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-planner --lib stats_info::tests -- --test-threads=1` — passed (4/4).
- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-planner --lib physical_property::required_property_tests -- --test-threads=1` — passed (11/11).
- `cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -p tidb-planner --lib` — passed.
- `cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -p tidb-executor --lib` — passed.
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check` and `git diff --check` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make lint` — passed (repository Ready gate).

The broader `tidb-planner --all-targets` check was attempted and is blocked by
an unrelated existing source-test initializer at
`rust/crates/tidb-planner/tests/core_logical_cte_topn_prune_source.rs:75`,
which omits the pre-existing `RuleContext.allow_agg_push_down` field. No
planner test artifact was changed here. This is a Rust-only batch: no Go
source, import section, Go test function, Bazel file, or module dependency
changed, so `make bazel_prepare` is not required. The package has no
failpoint use.

The repository-level Ready lint gate passes with the receipt and ExecPlan
updates included.

## Risks and boundaries

- Correctness: source-shaped discard regressions plus 11 physical, four stats,
  and one logical owner tests pass; all property arithmetic and exchange
  behavior remains covered by the existing tests.
- Compatibility: only Rust lint policy changed for APIs Go already permits to
  be ignored; no Rust signature or runtime behavior changed.
- Performance: no planner algorithm or allocation path changed.
- Not verified locally: the unrelated all-target planner test compile failure,
  full workspace tests, Bazel execution, and cross-platform planner builds.
