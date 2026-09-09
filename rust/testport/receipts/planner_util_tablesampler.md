# `pkg/planner/util/tablesampler` — Go-master parity audit receipt

Go authority: `origin/master` at
`aec988ea500de42dd6c8b2cf429dff907ce5bd41`.

## Complete Go inventory

The package contains exactly two tracked artifacts and 74 lines. Every
production file and build target was read in full before editing:

| Artifact | Lines | Role |
| --- | ---: | --- |
| `BUILD.bazel` | 14 | public Go library target and dependencies |
| `sample.go` | 60 | table-sample metadata and memory accounting |

The declaration inventory was checked function by function:
`(*TableSampleInfo).MemoryUsage` and `NewTableSampleInfo`, together with the
`TableSampleInfo` fields and nil/clone behavior. The package has no Go test
file, `doc.go`, `TestMain`, benchmark, fuzz target, example, fixture/testdata
tree, generated source, platform/build-tag variant, nested package, or other
support artifact. The BUILD file lists exactly the one production source file
above.

## Rust ownership and parity

The dependency-closed owner is
`rust/crates/tidb-planner/src/table_sampler.rs`. Its table-sample metadata,
schema clone, selected partition IDs, memory accounting, constructor, inline
owner test, and direct planner/read-only-scan consumers were re-read. Go
permits callers to discard both `MemoryUsage` and `NewTableSampleInfo` results;
Rust had imposed `#[must_use]` on both source-shaped APIs. Those annotations
were removed. Rust-owned schema/partition storage, nil-node handling,
clone-on-construction behavior, byte accounting, and physical planner wiring
remain unchanged.

The focused regression uses `#[deny(unused_must_use)]` and discards both
source-shaped results. In a temporary clean worktree at the pre-fix revision it
failed with exactly two `unused return value` diagnostics; after the edit it
passes 1/1.

## Validation (Ready profile)

- Current Go-master two-artifact inventory and complete declaration mapping —
  passed.
- Temporary clean-worktree probe:
  `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -q -p tidb-planner --lib table_sampler::tests::source_return_values_may_be_ignored_like_go -- --exact --nocapture` — failed before the edit with exactly two diagnostics (captured in `/tmp/tidb-codex/tablesampler-prefix.log`).
- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -q -p tidb-planner --lib table_sampler::tests::source_return_values_may_be_ignored_like_go -- --exact --nocapture` — passed after the edit (1/1).
- `cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -q -p tidb-planner --lib` — passed.
- `rustfmt --check rust/crates/tidb-planner/src/table_sampler.rs` and
  `git diff --check` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make lint` — passed (repository Ready gate).

The package has no Go test target and no failpoint use. This is a Rust-only
batch: no Go source, import section, Go test function, Bazel file, or module
dependency changed, so `make bazel_prepare` is not required. The generated
planner aggregate, full workspace tests, Bazel execution, and cross-platform
planner builds were not run; the aggregate remains blocked by the unrelated
`tests/core_logical_cte_topn_prune_source.rs:75` missing
`RuleContext.allow_agg_push_down` initializer recorded by adjacent receipts.

## Risks and boundaries

- Correctness: the fail-before/pass-after regression and planner library check
  pass; metadata cloning, memory accounting, and partition selection are
  unchanged.
- Compatibility: only Rust lint policy changed for APIs whose Go callers may
  discard results.
- Performance: no metadata layout, cloning, allocation, or accounting path
  changed.
- Not verified locally: the generated aggregate target, full workspace tests,
  Bazel execution, and cross-platform planner builds.
