# `pkg/planner/util/coretestsdk` Rust return-contract alignment

Date: 2026-09-07

The user narrowed this pass to Rust code. The original complete package
inventory and Go mapping remain pinned in
`rust/docs/planner/coretestsdk-package-parity-execplan.md`; no Go source was
read or changed during this follow-up.

## Complete Rust owner inventory

Before editing, every Rust owner artifact was read in full:

| Artifact | Lines | Role |
| --- | ---: | --- |
| `rust/crates/tidb-planner-coretestsdk/Cargo.toml` | 18 | package metadata and five path dependencies |
| `rust/crates/tidb-planner-coretestsdk/src/lib.rs` | 1,175 | all production helpers, ownership adapters, and four inline tests |

The workspace member and lockfile package entry were checked. No other crate
depends on or calls this test-support crate. It has one `cfg(test)` module and
no external test target, fixture directory, compile-time include, generated
input/output, platform variant, feature, example, benchmark, fuzz target, or
custom build script. After the regression and correction, the same two owner
artifacts contain 1,201 lines.

## Alignment and regression

The base owner had 23 `#[must_use]` annotations. Eighteen decorated direct
counterparts of Go package APIs: the ten table/infoschema fixture constructors,
`GetFieldValue`, `MockContext`, the four `PlannerSuite` getters, and both suite
constructors. Those annotations added Rust-only compiler behavior without
changing a value or runtime result, so they were removed.

The five annotations on Rust ownership seams remain:
`MockInfoSchema::{new,tables}` and
`MockContext::{info_schema,current_database,div_precision_increment}`. These
helpers adapt Go pointer/field access to Rust ownership and are not direct
source API contracts.

The inline `tests::source_api_returns_may_be_ignored_like_go` regression calls
and discards all 18 source-shaped returns under `#[deny(unused_must_use)]`.
With only the regression applied to base
`177449fd087d42d91d70853a549aebae1a5eb4da`, the focused build failed with
exactly 18 `unused_must_use` errors. It passes after the correction, while an
annotation count confirms the five Rust-native contracts remain.

## Validation

Ready profile:

- Focused regression: 1 passed, 4 filtered out.
- `cargo +nightly-2026-08-22 nextest run --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner-coretestsdk --no-fail-fast`: all 5 tests passed.
- `cargo +nightly-2026-08-22 check --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner-coretestsdk --all-targets`: passed with pre-existing dependency warnings only.
- `cargo +nightly-2026-08-22 clippy --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner-coretestsdk --all-targets --no-deps -- -D warnings`: passed; dependency warnings remained non-fatal and the owner emitted no warning.
- Scoped nightly `rustfmt`, repository `make lint`, and `git diff --check`: passed.

No Go file, import, Bazel file, module, Rust manifest, generated artifact, or
platform source changed, so `make bazel_prepare` was not required. Runtime and
performance risk are negligible because only compile-time discard diagnostics
changed; the complete owner suite protects the fixture and planner-test
behavior.
