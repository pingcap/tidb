# `pkg/planner/core/metrics` — Go-master parity audit receipt

Go authority: `origin/master` at
`aec988ea500de42dd6c8b2cf429dff907ce5bd41`.

## Complete Go inventory

The package contains exactly two tracked artifacts and 130 lines. Every
production file and build target was read in full before editing:

| Artifact | Lines | Role |
| --- | ---: | --- |
| `BUILD.bazel` | 12 | public Go library target and metrics/prometheus dependencies |
| `metrics.go` | 118 | planner pseudo-estimation and plan-cache metric children/accessors |

The declaration inventory was checked function by function:
`InitMetricsVars`, `GetPlanCacheHitCounter`, `GetPlanCacheMissCounter`,
`GetNonPrepPlanCacheUnsupportedCounter`, `GetPlanCacheInstanceNumCounter`,
`GetPlanCacheInstanceMemoryUsage`, `GetPlanCacheCloneDuration`,
`GetPlanCacheLookupDuration`, and `GetPlanCacheInstanceEvict`, plus all 15
metric variables and their exact labels. The package has no Go test file,
`doc.go`, `TestMain`, benchmark, fuzz target, example, fixture/testdata tree,
generated source, platform/build-tag variant, nested package, or other support
artifact. The BUILD file lists exactly the one production source file above.

## Rust ownership and parity

The dependency-closed owner is `rust/crates/tidb-planner/src/metrics.rs`. Its
metric families, labels (including Go's leading spaces), lazy child binding,
histogram buckets, inline routing test, and direct planner consumers were
re-read. Go accessors return Prometheus child handles that callers may discard;
Rust had imposed `#[must_use]` on all ten source-shaped metric getters:
pseudo-estimation children, prepared/non-prepared hit and miss counters,
unsupported counter, instance plan/memory gauges, eviction gauge, lookup
histogram, and clone histogram. Those annotations were removed. Metric names,
help strings, labels, child identity, lazy initialization, and bucket ranges
remain unchanged.

The focused regression uses `#[deny(unused_must_use)]` and discards all ten
source-shaped handles. In a temporary clean worktree at the pre-fix revision it
failed with exactly ten `unused return value` diagnostics; after the edit it
passes 1/1.

## Validation (Ready profile)

- Current Go-master two-artifact inventory and complete declaration mapping —
  passed.
- Temporary clean-worktree probe:
  `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -q -p tidb-planner --lib metrics::tests::source_return_values_may_be_ignored_like_go -- --exact --nocapture` — failed before the edit with exactly ten diagnostics (captured in `/tmp/tidb-codex/metrics-prefix.log`).
- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -q -p tidb-planner --lib metrics::tests::source_return_values_may_be_ignored_like_go -- --exact --nocapture` — passed after the edit (1/1).
- `cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -q -p tidb-planner --lib` — passed.
- `rustfmt --check rust/crates/tidb-planner/src/metrics.rs` and
  `git diff --check` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make lint` — passed (repository Ready gate).

The package has no Go test target and no failpoint use. This is a Rust-only
batch: no Go source, import section, Go test function, Bazel file, or module
dependency changed, so `make bazel_prepare` is not required. Full workspace
tests, Bazel execution, and cross-platform planner builds were not run.

## Risks and boundaries

- Correctness: the fail-before/pass-after regression and planner library check
  pass; metric child routing and registration behavior are unchanged.
- Compatibility: only Rust lint policy changed for source-shaped metric
  getters whose Go callers may discard handles.
- Performance: no metric family, child binding, allocation, or observation path
  changed.
- Not verified locally: full workspace tests, Bazel execution, and
  cross-platform planner builds.
