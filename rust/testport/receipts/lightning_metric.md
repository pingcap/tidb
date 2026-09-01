# `pkg/lightning/metric` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly three artifacts, all read in full. The local Go package
is byte-identical to the pin.

| Go artifact | Lines | Blob | Rust disposition |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 28 | `08a6bfb99d25cb3d3c548f56952c9e0a60fc950d` | `tidb-util::lightning_metric`; Cargo and `promutil` own native dependency, registry, and test metadata |
| `metric.go` | 479 | `3ce621cbce996e94e57b5c1a7f82f8e82295a3bd` | complete collector definitions, lifecycle, readings, result recording, and context propagation |
| `metric_test.go` | 156 | `7df213fef3da5ee665d4aab22e055f08b82583c3` | exactly six functional source tests |

There is no package doc, fixture, testdata, benchmark, generated source,
platform variant, README, or ownership artifact. Bazel dependency declarations
have no additional runtime behavior beyond the native Rust dependencies.

## Rust ownership and parity result

`rust/crates/tidb-util/src/lightning_metric.rs` owns the complete package. It
defines the exact label values, eight common collectors, fourteen Lightning
collectors, metric names, help text, namespaces, label dimensions, exponential
buckets, registration order, unregistration lifecycle, table/engine result
selection, counter and histogram readings, counter-vector label filtering, and
nested context propagation from the pinned Go source.

Rust's Prometheus counter and histogram handles expose infallible reads, so
Go's unreachable native collector `Write` error maps to the successful value
path. `read_all_counters` preserves Go's unusual match rule: a metric is
included when at least one of its label pairs exactly matches an entry in the
requested label map. The native registry has no Go `Describe` channel API, so
the source registration tests verify the same collector counts and actual
register/unregister outcomes through the registry interface.

Exactly `TestReadCounter`, `TestReadHistogramSum`, `TestRecordEngineCount`,
`TestMetricsRegister`, `TestMetricsUnregister`, and `TestContext` remain as
snake-case Rust test identities. There is no prior Rust owner, duplicate
collector set, supplemental test, or Rust-only metric policy.

## WIP validation

Profile: WIP. This completes one atomic package in an ongoing repository-wide
parity audit; it is not a Ready or repository-completion claim.

Inventory checks from the repository root:

```text
git ls-tree -r e2788410d8d696605e8cb002585877a063ccc909 pkg/lightning/metric
git diff --exit-code e2788410d8d696605e8cb002585877a063ccc909 -- pkg/lightning/metric
rg -n 'failpoint\.|testfailpoint\.|failpoint' pkg/lightning/metric
```

Passed from the repository root:

```text
go test -run '^(TestReadCounter|TestReadHistogramSum|TestRecordEngineCount|TestMetricsRegister|TestMetricsUnregister|TestContext)$' -tags=intest,deadlock ./pkg/lightning/metric -count=1
```

Passed from `rust/`:

```text
cargo fmt --all
cargo fmt --all -- --check
cargo test --quiet --offline -p tidb-util lightning_metric --lib -- --test-threads=1
cargo check --quiet --offline -p tidb-util
cargo clippy --quiet --offline -p tidb-util --lib --no-deps -- -A clippy::map-or-identity -A clippy::chunks-exact-to-as-chunks -A clippy::wrong-self-convention -A clippy::new-without-default -A clippy::len-without-is-empty -A clippy::should-implement-trait -D warnings
```

The package has no failpoint use or dependency. No Go, Bazel, module, or
generated artifact changed, so `make bazel_prepare` is not required.
Cross-platform execution, workspace-wide tests, and the Ready-profile
`make lint` were not run in this WIP iteration. Cargo emitted only the existing
`tidb-model` `unused_mut` and vendored TiKV-client `private_bounds` warnings.

## Risk

- Correctness: all three artifacts and production branches are mapped; exactly
  the six source test identities pass in both Go and Rust.
- Compatibility: context values retain shared metric identity; native
  Prometheus factory and registry boundaries replace their Go interfaces.
- Performance: collector operations retain the source shapes and add no
  sampling, caching, or synchronization policy.
