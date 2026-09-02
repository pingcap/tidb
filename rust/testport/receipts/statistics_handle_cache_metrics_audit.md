# `pkg/statistics/handle/cache/metrics` audit

Pinned source: `c6054025ed4c32ab3672a2a24ea46892714d21ec` (Go `master` at the
audit boundary).

## Atomic inventory

| Artifact | Lines | Git blob | SHA-256 | Disposition |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 12 | `719f0a3357c85bfb7a938f5abce618808ffc1686` | `c5d4d299524794699bc3fb70b021d3c1a06a33b3dca8f27cb86292a21d7a69cb` | build metadata inventoried |
| `metrics.go` | 55 | `c32e59ffcca683bed0884605d4ffe385849f9d43` | `87276846f34ca41cc839dcefe6c33a6b8c575a9bfb36d2b98fa8bc397bfdbc94` | unclaimed: shared runtime dependency absent |

This is the complete Go package: two artifacts and 67 lines. It has no
generated, platform-specific, test, fixture, or benchmark artifacts. The
current checkout is byte-identical to the pinned Go master source.

## Behavior and blocker

Package initialization calls `InitMetricsVars`. That function binds six
exported counters (`miss`, `hit`, `update`, `del`, `evict`, and `reject`) and
two exported gauges (`track` and `capacity`) to child handles of the shared
`pkg/metrics.StatsCacheCounter` and `pkg/metrics.StatsCacheGauge` vectors.

The pinned `pkg/metrics` package is not a completed Rust owner. Its direct
package inventory contains 33 artifacts, including the construction and
default-registry registration of these two shared vectors. Creating private
vectors in this leaf would change collector identity, registration, resets,
gathering, and every caller that uses the shared parent handles. It would not
be Go parity.

The former Rust `cache_metrics_labels` module retained only the eight label
strings and added two tests that do not exist in the pinned package. It had no
counters, gauges, initialization, registration, or update behavior. The
module and both tests were removed. The current leaf crate still creates
private Prometheus vectors as executable seed evidence; that is Rust-only
collector identity and cannot be accepted as Go behavior. This Go package
remains explicitly unclaimed until the complete shared `pkg/metrics`
dependency is available.

## Validation

Ready profile: this is a source-test-free blocker receipt refresh. The complete
Go inventory, current Rust seed owner, affected statistics owner, and hygiene
gates were checked without claiming private collector identity as parity.

- current and detached `go test ./pkg/statistics/handle/cache/metrics -count=1` (`[no test files]`)
- `env OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-stats-handle-cache-metrics`
- `env OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... cargo +nightly-2026-08-22 clippy --manifest-path rust/Cargo.toml --offline --locked -p tidb-stats-handle-cache-metrics --no-deps -- -D warnings`
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`
- pinned `make lint`
- `git diff --check`

`make bazel_prepare` is not required because no Go/Bazel source changed. Full
completion is blocked on the atomic `pkg/metrics` owner: private vectors in
this leaf would change registry identity, reset/gather behavior, and every
consumer's shared parent handles.
