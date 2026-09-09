# `pkg/util/metricsutil` — Go-master package boundary receipt

Go source: `origin/master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec` (2026-09-02). The package is
byte-for-byte unchanged from the earlier extraction pin
`e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

All three Go-master artifacts were read in full before deciding ownership:

| Artifact | Lines | SHA-256 | Inventory |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 49 | `67f0755c9f2a862726917251a86902dc1b3e615cf14885f01ce2a54cb7d43eaa` | library/test targets and the complete metrics, BR/PD, keyspace, store, SQL, and observer dependency set |
| `common.go` | 167 | `9c1c613a8932e48a9bd67642499eead447af0ab6a4f589f24372157c48764346` | process/BR registration, keyspace labels, all metrics-family initialization calls, and retrying PD keyspace lookup |
| `common_test.go` | 53 | `7a28867ebb90cbc998dec7aa93b56dd6974d2074b2867658bd1cb0923b4802a1` | one source test covering cloned labels, configured observability labels, and keyspace-ID publication |

There is no `doc.go`, generated or platform variant, fixture/testdata tree,
benchmark, fuzz target, example, nested package, or additional harness. The
package has 269 Go lines, eight production functions, one package-global PD
component identity, and one source test. Its current-master source delta is
empty.

## Go behavior and consumers

`RegisterMetrics` publishes next-gen keyspace-name labels, then initializes the
base registry and every domain, executor, infoschema, isolation, planner,
server, session, statistics, TopSQL, TTL, transaction, and optional UniStore
metric family. `RegisterMetricsForBR` applies the BR keyspace label policy,
constructs a PD client with TLS and a ten-second timeout, retries only
not-bootstrapped or missing-keyspace responses, publishes the keyspace ID, and
then performs the same initialization. Labels are cloned before configuration
values are merged so existing process labels survive registration.

## Rust ownership and decision

Rust has metric fragments in `tidb-exec`, `tidb-distsql`,
`tidb-stats-handle-*`, `tidb-ddl-*`, and other crates, plus keyspace-label
configuration in `tidb-config`. It does not have the Go process-wide metrics
registry or the complete family of initializers listed by `initMetrics`, nor a
BR/PD client composition root that can perform the source's retry and label
publication contract. Individual Prometheus collectors in one crate cannot
stand in for the cross-crate initialization order and optional UniStore path.

Adding a Rust-only central registry, label map, or PD lookup adapter would
duplicate existing metrics state and risk double registration, altered label
sets, or different retry behavior. No production Rust change or focused
regression was therefore added; this complete package remains explicitly
unclaimed until the metrics registry and BR/PD consumers move as one unit.

## Validation

Profile: **Ready** for this documentation-only boundary refresh; no Rust code
changed.

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/metricsutil -count=1` — passed (the one source test).
- `(cd /tmp/tidb-go-latest-c605 && PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/metricsutil -count=1)` — passed (the one source test).
- `git diff --stat c6054025ed4c32ab3672a2a24ea46892714d21ec -- pkg/util/metricsutil` — empty; source is unchanged at current Go master.
- Rust search of metrics, keyspace, BR, PD-client, and store crates — confirmed split owners and no dependency-closed `metricsutil` registry.

`cd rust && cargo +nightly-2026-08-22 fmt --all -- --check` and
`git diff --check` passed. The pinned `make lint` gate passed in the clean
detached Go-master checkout; the active checkout may be temporarily
instrumented by a concurrent failpoint test worker. No Go or Bazel file
changed, so `make bazel_prepare` is not required. Full metrics registration,
BR keyspace lookup, and live PD retry tests were not run for this explicitly
unclaimed boundary.

## Risks and unverified scope

- Correctness: the source test covers label cloning/publication; the registry
  initialization fan-out remains Go-owned.
- Compatibility: any future port must preserve initialization order, exact
  const-label merge behavior, next-gen/BR distinctions, TLS and ten-second PD
  timeout, and retry classification.
- Performance: no runtime metrics registration or PD request was added.
- Not verified locally: all downstream metric families, duplicate-registration
  handling, PD-not-bootstrapped retries, and BR integration.
