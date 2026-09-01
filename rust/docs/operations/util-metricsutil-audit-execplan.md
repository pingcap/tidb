# `pkg/util/metricsutil` parity audit ExecPlan

## Purpose

Inventory the complete Go metrics utility package and compare its process-wide
registration and BR/PD keyspace behavior with the Rust workspace at the
current Go-master pin.

## Progress

- Read all three artifacts: `common.go` (167 lines), `common_test.go` (53
  lines), and `BUILD.bazel` (49 lines), 269 lines total.
- Enumerated eight production functions and the package-global PD component
  identity. `RegisterMetrics` and `RegisterMetricsForBR` preserve labels,
  optional next-gen keyspace metadata, TLS and ten-second PD timeout, retry
  classification, and full cross-subsystem initializer order. The source test
  covers cloned labels, configured observability labels, and keyspace-ID
  publication.
- Confirmed there are no package docs, fixtures, generated/platform variants,
  benchmarks, fuzz targets, examples, nested packages, or additional harnesses.
- Compared with Go master
  `c6054025ed4c32ab3672a2a24ea46892714d21ec`; no Go source delta exists.
- Searched Rust metrics, keyspace, BR, PD-client, and store crates. Rust has
  split collector fragments but no dependency-closed process registry or BR/PD
  composition root implementing this complete contract.

## Decision

Keep `pkg/util/metricsutil` explicitly unclaimed. A Rust-only central registry,
label map, or PD adapter could double-register collectors or alter labels,
ordering, timeout, and retry behavior. No production change or additional
regression test is justified until the registry and its consumers can move as
one owner.

## Validation

- Active checkout: `go test ./pkg/util/metricsutil -count=1` — passed.
- Detached Go-master checkout: same focused suite — passed.
- Rust ownership search completed; no dependency-closed owner suite exists.
- Ready gates: Rust fmt check, pinned `make lint` in the clean detached
  checkout, and `git diff --check`.

## Risks and follow-up

Full metric-family initialization, duplicate-registration handling, PD
not-bootstrapped retries, and downstream BR integration remain unverified. A
future claim must preserve initialization order and exact label/timeout/retry
semantics while moving all affected consumers.
