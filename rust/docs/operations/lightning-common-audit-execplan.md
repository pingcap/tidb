# `pkg/lightning/common` parity audit ExecPlan

## Objective

Inventory every Go-master artifact in Lightning's shared common package,
including platform variants, error/retry policy, connection/TLS/storage
helpers, allocator/key contracts, tests, benchmarks, failpoints, and BUILD
metadata, then determine whether Rust has a dependency-closed owner.

## Completed

- Read all 24 pinned Go-master artifacts in full: 3,875 lines across 13
  production files, ten test/support files, BUILD metadata, and Unix/Windows
  storage variants.
- Counted 94 production declarations, 31 `TestXxx` functions (including the
  `TestMain` harness), three benchmarks, two logical failpoints, and the
  30-shard flaky test target.
- Mapped allocator discovery/rebase, gRPC pools, duplicate KV capture, error
  normalization, key adapters, atomic pause gates, transient-error policy,
  TLS/HTTP/PD/TiKV conversion, platform disk accounting, SQL retry helpers,
  identifier/index builders, and row-count safety.
- Confirmed no fixtures, testdata, fuzz inputs, generated source inputs,
  package docs, or extra artifacts exist beyond the platform files and BUILD
  select.
- Compared the hparser branch with Go master; this package is byte-identical.
- Searched Rust Lightning and utility crates and confirmed no
  dependency-closed `pkg/lightning/common` owner or consumer.

## Validation gate

- [x] Current-branch failpoint-aware package suite passes after a clean
      failpoint-generation rerun and returns refcount to zero.
- [x] Detached exact-Go-master failpoint-aware package suite passes and
      returns refcount to zero.
- [x] Ready formatting, lint, and diff checks pass for this receipt batch.
- [ ] Push the receipt/ExecPlan batch to `origin/hparser-integration`, verify
      local/tracking/advertised SHAs, and pull the explicit branch ref.

`make bazel_prepare` is not required because the batch changes no Go, Bazel,
module, generated, or Rust source.

## Remaining boundary

An executable Rust port must move the concrete TiDB/TiKV/PD clients, Pebble
storage, auto-ID service, SQL executor, table metadata, platform disk APIs, and
all downstream Lightning consumers with these shared contracts. Keep this
package as an explicit boundary until that dependency closure exists; do not
add an isolated utility facade or ignored parity tests.
