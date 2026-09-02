# `pkg/parser/tidb` parity audit ExecPlan

## Objective

Inventory every Go artifact in the parser feature-ID package, restore the
Go-master feature registry, add a focused regression, and record the Rust
ownership boundary.

## Progress

- [x] (2026-09-02) Read all two pre-edit artifacts (75 lines), including BUILD
      metadata; confirmed no fixtures, generated/platform variants, or hidden
      package test inputs.
- [x] (2026-09-02) Restored `FeatureIDPreSplit`, its deprecated compatibility
      alias, and `FeatureIDAutoPreSplit` allowlisting.
- [x] (2026-09-02) Added `TestCanParseFeaturePreSplitVariants` and the Bazel
      test target.
- [x] (2026-09-02) Ran the Ready gates, committed only this package plus its
      receipt/ExecPlan, pushed to `hparser-integration`, verified the remote
      SHA, and fast-forward pulled.

## Constraints and ownership

The registry is Go-owned. No Rust crate currently consumes it, so no
dependency-free Rust facade should be introduced. `make bazel_prepare` is
required by the BUILD/test-target change but is expected to remain blocked by
the missing local Bazel executable.
