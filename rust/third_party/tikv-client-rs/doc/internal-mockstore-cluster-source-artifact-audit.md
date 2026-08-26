# `internal/mockstore/cluster` source-artifact audit

This is the atomic completion receipt for client-go's
`internal/mockstore/cluster` package at pinned commit
`52c1e76cec993571493c81de442bcbef90cdc106`. The package defines the common
mock-cluster control contract; concrete topology behavior remains owned by the
separate complete `internal/mockstore/mocktikv` receipt.

## Immutable source inventory

The package contains exactly one artifact and 65 lines. Its Git tree is
`73d8b463381d92552eecd1563cfa4b8d7d54c194`.

| Kind | Source artifact | Lines | SHA-256 |
| --- | --- | ---: | --- |
| production | `internal/mockstore/cluster/cluster.go` | 65 | `896c02836077044750e44b27874d72ea63a844a224e8c86f968fdb23e823162d` |

There is no `doc.go`, test/support file, test harness, benchmark, example,
fixture, metadata file, package build file, generated input/output,
build-tag/platform variant, or `go:generate` directive.

## Complete contract mapping

`src/mock/cluster.rs` owns an object-safe `Cluster` trait with all nine source
methods:

- allocation of IDs shared by stores, regions, and peers;
- region, leader, buckets, and down-peer lookup by key;
- complete store enumeration;
- transaction/region delay scheduling;
- encoded-key and raw-key region splitting;
- even range splitting with a signed Go-`int`-compatible count;
- variadic-label store insertion and store removal.

Go pointer results map to Rust `Option`, slices map to owned or borrowed
vectors/slices according to ownership, and `time.Duration` maps to
`std::time::Duration`. Shared references preserve concurrent simulator use via
interior synchronization. The trait remains object-safe, matching a Go
interface value.

The concrete `src/mock/mocktikv/cluster.rs` implementation covers every method
through an explicit compile-time trait implementation. `src/testutils.rs`
re-exports the same trait for ordinary downstream builds. No parallel trait or
adapter contract exists.

## Tests and consumers

The pinned package has no Go test declaration or lifecycle harness. Rust's
`cluster_interface_is_object_safe_and_complete` conformance test invokes every
method through `&dyn Cluster`, including absent optional results, negative
split counts, labels, and both split forms. Concrete behavior is exercised by
the complete mocktikv source-derived and RPC adapter tests.

Mechanical source matching finds exactly two direct Go importers:

- `internal/mockstore/mocktikv/cluster.go`, which asserts the concrete
  implementation;
- `testutils/mockstore.go`, which exposes the public test-support alias.

The Rust edges are exactly the corresponding concrete implementation and
ordinary-build `testutils` re-export. Integration suites consume that facade
indirectly and remain owned by their package/live receipts.

## Validation

The original package compiles with the pinned Go 1.25.12 toolchain in ordinary
and race modes and reports `[no test files]`. Rust validation uses
`nightly-2026-08-22`:

```text
go test ./internal/mockstore/cluster -count=1
go test -race ./internal/mockstore/cluster -count=1
cargo test -p tikv-client --lib mock::cluster::tests::cluster_interface_is_object_safe_and_complete
cargo test -p tikv-client --lib mock::mocktikv
cargo check --workspace --all-targets --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo doc --workspace --all-features --no-deps --document-private-items
cargo test --workspace --doc --all-features
cargo fmt --all -- --check
git diff --check
```

The complete package has no real-cluster requirement: it is an interface-only
test-support boundary, and protocol/runtime behavior remains with mocktikv and
the repository live differential.
