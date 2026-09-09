# `pkg/ddl/resourcegroup` package receipt

Pinned source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete package inventory

| Go artifact | Lines | Blob | Rust disposition |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 18 | `9eddc1cd19ca9827dee918d78789e8864c66ef01` | workspace crate `tidb-ddl-resourcegroup` over the complete model input and vendored kvproto output |
| `errors.go` | 38 | `f7b54cc18800da4cbfc2c74681590bca4721c4bc` | all nine package error identities and exact messages in the crate `Error` enum |
| `group.go` | 88 | `8f895fdc9093c71c8ff57916b0dd78046a9f09b1` | exact `new_group_from_options` conversion in `tidb-ddl-resourcegroup/src/lib.rs` |

There is no package doc, package test, test harness, benchmark, fixture,
generated source/input, build/platform variant, or ownership artifact in the
pinned directory.

## Behavior and integration decision

The crate consumes `tidb-model`'s complete `ResourceGroupSettings` carrier and
emits the real vendored `resource_manager.ResourceGroup` protobuf. It preserves
Go's byte-length name limit, unchecked `uint64`-to-`uint32` priority cast,
runaway validation order, open action/watch ordinals, optional watch shape,
background fields, RU token bucket, RU/raw-option conflict, and RU-only mode
rejection. The remaining DDL create/alter/drop consumers belong to the parent
`pkg/ddl` package claim; no duplicate controller or alternate resource-group
wire type is introduced here.

## WIP validation

Run from `rust/`:

```text
cargo fmt --all -- --check
cargo check --offline -p tidb-ddl-resourcegroup
cargo test --locked -p tidb-ddl-resourcegroup
```

The crate has zero tests, matching the pinned package inventory.
