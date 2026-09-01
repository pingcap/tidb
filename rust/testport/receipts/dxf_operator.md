# `pkg/dxf/operator` package receipt

Pinned source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete package inventory

| Go artifact | Lines | Blob | Rust disposition |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 31 | `4f6230db9499f95837c70fc19ff2280e83609969` | workspace crate `tidb-dxf-operator` and its single package test target |
| `compose.go` | 60 | `cc5ba69b2062d4ce66720e92c0f1ebf15dd301bf` | `compose.rs`: source/sink seams, unbuffered shared channel, finish semantics, and composition |
| `operator.go` | 131 | `5724536d6ac5292c9f2d7c2d4bd1cc32c021c140` | `operator.rs`: operator and tuning contracts, first-error cancellation context, panic-safe workers, transforms, pool lifecycle, and resizing |
| `pipeline.go` | 89 | `179c2307ad7846b51c21aae1e3cd99e728d40e31` | `pipeline.rs`: ordered open, reverse cleanup after open failure, ordered close with first error, started state, string form, and four-stage reader/writer lookup |
| `pipeline_test.go` | 129 | `d4c71f55f87b4c5bb09dc66ff9072b7d2494d497` | exact success/error branches execute in `pipeline_test.rs`, including the source string, transformations, concurrent count, cancellation, and collected `hit` result |
| `wrapper.go` | 141 | `0953ea44123d3fadedc384ecc42c12741bd986ef` | `wrapper.rs`: simple data source, package-private sink and transforming operator, shared-context cancellation, drain, and close behavior |

There is no package doc, fixture, benchmark, generated artifact, platform
variant, or other test in the pinned directory.

## Native integration decision

Go implements `AsyncOperator` over `pkg/resourcemanager/pool/workerpool`.
Rust now uses the canonical `tidb_resourcemanager::workerpool` implementation
through the same package boundary. The earlier DXF-local context, worker,
panic, lifecycle, and tuning implementation was removed rather than retained
as a second execution path.

Go's channel type is bidirectional and closeable. `SimpleDataChannel` keeps one
shared close state around a zero-capacity native channel owned by the shared
resource-manager channel carrier. Every composed handoff is unbuffered, and a
second public finish panics like closing a closed Go channel. The native
`NoResult` marker is the spelling of external `workerpool.None`; ordinary
operators without a configured result consumer retain Go's blocking result
channel instead of silently discarding output.

## WIP validation

Run from `rust/`:

```text
cargo fmt --all -- --check
cargo test --locked -p tidb-dxf-operator
git diff --check
```
