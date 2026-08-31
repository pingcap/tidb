# `pkg/ddl/mock` package receipt

Pinned source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete package inventory

| Go artifact | Lines | Blob | Rust disposition |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 16 | `bde7b92a40f6d9b31e45661748bb1925aef3d394` | public workspace test-support crate `tidb-ddl-mock`, over the source-owned DDL and system-table traits |
| `schema_loader_mock.go` | 58 | `faa595e7e337fcd397bdb5babdb6d99500fc61bd` | complete one-method `MockSchemaLoader` recorder in `tidb-ddl-mock/src/lib.rs` |
| `systable_manager_mock.go` | 122 | `5ccaedcdc6a32ede16abf6621648d201381402c0` | complete five-method `MockManager` recorder in `tidb-ddl-mock/src/lib.rs` |

Both Go production files are MockGen output and retain their generator command
in the pinned headers. There is no package doc, package test, fixture,
benchmark, platform variant, or separate generator input in the directory.

## Behavior and integration decision

GoMock's reflection controller and matcher syntax are language mechanics, not
TiDB behavior. As with the completed `pkg/util/sqlexec/mock` package, Rust uses
a native recorder: expectations of each method are consumed in registration
order; unexpected calls panic; explicit `verify` and drop reject unconsumed
calls. The doubles implement the real package-owned traits rather than a
duplicate mock-only interface.

Go's supplied `*session.Session` in `GetJobBytesByIDWithSe` denotes the active
transaction used for that lookup. Rust's system-table owner operates directly
on the active `MetaSnapshot`, so the trait and generated double carry that same
borrowed transaction as `&mut dyn MetaSnapshot`; no cache or alternate query
path is introduced. A sized forwarding adapter lets the existing concrete
`SystemTableManager` implement the object-safe interface without changing its
storage behavior.

The source `SchemaLoader` seam is consumed by the real owner scheduler. On
becoming owner, Rust now retries catalog reload every exact source interval of
one second until success or cancellation. The three pinned
`TestMustReloadSchemas` scenarios and the complete `TestUnSyncedJobTracker`
contract execute in the mock crate, replacing the former ignored gap stubs.

## WIP validation

Run from `rust/`:

```text
cargo fmt --all -- --check
cargo test --locked -p tidb-ddl-mock
cargo check --locked -p tidb-server --tests
git diff --check
```

Warnings from existing workspace crates predate this package.
