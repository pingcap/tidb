# `pkg/lightning/importdef` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly two artifacts, both read in full. The local Go package
is byte-identical to the pin.

| Go artifact | Lines | Blob | Rust disposition |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 9 | `4a772b66d3b85642f2eccd8a4e1d097bf5f1d4c4` | `tidb-util::lightning_importdef`; Cargo owns native dependency metadata |
| `tidb.go` | 39 | `ff706a7de26e1243c7c570c1e894cdc0a2c69a6e` | target database and table metadata carriers |

There is no package doc, test, test support, fixture, testdata, benchmark,
generated source, platform variant, README, or ownership artifact.

## Rust ownership and parity result

`rust/crates/tidb-util/src/lightning_importdef.rs` owns the whole package.
`DbInfo` retains the signed ID, byte-preserving Go name, and nil-versus-present
table map. Table keys are byte-preserving Go strings and table values are
shared mutable pointers, matching Go map values of `*TableInfo`.

`TableInfo` retains the signed ID, byte-preserving database/table names, and
nil-versus-present shared mutable pointers to the canonical
`tidb_model::TableInfo` for both current and desired metadata. Its native value
copy preserves pointer identity, and equality compares those pointers rather
than model contents, matching Go's comparable struct. Both carriers expose a
native default for Go's zero value. `DbInfo` deliberately has no clone or
equality capability because a Go struct containing a map is not comparable and
copying a map aliases its header.

No constructors, helpers, tests, documentation policies, or behavior absent
from the two source artifacts were added. There were no prior Rust owners or
production consumers to migrate.

## WIP validation

Profile: WIP. This completes one atomic package in an ongoing repository-wide
parity audit; it is not a Ready or repository-completion claim.

Inventory checks from the repository root:

```text
git ls-tree -r e2788410d8d696605e8cb002585877a063ccc909 pkg/lightning/importdef
git diff --exit-code e2788410d8d696605e8cb002585877a063ccc909 -- pkg/lightning/importdef
```

Passed from `rust/`:

```text
cargo fmt --all
cargo fmt --all -- --check
cargo check --quiet --offline -p tidb-util
cargo clippy --quiet --offline -p tidb-util --lib --no-deps -- -A clippy::map-or-identity -A clippy::chunks-exact-to-as-chunks -A clippy::wrong-self-convention -A clippy::new-without-default -A clippy::len-without-is-empty -A clippy::should-implement-trait -D warnings
git diff --check
```

The source package has no tests and no `failpoint.`, `testfailpoint.`, or Bazel
failpoint dependency match. Its Go package build was attempted without
failpoint enablement:

```text
go test -tags=intest,deadlock ./pkg/lightning/importdef -count=1
```

The host Go 1.27 dependency stack failed before this package: `pkg/util/hack`
has no selected `checkMapABI` implementation and cached gRPC transport refers
to the unavailable HTTP/2 `TrailerPrefix`. No Go, Bazel, module, or generated
artifact changed, so `make bazel_prepare` is not required. The Cargo lockfile
only adds the existing workspace `tidb-model` package to `tidb-util`'s native
dependency list. Cross-platform execution, workspace-wide tests, and the
Ready-profile `make lint` were not run in this WIP iteration. Cargo emitted
only existing warnings in `tidb-model` and the vendored TiKV client.

## Risk

- Correctness: both artifacts and every field/nil state are mapped; the source
  package has no executable test surface.
- Compatibility: public names use Rust casing and byte-preserving strings;
  model and table pointers retain shared mutable identity.
- Performance: the package contains only carriers; its map and pointers retain
  the source allocation/aliasing shapes.
