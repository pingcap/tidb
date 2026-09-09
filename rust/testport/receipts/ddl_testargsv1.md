# `pkg/ddl/testargsv1` package receipt

Pinned source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete package inventory

| Go artifact | Lines | Blob | Rust disposition |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 11 | `f7fac0f94ea9edae6e8c359611d4a9166b94ac51` | workspace crate `tidb-ddl-testargsv1` with matching default and `ddlargsv1` feature builds |
| `force_v1.go` | 20 | `7fac47bed4620c32407570c1c3639d0e0dc51ad7` | `FORCE_V1=true` when the `ddlargsv1` feature is enabled |
| `normal.go` | 23 | `2e11b4e5c3f1a49ecbc7955787a05ad942b8a173` | `FORCE_V1=false` in the default build |

There is no package doc, package test, test harness, benchmark, fixture,
generated source/input, additional build/platform variant, or ownership
artifact in the pinned directory.

## Behavior and integration decision

The crate contains the package's sole fact and preserves both mutually
exclusive Go build-tag variants through one Cargo feature. The parent DDL
package remains responsible for selecting job version 1 versus 2 when this
flag is consumed; no test-only global override or runtime switch is added.

## WIP validation

Run from `rust/`:

```text
cargo check --offline -p tidb-ddl-testargsv1
cargo check --offline -p tidb-ddl-testargsv1 --features ddlargsv1
cargo test --locked -p tidb-ddl-testargsv1
cargo test --locked -p tidb-ddl-testargsv1 --features ddlargsv1
```

Both build variants have zero tests, matching the pinned inventory.
