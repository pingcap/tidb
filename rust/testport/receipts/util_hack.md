# `pkg/util/hack` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

| Artifact | Bytes | Blob |
| --- | ---: | --- |
| `BUILD.bazel` | 712 | `339fcc2c23972b7f1ec1a5a6fd0797f9dba3c441` |
| `hack.go` | 1,741 | `a8fd421029d7fef2f0c7bc636227be9d2c63f30f` |
| `hack_test.go` | 1,340 | `bc6e2dffc7d511601dfc350a4d6015bd87511962` |
| `main_test.go` | 1,189 | `34dd6ee7f2e6fcf20a44b5a958833096fac1809e` |
| `map_abi.go` | 13,536 | `bb685923e51f046841b72cfb0a6925d7e74eb023` |
| `map_abi_go126.go` | 13,338 | `c091a9d356ea3056df859d64b8bb074aa2972665` |
| `map_abi_test.go` | 5,782 | `ce56c89c7be71a8db58ff4b4f8cd73eeb1ad21d0` |
| `map_abi_test_type_go125_test.go` | 715 | `ef58a5f58d970709d94afd43ed07353aab94e03c` |
| `map_abi_test_type_go126_test.go` | 705 | `a54a15d76e961537bba70cd56e8afc09a8425915` |

There is no `doc.go`, fixture, generated source, ownership file, or other
platform variant. The package has three ordinary tests, one combined map ABI
test, and two benchmarks.

## Rust ownership and behavior

`tidb-hack` owns the complete package. `lib.rs` owns the deliberately aliased
byte/string views, raw-pointer byte view, and six exported map-memory
constants. `map.rs` owns the common public behavior of both Go build-tagged
runtime variants: source type geometry, eight-slot groups, 1,024-slot table
limit, extendible directory splitting, exact allocation size, seed/clear
state, and checkpointed `MemAwareMap` deltas. `benches/hack.rs` contains both
source benchmark identities and all four source input sizes.

Rust cannot read Go runtime type descriptors, so `MapValueLayout` carries the
same size/alignment input explicitly at native type boundaries. `GoString`
supplies Go's 16-byte string header from `tidb-datatype`; primitive, empty,
slice, string, and pointer counterparts are owned by `tidb-hack`. The
memory-aware decimal map stores a boxed value, matching Go's pointer-valued
map. The source map model reconstructs group growth, per-table split
distribution, directory depth, and retained allocation only for `Init`,
`RealBytes`, and `clear`; ordinary `Set` follows Go's constant-time
length/checkpoint path and exact 204/1000 policy independently of hashbrown's
allocation policy.

The retained `TestString`, `TestByte`, and `TestMutable` are direct identities
of `hack_test.go`. The single retained `TestSwissTable` covers every block in
the source test, including group sizes 136/72/24/136/264/200, slot offsets,
seeded key discovery, seed rotation, directory length 4, sizes 184, 360,
102,608 and 2,165,296, cumulative bytes 2,702,278, the 75% checkpoint bound,
clear allocation retention, and `SetExt` insertion reporting. No supplemental
unit test remains. Go's `TestMain` only installs the repository Go test setup
and ignores unrelated Go goroutines; this Rust crate starts no background
worker and therefore needs no package process hook.

All 118 pinned TiDB consumers of `hack.Slice` use the returned bytes read-only;
Rust preserves that complete observable use while refusing to manufacture a
mutable reference from an immutable `str`. The raw-pointer consumers are also
read-only. The Go-version ABI panic is unnecessary because Rust owns the
source layout model rather than casting a Rust runtime map to Go-private
structures.

The audit removed allocator statistics/profile configuration that has no
counterpart in `pkg/util/hack`; that native seam now lives in
`tidb-allocator-stats` for the existing `pkg/util/memory` and
`pkg/util/memoryusagealarm` owners. It also removed source-absent snapshot,
length, conversion and inspection helpers, the extra pointer-window and seed
panic tests, and the extra spare-capacity test scenario.

## WIP validation

- `cargo fmt --manifest-path rust/Cargo.toml --all -- --check`
- `cargo test --manifest-path rust/Cargo.toml -p tidb-hack --lib`
- `cargo test --manifest-path rust/Cargo.toml -p tidb-util set:: --lib`
- `cargo check --manifest-path rust/Cargo.toml -p tidb-hack -p tidb-datatype -p tidb-util`
- `cargo check --manifest-path rust/Cargo.toml -p tidb-util --features jemalloc`
- `cargo check --manifest-path rust/Cargo.toml --locked -p tidb-hack --bench hack`
- `cargo check --manifest-path rust/Cargo.toml --locked -p tidb-server --lib`
- `cargo clippy --manifest-path rust/Cargo.toml --locked -p tidb-hack --all-targets -- -D warnings`
- `cargo clippy --manifest-path rust/Cargo.toml --locked -p tidb-allocator-stats --all-targets --features jemalloc -- -D warnings`

All commands passed. Cargo emitted only existing warnings in `tidb-model` and
the vendored `tikv-client`. No Go or Bazel source changed, so
`make bazel_prepare` is not required. The local Go toolchain is Go 1.27, while
the pinned package intentionally provides ABI files only for Go 1.25 and 1.26,
so the pinned Go package test was not runnable with the installed toolchain.

## Risk

- Correctness: reduced; exact source layout, split, retained-size, and delta
  assertions replace the previous loose native-allocation checks.
- Compatibility: source-absent Rust helpers and tests were removed; native
  consumers use the source-shaped map and byte-view contracts.
- Performance: ordinary insertion has no duplicate source table or per-entry
  sidecar. Exact source allocation reconstruction remains on `RealBytes`,
  which Go also documents as expensive, and on `clear` to retain its size.
  Benchmark compilation retains the source comparison surface.
