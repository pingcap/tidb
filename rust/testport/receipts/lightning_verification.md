# `pkg/lightning/verification` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly three artifacts, all read in full. The local Go package
is byte-identical to the pin.

| Go artifact | Lines | Blob | Rust disposition |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 25 | `df7e9358335641f0e97380a19cbbc3d60e1c0fe0` | `tidb-util::lightning_verification`; Cargo owns native dependency and test metadata |
| `checksum.go` | 278 | `27d518a9e46bcab27957fc4b1f0dd1eaccd6ccab` | CRC-64/ECMA checksum, group checksum, JSON/string/log values, merge/subtract, raw groups, and totals |
| `checksum_test.go` | 134 | `92382432134963334180210ff344a6799330d919` | exactly four functional source tests |

There is no package doc, fixture, testdata, benchmark, generated source,
platform variant, README, or ownership artifact. The Go test is short, flaky,
and split into four Bazel shards; those scheduler attributes have no Cargo
behavior to port.

## Rust ownership and parity result

`rust/crates/tidb-util/src/lightning_verification.rs` owns the complete package.
Its private CRC-64/ECMA update matches Go's `hash/crc64` continuation behavior.
Checksum byte/KV totals and target-sized intermediate counters wrap at their
source widths. The checksum remains a zeroable, comparable, value-copy type;
JSON field order, `String`, nested log fields, XOR combination, subtraction,
keyspace prefix accounting, raw groups, cloned inner checksums, and merged
totals follow the source.

Group storage now uses an unordered native map instead of the previous
deterministically ordered `BTreeMap`, preserving Go's unspecified map
iteration order for log objects. The group borrows its constructor keyspace
slice for its lifetime rather than cloning it, retaining the source storage
relationship for groups created later. The required `pkg/lightning/common`
dependency is represented by the exact three-field `KvPair` carrier, including
the verification-ignored `RowID`; no narrowed two-field constructor or
comparison/default traits remain. This is an integration representation only,
not a partial or complete claim for the separate 24-artifact common package.

The audit removed the long narrowing document, exported CRC helper,
source-absent pair constructor, deterministic group log order, `must_use` and
`const fn` capabilities, group debug/default/comparison traits, and three
supplemental tests. Exactly `TestChecksum`, `TestChecksumJSON`,
`TestGroupChecksum`, and `TestKVChecksumOperation` remain. The nil slice in the
first source test maps to a second empty Rust slice call because Rust slices do
not encode nil separately and verification observes only their contents.

There are no production Rust consumers outside the owner and no duplicate
verification checksum implementation.

## WIP validation

Profile: WIP. This completes one atomic package in an ongoing repository-wide
parity audit; it is not a Ready or repository-completion claim.

Inventory checks from the repository root:

```text
git ls-tree -r e2788410d8d696605e8cb002585877a063ccc909 pkg/lightning/verification
git diff --exit-code e2788410d8d696605e8cb002585877a063ccc909 -- pkg/lightning/verification
```

Passed from `rust/`:

```text
cargo fmt --all
cargo fmt --all -- --check
cargo test --quiet --offline -p tidb-util lightning_verification --lib -- --test-threads=1
cargo check --quiet --offline -p tidb-util
cargo clippy --quiet --offline -p tidb-util --lib --no-deps -- -A clippy::map-or-identity -A clippy::chunks-exact-to-as-chunks -A clippy::wrong-self-convention -A clippy::new-without-default -A clippy::len-without-is-empty -A clippy::should-implement-trait -D warnings
git diff --check
```

The source package has no `failpoint.`, `testfailpoint.`, or Bazel failpoint
dependency match. Its targeted Go baseline command was attempted without
failpoint enablement:

```text
go test -run '^(TestChecksum|TestChecksumJSON|TestGroupChecksum|TestKVChecksumOperation)$' -tags=intest,deadlock ./pkg/lightning/verification -count=1
```

The host Go 1.27 dependency stack failed before this package: `pkg/util/hack`
has no selected `checkMapABI` implementation and cached gRPC transport refers
to the unavailable HTTP/2 `TrailerPrefix`. No Go, Bazel, module, or generated
artifact changed, so `make bazel_prepare` is not required. Cross-platform
execution, workspace-wide tests, and the Ready-profile `make lint` were not
run in this WIP iteration. Cargo emitted only the existing vendored
TiKV-client `private_bounds` warning.

## Risk

- Correctness: all three artifacts and production branches are mapped; exactly
  the four source test identities pass.
- Compatibility: the narrowed two-field pair type gained Go's required
  `RowID`, and deterministic group log order was intentionally removed.
- Performance: CRC and aggregation loops retain the source shapes; native hash
  map iteration and borrowed keyspace storage avoid the former sorting/copying.
