# `pkg/util/checksum` — complete Go-master parity receipt

Comparison source: Go `origin/master` at
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01). The package is
unchanged from the earlier pinned implementation; this receipt records the
rolling authority, complete artifact hashes, and the Rust-only API fix.

## Complete inventory

The package has exactly four Go-master artifacts and 786 lines, all read in
full: `BUILD.bazel`, `checksum.go`, `checksum_test.go`, and `main_test.go`.
There is no package `doc.go`, generated input/output, platform/build-tag
source, fixture/testdata directory, benchmark, fuzz target, example, README,
ownership file, or nested package.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 26 | `ce53cd1e7cb08e64284452c87825e96b02d1e229` | `8ff1e1dc4981e55b1daf7cea5ca0d8c8630f4b1dae6a6d77679653106a8d4784` | library and flaky short test target |
| `checksum.go` | 183 | `c29bdea27294f397690e8b3662a2804d61c61bb9` | `1ecf039cb0f071ac3a80505a825c52b90a6f79f1545600dfb0455915d32cf43c` | CRC framing writer and reader |
| `checksum_test.go` | 544 | `bfc3325a1f8ef0fd97e42dfe66ae0362840a6526` | `9d51e87d4559ed794e6e79403c879c21e3910823c5629e3208fd05fcefbcc368` | ten source tests and helpers |
| `main_test.go` | 33 | `f91dfc6d2981c395a9f7b0c7ec0b869eabb6cb51` | `4b0c73d0c67da69318d6930a484564aadc33fe97dede15390884f68bf6c8febd` | common setup and goleak harness |

The production surface is `Writer`/`NewWriter` with `AvailableSize`,
`Write`, `Buffered`, `Flush`, `GetCache`, `GetCacheDataOffset`, and `Close`,
plus `Reader`/`NewReader` and positional `ReadAt`. CRC-32 blocks use a
four-byte little-endian checksum and 1,020-byte payload. The writer preserves
sticky errors, short-write detection, cache offsets, auto-flush, and close
ordering; the reader validates complete and partial blocks, pools its read
buffer, preserves positional counts, and returns checksum/EOF errors exactly.
The ten source tests cover nested framing, encrypted and unencrypted
insert/delete/mutate corruption, empty files, block-size reads/writes, and
writer buffering. `main_test.go` contributes only common setup and goleak.

## Rust ownership and integration

`rust/crates/tidb-util/src/checksum/mod.rs` owns the complete package over the
shared `layered_io::{CloseWrite, ReadAt}` contracts. The live encrypted spill
consumer is `rust/crates/tidb-chunk/src/chunk_util.rs`; the encryption layer
and `benches/encrypt.rs` are additional consumers/validation artifacts. The
checksum owner preserves the source block geometry, CRC framing, pooled read
buffer, sticky errors, positional reads, encrypted cache overlays, and close
cascade.

The audit removed the Rust-only public `Writer::underlying` accessor and moved
the chunk spill stack to explicit checksum/cipher writer ownership. It also
removed six Rust-only `#[must_use]` diagnostics from constructors and cache
accessors. `TestReturnValuesMayBeIgnoredLikeGo` applies
`#[deny(unused_must_use)]` and discards all six values; before the fix it
failed with six lint errors and now passes. The ten source test identities
remain intact.

## Validation

Profile: **Ready** for this focused parity fix. Rust source and its focused
test changed, so owner/consumer tests, package checks, formatting, diff
quality, and the pinned detached Go lint gate were run. No Go source, imports,
Bazel metadata, or module file changed; `make bazel_prepare` is not required.

```text
git diff --exit-code 5e8a1a229a7591ddac49a0cd3b795587c2595ab9..origin/master \
  -- pkg/util/checksum
# passed: current package is unchanged from the previous authority pin

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/util/checksum -count=1
# passed (current worktree and exact detached Go-master worktree; ten tests)

# Before the fix, the focused regression failed with six unused_must_use
# errors; after removing the Rust-only annotations it passes.
OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-util \
  --lib checksum::tests --offline --locked -- --test-threads=1
# passed: ten source tests and TestReturnValuesMayBeIgnoredLikeGo (11 tests)

OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-chunk \
  --lib chunk_util --offline --locked -- --test-threads=1
# passed: encrypted/plain spill checksum consumer tests

OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml \
  -p tidb-util -p tidb-chunk --all-targets --offline --locked
# passed: owner, encryption benchmark, and spill consumer targets

cd rust && cargo +nightly-2026-08-22 fmt --all -- --check
# passed
git diff --check
# passed

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint
# passed in a clean detached worktree at this batch's commit
```

No Go or Bazel file changed, so `make bazel_prepare` is not required. The
local host does not provide a separate platform execution target; Unix and
Windows behavior are represented by the Rust owner and dependency boundary.
Full workspace tests and Bazel execution remain outside this leaf receipt.

## Risk

- Correctness: all ten source tests, the six-value diagnostic regression, and
  encrypted spill consumers cover framing, corruption, errors, and closure.
- Compatibility: public Rust APIs now match Go's ignored-return diagnostics;
  removing `underlying` affects only the Rust-only accessor and its one
  migrated consumer.
- Performance: checksum block size, CRC algorithm, pooled read buffer, and
  spill-layer ownership remain unchanged in the data path.
