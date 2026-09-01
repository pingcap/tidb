# `pkg/lightning/duplicate` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly seven artifacts, all read in full. The local Go package
is byte-identical to the pin.

| Go artifact | Lines | Blob | Rust disposition |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 38 | `6a9ecb00939859ee5b586f949d3e3b4f1efbcfc3` | `tidb-util::lightning_duplicate` plus the canonical `tidb-util::extsort`, `tidb-codec`, and Lightning logger owners |
| `detector.go` | 219 | `76e73283b56634813e3cfb7fc01997c7f1ab038d` | complete detector, key adder, defaults, handler state machine, sorter lifecycle, and worker coordination |
| `detector_test.go` | 184 | `a2264f0fa54e96b404b57db7cb0cbc6d77c7b832` | the 100,000-key/10-adder detector test and constructor-failure test |
| `internal.go` | 56 | `9fc325270780cf3f40eeb2831a41439715c3b73d` | exact internal key formatting, ordering, and canonical byte codec composition |
| `internal_test.go` | 66 | `0d9fbaf3de18dea2d7c1f136dcbaa1b962413a4d` | complete encode/decode/order source table |
| `worker.go` | 201 | `d4c23ba74a93ba1827cc904377e3e12bce91ceda` | complete worker loop, duplicate grouping, dynamic task splitting, cancellation, counts, handlers, and task logging |
| `worker_test.go` | 65 | `987dac5bcfe97d88a0409e64562f9f55794a4107` | exact six-case split-key table |

There is no package doc, fixture, testdata, benchmark, generated source,
platform variant, README, or ownership artifact. Bazel's short/flaky/four-
shard scheduling metadata has no Cargo runtime behavior to port.

## Rust ownership and parity result

`rust/crates/tidb-util/src/lightning_duplicate.rs` owns the complete package.
It composes the completed `pkg/util/extsort` owner rather than embedding a
cache or in-memory workaround, and uses `tidb-codec::encode_bytes` /
`decode_bytes` rather than duplicating TiDB's mem-comparable codec. Internal
keys retain user-key then key-ID ordering, uppercase `KEY@ID` logging, reusable
decode buffers, and exact malformed-key errors. Key adders retain writer flush
and close behavior.

Detection applies the source defaults, logs and performs the external sort,
reads inclusive/exclusive bounds, returns immediately for an empty range, and
starts one bounded task channel. The caller cancellation state is derived into
one errgroup-equivalent state shared by handler constructors and workers; any
worker failure cancels that same state. Each worker constructs one handler,
receives and dynamically splits ranges every 1,000 iterations, polls
cancellation at the source points, never splits a duplicate user-key group,
and shares the atomic distinct-duplicate count. The dynamic task counter and
post-errgroup drain preserve Go's ordering when constructors or workers fail.
The original worker error is retained before group cancellation, handler
`Close` runs after task completion and overrides only success, iterator-close
errors are ignored at the same deferred sites, and task logs retain source
fields/levels.

Handlers observe `Begin`, at least two lexicographically ordered `Append`
calls, `End`, and `Close`. Success, cancellation, begin/append/end/close errors,
constructor errors, split-channel contention, and no-op handling use the same
branches as Go. Exactly `TestDetector`, `TestDetectorFail`, `TestInternalKey`,
and `TestGenSplitKey` remain as snake-case Rust test identities. There is no
supplemental test, benchmark, alternate duplicate policy, duplicate sorter, or
legacy owner.

## WIP validation

Profile: WIP. This completes one atomic package in an ongoing repository-wide
parity audit; it is not a Ready or repository-completion claim.

Inventory checks from the repository root:

```text
git ls-tree -r e2788410d8d696605e8cb002585877a063ccc909 pkg/lightning/duplicate
git diff --exit-code e2788410d8d696605e8cb002585877a063ccc909 -- pkg/lightning/duplicate
rg -n 'failpoint\.|testfailpoint\.|failpoint' pkg/lightning/duplicate
```

The targeted source baseline was attempted from the repository root:

```text
go test -run '^(TestDetector|TestDetectorFail|TestInternalKey|TestGenSplitKey)$' -tags=intest,deadlock ./pkg/lightning/duplicate -count=1
```

The host dependency stack failed before compiling this package: `pkg/util/hack`
could not resolve `checkMapABI`, and cached gRPC `internal/transport` refers to
the unavailable HTTP/2 `TrailerPrefix` symbol. Its complete prerequisite
`pkg/util/extsort` Go baseline passed separately.

Passed from `rust/`:

```text
cargo fmt --all
cargo fmt --all -- --check
cargo check --quiet --offline -p tidb-util
cargo test --quiet --offline -p tidb-util 'lightning_duplicate::tests' --lib -- --test-threads=1
cargo clippy --quiet --offline -p tidb-util --lib --no-deps -- -A clippy::map-or-identity -A clippy::chunks-exact-to-as-chunks -A clippy::wrong-self-convention -A clippy::new-without-default -A clippy::len-without-is-empty -A clippy::should-implement-trait -D warnings
git diff --check
```

Neither package uses failpoints. No Go, Bazel, module, or generated artifact
changed, so `make bazel_prepare` is not required. The full 515-test `tidb-util`
sweep was attempted: 512 passed, two existing tests were ignored, and the
unrelated `cgmon` test failed because this host rejects `sysctl -n hw.memsize`
with `Operation not permitted`; it fails identically in isolation and no cgmon
file changed. Cross-platform execution, the blocked Go package baseline, and
the Ready-profile `make lint` were not verified locally. Cargo emitted only
the existing `tidb-model` `unused_mut` and vendored TiKV-client
`private_bounds` warnings.

## Risk

- Correctness: all seven artifacts and four source tests are mapped; the exact
  100,000-key concurrent Rust test passes, while the Go package baseline is
  blocked before package compilation by host dependency mismatches.
- Compatibility: the native cancellation token retains context identity at
  sorter/worker boundaries; arbitrary handler errors remain in the error
  source chain, and the completed canonical byte codec owns key encoding.
- Performance: adders remain concurrent and buffered, detection uses the
  external sorter and bounded task channel, ranges split only at 1,000-key
  checkpoints, and handler/result processing does not copy sorted values.
