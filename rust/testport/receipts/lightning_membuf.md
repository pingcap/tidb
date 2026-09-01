# `pkg/lightning/membuf` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly five artifacts, all read in full. The local Go package
is byte-identical to the pin.

| Go artifact | Lines | Blob | Rust disposition |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 31 | `8255a30c358b8cc28774d9dc368d598ead6e0cc3` | `tidb-util::membuf`; Cargo owns native build, test, and benchmark metadata |
| `buffer.go` | 411 | `a64470cad647aec3d25899bd6205b08572e54c1c` | pool options, allocator, block cache, buffer, slice locations, allocation, reset/destroy, and accounting |
| `buffer_test.go` | 451 | `64eae2ce4efbc34fb4d605c7bd5830756a030cec` | five functional tests and all nine benchmarks |
| `limiter.go` | 99 | `f1ad44b96a51dbb49496532044b298da7568d446` | FIFO blocking/nonblocking quota limiter and overflow logging |
| `limiter_test.go` | 72 | `00f9f38e25af9c1076b0df27803b928e9b4e4a10` | two concurrent limiter tests |

There is no package doc, fixture, testdata, generated source, platform variant,
README, or ownership artifact. The Go test is short, flaky, and split into
seven Bazel shards; those scheduler attributes have no Cargo behavior to port.

## Rust ownership and parity result

`rust/crates/tidb-util/src/membuf.rs` owns the complete production package.
Go `int` state and arithmetic use target-sized `isize`, with explicit wrapping
where debug Rust would otherwise introduce a source-absent overflow panic.
Unsigned alignment retains the source formula, including division-by-zero and
overflow boundaries. Options are applied in source order, so invalid earlier
options panic even if a later option would replace them.

The pool retains Go's bounded nonblocking channel behavior: acquire reuses or
allocates a full block, release caches or frees it, memory quota is charged at
the same points, closing drains cached blocks, a second close panics, receiving
after close yields the native empty block, and returning a block after close
panics. Nil allocator and limiter options retain their source failure/no-op
boundaries. The limiter preserves FIFO admission, prevents `TryAcquire` from
jumping queued waiters, wakes every fitting prefix, and logs overflow with a
stack trace.

The allocator boundary now returns a native `Block` byte owner instead of a
bare automatically dropped vector. Standard and custom blocks retain normal
release-on-drop behavior, while the complete `pkg/lightning/manual`
transcreation selects explicit-release-only storage. Pooling, aliasing,
allocation contents, cache order, allocator calls, and every public membuf
result are unchanged; the block owner exists solely to preserve the allocator
lifetime policy carried by Go's `[]byte`.

Go's returned `[]byte` is represented by `Bytes`, a cloneable native slice
header over shared block storage. Its read/write guards preserve aliasing,
full-slice capacity isolation, and multiple immutable readers without the old
Rust location-token substitution. Standalone over-block allocations still
bypass the pool and limiter. `None` is used only for Go nil slices. Reset,
destroy, reusable blocks, per-buffer block limits, slice-location lookup,
small-object overhead batches, and unchanged-on-failed-try allocation follow
the source branches. Rust ownership enforces the source-documented requirement
that byte aliases are released before `Destroy` can return their block.

The previous public `PoolConfig`, default constructor shortcut, allocator
implementation, pool internals, decoded allocation enum, cached-count and
block-size accessors, location-only allocation pipeline, debug formatting,
diagnostic attributes, and four supplemental tests were removed. Exactly the
seven Go functional test identities remain. `rust/crates/tidb-util/benches/
membuf.rs` carries all nine source benchmark identities and exact workload
sizes. Its deterministic native RNG supplies the same uniform ten-byte input
contract; Rust has no tracing garbage collector, so the two GC variants retain
their separate identities with a native no-op at the source `runtime.GC`
point. Byte aliases are cleared before destroy to satisfy Rust ownership and
the source's documented release-before-destroy precondition.

There are no production consumers outside the owner yet; the benchmark is the
only external Rust consumer. No duplicate membuf implementation remains.

## WIP validation

Profile: WIP. This completes one atomic package in an ongoing repository-wide
parity audit; it is not a Ready or repository-completion claim.

Passed from the repository root:

```text
git ls-tree -r e2788410d8d696605e8cb002585877a063ccc909 pkg/lightning/membuf
git diff --exit-code e2788410d8d696605e8cb002585877a063ccc909 -- pkg/lightning/membuf
go test -run '^(TestBufferPool|TestPoolMemLimit|TestBufferIsolation|TestBufferMemLimit|TestGetAlignedSizeGetBlockCnt|TestLimiter|TestWaitUpMultipleCaller)$' -tags=intest,deadlock ./pkg/lightning/membuf -count=1
```

Passed from `rust/`:

```text
cargo fmt --all
cargo fmt --all -- --check
cargo test --quiet --offline -p tidb-util membuf --lib -- --test-threads=1
cargo check --quiet --offline -p tidb-util --bench membuf
cargo clippy --quiet --offline -p tidb-util --lib --no-deps -- -A clippy::map-or-identity -A clippy::chunks-exact-to-as-chunks -A clippy::wrong-self-convention -A clippy::new-without-default -A clippy::len-without-is-empty -A clippy::should-implement-trait -D warnings
cargo clippy --quiet --offline -p tidb-util --bench membuf --no-deps -- -A clippy::map-or-identity -A clippy::chunks-exact-to-as-chunks -A clippy::wrong-self-convention -A clippy::new-without-default -A clippy::len-without-is-empty -A clippy::should-implement-trait -D warnings
```

The source package has no `failpoint.`, `testfailpoint.`, or Bazel failpoint
dependency match. No Go, Bazel, module, or generated artifact changed, so
`make bazel_prepare` is not required. The source-sized benchmarks were compiled
but not executed because they intentionally allocate multi-gigabyte working
sets. Cross-platform execution, workspace-wide tests, and the Ready-profile
`make lint` were not run in this WIP iteration. Cargo emitted only the existing
vendored TiKV-client `private_bounds` warning.

## Risk

- Correctness: all five artifacts, seven tests, and nine benchmark identities
  are mapped; both pinned Go and Rust functional suites pass.
- Compatibility: the old location-only Rust return API is intentionally
  removed in favor of the source byte-slice behavior. Custom native allocator
  implementations wrap their returned storage in `Block`; there were no
  production consumers to migrate.
- Performance: allocation and accounting shapes match the source. Native
  `RwLock` guards provide safe shared slice access, and the multi-gigabyte
  benchmark workloads were compile-checked rather than run locally.
