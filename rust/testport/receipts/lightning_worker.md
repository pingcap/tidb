# `pkg/lightning/worker` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly three artifacts, all read in full. The local Go package
is byte-identical to the pin.

| Go artifact | Lines | Blob | Rust disposition |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 20 | `73487c581757fee5b613e1036383ab4e262d1e6a` | `tidb-util::lightning_worker`; Cargo owns native dependency and test metadata |
| `worker.go` | 81 | `d2405f85a52a7d8bf9a91728b40accc90824daf1` | complete fixed worker pool, FIFO blocking operations, metrics, and availability query |
| `worker_test.go` | 45 | `ea0fc0a1497b8a2b67a89166e0d2c9d5e95b180e` | exactly the sole functional source test |

There is no package doc, fixture, testdata, benchmark, generated source,
platform variant, README, or ownership artifact.

## Rust ownership and parity result

`rust/crates/tidb-util/src/lightning_worker.rs` owns the complete package and
depends on the complete `pkg/lightning/metric` transcreation. Construction
creates workers numbered from one through the target-sized limit, propagates
the optional shared metric set, and initializes the idle gauge. `apply` and
`recycle` preserve FIFO order, blocking send/receive behavior, metric timing,
idle-count updates, shared worker identity, the nil-worker panic, and
`has_worker`'s buffered-length semantics.

The native mutex/condition-variable channel representation includes a direct
handoff for a zero-capacity pool. This is required by Go's unbuffered channel:
a waiting `apply` and `recycle` rendezvous, the send completes only after the
receive consumes the worker, and `has_worker` remains false because an
unbuffered channel's length is always zero. No Rust-only timeout, fairness,
growth, fallback, or supplemental test was added.

Exactly `TestApplyRecycle` remains as the snake-case Rust test identity. There
is no prior Rust owner or duplicate worker pool for this package.

## WIP validation

Profile: WIP. This completes one atomic package in an ongoing repository-wide
parity audit; it is not a Ready or repository-completion claim.

Inventory checks from the repository root:

```text
git ls-tree -r e2788410d8d696605e8cb002585877a063ccc909 pkg/lightning/worker
git diff --exit-code e2788410d8d696605e8cb002585877a063ccc909 -- pkg/lightning/worker
rg -n 'failpoint\.|testfailpoint\.|failpoint' pkg/lightning/worker
```

Passed from the repository root:

```text
go test -run '^TestApplyRecycle$' -tags=intest,deadlock ./pkg/lightning/worker -count=1
```

Passed from `rust/`:

```text
cargo fmt --all
cargo fmt --all -- --check
cargo test --quiet --offline -p tidb-util lightning_worker --lib -- --test-threads=1
cargo check --quiet --offline -p tidb-util
cargo clippy --quiet --offline -p tidb-util --lib --no-deps -- -A clippy::map-or-identity -A clippy::chunks-exact-to-as-chunks -A clippy::wrong-self-convention -A clippy::new-without-default -A clippy::len-without-is-empty -A clippy::should-implement-trait -D warnings
```

The package has no failpoint use or dependency. No Go, Bazel, module, or
generated artifact changed, so `make bazel_prepare` is not required.
Cross-platform execution, workspace-wide tests, and the Ready-profile
`make lint` were not run in this WIP iteration. Cargo emitted only the existing
`tidb-model` `unused_mut` and vendored TiKV-client `private_bounds` warnings.

## Risk

- Correctness: all three artifacts and production branches are mapped; the
  sole source test identity passes in both Go and Rust.
- Compatibility: worker identity is shared through `Arc`, while channel
  blocking and buffered-length behavior match Go for all nonnegative limits.
- Performance: the fixed FIFO and metric updates retain the source behavior;
  no Rust-specific crossover or scheduling policy exists.
