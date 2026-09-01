# `pkg/lightning/manual` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly four artifacts, all read in full. The local Go package
is byte-identical to the pin.

| Go artifact | Lines | Blob | Rust disposition |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 14 | `04d2ca20923caaca3db0bf11ba199a0051386d13` | `tidb-util::lightning_manual`; Cargo owns native build selection and atomic dependency metadata |
| `allocator.go` | 50 | `effef013577d7cd6f563f6c70b231ad1a8ba7ca4` | optional shared atomic live-allocation count and exact leak check |
| `manual.go` | 65 | `476e1f5b0d9f7371b5c768be1ccc6568bdeb716e` | production manual allocation, zeroing, explicit release, and safe cgo slice cap |
| `manual_nocgo.go` | 19 | `bfd30cd3fb6f6a553e4e15aead479b4ac251b518` | non-cgo zeroed allocation/release fallback |

There is no package doc, test, fixture, testdata, benchmark, generated source,
README, or ownership artifact. `manual.go` and `manual_nocgo.go` are mutually
exclusive cgo build variants; both were read and compiled.

## Rust ownership and parity result

`rust/crates/tidb-util/src/lightning_manual.rs` owns the complete package.
`new` returns a target-sized, zero-filled byte block, preserves the cgo
`MaxArrayLen` boundary, and uses the native allocator's fatal out-of-memory
path. A nonempty manual block deliberately does not release its allocation
when its Rust owner is dropped: only `free` releases it, retaining the source's
documented manual-lifetime and leak behavior. The zero-length path allocates no
storage and `free` remains a no-op for it, matching both Go variants. Rust has
no Go tracing heap or cgo boundary; its native allocation is already outside
the memory-management mechanism that motivated the cgo implementation, while
the non-cgo fallback contributes the same zeroed byte contents and release API.

The zero-value `Allocator` has no counter. When configured with a shared
`Arc<AtomicI64>`, allocation increments and free decrements sequentially and
before their delegated operation, just as Go's shared `*atomic.Int64` does.
`check_ref_count` performs the same two loads and returns the exact
`memory leak detected, refCnt: N` text for a nonzero count. Cloning the native
allocator shares counter identity, corresponding to copying Go's struct value.

The existing `tidb-util::membuf::Allocator` boundary now carries a native
`Block` rather than a bare `Vec`. Automatically managed allocators still free
on drop; this package selects explicit-release-only blocks. That integration
is necessary for the Go manual allocator to retain its lifetime behavior when
used by the already-complete `pkg/lightning/membuf` package. No unrelated
membuf behavior, source test, or benchmark changed, and no supplemental manual
test was introduced because the source package has none.

There was no previous Rust owner or duplicate manual allocator.

## WIP validation

Profile: WIP. This completes one atomic package in an ongoing repository-wide
parity audit; it is not a Ready or repository-completion claim.

Inventory checks from the repository root:

```text
git ls-tree -r e2788410d8d696605e8cb002585877a063ccc909 pkg/lightning/manual
git diff --exit-code e2788410d8d696605e8cb002585877a063ccc909 -- pkg/lightning/manual
rg -n 'failpoint\.|testfailpoint\.|failpoint' pkg/lightning/manual
```

Both source build variants passed from the repository root:

```text
CGO_ENABLED=1 go test -tags=intest,deadlock ./pkg/lightning/manual -count=1
CGO_ENABLED=0 go test -tags=intest,deadlock ./pkg/lightning/manual -count=1
```

Passed from `rust/`:

```text
cargo fmt --all
cargo fmt --all -- --check
cargo check --quiet --offline -p tidb-util
cargo test --quiet --offline -p tidb-util membuf --lib -- --test-threads=1
cargo check --quiet --offline -p tidb-util --bench membuf
cargo clippy --quiet --offline -p tidb-util --lib --no-deps -- -A clippy::map-or-identity -A clippy::chunks-exact-to-as-chunks -A clippy::wrong-self-convention -A clippy::new-without-default -A clippy::len-without-is-empty -A clippy::should-implement-trait -D warnings
cargo clippy --quiet --offline -p tidb-util --bench membuf --no-deps -- -A clippy::map-or-identity -A clippy::chunks-exact-to-as-chunks -A clippy::wrong-self-convention -A clippy::new-without-default -A clippy::len-without-is-empty -A clippy::should-implement-trait -D warnings
git diff --check
```

The package has no failpoint use or dependency. No Go, Bazel, module, or
generated artifact changed, so `make bazel_prepare` is not required.
Cross-platform execution, workspace-wide tests, and the Ready-profile
`make lint` were not run in this WIP iteration. Cargo emitted only the existing
`tidb-model` `unused_mut` and vendored TiKV-client `private_bounds` warnings.

## Risk

- Correctness: all four source/build artifacts are mapped; both Go variants
  compile, and the dependent complete membuf source suite remains green.
- Compatibility: native custom membuf allocators now return the byte-like
  `Block` owner so allocator lifetime remains explicit; there were no external
  Rust implementations to migrate.
- Performance: zeroing and allocation sizes retain the source shapes. The
  small owner wrapper adds no copy; manual blocks avoid source-absent automatic
  release and standard blocks keep ordinary Rust drop behavior.
