# `pkg/util/bitmap` — complete package transcreation

Go baseline: `origin/master` at
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01). The current Go
source is byte-for-byte unchanged from the earlier implementation; this
refresh uses the current Go-master authority.

## Complete inventory

The package has exactly four artifacts, all read in full. There is no package
doc, README, fixture, generated or platform variant, benchmark, fuzz target,
example, or ownership file.

| Artifact | Lines | Git blob | SHA-256 | Disposition |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 24 | `39f8942e7996e0e4415085724b124996fcf4074c` | `06642625735f436bd6adf4fb15882b89de8ccd18705cd0610f3225669ae5d066` | library/flaky test target and concurrency dependencies inventoried |
| `concurrent.go` | 132 | `a8440123a5b3dfd3ca7d8d928cbad16349ca0cb8` | `e9286132ae15befc716ebf0b36f8bc72b6bfe6e1b155449c3a4d8bbe87209e14` | fixed-length atomic 32-bit bitmap, clone/reset, memory accounting, and bounds inventoried |
| `concurrent_test.go` | 93 | `5a6b57e144adacdf4400008ed9f6be81303478dc` | `e520ed6e3c34f9a89f999084f858d1f3e710295537ea6a4554f4c9fef473dcff` | three source concurrency/reset tests inventoried |
| `main_test.go` | 33 | `c9049d47814530508dae36a5b293eab8e5d0a294` | `d059623a2b59a21c0c85546ea79ead35d56daa7650bd661525e2d9dd49705ef7` | common setup and goleak harness inventoried |

Total: 282 textual lines. The three named source test identities are the
complete test matrix; `main_test.go` contributes only `TestMain` setup.

Production behavior is a fixed-length, 32-bit-segment bitmap whose concurrent
`Set` uses atomic load/CAS and reports one winner per zero-to-one transition.
Clone, Reset, single-owner access, most-significant-bit-first numbering, and
capacity-based memory accounting complete the package.

## Rust ownership and audit result

`rust/crates/tidb-util/src/bitmap.rs` owns the complete package. Go `int`
lengths and indexes are represented as `isize`; segment rounding uses the same
wrapping signed addition and arithmetic shift. This restores the pinned Go
outcomes for negative and maximum lengths, including Reset's malformed
`MaxInt` state, instead of applying a Rust-only validity policy.

The audit removed deterministic oversized-length rejection, Rust-only
`must_use` diagnostics, supplemental tests, and the retired semantic manifest.
Exactly the three Go test identities remain.

## Validation

Profile: Ready for this docs-only authority refresh; the package owner and
focused signed-boundary regressions were implemented in the earlier atomic
batch.

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/bitmap -count=1` — passed.
- The same package test passed in an exact detached checkout of Go master at
  `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --offline --locked -p tidb-util bitmap::tests --lib -- --test-threads=1` — passed; three tests ran.
- `cargo check -p tidb-util --all-targets --locked`, `cargo fmt --all --check`,
  and `git diff --check` — passed.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: ordinary bitmap behavior is unchanged; signed invalid-input
  behavior now matches Go rather than a safer Rust policy.
- Compatibility: unused public length/index parameters change from `usize` or
  `i64` to Go-width `isize`; repository search found no production consumer.
- Performance: unchanged for valid production inputs.
