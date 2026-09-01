# `pkg/util/arena` — complete package transcreation

Go baseline: `origin/master` at
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01). The current source
is byte-for-byte unchanged from the earlier pinned extraction.

## Complete inventory

The package has exactly four artifacts, all read in full. There is no package
doc, README, fixture, generated or platform variant, benchmark, fuzz target,
example, or ownership file.

| Artifact | Lines | Git blob | SHA-256 | Disposition |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 24 | `7ef3e758c10cb451da78e44268143bad9019c33c` | `bd91b01a292d54a39aaea56ab375b2e11922edf8d6ad83b3c1644afc387e6818` | library and flaky test target inventoried |
| `arena.go` | 80 | `07ef575e8748d17219c19d17f07001d8af070af4` | `2e3dc44c7791b08740f594e961fb145852c0927db65c94b4cbf0cfe02683931b` | allocator interface, reusable arena, standard fallback, and reset inventoried |
| `arena_test.go` | 65 | `e81dd55dc29aa3f981f29e8bc13a1e0327b5c139` | `0130d21898acad7bc440ec720141664009828ec69bce7ff253dfccd74ec7b6dd` | fitting/fallback/reset and standard allocator assertions inventoried |
| `main_test.go` | 33 | `62e360f9c65def8e28bc05b1d94b601c2e7bc54f` | `b1536bcb5b0cd32422960eb01e1025e6b69a4559bb65392c9976564bc7c40c8c` | common setup and goleak harness inventoried |

Total: 202 textual lines. The two source tests are the complete named test
matrix; `main_test.go` contributes only `TestMain` setup.

Production behavior is an allocator interface, a simple allocator backed by
one reusable byte array, and a standard allocator that always allocates fresh
storage. A fitting simple allocation returns a slice descriptor over the
shared arena; `Reset` rewinds only its offset and does not clear bytes.

## Rust ownership and audit result

`rust/crates/tidb-util/src/arena.rs` owns the complete package. `ArenaBytes`
is a safe Go-slice descriptor over reference-counted `Cell<u8>` backing: cloned
descriptors share writes, reslicing exposes retained bytes, fitting allocations
reuse the simple arena, and fresh fallback and standard allocations remain
independent and zeroed. The
workspace's unsafe-code prohibition remains intact.

The audit replaced the previous owned-`Vec<u8>` approximation, which advanced
the offset but allocated every returned buffer separately. It removed
supplemental tests, a Rust-only `must_use` diagnostic, and the retired semantic
manifest. Exactly the two Go test identities remain.

## Validation

Profile: WIP; this completes one package in the continuing package-by-package
audit, not a repository-wide readiness claim.

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/arena -count=1` — passed.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --offline --locked -p tidb-util arena::tests --lib -- --test-threads=1` — passed; two tests ran.
- Complete `cargo test -q -p tidb-util --locked -- --test-threads=1`,
  `cargo check -p tidb-util --all-targets --locked`, `cargo fmt --all --check`,
  and `git diff --check` — passed; 575 library tests passed, 3 were ignored,
  and every integration and documentation test passed.
- `cargo clippy -p tidb-util --lib --no-deps --locked -- -A
  clippy::chunks-exact-to-as-chunks -A clippy::new-without-default -D warnings`
  — passed. The two allowances cover six pre-existing findings outside
  `arena.rs`; unscoped all-target Clippy also reaches pre-existing
  `tidb-mysql` findings and is not green on the current branch.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: shared storage and reset reuse now match Go; valid lengths,
  capacities, offset movement, fallback, and standard allocation remain.
- Compatibility: the unused public Rust return type changes from `Vec<u8>` to
  `ArenaBytes`; repository search found no production consumer.
- Performance: fitting allocations now reuse one backing allocation instead
  of allocating a new vector, restoring the package's purpose.
