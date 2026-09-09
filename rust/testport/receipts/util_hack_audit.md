# `pkg/util/hack` — Go-master parity audit

Comparison source: Go `origin/master` at
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01). The Rust owner is
the `tidb-hack` crate on `origin/hparser-integration`. The Go baseline contains
both Go 1.25 and Go 1.26 source variants; its Go 1.25 ABI guard admits only
the matching toolchain. The current worktree's guard additionally admits Go
1.26, but no Rust behavior depends on that local-only source delta.

## Complete inventory

All nine Go artifacts were read in full before comparison:

| Artifact | Lines | Git blob | SHA-256 | Disposition |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 31 | `339fcc2c23972b7f1ec1a5a6fd0797f9dba3c441` | `90200e3e82b7ec43966655641b3bb3b05e6dc63a65bb21776ccf7a1feedd05f7` | library/test targets and source rows |
| `hack.go` | 57 | `a8fd421029d7fef2f0c7bc636227be9d2c63f30f` | `5d1e65bf630622eb18bce38fe4e497b86272e5501153f10a686c5e6169040828` | zero-copy views, pointer window, constants, ABI guard |
| `hack_test.go` | 65 | `bc6e2dffc7d511601dfc350a4d6015bd87511962` | `4732775b346bfa716838f71b17d13cea74bee17f58c57d534b9ae07515630b04` | three mutable/zero-copy tests |
| `main_test.go` | 33 | `34dd6ee7f2e6fcf20a44b5a958833096fac1809e` | `c63c48360598e3f0ac7a8e44bff34a0eee5ab25f2c0999fb4ae4b9a6020b0502` | common setup and leak exclusions |
| `map_abi.go` | 430 | `e25620a15c08ee827a5e0b171054a4b4748a3467` | `1cfc63d794261442834c92dd5257dbd9f6fe9ed7ed17e07f8595de7d86b71b2d` | Go 1.25 Swiss-map ABI and map accounting |
| `map_abi_go126.go` | 430 | `c091a9d356ea3056df859d64b8bb074aa2972665` | `54bacfb89ebcaab7f6a04f3af49fecdb8832d05427476517d6d54e1a7ededa97` | Go 1.26 ABI variant |
| `map_abi_test.go` | 237 | `ce56c89c7be71a8db58ff4b4f8cd73eeb1ad21d0` | `3f3ae90243f332fbd809939fa83b4ba9912f0fe1e5f0b8ddbee121441a775f60` | geometry, memory, and benchmark helpers |
| `map_abi_test_type_go125_test.go` | 21 | `ef58a5f58d970709d94afd43ed07353aab94e03c` | `45151da7d2cc51e7cecf2281e1236f32a1d771502293be8d44c93fccedd76324` | Go 1.25 aliases |
| `map_abi_test_type_go126_test.go` | 21 | `a54a15d76e961537bba70cd56e8afc09a8425915` | `6d0dea8f5b537cef529b6e8008e4252b88bab3dd83f567fa7ef551888e1e4d2b` | Go 1.26 aliases |

The package has 1,325 Go lines, 13 production declarations (including the
`MemAwareMap` methods and ABI helpers), three source tests, one `TestMain`,
and two benchmark functions. There is no `doc.go`, fixture/testdata tree,
generated artifact, example, fuzz target, or nested package.

## Go behavior and Rust owner

`String` creates a mutable-string view over a byte slice without copying;
`Slice` creates the inverse unsafe byte view; `GetBytesFromPtr` exposes an
explicit pointer window. The Swiss-map files mirror the private Go runtime
layout for table geometry, exact `RealBytes`, capacity, slot pointers, and
the process ABI guard. `MemAwareMap` retains the source's nil-map panic,
insert/replace distinction, lookup helpers, deterministic test seed, and
checkpointed approximate-byte deltas (eight-slot initial checkpoint followed
by `min(used, 1024) + used`). `clear` retains allocation while changing the
runtime seed/clear sequence. The two build-tagged files differ only where Go
1.26 renamed the internal map types and ABI fields.

`rust/crates/tidb-hack/src/lib.rs` and `src/map.rs` are the dependency-closed
owner. `MutableBytes`/`MutableString` provide a safe, owned carrier for the
source's intentionally mutable aliases; `SwissMapWrap` and `MemAwareMap`
retain the public geometry, memory checkpoints, deterministic seed, and
lookup/set/clear behavior. Rust cannot inspect Go's private runtime map ABI,
so `real_bytes` measures its owned hashbrown table; this is an ownership
adaptation, not a second Go policy. The `tidb-hack` benchmark maps the source
map benchmark. No Rust-only scheduler, allocator, or map-order policy is
needed by the package.

## Validation and disposition

Profile: **WIP**. This is a current-baseline audit with no production fix;
therefore no package-complete Ready claim or `make bazel_prepare` is needed.

Passed:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/util/hack -count=1
# ok

OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
cargo +nightly-2026-08-22 test --offline --locked -p tidb-hack --lib -- --test-threads=1
# 6 passed

git diff --check -- pkg/util/hack rust/crates/tidb-hack
# passed; no package source diff was introduced
```

## Risks and unverified behavior

- Correctness: all Go 1.25 source tests and all six Rust owner tests pass on
  the host toolchain.
- Compatibility: exact `RealBytes` values cannot be identical across Go's
  private Swiss-map ABI and Rust's hashbrown allocation; only the source's
  checkpointed accounting contract is portable. The 1.26 ABI variant is
  inventoried but not executable under the installed Go 1.25 toolchain.
- Performance: no runtime path changed. The owner keeps source memory
  checkpoints and avoids exposing raw Rust table pointers.
- Not verified locally: Go 1.26 and non-host architectures, race-enabled
  Bazel execution, and consumers that use the memory constants (the full
  memory-arbitrator integration remains a separate package boundary).
