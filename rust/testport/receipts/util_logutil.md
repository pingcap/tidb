# `pkg/util/logutil` — Go-master package parity receipt

Go source: `origin/master` at
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01). The top-level
package is pinned by the existing logutil ExecPlan to
`3606de5c43fcf4fa5206596c41cd0793403b9818`; the package files are unchanged
from that pin.

## Complete inventory

All eight top-level artifacts were read in full before ownership review. The
nested `pkg/util/logutil/consistency` directory is a separate package and is
not included here.

| Artifact | Lines | Git blob | SHA-256 | Inventory |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 51 | `85c6357068424a400f10a68d7d5adf3f2e80d08c` | `fb948a6b50ce0aefe32c49251100a4a892560939f6cc39e4d0e8bbe07af6ee7f` | public library plus flaky test target, complete dependency graph |
| `general_logger.go` | 44 | `0e295c9c54d1c7fbcbfb74dbac0503a25844047e` | `1e9454abe7c3bf159d6986321ba675f2da33337d37e74cc2ce1f3f3592246869` | general logger constructor and config derivation |
| `hex.go` | 79 | `8777a15991a8ca057a28ee966b27eec6bcc9235e` | `4fe1364e7c05a2b76113a6bcdb5fde85261b28115bab59a96c2a073bab40207b` | protobuf `Hex` wrapper and reflection pretty-printer |
| `hex_test.go` | 59 | `b4c82d51340a34f9c02bfc74ef000f74a9a0db11` | `36fbbee52cfca1ea6da238a2d637e50818f46ababe900a7046cd2bdfbb7a3ee8` | byte-slice, key-range, and metapb.Region formatting goldens |
| `log.go` | 484 | `ffdba802c55f6751eb720c5eb8713f997da75b03` | `6feb3d0d3341d2dd777a72f8695fcf4339e7a8532064f4cf8f474c74c0b71961` | config/global logger lifecycle, context fields, trace hooks, proxy fields, and sampled factories |
| `log_test.go` | 388 | `0e6d3a4672981ed4567f309bf0b5a0c58846d9c5` | `4f307646d4647f8675ca6e893cc1e3b61c4880d9eff0a6da9e59bd3a063103bc` | 11 logger/config/proxy/sampling tests plus helpers |
| `main_test.go` | 52 | `e84a04de092d780c8e3de42d37a1fa7e579c9329` | `36b3be49dd68cf21bdc25b0802f586e1f0953de63151485545439379f35652f0` | common test setup and goleak `TestMain` |
| `slow_query_logger.go` | 103 | `d1cb3ff170ece866176be6580da2198e9e10c36f` | `2d6979d233745b3d5597571c93d2988cde8ef30dc823be21f7e886cf3af573d0` | slow logger/config constructors and no-field slow-log encoder |

The boundary has 1,260 Go lines, 30 production methods/functions (including
encoder methods), 12 top-level tests, one `TestMain`, and no fixture or
testdata tree, generated output, platform/build-tag variant, benchmark, fuzz
target, example, or failpoint hook. The build target has no failpoint
dependency.

## Rust ownership and parity decision

`rust/crates/tidb-util/src/logutil/{mod.rs,file_sink.rs,hex.rs}` is the
dependency-closed owner for the logger lifecycle, contextual fields, slow and
general encodings, file rotation/compression, sampled factories, proxy
variables, and explicit pretty-value formatting. Its `tidb-log` dependency
owns the unified encoder/config contract. Existing Rust tests cover every Go
test scenario plus deterministic regressions for slow-field composition,
RFC3339Nano formatting, shared sink/level identity, sampler level/hash/window
semantics, and replacement logger construction.

The Go-only gRPC logger replacement, opentracing `Event`/`Eventf`/`SetTag`,
and runtime/trace tee remain explicit integration omissions: Rust has no
corresponding ecosystem runtime or package assertion, so inventing detached
APIs would be Rust-only behavior. No further source fix was identified in
this re-audit; no Go or Rust source/build artifact changed in this receipt.

## Validation

Profile: WIP for the continuing repository audit; this receipt adds evidence
only and does not claim a new code fix.

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/logutil -count=1` — passed.
- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-util logutil --lib -- --test-threads=1` — passed (19 tests).
- `git diff --stat e2788410d8d696605e8cb002585877a063ccc909..origin/master -- pkg/util/logutil` — empty.
- `rg` inventory checks found no build tags, fixture/testdata files, generated inputs, or failpoint use in the boundary.

No Go or Bazel file changed, so `make bazel_prepare` is not required. The
full `tidb-util` crate, repository lint, and dependent server/session logging
consumers are not rerun for this evidence-only boundary while the repository
audit continues.

## Risks and unverified scope

- Correctness: the logger owner must preserve unified text bytes, slow-log
  headers, shared-vs-dedicated sink identity, sampler admission, and field
  ordering when consumers expand.
- Compatibility: the three omitted Go ecosystem integrations remain available
  only to Go callers; no Rust replacement API is promised.
- Performance: no runtime code changed; rotation, sampling, and sink locking
  retain the existing Rust implementation.
- Not verified locally: gRPC/opentracing/runtime-trace integration and every
  downstream server/session logger call path.
