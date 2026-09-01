# `pkg/config/configtypes` — Go-master parity audit receipt

Status: complete dependency-closed audit; no source behavior delta or
Rust-only execution policy was found.

Comparison source: Go `origin/master` at
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01). The package has
exactly three tracked artifacts and 204 lines:

| Artifact | Lines | Role |
| --- | ---: | --- |
| `BUILD.bazel` | 25 | library/test targets and dependencies |
| `types.go` | 97 | TOML/JSON `ByteSize` and `Duration` wrappers |
| `types_test.go` | 82 | source JSON/TOML round-trip tests |

There is no package `doc.go`, generated source, platform-specific variant,
fixture/testdata directory, benchmark, or nested package. The production file
contains 10 marshal/unmarshal method declarations; the test file contains two
tests and two config helper types. Every Go production, test, and Bazel
artifact was read in full before comparing the Rust owner.

## Rust owner comparison

`rust/crates/tidb-config/src/configtypes.rs` is the dependency-closed owner
and is registered by `src/lib.rs`. `ByteSize` preserves docker/go-units'
binary suffix parsing and four-significant-digit rendering, including the
legacy `KB`/`KiB` aliases and integer conversion. `Duration` preserves
`time.ParseDuration`'s signed multi-unit grammar, microsecond spellings,
overflow handling, and `Duration.String()` rendering. Serde implementations
are the native TOML/JSON carriers for Go's encoding interfaces and are used by
the ordinary config loader.

The owner includes the two source round-trip tests plus focused Go-derived
size and duration matrices covering accepted aliases, fractions, signs,
compound units, invalid units, and formatted sub-second values. No current-
master Go source delta exists and no Rust-only behavior was justified for
removal.

## Validation

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/config/configtypes -count=1`
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --offline --locked -p tidb-config --lib configtypes -- --test-threads=1` (from `rust/`)

The Go package and all five filtered Rust owner tests pass. No Go/Bazel or
Rust production source changed, so `make bazel_prepare`, failpoint toggling,
and code-change lint were not applicable. Broader config-loader and server
integration remains outside this leaf audit.

This receipt certifies the bounded `pkg/config/configtypes` inventory and
parity check; it is not a repository-wide transcreation claim.
