# `pkg/config/configtypes` — Go-master parity audit receipt

Status: complete dependency-closed audit with one focused Rust parity fix.

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
compound units, invalid units, and formatted sub-second values. The audit
found that Go 1.25's `strconv.ParseFloat` accepts hexadecimal floating
literals and valid digit separators; Rust now decodes those forms (and keeps
malformed separator placement rejected) before applying the same binary-unit
conversion. The regression failed before the fix on `0x1p10KiB` and
`1_000KiB`, then passed after the fix. No Rust-only behavior was introduced.

## Validation

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/config/configtypes -count=1`
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --offline --locked -p tidb-config --lib configtypes -- --test-threads=1` (from `rust/`)
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --offline --locked -p tidb-config --test all config_load_source -- --test-threads=1` (from `rust/`)
- `cargo +nightly-2026-08-22 fmt --all -- --check` (from `rust/`)
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 clippy --offline --locked -p tidb-config --lib -- -D warnings` (from `rust/`)
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint`

The Go package, all six filtered Rust owner tests, three config-loader source
tests, Rust formatting and clippy checks, and the Ready lint gate pass. The
first focused Rust regression run failed as expected before the parser fix;
the same command passes after it. No Go or Bazel artifact changed, so
`make bazel_prepare` and failpoint toggling were not applicable. Broader
server integration remains outside this leaf audit.

This receipt certifies the bounded `pkg/config/configtypes` inventory and
parity check; it is not a repository-wide transcreation claim.
