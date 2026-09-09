# `pkg/config/deploymode` — Go-master parity audit receipt

Status: complete dependency-closed audit; no source behavior delta or
Rust-only execution policy was found.

Comparison source: Go `origin/master` at
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01). The package has
exactly four tracked artifacts and 319 lines:

| Artifact | Lines | Role |
| --- | ---: | --- |
| `BUILD.bazel` | 26 | library/test targets and shard metadata |
| `doc.go` | 40 | deployment-mode and consistency contract |
| `mode.go` | 155 | process-wide mode, parsing, validation, and serialization |
| `mode_test.go` | 98 | JSON, TOML, kernel gating, and state tests |

There is no generated Go source, platform-specific variant, fixture/testdata
directory, benchmark, or nested package. The production file contains 11
function/method declarations; the test file contains three tests. Every Go
production, test, documentation, and Bazel artifact was read in full before
comparing the Rust owner.

## Rust owner comparison

`rust/crates/tidb-config/src/deploymode.rs` is the dependency-closed owner and
is registered by `src/lib.rs`. It preserves the three mode values and names,
the process-wide atomic state, NextGen-only predicates and setter, case-
insensitive parsing, display/validity/list behavior, JSON serialization and
deserialization, TOML configuration decoding, and the source error strings
for invalid values and Classic-kernel configuration. The Rust `Mode` wrapper
and `Display`/Serde traits are the native carriers for Go's integer methods
and encoding interfaces; they do not add an alternate execution policy.

The inline Rust tests cover every Go test assertion and additionally pin
invalid-value display/serialization and integer ordering used by downstream
configuration validation. Existing config-loader tests exercise the owner
through the ordinary configuration path. No current-master Go source delta
exists and no Rust-only behavior was justified for removal.

## Validation

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/config/deploymode -count=1`
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --offline --locked -p tidb-config --lib deploymode -- --test-threads=1` (from `rust/`)

The Go package and all three filtered Rust owner tests pass. No Go/Bazel or
Rust production source changed, so `make bazel_prepare`, failpoint toggling,
and code-change lint were not applicable. Broader config-loader, deployment,
and server integration is covered by its existing consumers and remains
outside this leaf audit.

This receipt certifies the bounded `pkg/config/deploymode` inventory and
parity check; it is not a repository-wide transcreation claim.
