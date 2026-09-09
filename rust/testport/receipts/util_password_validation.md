# `pkg/util/password-validation` — complete Go-master parity receipt

Comparison source: Go `origin/master` at commit
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01). The package is
unchanged from the earlier pinned implementation; this receipt refreshes the
authority, complete inventory, and current validation result.

## Complete inventory

The package contains three tracked artifacts and 379 lines. All production,
test, and Bazel files were read in full before this update. There is no package
doc, README, harness, fixture, benchmark, generated file, platform variant,
or ownership file.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 28 | `bf9ad67810b4351a6a8b3e525546fbb4c68b7886` | `1c07c99c1a74cf79fa6680ab100a59fc6a4fad5f196ea0e0368e8eddca0e01fc` | public validator library and flaky short test target |
| `password_validation.go` | 175 | `b3931de5e6ef6e0759f8f318ce61e24ba061b111` | `f5a89348c6c5aa7d10532226f1b052ce38e26232e976677a32b39116771196b9` | dictionary, username, low/medium policy, and full password validators |
| `password_validation_test.go` | 176 | `725f0a5392840d1e1858ae8a89e04801f1a5f4b3` | `3fec39cfda134a129e1f2b1ce520852b89e9fed5909e2439e616fd4a4cc1e024` | five source tests covering dictionary, username, low/medium, and full policy |

The validators read global sysvars, preserve Go's byte-oriented username
checks and Unicode rune policy counts, enforce LOW/MEDIUM/STRONG ordering, and
return the source warning/error text. The test matrix covers dictionary length
filters and Unicode words, username reversal, length and character classes,
and all three policy levels.

## Rust ownership and parity

`rust/crates/tidb-util/src/password_validation.rs` is the dependency-closed
owner. Its minimal `GlobalVarAccessor`, `PasswordUser`, and `PwdError` bridges
preserve the imported Go interfaces without adding policy. Passwords and user
names use `GoString` for arbitrary bytes; lowercasing and rune classification
follow Go's replacement behavior. Session account-DDL and expression callers
own enablement/error handling, matching their respective Go paths.

The prior implementation removed Rust-only validation enablement helpers,
duplicate sysvar constants/catalog entries, public error-code helpers, extra
derives, and supplemental tests. Exactly the five Go test behaviors remain in
the owner; no Rust-only password behavior is present.

## Validation and risk

Profile: **WIP** for this documentation-only authority refresh. No Go source,
imports, Bazel metadata, or module files changed; `make bazel_prepare` and the
Ready lint gate are not required.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/util/password-validation -count=1
# passed: five Go source tests in 0.502s

OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-util password_validation::tests:: --lib --offline --locked -- --nocapture
# passed: 5 Rust source-derived tests
```

The Rust command emitted existing workspace warnings only. Not verified here:
full downstream account/expression suites, Bazel execution, and full workspace
tests. Existing unrelated session worktree changes remain outside this receipt.
