# `pkg/parser/auth` — complete package parity receipt

Pinned Go source: `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`
(`origin/master`).

## Complete inventory

The package has exactly eight tracked artifacts and 920 text lines. Every
production, test, and BUILD line was read from the pinned tree before the
ownership decision.

| Go artifact | Lines | Blob | Role |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 36 | `5e814c5da26fcf6b4f5cf343765a97667fd33463` | auth library/test metadata and 16-shard target |
| `auth.go` | 95 | `dfd11b34b255793a61fa40ea0724284841704686` | user/role identities and restore/string forms |
| `caching_sha2.go` | 245 | `24314fac0b34fe14373d94e148c26a98422d3e23` | SHA-crypt password verification/generation |
| `caching_sha2_test.go` | 84 | `2f4df2af7110ac5f37d22307e5d299b6417d2123` | SHA-2 vectors and round-trip benchmark |
| `mysql_native_password.go` | 99 | `e97ab4c9b794e9ae28efce4479ddeaef20f1c3eb` | native SHA-1 scramble and password encoding |
| `mysql_native_password_test.go` | 48 | `d5ca7598d684c627ac95876f68e10b0cf0744cc3` | native password vectors and invalid-input test |
| `tidb_sm3.go` | 216 | `f7eecf69ad2c406cc85eb7ad9c6ca838117813df` | SM3 hash implementation and hash interface |
| `tidb_sm3_test.go` | 97 | `baef7e7923e70a496901384c62c5c4017892d472` | SM3 vectors and password round trips |

The production files contain 31 function declarations; the test files contain
18 test/benchmark declarations. There are no generated inputs, platform
variants, fixtures, fuzz corpora, or build artifacts beyond the BUILD target.

## Go-master comparison

`git diff HEAD..origin/master -- pkg/parser/auth` is empty. The current branch
matches Go master for identity restoration, nil/string behavior, native SHA-1
scrambling, caching-SHA2 SHA-crypt formatting, SM3 compression, salt-byte
constraints, malformed hash errors, and all source test vectors. No source fix
or new Go regression test is needed.

## Rust ownership and parity result

`tidb-parser::auth` is the dependency-closed Rust owner. It provides the
identity types, restore/string behavior, native SHA-1 helpers, SHA-crypt
SHA-256/SM3 verification and generation, byte-preserving password APIs, and
the SM3 `Hash` implementation. Its source-derived tests cover all Go vectors,
invalid digest/iteration/short-hash paths, random-salt constraints, non-UTF-8
Go-string bytes, identity quoting, and benchmark obligations. No Rust-only
behavior requiring removal was found.

## Validation

Profile: Ready for this documentation-only boundary receipt.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./auth -count=1 (current branch): PASS; 0.380s
Rust `cargo +nightly-2026-08-22 test -p tidb-parser --test all parser_auth -- --test-threads=1`: PASS; 20 tests
Rust `cargo +nightly-2026-08-22 fmt --all -- --check`: PASS
Pinned-Go `make lint`: PASS
`git diff --check`: PASS
```

No Go/Rust/Bazel/module source changed, so `make bazel_prepare` is not
required for this receipt.

## Risks and next boundary

- Correctness: authentication hashes and scramble bytes are wire/storage
  compatibility contracts; changing iteration, salt, or digest ordering breaks
  existing accounts and clients.
- Compatibility: identity quoting and password error categories are consumed
  by parser, session, executor, and protocol paths; mixed-version login tests
  remain important.
- Performance: SHA-crypt is intentionally CPU-heavy and SM3 is a block hash;
  the Rust owner preserves the Go loop structure without adding conversions.
