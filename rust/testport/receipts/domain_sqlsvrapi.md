# `pkg/domain/sqlsvrapi` parity receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The root package contains exactly two tracked artifacts and 82 lines: one
public Bazel target and one production interface file. Both artifacts were
read in full before this receipt. There is no `doc.go`, test, fixture,
`testdata`, generated source or input, platform variant, benchmark, fuzz
target, or `OWNERS` file in this package directory (the nested `mock` package
is audited separately).

| artifact | lines | Go-master blob | SHA-256 | role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 14 | `1e0fec28f152c8fbfa209fbae077e22600eedbb6` | `d7e043fe177ba1001e34f60511fedbb6da1d67289c4059658fc32686d4a0df80` | public `sqlsvrapi` library target |
| `server.go` | 68 | `220c2b2aaa12bbeba3db5ad7cf12e515a537f617` | `c2cebbea99e35cc53ae84b14fe0f73412ff80dde55ac61785e9aa3cde2e981a9` | `Runtime`, `KSRuntimeHandle`, and `Server` interfaces |

The production inventory has three top-level interface declarations. Current
files are byte-identical to the pinned Go-master artifacts.

## Native integration decision

`sqlsvrapi` is a Go-native public boundary for SQL runtime access, keyspace
handle release, table-mode DDL submission, and DDL-owner management. It is
coupled to TiDB KV, metadata, owner, and session-pool interfaces. Rust has no
dependency-closed replacement for this domain API or its current-keyspace and
cross-keyspace consumers. No Rust-only behavior or missing Go behavior was
found, so no source edit or speculative Rust adapter was justified.

## Validation and risk

Profile: **Ready** for this documentation-only boundary audit. The package
and its generated mock consumer package both compile:

    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
    GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
    go test ./pkg/domain/sqlsvrapi ./pkg/domain/sqlsvrapi/mock
    # ? .../pkg/domain/sqlsvrapi [no test files]
    # ? .../pkg/domain/sqlsvrapi/mock [no test files]

Rust formatting and `git diff --check` are shared receipt gates. `make lint`
passed in the preceding source batch, and `make bazel_prepare` is not required
for this documentation-only change; the local executable remains unavailable.

There is no runtime compatibility or performance risk from the receipt-only
change. The interface remains explicitly Go-owned until a complete Rust
dependency graph can provide the same SQL/session/DDL contract.

## Outcome

The complete root `sqlsvrapi` inventory and explicit Go-only boundary are
recorded. The nested generated mock package is recorded in the companion
receipt, and the rolling audit continues.
