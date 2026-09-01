# `pkg/keyspace` — Go-master parity audit receipt

Status: complete dependency-closed audit; no source behavior delta or
Rust-only execution policy was found.

Comparison source: Go `origin/master` at
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01). The package has
exactly five tracked artifacts and 404 lines:

| Artifact | Lines | Role |
| --- | ---: | --- |
| `BUILD.bazel` | 40 | library/test targets and shard metadata |
| `doc.go` | 38 | next-generation keyspace isolation contract |
| `keyspace.go` | 102 | names, etcd namespaces, codec bytes, logging, and PD context |
| `keyspace_test.go` | 132 | config, kernel-mode, policy, and benchmark coverage |
| `username_policy.go` | 92 | default and Starter username policies |

There is no generated Go source, platform-specific variant, fixture/testdata
directory, nested package, or additional build artifact. The production files
contain 17 function/method declarations plus the `CodecV1` value and keyspace
constants; the test file contains three tests and one benchmark. Every Go
production, test, documentation, and Bazel artifact was read in full before
comparing the Rust owner.

## Rust owner comparison

`rust/crates/tidb-util/src/keyspace.rs` is the dependency-closed owner and is
registered by `src/lib.rs`. It preserves the complete behavior of both Go
production files: API-v1 versus keyspace-ID etcd namespace paths (including
the slash variant), global-config name lookup, once-computed keyspace bytes
with Go's nil-on-classic result, empty-name detection, the `keyspaceName`
logger field, V1/V2 API-context construction, and the permissive/default and
Starter prefix username policies. The `KeyspaceCodec` trait and `ApiContext`
enum are local carriers for the absent client-go and PD types; they preserve
the source values and do not introduce a second runtime path.

The owner includes source-shaped regressions for both namespace forms, empty
and classic byte behavior, API context selection, default and prefix policy
validation/format/variants/original-name behavior, exact `[ddl:1468]` error
rendering, and the empty-prefix bootstrap case. No Go source delta exists at
the comparison commit, and no Rust-only behavior was justified for removal.
The Go benchmark has no production-observable contract and has no Rust
benchmark counterpart.

## Validation

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/keyspace -count=1`
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --offline --locked -p tidb-util --lib keyspace -- --test-threads=1` (from `rust/`)
- `cargo +nightly-2026-08-22 fmt --all -- --check` (from `rust/`)

The Go package and seven filtered Rust owner tests pass. The first Rust
attempt without the workspace OpenSSL environment was blocked by
`openssl-sys`; the rerun with the pinned workspace dependency path passed.
No Go/Bazel or Rust production source changed, so `make bazel_prepare`,
failpoint toggling, and code-change lint were not applicable. Broader server,
PD, and logger integration remains outside this leaf audit.

This receipt certifies the bounded `pkg/keyspace` inventory and parity check;
it is not a repository-wide transcreation claim.
