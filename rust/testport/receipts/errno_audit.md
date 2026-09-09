# `pkg/errno` — Go-master parity audit receipt

Status: Ready. The complete dependency-closed Go package is inventoried and
the current Go-master catalog delta is present in the Rust error authority.

Comparison source: Go `origin/master` at
`febee17ec716d86b1e355e5400ef9e4f4f190bad` (2026-09-02). The package has
eight tracked artifacts and 2,828 lines after the focused catalog regression:

| Artifact | Lines | Git blob | Role |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 31 | `d76b8b4e0b8a5a2558bd552148fd01e7766f59e3` | library/test target, embedded source, and shards |
| `errcode.go` | 1,202 | `b83a1d3fd47f1879e7ec873bfd072c5f917b981a` | MySQL, TiDB, DDL, resource, and storage error codes |
| `errname.go` | 1,217 | `4a0526849df701ca0848167c536f1f46bae91131` | code-to-message and redaction metadata |
| `errname_test.go` | 79 | working-tree batch | complete-code, reserved-range, and shared-lock-lost catalog checks |
| `infoschema.go` | 158 | `e0a8b0f9984ec4c8c70dfb9f0204b1fe2861d382` | global/user/host error-warning counters |
| `infoschema_test.go` | 81 | `3878f57b90ddfa421b00c9f63cf502bb5e90e1c5` | deep-copy safety regression |
| `logredaction.md` | 33 | `5afb3a0adea06171f8117d38157637ebc09f96d5` | redaction policy documentation |
| `main_test.go` | 27 | `1b13e50f544f86600faba4d0b236d9dcd6f117b0` | common test setup and teardown |

There is no package `doc.go`, generated source, platform variant, benchmark,
fixture/testdata directory, or nested Go package. The production counter file
contains nine function declarations (including `init`); the test artifacts
contain four declarations. Every Go production, test, build, and documentation
artifact was read in full before comparing Rust owners.

## Rust owner comparison

The complete catalog is split across the generated-from-source-shaped
`tidb-error::tidb::errcode` and `tidb::errname` modules (all constants,
messages, redaction positions, and source-order arrays), with
`tidb-error::tidb::infoschema` owning the `ErrorSummary` counters and deep
snapshots. The ordinary session/server consumers read and update that shared
authority; `FLUSH CLIENT_ERRORS_SUMMARY` and the information-schema tables do
not use a private Rust-only counter.

Go master adds `ErrSharedLockLost` (9015) and its exact message template. Rust
now carries the source-ordered constant and catalog entry,
including redaction position 1 for the encoded key. The source-derived
regression checks the numeric identity, complete message, redaction metadata,
and updated catalog totals (1,167 codes and 1,165 messages). The previously
audited dual-password and DDL disk-full identities remain present, and the
infoschema source test still covers the source copy-safety contract.

The Rust `Option<SystemTime>` representation for a not-yet-seen `LastSeen`
value is translated to Go's zero `time.Time` at the information-schema row
boundary. This is an ownership-neutral representation detail, not a semantic
divergence. No production edit or duplicate regression carrier was justified,
and no Rust-only behavior was removed.

The 2026-09-02 Go package batch restores the 9015 constant and message in the
integration branch and adds a focused regression for its numeric identity,
message template, and key-redaction position. The Rust catalog implementation
and its source-derived tests were already delivered in the adjacent
`errno: add shared lock lost error identity` commit; this batch changes only
the Go package and its receipt/plan.

## Validation

- Pre-fix `go test ./pkg/errno -run '^TestSharedLockLostErrorCatalog$' -count=1` failed to compile because `ErrSharedLockLost` was absent.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex go test ./pkg/errno -run '^TestSharedLockLostErrorCatalog$' -count=1` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex go test ./pkg/errno -count=1` — passed.
- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-error --test all catalog -- --test-threads=1`
- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-error --test all -- --test-threads=1`
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint`
- `git diff --check`

All Ready checks listed above pass for this Go package batch and the adjacent
Rust catalog change. Because this batch adds a top-level Go test and changes
the error catalog, `make bazel_prepare` is required; it is blocked locally by
the missing `bazel` executable (`make: bazel: No such file or directory`).
Failpoint toggling is not required. Broader transaction-driver/session use of error 9015 belongs to the
adjacent `pkg/kv`, `pkg/store/driver/txn`, and `pkg/session` package batches and
is not claimed here.

This receipt certifies the bounded `pkg/errno` inventory and parity check; it
is not a repository-wide transcreation claim.
