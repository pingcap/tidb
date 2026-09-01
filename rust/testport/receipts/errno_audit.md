# `pkg/errno` — Go-master parity audit receipt

Status: complete dependency-closed audit; the current Go-master catalog delta
is already present in the Rust error authority.

Comparison source: Go `origin/master` at
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01). The package has
eight tracked artifacts and 2,815 lines:

| Artifact | Lines | Role |
| --- | ---: | --- |
| `BUILD.bazel` | 31 | library/test target, embedded source, and shards |
| `errcode.go` | 1,201 | MySQL, TiDB, DDL, resource, and storage error codes |
| `errname.go` | 1,213 | code-to-message and redaction metadata |
| `errname_test.go` | 71 | complete-code message and reserved-range checks |
| `infoschema.go` | 158 | global/user/host error-warning counters |
| `infoschema_test.go` | 81 | deep-copy safety regression |
| `logredaction.md` | 33 | redaction policy documentation |
| `main_test.go` | 27 | common test setup and teardown |

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

Go master adds four code identities (three dual-password errors 3878/3894/3895
and `ErrDDLAutoPausedByKVDiskFull` 8276) plus their exact message templates.
Rust already carries each constant, catalog entry, redaction metadata, and
the executor/DDL error constructors. The Rust catalog test checks all 1,166
source codes and 1,164 message entries, including the dual-password vectors;
the infoschema source test covers the source copy-safety contract.

The Rust `Option<SystemTime>` representation for a not-yet-seen `LastSeen`
value is translated to Go's zero `time.Time` at the information-schema row
boundary. This is an ownership-neutral representation detail, not a semantic
divergence. No production edit or duplicate regression carrier was justified,
and no Rust-only behavior was removed.

## Validation

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/errno -count=1`
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --offline --locked -p tidb-error --test all catalog -- --test-threads=1` (from `rust/`)
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --offline --locked -p tidb-error --test all infoschema -- --test-threads=1` (from `rust/`)

All targeted suites pass: the Go package, eight catalog tests (including the
new-source identities), and the infoschema copy-safety test. No Go/Bazel or
Rust production source changed, so `make bazel_prepare`, failpoint toggling,
and code-change lint were not applicable. Broader server/session integration
is covered by their existing consumers and remains outside this leaf audit.

This receipt certifies the bounded `pkg/errno` inventory and parity check; it
is not a repository-wide transcreation claim.
