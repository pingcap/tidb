# `pkg/param` — complete Go-master parity receipt

Comparison source: Go `origin/master` at commit
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01).

## Complete inventory

The package contains two tracked artifacts and 44 lines. Both files were read
line by line. There is no package doc, Go test, fixture, generated/platform
variant, benchmark, fuzz target, or additional build input.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 12 | `5434815a2d0299ffb3bd4fcde97747efdc5f51da` | `3e28e401564a7e698bd415199474483b5254ced2b61bcceef87e383b873d1b82` | public library target with errno/dbterror dependencies |
| `binary_params.go` | 32 | `0a3d5197bb49a778f67d37642020d173ef945f60` | `96a761a0d5c7ccd39815878a749b4f7ad563d1cdb39f74a5a7779b7b1746c988` | `ErrUnknownFieldType` and the raw `BinaryParam` carrier |

`binary_params.go` has no parser or executable methods: it declares the
server error identity and the four source-visible fields (`Tp`, `IsUnsigned`,
`IsNull`, and raw `Val`) consumed by `pkg/server` and expression decoding.

## Rust ownership and decision

The complete carrier and its packet-splitting consumer are owned by
`tidb-protocol::binary_params`. Its `BinaryParam` preserves the four Go fields;
`BinaryParamError::UnknownFieldType` maps to Go's `ErrUnknownFieldType` errno,
while malformed packets retain the generic packet error boundary. The Rust
splitter is sourced from the adjacent Go `pkg/server/conn_stmt_params.go`, not
invented as a second implementation in this data-only package.

The existing Rust tests exercise fixed-width, temporal, length-encoded,
NULL-bitmap, long-data, charset-decoding, unsigned-flag, malformed, and
unknown-type behavior. No Go source or Rust production fix is needed for this
package; adding a second Rust carrier would be Rust-only duplication.

## Validation and risk

Profile: **WIP** for this docs-only inventory. No Go or Bazel source changed,
so `make bazel_prepare` is not required.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/param -count=1
# passed (no test files)

OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-protocol --test all binary_params --offline --locked
# passed; 12 binary-parameter tests
```

Correctness, compatibility, and performance are unchanged. Not verified:
workspace-wide Ready validation, Bazel analysis, and live protocol traffic.
