# `pkg/util/logutil/consistency` — Go-master package boundary receipt

Go source: `origin/master` at
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01). This nested
directory is a separate import path from `pkg/util/logutil` and has no source
delta from the extraction pin.

## Complete inventory

Both package artifacts were read in full before ownership review:

| Artifact | Lines | Git blob | SHA-256 | Inventory |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 23 | `3e0c7594177494f8ae08b59ec9ffefc936ed3ec0` | `88df90aecf53f2183a09ac7f92c911b82765834b701dea306b936ebd99788a85` | public library target and storage/tablecodec/logging dependencies |
| `reporter.go` | 313 | `8fe201c938333564bc9d340e281aae998afd45a6` | `fb142692a267397bbb5be6f15cff4624a743dfdbcf2bb3181e69d0f54668b9f0` | MVCC lookup/decode helpers, row/index report payloads, redaction, and inconsistency errors |

The boundary has 336 Go lines, 11 production functions/methods (including
`RecordData.String` and the unexported decode/region helpers), no tests,
benchmark, fuzz target, example, fixture/testdata tree, generated output, or
platform/build-tag variant. Its BUILD target has no failpoint dependency.

## Go behavior and Rust ownership decision

The package is a reporting adapter used by Go executor consistency checks. It
fetches MVCC by encoded row/index key, resolves region IDs, decodes row and
index values into JSON, truncates oversized payloads, applies redact-log mode,
and appends stack/error fields before returning the three typed TiDB errors.
The `Reporter` also carries table/index metadata and storage callbacks used to
construct the diagnostic fields.

Rust has no dependency-closed `Reporter` equivalent. The consistency check
itself is owned by `rust/crates/tidb-executor/src/admin_check.rs` and surfaced
by `rust/crates/tidb-session/src/admin_check_arm.rs`, which computes the same
count, missing-row, and indexed-value mismatch outcomes. It intentionally
returns structured `AdminCheckError` values mapped to the client error text;
there is no Rust MVCC helper/storage callback or redaction/logging subsystem
that can safely reproduce this diagnostic adapter in isolation.

No Rust-only behavior was found, and no safe missing Go behavior can be added
without porting the complete storage-backed diagnostic and redaction stack.
The Go package remains explicitly unclaimed as a cross-cutting reporting
adapter; no duplicate Rust implementation or regression carrier was added.

## Validation

Profile: WIP for the continuing repository audit; this receipt adds evidence
only and does not claim a source fix.

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/logutil/consistency -count=1` — passed (`[no test files]`; package compiles).
- `git diff --stat e2788410d8d696605e8cb002585877a063ccc909..origin/master -- pkg/util/logutil/consistency` — empty.
- `rg` inventory checks found no failpoint, build-tag, fixture, generated, or platform variant in the boundary.

No Go or Bazel file changed, so `make bazel_prepare` is not required. The
Rust admin-check suites were not rerun for this evidence-only boundary; their
existing source-backed tests cover the executable check path rather than the
Go-only MVCC diagnostic logger.

## Risks and unverified scope

- Correctness: future consistency diagnostics must preserve exact error codes,
  handle/index/value formatting, MVCC truncation, and redact-log policy.
- Compatibility: Rust client errors cover the check outcome but do not promise
  Go's structured log fields or MVCC JSON payloads.
- Performance: no runtime code changed; no new storage reads or decoding were
  introduced.
- Not verified locally: live TiKV MVCC response decoding, redaction modes,
  region-cache failures, and dependent executor logging paths.
