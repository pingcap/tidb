# `pkg/objstore/storeapi` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package contains three tracked artifacts and 404 lines. Every production
source, test source, and BUILD target was read in full before this receipt was
written. It has no `doc.go`, fixture directories, generated files, platform
variants, benchmarks, fuzz inputs, or additional build artifacts.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 25 | `4041f93210016581465df33e13546c80200d2219` | `f487a319f0a1f484f644daad6353adaef2df6ea631cdc62d43fee20880c2c623` | public storage-contract library and two-shard test target |
| `storage.go` | 320 | `11650b0de9b9276ec033a036e459ac069b450e1e` | `a5bfb9c8761546a999baedf349e471a33c325003076c96cb7f0f84629dbdaa04` | permissions, options, reader/writer/copy contracts, prefixes, ranges, and multipart limits |
| `storage_test.go` | 59 | `c6a617a2a5b3a7abc18ce54bf7d021a1885f9ad7` | `593ce66492acd2d858fbd86363c223f9de8d9cb73d7b57167629322ad668cd53` | prefix normalization and HTTP range truth-table tests |

The production source contains 11 functions/methods: prefix construction and
joining, object-key/path/string conversion, bucket-prefix helpers, HTTP range
formatting, and unique permission-check key generation. The public interfaces
(`StrongConsistency`, `Uploader`, `Copier`, and `Storage`) and option structs
were read field by field, including context/error/close contracts and
credential/access-recording semantics. The two tests cover slash trimming,
empty and nested prefixes, double-slash preservation for caller-supplied object
names, and full/partial/open-ended HTTP ranges.

The current Go-master delta from the earlier pinned source
`e2788410d8d696605e8cb002585877a063ccc909` was read in full: the obsolete
`ReadSeekCloser` interface was removed in favor of `objectio.Reader`; a shared
`MaxUploadParts = 10000` constant and `ErrExceedMaxUploadParts` sentinel were
added for S3/GCS/OSS writers; WriterOption documentation now states that not
all backends honor it; and the S3 retryer comment was clarified. BUILD gains
the PingCAP errors dependency needed by the sentinel. No platform, generated,
fixture, or test-source delta accompanied these contract changes.

## Rust ownership and explicit boundary

Rust has no dependency-closed owner for the Go object-store `Storage` contract,
permission model, backend-independent options, prefix/range helpers, or
multipart-limit sentinel. Existing Rust storage-shaped traits belong to
plan-replayer dump composition or unrelated TiKV RPC paths; they do not expose
the Go cloud/local backend API and cannot substitute for its consumers.

No Rust-only behavior was found to remove. A disconnected Rust trait or range
helper would be speculative without the root object-store package and every
cloud backend. This contract package remains an explicit parity boundary.

## Validation and risk

Profile: **WIP** for this documentation-only boundary record. No Go, Bazel,
module, or Rust source changed in this batch; failpoints are not used here, so
`make bazel_prepare`, Ready lint, and Rust cargo gates are not required.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/objstore/storeapi -count=1
# exact Go origin/master source: passed in 0.393s
```

Not verified here: the 50-shard root target, cloud backend integrations,
concurrent multipart behavior, Bazel, or full-workspace tests. No Rust
validation was applicable because no Rust source changed.
