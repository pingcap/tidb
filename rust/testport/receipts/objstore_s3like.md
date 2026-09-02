# `pkg/objstore/s3like` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`c6054025ed4c32ab3672a2a24ea46892714d21ec` (2026-09-01).

## Complete inventory

The root package contains eight tracked artifacts and 1,424 lines. Every
production source, test source, and BUILD target was read in full before this
receipt was written. It has no package fixtures, platform-specific files, or
generated outputs. The nested `pkg/objstore/s3like/mock` directory is a
separate Go package and is covered by its own receipt.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 51 | `3476e85d32b3d6aec99e4b1a60e8ff468d07a63a` | `a05b9fc997fb6e234893bc5555bb4fd2f1156d7b849e1390d3926c5d489b97df` | public S3-compatible library and flaky permission test target |
| `interface.go` | 119 | `f9ea8f912977e3c8791ed844eb758561ee13f64d` | `bc1bda28cec9b7aaaf07b414c33c5ed4314c855e4daf9719d5ea40ea1aec1226` | provider constants, response/value types, uploader and `PrefixClient` contracts |
| `io.go` | 201 | `0027c15e6dc54a129d747a136988c04ef6b2e5c8` | `76a9e1c2966b8564b3629292d4c37be485f336c52d6bd0cd2cc1a912fac63ea5` | retrying ranged reader/seek implementation and asynchronous multipart writer |
| `metrics.go` | 58 | `25081294272a03593ca9f754b37d0baee687f7e3` | `00f05b4e3d6801e702a4a50a5191f9d29d7fed31b12e91327ae845d64d02feca` | S3/OSS/KS3 API call counter and labels |
| `permission.go` | 50 | `71cb267e0d88c924546b75e83069055516941f06` | `9e72e4963407d68ce689384a56effeb52a3ee328c3aeb507d1c39f4c665a6b9c` | permission dispatch and annotated errors |
| `permission_test.go` | 68 | `77d516841643be20d95a49cd29347983b0edb923` | `ddbdabb62a6123dde1711f60f53f44679012e7cb538ab668b79656773a114742` | permission success, failure, and unknown-permission coverage |
| `retry.go` | 193 | `2137664600febb5d856af98fce7b53173d59c07f` | `bb7e3c81a3edd5762c1edd15f90277928b94d89f57fce805ff6d9ded78950251` | AWS/OSS retry adapter, delay policy, connection-error classifiers, failpoint |
| `store.go` | 684 | `03cbe0930fd20fb7d544a9d900ce56da58e1f620` | `6a61a641460aeb1c901836ec9bad0a99227366f8bb0522d7ebd3f9391bd65bd4` | S3 options/flags, CRUD, ranged opens, walks, multipart create, rename, presign |

The production sources contain 50 functions/methods. The source test contains
three top-level tests in this checkout: the original table-driven permission
checks plus focused regressions for retry-log suppression and non-positive
presign expiration. The reader, writer, retry, range parser, option
validation, replication status, deletion batching, walk filtering, and
presign behavior were read function by function; the BUILD dependency and
flaky test settings were also read rather than inferred.

The current Go-master delta from the earlier pinned source
`e2788410d8d696605e8cb002585877a063ccc909` was read in full. `Retryer` now
supports an optional `WithLogSuppressor` callback and suppresses retryability
warnings for matched errors while retaining retry metrics. `PresignFile`
rejects non-positive expiration durations before delegating to a capable
backend. No test, BUILD, fixture, platform, or generated-file delta
accompanied those changes.

## Rust ownership and explicit boundary

Rust has no dependency-closed owner for the Go S3-compatible backend contract,
cloud retry adapters, multipart/ranged I/O, provider option parsing, or
Prometheus labels. Rust references to presigned URLs and plan-replayer storage
traits are narrower consumers and are not this package's backend implementation.

No Rust-only behavior was found to remove, and no Rust implementation was
needed for the Go-only compatibility restoration. The checkout now matches
Go master for the retry suppressor and positive presign-expiration guard while
the package remains an explicit Rust parity boundary.

## Validation and risk

Profile: **Ready** for this bounded Go compatibility fix; the package remains
an explicit Rust parity boundary. The package uses failpoints, so the
canonical failpoint wrapper was used after the focused regressions were added.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh ./pkg/objstore/s3like -count=1
# PASS
```

The generated mock package was compiled independently from the exact same
source revision:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/objstore/s3like/mock -count=1
# exact Go origin/master source: [no test files], passed
```

The pre-fix focused test run failed to compile because `WithLogSuppressor` was
undefined; the post-fix focused and full failpoint-wrapped suites pass.
`make lint` and `git diff --check` are run for the pushed batch. Because the
new tests add top-level `Test*` functions, `make bazel_prepare` is required by
repository policy; it is attempted and blocked by the missing local `bazel`
executable. Not verified here: cloud-provider integration services, Bazel's
flaky target, Windows execution, or full-workspace tests. No Rust validation
was applicable because no Rust source changed.
