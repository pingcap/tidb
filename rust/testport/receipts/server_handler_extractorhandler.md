# `pkg/server/handler/extractorhandler` parity receipt

Status: complete inventory and explicit HTTP/domain boundary; no production edit
was required. This receipt covers the complete Go extractor handler package
and does not claim repository-wide parity.

Comparison source: Go `origin/master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec`.

## Complete Go inventory

Before deciding ownership, all four tracked artifacts were read in full: 439
total lines. There is no package `doc.go`, fixture, generated source, platform
variant, benchmark, fuzz target, or nested package.

| artifact | lines | role |
| --- | ---: | --- |
| `BUILD.bazel` | 56 | HTTP-handler target and integration dependencies |
| `extractor.go` | 166 | plan-extraction HTTP endpoint, task parsing, streaming response, and failpoint |
| `extract_test.go` | 141 | in-process server/extractor integration scenarios |
| `main_test.go` | 76 | common test bootstrap, metrics, failpoint, and leak checks |

The four files are byte-identical to the pinned Go master source.

## Rust ownership and parity decision

The handler coordinates Go's `domain.ExtractHandle`, extstore, HTTP router,
server lifecycle, statement-summary persistence, and an extraction failpoint.
Rust's `tidb-session` explicitly marks the underlying `ExtractTask` plan path
as a Go-parity gap (it needs internal SQL sessions, plan codec, and persisted
statement-summary storage); no dependency-closed Rust HTTP/domain owner exists
for this endpoint. Adding a facade would invent a second extraction lifecycle.

No Rust-only behavior was found to remove and no missing Go behavior can be
implemented safely outside the server/domain/external-storage migration unit.

## Validation

Profile: **Ready** for this documentation-only integration boundary.

- `./tools/check/failpoint-go-test.sh pkg/server/handler/extractorhandler -run 'TestExtractHandler$|TestExtractHandlerInfoSchemaV2$' -count=1` with pinned Go/GOPATH — passed; the wrapper enabled and disabled failpoints.
- `cmp` against Go master for all four artifacts — passed.
- `make lint` and `git diff --check` — passed in the surrounding Ready gate.
- No Go or Bazel artifact changed, so `make bazel_prepare` was not required.

## Risks and unverified scope

The focused integration tests pass against the in-process Go server. Rust live
HTTP extraction, persisted statement-summary storage, extstore streaming, and
non-host platform builds remain outside this explicit boundary.
