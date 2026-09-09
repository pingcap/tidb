# `pkg/objstore/mockobjstore` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

This generated helper is a separate Go package with two tracked artifacts and
232 lines. The BUILD target and generated GoMock output were read in full
before this receipt was written. There are no production sources, tests,
fixtures, platform variants, or additional generator inputs.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 13 | `9ac198f4dd974c2653fb0691bd97fb08d14f80c0` | `94c65a80c4c4a0ab85be2360d4c5c8d7484d194601ff5da5767194eac12ca4f7` | generated mock library target |
| `objstore_mock.go` | 219 | `a5a3e2a8136c7d0eab0ad0c7ae13210221cd4dd8` | `59bb22f7c39e79620148f0d300bb99bfcac3391c9d24967e9b2af040af996a5a` | MockGen implementation for all `storeapi.Storage` methods |

The generated file contains 27 constructors, accessors, method stubs, and
expectation recorders for the complete storage contract, including context,
reader/writer options, walk callbacks, presigning, and close semantics. It is
unchanged from the pinned source. If `storeapi.Storage` changes, regenerate
this output with MockGen rather than hand-editing it.

## Rust ownership and explicit boundary

Rust's plan-replayer tests define a local `DumpFileStorage` mock, and TiKV
tests define unrelated KV storage mocks; neither implements the Go
`storeapi.Storage` contract or owns this generated helper. Rust has no
GoMock-compatible object-store mock package, so adding one would be
speculative test scaffolding.

No Rust-only behavior was found to remove.

## Validation and risk

Profile: **WIP** for this documentation-only generated-artifact boundary
record. No source or BUILD files changed.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/objstore/mockobjstore -count=1
# exact Go origin/master source: [no test files], passed
```

Not verified here: MockGen regeneration, Bazel, or full-workspace tests. No
Rust validation was applicable because no Rust source changed.
