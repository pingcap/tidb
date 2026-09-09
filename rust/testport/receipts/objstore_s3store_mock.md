# `pkg/objstore/s3store/mock` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

This generated helper is a separate Go package with two tracked artifacts and
338 lines. Both the BUILD target and generated AWS `S3API` MockGen output were
read in full before this receipt was written. There are no tests, fixtures,
platform variants, or additional generator inputs.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 12 | `aeae8f456a26262ed9f0bb00afd58e3f2ca6fd1a` | `b004f357bad880c0f7dcd2ea68aef81e0739d4cad6d26d9560e94070fc2880a8` | generated mock library target |
| `s3api_mock.go` | 326 | `7a53ae3b96089f5b2a308bf1c860bb9de4879659` | `915f53058f4aa4c76116759941b87c45af4ac4f50bf6d6f94bbd9629466e73dd` | MockGen implementation for all `S3API` methods |

The generated output contains 31 constructors, accessors, method stubs, and
expectation recorders for the 14-method AWS v2 `S3API` contract. It is
unchanged from the pinned source. If the interface changes, regenerate this
file with MockGen rather than editing it directly.

## Rust ownership and explicit boundary

Rust has no GoMock-compatible AWS S3 API mock owner. This is Go test support,
not production behavior; a Rust mock would be speculative and would not close
the S3 backend package claim.

No Rust-only behavior was found to remove.

## Validation and risk

Profile: **WIP** for this documentation-only generated-artifact boundary
record. No source or BUILD files changed. The generated package was compiled
as part of the exact Go `origin/master` S3 failpoint suite:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh pkg/objstore/s3store -count=1
# exact Go origin/master source: PASS, 8.005s
```

Not verified here: MockGen regeneration, Bazel, cloud services, or full
workspace tests. No Rust validation was applicable because no Rust source
changed.
