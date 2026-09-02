# `pkg/store/copr` Go-master parity receipt

Status: Ready for this focused package batch. The receipt covers the complete
root Go package inventory at Go `origin/master` `94eb995357f34b7bab4889a82f0405797046447d`;
it is not a repository-wide parity claim.

## Complete inventory

All root production, test, build, and support artifacts were read before the
edit. The root package has 20 tracked artifacts and 10,649 lines. There is no
`doc.go`, generated Go source/input, platform-specific variant, fixture, or
benchmark fixture. The nested `copr_test` and `metrics` directories are
separate Go packages and remain separate receipt boundaries.

| Artifact | Lines | Git blob |
| --- | ---: | --- |
| `BUILD.bazel` | 128 | `1dff7d834976c871417db6989772dfe5307645df` |
| `batch_coprocessor.go` | 1,739 | `2fe7defee64374fe313d00950f6ef45cd7919b43` |
| `batch_coprocessor_test.go` | 587 | `42fb6e46be7d4297b3fae36d2425236160d3b2c1` |
| `batch_request_sender.go` | 119 | `5c6d9a6cbe1927395b5331b744c60899f2d93cdf` |
| `coprocessor.go` | 3,061 | `12d44cb352562b47a9f72aee90138d8b4238a254` |
| `coprocessor_cache.go` | 224 | `01da36c3c3df458901011c1be7ed129eeab4cce1` |
| `coprocessor_cache_test.go` | 259 | `24929e858830557a396c59697babb72baf9dc129` |
| `coprocessor_test.go` | 1,213 | `bcf0cb42443feb01d18f46e426008d3e2aa776ba` |
| `ema.go` | 64 | `dbeaf3ea16d9e9576cebf6cdea79a95397fd07ff` |
| `ema_test.go` | 183 | `f4619594355ec39c7fff2d233f416ef81d33d69c` |
| `key_ranges.go` | 165 | `d1d27077152aef556b7dd538c2f94538106a6b38` |
| `key_ranges_test.go` | 126 | `e2893a3fef1c69596a998633074a50ee54d44efc` |
| `main_test.go` | 47 | `ad6f13003c5043b448ce7de2e5aa6b5a06894b7f` |
| `mpp.go` | 357 | `b692def3d29e83f7e7cfb797d7ce622fb3cebe18` |
| `mpp_probe.go` | 335 | `8043e9f316c2c19556167d3aa41d6863174a3e35` |
| `mpp_probe_test.go` | 229 | `387bc90ff3d73e6746b8d008de727bfbb6cbd12c` |
| `range_diagnostics.go` | 94 | `14c360df8286aae87c4dbdd9ce87993e3b176684` |
| `region_cache.go` | 1,024 | `6e7ec9edf5364110ae3d8127959190a6ac77fce4` |
| `region_cache_test.go` | 539 | `337eedd2524644bd9bb7b9558e71d2aeda535eb9` |
| `store.go` | 156 | `bfb635bcafaf8dee17d39a7f597d0db075d7da3c` |

## Behavior and Rust boundary

Go commit `0c53024bd3` fixed store-batched coprocessor lock handling: each
`StoreBatchTaskResponse` owns its own `Locked` payload, which must be passed to
`handleLockErr` so the child task's lock is resolved and retried through the
ordinary fallback path. The branch was incorrectly reading the parent
response's lock field, silently skipping child-lock resolution. The production
fix uses the already-extracted `lockErr` value and leaves batching, retry, and
fallback sequencing unchanged.

`TestHandleBatchCopResponseResolvesChildLock` constructs a real mock TiKV
store, supplies a child-only pessimistic lock, and asserts the lock resolver
performs its RPC. It failed before the fix (`RPCStatsCount` 0) and passes after
the fix (`RPCStatsCount` 1), alongside the existing bucket-version regression.

The Rust owner is the dependency-closed `tidb-distsql`/`tidb-txnkv` transport
surface, which already models the StoreBatchTask wire envelope and lock
recovery primitives. It does not own Go's `pkg/store/copr` worker lifecycle, so
no Rust-only behavior was invented or removed in this batch. The live TiKV
batch-worker integration and remaining runtime-stat differences stay explicit
boundaries in `receipts/distsql_audit.md`.

## Ready validation

- Focused pre-fix regression: `go test ./pkg/store/copr -run '^TestHandleBatchCopResponseResolvesChildLock$' -count=1 -vet=off` failed as expected.
- Focused post-fix tests: failpoint-wrapped `TestHandleBatchCopResponse(ResolvesChildLock|UpdatesChildBucketsOnVersionNotMatch)` passed.
- Full package: `./tools/check/failpoint-go-test.sh ./pkg/store/copr -count=1 -vet=off` passed (all tests).
- `make bazel_prepare` was attempted as required for the new test/imports but is blocked because `bazel` is not installed; the two exact Gazelle test dependencies were added to `BUILD.bazel`.
- `make lint` and `git diff --check` are required completion gates for this batch and are recorded with the commit.

## Risks and remaining work

Correctness risk is limited to the previously skipped child-lock resolution;
the existing resolver handles the lock type and retry semantics. Compatibility
risk is limited to additional test dependencies. Performance is unchanged on
the no-lock path and now performs the intended resolver work when a child lock
is reported. External live-TiKV/PD behavior, Bazel analysis, and the nested
`copr_test`/`metrics` packages were not changed or claimed complete here.
