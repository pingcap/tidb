# `pkg/lightning/backend` — complete package parity receipt

Pinned Go source: `5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (`origin/master`).

## Complete inventory

The package has exactly three tracked artifacts and 846 Go lines. Every BUILD,
production, and test line was read in full from the pinned source; this path is
unchanged on the hparser branch.

| Go artifact | Lines | Blob | Rust disposition |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 44 | `c72a205ac0cbf4213baf86d165916213228eb68a` | public library and 14-shard flaky test target; no Rust build input |
| `backend.go` | 440 | `e2344f21770a86639b5dc745fd7802a041d9d53a` | backend/engine lifecycle contracts, deterministic UUIDs, retries, metrics, and writer interfaces; no Rust owner |
| `backend_test.go` | 362 | `2c4070b3cde177fbbf24e69ea9368ce10c0ea0e3` | 14 functional source tests plus gomock suite helpers |

The production file contains 18 function/method declarations and the public
types `EngineFileSize`, `LocalWriterConfig`, `EngineConfig`,
`LocalEngineConfig`, `ExternalEngineConfig`, `CheckCtx`, `TargetInfoGetter`,
`Backend`, `EngineManager`, `OpenedEngine`, `ClosedEngine`, and `EngineWriter`.
It defines deterministic table/engine UUID derivation, open/close/unsafe-close
flows, metric accounting, flush/import/cleanup retry semantics, logging, and
the local writer contract. The test file has 14 `TestXxx` functions, one
gomock suite constructor, one teardown helper, and no benchmark, fixture,
testdata, or generated input. The sole production failpoint is
`FailIfEngineCountExceeds`. There are no package docs, platform variants,
fuzz corpora, README files, or additional build artifacts.

## Rust ownership and parity result

No Rust crate owns this Lightning backend abstraction. Rust's generic storage,
transaction, tablecodec, and import-protocol modules do not provide the Go
`EngineManager`/`OpenedEngine`/`ClosedEngine` lifecycle, backend capability
interfaces, deterministic UUID/tag logging, metric counters, or retry and
duplicate-import semantics. Searches found no `MakeEngineManager`,
`OpenedEngine`, `EngineWriter`, `RetryImportDelay`, `ShouldPostProcess`, or
`MakeUUID` Rust owner or call site.

No Rust-only behavior was found to remove. A standalone lifecycle wrapper
would be unobservable without concrete Lightning local/external backends,
engine storage, metric context, and import protocol consumers, so no
speculative facade, ignored test, or cache-only path was added.

## Validation

Profile: Ready for this documentation-only boundary update; no Go, Bazel,
module, generated, or Rust source changed.

Passed from the repository root on the current branch with failpoint state
managed by the repository wrapper:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 ./tools/check/failpoint-go-test.sh ./pkg/lightning/backend -count=1
PASS
ok   github.com/pingcap/tidb/pkg/lightning/backend 1.339s
```

The same exact Go-master failpoint-enabled suite passed in a detached worktree
(`1.459s`), and both wrappers returned failpoint refcount 0 after cleanup.
Rust formatting, repository lint, and `git diff --check` are run for the
receipt batch. Since only documentation changes, `make bazel_prepare` is not
required. No Rust regression test is applicable while the dependency-closed
backend owner is absent.

## Risk and next boundary

- Correctness: all three artifacts, 18 production declarations, 14 tests,
  metric/retry branches, and the failpoint are mapped; exact Go-master tests
  pass.
- Compatibility: UUID namespace/tag shape, engine lifecycle ordering,
  context metrics, and retry/duplicate semantics remain an explicit Rust
  integration boundary.
- Performance: no runtime code changed; no alternate engine manager or writer
  path was introduced.

The next audit should cover a concrete local/external backend implementation
only after its engine storage, metrics, protocol, and writer dependencies can
be closed atomically.
