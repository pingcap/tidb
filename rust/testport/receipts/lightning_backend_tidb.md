# `pkg/lightning/backend/tidb` — complete package parity receipt

Pinned Go source: `5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (`origin/master`).

## Complete inventory

The package has exactly three tracked artifacts and 2,227 Go lines. Every
production, test, and BUILD line was read in full from the pinned source. The
current hparser branch is byte-identical for this path.

| Go artifact | Lines | Blob | Rust disposition |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 68 | `adc1afb6ca400a9646500e80cdee000525142513` | public library plus 17-shard flaky test target; no Rust build input |
| `tidb.go` | 1,063 | `83a3c51bdf9e246973a34fd3002dce83d7aa4847` | TiDB SQL backend, encoders, target metadata reader, retries, statement cache, and writer; no Rust owner |
| `tidb_test.go` | 1,096 | `0177b681685ff15c499459c8ae1fe60dc932c9c9` | 17 functional source tests, helpers, and SQL-mock fixtures |

The production file contains 42 function/method declarations. It covers
`EncodingBuilder`/`tidbEncoder`, SQL literal escaping for every supported
datum kind, row chunking, target database/table metadata discovery, auto-ID
compatibility across TiDB 4.0 and 4.x result shapes, conflict strategy
selection, prepared-statement caching, retry and row-by-row downgrade policy,
error-manager recording, and the no-op engine lifecycle. The two failpoint
injection points are `FetchRemoteTableModels_BeforeFetchTableAutoIDInfos` and
`FailIfImportedSomeRows`.

The test file has 17 `TestXxx` functions, one disabled strict-mode helper,
three suite/data helpers, and no benchmark. Its SQL-mock fixtures exercise
replace/ignore/error duplicate strategies, strict and non-strict SQL literal
encoding, metadata for old/new auto-ID schemas, dropped tables and concurrent
metadata batches, retry/error-threshold behavior, duplicate recording,
prepared statements, row-size/row-count chunking, and fallback row recording.
There are no package docs, testdata directories, binary fixtures, generated
sources, platform variants, fuzz corpora, README files, or extra build inputs.

## Rust ownership and parity result

No Rust crate implements the Lightning TiDB SQL backend or its dependency-closed
database/sql writer. The Rust workspace has generic transaction, tablecodec,
session, error-manager, and statement-cache components, but no owner for this
package's `sql.DB`/`sql.Tx` metadata queries, TiDB 4.x auto-ID compatibility,
SQL literal serializer, conflict/error-manager downgrade flow, prepared
statement lifecycle, or Lightning `EngineWriter` integration. Searches found
no `NewTiDBBackend`, `FetchRemoteTableModels`, `EncodeRowForRecord`,
`WriteBatchRowsToDB`, or `tidb_lightning_errors` Rust consumer.

No Rust-only behavior was found to remove. Porting a single serializer,
retry loop, or mock writer would create a second path without the required SQL
driver, Lightning backend, table/encoder, and error-manager closure. No
speculative facade, ignored test, or cache-only implementation was added.

## Validation

Profile: Ready for this documentation-only boundary update; no Go, Bazel,
module, generated, or Rust source changed.

Passed from the repository root on the current branch with failpoints enabled
and disabled by the repository wrapper:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 ./tools/check/failpoint-go-test.sh ./pkg/lightning/backend/tidb -count=1
PASS
ok   github.com/pingcap/tidb/pkg/lightning/backend/tidb 0.836s
```

The same failpoint-enabled suite passed in a detached worktree at the exact
Go-master pin (`0.967s`). The wrapper reported failpoint refcount 0 after
cleanup. Rust formatting, repository lint, and `git diff --check` are run for
the receipt batch. Because only documentation changes, `make bazel_prepare` is
not required. No Rust regression test is applicable while the dependency-
closed owner is absent.

## Risk and next boundary

- Correctness: all three artifacts, 42 production declarations, 17 tests,
  failpoint branches, SQL-mock fixtures, and the 17-shard target are mapped;
  the exact Go-master suite passes.
- Compatibility: SQL-driver behavior, TiDB versioned metadata, error-manager
  persistence, duplicate policy, and prepared statement ownership remain an
  explicit Rust integration boundary.
- Performance: no runtime code changed. The source's batching, retry limits,
  and statement-cache policy have not been approximated by a separate path.

The next audit should cover the parent `pkg/lightning/backend` contract or a
Rust dependency closure that can own the full TiDB SQL backend atomically.
