# `pkg/util/mock` — Go-master package boundary receipt

Go source: `origin/master` at
`a74cc59699d4f02a3c87bd91d01dbf347d9ed10f` (2026-09-02).

## Complete inventory

All ten Go-master artifacts were read in full before deciding ownership:

| Artifact | Lines | Blob | SHA-256 | Inventory |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 73 | `00c9a1dafb62d1ae23ac797b400dd2668be70efe` | `f72126b7c8e2131bf7f8454d43ad72264370ea367a2fcfde497e2d3686d07105` | library/test targets and the complete session/KV/chunk/mock dependency graph |
| `client.go` | 33 | `f675e96c99b51bcf6c3cbe8ec0802c951dfa3f63` | `df7ac2711a0056b72a9f9087d7ff5bf401607c4c0ec6ed9aeede2ac29c04711d` | mock KV client returning a configured response |
| `context.go` | 735 | `57096b74531c221e59ef4a7f90213ab4ba15365c0` | `0b05baed19e1ce6964a527926e8207a2aab1be53037672fe8d63cf25192e398a` | test-only session/plan/expression/SQL context, fake transaction lifecycle, session variables, infoschema, locks, no-op interface methods, and query cop-store limiter propagation |
| `fortest.go` | 28 | `87a301e2a6ad9bb823c902565034d0d68deeefe6` | `aa46c8f5c87c9927271a089e8530f1b1c096c3e04714e9fcf85d649087ebdaad` | `!codes` test-only constructor gate |
| `iter.go` | 133 | `8807567b3ea4060c7a4224552670cdee62f7afd5` | `a294e0782410b0728569aa1930865fdfc7f595406128a15784cdeb34f91fcc98` | slice iterator and injectable-error/close-count mock iterator |
| `iter_test.go` | 93 | `af76288ba6c6c1b3d93d52e4581e76fe70f951de` | `bc84be4e66e8c80b74252f31e3011901ef1b79a13afee624fa729cb3941af13d` | source iterator table test |
| `main_test.go` | 34 | `28264fbe37db8c6ab68a79a68736e55fcf1c83af` | `d6842d6fe6dd495890010de4a75102297b41d4eabdba4139ee5c3096d7bd03ae` | common testkit/goleak harness |
| `metrics.go` | 41 | `e0dcd89468682c852d7b65a58566bf74a24520d7` | `db64ef3b966f31daea8ac21d0ebbfa2b9e32cabe6eabaf5964eb33e42b33d73d` | Prometheus counter mock with atomic value |
| `mock_test.go` | 48 | `8c468a9d80721c916cbbe412db1c693cb3379a66` | `9ecd91d09daccef154cc080884c1e3fa7eb816cfdc5680aef5a8b32bcf45a896` | context value/clear test and constructor benchmark |
| `store.go` | 105 | `c4ba414da694a8d378be7f7f1cfd127864fccf74` | `4b67565cd2e3be3cd1c84f87aa4584e85fc1e5d2ded7e5fb4fda808a37d21748` | mock KV storage, transaction/snapshot stubs, status/options, and cluster metadata |

The package has 1,323 Go lines. There is no `doc.go`, generated/platform
variant, fixture/testdata tree, or nested package; `fortest.go` is a build-tag
variant and `main_test.go` is harness-only. The package is explicitly test
infrastructure despite its broad interface assertions, and has no production
runtime consumer outside Go tests.

## Rust ownership and decision

Rust has many local mocks (session contexts, timer stores, TiKV mock PD
clients, and restricted SQL executors), each tailored to one crate's trait
surface. None is a dependency-closed equivalent of this package's single
`sessionctx.Context` implementation, fake transaction, infoschema/session
variables, KV storage/client, iterator, and Prometheus counter set. The Go
types implement dozens of interfaces whose definitions cross parser, planner,
executor, session, table, statistics, and KV packages; moving only a mock
would either duplicate those traits or create a Rust-only test framework.

No Rust-only production behavior was found. The 2026-09-02 Go package batch
restores the missing `GetDistSQLCtx` propagation: positive
`SessionVars.QueryCopStoreLimit` values now create a query-scoped limiter with
the configured capacity, while zero disables it. The focused regression failed
before the fix with a nil limiter and capacity zero, then passed after the
five-line wiring change. This package remains explicitly test infrastructure;
its broader mock/session surface is still Go-only.

## Validation

Profile: **Ready** for the continuing repository audit.

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex go test ./pkg/util/mock -run '^TestGetDistSQLCtxQueryCopStoreLimiter$' -count=1` — passed in 0.524s; pre-fix assertion failed on nil/zero limiter.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex go test ./pkg/util/mock -count=1` — passed in 0.473s.
- The same pinned command passed in the exact detached Go-master worktree `/tmp/tidb-go-latest-c605`.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go list -tags=codes -f '{{.GoFiles}}|{{.IgnoredGoFiles}}|{{.TestGoFiles}}' ./pkg/util/mock` — passed in both worktrees and confirmed the `!codes` `fortest.go` variant is excluded.
- `git diff --exit-code a74cc59699d4f02a3c87bd91d01dbf347d9ed10f -- pkg/util/mock` — only the intended propagation and focused regression changes remain before commit.
- Rust search across session, timer, TiKV, and SQL mock owners — confirmed local trait-specific mocks and no package-level replacement.

Go production and test sources changed and a new top-level test was added, so
`make bazel_prepare` was required and attempted; it is blocked locally because
`bazel` is not installed (`make: bazel: No such file or directory`). The full
Go testkit/mockstore integration and every Rust local mock suite were not run
for this explicitly unclaimed boundary.

## Risks and unverified scope

- Correctness: fake transaction behavior intentionally warns and returns empty
  values; replacing it with a real store would alter tests' safety contract.
- Compatibility: interface method sets and no-op return values change whenever
  `sessionctx.Context` or `kv.Storage` evolves, so this package must be
  inventoried again with those owners.
- Performance: no production path changed; the constructor benchmark remains
  Go-only.
- Not verified locally: build-tag `codes`, goleak harness, and dependent
  package suites that rely on mutable global sysvars or mock infoschema.
