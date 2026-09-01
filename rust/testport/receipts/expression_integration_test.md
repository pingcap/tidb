# `pkg/expression/integration_test` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package contains four tracked artifacts and 4,906 lines. Every production
test helper, test function, README/build instruction, and 50-shard Bazel
target was read before this receipt was written. There is no `doc.go`, fixture
tree, generated source, platform-specific variant, or generator input.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 55 | `531bc1f867a362513bb0ffed01466ff2ee8bcfc9` | `ab8a96509505487777fc1b6659a914ba7bf0418e51fabff7a7a1cae7ceb41821` | 50-shard flaky integration-test target and dependency closure |
| `README.md` | 3 | `78311d5ff72921941eb8e4740e9734e10d534541` | `fa4f1648d1aa6fa22df01eff9fc1f24d851fcd883f10a141b60f9236475e48dd` | package rule: do not add tests here because the suite is already large |
| `integration_test.go` | 4,791 | `ba63626bd90780a4d6765e02b3279270f339c3d4` | `a6bb71f07ee798dcebe3692b05f576554332c86e97bc3c14a1df1cfca414b862` | 63 SQL integration tests plus seven package helpers |
| `main_test.go` | 57 | `b5bb1a3b8a5f88c9a9a441221839ccabd41fe574` | `46a94b4415356271c63258c0a83f9769f82cb915307e8c32f59ed6c82369ef78` | common test setup, failpoint enablement, timezone, and goleak harness |

`integration_test.go` declares 63 top-level tests covering FTS/parser syntax,
vector DDL and execution, vector indexes and operators, JSON/time/arithmetic
functions, pushdown and planner regressions, row checksums, user variables,
plan cache, and issue reproductions. Helpers cover the FTS starter-mode gate,
vector-search setup, lock/checksum/index utilities, and embedding-provider
starter/non-starter setup. `main_test.go`'s `TestMain` installs the common
test setup, failpoints, system timezone, and goleak exclusions. The README's
no-new-tests rule is part of the package contract.

The current Go master delta from the earlier pinned source
`e2788410d8d696605e8cb002585877a063ccc909` is confined to the integration
surface: `BUILD.bazel` adds executor, inference, and LOAD DATA/mydump deps;
`integration_test.go` adds `io`/`sync`, those imports, and four embedding
tests (`TestEmbedTextFunction`, `TestAutoEmbeddingGeneratedColumnDML`,
`TestAutoEmbeddingDDLValidation`, and
`TestAutoEmbeddingGeneratedColumnLoadData`) with three mode/provider helpers.
No existing test or harness behavior changed.

## Rust ownership and parity status

This package is an end-to-end Go SQL harness, not a Rust production package.
Its tests span parser, planner, executor, DDL, storage, domain, and deployment
mode. Rust source-parity carriers already account for the historical rows in
`rust/testport/receipts/b076.md`; the four new embedding tests remain explicit
cross-crate gaps. `tidb-expr` currently has only ignored source stubs for
`EMBED_TEXT` argument inference/evaluation, and there is no Rust domain embed
provider, deployment-mode switch, generated-column DDL policy, or LOAD DATA
executor path to which a leaf patch could safely attach.

No Rust-only behavior was found to remove in this test-only package. Adding a
partial EMBED_TEXT evaluator or weakening Go's starter-mode/DDL restrictions
would be speculative and would violate the package's dependency closure. The
correct implementation unit is the complete provider → expression → generated
column → executor/LOAD DATA pipeline, with focused tests at each owner.

## Validation and risk

Profile: **WIP** for this documentation-only boundary audit; no production,
test, or Bazel file was changed, so no new regression test or package-complete
Ready claim is made. Exact Go-master checks run from a detached worktree:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh pkg/expression/integration_test \
  -run 'TestEmbedTextFunction|TestAutoEmbedding' -count=1   # passed
```

The failpoint runner enabled and disabled Go failpoints with cleanup. The
starter-gated functional portions self-skipped on this non-nextgen host; the
non-starter error and API-key/provider assertions passed. Bazel execution,
all 50 shards, nextgen starter deployment, real embedding providers, and full
workspace suites were not verified. No Go/Bazel source changed, so
`make bazel_prepare` was not required.

Risks are confined to the documented cross-package embedding gap and the
environment-gated integration paths. Runtime correctness and performance are
unchanged by this receipt.

This receipt certifies the bounded `pkg/expression/integration_test` inventory
and its explicit integration-only boundary; it is not a repository-wide parity
claim.
