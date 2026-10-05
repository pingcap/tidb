# `pkg/session/test/nontransactionaltest` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package contains three tracked artifacts and 614 lines. Every batch-DML
sharding/error/constraint/foreign-key/metric/max-execution-time test, the
`testSharding` helper, TestMain/goleak harness, failpoint dependency and
six-shard flaky Bazel target was read before this receipt was written. There
is no `doc.go`, fixture or `testdata` directory, generated output,
platform-specific variant, benchmark, fuzz target, or generator input.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 26 | `92608921fec8a1971d09d44f939478b6d42cebd4` | `4a325cad067d036c51f744a173a775b18b230cda74da7e1933c6d37624830e55` | six-shard flaky batch-DML test target and failpoint/metrics dependency closure |
| `main_test.go` | 62 | `be4f11d5a13b2f9023d4ee90471695342c3a4a56` | `723413403b18bc996d3bd37a5c187d7f4ba9ae808e4bba03b4994644618ed9f3` | common setup, TiKV failpoints, async-commit settings, and goleak harness |
| `nontransactional_test.go` | 526 | `9507e9c5aa7bdf022cdc2c1521d05f075804074e` | `a778bfc1b3988ea9433d91e01cec3da7f9b99584ecb2f6854d03cfee7d1416cf` | six batch-DML behavior tests and the composition helper |

`nontransactional_test.go` declares
`TestNonTransactionalDMLSharding`, `TestNonTransactionalDMLErrorMessage`,
`TestNonTransactionalWithCheckConstraint`,
`TestNonTransactionalDMLWorkWithForeignKey`, `TestNonTransactionalMetrics`,
and `TestNonTransactionalDmlIgnoreMaxExecutionTime`, plus `testSharding`.
Together they cover clustered/nonclustered/indexed int and varchar sharding,
success/error aggregation and redacted SQL messages under failpoints,
snapshot/weak-read/autocommit/batch-mode/unsupported-statement constraints,
foreign-key consistency, DML metric increments, and the max-execution-time
worker failpoint. `main_test.go` configures common test state, TiKV failpoints,
and goleak exclusions.

The Go master delta from the earlier pinned source
`e2788410d8d696605e8cb002585877a063ccc909` is empty for all three artifacts.

## Rust ownership and explicit boundary

The historical ignored carriers and the disconnected metric-label model do not
establish coverage of this Go workload. The label model and its private tests
have been removed. The retained tidb-exec::nontransactional admission owner is
unchanged. Complete shard planning, cancellation/error aggregation, foreign-key
checks, metric publication and failpoint acceptance still require live-owner
validation; consult the current register rather than treating old carriers as tests.

## Validation and risk

Profile: **WIP** for this documentation-only boundary audit; no production,
test, or Bazel file changed, so no new regression test or package-complete
Ready claim is made.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh \
  ./pkg/session/test/nontransactionaltest \
  -run '^TestNonTransactionalDmlIgnoreMaxExecutionTime$' -count=1  # passed

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh \
  ./pkg/session/test/nontransactionaltest \
  -run '^TestNonTransactionalDMLSharding$' -count=1                # passed
```

Both commands ran from the exact detached Go-master worktree; the failpoint
wrapper enabled and disabled failpoints around each run. Rust source, Bazel,
and module files were unchanged; `make bazel_prepare` and Ready lint were not
required. Not verified: the four remaining behavior tests, all six Bazel
shards, live TiKV execution, or the full repository suite. Correctness risk
is concentrated in the untranscreated worker/shard and failpoint lifecycle;
runtime behavior is unchanged because this batch modifies documentation only.

This receipt certifies the bounded nontransactional test-package inventory and
explicit ownership boundary; it is not a repository-wide parity claim.
