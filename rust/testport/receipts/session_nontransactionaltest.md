# `pkg/session/test/nontransactionaltest` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

This test-only package contains three tracked artifacts and 614 lines. Every
test source and Bazel target was read in full before comparing the Rust
workspace. There is no production source, `doc.go`, fixture directory,
generated output, benchmark, fuzz target, or platform/build-tag variant.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 26 | `92608921fec8a1971d09d44f939478b6d42cebd4` | `4a325cad067d036c51f744a173a775b18b230cda74da7e1933c6d37624830e55` | six-shard flaky test target |
| `main_test.go` | 62 | `be4f11d5a13b2f9023d4ee90471695342c3a4a56` | `723413403b18bc996d3bd37a5c187d7f4ba9ae808e4bba03b4994644618ed9f3` | TestMain setup and goleak harness |
| `nontransactional_test.go` | 526 | `9507e9c5aa7bdf022cdc2c1521d05f075804074e` | `a778bfc1b3988ea9433d91e01cec3da7f9b99584ecb2f6854d03cfee7d1416cf` | six SQL behavior tests and sharding helper |

The test surface defines eight functions (including the sharding helper) and
seven top-level tests. Coverage includes BATCH DML sharding for integer,
varchar, primary-key, and secondary-index layouts; failpoint error
aggregation; snapshot/weak/autocommit behavior; check constraints; foreign
keys; nontransactional metrics; and max-execution-time handling. The harness
enables failpoints, aggregates shard errors, and checks for goroutine leaks.

## Rust ownership and explicit boundary

Rust's `tidb-parser::dml` parses `BatchDml`, and `tidb-exec::nontransactional`
provides only admission policy types and checks. The Rust workspace also has
six ignored source-carrier tests corresponding to these Go tests. It does not
implement the Go session's batch-DML planner, worker execution, failpoint
error aggregation, constraint/foreign-key semantics, metrics, or execution
time handling. Those behaviors are owned by the un-audited production package
`pkg/session/nontransactional.go` and its executor, planner, and storage
consumers.

No Rust-only behavior was found to remove, and no safe missing behavior can be
implemented in this test-only package without first closing that production
dependency graph. The package is therefore recorded as an explicit
SEED/boundary; the production owner must be audited as one complete package
before any parity implementation or regression test is added.

## Validation and risk

Profile: **WIP** for this documentation-only boundary record. No Go source,
imports, test declarations, Bazel metadata, or module files changed, so
`make bazel_prepare`, Rust compilation gates, and the Ready lint gate were not
required for this batch.

```text
(cd <detached-origin/master-worktree> && \
 PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
 GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
 ./tools/check/failpoint-go-test.sh ./pkg/session/test/nontransactionaltest -count=1)
# passed: pkg/session/test/nontransactionaltest (16.845s); failpoints enabled and disabled
```

The package was tested from an exact detached Go-master worktree. No Rust
code changed, so no Rust owner test was applicable. Not verified here: Bazel
execution, full Go repository tests, live TiKV batch-DML scheduling, or the
future dependency-closed Rust production implementation.

This receipt certifies the bounded test-package inventory and ownership
decision; it is not a repository-wide transcreation claim.
