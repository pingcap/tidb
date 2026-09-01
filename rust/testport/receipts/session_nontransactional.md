# `pkg/session/nontransactional.go` — production-owner parity boundary receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete target inventory

The nontransactional DML implementation is one production file in the root
`pkg/session` package. The target file has 873 lines and 21 functions, plus
the `job` and `statementBuildInfo` types. Its complete control flow was read
before comparing Rust: admission and session-state restoration; shard-column
constraints and table-reference checks; shard-job construction; dry-run and
worker execution; failpoint/error aggregation; max-execution-time handling;
table/index/handle discovery; SQL restoration; and result-set construction.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `pkg/session/nontransactional.go` | 873 | `c86f260a2a8c3bf51e788c9d229f84772c48ef81` | `554f058f7fe66d6443cb2c5da286161818dd1c7726b91efe3c00ef96a6ad362f` | nontransactional DML admission, planning, sharding, and execution |
| `pkg/session/BUILD.bazel` | 244 | `cf51a38f920ade4617689225c3f13ab1654a9ea4` | `eba274b23990a7a35d854c3f292ba377fa5774ea3ecc256763ff156e27ed505b` | root session library/test targets and dependency closure |
| `pkg/session/test/nontransactionaltest/BUILD.bazel` | 26 | `92608921fec8a1971d09d44f939478b6d42cebd4` | `4a325cad067d036c51f744a173a775b18b230cda74da7e1933c6d37624830e55` | six-shard nontransactional test target |
| `pkg/session/test/nontransactionaltest/main_test.go` | 62 | `be4f11d5a13b2f9023d4ee90471695342c3a4a56` | `723413403b18bc996d3bd37a5c187d7f4ba9ae808e4bba03b4994644618ed9f3` | failpoint/goleak test harness |
| `pkg/session/test/nontransactionaltest/nontransactional_test.go` | 526 | `9507e9c5aa7bdf022cdc2c1521d05f075804074e` | `a778bfc1b3988ea9433d91e01cec3da7f9b99584ecb2f6854d03cfee7d1416cf` | six SQL behavior tests and sharding helper |

The associated test package is fully inventoried in
`receipts/session_nontransactionaltest.md`. The root `pkg/session` package
also has 25 other direct Go/BUILD/ownership artifacts (15 production files,
nine tests, `BUILD.bazel`, and `OWNERS`) totaling 19,976 lines on Go master;
those unrelated bootstrap, transaction, upgrade, and server-lifecycle owners
remain deliberately outside this behavior slice. This receipt therefore does
not claim the root package itself is complete.

## Rust ownership and explicit boundary

Rust's `tidb-parser::dml` owns the `BATCH` grammar and AST envelope. The
`tidb-exec::nontransactional` module owns only a dependency-free admission
policy for autocommit/transaction, batch-DML compatibility variables, weak
consistency, snapshot pinning, and INSERT-source classification. Its source
tests intentionally stop before table-reference validation, shard selection,
worker execution, metrics, max-execution-time handling, and job-error
aggregation. Six ignored `tidb-session` source carriers preserve the Go test
identity but do not execute behavior.

The Go implementation's remaining behavior crosses the session, planner,
executor, catalog/index metadata, tablecodec, storage transaction, metrics,
failpoint, and result-set APIs. No dependency-closed Rust owner currently
spans those consumers. No Rust-only behavior was found to remove, and no
standalone missing implementation is safe without creating a second
nontransactional execution path or changing shard ordering, constraint, and
error semantics. The production owner is therefore recorded as an explicit
SEED/boundary for a future coordinated port.

## Validation and risk

Profile: **WIP** for this documentation-only boundary record. No Go source,
imports, test declarations, Bazel metadata, or module files changed, so
`make bazel_prepare`, the Ready lint gate, and a new regression test were not
required for this batch.

```text
# exact Go-master behavior suite (failpoint-managed)
./tools/check/failpoint-go-test.sh ./pkg/session/test/nontransactionaltest -count=1
# passed: 7 tests, 16.845s; failpoints enabled and disabled

# Rust parser source tests
cargo +nightly-2026-08-22 test -p tidb-parser --lib \
  tests::dml::batch_dml_preserves_the_go_nontransactional_wrapper -- --nocapture
# passed: 1 test
cargo +nightly-2026-08-22 test -p tidb-parser --lib \
  tests::dml::nontransactional_dml_source_table -- --nocapture
# passed: 1 test

# Rust admission test attempt
cargo +nightly-2026-08-22 test -p tidb-exec --test all nontransactional -- --nocapture
# environment-blocked: openssl-sys could not find pkg-config/OpenSSL
```

Not verified here: the full 19,976-line root session package, Bazel execution,
full Go repository tests, live TiKV batch-DML scheduling, or a future
dependency-closed Rust production implementation. Correctness risk is the
unported cross-owner execution semantics; compatibility and performance risk
remain unchanged because this batch changed documentation only.

This receipt certifies the complete nontransactional production-file audit and
its explicit ownership boundary; it is not a repository-wide transcreation
claim.
